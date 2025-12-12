use crate::DataVgError;
use bytes::Bytes;
use data_types::{DataBlobGuid, DataVgInfo, QuorumConfig, TraceId};
use futures::stream::{FuturesUnordered, StreamExt};
use metrics_wrapper::{counter, histogram};
use rand::seq::SliceRandom;
use rpc_client_bss::RpcClientBss;
use rpc_client_common::RpcError;
use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

static EPOCH: OnceLock<Instant> = OnceLock::new();

fn current_timestamp_nanos() -> u64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

/// Configuration for circuit breaker behavior
#[derive(Clone, Debug)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit
    pub failure_threshold: u32,
    /// Duration to keep circuit open before allowing probe requests
    pub open_duration: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 3,
            open_duration: Duration::from_secs(30),
        }
    }
}

/// Circuit breaker states
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CircuitState {
    Closed = 0,
    Open = 1,
    HalfOpen = 2,
}

impl From<u8> for CircuitState {
    fn from(val: u8) -> Self {
        match val {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }
}

/// Thread-safe circuit breaker state using atomic operations
struct CircuitBreaker {
    state: AtomicU8,
    failure_count: AtomicU32,
    opened_at: AtomicU64,
    config: CircuitBreakerConfig,
}

impl CircuitBreaker {
    fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: AtomicU8::new(CircuitState::Closed as u8),
            failure_count: AtomicU32::new(0),
            opened_at: AtomicU64::new(0),
            config,
        }
    }

    /// Check if the circuit allows requests.
    /// Returns true if request should proceed, false if node should be skipped.
    fn is_available(&self) -> bool {
        let state = CircuitState::from(self.state.load(Ordering::Acquire));
        match state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                let opened_at = self.opened_at.load(Ordering::Acquire);
                let now = current_timestamp_nanos();
                let elapsed_nanos = now.saturating_sub(opened_at);
                if elapsed_nanos >= self.config.open_duration.as_nanos() as u64 {
                    // Try to transition to half-open (allow probe)
                    if self
                        .state
                        .compare_exchange(
                            CircuitState::Open as u8,
                            CircuitState::HalfOpen as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return true;
                    }
                    // Another thread already transitioned, check new state
                    return CircuitState::from(self.state.load(Ordering::Acquire))
                        != CircuitState::Open;
                }
                false
            }
            CircuitState::HalfOpen => {
                // In half-open state, we allow requests to probe
                true
            }
        }
    }

    /// Record a successful request
    fn record_success(&self) {
        let state = CircuitState::from(self.state.load(Ordering::Acquire));
        match state {
            CircuitState::HalfOpen => {
                self.state
                    .store(CircuitState::Closed as u8, Ordering::Release);
                self.failure_count.store(0, Ordering::Release);
            }
            CircuitState::Closed => {
                self.failure_count.store(0, Ordering::Release);
            }
            CircuitState::Open => {
                // Should not happen normally
            }
        }
    }

    /// Record a failed request
    fn record_failure(&self) {
        let state = CircuitState::from(self.state.load(Ordering::Acquire));
        match state {
            CircuitState::Closed => {
                let count = self.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
                if count >= self.config.failure_threshold {
                    self.state
                        .store(CircuitState::Open as u8, Ordering::Release);
                    self.opened_at
                        .store(current_timestamp_nanos(), Ordering::Release);
                }
            }
            CircuitState::HalfOpen => {
                // Probe failed, re-open circuit
                self.state
                    .store(CircuitState::Open as u8, Ordering::Release);
                self.opened_at
                    .store(current_timestamp_nanos(), Ordering::Release);
            }
            CircuitState::Open => {
                // Already open, update timestamp
                self.opened_at
                    .store(current_timestamp_nanos(), Ordering::Release);
            }
        }
    }
}

struct BssNode {
    address: String,
    client: RpcClientBss,
    circuit_breaker: CircuitBreaker,
}

impl BssNode {
    fn new(address: String, cb_config: CircuitBreakerConfig, connection_timeout: Duration) -> Self {
        debug!("Creating BSS RPC client for {}", address);
        let client = RpcClientBss::new_from_address(address.clone(), connection_timeout);
        Self {
            address,
            client,
            circuit_breaker: CircuitBreaker::new(cb_config),
        }
    }

    fn get_client(&self) -> &RpcClientBss {
        &self.client
    }

    fn is_available(&self) -> bool {
        self.circuit_breaker.is_available()
    }

    fn record_success(&self) {
        self.circuit_breaker.record_success();
    }

    fn record_failure(&self) {
        self.circuit_breaker.record_failure();
    }
}

struct VolumeWithNodes {
    volume_id: u16,
    bss_nodes: Vec<Arc<BssNode>>,
}

pub struct DataVgProxy {
    volumes: Vec<VolumeWithNodes>,
    round_robin_counter: AtomicU64,
    quorum_config: QuorumConfig,
    rpc_timeout: Duration,
}

impl DataVgProxy {
    pub fn new(
        data_vg_info: DataVgInfo,
        rpc_request_timeout: Duration,
        rpc_connection_timeout: Duration,
    ) -> Result<Self, DataVgError> {
        Self::new_with_circuit_breaker(
            data_vg_info,
            rpc_request_timeout,
            rpc_connection_timeout,
            CircuitBreakerConfig::default(),
        )
    }

    pub fn new_with_circuit_breaker(
        data_vg_info: DataVgInfo,
        rpc_request_timeout: Duration,
        rpc_connection_timeout: Duration,
        cb_config: CircuitBreakerConfig,
    ) -> Result<Self, DataVgError> {
        info!(
            "Initializing DataVgProxy with {} volumes, circuit breaker config: {:?}",
            data_vg_info.volumes.len(),
            cb_config
        );

        let quorum_config = data_vg_info.quorum.ok_or_else(|| {
            DataVgError::InitializationError(
                "QuorumConfig is required but not provided in DataVgInfo".to_string(),
            )
        })?;

        let mut volumes_with_nodes = Vec::new();

        for volume in data_vg_info.volumes {
            let mut bss_nodes = Vec::new();

            for bss_node in volume.bss_nodes {
                let address = format!("{}:{}", bss_node.ip, bss_node.port);
                debug!(
                    "Creating BSS node for node {}: {}",
                    bss_node.node_id, address
                );

                bss_nodes.push(Arc::new(BssNode::new(
                    address,
                    cb_config.clone(),
                    rpc_connection_timeout,
                )));
            }

            volumes_with_nodes.push(VolumeWithNodes {
                volume_id: volume.volume_id,
                bss_nodes,
            });
        }

        debug!(
            "DataVgProxy initialized successfully with {} volumes",
            volumes_with_nodes.len()
        );

        Ok(Self {
            volumes: volumes_with_nodes,
            round_robin_counter: AtomicU64::new(0),
            quorum_config,
            rpc_timeout: rpc_request_timeout,
        })
    }

    pub fn select_volume_for_blob(&self) -> u16 {
        // Use round-robin to select volume
        let counter = self
            .round_robin_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let volume_index = (counter as usize) % self.volumes.len();
        self.volumes[volume_index].volume_id
    }

    async fn get_blob_from_node_instance(
        &self,
        bss_node: &BssNode,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        trace_id: &TraceId,
        fast_path: bool,
    ) -> Result<Bytes, RpcError> {
        tracing::debug!(%blob_guid, bss_address=%bss_node.address, block_number, content_len, fast_path, "get_blob_from_node_instance calling BSS");

        let bss_client = bss_node.get_client();

        let mut body = Bytes::new();

        if fast_path {
            // Fast path: single attempt, no retries
            bss_client
                .get_data_blob(
                    blob_guid,
                    block_number,
                    &mut body,
                    content_len,
                    Some(self.rpc_timeout),
                    trace_id,
                    0,
                )
                .await?;
        } else {
            // Normal path with retries
            let mut retries = 3;
            let mut backoff = Duration::from_millis(5);
            let mut retry_count = 0u32;

            loop {
                match bss_client
                    .get_data_blob(
                        blob_guid,
                        block_number,
                        &mut body,
                        content_len,
                        Some(self.rpc_timeout),
                        trace_id,
                        retry_count,
                    )
                    .await
                {
                    Ok(()) => break,
                    Err(e) if e.retryable() && retries > 0 => {
                        retries -= 1;
                        retry_count += 1;
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.saturating_mul(2);
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        tracing::debug!(%blob_guid, bss_address=%bss_node.address, block_number, data_size=body.len(), "get_blob_from_node_instance result");

        Ok(body)
    }

    async fn delete_blob_from_node(
        bss_node: Arc<BssNode>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        rpc_timeout: Duration,
        trace_id: TraceId,
    ) -> (Arc<BssNode>, String, Result<(), RpcError>) {
        let start_node = Instant::now();
        let address = bss_node.address.clone();

        let result = async {
            let bss_client = bss_node.get_client();

            let mut retries = 3;
            let mut backoff = Duration::from_millis(5);
            let mut retry_count = 0u32;

            loop {
                match bss_client
                    .delete_data_blob(
                        blob_guid,
                        block_number,
                        Some(rpc_timeout),
                        &trace_id,
                        retry_count,
                    )
                    .await
                {
                    Ok(()) => return Ok(()),
                    Err(e) if e.retryable() && retries > 0 => {
                        retries -= 1;
                        retry_count += 1;
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.saturating_mul(2);
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        .await;

        let _result_label = if result.is_ok() { "success" } else { "failure" };
        histogram!("datavg_delete_blob_node_nanos", "bss_node" => address.clone(), "result" => _result_label)
            .record(start_node.elapsed().as_nanos() as f64);

        (bss_node, address, result)
    }

    /// Create a new data blob GUID with a fresh UUID and selected volume
    pub fn create_data_blob_guid(&self) -> DataBlobGuid {
        let blob_id = Uuid::now_v7();
        let volume_id = self.select_volume_for_blob();
        DataBlobGuid { blob_id, volume_id }
    }

    /// Multi-BSS put_blob with quorum-based replication
    pub async fn put_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();
        let trace_id = *trace_id;
        histogram!("blob_size", "operation" => "put").record(body.len() as f64);

        // Use the volume_id from blob_guid to find the volume
        let selected_volume = self
            .volumes
            .iter()
            .find(|v| v.volume_id == blob_guid.volume_id)
            .ok_or_else(|| {
                DataVgError::InitializationError(format!(
                    "Volume {} not found in DataVgProxy",
                    blob_guid.volume_id
                ))
            })?;
        debug!("Using volume {} for put_blob", selected_volume.volume_id);

        let rpc_timeout = self.rpc_timeout;
        let write_quorum = self.quorum_config.w as usize;

        // Compute checksum once for all replicas
        let body_checksum = xxhash_rust::xxh3::xxh3_64(&body);

        // Filter available nodes based on circuit breaker state
        let available_nodes: Vec<_> = selected_volume
            .bss_nodes
            .iter()
            .filter(|node| {
                let available = node.is_available();
                if !available {
                    counter!("circuit_breaker_skipped", "node" => node.address.clone(), "operation" => "put").increment(1);
                    debug!("Skipping node {} due to open circuit breaker", node.address);
                }
                available
            })
            .cloned()
            .collect();

        // Check if we have enough available nodes for quorum
        if available_nodes.len() < write_quorum {
            histogram!("datavg_put_blob_nanos", "result" => "insufficient_nodes")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::QuorumFailure(format!(
                "Insufficient available nodes ({}/{}) for write quorum ({})",
                available_nodes.len(),
                selected_volume.bss_nodes.len(),
                write_quorum
            )));
        }

        let mut bss_node_indices: Vec<usize> = (0..available_nodes.len()).collect();
        bss_node_indices.shuffle(&mut rand::thread_rng());

        let mut write_futures = FuturesUnordered::new();
        for &index in &bss_node_indices {
            let bss_node = available_nodes[index].clone();
            write_futures.push(Self::put_blob_to_node(
                bss_node,
                blob_guid,
                block_number,
                body.clone(),
                body_checksum,
                rpc_timeout,
                trace_id,
            ));
        }

        let mut successful_writes = 0;
        let mut errors = Vec::with_capacity(available_nodes.len());

        // Wait only until we achieve write quorum
        while let Some((node, address, result)) = write_futures.next().await {
            match result {
                Ok(()) => {
                    node.record_success();
                    successful_writes += 1;
                    debug!("Successful write to BSS node: {}", address);
                }
                Err(rpc_error) => {
                    node.record_failure();
                    warn!("RPC error writing to BSS node {}: {}", address, rpc_error);
                    errors.push(format!("{}: {}", address, rpc_error));
                }
            }

            // Check if we've achieved write quorum
            if successful_writes >= write_quorum {
                // Spawn remaining writes as background task for eventual consistency
                tokio::spawn(async move {
                    while let Some((bg_node, addr, res)) = write_futures.next().await {
                        match res {
                            Ok(()) => {
                                bg_node.record_success();
                                debug!("Background write to {} completed", addr);
                            }
                            Err(e) => {
                                bg_node.record_failure();
                                warn!("Background write to {} failed: {}", addr, e);
                            }
                        }
                    }
                });

                histogram!("datavg_put_blob_nanos", "result" => "success")
                    .record(start.elapsed().as_nanos() as f64);
                debug!(
                    "Write quorum achieved ({}/{}) for blob {}:{}",
                    successful_writes,
                    available_nodes.len(),
                    blob_guid.blob_id,
                    block_number
                );
                return Ok(());
            }
        }

        // Write quorum not achieved
        histogram!("datavg_put_blob_nanos", "result" => "quorum_failure")
            .record(start.elapsed().as_nanos() as f64);
        error!(
            "Write quorum failed ({}/{}). Errors: {:?}",
            successful_writes, self.quorum_config.w, errors
        );
        Err(DataVgError::QuorumFailure(format!(
            "Write quorum failed ({}/{}): {}",
            successful_writes,
            self.quorum_config.w,
            errors.join("; ")
        )))
    }

    pub async fn put_blob_vectored(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        chunks: Vec<Bytes>,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();
        let trace_id = *trace_id;
        let total_size: usize = chunks.iter().map(|c| c.len()).sum();
        histogram!("blob_size", "operation" => "put").record(total_size as f64);

        let selected_volume = self
            .volumes
            .iter()
            .find(|v| v.volume_id == blob_guid.volume_id)
            .ok_or_else(|| {
                DataVgError::InitializationError(format!(
                    "Volume {} not found in DataVgProxy",
                    blob_guid.volume_id
                ))
            })?;
        debug!(
            "Using volume {} for put_blob_vectored",
            selected_volume.volume_id
        );

        let rpc_timeout = self.rpc_timeout;
        let write_quorum = self.quorum_config.w as usize;

        // Compute checksum once for all replicas
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        for chunk in &chunks {
            hasher.update(chunk);
        }
        let body_checksum = hasher.digest();

        // Filter available nodes based on circuit breaker state
        let available_nodes: Vec<_> = selected_volume
            .bss_nodes
            .iter()
            .filter(|node| {
                let available = node.is_available();
                if !available {
                    counter!("circuit_breaker_skipped", "node" => node.address.clone(), "operation" => "put_vectored").increment(1);
                    debug!("Skipping node {} due to open circuit breaker", node.address);
                }
                available
            })
            .cloned()
            .collect();

        // Check if we have enough available nodes for quorum
        if available_nodes.len() < write_quorum {
            histogram!("datavg_put_blob_nanos", "result" => "insufficient_nodes")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::QuorumFailure(format!(
                "Insufficient available nodes ({}/{}) for vectored write quorum ({})",
                available_nodes.len(),
                selected_volume.bss_nodes.len(),
                write_quorum
            )));
        }

        let mut bss_node_indices: Vec<usize> = (0..available_nodes.len()).collect();
        bss_node_indices.shuffle(&mut rand::thread_rng());

        let mut write_futures = FuturesUnordered::new();
        for &index in &bss_node_indices {
            let bss_node = available_nodes[index].clone();
            write_futures.push(Self::put_blob_to_node_vectored(
                bss_node,
                blob_guid,
                block_number,
                chunks.clone(),
                body_checksum,
                rpc_timeout,
                trace_id,
            ));
        }

        let mut successful_writes = 0;
        let mut errors = Vec::with_capacity(available_nodes.len());

        while let Some((node, address, result)) = write_futures.next().await {
            match result {
                Ok(()) => {
                    node.record_success();
                    successful_writes += 1;
                    debug!("Successful vectored write to BSS node: {}", address);
                }
                Err(rpc_error) => {
                    node.record_failure();
                    warn!("RPC error writing to BSS node {}: {}", address, rpc_error);
                    errors.push(format!("{}: {}", address, rpc_error));
                }
            }

            if successful_writes >= write_quorum {
                tokio::spawn(async move {
                    while let Some((bg_node, addr, res)) = write_futures.next().await {
                        match res {
                            Ok(()) => {
                                bg_node.record_success();
                                debug!("Background vectored write to {} completed", addr);
                            }
                            Err(e) => {
                                bg_node.record_failure();
                                warn!("Background vectored write to {} failed: {}", addr, e);
                            }
                        }
                    }
                });

                histogram!("datavg_put_blob_nanos", "result" => "success")
                    .record(start.elapsed().as_nanos() as f64);
                debug!(
                    "Vectored write quorum achieved ({}/{}) for blob {}:{}",
                    successful_writes,
                    available_nodes.len(),
                    blob_guid.blob_id,
                    block_number
                );
                return Ok(());
            }
        }

        histogram!("datavg_put_blob_nanos", "result" => "quorum_failure")
            .record(start.elapsed().as_nanos() as f64);
        error!(
            "Failed to achieve write quorum ({}/{}) for blob {}:{}: {}",
            successful_writes,
            self.quorum_config.w,
            blob_guid.blob_id,
            block_number,
            errors.join("; ")
        );
        Err(DataVgError::QuorumFailure(format!(
            "Failed to achieve write quorum ({}/{}): {}",
            successful_writes,
            self.quorum_config.w,
            errors.join("; ")
        )))
    }

    async fn put_blob_to_node(
        bss_node: Arc<BssNode>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        body_checksum: u64,
        rpc_timeout: Duration,
        trace_id: TraceId,
    ) -> (Arc<BssNode>, String, Result<(), RpcError>) {
        let start_node = Instant::now();
        let address = bss_node.address.clone();

        let bss_client = bss_node.get_client();
        let result = bss_client
            .put_data_blob(
                blob_guid,
                block_number,
                body,
                body_checksum,
                Some(rpc_timeout),
                &trace_id,
                0,
            )
            .await;

        let _result_label = if result.is_ok() { "success" } else { "failure" };
        histogram!("datavg_put_blob_node_nanos", "bss_node" => address.clone(), "result" => _result_label)
            .record(start_node.elapsed().as_nanos() as f64);

        (bss_node, address, result)
    }

    async fn put_blob_to_node_vectored(
        bss_node: Arc<BssNode>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        chunks: Vec<Bytes>,
        body_checksum: u64,
        rpc_timeout: Duration,
        trace_id: TraceId,
    ) -> (Arc<BssNode>, String, Result<(), RpcError>) {
        let start_node = Instant::now();
        let address = bss_node.address.clone();

        let bss_client = bss_node.get_client();
        let result = bss_client
            .put_data_blob_vectored(
                blob_guid,
                block_number,
                chunks,
                body_checksum,
                Some(rpc_timeout),
                &trace_id,
                0,
            )
            .await;

        let _result_label = if result.is_ok() { "success" } else { "failure" };
        histogram!("datavg_put_blob_node_nanos", "bss_node" => address.clone(), "result" => _result_label)
            .record(start_node.elapsed().as_nanos() as f64);

        (bss_node, address, result)
    }

    /// Multi-BSS get_blob with quorum-based reads
    pub async fn get_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();

        let blob_id = blob_guid.blob_id;
        let volume_id = blob_guid.volume_id;

        tracing::debug!(%blob_id, volume_id, available_volumes=?self.volumes.iter().map(|v| v.volume_id).collect::<Vec<_>>(), "get_blob looking for volume");

        let volume = self
            .volumes
            .iter()
            .find(|v| v.volume_id == volume_id)
            .ok_or_else(|| {
                tracing::error!(%blob_id, volume_id, available_volumes=?self.volumes.iter().map(|v| v.volume_id).collect::<Vec<_>>(), "Volume not found in DataVgProxy for get_blob");
                DataVgError::InitializationError(format!("Volume {} not found", volume_id))
            })?;

        // Filter available nodes for fast path (only try nodes with closed circuit)
        let available_nodes: Vec<_> = volume
            .bss_nodes
            .iter()
            .filter(|node| {
                let available = node.is_available();
                if !available {
                    counter!("circuit_breaker_skipped", "node" => node.address.clone(), "operation" => "get_fast").increment(1);
                    debug!("Skipping node {} for fast path due to open circuit breaker", node.address);
                }
                available
            })
            .collect();

        // Fast path: try reading from a randomly selected available node
        if !available_nodes.is_empty() {
            let selected_node = *available_nodes.choose(&mut rand::thread_rng()).unwrap();
            debug!(
                "Attempting fast path read from BSS node: {}",
                selected_node.address
            );
            match self
                .get_blob_from_node_instance(
                    selected_node,
                    blob_guid,
                    block_number,
                    content_len,
                    trace_id,
                    true, // fast_path: no retries
                )
                .await
            {
                Ok(blob_data) => {
                    selected_node.record_success();
                    histogram!("datavg_get_blob_nanos", "result" => "fast_path_success")
                        .record(start.elapsed().as_nanos() as f64);
                    *body = blob_data;
                    return Ok(());
                }
                Err(e) => {
                    selected_node.record_failure();
                    warn!(
                        "Fast path read failed from {}: {}, falling back to quorum read",
                        selected_node.address, e
                    );
                }
            }
        }

        // Fallback: read from all available nodes using spawned tasks
        // Re-filter available nodes (state may have changed after fast path failure)
        let fallback_nodes: Vec<_> = volume
            .bss_nodes
            .iter()
            .filter(|node| {
                let available = node.is_available();
                if !available {
                    counter!("circuit_breaker_skipped", "node" => node.address.clone(), "operation" => "get_fallback").increment(1);
                }
                available
            })
            .cloned()
            .collect();

        if fallback_nodes.is_empty() {
            histogram!("datavg_get_blob_nanos", "result" => "no_available_nodes")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::QuorumFailure(
                "No available nodes for read (all circuits open)".to_string(),
            ));
        }

        debug!(
            "Performing quorum read from {} available nodes",
            fallback_nodes.len()
        );

        let _read_quorum = self.quorum_config.r as usize;

        // Create read futures for all available nodes
        let mut read_futures = FuturesUnordered::new();
        for bss_node in fallback_nodes {
            let proxy = self;
            let node_clone = bss_node.clone();
            read_futures.push(async move {
                let result = proxy
                    .get_blob_from_node_instance(
                        &node_clone,
                        blob_guid,
                        block_number,
                        content_len,
                        trace_id,
                        false, // not fast_path: allow retries
                    )
                    .await;
                (node_clone, result)
            });
        }

        let mut successful_reads = 0;
        let mut successful_blob_data = None;

        // Wait until we get a successful read (quorum of 1) or all fail
        while let Some((node, result)) = read_futures.next().await {
            match result {
                Ok(blob_data) => {
                    node.record_success();
                    successful_reads += 1;
                    debug!("Successful read from BSS node: {}", node.address);
                    if successful_blob_data.is_none() {
                        successful_blob_data = Some(blob_data);
                        // For reads, we can return as soon as we get one successful result
                        break;
                    }
                }
                Err(rpc_error) => {
                    node.record_failure();
                    warn!(
                        "RPC error reading from BSS node {}: {}",
                        node.address, rpc_error
                    );
                }
            }
        }

        if let Some(blob_data) = successful_blob_data {
            histogram!("datavg_get_blob_nanos", "result" => "success")
                .record(start.elapsed().as_nanos() as f64);
            debug!(
                "Read successful from {}/{} nodes for blob {}:{}",
                successful_reads,
                volume.bss_nodes.len(),
                blob_id,
                block_number
            );
            *body = blob_data;
            return Ok(());
        }

        // All reads failed
        histogram!("datavg_get_blob_nanos", "result" => "all_failed")
            .record(start.elapsed().as_nanos() as f64);
        error!(
            "All read attempts failed for blob {}:{}",
            blob_id, block_number
        );
        Err(DataVgError::QuorumFailure(
            "All read attempts failed".to_string(),
        ))
    }

    pub async fn delete_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();
        let trace_id = *trace_id;

        let volume = self
            .volumes
            .iter()
            .find(|v| v.volume_id == blob_guid.volume_id)
            .ok_or_else(|| {
                DataVgError::InitializationError(format!(
                    "Volume {} not found",
                    blob_guid.volume_id
                ))
            })?;

        let rpc_timeout = self.rpc_timeout;
        let write_quorum = self.quorum_config.w as usize;

        // Filter available nodes based on circuit breaker state
        let available_nodes: Vec<_> = volume
            .bss_nodes
            .iter()
            .filter(|node| {
                let available = node.is_available();
                if !available {
                    counter!("circuit_breaker_skipped", "node" => node.address.clone(), "operation" => "delete").increment(1);
                    debug!("Skipping node {} due to open circuit breaker", node.address);
                }
                available
            })
            .cloned()
            .collect();

        // Check if we have enough available nodes for quorum
        if available_nodes.len() < write_quorum {
            histogram!("datavg_delete_blob_nanos", "result" => "insufficient_nodes")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::QuorumFailure(format!(
                "Insufficient available nodes ({}/{}) for delete quorum ({})",
                available_nodes.len(),
                volume.bss_nodes.len(),
                write_quorum
            )));
        }

        let mut delete_futures = FuturesUnordered::new();
        for bss_node in &available_nodes {
            delete_futures.push(Self::delete_blob_from_node(
                bss_node.clone(),
                blob_guid,
                block_number,
                rpc_timeout,
                trace_id,
            ));
        }

        let mut successful_deletes = 0;
        let mut errors = Vec::with_capacity(available_nodes.len());

        while let Some((node, address, result)) = delete_futures.next().await {
            match result {
                Ok(()) => {
                    node.record_success();
                    successful_deletes += 1;
                    debug!("Successful delete from BSS node: {}", address);
                }
                Err(rpc_error) => {
                    node.record_failure();
                    warn!(
                        "RPC error deleting from BSS node {}: {}",
                        address, rpc_error
                    );
                    errors.push(format!("{}: {}", address, rpc_error));
                }
            }

            if successful_deletes >= write_quorum {
                // Spawn remaining deletes as background task for eventual consistency
                tokio::spawn(async move {
                    while let Some((bg_node, addr, res)) = delete_futures.next().await {
                        match res {
                            Ok(()) => {
                                bg_node.record_success();
                                debug!("Background delete to {} completed", addr);
                            }
                            Err(e) => {
                                bg_node.record_failure();
                                warn!("Background delete to {} failed: {}", addr, e);
                            }
                        }
                    }
                });

                histogram!("datavg_delete_blob_nanos", "result" => "success")
                    .record(start.elapsed().as_nanos() as f64);
                debug!(
                    "Delete quorum achieved ({}/{}) for blob {}:{}",
                    successful_deletes,
                    available_nodes.len(),
                    blob_guid.blob_id,
                    block_number
                );
                return Ok(());
            }
        }

        histogram!("datavg_delete_blob_nanos", "result" => "quorum_failure")
            .record(start.elapsed().as_nanos() as f64);
        error!(
            "Delete quorum failed ({}/{}). Errors: {:?}",
            successful_deletes, self.quorum_config.w, errors
        );
        Err(DataVgError::QuorumFailure(format!(
            "Delete quorum failed ({}/{}): {}",
            successful_deletes,
            self.quorum_config.w,
            errors.join("; ")
        )))
    }
}
