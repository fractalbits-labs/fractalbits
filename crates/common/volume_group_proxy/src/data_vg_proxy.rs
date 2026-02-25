use crate::DataVgError;
use bytes::Bytes;
use data_types::{DataBlobGuid, DataVgInfo, EcVolume, QuorumConfig, TraceId};
use futures::stream::{FuturesUnordered, StreamExt};
use metrics_wrapper::{counter, histogram};
use rand::seq::SliceRandom;
use reed_solomon_simd::{decode as rs_decode, encode as rs_encode};
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

struct EcVolumeWithNodes {
    volume_id: u16,
    data_shards: u32,
    parity_shards: u32,
    bss_nodes: Vec<Arc<BssNode>>,
}

pub struct DataVgProxy {
    volumes: Vec<VolumeWithNodes>,
    ec_volumes: Vec<EcVolumeWithNodes>,
    round_robin_counter: AtomicU64,
    quorum_config: Option<QuorumConfig>,
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
            "Initializing DataVgProxy with {} volumes, {} EC volumes, circuit breaker config: {:?}",
            data_vg_info.volumes.len(),
            data_vg_info.ec_volumes.len(),
            cb_config
        );

        // Quorum config is required only when replicated volumes are present
        let quorum_config = if !data_vg_info.volumes.is_empty() {
            Some(data_vg_info.quorum.ok_or_else(|| {
                DataVgError::InitializationError(
                    "QuorumConfig is required when replicated volumes are present".to_string(),
                )
            })?)
        } else {
            data_vg_info.quorum
        };

        if data_vg_info.volumes.is_empty() && data_vg_info.ec_volumes.is_empty() {
            return Err(DataVgError::InitializationError(
                "No volumes (replicated or EC) configured".to_string(),
            ));
        }

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

        let mut ec_volumes_with_nodes = Vec::new();

        for ec_vol in data_vg_info.ec_volumes {
            if !EcVolume::is_ec_volume_id(ec_vol.volume_id) {
                return Err(DataVgError::InitializationError(format!(
                    "EC volume {} must be in 0x8000..0xFFFE range",
                    ec_vol.volume_id
                )));
            }
            if ec_vol.data_shards == 0 {
                return Err(DataVgError::InitializationError(format!(
                    "EC volume {} has invalid data_shards=0",
                    ec_vol.volume_id
                )));
            }
            if ec_vol.parity_shards == 0 {
                return Err(DataVgError::InitializationError(format!(
                    "EC volume {} has invalid parity_shards=0",
                    ec_vol.volume_id
                )));
            }

            let total_shards = ec_vol.data_shards + ec_vol.parity_shards;
            if ec_vol.bss_nodes.len() != total_shards as usize {
                return Err(DataVgError::InitializationError(format!(
                    "EC volume {} has {} nodes but expected k+m={}",
                    ec_vol.volume_id,
                    ec_vol.bss_nodes.len(),
                    total_shards
                )));
            }

            let mut bss_nodes = Vec::new();
            for bss_node in ec_vol.bss_nodes {
                let address = format!("{}:{}", bss_node.ip, bss_node.port);
                debug!(
                    "Creating EC BSS node for node {}: {}",
                    bss_node.node_id, address
                );
                bss_nodes.push(Arc::new(BssNode::new(
                    address,
                    cb_config.clone(),
                    rpc_connection_timeout,
                )));
            }

            info!(
                "EC volume {} initialized: k={}, m={}, {} nodes",
                ec_vol.volume_id,
                ec_vol.data_shards,
                ec_vol.parity_shards,
                bss_nodes.len()
            );

            ec_volumes_with_nodes.push(EcVolumeWithNodes {
                volume_id: ec_vol.volume_id,
                data_shards: ec_vol.data_shards,
                parity_shards: ec_vol.parity_shards,
                bss_nodes,
            });
        }

        debug!(
            "DataVgProxy initialized successfully with {} volumes, {} EC volumes",
            volumes_with_nodes.len(),
            ec_volumes_with_nodes.len()
        );

        Ok(Self {
            volumes: volumes_with_nodes,
            ec_volumes: ec_volumes_with_nodes,
            round_robin_counter: AtomicU64::new(0),
            quorum_config,
            rpc_timeout: rpc_request_timeout,
        })
    }

    pub fn select_volume_for_blob_with_preference(&self, prefer_ec: bool) -> u16 {
        let counter = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) as usize;

        if prefer_ec && !self.ec_volumes.is_empty() {
            let volume_index = counter % self.ec_volumes.len();
            return self.ec_volumes[volume_index].volume_id;
        }

        if !self.volumes.is_empty() {
            let volume_index = counter % self.volumes.len();
            return self.volumes[volume_index].volume_id;
        }

        let volume_index = counter % self.ec_volumes.len();
        self.ec_volumes[volume_index].volume_id
    }

    pub fn select_volume_for_blob(&self) -> u16 {
        self.select_volume_for_blob_with_preference(false)
    }

    fn find_ec_volume(&self, volume_id: u16) -> Option<&EcVolumeWithNodes> {
        self.ec_volumes.iter().find(|v| v.volume_id == volume_id)
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
        self.create_data_blob_guid_with_preference(false)
    }

    /// Create a new data blob GUID and optionally prefer EC volume selection.
    pub fn create_data_blob_guid_with_preference(&self, prefer_ec: bool) -> DataBlobGuid {
        let blob_id = Uuid::now_v7();
        let volume_id = self.select_volume_for_blob_with_preference(prefer_ec);
        DataBlobGuid { blob_id, volume_id }
    }

    /// Multi-BSS put_blob with quorum-based replication or EC encoding
    pub async fn put_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        if EcVolume::is_ec_volume_id(blob_guid.volume_id) {
            return self
                .put_blob_ec(blob_guid, block_number, body, trace_id)
                .await;
        }

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
        let write_quorum = self.quorum_config.as_ref().unwrap().w as usize;

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
            successful_writes,
            self.quorum_config.as_ref().unwrap().w,
            errors
        );
        Err(DataVgError::QuorumFailure(format!(
            "Write quorum failed ({}/{}): {}",
            successful_writes,
            self.quorum_config.as_ref().unwrap().w,
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
        if EcVolume::is_ec_volume_id(blob_guid.volume_id) {
            // Concatenate chunks for EC encoding
            let total_size: usize = chunks.iter().map(|c| c.len()).sum();
            let mut combined = Vec::with_capacity(total_size);
            for chunk in &chunks {
                combined.extend_from_slice(chunk);
            }
            return self
                .put_blob_ec(blob_guid, block_number, Bytes::from(combined), trace_id)
                .await;
        }

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
        let write_quorum = self.quorum_config.as_ref().unwrap().w as usize;

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
            self.quorum_config.as_ref().unwrap().w,
            blob_guid.blob_id,
            block_number,
            errors.join("; ")
        );
        Err(DataVgError::QuorumFailure(format!(
            "Failed to achieve write quorum ({}/{}): {}",
            successful_writes,
            self.quorum_config.as_ref().unwrap().w,
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

    /// Multi-BSS get_blob with quorum-based reads or EC decoding
    pub async fn get_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        if EcVolume::is_ec_volume_id(blob_guid.volume_id) {
            return self
                .get_blob_ec(blob_guid, block_number, content_len, body, trace_id)
                .await;
        }

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

        let _read_quorum = self.quorum_config.as_ref().unwrap().r as usize;

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
        if EcVolume::is_ec_volume_id(blob_guid.volume_id) {
            return self.delete_blob_ec(blob_guid, block_number, trace_id).await;
        }

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
        let write_quorum = self.quorum_config.as_ref().unwrap().w as usize;

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
            successful_deletes,
            self.quorum_config.as_ref().unwrap().w,
            errors
        );
        Err(DataVgError::QuorumFailure(format!(
            "Delete quorum failed ({}/{}): {}",
            successful_deletes,
            self.quorum_config.as_ref().unwrap().w,
            errors.join("; ")
        )))
    }

    // ---- EC (Erasure-Coded) blob operations ----

    /// Compute shard-to-node rotation for load balancing.
    /// rotation = crc32(blob_id bytes) % total_shards
    fn ec_rotation(blob_id: &Uuid, total_shards: u32) -> usize {
        let hash = crc32fast::hash(blob_id.as_bytes());
        (hash % total_shards) as usize
    }

    fn ec_padded_len(content_len: usize, data_shards: usize) -> usize {
        let stripe_size = data_shards * 2;
        if content_len.is_multiple_of(stripe_size) {
            content_len
        } else {
            content_len + (stripe_size - content_len % stripe_size)
        }
    }

    /// EC put: RS-encode block into k+m shards, send to nodes with W=k+1 quorum
    async fn put_blob_ec(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();
        let trace_id = *trace_id;
        histogram!("blob_size", "operation" => "put_ec").record(body.len() as f64);

        // Empty body: nothing to encode or store
        if body.is_empty() {
            histogram!("datavg_put_blob_nanos", "result" => "ec_empty")
                .record(start.elapsed().as_nanos() as f64);
            return Ok(());
        }

        let ec_vol = self.find_ec_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!("EC volume {} not found", blob_guid.volume_id))
        })?;

        let k = ec_vol.data_shards as usize;
        let m = ec_vol.parity_shards as usize;
        let total = k + m;
        let write_quorum = k + 1; // W = k + 1

        // Pad body to a full RS stripe with even shard size.
        let original_len = body.len();
        let padded_len = Self::ec_padded_len(original_len, k);
        let shard_size = padded_len / k;

        let mut padded = body.to_vec();
        padded.resize(padded_len, 0u8);

        // Split into k data shards
        let mut shards: Vec<Vec<u8>> = Vec::with_capacity(total);
        for i in 0..k {
            shards.push(padded[i * shard_size..(i + 1) * shard_size].to_vec());
        }
        let parity_shards = rs_encode(k, m, &shards)
            .map_err(|e| DataVgError::Internal(format!("RS encode failed: {}", e)))?;
        shards.extend(parity_shards);

        // Compute rotation for shard-to-node mapping
        let rotation = Self::ec_rotation(&blob_guid.blob_id, total as u32);

        let rpc_timeout = self.rpc_timeout;

        // Filter available nodes
        let available_mask: Vec<bool> = ec_vol
            .bss_nodes
            .iter()
            .map(|node| {
                let available = node.is_available();
                if !available {
                    counter!("circuit_breaker_skipped", "node" => node.address.clone(), "operation" => "put_ec")
                        .increment(1);
                }
                available
            })
            .collect();

        let available_count = available_mask.iter().filter(|&&a| a).count();
        if available_count < write_quorum {
            histogram!("datavg_put_blob_nanos", "result" => "ec_insufficient_nodes")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::QuorumFailure(format!(
                "EC put: insufficient available nodes ({}/{}) for write quorum ({})",
                available_count, total, write_quorum
            )));
        }

        // Send shard[i] to node[(i + rotation) % total]
        let mut write_futures = FuturesUnordered::new();
        for (shard_idx, shard) in shards.iter().enumerate() {
            let node_idx = (shard_idx + rotation) % total;
            if !available_mask[node_idx] {
                continue;
            }
            let node = ec_vol.bss_nodes[node_idx].clone();
            let shard_data = Bytes::from(shard.clone());
            let checksum = xxhash_rust::xxh3::xxh3_64(&shard_data);
            write_futures.push(Self::put_blob_to_node(
                node,
                blob_guid,
                block_number,
                shard_data,
                checksum,
                rpc_timeout,
                trace_id,
            ));
        }

        let mut successful_writes = 0;
        let mut errors = Vec::new();

        while let Some((node, address, result)) = write_futures.next().await {
            match result {
                Ok(()) => {
                    node.record_success();
                    successful_writes += 1;
                    debug!("EC shard write success to {}", address);
                }
                Err(rpc_error) => {
                    node.record_failure();
                    warn!("EC shard write failed to {}: {}", address, rpc_error);
                    errors.push(format!("{}: {}", address, rpc_error));
                }
            }

            if successful_writes >= write_quorum {
                // Background remaining writes
                tokio::spawn(async move {
                    while let Some((bg_node, addr, res)) = write_futures.next().await {
                        match res {
                            Ok(()) => {
                                bg_node.record_success();
                                debug!("EC background write to {} completed", addr);
                            }
                            Err(e) => {
                                bg_node.record_failure();
                                warn!("EC background write to {} failed: {}", addr, e);
                            }
                        }
                    }
                });

                histogram!("datavg_put_blob_nanos", "result" => "ec_success")
                    .record(start.elapsed().as_nanos() as f64);
                debug!(
                    "EC write quorum achieved ({}/{}) for blob {}:{}, original_len={}",
                    successful_writes, total, blob_guid.blob_id, block_number, original_len
                );
                return Ok(());
            }
        }

        histogram!("datavg_put_blob_nanos", "result" => "ec_quorum_failure")
            .record(start.elapsed().as_nanos() as f64);
        error!(
            "EC write quorum failed ({}/{}). Errors: {:?}",
            successful_writes, write_quorum, errors
        );
        Err(DataVgError::QuorumFailure(format!(
            "EC write quorum failed ({}/{}): {}",
            successful_writes,
            write_quorum,
            errors.join("; ")
        )))
    }

    /// EC get: fetch k data shards in parallel, RS-decode if degraded
    async fn get_blob_ec(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();

        // Empty body: nothing was stored, return empty
        if content_len == 0 {
            *body = Bytes::new();
            histogram!("datavg_get_blob_nanos", "result" => "ec_empty")
                .record(start.elapsed().as_nanos() as f64);
            return Ok(());
        }

        let ec_vol = self.find_ec_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!("EC volume {} not found", blob_guid.volume_id))
        })?;

        let k = ec_vol.data_shards as usize;
        let m = ec_vol.parity_shards as usize;
        let total = k + m;

        let rotation = Self::ec_rotation(&blob_guid.blob_id, total as u32);

        // Compute shard size, matching put_blob_ec padding.
        let padded_len = Self::ec_padded_len(content_len, k);
        let shard_size = padded_len / k;

        // Fetch the k data shards (indices 0..k) from their rotated nodes
        let mut shard_results: Vec<Option<Vec<u8>>> = vec![None; total];
        let mut data_shards_received = 0;
        let mut fetch_futures = FuturesUnordered::new();
        for shard_idx in 0..k {
            let node_idx = (shard_idx + rotation) % total;
            let node = &ec_vol.bss_nodes[node_idx];
            if !node.is_available() {
                continue;
            }
            let si = shard_idx;
            let ni = node_idx;
            fetch_futures.push(async move {
                let result = self
                    .get_blob_from_node_instance(
                        &ec_vol.bss_nodes[ni],
                        blob_guid,
                        block_number,
                        shard_size,
                        trace_id,
                        true, // fast path
                    )
                    .await;
                (si, ni, result)
            });
        }

        while let Some((shard_idx, node_idx, result)) = fetch_futures.next().await {
            match result {
                Ok(data) => {
                    ec_vol.bss_nodes[node_idx].record_success();
                    shard_results[shard_idx] = Some(data.to_vec());
                    data_shards_received += 1;
                }
                Err(e) => {
                    ec_vol.bss_nodes[node_idx].record_failure();
                    warn!(
                        "EC data shard {} fetch failed from {}: {}",
                        shard_idx, ec_vol.bss_nodes[node_idx].address, e
                    );
                }
            }
        }

        if data_shards_received == k {
            // All data shards received, concatenate directly (no RS decode needed)
            let mut result_data = Vec::new();
            for shard in shard_results.iter().take(k) {
                result_data.extend_from_slice(shard.as_ref().unwrap());
            }
            result_data.truncate(content_len);
            *body = Bytes::from(result_data);

            histogram!("datavg_get_blob_nanos", "result" => "ec_fast_success")
                .record(start.elapsed().as_nanos() as f64);
            return Ok(());
        }

        // Degraded read: need parity shards to reconstruct
        let missing_count = k - data_shards_received;
        if missing_count > m {
            histogram!("datavg_get_blob_nanos", "result" => "ec_too_many_failures")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::QuorumFailure(format!(
                "EC read: {} data shards failed, exceeds parity count {}",
                missing_count, m
            )));
        }

        // Fetch all parity shards that are currently available.
        // This avoids false negatives when one parity node fails but another can satisfy k-of-(k+m).
        let mut parity_futures = FuturesUnordered::new();
        for parity_idx in 0..m {
            let shard_idx = k + parity_idx;
            let node_idx = (shard_idx + rotation) % total;
            let node = &ec_vol.bss_nodes[node_idx];
            if !node.is_available() {
                continue;
            }
            let si = shard_idx;
            let ni = node_idx;
            parity_futures.push(async move {
                let result = self
                    .get_blob_from_node_instance(
                        &ec_vol.bss_nodes[ni],
                        blob_guid,
                        block_number,
                        shard_size,
                        trace_id,
                        false, // allow retries for parity
                    )
                    .await;
                (si, ni, result)
            });
        }

        while let Some((shard_idx, node_idx, result)) = parity_futures.next().await {
            match result {
                Ok(data) => {
                    ec_vol.bss_nodes[node_idx].record_success();
                    shard_results[shard_idx] = Some(data.to_vec());
                }
                Err(e) => {
                    ec_vol.bss_nodes[node_idx].record_failure();
                    warn!(
                        "EC parity shard {} fetch failed from {}: {}",
                        shard_idx, ec_vol.bss_nodes[node_idx].address, e
                    );
                }
            }
        }

        let total_shards_received = shard_results.iter().filter(|s| s.is_some()).count();
        if total_shards_received < k {
            histogram!("datavg_get_blob_nanos", "result" => "ec_quorum_failure")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::QuorumFailure(format!(
                "EC read: only {} shards available, need {}",
                total_shards_received, k
            )));
        }

        // RS reconstruct
        let shard_size = shard_results
            .iter()
            .find_map(|s| s.as_ref().map(|d| d.len()))
            .ok_or_else(|| {
                DataVgError::Internal("EC read: no shards received at all".to_string())
            })?;

        let shards_for_rs: Vec<Option<Vec<u8>>> = shard_results
            .into_iter()
            .map(|s| s.filter(|d| d.len() == shard_size))
            .collect();
        let original_shards: Vec<_> = shards_for_rs
            .iter()
            .take(k)
            .enumerate()
            .filter_map(|(index, shard)| shard.as_deref().map(|data| (index, data)))
            .collect();
        let recovery_shards: Vec<_> = shards_for_rs
            .iter()
            .skip(k)
            .enumerate()
            .filter_map(|(index, shard)| shard.as_deref().map(|data| (index, data)))
            .collect();
        let restored_original = rs_decode(k, m, original_shards, recovery_shards)
            .map_err(|e| DataVgError::Internal(format!("RS reconstruct failed: {}", e)))?;

        // Concatenate data shards
        let mut result_data = Vec::with_capacity(k * shard_size);
        for (index, shard) in shards_for_rs.iter().take(k).enumerate() {
            if let Some(shard) = shard {
                result_data.extend_from_slice(shard);
            } else if let Some(restored) = restored_original.get(&index) {
                result_data.extend_from_slice(restored);
            } else {
                return Err(DataVgError::Internal(format!(
                    "RS reconstruct missing shard {}",
                    index
                )));
            }
        }
        result_data.truncate(content_len);
        *body = Bytes::from(result_data);

        histogram!("datavg_get_blob_nanos", "result" => "ec_degraded_success")
            .record(start.elapsed().as_nanos() as f64);
        Ok(())
    }

    /// EC delete: send delete to all k+m nodes, wait for k+1 acks
    async fn delete_blob_ec(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();
        let trace_id = *trace_id;

        let ec_vol = self.find_ec_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!("EC volume {} not found", blob_guid.volume_id))
        })?;

        let k = ec_vol.data_shards as usize;
        let m = ec_vol.parity_shards as usize;
        let total = k + m;
        let write_quorum = k + 1;

        let rpc_timeout = self.rpc_timeout;

        // Filter available nodes
        let available_nodes: Vec<_> = ec_vol
            .bss_nodes
            .iter()
            .filter(|node| {
                let available = node.is_available();
                if !available {
                    counter!("circuit_breaker_skipped", "node" => node.address.clone(), "operation" => "delete_ec")
                        .increment(1);
                }
                available
            })
            .cloned()
            .collect();

        if available_nodes.len() < write_quorum {
            histogram!("datavg_delete_blob_nanos", "result" => "ec_insufficient_nodes")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::QuorumFailure(format!(
                "EC delete: insufficient available nodes ({}/{}) for quorum ({})",
                available_nodes.len(),
                total,
                write_quorum
            )));
        }

        // Send delete to all available nodes (each node has one shard for this blob)
        let mut delete_futures = FuturesUnordered::new();
        for node in &available_nodes {
            delete_futures.push(Self::delete_blob_from_node(
                node.clone(),
                blob_guid,
                block_number,
                rpc_timeout,
                trace_id,
            ));
        }

        let mut successful_deletes = 0;
        let mut errors = Vec::new();

        while let Some((node, address, result)) = delete_futures.next().await {
            match result {
                Ok(()) => {
                    node.record_success();
                    successful_deletes += 1;
                    debug!("EC delete success from {}", address);
                }
                Err(rpc_error) => {
                    node.record_failure();
                    warn!("EC delete failed from {}: {}", address, rpc_error);
                    errors.push(format!("{}: {}", address, rpc_error));
                }
            }

            if successful_deletes >= write_quorum {
                tokio::spawn(async move {
                    while let Some((bg_node, addr, res)) = delete_futures.next().await {
                        match res {
                            Ok(()) => {
                                bg_node.record_success();
                                debug!("EC background delete to {} completed", addr);
                            }
                            Err(e) => {
                                bg_node.record_failure();
                                warn!("EC background delete to {} failed: {}", addr, e);
                            }
                        }
                    }
                });

                histogram!("datavg_delete_blob_nanos", "result" => "ec_success")
                    .record(start.elapsed().as_nanos() as f64);
                debug!(
                    "EC delete quorum achieved ({}/{}) for blob {}:{}",
                    successful_deletes, total, blob_guid.blob_id, block_number
                );
                return Ok(());
            }
        }

        histogram!("datavg_delete_blob_nanos", "result" => "ec_quorum_failure")
            .record(start.elapsed().as_nanos() as f64);
        error!(
            "EC delete quorum failed ({}/{}). Errors: {:?}",
            successful_deletes, write_quorum, errors
        );
        Err(DataVgError::QuorumFailure(format!(
            "EC delete quorum failed ({}/{}): {}",
            successful_deletes,
            write_quorum,
            errors.join("; ")
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::EcVolume;

    #[test]
    fn ec_volume_id_range() {
        assert!(!EcVolume::is_ec_volume_id(0));
        assert!(!EcVolume::is_ec_volume_id(1));
        assert!(!EcVolume::is_ec_volume_id(0x7FFF));
        assert!(EcVolume::is_ec_volume_id(0x8000));
        assert!(EcVolume::is_ec_volume_id(0x8001));
        assert!(EcVolume::is_ec_volume_id(0xFFFE));
        assert!(!EcVolume::is_ec_volume_id(0xFFFF));
    }

    #[test]
    fn ec_rotation_deterministic() {
        let blob_id = Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef").unwrap();
        let total = 6u32;
        let r1 = DataVgProxy::ec_rotation(&blob_id, total);
        let r2 = DataVgProxy::ec_rotation(&blob_id, total);
        assert_eq!(r1, r2);
        assert!(r1 < total as usize);
    }

    #[test]
    fn ec_rotation_varies_by_blob_id() {
        let total = 6u32;
        let mut rotations = std::collections::HashSet::new();
        // Generate many blob IDs and check we get variety in rotations
        for i in 0..100u128 {
            let blob_id = Uuid::from_u128(i);
            let r = DataVgProxy::ec_rotation(&blob_id, total);
            assert!(r < total as usize);
            rotations.insert(r);
        }
        // With 100 random-ish UUIDs across 6 slots, we should hit at least 3
        assert!(rotations.len() >= 3, "rotations: {:?}", rotations);
    }

    #[test]
    fn rs_encode_decode_roundtrip() {
        let k = 4;
        let m = 2;

        // Create test data: 1024 bytes (divisible by k=4)
        let original: Vec<u8> = (0..1024u32).map(|i| (i % 256) as u8).collect();
        let shard_size = original.len() / k;
        let mut original_shards: Vec<Vec<u8>> = Vec::with_capacity(k);
        for i in 0..k {
            original_shards.push(original[i * shard_size..(i + 1) * shard_size].to_vec());
        }
        let recovery_shards = rs_encode(k, m, &original_shards).unwrap();

        assert_eq!(recovery_shards.len(), m);

        // Reconstruct with data shard 1 missing
        let restored = rs_decode(
            k,
            m,
            original_shards
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != 1)
                .map(|(index, shard)| (index, shard.as_slice())),
            [(0, recovery_shards[0].as_slice())],
        )
        .unwrap();

        // Verify data shards match original
        let mut reconstructed = Vec::new();
        for (index, shard) in original_shards.iter().enumerate() {
            if index == 1 {
                reconstructed.extend_from_slice(&restored[&index]);
            } else {
                reconstructed.extend_from_slice(shard);
            }
        }
        assert_eq!(reconstructed, original);
    }

    #[test]
    fn rs_encode_decode_with_padding() {
        let k = 4;
        let m = 2;
        let original_len = 99;
        let original: Vec<u8> = (0..original_len).map(|i| (i * 7 % 256) as u8).collect();

        let padded_len = DataVgProxy::ec_padded_len(original_len, k);
        assert_eq!(padded_len, 104);
        let shard_size = padded_len / k;
        assert_eq!(shard_size, 26);

        let mut padded = original.clone();
        padded.resize(padded_len, 0u8);

        let mut data_shards: Vec<Vec<u8>> = Vec::with_capacity(k);
        for i in 0..k {
            data_shards.push(padded[i * shard_size..(i + 1) * shard_size].to_vec());
        }

        let recovery_shards = rs_encode(k, m, &data_shards).unwrap();
        assert_eq!(recovery_shards.len(), m);

        // Reconstruct with all data shards (fast path)
        let mut result = Vec::new();
        for shard in &data_shards {
            result.extend_from_slice(shard);
        }
        result.truncate(original_len);
        assert_eq!(result, original);
    }

    #[test]
    fn rs_max_failures_respected() {
        let k = 4;
        let m = 2;

        let shard_size = 64;
        let data_shards: Vec<Vec<u8>> = (0..k)
            .map(|i| vec![(i as u8).wrapping_mul(37); shard_size])
            .collect();
        let recovery_shards = rs_encode(k, m, &data_shards).unwrap();

        // Can recover from m=2 failures
        let recovered = rs_decode(
            k,
            m,
            data_shards
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != 0 && *index != 3)
                .map(|(index, shard)| (index, shard.as_slice())),
            recovery_shards
                .iter()
                .enumerate()
                .map(|(index, shard)| (index, shard.as_slice())),
        );
        assert!(recovered.is_ok());

        // Cannot recover from m+1=3 failures
        let recovered = rs_decode(
            k,
            m,
            data_shards
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != 0 && *index != 2 && *index != 3)
                .map(|(index, shard)| (index, shard.as_slice())),
            recovery_shards
                .iter()
                .enumerate()
                .map(|(index, shard)| (index, shard.as_slice())),
        );
        assert!(recovered.is_err());
    }

    #[test]
    fn shard_rotation_covers_all_nodes() {
        // Verify that with rotation, shard i goes to node (i + rotation) % total
        let total = 6;
        for rotation in 0..total {
            let mut nodes_used: Vec<usize> = Vec::new();
            for shard_idx in 0..total {
                let node_idx = (shard_idx + rotation) % total;
                nodes_used.push(node_idx);
            }
            nodes_used.sort();
            assert_eq!(nodes_used, vec![0, 1, 2, 3, 4, 5]);
        }
    }

    #[test]
    fn parse_ec_config_json() {
        let json = r#"{
            "volumes": [],
            "ec_volumes": [{
                "volume_id": 32768,
                "data_shards": 4,
                "parity_shards": 2,
                "bss_nodes": [
                    {"node_id":"bss0","ip":"127.0.0.1","port":8088},
                    {"node_id":"bss1","ip":"127.0.0.1","port":8089},
                    {"node_id":"bss2","ip":"127.0.0.1","port":8090},
                    {"node_id":"bss3","ip":"127.0.0.1","port":8091},
                    {"node_id":"bss4","ip":"127.0.0.1","port":8092},
                    {"node_id":"bss5","ip":"127.0.0.1","port":8093}
                ]
            }]
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        assert!(info.volumes.is_empty());
        assert!(info.quorum.is_none());
        assert_eq!(info.ec_volumes.len(), 1);

        let ec = &info.ec_volumes[0];
        assert_eq!(ec.volume_id, 0x8000);
        assert_eq!(ec.data_shards, 4);
        assert_eq!(ec.parity_shards, 2);
        assert_eq!(ec.bss_nodes.len(), 6);
    }

    #[test]
    fn parse_backward_compatible_no_ec_volumes() {
        let json = r#"{
            "volumes": [{"volume_id":1,"bss_nodes":[{"node_id":"bss0","ip":"127.0.0.1","port":8088}]}],
            "quorum": {"n":1,"r":1,"w":1}
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.volumes.len(), 1);
        assert!(info.ec_volumes.is_empty());
        assert!(info.quorum.is_some());
    }

    #[test]
    fn datavgproxy_init_ec_only() {
        let json = r#"{
            "volumes": [],
            "ec_volumes": [{
                "volume_id": 32768,
                "data_shards": 4,
                "parity_shards": 2,
                "bss_nodes": [
                    {"node_id":"bss0","ip":"127.0.0.1","port":18088},
                    {"node_id":"bss1","ip":"127.0.0.1","port":18089},
                    {"node_id":"bss2","ip":"127.0.0.1","port":18090},
                    {"node_id":"bss3","ip":"127.0.0.1","port":18091},
                    {"node_id":"bss4","ip":"127.0.0.1","port":18092},
                    {"node_id":"bss5","ip":"127.0.0.1","port":18093}
                ]
            }]
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        let proxy = DataVgProxy::new(info, Duration::from_secs(5), Duration::from_secs(5)).unwrap();

        // Should select EC volume
        let guid = proxy.create_data_blob_guid();
        assert_eq!(guid.volume_id, 0x8000);
        assert!(EcVolume::is_ec_volume_id(guid.volume_id));
    }

    #[test]
    fn datavgproxy_init_ec_invalid_node_count() {
        let json = r#"{
            "volumes": [],
            "ec_volumes": [{
                "volume_id": 32768,
                "data_shards": 4,
                "parity_shards": 2,
                "bss_nodes": [
                    {"node_id":"bss0","ip":"127.0.0.1","port":18088},
                    {"node_id":"bss1","ip":"127.0.0.1","port":18089}
                ]
            }]
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        let result = DataVgProxy::new(info, Duration::from_secs(5), Duration::from_secs(5));
        assert!(result.is_err());
        let err = result.err().unwrap().to_string();
        assert!(err.contains("2 nodes but expected k+m=6"), "err: {}", err);
    }

    #[test]
    fn datavgproxy_init_ec_invalid_volume_id_range() {
        let json = r#"{
            "volumes": [],
            "ec_volumes": [{
                "volume_id": 65535,
                "data_shards": 4,
                "parity_shards": 2,
                "bss_nodes": [
                    {"node_id":"bss0","ip":"127.0.0.1","port":18088},
                    {"node_id":"bss1","ip":"127.0.0.1","port":18089},
                    {"node_id":"bss2","ip":"127.0.0.1","port":18090},
                    {"node_id":"bss3","ip":"127.0.0.1","port":18091},
                    {"node_id":"bss4","ip":"127.0.0.1","port":18092},
                    {"node_id":"bss5","ip":"127.0.0.1","port":18093}
                ]
            }]
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        let result = DataVgProxy::new(info, Duration::from_secs(5), Duration::from_secs(5));
        assert!(result.is_err());
        let err = result.err().unwrap().to_string();
        assert!(err.contains("0x8000..0xFFFE"), "err: {}", err);
    }

    #[test]
    fn datavgproxy_init_ec_zero_data_shards_fails() {
        let json = r#"{
            "volumes": [],
            "ec_volumes": [{
                "volume_id": 32768,
                "data_shards": 0,
                "parity_shards": 2,
                "bss_nodes": [
                    {"node_id":"bss0","ip":"127.0.0.1","port":18088},
                    {"node_id":"bss1","ip":"127.0.0.1","port":18089}
                ]
            }]
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        let result = DataVgProxy::new(info, Duration::from_secs(5), Duration::from_secs(5));
        assert!(result.is_err());
        let err = result.err().unwrap().to_string();
        assert!(err.contains("data_shards=0"), "err: {}", err);
    }

    #[test]
    fn datavgproxy_init_ec_zero_parity_shards_fails() {
        let json = r#"{
            "volumes": [],
            "ec_volumes": [{
                "volume_id": 32768,
                "data_shards": 4,
                "parity_shards": 0,
                "bss_nodes": [
                    {"node_id":"bss0","ip":"127.0.0.1","port":18088},
                    {"node_id":"bss1","ip":"127.0.0.1","port":18089},
                    {"node_id":"bss2","ip":"127.0.0.1","port":18090},
                    {"node_id":"bss3","ip":"127.0.0.1","port":18091}
                ]
            }]
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        let result = DataVgProxy::new(info, Duration::from_secs(5), Duration::from_secs(5));
        assert!(result.is_err());
        let err = result.err().unwrap().to_string();
        assert!(err.contains("parity_shards=0"), "err: {}", err);
    }

    #[test]
    fn datavgproxy_init_no_volumes_fails() {
        let json = r#"{"volumes": [], "ec_volumes": []}"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        let result = DataVgProxy::new(info, Duration::from_secs(5), Duration::from_secs(5));
        assert!(result.is_err());
        let err = result.err().unwrap().to_string();
        assert!(err.contains("No volumes"), "err: {}", err);
    }

    #[test]
    fn datavgproxy_init_replicated_requires_quorum() {
        let json = r#"{
            "volumes": [{"volume_id":1,"bss_nodes":[{"node_id":"bss0","ip":"127.0.0.1","port":18088}]}]
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        let result = DataVgProxy::new(info, Duration::from_secs(5), Duration::from_secs(5));
        assert!(result.is_err());
        let err = result.err().unwrap().to_string();
        assert!(err.contains("QuorumConfig is required"), "err: {}", err);
    }

    #[test]
    fn create_data_blob_guid_with_preference_uses_ec_when_available() {
        let json = r#"{
            "volumes": [{
                "volume_id": 1,
                "bss_nodes": [
                    {"node_id":"bss0","ip":"127.0.0.1","port":18088}
                ]
            }],
            "quorum": {"n":1,"r":1,"w":1},
            "ec_volumes": [{
                "volume_id": 32768,
                "data_shards": 4,
                "parity_shards": 2,
                "bss_nodes": [
                    {"node_id":"bss0","ip":"127.0.0.1","port":18088},
                    {"node_id":"bss1","ip":"127.0.0.1","port":18089},
                    {"node_id":"bss2","ip":"127.0.0.1","port":18090},
                    {"node_id":"bss3","ip":"127.0.0.1","port":18091},
                    {"node_id":"bss4","ip":"127.0.0.1","port":18092},
                    {"node_id":"bss5","ip":"127.0.0.1","port":18093}
                ]
            }]
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        let proxy = DataVgProxy::new(info, Duration::from_secs(5), Duration::from_secs(5)).unwrap();
        let guid = proxy.create_data_blob_guid_with_preference(true);
        assert_eq!(guid.volume_id, 0x8000);
    }
}
