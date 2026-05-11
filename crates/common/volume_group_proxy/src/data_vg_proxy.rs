use crate::DataVgError;
use bytes::Bytes;
use data_types::ec_utils::{ec_padded_len, ec_rotation};
use data_types::{DataBlobGuid, DataVgInfo, TraceId, Volume, VolumeMode};
use futures::stream::{FuturesUnordered, StreamExt};
use metrics_wrapper::{counter, histogram};
use rand::seq::{IndexedRandom, SliceRandom};
use reed_solomon_simd::{decode as rs_decode, encode as rs_encode};
use rpc_client_bss::{BlobCommitInfo, BlobInfo, BssBatchSubOp, RpcClientBss};
use rpc_client_common::RpcError;
use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{debug, error, warn};
use uuid::Uuid;

#[cfg(feature = "tokio-runtime")]
fn spawn_background<F: std::future::Future<Output = ()> + Send + 'static>(fut: F) {
    tokio::spawn(fut);
}

#[cfg(all(feature = "compio-runtime", not(feature = "tokio-runtime")))]
fn spawn_background<F: std::future::Future<Output = ()> + 'static>(fut: F) {
    compio_runtime::spawn(fut).detach();
}

static EPOCH: OnceLock<Instant> = OnceLock::new();

/// Per-node response from a fan-out read. Bound at the type-alias
/// layer so the multi-replica reader code doesn't blow past
/// `clippy::type_complexity`.
type NodeReadResponse = (Arc<BssNode>, Result<(Bytes, u64), RpcError>);

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
    mode: VolumeMode,
}

pub struct DataVgProxy {
    volumes: Vec<VolumeWithNodes>,
    round_robin_counter: AtomicU64,
    rpc_timeout: Duration,
}

/// Build a per-BSS-node sub-op list for an EC batch. Each input
/// `PutDataBlob` is RS-encoded into `k+m` shards; shard `s` is sent
/// to node `(s + rotation) % total`. The returned vector is indexed
/// by absolute position in `volume.bss_nodes`. Delete / Reserve
/// sub-ops aren't sharded -- they're replicated to every node
/// identically. EC rotation depends on `blob_guid.blob_id`, and the
/// batch is single-blob, so one rotation applies across all
/// PutDataBlob sub-ops in the input.
fn build_ec_per_node_subops(
    sub_ops: &[BssBatchSubOp],
    k: usize,
    m: usize,
    commit_blob_guid: DataBlobGuid,
) -> Result<Vec<Vec<BssBatchSubOp>>, DataVgError> {
    let total = k + m;
    let rotation = ec_rotation(&commit_blob_guid.blob_id, total as u32);
    let mut per_node: Vec<Vec<BssBatchSubOp>> = (0..total).map(|_| Vec::new()).collect();

    for op in sub_ops {
        match op {
            BssBatchSubOp::PutDataBlob {
                blob_guid,
                block_number,
                body,
                body_checksum: _,
                version,
            } => {
                // RS-encode this block into k+m shards.
                let original_len = body.len();
                let padded_len = ec_padded_len(original_len, k);
                let shard_size = padded_len / k;
                let mut padded = body.to_vec();
                padded.resize(padded_len, 0u8);
                let mut shards: Vec<Vec<u8>> = Vec::with_capacity(total);
                for i in 0..k {
                    shards.push(padded[i * shard_size..(i + 1) * shard_size].to_vec());
                }
                let parity = rs_encode(k, m, &shards)
                    .map_err(|e| DataVgError::Internal(format!("RS encode failed: {}", e)))?;
                shards.extend(parity);

                for (shard_idx, shard) in shards.into_iter().enumerate() {
                    let node_idx = (shard_idx + rotation) % total;
                    let shard_bytes = bytes::Bytes::from(shard);
                    let checksum = xxhash_rust::xxh3::xxh3_64(&shard_bytes);
                    per_node[node_idx].push(BssBatchSubOp::PutDataBlob {
                        blob_guid: *blob_guid,
                        block_number: *block_number,
                        body: shard_bytes,
                        body_checksum: checksum,
                        version: *version,
                    });
                }
            }
            BssBatchSubOp::DeleteDataBlob { .. } | BssBatchSubOp::ReserveBlocks { .. } => {
                // No body to shard -- replicate identically to every
                // data BSS node so the per-block tombstone /
                // reservation lands across the volume.
                for slot in per_node.iter_mut() {
                    slot.push(op.clone());
                }
            }
        }
    }
    Ok(per_node)
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
        debug!(
            "Initializing DataVgProxy with {} volumes, circuit breaker config: {:?}",
            data_vg_info.volumes.len(),
            cb_config
        );

        if data_vg_info.volumes.is_empty() {
            return Err(DataVgError::InitializationError(
                "No volumes (replicated or EC) configured".to_string(),
            ));
        }

        let mut volumes_with_nodes = Vec::new();

        for volume in data_vg_info.volumes {
            // Validate based on mode
            match &volume.mode {
                VolumeMode::Replicated { r, w, .. } => {
                    if *r == 0 {
                        return Err(DataVgError::InitializationError(format!(
                            "Volume {} has invalid r=0",
                            volume.volume_id
                        )));
                    }
                    if *w == 0 {
                        return Err(DataVgError::InitializationError(format!(
                            "Volume {} has invalid w=0",
                            volume.volume_id
                        )));
                    }
                    if *w as usize > volume.bss_nodes.len() {
                        return Err(DataVgError::InitializationError(format!(
                            "Volume {} has w={} but only {} nodes",
                            volume.volume_id,
                            w,
                            volume.bss_nodes.len()
                        )));
                    }
                }
                VolumeMode::ErasureCoded {
                    data_shards,
                    parity_shards,
                } => {
                    if !Volume::is_ec_volume_id(volume.volume_id) {
                        return Err(DataVgError::InitializationError(format!(
                            "EC volume {} must be in 0x8000..0xFFFE range",
                            volume.volume_id
                        )));
                    }
                    if *data_shards == 0 {
                        return Err(DataVgError::InitializationError(format!(
                            "EC volume {} has invalid data_shards=0",
                            volume.volume_id
                        )));
                    }
                    if *parity_shards == 0 {
                        return Err(DataVgError::InitializationError(format!(
                            "EC volume {} has invalid parity_shards=0",
                            volume.volume_id
                        )));
                    }
                    let total_shards = data_shards + parity_shards;
                    if volume.bss_nodes.len() != total_shards as usize {
                        return Err(DataVgError::InitializationError(format!(
                            "EC volume {} has {} nodes but expected k+m={}",
                            volume.volume_id,
                            volume.bss_nodes.len(),
                            total_shards
                        )));
                    }
                }
            }

            let volume_id = volume.volume_id;
            let mode = volume.mode;

            let mut bss_nodes = Vec::new();
            for bss_node in volume.bss_nodes {
                let address = format!("{}:{}", bss_node.ip, bss_node.port);
                debug!(
                    "Creating BSS node for volume {} node {}: {}",
                    volume_id, bss_node.node_id, address
                );
                bss_nodes.push(Arc::new(BssNode::new(
                    address,
                    cb_config.clone(),
                    rpc_connection_timeout,
                )));
            }

            if let VolumeMode::ErasureCoded {
                data_shards,
                parity_shards,
            } = &mode
            {
                debug!(
                    "EC volume {} initialized: k={}, m={}, {} nodes",
                    volume_id,
                    data_shards,
                    parity_shards,
                    bss_nodes.len()
                );
            }

            volumes_with_nodes.push(VolumeWithNodes {
                volume_id,
                bss_nodes,
                mode,
            });
        }

        debug!(
            "DataVgProxy initialized successfully with {} volumes",
            volumes_with_nodes.len(),
        );

        Ok(Self {
            volumes: volumes_with_nodes,
            round_robin_counter: AtomicU64::new(0),
            rpc_timeout: rpc_request_timeout,
        })
    }

    pub fn select_volume_for_blob_with_preference(&self, prefer_ec: bool) -> u16 {
        let counter = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) as usize;

        if prefer_ec {
            let ec_volumes: Vec<_> = self
                .volumes
                .iter()
                .filter(|v| matches!(v.mode, VolumeMode::ErasureCoded { .. }))
                .collect();
            if !ec_volumes.is_empty() {
                return ec_volumes[counter % ec_volumes.len()].volume_id;
            }
        }

        let replicated: Vec<_> = self
            .volumes
            .iter()
            .filter(|v| matches!(v.mode, VolumeMode::Replicated { .. }))
            .collect();
        if !replicated.is_empty() {
            return replicated[counter % replicated.len()].volume_id;
        }

        // Fallback to whatever is available
        self.volumes[counter % self.volumes.len()].volume_id
    }

    pub fn select_volume_for_blob(&self) -> u16 {
        self.select_volume_for_blob_with_preference(false)
    }

    fn find_volume(&self, volume_id: u16) -> Option<&VolumeWithNodes> {
        self.volumes.iter().find(|v| v.volume_id == volume_id)
    }

    async fn get_blob_from_node_instance(
        &self,
        bss_node: &BssNode,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        trace_id: &TraceId,
        fast_path: bool,
    ) -> Result<(Bytes, u64), RpcError> {
        tracing::debug!(%blob_guid, bss_address=%bss_node.address, block_number, content_len, fast_path, "get_blob_from_node_instance calling BSS");

        let bss_client = bss_node.get_client();

        let mut body = Bytes::new();
        let version: u64;

        if fast_path {
            // Fast path: single attempt, no retries
            version = bss_client
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
                    Ok(v) => {
                        version = v;
                        break;
                    }
                    Err(e) if e.retryable() && retries > 0 => {
                        retries -= 1;
                        retry_count += 1;
                        rpc_client_common::rpc_sleep(backoff).await;
                        backoff = backoff.saturating_mul(2);
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        tracing::debug!(%blob_guid, bss_address=%bss_node.address, block_number, data_size=body.len(), version, "get_blob_from_node_instance result");

        Ok((body, version))
    }

    async fn delete_blob_from_node(
        bss_node: Arc<BssNode>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
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
                        version,
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
                        rpc_client_common::rpc_sleep(backoff).await;
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
        version: u64,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let selected_volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!(
                "Volume {} not found in DataVgProxy",
                blob_guid.volume_id
            ))
        })?;

        if let VolumeMode::ErasureCoded { .. } = &selected_volume.mode {
            return self
                .put_blob_ec(blob_guid, block_number, body, version, trace_id)
                .await;
        }

        let start = Instant::now();
        let trace_id = *trace_id;
        histogram!("blob_size", "operation" => "put").record(body.len() as f64);

        debug!("Using volume {} for put_blob", selected_volume.volume_id);

        let rpc_timeout = self.rpc_timeout;
        let write_quorum = match &selected_volume.mode {
            VolumeMode::Replicated { w, .. } => *w as usize,
            VolumeMode::ErasureCoded { .. } => unreachable!(),
        };

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
        bss_node_indices.shuffle(&mut rand::rng());

        let mut write_futures = FuturesUnordered::new();
        for &index in &bss_node_indices {
            let bss_node = available_nodes[index].clone();
            write_futures.push(Self::put_blob_to_node(
                bss_node,
                blob_guid,
                block_number,
                body.clone(),
                body_checksum,
                version,
                rpc_timeout,
                trace_id,
            ));
        }

        let mut successful_writes = 0;
        let mut errors = Vec::with_capacity(available_nodes.len());

        // Wait only until we achieve write quorum
        while let Some((node, address, result)) = write_futures.next().await {
            match result {
                Ok(()) | Err(RpcError::VersionSkipped) => {
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
                // Override-style writes (version > 1) must land on every
                // available replica before we return so an immediate
                // read can't pick a lagging replica that still serves
                // the older content.
                if version > 1 {
                    while let Some((node, address, result)) = write_futures.next().await {
                        match result {
                            Ok(()) | Err(RpcError::VersionSkipped) => {
                                node.record_success();
                                debug!("Override write to {} completed", address);
                            }
                            Err(e) => {
                                node.record_failure();
                                warn!("Override write to {} failed: {}", address, e);
                            }
                        }
                    }
                } else {
                    // Spawn remaining writes as background task for eventual consistency
                    spawn_background(async move {
                        while let Some((bg_node, addr, res)) = write_futures.next().await {
                            match res {
                                Ok(()) | Err(RpcError::VersionSkipped) => {
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
                }

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
            successful_writes, write_quorum, errors
        );
        Err(DataVgError::QuorumFailure(format!(
            "Write quorum failed ({}/{}): {}",
            successful_writes,
            write_quorum,
            errors.join("; ")
        )))
    }

    pub async fn put_blob_vectored(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        chunks: Vec<Bytes>,
        version: u64,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let selected_volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!(
                "Volume {} not found in DataVgProxy",
                blob_guid.volume_id
            ))
        })?;

        if let VolumeMode::ErasureCoded { .. } = &selected_volume.mode {
            let total_size: usize = chunks.iter().map(|c| c.len()).sum();
            let mut combined = Vec::with_capacity(total_size);
            for chunk in &chunks {
                combined.extend_from_slice(chunk);
            }
            return self
                .put_blob_ec(
                    blob_guid,
                    block_number,
                    Bytes::from(combined),
                    version,
                    trace_id,
                )
                .await;
        }

        let start = Instant::now();
        let trace_id = *trace_id;
        let total_size: usize = chunks.iter().map(|c| c.len()).sum();
        histogram!("blob_size", "operation" => "put").record(total_size as f64);

        debug!(
            "Using volume {} for put_blob_vectored",
            selected_volume.volume_id
        );

        let rpc_timeout = self.rpc_timeout;
        let write_quorum = match &selected_volume.mode {
            VolumeMode::Replicated { w, .. } => *w as usize,
            VolumeMode::ErasureCoded { .. } => unreachable!(),
        };

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
        bss_node_indices.shuffle(&mut rand::rng());

        let mut write_futures = FuturesUnordered::new();
        for &index in &bss_node_indices {
            let bss_node = available_nodes[index].clone();
            write_futures.push(Self::put_blob_to_node_vectored(
                bss_node,
                blob_guid,
                block_number,
                chunks.clone(),
                body_checksum,
                version,
                rpc_timeout,
                trace_id,
            ));
        }

        let mut successful_writes = 0;
        let mut errors = Vec::with_capacity(available_nodes.len());

        while let Some((node, address, result)) = write_futures.next().await {
            match result {
                Ok(()) | Err(RpcError::VersionSkipped) => {
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
                if version > 1 {
                    while let Some((node, address, result)) = write_futures.next().await {
                        match result {
                            Ok(()) | Err(RpcError::VersionSkipped) => {
                                node.record_success();
                                debug!("Override vectored write to {} completed", address);
                            }
                            Err(e) => {
                                node.record_failure();
                                warn!("Override vectored write to {} failed: {}", address, e);
                            }
                        }
                    }
                } else {
                    spawn_background(async move {
                        while let Some((bg_node, addr, res)) = write_futures.next().await {
                            match res {
                                Ok(()) | Err(RpcError::VersionSkipped) => {
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
                }

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
            write_quorum,
            blob_guid.blob_id,
            block_number,
            errors.join("; ")
        );
        Err(DataVgError::QuorumFailure(format!(
            "Failed to achieve write quorum ({}/{}): {}",
            successful_writes,
            write_quorum,
            errors.join("; ")
        )))
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_blob_to_node(
        bss_node: Arc<BssNode>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        body_checksum: u64,
        version: u64,
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
                version,
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

    #[allow(clippy::too_many_arguments)]
    async fn put_blob_to_node_vectored(
        bss_node: Arc<BssNode>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        chunks: Vec<Bytes>,
        body_checksum: u64,
        version: u64,
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
                version,
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
        self.get_blob_with_version(blob_guid, block_number, content_len, None, body, trace_id)
            .await
    }

    /// Variant of `get_blob` that enforces a read-side version check.
    ///
    /// When `expected_version = Some(v)`, the returned block's BSS-stamped
    /// version must equal `v`. Replicas that return a different version
    /// (typically a lagging replica that hasn't received the latest write
    /// quorum yet) are skipped, and the read falls through to the next
    /// replica. If every reachable replica returns a mismatched version,
    /// `DataVgError::StaleVersion` is returned so the caller can retry or
    /// surface the staleness to the writer's flush sequence.
    ///
    /// When `expected_version = None`, behaviour matches `get_blob`: the
    /// first successful read is returned regardless of its version (no
    /// behavioural change for callers that don't care).
    pub async fn get_blob_with_version(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        expected_version: Option<u64>,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let volume_id = blob_guid.volume_id;
        let volume = self.find_volume(volume_id).ok_or_else(|| {
            tracing::error!(volume_id, available_volumes=?self.volumes.iter().map(|v| v.volume_id).collect::<Vec<_>>(), "Volume not found in DataVgProxy for get_blob");
            DataVgError::InitializationError(format!("Volume {} not found", volume_id))
        })?;

        if let VolumeMode::ErasureCoded { .. } = &volume.mode {
            return self
                .get_blob_ec(blob_guid, block_number, content_len, body, trace_id)
                .await;
        }

        let start = Instant::now();

        let blob_id = blob_guid.blob_id;

        tracing::debug!(%blob_id, volume_id, ?expected_version, available_volumes=?self.volumes.iter().map(|v| v.volume_id).collect::<Vec<_>>(), "get_blob looking for volume");

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

        // Fast path: try reading from a randomly selected available node.
        // A version mismatch is treated like a transient failure on this
        // replica so the fallback loop can try another node.
        let mut saw_stale_version = false;
        if !available_nodes.is_empty() {
            let selected_node = *available_nodes.choose(&mut rand::rng()).unwrap();
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
                Ok((blob_data, returned_version)) => {
                    if let Some(expected) = expected_version
                        && returned_version != expected
                    {
                        // Don't penalise the node — it answered correctly,
                        // just hasn't received the latest version yet.
                        warn!(
                            "Fast path read from {} returned version {} but expected {}, falling back",
                            selected_node.address, returned_version, expected
                        );
                        saw_stale_version = true;
                    } else {
                        selected_node.record_success();
                        histogram!("datavg_get_blob_nanos", "result" => "fast_path_success")
                            .record(start.elapsed().as_nanos() as f64);
                        *body = blob_data;
                        return Ok(());
                    }
                }
                Err(RpcError::NotFound) => {
                    // Hole on this replica. Don't penalise the node;
                    // fall through to the fallback path which polls
                    // every replica and either finds a copy somewhere
                    // or concludes BlockNotFound for the whole quorum.
                    debug!(
                        "Fast path: BSS node {} reports BlockNotFound, falling back",
                        selected_node.address
                    );
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

        let _read_quorum = match &volume.mode {
            VolumeMode::Replicated { r, .. } => *r as usize,
            VolumeMode::ErasureCoded { .. } => unreachable!(),
        };

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
        // Track whether every responding replica reported NotFound, so a
        // sparse-file hole (block legitimately missing on every replica)
        // can be surfaced as DataVgError::BlockNotFound rather than a
        // generic quorum failure.
        let mut not_found_count: usize = 0;
        let mut other_error_count: usize = 0;

        // Wait until we get a successful read (quorum of 1) or all fail.
        // A response with a mismatched version is treated like a transient
        // failure on this replica: the node is not penalised (its data is
        // intact, just lagging) but we keep polling other replicas.
        while let Some((node, result)) = read_futures.next().await {
            match result {
                Ok((blob_data, returned_version)) => {
                    if let Some(expected) = expected_version
                        && returned_version != expected
                    {
                        warn!(
                            "Read from BSS node {} returned version {} but expected {}, trying other replicas",
                            node.address, returned_version, expected
                        );
                        saw_stale_version = true;
                        continue;
                    }
                    node.record_success();
                    successful_reads += 1;
                    debug!("Successful read from BSS node: {}", node.address);
                    if successful_blob_data.is_none() {
                        successful_blob_data = Some(blob_data);
                        // For reads, we can return as soon as we get one successful result
                        break;
                    }
                }
                Err(RpcError::NotFound) => {
                    // Hole semantics: this replica agrees the block does
                    // not exist. Don't penalise the node -- it answered
                    // promptly and correctly.
                    not_found_count += 1;
                    debug!("BSS node {} reports BlockNotFound", node.address);
                }
                Err(rpc_error) => {
                    node.record_failure();
                    other_error_count += 1;
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

        // No replica returned the expected version. Distinguish stale-quorum
        // from outright failure so the caller can react accordingly (e.g. a
        // writer's flush sequence may want to wait for replication catchup
        // and retry, while an outright failure should propagate as today).
        if saw_stale_version && let Some(expected) = expected_version {
            histogram!("datavg_get_blob_nanos", "result" => "stale_version")
                .record(start.elapsed().as_nanos() as f64);
            warn!(
                "All reachable replicas returned a version older than expected {} for blob {}:{}",
                expected, blob_id, block_number
            );
            return Err(DataVgError::StaleVersion { expected });
        }

        // Sparse-file hole: every responding replica agrees the block
        // does not exist. The caller (fs_server's read path) maps this
        // to zeros within the file's logical EOF.
        if not_found_count > 0 && other_error_count == 0 {
            histogram!("datavg_get_blob_nanos", "result" => "block_not_found")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::BlockNotFound);
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

    /// Read a block with strict version-aware semantics: fan out to
    /// every available replica, pick the bytes at `max(stored_version)`,
    /// and inline-repair laggers if max-version coverage is below read
    /// quorum.
    ///
    /// This is the partition-rejoin-safe read variant: a replica that
    /// missed a flush silently holds pre-bump bytes for the affected
    /// blocks (same per-key version on every replica is impossible to
    /// tell apart without comparing across replicas). Without
    /// max-version selection, a random fast-path pick on the lagger
    /// returns stale bytes. Mirrors
    /// `core/nss_server/buffer_manager/blob_io.zig::MultiBssObjectReader`.
    ///
    /// Caller decides when to use this vs the cheaper `get_blob`:
    /// today `backend::read_block` routes here when
    /// `ObjectLayout.blob_version > 1` (the file has been overridden at
    /// least once and a partition-rejoin window is possible).
    pub async fn get_blob_with_quorum_check(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let volume_id = blob_guid.volume_id;
        let volume = self.find_volume(volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!("Volume {} not found", volume_id))
        })?;

        // EC fan-out + strict-max-version filter. Per-read inline
        // repair (re-encoding stale shards and writing them back to
        // laggers) is a follow-up; bss_repair scans converge them
        // asynchronously today.
        if let VolumeMode::ErasureCoded { .. } = &volume.mode {
            return self
                .get_blob_ec_with_quorum_check(blob_guid, block_number, content_len, body, trace_id)
                .await;
        }

        let start = Instant::now();
        let blob_id = blob_guid.blob_id;
        let read_quorum = match &volume.mode {
            VolumeMode::Replicated { r, .. } => *r as usize,
            VolumeMode::ErasureCoded { .. } => unreachable!(),
        };

        let available_nodes: Vec<_> = volume
            .bss_nodes
            .iter()
            .filter(|node| {
                let avail = node.is_available();
                if !avail {
                    counter!("circuit_breaker_skipped", "node" => node.address.clone(), "operation" => "get_quorum_check").increment(1);
                }
                avail
            })
            .cloned()
            .collect();

        if available_nodes.len() < read_quorum {
            histogram!("datavg_get_blob_quorum_nanos", "result" => "insufficient_nodes")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::QuorumFailure(format!(
                "Insufficient available nodes ({}/{}) for read quorum ({})",
                available_nodes.len(),
                volume.bss_nodes.len(),
                read_quorum
            )));
        }

        // Fan out reads to every available replica in parallel.
        let mut read_futures = FuturesUnordered::new();
        for bss_node in available_nodes.iter().cloned() {
            let proxy = self;
            read_futures.push(async move {
                let result = proxy
                    .get_blob_from_node_instance(
                        &bss_node,
                        blob_guid,
                        block_number,
                        content_len,
                        trace_id,
                        false, // not fast_path: allow retries on transient failures
                    )
                    .await;
                (bss_node, result)
            });
        }

        // Each entry: (node, Ok((bytes, version)) | Err(rpc_err)).
        // Each underlying RPC carries its own per-request timeout
        // (self.rpc_timeout), so a stuck replica fails its own future
        // rather than blocking the fan-out indefinitely. We wait for
        // every replica's response, then dispatch on quorum.
        let mut responses: Vec<NodeReadResponse> = Vec::new();
        let mut success_count: usize = 0;
        let mut not_found_count: usize = 0;
        let mut other_err_count: usize = 0;
        while let Some((node, result)) = read_futures.next().await {
            match &result {
                Ok(_) => success_count += 1,
                Err(RpcError::NotFound) => not_found_count += 1,
                Err(_) => other_err_count += 1,
            }
            responses.push((node, result));
        }

        // Sparse-file hole: every responding replica agreed the
        // block does not exist (no transient errors). Surface as
        // BlockNotFound so the fs_server read path can map to zeros.
        if success_count == 0 && other_err_count == 0 && not_found_count > 0 {
            histogram!("datavg_get_blob_quorum_nanos", "result" => "block_not_found")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::BlockNotFound);
        }

        if success_count < read_quorum {
            histogram!("datavg_get_blob_quorum_nanos", "result" => "quorum_failure")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::QuorumFailure(format!(
                "Quorum read failed: {}/{} successful responses (need {})",
                success_count,
                available_nodes.len(),
                read_quorum
            )));
        }

        // Compute max_version across successful responses.
        let mut max_version: u64 = 0;
        for (_, res) in &responses {
            if let Ok((_, v)) = res
                && *v > max_version
            {
                max_version = *v;
            }
        }

        // Cohort at max_version. Sanity check: same version on
        // different replicas must agree on body length + content hash.
        let max_cohort: Vec<&NodeReadResponse> = responses
            .iter()
            .filter(|(_, r)| matches!(r, Ok((_, v)) if *v == max_version))
            .collect();

        if max_cohort.is_empty() {
            histogram!("datavg_get_blob_quorum_nanos", "result" => "max_cohort_empty")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::QuorumFailure(
                "No replica reported max_version (post-filter)".to_string(),
            ));
        }

        let (canon_bytes, _) = match &max_cohort[0].1 {
            Ok(pair) => pair,
            Err(_) => unreachable!("filter rejected Err"),
        };
        let canon_checksum = xxhash_rust::xxh3::xxh3_64(canon_bytes);
        let canon_len = canon_bytes.len();
        for (node, r) in max_cohort.iter().skip(1) {
            if let Ok((b, _)) = r
                && (b.len() != canon_len || xxhash_rust::xxh3::xxh3_64(b) != canon_checksum)
            {
                error!(
                    "Data divergence at version={} on blob {}:{} (node={}): same version, different bytes",
                    max_version, blob_id, block_number, node.address
                );
                return Err(DataVgError::Corrupted);
            }
        }

        // Happy path: max-version cohort meets read quorum.
        if max_cohort.len() >= read_quorum {
            histogram!("datavg_get_blob_quorum_nanos", "result" => "quorum_at_max")
                .record(start.elapsed().as_nanos() as f64);
            *body = canon_bytes.clone();
            return Ok(());
        }

        // Inline-repair: write the max-version bytes back to every
        // available replica at version=max_version. Lagging replicas
        // advance via bssOverwriteCheck (new > old -> overwrite);
        // already-max replicas idempotent-skip.
        warn!(
            "get_blob_with_quorum_check: max_cohort {}/{} below read_quorum {} for blob {}:{} (max_version={}); inline-repair",
            max_cohort.len(),
            available_nodes.len(),
            read_quorum,
            blob_id,
            block_number,
            max_version
        );

        let canon_body = canon_bytes.clone();
        let write_quorum = match &volume.mode {
            VolumeMode::Replicated { w, .. } => *w as usize,
            VolumeMode::ErasureCoded { .. } => unreachable!(),
        };

        let mut repair_futs = FuturesUnordered::new();
        for node in available_nodes.iter().cloned() {
            repair_futs.push(Self::put_blob_to_node(
                node,
                blob_guid,
                block_number,
                canon_body.clone(),
                canon_checksum,
                max_version,
                self.rpc_timeout,
                *trace_id,
            ));
        }

        let mut repair_ok: usize = 0;
        while let Some((node, address, result)) = repair_futs.next().await {
            match result {
                Ok(()) | Err(RpcError::VersionSkipped) => {
                    node.record_success();
                    repair_ok += 1;
                    debug!("inline-repair write succeeded on {}", address);
                }
                Err(e) => {
                    node.record_failure();
                    warn!("inline-repair write failed on {}: {}", address, e);
                }
            }
        }

        if repair_ok < write_quorum {
            histogram!("datavg_get_blob_quorum_nanos", "result" => "repair_quorum_failure")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::QuorumFailure(format!(
                "Inline-repair failed to achieve write quorum ({}/{})",
                repair_ok, write_quorum,
            )));
        }

        histogram!("datavg_get_blob_quorum_nanos", "result" => "repaired")
            .record(start.elapsed().as_nanos() as f64);
        *body = canon_body;
        Ok(())
    }

    pub async fn delete_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!("Volume {} not found", blob_guid.volume_id))
        })?;

        if let VolumeMode::ErasureCoded { .. } = &volume.mode {
            return self
                .delete_blob_ec(blob_guid, block_number, version, trace_id)
                .await;
        }

        let start = Instant::now();
        let trace_id = *trace_id;

        let rpc_timeout = self.rpc_timeout;
        let write_quorum = match &volume.mode {
            VolumeMode::Replicated { w, .. } => *w as usize,
            VolumeMode::ErasureCoded { .. } => unreachable!(),
        };

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
                version,
                rpc_timeout,
                trace_id,
            ));
        }

        let mut successful_deletes = 0;
        let mut not_found_count = 0;
        let mut other_error_count = 0;
        let mut errors = Vec::with_capacity(available_nodes.len());

        while let Some((node, address, result)) = delete_futures.next().await {
            match result {
                Ok(()) => {
                    node.record_success();
                    successful_deletes += 1;
                    debug!("Successful delete from BSS node: {}", address);
                }
                Err(RpcError::NotFound) => {
                    // The block was never on this replica -- a hole, or a
                    // node that legitimately missed the original write.
                    // Tracked separately from real errors so that a
                    // unanimous NotFound across all responding nodes
                    // collapses to a successful delete-of-hole instead
                    // of a quorum failure.
                    node.record_success();
                    debug!("BSS node {} reports NotFound on delete", address);
                    not_found_count += 1;
                }
                Err(rpc_error) => {
                    node.record_failure();
                    warn!(
                        "RPC error deleting from BSS node {}: {}",
                        address, rpc_error
                    );
                    errors.push(format!("{}: {}", address, rpc_error));
                    other_error_count += 1;
                }
            }

            if successful_deletes >= write_quorum {
                // Spawn remaining deletes as background task for eventual consistency
                spawn_background(async move {
                    while let Some((bg_node, addr, res)) = delete_futures.next().await {
                        match res {
                            Ok(()) | Err(RpcError::NotFound) => {
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

        // Hole tolerance: every responding node agreed the block was
        // already absent. The block is therefore deleted by definition
        // (it was never there on the surviving replicas). Sparse files
        // hit this on shrink-trim of holes, and the strict quorum path
        // would otherwise spuriously fail the flush.
        if successful_deletes == 0 && other_error_count == 0 && not_found_count > 0 {
            histogram!("datavg_delete_blob_nanos", "result" => "hole_tolerated")
                .record(start.elapsed().as_nanos() as f64);
            debug!(
                "Delete: all {} responding nodes reported NotFound for blob {}:{} -- treating as success",
                not_found_count, blob_guid.blob_id, block_number
            );
            return Ok(());
        }

        histogram!("datavg_delete_blob_nanos", "result" => "quorum_failure")
            .record(start.elapsed().as_nanos() as f64);
        error!(
            "Delete quorum failed ({}/{}). Errors: {:?}",
            successful_deletes, write_quorum, errors
        );
        Err(DataVgError::QuorumFailure(format!(
            "Delete quorum failed ({}/{}): {}",
            successful_deletes,
            write_quorum,
            errors.join("; ")
        )))
    }

    // ---- EC (Erasure-Coded) blob operations ----

    /// EC put: RS-encode block into k+m shards, send to nodes with W=k+1 quorum
    async fn put_blob_ec(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        version: u64,
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

        let ec_vol = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!("EC volume {} not found", blob_guid.volume_id))
        })?;

        let (k, m) = match &ec_vol.mode {
            VolumeMode::ErasureCoded {
                data_shards,
                parity_shards,
            } => (*data_shards as usize, *parity_shards as usize),
            _ => unreachable!(),
        };
        let total = k + m;
        let write_quorum = k + 1; // W = k + 1

        // Pad body to a full RS stripe with even shard size.
        let original_len = body.len();
        let padded_len = ec_padded_len(original_len, k);
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
        let rotation = ec_rotation(&blob_guid.blob_id, total as u32);

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
                version,
                rpc_timeout,
                trace_id,
            ));
        }

        let mut successful_writes = 0;
        let mut errors = Vec::new();

        while let Some((node, address, result)) = write_futures.next().await {
            match result {
                Ok(()) | Err(RpcError::VersionSkipped) => {
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
                // For override-style writes (version > 1), all shards must
                // be at the new version before we return: an immediate
                // reader assembles the file from k data shards, and a
                // mix of old/new shards yields a corrupted result. The
                // quorum-then-background optimisation is only safe for
                // brand-new objects (version == 1), where there's no
                // older content to mix with.
                if version > 1 {
                    while let Some((node, address, result)) = write_futures.next().await {
                        match result {
                            Ok(()) | Err(RpcError::VersionSkipped) => {
                                node.record_success();
                                debug!("EC override write to {} completed", address);
                            }
                            Err(e) => {
                                node.record_failure();
                                warn!("EC override write to {} failed: {}", address, e);
                            }
                        }
                    }
                } else {
                    // Background remaining writes for initial create.
                    spawn_background(async move {
                        while let Some((bg_node, addr, res)) = write_futures.next().await {
                            match res {
                                Ok(()) | Err(RpcError::VersionSkipped) => {
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
                }

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
        self.get_blob_ec_impl(blob_guid, block_number, content_len, body, trace_id, false)
            .await
    }

    /// Version-aware EC read. Fans out to all k+m shards, prefers
    /// the max-`BlobMeta.version` cohort for reconstruction, and
    /// fails with `StaleVersion` if fewer than k shards at
    /// max_version are available (rather than silently mixing
    /// versions in the RS decode, which would yield garbage).
    /// Inline repair of laggers via bss_repair scans; per-read
    /// repair (re-encoding + writing back lagging shards) is a
    /// follow-up.
    async fn get_blob_ec_with_quorum_check(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        self.get_blob_ec_impl(blob_guid, block_number, content_len, body, trace_id, true)
            .await
    }

    async fn get_blob_ec_impl(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        body: &mut Bytes,
        trace_id: &TraceId,
        strict_max_version: bool,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();

        // Empty body: nothing was stored, return empty
        if content_len == 0 {
            *body = Bytes::new();
            histogram!("datavg_get_blob_nanos", "result" => "ec_empty")
                .record(start.elapsed().as_nanos() as f64);
            return Ok(());
        }

        let ec_vol = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!("EC volume {} not found", blob_guid.volume_id))
        })?;

        let (k, m) = match &ec_vol.mode {
            VolumeMode::ErasureCoded {
                data_shards,
                parity_shards,
            } => (*data_shards as usize, *parity_shards as usize),
            _ => unreachable!(),
        };
        let total = k + m;

        let rotation = ec_rotation(&blob_guid.blob_id, total as u32);

        // Compute shard size, matching put_blob_ec padding.
        let padded_len = ec_padded_len(content_len, k);
        let shard_size = padded_len / k;

        // Fetch the k data shards (indices 0..k) from their rotated
        // nodes. We track the BSS-stamped version per shard alongside
        // the bytes so the strict_max_version path can reject stale
        // shards before they reach the RS decoder.
        let mut shard_results: Vec<Option<Vec<u8>>> = vec![None; total];
        let mut shard_versions: Vec<Option<u64>> = vec![None; total];
        let mut data_shards_received = 0;
        let mut data_shards_not_found = 0usize;
        let mut data_shards_other_err = 0usize;
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
                Ok((data, version)) => {
                    ec_vol.bss_nodes[node_idx].record_success();
                    shard_results[shard_idx] = Some(data.to_vec());
                    shard_versions[shard_idx] = Some(version);
                    data_shards_received += 1;
                }
                Err(RpcError::NotFound) => {
                    // Hole on this shard. Don't penalise the node.
                    data_shards_not_found += 1;
                    debug!(
                        "EC data shard {} reports BlockNotFound from {}",
                        shard_idx, ec_vol.bss_nodes[node_idx].address
                    );
                }
                Err(e) => {
                    ec_vol.bss_nodes[node_idx].record_failure();
                    data_shards_other_err += 1;
                    warn!(
                        "EC data shard {} fetch failed from {}: {}",
                        shard_idx, ec_vol.bss_nodes[node_idx].address, e
                    );
                }
            }
        }

        // If every data-shard fetch came back as BlockNotFound (and
        // no transient errors), the block legitimately doesn't exist
        // on the EC volume either. Surface as BlockNotFound so the
        // fs_server read path maps it to zeros.
        if data_shards_received == 0 && data_shards_other_err == 0 && data_shards_not_found > 0 {
            histogram!("datavg_get_blob_nanos", "result" => "ec_block_not_found")
                .record(start.elapsed().as_nanos() as f64);
            return Err(DataVgError::BlockNotFound);
        }

        if data_shards_received == k && !strict_max_version {
            // All data shards received, concatenate directly (no RS decode needed).
            // strict_max_version goes through the parity-aware path
            // below so we can check parity versions and inline-repair
            // any stale parity shards too.
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

        let mut parity_shards_not_found = 0usize;
        let mut parity_shards_other_err = 0usize;
        while let Some((shard_idx, node_idx, result)) = parity_futures.next().await {
            match result {
                Ok((data, version)) => {
                    ec_vol.bss_nodes[node_idx].record_success();
                    shard_results[shard_idx] = Some(data.to_vec());
                    shard_versions[shard_idx] = Some(version);
                }
                Err(RpcError::NotFound) => {
                    parity_shards_not_found += 1;
                    debug!(
                        "EC parity shard {} reports BlockNotFound from {}",
                        shard_idx, ec_vol.bss_nodes[node_idx].address
                    );
                }
                Err(e) => {
                    ec_vol.bss_nodes[node_idx].record_failure();
                    parity_shards_other_err += 1;
                    warn!(
                        "EC parity shard {} fetch failed from {}: {}",
                        shard_idx, ec_vol.bss_nodes[node_idx].address, e
                    );
                }
            }
        }

        // Strict-max-version filter: reject shards at less than the
        // observed max version. Mixing versions in an RS decode would
        // produce garbage; failing loudly is strictly better than
        // silent corruption. bss_repair scans converge the laggers
        // asynchronously.
        // Computed for the strict_max_version path; visible to the
        // inline-repair tail below so it can stamp shard writes at
        // the right version.
        let mut max_version: u64 = 0;
        if strict_max_version {
            max_version = shard_versions.iter().filter_map(|v| *v).max().unwrap_or(0);
            if max_version > 0 {
                for (i, ver) in shard_versions.iter().enumerate() {
                    if let Some(v) = ver
                        && *v < max_version
                    {
                        debug!(
                            "EC shard {} at version {} dropped (max_version={}); strict_max_version",
                            i, v, max_version
                        );
                        shard_results[i] = None;
                    }
                }
                let max_cohort = shard_versions
                    .iter()
                    .filter(|v| matches!(v, Some(x) if *x == max_version))
                    .count();
                if max_cohort < k {
                    histogram!("datavg_get_blob_nanos", "result" => "ec_stale_version")
                        .record(start.elapsed().as_nanos() as f64);
                    warn!(
                        "EC read: only {}/{} shards at max_version={}; need at least {} for decode",
                        max_cohort, total, max_version, k
                    );
                    return Err(DataVgError::StaleVersion {
                        expected: max_version,
                    });
                }
            }
        }

        let total_shards_received = shard_results.iter().filter(|s| s.is_some()).count();
        if total_shards_received < k {
            // If the unrecoverable read is because every reachable
            // shard agreed the block doesn't exist (no transient
            // errors at all), surface BlockNotFound so the fs_server
            // read path can map to zeros for sparse files.
            if total_shards_received == 0
                && data_shards_other_err == 0
                && parity_shards_other_err == 0
                && (data_shards_not_found > 0 || parity_shards_not_found > 0)
            {
                histogram!("datavg_get_blob_nanos", "result" => "ec_block_not_found")
                    .record(start.elapsed().as_nanos() as f64);
                return Err(DataVgError::BlockNotFound);
            }
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

        // Concatenate data shards (padded; truncated to content_len
        // before being handed to the caller). We keep the un-truncated
        // form around so the inline-repair tail below can re-encode
        // parity from it without reconstructing the padding.
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
        let padded_body = result_data.clone();
        result_data.truncate(content_len);
        *body = Bytes::from(result_data);

        // EC inline-repair (strict_max_version only): if any shard
        // came back at a version below `max_version`, re-encode the
        // padded body into k+m shards and push the freshened bytes
        // to those lagging shards' nodes at `version=max_version`.
        // bssOverwriteCheck advances the lagger; the already-current
        // shards idempotent-skip. Best-effort — we log failures and
        // proceed; bss_repair scans converge anything we miss.
        if strict_max_version {
            let stale_indices: Vec<usize> = shard_versions
                .iter()
                .enumerate()
                .filter_map(|(i, v)| match v {
                    Some(ver) if *ver < max_version => Some(i),
                    _ => None,
                })
                .collect();
            if !stale_indices.is_empty() && max_version > 0 {
                // Re-split the padded body into k data shards, then
                // run RS encode for parity. This mirrors put_blob_ec
                // exactly so the shard layout matches what BSS
                // already has at max_version.
                let mut data_shards: Vec<Vec<u8>> = Vec::with_capacity(k);
                for i in 0..k {
                    data_shards.push(padded_body[i * shard_size..(i + 1) * shard_size].to_vec());
                }
                let parity = rs_encode(k, m, &data_shards)
                    .map_err(|e| DataVgError::Internal(format!("RS encode failed: {}", e)))?;
                let mut all_shards: Vec<Vec<u8>> = Vec::with_capacity(total);
                all_shards.extend(data_shards);
                all_shards.extend(parity);

                let mut repair_futs = FuturesUnordered::new();
                for shard_idx in stale_indices {
                    let node_idx = (shard_idx + rotation) % total;
                    if !ec_vol.bss_nodes[node_idx].is_available() {
                        continue;
                    }
                    let node = ec_vol.bss_nodes[node_idx].clone();
                    let shard_data = Bytes::from(all_shards[shard_idx].clone());
                    let checksum = xxhash_rust::xxh3::xxh3_64(&shard_data);
                    repair_futs.push(Self::put_blob_to_node(
                        node,
                        blob_guid,
                        block_number,
                        shard_data,
                        checksum,
                        max_version,
                        self.rpc_timeout,
                        *trace_id,
                    ));
                }
                while let Some((node, address, result)) = repair_futs.next().await {
                    match result {
                        Ok(()) | Err(RpcError::VersionSkipped) => {
                            node.record_success();
                            debug!("EC inline-repair: shard write succeeded on {}", address);
                        }
                        Err(e) => {
                            node.record_failure();
                            warn!("EC inline-repair: shard write failed on {}: {}", address, e);
                        }
                    }
                }
            }
        }

        histogram!("datavg_get_blob_nanos", "result" => "ec_degraded_success")
            .record(start.elapsed().as_nanos() as f64);
        Ok(())
    }

    /// EC delete: send delete to all k+m nodes, wait for k+1 acks
    async fn delete_blob_ec(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();
        let trace_id = *trace_id;

        let ec_vol = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!("EC volume {} not found", blob_guid.volume_id))
        })?;

        let (k, m) = match &ec_vol.mode {
            VolumeMode::ErasureCoded {
                data_shards,
                parity_shards,
            } => (*data_shards as usize, *parity_shards as usize),
            _ => unreachable!(),
        };
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
                version,
                rpc_timeout,
                trace_id,
            ));
        }

        let mut successful_deletes = 0;
        let mut not_found_count = 0;
        let mut other_error_count = 0;
        let mut errors = Vec::new();

        while let Some((node, address, result)) = delete_futures.next().await {
            match result {
                Ok(()) => {
                    node.record_success();
                    successful_deletes += 1;
                    debug!("EC delete success from {}", address);
                }
                Err(RpcError::NotFound) => {
                    // Hole on this shard. Counted separately so unanimous
                    // NotFound across responding nodes collapses to a
                    // successful delete-of-hole below.
                    node.record_success();
                    debug!("EC delete: BSS node {} reports NotFound", address);
                    not_found_count += 1;
                }
                Err(rpc_error) => {
                    node.record_failure();
                    warn!("EC delete failed from {}: {}", address, rpc_error);
                    errors.push(format!("{}: {}", address, rpc_error));
                    other_error_count += 1;
                }
            }

            if successful_deletes >= write_quorum {
                spawn_background(async move {
                    while let Some((bg_node, addr, res)) = delete_futures.next().await {
                        match res {
                            Ok(()) | Err(RpcError::NotFound) => {
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

        if successful_deletes == 0 && other_error_count == 0 && not_found_count > 0 {
            histogram!("datavg_delete_blob_nanos", "result" => "ec_hole_tolerated")
                .record(start.elapsed().as_nanos() as f64);
            debug!(
                "EC delete: all {} responding shards reported NotFound for blob {}:{} -- treating as success",
                not_found_count, blob_guid.blob_id, block_number
            );
            return Ok(());
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

    /// Reserve a single block on every replica of `blob_guid` at
    /// `expected_version`. Replicated only -- EC volumes treat
    /// ReserveBlocks as a no-op for now (the EC stripe layout means a
    /// reservation has nothing to claim until real data lands; this
    /// matches the spec's "best-effort" framing).
    pub async fn reserve_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        block_size: u32,
        expected_version: u64,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!("Volume {} not found", blob_guid.volume_id))
        })?;
        if let VolumeMode::ErasureCoded { .. } = &volume.mode {
            return Ok(());
        }

        let rpc_timeout = self.rpc_timeout;
        let write_quorum = match &volume.mode {
            VolumeMode::Replicated { w, .. } => *w as usize,
            VolumeMode::ErasureCoded { .. } => unreachable!(),
        };

        let available_nodes: Vec<_> = volume
            .bss_nodes
            .iter()
            .filter(|node| node.is_available())
            .cloned()
            .collect();

        if available_nodes.len() < write_quorum {
            return Err(DataVgError::QuorumFailure(format!(
                "Insufficient available nodes ({}/{}) for reserve quorum ({})",
                available_nodes.len(),
                volume.bss_nodes.len(),
                write_quorum
            )));
        }

        let mut futures = FuturesUnordered::new();
        for bss_node in &available_nodes {
            let node = bss_node.clone();
            let trace_id = *trace_id;
            futures.push(async move {
                let address = node.address.clone();
                let result = node
                    .get_client()
                    .reserve_blocks(
                        blob_guid,
                        block_number,
                        block_size,
                        expected_version,
                        Some(rpc_timeout),
                        &trace_id,
                        0,
                    )
                    .await;
                (node, address, result)
            });
        }

        let mut successes = 0usize;
        let mut errors = Vec::new();
        while let Some((node, address, result)) = futures.next().await {
            match result {
                Ok(()) | Err(RpcError::VersionSkipped) => {
                    node.record_success();
                    successes += 1;
                }
                Err(e) => {
                    node.record_failure();
                    errors.push(format!("{}: {}", address, e));
                }
            }
            if successes >= write_quorum {
                return Ok(());
            }
        }

        Err(DataVgError::QuorumFailure(format!(
            "Reserve quorum failed ({}/{}): {}",
            successes,
            write_quorum,
            errors.join("; ")
        )))
    }

    /// Send N block mutations against a single Replicated volume as
    /// one batched RPC per replica, with M-of-N quorum reduction per
    /// entry. Returns one `Result<(), RpcError>` per input sub-op in
    /// the same order.
    ///
    /// All sub-ops must target the same `volume_id`; the caller groups
    /// by volume before invoking. Per-entry semantics mirror the
    /// non-batched paths (`put_data_blob_vectored`, `delete_data_blob`,
    /// `reserve_blocks`):
    ///
    /// - Each replica's per-entry status of `Ok(())` or
    ///   `Err(VersionSkipped)` counts as a per-entry success
    ///   (idempotent).
    /// - An entry is committed when its replica-success count reaches
    ///   `write_quorum`.
    /// - Override path (`max_entry_version > 1`) drains all replicas
    ///   before returning so background overrides never lag.
    /// - First-write path (`max_entry_version == 1`) terminates as
    ///   soon as every entry is *decided* -- either reached quorum, or
    ///   provably cannot with the remaining responses -- and finishes
    ///   the rest in a background task.
    ///
    /// `commit` publishes the blob's authoritative state after the
    /// batch lands -- `(total_size, block_count)` at `blob_version`.
    /// BSS publishes it once all sub-ops resolve; `get_blob_info`
    /// reads it back. Every batch publishes a commit; callers that
    /// don't change file state still pass the current
    /// `(total_size, block_count)` re-stamped at the new blob_version.
    ///
    /// EC volumes: each PutDataBlob sub-op is split into `k+m`
    /// shards routed via `ec_rotation`; the per-node batch's
    /// PutDataBlob sub-ops carry shard bodies. Delete / Reserve
    /// sub-ops fan to every data BSS node identically (no body to
    /// shard). The same envelope commit rides every per-node batch
    /// so the commit lands replicated across all data nodes in the
    /// volume. Quorum is `k+1`.
    pub async fn put_blocks_batched(
        &self,
        sub_ops: Vec<BssBatchSubOp>,
        commit: BlobCommitInfo,
        trace_id: &TraceId,
    ) -> Result<Vec<Result<(), RpcError>>, DataVgError> {
        // BSS recv buffers are sized for one max-shard payload + one
        // header (`max_payload_size + page_size`, ~512KB + 4KB today --
        // see core/common/configs.zig `blob_size`). Each outer BssBatch
        // RPC's body must therefore fit under ~512KB. With 128KB-block
        // sub_ops, three full payloads (~394KB) fit; a fourth (~525KB)
        // does not. We aim for ~480KB as the split point so a chunk
        // can pack three 128KB blocks plus header overhead with a
        // small safety margin. Multi-MB writes split into multiple
        // sequential BssBatch RPCs and per-entry results concatenate
        // in input order; this still beats the strict serial path
        // (one RPC per block per replica) by roughly
        // `entries_per_chunk` x.
        const MAX_BATCH_BODY_SIZE: usize = 480 * 1024;
        const SUB_HEADER_SIZE: usize = 160;
        if sub_ops.len() > 1 {
            let mut chunks: Vec<Vec<BssBatchSubOp>> = Vec::new();
            let mut current: Vec<BssBatchSubOp> = Vec::new();
            let mut current_size: usize = 0;
            for op in sub_ops.into_iter() {
                let entry_size = SUB_HEADER_SIZE
                    + match &op {
                        BssBatchSubOp::PutDataBlob { body, .. } => body.len(),
                        _ => 0,
                    };
                if !current.is_empty() && current_size + entry_size > MAX_BATCH_BODY_SIZE {
                    chunks.push(std::mem::take(&mut current));
                    current_size = 0;
                }
                current_size += entry_size;
                current.push(op);
            }
            if !current.is_empty() {
                chunks.push(current);
            }
            if chunks.len() > 1 {
                // Multi-chunk: every chunk carries the same envelope
                // commit. Intermediate chunks publish a brief commit
                // window that's immediately overwritten by the next
                // chunk; readers might observe a partial state until
                // the last chunk lands. Acceptable for now -- flushes
                // of this size are rare and short.
                let mut all_results: Vec<Result<(), RpcError>> =
                    Vec::with_capacity(chunks.iter().map(|c| c.len()).sum());
                for chunk in chunks {
                    let part = Box::pin(self.put_blocks_batched(chunk, commit, trace_id)).await?;
                    all_results.extend(part);
                }
                return Ok(all_results);
            }
            // Single chunk -- fall through to the unchunked dispatch.
            let only = chunks.into_iter().next().unwrap();
            return Box::pin(self.put_blocks_batched_one_chunk(only, commit, trace_id)).await;
        }

        self.put_blocks_batched_one_chunk(sub_ops, commit, trace_id)
            .await
    }

    /// Inner dispatch for a single chunk that fits under the BSS
    /// per-message size limit. `put_blocks_batched` slices the input
    /// and calls this for each chunk.
    #[allow(clippy::cognitive_complexity)]
    async fn put_blocks_batched_one_chunk(
        &self,
        sub_ops: Vec<BssBatchSubOp>,
        commit: BlobCommitInfo,
        trace_id: &TraceId,
    ) -> Result<Vec<Result<(), RpcError>>, DataVgError> {
        let n = sub_ops.len();
        let volume_id = commit.blob_guid.volume_id;
        let volume = self.find_volume(volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!("Volume {volume_id} not found in DataVgProxy"))
        })?;
        let (write_quorum, ec_dims) = match &volume.mode {
            VolumeMode::Replicated { w, .. } => (*w as usize, None),
            VolumeMode::ErasureCoded {
                data_shards,
                parity_shards,
            } => {
                // EC volumes shard each PutDataBlob across k+m data
                // BSS nodes. Quorum is k+1. Per-node sub-ops carry
                // shard bodies; the envelope commit is identical
                // across nodes.
                let k = *data_shards as usize;
                let m = *parity_shards as usize;
                (k + 1, Some((k, m)))
            }
        };

        // The override-path heuristic uses the highest version across
        // entries. A batch that covers a fresh write (v=1) plus an
        // override (v>1) drains all replicas because at least one
        // entry needs the post-quorum sync.
        let max_entry_version: u64 = sub_ops
            .iter()
            .map(|op| match op {
                BssBatchSubOp::PutDataBlob { version, .. } => *version,
                BssBatchSubOp::DeleteDataBlob { version, .. } => *version,
                BssBatchSubOp::ReserveBlocks {
                    expected_version, ..
                } => *expected_version,
            })
            .max()
            .unwrap_or(1);

        let rpc_timeout = self.rpc_timeout;
        let trace_id = *trace_id;

        let available_nodes: Vec<_> = volume
            .bss_nodes
            .iter()
            .filter(|node| node.is_available())
            .cloned()
            .collect();
        if available_nodes.len() < write_quorum {
            return Err(DataVgError::QuorumFailure(format!(
                "Insufficient available nodes ({}/{}) for batched write quorum ({})",
                available_nodes.len(),
                volume.bss_nodes.len(),
                write_quorum
            )));
        }
        let total_nodes = available_nodes.len();

        // EC: per-node sub-op lists differ (each carries shards
        //     destined for that node).
        // Replicated: every node receives the same sub_ops vector.
        // `available_nodes` is filtered for circuit-breaker; the
        // per-node-subops vector is indexed by position in
        // `volume.bss_nodes`, then we send only to the available
        // subset. EC routing depends on absolute node position, so
        // we walk the full volume.bss_nodes and skip unavailable
        // ones.
        let per_node_subops = match ec_dims {
            None => None,
            Some((k, m)) => Some(build_ec_per_node_subops(&sub_ops, k, m, commit.blob_guid)?),
        };

        let mut futures = FuturesUnordered::new();
        for (node_idx, node) in volume.bss_nodes.iter().enumerate() {
            if !node.is_available() {
                continue;
            }
            let node = node.clone();
            let node_subops = match &per_node_subops {
                None => sub_ops.clone(),
                Some(per_node) => per_node[node_idx].clone(),
            };
            futures.push(async move {
                let address = node.address.clone();
                let result = node
                    .get_client()
                    .put_bss_batch(node_subops, commit, Some(rpc_timeout), &trace_id, 0)
                    .await;
                (node, address, result)
            });
        }

        // Per-entry success counts feed the quorum check; per-entry
        // last-error keeps a representative error string to surface
        // when an entry can't reach quorum (RpcError isn't Clone, so
        // we stash the formatted message instead and rebuild a generic
        // RpcError at the end).
        let mut per_entry_success: Vec<usize> = vec![0; n];
        let mut per_entry_last_err: Vec<Option<String>> = vec![None; n];
        let mut nodes_responded: usize = 0;
        let mut decided = false;

        while let Some((node, address, result)) = futures.next().await {
            nodes_responded += 1;
            match result {
                Ok(per_entry_results) => {
                    node.record_success();
                    if per_entry_results.len() != n {
                        warn!(
                            "BSS node {address} returned {} entries for batch of {n}; ignoring",
                            per_entry_results.len()
                        );
                    } else {
                        for (i, r) in per_entry_results.into_iter().enumerate() {
                            match r.status {
                                Ok(()) | Err(RpcError::VersionSkipped) => {
                                    per_entry_success[i] += 1;
                                }
                                Err(e) => {
                                    per_entry_last_err[i] = Some(e.to_string());
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    node.record_failure();
                    let err_str = format!("{address}: {e}");
                    warn!("BSS batch RPC to {address} failed: {e}");
                    // A whole-batch transport failure means none of
                    // this node's entries count; record the err as a
                    // last-resort failure surface for any entry that
                    // never gets an Ok.
                    for slot in per_entry_last_err.iter_mut() {
                        if slot.is_none() {
                            *slot = Some(err_str.clone());
                        }
                    }
                }
            }

            // Early termination: every entry is decided once it has
            // either reached quorum, or its remaining nodes can't
            // possibly push it to quorum.
            let remaining = total_nodes - nodes_responded;
            decided = per_entry_success
                .iter()
                .all(|&s| s >= write_quorum || s + remaining < write_quorum);
            if decided && max_entry_version == 1 {
                break;
            }
            if decided && max_entry_version > 1 && nodes_responded == total_nodes {
                break;
            }
        }

        // Drain remaining replicas:
        //   - override path: synchronously, like put_data_blob_vectored
        //   - first-write path: in the background to record success/failure
        //     telemetry without holding up the caller.
        if !decided || nodes_responded < total_nodes {
            if max_entry_version > 1 {
                while let Some((node, address, result)) = futures.next().await {
                    match result {
                        Ok(_) => {
                            node.record_success();
                            debug!("Override batched write to {address} completed");
                        }
                        Err(e) => {
                            node.record_failure();
                            warn!("Override batched write to {address} failed: {e}");
                        }
                    }
                }
            } else {
                spawn_background(async move {
                    while let Some((node, address, result)) = futures.next().await {
                        match result {
                            Ok(_) => {
                                node.record_success();
                                debug!("Background batched write to {address} completed");
                            }
                            Err(e) => {
                                node.record_failure();
                                warn!("Background batched write to {address} failed: {e}");
                            }
                        }
                    }
                });
            }
        }

        let entry_results: Vec<Result<(), RpcError>> = per_entry_success
            .iter()
            .zip(per_entry_last_err.into_iter())
            .map(|(&succ, err_msg)| {
                if succ >= write_quorum {
                    Ok(())
                } else {
                    Err(RpcError::InternalResponseError(format!(
                        "BssBatch entry quorum not met ({succ}/{write_quorum}){}",
                        err_msg.map(|m| format!(": {m}")).unwrap_or_default()
                    )))
                }
            })
            .collect();
        Ok(entry_results)
    }

    /// Read the BSS-recorded commit info for a blob. Fans
    /// `GetBlobInfo` to every available BSS node in the volume and
    /// picks the response with the highest `blob_version`. For EC
    /// volumes the commit is replicated (not sharded), so any data
    /// node's reply suffices. Returns `Ok(None)` when no replica
    /// has a commit recorded (new blob, or partition that missed
    /// every publish).
    pub async fn get_blob_info(
        &self,
        blob_guid: DataBlobGuid,
        expected_version: u64,
        trace_id: &TraceId,
    ) -> Result<Option<BlobInfo>, DataVgError> {
        let volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!(
                "Volume {} not found in DataVgProxy",
                blob_guid.volume_id
            ))
        })?;
        let rpc_timeout = self.rpc_timeout;
        let trace_id = *trace_id;

        let mut futures = FuturesUnordered::new();
        for node in &volume.bss_nodes {
            if !node.is_available() {
                continue;
            }
            let node = node.clone();
            futures.push(async move {
                let address = node.address.clone();
                let result = node
                    .get_client()
                    .get_blob_info(blob_guid, expected_version, Some(rpc_timeout), &trace_id, 0)
                    .await;
                (node, address, result)
            });
        }

        let mut best: Option<BlobInfo> = None;
        while let Some((node, _address, result)) = futures.next().await {
            match result {
                Ok(Some(info)) => {
                    node.record_success();
                    best = Some(match best {
                        None => info,
                        Some(b) if info.blob_version > b.blob_version => info,
                        Some(b) => b,
                    });
                }
                Ok(None) => {
                    node.record_success();
                }
                Err(e) => {
                    node.record_failure();
                    debug!("get_blob_info RPC error: {}", e);
                }
            }
        }
        Ok(best)
    }

    /// Enumerate the BSS-side block-level entries for `blob_guid`
    /// over `[first_block, first_block + block_count)`. Tries
    /// available replicas in random order and returns the first
    /// successful response. The result intentionally is whatever a
    /// single replica saw -- callers that need a coherent view (scan
    /// & repair) should query each replica directly.
    pub async fn list_blob_blocks(
        &self,
        blob_guid: DataBlobGuid,
        first_block: u32,
        block_count: u32,
        trace_id: &TraceId,
    ) -> Result<Vec<bss_codec::list_blob_blocks_response::BlobBlockEntry>, DataVgError> {
        let volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!("Volume {} not found", blob_guid.volume_id))
        })?;
        if let VolumeMode::ErasureCoded { .. } = &volume.mode {
            // EC: every parity / data shard sees the same key space; the
            // first available node responds.
        }

        let mut available_nodes: Vec<_> = volume
            .bss_nodes
            .iter()
            .filter(|node| node.is_available())
            .cloned()
            .collect();
        available_nodes.shuffle(&mut rand::rng());

        if available_nodes.is_empty() {
            return Err(DataVgError::QuorumFailure(
                "No available BSS nodes for list_blob_blocks".to_string(),
            ));
        }

        let trace_id = *trace_id;
        let rpc_timeout = self.rpc_timeout;
        let mut last_err: Option<String> = None;
        for node in &available_nodes {
            let result = node
                .get_client()
                .list_blob_blocks(
                    blob_guid,
                    first_block,
                    block_count,
                    Some(rpc_timeout),
                    &trace_id,
                    0,
                )
                .await;
            match result {
                Ok(entries) => {
                    node.record_success();
                    return Ok(entries);
                }
                Err(e) => {
                    node.record_failure();
                    last_err = Some(format!("{}: {}", node.address, e));
                }
            }
        }
        Err(DataVgError::QuorumFailure(format!(
            "list_blob_blocks: every replica failed ({})",
            last_err.unwrap_or_default()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::Volume;

    #[test]
    fn ec_volume_id_range() {
        assert!(!Volume::is_ec_volume_id(0));
        assert!(!Volume::is_ec_volume_id(1));
        assert!(!Volume::is_ec_volume_id(0x7FFF));
        assert!(Volume::is_ec_volume_id(0x8000));
        assert!(Volume::is_ec_volume_id(0x8001));
        assert!(Volume::is_ec_volume_id(0xFFFE));
        assert!(!Volume::is_ec_volume_id(0xFFFF));
    }

    #[test]
    fn ec_rotation_deterministic() {
        let blob_id = Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef").unwrap();
        let total = 6u32;
        let r1 = ec_rotation(&blob_id, total);
        let r2 = ec_rotation(&blob_id, total);
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
            let r = ec_rotation(&blob_id, total);
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

        let padded_len = ec_padded_len(original_len, k);
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
            "volumes": [{
                "volume_id": 32768,
                "bss_nodes": [
                    {"node_id":"bss-0","ip":"127.0.0.1","port":8088},
                    {"node_id":"bss-1","ip":"127.0.0.1","port":8089},
                    {"node_id":"bss-2","ip":"127.0.0.1","port":8090},
                    {"node_id":"bss-3","ip":"127.0.0.1","port":8091},
                    {"node_id":"bss-4","ip":"127.0.0.1","port":8092},
                    {"node_id":"bss-5","ip":"127.0.0.1","port":8093}
                ],
                "mode": {"type":"erasure_coded","data_shards":4,"parity_shards":2}
            }]
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.volumes.len(), 1);

        let ec = &info.volumes[0];
        assert_eq!(ec.volume_id, 0x8000);
        assert!(ec.is_ec());
        if let VolumeMode::ErasureCoded {
            data_shards,
            parity_shards,
        } = &ec.mode
        {
            assert_eq!(*data_shards, 4);
            assert_eq!(*parity_shards, 2);
        }
        assert_eq!(ec.bss_nodes.len(), 6);
    }

    #[test]
    fn parse_replicated_config_json() {
        let json = r#"{
            "volumes": [{"volume_id":1,"bss_nodes":[{"node_id":"bss-0","ip":"127.0.0.1","port":8088}],"mode":{"type":"replicated","n":1,"r":1,"w":1}}]
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.volumes.len(), 1);
        assert!(!info.volumes[0].is_ec());
    }

    #[test]
    fn datavgproxy_init_ec_only() {
        let json = r#"{
            "volumes": [{
                "volume_id": 32768,
                "bss_nodes": [
                    {"node_id":"bss-0","ip":"127.0.0.1","port":18088},
                    {"node_id":"bss-1","ip":"127.0.0.1","port":18089},
                    {"node_id":"bss-2","ip":"127.0.0.1","port":18090},
                    {"node_id":"bss-3","ip":"127.0.0.1","port":18091},
                    {"node_id":"bss-4","ip":"127.0.0.1","port":18092},
                    {"node_id":"bss-5","ip":"127.0.0.1","port":18093}
                ],
                "mode": {"type":"erasure_coded","data_shards":4,"parity_shards":2}
            }]
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        let proxy = DataVgProxy::new(info, Duration::from_secs(5), Duration::from_secs(5)).unwrap();

        // Should select EC volume
        let guid = proxy.create_data_blob_guid();
        assert_eq!(guid.volume_id, 0x8000);
        assert!(Volume::is_ec_volume_id(guid.volume_id));
    }

    #[test]
    fn datavgproxy_init_ec_invalid_node_count() {
        let json = r#"{
            "volumes": [{
                "volume_id": 32768,
                "bss_nodes": [
                    {"node_id":"bss-0","ip":"127.0.0.1","port":18088},
                    {"node_id":"bss-1","ip":"127.0.0.1","port":18089}
                ],
                "mode": {"type":"erasure_coded","data_shards":4,"parity_shards":2}
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
            "volumes": [{
                "volume_id": 65535,
                "bss_nodes": [
                    {"node_id":"bss-0","ip":"127.0.0.1","port":18088},
                    {"node_id":"bss-1","ip":"127.0.0.1","port":18089},
                    {"node_id":"bss-2","ip":"127.0.0.1","port":18090},
                    {"node_id":"bss-3","ip":"127.0.0.1","port":18091},
                    {"node_id":"bss-4","ip":"127.0.0.1","port":18092},
                    {"node_id":"bss-5","ip":"127.0.0.1","port":18093}
                ],
                "mode": {"type":"erasure_coded","data_shards":4,"parity_shards":2}
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
            "volumes": [{
                "volume_id": 32768,
                "bss_nodes": [
                    {"node_id":"bss-0","ip":"127.0.0.1","port":18088},
                    {"node_id":"bss-1","ip":"127.0.0.1","port":18089}
                ],
                "mode": {"type":"erasure_coded","data_shards":0,"parity_shards":2}
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
            "volumes": [{
                "volume_id": 32768,
                "bss_nodes": [
                    {"node_id":"bss-0","ip":"127.0.0.1","port":18088},
                    {"node_id":"bss-1","ip":"127.0.0.1","port":18089},
                    {"node_id":"bss-2","ip":"127.0.0.1","port":18090},
                    {"node_id":"bss-3","ip":"127.0.0.1","port":18091}
                ],
                "mode": {"type":"erasure_coded","data_shards":4,"parity_shards":0}
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
        let json = r#"{"volumes": []}"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        let result = DataVgProxy::new(info, Duration::from_secs(5), Duration::from_secs(5));
        assert!(result.is_err());
        let err = result.err().unwrap().to_string();
        assert!(err.contains("No volumes"), "err: {}", err);
    }

    #[test]
    fn create_data_blob_guid_with_preference_uses_ec_when_available() {
        let json = r#"{
            "volumes": [
                {
                    "volume_id": 1,
                    "bss_nodes": [
                        {"node_id":"bss-0","ip":"127.0.0.1","port":18088}
                    ],
                    "mode": {"type":"replicated","n":1,"r":1,"w":1}
                },
                {
                    "volume_id": 32768,
                    "bss_nodes": [
                        {"node_id":"bss-0","ip":"127.0.0.1","port":18088},
                        {"node_id":"bss-1","ip":"127.0.0.1","port":18089},
                        {"node_id":"bss-2","ip":"127.0.0.1","port":18090},
                        {"node_id":"bss-3","ip":"127.0.0.1","port":18091},
                        {"node_id":"bss-4","ip":"127.0.0.1","port":18092},
                        {"node_id":"bss-5","ip":"127.0.0.1","port":18093}
                    ],
                    "mode": {"type":"erasure_coded","data_shards":4,"parity_shards":2}
                }
            ]
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        let proxy = DataVgProxy::new(info, Duration::from_secs(5), Duration::from_secs(5)).unwrap();
        let guid = proxy.create_data_blob_guid_with_preference(true);
        assert_eq!(guid.volume_id, 0x8000);
    }
}
