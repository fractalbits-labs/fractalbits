use crate::DataVgError;
use bytes::Bytes;
use data_types::ec_utils::{ec_padded_len, ec_rotation};
use data_types::{DataBlobGuid, DataVgInfo, TraceId, Volume, VolumeMode};
use futures::stream::{FuturesUnordered, StreamExt};
use metrics_wrapper::{counter, histogram};
use rand::RngExt;
use rand::seq::{IndexedRandom, SliceRandom};
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
use tracing::{debug, error, warn};
use uuid::Uuid;

type ListedGeneration = (u32, GenerationIdentity, bool);
type ListedGenerationsByBlock = std::collections::HashMap<u32, Vec<(GenerationIdentity, bool)>>;

const GENERATION_LIST_PAGE_SIZE: u32 = 512;
const GENERATION_CANDIDATE_CAP: u32 = GENERATION_LIST_PAGE_SIZE;
const ENTRY_TYPE_RESERVED: u32 = 1;
const SUPERSEDED_GENERATION_READER_GRACE: Duration = Duration::from_secs(15 * 60);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct GenerationIdentity {
    version: u64,
    write_token: u64,
}

#[derive(Clone, Debug)]
enum GenerationFragment {
    Data {
        identity: GenerationIdentity,
        body: Bytes,
    },
    Tombstone {
        identity: GenerationIdentity,
    },
    Reserved {
        identity: GenerationIdentity,
    },
}

enum NodeGenerationLookup {
    Missing,
    Fragment(GenerationFragment),
    Unreadable(GenerationIdentity),
}

impl GenerationFragment {
    fn identity(&self) -> GenerationIdentity {
        match self {
            Self::Data { identity, .. }
            | Self::Tombstone { identity }
            | Self::Reserved { identity } => *identity,
        }
    }
}

fn generation_key_prefix(blob_guid: DataBlobGuid, block_number: u32) -> String {
    format!(
        "/d{}/{}-p{:08x}-rv",
        blob_guid.volume_id, blob_guid.blob_id, block_number
    )
}

fn generation_scan_marker(prefix: &str, committed_version: u64) -> String {
    let min_reverse = u64::MAX - committed_version;
    if min_reverse == 0 {
        String::new()
    } else {
        format!("{prefix}{:016x}-tffffffffffffffff", min_reverse - 1)
    }
}

fn parse_generation_key(key: &str) -> Option<(u32, GenerationIdentity)> {
    let key = key.trim_end_matches('\0');
    let (_, suffix) = key.rsplit_once("-p")?;
    if suffix.len() != 8 + 3 + 16 + 2 + 16 {
        return None;
    }
    let block_number = u32::from_str_radix(&suffix[..8], 16).ok()?;
    if &suffix[8..11] != "-rv" || &suffix[27..29] != "-t" {
        return None;
    }
    let reverse_version = u64::from_str_radix(&suffix[11..27], 16).ok()?;
    let write_token = u64::from_str_radix(&suffix[29..45], 16).ok()?;
    Some((
        block_number,
        GenerationIdentity {
            version: u64::MAX - reverse_version,
            write_token,
        },
    ))
}

fn select_generation_candidate<'a>(
    entries: impl IntoIterator<Item = (&'a str, bool, u32, u32)>,
    block_number: u32,
    committed_version: u64,
    committed_token: u64,
    has_more: bool,
) -> Result<Option<(GenerationIdentity, bool, u32, u32)>, DataVgError> {
    let mut chosen: Option<(GenerationIdentity, bool, u32, u32)> = None;
    let mut older_cohort_closed = false;
    for (key, is_deleted, total_bytes, entry_type) in entries {
        let Some((parsed_block, identity)) = parse_generation_key(key) else {
            continue;
        };
        if parsed_block != block_number || identity.version > committed_version {
            continue;
        }
        if identity.version == committed_version {
            if identity.write_token == committed_token {
                return Ok(Some((identity, is_deleted, total_bytes, entry_type)));
            }
            continue;
        }

        match chosen {
            None => chosen = Some((identity, is_deleted, total_bytes, entry_type)),
            Some((current, _, _, _)) if identity.version == current.version => {
                if identity.write_token != current.write_token {
                    return Err(DataVgError::AmbiguousOlderTokens {
                        version: identity.version,
                    });
                }
            }
            Some((current, _, _, _)) if identity.version < current.version => {
                older_cohort_closed = true;
                break;
            }
            Some(_) => {}
        }
    }

    if has_more && (chosen.is_none() || !older_cohort_closed) {
        return Err(DataVgError::GenerationCandidateLimit {
            limit: GENERATION_CANDIDATE_CAP,
        });
    }
    Ok(chosen)
}

fn select_observed_generation(
    identities: &[GenerationIdentity],
) -> Result<Option<GenerationIdentity>, DataVgError> {
    let Some(max_version) = identities.iter().map(|identity| identity.version).max() else {
        return Ok(None);
    };
    let tokens: std::collections::HashSet<u64> = identities
        .iter()
        .filter(|identity| identity.version == max_version)
        .map(|identity| identity.write_token)
        .collect();
    if tokens.len() != 1 {
        return Err(DataVgError::AmbiguousOlderTokens {
            version: max_version,
        });
    }
    Ok(Some(GenerationIdentity {
        version: max_version,
        write_token: *tokens.iter().next().expect("one token after length check"),
    }))
}

fn listed_block_has_data_at_or_before(
    responses: &[ListedGenerationsByBlock],
    block_number: u32,
    committed_version: u64,
    committed_token: u64,
    read_threshold: usize,
) -> Result<bool, DataVgError> {
    let mut candidates = Vec::new();
    for response in responses {
        let Some(entries) = response.get(&block_number) else {
            continue;
        };
        let mut exact = None;
        let mut older = Vec::new();
        for &(identity, is_deleted) in entries {
            if identity.version > committed_version {
                continue;
            }
            if identity.version == committed_version {
                if identity.write_token == committed_token {
                    exact = Some((identity, is_deleted));
                }
                continue;
            }
            older.push((identity, is_deleted));
        }
        let older = if exact.is_none() {
            let max_version = older.iter().map(|(identity, _)| identity.version).max();
            match max_version {
                Some(version) => {
                    let matching: Vec<_> = older
                        .iter()
                        .filter(|(identity, _)| identity.version == version)
                        .collect();
                    let tokens: std::collections::HashSet<u64> = matching
                        .iter()
                        .map(|(identity, _)| identity.write_token)
                        .collect();
                    if tokens.len() != 1 {
                        return Err(DataVgError::AmbiguousOlderTokens { version });
                    }
                    matching.first().map(|candidate| **candidate)
                }
                None => None,
            }
        } else {
            None
        };
        if let Some(candidate) = exact.or(older) {
            candidates.push(candidate);
        }
    }
    let Some(max_version) = candidates
        .iter()
        .map(|(identity, _)| identity.version)
        .max()
    else {
        return Ok(false);
    };
    let tokens: std::collections::HashSet<u64> = candidates
        .iter()
        .filter(|(identity, _)| identity.version == max_version)
        .map(|(identity, _)| identity.write_token)
        .collect();
    if tokens.len() != 1 {
        return Err(DataVgError::AmbiguousOlderTokens {
            version: max_version,
        });
    }
    let token = *tokens.iter().next().expect("one token after length check");
    let cohort: Vec<_> = candidates
        .iter()
        .filter(|(identity, _)| identity.version == max_version && identity.write_token == token)
        .collect();
    if cohort.len() < read_threshold {
        return Err(DataVgError::StaleVersion {
            expected: max_version,
        });
    }
    let deleted = cohort.iter().filter(|(_, is_deleted)| *is_deleted).count();
    if deleted != 0 && deleted != cohort.len() {
        return Err(DataVgError::Corrupted);
    }
    Ok(deleted == 0)
}

fn index_listed_generations(entries: Vec<ListedGeneration>) -> ListedGenerationsByBlock {
    let mut by_block = ListedGenerationsByBlock::new();
    for (block_number, identity, is_deleted) in entries {
        by_block
            .entry(block_number)
            .or_default()
            .push((identity, is_deleted));
    }
    by_block
}

#[cfg(feature = "tokio-runtime")]
fn spawn_background<F: std::future::Future<Output = ()> + Send + 'static>(fut: F) {
    tokio::spawn(fut);
}

#[cfg(all(feature = "compio-runtime", not(feature = "tokio-runtime")))]
fn spawn_background<F: std::future::Future<Output = ()> + 'static>(fut: F) {
    compio_runtime::spawn(fut).detach();
}

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
    mode: VolumeMode,
    /// Number of write requests currently in flight against this volume.
    /// Used as the load signal for Power-of-Two-Choices volume selection.
    inflight: AtomicU64,
}

/// RAII guard that decrements a volume's in-flight write counter on drop,
/// so every early return / `?` in the write path is accounted for.
struct InflightGuard<'a> {
    counter: &'a AtomicU64,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Policy used to pick a volume out of a candidate tier on the write path.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VolumeSelectionPolicy {
    /// Rotate through the candidates with a shared counter. Spreads writes
    /// evenly by count but ignores how busy each volume currently is.
    RoundRobin,
    /// Power-of-Two-Choices over the in-flight write counters: sample two
    /// distinct candidates and route to the one with fewer in-flight writes.
    /// Default, because it steers away from temporarily slow/busy volumes.
    #[default]
    LeastQd,
}

pub struct DataVgProxy {
    volumes: Vec<VolumeWithNodes>,
    round_robin_counter: AtomicU64,
    rpc_timeout: Duration,
    policy: VolumeSelectionPolicy,
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
                VolumeMode::Replicated { n, r, w } => {
                    if *n as usize != volume.bss_nodes.len() {
                        return Err(DataVgError::InitializationError(format!(
                            "Volume {} has n={} but {} nodes",
                            volume.volume_id,
                            n,
                            volume.bss_nodes.len()
                        )));
                    }
                    let majority = *n / 2 + 1;
                    if *r < majority {
                        return Err(DataVgError::InitializationError(format!(
                            "Volume {} has r={} below majority {}",
                            volume.volume_id, r, majority
                        )));
                    }
                    if *w < majority {
                        return Err(DataVgError::InitializationError(format!(
                            "Volume {} has w={} below majority {}",
                            volume.volume_id, w, majority
                        )));
                    }
                    if *r > *n || *w > *n {
                        return Err(DataVgError::InitializationError(format!(
                            "Volume {} has n={}, r={}, w={}",
                            volume.volume_id, n, r, w
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
                inflight: AtomicU64::new(0),
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
            policy: VolumeSelectionPolicy::default(),
        })
    }

    /// Override the volume selection policy (defaults to
    /// [`VolumeSelectionPolicy::LeastQd`]).
    pub fn with_selection_policy(mut self, policy: VolumeSelectionPolicy) -> Self {
        self.policy = policy;
        self
    }

    pub fn select_volume_for_blob_with_preference(&self, prefer_ec: bool) -> u16 {
        // Build the candidate tier. Large objects (prefer_ec) route to EC
        // volumes when any exist; otherwise everything routes to replicated
        // volumes. If the preferred tier is empty we fall back to whatever is
        // configured so a single-tier deployment still works.
        let mut candidates: Vec<&VolumeWithNodes> = Vec::new();
        if prefer_ec {
            candidates.extend(
                self.volumes
                    .iter()
                    .filter(|v| matches!(v.mode, VolumeMode::ErasureCoded { .. })),
            );
        }
        if candidates.is_empty() {
            candidates.extend(
                self.volumes
                    .iter()
                    .filter(|v| matches!(v.mode, VolumeMode::Replicated { .. })),
            );
        }
        if candidates.is_empty() {
            candidates.extend(self.volumes.iter());
        }

        self.pick_volume(&candidates).volume_id
    }

    /// Pick a volume out of a candidate tier according to the configured
    /// [`VolumeSelectionPolicy`].
    fn pick_volume<'a>(&self, candidates: &[&'a VolumeWithNodes]) -> &'a VolumeWithNodes {
        match candidates.len() {
            0 => unreachable!("DataVgProxy always has at least one volume configured"),
            1 => return candidates[0],
            _ => {}
        }

        match self.policy {
            VolumeSelectionPolicy::RoundRobin => self.pick_volume_round_robin(candidates),
            VolumeSelectionPolicy::LeastQd => self.pick_volume_least_qd(candidates),
        }
    }

    /// Rotate through the candidates with the shared round-robin counter.
    fn pick_volume_round_robin<'a>(
        &self,
        candidates: &[&'a VolumeWithNodes],
    ) -> &'a VolumeWithNodes {
        let counter = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) as usize;
        candidates[counter % candidates.len()]
    }

    /// Power-of-Two-Choices selection over a candidate tier: sample two
    /// distinct volumes and route to the one with fewer in-flight writes.
    ///
    /// Sampling only two keeps the decision O(1) regardless of how many
    /// volumes exist, while still collapsing worst-case load imbalance from
    /// ~log N (blind round-robin / random) down to ~log log N. The benefit
    /// grows as volumes are added, which is exactly the regime we expect.
    /// Equal-load ties pick one of the two samples at random so identically
    /// loaded volumes are not biased towards the lower index.
    fn pick_volume_least_qd<'a>(&self, candidates: &[&'a VolumeWithNodes]) -> &'a VolumeWithNodes {
        let len = candidates.len();
        let mut rng = rand::rng();
        let i = rng.random_range(0..len);
        // Pick a second, distinct index uniformly over the remaining volumes.
        let mut j = rng.random_range(0..len - 1);
        if j >= i {
            j += 1;
        }

        let a = candidates[i];
        let b = candidates[j];
        let a_load = a.inflight.load(Ordering::Relaxed);
        let b_load = b.inflight.load(Ordering::Relaxed);

        if a_load < b_load {
            a
        } else if b_load < a_load {
            b
        } else if rng.random_bool(0.5) {
            a
        } else {
            b
        }
    }

    pub fn select_volume_for_blob(&self) -> u16 {
        self.select_volume_for_blob_with_preference(false)
    }

    fn find_volume(&self, volume_id: u16) -> Option<&VolumeWithNodes> {
        self.volumes.iter().find(|v| v.volume_id == volume_id)
    }
    #[allow(clippy::too_many_arguments)]
    async fn get_generation_from_node_instance(
        &self,
        bss_node: &BssNode,
        blob_guid: DataBlobGuid,
        block_number: u32,
        committed_version: u64,
        committed_token: u64,
        trace_id: &TraceId,
    ) -> Result<NodeGenerationLookup, DataVgError> {
        let prefix = generation_key_prefix(blob_guid, block_number);
        let marker = generation_scan_marker(&prefix, committed_version);
        let page = bss_node
            .get_client()
            .list_data_blobs(
                blob_guid.volume_id,
                &prefix,
                &marker,
                GENERATION_CANDIDATE_CAP,
                Some(self.rpc_timeout),
                trace_id,
                0,
                false,
            )
            .await?;

        let chosen = select_generation_candidate(
            page.blobs
                .iter()
                .filter(|entry| !entry.is_physically_deleted)
                .map(|entry| {
                    (
                        entry.key.as_str(),
                        entry.is_deleted,
                        entry.total_bytes,
                        entry.entry_type,
                    )
                }),
            block_number,
            committed_version,
            committed_token,
            page.has_more,
        )?;
        if let Some((identity, is_deleted, total_bytes, entry_type)) = chosen {
            if is_deleted {
                return Ok(NodeGenerationLookup::Fragment(
                    GenerationFragment::Tombstone { identity },
                ));
            }
            if entry_type == ENTRY_TYPE_RESERVED {
                return Ok(NodeGenerationLookup::Fragment(
                    GenerationFragment::Reserved { identity },
                ));
            }
            let mut body = Bytes::new();
            return match bss_node
                .get_client()
                .get_data_blob(
                    blob_guid,
                    block_number,
                    &mut body,
                    total_bytes as usize,
                    identity.version,
                    identity.write_token,
                    Some(self.rpc_timeout),
                    trace_id,
                    0,
                )
                .await
            {
                Ok(()) => Ok(NodeGenerationLookup::Fragment(GenerationFragment::Data {
                    identity,
                    body,
                })),
                Err(error) => {
                    debug!(
                        node = %bss_node.address,
                        version = identity.version,
                        write_token = identity.write_token,
                        error = %error,
                        "listed generation body is unavailable"
                    );
                    Ok(NodeGenerationLookup::Unreadable(identity))
                }
            };
        }

        Ok(NodeGenerationLookup::Missing)
    }

    async fn list_generations_from_node_instance(
        &self,
        bss_node: &BssNode,
        blob_guid: DataBlobGuid,
        first_block: u32,
        block_count: u32,
        trace_id: &TraceId,
    ) -> Result<Vec<ListedGeneration>, DataVgError> {
        let prefix = format!("/d{}/{}-p", blob_guid.volume_id, blob_guid.blob_id);
        let last_block = first_block.saturating_add(block_count);
        let mut marker = String::new();
        let mut generations = Vec::new();
        let mut per_block_count = std::collections::HashMap::<u32, u32>::new();
        loop {
            let page = bss_node
                .get_client()
                .list_data_blobs(
                    blob_guid.volume_id,
                    &prefix,
                    &marker,
                    GENERATION_LIST_PAGE_SIZE,
                    Some(self.rpc_timeout),
                    trace_id,
                    0,
                    false,
                )
                .await?;
            let next_marker = if page.next_marker.is_empty() {
                page.blobs.last().map(|entry| entry.key.clone())
            } else {
                Some(page.next_marker.clone())
            };
            for entry in page.blobs {
                if entry.is_physically_deleted {
                    continue;
                }
                let Some((block_number, identity)) = parse_generation_key(&entry.key) else {
                    continue;
                };
                if block_number < first_block || block_number >= last_block {
                    continue;
                }
                let count = per_block_count.entry(block_number).or_default();
                *count += 1;
                if *count > GENERATION_CANDIDATE_CAP {
                    return Err(DataVgError::GenerationCandidateLimit {
                        limit: GENERATION_CANDIDATE_CAP,
                    });
                }
                generations.push((block_number, identity, entry.is_deleted));
            }
            if !page.has_more {
                break;
            }
            let Some(next_marker) = next_marker else {
                return Err(DataVgError::Internal(
                    "generation list has_more without a marker".to_string(),
                ));
            };
            if next_marker <= marker {
                return Err(DataVgError::Internal(
                    "generation list marker did not advance".to_string(),
                ));
            }
            marker = next_marker;
        }
        Ok(generations)
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

    async fn delete_fenced_token_from_node(
        &self,
        node: &BssNode,
        blob_guid: DataBlobGuid,
        version: u64,
        write_token: u64,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let prefix = format!("/d{}/{}-p", blob_guid.volume_id, blob_guid.blob_id);
        let mut marker = String::new();
        loop {
            let page = node
                .get_client()
                .list_data_blobs_including_fenced(
                    blob_guid.volume_id,
                    &prefix,
                    &marker,
                    GENERATION_LIST_PAGE_SIZE,
                    Some(self.rpc_timeout),
                    trace_id,
                    0,
                    true,
                )
                .await?;
            let has_more = page.has_more;
            let next_marker = if page.next_marker.is_empty() {
                page.blobs.last().map(|entry| entry.key.clone())
            } else {
                Some(page.next_marker.clone())
            };

            for entry in page.blobs {
                if entry.is_physically_deleted {
                    continue;
                }
                let Some((block_number, identity)) = parse_generation_key(&entry.key) else {
                    continue;
                };
                if identity.version != version || identity.write_token != write_token {
                    continue;
                }
                match node
                    .get_client()
                    .delete_data_blob(
                        blob_guid,
                        block_number,
                        version,
                        write_token,
                        Some(self.rpc_timeout),
                        trace_id,
                        0,
                    )
                    .await
                {
                    Ok(()) | Err(RpcError::NotFound) | Err(RpcError::VersionSkipped) => {}
                    Err(error) => return Err(error.into()),
                }
            }

            if !has_more {
                return Ok(());
            }
            let Some(next_marker) = next_marker else {
                return Err(DataVgError::Internal(
                    "fenced generation list has_more without a marker".to_string(),
                ));
            };
            if next_marker <= marker {
                return Err(DataVgError::Internal(
                    "fenced generation list marker did not advance".to_string(),
                ));
            }
            marker = next_marker;
        }
    }

    async fn delete_generations_before_from_node(
        node: &BssNode,
        blob_guid: DataBlobGuid,
        block_number: u32,
        keep_version: u64,
        rpc_timeout: Duration,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let prefix = generation_key_prefix(blob_guid, block_number);
        let mut marker = String::new();
        loop {
            let page = node
                .get_client()
                .list_data_blobs_including_fenced(
                    blob_guid.volume_id,
                    &prefix,
                    &marker,
                    GENERATION_LIST_PAGE_SIZE,
                    Some(rpc_timeout),
                    trace_id,
                    0,
                    true,
                )
                .await?;
            let has_more = page.has_more;
            let next_marker = if page.next_marker.is_empty() {
                page.blobs.last().map(|entry| entry.key.clone())
            } else {
                Some(page.next_marker.clone())
            };

            for entry in page.blobs {
                if entry.is_physically_deleted {
                    continue;
                }
                let Some((parsed_block, identity)) = parse_generation_key(&entry.key) else {
                    continue;
                };
                if parsed_block != block_number || identity.version >= keep_version {
                    continue;
                }
                match node
                    .get_client()
                    .delete_data_blob(
                        blob_guid,
                        block_number,
                        identity.version,
                        identity.write_token,
                        Some(rpc_timeout),
                        trace_id,
                        0,
                    )
                    .await
                {
                    Ok(()) | Err(RpcError::NotFound) | Err(RpcError::VersionSkipped) => {}
                    Err(error) => return Err(error.into()),
                }
            }

            if !has_more {
                break;
            }
            let Some(next_marker) = next_marker else {
                return Err(DataVgError::Internal(
                    "generation GC list has_more without a marker".to_string(),
                ));
            };
            if next_marker <= marker {
                return Err(DataVgError::Internal(
                    "generation GC list marker did not advance".to_string(),
                ));
            }
            marker = next_marker;
        }
        Ok(())
    }

    async fn delete_all_generations_from_node(
        &self,
        node: &BssNode,
        blob_guid: DataBlobGuid,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let prefix = format!("/d{}/{}-p", blob_guid.volume_id, blob_guid.blob_id);
        let mut marker = String::new();
        loop {
            let page = node
                .get_client()
                .list_data_blobs_including_fenced(
                    blob_guid.volume_id,
                    &prefix,
                    &marker,
                    GENERATION_LIST_PAGE_SIZE,
                    Some(self.rpc_timeout),
                    trace_id,
                    0,
                    true,
                )
                .await?;
            let has_more = page.has_more;
            let next_marker = if page.next_marker.is_empty() {
                page.blobs.last().map(|entry| entry.key.clone())
            } else {
                Some(page.next_marker.clone())
            };

            for entry in page.blobs {
                if entry.is_physically_deleted {
                    continue;
                }
                let Some((block_number, identity)) = parse_generation_key(&entry.key) else {
                    continue;
                };
                let (version, write_token) = (identity.version, identity.write_token);
                match node
                    .get_client()
                    .delete_data_blob(
                        blob_guid,
                        block_number,
                        version,
                        write_token,
                        Some(self.rpc_timeout),
                        trace_id,
                        0,
                    )
                    .await
                {
                    Ok(()) | Err(RpcError::NotFound) | Err(RpcError::VersionSkipped) => {}
                    Err(error) => return Err(error.into()),
                }
            }

            if !has_more {
                return Ok(());
            }
            let Some(next_marker) = next_marker else {
                return Err(DataVgError::Internal(
                    "blob generation list has_more without a marker".to_string(),
                ));
            };
            if next_marker <= marker {
                return Err(DataVgError::Internal(
                    "blob generation list marker did not advance".to_string(),
                ));
            }
            marker = next_marker;
        }
    }

    pub async fn delete_all_blob_generations(
        &self,
        blob_guid: DataBlobGuid,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!(
                "Volume {} not found in DataVgProxy",
                blob_guid.volume_id
            ))
        })?;
        let mut requests = FuturesUnordered::new();
        for node in volume.bss_nodes.iter().cloned() {
            requests.push(async move {
                let result = self
                    .delete_all_generations_from_node(&node, blob_guid, trace_id)
                    .await;
                (node, result)
            });
        }

        let mut failures = Vec::new();
        while let Some((node, result)) = requests.next().await {
            match result {
                Ok(()) => node.record_success(),
                Err(error) => {
                    node.record_failure();
                    failures.push(format!("{}: {}", node.address, error));
                }
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DataVgError::QuorumFailure(format!(
                "Blob generation cleanup failed on placement nodes: {}",
                failures.join("; ")
            )))
        }
    }

    pub fn enqueue_superseded_generation_gc(
        &self,
        blob_guid: DataBlobGuid,
        block_numbers: Vec<u32>,
        keep_version: u64,
        trace_id: TraceId,
    ) -> Result<(), DataVgError> {
        if block_numbers.is_empty() || keep_version == 0 {
            return Ok(());
        }
        let volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!(
                "Volume {} not found in DataVgProxy",
                blob_guid.volume_id
            ))
        })?;
        let nodes = volume.bss_nodes.clone();
        let rpc_timeout = self.rpc_timeout;
        let block_numbers = Arc::new(block_numbers);
        spawn_background(async move {
            rpc_client_common::rpc_sleep(SUPERSEDED_GENERATION_READER_GRACE).await;
            let mut requests = FuturesUnordered::new();
            for node in nodes {
                let block_numbers = block_numbers.clone();
                requests.push(async move {
                    for block_number in block_numbers.iter().copied() {
                        if let Err(error) = Self::delete_generations_before_from_node(
                            &node,
                            blob_guid,
                            block_number,
                            keep_version,
                            rpc_timeout,
                            &trace_id,
                        )
                        .await
                        {
                            node.record_failure();
                            warn!(
                                node = %node.address,
                                %blob_guid,
                                block_number,
                                keep_version,
                                error = %error,
                                "superseded generation GC failed"
                            );
                            return;
                        }
                    }
                    node.record_success();
                });
            }
            while requests.next().await.is_some() {}
        });
        Ok(())
    }

    pub async fn fence_data_write_token(
        &self,
        blob_guid: DataBlobGuid,
        version: u64,
        write_token: u64,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        if write_token == 0 {
            return Err(DataVgError::Internal(
                "data write fence requires a nonzero token".to_string(),
            ));
        }
        let volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!(
                "Volume {} not found in DataVgProxy",
                blob_guid.volume_id
            ))
        })?;
        let mut requests = FuturesUnordered::new();
        for node in volume.bss_nodes.iter().cloned() {
            requests.push(async move {
                let result = node
                    .get_client()
                    .fence_data_write_token(
                        blob_guid,
                        version,
                        write_token,
                        Some(self.rpc_timeout),
                        trace_id,
                        0,
                    )
                    .await;
                (node, result)
            });
        }

        let mut failures = Vec::new();
        while let Some((node, result)) = requests.next().await {
            match result {
                Ok(()) => node.record_success(),
                Err(error) => {
                    node.record_failure();
                    failures.push(format!("{}: {}", node.address, error));
                }
            }
        }
        if !failures.is_empty() {
            return Err(DataVgError::QuorumFailure(format!(
                "Data write fence failed on placement nodes: {}",
                failures.join("; ")
            )));
        }

        let mut cleanup_requests = FuturesUnordered::new();
        for node in volume.bss_nodes.iter().cloned() {
            cleanup_requests.push(async move {
                let result = self
                    .delete_fenced_token_from_node(&node, blob_guid, version, write_token, trace_id)
                    .await;
                (node, result)
            });
        }
        while let Some((node, result)) = cleanup_requests.next().await {
            match result {
                Ok(()) => node.record_success(),
                Err(error) => {
                    node.record_failure();
                    failures.push(format!("{}: {}", node.address, error));
                }
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DataVgError::QuorumFailure(format!(
                "Fenced generation cleanup failed on placement nodes: {}",
                failures.join("; ")
            )))
        }
    }

    /// Multi-BSS quorum put (replicated) or EC encode of one exact data
    /// generation. Every data-volume write carries a nonzero token.
    pub async fn put_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        version: u64,
        write_token: u64,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        if write_token == 0 {
            return Err(DataVgError::Internal(
                "a data write requires a nonzero token".to_string(),
            ));
        }
        self.put_blob_inner(
            blob_guid,
            block_number,
            body,
            version,
            write_token,
            false,
            trace_id,
        )
        .await
    }

    pub async fn put_blob_tombstone(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        write_token: u64,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        if write_token == 0 {
            return Err(DataVgError::Internal(
                "a tombstone requires a nonzero token".to_string(),
            ));
        }
        self.put_blob_inner(
            blob_guid,
            block_number,
            Bytes::new(),
            version,
            write_token,
            true,
            trace_id,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_blob_inner(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        version: u64,
        write_token: u64,
        is_deleted: bool,
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
                .put_blob_ec(
                    blob_guid,
                    block_number,
                    body,
                    version,
                    write_token,
                    is_deleted,
                    trace_id,
                )
                .await;
        }

        // Track this write against the volume so concurrent selections can
        // steer away from a busy volume (Power-of-Two-Choices load signal).
        selected_volume.inflight.fetch_add(1, Ordering::Relaxed);
        let _inflight = InflightGuard {
            counter: &selected_volume.inflight,
        };

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
                write_token,
                is_deleted,
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

    #[allow(clippy::too_many_arguments)]
    pub async fn put_blob_vectored(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        chunks: Vec<Bytes>,
        version: u64,
        write_token: u64,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        if write_token == 0 {
            return Err(DataVgError::Internal(
                "a data write requires a nonzero token".to_string(),
            ));
        }
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
                    write_token,
                    false,
                    trace_id,
                )
                .await;
        }

        selected_volume.inflight.fetch_add(1, Ordering::Relaxed);
        let _inflight = InflightGuard {
            counter: &selected_volume.inflight,
        };

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
                write_token,
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
        write_token: u64,
        is_deleted: bool,
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
                write_token,
                is_deleted,
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
    #[allow(clippy::too_many_arguments)]
    async fn put_blob_to_node_vectored(
        bss_node: Arc<BssNode>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        chunks: Vec<Bytes>,
        body_checksum: u64,
        version: u64,
        write_token: u64,
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
                write_token,
                false,
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
    #[allow(clippy::too_many_arguments)]
    pub async fn get_blob_at_or_before(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        committed_version: u64,
        committed_token: u64,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        if committed_token == 0 {
            return Err(DataVgError::Internal(
                "an at-or-before read requires a nonzero committed token".to_string(),
            ));
        }
        let volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!(
                "Volume {} not found in DataVgProxy",
                blob_guid.volume_id
            ))
        })?;
        match volume.mode {
            VolumeMode::Replicated { .. } => {
                self.get_blob_replicated_at_or_before(
                    volume,
                    blob_guid,
                    block_number,
                    content_len,
                    committed_version,
                    committed_token,
                    body,
                    trace_id,
                )
                .await
            }
            VolumeMode::ErasureCoded { .. } => {
                self.get_blob_ec_at_or_before(
                    volume,
                    blob_guid,
                    block_number,
                    content_len,
                    committed_version,
                    committed_token,
                    body,
                    trace_id,
                )
                .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_blob_replicated_at_or_before(
        &self,
        volume: &VolumeWithNodes,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        committed_version: u64,
        committed_token: u64,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let (read_quorum, write_quorum) = match volume.mode {
            VolumeMode::Replicated { r, w, .. } => (r as usize, w as usize),
            VolumeMode::ErasureCoded { .. } => unreachable!(),
        };
        let available_nodes: Vec<_> = volume
            .bss_nodes
            .iter()
            .filter(|node| node.is_available())
            .cloned()
            .collect();
        let fast_path_node = {
            let mut rng = rand::rng();
            available_nodes.choose(&mut rng).cloned()
        };
        if let Some(node) = fast_path_node {
            let mut exact = Bytes::new();
            match node
                .get_client()
                .get_data_blob(
                    blob_guid,
                    block_number,
                    &mut exact,
                    content_len,
                    committed_version,
                    committed_token,
                    Some(self.rpc_timeout),
                    trace_id,
                    0,
                )
                .await
            {
                Ok(()) => {
                    node.record_success();
                    *body = exact.slice(..content_len.min(exact.len()));
                    return Ok(());
                }
                Err(RpcError::NotFound) => node.record_success(),
                Err(e) => {
                    node.record_failure();
                    debug!(node = %node.address, error = %e, "exact generation fast path failed");
                }
            }
        }
        if available_nodes.len() < read_quorum {
            return Err(DataVgError::QuorumFailure(format!(
                "Insufficient available nodes ({}/{}) for read quorum ({})",
                available_nodes.len(),
                volume.bss_nodes.len(),
                read_quorum
            )));
        }

        let mut futures = FuturesUnordered::new();
        for node in available_nodes.iter().cloned() {
            futures.push(async move {
                let result = self
                    .get_generation_from_node_instance(
                        &node,
                        blob_guid,
                        block_number,
                        committed_version,
                        committed_token,
                        trace_id,
                    )
                    .await;
                (node, result)
            });
        }

        let mut fragments = Vec::new();
        let mut observed = Vec::new();
        let mut failures = 0usize;
        while let Some((node, result)) = futures.next().await {
            match result {
                Ok(NodeGenerationLookup::Fragment(fragment)) => {
                    node.record_success();
                    observed.push(fragment.identity());
                    fragments.push(fragment);
                }
                Ok(NodeGenerationLookup::Missing) => node.record_success(),
                Ok(NodeGenerationLookup::Unreadable(identity)) => {
                    node.record_failure();
                    observed.push(identity);
                    failures += 1;
                }
                Err(DataVgError::AmbiguousOlderTokens { version }) => {
                    return Err(DataVgError::AmbiguousOlderTokens { version });
                }
                Err(DataVgError::GenerationCandidateLimit { limit }) => {
                    return Err(DataVgError::GenerationCandidateLimit { limit });
                }
                Err(e) => {
                    node.record_failure();
                    failures += 1;
                    warn!(node = %node.address, error = %e, "generation lookup failed");
                }
            }
        }
        if observed.is_empty() {
            return if failures == 0 {
                Err(DataVgError::BlockNotFound)
            } else {
                Err(DataVgError::QuorumFailure(format!(
                    "No generation found and {} node lookups failed",
                    failures
                )))
            };
        }

        let identity = select_observed_generation(&observed)?.expect("observed is not empty");
        let cohort: Vec<_> = fragments
            .iter()
            .filter(|fragment| fragment.identity() == identity)
            .collect();
        if cohort.is_empty() || (identity.version < committed_version && cohort.len() < read_quorum)
        {
            return Err(DataVgError::StaleVersion {
                expected: identity.version,
            });
        }
        let tombstone_count = cohort
            .iter()
            .filter(|fragment| matches!(fragment, GenerationFragment::Tombstone { .. }))
            .count();
        let reserved_count = cohort
            .iter()
            .filter(|fragment| matches!(fragment, GenerationFragment::Reserved { .. }))
            .count();
        if reserved_count != 0 {
            if reserved_count != cohort.len() {
                return Err(DataVgError::Corrupted);
            }
            if cohort.len() < write_quorum {
                let mut repairs = FuturesUnordered::new();
                for node in available_nodes.iter().cloned() {
                    repairs.push(async move {
                        let result = node
                            .get_client()
                            .reserve_blocks(
                                blob_guid,
                                block_number,
                                content_len as u32,
                                identity.version,
                                identity.write_token,
                                Some(self.rpc_timeout),
                                trace_id,
                                0,
                            )
                            .await;
                        (node, result)
                    });
                }
                let mut repaired = 0usize;
                while let Some((node, result)) = repairs.next().await {
                    match result {
                        Ok(()) | Err(RpcError::VersionSkipped) => {
                            node.record_success();
                            repaired += 1;
                        }
                        Err(_) => node.record_failure(),
                    }
                    if repaired >= write_quorum {
                        break;
                    }
                }
                if repaired < write_quorum {
                    warn!(
                        version = identity.version,
                        repaired, write_quorum, "reserved generation repair incomplete"
                    );
                }
            }
            return Err(DataVgError::BlockNotFound);
        }
        if tombstone_count != 0 && tombstone_count != cohort.len() {
            return Err(DataVgError::Corrupted);
        }

        let canonical_body = if tombstone_count == 0 {
            let GenerationFragment::Data { body, .. } = cohort[0] else {
                unreachable!()
            };
            let checksum = xxhash_rust::xxh3::xxh3_64(body);
            if cohort.iter().any(|fragment| match fragment {
                GenerationFragment::Data { body: other, .. } => {
                    other.len() != body.len() || xxhash_rust::xxh3::xxh3_64(other) != checksum
                }
                GenerationFragment::Tombstone { .. } | GenerationFragment::Reserved { .. } => true,
            }) {
                return Err(DataVgError::Corrupted);
            }
            Some((body.clone(), checksum))
        } else {
            None
        };

        if cohort.len() < write_quorum {
            let mut repairs = FuturesUnordered::new();
            for node in available_nodes.iter().cloned() {
                let (repair_body, checksum, is_deleted) = match &canonical_body {
                    Some((data, checksum)) => (data.clone(), *checksum, false),
                    None => (Bytes::new(), xxhash_rust::xxh3::xxh3_64(&[]), true),
                };
                repairs.push(Self::put_blob_to_node(
                    node,
                    blob_guid,
                    block_number,
                    repair_body,
                    checksum,
                    identity.version,
                    identity.write_token,
                    is_deleted,
                    self.rpc_timeout,
                    *trace_id,
                ));
            }
            let mut repaired = 0usize;
            while let Some((node, _, result)) = repairs.next().await {
                match result {
                    Ok(()) | Err(RpcError::VersionSkipped) => {
                        node.record_success();
                        repaired += 1;
                    }
                    Err(_) => node.record_failure(),
                }
                if repaired >= write_quorum {
                    break;
                }
            }
            if repaired < write_quorum {
                warn!(
                    version = identity.version,
                    repaired, write_quorum, "generation repair incomplete"
                );
            }
        }

        match canonical_body {
            Some((data, _)) => {
                *body = data.slice(..content_len.min(data.len()));
                Ok(())
            }
            None => Err(DataVgError::BlockNotFound),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_blob_ec_at_or_before(
        &self,
        volume: &VolumeWithNodes,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        committed_version: u64,
        committed_token: u64,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let (k, m) = match volume.mode {
            VolumeMode::ErasureCoded {
                data_shards,
                parity_shards,
            } => (data_shards as usize, parity_shards as usize),
            VolumeMode::Replicated { .. } => unreachable!(),
        };
        let total = k + m;
        let min_shard_size = ec_padded_len(content_len, k) / k;
        let rotation = ec_rotation(&blob_guid.blob_id, total as u32);
        let data_nodes_available = (0..k)
            .all(|shard_index| volume.bss_nodes[(shard_index + rotation) % total].is_available());
        if data_nodes_available {
            let mut exact_reads = FuturesUnordered::new();
            for shard_index in 0..k {
                let node_index = (shard_index + rotation) % total;
                let node = volume.bss_nodes[node_index].clone();
                exact_reads.push(async move {
                    let mut shard = Bytes::new();
                    let result = node
                        .get_client()
                        .get_data_blob(
                            blob_guid,
                            block_number,
                            &mut shard,
                            min_shard_size,
                            committed_version,
                            committed_token,
                            Some(self.rpc_timeout),
                            trace_id,
                            0,
                        )
                        .await;
                    (shard_index, node, shard, result)
                });
            }
            let mut exact_shards = vec![None; k];
            while let Some((shard_index, node, shard, result)) = exact_reads.next().await {
                match result {
                    Ok(()) => {
                        node.record_success();
                        exact_shards[shard_index] = Some(shard);
                    }
                    Err(RpcError::NotFound) => node.record_success(),
                    Err(_) => node.record_failure(),
                }
            }
            if exact_shards.iter().all(Option::is_some) {
                let shard_sizes: std::collections::HashSet<usize> = exact_shards
                    .iter()
                    .filter_map(|shard| shard.as_ref().map(Bytes::len))
                    .collect();
                if shard_sizes.len() == 1 {
                    let mut data = Vec::with_capacity(
                        exact_shards
                            .iter()
                            .filter_map(Option::as_ref)
                            .map(Bytes::len)
                            .sum(),
                    );
                    for shard in exact_shards {
                        data.extend_from_slice(&shard.expect("all exact data shards are present"));
                    }
                    data.truncate(content_len);
                    *body = Bytes::from(data);
                    return Ok(());
                }
            }
        }
        let available_count = volume
            .bss_nodes
            .iter()
            .filter(|node| node.is_available())
            .count();
        if available_count < k {
            return Err(DataVgError::QuorumFailure(format!(
                "EC read has {}/{} available nodes, needs {}",
                available_count, total, k
            )));
        }
        let mut futures = FuturesUnordered::new();
        for (node_index, node) in volume.bss_nodes.iter().cloned().enumerate() {
            if !node.is_available() {
                continue;
            }
            futures.push(async move {
                let result = self
                    .get_generation_from_node_instance(
                        &node,
                        blob_guid,
                        block_number,
                        committed_version,
                        committed_token,
                        trace_id,
                    )
                    .await;
                (node_index, node, result)
            });
        }

        let mut fragments = Vec::new();
        let mut observed = Vec::new();
        let mut failures = 0usize;
        while let Some((node_index, node, result)) = futures.next().await {
            match result {
                Ok(NodeGenerationLookup::Fragment(fragment)) => {
                    node.record_success();
                    observed.push(fragment.identity());
                    fragments.push((node_index, fragment));
                }
                Ok(NodeGenerationLookup::Missing) => node.record_success(),
                Ok(NodeGenerationLookup::Unreadable(identity)) => {
                    node.record_failure();
                    observed.push(identity);
                    failures += 1;
                }
                Err(DataVgError::AmbiguousOlderTokens { version }) => {
                    return Err(DataVgError::AmbiguousOlderTokens { version });
                }
                Err(DataVgError::GenerationCandidateLimit { limit }) => {
                    return Err(DataVgError::GenerationCandidateLimit { limit });
                }
                Err(e) => {
                    node.record_failure();
                    failures += 1;
                    warn!(node = %node.address, error = %e, "EC generation lookup failed");
                }
            }
        }
        if observed.is_empty() {
            return if failures == 0 {
                Err(DataVgError::BlockNotFound)
            } else {
                Err(DataVgError::QuorumFailure(format!(
                    "No EC generation found and {} node lookups failed",
                    failures
                )))
            };
        }

        let identity = select_observed_generation(&observed)?.expect("observed is not empty");
        let cohort: Vec<_> = fragments
            .into_iter()
            .filter(|(_, fragment)| fragment.identity() == identity)
            .collect();
        let tombstone_count = cohort
            .iter()
            .filter(|(_, fragment)| matches!(fragment, GenerationFragment::Tombstone { .. }))
            .count();
        let reserved_count = cohort
            .iter()
            .filter(|(_, fragment)| matches!(fragment, GenerationFragment::Reserved { .. }))
            .count();
        if reserved_count != 0 {
            if reserved_count == cohort.len() && cohort.len() >= k {
                let present: std::collections::HashSet<usize> =
                    cohort.iter().map(|(node_index, _)| *node_index).collect();
                let mut repairs = FuturesUnordered::new();
                for (node_index, node) in volume.bss_nodes.iter().cloned().enumerate() {
                    if present.contains(&node_index) || !node.is_available() {
                        continue;
                    }
                    repairs.push(async move {
                        let result = node
                            .get_client()
                            .reserve_blocks(
                                blob_guid,
                                block_number,
                                content_len as u32,
                                identity.version,
                                identity.write_token,
                                Some(self.rpc_timeout),
                                trace_id,
                                0,
                            )
                            .await;
                        (node, result)
                    });
                }
                while let Some((node, result)) = repairs.next().await {
                    match result {
                        Ok(()) | Err(RpcError::VersionSkipped) => node.record_success(),
                        Err(_) => node.record_failure(),
                    }
                }
                return Err(DataVgError::BlockNotFound);
            }
            return Err(DataVgError::StaleVersion {
                expected: identity.version,
            });
        }
        if tombstone_count != 0 {
            if tombstone_count == cohort.len() && cohort.len() >= k {
                let present: std::collections::HashSet<usize> =
                    cohort.iter().map(|(node_index, _)| *node_index).collect();
                let mut repairs = FuturesUnordered::new();
                for (node_index, node) in volume.bss_nodes.iter().cloned().enumerate() {
                    if present.contains(&node_index) || !node.is_available() {
                        continue;
                    }
                    repairs.push(Self::put_blob_to_node(
                        node,
                        blob_guid,
                        block_number,
                        Bytes::new(),
                        xxhash_rust::xxh3::xxh3_64(&[]),
                        identity.version,
                        identity.write_token,
                        true,
                        self.rpc_timeout,
                        *trace_id,
                    ));
                }
                while let Some((node, _, result)) = repairs.next().await {
                    match result {
                        Ok(()) | Err(RpcError::VersionSkipped) => node.record_success(),
                        Err(_) => node.record_failure(),
                    }
                }
                return Err(DataVgError::BlockNotFound);
            }
            return Err(DataVgError::StaleVersion {
                expected: identity.version,
            });
        }
        if cohort.len() < k {
            return Err(DataVgError::StaleVersion {
                expected: identity.version,
            });
        }

        let shard_sizes: std::collections::HashSet<usize> = cohort
            .iter()
            .filter_map(|(_, fragment)| match fragment {
                GenerationFragment::Data { body, .. } => Some(body.len()),
                GenerationFragment::Tombstone { .. } | GenerationFragment::Reserved { .. } => None,
            })
            .collect();
        if shard_sizes.len() != 1 {
            return Err(DataVgError::Corrupted);
        }
        let shard_size = *shard_sizes
            .iter()
            .next()
            .expect("one shard size after length check");

        let mut shards: Vec<Option<Vec<u8>>> = vec![None; total];
        for (node_index, fragment) in cohort {
            let GenerationFragment::Data { body, .. } = fragment else {
                continue;
            };
            if body.len() != shard_size {
                continue;
            }
            let shard_index = (node_index + total - rotation) % total;
            shards[shard_index] = Some(body.to_vec());
        }
        if shards.iter().filter(|shard| shard.is_some()).count() < k {
            return Err(DataVgError::StaleVersion {
                expected: identity.version,
            });
        }

        let data_shards: Vec<Vec<u8>> = if shards.iter().take(k).all(Option::is_some) {
            shards
                .iter()
                .take(k)
                .map(|shard| shard.as_ref().expect("checked data shard").clone())
                .collect()
        } else {
            let original_shards: Vec<_> = shards
                .iter()
                .take(k)
                .enumerate()
                .filter_map(|(index, shard)| shard.as_deref().map(|data| (index, data)))
                .collect();
            let recovery_shards: Vec<_> = shards
                .iter()
                .skip(k)
                .enumerate()
                .filter_map(|(index, shard)| shard.as_deref().map(|data| (index, data)))
                .collect();
            let restored = rs_decode(k, m, original_shards, recovery_shards)
                .map_err(|e| DataVgError::Internal(format!("RS reconstruct failed: {}", e)))?;
            let mut data_shards = Vec::with_capacity(k);
            for (index, shard) in shards.iter().take(k).enumerate() {
                if let Some(shard) = shard {
                    data_shards.push(shard.clone());
                } else if let Some(shard) = restored.get(&index) {
                    data_shards.push(shard.clone());
                } else {
                    return Err(DataVgError::Internal(format!(
                        "RS reconstruct missing shard {}",
                        index
                    )));
                }
            }
            data_shards
        };

        let missing: Vec<usize> = shards
            .iter()
            .enumerate()
            .filter_map(|(index, shard)| shard.is_none().then_some(index))
            .collect();
        if !missing.is_empty() {
            let parity = rs_encode(k, m, &data_shards)
                .map_err(|e| DataVgError::Internal(format!("RS encode failed: {}", e)))?;
            let mut all_shards = data_shards.clone();
            all_shards.extend(parity);
            let mut repairs = FuturesUnordered::new();
            for shard_index in missing {
                let node_index = (shard_index + rotation) % total;
                let node = volume.bss_nodes[node_index].clone();
                if !node.is_available() {
                    continue;
                }
                let shard = Bytes::from(all_shards[shard_index].clone());
                let checksum = xxhash_rust::xxh3::xxh3_64(&shard);
                repairs.push(Self::put_blob_to_node(
                    node,
                    blob_guid,
                    block_number,
                    shard,
                    checksum,
                    identity.version,
                    identity.write_token,
                    false,
                    self.rpc_timeout,
                    *trace_id,
                ));
            }
            while let Some((node, _, result)) = repairs.next().await {
                match result {
                    Ok(()) | Err(RpcError::VersionSkipped) => node.record_success(),
                    Err(_) => node.record_failure(),
                }
            }
        }

        let mut result_data = Vec::with_capacity(k * shard_size);
        for shard in data_shards {
            result_data.extend_from_slice(&shard);
        }
        result_data.truncate(content_len);
        *body = Bytes::from(result_data);
        Ok(())
    }

    /// Reserve a single block at the volume write quorum.
    /// Reserve one exact data generation at the volume write quorum.
    pub async fn reserve_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        block_size: u32,
        expected_version: u64,
        write_token: u64,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        if write_token == 0 {
            return Err(DataVgError::Internal(
                "a reservation requires a nonzero token".to_string(),
            ));
        }
        self.reserve_blob_inner(
            blob_guid,
            block_number,
            block_size,
            expected_version,
            write_token,
            trace_id,
        )
        .await
    }

    async fn reserve_blob_inner(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        block_size: u32,
        expected_version: u64,
        write_token: u64,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!("Volume {} not found", blob_guid.volume_id))
        })?;
        let rpc_timeout = self.rpc_timeout;
        let write_quorum = match &volume.mode {
            VolumeMode::Replicated { w, .. } => *w as usize,
            VolumeMode::ErasureCoded { data_shards, .. } => *data_shards as usize + 1,
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
                        write_token,
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
                spawn_background(async move {
                    while let Some((node, _, result)) = futures.next().await {
                        match result {
                            Ok(()) | Err(RpcError::VersionSkipped) => node.record_success(),
                            Err(_) => node.record_failure(),
                        }
                    }
                });
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

    /// Enumerate the BSS-visible block entries for one blob over
    /// `[first_block, first_block + block_count)`. The first available node
    /// responds; absent blocks are holes.
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

    pub async fn list_blob_blocks_at_or_before(
        &self,
        blob_guid: DataBlobGuid,
        first_block: u32,
        block_count: u32,
        committed_version: u64,
        committed_token: u64,
        trace_id: &TraceId,
    ) -> Result<std::collections::BTreeSet<u32>, DataVgError> {
        let volume = self.find_volume(blob_guid.volume_id).ok_or_else(|| {
            DataVgError::InitializationError(format!("Volume {} not found", blob_guid.volume_id))
        })?;
        let read_threshold = match volume.mode {
            VolumeMode::Replicated { r, .. } => r as usize,
            VolumeMode::ErasureCoded { data_shards, .. } => data_shards as usize,
        };
        let available_nodes: Vec<_> = volume
            .bss_nodes
            .iter()
            .filter(|node| node.is_available())
            .cloned()
            .collect();
        if available_nodes.len() < read_threshold {
            return Err(DataVgError::QuorumFailure(format!(
                "List has {}/{} available nodes, needs {}",
                available_nodes.len(),
                volume.bss_nodes.len(),
                read_threshold
            )));
        }

        let mut futures = FuturesUnordered::new();
        for node in available_nodes {
            futures.push(async move {
                let result = self
                    .list_generations_from_node_instance(
                        &node,
                        blob_guid,
                        first_block,
                        block_count,
                        trace_id,
                    )
                    .await;
                (node, result)
            });
        }

        let mut responses = Vec::new();
        while let Some((node, result)) = futures.next().await {
            match result {
                Ok(entries) => {
                    node.record_success();
                    responses.push(index_listed_generations(entries));
                }
                Err(e) => {
                    node.record_failure();
                    warn!(node = %node.address, error = %e, "generation list failed");
                }
            }
        }
        if responses.len() < read_threshold {
            return Err(DataVgError::QuorumFailure(format!(
                "List succeeded on {}/{} nodes, needs {}",
                responses.len(),
                volume.bss_nodes.len(),
                read_threshold
            )));
        }

        let last_block = first_block.saturating_add(block_count);
        let mut data_blocks = std::collections::BTreeSet::new();
        for block_number in first_block..last_block {
            if listed_block_has_data_at_or_before(
                &responses,
                block_number,
                committed_version,
                committed_token,
                read_threshold,
            )? {
                data_blocks.insert(block_number);
            }
        }
        Ok(data_blocks)
    }

    /// EC put: RS-encode block into k+m shards, send to nodes with W=k+1 quorum
    #[allow(clippy::too_many_arguments)]
    async fn put_blob_ec(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        version: u64,
        write_token: u64,
        is_deleted: bool,
        trace_id: &TraceId,
    ) -> Result<(), DataVgError> {
        let start = Instant::now();
        let trace_id = *trace_id;
        histogram!("blob_size", "operation" => "put_ec").record(body.len() as f64);

        // An empty data write has no fragment to encode. An MVCC tombstone
        // is an empty generation that must still reach the EC write quorum.
        if body.is_empty() && !is_deleted {
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

        ec_vol.inflight.fetch_add(1, Ordering::Relaxed);
        let _inflight = InflightGuard {
            counter: &ec_vol.inflight,
        };

        if is_deleted {
            let available_nodes: Vec<_> = ec_vol
                .bss_nodes
                .iter()
                .filter(|node| node.is_available())
                .cloned()
                .collect();
            if available_nodes.len() < write_quorum {
                return Err(DataVgError::QuorumFailure(format!(
                    "EC tombstone: insufficient available nodes ({}/{}) for write quorum ({})",
                    available_nodes.len(),
                    total,
                    write_quorum
                )));
            }
            let empty_checksum = xxhash_rust::xxh3::xxh3_64(&[]);
            let mut futures = FuturesUnordered::new();
            for node in available_nodes {
                futures.push(Self::put_blob_to_node(
                    node,
                    blob_guid,
                    block_number,
                    Bytes::new(),
                    empty_checksum,
                    version,
                    write_token,
                    true,
                    self.rpc_timeout,
                    trace_id,
                ));
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
                    spawn_background(async move {
                        while let Some((node, _, result)) = futures.next().await {
                            match result {
                                Ok(()) | Err(RpcError::VersionSkipped) => node.record_success(),
                                Err(_) => node.record_failure(),
                            }
                        }
                    });
                    return Ok(());
                }
            }
            return Err(DataVgError::QuorumFailure(format!(
                "EC tombstone write quorum failed ({}/{}): {}",
                successes,
                write_quorum,
                errors.join("; ")
            )));
        }

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
                write_token,
                is_deleted,
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
                // Background remaining writes
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{BssNode as DataBssNode, Volume};

    const TEST_BLOB_ID: &str = "01234567-89ab-cdef-0123-456789abcdef";

    fn test_generation_key(block_number: u32, version: u64, write_token: u64) -> String {
        format!(
            "/d7/{TEST_BLOB_ID}-p{block_number:08x}-rv{:016x}-t{write_token:016x}",
            u64::MAX - version
        )
    }

    fn replicated_info(n: u32, r: u32, w: u32) -> DataVgInfo {
        let bss_nodes = (0..n)
            .map(|index| DataBssNode {
                node_id: format!("bss-{index}"),
                ip: "127.0.0.1".to_string(),
                port: 18088 + index as u16,
            })
            .collect();
        DataVgInfo {
            volumes: vec![Volume {
                volume_id: 7,
                uuid: "test-volume".to_string(),
                bss_nodes,
                mode: VolumeMode::Replicated { n, r, w },
            }],
        }
    }

    #[test]
    fn generation_key_parses_fixed_width_boundaries() {
        let max = parse_generation_key(&format!(
            "/d7/{TEST_BLOB_ID}-pffffffff-rv0000000000000000-tffffffffffffffff\0"
        ))
        .expect("maximum generation key should parse");
        assert_eq!(max.0, u32::MAX);
        assert_eq!(max.1.version, u64::MAX);
        assert_eq!(max.1.write_token, u64::MAX);

        let zero = parse_generation_key(&format!(
            "/d7/{TEST_BLOB_ID}-p00000000-rvffffffffffffffff-t0000000000000000"
        ))
        .expect("zero generation key should parse");
        assert_eq!(zero.0, 0);
        assert_eq!(zero.1.version, 0);
        assert_eq!(zero.1.write_token, 0);

        assert!(
            parse_generation_key(&format!(
                "/d7/{TEST_BLOB_ID}-p0000000-rvffffffffffffffff-t0000000000000000"
            ))
            .is_none()
        );
        assert!(
            parse_generation_key(&format!(
                "/d7/{TEST_BLOB_ID}-p00000000-vffffffffffffffff-t0000000000000000"
            ))
            .is_none()
        );
    }

    #[test]
    fn generation_scan_marker_starts_at_the_ceiling_version() {
        let blob_guid = DataBlobGuid {
            blob_id: Uuid::parse_str(TEST_BLOB_ID).expect("test blob ID should parse"),
            volume_id: 7,
        };
        let prefix = generation_key_prefix(blob_guid, 9);
        let marker = generation_scan_marker(&prefix, 12);
        assert!(test_generation_key(9, 13, u64::MAX) <= marker);
        assert!(test_generation_key(9, 12, 0) > marker);
        assert!(generation_scan_marker(&prefix, u64::MAX).is_empty());
    }

    #[test]
    fn generation_selection_enforces_ceiling_identity() {
        let mut entries = [
            (test_generation_key(9, 13, 1), false),
            (test_generation_key(9, 12, 3), false),
            (test_generation_key(9, 12, 7), true),
            (test_generation_key(9, 11, 4), false),
        ];
        entries.sort_by(|left, right| left.0.cmp(&right.0));

        let selected = select_generation_candidate(
            entries
                .iter()
                .map(|(key, is_deleted)| (key.as_str(), *is_deleted, 0, 0)),
            9,
            12,
            7,
            false,
        )
        .expect("committed generation should be selected")
        .expect("committed generation should exist");
        assert_eq!(selected.0.version, 12);
        assert_eq!(selected.0.write_token, 7);
        assert!(selected.1);
    }

    #[test]
    fn generation_selection_preserves_storage_metadata() {
        let key = test_generation_key(9, 12, 7);

        let selected = select_generation_candidate(
            [(key.as_str(), false, 1234, ENTRY_TYPE_RESERVED)],
            9,
            12,
            7,
            false,
        )
        .expect("generation selection should succeed")
        .expect("committed generation should exist");

        assert_eq!(selected.2, 1234);
        assert_eq!(selected.3, ENTRY_TYPE_RESERVED);
    }

    #[test]
    fn generation_selection_uses_newest_eligible_older_version() {
        let mut entries = [
            (test_generation_key(9, 14, 1), false),
            (test_generation_key(9, 13, 8), false),
            (test_generation_key(9, 12, 5), false),
            (test_generation_key(9, 11, 2), false),
        ];
        entries.sort_by(|left, right| left.0.cmp(&right.0));

        let selected = select_generation_candidate(
            entries
                .iter()
                .map(|(key, is_deleted)| (key.as_str(), *is_deleted, 0, 0)),
            9,
            13,
            7,
            true,
        )
        .expect("closed older cohort should not exceed the scan cap")
        .expect("older generation should exist");
        assert_eq!(selected.0.version, 12);
        assert_eq!(selected.0.write_token, 5);
    }

    #[test]
    fn generation_selection_rejects_ambiguous_older_tokens() {
        let mut entries = [
            (test_generation_key(9, 12, 4), false),
            (test_generation_key(9, 12, 5), false),
        ];
        entries.sort_by(|left, right| left.0.cmp(&right.0));

        let error = select_generation_candidate(
            entries
                .iter()
                .map(|(key, is_deleted)| (key.as_str(), *is_deleted, 0, 0)),
            9,
            13,
            7,
            false,
        )
        .expect_err("multiple older tokens should fail");
        assert!(matches!(
            error,
            DataVgError::AmbiguousOlderTokens { version: 12 }
        ));
    }

    #[test]
    fn generation_selection_fails_when_candidate_scan_is_incomplete() {
        let entries = [(test_generation_key(9, 14, 1), false)];
        let error = select_generation_candidate(
            entries
                .iter()
                .map(|(key, is_deleted)| (key.as_str(), *is_deleted, 0, 0)),
            9,
            13,
            7,
            true,
        )
        .expect_err("incomplete candidate scan should fail");
        assert!(matches!(
            error,
            DataVgError::GenerationCandidateLimit {
                limit: GENERATION_CANDIDATE_CAP
            }
        ));
    }

    #[test]
    fn list_selection_handles_later_block_ranges() {
        let identity = GenerationIdentity {
            version: 12,
            write_token: 7,
        };
        let entries: Vec<_> = (0..GENERATION_LIST_PAGE_SIZE)
            .map(|block_number| (block_number, identity, false))
            .chain([(4096, identity, false)])
            .collect();
        let indexed = index_listed_generations(entries);
        let responses = vec![indexed.clone(), indexed];

        let has_data = listed_block_has_data_at_or_before(&responses, 4096, 12, 7, 2)
            .expect("committed generation should satisfy the read quorum");

        assert!(has_data);
    }

    #[test]
    fn unreadable_newer_generation_prevents_stale_descent() {
        let selected = select_observed_generation(&[
            GenerationIdentity {
                version: 11,
                write_token: 4,
            },
            GenerationIdentity {
                version: 12,
                write_token: 7,
            },
        ])
        .expect("observed generations should be unambiguous")
        .expect("an observed generation should be selected");

        assert_eq!(selected.version, 12);
        assert_eq!(selected.write_token, 7);
    }

    #[test]
    fn replicated_read_quorum_below_majority_is_rejected() {
        let error = DataVgProxy::new(
            replicated_info(3, 1, 2),
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .err()
        .expect("sub-majority read quorum should fail");
        assert!(matches!(
            error,
            DataVgError::InitializationError(message)
                if message.contains("r=1 below majority 2")
        ));
    }

    #[test]
    fn replicated_write_quorum_below_majority_is_rejected() {
        let error = DataVgProxy::new(
            replicated_info(3, 2, 1),
            Duration::from_secs(5),
            Duration::from_secs(5),
        )
        .err()
        .expect("sub-majority write quorum should fail");
        assert!(matches!(
            error,
            DataVgError::InitializationError(message)
                if message.contains("w=1 below majority 2")
        ));
    }

    #[test]
    fn replicated_majority_and_raised_write_quorums_are_accepted() {
        let majority = DataVgProxy::new(
            replicated_info(3, 2, 2),
            Duration::from_secs(5),
            Duration::from_secs(5),
        );
        assert!(majority.is_ok());

        let raised = DataVgProxy::new(
            replicated_info(3, 2, 3),
            Duration::from_secs(5),
            Duration::from_secs(5),
        );
        assert!(raised.is_ok());
    }

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
                "volume_id": 32768,"uuid":"test-uuid",
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
            "volumes": [{"volume_id":1,"uuid":"test-uuid","bss_nodes":[{"node_id":"bss-0","ip":"127.0.0.1","port":8088}],"mode":{"type":"replicated","n":1,"r":1,"w":1}}]
        }"#;

        let info: DataVgInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.volumes.len(), 1);
        assert!(!info.volumes[0].is_ec());
    }

    #[test]
    fn datavgproxy_init_ec_only() {
        let json = r#"{
            "volumes": [{
                "volume_id": 32768,"uuid":"test-uuid",
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
                "volume_id": 32768,"uuid":"test-uuid",
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
                "volume_id": 65535,"uuid":"test-uuid",
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
                "volume_id": 32768,"uuid":"test-uuid",
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
                "volume_id": 32768,"uuid":"test-uuid",
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
                    "volume_id": 1,"uuid":"test-uuid",
                    "bss_nodes": [
                        {"node_id":"bss-0","ip":"127.0.0.1","port":18088}
                    ],
                    "mode": {"type":"replicated","n":1,"r":1,"w":1}
                },
                {
                    "volume_id": 32768,"uuid":"test-uuid",
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
