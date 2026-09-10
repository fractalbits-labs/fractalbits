//! Background reclamation: superseded-generation sweeps, whole-blob
//! teardown (data keys plus `@ovr/` rows), and the `@ovr-gc/` marker
//! protocol that makes teardown crash-safe.
//!
//! The client derives the work (which generations a commit superseded,
//! which blobs became unreachable) and ships it as `SweepBlob`; the
//! gateway coalesces per blob, applies the reader grace, retries, and
//! survives the client disconnecting mid-sweep.

use std::collections::{HashMap, HashSet, hash_map};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use data_types::ovr_map::{
    OVR_GC_PREFIX, OvrGcMarker, encode_ovr_gc_data_pending, encode_ovr_gc_rows_ready, ovr_gc_key,
    ovr_row_prefix, parse_ovr_gc_blob_id, parse_ovr_gc_marker,
};
use data_types::{DataBlobGuid, TraceId};
use fs_gateway_codec::SweepBlobRequest;
use futures::{FutureExt, StreamExt, stream};
use rpc_client_common::reclamation_grace;
use uuid::Uuid;

use crate::backend::{BackendConfig, StorageBackend};
use crate::error::FsError;

const SWEEP_CONCURRENCY: usize = 8;
const SWEEP_POLL_INTERVAL: Duration = Duration::from_millis(250);
/// NSS listing page size for row teardown. Deliberately above the NSS
/// clamp so the `has_more` loop, not the request size, bounds coverage.
const ROW_TEARDOWN_PAGE: u32 = 1000;
const TEARDOWN_MARKER_CAS_RETRIES: u32 = 16;

#[derive(Clone)]
enum TeardownMarkerWrite {
    DataPending {
        volume_id: u16,
        not_before_unix_ms: u64,
    },
    RowsReady {
        volume_id: u16,
    },
}

impl TeardownMarkerWrite {
    fn value(&self) -> Bytes {
        match self {
            Self::DataPending {
                volume_id,
                not_before_unix_ms,
            } => {
                Bytes::copy_from_slice(&encode_ovr_gc_data_pending(*volume_id, *not_before_unix_ms))
            }
            Self::RowsReady { volume_id } => {
                Bytes::copy_from_slice(&encode_ovr_gc_rows_ready(*volume_id))
            }
        }
    }

    fn satisfied_by(&self, current: Option<&[u8]>) -> bool {
        let Some(current) = current else {
            return false;
        };
        match self {
            Self::DataPending { volume_id, .. } => matches!(
                parse_ovr_gc_marker(current),
                OvrGcMarker::DataPending {
                    volume_id: stored,
                    ..
                } | OvrGcMarker::RowsReady { volume_id: stored }
                    if stored == *volume_id
            ),
            Self::RowsReady { volume_id } => matches!(
                parse_ovr_gc_marker(current),
                OvrGcMarker::RowsReady { volume_id: stored } if stored == *volume_id
            ),
        }
    }
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn instant_for_unix_deadline(deadline_unix_ms: u64) -> Instant {
    let remaining = Duration::from_millis(deadline_unix_ms.saturating_sub(unix_time_millis()));
    Instant::now()
        .checked_add(remaining)
        .unwrap_or_else(Instant::now)
}

pub(crate) struct SweepWork {
    /// Bucket the blob belongs to. `None` only in unit tests.
    backend_config: Option<Arc<BackendConfig>>,
    /// Exact identities to delete on every placement node: superseded
    /// generations handed over by the row CAS (the outgoing `cur`), plus
    /// orphan fragments of failed unpublished creates.
    victims: HashSet<(u32, u64)>,
    /// Blocks whose stale generations cannot be named exactly (a filled
    /// fallocate claim over a `Hole` row has no row record): delete every
    /// listed generation strictly below the recorded floor. Resolved via
    /// one all-node listing, so keep it rare.
    below: HashMap<u32, u64>,
    /// Tear down every physical data/reservation key of the blob.
    delete_all_blocks: bool,
    /// Tear down every `@ovr/` row of the blob, then clear its
    /// `@ovr-gc/` marker.
    delete_rows: bool,
    /// Retry a failed transition from conditional intent to durable
    /// data-pending teardown before any physical deletion. The value is
    /// the wall-clock grace deadline encoded into the marker.
    marker_data_pending: Option<u64>,
    grace_until: Option<Instant>,
    retry_count: u32,
    ready_at: Instant,
}

impl SweepWork {
    fn new(backend_config: Option<Arc<BackendConfig>>) -> Self {
        Self {
            backend_config,
            victims: HashSet::new(),
            below: HashMap::new(),
            delete_all_blocks: false,
            delete_rows: false,
            marker_data_pending: None,
            grace_until: None,
            retry_count: 0,
            ready_at: Instant::now(),
        }
    }

    fn is_empty(&self) -> bool {
        self.victims.is_empty()
            && self.below.is_empty()
            && !self.delete_all_blocks
            && !self.delete_rows
            && self.marker_data_pending.is_none()
            && self.grace_until.is_none()
    }

    fn merge(&mut self, mut other: Self) {
        if self.backend_config.is_none() {
            self.backend_config = other.backend_config.take();
        }
        self.victims.extend(other.victims.drain());
        for (block, keep_from) in other.below.drain() {
            let slot = self.below.entry(block).or_insert(keep_from);
            *slot = (*slot).max(keep_from);
        }
        self.delete_all_blocks |= other.delete_all_blocks;
        self.delete_rows |= other.delete_rows;
        self.marker_data_pending = match (self.marker_data_pending, other.marker_data_pending) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (deadline @ Some(_), None) | (None, deadline @ Some(_)) => deadline,
            (None, None) => None,
        };
        self.grace_until = match (self.grace_until, other.grace_until) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (deadline @ Some(_), None) | (None, deadline @ Some(_)) => deadline,
            (None, None) => None,
        };
        self.retry_count = self.retry_count.max(other.retry_count);
        self.ready_at = self.ready_at.min(other.ready_at);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepAttempt {
    Complete,
    GracePending,
    Failed,
}

#[derive(Default)]
pub(crate) struct SweepQueue {
    pending: HashMap<DataBlobGuid, SweepWork>,
    active: HashSet<DataBlobGuid>,
}

impl SweepQueue {
    /// Coalesce `work` into the blob's pending entry (merge, never
    /// replace: a replace would silently drop queued victims).
    fn enqueue(&mut self, blob_guid: DataBlobGuid, work: SweepWork) {
        match self.pending.entry(blob_guid) {
            hash_map::Entry::Occupied(mut entry) => entry.get_mut().merge(work),
            hash_map::Entry::Vacant(entry) => {
                entry.insert(work);
            }
        }
    }
}

#[derive(Default)]
pub struct SweepCoordinator {
    pub(crate) queue: parking_lot::Mutex<SweepQueue>,
    worker_started: AtomicBool,
}

/// Translate a client's `SweepBlob` hint into queued work. The reader
/// grace is the gateway's, derived from its own RPC timeout.
pub fn enqueue_sweep_request(
    coordinator: &SweepCoordinator,
    backend_config: Arc<BackendConfig>,
    blob_guid: DataBlobGuid,
    req: &SweepBlobRequest,
) {
    let mut work = SweepWork::new(Some(backend_config.clone()));
    work.victims
        .extend(req.victims.iter().map(|v| (v.block_number, v.version)));
    for floor in &req.below {
        let slot = work
            .below
            .entry(floor.block_number)
            .or_insert(floor.keep_from);
        *slot = (*slot).max(floor.keep_from);
    }
    work.delete_all_blocks = req.delete_all_blocks;
    work.delete_rows = req.delete_rows;
    work.marker_data_pending = req.marker_data_pending_unix_ms;
    if req.with_grace {
        let now = Instant::now();
        let grace_until = now
            .checked_add(reclamation_grace(
                backend_config.config.rpc_request_timeout(),
            ))
            .unwrap_or(now);
        work.grace_until = Some(grace_until);
        work.ready_at = grace_until;
    }
    if work.is_empty() {
        return;
    }
    coordinator.queue.lock().enqueue(blob_guid, work);
}

struct SweepClaim {
    blob_guid: DataBlobGuid,
    work: SweepWork,
    coordinator: Arc<SweepCoordinator>,
}

impl SweepClaim {
    fn take_ready(coordinator: &Arc<SweepCoordinator>) -> Option<Self> {
        let mut queue = coordinator.queue.lock();
        if queue.active.len() >= SWEEP_CONCURRENCY {
            return None;
        }
        let now = Instant::now();
        let blob_guid = queue
            .pending
            .iter()
            .find(|(blob_guid, work)| !queue.active.contains(blob_guid) && work.ready_at <= now)
            .map(|(blob_guid, _)| *blob_guid)?;
        let work = queue
            .pending
            .remove(&blob_guid)
            .expect("ready sweep selected above");
        queue.active.insert(blob_guid);
        drop(queue);
        Some(Self {
            blob_guid,
            work,
            coordinator: coordinator.clone(),
        })
    }

    fn schedule_retry(&mut self, attempt: SweepAttempt) {
        let now = Instant::now();
        match attempt {
            SweepAttempt::Complete => {}
            SweepAttempt::GracePending => {
                self.work.retry_count = 0;
                self.work.ready_at = self.work.grace_until.unwrap_or(now);
            }
            SweepAttempt::Failed => {
                self.work.retry_count = self.work.retry_count.saturating_add(1);
                let shift = self.work.retry_count.min(6);
                self.work.ready_at = now
                    .checked_add(Duration::from_secs(1_u64 << shift))
                    .unwrap_or(now);
                tracing::warn!(
                    blob_guid = %self.blob_guid,
                    victims = self.work.victims.len(),
                    below = self.work.below.len(),
                    delete_all_blocks = self.work.delete_all_blocks,
                    delete_rows = self.work.delete_rows,
                    retry = self.work.retry_count,
                    "blob reclamation incomplete; queued retry"
                );
            }
        }
    }
}

impl Drop for SweepClaim {
    fn drop(&mut self) {
        let mut queue = self.coordinator.queue.lock();
        queue.active.remove(&self.blob_guid);
        if self.work.is_empty() {
            return;
        }
        let backend_config = self.work.backend_config.clone();
        let work = std::mem::replace(&mut self.work, SweepWork::new(backend_config));
        queue.enqueue(self.blob_guid, work);
    }
}

async fn write_teardown_marker_cas(
    backend: &StorageBackend,
    blob_guid: DataBlobGuid,
    target: TeardownMarkerWrite,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    let key = ovr_gc_key(&blob_guid.blob_id);
    let value = target.value();
    let mut current: Option<Bytes> = None;
    for _ in 0..TEARDOWN_MARKER_CAS_RETRIES {
        if target.satisfied_by(current.as_deref()) {
            return Ok(());
        }
        let expected = current.clone().unwrap_or_default();
        match backend
            .put_inode_cas(&key, value.clone(), expected, trace_id)
            .await
        {
            Ok(_) => return Ok(()),
            Err(FsError::CasConflict) => {
                current = match backend.get_inode_raw(&key, trace_id).await {
                    Ok(stored) => Some(stored),
                    Err(FsError::NotFound) => None,
                    Err(error) => return Err(error),
                };
            }
            Err(error) => {
                let probe = backend.get_inode_raw(&key, trace_id).await;
                if target.satisfied_by(probe.as_ref().ok().map(Bytes::as_ref)) {
                    return Ok(());
                }
                return Err(error);
            }
        }
    }
    Err(FsError::Internal(format!(
        "teardown marker CAS budget exhausted for {key}"
    )))
}

/// Start the reclamation supervisor once. Runs on the caller's runtime,
/// which must outlive every request worker.
pub fn ensure_sweep_worker_started(coordinator: &Arc<SweepCoordinator>) {
    if coordinator
        .worker_started
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    compio_runtime::spawn(run_sweep_worker(coordinator.clone())).detach();
    tracing::info!("blob reclamation supervisor started");
}

async fn run_sweep_worker(coordinator: Arc<SweepCoordinator>) {
    let mut active = stream::FuturesUnordered::new();
    loop {
        while active.len() < SWEEP_CONCURRENCY {
            let Some(mut claim) = SweepClaim::take_ready(&coordinator) else {
                break;
            };
            active.push(async move {
                let attempt = process_sweep_work(claim.blob_guid, &mut claim.work).await;
                claim.schedule_retry(attempt);
            });
        }

        if active.is_empty() {
            rpc_client_common::rpc_sleep(SWEEP_POLL_INTERVAL).await;
            continue;
        }

        let completed = active.next().fuse();
        let poll = rpc_client_common::rpc_sleep(SWEEP_POLL_INTERVAL).fuse();
        futures::pin_mut!(completed, poll);
        futures::select_biased! {
            _ = completed => {}
            _ = poll => {}
        }
    }
}

/// Delete every `@ovr/` row of `blob_id`, paginated past the NSS clamp,
/// then remove the `@ovr-gc/` marker. Deleting the marker last is what
/// makes the protocol crash-safe: the marker survives any partial pass.
async fn delete_all_ovr_rows(
    backend: &StorageBackend,
    blob_id: Uuid,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    let prefix = ovr_row_prefix(&blob_id);
    let mut start_after = String::new();
    loop {
        let (page, has_more) = match backend
            .list_inodes_raw_page(&prefix, &start_after, ROW_TEARDOWN_PAGE, trace_id)
            .await
        {
            Ok(page) => page,
            Err(FsError::NotFound) => break,
            Err(error) => return Err(error),
        };
        let Some(last_key) = page.last().map(|(key, _)| key.clone()) else {
            break;
        };
        let results = stream::iter(page)
            .map(|(key, _)| async move {
                match backend.delete_inode(&key, trace_id).await {
                    Ok(_) | Err(FsError::NotFound) => Ok(()),
                    Err(error) => Err(error),
                }
            })
            .buffer_unordered(32)
            .collect::<Vec<_>>()
            .await;
        if let Some(error) = results.into_iter().find_map(Result::err) {
            return Err(error);
        }
        if !has_more {
            break;
        }
        start_after = last_key;
    }
    match backend.delete_inode(&ovr_gc_key(&blob_id), trace_id).await {
        Ok(_) | Err(FsError::NotFound) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn process_sweep_work(blob_guid: DataBlobGuid, work: &mut SweepWork) -> SweepAttempt {
    let Some(backend_config) = work.backend_config.clone() else {
        tracing::error!(%blob_guid, "sweep work without a bucket; dropping");
        work.victims.clear();
        work.below.clear();
        work.delete_all_blocks = false;
        work.delete_rows = false;
        work.marker_data_pending = None;
        work.grace_until = None;
        return SweepAttempt::Complete;
    };
    let backend = match StorageBackend::new(&backend_config) {
        Ok(backend) => backend,
        Err(error) => {
            tracing::warn!(%blob_guid, %error, "blob reclamation backend initialization failed");
            return SweepAttempt::Failed;
        }
    };
    let trace_id = TraceId::new();
    if let Some(not_before_unix_ms) = work.marker_data_pending {
        let target = TeardownMarkerWrite::DataPending {
            volume_id: blob_guid.volume_id,
            not_before_unix_ms,
        };
        match write_teardown_marker_cas(&backend, blob_guid, target, &trace_id).await {
            Ok(()) => work.marker_data_pending = None,
            Err(error) => {
                tracing::warn!(%blob_guid, %error, "data-pending teardown marker retry failed");
                return SweepAttempt::Failed;
            }
        }
    }
    if let Some(grace_until) = work.grace_until {
        if Instant::now() < grace_until {
            return SweepAttempt::GracePending;
        }
        work.grace_until = None;
    }

    let mut failed = false;
    if work.delete_all_blocks {
        if let Err(error) = backend.delete_blob_blocks(blob_guid, &trace_id).await {
            tracing::warn!(%blob_guid, %error, "whole-blob data deletion failed");
            failed = true;
        } else if work.delete_rows {
            let target = TeardownMarkerWrite::RowsReady {
                volume_id: blob_guid.volume_id,
            };
            match write_teardown_marker_cas(&backend, blob_guid, target, &trace_id).await {
                Ok(()) => {
                    work.delete_all_blocks = false;
                    work.victims.clear();
                    work.below.clear();
                }
                Err(error) => {
                    tracing::warn!(%blob_guid, %error, "rows-ready teardown marker write failed");
                    failed = true;
                }
            }
        } else {
            work.delete_all_blocks = false;
            work.victims.clear();
            work.below.clear();
        }
    }
    if work.delete_rows && !work.delete_all_blocks {
        if delete_all_ovr_rows(&backend, blob_guid.blob_id, &trace_id)
            .await
            .is_ok()
        {
            work.delete_rows = false;
        } else {
            failed = true;
        }
    }

    // Below-floor reclamation: one all-node listing resolves each block's
    // stale generations into exact identities.
    if !work.below.is_empty() {
        match backend.list_all_blob_blocks(blob_guid, &trace_id).await {
            Ok(entries) => {
                let resolved: Vec<(u32, u64)> = entries
                    .into_iter()
                    .filter_map(|entry| {
                        let superseded = work
                            .below
                            .get(&entry.block_number)
                            .is_some_and(|keep_from| entry.version < *keep_from);
                        superseded.then_some((entry.block_number, entry.version))
                    })
                    .collect();
                work.victims.extend(resolved);
                work.below.clear();
            }
            Err(error) => {
                tracing::warn!(%blob_guid, %error, "below-floor sweep listing failed");
                failed = true;
            }
        }
    }

    let victims = work.victims.iter().copied().collect::<Vec<_>>();
    let backend_ref = &backend;
    let trace_id_ref = &trace_id;
    let victim_results = stream::iter(victims)
        .map(move |identity @ (block, version)| async move {
            (
                identity,
                backend_ref
                    .delete_block(blob_guid, block, version, trace_id_ref)
                    .await,
            )
        })
        .buffer_unordered(32)
        .collect::<Vec<_>>()
        .await;
    for (identity, result) in victim_results {
        if result.is_ok() {
            work.victims.remove(&identity);
        } else {
            failed = true;
        }
    }

    if work.is_empty() {
        SweepAttempt::Complete
    } else {
        debug_assert!(failed);
        SweepAttempt::Failed
    }
}

enum ScavengedMarker {
    Conditional,
    LegacyDataPending,
    Work(DataBlobGuid, SweepWork),
}

fn work_for_teardown_marker(
    backend_config: Option<Arc<BackendConfig>>,
    blob_id: Uuid,
    value: &[u8],
) -> ScavengedMarker {
    match parse_ovr_gc_marker(value) {
        OvrGcMarker::Conditional => ScavengedMarker::Conditional,
        OvrGcMarker::LegacyDataPending => ScavengedMarker::LegacyDataPending,
        OvrGcMarker::DataPending {
            volume_id,
            not_before_unix_ms,
        } => {
            let blob_guid = DataBlobGuid { blob_id, volume_id };
            let mut work = SweepWork::new(backend_config);
            let grace_until = instant_for_unix_deadline(not_before_unix_ms);
            work.grace_until = Some(grace_until);
            work.ready_at = grace_until;
            work.delete_all_blocks = true;
            work.delete_rows = true;
            ScavengedMarker::Work(blob_guid, work)
        }
        OvrGcMarker::RowsReady { volume_id } => {
            let blob_guid = DataBlobGuid { blob_id, volume_id };
            let mut work = SweepWork::new(backend_config);
            work.delete_rows = true;
            ScavengedMarker::Work(blob_guid, work)
        }
    }
}

/// One pass over `@ovr-gc/`: resume the durable teardown phase a previous
/// process did not finish. Data-pending markers carry both the volume and
/// the wall-clock grace deadline, so restart waits the remaining grace,
/// deletes every physical key, and only then advances the marker to
/// rows-ready. A rows-ready marker proves data deletion already finished.
///
/// A marker is an INTENT, not proof: unlink and rename write it before
/// the namespace mutation, which can still fail and leave the blob
/// live. Every unrecognized value is an opaque conditional intent. The
/// scavenger neither probes its mutable path nor removes it: path state
/// is not global reachability proof, and removal would race a concurrent
/// promotion. Legacy `gc` markers lack a volume and grace deadline, so
/// they are retained fail-closed as well.
pub async fn scavenge_teardown_markers(
    backend_config: Arc<BackendConfig>,
    coordinator: Arc<SweepCoordinator>,
) {
    let backend = match StorageBackend::new(&backend_config) {
        Ok(backend) => backend,
        Err(error) => {
            tracing::warn!(%error, "teardown scavenger backend initialization failed");
            return;
        }
    };
    let trace_id = TraceId::new();
    let mut start_after = String::new();
    loop {
        let (page, has_more) = match backend
            .list_inodes_raw_page(OVR_GC_PREFIX, &start_after, ROW_TEARDOWN_PAGE, &trace_id)
            .await
        {
            Ok(page) => page,
            Err(FsError::NotFound) => return,
            Err(error) => {
                tracing::warn!(%error, "teardown scavenger listing failed");
                return;
            }
        };
        let Some(last_key) = page.last().map(|(key, _)| key.clone()) else {
            return;
        };
        for (key, value) in &page {
            let Some(blob_id) = parse_ovr_gc_blob_id(key) else {
                tracing::warn!(%key, "malformed @ovr-gc marker skipped");
                continue;
            };
            let (blob_guid, work) = match work_for_teardown_marker(
                Some(backend_config.clone()),
                blob_id,
                value,
            ) {
                ScavengedMarker::Conditional => {
                    tracing::warn!(%key, "conditional @ovr-gc marker retained without teardown commit");
                    continue;
                }
                ScavengedMarker::LegacyDataPending => {
                    tracing::warn!(%key, "legacy @ovr-gc marker retained without safe replay metadata");
                    continue;
                }
                ScavengedMarker::Work(blob_guid, work) => (blob_guid, work),
            };
            coordinator.queue.lock().enqueue(blob_guid, work);
        }
        if !has_more {
            return;
        }
        start_after = last_key;
    }
}

#[cfg(test)]
mod sweep_tests {
    use super::*;

    #[test]
    fn marker_writes_are_monotonic() {
        let data_pending = TeardownMarkerWrite::DataPending {
            volume_id: 7,
            not_before_unix_ms: 123,
        };
        let rows_ready = TeardownMarkerWrite::RowsReady { volume_id: 7 };
        let pending_value = encode_ovr_gc_data_pending(7, 123);
        let ready_value = encode_ovr_gc_rows_ready(7);

        assert!(!data_pending.satisfied_by(None));
        assert!(!data_pending.satisfied_by(Some(b"/doomed")));
        assert!(!data_pending.satisfied_by(Some(data_types::ovr_map::OVR_GC_LEGACY_VALUE)));
        assert!(data_pending.satisfied_by(Some(&pending_value)));
        assert!(data_pending.satisfied_by(Some(&ready_value)));

        assert!(!rows_ready.satisfied_by(None));
        assert!(!rows_ready.satisfied_by(Some(&pending_value)));
        assert!(rows_ready.satisfied_by(Some(&ready_value)));
    }

    #[test]
    fn scavenger_resumes_only_durable_teardown_phases() {
        let blob_id = Uuid::nil();
        assert!(matches!(
            work_for_teardown_marker(None, blob_id, b"/doomed"),
            ScavengedMarker::Conditional
        ));
        assert!(matches!(
            work_for_teardown_marker(None, blob_id, data_types::ovr_map::OVR_GC_LEGACY_VALUE),
            ScavengedMarker::LegacyDataPending
        ));

        let pending = encode_ovr_gc_data_pending(9, unix_time_millis().saturating_add(60_000));
        let ScavengedMarker::Work(guid, pending_work) =
            work_for_teardown_marker(None, blob_id, &pending)
        else {
            unreachable!("data-pending marker must produce work");
        };
        assert_eq!(guid.volume_id, 9);
        assert!(pending_work.delete_all_blocks);
        assert!(pending_work.delete_rows);
        assert!(pending_work.grace_until.is_some());

        let ready = encode_ovr_gc_rows_ready(11);
        let ScavengedMarker::Work(guid, ready_work) =
            work_for_teardown_marker(None, blob_id, &ready)
        else {
            unreachable!("rows-ready marker must produce work");
        };
        assert_eq!(guid.volume_id, 11);
        assert!(!ready_work.delete_all_blocks);
        assert!(ready_work.delete_rows);
        assert!(ready_work.grace_until.is_none());
    }

    #[test]
    fn pending_sweeps_coalesce_by_blob() {
        let mut pending = SweepWork::new(None);
        pending.victims.insert((3, 1));
        pending.below.insert(9, 6);
        let mut newer = SweepWork::new(None);
        newer.victims.extend([(3, 1), (4, 2)]);
        newer.below.insert(9, 4);
        newer.delete_rows = true;

        pending.merge(newer);

        assert_eq!(pending.victims, HashSet::from([(3, 1), (4, 2)]));
        assert_eq!(
            pending.below,
            HashMap::from([(9, 6)]),
            "below merges by max keep-from"
        );
        assert!(pending.delete_rows);
    }

    #[test]
    fn cancelled_sweep_claim_requeues_exact_work() {
        let blob_guid = DataBlobGuid {
            blob_id: Uuid::nil(),
            volume_id: 1,
        };
        let coordinator = Arc::new(SweepCoordinator::default());
        let mut work = SweepWork::new(None);
        work.victims.insert((7, 3));
        coordinator.queue.lock().pending.insert(blob_guid, work);

        let claim = SweepClaim::take_ready(&coordinator).expect("sweep claim should be ready");
        assert!(coordinator.queue.lock().active.contains(&blob_guid));
        drop(claim);

        let queue = coordinator.queue.lock();
        assert!(queue.active.is_empty());
        assert_eq!(
            queue
                .pending
                .get(&blob_guid)
                .expect("cancelled claim should be pending")
                .victims,
            HashSet::from([(7, 3)])
        );
    }

    #[test]
    fn cancelled_sweep_claim_merges_new_pending_work() {
        let blob_guid = DataBlobGuid {
            blob_id: Uuid::nil(),
            volume_id: 2,
        };
        let coordinator = Arc::new(SweepCoordinator::default());
        let mut claimed_work = SweepWork::new(None);
        claimed_work.victims.insert((7, 3));
        coordinator
            .queue
            .lock()
            .pending
            .insert(blob_guid, claimed_work);
        let claim = SweepClaim::take_ready(&coordinator).expect("sweep claim should be ready");

        let mut new_work = SweepWork::new(None);
        new_work.below.insert(2, 4);
        coordinator.queue.lock().pending.insert(blob_guid, new_work);
        drop(claim);

        let queue = coordinator.queue.lock();
        let pending = queue
            .pending
            .get(&blob_guid)
            .expect("merged sweep should be pending");
        assert_eq!(pending.victims, HashSet::from([(7, 3)]));
        assert_eq!(pending.below, HashMap::from([(2, 4)]));
    }
}
