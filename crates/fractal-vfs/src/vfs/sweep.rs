//! Background reclamation: superseded-generation sweeps, whole-blob
//! teardown (data keys plus `@ovr/` rows), and the `@ovr-gc/` marker
//! protocol that makes teardown crash-safe.

use std::collections::{HashMap, HashSet, hash_map};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use data_types::ovr_map::{OVR_GC_PREFIX, ovr_gc_key, ovr_row_prefix, parse_ovr_gc_blob_id};
use data_types::{DataBlobGuid, TraceId};
use futures::{FutureExt, StreamExt, stream};
use uuid::Uuid;

use crate::backend::{BackendConfig, StorageBackend};
use crate::error::FsError;
use crate::vfs::VfsCore;
use data_types::object_layout::ObjectLayout;

pub(crate) const SWEEP_CONCURRENCY: usize = 8;
const SWEEP_POLL_INTERVAL: Duration = Duration::from_millis(250);
/// NSS listing page size for row teardown. Deliberately above the NSS
/// clamp so the `has_more` loop, not the request size, bounds coverage.
const ROW_TEARDOWN_PAGE: u32 = 1000;

// Grace before reclaiming a superseded generation: see
// `rpc_client_common::reclamation_grace` (shared with the api_server
// deletion worker). A read that outlives it can fail mid-flight but
// never sees wrong bytes (keys are write-once: the failure mode is a
// miss, not stale data).
use rpc_client_common::reclamation_grace;

#[derive(Debug)]
pub(crate) struct SweepWork {
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
    grace_until: Option<Instant>,
    retry_count: u32,
    ready_at: Instant,
}

impl SweepWork {
    pub(crate) fn new() -> Self {
        Self {
            victims: HashSet::new(),
            below: HashMap::new(),
            delete_all_blocks: false,
            delete_rows: false,
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
            && self.grace_until.is_none()
    }

    fn merge(&mut self, mut other: Self) {
        self.victims.extend(other.victims.drain());
        for (block, keep_from) in other.below.drain() {
            let slot = self.below.entry(block).or_insert(keep_from);
            *slot = (*slot).max(keep_from);
        }
        self.delete_all_blocks |= other.delete_all_blocks;
        self.delete_rows |= other.delete_rows;
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
pub(crate) struct SweepCoordinator {
    pub(crate) queue: parking_lot::Mutex<SweepQueue>,
    worker_started: AtomicBool,
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
        let work = std::mem::replace(&mut self.work, SweepWork::new());
        queue.enqueue(self.blob_guid, work);
    }
}

impl VfsCore {
    fn graced_work(&self) -> SweepWork {
        let mut work = SweepWork::new();
        let now = Instant::now();
        let grace_until = now
            .checked_add(reclamation_grace(
                self.backend_config.config.rpc_request_timeout(),
            ))
            .unwrap_or(now);
        work.grace_until = Some(grace_until);
        work.ready_at = grace_until;
        work
    }

    /// Queue reclamation of the identities a commit superseded. `victims`
    /// come exactly from the row CAS (the displaced `cur` slots plus the
    /// v1 generation of rewritten unmapped blocks); `below` covers blocks
    /// whose stale claims have no row record and need one listing.
    pub(crate) fn enqueue_superseded_sweep(
        &self,
        blob_guid: DataBlobGuid,
        victims: Vec<(u32, u64)>,
        below: Vec<(u32, u64)>,
    ) {
        if victims.is_empty() && below.is_empty() {
            return;
        }
        let mut work = self.graced_work();
        work.victims.extend(victims);
        for (block, keep_from) in below {
            let slot = work.below.entry(block).or_insert(keep_from);
            *slot = (*slot).max(keep_from);
        }
        self.enqueue_sweep_work(blob_guid, work);
    }

    /// Reclaim the fragments of a create attempt whose publish never
    /// landed. The blob_guid was freshly minted and is unreachable, so
    /// tear everything down after the grace.
    pub(crate) async fn cleanup_unpublished_blob(
        &self,
        blob_guid: DataBlobGuid,
        identities: Vec<(u32, u64)>,
    ) {
        let mut work = self.graced_work();
        work.victims.extend(identities);
        work.delete_all_blocks = true;
        self.enqueue_sweep_work(blob_guid, work);
    }

    /// Tear down every exact data/reservation key and every `@ovr/` row
    /// belonging to a blob, after the reclamation grace. The caller must
    /// have written the `@ovr-gc/` marker (see `write_teardown_marker`)
    /// before the inode was deleted; the sweep removes it when the rows
    /// are gone, and the init-time scavenger replays markers a crash
    /// left behind.
    pub(crate) async fn teardown_blob(&self, layout: &ObjectLayout) {
        let Ok(blob_guid) = layout.blob_guid() else {
            return;
        };
        let mut work = self.graced_work();
        work.delete_all_blocks = true;
        work.delete_rows = true;
        self.enqueue_sweep_work(blob_guid, work);
    }

    /// Best-effort `@ovr-gc/{blob_id}` teardown-intent marker. Written
    /// before the inode delete so a crash mid-teardown cannot leak the
    /// blob's rows forever (the blob_id is unrecoverable from any
    /// surviving key once the inode is gone). Only mapped blobs need
    /// one: an unmapped blob has no rows to leak.
    pub(crate) async fn write_teardown_marker(&self, layout: &ObjectLayout, trace_id: &TraceId) {
        if !layout.is_mapped() {
            return;
        }
        let Ok(blob_guid) = layout.blob_guid() else {
            return;
        };
        let key = ovr_gc_key(&blob_guid.blob_id);
        // Value content is irrelevant (NSS rejects empty values); the
        // marker's existence is the record.
        if let Err(error) = self
            .backend()
            .put_inode(&key, bytes::Bytes::from_static(b"gc"), trace_id)
            .await
        {
            tracing::warn!(%blob_guid, %error, "teardown marker write failed; rows may leak on crash");
        }
    }

    pub(crate) fn enqueue_sweep_work(&self, blob_guid: DataBlobGuid, work: SweepWork) {
        self.sweep_coordinator.queue.lock().enqueue(blob_guid, work);
    }

    /// Start the reclamation supervisor on the lifecycle runtime. This
    /// runtime survives every request ring and is the only runtime that
    /// owns sweep claims.
    pub(crate) fn ensure_sweep_worker_started(&self) {
        if self
            .sweep_coordinator
            .worker_started
            .load(Ordering::Relaxed)
        {
            return;
        }
        if self
            .sweep_coordinator
            .worker_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        compio_runtime::spawn(run_sweep_worker(
            self.backend_config.clone(),
            self.sweep_coordinator.clone(),
        ))
        .detach();
        // One lazy scavenge pass per mount: markers a crashed teardown
        // left behind become row-teardown work again.
        compio_runtime::spawn(scavenge_teardown_markers(
            self.backend_config.clone(),
            self.sweep_coordinator.clone(),
        ))
        .detach();
        tracing::info!("blob reclamation supervisor started");
    }

    /// Release mount-local writer state and enqueue cleanup that was
    /// waiting for the final open handle. Runs after dirty handles and
    /// metadata have drained, when no request worker can create another
    /// handle.
    pub async fn prepare_sweep_shutdown(&self) {
        let handle_ids = self
            .file_handles
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for fh in handle_ids {
            let Some((_, handle)) = self.file_handles.remove(&fh) else {
                continue;
            };
            if handle.write_buf.is_some() {
                self.release_write_lock(handle.ino, fh);
            }
        }
        self.inode_write_owner.clear();

        let trace_id = TraceId::new();
        let deferred_inodes = self
            .deferred_blob_cleanup
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for ino in deferred_inodes {
            if let Some((_, old_bytes)) = self.deferred_blob_cleanup.remove(&ino) {
                self.cleanup_orphaned_value("", Some(ino), old_bytes, &trace_id)
                    .await;
            }
        }
    }

    /// Wait until every runnable reclamation item finishes. A pending
    /// item whose ready_at was re-armed into the future (grace or
    /// failure backoff) is abandoned: the end state is the same
    /// tolerated invisible garbage as the bounded-timeout path, and
    /// `log_incomplete_sweep_work` reports it.
    pub async fn drain_sweep_work(&self) {
        {
            let now = Instant::now();
            let mut queue = self.sweep_coordinator.queue.lock();
            for work in queue.pending.values_mut() {
                work.ready_at = now;
            }
        }
        loop {
            let idle = {
                let now = Instant::now();
                let queue = self.sweep_coordinator.queue.lock();
                queue.active.is_empty() && queue.pending.values().all(|work| work.ready_at > now)
            };
            if idle {
                return;
            }
            compio_runtime::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Report reclamation left behind when the bounded shutdown drain
    /// expires. Namespace visibility is already gone, but physical
    /// blocks or `@ovr/` rows can remain until a later pass.
    pub fn log_incomplete_sweep_work(&self) {
        let queue = self.sweep_coordinator.queue.lock();
        if queue.pending.is_empty() && queue.active.is_empty() {
            return;
        }
        let pending_victims = queue
            .pending
            .values()
            .map(|work| work.victims.len())
            .sum::<usize>();
        let pending_below = queue
            .pending
            .values()
            .map(|work| work.below.len())
            .sum::<usize>();
        tracing::error!(
            pending_blobs = queue.pending.len(),
            active_blobs = queue.active.len(),
            pending_victims,
            pending_below,
            open_handles = self.file_handles.len(),
            deferred_blobs = self.deferred_blob_cleanup.len(),
            "destroy: reclamation incomplete; invisible physical garbage may remain"
        );
    }
}

async fn run_sweep_worker(backend_config: Arc<BackendConfig>, coordinator: Arc<SweepCoordinator>) {
    let mut active = stream::FuturesUnordered::new();
    loop {
        while active.len() < SWEEP_CONCURRENCY {
            let Some(mut claim) = SweepClaim::take_ready(&coordinator) else {
                break;
            };
            let backend_config = backend_config.clone();
            active.push(async move {
                let attempt =
                    process_sweep_work(&backend_config, claim.blob_guid, &mut claim.work).await;
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

async fn process_sweep_work(
    backend_config: &BackendConfig,
    blob_guid: DataBlobGuid,
    work: &mut SweepWork,
) -> SweepAttempt {
    let backend = match StorageBackend::new(backend_config) {
        Ok(backend) => backend,
        Err(error) => {
            tracing::warn!(%blob_guid, %error, "blob reclamation backend initialization failed");
            return SweepAttempt::Failed;
        }
    };
    let trace_id = TraceId::new();
    if let Some(grace_until) = work.grace_until {
        if Instant::now() < grace_until {
            return SweepAttempt::GracePending;
        }
        work.grace_until = None;
    }

    let mut failed = false;
    if work.delete_all_blocks {
        if backend
            .delete_blob_blocks(blob_guid, &trace_id)
            .await
            .is_ok()
        {
            work.delete_all_blocks = false;
            work.victims.clear();
            work.below.clear();
        } else {
            failed = true;
        }
    }
    if work.delete_rows {
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

/// One pass over `@ovr-gc/`: every marker is a teardown a previous
/// process started but did not finish. Re-enqueue its row teardown.
/// The data-key side needs no replay: `delete_blob_blocks` ran before
/// the marker's rows, and a leaked data key without an inode is found
/// by scrub, not by this scavenger.
async fn scavenge_teardown_markers(
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
        for (key, _) in &page {
            let Some(blob_id) = parse_ovr_gc_blob_id(key) else {
                tracing::warn!(%key, "malformed @ovr-gc marker skipped");
                continue;
            };
            // volume_id is unknown from the marker; rows are keyed by
            // blob_id alone and delete_rows never touches BSS, so a
            // placeholder volume is fine for this work item.
            let blob_guid = DataBlobGuid {
                blob_id,
                volume_id: 0,
            };
            let mut work = SweepWork::new();
            work.delete_rows = true;
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
    fn pending_sweeps_coalesce_by_blob() {
        let mut pending = SweepWork::new();
        pending.victims.insert((3, 1));
        pending.below.insert(9, 6);
        let mut newer = SweepWork::new();
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
        let mut work = SweepWork::new();
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
        let mut claimed_work = SweepWork::new();
        claimed_work.victims.insert((7, 3));
        coordinator
            .queue
            .lock()
            .pending
            .insert(blob_guid, claimed_work);
        let claim = SweepClaim::take_ready(&coordinator).expect("sweep claim should be ready");

        let mut new_work = SweepWork::new();
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
