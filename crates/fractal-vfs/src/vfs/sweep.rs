//! Background reclamation: touched-block sweeps and whole-blob teardown.

#[allow(unused_imports)]
use super::*;

impl VfsCore {
    /// Queue touched-block reclamation for coalesced, bounded background
    /// work: for each block, every listed generation below its new
    /// committed identity is deleted on every placement node, after the
    /// reclamation grace. INTERIM: reader-lease pinning was removed, so
    /// the grace is the only protection an in-flight read of a superseded
    /// generation gets; a positive reader-pinning mechanism is future
    /// work, and until then a read that outlives the grace can fail
    /// mid-flight (never serve wrong bytes: keys are write-once).
    pub(crate) fn enqueue_superseded_sweep(
        &self,
        blob_guid: data_types::DataBlobGuid,
        _committed_version: u64,
        below: Vec<(u32, u64)>,
    ) {
        if below.is_empty() {
            return;
        }
        let mut work = SweepWork::new();
        for (block, keep_from) in below {
            let slot = work.below.entry(block).or_insert(keep_from);
            *slot = (*slot).max(keep_from);
        }
        let now = Instant::now();
        let grace_until = now
            .checked_add(reclamation_grace(
                self.backend_config.config.rpc_request_timeout(),
            ))
            .unwrap_or(now);
        work.grace_until = Some(grace_until);
        work.ready_at = grace_until;
        self.enqueue_sweep_work(blob_guid, work);
    }

    pub(crate) fn enqueue_sweep_work(&self, blob_guid: DataBlobGuid, work: SweepWork) {
        let mut queue = self.sweep_coordinator.queue.lock();
        match queue.pending.entry(blob_guid) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().merge(work);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(work);
            }
        }
    }

    pub(crate) async fn cleanup_unpublished_blob(
        &self,
        blob_guid: data_types::DataBlobGuid,
        identities: Vec<(u32, u64)>,
        _trace_id: &TraceId,
    ) {
        let mut work = SweepWork::new();
        work.victims.extend(identities);
        let now = Instant::now();
        let grace_until = now
            .checked_add(reclamation_grace(
                self.backend_config.config.rpc_request_timeout(),
            ))
            .unwrap_or(now);
        work.grace_until = Some(grace_until);
        work.ready_at = grace_until;
        work.delete_all_blocks = true;
        self.enqueue_sweep_work(blob_guid, work);
    }

    /// Tear down every exact data and reservation key belonging to a
    /// blob, after the reclamation grace. The data listing is
    /// proportional to physical keys, so a sparse file does not trigger
    /// a logical-size walk. INTERIM: with reader-lease pinning removed,
    /// the grace is the only cover an in-flight read of the unlinked
    /// blob gets.
    pub(crate) async fn teardown_blob(&self, layout: &ObjectLayout, _trace_id: &TraceId) {
        let Ok(blob_guid) = layout.blob_guid() else {
            return;
        };
        let mut work = SweepWork::new();
        let now = Instant::now();
        let grace_until = now
            .checked_add(reclamation_grace(
                self.backend_config.config.rpc_request_timeout(),
            ))
            .unwrap_or(now);
        work.grace_until = Some(grace_until);
        work.ready_at = grace_until;
        work.delete_all_blocks = true;
        self.enqueue_sweep_work(blob_guid, work);
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
        tracing::info!("blob reclamation supervisor started");
    }

    /// Release mount-local reader state and enqueue cleanup that was waiting
    /// for the final open handle. This runs after dirty handles and metadata
    /// have drained, when no request worker can create another handle.
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

    /// Wait until every queued or claimed reclamation item finishes. The
    /// caller supplies the shutdown deadline so cancellation drops active
    /// claims back into the pending queue without losing exact identities.
    pub async fn drain_sweep_work(&self) {
        {
            let now = Instant::now();
            let mut queue = self.sweep_coordinator.queue.lock();
            for work in queue.pending.values_mut() {
                work.ready_at = now;
            }
        }
        // Drain only work that is runnable now. A pending item whose
        // ready_at moved back into the future was re-armed after the
        // force-ready pass above (the reclamation grace, a reader-lease
        // recheck, or a failure backoff); waiting those out would hold
        // shutdown for up to reclamation_grace (rpc timeout + slack)
        // per commit. Abandon them instead: the end state is the same
        // tolerated invisible garbage as the bounded-timeout path, and
        // `log_incomplete_sweep_work` reports it.
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
    /// expires. Namespace visibility is already gone, but physical blocks
    /// or block-map rows can remain until a later garbage-collection pass.
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
