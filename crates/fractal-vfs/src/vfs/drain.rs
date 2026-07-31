//! Flush orchestration: fsync/close flushes, writeback draining, release.

use data_types::TraceId;
use data_types::object_layout::ObjectLayout;
use fractal_fuse::{FileHandleId, InodeId};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::WritebackMode;
use crate::error::FsError;
use crate::inode::EntryType;
use crate::vfs::VfsCore;
use crate::writeback::{Generation, WritebackQueue};

impl VfsCore {
    pub async fn vfs_flush(&self, fh: FileHandleId) -> Result<(), FsError> {
        self.ensure_writeback_worker_started();

        let inode = if self.writeback_mode == WritebackMode::Default {
            self.file_handles.get(&fh).map(|h| h.ino)
        } else {
            None
        };

        // Drain queued cycles BEFORE the inline flush so a queued
        // lower-generation publish lands first and can't be reordered
        // after (and a stale worker put can't overwrite) the fresh CAS
        // publish this flush is about to make.
        if let Some(inode) = inode {
            self.drain_inode_to_barrier(inode).await?;
        }

        self.flush_write_buffer(fh).await?;

        // Post-flush drain: wait for any cycle that raced in (e.g. an
        // async release flush on another handle) and surface deferred
        // EIO if a drained cycle failed. No-op in strict mode (the
        // queue is always empty there) and for idle inodes.
        //
        // This is the durability barrier used by fsync(2) / O_SYNC. The
        // default-mode close(2) path leaves work to FUSE_RELEASE:
        // blocking every close on a worker tick erases the writeback win
        // on create-heavy workloads (tar -xf, cp -r).
        if let Some(inode) = inode {
            self.drain_inode_to_barrier(inode).await?;
        }

        Ok(())
    }

    /// FUSE_FLUSH close path. Strict mode keeps legacy close-time error
    /// reporting by flushing synchronously. Default writeback mode leaves
    /// the dirty publish to FUSE_RELEASE so create-heavy closes can
    /// pipeline.
    pub async fn vfs_flush_for_close(&self, fh: FileHandleId) -> Result<(), FsError> {
        match self.writeback_mode {
            WritebackMode::Strict => self.vfs_flush(fh).await,
            WritebackMode::Default => Ok(()),
        }
    }

    /// Variant of `vfs_flush` for callers that need to publish buffered
    /// data without waiting on the writeback barrier: it
    /// publishes the buffered write data synchronously (so write errors
    /// still surface at close) but does not wait on the writeback
    /// barrier. The placeholder/metadata cycle stays queued and the
    /// worker drains it on its next tick; any deferred error propagates
    /// on the next open/fsync of the same path.
    pub async fn vfs_flush_no_drain(&self, fh: FileHandleId) -> Result<(), FsError> {
        self.ensure_writeback_worker_started();
        self.flush_write_buffer(fh).await
    }

    /// Force any still-dirty write handles through the publish path. Used by
    /// destroy because FUSE_RELEASE may still be queued when shutdown starts.
    pub async fn flush_open_dirty_handles(&self) -> Result<(), FsError> {
        if self.writeback_mode != WritebackMode::Default {
            return Ok(());
        }
        let dirty_fhs: Vec<(FileHandleId, InodeId)> = self
            .file_handles
            .iter()
            .filter(|e| e.value().write_buf.as_ref().is_some_and(|wb| wb.dirty))
            .map(|e| (*e.key(), e.value().ino))
            .collect();

        self.flush_dirty_handles(dirty_fhs, "all").await
    }

    pub(crate) fn dirty_handles_for_inode(&self, inode: InodeId) -> Vec<(FileHandleId, InodeId)> {
        self.dirty_write_owner(inode)
            .map(|fh| vec![(fh, inode)])
            .unwrap_or_default()
    }

    pub(crate) fn dirty_handles_under_prefix(&self, prefix: &str) -> Vec<(FileHandleId, InodeId)> {
        self.file_handles
            .iter()
            .filter(|e| {
                e.value().s3_key.starts_with(prefix)
                    && e.value().write_buf.as_ref().is_some_and(|wb| wb.dirty)
            })
            .map(|e| (*e.key(), e.value().ino))
            .collect()
    }

    pub(crate) async fn flush_dirty_handles_for_inode(
        &self,
        inode: InodeId,
    ) -> Result<(), FsError> {
        if self.writeback_mode != WritebackMode::Default {
            return Ok(());
        }
        let dirty_fhs = self.dirty_handles_for_inode(inode);
        self.flush_dirty_handles(dirty_fhs, "inode").await
    }

    pub(crate) async fn flush_dirty_handles_under_prefix(
        &self,
        prefix: &str,
    ) -> Result<(), FsError> {
        if self.writeback_mode != WritebackMode::Default {
            return Ok(());
        }
        let dirty_fhs = self.dirty_handles_under_prefix(prefix);
        self.flush_dirty_handles(dirty_fhs, "prefix").await
    }

    pub(crate) async fn flush_dirty_handles(
        &self,
        dirty_fhs: Vec<(FileHandleId, InodeId)>,
        scope: &'static str,
    ) -> Result<(), FsError> {
        let mut failed = false;
        for (fh, ino) in dirty_fhs {
            match self.flush_write_buffer(fh).await {
                Ok(()) | Err(FsError::BadFd) => {}
                Err(e) => {
                    failed = true;
                    tracing::warn!(fh = fh.0, ino = ino.0, scope, error = %e, "dirty handle flush failed");
                }
            }
        }
        if failed {
            return Err(FsError::Internal("writeback drain".to_string()));
        }
        Ok(())
    }

    /// Drain every writeback cycle for `inode` whose generation is
    /// at or below the barrier captured at entry. Returns when every
    /// cycle has reached `Done` (success or short-circuit on failure).
    /// Surfaces deferred `EIO` if any drained cycle failed.
    pub async fn drain_inode_to_barrier(&self, inode: InodeId) -> Result<(), FsError> {
        let Some(barrier) = self.writeback.fsync_barrier(inode) else {
            // Idle inode: nothing queued. A lingering taint from an
            // earlier failed publish still surfaces as deferred EIO,
            // once: consuming it here keeps a single failed publish
            // from wedging every later open/fsync of the inode in
            // permanent EIO with no recovery path.
            if self.writeback.take_taint(inode) {
                self.drop_cached_layout(inode);
                return Err(FsError::Internal("writeback drain".to_string()));
            }
            return Ok(());
        };

        self.wait_cycles_drained(inode, barrier).await?;

        // Surface a deferred error (once) if the drained cycles tainted
        // the inode. The FUSE layer will translate to EIO; the
        // application is expected to close-and-reopen on the remote
        // winner.
        if self.writeback.take_taint(inode) {
            self.drop_cached_layout(inode);
            return Err(FsError::Internal("writeback drain".to_string()));
        }

        Ok(())
    }

    /// Drop the cached layout for `inode` so the next access cold-fetches
    /// from NSS. Called when a publish taint is consumed: the local
    /// layout (and any symlink target it carries) is what failed to
    /// publish, so re-serving it after the one-shot EIO would hand back
    /// stale state that lost to the remote winner. Clearing it forces the
    /// post-EIO retry down the cold-fetch path, mirroring an eviction.
    pub(crate) fn drop_cached_layout(&self, inode: InodeId) {
        if let Some(mut e) = self.inodes.get_mut(inode) {
            e.layout = None;
        }
    }

    /// Every inode with writeback state attached to `key`: the cached
    /// InodeTable entry plus any inode the queue still tracks for the
    /// key (an intent outlives its entry when a FORGET raced the
    /// enqueue). Draining only the table's inode would let the worker
    /// commit the orphaned intent after a delete and resurrect the name.
    pub(crate) fn writeback_drain_targets(&self, key: &str, ino: Option<InodeId>) -> Vec<InodeId> {
        let mut targets = self.writeback.intent_inodes_for_key(key);
        if let Some(ino) = ino
            && !targets.contains(&ino)
        {
            targets.push(ino);
        }
        targets
    }

    pub(crate) fn writeback_drain_targets_under_prefix(&self, prefix: &str) -> Vec<InodeId> {
        let mut targets = self.writeback.intent_inodes_for_key_prefix(prefix);
        for (inode, _) in self.writeback.snapshot_dirty_cycles() {
            if let Some(entry) = self.inodes.get(inode)
                && entry.s3_key.starts_with(prefix)
                && !targets.contains(&inode)
            {
                targets.push(inode);
            }
        }
        targets.sort_unstable();
        targets.dedup();
        targets
    }

    pub(crate) async fn drain_writeback_under_prefix(&self, prefix: &str) -> Result<(), FsError> {
        if self.writeback_mode != WritebackMode::Default {
            return Ok(());
        }
        self.flush_dirty_handles_under_prefix(prefix).await?;
        for ino in self.writeback_drain_targets_under_prefix(prefix) {
            self.drain_inode_to_barrier(ino).await?;
        }
        Ok(())
    }

    /// True if a regular-file child lives under `dir_key` in local state
    /// that the NSS emptiness probe cannot yet see. A file create publishes
    /// its final layout on FUSE_RELEASE (a writeback cycle), not as a
    /// PutInode intent, so between create and the release publish landing in
    /// NSS the child is visible only here: as an open handle or an in-flight
    /// writeback cycle. `rmdir` consults this so it honours the POSIX
    /// non-empty contract instead of deleting a directory out from under an
    /// in-flight child publish. `dir_key` ends in '/'; the directory marker
    /// itself is excluded by the length check.
    pub(crate) fn has_local_file_child_under(&self, dir_key: &str) -> bool {
        for h in self.file_handles.iter() {
            let k = &h.value().s3_key;
            if k.len() > dir_key.len()
                && k.starts_with(dir_key)
                // An unlinked-but-still-open fd is a POSIX orphan, not a
                // directory child: its publish is suppressed
                // (`name_removed`), so it must not hold rmdir hostage.
                && !self
                    .inodes
                    .get(h.value().ino)
                    .is_some_and(|e| e.name_removed)
            {
                return true;
            }
        }
        for (inode, _) in self.writeback.snapshot_dirty_cycles() {
            if let Some(e) = self.inodes.get(inode)
                && e.entry_type == EntryType::File
                && !e.name_removed
                && e.s3_key.len() > dir_key.len()
                && e.s3_key.starts_with(dir_key)
            {
                return true;
            }
        }
        false
    }

    /// Drain variant for the unlink / rmdir path. Like
    /// `drain_inode_to_barrier` it waits for in-flight publishes so the
    /// worker cannot resurrect the name after the delete, but a publish
    /// taint does NOT abort the delete: removing a name has no durability
    /// dependency on a prior write, and `mark_failed` already
    /// dropped the pending intent, so nothing can be resurrected. The
    /// taint is cleared since the inode is going away. A genuine drain
    /// timeout still aborts (an intent may still be in flight).
    ///
    /// Returns `true` if a taint was cleared: the entry's create publish
    /// failed, so NSS has nothing for the (still locally visible) name and
    /// the caller must treat a NSS miss as a successful local-only delete,
    /// not ENOENT.
    pub async fn drain_inode_for_delete(&self, inode: InodeId) -> Result<bool, FsError> {
        if let Some(barrier) = self.writeback.fsync_barrier(inode) {
            self.wait_cycles_drained(inode, barrier).await?;
        }
        Ok(self.writeback.clear_taint(inode))
    }

    /// Poll until every cycle on `inode` at or below `barrier` reaches
    /// `Done`, or the drain deadline elapses. The worker drains every
    /// poll_ms; a 1ms poll keeps the barrier latency bounded to a tick or
    /// two.
    pub(crate) async fn wait_cycles_drained(
        &self,
        inode: InodeId,
        barrier: Generation,
    ) -> Result<(), FsError> {
        let poll_dur = Duration::from_millis(1);
        let timeout_secs = self.backend_config.config.rpc_request_timeout_seconds * 4;
        // Monotonic clock: a wall-clock step (NTP/manual) must not turn a
        // healthy drain into a spurious timeout or an unbounded wait.
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        loop {
            if self.writeback.cycles_at_or_below_drained(inode, barrier) {
                return Ok(());
            }
            if Instant::now() > deadline {
                tracing::warn!(
                    inode = inode.0,
                    barrier = barrier.0,
                    "writeback drain timeout"
                );
                return Err(FsError::Internal("writeback drain".to_string()));
            }
            compio_runtime::time::sleep(poll_dur).await;
        }
    }

    /// Mount-wide writeback barrier: drain every dirty cycle the queue
    /// currently knows about. Used by `fsyncdir(2)`. A true subtree-scoped
    /// variant is a future optimization; this is a cheap, correct barrier.
    pub async fn drain_all_dirty_cycles(&self) -> Result<(), FsError> {
        self.flush_open_dirty_handles().await?;
        self.drain_dirty_cycles_inner(false).await
    }

    /// fsyncdir(2): drain every dirty cycle, then surface (and consume)
    /// deferred publish failures for entries under this directory. The
    /// durability-conscious create protocol (create, write, close, then
    /// fsync the parent dir fd) never re-opens the child, so this barrier
    /// is its only chance to learn a queued child publish was dropped;
    /// returning success there would report a lost file as durable.
    /// Taints outside the subtree stay put for their own fsync/open path.
    pub async fn vfs_fsyncdir(&self, dir_ino: InodeId) -> Result<(), FsError> {
        self.drain_all_dirty_cycles().await?;
        if self.writeback_mode != WritebackMode::Default {
            return Ok(());
        }
        let Some(prefix) = self.dir_prefix(dir_ino) else {
            return Ok(());
        };
        let mut failed = false;
        for ino in self.writeback.tainted_inodes() {
            let under = self
                .inodes
                .get(ino)
                .is_some_and(|e| e.s3_key.len() > prefix.len() && e.s3_key.starts_with(&prefix));
            if under && self.writeback.take_taint(ino) {
                self.drop_cached_layout(ino);
                failed = true;
            }
        }
        if failed {
            return Err(FsError::Internal("writeback drain".to_string()));
        }
        Ok(())
    }

    /// Unmount variant of `drain_all_dirty_cycles`: re-snapshots until a
    /// pass comes back empty, so a cycle enqueued after an earlier
    /// snapshot (e.g. a release flush racing shutdown) is waited on too.
    /// Terminates because `set_enqueue_blocked` stops the intent side
    /// from growing and the remaining producers (in-flight releases)
    /// are finite.
    pub async fn drain_all_dirty_cycles_until_empty(&self) -> Result<(), FsError> {
        self.flush_open_dirty_handles().await?;
        self.drain_dirty_cycles_inner(true).await
    }

    pub(crate) async fn drain_dirty_cycles_inner(&self, until_empty: bool) -> Result<(), FsError> {
        let poll_dur = Duration::from_millis(5);
        let timeout_secs = self.backend_config.config.rpc_request_timeout_seconds * 4;
        // Monotonic clock: see wait_cycles_drained.
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        let mut empty_passes = 0u8;
        loop {
            let snapshot = self.writeback.snapshot_dirty_cycles();
            if snapshot.is_empty() {
                if until_empty && empty_passes == 0 {
                    empty_passes += 1;
                    if Instant::now() > deadline {
                        return Err(FsError::Internal("writeback drain timeout".to_string()));
                    }
                    compio_runtime::time::sleep(poll_dur).await;
                    continue;
                }
                break;
            }
            empty_passes = 0;
            for (inode, barrier) in snapshot {
                loop {
                    if self.writeback.cycles_at_or_below_drained(inode, barrier) {
                        break;
                    }
                    if Instant::now() > deadline {
                        tracing::warn!(
                            inode = inode.0,
                            barrier = barrier.0,
                            "drain_all_dirty_cycles timeout"
                        );
                        return Err(FsError::Internal("writeback drain timeout".to_string()));
                    }
                    compio_runtime::time::sleep(poll_dur).await;
                }
            }
            if !until_empty {
                break;
            }
        }
        // This barrier waits for in-flight cycles but must NOT consume a
        // per-inode taint. A taint belongs to that file's own fsync / open
        // path, or, for entries under a fsyncdir target, to the scoped
        // sweep in `vfs_fsyncdir`; consuming it here on an unrelated
        // barrier would both hand this caller a spurious EIO and
        // hide the deferred write error from the file's real owner. The
        // unmount drain (until_empty) does sweep taints so a lost publish is
        // surfaced, and pairs each with a cached-layout drop so post-error
        // access cold-fetches instead of re-serving the layout that lost.
        if until_empty {
            let tainted = self.writeback.take_all_taints();
            for &inode in &tainted {
                self.drop_cached_layout(inode);
            }
            if !tainted.is_empty() {
                return Err(FsError::Internal("writeback drain".to_string()));
            }
        }
        Ok(())
    }

    pub async fn vfs_release(&self, fh: FileHandleId) -> Result<(), FsError> {
        // Flush any dirty write buffer before releasing
        let (has_dirty, was_writer) = self
            .file_handles
            .get(&fh)
            .map(|h| {
                let dirty = h.write_buf.as_ref().map(|wb| wb.dirty).unwrap_or(false);
                let writer = h.write_buf.is_some();
                (dirty, writer)
            })
            .unwrap_or((false, false));

        // Flush, but DON'T early-return on error: the handle and its
        // inode-scoped write lock must always be torn down on release,
        // even when the close-time flush fails (e.g. a transient CAS
        // conflict or RPC timeout). Returning early here would leave the
        // FileHandle in `file_handles`, so `acquire_write_lock`'s
        // stale-owner reclaim (which only fires when the owner fh is GONE
        // from the table) never triggers, and the inode stays wedged at
        // EBUSY for the lifetime of the mount, observed as
        // `echo x > f; open f O_TRUNC` returning EBUSY in open/00.t. The
        // flush error is still surfaced to the caller after cleanup.
        let flush_res = if has_dirty {
            self.flush_write_buffer(fh).await
        } else {
            Ok(())
        };

        // Get the inode before removing the handle
        let ino = self.file_handles.get(&fh).map(|h| h.ino);
        self.file_handles.remove(&fh);

        // Release the inode-scoped write lock if this handle held it.
        // Read-only handles never acquired it.
        if was_writer && let Some(ino) = ino {
            self.release_write_lock(ino, fh);
        }

        flush_res?;

        // Handle deferred blob cleanup for unlinked files
        if let Some(ino) = ino
            && let Some((_, old_bytes)) = self.deferred_blob_cleanup.remove(&ino)
        {
            if !self.has_open_handles_for_inode(ino, None) {
                // Last handle closed, clean up blobs now
                let trace_id = TraceId::new();
                if let Ok(old_layout) =
                    rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
                    && let Ok(blob_guid) = old_layout.blob_guid()
                    && let Err(e) = self
                        .backend()
                        .delete_blob_blocks(blob_guid, &trace_id)
                        .await
                {
                    tracing::warn!(%blob_guid, error = %e, "orphaned blob cleanup incomplete");
                }
            } else {
                // Still more handles open, re-insert
                self.deferred_blob_cleanup.insert(ino, old_bytes);
            }
        }

        Ok(())
    }

    /// Decide whether `FUSE_RELEASE` should flush this handle off the
    /// FUSE worker thread. Returns `Some(inode)` only when the mount is in
    /// `Default` writeback mode AND the handle has a dirty write buffer,
    /// i.e. an async close-flush is both safe and worthwhile. `None` means
    /// "release inline": a read-only handle, a clean buffer, or `Strict`
    /// mode where close must publish synchronously.
    pub fn peek_release_state(&self, fh: FileHandleId) -> Option<InodeId> {
        if self.writeback_mode != WritebackMode::Default {
            return None;
        }
        let handle = self.file_handles.get(&fh)?;
        let wb = handle.write_buf.as_ref()?;
        if !wb.dirty {
            return None;
        }
        Some(handle.ino)
    }

    /// Flush + release a dirty write handle asynchronously, off the FUSE
    /// worker thread. Registers a writeback cycle (so `fsync` / unlink /
    /// open barriers can wait for the in-flight close), then spawns the
    /// synchronous `vfs_release` (which runs `flush_write_buffer` and
    /// drops the inode write lock) and collapses the cycle to `Done`
    /// when the publish lands. Returns immediately; the FUSE_RELEASE
    /// reply does not wait on the publish. A failed flush taints the
    /// inode via `record_failure`, surfacing deferred EIO on the next
    /// fsync / open of the same path (POSIX close gives no durability
    /// guarantee). Only invoked for dirty handles in Default mode (see
    /// `peek_release_state`); single-writer-per-inode keeps at most one
    /// async close-flush in flight per inode.
    pub fn spawn_release_flush(self: Arc<Self>, fh: FileHandleId, ino: InodeId) {
        let generation = self.writeback.open_next_cycle(ino);
        compio_runtime::spawn(async move {
            // Ensure the cycle always collapses to Done even if this task
            // is dropped mid-flush (ring runtime torn down at unmount);
            // otherwise destroy's drain barrier hangs on the orphaned cycle.
            let mut cycle_guard = ReleaseCycleGuard {
                writeback: Arc::clone(&self.writeback),
                ino,
                generation,
                armed: true,
            };
            // Order behind lower-generation cycles (queued metadata
            // intents, an earlier in-flight flush) so this CAS publish
            // and the worker's puts land in generation order. A timeout
            // proceeds anyway: the flush's CAS and the worker's
            // SetPosix CAS both tolerate reordering; durability of the
            // buffered data comes first.
            if let Err(e) = self
                .wait_cycles_drained(ino, Generation(generation.0 - 1))
                .await
            {
                tracing::warn!(fh = fh.0, ino = ino.0, error = %e, "release flush: pre-drain timed out");
            }
            // Retry transient flush errors before the deferred-taint
            // path: for a brand-new file a failed FIRST publish leaves
            // nothing in NSS, so every retry here is one less silently
            // lost file. `flush_write_buffer` re-arms its snapshot on
            // failure, so a retry republishes the same data.
            for attempt in 1..3u32 {
                match self.flush_write_buffer(fh).await {
                    Ok(()) => break,
                    Err(e) => {
                        tracing::warn!(fh = fh.0, ino = ino.0, attempt, error = %e, "release flush retrying");
                        compio_runtime::time::sleep(Duration::from_millis(20 * attempt as u64))
                            .await;
                    }
                }
            }
            // vfs_release flushes any still-dirty buffer itself, so its
            // result is authoritative for the publish outcome. Disarm the
            // guard and advance the cycle explicitly on both normal paths.
            match self.vfs_release(fh).await {
                Ok(()) => {
                    cycle_guard.armed = false;
                    self.writeback.advance_to_done(ino, generation);
                }
                Err(e) => {
                    tracing::warn!(
                        fh = fh.0,
                        ino = ino.0,
                        generation = generation.0,
                        error = %e,
                        "async release flush failed; tainting inode"
                    );
                    self.writeback.record_failure(ino);
                    cycle_guard.armed = false;
                    self.writeback.advance_to_done(ino, generation);
                }
            }
            // Finish a FORGET that raced this flush (the open handle
            // pinned the entry so the publish above kept its posix).
            self.reap_forgotten_inode(ino);
        })
        .detach();
    }
}

/// Collapses a release-flush cycle to `Done` when the flush task is dropped
/// before it can advance the cycle itself (the ring runtime hosting the
/// detached task is torn down at unmount). An orphaned non-`Done` cycle
/// would otherwise wedge `destroy`'s drain barrier until it times out. The
/// paired `FlushSnapshotGuard` has by then restored the buffer dirty, so
/// `destroy`'s `flush_open_dirty_handles` still republishes the data.
/// Disarmed on the normal paths, which advance the cycle explicitly.
struct ReleaseCycleGuard {
    writeback: Arc<WritebackQueue>,
    ino: InodeId,
    generation: Generation,
    armed: bool,
}

impl Drop for ReleaseCycleGuard {
    fn drop(&mut self) {
        if self.armed {
            self.writeback.advance_to_done(self.ino, self.generation);
        }
    }
}
