//! Unlinked-but-open files. The gateway owns reclamation; what the mount
//! keeps is the one fact only it knows, which handles are still open.
//! An unlink or rename-over of an open file asks the gateway to move the
//! value to a hidden `@orphan/` key instead of deleting it, so the open
//! handles keep a key to read and flush through; the last close deletes
//! that key with teardown.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use data_types::TraceId;
use fractal_fuse::InodeId;

use crate::vfs::VfsCore;

impl VfsCore {
    /// Point an unlinked inode and its open handles at the orphan key
    /// the gateway moved its value to. The name mapping is already gone.
    pub(crate) fn adopt_orphan(&self, ino: InodeId, orphan_key: &str) {
        self.inodes.update_s3_key(ino, orphan_key);
        if let Some(mut entry) = self.inodes.get_mut(ino) {
            entry.name_removed = true;
        }
        for mut handle in self.file_handles.iter_mut() {
            if handle.value().ino == ino {
                handle.value_mut().s3_key = orphan_key.to_string();
            }
        }
        self.orphans.insert(ino, orphan_key.to_string());
    }

    /// Delete an orphan key with teardown, off the caller's path.
    /// `sweep_inflight` lets `destroy` wait for the hand-off itself (not
    /// the reclamation) before the process exits.
    fn spawn_orphan_teardown(&self, orphan_key: String) {
        let backend = self.backend();
        let inflight = Arc::clone(&self.sweep_inflight);
        inflight.fetch_add(1, Ordering::AcqRel);
        compio_runtime::spawn(async move {
            if let Err(error) = backend
                .delete_inode(&orphan_key, true, false, &TraceId::new())
                .await
            {
                tracing::warn!(%orphan_key, %error, "orphan teardown failed; garbage may remain");
            }
            inflight.fetch_sub(1, Ordering::AcqRel);
        })
        .detach();
    }

    /// After a handle on `ino` closed: if the inode is an orphan and no
    /// handle remains, reclaim it.
    pub(crate) fn release_orphan_if_last(&self, ino: InodeId) {
        if !self.orphans.contains_key(&ino) || self.has_open_handles_for_inode(ino, None) {
            return;
        }
        if let Some((_, orphan_key)) = self.orphans.remove(&ino) {
            self.spawn_orphan_teardown(orphan_key);
        }
    }

    /// Release mount-local writer state and reclaim every orphan that was
    /// waiting for its final open handle. Runs after dirty handles and
    /// metadata have drained, when no request worker can create another
    /// handle. An orphan a crashed mount leaves behind stays under its
    /// hidden key until a scavenger reclaims it.
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

        let orphans = self
            .orphans
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for ino in orphans {
            if let Some((_, orphan_key)) = self.orphans.remove(&ino) {
                self.spawn_orphan_teardown(orphan_key);
            }
        }
    }

    /// Wait until every queued orphan hand-off has reached the gateway.
    /// The reclamation itself continues there after this mount is gone.
    pub async fn drain_sweep_work(&self) {
        while self.sweep_inflight.load(Ordering::Acquire) > 0 {
            compio_runtime::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Report hand-offs still pending when the bounded shutdown drain
    /// expires. The orphan keys survive under their hidden names.
    pub fn log_incomplete_sweep_work(&self) {
        let pending = self.sweep_inflight.load(Ordering::Acquire);
        if pending == 0 {
            return;
        }
        tracing::error!(
            pending_sweeps = pending,
            open_handles = self.file_handles.len(),
            orphans = self.orphans.len(),
            "destroy: orphan hand-off incomplete; hidden orphan keys may remain"
        );
    }
}
