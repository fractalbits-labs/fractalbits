//! `vfs_open` and the inode-scoped write lock it enforces.

use data_types::TraceId;
use data_types::object_layout::ObjectState;
use fractal_fuse::{FileHandleId, InodeId};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::config::WritebackMode;
use crate::error::FsError;
use crate::inode::EntryType;
use crate::prefetch::{cache_pressure_high, prefetch_blob, should_prefetch};
use crate::vfs::write_buffer::WriteBuffer;
use crate::vfs::{DEFAULT_BLOCK_SIZE, FileHandle, VfsCore};

impl VfsCore {
    /// Acquire the inode-scoped write lock for `fh`. Returns `Busy` if another
    /// write-mode handle currently owns it.
    ///
    /// Reclaim rule: if the recorded owner fh has been released (no entry in
    /// `file_handles`), the lock is stale and we take it. This recovers from
    /// any path that removes a handle without first calling
    /// `release_write_lock` (e.g. lookup races during shutdown).
    fn acquire_write_lock(&self, inode: InodeId, fh: FileHandleId) -> Result<(), FsError> {
        use dashmap::mapref::entry::Entry;
        match self.inode_write_owner.entry(inode) {
            Entry::Vacant(slot) => {
                slot.insert(fh);
                Ok(())
            }
            Entry::Occupied(mut slot) => {
                let owner = *slot.get();
                if !self.file_handles.contains_key(&owner) {
                    slot.insert(fh);
                    Ok(())
                } else {
                    Err(FsError::Busy)
                }
            }
        }
    }

    /// Acquire the inode write lock, briefly retrying to absorb the
    /// close-then-reopen-for-write race: a just-closed handle's FUSE_RELEASE
    /// (which drops this lock via `release_write_lock`) is asynchronous and
    /// may not have been processed by the time the kernel sends the next
    /// OPEN, so a single-process `write(); open(O_WRONLY)` would otherwise
    /// spuriously EBUSY (observed in truncate/O_TRUNC tests once per-flush
    /// latency grew). A genuinely concurrent writer keeps its handle open
    /// past the budget and still gets EBUSY.
    pub(crate) async fn acquire_write_lock_retry(
        &self,
        inode: InodeId,
        fh: FileHandleId,
    ) -> Result<(), FsError> {
        if self.acquire_write_lock(inode, fh).is_ok() {
            return Ok(());
        }
        // The lock may be held by an in-flight async close-flush:
        // FUSE_RELEASE spawns `vfs_release` off-thread and only drops the
        // write lock once the publish lands. Drain this inode's writeback
        // barrier so a re-open of a just-closed file (e.g. an O_TRUNC
        // reopen, or `echo x > f; cat f`) waits for the prior close to
        // commit (and reads its freshly published layout) instead of
        // spuriously failing EBUSY. No-op on an idle inode.
        self.drain_inode_to_barrier(inode).await?;
        if self.acquire_write_lock(inode, fh).is_ok() {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_millis(200);
        while Instant::now() < deadline {
            compio_runtime::time::sleep(Duration::from_millis(5)).await;
            // OPEN can beat the kernel's later RELEASE request for the
            // previous fd. Re-check the barrier in the retry loop so once
            // RELEASE registers its cycle, this path waits for the full
            // publish instead of timing out on the fixed dispatch window.
            self.drain_inode_to_barrier(inode).await?;
            if self.acquire_write_lock(inode, fh).is_ok() {
                return Ok(());
            }
        }
        Err(FsError::Busy)
    }

    pub(crate) fn release_write_lock(&self, inode: InodeId, fh: FileHandleId) {
        self.inode_write_owner
            .remove_if(&inode, |_, owner| *owner == fh);
    }

    pub async fn vfs_open(&self, inode: InodeId, flags: u32) -> Result<FileHandleId, FsError> {
        let write_flags = libc::O_WRONLY as u32
            | libc::O_RDWR as u32
            | libc::O_APPEND as u32
            | libc::O_TRUNC as u32;
        let is_write = flags & write_flags != 0;

        if is_write && !self.read_write {
            return Err(FsError::ReadOnly);
        }

        {
            let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
            if entry.entry_type != EntryType::File {
                return Err(FsError::IsDir);
            }
        }

        // In default writeback mode, every open is the recovery point for a
        // deferred publish error. Read opens additionally publish any dirty
        // local handle inline first: the kernel sends RELEASE lazily after
        // close(2) returns (and a dup'ed fd can delay it), so waiting on
        // cycles alone could serve a stale pre-flush layout when OPEN wins
        // that race. Write opens do not flush another live writer; they just
        // drain any already-registered release cycle and let the write lock
        // below return EBUSY if the old writer is still open.
        if self.writeback_mode == WritebackMode::Default {
            if !is_write && let Some(dirty_fh) = self.dirty_write_owner(inode) {
                match self.flush_write_buffer(dirty_fh).await {
                    // The handle raced its release; the release path
                    // owns the flush now and the drain below waits it.
                    Err(FsError::BadFd) => {}
                    res => res?,
                }
            }
            self.drain_inode_to_barrier(inode).await?;
        }

        let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
        let s3_key = entry.s3_key.clone();
        let layout = entry.layout.clone();
        let cached_inode_id = entry.inode_id;
        drop(entry);

        // Enforce single-writer per inode. The first writer
        // wins and subsequent write-mode opens fail with EBUSY. The lock
        // is process-local in-memory state and dies with the process on
        // crash, so the next open reacquires.
        let fh = self.alloc_fh();
        if is_write {
            self.acquire_write_lock_retry(inode, fh).await?;
        }

        // Resolve the layout (cold-fetch on a cache miss, then follow a
        // hardlink redirect to the shared record's real layout) and persist
        // any resolved hardlink identity back to the inode table. Wrapped so
        // a failure after the write lock was acquired still releases it;
        // otherwise the inode is left permanently EBUSY.
        //
        // Persisting the resolved `inode_id` is also what stops a cold-cache
        // Indirect entry (e.g. populated by readdirplus without a prior
        // vfs_lookup) from flushing a Normal layout over its redirect: the
        // flush keys its record-aware path on `entry.inode_id`. The redirect
        // itself has no blob_guid, so the resolved real layout is also what
        // lets the write buffer seed from the shared blob and reconcile at
        // the correct blob_version. Covers a cold cache (layout is
        // `Indirect`) and a warm one (cached `inode_id`, possibly a stale
        // pre-promotion layout copy).
        let resolved = async {
            let layout = match layout {
                Some(l) => Some(l),
                None => match self.backend().get_inode(&s3_key, &TraceId::new()).await {
                    Ok(l) => Some(l),
                    Err(FsError::NotFound) if is_write => None,
                    Err(FsError::NotFound) if !is_write => {
                        self.drain_inode_to_barrier(inode).await?;
                        match self.backend().get_inode(&s3_key, &TraceId::new()).await {
                            Ok(l) => Some(l),
                            Err(e) => return Err(e),
                        }
                    }
                    Err(e) => return Err(e),
                },
            };
            match layout {
                Some(l) => {
                    let (real, resolved_id) = if let Some(id) = cached_inode_id {
                        let real = self
                            .backend()
                            .get_inode_record(id, &TraceId::new())
                            .await?
                            .layout;
                        (real, Some(id))
                    } else if matches!(l.state, ObjectState::Indirect(_)) {
                        let (real, id, _nlink) = self.resolve_indirect(l, &TraceId::new()).await?;
                        (real, id)
                    } else {
                        (l, None)
                    };
                    if let Some(id) = resolved_id
                        && let Some(mut e) = self.inodes.get_mut(inode)
                    {
                        e.inode_id = Some(id);
                        e.layout = Some(real.clone());
                    }
                    Ok(Some(real))
                }
                None => Ok(None),
            }
        }
        .await;
        let layout = match resolved {
            Ok(l) => l,
            Err(e) => {
                if is_write {
                    self.release_write_lock(inode, fh);
                }
                return Err(e);
            }
        };

        // The FUSE data path only speaks the BSS block protocol; an S3
        // hybrid-volume object cannot be opened for data access.
        if let Some(ref l) = layout
            && let Err(error) = self.ensure_data_layout_supported(l, &TraceId::new()).await
        {
            if is_write {
                self.release_write_lock(inode, fh);
            }
            return Err(error);
        }

        // Cross-instance staleness reconciliation: if the cache file's
        // authoritative_blob_v lags the inode's blob_version, another
        // instance has bumped the version since we last sync'd. Clear
        // the cache file so subsequent reads cold-fetch from BSS.
        // Done on every open (read or write) so read-only handles
        // don't keep serving stale bytes.
        if let Some(dc) = &self.disk_cache
            && let Some(ref l) = layout
            && let Ok(blob_guid) = l.blob_guid()
            && let Err(e) = dc.reconcile_on_open(blob_guid, l.blob_version).await
        {
            tracing::warn!(
                %blob_guid, error = %e,
                "disk cache reconcile_on_open failed; continuing"
            );
        }

        let has_trunc = flags & libc::O_TRUNC as u32 != 0;
        let write_buf = if is_write {
            if let Some(ref l) = layout
                && !has_trunc
            {
                // Existing file, no O_TRUNC: seed a sparse buffer from the
                // committed geometry. No whole-file preload; partial-block
                // edits lazy-load only the blocks they touch.
                let blob_guid = l.blob_guid().ok();
                let committed_size = l.size().unwrap_or(0);
                Some(WriteBuffer::new(blob_guid, committed_size, l.block_size))
            } else if let Some(ref l) = layout {
                // O_TRUNC on an existing file: file_size 0, keep blob_guid so
                // the override flush trims the old blocks; size_changed/dirty
                // so flush sees the truncate. The committed layout size still
                // bounds the flush trim range.
                let blob_guid = l.blob_guid().ok();
                let mut wb = WriteBuffer::new(blob_guid, 0, l.block_size);
                wb.size_changed = true;
                wb.dirty = true;
                Some(wb)
            } else {
                // Brand-new file (NSS lookup returned NotFound).
                Some(WriteBuffer::new(None, 0, DEFAULT_BLOCK_SIZE))
            }
        } else {
            None
        };

        // Promote the cached entry to MRU on every open. Reads served
        // by `FUSE_PASSTHROUGH` bypass the per-block touch path
        // entirely, so without this hook a hot file served via
        // passthrough would never advance in LRU and the evictor would
        // treat it as cold.
        if !is_write
            && let Some(dc) = &self.disk_cache
            && let Some(ref l) = layout
            && let Ok(blob_guid) = l.blob_guid()
        {
            dc.touch_blob(blob_guid);
        }

        // Spawn a whole-blob prefetch when the open-time policy says
        // yes and the cache is not already complete. Read-only opens
        // only; writers own the blob's bytes via `WriteBuffer` and
        // have no need for a parallel prefetch.
        if !is_write
            && let Some(dc) = &self.disk_cache
            && let Some(ref l) = layout
            && let Ok(file_size) = l.size()
            && let Ok(blob_guid) = l.blob_guid()
        {
            let usage = dc.current_usage();
            let capacity = dc.capacity_bytes();
            // FOPEN_KEEP_CACHE is the kernel's sequential-read hint;
            // the open(2) flag itself does not directly map, so for
            // now we treat any non-O_RANDOM read as a candidate.
            // O_RANDOM is not a portable flag; absent it on Linux,
            // the conservative default is `false`; only the
            // full-threshold and workload_bulk_read branches fire.
            let keep_cache_hint = false;
            if !cache_pressure_high(usage, capacity, &self.prefetch_policy)
                && should_prefetch(file_size, keep_cache_hint, &self.prefetch_policy)
                && !dc.is_complete(blob_guid, file_size)
            {
                let dc_arc = Arc::clone(dc);
                let backend_cfg = Arc::clone(&self.backend_config);
                let layout_clone = l.clone();
                let rows = self.row_map_for_prefetch(l).await;
                compio_runtime::spawn(async move {
                    prefetch_blob(backend_cfg, dc_arc, layout_clone, rows).await;
                })
                .detach();
            }
        }

        self.file_handles.insert(
            fh,
            FileHandle {
                ino: inode,
                s3_key,
                layout,
                layout_refreshed_at: Instant::now(),
                operation_lock: Arc::new(futures::lock::Mutex::new(())),
                write_buf,
                backing_id: None,
            },
        );

        Ok(fh)
    }
}
