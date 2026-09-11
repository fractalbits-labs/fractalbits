//! Write buffering and the flush path, truncate, fallocate, lseek.
//!
//! The mount buffers dirty blocks per handle and hands a flush to the
//! gateway in three steps: `BeginFlush` prepares the inode key against
//! the committed layout the buffer was built on, `WriteFlushBlock` ships
//! the dirty bodies concurrently, `CommitFlush` publishes size, rows and
//! the new ceiling atomically and returns the committed layout. Blob
//! identity, block generations and reclamation are the gateway's; this
//! side only ever names an inode key and logical block numbers.

use bytes::{Bytes, BytesMut};
use data_types::TraceId;
use data_types::object_layout::{InodeRecord, ORPHAN_PREFIX, ObjectLayout};
use data_types::ovr_map::zeros;
use fractal_fuse::{FileHandleId, InodeId};
use futures::{StreamExt, TryStreamExt, stream};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use crate::cache::DirEntryKind;
use crate::config::WritebackMode;
use crate::error::FsError;
use crate::vfs::write_buffer::BlockState;
use crate::vfs::write_buffer::WriteBuffer;
use crate::vfs::{DEFAULT_BLOCK_SIZE, MAX_INMEM_FILE_SIZE, VfsAttr, VfsCore, parent_prefix_of};

/// Concurrent body writes per flush. Bodies are independent write-once
/// keys on the gateway side, so pipelining them is safe.
const BODY_WRITE_CONCURRENCY: usize = 16;

impl VfsCore {
    /// Load one block's committed bytes for an RMW / dirty read / flush
    /// tail-zero. Returns zeros (length `fallback_content_len`) for a
    /// brand-new file or a block the gateway resolves to a hole;
    /// propagates other errors.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn lazy_load_block_for_flush(
        &self,
        key: &str,
        has_committed_data: bool,
        committed_layout: Option<&ObjectLayout>,
        block_num: u32,
        committed_content_len: usize,
        fallback_content_len: usize,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        if !has_committed_data || committed_content_len == 0 {
            return Ok(zeros(fallback_content_len));
        }
        let expected = committed_layout.map(|layout| layout.version_id);
        match self
            .backend()
            .read_block(key, block_num, expected, trace_id)
            .await?
        {
            Some(data) if data.len() > committed_content_len => {
                Ok(data.slice(0..committed_content_len))
            }
            Some(data) => Ok(data),
            None => Ok(zeros(fallback_content_len)),
        }
    }

    /// Serve a read against a dirty write handle by merging per-block
    /// intents (`Rewrite` bytes, `Delete`/shrunk-range zeros,
    /// else lazy-loaded committed bytes) over the buffered `file_size`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_dirty_handle(
        &self,
        key: &str,
        file_size: u64,
        block_size: u32,
        has_committed_data: bool,
        committed_layout: Option<&ObjectLayout>,
        blocks: &BTreeMap<u32, BlockState>,
        eof_low_watermark: Option<u32>,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, FsError> {
        if buf.is_empty() || offset >= file_size {
            return Ok(0);
        }
        let bsz = block_size as u64;
        let read_end = std::cmp::min(offset + buf.len() as u64, file_size);
        let actual_len = (read_end - offset) as usize;
        let first_block = (offset / bsz) as u32;
        let last_block = ((read_end - 1) / bsz) as u32;
        let trace_id = TraceId::new();

        let mut written = 0usize;
        for b in first_block..=last_block {
            let block_start = b as u64 * bsz;
            let block_content_len = std::cmp::min(bsz, file_size - block_start) as usize;
            let slice_start = if b == first_block {
                (offset - block_start) as usize
            } else {
                0
            };
            let slice_end = if b == last_block {
                (read_end - block_start) as usize
            } else {
                block_content_len
            };
            let chunk_len = slice_end.saturating_sub(slice_start);

            let block_bytes: Bytes = match blocks.get(&b) {
                Some(BlockState::Rewrite(b2)) => b2.clone(),
                Some(BlockState::Delete) => zeros(block_content_len),
                None => {
                    if eof_low_watermark.is_some_and(|low| b >= low) {
                        zeros(block_content_len)
                    } else {
                        self.lazy_load_block_for_flush(
                            key,
                            has_committed_data,
                            committed_layout,
                            b,
                            block_content_len,
                            block_content_len,
                            &trace_id,
                        )
                        .await?
                    }
                }
            };
            let take = chunk_len.min(block_bytes.len().saturating_sub(slice_start));
            if take > 0 {
                buf[written..written + take]
                    .copy_from_slice(&block_bytes[slice_start..slice_start + take]);
                written += take;
            }
            if take < chunk_len {
                let pad = chunk_len - take;
                for byte in &mut buf[written..written + pad] {
                    *byte = 0;
                }
                written += pad;
            }
        }
        Ok(written.min(actual_len))
    }

    /// Re-arm a flush's snapshotted buffer after a post-snapshot failure,
    /// so a later fsync retries instead of seeing a falsely-clean buffer:
    /// the flush takes `blocks` and clears `dirty` up front, so any error
    /// after that point must put them back or the write is silently lost.
    /// Re-inserts without clobbering newer writes.
    pub(crate) fn restore_flush_snapshot(
        &self,
        fh_id: FileHandleId,
        blocks: BTreeMap<u32, BlockState>,
    ) {
        if let Some(mut handle) = self.file_handles.get_mut(&fh_id)
            && let Some(ref mut wb) = handle.write_buf
        {
            for (b, st) in blocks {
                wb.blocks.entry(b).or_insert(st);
            }
            wb.dirty = true;
        }
    }

    /// Install a layout the gateway published for this inode (a prepare
    /// or a commit) into the flushing handle and the inode entry.
    fn adopt_published_layout(&self, fh_id: FileHandleId, ino: InodeId, layout: &ObjectLayout) {
        if let Some(mut handle) = self.file_handles.get_mut(&fh_id) {
            handle.layout = Some(layout.clone());
            handle.layout_refreshed_at = Instant::now();
        }
        if let Some(mut entry) = self.inodes.get_mut(ino) {
            entry.layout = Some(layout.clone());
        }
    }

    pub(crate) async fn flush_write_buffer(&self, fh_id: FileHandleId) -> Result<(), FsError> {
        let operation_lock = self
            .file_handles
            .get(&fh_id)
            .ok_or(FsError::BadFd)?
            .operation_lock
            .clone();
        let _operation_guard = operation_lock.lock().await;

        // Snapshot the sparse buffer under the guard and clear `dirty` so a
        // concurrent flush of the same fh sees a clean buffer and
        // early-returns rather than racing in to republish.
        let (s3_key, ino, file_size, blocks, eof_low_watermark, trim_upper) = {
            let mut handle = self.file_handles.get_mut(&fh_id).ok_or(FsError::BadFd)?;
            let s3_key = handle.s3_key.clone();
            let ino = handle.ino;
            let wb = match &mut handle.write_buf {
                Some(wb) if wb.dirty => wb,
                _ => return Ok(()),
            };
            let file_size = wb.file_size;
            let blocks = std::mem::take(&mut wb.blocks);
            let eof_low_watermark = wb.eof_low_watermark;
            let trim_upper = wb.trim_upper;
            wb.dirty = false;
            (
                s3_key,
                ino,
                file_size,
                blocks,
                eof_low_watermark,
                trim_upper,
            )
        };

        // A name unlinked while its fd stayed open flushes to the hidden
        // orphan key the gateway moved it to, or to the shared hardlink
        // record. A removed name with neither (its create publish never
        // landed) must not be resurrected.
        let (name_removed, promoted_inode_id) = self
            .inodes
            .get(ino)
            .map(|e| (e.name_removed, e.inode_id))
            .unwrap_or((false, None));
        if name_removed && promoted_inode_id.is_none() && !s3_key.starts_with(ORPHAN_PREFIX) {
            if let Some(mut handle) = self.file_handles.get_mut(&fh_id)
                && let Some(ref mut wb) = handle.write_buf
            {
                wb.dirty = false;
                wb.size_changed = false;
            }
            return Ok(());
        }

        // Own the taken snapshot in a guard that re-installs it into the
        // handle if this flush errors out or is cancelled mid-publish, so a
        // dropped release-flush future doesn't leave the buffer looking
        // clean (and silently lost). Disarmed on success below.
        let mut snap = FlushSnapshotGuard {
            vfs: self,
            fh_id,
            blocks,
            armed: true,
        };

        let trace_id = TraceId::new();

        // Promoted (hardlink) inodes flush into the shared InodeRecord at
        // `@hardlink/<id>`, not at this name's s3_key. Fetch the record up
        // front: its layout is the committed base of this flush.
        let publish_key = match promoted_inode_id {
            Some(id) => InodeRecord::key_for(id),
            None => s3_key.clone(),
        };
        let base_layout: Option<ObjectLayout> = match promoted_inode_id {
            Some(id) => Some(self.backend().get_inode_record(id, &trace_id).await?.layout),
            None => self.file_handles.get(&fh_id).and_then(|h| h.layout.clone()),
        };

        let rewrites: Vec<u32> = snap
            .blocks
            .iter()
            .filter_map(|(block, state)| matches!(state, BlockState::Rewrite(_)).then_some(*block))
            .collect();
        let punched: Vec<u32> = snap
            .blocks
            .iter()
            .filter_map(|(block, state)| matches!(state, BlockState::Delete).then_some(*block))
            .collect();

        // Step 1: prepare. The gateway checks the committed data the buffer
        // was built against is still what the key publishes, burns the
        // generation, and hands back the prepared layout so a retry after
        // a mid-flush failure buffers against it.
        let prepared = self
            .backend()
            .begin_flush(
                &publish_key,
                base_layout.as_ref(),
                file_size,
                &rewrites,
                &punched,
                eof_low_watermark,
                trim_upper,
                &trace_id,
            )
            .await?;
        if let Some(prepare) = &prepared.layout {
            self.adopt_published_layout(fh_id, ino, prepare);
        }
        let ticket = prepared.ticket;

        // Step 2: bodies, pipelined. The gateway assigns each block its
        // generation from the prepared classification.
        let body_writes = stream::iter(snap.blocks.iter())
            .filter_map(|(b, st)| async move {
                match st {
                    BlockState::Rewrite(bytes) => Some((*b, bytes.clone())),
                    _ => None,
                }
            })
            .map(|(b, bytes)| {
                let trace_id = &trace_id;
                let ticket = &ticket;
                async move {
                    self.backend()
                        .write_flush_block(ticket, b, bytes, trace_id)
                        .await
                }
            })
            .buffer_unordered(BODY_WRITE_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await;
        if let Err(e) = body_writes {
            // Nothing was published: a create's fresh blob can be reclaimed.
            self.backend().abort_flush(&ticket).await;
            return Err(e);
        }

        // Step 3: commit. A create takes its attributes from the inode; an
        // overwrite carries the stored ones forward (the gateway rebases
        // over a concurrent chmod). Never abort after a failed commit: the
        // CAS may have landed with a lost reply, and a whole-blob deletion
        // would then destroy acknowledged data.
        let posix = self.inodes.get(ino).map(|e| e.posix).unwrap_or_default();
        let mut final_layout = self
            .backend()
            .commit_flush(&ticket, &rewrites, &punched, posix, &trace_id)
            .await?;
        snap.armed = false;

        // Update file handle: install the new layout (next flush's base),
        // clear dirty/size_changed, reset shrink state.
        if let Some(mut handle) = self.file_handles.get_mut(&fh_id) {
            handle.layout = Some(final_layout.clone());
            handle.layout_refreshed_at = Instant::now();
            if let Some(ref mut wb) = handle.write_buf {
                wb.dirty = false;
                wb.size_changed = false;
                wb.eof_low_watermark = None;
                wb.trim_upper = None;
                wb.has_committed_data = true;
            }
        }
        // Other clean handles on the same inode adopt the new committed
        // layout immediately (their TTL refresh would otherwise lag one
        // commit behind this writer).
        for mut other in self.file_handles.iter_mut() {
            if *other.key() == fh_id || other.value().ino != ino {
                continue;
            }
            if other.value().write_buf.as_ref().is_some_and(|wb| wb.dirty) {
                continue;
            }
            other.value_mut().layout = Some(final_layout.clone());
            other.value_mut().layout_refreshed_at = Instant::now();
        }

        // Mirror the just-published layout onto the inode entry so a
        // subsequent getattr / setattr can serve the correct size + type
        // from memory without a cross-instance coherency round-trip. A
        // promoted hardlink also persists the record identity and posix.
        if let Some(mut e) = self.inodes.get_mut(ino) {
            e.layout = Some(final_layout.clone());
            if let Some(id) = promoted_inode_id {
                e.inode_id = Some(id);
                e.posix = crate::inode::layout_posix(&final_layout);
            }
        }

        let parent_prefix = parent_prefix_of(&s3_key);
        let name = s3_key
            .trim_end_matches('/')
            .rsplit_once('/')
            .map(|(_, n)| n.to_string())
            .unwrap_or_else(|| s3_key.clone());
        self.cache_dir_entry(&parent_prefix, &name, ino, DirEntryKind::RegularFile);

        if promoted_inode_id.is_none() {
            match self
                .publish_posix_catchup_after_flush(ino, &s3_key, &final_layout, &trace_id)
                .await
            {
                Ok(Some(posix_layout)) => {
                    final_layout = posix_layout;
                    self.adopt_published_layout(fh_id, ino, &final_layout);
                }
                Ok(None) => {}
                Err(e) => {
                    // The data publish already landed and the buffer is
                    // clean, so a retry of this flush no-ops with Ok and
                    // the posix update would be silently lost (the async
                    // release retry loop would report success). Taint so
                    // the failure surfaces as deferred EIO.
                    if self.writeback_mode == WritebackMode::Default {
                        self.writeback.record_failure(ino);
                    }
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    pub async fn vfs_write(
        &self,
        fh: FileHandleId,
        offset: u64,
        data: &[u8],
    ) -> Result<u32, FsError> {
        // POSIX: zero-byte writes are a no-op and must NOT extend the
        // file. Early return also avoids the `end - 1` underflow below.
        if data.is_empty() {
            return Ok(0);
        }
        let operation_lock = self
            .file_handles
            .get(&fh)
            .ok_or(FsError::BadFd)?
            .operation_lock
            .clone();
        let _operation_guard = operation_lock.lock().await;
        self.refresh_handle_layout(fh, false).await?;
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or(FsError::InvalidArg)?;

        // Phase 1: snapshot block_size, committed geometry, and which
        // partially-touched blocks need a lazy read-modify-write load.
        // Releases the guard before any await.
        let (block_size, has_committed_data, committed_size, committed_layout, blocks_to_load) = {
            let mut handle = self.file_handles.get_mut(&fh).ok_or(FsError::BadFd)?;
            let bsize = handle
                .layout
                .as_ref()
                .map(|l| l.block_size)
                .unwrap_or(DEFAULT_BLOCK_SIZE);
            let committed_size = handle
                .layout
                .as_ref()
                .and_then(|l| l.size().ok())
                .unwrap_or(0);
            let has_layout = handle.layout.is_some();
            let committed_layout = handle.layout.clone();
            let wb = handle
                .write_buf
                .get_or_insert_with(|| WriteBuffer::new(has_layout, committed_size, bsize));
            let bsz_u64 = wb.block_size as u64;
            if end.div_ceil(bsz_u64) > u32::MAX as u64 {
                return Err(FsError::InvalidArg);
            }
            let first_block = (offset / bsz_u64) as u32;
            let last_block = ((end - 1) / bsz_u64) as u32;
            // Blocks needing lazy load: partially-touched, not already
            // buffered, not fully overwritten, and not destroyed by an
            // earlier shrink (those read as zeros per POSIX).
            let mut to_load = Vec::new();
            for b in first_block..=last_block {
                if wb.blocks.contains_key(&b) {
                    continue;
                }
                let block_start = b as u64 * bsz_u64;
                let block_end = block_start + bsz_u64;
                let fully_covered = offset <= block_start && end >= block_end;
                if fully_covered {
                    continue;
                }
                if wb.block_destroyed_by_shrink(b) {
                    continue;
                }
                to_load.push(b);
            }
            (
                wb.block_size,
                wb.has_committed_data,
                committed_size,
                committed_layout,
                to_load,
            )
        };

        // Phase 2: lazy-load the partial blocks outside the guard. The
        // common aligned streaming write loads nothing.
        let trace_id = TraceId::new();
        let mut loaded: BTreeMap<u32, Bytes> = BTreeMap::new();
        let key = self.handle_key(fh)?;
        let bsz_u64 = block_size as u64;
        for b in blocks_to_load {
            let block_start = b as u64 * bsz_u64;
            let committed_content_len = if block_start < committed_size {
                std::cmp::min(bsz_u64, committed_size - block_start) as usize
            } else {
                0
            };
            let bytes = self
                .lazy_load_block_for_flush(
                    &key,
                    has_committed_data,
                    committed_layout.as_ref(),
                    b,
                    committed_content_len,
                    block_size as usize,
                    &trace_id,
                )
                .await?;
            loaded.insert(b, bytes);
        }

        // Phase 3: re-acquire the guard, splice user bytes per block.
        let mut handle = self.file_handles.get_mut(&fh).ok_or(FsError::BadFd)?;
        let wb = handle
            .write_buf
            .as_mut()
            .ok_or(FsError::Internal("write_buf gone".into()))?;
        let bsz_u64 = wb.block_size as u64;
        let first_block = (offset / bsz_u64) as u32;
        let last_block = ((end - 1) / bsz_u64) as u32;
        for b in first_block..=last_block {
            let block_start = b as u64 * bsz_u64;
            let block_end = block_start + bsz_u64;
            let copy_src_start = block_start.saturating_sub(offset).min(data.len() as u64) as usize;
            let copy_src_end = block_end.saturating_sub(offset).min(data.len() as u64) as usize;
            let copy_dst_start = offset.saturating_sub(block_start).min(bsz_u64) as usize;
            let copy_dst_end = (end.saturating_sub(block_start).min(bsz_u64)) as usize;
            let mut block_bytes: BytesMut = match wb.blocks.get(&b) {
                Some(BlockState::Rewrite(b2)) => {
                    let mut bm = BytesMut::with_capacity(wb.block_size as usize);
                    bm.extend_from_slice(b2);
                    if bm.len() < wb.block_size as usize {
                        bm.resize(wb.block_size as usize, 0);
                    }
                    bm
                }
                Some(BlockState::Delete) => BytesMut::zeroed(wb.block_size as usize),
                None => {
                    if let Some(loaded_bytes) = loaded.get(&b) {
                        let mut bm = BytesMut::with_capacity(wb.block_size as usize);
                        bm.extend_from_slice(loaded_bytes);
                        if bm.len() < wb.block_size as usize {
                            bm.resize(wb.block_size as usize, 0);
                        }
                        bm
                    } else {
                        BytesMut::zeroed(wb.block_size as usize)
                    }
                }
            };
            block_bytes[copy_dst_start..copy_dst_end]
                .copy_from_slice(&data[copy_src_start..copy_src_end]);
            wb.blocks
                .insert(b, BlockState::Rewrite(block_bytes.freeze()));
        }
        if end > wb.file_size {
            wb.file_size = end;
            wb.size_changed = true;
        }
        wb.dirty = true;

        Ok(data.len() as u32)
    }

    pub async fn vfs_fallocate(
        &self,
        fh: FileHandleId,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> Result<(), FsError> {
        self.check_write_enabled()?;
        if length == 0 {
            return Ok(());
        }
        let keep_size = mode & libc::FALLOC_FL_KEEP_SIZE as u32 != 0;
        let punch_hole = mode & libc::FALLOC_FL_PUNCH_HOLE as u32 != 0;
        // Linux requires PUNCH_HOLE be combined with KEEP_SIZE.
        if punch_hole && !keep_size {
            return Err(FsError::InvalidArg);
        }
        // Reject mode bits we don't model. Allowing them silently
        // would let userspace assume semantics we never delivered.
        let known = libc::FALLOC_FL_KEEP_SIZE | libc::FALLOC_FL_PUNCH_HOLE;
        if mode & !(known as u32) != 0 {
            return Err(FsError::InvalidArg);
        }

        let operation_lock = self
            .file_handles
            .get(&fh)
            .ok_or(FsError::BadFd)?
            .operation_lock
            .clone();
        let operation_guard = operation_lock.lock().await;
        self.refresh_handle_layout(fh, false).await?;

        let end = offset.checked_add(length).ok_or(FsError::InvalidArg)?;

        // Phase 1: snapshot enough state to compute the touched range
        // and decide which blocks need a lazy load for edge zeroing.
        let (block_size, has_committed_data, committed_size, committed_layout, edge_loads) = {
            let mut handle = self.file_handles.get_mut(&fh).ok_or(FsError::BadFd)?;
            let block_size = handle
                .layout
                .as_ref()
                .map(|l| l.block_size)
                .unwrap_or(DEFAULT_BLOCK_SIZE);
            let committed_size = handle
                .layout
                .as_ref()
                .and_then(|l| l.size().ok())
                .unwrap_or(0);
            let has_layout = handle.layout.is_some();
            let committed_layout = handle.layout.clone();
            let wb = handle
                .write_buf
                .get_or_insert_with(|| WriteBuffer::new(has_layout, committed_size, block_size));
            let bsz_u64 = wb.block_size as u64;
            if end.div_ceil(bsz_u64) > u32::MAX as u64 {
                return Err(FsError::InvalidArg);
            }
            let mut edge_loads: Vec<u32> = Vec::new();

            if punch_hole {
                let hole_end = end;
                let lo_partial = !offset.is_multiple_of(bsz_u64);
                let hi_partial = !hole_end.is_multiple_of(bsz_u64);
                let first_full = offset.div_ceil(bsz_u64) as u32;

                let lo_block = (offset / bsz_u64) as u32;
                let hi_block = (hole_end / bsz_u64) as u32;

                // Determine which edge blocks need a lazy load. We only
                // load when the block has committed bytes, there isn't
                // already a buffered `Rewrite` copy we can edit in place,
                // and the shrink-destroys watermark hasn't already turned
                // this block into zeros.
                let mut consider_edge = |b: u32| {
                    if matches!(wb.blocks.get(&b), Some(BlockState::Rewrite(_))) {
                        return;
                    }
                    if wb.block_destroyed_by_shrink(b) {
                        return;
                    }
                    let block_start = b as u64 * bsz_u64;
                    if block_start >= committed_size {
                        return;
                    }
                    edge_loads.push(b);
                };

                if lo_partial {
                    consider_edge(lo_block);
                }
                // Only schedule the trailing edge load when it isn't the
                // same block as the leading edge AND isn't a fully-covered
                // interior block (which we Delete instead of zeroing).
                if hi_partial && hi_block != lo_block && hi_block >= first_full {
                    consider_edge(hi_block);
                }
            }
            (
                block_size,
                wb.has_committed_data,
                committed_size,
                committed_layout,
                edge_loads,
            )
        };

        // Phase 2: lazy-load edge blocks outside the DashMap guard.
        let trace_id = TraceId::new();
        let mut loaded: BTreeMap<u32, Bytes> = BTreeMap::new();
        if punch_hole && !edge_loads.is_empty() {
            let key = self.handle_key(fh)?;
            let bsz_u64 = block_size as u64;
            for b in edge_loads {
                let block_start = b as u64 * bsz_u64;
                let committed_content_len = if block_start < committed_size {
                    std::cmp::min(bsz_u64, committed_size - block_start) as usize
                } else {
                    0
                };
                let bytes = self
                    .lazy_load_block_for_flush(
                        &key,
                        has_committed_data,
                        committed_layout.as_ref(),
                        b,
                        committed_content_len,
                        block_size as usize,
                        &trace_id,
                    )
                    .await?;
                loaded.insert(b, bytes);
            }
        }

        // Phase 3: re-acquire the guard and apply the buffered edits.
        let mut handle = self.file_handles.get_mut(&fh).ok_or(FsError::BadFd)?;
        let wb = handle
            .write_buf
            .as_mut()
            .ok_or(FsError::Internal("write_buf gone".into()))?;
        let bsz_u64 = wb.block_size as u64;
        let bsz_usize = wb.block_size as usize;

        if punch_hole {
            let hole_end = end;
            let first_full = offset.div_ceil(bsz_u64) as u32;
            let last_full_excl = (hole_end / bsz_u64) as u32;
            let lo_block = (offset / bsz_u64) as u32;
            let hi_block = (hole_end / bsz_u64) as u32;

            let edge_zero = |wb: &mut WriteBuffer,
                             loaded: &BTreeMap<u32, Bytes>,
                             b: u32,
                             lo: usize,
                             hi: usize| {
                let mut buf = BytesMut::with_capacity(bsz_usize);
                let existing: Option<Bytes> = match wb.blocks.get(&b) {
                    Some(BlockState::Rewrite(b2)) => Some(b2.clone()),
                    _ => loaded.get(&b).cloned(),
                };
                if let Some(existing) = existing {
                    buf.extend_from_slice(&existing);
                }
                if buf.len() < bsz_usize {
                    buf.resize(bsz_usize, 0);
                }
                for byte in &mut buf[lo..hi] {
                    *byte = 0;
                }
                wb.blocks.insert(b, BlockState::Rewrite(buf.freeze()));
            };

            // Special case: hole confined to a single partial block.
            if lo_block == hi_block
                && !offset.is_multiple_of(bsz_u64)
                && !hole_end.is_multiple_of(bsz_u64)
            {
                edge_zero(
                    wb,
                    &loaded,
                    lo_block,
                    (offset % bsz_u64) as usize,
                    (hole_end % bsz_u64) as usize,
                );
            } else {
                if !offset.is_multiple_of(bsz_u64) {
                    let lo = (offset % bsz_u64) as usize;
                    edge_zero(wb, &loaded, lo_block, lo, bsz_usize);
                }
                if !hole_end.is_multiple_of(bsz_u64) && hi_block >= first_full {
                    let hi = (hole_end % bsz_u64) as usize;
                    edge_zero(wb, &loaded, hi_block, 0, hi);
                }
            }

            if first_full < last_full_excl {
                for b in first_full..last_full_excl {
                    wb.blocks.insert(b, BlockState::Delete);
                }
            }
            wb.dirty = true;
            drop(handle);
            drop(operation_guard);
            return self.flush_write_buffer(fh).await;
        }

        // mode == 0 or KEEP_SIZE: we reserve no space, so the only effect is
        // on the size. An unwritten block stays a hole and reads as zeros.
        // See 10-fs-v2-fallocate-append.md section 6.2 for the deliberate
        // posix_fallocate deviation this accepts.
        if !keep_size && end > wb.file_size {
            wb.file_size = end;
            wb.size_changed = true;
        }
        wb.dirty = true;
        drop(handle);
        drop(operation_guard);
        self.flush_write_buffer(fh).await
    }

    /// lseek(SEEK_DATA / SEEK_HOLE). Classifies each block in
    /// `[offset, file_size)` as data or hole and returns the offset of
    /// the first match. Buffered intents answer first; committed blocks
    /// are classified by the gateway from the row map and the stored
    /// base-version entries.
    pub async fn vfs_lseek(
        &self,
        fh: FileHandleId,
        offset: u64,
        whence: u32,
    ) -> Result<u64, FsError> {
        let seek_data = whence == libc::SEEK_DATA as u32;
        let seek_hole = whence == libc::SEEK_HOLE as u32;
        if !seek_data && !seek_hole {
            return Err(FsError::InvalidArg);
        }

        let operation_lock = self
            .file_handles
            .get(&fh)
            .ok_or(FsError::BadFd)?
            .operation_lock
            .clone();
        let _operation_guard = operation_lock.lock().await;
        self.refresh_handle_layout(fh, false).await?;

        // Snapshot the bits we need without holding the guard across awaits.
        let (
            committed_layout,
            file_size,
            block_size,
            has_committed_data,
            blocks,
            eof_low_watermark,
        ) = {
            let handle = self.file_handles.get(&fh).ok_or(FsError::BadFd)?;
            let committed_layout = handle.layout.clone();
            let layout_block_size = committed_layout
                .as_ref()
                .map(|l| l.block_size)
                .unwrap_or(DEFAULT_BLOCK_SIZE);
            let layout_size = committed_layout
                .as_ref()
                .and_then(|l| l.size().ok())
                .unwrap_or(0);
            if let Some(ref wb) = handle.write_buf {
                (
                    committed_layout,
                    wb.file_size,
                    wb.block_size,
                    wb.has_committed_data,
                    wb.blocks.clone(),
                    wb.eof_low_watermark,
                )
            } else {
                (
                    committed_layout,
                    layout_size,
                    layout_block_size,
                    true,
                    BTreeMap::new(),
                    None,
                )
            }
        };

        // Match Linux semantics: offset >= file_size returns ENXIO for both
        // SEEK_HOLE and SEEK_DATA.
        if offset >= file_size {
            return Err(FsError::NoData);
        }

        let bsz_u64 = block_size as u64;
        let first_block = (offset / bsz_u64) as u32;
        let last_block_excl = file_size.div_ceil(bsz_u64) as u32;

        // Per-block classifier. `Some(true)` -> data, `Some(false)` -> hole,
        // `None` -> not buffered, fall through to the committed probe.
        let buffered_kind = |b: u32| -> Option<bool> {
            match blocks.get(&b) {
                Some(BlockState::Rewrite(_)) => Some(true),
                Some(BlockState::Delete) => Some(false),
                None => {
                    if eof_low_watermark.is_some_and(|low| b >= low) {
                        return Some(false);
                    }
                    None
                }
            }
        };

        let committed_data: BTreeSet<u32> = match committed_layout.as_ref() {
            Some(layout) if has_committed_data => {
                let count = last_block_excl.saturating_sub(first_block);
                if count == 0 {
                    BTreeSet::new()
                } else {
                    let key = self.handle_key(fh)?;
                    self.backend()
                        .probe_data_blocks(
                            &key,
                            first_block,
                            count,
                            Some(layout.version_id),
                            &TraceId::new(),
                        )
                        .await?
                        .into_iter()
                        .collect()
                }
            }
            _ => BTreeSet::new(),
        };

        for b in first_block..last_block_excl {
            let is_data = match buffered_kind(b) {
                Some(d) => d,
                None => committed_data.contains(&b),
            };
            let result_offset = if b == first_block {
                offset
            } else {
                b as u64 * bsz_u64
            };
            if seek_data && is_data {
                return Ok(result_offset);
            }
            if seek_hole && !is_data {
                return Ok(result_offset);
            }
        }

        if seek_hole {
            // No further data in the file; SEEK_HOLE returns the EOF.
            Ok(file_size)
        } else {
            // SEEK_DATA hit no data: ENXIO.
            Err(FsError::NoData)
        }
    }

    /// Handle size changes via setattr (truncate, extend, or truncate-to-zero).
    pub async fn vfs_setattr_size(
        &self,
        inode: InodeId,
        fh: FileHandleId,
        new_size: u64,
    ) -> Result<VfsAttr, FsError> {
        // A negative ftruncate length wraps to a near-u64::MAX value;
        // pjdfstest expects EINVAL for those. Reject before touching the
        // buffer. (The buffer is now sparse, so this is a sanity bound,
        // not an allocation guard.)
        if new_size > MAX_INMEM_FILE_SIZE {
            return Err(FsError::InvalidArg);
        }
        let operation_lock = self
            .file_handles
            .get(&fh)
            .ok_or(FsError::BadFd)?
            .operation_lock
            .clone();
        let _operation_guard = operation_lock.lock().await;
        self.refresh_handle_layout(fh, false).await?;

        // Phase 1: snapshot, drop intents past the new EOF, lower the
        // shrink-destroys watermark, and decide whether the surviving last
        // block of a non-block-aligned shrink needs a synthesized
        // tail-zero `Rewrite`. Releases the guard before any await.
        let (block_size, committed_size, has_committed_data, committed_layout, tail_zero_target) = {
            let mut handle = self.file_handles.get_mut(&fh).ok_or(FsError::BadFd)?;
            let block_size = handle
                .layout
                .as_ref()
                .map(|l| l.block_size)
                .unwrap_or(DEFAULT_BLOCK_SIZE);
            let committed_size = handle
                .layout
                .as_ref()
                .and_then(|l| l.size().ok())
                .unwrap_or(0);
            let has_layout = handle.layout.is_some();
            let committed_layout = handle.layout.clone();
            let wb = handle
                .write_buf
                .get_or_insert_with(|| WriteBuffer::new(has_layout, committed_size, block_size));
            let bsz_u64 = block_size as u64;
            let mut tail_zero_target: Option<(u32, usize, Option<Bytes>)> = None;
            if new_size < wb.file_size {
                let new_last_block_excl = new_size.div_ceil(bsz_u64) as u32;
                wb.drop_blocks_past(new_last_block_excl);
                wb.eof_low_watermark = Some(
                    wb.eof_low_watermark
                        .map(|low| low.min(new_last_block_excl))
                        .unwrap_or(new_last_block_excl),
                );
                if wb.trim_upper.is_none() {
                    let committed_block_count = committed_size.div_ceil(bsz_u64) as u32;
                    if committed_block_count > new_last_block_excl {
                        wb.trim_upper = Some(committed_block_count);
                    }
                }
                if new_size > 0 && !new_size.is_multiple_of(bsz_u64) {
                    let last = (new_size / bsz_u64) as u32;
                    let kept = (new_size % bsz_u64) as usize;
                    let block_was_committed = (last as u64) * bsz_u64 < committed_size;
                    let buffered_prefix: Option<Bytes> = match wb.blocks.get(&last) {
                        Some(BlockState::Rewrite(b)) => Some(b.clone()),
                        _ => None,
                    };
                    if block_was_committed || buffered_prefix.is_some() {
                        tail_zero_target = Some((last, kept, buffered_prefix));
                    }
                }
            }
            if new_size != wb.file_size {
                wb.file_size = new_size;
                wb.size_changed = true;
                wb.dirty = true;
            }
            (
                block_size,
                committed_size,
                wb.has_committed_data,
                committed_layout,
                tail_zero_target,
            )
        };

        // Phase 2: lazy-load the surviving last block (if not buffered)
        // outside the guard and insert the synthesized tail-zero Rewrite.
        if let Some((last, kept, buffered_prefix)) = tail_zero_target {
            let bsz_usize = block_size as usize;
            let prefix_bytes = match buffered_prefix {
                Some(b) => b,
                None => {
                    let trace_id = TraceId::new();
                    let key = self.handle_key(fh)?;
                    let block_start = (last as u64) * (block_size as u64);
                    let committed_content_len = if block_start < committed_size {
                        std::cmp::min(block_size as u64, committed_size - block_start) as usize
                    } else {
                        0
                    };
                    self.lazy_load_block_for_flush(
                        &key,
                        has_committed_data,
                        committed_layout.as_ref(),
                        last,
                        committed_content_len,
                        bsz_usize,
                        &trace_id,
                    )
                    .await?
                }
            };
            let mut buf = BytesMut::with_capacity(bsz_usize);
            let prefix_len = std::cmp::min(kept, prefix_bytes.len());
            buf.extend_from_slice(&prefix_bytes[..prefix_len]);
            buf.resize(bsz_usize, 0);
            if let Some(mut handle) = self.file_handles.get_mut(&fh)
                && let Some(ref mut wb) = handle.write_buf
            {
                wb.blocks.insert(last, BlockState::Rewrite(buf.freeze()));
                wb.dirty = true;
            }
        }

        let new_attr_size = self
            .file_handles
            .get(&fh)
            .ok_or(FsError::BadFd)?
            .write_buf
            .as_ref()
            .map(|wb| wb.file_size)
            .unwrap_or(new_size);
        Ok(self.make_new_file_attr(inode, new_attr_size))
    }
}

/// Restores a flush's taken block snapshot back into the file handle if the
/// flush does not complete: on an error return OR on future cancellation
/// (e.g. a release-flush task dropped when its ring runtime is torn down at
/// unmount). `flush_write_buffer` moves the blocks out and clears `dirty`
/// up front; without this guard a cancelled flush would leave the handle
/// looking clean, so `destroy`'s `flush_open_dirty_handles` would skip it
/// and the buffered data would be silently lost. Disarmed once the publish
/// succeeds, after which the snapshot is discarded normally.
struct FlushSnapshotGuard<'a> {
    vfs: &'a VfsCore,
    fh_id: FileHandleId,
    blocks: BTreeMap<u32, BlockState>,
    armed: bool,
}

impl Drop for FlushSnapshotGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.vfs
                .restore_flush_snapshot(self.fh_id, std::mem::take(&mut self.blocks));
        }
    }
}
