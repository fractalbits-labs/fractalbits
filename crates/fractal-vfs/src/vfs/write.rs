//! Write buffering and the flush/commit path, truncate, fallocate, lseek.
//!
//! The flush is a prepare/commit protocol over versioned write-once BSS
//! keys and `@ovr/` rows:
//!
//! 1. Classify dirty blocks. First writes beyond the committed EOF are
//!    version-1 append territory: they take no row and no burned
//!    version, which is what keeps the map O(overwrites). Everything
//!    else (a row-covered block, a block below the committed EOF, or a
//!    block inside an interrupted append's `pending_append` range) lands
//!    at one freshly burned generation with a row.
//! 2. Prepare CAS: durably burn the generation and record the
//!    version-1 append range before any data I/O. Version 1 is the one
//!    generation two attempts can contest, so an interrupted attempt
//!    must be recorded; a burned generation needs no record beyond the
//!    burn (write-once keys make its fragments invisible and
//!    uncontendable).
//! 3. Bodies, then rows, then reservations. A block's row CAS is issued
//!    only after that block's own body write is acknowledged, or a crash
//!    could leave a row naming a version with no data. Rows carry
//!    `cur_version` above the ceiling: invisible until commit.
//! 4. Commit CAS: the ceiling advances to the burned generation, every
//!    staged row becomes visible atomically, `pending_append` clears,
//!    and `map_epoch` bumps iff any row was written.
//! 5. The superseded exact identities (handed over by the row CASes)
//!    go to the background sweep, after a reader grace.

use bytes::{Bytes, BytesMut};
use data_types::TraceId;
use data_types::object_layout::{InodeRecord, ObjectLayout, ObjectState};
use data_types::object_layout::{ObjectCoreMetaData, ObjectMetaData};
use data_types::ovr_map::{
    BlockFetchPlan, BlockResolution, OvrRowMap, RowState, block_fetch_plan, zeros,
};
use fractal_fuse::{FileHandleId, InodeId};
use futures::{StreamExt, TryStreamExt, stream};
use rkyv::api::high::to_bytes_in;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::cache::DirEntryKind;
use crate::config::WritebackMode;
use crate::disk_cache::{MIRROR_BYTE_BUDGET, MirrorJob};
use crate::error::FsError;
use crate::vfs::row_map::RowWrite;
use crate::vfs::write_buffer::BlockState;
use crate::vfs::write_buffer::WriteBuffer;
use crate::vfs::{
    DEFAULT_BLOCK_SIZE, MAX_INMEM_FILE_SIZE, VfsAttr, VfsCore, parent_prefix_of, posix_only_moved,
};

/// Concurrent BSS body writes per flush. Bodies are independent
/// write-once keys, so pipelining them is safe: the ordering rule is
/// per block, body before that block's row, never body after body.
const BODY_WRITE_CONCURRENCY: usize = 16;
/// Bound on prepare/commit CAS retries that lose only to a posix
/// republish (the async SetPosix worker racing this same handle's
/// flush); a pathological utimensat storm still errors out.
const MAX_POSIX_REBASE_ATTEMPTS: u32 = 16;

/// Merge two inclusive block ranges into their covering span.
fn union_ranges(a: Option<(u32, u32)>, b: Option<(u32, u32)>) -> Option<(u32, u32)> {
    match (a, b) {
        (Some((alo, ahi)), Some((blo, bhi))) => Some((alo.min(blo), ahi.max(bhi))),
        (range @ Some(_), None) | (None, range @ Some(_)) => range,
        (None, None) => None,
    }
}

fn in_range(range: Option<(u32, u32)>, block: u32) -> bool {
    range.is_some_and(|(lo, hi)| lo <= block && block <= hi)
}

impl VfsCore {
    /// Load one block's committed bytes from BSS for an RMW / dirty read /
    /// flush tail-zero, at the exact identity the committed rows resolve.
    /// Returns zeros (length `fallback_content_len`) for a brand-new file,
    /// a hole (row `Hole` / `committed_content_len == 0` / a base-version
    /// miss); propagates other errors. A row-committed generation missing
    /// everywhere is data loss and fails the load.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn lazy_load_block_for_flush(
        &self,
        existing_blob_guid: Option<data_types::DataBlobGuid>,
        committed_rows: Option<&OvrRowMap>,
        committed_ceiling: u64,
        block_num: u32,
        committed_content_len: usize,
        block_size: usize,
        fallback_content_len: usize,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        let Some(guid) = existing_blob_guid else {
            return Ok(zeros(fallback_content_len));
        };
        if committed_content_len == 0 {
            return Ok(zeros(fallback_content_len));
        }
        let (version, read_len, miss_is_loss) = match block_fetch_plan(
            committed_rows,
            block_num,
            committed_ceiling,
            block_size,
            committed_content_len,
        ) {
            BlockFetchPlan::Zeros => return Ok(zeros(fallback_content_len)),
            // The write owner's base snapshot is self-consistent (rows
            // are loaded at the base epoch), so a both-slots-above-
            // ceiling row here is an invariant violation, not a
            // routine race.
            BlockFetchPlan::Stale => {
                return Err(FsError::Internal(format!(
                    "row pair above the committed ceiling during flush load (block {block_num})"
                )));
            }
            BlockFetchPlan::Fetch {
                version,
                read_len,
                miss_is_loss,
            } => (version, read_len, miss_is_loss),
        };
        match self
            .backend()
            .read_block(guid, version, block_num, read_len, trace_id)
            .await
        {
            Ok((data, _)) => Ok(if data.len() > committed_content_len {
                data.slice(0..committed_content_len)
            } else {
                data
            }),
            Err(e) if e.is_block_missing() => {
                if miss_is_loss {
                    tracing::error!(
                        %guid,
                        block_num,
                        version,
                        "DATA LOSS: row-committed generation missing during flush load"
                    );
                    return Err(FsError::DataVg(volume_group_proxy::DataVgError::Corrupted));
                }
                Ok(zeros(fallback_content_len))
            }
            Err(e) => Err(e),
        }
    }

    /// Serve a read against a dirty write handle by merging per-block
    /// intents (`Rewrite` bytes, `Delete`/shrunk-range zeros,
    /// else lazy-loaded committed bytes) over the buffered `file_size`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_dirty_handle(
        &self,
        file_size: u64,
        block_size: u32,
        existing_blob_guid: Option<data_types::DataBlobGuid>,
        committed_rows: Option<&OvrRowMap>,
        committed_ceiling: u64,
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
                            existing_blob_guid,
                            committed_rows,
                            committed_ceiling,
                            b,
                            block_content_len,
                            block_size as usize,
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

    /// After a lost inode CAS, fetch the stored row and return the current
    /// layout iff it diverged from `expected` only by a posix republish
    /// (see `posix_only_moved`); a promoted record must also keep its
    /// nlink and orphan_since. Any real conflict returns None.
    async fn refetch_posix_moved_base(
        &self,
        publish_key: &str,
        promoted_record: Option<&InodeRecord>,
        promoted_inode_id: Option<uuid::Uuid>,
        expected: &ObjectLayout,
        trace_id: &TraceId,
    ) -> Option<ObjectLayout> {
        match promoted_inode_id {
            Some(id) => {
                let record = self.backend().get_inode_record(id, trace_id).await.ok()?;
                let unchanged_record = promoted_record.is_some_and(|prev| {
                    prev.nlink == record.nlink && prev.orphan_since == record.orphan_since
                });
                (unchanged_record && posix_only_moved(expected, &record.layout))
                    .then_some(record.layout)
            }
            None => {
                let current = self.backend().get_inode(publish_key, trace_id).await.ok()?;
                posix_only_moved(expected, &current).then_some(current)
            }
        }
    }

    /// Whether the CAS bytes `wanted` are what the store currently holds
    /// (a lost-reply idempotency probe after a failed inode CAS).
    async fn publish_landed(
        &self,
        publish_key: &str,
        promoted_record: Option<&InodeRecord>,
        promoted_inode_id: Option<uuid::Uuid>,
        wanted: &Bytes,
        trace_id: &TraceId,
    ) -> bool {
        match promoted_inode_id {
            Some(id) => self
                .backend()
                .get_inode_record(id, trace_id)
                .await
                .ok()
                .and_then(|record| wrap_for_publish(promoted_record, &record.layout).ok())
                .is_some_and(|current| current == *wanted),
            None => self
                .backend()
                .get_inode(publish_key, trace_id)
                .await
                .ok()
                .and_then(|layout| wrap_for_publish(None, &layout).ok())
                .is_some_and(|current| current == *wanted),
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
        let (s3_key, ino, file_size, block_size, blocks, eof_low_watermark, trim_upper) = {
            let mut handle = self.file_handles.get_mut(&fh_id).ok_or(FsError::BadFd)?;
            let s3_key = handle.s3_key.clone();
            let ino = handle.ino;
            let wb = match &mut handle.write_buf {
                Some(wb) if wb.dirty => wb,
                _ => return Ok(()),
            };
            let file_size = wb.file_size;
            let block_size = wb.block_size as usize;
            let blocks = std::mem::take(&mut wb.blocks);
            let eof_low_watermark = wb.eof_low_watermark;
            let trim_upper = wb.trim_upper;
            wb.dirty = false;
            (
                s3_key,
                ino,
                file_size,
                block_size,
                blocks,
                eof_low_watermark,
                trim_upper,
            )
        };

        // A name unlinked while its fd stayed open must not be resurrected
        // in NSS, unless the inode was promoted to a hardlink, in which
        // case its data lives in the shared `@hardlink/<id>` InodeRecord
        // blob and the other names still reference it, so the write must
        // still flush (routed to the record below, not this s3_key, whose
        // NSS row holds only an Indirect redirect).
        let (name_removed, promoted_inode_id) = self
            .inodes
            .get(ino)
            .map(|e| (e.name_removed, e.inode_id))
            .unwrap_or((false, None));
        if name_removed && promoted_inode_id.is_none() {
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
        let bsz_u64 = block_size as u64;
        let new_num_blocks = file_size.div_ceil(bsz_u64) as u32;

        // Promoted (hardlink) inodes flush into the shared InodeRecord at
        // `@hardlink/<id>` via CAS, not at this name's s3_key. Fetch the
        // record up front: its layout seeds the flush base (the shared
        // blob_guid + blob_version) and its nlink/orphan_since are
        // preserved on republish.
        let promoted_record_key = promoted_inode_id.map(InodeRecord::key_for);
        let mut promoted_record: Option<InodeRecord> = match promoted_inode_id {
            Some(id) => match self.backend().get_inode_record(id, &trace_id).await {
                Ok(rec) => Some(rec),
                Err(e) => return Err(e),
            },
            None => None,
        };

        let mut base_layout: Option<ObjectLayout> = match &promoted_record {
            Some(rec) => Some(rec.layout.clone()),
            None => self.file_handles.get(&fh_id).and_then(|h| h.layout.clone()),
        };

        // Reclamation input recorded by the commit.
        let mut sweep_victims: Vec<(u32, u64)> = Vec::new();
        let mut sweep_below: Vec<(u32, u64)> = Vec::new();
        let mut posix_rebase_attempts = 0u32;
        // Exact generation of every committed rewrite, for the disk-cache
        // mirror.
        let mut committed_write_versions: BTreeMap<u32, u64> = BTreeMap::new();
        // Claims this flush published, folded back into the write buffer.
        // Rows as stored after this flush's CASes, for the write-through
        // into the cached row snapshot.
        let mut committed_rows: Vec<(u32, data_types::ovr_map::OvrRow)> = Vec::new();
        let (mut final_layout, final_committed_size) = loop {
            let publish_key = promoted_record_key
                .clone()
                .unwrap_or_else(|| s3_key.clone());

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            // On the promoted (hardlink) path, carry the freshly-fetched
            // record's posix forward, NOT the local snapshot taken before
            // this flush: another alias may have chmod/chown'd the shared
            // record between the snapshot and this CAS attempt, and a data
            // write changes only size/data fields (never posix).
            let effective_posix = if promoted_record.is_some() {
                base_layout
                    .as_ref()
                    .map(crate::inode::layout_posix)
                    .unwrap_or_else(|| self.inodes.get(ino).map(|e| e.posix).unwrap_or_default())
            } else {
                self.inodes.get(ino).map(|e| e.posix).unwrap_or_default()
            };
            let build_layout = |blob_guid: data_types::DataBlobGuid,
                                blob_version: u64,
                                next_version: u64,
                                pending_append: Option<(u32, u32)>,
                                map_epoch: u64| {
                let mut layout = ObjectLayout {
                    version_id: ObjectLayout::gen_version_id(),
                    block_size: block_size as u32,
                    timestamp,
                    blob_version,
                    fs_ext: ObjectLayout::fs_ext_from(Some(effective_posix)),
                    state: ObjectState::Normal(ObjectMetaData {
                        blob_guid,
                        core_meta_data: ObjectCoreMetaData {
                            size: file_size,
                            etag: blob_guid.blob_id.simple().to_string(),
                            headers: vec![],
                            checksum: None,
                        },
                    }),
                };
                layout.set_next_version(next_version);
                layout.set_pending_append(pending_append);
                layout.set_map_epoch(map_epoch);
                layout
            };

            let base = base_layout
                .as_ref()
                .and_then(|l| l.blob_guid().ok().map(|g| (g, l.clone())));

            // Create path: no committed base. A fresh blob_guid is minted
            // per attempt, so no key this attempt writes can ever collide
            // with another attempt's bytes: everything (bodies and
            // fallocate claims alike) lands at version 1, unpadded, with
            // no rows.
            let Some((blob_guid, base)) = base else {
                let blob_guid = self.backend().create_blob_guid();
                let unpublished_identities: Vec<(u32, u64)> = snap
                    .blocks
                    .iter()
                    .filter_map(|(block, state)| {
                        matches!(state, BlockState::Rewrite(_)).then_some((*block, 1))
                    })
                    .collect();
                let body_writes = stream::iter(snap.blocks.iter())
                    .filter_map(|(b, st)| async move {
                        match st {
                            BlockState::Rewrite(bytes) => Some((*b, bytes.clone())),
                            _ => None,
                        }
                    })
                    .map(|(b, bytes)| {
                        let trace_id = &trace_id;
                        async move {
                            self.backend()
                                .write_block(blob_guid, b, bytes, 1, trace_id)
                                .await
                        }
                    })
                    .buffer_unordered(BODY_WRITE_CONCURRENCY)
                    .try_collect::<Vec<_>>()
                    .await;
                if let Err(e) = body_writes {
                    self.cleanup_unpublished_blob(blob_guid, unpublished_identities)
                        .await;
                    return Err(e);
                }

                let layout = build_layout(blob_guid, 1, 0, None, 0);
                let publish_bytes = match wrap_for_publish(promoted_record.as_ref(), &layout) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        self.cleanup_unpublished_blob(blob_guid, unpublished_identities)
                            .await;
                        return Err(error);
                    }
                };
                match self
                    .backend()
                    .put_inode_cas(&publish_key, publish_bytes.clone(), Bytes::new(), &trace_id)
                    .await
                {
                    Ok(_prev) => {
                        for (block, state) in snap.blocks.iter() {
                            if matches!(state, BlockState::Rewrite(_)) {
                                committed_write_versions.insert(*block, 1);
                            }
                        }
                        snap.armed = false;
                        break (layout, 0);
                    }
                    Err(FsError::CasConflict) => {
                        // A first publish is a create, not an overwrite. If
                        // the CAS reply was lost and an internal retry saw
                        // the row present, the stored bytes match exactly
                        // and the publish is idempotently complete.
                        // Otherwise another creator won the name.
                        match self.backend().get_inode(&publish_key, &trace_id).await {
                            Ok(cur) => {
                                let cur_bytes = wrap_for_publish(None, &cur);
                                if cur_bytes.is_ok_and(|current| current == publish_bytes) {
                                    for (block, state) in snap.blocks.iter() {
                                        if matches!(state, BlockState::Rewrite(_)) {
                                            committed_write_versions.insert(*block, 1);
                                        }
                                    }
                                    snap.armed = false;
                                    break (layout, 0);
                                }
                            }
                            Err(FsError::NotFound) => {}
                            Err(e) => {
                                self.cleanup_unpublished_blob(blob_guid, unpublished_identities)
                                    .await;
                                return Err(e);
                            }
                        }
                        self.cleanup_unpublished_blob(blob_guid, unpublished_identities)
                            .await;
                        return Err(FsError::CasConflict);
                    }
                    Err(e) => {
                        self.cleanup_unpublished_blob(blob_guid, unpublished_identities)
                            .await;
                        return Err(e);
                    }
                }
            };

            // Overwrite/append path against a committed base.
            let committed_size = base.size().unwrap_or(0);
            let committed_bc = committed_size.div_ceil(bsz_u64) as u32;
            let base_ceiling = base.blob_version;
            let version = base.next_burn_version();
            let base_rows = self.layout_row_map(&base).await?;
            let base_rows_ref = base_rows.as_deref();
            let abandoned = base.pending_append();

            let has_row =
                |b: u32| -> bool { base_rows_ref.is_some_and(|rows| rows.get(b).is_some()) };

            // Version-1 append territory: first writes beyond the
            // committed EOF, outside any interrupted append's range. A
            // recorded range is contested at version 1, so every block
            // inside it must be re-attempted at the burned generation.
            let v1_append_blocks: BTreeSet<u32> = snap
                .blocks
                .iter()
                .filter_map(|(b, st)| {
                    (matches!(st, BlockState::Rewrite(_))
                        && !has_row(*b)
                        && *b >= committed_bc
                        && !in_range(abandoned, *b))
                    .then_some(*b)
                })
                .collect();
            let v1_span = match (v1_append_blocks.first(), v1_append_blocks.last()) {
                (Some(lo), Some(hi)) => Some((*lo, *hi)),
                _ => None,
            };
            // The prepared record carries the union of the abandoned
            // range and this attempt's own version-1 span: if this
            // attempt also dies, the next one sees the whole contested
            // territory. The commit clears it, having resolved every
            // block in the union (written at `version`, written at v1 as
            // committed content, or Hole-rowed by the remainder pass
            // below).
            let pending_union = union_ranges(abandoned, v1_span);

            // Rewrites at the burned generation: everything dirty that is
            // not version-1 territory.
            let burned_rewrites: BTreeSet<u32> = snap
                .blocks
                .iter()
                .filter_map(|(b, st)| {
                    (matches!(st, BlockState::Rewrite(_)) && !v1_append_blocks.contains(b))
                        .then_some(*b)
                })
                .collect();
            let punched: BTreeSet<u32> = snap
                .blocks
                .iter()
                .filter_map(|(b, st)| matches!(st, BlockState::Delete).then_some(*b))
                .collect();

            // Trim range: blocks logically destroyed by a shrink must
            // read zeros even while their superseded generations await
            // the sweep, and must never resurrect on a regrow, so they
            // get Hole rows. One bounded listing finds which unmapped
            // blocks actually hold physical keys, so a sparse trim rows
            // nothing (the design's metadata-only claim holds for mapped
            // blocks; version-1 territory needs this one probe).
            let trim_lo =
                std::cmp::min(new_num_blocks, eof_low_watermark.unwrap_or(new_num_blocks));
            let trim_hi = committed_bc.max(trim_upper.unwrap_or(0));
            let mut trim_hole_blocks: BTreeSet<u32> = BTreeSet::new();
            if trim_lo < trim_hi {
                if let Some(rows) = base_rows_ref {
                    for (b, row) in rows.range(trim_lo, trim_hi) {
                        if snap.blocks.contains_key(&b) {
                            continue;
                        }
                        if row.cur_version <= base_ceiling && row.cur_state == RowState::Written {
                            trim_hole_blocks.insert(b);
                            sweep_victims.push((b, row.cur_version));
                        }
                        // A committed Hole needs no new row; a staged cur
                        // above the ceiling is superseded by the row CAS
                        // if this block is ever touched again.
                    }
                }
                let count = trim_hi - trim_lo;
                let entries = self
                    .backend()
                    .list_blob_blocks(blob_guid, trim_lo, count, &trace_id)
                    .await?;
                for entry in entries {
                    let b = entry.block_number;
                    if snap.blocks.contains_key(&b) || has_row(b) {
                        continue;
                    }
                    if entry.version == 1 {
                        // Committed (or claimed) base-version content:
                        // must become a Hole row or a regrow resurrects
                        // it.
                        trim_hole_blocks.insert(b);
                    }
                    // Every listed generation of an unmapped trimmed
                    // block is garbage once the shrink commits; orphans
                    // above the ceiling included.
                    if entry.version != version {
                        sweep_victims.push((b, entry.version));
                    }
                }
            }

            // The abandoned range's unresolved remainder becomes Hole
            // rows at `version`, so the contested version-1 fragments
            // can never surface where zeros are required (a later
            // regrow included).
            let mut remainder_holes: BTreeSet<u32> = BTreeSet::new();
            if let Some((lo, hi)) = abandoned {
                for b in lo..=hi {
                    if !burned_rewrites.contains(&b) && !punched.contains(&b) {
                        remainder_holes.insert(b);
                    }
                }
            }

            // Step 2: prepare CAS. Durably burns `version` and records
            // the version-1 territory before any data I/O. `blob_version`
            // stays at the reader-visible ceiling until commit. The
            // doomed-preparer property starts here: any older prepared
            // flush can no longer commit, so the ceiling cannot move
            // before this flush commits or aborts, which is what makes
            // the row CASes' conditional promotion sound.
            let mut prepare = base.clone();
            prepare.set_next_version(version + 1);
            prepare.set_pending_append(pending_union);
            {
                let old_bytes = wrap_for_publish(promoted_record.as_ref(), &base)?;
                let new_bytes = wrap_for_publish(promoted_record.as_ref(), &prepare)?;
                if let Err(error) = self
                    .backend()
                    .put_inode_cas(&publish_key, new_bytes.clone(), old_bytes, &trace_id)
                    .await
                {
                    let landed = self
                        .publish_landed(
                            &publish_key,
                            promoted_record.as_ref(),
                            promoted_inode_id,
                            &new_bytes,
                            &trace_id,
                        )
                        .await;
                    if !landed {
                        if posix_rebase_attempts < MAX_POSIX_REBASE_ATTEMPTS
                            && let Some(current) = self
                                .refetch_posix_moved_base(
                                    &publish_key,
                                    promoted_record.as_ref(),
                                    promoted_inode_id,
                                    &base,
                                    &trace_id,
                                )
                                .await
                        {
                            posix_rebase_attempts += 1;
                            if let Some(mut handle) = self.file_handles.get_mut(&fh_id) {
                                handle.layout = Some(current.clone());
                                handle.layout_refreshed_at = Instant::now();
                            }
                            if let Some(mut entry) = self.inodes.get_mut(ino) {
                                entry.layout = Some(current.clone());
                            }
                            base_layout = Some(current);
                            sweep_victims.clear();
                            continue;
                        }
                        return Err(error);
                    }
                }
            }
            if let Some(rec) = promoted_record.as_mut() {
                rec.layout = prepare.clone();
            }
            if let Some(mut handle) = self.file_handles.get_mut(&fh_id) {
                handle.layout = Some(prepare.clone());
                handle.layout_refreshed_at = Instant::now();
            }
            if let Some(mut entry) = self.inodes.get_mut(ino) {
                entry.layout = Some(prepare.clone());
            }

            // Step 3: bodies, pipelined (independent write-once keys;
            // only a block's own row has to wait for it). Burned
            // generations are padded to a full block_size (constant EC
            // shard size); version-1 appends keep their natural length.
            // A version-1 write that collides with an existing claim of
            // the same identity is a fallocate fill: retry it as a
            // reserved-claim conversion (padded, since the claim's
            // allocation is block_size wide).
            let v1_append_blocks_ref = &v1_append_blocks;
            stream::iter(snap.blocks.iter())
                .filter_map(|(b, st)| async move {
                    match st {
                        BlockState::Rewrite(bytes) => Some((*b, bytes.clone())),
                        _ => None,
                    }
                })
                .map(|(b, bytes)| {
                    let trace_id = &trace_id;
                    async move {
                        let write_version = if v1_append_blocks_ref.contains(&b) {
                            1
                        } else {
                            version
                        };
                        let pad = write_version > 1;
                        let body = if pad && bytes.len() < block_size {
                            let mut buf = BytesMut::with_capacity(block_size);
                            buf.extend_from_slice(&bytes);
                            buf.resize(block_size, 0);
                            buf.freeze()
                        } else {
                            bytes.clone()
                        };
                        self.backend()
                            .write_block(blob_guid, b, body, write_version, trace_id)
                            .await
                    }
                })
                .buffer_unordered(BODY_WRITE_CONCURRENCY)
                .try_collect::<Vec<_>>()
                .await?;

            // Step 4: rows, issued only now that every body this flush
            // names is acknowledged. All staged rows carry
            // cur_version = `version` above the ceiling: invisible until
            // the commit, all-or-nothing at the commit.
            let mut row_writes: Vec<RowWrite> = Vec::new();
            for b in burned_rewrites.iter() {
                row_writes.push(RowWrite {
                    block: *b,
                    state: RowState::Written,
                });
            }
            for b in punched
                .iter()
                .chain(trim_hole_blocks.iter())
                .chain(remainder_holes.iter())
            {
                row_writes.push(RowWrite {
                    block: *b,
                    state: RowState::Hole,
                });
            }
            row_writes.sort_by_key(|write| write.block);
            row_writes.dedup_by_key(|write| write.block);
            let rows_written = !row_writes.is_empty();
            if rows_written {
                let outcome = self
                    .write_rows_for_flush(
                        blob_guid,
                        &row_writes,
                        version,
                        base_ceiling,
                        base_rows_ref,
                        &trace_id,
                    )
                    .await?;
                sweep_victims.extend(outcome.victims);
                committed_rows = outcome.rows;
            }
            // Superseded base-version identities of unmapped territory:
            // a burned rewrite or punch below the committed EOF replaces
            // whatever sat at version 1 (data or a claim; a miss is an
            // idempotent no-op for the sweep).
            for b in burned_rewrites.iter().chain(punched.iter()) {
                if !has_row(*b) && *b < committed_bc {
                    sweep_victims.push((*b, 1));
                }
                if in_range(abandoned, *b) {
                    // Re-attempted at the burned version instead of v1,
                    // so the contested version-1 fragment this block
                    // may carry is abandoned; reclaim it.
                    sweep_victims.push((*b, 1));
                }
            }
            // A burned rewrite over a Hole row may be superseding a
            // claim at an unknown burned generation (a filled fallocate
            // over a punched block): resolve those via one listing in
            // the sweep.
            for b in burned_rewrites.iter() {
                if let Some(row) = base_rows_ref.and_then(|rows| rows.get(*b))
                    && row.cur_version <= base_ceiling
                    && row.cur_state == RowState::Hole
                {
                    sweep_below.push((*b, version));
                }
            }

            // Step 6: commit CAS. The ceiling advances to `version`,
            // every staged row becomes visible together, the version-1
            // territory is resolved (pending_append clears), and
            // map_epoch bumps iff this commit wrote any row.
            let map_epoch = if rows_written {
                version
            } else {
                base.map_epoch()
            };
            let layout = build_layout(blob_guid, version, version + 1, None, map_epoch);
            let new_bytes = wrap_for_publish(promoted_record.as_ref(), &layout)?;
            let mut commit_guard = prepare.clone();
            loop {
                let old_bytes = wrap_for_publish(promoted_record.as_ref(), &commit_guard)?;
                let commit_result = self
                    .backend()
                    .put_inode_cas(&publish_key, new_bytes.clone(), old_bytes, &trace_id)
                    .await;
                let Err(error) = commit_result else { break };
                if self
                    .publish_landed(
                        &publish_key,
                        promoted_record.as_ref(),
                        promoted_inode_id,
                        &new_bytes,
                        &trace_id,
                    )
                    .await
                {
                    break;
                }
                if posix_rebase_attempts < MAX_POSIX_REBASE_ATTEMPTS
                    && let Some(current) = self
                        .refetch_posix_moved_base(
                            &publish_key,
                            promoted_record.as_ref(),
                            promoted_inode_id,
                            &commit_guard,
                            &trace_id,
                        )
                        .await
                {
                    posix_rebase_attempts += 1;
                    commit_guard = current;
                    continue;
                }
                return Err(error);
            }

            for b in snap
                .blocks
                .iter()
                .filter_map(|(b, st)| matches!(st, BlockState::Rewrite(_)).then_some(*b))
            {
                let committed_version = if v1_append_blocks.contains(&b) {
                    1
                } else {
                    version
                };
                committed_write_versions.insert(b, committed_version);
            }
            snap.armed = false;
            break (layout, committed_size);
        };

        // Write-through the committed rows into the cached snapshot so
        // this writer never reloads its own rows: clone the base
        // snapshot, overlay the stored rows, retag at the new epoch.
        if let Ok(final_blob_guid) = final_layout.blob_guid() {
            if committed_rows.is_empty() {
                // No rows written: any cached snapshot is still valid at
                // its (unchanged) epoch.
            } else {
                let mut fresh = self
                    .row_maps
                    .lock()
                    .get(&final_blob_guid.blob_id)
                    .map(|cached| (**cached).clone())
                    .unwrap_or_default();
                fresh.epoch = final_layout.map_epoch();
                for (block, row) in committed_rows.drain(..) {
                    fresh.insert(block, row);
                }
                self.install_row_map(final_blob_guid.blob_id, std::sync::Arc::new(fresh));
            }
        }

        // Update file handle: install the new layout (next CAS guard),
        // clear dirty/size_changed, reset shrink state, and point the buffer
        // at the published blob_guid for subsequent lazy loads.
        if let Some(mut handle) = self.file_handles.get_mut(&fh_id) {
            handle.layout = Some(final_layout.clone());
            handle.layout_refreshed_at = Instant::now();
            if let Some(ref mut wb) = handle.write_buf {
                wb.dirty = false;
                wb.size_changed = false;
                wb.eof_low_watermark = None;
                wb.trim_upper = None;
                wb.existing_blob_guid = final_layout.blob_guid().ok();
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
        // from memory without a cross-instance coherency round-trip.
        if promoted_inode_id.is_none()
            && let Some(mut e) = self.inodes.get_mut(ino)
        {
            e.layout = Some(final_layout.clone());
        }

        // If this inode is a promoted hardlink, persist the record identity
        // and resolved layout/posix onto the inode entry.
        if let Some(id) = promoted_inode_id
            && let Some(mut e) = self.inodes.get_mut(ino)
        {
            e.inode_id = Some(id);
            e.posix = crate::inode::layout_posix(&final_layout);
            e.layout = Some(final_layout.clone());
        }

        let parent_prefix = parent_prefix_of(&s3_key);
        let name = s3_key
            .trim_end_matches('/')
            .rsplit_once('/')
            .map(|(_, n)| n.to_string())
            .unwrap_or_else(|| s3_key.clone());
        self.cache_dir_entry(&parent_prefix, &name, ino, DirEntryKind::RegularFile);

        // Sync the local disk cache to the writer's just-published
        // state: rewrites land at their natural offsets with their exact
        // committed generation, deletes punch holes, and the file-level
        // commit epoch advances to fence stale mirror jobs.
        //
        // Best-effort on the create path: a sync failure is logged and
        // the next read cold-fetches from BSS.
        if let Some(dc) = &self.disk_cache
            && let Ok(final_blob_guid) = final_layout.blob_guid()
        {
            let bsz_u64 = block_size as u64;
            let rewrites: Vec<(u32, Bytes)> = snap
                .blocks
                .iter()
                .filter_map(|(b, s)| match s {
                    BlockState::Rewrite(bytes) => Some((*b, bytes.clone())),
                    _ => None,
                })
                .collect();

            let new_bc = file_size.div_ceil(bsz_u64) as u32;
            let committed_bc = final_committed_size.div_ceil(bsz_u64) as u32;
            let trim_lo = eof_low_watermark.map(|w| w.min(new_bc)).unwrap_or(new_bc);
            let trim_hi = trim_upper.unwrap_or(committed_bc).max(committed_bc);

            let mut deletes: Vec<u32> = (trim_lo..trim_hi)
                .filter(|b| !matches!(snap.blocks.get(b), Some(BlockState::Rewrite(_))))
                .collect();
            for (b, s) in snap.blocks.iter() {
                if matches!(s, BlockState::Delete) {
                    deletes.push(*b);
                }
            }

            let blob_version = final_layout.blob_version;

            if blob_version > 1 {
                // Overwrite path: mirror the cache SYNCHRONOUSLY before
                // the flush returns. An overwrite can have a pre-existing
                // cache file that concurrent readers already trust; each
                // rewritten block is stamped with its exact committed
                // generation, and the file epoch fences any still-queued
                // older mirror job. fdatasync is still dropped, so this
                // is page-cache-cheap.
                let exact_rewrites: Vec<(u32, u64, Bytes)> = rewrites
                    .iter()
                    .map(|(block, bytes)| {
                        (
                            *block,
                            committed_write_versions
                                .get(block)
                                .copied()
                                .unwrap_or(blob_version),
                            bytes.clone(),
                        )
                    })
                    .collect();
                if let Err(e) = dc
                    .sync_after_flush_exact(
                        final_blob_guid,
                        blob_version,
                        &exact_rewrites,
                        &deletes,
                    )
                    .await
                {
                    // An overwrite mirror cannot be best-effort: a partial
                    // failure can leave a superseded block as a valid
                    // populated+checksum hit. Drop the whole cache file so
                    // every block cold-fetches the authoritative bytes.
                    tracing::warn!(
                        %final_blob_guid,
                        error = %e,
                        "disk cache overwrite mirror failed; dropping cache file"
                    );
                    dc.drop_blob(final_blob_guid, blob_version).await;
                }
            } else if let Some(mirror) = &self.mirror {
                // Fresh create (the create-storm hot path): hand the cache
                // write to the dedicated mirror thread so the local I/O +
                // xxh3 never run on a FUSE worker. A fresh blob has no pre-
                // existing cache file and a single version, so there is no
                // stale-byte window for any reader. `try_send` never
                // blocks; the queue is bounded by both job count and
                // retained bytes, and over budget the job is dropped (best-
                // effort; the block cold-fills from BSS on the next read).
                let byte_len: usize = rewrites.iter().map(|(_, b)| b.len()).sum();
                let queued = mirror.queued_bytes.fetch_add(byte_len, Ordering::Relaxed);
                if queued + byte_len > MIRROR_BYTE_BUDGET {
                    mirror.queued_bytes.fetch_sub(byte_len, Ordering::Relaxed);
                    tracing::trace!(
                        %final_blob_guid,
                        byte_len,
                        "disk cache mirror byte budget exceeded; dropping (best-effort)"
                    );
                } else {
                    let job = MirrorJob {
                        blob_guid: final_blob_guid,
                        blob_version,
                        rewrites,
                        deletes,
                        byte_len,
                    };
                    if let Err(e) = mirror.tx.clone().try_send(job) {
                        mirror.queued_bytes.fetch_sub(byte_len, Ordering::Relaxed);
                        if e.is_full() {
                            tracing::trace!(
                                %final_blob_guid,
                                "disk cache mirror queue full; dropping (best-effort)"
                            );
                        } else {
                            tracing::warn!(
                                %final_blob_guid,
                                "disk cache mirror channel closed; dropping (best-effort)"
                            );
                        }
                    }
                }
            }
        }

        // Reclaim what this commit superseded: the exact identities the
        // row CASes displaced, the base-version keys of re-identified
        // unmapped blocks, and (rarely) below-floor claims resolved via
        // one listing. Best-effort and off the flush path, after the
        // reader grace; a crash before the sweep leaks invisible garbage
        // until the block is rewritten or the file unlinked.
        if let Ok(final_blob_guid) = final_layout.blob_guid() {
            self.enqueue_superseded_sweep(
                final_blob_guid,
                std::mem::take(&mut sweep_victims),
                std::mem::take(&mut sweep_below),
            );
        }

        // Update inode table layout
        {
            let handle = self.file_handles.get(&fh_id);
            if let Some(handle) = handle
                && let Some(mut entry) = self.inodes.get_mut(handle.ino)
            {
                entry.layout = Some(final_layout.clone());
            }
        }

        if promoted_inode_id.is_none() {
            match self
                .publish_posix_catchup_after_flush(ino, &s3_key, &final_layout, &trace_id)
                .await
            {
                Ok(Some(posix_layout)) => {
                    final_layout = posix_layout;
                    if let Some(mut handle) = self.file_handles.get_mut(&fh_id) {
                        handle.layout = Some(final_layout.clone());
                        handle.layout_refreshed_at = Instant::now();
                    }
                    if let Some(mut entry) = self.inodes.get_mut(ino) {
                        entry.layout = Some(final_layout.clone());
                    }
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
        let (block_size, existing_blob_guid, committed_size, committed_layout, blocks_to_load) = {
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
            let layout_blob_guid = handle.layout.as_ref().and_then(|l| l.blob_guid().ok());
            let committed_layout = handle.layout.clone();
            let wb = handle
                .write_buf
                .get_or_insert_with(|| WriteBuffer::new(layout_blob_guid, committed_size, bsize));
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
                wb.existing_blob_guid,
                committed_size,
                committed_layout,
                to_load,
            )
        };

        // Phase 2: lazy-load the partial blocks outside the guard. The
        // common aligned streaming write loads nothing and skips the
        // row-map lookup entirely.
        let trace_id = TraceId::new();
        let mut loaded: BTreeMap<u32, Bytes> = BTreeMap::new();
        let (committed_rows, committed_ceiling) = if blocks_to_load.is_empty() {
            (None, 0)
        } else {
            self.rows_and_ceiling(committed_layout.as_ref()).await?
        };
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
                    existing_blob_guid,
                    committed_rows.as_deref(),
                    committed_ceiling,
                    b,
                    committed_content_len,
                    block_size as usize,
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
            // A real upload supersedes any prior fallocate reservation.
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
        let (block_size, existing_blob_guid, committed_size, committed_layout, edge_loads) = {
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
            let layout_blob_guid = handle.layout.as_ref().and_then(|l| l.blob_guid().ok());
            let committed_layout = handle.layout.clone();
            let wb = handle.write_buf.get_or_insert_with(|| {
                WriteBuffer::new(layout_blob_guid, committed_size, block_size)
            });
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
                let last_full_excl = (hole_end / bsz_u64) as u32;

                let lo_block = (offset / bsz_u64) as u32;
                let hi_block = (hole_end / bsz_u64) as u32;

                // Determine which edge blocks need a lazy load. We only
                // load when:
                //   - The block has committed bytes in BSS, AND
                //   - There isn't already a buffered `Rewrite`
                //     copy we can edit in place, AND
                //   - The shrink-destroys watermark hasn't already
                //     turned this block into zeros.
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
                    // hi_block >= first_full means hi_block is past the
                    // last fully-covered interior block.
                    let _ = last_full_excl; // silence unused warning when no full blocks
                    consider_edge(hi_block);
                }
            }
            (
                block_size,
                wb.existing_blob_guid,
                committed_size,
                committed_layout,
                edge_loads,
            )
        };

        // Phase 2: lazy-load edge blocks outside the DashMap guard.
        let trace_id = TraceId::new();
        let mut loaded: BTreeMap<u32, Bytes> = BTreeMap::new();
        if punch_hole && !edge_loads.is_empty() {
            let (committed_rows, committed_ceiling) =
                self.rows_and_ceiling(committed_layout.as_ref()).await?;
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
                        existing_blob_guid,
                        committed_rows.as_deref(),
                        committed_ceiling,
                        b,
                        committed_content_len,
                        block_size as usize,
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
    /// the first match. Buffered intents answer first; then the row
    /// snapshot (Written = data, Hole = hole); unmapped blocks fall
    /// through to one bounded `ListBlobBlocks` probe where a
    /// base-version entry (data or a reserved claim, per the Linux
    /// SEEK_DATA convention) counts as data. Listed entries at other
    /// versions are pre-sweep garbage of row-covered blocks and must
    /// not override the rows' answer.
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
        let committed_layout = self
            .file_handles
            .get(&fh)
            .ok_or(FsError::BadFd)?
            .layout
            .clone();
        let (file_size, block_size, probe_blob_guid, blocks, eof_low_watermark) = {
            let handle = self.file_handles.get(&fh).ok_or(FsError::BadFd)?;
            let layout_block_size = handle
                .layout
                .as_ref()
                .map(|l| l.block_size)
                .unwrap_or(DEFAULT_BLOCK_SIZE);
            let layout_size = handle
                .layout
                .as_ref()
                .and_then(|l| l.size().ok())
                .unwrap_or(0);
            let layout_blob_guid = handle.layout.as_ref().and_then(|l| l.blob_guid().ok());
            if let Some(ref wb) = handle.write_buf {
                (
                    wb.file_size,
                    wb.block_size,
                    wb.existing_blob_guid,
                    wb.blocks.clone(),
                    wb.eof_low_watermark,
                )
            } else {
                (
                    layout_size,
                    layout_block_size,
                    layout_blob_guid,
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
        // `None` -> not buffered, fall through to rows / the BSS probe.
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

        let (committed_rows, committed_ceiling) =
            self.rows_and_ceiling(committed_layout.as_ref()).await?;
        let row_kind = |b: u32| -> Option<bool> {
            committed_rows
                .as_deref()
                .and_then(|rows| match rows.resolve(b, committed_ceiling) {
                    BlockResolution::Exact { .. } => Some(true),
                    BlockResolution::Hole => Some(false),
                    BlockResolution::Base | BlockResolution::Stale => None,
                })
        };

        let trace_id = TraceId::new();
        let v1_present: BTreeSet<u32> = match probe_blob_guid {
            Some(guid) => {
                let count = last_block_excl.saturating_sub(first_block);
                if count == 0 {
                    BTreeSet::new()
                } else {
                    let entries = self
                        .backend()
                        .list_blob_blocks(guid, first_block, count, &trace_id)
                        .await?;
                    entries
                        .into_iter()
                        .filter(|e| e.version == 1)
                        .map(|e| e.block_number)
                        .collect()
                }
            }
            None => BTreeSet::new(),
        };

        for b in first_block..last_block_excl {
            let is_data = match buffered_kind(b) {
                Some(d) => d,
                None => match row_kind(b) {
                    Some(d) => d,
                    None => v1_present.contains(&b),
                },
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
        let (block_size, committed_size, existing_blob_guid, committed_layout, tail_zero_target) = {
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
            let existing_blob_guid = handle.layout.as_ref().and_then(|l| l.blob_guid().ok());
            let committed_layout = handle.layout.clone();
            let wb = handle.write_buf.get_or_insert_with(|| {
                WriteBuffer::new(existing_blob_guid, committed_size, block_size)
            });
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
                existing_blob_guid,
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
                    let (committed_rows, committed_ceiling) =
                        self.rows_and_ceiling(committed_layout.as_ref()).await?;
                    let block_start = (last as u64) * (block_size as u64);
                    let committed_content_len = if block_start < committed_size {
                        std::cmp::min(block_size as u64, committed_size - block_start) as usize
                    } else {
                        0
                    };
                    self.lazy_load_block_for_flush(
                        existing_blob_guid,
                        committed_rows.as_deref(),
                        committed_ceiling,
                        last,
                        committed_content_len,
                        bsz_usize,
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

/// Serialize `layout` as the publish value/guard for a file: bare layout
/// at the s3_key, or wrapped in the shared InodeRecord for a promoted
/// inode (rkyv is deterministic for these types, which is what makes
/// byte-equality CAS sound).
fn wrap_for_publish(rec: Option<&InodeRecord>, layout: &ObjectLayout) -> Result<Bytes, FsError> {
    match rec {
        Some(rec) => {
            let record = InodeRecord {
                layout: layout.clone(),
                nlink: rec.nlink,
                orphan_since: rec.orphan_since,
            };
            Ok(to_bytes_in::<_, rkyv::rancor::Error>(&record, Vec::new())
                .map_err(FsError::from)?
                .into())
        }
        None => Ok(to_bytes_in::<_, rkyv::rancor::Error>(layout, Vec::new())
            .map_err(FsError::from)?
            .into()),
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

#[cfg(test)]
mod range_tests {
    use super::*;

    #[test]
    fn pending_append_union_spans() {
        assert_eq!(union_ranges(None, None), None);
        assert_eq!(union_ranges(Some((3, 5)), None), Some((3, 5)));
        assert_eq!(union_ranges(None, Some((7, 9))), Some((7, 9)));
        assert_eq!(union_ranges(Some((3, 5)), Some((7, 9))), Some((3, 9)));
        assert_eq!(union_ranges(Some((7, 9)), Some((3, 5))), Some((3, 9)));
        assert!(in_range(Some((3, 5)), 3));
        assert!(in_range(Some((3, 5)), 5));
        assert!(!in_range(Some((3, 5)), 6));
        assert!(!in_range(None, 0));
    }
}
