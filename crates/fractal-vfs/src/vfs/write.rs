//! Write buffering and the flush/commit path, truncate, fallocate, lseek.

use bytes::{Bytes, BytesMut};
use data_types::TraceId;
use data_types::object_layout::{InodeRecord, ObjectLayout, ObjectState};
use data_types::object_layout::{ObjectCoreMetaData, ObjectMetaData};
use fractal_fuse::{FileHandleId, InodeId};
use futures::{StreamExt, TryStreamExt, stream};
use rkyv::api::high::to_bytes_in;
use std::sync::atomic::Ordering;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use volume_group_proxy::AtOrBeforeRead;

use crate::vfs::RESERVATION_CONCURRENCY;

/// Concurrent body/tombstone puts per flush (independent write-once
/// keys; ordering only matters against the commit CAS).
const BODY_WRITE_CONCURRENCY: usize = 16;

use crate::cache::DirEntryKind;
use crate::config::WritebackMode;
use crate::disk_cache::{MIRROR_BYTE_BUDGET, MirrorJob};
use crate::error::FsError;
use crate::vfs::write_buffer::BlockState;
use crate::vfs::write_buffer::WriteBuffer;
use crate::vfs::{DEFAULT_BLOCK_SIZE, MAX_INMEM_FILE_SIZE, VfsAttr, VfsCore, parent_prefix_of};

impl VfsCore {
    /// Load one block's committed bytes from BSS for an RMW / dirty read /
    /// flush tail-zero, via at-or-before selection at the committed
    /// ceiling. Returns zeros (length `fallback_content_len`) for a
    /// brand-new file, a hole, or a sparse miss; propagates other errors.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn lazy_load_block_for_flush(
        &self,
        existing_blob_guid: Option<data_types::DataBlobGuid>,
        ceiling: u64,
        block_num: u32,
        committed_content_len: usize,
        block_size: usize,
        fallback_content_len: usize,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        let Some(guid) = existing_blob_guid else {
            return Ok(Bytes::from(vec![0u8; fallback_content_len]));
        };
        if committed_content_len == 0 {
            return Ok(Bytes::from(vec![0u8; fallback_content_len]));
        }
        let read_len = block_size.max(committed_content_len);
        match self
            .backend()
            .read_block_at_or_before(guid, ceiling, block_num, read_len, trace_id)
            .await?
        {
            AtOrBeforeRead::Data { body, .. } => Ok(if body.len() > committed_content_len {
                body.slice(0..committed_content_len)
            } else {
                body
            }),
            AtOrBeforeRead::Zeros { .. }
            | AtOrBeforeRead::Hole { .. }
            | AtOrBeforeRead::SparseHole => Ok(Bytes::from(vec![0u8; fallback_content_len])),
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
        committed_ceiling: u64,
        blocks: &std::collections::BTreeMap<u32, BlockState>,
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
                Some(BlockState::Delete) => Bytes::from(vec![0u8; block_content_len]),
                None => {
                    if eof_low_watermark.is_some_and(|low| b >= low) {
                        Bytes::from(vec![0u8; block_content_len])
                    } else {
                        self.lazy_load_block_for_flush(
                            existing_blob_guid,
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
    /// the flush takes `blocks`/`pending_reservations` and clears `dirty`
    /// up front, so any error after that point must put them back or the
    /// write is silently lost. Re-inserts without clobbering newer writes.
    pub(crate) fn restore_flush_snapshot(
        &self,
        fh_id: FileHandleId,
        blocks: std::collections::BTreeMap<u32, BlockState>,
        pending_reservations: std::collections::BTreeSet<u32>,
    ) {
        if let Some(mut handle) = self.file_handles.get_mut(&fh_id)
            && let Some(ref mut wb) = handle.write_buf
        {
            for (b, st) in blocks {
                wb.blocks.entry(b).or_insert(st);
            }
            for b in pending_reservations {
                wb.pending_reservations.insert(b);
            }
            wb.dirty = true;
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
        let (
            s3_key,
            ino,
            file_size,
            block_size,
            blocks,
            eof_low_watermark,
            trim_upper,
            pending_reservations,
            committed_reservations,
        ) = {
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
            let pending_reservations = std::mem::take(&mut wb.pending_reservations);
            let committed_reservations = wb.committed_reservations.clone();
            wb.dirty = false;
            (
                s3_key,
                ino,
                file_size,
                block_size,
                blocks,
                eof_low_watermark,
                trim_upper,
                pending_reservations,
                committed_reservations,
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
            pending_reservations,
            armed: true,
        };

        let trace_id = TraceId::new();
        let bsz_u64 = block_size as u64;
        let new_num_blocks = file_size.div_ceil(bsz_u64) as u32;

        // Promoted (hardlink) inodes flush into the shared InodeRecord at
        // `@hardlink/<id>` via CAS, not at this name's s3_key. Fetch the
        // record up front: its layout seeds the override-flush base (the
        // shared blob_guid + blob_version) and its nlink/orphan_since are
        // preserved on republish.
        let promoted_record_key = promoted_inode_id.map(InodeRecord::key_for);
        // The publish CAS guards on the fetched record re-serialized (rkyv is
        // deterministic for these types, as the s3_key flush CAS also relies
        // on), so we keep only the decoded record here.
        let mut promoted_record: Option<InodeRecord> = match promoted_inode_id {
            Some(id) => match self.backend().get_inode_record(id, &trace_id).await {
                Ok(rec) => Some(rec),
                Err(e) => return Err(e),
            },
            None => None,
        };

        // Override flush: reuse the file's stable blob_guid, bump
        // blob_version, write only the dirty (`Rewrite`) blocks in place at
        // the new version, CAS-publish the layout, then trim blocks past the
        // (possibly shrunk) EOF and replay PUNCH_HOLE deletes. Old blocks
        // are never blindly deleted; holes (absent blocks) are never
        // written. The CAS guard makes a stale/cross-instance publish lose
        // the race instead of clobbering the winner. For a promoted inode
        // the base is the record's layout (the shared blob), not the
        // redirect at the handle's s3_key.
        let mut base_layout: Option<ObjectLayout> = match &promoted_record {
            Some(rec) => Some(rec.layout.clone()),
            None => self.file_handles.get(&fh_id).and_then(|h| h.layout.clone()),
        };

        // Reclamation input recorded by the commit: for every touched
        // block, delete generations below its committed identity on every
        // placement node (superseded generations, plus any orphan
        // fragments earlier interrupted attempts left on these blocks),
        // after the reclamation grace.
        let mut sweep_below: Vec<(u32, u64)> = Vec::new();
        // A CAS that loses only to a posix republish (the async SetPosix
        // worker racing this same handle's flush) rebases and retries;
        // bounded so a pathological utimensat storm still errors out.
        const MAX_POSIX_REBASE_ATTEMPTS: u32 = 16;
        let mut posix_rebase_attempts = 0u32;
        let mut committed_write_versions: std::collections::BTreeMap<u32, u64> =
            std::collections::BTreeMap::new();
        // Committed fallocate claims to fold back into the write buffer
        // after a successful commit.
        let mut new_committed_reservations: Vec<(u32, u64)> = Vec::new();
        let (mut final_layout, final_committed_size) = loop {
            // Serialize `layout` as the publish value/guard for this file:
            // bare layout at the s3_key, or wrapped in the shared
            // InodeRecord for a promoted inode (rkyv is deterministic for
            // these types, which is what makes byte-equality CAS sound).
            fn wrap_for_publish(
                rec: Option<&InodeRecord>,
                layout: &ObjectLayout,
            ) -> Result<Bytes, FsError> {
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
            let build_final_layout =
                |blob_guid: data_types::DataBlobGuid, blob_version: u64, next_version: u64| {
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
                    layout
                };

            let base = base_layout
                .as_ref()
                .and_then(|l| l.blob_guid().ok().map(|g| (g, l.clone())));

            // Create path: no committed base. A fresh blob_guid is
            // minted per attempt, so no key this attempt writes can ever
            // collide with another attempt's bytes: everything lands at
            // version 1, unpadded, with no map.
            let Some((blob_guid, base)) = base else {
                let blob_guid = self.backend().create_blob_guid();
                let mut unpublished_identities: Vec<(u32, u64)> = snap
                    .blocks
                    .iter()
                    .filter_map(|(block, state)| {
                        matches!(state, BlockState::Rewrite(_)).then_some((*block, 1))
                    })
                    .collect();
                // Bodies are independent write-once keys: pipeline them.
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
                    self.cleanup_unpublished_blob(blob_guid, unpublished_identities, &trace_id)
                        .await;
                    return Err(e);
                }

                // A create-time fallocate claim is part of the publish
                // precondition. Reserve every untouched block before the
                // inode CAS so ENOSPC or a reserve quorum failure reaches the
                // caller and no metadata can advertise unallocated space.
                // Claims land at version 2 so the published ceiling (2)
                // makes them visible to at-or-before reads as zeros.
                let reservation_version = 2;
                let reservation_blocks: Vec<u32> = snap
                    .pending_reservations
                    .iter()
                    .copied()
                    .filter(|block| !snap.blocks.contains_key(block))
                    .collect();
                unpublished_identities.extend(
                    reservation_blocks
                        .iter()
                        .map(|block| (*block, reservation_version)),
                );
                let reserve_results = stream::iter(reservation_blocks.iter().copied())
                    .map(|block| async move {
                        self.backend()
                            .reserve_block(
                                blob_guid,
                                block,
                                block_size as u32,
                                reservation_version,
                                &trace_id,
                            )
                            .await
                    })
                    .buffer_unordered(RESERVATION_CONCURRENCY)
                    .collect::<Vec<_>>()
                    .await;
                if let Some(error) = reserve_results.into_iter().find_map(Result::err) {
                    self.cleanup_unpublished_blob(blob_guid, unpublished_identities, &trace_id)
                        .await;
                    return Err(error);
                }
                let layout_version = if reservation_blocks.is_empty() {
                    1
                } else {
                    reservation_version
                };
                new_committed_reservations.extend(
                    reservation_blocks
                        .iter()
                        .map(|block| (*block, reservation_version)),
                );
                let layout = build_final_layout(blob_guid, layout_version, 0);
                let publish_bytes = match wrap_for_publish(promoted_record.as_ref(), &layout) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        self.cleanup_unpublished_blob(blob_guid, unpublished_identities, &trace_id)
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
                        // the row present, the stored bytes match exactly and
                        // the publish is idempotently complete. Otherwise
                        // another creator won the name; retry as an override
                        // against the winner.
                        match self.backend().get_inode(&publish_key, &trace_id).await {
                            Ok(cur) => {
                                let cur_bytes: Bytes =
                                    match to_bytes_in::<_, rkyv::rancor::Error>(&cur, Vec::new()) {
                                        Ok(b) => b.into(),
                                        Err(e) => {
                                            self.cleanup_unpublished_blob(
                                                blob_guid,
                                                unpublished_identities,
                                                &trace_id,
                                            )
                                            .await;
                                            return Err(FsError::from(e));
                                        }
                                    };
                                if cur_bytes == publish_bytes {
                                    for (block, state) in snap.blocks.iter() {
                                        if matches!(state, BlockState::Rewrite(_)) {
                                            committed_write_versions.insert(*block, 1);
                                        }
                                    }
                                    snap.armed = false;
                                    break (layout, 0);
                                }
                                self.cleanup_unpublished_blob(
                                    blob_guid,
                                    unpublished_identities,
                                    &trace_id,
                                )
                                .await;
                                return Err(FsError::CasConflict);
                            }
                            Err(FsError::NotFound) => {
                                self.cleanup_unpublished_blob(
                                    blob_guid,
                                    unpublished_identities,
                                    &trace_id,
                                )
                                .await;
                                return Err(FsError::CasConflict);
                            }
                            Err(e) => {
                                self.cleanup_unpublished_blob(
                                    blob_guid,
                                    unpublished_identities,
                                    &trace_id,
                                )
                                .await;
                                return Err(e);
                            }
                        }
                    }
                    Err(e) => return Err(e),
                }
            };

            // Overwrite/append path against a committed base.
            let committed_size = base.size().unwrap_or(0);
            let committed_bc = committed_size.div_ceil(bsz_u64) as u32;

            // Every dirty block writes at one freshly burned generation:
            // there is no version-1 append territory and no per-block map.
            // Appends, rewrites, punches, trims, and reservations all land
            // at `version`; nothing ever mutates an identity at or below
            // the ceiling. A write over a committed fallocate claim takes
            // a fresh identity too; the old claim is reclaimed by the
            // touched-block sweep (a transient double-claim, bounded by
            // the reclamation grace).
            let version = base.next_burn_version();

            // Classify fallocate claims against committed state with one
            // bounded listing probe per contiguous span: an existing
            // committed-visible Data or Reserved entry is a no-op, while
            // holes (tombstoned or sparse) and beyond-EOF blocks receive a
            // new burned claim.
            let mut reservation_blocks = std::collections::BTreeSet::new();
            let mut unresolved = Vec::new();
            for b in snap.pending_reservations.iter().copied() {
                if snap.blocks.contains_key(&b) {
                    continue;
                }
                if committed_reservations.contains_key(&b) {
                    continue;
                }
                if b >= committed_bc {
                    reservation_blocks.insert(b);
                } else {
                    unresolved.push(b);
                }
            }
            if !unresolved.is_empty() {
                let mut spans: Vec<(u32, u32)> = Vec::new();
                for b in unresolved.iter().copied() {
                    match spans.last_mut() {
                        Some((_, end)) if end.checked_add(1) == Some(b) => *end = b,
                        _ => spans.push((b, b)),
                    }
                }
                let mut covered = std::collections::BTreeSet::new();
                for (first, last) in spans {
                    let count = last
                        .checked_sub(first)
                        .and_then(|n| n.checked_add(1))
                        .ok_or_else(|| {
                            FsError::Internal("fallocate probe range overflow".to_string())
                        })?;
                    let entries = self
                        .backend()
                        .list_blob_blocks(blob_guid, first, count, &trace_id)
                        .await?;
                    // Newest committed-visible entry per block decides:
                    // data or an existing claim needs no new claim; a
                    // tombstone is a hole and does.
                    let mut newest: std::collections::BTreeMap<u32, (u64, bool)> =
                        std::collections::BTreeMap::new();
                    for entry in entries {
                        if entry.version > base.blob_version {
                            continue;
                        }
                        let slot = newest.entry(entry.block_number).or_insert((0, false));
                        if entry.version > slot.0 {
                            *slot = (entry.version, !entry.is_tombstone);
                        }
                    }
                    covered.extend(
                        newest
                            .iter()
                            .filter_map(|(block, (_, has_content))| has_content.then_some(*block)),
                    );
                }
                for b in unresolved {
                    if !covered.contains(&b) {
                        reservation_blocks.insert(b);
                    }
                }
            }

            // Step 2: prepare CAS. This durably burns `version` before any
            // data I/O: the allocator never decreases and a burned version
            // is never handed out again, so every data key this attempt
            // writes is written by this attempt alone, ever. That is the
            // whole abort story. An interrupted attempt needs no record
            // and no recovery: its fragments sit above the ceiling until
            // some later commit passes them, after which they read as
            // ordinary content, the POSIX unspecified-state outcome of a
            // failed overwrite. `blob_version` stays at the reader-visible
            // ceiling until commit.
            let mut prepare = base.clone();
            prepare.set_next_version(version + 1);
            {
                let old_bytes = wrap_for_publish(promoted_record.as_ref(), &base)?;
                let new_bytes = wrap_for_publish(promoted_record.as_ref(), &prepare)?;
                if let Err(error) = self
                    .backend()
                    .put_inode_cas(&publish_key, new_bytes.clone(), old_bytes, &trace_id)
                    .await
                {
                    let landed = match promoted_inode_id {
                        Some(id) => self
                            .backend()
                            .get_inode_record(id, &trace_id)
                            .await
                            .ok()
                            .and_then(|record| wrap_for_publish(Some(&record), &record.layout).ok())
                            .is_some_and(|current| current == new_bytes),
                        None => self
                            .backend()
                            .get_inode(&publish_key, &trace_id)
                            .await
                            .ok()
                            .and_then(|layout| wrap_for_publish(None, &layout).ok())
                            .is_some_and(|current| current == new_bytes),
                    };
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

            // Step 3: trim range. Blocks logically destroyed by a shrink
            // must read zeros even while their superseded generations await
            // the sweep; with no map, the zeros must be tombstone
            // generations. One bounded listing selects only blocks that
            // actually hold committed-visible content, so a sparse trim
            // writes nothing.
            let trim_lo =
                std::cmp::min(new_num_blocks, eof_low_watermark.unwrap_or(new_num_blocks));
            let trim_hi = committed_bc.max(trim_upper.unwrap_or(0));
            let trim_spans = trim_victim_spans(trim_lo, trim_hi);
            let mut tombstone_blocks: std::collections::BTreeSet<u32> =
                std::collections::BTreeSet::new();
            if trim_span_block_count(&trim_spans) > 0 {
                let (first, last) = (trim_spans[0].0, trim_spans[trim_spans.len() - 1].1);
                let count = last
                    .checked_sub(first)
                    .and_then(|width| width.checked_add(1))
                    .unwrap_or(0);
                let entries = self
                    .backend()
                    .list_blob_blocks(blob_guid, first, count, &trace_id)
                    .await?;
                let mut newest: std::collections::BTreeMap<u32, (u64, bool)> =
                    std::collections::BTreeMap::new();
                for entry in entries {
                    if entry.version > base.blob_version {
                        continue;
                    }
                    if !block_in_trim_spans(entry.block_number, &trim_spans) {
                        continue;
                    }
                    let slot = newest.entry(entry.block_number).or_insert((0, false));
                    if entry.version > slot.0 {
                        *slot = (entry.version, !entry.is_tombstone);
                    }
                }
                tombstone_blocks.extend(newest.iter().filter_map(|(b, (_, has_content))| {
                    (*has_content
                        && !snap.blocks.contains_key(b)
                        && !reservation_blocks.contains(b))
                    .then_some(*b)
                }));
            }
            // Punch holes are tombstones at `version` too.
            for (b, st) in snap.blocks.iter() {
                if matches!(st, BlockState::Delete) {
                    tombstone_blocks.insert(*b);
                }
            }

            // Everything this attempt lands at `version`: the commit's
            // touched-block sweep input.
            let touched_at_v: std::collections::BTreeSet<u32> = snap
                .blocks
                .iter()
                .filter_map(|(b, st)| matches!(st, BlockState::Rewrite(_)).then_some(*b))
                .chain(tombstone_blocks.iter().copied())
                .chain(reservation_blocks.iter().copied())
                .collect();

            // Step 4: write every dirty block. Burned generations are
            // padded to a full block_size (constant EC shard size).
            // Bodies are independent write-once keys: pipeline them, as
            // are tombstones.
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
                        let body = if bytes.len() < block_size {
                            let mut buf = BytesMut::with_capacity(block_size);
                            buf.extend_from_slice(&bytes);
                            buf.resize(block_size, 0);
                            buf.freeze()
                        } else {
                            bytes.clone()
                        };
                        self.backend()
                            .write_block(blob_guid, b, body, version, trace_id)
                            .await
                    }
                })
                .buffer_unordered(BODY_WRITE_CONCURRENCY)
                .try_collect::<Vec<_>>()
                .await?;
            stream::iter(tombstone_blocks.iter().copied())
                .map(|b| {
                    let trace_id = &trace_id;
                    async move {
                        self.backend()
                            .write_tombstone_block(blob_guid, b, version, trace_id)
                            .await
                    }
                })
                .buffer_unordered(BODY_WRITE_CONCURRENCY)
                .try_collect::<Vec<_>>()
                .await?;

            // Reservation quorum is a precondition of the metadata commit.
            // Publishing first would let fallocate report success after
            // ENOSPC and advertise unallocated space.
            let reservation_blocks_vec: Vec<u32> = reservation_blocks.iter().copied().collect();
            for batch in reservation_blocks_vec.chunks(RESERVATION_CONCURRENCY) {
                let batch = batch.to_vec();
                let reserve_result = stream::iter(batch)
                    .map(|block| {
                        self.backend().reserve_block(
                            blob_guid,
                            block,
                            block_size as u32,
                            version,
                            &trace_id,
                        )
                    })
                    .buffer_unordered(RESERVATION_CONCURRENCY)
                    .try_collect::<Vec<_>>()
                    .await;
                reserve_result?;
            }

            // Step 5: commit CAS. The ceiling advances to `version` in one
            // byte-equality CAS against the prepared record.
            let layout = build_final_layout(blob_guid, version, version + 1);
            let new_bytes = wrap_for_publish(promoted_record.as_ref(), &layout)?;
            let mut commit_guard = prepare.clone();
            loop {
                let old_bytes = wrap_for_publish(promoted_record.as_ref(), &commit_guard)?;
                let commit_result = self
                    .backend()
                    .put_inode_cas(&publish_key, new_bytes.clone(), old_bytes, &trace_id)
                    .await;
                let Err(error) = commit_result else { break };
                let landed = match promoted_inode_id {
                    Some(id) => self
                        .backend()
                        .get_inode_record(id, &trace_id)
                        .await
                        .ok()
                        .and_then(|record| wrap_for_publish(Some(&record), &record.layout).ok())
                        .is_some_and(|current| current == new_bytes),
                    None => self
                        .backend()
                        .get_inode(&publish_key, &trace_id)
                        .await
                        .ok()
                        .and_then(|layout| wrap_for_publish(None, &layout).ok())
                        .is_some_and(|current| current == new_bytes),
                };
                if landed {
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
            // Touched-block reclamation: for every block this commit gave a
            // new identity, delete the generations below that identity on
            // every placement node (superseded generations plus any orphan
            // fragments of interrupted attempts), after the grace.
            sweep_below = touched_at_v.iter().map(|b| (*b, version)).collect();
            new_committed_reservations
                .extend(reservation_blocks.iter().map(|block| (*block, version)));
            for (b, st) in snap.blocks.iter() {
                if matches!(st, BlockState::Rewrite(_)) {
                    committed_write_versions.insert(*b, version);
                }
            }
            snap.armed = false;
            break (layout, committed_size);
        };

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
                // A block this commit re-identified supersedes any claim the
                // handle tracked for it; the sweep reclaims the old key.
                for (block, _) in sweep_below.iter() {
                    wb.committed_reservations.remove(block);
                }
                for (block, reserved_version) in new_committed_reservations.drain(..) {
                    wb.committed_reservations.insert(block, reserved_version);
                }
            }
        }
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
        // from memory without a cross-instance coherency round-trip. The
        // single-writer-per-inode lock makes the local layout
        // authoritative for this window. The promoted-hardlink block
        // below re-sets `entry.layout` from the resolved record, so skip
        // it here when this inode is promoted.
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
        // state: rewrites land at their natural offsets, deletes
        // punch holes, and the file-level authoritative_blob_v in
        // the cache header advances to match. Under the single-
        // writer-per-inode policy this is safe to do without any
        // additional locking; no other instance has a write in
        // flight on this inode at this moment.
        //
        // Best-effort: a sync failure (e.g. ENOSPC) is logged and
        // does not affect flush durability. The next read on an
        // affected block cold-fetches from BSS and re-populates.
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
                // Override path: mirror the cache SYNCHRONOUSLY before the
                // flush returns. An override can have a pre-existing cache
                // file that other readers already trust: a passthrough
                // backing fd reading raw cache bytes (which never consults
                // our metadata), or a concurrent reader on a stale handle.
                // An async write would leave those bytes stale until (or
                // unless) the mirror lands, so the rewritten bytes must be
                // correct at flush time. The file commit epoch fences any
                // older queued mirror job, while each rewritten block keeps
                // its exact generation. fdatasync is still dropped, so this
                // remains page-cache-cheap.
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
                    // An override mirror cannot be best-effort: a partial
                    // failure (header/floor advanced, block write failed)
                    // can leave the superseded block as a valid
                    // populated+checksum hit. Drop the whole cache file so
                    // every block cold-fetches the authoritative bytes from
                    // BSS before this flush reports success.
                    tracing::warn!(
                        %final_blob_guid,
                        error = %e,
                        "disk cache override mirror failed; dropping cache file"
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

        // Reclaim what this commit superseded: for every touched block,
        // generations below its new committed identity (including orphan
        // fragments interrupted attempts left on these blocks), after the
        // reclamation grace. Best-effort and off the flush path; a crash
        // before the sweep leaks garbage until the block is rewritten or
        // the file unlinked.
        if let Ok(final_blob_guid) = final_layout.blob_guid() {
            self.enqueue_superseded_sweep(
                final_blob_guid,
                final_layout.blob_version,
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

        // Phase 2: lazy-load the partial blocks outside the guard.
        let trace_id = TraceId::new();
        let mut loaded: std::collections::BTreeMap<u32, Bytes> = std::collections::BTreeMap::new();
        let committed_ceiling = committed_layout.as_ref().map_or(0, |l| l.blob_version);
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
            wb.pending_reservations.remove(&b);
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
        let mut loaded: std::collections::BTreeMap<u32, Bytes> = std::collections::BTreeMap::new();
        if punch_hole {
            let committed_ceiling = committed_layout.as_ref().map_or(0, |l| l.blob_version);
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
                             loaded: &std::collections::BTreeMap<u32, Bytes>,
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
                wb.pending_reservations.remove(&b);
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
                    wb.pending_reservations.remove(&b);
                }
            }
            wb.dirty = true;
            drop(handle);
            drop(operation_guard);
            return self.flush_write_buffer(fh).await;
        }

        // mode == 0 or KEEP_SIZE: reservation-only path. Publish before the
        // syscall returns so allocation failure is reported by fallocate and
        // the claim survives a process crash.
        let first_block = (offset / bsz_u64) as u32;
        let last_block_excl = end.div_ceil(bsz_u64) as u32;
        for b in first_block..last_block_excl {
            // Don't shadow buffered Rewrite or committed Data with a
            // reservation entry; the reservation is only for blocks
            // that don't already have content.
            if matches!(wb.blocks.get(&b), Some(BlockState::Rewrite(_))) {
                continue;
            }
            wb.pending_reservations.insert(b);
        }

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
    /// `[offset, file_size)` as data or hole and returns the offset of the
    /// first match. EOF source: a write handle uses the in-memory
    /// `WriteBuffer::file_size`; a read-only handle uses the inode-published
    /// `layout.size()` (the override flush publishes the authoritative size
    /// into the inode via `put_inode_cas`, so no separate BSS geometry probe
    /// is needed). Per-block classification merges buffer state with a single
    /// bounded `ListBlobBlocks` probe (present => data, absent => hole).
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
        let (
            file_size,
            block_size,
            probe_blob_guid,
            blocks,
            pending_reservations,
            eof_low_watermark,
        ) = {
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
                    wb.pending_reservations.clone(),
                    wb.eof_low_watermark,
                )
            } else {
                (
                    layout_size,
                    layout_block_size,
                    layout_blob_guid,
                    std::collections::BTreeMap::new(),
                    std::collections::BTreeSet::new(),
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
        // `None` -> not buffered, fall through to the BSS probe.
        let buffered_kind = |b: u32| -> Option<bool> {
            match blocks.get(&b) {
                Some(BlockState::Rewrite(_)) => Some(true),
                Some(BlockState::Delete) => Some(false),
                None => {
                    if pending_reservations.contains(&b) {
                        return Some(true);
                    }
                    if eof_low_watermark.is_some_and(|low| b >= low) {
                        return Some(false);
                    }
                    None
                }
            }
        };

        // Committed classification: one ListBlobBlocks call covers the
        // whole walk range, and per block the newest entry at or below
        // the ceiling decides. Data and Reserved count as data per the
        // Linux SEEK_DATA convention; a punch tombstone or no entry is a
        // hole.
        let trace_id = TraceId::new();
        let ceiling = committed_layout.as_ref().map_or(0, |l| l.blob_version);
        let committed_data: std::collections::BTreeSet<u32> = match probe_blob_guid {
            Some(guid) => {
                let count = last_block_excl.saturating_sub(first_block);
                if count == 0 {
                    std::collections::BTreeSet::new()
                } else {
                    let entries = self
                        .backend()
                        .list_blob_blocks(guid, first_block, count, &trace_id)
                        .await?;
                    let mut newest: std::collections::BTreeMap<u32, (u64, bool)> =
                        std::collections::BTreeMap::new();
                    for e in entries {
                        if e.version > ceiling {
                            continue;
                        }
                        let slot = newest.entry(e.block_number).or_insert((0, false));
                        if e.version > slot.0 {
                            *slot = (e.version, !e.is_tombstone);
                        }
                    }
                    newest
                        .into_iter()
                        .filter_map(|(b, (_, has_content))| has_content.then_some(b))
                        .collect()
                }
            }
            None => std::collections::BTreeSet::new(),
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
                    let block_start = (last as u64) * (block_size as u64);
                    let committed_content_len = if block_start < committed_size {
                        std::cmp::min(block_size as u64, committed_size - block_start) as usize
                    } else {
                        0
                    };
                    let committed_ceiling = committed_layout.as_ref().map_or(0, |l| l.blob_version);
                    self.lazy_load_block_for_flush(
                        existing_blob_guid,
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

    /// After a lost inode CAS, fetch the stored row and return the current
    /// layout iff it diverged from `expected` only by a posix republish
    /// (see `posix_only_moved`); a promoted record must also keep its
    /// nlink and orphan_since. Any real conflict returns None.
    pub(crate) async fn refetch_posix_moved_base(
        &self,
        publish_key: &str,
        promoted_record: Option<&InodeRecord>,
        promoted_inode_id: Option<Uuid>,
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
    blocks: std::collections::BTreeMap<u32, BlockState>,
    pending_reservations: std::collections::BTreeSet<u32>,
    armed: bool,
}

impl Drop for FlushSnapshotGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.vfs.restore_flush_snapshot(
                self.fh_id,
                std::mem::take(&mut self.blocks),
                std::mem::take(&mut self.pending_reservations),
            );
        }
    }
}

fn trim_victim_spans(trim_lo: u32, trim_hi: u32) -> Vec<(u32, u32)> {
    if trim_lo < trim_hi {
        vec![(trim_lo, trim_hi - 1)]
    } else {
        Vec::new()
    }
}

fn trim_span_block_count(spans: &[(u32, u32)]) -> u64 {
    spans.iter().fold(0_u64, |count, (start, end)| {
        count.saturating_add(u64::from(*end) - u64::from(*start) + 1)
    })
}

fn block_in_trim_spans(block: u32, spans: &[(u32, u32)]) -> bool {
    let index = spans.partition_point(|(_, end)| *end < block);
    spans
        .get(index)
        .is_some_and(|(start, end)| *start <= block && block <= *end)
}

/// True when `current` differs from `expected` only in posix attributes:
/// the async SetPosix worker (or a chmod/utimensat) republished the row
/// between this flush's base snapshot and its CAS. Metadata updates clone
/// the fetched layout and carry the versioning fields forward
/// unchanged, so a data flush can rebase over them; any other
/// divergence is a foreign writer and stays a hard conflict.
fn posix_only_moved(expected: &ObjectLayout, current: &ObjectLayout) -> bool {
    if !matches!(expected.state, ObjectState::Normal(_))
        || !matches!(current.state, ObjectState::Normal(_))
    {
        return false;
    }
    // `set_fs_posix` re-normalizes the fs_ext box, so a republish that
    // only touched posix collapses back to the expected shape (including
    // the ext disappearing entirely when nothing else is in it).
    let mut normalized = current.clone();
    normalized.set_fs_posix(expected.fs_posix());
    // rkyv encoding is deterministic for these types (the CAS guard itself
    // relies on this), so byte equality is exact structural equality.
    let expected_bytes = to_bytes_in::<_, rkyv::rancor::Error>(expected, Vec::new());
    let normalized_bytes = to_bytes_in::<_, rkyv::rancor::Error>(&normalized, Vec::new());
    match (expected_bytes, normalized_bytes) {
        (Ok(expected_bytes), Ok(normalized_bytes)) => expected_bytes == normalized_bytes,
        _ => false,
    }
}

#[cfg(test)]
mod posix_only_moved_tests {
    use super::*;
    use data_types::DataBlobGuid;
    use data_types::object_layout::{ObjectCoreMetaData, ObjectMetaData, PosixAttrs};

    fn layout_with(mtime_ns: u64, blob_version: u64) -> ObjectLayout {
        ObjectLayout {
            timestamp: 1,
            version_id: uuid::Uuid::nil(),
            block_size: DEFAULT_BLOCK_SIZE,
            blob_version,
            fs_ext: ObjectLayout::fs_ext_from(Some(PosixAttrs {
                mode: 0o100644,
                uid: 1000,
                gid: 1000,
                mtime_ns,
                ctime_ns: mtime_ns,
            })),
            state: ObjectState::Normal(ObjectMetaData {
                blob_guid: DataBlobGuid {
                    blob_id: uuid::Uuid::nil(),
                    volume_id: 1,
                },
                core_meta_data: ObjectCoreMetaData {
                    size: 2,
                    etag: "etag".to_string(),
                    headers: vec![],
                    checksum: None,
                },
            }),
        }
    }

    #[test]
    fn posix_republish_is_benign() {
        let base = layout_with(100, 1);
        let moved = layout_with(200, 1);
        assert!(posix_only_moved(&base, &moved), "mtime-only move rebases");
        assert!(posix_only_moved(&base, &base), "identical rows rebase");
    }

    #[test]
    fn structural_divergence_stays_a_conflict() {
        let base = layout_with(100, 1);
        let mut advanced = layout_with(100, 2);
        assert!(
            !posix_only_moved(&base, &advanced),
            "blob_version change is a real writer"
        );
        advanced = layout_with(100, 1);
        if let ObjectState::Normal(meta) = &mut advanced.state {
            meta.core_meta_data.size = 3;
        }
        assert!(
            !posix_only_moved(&base, &advanced),
            "size change is a real writer"
        );
    }
}

#[cfg(test)]
mod trim_span_tests {
    use super::*;

    #[test]
    fn trim_spans_are_bounded_and_queryable() {
        assert_eq!(trim_victim_spans(4, 4), Vec::<(u32, u32)>::new());
        let spans = trim_victim_spans(2, 4);
        assert_eq!(spans, vec![(2, 3)]);
        assert_eq!(trim_span_block_count(&spans), 2);
        assert!(block_in_trim_spans(3, &spans));
        assert!(!block_in_trim_spans(4, &spans));
    }
}
