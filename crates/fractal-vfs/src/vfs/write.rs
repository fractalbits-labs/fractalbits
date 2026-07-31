//! Write buffering and the flush/commit path, truncate, fallocate, lseek.

use bytes::{Bytes, BytesMut};
use data_types::TraceId;
use data_types::object_layout::{InodeRecord, ObjectLayout, ObjectState};
use data_types::object_layout::{ObjectCoreMetaData, ObjectMetaData};
use fractal_fuse::{FileHandleId, InodeId};
use rkyv::api::high::to_bytes_in;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cache::DirEntryKind;
use crate::config::WritebackMode;
use crate::disk_cache::{MIRROR_BYTE_BUDGET, MirrorJob};
use crate::error::FsError;
use crate::vfs::write_buffer::BlockState;
use crate::vfs::write_buffer::WriteBuffer;
use crate::vfs::{DEFAULT_BLOCK_SIZE, MAX_INMEM_FILE_SIZE, VfsAttr, VfsCore, parent_prefix_of};

impl VfsCore {
    /// Load one block's committed bytes from BSS for an RMW / dirty read /
    /// flush tail-zero. Returns zeros (length `fallback_content_len`) for a
    /// brand-new file, a hole (`committed_content_len == 0`), or a missing
    /// block (`BlockNotFound` / `NotFound`); propagates other errors.
    pub(crate) async fn lazy_load_block_for_flush(
        &self,
        existing_blob_guid: Option<data_types::DataBlobGuid>,
        committed_blob_version: u64,
        block_num: u32,
        committed_content_len: usize,
        fallback_content_len: usize,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        let Some(guid) = existing_blob_guid else {
            return Ok(Bytes::from(vec![0u8; fallback_content_len]));
        };
        if committed_content_len == 0 {
            return Ok(Bytes::from(vec![0u8; fallback_content_len]));
        }
        match self
            .backend()
            .read_block(
                guid,
                committed_blob_version,
                block_num,
                committed_content_len,
                trace_id,
            )
            .await
        {
            Ok((data, _)) => Ok(data),
            Err(FsError::DataVg(volume_group_proxy::DataVgError::BlockNotFound)) => {
                Ok(Bytes::from(vec![0u8; fallback_content_len]))
            }
            Err(FsError::Rpc(rpc_client_common::RpcError::NotFound)) => {
                Ok(Bytes::from(vec![0u8; fallback_content_len]))
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
        committed_blob_version: u64,
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
                            committed_blob_version,
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
            )
        };

        // A name unlinked while its fd stayed open must not be resurrected
        // in NSS, unless the inode was promoted to a hardlink, in which
        // case its data lives in the shared `@hardlink/<id>` InodeRecord
        // blob and the other names still reference it, so the write must
        // still flush (routed to the record below, not this s3_key, whose
        // NSS row holds only an Indirect redirect).
        let (name_removed, mut promoted_inode_id) = self
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
        let mut promoted_record_key = promoted_inode_id.map(InodeRecord::key_for);
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

        const MAX_CAS_RETRIES: u32 = 5;
        let mut attempt: u32 = 0;
        let (mut final_layout, final_committed_size) = loop {
            attempt += 1;

            let (blob_guid, base_version, committed_size, expected_old, is_override) =
                match base_layout
                    .as_ref()
                    .and_then(|l| l.blob_guid().ok().map(|g| (g, l)))
                {
                    Some((g, l)) => {
                        let bytes: Bytes =
                            match to_bytes_in::<_, rkyv::rancor::Error>(l, Vec::new()) {
                                Ok(b) => b.into(),
                                Err(e) => return Err(FsError::from(e)),
                            };
                        (g, l.blob_version, l.size().unwrap_or(0), bytes, true)
                    }
                    None => (self.backend().create_blob_guid(), 0, 0, Bytes::new(), false),
                };
            // Override versions start at 2 so a committed legacy record at
            // blob_version 0/1 (whose BSS blocks sit at v1) can't collide
            // with a same-version idempotency check. A brand-new file's
            // first flush is v1 (unpadded, read at exact length).
            let new_version = if is_override {
                (base_version + 1).max(2)
            } else {
                1
            };
            let pad_blocks = is_override;

            // Write only the Rewrite blocks at the new version (zero-padded
            // to block_size on override so the EC shard size is constant).
            let mut flush_err: Option<FsError> = None;
            for (b, st) in snap.blocks.iter() {
                let BlockState::Rewrite(bytes) = st else {
                    continue;
                };
                let body = if pad_blocks && bytes.len() < block_size {
                    let mut buf = BytesMut::with_capacity(block_size);
                    buf.extend_from_slice(bytes);
                    buf.resize(block_size, 0);
                    buf.freeze()
                } else {
                    bytes.clone()
                };
                if let Err(e) = self
                    .backend()
                    .write_block(blob_guid, *b, body, new_version, &trace_id)
                    .await
                {
                    flush_err = Some(e);
                    break;
                }
            }
            if let Some(e) = flush_err {
                // The guard restores the taken blocks on this return so a
                // later flush can retry (CasConflict never reaches here).
                return Err(e);
            }

            // Build + serialize the new layout at the bumped version.
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            // On the promoted (hardlink) path, carry the freshly-fetched
            // record's posix forward, NOT the local snapshot taken before
            // this flush: another alias may have chmod/chown'd the shared
            // record between the snapshot and this CAS attempt, and a data
            // write changes only size/blob_version (never posix), so the
            // snapshot has nothing of ours to merge. Using it would undo a
            // concurrent metadata change. The non-promoted path is
            // single-writer-per-inode, so the local snapshot is correct.
            let effective_posix = if promoted_record.is_some() {
                base_layout
                    .as_ref()
                    .map(crate::inode::layout_posix)
                    .unwrap_or_else(|| self.inodes.get(ino).map(|e| e.posix).unwrap_or_default())
            } else {
                self.inodes.get(ino).map(|e| e.posix).unwrap_or_default()
            };
            let layout = ObjectLayout {
                version_id: ObjectLayout::gen_version_id(),
                block_size: DEFAULT_BLOCK_SIZE,
                timestamp,
                blob_version: new_version,
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
            // Choose the publish target. A promoted inode republishes its
            // layout inside the shared InodeRecord at the `@hardlink/<id>`
            // key, CAS'd on the current record bytes so a concurrent writer
            // on another hardlink name (a different FUSE inode with its own
            // write lock) loses the race and retries instead of clobbering.
            // A normal file publishes the bare layout at its own s3_key.
            let (publish_key, publish_bytes, publish_expected_old) = match &promoted_record {
                Some(rec) => {
                    let new_record = InodeRecord {
                        layout: layout.clone(),
                        nlink: rec.nlink,
                        orphan_since: rec.orphan_since,
                    };
                    let new_bytes: Bytes =
                        match to_bytes_in::<_, rkyv::rancor::Error>(&new_record, Vec::new()) {
                            Ok(b) => b.into(),
                            Err(e) => return Err(FsError::from(e)),
                        };
                    // Guard on the record as fetched (re-serialized); rkyv is
                    // deterministic for these types.
                    let old_bytes: Bytes =
                        match to_bytes_in::<_, rkyv::rancor::Error>(rec, Vec::new()) {
                            Ok(b) => b.into(),
                            Err(e) => return Err(FsError::from(e)),
                        };
                    (
                        promoted_record_key
                            .clone()
                            .expect("promoted_record implies a record key"),
                        new_bytes,
                        old_bytes,
                    )
                }
                None => {
                    let layout_bytes: Bytes =
                        match to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new()) {
                            Ok(b) => b.into(),
                            Err(e) => return Err(FsError::from(e)),
                        };
                    (s3_key.clone(), layout_bytes, expected_old)
                }
            };

            let first_publish = promoted_record.is_none() && publish_expected_old.is_empty();

            // CAS-publish: only lands if NSS still holds `publish_expected_old`.
            match self
                .backend()
                .put_inode_cas(
                    &publish_key,
                    publish_bytes.clone(),
                    publish_expected_old,
                    &trace_id,
                )
                .await
            {
                Ok(_prev) => {
                    // EOF-trim: delete blocks in the union of the shrink
                    // range and the committed range, excluding blocks a
                    // Rewrite just wrote. Deleted at the bumped version so
                    // the guard drops the now-orphaned blocks.
                    let committed_bc = committed_size.div_ceil(bsz_u64) as u32;
                    let lower =
                        std::cmp::min(new_num_blocks, eof_low_watermark.unwrap_or(new_num_blocks));
                    let upper = std::cmp::max(committed_bc, trim_upper.unwrap_or(0));
                    // Blind-delete the trim range. Deleting a hole is now an
                    // idempotent no-op at the DataVgProxy layer (a delete that
                    // hits RpcError::NotFound is treated as success, not a
                    // circuit-breaker failure), so sparse holes in [lower, upper)
                    // no longer trip the per-node breaker.
                    for b in lower..upper {
                        if matches!(snap.blocks.get(&b), Some(BlockState::Rewrite(_))) {
                            continue;
                        }
                        let _ = self
                            .backend()
                            .delete_block(blob_guid, b, new_version, &trace_id)
                            .await;
                    }
                    // Replay PUNCH_HOLE intents.
                    for (b, st) in snap.blocks.iter() {
                        if matches!(st, BlockState::Delete) {
                            let _ = self
                                .backend()
                                .delete_block(blob_guid, *b, new_version, &trace_id)
                                .await;
                        }
                    }
                    // Publish landed: disarm the restore guard so the taken
                    // snapshot is discarded instead of re-marking the handle
                    // dirty.
                    snap.armed = false;
                    break (layout, committed_size);
                }
                Err(FsError::CasConflict) if first_publish => {
                    // A first publish is a create, not an overwrite. If the
                    // CAS reply was lost and an internal retry saw the row
                    // present, the stored bytes match exactly and the publish
                    // is idempotently complete. Otherwise another creator won
                    // the name and retrying as an override would clobber it.
                    match self.backend().get_inode(&publish_key, &trace_id).await {
                        Ok(cur) => {
                            let cur_bytes: Bytes =
                                match to_bytes_in::<_, rkyv::rancor::Error>(&cur, Vec::new()) {
                                    Ok(b) => b.into(),
                                    Err(e) => return Err(FsError::from(e)),
                                };
                            if cur_bytes == publish_bytes {
                                snap.armed = false;
                                break (layout, committed_size);
                            }
                            return Err(FsError::CasConflict);
                        }
                        Err(FsError::NotFound) => return Err(FsError::CasConflict),
                        Err(e) => return Err(e),
                    }
                }
                Err(FsError::CasConflict) => {
                    if attempt >= MAX_CAS_RETRIES {
                        tracing::warn!(
                            key = %publish_key,
                            "flush_write_buffer: CAS still conflicting after retries"
                        );
                        // The guard restores blocks so a later flush retries.
                        return Err(FsError::CasConflict);
                    }
                    // Re-fetch the base for the next attempt: the shared
                    // record for a promoted inode, else the s3_key layout.
                    if let Some(id) = promoted_inode_id {
                        match self.backend().get_inode_record(id, &trace_id).await {
                            Ok(rec) => {
                                base_layout = Some(rec.layout.clone());
                                promoted_record = Some(rec);
                            }
                            Err(e) => return Err(e),
                        }
                    } else {
                        match self.backend().get_inode(&s3_key, &trace_id).await {
                            Ok(cur) => {
                                if let ObjectState::Indirect(redirect) = &cur.state {
                                    // The file was promoted to a hardlink
                                    // concurrently (another client/instance)
                                    // since we seeded from a cached normal
                                    // layout. Switch to the record path so we
                                    // publish into the shared record instead
                                    // of clobbering the redirect with a normal
                                    // layout.
                                    let id = redirect.inode_id;
                                    match self.backend().get_inode_record(id, &trace_id).await {
                                        Ok(rec) => {
                                            base_layout = Some(rec.layout.clone());
                                            promoted_record = Some(rec);
                                            promoted_inode_id = Some(id);
                                            promoted_record_key = Some(InodeRecord::key_for(id));
                                        }
                                        Err(e) => return Err(e),
                                    }
                                } else {
                                    base_layout = Some(cur);
                                }
                            }
                            Err(FsError::NotFound) => base_layout = None,
                            Err(e) => return Err(e),
                        }
                    }
                    continue;
                }
                Err(e) => return Err(e),
            }
        };

        // Update file handle: install the new layout (next CAS guard),
        // clear dirty/size_changed, reset shrink state, and point the buffer
        // at the published blob_guid for subsequent lazy loads.
        if let Some(mut handle) = self.file_handles.get_mut(&fh_id) {
            handle.layout = Some(final_layout.clone());
            if let Some(ref mut wb) = handle.write_buf {
                wb.dirty = false;
                wb.size_changed = false;
                wb.eof_low_watermark = None;
                wb.trim_upper = None;
                wb.existing_blob_guid = final_layout.blob_guid().ok();
            }
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

        // If this inode is a promoted hardlink (including one discovered
        // mid-flush when a CAS conflict revealed an Indirect redirect),
        // persist the record identity + resolved layout/posix onto the
        // inode entry. Otherwise a later setattr would see inode_id == None,
        // take the non-hardlink path, and overwrite the name's Indirect
        // redirect with a normal layout.
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
                // correct at flush time. sync_after_flush also advances the
                // version floor, which fences any still-queued OLDER create
                // job for this blob. fdatasync is still dropped, so this is
                // page-cache-cheap; overrides are not the create-storm path.
                if let Err(e) = dc
                    .sync_after_flush(final_blob_guid, blob_version, &rewrites, &deletes)
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
        let end = offset + data.len() as u64;

        // Phase 1: snapshot block_size, committed geometry, and which
        // partially-touched blocks need a lazy read-modify-write load.
        // Releases the guard before any await.
        let (
            block_size,
            existing_blob_guid,
            committed_size,
            committed_blob_version,
            blocks_to_load,
        ) = {
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
            let committed_blob_version =
                handle.layout.as_ref().map(|l| l.blob_version).unwrap_or(0);
            let wb = handle
                .write_buf
                .get_or_insert_with(|| WriteBuffer::new(layout_blob_guid, committed_size, bsize));
            let bsz_u64 = wb.block_size as u64;
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
                committed_blob_version,
                to_load,
            )
        };

        // Phase 2: lazy-load the partial blocks outside the guard.
        let trace_id = TraceId::new();
        let mut loaded: std::collections::BTreeMap<u32, Bytes> = std::collections::BTreeMap::new();
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
                    committed_blob_version,
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

        let end = offset + length;

        // Phase 1: snapshot enough state to compute the touched range
        // and decide which blocks need a lazy load for edge zeroing.
        let (block_size, existing_blob_guid, committed_size, committed_blob_version, edge_loads) = {
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
            let committed_blob_version =
                handle.layout.as_ref().map(|l| l.blob_version).unwrap_or(0);
            let wb = handle.write_buf.get_or_insert_with(|| {
                WriteBuffer::new(layout_blob_guid, committed_size, block_size)
            });
            let bsz_u64 = wb.block_size as u64;
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
                committed_blob_version,
                edge_loads,
            )
        };

        // Phase 2: lazy-load edge blocks outside the DashMap guard.
        let trace_id = TraceId::new();
        let mut loaded: std::collections::BTreeMap<u32, Bytes> = std::collections::BTreeMap::new();
        if punch_hole {
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
                        committed_blob_version,
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
            return Ok(());
        }

        // mode == 0 or KEEP_SIZE: reservation-only path. Record the
        // touched range so flush has something to publish if the user
        // did nothing else, and so SEEK_DATA / dirty-handle reads count
        // the range as data per Linux convention.
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
        Ok(())
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

        // Snapshot the bits we need without holding the guard across awaits.
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

        // BSS-side classification: one ListBlobBlocks call covers the whole
        // walk range. Reserved entries count as data (Linux SEEK_DATA
        // convention), Data is data, anything not returned is a hole.
        let trace_id = TraceId::new();
        let block_map: std::collections::BTreeSet<u32> = match probe_blob_guid {
            Some(guid) => {
                let count = last_block_excl.saturating_sub(first_block);
                if count == 0 {
                    std::collections::BTreeSet::new()
                } else {
                    let entries = self
                        .backend()
                        .list_blob_blocks(guid, first_block, count, &trace_id)
                        .await?;
                    entries.into_iter().map(|e| e.block_number).collect()
                }
            }
            None => std::collections::BTreeSet::new(),
        };

        for b in first_block..last_block_excl {
            let is_data = match buffered_kind(b) {
                Some(d) => d,
                None => block_map.contains(&b),
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
        // Phase 1: snapshot, drop intents past the new EOF, lower the
        // shrink-destroys watermark, and decide whether the surviving last
        // block of a non-block-aligned shrink needs a synthesized
        // tail-zero `Rewrite`. Releases the guard before any await.
        let (
            block_size,
            committed_size,
            existing_blob_guid,
            committed_blob_version,
            tail_zero_target,
        ) = {
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
            let committed_blob_version =
                handle.layout.as_ref().map(|l| l.blob_version).unwrap_or(0);
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
                committed_blob_version,
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
                    self.lazy_load_block_for_flush(
                        existing_blob_guid,
                        committed_blob_version,
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
