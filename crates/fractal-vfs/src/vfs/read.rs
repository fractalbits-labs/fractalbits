//! Data read paths: cached block reads, MPU stitching, vfs_read.

use bytes::{Bytes, BytesMut};
use data_types::TraceId;
use data_types::object_layout::{MpuState, ObjectLayout, ObjectState};
use fractal_fuse::FileHandleId;

use crate::error::FsError;
use crate::vfs::{TTL, VfsCore};
use std::collections::HashSet;
use std::time::Instant;
use volume_group_proxy::AtOrBeforeRead;

impl VfsCore {
    /// The exact BSS identity of one committed block, resolved from the
    /// block map: `Some((version, mapped))` to fetch, `None` for zeros with
    /// no BSS access (hole / reserved). Unmapped blocks are version 1
    /// (write-once first writes); `mapped` distinguishes "all replicas
    /// missing" semantics: a mapped generation must exist (detected data
    /// loss), an unmapped v1 miss is a legitimate sparse hole.
    /// Read a block via at-or-before selection against the layout's
    /// committed ceiling. On a fetch, populates the disk cache at the
    /// exact identity the read resolved.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_block_cached(
        &self,
        blob_guid: data_types::DataBlobGuid,
        ceiling: u64,
        block_num: u32,
        block_content_len: usize,
        block_size: usize,
        validated_sparse_blocks: &HashSet<(data_types::DataBlobGuid, u32)>,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        // A cached body at exactly the ceiling version is authoritative:
        // nothing above the ceiling is committed, so nothing can shadow
        // it. Below-ceiling cache entries cannot be validated locally
        // (a newer generation may exist elsewhere), so only the ceiling
        // identity is probed before the walk.
        if let Some(dc) = &self.disk_cache
            && let Some(cached) = dc
                .get_block_exact(blob_guid, block_num, ceiling, block_content_len)
                .await
        {
            return Ok(cached);
        }

        // Burned generations are padded to block_size; version-1 creates
        // keep their natural length. Either way the caller clamps.
        let read_len = block_size.max(block_content_len);
        match self
            .backend()
            .read_block_at_or_before(blob_guid, ceiling, block_num, read_len, trace_id)
            .await?
        {
            AtOrBeforeRead::Data { version, body } => {
                let mut data = body;
                if data.len() > block_content_len {
                    data = data.slice(0..block_content_len);
                }
                if let Some(dc) = &self.disk_cache {
                    let _ = dc.insert_block(blob_guid, block_num, version, &data).await;
                }
                Ok(data)
            }
            // A tombstone (punched/trimmed hole) or a Reserved claim
            // reads as zeros straight from the selected identity.
            AtOrBeforeRead::Zeros { .. } | AtOrBeforeRead::Hole { .. } => {
                Ok(Bytes::from(vec![0u8; block_content_len]))
            }
            AtOrBeforeRead::SparseHole => {
                // A sparse hole is zeros only after NSS confirms this
                // handle still names the layout that issued the miss.
                if validated_sparse_blocks.contains(&(blob_guid, block_num)) {
                    Ok(Bytes::from(vec![0u8; block_content_len]))
                } else {
                    Err(FsError::StaleLayout(blob_guid, block_num))
                }
            }
        }
    }

    /// Authoritative logical file size for data reads: the NSS layout is
    /// the sole size authority (the BSS geometry sentinel is gone with the
    /// versioned-key design). Cross-instance freshness comes from the
    /// attr-TTL-bounded layout refresh, same as every other attribute.
    pub(crate) async fn authoritative_file_size(
        &self,
        layout: &ObjectLayout,
    ) -> Result<u64, FsError> {
        layout.size().map_err(FsError::from)
    }

    pub(crate) async fn read_mpu(
        &self,
        key: &str,
        layout: &ObjectLayout,
        offset: u64,
        size: u32,
        validated_sparse_blocks: &HashSet<(data_types::DataBlobGuid, u32)>,
    ) -> Result<Bytes, FsError> {
        let file_size = layout.size()?;
        if size == 0 || offset >= file_size {
            return Ok(Bytes::new());
        }

        let read_end = std::cmp::min(offset.saturating_add(size as u64), file_size);
        let actual_len = (read_end - offset) as usize;
        let trace_id = TraceId::new();

        let parts = self.backend().list_mpu_parts(key, &trace_id).await?;

        let mut result = BytesMut::with_capacity(actual_len);
        let mut obj_offset: u64 = 0;

        for (_part_key, part_obj) in &parts {
            let part_size = part_obj.size()?;
            let part_end = obj_offset + part_size;

            if obj_offset >= read_end {
                break;
            }

            if part_end > offset {
                let blob_guid = part_obj.blob_guid()?;
                let part_ceiling = part_obj.blob_version;
                let block_size = part_obj.block_size as u64;

                let part_read_start = offset.saturating_sub(obj_offset);
                let part_read_end = if read_end < part_end {
                    read_end - obj_offset
                } else {
                    part_size
                };

                let first_block = (part_read_start / block_size) as u32;
                let last_block = ((part_read_end - 1) / block_size) as u32;

                for block_num in first_block..=last_block {
                    let block_start = block_num as u64 * block_size;
                    let block_content_len =
                        std::cmp::min(block_size, part_size - block_start) as usize;

                    let block_data = self
                        .read_block_cached(
                            blob_guid,
                            part_ceiling,
                            block_num,
                            block_content_len,
                            block_size as usize,
                            validated_sparse_blocks,
                            &trace_id,
                        )
                        .await?;

                    let slice_start = if block_num == first_block {
                        (part_read_start - block_start) as usize
                    } else {
                        0
                    };
                    let slice_end = if block_num == last_block {
                        (part_read_end - block_start) as usize
                    } else {
                        block_data.len()
                    };

                    if slice_start < block_data.len() {
                        let end = std::cmp::min(slice_end, block_data.len());
                        result.extend_from_slice(&block_data[slice_start..end]);
                    }
                }
            }

            obj_offset = part_end;
        }

        Ok(result.freeze())
    }

    /// Read a cached block directly into `buf`. Returns bytes written on
    /// hit, `None` on cache miss (caller should fall back to the Bytes
    /// path). Without a map, only a cached entry at exactly the ceiling
    /// identity can be validated locally; anything else is a miss.
    pub(crate) async fn read_block_cached_into(
        &self,
        blob_guid: data_types::DataBlobGuid,
        ceiling: u64,
        block_num: u32,
        block_content_len: usize,
        buf: &mut [u8],
    ) -> Option<usize> {
        if let Some(dc) = &self.disk_cache {
            dc.get_block_into_exact(blob_guid, block_num, ceiling, block_content_len, buf)
                .await
        } else {
            None
        }
    }

    /// Read a normal (non-MPU) object directly into a buffer.
    /// Returns the number of bytes written, or falls back to the Bytes path
    /// on any cache miss.
    pub(crate) async fn read_normal_buf(
        &self,
        layout: &ObjectLayout,
        offset: u64,
        buf: &mut [u8],
        validated_sparse_blocks: &HashSet<(data_types::DataBlobGuid, u32)>,
    ) -> Result<usize, FsError> {
        let file_size = self.authoritative_file_size(layout).await?;
        let size = buf.len() as u32;
        if size == 0 || offset >= file_size {
            return Ok(0);
        }

        let blob_guid = layout.blob_guid()?;
        let ceiling = layout.blob_version;
        let block_size = layout.block_size as u64;
        let read_end = std::cmp::min(offset.saturating_add(size as u64), file_size);
        let actual_len = (read_end - offset) as usize;

        let first_block = (offset / block_size) as u32;
        let last_block = ((read_end - 1) / block_size) as u32;

        let mut written = 0usize;

        for block_num in first_block..=last_block {
            let block_start = block_num as u64 * block_size;
            let block_content_len = std::cmp::min(block_size, file_size - block_start) as usize;

            let slice_start = if block_num == first_block {
                (offset - block_start) as usize
            } else {
                0
            };
            let slice_end = if block_num == last_block {
                (read_end - block_start) as usize
            } else {
                block_content_len
            };
            let chunk_len = slice_end.saturating_sub(slice_start);

            if slice_start == 0 && chunk_len == block_content_len {
                // Whole block: read directly into the output buffer
                if let Some(n) = self
                    .read_block_cached_into(
                        blob_guid,
                        ceiling,
                        block_num,
                        block_content_len,
                        &mut buf[written..written + chunk_len],
                    )
                    .await
                {
                    let copy_len = n.min(chunk_len);
                    written += copy_len;
                    continue;
                }
            } else {
                // Partial block: try to read full block into a temp region, then
                // slice the needed portion
                let mut tmp = vec![0u8; block_content_len];
                if let Some(n) = self
                    .read_block_cached_into(
                        blob_guid,
                        ceiling,
                        block_num,
                        block_content_len,
                        &mut tmp,
                    )
                    .await
                {
                    let end = slice_end.min(n);
                    if slice_start < end {
                        let copy_len = end - slice_start;
                        buf[written..written + copy_len].copy_from_slice(&tmp[slice_start..end]);
                        written += copy_len;
                        continue;
                    }
                }
            }

            // Cache miss: fall back to the Bytes path for this block and
            // the remaining blocks
            let trace_id = TraceId::new();
            let remaining = &mut buf[written..];
            let mut remaining_offset = written;

            for bn in block_num..=last_block {
                let bs = bn as u64 * block_size;
                let bcl = std::cmp::min(block_size, file_size - bs) as usize;

                let block_data = self
                    .read_block_cached(
                        blob_guid,
                        ceiling,
                        bn,
                        bcl,
                        block_size as usize,
                        validated_sparse_blocks,
                        &trace_id,
                    )
                    .await?;

                let ss = if bn == first_block {
                    (offset - bs) as usize
                } else {
                    0
                };
                let se = if bn == last_block {
                    (read_end - bs) as usize
                } else {
                    block_data.len()
                };

                if ss < block_data.len() {
                    let end = std::cmp::min(se, block_data.len());
                    let copy_len = end - ss;
                    let dest_end = (remaining_offset - written) + copy_len;
                    remaining[remaining_offset - written..dest_end]
                        .copy_from_slice(&block_data[ss..end]);
                    remaining_offset += copy_len;
                }
            }

            return Ok(remaining_offset);
        }

        Ok(written.min(actual_len))
    }

    /// Read data directly into a caller-provided buffer (zero-copy path).
    ///
    /// Tries to read from disk cache directly into `buf`. For cache misses
    /// or unsupported object states, falls back to the Bytes path internally.
    pub async fn vfs_read(
        &self,
        fh: FileHandleId,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, FsError> {
        let operation_lock = self
            .file_handles
            .get(&fh)
            .ok_or(FsError::BadFd)?
            .operation_lock
            .clone();
        let _operation_guard = operation_lock.lock().await;
        let handle = self.file_handles.get(&fh).ok_or(FsError::BadFd)?;

        // Dirty write buffer: merge per-block intents over the committed
        // bytes (sparse-aware read-your-own-writes within the handle).
        if let Some(ref wb) = handle.write_buf
            && wb.dirty
        {
            let file_size = wb.file_size;
            let block_size = wb.block_size;
            let existing_blob_guid = wb.existing_blob_guid;
            let eof_low_watermark = wb.eof_low_watermark;
            let blocks = wb.blocks.clone();
            let committed_layout = handle.layout.clone();
            drop(handle);
            let committed_ceiling = committed_layout.as_ref().map_or(0, |l| l.blob_version);
            let result = self
                .read_dirty_handle(
                    file_size,
                    block_size,
                    existing_blob_guid,
                    committed_ceiling,
                    &blocks,
                    eof_low_watermark,
                    offset,
                    buf,
                )
                .await;
            return result;
        }
        drop(handle);

        self.refresh_handle_layout(fh, false).await?;
        let mut validated_sparse_blocks = HashSet::new();
        let mut retried_corruption = false;
        let mut refresh_attempts = 0u32;
        loop {
            let version_id = self
                .file_handles
                .get(&fh)
                .and_then(|handle| handle.layout.as_ref().map(|layout| layout.version_id));
            let result = self
                .read_clean_handle(fh, offset, buf, &validated_sparse_blocks)
                .await;
            match result {
                Err(FsError::StaleLayout(blob_guid, block_number)) => {
                    refresh_attempts += 1;
                    if refresh_attempts > 64 {
                        return Err(FsError::StaleLayout(blob_guid, block_number));
                    }
                    self.refresh_handle_layout(fh, true).await?;
                    let refreshed_version = self
                        .file_handles
                        .get(&fh)
                        .and_then(|handle| handle.layout.as_ref().map(|layout| layout.version_id));
                    if refreshed_version == version_id {
                        if !validated_sparse_blocks.insert((blob_guid, block_number)) {
                            return Err(FsError::StaleLayout(blob_guid, block_number));
                        }
                    } else {
                        validated_sparse_blocks.clear();
                    }
                }
                Err(FsError::DataVg(volume_group_proxy::DataVgError::Corrupted))
                    if !retried_corruption =>
                {
                    self.refresh_handle_layout(fh, true).await?;
                    validated_sparse_blocks.clear();
                    retried_corruption = true;
                }
                other => {
                    return other;
                }
            }
        }
    }

    pub(crate) async fn read_clean_handle(
        &self,
        fh: FileHandleId,
        offset: u64,
        buf: &mut [u8],
        validated_sparse_blocks: &HashSet<(data_types::DataBlobGuid, u32)>,
    ) -> Result<usize, FsError> {
        let handle = self.file_handles.get(&fh).ok_or(FsError::BadFd)?;
        let layout = match &handle.layout {
            Some(layout) => layout.clone(),
            None => return Ok(0),
        };
        let s3_key = handle.s3_key.clone();
        drop(handle);

        match &layout.state {
            ObjectState::Normal(_) => {
                self.read_normal_buf(&layout, offset, buf, validated_sparse_blocks)
                    .await
            }
            ObjectState::Mpu(MpuState::Completed(_)) => {
                let data = self
                    .read_mpu(
                        &s3_key,
                        &layout,
                        offset,
                        buf.len() as u32,
                        validated_sparse_blocks,
                    )
                    .await?;
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                Ok(n)
            }
            _ => Err(FsError::InvalidState),
        }
    }

    /// Refresh a clean handle's committed layout on the same TTL used for
    /// inode attributes. Open file handles otherwise pin a map forever,
    /// while the superseded-generation sweep reclaims that map and its data
    /// after the reader grace. A forced refresh is used to retry a read that
    /// raced reclamation.
    pub(crate) async fn refresh_handle_layout(
        &self,
        fh: FileHandleId,
        force: bool,
    ) -> Result<(), FsError> {
        let (ino, s3_key, version_id) = {
            let handle = self.file_handles.get(&fh).ok_or(FsError::BadFd)?;
            if handle.write_buf.as_ref().is_some_and(|wb| wb.dirty) {
                return Ok(());
            }
            let Some(layout) = handle.layout.as_ref() else {
                return Ok(());
            };
            if !force && handle.layout_refreshed_at.elapsed() < TTL {
                return Ok(());
            }
            (handle.ino, handle.s3_key.clone(), layout.version_id)
        };

        let (inode_id, name_removed) = self
            .inodes
            .get(ino)
            .map(|entry| (entry.inode_id, entry.name_removed))
            .unwrap_or((None, false));
        if name_removed && inode_id.is_none() {
            if let Some(mut handle) = self.file_handles.get_mut(&fh) {
                handle.layout_refreshed_at = Instant::now();
            }
            return Ok(());
        }

        let trace_id = TraceId::new();
        let (fresh, resolved_id) = if let Some(id) = inode_id {
            (
                self.backend().get_inode_record(id, &trace_id).await?.layout,
                Some(id),
            )
        } else {
            let layout = match self.backend().get_inode(&s3_key, &trace_id).await {
                Ok(layout) => layout,
                // A rename can remove the original name while an open fd
                // legitimately keeps the old blob alive. Retain that handle
                // snapshot; rename does not enqueue a data sweep.
                Err(FsError::NotFound) => {
                    if let Some(mut handle) = self.file_handles.get_mut(&fh)
                        && handle.layout.as_ref().map(|l| l.version_id) == Some(version_id)
                    {
                        handle.layout_refreshed_at = Instant::now();
                    }
                    return Ok(());
                }
                Err(e) => return Err(e),
            };
            let (layout, id, _) = self.resolve_indirect(layout, &trace_id).await?;
            (layout, id)
        };

        let retain_open_identity = self
            .file_handles
            .get(&fh)
            .and_then(|handle| handle.layout.clone())
            .is_some_and(|current| match &current.state {
                ObjectState::Normal(_) => current.blob_guid().ok() != fresh.blob_guid().ok(),
                _ => false,
            });
        if retain_open_identity {
            if let Some(mut handle) = self.file_handles.get_mut(&fh) {
                handle.layout_refreshed_at = Instant::now();
            }
            return Ok(());
        }

        let mut updated = false;
        if let Some(mut handle) = self.file_handles.get_mut(&fh)
            && handle.layout.as_ref().map(|l| l.version_id) == Some(version_id)
        {
            handle.layout = Some(fresh.clone());
            handle.layout_refreshed_at = Instant::now();
            if let Some(wb) = handle.write_buf.as_mut()
                && !wb.dirty
            {
                wb.file_size = fresh.size()?;
                wb.existing_blob_guid = fresh.blob_guid().ok();
                wb.block_size = fresh.block_size;
                wb.size_changed = false;
                wb.eof_low_watermark = None;
                wb.trim_upper = None;
            }
            updated = true;
        }
        if updated && let Some(mut entry) = self.inodes.get_mut(ino) {
            entry.layout = Some(fresh);
            if let Some(id) = resolved_id {
                entry.inode_id = Some(id);
            }
        }
        Ok(())
    }
}
