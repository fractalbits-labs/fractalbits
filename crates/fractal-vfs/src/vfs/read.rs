//! Data read paths: row-resolved exact block reads, MPU stitching,
//! vfs_read, and the TTL-bounded clean-handle layout refresh.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use data_types::TraceId;
use data_types::object_layout::{MpuState, ObjectLayout, ObjectState};
use data_types::ovr_map::{BlockFetchPlan, OvrRowMap, block_fetch_plan, zeros};
use fractal_fuse::FileHandleId;

use crate::error::FsError;
use crate::vfs::{TTL, VfsCore};

impl VfsCore {
    /// Read a block by resolving its exact committed identity from the
    /// row snapshot (base version 1 when unmapped/absent), checking the
    /// disk cache at that identity, then fetching from BSS. A `Hole`
    /// resolution returns zeros with no BSS access.
    ///
    /// Miss semantics carry the row's durability contract: a
    /// row-committed generation missing on every replica is detected
    /// data loss (fail loudly), while a base-version miss is a sparse
    /// hole only after the layout is revalidated against NSS (the
    /// `validated_sparse_blocks` / `StaleLayout` protocol).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_block_cached(
        &self,
        blob_guid: data_types::DataBlobGuid,
        rows: Option<&OvrRowMap>,
        ceiling: u64,
        block_num: u32,
        block_content_len: usize,
        block_size: usize,
        validated_sparse_blocks: &HashSet<(data_types::DataBlobGuid, u32)>,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        let (version, read_len, miss_is_loss) =
            match block_fetch_plan(rows, block_num, ceiling, block_size, block_content_len) {
                BlockFetchPlan::Zeros => return Ok(zeros(block_content_len)),
                // Both row slots sit above this reader's ceiling: the
                // layout snapshot is older than the row. Refresh both
                // and retry via the stale-layout protocol.
                BlockFetchPlan::Stale => {
                    return Err(FsError::StaleLayout(blob_guid, block_num));
                }
                BlockFetchPlan::Fetch {
                    version,
                    read_len,
                    miss_is_loss,
                } => (version, read_len, miss_is_loss),
            };

        if let Some(dc) = &self.disk_cache
            && let Some(cached) = dc
                .get_block_exact(blob_guid, block_num, version, block_content_len)
                .await
        {
            return Ok(cached);
        }

        let (mut data, _checksum) = match self
            .backend()
            .read_block(blob_guid, version, block_num, read_len, trace_id)
            .await
        {
            Ok(r) => r,
            Err(e) if e.is_block_missing() => {
                if miss_is_loss {
                    // The row committed this exact generation; nobody
                    // holding it is data loss, never a hole. Fail loudly
                    // instead of serving zeros or older bytes.
                    tracing::error!(
                        %blob_guid,
                        block_num,
                        version,
                        "DATA LOSS: row-committed generation missing on every replica"
                    );
                    return Err(FsError::DataVg(volume_group_proxy::DataVgError::Corrupted));
                }
                // A base-version miss is zeros only after NSS confirms
                // this handle still names the layout that issued it.
                if validated_sparse_blocks.contains(&(blob_guid, block_num)) {
                    return Ok(zeros(block_content_len));
                }
                return Err(FsError::StaleLayout(blob_guid, block_num));
            }
            Err(e) => return Err(e),
        };
        if data.len() > block_content_len {
            data = data.slice(0..block_content_len);
        }

        // Populate the disk cache at the exact identity fetched.
        if let Some(dc) = &self.disk_cache {
            let _ = dc.insert_block(blob_guid, block_num, version, &data).await;
        }

        Ok(data)
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

        let parts = self
            .backend()
            .list_mpu_parts(key, layout.version_id, &trace_id)
            .await?;

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
                let part_rows = self.layout_row_map(part_obj).await?;
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
                            part_rows.as_deref(),
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
    /// hit (including a metadata-resolved hole), `None` on cache miss
    /// (caller falls back to the Bytes path). A hit counts only when the
    /// cached entry carries the block's exact committed identity; a
    /// `Stale` resolution is a miss so the Bytes path surfaces it.
    pub(crate) async fn read_block_cached_into(
        &self,
        blob_guid: data_types::DataBlobGuid,
        rows: Option<&OvrRowMap>,
        ceiling: u64,
        block_num: u32,
        block_content_len: usize,
        buf: &mut [u8],
    ) -> Option<usize> {
        let version = match block_fetch_plan(
            rows,
            block_num,
            ceiling,
            block_content_len,
            block_content_len,
        ) {
            BlockFetchPlan::Zeros => {
                let n = block_content_len.min(buf.len());
                buf[..n].fill(0);
                return Some(n);
            }
            BlockFetchPlan::Stale => return None,
            BlockFetchPlan::Fetch { version, .. } => version,
        };
        if let Some(dc) = &self.disk_cache {
            dc.get_block_into_exact(blob_guid, block_num, version, block_content_len, buf)
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
        // The NSS layout is the sole size authority (the BSS geometry
        // sentinel is gone with the versioned-key design); freshness
        // comes from the attr-TTL-bounded layout refresh.
        let file_size = layout.size()?;
        let size = buf.len() as u32;
        if size == 0 || offset >= file_size {
            return Ok(0);
        }

        let blob_guid = layout.blob_guid()?;
        let rows = self.layout_row_map(layout).await?;
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
                        rows.as_deref(),
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
                        rows.as_deref(),
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
                        rows.as_deref(),
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

    async fn read_clean_handle(
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
            let (committed_rows, committed_ceiling) =
                self.rows_and_ceiling(committed_layout.as_ref()).await?;
            return self
                .read_dirty_handle(
                    file_size,
                    block_size,
                    existing_blob_guid,
                    committed_rows.as_deref(),
                    committed_ceiling,
                    &blocks,
                    eof_low_watermark,
                    offset,
                    buf,
                )
                .await;
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
                        tracing::error!(
                            %blob_guid,
                            block_number,
                            "stale-layout retry budget exhausted; ceiling never advanced"
                        );
                        return Err(FsError::StaleLayout(blob_guid, block_number));
                    }
                    self.refresh_handle_layout(fh, true).await?;
                    let refreshed_version = self
                        .file_handles
                        .get(&fh)
                        .and_then(|handle| handle.layout.as_ref().map(|layout| layout.version_id));
                    if refreshed_version == version_id {
                        // NSS still names the very layout that produced
                        // the miss: the miss is a genuine sparse hole for
                        // this identity. Restarting the whole request at
                        // the (unchanged) ceiling keeps one read() from
                        // straddling two snapshots.
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

    /// Refresh a clean handle's committed layout on the same TTL used
    /// for inode attributes. Open file handles otherwise pin a
    /// generation set forever, while the superseded-generation sweep
    /// reclaims it after the reader grace. A forced refresh retries a
    /// read that raced reclamation or a remote commit.
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

        // An S3 PUT that replaced the object installs a fresh blob_guid;
        // the open fd keeps its snapshot of the old blob (Unix unlink
        // semantics) and takes its chances against that blob's deletion.
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

    /// Prefetch helper: hand the row snapshot to the whole-blob
    /// prefetcher so every insert lands at the exact committed identity.
    pub(crate) async fn row_map_for_prefetch(
        &self,
        layout: &ObjectLayout,
    ) -> Option<Arc<OvrRowMap>> {
        self.layout_row_map(layout).await.ok().flatten()
    }
}
