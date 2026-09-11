//! Data read paths: key-addressed block reads, MPU stitching, vfs_read,
//! and the TTL-bounded clean-handle layout refresh.

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use data_types::TraceId;
use data_types::object_layout::{MpuState, ObjectLayout, ObjectState};
use data_types::ovr_map::zeros;
use fractal_fuse::FileHandleId;
use futures::{StreamExt, TryStreamExt, stream};

use crate::error::FsError;
use crate::vfs::{TTL, VfsCore};

/// Blocks fetched in parallel for one multi-block read.
const READ_CONCURRENCY: usize = 8;
/// Bound on stale-layout refreshes within one read.
const MAX_STALE_REFRESHES: u32 = 64;

impl VfsCore {
    /// Read one committed block of the file at `key` against `layout`.
    /// The gateway resolves the exact generation and serves its disk
    /// cache first; a hole comes back as zeros with no data on the wire.
    /// A `StaleLayout` error means the key moved past `layout`.
    pub(crate) async fn read_block_committed(
        &self,
        key: &str,
        layout: &ObjectLayout,
        block_num: u32,
        block_content_len: usize,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        match self
            .backend()
            .read_block(key, block_num, Some(layout.version_id), trace_id)
            .await?
        {
            Some(data) if data.len() > block_content_len => Ok(data.slice(0..block_content_len)),
            Some(data) => Ok(data),
            None => Ok(zeros(block_content_len)),
        }
    }

    pub(crate) async fn read_mpu(
        &self,
        key: &str,
        layout: &ObjectLayout,
        offset: u64,
        size: u32,
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

        for (part_key, part_obj) in &parts {
            let part_size = part_obj.size()?;
            let part_end = obj_offset + part_size;

            if obj_offset >= read_end {
                break;
            }

            if part_end > offset {
                let block_size = part_obj.block_size as u64;

                let part_read_start = offset.saturating_sub(obj_offset);
                let part_read_end = if read_end < part_end {
                    read_end - obj_offset
                } else {
                    part_size
                };

                let first_block = (part_read_start / block_size) as u32;
                let last_block = ((part_read_end - 1) / block_size) as u32;

                let fetched = stream::iter(first_block..=last_block)
                    .map(|block_num| {
                        let trace_id = &trace_id;
                        async move {
                            let block_start = block_num as u64 * block_size;
                            let block_content_len =
                                std::cmp::min(block_size, part_size - block_start) as usize;
                            self.read_block_committed(
                                part_key,
                                part_obj,
                                block_num,
                                block_content_len,
                                trace_id,
                            )
                            .await
                            .map(|data| (block_num, data))
                        }
                    })
                    .buffered(READ_CONCURRENCY)
                    .try_collect::<Vec<_>>()
                    .await?;

                for (block_num, block_data) in fetched {
                    let block_start = block_num as u64 * block_size;
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

    /// Read a normal (non-MPU) object directly into a buffer. Returns the
    /// number of bytes written.
    pub(crate) async fn read_normal_buf(
        &self,
        key: &str,
        layout: &ObjectLayout,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, FsError> {
        // The committed layout is the sole size authority; freshness comes
        // from the attr-TTL-bounded layout refresh.
        let file_size = layout.size()?;
        let size = buf.len() as u32;
        if size == 0 || offset >= file_size {
            return Ok(0);
        }

        let block_size = layout.block_size as u64;
        let read_end = std::cmp::min(offset.saturating_add(size as u64), file_size);
        let actual_len = (read_end - offset) as usize;

        let first_block = (offset / block_size) as u32;
        let last_block = ((read_end - 1) / block_size) as u32;

        // Blocks are fetched from the gateway concurrently (in order) so
        // a multi-block read pays one round trip, not one per block.
        let trace_id = TraceId::new();
        let fetched = stream::iter(first_block..=last_block)
            .map(|block_num| {
                let trace_id = &trace_id;
                async move {
                    let block_start = block_num as u64 * block_size;
                    let block_content_len =
                        std::cmp::min(block_size, file_size - block_start) as usize;
                    self.read_block_committed(key, layout, block_num, block_content_len, trace_id)
                        .await
                        .map(|data| (block_num, data))
                }
            })
            .buffered(READ_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;

        let mut written = 0usize;
        for (block_num, block_data) in fetched {
            let block_start = block_num as u64 * block_size;
            let slice_start = if block_num == first_block {
                (offset - block_start) as usize
            } else {
                0
            };
            let slice_end = if block_num == last_block {
                (read_end - block_start) as usize
            } else {
                block_data.len()
            };
            if slice_start < block_data.len() {
                let end = std::cmp::min(slice_end, block_data.len());
                let copy_len = end - slice_start;
                buf[written..written + copy_len].copy_from_slice(&block_data[slice_start..end]);
                written += copy_len;
            }
        }

        Ok(written.min(actual_len))
    }

    async fn read_clean_handle(
        &self,
        fh: FileHandleId,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, FsError> {
        let handle = self.file_handles.get(&fh).ok_or(FsError::BadFd)?;
        let layout = match &handle.layout {
            Some(layout) => layout.clone(),
            None => return Ok(0),
        };
        let s3_key = handle.s3_key.clone();
        drop(handle);

        match &layout.state {
            ObjectState::Normal(_) => self.read_normal_buf(&s3_key, &layout, offset, buf).await,
            ObjectState::Mpu(MpuState::Completed(_)) => {
                let data = self
                    .read_mpu(&s3_key, &layout, offset, buf.len() as u32)
                    .await?;
                let n = data.len().min(buf.len());
                buf[..n].copy_from_slice(&data[..n]);
                Ok(n)
            }
            _ => Err(FsError::InvalidState),
        }
    }

    /// Read data directly into a caller-provided buffer.
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
            let has_committed_data = wb.has_committed_data;
            let eof_low_watermark = wb.eof_low_watermark;
            let blocks = wb.blocks.clone();
            let committed_layout = handle.layout.clone();
            let s3_key = handle.s3_key.clone();
            drop(handle);
            return self
                .read_dirty_handle(
                    &s3_key,
                    file_size,
                    block_size,
                    has_committed_data,
                    committed_layout.as_ref(),
                    &blocks,
                    eof_low_watermark,
                    offset,
                    buf,
                )
                .await;
        }
        drop(handle);

        self.refresh_handle_layout(fh, false).await?;
        let mut retried_corruption = false;
        let mut refresh_attempts = 0u32;
        loop {
            match self.read_clean_handle(fh, offset, buf).await {
                // The gateway resolved against a newer committed layout
                // than this handle holds. Refresh and restart the whole
                // request so one read() never straddles two commits.
                Err(FsError::StaleLayout) => {
                    refresh_attempts += 1;
                    if refresh_attempts > MAX_STALE_REFRESHES {
                        tracing::error!(
                            fh = fh.0,
                            "stale-layout retry budget exhausted; layout never settled"
                        );
                        return Err(FsError::StaleLayout);
                    }
                    self.refresh_handle_layout(fh, true).await?;
                }
                Err(FsError::Corrupted) if !retried_corruption => {
                    self.refresh_handle_layout(fh, true).await?;
                    retried_corruption = true;
                }
                other => return other,
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

        let inode_id = self.inodes.get(ino).and_then(|entry| entry.inode_id);

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
                // legitimately keeps the old data alive. Retain that handle
                // snapshot; rename does not tear the data down.
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

        // An S3 PUT that replaced the object installs a fresh blob. Data is
        // addressed by key, so the open fd adopts the replacement rather
        // than keeping a snapshot it could no longer read (a deliberate
        // departure from unlink semantics for out-of-band replacement).
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
                wb.has_committed_data = true;
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
