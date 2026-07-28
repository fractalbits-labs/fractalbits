//! Data read paths: cached block reads, MPU stitching, vfs_read.

use bytes::{Bytes, BytesMut};
use data_types::TraceId;
use data_types::object_layout::{MpuState, ObjectLayout, ObjectState};
use fractal_fuse::FileHandleId;

use crate::error::FsError;
use crate::vfs::{DEFAULT_BLOCK_SIZE, VfsCore};

impl VfsCore {
    /// Read a block, checking disk cache first. On miss, fetches from backend
    /// and populates disk cache.
    pub(crate) async fn read_block_cached(
        &self,
        blob_guid: data_types::DataBlobGuid,
        blob_version: u64,
        block_num: u32,
        block_content_len: usize,
        _file_size: u64,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        // Try disk cache
        if let Some(dc) = &self.disk_cache
            && let Some(cached) = dc.get_block(blob_guid, block_num, block_content_len).await
        {
            return Ok(cached);
        }

        // Cache miss: fetch from backend at a version no older than the
        // cache's floor. A reader on a stale handle still carries its open-
        // time `blob_version`; if a newer override has since raised the
        // floor, fetching at the stale version could trip BSS's non-quorum
        // `v <= 1` path and return pre-override bytes. Lower-bounding by the
        // floor matches what a cache hit would have returned (the latest
        // this instance published).
        let read_version = match &self.disk_cache {
            Some(dc) => blob_version.max(dc.floor_version(blob_guid).await.unwrap_or(0)),
            None => blob_version,
        };

        // Override (read_version > 1) blocks are zero-padded to a full
        // block_size on disk, so the EC shard size is block_size/k;
        // request the full block_size (otherwise the EC read derives a
        // smaller shard size from the logical length and filters out the
        // padded shards), then truncate to the logical content length.
        // Non-override blocks are stored at their exact length and read
        // as-is.
        let read_len = if read_version > 1 {
            (DEFAULT_BLOCK_SIZE as usize).max(block_content_len)
        } else {
            block_content_len
        };
        let (mut data, _checksum) = match self
            .backend()
            .read_block(blob_guid, read_version, block_num, read_len, trace_id)
            .await
        {
            Ok(r) => r,
            // A missing block is a hole: serve zeros (do not cache the hole).
            Err(FsError::DataVg(volume_group_proxy::DataVgError::BlockNotFound))
            | Err(FsError::Rpc(rpc_client_common::RpcError::NotFound)) => {
                return Ok(Bytes::from(vec![0u8; block_content_len]));
            }
            Err(e) => return Err(e),
        };
        if data.len() > block_content_len {
            data = data.slice(0..block_content_len);
        }

        // Populate disk cache at the version actually fetched.
        if let Some(dc) = &self.disk_cache {
            let _ = dc
                .insert_block(blob_guid, block_num, read_version, &data)
                .await;
        }

        Ok(data)
    }

    /// Authoritative logical file size for data reads. The geometry
    /// sentinel (our BSS-parent-size authority) reflects the latest
    /// committed override regardless of our cached layout version, so a
    /// read on a handle whose cached layout lags a peer's overwrite (or
    /// this instance's own just-committed flush) still sees the right EOF.
    /// The cached/NSS layout size is a lazy copy. Falls back to the cached
    /// size when no sentinel exists or it is older than the cached layout
    /// (so a stale sentinel never shrinks a fresher local size).
    pub(crate) async fn authoritative_file_size(
        &self,
        layout: &ObjectLayout,
    ) -> Result<u64, FsError> {
        let cached = layout.size()?;
        if layout.is_symlink() || layout.special().is_some() {
            return Ok(cached);
        }
        if let Ok(guid) = layout.blob_guid() {
            let trace_id = TraceId::new();
            if let Ok(Some(info)) = self.backend().get_blob_info(guid, &trace_id).await
                && info.blob_version >= layout.blob_version
            {
                return Ok(info.total_size);
            }
        }
        Ok(cached)
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
                            part_obj.blob_version,
                            block_num,
                            block_content_len,
                            part_size,
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

    /// Read a cached block directly into `buf`. Returns bytes written on hit,
    /// or `None` on cache miss (caller should fall back to the Bytes path).
    pub(crate) async fn read_block_cached_into(
        &self,
        blob_guid: data_types::DataBlobGuid,
        _blob_version: u64,
        block_num: u32,
        block_content_len: usize,
        buf: &mut [u8],
    ) -> Option<usize> {
        if let Some(dc) = &self.disk_cache {
            dc.get_block_into(blob_guid, block_num, block_content_len, buf)
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
    ) -> Result<usize, FsError> {
        let file_size = self.authoritative_file_size(layout).await?;
        let size = buf.len() as u32;
        if size == 0 || offset >= file_size {
            return Ok(0);
        }

        let blob_guid = layout.blob_guid()?;
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
                        layout.blob_version,
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
                        layout.blob_version,
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
                        layout.blob_version,
                        bn,
                        bcl,
                        file_size,
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
            let committed_blob_version =
                handle.layout.as_ref().map(|l| l.blob_version).unwrap_or(0);
            drop(handle);
            return self
                .read_dirty_handle(
                    file_size,
                    block_size,
                    existing_blob_guid,
                    committed_blob_version,
                    &blocks,
                    eof_low_watermark,
                    offset,
                    buf,
                )
                .await;
        }

        let layout = match &handle.layout {
            Some(l) => l.clone(),
            None => return Ok(0),
        };
        let s3_key = handle.s3_key.clone();
        drop(handle);

        match &layout.state {
            ObjectState::Normal(_) => self.read_normal_buf(&layout, offset, buf).await,
            ObjectState::Mpu(MpuState::Completed(_)) => {
                // MPU: fall back to the Bytes path and copy
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
}
