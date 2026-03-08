use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use data_types::TraceId;
use rkyv::api::high::to_bytes_in;
use std::cell::Cell;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::backend::{BackendConfig, StorageBackend};
use crate::cache::{DirCache, DirEntry};
use crate::disk_cache::DiskCache;
use crate::error::FsError;
use crate::inode::{EntryType, InodeTable, ROOT_INODE};
use data_types::object_layout::{
    MpuState, ObjectCoreMetaData, ObjectLayout, ObjectMetaData, ObjectState,
};
use fractal_fuse::FuseNotifier;

pub const TTL: Duration = Duration::from_secs(1);
pub const DEFAULT_BLOCK_SIZE: u32 = 1024 * 1024 - 256;

/// Protocol-agnostic file/directory attributes.
#[derive(Debug, Clone, Copy)]
pub struct VfsAttr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime_secs: u64,
    pub mtime_secs: u64,
    pub ctime_secs: u64,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
}

#[derive(Debug, Clone)]
pub struct VfsDirEntry {
    pub ino: u64,
    pub offset: u64,
    pub is_dir: bool,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct VfsDirEntryPlus {
    pub ino: u64,
    pub offset: u64,
    pub is_dir: bool,
    pub name: String,
    pub attr: VfsAttr,
}

#[derive(Debug, Clone, Copy)]
pub struct VfsStatfs {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u32,
    pub namelen: u32,
    pub frsize: u32,
}

thread_local! {
    static THREAD_BACKEND: Cell<Option<&'static StorageBackend>> = const { Cell::new(None) };
}

struct WriteBuffer {
    data: BytesMut,
    dirty: bool,
}

struct FileHandle {
    ino: u64,
    s3_key: String,
    layout: Option<ObjectLayout>,
    write_buf: Option<WriteBuffer>,
    backing_id: Option<i32>,
}

pub struct VfsCore {
    backend_config: Arc<BackendConfig>,
    inodes: Arc<InodeTable>,
    disk_cache: Option<DiskCache>,
    dir_cache: DirCache,
    file_handles: DashMap<u64, FileHandle>,
    next_fh: AtomicU64,
    read_write: bool,
    passthrough_enabled: bool,
    passthrough_max_object_size: u64,
    fuse_dev_fd: AtomicI32,
    notifier: OnceLock<FuseNotifier>,
    // Tracks blob data for unlinked files that still have open handles.
    // Cleanup is deferred until the last handle is released.
    deferred_blob_cleanup: DashMap<u64, Bytes>,
}

impl VfsCore {
    pub fn new(
        backend_config: Arc<BackendConfig>,
        inodes: Arc<InodeTable>,
        read_write: bool,
    ) -> Self {
        let config = &backend_config.config;
        let dir_cache_ttl = config.dir_cache_ttl();

        let disk_cache = if config.disk_cache_enabled {
            match DiskCache::new(
                &config.disk_cache_path,
                config.disk_cache_size_gb,
                DEFAULT_BLOCK_SIZE as u64,
            ) {
                Ok(dc) => {
                    tracing::info!(
                        path = %config.disk_cache_path,
                        size_gb = config.disk_cache_size_gb,
                        "disk cache enabled"
                    );
                    Some(dc)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to init disk cache, falling back to no cache");
                    None
                }
            }
        } else {
            None
        };

        let passthrough_enabled = config.passthrough_enabled;
        let passthrough_max_object_size =
            config.passthrough_max_object_size_gb * 1024 * 1024 * 1024;

        Self {
            backend_config,
            inodes,
            disk_cache,
            dir_cache: DirCache::new(dir_cache_ttl),
            file_handles: DashMap::new(),
            next_fh: AtomicU64::new(1),
            read_write,
            passthrough_enabled,
            passthrough_max_object_size,
            fuse_dev_fd: AtomicI32::new(-1),
            notifier: OnceLock::new(),
            deferred_blob_cleanup: DashMap::new(),
        }
    }

    // ── Internal helpers ──

    /// Get the per-thread StorageBackend, creating it on first access.
    /// The backend is leaked into 'static storage because each compio thread
    /// runs for the lifetime of the process and we need references that can
    /// be held across await points.
    fn backend(&self) -> &StorageBackend {
        THREAD_BACKEND.with(|cell| match cell.get() {
            Some(b) => b,
            None => {
                let b = Box::new(
                    StorageBackend::new(&self.backend_config)
                        .expect("Failed to create per-thread StorageBackend"),
                );
                let leaked: &'static StorageBackend = Box::leak(b);
                cell.set(Some(leaked));
                leaked
            }
        })
    }

    fn alloc_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }

    fn dir_prefix(&self, ino: u64) -> Option<String> {
        self.inodes.get_s3_key(ino)
    }

    fn check_write_enabled(&self) -> Result<(), FsError> {
        if !self.read_write {
            return Err(FsError::ReadOnly);
        }
        Ok(())
    }

    fn has_open_handles_for_inode(&self, ino: u64, exclude_fh: Option<u64>) -> bool {
        self.file_handles.iter().any(|entry| {
            entry.value().ino == ino && exclude_fh.is_none_or(|excl| *entry.key() != excl)
        })
    }

    fn file_perm(&self) -> u16 {
        if self.read_write { 0o644 } else { 0o444 }
    }

    fn dir_perm(&self) -> u16 {
        if self.read_write { 0o755 } else { 0o555 }
    }

    // ── Attribute builders ──

    fn make_file_attr(&self, ino: u64, layout: &ObjectLayout) -> Result<VfsAttr, FsError> {
        let size = layout.size()?;
        let ts = layout.timestamp / 1000;
        Ok(VfsAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime_secs: ts,
            mtime_secs: ts,
            ctime_secs: ts,
            mode: file_mode(self.file_perm()),
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        })
    }

    /// Fallback file attr when layout is unavailable (e.g., inode evicted
    /// between fetch_dir_entries and readdirplus iteration). Uses correct
    /// kind=RegularFile to avoid on-wire inconsistency.
    fn make_default_file_attr(&self, ino: u64) -> VfsAttr {
        VfsAttr {
            ino,
            size: 0,
            blocks: 0,
            atime_secs: 0,
            mtime_secs: 0,
            ctime_secs: 0,
            mode: file_mode(self.file_perm()),
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        }
    }

    fn make_dir_attr(&self, ino: u64) -> VfsAttr {
        VfsAttr {
            ino,
            size: 0,
            blocks: 0,
            atime_secs: 0,
            mtime_secs: 0,
            ctime_secs: 0,
            mode: dir_mode(self.dir_perm()),
            nlink: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        }
    }

    fn make_new_file_attr(&self, ino: u64, size: u64) -> VfsAttr {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        VfsAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime_secs: now_secs,
            mtime_secs: now_secs,
            ctime_secs: now_secs,
            mode: file_mode(self.file_perm()),
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        }
    }

    // ── Passthrough helpers ──

    pub fn set_fuse_dev_fd(&self, fd: i32) {
        self.fuse_dev_fd.store(fd, Ordering::Relaxed);
        let _ = self.notifier.set(FuseNotifier::new(fd));
    }

    /// Try to set up passthrough for a file handle. Returns (open_flags, backing_id)
    /// if passthrough is activated, or (0, 0) otherwise.
    pub fn try_passthrough(&self, fh: u64, layout: &ObjectLayout) -> (u32, i32) {
        if !self.passthrough_enabled {
            return (0, 0);
        }

        let dc = match &self.disk_cache {
            Some(dc) => dc,
            None => return (0, 0),
        };

        let file_size = match layout.size() {
            Ok(s) => s,
            Err(_) => return (0, 0),
        };

        // Skip large files
        if file_size > self.passthrough_max_object_size || file_size == 0 {
            return (0, 0);
        }

        let blob_guid = match layout.blob_guid() {
            Ok(g) => g,
            Err(_) => return (0, 0),
        };

        // Check if fully cached
        if !dc.is_complete(blob_guid.blob_id, blob_guid.volume_id, file_size) {
            return (0, 0);
        }

        let fuse_fd = self.fuse_dev_fd.load(Ordering::Relaxed);
        if fuse_fd < 0 {
            return (0, 0);
        }

        // Open the cache file and register as backing fd
        let cache_path = dc.cache_file_path(blob_guid.blob_id, blob_guid.volume_id);
        let backing_file = match std::fs::File::open(&cache_path) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(error = %e, "failed to open cache file for passthrough");
                return (0, 0);
            }
        };

        use std::os::fd::AsRawFd;
        let backing_fd = backing_file.as_raw_fd();

        match fractal_fuse::passthrough::fuse_backing_open(fuse_fd, backing_fd) {
            Ok(bid) => {
                tracing::info!(fh, backing_id = bid, "passthrough activated");
                // Store backing_id in file handle for cleanup
                if let Some(mut handle) = self.file_handles.get_mut(&fh) {
                    handle.backing_id = Some(bid);
                }
                (fractal_fuse::abi::FOPEN_PASSTHROUGH, bid)
            }
            Err(e) => {
                tracing::debug!(error = %e, "passthrough ioctl failed (not supported?)");
                (0, 0)
            }
        }
    }

    /// Try passthrough for an already-opened file handle.
    pub fn try_passthrough_for_fh(&self, fh: u64) -> Option<(u32, i32)> {
        let handle = self.file_handles.get(&fh)?;
        let layout = handle.layout.as_ref()?;
        Some(self.try_passthrough(fh, layout))
    }

    /// Clean up passthrough backing_id on file release.
    pub fn release_passthrough(&self, fh: u64) {
        let backing_id = self.file_handles.get(&fh).and_then(|h| h.backing_id);

        if let Some(bid) = backing_id {
            let fuse_fd = self.fuse_dev_fd.load(Ordering::Relaxed);
            if fuse_fd >= 0
                && let Err(e) = fractal_fuse::passthrough::fuse_backing_close(fuse_fd, bid)
            {
                tracing::warn!(backing_id = bid, error = %e, "failed to close backing");
            }
        }
    }

    // ── Cache helpers ──

    /// Read a block, checking disk cache first. On miss, fetches from backend
    /// and populates disk cache.
    async fn read_block_cached(
        &self,
        blob_guid: data_types::DataBlobGuid,
        block_num: u32,
        block_content_len: usize,
        file_size: u64,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        // Try disk cache
        if let Some(dc) = &self.disk_cache
            && let Some(cached) = dc
                .get(blob_guid.blob_id, blob_guid.volume_id, block_num, file_size)
                .await
        {
            return Ok(cached);
        }

        // Cache miss: fetch from backend
        let (data, checksum) = self
            .backend()
            .read_block(blob_guid, block_num, block_content_len, trace_id)
            .await?;

        // Populate disk cache
        if let Some(dc) = &self.disk_cache {
            dc.insert(
                blob_guid.blob_id,
                blob_guid.volume_id,
                block_num,
                file_size,
                &data,
                checksum,
            )
            .await;
        }

        Ok(data)
    }

    // ── Read helpers ──

    async fn preload_file_content(
        &self,
        s3_key: &str,
        layout: &ObjectLayout,
    ) -> Result<BytesMut, FsError> {
        let size = layout.size()?;
        if size == 0 {
            return Ok(BytesMut::new());
        }
        let read_size = size.min(u32::MAX as u64) as u32;
        let data = match &layout.state {
            ObjectState::Normal(_) => self.read_normal(layout, 0, read_size).await?,
            ObjectState::Mpu(MpuState::Completed(_)) => {
                self.read_mpu(s3_key, layout, 0, read_size).await?
            }
            _ => return Err(FsError::InvalidState),
        };
        Ok(BytesMut::from(data.as_ref()))
    }

    async fn read_normal(
        &self,
        layout: &ObjectLayout,
        offset: u64,
        size: u32,
    ) -> Result<Bytes, FsError> {
        let file_size = layout.size()?;
        if size == 0 || offset >= file_size {
            return Ok(Bytes::new());
        }

        let blob_guid = layout.blob_guid()?;
        let block_size = layout.block_size as u64;
        let read_end = std::cmp::min(offset.saturating_add(size as u64), file_size);
        let actual_len = (read_end - offset) as usize;

        let first_block = (offset / block_size) as u32;
        let last_block = ((read_end - 1) / block_size) as u32;

        let trace_id = TraceId::new();

        // Fast path: single-block read can return a zero-copy Bytes slice
        if first_block == last_block {
            let block_num = first_block;
            let block_start = block_num as u64 * block_size;
            let block_content_len = std::cmp::min(block_size, file_size - block_start) as usize;

            let block_data = self
                .read_block_cached(
                    blob_guid,
                    block_num,
                    block_content_len,
                    file_size,
                    &trace_id,
                )
                .await?;

            let slice_start = (offset - block_start) as usize;
            let slice_end = std::cmp::min((read_end - block_start) as usize, block_data.len());
            return Ok(block_data.slice(slice_start..slice_end));
        }

        // Multi-block read: assemble from multiple blocks
        let mut result = BytesMut::with_capacity(actual_len);

        for block_num in first_block..=last_block {
            let block_start = block_num as u64 * block_size;
            let block_content_len = std::cmp::min(block_size, file_size - block_start) as usize;

            let block_data = self
                .read_block_cached(
                    blob_guid,
                    block_num,
                    block_content_len,
                    file_size,
                    &trace_id,
                )
                .await?;

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
                result.extend_from_slice(&block_data[slice_start..end]);
            }
        }

        Ok(result.freeze())
    }

    async fn read_mpu(
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

    // ── Zero-copy read helpers (direct-to-buffer) ──

    /// Read a cached block directly into `buf`. Returns bytes written on hit,
    /// or `None` on cache miss (caller should fall back to the Bytes path).
    async fn read_block_cached_into(
        &self,
        blob_guid: data_types::DataBlobGuid,
        block_num: u32,
        file_size: u64,
        buf: &mut [u8],
    ) -> Option<usize> {
        if let Some(dc) = &self.disk_cache {
            dc.get_into(
                blob_guid.blob_id,
                blob_guid.volume_id,
                block_num,
                file_size,
                buf,
            )
            .await
        } else {
            None
        }
    }

    /// Read a normal (non-MPU) object directly into a buffer.
    /// Returns the number of bytes written, or falls back to the Bytes path
    /// on any cache miss.
    async fn read_normal_buf(
        &self,
        layout: &ObjectLayout,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize, FsError> {
        let file_size = layout.size()?;
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
                        block_num,
                        file_size,
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
                    .read_block_cached_into(blob_guid, block_num, file_size, &mut tmp)
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
                    .read_block_cached(blob_guid, bn, bcl, file_size, &trace_id)
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
    pub async fn vfs_read(&self, fh: u64, offset: u64, buf: &mut [u8]) -> Result<usize, FsError> {
        let handle = self.file_handles.get(&fh).ok_or(FsError::BadFd)?;

        // Dirty write buffer: copy from it
        if let Some(ref wb) = handle.write_buf
            && wb.dirty
        {
            let buf_len = wb.data.len() as u64;
            if offset >= buf_len {
                return Ok(0);
            }
            let end = std::cmp::min(offset + buf.len() as u64, buf_len) as usize;
            let src = &wb.data[offset as usize..end];
            buf[..src.len()].copy_from_slice(src);
            return Ok(src.len());
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

    // ── Write helpers ──

    async fn flush_write_buffer(&self, fh_id: u64) -> Result<(), FsError> {
        let (s3_key, data) = {
            let mut handle = self.file_handles.get_mut(&fh_id).ok_or(FsError::BadFd)?;
            let s3_key = handle.s3_key.clone();
            let wb = match &mut handle.write_buf {
                Some(wb) if wb.dirty => wb,
                _ => return Ok(()),
            };
            (s3_key, wb.data.split().freeze())
        };

        let trace_id = TraceId::new();
        let blob_guid = self.backend().create_blob_guid();
        let block_size = DEFAULT_BLOCK_SIZE as usize;

        // Write data blocks
        let num_blocks = if data.is_empty() {
            0
        } else {
            data.len().div_ceil(block_size)
        };
        for block_i in 0..num_blocks {
            let start = block_i * block_size;
            let end = std::cmp::min(start + block_size, data.len());
            let chunk = data.slice(start..end);
            self.backend()
                .write_block(blob_guid, block_i as u32, chunk, &trace_id)
                .await?;
        }

        // Build ObjectLayout
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let layout = ObjectLayout {
            version_id: ObjectLayout::gen_version_id(),
            block_size: DEFAULT_BLOCK_SIZE,
            timestamp,
            state: ObjectState::Normal(ObjectMetaData {
                blob_guid,
                core_meta_data: ObjectCoreMetaData {
                    size: data.len() as u64,
                    etag: blob_guid.blob_id.simple().to_string(),
                    headers: vec![],
                    checksum: None,
                },
            }),
        };

        // Serialize layout
        let layout_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())
            .map_err(FsError::from)?
            .into();

        // Put inode in NSS, get old object bytes
        let old_bytes = self
            .backend()
            .put_inode(&s3_key, layout_bytes, &trace_id)
            .await?;

        // Delete old blob blocks if there was a previous version
        if !old_bytes.is_empty()
            && let Ok(old_layout) =
                rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
        {
            self.backend()
                .delete_blob_blocks(&old_layout, &trace_id)
                .await;
        }

        // Update file handle with new layout and clear dirty flag
        if let Some(mut handle) = self.file_handles.get_mut(&fh_id) {
            handle.layout = Some(layout.clone());
            if let Some(ref mut wb) = handle.write_buf {
                wb.dirty = false;
            }
        }

        // Update inode table layout
        {
            let handle = self.file_handles.get(&fh_id);
            if let Some(handle) = handle
                && let Some(mut entry) = self.inodes.get_mut(handle.ino)
            {
                entry.layout = Some(layout);
            }
        }

        // Invalidate dir cache for parent prefix
        let parent_prefix = parent_prefix_of(&s3_key);
        self.dir_cache.invalidate(&parent_prefix);

        Ok(())
    }

    async fn fetch_dir_entries(
        &self,
        parent: u64,
        prefix: &str,
    ) -> Result<Arc<Vec<DirEntry>>, FsError> {
        if let Some(cached) = self.dir_cache.get(prefix) {
            let stale = cached
                .iter()
                .any(|entry| self.inodes.get(entry.ino).is_none());
            if !stale {
                return Ok(cached);
            }
            tracing::debug!(%prefix, "Directory cache contains stale inode(s), rebuilding");
            self.dir_cache.invalidate(prefix);
        }

        let trace_id = TraceId::new();
        let mut all_entries = Vec::new();

        // Resolve parent-of-parent inode for ".." entry.
        // For root ("/") or top-level dirs, parent-of-parent is root.
        let dotdot_ino = if parent == ROOT_INODE {
            ROOT_INODE
        } else {
            let trimmed = prefix.trim_end_matches('/');
            match trimmed.rfind('/') {
                Some(pos) => {
                    let parent_key = &prefix[..=pos];
                    if parent_key == "/" {
                        ROOT_INODE
                    } else {
                        let (ino, _) =
                            self.inodes
                                .lookup_or_insert(parent_key, EntryType::Directory, None);
                        ino
                    }
                }
                None => ROOT_INODE,
            }
        };

        all_entries.push(DirEntry {
            name: ".".to_string(),
            ino: parent,
            is_dir: true,
        });
        all_entries.push(DirEntry {
            name: "..".to_string(),
            ino: dotdot_ino,
            is_dir: true,
        });

        let mut start_after = String::new();
        loop {
            let entries = self
                .backend()
                .list_inodes(prefix, "/", &start_after, 1000, &trace_id)
                .await?;

            if entries.is_empty() {
                break;
            }

            let last_key = entries.last().map(|e| e.key.clone());

            for entry in entries {
                let raw_key = &entry.key;

                let name = if raw_key.len() >= prefix.len() {
                    &raw_key[prefix.len()..]
                } else {
                    raw_key.as_str()
                };

                if entry.layout.is_none() {
                    // Directory (common prefix)
                    let dir_name = name.trim_end_matches('/');
                    if dir_name.is_empty() {
                        continue;
                    }
                    let dir_key = raw_key.clone();
                    let (ino, _) =
                        self.inodes
                            .lookup_or_insert(&dir_key, EntryType::Directory, None);
                    all_entries.push(DirEntry {
                        name: dir_name.to_string(),
                        ino,
                        is_dir: true,
                    });
                } else {
                    // File - backend already stripped trailing \0 from keys
                    let layout = entry.layout.as_ref().unwrap();
                    if !layout.is_listable() {
                        continue;
                    }
                    if name.is_empty() {
                        continue;
                    }
                    let (ino, _) =
                        self.inodes
                            .lookup_or_insert(raw_key, EntryType::File, entry.layout);
                    all_entries.push(DirEntry {
                        name: name.to_string(),
                        ino,
                        is_dir: false,
                    });
                }
            }

            if let Some(last) = last_key {
                start_after = last;
            } else {
                break;
            }
        }

        let entries = Arc::new(all_entries);
        self.dir_cache.insert(prefix.to_string(), entries.clone());
        Ok(entries)
    }

    // ── Public VFS operations ──

    pub fn vfs_init(&self) {
        if let Some(dc) = &self.disk_cache {
            dc.spawn_evictor();
        }
        tracing::info!("Filesystem initialized");
    }

    pub fn vfs_destroy(&self) {
        tracing::info!("Filesystem destroyed");
    }

    pub async fn vfs_lookup(&self, parent: u64, name: &str) -> Result<VfsAttr, FsError> {
        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;

        let full_key = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}{}", prefix, name)
        };

        let trace_id = TraceId::new();

        // Try as file first
        match self.backend().get_inode(&full_key, &trace_id).await {
            Ok(layout) => {
                if !layout.is_listable() {
                    return Err(FsError::NotFound);
                }
                let (ino, _) =
                    self.inodes
                        .lookup_or_insert(&full_key, EntryType::File, Some(layout.clone()));
                return self.make_file_attr(ino, &layout);
            }
            Err(FsError::NotFound) => {}
            Err(e) => return Err(e),
        }

        // Try as directory
        let dir_key = format!("{}/", full_key);
        let entries = self
            .backend()
            .list_inodes(&dir_key, "/", "", 1, &trace_id)
            .await;

        match entries {
            Ok(entries) if !entries.is_empty() => {
                let (ino, _) = self
                    .inodes
                    .lookup_or_insert(&dir_key, EntryType::Directory, None);
                Ok(self.make_dir_attr(ino))
            }
            _ => Err(FsError::NotFound),
        }
    }

    pub fn vfs_forget(&self, inode: u64, nlookup: u64) {
        self.inodes.forget(inode, nlookup);
    }

    pub async fn vfs_getattr(&self, inode: u64, fh: Option<u64>) -> Result<VfsAttr, FsError> {
        if inode == ROOT_INODE {
            return Ok(self.make_dir_attr(ROOT_INODE));
        }

        // If there's an open write handle with a dirty buffer, report its size
        if let Some(fh_id) = fh
            && let Some(handle) = self.file_handles.get(&fh_id)
            && let Some(ref wb) = handle.write_buf
            && wb.dirty
        {
            return Ok(self.make_new_file_attr(inode, wb.data.len() as u64));
        }

        let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;

        match entry.entry_type {
            EntryType::Directory => Ok(self.make_dir_attr(inode)),
            EntryType::File => {
                if let Some(ref layout) = entry.layout {
                    self.make_file_attr(inode, layout)
                } else {
                    let key = entry.s3_key.clone();
                    drop(entry);
                    let trace_id = TraceId::new();
                    let layout = self.backend().get_inode(&key, &trace_id).await?;
                    let attr = self.make_file_attr(inode, &layout)?;
                    if let Some(mut entry) = self.inodes.get_mut(inode) {
                        entry.layout = Some(layout);
                    }
                    Ok(attr)
                }
            }
        }
    }

    /// Handle size changes via setattr (truncate, extend, or truncate-to-zero).
    pub async fn vfs_setattr_size(
        &self,
        inode: u64,
        fh: u64,
        new_size: u64,
    ) -> Result<VfsAttr, FsError> {
        let mut handle = self.file_handles.get_mut(&fh).ok_or(FsError::BadFd)?;
        let wb = handle.write_buf.get_or_insert_with(|| WriteBuffer {
            data: BytesMut::new(),
            dirty: false,
        });
        let new_size = new_size as usize;
        if new_size != wb.data.len() {
            wb.data.resize(new_size, 0);
            wb.dirty = true;
        }
        Ok(self.make_new_file_attr(inode, new_size as u64))
    }

    pub async fn vfs_open(&self, inode: u64, flags: u32) -> Result<u64, FsError> {
        let write_flags = libc::O_WRONLY as u32
            | libc::O_RDWR as u32
            | libc::O_APPEND as u32
            | libc::O_TRUNC as u32;
        let is_write = flags & write_flags != 0;

        if is_write && !self.read_write {
            return Err(FsError::ReadOnly);
        }

        let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;

        if entry.entry_type != EntryType::File {
            return Err(FsError::IsDir);
        }

        let s3_key = entry.s3_key.clone();
        let layout = entry.layout.clone();
        drop(entry);

        // Resolve layout if not cached
        let layout = match layout {
            Some(l) => Some(l),
            None => {
                let trace_id = TraceId::new();
                match self.backend().get_inode(&s3_key, &trace_id).await {
                    Ok(l) => Some(l),
                    Err(FsError::NotFound) if is_write => None,
                    Err(e) => return Err(e),
                }
            }
        };

        let has_trunc = flags & libc::O_TRUNC as u32 != 0;
        let write_buf = if is_write {
            if let Some(ref l) = layout
                && !has_trunc
            {
                // Existing file without truncate: preload content so partial
                // writes don't lose surrounding data
                let data = self.preload_file_content(&s3_key, l).await?;
                Some(WriteBuffer { data, dirty: false })
            } else {
                // O_TRUNC or new file: start empty
                Some(WriteBuffer {
                    data: BytesMut::new(),
                    dirty: false,
                })
            }
        } else {
            None
        };

        let fh = self.alloc_fh();
        self.file_handles.insert(
            fh,
            FileHandle {
                ino: inode,
                s3_key,
                layout,
                write_buf,
                backing_id: None,
            },
        );

        Ok(fh)
    }

    /// Read data from an open file handle, returning owned Bytes.
    /// Used by NFS path (vfs_read_by_ino) which needs owned data.
    async fn vfs_read_bytes(&self, fh: u64, offset: u64, size: u32) -> Result<Bytes, FsError> {
        let handle = self.file_handles.get(&fh).ok_or(FsError::BadFd)?;

        // If there's a dirty write buffer, read from it
        if let Some(ref wb) = handle.write_buf
            && wb.dirty
        {
            let buf_len = wb.data.len() as u64;
            if offset >= buf_len {
                return Ok(Bytes::new());
            }
            let end = std::cmp::min(offset + size as u64, buf_len) as usize;
            let data = Bytes::copy_from_slice(&wb.data[offset as usize..end]);
            return Ok(data);
        }

        let s3_key = handle.s3_key.clone();
        let layout = match &handle.layout {
            Some(l) => l.clone(),
            None => return Ok(Bytes::new()),
        };
        drop(handle);

        match &layout.state {
            ObjectState::Normal(_) => self.read_normal(&layout, offset, size).await,
            ObjectState::Mpu(MpuState::Completed(_)) => {
                self.read_mpu(&s3_key, &layout, offset, size).await
            }
            _ => Err(FsError::InvalidState),
        }
    }

    pub async fn vfs_write(&self, fh: u64, offset: u64, data: &[u8]) -> Result<u32, FsError> {
        let mut handle = self.file_handles.get_mut(&fh).ok_or(FsError::BadFd)?;

        let wb = handle.write_buf.get_or_insert_with(|| WriteBuffer {
            data: BytesMut::new(),
            dirty: false,
        });

        let needed = offset as usize + data.len();
        if needed > wb.data.len() {
            wb.data.resize(needed, 0);
        }
        wb.data[offset as usize..offset as usize + data.len()].copy_from_slice(data);
        wb.dirty = true;

        Ok(data.len() as u32)
    }

    pub async fn vfs_flush(&self, fh: u64) -> Result<(), FsError> {
        self.flush_write_buffer(fh).await
    }

    pub async fn vfs_release(&self, fh: u64) -> Result<(), FsError> {
        // Flush any dirty write buffer before releasing
        let has_dirty = self
            .file_handles
            .get(&fh)
            .map(|h| h.write_buf.as_ref().map(|wb| wb.dirty).unwrap_or(false))
            .unwrap_or(false);

        if has_dirty {
            self.flush_write_buffer(fh).await?;
        }

        // Get the inode before removing the handle
        let ino = self.file_handles.get(&fh).map(|h| h.ino);
        self.file_handles.remove(&fh);

        // Handle deferred blob cleanup for unlinked files
        if let Some(ino) = ino
            && let Some((_, old_bytes)) = self.deferred_blob_cleanup.remove(&ino)
        {
            if !self.has_open_handles_for_inode(ino, None) {
                // Last handle closed, clean up blobs now
                let trace_id = TraceId::new();
                if let Ok(old_layout) =
                    rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
                {
                    self.backend()
                        .delete_blob_blocks(&old_layout, &trace_id)
                        .await;
                }
            } else {
                // Still more handles open, re-insert
                self.deferred_blob_cleanup.insert(ino, old_bytes);
            }
        }

        Ok(())
    }

    pub async fn vfs_create(&self, parent: u64, name: &str) -> Result<(VfsAttr, u64), FsError> {
        self.check_write_enabled()?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        let key = format!("{}{}", prefix, name);

        let (ino, _) = self.inodes.lookup_or_insert(&key, EntryType::File, None);

        let fh = self.alloc_fh();
        self.file_handles.insert(
            fh,
            FileHandle {
                ino,
                s3_key: key,
                layout: None,
                write_buf: Some(WriteBuffer {
                    data: BytesMut::new(),
                    dirty: true,
                }),
                backing_id: None,
            },
        );

        let attr = self.make_new_file_attr(ino, 0);

        // Invalidate dir cache so the new file shows up in listings
        self.dir_cache.invalidate(&prefix);

        Ok((attr, fh))
    }

    pub async fn vfs_unlink(&self, parent: u64, name: &str) -> Result<(), FsError> {
        self.check_write_enabled()?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        let key = format!("{}{}", prefix, name);

        let trace_id = TraceId::new();

        // Delete the inode from NSS
        let old_bytes = self.backend().delete_inode(&key, &trace_id).await?;

        // Return ENOENT if file didn't exist
        let old_bytes = old_bytes.ok_or(FsError::NotFound)?;

        // Remove name mapping from inode table (read-only lookup, no refcount leak)
        let ino = self.inodes.find_ino_by_key(&key, EntryType::File);
        if let Some(ino) = ino {
            self.inodes.remove_name_mapping(ino);
        }

        // Handle blob cleanup: defer if file has open handles
        if !old_bytes.is_empty() {
            if let Some(ino) = ino
                && self.has_open_handles_for_inode(ino, None)
            {
                // Defer blob cleanup until last handle is released
                self.deferred_blob_cleanup.insert(ino, old_bytes);
            } else if let Ok(old_layout) =
                rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
            {
                match &old_layout.state {
                    ObjectState::Normal(_) => {
                        self.backend()
                            .delete_blob_blocks(&old_layout, &trace_id)
                            .await;
                    }
                    ObjectState::Mpu(MpuState::Completed(_)) => {
                        if let Ok(parts) = self.backend().list_mpu_parts(&key, &trace_id).await {
                            for (part_key, part_layout) in &parts {
                                self.backend()
                                    .delete_blob_blocks(part_layout, &trace_id)
                                    .await;
                                let _ = self.backend().delete_inode(part_key, &trace_id).await;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // Invalidate dir cache for parent
        self.dir_cache.invalidate(&prefix);

        Ok(())
    }

    pub async fn vfs_mkdir(&self, parent: u64, name: &str) -> Result<VfsAttr, FsError> {
        self.check_write_enabled()?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        let key = format!("{}{}/", prefix, name);

        let trace_id = TraceId::new();
        self.backend().put_dir_marker(&key, &trace_id).await?;

        let (ino, _) = self
            .inodes
            .lookup_or_insert(&key, EntryType::Directory, None);

        // Invalidate dir cache for parent
        self.dir_cache.invalidate(&prefix);

        Ok(self.make_dir_attr(ino))
    }

    pub async fn vfs_rmdir(&self, parent: u64, name: &str) -> Result<(), FsError> {
        self.check_write_enabled()?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        let key = format!("{}{}/", prefix, name);

        let trace_id = TraceId::new();

        // List to check existence and emptiness
        let entries = self
            .backend()
            .list_inodes(&key, "/", "", 2, &trace_id)
            .await?;

        // If no entries at all, directory doesn't exist
        if entries.is_empty() {
            return Err(FsError::NotFound);
        }

        let has_children = entries.iter().any(|e| e.key != key);
        if has_children {
            return Err(FsError::NotEmpty);
        }

        // Delete the directory marker
        self.backend().delete_inode(&key, &trace_id).await?;

        // Remove from inode table (read-only lookup, no refcount leak)
        if let Some(ino) = self.inodes.find_ino_by_key(&key, EntryType::Directory) {
            self.inodes.remove_name_mapping(ino);
        }

        // Invalidate dir cache for parent and self
        self.dir_cache.invalidate(&prefix);
        self.dir_cache.invalidate(&key);

        Ok(())
    }

    pub async fn vfs_rename(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
    ) -> Result<(), FsError> {
        self.check_write_enabled()?;

        let src_prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        let dst_prefix = self.dir_prefix(new_parent).ok_or(FsError::NotFound)?;

        let src_key = format!("{}{}", src_prefix, name);
        let dst_key = format!("{}{}", dst_prefix, new_name);

        let trace_id = TraceId::new();

        // Determine type by probing NSS backend directly (no inode side effects)
        let is_dir = match self.backend().get_inode(&src_key, &trace_id).await {
            Ok(_) => false,
            Err(FsError::NotFound) => true,
            Err(e) => return Err(e),
        };

        if is_dir {
            let src_dir_key = format!("{}/", src_key);
            let dst_dir_key = format!("{}/", dst_key);

            self.backend()
                .rename_folder(&src_dir_key, &dst_dir_key, &trace_id)
                .await?;

            // Update the directory inode's s3_key since the kernel still
            // holds a reference to it after rename.
            if let Some(ino) = self
                .inodes
                .find_ino_by_key(&src_dir_key, EntryType::Directory)
            {
                self.inodes.update_s3_key(ino, &dst_dir_key);
            }

            // Update cached child inodes to reflect the new prefix so the
            // kernel's existing inode references remain valid.
            self.inodes.rename_children(&src_dir_key, &dst_dir_key);

            self.dir_cache.invalidate(&src_prefix);
            self.dir_cache.invalidate(&dst_prefix);
            self.dir_cache.invalidate(&src_dir_key);
        } else {
            self.backend()
                .rename_file(&src_key, &dst_key, &trace_id)
                .await?;

            // Update inode s3_key if cached (read-only lookup, no refcount leak)
            if let Some(ino) = self.inodes.find_ino_by_key(&src_key, EntryType::File) {
                self.inodes.update_s3_key(ino, &dst_key);
            }

            // Update any open file handles to reflect the new key
            for mut fh_entry in self.file_handles.iter_mut() {
                if fh_entry.value().s3_key == src_key {
                    fh_entry.value_mut().s3_key = dst_key.clone();
                }
            }

            self.dir_cache.invalidate(&src_prefix);
            self.dir_cache.invalidate(&dst_prefix);
        }

        Ok(())
    }

    pub fn vfs_opendir(&self, inode: u64) -> Result<u64, FsError> {
        if inode != ROOT_INODE {
            let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
            if entry.entry_type != EntryType::Directory {
                return Err(FsError::NotDir);
            }
        }

        Ok(self.alloc_fh())
    }

    pub async fn vfs_readdir(&self, parent: u64, offset: u64) -> Result<Vec<VfsDirEntry>, FsError> {
        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        let dir_entries = self.fetch_dir_entries(parent, &prefix).await?;

        let offset = offset as usize;
        let entries = dir_entries
            .iter()
            .skip(offset)
            .enumerate()
            .map(|(idx, entry)| VfsDirEntry {
                ino: entry.ino,
                is_dir: entry.is_dir,
                name: entry.name.clone(),
                offset: (offset + idx + 1) as u64,
            })
            .collect();

        Ok(entries)
    }

    pub async fn vfs_readdirplus(
        &self,
        parent: u64,
        offset: u64,
    ) -> Result<Vec<VfsDirEntryPlus>, FsError> {
        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        let dir_entries = self.fetch_dir_entries(parent, &prefix).await?;

        let offset = offset as usize;
        let entries: Result<Vec<VfsDirEntryPlus>, FsError> = dir_entries
            .iter()
            .skip(offset)
            .enumerate()
            .map(|(idx, entry)| {
                let attr = if entry.is_dir {
                    self.make_dir_attr(entry.ino)
                } else {
                    self.inodes
                        .get(entry.ino)
                        .and_then(|e| e.layout.as_ref().map(|l| self.make_file_attr(entry.ino, l)))
                        .transpose()?
                        .unwrap_or_else(|| self.make_default_file_attr(entry.ino))
                };
                Ok(VfsDirEntryPlus {
                    ino: entry.ino,
                    is_dir: entry.is_dir,
                    name: entry.name.clone(),
                    offset: (offset + idx + 1) as u64,
                    attr,
                })
            })
            .collect();

        entries
    }

    /// Stateless read by inode (for NFS). Opens, reads, and releases in one call.
    pub async fn vfs_read_by_ino(
        &self,
        inode: u64,
        offset: u64,
        count: u32,
    ) -> Result<Bytes, FsError> {
        let fh = self.vfs_open(inode, libc::O_RDONLY as u32).await?;
        let result = self.vfs_read_bytes(fh, offset, count).await;
        let _ = self.vfs_release(fh).await;
        result
    }

    /// Stateless write by inode (for NFS). Opens, writes, flushes, and releases.
    pub async fn vfs_write_by_ino(
        &self,
        inode: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<u32, FsError> {
        let fh = self.vfs_open(inode, libc::O_WRONLY as u32).await?;
        let result = self.vfs_write(fh, offset, data).await;
        if result.is_ok() {
            let _ = self.vfs_flush(fh).await;
        }
        let _ = self.vfs_release(fh).await;
        result
    }

    /// Evict stale inodes that have no open file handles. For NFS mode where
    /// there is no FUSE FORGET mechanism.
    pub fn vfs_evict_stale_inodes(&self, ttl: Duration) {
        let evicted = self.inodes.evict_stale(ttl);
        // Re-insert any inodes that still have open file handles
        for ino in &evicted {
            if self.has_open_handles_for_inode(*ino, None) {
                // The inode was evicted but still has open handles.
                // The handle holds its own s3_key/layout, so NFS ops
                // in flight will still work. New lookups will re-create
                // the inode entry.
                tracing::debug!(ino = ino, "skipped eviction: open handles");
            }
        }
        if !evicted.is_empty() {
            tracing::debug!(count = evicted.len(), "evicted stale inodes");
        }
    }

    // ── FUSE cache invalidation ──

    /// Invalidate a specific directory entry in the kernel dcache.
    /// Called when we learn about a remote rename/delete/create.
    pub fn invalidate_entry(&self, parent_s3_key: &str, name: &str) {
        let Some(notifier) = self.notifier.get() else {
            return;
        };
        // Look up parent inode
        let parent_ino = self
            .inodes
            .find_ino_by_key(parent_s3_key, EntryType::Directory)
            .unwrap_or(ROOT_INODE);

        if let Err(e) = notifier.inval_entry(parent_ino, name.as_bytes()) {
            tracing::warn!(
                parent = parent_s3_key,
                name,
                error = %e,
                "FUSE inval_entry failed"
            );
        }
        // Also invalidate internal DirCache
        self.dir_cache.invalidate(parent_s3_key);
    }

    /// Invalidate all cached entries under a directory prefix.
    /// Only invalidates the parent directory inode (dropping readdir
    /// page cache), NOT individual child entries, to avoid thundering herd.
    pub fn invalidate_dir(&self, dir_s3_key: &str) {
        let Some(notifier) = self.notifier.get() else {
            return;
        };
        // Look up dir inode
        if let Some(dir_ino) = self
            .inodes
            .find_ino_by_key(dir_s3_key, EntryType::Directory)
        {
            // Invalidate inode attrs + page cache (readdir results)
            if let Err(e) = notifier.inval_inode(dir_ino, -1, -1) {
                tracing::warn!(
                    dir = dir_s3_key,
                    ino = dir_ino,
                    error = %e,
                    "FUSE inval_inode failed"
                );
            }
        }
        // Invalidate DirCache
        self.dir_cache.invalidate(dir_s3_key);
    }

    /// Invalidate inode attributes + page cache for a file.
    /// Used when file content changes remotely.
    pub fn invalidate_inode(&self, s3_key: &str) {
        let Some(notifier) = self.notifier.get() else {
            return;
        };
        if let Some(ino) = self.inodes.find_ino_by_key(s3_key, EntryType::File)
            && let Err(e) = notifier.inval_inode(ino, -1, -1)
        {
            tracing::warn!(
                key = s3_key,
                ino,
                error = %e,
                "FUSE inval_inode failed"
            );
        }
        // Evict from DiskCache if present
        // (DiskCache uses blob_id/volume_id, not s3_key, so we'd need the
        // layout. For now, invalidating the inode is sufficient -- the kernel
        // will re-fetch on next access and the disk cache will be bypassed
        // when the layout's ETag changes.)
    }

    pub fn vfs_statfs(&self) -> VfsStatfs {
        VfsStatfs {
            blocks: 1024 * 1024,
            bfree: if self.read_write { 512 * 1024 } else { 0 },
            bavail: if self.read_write { 512 * 1024 } else { 0 },
            files: 1024 * 1024,
            ffree: if self.read_write { 512 * 1024 } else { 0 },
            bsize: DEFAULT_BLOCK_SIZE,
            namelen: 1024,
            frsize: DEFAULT_BLOCK_SIZE,
        }
    }
}

/// Extract the parent prefix from an s3_key.
/// e.g. "/foo/bar" -> "/foo/", "/top" -> "/"
fn parent_prefix_of(key: &str) -> String {
    let trimmed = key.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(pos) => trimmed[..=pos].to_string(),
        None => "/".to_string(),
    }
}

fn file_mode(perm: u16) -> u32 {
    libc::S_IFREG | perm as u32
}

fn dir_mode(perm: u16) -> u32 {
    libc::S_IFDIR | perm as u32
}
