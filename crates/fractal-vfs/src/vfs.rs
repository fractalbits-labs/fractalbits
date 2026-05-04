use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use data_types::TraceId;
use rkyv::api::high::to_bytes_in;
use std::cell::Cell;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::backend::{BackendConfig, StorageBackend};
use crate::cache::{DirCache, DirEntry};
use crate::disk_cache::DiskCache;
use crate::error::FsError;
use crate::inode::{EntryType, InodeTable, ROOT_INODE};
use data_types::object_layout::{
    MpuState, ObjectCoreMetaData, ObjectLayout, ObjectMetaData, ObjectState,
};
pub const TTL: Duration = Duration::from_secs(1);
pub const DEFAULT_BLOCK_SIZE: u32 = 128 * 1024;

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

/// Per-block content intent for the sparse WriteBuffer.
///
/// Blocks NOT in the map are implicitly "Keep": no buffered work, BSS is
/// authoritative. Flush still goes through the legacy replace-on-flush
/// path -- the sparse buffer's role today is to keep in-memory ops O(1),
/// avoid whole-file preload on open, and serve dirty-handle reads
/// per-block. Override flush wires up once the BSS/NSS protocol changes
/// land.
#[derive(Debug, Clone)]
enum BlockState {
    /// Bytes lazily loaded from BSS for read or partial-block edit.
    /// Flush does not upload these; they exist so reads and RMW can avoid
    /// re-fetching from BSS within the same handle session.
    ///
    /// Currently unused -- reserved for the read-side caching optimization
    /// that materializes once `BlockNotFound -> zeros` lands; today we
    /// always re-fetch on dirty-handle reads.
    #[allow(dead_code)]
    Cached(Bytes),
    /// Definitive bytes for this block. Origin: vfs_write or shrink
    /// tail-zero. The current flush path materializes these into the
    /// contiguous buffer it hands to replace-on-flush.
    Rewrite(Bytes),
    /// PUNCH_HOLE intent: schedule a versioned `delete_block` at flush
    /// time so the BSS entry is dropped at the new blob_version. Reads
    /// (dirty-handle merge and post-flush) treat the block as zeros via
    /// `BlockNotFound`. Distinguished from a plain hole because a punched
    /// block sits inside the file's logical range and the deletion must
    /// be replayed on flush even if it has no Rewrite content.
    Delete,
}

struct WriteBuffer {
    /// Logical file size (includes holes). Authoritative within this
    /// handle session for stat / read clamping until flush commits.
    file_size: u64,
    /// True if `file_size` differs from the committed layout.size at
    /// open time, or if any block intent is `Rewrite`. Used as the
    /// flush-eligibility predicate.
    size_changed: bool,
    /// Block guid of the file at open time, used by `ensure_loaded` to
    /// lazy-load committed bytes for partial-block edits and dirty reads.
    /// `None` for brand-new files.
    existing_blob_guid: Option<data_types::DataBlobGuid>,
    /// Block size copied from the committed layout (or DEFAULT for new
    /// files), so the buffer can be reasoned about without holding
    /// `handle.layout` for every operation.
    block_size: u32,
    /// Per-block content intents. Keyed by block index.
    blocks: std::collections::BTreeMap<u32, BlockState>,
    /// True if any flush-worthy work is buffered.
    dirty: bool,
    /// Smallest `ceil(new_size / block_size)` reached by any shrink in
    /// this buffer session. Blocks at index `>= eof_low_watermark` had
    /// their committed BSS data logically destroyed by the shrink and
    /// must read as zeros until the flush trim deletes them, even if a
    /// later grow brings the index back into the file. Reset to `None`
    /// only on a successful flush. Without this guard a
    /// `truncate(small); write(past old EOF)` would lazy-load the
    /// pre-shrink BSS bytes and merge user data on top, resurrecting
    /// bytes POSIX requires to be zero.
    eof_low_watermark: Option<u32>,
    /// `committed_block_count` pinned at the FIRST shrink in this
    /// buffer session. Pairs with `eof_low_watermark` to bound the
    /// EOF-trim range across post-CAS-failure retries: step 5a of the
    /// flush promotes `handle.layout.size` to the smaller new size, so
    /// recomputing the upper bound from `handle.layout` on retry would
    /// lose the original committed bound. Reset to `None` only on a
    /// successful flush.
    trim_upper: Option<u32>,
    /// Block indices that fallocate has reserved. The set lives only
    /// on the client -- there is no backing BSS reservation entry yet,
    /// so on flush a reserved-but-unwritten block is materialised the
    /// same way a hole is (absent from BSS, read as zero). Reads and
    /// `lseek(SEEK_DATA)` treat reserved blocks as logical-data per
    /// Linux convention even before flush; once a write replaces the
    /// reservation, the reservation entry is removed.
    pending_reservations: std::collections::BTreeSet<u32>,
}

impl WriteBuffer {
    fn new(
        existing_blob_guid: Option<data_types::DataBlobGuid>,
        file_size: u64,
        block_size: u32,
    ) -> Self {
        Self {
            file_size,
            size_changed: false,
            existing_blob_guid,
            block_size,
            blocks: std::collections::BTreeMap::new(),
            dirty: false,
            eof_low_watermark: None,
            trim_upper: None,
            pending_reservations: std::collections::BTreeSet::new(),
        }
    }

    /// Drop any per-block intents past the new EOF. Called by shrink.
    /// Also drops pending reservations past the new EOF -- a shrink
    /// supersedes any fallocate reservation that landed on a block the
    /// file no longer covers.
    fn drop_blocks_past(&mut self, new_last_block_excl: u32) {
        self.blocks.retain(|b, _| *b < new_last_block_excl);
        self.pending_reservations
            .retain(|b| *b < new_last_block_excl);
    }

    /// Returns true when block index `b` sits in a range whose committed
    /// BSS bytes were destroyed by a shrink earlier in this buffer
    /// session. Lazy-load and dirty-read paths must return zeros for
    /// such blocks instead of consulting BSS, otherwise
    /// `truncate(small); write_at(re-extended block)` would resurrect
    /// pre-shrink bytes.
    fn block_destroyed_by_shrink(&self, b: u32) -> bool {
        self.eof_low_watermark.is_some_and(|low| b >= low)
    }
}

struct FileHandle {
    ino: u64,
    s3_key: String,
    layout: Option<ObjectLayout>,
    /// Bytes the NSS handed back for this inode at the most recent
    /// successful read or successful CAS write. Used as
    /// `expected_old_value` on the next override-flush CAS so a
    /// concurrent cross-instance writer fails the guard instead of
    /// silently winning the race. `None` for brand-new files (initial
    /// create uses unconditional `put_inode`) and for read-only handles
    /// that never need it.
    layout_bytes: Option<Bytes>,
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
    fuse_dev_fd: Option<Arc<OwnedFd>>,
    // Tracks blob data for unlinked files that still have open handles.
    // Cleanup is deferred until the last handle is released.
    deferred_blob_cleanup: DashMap<u64, Bytes>,
    // Inode-scoped write lock. At most one write-mode handle per inode
    // is allowed. The map value is the owning fh, so a stale lock from
    // a handle that disappeared without going through release can be
    // reclaimed by the next opener. Reads do not touch this lock.
    inode_write_owner: DashMap<u64, u64>,
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
            fuse_dev_fd: None,
            deferred_blob_cleanup: DashMap::new(),
            inode_write_owner: DashMap::new(),
        }
    }

    /// Install the shared `/dev/fuse` fd, obtained from
    /// `Session::fuse_fd()`, before the session is run. FUSE-mode only;
    /// NFS mode never calls this. The fd is needed by passthrough open /
    /// close paths that may fire on the very first FUSE request.
    pub fn with_fuse_fd(mut self, fuse_dev_fd: Arc<OwnedFd>) -> Self {
        self.fuse_dev_fd = Some(fuse_dev_fd);
        self
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

    /// Acquire the inode-scoped write lock for `fh`. Returns `Busy` if another
    /// write-mode handle currently owns it.
    ///
    /// Reclaim rule: if the recorded owner fh has been released (no entry in
    /// `file_handles`), the lock is stale and we take it. This recovers from
    /// any path that removes a handle without first calling
    /// `release_write_lock` (e.g. lookup races during shutdown).
    fn acquire_write_lock(&self, inode: u64, fh: u64) -> Result<(), FsError> {
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

    fn release_write_lock(&self, inode: u64, fh: u64) {
        self.inode_write_owner
            .remove_if(&inode, |_, owner| *owner == fh);
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

        let fuse_fd = match self.fuse_dev_fd.as_ref() {
            Some(fd) => fd.as_raw_fd(),
            None => return (0, 0),
        };

        // Open the cache file and register as backing fd
        let cache_path = dc.cache_file_path(blob_guid.blob_id, blob_guid.volume_id);
        let backing_file = match std::fs::File::open(&cache_path) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(error = %e, "failed to open cache file for passthrough");
                return (0, 0);
            }
        };

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

        if let Some(bid) = backing_id
            && let Some(fuse_dev_fd) = self.fuse_dev_fd.as_ref()
            && let Err(e) =
                fractal_fuse::passthrough::fuse_backing_close(fuse_dev_fd.as_raw_fd(), bid)
        {
            tracing::warn!(backing_id = bid, error = %e, "failed to close backing");
        }
    }

    // ── Cache helpers ──

    /// Read a block, checking disk cache first. On miss, fetches from backend
    /// and populates disk cache.
    ///
    /// A sparse-file hole (block legitimately missing on every replica)
    /// is surfaced as a synthetic zero-filled block of `block_content_len`.
    /// The disk cache is intentionally NOT populated for holes -- there's
    /// no checksum to validate against and a future override-flush could
    /// fill the hole with real data.
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
        let (data, checksum) = match self
            .backend()
            .read_block(blob_guid, block_num, block_content_len, trace_id)
            .await
        {
            Ok(r) => r,
            Err(FsError::DataVg(volume_group_proxy::DataVgError::BlockNotFound))
            | Err(FsError::Rpc(rpc_client_common::RpcError::NotFound)) => {
                return Ok(Bytes::from(vec![0u8; block_content_len]));
            }
            Err(e) => return Err(e),
        };

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

        // Dirty-handle read merge: a per-block path that mirrors
        // flush-time semantics. Read len is clamped to wb.file_size so
        // a buffered truncate / write-into-EOF is visible to
        // same-handle reads.
        if let Some(ref wb) = handle.write_buf
            && wb.dirty
        {
            let file_size = wb.file_size;
            let block_size = wb.block_size;
            let existing_blob_guid = wb.existing_blob_guid;
            let blocks = wb.blocks.clone();
            let eof_low_watermark = wb.eof_low_watermark;
            drop(handle);
            return self
                .read_dirty_handle(
                    file_size,
                    block_size,
                    existing_blob_guid,
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

    /// Per-block read merge for a dirty handle. Reads of buffered Rewrite
    /// or Cached blocks return those bytes; an absent block falls through
    /// to lazy-load from BSS, treating BlockNotFound as a hole. The total
    /// read length is clamped to `file_size` so a buffered truncate /
    /// extend is observable.
    #[allow(clippy::too_many_arguments)]
    async fn read_dirty_handle(
        &self,
        file_size: u64,
        block_size: u32,
        existing_blob_guid: Option<data_types::DataBlobGuid>,
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
                Some(BlockState::Rewrite(b2)) | Some(BlockState::Cached(b2)) => b2.clone(),
                Some(BlockState::Delete) => {
                    // Buffered PUNCH_HOLE: read as zeros for the same-handle
                    // dirty merge, matching what the post-flush read will
                    // see once the per-block delete lands.
                    Bytes::from(vec![0u8; block_content_len])
                }
                None => {
                    // Block destroyed by an earlier shrink in this
                    // session: POSIX requires zeros, so don't consult
                    // BSS even though re-extension brought the index
                    // back into the file.
                    if eof_low_watermark.is_some_and(|low| b >= low) {
                        Bytes::from(vec![0u8; block_content_len])
                    } else {
                        // Read against the buffered file_size: blocks past it
                        // shouldn't be reachable from this loop (we clamp on
                        // entry), so the committed length matches block_content_len.
                        self.lazy_load_block_for_flush(
                            existing_blob_guid,
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

    // ── Write helpers ──

    /// Build the contiguous byte stream that the existing replace-on-flush
    /// path needs out of the sparse WriteBuffer. Lazy-loads any block
    /// that is within the new file_size but not buffered.
    ///
    /// `committed_size` is the size BSS currently has under
    /// `existing_blob_guid`; it disambiguates a block that lives in BSS
    /// at its original length from a block that the buffer has shrunk
    /// or never written. The BSS read path enforces a strict
    /// content_len match, so a lazy-load for a block that the buffer is
    /// shrinking must request the original (committed) content length
    /// and truncate the result locally.
    async fn materialize_for_flush(
        &self,
        existing_blob_guid: Option<data_types::DataBlobGuid>,
        file_size: u64,
        committed_size: u64,
        block_size: u32,
        blocks: &std::collections::BTreeMap<u32, BlockState>,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        if file_size == 0 {
            return Ok(Bytes::new());
        }
        let bsz = block_size as u64;
        let block_count = file_size.div_ceil(bsz) as u32;
        let mut out = BytesMut::with_capacity(file_size as usize);
        for b in 0..block_count {
            let block_start = b as u64 * bsz;
            let new_content_len = std::cmp::min(bsz, file_size - block_start) as usize;
            let block_bytes = match blocks.get(&b) {
                Some(BlockState::Rewrite(bytes)) | Some(BlockState::Cached(bytes)) => bytes.clone(),
                Some(BlockState::Delete) => {
                    // Punched-hole block: materialize as zeros so the
                    // replace-on-flush body matches the post-flush read view.
                    Bytes::from(vec![0u8; new_content_len])
                }
                None => {
                    let committed_content_len = if block_start < committed_size {
                        std::cmp::min(bsz, committed_size - block_start) as usize
                    } else {
                        // Hole (block past the committed EOF -- pure grow).
                        0
                    };
                    self.lazy_load_block_for_flush(
                        existing_blob_guid,
                        b,
                        committed_content_len,
                        new_content_len,
                        trace_id,
                    )
                    .await?
                }
            };
            let take = std::cmp::min(new_content_len, block_bytes.len());
            out.extend_from_slice(&block_bytes[..take]);
            if take < new_content_len {
                out.resize(out.len() + (new_content_len - take), 0);
            }
        }
        Ok(out.freeze())
    }

    /// Override-style flush: write only the Rewrite intents to the
    /// existing blob_guid at `new_version`, then delete blocks past the
    /// new EOF if the file shrunk. Shrunk blocks are deleted at
    /// `new_version` so bssEraseCheck accepts them and a later
    /// re-extend reads zeros (POSIX shrink-destroys semantics).
    ///
    /// `trim_lower` / `trim_upper` extend the EOF-trim across a
    /// shrink-then-grow within a single buffer session: after a
    /// shrink the watermark is pinned at the lowest reached
    /// `block_count` and the originally-committed `block_count`, and
    /// every committed block in that range is deleted at
    /// `new_version` regardless of whether `file_size` later grew
    /// back. Without this, a `truncate(small); truncate(committed)`
    /// pair would leave the originally-committed blocks intact and a
    /// reader of the regrown range would see pre-shrink bytes,
    /// violating POSIX shrink-destroys.
    #[allow(clippy::too_many_arguments)]
    async fn override_flush_blocks(
        &self,
        blob_guid: data_types::DataBlobGuid,
        new_version: u64,
        block_size: u32,
        file_size: u64,
        committed_size: u64,
        blocks: &std::collections::BTreeMap<u32, BlockState>,
        trim_lower: Option<u32>,
        trim_upper: Option<u32>,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let block_size_usize = block_size as usize;
        let bsz_u64 = block_size as u64;

        // Identify the surviving last block of a non-aligned shrink so
        // its tail can be zeroed (POSIX shrink-destroys: bytes between
        // new EOF and the next block boundary must not resurface on a
        // later re-extend).
        let needs_tail_zero =
            file_size > 0 && file_size < committed_size && !file_size.is_multiple_of(bsz_u64);
        let tail_block = if needs_tail_zero {
            Some((file_size / bsz_u64) as u32)
        } else {
            None
        };
        let kept = (file_size % bsz_u64) as usize;

        // Step 1: write Rewrite intents at new_version. Cached and
        // implicit (absent) blocks are not re-uploaded -- they stay at
        // their previously-stored version on disk and remain reachable
        // through the new layout because BSS keys are versioned.
        // When the Rewrite covers the surviving last block of a
        // non-aligned shrink, the tail beyond `kept` is zeroed before
        // upload so the buffered user write doesn't preserve bytes
        // past the new EOF.
        let mut wrote_tail_block = false;
        for (block_num, state) in blocks {
            if let BlockState::Rewrite(bytes) = state {
                let mut block_bytes = BytesMut::with_capacity(block_size_usize);
                block_bytes.extend_from_slice(bytes);
                if block_bytes.len() < block_size_usize {
                    block_bytes.resize(block_size_usize, 0);
                }
                if Some(*block_num) == tail_block {
                    for byte in &mut block_bytes[kept..] {
                        *byte = 0;
                    }
                    wrote_tail_block = true;
                }
                self.backend()
                    .write_block(
                        blob_guid,
                        *block_num,
                        block_bytes.freeze(),
                        new_version,
                        trace_id,
                    )
                    .await?;
            }
        }

        // Step 2: EOF-trim. Delete the committed-and-now-destroyed
        // block range at new_version. Two contributors:
        //
        //   - Plain shrink with file_size < committed_size:
        //     [new_block_count, committed_block_count).
        //   - Shrink-then-grow within the same buffer session:
        //     [trim_lower, trim_upper). The watermark was pinned at
        //     the FIRST shrink so a later regrow doesn't lose the
        //     committed bound; without this the regrown range would
        //     resurface pre-shrink bytes on read.
        //
        // The two ranges are unioned and the ones we are about to
        // overwrite via a buffered Rewrite are skipped (the upload at
        // new_version handles the version bump on its own; deleting
        // first would just be a wasted RPC).
        let new_block_count = file_size.div_ceil(bsz_u64) as u32;
        let committed_block_count = committed_size.div_ceil(bsz_u64) as u32;
        let plain_lower = new_block_count;
        let plain_upper = committed_block_count;
        let watermark_lower = trim_lower.unwrap_or(u32::MAX);
        let watermark_upper = trim_upper.unwrap_or(0);
        let lower = std::cmp::min(plain_lower, watermark_lower);
        let upper = std::cmp::max(plain_upper, watermark_upper);
        if lower < upper {
            for block_num in lower..upper {
                if matches!(blocks.get(&block_num), Some(BlockState::Rewrite(_))) {
                    continue;
                }
                if let Err(e) = self
                    .backend()
                    .delete_block(blob_guid, block_num, new_version, trace_id)
                    .await
                {
                    tracing::warn!(
                        %blob_guid,
                        block_num,
                        new_version,
                        error = %e,
                        "Failed to delete shrunken block"
                    );
                }
            }
        }

        // Step 2b: PUNCH_HOLE deletes for blocks the user explicitly
        // dropped via fallocate. These sit inside the file's logical
        // range (block_num < new_block_count) -- they are NOT covered by
        // the EOF-trim above, which only walks blocks past EOF. Issued
        // at new_version so a concurrent reader at the previous version
        // still sees the old block until the layout flips.
        for (block_num, state) in blocks {
            if !matches!(state, BlockState::Delete) {
                continue;
            }
            if let Err(e) = self
                .backend()
                .delete_block(blob_guid, *block_num, new_version, trace_id)
                .await
            {
                tracing::warn!(
                    %blob_guid,
                    block_num,
                    new_version,
                    error = %e,
                    "Failed to delete punched block"
                );
            }
        }

        // Step 3: synthesised tail-zero for shrink with no buffered
        // Rewrite for the last block. Lazy-load the committed block,
        // zero everything after `kept`, write at new_version.
        if let Some(last_block) = tail_block
            && !wrote_tail_block
        {
            let committed_block_start = last_block as u64 * bsz_u64;
            let committed_content_len = if committed_block_start < committed_size {
                std::cmp::min(bsz_u64, committed_size - committed_block_start) as usize
            } else {
                0
            };
            let existing = self
                .lazy_load_block_for_flush(
                    Some(blob_guid),
                    last_block,
                    committed_content_len,
                    block_size_usize,
                    trace_id,
                )
                .await?;
            let mut buf = BytesMut::with_capacity(block_size_usize);
            let prefix_len = std::cmp::min(kept, existing.len());
            buf.extend_from_slice(&existing[..prefix_len]);
            buf.resize(block_size_usize, 0);
            self.backend()
                .write_block(blob_guid, last_block, buf.freeze(), new_version, trace_id)
                .await?;
        }

        Ok(())
    }

    /// Lazy-load a single block from BSS at flush time. Returns zeros
    /// when the block doesn't exist (sparse-file hole) or when no
    /// existing blob is known. Other failures propagate.
    async fn lazy_load_block_for_flush(
        &self,
        existing_blob_guid: Option<data_types::DataBlobGuid>,
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
            .read_block(guid, block_num, committed_content_len, trace_id)
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

    async fn flush_write_buffer(&self, fh_id: u64) -> Result<(), FsError> {
        // Snapshot the WriteBuffer and detach its block map so we can
        // run async work outside the DashMap guard. The block map is
        // moved out to avoid holding the DashMap guard across awaits;
        // if any post-snapshot step fails, restore_blocks_on_failure
        // puts them back so the next flush invocation retries the
        // same work (forward retry). On success the deferred state is
        // cleared at the end of the function.
        let (
            s3_key,
            file_size,
            committed_size,
            committed_blob_version,
            existing_blob_guid,
            block_size,
            blocks,
            expected_layout_bytes,
            eof_low_watermark,
            trim_upper,
            pending_reservations,
        ) = {
            let mut handle = self.file_handles.get_mut(&fh_id).ok_or(FsError::BadFd)?;
            let s3_key = handle.s3_key.clone();
            let committed_size = handle
                .layout
                .as_ref()
                .and_then(|l| l.size().ok())
                .unwrap_or(0);
            let committed_blob_version =
                handle.layout.as_ref().map(|l| l.blob_version).unwrap_or(0);
            let expected_layout_bytes = handle.layout_bytes.clone();
            let wb = match &mut handle.write_buf {
                Some(wb) if wb.dirty => wb,
                _ => return Ok(()),
            };
            let blocks = std::mem::take(&mut wb.blocks);
            let pending_reservations = std::mem::take(&mut wb.pending_reservations);
            (
                s3_key,
                wb.file_size,
                committed_size,
                committed_blob_version,
                wb.existing_blob_guid,
                wb.block_size,
                blocks,
                expected_layout_bytes,
                wb.eof_low_watermark,
                wb.trim_upper,
                pending_reservations,
            )
        };

        let trace_id = TraceId::new();

        // Run the post-snapshot BSS + NSS work. On any error, restore
        // the blocks back into wb so the next flush retries them.
        let result = self
            .flush_publish(
                fh_id,
                &s3_key,
                file_size,
                committed_size,
                committed_blob_version,
                existing_blob_guid,
                block_size,
                &blocks,
                expected_layout_bytes,
                eof_low_watermark,
                trim_upper,
                &pending_reservations,
                &trace_id,
            )
            .await;

        let (layout, new_layout_bytes) = match result {
            Ok(pair) => pair,
            Err(e) => {
                // Restore blocks for forward-retry. The handle's
                // single-writer invariant means no concurrent vfs_write
                // ran during this call, so the slot we took the blocks
                // out of is still empty and the put-back is a direct
                // assignment.
                //
                // CasConflict skips the restoration: the buffer is
                // proven stale (a cross-instance writer wrote on top
                // of us), so retrying would just lose. Userspace gets
                // ESTALE and must close/reopen.
                if !matches!(e, FsError::CasConflict)
                    && let Some(mut handle) = self.file_handles.get_mut(&fh_id)
                    && let Some(ref mut wb) = handle.write_buf
                {
                    if wb.blocks.is_empty() {
                        wb.blocks = blocks;
                    } else {
                        for (b, state) in blocks {
                            wb.blocks.entry(b).or_insert(state);
                        }
                    }
                    // Restore reservations the same way -- forward-retry
                    // must replay them at the next bumped version.
                    for b in pending_reservations {
                        wb.pending_reservations.insert(b);
                    }
                }
                return Err(e);
            }
        };

        // Update file handle with new layout and clear deferred state.
        // The committed file_size has just been published so the buffer
        // becomes clean; existing_blob_guid is updated so subsequent
        // partial-block edits lazy-load from the new blob. The freshly
        // installed layout_bytes become the next CAS guard's
        // expected_old_value.
        if let Some(mut handle) = self.file_handles.get_mut(&fh_id) {
            handle.layout = Some(layout.clone());
            handle.layout_bytes = Some(new_layout_bytes);
            if let Some(ref mut wb) = handle.write_buf {
                wb.dirty = false;
                wb.size_changed = false;
                wb.existing_blob_guid = layout.blob_guid().ok();
                wb.block_size = block_size;
                // The shrink-destroys watermark and pinned trim bound
                // are session-scoped: once the trim has landed via the
                // override flush, a later shrink-then-grow within the
                // SAME handle starts a fresh session and re-pins from
                // the new committed state.
                wb.eof_low_watermark = None;
                wb.trim_upper = None;
                wb.pending_reservations.clear();
                // wb.blocks already drained when we took it out above.
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

    /// Publish the buffered changes to BSS + NSS. Returns the new
    /// `ObjectLayout` plus the on-NSS bytes that were just installed
    /// (so the caller can refresh `handle.layout_bytes` for the next
    /// CAS) on success. Failure leaves the caller responsible for
    /// restoring `wb.blocks` so a subsequent flush can retry.
    ///
    /// `expected_layout_bytes` carries the bytes the caller believes
    /// NSS has stored for this key; when present, the override-flush
    /// NSS write goes through `put_inode_cas` and a guard mismatch
    /// surfaces as `FsError::CasConflict`. Pass `None` to skip the
    /// guard (initial-create flow, where we expect the slot to be new
    /// or whatever is there is the loser of an earlier crash).
    #[allow(clippy::too_many_arguments)]
    async fn flush_publish(
        &self,
        fh_id: u64,
        s3_key: &str,
        file_size: u64,
        committed_size: u64,
        committed_blob_version: u64,
        existing_blob_guid: Option<data_types::DataBlobGuid>,
        block_size: u32,
        blocks: &std::collections::BTreeMap<u32, BlockState>,
        expected_layout_bytes: Option<Bytes>,
        trim_lower: Option<u32>,
        trim_upper: Option<u32>,
        pending_reservations: &std::collections::BTreeSet<u32>,
        trace_id: &TraceId,
    ) -> Result<(ObjectLayout, Bytes), FsError> {
        let block_size_usize = block_size as usize;
        let _ = fh_id; // currently unused; reserved for future per-handle context

        // Override-flush vs replace-flush dispatch.
        //
        // Override flush: keep the existing blob_guid, bump blob_version
        // V -> V+1, write only the per-block Rewrite intents (other
        // blocks stay at V on disk and remain reachable through the
        // V+1 layout because BSS returns the latest stored version per
        // key). This is what makes a 1-block edit of a 100-block file
        // cost 1 block-write instead of a 100-block re-upload + delete
        // dance. Shrink-past-EOF blocks are deleted at V+1 so a later
        // re-extend reads zeros (POSIX semantics).
        //
        // Replace flush: brand-new file. Allocate a fresh blob_guid,
        // materialize the whole buffer, write all blocks at version=1.
        // If NSS happens to have a stale entry under the same key, its
        // old blob's blocks get cleaned up via the existing
        // delete_blob_blocks fire-and-forget path.
        let (final_blob_guid, final_blob_version, is_override) =
            if let Some(guid) = existing_blob_guid {
                // Bump to V+1, but guarantee at least 2 even when the
                // committed layout has blob_version=0 (uninitialised
                // record): those records' BSS blocks are stored at
                // version=1 (the previous hardcoded default), so an
                // overwrite at version=1 would land in bssOverwriteCheck's
                // idempotency branch and panic on different content.
                let new_version = committed_blob_version.saturating_add(1).max(2);
                self.override_flush_blocks(
                    guid,
                    new_version,
                    block_size,
                    file_size,
                    committed_size,
                    blocks,
                    trim_lower,
                    trim_upper,
                    trace_id,
                )
                .await?;
                // Issue per-block ReserveBlocks for any range fallocate
                // requested. Skipped on blocks that already have a
                // Rewrite intent or a Delete intent in this flush --
                // those entries already supersede the reservation. The
                // reservation is best-effort: a partial failure is
                // logged and not fatal to the flush, mirroring the way
                // the parent inode update itself is best-effort.
                for &block_num in pending_reservations.iter() {
                    if matches!(
                        blocks.get(&block_num),
                        Some(BlockState::Rewrite(_)) | Some(BlockState::Delete)
                    ) {
                        continue;
                    }
                    if let Err(e) = self
                        .backend()
                        .reserve_block(guid, block_num, block_size, new_version, trace_id)
                        .await
                    {
                        tracing::warn!(
                            %guid,
                            block_num,
                            new_version,
                            error = %e,
                            "Failed to reserve block; continuing"
                        );
                    }
                }
                (guid, new_version, true)
            } else {
                let data = self
                    .materialize_for_flush(None, file_size, 0, block_size, blocks, trace_id)
                    .await?;
                let blob_guid = self.backend().create_blob_guid();
                let num_blocks = if data.is_empty() {
                    0
                } else {
                    data.len().div_ceil(block_size_usize)
                };
                for block_i in 0..num_blocks {
                    let start = block_i * block_size_usize;
                    let end = std::cmp::min(start + block_size_usize, data.len());
                    let chunk = data.slice(start..end);
                    let padded = pad_to_block_size(chunk, block_size_usize);
                    self.backend()
                        .write_block(blob_guid, block_i as u32, padded, 1, trace_id)
                        .await?;
                }
                (blob_guid, 1, false)
            };

        // Write the parent inode meta record after all data block
        // writes have landed. The parent record carries the new
        // (total_size, block_count) at this version and is the
        // single commit point for the file's logical size on data
        // reads -- a reader who races the flush sees either the old
        // parent (and reads bytes against the old size) or the new
        // one, but never a half-applied state.
        //
        // Failure here is non-fatal: the data blocks landed, the
        // logical size is still recoverable from NSS layout.size,
        // and the next flush re-publishes the parent at a higher
        // version. We log and proceed.
        let parent_meta = data_types::parent_inode::ParentInodeMeta::new(
            final_blob_version,
            file_size,
            block_size,
        );
        if let Err(e) = self
            .backend()
            .write_parent_inode(final_blob_guid, parent_meta, trace_id)
            .await
        {
            tracing::warn!(
                %final_blob_guid,
                final_blob_version,
                file_size,
                error = %e,
                "Failed to publish parent inode; continuing"
            );
        }

        // Build ObjectLayout
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let layout = ObjectLayout {
            version_id: ObjectLayout::gen_version_id(),
            block_size,
            timestamp,
            blob_version: final_blob_version,
            state: ObjectState::Normal(ObjectMetaData {
                blob_guid: final_blob_guid,
                core_meta_data: ObjectCoreMetaData {
                    size: file_size,
                    etag: final_blob_guid.blob_id.simple().to_string(),
                    headers: vec![],
                    checksum: None,
                },
            }),
        };

        // Serialize layout
        let layout_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())
            .map_err(FsError::from)?
            .into();

        // Override flushes go through the CAS variant when we have a
        // snapshot of what NSS held at open time. A guard mismatch
        // means a cross-instance writer published a newer version
        // first; the in-memory write buffer is provably stale, so we
        // surface CasConflict (ESTALE) to userspace and let the caller
        // close + reopen. Non-override (initial-create) and the
        // missing-bytes fallback (no read at open) keep the original
        // unconditional put_inode path.
        let old_bytes = if is_override && let Some(expected) = expected_layout_bytes {
            self.backend()
                .put_inode_cas(s3_key, layout_bytes.clone(), expected, trace_id)
                .await?
        } else {
            self.backend()
                .put_inode(s3_key, layout_bytes.clone(), trace_id)
                .await?
        };

        // Replace flush only: clean up the old blob's blocks if NSS had
        // a stale entry under the same key. Override flush kept the
        // same blob_guid so its old_bytes refer to the SAME blob -- the
        // shrunken blocks were already handled inline above and any
        // blocks past new EOF were deleted at V+1.
        if !is_override
            && !old_bytes.is_empty()
            && let Ok(old_layout) =
                rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
            && old_layout.blob_guid().ok() != Some(final_blob_guid)
        {
            self.backend()
                .delete_blob_blocks(&old_layout, trace_id)
                .await;
        }

        Ok((layout, layout_bytes))
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

        // Dirty-handle stat reports wb.file_size whenever the handle
        // has buffered any size change (bare truncate or write-into-EOF).
        // Otherwise fall through to the committed layout.
        if let Some(fh_id) = fh
            && let Some(handle) = self.file_handles.get(&fh_id)
            && let Some(ref wb) = handle.write_buf
            && wb.size_changed
        {
            return Ok(self.make_new_file_attr(inode, wb.file_size));
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
    ///
    /// Buffered locally and O(1) regardless of `new_size`. The flat-buffer
    /// `BytesMut::resize` is gone -- a 100GB truncate updates a couple of
    /// fields and drops out-of-range block intents.
    pub async fn vfs_setattr_size(
        &self,
        inode: u64,
        fh: u64,
        new_size: u64,
    ) -> Result<VfsAttr, FsError> {
        // Phase 1: snapshot, drop intents past new EOF, lower the
        // shrink-destroys watermark, and decide whether the surviving
        // last block of a non-block-aligned shrink needs a synthesized
        // tail-zero `Rewrite`. Releases the DashMap guard before any
        // await; the lazy-load (if any) happens in phase 2.
        let (block_size, committed_size, existing_blob_guid, tail_zero_target) = {
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
                // Pin trim_upper at the FIRST shrink. Step 5a of flush
                // promotes handle.layout.size to the smaller new size,
                // so recomputing the bound from handle.layout on retry
                // would lose the committed bound.
                if wb.trim_upper.is_none() {
                    let committed_block_count = committed_size.div_ceil(bsz_u64) as u32;
                    if committed_block_count > new_last_block_excl {
                        wb.trim_upper = Some(committed_block_count);
                    }
                }
                // Non-block-aligned shrink: the surviving last block
                // contains [0..kept) of the original content and
                // [kept..block_size) of POSIX-destroyed bytes. The
                // override-flush tail-zero only inspects file_size AT
                // flush time; if a re-grow lifts file_size past the
                // shrink point before flush, that synthesis would
                // tail-zero the WRONG block. Synthesize the Rewrite
                // here so the destroyed tail is captured even across
                // shrink-then-grow within the same session.
                if new_size > 0 && !new_size.is_multiple_of(bsz_u64) {
                    let last = (new_size / bsz_u64) as u32;
                    let kept = (new_size % bsz_u64) as usize;
                    let block_was_committed = (last as u64) * bsz_u64 < committed_size;
                    let buffered_prefix: Option<Bytes> = match wb.blocks.get(&last) {
                        Some(BlockState::Rewrite(b)) | Some(BlockState::Cached(b)) => {
                            Some(b.clone())
                        }
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
                tail_zero_target,
            )
        };

        // Phase 2: lazy-load the surviving last block from BSS (if not
        // already buffered) outside the DashMap guard, then insert the
        // synthesized Rewrite. A subsequent re-grow that writes into
        // an offset > kept on the same block sees this Rewrite and
        // merges over the zeros; a write strictly inside [0..kept)
        // also merges, preserving the zeros in [kept..block_size).
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

        // Single-writer per inode. First writer wins; subsequent
        // write-mode opens fail with EBUSY. The lock is process-local
        // in-memory state and dies with the process on crash, so the
        // next open reacquires.
        let fh = self.alloc_fh();
        if is_write {
            self.acquire_write_lock(inode, fh)?;
        }

        // Resolve layout. Write-mode opens always fetch fresh from NSS
        // (even when the inode cache is hot) so the override-flush CAS
        // has the bytes NSS actually has -- a stale cached layout could
        // pass a CAS check that the server would reject. Read-only
        // opens reuse the cached layout when available and never need
        // bytes.
        let (layout, layout_bytes) = if is_write {
            let trace_id = TraceId::new();
            match self
                .backend()
                .get_inode_with_bytes(&s3_key, &trace_id)
                .await
            {
                Ok((l, bytes)) => (Some(l), Some(bytes)),
                Err(FsError::NotFound) => (None, None),
                Err(e) => return Err(e),
            }
        } else {
            match layout {
                Some(l) => (Some(l), None),
                None => {
                    let trace_id = TraceId::new();
                    match self.backend().get_inode(&s3_key, &trace_id).await {
                        Ok(l) => (Some(l), None),
                        Err(e) => return Err(e),
                    }
                }
            }
        };

        // For an existing file opened for write, ask BSS for the
        // parent inode and prefer its total_size when present. The
        // parent inode is published as part of the override flush
        // sequence and is the authoritative source for the file's
        // logical size; a successful read here picks up the latest
        // committed size even when the in-memory inode cache is
        // ahead/behind the BSS state. Failure is tolerated -- older
        // blobs that pre-date parent-inode support, or a transient
        // missing-replica situation, return None and we fall back to
        // layout.size.
        let parent_size = if is_write {
            if let Some(ref l) = layout
                && let Ok(blob_guid) = l.blob_guid()
            {
                let trace_id = TraceId::new();
                match self.backend().read_parent_inode(blob_guid, &trace_id).await {
                    Ok(Some(meta)) => Some(meta.total_size),
                    Ok(None) => None,
                    Err(e) => {
                        tracing::warn!(
                            %blob_guid, error = %e,
                            "read_parent_inode failed during open; falling back to layout size"
                        );
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        // No preload. Existing files seed the WriteBuffer with the
        // committed file_size + blob_guid; partial writes lazy-load the
        // touched blocks at write time. O_TRUNC is honored by setting
        // file_size = 0.
        let has_trunc = flags & libc::O_TRUNC as u32 != 0;
        let write_buf = if is_write {
            if let Some(ref l) = layout
                && !has_trunc
            {
                let blob_guid = l.blob_guid().ok();
                let committed_size = parent_size.unwrap_or_else(|| l.size().unwrap_or(0));
                Some(WriteBuffer::new(blob_guid, committed_size, l.block_size))
            } else if let Some(ref l) = layout {
                // O_TRUNC on an existing file: drop bytes from the buffer
                // but keep blob_guid so a subsequent write that reaches into
                // an old (already-truncated-away) block can't accidentally
                // lazy-load. file_size is 0; size_changed flips on so flush
                // sees the truncate.
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

        self.file_handles.insert(
            fh,
            FileHandle {
                ino: inode,
                s3_key,
                layout,
                layout_bytes,
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

        // Dirty-handle read merge: materialize into a Vec<u8> and freeze
        // for callers that need owned bytes.
        if let Some(ref wb) = handle.write_buf
            && wb.dirty
        {
            let file_size = wb.file_size;
            let block_size = wb.block_size;
            let existing_blob_guid = wb.existing_blob_guid;
            let blocks = wb.blocks.clone();
            let eof_low_watermark = wb.eof_low_watermark;
            drop(handle);
            let cap = std::cmp::min(size as u64, file_size.saturating_sub(offset)) as usize;
            let mut buf = vec![0u8; cap];
            let n = self
                .read_dirty_handle(
                    file_size,
                    block_size,
                    existing_blob_guid,
                    &blocks,
                    eof_low_watermark,
                    offset,
                    &mut buf,
                )
                .await?;
            buf.truncate(n);
            return Ok(Bytes::from(buf));
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

    /// Sparse write path: lazy-loads only the affected blocks (no
    /// whole-file preload), inserts a `Rewrite` intent for each, and
    /// grows the logical EOF if the write extends past it.
    pub async fn vfs_write(&self, fh: u64, offset: u64, data: &[u8]) -> Result<u32, FsError> {
        // POSIX: zero-byte writes are a no-op and must NOT extend the
        // file. Early return avoids the (end - 1) underflow below.
        if data.is_empty() {
            return Ok(0);
        }
        let end = offset + data.len() as u64;

        // Phase 1: snapshot the bits of state we need (block_size, the
        // blob_guid for lazy-loading, current intents) without holding
        // the DashMap guard across awaits. Initialize the buffer in
        // place if missing.
        let (block_size, existing_blob_guid, committed_size, blocks_to_load) = {
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
            let wb = handle
                .write_buf
                .get_or_insert_with(|| WriteBuffer::new(layout_blob_guid, committed_size, bsize));
            let bsz_u64 = wb.block_size as u64;
            let first_block = (offset / bsz_u64) as u32;
            let last_block = ((end - 1) / bsz_u64) as u32;
            // Identify which blocks need lazy load: blocks touched by a
            // partial write that aren't already buffered AND not fully
            // overwritten by this call. Blocks whose committed bytes
            // were destroyed by an earlier shrink in this buffer
            // session are explicitly skipped from the load list -- they
            // read as zeros per POSIX, and Phase 3's `None` arm builds
            // a zeroed buffer when neither `wb.blocks` nor `loaded`
            // carries content. Without this guard the lazy-load would
            // resurrect pre-shrink BSS bytes under user data.
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
                to_load,
            )
        };

        // Phase 2: lazy-load missing blocks (outside the guard).
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
                    b,
                    committed_content_len,
                    block_size as usize,
                    &trace_id,
                )
                .await?;
            loaded.insert(b, bytes);
        }

        // Phase 3: re-acquire the guard, apply edits, grow file_size.
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
            // Determine the slice of `data` that lands in this block.
            let copy_src_start = block_start.saturating_sub(offset).min(data.len() as u64) as usize;
            let copy_src_end = block_end.saturating_sub(offset).min(data.len() as u64) as usize;
            let copy_dst_start = offset.saturating_sub(block_start).min(bsz_u64) as usize;
            let copy_dst_end = (end.saturating_sub(block_start).min(bsz_u64)) as usize;
            // Build the new block bytes. Start from existing content
            // (Rewrite/Cached/loaded) or zeros for a fresh block.
            // A buffered Delete (PUNCH_HOLE) on this block is overwritten
            // by the user write, which means the hole is no longer
            // logically present -- start from zeros and let the write
            // populate the touched range.
            let mut block_bytes: BytesMut = match wb.blocks.get(&b) {
                Some(BlockState::Rewrite(b2)) | Some(BlockState::Cached(b2)) => {
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
                        // Fully overwritten new block.
                        BytesMut::zeroed(wb.block_size as usize)
                    }
                }
            };
            block_bytes[copy_dst_start..copy_dst_end]
                .copy_from_slice(&data[copy_src_start..copy_src_end]);
            wb.blocks
                .insert(b, BlockState::Rewrite(block_bytes.freeze()));
            // A real upload supersedes any prior fallocate reservation
            // for this block index.
            wb.pending_reservations.remove(&b);
        }
        if end > wb.file_size {
            wb.file_size = end;
            wb.size_changed = true;
        }
        wb.dirty = true;

        Ok(data.len() as u32)
    }

    /// `fallocate(2)` for FUSE.
    ///
    /// Supported modes:
    ///
    /// - `0`: pre-allocate / extend. Records a reservation hint for the
    ///   touched range and grows `wb.file_size` to `max(file_size,
    ///   offset + length)`. Reads of unwritten blocks in the reserved
    ///   range observe zeros.
    /// - `FALLOC_FL_KEEP_SIZE`: pre-allocate without growing. Same as
    ///   above but `wb.file_size` is left untouched.
    /// - `FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE`: drop fully
    ///   covered interior blocks via `BlockState::Delete` and zero the
    ///   touched range of any partially covered edge block via an
    ///   `edge_block_zero` Rewrite. `wb.file_size` is untouched.
    ///
    /// All state stays in the WriteBuffer; the BSS-side mutations land
    /// at flush time.
    pub async fn vfs_fallocate(
        &self,
        fh: u64,
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
        let (block_size, existing_blob_guid, committed_size, edge_loads) = {
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
                //   - There isn't already a buffered (Rewrite/Cached)
                //     copy we can edit in place, AND
                //   - The shrink-destroys watermark hasn't already
                //     turned this block into zeros.
                let mut consider_edge = |b: u32| {
                    if matches!(
                        wb.blocks.get(&b),
                        Some(BlockState::Rewrite(_)) | Some(BlockState::Cached(_))
                    ) {
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
                    Some(BlockState::Rewrite(b2)) | Some(BlockState::Cached(b2)) => {
                        Some(b2.clone())
                    }
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
            if matches!(
                wb.blocks.get(&b),
                Some(BlockState::Rewrite(_)) | Some(BlockState::Cached(_))
            ) {
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

    /// `lseek(fd, offset, SEEK_HOLE | SEEK_DATA)`.
    ///
    /// Walks the blocks of the file from `ceil(offset / block_size)`
    /// forward, consulting the dirty WriteBuffer first and falling
    /// back to a per-block BSS probe (`read_block`, treating
    /// `BlockNotFound` as a hole). The EOF source depends on whether
    /// the handle has a write buffer:
    ///   - dirty/write handle -> `wb.file_size`
    ///   - read-only handle  -> the BSS parent inode's `total_size`
    ///     when available, otherwise the cached layout `size`.
    pub async fn vfs_lseek(&self, fh: u64, offset: u64, whence: u32) -> Result<u64, FsError> {
        let seek_data = whence == libc::SEEK_DATA as u32;
        let seek_hole = whence == libc::SEEK_HOLE as u32;
        if !seek_data && !seek_hole {
            return Err(FsError::InvalidArg);
        }

        // Snapshot the bits we need without holding the guard across awaits.
        let (
            file_size_hint,
            block_size,
            existing_blob_guid,
            blocks,
            pending_reservations,
            eof_low_watermark,
            has_write_buffer,
        ) = {
            let handle = self.file_handles.get(&fh).ok_or(FsError::BadFd)?;
            let block_size = handle
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
                    true,
                )
            } else {
                (
                    layout_size,
                    block_size,
                    layout_blob_guid,
                    std::collections::BTreeMap::new(),
                    std::collections::BTreeSet::new(),
                    None,
                    false,
                )
            }
        };

        // Read-only handle: refresh authoritative size from the BSS
        // parent inode if available. Same policy as the read path --
        // no per-handle cache.
        let trace_id = TraceId::new();
        let file_size = if !has_write_buffer {
            if let Some(guid) = existing_blob_guid {
                match self.backend().read_parent_inode(guid, &trace_id).await {
                    Ok(Some(meta)) => meta.total_size,
                    Ok(None) => file_size_hint,
                    Err(e) => {
                        tracing::warn!(%guid, error = %e, "read_parent_inode failed during lseek; falling back");
                        file_size_hint
                    }
                }
            } else {
                file_size_hint
            }
        } else {
            file_size_hint
        };

        // Match Linux semantics: offset >= file_size returns ENXIO
        // for both SEEK_HOLE and SEEK_DATA.
        if offset >= file_size {
            return Err(FsError::NoData);
        }

        let bsz_u64 = block_size as u64;
        let first_block = (offset / bsz_u64) as u32;
        let last_block_excl = file_size.div_ceil(bsz_u64) as u32;

        // Per-block classifier. `Some(true)` -> data, `Some(false)` ->
        // hole, `None` -> not buffered, fall through to BSS probe.
        let buffered_kind = |b: u32| -> Option<bool> {
            match blocks.get(&b) {
                Some(BlockState::Rewrite(_)) | Some(BlockState::Cached(_)) => Some(true),
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

        // BSS-side classification: one ListBlobBlocks call covers the
        // whole walk range. Reserved entries count as data (Linux
        // SEEK_DATA convention), Data is data, anything not in the
        // returned set is a hole.
        let block_map: std::collections::BTreeSet<u32> = match existing_blob_guid {
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

    pub async fn vfs_flush(&self, fh: u64) -> Result<(), FsError> {
        self.flush_write_buffer(fh).await
    }

    pub async fn vfs_release(&self, fh: u64) -> Result<(), FsError> {
        // Flush any dirty write buffer before releasing
        let (has_dirty, was_writer) = self
            .file_handles
            .get(&fh)
            .map(|h| {
                let dirty = h.write_buf.as_ref().map(|wb| wb.dirty).unwrap_or(false);
                let writer = h.write_buf.is_some();
                (dirty, writer)
            })
            .unwrap_or((false, false));

        if has_dirty {
            self.flush_write_buffer(fh).await?;
        }

        // Get the inode before removing the handle
        let ino = self.file_handles.get(&fh).map(|h| h.ino);
        self.file_handles.remove(&fh);

        // Release the inode-scoped write lock if this handle held it.
        // Read-only handles never acquired it.
        if was_writer && let Some(ino) = ino {
            self.release_write_lock(ino, fh);
        }

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
        // vfs_create implicitly opens the new file for writing, so it
        // must obey the inode-scoped write lock. A re-create on an
        // inode that already has a live write handle returns EBUSY.
        self.acquire_write_lock(ino, fh)?;
        // size_changed=true so a subsequent close-without-write still
        // creates an empty NSS layout, matching legacy behavior where
        // creat()+close() materializes a 0-byte object.
        let mut wb = WriteBuffer::new(None, 0, DEFAULT_BLOCK_SIZE);
        wb.size_changed = true;
        wb.dirty = true;
        self.file_handles.insert(
            fh,
            FileHandle {
                ino,
                s3_key: key,
                layout: None,
                layout_bytes: None,
                write_buf: Some(wb),
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
/// Zero-pad `bytes` up to `block_size_usize`, returning the original
/// (cheap clone) if it's already at least that large. Used by the
/// override flush + replace flush write paths so every block lands on
/// disk at full block_size.
fn pad_to_block_size(bytes: Bytes, block_size_usize: usize) -> Bytes {
    if bytes.len() >= block_size_usize {
        bytes
    } else {
        let mut buf = BytesMut::with_capacity(block_size_usize);
        buf.extend_from_slice(&bytes);
        buf.resize(block_size_usize, 0);
        buf.freeze()
    }
}

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
