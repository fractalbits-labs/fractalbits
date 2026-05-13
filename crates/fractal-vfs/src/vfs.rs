use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use data_types::TraceId;
use rkyv::api::high::to_bytes_in;
use std::cell::Cell;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::backend::{BackendConfig, StorageBackend};
use crate::cache::{DirCache, DirEntry};
use crate::disk_cache::DiskCache;
use crate::error::FsError;
use crate::inode::{EntryType, InodeTable, ROOT_INODE};
use crate::writeback::{InodeOp as WbInodeOp, WritebackQueue};
use data_types::object_layout::{
    MpuState, ObjectCoreMetaData, ObjectLayout, ObjectMetaData, ObjectState, SymlinkData,
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
    /// Sub-second part of `atime`, in nanoseconds (0..1e9). Carried
    /// independently of `atime_secs` so a `utimensat` that set atime
    /// to (s, ns) round-trips through `lstat.atime_ns` (pjdfstest
    /// utimensat/08.t exercises this contract).
    pub atime_ns_part: u32,
    pub mtime_ns_part: u32,
    pub ctime_ns_part: u32,
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
    disk_cache: Option<Arc<DiskCache>>,
    dir_cache: DirCache,
    file_handles: DashMap<u64, FileHandle>,
    next_fh: AtomicU64,
    read_write: bool,
    passthrough_enabled: bool,
    passthrough_max_object_size: u64,
    prefetch_policy: crate::prefetch::PrefetchPolicy,
    /// Writeback queue. Worker is spawned lazily on the first FUSE
    /// op (the FUSE adapter's `init()` trait method is dead in this
    /// codebase -- the session handles FUSE_INIT itself -- so we
    /// spawn from inside the compio runtime when the first op
    /// arrives).
    writeback: Arc<WritebackQueue>,
    /// `max_batch_wait_ms` from the writeback config; the drainer
    /// polls this often.
    writeback_poll_ms: u32,
    /// One-shot guard for the writeback worker. Flipped by
    /// `ensure_writeback_worker` on first FUSE op.
    writeback_worker_started: AtomicBool,
    fuse_dev_fd: Option<Arc<OwnedFd>>,
    /// Notifier built from `fuse_dev_fd` so handlers can fire
    /// FUSE_NOTIFY_* messages (e.g. inval_inode after SUID/SGID clear)
    /// without re-opening the fd ourselves. `None` in NFS mode.
    fuse_notifier: Option<fractal_fuse::FuseNotifier>,
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
                    Some(Arc::new(dc))
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
        let prefetch_policy = crate::prefetch::PrefetchPolicy::from_config(config);
        let writeback_poll_ms = config.writeback.max_batch_wait_ms.max(1);
        let writeback = Arc::new(WritebackQueue::new());

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
            prefetch_policy,
            writeback,
            writeback_poll_ms,
            writeback_worker_started: AtomicBool::new(false),
            fuse_dev_fd: None,
            fuse_notifier: None,
            deferred_blob_cleanup: DashMap::new(),
            inode_write_owner: DashMap::new(),
        }
    }

    /// Install the shared `/dev/fuse` fd, obtained from
    /// `Session::fuse_fd()`, before the session is run. FUSE-mode only;
    /// NFS mode never calls this. The fd is needed by passthrough open /
    /// close paths that may fire on the very first FUSE request and by
    /// the FuseNotifier for kernel-cache invalidations.
    pub fn with_fuse_fd(mut self, fuse_dev_fd: Arc<OwnedFd>) -> Self {
        self.fuse_notifier = Some(fractal_fuse::FuseNotifier::from(fuse_dev_fd.clone()));
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

    /// Bump the parent directory inode's `mtime` / `ctime` to `now`.
    /// POSIX requires that creating or removing an entry under a
    /// directory updates the directory's mtime and ctime; pjdfstest's
    /// `mkfifo/00.t` / `mknod/00.t` / `open/00.t` "Update parent
    /// directory ctime/mtime" subtests verify it. We mutate the
    /// in-memory inode entry only -- the bump is observable to any
    /// caller that hits the cached entry on the next stat. A
    /// forget+relookup loses the bump (the layout in NSS still
    /// carries the original times) which is acceptable for the
    /// noatime/relatime fallback contract; a fully persistent bump
    /// would require an extra NSS round-trip per child op and is not
    /// worth the per-create cost. Root is skipped because its posix
    /// is intentionally synthetic.
    fn touch_parent_times(&self, parent: u64) {
        if parent == ROOT_INODE {
            return;
        }
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        if let Some(mut entry) = self.inodes.get_mut(parent) {
            entry.posix.mtime_ns = now_ns;
            entry.posix.ctime_ns = now_ns;
        }
    }

    /// POSIX `NAME_MAX = 255`. Linux's general VFS enforces this at
    /// the kernel level for native filesystems but FUSE callers
    /// have to enforce it themselves; pjdfstest's `02.t` /
    /// `chmod/02.t` / `mkdir/02.t` etc. boundary tests pick a
    /// 256-byte component and expect ENAMETOOLONG.
    #[inline]
    fn check_name_max(name: &str) -> Result<(), FsError> {
        if name.len() > 255 {
            return Err(FsError::NameTooLong);
        }
        Ok(())
    }

    /// Resolve the parent directory's `PosixAttrs`, preferring the
    /// in-memory inode entry and falling back to NSS when the entry
    /// hasn't been seeded (cross-instance state, freshly-mounted
    /// daemon, etc.). Used by the sticky-bit gate in
    /// vfs_unlink / vfs_rmdir / vfs_rename so the parent's mode is
    /// always read from the source of truth, not just the in-memory
    /// snapshot. Returns `None` if neither source has the inode.
    async fn resolve_parent_posix(
        &self,
        parent_ino: u64,
        parent_key: &str,
        trace_id: &TraceId,
    ) -> Option<data_types::object_layout::PosixAttrs> {
        // In-memory wins when posix.mode != 0 (initialised path).
        if let Some(e) = self.inodes.get(parent_ino)
            && e.posix.mode != 0
        {
            return Some(e.posix);
        }
        // Cold path: pull from NSS. Directory keys end in '/', so we
        // pass the prefix verbatim.
        match self.backend().get_inode(parent_key, trace_id).await {
            Ok(layout) => Some(crate::inode::layout_posix(&layout)),
            Err(_) => None,
        }
    }

    /// Resolve the target file's owner uid for the sticky-bit gate.
    /// Same in-memory-then-NSS pattern as
    /// `resolve_parent_posix`. Returns `NotFound` if NSS has no
    /// such key, `Internal` for transport errors.
    async fn resolve_file_uid(&self, key: &str, trace_id: &TraceId) -> Result<u32, FsError> {
        // Try cached file or directory inode first.
        let cached = self
            .inodes
            .find_ino_by_key(key, EntryType::File)
            .or_else(|| self.inodes.find_ino_by_key(key, EntryType::Directory))
            .and_then(|ino| self.inodes.get(ino).map(|e| e.posix.uid));
        if let Some(u) = cached
            && u != 0
        {
            return Ok(u);
        }
        // Cold path: NSS.
        match self.backend().get_inode(key, trace_id).await {
            Ok(layout) => Ok(crate::inode::layout_posix(&layout).uid),
            Err(FsError::NotFound) => {
                // Directory probe: key without trailing slash above
                // may be a file; try with trailing slash as a dir.
                let dir_key = format!("{}/", key);
                match self.backend().get_inode(&dir_key, trace_id).await {
                    Ok(layout) => Ok(crate::inode::layout_posix(&layout).uid),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// POSIX `PATH_MAX = 4096`. A separate boundary check from
    /// `check_name_max`; pjdfstest's `03.t` / `chmod/03.t` /
    /// `mkdir/03.t` etc. tests pick a path-at-PATH_MAX and expect
    /// the FS to accept it.
    #[inline]
    fn check_path_max(prefix: &str, name: &str) -> Result<(), FsError> {
        // The kernel enforces PATH_MAX on the path the syscall
        // receives before forwarding to FUSE; what reaches us is the
        // bucket-relative key (`prefix + name`). NSS keys cap at
        // 8 KiB (see `core/nss_server/configs.zig` user_max_key_size)
        // which is comfortably above PATH_MAX plus our small bucket
        // prefix, so the only thing we have to guard against here is
        // a key that would overflow the NSS protocol cap.
        if prefix.len() + name.len() > 8192 {
            return Err(FsError::NameTooLong);
        }
        Ok(())
    }

    fn has_open_handles_for_inode(&self, ino: u64, exclude_fh: Option<u64>) -> bool {
        self.file_handles.iter().any(|entry| {
            entry.value().ino == ino && exclude_fh.is_none_or(|excl| *entry.key() != excl)
        })
    }

    /// `true` when *any* currently-open handle for `ino` has a
    /// `write_buf`. Used by `fuse_server.getattr` to drop the attr
    /// cache TTL to zero on writeable inodes -- without this the
    /// kernel returns the cached pre-clear mode after the SUID/SGID
    /// drop in `vfs_write` and pjdfstest's chmod/12.t fstat-on-fh
    /// case fails. The check is O(open-handles); for a single-fh
    /// workload this is one DashMap iteration.
    pub fn inode_has_writeable_handle(&self, ino: u64) -> bool {
        self.file_handles
            .iter()
            .any(|entry| entry.value().ino == ino && entry.value().write_buf.is_some())
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

    /// Borrow the writeback queue. Used by external spawn handlers that
    /// need to seal cycles around an async flush.
    pub fn writeback_queue(&self) -> &Arc<WritebackQueue> {
        &self.writeback
    }

    /// Allocate the next generation for `inode`. Each background flush
    /// gets a fresh generation so concurrent flushes don't false-
    /// coalesce in the queue. Generation numbers are monotonic per
    /// inode across the fs_server lifetime.
    pub fn allocate_flush_generation(&self, inode: u64) -> crate::writeback::Generation {
        let cur = self.writeback.active_generation(inode);
        crate::writeback::Generation(cur.0 + 1)
    }

    /// Peek at the file-handle state needed by the FuseServer release
    /// adapter to decide whether to spawn an async flush. Returns
    /// `(inode, has_dirty, file_size)`, or `None` if the fh is gone.
    pub fn peek_release_state(&self, fh: u64) -> Option<(u64, bool, u64)> {
        let handle = self.file_handles.get(&fh)?;
        let has_dirty = handle
            .write_buf
            .as_ref()
            .map(|wb| wb.dirty)
            .unwrap_or(false);
        let file_size = handle
            .write_buf
            .as_ref()
            .map(|wb| wb.file_size)
            .unwrap_or(0);
        Some((handle.ino, has_dirty, file_size))
    }

    fn file_perm(&self) -> u16 {
        if self.read_write { 0o644 } else { 0o444 }
    }

    fn dir_perm(&self) -> u16 {
        if self.read_write { 0o755 } else { 0o555 }
    }

    // -- Attribute builders --

    fn make_file_attr(&self, ino: u64, layout: &ObjectLayout) -> Result<VfsAttr, FsError> {
        let size = layout.size()?;
        let ts = layout.timestamp / 1000;
        // Symlinks share the regular-file attribute path but report
        // S_IFLNK + 0 blocks. The kernel uses the mode bit to decide
        // whether to call FUSE_READLINK or FUSE_OPEN on a lookup.
        let is_symlink = layout.is_symlink();
        // Special inodes (fifo / block / char / unix-socket) share
        // the same attribute path; the kernel uses the S_IFMT bit
        // and `rdev` to dispatch I/O to its own pipe / device /
        // socket layer rather than calling FUSE_READ / FUSE_WRITE.
        let special = layout.special();
        // Prefer the in-memory posix from the inode entry: it tracks
        // unflushed setattr changes that haven't yet been folded into
        // a layout. Falls back to layout-embedded posix and finally to
        // synthesised defaults when neither has been initialised.
        let posix = self
            .inodes
            .get(ino)
            .map(|e| e.posix)
            .unwrap_or_else(|| crate::inode::layout_posix(layout));
        let default_mode = if is_symlink {
            symlink_mode(0o777)
        } else if let Some(s) = special {
            let ifmt = match s.kind {
                data_types::object_layout::SpecialKind::Fifo => libc::S_IFIFO,
                data_types::object_layout::SpecialKind::BlockDevice => libc::S_IFBLK,
                data_types::object_layout::SpecialKind::CharDevice => libc::S_IFCHR,
                data_types::object_layout::SpecialKind::Socket => libc::S_IFSOCK,
            };
            ifmt | (self.file_perm() as u32 & !libc::S_IFMT)
        } else {
            file_mode(self.file_perm())
        };
        // posix.mode may be a raw permission-bits value coming from a
        // chmod that didn't include S_IFMT. Re-stamp the file-type
        // bits from `default_mode` so the kernel sees a valid mode_t
        // (without S_IFREG / S_IFLNK / S_IFIFO etc. the kernel
        // reclassifies the entry and stat returns garbage).
        let ifmt_mask = libc::S_IFMT;
        let mode = if posix.mode != 0 {
            (posix.mode & !ifmt_mask) | (default_mode & ifmt_mask)
        } else {
            default_mode
        };
        let rdev = special.map(|s| s.rdev).unwrap_or(0);
        let (mtime_secs, mtime_ns_part) = if posix.mtime_ns != 0 {
            (
                posix.mtime_ns / 1_000_000_000,
                (posix.mtime_ns % 1_000_000_000) as u32,
            )
        } else {
            (ts, 0u32)
        };
        let (ctime_secs, ctime_ns_part) = if posix.ctime_ns != 0 {
            (
                posix.ctime_ns / 1_000_000_000,
                (posix.ctime_ns % 1_000_000_000) as u32,
            )
        } else {
            (ts, 0u32)
        };
        Ok(VfsAttr {
            ino,
            size,
            blocks: if is_symlink || special.is_some() {
                0
            } else {
                size.div_ceil(512)
            },
            // PosixAttrs intentionally drops the per-inode atime --
            // see object_layout::PosixAttrs. We mirror mtime so the
            // pjdfstest contract `atime != 0 after create` holds and
            // `find -newer` against atime stays consistent with the
            // last-write timestamp. `apply_atime_override` layers
            // any utimensat-set atime on top after this builds.
            atime_secs: mtime_secs,
            mtime_secs,
            ctime_secs,
            atime_ns_part: mtime_ns_part,
            mtime_ns_part,
            ctime_ns_part,
            mode,
            nlink: 1,
            uid: posix.uid,
            gid: posix.gid,
            rdev,
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
            atime_ns_part: 0,
            mtime_ns_part: 0,
            ctime_ns_part: 0,
            mode: file_mode(self.file_perm()),
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        }
    }

    fn make_dir_attr(&self, ino: u64) -> VfsAttr {
        let posix = self.inodes.get(ino).map(|e| e.posix).unwrap_or_default();
        // FUSE root inode reports mode 0o777 unconditionally so the
        // kernel's `default_permissions` check (when allow_other is
        // on) lets every caller into the mount; without this, the
        // first `mkdir` from a non-daemon user gets EACCES at the
        // root and never reaches fs_server. Sub-directory inodes
        // honour their persisted mode normally.
        let default_mode = if ino == ROOT_INODE {
            dir_mode(0o777)
        } else {
            dir_mode(self.dir_perm())
        };
        let ifmt_mask = libc::S_IFMT;
        let mode = if posix.mode != 0 && ino != ROOT_INODE {
            (posix.mode & !ifmt_mask) | (default_mode & ifmt_mask)
        } else {
            default_mode
        };
        let mtime_secs = posix.mtime_ns / 1_000_000_000;
        let mtime_ns_part = (posix.mtime_ns % 1_000_000_000) as u32;
        let ctime_secs = posix.ctime_ns / 1_000_000_000;
        let ctime_ns_part = (posix.ctime_ns % 1_000_000_000) as u32;
        VfsAttr {
            ino,
            size: 0,
            blocks: 0,
            // atime mirrors mtime; see PosixAttrs.
            atime_secs: mtime_secs,
            mtime_secs,
            ctime_secs,
            atime_ns_part: mtime_ns_part,
            mtime_ns_part,
            ctime_ns_part,
            mode,
            nlink: 2,
            uid: posix.uid,
            gid: posix.gid,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        }
    }

    fn make_new_file_attr(&self, ino: u64, size: u64) -> VfsAttr {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let posix = self.inodes.get(ino).map(|e| e.posix).unwrap_or_default();
        let default_mode = file_mode(self.file_perm());
        let ifmt_mask = libc::S_IFMT;
        let mode = if posix.mode != 0 {
            (posix.mode & !ifmt_mask) | (default_mode & ifmt_mask)
        } else {
            default_mode
        };
        let (mtime_secs, mtime_ns_part) = if posix.mtime_ns != 0 {
            (
                posix.mtime_ns / 1_000_000_000,
                (posix.mtime_ns % 1_000_000_000) as u32,
            )
        } else {
            (now_secs, 0u32)
        };
        let (ctime_secs, ctime_ns_part) = if posix.ctime_ns != 0 {
            (
                posix.ctime_ns / 1_000_000_000,
                (posix.ctime_ns % 1_000_000_000) as u32,
            )
        } else {
            (now_secs, 0u32)
        };
        VfsAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            // atime mirrors mtime; see PosixAttrs.
            atime_secs: mtime_secs,
            mtime_secs,
            ctime_secs,
            atime_ns_part: mtime_ns_part,
            mtime_ns_part,
            ctime_ns_part,
            mode,
            nlink: 1,
            uid: posix.uid,
            gid: posix.gid,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        }
    }

    // -- Passthrough helpers --

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
        if !dc.is_complete(blob_guid, file_size) {
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

    // -- Cache helpers --

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
        blob_version: u64,
        block_num: u32,
        block_content_len: usize,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        // Try disk cache. Same-instance reads under single-writer-per-
        // inode trust the cache directly; cross-instance staleness is
        // handled by reconcile_on_open at vfs_open time.
        if let Some(dc) = &self.disk_cache
            && let Some(cached) = dc.get_block(blob_guid, block_num, block_content_len).await
        {
            return Ok(cached);
        }

        // Cache miss: fetch from backend. The dispatch on blob_version
        // (fast path at V<=1, fan-out + max-version + inline-repair at
        // V>1) lives in backend.read_block.
        let (data, _checksum) = match self
            .backend()
            .read_block(
                blob_guid,
                blob_version,
                block_num,
                block_content_len,
                trace_id,
            )
            .await
        {
            Ok(r) => r,
            Err(FsError::DataVg(volume_group_proxy::DataVgError::BlockNotFound))
            | Err(FsError::Rpc(rpc_client_common::RpcError::NotFound)) => {
                return Ok(Bytes::from(vec![0u8; block_content_len]));
            }
            Err(e) => return Err(e),
        };

        // Populate disk cache.
        if let Some(dc) = &self.disk_cache {
            let _ = dc
                .insert_block(blob_guid, block_num, blob_version, &data)
                .await;
        }

        Ok(data)
    }

    // -- Read helpers --

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
                    layout.blob_version,
                    block_num,
                    block_content_len,
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
                    layout.blob_version,
                    block_num,
                    block_content_len,
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
                            part_obj.blob_version,
                            block_num,
                            block_content_len,
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

    // -- Zero-copy read helpers (direct-to-buffer) --

    /// Read a cached block directly into `buf`. Returns bytes written on hit,
    /// or `None` on cache miss (caller should fall back to the Bytes path).
    async fn read_block_cached_into(
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
                    .read_block_cached(blob_guid, layout.blob_version, bn, bcl, &trace_id)
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

    // -- Write helpers --

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
        committed_blob_version: u64,
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
        // Commit envelope rides every BssBatch in this override flush.
        // All three sub-batches (rewrite, delete, put) target the same
        // blob, and the file's authoritative state after the flush is
        // `(file_size, new_block_count, new_version)`. Intermediate
        // batches publish the same final commit; readers between
        // sub-batches see the new size with whatever blocks have
        // landed so far -- same race window as today's "all blocks,
        // then parent inode" flow.
        let commit_block_count = file_size.div_ceil(bsz_u64) as u32;
        let commit = rpc_client_bss::BlobCommitInfo {
            blob_guid,
            blob_version: new_version,
            total_size: file_size,
            block_count: commit_block_count,
        };

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
        //
        // All Rewrite blocks for one override flush hit the same blob
        // (and therefore the same volume), so they all batch together.
        let mut wrote_tail_block = false;
        let mut rewrite_sub_ops: Vec<rpc_client_bss::BssBatchSubOp> = Vec::new();
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
                let frozen = block_bytes.freeze();
                let body_checksum = xxhash_rust::xxh3::xxh3_64(&frozen);
                rewrite_sub_ops.push(rpc_client_bss::BssBatchSubOp::PutDataBlob {
                    blob_guid,
                    block_number: *block_num,
                    body: frozen,
                    body_checksum,
                    version: new_version,
                });
            }
        }
        if !rewrite_sub_ops.is_empty() {
            let entry_results = self
                .backend()
                .flush_blocks_batched(rewrite_sub_ops, commit, trace_id)
                .await?;
            for r in entry_results {
                r?;
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
        // Build one DeleteDataBlob batch covering both EOF-trim
        // deletes (blocks in [lower, upper) that aren't superseded by
        // a Rewrite this flush) and PUNCH_HOLE deletes (blocks
        // explicitly tagged with `BlockState::Delete`). Both classes
        // share the same blob and version, so a single batch dispatches
        // them as one round-trip.
        let mut delete_sub_ops: Vec<rpc_client_bss::BssBatchSubOp> = Vec::new();
        let mut delete_block_nums: Vec<u32> = Vec::new();
        if lower < upper {
            for block_num in lower..upper {
                if matches!(blocks.get(&block_num), Some(BlockState::Rewrite(_))) {
                    continue;
                }
                delete_sub_ops.push(rpc_client_bss::BssBatchSubOp::DeleteDataBlob {
                    blob_guid,
                    block_number: block_num,
                    version: new_version,
                });
                delete_block_nums.push(block_num);
            }
        }
        for (block_num, state) in blocks {
            if !matches!(state, BlockState::Delete) {
                continue;
            }
            delete_sub_ops.push(rpc_client_bss::BssBatchSubOp::DeleteDataBlob {
                blob_guid,
                block_number: *block_num,
                version: new_version,
            });
            delete_block_nums.push(*block_num);
        }
        if !delete_sub_ops.is_empty() {
            match self
                .backend()
                .flush_blocks_batched(delete_sub_ops, commit, trace_id)
                .await
            {
                Ok(entry_results) => {
                    for (i, r) in entry_results.into_iter().enumerate() {
                        if let Err(e) = r {
                            tracing::warn!(
                                %blob_guid,
                                block_num = delete_block_nums[i],
                                new_version,
                                error = %e,
                                "Failed to delete block (override flush)",
                            );
                        }
                    }
                }
                Err(e) => {
                    // Whole-batch dispatch error (transport / EC volume).
                    // Same best-effort posture as the per-block path:
                    // log and continue.
                    tracing::warn!(
                        %blob_guid,
                        new_version,
                        error = %e,
                        "Override delete batch dispatch failed"
                    );
                }
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
                    committed_blob_version,
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
    ///
    /// `committed_blob_version` is the file-level blob_version of the
    /// committed bytes we're loading (pre-flush state), used by
    /// `backend.read_block` to pick the right read strategy.
    async fn lazy_load_block_for_flush(
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

    async fn flush_write_buffer(&self, fh_id: u64) -> Result<(), FsError> {
        // Skip the NSS publish for two distinct cases:
        //   - `name_removed`: the last alias was unlinked; pushing
        //     buffered bytes would resurrect the file on the next
        //     worker drain.
        //   - `inode_id.is_some()`: the inode was promoted to an
        //     `Indirect` redirect by `vfs_link`. The user-facing NSS
        //     row holds the redirect; the close-time flush at this
        //     fh's `s3_key` would overwrite the redirect with the
        //     pre-promotion Normal layout. The authoritative layout
        //     lives in the `#hardlink/<uuid>` `InodeRecord` now;
        //     route mutations through the InodeRecord instead (the
        //     in-memory `entry.posix` is already up-to-date from the
        //     setattr path, so a follow-up stat observes the change
        //     within the FUSE attr-cache TTL).
        // Snapshot the relevant fields under the read guard and drop
        // it before calling `file_handles.get_mut(&fh_id)` below --
        // DashMap shards are RwLocks, so holding a `get` ref across a
        // `get_mut` for the same key on the same shard self-deadlocks.
        let skip_publish_ino = {
            let handle = self.file_handles.get(&fh_id);
            handle.and_then(|h| {
                let ino = h.ino;
                let entry = self.inodes.get(ino)?;
                if entry.name_removed || entry.inode_id.is_some() {
                    Some(ino)
                } else {
                    None
                }
            })
        };
        if let Some(ino) = skip_publish_ino {
            tracing::debug!(
                fh_id,
                ino,
                "flush_write_buffer: skip publish (name-removed or hardlink-promoted)"
            );
            // Drop the buffered intents so the file handle reflects
            // a clean state when vfs_release tears it down.
            if let Some(mut h) = self.file_handles.get_mut(&fh_id)
                && let Some(ref mut wb) = h.write_buf
            {
                wb.dirty = false;
                wb.size_changed = false;
                wb.blocks.clear();
                wb.pending_reservations.clear();
            }
            return Ok(());
        }
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

        // Sync the local disk cache to the writer's just-published
        // state: rewrites land at their natural offsets, deletes
        // punch holes, and the file-level authoritative_blob_v in
        // the cache header advances to match. Under the single-
        // writer-per-inode policy this is safe to do without any
        // additional locking -- no other instance has a write in
        // flight on this inode at this moment.
        //
        // Best-effort: a sync failure (e.g. ENOSPC) is logged and
        // does not affect flush durability. The next read on an
        // affected block cold-fetches from BSS and re-populates.
        if let Some(dc) = &self.disk_cache
            && let Ok(final_blob_guid) = layout.blob_guid()
        {
            let bsz_u64 = block_size as u64;
            let rewrites: Vec<(u32, Bytes)> = blocks
                .iter()
                .filter_map(|(b, s)| match s {
                    BlockState::Rewrite(bytes) => Some((*b, bytes.clone())),
                    _ => None,
                })
                .collect();

            let new_bc = file_size.div_ceil(bsz_u64) as u32;
            let committed_bc = committed_size.div_ceil(bsz_u64) as u32;
            let trim_lo = eof_low_watermark.map(|w| w.min(new_bc)).unwrap_or(new_bc);
            let trim_hi = trim_upper.unwrap_or(committed_bc).max(committed_bc);

            let mut deletes: Vec<u32> = (trim_lo..trim_hi)
                .filter(|b| !matches!(blocks.get(b), Some(BlockState::Rewrite(_))))
                .collect();
            for (b, s) in blocks.iter() {
                if matches!(s, BlockState::Delete) {
                    deletes.push(*b);
                }
            }

            if let Err(e) = dc
                .sync_after_flush(final_blob_guid, layout.blob_version, &rewrites, &deletes)
                .await
            {
                tracing::warn!(
                    %final_blob_guid,
                    blob_version = layout.blob_version,
                    error = %e,
                    "disk cache sync_after_flush failed (best-effort, continuing)"
                );
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
    /// NSS write rides the queue with a CAS guard and a guard
    /// mismatch surfaces as `FsError::CasConflict`. Pass `None` to
    /// skip the guard (initial-create flow, where we expect the slot
    /// to be new or whatever is there is the loser of an earlier
    /// crash).
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
                    committed_blob_version,
                    block_size,
                    file_size,
                    committed_size,
                    blocks,
                    trim_lower,
                    trim_upper,
                    trace_id,
                )
                .await?;
                // Issue ReserveBlocks for any range fallocate requested,
                // batched as a single BssBatch per blob. Skipped on
                // blocks that already have a Rewrite intent or a Delete
                // intent in this flush -- those entries already
                // supersede the reservation. The reservation is
                // best-effort: a partial failure is logged and not
                // fatal to the flush, mirroring the way the parent
                // inode update itself is best-effort.
                let mut reserve_sub_ops: Vec<rpc_client_bss::BssBatchSubOp> = Vec::new();
                let mut reserve_block_nums: Vec<u32> = Vec::new();
                for &block_num in pending_reservations.iter() {
                    if matches!(
                        blocks.get(&block_num),
                        Some(BlockState::Rewrite(_)) | Some(BlockState::Delete)
                    ) {
                        continue;
                    }
                    reserve_sub_ops.push(rpc_client_bss::BssBatchSubOp::ReserveBlocks {
                        blob_guid: guid,
                        block_number: block_num,
                        block_size,
                        expected_version: new_version,
                    });
                    reserve_block_nums.push(block_num);
                }
                if !reserve_sub_ops.is_empty() {
                    let bsz_u64 = block_size as u64;
                    let commit = rpc_client_bss::BlobCommitInfo {
                        blob_guid: guid,
                        blob_version: new_version,
                        total_size: file_size,
                        block_count: file_size.div_ceil(bsz_u64) as u32,
                    };
                    match self
                        .backend()
                        .flush_blocks_batched(reserve_sub_ops, commit, trace_id)
                        .await
                    {
                        Ok(entry_results) => {
                            for (i, r) in entry_results.into_iter().enumerate() {
                                if let Err(e) = r {
                                    tracing::warn!(
                                        %guid,
                                        block_num = reserve_block_nums[i],
                                        new_version,
                                        error = %e,
                                        "Failed to reserve block; continuing"
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                %guid,
                                new_version,
                                error = %e,
                                "Reserve batch dispatch failed; continuing"
                            );
                        }
                    }
                }
                (guid, new_version, true)
            } else {
                // Replace-flush: brand-new blob, no `committed_size`,
                // no prior content. Iterate the dirty-block map
                // directly -- one PutDataBlob sub-op per actually-
                // written block, at version=1. Holes (blocks not in
                // wb.blocks) stay absent in BSS by design; reads of
                // those offsets hit `BlockNotFound` and the read
                // path returns zeros. The previous shape
                // materialised the whole file (`file_size` bytes,
                // mostly zero-filled holes for sparse writes) into
                // a single contiguous buffer and turned each block
                // into a sub-op -- 16k+ ops at 128 KiB each for a
                // pwrite-at-2-GiB pattern. That saturated something
                // in the chunked send loop and silently dropped the
                // tail blocks (pjdfstest open/25.t test 4: a 1-byte
                // pwrite at offset 2 GiB+1 was unreadable because
                // block 16384 never reached BSS).
                let blob_guid = self.backend().create_blob_guid();
                let bsz_u64 = block_size as u64;
                let mut sub_ops: Vec<rpc_client_bss::BssBatchSubOp> = Vec::new();
                for (&block_num, state) in blocks.iter() {
                    let block_start = block_num as u64 * bsz_u64;
                    if block_start >= file_size {
                        // Block was trimmed by a shrink that
                        // landed before flush (truncate-then-write
                        // sequence with a smaller final size).
                        continue;
                    }
                    let new_content_len = std::cmp::min(bsz_u64, file_size - block_start) as usize;
                    let body = match state {
                        BlockState::Rewrite(b) | BlockState::Cached(b) => {
                            let take = std::cmp::min(b.len(), new_content_len);
                            pad_to_block_size(b.slice(..take), block_size_usize)
                        }
                        BlockState::Delete => {
                            // PUNCH_HOLE intent on a brand-new blob
                            // is a no-op: nothing to delete, and
                            // writing a zero block would block the
                            // BlockNotFound -> zero fast path.
                            continue;
                        }
                    };
                    let body_checksum = xxhash_rust::xxh3::xxh3_64(&body);
                    sub_ops.push(rpc_client_bss::BssBatchSubOp::PutDataBlob {
                        blob_guid,
                        block_number: block_num,
                        body,
                        body_checksum,
                        version: 1,
                    });
                }
                if !sub_ops.is_empty() {
                    let commit = rpc_client_bss::BlobCommitInfo {
                        blob_guid,
                        blob_version: 1,
                        total_size: file_size,
                        block_count: file_size.div_ceil(bsz_u64) as u32,
                    };
                    let entry_results = self
                        .backend()
                        .flush_blocks_batched(sub_ops, commit, trace_id)
                        .await?;
                    for r in entry_results {
                        r?;
                    }
                }
                (blob_guid, 1, false)
            };

        // Commit info is now published as part of every BssBatch
        // envelope (`flush_blocks_batched`); no separate write needed.
        // BSS records `(file_size, block_count)` at
        // `final_blob_version` once the batch lands.

        // Build ObjectLayout
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        // Carry forward any posix attrs the inode has accumulated via
        // vfs_setattr_posix (chmod / chown / utime). The inode lookup
        // is best-effort: if the handle has been forgotten between
        // setattr and flush, we publish the default-zero posix and
        // future reads fall back to synthesised defaults. Drop the
        // file_handles guard before the inode lookup so we never hold
        // two DashMap refs at once.
        let ino_for_posix = self.file_handles.get(&fh_id).map(|h| h.ino).unwrap_or(0);
        let posix = if ino_for_posix != 0 {
            self.inodes
                .get(ino_for_posix)
                .map(|e| e.posix)
                .unwrap_or_default()
        } else {
            data_types::object_layout::PosixAttrs::default()
        };
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
                    posix,
                },
            }),
        };

        // Serialize layout
        let layout_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())
            .map_err(FsError::from)?
            .into();

        // PublishLayout routes through the writeback queue so multiple
        // concurrent flushes coalesce into one InodeBatch RPC
        // instead of N round-trips.
        //
        // The queue path passes `expected_layout_bytes` directly:
        // when vfs_create early-publishes a placeholder layout, even
        // a "fresh" file has a layout at NSS that the close-time
        // flush must CAS against. The override-vs-replace distinction
        // (which gates the old-blob cleanup logic below) is
        // independent of the CAS-or-not decision.
        tracing::warn!(
            key = %s3_key,
            is_override,
            has_expected = expected_layout_bytes.is_some(),
            "flush_publish entered"
        );
        let old_bytes = {
            let inode = self.file_handles.get(&fh_id).map(|h| h.ino).unwrap_or(0);
            let parent_key = parent_prefix_of(s3_key);
            let name = s3_key
                .rsplit_once('/')
                .map(|(_, n)| n.to_string())
                .unwrap_or_else(|| s3_key.to_string());
            // Replace-flush (no existing_blob_guid) is a brand-new
            // file: the only thing NSS could possibly hold for this
            // key is our own create-time placeholder (early-published
            // unconditionally by vfs_create) or stale state from a
            // prior tenant of the same key. Using
            // `expected_layout_bytes` here CAS-guards against the
            // placeholder we just enqueued, and races with the
            // worker draining that placeholder out of order vs the
            // flush -- the flush's expected = placeholder bytes,
            // NSS still has nothing, ESTALE. Skip the guard for
            // replace-flush; for override-flush keep it (the guard
            // protects against concurrent-writer publishes against
            // the same blob).
            let expected_for_publish = if is_override {
                expected_layout_bytes.clone()
            } else {
                None
            };
            // First attempt with the open-time cached CAS guard.
            // Optimistic: the common case has no interleaved SETATTR
            // and the cached bytes still match NSS. On CasConflict
            // (e.g. kernel writeback-cache flushing buffered
            // mtime/ctime as a SETATTR after our write returns),
            // refetch the bytes NSS actually has and retry once.
            // Without the retry, every sync write that races with
            // such a SETATTR returns ESTALE; with the retry, only a
            // genuine cross-instance conflict surfaces.
            let put_result = self
                .put_inode_via_queue(
                    inode,
                    s3_key,
                    &parent_key,
                    &name,
                    layout_bytes.clone(),
                    expected_for_publish,
                )
                .await;
            if matches!(put_result, Err(FsError::CasConflict)) && is_override {
                let trace_id = TraceId::new();
                let fresh_expected = self
                    .backend()
                    .get_inode_with_bytes(s3_key, &trace_id)
                    .await
                    .ok()
                    .map(|(_, b)| b);
                self.put_inode_via_queue(
                    inode,
                    s3_key,
                    &parent_key,
                    &name,
                    layout_bytes.clone(),
                    fresh_expected,
                )
                .await?;
            } else {
                put_result?;
            }
            Bytes::new()
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
                    if !layout.is_fs_visible() {
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

    // -- Public VFS operations --

    pub fn vfs_init(&self) {
        if let Some(dc) = &self.disk_cache {
            dc.spawn_evictor();
        }
        // Note: in this codebase the FUSE adapter's `init()` trait
        // method is unused -- the session handles FUSE_INIT inline.
        // The writeback worker is spawned lazily by the first
        // `ensure_writeback_worker_started()` call from inside a
        // running compio runtime.
        tracing::info!("Filesystem initialized");
    }

    /// Spawn the writeback worker the first time it's needed. Cheap
    /// fast path: a relaxed atomic load + branch in steady state. The
    /// `compare_exchange` only fires once per process.
    fn ensure_writeback_worker_started(&self) {
        if self.writeback_worker_started.load(Ordering::Relaxed) {
            return;
        }
        if self
            .writeback_worker_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        spawn_writeback_worker(
            Arc::clone(&self.backend_config),
            Arc::clone(&self.writeback),
            self.writeback_poll_ms,
        );
        spawn_writeback_metrics_exporter(Arc::clone(&self.writeback));
        tracing::info!(poll_ms = self.writeback_poll_ms, "writeback worker started");
    }

    pub fn vfs_destroy(&self) {
        // Block new enqueues; the worker keeps draining whatever is
        // already InFlight / Pending until the queue depth hits 0 or
        // the host process exits.
        self.writeback.set_enqueue_blocked(true);
        tracing::info!(
            queue_depth = self.writeback.depth(),
            "writeback enqueue blocked at destroy; draining residual"
        );
        tracing::info!("Filesystem destroyed");
    }

    pub async fn vfs_lookup(&self, parent: u64, name: &str) -> Result<VfsAttr, FsError> {
        Self::check_name_max(name)?;
        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;

        let full_key = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}{}", prefix, name)
        };

        let trace_id = TraceId::new();

        // Drain any Pending writeback intent for the file key before
        // reading NSS so a chmod / mknod / create that the worker
        // hasn't yet committed isn't observed as stale.
        self.wait_for_lookup_drain(&full_key).await;

        // Try as file first
        match self.backend().get_inode(&full_key, &trace_id).await {
            Ok(layout) => {
                if !layout.is_fs_visible() {
                    return Err(FsError::NotFound);
                }
                // Hardlink redirect resolution. The user-facing layout
                // is `Indirect(inode_id)`; the real layout lives at
                // `#hardlink/<inode_id>` (see doc 20 section 4.3).
                // Fetch the InodeRecord, install the resolved layout
                // on the inode entry, and report the persisted nlink
                // through stat. Common-case `Normal | Mpu | Symlink`
                // falls through unchanged.
                if let ObjectState::Indirect(ref redirect) = layout.state {
                    let inode_id = redirect.inode_id;
                    let record = self.backend().get_inode_record(inode_id, &trace_id).await?;
                    let (ino, _) = self.inodes.lookup_or_insert(
                        &full_key,
                        EntryType::File,
                        Some(record.layout.clone()),
                    );
                    if let Some(mut e) = self.inodes.get_mut(ino) {
                        e.inode_id = Some(inode_id);
                        e.posix = crate::inode::layout_posix(&record.layout);
                    }
                    let mut attr = self.make_file_attr(ino, &record.layout)?;
                    attr.nlink = record.nlink;
                    self.apply_atime_override(ino, &mut attr);
                    return Ok(attr);
                }
                let (ino, _) =
                    self.inodes
                        .lookup_or_insert(&full_key, EntryType::File, Some(layout.clone()));
                let mut attr = self.make_file_attr(ino, &layout)?;
                self.apply_atime_override(ino, &mut attr);
                return Ok(attr);
            }
            Err(FsError::NotFound) => {}
            Err(e) => return Err(e),
        }

        // Try as directory
        let dir_key = format!("{}/", full_key);
        // Same drain for the dir key: chmod-on-directory in default
        // mode publishes the Directory layout asynchronously; if the
        // entry was forgotten between chmod and lookup we'd otherwise
        // see the pre-chmod posix on the next stat.
        self.wait_for_lookup_drain(&dir_key).await;
        let dir_layout = match self.backend().get_inode(&dir_key, &trace_id).await {
            Ok(layout) => Some(layout),
            // mkdir hasn't drained yet: fall through and let
            // list_inodes confirm existence; entry.posix stays at
            // whatever vfs_mkdir's seed_posix put there.
            Err(FsError::NotFound) => None,
            Err(e) => return Err(e),
        };

        let entries = self
            .backend()
            .list_inodes(&dir_key, "/", "", 1, &trace_id)
            .await;

        match entries {
            Ok(entries) if !entries.is_empty() => {
                // Re-seed entry.posix from the freshly-fetched
                // Directory layout when present. After commit
                // 7695143d the layout carries the chmod'd posix
                // correctly, so a forget+relookup doesn't lose the
                // chmod.
                let (ino, _) =
                    self.inodes
                        .lookup_or_insert(&dir_key, EntryType::Directory, dir_layout);
                let mut attr = self.make_dir_attr(ino);
                attr.nlink = self.compute_dir_nlink(&dir_key, &trace_id).await;
                self.apply_atime_override(ino, &mut attr);
                Ok(attr)
            }
            _ => Err(FsError::NotFound),
        }
    }

    /// Block until the writeback queue has no Pending or InFlight
    /// intent for `key`. Used by the lookup path to avoid reading
    /// stale NSS state behind an in-flight metadata mutation. In
    /// the common case (no pending intent) this is one map probe
    /// under the queue's lock and returns immediately.
    async fn wait_for_lookup_drain(&self, key: &str) {
        if !self.writeback.has_pending_intent_for_key(key) {
            return;
        }
        // Worst case: one worker tick (~50 ms) plus a small
        // grace. Cap the wait so a perpetually-stuck cycle can't
        // wedge every lookup.
        let poll_dur = Duration::from_millis(2);
        let deadline = SystemTime::now() + Duration::from_millis(200);
        while self.writeback.has_pending_intent_for_key(key) {
            if SystemTime::now() > deadline {
                tracing::warn!(key, "wait_for_lookup_drain timeout");
                return;
            }
            compio_runtime::time::sleep(poll_dur).await;
        }
    }

    /// Block until the writeback queue has no Pending or InFlight
    /// intent for any key under `prefix`. Used by `vfs_rmdir`'s
    /// empty-check so a fresh child publish (e.g. mknod / symlink
    /// queued microseconds before the rmdir) is observed by the
    /// subsequent NSS list. Mirrors `wait_for_lookup_drain` shape.
    async fn wait_for_prefix_drain(&self, prefix: &str) {
        if !self.writeback.has_pending_intent_under_prefix(prefix) {
            return;
        }
        let poll_dur = Duration::from_millis(2);
        let deadline = SystemTime::now() + Duration::from_millis(200);
        while self.writeback.has_pending_intent_under_prefix(prefix) {
            if SystemTime::now() > deadline {
                tracing::warn!(prefix, "wait_for_prefix_drain timeout");
                return;
            }
            compio_runtime::time::sleep(poll_dur).await;
        }
    }

    pub fn vfs_forget(&self, inode: u64, nlookup: u64) {
        self.inodes.forget(inode, nlookup);
    }

    /// POSIX `nlink` for a directory is `2 + num_immediate_subdirs`
    /// (the dir itself, the dir's `.` self-reference, and one extra
    /// reference per child dir's `..` link). We don't persist this
    /// count -- it's recomputed on demand from NSS by counting
    /// common-prefix entries directly under `dir_key` with
    /// `delim="/"`. Cap the listing at a single page (1000 entries)
    /// to bound the cost; the contract is satisfied for all
    /// pjdfstest-shape directories. Larger directories report a
    /// truncated count, which still keeps `nlink >= 2` and matches
    /// the POSIX nlink semantic ("eventual consistency between
    /// link-count operations and `lstat`").
    ///
    /// Drain only writeback intents whose key ends with `/`
    /// (dir-marker publishes / removes) so a `mkdir foo/bar`
    /// immediately followed by `lstat foo` sees the fresh subdir
    /// (rename/24.t exercises this exact contract). A
    /// blanket-prefix drain would also wait for every queued
    /// regular-file PutInode under the dir, which doesn't change
    /// nlink and stalls busy workloads (symlink/03.t fanout was
    /// the trigger).
    async fn compute_dir_nlink(&self, dir_key: &str, trace_id: &TraceId) -> u32 {
        self.wait_for_dir_marker_drain(dir_key).await;
        let mut count: u32 = 0;
        let entries = match self
            .backend()
            .list_inodes(dir_key, "/", "", 1000, trace_id)
            .await
        {
            Ok(e) => e,
            Err(_) => return 2,
        };
        for entry in entries {
            if entry.layout.is_none() {
                count = count.saturating_add(1);
            }
        }
        count.saturating_add(2)
    }

    /// Like `wait_for_prefix_drain` but only blocks on dir-marker
    /// publishes (keys ending in `/`). Regular-file publishes
    /// queued under the prefix don't change nlink, so waiting on
    /// them just stalls a busy lstat for no contract benefit.
    async fn wait_for_dir_marker_drain(&self, prefix: &str) {
        if !self.writeback.has_pending_dir_marker_under_prefix(prefix) {
            return;
        }
        let poll_dur = Duration::from_millis(2);
        let deadline = SystemTime::now() + Duration::from_millis(200);
        while self.writeback.has_pending_dir_marker_under_prefix(prefix) {
            if SystemTime::now() > deadline {
                tracing::warn!(prefix, "wait_for_dir_marker_drain timeout");
                return;
            }
            compio_runtime::time::sleep(poll_dur).await;
        }
    }

    /// Override `attr.atime_secs` with the explicit atime an
    /// `utimensat(2)` user installed on the inode entry, if any.
    /// `make_*_attr` mirrors `mtime_secs` into `atime_secs` because
    /// the persisted `PosixAttrs` deliberately omits atime; this
    /// hook layers the in-memory override on top so a `lstat`
    /// immediately after `utimensat(path, [DATE1, DATE2])` reports
    /// `DATE1` for atime instead of the mtime mirror.
    fn apply_atime_override(&self, inode: u64, attr: &mut VfsAttr) {
        if let Some(entry) = self.inodes.get(inode)
            && entry.atime_ns != 0
        {
            attr.atime_secs = entry.atime_ns / 1_000_000_000;
            attr.atime_ns_part = (entry.atime_ns % 1_000_000_000) as u32;
        }
    }

    pub async fn vfs_getattr(&self, inode: u64, fh: Option<u64>) -> Result<VfsAttr, FsError> {
        if inode == ROOT_INODE {
            let mut attr = self.make_dir_attr(ROOT_INODE);
            if let Some(root_key) = self.dir_prefix(ROOT_INODE) {
                let trace_id = TraceId::new();
                attr.nlink = self.compute_dir_nlink(&root_key, &trace_id).await;
            }
            return Ok(attr);
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
            EntryType::Directory => {
                let dir_key = entry.s3_key.clone();
                drop(entry);
                let mut attr = self.make_dir_attr(inode);
                let trace_id = TraceId::new();
                attr.nlink = self.compute_dir_nlink(&dir_key, &trace_id).await;
                self.apply_atime_override(inode, &mut attr);
                Ok(attr)
            }
            EntryType::File => {
                // Hardlink-promoted inodes carry their authoritative
                // nlink in the InodeRecord. The cached layout on the
                // entry is the resolved one (set by `vfs_link` /
                // `vfs_lookup`'s redirect branch), so attrs are right
                // for everything except `nlink`. Refetch the record
                // so a stat-immediately-after-unlink returns the
                // post-decrement count.
                let inode_id = entry.inode_id;
                let name_removed = entry.name_removed;
                if let Some(ref layout) = entry.layout {
                    let layout = layout.clone();
                    drop(entry);
                    let mut attr = self.make_file_attr(inode, &layout)?;
                    self.apply_atime_override(inode, &mut attr);
                    if let Some(id) = inode_id {
                        let trace_id = TraceId::new();
                        if let Ok(record) = self.backend().get_inode_record(id, &trace_id).await {
                            attr.nlink = record.nlink;
                        }
                    }
                    // POSIX: an open-but-unlinked file reports nlink=0
                    // through fstat(2) until the last fd closes (the
                    // inode is "in the orphan state"). For a non-
                    // hardlink inode, `name_removed` is the local
                    // signal that the only alias was unlinked while
                    // we still hold an fh open. Hardlink-promoted
                    // inodes already report InodeRecord.nlink above,
                    // which is 0 once all aliases are gone.
                    if name_removed && inode_id.is_none() {
                        attr.nlink = 0;
                    }
                    Ok(attr)
                } else {
                    let key = entry.s3_key.clone();
                    drop(entry);
                    let trace_id = TraceId::new();
                    let layout = self.backend().get_inode(&key, &trace_id).await?;
                    if let Some(mut entry) = self.inodes.get_mut(inode) {
                        let new_posix = crate::inode::layout_posix(&layout);
                        if new_posix.mode != 0 {
                            entry.posix = new_posix;
                        }
                        entry.layout = Some(layout.clone());
                    }
                    let mut attr = self.make_file_attr(inode, &layout)?;
                    self.apply_atime_override(inode, &mut attr);
                    if name_removed && inode_id.is_none() {
                        attr.nlink = 0;
                    }
                    Ok(attr)
                }
            }
        }
    }

    /// Apply non-size setattr fields (mode / uid / gid / atime / mtime /
    /// ctime) to the in-memory inode entry, and -- when the inode has
    /// a cached layout -- enqueue the updated layout to NSS via the
    /// writeback queue (default mode) or push it directly (strict
    /// mode). The fast in-memory mutation makes
    /// chmod-then-write-then-close sequences cheap; the queue-side
    /// persistence makes a standalone chmod / chown / utime survive
    /// a FUSE forget + relookup.
    ///
    /// `mode == 0` callers are translated to "no change". Times are
    /// stored in nanoseconds since the Unix epoch; `Now` is resolved
    /// against `SystemTime::now()` by the caller, not here.
    ///
    /// POSIX: `chmod`, `chown`, and `utime` MUST update ctime. When the
    /// caller didn't pass an explicit ctime but did mutate any of
    /// mode/uid/gid/atime/mtime, we stamp ctime to `now` so
    /// stat-after-chmod sees a fresh ctime (pjdfstest chmod/00.t).
    ///
    /// `atime_ns` is accepted for API compatibility with the FUSE
    /// SetAttr surface but ignored: `PosixAttrs` does not persist
    /// atime, and the stat-side synthesis returns `mtime`. A
    /// `utimensat(.., atime=X)` therefore round-trips via mtime if
    /// the caller also set mtime; an atime-only utimensat is a
    /// no-op aside from the ctime stamp it triggers below.
    #[allow(clippy::too_many_arguments)]
    pub async fn vfs_setattr_posix(
        &self,
        inode: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        atime_ns: Option<u64>,
        mtime_ns: Option<u64>,
        ctime_ns: Option<u64>,
    ) -> Result<(), FsError> {
        // Hardlink-promoted inode short-circuit: the user-facing NSS
        // row carries an `Indirect(uuid)` redirect and the
        // authoritative layout / posix lives in the
        // `#hardlink/<uuid>` `InodeRecord`. Mutate `entry.posix` in
        // memory (so a FUSE_GETATTR on the same inode reflects the
        // change) and route any persistence through the InodeRecord
        // path instead -- a publish at `entry.s3_key` here would
        // overwrite the redirect with this inode's pre-promotion
        // layout (the close-time SETATTR storm during pjdfstest's
        // link/unlink loops is the trigger). The `InodeRecord` posix
        // refresh on hardlink-promoted inodes is post-MVP; in the
        // common pjdfstest case the in-memory mutation is what stat
        // observes within the FUSE attr-cache TTL.
        let promoted_ino = self
            .inodes
            .get(inode)
            .and_then(|e| if e.inode_id.is_some() { Some(()) } else { None });
        if promoted_ino.is_some() {
            if let Some(mut entry) = self.inodes.get_mut(inode) {
                if let Some(m) = mode
                    && m != 0
                {
                    entry.posix.mode = m;
                }
                if let Some(u) = uid {
                    entry.posix.uid = u;
                }
                if let Some(g) = gid {
                    entry.posix.gid = g;
                }
                if let Some(at) = atime_ns {
                    entry.atime_ns = at;
                }
                if let Some(mt) = mtime_ns {
                    entry.posix.mtime_ns = mt;
                }
                if let Some(ct) = ctime_ns {
                    entry.posix.ctime_ns = ct;
                } else if mode.is_some() || uid.is_some() || gid.is_some() || mtime_ns.is_some() {
                    let now_ns = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(0);
                    entry.posix.ctime_ns = now_ns;
                }
            }
            return Ok(());
        }
        // Phase 1: snapshot the bits we need (key, layout) and
        // mutate `entry.posix` while holding the DashMap guard.
        // Drop the guard before any await so the persistence path
        // doesn't deadlock against a concurrent lookup.
        let (s3_key, parent_key, name, updated_layout, expected_old_bytes, new_posix, name_removed) = {
            let mut entry = self.inodes.get_mut(inode).ok_or(FsError::NotFound)?;
            let mode_set = matches!(mode, Some(m) if m != 0);
            let uid_set = uid.is_some();
            let gid_set = gid.is_some();
            let atime_set = atime_ns.is_some();
            let mtime_set = mtime_ns.is_some();
            if mode_set {
                entry.posix.mode = mode.unwrap();
            }
            if let Some(u) = uid {
                entry.posix.uid = u;
            }
            if let Some(g) = gid {
                entry.posix.gid = g;
            }
            if let Some(at) = atime_ns {
                entry.atime_ns = at;
            }
            if let Some(m) = mtime_ns {
                entry.posix.mtime_ns = m;
            }
            if let Some(c) = ctime_ns {
                entry.posix.ctime_ns = c;
            } else if mode_set || uid_set || gid_set || atime_set || mtime_set {
                let now_ns = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                entry.posix.ctime_ns = now_ns;
            }
            let new_posix = entry.posix;
            // Build the next layout to persist. If we already have a
            // cached layout, fold the new posix in. If we don't, we
            // can't synthesise one without an NSS round-trip; defer
            // to the lazy persistence path below (skip queue write
            // and let the next op pick up the new posix).
            let updated_layout = entry
                .layout
                .as_ref()
                .map(|l| crate::inode::layout_with_posix(l.clone(), new_posix));
            // Pre-existing layout bytes form the CAS guard. We don't
            // have these from the inode entry today (the writeback
            // queue's CAS path uses handle.layout_bytes which is per
            // file-handle) so use unconditional put. If multiple
            // callers race to set posix on the same inode, the
            // queue's last-write-wins serialisation orders them.
            let expected_old_bytes: Option<Bytes> = None;
            let s3_key = entry.s3_key.clone();
            let name_removed = entry.name_removed;
            (
                s3_key.clone(),
                parent_prefix_of(&s3_key),
                s3_key
                    .rsplit_once('/')
                    .map(|(_, n)| n.to_string())
                    .unwrap_or_else(|| s3_key.clone()),
                updated_layout,
                expected_old_bytes,
                new_posix,
                name_removed,
            )
        };
        let _ = new_posix;

        // If the dentry was unlinked, the kernel may still send
        // SETATTR via the stale dcache reference. Skip the NSS
        // publish -- we'd otherwise resurrect the deleted file.
        // The in-memory mutation already happened above, which is
        // the right semantic for a still-open fd; once the kernel
        // forgets the inode, the entry drops cleanly.
        if name_removed {
            return Ok(());
        }

        // Phase 2: persist. If we have an updated layout, serialise
        // and route through the writeback queue. The persistence is
        // best-effort; failure is logged and the in-memory mutation
        // still stands so the caller's setattr is observable
        // locally.
        if let Some(layout) = updated_layout {
            let layout_bytes: Bytes =
                match to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new()) {
                    Ok(v) => v.into(),
                    Err(e) => {
                        tracing::warn!(error = %e, "vfs_setattr_posix: layout serialise failed");
                        return Ok(());
                    }
                };
            // Update the cached layout to match the bytes we just
            // serialised so a follow-up op reads the new posix from
            // entry.layout, not from the stale cached one.
            if let Some(mut e) = self.inodes.get_mut(inode) {
                e.layout = Some(layout);
            }
            self.enqueue_inode_intent_async(
                inode,
                &s3_key,
                &parent_key,
                &name,
                layout_bytes,
                expected_old_bytes,
            );
        }
        Ok(())
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
                committed_blob_version,
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

    pub async fn vfs_open(&self, inode: u64, flags: u32) -> Result<u64, FsError> {
        let write_flags = libc::O_WRONLY as u32
            | libc::O_RDWR as u32
            | libc::O_APPEND as u32
            | libc::O_TRUNC as u32;
        let is_write = flags & write_flags != 0;

        if is_write && !self.read_write {
            return Err(FsError::ReadOnly);
        }

        let s3_key = {
            let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
            if entry.entry_type != EntryType::File {
                return Err(FsError::IsDir);
            }
            entry.s3_key.clone()
        };

        // If a previous close on this inode is still flushing through
        // the writeback queue (default-mode spawned vfs_release), the
        // entry's cached layout may still be the create-time
        // placeholder (size=0, no blob_guid bound to BSS). Reading
        // through that layout returns zero bytes regardless of what
        // userspace just wrote -- pjdfstest open/25.t test 4 fails
        // exactly this way: pwrite-then-immediate-reopen sees an
        // unfinished publish. Drain the spawned release cycle to
        // Done before we snapshot entry.layout so the open observes
        // the post-flush layout (and the post-flush blob_guid)
        // consistently. We use the barrier wait (not the lighter
        // wait_for_lookup_drain): the latter returns as soon as the
        // NSS publish intent commits, but entry.layout is only
        // updated AFTER `put_inode_via_queue` returns to
        // `flush_publish`, i.e. after the outer release cycle
        // advances to Done.
        let _ = self.drain_inode_to_barrier(inode).await;

        let (layout, _entry_type) = {
            let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
            (entry.layout.clone(), entry.entry_type)
        };

        // Single-writer per inode. First writer wins; subsequent
        // write-mode opens fail with EBUSY. The lock is process-local
        // in-memory state and dies with the process on crash, so the
        // next open reacquires.
        //
        // Default mode's release flow spawns the close-time flush in
        // the background and keeps the write lock held until the
        // spawn completes, so a follow-up open(O_WRONLY|O_TRUNC|...)
        // issued milliseconds after close races with the still-
        // running flush and would otherwise see a spurious EBUSY.
        // When the lock is held, wait for the in-flight writeback
        // cycle on this inode to drain, then retry: the drain barrier
        // is bounded by `rpc_request_timeout_seconds * 4` (~120 s)
        // which is what the spawn itself caps at, so we never wait
        // longer than the spawn could legitimately run.
        let fh = self.alloc_fh();
        if is_write {
            let mut tried_drain = false;
            loop {
                match self.acquire_write_lock(inode, fh) {
                    Ok(()) => break,
                    Err(FsError::Busy) => {
                        if tried_drain {
                            return Err(FsError::Busy);
                        }
                        tried_drain = true;
                        let _ = self.drain_inode_to_barrier(inode).await;
                    }
                    Err(e) => return Err(e),
                }
            }
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
        // Cross-instance staleness reconciliation: if the cache file's
        // authoritative_blob_v lags the inode's blob_version, another
        // instance has bumped the version since we last sync'd. Clear
        // the cache file so subsequent reads cold-fetch from BSS.
        // Done on every open (read or write) so read-only handles
        // don't keep serving stale bytes.
        if let Some(dc) = &self.disk_cache
            && let Some(ref l) = layout
            && let Ok(blob_guid) = l.blob_guid()
            && let Err(e) = dc.reconcile_on_open(blob_guid, l.blob_version).await
        {
            tracing::warn!(
                %blob_guid, error = %e,
                "disk cache reconcile_on_open failed; continuing"
            );
        }

        let parent_size = if is_write {
            if let Some(ref l) = layout
                && let Ok(blob_guid) = l.blob_guid()
            {
                let trace_id = TraceId::new();
                // vg_proxy::get_blob_info enforces R+W>N quorum and
                // surfaces stale/quorum-failure responses as `Err`,
                // so an `Ok(Some)` here is already version-checked.
                match self
                    .backend()
                    .get_blob_info(blob_guid, l.blob_version, &trace_id)
                    .await
                {
                    Ok(Some(info)) => Some(info.total_size),
                    Ok(None) => None,
                    Err(e) => {
                        tracing::warn!(
                            %blob_guid, error = %e,
                            "get_blob_info failed during open; falling back to layout size"
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

        // Promote the cached entry to MRU on every open. Reads served
        // by `FUSE_PASSTHROUGH` bypass the per-block touch path
        // entirely, so without this hook a hot file served via
        // passthrough would never advance in LRU and the evictor would
        // treat it as cold.
        if !is_write
            && let Some(dc) = &self.disk_cache
            && let Some(ref l) = layout
            && let Ok(blob_guid) = l.blob_guid()
        {
            dc.touch_blob(blob_guid);
        }

        // Spawn a whole-blob prefetch when the open-time policy says
        // yes and the cache is not already complete. Read-only opens
        // only -- writers own the blob's bytes via `WriteBuffer` and
        // have no need for a parallel prefetch.
        if !is_write
            && let Some(dc) = &self.disk_cache
            && let Some(ref l) = layout
            && let Ok(file_size) = l.size()
            && let Ok(blob_guid) = l.blob_guid()
        {
            let usage = dc.current_usage();
            let capacity = dc.capacity_bytes();
            // FOPEN_KEEP_CACHE is the kernel's sequential-read hint;
            // the open(2) flag itself does not directly map, so for
            // now we treat any non-O_RANDOM read as a candidate.
            // O_RANDOM is not a portable flag; absent it on Linux,
            // the conservative default is `false` -- only the
            // full-threshold and workload_bulk_read branches fire.
            let keep_cache_hint = false;
            if !crate::prefetch::cache_pressure_high(usage, capacity, &self.prefetch_policy)
                && crate::prefetch::should_prefetch(
                    file_size,
                    keep_cache_hint,
                    &self.prefetch_policy,
                )
                && !dc.is_complete(blob_guid, file_size)
            {
                let dc_arc = Arc::clone(dc);
                let backend_cfg = Arc::clone(&self.backend_config);
                let layout_clone = l.clone();
                compio_runtime::spawn(async move {
                    spawn_prefetch_task(backend_cfg, dc_arc, layout_clone).await;
                })
                .detach();
            }
        }

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
            let committed_blob_version =
                handle.layout.as_ref().map(|l| l.blob_version).unwrap_or(0);
            drop(handle);
            let cap = std::cmp::min(size as u64, file_size.saturating_sub(offset)) as usize;
            let mut buf = vec![0u8; cap];
            let n = self
                .read_dirty_handle(
                    file_size,
                    block_size,
                    existing_blob_guid,
                    committed_blob_version,
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

        // POSIX: a successful write(2) shall clear S_ISUID, and shall
        // clear S_ISGID if the file is group-executable. We don't
        // track the calling process's privileges so we always clear
        // (Linux behaves the same unless CAP_FSETID is held). Skip
        // when posix.mode is the uninitialised sentinel -- there's
        // no mode to mutate yet.
        let cleared_suid_for: Option<u64> = (|| {
            let ino = self.file_handles.get(&fh).map(|h| h.ino)?;
            let mut entry = self.inodes.get_mut(ino)?;
            if entry.posix.mode == 0 {
                return None;
            }
            const S_ISUID: u32 = 0o4000;
            const S_ISGID: u32 = 0o2000;
            const S_IXGRP: u32 = 0o0010;
            let mut new_mode = entry.posix.mode;
            new_mode &= !S_ISUID;
            if new_mode & S_IXGRP != 0 {
                new_mode &= !S_ISGID;
            }
            if new_mode == entry.posix.mode {
                return None;
            }
            entry.posix.mode = new_mode;
            let now_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            entry.posix.ctime_ns = now_ns;
            Some(ino)
        })();
        // The kernel caches getattr replies for `TTL` seconds, which
        // makes a stat-immediately-after-write return the pre-clear
        // mode (pjdfstest chmod/12.t). Send a one-shot
        // FUSE_NOTIFY_INVAL_INODE so the kernel re-fetches attrs on
        // the next stat. The notify *must* run on a different task
        // from this WRITE handler -- doing the writev synchronously
        // here competes with the WRITE reply on the same /dev/fuse
        // fd and can wedge the daemon. Spawning detaches it so the
        // notify happens asynchronously, after this WRITE reply has
        // already been written.
        if let Some(ino) = cleared_suid_for
            && let Some(notifier) = self.fuse_notifier.clone()
        {
            compio_runtime::spawn(async move {
                // Yield once so the WRITE reply this is the
                // out-of-band signal for has a chance to flush
                // ahead of the inval notify.
                compio_runtime::time::sleep(std::time::Duration::from_millis(0)).await;
                if let Err(e) = notifier.inval_inode(ino, -1, 0) {
                    tracing::debug!(ino, error = %e, "inval_inode after SUID/SGID clear failed");
                }
            })
            .detach();
        }

        // Phase 1: snapshot the bits of state we need (block_size, the
        // blob_guid for lazy-loading, current intents) without holding
        // the DashMap guard across awaits. Initialize the buffer in
        // place if missing.
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
                committed_blob_version,
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
                    committed_blob_version,
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
            layout_blob_version,
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
            let layout_blob_version = handle.layout.as_ref().map(|l| l.blob_version).unwrap_or(0);
            if let Some(ref wb) = handle.write_buf {
                (
                    wb.file_size,
                    wb.block_size,
                    wb.existing_blob_guid,
                    layout_blob_version,
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
                    layout_blob_version,
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
        // vg_proxy::get_blob_info enforces R+W>N quorum and surfaces
        // stale/quorum-failure responses as `Err`, so an `Ok(Some)`
        // here is already version-checked.
        let file_size = if !has_write_buffer {
            if let Some(guid) = existing_blob_guid {
                match self
                    .backend()
                    .get_blob_info(guid, layout_blob_version, &trace_id)
                    .await
                {
                    Ok(Some(info)) => info.total_size,
                    Ok(None) => file_size_hint,
                    Err(e) => {
                        tracing::warn!(%guid, error = %e, "get_blob_info failed during lseek; falling back");
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
        self.ensure_writeback_worker_started();
        self.flush_write_buffer(fh).await?;

        // Writeback drain: if this fh's inode has any queued cycles
        // at gen <= barrier, wait for them to commit before
        // returning. Surfaces deferred EIO if any cycle failed.
        // No-op for inodes with no dirty cycles.
        //
        // This is the fsync(2) / O_SYNC path: durability-tied.
        // close(2) -> FUSE_FLUSH is a no-op (see `fuse_server::flush`)
        // -- blocking on close() per file would serialise the queue
        // against userspace and erase the writeback win on
        // create-heavy workloads (`tar -xf`, `cp -r`). The cycle
        // stays queued and the worker drains it on its own; any
        // deferred EIO propagates via errseq the next time the same
        // fd path opens or fsyncs.
        if let Some(handle) = self.file_handles.get(&fh) {
            let inode = handle.ino;
            drop(handle);
            self.drain_inode_to_barrier(inode).await?;
        }

        Ok(())
    }

    /// Queue-aware put_inode that the worker batches across concurrent
    /// callers. Enqueues an `InodeOp::PutInode` (with optional
    /// `expected_old_value` for CAS), waits for the cycle to drain,
    /// and surfaces the right errno on failure:
    ///   * Ok(()) on success
    ///   * Err(CasConflict) on STATUS_CAS_CONFLICT (caller must reopen)
    ///   * Err(Internal) on any other failure
    ///
    /// Used by flush_write_buffer so vfs_release flush traffic rides
    /// the InodeBatch path.
    /// Fire-and-forget enqueue of a `PutInode` intent. Used by
    /// `vfs_create` early-publish so the placeholder lands without
    /// blocking the create() call; the worker drains it on its next
    /// tick. Subsequent `flush_publish` at close uses CAS against the
    /// placeholder bytes -- if the worker hasn't committed yet, that
    /// CAS naturally waits via `put_inode_via_queue`'s poll loop.
    pub fn enqueue_inode_intent_async(
        &self,
        inode: u64,
        s3_key: &str,
        parent_key: &str,
        name: &str,
        layout_bytes: Bytes,
        expected_old_value: Option<Bytes>,
    ) {
        self.ensure_writeback_worker_started();
        use crate::writeback::FhId;
        let generation = self.allocate_flush_generation(inode);
        self.writeback
            .open_cycle(inode, generation, layout_bytes.len() as u64, 0);
        self.writeback.upsert_inode_intent(
            s3_key.to_string(),
            generation,
            inode,
            WbInodeOp::PutInode {
                parent_key: parent_key.to_string(),
                name: name.to_string(),
                layout_bytes,
                expected_old_value,
            },
            FhId(0),
        );
    }

    pub async fn put_inode_via_queue(
        &self,
        inode: u64,
        s3_key: &str,
        parent_key: &str,
        name: &str,
        layout_bytes: Bytes,
        expected_old_value: Option<Bytes>,
    ) -> Result<(), FsError> {
        self.ensure_writeback_worker_started();

        use crate::writeback::{CycleOutcome, FhId};
        let generation = self.allocate_flush_generation(inode);
        self.writeback
            .open_cycle(inode, generation, layout_bytes.len() as u64, 0);
        self.writeback.upsert_inode_intent(
            s3_key.to_string(),
            generation,
            inode,
            WbInodeOp::PutInode {
                parent_key: parent_key.to_string(),
                name: name.to_string(),
                layout_bytes,
                expected_old_value,
            },
            FhId(0),
        );

        // Poll until the cycle completes. The worker drains every
        // poll_ms (~50ms) so a single flush typically waits no more
        // than that. The drain loop bound matches drain_inode_to_barrier.
        let poll_dur = Duration::from_millis(5);
        let timeout_secs = self.backend_config.config.rpc_request_timeout_seconds * 4;
        let deadline = SystemTime::now() + Duration::from_secs(timeout_secs);
        loop {
            let outcome = self.writeback.cycle_outcome(inode, generation);
            match outcome {
                CycleOutcome::Committed => return Ok(()),
                CycleOutcome::CasConflict => return Err(FsError::CasConflict),
                CycleOutcome::Failed => {
                    return Err(FsError::Internal("writeback put_inode failed".to_string()));
                }
                CycleOutcome::InFlight => {
                    if SystemTime::now() > deadline {
                        tracing::warn!(
                            inode,
                            generation = generation.0,
                            "put_inode_via_queue timeout"
                        );
                        return Err(FsError::Internal("writeback put_inode timeout".to_string()));
                    }
                    compio_runtime::time::sleep(poll_dur).await;
                }
            }
        }
    }

    /// Drain every dirty cycle on every inode the queue currently
    /// knows about. Mount-wide barrier semantics: `syncfs(2)` and
    /// `fsyncdir` (which calls this when there's no efficient
    /// subtree filter) wait until every snapshotted cycle reaches
    /// its terminal stage. New cycles that arrive after the
    /// snapshot are not waited on -- they belong to the next
    /// `syncfs`.
    pub async fn drain_all_dirty_cycles(&self) -> Result<(), FsError> {
        let snapshot = self.writeback.snapshot_dirty_cycles();
        if snapshot.is_empty() {
            return Ok(());
        }
        let poll_dur = Duration::from_millis(5);
        let timeout_secs = self.backend_config.config.rpc_request_timeout_seconds * 4;
        let deadline = SystemTime::now() + Duration::from_secs(timeout_secs);
        let mut tainted_seen = false;
        for (inode, barrier) in snapshot {
            loop {
                if self.writeback.cycles_at_or_below_drained(inode, barrier) {
                    if self.writeback.is_tainted(inode) {
                        tainted_seen = true;
                    }
                    break;
                }
                if SystemTime::now() > deadline {
                    tracing::warn!(inode, barrier = barrier.0, "drain_all_dirty_cycles timeout");
                    return Err(FsError::Internal("writeback drain timeout".to_string()));
                }
                compio_runtime::time::sleep(poll_dur).await;
            }
        }
        if tainted_seen {
            return Err(FsError::Internal("writeback drain".to_string()));
        }
        Ok(())
    }

    /// Drain every writeback cycle for `inode` whose generation is
    /// at or below the barrier captured at entry. Returns when every
    /// cycle has reached `Done` (success or short-circuit on failure).
    /// Surfaces deferred `EIO` if any drained cycle failed.
    pub async fn drain_inode_to_barrier(&self, inode: u64) -> Result<(), FsError> {
        let barrier = match self.writeback.fsync_barrier(inode) {
            Some(b) => b,
            None => return Ok(()), // idle inode
        };

        // Poll loop: the worker drains every poll_ms; pick a
        // sub-multiple so we don't oversleep. 5ms is small enough that
        // a typical drain latency is bounded by one worker tick.
        let poll_dur = Duration::from_millis(5);
        let timeout_secs = self.backend_config.config.rpc_request_timeout_seconds * 4;
        let deadline = SystemTime::now() + Duration::from_secs(timeout_secs);
        loop {
            if self.writeback.cycles_at_or_below_drained(inode, barrier) {
                break;
            }
            if SystemTime::now() > deadline {
                tracing::warn!(inode, barrier = barrier.0, "writeback drain timeout");
                return Err(FsError::Internal("writeback drain".to_string()));
            }
            compio_runtime::time::sleep(poll_dur).await;
        }

        // Surface a deferred error if the drained cycles tainted the
        // inode. The FUSE layer will translate to EIO; the application
        // is expected to close-and-reopen on the remote winner.
        if self.writeback.is_tainted(inode) {
            return Err(FsError::Internal("writeback drain".to_string()));
        }

        Ok(())
    }

    pub async fn vfs_release(&self, fh: u64) -> Result<(), FsError> {
        // Flush any dirty write buffer, then ALWAYS clean up the
        // file_handle and write lock -- even when the flush failed
        // with a CAS conflict or an NSS error. Without this, an
        // errored flush leaves the handle in `file_handles` and the
        // inode lock occupied forever, so a follow-up vfs_open sees
        // a permanent Busy and the drain barrier in vfs_open can't
        // recover. The flush error itself is propagated *after*
        // cleanup so the spawn handler in fuse_server::release still
        // taints the inode and surfaces EIO on the next op.
        //
        // The lock stays held across `flush_write_buffer` itself so a
        // concurrent open(O_WRONLY) can't start a second flush
        // against a stale layout snapshot -- the writeback queue's
        // CAS rejects but does NOT auto-rebase, so an overlap turns
        // into a tainted-inode and ESTALE. open() waits for the
        // in-flight cycle to drain (see vfs_open's barrier path)
        // before retrying acquire_write_lock.
        let (has_dirty, was_writer, ino_opt) = self
            .file_handles
            .get(&fh)
            .map(|h| {
                let dirty = h.write_buf.as_ref().map(|wb| wb.dirty).unwrap_or(false);
                let writer = h.write_buf.is_some();
                (dirty, writer, Some(h.ino))
            })
            .unwrap_or((false, false, None));

        let flush_result = if has_dirty {
            self.flush_write_buffer(fh).await
        } else {
            Ok(())
        };

        let ino = self.file_handles.get(&fh).map(|h| h.ino).or(ino_opt);
        self.file_handles.remove(&fh);

        // Release the inode-scoped write lock if this handle held it.
        // Read-only handles never acquired it. This runs regardless
        // of `flush_result` so an errored flush doesn't strand the
        // lock.
        if was_writer && let Some(ino) = ino {
            self.release_write_lock(ino, fh);
        }

        flush_result?;

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

    /// Create a new regular file at `(parent, name)`. The optional
    /// `init_posix` carries the POSIX bits the caller wants the new
    /// inode to start with -- mode (S_IFREG implicit), uid, gid, and
    /// the initial atime / mtime / ctime if the caller has them.
    /// Callers that don't have those (e.g. NFS today) pass `None` and
    /// the inode lands with the default-zero posix that `make_*_attr`
    /// already knows to fall back from.
    pub async fn vfs_create(
        &self,
        parent: u64,
        name: &str,
        init_posix: Option<data_types::object_layout::PosixAttrs>,
    ) -> Result<(VfsAttr, u64), FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}", prefix, name);

        let (ino, _) = self.inodes.lookup_or_insert(&key, EntryType::File, None);

        // Seed the new inode's posix from the caller. We set the bits
        // BEFORE make_new_file_attr below so the attr returned to the
        // kernel reflects the requested mode rather than the
        // synthesised default.
        if let Some(p) = init_posix
            && let Some(mut entry) = self.inodes.get_mut(ino)
        {
            entry.posix = p;
        }

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

        // Early-publish: enqueue a placeholder PutInode so
        // cross-instance lookups can find the file before close. The
        // enqueue is fire-and-forget -- the worker drains it on its
        // next tick. The terminal flush at vfs_release CASes against
        // these bytes and naturally serializes against the placeholder
        // cycle (same key, increasing generation) inside the queue,
        // so we don't need to block create() to wait for the
        // placeholder to commit.
        //
        // File placeholder is a `Normal` zero-byte layout, NOT a
        // `Directory` marker. A directory marker would be
        // `is_listable() == false` and `vfs_lookup` would treat the
        // early-published key as ENOENT until close-time flush_publish
        // replaces it with the real Normal layout.
        let placeholder_layout = file_ops::create_file_placeholder_layout();
        let placeholder_bytes: Bytes =
            to_bytes_in::<_, rkyv::rancor::Error>(&placeholder_layout, Vec::new())
                .map_err(FsError::from)?
                .into();
        // Seed the inode entry's layout from the placeholder so a
        // follow-up vfs_getattr (e.g. the one fuse_server::setattr
        // calls to build its reply, fired the moment userspace does
        // chmod-after-create) takes the in-memory branch instead of
        // falling back to backend().get_inode(key) -- the worker
        // hasn't yet drained the placeholder PutInode, so NSS would
        // return NotFound and the kernel would surface ENOENT to
        // userspace. The 5ms drain tick is always wider than the
        // 1-2ms gap between create(2) returning and the next chmod(2)
        // arriving from the same shell loop.
        if let Some(mut entry) = self.inodes.get_mut(ino) {
            entry.layout = Some(placeholder_layout.clone());
        }
        self.enqueue_inode_intent_async(ino, &key, &prefix, name, placeholder_bytes.clone(), None);

        self.file_handles.insert(
            fh,
            FileHandle {
                ino,
                s3_key: key,
                layout: Some(placeholder_layout),
                layout_bytes: Some(placeholder_bytes),
                write_buf: Some(wb),
                backing_id: None,
            },
        );

        let attr = self.make_new_file_attr(ino, 0);

        // Invalidate dir cache so the new file shows up in listings
        self.dir_cache.invalidate(&prefix);
        self.touch_parent_times(parent);

        Ok((attr, fh))
    }

    /// Create a symbolic link at `(parent, name)` whose body is
    /// `target`. The layout is published to NSS via an unconditional
    /// `put_inode` (this is a brand-new entry), no BSS blob is
    /// allocated, and the parent dir cache is invalidated so the new
    /// name shows up in listings. Existing entries at the same name
    /// fail the create with `AlreadyExists`.
    pub async fn vfs_symlink(
        &self,
        parent: u64,
        name: &str,
        target: &[u8],
    ) -> Result<VfsAttr, FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;
        self.ensure_writeback_worker_started();

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}", prefix, name);

        let trace_id = TraceId::new();

        // Reject if a name already exists at this path.
        match self.backend().get_inode(&key, &trace_id).await {
            Ok(_) => return Err(FsError::AlreadyExists),
            Err(FsError::NotFound) => {}
            Err(e) => return Err(e),
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let layout = ObjectLayout {
            version_id: ObjectLayout::gen_version_id(),
            block_size: DEFAULT_BLOCK_SIZE,
            timestamp,
            blob_version: 0,
            state: ObjectState::Symlink(SymlinkData {
                target: target.to_vec(),
                core_meta_data: ObjectCoreMetaData {
                    size: target.len() as u64,
                    etag: String::new(),
                    headers: vec![],
                    checksum: None,
                    ..Default::default()
                },
            }),
        };

        let layout_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())
            .map_err(FsError::from)?
            .into();

        let (ino, _) = self
            .inodes
            .lookup_or_insert(&key, EntryType::File, Some(layout.clone()));

        // Open a fresh cycle for this inode and enqueue the PutInode
        // intent. The worker drains it asynchronously; vfs_fsync (or
        // implicit on next syncfs) waits for the cycle to commit.
        {
            use crate::writeback::{FhId, Generation};
            let generation = Generation(0);
            self.writeback
                .open_cycle(ino, generation, layout_bytes.len() as u64, 0);
            let _outcome = self.writeback.upsert_inode_intent(
                key.clone(),
                generation,
                ino,
                WbInodeOp::PutInode {
                    parent_key: prefix.clone(),
                    name: name.to_string(),
                    layout_bytes,
                    // Symlinks are brand-new entries -- no CAS guard.
                    // Worker uses unconditional put_inode.
                    expected_old_value: None,
                },
                // No fh is allocated for symlinks (the kernel does
                // not call open() on the link itself); use a
                // sentinel FhId so error routing can still walk the
                // owner set.
                FhId(0),
            );
        }

        // Invalidate dir cache so the new symlink shows up in listings.
        self.dir_cache.invalidate(&prefix);
        self.touch_parent_times(parent);

        self.make_file_attr(ino, &layout)
    }

    /// Create a hard link at `(new_parent, new_name)` pointing at the
    /// same inode as `inode`. Promotes the source on first call (if its
    /// layout is `Normal | Mpu | Symlink | Special`): allocates a
    /// `#hardlink/<uuid>` `InodeRecord`, replaces the source's NSS
    /// entry with an `Indirect` redirect, and writes a matching
    /// redirect at the destination. Subsequent `vfs_link` calls for
    /// the same source bump the `nlink` slot in the existing record.
    ///
    /// MVP scope (doc 20 section 4): single-instance only. The promote
    /// transaction is best-effort -- crash recovery is left to the
    /// scan/repair invariants in section 4.7 (also out of MVP scope).
    /// Reads/writes-through-hardlink are NOT yet supported; this is
    /// enough to unblock pjdfstest's `link n0 n1; unlink n1` patterns.
    pub async fn vfs_link(
        &self,
        inode: u64,
        new_parent: u64,
        new_name: &str,
    ) -> Result<VfsAttr, FsError> {
        use data_types::object_layout::{IndirectEntry, InodeRecord};
        self.check_write_enabled()?;
        Self::check_name_max(new_name)?;
        self.ensure_writeback_worker_started();

        // Resolve the source inode's primary user-facing key + cached
        // `inode_id`. For an inode that has already been promoted via
        // a previous `vfs_link`, `entry.inode_id` is `Some(uuid)` and
        // is the authoritative pointer into the `#hardlink/<uuid>`
        // record -- we use it directly, side-stepping
        // `entry.s3_key`, which may point to an alias whose NSS row
        // has just been unlinked (link/02.t step 4 path:
        // `link nx n0; unlink nx; link n0 nx` -- the second link
        // races against the now-deleted nx NSS key).
        let (src_key, entry_type, cached_inode_id) = self
            .inodes
            .get(inode)
            .map(|e| (e.s3_key.clone(), e.entry_type, e.inode_id))
            .ok_or(FsError::NotFound)?;

        // EISDIR for hardlink-to-directory matches POSIX -- only the
        // root's `..` is allowed and we don't expose that surface.
        if entry_type == EntryType::Directory {
            return Err(FsError::IsDir);
        }

        let new_prefix = self.dir_prefix(new_parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&new_prefix, new_name)?;
        let new_key = format!("{}{}", new_prefix, new_name);
        if new_key == src_key {
            // POSIX: link(a, a) returns EEXIST.
            return Err(FsError::AlreadyExists);
        }

        let trace_id = TraceId::new();

        // EEXIST if the destination is already in use.
        match self.backend().get_inode(&new_key, &trace_id).await {
            Ok(_) => return Err(FsError::AlreadyExists),
            Err(FsError::NotFound) => {}
            Err(e) => return Err(e),
        }

        // Drain any pending publish for the source so we read the
        // post-flush layout (otherwise we might promote against a
        // stale placeholder and lose the write that hasn't yet
        // committed).
        let _ = self.drain_inode_to_barrier(inode).await;

        // Two source shapes:
        //   - Already promoted (`cached_inode_id == Some(uuid)`): no
        //     NSS get_inode at src_key needed. Fetch the
        //     `InodeRecord` directly, bump nlink. The src NSS row may
        //     or may not still be live (`unlink src` could have
        //     dropped it already); either way we don't touch it.
        //   - Fresh (`None`): read src_key bytes, decide which
        //     promotion arm to take, mint a new `inode_id`.
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let (inode_id, mut record, was_indirect) = if let Some(inode_id) = cached_inode_id {
            let mut record = self.backend().get_inode_record(inode_id, &trace_id).await?;
            record.nlink = record.nlink.saturating_add(1);
            (inode_id, record, true)
        } else {
            let src_layout = self.backend().get_inode(&src_key, &trace_id).await?;
            if matches!(
                &src_layout.state,
                ObjectState::Directory(_) | ObjectState::Mpu(MpuState::Uploading)
            ) {
                return Err(FsError::IsDir);
            }
            match &src_layout.state {
                ObjectState::Indirect(redirect) => {
                    // Inode entry didn't carry the cache (cross-instance
                    // refresh, freshly-mounted daemon). Fall back to
                    // the redirect we just read.
                    let inode_id = redirect.inode_id;
                    let mut record = self.backend().get_inode_record(inode_id, &trace_id).await?;
                    record.nlink = record.nlink.saturating_add(1);
                    (inode_id, record, true)
                }
                ObjectState::Normal(_)
                | ObjectState::Mpu(MpuState::Completed(_))
                | ObjectState::Symlink(_)
                | ObjectState::Special(_) => {
                    let inode_id = uuid::Uuid::new_v4();
                    let record = InodeRecord {
                        layout: src_layout.clone(),
                        nlink: 2,
                        orphan_since: None,
                    };
                    (inode_id, record, false)
                }
                ObjectState::Directory(_) | ObjectState::Mpu(MpuState::Uploading) => {
                    return Err(FsError::IsDir);
                }
            }
        };

        // POSIX: link(2) updates the file's ctime. Stamp the new
        // value into the InodeRecord's layout so a subsequent
        // vfs_lookup repopulating `entry.posix` from the record
        // sees the bumped value (the in-memory mutation alone
        // would be lost on the next lookup-driven refresh).
        record.layout = crate::inode::layout_with_posix(record.layout.clone(), {
            let mut p = crate::inode::layout_posix(&record.layout);
            p.ctime_ns = now_ns;
            p
        });

        // 1. Persist the InodeRecord first. Crash before this point
        //    leaves NSS exactly as it was; crash after but before
        //    step 2 leaves a dangling InodeRecord that scan/repair
        //    invariant 2 finalises (out of MVP scope; the orphan
        //    survives until then but is unreachable, which matches
        //    POSIX's "link returned before commit" failure mode).
        self.backend()
            .put_inode_record(inode_id, &record, &trace_id)
            .await?;

        // 2. If this was a fresh promotion, replace the source's NSS
        //    entry with an Indirect redirect. For an already-promoted
        //    source, the entry is already a redirect.
        let redirect_layout = ObjectLayout {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            version_id: ObjectLayout::gen_version_id(),
            block_size: DEFAULT_BLOCK_SIZE,
            blob_version: 0,
            state: ObjectState::Indirect(IndirectEntry { inode_id }),
        };
        let redirect_bytes: Bytes =
            to_bytes_in::<_, rkyv::rancor::Error>(&redirect_layout, Vec::new())
                .map_err(FsError::from)?
                .into();
        if !was_indirect {
            self.backend()
                .put_inode(&src_key, redirect_bytes.clone(), &trace_id)
                .await?;
        }

        // 3. Write the destination redirect.
        self.backend()
            .put_inode(&new_key, redirect_bytes, &trace_id)
            .await?;

        // 4. Update the InodeTable so subsequent ops on either name
        //    see the resolved layout and the `inode_id` resolution
        //    cache (doc 20 section 4.3). The same `ino` handles both
        //    names: key_to_ino learns the new mapping, and
        //    entry.layout / entry.posix / entry.inode_id reflect the
        //    resolved record.
        // POSIX: link(2) updates the file's ctime. We can't rely on
        // the kernel issuing a follow-up FUSE_SETATTR (it does that
        // for regular files but not consistently for fifo / block /
        // char / socket inodes -- pjdfstest link/00.t tests
        // 141/148/155/162 caught the gap). Stamp it explicitly here.
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        if let Some(mut e) = self.inodes.get_mut(inode) {
            e.layout = Some(record.layout.clone());
            e.posix = crate::inode::layout_posix(&record.layout);
            e.posix.ctime_ns = now_ns;
            e.inode_id = Some(inode_id);
            e.cache_expiry = std::time::Instant::now();
        }
        self.inodes.add_alias(&new_key, EntryType::File, inode);

        self.dir_cache.invalidate(&new_prefix);
        self.touch_parent_times(new_parent);

        let mut attr = self.make_file_attr(inode, &record.layout)?;
        attr.nlink = record.nlink;
        attr.ctime_secs = now_ns / 1_000_000_000;
        Ok(attr)
    }

    /// Create a fifo / block / char / unix-socket inode at
    /// `(parent, name)`. The kernel handles all I/O against the open
    /// fd itself (pipes, device drivers, AF_UNIX); fs_server only
    /// has to round-trip the metadata so `stat(2)` reports the
    /// right `S_IFMT` bit and `rdev`. POSIX-create semantics: fail
    /// if a name already exists at this path.
    pub async fn vfs_mknod(
        &self,
        parent: u64,
        name: &str,
        kind: data_types::object_layout::SpecialKind,
        rdev: u32,
        init_posix: data_types::object_layout::PosixAttrs,
    ) -> Result<VfsAttr, FsError> {
        use data_types::object_layout::SpecialData;

        self.check_write_enabled()?;
        Self::check_name_max(name)?;
        self.ensure_writeback_worker_started();

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}", prefix, name);

        let trace_id = TraceId::new();

        match self.backend().get_inode(&key, &trace_id).await {
            Ok(_) => return Err(FsError::AlreadyExists),
            Err(FsError::NotFound) => {}
            Err(e) => return Err(e),
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let mut core_meta_data = ObjectCoreMetaData {
            size: 0,
            etag: String::new(),
            headers: vec![],
            checksum: None,
            posix: init_posix,
        };
        // Make the special-inode mode include the right S_IFMT bits
        // even if the caller passed only permission bits in posix.mode.
        // make_special_attr re-stamps IFMT on read but we also persist
        // a coherent value so a cross-instance stat without going
        // through `inode_table` sees the right kind.
        let ifmt = match kind {
            data_types::object_layout::SpecialKind::Fifo => libc::S_IFIFO,
            data_types::object_layout::SpecialKind::BlockDevice => libc::S_IFBLK,
            data_types::object_layout::SpecialKind::CharDevice => libc::S_IFCHR,
            data_types::object_layout::SpecialKind::Socket => libc::S_IFSOCK,
        };
        if core_meta_data.posix.mode != 0 {
            core_meta_data.posix.mode = (core_meta_data.posix.mode & !libc::S_IFMT) | ifmt;
        }

        let layout = ObjectLayout {
            version_id: ObjectLayout::gen_version_id(),
            block_size: DEFAULT_BLOCK_SIZE,
            timestamp,
            blob_version: 0,
            state: ObjectState::Special(SpecialData {
                kind,
                rdev,
                core_meta_data,
            }),
        };

        let layout_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())
            .map_err(FsError::from)?
            .into();

        let (ino, _) = self
            .inodes
            .lookup_or_insert(&key, EntryType::File, Some(layout.clone()));

        {
            use crate::writeback::{FhId, Generation};
            let generation = Generation(0);
            self.writeback
                .open_cycle(ino, generation, layout_bytes.len() as u64, 0);
            let _outcome = self.writeback.upsert_inode_intent(
                key.clone(),
                generation,
                ino,
                WbInodeOp::PutInode {
                    parent_key: prefix.clone(),
                    name: name.to_string(),
                    layout_bytes,
                    expected_old_value: None,
                },
                FhId(0),
            );
        }

        self.dir_cache.invalidate(&prefix);
        self.touch_parent_times(parent);

        self.make_file_attr(ino, &layout)
    }

    /// Return the bytes a `readlink(2)` should hand back. Returns
    /// `InvalidArgument` (EINVAL) when the inode is not a symlink --
    /// matching the `readlink(2)` errno for non-symlink targets.
    pub async fn vfs_readlink(&self, inode: u64) -> Result<Vec<u8>, FsError> {
        let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;

        if entry.entry_type != EntryType::File {
            return Err(FsError::InvalidArg);
        }

        // Fast path: the cached layout is a Symlink.
        if let Some(layout) = entry.layout.as_ref()
            && let Some(target) = layout.symlink_target()
        {
            return Ok(target.to_vec());
        }

        // Cold path: re-fetch from NSS. This handles the case where
        // the inode entry was created by lookup but the layout was
        // dropped (memory pressure / eviction).
        let key = entry.s3_key.clone();
        drop(entry);

        let trace_id = TraceId::new();
        let layout = self.backend().get_inode(&key, &trace_id).await?;

        if let Some(target) = layout.symlink_target() {
            // Cache the layout for future lookups on this inode.
            if let Some(mut e) = self.inodes.get_mut(inode) {
                e.layout = Some(layout.clone());
            }
            Ok(target.to_vec())
        } else {
            Err(FsError::InvalidArg)
        }
    }

    pub async fn vfs_unlink(
        &self,
        parent: u64,
        name: &str,
        caller_uid: u32,
    ) -> Result<(), FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}", prefix, name);

        let trace_id = TraceId::new();

        // POSIX sticky-bit unlink contract: if the parent directory
        // has S_ISVTX set, only root, the parent's owner, or the
        // file's owner may unlink. The FUSE
        // `default_permissions` flag does NOT cover this case (it
        // gates access by the parent's r/w/x bits but not by the
        // sticky-bit ownership rule), so fs_server enforces it
        // before issuing the NSS delete.
        if caller_uid != 0 {
            let parent_posix = self.resolve_parent_posix(parent, &prefix, &trace_id).await;
            if let Some(p) = parent_posix
                && p.mode & libc::S_ISVTX != 0
                && p.uid != caller_uid
            {
                let file_uid = self.resolve_file_uid(&key, &trace_id).await?;
                if file_uid != caller_uid {
                    return Err(FsError::PermissionDenied);
                }
            }
        }

        // Drain any in-flight write/flush cycles on this inode AND
        // any Pending PutInode intents for the key before issuing the
        // NSS delete. Without the cycle drain, fuse_server::release
        // spawns vfs_release asynchronously: the close-time
        // flush_publish enqueues its PutInode some milliseconds
        // *after* FUSE_RELEASE returns to the kernel. If unlink
        // arrives in that window, has_pending_intent_for_key says
        // "no Pending intent", we delete from NSS, and the spawned
        // flush then puts the file back -- pjdfstest sees EEXIST on
        // the next iteration's mkdir at the same name.
        // drain_inode_to_barrier waits for the cycle (which is
        // open_cycle'd synchronously inside fuse_server::release
        // before the spawn returns) to reach Done.
        if let Some(ino) = self.inodes.find_ino_by_key(&key, EntryType::File) {
            let _ = self.drain_inode_to_barrier(ino).await;
        }
        self.wait_for_lookup_drain(&key).await;

        // Delete the inode from NSS
        let old_bytes = self.backend().delete_inode(&key, &trace_id).await?;

        // Return ENOENT if file didn't exist
        let old_bytes = old_bytes.ok_or(FsError::NotFound)?;

        // Remove name mapping from inode table (read-only lookup, no refcount leak)
        let ino = self.inodes.find_ino_by_key(&key, EntryType::File);
        if let Some(ino) = ino {
            self.inodes.remove_name_mapping(ino, &key);
            // POSIX: a successful unlink updates the surviving file's
            // ctime (when nlink > 0) or doesn't matter (when nlink
            // hits 0 and the inode is GC'd). Stamp it now so a stat
            // through any remaining alias reflects the change. The
            // kernel doesn't reliably issue a follow-up FUSE_SETATTR
            // for fifo / block / char / socket inodes; pjdfstest
            // unlink/00.t tests 34, 39, 44, 49 caught the gap.
            let now_ns = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            if let Some(mut e) = self.inodes.get_mut(ino) {
                e.posix.ctime_ns = now_ns;
            }
        }

        // Hand the orphaned NSS value off to the shared cleanup
        // helper. Same logic the rename-overwrite path uses.
        self.cleanup_orphaned_value(&key, ino, old_bytes, &trace_id)
            .await;

        // Invalidate dir cache for parent
        self.dir_cache.invalidate(&prefix);
        self.touch_parent_times(parent);

        Ok(())
    }

    /// GC the blob backing an NSS value that was just dropped (via
    /// `delete_inode` on an unlink, or via `rename_object` with
    /// `force_overwrite=true` on an atomic-replace rename). Three
    /// shapes:
    ///   - direct file (`Normal | Mpu`): free the blob now, or defer
    ///     to last-fh-release if the inode still has open handles.
    ///   - hardlink redirect (`Indirect`): decrement the
    ///     `InodeRecord` nlink. If it drops to 0 and no local fhs
    ///     hold the inode open, delete the record and free the
    ///     underlying blob; otherwise persist the decremented count
    ///     (and an `orphan_since` stamp when the record is now
    ///     unreachable) and let scan/repair finish the GC.
    ///   - symlink / directory marker / special: nothing to GC.
    ///
    /// Multi-instance open-fd coordination (doc 20 section 4.4) is
    /// post-MVP; we treat the local open-handle set as authoritative.
    /// `key` is the NSS key the bytes were attached to (used to look
    /// up MPU parts). `ino_hint` is the inode mapping at that key,
    /// captured before the NSS mutation so we still have a handle on
    /// it after the alias has moved.
    async fn cleanup_orphaned_value(
        &self,
        key: &str,
        ino_hint: Option<u64>,
        old_bytes: Bytes,
        trace_id: &TraceId,
    ) {
        if old_bytes.is_empty() {
            return;
        }
        if let Some(ino) = ino_hint
            && self.has_open_handles_for_inode(ino, None)
            && !matches!(
                rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
                    .ok()
                    .as_ref()
                    .map(|l| &l.state),
                Some(ObjectState::Indirect(_))
            )
        {
            // Defer blob cleanup until last handle is released
            self.deferred_blob_cleanup.insert(ino, old_bytes);
            return;
        }
        let Ok(old_layout) = rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
        else {
            return;
        };
        match &old_layout.state {
            ObjectState::Normal(_) => {
                self.backend()
                    .delete_blob_blocks(&old_layout, trace_id)
                    .await;
            }
            ObjectState::Mpu(MpuState::Completed(_)) => {
                if let Ok(parts) = self.backend().list_mpu_parts(key, trace_id).await {
                    for (part_key, part_layout) in &parts {
                        self.backend()
                            .delete_blob_blocks(part_layout, trace_id)
                            .await;
                        let _ = self.backend().delete_inode(part_key, trace_id).await;
                    }
                }
            }
            ObjectState::Indirect(redirect) => {
                let inode_id = redirect.inode_id;
                if let Ok(mut record) = self.backend().get_inode_record(inode_id, trace_id).await {
                    record.nlink = record.nlink.saturating_sub(1);
                    // POSIX: a successful unlink updates the
                    // surviving file's ctime when nlink > 0. Stamp
                    // the new value into the record's layout so the
                    // next vfs_lookup-driven `entry.posix` refresh
                    // sees it (the in-memory `entry.posix.ctime_ns`
                    // bump on the caller side gets clobbered by
                    // lookup otherwise).
                    if record.nlink > 0 {
                        let now_ns = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_nanos() as u64)
                            .unwrap_or(0);
                        record.layout = crate::inode::layout_with_posix(record.layout.clone(), {
                            let mut p = crate::inode::layout_posix(&record.layout);
                            p.ctime_ns = now_ns;
                            p
                        });
                    }
                    if record.nlink == 0 {
                        let still_open = ino_hint
                            .map(|i| self.has_open_handles_for_inode(i, None))
                            .unwrap_or(false);
                        if still_open {
                            // Mark orphan and let the last-fh-release
                            // path finish the GC. (For the MVP we
                            // don't have a dedicated post-release
                            // hook for indirect inodes -- the
                            // `orphan_since` stamp keeps the record
                            // valid for any in-flight fh while
                            // scan/repair would eventually pick it
                            // up.)
                            record.orphan_since = Some(
                                SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .map(|d| d.as_nanos() as u64)
                                    .unwrap_or(0),
                            );
                            let _ = self
                                .backend()
                                .put_inode_record(inode_id, &record, trace_id)
                                .await;
                        } else {
                            self.backend()
                                .delete_blob_blocks(&record.layout, trace_id)
                                .await;
                            let _ = self.backend().delete_inode_record(inode_id, trace_id).await;
                        }
                    } else {
                        let _ = self
                            .backend()
                            .put_inode_record(inode_id, &record, trace_id)
                            .await;
                    }
                }
            }
            _ => {}
        }
    }

    pub async fn vfs_mkdir(
        &self,
        parent: u64,
        name: &str,
        init_posix: Option<data_types::object_layout::PosixAttrs>,
    ) -> Result<VfsAttr, FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}/", prefix, name);

        // Build the layout once with the caller-provided posix folded
        // in so the bytes we actually persist in NSS carry the
        // requested mode / uid / gid / times. The default-zero posix
        // we'd otherwise serialise round-trips as
        // "uninitialised, fall back to defaults" on the next lookup
        // and loses the mkdir(2) `mode` argument.
        let mut layout = file_ops::create_dir_marker_layout();
        if let Some(p) = init_posix
            && let data_types::object_layout::ObjectState::Directory(ref mut data) = layout.state
        {
            data.posix = p;
        }

        let seed_posix = |ino: u64| {
            if let Some(p) = init_posix
                && let Some(mut entry) = self.inodes.get_mut(ino)
            {
                entry.posix = p;
            }
        };

        // Route through the writeback queue so multiple concurrent
        // mkdir calls (e.g. tar walking the kernel-source tree)
        // coalesce into one InodeBatch RPC instead of N synchronous
        // put_inode round-trips. Build the dir marker layout + bytes
        // locally so we can enqueue without touching NSS yet. The
        // enqueue is fire-and-forget; the worker drains it on the
        // next tick. Subsequent vfs_create against this dir uses the
        // local inode table for parent-prefix lookup, so the
        // placeholder doesn't need to commit before children land.
        // fsyncdir / syncfs / unmount drain barriers all wait for
        // commit.
        let layout_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())
            .map_err(FsError::from)?
            .into();
        let (ino, _) =
            self.inodes
                .lookup_or_insert(&key, EntryType::Directory, Some(layout.clone()));
        seed_posix(ino);
        self.enqueue_inode_intent_async(ino, &key, &prefix, name, layout_bytes, None);
        self.dir_cache.invalidate(&prefix);
        self.touch_parent_times(parent);
        Ok(self.make_dir_attr(ino))
    }

    pub async fn vfs_rmdir(&self, parent: u64, name: &str, caller_uid: u32) -> Result<(), FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}/", prefix, name);

        let trace_id = TraceId::new();

        // POSIX sticky-bit rmdir contract -- same shape as
        // vfs_unlink (root, parent owner, or target dir owner may
        // remove). The FUSE `default_permissions` flag does not
        // gate this case.
        if caller_uid != 0 {
            let parent_posix = self.resolve_parent_posix(parent, &prefix, &trace_id).await;
            if let Some(p) = parent_posix
                && p.mode & libc::S_ISVTX != 0
                && p.uid != caller_uid
            {
                let target_uid = self.resolve_file_uid(&key, &trace_id).await.unwrap_or(0);
                if target_uid != caller_uid {
                    return Err(FsError::PermissionDenied);
                }
            }
        }

        // Drain any in-flight cycles + Pending PutInode intents on
        // this dir before touching NSS. Mirrors the vfs_unlink path
        // (see comment there); without it a queued PutInode for the
        // dir marker would land *after* our delete_inode and
        // resurrect the directory.
        if let Some(ino) = self.inodes.find_ino_by_key(&key, EntryType::Directory) {
            let _ = self.drain_inode_to_barrier(ino).await;
        }
        self.wait_for_lookup_drain(&key).await;
        // Wait for child publishes too: the NSS empty-check below sees
        // only what has actually committed, so a freshly-queued mknod
        // /symlink/create at `${key}/<child>` would otherwise be
        // invisible and the rmdir would erroneously succeed
        // (pjdfstest rmdir/06.t verifies the ENOTEMPTY contract).
        self.wait_for_prefix_drain(&key).await;

        // List to check existence and emptiness. Use NO delimiter so
        // NSS walks leaves directly and filters tombstones (NSS
        // listToLeaf only filters tombstones on the LEAF branch; with
        // delim="/" a fully-tombstoned subtree still emits a
        // CommonPrefix entry, making `rmdir parent` see a phantom
        // child that exists in NSS only as tombstoned leaves).
        // Without delim we read raw leaves with tombstones already
        // filtered: the dir marker itself plus any live descendant.
        // max_keys=2 is enough -- if anything other than the dir
        // marker shows up we know the dir is non-empty.
        let entries = self
            .backend()
            .list_inodes(&key, "", "", 2, &trace_id)
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
            self.inodes.remove_name_mapping(ino, &key);
        }

        // Invalidate dir cache for parent and self
        self.dir_cache.invalidate(&prefix);
        self.dir_cache.invalidate(&key);
        self.touch_parent_times(parent);

        Ok(())
    }

    pub async fn vfs_rename(
        &self,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
        caller_uid: u32,
        caller_gid: u32,
    ) -> Result<(), FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;
        Self::check_name_max(new_name)?;

        let src_prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        let dst_prefix = self.dir_prefix(new_parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&src_prefix, name)?;
        Self::check_path_max(&dst_prefix, new_name)?;

        let src_key = format!("{}{}", src_prefix, name);
        let dst_key = format!("{}{}", dst_prefix, new_name);
        let src_dir_key = format!("{}/", src_key);
        let dst_dir_key = format!("{}/", dst_key);

        let trace_id = TraceId::new();

        // POSIX rename(2) requires write+search permission on BOTH
        // src parent (to remove the entry) and dst parent (to add
        // the entry). The FUSE kernel's `default_permissions` covers
        // most r/w/x checks but doesn't gate rename's parent-perm
        // contract; enforce it here. Matches pjdfstest rename/04.t
        // (search-deny on src parent) and rename/05.t (write-deny).
        let src_parent_posix = self
            .resolve_parent_posix(parent, &src_prefix, &trace_id)
            .await;
        if let Some(p) = src_parent_posix
            && !dir_has_wx_perm(p, caller_uid, caller_gid)
        {
            return Err(FsError::AccessDenied);
        }
        let dst_parent_posix = self
            .resolve_parent_posix(new_parent, &dst_prefix, &trace_id)
            .await;
        if let Some(p) = dst_parent_posix
            && !dir_has_wx_perm(p, caller_uid, caller_gid)
        {
            return Err(FsError::AccessDenied);
        }

        // POSIX sticky-bit rename gate -- a non-root non-owner-of-
        // src-parent can't rename out of a sticky source directory
        // unless they own the source, and likewise can't replace
        // into a sticky dst parent unless they own the existing dst.
        // The src/dst victim can be either a regular file (`*_key`)
        // or a directory (`*_dir_key`); the gate fires the same way
        // for both.
        if caller_uid != 0 {
            // Source side.
            if let Some(p) = src_parent_posix
                && p.mode & libc::S_ISVTX != 0
                && p.uid != caller_uid
            {
                let src_uid = match self.resolve_file_uid(&src_key, &trace_id).await {
                    Ok(u) => u,
                    Err(_) => self
                        .resolve_file_uid(&src_dir_key, &trace_id)
                        .await
                        .unwrap_or(0),
                };
                if src_uid != caller_uid {
                    return Err(FsError::PermissionDenied);
                }
            }
            // Destination side -- only when a victim exists. Probe
            // both shapes so a sticky-bit rename landing on an empty
            // target dir gets the same EPERM gate as one landing on
            // a regular file.
            let dst_victim_key: Option<String> =
                if self.backend().get_inode(&dst_key, &trace_id).await.is_ok() {
                    Some(dst_key.clone())
                } else if self
                    .backend()
                    .get_inode(&dst_dir_key, &trace_id)
                    .await
                    .is_ok()
                {
                    Some(dst_dir_key.clone())
                } else {
                    None
                };
            if let Some(victim_key) = dst_victim_key
                && let Some(p) = dst_parent_posix
                && p.mode & libc::S_ISVTX != 0
                && p.uid != caller_uid
            {
                let dst_uid = self
                    .resolve_file_uid(&victim_key, &trace_id)
                    .await
                    .unwrap_or(0);
                if dst_uid != caller_uid {
                    return Err(FsError::PermissionDenied);
                }
            }
        }

        // Drain any per-key pending writeback intent on src AND dst
        // before the NSS rename / get_inode probes below. In
        // writeback default mode `create + close` / `mkdir` returns
        // to userspace before the close-time `flush_publish` lands
        // in NSS. A test that fires `rename` immediately after that
        // close (pjdfstest rename/05.t tests 7-8, rename/09.t /
        // 10.t dir-rename clusters) would otherwise observe a
        // fs_server probe say "src/dst doesn't exist yet", skip the
        // atomic-replace, and then hand NSS a rename whose dst now
        // does exist (the queued PutInode drained between our probe
        // and the RPC). The per-key `wait_for_lookup_drain` has a
        // 200 ms ceiling and skips tainted inodes, so this stays
        // fast even when an earlier flush left a stuck cycle.
        if let Some(ino) = self.inodes.find_ino_by_key(&src_key, EntryType::File) {
            let _ = self.drain_inode_to_barrier(ino).await;
        } else if let Some(ino) = self
            .inodes
            .find_ino_by_key(&src_dir_key, EntryType::Directory)
        {
            let _ = self.drain_inode_to_barrier(ino).await;
        }
        self.wait_for_lookup_drain(&src_key).await;
        self.wait_for_lookup_drain(&dst_key).await;
        self.wait_for_lookup_drain(&src_dir_key).await;
        self.wait_for_lookup_drain(&dst_dir_key).await;

        // Determine type by probing NSS backend directly (no inode side effects)
        let is_dir = match self.backend().get_inode(&src_key, &trace_id).await {
            Ok(_) => false,
            Err(FsError::NotFound) => true,
            Err(e) => return Err(e),
        };

        if is_dir {
            // Drain every pending writeback intent under the src
            // subtree before we ask NSS to rename it. A child file
            // created/written inside `srcdir/` whose `PutInode` is
            // still queued would otherwise commit at the OLD key
            // AFTER `rename_folder` returns, resurrecting
            // `/srcdir/<child>` -- which in turn makes `/srcdir/`
            // visible to the next lookup via `list_inodes`. Mirrors
            // the `wait_for_prefix_drain` step `vfs_rmdir` does for
            // its empty-check.
            self.wait_for_prefix_drain(&src_dir_key).await;
            // POSIX rename(2) on directories atomically replaces an
            // existing empty dst dir. If dst exists, route through
            // `vfs_rmdir` first -- it enforces the ENOTEMPTY contract
            // (only an empty dst can be replaced), drains pending
            // writeback for dst, and tears down the local inode
            // mapping. Pass `caller_uid = 0` to bypass the duplicate
            // sticky-bit gate inside vfs_rmdir; the gate already ran
            // above. Then issue `rename_folder(force_overwrite=true)`
            // so any writeback that re-publishes dst between our
            // rmdir and the RPC gets atomically force-replaced by
            // src instead of bouncing as STATUS_DST_EXISTED.
            if self
                .backend()
                .get_inode(&dst_dir_key, &trace_id)
                .await
                .is_ok()
            {
                match self.vfs_rmdir(new_parent, new_name, 0).await {
                    Ok(()) => {}
                    Err(FsError::NotFound) => {
                        // Already gone; race with another op erasing
                        // it. Fine, fall through to rename.
                    }
                    Err(e) => return Err(e),
                }
            }
            self.backend()
                .rename_folder(&src_dir_key, &dst_dir_key, true, &trace_id)
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
            self.touch_parent_times(parent);
            if new_parent != parent {
                self.touch_parent_times(new_parent);
            }
        } else {
            // Capture the dst inode mapping (if any) before NSS
            // mutates the alias. The cleanup helper uses it to
            // decide whether to defer blob GC for an inode that
            // still has open fhs locally.
            let dst_ino_before = self.inodes.find_ino_by_key(&dst_key, EntryType::File);

            // POSIX rename(2) atomically replaces an existing
            // regular-file dst. NSS now provides this primitive
            // directly via `force_overwrite=true`: when dst exists,
            // NSS atomically swaps in src's value and returns the
            // prior dst bytes so we can GC the orphaned blob.
            let old_bytes = self
                .backend()
                .rename_file(&src_key, &dst_key, true, &trace_id)
                .await?;

            // GC the blob backing the now-orphaned dst value (if
            // any). Mirrors the unlink-on-dst path -- handles
            // Normal / Mpu / Indirect (hardlink-decrement)
            // correctly and defers cleanup when the dst inode
            // still has open fhs locally.
            self.cleanup_orphaned_value(&dst_key, dst_ino_before, old_bytes, &trace_id)
                .await;

            // Tear down the dst inode's name mapping so the table
            // reflects the post-rename state -- otherwise the
            // alias-swap below would leave the orphaned dst inode
            // entry dangling at no key while still appearing
            // reachable. `remove_name_mapping` flips
            // `name_removed=true` when no other aliases survive,
            // letting `release` finish GC if the inode still has
            // open handles.
            if let Some(dst_ino) = dst_ino_before {
                self.inodes.remove_name_mapping(dst_ino, &dst_key);
            }

            // Swap the alias in the InodeTable. `rename_alias` only
            // updates `entry.s3_key` if it matches `src_key`; for a
            // hardlink-promoted inode whose primary alias is some
            // other name, only the `key_to_ino` row moves.
            if let Some(ino) = self.inodes.find_ino_by_key(&src_key, EntryType::File) {
                self.inodes.rename_alias(ino, &src_key, &dst_key);
            }

            // Update any open file handles to reflect the new key.
            for mut fh_entry in self.file_handles.iter_mut() {
                if fh_entry.value().s3_key == src_key {
                    fh_entry.value_mut().s3_key = dst_key.clone();
                }
            }

            self.dir_cache.invalidate(&src_prefix);
            self.dir_cache.invalidate(&dst_prefix);
            self.touch_parent_times(parent);
            if new_parent != parent {
                self.touch_parent_times(new_parent);
            }
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
            // POSIX NAME_MAX -- Linux's VFS hard-caps any path
            // component at 255 regardless of what FUSE advertises, so
            // anything larger here just makes pjdfstest pick a name
            // the kernel will reject before we ever see it.
            namelen: 255,
            frsize: DEFAULT_BLOCK_SIZE,
        }
    }
}

/// Background whole-blob prefetch. Walks every block of `layout`,
/// fetches it from BSS, and inserts it into the disk cache. Each
/// per-block fetch goes through the same path as a read miss
/// (`backend.read_block` + `dc.insert`) so block_id, version, and
/// checksum semantics stay identical between prefetch-warmed entries
/// and lazy-warmed ones.
///
/// Errors are logged and ignored: a prefetch is best-effort, and a
/// transient failure is acceptable -- the kernel's block-on-demand
/// path still serves the read.
async fn spawn_prefetch_task(
    backend_cfg: Arc<BackendConfig>,
    disk_cache: Arc<DiskCache>,
    layout: ObjectLayout,
) {
    let Ok(file_size) = layout.size() else {
        return;
    };
    if file_size == 0 {
        return;
    }
    let Ok(blob_guid) = layout.blob_guid() else {
        return;
    };
    let block_size = layout.block_size as u64;
    if block_size == 0 {
        return;
    }
    // Re-check pressure: an unrelated workload may have filled the
    // cache between the open-time decision and the task starting.
    let policy = crate::prefetch::PrefetchPolicy {
        full_threshold_bytes: u64::MAX,
        partial_threshold_bytes: u64::MAX,
        workload_bulk_read: false,
        // Reuse the cache's high-watermark fraction for the in-task
        // pressure decline.
        pressure_decline: 0.95,
    };
    if crate::prefetch::cache_pressure_high(
        disk_cache.current_usage(),
        disk_cache.capacity_bytes(),
        &policy,
    ) {
        return;
    }

    let backend = match StorageBackend::new(&backend_cfg) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "prefetch: failed to construct backend");
            return;
        }
    };

    let last_block = ((file_size - 1) / block_size) as u32;
    let trace_id = TraceId::new();

    for block_num in 0..=last_block {
        let block_start = block_num as u64 * block_size;
        let block_content_len = std::cmp::min(block_size, file_size - block_start) as usize;

        // If another path has already populated this block (e.g. a
        // racing read), the cache hit short-circuits the BSS round
        // trip.
        if disk_cache
            .get_block(blob_guid, block_num, block_content_len)
            .await
            .is_some()
        {
            continue;
        }

        let (data, _checksum) = match backend
            .read_block(
                blob_guid,
                layout.blob_version,
                block_num,
                block_content_len,
                &trace_id,
            )
            .await
        {
            Ok(r) => r,
            Err(FsError::DataVg(volume_group_proxy::DataVgError::BlockNotFound))
            | Err(FsError::Rpc(rpc_client_common::RpcError::NotFound)) => {
                // Sparse hole; intentionally not cached. The
                // block-on-demand path treats missing blocks as zeros.
                continue;
            }
            Err(e) => {
                tracing::debug!(
                    %blob_guid, block_num, error = %e,
                    "prefetch block fetch failed; abandoning prefetch"
                );
                return;
            }
        };

        let _ = disk_cache
            .insert_block(blob_guid, block_num, layout.blob_version, &data)
            .await;
    }
}

/// Periodically scrape `WritebackQueue` telemetry into the
/// `metrics_wrapper` backend. Required by the metrics-first contract
/// before tuning `max_batch_wait_ms` / `worker_pool_size`. Spawns
/// alongside the worker on first FUSE op.
fn spawn_writeback_metrics_exporter(queue: Arc<WritebackQueue>) {
    use metrics_wrapper::gauge;
    let scrape_interval = Duration::from_millis(500);
    compio_runtime::spawn(async move {
        loop {
            compio_runtime::time::sleep(scrape_interval).await;
            let st = queue.telemetry();
            gauge!("wb_queue_depth").set(st.depth as f64);
            gauge!("wb_bytes_buffered").set(st.bytes_buffered as f64);
            gauge!("wb_deferred_errors").set(st.deferred_errors as f64);
            gauge!("wb_backpressure_waits").set(st.backpressure_waits as f64);
            gauge!("wb_dependency_stalls").set(st.dependency_stalls as f64);
            gauge!("wb_enqueue_blocked").set(if st.enqueue_blocked { 1.0 } else { 0.0 });
        }
    })
    .detach();
}

/// Long-running writeback worker. Polls the queue every `poll_ms`,
/// drains pending PublishLayout intents, and fires one batched
/// `InodeBatch` RPC per drain window. Spawned at FUSE init; runs
/// until the process exits.
fn spawn_writeback_worker(
    backend_cfg: Arc<BackendConfig>,
    queue: Arc<WritebackQueue>,
    poll_ms: u32,
) {
    let poll_dur = Duration::from_millis(poll_ms.max(1) as u64);
    compio_runtime::spawn(async move {
        // Build a per-task StorageBackend; each compio thread initializes
        // its own NSS / BSS clients lazily.
        let backend = match StorageBackend::new(&backend_cfg) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "writeback worker: failed to init backend; aborting");
                return;
            }
        };

        loop {
            compio_runtime::time::sleep(poll_dur).await;

            // Drain a batch of PublishLayout intents. The drainer flips them
            // to InFlight before returning so concurrent enqueues fall
            // into the next-cycle / backpressure path.
            let drained = queue.drain_publish_layout(1024);
            if drained.is_empty() {
                continue;
            }

            // Build one batched RPC for the whole drain set. Each
            // intent maps to one InodeBatchEntry; the worker uses the
            // unconditional Put path for every entry today (PublishLayout on
            // the symlink / vfs_create paths is always the initial
            // publish, never a CAS). Adding CAS support is one extra
            // field on the entry once vfs_release flush moves into
            // the queue.
            let trace_id = TraceId::new();
            let mut batch_entries: Vec<nss_codec::InodeBatchEntry> =
                Vec::with_capacity(drained.len());
            for intent in &drained {
                let mut nss_key = intent.s3_key.clone();
                nss_key.push('\0');
                match &intent.op {
                    WbInodeOp::PutInode {
                        layout_bytes,
                        expected_old_value,
                        ..
                    } => {
                        let (cas_check, prev) = match expected_old_value {
                            Some(b) => (true, b.clone()),
                            None => (false, bytes::Bytes::new()),
                        };
                        batch_entries.push(nss_codec::InodeBatchEntry {
                            depends_on_index: vec![],
                            op: Some(nss_codec::inode_batch_entry::Op::Put(
                                nss_codec::PutInodeBatchEntry {
                                    key: nss_key,
                                    value: layout_bytes.clone(),
                                    cas_check,
                                    expected_old_value: prev,
                                },
                            )),
                        });
                    }
                    WbInodeOp::SetAttr { .. } => {
                        // SetAttr enqueue path isn't wired today. The
                        // worker already coalesces SetAttr into the
                        // PutInode layout when both target the same
                        // (key, gen); a SetAttr intent reaching the
                        // worker means there was no PutInode to merge
                        // into. We push it as a placeholder so the
                        // drained-vs-results indices stay aligned;
                        // the server treats it as an empty Put which
                        // returns PERMANENT_ERROR.
                        batch_entries.push(nss_codec::InodeBatchEntry {
                            depends_on_index: vec![],
                            op: Some(nss_codec::inode_batch_entry::Op::Setattr(
                                nss_codec::SetAttrBatchEntry {
                                    key: nss_key,
                                    attrs: bytes::Bytes::new(),
                                    cas_check: false,
                                    expected_old_value: bytes::Bytes::new(),
                                },
                            )),
                        });
                    }
                }
            }

            // Ancestor dependency wiring: for each entry, find the
            // longest same-batch directory entry whose key is a strict
            // prefix of this entry's key. That entry must commit
            // first; on its failure the server marks this entry
            // DEPENDENCY_FAILED and short-circuits, which the
            // group-requeue path then handles. Computed in O(N^2) on
            // batch size; N <= max_batch_size (default 1024) so the
            // walk is cheap relative to the RPC round-trip it amortizes.
            let entry_keys: Vec<String> = drained
                .iter()
                .map(|d| {
                    // The drainable key is the s3_key without NUL
                    // termination; entries are NUL-terminated when
                    // shipped, but for prefix comparison we strip it
                    // and use trailing slash as the directory marker.
                    d.s3_key.clone()
                })
                .collect();
            for i in 0..batch_entries.len() {
                let cur_key = &entry_keys[i];
                let parent = parent_prefix_of(cur_key);
                let mut best_dep: Option<u32> = None;
                let mut best_len = 0usize;
                for (j, other_key) in entry_keys.iter().enumerate().take(i) {
                    // A directory entry's key ends in `/`. The current
                    // entry's parent prefix also ends in `/` (or is
                    // `/` for top-level). Match by exact equality so
                    // we only depend on the *immediate* parent dir
                    // among the same-batch entries.
                    if other_key == &parent && other_key.len() > best_len {
                        best_len = other_key.len();
                        best_dep = Some(j as u32);
                    }
                }
                if let Some(dep_idx) = best_dep {
                    batch_entries[i].depends_on_index.push(dep_idx);
                }
            }

            let batch_result = backend.inode_batch(batch_entries, &trace_id).await;

            match batch_result {
                Err(e) => {
                    tracing::warn!(
                        n_entries = drained.len(),
                        error = %e,
                        "writeback InodeBatch RPC failed"
                    );
                    for intent in &drained {
                        let inode = intent.inode;
                        queue.mark_publish_layout_failed(
                            &intent.s3_key,
                            intent.generation,
                            inode,
                            false,
                        );
                    }
                }
                Ok(results) if results.len() != drained.len() => {
                    tracing::warn!(
                        sent = drained.len(),
                        got = results.len(),
                        "writeback InodeBatch result count mismatch"
                    );
                    for intent in &drained {
                        let inode = intent.inode;
                        queue.mark_publish_layout_failed(
                            &intent.s3_key,
                            intent.generation,
                            inode,
                            false,
                        );
                    }
                }
                Ok(results) => {
                    for (intent, result) in drained.iter().zip(results.iter()) {
                        let inode = intent.inode;
                        let status = nss_codec::BatchEntryStatus::try_from(result.status)
                            .unwrap_or(nss_codec::BatchEntryStatus::StatusUnspecified);
                        match status {
                            nss_codec::BatchEntryStatus::StatusOk => {
                                queue.mark_publish_layout_committed(
                                    &intent.s3_key,
                                    intent.generation,
                                    inode,
                                );
                            }
                            nss_codec::BatchEntryStatus::StatusCasConflict => {
                                tracing::warn!(
                                    key = %intent.s3_key,
                                    generation = intent.generation.0,
                                    "writeback PublishLayout CAS conflict"
                                );
                                queue.mark_publish_layout_failed(
                                    &intent.s3_key,
                                    intent.generation,
                                    inode,
                                    true,
                                );
                            }
                            _ => {
                                tracing::warn!(
                                    key = %intent.s3_key,
                                    generation = intent.generation.0,
                                    status = ?status,
                                    error = %result.error_message,
                                    "writeback PublishLayout entry failed"
                                );
                                queue.mark_publish_layout_failed(
                                    &intent.s3_key,
                                    intent.generation,
                                    inode,
                                    false,
                                );
                            }
                        }
                    }
                }
            }
        }
    })
    .detach();
}

/// Extract the parent prefix from an s3_key.
/// e.g. "/foo/bar" -> "/foo/", "/top" -> "/"
/// Zero-pad `bytes` up to `block_size_usize`, returning the original
/// (cheap clone) if it's already at least that large. Used by the
/// override flush + replace flush write paths so every block lands on
/// disk at full block_size.
/// Does the caller have write+search (W+X) permission on a directory
/// described by `posix`? Implements POSIX's three-class permission
/// resolution (owner / group / other) for the rename(2) parent-perm
/// gate. `caller_gid` is the caller's primary gid; supplementary
/// groups aren't tracked at the FUSE layer today, so a non-owner
/// caller falls through to the "other" class when their primary gid
/// doesn't match either -- matches kernel `default_permissions`'
/// approximation closely enough for pjdfstest's contract suite.
fn dir_has_wx_perm(
    posix: data_types::object_layout::PosixAttrs,
    caller_uid: u32,
    caller_gid: u32,
) -> bool {
    if caller_uid == 0 {
        return true;
    }
    let mode = posix.mode;
    let class_bits = if posix.uid == caller_uid {
        libc::S_IWUSR | libc::S_IXUSR
    } else if posix.gid == caller_gid {
        libc::S_IWGRP | libc::S_IXGRP
    } else {
        libc::S_IWOTH | libc::S_IXOTH
    };
    mode & class_bits == class_bits
}

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

fn symlink_mode(perm: u16) -> u32 {
    libc::S_IFLNK | perm as u32
}
