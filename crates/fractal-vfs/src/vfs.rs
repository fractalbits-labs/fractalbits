mod attr;
mod data_layout;
mod dir;
mod drain;
mod namespace;
mod open;
mod publish;
mod read;
mod sweep;
mod write;
mod write_buffer;

use dashmap::DashMap;
use data_types::object_layout::{ObjectLayout, ObjectState, SpecialKind};
use fractal_fuse::{FileHandleId, InodeId};
use std::cell::Cell;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::backend::{BackendConfig, StorageBackend};
use crate::cache::{DirCache, DirEntry, DirEntryKind};
use crate::config::WritebackMode;
use crate::error::FsError;
use crate::inode::InodeTable;
use crate::vfs::publish::spawn_writeback_worker;
use crate::vfs::write_buffer::WriteBuffer;
use crate::writeback::WritebackQueue;
pub const TTL: Duration = Duration::from_secs(1);
pub const DEFAULT_BLOCK_SIZE: u32 = 128 * 1024;
/// Upper bound on a single file's in-memory write buffer. The buffer is
/// a flat `BytesMut`, so a truncate/extend allocates the whole size; a
/// target beyond this is rejected with EINVAL rather than attempting a
/// runaway allocation (which would abort the process).
pub const MAX_INMEM_FILE_SIZE: u64 = 4 * 1024 * 1024 * 1024;

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
    /// to (s, ns) round-trips through `lstat.atime_ns`.
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

impl VfsAttr {
    /// Synthetic `VfsAttr` for a negative-dentry FUSE_LOOKUP reply.
    /// `ino == 0` is the FUSE protocol sentinel for "name does not
    /// exist"; combined with a non-zero entry TTL the kernel caches
    /// the absence and skips future LOOKUPs for the same name. The
    /// kernel reads only `nodeid` for negative entries, so the rest
    /// are zeros.
    pub fn negative_dentry() -> Self {
        Self {
            ino: 0,
            size: 0,
            blocks: 0,
            atime_secs: 0,
            mtime_secs: 0,
            ctime_secs: 0,
            atime_ns_part: 0,
            mtime_ns_part: 0,
            ctime_ns_part: 0,
            mode: 0,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VfsDirEntry {
    pub ino: u64,
    pub offset: u64,
    pub kind: DirEntryKind,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct VfsDirEntryPlus {
    pub ino: u64,
    pub offset: u64,
    pub kind: DirEntryKind,
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

struct FileHandle {
    ino: InodeId,
    s3_key: String,
    layout: Option<ObjectLayout>,
    /// When the committed layout snapshot was last confirmed against
    /// the metadata store. Clean handles refresh on the attr TTL so a long-lived open
    /// fd cannot pin a superseded generation set past the sweep.
    layout_refreshed_at: Instant,
    /// Serializes this handle's data operations (read / write / flush /
    /// truncate / fallocate / lseek) so a mid-operation layout refresh
    /// cannot interleave with a flush's prepare/commit window.
    operation_lock: Arc<futures::lock::Mutex<()>>,
    write_buf: Option<WriteBuffer>,
}

pub struct VfsCore {
    backend_config: Arc<BackendConfig>,
    inodes: Arc<InodeTable>,
    dir_cache: DirCache,
    file_handles: DashMap<FileHandleId, FileHandle>,
    next_fh: AtomicU64,
    read_write: bool,
    prefetch_policy: crate::prefetch::PrefetchPolicy,
    /// Writeback queue. Always present, but only consulted when
    /// `writeback_mode` is `Default`. The worker is spawned from
    /// `vfs_init` (see there for why the FUSE lifecycle thread's runtime
    /// is the right host); the metadata paths also call the idempotent
    /// starter so the queue is never drained by nobody.
    writeback: Arc<WritebackQueue>,
    writeback_mode: WritebackMode,
    /// `max_batch_wait_ms` from the writeback config; the drainer
    /// polls this often.
    writeback_poll_ms: u32,
    /// One-shot guard for the writeback worker. Flipped by
    /// `ensure_writeback_worker_started`.
    writeback_worker_started: AtomicBool,
    /// Unlinked inodes whose value the gateway moved to a hidden
    /// `@orphan/` key for the still-open handles; the last close deletes
    /// the key with teardown.
    orphans: DashMap<InodeId, String>,
    // InodeId-scoped write lock. At most one write-mode handle per inode is
    // allowed. Map value is the owning fh so a stale lock for a closed fh
    // can be reclaimed by the next opener. Reads do not touch
    // this lock.
    inode_write_owner: DashMap<InodeId, FileHandleId>,
    /// Orphan hand-offs to the gateway not yet acknowledged; `destroy`
    /// waits for these before exiting.
    sweep_inflight: Arc<AtomicUsize>,
}

impl VfsCore {
    pub fn new(
        backend_config: Arc<BackendConfig>,
        inodes: Arc<InodeTable>,
        read_write: bool,
    ) -> Self {
        let config = &backend_config.config;
        let dir_cache_ttl = config.dir_cache_ttl();

        let prefetch_policy = crate::prefetch::PrefetchPolicy::from_config(config);
        // An unparseable mode is a misconfiguration: warn loudly and fall
        // back to Strict (fail-safe for durability) instead of silently
        // running a mode the operator did not ask for.
        let writeback_mode = WritebackMode::from_str(&config.writeback_mode).unwrap_or_else(|_| {
            tracing::warn!(
                value = %config.writeback_mode,
                "invalid FS_SERVER_WRITEBACK_MODE; falling back to strict"
            );
            WritebackMode::Strict
        });
        // Worker poll interval; honoured as configured (default 2ms). The
        // metadata path issues one put_inode per intent, so a large poll
        // just adds latency that drain_inode_to_barrier (every
        // unlink/rmdir/close) then waits out; keep the default tight. A
        // wake-on-enqueue notify would remove the residual poll latency
        // entirely and is the natural follow-up.
        let writeback_poll_ms = config.writeback_poll_ms.clamp(1, 1000);
        let writeback = Arc::new(WritebackQueue::new());

        Self {
            backend_config,
            inodes,
            dir_cache: DirCache::new(dir_cache_ttl),
            file_handles: DashMap::new(),
            next_fh: AtomicU64::new(1),
            read_write,
            prefetch_policy,
            writeback,
            writeback_mode,
            writeback_poll_ms,
            writeback_worker_started: AtomicBool::new(false),
            orphans: DashMap::new(),
            inode_write_owner: DashMap::new(),
            sweep_inflight: Arc::new(AtomicUsize::new(0)),
        }
    }

    // ---------- Internal helpers ----------

    /// Get the per-thread StorageBackend, creating it on first access.
    /// The backend is leaked into 'static storage because each compio thread
    /// runs for the lifetime of the process and we need references that can
    /// be held across await points.
    fn backend(&self) -> &'static StorageBackend {
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

    /// The inode key an open handle reads and publishes through: its
    /// name, or the hidden orphan key of an unlinked-but-open file.
    pub(crate) fn handle_key(&self, fh: FileHandleId) -> Result<String, FsError> {
        Ok(self
            .file_handles
            .get(&fh)
            .ok_or(FsError::BadFd)?
            .s3_key
            .clone())
    }

    fn alloc_fh(&self) -> FileHandleId {
        FileHandleId(self.next_fh.fetch_add(1, Ordering::Relaxed))
    }

    fn dir_prefix(&self, ino: InodeId) -> Option<String> {
        self.inodes.get_s3_key(ino)
    }

    fn cache_dir_entry(&self, prefix: &str, name: &str, ino: InodeId, kind: DirEntryKind) {
        self.dir_cache.upsert(
            prefix,
            DirEntry {
                name: name.to_string(),
                ino: ino.0,
                kind,
            },
        );
    }

    fn dir_entry_kind_from_layout(layout: &ObjectLayout) -> DirEntryKind {
        match &layout.state {
            ObjectState::Symlink(_) => DirEntryKind::Symlink,
            ObjectState::Special(data) => match data.kind {
                SpecialKind::Fifo => DirEntryKind::NamedPipe,
                SpecialKind::BlockDevice => DirEntryKind::BlockDevice,
                SpecialKind::CharDevice => DirEntryKind::CharDevice,
                SpecialKind::Socket => DirEntryKind::Socket,
            },
            ObjectState::Directory(_) => DirEntryKind::Directory,
            _ => DirEntryKind::RegularFile,
        }
    }

    fn check_write_enabled(&self) -> Result<(), FsError> {
        if !self.read_write {
            return Err(FsError::ReadOnly);
        }
        Ok(())
    }

    fn has_open_handles_for_inode(&self, ino: InodeId, exclude_fh: Option<FileHandleId>) -> bool {
        self.file_handles.iter().any(|entry| {
            entry.value().ino == ino && exclude_fh.is_none_or(|excl| *entry.key() != excl)
        })
    }

    /// The inode's registered write-owner fh, if its buffer is dirty.
    /// Single-writer-per-inode makes this the only handle that can carry
    /// a dirty buffer (a reclaimed owner's handle is already gone from
    /// `file_handles`), so callers get O(1) instead of scanning every
    /// open handle on the hot open path.
    fn dirty_write_owner(&self, inode: InodeId) -> Option<FileHandleId> {
        let fh = self.inode_write_owner.get(&inode).map(|e| *e.value())?;
        self.file_handles
            .get(&fh)?
            .write_buf
            .as_ref()
            .is_some_and(|wb| wb.dirty)
            .then_some(fh)
    }

    /// Live size of the inode's dirty write buffer, or `None` when no
    /// write-mode handle currently holds one. Distinguishes "no dirty
    /// handle" from "dirty handle whose buffer is empty" (size 0), which
    /// the read-your-writes lookup path needs to decide whether the live
    /// buffer size should override a stale cached layout size.
    fn dirty_write_buffer_size(&self, ino: InodeId) -> Option<u64> {
        self.inode_write_owner
            .get(&ino)
            .map(|e| *e.value())
            .and_then(|fh| {
                self.file_handles
                    .get(&fh)
                    .and_then(|h| h.write_buf.as_ref().map(|wb| wb.file_size))
            })
    }

    fn dirty_buffer_size(&self, ino: InodeId) -> u64 {
        self.dirty_write_buffer_size(ino).unwrap_or(0)
    }

    // ---------- Public VFS operations ----------

    pub fn vfs_init(&self) {
        // Start the writeback worker here, on the FUSE lifecycle thread's
        // runtime. That runtime outlives the per-ring worker runtimes (it
        // drives `destroy` after every ring thread is joined), so the
        // worker keeps draining queued metadata through unmount instead of
        // dying with a ring runtime and leaving destroy to time out on a
        // dead drainer. `ensure_writeback_worker_started` is idempotent, so
        // the lazy calls on the metadata paths become no-ops.
        self.ensure_writeback_worker_started();
        tracing::info!("Filesystem initialized");
    }

    /// Spawn the writeback worker the first time it's needed. Cheap
    /// fast path: a relaxed atomic load + branch in steady state. The
    /// `compare_exchange` only fires once per process.
    fn ensure_writeback_worker_started(&self) {
        if self.writeback_mode != WritebackMode::Default {
            return;
        }
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
        tracing::info!(poll_ms = self.writeback_poll_ms, "writeback worker started");
    }

    pub fn vfs_destroy(&self) {
        // Block new enqueues; the worker keeps draining whatever is
        // already InFlight / Pending until the queue depth hits 0 or
        // the host process exits.
        if self.writeback_mode == WritebackMode::Default {
            self.writeback.set_enqueue_blocked(true);
            tracing::info!(
                queue_depth = self.writeback.depth(),
                "writeback enqueue blocked at destroy; draining residual"
            );
        }
        tracing::info!("Filesystem destroyed");
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

/// Wall-clock nanoseconds since the Unix epoch. `0` on the (impossible)
/// pre-epoch clock so callers can treat `0` as the uninitialised
/// sentinel.
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
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
