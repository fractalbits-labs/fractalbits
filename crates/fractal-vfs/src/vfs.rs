mod attr;
mod data_layout;
mod dir;
mod drain;
mod namespace;
mod open;
mod publish;
mod read;
mod row_map;
mod sweep;
mod write;
mod write_buffer;

use bytes::Bytes;
use dashmap::DashMap;
use data_types::object_layout::{ObjectLayout, ObjectState, SpecialKind};
use data_types::ovr_map::OvrRowMap;
use fractal_fuse::{FileHandleId, InodeId};
use rkyv::api::high::to_bytes_in;
use std::cell::Cell;
use std::os::fd::{AsRawFd, OwnedFd};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::backend::{BackendConfig, StorageBackend};
use crate::cache::{DirCache, DirEntry, DirEntryKind};
use crate::config::WritebackMode;
use crate::disk_cache::{DiskCache, MirrorHandle, spawn_mirror_worker};
use crate::error::FsError;
use crate::inode::InodeTable;
use crate::vfs::publish::spawn_writeback_worker;
use crate::vfs::sweep::SweepCoordinator;
use crate::vfs::write_buffer::WriteBuffer;
use crate::writeback::WritebackQueue;
pub const TTL: Duration = Duration::from_secs(1);
/// Bound on cached per-blob row snapshots.
const ROW_MAP_CACHE_CAP: usize = 4096;
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
    /// NSS. Clean handles refresh on the attr TTL so a long-lived open
    /// fd cannot pin a superseded generation set past the sweep.
    layout_refreshed_at: Instant,
    /// Serializes this handle's data operations (read / write / flush /
    /// truncate / fallocate / lseek) so a mid-operation layout refresh
    /// cannot interleave with a flush's prepare/commit window.
    operation_lock: Arc<futures::lock::Mutex<()>>,
    write_buf: Option<WriteBuffer>,
    backing_id: Option<i32>,
}

/// True when `current` differs from `expected` only in posix attributes:
/// the async SetPosix worker (or a chmod/utimensat) republished the row
/// between this flush's base snapshot and its CAS. Metadata updates clone
/// the fetched layout and carry the versioning fields (blob_version,
/// next_version, pending_append, map_epoch) forward unchanged, so a data
/// flush can rebase over them; any other divergence is a foreign writer
/// and stays a hard conflict. The row-CAS promotion rule depends on
/// exactly this narrowness: it promotes a stored `cur` into `prev` only
/// when that `cur` is at or below the ceiling observed at prepare time,
/// which is sound only because the ceiling cannot move between a flush's
/// prepare and commit. Widening the rebase to accept a moved
/// blob_version would silently unsound the rows' prev slots.
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

pub struct VfsCore {
    backend_config: Arc<BackendConfig>,
    inodes: Arc<InodeTable>,
    disk_cache: Option<Arc<DiskCache>>,
    dir_cache: DirCache,
    file_handles: DashMap<FileHandleId, FileHandle>,
    next_fh: AtomicU64,
    read_write: bool,
    passthrough_enabled: bool,
    passthrough_max_object_size: u64,
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
    fuse_dev_fd: Option<Arc<OwnedFd>>,
    // Tracks blob data for unlinked files that still have open handles.
    // Cleanup is deferred until the last handle is released.
    deferred_blob_cleanup: DashMap<InodeId, Bytes>,
    // InodeId-scoped write lock. At most one write-mode handle per inode is
    // allowed. Map value is the owning fh so a stale lock for a closed fh
    // can be reclaimed by the next opener. Reads do not touch
    // this lock.
    inode_write_owner: DashMap<InodeId, FileHandleId>,
    // Handle to the dedicated disk-cache mirror thread. `None` when the
    // disk cache is disabled or the mirror thread failed to start. Keeps
    // the best-effort local-cache write off the FUSE worker threads so it
    // does not steal foreground cycles on a create-heavy workload.
    mirror: Option<MirrorHandle>,
    /// Per-blob `@ovr/` row snapshots keyed by blob_id, each tagged with
    /// the `map_epoch` it was loaded under. A snapshot at epoch M serves
    /// any read whose layout still carries M (rows change only under a
    /// commit CAS that bumps the epoch), so invalidation is a cheap
    /// epoch compare, never a TTL. LRU-bounded: eviction reloads one
    /// blob's prefix, one listing page per 1000 rows.
    row_maps: parking_lot::Mutex<lru::LruCache<Uuid, Arc<OvrRowMap>>>,
    /// Coalesces per-blob reclamation and bounds concurrent cleanup.
    sweep_coordinator: Arc<SweepCoordinator>,
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

        // The mirror thread owns a clone of the disk-cache handle and
        // drains queued writes off the FUSE worker threads.
        let mirror = disk_cache
            .as_ref()
            .and_then(|dc| spawn_mirror_worker(dc.clone()));

        // A passthrough backing fd cannot be revoked when another instance
        // commits new row-mapped generations and the cache mirror changes
        // the stable file in place. Keep the raw-fd path disabled until
        // cache files are generation-specific or FUSE can revoke active
        // backing mappings.
        let passthrough_enabled = false;
        if config.passthrough_enabled {
            tracing::warn!("FUSE passthrough disabled for mutable versioned blobs");
        }
        let passthrough_max_object_size =
            config.passthrough_max_object_size_gb * 1024 * 1024 * 1024;
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
            disk_cache,
            dir_cache: DirCache::new(dir_cache_ttl),
            file_handles: DashMap::new(),
            next_fh: AtomicU64::new(1),
            read_write,
            passthrough_enabled,
            passthrough_max_object_size,
            prefetch_policy,
            writeback,
            writeback_mode,
            writeback_poll_ms,
            writeback_worker_started: AtomicBool::new(false),
            fuse_dev_fd: None,
            deferred_blob_cleanup: DashMap::new(),
            inode_write_owner: DashMap::new(),
            mirror,
            row_maps: parking_lot::Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(ROW_MAP_CACHE_CAP).expect("row map cap is nonzero"),
            )),
            sweep_coordinator: Arc::new(SweepCoordinator::default()),
        }
    }

    /// Install the shared `/dev/fuse` fd, obtained from
    /// `Session::fuse_fd()`, before the session is run. The fd is needed
    /// by passthrough open / close paths that may fire on the very first
    /// FUSE request.
    pub fn with_fuse_fd(mut self, fuse_dev_fd: Arc<OwnedFd>) -> Self {
        self.fuse_dev_fd = Some(fuse_dev_fd);
        self
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

    // ---------- Passthrough helpers ----------

    /// Try to set up passthrough for a file handle. Returns (open_flags, backing_id)
    /// if passthrough is activated, or (0, 0) otherwise.
    pub fn try_passthrough(&self, fh: FileHandleId, layout: &ObjectLayout) -> (u32, i32) {
        if !self.passthrough_enabled {
            return (0, 0);
        }
        if self.read_write {
            // A read-write mount can later override this blob. Once the
            // kernel has a passthrough backing fd, metadata floors and cache
            // file unlinks cannot revoke that raw fd, so only arm passthrough
            // on read-only mounts.
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

        // Passthrough bypasses the per-read exact-version check. Only arm
        // it for never-overwritten, unmapped layouts, where every block is
        // at its create-time identity.
        if layout.blob_version > 1
            || layout.is_mapped()
            || layout.next_burn_version() > layout.blob_version + 1
        {
            return (0, 0);
        }

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
                tracing::info!(fh = fh.0, backing_id = bid, "passthrough activated");
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
    pub fn try_passthrough_for_fh(&self, fh: FileHandleId) -> Option<(u32, i32)> {
        let handle = self.file_handles.get(&fh)?;
        let layout = handle.layout.as_ref()?;
        Some(self.try_passthrough(fh, layout))
    }

    /// Clean up passthrough backing_id on file release.
    pub fn release_passthrough(&self, fh: FileHandleId) {
        let backing_id = self.file_handles.get(&fh).and_then(|h| h.backing_id);

        if let Some(bid) = backing_id
            && let Some(fuse_dev_fd) = self.fuse_dev_fd.as_ref()
            && let Err(e) =
                fractal_fuse::passthrough::fuse_backing_close(fuse_dev_fd.as_raw_fd(), bid)
        {
            tracing::warn!(backing_id = bid, error = %e, "failed to close backing");
        }
    }

    // ---------- Public VFS operations ----------

    pub fn vfs_init(&self) {
        if let Some(dc) = &self.disk_cache {
            dc.spawn_evictor();
        }
        // Start the writeback worker here, on the FUSE lifecycle thread's
        // runtime. That runtime outlives the per-ring worker runtimes (it
        // drives `destroy` after every ring thread is joined), so the
        // worker keeps draining queued metadata through unmount instead of
        // dying with a ring runtime and leaving destroy to time out on a
        // dead drainer. `ensure_writeback_worker_started` is idempotent, so
        // the lazy calls on the metadata paths become no-ops.
        self.ensure_writeback_worker_started();
        self.ensure_sweep_worker_started();
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
        let mut mapped = layout_with(100, 1);
        mapped.set_map_epoch(4);
        assert!(
            !posix_only_moved(&base, &mapped),
            "a row-writing commit is a real writer"
        );
        let mut pending = layout_with(100, 1);
        pending.set_pending_append(Some((3, 5)));
        assert!(
            !posix_only_moved(&base, &pending),
            "an in-flight append record is a real writer"
        );
    }
}
