use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use data_types::TraceId;
use fractal_fuse::{FileHandleId, InodeId};
use rkyv::api::high::to_bytes_in;
use std::cell::Cell;
use std::os::fd::{AsRawFd, OwnedFd};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::backend::{BackendConfig, BlobInfo, StorageBackend};
use crate::cache::{DirCache, DirEntry, DirEntryKind};
use crate::config::WritebackMode;
use crate::disk_cache::DiskCache;
use crate::error::FsError;
use crate::inode::{EntryType, ForgetOutcome, InodeTable, ROOT_INODE};
use crate::writeback::{
    CoalesceOutcome, DrainableInodeIntent, Generation, InodeOp as WbInodeOp, WritebackQueue,
};
use data_types::object_layout::{
    DirectoryData, IndirectEntry, InodeRecord, MpuState, ObjectCoreMetaData, ObjectLayout,
    ObjectMetaData, ObjectState, PosixAttrs, SpecialData, SpecialKind, SymlinkData,
};
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

/// Per-block content intent for the sparse WriteBuffer.
///
/// Blocks NOT in the map are implicitly "Keep": no buffered work, BSS is
/// authoritative. The override flush uploads only `Rewrite` blocks (in
/// place at the bumped blob_version), replays `Delete` intents as
/// versioned block deletes, and never touches "Keep"/absent blocks. The
/// sparse buffer keeps in-memory ops O(1), avoids whole-file preload on
/// open, and serves dirty-handle reads per block.
#[derive(Debug, Clone)]
enum BlockState {
    /// Definitive new bytes for this block. Origin: `vfs_write`, a shrink
    /// tail-zero, or a punch-hole partial edge. The override flush uploads
    /// these (zero-padded to block_size) at the new blob_version.
    Rewrite(Bytes),
    /// PUNCH_HOLE intent: the override flush schedules a versioned
    /// `delete_block` so the BSS entry is dropped at the new blob_version.
    /// Reads (dirty-handle merge and post-flush via `BlockNotFound`) treat
    /// the block as zeros. Distinguished from a plain hole because a
    /// punched block sits inside the file's logical range and the deletion
    /// must be replayed on flush even with no `Rewrite` content.
    Delete,
}

struct WriteBuffer {
    /// Logical file size (includes holes). Authoritative within this
    /// handle session for stat / read clamping until flush commits.
    file_size: u64,
    /// True if `file_size` differs from the committed layout size at open
    /// time, or any block intent was buffered. Flush-eligibility predicate.
    size_changed: bool,
    /// Blob guid of the file at open time; used to lazy-load committed
    /// bytes for partial-block edits and dirty reads, and reused by the
    /// override flush. `None` for brand-new files.
    existing_blob_guid: Option<data_types::DataBlobGuid>,
    /// Block size copied from the committed layout (or DEFAULT for new
    /// files).
    block_size: u32,
    /// Per-block content intents, keyed by block index.
    blocks: std::collections::BTreeMap<u32, BlockState>,
    /// True if any flush-worthy work is buffered.
    dirty: bool,
    /// Smallest `ceil(new_size / block_size)` reached by any shrink in this
    /// session. Blocks at index `>= eof_low_watermark` had their committed
    /// BSS data logically destroyed by the shrink and must read as zeros
    /// until the flush trim deletes them, even if a later grow brings the
    /// index back into the file. Reset to `None` only on a successful
    /// flush. Without it, `truncate(small); write(past old EOF)` would
    /// lazy-load pre-shrink bytes and resurrect data POSIX requires zeroed.
    eof_low_watermark: Option<u32>,
    /// `committed_block_count` pinned at the FIRST shrink this session.
    /// Pairs with `eof_low_watermark` to bound the EOF-trim across
    /// post-CAS-failure retries: the flush promotes the committed size to
    /// the smaller new size, so recomputing the upper bound from the layout
    /// on retry would lose the original committed bound. Reset on flush.
    trim_upper: Option<u32>,
    /// Block indices fallocate has reserved. On flush these become
    /// `ReserveBlocks` (single-op, no batch) for blocks not superseded by a
    /// `Rewrite`/`Delete`. Reads and `lseek(SEEK_DATA)` treat reserved
    /// blocks as logical-data per Linux convention even before flush.
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

    /// Drop per-block intents and reservations past the new EOF (shrink).
    fn drop_blocks_past(&mut self, new_last_block_excl: u32) {
        self.blocks.retain(|b, _| *b < new_last_block_excl);
        self.pending_reservations
            .retain(|b| *b < new_last_block_excl);
    }

    /// True when block `b` sits in a range whose committed BSS bytes were
    /// destroyed by a shrink earlier this session; lazy-load and
    /// dirty-read paths must return zeros for such blocks.
    fn block_destroyed_by_shrink(&self, b: u32) -> bool {
        self.eof_low_watermark.is_some_and(|low| b >= low)
    }
}

struct FileHandle {
    ino: InodeId,
    s3_key: String,
    layout: Option<ObjectLayout>,
    write_buf: Option<WriteBuffer>,
    backing_id: Option<i32>,
}

/// A best-effort disk-cache mirror write, handed to the dedicated mirror
/// thread so the local-cache I/O + checksum never run on a FUSE worker.
struct MirrorJob {
    blob_guid: data_types::DataBlobGuid,
    blob_version: u64,
    rewrites: Vec<(u32, Bytes)>,
    deletes: Vec<u32>,
    /// Retained `rewrites` payload size, used to keep `mirror_queued_bytes`
    /// balanced (added on enqueue, subtracted once the job is processed).
    byte_len: usize,
}

/// Sender + the shared queued-byte counter for the mirror channel.
struct MirrorHandle {
    tx: futures::channel::mpsc::Sender<MirrorJob>,
    queued_bytes: Arc<AtomicUsize>,
}

/// Bound on queued mirror jobs by count. The dedicated thread drains local
/// page-cache writes far faster than the network publish feeds it, so this
/// rarely fills; when it does, `try_send` drops the job (best-effort; the
/// block cold-fills from BSS on the next read) instead of blocking a FUSE
/// worker.
const MIRROR_QUEUE_CAP: usize = 4096;

/// Byte bound on the mirror queue. A job retains its rewritten `Bytes`
/// until the mirror thread writes them, so a slow cache device could
/// otherwise pin unbounded flushed write buffers (one large-file override
/// flush is a single job but many MiB). When the in-flight payload exceeds
/// this, new jobs are dropped (best-effort) before their `Bytes` are
/// retained. 256 MiB caps memory while staying far above the steady-state
/// backlog of an 83k-file untar.
const MIRROR_BYTE_BUDGET: usize = 256 * 1024 * 1024;

/// Spawn the dedicated disk-cache mirror thread. It owns its own compio
/// runtime (separate io_uring) and drains the job channel, so a
/// create-heavy workload's cache writes never steal cycles from the FUSE
/// worker threads. Returns the handle, or `None` if the runtime could not
/// be built (mirror then silently disabled; the cache still serves
/// reads via cold-fill, just not write-populated).
fn spawn_mirror_worker(dc: Arc<DiskCache>) -> Option<MirrorHandle> {
    let (tx, mut rx) = futures::channel::mpsc::channel::<MirrorJob>(MIRROR_QUEUE_CAP);
    let queued_bytes = Arc::new(AtomicUsize::new(0));
    let worker_bytes = queued_bytes.clone();
    let spawned = std::thread::Builder::new()
        .name("fb-disk-mirror".to_string())
        .spawn(move || {
            let rt = match compio_runtime::Runtime::builder().build() {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::warn!(error = %e, "disk-cache mirror thread: runtime build failed");
                    return;
                }
            };
            rt.block_on(async move {
                use futures::StreamExt;
                while let Some(job) = rx.next().await {
                    if let Err(e) = dc
                        .sync_after_flush(
                            job.blob_guid,
                            job.blob_version,
                            &job.rewrites,
                            &job.deletes,
                        )
                        .await
                    {
                        tracing::warn!(
                            blob_version = job.blob_version,
                            error = %e,
                            "disk cache mirror write failed (best-effort)"
                        );
                    }
                    worker_bytes.fetch_sub(job.byte_len, Ordering::Relaxed);
                }
            });
        });
    match spawned {
        Ok(_) => Some(MirrorHandle { tx, queued_bytes }),
        Err(e) => {
            tracing::warn!(error = %e, "failed to spawn disk-cache mirror thread");
            None
        }
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
    /// `writeback_mode` is `Default`. Worker is spawned lazily on
    /// the first FUSE op (the FUSE adapter's `init()` trait method
    /// is dead in this codebase; the session handles FUSE_INIT
    /// itself, so we spawn from inside the compio runtime when
    /// the first op arrives).
    writeback: Arc<WritebackQueue>,
    writeback_mode: WritebackMode,
    /// `max_batch_wait_ms` from the writeback config; the drainer
    /// polls this often.
    writeback_poll_ms: u32,
    /// One-shot guard for the writeback worker. Flipped by
    /// `ensure_writeback_worker` on first FUSE op.
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

        let passthrough_enabled = config.passthrough_enabled;
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

    /// Largest buffered file size across this inode's write handles: the
    /// in-memory EOF of a file whose first flush hasn't published yet.
    /// `0` when no write handle survives (e.g. the flush failed and the
    /// handle is gone). Single-writer-per-inode: the registered owner is
    /// the only handle that can hold a write buffer.
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

    /// Acquire the inode-scoped write lock for `fh`. Returns `Busy` if another
    /// write-mode handle currently owns it.
    ///
    /// Reclaim rule: if the recorded owner fh has been released (no entry in
    /// `file_handles`), the lock is stale and we take it. This recovers from
    /// any path that removes a handle without first calling
    /// `release_write_lock` (e.g. lookup races during shutdown).
    fn acquire_write_lock(&self, inode: InodeId, fh: FileHandleId) -> Result<(), FsError> {
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

    /// Acquire the inode write lock, briefly retrying to absorb the
    /// close-then-reopen-for-write race: a just-closed handle's FUSE_RELEASE
    /// (which drops this lock via `release_write_lock`) is asynchronous and
    /// may not have been processed by the time the kernel sends the next
    /// OPEN, so a single-process `write(); open(O_WRONLY)` would otherwise
    /// spuriously EBUSY (observed in truncate/O_TRUNC tests once per-flush
    /// latency grew). A genuinely concurrent writer keeps its handle open
    /// past the budget and still gets EBUSY.
    async fn acquire_write_lock_retry(
        &self,
        inode: InodeId,
        fh: FileHandleId,
    ) -> Result<(), FsError> {
        if self.acquire_write_lock(inode, fh).is_ok() {
            return Ok(());
        }
        // The lock may be held by an in-flight async close-flush:
        // FUSE_RELEASE spawns `vfs_release` off-thread and only drops the
        // write lock once the publish lands. Drain this inode's writeback
        // barrier so a re-open of a just-closed file (e.g. an O_TRUNC
        // reopen, or `echo x > f; cat f`) waits for the prior close to
        // commit (and reads its freshly published layout) instead of
        // spuriously failing EBUSY. No-op on an idle inode.
        self.drain_inode_to_barrier(inode).await?;
        if self.acquire_write_lock(inode, fh).is_ok() {
            return Ok(());
        }
        let deadline = Instant::now() + Duration::from_millis(200);
        while Instant::now() < deadline {
            compio_runtime::time::sleep(Duration::from_millis(5)).await;
            // OPEN can beat the kernel's later RELEASE request for the
            // previous fd. Re-check the barrier in the retry loop so once
            // RELEASE registers its cycle, this path waits for the full
            // publish instead of timing out on the fixed dispatch window.
            self.drain_inode_to_barrier(inode).await?;
            if self.acquire_write_lock(inode, fh).is_ok() {
                return Ok(());
            }
        }
        Err(FsError::Busy)
    }

    fn release_write_lock(&self, inode: InodeId, fh: FileHandleId) {
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

    fn make_file_attr(&self, ino: InodeId, layout: &ObjectLayout) -> Result<VfsAttr, FsError> {
        let size = layout.size()?;
        let ts = layout.timestamp / 1000;
        // Symlinks share the regular-file attribute path but report
        // S_IFLNK + 0 blocks. The kernel uses the mode bit to decide
        // whether to call FUSE_READLINK or FUSE_OPEN on a lookup.
        let is_symlink = layout.is_symlink();
        // Special inodes (fifo / block / char / unix-socket) share the
        // same attribute path; the kernel uses the S_IFMT bit and
        // `rdev` to dispatch I/O to its own pipe / device / socket
        // layer rather than calling FUSE_READ / FUSE_WRITE.
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
                SpecialKind::Fifo => libc::S_IFIFO,
                SpecialKind::BlockDevice => libc::S_IFBLK,
                SpecialKind::CharDevice => libc::S_IFCHR,
                SpecialKind::Socket => libc::S_IFSOCK,
            };
            ifmt | (self.file_perm() as u32 & !libc::S_IFMT)
        } else {
            file_mode(self.file_perm())
        };
        // posix.mode may be a raw permission-bits value coming from a
        // chmod that didn't include S_IFMT. Re-stamp the file-type
        // bits from `default_mode` so the kernel sees a valid mode_t.
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
        let attr = VfsAttr {
            ino: ino.0,
            size,
            blocks: if is_symlink || special.is_some() {
                0
            } else {
                size.div_ceil(512)
            },
            // PosixAttrs intentionally drops the per-inode atime; we
            // mirror mtime so a freshly created inode reports a
            // non-zero atime. apply_atime_override layers any
            // utimensat-set atime on top after this builds.
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
        };
        Ok(self.apply_atime_override(ino, attr))
    }

    /// Fallback file attr when layout is unavailable (e.g., inode evicted
    /// between fetch_dir_entries and readdirplus iteration). Uses correct
    /// kind=RegularFile to avoid on-wire inconsistency.
    fn make_default_file_attr(&self, ino: InodeId) -> VfsAttr {
        VfsAttr {
            ino: ino.0,
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

    fn make_dir_attr(&self, ino: InodeId) -> VfsAttr {
        let posix = self.inodes.get(ino).map(|e| e.posix).unwrap_or_default();
        // FUSE root inode reports mode 0o777 unconditionally so the
        // kernel's permission check lets every caller into the mount;
        // sub-directory inodes honour their persisted mode normally.
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
        let attr = VfsAttr {
            ino: ino.0,
            size: 0,
            blocks: 0,
            atime_secs: mtime_secs,
            mtime_secs,
            ctime_secs,
            atime_ns_part: mtime_ns_part,
            mtime_ns_part,
            ctime_ns_part,
            mode,
            // We do not maintain the traditional `2 + immediate_subdirs`
            // directory link count (it would cost an NSS listing per
            // stat), so report `1` (the btrfs convention) instead of a
            // constant `2`. A constant `nlink == 2` falsely tells
            // `find`/`du`/`fts` the directory has zero subdirectories, so
            // their leaf optimisation can skip recursing into real
            // children. A count below 2 is the standard "link count not
            // tracked, scan every entry" signal. POSIX permits nlink=1
            // for directories; the `2 + subdirs` scheme is a
            // traditional-FS convention, not a requirement.
            nlink: 1,
            uid: posix.uid,
            gid: posix.gid,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        };
        self.apply_atime_override(ino, attr)
    }

    fn make_new_file_attr(&self, ino: InodeId, size: u64) -> VfsAttr {
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
        let attr = VfsAttr {
            ino: ino.0,
            size,
            blocks: size.div_ceil(512),
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
        };
        self.apply_atime_override(ino, attr)
    }

    /// Layer an explicit `utimensat`-set atime (held in
    /// `InodeEntry.atime_ns`, volatile) on top of the mtime-mirrored
    /// atime the builders emit. No-op when no override is set.
    fn apply_atime_override(&self, ino: InodeId, mut attr: VfsAttr) -> VfsAttr {
        if let Some(entry) = self.inodes.get(ino)
            && entry.atime_ns != 0
        {
            attr.atime_secs = entry.atime_ns / 1_000_000_000;
            attr.atime_ns_part = (entry.atime_ns % 1_000_000_000) as u32;
        }
        attr
    }

    // ── Passthrough helpers ──

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

    // ── Cache helpers ──

    /// Read a block, checking disk cache first. On miss, fetches from backend
    /// and populates disk cache.
    async fn read_block_cached(
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

    // ── Read helpers ──

    /// Authoritative logical file size for data reads. The geometry
    /// sentinel (our BSS-parent-size authority) reflects the latest
    /// committed override regardless of our cached layout version, so a
    /// read on a handle whose cached layout lags a peer's overwrite (or
    /// this instance's own just-committed flush) still sees the right EOF.
    /// The cached/NSS layout size is a lazy copy. Falls back to the cached
    /// size when no sentinel exists or it is older than the cached layout
    /// (so a stale sentinel never shrinks a fresher local size).
    async fn authoritative_file_size(&self, layout: &ObjectLayout) -> Result<u64, FsError> {
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

    // ── Write helpers ──

    /// Load one block's committed bytes from BSS for an RMW / dirty read /
    /// flush tail-zero. Returns zeros (length `fallback_content_len`) for a
    /// brand-new file, a hole (`committed_content_len == 0`), or a missing
    /// block (`BlockNotFound` / `NotFound`); propagates other errors.
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

    /// Serve a read against a dirty write handle by merging per-block
    /// intents (`Rewrite` bytes, `Delete`/shrunk-range zeros,
    /// else lazy-loaded committed bytes) over the buffered `file_size`.
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
    fn restore_flush_snapshot(
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

    async fn flush_write_buffer(&self, fh_id: FileHandleId) -> Result<(), FsError> {
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
        // case its data lives in the shared `#hardlink/<id>` InodeRecord
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
        // `#hardlink/<id>` via CAS, not at this name's s3_key. Fetch the
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
                data_write_token: 0,
                next_data_write_token: 0,
                pending_data_write: None,
                state: ObjectState::Normal(ObjectMetaData {
                    blob_guid,
                    core_meta_data: ObjectCoreMetaData {
                        size: file_size,
                        etag: blob_guid.blob_id.simple().to_string(),
                        headers: vec![],
                        checksum: None,
                        posix: Some(Box::new(effective_posix)),
                    },
                }),
            };
            // Choose the publish target. A promoted inode republishes its
            // layout inside the shared InodeRecord at the `#hardlink/<id>`
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
                        self.backend()
                            .delete_block(blob_guid, b, new_version, &trace_id)
                            .await;
                    }
                    // Replay PUNCH_HOLE intents.
                    for (b, st) in snap.blocks.iter() {
                        if matches!(st, BlockState::Delete) {
                            self.backend()
                                .delete_block(blob_guid, *b, new_version, &trace_id)
                                .await;
                        }
                    }
                    // Reserve fallocate-claimed blocks not superseded by a
                    // Rewrite/Delete this flush (single-op; EC is a no-op).
                    for b in snap.pending_reservations.iter() {
                        if snap.blocks.contains_key(b) {
                            continue;
                        }
                        let _ = self
                            .backend()
                            .reserve_block(blob_guid, *b, block_size as u32, new_version, &trace_id)
                            .await;
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

        // Publish the authoritative blob-geometry sentinel so a peer instance
        // serving vfs_getattr from a stale cached layout still observes the
        // latest cross-instance size override (the inode size+blob_version it
        // cached may lag this flush). Initial creates use a fresh blob_guid
        // and publish exact size in NSS, so only override versions need this
        // extra BSS write.
        if final_layout.blob_version > 1
            && let Ok(geom_guid) = final_layout.blob_guid()
        {
            let new_bc = file_size.div_ceil(block_size as u64) as u32;
            let info = BlobInfo {
                total_size: file_size,
                block_count: new_bc,
                blob_version: final_layout.blob_version,
            };
            if let Err(e) = self
                .backend()
                .write_blob_info(geom_guid, info, final_layout.blob_version, &trace_id)
                .await
            {
                tracing::warn!(
                    %geom_guid,
                    blob_version = final_layout.blob_version,
                    error = %e,
                    "write_blob_info (geometry sentinel) failed; cross-instance size may lag until next flush"
                );
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

    async fn fetch_dir_entries(
        &self,
        parent: InodeId,
        prefix: &str,
    ) -> Result<Arc<Vec<DirEntry>>, FsError> {
        if let Some(cached) = self.dir_cache.get(prefix) {
            let stale = cached
                .iter()
                .any(|entry| self.inodes.get(InodeId(entry.ino)).is_none());
            if !stale {
                return Ok(cached);
            }
            tracing::debug!(%prefix, "Directory cache contains stale inode(s), rebuilding");
            self.dir_cache.invalidate(prefix);
        }

        // A cold listing races the async worker: a queued create
        // (mkdir/symlink/mknod PutInode) or an in-flight release publish
        // may not be in NSS yet, and the incomplete listing would then be
        // cached until the TTL, hiding an entry the caller already saw
        // created. Wait for those cycles to commit first. Taints are left
        // in place (readdir is not an error-reporting point); a failed
        // create is simply absent from the listing.
        if self.writeback_mode == WritebackMode::Default {
            // Flush still-dirty open handles first: a file in the
            // close(2)-to-FUSE_RELEASE window has no registered cycle yet,
            // so draining only known cycles/intents would list NSS without
            // it and cache the incomplete listing. Flushing registers the
            // cycle so the wait below blocks on it. Mirrors
            // drain_writeback_under_prefix / vfs_fsyncdir.
            self.flush_dirty_handles_under_prefix(prefix).await?;
            for ino in self.writeback_drain_targets_under_prefix(prefix) {
                if let Some(barrier) = self.writeback.fsync_barrier(ino) {
                    self.wait_cycles_drained(ino, barrier).await?;
                }
            }
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
            ino: parent.0,
            kind: DirEntryKind::Directory,
        });
        all_entries.push(DirEntry {
            name: "..".to_string(),
            ino: dotdot_ino.0,
            kind: DirEntryKind::Directory,
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

                if let Some(layout) = entry.layout.as_ref() {
                    // File - backend already stripped trailing \0 from keys
                    if !layout.is_listable() {
                        continue;
                    }
                    if name.is_empty() {
                        continue;
                    }
                    let kind = Self::dir_entry_kind_from_layout(layout);
                    let (ino, _) =
                        self.inodes
                            .lookup_or_insert(raw_key, EntryType::File, entry.layout);
                    all_entries.push(DirEntry {
                        name: name.to_string(),
                        ino: ino.0,
                        kind,
                    });
                } else {
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
                        ino: ino.0,
                        kind: DirEntryKind::Directory,
                    });
                }
            }

            if let Some(last) = last_key {
                start_after = last;
            } else {
                break;
            }
        }

        Ok(self.dir_cache.insert(prefix.to_string(), all_entries))
    }

    // ── Public VFS operations ──

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

    /// POSIX `NAME_MAX = 255`. Linux's general VFS enforces this at
    /// the kernel level for native filesystems but FUSE callers have
    /// to enforce it themselves; pjdfstest's `02.t` boundary tests
    /// (chmod/02.t, mkdir/02.t, etc.) pick a 256-byte component and
    /// expect ENAMETOOLONG.
    #[inline]
    fn check_name_max(name: &str) -> Result<(), FsError> {
        if name.len() > 255 {
            return Err(FsError::NameTooLong);
        }
        Ok(())
    }

    /// PATH_MAX boundary guard, separate from `check_name_max`. The
    /// kernel enforces PATH_MAX on the path the syscall receives
    /// before forwarding to FUSE; what reaches us is the
    /// bucket-relative key (`prefix + name`). NSS keys cap at 8 KiB
    /// (see `core/nss_server/configs.zig` user_max_key_size), so the
    /// only thing we guard here is a key that would overflow the NSS
    /// protocol cap.
    #[inline]
    fn check_path_max(prefix: &str, name: &str) -> Result<(), FsError> {
        if prefix.len() + name.len() > 8192 {
            return Err(FsError::NameTooLong);
        }
        Ok(())
    }

    /// POSIX: creating or removing an entry in a directory marks that
    /// directory's mtime and ctime for update (pjdfstest mkdir/00.t,
    /// unlink/00.t, etc.). We bump the parent's in-memory posix only:
    /// the immediately-following getattr reads it from the cached
    /// entry, and the parent's persisted layout is unaffected. Root
    /// has no inode entry of its own, so skip it.
    fn touch_parent_times(&self, parent: InodeId) {
        if parent == ROOT_INODE {
            return;
        }
        let now = now_ns();
        if let Some(mut entry) = self.inodes.get_mut(parent) {
            entry.posix.mtime_ns = now;
            entry.posix.ctime_ns = now;
        }
    }

    /// If `layout` is an `Indirect` hardlink redirect, fetch its
    /// `InodeRecord` and return the resolved `(real_layout, inode_id,
    /// nlink)`. For any non-redirect layout, return it unchanged with
    /// `nlink = 1` and no `inode_id`.
    async fn resolve_indirect(
        &self,
        layout: ObjectLayout,
        trace_id: &TraceId,
    ) -> Result<(ObjectLayout, Option<uuid::Uuid>, u32), FsError> {
        if let ObjectState::Indirect(redirect) = &layout.state {
            let inode_id = redirect.inode_id;
            let record = self.backend().get_inode_record(inode_id, trace_id).await?;
            Ok((record.layout, Some(inode_id), record.nlink))
        } else {
            Ok((layout, None, 1))
        }
    }

    /// Read-modify-write an `InodeRecord` under the same byte-equality CAS
    /// the record-aware flush uses, retrying on conflict. Without this,
    /// link / setattr / unlink would read-modify-write the record
    /// unconditionally and could clobber a concurrent flush that bumped the
    /// shared blob's version/size (and vice versa). Returns the committed
    /// record. `NotFound` propagates (the caller decides whether a vanished
    /// record is an error).
    async fn cas_mutate_inode_record(
        &self,
        inode_id: uuid::Uuid,
        trace_id: &TraceId,
        mut mutate: impl FnMut(&mut InodeRecord) -> Result<(), FsError>,
    ) -> Result<InodeRecord, FsError> {
        const MAX_RETRIES: u32 = 5;
        let key = InodeRecord::key_for(inode_id);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut record = self.backend().get_inode_record(inode_id, trace_id).await?;
            // Re-serialize the fetched record as the CAS guard. rkyv output is
            // deterministic for these map-free layout types, and the override
            // flush's own CAS already relies on exactly that, so this matches
            // the stored bytes without a separate raw-bytes fetch.
            let old_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&record, Vec::new())
                .map_err(FsError::from)?
                .into();
            // A fallible mutate lets the caller abort against the freshly
            // fetched record (e.g. `link` refusing to revive a record whose
            // last link is already gone) without publishing anything.
            mutate(&mut record)?;
            let new_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&record, Vec::new())
                .map_err(FsError::from)?
                .into();
            match self
                .backend()
                .put_inode_cas(&key, new_bytes, old_bytes, trace_id)
                .await
            {
                Ok(_) => return Ok(record),
                Err(FsError::CasConflict) if attempt < MAX_RETRIES => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Undo the nlink increment a `link` made when its destination publish
    /// then failed, so a failed first link can't strand a record at an
    /// inflated count (which would block its eventual reclamation). The
    /// decrement is itself a retrying CAS (`cas_mutate_inode_record`); if it
    /// still fails it is surfaced loudly; the residual case needs the same
    /// orphan-reconcile sweep as the unlink path.
    async fn compensate_link_increment(&self, inode_id: uuid::Uuid, trace_id: &TraceId) {
        if let Err(e) = self
            .cas_mutate_inode_record(inode_id, trace_id, |r| {
                r.nlink = r.nlink.saturating_sub(1);
                Ok(())
            })
            .await
        {
            tracing::warn!(
                %inode_id, error = %e,
                "link: could not compensate nlink after a failed destination \
                 publish; link count may be inflated until reconciled"
            );
        }
    }

    /// Create a hardlink `new_parent/new_name` to the file at `inode`.
    ///
    /// The first link promotes the file: its real layout is moved into a
    /// `#hardlink/<uuid>` `InodeRecord` (nlink=2) and both the original
    /// name and the new name become `Indirect(uuid)` redirects to it.
    /// A subsequent link to an already-promoted inode just bumps nlink
    /// and writes another redirect. Hardlinks to directories are EPERM
    /// (EISDIR here).
    pub async fn vfs_link(
        &self,
        inode: InodeId,
        new_parent: InodeId,
        new_name: &str,
    ) -> Result<VfsAttr, FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(new_name)?;
        self.ensure_writeback_worker_started();

        // Source key + cached inode_id (Some once already promoted).
        let (src_key, entry_type, cached_inode_id) = self
            .inodes
            .get(inode)
            .map(|e| (e.s3_key.clone(), e.entry_type, e.inode_id))
            .ok_or(FsError::NotFound)?;

        if entry_type == EntryType::Directory {
            return Err(FsError::IsDir);
        }

        let new_prefix = self.dir_prefix(new_parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&new_prefix, new_name)?;
        let new_key = format!("{}{}", new_prefix, new_name);

        let trace_id = TraceId::new();

        // Drain any pending publish for the destination name before the
        // EEXIST probe, including the queue's per-key inode records: a
        // FORGET can evict the InodeTable entry while a create intent is
        // still queued, and draining only the table's inode would let that
        // create commit after the link and clobber it. Mirrors the
        // unlink / rmdir / rename drains.
        let dst_ino = self.inodes.find_ino_by_key(&new_key, EntryType::File);
        for ino in self.writeback_drain_targets(&new_key, dst_ino) {
            self.flush_dirty_handles_for_inode(ino).await?;
            self.drain_inode_to_barrier(ino).await?;
        }

        // EEXIST if the destination name already exists. This also
        // subsumes the `link(a, a)` case (the source name is live, so
        // get_inode returns it), without a separate `new_key ==
        // src_key` guard, which would misfire for a promoted inode whose
        // cached `s3_key` is a since-unlinked alias (link/02.t,
        // link/03.t re-link a freed long name).
        match self.backend().get_inode(&new_key, &trace_id).await {
            Ok(_) => return Err(FsError::AlreadyExists),
            Err(FsError::NotFound) => {}
            Err(e) => return Err(e),
        }

        // Drain a pending publish for the source so we promote against
        // the post-flush layout, not a stale placeholder.
        self.flush_dirty_handles_for_inode(inode).await?;
        self.drain_inode_to_barrier(inode).await?;

        let now = now_ns();

        // POSIX: link(2) bumps the file's ctime. Stamp it into the record's
        // layout so a later lookup repopulating posix from the record sees
        // it (the in-memory mutation alone would be lost).
        let bump_link = |r: &mut InodeRecord| -> Result<(), FsError> {
            // Refuse to revive a record whose last link is already gone
            // (nlink == 0, awaiting reclaim by a concurrent unlink). Once
            // nlink hits 0 it stays there, so the unlink's post-commit
            // reclaim is safe: a racing link either commits its bump before
            // the decrement (the decrement then observes nlink > 0 and skips
            // reclaim) or observes nlink == 0 here and fails with ENOENT.
            if r.nlink == 0 {
                return Err(FsError::NotFound);
            }
            r.nlink = r.nlink.saturating_add(1);
            let mut p = crate::inode::layout_posix(&r.layout);
            p.ctime_ns = now;
            r.layout = crate::inode::layout_with_posix(r.layout.clone(), p);
            Ok(())
        };

        // The Indirect redirect bytes written at a promoted name (non-state
        // fields are placeholders; the record is authoritative).
        let make_redirect_bytes = |id: uuid::Uuid| -> Result<Bytes, FsError> {
            let l = ObjectLayout {
                timestamp: now / 1_000_000,
                version_id: ObjectLayout::gen_version_id(),
                block_size: DEFAULT_BLOCK_SIZE,
                blob_version: 0,
                data_write_token: 0,
                next_data_write_token: 0,
                pending_data_write: None,
                state: ObjectState::Indirect(IndirectEntry { inode_id: id }),
            };
            let b: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&l, Vec::new())
                .map_err(FsError::from)?
                .into();
            Ok(b)
        };

        // Resolve to a shared inode_id, joining or creating the record.
        //   - cached inode_id: already promoted; bump nlink under CAS.
        //   - src layout Indirect: promoted, cache cold; follow + bump.
        //   - fresh normal source: promote ATOMICALLY: mint a record then
        //     CAS the source's NSS row from its exact normal bytes to an
        //     Indirect redirect. If that CAS loses (another client promoted
        //     first), discard our orphan record, re-read the now-Indirect
        //     redirect, and join the winner's record via the bump path, so
        //     concurrent first links converge on one record instead of each
        //     minting a divergent one and clobbering the source redirect.
        let (inode_id, record) = if let Some(inode_id) = cached_inode_id {
            let record = self
                .cas_mutate_inode_record(inode_id, &trace_id, bump_link)
                .await?;
            (inode_id, record)
        } else {
            // Promote a fresh source. A source-promotion CAS conflict does
            // NOT necessarily mean another linker won: a concurrent ordinary
            // write/chmod can also rewrite a still-normal source. So loop
            // (bounded): re-read the source each time and either join a
            // winner's record (now Indirect) or re-promote from the fresh
            // normal bytes (still Normal). One minted record id is reused
            // across attempts and dropped if we end up joining.
            let new_id = uuid::Uuid::new_v4();
            let mut record_created = false;
            const MAX_PROMOTE_RETRIES: u32 = 5;
            let mut attempt = 0;
            loop {
                attempt += 1;
                let src_layout = self.backend().get_inode(&src_key, &trace_id).await?;
                match &src_layout.state {
                    ObjectState::Indirect(redirect) => {
                        let id = redirect.inode_id;
                        if id == new_id {
                            // An earlier ambiguous CAS (e.g. a timeout) had
                            // actually installed our redirect. Recover it as a
                            // successful promotion rather than deleting new_id
                            // and dangling the source.
                            let record = self.backend().get_inode_record(new_id, &trace_id).await?;
                            break (new_id, record);
                        }
                        // Another linker won; the source points elsewhere, so
                        // our CAS never landed; drop our orphan and join theirs.
                        if record_created {
                            let _ = self.backend().delete_inode_record(new_id, &trace_id).await;
                        }
                        let record = self
                            .cas_mutate_inode_record(id, &trace_id, bump_link)
                            .await?;
                        break (id, record);
                    }
                    ObjectState::Directory(_) | ObjectState::Mpu(MpuState::Uploading) => {
                        // Source is not Indirect -> our CAS never landed.
                        if record_created {
                            let _ = self.backend().delete_inode_record(new_id, &trace_id).await;
                        }
                        return Err(FsError::IsDir);
                    }
                    ObjectState::Normal(_)
                    | ObjectState::Mpu(MpuState::Completed(_))
                    | ObjectState::Symlink(_)
                    | ObjectState::Special(_) => {
                        if attempt > MAX_PROMOTE_RETRIES {
                            // Still normal after all retries -> our CAS never
                            // landed -> new_id is a true orphan.
                            if record_created {
                                let _ = self.backend().delete_inode_record(new_id, &trace_id).await;
                            }
                            return Err(FsError::CasConflict);
                        }
                        let record = InodeRecord {
                            layout: crate::inode::layout_with_posix(src_layout.clone(), {
                                let mut p = crate::inode::layout_posix(&src_layout);
                                p.ctime_ns = now;
                                p
                            }),
                            nlink: 2,
                            orphan_since: None,
                        };
                        // (Re)seed the record from the current bytes, then
                        // flip the source row guarded on those exact bytes
                        // (the current normal layout re-serialized). On ANY CAS
                        // failure, conflict OR ambiguous (timeout), do NOT
                        // delete here: loop and re-read. The next iteration
                        // recovers (Indirect == new_id), joins (Indirect !=
                        // new_id), or re-promotes (still Normal).
                        let src_bytes: Bytes =
                            to_bytes_in::<_, rkyv::rancor::Error>(&src_layout, Vec::new())
                                .map_err(FsError::from)?
                                .into();
                        self.backend()
                            .put_inode_record(new_id, &record, &trace_id)
                            .await?;
                        record_created = true;
                        if self
                            .backend()
                            .put_inode_cas(
                                &src_key,
                                make_redirect_bytes(new_id)?,
                                src_bytes,
                                &trace_id,
                            )
                            .await
                            .is_ok()
                        {
                            break (new_id, record);
                        }
                    }
                }
            }
        };

        // Persist the source's resolved hardlink identity NOW, before the
        // destination write. If the destination absence-CAS below fails
        // (EEXIST), the source must not be left cached as a normal layout
        // with inode_id == None; a later setattr would then take the
        // non-hardlink path and publish that stale layout over the source's
        // Indirect redirect.
        if let Some(mut e) = self.inodes.get_mut(inode) {
            e.layout = Some(record.layout.clone());
            e.posix = crate::inode::layout_posix(&record.layout);
            e.inode_id = Some(inode_id);
            e.cache_expiry = std::time::Instant::now();
        }

        // Create the destination redirect with an absence CAS (empty
        // expected_old requires the key to be absent). Two concurrent links
        // to the same new name, or different sources racing the same name,
        // can both pass the earlier existence check; the absence CAS lets
        // only one win.
        //
        // Reconcile the outcome carefully so a failed publish never strands
        // the record at an inflated nlink, and, more importantly, never
        // *under*-counts a live destination (which would let a later source
        // unlink drive nlink to 0 and reclaim a still-referenced record):
        //   - Ok: our redirect landed -> success.
        //   - CasConflict: the name is taken -> EEXIST + compensate.
        //   - other (ambiguous, e.g. timeout): re-read the name's exact
        //     bytes. Only if they equal the exact redirect WE wrote did our
        //     publish land (matching inode_id alone is insufficient: two
        //     concurrent links to the same destination share it); then it is
        //     success. If the name holds other bytes -> EEXIST + compensate.
        //     If it is confirmed absent -> the publish did not land ->
        //     surface the error + compensate. If the re-read itself fails we
        //     CANNOT confirm absence, so we do NOT compensate (an inflated
        //     count merely leaks; under-counting a live link loses data).
        let dst_redirect = make_redirect_bytes(inode_id)?;
        match self
            .backend()
            .put_inode_cas(&new_key, dst_redirect.clone(), Bytes::new(), &trace_id)
            .await
        {
            Ok(_) => {}
            Err(FsError::CasConflict) => {
                self.compensate_link_increment(inode_id, &trace_id).await;
                return Err(FsError::AlreadyExists);
            }
            Err(e) => match self.backend().get_inode(&new_key, &trace_id).await {
                Ok(l) => {
                    // Re-serialize and compare to the exact redirect we wrote:
                    // equal iff our (ambiguous) CAS actually landed. rkyv is
                    // deterministic for these types.
                    let raw: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&l, Vec::new())
                        .map_err(FsError::from)?
                        .into();
                    if raw != dst_redirect {
                        // Name occupied by something we did not write.
                        self.compensate_link_increment(inode_id, &trace_id).await;
                        return Err(FsError::AlreadyExists);
                    }
                    // Our redirect is present -> the ambiguous CAS landed ->
                    // success (fall through).
                }
                Err(FsError::NotFound) => {
                    // Confirmed absent -> publish did not land.
                    self.compensate_link_increment(inode_id, &trace_id).await;
                    return Err(e);
                }
                Err(_reread_err) => {
                    // Indeterminate: the publish may have committed. Leave
                    // nlink as-is rather than risk under-counting a live link.
                    return Err(e);
                }
            },
        }

        // Map the new name to the inode and refresh dir caches/times.
        self.inodes.add_alias(&new_key, EntryType::File, inode);

        self.cache_dir_entry(&new_prefix, new_name, inode, DirEntryKind::RegularFile);
        self.touch_parent_times(new_parent);

        let mut attr = self.make_file_attr(inode, &record.layout)?;
        attr.nlink = record.nlink;
        Ok(attr)
    }

    pub async fn vfs_lookup(&self, parent: InodeId, name: &str) -> Result<VfsAttr, FsError> {
        Self::check_name_max(name)?;
        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;

        let full_key = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}{}", prefix, name)
        };
        let dir_key = format!("{}/", full_key);

        // Directory membership survives FUSE_FORGET. Local mutations
        // invalidate this snapshot, and its TTL bounds peer changes.
        if let Some(false) = self.dir_cache.contains_name(&prefix, name) {
            return Err(FsError::NotFound);
        }

        let trace_id = TraceId::new();

        // Read-your-writes before the NSS probe only when local writeback
        // proves this name has an in-flight publish. This closes the race
        // where NSS returns NotFound and the worker commits before the later
        // fallback check, without serving arbitrary stale cached entries.
        if let Some(ino) = self.inodes.find_ino_by_key(&full_key, EntryType::File)
            && let Some(entry) = self.inodes.get(ino)
            && !entry.name_removed
            && (self.writeback.has_pending_intent_for_key(&full_key)
                || self.writeback.fsync_barrier(ino).is_some())
        {
            let layout = entry.layout.clone();
            drop(entry);
            // Decide what to serve BEFORE taking a refcount, so a layout we
            // cannot resolve locally falls through to the NSS resolve path
            // instead of leaking the kernel-lookup count on an error reply.
            let ryw_attr = if let Some(size) = self.dirty_write_buffer_size(ino) {
                // Fresh create whose first flush hasn't landed, or an
                // in-flight overwrite: the live write buffer is the
                // authoritative local size. A cached layout (if any) still
                // holds the stale pre-flush committed size, so prefer the
                // buffer. This also keeps the async close-flush window from
                // caching a negative dentry for a file that exists.
                Some(self.make_new_file_attr(ino, size))
            } else {
                match &layout {
                    // An Indirect hardlink redirect cached by a plain readdir
                    // has no servable size() (InvalidState). The alias already
                    // exists in NSS (link publishes it synchronously), so
                    // there is no negative-dentry race: fall through to the
                    // NSS resolve path, which follows the redirect correctly.
                    Some(l) if matches!(l.state, ObjectState::Indirect(_)) => None,
                    Some(l) => Some(self.make_file_attr(ino, l)?),
                    None => Some(self.make_new_file_attr(ino, 0)),
                }
            };
            if let Some(attr) = ryw_attr {
                // This LOOKUP reply resolves the inode without going through
                // `lookup_or_insert`, so bump the kernel-lookup refcount here
                // or a later FORGET under-counts and evicts a live inode.
                if let Some(e) = self.inodes.get(ino) {
                    e.increment_ref();
                }
                return Ok(attr);
            }
        }
        if let Some(ino) = self.inodes.find_ino_by_key(&dir_key, EntryType::Directory)
            && let Some(entry) = self.inodes.get(ino)
            && !entry.name_removed
        {
            let has_pending = self.writeback.has_pending_intent_for_key(&dir_key);
            let is_tainted = self.writeback.is_tainted(ino);
            if has_pending || is_tainted {
                drop(entry);
                if is_tainted {
                    self.drain_inode_to_barrier(ino).await?;
                }
                let entry = self.inodes.get(ino).ok_or(FsError::NotFound)?;
                if entry.name_removed {
                    return Err(FsError::NotFound);
                }
                entry.increment_ref();
                drop(entry);
                return Ok(self.make_dir_attr(ino));
            }
        }

        // Try as file first. Use is_fs_visible (not is_listable) so
        // special files (fifo / device / socket), which the S3 listing
        // API hides, are still resolvable through FUSE lookup.
        match self.backend().get_inode(&full_key, &trace_id).await {
            Ok(layout) => {
                if !layout.is_fs_visible() {
                    return Err(FsError::NotFound);
                }
                // Follow an Indirect hardlink redirect to its real
                // layout + nlink, caching the inode_id on the entry.
                let (real_layout, inode_id, nlink) =
                    self.resolve_indirect(layout, &trace_id).await?;
                let (ino, _) = self.inodes.lookup_or_insert(
                    &full_key,
                    EntryType::File,
                    Some(real_layout.clone()),
                );
                if let Some(mut e) = self.inodes.get_mut(ino) {
                    // Cross-instance coherency: lookup_or_insert leaves an
                    // EXISTING entry's layout untouched, so a peer instance's
                    // override (new blob_version + size) would otherwise stay
                    // masked behind our stale cached layout; getattr reads
                    // size from entry.layout, so a follow-up stat (after the
                    // 1s lookup-attr TTL) would report the old size even
                    // though this lookup already fetched the fresh one from
                    // NSS. Refresh the cached layout to the just-read
                    // authoritative one. (Local unflushed writes live in the
                    // handle's write_buf, and unflushed setattr in
                    // entry.posix, so neither is clobbered here.)
                    e.layout = Some(real_layout.clone());
                    if let Some(id) = inode_id {
                        // Hardlink: also refresh the cached posix from the
                        // shared record so a chmod/chown/unlink-ctime-bump
                        // made via another name isn't masked by stale posix
                        // (make_file_attr reads posix for mode/times).
                        e.inode_id = Some(id);
                        e.posix = crate::inode::layout_posix(&real_layout);
                    }
                }
                let mut attr = self.make_file_attr(ino, &real_layout)?;
                attr.nlink = nlink;
                // Size authority: the NSS layout size is a lazy copy that can
                // lag a peer instance's most recent override, so the dentry
                // attr this LOOKUP installs (and the i_size the kernel derives
                // from it) would otherwise be stale; a follow-up read clamps
                // to the old size. Override with the authoritative geometry
                // sentinel so cross-instance stat/read see the latest EOF.
                let auth_size = self.authoritative_file_size(&real_layout).await?;
                if auth_size != attr.size {
                    attr.size = auth_size;
                    attr.blocks = auth_size.div_ceil(512);
                }
                return Ok(attr);
            }
            Err(FsError::NotFound) => {}
            Err(e) => return Err(e),
        }

        // Try as directory. Read the directory's own layout when a
        // marker is present so its persisted posix (mode/uid/gid/times)
        // seeds the inode entry; a Directory layout carries posix, a
        // legacy Normal marker does not (defaults apply).
        match self.backend().get_inode(&dir_key, &trace_id).await {
            Ok(layout) => {
                let seed = if layout.is_directory() {
                    Some(layout)
                } else {
                    None
                };
                let (ino, _) = self
                    .inodes
                    .lookup_or_insert(&dir_key, EntryType::Directory, seed);
                return Ok(self.make_dir_attr(ino));
            }
            Err(FsError::NotFound) => {}
            Err(e) => return Err(e),
        }

        // Read-your-writes: a just-created entry NSS doesn't have yet must
        // still resolve from the in-memory inode, but only when there's a
        // genuine in-flight reason it's missing from NSS, NOT for any stale
        // cached entry. Otherwise an entry deleted by another instance (NSS
        // says gone, but our cache still holds it because it was never
        // FUSE-unlinked here) would be resurrected and a follow-up read
        // would EIO on the deleted blocks instead of returning ENOENT.
        //
        // "In-flight" means either a pending writeback intent (async
        // metadata create/chmod/mkdir/symlink/mknod not yet drained) or an
        // open file handle (a regular-file create whose close-time flush
        // hasn't published to NSS yet). When neither holds, NSS's miss is
        // authoritative.
        if let Some(ino) = self.inodes.find_ino_by_key(&full_key, EntryType::File)
            && let Some(entry) = self.inodes.get(ino)
            && !entry.name_removed
            && (self.writeback.has_pending_intent_for_key(&full_key)
                || self.has_open_handles_for_inode(ino, None)
                // A tainted inode had its publish fail: NSS has nothing,
                // but the name must stay resolvable so the deferred EIO
                // is reachable through the next open instead of the file
                // silently vanishing as ENOENT.
                || self.writeback.is_tainted(ino))
        {
            let layout = entry.layout.clone();
            entry.increment_ref();
            drop(entry);
            return match layout {
                Some(layout) => self.make_file_attr(ino, &layout),
                None => Ok(self.make_new_file_attr(ino, self.dirty_buffer_size(ino))),
            };
        }
        if let Some(ino) = self.inodes.find_ino_by_key(&dir_key, EntryType::Directory)
            && let Some(entry) = self.inodes.get(ino)
            && !entry.name_removed
        {
            let has_pending = self.writeback.has_pending_intent_for_key(&dir_key);
            let is_tainted = self.writeback.is_tainted(ino);
            if has_pending || is_tainted {
                drop(entry);
                if is_tainted {
                    self.drain_inode_to_barrier(ino).await?;
                }
                let entry = self.inodes.get(ino).ok_or(FsError::NotFound)?;
                if entry.name_removed {
                    return Err(FsError::NotFound);
                }
                entry.increment_ref();
                drop(entry);
                return Ok(self.make_dir_attr(ino));
            }
        }

        // Fall back to a prefix listing for implicit directories that
        // have children but no marker inode of their own.
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

    pub fn vfs_forget(&self, inode: InodeId, nlookup: u64) {
        // Pin the entry while an open handle (async release flush) or
        // queued writeback state still needs it: a flush whose entry
        // vanished mid-flight would publish default posix (mode 0,
        // uid 0), and a queued intent would lose its read-your-writes
        // anchor and its delete-drain identity. The pin is reaped once
        // the flush / worker drains (`reap_forgotten_inode`).
        let pin =
            self.has_open_handles_for_inode(inode, None) || self.writeback.has_live_state(inode);
        match self.inodes.forget(inode, nlookup, pin) {
            ForgetOutcome::Removed => self.writeback.prune_inode_if_idle(inode),
            ForgetOutcome::KeptZeroed => self.writeback.mark_forgotten(inode),
            ForgetOutcome::Live => {}
        }
        // Sweep entries whose pin drained after their FORGET.
        for ino in self.writeback.take_reapable() {
            self.reap_forgotten_inode(ino);
        }
    }

    /// Finish a FORGET that was deferred because writeback state or an
    /// open handle pinned the entry. No-op when the kernel still
    /// references the inode or a lookup revived it; re-queued when the
    /// pin is still live.
    fn reap_forgotten_inode(&self, ino: InodeId) {
        if !self.inodes.is_unreferenced(ino) {
            return;
        }
        if self.has_open_handles_for_inode(ino, None) || self.writeback.has_live_state(ino) {
            self.writeback.mark_forgotten(ino);
            return;
        }
        if self.inodes.remove_if_unreferenced(ino) {
            self.writeback.prune_inode_if_idle(ino);
        }
    }

    pub async fn vfs_getattr(
        &self,
        inode: InodeId,
        fh: Option<FileHandleId>,
    ) -> Result<VfsAttr, FsError> {
        if inode == ROOT_INODE {
            return Ok(self.make_dir_attr(ROOT_INODE));
        }

        // If there's an open write handle with a dirty buffer, report its size
        if let Some(fh_id) = fh
            && let Some(handle) = self.file_handles.get(&fh_id)
            && let Some(ref wb) = handle.write_buf
            && wb.dirty
        {
            return Ok(self.make_new_file_attr(inode, wb.file_size));
        }

        // A directory materialised from a delimiter listing carries only
        // placeholder posix (uid 0 / mode 0); fetch its marker so stat and
        // the setattr owner check see the real owner. No-op for files or an
        // already-authoritative entry.
        self.refresh_dir_posix_if_unknown(inode).await;

        let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;

        match entry.entry_type {
            EntryType::Directory => Ok(self.make_dir_attr(inode)),
            EntryType::File => {
                let inode_id = entry.inode_id;
                let name_removed = entry.name_removed;
                if let Some(ref layout) = entry.layout {
                    let layout = layout.clone();
                    drop(entry);
                    if let Some(id) = inode_id {
                        // Hardlink: the authoritative layout (mode / uid /
                        // gid / times) AND nlink live in the shared
                        // record, and may have changed via another name
                        // (chmod/chown/unlink-ctime-bump). Refetch the
                        // record rather than trusting the cached layout
                        // (unlink/00.t ctime checks, link/00.t chmod).
                        // `make_file_attr` reads times/mode from
                        // `entry.posix`, so refresh that from the record
                        // BEFORE building the attr; a stale posix would
                        // otherwise mask the just-bumped ctime.
                        let trace_id = TraceId::new();
                        if let Ok(record) = self.backend().get_inode_record(id, &trace_id).await {
                            if let Some(mut e) = self.inodes.get_mut(inode) {
                                e.posix = crate::inode::layout_posix(&record.layout);
                                e.layout = Some(record.layout.clone());
                            }
                            let mut attr = self.make_file_attr(inode, &record.layout)?;
                            attr.nlink = record.nlink;
                            return Ok(attr);
                        }
                    }
                    let mut attr = self.make_file_attr(inode, &layout)?;
                    // Cross-instance size authority: this entry's cached layout
                    // (size + blob_version) may lag a peer instance's most
                    // recent overwrite, so make_file_attr's size can be stale.
                    // Re-read the authoritative geometry sentinel from BSS via a
                    // max-version quorum read, which reflects the latest
                    // published override regardless of our cached layout
                    // version. Skips symlinks/special files (they report their
                    // own size and have no data blob). getattr is gated by the
                    // 1s FUSE attr TTL, so this BSS read happens at most about
                    // once/sec/inode: a bounded, throttled extra read.
                    if !layout.is_symlink()
                        && layout.special().is_none()
                        && let Ok(geom_guid) = layout.blob_guid()
                    {
                        let trace_id = TraceId::new();
                        match self.backend().get_blob_info(geom_guid, &trace_id).await {
                            // Only let the sentinel move size FORWARD: apply it
                            // when it is at least as new as our cached layout
                            // (vfs_lookup refreshes the cached layout from NSS,
                            // so a stale sentinel must never downgrade a fresh
                            // size back to an older value).
                            Ok(Some(info)) if info.blob_version >= layout.blob_version => {
                                attr.size = info.total_size;
                                // make_file_attr derives st_blocks from size
                                // (512-byte units) for regular files; keep it
                                // consistent with the refreshed size.
                                attr.blocks = info.total_size.div_ceil(512);
                            }
                            // Sentinel older than our cached layout: keep the
                            // (fresher) cached-layout size.
                            Ok(Some(_)) => {}
                            // No sentinel yet: keep the cached-layout size.
                            Ok(None) => {}
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "getattr get_blob_info failed; using cached size"
                                );
                            }
                        }
                    }
                    if name_removed {
                        // POSIX: an open-but-unlinked file with no
                        // remaining links reports nlink=0 (unlink/14.t).
                        attr.nlink = 0;
                    }
                    Ok(attr)
                } else {
                    let key = entry.s3_key.clone();
                    drop(entry);
                    let trace_id = TraceId::new();
                    match self.backend().get_inode(&key, &trace_id).await {
                        Ok(layout) => {
                            let (real_layout, resolved_id, nlink) =
                                self.resolve_indirect(layout, &trace_id).await?;
                            let mut attr = self.make_file_attr(inode, &real_layout)?;
                            attr.nlink = nlink;
                            if let Some(mut entry) = self.inodes.get_mut(inode) {
                                entry.layout = Some(real_layout);
                                if let Some(id) = resolved_id {
                                    entry.inode_id = Some(id);
                                }
                            }
                            Ok(attr)
                        }
                        // A freshly created file that hasn't flushed to NSS
                        // yet has no committed layout, so it isn't resolvable
                        // by key. It still exists in memory behind an open
                        // write handle; synthesize its attr from the cached
                        // posix + the largest open write-buffer size. Without
                        // this, an fd-based stat/utimes before the first flush
                        // (tar -x does openat(O_CREAT) then futimens(fd)
                        // before close, and the kernel may not forward the fh
                        // on SETATTR) would wrongly return ENOENT.
                        Err(FsError::NotFound) if self.has_open_handles_for_inode(inode, None) => {
                            let size = self
                                .file_handles
                                .iter()
                                .filter(|e| e.value().ino == inode)
                                .filter_map(|e| e.value().write_buf.as_ref().map(|wb| wb.file_size))
                                .max()
                                .unwrap_or(0);
                            Ok(self.make_new_file_attr(inode, size))
                        }
                        Err(e) => Err(e),
                    }
                }
            }
        }
    }

    /// In-memory-only attributes: like `vfs_getattr` but never touches
    /// the backend. Serves uid/gid/mode/times from the inode entry's
    /// `posix` and size/type from the cached `layout` (which the flush
    /// keeps current under the single-writer lock). Used on the setattr
    /// path (both the permission precheck and the post-mutation reply),
    /// so a `chmod`/`chown`/`utimensat` does not pay the two
    /// cross-instance coherency round-trips `vfs_getattr` makes
    /// (`get_inode` on a cold layout, `get_blob_info` size sentinel).
    /// This is the dominant per-file cost on create-heavy workloads
    /// (tar -xf issues one `utimensat` per file). Cross-instance
    /// freshness is still provided by the 1s FUSE attr TTL, after which
    /// the kernel re-issues a full `getattr`.
    ///
    /// True if the inode is a promoted hardlink (its `nlink` and shared
    /// posix live in the NSS `InodeRecord`, not the in-memory entry). The
    /// in-memory attr fast path below can't see that nlink, so a caller
    /// that replies an attr to the kernel must resolve the record for
    /// these (otherwise it clobbers the kernel's cached link count to 1).
    pub fn is_hardlink(&self, inode: InodeId) -> bool {
        self.inodes
            .get(inode)
            .map(|e| e.inode_id.is_some())
            .unwrap_or(false)
    }

    pub fn is_dir(&self, inode: InodeId) -> bool {
        self.inodes
            .get(inode)
            .map(|e| e.entry_type == EntryType::Directory)
            .unwrap_or(false)
    }

    /// Seed authoritative posix into a directory entry whose owner/mode is
    /// still a listing-materialised placeholder (`posix_known == false`),
    /// by reading its NSS marker. No-op for files, the root, an entry with
    /// known posix, or a marker that has no directory layout (a legacy
    /// Normal marker / implicit directory keeps its default). Guarded on
    /// `!posix_known` again after the fetch so a concurrent local posix
    /// mutation is never clobbered.
    async fn refresh_dir_posix_if_unknown(&self, inode: InodeId) {
        let dir_key = match self.inodes.get(inode) {
            Some(e) if e.entry_type == EntryType::Directory && !e.posix_known => e.s3_key.clone(),
            _ => return,
        };
        let trace_id = TraceId::new();
        if let Ok(layout) = self.backend().get_inode(&dir_key, &trace_id).await
            && layout.is_directory()
            && let Some(mut e) = self.inodes.get_mut(inode)
            && !e.posix_known
        {
            e.posix = crate::inode::layout_posix(&layout);
            e.posix_known = true;
        }
    }

    pub fn vfs_getattr_inmem(
        &self,
        inode: InodeId,
        fh: Option<FileHandleId>,
    ) -> Result<VfsAttr, FsError> {
        if inode == ROOT_INODE {
            return Ok(self.make_dir_attr(ROOT_INODE));
        }
        // An open write handle's dirty buffer is the authoritative size.
        if let Some(fh_id) = fh
            && let Some(handle) = self.file_handles.get(&fh_id)
            && let Some(ref wb) = handle.write_buf
            && wb.dirty
        {
            return Ok(self.make_new_file_attr(inode, wb.file_size));
        }
        let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
        match entry.entry_type {
            EntryType::Directory => Ok(self.make_dir_attr(inode)),
            EntryType::File => match entry.layout.as_ref() {
                // `make_file_attr` preserves size + S_IFMT (symlink /
                // device) from the layout and reads mode/uid/gid/times
                // from `entry.posix`, all in-memory, no round-trip.
                Some(layout) => {
                    let layout = layout.clone();
                    drop(entry);
                    self.make_file_attr(inode, &layout)
                }
                // No cached layout yet (a brand-new file whose flush has
                // not landed): report a zero-size regular file from the
                // in-memory posix. setattr changes mode/owner/times (all
                // in posix), not size, so this is correct for the reply;
                // the TTL-bounded next getattr fills in the real size.
                None => {
                    drop(entry);
                    Ok(self.make_new_file_attr(inode, 0))
                }
            },
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

    /// Decide whether `FUSE_RELEASE` should flush this handle off the
    /// FUSE worker thread. Returns `Some(inode)` only when the mount is in
    /// `Default` writeback mode AND the handle has a dirty write buffer,
    /// i.e. an async close-flush is both safe and worthwhile. `None` means
    /// "release inline": a read-only handle, a clean buffer, or `Strict`
    /// mode where close must publish synchronously.
    pub fn peek_release_state(&self, fh: FileHandleId) -> Option<InodeId> {
        if self.writeback_mode != WritebackMode::Default {
            return None;
        }
        let handle = self.file_handles.get(&fh)?;
        let wb = handle.write_buf.as_ref()?;
        if !wb.dirty {
            return None;
        }
        Some(handle.ino)
    }

    /// Flush + release a dirty write handle asynchronously, off the FUSE
    /// worker thread. Registers a writeback cycle (so `fsync` / unlink /
    /// open barriers can wait for the in-flight close), then spawns the
    /// synchronous `vfs_release` (which runs `flush_write_buffer` and
    /// drops the inode write lock) and collapses the cycle to `Done`
    /// when the publish lands. Returns immediately; the FUSE_RELEASE
    /// reply does not wait on the publish. A failed flush taints the
    /// inode via `record_failure`, surfacing deferred EIO on the next
    /// fsync / open of the same path (POSIX close gives no durability
    /// guarantee). Only invoked for dirty handles in Default mode (see
    /// `peek_release_state`); single-writer-per-inode keeps at most one
    /// async close-flush in flight per inode.
    pub fn spawn_release_flush(self: Arc<Self>, fh: FileHandleId, ino: InodeId) {
        let generation = self.writeback.open_next_cycle(ino);
        compio_runtime::spawn(async move {
            // Ensure the cycle always collapses to Done even if this task
            // is dropped mid-flush (ring runtime torn down at unmount);
            // otherwise destroy's drain barrier hangs on the orphaned cycle.
            let mut cycle_guard = ReleaseCycleGuard {
                writeback: Arc::clone(&self.writeback),
                ino,
                generation,
                armed: true,
            };
            // Order behind lower-generation cycles (queued metadata
            // intents, an earlier in-flight flush) so this CAS publish
            // and the worker's puts land in generation order. A timeout
            // proceeds anyway: the flush's CAS and the worker's
            // SetPosix CAS both tolerate reordering; durability of the
            // buffered data comes first.
            if let Err(e) = self
                .wait_cycles_drained(ino, Generation(generation.0 - 1))
                .await
            {
                tracing::warn!(fh = fh.0, ino = ino.0, error = %e, "release flush: pre-drain timed out");
            }
            // Retry transient flush errors before the deferred-taint
            // path: for a brand-new file a failed FIRST publish leaves
            // nothing in NSS, so every retry here is one less silently
            // lost file. `flush_write_buffer` re-arms its snapshot on
            // failure, so a retry republishes the same data.
            for attempt in 1..3u32 {
                match self.flush_write_buffer(fh).await {
                    Ok(()) => break,
                    Err(e) => {
                        tracing::warn!(fh = fh.0, ino = ino.0, attempt, error = %e, "release flush retrying");
                        compio_runtime::time::sleep(Duration::from_millis(20 * attempt as u64))
                            .await;
                    }
                }
            }
            // vfs_release flushes any still-dirty buffer itself, so its
            // result is authoritative for the publish outcome. Disarm the
            // guard and advance the cycle explicitly on both normal paths.
            match self.vfs_release(fh).await {
                Ok(()) => {
                    cycle_guard.armed = false;
                    self.writeback.advance_to_done(ino, generation);
                }
                Err(e) => {
                    tracing::warn!(
                        fh = fh.0,
                        ino = ino.0,
                        generation = generation.0,
                        error = %e,
                        "async release flush failed; tainting inode"
                    );
                    self.writeback.record_failure(ino);
                    cycle_guard.armed = false;
                    self.writeback.advance_to_done(ino, generation);
                }
            }
            // Finish a FORGET that raced this flush (the open handle
            // pinned the entry so the publish above kept its posix).
            self.reap_forgotten_inode(ino);
        })
        .detach();
    }

    /// Persist a freshly-built inode layout at `key`. Metadata
    /// publishes (symlink / special-file create, chmod / chown /
    /// utimensat, directory create) honour the writeback mode: `Strict`
    /// writes through synchronously; `Default` enqueues a `PutInode`
    /// intent so the worker commits it asynchronously, which is what
    /// makes the metadata cache a cache. Correctness against a
    /// follow-up unlink / lookup is provided by `drain_inode_to_barrier`
    /// on unlink/rmdir (so a delete can't race a not-yet-drained
    /// publish) and by `vfs_lookup`'s in-memory read-your-writes
    /// fallback (so a pending create is still visible), not by
    /// forcing every publish through NSS. `rmdir` additionally checks
    /// the queue for pending child creates before trusting the NSS
    /// emptiness probe.
    async fn publish_inode_layout(
        &self,
        ino: InodeId,
        key: &str,
        parent_key: &str,
        name: &str,
        layout_bytes: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        match self.writeback_mode {
            WritebackMode::Strict => {
                // Guard on absence (empty expected bytes): a brand-new
                // create must not blind-overwrite a peer that won the
                // name between the caller's absence check and this
                // publish. A lost race is reported as EEXIST, mirroring
                // the hardlink publish path. Idempotent under an
                // internally-retried RPC (lost reply after commit).
                match put_inode_create_idempotent(self.backend(), key, layout_bytes, trace_id).await
                {
                    Ok(()) => {}
                    Err(FsError::CasConflict) => return Err(FsError::AlreadyExists),
                    Err(e) => return Err(e),
                }
            }
            WritebackMode::Default => {
                self.ensure_writeback_worker_started();
                let generation = self.writeback.open_next_cycle(ino);
                let outcome = self.writeback.upsert_inode_intent(
                    key.to_string(),
                    ino,
                    generation,
                    WbInodeOp::PutInode {
                        parent_key: parent_key.to_string(),
                        name: name.to_string(),
                        layout_bytes: layout_bytes.clone(),
                    },
                );
                if outcome == CoalesceOutcome::Blocked {
                    // Unmount drain in progress: publish synchronously
                    // so the metadata isn't dropped on the floor. Guard
                    // on absence (empty expected bytes) for the same
                    // reason the async worker does: a brand-new create
                    // must not clobber a peer that won the name.
                    match put_inode_create_idempotent(self.backend(), key, layout_bytes, trace_id)
                        .await
                    {
                        Ok(()) => self.writeback.advance_to_done(ino, generation),
                        // A peer won the name. The error is delivered
                        // synchronously as EEXIST, so resolve the cycle
                        // cleanly instead of tainting (there is no
                        // deferred error to surface later).
                        Err(FsError::CasConflict) => {
                            self.writeback.advance_to_done(ino, generation);
                            return Err(FsError::AlreadyExists);
                        }
                        Err(e) => {
                            self.writeback.mark_failed(key, generation, ino);
                            return Err(e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Persist a posix-only update (chmod / chown / utimensat) at `key`.
    /// `Strict` writes through synchronously. `Default` enqueues a
    /// `SetPosix` intent the worker applies via CAS (guarding on the
    /// layout snapshot taken here, re-fetching and re-folding on
    /// conflict), so this metadata publish can never blind-put a stale
    /// data layout over a concurrent flush's CAS publish.
    async fn publish_posix_update(
        &self,
        ino: InodeId,
        key: &str,
        posix: PosixAttrs,
        expected_layout_bytes: Bytes,
        layout_bytes: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        match self.writeback_mode {
            WritebackMode::Strict => {
                publish_set_posix(
                    self.backend(),
                    key,
                    &posix,
                    &expected_layout_bytes,
                    &layout_bytes,
                    trace_id,
                )
                .await?;
            }
            WritebackMode::Default => {
                self.ensure_writeback_worker_started();
                let generation = self.writeback.open_next_cycle(ino);
                let outcome = self.writeback.upsert_inode_intent(
                    key.to_string(),
                    ino,
                    generation,
                    WbInodeOp::SetPosix {
                        posix,
                        expected_layout_bytes: expected_layout_bytes.clone(),
                        layout_bytes: layout_bytes.clone(),
                    },
                );
                if outcome == CoalesceOutcome::Blocked {
                    match publish_set_posix(
                        self.backend(),
                        key,
                        &posix,
                        &expected_layout_bytes,
                        &layout_bytes,
                        trace_id,
                    )
                    .await
                    {
                        Ok(_) => self.writeback.advance_to_done(ino, generation),
                        Err(e) => {
                            self.writeback.mark_failed(key, generation, ino);
                            return Err(e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn publish_posix_catchup_after_flush(
        &self,
        ino: InodeId,
        key: &str,
        layout: &ObjectLayout,
        trace_id: &TraceId,
    ) -> Result<Option<ObjectLayout>, FsError> {
        let Some(current_posix) = self.inodes.get(ino).map(|e| e.posix) else {
            return Ok(None);
        };
        if current_posix == crate::inode::layout_posix(layout) {
            return Ok(None);
        }

        let updated_layout = crate::inode::layout_with_posix(layout.clone(), current_posix);
        let expected_layout_bytes: Bytes =
            to_bytes_in::<_, rkyv::rancor::Error>(layout, Vec::new())
                .map_err(FsError::from)?
                .into();
        let updated_layout_bytes: Bytes =
            to_bytes_in::<_, rkyv::rancor::Error>(&updated_layout, Vec::new())
                .map_err(FsError::from)?
                .into();

        publish_set_posix(
            self.backend(),
            key,
            &current_posix,
            &expected_layout_bytes,
            &updated_layout_bytes,
            trace_id,
        )
        .await?;

        Ok(Some(updated_layout))
    }

    /// Apply a chmod / chown / utimensat to an inode. Each field is
    /// optional; `mode == Some(0)` is treated as "unset" (the kernel
    /// never sends a real mode of 0). The change is applied to the
    /// in-memory `entry.posix` immediately (so a getattr within the
    /// attr-cache TTL reflects it) and folded into the cached layout,
    /// which is then routed through the writeback / strict publish
    /// path so it survives a forget+relookup.
    #[allow(clippy::too_many_arguments)]
    pub async fn vfs_setattr_posix(
        &self,
        inode: InodeId,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        atime_ns: Option<u64>,
        mtime_ns: Option<u64>,
        ctime_ns: Option<u64>,
    ) -> Result<(), FsError> {
        self.ensure_writeback_worker_started();

        // Phase 1: mutate entry.posix under the guard, snapshot what we
        // need to persist, drop the guard before any await.
        let (s3_key, base_layout, updated_layout, new_posix, name_removed, inode_id) = {
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
                // POSIX: any of these changes bumps ctime to now unless
                // the caller set ctime explicitly.
                entry.posix.ctime_ns = now_ns();
            }
            let new_posix = entry.posix;
            // Fold the new posix into the cached layout when we have
            // one. With no cached layout we can't synthesise one
            // without an NSS round-trip; the in-memory mutation still
            // stands and the next op picks it up. The unfolded base is
            // kept too: the worker CAS-guards its publish on it.
            let base_layout = entry.layout.clone();
            let updated_layout = base_layout
                .as_ref()
                .map(|l| crate::inode::layout_with_posix(l.clone(), new_posix));
            let s3_key = entry.s3_key.clone();
            let name_removed = entry.name_removed;
            // Derive the hardlink id from a cached Indirect redirect when the
            // entry's `inode_id` was never set (e.g. the layout was cached by
            // a plain readdir that did not resolve it). Without this the
            // metadata update would take the non-hardlink path and overwrite
            // the redirect with a normal layout.
            let inode_id = entry.inode_id.or_else(|| match entry.layout.as_ref() {
                Some(l) => match &l.state {
                    ObjectState::Indirect(redir) => Some(redir.inode_id),
                    _ => None,
                },
                None => None,
            });
            (
                s3_key,
                base_layout,
                updated_layout,
                new_posix,
                name_removed,
                inode_id,
            )
        };

        // The dentry was unlinked; skip the NSS publish so we don't
        // resurrect the deleted file. The in-memory mutation already
        // happened, which is the right semantic for a still-open fd.
        if name_removed {
            return Ok(());
        }

        if let Some(layout) = updated_layout {
            // Hardlink: the shared metadata (mode/uid/gid/times) lives in
            // the `#hardlink/<inode_id>` InodeRecord, not at this name's
            // redirect. Fold the new posix into the record's layout so
            // every name observes the chmod/chown/utimes; nlink and
            // orphan_since are preserved.
            if let Some(id) = inode_id {
                let trace_id = TraceId::new();
                // Apply only the requested posix deltas to the FRESHLY
                // fetched record layout inside the CAS. Replacing the whole
                // layout with the snapshot-derived `layout` would restore a
                // stale size/blob_version if a hardlink-write flush bumped
                // the record between our snapshot and this CAS; and merging
                // field-by-field (rather than overwriting posix wholesale)
                // preserves a concurrent change to fields this call does not
                // touch.
                let committed = self
                    .cas_mutate_inode_record(id, &trace_id, |r| {
                        let mut p = crate::inode::layout_posix(&r.layout);
                        if let Some(m) = mode
                            && m != 0
                        {
                            p.mode = m;
                        }
                        if let Some(u) = uid {
                            p.uid = u;
                        }
                        if let Some(g) = gid {
                            p.gid = g;
                        }
                        if let Some(mt) = mtime_ns {
                            p.mtime_ns = mt;
                        }
                        if let Some(c) = ctime_ns {
                            p.ctime_ns = c;
                        } else if mode.is_some_and(|m| m != 0)
                            || uid.is_some()
                            || gid.is_some()
                            || atime_ns.is_some()
                            || mtime_ns.is_some()
                        {
                            p.ctime_ns = now_ns();
                        }
                        r.layout = crate::inode::layout_with_posix(r.layout.clone(), p);
                        Ok(())
                    })
                    .await?;
                // Reflect the committed record (our deltas + any concurrent
                // flush's size/version) into the local cache. Persist the
                // hardlink id too: when it was derived from a cached Indirect
                // redirect rather than `entry.inode_id`, the committed layout
                // we cache is the record's normal layout, so without setting
                // inode_id a second setattr would see a normal layout with no
                // id, take the non-hardlink path, and clobber the redirect.
                if let Some(mut e) = self.inodes.get_mut(inode) {
                    e.inode_id = Some(id);
                    e.posix = crate::inode::layout_posix(&committed.layout);
                    e.layout = Some(committed.layout.clone());
                }
                return Ok(());
            }

            let layout_bytes: Bytes =
                match to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new()) {
                    Ok(v) => v.into(),
                    Err(e) => {
                        tracing::warn!(error = %e, "vfs_setattr_posix: layout serialise failed");
                        return Ok(());
                    }
                };
            let expected_layout_bytes: Bytes = match to_bytes_in::<_, rkyv::rancor::Error>(
                &base_layout.expect("updated_layout implies a base layout"),
                Vec::new(),
            ) {
                Ok(v) => v.into(),
                Err(e) => {
                    tracing::warn!(error = %e, "vfs_setattr_posix: layout serialise failed");
                    return Ok(());
                }
            };
            // Keep the cached layout in sync with the bytes we publish so
            // a follow-up op reads the new posix from entry.layout (and a
            // follow-up setattr CAS-chains off it).
            if let Some(mut e) = self.inodes.get_mut(inode) {
                e.layout = Some(layout);
            }
            let trace_id = TraceId::new();
            self.publish_posix_update(
                inode,
                &s3_key,
                new_posix,
                expected_layout_bytes,
                layout_bytes,
                &trace_id,
            )
            .await?;
        }
        Ok(())
    }

    /// Create a fifo / block / char / unix-socket inode (the kernel
    /// routes both `mknod(2)` and `mkfifo(2)` here). fs_server only
    /// round-trips the metadata; the kernel owns all I/O against the
    /// open fd.
    pub async fn vfs_mknod(
        &self,
        parent: InodeId,
        name: &str,
        kind: SpecialKind,
        rdev: u32,
        init_posix: PosixAttrs,
    ) -> Result<VfsAttr, FsError> {
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

        let ifmt = match kind {
            SpecialKind::Fifo => libc::S_IFIFO,
            SpecialKind::BlockDevice => libc::S_IFBLK,
            SpecialKind::CharDevice => libc::S_IFCHR,
            SpecialKind::Socket => libc::S_IFSOCK,
        };
        let mut posix = init_posix;
        // Re-stamp the right S_IFMT bits even if the caller passed only
        // permission bits, so a cross-instance stat sees the right kind.
        if posix.mode != 0 {
            posix.mode = (posix.mode & !libc::S_IFMT) | ifmt;
        }

        let layout = ObjectLayout {
            version_id: ObjectLayout::gen_version_id(),
            block_size: DEFAULT_BLOCK_SIZE,
            timestamp: now_ns() / 1_000_000,
            blob_version: 0,
            data_write_token: 0,
            next_data_write_token: 0,
            pending_data_write: None,
            state: ObjectState::Special(SpecialData {
                kind,
                rdev,
                core_meta_data: ObjectCoreMetaData {
                    size: 0,
                    etag: String::new(),
                    headers: vec![],
                    checksum: None,
                    posix: Some(Box::new(posix)),
                },
            }),
        };

        let layout_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())
            .map_err(FsError::from)?
            .into();

        // Cache the inode (and its layout) before publishing so the
        // async path has an `ino` to open a cycle against and a
        // read-your-writes lookup can serve the not-yet-committed entry.
        let (ino, _) = self
            .inodes
            .lookup_or_insert(&key, EntryType::File, Some(layout.clone()));

        self.publish_inode_layout(ino, &key, &prefix, name, layout_bytes, &trace_id)
            .await?;

        self.cache_dir_entry(
            &prefix,
            name,
            ino,
            Self::dir_entry_kind_from_layout(&layout),
        );
        self.touch_parent_times(parent);

        self.make_file_attr(ino, &layout)
    }

    pub async fn vfs_open(&self, inode: InodeId, flags: u32) -> Result<FileHandleId, FsError> {
        let write_flags = libc::O_WRONLY as u32
            | libc::O_RDWR as u32
            | libc::O_APPEND as u32
            | libc::O_TRUNC as u32;
        let is_write = flags & write_flags != 0;

        if is_write && !self.read_write {
            return Err(FsError::ReadOnly);
        }

        {
            let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
            if entry.entry_type != EntryType::File {
                return Err(FsError::IsDir);
            }
        }

        // In default writeback mode, every open is the recovery point for a
        // deferred publish error. Read opens additionally publish any dirty
        // local handle inline first: the kernel sends RELEASE lazily after
        // close(2) returns (and a dup'ed fd can delay it), so waiting on
        // cycles alone could serve a stale pre-flush layout when OPEN wins
        // that race. Write opens do not flush another live writer; they just
        // drain any already-registered release cycle and let the write lock
        // below return EBUSY if the old writer is still open.
        if self.writeback_mode == WritebackMode::Default {
            if !is_write && let Some(dirty_fh) = self.dirty_write_owner(inode) {
                match self.flush_write_buffer(dirty_fh).await {
                    // The handle raced its release; the release path
                    // owns the flush now and the drain below waits it.
                    Err(FsError::BadFd) => {}
                    res => res?,
                }
            }
            self.drain_inode_to_barrier(inode).await?;
        }

        let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
        let s3_key = entry.s3_key.clone();
        let layout = entry.layout.clone();
        let cached_inode_id = entry.inode_id;
        drop(entry);

        // Enforce single-writer per inode. The first writer
        // wins and subsequent write-mode opens fail with EBUSY. The lock
        // is process-local in-memory state and dies with the process on
        // crash, so the next open reacquires.
        let fh = self.alloc_fh();
        if is_write {
            self.acquire_write_lock_retry(inode, fh).await?;
        }

        // Resolve the layout (cold-fetch on a cache miss, then follow a
        // hardlink redirect to the shared record's real layout) and persist
        // any resolved hardlink identity back to the inode table. Wrapped so
        // a failure after the write lock was acquired still releases it;
        // otherwise the inode is left permanently EBUSY.
        //
        // Persisting the resolved `inode_id` is also what stops a cold-cache
        // Indirect entry (e.g. populated by readdirplus without a prior
        // vfs_lookup) from flushing a Normal layout over its redirect: the
        // flush keys its record-aware path on `entry.inode_id`. The redirect
        // itself has no blob_guid, so the resolved real layout is also what
        // lets the write buffer seed from the shared blob and reconcile at
        // the correct blob_version. Covers a cold cache (layout is
        // `Indirect`) and a warm one (cached `inode_id`, possibly a stale
        // pre-promotion layout copy).
        let resolved = async {
            let layout = match layout {
                Some(l) => Some(l),
                None => match self.backend().get_inode(&s3_key, &TraceId::new()).await {
                    Ok(l) => Some(l),
                    Err(FsError::NotFound) if is_write => None,
                    Err(FsError::NotFound) if !is_write => {
                        self.drain_inode_to_barrier(inode).await?;
                        match self.backend().get_inode(&s3_key, &TraceId::new()).await {
                            Ok(l) => Some(l),
                            Err(e) => return Err(e),
                        }
                    }
                    Err(e) => return Err(e),
                },
            };
            match layout {
                Some(l) => {
                    let (real, resolved_id) = if let Some(id) = cached_inode_id {
                        let real = self
                            .backend()
                            .get_inode_record(id, &TraceId::new())
                            .await?
                            .layout;
                        (real, Some(id))
                    } else if matches!(l.state, ObjectState::Indirect(_)) {
                        let (real, id, _nlink) = self.resolve_indirect(l, &TraceId::new()).await?;
                        (real, id)
                    } else {
                        (l, None)
                    };
                    if let Some(id) = resolved_id
                        && let Some(mut e) = self.inodes.get_mut(inode)
                    {
                        e.inode_id = Some(id);
                        e.layout = Some(real.clone());
                    }
                    Ok(Some(real))
                }
                None => Ok(None),
            }
        }
        .await;
        let layout = match resolved {
            Ok(l) => l,
            Err(e) => {
                if is_write {
                    self.release_write_lock(inode, fh);
                }
                return Err(e);
            }
        };

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

        let has_trunc = flags & libc::O_TRUNC as u32 != 0;
        let write_buf = if is_write {
            if let Some(ref l) = layout
                && !has_trunc
            {
                // Existing file, no O_TRUNC: seed a sparse buffer from the
                // committed geometry. No whole-file preload; partial-block
                // edits lazy-load only the blocks they touch.
                let blob_guid = l.blob_guid().ok();
                let committed_size = l.size().unwrap_or(0);
                Some(WriteBuffer::new(blob_guid, committed_size, l.block_size))
            } else if let Some(ref l) = layout {
                // O_TRUNC on an existing file: file_size 0, keep blob_guid so
                // the override flush trims the old blocks; size_changed/dirty
                // so flush sees the truncate. The committed layout size still
                // bounds the flush trim range.
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
        // only; writers own the blob's bytes via `WriteBuffer` and
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
            // the conservative default is `false`; only the
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
                write_buf,
                backing_id: None,
            },
        );

        Ok(fh)
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

    pub async fn vfs_flush(&self, fh: FileHandleId) -> Result<(), FsError> {
        self.ensure_writeback_worker_started();

        let inode = if self.writeback_mode == WritebackMode::Default {
            self.file_handles.get(&fh).map(|h| h.ino)
        } else {
            None
        };

        // Drain queued cycles BEFORE the inline flush so a queued
        // lower-generation publish lands first and can't be reordered
        // after (and a stale worker put can't overwrite) the fresh CAS
        // publish this flush is about to make.
        if let Some(inode) = inode {
            self.drain_inode_to_barrier(inode).await?;
        }

        self.flush_write_buffer(fh).await?;

        // Post-flush drain: wait for any cycle that raced in (e.g. an
        // async release flush on another handle) and surface deferred
        // EIO if a drained cycle failed. No-op in strict mode (the
        // queue is always empty there) and for idle inodes.
        //
        // This is the durability barrier used by fsync(2) / O_SYNC. The
        // default-mode close(2) path leaves work to FUSE_RELEASE:
        // blocking every close on a worker tick erases the writeback win
        // on create-heavy workloads (tar -xf, cp -r).
        if let Some(inode) = inode {
            self.drain_inode_to_barrier(inode).await?;
        }

        Ok(())
    }

    /// FUSE_FLUSH close path. Strict mode keeps legacy close-time error
    /// reporting by flushing synchronously. Default writeback mode leaves
    /// the dirty publish to FUSE_RELEASE so create-heavy closes can
    /// pipeline.
    pub async fn vfs_flush_for_close(&self, fh: FileHandleId) -> Result<(), FsError> {
        match self.writeback_mode {
            WritebackMode::Strict => self.vfs_flush(fh).await,
            WritebackMode::Default => Ok(()),
        }
    }

    /// Variant of `vfs_flush` for callers that need to publish buffered
    /// data without waiting on the writeback barrier: it
    /// publishes the buffered write data synchronously (so write errors
    /// still surface at close) but does not wait on the writeback
    /// barrier. The placeholder/metadata cycle stays queued and the
    /// worker drains it on its next tick; any deferred error propagates
    /// on the next open/fsync of the same path.
    pub async fn vfs_flush_no_drain(&self, fh: FileHandleId) -> Result<(), FsError> {
        self.ensure_writeback_worker_started();
        self.flush_write_buffer(fh).await
    }

    /// Force any still-dirty write handles through the publish path. Used by
    /// destroy because FUSE_RELEASE may still be queued when shutdown starts.
    pub async fn flush_open_dirty_handles(&self) -> Result<(), FsError> {
        if self.writeback_mode != WritebackMode::Default {
            return Ok(());
        }
        let dirty_fhs: Vec<(FileHandleId, InodeId)> = self
            .file_handles
            .iter()
            .filter(|e| e.value().write_buf.as_ref().is_some_and(|wb| wb.dirty))
            .map(|e| (*e.key(), e.value().ino))
            .collect();

        self.flush_dirty_handles(dirty_fhs, "all").await
    }

    fn dirty_handles_for_inode(&self, inode: InodeId) -> Vec<(FileHandleId, InodeId)> {
        self.dirty_write_owner(inode)
            .map(|fh| vec![(fh, inode)])
            .unwrap_or_default()
    }

    fn dirty_handles_under_prefix(&self, prefix: &str) -> Vec<(FileHandleId, InodeId)> {
        self.file_handles
            .iter()
            .filter(|e| {
                e.value().s3_key.starts_with(prefix)
                    && e.value().write_buf.as_ref().is_some_and(|wb| wb.dirty)
            })
            .map(|e| (*e.key(), e.value().ino))
            .collect()
    }

    async fn flush_dirty_handles_for_inode(&self, inode: InodeId) -> Result<(), FsError> {
        if self.writeback_mode != WritebackMode::Default {
            return Ok(());
        }
        let dirty_fhs = self.dirty_handles_for_inode(inode);
        self.flush_dirty_handles(dirty_fhs, "inode").await
    }

    async fn flush_dirty_handles_under_prefix(&self, prefix: &str) -> Result<(), FsError> {
        if self.writeback_mode != WritebackMode::Default {
            return Ok(());
        }
        let dirty_fhs = self.dirty_handles_under_prefix(prefix);
        self.flush_dirty_handles(dirty_fhs, "prefix").await
    }

    async fn flush_dirty_handles(
        &self,
        dirty_fhs: Vec<(FileHandleId, InodeId)>,
        scope: &'static str,
    ) -> Result<(), FsError> {
        let mut failed = false;
        for (fh, ino) in dirty_fhs {
            match self.flush_write_buffer(fh).await {
                Ok(()) | Err(FsError::BadFd) => {}
                Err(e) => {
                    failed = true;
                    tracing::warn!(fh = fh.0, ino = ino.0, scope, error = %e, "dirty handle flush failed");
                }
            }
        }
        if failed {
            return Err(FsError::Internal("writeback drain".to_string()));
        }
        Ok(())
    }

    /// Drain every writeback cycle for `inode` whose generation is
    /// at or below the barrier captured at entry. Returns when every
    /// cycle has reached `Done` (success or short-circuit on failure).
    /// Surfaces deferred `EIO` if any drained cycle failed.
    pub async fn drain_inode_to_barrier(&self, inode: InodeId) -> Result<(), FsError> {
        let Some(barrier) = self.writeback.fsync_barrier(inode) else {
            // Idle inode: nothing queued. A lingering taint from an
            // earlier failed publish still surfaces as deferred EIO,
            // once: consuming it here keeps a single failed publish
            // from wedging every later open/fsync of the inode in
            // permanent EIO with no recovery path.
            if self.writeback.take_taint(inode) {
                self.drop_cached_layout(inode);
                return Err(FsError::Internal("writeback drain".to_string()));
            }
            return Ok(());
        };

        self.wait_cycles_drained(inode, barrier).await?;

        // Surface a deferred error (once) if the drained cycles tainted
        // the inode. The FUSE layer will translate to EIO; the
        // application is expected to close-and-reopen on the remote
        // winner.
        if self.writeback.take_taint(inode) {
            self.drop_cached_layout(inode);
            return Err(FsError::Internal("writeback drain".to_string()));
        }

        Ok(())
    }

    /// Drop the cached layout for `inode` so the next access cold-fetches
    /// from NSS. Called when a publish taint is consumed: the local
    /// layout (and any symlink target it carries) is what failed to
    /// publish, so re-serving it after the one-shot EIO would hand back
    /// stale state that lost to the remote winner. Clearing it forces the
    /// post-EIO retry down the cold-fetch path, mirroring an eviction.
    fn drop_cached_layout(&self, inode: InodeId) {
        if let Some(mut e) = self.inodes.get_mut(inode) {
            e.layout = None;
        }
    }

    /// Every inode with writeback state attached to `key`: the cached
    /// InodeTable entry plus any inode the queue still tracks for the
    /// key (an intent outlives its entry when a FORGET raced the
    /// enqueue). Draining only the table's inode would let the worker
    /// commit the orphaned intent after a delete and resurrect the name.
    fn writeback_drain_targets(&self, key: &str, ino: Option<InodeId>) -> Vec<InodeId> {
        let mut targets = self.writeback.intent_inodes_for_key(key);
        if let Some(ino) = ino
            && !targets.contains(&ino)
        {
            targets.push(ino);
        }
        targets
    }

    fn writeback_drain_targets_under_prefix(&self, prefix: &str) -> Vec<InodeId> {
        let mut targets = self.writeback.intent_inodes_for_key_prefix(prefix);
        for (inode, _) in self.writeback.snapshot_dirty_cycles() {
            if let Some(entry) = self.inodes.get(inode)
                && entry.s3_key.starts_with(prefix)
                && !targets.contains(&inode)
            {
                targets.push(inode);
            }
        }
        targets.sort_unstable();
        targets.dedup();
        targets
    }

    async fn drain_writeback_under_prefix(&self, prefix: &str) -> Result<(), FsError> {
        if self.writeback_mode != WritebackMode::Default {
            return Ok(());
        }
        self.flush_dirty_handles_under_prefix(prefix).await?;
        for ino in self.writeback_drain_targets_under_prefix(prefix) {
            self.drain_inode_to_barrier(ino).await?;
        }
        Ok(())
    }

    /// True if a regular-file child lives under `dir_key` in local state
    /// that the NSS emptiness probe cannot yet see. A file create publishes
    /// its final layout on FUSE_RELEASE (a writeback cycle), not as a
    /// PutInode intent, so between create and the release publish landing in
    /// NSS the child is visible only here: as an open handle or an in-flight
    /// writeback cycle. `rmdir` consults this so it honours the POSIX
    /// non-empty contract instead of deleting a directory out from under an
    /// in-flight child publish. `dir_key` ends in '/'; the directory marker
    /// itself is excluded by the length check.
    fn has_local_file_child_under(&self, dir_key: &str) -> bool {
        for h in self.file_handles.iter() {
            let k = &h.value().s3_key;
            if k.len() > dir_key.len()
                && k.starts_with(dir_key)
                // An unlinked-but-still-open fd is a POSIX orphan, not a
                // directory child: its publish is suppressed
                // (`name_removed`), so it must not hold rmdir hostage.
                && !self
                    .inodes
                    .get(h.value().ino)
                    .is_some_and(|e| e.name_removed)
            {
                return true;
            }
        }
        for (inode, _) in self.writeback.snapshot_dirty_cycles() {
            if let Some(e) = self.inodes.get(inode)
                && e.entry_type == EntryType::File
                && !e.name_removed
                && e.s3_key.len() > dir_key.len()
                && e.s3_key.starts_with(dir_key)
            {
                return true;
            }
        }
        false
    }

    /// Drain variant for the unlink / rmdir path. Like
    /// `drain_inode_to_barrier` it waits for in-flight publishes so the
    /// worker cannot resurrect the name after the delete, but a publish
    /// taint does NOT abort the delete: removing a name has no durability
    /// dependency on a prior write, and `mark_failed` already
    /// dropped the pending intent, so nothing can be resurrected. The
    /// taint is cleared since the inode is going away. A genuine drain
    /// timeout still aborts (an intent may still be in flight).
    ///
    /// Returns `true` if a taint was cleared: the entry's create publish
    /// failed, so NSS has nothing for the (still locally visible) name and
    /// the caller must treat a NSS miss as a successful local-only delete,
    /// not ENOENT.
    pub async fn drain_inode_for_delete(&self, inode: InodeId) -> Result<bool, FsError> {
        if let Some(barrier) = self.writeback.fsync_barrier(inode) {
            self.wait_cycles_drained(inode, barrier).await?;
        }
        Ok(self.writeback.clear_taint(inode))
    }

    /// Poll until every cycle on `inode` at or below `barrier` reaches
    /// `Done`, or the drain deadline elapses. The worker drains every
    /// poll_ms; a 1ms poll keeps the barrier latency bounded to a tick or
    /// two.
    async fn wait_cycles_drained(
        &self,
        inode: InodeId,
        barrier: Generation,
    ) -> Result<(), FsError> {
        let poll_dur = Duration::from_millis(1);
        let timeout_secs = self.backend_config.config.rpc_request_timeout_seconds * 4;
        // Monotonic clock: a wall-clock step (NTP/manual) must not turn a
        // healthy drain into a spurious timeout or an unbounded wait.
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        loop {
            if self.writeback.cycles_at_or_below_drained(inode, barrier) {
                return Ok(());
            }
            if Instant::now() > deadline {
                tracing::warn!(
                    inode = inode.0,
                    barrier = barrier.0,
                    "writeback drain timeout"
                );
                return Err(FsError::Internal("writeback drain".to_string()));
            }
            compio_runtime::time::sleep(poll_dur).await;
        }
    }

    /// Mount-wide writeback barrier: drain every dirty cycle the queue
    /// currently knows about. Used by `fsyncdir(2)`. A true subtree-scoped
    /// variant is a future optimization; this is a cheap, correct barrier.
    pub async fn drain_all_dirty_cycles(&self) -> Result<(), FsError> {
        self.flush_open_dirty_handles().await?;
        self.drain_dirty_cycles_inner(false).await
    }

    /// fsyncdir(2): drain every dirty cycle, then surface (and consume)
    /// deferred publish failures for entries under this directory. The
    /// durability-conscious create protocol (create, write, close, then
    /// fsync the parent dir fd) never re-opens the child, so this barrier
    /// is its only chance to learn a queued child publish was dropped;
    /// returning success there would report a lost file as durable.
    /// Taints outside the subtree stay put for their own fsync/open path.
    pub async fn vfs_fsyncdir(&self, dir_ino: InodeId) -> Result<(), FsError> {
        self.drain_all_dirty_cycles().await?;
        if self.writeback_mode != WritebackMode::Default {
            return Ok(());
        }
        let Some(prefix) = self.dir_prefix(dir_ino) else {
            return Ok(());
        };
        let mut failed = false;
        for ino in self.writeback.tainted_inodes() {
            let under = self
                .inodes
                .get(ino)
                .is_some_and(|e| e.s3_key.len() > prefix.len() && e.s3_key.starts_with(&prefix));
            if under && self.writeback.take_taint(ino) {
                self.drop_cached_layout(ino);
                failed = true;
            }
        }
        if failed {
            return Err(FsError::Internal("writeback drain".to_string()));
        }
        Ok(())
    }

    /// Unmount variant of `drain_all_dirty_cycles`: re-snapshots until a
    /// pass comes back empty, so a cycle enqueued after an earlier
    /// snapshot (e.g. a release flush racing shutdown) is waited on too.
    /// Terminates because `set_enqueue_blocked` stops the intent side
    /// from growing and the remaining producers (in-flight releases)
    /// are finite.
    pub async fn drain_all_dirty_cycles_until_empty(&self) -> Result<(), FsError> {
        self.flush_open_dirty_handles().await?;
        self.drain_dirty_cycles_inner(true).await
    }

    async fn drain_dirty_cycles_inner(&self, until_empty: bool) -> Result<(), FsError> {
        let poll_dur = Duration::from_millis(5);
        let timeout_secs = self.backend_config.config.rpc_request_timeout_seconds * 4;
        // Monotonic clock: see wait_cycles_drained.
        let deadline = Instant::now() + Duration::from_secs(timeout_secs);
        let mut empty_passes = 0u8;
        loop {
            let snapshot = self.writeback.snapshot_dirty_cycles();
            if snapshot.is_empty() {
                if until_empty && empty_passes == 0 {
                    empty_passes += 1;
                    if Instant::now() > deadline {
                        return Err(FsError::Internal("writeback drain timeout".to_string()));
                    }
                    compio_runtime::time::sleep(poll_dur).await;
                    continue;
                }
                break;
            }
            empty_passes = 0;
            for (inode, barrier) in snapshot {
                loop {
                    if self.writeback.cycles_at_or_below_drained(inode, barrier) {
                        break;
                    }
                    if Instant::now() > deadline {
                        tracing::warn!(
                            inode = inode.0,
                            barrier = barrier.0,
                            "drain_all_dirty_cycles timeout"
                        );
                        return Err(FsError::Internal("writeback drain timeout".to_string()));
                    }
                    compio_runtime::time::sleep(poll_dur).await;
                }
            }
            if !until_empty {
                break;
            }
        }
        // This barrier waits for in-flight cycles but must NOT consume a
        // per-inode taint. A taint belongs to that file's own fsync / open
        // path, or, for entries under a fsyncdir target, to the scoped
        // sweep in `vfs_fsyncdir`; consuming it here on an unrelated
        // barrier would both hand this caller a spurious EIO and
        // hide the deferred write error from the file's real owner. The
        // unmount drain (until_empty) does sweep taints so a lost publish is
        // surfaced, and pairs each with a cached-layout drop so post-error
        // access cold-fetches instead of re-serving the layout that lost.
        if until_empty {
            let tainted = self.writeback.take_all_taints();
            for &inode in &tainted {
                self.drop_cached_layout(inode);
            }
            if !tainted.is_empty() {
                return Err(FsError::Internal("writeback drain".to_string()));
            }
        }
        Ok(())
    }

    pub async fn vfs_release(&self, fh: FileHandleId) -> Result<(), FsError> {
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

        // Flush, but DON'T early-return on error: the handle and its
        // inode-scoped write lock must always be torn down on release,
        // even when the close-time flush fails (e.g. a transient CAS
        // conflict or RPC timeout). Returning early here would leave the
        // FileHandle in `file_handles`, so `acquire_write_lock`'s
        // stale-owner reclaim (which only fires when the owner fh is GONE
        // from the table) never triggers, and the inode stays wedged at
        // EBUSY for the lifetime of the mount, observed as
        // `echo x > f; open f O_TRUNC` returning EBUSY in open/00.t. The
        // flush error is still surfaced to the caller after cleanup.
        let flush_res = if has_dirty {
            self.flush_write_buffer(fh).await
        } else {
            Ok(())
        };

        // Get the inode before removing the handle
        let ino = self.file_handles.get(&fh).map(|h| h.ino);
        self.file_handles.remove(&fh);

        // Release the inode-scoped write lock if this handle held it.
        // Read-only handles never acquired it.
        if was_writer && let Some(ino) = ino {
            self.release_write_lock(ino, fh);
        }

        flush_res?;

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

    pub async fn vfs_create(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<(VfsAttr, FileHandleId), FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}", prefix, name);

        let (ino, _) = self.inodes.lookup_or_insert(&key, EntryType::File, None);

        // Seed the in-memory posix from the create mode + caller ids so
        // the file reports the right st_mode/uid/gid before its first
        // flush; the flush folds this into the persisted layout.
        let now = now_ns();
        if let Some(mut entry) = self.inodes.get_mut(ino) {
            entry.posix = PosixAttrs {
                mode: (mode & !libc::S_IFMT) | libc::S_IFREG,
                uid,
                gid,
                mtime_ns: now,
                ctime_ns: now,
            };
            entry.name_removed = false;
            entry.atime_ns = 0;
        }

        let fh = self.alloc_fh();
        // vfs_create implicitly opens the new file for writing,
        // so it must obey the inode-scoped write lock. A re-create on an
        // inode that already has a live write handle returns EBUSY.
        self.acquire_write_lock_retry(ino, fh).await?;
        self.file_handles.insert(
            fh,
            FileHandle {
                ino,
                s3_key: key,
                layout: None,
                write_buf: Some({
                    // Fresh empty file; dirty so the close-time flush
                    // publishes the 0-byte inode.
                    let mut wb = WriteBuffer::new(None, 0, DEFAULT_BLOCK_SIZE);
                    wb.dirty = true;
                    wb.size_changed = true;
                    wb
                }),
                backing_id: None,
            },
        );

        let attr = self.make_new_file_attr(ino, 0);

        self.cache_dir_entry(&prefix, name, ino, DirEntryKind::RegularFile);
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
        parent: InodeId,
        name: &str,
        target: &[u8],
        uid: u32,
        gid: u32,
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

        // Symlink permission bits are conventionally 0777 and ignored
        // by the kernel; uid/gid come from the caller so lchown can
        // adjust them.
        let now = now_ns();
        let posix = PosixAttrs {
            mode: symlink_mode(0o777),
            uid,
            gid,
            mtime_ns: now,
            ctime_ns: now,
        };

        let layout = ObjectLayout {
            version_id: ObjectLayout::gen_version_id(),
            block_size: DEFAULT_BLOCK_SIZE,
            timestamp,
            blob_version: 0,
            data_write_token: 0,
            next_data_write_token: 0,
            pending_data_write: None,
            state: ObjectState::Symlink(SymlinkData {
                target: target.to_vec(),
                core_meta_data: ObjectCoreMetaData {
                    size: target.len() as u64,
                    etag: String::new(),
                    headers: vec![],
                    checksum: None,
                    posix: Some(Box::new(posix)),
                },
            }),
        };

        let layout_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())
            .map_err(FsError::from)?
            .into();

        // Cache the inode (and layout) before publishing so the async
        // path has an `ino` for its cycle and a read-your-writes lookup
        // can serve the not-yet-committed symlink.
        let (ino, _) = self
            .inodes
            .lookup_or_insert(&key, EntryType::File, Some(layout.clone()));

        self.publish_inode_layout(ino, &key, &prefix, name, layout_bytes, &trace_id)
            .await?;

        self.cache_dir_entry(&prefix, name, ino, DirEntryKind::Symlink);
        self.touch_parent_times(parent);

        self.make_file_attr(ino, &layout)
    }

    /// Return the bytes a `readlink(2)` should hand back. Returns
    /// `InvalidArgument` (EINVAL) when the inode is not a symlink,
    /// matching the `readlink(2)` errno for non-symlink targets.
    pub async fn vfs_readlink(&self, inode: InodeId) -> Result<Vec<u8>, FsError> {
        let (key, cached_target, known_non_symlink) = {
            let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
            if entry.entry_type != EntryType::File {
                return Err(FsError::InvalidArg);
            }
            let cached_target = entry
                .layout
                .as_ref()
                .and_then(|layout| layout.symlink_target().map(|target| target.to_vec()));
            let known_non_symlink = entry
                .layout
                .as_ref()
                .is_some_and(|layout| cached_target.is_none() && !layout.is_symlink());
            (entry.s3_key.clone(), cached_target, known_non_symlink)
        };

        if let Some(target) = cached_target {
            // The cached target is authoritative for read-your-writes, so
            // don't block on the async publish barrier. Still surface a
            // deferred publish failure once (errseq-style) so a lost symlink
            // create isn't silently hidden behind a stale local target.
            if self.writeback.take_taint(inode) {
                self.drop_cached_layout(inode);
                return Err(FsError::Internal("writeback drain".to_string()));
            }
            return Ok(target);
        }
        if known_non_symlink {
            return Err(FsError::InvalidArg);
        }

        // Cold path: re-fetch from NSS. This handles the case where
        // the inode entry was created by lookup but the layout was
        // dropped (memory pressure / eviction).
        if self.writeback.has_pending_intent_for_key(&key) || self.writeback.is_tainted(inode) {
            self.drain_inode_to_barrier(inode).await?;
        }

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

    /// Clean up the value that previously lived at `key` after it was
    /// unlinked or replaced by a rename. Handles every layout shape:
    ///   - `Normal`: GC the blob blocks (deferred when a handle is still
    ///     open so reads against the open fd keep working).
    ///   - `Mpu(Completed)`: GC each part blob and delete the part inodes.
    ///   - `Indirect`: decrement the shared `InodeRecord`'s nlink, bumping
    ///     the surviving file's ctime; when nlink reaches 0 delete the
    ///     record and GC the real blob (or stamp `orphan_since` if a
    ///     handle is still open). A redirect shares its blob with other
    ///     names, so it is never deferred as a whole-blob cleanup.
    async fn cleanup_orphaned_value(
        &self,
        key: &str,
        ino_hint: Option<InodeId>,
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
                // Whether an open fd still references the inode is
                // independent of nlink; decide it up front so the CAS
                // mutation can fold orphan-marking into the same write.
                let still_open = ino_hint
                    .map(|i| self.has_open_handles_for_inode(i, None))
                    .unwrap_or(false);
                // CAS-decrement so a concurrent record-aware flush isn't
                // clobbered (and vice versa); on nlink>0 stamp the surviving
                // file's ctime, on the last link mark orphan if a handle
                // still holds it.
                let committed = self
                    .cas_mutate_inode_record(inode_id, trace_id, |r| {
                        r.nlink = r.nlink.saturating_sub(1);
                        if r.nlink > 0 {
                            let mut p = crate::inode::layout_posix(&r.layout);
                            p.ctime_ns = now_ns();
                            r.layout = crate::inode::layout_with_posix(r.layout.clone(), p);
                        } else if still_open {
                            r.orphan_since = Some(now_ns());
                        }
                        Ok(())
                    })
                    .await;
                match committed {
                    Ok(record) if record.nlink == 0 && !still_open => {
                        // Reclaim the shared blob + record. This is safe
                        // against a racing link: `bump_link` refuses to
                        // revive an nlink==0 record, so a link can only have
                        // committed *before* our decrement (then we observe
                        // nlink>0 above and skip), never after. The re-read
                        // confirms nlink is still 0 before deleting.
                        if let Ok(fresh) = self.backend().get_inode_record(inode_id, trace_id).await
                            && fresh.nlink == 0
                        {
                            self.backend()
                                .delete_blob_blocks(&fresh.layout, trace_id)
                                .await;
                            let _ = self.backend().delete_inode_record(inode_id, trace_id).await;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        // The name is already removed but the shared link
                        // count could not be decremented (e.g. CAS retries
                        // exhausted under sustained contention). Surface it
                        // rather than silently leaving st_nlink too high /
                        // leaking the blob; a record repair/GC sweep would
                        // reconcile.
                        tracing::warn!(
                            %inode_id, error = %e,
                            "unlink: failed to decrement hardlink record nlink; \
                             link count may be stale until reconciled"
                        );
                    }
                }
            }
            _ => {}
        }
    }

    pub async fn vfs_unlink(&self, parent: InodeId, name: &str) -> Result<(), FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}", prefix, name);

        let trace_id = TraceId::new();

        // With async metadata, a just-created (or just-chmod'd) inode
        // may still have a PutInode queued. Drain it before the delete
        // so (a) the delete sees the entry in NSS instead of racing to
        // a spurious ENOENT, and (b) the worker can't re-publish it
        // after the delete and resurrect the name. The queue's own
        // per-key inode records are drained too: a FORGET can evict the
        // InodeTable entry while its intent is still queued.
        let ino = self.inodes.find_ino_by_key(&key, EntryType::File);
        let mut tainted_delete = false;
        if self.writeback_mode == WritebackMode::Default {
            for target in self.writeback_drain_targets(&key, ino) {
                // Best-effort: the buffered data is discarded with the name
                // anyway (see drain_inode_for_delete's taint-tolerant
                // contract), so a failed publish must not wedge the delete
                // in permanent EIO. Blocks from a partial flush are
                // reconciled by GC.
                if let Err(e) = self.flush_dirty_handles_for_inode(target).await {
                    tracing::warn!(
                        target = target.0, key = %key, error = %e,
                        "unlink: pre-delete flush failed; proceeding with delete"
                    );
                }
                tainted_delete |= self.drain_inode_for_delete(target).await?;
            }
        }

        // Delete the inode from NSS
        let old_bytes = self.backend().delete_inode(&key, &trace_id).await?;

        let old_bytes = match old_bytes {
            Some(bytes) => bytes,
            // A tainted target's create publish failed: NSS has nothing,
            // but the name is still locally visible (lookup keeps a tainted
            // name resolvable). Finish the delete locally instead of
            // failing a visible name with ENOENT.
            None if tainted_delete => {
                if let Some(ino) = ino {
                    self.inodes.remove_name_mapping(ino);
                }
                self.dir_cache.invalidate(&prefix);
                self.touch_parent_times(parent);
                return Ok(());
            }
            // Return ENOENT if file didn't exist
            None => return Err(FsError::NotFound),
        };

        // Drop this name from the inode table. A hardlink redirect keeps
        // the inode (and its other names) live, so only its alias goes;
        // a single-named file is marked `name_removed` so a still-open fd
        // reports nlink=0 and any in-flight setattr/flush skips re-publish.
        let is_indirect = rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
            .map(|l| matches!(l.state, ObjectState::Indirect(_)))
            .unwrap_or(false);
        if is_indirect {
            self.inodes.remove_alias(&key, EntryType::File);
        } else if let Some(ino) = ino {
            self.inodes.remove_name_mapping(ino);
        }

        // GC the value (blob blocks, or a hardlink nlink decrement).
        self.cleanup_orphaned_value(&key, ino, old_bytes, &trace_id)
            .await;

        // Invalidate dir cache for parent
        self.dir_cache.invalidate(&prefix);
        self.touch_parent_times(parent);

        Ok(())
    }

    pub async fn vfs_mkdir(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<VfsAttr, FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}/", prefix, name);

        let trace_id = TraceId::new();

        // Persist a Directory layout carrying the requested mode + caller
        // ids (instead of the plain marker) so chmod/chown/utime against
        // the directory survive a forget+relookup.
        let now = now_ns();
        let posix = PosixAttrs {
            mode: (mode & !libc::S_IFMT) | libc::S_IFDIR,
            uid,
            gid,
            mtime_ns: now,
            ctime_ns: now,
        };
        let layout = ObjectLayout {
            version_id: ObjectLayout::gen_version_id(),
            block_size: DEFAULT_BLOCK_SIZE,
            timestamp: now / 1_000_000,
            blob_version: 1,
            data_write_token: 0,
            next_data_write_token: 0,
            pending_data_write: None,
            state: ObjectState::Directory(DirectoryData { posix }),
        };
        let layout_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())
            .map_err(FsError::from)?
            .into();

        let (ino, _) =
            self.inodes
                .lookup_or_insert(&key, EntryType::Directory, Some(layout.clone()));

        self.publish_inode_layout(ino, &key, &prefix, name, layout_bytes, &trace_id)
            .await?;

        self.cache_dir_entry(&prefix, name, ino, DirEntryKind::Directory);
        self.dir_cache
            .insert_empty_dir(key.clone(), ino.0, parent.0);
        self.touch_parent_times(parent);

        Ok(self.make_dir_attr(ino))
    }

    pub async fn vfs_rmdir(&self, parent: InodeId, name: &str) -> Result<(), FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}/", prefix, name);

        let trace_id = TraceId::new();

        // Drain a pending async directory publish before the existence /
        // emptiness probe, so a just-created dir is committed to NSS and
        // the worker can't re-publish it after the delete. Includes the
        // queue's per-key inode records: a FORGET can evict the
        // InodeTable entry while its intent is still queued.
        let ino = self.inodes.find_ino_by_key(&key, EntryType::Directory);
        let mut tainted_delete = false;
        if self.writeback_mode == WritebackMode::Default {
            for target in self.writeback_drain_targets(&key, ino) {
                tainted_delete |= self.drain_inode_for_delete(target).await?;
            }
        }

        // A child create may have returned to the caller while its
        // default-mode PutInode is still queued or in flight. NSS
        // listing alone can miss that child, so preserve the POSIX
        // non-empty contract from the in-memory writeback queue first.
        if self.writeback_mode == WritebackMode::Default
            && self.writeback.has_pending_child_put_inode_for_parent(&key)
        {
            return Err(FsError::NotEmpty);
        }

        // Regular-file creates publish their final layout on close, so the
        // writeback queue child check above cannot see an open or async-
        // closing FILE child. A locally cached file child is already
        // visible to this mount and must keep rmdir from winning the race.
        // Only files, not dirs: a cached dir child can be a phantom (a
        // tombstoned subtree still emits a CommonPrefix into the readdir
        // cache), so dir emptiness is decided by the tombstone-filtering
        // no-delimiter NSS list below, not this cache (pjdfstest
        // mkdir/03.t, rmdir/03.t: rm -rf of a deep tree after a
        // mkdir+rmdir of the leaf).
        if self.dir_cache.has_file_children(&key) == Some(true) {
            return Err(FsError::NotEmpty);
        }

        // The dir_cache check above only sees children this mount cached a
        // listing for. A file child created and released under `key` while
        // the parent listing was absent or invalidated publishes its layout
        // via an async release cycle (not a PutInode intent), so it is
        // invisible to both checks above and not yet in NSS. Consult local
        // open-handle / in-flight-cycle state so rmdir can't delete the
        // directory out from under it.
        if self.writeback_mode == WritebackMode::Default && self.has_local_file_child_under(&key) {
            return Err(FsError::NotEmpty);
        }

        // List to check existence and emptiness. Use NO delimiter so
        // NSS walks leaves directly and filters tombstones: the list
        // path only drops tombstoned entries on the LEAF branch. With
        // delimiter "/" a fully-tombstoned subtree still emits a
        // CommonPrefix entry, so `rm -rf` of a deep tree would see a
        // phantom child here and fail with ENOTEMPTY even though every
        // descendant is already deleted (pjdfstest chmod/03.t). Without
        // a delimiter we read raw leaves with tombstones filtered: the
        // dir marker itself plus any live descendant. max_keys=2 is
        // enough; anything other than the marker means non-empty.
        let entries = self
            .backend()
            .list_inodes(&key, "", "", 2, &trace_id)
            .await?;

        // If no entries at all, directory doesn't exist. Exception: a
        // tainted target's mkdir publish failed, so NSS has no marker but
        // the name is still locally visible (lookup keeps a tainted dir
        // resolvable). Finish the delete locally instead of failing the
        // visible name with ENOENT.
        if entries.is_empty() {
            if tainted_delete {
                if let Some(ino) = ino {
                    self.inodes.remove_name_mapping(ino);
                }
                self.dir_cache.invalidate(&prefix);
                self.dir_cache.invalidate(&key);
                self.touch_parent_times(parent);
                return Ok(());
            }
            return Err(FsError::NotFound);
        }

        let has_children = entries.iter().any(|e| e.key != key);
        if has_children {
            return Err(FsError::NotEmpty);
        }

        // Delete the directory marker
        self.backend().delete_inode(&key, &trace_id).await?;

        // Remove from inode table (marks name_removed, no refcount leak)
        if let Some(ino) = ino {
            self.inodes.remove_name_mapping(ino);
        }

        // Invalidate dir cache for parent and self
        self.dir_cache.invalidate(&prefix);
        self.dir_cache.invalidate(&key);
        self.touch_parent_times(parent);

        Ok(())
    }

    pub async fn vfs_rename(
        &self,
        parent: InodeId,
        name: &str,
        new_parent: InodeId,
        new_name: &str,
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

        let trace_id = TraceId::new();

        let src_file_ino = self.inodes.find_ino_by_key(&src_key, EntryType::File);
        for ino in self.writeback_drain_targets(&src_key, src_file_ino) {
            self.flush_dirty_handles_for_inode(ino).await?;
            self.drain_inode_to_barrier(ino).await?;
        }
        let dst_ino_before = self.inodes.find_ino_by_key(&dst_key, EntryType::File);
        for ino in self.writeback_drain_targets(&dst_key, dst_ino_before) {
            self.flush_dirty_handles_for_inode(ino).await?;
            self.drain_inode_to_barrier(ino).await?;
        }
        // A just-created directory publishes its marker via an async
        // PutInode (Default writeback mode), so the NSS probe + rename below
        // would otherwise miss it and ENOENT, and a queued publish could
        // resurrect the old name after the rename. Drain the source (and a
        // replaced destination) directory barrier first, mirroring the file
        // drains above (pjdfstest rename/21.t renames a just-mkdir'd dir).
        // Like the delete drains, the queue's per-key inode records are
        // included so a FORGET-evicted entry can't skip the drain.
        let src_dir_probe = format!("{}/", src_key);
        let src_dir_ino = self
            .inodes
            .find_ino_by_key(&src_dir_probe, EntryType::Directory);
        for ino in self.writeback_drain_targets(&src_dir_probe, src_dir_ino) {
            self.drain_inode_to_barrier(ino).await?;
        }
        let dst_dir_probe = format!("{}/", dst_key);
        let dst_dir_ino = self
            .inodes
            .find_ino_by_key(&dst_dir_probe, EntryType::Directory);
        for ino in self.writeback_drain_targets(&dst_dir_probe, dst_dir_ino) {
            self.drain_inode_to_barrier(ino).await?;
        }

        // Determine type by probing NSS backend directly (no inode side effects)
        let is_dir = match self.backend().get_inode(&src_key, &trace_id).await {
            Ok(_) => false,
            Err(FsError::NotFound) => true,
            Err(e) => return Err(e),
        };

        if is_dir {
            let src_dir_key = format!("{}/", src_key);
            let dst_dir_key = format!("{}/", dst_key);

            // Block async enqueues under the source subtree across the drain
            // + rename. The kernel locks only the rename's parents, not the
            // moved directory, so a create racing in under it (e.g. mkdir
            // dir/sub during mv dir dir2) would otherwise leave an intent the
            // worker commits at the stale pre-rename key long after
            // rename_folder ran, resurrecting a ghost under the old path.
            // Blocking first forces such a create onto the synchronous
            // publish fallback, narrowing the residual race to the strict-mode
            // window. The guard releases the block on every exit path.
            self.writeback.block_prefix(&src_dir_key);
            let _block_guard = PrefixBlockGuard {
                writeback: Arc::clone(&self.writeback),
                prefix: src_dir_key.clone(),
            };

            self.drain_writeback_under_prefix(&src_dir_key).await?;

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
            self.touch_parent_times(parent);
            if new_parent != parent {
                self.touch_parent_times(new_parent);
            }
        } else {
            // Drain pending writeback on src AND dst before the NSS
            // rename so we operate on the post-flush layout and a queued
            // publish can't resurrect either name after the atomic
            // replace (create+close returns to userspace before the
            // close-time publish lands in NSS; rename/09.t / 10.t fire
            // the rename immediately after).
            // POSIX rename(2) atomically replaces an existing
            // regular-file dst. NSS does the swap via
            // `force_overwrite=true` and hands back the prior dst value
            // so we can GC the orphaned blob.
            let old_bytes = self
                .backend()
                .rename_file(&src_key, &dst_key, true, &trace_id)
                .await?;

            // Drop the replaced dst's name from the inode table. A
            // hardlink redirect keeps its inode (other names) live; a
            // single-named file is marked removed so a still-open dst fd
            // won't republish the now-overwritten name.
            let dst_was_indirect =
                rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
                    .map(|l| matches!(l.state, ObjectState::Indirect(_)))
                    .unwrap_or(false);
            if dst_was_indirect {
                self.inodes.remove_alias(&dst_key, EntryType::File);
            } else if let Some(dst_ino) = dst_ino_before {
                self.inodes.remove_name_mapping(dst_ino);
            }

            // GC the value the rename displaced: a blob for a Normal/Mpu
            // file, or an nlink decrement for a hardlink redirect (so a
            // rename over a multiply-linked file leaves the survivors at
            // the right count, rename/23.t).
            self.cleanup_orphaned_value(&dst_key, dst_ino_before, old_bytes, &trace_id)
                .await;

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
            self.touch_parent_times(parent);
            if new_parent != parent {
                self.touch_parent_times(new_parent);
            }
        }

        Ok(())
    }

    pub fn vfs_opendir(&self, inode: InodeId) -> Result<FileHandleId, FsError> {
        if inode != ROOT_INODE {
            // Drop the read guard before `drop_cached_layout` takes a
            // write guard on the same inode: holding both on one shard
            // self-deadlocks.
            {
                let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
                if entry.entry_type != EntryType::Directory {
                    return Err(FsError::NotDir);
                }
            }
            if self.writeback.take_taint(inode) {
                self.drop_cached_layout(inode);
                return Err(FsError::Internal("writeback drain".to_string()));
            }
        }

        Ok(self.alloc_fh())
    }

    pub async fn vfs_readdir(
        &self,
        parent: InodeId,
        offset: u64,
    ) -> Result<Vec<VfsDirEntry>, FsError> {
        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        let dir_entries = self.fetch_dir_entries(parent, &prefix).await?;

        let offset = offset as usize;
        let entries = dir_entries
            .iter()
            .skip(offset)
            .enumerate()
            .map(|(idx, entry)| VfsDirEntry {
                ino: entry.ino,
                kind: entry.kind,
                name: entry.name.clone(),
                offset: (offset + idx + 1) as u64,
            })
            .collect();

        Ok(entries)
    }

    pub async fn vfs_readdirplus(
        &self,
        parent: InodeId,
        offset: u64,
    ) -> Result<Vec<VfsDirEntryPlus>, FsError> {
        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        let dir_entries = self.fetch_dir_entries(parent, &prefix).await?;

        let offset = offset as usize;

        // A subdirectory row comes from the delimiter listing as a
        // common-prefix with no posix, so its entry carries the uid-0
        // placeholder. Seed the real owner from each marker before building
        // attrs, or readdirplus emits uid 0 and the kernel caches it (a
        // later stat/chmod then sees the placeholder owner). Concurrent to
        // bound the cost on a cold `ls` of a many-subdir directory; a
        // posix-known entry is skipped, so repeat listings pay nothing.
        let unknown_dirs: Vec<InodeId> = dir_entries
            .iter()
            .skip(offset)
            .filter(|e| e.kind.is_dir())
            .map(|e| InodeId(e.ino))
            .filter(|&ino| {
                self.inodes
                    .get(ino)
                    .map(|e| !e.posix_known)
                    .unwrap_or(false)
            })
            .collect();
        if !unknown_dirs.is_empty() {
            futures::future::join_all(
                unknown_dirs
                    .into_iter()
                    .map(|ino| self.refresh_dir_posix_if_unknown(ino)),
            )
            .await;
        }

        let trace_id = TraceId::new();
        let mut entries: Vec<VfsDirEntryPlus> =
            Vec::with_capacity(dir_entries.len().saturating_sub(offset));
        // Per-page cache so a directory holding many aliases of one hardlink
        // resolves the shared InodeRecord once, not once per name (otherwise
        // a single readdirplus fans out into N identical record RPCs).
        let mut record_cache: std::collections::HashMap<uuid::Uuid, InodeRecord> =
            std::collections::HashMap::new();
        for (idx, entry) in dir_entries.iter().skip(offset).enumerate() {
            let attr = if entry.kind.is_dir() {
                self.make_dir_attr(InodeId(entry.ino))
            } else {
                // Clone the cached layout out (dropping the map guard before
                // any await), then resolve a hardlink redirect to the shared
                // record's real layout: make_file_attr needs a sized layout,
                // and an `Indirect` redirect has none; `layout.size()`
                // would error and fail the whole readdirplus, surfacing as
                // EINVAL on the first `ls` of a directory holding a hardlink.
                let (cached_layout, cached_id) = self
                    .inodes
                    .get(InodeId(entry.ino))
                    .map(|e| (e.layout.clone(), e.inode_id))
                    .unwrap_or((None, None));
                match cached_layout {
                    Some(l) => {
                        // A hardlink alias either already carries the record
                        // id on its entry (a prior pass replaced the Indirect
                        // redirect with the record's normal layout) or still
                        // has the Indirect redirect cached. Either way resolve
                        // through the per-page record cache.
                        let id_opt = cached_id.or(match &l.state {
                            ObjectState::Indirect(redir) => Some(redir.inode_id),
                            _ => None,
                        });
                        let (resolved, resolved_id, nlink) = if let Some(id) = id_opt {
                            let rec = match record_cache.get(&id) {
                                Some(r) => r.clone(),
                                None => {
                                    let r = self.backend().get_inode_record(id, &trace_id).await?;
                                    record_cache.insert(id, r.clone());
                                    r
                                }
                            };
                            (rec.layout, Some(id), rec.nlink)
                        } else {
                            (l, None, 1)
                        };
                        // Persist the resolved hardlink identity + real
                        // layout + record posix so later lookups/opens/
                        // flushes target the shared record, and so the attr
                        // below reports the record's mode/uid/gid/times
                        // rather than stale cached defaults.
                        if let Some(id) = resolved_id
                            && let Some(mut e) = self.inodes.get_mut(InodeId(entry.ino))
                        {
                            e.inode_id = Some(id);
                            e.posix = crate::inode::layout_posix(&resolved);
                            e.layout = Some(resolved.clone());
                        }
                        let mut attr = self.make_file_attr(InodeId(entry.ino), &resolved)?;
                        // resolve_indirect returns the record's true link
                        // count; the redirect layout carries none.
                        attr.nlink = nlink;
                        attr
                    }
                    None => self.make_default_file_attr(InodeId(entry.ino)),
                }
            };
            entries.push(VfsDirEntryPlus {
                ino: entry.ino,
                kind: entry.kind,
                name: entry.name.clone(),
                offset: (offset + idx + 1) as u64,
                attr,
            });
        }

        Ok(entries)
    }

    pub fn vfs_statfs(&self) -> VfsStatfs {
        VfsStatfs {
            blocks: 1024 * 1024,
            bfree: if self.read_write { 512 * 1024 } else { 0 },
            bavail: if self.read_write { 512 * 1024 } else { 0 },
            files: 1024 * 1024,
            ffree: if self.read_write { 512 * 1024 } else { 0 },
            bsize: DEFAULT_BLOCK_SIZE,
            // POSIX NAME_MAX; Linux's VFS hard-caps any path
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
/// transient failure is acceptable; the kernel's block-on-demand
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

        // Override (blob_version > 1) blocks are padded to block_size on
        // disk; request the full block so the EC shard size matches, then
        // truncate to the logical content length (mirrors read_block_cached).
        let read_len = if layout.blob_version > 1 {
            (DEFAULT_BLOCK_SIZE as usize).max(block_content_len)
        } else {
            block_content_len
        };
        let (mut data, _checksum) = match backend
            .read_block(
                blob_guid,
                layout.blob_version,
                block_num,
                read_len,
                &trace_id,
            )
            .await
        {
            Ok(r) => r,
            Err(FsError::Rpc(rpc_client_common::RpcError::NotFound)) => {
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
        if data.len() > block_content_len {
            data = data.slice(0..block_content_len);
        }

        let _ = disk_cache
            .insert_block(blob_guid, block_num, layout.blob_version, &data)
            .await;
    }
}

/// Long-running writeback worker. Polls the queue every `poll_ms`,
/// drains pending intents, and fires NSS `put_inode` for each.
/// Spawned at FUSE init when `WritebackMode::Default` is configured;
/// runs until the process exits. Each intent ships as a single-op
/// `put_inode` RPC; the pipelining win comes from overlapping many such
/// round-trips concurrently, not from coalescing them.
///
/// Max concurrent `put_inode` RPCs per drained batch. Intents in a batch
/// are on distinct inodes (see `drain_pending`), so they publish in
/// parallel; the cap bounds in-flight RPCs against NSS.
const PUBLISH_CONCURRENCY: usize = 32;

fn spawn_writeback_worker(
    backend_cfg: Arc<BackendConfig>,
    queue: Arc<WritebackQueue>,
    poll_ms: u32,
) {
    let poll_dur = Duration::from_millis(poll_ms.max(1) as u64);
    compio_runtime::spawn(async move {
        // One backend per concurrent publish lane. StorageBackend has
        // RefCell-backed clients so independent futures must not share one
        // instance across awaits, especially when failover refresh mutates the
        // cached NSS client.
        let mut backends = Vec::with_capacity(PUBLISH_CONCURRENCY);
        for lane in 0..PUBLISH_CONCURRENCY {
            match StorageBackend::new(&backend_cfg) {
                Ok(b) => backends.push(b),
                Err(e) => {
                    tracing::warn!(
                        lane,
                        error = %e,
                        "writeback worker: failed to init backend; aborting"
                    );
                    return;
                }
            }
        }

        loop {
            compio_runtime::time::sleep(poll_dur).await;

            // Drain a batch of pending intents. The drainer flips them
            // to InFlight before returning so concurrent enqueues fall
            // into the next-cycle / backpressure path.
            let drained = queue.drain_pending(1024);
            if drained.is_empty() {
                continue;
            }

            // Publish independent intents concurrently. `drain_pending`
            // returns at most one generation per inode, so no two intents in
            // the batch touch the same inode; they are order-independent and
            // safe to fire together. Bounded chunks cap the fan-out on NSS so
            // a large batch cannot open thousands of in-flight RPCs at once.
            let queue = &queue;
            for chunk in drained.chunks(PUBLISH_CONCURRENCY) {
                futures::future::join_all(chunk.iter().enumerate().map(|(lane, intent)| {
                    let backend = &backends[lane];
                    async move {
                        let inode = intent.inode;
                        match publish_intent_with_retry(backend, intent).await {
                            Ok(_) => {
                                queue.mark_committed(&intent.s3_key, intent.generation, inode);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    key = %intent.s3_key,
                                    generation = intent.generation.0,
                                    error = %e,
                                    "writeback publish failed"
                                );
                                queue.mark_failed(&intent.s3_key, intent.generation, inode);
                            }
                        }
                    }
                }))
                .await;
            }
        }
    })
    .detach();
}

/// Releases a `block_prefix` hold when a directory rename returns or
/// unwinds, so a blocked prefix never outlives the rename that set it
/// (e.g. an early `?` on a drain or `rename_folder` error, or future
/// cancellation at unmount).
struct PrefixBlockGuard {
    writeback: Arc<WritebackQueue>,
    prefix: String,
}

impl Drop for PrefixBlockGuard {
    fn drop(&mut self) {
        self.writeback.unblock_prefix(&self.prefix);
    }
}

/// Collapses a release-flush cycle to `Done` when the flush task is dropped
/// before it can advance the cycle itself (the ring runtime hosting the
/// detached task is torn down at unmount). An orphaned non-`Done` cycle
/// would otherwise wedge `destroy`'s drain barrier until it times out. The
/// paired `FlushSnapshotGuard` has by then restored the buffer dirty, so
/// `destroy`'s `flush_open_dirty_handles` still republishes the data.
/// Disarmed on the normal paths, which advance the cycle explicitly.
struct ReleaseCycleGuard {
    writeback: Arc<WritebackQueue>,
    ino: InodeId,
    generation: Generation,
    armed: bool,
}

impl Drop for ReleaseCycleGuard {
    fn drop(&mut self) {
        if self.armed {
            self.writeback.advance_to_done(self.ino, self.generation);
        }
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

/// Absence-guarded create that tolerates an internally-retried RPC whose
/// first attempt committed but whose reply was lost. A blind `put_inode`
/// was idempotent under such a retry; the CAS-on-absence is not: the
/// re-sent attempt sees the key present and returns `CasConflict` against
/// the mount's own committed layout. On `CasConflict`, re-fetch and
/// compare bytes: if the stored inode byte-equals what we are publishing it
/// is our own commit (success); otherwise a peer won the name (a real
/// `CasConflict`). A peer's create never matches because the layout carries
/// a per-publish `version_id`.
async fn put_inode_create_idempotent(
    backend: &StorageBackend,
    key: &str,
    layout_bytes: Bytes,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    match backend
        .put_inode_cas(key, layout_bytes.clone(), Bytes::new(), trace_id)
        .await
    {
        Ok(_) => Ok(()),
        Err(FsError::CasConflict) => match backend.get_inode(key, trace_id).await {
            Ok(cur) => {
                let cur_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&cur, Vec::new())
                    .map(Bytes::from)
                    .map_err(FsError::from)?;
                if cur_bytes == layout_bytes {
                    Ok(())
                } else {
                    Err(FsError::CasConflict)
                }
            }
            // The key vanished between the CAS and this fetch (a concurrent
            // delete): treat as a lost race, not our own commit.
            Err(FsError::NotFound) => Err(FsError::CasConflict),
            Err(e) => Err(e),
        },
        Err(e) => Err(e),
    }
}

/// Ship one intent to NSS with bounded retries, so a transient backend
/// blip doesn't taint the inode and silently drop metadata the caller
/// already saw succeed.
async fn publish_intent_with_retry(
    backend: &StorageBackend,
    intent: &DrainableInodeIntent,
) -> Result<(), FsError> {
    const MAX_ATTEMPTS: u32 = 3;
    let mut result = Ok(());
    for attempt in 1..=MAX_ATTEMPTS {
        let trace_id = TraceId::new();
        result = match &intent.op {
            // Brand-new entry create. Guard on absence (empty expected
            // bytes) so a peer that created the same name during the
            // async window is not blindly overwritten; a lost race
            // surfaces as CasConflict, taints the inode, and the caller
            // re-looks-up the winner.
            WbInodeOp::PutInode { layout_bytes, .. } => {
                put_inode_create_idempotent(
                    backend,
                    &intent.s3_key,
                    layout_bytes.clone(),
                    &trace_id,
                )
                .await
            }
            WbInodeOp::SetPosix {
                posix,
                expected_layout_bytes,
                layout_bytes,
            } => {
                publish_set_posix(
                    backend,
                    &intent.s3_key,
                    posix,
                    expected_layout_bytes,
                    layout_bytes,
                    &trace_id,
                )
                .await
            }
        };
        match &result {
            Ok(()) => return Ok(()),
            // An absence-guarded create that hits CasConflict lost the
            // name to a peer; that is terminal (retrying can only lose
            // again), so surface it now to taint and re-lookup. SetPosix
            // keeps the outer retry: its own fold loop re-fetches fresh
            // state, so a later attempt can still win a bursty conflict.
            Err(FsError::CasConflict) if matches!(intent.op, WbInodeOp::PutInode { .. }) => {
                return result;
            }
            Err(e) if attempt < MAX_ATTEMPTS => {
                tracing::warn!(
                    key = %intent.s3_key,
                    attempt,
                    error = %e,
                    "writeback publish retrying"
                );
                compio_runtime::time::sleep(Duration::from_millis(20 * attempt as u64)).await;
            }
            Err(_) => {}
        }
    }
    result
}

/// Apply a posix-only update via CAS. Fast path: one `put_inode_cas`
/// guarded on the layout snapshot taken at enqueue. On conflict the
/// fresh layout is fetched and the posix folded onto it, so a
/// concurrent data publish (close-flush CAS) is never rolled back to
/// the enqueue-time blob state. A missing key means the entry was
/// deleted after the enqueue; the update is moot.
async fn publish_set_posix(
    backend: &StorageBackend,
    key: &str,
    posix: &PosixAttrs,
    expected: &Bytes,
    folded: &Bytes,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    match backend
        .put_inode_cas(key, folded.clone(), expected.clone(), trace_id)
        .await
    {
        Ok(_) => return Ok(()),
        Err(FsError::CasConflict) => {}
        Err(FsError::NotFound) => return Ok(()),
        Err(e) => return Err(e),
    }
    const MAX_CAS_RETRIES: u32 = 4;
    for _ in 0..MAX_CAS_RETRIES {
        let cur = match backend.get_inode(key, trace_id).await {
            Ok(l) => l,
            Err(FsError::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        };
        // A concurrent hardlink promotion moved the posix into the
        // shared record; follow the redirect and publish there instead
        // of folding metadata into the redirect row.
        if let ObjectState::Indirect(redirect) = &cur.state {
            return publish_set_posix_record(backend, redirect.inode_id, posix, trace_id).await;
        }
        let cur_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&cur, Vec::new())
            .map_err(FsError::from)?
            .into();
        let new_layout = crate::inode::layout_with_posix(cur, *posix);
        let new_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&new_layout, Vec::new())
            .map_err(FsError::from)?
            .into();
        match backend
            .put_inode_cas(key, new_bytes, cur_bytes, trace_id)
            .await
        {
            Ok(_) => return Ok(()),
            Err(FsError::CasConflict) => continue,
            Err(FsError::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
    Err(FsError::CasConflict)
}

async fn publish_set_posix_record(
    backend: &StorageBackend,
    inode_id: uuid::Uuid,
    posix: &PosixAttrs,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    const MAX_CAS_RETRIES: u32 = 4;
    let key = InodeRecord::key_for(inode_id);
    for _ in 0..MAX_CAS_RETRIES {
        let mut record = match backend.get_inode_record(inode_id, trace_id).await {
            Ok(record) => record,
            Err(FsError::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        };
        let old_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&record, Vec::new())
            .map_err(FsError::from)?
            .into();
        record.layout = crate::inode::layout_with_posix(record.layout.clone(), *posix);
        let new_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&record, Vec::new())
            .map_err(FsError::from)?
            .into();
        match backend
            .put_inode_cas(&key, new_bytes, old_bytes, trace_id)
            .await
        {
            Ok(_) => return Ok(()),
            Err(FsError::CasConflict) => continue,
            Err(FsError::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
    Err(FsError::CasConflict)
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
