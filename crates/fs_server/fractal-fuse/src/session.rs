use std::collections::HashSet;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::panic::{self, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;

use compio_runtime::{Runtime, register_files, unregister_files};
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::abi::*;
use crate::dispatch;
use crate::filesystem::{Filesystem, FsResult};
use crate::mount::{self, MountOptions};
use crate::ring::*;
use crate::types::{ReplyInit, Request};

/// Hard ceiling for the FUSE write payload size this transport will allocate.
const MAX_WRITE_SIZE: u32 = 16 * 1024 * 1024;

/// Clonable handle used to request graceful shutdown of a running session.
#[derive(Clone, Debug)]
pub struct SessionShutdownHandle {
    token: CancellationToken,
}

impl SessionShutdownHandle {
    /// Request [`Session::run`] to stop after in-flight requests complete.
    pub fn shutdown(&self) {
        self.token.cancel();
    }

    /// Returns true after shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.token.is_cancelled()
    }
}

/// FUSE session managing the lifecycle from mount to shutdown.
pub struct Session {
    mount_path: PathBuf,
    mount_options: MountOptions,
    fd: Arc<OwnedFd>,
    queue_depth: u16,
    worker_count: usize,
    shutdown: CancellationToken,
}

impl Session {
    pub fn new(mount_path: PathBuf, mount_options: MountOptions) -> io::Result<Self> {
        info!("mounting FUSE filesystem at {:?}", mount_path);
        let fd = mount::fusermount(&mount_options, &mount_path)?;
        info!("FUSE fd: {}", fd.as_raw_fd());
        Ok(Self {
            mount_path,
            mount_options,
            fd: Arc::new(fd),
            queue_depth: DEFAULT_QUEUE_DEPTH,
            worker_count: num_possible_cpus(),
            shutdown: CancellationToken::new(),
        })
    }

    /// Returns a handle that can request graceful session shutdown.
    pub fn shutdown_handle(&self) -> SessionShutdownHandle {
        SessionShutdownHandle {
            token: self.shutdown.clone(),
        }
    }

    /// Returns a shared owning handle to the kernel `/dev/fuse` fd backing
    /// this session.
    ///
    /// Use this to construct a [`FuseNotifier`](crate::FuseNotifier) (e.g.
    /// `FuseNotifier::from(session.fuse_fd())`) or perform raw FUSE-fd
    /// operations (e.g. passthrough ioctls) outside of the [`Filesystem`]
    /// trait. The returned `Arc` keeps the fd open for as long as any
    /// clone exists, so notifiers remain usable across the full lifetime
    /// of the mount even after the `Session` is consumed by
    /// [`Session::run`].
    pub fn fuse_fd(&self) -> Arc<OwnedFd> {
        self.fd.clone()
    }

    /// Number of io_uring entries per queue (defaults to `DEFAULT_QUEUE_DEPTH` = 256).
    pub fn with_queue_depth(mut self, depth: u16) -> Self {
        self.queue_depth = depth;
        self
    }

    /// Number of worker threads driving io_uring rings (defaults to the
    /// number of possible CPUs).
    ///
    /// The kernel always allocates one fuse-uring queue per possible CPU
    /// and routes each request to qid = task_cpu() of the caller. So we
    /// must still register entries for every qid in `0..num_possible_cpus`,
    /// otherwise requests from un-covered CPUs hang forever. When
    /// `worker_count < num_possible_cpus`, qids are distributed across
    /// workers in a stride so every qid is covered. Capped at
    /// `num_possible_cpus` internally; setting more is a no-op.
    pub fn with_worker_count(mut self, workers: usize) -> Self {
        self.worker_count = workers;
        self
    }

    /// Negotiate FUSE_INIT, setup io_uring rings, and run until shutdown.
    /// This function blocks the calling thread.
    pub fn run<F: Filesystem>(self, fs: F) -> io::Result<()> {
        let result = self.run_inner(fs);

        if let Ok(true) | Err(_) = result {
            // Phase 4: Unmount
            info!("unmounting {:?}", self.mount_path);
            if let Err(e) = mount::fusermount_unmount(&self.mount_path) {
                warn!("unmount failed: {}", e);
            }
        }

        result.map(|_| ())
    }

    /// Returns whether we think the filesystem is still mounted
    fn run_inner<F: Filesystem>(&self, fs: F) -> io::Result<bool> {
        let fs = Arc::new(fs);
        // Phase 2: read kernel FUSE_INIT, run fs.init() on the lifecycle
        // thread, then write the kernel reply.
        let parsed = read_fuse_init(self.fd.as_fd())?;
        let init_request = parsed.request;

        // The lifecycle thread hosts a compio runtime dedicated to the
        // filesystem's lifecycle hooks (init, destroy). It stays alive for
        // the whole mount so any background tasks those hooks spawn (e.g.
        // the disk-cache evictor's 60s timer) keep being polled. /dev/fuse
        // never delivers FUSE_DESTROY (see abi.rs FUSE_DESTROY note), so
        // destroy is signaled by the LifecycleGuard drop below.
        let destroy_signal = CancellationToken::new();
        let (init_tx, init_rx) = mpsc::sync_channel::<FsResult<ReplyInit>>(1);
        let lifecycle_fs = fs.clone();
        let lifecycle_destroy = destroy_signal.clone();
        let lifecycle_thread = thread::Builder::new()
            .name("fuse-lifecycle".to_string())
            .spawn(move || -> io::Result<()> {
                let rt = Runtime::builder().build().map_err(|e| {
                    error!("failed to create lifecycle runtime: {e}");
                    e
                })?;
                rt.block_on(async {
                    match lifecycle_fs.init(init_request).await {
                        Ok(reply) => {
                            // Send reply before awaiting destroy so the
                            // main thread can resume mount setup while
                            // background tasks keep running here.
                            let _ = init_tx.send(Ok(reply));
                            lifecycle_destroy.cancelled().await;
                            lifecycle_fs.destroy().await;
                        }
                        Err(errno) => {
                            // init failed: no destroy. Thread exits, the
                            // runtime drops, any spawned tasks are cancelled.
                            let _ = init_tx.send(Err(errno));
                        }
                    }
                });
                Ok(())
            })?;

        let reply = match init_rx.recv() {
            Ok(Ok(r)) => r,
            Ok(Err(errno)) => {
                let _ = lifecycle_thread.join();
                return Err(io::Error::other(format!(
                    "fs.init() failed: errno {}",
                    errno
                )));
            }
            Err(_) => {
                let join_result = lifecycle_thread.join();
                return Err(io::Error::other(format!(
                    "lifecycle thread exited before init completed: {:?}",
                    join_result
                )));
            }
        };

        // From here on, fs.destroy() must run on every exit path. The
        // guard's Drop cancels the destroy signal and joins the lifecycle
        // thread, which runs destroy on its own runtime.
        let _lifecycle = LifecycleGuard {
            token: destroy_signal,
            thread: Some(lifecycle_thread),
        };

        // Cap the FS-requested max_write at the transport ceiling so ring
        // buffer allocation (max_payload below) cannot underflow what we
        // advertise to the kernel.
        let max_write = reply.max_write.min(MAX_WRITE_SIZE);
        write_fuse_init_reply(
            self.fd.as_fd(),
            &parsed,
            max_write,
            &reply,
            &self.mount_options,
        )?;

        let max_payload = max_write as usize;
        let queue_depth = self.queue_depth;

        // The kernel allocates `nr_queues = num_possible_cpus()` fuse-uring
        // queues and routes each request to qid = task_cpu(); when
        // task_cpu() >= nr_queues (non-contiguous possible mask) the
        // dispatch path falls back to qid 0. We must register at least one
        // entry for every qid in `0..num_qids`, otherwise ops from
        // un-covered qids hang waiting for an entry. num_qids is the
        // kernel's count, not max_id+1: that's what the kernel actually
        // allocates and what the dispatch path bounds-checks against.
        let num_qids = num_possible_cpus();
        let workers = self.worker_count.min(num_qids).max(1);

        info!(
            "FUSE_INIT done: max_write={}, workers={}, qids={}, depth={}",
            max_write, workers, num_qids, queue_depth
        );

        // Phase 3: Spawn `workers` threads, each running a compio Runtime
        // that drives a stride of qids: worker `w` handles
        // `qid in {w, w + workers, w + 2*workers, ...}` while `qid < num_qids`.
        let mut threads = Vec::with_capacity(workers);
        let connected = Arc::new(AtomicBool::new(true));
        // Set by any worker that exits with an Err so the session returns
        // Err on this path instead of swallowing the failure as a clean
        // shutdown.
        let any_failed = Arc::new(AtomicBool::new(false));
        let fuse_raw_fd = self.fd.as_raw_fd();
        for worker_id in 0..workers {
            let qids: Vec<u16> = (worker_id..num_qids)
                .step_by(workers)
                .map(|q| q as u16)
                .collect();
            let fs = fs.clone();
            let shutdown = self.shutdown.clone();
            let connected = connected.clone();
            let any_failed = any_failed.clone();

            let spawn_result = thread::Builder::new()
                .name(format!("fuse-w{}", worker_id))
                .spawn(move || {
                    let result = panic::catch_unwind(AssertUnwindSafe(|| {
                        let mut cpus = HashSet::new();
                        // Pin to the first qid's CPU as a hint; other qids
                        // serviced by this worker are owned by other CPUs but
                        // routing is per-request, not thread-bound.
                        cpus.insert(worker_id);

                        let rt = match Runtime::builder().thread_affinity(cpus).build() {
                            Ok(rt) => rt,
                            Err(e) => {
                                error!("worker {} failed to create runtime: {}", worker_id, e);
                                any_failed.store(true, Ordering::Relaxed);
                                shutdown.cancel();
                                return;
                            }
                        };

                        let shutdown_for_run = shutdown.clone();
                        rt.block_on(async {
                            match run_worker(
                                fuse_raw_fd,
                                &qids,
                                queue_depth,
                                max_payload,
                                fs,
                                shutdown_for_run,
                            )
                            .await
                            {
                                Ok(worker_connected) => {
                                    connected.fetch_and(worker_connected, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    error!("worker {} failed: {}", worker_id, e);
                                    any_failed.store(true, Ordering::Relaxed);
                                    // Wake up peer workers: their REGISTER ops
                                    // block on kernel-side request delivery. If
                                    // this worker's qids are now uncovered, the
                                    // kernel can route requests there and stall
                                    // the mount; if any qid this worker owned
                                    // was the only registrant, others won't
                                    // unblock without an external signal.
                                    // Cancel the shared shutdown so every
                                    // worker exits its REGISTER and lets the
                                    // session unwind cleanly.
                                    shutdown.cancel();
                                }
                            }
                        });
                    }));

                    if let Err(e) = result {
                        if let Some(msg) = e
                            .downcast_ref::<String>()
                            .map(|x| &**x)
                            .or_else(|| e.downcast_ref::<&str>().copied())
                        {
                            error!("worker {} panicked: {}", worker_id, msg);
                        } else {
                            error!("worker {} panicked", worker_id);
                        }
                        any_failed.store(true, Ordering::Relaxed);
                        shutdown.cancel();
                    }
                });

            match spawn_result {
                Ok(handle) => threads.push(handle),
                Err(e) => {
                    // Partial-spawn cleanup: already-started workers hold
                    // clones of `fs` and the raw FUSE fd. Cancel the
                    // shutdown token to wind them down, then join them
                    // before returning. This must complete before the
                    // LifecycleGuard's drop runs fs.destroy(), or destroy
                    // would race with live worker threads.
                    error!(
                        "failed to spawn worker {} (after starting {}): {}",
                        worker_id,
                        threads.len(),
                        e
                    );
                    self.shutdown.cancel();
                    for h in threads {
                        h.join().unwrap_or_else(|p| {
                            error!("ring thread panicked during cleanup: {:?}", p);
                        });
                    }
                    return Err(e);
                }
            }
        }

        // Wait for all ring threads to complete. Panics inside the worker
        // closure are caught via panic::catch_unwind above (where they're
        // recorded in `any_failed` and trigger shutdown.cancel()), so
        // handle.join() should always return Ok here.
        for handle in threads {
            let _ = handle.join();
        }

        // _lifecycle drops here, signaling destroy and joining the thread.
        if any_failed.load(Ordering::Relaxed) {
            return Err(io::Error::other("fuse worker failed"));
        }
        Ok(connected.load(Ordering::Relaxed))
    }
}

/// RAII guard that drives `Filesystem::destroy` and joins the lifecycle
/// thread when dropped. Constructed only after `init` succeeded, so the
/// destroy hook runs on every post-init exit path (including panics and
/// early returns from later setup steps).
struct LifecycleGuard {
    token: CancellationToken,
    thread: Option<thread::JoinHandle<io::Result<()>>>,
}

impl Drop for LifecycleGuard {
    fn drop(&mut self) {
        self.token.cancel();
        if let Some(t) = self.thread.take() {
            match t.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => error!("lifecycle thread error: {}", e),
                Err(e) => error!("lifecycle thread panicked: {:?}", e),
            }
        }
    }
}

/// Run a single queue on a compio runtime thread.
/// Spawns an independent task per ring entry so they can process requests
/// concurrently (register blocks until the kernel sends a request, so
/// sequential registration would deadlock).
async fn run_worker<F: Filesystem>(
    fuse_raw_fd: i32,
    qids: &[u16],
    queue_depth: u16,
    max_payload: usize,
    fs: Arc<F>,
    shutdown: CancellationToken,
) -> io::Result<bool> {
    // Register fuse fd with this worker's io_uring (once per thread).
    register_files(&[fuse_raw_fd])?;

    debug!(
        "worker registered fuse fd, allocating {} entries per qid for qids {:?}",
        queue_depth, qids
    );

    // Spawn independent task per (qid, entry): each registers with the
    // kernel and then loops dispatching requests. This avoids deadlock
    // since REGISTER blocks until the kernel delivers a request to that
    // entry. All tasks share the worker's single compio runtime.
    //
    // We collect handles into a FuturesUnordered and await in completion
    // order rather than spawn order: REGISTER blocks indefinitely, so a
    // later entry that fails or panics while an earlier one is still
    // parked must be observable immediately. Spawn-order draining would
    // miss it, leaving shutdown un-cancelled and the mount hung.
    let handles: FuturesUnordered<_> = FuturesUnordered::new();
    for &qid in qids {
        // Allocate page-aligned buffers; one set per qid.
        let entries = allocate_ring_entries(queue_depth, max_payload)?;
        for mut entry in entries {
            let fs = fs.clone();
            let shutdown = shutdown.clone();
            handles.push(compio_runtime::spawn(async move {
                run_entry(qid, &mut entry, &*fs, &shutdown).await
            }));
        }
    }

    let mut connected = true;
    let mut failed = false;
    let mut handles = handles;
    while let Some(result) = handles.next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.kind() == io::ErrorKind::NotConnected => {
                connected = false;
            }
            Ok(Err(e)) => {
                error!("entry task failed: {}", e);
                failed = true;
                shutdown.cancel();
            }
            Err(e) => {
                // `e` is compio's JoinError; its Display reports whether the
                // task was cancelled or panicked. The panic payload message
                // itself is already emitted to stderr by the default panic
                // hook when the task unwinds, so there's no need to downcast.
                error!("entry task aborted: {}", e);
                failed = true;
                shutdown.cancel();
            }
        }
    }

    unregister_files()?;
    if failed {
        Err(io::Error::other("fuse entry task failed"))
    } else {
        Ok(connected)
    }
}

/// Run a single ring entry: register with the kernel, then loop
/// dispatching requests and committing responses.
async fn run_entry<F: Filesystem>(
    queue_id: u16,
    entry: &mut RingEntry,
    fs: &F,
    shutdown: &CancellationToken,
) -> io::Result<()> {
    // Register this entry's buffers with the kernel (blocks until first request)
    if submit_cancelable(shutdown, "register", FuseRegister::new(entry, queue_id)).await? {
        return Ok(());
    }

    // Process requests in a loop: dispatch -> commit response + fetch next
    loop {
        let (needs_response, panic_result) = dispatch::dispatch(fs, entry).await;

        if let Err(e) = panic_result {
            if let Some(msg) = e
                .downcast_ref::<String>()
                .map(|x| &**x)
                .or_else(|| e.downcast_ref::<&str>().copied())
            {
                error!("filesystem op panicked: {}", msg);
            } else {
                error!("filesystem op panicked");
            }
        }

        if needs_response.is_none() {
            // FORGET-type op: re-register without sending a response
            if submit_cancelable(shutdown, "re-register", FuseRegister::new(entry, queue_id))
                .await?
            {
                break;
            }
            continue;
        }

        // Commit response + fetch next request
        let commit_id = entry.commit_id();
        if submit_cancelable(
            shutdown,
            "commit",
            FuseCommitAndFetch::new(queue_id, commit_id),
        )
        .await?
        {
            break;
        }
    }

    Ok(())
}

/// Returns whether to shut down
async fn submit_cancelable<T: compio_driver::OpCode + 'static>(
    token: &CancellationToken,
    op_name: &'static str,
    op: T,
) -> io::Result<bool> {
    let result = token.run_until_cancelled(compio_runtime::submit(op)).await;
    match result.map(|x| x.0) {
        Some(Ok(_)) => Ok(false),
        Some(Err(e)) if e.kind() == io::ErrorKind::NotConnected => Err(e),
        Some(Err(e)) => {
            error!("FUSE {op_name} failed: {e}");
            Err(io::Error::other(e.to_string()))
        }
        // Shutting down
        None => Ok(true),
    }
}

/// Parsed FUSE_INIT request from the kernel, retained until the reply is
/// sent so we can echo back `unique` and intersect capability flags.
struct ParsedFuseInit {
    unique: u64,
    request: Request,
    kernel_flags: u64,
    kernel_max_readahead: u32,
}

/// Read and parse the kernel's FUSE_INIT request over blocking /dev/fuse.
fn read_fuse_init(fuse_fd: BorrowedFd<'_>) -> io::Result<ParsedFuseInit> {
    let mut buf = vec![0u8; 8192];
    let n = nix::unistd::read(fuse_fd, &mut buf).map_err(io::Error::from)?;
    if n < std::mem::size_of::<fuse_in_header>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "FUSE_INIT read too short",
        ));
    }

    let in_hdr = unsafe { &*(buf.as_ptr() as *const fuse_in_header) };
    if in_hdr.opcode != FUSE_INIT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected FUSE_INIT, got opcode {}", in_hdr.opcode),
        ));
    }

    let unique = in_hdr.unique;
    let request = Request {
        unique,
        uid: in_hdr.uid,
        gid: in_hdr.gid,
        pid: in_hdr.pid,
    };

    let in_body_offset = std::mem::size_of::<fuse_in_header>();
    let init_in = unsafe { &*(buf.as_ptr().add(in_body_offset) as *const fuse_init_in) };

    let major = init_in.major;
    let minor = init_in.minor;
    info!(
        "FUSE_INIT: kernel version {}.{}, max_readahead={}",
        major, minor, init_in.max_readahead
    );

    if major != FUSE_KERNEL_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported FUSE protocol version {}.{} (want {}.x)",
                major, minor, FUSE_KERNEL_VERSION
            ),
        ));
    }

    // Reconstruct 64-bit flags: flags | (flags2 << 32)
    let kernel_flags = (init_in.flags as u64) | ((init_in.flags2 as u64) << 32);
    debug!("kernel capabilities: 0x{:016x}", kernel_flags);

    if kernel_flags & FUSE_OVER_IO_URING == 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "kernel does not support FUSE_OVER_IO_URING (requires Linux 6.14+)",
        ));
    }

    Ok(ParsedFuseInit {
        unique,
        request,
        kernel_flags,
        kernel_max_readahead: init_in.max_readahead,
    })
}

/// Send the FUSE_INIT reply: intersect the kernel's capabilities with
/// what the mount options request, and use the negotiated init values for
/// `max_write`, readahead, and background tuning.
fn write_fuse_init_reply(
    fuse_fd: BorrowedFd<'_>,
    parsed: &ParsedFuseInit,
    max_write: u32,
    reply: &ReplyInit,
    opts: &MountOptions,
) -> io::Result<()> {
    let kernel_flags = parsed.kernel_flags;

    // Build capability flags
    let mut want_flags: u64 = FUSE_OVER_IO_URING;
    want_flags |= FUSE_INIT_EXT as u64;
    want_flags |= FUSE_ASYNC_READ as u64;
    want_flags |= FUSE_BIG_WRITES as u64;
    want_flags |= FUSE_AUTO_INVAL_DATA as u64;
    want_flags |= FUSE_DO_READDIRPLUS as u64;
    want_flags |= FUSE_READDIRPLUS_AUTO as u64;
    want_flags |= FUSE_ASYNC_DIO as u64;
    want_flags |= FUSE_PARALLEL_DIROPS as u64;
    want_flags |= FUSE_MAX_PAGES as u64;
    want_flags |= FUSE_ATOMIC_O_TRUNC as u64;
    want_flags |= FUSE_SETXATTR_EXT as u64;

    // Lock advertisement is opt-in: enabling these flags routes all
    // fcntl/flock calls to userspace, where they ENOSYS unless the
    // Filesystem impl provides getlk/setlk/flock. Leaving them off lets
    // the kernel handle local-only locks.
    if opts.posix_locks {
        want_flags |= FUSE_POSIX_LOCKS as u64;
    }
    if opts.flock_locks {
        want_flags |= FUSE_FLOCK_LOCKS as u64;
    }

    if opts.dont_mask {
        want_flags |= FUSE_DONT_MASK as u64;
    }
    if opts.no_open_support {
        want_flags |= FUSE_NO_OPEN_SUPPORT as u64;
    }
    if opts.no_open_dir_support {
        want_flags |= FUSE_NO_OPENDIR_SUPPORT as u64;
    }
    if opts.handle_killpriv {
        // V1 (bit 19) only covers FUSE_WRITE -- the kernel still
        // clears suid/sgid in-kernel for SETATTR/OPEN paths and
        // never lifts the clear up to userspace, so a non-root
        // chown(2) on a setuid file leaves our daemon's mode
        // unchanged. V2 (bit 28, Linux 5.12+) extends the contract
        // to SETATTR and OPEN: the kernel sets FATTR_KILL_SUIDGID
        // on those requests so userspace handles the clear.
        // Advertise both so a non-root chown's SETATTR carries the
        // flag; kernels that only know V1 will mask V2 out during
        // negotiation, leaving V1 (which is enough for the
        // non-owner-write contract) intact.
        want_flags |= FUSE_HANDLE_KILLPRIV as u64;
        want_flags |= FUSE_HANDLE_KILLPRIV_V2 as u64;
    }
    if opts.passthrough {
        // FUSE_PASSTHROUGH and FUSE_WRITEBACK_CACHE are mutually exclusive
        want_flags |= FUSE_PASSTHROUGH;
    } else if opts.write_back {
        want_flags |= FUSE_WRITEBACK_CACHE as u64;
    }

    // Only enable flags the kernel supports
    want_flags &= kernel_flags;

    if want_flags & FUSE_OVER_IO_URING == 0 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "FUSE_OVER_IO_URING not supported after negotiation",
        ));
    }

    // The kernel proposes max_readahead; the FS can only lower it.
    let max_readahead = parsed.kernel_max_readahead.min(reply.max_readahead);

    let out_hdr = fuse_out_header {
        len: (std::mem::size_of::<fuse_out_header>() + std::mem::size_of::<fuse_init_out>()) as u32,
        error: 0,
        unique: parsed.unique,
    };

    let init_out = fuse_init_out {
        major: FUSE_KERNEL_VERSION,
        minor: FUSE_KERNEL_MINOR_VERSION,
        max_readahead,
        flags: (want_flags & 0xFFFF_FFFF) as u32,
        max_background: reply.max_background,
        congestion_threshold: reply.congestion_threshold,
        max_write,
        time_gran: 1,
        max_pages: (max_write / 4096).max(1) as u16,
        map_alignment: 0,
        flags2: ((want_flags >> 32) & 0xFFFF_FFFF) as u32,
        max_stack_depth: if opts.passthrough { 1 } else { 0 },
        request_timeout: 0,
        unused: [0; 11],
    };

    let hdr_bytes = unsafe {
        std::slice::from_raw_parts(
            &out_hdr as *const _ as *const u8,
            std::mem::size_of::<fuse_out_header>(),
        )
    };
    let body_bytes = unsafe {
        std::slice::from_raw_parts(
            &init_out as *const _ as *const u8,
            std::mem::size_of::<fuse_init_out>(),
        )
    };

    let mut response = Vec::with_capacity(hdr_bytes.len() + body_bytes.len());
    response.extend_from_slice(hdr_bytes);
    response.extend_from_slice(body_bytes);

    nix::unistd::write(fuse_fd, &response).map_err(io::Error::from)?;

    info!(
        "FUSE_INIT reply sent: flags=0x{:016x}, max_write={}",
        want_flags, max_write
    );

    Ok(())
}

/// Returns the kernel's `num_possible_cpus()` count, i.e. how many fuse-uring
/// qids the kernel will allocate (`ring->nr_queues`). This is a *count*, not
/// a max-id, mirroring the kernel definition (`cpumask_weight(cpu_possible_mask)`).
/// On non-contiguous masks like `0-3,7-11` the kernel allocates 9 queues
/// (indexed `0..=8`) and the dispatch path falls back to `qid = 0` when
/// `task_cpu(current) >= nr_queues`, so registering up to the count is both
/// necessary and sufficient.
///
/// `std::thread::available_parallelism()` is intentionally NOT used here:
/// it reports the process's allowed parallelism, which can be shrunk by
/// cpusets, taskset, container CPU limits, and similar mechanisms. The
/// kernel's fuse-uring still allocates queues against the full possible
/// CPU range, so under-registering would leave qids un-covered and
/// requests routed there would hang.
fn num_possible_cpus() -> usize {
    match std::fs::read_to_string("/sys/devices/system/cpu/possible") {
        Ok(s) => parse_cpu_list_count(s.trim()).unwrap_or_else(fallback_cpus),
        Err(_) => fallback_cpus(),
    }
}

fn fallback_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Parse a Linux cpu list (e.g. `"0-23"`, `"0-3,7-11"`, `"5"`) into the
/// total CPU count, matching the kernel's `cpumask_weight(cpu_possible_mask)`.
/// Returns `None` if the input is malformed or empty.
fn parse_cpu_list_count(s: &str) -> Option<usize> {
    if s.is_empty() {
        return None;
    }
    let mut count: usize = 0;
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return None;
        }
        let n = match part.split_once('-') {
            Some((lo, hi)) => {
                let lo: usize = lo.parse().ok()?;
                let hi: usize = hi.parse().ok()?;
                if hi < lo {
                    return None;
                }
                hi - lo + 1
            }
            None => {
                part.parse::<usize>().ok()?;
                1
            }
        };
        count = count.checked_add(n)?;
    }
    if count == 0 { None } else { Some(count) }
}

#[cfg(test)]
mod tests {
    use super::parse_cpu_list_count;

    #[test]
    fn parse_contiguous() {
        assert_eq!(parse_cpu_list_count("0-23"), Some(24));
    }

    #[test]
    fn parse_single() {
        assert_eq!(parse_cpu_list_count("0"), Some(1));
        assert_eq!(parse_cpu_list_count("5"), Some(1));
    }

    #[test]
    fn parse_non_contiguous() {
        // 0-3 = 4 CPUs, 7-11 = 5 CPUs. Kernel's nr_queues = 9, so qids
        // 0..=8 cover task_cpu() <= 8 directly. Only task_cpu() in {9,
        // 10, 11} (>= 9) falls back to qid 0.
        assert_eq!(parse_cpu_list_count("0-3,7-11"), Some(9));
    }

    #[test]
    fn parse_malformed() {
        assert_eq!(parse_cpu_list_count(""), None);
        assert_eq!(parse_cpu_list_count("abc"), None);
        assert_eq!(parse_cpu_list_count("0-x"), None);
        assert_eq!(parse_cpu_list_count("5-2"), None);
        assert_eq!(parse_cpu_list_count("0,,3"), None);
    }
}
