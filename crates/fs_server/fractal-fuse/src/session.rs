use std::collections::HashSet;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use compio_runtime::Runtime;
use tracing::{debug, error, info, warn};

use crate::abi::*;
use crate::dispatch;
use crate::filesystem::Filesystem;
use crate::mount::{self, MountOptions};
use crate::ring::*;

/// Default max_write size (1MB).
const DEFAULT_MAX_WRITE: u32 = 1024 * 1024;

/// FUSE session managing the lifecycle from mount to shutdown.
pub struct Session {
    mount_options: MountOptions,
    queue_depth: u16,
}

impl Session {
    pub fn new(mount_options: MountOptions) -> Self {
        Self {
            mount_options,
            queue_depth: DEFAULT_QUEUE_DEPTH,
        }
    }

    pub fn queue_depth(mut self, depth: u16) -> Self {
        self.queue_depth = depth;
        self
    }

    /// Mount, negotiate FUSE_INIT, setup io_uring rings, and run until shutdown.
    /// This function blocks the calling thread.
    pub fn run<F: Filesystem>(self, fs: F, mount_path: &Path) -> io::Result<()> {
        info!("mounting FUSE filesystem at {:?}", mount_path);

        // Phase 1: Mount via fusermount3
        let fuse_fd = mount::fusermount(&self.mount_options, mount_path)?;
        info!("FUSE fd: {}", fuse_fd.as_raw_fd());

        let shutdown = Arc::new(AtomicBool::new(false));

        let result = self.run_inner(fs, &fuse_fd, shutdown);

        // Phase 4: Unmount
        info!("unmounting {:?}", mount_path);
        if let Err(e) = mount::fusermount_unmount(mount_path) {
            warn!("unmount failed: {}", e);
        }

        result
    }

    fn run_inner<F: Filesystem>(
        &self,
        fs: F,
        fuse_fd: &OwnedFd,
        shutdown: Arc<AtomicBool>,
    ) -> io::Result<()> {
        // Phase 2: FUSE_INIT over blocking /dev/fuse
        let (max_write, num_queues) = fuse_init(fuse_fd.as_fd(), &self.mount_options)?;
        let max_payload = max_write as usize;
        let queue_depth = self.queue_depth;

        info!(
            "FUSE_INIT done: max_write={}, queues={}, depth={}",
            max_write, num_queues, queue_depth
        );

        // Phase 3: Spawn per-CPU ring threads, each with its own compio Runtime
        let fs = Arc::new(fs);
        let fuse_raw_fd = fuse_fd.as_raw_fd();

        let mut threads = Vec::with_capacity(num_queues);
        for queue_id in 0..num_queues {
            let fs = fs.clone();
            let shutdown = shutdown.clone();

            let handle = thread::Builder::new()
                .name(format!("fuse-q{}", queue_id))
                .spawn(move || {
                    let mut cpus = HashSet::new();
                    cpus.insert(queue_id);

                    let rt = Runtime::builder()
                        .thread_affinity(cpus)
                        .build()
                        .expect("cannot create compio runtime");

                    rt.block_on(async {
                        if let Err(e) = run_queue(
                            fuse_raw_fd,
                            queue_id as u16,
                            queue_depth,
                            max_payload,
                            fs,
                            shutdown,
                        )
                        .await
                        {
                            error!("queue {} failed: {}", queue_id, e);
                        }
                    });
                })?;
            threads.push(handle);
        }

        // Wait for all ring threads to complete
        for handle in threads {
            handle.join().unwrap_or_else(|e| {
                error!("ring thread panicked: {:?}", e);
            });
        }

        Ok(())
    }
}

/// Run a single queue on a compio runtime thread.
/// Spawns an independent task per ring entry so they can process requests
/// concurrently (register blocks until the kernel sends a request, so
/// sequential registration would deadlock).
async fn run_queue<F: Filesystem>(
    fuse_raw_fd: i32,
    queue_id: u16,
    queue_depth: u16,
    max_payload: usize,
    fs: Arc<F>,
    shutdown: Arc<AtomicBool>,
) -> io::Result<()> {
    // Register fuse fd with this thread's io_uring
    compio_runtime::register_files(&[fuse_raw_fd])?;

    debug!(
        "queue {}: registered fuse fd, allocating {} entries",
        queue_id, queue_depth
    );

    // Allocate page-aligned buffers
    let entries = allocate_ring_entries(queue_depth, max_payload)?;

    // Spawn independent task per entry: each registers with the kernel and
    // then loops dispatching requests. This avoids deadlock since REGISTER
    // blocks until the kernel delivers a request to that entry.
    let mut handles = Vec::with_capacity(entries.len());
    for mut entry in entries {
        let fs = fs.clone();
        let shutdown = shutdown.clone();
        let handle =
            compio_runtime::spawn(
                async move { run_entry(queue_id, &mut entry, &*fs, &shutdown).await },
            );
        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!("entry task failed: {}", e),
            Err(e) => error!("entry task panicked: {:?}", e),
        }
    }

    compio_runtime::unregister_files()?;
    Ok(())
}

/// Run a single ring entry: register with the kernel, then loop
/// dispatching requests and committing responses.
async fn run_entry<F: Filesystem>(
    queue_id: u16,
    entry: &mut RingEntry,
    fs: &F,
    shutdown: &AtomicBool,
) -> io::Result<()> {
    // Register this entry's buffers with the kernel (blocks until first request)
    let result = compio_runtime::submit(FuseRegister::new(entry, queue_id)).await;
    if let Err(e) = result.0 {
        return Err(io::Error::other(format!("FUSE register failed: {}", e)));
    }

    // Process requests in a loop: dispatch -> commit response + fetch next
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let needs_response = dispatch::dispatch(fs, entry).await;

        if needs_response.is_none() {
            // FORGET-type op: re-register without sending a response
            let result = compio_runtime::submit(FuseRegister::new(entry, queue_id)).await;
            if let Err(e) = result.0 {
                error!("FUSE re-register failed: {}", e);
                return Err(io::Error::other(e.to_string()));
            }
            continue;
        }

        // Commit response + fetch next request
        let commit_id = entry.commit_id();
        let result = compio_runtime::submit(FuseCommitAndFetch::new(queue_id, commit_id)).await;
        if let Err(e) = result.0 {
            error!("FUSE commit failed: {}", e);
            return Err(io::Error::other(e.to_string()));
        }
    }

    Ok(())
}

/// Perform FUSE_INIT handshake over blocking /dev/fuse.
/// Returns (max_write, num_queues).
fn fuse_init(fuse_fd: BorrowedFd<'_>, opts: &MountOptions) -> io::Result<(u32, usize)> {
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
        want_flags |= FUSE_HANDLE_KILLPRIV as u64;
    }
    if opts.write_back {
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

    let max_write = DEFAULT_MAX_WRITE;
    let num_cpus = num_cpus();

    let out_hdr = fuse_out_header {
        len: (std::mem::size_of::<fuse_out_header>() + std::mem::size_of::<fuse_init_out>()) as u32,
        error: 0,
        unique,
    };

    let init_out = fuse_init_out {
        major: FUSE_KERNEL_VERSION,
        minor: FUSE_KERNEL_MINOR_VERSION,
        max_readahead: init_in.max_readahead,
        flags: (want_flags & 0xFFFF_FFFF) as u32,
        max_background: 16,
        congestion_threshold: 12,
        max_write,
        time_gran: 1,
        max_pages: (max_write / 4096).max(1) as u16,
        map_alignment: 0,
        flags2: ((want_flags >> 32) & 0xFFFF_FFFF) as u32,
        max_stack_depth: 0,
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
        "FUSE_INIT reply sent: flags=0x{:016x}, max_write={}, cpus={}",
        want_flags, max_write, num_cpus
    );

    Ok((max_write, num_cpus))
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
