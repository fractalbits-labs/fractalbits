//! Verifies that `FuseNotifier::inval_entry` actually reaches the kernel and
//! drops the cached dentry, forcing a fresh LOOKUP on the next access.
//!
//! This is a regression test for the notification `error`-code sign: FUSE
//! notifications carry their code in `fuse_out_header.error` as a POSITIVE
//! value (the kernel passes `oh.error` straight into `fuse_notify()`, whose
//! switch matches the positive `FUSE_NOTIFY_*` constants). Sending the code
//! negated makes the kernel hit `default: -EINVAL` and silently drop the
//! invalidation, so a stale dentry would never be re-looked-up. With a long
//! entry TTL the only thing that can force a second LOOKUP is the kernel
//! honoring our inval_entry message.
//!
//! Like the readdir-panic test this mounts a real FUSE-over-io_uring
//! filesystem, so it needs an unprivileged `fusermount3` and a kernel with
//! `fuse` io_uring support. When that isn't available the test skips.

use std::ffi::OsStr;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use fractal_fuse::abi::FUSE_ROOT_ID;
use fractal_fuse::{
    ENOENT, FileAttr, FileHandleId, FileType, Filesystem, FsResult, FuseNotifier, InodeId,
    MountOptions, ReplyAttr, ReplyEntry, Request, Session, Timestamp,
};

const WATCHED_INO: u64 = 2;
const WATCHED_NAME: &str = "watched";
// Long TTL: once the dentry is cached, nothing but an explicit invalidation
// (or unmount) should provoke another LOOKUP within the test's lifetime.
const TTL: Duration = Duration::from_secs(600);

fn now_ts() -> Timestamp {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp::new(d.as_secs(), d.subsec_nanos())
}

fn dir_attr(ino: u64) -> FileAttr {
    let ts = now_ts();
    FileAttr {
        ino,
        size: 0,
        blocks: 0,
        atime: ts,
        mtime: ts,
        ctime: ts,
        mode: FileType::Directory.to_mode() | 0o755,
        nlink: 2,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 512,
    }
}

fn watched_attr() -> FileAttr {
    let ts = now_ts();
    FileAttr {
        ino: WATCHED_INO,
        size: 0,
        blocks: 0,
        atime: ts,
        mtime: ts,
        ctime: ts,
        mode: FileType::RegularFile.to_mode() | 0o444,
        nlink: 1,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 512,
    }
}

/// Serves a single file `watched` under the root and counts every LOOKUP the
/// kernel issues for that name. The count is shared with the test via an
/// `Arc<AtomicU64>` so it can observe when the kernel re-resolves the dentry.
struct CountingFs {
    watched_lookups: Arc<AtomicU64>,
}

impl Filesystem for CountingFs {
    async fn lookup(&self, _req: Request, parent: InodeId, name: &OsStr) -> FsResult<ReplyEntry> {
        if parent.0 == FUSE_ROOT_ID && name == WATCHED_NAME {
            self.watched_lookups.fetch_add(1, Ordering::SeqCst);
            Ok(ReplyEntry {
                ttl: TTL,
                attr: watched_attr(),
                generation: 0,
            })
        } else {
            Err(ENOENT)
        }
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: InodeId,
        _fh: Option<FileHandleId>,
        _flags: u32,
    ) -> FsResult<ReplyAttr> {
        match inode.0 {
            FUSE_ROOT_ID => Ok(ReplyAttr {
                ttl: TTL,
                attr: dir_attr(FUSE_ROOT_ID),
            }),
            WATCHED_INO => Ok(ReplyAttr {
                ttl: TTL,
                attr: watched_attr(),
            }),
            _ => Err(ENOENT),
        }
    }
}

/// Whether this host can do unprivileged fuse-over-io_uring mounts.
fn mount_supported() -> bool {
    let has_fusermount = Command::new("fusermount3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    let uring_enabled = std::fs::read_to_string("/sys/module/fuse/parameters/enable_uring")
        .map(|s| s.trim() == "Y")
        .unwrap_or(false);
    has_fusermount && uring_enabled
}

fn is_mountpoint(path: &str) -> bool {
    Command::new("mountpoint")
        .arg("-q")
        .arg(path)
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// Unmounts and removes the temp mountpoint on drop, so an assertion failure
/// can't leave a wedged mount behind for the next run.
struct MountGuard(String);

impl Drop for MountGuard {
    fn drop(&mut self) {
        let _ = Command::new("fusermount3").args(["-u", &self.0]).status();
        let _ = std::fs::remove_dir(&self.0);
    }
}

#[test]
fn inval_entry_forces_fresh_lookup() {
    if !mount_supported() {
        eprintln!("SKIP: fuse-over-io_uring unprivileged mount not available on this host");
        return;
    }

    let mount = format!("/tmp/fractal_fuse_notify_inval_{}", std::process::id());
    let _ = Command::new("fusermount3").args(["-u", &mount]).status();
    std::fs::create_dir_all(&mount).expect("create mountpoint");
    let _guard = MountGuard(mount.clone());

    let watched_lookups = Arc::new(AtomicU64::new(0));

    let opts = MountOptions::new()
        .fs_name("notifyfs")
        .read_only(true)
        .default_permissions(true);
    let session = Session::new(PathBuf::from(&mount), opts).expect("mount session");

    // Grab a notifier (shares the /dev/fuse fd) before `run` consumes the
    // session on the worker thread.
    let notifier = FuseNotifier::from(session.fuse_fd());

    let fs = CountingFs {
        watched_lookups: Arc::clone(&watched_lookups),
    };
    let run_thread = thread::spawn(move || session.run(fs));

    // Wait for the mount to appear.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !is_mountpoint(&mount) {
        assert!(
            Instant::now() < deadline,
            "filesystem did not mount within 10s"
        );
        thread::sleep(Duration::from_millis(100));
    }

    // Watchdog: force-unmount if an assertion below leaves a syscall wedged, so
    // the test fails fast instead of hanging. Harmless no-op on the happy path.
    {
        let mount = mount.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(20));
            let _ = Command::new("fusermount3").args(["-u", &mount]).status();
        });
    }

    let path = format!("{mount}/{WATCHED_NAME}");

    // (1) First stat -> kernel has no cached dentry -> exactly one LOOKUP.
    std::fs::metadata(&path).expect("first stat of watched should succeed");
    let after_first = watched_lookups.load(Ordering::SeqCst);
    assert_eq!(
        after_first, 1,
        "first stat should trigger exactly one LOOKUP for {WATCHED_NAME}"
    );

    // (2) Second stat within TTL -> served from the dcache -> no new LOOKUP.
    // This proves the entry really is cached, so any later LOOKUP can only be
    // the result of an invalidation (not TTL expiry or revalidation).
    std::fs::metadata(&path).expect("second stat of watched should succeed");
    let after_cached = watched_lookups.load(Ordering::SeqCst);
    assert_eq!(
        after_cached, after_first,
        "second stat within TTL should be served from cache, not re-LOOKUP'd"
    );

    // (3) Invalidate the dentry, then stat again. The kernel must drop the
    // cached entry and re-issue LOOKUP. With a wrong (negated) notification
    // code the kernel silently ignores this and the count stays put.
    notifier
        .inval_entry(FUSE_ROOT_ID, WATCHED_NAME.as_bytes())
        .expect("inval_entry should write to /dev/fuse without error");

    std::fs::metadata(&path).expect("stat after invalidation should succeed");
    let after_inval = watched_lookups.load(Ordering::SeqCst);
    assert_eq!(
        after_inval,
        after_cached + 1,
        "inval_entry must force a fresh LOOKUP (the notification was dropped if this fails)"
    );

    // Clean up: unmount and confirm the run thread returns.
    let umount_ok = Command::new("fusermount3")
        .args(["-u", &mount])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    assert!(umount_ok, "fusermount3 -u failed");

    let joined = run_thread.join();
    assert!(joined.is_ok(), "Session::run thread panicked");
}
