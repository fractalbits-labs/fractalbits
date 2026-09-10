//! Regression test for issue #28: a panic inside a `Filesystem` callback must
//! surface EIO to the client, leave the rest of the mount serving, and still
//! allow a clean unmount.
//!
//! This mounts a real FUSE-over-io_uring filesystem, so it needs an unprivileged
//! `fusermount3` and a kernel with `fuse` io_uring support enabled. When that
//! isn't available the test skips rather than failing.

use std::ffi::OsStr;
use std::path::PathBuf;
use std::process::Command;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use fractal_fuse::abi::FUSE_ROOT_ID;
use fractal_fuse::{
    DirectoryEntry, DirectoryEntryPlus, EIO, ENOENT, FileAttr, FileHandleId, FileType, Filesystem,
    FsResult, InodeId, MountOptions, ReplyAttr, ReplyEntry, ReplyOpen, ReplyStatfs, Request,
    Session, Timestamp,
};

const HELLO_INO: u64 = 2;
const HELLO_CONTENT: &[u8] = b"Hello, FUSE over io_uring!\n";
const TTL: Duration = Duration::from_secs(60);

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
        // World-readable/executable, so ownership is irrelevant to the test;
        // avoid pulling libc into the test crate's dependency closure.
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 512,
    }
}

fn hello_attr() -> FileAttr {
    let ts = now_ts();
    FileAttr {
        ino: HELLO_INO,
        size: HELLO_CONTENT.len() as u64,
        blocks: 1,
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

/// A filesystem that serves a single "hello" file but deliberately panics on
/// any directory listing, so the panic path can be exercised end to end.
struct PanicReaddirFs;

impl Filesystem for PanicReaddirFs {
    async fn lookup(&self, _req: Request, parent: InodeId, name: &OsStr) -> FsResult<ReplyEntry> {
        if parent.0 == FUSE_ROOT_ID && name == "hello" {
            Ok(ReplyEntry {
                ttl: TTL,
                attr: hello_attr(),
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
            HELLO_INO => Ok(ReplyAttr {
                ttl: TTL,
                attr: hello_attr(),
            }),
            _ => Err(ENOENT),
        }
    }

    async fn open(&self, _req: Request, inode: InodeId, _flags: u32) -> FsResult<ReplyOpen> {
        if inode.0 == HELLO_INO {
            Ok(ReplyOpen {
                fh: FileHandleId(0),
                flags: 0,
                backing_id: 0,
            })
        } else {
            Err(ENOENT)
        }
    }

    async fn read(
        &self,
        _req: Request,
        inode: InodeId,
        _fh: FileHandleId,
        offset: u64,
        buf: &mut [u8],
    ) -> FsResult<usize> {
        if inode.0 != HELLO_INO {
            return Err(ENOENT);
        }
        let offset = offset as usize;
        if offset >= HELLO_CONTENT.len() {
            return Ok(0);
        }
        let end = (offset + buf.len()).min(HELLO_CONTENT.len());
        let src = &HELLO_CONTENT[offset..end];
        buf[..src.len()].copy_from_slice(src);
        Ok(src.len())
    }

    async fn opendir(&self, _req: Request, _inode: InodeId, _flags: u32) -> FsResult<ReplyOpen> {
        Ok(ReplyOpen {
            fh: FileHandleId(0),
            flags: 0,
            backing_id: 0,
        })
    }

    async fn readdir(
        &self,
        _req: Request,
        _inode: InodeId,
        _fh: FileHandleId,
        _offset: u64,
        _size: u32,
    ) -> FsResult<Vec<DirectoryEntry>> {
        panic!("injected readdir panic (issue #28 regression test)");
    }

    async fn readdirplus(
        &self,
        _req: Request,
        _inode: InodeId,
        _fh: FileHandleId,
        _offset: u64,
        _size: u32,
    ) -> FsResult<Vec<DirectoryEntryPlus>> {
        panic!("injected readdirplus panic (issue #28 regression test)");
    }

    async fn statfs(&self, _req: Request, _inode: InodeId) -> FsResult<ReplyStatfs> {
        Ok(ReplyStatfs {
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 2,
            ffree: 0,
            bsize: 512,
            namelen: 255,
            frsize: 512,
        })
    }

    async fn access(&self, _req: Request, _inode: InodeId, _mask: u32) -> FsResult<()> {
        Ok(())
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

/// First error encountered listing `path`, or `None` on success. An empty
/// successful listing (the issue #28 bug) also yields `None`.
fn read_dir_first_error(path: &str) -> Option<std::io::Error> {
    match std::fs::read_dir(path) {
        Err(e) => Some(e),
        Ok(mut rd) => match rd.next() {
            Some(Err(e)) => Some(e),
            _ => None,
        },
    }
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
fn readdir_panic_returns_eio_and_keeps_serving() {
    if !mount_supported() {
        eprintln!("SKIP: fuse-over-io_uring unprivileged mount not available on this host");
        return;
    }

    let mount = format!("/tmp/fractal_fuse_readdir_panic_{}", std::process::id());
    let _ = Command::new("fusermount3").args(["-u", &mount]).status();
    std::fs::create_dir_all(&mount).expect("create mountpoint");
    let _guard = MountGuard(mount.clone());

    let opts = MountOptions::new()
        .fs_name("panicfs")
        .read_only(true)
        .default_permissions(true);
    let session = Session::new(PathBuf::from(&mount), opts).expect("mount session");

    // Session::run blocks, so drive it on a worker thread and signal completion
    // through a channel for a bounded join.
    let (done_tx, done_rx) = mpsc::channel();
    let run_thread = thread::spawn(move || {
        let result = session.run(PanicReaddirFs);
        let _ = done_tx.send(());
        result
    });

    // Wait for the mount to appear.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !is_mountpoint(&mount) {
        assert!(
            Instant::now() < deadline,
            "filesystem did not mount within 10s"
        );
        thread::sleep(Duration::from_millis(100));
    }

    // Watchdog: if the panic handling regresses, the request below is left
    // uncommitted and the getdents/read syscall blocks forever. Force-unmount
    // after a deadline so the blocked syscall returns an error and the test
    // fails fast instead of hanging CI. On the happy path the test finishes in
    // a few seconds and this is a harmless no-op (mount already gone).
    {
        let mount = mount.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(20));
            let _ = Command::new("fusermount3").args(["-u", &mount]).status();
        });
    }

    // (1) A directory listing panics in the fs -> the client must see EIO, not a
    // bogus empty-success reply.
    let err = read_dir_first_error(&mount);
    assert_eq!(
        err.as_ref().and_then(|e| e.raw_os_error()),
        Some(EIO),
        "readdir on a panicking fs should return EIO, got {err:?}"
    );

    // (2) A non-panicking op still works: the panic failed one request, not the
    // whole mount.
    let content = std::fs::read_to_string(format!("{mount}/hello"))
        .expect("reading hello after a readdir panic should still succeed");
    assert_eq!(
        content,
        String::from_utf8_lossy(HELLO_CONTENT),
        "hello content mismatch after panic"
    );

    // (3) The mount unmounts cleanly (issue #28: no EBUSY / wedged mountpoint).
    let umount_ok = Command::new("fusermount3")
        .args(["-u", &mount])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    assert!(
        umount_ok,
        "fusermount3 -u failed after a readdir panic (issue #28)"
    );

    // (4) Session::run returns promptly once unmounted.
    assert!(
        done_rx.recv_timeout(Duration::from_secs(15)).is_ok(),
        "Session::run did not return within 15s after unmount"
    );
    let _ = run_thread.join();
}
