use std::ffi::OsStr;

use crate::types::*;

pub type FsResult<T> = std::result::Result<T, Errno>;

/// Async filesystem trait for FUSE operations.
///
/// All methods default to returning ENOSYS. Implement only the operations
/// your filesystem supports. Futures are `!Send` (compio single-threaded).
/// The trait object itself is `Send + Sync` for sharing via Arc across threads.
#[allow(clippy::too_many_arguments)]
pub trait Filesystem: Send + Sync + 'static {
    /// Called after FUSE_INIT to provide the /dev/fuse fd for passthrough ioctls.
    fn set_fuse_dev_fd(&self, _fd: i32) {}

    /// Initialize the filesystem.
    ///
    /// Called once during mount before any other operations. Returns
    /// [`ReplyInit`] containing `max_write` size and capability flags that
    /// the kernel negotiates with the FUSE client.
    fn init(&self, req: Request) -> impl std::future::Future<Output = FsResult<ReplyInit>> {
        let _ = req;
        async { Ok(ReplyInit::default()) }
    }

    /// Clean up filesystem on unmount.
    ///
    /// Called when the filesystem is being unmounted. Use this to flush
    /// dirty data, close open handles, and release any resources held by
    /// the filesystem implementation.
    fn destroy(&self) -> impl std::future::Future<Output = ()> {
        async {}
    }

    /// Look up a directory entry by name and return its attributes.
    ///
    /// The kernel calls this to resolve path components. On success, the
    /// returned [`ReplyEntry`] includes the inode number, generation, and
    /// attributes along with cache timeout hints. Each successful lookup
    /// increments the inode's reference count (decremented by `forget`).
    fn lookup(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
    ) -> impl std::future::Future<Output = FsResult<ReplyEntry>> {
        let _ = (req, parent, name);
        async { Err(ENOSYS) }
    }

    /// Release an inode reference obtained via `lookup` or other entry-creating
    /// operations.
    ///
    /// `nlookup` is the number of references to drop. The filesystem should
    /// not return errors; after this call, the inode may be evicted if no
    /// references remain.
    fn forget(&self, req: Request, inode: Inode, nlookup: u64) {
        let _ = (req, inode, nlookup);
    }

    /// Release multiple inode references in a single call.
    ///
    /// Each element in `inodes` is an `(inode, nlookup)` pair. The default
    /// implementation delegates to `forget` for each entry.
    fn batch_forget(&self, req: Request, inodes: &[(Inode, u64)]) {
        for &(inode, nlookup) in inodes {
            self.forget(req, inode, nlookup);
        }
    }

    /// Get file attributes.
    ///
    /// If a file handle `fh` is provided, use it instead of the inode for
    /// fetching attributes (useful when the file is open and the handle
    /// carries additional state). Returns [`ReplyAttr`] with the attributes
    /// and a cache validity timeout.
    fn getattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        flags: u32,
    ) -> impl std::future::Future<Output = FsResult<ReplyAttr>> {
        let _ = (req, inode, fh, flags);
        async { Err(ENOSYS) }
    }

    /// Get extended file attributes via statx(2).
    ///
    /// Similar to `getattr`, but returns additional fields like birth time
    /// and file attributes. The `flags` parameter contains statx flags
    /// (e.g., `AT_NO_AUTOMOUNT`), and `mask` specifies which attributes
    /// to retrieve. Returns [`ReplyStatx`] with extended attributes.
    fn statx(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        flags: u32,
        mask: u32,
    ) -> impl std::future::Future<Output = FsResult<ReplyStatx>> {
        let _ = (req, inode, fh, flags, mask);
        async { Err(ENOSYS) }
    }
    /// Set file attributes.
    ///
    /// The [`SetAttr`] struct indicates which fields to change (size, mode,
    /// uid/gid, timestamps, etc.). If a file handle `fh` is provided, it
    /// should be used for the operation (e.g., ftruncate uses fh). Returns
    /// the updated attributes.
    fn setattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> impl std::future::Future<Output = FsResult<ReplyAttr>> {
        let _ = (req, inode, fh, set_attr);
        async { Err(ENOSYS) }
    }

    /// Read the target of a symbolic link.
    fn readlink(
        &self,
        req: Request,
        inode: Inode,
    ) -> impl std::future::Future<Output = FsResult<ReplyReadlink>> {
        let _ = (req, inode);
        async { Err(ENOSYS) }
    }

    /// Create a symbolic link.
    ///
    /// Creates a symlink named `name` in `parent` directory that points to
    /// `link`. Returns the entry attributes for the new symlink inode.
    fn symlink(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        link: &OsStr,
    ) -> impl std::future::Future<Output = FsResult<ReplyEntry>> {
        let _ = (req, parent, name, link);
        async { Err(ENOSYS) }
    }

    /// Create a special file (device node, named pipe, or socket).
    ///
    /// `mode` encodes the file type and permissions (see `S_IFMT` in
    /// stat(2)). `rdev` is the device number for block/char devices.
    fn mknod(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> impl std::future::Future<Output = FsResult<ReplyEntry>> {
        let _ = (req, parent, name, mode, rdev);
        async { Err(ENOSYS) }
    }

    /// Create a directory.
    ///
    /// `mode` specifies the permissions and `umask` is the process umask
    /// that should be applied. Returns the entry for the new directory.
    fn mkdir(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> impl std::future::Future<Output = FsResult<ReplyEntry>> {
        let _ = (req, parent, name, mode, umask);
        async { Err(ENOSYS) }
    }

    /// Remove a file from a directory.
    ///
    /// The actual file data may persist until all open handles and remaining
    /// lookup references are released.
    fn unlink(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
    ) -> impl std::future::Future<Output = FsResult<()>> {
        let _ = (req, parent, name);
        async { Err(ENOSYS) }
    }

    /// Remove an empty directory.
    fn rmdir(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
    ) -> impl std::future::Future<Output = FsResult<()>> {
        let _ = (req, parent, name);
        async { Err(ENOSYS) }
    }

    /// Rename or move a file or directory.
    ///
    /// Moves the entry `name` from `parent` to `new_name` under `new_parent`.
    /// `flags` may include `RENAME_EXCHANGE` or `RENAME_NOREPLACE` (see
    /// rename2(2)).
    fn rename(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        new_parent: Inode,
        new_name: &OsStr,
        flags: u32,
    ) -> impl std::future::Future<Output = FsResult<()>> {
        let _ = (req, parent, name, new_parent, new_name, flags);
        async { Err(ENOSYS) }
    }

    /// Create a hard link.
    ///
    /// Creates a new directory entry `new_name` in `new_parent` pointing to
    /// the existing `inode`. This increments the inode's link count.
    fn link(
        &self,
        req: Request,
        inode: Inode,
        new_parent: Inode,
        new_name: &OsStr,
    ) -> impl std::future::Future<Output = FsResult<ReplyEntry>> {
        let _ = (req, inode, new_parent, new_name);
        async { Err(ENOSYS) }
    }

    /// Open a file.
    ///
    /// `flags` are the open(2) flags (O_RDONLY, O_WRONLY, O_RDWR, etc.).
    /// Return a [`ReplyOpen`] containing an opaque file handle (`fh`) that
    /// will be passed to subsequent read/write/flush/release calls, along
    /// with `open_flags` that may adjust kernel caching behavior
    /// (e.g., `FOPEN_DIRECT_IO`, `FOPEN_KEEP_CACHE`).
    fn open(
        &self,
        req: Request,
        inode: Inode,
        flags: u32,
    ) -> impl std::future::Future<Output = FsResult<ReplyOpen>> {
        let _ = (req, inode, flags);
        async { Err(ENOSYS) }
    }

    /// Read data from an open file into a caller-provided buffer.
    ///
    /// Reads up to `buf.len()` bytes starting at `offset` from the file
    /// identified by `fh`, writing directly into `buf`. Returns the number
    /// of bytes read. This is the primary read path used by the FUSE
    /// dispatch layer, avoiding intermediate allocations and copies.
    fn read(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        buf: &mut [u8],
    ) -> impl std::future::Future<Output = FsResult<usize>> {
        let _ = (req, inode, fh, offset, buf);
        async { Err(ENOSYS) }
    }

    /// Write data to an open file.
    ///
    /// Write `data` at `offset` to the file identified by `fh`.
    /// `write_flags` may include `FUSE_WRITE_CACHE` (from writeback cache).
    /// `flags` are the open(2) flags. Returns the number of bytes written.
    fn write(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        data: &[u8],
        write_flags: u32,
        flags: u32,
    ) -> impl std::future::Future<Output = FsResult<usize>> {
        let _ = (req, inode, fh, offset, data, write_flags, flags);
        async { Err(ENOSYS) }
    }

    /// Flush pending data for a file handle.
    ///
    /// Called on each close(2) of a file descriptor (a file may have
    /// multiple open fds). This is not the same as fsync -- it is called
    /// per-fd, not per-file. `lock_owner` identifies the lock owner for
    /// POSIX file locks that should be released.
    fn flush(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        lock_owner: u64,
    ) -> impl std::future::Future<Output = FsResult<()>> {
        let _ = (req, inode, fh, lock_owner);
        async { Err(ENOSYS) }
    }

    /// Release (close) an open file handle.
    ///
    /// Called when the last file descriptor for a file handle is closed.
    /// `flush` indicates whether pending data should be flushed before
    /// release. After this call, the file handle `fh` is no longer valid.
    fn release(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> impl std::future::Future<Output = FsResult<()>> {
        let _ = (req, inode, fh, flags, lock_owner, flush);
        async { Err(ENOSYS) }
    }

    /// Synchronize file contents to storage.
    ///
    /// If `datasync` is true, only the file data needs to be flushed (not
    /// metadata like timestamps or size), equivalent to fdatasync(2).
    fn fsync(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        datasync: bool,
    ) -> impl std::future::Future<Output = FsResult<()>> {
        let _ = (req, inode, fh, datasync);
        async { Err(ENOSYS) }
    }

    /// Open a directory for reading.
    ///
    /// Returns a [`ReplyOpen`] with a directory handle (`fh`) that will be
    /// passed to readdir/readdirplus/releasedir calls.
    fn opendir(
        &self,
        req: Request,
        inode: Inode,
        flags: u32,
    ) -> impl std::future::Future<Output = FsResult<ReplyOpen>> {
        let _ = (req, inode, flags);
        async { Err(ENOSYS) }
    }

    /// Read directory entries.
    ///
    /// Return entries starting after the given `offset`. The `offset` is an
    /// opaque value from a previous readdir result (or 0 for the first
    /// call). `size` is the maximum response buffer size in bytes. The
    /// kernel will stop requesting more entries once the buffer is full or
    /// an empty result is returned.
    fn readdir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> impl std::future::Future<Output = FsResult<Vec<DirectoryEntry>>> {
        let _ = (req, inode, fh, offset, size);
        async { Err(ENOSYS) }
    }

    /// Read directory entries with full attributes.
    ///
    /// Like `readdir`, but each entry includes a full [`ReplyEntry`] with
    /// inode attributes, avoiding separate `lookup` calls. This is a
    /// performance optimization for directory listing.
    fn readdirplus(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> impl std::future::Future<Output = FsResult<Vec<DirectoryEntryPlus>>> {
        let _ = (req, inode, fh, offset, size);
        async { Err(ENOSYS) }
    }

    /// Release (close) an open directory handle.
    fn releasedir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
    ) -> impl std::future::Future<Output = FsResult<()>> {
        let _ = (req, inode, fh, flags);
        async { Err(ENOSYS) }
    }

    /// Synchronize directory contents to storage.
    ///
    /// If `datasync` is true, only directory entry data needs to be flushed
    /// (not directory metadata).
    fn fsyncdir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        datasync: bool,
    ) -> impl std::future::Future<Output = FsResult<()>> {
        let _ = (req, inode, fh, datasync);
        async { Err(ENOSYS) }
    }

    /// Get filesystem statistics.
    ///
    /// Returns a [`ReplyStatfs`] with block counts, free space, inode
    /// counts, and other filesystem-wide metrics (equivalent to statfs(2)).
    /// The default implementation returns a zeroed struct with 512-byte
    /// blocks and 255-char name limit.
    fn statfs(
        &self,
        req: Request,
        inode: Inode,
    ) -> impl std::future::Future<Output = FsResult<ReplyStatfs>> {
        let _ = (req, inode);
        async {
            Ok(ReplyStatfs {
                blocks: 0,
                bfree: 0,
                bavail: 0,
                files: 0,
                ffree: 0,
                bsize: 512,
                namelen: 255,
                frsize: 512,
            })
        }
    }

    /// Check file access permissions.
    ///
    /// Called when `default_permissions` mount option is NOT set. `mask` is
    /// a combination of `R_OK`, `W_OK`, `X_OK`, and `F_OK` (see access(2)).
    /// Return `Ok(())` if access is permitted, or an appropriate errno.
    fn access(
        &self,
        req: Request,
        inode: Inode,
        mask: u32,
    ) -> impl std::future::Future<Output = FsResult<()>> {
        let _ = (req, inode, mask);
        async { Err(ENOSYS) }
    }

    /// Atomically create and open a file.
    ///
    /// Combines mknod + open into a single operation, avoiding a race
    /// between creation and opening. `mode` specifies file permissions and
    /// `flags` are the open(2) flags. Returns a [`ReplyCreate`] with both
    /// the entry attributes and the open file handle.
    fn create(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> impl std::future::Future<Output = FsResult<ReplyCreate>> {
        let _ = (req, parent, name, mode, flags);
        async { Err(ENOSYS) }
    }

    /// Pre-allocate or deallocate file space.
    ///
    /// Ensures that `length` bytes starting at `offset` are allocated on
    /// storage without writing data. `mode` flags control the behavior
    /// (e.g., `FALLOC_FL_KEEP_SIZE`, `FALLOC_FL_PUNCH_HOLE`). See
    /// fallocate(2).
    fn fallocate(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> impl std::future::Future<Output = FsResult<()>> {
        let _ = (req, inode, fh, offset, length, mode);
        async { Err(ENOSYS) }
    }

    /// Find the next data or hole offset in a file.
    ///
    /// `whence` is either `SEEK_DATA` or `SEEK_HOLE`. Returns the resulting
    /// offset. This enables sparse file support. See lseek(2).
    fn lseek(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        whence: u32,
    ) -> impl std::future::Future<Output = FsResult<u64>> {
        let _ = (req, inode, fh, offset, whence);
        async { Err(ENOSYS) }
    }

    /// Copy a range of data from one file to another server-side.
    ///
    /// Copies `length` bytes from `(inode_in, fh_in)` at `off_in` to
    /// `(inode_out, fh_out)` at `off_out` without transferring data through
    /// userspace. Returns the number of bytes copied. See
    /// copy_file_range(2).
    fn copy_file_range(
        &self,
        req: Request,
        inode_in: Inode,
        fh_in: u64,
        off_in: u64,
        inode_out: Inode,
        fh_out: u64,
        off_out: u64,
        length: u64,
        flags: u64,
    ) -> impl std::future::Future<Output = FsResult<usize>> {
        let _ = (
            req, inode_in, fh_in, off_in, inode_out, fh_out, off_out, length, flags,
        );
        async { Err(ENOSYS) }
    }
}
