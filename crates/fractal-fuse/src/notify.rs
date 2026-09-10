use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;

use crate::abi::{
    FUSE_NOTIFY_DELETE, FUSE_NOTIFY_INVAL_ENTRY, FUSE_NOTIFY_INVAL_INODE, fuse_notify_delete_out,
    fuse_notify_inval_entry_out, fuse_notify_inval_inode_out, fuse_out_header,
};

/// Handle for sending unsolicited notifications to the FUSE kernel module.
///
/// Writes one-way messages (not request/response) directly to `/dev/fuse`,
/// telling the kernel to drop cached dentries, inode attributes, or
/// page-cache ranges when underlying data changes outside the kernel's
/// view. Shares ownership of the `/dev/fuse` fd via `Arc<OwnedFd>`, so the
/// fd stays open for as long as any clone exists. Construct one from the
/// `Arc<OwnedFd>` returned by
/// [`Session::fuse_fd`](crate::Session::fuse_fd), e.g.
/// `FuseNotifier::from(session.fuse_fd())`.
#[derive(Clone)]
pub struct FuseNotifier {
    fuse_dev_fd: Arc<OwnedFd>,
}

impl From<Arc<OwnedFd>> for FuseNotifier {
    fn from(fuse_dev_fd: Arc<OwnedFd>) -> Self {
        Self { fuse_dev_fd }
    }
}

impl FuseNotifier {
    /// Invalidate a directory entry from the kernel dcache.
    /// After this, the kernel will re-issue LOOKUP for this name.
    pub fn inval_entry(&self, parent: u64, name: &[u8]) -> io::Result<()> {
        let notify = fuse_notify_inval_entry_out {
            parent,
            namelen: name.len() as u32,
            flags: 0,
        };
        let header = fuse_out_header {
            // +1: the kernel requires the name to be NUL-terminated on the wire
            // (it reads namelen+1 bytes); see write_notify and fs/fuse/dev.c.
            len: (size_of::<fuse_out_header>()
                + size_of::<fuse_notify_inval_entry_out>()
                + name.len()
                + 1) as u32,
            error: FUSE_NOTIFY_INVAL_ENTRY,
            unique: 0,
        };
        self.write_notify(&header, as_bytes(&notify), name)
    }

    /// Invalidate inode attributes and optionally a page cache range.
    /// Use offset=-1, len=-1 to invalidate all cached data.
    pub fn inval_inode(&self, ino: u64, offset: i64, len: i64) -> io::Result<()> {
        let notify = fuse_notify_inval_inode_out {
            ino,
            off: offset,
            len,
        };
        let header = fuse_out_header {
            len: (size_of::<fuse_out_header>() + size_of::<fuse_notify_inval_inode_out>()) as u32,
            error: FUSE_NOTIFY_INVAL_INODE,
            unique: 0,
        };
        self.write_notify(&header, as_bytes(&notify), &[])
    }

    /// Invalidate a directory entry and notify inotify watchers.
    /// Use for deletes where applications may be watching.
    pub fn delete(&self, parent: u64, child: u64, name: &[u8]) -> io::Result<()> {
        let notify = fuse_notify_delete_out {
            parent,
            child,
            namelen: name.len() as u32,
            padding: 0,
        };
        let header = fuse_out_header {
            // +1: the kernel requires the name to be NUL-terminated on the wire
            // (it reads namelen+1 bytes); see write_notify and fs/fuse/dev.c.
            len: (size_of::<fuse_out_header>()
                + size_of::<fuse_notify_delete_out>()
                + name.len()
                + 1) as u32,
            error: FUSE_NOTIFY_DELETE,
            unique: 0,
        };
        self.write_notify(&header, as_bytes(&notify), name)
    }

    /// Write a notification message to /dev/fuse using writev.
    /// The message is: [fuse_out_header] [notify struct] [name bytes] [NUL].
    /// The kernel expects names to be NUL-terminated (it reads namelen+1 bytes
    /// and stores the terminator), so a trailing NUL is appended whenever a
    /// name is present. The `namelen` field itself stays the un-terminated
    /// length. Notifications without a name (e.g. inval_inode) omit both.
    fn write_notify(
        &self,
        header: &fuse_out_header,
        notify_bytes: &[u8],
        name: &[u8],
    ) -> io::Result<()> {
        const NUL: [u8; 1] = [0];
        let mut iovecs = [
            libc::iovec {
                iov_base: header as *const _ as *mut _,
                iov_len: size_of::<fuse_out_header>(),
            },
            libc::iovec {
                iov_base: notify_bytes.as_ptr() as *mut _,
                iov_len: notify_bytes.len(),
            },
            libc::iovec {
                iov_base: name.as_ptr() as *mut _,
                iov_len: name.len(),
            },
            libc::iovec {
                iov_base: NUL.as_ptr() as *mut _,
                iov_len: NUL.len(),
            },
        ];
        let iov_count = if name.is_empty() { 2 } else { 4 };

        let ret =
            unsafe { libc::writev(self.fuse_dev_fd.as_raw_fd(), iovecs.as_mut_ptr(), iov_count) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            // ENOENT means the inode/entry was already gone from kernel cache.
            // This is expected and not an error.
            if err.raw_os_error() == Some(libc::ENOENT) {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }
}

fn as_bytes<T: Sized>(val: &T) -> &[u8] {
    unsafe { std::slice::from_raw_parts(val as *const T as *const u8, size_of::<T>()) }
}
