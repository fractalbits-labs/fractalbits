use std::io;
use std::os::fd::RawFd;

use crate::abi::{
    FUSE_NOTIFY_DELETE, FUSE_NOTIFY_INVAL_ENTRY, FUSE_NOTIFY_INVAL_INODE, fuse_notify_delete_out,
    fuse_notify_inval_entry_out, fuse_notify_inval_inode_out, fuse_out_header,
};

/// Sends FUSE kernel notifications to invalidate cached entries.
///
/// Writes notification messages directly to `/dev/fuse`. These are
/// one-way messages (not request-response) that tell the kernel to
/// drop cached dentries, inode attributes, or page cache ranges.
pub struct FuseNotifier {
    fuse_dev_fd: RawFd,
}

impl FuseNotifier {
    pub fn new(fuse_dev_fd: RawFd) -> Self {
        Self { fuse_dev_fd }
    }

    /// Invalidate a directory entry from the kernel dcache.
    /// After this, the kernel will re-issue LOOKUP for this name.
    pub fn inval_entry(&self, parent: u64, name: &[u8]) -> io::Result<()> {
        let notify = fuse_notify_inval_entry_out {
            parent,
            namelen: name.len() as u32,
            flags: 0,
        };
        let header = fuse_out_header {
            len: (size_of::<fuse_out_header>()
                + size_of::<fuse_notify_inval_entry_out>()
                + name.len()) as u32,
            error: -FUSE_NOTIFY_INVAL_ENTRY,
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
            error: -FUSE_NOTIFY_INVAL_INODE,
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
            len: (size_of::<fuse_out_header>() + size_of::<fuse_notify_delete_out>() + name.len())
                as u32,
            error: -FUSE_NOTIFY_DELETE,
            unique: 0,
        };
        self.write_notify(&header, as_bytes(&notify), name)
    }

    /// Write a notification message to /dev/fuse using writev.
    /// The message is: [fuse_out_header] [notify struct] [optional name bytes]
    fn write_notify(
        &self,
        header: &fuse_out_header,
        notify_bytes: &[u8],
        name: &[u8],
    ) -> io::Result<()> {
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
        ];
        let iov_count = if name.is_empty() { 2 } else { 3 };

        let ret = unsafe { libc::writev(self.fuse_dev_fd, iovecs.as_mut_ptr(), iov_count) };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_struct_sizes() {
        // Verify struct sizes match kernel expectations
        assert_eq!(size_of::<fuse_out_header>(), 16);
        assert_eq!(size_of::<fuse_notify_inval_inode_out>(), 24);
        assert_eq!(size_of::<fuse_notify_inval_entry_out>(), 16);
        assert_eq!(size_of::<fuse_notify_delete_out>(), 24);
    }
}
