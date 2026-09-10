//! FUSE passthrough ioctl wrappers.
//!
//! These use the FUSE_DEV_IOC_BACKING_OPEN/CLOSE ioctls (Linux 6.9+) to
//! register a backing file descriptor with the FUSE device. When the
//! filesystem returns FOPEN_PASSTHROUGH with a backing_id from open(), the
//! kernel reads/writes the backing file directly, bypassing FUSE entirely.

use std::io;
use std::os::fd::RawFd;

/// FUSE device ioctl magic number.
const FUSE_DEV_IOC_MAGIC: u32 = 229;

// ioctl command numbers for FUSE backing operations
// _IOC(dir, type, nr, size) = (dir << 30) | (type << 8) | (nr << 0) | (size << 16)
// _IOW = 0x40000000 (write direction)
// _IOWR = 0xC0000000 (read+write direction)

/// struct fuse_backing_map { int32_t fd; uint32_t flags; uint64_t padding; }
#[repr(C)]
struct FuseBackingMap {
    fd: i32,
    flags: u32,
    padding: u64,
}

const BACKING_MAP_SIZE: u32 = std::mem::size_of::<FuseBackingMap>() as u32;

/// FUSE_DEV_IOC_BACKING_OPEN = _IOWR(229, 1, struct fuse_backing_map)
const FUSE_DEV_IOC_BACKING_OPEN: libc::c_ulong = 0xC000_0000
    | ((BACKING_MAP_SIZE as libc::c_ulong) << 16)
    | ((FUSE_DEV_IOC_MAGIC as libc::c_ulong) << 8)
    | 1;

/// FUSE_DEV_IOC_BACKING_CLOSE = _IOW(229, 2, struct fuse_backing_map)
const FUSE_DEV_IOC_BACKING_CLOSE: libc::c_ulong = 0x4000_0000
    | ((BACKING_MAP_SIZE as libc::c_ulong) << 16)
    | ((FUSE_DEV_IOC_MAGIC as libc::c_ulong) << 8)
    | 2;

/// Register a backing file with the FUSE device. Returns a backing_id that
/// can be used in ReplyOpen to enable passthrough for that file handle.
pub fn fuse_backing_open(fuse_dev_fd: RawFd, backing_fd: RawFd) -> io::Result<i32> {
    let mut map = FuseBackingMap {
        fd: backing_fd,
        flags: 0,
        padding: 0,
    };

    let ret = unsafe {
        libc::ioctl(
            fuse_dev_fd,
            FUSE_DEV_IOC_BACKING_OPEN,
            &mut map as *mut FuseBackingMap,
        )
    };

    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(ret)
    }
}

/// Unregister a backing file from the FUSE device.
pub fn fuse_backing_close(fuse_dev_fd: RawFd, backing_id: i32) -> io::Result<()> {
    let mut map = FuseBackingMap {
        fd: backing_id,
        flags: 0,
        padding: 0,
    };

    let ret = unsafe {
        libc::ioctl(
            fuse_dev_fd,
            FUSE_DEV_IOC_BACKING_CLOSE,
            &mut map as *mut FuseBackingMap,
        )
    };

    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}
