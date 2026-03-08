// FUSE kernel protocol v7.45 ABI definitions
// Reference: libfuse/include/fuse_kernel.h

#![allow(non_camel_case_types, dead_code)]

pub const FUSE_KERNEL_VERSION: u32 = 7;
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 45;
pub const FUSE_ROOT_ID: u64 = 1;
pub const FUSE_MIN_READ_BUFFER: u32 = 8192;

// Opcodes
pub const FUSE_LOOKUP: u32 = 1;
pub const FUSE_FORGET: u32 = 2;
pub const FUSE_GETATTR: u32 = 3;
pub const FUSE_SETATTR: u32 = 4;
pub const FUSE_READLINK: u32 = 5;
pub const FUSE_SYMLINK: u32 = 6;
pub const FUSE_MKNOD: u32 = 8;
pub const FUSE_MKDIR: u32 = 9;
pub const FUSE_UNLINK: u32 = 10;
pub const FUSE_RMDIR: u32 = 11;
pub const FUSE_RENAME: u32 = 12;
pub const FUSE_LINK: u32 = 13;
pub const FUSE_OPEN: u32 = 14;
pub const FUSE_READ: u32 = 15;
pub const FUSE_WRITE: u32 = 16;
pub const FUSE_STATFS: u32 = 17;
pub const FUSE_RELEASE: u32 = 18;
pub const FUSE_FSYNC: u32 = 20;
pub const FUSE_SETXATTR: u32 = 21;
pub const FUSE_GETXATTR: u32 = 22;
pub const FUSE_LISTXATTR: u32 = 23;
pub const FUSE_REMOVEXATTR: u32 = 24;
pub const FUSE_FLUSH: u32 = 25;
pub const FUSE_INIT: u32 = 26;
pub const FUSE_OPENDIR: u32 = 27;
pub const FUSE_READDIR: u32 = 28;
pub const FUSE_RELEASEDIR: u32 = 29;
pub const FUSE_FSYNCDIR: u32 = 30;
pub const FUSE_GETLK: u32 = 31;
pub const FUSE_SETLK: u32 = 32;
pub const FUSE_SETLKW: u32 = 33;
pub const FUSE_ACCESS: u32 = 34;
pub const FUSE_CREATE: u32 = 35;
pub const FUSE_INTERRUPT: u32 = 36;
pub const FUSE_BMAP: u32 = 37;
pub const FUSE_DESTROY: u32 = 38;
pub const FUSE_IOCTL: u32 = 39;
pub const FUSE_POLL: u32 = 40;
pub const FUSE_NOTIFY_REPLY: u32 = 41;
pub const FUSE_BATCH_FORGET: u32 = 42;
pub const FUSE_FALLOCATE: u32 = 43;
pub const FUSE_READDIRPLUS: u32 = 44;
pub const FUSE_RENAME2: u32 = 45;
pub const FUSE_LSEEK: u32 = 46;
pub const FUSE_COPY_FILE_RANGE: u32 = 47;
pub const FUSE_SETUPMAPPING: u32 = 48;
pub const FUSE_REMOVEMAPPING: u32 = 49;
pub const FUSE_SYNCFS: u32 = 50;
pub const FUSE_TMPFILE: u32 = 51;
pub const FUSE_STATX: u32 = 52;

// FUSE_INIT capability flags (bits 0-31, stored in flags field)
pub const FUSE_ASYNC_READ: u32 = 1 << 0;
pub const FUSE_POSIX_LOCKS: u32 = 1 << 1;
pub const FUSE_FILE_OPS: u32 = 1 << 2;
pub const FUSE_ATOMIC_O_TRUNC: u32 = 1 << 3;
pub const FUSE_EXPORT_SUPPORT: u32 = 1 << 4;
pub const FUSE_BIG_WRITES: u32 = 1 << 5;
pub const FUSE_DONT_MASK: u32 = 1 << 6;
pub const FUSE_SPLICE_WRITE: u32 = 1 << 7;
pub const FUSE_SPLICE_MOVE: u32 = 1 << 8;
pub const FUSE_SPLICE_READ: u32 = 1 << 9;
pub const FUSE_FLOCK_LOCKS: u32 = 1 << 10;
pub const FUSE_HAS_IOCTL_DIR: u32 = 1 << 11;
pub const FUSE_AUTO_INVAL_DATA: u32 = 1 << 12;
pub const FUSE_DO_READDIRPLUS: u32 = 1 << 13;
pub const FUSE_READDIRPLUS_AUTO: u32 = 1 << 14;
pub const FUSE_ASYNC_DIO: u32 = 1 << 15;
pub const FUSE_WRITEBACK_CACHE: u32 = 1 << 16;
pub const FUSE_NO_OPEN_SUPPORT: u32 = 1 << 17;
pub const FUSE_PARALLEL_DIROPS: u32 = 1 << 18;
pub const FUSE_HANDLE_KILLPRIV: u32 = 1 << 19;
pub const FUSE_POSIX_ACL: u32 = 1 << 20;
pub const FUSE_ABORT_ERROR: u32 = 1 << 21;
pub const FUSE_MAX_PAGES: u32 = 1 << 22;
pub const FUSE_CACHE_SYMLINKS: u32 = 1 << 23;
pub const FUSE_NO_OPENDIR_SUPPORT: u32 = 1 << 24;
pub const FUSE_EXPLICIT_INVAL_DATA: u32 = 1 << 25;
pub const FUSE_MAP_ALIGNMENT: u32 = 1 << 26;
pub const FUSE_SUBMOUNTS: u32 = 1 << 27;
pub const FUSE_HANDLE_KILLPRIV_V2: u32 = 1 << 28;
pub const FUSE_SETXATTR_EXT: u32 = 1 << 29;
pub const FUSE_INIT_EXT: u32 = 1 << 30;
pub const FUSE_INIT_RESERVED: u32 = 1 << 31;

// Extended FUSE_INIT flags (64-bit, bits 32+)
pub const FUSE_SECURITY_CTX: u64 = 1 << 32;
pub const FUSE_HAS_INODE_DAX: u64 = 1 << 33;
pub const FUSE_CREATE_SUPP_GROUP: u64 = 1 << 34;
pub const FUSE_HAS_EXPIRE_ONLY: u64 = 1 << 35;
pub const FUSE_DIRECT_IO_ALLOW_MMAP: u64 = 1 << 36;
pub const FUSE_PASSTHROUGH: u64 = 1 << 37;
pub const FUSE_NO_EXPORT_SUPPORT: u64 = 1 << 38;
pub const FUSE_HAS_RESEND: u64 = 1 << 39;
pub const FUSE_ALLOW_IDMAP: u64 = 1 << 40;
pub const FUSE_OVER_IO_URING: u64 = 1 << 41;
pub const FUSE_REQUEST_TIMEOUT: u64 = 1 << 42;

// FOPEN flags
pub const FOPEN_DIRECT_IO: u32 = 1 << 0;
pub const FOPEN_KEEP_CACHE: u32 = 1 << 1;
pub const FOPEN_NONSEEKABLE: u32 = 1 << 2;
pub const FOPEN_CACHE_DIR: u32 = 1 << 3;
pub const FOPEN_STREAM: u32 = 1 << 4;
pub const FOPEN_NOFLUSH: u32 = 1 << 5;
pub const FOPEN_PARALLEL_DIRECT_WRITES: u32 = 1 << 6;
pub const FOPEN_PASSTHROUGH: u32 = 1 << 7;

// FATTR flags (setattr valid mask)
pub const FATTR_MODE: u32 = 1 << 0;
pub const FATTR_UID: u32 = 1 << 1;
pub const FATTR_GID: u32 = 1 << 2;
pub const FATTR_SIZE: u32 = 1 << 3;
pub const FATTR_ATIME: u32 = 1 << 4;
pub const FATTR_MTIME: u32 = 1 << 5;
pub const FATTR_FH: u32 = 1 << 6;
pub const FATTR_ATIME_NOW: u32 = 1 << 7;
pub const FATTR_MTIME_NOW: u32 = 1 << 8;
pub const FATTR_LOCKOWNER: u32 = 1 << 9;
pub const FATTR_CTIME: u32 = 1 << 10;
pub const FATTR_KILL_SUIDGID: u32 = 1 << 11;

// FUSE write flags
pub const FUSE_WRITE_CACHE: u32 = 1 << 0;
pub const FUSE_WRITE_LOCKOWNER: u32 = 1 << 1;
pub const FUSE_WRITE_KILL_SUIDGID: u32 = 1 << 2;

// GETATTR flags
pub const FUSE_GETATTR_FH: u32 = 1 << 0;

// io_uring protocol constants
pub const FUSE_URING_IN_OUT_HEADER_SZ: usize = 128;
pub const FUSE_URING_OP_IN_OUT_SZ: usize = 128;

pub const FUSE_IO_URING_CMD_INVALID: u32 = 0;
pub const FUSE_IO_URING_CMD_REGISTER: u32 = 1;
pub const FUSE_IO_URING_CMD_COMMIT_AND_FETCH: u32 = 2;

// ---------- Core ABI structures ----------

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_in_header {
    pub len: u32,
    pub opcode: u32,
    pub unique: u64,
    pub nodeid: u64,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
    pub total_extlen: u16,
    pub padding: u16,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_out_header {
    pub len: u32,
    pub error: i32,
    pub unique: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_attr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
    pub atimensec: u32,
    pub mtimensec: u32,
    pub ctimensec: u32,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
}

// ---------- Request structures ----------

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_init_in {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
    pub flags2: u32,
    pub unused: [u32; 11],
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_init_out {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
    pub max_background: u16,
    pub congestion_threshold: u16,
    pub max_write: u32,
    pub time_gran: u32,
    pub max_pages: u16,
    pub map_alignment: u16,
    pub flags2: u32,
    pub max_stack_depth: u32,
    pub request_timeout: u16,
    pub unused: [u16; 11],
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_getattr_in {
    pub getattr_flags: u32,
    pub dummy: u32,
    pub fh: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_setattr_in {
    pub valid: u32,
    pub padding: u32,
    pub fh: u64,
    pub size: u64,
    pub lock_owner: u64,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
    pub atimensec: u32,
    pub mtimensec: u32,
    pub ctimensec: u32,
    pub mode: u32,
    pub unused4: u32,
    pub uid: u32,
    pub gid: u32,
    pub unused5: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_open_in {
    pub flags: u32,
    pub open_flags: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_create_in {
    pub flags: u32,
    pub mode: u32,
    pub umask: u32,
    pub open_flags: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_read_in {
    pub fh: u64,
    pub offset: u64,
    pub size: u32,
    pub read_flags: u32,
    pub lock_owner: u64,
    pub flags: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_write_in {
    pub fh: u64,
    pub offset: u64,
    pub size: u32,
    pub write_flags: u32,
    pub lock_owner: u64,
    pub flags: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_release_in {
    pub fh: u64,
    pub flags: u32,
    pub release_flags: u32,
    pub lock_owner: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_flush_in {
    pub fh: u64,
    pub unused: u32,
    pub padding: u32,
    pub lock_owner: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_mkdir_in {
    pub mode: u32,
    pub umask: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_mknod_in {
    pub mode: u32,
    pub rdev: u32,
    pub umask: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_link_in {
    pub oldnodeid: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_rename_in {
    pub newdir: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_rename2_in {
    pub newdir: u64,
    pub flags: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_access_in {
    pub mask: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_forget_in {
    pub nlookup: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_forget_one {
    pub nodeid: u64,
    pub nlookup: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_batch_forget_in {
    pub count: u32,
    pub dummy: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_fsync_in {
    pub fh: u64,
    pub fsync_flags: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_poll_in {
    pub fh: u64,
    pub kh: u64,
    pub flags: u32,
    pub events: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_fallocate_in {
    pub fh: u64,
    pub offset: u64,
    pub length: u64,
    pub mode: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_lseek_in {
    pub fh: u64,
    pub offset: u64,
    pub whence: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_copy_file_range_in {
    pub fh_in: u64,
    pub off_in: u64,
    pub nodeid_out: u64,
    pub fh_out: u64,
    pub off_out: u64,
    pub len: u64,
    pub flags: u64,
}

// ---------- Response structures ----------

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_entry_out {
    pub nodeid: u64,
    pub generation: u64,
    pub entry_valid: u64,
    pub attr_valid: u64,
    pub entry_valid_nsec: u32,
    pub attr_valid_nsec: u32,
    pub attr: fuse_attr,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_attr_out {
    pub attr_valid: u64,
    pub attr_valid_nsec: u32,
    pub dummy: u32,
    pub attr: fuse_attr,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_open_out {
    pub fh: u64,
    pub open_flags: u32,
    pub backing_id: i32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_write_out {
    pub size: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_kstatfs {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u32,
    pub namelen: u32,
    pub frsize: u32,
    pub padding: u32,
    pub spare: [u32; 6],
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_statfs_out {
    pub st: fuse_kstatfs,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_poll_out {
    pub revents: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_lseek_out {
    pub offset: u64,
}

// ---------- Directory entry structures ----------

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_dirent {
    pub ino: u64,
    pub off: u64,
    pub namelen: u32,
    pub typ: u32,
    // Followed by name[namelen] then padding to 8-byte boundary
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_direntplus {
    pub entry_out: fuse_entry_out,
    pub dirent: fuse_dirent,
    // Followed by name[namelen] then padding to 8-byte boundary
}

// ---------- io_uring protocol structures ----------

/// Per-ring-entry metadata exchanged between kernel and userspace.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_uring_ent_in_out {
    pub flags: u64,
    pub commit_id: u64,
    pub payload_sz: u32,
    pub padding: u32,
    pub reserved: u64,
}

/// Header buffer layout for each ring entry.
/// Total: 128 + 128 + 32 = 288 bytes
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct fuse_uring_req_header {
    /// fuse_in_header (request from kernel) / fuse_out_header (response to kernel)
    pub in_out: [u8; FUSE_URING_IN_OUT_HEADER_SZ],
    /// Per-opcode arguments (fuse_open_in, fuse_read_in, etc.)
    pub op_in: [u8; FUSE_URING_OP_IN_OUT_SZ],
    /// Ring entry metadata
    pub ring_ent_in_out: fuse_uring_ent_in_out,
}

impl Default for fuse_uring_req_header {
    fn default() -> Self {
        Self {
            in_out: [0u8; FUSE_URING_IN_OUT_HEADER_SZ],
            op_in: [0u8; FUSE_URING_OP_IN_OUT_SZ],
            ring_ent_in_out: fuse_uring_ent_in_out::default(),
        }
    }
}

/// Command data embedded in the SQE's 80-byte cmd area.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_uring_cmd_req {
    pub flags: u64,
    pub commit_id: u64,
    pub qid: u16,
    pub padding: [u8; 6],
}

// ---------- Notification constants ----------
// Used for userspace-initiated cache invalidation via writes to /dev/fuse.
// The kernel dispatches on these in fuse_dev_do_write().

pub const FUSE_NOTIFY_INVAL_INODE: i32 = 2;
pub const FUSE_NOTIFY_INVAL_ENTRY: i32 = 3;
pub const FUSE_NOTIFY_DELETE: i32 = 6;

// ---------- Notification structures ----------

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_notify_inval_inode_out {
    pub ino: u64,
    pub off: i64,
    pub len: i64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_notify_inval_entry_out {
    pub parent: u64,
    pub namelen: u32,
    pub flags: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct fuse_notify_delete_out {
    pub parent: u64,
    pub child: u64,
    pub namelen: u32,
    pub padding: u32,
}

// Alignment helper for fuse_dirent/fuse_direntplus
pub const fn fuse_dirent_align(x: usize) -> usize {
    (x + 7) & !7
}

pub const fn fuse_dirent_size(namelen: usize) -> usize {
    fuse_dirent_align(std::mem::size_of::<fuse_dirent>() + namelen)
}

pub const fn fuse_direntplus_size(namelen: usize) -> usize {
    fuse_dirent_align(std::mem::size_of::<fuse_direntplus>() + namelen)
}

impl fuse_uring_req_header {
    pub fn in_header(&self) -> &fuse_in_header {
        unsafe { &*(self.in_out.as_ptr() as *const fuse_in_header) }
    }

    pub fn out_header_mut(&mut self) -> &mut fuse_out_header {
        unsafe { &mut *(self.in_out.as_mut_ptr() as *mut fuse_out_header) }
    }

    pub fn op_in_as<T>(&self) -> &T {
        assert!(std::mem::size_of::<T>() <= FUSE_URING_OP_IN_OUT_SZ);
        unsafe { &*(self.op_in.as_ptr() as *const T) }
    }

    pub fn op_in_as_mut<T>(&mut self) -> &mut T {
        assert!(std::mem::size_of::<T>() <= FUSE_URING_OP_IN_OUT_SZ);
        unsafe { &mut *(self.op_in.as_mut_ptr() as *mut T) }
    }
}

// Size assertions
const _: () = {
    assert!(std::mem::size_of::<fuse_in_header>() <= FUSE_URING_IN_OUT_HEADER_SZ);
    assert!(std::mem::size_of::<fuse_out_header>() <= FUSE_URING_IN_OUT_HEADER_SZ);
    assert!(std::mem::size_of::<fuse_uring_req_header>() == 288);
    assert!(std::mem::size_of::<fuse_uring_cmd_req>() == 24);
    assert!(std::mem::size_of::<fuse_uring_ent_in_out>() == 32);
};
