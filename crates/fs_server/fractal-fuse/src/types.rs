use std::time::Duration;

use crate::abi;

pub type Inode = u64;
pub type Errno = i32;

// Standard errno values
pub const ENOSYS: Errno = libc::ENOSYS;
pub const ENOENT: Errno = libc::ENOENT;
pub const EIO: Errno = libc::EIO;
pub const ENOTDIR: Errno = libc::ENOTDIR;
pub const EISDIR: Errno = libc::EISDIR;
pub const EEXIST: Errno = libc::EEXIST;
pub const ENOTEMPTY: Errno = libc::ENOTEMPTY;
pub const EACCES: Errno = libc::EACCES;
pub const EPERM: Errno = libc::EPERM;
pub const EINVAL: Errno = libc::EINVAL;
pub const ENOSPC: Errno = libc::ENOSPC;
pub const ENAMETOOLONG: Errno = libc::ENAMETOOLONG;
pub const ERANGE: Errno = libc::ERANGE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    RegularFile,
    Directory,
    Symlink,
    BlockDevice,
    CharDevice,
    NamedPipe,
    Socket,
}

impl FileType {
    pub fn to_mode(self) -> u32 {
        match self {
            FileType::RegularFile => libc::S_IFREG,
            FileType::Directory => libc::S_IFDIR,
            FileType::Symlink => libc::S_IFLNK,
            FileType::BlockDevice => libc::S_IFBLK,
            FileType::CharDevice => libc::S_IFCHR,
            FileType::NamedPipe => libc::S_IFIFO,
            FileType::Socket => libc::S_IFSOCK,
        }
    }

    pub fn to_dirent_type(self) -> u32 {
        match self {
            FileType::RegularFile => libc::DT_REG as u32,
            FileType::Directory => libc::DT_DIR as u32,
            FileType::Symlink => libc::DT_LNK as u32,
            FileType::BlockDevice => libc::DT_BLK as u32,
            FileType::CharDevice => libc::DT_CHR as u32,
            FileType::NamedPipe => libc::DT_FIFO as u32,
            FileType::Socket => libc::DT_SOCK as u32,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Timestamp {
    pub sec: u64,
    pub nsec: u32,
}

impl Timestamp {
    pub fn new(sec: u64, nsec: u32) -> Self {
        Self { sec, nsec }
    }
}

impl From<Duration> for Timestamp {
    fn from(d: Duration) -> Self {
        Self {
            sec: d.as_secs(),
            nsec: d.subsec_nanos(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FileAttr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime: Timestamp,
    pub mtime: Timestamp,
    pub ctime: Timestamp,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
}

impl FileAttr {
    pub fn to_fuse_attr(&self) -> abi::fuse_attr {
        abi::fuse_attr {
            ino: self.ino,
            size: self.size,
            blocks: self.blocks,
            atime: self.atime.sec,
            mtime: self.mtime.sec,
            ctime: self.ctime.sec,
            atimensec: self.atime.nsec,
            mtimensec: self.mtime.nsec,
            ctimensec: self.ctime.nsec,
            mode: self.mode,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: self.rdev,
            blksize: self.blksize,
            flags: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct FileStatx {
    pub mask: u32,
    pub blksize: u32,
    pub attributes: u64,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub mode: u16,
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub attributes_mask: u64,
    pub atime: Timestamp,
    pub mtime: Timestamp,
    pub ctime: Timestamp,
    pub btime: Timestamp,
    pub rdev: (u32, u32),
    pub dev: (u32, u32),
}

impl From<FileStatx> for abi::fuse_statx {
    fn from(value: FileStatx) -> Self {
        abi::fuse_statx {
            mask: value.mask,
            blksize: value.blksize,
            attributes: value.attributes,
            nlink: value.nlink,
            uid: value.uid,
            gid: value.gid,
            mode: value.mode,
            ino: value.ino,
            size: value.size,
            blocks: value.blocks,
            attributes_mask: value.attributes_mask,
            atime: value.atime.into(),
            btime: value.btime.into(),
            ctime: value.ctime.into(),
            mtime: value.mtime.into(),
            rdev_major: value.rdev.0,
            rdev_minor: value.rdev.1,
            dev_major: value.dev.0,
            dev_minor: value.dev.1,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SetAttr {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<SetAttrTime>,
    pub mtime: Option<SetAttrTime>,
    pub ctime: Option<Timestamp>,
    pub fh: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub enum SetAttrTime {
    Now,
    Specific(Timestamp),
}

impl SetAttr {
    pub fn from_raw(raw: &abi::fuse_setattr_in) -> Self {
        let valid = raw.valid;
        Self {
            mode: if valid & abi::FATTR_MODE != 0 {
                Some(raw.mode)
            } else {
                None
            },
            uid: if valid & abi::FATTR_UID != 0 {
                Some(raw.uid)
            } else {
                None
            },
            gid: if valid & abi::FATTR_GID != 0 {
                Some(raw.gid)
            } else {
                None
            },
            size: if valid & abi::FATTR_SIZE != 0 {
                Some(raw.size)
            } else {
                None
            },
            atime: if valid & abi::FATTR_ATIME_NOW != 0 {
                Some(SetAttrTime::Now)
            } else if valid & abi::FATTR_ATIME != 0 {
                Some(SetAttrTime::Specific(Timestamp::new(
                    raw.atime,
                    raw.atimensec,
                )))
            } else {
                None
            },
            mtime: if valid & abi::FATTR_MTIME_NOW != 0 {
                Some(SetAttrTime::Now)
            } else if valid & abi::FATTR_MTIME != 0 {
                Some(SetAttrTime::Specific(Timestamp::new(
                    raw.mtime,
                    raw.mtimensec,
                )))
            } else {
                None
            },
            ctime: if valid & abi::FATTR_CTIME != 0 {
                Some(Timestamp::new(raw.ctime, raw.ctimensec))
            } else {
                None
            },
            fh: if valid & abi::FATTR_FH != 0 {
                Some(raw.fh)
            } else {
                None
            },
        }
    }
}

/// Context from a FUSE request
#[derive(Debug, Clone, Copy)]
pub struct Request {
    pub unique: u64,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
}

// ---------- Reply types ----------

#[derive(Debug)]
pub struct ReplyInit {
    pub max_write: u32,
    pub max_readahead: u32,
    pub max_background: u16,
    pub congestion_threshold: u16,
}

impl Default for ReplyInit {
    fn default() -> Self {
        Self {
            max_write: 1024 * 1024,
            max_readahead: 1024 * 1024,
            max_background: 16,
            congestion_threshold: 12,
        }
    }
}

#[derive(Debug)]
pub struct ReplyEntry {
    pub ttl: Duration,
    pub attr: FileAttr,
    pub generation: u64,
}

#[derive(Debug)]
pub struct ReplyAttr {
    pub ttl: Duration,
    pub attr: FileAttr,
}

#[derive(Debug)]
pub struct ReplyOpen {
    pub fh: u64,
    pub flags: u32,
    pub backing_id: i32,
}

#[derive(Debug)]
pub struct ReplyCreate {
    pub ttl: Duration,
    pub attr: FileAttr,
    pub generation: u64,
    pub fh: u64,
    pub flags: u32,
}

#[derive(Debug)]
pub struct ReplyStatfs {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u32,
    pub namelen: u32,
    pub frsize: u32,
}

#[derive(Debug)]
pub struct DirectoryEntry {
    pub ino: u64,
    pub offset: u64,
    pub kind: FileType,
    pub name: Vec<u8>,
}

#[derive(Debug)]
pub struct DirectoryEntryPlus {
    pub ino: u64,
    pub offset: u64,
    pub kind: FileType,
    pub name: Vec<u8>,
    pub entry_ttl: Duration,
    pub attr: FileAttr,
    pub generation: u64,
}

#[derive(Debug)]
pub struct ReplyStatx {
    pub ttl: Duration,
    pub flags: u32,
    pub stat: FileStatx,
}

#[derive(Debug)]
pub struct ReplyReadlink {
    pub data: Vec<u8>,
}
