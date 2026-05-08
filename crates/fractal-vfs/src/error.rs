use rpc_client_common::RpcError;
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FsError {
    #[error("not found")]
    NotFound,

    #[error("already exists")]
    AlreadyExists,

    #[error("directory not empty")]
    NotEmpty,

    #[error("is a directory")]
    IsDir,

    #[error("not a directory")]
    NotDir,

    #[error("read-only filesystem")]
    ReadOnly,

    #[error("bad file descriptor")]
    BadFd,

    #[error("file is busy: another writer holds the inode-scoped write lock")]
    Busy,

    /// CAS guard mismatched on a flush. Surface as ESTALE so userspace
    /// notices the cross-instance conflict and reopens the file.
    #[error("flush CAS conflict: another writer published a new version first")]
    CasConflict,

    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),

    #[error("DataVg error: {0}")]
    DataVg(#[from] volume_group_proxy::DataVgError),

    #[error("invalid object state")]
    InvalidState,

    /// `lseek(SEEK_HOLE | SEEK_DATA)` past EOF, or `SEEK_DATA` with no
    /// further data in the file. Maps to ENXIO.
    #[error("no such device or address")]
    NoData,

    /// Invalid argument to a syscall (e.g. unknown fallocate mode bits,
    /// unsupported lseek `whence`). Maps to EINVAL.
    #[error("invalid argument")]
    InvalidArg,

    #[error("deserialization error: {0}")]
    Deserialize(String),

    #[error("internal error: {0}")]
    Internal(String),

    /// Path component longer than NAME_MAX (255). Maps to
    /// ENAMETOOLONG.
    #[error("name too long")]
    NameTooLong,

    /// Permission denied (typically chmod / chown / unlink in a
    /// sticky directory by a non-owner). Maps to EPERM. The kernel
    /// already gates a lot of these with `default_permissions`, but
    /// fs_server is the source of truth for the FUSE_SETATTR /
    /// FUSE_UNLINK / FUSE_RENAME paths when we don't pass that flag.
    #[error("operation not permitted")]
    PermissionDenied,

    /// Search permission denied on a path component (typically a
    /// directory the caller doesn't have execute on). Maps to EACCES.
    #[error("permission denied")]
    AccessDenied,
}

impl From<FsError> for io::Error {
    fn from(e: FsError) -> Self {
        match e {
            FsError::NotFound => io::Error::from_raw_os_error(libc::ENOENT),
            FsError::AlreadyExists => io::Error::from_raw_os_error(libc::EEXIST),
            FsError::NotEmpty => io::Error::from_raw_os_error(libc::ENOTEMPTY),
            FsError::IsDir => io::Error::from_raw_os_error(libc::EISDIR),
            FsError::NotDir => io::Error::from_raw_os_error(libc::ENOTDIR),
            FsError::ReadOnly => io::Error::from_raw_os_error(libc::EROFS),
            FsError::BadFd => io::Error::from_raw_os_error(libc::EBADF),
            FsError::Busy => io::Error::from_raw_os_error(libc::EBUSY),
            FsError::CasConflict => io::Error::from_raw_os_error(libc::ESTALE),
            FsError::Rpc(ref e) => {
                if e.retryable() {
                    io::Error::from_raw_os_error(libc::EAGAIN)
                } else {
                    io::Error::from_raw_os_error(libc::EIO)
                }
            }
            FsError::DataVg(_) => io::Error::from_raw_os_error(libc::EIO),
            FsError::InvalidState => io::Error::from_raw_os_error(libc::EINVAL),
            FsError::NoData => io::Error::from_raw_os_error(libc::ENXIO),
            FsError::InvalidArg => io::Error::from_raw_os_error(libc::EINVAL),
            FsError::Deserialize(_) => io::Error::from_raw_os_error(libc::EIO),
            FsError::Internal(_) => io::Error::from_raw_os_error(libc::EIO),
            FsError::NameTooLong => io::Error::from_raw_os_error(libc::ENAMETOOLONG),
            FsError::PermissionDenied => io::Error::from_raw_os_error(libc::EPERM),
            FsError::AccessDenied => io::Error::from_raw_os_error(libc::EACCES),
        }
    }
}

impl From<FsError> for fractal_fuse::Errno {
    fn from(e: FsError) -> Self {
        let io_err: io::Error = e.into();
        io_err.raw_os_error().unwrap_or(libc::EIO)
    }
}

impl From<data_types::object_layout::ObjectLayoutError> for FsError {
    fn from(_: data_types::object_layout::ObjectLayoutError) -> Self {
        FsError::InvalidState
    }
}

impl From<rkyv::rancor::Error> for FsError {
    fn from(e: rkyv::rancor::Error) -> Self {
        FsError::Deserialize(e.to_string())
    }
}

impl From<file_ops::NssError> for FsError {
    fn from(e: file_ops::NssError) -> Self {
        match e {
            file_ops::NssError::NotFound => FsError::NotFound,
            // fs_server doesn't model "bucket gone" as a distinct case; the
            // caller surfaces this as NotFound, same as a missing inode.
            file_ops::NssError::NoSuchRootBlob => FsError::NotFound,
            file_ops::NssError::AlreadyExists => FsError::AlreadyExists,
            file_ops::NssError::Internal(msg) => FsError::Internal(msg),
            file_ops::NssError::Deserialization(msg) => FsError::Deserialize(msg),
            // The conflict bytes are dropped on this conversion -- the
            // override flush path uses the typed CasConflict variant
            // directly so it can act on the bytes before lowering.
            file_ops::NssError::CasConflict(_) => FsError::CasConflict,
        }
    }
}
