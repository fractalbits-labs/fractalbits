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

    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),

    #[error("DataVg error: {0}")]
    DataVg(#[from] volume_group_proxy::DataVgError),

    #[error("invalid object state")]
    InvalidState,

    #[error("deserialization error: {0}")]
    Deserialize(String),

    #[error("internal error: {0}")]
    Internal(String),
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
            FsError::Rpc(ref e) => {
                if e.retryable() {
                    io::Error::from_raw_os_error(libc::EAGAIN)
                } else {
                    io::Error::from_raw_os_error(libc::EIO)
                }
            }
            FsError::DataVg(_) => io::Error::from_raw_os_error(libc::EIO),
            FsError::InvalidState => io::Error::from_raw_os_error(libc::EINVAL),
            FsError::Deserialize(_) => io::Error::from_raw_os_error(libc::EIO),
            FsError::Internal(_) => io::Error::from_raw_os_error(libc::EIO),
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
        }
    }
}
