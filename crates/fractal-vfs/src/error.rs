use fs_gateway_codec::ErrorKind;
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

    #[error("file name too long")]
    NameTooLong,

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

    /// Transport failure between this mount and the gateway.
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),

    /// The addressed exact block generation exists on no replica.
    #[error("block not found")]
    BlockNotFound,

    /// A row-committed generation is missing or the replicas disagree:
    /// detected data loss, never served as zeros.
    #[error("block data corrupted or lost")]
    Corrupted,

    #[error("invalid object state")]
    InvalidState,

    #[error("invalid argument")]
    InvalidArg,

    #[error("no data available at offset (lseek SEEK_DATA past EOF / SEEK_HOLE/DATA beyond end)")]
    NoData,

    #[error("deserialization error: {0}")]
    Deserialize(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("cas conflict: stored value changed under the put_inode_cas guard")]
    CasConflict,

    #[error("layout may have changed while reading sparse block {1} of {0}")]
    StaleLayout(data_types::DataBlobGuid, u32),

    /// The gateway rejected the session even after a re-mount.
    #[error("unauthorized: {0}")]
    Unauthorized(String),
}

impl FsError {
    /// The addressed exact generation does not exist on enough nodes.
    pub(crate) fn is_block_missing(&self) -> bool {
        matches!(
            self,
            FsError::BlockNotFound | FsError::Rpc(RpcError::NotFound)
        )
    }
}

impl From<FsError> for io::Error {
    fn from(e: FsError) -> Self {
        match e {
            FsError::NotFound => io::Error::from_raw_os_error(libc::ENOENT),
            FsError::AlreadyExists => io::Error::from_raw_os_error(libc::EEXIST),
            FsError::NotEmpty => io::Error::from_raw_os_error(libc::ENOTEMPTY),
            FsError::NameTooLong => io::Error::from_raw_os_error(libc::ENAMETOOLONG),
            FsError::IsDir => io::Error::from_raw_os_error(libc::EISDIR),
            FsError::NotDir => io::Error::from_raw_os_error(libc::ENOTDIR),
            FsError::ReadOnly => io::Error::from_raw_os_error(libc::EROFS),
            FsError::BadFd => io::Error::from_raw_os_error(libc::EBADF),
            FsError::Busy => io::Error::from_raw_os_error(libc::EBUSY),
            FsError::Rpc(RpcError::NoSpace) => io::Error::from_raw_os_error(libc::ENOSPC),
            FsError::Rpc(ref e) => {
                if e.retryable() {
                    io::Error::from_raw_os_error(libc::EAGAIN)
                } else {
                    io::Error::from_raw_os_error(libc::EIO)
                }
            }
            FsError::BlockNotFound => io::Error::from_raw_os_error(libc::EIO),
            FsError::Corrupted => io::Error::from_raw_os_error(libc::EIO),
            FsError::InvalidState => io::Error::from_raw_os_error(libc::EINVAL),
            FsError::InvalidArg => io::Error::from_raw_os_error(libc::EINVAL),
            FsError::NoData => io::Error::from_raw_os_error(libc::ENXIO),
            FsError::Deserialize(_) => io::Error::from_raw_os_error(libc::EIO),
            FsError::Internal(_) => io::Error::from_raw_os_error(libc::EIO),
            // A CAS conflict means the guarded inode bytes changed before
            // this publish. ESTALE is the honest kernel result.
            FsError::CasConflict => io::Error::from_raw_os_error(libc::ESTALE),
            FsError::StaleLayout(_, _) => io::Error::from_raw_os_error(libc::ESTALE),
            FsError::Unauthorized(_) => io::Error::from_raw_os_error(libc::EACCES),
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

/// Gateway outcomes keep their kind so retry and CAS-rebase logic works
/// over the wire exactly as it did against the in-process backend.
impl From<fs_gateway_codec::Error> for FsError {
    fn from(e: fs_gateway_codec::Error) -> Self {
        match e.error_kind() {
            ErrorKind::NotFound => FsError::NotFound,
            ErrorKind::AlreadyExists => FsError::AlreadyExists,
            ErrorKind::CasConflict => FsError::CasConflict,
            ErrorKind::NoSpace => FsError::Rpc(RpcError::NoSpace),
            ErrorKind::Mismatch => FsError::Rpc(RpcError::Mismatch),
            ErrorKind::BlockNotFound => FsError::BlockNotFound,
            ErrorKind::Corrupted => FsError::Corrupted,
            ErrorKind::InvalidState => FsError::InvalidState,
            ErrorKind::Unauthorized => FsError::Unauthorized(e.message),
            ErrorKind::ReadOnly => FsError::ReadOnly,
            ErrorKind::Deserialize => FsError::Deserialize(e.message),
            ErrorKind::Internal => FsError::Internal(e.message),
        }
    }
}
