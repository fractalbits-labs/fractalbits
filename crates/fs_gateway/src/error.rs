use fs_gateway_codec::{Error as WireError, ErrorKind};
use rpc_client_common::RpcError;
use thiserror::Error;
use volume_group_proxy::DataVgError;

/// Backend outcome inside the gateway. The subset of the old fs_server
/// `FsError` that the storage layer produces; everything filesystem
/// shaped now lives in the client.
#[derive(Error, Debug)]
pub enum FsError {
    #[error("not found")]
    NotFound,

    #[error("already exists")]
    AlreadyExists,

    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),

    #[error("DataVg error: {0}")]
    DataVg(#[from] DataVgError),

    #[error("invalid object state")]
    InvalidState,

    #[error("deserialization error: {0}")]
    Deserialize(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("cas conflict: stored value changed under the put_inode_cas guard")]
    CasConflict,

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("read-only session")]
    ReadOnly,

    /// The layout at the key no longer matches the version the client
    /// read against, or a row pair sits above every ceiling observed.
    #[error("layout changed under the read")]
    StaleLayout,

    /// A row-committed generation is missing on every replica.
    #[error("block data corrupted or lost")]
    Corrupted,
}

impl FsError {
    /// The addressed exact generation does not exist on enough nodes.
    pub fn is_block_missing(&self) -> bool {
        matches!(
            self,
            FsError::DataVg(DataVgError::BlockNotFound)
                | FsError::DataVg(DataVgError::BssRpc(RpcError::NotFound))
                | FsError::Rpc(RpcError::NotFound)
        )
    }

    pub fn to_wire(&self) -> WireError {
        let kind = match self {
            FsError::NotFound => ErrorKind::NotFound,
            FsError::AlreadyExists => ErrorKind::AlreadyExists,
            FsError::CasConflict => ErrorKind::CasConflict,
            FsError::InvalidState => ErrorKind::InvalidState,
            FsError::Deserialize(_) => ErrorKind::Deserialize,
            FsError::Unauthorized(_) => ErrorKind::Unauthorized,
            FsError::ReadOnly => ErrorKind::ReadOnly,
            FsError::StaleLayout => ErrorKind::StaleLayout,
            FsError::Corrupted => ErrorKind::Corrupted,
            FsError::Rpc(RpcError::NotFound) => ErrorKind::NotFound,
            FsError::Rpc(RpcError::AlreadyExists) => ErrorKind::AlreadyExists,
            FsError::Rpc(RpcError::NoSpace) => ErrorKind::NoSpace,
            FsError::Rpc(RpcError::Mismatch) => ErrorKind::Mismatch,
            FsError::Rpc(_) => ErrorKind::Internal,
            FsError::DataVg(DataVgError::BlockNotFound)
            | FsError::DataVg(DataVgError::BssRpc(RpcError::NotFound)) => ErrorKind::BlockNotFound,
            FsError::DataVg(DataVgError::BssRpc(RpcError::NoSpace)) => ErrorKind::NoSpace,
            FsError::DataVg(DataVgError::BssRpc(RpcError::Mismatch)) => ErrorKind::Mismatch,
            FsError::DataVg(DataVgError::Corrupted) => ErrorKind::Corrupted,
            FsError::DataVg(_) => ErrorKind::Internal,
            FsError::Internal(_) => ErrorKind::Internal,
        };
        WireError::new(kind, self.to_string())
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
            // The gateway does not model "bucket gone" as a distinct case;
            // the client surfaces this as NotFound, same as a missing inode.
            file_ops::NssError::NoSuchRootBlob => FsError::NotFound,
            file_ops::NssError::AlreadyExists => FsError::AlreadyExists,
            file_ops::NssError::Internal(msg) => FsError::Internal(msg),
            file_ops::NssError::Deserialization(msg) => FsError::Deserialize(msg),
            file_ops::NssError::CasConflict(_) => FsError::CasConflict,
        }
    }
}
