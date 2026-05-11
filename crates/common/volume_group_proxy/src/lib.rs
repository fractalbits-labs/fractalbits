pub mod data_vg_proxy;
pub use data_vg_proxy::{CircuitBreakerConfig, DataVgProxy};

#[derive(Debug, thiserror::Error)]
pub enum DataVgError {
    #[error("BSS RPC error: {0}")]
    BssRpc(#[from] rpc_client_common::RpcError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Initialization error: {0}")]
    InitializationError(String),

    #[error("Quorum failure: {0}")]
    QuorumFailure(String),

    #[error("Stale version: expected {expected}, all reachable replicas returned older versions")]
    StaleVersion { expected: u64 },

    /// All responding replicas agreed the block does not exist. The
    /// caller can treat this as a sparse-file hole and substitute
    /// zeros, rather than treating it as a quorum failure.
    #[error("Block not found on any replica")]
    BlockNotFound,

    /// Two or more replicas reported the same `BlobMeta.version` but
    /// disagree on body length or checksum. This is the divergence
    /// case the inline-repair read path cannot resolve safely; the
    /// caller must surface it for operator investigation rather than
    /// silently picking one cohort.
    #[error("Replicated data divergence: same version, different bytes")]
    Corrupted,

    #[error("Internal error: {0}")]
    Internal(String),
}
