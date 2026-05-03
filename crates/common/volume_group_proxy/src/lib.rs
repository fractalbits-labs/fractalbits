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

    #[error("Internal error: {0}")]
    Internal(String),
}
