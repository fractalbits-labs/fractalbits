pub mod data_vg_proxy;
pub use data_vg_proxy::{CircuitBreakerConfig, DataVgProxy, VolumeSelectionPolicy};

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

    /// All responding replicas (or, on EC, all reachable data shards)
    /// agreed the block does not exist. The caller can treat this as a
    /// sparse-file hole and substitute zeros rather than a quorum failure.
    #[error("Block not found on any replica")]
    BlockNotFound,

    /// Two or more replicas reported the same version but disagree on body
    /// length / checksum, a divergence the inline-repair read path can't
    /// resolve safely; surfaced for operator investigation.
    #[error("Replicated data divergence: same version, different bytes")]
    Corrupted,

    #[error("Internal error: {0}")]
    Internal(String),
}
