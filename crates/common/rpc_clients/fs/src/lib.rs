//! Client for the `fs_gateway` protocol. Linked only by `VfsCore` hosts
//! (`artfs-mount`); the gateway itself only uses the codec.

pub mod client;
pub mod rpc;

pub use client::RpcClient as RpcClientFs;
pub use fs_gateway_codec as codec;
pub use rpc_client_common::RpcError as RpcErrorFs;
