pub mod message;

pub use message::{Command, MessageHeader};

// Type alias for shared codec implementation
pub type MessageCodec = rpc_codec_common::MessageCodec<MessageHeader>;

// Re-export protobuf generated types
include!(concat!(env!("OUT_DIR"), "/bss_ops.rs"));

// Implement RpcCodec trait
use rpc_client_common::RpcCodec;
impl RpcCodec<MessageHeader> for MessageCodec {
    const RPC_TYPE: &'static str = "bss";
}
