//! Wire protocol between `fractalbits-mount` (client) and `fs_gateway`.
//!
//! The command set is the storage-backend surface `VfsCore` consumes:
//! inode primitives, block primitives, and two best-effort hints
//! (prefetch, sweep) for the gateway-side caches and reclamation.

pub mod message;

pub use message::MessageHeader;

// Type alias for shared codec implementation
pub type MessageCodec = rpc_codec_common::MessageCodec<MessageHeader>;

// Re-export protobuf generated types
include!(concat!(env!("OUT_DIR"), "/fs_gateway_ops.rs"));

// Implement RpcCodec trait
use rpc_client_common::RpcCodec;
impl RpcCodec<MessageHeader> for MessageCodec {
    const RPC_TYPE: &'static str = "fs_gateway";
}

use data_types::DataBlobGuid;

impl From<DataBlobGuid> for BlobGuid {
    fn from(guid: DataBlobGuid) -> Self {
        Self {
            blob_id: bytes::Bytes::copy_from_slice(guid.blob_id.as_bytes()),
            volume_id: guid.volume_id as u32,
        }
    }
}

impl TryFrom<&BlobGuid> for DataBlobGuid {
    type Error = String;

    fn try_from(guid: &BlobGuid) -> Result<Self, Self::Error> {
        let blob_id =
            uuid::Uuid::from_slice(&guid.blob_id).map_err(|e| format!("invalid blob id: {e}"))?;
        let volume_id =
            u16::try_from(guid.volume_id).map_err(|_| "volume id out of range".to_string())?;
        Ok(Self { blob_id, volume_id })
    }
}

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind: kind as i32,
            message: message.into(),
        }
    }

    pub fn error_kind(&self) -> ErrorKind {
        ErrorKind::try_from(self.kind).unwrap_or(ErrorKind::Internal)
    }
}

/// HMAC-SHA256 over `key_id | bucket | timestamp_ms | nonce`, shared by
/// the mount client and the gateway so both sides sign the same bytes.
pub fn mount_signature(
    secret_key: &[u8],
    key_id: &str,
    bucket: &str,
    timestamp_ms: u64,
    nonce: &[u8],
) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    let mut mac =
        Hmac::<sha2::Sha256>::new_from_slice(secret_key).expect("hmac accepts any key length");
    mac.update(key_id.as_bytes());
    mac.update(b"\n");
    mac.update(bucket.as_bytes());
    mac.update(b"\n");
    mac.update(&timestamp_ms.to_le_bytes());
    mac.update(nonce);
    mac.finalize().into_bytes().to_vec()
}
