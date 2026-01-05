//! Blob storage server message format.
//! Note if this file is updated, the corresponding message.zig file also needs to be updated!
use bytemuck::{Pod, Zeroable};
use data_types::TraceId;
use rpc_codec_common::MessageHeaderTrait;
use std::mem::size_of;
use xxhash_rust::xxh3::{Xxh3, xxh3_64};

/// XXH3-64 hash of an empty buffer (seed=0)
/// This is the correct checksum value for empty message bodies
const EMPTY_BODY_CHECKSUM: u64 = 0x2d06800538d394c2;

#[repr(C)]
#[derive(Pod, Debug, Clone, Copy, Zeroable)]
pub struct MessageHeader {
    /// A checksum covering only the remainder of this header.
    /// This allows the header to be trusted without having to recv() or read() the associated body.
    pub checksum: u64,
    /// The current protocol version, note the position should never be changed
    /// so that we can upgrade proto version in the future.
    pub proto_version: u8,
    /// Number of retry attempts for this request (0 = first attempt)
    pub retry_count: u8,
    /// Volume ID for multi-BSS support
    pub volume_id: u16,
    /// The size of the Header structure, plus any associated body.
    pub size: u32,

    /// A checksum covering only the associated body after this header.
    pub checksum_body: u64,
    /// The protocol command (method) for this message. i32 size, defined as enum type
    pub command: Command,
    /// Every request would be sent with a unique id, so the client can get the right response
    pub id: u32,

    /// Bucket Id
    pub bucket_id: [u8; 16],

    /// Blob Id
    pub blob_id: [u8; 16],

    /// Version number for quorum protocol
    pub version: u64,
    /// The bss block number
    pub block_number: u32,
    /// Content (body) length
    pub content_len: u32,

    /// Trace ID for distributed tracing
    pub trace_id: u64,
    /// Errno which can be converted into `std::io::Error`(`from_raw_os_error()`)
    pub errno: i32,
    /// Flag to indicate if this is a new metadata blob (vs update)
    pub is_new: u8,
    /// Flag to indicate whether this blob is deleted
    pub is_deleted: u8,
    /// Reserved parts for padding
    /// TODO: will add device id, fence_token, nss-active-id, for meta-blob use
    pub reserved2: [u8; 2],
    pub reserved1: [u8; 32],
    pub reserved0: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(i32)]
pub enum Command {
    Invalid = 0,
    Handshake = 1, // Reserved for RPC handshake
    // Application-specific commands start from 16
    PutDataBlob = 16,
    GetDataBlob = 17,
    DeleteDataBlob = 18,
    PutMetadataBlob = 19,
    GetMetadataBlob = 20,
    DeleteMetadataBlob = 21,
}

#[allow(clippy::derivable_impls)]
impl Default for Command {
    fn default() -> Self {
        Command::Invalid
    }
}

// Safety: Command is defined as enum type (i32), and 0 as Invalid. There is also no padding
// as verified from the zig side. With header checksum validation, we can also be sure no invalid
// enum value being interpreted.
unsafe impl Pod for Command {}
unsafe impl Zeroable for Command {}

impl Default for MessageHeader {
    fn default() -> Self {
        Self {
            proto_version: Self::PROTO_VERSION,
            checksum: 0,
            size: 0,
            checksum_body: EMPTY_BODY_CHECKSUM,
            command: Command::Invalid,
            id: 0,
            bucket_id: [0u8; 16],
            blob_id: [0u8; 16],
            trace_id: 0,
            version: 0,
            block_number: 0,
            errno: 0,
            content_len: 0,
            volume_id: 0,
            retry_count: 0,
            is_new: 0,
            is_deleted: 0,
            reserved2: [0u8; 2],
            reserved1: [0u8; 32],
            reserved0: [0u8; 32],
        }
    }
}

impl MessageHeader {
    const _SIZE_OK: () = assert!(size_of::<Self>() == 160);
    pub const PROTO_VERSION: u8 = 1;

    /// Calculate and set the body checksum field.
    /// The checksum covers the message body after this header.
    pub fn set_body_checksum(&mut self, body: &[u8]) {
        self.checksum_body = xxh3_64(body);
    }

    /// Calculate and set the body checksum field for vectored I/O.
    /// The checksum covers all chunks combined.
    /// Uses streaming API since data is not contiguous.
    pub fn set_body_checksum_vectored(&mut self, chunks: &[bytes::Bytes]) {
        let mut hasher = Xxh3::new();
        for chunk in chunks {
            hasher.update(chunk);
        }
        self.checksum_body = hasher.digest();
    }
}

impl MessageHeaderTrait for MessageHeader {
    fn encode(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }

    fn decode(src: &[u8]) -> Self {
        bytemuck::pod_read_unaligned::<Self>(&src[..size_of::<Self>()]).to_owned()
    }

    fn get_size(&self) -> usize {
        self.size as usize
    }

    fn get_id(&self) -> u32 {
        self.id
    }

    fn get_trace_id(&self) -> TraceId {
        self.trace_id.into()
    }

    fn set_checksum(&mut self) {
        let header_bytes: &[u8] = bytemuck::bytes_of(self);
        let checksum_offset = std::mem::offset_of!(MessageHeader, checksum);
        let bytes_to_hash = &header_bytes[checksum_offset + size_of::<u64>()..size_of::<Self>()];

        self.checksum = xxh3_64(bytes_to_hash);
    }

    fn verify_body_checksum(&self, body: &[u8]) -> bool {
        let calculated = xxh3_64(body);
        self.checksum_body == calculated
    }
}
