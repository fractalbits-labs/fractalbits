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
    /// When set to 1, skip fence token validation (used by repair service)
    pub skip_fence_token: u8,
    /// Set only for the NSS metadata root blob.
    pub is_root: u8,
    /// Fence token for fencing stale NSS instances
    pub fence_token: u64,
    /// Commit envelope: file `total_size` after a BssBatch lands, or
    /// the stored value returned by a GetBlobInfo response. Zero on
    /// other commands.
    pub commit_total_size: u64,
    /// Commit envelope: `ceil(total_size / block_size)` after a
    /// BssBatch lands, or the stored value returned by a GetBlobInfo
    /// response. Zero on other commands.
    pub commit_block_count: u32,
    /// Reserved padding (was reserve1; carved 12 B for commit_*).
    pub reserve1: [u8; 12],
    pub reserve2: [u8; 32],
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
    ListBlobs = 22,
    // 23 / 24 reserved for PutJournal / GetJournal on the BSS side --
    // not used by the Rust client today; keep contiguous numbering.
    ReserveBlocks = 25,
    ListBlobBlocks = 26,
    /// Batched block mutations: one outer header followed by N
    /// (sub_header, sub_body) records in the body. Each sub_header is
    /// the same `MessageHeader` type with its own per-entry command,
    /// blob_id, version, block_number, content_len; the sub_body is
    /// `sub_header.content_len` bytes of payload. Server walks the
    /// body, dispatches each sub-record through the existing per-op
    /// handler, returns a response body of N (sub_header_with_errno,
    /// optional sub_body) records reporting per-entry status.
    BssBatch = 27,
    /// Read the BSS-recorded commit info for a blob and return
    /// `(total_size, block_count, blob_version)` in the response
    /// header's `commit_total_size`, `commit_block_count`, and
    /// `version` fields. Request carries the target `blob_id` and an
    /// `expected version` (0 = any). No request or response body.
    /// Used by fs_server's `get_blob_info` to size files on open and
    /// to anchor lseek SEEK_HOLE.
    GetBlobInfo = 28,
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
            skip_fence_token: 0,
            is_root: 0,
            fence_token: 0,
            commit_total_size: 0,
            commit_block_count: 0,
            reserve1: [0u8; 12],
            reserve2: [0u8; 32],
        }
    }
}

impl MessageHeader {
    const _SIZE_OK: () = assert!(size_of::<Self>() == 160);
    const _FENCE_TOKEN_OFFSET_OK: () = assert!(std::mem::offset_of!(Self, fence_token) == 96);
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
