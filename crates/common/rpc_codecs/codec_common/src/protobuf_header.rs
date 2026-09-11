use bytemuck::{Pod, Zeroable};
use data_types::TraceId;
use xxhash_rust::xxh3::xxh3_64;

use crate::{MessageHeaderTrait, ProtobufRequestHeader};

/// XXH3-64 hash of an empty buffer (seed=0)
/// This is the correct checksum value for empty message bodies
pub const EMPTY_BODY_CHECKSUM: u64 = 0x2d06800538d394c2;

/// Wire header shared by the protobuf-based RPC protocols.
///
/// The command is stored as its raw protobuf `i32` value so every bit
/// pattern read off the wire is a valid header; servers validate it with
/// the protocol enum's `TryFrom<i32>` before dispatch. `Pod` is derived,
/// which has the compiler check the `repr(C)`, no-padding and all-fields-
/// `Pod` requirements the raw decode relies on.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy, Pod, Zeroable)]
pub struct ProtobufMessageHeader {
    /// A checksum covering only the remainder of this header.
    /// This allows the header to be trusted without having to recv() or read() the associated body.
    pub checksum: u64,
    /// The current protocol version, note the position should never be changed
    /// so that we can upgrade proto version in the future.
    pub proto_version: u8,
    /// Number of retry attempts for this request (0 = first attempt)
    pub retry_count: u8,
    /// Reserved for future use
    pub _reserved0: u16,
    /// The size of the Header structure, plus any associated body.
    pub size: u32,

    /// A checksum covering only the associated body after this header.
    pub checksum_body: u64,
    /// Every request would be sent with a unique id, so the client can get the right response
    pub id: u32,
    /// Raw protobuf command value. Keeping this as `i32` makes every wire bit
    /// pattern valid until the protocol-specific server validates it.
    pub command: i32,

    /// Trace ID for distributed tracing
    pub trace_id: u64,
    pub _reserved1: u64,
}

impl ProtobufMessageHeader {
    const _SIZE_OK: () = assert!(size_of::<Self>() == 48);

    /// Calculate and set the checksum field for this header.
    /// The checksum covers all header fields after the checksum field itself.
    pub fn set_checksum(&mut self) {
        let checksum_offset = std::mem::offset_of!(Self, checksum);
        let bytes: &[u8] = bytemuck::bytes_of(self);
        let bytes_to_hash = &bytes[checksum_offset + size_of::<u64>()..size_of::<Self>()];
        self.checksum = xxh3_64(bytes_to_hash);
    }

    /// Calculate and set the body checksum field.
    /// The checksum covers the message body after this header.
    pub fn set_body_checksum(&mut self, body: &[u8]) {
        self.checksum_body = if body.is_empty() {
            EMPTY_BODY_CHECKSUM
        } else {
            xxh3_64(body)
        };
    }

    /// Verify that the body checksum field matches the calculated checksum.
    /// Returns true if valid, false otherwise.
    pub fn verify_body_checksum(&self, body: &[u8]) -> bool {
        let calculated = if body.is_empty() {
            EMPTY_BODY_CHECKSUM
        } else {
            xxh3_64(body)
        };
        self.checksum_body == calculated
    }

    pub fn set_trace_id(&mut self, trace_id: &TraceId) {
        self.trace_id = trace_id.0;
    }
}

impl ProtobufRequestHeader for ProtobufMessageHeader {
    fn set_request(&mut self, id: u32, command: i32, retry_count: u8, trace_id: &TraceId) {
        self.id = id;
        self.command = command;
        self.retry_count = retry_count;
        self.set_trace_id(trace_id);
    }

    fn set_body(&mut self, body: &[u8]) {
        self.size = (size_of::<Self>() + body.len()) as u32;
        self.set_body_checksum(body);
    }
}

impl MessageHeaderTrait for ProtobufMessageHeader {
    fn encode(&self) -> &[u8] {
        bytemuck::bytes_of(self)
    }

    fn decode(src: &[u8]) -> Self {
        bytemuck::pod_read_unaligned::<Self>(&src[..size_of::<Self>()])
    }

    fn get_size(&self) -> usize {
        self.size as usize
    }

    fn get_id(&self) -> u32 {
        self.id
    }

    fn get_body_size(&self) -> usize {
        (self.size as usize).saturating_sub(size_of::<Self>())
    }

    fn get_trace_id(&self) -> TraceId {
        TraceId::from(self.trace_id)
    }

    fn set_checksum(&mut self) {
        self.set_checksum()
    }

    fn verify_body_checksum(&self, body: &[u8]) -> bool {
        self.verify_body_checksum(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arbitrary_wire_command_decodes_as_raw_integer() {
        let mut header = ProtobufMessageHeader {
            command: i32::MIN,
            size: size_of::<ProtobufMessageHeader>() as u32,
            ..Default::default()
        };
        header.set_checksum();
        let wire = header.encode().to_vec();

        assert!(ProtobufMessageHeader::verify_header_checksum_raw(&wire));
        let decoded = ProtobufMessageHeader::decode(&wire);
        assert_eq!(decoded.command, i32::MIN);
    }
}
