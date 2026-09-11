use bytes::Bytes;
use data_types::TraceId;
use std::mem::size_of;
use xxhash_rust::xxh3::xxh3_64;

pub mod protobuf_header;
pub use protobuf_header::{EMPTY_BODY_CHECKSUM, ProtobufMessageHeader};

pub trait MessageHeaderTrait: Sized + Clone + Copy + Send + Sync + 'static {
    fn encode(&self) -> &[u8];

    fn decode(src: &[u8]) -> Self;

    fn get_id(&self) -> u32;

    fn get_trace_id(&self) -> TraceId;

    fn get_size(&self) -> usize;

    fn get_body_size(&self) -> usize {
        self.get_size().saturating_sub(size_of::<Self>())
    }

    // Default implementation to verify header checksum on raw bytes BEFORE decoding, to prevent UB
    // from corrupted members. We assume the checksum member is at the beginning, and using xxh3 u64
    // as checksum algorithm; otherwise, the user needs to provide their own implementation.
    fn verify_header_checksum_raw(header_bytes: &[u8]) -> bool {
        const CHECKSUM_OFFSET: usize = 0;
        if header_bytes.len() < size_of::<Self>() {
            return false;
        }

        let checksum_offset = CHECKSUM_OFFSET;
        let stored_checksum = u64::from_le_bytes(
            header_bytes[checksum_offset..checksum_offset + size_of::<u64>()]
                .try_into()
                .expect("slice is exactly 8 bytes"),
        );

        let bytes_to_hash = &header_bytes[checksum_offset + size_of::<u64>()..size_of::<Self>()];
        let calculated = xxh3_64(bytes_to_hash);

        stored_checksum == calculated
    }

    fn verify_body_checksum(&self, body: &[u8]) -> bool;

    fn set_checksum(&mut self);
}

pub struct MessageFrame<H: MessageHeaderTrait, B = Bytes> {
    pub header: H,
    pub body: B,
}

impl<H: MessageHeaderTrait, B> MessageFrame<H, B> {
    pub fn new(header: H, body: B) -> Self {
        Self { header, body }
    }
}

impl<H: MessageHeaderTrait> MessageFrame<H, Bytes> {
    pub fn from_bytes(header: H, body: Bytes) -> Self {
        Self { header, body }
    }
}

impl<'a, H: MessageHeaderTrait> MessageFrame<H, &'a [u8]> {
    pub fn from_slice(header: H, body: &'a [u8]) -> Self {
        Self { header, body }
    }
}

#[derive(Default, Clone)]
pub struct MessageCodec<H: MessageHeaderTrait> {
    _phantom: std::marker::PhantomData<H>,
}

#[macro_export]
macro_rules! impl_protobuf_message_header {
    ($header_type:ident) => {
        impl std::ops::Deref for $header_type {
            type Target = $crate::ProtobufMessageHeader;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::ops::DerefMut for $header_type {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl $crate::MessageHeaderTrait for $header_type {
            fn encode(&self) -> &[u8] {
                self.0.encode()
            }

            fn decode(src: &[u8]) -> Self {
                Self($crate::ProtobufMessageHeader::decode(src))
            }

            fn get_size(&self) -> usize {
                self.0.get_size()
            }

            fn get_id(&self) -> u32 {
                self.0.get_id()
            }

            fn get_trace_id(&self) -> data_types::TraceId {
                self.0.get_trace_id()
            }

            fn set_checksum(&mut self) {
                self.0.set_checksum()
            }

            fn verify_body_checksum(&self, body: &[u8]) -> bool {
                self.0.verify_body_checksum(body)
            }
        }
    };
}
