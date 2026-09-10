use crate::Command;

#[repr(transparent)]
#[derive(Debug, Default, Clone, Copy)]
pub struct MessageHeader(pub rpc_codec_common::ProtobufMessageHeader<Command>);

rpc_codec_common::impl_protobuf_message_header!(MessageHeader, Command);
