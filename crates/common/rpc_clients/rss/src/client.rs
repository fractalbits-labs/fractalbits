use bytes::Bytes;
use rpc_client_common::{AutoReconnectRpcClient, MessageFrame, ProtobufRpc, RpcError};
use rss_codec::MessageHeader;
use std::time::Duration;

pub struct RpcClient {
    inner: AutoReconnectRpcClient<rss_codec::MessageCodec, MessageHeader>,
}

impl RpcClient {
    pub fn new_from_addresses(addresses: Vec<String>, connection_timeout: Duration) -> Self {
        let inner = AutoReconnectRpcClient::new_from_addresses(addresses, connection_timeout);
        Self { inner }
    }
}

impl ProtobufRpc for RpcClient {
    type Header = MessageHeader;
    const RPC_TYPE: &'static str = "rss";

    fn gen_request_id(&self) -> u32 {
        self.inner.gen_request_id()
    }

    async fn send_request(
        &self,
        frame: MessageFrame<MessageHeader, Bytes>,
        timeout: Option<Duration>,
    ) -> Result<MessageFrame<MessageHeader>, RpcError> {
        self.inner.send_request(frame, timeout).await
    }
}
