use rpc_client_common::AutoReconnectRpcClient;
use std::time::Duration;

pub struct RpcClient {
    inner: AutoReconnectRpcClient<rss_codec::MessageCodec, rss_codec::MessageHeader>,
}

impl RpcClient {
    pub fn new_from_addresses(addresses: Vec<String>, connection_timeout: Duration) -> Self {
        let inner = AutoReconnectRpcClient::new_from_addresses(addresses, connection_timeout);
        Self { inner }
    }

    pub fn gen_request_id(&self) -> u32 {
        self.inner.gen_request_id()
    }

    pub async fn send_request(
        &self,
        frame: rpc_codec_common::MessageFrame<rss_codec::MessageHeader, bytes::Bytes>,
        timeout: Option<std::time::Duration>,
    ) -> Result<rpc_codec_common::MessageFrame<rss_codec::MessageHeader>, rpc_client_common::RpcError>
    {
        self.inner.send_request(frame, timeout).await
    }
}
