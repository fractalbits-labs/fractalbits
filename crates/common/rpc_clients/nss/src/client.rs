use bytes::Bytes;
use nss_codec::MessageHeader;
use rpc_client_common::{AutoReconnectRpcClient, MessageFrame, ProtobufRpc, RpcError};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const CONNS_PER_CORE: usize = 8;

pub struct RpcClient {
    connections: Vec<Arc<AutoReconnectRpcClient<nss_codec::MessageCodec, MessageHeader>>>,
    next_conn: AtomicUsize,
}

impl RpcClient {
    pub fn new_from_address(address: String, connection_timeout: Duration) -> Self {
        let mut connections = Vec::with_capacity(CONNS_PER_CORE);

        for _ in 0..CONNS_PER_CORE {
            let inner =
                AutoReconnectRpcClient::new_from_address(address.clone(), connection_timeout);
            connections.push(Arc::new(inner));
        }

        Self {
            connections,
            next_conn: AtomicUsize::new(0),
        }
    }

    fn get_connection(
        &self,
    ) -> &Arc<AutoReconnectRpcClient<nss_codec::MessageCodec, MessageHeader>> {
        let idx = self.next_conn.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        &self.connections[idx]
    }
}

impl ProtobufRpc for RpcClient {
    type Header = MessageHeader;
    const RPC_TYPE: &'static str = "nss";

    fn gen_request_id(&self) -> u32 {
        self.get_connection().gen_request_id()
    }

    async fn send_request(
        &self,
        frame: MessageFrame<MessageHeader, Bytes>,
        timeout: Option<Duration>,
    ) -> Result<MessageFrame<MessageHeader>, RpcError> {
        self.get_connection().send_request(frame, timeout).await
    }
}
