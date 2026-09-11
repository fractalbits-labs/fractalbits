use crate::rpc::check_response_errno;
use bss_codec::MessageHeader;
use bytes::Bytes;
use rpc_client_common::{AutoReconnectRpcClient, MessageFrame, ProtobufRpc, RpcError};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const CONNS_PER_CORE: usize = 2;

pub struct RpcClient {
    connections: Vec<Arc<AutoReconnectRpcClient<bss_codec::MessageCodec, MessageHeader>>>,
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
    ) -> &Arc<AutoReconnectRpcClient<bss_codec::MessageCodec, MessageHeader>> {
        let idx = self.next_conn.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        &self.connections[idx]
    }

    pub async fn send_request_vectored(
        &self,
        frame: rpc_codec_common::MessageFrame<bss_codec::MessageHeader, Vec<bytes::Bytes>>,
        timeout: Option<std::time::Duration>,
        operation: Option<crate::stats::OperationType>,
    ) -> Result<rpc_codec_common::MessageFrame<bss_codec::MessageHeader>, rpc_client_common::RpcError>
    {
        let _guard = operation.map(crate::stats::BssStatsGuard::new);
        self.get_connection()
            .send_request_vectored(frame, timeout)
            .await
    }
}

impl ProtobufRpc for RpcClient {
    type Header = MessageHeader;
    const RPC_TYPE: &'static str = "bss";

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

    fn check_response(header: &MessageHeader) -> Result<(), RpcError> {
        check_response_errno(header)
    }
}
