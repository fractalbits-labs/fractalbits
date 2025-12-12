use bytes::{Bytes, BytesMut};
use data_types::TraceId;
use prost::Message as PbMessage;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, error};

#[cfg(feature = "metrics")]
use metrics_wrapper::{Gauge, counter, gauge, histogram};

pub mod generic_client;
pub use generic_client::RpcCodec;
pub use rpc_codec_common::{MessageFrame, MessageHeaderTrait};

use generic_client::RpcClient as GenericRpcClient;

#[derive(Error, Debug)]
pub enum RpcError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    OneshotRecvError(tokio::sync::oneshot::error::RecvError),
    #[error("Internal request sending error: {0}")]
    InternalRequestError(String),
    #[error("Internal response error: {0}")]
    InternalResponseError(String),
    #[error("Entry not found")]
    NotFound,
    #[error("Entry already exists")]
    AlreadyExists,
    #[error("Bucket already owned by you")]
    BucketAlreadyOwnedByYou,
    #[error("Send error: {0}")]
    SendError(String),
    #[error("Encode error: {0}")]
    EncodeError(String),
    #[error("Decode error: {0}")]
    DecodeError(String),
    #[error("Retry")]
    Retry,
    #[error("Connection closed")]
    ConnectionClosed,
    #[error("Checksum mismatch")]
    ChecksumMismatch,
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for RpcError {
    fn from(e: tokio::sync::mpsc::error::SendError<T>) -> Self {
        RpcError::SendError(e.to_string())
    }
}

impl RpcError {
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            RpcError::OneshotRecvError(_)
                | RpcError::InternalRequestError(_)
                | RpcError::InternalResponseError(_)
                | RpcError::ConnectionClosed
        )
    }
}

pub struct AutoReconnectRpcClient<Codec, Header>
where
    Codec: RpcCodec<Header>,
    Header: MessageHeaderTrait + Clone + Send + Sync + 'static,
{
    inner: RwLock<Option<Arc<GenericRpcClient<Codec, Header>>>>,
    addresses: Vec<String>,
    next_id: Arc<AtomicU32>,
    connection_timeout: Duration,
}

impl<Codec, Header> AutoReconnectRpcClient<Codec, Header>
where
    Codec: RpcCodec<Header>,
    Header: MessageHeaderTrait + Clone + Send + Sync + 'static + Default,
{
    pub fn new_from_address(address: String, connection_timeout: Duration) -> Self {
        Self {
            inner: RwLock::new(None),
            addresses: vec![address],
            next_id: Arc::new(AtomicU32::new(1)),
            connection_timeout,
        }
    }

    pub fn new_from_addresses(addresses: Vec<String>, connection_timeout: Duration) -> Self {
        Self {
            inner: RwLock::new(None),
            addresses,
            next_id: Arc::new(AtomicU32::new(1)),
            connection_timeout,
        }
    }

    async fn ensure_connected(&self) -> Result<(), RpcError> {
        let rpc_type = Codec::RPC_TYPE;
        {
            let read = self.inner.read().await;
            if let Some(client) = read.as_ref()
                && !client.is_closed()
            {
                return Ok(());
            }
        }

        let mut write = self.inner.write().await;
        if let Some(client) = write.as_ref()
            && !client.is_closed()
        {
            return Ok(());
        }

        // Try all addresses
        for address in &self.addresses {
            debug!(%rpc_type, %address, "Trying to connect to RPC server");
            match GenericRpcClient::<Codec, Header>::establish_connection(
                address.clone(),
                self.connection_timeout,
            )
            .await
            {
                Ok(new_client) => {
                    debug!(%rpc_type, %address, "Successfully connected to RPC server");
                    *write = Some(Arc::new(new_client));
                    return Ok(());
                }
                Err(e) => {
                    debug!(%rpc_type, %address, error=%e, "Failed to connect, trying next address");
                    continue;
                }
            }
        }

        error!(%rpc_type, addresses=?self.addresses, "Failed to establish RPC connection to any address");
        Err(RpcError::ConnectionClosed)
    }

    pub fn gen_request_id(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    pub async fn send_request(
        &self,
        frame: MessageFrame<Header, Bytes>,
        timeout: Option<Duration>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        self.ensure_connected().await?;
        let client = {
            let read = self.inner.read().await;
            Arc::clone(read.as_ref().unwrap())
        };
        client.send_request(frame, timeout).await
    }

    pub async fn send_request_vectored(
        &self,
        frame: MessageFrame<Header, Vec<bytes::Bytes>>,
        timeout: Option<Duration>,
    ) -> Result<MessageFrame<Header>, RpcError> {
        self.ensure_connected().await?;
        let client = {
            let read = self.inner.read().await;
            Arc::clone(read.as_ref().unwrap())
        };
        client.send_request_vectored(frame, timeout).await
    }
}

#[cfg(feature = "metrics")]
pub struct InflightRpcGuard {
    start: std::time::Instant,
    gauge: Gauge,
    rpc_type: &'static str,
    rpc_name: &'static str,
}

#[cfg(not(feature = "metrics"))]
pub struct InflightRpcGuard;

#[cfg(feature = "metrics")]
impl InflightRpcGuard {
    pub fn new(rpc_type: &'static str, rpc_name: &'static str) -> Self {
        let gauge = gauge!("inflight_rpc", "type" => rpc_type, "name" => rpc_name);
        gauge.increment(1.0);
        counter!("rpc_request_sent", "type" => rpc_type, "name" => rpc_name).increment(1);

        Self {
            start: std::time::Instant::now(),
            gauge,
            rpc_type,
            rpc_name,
        }
    }
}

#[cfg(not(feature = "metrics"))]
impl InflightRpcGuard {
    #[inline(always)]
    pub fn new(_rpc_type: &'static str, _rpc_name: &'static str) -> Self {
        Self
    }
}

#[cfg(feature = "metrics")]
impl Drop for InflightRpcGuard {
    fn drop(&mut self) {
        histogram!("rpc_duration_nanos", "type" => self.rpc_type, "name" => self.rpc_name)
            .record(self.start.elapsed().as_nanos() as f64);
        self.gauge.decrement(1.0);
    }
}

#[macro_export]
macro_rules! rpc_retry {
    ($rpc_type:expr, $client:expr, $method:ident($($args:expr),*)) => {
        async {
            let mut retries = 3;
            let mut backoff = std::time::Duration::from_millis(5);
            let mut retry_count = 0u32;
            loop {
                match $client.$method($($args,)* retry_count).await {
                    Ok(val) => {
                        return Ok(val);
                    },
                    Err(e) => {
                        if e.retryable() && retries > 0 {
                            retries -= 1;
                            retry_count += 1;
                            tokio::time::sleep(backoff).await;
                            backoff = backoff.saturating_mul(2);
                        } else {
                            if e.retryable() {
                                ::tracing::error!(
                                    rpc_type=%$rpc_type,
                                    method=stringify!($method),
                                    error=%e,
                                    "RPC call failed after multiple retries"
                                );
                            }
                            return Err(e);
                        }
                    }
                }
            }
        }
    };
}

#[macro_export]
macro_rules! bss_rpc_retry {
    ($client:expr, $method:ident($($args:expr),*)) => {
        $crate::rpc_retry!("bss", $client, $method($($args),*))
    };
}

#[macro_export]
macro_rules! nss_rpc_retry {
    ($client:expr, $method:ident($($args:expr),*)) => {
        $crate::rpc_retry!("nss", $client, $method($($args),*))
    };
}

#[macro_export]
macro_rules! rss_rpc_retry {
    ($client:expr, $method:ident($($args:expr),*)) => {
        $crate::rpc_retry!("rss", $client, $method($($args),*))
    };
}

pub fn encode_protobuf<M: PbMessage>(msg: M, _trace_id: &TraceId) -> Result<Bytes, RpcError> {
    let mut msg_bytes = BytesMut::with_capacity(1024);
    msg.encode(&mut msg_bytes)
        .map_err(|e| RpcError::EncodeError(e.to_string()))?;
    Ok(msg_bytes.freeze())
}
