use bytes::{Bytes, BytesMut};
use data_types::TraceId;
use prost::Message as PbMessage;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, error};

#[cfg(all(feature = "compio-runtime", not(feature = "tokio-runtime")))]
pub async fn rpc_timeout<F: std::future::Future>(
    duration: Duration,
    future: F,
) -> Result<F::Output, std::io::Error> {
    compio_runtime::time::timeout(duration, future)
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "rpc timeout"))
}

#[cfg(feature = "tokio-runtime")]
pub async fn rpc_timeout<F: std::future::Future>(
    duration: Duration,
    future: F,
) -> Result<F::Output, std::io::Error> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "rpc timeout"))
}

#[cfg(all(feature = "compio-runtime", not(feature = "tokio-runtime")))]
pub async fn rpc_sleep(duration: Duration) {
    compio_runtime::time::sleep(duration).await;
}

#[cfg(feature = "tokio-runtime")]
pub async fn rpc_sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

/// Half-jitter helper: returns a uniformly random duration in
/// [base_ms/2, base_ms). When `base_ms <= 1`, returns 1 ms (we never
/// want a zero sleep on a retry path). Lives in a function rather than
/// inline in the retry macros so callers of the macros don't need
/// `rand` in their own Cargo.toml.
///
/// "Half jitter" (vs full jitter) keeps a floor under the sleep so the
/// expected wait still grows exponentially with the backoff, while
/// breaking the lockstep that arises when an entire multiplexed
/// connection's worth of in-flight RPCs all retry on the same
/// connection-drop tick.
pub fn jitter_backoff(base_ms: u64) -> Duration {
    use rand::RngExt;
    let lo = base_ms / 2;
    let range = base_ms.saturating_sub(lo).max(1);
    let ms = lo + rand::rng().random_range(0..range);
    Duration::from_millis(ms.max(1))
}

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
    /// The bucket's NSS root blob does not exist (deleted or never created).
    /// Distinct from `NotFound`, which is "key not present in an existing
    /// tree". Maps to `S3Error::NoSuchBucket` in api_server.
    #[error("Root blob does not exist")]
    NoSuchRootBlob,
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
    #[error("Version skipped")]
    VersionSkipped,
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
            let mut backoff_ms = 2u64;
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
                            // Half jitter to break the lockstep that arises when
                            // an entire multiplexed connection's worth of
                            // in-flight RPCs all retry on the same disconnect.
                            $crate::rpc_sleep($crate::jitter_backoff(backoff_ms)).await;
                            backoff_ms = backoff_ms.saturating_mul(2);
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

/// Internal: shared retry loop body used by both nss_rpc_retry! arms.
/// Callers supply the expressions for refresh/get-client so the retry logic
/// itself lives in exactly one place, independent of whether the caller caches
/// NSS clients per routing_key (api_server) or holds a single client
/// (fs_server). Do not invoke directly — use `nss_rpc_retry!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __nss_rpc_retry_body {
    ($client:expr, $method:ident($($args:expr),*), $refresh:expr, $get_client:expr) => {
        async {
            let failover_timeout = std::time::Duration::from_secs(30);
            let failover_start = std::time::Instant::now();
            let mut refresh_attempt = 0u32;

            let initial_result = $crate::rpc_retry!("nss", $client, $method($($args),*)).await;
            if initial_result.is_ok()
                || !initial_result.as_ref().err().map(|e| e.retryable()).unwrap_or(false)
            {
                return initial_result;
            }

            loop {
                if failover_start.elapsed() > failover_timeout {
                    ::tracing::warn!(
                        "NSS RPC failed after {}s failover timeout",
                        failover_start.elapsed().as_secs()
                    );
                    return $crate::rpc_retry!("nss", $client, $method($($args),*)).await;
                }

                if $refresh.await {
                    ::tracing::info!(
                        "NSS address refreshed after {}ms, retrying with new address",
                        failover_start.elapsed().as_millis()
                    );
                    if let Ok(new_client) = $get_client.await {
                        let result =
                            $crate::rpc_retry!("nss", new_client, $method($($args),*)).await;
                        if result.is_ok()
                            || !result.as_ref().err().map(|e| e.retryable()).unwrap_or(false)
                        {
                            return result;
                        }
                    }
                    refresh_attempt = 0;
                }

                // Exponential backoff with half jitter: pick uniformly from
                // [base/2, base) where base = min(200 * 2^min(n,3), 1000) ms.
                // Half jitter breaks the lockstep that arises when a whole
                // multiplexed NSS connection's worth of in-flight RPCs all
                // enter this loop on the same disconnect, then retry NSS in
                // unison after each backoff -- which can knock NSS over again
                // mid-recovery.
                let base_ms = std::cmp::min(200u64 * (1u64 << refresh_attempt.min(3)), 1000);
                let backoff = $crate::jitter_backoff(base_ms);
                ::tracing::debug!(
                    "NSS failover: waiting {}ms before retry (attempt {}, elapsed {}ms)",
                    backoff.as_millis(),
                    refresh_attempt + 1,
                    failover_start.elapsed().as_millis()
                );
                $crate::rpc_sleep(backoff).await;
                refresh_attempt = refresh_attempt.saturating_add(1);

                // Retry with same address — NSS may have recovered without an
                // address change (e.g., nss_role_agent briefly restarted on
                // the same port).
                let same_addr_result =
                    $crate::rpc_retry!("nss", $client, $method($($args),*)).await;
                if same_addr_result.is_ok()
                    || !same_addr_result
                        .as_ref()
                        .err()
                        .map(|e| e.retryable())
                        .unwrap_or(false)
                {
                    if same_addr_result.is_ok() {
                        ::tracing::info!(
                            "NSS recovered at same address after {}ms",
                            failover_start.elapsed().as_millis()
                        );
                    }
                    return same_addr_result;
                }
            }
        }
    };
}

/// NSS RPC retry macro with automatic address refresh on connection failure.
/// When all retries are exhausted due to connection errors, it enters a
/// failover retry loop that keeps trying for up to 30 seconds, periodically
/// asking the caller to refresh the NSS address from RSS.
///
/// Two forms:
/// - 5-arg (multi-NSS): `(client, method(args), app, routing_key, trace_id)` —
///   caller's `app` exposes `get_nss_rpc_client(&RoutingKey)` and
///   `try_refresh_nss_address(&RoutingKey, &TraceId)`. Used by api_server.
/// - 4-arg (single-NSS): `(client, method(args), app, trace_id)` — caller's
///   `app` exposes `get_nss_rpc_client()` and `try_refresh_nss_address(&TraceId)`.
///   Used by fs_server.
///
/// Both forms share the retry loop in `__nss_rpc_retry_body!`; only the refresh
/// and get-client expressions differ.
#[macro_export]
macro_rules! nss_rpc_retry {
    // Multi-NSS form: keyed lookup per routing_key.
    ($client:expr, $method:ident($($args:expr),*), $app:expr, $routing_key:expr, $trace_id:expr) => {
        $crate::__nss_rpc_retry_body!(
            $client,
            $method($($args),*),
            $app.try_refresh_nss_address($routing_key, $trace_id),
            $app.get_nss_rpc_client($routing_key)
        )
    };
    // Single-NSS form: caller holds exactly one NSS client.
    ($client:expr, $method:ident($($args:expr),*), $app:expr, $trace_id:expr) => {
        $crate::__nss_rpc_retry_body!(
            $client,
            $method($($args),*),
            $app.try_refresh_nss_address($trace_id),
            $app.get_nss_rpc_client()
        )
    };
    // No-refresh form (used outside api_server/fs_server).
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
