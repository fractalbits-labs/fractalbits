mod all_in_bss_single_az_storage;
mod retry;
mod s3_common;
mod s3_express_multi_az_storage;
mod s3_hybrid_single_az_storage;

pub use crate::config::RatelimitConfig;
pub use all_in_bss_single_az_storage::AllInBssSingleAzStorage;
pub use data_types::DataBlobGuid;
pub use retry::S3RetryConfig;
pub use s3_common::chunks_to_bytestream;
pub use s3_express_multi_az_storage::S3ExpressMultiAzStorage;
pub use s3_hybrid_single_az_storage::S3HybridSingleAzStorage;
pub use volume_group_proxy::DataVgProxy;

use aws_config::{BehaviorVersion, retry::RetryConfig};
use aws_sdk_s3::{
    Client as S3Client, Config as S3Config,
    config::{Credentials, Region},
    error::SdkError,
    operation::{
        delete_object::DeleteObjectError, get_object::GetObjectError, put_object::PutObjectError,
    },
};
use bytes::Bytes;
use data_types::TraceId;
use governor::{Quota, RateLimiter};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use uuid::Uuid;

/// Specifies where a blob should be stored/retrieved from
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BlobLocation {
    /// Small blobs stored in DataVgProxy
    DataVgProxy,
    /// Large blobs stored in S3
    S3,
}

#[allow(clippy::enum_variant_names)]
pub enum BlobStorageImpl {
    HybridSingleAz(S3HybridSingleAzStorage),
    S3ExpressMultiAz(S3ExpressMultiAzStorage),
    AllInBssSingleAz(AllInBssSingleAzStorage),
}

/// S3 operation retry macro - similar to rpc_retry but for S3 operations
#[macro_export]
macro_rules! s3_retry {
    ($operation_name:expr, $storage_type:expr, $bucket:expr, $retry_config:expr, $operation:expr) => {{
        use $crate::blob_storage::retry;
        retry::retry_s3_operation(
            $operation_name,
            $storage_type,
            $bucket,
            $retry_config,
            || async { $operation.await },
        )
        .await
    }};
}

/// Generate a consistent S3 key format for blob storage
pub fn blob_key(blob_id: Uuid, block_number: u32) -> String {
    format!("{blob_id}-p{block_number}")
}

/// S3 client wrapper that can be either rate-limited or direct
#[derive(Clone)]
pub enum S3ClientWrapper {
    RateLimited(RateLimitedS3Client),
    Direct(S3Client),
}

impl S3ClientWrapper {
    /// Rate-limited or direct put_object operation
    pub async fn put_object(
        &self,
    ) -> aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder {
        match self {
            S3ClientWrapper::RateLimited(client) => client.put_object().await,
            S3ClientWrapper::Direct(client) => client.put_object(),
        }
    }

    /// Rate-limited or direct get_object operation
    pub async fn get_object(
        &self,
    ) -> aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder {
        match self {
            S3ClientWrapper::RateLimited(client) => client.get_object().await,
            S3ClientWrapper::Direct(client) => client.get_object(),
        }
    }

    /// Rate-limited or direct delete_object operation
    pub async fn delete_object(
        &self,
    ) -> aws_sdk_s3::operation::delete_object::builders::DeleteObjectFluentBuilder {
        match self {
            S3ClientWrapper::RateLimited(client) => client.delete_object().await,
            S3ClientWrapper::Direct(client) => client.delete_object(),
        }
    }
}

/// S3 client wrapper with per-operation rate limiting
#[derive(Clone)]
pub struct RateLimitedS3Client {
    client: S3Client,
    put_rate_limiter: Arc<governor::DefaultDirectRateLimiter>,
    get_rate_limiter: Arc<governor::DefaultDirectRateLimiter>,
    delete_rate_limiter: Arc<governor::DefaultDirectRateLimiter>,
}

impl RateLimitedS3Client {
    pub fn new(client: S3Client, config: &RatelimitConfig) -> Self {
        let put_quota = Quota::per_second(
            NonZeroU32::new(config.put_qps).unwrap_or(NonZeroU32::new(1).unwrap()),
        );
        let get_quota = Quota::per_second(
            NonZeroU32::new(config.get_qps).unwrap_or(NonZeroU32::new(1).unwrap()),
        );
        let delete_quota = Quota::per_second(
            NonZeroU32::new(config.delete_qps).unwrap_or(NonZeroU32::new(1).unwrap()),
        );

        Self {
            client,
            put_rate_limiter: Arc::new(RateLimiter::direct(put_quota)),
            get_rate_limiter: Arc::new(RateLimiter::direct(get_quota)),
            delete_rate_limiter: Arc::new(RateLimiter::direct(delete_quota)),
        }
    }

    /// Rate-limited put_object operation
    pub async fn put_object(
        &self,
    ) -> aws_sdk_s3::operation::put_object::builders::PutObjectFluentBuilder {
        self.put_rate_limiter.until_ready().await;
        self.client.put_object()
    }

    /// Rate-limited get_object operation
    pub async fn get_object(
        &self,
    ) -> aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder {
        self.get_rate_limiter.until_ready().await;
        self.client.get_object()
    }

    /// Rate-limited delete_object operation
    pub async fn delete_object(
        &self,
    ) -> aws_sdk_s3::operation::delete_object::builders::DeleteObjectFluentBuilder {
        self.delete_rate_limiter.until_ready().await;
        self.client.delete_object()
    }
}

/// Create an S3 client wrapper (rate-limited or direct) based on configuration
pub async fn create_s3_client_wrapper(
    s3_host: &str,
    s3_port: u16,
    s3_region: &str,
    force_path_style: bool,
    rate_limit_config: &RatelimitConfig,
) -> S3ClientWrapper {
    // Use optimized client for AWS S3, standard client for minio
    let s3_client = if s3_host.ends_with("amazonaws.com") {
        create_optimized_s3_client(s3_host, s3_port, s3_region, force_path_style, None).await
    } else {
        create_s3_client(s3_host, s3_port, s3_region, force_path_style).await
    };

    if rate_limit_config.enabled {
        S3ClientWrapper::RateLimited(RateLimitedS3Client::new(s3_client, rate_limit_config))
    } else {
        S3ClientWrapper::Direct(s3_client)
    }
}

/// Configuration for S3 client connection pool optimization
#[derive(Debug, Clone)]
pub struct S3ClientPoolConfig {
    /// Maximum number of connections per host
    pub max_connections_per_host: usize,
    /// Connection idle timeout (should match S3 Express session lifetime)
    pub connection_idle_timeout: Duration,
    /// Connection timeout for new connections
    #[allow(dead_code)]
    pub connection_timeout: Duration,
    /// Pool idle timeout
    #[allow(dead_code)]
    pub pool_idle_timeout: Duration,
}

impl Default for S3ClientPoolConfig {
    fn default() -> Self {
        Self {
            // Increased for S3 Express multi-AZ concurrent operations
            max_connections_per_host: 32,
            // Match S3 Express session lifetime (~5 minutes)
            connection_idle_timeout: Duration::from_secs(280),
            // Fast connection establishment
            connection_timeout: Duration::from_secs(10),
            // Pool idle timeout should be longer than session lifetime
            pool_idle_timeout: Duration::from_secs(350),
        }
    }
}

/// Create an optimized S3 client with enhanced connection pool settings
/// Note: Connection pool optimization is handled automatically by the AWS SDK's underlying HTTP client
pub async fn create_optimized_s3_client(
    s3_host: &str,
    s3_port: u16,
    s3_region: &str,
    force_path_style: bool,
    pool_config: Option<S3ClientPoolConfig>,
) -> S3Client {
    let config = pool_config.unwrap_or_default();

    info!(
        "Creating optimized S3 client for host: {} with connection pool config: max_connections_per_host={}, connection_idle_timeout={}s",
        s3_host,
        config.max_connections_per_host,
        config.connection_idle_timeout.as_secs()
    );

    // Create AWS config with default optimizations
    // The AWS SDK automatically handles connection pooling with sensible defaults
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(s3_region.to_string()))
        .load()
        .await;

    // Create S3-specific config
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);

    // Configure endpoint if not using AWS
    if !s3_host.ends_with("amazonaws.com") {
        let endpoint_url = if s3_port == 80 {
            format!("http://{s3_host}")
        } else if s3_port == 443 {
            format!("https://{s3_host}")
        } else {
            format!("https://{s3_host}:{s3_port}")
        };
        s3_config_builder = s3_config_builder.endpoint_url(&endpoint_url);
    }

    // Configure path style if needed
    if force_path_style {
        s3_config_builder = s3_config_builder.force_path_style(true);
    }

    let s3_config = s3_config_builder.build();
    S3Client::from_conf(s3_config)
}

/// Create an S3 client configured for either AWS S3 or local minio (legacy function)
pub async fn create_s3_client(
    s3_host: &str,
    s3_port: u16,
    s3_region: &str,
    force_path_style: bool,
) -> S3Client {
    // Disable SDK retries - we'll handle retries ourselves for better visibility
    let retry_config = RetryConfig::disabled(); // No retries at SDK level

    if s3_host.ends_with("amazonaws.com") {
        // Real AWS S3
        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(s3_region.to_string()))
            .retry_config(retry_config)
            .load()
            .await;
        S3Client::new(&aws_config)
    } else {
        // Local minio or other S3-compatible service
        let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "minio");
        let endpoint_url = format!("{s3_host}:{s3_port}");

        let mut s3_config_builder = S3Config::builder()
            .endpoint_url(&endpoint_url)
            .region(Region::new(s3_region.to_string()))
            .credentials_provider(credentials)
            .retry_config(retry_config)
            .behavior_version(BehaviorVersion::latest())
            .disable_s3_express_session_auth(true); // Disable for minio compatibility

        if force_path_style {
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        S3Client::from_conf(s3_config_builder.build())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BlobStorageError {
    #[error("BSS RPC error: {0}")]
    BssRpc(#[from] rpc_client_common::RpcError),

    #[error("Data VG error: {0}")]
    DataVg(#[from] volume_group_proxy::DataVgError),

    #[error("S3 error: {0}")]
    S3(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Initialization error: {0}")]
    InitializationError(String),

    #[error("Quorum failure: {0}")]
    QuorumFailure(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<SdkError<PutObjectError>> for BlobStorageError {
    fn from(err: SdkError<PutObjectError>) -> Self {
        BlobStorageError::S3(err.to_string())
    }
}

impl From<SdkError<GetObjectError>> for BlobStorageError {
    fn from(err: SdkError<GetObjectError>) -> Self {
        BlobStorageError::S3(err.to_string())
    }
}

impl From<SdkError<DeleteObjectError>> for BlobStorageError {
    fn from(err: SdkError<DeleteObjectError>) -> Self {
        BlobStorageError::S3(err.to_string())
    }
}

impl BlobStorageImpl {
    pub async fn put_blob(
        &self,
        tracking_root_blob_name: Option<&str>,
        blob_id: Uuid,
        volume_id: u16,
        block_number: u32,
        body: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        match self {
            BlobStorageImpl::HybridSingleAz(storage) => {
                storage
                    .put_blob(blob_id, volume_id, block_number, body, trace_id)
                    .await
            }
            BlobStorageImpl::S3ExpressMultiAz(storage) => {
                storage
                    .put_blob(tracking_root_blob_name, blob_id, block_number, body)
                    .await
            }
            BlobStorageImpl::AllInBssSingleAz(storage) => {
                storage
                    .put_blob(blob_id, volume_id, block_number, body, trace_id)
                    .await
            }
        }
    }

    pub async fn put_blob_vectored(
        &self,
        tracking_root_blob_name: Option<&str>,
        blob_id: Uuid,
        volume_id: u16,
        block_number: u32,
        chunks: Vec<actix_web::web::Bytes>,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        match self {
            BlobStorageImpl::HybridSingleAz(storage) => {
                storage
                    .put_blob_vectored(blob_id, volume_id, block_number, chunks, trace_id)
                    .await
            }
            BlobStorageImpl::S3ExpressMultiAz(storage) => {
                storage
                    .put_blob_vectored(tracking_root_blob_name, blob_id, block_number, chunks)
                    .await
            }
            BlobStorageImpl::AllInBssSingleAz(storage) => {
                storage
                    .put_blob_vectored(blob_id, volume_id, block_number, chunks, trace_id)
                    .await
            }
        }
    }

    pub async fn get_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        location: BlobLocation,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        match self {
            BlobStorageImpl::HybridSingleAz(storage) => {
                storage
                    .get_blob(
                        blob_guid,
                        block_number,
                        content_len,
                        location,
                        body,
                        trace_id,
                    )
                    .await
            }
            BlobStorageImpl::S3ExpressMultiAz(storage) => {
                storage.get_blob(blob_guid, block_number, body).await
            }
            BlobStorageImpl::AllInBssSingleAz(storage) => {
                storage
                    .get_blob(blob_guid, block_number, content_len, body, trace_id)
                    .await
            }
        }
    }

    pub async fn delete_blob(
        &self,
        tracking_root_blob_name: Option<&str>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        location: BlobLocation,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        match self {
            BlobStorageImpl::HybridSingleAz(storage) => {
                storage
                    .delete_blob(blob_guid, block_number, location, trace_id)
                    .await
            }
            BlobStorageImpl::S3ExpressMultiAz(storage) => {
                storage
                    .delete_blob(tracking_root_blob_name, blob_guid, block_number)
                    .await
            }
            BlobStorageImpl::AllInBssSingleAz(storage) => {
                storage.delete_blob(blob_guid, block_number, trace_id).await
            }
        }
    }
}
