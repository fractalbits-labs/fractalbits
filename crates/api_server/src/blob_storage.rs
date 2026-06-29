mod all_in_bss_single_az_storage;
mod retry;
mod s3_common;
mod s3_hybrid_single_az_storage;

pub use all_in_bss_single_az_storage::AllInBssSingleAzStorage;
pub use data_types::DataBlobGuid;
pub use retry::S3RetryConfig;
pub use s3_common::chunks_to_bytestream;
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
use uuid::Uuid;

pub use data_types::object_layout::BlobLocation;

#[allow(clippy::enum_variant_names)]
pub enum BlobStorageImpl {
    HybridSingleAz(S3HybridSingleAzStorage),
    AllInBssSingleAz(AllInBssSingleAzStorage),
}

/// Generate a consistent S3 key format for blob storage
pub fn blob_key(blob_id: Uuid, block_number: u32) -> String {
    format!("{blob_id}-p{block_number}")
}

/// Create an S3 client configured for either AWS S3 or local minio
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
            BlobStorageImpl::AllInBssSingleAz(storage) => {
                storage
                    .put_blob(blob_id, volume_id, block_number, body, trace_id)
                    .await
            }
        }
    }

    pub async fn put_blob_vectored(
        &self,
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
            BlobStorageImpl::AllInBssSingleAz(storage) => {
                storage
                    .get_blob(blob_guid, block_number, content_len, body, trace_id)
                    .await
            }
        }
    }

    pub async fn delete_blob(
        &self,
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
            BlobStorageImpl::AllInBssSingleAz(storage) => {
                storage.delete_blob(blob_guid, block_number, trace_id).await
            }
        }
    }
}
