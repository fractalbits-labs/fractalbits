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

use aws_sdk_s3::{
    error::SdkError,
    operation::{
        delete_object::DeleteObjectError, get_object::GetObjectError, put_object::PutObjectError,
    },
};
use bytes::Bytes;
use data_types::TraceId;
pub use s3_blob_store::{S3BlobError, create_s3_client};
use uuid::Uuid;

pub use data_types::object_layout::BlobLocation;

#[allow(clippy::enum_variant_names)]
pub enum BlobStorageImpl {
    HybridSingleAz(S3HybridSingleAzStorage),
    AllInBssSingleAz(AllInBssSingleAzStorage),
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

impl From<S3BlobError> for BlobStorageError {
    fn from(err: S3BlobError) -> Self {
        BlobStorageError::S3(err.to_string())
    }
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
    pub async fn list_blob_blocks(
        &self,
        blob_guid: DataBlobGuid,
        trace_id: &TraceId,
    ) -> Result<Vec<(u32, u64)>, BlobStorageError> {
        match self {
            BlobStorageImpl::HybridSingleAz(storage) => {
                storage.list_blob_blocks(blob_guid, trace_id).await
            }
            BlobStorageImpl::AllInBssSingleAz(storage) => {
                storage.list_blob_blocks(blob_guid, trace_id).await
            }
        }
    }

    /// Every stored generation of an S3-resident blob. FUSE writes
    /// versioned keys, so teardown must enumerate rather than assume
    /// generation 1 per block.
    pub async fn list_s3_blob_blocks(
        &self,
        blob_id: Uuid,
    ) -> Result<Vec<(u32, u64)>, BlobStorageError> {
        match self {
            BlobStorageImpl::HybridSingleAz(storage) => storage.list_s3_blob_blocks(blob_id).await,
            BlobStorageImpl::AllInBssSingleAz(_) => Err(BlobStorageError::Config(
                "S3-resident blob on a BSS-only backend".into(),
            )),
        }
    }

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

    #[allow(clippy::too_many_arguments)]
    pub async fn get_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
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
                        version,
                        content_len,
                        location,
                        body,
                        trace_id,
                    )
                    .await
            }
            BlobStorageImpl::AllInBssSingleAz(storage) => {
                storage
                    .get_blob(
                        blob_guid,
                        block_number,
                        version,
                        content_len,
                        body,
                        trace_id,
                    )
                    .await
            }
        }
    }

    pub async fn delete_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        location: BlobLocation,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        match self {
            BlobStorageImpl::HybridSingleAz(storage) => {
                storage
                    .delete_blob(blob_guid, block_number, version, location, trace_id)
                    .await
            }
            BlobStorageImpl::AllInBssSingleAz(storage) => {
                storage
                    .delete_blob(blob_guid, block_number, version, trace_id)
                    .await
            }
        }
    }
}
