use crate::{
    blob_storage::{
        AllInBssSingleAzStorage, BlobLocation, BlobStorageError, BlobStorageImpl,
        S3HybridSingleAzStorage,
    },
    config::{BlobStorageBackend, BlobStorageConfig},
};
use bytes::Bytes;
use data_types::object_layout::ObjectLayout;
use data_types::{DataBlobGuid, TraceId};
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc::Receiver, task::JoinHandle};

#[derive(Debug)]
pub struct BlobDeletionRequest {
    pub blob_guid: DataBlobGuid,
    pub block_number: u32,
    pub location: BlobLocation,
}

pub struct BlobClient {
    storage: Arc<BlobStorageImpl>,
    #[allow(dead_code)]
    blob_deletion_task_handle: JoinHandle<()>,
}

impl BlobClient {
    pub async fn new_with_data_vg_info(
        blob_storage_config: &BlobStorageConfig,
        rx: Receiver<BlobDeletionRequest>,
        rpc_request_timeout: Duration,
        rpc_connection_timeout: Duration,
        data_vg_info: data_types::DataVgInfo,
    ) -> Result<Self, BlobStorageError> {
        let storage = Self::create_storage_impl(
            blob_storage_config,
            rpc_request_timeout,
            rpc_connection_timeout,
            data_vg_info,
        )
        .await?;

        Ok(Self::create_client_with_task(storage, rx))
    }

    async fn create_storage_impl(
        blob_storage_config: &BlobStorageConfig,
        rpc_request_timeout: Duration,
        rpc_connection_timeout: Duration,
        data_vg_info: data_types::DataVgInfo,
    ) -> Result<Arc<BlobStorageImpl>, BlobStorageError> {
        let storage = match &blob_storage_config.backend {
            BlobStorageBackend::S3HybridSingleAz => {
                let s3_hybrid_config = blob_storage_config
                    .s3_hybrid_single_az
                    .as_ref()
                    .ok_or_else(|| {
                        BlobStorageError::Config(
                            "S3 hybrid configuration required for Hybrid backend".into(),
                        )
                    })?;

                BlobStorageImpl::HybridSingleAz(
                    S3HybridSingleAzStorage::new_with_data_vg_info(
                        data_vg_info.clone(),
                        s3_hybrid_config,
                        rpc_request_timeout,
                        rpc_connection_timeout,
                    )
                    .await?,
                )
            }
            BlobStorageBackend::AllInBssSingleAz => BlobStorageImpl::AllInBssSingleAz(
                AllInBssSingleAzStorage::new_with_data_vg_info(
                    data_vg_info.clone(),
                    rpc_request_timeout,
                    rpc_connection_timeout,
                )
                .await?,
            ),
        };

        Ok(Arc::new(storage))
    }

    fn create_client_with_task(
        storage: Arc<BlobStorageImpl>,
        rx: Receiver<BlobDeletionRequest>,
    ) -> Self {
        let blob_deletion_task_handle = tokio::spawn({
            let storage = storage.clone();
            async move {
                if let Err(e) = Self::blob_deletion_task(storage, rx).await {
                    tracing::error!("FATAL: blob deletion task error: {e}");
                }
            }
        });

        Self {
            storage,
            blob_deletion_task_handle,
        }
    }

    async fn blob_deletion_task(
        storage: Arc<BlobStorageImpl>,
        mut input: Receiver<BlobDeletionRequest>,
    ) -> Result<(), BlobStorageError> {
        while let Some(request) = input.recv().await {
            let res = storage
                .delete_blob(
                    request.blob_guid,
                    request.block_number,
                    request.location,
                    &TraceId::new(),
                )
                .await;
            match res {
                Ok(()) => {}
                Err(e) => {
                    tracing::warn!(
                        "delete {}-p{} failed: {e}",
                        request.blob_guid,
                        request.block_number
                    );
                }
            }
        }
        Ok(())
    }

    pub fn create_data_blob_guid(&self) -> DataBlobGuid {
        match &*self.storage {
            BlobStorageImpl::HybridSingleAz(storage) => storage.create_data_blob_guid(),
            BlobStorageImpl::AllInBssSingleAz(storage) => storage.create_data_blob_guid(),
        }
    }

    pub fn create_data_blob_guid_with_size_hint(&self, content_len: Option<usize>) -> DataBlobGuid {
        let prefer_ec =
            content_len.is_none_or(|size| size >= ObjectLayout::DEFAULT_BLOCK_SIZE as usize);
        match &*self.storage {
            BlobStorageImpl::HybridSingleAz(storage) => {
                storage.create_data_blob_guid_with_preference(prefer_ec)
            }
            BlobStorageImpl::AllInBssSingleAz(storage) => {
                storage.create_data_blob_guid_with_preference(prefer_ec)
            }
        }
    }

    pub async fn put_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        self.storage
            .put_blob(
                blob_guid.blob_id,
                blob_guid.volume_id,
                block_number,
                body,
                trace_id,
            )
            .await
    }

    pub async fn put_blob_vectored(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        chunks: Vec<actix_web::web::Bytes>,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        self.storage
            .put_blob_vectored(
                blob_guid.blob_id,
                blob_guid.volume_id,
                block_number,
                chunks,
                trace_id,
            )
            .await
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
        self.storage
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

    pub async fn delete_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        location: BlobLocation,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        self.storage
            .delete_blob(blob_guid, block_number, location, trace_id)
            .await
    }
}
