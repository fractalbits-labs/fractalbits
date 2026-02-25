use crate::{
    blob_storage::{
        AllInBssSingleAzStorage, BlobLocation, BlobStorageError, BlobStorageImpl,
        S3ExpressMultiAzStorage, S3HybridSingleAzStorage,
    },
    config::{BlobStorageBackend, BlobStorageConfig},
    object_layout::ObjectLayout,
};
use bytes::Bytes;
use data_blob_tracking::DataBlobTracker;
use data_types::{DataBlobGuid, TraceId};
use moka::future::Cache;
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc::Receiver, task::JoinHandle};

#[derive(Debug)]
pub struct BlobDeletionRequest {
    pub tracking_root_blob_name: Option<String>,
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
        data_blob_tracker: Option<Arc<DataBlobTracker>>,
        data_vg_info: data_types::DataVgInfo,
    ) -> Result<(Self, Option<Arc<Cache<String, String>>>), BlobStorageError> {
        let (storage, az_status_cache) = Self::create_storage_impl(
            blob_storage_config,
            rpc_request_timeout,
            rpc_connection_timeout,
            data_blob_tracker,
            data_vg_info,
        )
        .await?;

        let client = Self::create_client_with_task(storage, rx);
        Ok((client, az_status_cache))
    }

    async fn create_storage_impl(
        blob_storage_config: &BlobStorageConfig,
        rpc_request_timeout: Duration,
        rpc_connection_timeout: Duration,
        data_blob_tracker: Option<Arc<DataBlobTracker>>,
        data_vg_info: data_types::DataVgInfo,
    ) -> Result<(Arc<BlobStorageImpl>, Option<Arc<Cache<String, String>>>), BlobStorageError> {
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
            BlobStorageBackend::S3ExpressMultiAz => {
                let data_blob_tracker = data_blob_tracker.ok_or_else(|| {
                    BlobStorageError::Config(
                        "DataBlobTracker required for S3ExpressWithTracking backend".into(),
                    )
                })?;

                let s3_express_multi_az_config =
                    Self::get_s3_express_multi_az_config(blob_storage_config, "S3ExpressMultiAz")?;

                let az_status_cache = Arc::new(
                    Cache::builder()
                        .time_to_idle(Duration::from_secs(30))
                        .max_capacity(100)
                        .build(),
                );

                let storage = BlobStorageImpl::S3ExpressMultiAz(
                    S3ExpressMultiAzStorage::new(
                        s3_express_multi_az_config,
                        data_blob_tracker,
                        az_status_cache.clone(),
                    )
                    .await?,
                );

                return Ok((Arc::new(storage), Some(az_status_cache)));
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

        Ok((Arc::new(storage), None))
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

    fn get_s3_express_multi_az_config<'a>(
        blob_storage_config: &'a BlobStorageConfig,
        backend_name: &str,
    ) -> Result<&'a crate::config::S3ExpressMultiAzConfig, BlobStorageError> {
        blob_storage_config
            .s3_express_multi_az
            .as_ref()
            .ok_or_else(|| {
                BlobStorageError::Config(format!(
                    "S3 Express configuration required for {backend_name} backend"
                ))
            })
    }

    async fn blob_deletion_task(
        storage: Arc<BlobStorageImpl>,
        mut input: Receiver<BlobDeletionRequest>,
    ) -> Result<(), BlobStorageError> {
        while let Some(request) = input.recv().await {
            let res = storage
                .delete_blob(
                    request.tracking_root_blob_name.as_deref(),
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
            BlobStorageImpl::S3ExpressMultiAz(storage) => storage.create_data_blob_guid(),
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
            BlobStorageImpl::S3ExpressMultiAz(storage) => storage.create_data_blob_guid(),
            BlobStorageImpl::AllInBssSingleAz(storage) => {
                storage.create_data_blob_guid_with_preference(prefer_ec)
            }
        }
    }

    pub async fn put_blob(
        &self,
        tracking_root_blob_name: Option<&str>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        self.storage
            .put_blob(
                tracking_root_blob_name,
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
        tracking_root_blob_name: Option<&str>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        chunks: Vec<actix_web::web::Bytes>,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        self.storage
            .put_blob_vectored(
                tracking_root_blob_name,
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
        tracking_root_blob_name: Option<&str>,
        blob_guid: DataBlobGuid,
        block_number: u32,
        location: BlobLocation,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        self.storage
            .delete_blob(
                tracking_root_blob_name,
                blob_guid,
                block_number,
                location,
                trace_id,
            )
            .await
    }
}
