use super::{BlobStorageError, DataVgProxy};
use bytes::Bytes;
use data_types::{DataBlobGuid, DataVgInfo, TraceId};
use metrics_wrapper::histogram;
use std::{sync::Arc, time::Duration};
use tracing::debug;

pub struct AllInBssSingleAzStorage {
    data_vg_proxy: Arc<DataVgProxy>,
}

impl AllInBssSingleAzStorage {
    pub async fn new_with_data_vg_info(
        data_vg_info: DataVgInfo,
        rpc_request_timeout: Duration,
        rpc_connection_timeout: Duration,
    ) -> Result<Self, BlobStorageError> {
        debug!("Initializing AllInBssSingleAzStorage with pre-fetched DataVgInfo");

        let data_vg_proxy = Arc::new(
            DataVgProxy::new(data_vg_info, rpc_request_timeout, rpc_connection_timeout).map_err(
                |e| BlobStorageError::Config(format!("Failed to initialize DataVgProxy: {}", e)),
            )?,
        );

        Ok(Self { data_vg_proxy })
    }

    pub fn create_data_blob_guid(&self) -> DataBlobGuid {
        self.data_vg_proxy.create_data_blob_guid()
    }

    pub fn create_data_blob_guid_with_preference(&self, prefer_ec: bool) -> DataBlobGuid {
        self.data_vg_proxy
            .create_data_blob_guid_with_preference(prefer_ec)
    }
}

impl AllInBssSingleAzStorage {
    pub async fn put_blob(
        &self,
        blob_id: uuid::Uuid,
        volume_id: u16,
        block_number: u32,
        body: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        histogram!("blob_size", "operation" => "put").record(body.len() as f64);

        let blob_guid = DataBlobGuid { blob_id, volume_id };
        self.data_vg_proxy
            .put_blob(blob_guid, block_number, body, trace_id)
            .await?;

        Ok(())
    }

    pub async fn put_blob_vectored(
        &self,
        blob_id: uuid::Uuid,
        volume_id: u16,
        block_number: u32,
        chunks: Vec<actix_web::web::Bytes>,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        let total_size: usize = chunks.iter().map(|c| c.len()).sum();
        histogram!("blob_size", "operation" => "put").record(total_size as f64);

        let blob_guid = DataBlobGuid { blob_id, volume_id };
        self.data_vg_proxy
            .put_blob_vectored(blob_guid, block_number, chunks, trace_id)
            .await?;

        Ok(())
    }

    pub async fn get_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        self.data_vg_proxy
            .get_blob(blob_guid, block_number, content_len, body, trace_id)
            .await?;

        histogram!("blob_size", "operation" => "get").record(body.len() as f64);
        Ok(())
    }

    pub async fn delete_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        self.data_vg_proxy
            .delete_blob(blob_guid, block_number, trace_id)
            .await?;

        Ok(())
    }
}
