use super::{
    BlobLocation, BlobStorageError, DataVgProxy, blob_key, chunks_to_bytestream, create_s3_client,
};
use crate::{config::S3HybridSingleAzConfig, object_layout::ObjectLayout};
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use data_types::{DataBlobGuid, DataVgInfo, EcVolume, TraceId};
use metrics_wrapper::histogram;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::debug;
use uuid::Uuid;

pub struct S3HybridSingleAzStorage {
    data_vg_proxy: Arc<DataVgProxy>,
    client_s3: S3Client,
    data_blob_in_s3_bucket: String,
}

impl S3HybridSingleAzStorage {
    pub async fn new_with_data_vg_info(
        data_vg_info: DataVgInfo,
        s3_hybrid_config: &S3HybridSingleAzConfig,
        rpc_request_timeout: Duration,
        rpc_connection_timeout: Duration,
    ) -> Result<Self, BlobStorageError> {
        debug!("Initializing S3HybridSingleAzStorage with pre-fetched DataVgInfo");

        let data_vg_proxy = Arc::new(
            DataVgProxy::new(data_vg_info, rpc_request_timeout, rpc_connection_timeout).map_err(
                |e| BlobStorageError::Config(format!("Failed to initialize DataVgProxy: {}", e)),
            )?,
        );

        let client_s3 = create_s3_client(
            &s3_hybrid_config.s3_host,
            s3_hybrid_config.s3_port,
            &s3_hybrid_config.s3_region,
            false,
        )
        .await;

        Ok(Self {
            data_vg_proxy,
            client_s3,
            data_blob_in_s3_bucket: s3_hybrid_config.s3_bucket.clone(),
        })
    }

    pub fn create_data_blob_guid(&self) -> DataBlobGuid {
        self.data_vg_proxy.create_data_blob_guid()
    }

    pub fn create_data_blob_guid_with_preference(&self, prefer_ec: bool) -> DataBlobGuid {
        self.data_vg_proxy
            .create_data_blob_guid_with_preference(prefer_ec)
    }
}

impl S3HybridSingleAzStorage {
    pub async fn put_blob(
        &self,
        blob_id: Uuid,
        volume_id: u16,
        block_number: u32,
        body: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        histogram!("blob_size", "operation" => "put").record(body.len() as f64);
        let start = Instant::now();

        // Determine location based on size (single block and small size)
        let is_small = block_number == 0 && body.len() < ObjectLayout::DEFAULT_BLOCK_SIZE as usize;

        if is_small || EcVolume::is_ec_volume_id(volume_id) {
            // Small blob or EC-routed blob - store in DataVgProxy
            let blob_guid = DataBlobGuid { blob_id, volume_id };
            self.data_vg_proxy
                .put_blob(blob_guid, block_number, body, trace_id)
                .await?;
        } else {
            // Large blob - store in S3 (volume_id doesn't matter for S3 storage, but we'll use S3_VOLUME for metadata consistency)
            let s3_key = blob_key(blob_id, block_number);
            self.client_s3
                .put_object()
                .bucket(&self.data_blob_in_s3_bucket)
                .key(&s3_key)
                .body(body.into())
                .send()
                .await
                .map_err(|e| BlobStorageError::S3(e.to_string()))?;

            histogram!("rpc_duration_nanos", "type" => "s3", "name" => "put_blob_s3")
                .record(start.elapsed().as_nanos() as f64);
        }

        Ok(())
    }

    pub async fn put_blob_vectored(
        &self,
        blob_id: Uuid,
        volume_id: u16,
        block_number: u32,
        chunks: Vec<actix_web::web::Bytes>,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        let total_size: usize = chunks.iter().map(|c| c.len()).sum();
        histogram!("blob_size", "operation" => "put").record(total_size as f64);
        let start = Instant::now();

        let is_small = block_number == 0 && total_size < ObjectLayout::DEFAULT_BLOCK_SIZE as usize;

        if is_small || EcVolume::is_ec_volume_id(volume_id) {
            let blob_guid = DataBlobGuid { blob_id, volume_id };
            self.data_vg_proxy
                .put_blob_vectored(blob_guid, block_number, chunks, trace_id)
                .await?;
        } else {
            let s3_key = blob_key(blob_id, block_number);
            self.client_s3
                .put_object()
                .bucket(&self.data_blob_in_s3_bucket)
                .key(&s3_key)
                .body(chunks_to_bytestream(chunks))
                .send()
                .await
                .map_err(|e| {
                    tracing::error!(
                        "S3 put_object failed: bucket={}, key={}, error={:?}",
                        self.data_blob_in_s3_bucket,
                        s3_key,
                        e
                    );
                    BlobStorageError::S3(e.to_string())
                })?;

            histogram!("rpc_duration_nanos", "type" => "s3", "name" => "put_blob_s3")
                .record(start.elapsed().as_nanos() as f64);
        }

        Ok(())
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
        match location {
            BlobLocation::DataVgProxy => {
                // Small blob - get from DataVgProxy
                self.data_vg_proxy
                    .get_blob(blob_guid, block_number, content_len, body, trace_id)
                    .await?;
            }
            BlobLocation::S3 => {
                // Large blob - get from S3
                let s3_key = blob_key(blob_guid.blob_id, block_number);
                let result = self
                    .client_s3
                    .get_object()
                    .bucket(&self.data_blob_in_s3_bucket)
                    .key(&s3_key)
                    .send()
                    .await
                    .map_err(|e| {
                        tracing::error!(
                            "S3 get_object failed: bucket={}, key={}, error={:?}",
                            self.data_blob_in_s3_bucket,
                            s3_key,
                            e
                        );
                        BlobStorageError::S3(e.to_string())
                    })?;

                let bytes = result
                    .body
                    .collect()
                    .await
                    .map_err(|e| BlobStorageError::S3(e.to_string()))?
                    .into_bytes();

                *body = bytes;
            }
        }

        histogram!("blob_size", "operation" => "get").record(body.len() as f64);
        Ok(())
    }

    pub async fn delete_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        location: BlobLocation,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        match location {
            BlobLocation::DataVgProxy => {
                // Small blob - delete from DataVgProxy
                self.data_vg_proxy
                    .delete_blob(blob_guid, block_number, trace_id)
                    .await?;
            }
            BlobLocation::S3 => {
                // Large blob - delete from S3
                let s3_key = blob_key(blob_guid.blob_id, block_number);
                self.client_s3
                    .delete_object()
                    .bucket(&self.data_blob_in_s3_bucket)
                    .key(&s3_key)
                    .send()
                    .await
                    .map_err(|e| {
                        tracing::warn!("delete {s3_key} failed: {e}");
                        BlobStorageError::S3(e.to_string())
                    })?;
            }
        }

        Ok(())
    }
}
