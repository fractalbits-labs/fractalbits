//! S3-backed data volume for the VFS.
//!
//! Blocks live at versioned write-once keys (see `s3_blob_store`), which
//! is the same contract the flush protocol relies on for BSS. The AWS SDK
//! needs tokio, so the store runs on a small runtime owned here and the
//! compio callers await the spawned join handles.

use bss_codec::list_blob_blocks_response::BlobBlockEntry;
use bytes::Bytes;
use data_types::DataBlobGuid;
use s3_blob_store::{ByteStream, S3BlobError, S3BlobStore, create_s3_client};
use std::future::Future;
use tokio::runtime::Runtime;
use volume_group_proxy::DataVgError;

use crate::config::Config;
use crate::error::FsError;

pub struct S3DataVolume {
    runtime: Runtime,
    store: S3BlobStore,
}

impl S3DataVolume {
    /// `None` when no bucket is configured. Errors if the config asks for
    /// S3 as the data volume without naming a bucket.
    pub fn from_config(config: &Config) -> Result<Option<Self>, String> {
        if !config.s3_enabled() {
            if config.data_volume_is_s3() {
                return Err("data_volume=s3 requires s3_bucket".to_string());
            }
            return Ok(None);
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("fs-s3")
            .enable_all()
            .build()
            .map_err(|e| format!("failed to start S3 runtime: {e}"))?;
        let client = runtime.block_on(create_s3_client(
            &config.s3_host,
            config.s3_port,
            &config.s3_region,
            false,
        ));
        tracing::info!(
            bucket = %config.s3_bucket,
            host = %config.s3_host,
            data_volume = %config.data_volume,
            "S3 data volume enabled"
        );
        Ok(Some(Self {
            runtime,
            store: S3BlobStore::new(client, config.s3_bucket.clone()),
        }))
    }

    async fn run<F>(&self, fut: F) -> F::Output
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.runtime
            .handle()
            .spawn(fut)
            .await
            .expect("S3 data volume task panicked")
    }

    pub async fn read_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
    ) -> Result<Bytes, FsError> {
        let store = self.store.clone();
        self.run(async move { store.get(blob_guid.blob_id, block_number, version).await })
            .await
            .map_err(map_err)
    }

    /// Write-once put. A key that already exists is treated like BSS's
    /// VersionSkipped: the generation is present, so the write succeeded.
    pub async fn write_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        version: u64,
    ) -> Result<(), FsError> {
        let store = self.store.clone();
        let result = self
            .run(async move {
                store
                    .put_if_absent(
                        blob_guid.blob_id,
                        block_number,
                        version,
                        ByteStream::from(body),
                    )
                    .await
            })
            .await;
        match result {
            Ok(()) => Ok(()),
            Err(S3BlobError::AlreadyExists) => {
                tracing::debug!(%blob_guid, block_number, version, "S3 block already present");
                Ok(())
            }
            Err(e) => Err(map_err(e)),
        }
    }

    pub async fn delete_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
    ) -> Result<(), FsError> {
        let store = self.store.clone();
        self.run(async move { store.delete(blob_guid.blob_id, block_number, version).await })
            .await
            .map_err(map_err)
    }

    /// Every stored generation of the blob, in BSS listing shape.
    pub async fn list_blob_blocks(
        &self,
        blob_guid: DataBlobGuid,
    ) -> Result<Vec<BlobBlockEntry>, FsError> {
        let store = self.store.clone();
        let entries = self
            .run(async move { store.list_blob_blocks(blob_guid.blob_id).await })
            .await
            .map_err(map_err)?;
        Ok(entries
            .into_iter()
            .map(|(block_number, version)| BlobBlockEntry {
                block_number,
                version,
                cohort_tag: 0,
            })
            .collect())
    }
}

fn map_err(e: S3BlobError) -> FsError {
    match e {
        S3BlobError::NotFound => FsError::DataVg(DataVgError::BlockNotFound),
        S3BlobError::AlreadyExists => FsError::AlreadyExists,
        S3BlobError::Other(msg) => FsError::Internal(format!("S3 data volume: {msg}")),
    }
}
