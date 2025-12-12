use bytes::Bytes;
use data_types::TraceId;
use nss_codec::{get_inode_response, list_inodes_response};
use rpc_client_common::{RpcError, nss_rpc_retry, rss_rpc_retry};
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcClientRss;
use rss_codec::AzStatusMap;
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataBlobTrackingError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError),
    #[error("Blob not found")]
    NotFound,
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Helper struct for managing data blob tracking operations
pub struct DataBlobTracker {
    rss_client: RpcClientRss,
    nss_client: RpcClientNss,
}

impl Default for DataBlobTracker {
    fn default() -> Self {
        Self::new()
    }
}

const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

impl DataBlobTracker {
    pub fn new() -> Self {
        let rss_client = RpcClientRss::new_from_addresses(
            vec!["localhost:9000".to_string()],
            DEFAULT_CONNECTION_TIMEOUT,
        );
        let nss_client = RpcClientNss::new_from_address(
            "localhost:8000".to_string(),
            DEFAULT_CONNECTION_TIMEOUT,
        );
        Self {
            rss_client,
            nss_client,
        }
    }

    pub fn with_endpoints(rss_endpoint: String, nss_endpoint: String) -> Self {
        Self::with_endpoints_and_timeout(rss_endpoint, nss_endpoint, DEFAULT_CONNECTION_TIMEOUT)
    }

    pub fn with_endpoints_and_timeout(
        rss_endpoint: String,
        nss_endpoint: String,
        connection_timeout: Duration,
    ) -> Self {
        let rss_client = RpcClientRss::new_from_addresses(vec![rss_endpoint], connection_timeout);
        let nss_client = RpcClientNss::new_from_address(nss_endpoint, connection_timeout);
        Self {
            rss_client,
            nss_client,
        }
    }

    /// Record a blob that exists only in local AZ
    pub async fn put_single_copy_data_blob(
        &self,
        tracking_root_blob_name: &str,
        blob_key: &str,
        metadata: &[u8],
    ) -> Result<(), DataBlobTrackingError> {
        let key = format!("single/{blob_key}");
        let trace_id = TraceId::new();
        nss_rpc_retry!(
            &self.nss_client,
            put_inode(
                tracking_root_blob_name,
                &key,
                Bytes::copy_from_slice(metadata),
                None,
                &trace_id
            )
        )
        .await?;
        Ok(())
    }

    /// Check if a blob is single-copy
    pub async fn get_single_copy_data_blob(
        &self,
        tracking_root_blob_name: &str,
        blob_key: &str,
    ) -> Result<Option<Vec<u8>>, DataBlobTrackingError> {
        let key = format!("single/{blob_key}");
        let trace_id = TraceId::new();
        match nss_rpc_retry!(
            &self.nss_client,
            get_inode(tracking_root_blob_name, &key, None, &trace_id)
        )
        .await
        {
            Ok(response) => {
                // Extract bytes from response
                match response.result {
                    Some(get_inode_response::Result::Ok(bytes)) => Ok(Some(bytes.to_vec())),
                    Some(get_inode_response::Result::ErrNotFound(_)) => Ok(None),
                    _ => Err(DataBlobTrackingError::Internal(
                        "Unexpected NSS response".into(),
                    )),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Remove blob from single-copy tracking
    pub async fn delete_single_copy_data_blob(
        &self,
        tracking_root_blob_name: &str,
        blob_key: &str,
    ) -> Result<(), DataBlobTrackingError> {
        let key = format!("single/{blob_key}");
        let trace_id = TraceId::new();
        match nss_rpc_retry!(
            &self.nss_client,
            delete_inode(tracking_root_blob_name, &key, None, &trace_id)
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(RpcError::NotFound) => Ok(()), // Already deleted, that's fine
            Err(e) => Err(e.into()),
        }
    }

    /// Remove blob from single-copy tracking by blob key (with null terminator handling)
    pub async fn delete_single_copy_data_blob_by_key(
        &self,
        tracking_root_blob_name: &str,
        blob_key: &str,
    ) -> Result<(), DataBlobTrackingError> {
        // Use the blob key with single prefix, trimming null terminators
        let trimmed_key = blob_key.trim_end_matches('\0');
        let key = format!("single/{trimmed_key}");
        let trace_id = TraceId::new();
        match nss_rpc_retry!(
            &self.nss_client,
            delete_inode(tracking_root_blob_name, &key, None, &trace_id)
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(RpcError::NotFound) => Ok(()), // Already deleted, that's fine
            Err(e) => Err(e.into()),
        }
    }

    /// Record a deleted blob to skip during resync
    pub async fn put_deleted_data_blob(
        &self,
        tracking_root_blob_name: &str,
        blob_key: &str,
        timestamp: &[u8],
    ) -> Result<(), DataBlobTrackingError> {
        let key = format!("deleted/{blob_key}");
        let trace_id = TraceId::new();
        nss_rpc_retry!(
            &self.nss_client,
            put_inode(
                tracking_root_blob_name,
                &key,
                Bytes::copy_from_slice(timestamp),
                None,
                &trace_id
            )
        )
        .await?;
        Ok(())
    }

    /// Check if a blob is marked as deleted
    pub async fn get_deleted_data_blob(
        &self,
        tracking_root_blob_name: &str,
        blob_key: &str,
    ) -> Result<Option<Vec<u8>>, DataBlobTrackingError> {
        let key = format!("deleted/{blob_key}");
        let trace_id = TraceId::new();
        match nss_rpc_retry!(
            &self.nss_client,
            get_inode(tracking_root_blob_name, &key, None, &trace_id)
        )
        .await
        {
            Ok(response) => {
                // Extract bytes from response
                match response.result {
                    Some(get_inode_response::Result::Ok(bytes)) => Ok(Some(bytes.to_vec())),
                    Some(get_inode_response::Result::ErrNotFound(_)) => Ok(None),
                    _ => Err(DataBlobTrackingError::Internal(
                        "Unexpected NSS response".into(),
                    )),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Get deleted data blob tracking entry by blob key
    pub async fn get_deleted_data_blob_by_key(
        &self,
        tracking_root_blob_name: &str,
        blob_key: &str,
    ) -> Result<Option<Vec<u8>>, DataBlobTrackingError> {
        // Use the blob key with deleted prefix, trimming null terminators
        let trimmed_key = blob_key.trim_end_matches('\0');
        let key = format!("deleted/{trimmed_key}");
        let trace_id = TraceId::new();
        match nss_rpc_retry!(
            &self.nss_client,
            get_inode(tracking_root_blob_name, &key, None, &trace_id)
        )
        .await
        {
            Ok(response) => match response.result {
                Some(get_inode_response::Result::Ok(bytes)) => Ok(Some(bytes.to_vec())),
                Some(get_inode_response::Result::ErrNotFound(_)) => Ok(None),
                _ => Err(DataBlobTrackingError::Internal(
                    "Unexpected NSS response".into(),
                )),
            },
            Err(e) => Err(e.into()),
        }
    }

    /// Remove blob from deleted tracking (used during sanitize)
    pub async fn delete_deleted_data_blob(
        &self,
        tracking_root_blob_name: &str,
        blob_key: &str,
    ) -> Result<(), DataBlobTrackingError> {
        let key = format!("deleted/{blob_key}");
        let trace_id = TraceId::new();
        match nss_rpc_retry!(
            &self.nss_client,
            delete_inode(tracking_root_blob_name, &key, None, &trace_id)
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(RpcError::NotFound) => Ok(()), // Already deleted, that's fine
            Err(e) => Err(e.into()),
        }
    }

    /// List single-copy data blobs for resync
    pub async fn list_single_copy_data_blobs(
        &self,
        tracking_root_blob_name: &str,
        prefix: &str,
        start_after: &str,
        max_keys: u32,
    ) -> Result<Vec<(String, String)>, DataBlobTrackingError> {
        let trace_id = TraceId::new();
        // Add single/ prefix to the search parameters
        let search_prefix = if prefix.is_empty() {
            "single/".to_string()
        } else {
            format!("single/{prefix}")
        };
        let search_start_after = if start_after.is_empty() {
            "".to_string() // Let NSS handle empty start_after with the prefix
        } else {
            format!("single/{start_after}")
        };

        let response = nss_rpc_retry!(
            &self.nss_client,
            list_inodes(
                tracking_root_blob_name,
                max_keys,
                &search_prefix,
                "",
                &search_start_after,
                false,
                None,
                &trace_id
            )
        )
        .await?;

        // Extract the list from the response
        match response.result {
            Some(list_inodes_response::Result::Ok(inodes)) => {
                let result = inodes
                    .inodes
                    .into_iter()
                    .filter_map(|inode| {
                        // Strip the "single/" prefix from the key
                        if let Some(blob_key) = inode.key.strip_prefix("single/") {
                            let missing_az = String::from_utf8_lossy(&inode.inode).to_string();
                            Some((blob_key.to_string(), missing_az))
                        } else {
                            None
                        }
                    })
                    .collect();
                Ok(result)
            }
            _ => Err(DataBlobTrackingError::Internal(
                "Unexpected NSS list response".into(),
            )),
        }
    }

    /// List deleted data blobs for sanitize
    pub async fn list_deleted_data_blobs(
        &self,
        tracking_root_blob_name: &str,
        prefix: &str,
        start_after: &str,
        max_keys: u32,
    ) -> Result<Vec<(String, Vec<u8>)>, DataBlobTrackingError> {
        let trace_id = TraceId::new();
        // Add deleted/ prefix to the search parameters
        let search_prefix = if prefix.is_empty() {
            "deleted/".to_string()
        } else {
            format!("deleted/{prefix}")
        };
        let search_start_after = if start_after.is_empty() {
            "".to_string() // Let NSS handle empty start_after with the prefix
        } else {
            format!("deleted/{start_after}")
        };

        let response = nss_rpc_retry!(
            &self.nss_client,
            list_inodes(
                tracking_root_blob_name,
                max_keys,
                &search_prefix,
                "",
                &search_start_after,
                false,
                None,
                &trace_id
            )
        )
        .await?;

        // Extract the list from the response
        match response.result {
            Some(list_inodes_response::Result::Ok(inodes)) => {
                let result = inodes
                    .inodes
                    .into_iter()
                    .filter_map(|inode| {
                        // Strip the "deleted/" prefix from the key
                        if let Some(blob_key) = inode.key.strip_prefix("deleted/") {
                            Some((blob_key.to_string(), inode.inode.to_vec()))
                        } else {
                            None
                        }
                    })
                    .collect();
                Ok(result)
            }
            _ => Err(DataBlobTrackingError::Internal(
                "Unexpected NSS list response".into(),
            )),
        }
    }

    /// List all buckets with their tracking root blob names
    /// Uses efficient RSS list operation that now returns bucket values directly
    pub async fn list_buckets(
        &self,
    ) -> Result<Vec<(String, Option<String>)>, DataBlobTrackingError> {
        let trace_id = TraceId::new();
        let prefix = "bucket:";
        let bucket_values = rss_rpc_retry!(&self.rss_client, list(prefix, None, &trace_id)).await?;

        let mut buckets = Vec::new();
        for bucket_value in &bucket_values {
            // Parse the bucket JSON data directly from RSS list response
            match serde_json::from_str::<data_types::Bucket>(bucket_value) {
                Ok(bucket) => {
                    buckets.push((bucket.bucket_name, bucket.tracking_root_blob_name));
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to parse bucket data: {} (data: '{}')",
                        e,
                        bucket_value
                    );
                }
            }
        }

        Ok(buckets)
    }

    /// Get current timestamp as bytes for deleted blob tracking
    pub fn current_timestamp_bytes() -> Vec<u8> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        timestamp.to_le_bytes().to_vec()
    }

    /// Get AZ status from RSS service discovery
    pub async fn get_az_status(
        &self,
        timeout: Option<Duration>,
    ) -> Result<AzStatusMap, DataBlobTrackingError> {
        let trace_id = TraceId::new();
        rss_rpc_retry!(&self.rss_client, get_az_status(timeout, &trace_id))
            .await
            .map_err(|e| e.into())
    }

    /// Set AZ status in RSS service discovery
    pub async fn set_az_status(
        &self,
        az_id: &str,
        status: &str,
        timeout: Option<Duration>,
    ) -> Result<(), DataBlobTrackingError> {
        let trace_id = TraceId::new();
        rss_rpc_retry!(
            &self.rss_client,
            set_az_status(az_id, status, timeout, &trace_id)
        )
        .await
        .map_err(|e| e.into())
    }
}
