//! Block-addressed blob storage on an S3 bucket, shared by the S3 gateway
//! (hybrid data volume) and the FUSE client (S3 data volume).
//!
//! Every block generation is one object at `{blob_id}-p{block}-v{version}`,
//! the BSS key layout minus its volume prefix. The version is 16-digit
//! zero-padded hex, as on BSS, so a listing under `{blob_id}-p{block}-v`
//! returns generations in order. Together with a conditional PUT this
//! gives the same write-once-per-key contract BSS enforces, which the VFS
//! flush protocol relies on.

use aws_config::{BehaviorVersion, retry::RetryConfig};
pub use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{
    Client as S3Client, Config as S3Config,
    config::{Credentials, Region},
    error::SdkError,
    operation::get_object::GetObjectError,
};
use bytes::Bytes;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum S3BlobError {
    #[error("object not found")]
    NotFound,
    #[error("object already exists")]
    AlreadyExists,
    #[error("s3 error: {0}")]
    Other(String),
}

/// Object key for one exact block generation.
pub fn blob_key(blob_id: Uuid, block_number: u32, version: u64) -> String {
    format!("{blob_id}-p{block_number}-v{version:016x}")
}

/// Listing prefix covering every block generation of a blob.
pub fn blob_prefix(blob_id: Uuid) -> String {
    format!("{blob_id}-p")
}

/// Inverse of [`blob_key`] for keys under [`blob_prefix`].
pub fn parse_blob_key(key: &str, blob_id: Uuid) -> Option<(u32, u64)> {
    let rest = key.strip_prefix(&blob_prefix(blob_id))?;
    let (block, version) = rest.split_once("-v")?;
    Some((block.parse().ok()?, u64::from_str_radix(version, 16).ok()?))
}

/// Create an S3 client for AWS or a local S3-compatible service (minio).
pub async fn create_s3_client(
    s3_host: &str,
    s3_port: u16,
    s3_region: &str,
    force_path_style: bool,
) -> S3Client {
    // Callers handle retries themselves for better visibility.
    let retry_config = RetryConfig::disabled();

    if s3_host.ends_with("amazonaws.com") {
        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(s3_region.to_string()))
            .retry_config(retry_config)
            .load()
            .await;
        S3Client::new(&aws_config)
    } else {
        let credentials = Credentials::new("minioadmin", "minioadmin", None, None, "minio");
        let endpoint_url = format!("{s3_host}:{s3_port}");

        let mut s3_config_builder = S3Config::builder()
            .endpoint_url(&endpoint_url)
            .region(Region::new(s3_region.to_string()))
            .credentials_provider(credentials)
            .retry_config(retry_config)
            .behavior_version(BehaviorVersion::latest())
            .disable_s3_express_session_auth(true); // minio compatibility

        if force_path_style {
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        S3Client::from_conf(s3_config_builder.build())
    }
}

#[derive(Clone)]
pub struct S3BlobStore {
    client: S3Client,
    bucket: String,
}

impl S3BlobStore {
    pub fn new(client: S3Client, bucket: String) -> Self {
        Self { client, bucket }
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub async fn get(
        &self,
        blob_id: Uuid,
        block_number: u32,
        version: u64,
    ) -> Result<Bytes, S3BlobError> {
        let key = blob_key(blob_id, block_number, version);
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| match &e {
                SdkError::ServiceError(se) if matches!(se.err(), GetObjectError::NoSuchKey(_)) => {
                    S3BlobError::NotFound
                }
                _ => {
                    tracing::error!(bucket = %self.bucket, %key, error = ?e, "S3 get_object failed");
                    S3BlobError::Other(e.to_string())
                }
            })?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|e| S3BlobError::Other(e.to_string()))?
            .into_bytes();
        Ok(bytes)
    }

    /// Unconditional put. Used where the caller already guarantees a fresh
    /// key (the S3 gateway allocates a new blob id per object) or must be
    /// able to re-send the same bytes (MPU part retries).
    pub async fn put(
        &self,
        blob_id: Uuid,
        block_number: u32,
        version: u64,
        body: ByteStream,
    ) -> Result<(), S3BlobError> {
        self.put_inner(blob_id, block_number, version, body, false)
            .await
    }

    /// Write-once put: fails with `AlreadyExists` if the key is present.
    pub async fn put_if_absent(
        &self,
        blob_id: Uuid,
        block_number: u32,
        version: u64,
        body: ByteStream,
    ) -> Result<(), S3BlobError> {
        self.put_inner(blob_id, block_number, version, body, true)
            .await
    }

    async fn put_inner(
        &self,
        blob_id: Uuid,
        block_number: u32,
        version: u64,
        body: ByteStream,
        if_absent: bool,
    ) -> Result<(), S3BlobError> {
        let key = blob_key(blob_id, block_number, version);
        let mut req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body);
        if if_absent {
            req = req.if_none_match("*");
        }
        req.send().await.map_err(|e| match &e {
            SdkError::ServiceError(se) if se.raw().status().as_u16() == 412 => {
                S3BlobError::AlreadyExists
            }
            _ => {
                tracing::error!(bucket = %self.bucket, %key, error = ?e, "S3 put_object failed");
                S3BlobError::Other(e.to_string())
            }
        })?;
        Ok(())
    }

    /// Delete one exact generation. Deleting an absent key is not an error.
    pub async fn delete(
        &self,
        blob_id: Uuid,
        block_number: u32,
        version: u64,
    ) -> Result<(), S3BlobError> {
        let key = blob_key(blob_id, block_number, version);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| {
                tracing::warn!(bucket = %self.bucket, %key, error = ?e, "S3 delete_object failed");
                S3BlobError::Other(e.to_string())
            })?;
        Ok(())
    }

    /// Every `(block_number, version)` stored for a blob, including
    /// generations no layout references any more.
    pub async fn list_blob_blocks(&self, blob_id: Uuid) -> Result<Vec<(u32, u64)>, S3BlobError> {
        let prefix = blob_prefix(blob_id);
        let mut entries = Vec::new();
        let mut continuation: Option<String> = None;
        loop {
            let output = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&prefix)
                .set_continuation_token(continuation.take())
                .send()
                .await
                .map_err(|e| {
                    tracing::warn!(bucket = %self.bucket, %prefix, error = ?e, "S3 list_objects_v2 failed");
                    S3BlobError::Other(e.to_string())
                })?;
            for object in output.contents() {
                if let Some((block, version)) =
                    object.key().and_then(|k| parse_blob_key(k, blob_id))
                {
                    entries.push((block, version));
                }
            }
            match output.next_continuation_token() {
                Some(token) if output.is_truncated().unwrap_or(false) => {
                    continuation = Some(token.to_string());
                }
                _ => break,
            }
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_round_trip() {
        let id = Uuid::now_v7();
        assert_eq!(blob_key(id, 3, 7), format!("{id}-p3-v0000000000000007"));
        assert_eq!(parse_blob_key(&blob_key(id, 3, 1), id), Some((3, 1)));
        assert_eq!(parse_blob_key(&blob_key(id, 3, 7), id), Some((3, 7)));
        assert_eq!(
            parse_blob_key(&blob_key(id, 3, u64::MAX), id),
            Some((3, u64::MAX))
        );
        // Padded hex keeps listing order equal to generation order.
        assert!(blob_key(id, 3, 9) < blob_key(id, 3, 10));
        assert_eq!(parse_blob_key(&blob_key(Uuid::now_v7(), 3, 7), id), None);
        assert_eq!(parse_blob_key(&format!("{id}-p3"), id), None);
        assert_eq!(parse_blob_key(&format!("{id}-pX-v1"), id), None);
    }
}
