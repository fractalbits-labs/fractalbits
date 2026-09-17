#![allow(dead_code)]
use super::permission::BucketKeyPerm;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub key_id: String,
    pub secret_key: String,
    pub name: String,
    pub allow_create_bucket: bool,
    pub authorized_buckets: HashMap<String /* bucket name */, BucketKeyPerm>,
    pub is_deleted: bool,
}

impl ApiKey {
    pub fn new(name: &str) -> Self {
        let key_id = hex::encode(&rand::random::<[u8; 12]>()[..]);
        let secret_key = hex::encode(&rand::random::<[u8; 32]>()[..]);
        Self {
            key_id,
            secret_key,
            name: name.to_string(),
            allow_create_bucket: true,
            authorized_buckets: HashMap::new(),
            is_deleted: false,
        }
    }

    pub fn new_for_test() -> Self {
        let name = "api_key_for_test".to_string();
        let key_id = "test_api_key".to_string();
        let secret_key = "test_api_secret".to_string();
        Self {
            key_id,
            secret_key,
            name,
            allow_create_bucket: true,
            authorized_buckets: HashMap::new(),
            is_deleted: false,
        }
    }

    /// Get permissions for a bucket
    pub fn bucket_permissions(&self, bucket: &str) -> BucketKeyPerm {
        self.authorized_buckets
            .get(bucket)
            .cloned()
            .unwrap_or(BucketKeyPerm::NO_PERMISSIONS)
    }

    /// Check if `Key` is allowed to read in bucket
    pub fn allow_read(&self, bucket: &str) -> bool {
        self.bucket_permissions(bucket).allow_read
    }

    /// Check if `Key` is allowed to write in bucket
    pub fn allow_write(&self, bucket: &str) -> bool {
        self.bucket_permissions(bucket).allow_write
    }

    /// Check if `Key` is owner of bucket
    pub fn allow_owner(&self, bucket: &str) -> bool {
        self.bucket_permissions(bucket).allow_owner
    }
}
