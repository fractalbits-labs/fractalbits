#![allow(dead_code)]
use super::permission::BucketKeyPerm;
use super::routing_key::RoutingKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize)]
pub struct Bucket {
    pub bucket_name: String,
    pub creation_date: u64,
    pub authorized_keys: HashMap<String /* ApiKey id */, BucketKeyPerm>,
    pub root_blob_name: String,
    pub tracking_root_blob_name: Option<String>,
    pub routing_key: RoutingKey,
}

impl Bucket {
    pub fn new(
        bucket_name: String,
        root_blob_name: String,
        tracking_root_blob_name: Option<String>,
        routing_key: RoutingKey,
    ) -> Self {
        let creation_date = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            bucket_name,
            creation_date,
            authorized_keys: HashMap::new(),
            root_blob_name,
            tracking_root_blob_name,
            routing_key,
        }
    }
}
