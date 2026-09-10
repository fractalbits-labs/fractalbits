mod create_bucket;
mod delete_bucket;
mod head_bucket;
mod list_buckets;

pub use create_bucket::create_bucket_handler;
pub use delete_bucket::delete_bucket_handler;
pub use head_bucket::head_bucket_handler;
pub use list_buckets::list_buckets_handler;

use super::common::{authorization::Authorization, s3_error::S3Error};
use crate::AppState;
use data_types::{Bucket, TraceId};
use metrics_wrapper::histogram;
use rpc_client_common::RpcError;
use std::time::Instant;

pub async fn resolve_bucket(
    app: &AppState,
    bucket_name: &str,
    trace_id: &TraceId,
) -> Result<Bucket, S3Error> {
    let start = Instant::now();
    match app.get_bucket(bucket_name, trace_id).await {
        Ok(bucket) => {
            let duration = start.elapsed();
            histogram!("resolve_bucket_nanos", "status" => "Ok").record(duration.as_nanos() as f64);
            Ok(bucket.data)
        }
        Err(RpcError::NotFound) => {
            let duration = start.elapsed();
            histogram!("resolve_bucket_nanos", "status" => "Fail_NotFound")
                .record(duration.as_nanos() as f64);
            Err(S3Error::NoSuchBucket)
        }
        Err(e) => {
            let duration = start.elapsed();
            histogram!("resolve_bucket_nanos", "status" => "Fail_Others")
                .record(duration.as_nanos() as f64);
            Err(e.into())
        }
    }
}

/// Resolve a bucket directly from RSS, bypassing the local cache. Used by
/// metadata-only endpoints (HEAD bucket, etc.) where serving stale
/// "exists / does not exist" answers is worse than the extra RSS RPC.
pub async fn resolve_bucket_no_cache(
    app: &AppState,
    bucket_name: &str,
    trace_id: &TraceId,
) -> Result<Bucket, S3Error> {
    let start = Instant::now();
    match app.fetch_bucket_no_cache(bucket_name, trace_id).await {
        Ok(bucket) => {
            let duration = start.elapsed();
            histogram!("resolve_bucket_no_cache_nanos", "status" => "Ok")
                .record(duration.as_nanos() as f64);
            Ok(bucket.data)
        }
        Err(RpcError::NotFound) => {
            let duration = start.elapsed();
            histogram!("resolve_bucket_no_cache_nanos", "status" => "Fail_NotFound")
                .record(duration.as_nanos() as f64);
            Err(S3Error::NoSuchBucket)
        }
        Err(e) => {
            let duration = start.elapsed();
            histogram!("resolve_bucket_no_cache_nanos", "status" => "Fail_Others")
                .record(duration.as_nanos() as f64);
            Err(e.into())
        }
    }
}

pub enum BucketEndpoint {
    CreateBucket,
    DeleteBucket,
    HeadBucket,
    ListBuckets,
}

impl BucketEndpoint {
    pub fn authorization_type(&self) -> Authorization {
        match self {
            BucketEndpoint::CreateBucket => Authorization::None,
            BucketEndpoint::DeleteBucket => Authorization::Owner,
            BucketEndpoint::HeadBucket => Authorization::Read,
            BucketEndpoint::ListBuckets => Authorization::None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            BucketEndpoint::CreateBucket => "CreateBucket",
            BucketEndpoint::DeleteBucket => "DeleteBucket",
            BucketEndpoint::HeadBucket => "HeadBucket",
            BucketEndpoint::ListBuckets => "ListBuckets",
        }
    }
}
