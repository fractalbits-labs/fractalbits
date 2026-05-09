use actix_web::HttpResponse;

use super::resolve_bucket_no_cache;
use crate::handler::{BucketRequestContext, common::s3_error::S3Error};
use tracing::error;

pub async fn head_bucket_handler(ctx: BucketRequestContext) -> Result<HttpResponse, S3Error> {
    match ctx.api_key.data.authorized_buckets.get(&ctx.bucket_name) {
        None => {
            error!(
                "bucket {} is not associated with api_key: {}",
                ctx.bucket_name, ctx.api_key.data.key_id
            );
            return Err(S3Error::InvalidAccessKeyId);
        }
        Some(bucket_key_perm) => {
            if !bucket_key_perm.allow_read {
                error!(
                    "bucket {} is not associated with api_key: {}",
                    ctx.bucket_name, ctx.api_key.data.key_id
                );
                return Err(S3Error::AccessDenied);
            }
        }
    }

    // HEAD bucket is a metadata-only existence check; serving it from a
    // possibly-stale local cache could falsely report "exists" for a bucket
    // that was just deleted on another api_server. Always re-validate
    // against RSS.
    resolve_bucket_no_cache(&ctx.app, &ctx.bucket_name, &ctx.trace_id)
        .await
        .map_err(|e| {
            error!("head_bucket failed due to bucket resolving: {e}");
            e
        })?;
    Ok(HttpResponse::Ok().finish())
}
