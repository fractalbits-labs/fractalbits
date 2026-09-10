use actix_web::HttpResponse;
use rpc_client_common::RpcError;
use tracing::info;

use crate::handler::{BucketRequestContext, common::s3_error::S3Error};

pub async fn delete_bucket_handler(ctx: BucketRequestContext) -> Result<HttpResponse, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    info!("handling delete_bucket request: {}", bucket.bucket_name);

    let api_key_id = {
        if !ctx
            .api_key
            .data
            .authorized_buckets
            .contains_key(&bucket.bucket_name)
        {
            return Err(S3Error::AccessDenied);
        }
        ctx.api_key.data.key_id.clone()
    };

    let result = ctx
        .app
        .delete_bucket(&bucket.bucket_name, &api_key_id, ctx.trace_id)
        .await;
    match result {
        Ok(_) => {
            info!("Successfully deleted bucket: {}", bucket.bucket_name);
            Ok(HttpResponse::NoContent().finish())
        }
        Err(e) => {
            tracing::error!("Failed to delete bucket {}: {}", bucket.bucket_name, e);
            match e {
                RpcError::InternalResponseError(msg) => {
                    if msg.contains("not empty") || msg.contains("not found") {
                        if msg.contains("not empty") {
                            Err(S3Error::BucketNotEmpty)
                        } else {
                            Err(S3Error::NoSuchBucket)
                        }
                    } else {
                        Err(S3Error::InternalError)
                    }
                }
                _ => Err(e.into()),
            }
        }
    }
}
