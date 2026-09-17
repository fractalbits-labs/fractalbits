use actix_web::HttpResponse;
use bytes::Buf;
use data_types::drive::is_valid_bucket_name;
use rpc_client_common::RpcError;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::handler::{
    BucketRequestContext,
    common::{buffer_payload, s3_error::S3Error},
};

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CreateBucketConfiguration {
    #[serde(default)]
    location_constraint: String,
    #[serde(default)]
    location: Location,
    #[serde(default)]
    bucket: BucketConfig,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Location {
    name: String,
    #[serde(rename = "Type")]
    location_type: String,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct BucketConfig {
    data_redundancy: String,
    #[serde(rename = "Type")]
    bucket_type: String,
}

pub async fn create_bucket_handler(ctx: BucketRequestContext) -> Result<HttpResponse, S3Error> {
    info!("handling create_bucket request: {}", ctx.bucket_name);

    // Validate permissions and bucket name
    let api_key_id = {
        // The cached key may still list a bucket that another gateway or the
        // management API deleted since; confirm against RSS before answering
        // "already owned", or the create would be skipped for a bucket that
        // no longer exists.
        if ctx
            .api_key
            .data
            .authorized_buckets
            .contains_key(&ctx.bucket_name)
        {
            let fresh = ctx
                .app
                .refresh_api_key(ctx.api_key.data.key_id.clone(), &ctx.trace_id)
                .await?;
            if fresh.data.authorized_buckets.contains_key(&ctx.bucket_name) {
                return Err(S3Error::BucketAlreadyOwnedByYou);
            }
        }
        if !ctx.api_key.data.allow_create_bucket {
            return Err(S3Error::AccessDenied);
        }
        ctx.api_key.data.key_id.clone()
    };

    if !is_valid_bucket_name(&ctx.bucket_name) {
        return Err(S3Error::InvalidBucketName);
    }

    // Parse and validate the request body
    let chunks = buffer_payload(ctx.payload).await?;
    let body = crate::handler::common::merge_chunks(chunks);
    if !body.is_empty() {
        let create_bucket_conf: CreateBucketConfiguration =
            quick_xml::de::from_reader(body.reader())?;
        let location_constraint = create_bucket_conf.location_constraint;
        if !location_constraint.is_empty() && location_constraint != ctx.app.config.region {
            return Err(S3Error::InvalidLocationConstraint);
        }
    }

    let result = ctx
        .app
        .create_bucket(&ctx.bucket_name, &api_key_id, ctx.trace_id)
        .await;
    match result {
        Ok(_) => {
            info!("Successfully created bucket: {}", ctx.bucket_name);
            Ok(HttpResponse::Ok()
                .insert_header(("location", format!("/{}", ctx.bucket_name)))
                .finish())
        }
        Err(e) => {
            tracing::error!("Failed to create bucket {}: {}", ctx.bucket_name, e);
            match e {
                RpcError::AlreadyExists => Err(S3Error::BucketAlreadyExists),
                RpcError::BucketAlreadyOwnedByYou => Err(S3Error::BucketAlreadyOwnedByYou),
                RpcError::InternalResponseError(msg) if msg.contains("API key not found") => {
                    Err(S3Error::AccessDenied)
                }
                _ => Err(S3Error::InternalError),
            }
        }
    }
}
