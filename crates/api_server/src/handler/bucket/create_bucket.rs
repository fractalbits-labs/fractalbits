use bytes::Buf;
use ntex::web::HttpResponse;
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
        if ctx
            .api_key
            .data
            .authorized_buckets
            .contains_key(&ctx.bucket_name)
        {
            return Err(S3Error::BucketAlreadyOwnedByYou);
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

    // Determine if we're in multi-AZ mode based on the blob storage backend
    let is_multi_az = matches!(
        ctx.app.config.blob_storage.backend,
        crate::BlobStorageBackend::S3ExpressMultiAz
    );

    let result = ctx
        .app
        .create_bucket(&ctx.bucket_name, &api_key_id, is_multi_az, ctx.trace_id)
        .await;
    match result {
        Ok(_) => {
            info!("Successfully created bucket: {}", ctx.bucket_name);
            Ok(HttpResponse::Ok()
                .set_header("location", format!("/{}", ctx.bucket_name))
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

// Check if a bucket name is valid.
//
// The requirements are listed here:
// <https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html>
fn is_valid_bucket_name(n: &str) -> bool {
    // Bucket names must be between 3 and 63 characters
    n.len() >= 3 && n.len() <= 63
	// Bucket names must be composed of lowercase letters, numbers,
	// dashes and dots
	&& n.chars().all(|c| matches!(c, '.' | '-' | 'a'..='z' | '0'..='9'))
	//  Bucket names must start and end with a letter or a number
	&& !n.starts_with(&['-', '.'][..])
	&& !n.ends_with(&['-', '.'][..])
	// Bucket names must not be formatted as an IP address
	&& n.parse::<std::net::IpAddr>().is_err()
	// Bucket names must not start with "xn--"
	&& !n.starts_with("xn--")
	&& !n.contains(".xn--")
	// Bucket names must not end with "-s3alias"
	&& !n.ends_with("-s3alias")
}
