use crate::handler::ObjectRequestContext;
use crate::handler::common::{mpu_get_part_prefix, s3_error::S3Error};
use crate::handler::put::put_object_handler;
use ntex::web::HttpResponse;
use serde::Serialize;

#[allow(dead_code)]
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct ResponseHeaders {
    x_amz_server_side_encryption: String,
    #[serde(rename = "ETag")]
    etag: String,
    x_amz_checksum_crc32: String,
    x_amz_checksum_crc32c: String,
    x_amz_checksum_sha1: String,
    x_amz_checksum_sha256: String,
    x_amz_server_side_encryption_customer_algorithm: String,
    #[serde(rename = "x-amz-server-side-encryption-customer-key-MD5")]
    x_amz_server_side_encryption_customer_key_md5: String,
    x_amz_server_side_encryption_aws_kms_key_id: String,
    x_amz_server_side_encryption_bucket_key_enabled: String,
    x_amz_request_charged: String,
}

pub async fn upload_part_handler(
    ctx: ObjectRequestContext,
    part_number: u64,
    upload_id: String,
) -> Result<HttpResponse, S3Error> {
    if !(1..=10_000).contains(&part_number) {
        return Err(S3Error::InvalidPart);
    }

    // Basic upload_id validation - check it's a valid UUID format
    if uuid::Uuid::parse_str(&upload_id).is_err() {
        return Err(S3Error::NoSuchUpload);
    }

    let part_key = mpu_get_part_prefix(ctx.key.clone(), part_number);
    tracing::debug!(
        "Upload part {} for upload {} to {}/{}",
        part_number,
        upload_id,
        ctx.bucket_name,
        part_key
    );

    tracing::debug!("Upload part request headers:");
    for (name, value) in ctx.request.headers().iter() {
        tracing::debug!("  {}: {:?}", name, value);
    }

    // Create a new context for the part object
    let part_ctx = ObjectRequestContext::new(
        ctx.app,
        ctx.request,
        ctx.api_key,
        ctx.bucket_name,
        part_key,
        ctx.checksum_value, // Pass through the original checksum value
        ctx.payload,
        ctx.trace_id,
    );

    // Call put_object_handler to store the part
    let _response = put_object_handler(part_ctx).await?;

    let etag = format!("{upload_id}{part_number}");
    Ok(HttpResponse::Ok().set_header("ETag", etag).finish())
}
