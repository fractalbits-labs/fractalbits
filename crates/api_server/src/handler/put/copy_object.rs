use std::sync::Arc;

use crate::{
    AppState,
    handler::{
        ObjectRequestContext, bucket,
        common::{
            checksum::ChecksumValue,
            get_raw_object,
            request::extract::BucketAndKeyName,
            response::xml::{Xml, XmlnsS3},
            s3_error::S3Error,
            time, xheader,
        },
        get::get_object_content_as_bytes,
        put::put_object_handler,
    },
};
use actix_web::http::header::{self, HeaderMap, HeaderValue};
use base64::{Engine, prelude::BASE64_STANDARD};
use bytes::Bytes;
use data_types::object_layout::*;
use data_types::{ApiKey, TraceId, Versioned};
use serde::Serialize;

#[allow(dead_code)]
#[derive(Debug, Default)]
struct HeaderOpts<'a> {
    x_amz_acl: Option<&'a HeaderValue>,
    cache_control: Option<&'a HeaderValue>,
    x_amz_checksum_algorithm: Option<&'a HeaderValue>,
    content_disposition: Option<&'a HeaderValue>,
    content_encoding: Option<&'a HeaderValue>,
    content_language: Option<&'a HeaderValue>,
    content_type: Option<&'a HeaderValue>,
    x_amz_copy_source: String, // required
    x_amz_copy_source_if_match: Option<&'a HeaderValue>,
    x_amz_copy_source_if_modified_since: Option<&'a HeaderValue>,
    x_amz_copy_source_if_none_match: Option<&'a HeaderValue>,
    x_amz_copy_source_if_unmodified_since: Option<&'a HeaderValue>,
    expires: Option<&'a HeaderValue>,
    x_amz_grant_full_control: Option<&'a HeaderValue>,
    x_amz_grant_read: Option<&'a HeaderValue>,
    x_amz_grant_read_acp: Option<&'a HeaderValue>,
    x_amz_grant_write_acp: Option<&'a HeaderValue>,
    x_amz_metadata_directive: Option<&'a HeaderValue>,
    x_amz_tagging_directive: Option<&'a HeaderValue>,
    x_amz_server_side_encryption: Option<&'a HeaderValue>,
    x_amz_storage_class: Option<&'a HeaderValue>,
    x_amz_website_redirect_location: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_algorithm: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_key: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_key_md5: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_aws_kms_key_id: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_context: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_bucket_key_enabled: Option<&'a HeaderValue>,
    x_amz_copy_source_server_side_encryption_customer_algorithm: Option<&'a HeaderValue>,
    x_amz_copy_source_server_side_encryption_customer_key: Option<&'a HeaderValue>,
    x_amz_copy_source_server_side_encryption_customer_key_md5: Option<&'a HeaderValue>,
    x_amz_request_payer: Option<&'a HeaderValue>,
    x_amz_tagging: Option<&'a HeaderValue>,
    x_amz_object_lock_mode: Option<&'a HeaderValue>,
    x_amz_object_lock_retain_until_date: Option<&'a HeaderValue>,
    x_amz_object_lock_legal_hold: Option<&'a HeaderValue>,
    x_amz_expected_bucket_owner: Option<&'a HeaderValue>,
    x_amz_source_expected_bucket_owner: Option<&'a HeaderValue>,
}

impl<'a> HeaderOpts<'a> {
    fn from_headers(headers: &'a HeaderMap) -> Result<Self, S3Error> {
        Ok(Self {
            x_amz_acl: headers.get(xheader::X_AMZ_ACL),
            cache_control: headers.get(header::CACHE_CONTROL),
            x_amz_checksum_algorithm: headers.get(xheader::X_AMZ_CHECKSUM_ALGORITHM),
            content_disposition: headers.get(header::CONTENT_DISPOSITION),
            content_encoding: headers.get(header::CONTENT_ENCODING),
            content_language: headers.get(header::CONTENT_LANGUAGE),
            content_type: headers.get(header::CONTENT_TYPE),
            x_amz_copy_source: headers
                .get(xheader::X_AMZ_COPY_SOURCE)
                .ok_or(S3Error::InvalidArgument2)?
                .to_str()?
                .to_owned(),
            x_amz_copy_source_if_match: headers.get(xheader::X_AMZ_COPY_SOURCE_IF_MATCH),
            x_amz_copy_source_if_modified_since: headers
                .get(xheader::X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE),
            x_amz_copy_source_if_none_match: headers.get(xheader::X_AMZ_COPY_SOURCE_IF_NONE_MATCH),
            x_amz_copy_source_if_unmodified_since: headers
                .get(xheader::X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE),
            expires: headers.get(header::EXPIRES),
            x_amz_grant_full_control: headers.get(xheader::X_AMZ_GRANT_FULL_CONTROL),
            x_amz_grant_read: headers.get(xheader::X_AMZ_GRANT_READ),
            x_amz_grant_read_acp: headers.get(xheader::X_AMZ_GRANT_READ_ACP),
            x_amz_grant_write_acp: headers.get(xheader::X_AMZ_GRANT_WRITE_ACP),
            x_amz_metadata_directive: headers.get(xheader::X_AMZ_METADATA_DIRECTIVE),
            x_amz_tagging_directive: headers.get(xheader::X_AMZ_TAGGING_DIRECTIVE),
            x_amz_server_side_encryption: headers.get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION),
            x_amz_storage_class: headers.get(xheader::X_AMZ_STORAGE_CLASS),
            x_amz_website_redirect_location: headers.get(xheader::X_AMZ_WEBSITE_REDIRECT_LOCATION),
            x_amz_server_side_encryption_customer_algorithm: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM),
            x_amz_server_side_encryption_customer_key: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY),
            x_amz_server_side_encryption_customer_key_md5: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5),
            x_amz_server_side_encryption_aws_kms_key_id: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID),
            x_amz_server_side_encryption_context: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_CONTEXT),
            x_amz_server_side_encryption_bucket_key_enabled: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_BUCKET_KEY_ENABLED),
            x_amz_copy_source_server_side_encryption_customer_algorithm: headers
                .get(xheader::X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM),
            x_amz_copy_source_server_side_encryption_customer_key: headers
                .get(xheader::X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY),
            x_amz_copy_source_server_side_encryption_customer_key_md5: headers
                .get(xheader::X_AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5),
            x_amz_request_payer: headers.get(xheader::X_AMZ_REQUEST_PAYER),
            x_amz_tagging: headers.get(xheader::X_AMZ_TAGGING),
            x_amz_object_lock_mode: headers.get(xheader::X_AMZ_STORAGE_OBJECT_LOCK_MODE),
            x_amz_object_lock_retain_until_date: headers
                .get(xheader::X_AMZ_STORAGE_OBJECT_LOCK_RETAIN_UNTIL_DATE),
            x_amz_object_lock_legal_hold: headers
                .get(xheader::X_AMZ_STORAGE_OBJECT_LOCK_LEGAL_HOLD),
            x_amz_expected_bucket_owner: headers.get(xheader::X_AMZ_EXPECTED_BUCKET_OWNER),
            x_amz_source_expected_bucket_owner: headers
                .get(xheader::X_AMZ_STORAGE_SOURCE_EXPECTED_BUCKET_OWNER),
        })
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CopyObjectResult {
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
    #[serde(rename = "ETag")]
    etag: String,
    last_modified: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum_type: Option<String>,
    #[serde(rename = "ChecksumCRC32", skip_serializing_if = "Option::is_none")]
    checksum_crc32: Option<String>,
    #[serde(rename = "ChecksumCRC32C", skip_serializing_if = "Option::is_none")]
    checksum_crc32c: Option<String>,
    #[serde(rename = "ChecksumCRC64NVME", skip_serializing_if = "Option::is_none")]
    checksum_crc64nvme: Option<String>,
    #[serde(rename = "ChecksumSHA1", skip_serializing_if = "Option::is_none")]
    checksum_sha1: Option<String>,
    #[serde(rename = "ChecksumSHA256", skip_serializing_if = "Option::is_none")]
    checksum_sha256: Option<String>,
}

impl CopyObjectResult {
    fn etag(self, etag: String) -> Self {
        Self { etag, ..self }
    }

    fn last_modified(self, last_modified: String) -> Self {
        Self {
            last_modified,
            ..self
        }
    }

    fn checksum(mut self, checksum: Option<ChecksumValue>) -> Self {
        match checksum {
            Some(ChecksumValue::Crc32(crc32)) => {
                self.checksum_crc32 = Some(BASE64_STANDARD.encode(crc32));
                self.checksum_type = Some("CRC32".to_string());
            }
            Some(ChecksumValue::Crc32c(crc32c)) => {
                self.checksum_crc32c = Some(BASE64_STANDARD.encode(crc32c));
                self.checksum_type = Some("CRC32C".to_string());
            }
            Some(ChecksumValue::Sha1(sha1)) => {
                self.checksum_sha1 = Some(BASE64_STANDARD.encode(sha1));
                self.checksum_type = Some("SHA1".to_string());
            }
            Some(ChecksumValue::Sha256(sha256)) => {
                self.checksum_sha256 = Some(BASE64_STANDARD.encode(sha256));
                self.checksum_type = Some("SHA256".to_string());
            }
            Some(ChecksumValue::Crc64Nvme(crc64nvme)) => {
                self.checksum_crc64nvme = Some(BASE64_STANDARD.encode(crc64nvme));
                self.checksum_type = Some("CRC64NVME".to_string());
            }
            None => {}
        }
        self
    }
}

pub async fn copy_object_handler(
    ctx: ObjectRequestContext,
) -> Result<actix_web::HttpResponse, S3Error> {
    let _bucket = ctx.resolve_bucket().await?;
    let api_key = ctx.api_key.ok_or(S3Error::InternalError)?;
    let header_opts = HeaderOpts::from_headers(ctx.request.headers())?;
    // We need to get the source object first. Since Versioned doesn't implement Clone,
    // we'll create a simple ApiKey for the source operation and reuse the original for put
    let source_api_key = Versioned::new(
        0,
        ApiKey {
            key_id: api_key.data.key_id.clone(),
            secret_key: api_key.data.secret_key.clone(),
            name: api_key.data.name.clone(),
            allow_create_bucket: api_key.data.allow_create_bucket,
            authorized_buckets: api_key.data.authorized_buckets.clone(),
            is_deleted: api_key.data.is_deleted,
        },
    );
    let (source_obj, source_body) = get_copy_source_object(
        ctx.app.clone(),
        &source_api_key,
        &header_opts.x_amz_copy_source,
        ctx.trace_id,
    )
    .await?;

    tracing::info!(
        "Copy object request: bucket={}, key={}, source={}",
        ctx.bucket_name,
        ctx.key,
        header_opts.x_amz_copy_source
    );

    // Convert the source body to bytes
    let source_body_bytes = actix_web::body::to_bytes(source_body)
        .await
        .map_err(|_| S3Error::InternalError)?;
    let actix_body_bytes = source_body_bytes;

    // Use the existing put_object handler to store the copied object
    let new_ctx = ObjectRequestContext::new(
        ctx.app,
        ctx.request,
        Some(api_key),
        ctx.bucket_name,
        ctx.key,
        ctx.checksum_value, // Pass through the original checksum value
        actix_web::dev::Payload::from(actix_body_bytes),
        ctx.trace_id,
    );
    let _put_response = put_object_handler(new_ctx).await?;

    Xml(CopyObjectResult::default()
        .etag(source_obj.etag()?)
        .last_modified(time::format_http_date(source_obj.timestamp))
        .checksum(source_obj.checksum()?))
    .try_into()
}

async fn get_copy_source_object(
    app: Arc<AppState>,
    api_key: &Versioned<ApiKey>,
    copy_source: &str,
    trace_id: TraceId,
) -> Result<(ObjectLayout, Bytes), S3Error> {
    let copy_source = percent_encoding::percent_decode_str(copy_source).decode_utf8()?;

    let (source_bucket_name, source_key) =
        BucketAndKeyName::get_bucket_and_key_from_path(&copy_source);

    if !api_key.data.allow_read(&source_bucket_name) {
        return Err(S3Error::AccessDenied);
    }

    let source_bucket = bucket::resolve_bucket(&app, &source_bucket_name, &trace_id).await?;
    let source_obj =
        get_raw_object(&app, &source_bucket.root_blob_name, &source_key, &trace_id).await?;
    let (source_obj_content, _) = get_object_content_as_bytes(
        app,
        &source_bucket,
        &source_obj,
        source_key,
        None,
        &trace_id,
    )
    .await?;
    Ok((source_obj, source_obj_content))
}
