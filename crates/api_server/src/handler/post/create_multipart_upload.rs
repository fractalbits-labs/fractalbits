use bytes::Bytes;
use rpc_client_common::nss_rpc_retry;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::handler::{
    ObjectRequestContext,
    common::{
        response::xml::{Xml, XmlnsS3},
        s3_error::S3Error,
    },
};
use crate::object_layout::{MpuState, ObjectLayout, ObjectState, gen_version_id};
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use serde::Serialize;

#[allow(dead_code)]
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct ResponseHeaders {
    x_amz_abort_date: String,
    x_amz_abort_rule_id: String,
    x_amz_server_side_encryption: String,
    x_amz_server_side_encryption_customer_algorithm: String,
    #[serde(rename = "x-amz-server-side-encryption-customer-key-MD5")]
    x_amz_server_side_encryption_customer_key_md5: String,
    x_amz_server_side_encryption_aws_kms_key_id: String,
    x_amz_server_side_encryption_context: String,
    x_amz_server_side_encryption_bucket_key_enabled: String,
    x_amz_request_charged: String,
    x_amz_checksum_algorithm: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct InitiateMultipartUploadResult {
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
    bucket: String,
    key: String,
    upload_id: String,
}

pub async fn create_multipart_upload_handler(
    ctx: ObjectRequestContext,
) -> Result<actix_web::HttpResponse, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let version_id = gen_version_id();
    let object_layout = ObjectLayout {
        version_id,
        block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
        timestamp,
        state: ObjectState::Mpu(MpuState::Uploading),
    };
    let object_layout_bytes: Bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new())?.into();
    let nss_client = ctx.app.get_nss_rpc_client();
    let _resp = nss_rpc_retry!(
        nss_client,
        put_inode(
            &bucket.root_blob_name,
            &ctx.key,
            object_layout_bytes.clone(),
            Some(ctx.app.config.rpc_request_timeout()),
            &ctx.trace_id
        )
    )
    .await?;

    let init_mpu_res = InitiateMultipartUploadResult {
        xmlns: Default::default(),
        bucket: bucket.bucket_name.clone(),
        key: ctx.key,
        upload_id: version_id.simple().to_string(),
    };

    Xml(init_mpu_res).try_into()
}
