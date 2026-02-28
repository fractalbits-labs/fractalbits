use crate::handler::{
    ObjectRequestContext,
    common::{
        buffer_payload,
        checksum::{ChecksumAlgorithm, ChecksumValue, Checksummer},
        extract_metadata_headers, gen_etag, get_raw_object, list_raw_objects, mpu_get_part_prefix,
        mpu_parse_part_number,
        response::xml::{Xml, XmlnsS3},
        s3_error::S3Error,
    },
    delete::delete_object_handler,
};
use actix_web::http::header::HeaderValue;
use actix_web::web::Bytes;
use base64::{Engine, prelude::BASE64_STANDARD};
use bytes::Buf;
use data_types::object_layout::{MpuState, ObjectCoreMetaData, ObjectState};
use nss_codec::put_inode_response;
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_common::nss_rpc_retry;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct HeaderOpts<'a> {
    x_amz_checksum_crc32: Option<&'a HeaderValue>,
    x_amz_checksum_crc32c: Option<&'a HeaderValue>,
    x_amz_checksum_crc64nvme: Option<&'a HeaderValue>,
    x_amz_checksum_sha1: Option<&'a HeaderValue>,
    x_amz_checksum_sha256: Option<&'a HeaderValue>,
    x_amz_checksum_type: Option<&'a HeaderValue>,
    x_amz_mp_object_size: Option<&'a HeaderValue>,
    x_amz_checksum_mode_enabled: bool,
    x_amz_request_payer: Option<&'a HeaderValue>,
    x_amz_expected_bucket_owner: Option<&'a HeaderValue>,
    if_match: Option<&'a HeaderValue>,
    if_none_match: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_algorithm: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_key: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_key_md5: Option<&'a HeaderValue>,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CompleteMultipartUpload {
    part: Vec<Part>,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Part {
    #[serde(default)]
    checksum_crc32: String,
    #[serde(default)]
    checksum_crc32c: String,
    #[serde(default)]
    checksum_sha1: String,
    #[serde(default)]
    checksum_sha256: String,
    #[serde(rename = "ETag")]
    etag: String,
    part_number: u32,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CompleteMultipartUploadResult {
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
    location: String,
    bucket: String,
    key: String,
    #[serde(rename = "ETag", skip_serializing_if = "Option::is_none")]
    etag: Option<String>,

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
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum_type: Option<String>,
}

impl CompleteMultipartUploadResult {
    fn bucket(self, bucket: String) -> Self {
        Self { bucket, ..self }
    }

    fn key(self, key: String) -> Self {
        Self { key, ..self }
    }

    fn etag(self, etag: String) -> Self {
        Self {
            etag: Some(etag),
            ..self
        }
    }

    fn checksum(self, checksum: Option<ChecksumValue>) -> Self {
        match checksum {
            Some(ChecksumValue::Crc32(crc32)) => Self {
                checksum_crc32: Some(BASE64_STANDARD.encode(crc32)),
                checksum_type: Some("COMPOSITE".to_string()),
                ..self
            },
            Some(ChecksumValue::Crc32c(crc32c)) => Self {
                checksum_crc32c: Some(BASE64_STANDARD.encode(crc32c)),
                checksum_type: Some("COMPOSITE".to_string()),
                ..self
            },
            Some(ChecksumValue::Sha1(sha1)) => Self {
                checksum_sha1: Some(BASE64_STANDARD.encode(sha1)),
                checksum_type: Some("COMPOSITE".to_string()),
                ..self
            },
            Some(ChecksumValue::Sha256(sha256)) => Self {
                checksum_sha256: Some(BASE64_STANDARD.encode(sha256)),
                checksum_type: Some("COMPOSITE".to_string()),
                ..self
            },
            Some(ChecksumValue::Crc64Nvme(crc64nvme)) => Self {
                checksum_crc64nvme: Some(BASE64_STANDARD.encode(crc64nvme)),
                checksum_type: Some("COMPOSITE".to_string()),
                ..self
            },
            None => self,
        }
    }
}

#[derive(Default)]
pub(crate) struct MpuChecksummer {
    // We only support composite checksum for speed reason
    pub composite_checksum: Option<Checksummer>,
}

impl MpuChecksummer {
    pub(crate) fn init(algo: Option<ChecksumAlgorithm>) -> Self {
        Self {
            composite_checksum: algo.map(Checksummer::new),
        }
    }

    pub(crate) fn update(&mut self, checksum: Option<ChecksumValue>) -> Result<(), S3Error> {
        if let (Some(checksummer), Some(checksum_value)) = (&mut self.composite_checksum, checksum)
        {
            checksummer.update(checksum_value.as_bytes());
        }
        Ok(())
    }

    pub(crate) fn finalize(self) -> Option<ChecksumValue> {
        self.composite_checksum
            .map(|checksummer| checksummer.finalize())
    }
}

pub async fn complete_multipart_upload_handler(
    ctx: ObjectRequestContext,
    upload_id: String,
) -> Result<actix_web::HttpResponse, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let _headers = extract_metadata_headers(ctx.request.headers())?;
    let _expected_checksum = ctx.checksum_value;

    // Extract body from payload
    let chunks = buffer_payload(ctx.payload).await?;
    let body = crate::handler::common::merge_chunks(chunks);

    // Parse the request body to get the parts list
    let req_body: CompleteMultipartUpload = quick_xml::de::from_reader(body.reader())?;
    let mut valid_part_numbers: HashSet<u32> =
        req_body.part.iter().map(|part| part.part_number).collect();

    let mut object =
        get_raw_object(&ctx.app, &bucket.root_blob_name, &ctx.key, &ctx.trace_id).await?;
    if object.version_id.simple().to_string() != upload_id {
        return Err(S3Error::NoSuchVersion);
    }
    if ObjectState::Mpu(MpuState::Uploading) != object.state {
        return Err(S3Error::InvalidObjectState);
    }

    let max_parts = 10000;
    let mpu_prefix = mpu_get_part_prefix(ctx.key.clone(), 0);
    let mpu_objs = list_raw_objects(
        &ctx.app,
        &bucket.root_blob_name,
        max_parts,
        &mpu_prefix,
        "",
        "",
        false,
        &ctx.trace_id,
    )
    .await?;

    // Extract headers from request for metadata
    let headers = crate::handler::common::extract_metadata_headers(ctx.request.headers())?;

    // Extract expected checksum from headers
    let expected_checksum = ctx.checksum_value;

    // Use MpuChecksummer like the original implementation
    let mut total_size = 0;
    let mut invalid_part_keys = HashSet::new();
    let mut checksummer = MpuChecksummer::init(expected_checksum.map(|x| x.algorithm()));

    tracing::info!("Found {} mpu objects", mpu_objs.len());
    for (mut mpu_key, mpu_obj) in mpu_objs {
        assert_eq!(Some('\0'), mpu_key.pop());
        let part_number = mpu_parse_part_number(&mpu_key)?;
        tracing::info!(
            "Processing part {} with size {}",
            part_number,
            mpu_obj.size().unwrap_or(0)
        );
        if !valid_part_numbers.remove(&part_number) {
            invalid_part_keys.insert(mpu_key.clone());
            tracing::info!("Part {} is invalid", part_number);
        } else {
            checksummer.update(mpu_obj.checksum()?)?;
            let part_size = mpu_obj.size()?;
            total_size += part_size;
            tracing::info!(
                "Added part {} size {} to total, new total: {}",
                part_number,
                part_size,
                total_size
            );
        }
    }
    tracing::info!("Final total_size: {}", total_size);

    let checksum = checksummer.finalize();
    tracing::info!(
        "Computed checksum: {:?}, Expected checksum: {:?}",
        checksum,
        expected_checksum
    );
    if expected_checksum.is_some() && checksum != expected_checksum {
        return Err(S3Error::InvalidDigest);
    }

    if !valid_part_numbers.is_empty() {
        return Err(S3Error::InvalidPart);
    }
    // Delete invalid parts that weren't included in the completed multipart upload
    for invalid_key in invalid_part_keys {
        tracing::info!("Deleting invalid part: {}", invalid_key);
        let delete_ctx = ObjectRequestContext::new(
            ctx.app.clone(),
            ctx.request.clone(),
            None,
            ctx.bucket_name.clone(),
            invalid_key,
            None, // No checksum value needed for delete
            actix_web::dev::Payload::None,
            ctx.trace_id,
        );
        delete_object_handler(delete_ctx).await?;
    }

    let etag = gen_etag();
    object.state = ObjectState::Mpu(MpuState::Completed(ObjectCoreMetaData {
        size: total_size,
        etag: etag.clone(),
        headers,
        checksum: expected_checksum,
    }));
    let new_object_bytes: Bytes = to_bytes_in::<_, Error>(&object, Vec::new())?.into();
    let nss_client = ctx.app.get_nss_rpc_client().await?;
    let resp = nss_rpc_retry!(
        nss_client,
        put_inode(
            &bucket.root_blob_name,
            &ctx.key,
            new_object_bytes.clone(),
            Some(ctx.app.config.rpc_request_timeout()),
            &ctx.trace_id
        ),
        ctx.app,
        &ctx.trace_id
    )
    .await?;
    match resp.result.unwrap() {
        put_inode_response::Result::Ok(_) => {}
        put_inode_response::Result::Err(e) => {
            tracing::error!("put_inode error: {}", e);
            return Err(S3Error::InternalError);
        }
    };

    let resp = CompleteMultipartUploadResult::default()
        .bucket(bucket.bucket_name.clone())
        .key(ctx.key)
        .etag(object.etag()?)
        .checksum(object.checksum()?);

    Xml(resp).try_into()
}
