use std::sync::Arc;

use crate::AppState;
use crate::handler::ObjectRequestContext;
use crate::handler::common::mpu_parse_part_number;
use crate::handler::common::{
    checksum::ChecksumValue,
    get_raw_object, list_raw_objects, mpu_get_part_prefix,
    response::xml::{Xml, XmlnsS3},
    s3_error::S3Error,
    time,
};
use actix_web::web::Query;
use base64::prelude::*;
use data_types::object_layout::{MpuState, ObjectState};
use data_types::{Bucket, TraceId};
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
struct QueryOpts {
    max_parts: Option<u32>,
    part_number_marker: Option<u32>,
    #[serde(rename = "uploadId")]
    upload_id: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListPartsResult {
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
    bucket: String,
    key: String,
    upload_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    part_number_marker: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_part_number_marker: Option<u32>,
    max_parts: u32,
    is_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    part: Option<Vec<Part>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    initiator: Option<Initiator>,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<Owner>,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum_algorithm: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum_type: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Part {
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
    #[serde(rename = "ETag")]
    etag: String,
    last_modified: String, // timestamp
    part_number: u32,
    size: u64,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Initiator {
    display_name: String,
    #[serde(rename = "ID")]
    id: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Owner {
    display_name: String,
    #[serde(rename = "ID")]
    id: String,
}

pub async fn list_parts_handler(
    ctx: ObjectRequestContext,
) -> Result<actix_web::HttpResponse, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let query_opts = Query::<QueryOpts>::from_query(ctx.request.query_string())
        .unwrap_or_else(|_| Query(Default::default()))
        .into_inner();

    let max_parts = query_opts.max_parts.unwrap_or(1000);
    let object = get_raw_object(&ctx.app, &bucket.root_blob_name, &ctx.key, &ctx.trace_id).await?;
    if object.version_id.simple().to_string() != query_opts.upload_id {
        return Err(S3Error::NoSuchUpload);
    }
    if ObjectState::Mpu(MpuState::Uploading) != object.state {
        return Err(S3Error::InvalidObjectState);
    }

    let (parts, next_part_number_marker) = fetch_mpu_parts(
        ctx.app,
        &bucket,
        ctx.key.clone(),
        &query_opts,
        max_parts,
        &ctx.trace_id,
    )
    .await?;

    let resp = ListPartsResult {
        bucket: bucket.bucket_name.to_string(),
        key: ctx
            .key
            .strip_prefix("/")
            .ok_or(S3Error::InternalError)?
            .to_owned(),
        part_number_marker: query_opts.part_number_marker,
        max_parts,
        next_part_number_marker,
        is_truncated: next_part_number_marker.is_some(),
        upload_id: query_opts.upload_id,
        part: if parts.is_empty() { None } else { Some(parts) },
        ..Default::default()
    };

    Xml(resp).try_into()
}

async fn fetch_mpu_parts(
    app: Arc<AppState>,
    bucket: &Bucket,
    key: String,
    query_opts: &QueryOpts,
    max_parts: u32,
    trace_id: &TraceId,
) -> Result<(Vec<Part>, Option<u32>), S3Error> {
    let mpu_prefix = mpu_get_part_prefix(key.clone(), 0);
    let mpus = list_raw_objects(
        &app,
        &bucket.root_blob_name,
        10000, // TODO: use max_parts and retry if there are not enough valid
        &mpu_prefix,
        "",
        "",
        false,
        trace_id,
    )
    .await?;
    let mut parts = Vec::with_capacity(mpus.len());
    tracing::info!(
        "list_raw_objects returned {} objects for prefix {}",
        mpus.len(),
        mpu_prefix
    );
    for (mut mpu_key, mpu) in mpus {
        if let (Ok(etag), Ok(size)) = (mpu.etag(), mpu.size()) {
            assert_eq!(Some('\0'), mpu_key.pop());
            let last_modified = time::format_timestamp(mpu.timestamp);
            let part_number = mpu_parse_part_number(&mpu_key)?;
            let mut part = Part {
                last_modified,
                etag,
                size,
                part_number,
                ..Default::default()
            };
            match mpu.checksum()? {
                Some(ChecksumValue::Crc32(x)) => {
                    part.checksum_crc32 = Some(BASE64_STANDARD.encode(x))
                }
                Some(ChecksumValue::Crc32c(x)) => {
                    part.checksum_crc32c = Some(BASE64_STANDARD.encode(x))
                }
                Some(ChecksumValue::Crc64Nvme(x)) => {
                    part.checksum_crc64nvme = Some(BASE64_STANDARD.encode(x))
                }
                Some(ChecksumValue::Sha1(x)) => {
                    part.checksum_sha1 = Some(BASE64_STANDARD.encode(x))
                }
                Some(ChecksumValue::Sha256(x)) => {
                    part.checksum_sha256 = Some(BASE64_STANDARD.encode(x))
                }
                None => {}
            }
            parts.push(part);
        }
    }

    // Cut the beginning if we have a marker
    if let Some(marker) = query_opts.part_number_marker {
        let next = marker + 1;
        let part_idx = parts
            .binary_search_by(|part| part.part_number.cmp(&next))
            .unwrap_or_else(|x| x);
        parts = parts.split_off(part_idx);
    }

    // Cut the end if we have too many parts
    if parts.len() > max_parts as usize {
        parts.truncate(max_parts as usize);
        let pagination = Some(parts.last().unwrap().part_number);
        return Ok((parts, pagination));
    }

    Ok((parts, None))
}
