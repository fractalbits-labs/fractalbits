use std::collections::HashSet;

use crate::handler::{
    ObjectRequestContext,
    common::{
        checksum::ChecksumValue,
        get_raw_object,
        response::xml::{Xml, XmlnsS3},
        s3_error::S3Error,
        time, xheader,
    },
};
use actix_web::{
    http::header::{HeaderMap, HeaderValue},
    web::Query,
};
use base64::{Engine, prelude::BASE64_STANDARD};
use serde::{Deserialize, Serialize};

#[allow(dead_code)]
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct QueryOpts {
    version_id: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Default)]
struct HeaderOpts<'a> {
    x_amz_max_parts: Option<&'a HeaderValue>,
    x_amz_part_number_marker: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_algorithm: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_key: Option<&'a HeaderValue>,
    x_amz_server_side_encryption_customer_key_md5: Option<&'a HeaderValue>,
    x_amz_request_payer: Option<&'a HeaderValue>,
    x_amz_expected_bucket_owner: Option<&'a HeaderValue>,
    x_amz_object_attributes: HashSet<String>, // required
}

impl<'a> HeaderOpts<'a> {
    fn from_headers(headers: &'a HeaderMap) -> Result<Self, S3Error> {
        Ok(Self {
            x_amz_max_parts: headers.get(xheader::X_AMZ_MAX_PARTS),
            x_amz_part_number_marker: headers.get(xheader::X_AMZ_PART_NUMBER_MARKER),
            x_amz_server_side_encryption_customer_algorithm: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM),
            x_amz_server_side_encryption_customer_key: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY),
            x_amz_server_side_encryption_customer_key_md5: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5),
            x_amz_request_payer: headers.get(xheader::X_AMZ_REQUEST_PAYER),
            x_amz_expected_bucket_owner: headers.get(xheader::X_AMZ_EXPECTED_BUCKET_OWNER),
            x_amz_object_attributes: headers
                .get(xheader::X_AMZ_OBJECT_ATTRIBUTES)
                .ok_or(S3Error::InvalidArgument2)?
                .to_str()?
                .split(',')
                .map(|x| x.to_lowercase())
                .collect(),
        })
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct GetObjectAttributesOutput {
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
    #[serde(rename = "ETag", skip_serializing_if = "Option::is_none")]
    etag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checksum: Option<Checksum>,
    #[serde(skip_serializing_if = "Option::is_none")]
    object_parts: Option<ObjectParts>,
    #[serde(skip_serializing_if = "Option::is_none")]
    storage_class: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    object_size: Option<usize>,
}

impl GetObjectAttributesOutput {
    fn etag(self, etag: String) -> Self {
        Self {
            etag: Some(etag),
            ..self
        }
    }

    fn object_size(self, object_size: usize) -> Self {
        Self {
            object_size: Some(object_size),
            ..self
        }
    }

    fn checksum(mut self, checksum: Option<ChecksumValue>) -> Self {
        match checksum {
            Some(ChecksumValue::Crc32(crc32)) => {
                let checksum = Checksum {
                    checksum_crc32: Some(BASE64_STANDARD.encode(crc32)),
                    checksum_type: "FULL_OBJECT".to_string(),
                    ..Default::default()
                };
                self.checksum = Some(checksum);
            }
            Some(ChecksumValue::Crc32c(crc32c)) => {
                let checksum = Checksum {
                    checksum_crc32c: Some(BASE64_STANDARD.encode(crc32c)),
                    checksum_type: "FULL_OBJECT".to_string(),
                    ..Default::default()
                };
                self.checksum = Some(checksum);
            }
            Some(ChecksumValue::Sha1(sha1)) => {
                let checksum = Checksum {
                    checksum_sha1: Some(BASE64_STANDARD.encode(sha1)),
                    checksum_type: "FULL_OBJECT".to_string(),
                    ..Default::default()
                };
                self.checksum = Some(checksum);
            }
            Some(ChecksumValue::Sha256(sha256)) => {
                let checksum = Checksum {
                    checksum_sha256: Some(BASE64_STANDARD.encode(sha256)),
                    checksum_type: "FULL_OBJECT".to_string(),
                    ..Default::default()
                };
                self.checksum = Some(checksum);
            }
            Some(ChecksumValue::Crc64Nvme(crc64nvme)) => {
                let checksum = Checksum {
                    checksum_crc64nvme: Some(BASE64_STANDARD.encode(crc64nvme)),
                    checksum_type: "FULL_OBJECT".to_string(),
                    ..Default::default()
                };
                self.checksum = Some(checksum);
            }
            None => (),
        }
        self
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Checksum {
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
    checksum_type: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ObjectParts {
    is_truncated: bool,
    max_parts: usize,
    next_part_number_marker: usize,
    part_number_marker: usize,
    part: Part,
    parts_count: usize,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Part {
    checksum_crc32: String,
    checksum_crc32c: String,
    checksum_sha1: String,
    checksum_sha256: String,
    part_number: usize,
    size: usize,
}

pub async fn get_object_attributes_handler(
    ctx: ObjectRequestContext,
) -> Result<actix_web::HttpResponse, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let _query_opts = Query::<QueryOpts>::from_query(ctx.request.query_string())
        .unwrap_or_else(|_| Query(Default::default()));

    // Parse object attributes from headers
    let header_opts = HeaderOpts::from_headers(ctx.request.headers())?;
    let obj = get_raw_object(
        &ctx.app,
        &bucket.routing_key,
        &bucket.root_blob_name,
        &ctx.key,
        &ctx.trace_id,
    )
    .await?;
    let last_modified = time::format_http_date(obj.timestamp);

    let mut resp = GetObjectAttributesOutput::default();
    if header_opts.x_amz_object_attributes.contains("etag") {
        resp = resp.etag(obj.etag()?);
    }
    if header_opts.x_amz_object_attributes.contains("checksum") {
        resp = resp.checksum(obj.checksum()?);
    }
    if header_opts.x_amz_object_attributes.contains("objectsize") {
        resp = resp.object_size(obj.size()? as usize);
    }
    // TODO: ObjectParts | StorageClass
    let mut resp: actix_web::HttpResponse = Xml(resp).try_into()?;
    resp.head_mut().headers_mut().insert(
        actix_web::http::header::LAST_MODIFIED,
        actix_web::http::header::HeaderValue::from_str(&last_modified)?,
    );
    Ok(resp)
}
