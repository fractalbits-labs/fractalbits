use actix_web::web::Query;
use nss_codec::list_inodes_response;
use rpc_client_common::nss_rpc_retry;
use std::sync::Arc;

use crate::object_layout::ObjectLayout;
use crate::{
    AppState,
    handler::{
        ObjectRequestContext,
        common::{
            response::xml::{Xml, XmlnsS3},
            s3_error::S3Error,
            time::format_timestamp,
        },
    },
};
use data_types::{Bucket, TraceId};
use rkyv::{self, rancor::Error};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
struct QueryOpts {
    list_type: Option<String>,
    continuation_token: Option<String>,
    delimiter: Option<String>,
    encoding_type: Option<String>,
    #[allow(dead_code)]
    fetch_owner: Option<bool>,
    max_keys: Option<u32>,
    prefix: Option<String>,
    start_after: Option<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
    is_truncated: bool,
    contents: Vec<Object>,
    name: String,
    prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delimiter: Option<String>,
    max_keys: u32,
    common_prefixes: Vec<Prefix>,
    encoding_type: String,
    key_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    continuation_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_continuation_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_after: Option<String>,
}

impl Default for ListBucketResult {
    fn default() -> Self {
        Self {
            xmlns: Default::default(),
            is_truncated: false,
            contents: Default::default(),
            name: Default::default(),
            prefix: Default::default(),
            delimiter: Default::default(),
            max_keys: 1000,
            common_prefixes: Default::default(),
            encoding_type: "url".into(),
            key_count: 0,
            continuation_token: Default::default(),
            next_continuation_token: Default::default(),
            start_after: Default::default(),
        }
    }
}

impl ListBucketResult {
    fn truncated(self, is_truncated: bool) -> Self {
        Self {
            is_truncated,
            ..self
        }
    }

    fn contents(self, contents: Vec<Object>) -> Self {
        Self { contents, ..self }
    }

    fn bucket_name(self, bucket_name: String) -> Self {
        Self {
            name: bucket_name,
            ..self
        }
    }

    fn prefix(self, prefix: Option<String>) -> Self {
        Self { prefix, ..self }
    }

    fn delimiter(self, delimiter: Option<String>) -> Self {
        Self { delimiter, ..self }
    }

    fn max_keys(self, max_keys: u32) -> Self {
        Self { max_keys, ..self }
    }

    fn common_prefixes(self, common_prefixes: Vec<Prefix>) -> Self {
        Self {
            common_prefixes,
            ..self
        }
    }

    fn key_count(self, key_count: usize) -> Self {
        Self { key_count, ..self }
    }

    fn continuation_token(self, continuation_token: Option<String>) -> Self {
        Self {
            continuation_token,
            ..self
        }
    }

    fn next_continuation_token(self, next_continuation_token: Option<String>) -> Self {
        Self {
            next_continuation_token,
            ..self
        }
    }

    fn start_after(self, start_after: Option<String>) -> Self {
        Self {
            start_after,
            ..self
        }
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct Object {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_algorithm: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_type: Option<String>,
    #[serde(rename = "ETag")]
    pub etag: String,
    pub key: String,
    pub last_modified: String, // timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<Owner>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore_status: Option<RestoreStatus>,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
}

impl Object {
    pub(crate) fn from_layout_and_key(obj: ObjectLayout, key: String) -> Result<Self, S3Error> {
        Ok(Self {
            key,
            last_modified: format_timestamp(obj.timestamp),
            etag: obj.etag()?,
            size: obj.size()?,
            ..Default::default()
        })
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct Owner {
    pub display_name: String,
    pub id: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct RestoreStatus {
    pub is_restore_in_progress: bool,
    pub restore_expiry_date: String, // timestamp
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct Prefix {
    prefix: String,
}

pub async fn list_objects_v2_handler(
    ctx: ObjectRequestContext,
) -> Result<actix_web::HttpResponse, S3Error> {
    let opts = Query::<QueryOpts>::from_query(ctx.request.query_string())
        .unwrap_or_else(|_| Query(Default::default()))
        .into_inner();
    tracing::debug!(
        "list_objects_v2 query_string='{}' parsed={opts:?}",
        ctx.request.query_string()
    );

    // Sanity checks
    if opts.list_type != Some("2".into()) {
        tracing::warn!(
            "expecting list_type as \"2\" only, got {:?}",
            opts.list_type
        );
        return Err(S3Error::InvalidArgument1);
    }
    if let Some(encoding_type) = &opts.encoding_type
        && encoding_type != "url"
    {
        tracing::warn!(
            "expecting content_type as \"url\" only, got {}",
            encoding_type
        );
        return Err(S3Error::InvalidArgument1);
    }

    // Get bucket info
    let bucket = ctx.resolve_bucket().await?;

    let max_keys = opts.max_keys.unwrap_or(1000);
    let prefix = format!("/{}", opts.prefix.clone().unwrap_or_default());
    let delimiter = opts.delimiter.clone().unwrap_or("".into());
    if !delimiter.is_empty() && delimiter != "/" {
        tracing::warn!("Got delimiter: {delimiter}, which is not supported.");
        return Err(S3Error::UnsupportedArgument);
    }
    let mut start_after = match opts.start_after {
        Some(ref start_after_key) => start_after_key.clone(),
        None => opts.continuation_token.clone().unwrap_or_default(),
    };
    // Prepend "/" for valid start_after key
    if !start_after.is_empty() {
        start_after = format!("/{start_after}");
    }

    tracing::debug!(
        "Calling list_objects with max_keys={}, prefix='{}', delimiter='{}', start_after='{}'",
        max_keys,
        prefix,
        delimiter,
        start_after
    );

    let (objs, common_prefixes, next_continuation_token) = list_objects(
        ctx.app,
        &bucket,
        max_keys,
        prefix,
        delimiter,
        start_after,
        ctx.trace_id,
    )
    .await?;

    tracing::debug!(
        "list_objects returned {} objects, {} common_prefixes",
        objs.len(),
        common_prefixes.len()
    );

    Xml(ListBucketResult::default()
        .key_count(objs.len())
        .contents(objs)
        .bucket_name(bucket.bucket_name.clone())
        .prefix(opts.prefix)
        .start_after(opts.start_after)
        .delimiter(opts.delimiter)
        .max_keys(max_keys)
        .common_prefixes(common_prefixes)
        .continuation_token(opts.continuation_token)
        .truncated(next_continuation_token.is_some())
        .next_continuation_token(next_continuation_token))
    .try_into()
}

pub async fn list_objects(
    app: Arc<AppState>,
    bucket: &Bucket,
    max_keys: u32,
    prefix: String,
    delimiter: String,
    start_after: String,
    trace_id: TraceId,
) -> Result<(Vec<Object>, Vec<Prefix>, Option<String>), S3Error> {
    tracing::debug!(
        "NSS list_inodes call with root_blob_name='{}', max_keys={}, prefix='{}', delimiter='{}', start_after='{}'",
        bucket.root_blob_name,
        max_keys,
        prefix,
        delimiter,
        start_after
    );

    let nss_client = app.get_nss_rpc_client();
    let resp = nss_rpc_retry!(
        nss_client,
        list_inodes(
            &bucket.root_blob_name,
            max_keys,
            &prefix,
            &delimiter,
            &start_after,
            true,
            Some(app.config.rpc_request_timeout()),
            &trace_id
        )
    )
    .await?;

    // Process results
    let (inodes, has_more) = match resp.result.unwrap() {
        list_inodes_response::Result::Ok(res) => {
            tracing::debug!(
                "NSS returned {} inodes, has_more={}",
                res.inodes.len(),
                res.has_more
            );
            (res.inodes, res.has_more)
        }
        list_inodes_response::Result::Err(e) => {
            tracing::error!("NSS list_inodes error: {}", e);
            return Err(S3Error::InternalError);
        }
    };

    let mut objs = Vec::new();
    let mut common_prefixes = Vec::new();
    for inode_with_key in inodes.iter() {
        if inode_with_key.inode.is_empty() {
            common_prefixes.push(Prefix {
                prefix: inode_with_key.key[1..].to_owned(), // remove first "/"
            });
            continue;
        }

        match rkyv::from_bytes::<ObjectLayout, Error>(&inode_with_key.inode) {
            Err(e) => return Err(e.into()),
            Ok(obj) => {
                // Skip objects in non-final states (e.g., in-progress or aborted MPU)
                if !obj.is_listable() {
                    continue;
                }
                let mut key = inode_with_key.key[1..].to_owned();
                assert_eq!(Some('\0'), key.pop()); // removing nss's trailing '\0'
                objs.push(Object::from_layout_and_key(obj, key)?);
            }
        }
    }

    let next_continuation_token = if !has_more {
        None
    } else {
        inodes
            .last()
            .map(|inode| inode.key[1..].trim_end_matches('\0').to_owned())
    };

    Ok((objs, common_prefixes, next_continuation_token))
}
