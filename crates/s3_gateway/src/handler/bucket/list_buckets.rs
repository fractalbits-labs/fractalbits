use actix_web::HttpResponse;
use serde::{Deserialize, Serialize};

use crate::handler::{
    BucketRequestContext,
    common::{
        response::xml::{Xml, XmlnsS3},
        s3_error::S3Error,
        time::format_timestamp,
    },
};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListBucketsOptions {
    bucket_region: Option<String>,
    continuation_token: Option<String>,
    max_buckets: Option<u32>,
    prefix: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListAllMyBucketsResult {
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
    buckets: Buckets,
    owner: Owner,
    continuation_token: String,
    prefix: String,
}

// Needs to create wrapper to create the correct lists, see
// https://docs.rs/quick-xml/latest/quick_xml/de/index.html#element-lists
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Buckets {
    bucket: Vec<Bucket>,
}

impl From<Vec<Bucket>> for ListAllMyBucketsResult {
    fn from(buckets: Vec<Bucket>) -> Self {
        Self {
            buckets: Buckets { bucket: buckets },
            ..Default::default()
        }
    }
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Bucket {
    bucket_region: String,
    creation_date: String, // timestamp
    name: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Owner {
    display_name: String,
    #[serde(rename = "ID")]
    id: String,
}

pub async fn list_buckets_handler(ctx: BucketRequestContext) -> Result<HttpResponse, S3Error> {
    // TODO: Extract query parameters for ListBucketsOptions if needed
    let buckets: Vec<Bucket> = ctx
        .app
        .list_buckets(ctx.trace_id)
        .await?
        .iter()
        .map(|bucket| Bucket {
            bucket_region: ctx.app.config.region.clone(),
            creation_date: format_timestamp(bucket.creation_date),
            name: bucket.bucket_name.clone(),
        })
        .collect();
    Xml(ListAllMyBucketsResult::from(buckets)).try_into()
}
