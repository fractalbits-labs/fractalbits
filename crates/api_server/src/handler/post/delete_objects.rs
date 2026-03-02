use crate::handler::ObjectRequestContext;
use crate::handler::common::{
    buffer_payload,
    response::xml::{Xml, XmlnsS3},
    s3_error::S3Error,
};
use crate::handler::delete::delete_object_handler;
use bytes::Buf;
use serde::{Deserialize, Serialize};

// Xml request
#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Delete {
    object: Vec<Object>,
    #[serde(default)]
    quiet: bool,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Object {
    #[serde(default)]
    etag: String,
    key: String,
    #[serde(default)]
    last_modified_time: String, // timestamp
    #[serde(default)]
    size: u64,
    #[serde(default)]
    version_id: String,
}

// Xml response
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct DeleteResult {
    #[serde(rename = "@xmlns")]
    xmlns: XmlnsS3,
    deleted: Vec<Deleted>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<Error>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Deleted {
    #[serde(skip_serializing_if = "Option::is_none")]
    delete_marker: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    delete_marker_version_id: Option<String>,
    key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version_id: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Error {
    code: String,
    key: String,
    message: String,
    version_id: String,
}

pub async fn delete_objects_handler(
    ctx: ObjectRequestContext,
) -> Result<ntex::web::HttpResponse, S3Error> {
    let _bucket = ctx.resolve_bucket().await?;

    // Extract body from payload using the helper function
    let chunks = buffer_payload(ctx.payload).await?;
    let body = crate::handler::common::merge_chunks(chunks);

    // Parse the XML body to get the list of objects to delete
    let to_be_deleted: Delete = quick_xml::de::from_reader(body.reader())?;

    let mut delete_result = DeleteResult::default();

    // Process each object to be deleted
    for obj in to_be_deleted.object {
        let key = format!("/{}", obj.key);
        let delete_ctx = ObjectRequestContext::new(
            ctx.app.clone(),
            ctx.request.clone(),
            None,
            ctx.bucket_name.clone(),
            key,
            None, // No checksum value needed for delete
            ntex::http::Payload::None,
            ctx.trace_id,
        );
        match delete_object_handler(delete_ctx).await {
            Ok(_) => {
                let deleted = Deleted {
                    key: obj.key,
                    ..Default::default()
                };
                delete_result.deleted.push(deleted);
            }
            Err(e) => {
                // Check if it's a "not found" error which is allowed in S3
                if matches!(e, S3Error::NoSuchKey) {
                    // S3 allows deleting non-existing objects
                    let deleted = Deleted {
                        key: obj.key,
                        ..Default::default()
                    };
                    delete_result.deleted.push(deleted);
                } else {
                    // Record the error but continue processing remaining objects.
                    // Stopping at the first error would leave the rest of the batch
                    // unprocessed and is not correct S3 batch delete behavior.
                    delete_result.error = Some(Error {
                        code: e.as_ref().to_owned(),
                        key: obj.key,
                        message: e.to_string(),
                        version_id: "".to_string(),
                    });
                }
            }
        }
    }

    // Generate XML response using Xml wrapper
    Xml(delete_result).try_into()
}
