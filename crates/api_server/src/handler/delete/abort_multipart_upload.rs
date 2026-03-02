use crate::handler::{ObjectRequestContext, common::s3_error::S3Error};
use actix_web::HttpResponse;
use bytes::Bytes;
use file_ops::{parse_get_inode, parse_put_inode};
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use rpc_client_common::nss_rpc_retry;

pub async fn abort_multipart_upload_handler(
    ctx: ObjectRequestContext,
    upload_id: String,
) -> Result<HttpResponse, S3Error> {
    tracing::info!(
        "Aborting multipart upload {} for {}/{}",
        upload_id,
        ctx.bucket_name,
        ctx.key
    );

    // Basic upload_id validation - check it's a valid UUID format
    if uuid::Uuid::parse_str(&upload_id).is_err() {
        return Err(S3Error::NoSuchUpload);
    }

    let bucket = ctx.resolve_bucket().await?;
    let rpc_timeout = ctx.app.config.rpc_request_timeout();
    let nss_client = ctx.app.get_nss_rpc_client().await?;
    let resp = nss_rpc_retry!(
        nss_client,
        get_inode(
            &bucket.root_blob_name,
            &ctx.key,
            Some(rpc_timeout),
            &ctx.trace_id
        ),
        ctx.app,
        &ctx.trace_id
    )
    .await?;

    let mut object = match parse_get_inode(resp) {
        Ok(layout) => layout,
        Err(file_ops::NssError::NotFound) => return Err(S3Error::NoSuchUpload),
        Err(e) => return Err(e.into()),
    };
    if object.version_id.simple().to_string() != upload_id {
        return Err(S3Error::NoSuchUpload);
    }

    object.state =
        data_types::object_layout::ObjectState::Mpu(data_types::object_layout::MpuState::Aborted);
    let new_object_bytes: Bytes = to_bytes_in::<_, Error>(&object, Vec::new())?.into();

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

    parse_put_inode(resp)?;

    Ok(HttpResponse::NoContent().finish())
}
