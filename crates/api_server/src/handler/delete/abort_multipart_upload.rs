use crate::handler::{
    ObjectRequestContext,
    common::{list_raw_objects, mpu_get_part_prefix, s3_error::S3Error},
    delete::delete_object::delete_blob,
};
use actix_web::HttpResponse;
use file_ops::{parse_delete_inode, parse_get_inode};
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
    let routing_key = &bucket.routing_key;
    let blob_deletion = ctx.app.get_blob_deletion();
    let rpc_timeout = ctx.app.config.rpc_request_timeout();
    let nss_client = ctx.app.get_nss_rpc_client(routing_key).await?;

    // Verify the upload exists and the upload_id matches
    let resp = nss_rpc_retry!(
        nss_client,
        get_inode(
            &bucket.root_blob_name,
            &ctx.key,
            Some(rpc_timeout),
            &ctx.trace_id
        ),
        ctx.app,
        routing_key,
        &ctx.trace_id
    )
    .await?;

    let object = match parse_get_inode(resp) {
        Ok(layout) => layout,
        Err(file_ops::NssError::NotFound) => return Err(S3Error::NoSuchUpload),
        Err(e) => return Err(e.into()),
    };
    if object.version_id.simple().to_string() != upload_id {
        return Err(S3Error::NoSuchUpload);
    }

    // Delete all uploaded parts and their blobs
    let mpu_prefix = mpu_get_part_prefix(ctx.key.clone(), 0);
    let parts = list_raw_objects(
        &ctx.app,
        routing_key,
        &bucket.root_blob_name,
        10000,
        &mpu_prefix,
        "",
        "",
        false,
        &ctx.trace_id,
    )
    .await?;
    for (part_key, part_obj) in parts.iter() {
        nss_rpc_retry!(
            nss_client,
            delete_inode(
                &bucket.root_blob_name,
                part_key,
                Some(rpc_timeout),
                &ctx.trace_id
            ),
            ctx.app,
            routing_key,
            &ctx.trace_id
        )
        .await?;
        delete_blob(
            bucket.tracking_root_blob_name.clone(),
            part_obj,
            blob_deletion.clone(),
        )
        .await?;
    }

    // Delete the main MPU inode
    let resp = nss_rpc_retry!(
        nss_client,
        delete_inode(
            &bucket.root_blob_name,
            &ctx.key,
            Some(rpc_timeout),
            &ctx.trace_id
        ),
        ctx.app,
        routing_key,
        &ctx.trace_id
    )
    .await?;
    parse_delete_inode(resp)?;

    Ok(HttpResponse::NoContent().finish())
}
