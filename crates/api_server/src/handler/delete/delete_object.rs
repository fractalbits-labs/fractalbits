use crate::{
    blob_client::enqueue_blob_deletion,
    handler::{
        ObjectRequestContext,
        common::{list_raw_objects, mpu_get_part_prefix, s3_error::S3Error},
    },
};
use actix_web::HttpResponse;
use data_types::object_layout::{MpuState, ObjectLayout, ObjectState};
use file_ops::parse_delete_inode;
use metrics_wrapper::histogram;
use rkyv::{self, rancor::Error};
use rpc_client_common::nss_rpc_retry;

pub async fn delete_object_handler(ctx: ObjectRequestContext) -> Result<HttpResponse, S3Error> {
    tracing::debug!("DeleteObject handler: {}/{}", ctx.bucket_name, ctx.key);

    let bucket = ctx.resolve_bucket().await?;
    let routing_key = &bucket.routing_key;
    ctx.app
        .get_blob_client(routing_key)
        .await
        .map_err(|error| {
            tracing::warn!(%error, "failed to start deletion worker before object delete");
            S3Error::InternalError
        })?;
    let rpc_timeout = ctx.app.config.rpc_request_timeout();
    let nss_client = ctx.app.get_nss_rpc_client(routing_key).await?;
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

    let object_bytes = match parse_delete_inode(resp)? {
        Some(bytes) => bytes,
        None => {
            // Object doesn't exist or already deleted. S3 returns success for idempotent operations.
            // A previous delete may have removed the main inode before cleaning up MPU part inodes.
            tracing::debug!(
                "delete non-existing or already-deleted object {}/{}",
                bucket.bucket_name,
                ctx.key
            );
            let mpu_prefix = mpu_get_part_prefix(ctx.key.clone(), 0);
            if let Ok(mpus) = list_raw_objects(
                &ctx.app,
                routing_key,
                &bucket.root_blob_name,
                10_000,
                &mpu_prefix,
                "",
                "",
                false,
                &ctx.trace_id,
            )
            .await
            {
                for (mpu_key, mpu_obj) in &mpus {
                    let _ = nss_rpc_retry!(
                        nss_client,
                        delete_inode(
                            &bucket.root_blob_name,
                            mpu_key,
                            Some(rpc_timeout),
                            &ctx.trace_id
                        ),
                        ctx.app,
                        routing_key,
                        &ctx.trace_id
                    )
                    .await;
                    let _ = enqueue_blob_deletion(
                        ctx.app.clone(),
                        *routing_key,
                        &bucket.root_blob_name,
                        mpu_obj,
                    )
                    .await;
                }
                if !mpus.is_empty() {
                    tracing::info!(
                        "Cleaned up {} orphaned MPU parts for {}/{}",
                        mpus.len(),
                        bucket.bucket_name,
                        ctx.key
                    );
                }
            }
            return Ok(HttpResponse::NoContent().finish());
        }
    };

    if !object_bytes.is_empty() {
        let object: ObjectLayout =
            rkyv::from_bytes::<ObjectLayout, Error>(&object_bytes).map_err(|e| {
                tracing::error!("Failed to deserialize object: {e}");
                S3Error::InternalError
            })?;

        // Record metrics for deleted object size
        if let Ok(size) = object.size() {
            histogram!("object_size", "operation" => "delete").record(size as f64);
        }

        // Handle cleanup based on object state
        match &object.state {
            ObjectState::Normal(..) => {
                // Delete blob for normal objects
                enqueue_blob_deletion(
                    ctx.app.clone(),
                    *routing_key,
                    &bucket.root_blob_name,
                    &object,
                )
                .await?;
            }
            ObjectState::Mpu(mpu_state) => match mpu_state {
                MpuState::Uploading => {
                    tracing::warn!("invalid mpu state: Uploading");
                    return Err(S3Error::InvalidObjectState);
                }
                MpuState::Completed { .. } => {
                    // Clean up completed multipart upload parts
                    let mpu_prefix = mpu_get_part_prefix(ctx.key.clone(), 0);
                    let mpus = list_raw_objects(
                        &ctx.app,
                        routing_key,
                        &bucket.root_blob_name,
                        10_000,
                        &mpu_prefix,
                        "",
                        "",
                        false,
                        &ctx.trace_id,
                    )
                    .await?;
                    for (mpu_key, mpu_obj) in &mpus {
                        nss_rpc_retry!(
                            nss_client,
                            delete_inode(
                                &bucket.root_blob_name,
                                mpu_key,
                                Some(rpc_timeout),
                                &ctx.trace_id
                            ),
                            ctx.app,
                            routing_key,
                            &ctx.trace_id
                        )
                        .await?;
                        enqueue_blob_deletion(
                            ctx.app.clone(),
                            *routing_key,
                            &bucket.root_blob_name,
                            mpu_obj,
                        )
                        .await?;
                    }
                }
            },
            // Symlinks, special files (fifo / device / socket) and
            // directory inodes are FS-only concepts with no associated
            // blob to clean up; the namespace-level delete above is
            // sufficient. Indirect entries are schema-only today and
            // should never reach this handler.
            ObjectState::Symlink(_)
            | ObjectState::Special(_)
            | ObjectState::Directory(_)
            | ObjectState::Indirect(_) => {}
        }
    }

    Ok(HttpResponse::NoContent().finish())
}
