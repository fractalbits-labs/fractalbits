use crate::{
    blob_client::BlobDeletionRequest,
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
use tokio::sync::mpsc::Sender;

pub async fn delete_object_handler(ctx: ObjectRequestContext) -> Result<HttpResponse, S3Error> {
    tracing::debug!("DeleteObject handler: {}/{}", ctx.bucket_name, ctx.key);

    let bucket = ctx.resolve_bucket().await?;
    let blob_deletion = ctx.app.get_blob_deletion();
    let rpc_timeout = ctx.app.config.rpc_request_timeout();
    let nss_client = ctx.app.get_nss_rpc_client().await?;
    let resp = nss_rpc_retry!(
        nss_client,
        delete_inode(
            &bucket.root_blob_name,
            &ctx.key,
            Some(rpc_timeout),
            &ctx.trace_id
        ),
        ctx.app,
        &ctx.trace_id
    )
    .await?;

    let object_bytes = match parse_delete_inode(resp)? {
        Some(bytes) => bytes,
        None => {
            // Object doesn't exist or already deleted - S3 returns success for idempotent operations.
            // However, a previous delete may have removed the main inode but failed before cleaning
            // up MPU part inodes (e.g., due to a transient RPC error). Attempt best-effort cleanup
            // of any orphaned MPU parts so the bucket can eventually be deleted.
            tracing::debug!(
                "delete non-existing or already-deleted object {}/{}",
                bucket.bucket_name,
                ctx.key
            );
            let mpu_prefix = mpu_get_part_prefix(ctx.key.clone(), 0);
            if let Ok(mpus) = list_raw_objects(
                &ctx.app,
                &bucket.root_blob_name,
                10000,
                &mpu_prefix,
                "",
                "",
                false,
                &ctx.trace_id,
            )
            .await
            {
                for (mpu_key, mpu_obj) in mpus.iter() {
                    let _ = nss_rpc_retry!(
                        nss_client,
                        delete_inode(
                            &bucket.root_blob_name,
                            mpu_key,
                            Some(rpc_timeout),
                            &ctx.trace_id
                        ),
                        ctx.app,
                        &ctx.trace_id
                    )
                    .await;
                    let _ = delete_blob(
                        bucket.tracking_root_blob_name.clone(),
                        mpu_obj,
                        blob_deletion.clone(),
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
                delete_blob(
                    bucket.tracking_root_blob_name.clone(),
                    &object,
                    blob_deletion,
                )
                .await?;
            }
            ObjectState::Mpu(mpu_state) => match mpu_state {
                MpuState::Uploading => {
                    tracing::warn!("invalid mpu state: Uploading");
                    return Err(S3Error::InvalidObjectState);
                }
                MpuState::Aborted => {
                    tracing::warn!("invalid mpu state: Aborted");
                    return Err(S3Error::InvalidObjectState);
                }
                MpuState::Completed { .. } => {
                    // Clean up completed multipart upload parts
                    let mpu_prefix = mpu_get_part_prefix(ctx.key.clone(), 0);
                    let mpus = list_raw_objects(
                        &ctx.app,
                        &bucket.root_blob_name,
                        10000,
                        &mpu_prefix,
                        "",
                        "",
                        false,
                        &ctx.trace_id,
                    )
                    .await?;
                    for (mpu_key, mpu_obj) in mpus.iter() {
                        nss_rpc_retry!(
                            nss_client,
                            delete_inode(
                                &bucket.root_blob_name,
                                &mpu_key,
                                Some(rpc_timeout),
                                &ctx.trace_id
                            ),
                            ctx.app,
                            &ctx.trace_id
                        )
                        .await?;
                        // Delete blob for each multipart upload part
                        delete_blob(
                            bucket.tracking_root_blob_name.clone(),
                            mpu_obj,
                            blob_deletion.clone(),
                        )
                        .await?;
                    }
                }
            },
        }
    }

    Ok(HttpResponse::NoContent().finish())
}

async fn delete_blob(
    tracking_root_blob_name: Option<String>,
    object: &ObjectLayout,
    blob_deletion: Sender<BlobDeletionRequest>,
) -> Result<(), S3Error> {
    let blob_guid = object.blob_guid()?;
    let num_blocks = object.num_blocks()?;
    let blob_location = object.get_blob_location()?;

    // Send deletion request for each block
    for block_number in 0..num_blocks {
        let request = BlobDeletionRequest {
            tracking_root_blob_name: tracking_root_blob_name.clone(),
            blob_guid,
            block_number: block_number as u32,
            location: blob_location,
        };

        if let Err(e) = blob_deletion.send(request).await {
            tracing::warn!(
                "Failed to send blob {blob_guid} block={block_number} for background deletion: {e}"
            );
        }
    }

    Ok(())
}
