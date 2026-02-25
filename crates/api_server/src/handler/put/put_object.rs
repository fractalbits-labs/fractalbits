use data_types::{DataBlobGuid, Volume};
use metrics_wrapper::histogram;
use rpc_client_common::nss_rpc_retry;
use std::hash::Hasher;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use actix_web::{HttpResponse, http::header};
use aws_signature::{STREAMING_PAYLOAD, sigv4::get_signing_key_cached};
use crc32c::Crc32cHasher as Crc32c;
use crc32fast::Hasher as Crc32;
use futures::{StreamExt, TryStreamExt};
use nss_codec::put_inode_response;
use rkyv::{self, api::high::to_bytes_in, rancor::Error};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use tracing::{Instrument, Span};

use super::block_data_stream::BlockDataStream;
use super::s3_streaming::S3StreamingPayload;
use crate::{
    BlobStorageBackend,
    blob_client::BlobDeletionRequest,
    handler::{
        ObjectRequestContext,
        common::{
            buffer_payload_with_capacity,
            checksum::{self, ChecksumAlgorithm, ChecksumValue},
            extract_metadata_headers,
            request::extract::extract_authentication,
            s3_error::S3Error,
            signature::ChunkSignatureContext,
        },
    },
    object_layout::*,
};

fn split_chunks_into_blocks(
    chunks: Vec<actix_web::web::Bytes>,
    block_size: usize,
) -> Vec<Vec<actix_web::web::Bytes>> {
    let mut blocks = Vec::new();
    let mut current_block = Vec::new();
    let mut current_block_size = 0;

    for chunk in chunks {
        let mut chunk_remaining = chunk;

        while !chunk_remaining.is_empty() {
            let space_left = block_size - current_block_size;

            if chunk_remaining.len() <= space_left {
                current_block_size += chunk_remaining.len();
                current_block.push(chunk_remaining);
                break;
            } else {
                let (fit, rest) = (
                    chunk_remaining.slice(0..space_left),
                    chunk_remaining.slice(space_left..),
                );
                current_block.push(fit);

                blocks.push(current_block);
                current_block = Vec::new();
                current_block_size = 0;
                chunk_remaining = rest;
            }
        }
    }

    if !current_block.is_empty() {
        blocks.push(current_block);
    }

    blocks
}

pub async fn put_object_handler(ctx: ObjectRequestContext) -> Result<HttpResponse, S3Error> {
    // Debug: log all request headers to understand what's being sent
    tracing::debug!("PUT object request headers:");
    for (name, value) in ctx.request.headers().iter() {
        tracing::debug!("  {}: {:?}", name, value);
    }

    // Create blob GUID once for this object upload
    let blob_client = ctx
        .app
        .get_blob_client()
        .await
        .map_err(|_| S3Error::InternalError)?;
    let content_len_hint = ctx
        .request
        .headers()
        .get("content-length")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok());
    let blob_guid = blob_client.create_data_blob_guid_with_size_hint(content_len_hint);

    // Decide whether to use streaming based on request characteristics
    if should_use_streaming(&ctx.request) {
        tracing::debug!("Using streaming path for PUT object");
        put_object_streaming_internal(ctx, blob_guid).await
    } else {
        tracing::debug!("Using buffered path for PUT object");
        put_object_with_no_trailer(ctx, blob_guid).await
    }
}

// Helper function to calculate checksum for the given body data
fn calculate_checksum_for_chunks(
    chunks: &[actix_web::web::Bytes],
    expected_checksum: &Option<ChecksumValue>,
) -> Result<Option<ChecksumValue>, S3Error> {
    match expected_checksum {
        Some(ChecksumValue::Crc32(expected_bytes)) => {
            let mut hasher = Crc32::new();
            for chunk in chunks {
                hasher.write(chunk);
            }
            let calculated = hasher.finalize().to_be_bytes();

            // Verify against expected if provided
            if calculated != *expected_bytes {
                tracing::error!(
                    "CRC32 checksum mismatch: expected {:?}, calculated {:?}",
                    expected_bytes,
                    calculated
                );
                return Err(S3Error::InvalidDigest);
            }
            Ok(Some(ChecksumValue::Crc32(calculated)))
        }
        Some(ChecksumValue::Crc32c(expected_bytes)) => {
            let mut hasher = Crc32c::new(0);
            for chunk in chunks {
                hasher.write(chunk);
            }
            let calculated = (hasher.finish() as u32).to_be_bytes();

            if calculated != *expected_bytes {
                tracing::error!(
                    "CRC32C checksum mismatch: expected {:?}, calculated {:?}",
                    expected_bytes,
                    calculated
                );
                return Err(S3Error::InvalidDigest);
            }
            Ok(Some(ChecksumValue::Crc32c(calculated)))
        }
        Some(ChecksumValue::Crc64Nvme(expected_bytes)) => {
            let mut hasher = crc64fast_nvme::Digest::new();
            for chunk in chunks {
                hasher.write(chunk);
            }
            let calculated = hasher.sum64().to_be_bytes();

            if calculated != *expected_bytes {
                tracing::error!(
                    "CRC64NVME checksum mismatch: expected {:?}, calculated {:?}",
                    expected_bytes,
                    calculated
                );
                return Err(S3Error::InvalidDigest);
            }
            Ok(Some(ChecksumValue::Crc64Nvme(calculated)))
        }
        Some(ChecksumValue::Sha1(expected_bytes)) => {
            let mut hasher = Sha1::new();
            for chunk in chunks {
                hasher.update(chunk);
            }
            let calculated: [u8; 20] = hasher.finalize().into();

            if calculated != *expected_bytes {
                tracing::error!(
                    "SHA1 checksum mismatch: expected {:?}, calculated {:?}",
                    expected_bytes,
                    calculated
                );
                return Err(S3Error::InvalidDigest);
            }
            Ok(Some(ChecksumValue::Sha1(calculated)))
        }
        Some(ChecksumValue::Sha256(expected_bytes)) => {
            let mut hasher = Sha256::new();
            for chunk in chunks {
                hasher.update(chunk);
            }
            let calculated: [u8; 32] = hasher.finalize().into();

            if calculated != *expected_bytes {
                tracing::error!(
                    "SHA256 checksum mismatch: expected {:?}, calculated {:?}",
                    expected_bytes,
                    calculated
                );
                return Err(S3Error::InvalidDigest);
            }
            Ok(Some(ChecksumValue::Sha256(calculated)))
        }
        None => Ok(None),
    }
}

// Helper function to calculate checksum for chunks using specific algorithm
fn calculate_checksum_for_chunks_with_algorithm(
    chunks: &[actix_web::web::Bytes],
    algorithm: ChecksumAlgorithm,
) -> Result<Option<ChecksumValue>, S3Error> {
    match algorithm {
        ChecksumAlgorithm::Crc32 => {
            let mut hasher = Crc32::new();
            for chunk in chunks {
                hasher.write(chunk);
            }
            let calculated = hasher.finalize().to_be_bytes();
            Ok(Some(ChecksumValue::Crc32(calculated)))
        }
        ChecksumAlgorithm::Crc32c => {
            let mut hasher = Crc32c::new(0);
            for chunk in chunks {
                hasher.write(chunk);
            }
            let calculated = (hasher.finish() as u32).to_be_bytes();
            Ok(Some(ChecksumValue::Crc32c(calculated)))
        }
        ChecksumAlgorithm::Crc64Nvme => {
            let mut hasher = crc64fast_nvme::Digest::new();
            for chunk in chunks {
                hasher.write(chunk);
            }
            let calculated = hasher.sum64().to_be_bytes();
            Ok(Some(ChecksumValue::Crc64Nvme(calculated)))
        }
        ChecksumAlgorithm::Sha1 => {
            let mut hasher = Sha1::new();
            for chunk in chunks {
                hasher.update(chunk);
            }
            let calculated: [u8; 20] = hasher.finalize().into();
            Ok(Some(ChecksumValue::Sha1(calculated)))
        }
        ChecksumAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            for chunk in chunks {
                hasher.update(chunk);
            }
            let calculated: [u8; 32] = hasher.finalize().into();
            Ok(Some(ChecksumValue::Sha256(calculated)))
        }
    }
}

// Helper function to decide whether to use streaming based on request
fn should_use_streaming(request: &actix_web::HttpRequest) -> bool {
    // Always stream if trailers present (requires streaming to extract them)
    if request.headers().get("x-amz-trailer").is_some() {
        tracing::debug!("Streaming due to x-amz-trailer header");
        return true;
    }

    // Always stream if checksum algorithm is specified (for streaming checksum calculation)
    if request.headers().get("x-amz-checksum-algorithm").is_some() {
        tracing::debug!("Streaming due to x-amz-checksum-algorithm header");
        return true;
    }

    // Always stream for chunked transfer encoding (which AWS SDK uses for streaming checksums)
    if let Some(transfer_encoding) = request.headers().get("transfer-encoding")
        && let Ok(encoding) = transfer_encoding.to_str()
        && encoding.to_lowercase().contains("chunked")
    {
        tracing::debug!("Streaming due to chunked transfer-encoding");
        return true;
    }

    // Always stream for AWS chunk-signed requests
    if let Some(content_encoding) = request.headers().get("content-encoding")
        && let Ok(encoding) = content_encoding.to_str()
        && encoding.to_lowercase() == "aws-chunked"
    {
        tracing::debug!("Streaming due to aws-chunked content-encoding");
        return true;
    }

    // Get content length for size-based decisions
    let size = if let Some(cl) = request.headers().get("content-length") {
        cl.to_str()
            .unwrap_or("0")
            .parse::<usize>()
            .unwrap_or(usize::MAX)
    } else {
        usize::MAX // Unknown size, assume large
    };
    // Check content-length for larger objects
    if size != usize::MAX {
        // Use streaming for objects >= 1 block size
        let should_stream = size >= ObjectLayout::DEFAULT_BLOCK_SIZE as usize;
        tracing::debug!(
            "Content-Length: {}, DEFAULT_BLOCK_SIZE: {}, streaming: {}",
            size,
            ObjectLayout::DEFAULT_BLOCK_SIZE,
            should_stream
        );
        return should_stream;
    }

    // Stream if size is unknown (no content-length header)
    tracing::debug!("Streaming due to missing content-length");
    true
}

// Internal streaming handler that processes chunks as they arrive
async fn put_object_streaming_internal(
    ctx: ObjectRequestContext,
    mut blob_guid: DataBlobGuid,
) -> Result<HttpResponse, S3Error> {
    let start = Instant::now();

    tracing::debug!(
        "PutObject streaming handler: {}/{}, starting streaming upload",
        ctx.bucket_name,
        ctx.key,
    );

    // Resolve bucket to get bucket details
    let bucket_obj = ctx.resolve_bucket().await?;
    let tracking_root_blob_name = bucket_obj.tracking_root_blob_name.clone();

    // Extract metadata headers
    let headers = extract_metadata_headers(ctx.request.headers())?;

    // Extract chunk signature context before consuming payload
    let signature_context = extract_chunk_signature_context(&ctx)?;

    // Create S3 streaming payload with checksum calculation and chunk signature validation
    let payload = ctx.payload;
    let (s3_payload, checksum_future) = if signature_context.is_some() {
        tracing::debug!("Using chunk signature validation for streaming upload");
        S3StreamingPayload::with_checksums_and_signature(
            payload,
            &ctx.request,
            ctx.checksum_value,
            signature_context,
        )?
    } else {
        S3StreamingPayload::with_checksums(payload, &ctx.request, ctx.checksum_value)?
    };

    // Use the blob GUID passed from the main handler
    let blob_client = ctx
        .app
        .get_blob_client()
        .await
        .map_err(|_| S3Error::InternalError)?;

    // Convert S3 payload to block stream
    let block_stream = BlockDataStream::new(s3_payload, ObjectLayout::DEFAULT_BLOCK_SIZE);

    // Process blocks as they arrive, uploading them concurrently
    let size = block_stream
        .enumerate()
        .map(|(i, block_result)| {
            let blob_client = blob_client.clone();
            let tracking_root_blob_name = tracking_root_blob_name.clone();

            async move {
                let chunks = block_result.map_err(|e| {
                    tracing::error!("Stream error: {}", e);
                    S3Error::InternalError
                })?;

                let len: usize = chunks.iter().map(|c| c.len()).sum();
                let put_result = blob_client
                    .put_blob_vectored(
                        tracking_root_blob_name.as_deref(),
                        blob_guid,
                        i as u32,
                        chunks,
                        &ctx.trace_id,
                    )
                    .await;

                match put_result {
                    Ok(_blob_guid) => Ok(len as u64),
                    Err(e) => {
                        tracing::error!("Failed to store blob block {}: {}", i, e);
                        Err(S3Error::InternalError)
                    }
                }
            }
            .instrument(Span::current())
        })
        .buffer_unordered(5) // Process up to 5 blocks concurrently
        .try_fold(0u64, |acc, len| async move { Ok(acc + len) })
        .await
        .map_err(|_| S3Error::InternalError)?;

    let total_size = size;
    // Only use S3_VOLUME for large objects when using S3-based backends
    let uses_s3_for_large_blobs = matches!(
        ctx.app.config.blob_storage.backend,
        BlobStorageBackend::S3HybridSingleAz | BlobStorageBackend::S3ExpressMultiAz
    );
    if uses_s3_for_large_blobs
        && total_size >= ObjectLayout::DEFAULT_BLOCK_SIZE as u64
        && !Volume::is_ec_volume_id(blob_guid.volume_id)
    {
        blob_guid.volume_id = DataBlobGuid::S3_VOLUME;
    }

    histogram!("object_size", "operation" => "put").record(total_size as f64);
    histogram!("put_object_handler", "stage" => "put_blob")
        .record(start.elapsed().as_nanos() as f64);

    // Await checksum calculation completion
    let calculated_checksum = match checksum_future.await {
        Ok(Ok(checksum)) => {
            tracing::debug!("Streaming checksum verification completed successfully");
            checksum
        }
        Ok(Err(e)) => {
            tracing::error!("Checksum verification failed: {:?}", e);
            return Err(e);
        }
        Err(e) => {
            tracing::error!("Checksum calculation task failed: {:?}", e);
            return Err(S3Error::InternalError);
        }
    };

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let etag = blob_guid.blob_id.simple().to_string();
    let version_id = gen_version_id();

    // Create object layout
    let object_layout = ObjectLayout {
        version_id,
        block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
        timestamp,
        state: ObjectState::Normal(ObjectMetaData {
            blob_guid,
            core_meta_data: ObjectCoreMetaData {
                size: total_size,
                etag: etag.clone(),
                headers,
                checksum: calculated_checksum,
            },
        }),
    };

    // Serialize and store object metadata
    let object_layout_bytes: bytes::Bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new())
        .map_err(|e| {
            tracing::error!("Failed to serialize object layout: {e}");
            S3Error::InternalError
        })?
        .into();

    // Store object metadata in NSS
    let nss_client = ctx.app.get_nss_rpc_client().await?;
    let resp = nss_rpc_retry!(
        nss_client,
        put_inode(
            &bucket_obj.root_blob_name,
            &ctx.key,
            object_layout_bytes.clone(),
            Some(ctx.app.config.rpc_request_timeout()),
            &ctx.trace_id
        ),
        ctx.app,
        &ctx.trace_id
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to store object metadata: {e}");
        S3Error::from(e)
    })?;

    // Delete old object if it is an overwrite request
    // But skip deletion for multipart parts (keys containing '#') to avoid race conditions
    let old_object_bytes = match resp.result.unwrap() {
        put_inode_response::Result::Ok(res) => res,
        put_inode_response::Result::Err(e) => {
            tracing::error!("put_inode error: {}", e);
            return Err(S3Error::InternalError);
        }
    };

    let is_multipart_part = ctx.key.contains('#');
    if !old_object_bytes.is_empty() && !is_multipart_part {
        let old_object =
            rkyv::from_bytes::<ObjectLayout, Error>(&old_object_bytes).map_err(|e| {
                tracing::error!("Failed to deserialize old object layout: {e}");
                S3Error::InternalError
            })?;

        if let Ok(size) = old_object.size() {
            histogram!("object_size", "operation" => "delete_old_blob").record(size as f64);
        }
        let old_blob_guid = old_object.blob_guid().map_err(|e| {
            tracing::error!("Failed to get blob_id from old object: {e}");
            S3Error::InternalError
        })?;

        // Only delete old blob if it's different from the new one
        if old_blob_guid != blob_guid {
            let num_blocks = old_object.num_blocks().map_err(|e| {
                tracing::error!("Failed to get num_blocks from old object: {e}");
                S3Error::InternalError
            })?;

            let blob_deletion = ctx.app.get_blob_deletion();

            // Send deletion request for each block
            let blob_location = old_object.get_blob_location().map_err(|e| {
                tracing::error!("Failed to get blob_location from old object: {e}");
                S3Error::InternalError
            })?;
            for block_number in 0..num_blocks {
                let request = BlobDeletionRequest {
                    tracking_root_blob_name: bucket_obj.tracking_root_blob_name.clone(),
                    blob_guid: old_blob_guid,
                    block_number: block_number as u32,
                    location: blob_location,
                };

                if let Err(e) = blob_deletion.send(request).await {
                    tracing::warn!(
                        "Failed to send blob {old_blob_guid} block={block_number} for background deletion: {e}"
                    );
                }
            }
        } else {
            tracing::warn!(
                "Skipping deletion of old blob as it matches new blob GUID: {}",
                blob_guid.blob_id
            );
        }
    }

    histogram!("put_object_handler", "stage" => "done").record(start.elapsed().as_nanos() as f64);

    tracing::debug!(
        "Successfully stored object {}/{} with size {} via streaming",
        ctx.bucket_name,
        ctx.key,
        total_size
    );

    Ok(HttpResponse::Ok()
        .insert_header((header::ETAG, etag))
        .insert_header(("X-Amz-Object-Size", total_size.to_string()))
        .finish())
}

// Helper function for buffered upload with pre-resolved bucket
async fn put_object_with_no_trailer(
    ctx: ObjectRequestContext,
    blob_guid: DataBlobGuid,
) -> Result<HttpResponse, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let expected_size = ctx
        .request
        .headers()
        .get("content-length")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse().ok());
    // Buffer the entire payload for small objects with pre-allocation
    let chunks = buffer_payload_with_capacity(ctx.payload, expected_size).await?;

    let total_size: usize = chunks.iter().map(|c| c.len()).sum();
    tracing::debug!(
        "PutObject actix handler with resolved bucket: {}/{}, body size: {}",
        ctx.bucket_name,
        ctx.key,
        total_size
    );

    // Extract metadata headers
    let headers = extract_metadata_headers(ctx.request.headers())?;

    // Extract expected checksum from headers
    let expected_checksum = ctx.checksum_value;

    // Check if there's a trailer checksum algorithm specified
    let trailer_algo = checksum::request_trailer_checksum_algorithm(ctx.request.headers())?;

    // Calculate checksums if expected or if trailer algo is specified
    let calculated_checksum = if expected_checksum.is_some() {
        calculate_checksum_for_chunks(&chunks, &expected_checksum)?
    } else if let Some(algo) = trailer_algo {
        calculate_checksum_for_chunks_with_algorithm(&chunks, algo)?
    } else {
        None
    };

    // Store data in chunks
    let blob_client = ctx
        .app
        .get_blob_client()
        .await
        .map_err(|_| S3Error::InternalError)?;
    let size = total_size as u64;
    let block_size = ObjectLayout::DEFAULT_BLOCK_SIZE as usize;
    let tracking_root_blob_name = bucket.tracking_root_blob_name.as_deref();

    // If total size fits in one block, use vectored API to avoid copying
    if total_size <= block_size {
        blob_client
            .put_blob_vectored(tracking_root_blob_name, blob_guid, 0, chunks, &ctx.trace_id)
            .await
            .map_err(|e| {
                tracing::error!("Failed to store blob: {e}");
                S3Error::InternalError
            })?;
    } else {
        let blocks = split_chunks_into_blocks(chunks, block_size);
        let mut futures = Vec::with_capacity(blocks.len());

        for (block_num, block_chunks) in blocks.into_iter().enumerate() {
            let blob_client = blob_client.clone();

            let future = async move {
                blob_client
                    .put_blob_vectored(
                        tracking_root_blob_name,
                        blob_guid,
                        block_num as u32,
                        block_chunks,
                        &ctx.trace_id,
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!("Failed to store blob block {}: {e}", block_num);
                        S3Error::InternalError
                    })
            }
            .instrument(Span::current());
            futures.push(future);
        }

        let results: Vec<Result<(), S3Error>> = futures::future::join_all(futures).await;
        for result in results {
            result?;
        }
    }

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let etag = blob_guid.blob_id.simple().to_string();
    let version_id = gen_version_id();

    // Create object layout with calculated checksum
    let object_layout = ObjectLayout {
        version_id,
        block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
        timestamp,
        state: ObjectState::Normal(ObjectMetaData {
            blob_guid,
            core_meta_data: ObjectCoreMetaData {
                size,
                etag: etag.clone(),
                headers,
                checksum: calculated_checksum,
            },
        }),
    };

    // Serialize and store object metadata
    let object_layout_bytes: bytes::Bytes = to_bytes_in::<_, Error>(&object_layout, Vec::new())
        .map_err(|e| {
            tracing::error!("Failed to serialize object layout: {e}");
            S3Error::InternalError
        })?
        .into();

    // Store object metadata in NSS using the resolved bucket
    let nss_client = ctx.app.get_nss_rpc_client().await?;
    let resp = nss_rpc_retry!(
        nss_client,
        put_inode(
            &bucket.root_blob_name,
            &ctx.key,
            object_layout_bytes.clone(),
            Some(ctx.app.config.rpc_request_timeout()),
            &ctx.trace_id
        ),
        ctx.app,
        &ctx.trace_id
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to store object metadata: {e}");
        S3Error::from(e)
    })?;

    // Delete old object if it is an overwrite request
    // But skip deletion for multipart parts (keys containing '#') to avoid race conditions
    let old_object_bytes = match resp.result.unwrap() {
        put_inode_response::Result::Ok(res) => res,
        put_inode_response::Result::Err(e) => {
            tracing::error!("put_inode error: {}", e);
            return Err(S3Error::InternalError);
        }
    };
    let is_multipart_part = ctx.key.contains('#');
    if !old_object_bytes.is_empty() && !is_multipart_part {
        let old_object =
            rkyv::from_bytes::<ObjectLayout, Error>(&old_object_bytes).map_err(|e| {
                tracing::error!("Failed to deserialize old object layout: {e}");
                S3Error::InternalError
            })?;

        if let Ok(size) = old_object.size() {
            histogram!("object_size", "operation" => "delete_old_blob").record(size as f64);
        }
        let old_blob_guid = old_object.blob_guid().map_err(|e| {
            tracing::error!("Failed to get blob_guid from old object: {e}");
            S3Error::InternalError
        })?;

        // Only delete old blob if it's different from the new one
        if old_blob_guid != blob_guid {
            let num_blocks = old_object.num_blocks().map_err(|e| {
                tracing::error!("Failed to get num_blocks from old object: {e}");
                S3Error::InternalError
            })?;

            let blob_deletion = ctx.app.get_blob_deletion();

            // Send deletion request for each block
            let blob_location = old_object.get_blob_location().map_err(|e| {
                tracing::error!("Failed to get blob_location from old object: {e}");
                S3Error::InternalError
            })?;
            for block_number in 0..num_blocks {
                let request = BlobDeletionRequest {
                    tracking_root_blob_name: bucket.tracking_root_blob_name.clone(),
                    blob_guid: old_blob_guid,
                    block_number: block_number as u32,
                    location: blob_location,
                };

                if let Err(e) = blob_deletion.send(request).await {
                    tracing::warn!(
                        "Failed to send blob {old_blob_guid} block={block_number} for background deletion: {e}"
                    );
                }
            }
        } else {
            tracing::warn!(
                "Skipping deletion of old blob as it matches new blob GUID: {}",
                blob_guid.blob_id
            );
        }
    }

    tracing::debug!(
        "Successfully stored object {}/{} with size {}",
        ctx.bucket_name,
        ctx.key,
        size
    );

    Ok(HttpResponse::Ok()
        .insert_header((header::ETAG, etag))
        .insert_header(("X-Amz-Object-Size", size.to_string()))
        .finish())
}

/// Extract chunk signature context from request if it's a chunk-signed request
fn extract_chunk_signature_context(
    ctx: &ObjectRequestContext,
) -> Result<Option<(ChunkSignatureContext, Option<String>)>, S3Error> {
    // Extract auth from request
    let auth = match extract_authentication(&ctx.request) {
        Ok(Some(auth)) => auth,
        Ok(None) => return Ok(None),
        Err(_) => return Ok(None),
    };

    // Check if this is a streaming chunked request
    if auth.content_sha256 == STREAMING_PAYLOAD {
        let api_key = ctx.api_key.as_ref().ok_or(S3Error::InvalidAccessKeyId)?;

        // Create signing key
        let signing_key =
            get_signing_key_cached(auth.date, &api_key.data.secret_key, &ctx.app.config.region)
                .map_err(|_| S3Error::InternalError)?;

        let chunk_context = ChunkSignatureContext {
            signing_key,
            datetime: auth.date,
            scope_string: auth.scope_string.clone(),
        };

        // Take ownership of the signature string
        let seed_signature = auth.signature.to_string();
        return Ok(Some((chunk_context, Some(seed_signature))));
    }

    Ok(None)
}
