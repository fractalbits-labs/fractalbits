use std::sync::Arc;

use crate::{AppState, blob_storage::BlobLocation};
use crate::{
    BlobClient,
    handler::{
        ObjectRequestContext,
        common::{
            get_raw_object, list_raw_objects, mpu_get_part_prefix, object_headers,
            s3_error::S3Error, xheader,
        },
    },
};
use actix_web::{
    HttpResponse,
    http::{StatusCode, header, header::HeaderValue},
    web::Query,
};
use bytes::Bytes;
use data_types::DataBlobGuid;
use data_types::object_layout::{MpuState, ObjectLayout, ObjectState};
use data_types::{Bucket, TraceId};
use futures::{StreamExt, TryStreamExt, stream};
use metrics_wrapper::histogram;
use serde::Deserialize;
use tracing::{Instrument, Span};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct QueryOpts {
    #[serde(rename(deserialize = "partNumber"))]
    part_number: Option<u32>,
    #[allow(dead_code)]
    #[serde(rename(deserialize = "versionId"))]
    version_id: Option<String>,
    response_cache_control: Option<String>,
    response_content_disposition: Option<String>,
    response_content_encoding: Option<String>,
    response_content_language: Option<String>,
    response_content_type: Option<String>,
    response_expires: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct HeaderOpts<'a> {
    pub if_match: Option<&'a HeaderValue>,
    pub if_modified_since: Option<&'a HeaderValue>,
    pub if_none_match: Option<&'a HeaderValue>,
    pub if_unmodified_since: Option<&'a HeaderValue>,
    pub range: Option<&'a HeaderValue>,
    pub x_amz_server_side_encryption_customer_algorithm: Option<&'a HeaderValue>,
    pub x_amz_server_side_encryption_customer_key: Option<&'a HeaderValue>,
    pub x_amz_server_side_encryption_customer_key_md5: Option<&'a HeaderValue>,
    pub x_amz_request_payer: Option<&'a HeaderValue>,
    pub x_amz_expected_bucket_owner: Option<&'a HeaderValue>,
    pub x_amz_checksum_mode_enabled: bool,
}

impl<'a> HeaderOpts<'a> {
    pub fn from_headers(headers: &'a header::HeaderMap) -> Result<Self, S3Error> {
        Ok(Self {
            if_match: headers.get(header::IF_MATCH),
            if_modified_since: headers.get(header::IF_MODIFIED_SINCE),
            if_none_match: headers.get(header::IF_NONE_MATCH),
            if_unmodified_since: headers.get(header::IF_UNMODIFIED_SINCE),
            range: headers.get(header::RANGE),
            x_amz_server_side_encryption_customer_algorithm: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM.as_str()),
            x_amz_server_side_encryption_customer_key: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY.as_str()),
            x_amz_server_side_encryption_customer_key_md5: headers
                .get(xheader::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5.as_str()),
            x_amz_request_payer: headers.get(xheader::X_AMZ_REQUEST_PAYER.as_str()),
            x_amz_expected_bucket_owner: headers.get(xheader::X_AMZ_EXPECTED_BUCKET_OWNER.as_str()),
            x_amz_checksum_mode_enabled: headers
                .get(xheader::X_AMZ_CHECKSUM_MODE.as_str())
                .map(|x| x == "ENABLED")
                .unwrap_or(false),
        })
    }
}

pub async fn get_object_handler(ctx: ObjectRequestContext) -> Result<HttpResponse, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let query_opts = Query::<QueryOpts>::from_query(ctx.request.query_string())
        .map_err(|_| S3Error::UnsupportedArgument)?;

    // Extract header options from headers
    let header_opts = HeaderOpts::from_headers(ctx.request.headers())?;
    let checksum_mode_enabled = header_opts.x_amz_checksum_mode_enabled;

    // Get the raw object
    let object = get_raw_object(&ctx.app, &bucket.root_blob_name, &ctx.key, &ctx.trace_id).await?;
    let total_size = object.size()?;
    histogram!("object_size", "operation" => "get").record(total_size as f64);

    // Parse range header
    let range = parse_range_header(header_opts.range, total_size)?;

    match (query_opts.part_number, range) {
        (_, None) => {
            // Full object request with streaming
            let (body_stream, body_size) = get_object_content(
                ctx.app,
                &bucket,
                &object,
                ctx.key,
                query_opts.part_number,
                &ctx.trace_id,
            )
            .await?;

            // Build streaming response
            let mut response = HttpResponse::Ok();
            object_headers(&mut response, &object, checksum_mode_enabled)?;
            override_headers(&mut response, &query_opts)?;

            // Convert the stream to actix-web compatible format
            let actix_stream = body_stream.map(|result| {
                result.map_err(|e| {
                    tracing::error!("Stream error: {e:?}");
                    std::io::Error::other(format!("Stream error: {e:?}"))
                })
            });

            Ok(response
                .no_chunking(body_size)
                .body(actix_web::body::SizedStream::new(body_size, actix_stream)))
        }
        (None, Some(range)) => {
            // Range request with streaming
            let body_stream =
                get_object_range_content(ctx.app, &bucket, &object, ctx.key, &range, &ctx.trace_id)
                    .await?;

            let range_length = range.end - range.start;
            let content_range = format!("bytes {}-{}/{}", range.start, range.end - 1, total_size);

            // Build response for partial content
            let mut response = HttpResponse::build(StatusCode::PARTIAL_CONTENT);
            object_headers(&mut response, &object, false)?;
            response.insert_header((header::CONTENT_RANGE, content_range));
            response.insert_header((header::CONTENT_LENGTH, range_length.to_string()));
            override_headers(&mut response, &query_opts)?;

            // Convert the stream to actix-web compatible format
            let actix_stream = body_stream.map(|result| {
                result.map_err(|e| {
                    tracing::error!("Stream error: {e:?}");
                    std::io::Error::other(format!("Stream error: {e:?}"))
                })
            });

            // Use streaming response
            Ok(response.streaming(actix_stream))
        }
        (Some(_), Some(_)) => Err(S3Error::InvalidArgument1),
    }
}

pub fn override_headers(
    resp: &mut actix_web::HttpResponseBuilder,
    query_opts: &QueryOpts,
) -> Result<(), S3Error> {
    // override headers, see https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
    let overrides = [
        (header::CACHE_CONTROL, &query_opts.response_cache_control),
        (
            header::CONTENT_DISPOSITION,
            &query_opts.response_content_disposition,
        ),
        (
            header::CONTENT_ENCODING,
            &query_opts.response_content_encoding,
        ),
        (
            header::CONTENT_LANGUAGE,
            &query_opts.response_content_language,
        ),
        (header::CONTENT_TYPE, &query_opts.response_content_type),
        (header::EXPIRES, &query_opts.response_expires),
    ];

    for (hdr, val_opt) in overrides {
        if let Some(val) = val_opt {
            resp.insert_header((hdr, val.as_str()));
        }
    }

    Ok(())
}

pub async fn get_object_content(
    app: Arc<AppState>,
    bucket: &Bucket,
    object: &ObjectLayout,
    key: String,
    part_number: Option<u32>,
    trace_id: &TraceId,
) -> Result<
    (
        std::pin::Pin<Box<dyn stream::Stream<Item = Result<Bytes, S3Error>> + Send>>,
        u64,
    ),
    S3Error,
> {
    let blob_client = app
        .get_blob_client()
        .await
        .map_err(|_| S3Error::InternalError)?;
    match object.state {
        ObjectState::Normal(ref _obj_data) => {
            let blob_guid = object.blob_guid()?;
            let num_blocks = object.num_blocks()?;
            let size = object.size()?;
            let block_size = object.block_size as usize;
            let blob_location = object.get_blob_location()?;
            let body_stream = get_full_blob_stream(
                blob_client,
                blob_guid,
                num_blocks,
                size,
                block_size,
                blob_location,
                *trace_id,
            )
            .await?;
            Ok((Box::pin(body_stream), size))
        }
        ObjectState::Mpu(ref mpu_state) => match mpu_state {
            MpuState::Uploading => {
                tracing::warn!("invalid mpu state: Uploading");
                Err(S3Error::InvalidObjectState)
            }
            MpuState::Aborted => {
                tracing::warn!("invalid mpu state: Aborted");
                Err(S3Error::InvalidObjectState)
            }
            MpuState::Completed(core_meta_data) => {
                let mpu_prefix = mpu_get_part_prefix(key, 0);
                let mut mpus = list_raw_objects(
                    &app,
                    &bucket.root_blob_name,
                    10000,
                    &mpu_prefix,
                    "",
                    "",
                    false,
                    trace_id,
                )
                .await?;
                // Do filtering if there is part_number option
                let (mpus_vec, body_size) = match part_number {
                    None => (mpus.into_iter().collect::<Vec<_>>(), core_meta_data.size),
                    Some(n) => {
                        let mpu_obj = mpus.swap_remove(n as usize - 1);
                        let mpu_size = mpu_obj.1.size()?;
                        (vec![mpu_obj], mpu_size)
                    }
                };

                // Create a stream that concatenates all multipart streams
                // Following the axum pattern for multipart streaming
                let trace_id = *trace_id;
                let mpu_stream = stream::iter(mpus_vec)
                    .then(move |(_key, mpu_obj)| {
                        let blob_client = blob_client.clone();
                        async move {
                            let blob_guid = mpu_obj.blob_guid()?;
                            let num_blocks = mpu_obj.num_blocks()?;
                            let mpu_size = mpu_obj.size()?;
                            let block_size = mpu_obj.block_size as usize;
                            let blob_location = mpu_obj.get_blob_location()?;
                            get_full_blob_stream(
                                blob_client,
                                blob_guid,
                                num_blocks,
                                mpu_size,
                                block_size,
                                blob_location,
                                trace_id,
                            )
                            .await
                        }
                    })
                    .try_flatten();

                Ok((Box::pin(mpu_stream), body_size))
            }
        },
    }
}

async fn get_object_range_content(
    app: Arc<AppState>,
    bucket: &Bucket,
    object: &ObjectLayout,
    key: String,
    range: &std::ops::Range<usize>,
    trace_id: &TraceId,
) -> Result<std::pin::Pin<Box<dyn stream::Stream<Item = Result<Bytes, S3Error>> + Send>>, S3Error> {
    let blob_client = app
        .get_blob_client()
        .await
        .map_err(|_| S3Error::InternalError)?;
    let block_size = object.block_size as usize;
    match object.state {
        ObjectState::Normal(ref _obj_data) => {
            let blob_guid = object.blob_guid()?;
            let blob_location = object.get_blob_location()?;
            let object_size = object.size()?;
            let num_blocks = object.num_blocks()?;
            let body_stream = get_range_blob_stream(
                blob_client,
                blob_guid,
                block_size,
                object_size,
                num_blocks,
                range.start,
                range.end,
                blob_location,
                *trace_id,
            );
            Ok(Box::pin(body_stream))
        }
        ObjectState::Mpu(ref mpu_state) => match mpu_state {
            MpuState::Uploading => {
                tracing::warn!("invalid mpu state: Uploading");
                Err(S3Error::InvalidObjectState)
            }
            MpuState::Aborted => {
                tracing::warn!("invalid mpu state: Aborted");
                Err(S3Error::InvalidObjectState)
            }
            MpuState::Completed { .. } => {
                let mpu_prefix = mpu_get_part_prefix(key, 0);
                let mpus = list_raw_objects(
                    &app,
                    &bucket.root_blob_name,
                    10000,
                    &mpu_prefix,
                    "",
                    "",
                    false,
                    trace_id,
                )
                .await?;

                let mut mpu_blobs: Vec<(DataBlobGuid, u64, usize, usize, usize)> = Vec::new();
                let mut obj_offset = 0;
                for (_mpu_key, mpu_obj) in mpus {
                    let mpu_size = mpu_obj.size()? as usize;
                    if obj_offset >= range.end {
                        break;
                    }
                    // with intersection
                    if obj_offset < range.end && obj_offset + mpu_size > range.start {
                        let blob_start = range.start.saturating_sub(obj_offset);
                        let blob_end = if range.end > obj_offset + mpu_size {
                            mpu_size - blob_start
                        } else {
                            range.end - obj_offset
                        };
                        let part_size = mpu_obj.size()?;
                        let part_num_blocks = mpu_obj.num_blocks()?;
                        mpu_blobs.push((
                            mpu_obj.blob_guid()?,
                            part_size,
                            part_num_blocks,
                            blob_start,
                            blob_end,
                        ));
                    }
                    obj_offset += mpu_size;
                }

                let trace_id = *trace_id;
                let body_stream = stream::iter(mpu_blobs.into_iter())
                    .then(
                        move |(blob_guid, part_size, part_num_blocks, blob_start, blob_end)| {
                            let blob_client = blob_client.clone();
                            async move {
                                // Note: In MPU range case, we need to determine blob_location from the specific MPU object
                                // For now, assume all MPU parts use S3 storage (large objects)
                                Ok::<_, S3Error>(get_range_blob_stream(
                                    blob_client,
                                    blob_guid,
                                    block_size,
                                    part_size,
                                    part_num_blocks,
                                    blob_start,
                                    blob_end,
                                    BlobLocation::S3,
                                    trace_id,
                                ))
                            }
                        },
                    )
                    .try_flatten();
                Ok(Box::pin(body_stream))
            }
        },
    }
}

async fn get_full_blob_stream(
    blob_client: Arc<BlobClient>,
    blob_guid: DataBlobGuid,
    num_blocks: usize,
    object_size: u64,
    block_size: usize,
    blob_location: BlobLocation,
    trace_id: TraceId,
) -> Result<impl stream::Stream<Item = Result<Bytes, S3Error>>, S3Error> {
    if num_blocks == 0 {
        return Ok(stream::empty().boxed());
    }

    let first_block_len = if num_blocks == 1 {
        object_size as usize
    } else {
        block_size
    };

    // Get the first block
    let mut first_block = Bytes::new();
    blob_client
        .get_blob(
            blob_guid,
            0,
            first_block_len,
            blob_location,
            &mut first_block,
            &trace_id,
        )
        .await
        .map_err(|e| {
            tracing::error!(%blob_guid, block_number=0, error=?e, "failed to get blob");
            S3Error::from(e)
        })?;

    if num_blocks == 1 {
        // Single block optimization - return immediately without streaming overhead
        return Ok(stream::once(async { Ok(first_block) }).boxed());
    }

    // Multi-block case: stream first block + remaining blocks
    let remaining_stream = stream::iter(1..num_blocks).then(move |i| {
        let blob_client = blob_client.clone();
        async move {
            let is_last_block = i == num_blocks - 1;
            let content_len = if is_last_block {
                (object_size as usize) - (block_size * i)
            } else {
                block_size
            };
            let mut block = Bytes::new();
            match blob_client
                .get_blob(
                    blob_guid,
                    i as u32,
                    content_len,
                    blob_location,
                    &mut block,
                    &trace_id,
                )
                .await
            {
                Err(e) => {
                    tracing::error!(%blob_guid, block_number=i, error=?e, "failed to get blob");
                    Err(S3Error::from(e))
                }
                Ok(_) => Ok(block),
            }
        }
    });

    let full_stream = stream::once(async { Ok(first_block) }).chain(remaining_stream);
    Ok(full_stream.boxed())
}

#[allow(clippy::too_many_arguments)]
fn get_range_blob_stream(
    blob_client: Arc<BlobClient>,
    blob_guid: DataBlobGuid,
    block_size: usize,
    object_size: u64,
    num_blocks: usize,
    start: usize,
    end: usize,
    blob_location: BlobLocation,
    trace_id: TraceId,
) -> impl stream::Stream<Item = Result<Bytes, S3Error>> {
    let start_block_i = start / block_size;
    let end_block_i = (end - 1) / block_size;
    let blob_offset: usize = block_size * start_block_i;

    let span = Span::current();
    futures::stream::iter(start_block_i..=end_block_i)
        .then(move |i| {
            let blob_client = blob_client.clone();
            async move {
                let mut block = Bytes::new();
                // For range reads, we always read full blocks and trim in the scan below
                // except for the last block which might be partial
                let is_last_block = i == num_blocks - 1;
                let content_len = if is_last_block {
                    (object_size as usize) - (block_size * i)
                } else {
                    block_size
                };
                match blob_client
                    .get_blob(
                        blob_guid,
                        i as u32,
                        content_len,
                        blob_location,
                        &mut block,
                        &trace_id,
                    )
                    .await
                {
                    Err(e) => {
                        tracing::error!(%blob_guid, block_number=i, error=?e, "failed to get blob");
                        Err(S3Error::from(e))
                    }
                    Ok(_) => Ok(block),
                }
            }
            .instrument(span.clone())
        })
        .scan(blob_offset, move |chunk_offset, chunk| {
            let r = match chunk {
                Ok(chunk_bytes) => {
                    let chunk_len = chunk_bytes.len();
                    let r = if *chunk_offset >= end {
                        // The current chunk is after the part we want to read.
                        // Returning None here will stop the scan, the rest of the
                        // stream will be ignored
                        None
                    } else if *chunk_offset + chunk_len <= start {
                        // The current chunk is before the part we want to read.
                        // We return a None that will be removed by the filter_map
                        // below.
                        Some(None)
                    } else {
                        // The chunk has an intersection with the requested range
                        let start_in_chunk = start.saturating_sub(*chunk_offset);
                        let end_in_chunk = if *chunk_offset + chunk_len < end {
                            chunk_len
                        } else {
                            end - *chunk_offset
                        };
                        Some(Some(Ok(chunk_bytes.slice(start_in_chunk..end_in_chunk))))
                    };
                    *chunk_offset += chunk_bytes.len();
                    r
                }
                Err(e) => Some(Some(Err(e))),
            };
            futures::future::ready(r)
        })
        .filter_map(futures::future::ready)
}

pub async fn get_object_content_as_bytes(
    app: Arc<AppState>,
    bucket: &Bucket,
    object: &ObjectLayout,
    key: String,
    part_number: Option<u32>,
    trace_id: &TraceId,
) -> Result<(Bytes, u64), S3Error> {
    let (stream, size) =
        get_object_content(app, bucket, object, key, part_number, trace_id).await?;

    // Collect the stream into bytes
    let stream_bytes = stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|_| S3Error::InternalError)?;

    let mut full_bytes = Bytes::new();
    for bytes in stream_bytes {
        full_bytes = [full_bytes, bytes].concat().into();
    }

    Ok((full_bytes, size))
}

fn parse_range_header(
    range_header: Option<&HeaderValue>,
    total_size: u64,
) -> Result<Option<std::ops::Range<usize>>, S3Error> {
    let range = match range_header {
        Some(range) => {
            let range_str = range.to_str()?;
            let mut ranges = http_range::HttpRange::parse(range_str, total_size)?;
            if ranges.len() > 1 {
                // Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
                tracing::debug!("Found more than one ranges: {range_str}");
                return Err(S3Error::InvalidRange);
            } else {
                ranges.pop().map(|http_range| {
                    http_range.start as usize..(http_range.start + http_range.length) as usize
                })
            }
        }
        None => None,
    };
    Ok(range)
}
