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
use data_types::object_layout::{MpuState, ObjectLayout, ObjectState};
use data_types::ovr_map::{
    BlockFetchPlan, OVR_ABORT_VALUE, OvrRow, OvrRowMap, block_fetch_plan, ovr_row_prefix,
    parse_ovr_abort_range, parse_ovr_row_block, zeros,
};
use data_types::{Bucket, DataBlobGuid, RoutingKey, TraceId};
use file_ops::parse_list_inodes_raw;
use futures::{StreamExt, TryStreamExt, stream};
use metrics_wrapper::histogram;
use rpc_client_common::nss_rpc_retry;
use serde::Deserialize;
use tracing::{Instrument, Span};

/// NSS listing page for row loads; the has_more loop, not the page
/// size, bounds coverage (never trust one clamped page).
const ROW_LOAD_PAGE: u32 = 1_000;

#[derive(Clone)]
struct ReadNamespace {
    app: Arc<AppState>,
    routing_key: RoutingKey,
    root_blob_name: String,
    bucket_name: String,
}

impl ReadNamespace {
    fn new(app: Arc<AppState>, bucket: &Bucket) -> Arc<Self> {
        Arc::new(Self {
            app,
            routing_key: bucket.routing_key,
            root_blob_name: bucket.root_blob_name.clone(),
            bucket_name: bucket.bucket_name.clone(),
        })
    }
}

#[derive(Clone)]
struct ReadSnapshot {
    namespace: Arc<ReadNamespace>,
    key: String,
    layout: ObjectLayout,
}

impl ReadSnapshot {
    fn new(namespace: Arc<ReadNamespace>, key: String, layout: ObjectLayout) -> Arc<Self> {
        Arc::new(Self {
            namespace,
            key,
            layout,
        })
    }

    async fn validate_base_miss(&self, trace_id: &TraceId) -> Result<(), S3Error> {
        let current = get_raw_object(
            &self.namespace.app,
            &self.namespace.routing_key,
            &self.namespace.root_blob_name,
            &self.namespace.bucket_name,
            &self.key,
            trace_id,
        )
        .await;
        match current {
            Ok(current) if same_committed_read_snapshot(&self.layout, &current) => Ok(()),
            Ok(current) => {
                tracing::warn!(
                    key = %self.key,
                    expected_version_id = %self.layout.version_id,
                    current_version_id = %current.version_id,
                    "base generation disappeared after the GET snapshot changed"
                );
                Err(S3Error::InternalError)
            }
            Err(S3Error::NoSuchKey | S3Error::NoSuchBucket) => {
                tracing::warn!(
                    key = %self.key,
                    expected_version_id = %self.layout.version_id,
                    "base generation disappeared after the GET snapshot was removed"
                );
                Err(S3Error::InternalError)
            }
            Err(error) => Err(error),
        }
    }
}

fn same_committed_read_snapshot(expected: &ObjectLayout, current: &ObjectLayout) -> bool {
    expected.version_id == current.version_id
        && expected.block_size == current.block_size
        && expected.blob_version == current.blob_version
        && expected.map_epoch() == current.map_epoch()
        && expected.state == current.state
}

/// The blob's committed `@ovr/` snapshot for `layout`, or `None` for an
/// unmapped blob. Cached per (blob_id, map_epoch): every change that can
/// affect resolution is published by a commit CAS that bumps the epoch.
async fn load_row_map(
    app: &Arc<AppState>,
    routing_key: &RoutingKey,
    root_blob_name: &str,
    layout: &ObjectLayout,
    trace_id: &TraceId,
) -> Result<Option<Arc<OvrRowMap>>, S3Error> {
    if !layout.is_mapped() {
        return Ok(None);
    }
    let blob_guid = layout.blob_guid()?;
    let map_epoch = layout.map_epoch();
    let cache_key = (blob_guid.blob_id, map_epoch);
    if let Some(map) = app.row_maps.get(&cache_key).await {
        return Ok(Some(map));
    }

    let prefix = ovr_row_prefix(&blob_guid.blob_id);
    let mut map = OvrRowMap::new(map_epoch);
    let mut start_after = String::new();
    loop {
        let nss_client = app.get_nss_rpc_client(routing_key).await?;
        let response = nss_rpc_retry!(
            nss_client,
            list_inodes(
                root_blob_name,
                ROW_LOAD_PAGE,
                &prefix,
                "",
                &start_after,
                true,
                Some(app.config.rpc_request_timeout()),
                trace_id
            ),
            app.as_ref(),
            routing_key,
            trace_id
        )
        .await?;
        let (page, has_more) = match parse_list_inodes_raw(response) {
            Ok(page) => page,
            Err(file_ops::NssError::NoSuchRootBlob) => return Err(S3Error::NoSuchBucket),
            Err(error) => {
                tracing::error!(%blob_guid, %error, "@ovr row listing failed");
                return Err(S3Error::InternalError);
            }
        };
        let last_key = page.last().map(|(key, _)| key.clone());
        for (key, value) in page {
            if let Some(block) = parse_ovr_row_block(&key) {
                let Some(row) = OvrRow::decode(&value) else {
                    tracing::error!(%blob_guid, %key, "malformed @ovr row");
                    return Err(S3Error::InternalError);
                };
                map.insert(block, row);
            } else if let Some((lo, hi)) = parse_ovr_abort_range(&key) {
                if value.as_ref() != OVR_ABORT_VALUE {
                    tracing::error!(%blob_guid, %key, "malformed @ovr abort record");
                    return Err(S3Error::InternalError);
                }
                map.add_aborted_range(lo, hi);
            } else {
                tracing::error!(%blob_guid, %key, "malformed @ovr key");
                return Err(S3Error::InternalError);
            }
        }
        let Some(last_key) = last_key else { break };
        if !has_more {
            break;
        }
        start_after = last_key;
    }

    let map = Arc::new(map);
    app.row_maps.insert(cache_key, map.clone()).await;
    Ok(Some(map))
}

/// Read one block at its exact committed identity. A `Hole` returns
/// zeros with no RPC; a row-committed miss is detected data loss (never
/// a hole); a base-version miss is a sparse hole only after the exact
/// committed namespace snapshot is revalidated.
#[allow(clippy::too_many_arguments)]
async fn read_block(
    blob_client: &BlobClient,
    snapshot: &ReadSnapshot,
    blob_guid: DataBlobGuid,
    block_number: u32,
    content_len: usize,
    block_size: usize,
    blob_location: BlobLocation,
    rows: Option<&OvrRowMap>,
    ceiling: u64,
    trace_id: &TraceId,
) -> Result<Bytes, S3Error> {
    let (version, read_len, miss_is_loss) =
        match block_fetch_plan(rows, block_number, ceiling, block_size, content_len) {
            BlockFetchPlan::Zeros => return Ok(zeros(content_len)),
            // The layout snapshot predates the row's last write; a fresh
            // GET re-reads both. Fail this one rather than guess.
            BlockFetchPlan::Stale => {
                tracing::warn!(%blob_guid, block_number, "row pair above the GET's ceiling");
                return Err(S3Error::InternalError);
            }
            BlockFetchPlan::Fetch {
                version,
                read_len,
                miss_is_loss,
            } => (version, read_len, miss_is_loss),
        };
    let mut body = Bytes::new();
    match blob_client
        .get_blob(
            blob_guid,
            block_number,
            version,
            read_len,
            blob_location,
            &mut body,
            trace_id,
        )
        .await
    {
        Ok(()) => {
            if body.len() > content_len {
                body = body.slice(..content_len);
            }
            Ok(body)
        }
        Err(crate::blob_storage::BlobStorageError::DataVg(
            volume_group_proxy::DataVgError::BlockNotFound,
        )) => {
            if miss_is_loss {
                tracing::error!(
                    %blob_guid,
                    block_number,
                    version,
                    "DATA LOSS: row-committed generation missing on every replica"
                );
                return Err(S3Error::InternalError);
            }
            snapshot.validate_base_miss(trace_id).await?;
            Ok(zeros(content_len))
        }
        Err(error) => Err(error.into()),
    }
}

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
    let object = get_raw_object(
        &ctx.app,
        &bucket.routing_key,
        &bucket.root_blob_name,
        &ctx.bucket_name,
        &ctx.key,
        &ctx.trace_id,
    )
    .await?;
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
    let read_namespace = ReadNamespace::new(app.clone(), bucket);
    let blob_client = app
        .get_blob_client(&bucket.routing_key)
        .await
        .map_err(|_| S3Error::InternalError)?;
    match object.state {
        ObjectState::Normal(ref _obj_data) => {
            let blob_guid = object.blob_guid()?;
            let num_blocks = object.num_blocks()?;
            let size = object.size()?;
            let block_size = object.block_size as usize;
            let blob_location = object.get_blob_location()?;
            let rows = load_row_map(
                &app,
                &bucket.routing_key,
                &bucket.root_blob_name,
                object,
                trace_id,
            )
            .await?;
            let snapshot = ReadSnapshot::new(read_namespace, key, object.clone());
            let body_stream = get_full_blob_stream(
                blob_client,
                snapshot,
                blob_guid,
                num_blocks,
                size,
                block_size,
                blob_location,
                rows,
                object.blob_version,
                *trace_id,
            )
            .await?;
            Ok((Box::pin(body_stream), size))
        }
        ObjectState::Symlink(_)
        | ObjectState::Special(_)
        | ObjectState::Directory(_)
        | ObjectState::Indirect(_) => {
            // Symlinks, special files, directory inodes and hardlink
            // indirections are FS-only schema variants. The S3 API
            // treats them as opaque and refuses to serve them as
            // object bodies; clients should treat them as a different
            // resource kind.
            Err(S3Error::InvalidObjectState)
        }
        ObjectState::Mpu(ref mpu_state) => match mpu_state {
            MpuState::Uploading => {
                tracing::warn!("invalid mpu state: Uploading");
                Err(S3Error::InvalidObjectState)
            }
            MpuState::Completed(core_meta_data) => {
                let mpu_prefix = mpu_get_part_prefix(key, object.version_id, 0);
                let mut mpus = list_raw_objects(
                    &app,
                    &bucket.routing_key,
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
                let read_namespace = read_namespace.clone();
                let mpu_stream = stream::iter(mpus_vec)
                    .then(move |(mpu_key, mpu_obj)| {
                        let blob_client = blob_client.clone();
                        let read_namespace = read_namespace.clone();
                        async move {
                            let blob_guid = mpu_obj.blob_guid()?;
                            let num_blocks = mpu_obj.num_blocks()?;
                            let mpu_size = mpu_obj.size()?;
                            let block_size = mpu_obj.block_size as usize;
                            let blob_location = mpu_obj.get_blob_location()?;
                            // MPU parts are uploaded whole and never
                            // overwritten in place (a FUSE overwrite
                            // republishes as a Normal layout with a fresh
                            // blob), so they carry no rows.
                            let snapshot =
                                ReadSnapshot::new(read_namespace, mpu_key, mpu_obj.clone());
                            get_full_blob_stream(
                                blob_client,
                                snapshot,
                                blob_guid,
                                num_blocks,
                                mpu_size,
                                block_size,
                                blob_location,
                                None,
                                mpu_obj.blob_version,
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
    let read_namespace = ReadNamespace::new(app.clone(), bucket);
    let blob_client = app
        .get_blob_client(&bucket.routing_key)
        .await
        .map_err(|_| S3Error::InternalError)?;
    let block_size = object.block_size as usize;
    match object.state {
        ObjectState::Normal(ref _obj_data) => {
            let blob_guid = object.blob_guid()?;
            let blob_location = object.get_blob_location()?;
            let object_size = object.size()?;
            let num_blocks = object.num_blocks()?;
            let rows = load_row_map(
                &app,
                &bucket.routing_key,
                &bucket.root_blob_name,
                object,
                trace_id,
            )
            .await?;
            let snapshot = ReadSnapshot::new(read_namespace, key, object.clone());
            let body_stream = get_range_blob_stream(
                blob_client,
                snapshot,
                blob_guid,
                block_size,
                object_size,
                num_blocks,
                range.start,
                range.end,
                blob_location,
                rows,
                object.blob_version,
                *trace_id,
            );
            Ok(Box::pin(body_stream))
        }
        ObjectState::Symlink(_)
        | ObjectState::Special(_)
        | ObjectState::Directory(_)
        | ObjectState::Indirect(_) => {
            // Range GETs on a symlink / special / directory / indirect
            // entry have no meaningful semantics in the S3 API
            // surface; reject.
            Err(S3Error::InvalidObjectState)
        }
        ObjectState::Mpu(ref mpu_state) => match mpu_state {
            MpuState::Uploading => {
                tracing::warn!("invalid mpu state: Uploading");
                Err(S3Error::InvalidObjectState)
            }
            MpuState::Completed { .. } => {
                let mpu_prefix = mpu_get_part_prefix(key, object.version_id, 0);
                let mpus = list_raw_objects(
                    &app,
                    &bucket.routing_key,
                    &bucket.root_blob_name,
                    10000,
                    &mpu_prefix,
                    "",
                    "",
                    false,
                    trace_id,
                )
                .await?;

                let mut mpu_blobs: Vec<(String, ObjectLayout, usize, usize)> = Vec::new();
                let mut obj_offset = 0;
                for (mpu_key, mpu_obj) in mpus {
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
                        mpu_blobs.push((mpu_key, mpu_obj, blob_start, blob_end));
                    }
                    obj_offset += mpu_size;
                }

                let trace_id = *trace_id;
                let read_namespace = read_namespace.clone();
                let body_stream = stream::iter(mpu_blobs)
                    .then(move |(mpu_key, mpu_obj, blob_start, blob_end)| {
                        let blob_client = blob_client.clone();
                        let read_namespace = read_namespace.clone();
                        async move {
                            let blob_guid = mpu_obj.blob_guid()?;
                            let part_size = mpu_obj.size()?;
                            let part_num_blocks = mpu_obj.num_blocks()?;
                            let block_size = mpu_obj.block_size as usize;
                            let blob_location = mpu_obj.get_blob_location()?;
                            let ceiling = mpu_obj.blob_version;
                            let snapshot = ReadSnapshot::new(read_namespace, mpu_key, mpu_obj);
                            Ok::<_, S3Error>(get_range_blob_stream(
                                blob_client,
                                snapshot,
                                blob_guid,
                                block_size,
                                part_size,
                                part_num_blocks,
                                blob_start,
                                blob_end,
                                blob_location,
                                None,
                                ceiling,
                                trace_id,
                            ))
                        }
                    })
                    .try_flatten();
                Ok(Box::pin(body_stream))
            }
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn get_full_blob_stream(
    blob_client: Arc<BlobClient>,
    snapshot: Arc<ReadSnapshot>,
    blob_guid: DataBlobGuid,
    num_blocks: usize,
    object_size: u64,
    block_size: usize,
    blob_location: BlobLocation,
    rows: Option<Arc<OvrRowMap>>,
    ceiling: u64,
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

    let first_block = read_block(
        &blob_client,
        &snapshot,
        blob_guid,
        0,
        first_block_len,
        block_size,
        blob_location,
        rows.as_deref(),
        ceiling,
        &trace_id,
    )
    .await
    .inspect_err(|error| {
        tracing::error!(%blob_guid, block_number = 0, %error, "failed to get blob");
    })?;

    if num_blocks == 1 {
        // Single block optimization - return immediately without streaming overhead
        return Ok(stream::once(async { Ok(first_block) }).boxed());
    }

    // Multi-block case: stream first block + remaining blocks
    let remaining_stream = stream::iter(1..num_blocks).then(move |i| {
        let blob_client = blob_client.clone();
        let snapshot = snapshot.clone();
        let rows = rows.clone();
        async move {
            let is_last_block = i == num_blocks - 1;
            let content_len = if is_last_block {
                (object_size as usize) - (block_size * i)
            } else {
                block_size
            };
            read_block(
                &blob_client,
                &snapshot,
                blob_guid,
                i as u32,
                content_len,
                block_size,
                blob_location,
                rows.as_deref(),
                ceiling,
                &trace_id,
            )
            .await
            .inspect_err(|error| {
                tracing::error!(%blob_guid, block_number = i, %error, "failed to get blob");
            })
        }
    });

    let full_stream = stream::once(async { Ok(first_block) }).chain(remaining_stream);
    Ok(full_stream.boxed())
}

#[allow(clippy::too_many_arguments)]
fn get_range_blob_stream(
    blob_client: Arc<BlobClient>,
    snapshot: Arc<ReadSnapshot>,
    blob_guid: DataBlobGuid,
    block_size: usize,
    object_size: u64,
    num_blocks: usize,
    start: usize,
    end: usize,
    blob_location: BlobLocation,
    rows: Option<Arc<OvrRowMap>>,
    ceiling: u64,
    trace_id: TraceId,
) -> impl stream::Stream<Item = Result<Bytes, S3Error>> {
    let start_block_i = start / block_size;
    let end_block_i = (end - 1) / block_size;
    let blob_offset: usize = block_size * start_block_i;

    let span = Span::current();
    futures::stream::iter(start_block_i..=end_block_i)
        .then(move |i| {
            let blob_client = blob_client.clone();
            let snapshot = snapshot.clone();
            let rows = rows.clone();
            async move {
                // For range reads, we always read full blocks and trim in the scan below
                // except for the last block which might be partial
                let is_last_block = i == num_blocks - 1;
                let content_len = if is_last_block {
                    (object_size as usize) - (block_size * i)
                } else {
                    block_size
                };
                read_block(
                    &blob_client,
                    &snapshot,
                    blob_guid,
                    i as u32,
                    content_len,
                    block_size,
                    blob_location,
                    rows.as_deref(),
                    ceiling,
                    &trace_id,
                )
                .await
                .inspect_err(|error| {
                    tracing::error!(%blob_guid, block_number = i, %error, "failed to get blob");
                })
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

#[cfg(test)]
mod tests {
    use super::same_committed_read_snapshot;
    use data_types::DataBlobGuid;
    use data_types::object_layout::{
        ObjectCoreMetaData, ObjectLayout, ObjectMetaData, ObjectState,
    };
    use uuid::Uuid;

    fn normal_layout() -> ObjectLayout {
        let blob_id = Uuid::now_v7();
        ObjectLayout {
            timestamp: 1,
            version_id: Uuid::now_v7(),
            block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
            blob_version: 3,
            fs_ext: None,
            state: ObjectState::Normal(ObjectMetaData {
                blob_guid: DataBlobGuid {
                    blob_id,
                    volume_id: 7,
                },
                core_meta_data: ObjectCoreMetaData {
                    size: u64::from(ObjectLayout::DEFAULT_BLOCK_SIZE) * 2,
                    etag: blob_id.simple().to_string(),
                    headers: Vec::new(),
                    checksum: None,
                },
            }),
        }
    }

    #[test]
    fn read_snapshot_ignores_prepare_only_and_posix_changes() {
        let expected = normal_layout();
        let mut current = expected.clone();
        current.timestamp += 1;
        current.set_next_version(expected.blob_version + 2);
        current.set_pending_append(Some((2, 4)));

        assert!(same_committed_read_snapshot(&expected, &current));
    }

    #[test]
    fn read_snapshot_rejects_committed_data_changes() {
        let expected = normal_layout();

        let mut current = expected.clone();
        current.version_id = Uuid::now_v7();
        assert!(!same_committed_read_snapshot(&expected, &current));

        let mut current = expected.clone();
        current.blob_version += 1;
        assert!(!same_committed_read_snapshot(&expected, &current));

        let mut current = expected.clone();
        current.set_map_epoch(4);
        assert!(!same_committed_read_snapshot(&expected, &current));

        let metadata = match &mut current.state {
            ObjectState::Normal(metadata) => Some(metadata),
            _ => None,
        }
        .expect("normal layout helper must create a normal state");
        metadata.core_meta_data.size += 1;
        assert!(!same_committed_read_snapshot(&expected, &current));
    }
}
