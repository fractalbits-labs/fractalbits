use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    AppState,
    blob_storage::{BlobLocation, BlobStorageError},
};
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
use data_types::block_map::{BlockMap, RangeState, block_reader_key, bmap_chunk_key};
use data_types::object_layout::{MpuState, ObjectLayout, ObjectState};
use data_types::{Bucket, DataBlobGuid, RoutingKey, TraceId};
use file_ops::{NssError, parse_delete_inode, parse_put_inode_cas};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use metrics_wrapper::histogram;
use nss_codec::get_inode_response;
use pin_project_lite::pin_project;
use rpc_client_common::nss_rpc_retry;
use serde::Deserialize;
use tokio::sync::oneshot;
use tracing::{Instrument, Span};
use uuid::Uuid;

const BLOCK_MAP_CHUNK_CONCURRENCY: usize = 16;
const BLOCK_READER_LEASE_RPC_CONCURRENCY: usize = 32;
const BLOCK_READER_LEASE_MIN_TTL: Duration = Duration::from_secs(15);
const BLOCK_READER_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(5);

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
    validate_get_part_number(query_opts.part_number)?;

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

#[derive(Clone)]
struct BlockReaderLeaseRecord {
    blob_guid: DataBlobGuid,
    blob_version: u64,
    key: String,
    value: Bytes,
}

struct BlockReaderLeases {
    shutdown_tx: Option<oneshot::Sender<bool>>,
    lease_lost_rx: Option<oneshot::Receiver<()>>,
    confirmed_until_ms: Option<Arc<AtomicU64>>,
}

impl Drop for BlockReaderLeases {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(true);
        }
    }
}

impl BlockReaderLeases {
    fn abandon(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(false);
        }
    }
}

pin_project! {
    struct BlockReaderLeaseStream<S> {
        #[pin]
        stream: S,
        #[pin]
        lease_lost_rx: Option<oneshot::Receiver<()>>,
        #[pin]
        expiry_sleep: Option<tokio::time::Sleep>,
        leases: BlockReaderLeases,
        terminated: bool,
    }
}

impl<S> BlockReaderLeaseStream<S> {
    fn new(stream: S, mut leases: BlockReaderLeases) -> Self {
        let expiry_sleep = leases.confirmed_until_ms.as_ref().map(|deadline| {
            tokio::time::sleep(Duration::from_millis(
                deadline
                    .load(Ordering::Acquire)
                    .saturating_sub(wall_clock_ms()),
            ))
        });
        Self {
            stream,
            lease_lost_rx: leases.lease_lost_rx.take(),
            expiry_sleep,
            leases,
            terminated: false,
        }
    }
}

impl<S> Stream for BlockReaderLeaseStream<S>
where
    S: Stream<Item = Result<Bytes, S3Error>>,
{
    type Item = Result<Bytes, S3Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.terminated {
            return Poll::Ready(None);
        }

        let expiry_woke = this
            .expiry_sleep
            .as_mut()
            .as_pin_mut()
            .is_some_and(|sleep| sleep.poll(cx).is_ready());
        let now_ms = wall_clock_ms();
        let confirmed_until_ms = this
            .leases
            .confirmed_until_ms
            .as_ref()
            .map(|deadline| deadline.load(Ordering::Acquire));
        let expired = confirmed_until_ms.is_some_and(|deadline| deadline <= now_ms);
        if expiry_woke
            && !expired
            && let (Some(deadline), Some(mut sleep)) =
                (confirmed_until_ms, this.expiry_sleep.as_mut().as_pin_mut())
        {
            sleep.as_mut().reset(
                tokio::time::Instant::now()
                    + Duration::from_millis(deadline.saturating_sub(now_ms)),
            );
        }
        let renewal_failed = this
            .lease_lost_rx
            .as_mut()
            .as_pin_mut()
            .is_some_and(|receiver| receiver.poll(cx).is_ready());
        if expired || renewal_failed {
            *this.terminated = true;
            this.leases.abandon();
            return Poll::Ready(Some(Err(S3Error::InternalError)));
        }

        this.stream.poll_next(cx)
    }
}

fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn block_reader_lease_value(expires_at_ms: u64, blob_version: u64) -> Bytes {
    let mut value = [0_u8; 16];
    value[..8].copy_from_slice(&expires_at_ms.to_le_bytes());
    value[8..].copy_from_slice(&blob_version.to_le_bytes());
    Bytes::copy_from_slice(&value)
}

fn block_reader_lease_ttl(app: &AppState) -> Duration {
    let request_guard = app
        .config
        .rpc_request_timeout()
        .max(app.config.http_request_timeout())
        .saturating_add(BLOCK_READER_LEASE_RENEW_INTERVAL.saturating_mul(2));
    BLOCK_READER_LEASE_MIN_TTL.max(request_guard)
}

fn block_reader_lease_expiry(value: &Bytes) -> Option<u64> {
    let value: &[u8; 16] = value.as_ref().try_into().ok()?;
    Some(u64::from_le_bytes(value[..8].try_into().ok()?))
}

fn block_reader_lease_is_returnable(expires_at_ms: u64, now_ms: u64) -> bool {
    let renewal_margin_ms = BLOCK_READER_LEASE_RENEW_INTERVAL
        .saturating_mul(2)
        .as_millis() as u64;
    expires_at_ms > now_ms.saturating_add(renewal_margin_ms)
}

fn deduplicate_blob_versions(blob_versions: &[(DataBlobGuid, u64)]) -> Vec<(DataBlobGuid, u64)> {
    let mut seen = HashSet::with_capacity(blob_versions.len());
    blob_versions
        .iter()
        .copied()
        .filter(|blob_version| seen.insert(*blob_version))
        .collect()
}

enum BlockReaderLeaseCasError {
    Conflict,
    Other(S3Error),
}

impl BlockReaderLeaseCasError {
    fn into_s3_error(self) -> S3Error {
        match self {
            Self::Conflict => S3Error::InternalError,
            Self::Other(error) => error,
        }
    }
}

async fn compare_and_swap_block_reader_lease(
    app: &AppState,
    routing_key: &RoutingKey,
    root_blob_name: &str,
    record: &BlockReaderLeaseRecord,
    new_value: &Bytes,
    trace_id: &TraceId,
) -> Result<(), BlockReaderLeaseCasError> {
    let nss_client = app
        .get_nss_rpc_client(routing_key)
        .await
        .map_err(BlockReaderLeaseCasError::Other)?;
    let response = nss_rpc_retry!(
        nss_client,
        put_inode_cas(
            root_blob_name,
            &record.key,
            new_value.clone(),
            record.value.clone(),
            Some(app.config.rpc_request_timeout()),
            trace_id
        ),
        app,
        routing_key,
        trace_id
    )
    .await
    .map_err(|error| BlockReaderLeaseCasError::Other(error.into()))?;
    match parse_put_inode_cas(response) {
        Ok(previous) if previous == record.value => Ok(()),
        Ok(previous) => {
            tracing::error!(
                key = %record.key,
                expected_len = record.value.len(),
                actual_len = previous.len(),
                "block reader lease CAS returned an unexpected prior value"
            );
            Err(BlockReaderLeaseCasError::Other(S3Error::InternalError))
        }
        Err(NssError::CasConflict(current)) => {
            tracing::warn!(
                key = %record.key,
                expected_len = record.value.len(),
                actual_len = current.len(),
                "block reader lease ownership lost"
            );
            Err(BlockReaderLeaseCasError::Conflict)
        }
        Err(error) => Err(BlockReaderLeaseCasError::Other(error.into())),
    }
}

async fn delete_block_reader_lease(
    app: &AppState,
    routing_key: &RoutingKey,
    root_blob_name: &str,
    record: &BlockReaderLeaseRecord,
    trace_id: &TraceId,
) -> Result<(), S3Error> {
    let expired = block_reader_lease_value(0, record.blob_version);
    match compare_and_swap_block_reader_lease(
        app,
        routing_key,
        root_blob_name,
        record,
        &expired,
        trace_id,
    )
    .await
    {
        Ok(()) => {}
        Err(BlockReaderLeaseCasError::Conflict) => return Ok(()),
        Err(BlockReaderLeaseCasError::Other(error)) => return Err(error),
    }

    let nss_client = app.get_nss_rpc_client(routing_key).await?;
    let response = nss_rpc_retry!(
        nss_client,
        delete_inode(
            root_blob_name,
            &record.key,
            Some(app.config.rpc_request_timeout()),
            trace_id
        ),
        app,
        routing_key,
        trace_id
    )
    .await?;
    drop(parse_delete_inode(response)?);
    Ok(())
}

async fn renew_block_reader_leases(
    app: &AppState,
    routing_key: &RoutingKey,
    root_blob_name: &str,
    records: &mut Vec<BlockReaderLeaseRecord>,
) -> Result<u64, ()> {
    let ttl_ms = block_reader_lease_ttl(app).as_millis() as u64;
    let trace_id = TraceId::new();
    let renewals = stream::iter(records.iter().cloned())
        .map(|mut record| async move {
            let now_ms = wall_clock_ms();
            if block_reader_lease_expiry(&record.value)
                .is_none_or(|expires_at_ms| expires_at_ms <= now_ms)
            {
                tracing::warn!(key = %record.key, "expired block reader lease cannot renew");
                return Err(());
            }
            let expires_at_ms = now_ms.saturating_add(ttl_ms);
            let new_value = block_reader_lease_value(expires_at_ms, record.blob_version);
            match compare_and_swap_block_reader_lease(
                app,
                routing_key,
                root_blob_name,
                &record,
                &new_value,
                &trace_id,
            )
            .await
            {
                Ok(()) => {
                    record.value = new_value;
                    Ok((record, expires_at_ms))
                }
                Err(error) => {
                    let error = error.into_s3_error();
                    tracing::warn!(
                        blob_guid = %record.blob_guid,
                        key = %record.key,
                        %error,
                        "block reader lease renewal failed"
                    );
                    Err(())
                }
            }
        })
        .buffer_unordered(BLOCK_READER_LEASE_RPC_CONCURRENCY)
        .collect::<Vec<_>>();
    match tokio::time::timeout(block_reader_lease_ttl(app), renewals).await {
        Ok(results) => {
            let mut renewed = Vec::with_capacity(results.len());
            let mut confirmed_until_ms = u64::MAX;
            for result in results {
                match result {
                    Ok((record, expires_at_ms)) => {
                        renewed.push(record);
                        confirmed_until_ms = confirmed_until_ms.min(expires_at_ms);
                    }
                    Err(()) => return Err(()),
                }
            }
            if !block_reader_lease_is_returnable(confirmed_until_ms, wall_clock_ms()) {
                return Err(());
            }
            *records = renewed;
            Ok(confirmed_until_ms)
        }
        Err(_) => {
            tracing::warn!("block reader lease renewal batch timed out");
            Err(())
        }
    }
}

async fn remove_block_reader_leases(
    app: &AppState,
    routing_key: &RoutingKey,
    root_blob_name: &str,
    records: &[BlockReaderLeaseRecord],
) {
    let trace_id = TraceId::new();
    stream::iter(records.iter().cloned())
        .for_each_concurrent(BLOCK_READER_LEASE_RPC_CONCURRENCY, |record| async move {
            if let Err(error) =
                delete_block_reader_lease(app, routing_key, root_blob_name, &record, &trace_id)
                    .await
            {
                tracing::warn!(
                    blob_guid = %record.blob_guid,
                    key = %record.key,
                    %error,
                    "block reader lease cleanup failed"
                );
            }
        })
        .await;
}

async fn maintain_block_reader_leases(
    app: Arc<AppState>,
    routing_key: RoutingKey,
    root_blob_name: String,
    mut records: Vec<BlockReaderLeaseRecord>,
    confirmed_until_ms: Arc<AtomicU64>,
    lease_lost_tx: oneshot::Sender<()>,
    mut shutdown_rx: oneshot::Receiver<bool>,
) {
    let cleanup = loop {
        tokio::select! {
            biased;
            cleanup = &mut shutdown_rx => break cleanup.unwrap_or(false),
            _ = tokio::time::sleep(BLOCK_READER_LEASE_RENEW_INTERVAL) => {
                match renew_block_reader_leases(
                    &app,
                    &routing_key,
                    &root_blob_name,
                    &mut records,
                )
                .await
                {
                    Ok(expires_at_ms) => {
                        confirmed_until_ms.store(expires_at_ms, Ordering::Release);
                    }
                    Err(()) => {
                        let _ = lease_lost_tx.send(());
                        return;
                    }
                }
            }
        }
    };
    if cleanup {
        remove_block_reader_leases(&app, &routing_key, &root_blob_name, &records).await;
    }
}

async fn acquire_block_reader_leases(
    app: Arc<AppState>,
    routing_key: &RoutingKey,
    root_blob_name: &str,
    blob_versions: &[(DataBlobGuid, u64)],
    trace_id: &TraceId,
) -> Result<BlockReaderLeases, S3Error> {
    let records = deduplicate_blob_versions(blob_versions)
        .into_iter()
        .map(|(blob_guid, blob_version)| BlockReaderLeaseRecord {
            blob_guid,
            blob_version,
            key: block_reader_key(&blob_guid, Uuid::new_v4()),
            value: Bytes::new(),
        })
        .collect::<Vec<_>>();
    if records.is_empty() {
        return Ok(BlockReaderLeases {
            shutdown_tx: None,
            lease_lost_rx: None,
            confirmed_until_ms: None,
        });
    }

    let ttl_ms = block_reader_lease_ttl(&app).as_millis() as u64;
    let app_ref = app.as_ref();
    let results = stream::iter(records)
        .map(|mut record| async move {
            let expires_at_ms = wall_clock_ms().saturating_add(ttl_ms);
            let new_value = block_reader_lease_value(expires_at_ms, record.blob_version);
            let result = compare_and_swap_block_reader_lease(
                app_ref,
                routing_key,
                root_blob_name,
                &record,
                &new_value,
                trace_id,
            )
            .await;
            if result.is_ok() {
                record.value = new_value;
            }
            (record, expires_at_ms, result)
        })
        .buffer_unordered(BLOCK_READER_LEASE_RPC_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    let mut acquired = Vec::with_capacity(results.len());
    let mut first_error = None;
    let mut confirmed_until_ms = u64::MAX;
    for (record, expires_at_ms, result) in results {
        match result {
            Ok(()) => {
                acquired.push(record);
                confirmed_until_ms = confirmed_until_ms.min(expires_at_ms);
            }
            Err(error) if first_error.is_none() => first_error = Some(error.into_s3_error()),
            Err(_) => {}
        }
    }
    if let Some(error) = first_error {
        remove_block_reader_leases(&app, routing_key, root_blob_name, &acquired).await;
        return Err(error);
    }
    if !block_reader_lease_is_returnable(confirmed_until_ms, wall_clock_ms()) {
        tracing::warn!(%confirmed_until_ms, "block reader lease acquisition expired before use");
        remove_block_reader_leases(&app, routing_key, root_blob_name, &acquired).await;
        return Err(S3Error::InternalError);
    }

    let confirmed_until_ms = Arc::new(AtomicU64::new(confirmed_until_ms));
    let (lease_lost_tx, lease_lost_rx) = oneshot::channel();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    drop(tokio::spawn(maintain_block_reader_leases(
        app,
        *routing_key,
        root_blob_name.to_string(),
        acquired,
        confirmed_until_ms.clone(),
        lease_lost_tx,
        shutdown_rx,
    )));
    Ok(BlockReaderLeases {
        shutdown_tx: Some(shutdown_tx),
        lease_lost_rx: Some(lease_lost_rx),
        confirmed_until_ms: Some(confirmed_until_ms),
    })
}

async fn validate_leased_object(
    app: &AppState,
    bucket: &Bucket,
    key: &str,
    expected_version_id: Uuid,
    trace_id: &TraceId,
) -> Result<(), S3Error> {
    let fresh = get_raw_object(
        app,
        &bucket.routing_key,
        &bucket.root_blob_name,
        &bucket.bucket_name,
        key,
        trace_id,
    )
    .await?;
    if fresh.version_id != expected_version_id {
        tracing::warn!(
            %key,
            expected_version_id = %expected_version_id,
            actual_version_id = %fresh.version_id,
            "object changed while acquiring block reader lease"
        );
        return Err(S3Error::InternalError);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockSource {
    Data { version: u64, mapped: bool },
    Hole,
}

fn validate_get_part_number(part_number: Option<u32>) -> Result<(), S3Error> {
    if part_number.is_some_and(|part_number| !(1..=10_000).contains(&part_number)) {
        return Err(S3Error::InvalidArgument1);
    }
    Ok(())
}

fn resolve_block_source(map: Option<&BlockMap>, block_number: u32) -> BlockSource {
    match map.and_then(|map| map.lookup(block_number)) {
        Some(RangeState::Written(version)) => BlockSource::Data {
            version,
            mapped: true,
        },
        Some(RangeState::Hole) | Some(RangeState::Reserved(_)) => BlockSource::Hole,
        None => BlockSource::Data {
            version: 1,
            mapped: false,
        },
    }
}

fn block_read_len(source: BlockSource, content_len: usize, block_size: usize) -> usize {
    match source {
        BlockSource::Data { version, .. } if version > 1 => block_size.max(content_len),
        _ => content_len,
    }
}

fn is_sparse_v1_not_found(source: BlockSource, error: &BlobStorageError) -> bool {
    matches!(
        (source, error),
        (
            BlockSource::Data {
                version: 1,
                mapped: false
            },
            BlobStorageError::DataVg(volume_group_proxy::DataVgError::BlockNotFound)
        )
    )
}

async fn load_block_map(
    app: &AppState,
    routing_key: &RoutingKey,
    root_blob_name: &str,
    layout: &ObjectLayout,
    trace_id: &TraceId,
) -> Result<Option<Arc<BlockMap>>, S3Error> {
    let Some(map_ref) = layout.block_map() else {
        return Ok(None);
    };
    let blob_guid = layout.blob_guid()?;
    if layout.get_blob_location()? == BlobLocation::S3 {
        tracing::error!(%blob_guid, map_id = %map_ref.map_id, "S3 layout references a block map");
        return Err(S3Error::InternalError);
    }
    if let Some(map) = app.block_maps.get(&map_ref.map_id).await {
        return Ok(Some(map));
    }
    let chunks = stream::iter(0..map_ref.chunk_count)
        .map(|chunk_number| async move {
            let key = bmap_chunk_key(&blob_guid, map_ref.map_id, chunk_number);
            let nss_client = app.get_nss_rpc_client(routing_key).await?;
            let response = nss_rpc_retry!(
                nss_client,
                get_inode(
                    root_blob_name,
                    &key,
                    Some(app.config.rpc_request_timeout()),
                    trace_id
                ),
                app,
                routing_key,
                trace_id
            )
            .await?;
            match response.result {
                Some(get_inode_response::Result::Ok(bytes)) => Ok(bytes),
                Some(get_inode_response::Result::ErrNotFound(())) => {
                    tracing::error!(%blob_guid, %key, "block map chunk is missing");
                    Err(S3Error::InternalError)
                }
                Some(get_inode_response::Result::ErrNoSuchRootBlob(())) => {
                    Err(S3Error::NoSuchBucket)
                }
                Some(get_inode_response::Result::ErrOther(error)) => {
                    tracing::error!(%blob_guid, %key, %error, "block map chunk read failed");
                    Err(S3Error::InternalError)
                }
                None => {
                    tracing::error!(%blob_guid, %key, "block map chunk response is empty");
                    Err(S3Error::InternalError)
                }
            }
        })
        .buffered(BLOCK_MAP_CHUNK_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    let map = Arc::new(BlockMap::from_chunks(&chunks).map_err(|error| {
        tracing::error!(%blob_guid, map_id = %map_ref.map_id, %error, "block map decode failed");
        S3Error::InternalError
    })?);
    app.block_maps.insert(map_ref.map_id, map.clone()).await;
    Ok(Some(map))
}

#[allow(clippy::too_many_arguments)]
async fn read_block(
    blob_client: &BlobClient,
    blob_guid: DataBlobGuid,
    block_number: u32,
    content_len: usize,
    block_size: usize,
    blob_location: BlobLocation,
    map: Option<&BlockMap>,
    trace_id: &TraceId,
) -> Result<Bytes, S3Error> {
    let source = resolve_block_source(map, block_number);
    let version = match source {
        BlockSource::Data { version, .. } => version,
        BlockSource::Hole => return Ok(Bytes::from(vec![0; content_len])),
    };
    let read_len = block_read_len(source, content_len, block_size);
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
        Err(error) if is_sparse_v1_not_found(source, &error) => {
            Ok(Bytes::from(vec![0; content_len]))
        }
        Err(error) => Err(error.into()),
    }
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
            let leases = acquire_block_reader_leases(
                app.clone(),
                &bucket.routing_key,
                &bucket.root_blob_name,
                &[(blob_guid, object.blob_version)],
                trace_id,
            )
            .await?;
            validate_leased_object(&app, bucket, &key, object.version_id, trace_id).await?;
            let block_map = load_block_map(
                &app,
                &bucket.routing_key,
                &bucket.root_blob_name,
                object,
                trace_id,
            )
            .await?;
            let body_stream = get_full_blob_stream(
                blob_client,
                blob_guid,
                num_blocks,
                size,
                block_size,
                blob_location,
                block_map,
                *trace_id,
            )
            .await?;
            Ok((
                Box::pin(BlockReaderLeaseStream::new(body_stream, leases)),
                size,
            ))
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
                let mpu_prefix = mpu_get_part_prefix(key, 0);
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
                                None,
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
            let leases = acquire_block_reader_leases(
                app.clone(),
                &bucket.routing_key,
                &bucket.root_blob_name,
                &[(blob_guid, object.blob_version)],
                trace_id,
            )
            .await?;
            validate_leased_object(&app, bucket, &key, object.version_id, trace_id).await?;
            let block_map = load_block_map(
                &app,
                &bucket.routing_key,
                &bucket.root_blob_name,
                object,
                trace_id,
            )
            .await?;
            let body_stream = get_range_blob_stream(
                blob_client,
                blob_guid,
                block_size,
                object_size,
                num_blocks,
                range.start,
                range.end,
                blob_location,
                block_map,
                *trace_id,
            );
            Ok(Box::pin(BlockReaderLeaseStream::new(body_stream, leases)))
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
                let mpu_prefix = mpu_get_part_prefix(key, 0);
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
                let body_stream = stream::iter(mpu_blobs)
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
                                    None,
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

#[allow(clippy::too_many_arguments)]
async fn get_full_blob_stream(
    blob_client: Arc<BlobClient>,
    blob_guid: DataBlobGuid,
    num_blocks: usize,
    object_size: u64,
    block_size: usize,
    blob_location: BlobLocation,
    block_map: Option<Arc<BlockMap>>,
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
        blob_guid,
        0,
        first_block_len,
        block_size,
        blob_location,
        block_map.as_deref(),
        &trace_id,
    )
    .await
    .inspect_err(|error| {
        tracing::error!(%blob_guid, block_number = 0, %error, "failed to get blob");
    })?;

    if num_blocks == 1 {
        return Ok(stream::once(async { Ok(first_block) }).boxed());
    }

    let remaining_stream = stream::iter(1..num_blocks).then(move |i| {
        let blob_client = blob_client.clone();
        let block_map = block_map.clone();
        async move {
            let is_last_block = i == num_blocks - 1;
            let content_len = if is_last_block {
                (object_size as usize) - (block_size * i)
            } else {
                block_size
            };
            read_block(
                &blob_client,
                blob_guid,
                i as u32,
                content_len,
                block_size,
                blob_location,
                block_map.as_deref(),
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
    blob_guid: DataBlobGuid,
    block_size: usize,
    object_size: u64,
    num_blocks: usize,
    start: usize,
    end: usize,
    blob_location: BlobLocation,
    block_map: Option<Arc<BlockMap>>,
    trace_id: TraceId,
) -> impl stream::Stream<Item = Result<Bytes, S3Error>> {
    let start_block_i = start / block_size;
    let end_block_i = (end - 1) / block_size;
    let blob_offset: usize = block_size * start_block_i;

    let span = Span::current();
    futures::stream::iter(start_block_i..=end_block_i)
        .then(move |i| {
            let blob_client = blob_client.clone();
            let block_map = block_map.clone();
            async move {
                let is_last_block = i == num_blocks - 1;
                let content_len = if is_last_block {
                    (object_size as usize) - (block_size * i)
                } else {
                    block_size
                };
                read_block(
                    &blob_client,
                    blob_guid,
                    i as u32,
                    content_len,
                    block_size,
                    blob_location,
                    block_map.as_deref(),
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
    use super::*;
    use volume_group_proxy::DataVgError;

    #[test]
    fn block_source_uses_exact_mapped_version_and_sparse_v1_default() {
        let mut map = BlockMap::new();
        map.overlay(1, 1, RangeState::Written(7));
        map.overlay(2, 2, RangeState::Hole);
        map.overlay(3, 3, RangeState::Reserved(8));

        assert_eq!(
            resolve_block_source(Some(&map), 0),
            BlockSource::Data {
                version: 1,
                mapped: false,
            }
        );
        assert_eq!(
            resolve_block_source(Some(&map), 1),
            BlockSource::Data {
                version: 7,
                mapped: true,
            }
        );
        assert_eq!(resolve_block_source(Some(&map), 2), BlockSource::Hole);
        assert_eq!(resolve_block_source(Some(&map), 3), BlockSource::Hole);
        assert_eq!(
            resolve_block_source(None, 4),
            BlockSource::Data {
                version: 1,
                mapped: false,
            }
        );
    }

    #[test]
    fn versioned_rewrite_reads_full_block() {
        let mapped_v1 = BlockSource::Data {
            version: 1,
            mapped: true,
        };
        let mapped_v2 = BlockSource::Data {
            version: 2,
            mapped: true,
        };

        assert_eq!(block_read_len(mapped_v1, 17, 128), 17);
        assert_eq!(block_read_len(mapped_v2, 17, 128), 128);
        assert_eq!(block_read_len(mapped_v2, 256, 128), 256);
    }

    #[test]
    fn only_unmapped_v1_not_found_is_a_sparse_hole() {
        let missing = BlobStorageError::DataVg(DataVgError::BlockNotFound);
        let unmapped_v1 = BlockSource::Data {
            version: 1,
            mapped: false,
        };
        let mapped_v1 = BlockSource::Data {
            version: 1,
            mapped: true,
        };
        let mapped_v2 = BlockSource::Data {
            version: 2,
            mapped: true,
        };
        let unavailable = BlobStorageError::DataVg(DataVgError::QuorumFailure(
            "replicas unavailable".to_string(),
        ));

        assert!(is_sparse_v1_not_found(unmapped_v1, &missing));
        assert!(!is_sparse_v1_not_found(mapped_v1, &missing));
        assert!(!is_sparse_v1_not_found(mapped_v2, &missing));
        assert!(!is_sparse_v1_not_found(unmapped_v1, &unavailable));
    }

    #[test]
    fn block_reader_lease_key_and_value_match_sweeper_contract() {
        let blob_guid = DataBlobGuid {
            blob_id: Uuid::from_u128(1),
            volume_id: 7,
        };
        let reader_id = Uuid::from_u128(2);
        assert_eq!(
            block_reader_key(&blob_guid, reader_id),
            "#bmap-reader/00000000-0000-0000-0000-000000000001/00000000-0000-0000-0000-000000000002"
        );

        let value = block_reader_lease_value(15_123, 17);
        assert_eq!(&value[..8], 15_123_u64.to_le_bytes().as_slice());
        assert_eq!(&value[8..], 17_u64.to_le_bytes().as_slice());
    }

    #[test]
    fn block_reader_leases_are_deduplicated_by_full_blob_guid() {
        let volume_1 = DataBlobGuid {
            blob_id: Uuid::from_u128(1),
            volume_id: 1,
        };
        let volume_2 = DataBlobGuid {
            blob_id: Uuid::from_u128(1),
            volume_id: 2,
        };

        assert_eq!(
            deduplicate_blob_versions(&[
                (volume_1, 3),
                (volume_1, 3),
                (volume_2, 3),
                (volume_1, 4),
            ]),
            vec![(volume_1, 3), (volume_2, 3), (volume_1, 4)]
        );
    }

    #[test]
    fn block_reader_lease_requires_a_full_renewal_margin() {
        let margin_ms = BLOCK_READER_LEASE_RENEW_INTERVAL
            .saturating_mul(2)
            .as_millis() as u64;
        assert!(!block_reader_lease_is_returnable(100 + margin_ms, 100));
        assert!(block_reader_lease_is_returnable(101 + margin_ms, 100));
    }

    #[test]
    fn body_stream_owns_reader_lease_lifetime() {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let body_stream = BlockReaderLeaseStream::new(
            stream::pending::<Result<Bytes, S3Error>>(),
            BlockReaderLeases {
                shutdown_tx: Some(shutdown_tx),
                lease_lost_rx: None,
                confirmed_until_ms: None,
            },
        );

        assert!(matches!(
            shutdown_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drop(body_stream);
        assert!(matches!(shutdown_rx.try_recv(), Ok(true)));
    }

    #[tokio::test]
    async fn expired_reader_lease_terminates_body_without_cleanup() {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let mut body_stream = Box::pin(BlockReaderLeaseStream::new(
            stream::pending::<Result<Bytes, S3Error>>(),
            BlockReaderLeases {
                shutdown_tx: Some(shutdown_tx),
                lease_lost_rx: None,
                confirmed_until_ms: Some(Arc::new(AtomicU64::new(0))),
            },
        ));

        let result = body_stream
            .next()
            .await
            .expect("lease failure should produce one stream error");
        assert!(matches!(result, Err(S3Error::InternalError)));
        assert!(matches!(shutdown_rx.try_recv(), Ok(false)));
        assert!(body_stream.next().await.is_none());
    }

    #[tokio::test]
    async fn lease_expiry_wakes_a_pending_body_stream() {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let deadline = wall_clock_ms().saturating_add(25);
        let mut body_stream = Box::pin(BlockReaderLeaseStream::new(
            stream::pending::<Result<Bytes, S3Error>>(),
            BlockReaderLeases {
                shutdown_tx: Some(shutdown_tx),
                lease_lost_rx: None,
                confirmed_until_ms: Some(Arc::new(AtomicU64::new(deadline))),
            },
        ));

        let result = tokio::time::timeout(Duration::from_secs(1), body_stream.next())
            .await
            .expect("lease timer should wake the pending body")
            .expect("lease expiry should produce one stream error");
        assert!(matches!(result, Err(S3Error::InternalError)));
        assert!(matches!(shutdown_rx.try_recv(), Ok(false)));
    }
}
