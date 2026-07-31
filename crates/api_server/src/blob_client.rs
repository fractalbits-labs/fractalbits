use crate::{
    AppState,
    blob_storage::{
        AllInBssSingleAzStorage, BlobLocation, BlobStorageError, BlobStorageImpl,
        S3HybridSingleAzStorage,
    },
    config::{BlobStorageBackend, BlobStorageConfig},
};
use bytes::Bytes;
use data_types::object_layout::{ObjectLayout, ObjectLayoutError};
use data_types::ovr_map::{ovr_gc_key, ovr_row_prefix};
use data_types::{DataBlobGuid, RoutingKey, TraceId};
use file_ops::{parse_delete_inode, parse_list_inodes_raw};
use futures::{
    StreamExt,
    future::{BoxFuture, pending},
    stream::{self, FuturesUnordered},
};
use rpc_client_common::{nss_rpc_retry, reclamation_grace};
use std::{
    collections::VecDeque,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::{
    sync::{Mutex, Semaphore, mpsc::Receiver, mpsc::error::TryRecvError, oneshot},
    task::JoinHandle,
    time::Instant,
};

/// A whole-blob background deletion: every physical data key (enumerated
/// via an all-node listing, so multi-generation FUSE blobs are fully
/// reclaimed), then the blob's `@ovr/` rows, then its `@ovr-gc/` marker.
pub struct BlobDeletionRequest {
    app: Arc<AppState>,
    routing_key: RoutingKey,
    root_blob_name: String,
    blob_guid: DataBlobGuid,
    num_blocks: u32,
    location: BlobLocation,
    /// Rows can only exist for a mapped blob; skip the NSS sweep
    /// otherwise.
    mapped: bool,
    /// `@ovr-gc/` teardown intent recorded. Written as the worker's
    /// first step: once the enqueuing NSS mutation is done, the marker
    /// is the only durable record that this blob's rows are doomed, so
    /// a crash mid-cleanup stays recoverable by the marker scavenger.
    marker_written: bool,
    /// INTERIM reader cover: with no positive reader pinning, an
    /// in-flight GET of the doomed blob gets one grace window before
    /// its write-once keys start disappearing (it then fails, never
    /// serves wrong bytes).
    grace_until: Option<Instant>,
    blocks_deleted: bool,
}

const BLOB_DELETE_REQUEST_CONCURRENCY: usize = 4;
const BLOB_DELETE_BLOCK_CONCURRENCY: usize = 8;
const ROW_DELETE_CONCURRENCY: usize = 8;
/// Deliberately at the NSS clamp: the has_more loop, not the request
/// size, bounds row coverage (a truncated-at-1000 sweep once left
/// buckets permanently undeletable).
const ROW_PAGE_SIZE: u32 = 1_000;
const DELETE_RETRY_DELAY: Duration = Duration::from_secs(1);
static BLOB_DELETE_DATA_RPC_LIMIT: Semaphore = Semaphore::const_new(32);
static BLOB_DELETE_NSS_RPC_LIMIT: Semaphore = Semaphore::const_new(32);

/// Queue a whole-object blob deletion. The caller has already made the
/// object unreachable in NSS (delete/rename/overwrite), so everything
/// here is best-effort space reclamation.
pub fn enqueue_blob_deletion(
    app: Arc<AppState>,
    routing_key: RoutingKey,
    root_blob_name: &str,
    object: &ObjectLayout,
) -> BoxFuture<'static, Result<(), ObjectLayoutError>> {
    let request = match new_blob_deletion_request(
        app.clone(),
        routing_key,
        root_blob_name.to_string(),
        object,
    ) {
        Ok(request) => request,
        Err(error) => return Box::pin(async move { Err(error) }),
    };
    let blob_guid = request.blob_guid;

    Box::pin(async move {
        app.get_blob_client(&routing_key).await.map_err(|error| {
            tracing::warn!(%blob_guid, %error, "failed to start background blob deletion");
            ObjectLayoutError::InvalidState
        })?;
        app.get_blob_deletion()
            .send(request)
            .await
            .map_err(|error| {
                tracing::warn!(%blob_guid, %error, "failed to queue blob for background deletion");
                ObjectLayoutError::InvalidState
            })
    })
}

fn new_blob_deletion_request(
    app: Arc<AppState>,
    routing_key: RoutingKey,
    root_blob_name: String,
    object: &ObjectLayout,
) -> Result<BlobDeletionRequest, ObjectLayoutError> {
    let blob_guid = object.blob_guid()?;
    let num_blocks = object
        .num_blocks()
        .and_then(|count| u32::try_from(count).map_err(|_| ObjectLayoutError::InvalidState))?;
    let location = object.get_blob_location()?;
    let grace = reclamation_grace(app.config.rpc_request_timeout());
    Ok(BlobDeletionRequest {
        routing_key,
        root_blob_name,
        blob_guid,
        num_blocks,
        location,
        mapped: object.may_have_ovr_records(),
        marker_written: !object.may_have_ovr_records(),
        grace_until: Some(Instant::now() + grace),
        blocks_deleted: false,
        app,
    })
}

pub struct BlobClient {
    storage: Arc<BlobStorageImpl>,
    blob_deletion_shutdown: Mutex<Option<oneshot::Sender<()>>>,
    blob_deletion_task_handle: Mutex<Option<JoinHandle<()>>>,
}

impl BlobClient {
    pub(crate) async fn create_storage_with_data_vg_info(
        blob_storage_config: &BlobStorageConfig,
        rpc_request_timeout: Duration,
        rpc_connection_timeout: Duration,
        data_vg_info: data_types::DataVgInfo,
    ) -> Result<Arc<BlobStorageImpl>, BlobStorageError> {
        let storage = match &blob_storage_config.backend {
            BlobStorageBackend::S3HybridSingleAz => {
                let s3_hybrid_config = blob_storage_config
                    .s3_hybrid_single_az
                    .as_ref()
                    .ok_or_else(|| {
                        BlobStorageError::Config(
                            "S3 hybrid configuration required for Hybrid backend".into(),
                        )
                    })?;

                BlobStorageImpl::HybridSingleAz(
                    S3HybridSingleAzStorage::new_with_data_vg_info(
                        data_vg_info.clone(),
                        s3_hybrid_config,
                        rpc_request_timeout,
                        rpc_connection_timeout,
                    )
                    .await?,
                )
            }
            BlobStorageBackend::AllInBssSingleAz => BlobStorageImpl::AllInBssSingleAz(
                AllInBssSingleAzStorage::new_with_data_vg_info(
                    data_vg_info.clone(),
                    rpc_request_timeout,
                    rpc_connection_timeout,
                )
                .await?,
            ),
        };

        Ok(Arc::new(storage))
    }

    pub(crate) fn new_with_storage(
        storage: Arc<BlobStorageImpl>,
        rx: Receiver<BlobDeletionRequest>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let blob_deletion_task_handle = tokio::spawn({
            let storage = storage.clone();
            async move { Self::blob_deletion_task(storage, rx, shutdown_rx).await }
        });

        Self {
            storage,
            blob_deletion_shutdown: Mutex::new(Some(shutdown_tx)),
            blob_deletion_task_handle: Mutex::new(Some(blob_deletion_task_handle)),
        }
    }

    /// Drain the deletion worker: close the intake and wait for queued
    /// blobs to finish, so a graceful shutdown does not strand
    /// enumerable garbage.
    pub async fn shutdown(&self) {
        if let Some(shutdown) = self.blob_deletion_shutdown.lock().await.take() {
            let _ = shutdown.send(());
        }
        let task = self.blob_deletion_task_handle.lock().await.take();
        let Some(task) = task else {
            return;
        };
        if let Err(error) = task.await {
            tracing::warn!(%error, "blob deletion worker failed during shutdown");
        }
    }

    async fn blob_deletion_task(
        storage: Arc<BlobStorageImpl>,
        mut input: Receiver<BlobDeletionRequest>,
        mut shutdown: oneshot::Receiver<()>,
    ) {
        let mut input_closed = false;
        let mut shutdown_requested = false;
        let mut active: FuturesUnordered<BoxFuture<'static, Option<BlobDeletionRequest>>> =
            FuturesUnordered::new();
        let mut retries = VecDeque::new();
        let mut prefer_retry = true;

        loop {
            while active.len() < BLOB_DELETE_REQUEST_CONCURRENCY {
                let now = Instant::now();
                let retry_ready = retries
                    .front()
                    .is_some_and(|(ready_at, _)| *ready_at <= now);
                if prefer_retry && retry_ready {
                    let (_, request) = retries.pop_front().expect("retry queue is nonempty");
                    active.push(blob_deletion_attempt(storage.clone(), request));
                    prefer_retry = false;
                    continue;
                }
                if !input_closed {
                    match input.try_recv() {
                        Ok(request) => {
                            active.push(blob_deletion_attempt(storage.clone(), request));
                            prefer_retry = true;
                            continue;
                        }
                        Err(TryRecvError::Empty) => {}
                        Err(TryRecvError::Disconnected) => input_closed = true,
                    }
                }
                if retry_ready {
                    let (_, request) = retries.pop_front().expect("retry queue is nonempty");
                    active.push(blob_deletion_attempt(storage.clone(), request));
                    prefer_retry = false;
                    continue;
                }
                break;
            }

            if input_closed && active.is_empty() && retries.is_empty() {
                return;
            }

            let retry_deadline = retries.front().map(|(ready_at, _)| *ready_at);
            tokio::select! {
                request = input.recv(), if !input_closed && active.len() < BLOB_DELETE_REQUEST_CONCURRENCY => {
                    match request {
                        Some(request) => {
                            active.push(blob_deletion_attempt(storage.clone(), request));
                            prefer_retry = true;
                        }
                        None => input_closed = true,
                    }
                }
                result = active.next(), if !active.is_empty() => {
                    if let Some(Some(request)) = result {
                        retries.push_back((Instant::now() + DELETE_RETRY_DELAY, request));
                    }
                }
                _ = async {
                    match retry_deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => pending::<()>().await,
                    }
                }, if retry_deadline.is_some() => {}
                _ = &mut shutdown, if !shutdown_requested => {
                    input.close();
                    shutdown_requested = true;
                }
            }
        }
    }

    pub fn create_data_blob_guid(&self) -> DataBlobGuid {
        match &*self.storage {
            BlobStorageImpl::HybridSingleAz(storage) => storage.create_data_blob_guid(),
            BlobStorageImpl::AllInBssSingleAz(storage) => storage.create_data_blob_guid(),
        }
    }

    pub fn create_data_blob_guid_with_size_hint(&self, content_len: Option<usize>) -> DataBlobGuid {
        let prefer_ec =
            content_len.is_none_or(|size| size >= ObjectLayout::DEFAULT_BLOCK_SIZE as usize);
        match &*self.storage {
            BlobStorageImpl::HybridSingleAz(storage) => {
                storage.create_data_blob_guid_with_preference(prefer_ec)
            }
            BlobStorageImpl::AllInBssSingleAz(storage) => {
                storage.create_data_blob_guid_with_preference(prefer_ec)
            }
        }
    }

    pub async fn put_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        self.storage
            .put_blob(
                blob_guid.blob_id,
                blob_guid.volume_id,
                block_number,
                body,
                trace_id,
            )
            .await
    }

    pub async fn put_blob_vectored(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        chunks: Vec<actix_web::web::Bytes>,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        self.storage
            .put_blob_vectored(
                blob_guid.blob_id,
                blob_guid.volume_id,
                block_number,
                chunks,
                trace_id,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        content_len: usize,
        location: BlobLocation,
        body: &mut Bytes,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        self.storage
            .get_blob(
                blob_guid,
                block_number,
                version,
                content_len,
                location,
                body,
                trace_id,
            )
            .await
    }

    pub async fn delete_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        location: BlobLocation,
        trace_id: &TraceId,
    ) -> Result<(), BlobStorageError> {
        self.storage
            .delete_blob(blob_guid, block_number, version, location, trace_id)
            .await
    }
}

fn blob_deletion_attempt(
    storage: Arc<BlobStorageImpl>,
    mut request: BlobDeletionRequest,
) -> BoxFuture<'static, Option<BlobDeletionRequest>> {
    Box::pin(async move {
        let trace_id = TraceId::new();
        if delete_blob_once(&storage, &mut request, &trace_id).await {
            None
        } else {
            Some(request)
        }
    })
}

async fn delete_blob_once(
    storage: &BlobStorageImpl,
    request: &mut BlobDeletionRequest,
    trace_id: &TraceId,
) -> bool {
    if !request.marker_written {
        if let Err(error) = write_gc_marker(request, trace_id).await {
            tracing::warn!(
                blob_guid = %request.blob_guid,
                %error,
                "@ovr-gc marker write failed, retaining blob deletion request"
            );
            return false;
        }
        request.marker_written = true;
    }
    if request
        .grace_until
        .is_some_and(|deadline| deadline > Instant::now())
    {
        return false;
    }
    request.grace_until = None;
    if !request.blocks_deleted {
        if !delete_layout_blocks(storage, request, trace_id).await {
            return false;
        }
        request.blocks_deleted = true;
    }
    if request.mapped
        && let Err(error) = delete_ovr_rows(request, trace_id).await
    {
        tracing::warn!(
            blob_guid = %request.blob_guid,
            %error,
            "@ovr row cleanup failed, retaining blob deletion request"
        );
        return false;
    }
    true
}

async fn delete_layout_blocks(
    storage: &BlobStorageImpl,
    request: &BlobDeletionRequest,
    trace_id: &TraceId,
) -> bool {
    let mut identities = match request.location {
        BlobLocation::DataVgProxy => {
            match storage.list_blob_blocks(request.blob_guid, trace_id).await {
                Ok(identities) => identities,
                Err(error) => {
                    tracing::warn!(
                        blob_guid = %request.blob_guid,
                        %error,
                        "background blob enumeration failed"
                    );
                    return false;
                }
            }
        }
        // S3-side keys are not generation-specific; the base
        // generation stands in for the whole logical range.
        BlobLocation::S3 => (0..request.num_blocks)
            .map(|block_number| (block_number, 1))
            .collect(),
    };
    identities.sort_unstable();
    identities.dedup();

    let failed = AtomicBool::new(false);
    stream::iter(identities)
        .for_each_concurrent(BLOB_DELETE_BLOCK_CONCURRENCY, |(block_number, version)| {
            let failed = &failed;
            async move {
                let _permit = BLOB_DELETE_DATA_RPC_LIMIT
                    .acquire()
                    .await
                    .expect("blob delete semaphore remains open");
                if let Err(error) = storage
                    .delete_blob(
                        request.blob_guid,
                        block_number,
                        version,
                        request.location,
                        trace_id,
                    )
                    .await
                {
                    failed.store(true, Ordering::Relaxed);
                    tracing::warn!(
                        blob_guid = %request.blob_guid,
                        block_number,
                        version,
                        %error,
                        "background block deletion failed"
                    );
                }
            }
        })
        .await;
    !failed.load(Ordering::Relaxed)
}

async fn write_gc_marker(request: &BlobDeletionRequest, trace_id: &TraceId) -> Result<(), String> {
    let _permit = BLOB_DELETE_NSS_RPC_LIMIT
        .acquire()
        .await
        .map_err(|error| error.to_string())?;
    let nss_client = request
        .app
        .get_nss_rpc_client(&request.routing_key)
        .await
        .map_err(|error| error.to_string())?;
    let key = ovr_gc_key(&request.blob_guid.blob_id);
    // The "gc" sentinel marks an UNCONDITIONAL teardown: this worker
    // only runs after the doomed object is already unreachable in NSS,
    // so the fs_server marker scavenger may replay it. Pre-mutation
    // (unlink/rename) markers carry the doomed key and are retained as
    // opaque conditional intents.
    nss_rpc_retry!(
        nss_client,
        put_inode(
            &request.root_blob_name,
            &key,
            Bytes::from_static(b"gc"),
            Some(request.app.config.rpc_request_timeout()),
            trace_id
        ),
        request.app.as_ref(),
        &request.routing_key,
        trace_id
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(())
}

async fn delete_nss_key(
    request: &BlobDeletionRequest,
    key: &str,
    trace_id: &TraceId,
) -> Result<(), String> {
    let _permit = BLOB_DELETE_NSS_RPC_LIMIT
        .acquire()
        .await
        .map_err(|error| error.to_string())?;
    let nss_client = request
        .app
        .get_nss_rpc_client(&request.routing_key)
        .await
        .map_err(|error| error.to_string())?;
    let response = nss_rpc_retry!(
        nss_client,
        delete_inode(
            &request.root_blob_name,
            key,
            Some(request.app.config.rpc_request_timeout()),
            trace_id
        ),
        request.app.as_ref(),
        &request.routing_key,
        trace_id
    )
    .await
    .map_err(|error| error.to_string())?;
    drop(parse_delete_inode(response).map_err(|error| error.to_string())?);
    Ok(())
}

/// Delete every `@ovr/` row of the blob (paginated past the NSS clamp),
/// then its `@ovr-gc/` teardown marker. Marker last: it is what makes a
/// crash mid-sweep recoverable.
async fn delete_ovr_rows(request: &BlobDeletionRequest, trace_id: &TraceId) -> Result<(), String> {
    let prefix = ovr_row_prefix(&request.blob_guid.blob_id);
    let mut start_after = String::new();

    loop {
        let _permit = BLOB_DELETE_NSS_RPC_LIMIT
            .acquire()
            .await
            .map_err(|error| error.to_string())?;
        let nss_client = request
            .app
            .get_nss_rpc_client(&request.routing_key)
            .await
            .map_err(|error| error.to_string())?;
        let response = nss_rpc_retry!(
            nss_client,
            list_inodes(
                &request.root_blob_name,
                ROW_PAGE_SIZE,
                &prefix,
                "",
                &start_after,
                true,
                Some(request.app.config.rpc_request_timeout()),
                trace_id
            ),
            request.app.as_ref(),
            &request.routing_key,
            trace_id
        )
        .await
        .map_err(|error| error.to_string())?;
        drop(_permit);

        let (page, has_more) = match parse_list_inodes_raw(response) {
            Ok(page) => page,
            Err(file_ops::NssError::NoSuchRootBlob) => break,
            Err(error) => return Err(error.to_string()),
        };
        let keys: Vec<String> = page.into_iter().map(|(key, _)| key).collect();
        let Some(next_start_after) = keys.last().cloned() else {
            break;
        };

        let failed = AtomicBool::new(false);
        stream::iter(&keys)
            .for_each_concurrent(ROW_DELETE_CONCURRENCY, |key| {
                let failed = &failed;
                async move {
                    if let Err(error) = delete_nss_key(request, key, trace_id).await {
                        failed.store(true, Ordering::Relaxed);
                        tracing::warn!(
                            blob_guid = %request.blob_guid,
                            %key,
                            %error,
                            "@ovr row deletion failed"
                        );
                    }
                }
            })
            .await;
        if failed.load(Ordering::Relaxed) {
            return Err("one or more @ovr row deletions failed".to_string());
        }
        if !has_more {
            break;
        }
        start_after = next_start_after;
    }

    // Absent marker parses as Ok(None): deleting it is idempotent.
    delete_nss_key(request, &ovr_gc_key(&request.blob_guid.blob_id), trace_id).await
}
