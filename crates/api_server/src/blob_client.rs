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
use data_types::{DataBlobGuid, RoutingKey, TraceId};
use futures::{
    StreamExt,
    future::{BoxFuture, pending},
    stream::{self, FuturesUnordered},
};
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

pub struct BlobDeletionRequest {
    state: BlobDeletionState,
}

struct BlobDeletionState {
    app: Arc<AppState>,
    blob_guid: DataBlobGuid,
    num_blocks: u32,
    location: BlobLocation,
    writer_grace_until: Option<Instant>,
    blocks_deleted: bool,
}

const BLOB_DELETE_REQUEST_CONCURRENCY: usize = 4;
const BLOB_DELETE_BLOCK_CONCURRENCY: usize = 8;
const BLOB_DELETE_RECHECK: Duration = Duration::from_secs(1);
const WRITER_COMPLETION_SAFETY: Duration = Duration::from_secs(10);
static BLOB_DELETE_DATA_RPC_LIMIT: Semaphore = Semaphore::const_new(32);

pub fn enqueue_blob_deletion(
    app: Arc<AppState>,
    routing_key: RoutingKey,
    _root_blob_name: &str,
    object: &ObjectLayout,
) -> BoxFuture<'static, Result<(), ObjectLayoutError>> {
    let state = match new_blob_deletion_state(app.clone(), object) {
        Ok(state) => state,
        Err(error) => return Box::pin(async move { Err(error) }),
    };
    let blob_guid = state.blob_guid;
    let request = BlobDeletionRequest { state };

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

fn new_blob_deletion_state(
    app: Arc<AppState>,
    object: &ObjectLayout,
) -> Result<BlobDeletionState, ObjectLayoutError> {
    let blob_guid = object.blob_guid()?;
    let num_blocks = object
        .num_blocks()
        .and_then(|count| u32::try_from(count).map_err(|_| ObjectLayoutError::InvalidState))?;
    let location = object.get_blob_location()?;
    Ok(BlobDeletionState {
        blob_guid,
        num_blocks,
        location,
        writer_grace_until: None,
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
                        retries.push_back((Instant::now() + BLOB_DELETE_RECHECK, request));
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

    /// At-or-before read for DataVg-backed blobs.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_blob_at_or_before(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        ceiling: u64,
        content_len: usize,
        trace_id: &TraceId,
    ) -> Result<volume_group_proxy::AtOrBeforeRead, BlobStorageError> {
        self.storage
            .get_blob_at_or_before(blob_guid, block_number, ceiling, content_len, trace_id)
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
        if delete_blob_once(&storage, &mut request.state, &trace_id).await {
            None
        } else {
            Some(request)
        }
    })
}

async fn delete_layout_blocks(
    storage: &BlobStorageImpl,
    request: &BlobDeletionState,
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

async fn delete_blob_once(
    storage: &BlobStorageImpl,
    request: &mut BlobDeletionState,
    trace_id: &TraceId,
) -> bool {
    // INTERIM: reader-lease pinning was removed, so an in-flight GET of
    // this blob is covered only by this grace; a positive reader-pinning
    // mechanism is future work. The grace also bounds a dead writer's
    // in-flight puts (its original purpose).
    if request.writer_grace_until.is_none() {
        request.writer_grace_until = Some(
            Instant::now() + request.app.config.rpc_request_timeout() + WRITER_COMPLETION_SAFETY,
        );
        return false;
    }
    if request
        .writer_grace_until
        .is_some_and(|deadline| deadline > Instant::now())
    {
        return false;
    }
    if !request.blocks_deleted {
        if !delete_layout_blocks(storage, request, trace_id).await {
            return false;
        }
        request.blocks_deleted = true;
    }
    true
}
