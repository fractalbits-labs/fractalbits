use crate::{
    AppState,
    blob_storage::{
        AllInBssSingleAzStorage, BlobLocation, BlobStorageError, BlobStorageImpl,
        S3HybridSingleAzStorage,
    },
    config::{BlobStorageBackend, BlobStorageConfig},
};
use bytes::Bytes;
use data_types::block_map::{block_reader_prefix, bmap_prefix};
use data_types::object_layout::{ObjectLayout, ObjectLayoutError};
use data_types::{DataBlobGuid, RoutingKey, TraceId};
use file_ops::{NssError, parse_delete_inode, parse_put_inode_cas};
use futures::{
    StreamExt,
    future::{BoxFuture, pending},
    stream::{self, FuturesUnordered},
};
use rpc_client_common::nss_rpc_retry;
use std::{
    collections::VecDeque,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
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
    routing_key: RoutingKey,
    root_blob_name: String,
    blob_guid: DataBlobGuid,
    num_blocks: u32,
    location: BlobLocation,
    readers_drained: bool,
    writer_grace_until: Option<Instant>,
    blocks_deleted: bool,
}

const BLOB_DELETE_REQUEST_CONCURRENCY: usize = 4;
const BLOB_DELETE_BLOCK_CONCURRENCY: usize = 8;
const READER_LEASE_DELETE_CONCURRENCY: usize = 8;
const READER_LEASE_PAGE_SIZE: u32 = 1_000;
const READER_LEASE_RECHECK: Duration = Duration::from_secs(1);
const READER_LEASE_CLOCK_SKEW: Duration = Duration::from_secs(30);
const WRITER_COMPLETION_SAFETY: Duration = Duration::from_secs(10);
static BLOB_DELETE_DATA_RPC_LIMIT: Semaphore = Semaphore::const_new(32);
static BLOB_DELETE_NSS_RPC_LIMIT: Semaphore = Semaphore::const_new(32);

pub fn enqueue_blob_deletion(
    app: Arc<AppState>,
    routing_key: RoutingKey,
    root_blob_name: &str,
    object: &ObjectLayout,
) -> BoxFuture<'static, Result<(), ObjectLayoutError>> {
    let state =
        match new_blob_deletion_state(app.clone(), routing_key, root_blob_name.to_string(), object)
        {
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
    routing_key: RoutingKey,
    root_blob_name: String,
    object: &ObjectLayout,
) -> Result<BlobDeletionState, ObjectLayoutError> {
    let blob_guid = object.blob_guid()?;
    let num_blocks = object
        .num_blocks()
        .and_then(|count| u32::try_from(count).map_err(|_| ObjectLayoutError::InvalidState))?;
    let location = object.get_blob_location()?;
    Ok(BlobDeletionState {
        routing_key,
        root_blob_name,
        blob_guid,
        num_blocks,
        location,
        readers_drained: false,
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
                        retries.push_back((Instant::now() + READER_LEASE_RECHECK, request));
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
    if !request.readers_drained {
        match blob_has_active_readers(request, trace_id).await {
            Ok(false) => {
                request.readers_drained = true;
                request.writer_grace_until = Some(
                    Instant::now()
                        + request.app.config.rpc_request_timeout()
                        + WRITER_COMPLETION_SAFETY,
                );
                return false;
            }
            Ok(true) => return false,
            Err(error) => {
                tracing::warn!(
                    blob_guid = %request.blob_guid,
                    %error,
                    "reader lease scan failed, delaying blob deletion"
                );
                return false;
            }
        }
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
    if let Err(error) = delete_block_map_records(request, trace_id).await {
        tracing::warn!(
            blob_guid = %request.blob_guid,
            %error,
            "block map cleanup failed, retaining blob deletion request"
        );
        return false;
    }
    true
}

fn reader_lease_is_active(value: &Bytes, now_ms: u64) -> bool {
    let Some((expires_at_ms, _)) = reader_lease_fields(value) else {
        return true;
    };
    expires_at_ms > now_ms
}

fn reader_lease_fields(value: &Bytes) -> Option<(u64, u64)> {
    let value: &[u8; 16] = value.as_ref().try_into().ok()?;
    Some((
        u64::from_le_bytes(value[..8].try_into().ok()?),
        u64::from_le_bytes(value[8..].try_into().ok()?),
    ))
}

fn reader_lease_value(expires_at_ms: u64, blob_version: u64) -> Bytes {
    let mut value = [0_u8; 16];
    value[..8].copy_from_slice(&expires_at_ms.to_le_bytes());
    value[8..].copy_from_slice(&blob_version.to_le_bytes());
    Bytes::copy_from_slice(&value)
}

async fn delete_inode_key(
    request: &BlobDeletionState,
    key: &str,
    trace_id: &TraceId,
) -> Result<(), String> {
    delete_nss_inode(
        &request.app,
        &request.routing_key,
        &request.root_blob_name,
        key,
        trace_id,
    )
    .await
}

async fn delete_nss_inode(
    app: &AppState,
    routing_key: &RoutingKey,
    root_blob_name: &str,
    key: &str,
    trace_id: &TraceId,
) -> Result<(), String> {
    let _permit = BLOB_DELETE_NSS_RPC_LIMIT
        .acquire()
        .await
        .map_err(|error| error.to_string())?;
    let nss_client = app
        .get_nss_rpc_client(routing_key)
        .await
        .map_err(|error| error.to_string())?;
    let response = nss_rpc_retry!(
        nss_client,
        delete_inode(
            root_blob_name,
            key,
            Some(app.config.rpc_request_timeout()),
            trace_id
        ),
        app,
        routing_key,
        trace_id
    )
    .await
    .map_err(|error| error.to_string())?;
    drop(parse_delete_inode(response).map_err(|error| error.to_string())?);
    Ok(())
}

async fn retire_expired_reader_lease(
    request: &BlobDeletionState,
    key: &str,
    expected_value: Bytes,
    trace_id: &TraceId,
) -> Result<bool, String> {
    let (_, blob_version) = reader_lease_fields(&expected_value)
        .ok_or_else(|| format!("invalid reader lease value: {key}"))?;
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
        put_inode_cas(
            &request.root_blob_name,
            key,
            reader_lease_value(0, blob_version),
            expected_value.clone(),
            Some(request.app.config.rpc_request_timeout()),
            trace_id
        ),
        request.app.as_ref(),
        &request.routing_key,
        trace_id
    )
    .await
    .map_err(|error| error.to_string())?;
    match parse_put_inode_cas(response) {
        Ok(_) => {
            drop(_permit);
            if let Err(error) = delete_inode_key(request, key, trace_id).await {
                tracing::warn!(
                    blob_guid = %request.blob_guid,
                    %key,
                    %error,
                    "retired reader lease cleanup failed"
                );
            }
            Ok(true)
        }
        Err(NssError::CasConflict(_)) => Ok(false),
        Err(error) => Err(error.to_string()),
    }
}

async fn delete_block_map_records(
    request: &BlobDeletionState,
    trace_id: &TraceId,
) -> Result<(), String> {
    let prefix = bmap_prefix(&request.blob_guid);
    let mut start_after = String::new();
    let mut keys = Vec::new();

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
                READER_LEASE_PAGE_SIZE,
                &prefix,
                "",
                &start_after,
                false,
                Some(request.app.config.rpc_request_timeout()),
                trace_id
            ),
            request.app.as_ref(),
            &request.routing_key,
            trace_id
        )
        .await
        .map_err(|error| error.to_string())?;

        let (inodes, has_more) = match response.result {
            Some(nss_codec::list_inodes_response::Result::Ok(result)) => {
                (result.inodes, result.has_more)
            }
            Some(nss_codec::list_inodes_response::Result::ErrNoSuchRootBlob(())) => return Ok(()),
            Some(nss_codec::list_inodes_response::Result::ErrOther(error)) => return Err(error),
            None => return Err("empty ListInodesResponse".to_string()),
        };
        let next_start_after = inodes
            .last()
            .map(|inode| inode.key.trim_end_matches('\0').to_string());
        keys.extend(
            inodes
                .into_iter()
                .map(|inode| inode.key.trim_end_matches('\0').to_string()),
        );

        if !has_more {
            break;
        }
        start_after = next_start_after
            .filter(|key| !key.is_empty())
            .ok_or_else(|| "truncated block map page contained no keys".to_string())?;
    }

    let failed = AtomicBool::new(false);
    stream::iter(keys)
        .for_each_concurrent(READER_LEASE_DELETE_CONCURRENCY, |key| {
            let failed = &failed;
            async move {
                if let Err(error) = delete_inode_key(request, &key, trace_id).await {
                    failed.store(true, Ordering::Relaxed);
                    tracing::warn!(
                        blob_guid = %request.blob_guid,
                        %key,
                        %error,
                        "block map record cleanup failed"
                    );
                }
            }
        })
        .await;
    if failed.load(Ordering::Relaxed) {
        return Err("one or more block map record deletions failed".to_string());
    }
    Ok(())
}

async fn blob_has_active_readers(
    request: &BlobDeletionState,
    trace_id: &TraceId,
) -> Result<bool, String> {
    let prefix = block_reader_prefix(&request.blob_guid);
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let mut start_after = String::new();
    let mut expired_leases = Vec::new();
    let mut active = false;

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
                READER_LEASE_PAGE_SIZE,
                &prefix,
                "",
                &start_after,
                false,
                Some(request.app.config.rpc_request_timeout()),
                trace_id
            ),
            request.app.as_ref(),
            &request.routing_key,
            trace_id
        )
        .await
        .map_err(|error| error.to_string())?;

        let (inodes, has_more) = match response.result {
            Some(nss_codec::list_inodes_response::Result::Ok(result)) => {
                (result.inodes, result.has_more)
            }
            Some(nss_codec::list_inodes_response::Result::ErrNoSuchRootBlob(())) => {
                return Ok(false);
            }
            Some(nss_codec::list_inodes_response::Result::ErrOther(error)) => return Err(error),
            None => return Err("empty ListInodesResponse".to_string()),
        };

        let next_start_after = inodes
            .last()
            .map(|inode| inode.key.trim_end_matches('\0').to_string());
        for inode in inodes {
            let key = inode.key.trim_end_matches('\0').to_string();
            if reader_lease_is_active(
                &inode.inode,
                now_ms.saturating_sub(READER_LEASE_CLOCK_SKEW.as_millis() as u64),
            ) {
                active = true;
            } else {
                expired_leases.push((key, inode.inode));
            }
        }

        if !has_more {
            break;
        }
        start_after = next_start_after
            .filter(|key| !key.is_empty())
            .ok_or_else(|| "truncated reader lease page contained no keys".to_string())?;
    }

    let cleanup_results = stream::iter(expired_leases)
        .map(|(key, value)| async move {
            let result = retire_expired_reader_lease(request, &key, value, trace_id).await;
            (key, result)
        })
        .buffer_unordered(READER_LEASE_DELETE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    for (key, result) in cleanup_results {
        match result {
            Ok(true) => {}
            Ok(false) => active = true,
            Err(error) => {
                active = true;
                tracing::warn!(
                    blob_guid = %request.blob_guid,
                    %key,
                    %error,
                    "expired reader lease retirement failed"
                );
            }
        }
    }

    Ok(active)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reader_lease_values_are_checked_conservatively() {
        assert!(reader_lease_is_active(&reader_lease_value(101, 7), 100));
        assert!(!reader_lease_is_active(&reader_lease_value(100, 7), 100));
        assert!(reader_lease_is_active(
            &Bytes::copy_from_slice(&101_u64.to_le_bytes()),
            100
        ));
        assert!(reader_lease_is_active(&Bytes::from_static(b"invalid"), 100));
    }
}
