//! Listener, per-connection request loop, and the command dispatcher.
//!
//! The gateway holds no per-mount state: every request is authorized
//! by its session token, resolved to a bucket backend, executed, and
//! answered. Requests on one connection run concurrently; replies are
//! matched by request id on the client.

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};
use compio_buf::BufResult;
use compio_io::{AsyncReadExt, AsyncWriteExt};
use compio_net::{TcpListener, TcpSocket, TcpStream};
use data_types::TraceId;
use data_types::object_layout::{HARDLINK_PREFIX, InodeRecord, ObjectLayout, ObjectState};
use data_types::ovr_map::{OvrRow, parse_ovr_row_block};
use fs_gateway_codec::*;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use prost::Message;
use rpc_client_rss::RpcClientRss;
use rpc_codec_common::MessageHeaderTrait;

use crate::auth::{Auth, Session};
use crate::backend::{BLOB_OWNER_PREFIX, BackendConfig, StorageBackend};
use crate::config::Config;
use crate::disk_cache::{DiskCache, MIRROR_BYTE_BUDGET, MirrorHandle, MirrorJob};
use crate::error::FsError;
use crate::prefetch;
use crate::s3_volume::S3DataVolume;
use crate::sweep::{SweepCoordinator, enqueue_sweep_request, scavenge_teardown_markers};

const HEADER_SIZE: usize = size_of::<MessageHeader>();
/// Largest encoded request accepted. Data commands apply the tighter block
/// limit after protobuf decoding.
const MAX_BODY: usize = 1024 * 1024;
const LISTEN_BACKLOG: i32 = 1024;
const REPLY_QUEUE: usize = 4096;
/// In-flight request bounds. The reader stops pulling frames off a
/// connection while it is at its limit, so a slow backend backpressures
/// the client instead of growing detached tasks without bound. A mount
/// runs one connection per worker thread with up to sixteen concurrent
/// block writes per flush plus readahead, so the per-connection bound
/// stays well above what one thread issues.
const CONNECTION_INFLIGHT_LIMIT: usize = 256;
const GLOBAL_INFLIGHT_LIMIT: usize = 2048;
const PREFETCH_INFLIGHT_LIMIT: usize = 8;
const MAX_PREFETCH_BLOCKS: usize = 8192;

pub struct Gateway {
    pub config: Arc<Config>,
    pub auth: Auth,
    pub disk_cache: Option<Arc<DiskCache>>,
    pub mirror: Option<MirrorHandle>,
    /// Shared S3 data volume (one SDK runtime per process), when configured.
    s3: Option<Arc<S3DataVolume>>,
    pub sweep: Arc<SweepCoordinator>,
    /// Bucket resolution, refreshed by every `Mount` so a bucket that
    /// was deleted and recreated under the same name (new root blob) is
    /// never served through a stale backend.
    buckets: parking_lot::Mutex<HashMap<String, Arc<BackendConfig>>>,
    mount_generation: AtomicU64,
    request_limit: Arc<tokio::sync::Semaphore>,
    prefetch_limit: Arc<tokio::sync::Semaphore>,
}

thread_local! {
    static BACKENDS: RefCell<HashMap<String, (u64, Rc<StorageBackend>)>> =
        RefCell::new(HashMap::new());
    static RSS_CLIENT: RefCell<Option<&'static RpcClientRss>> = const { RefCell::new(None) };
}

/// Per-thread `StorageBackend` for a bucket, rebuilt when the bucket's
/// mount generation moves. The old one is released once its in-flight
/// requests finish.
fn thread_backend(cfg: &Arc<BackendConfig>) -> Result<Rc<StorageBackend>, FsError> {
    BACKENDS.with(|cell| {
        if let Some((generation, backend)) = cell.borrow().get(&cfg.root_blob_name)
            && *generation == cfg.generation
        {
            return Ok(backend.clone());
        }
        let backend = Rc::new(StorageBackend::new(cfg).map_err(FsError::Internal)?);
        cell.borrow_mut().insert(
            cfg.root_blob_name.clone(),
            (cfg.generation, backend.clone()),
        );
        Ok(backend)
    })
}

fn thread_rss(config: &Config) -> &'static RpcClientRss {
    RSS_CLIENT.with(|cell| {
        if let Some(c) = *cell.borrow() {
            return c;
        }
        let client = RpcClientRss::new_from_addresses(
            config.rss_addrs.clone(),
            config.rpc_connection_timeout(),
        );
        let leaked: &'static RpcClientRss = Box::leak(Box::new(client));
        *cell.borrow_mut() = Some(leaked);
        leaked
    })
}

fn encode<M: Message>(m: &M) -> Bytes {
    let mut buf = BytesMut::with_capacity(m.encoded_len());
    m.encode(&mut buf).expect("BytesMut encode is infallible");
    buf.freeze()
}

/// Wrap a handler outcome into a `oneof result` response body.
macro_rules! respond {
    ($resp:ident, $module:ident, $result:expr) => {{
        let result = match $result {
            Ok(v) => $module::Result::Ok(v),
            Err(e) => $module::Result::Err(e.to_wire()),
        };
        encode(&$resp {
            result: Some(result),
        })
    }};
}

fn decode_guid(guid: Option<&BlobGuid>) -> Result<data_types::DataBlobGuid, FsError> {
    let guid = guid.ok_or_else(|| FsError::Internal("missing blob guid".into()))?;
    data_types::DataBlobGuid::try_from(guid).map_err(FsError::Internal)
}

impl Gateway {
    pub fn new(config: Arc<Config>) -> Self {
        let disk_cache = if config.disk_cache_enabled {
            match DiskCache::new(
                &config.disk_cache_path,
                config.disk_cache_size_gb,
                ObjectLayout::DEFAULT_BLOCK_SIZE as u64,
            ) {
                Ok(dc) => {
                    tracing::info!(
                        path = %config.disk_cache_path,
                        size_gb = config.disk_cache_size_gb,
                        "disk cache enabled"
                    );
                    Some(Arc::new(dc))
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to init disk cache, falling back to no cache");
                    None
                }
            }
        } else {
            None
        };
        let mirror = disk_cache
            .as_ref()
            .and_then(|dc| crate::disk_cache::spawn_mirror_worker(dc.clone()));
        let s3 = match S3DataVolume::from_config(&config) {
            Ok(s3) => s3.map(Arc::new),
            Err(e) => {
                tracing::error!(error = %e, "S3 data volume configuration invalid");
                std::process::exit(1);
            }
        };
        Self {
            auth: Auth::new(&config),
            config,
            disk_cache,
            mirror,
            s3,
            sweep: Arc::new(SweepCoordinator::default()),
            buckets: parking_lot::Mutex::new(HashMap::new()),
            mount_generation: AtomicU64::new(1),
            request_limit: Arc::new(tokio::sync::Semaphore::new(GLOBAL_INFLIGHT_LIMIT)),
            prefetch_limit: Arc::new(tokio::sync::Semaphore::new(PREFETCH_INFLIGHT_LIMIT)),
        }
    }

    fn bucket_config(&self, bucket: &str) -> Result<Arc<BackendConfig>, FsError> {
        self.buckets
            .lock()
            .get(bucket)
            .cloned()
            .ok_or_else(|| FsError::Unauthorized("bucket not mounted on this gateway".into()))
    }

    async fn resolve_bucket(&self, bucket: &str) -> Result<Arc<BackendConfig>, FsError> {
        let generation = self.mount_generation.fetch_add(1, Ordering::Relaxed);
        let mut discovered =
            BackendConfig::discover(&self.config, bucket, self.s3.clone(), generation)
                .await
                .map_err(FsError::Internal)?;
        if let Some(previous) = self.buckets.lock().get(bucket)
            && previous.root_blob_name == discovered.root_blob_name
        {
            discovered.owned_blobs = previous.owned_blobs.clone();
        }
        let cfg = Arc::new(discovered);
        self.buckets.lock().insert(bucket.to_string(), cfg.clone());
        Ok(cfg)
    }

    /// Verify the session token, enforce read-only, and resolve the
    /// bucket's per-thread backend.
    fn authorize(
        &self,
        caller: Option<&Caller>,
        mutating: bool,
    ) -> Result<(Session, Arc<BackendConfig>, Rc<StorageBackend>), FsError> {
        let token: &[u8] = caller.map(|c| c.token.as_ref()).unwrap_or(&[]);
        let session = self.auth.verify_token(token)?;
        if mutating && !session.read_write {
            return Err(FsError::ReadOnly);
        }
        let cfg = self.bucket_config(&session.bucket)?;
        let backend = thread_backend(&cfg)?;
        Ok((session, cfg, backend))
    }

    /// Best-effort disk-cache population off the request path.
    fn mirror_insert(
        &self,
        blob_guid: data_types::DataBlobGuid,
        block: u32,
        version: u64,
        data: Bytes,
    ) {
        let Some(mirror) = &self.mirror else {
            return;
        };
        let byte_len = data.len();
        let queued = mirror.queued_bytes.fetch_add(byte_len, Ordering::Relaxed);
        if queued + byte_len > MIRROR_BYTE_BUDGET {
            mirror.queued_bytes.fetch_sub(byte_len, Ordering::Relaxed);
            tracing::trace!(%blob_guid, block, "disk cache mirror byte budget exceeded; dropping");
            return;
        }
        let job = MirrorJob {
            blob_guid,
            block,
            version,
            data,
        };
        if let Err(e) = mirror.tx.clone().try_send(job) {
            mirror.queued_bytes.fetch_sub(byte_len, Ordering::Relaxed);
            if e.is_full() {
                tracing::trace!(%blob_guid, block, "disk cache mirror queue full; dropping");
            } else {
                tracing::warn!(%blob_guid, block, "disk cache mirror channel closed; dropping");
            }
        }
    }

    // ---------- handlers ----------

    async fn mount(
        &self,
        req: MountRequest,
        trace_id: &TraceId,
    ) -> Result<mount_response::Session, FsError> {
        let rss = thread_rss(&self.config);
        self.auth
            .verify_mount(&req, rss, self.config.rss_rpc_timeout(), trace_id)
            .await?;
        let cfg = self.resolve_bucket(&req.bucket).await?;
        tracing::info!(
            bucket = %cfg.bucket_name,
            read_write = req.read_write,
            "mount session issued"
        );
        // One scavenge pass per mount, as the old per-process fs_server
        // did at startup: markers a crashed teardown left behind become
        // row-teardown work again.
        compio_runtime::spawn(scavenge_teardown_markers(cfg.clone(), self.sweep.clone())).detach();
        let session = Session {
            bucket: req.bucket,
            read_write: req.read_write,
        };
        Ok(mount_response::Session {
            token: self.auth.issue_token(&session),
            supports_s3_volume: cfg.s3.is_some(),
        })
    }

    async fn read_block(
        &self,
        req: ReadBlockRequest,
        trace_id: &TraceId,
    ) -> Result<read_block_response::Block, FsError> {
        let (_, _, backend) = self.authorize(req.caller.as_ref(), false)?;
        let blob_guid = decode_guid(req.blob.as_ref())?;
        let content_len = req.content_len as usize;
        if content_len > ObjectLayout::DEFAULT_BLOCK_SIZE as usize || req.version == 0 {
            return Err(FsError::InvalidState);
        }
        backend
            .verify_blob_owner(blob_guid, &req.key, trace_id)
            .await?;
        if let Some(dc) = &self.disk_cache
            && let Some(cached) = dc
                .get_block_exact(blob_guid, req.block_number, req.version, content_len)
                .await
        {
            return Ok(read_block_response::Block { data: cached });
        }
        let (mut data, _checksum) = backend
            .read_block(
                blob_guid,
                req.version,
                req.block_number,
                content_len,
                trace_id,
            )
            .await?;
        if data.len() > content_len {
            data = data.slice(0..content_len);
        }
        // Cold fill inline (as the in-process cache did) so a re-read
        // right after this one is already a disk hit.
        if let Some(dc) = &self.disk_cache {
            let _ = dc
                .insert_block(blob_guid, req.block_number, req.version, &data)
                .await;
        }
        Ok(read_block_response::Block { data })
    }

    async fn write_block(&self, req: WriteBlockRequest, trace_id: &TraceId) -> Result<(), FsError> {
        let (_, _, backend) = self.authorize(req.caller.as_ref(), true)?;
        let blob_guid = decode_guid(req.blob.as_ref())?;
        // BSS rejects anything above one shard; enforce it here so a bad
        // client cannot reach that check on a shared node.
        if req.data.len() > ObjectLayout::DEFAULT_BLOCK_SIZE as usize || req.version == 0 {
            return Err(FsError::InvalidState);
        }
        backend
            .verify_blob_owner(blob_guid, &req.key, trace_id)
            .await?;
        let stored = backend
            .write_block(
                blob_guid,
                req.block_number,
                req.data.clone(),
                req.version,
                trace_id,
            )
            .await?;
        // A write-once key that already existed kept its original bytes;
        // mirroring the caller's copy would poison the disk cache.
        if stored {
            self.mirror_insert(blob_guid, req.block_number, req.version, req.data);
        }
        Ok(())
    }

    async fn prefetch_blob(
        &self,
        req: PrefetchBlobRequest,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let (_, _, backend) = self.authorize(req.caller.as_ref(), false)?;
        let Some(dc) = &self.disk_cache else {
            return Ok(());
        };
        let blob_guid = decode_guid(req.blob.as_ref())?;
        backend
            .verify_blob_owner(blob_guid, &req.key, trace_id)
            .await?;
        if req.blocks.len() > MAX_PREFETCH_BLOCKS
            || req.blocks.iter().any(|plan| {
                plan.version == 0
                    || plan.content_len > ObjectLayout::DEFAULT_BLOCK_SIZE
                    || plan.read_len > ObjectLayout::DEFAULT_BLOCK_SIZE
                    || plan.content_len > plan.read_len
            })
        {
            return Err(FsError::InvalidState);
        }
        if prefetch::cache_pressure_high(
            dc.current_usage(),
            dc.capacity_bytes(),
            self.config.prefetch_pressure_decline,
        ) || dc.is_complete(blob_guid, req.file_size)
        {
            return Ok(());
        }
        let Ok(permit) = self.prefetch_limit.clone().try_acquire_owned() else {
            return Ok(());
        };
        let dc = Arc::clone(dc);
        compio_runtime::spawn(async move {
            let _permit = permit;
            prefetch::prefetch_blob(backend, dc, blob_guid, req.blocks).await;
        })
        .detach();
        Ok(())
    }

    fn reject_owner_key(key: &str) -> Result<(), FsError> {
        if key.starts_with(BLOB_OWNER_PREFIX) {
            return Err(FsError::Unauthorized("reserved gateway key".into()));
        }
        Ok(())
    }

    /// Semantic checks on a layout a client wants stored. Returns the
    /// data blob it names, if any, for the ownership check.
    fn validate_object_layout(
        layout: &ObjectLayout,
    ) -> Result<Option<data_types::DataBlobGuid>, FsError> {
        if layout.block_size != ObjectLayout::DEFAULT_BLOCK_SIZE {
            return Err(FsError::InvalidState);
        }
        let ObjectState::Normal(data) = &layout.state else {
            return Ok(None);
        };
        if data.blob_guid.blob_id.is_nil() {
            return if data.core_meta_data.size == 0 {
                Ok(None)
            } else {
                Err(FsError::InvalidState)
            };
        }
        // Readers select generations `<= blob_version`; 0 would hide every
        // block. Block numbers are u32 on the wire.
        if layout.blob_version == 0
            || data.core_meta_data.size.div_ceil(layout.block_size as u64) > u32::MAX as u64
        {
            return Err(FsError::InvalidState);
        }
        Ok(Some(data.blob_guid))
    }

    /// Validate a client-supplied inode value before it lands where the
    /// S3 gateway and other mounts will parse it. Returns the data blob
    /// the value names, if any.
    fn validate_layout(
        key: &str,
        value: &[u8],
    ) -> Result<Option<data_types::DataBlobGuid>, FsError> {
        Self::reject_owner_key(key)?;
        if key.starts_with(HARDLINK_PREFIX) {
            let record = rkyv::from_bytes::<InodeRecord, rkyv::rancor::Error>(value)
                .map_err(|e| FsError::Deserialize(format!("inode record rejected: {e}")))?;
            if matches!(record.layout.state, ObjectState::Indirect(_)) {
                return Err(FsError::InvalidState);
            }
            return Self::validate_object_layout(&record.layout);
        }
        if parse_ovr_row_block(key).is_some() {
            return OvrRow::decode(value)
                .map(|_| None)
                .ok_or_else(|| FsError::Deserialize("row rejected".into()));
        }
        // Remaining internal keyspaces (`@ovr/` abort records, `@ovr-gc/`
        // markers) hold opaque values the gateway never interprets.
        if key.starts_with('@') {
            return Ok(None);
        }
        let layout = rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(value)
            .map_err(|e| FsError::Deserialize(format!("layout rejected: {e}")))?;
        Self::validate_object_layout(&layout)
    }

    /// Serving a layout out of this bucket's namespace proves the blob it
    /// names belongs here; remember that so the data requests that follow
    /// skip the marker lookup.
    fn note_served_layout(backend: &StorageBackend, key: &str, value: &[u8]) {
        if key.starts_with('@') && !key.starts_with(HARDLINK_PREFIX) {
            return;
        }
        let layout = if key.starts_with(HARDLINK_PREFIX) {
            rkyv::from_bytes::<InodeRecord, rkyv::rancor::Error>(value)
                .ok()
                .map(|record| record.layout)
        } else {
            rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(value).ok()
        };
        if let Some(blob_guid) = layout.and_then(|l| l.blob_guid().ok()) {
            backend.note_owned_blob(blob_guid);
        }
    }

    /// Store an inode value the client supplied: validate it, and prove
    /// any blob it names belongs to this bucket (the value already stored
    /// at the same key is one acceptable proof, so republishing a
    /// layout the S3 API created works without a marker).
    async fn store_inode(
        &self,
        backend: &StorageBackend,
        key: &str,
        value: &[u8],
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        if let Some(blob_guid) = Self::validate_layout(key, value)? {
            backend.verify_blob_owner(blob_guid, key, trace_id).await?;
        }
        Ok(())
    }

    /// Decode, authorize, execute, encode. Returns `None` for a request
    /// the gateway cannot even decode; the connection is then dropped.
    async fn dispatch(&self, command: Command, body: Bytes, trace_id: &TraceId) -> Option<Bytes> {
        macro_rules! decode {
            ($ty:ty) => {
                match <$ty>::decode(body) {
                    Ok(req) => req,
                    Err(e) => {
                        tracing::warn!(?command, error = %e, "malformed request");
                        return None;
                    }
                }
            };
        }
        let out = match command {
            Command::Mount => {
                let req = decode!(MountRequest);
                respond!(
                    MountResponse,
                    mount_response,
                    self.mount(req, trace_id).await
                )
            }
            Command::GetInode => {
                let req = decode!(GetInodeRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), false)?;
                    Self::reject_owner_key(&req.key)?;
                    let value = backend.get_inode_raw(&req.key, trace_id).await?;
                    Self::note_served_layout(&backend, &req.key, &value);
                    Ok::<_, FsError>(value)
                };
                respond!(GetInodeResponse, get_inode_response, run.await)
            }
            Command::ListInodes => {
                let req = decode!(ListInodesRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), false)?;
                    let (entries, has_more) = backend
                        .list_inodes_page(
                            &req.prefix,
                            &req.delimiter,
                            &req.start_after,
                            req.max_keys,
                            trace_id,
                        )
                        .await?;
                    Ok::<_, FsError>(list_inodes_response::Page {
                        entries: entries
                            .into_iter()
                            .filter(|(key, _)| !key.starts_with(BLOB_OWNER_PREFIX))
                            .map(|(key, value)| list_inodes_response::Entry { key, value })
                            .collect(),
                        has_more,
                    })
                };
                respond!(ListInodesResponse, list_inodes_response, run.await)
            }
            Command::PutInode => {
                let req = decode!(PutInodeRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), true)?;
                    self.store_inode(&backend, &req.key, &req.value, trace_id)
                        .await?;
                    backend.put_inode(&req.key, req.value, trace_id).await
                };
                respond!(PutInodeResponse, put_inode_response, run.await)
            }
            Command::PutInodeCas => {
                let req = decode!(PutInodeCasRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), true)?;
                    self.store_inode(&backend, &req.key, &req.value, trace_id)
                        .await?;
                    backend
                        .put_inode_cas(&req.key, req.value, req.expected_old_value, trace_id)
                        .await
                };
                respond!(PutInodeCasResponse, put_inode_cas_response, run.await)
            }
            Command::DeleteInode => {
                let req = decode!(DeleteInodeRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), true)?;
                    Self::reject_owner_key(&req.key)?;
                    let previous = backend.delete_inode(&req.key, trace_id).await?;
                    // The teardown hint for the displaced blob follows; its
                    // only proof of ownership was the value just removed.
                    if let Some(previous) = &previous {
                        Self::note_served_layout(&backend, &req.key, previous);
                    }
                    Ok::<_, FsError>(delete_inode_response::Deleted {
                        existed: previous.is_some(),
                        previous: previous.unwrap_or_default(),
                    })
                };
                respond!(DeleteInodeResponse, delete_inode_response, run.await)
            }
            Command::RenameFile => {
                let req = decode!(RenameFileRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), true)?;
                    Self::reject_owner_key(&req.src_key)?;
                    Self::reject_owner_key(&req.dst_key)?;
                    let displaced = backend
                        .rename_file(&req.src_key, &req.dst_key, req.force_overwrite, trace_id)
                        .await?;
                    if !displaced.is_empty() {
                        Self::note_served_layout(&backend, &req.dst_key, &displaced);
                    }
                    Ok::<_, FsError>(displaced)
                };
                respond!(RenameFileResponse, rename_file_response, run.await)
            }
            Command::RenameFolder => {
                let req = decode!(RenameFolderRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), true)?;
                    Self::reject_owner_key(&req.src_key)?;
                    Self::reject_owner_key(&req.dst_key)?;
                    backend
                        .rename_folder(&req.src_key, &req.dst_key, trace_id)
                        .await
                };
                respond!(RenameFolderResponse, rename_folder_response, run.await)
            }
            Command::PutDirMarker => {
                let req = decode!(PutDirMarkerRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), true)?;
                    Self::reject_owner_key(&req.key)?;
                    backend.put_dir_marker(&req.key, trace_id).await
                };
                respond!(PutDirMarkerResponse, put_dir_marker_response, run.await)
            }
            Command::ListMpuParts => {
                let req = decode!(ListMpuPartsRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), false)?;
                    let upload_id = uuid::Uuid::from_slice(&req.upload_id)
                        .map_err(|e| FsError::Internal(format!("invalid upload id: {e}")))?;
                    let parts = backend
                        .list_mpu_parts(&req.key, upload_id, trace_id)
                        .await?;
                    for (_, layout) in &parts {
                        if let Ok(blob_guid) = layout.blob_guid() {
                            backend.note_owned_blob(blob_guid);
                        }
                    }
                    let mut out = Vec::with_capacity(parts.len());
                    for (key, layout) in parts {
                        let bytes: Vec<u8> = rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(
                            &layout,
                            Vec::new(),
                        )?;
                        out.push(list_mpu_parts_response::Part {
                            key,
                            layout: Bytes::from(bytes),
                        });
                    }
                    Ok::<_, FsError>(list_mpu_parts_response::Parts { parts: out })
                };
                respond!(ListMpuPartsResponse, list_mpu_parts_response, run.await)
            }
            Command::AllocateBlobGuid => {
                let req = decode!(AllocateBlobGuidRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), true)?;
                    let blob_guid = backend.create_blob_guid();
                    backend.claim_blob(blob_guid, trace_id).await?;
                    Ok::<_, FsError>(BlobGuid::from(blob_guid))
                };
                respond!(
                    AllocateBlobGuidResponse,
                    allocate_blob_guid_response,
                    run.await
                )
            }
            Command::ReadBlock => {
                let req = decode!(ReadBlockRequest);
                respond!(
                    ReadBlockResponse,
                    read_block_response,
                    self.read_block(req, trace_id).await
                )
            }
            Command::WriteBlock => {
                let req = decode!(WriteBlockRequest);
                respond!(
                    WriteBlockResponse,
                    write_block_response,
                    self.write_block(req, trace_id).await
                )
            }
            Command::ListBlobBlocks => {
                let req = decode!(ListBlobBlocksRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), false)?;
                    let blob_guid = decode_guid(req.blob.as_ref())?;
                    backend
                        .verify_blob_owner(blob_guid, &req.key, trace_id)
                        .await?;
                    let entries = if req.all_nodes {
                        backend.list_all_blob_blocks(blob_guid, trace_id).await?
                    } else {
                        backend
                            .list_blob_blocks(blob_guid, req.first_block, req.block_count, trace_id)
                            .await?
                    };
                    Ok::<_, FsError>(list_blob_blocks_response::Entries {
                        entries: entries
                            .into_iter()
                            .map(|e| BlockIdentity {
                                block_number: e.block_number,
                                version: e.version,
                            })
                            .collect(),
                    })
                };
                respond!(ListBlobBlocksResponse, list_blob_blocks_response, run.await)
            }
            Command::PrefetchBlob => {
                let req = decode!(PrefetchBlobRequest);
                respond!(
                    PrefetchBlobResponse,
                    prefetch_blob_response,
                    self.prefetch_blob(req, trace_id).await
                )
            }
            Command::SweepBlob => {
                let req = decode!(SweepBlobRequest);
                let run = async {
                    let (_, cfg, backend) = self.authorize(req.caller.as_ref(), true)?;
                    let blob_guid = decode_guid(req.blob.as_ref())?;
                    if req.victims.iter().any(|victim| victim.version == 0)
                        || req.below.iter().any(|floor| floor.keep_from == 0)
                        || (req.delete_rows && !req.delete_all_blocks)
                    {
                        return Err(FsError::InvalidState);
                    }
                    backend
                        .verify_blob_owner(blob_guid, &req.key, trace_id)
                        .await?;
                    // What is reclaimable is re-derived by the sweep worker
                    // from this bucket's rows and layouts, off the request path.
                    enqueue_sweep_request(&self.sweep, cfg, blob_guid, &req);
                    Ok::<_, FsError>(())
                };
                respond!(SweepBlobResponse, sweep_blob_response, run.await)
            }
            Command::Invalid | Command::Handshake => {
                tracing::warn!(?command, "unsupported command");
                return None;
            }
        };
        Some(out)
    }
}

fn reply_header(id: u32, command: Command, trace_id: &TraceId, body: &[u8]) -> MessageHeader {
    let mut resp = MessageHeader(rpc_codec_common::ProtobufMessageHeader {
        id,
        command: command as i32,
        size: (HEADER_SIZE + body.len()) as u32,
        ..Default::default()
    });
    resp.set_trace_id(trace_id);
    resp.set_body_checksum(body);
    resp.set_checksum();
    resp
}

/// One accepted connection: read frames, run each request as its own
/// task, and serialize replies through a single writer task.
async fn serve_connection(gw: Arc<Gateway>, stream: TcpStream, peer: SocketAddr) {
    let (mut reader, mut writer) = stream.into_split();
    let (tx, mut rx) = mpsc::channel::<(MessageHeader, Bytes)>(REPLY_QUEUE);
    let connection_limit = Arc::new(tokio::sync::Semaphore::new(CONNECTION_INFLIGHT_LIMIT));

    let writer_task = compio_runtime::spawn(async move {
        while let Some((header, body)) = rx.next().await {
            let mut out = Vec::with_capacity(HEADER_SIZE + body.len());
            out.extend_from_slice(header.encode());
            out.extend_from_slice(&body);
            let BufResult(r, _) = writer.write_all(out).await;
            if let Err(e) = r {
                tracing::debug!(%peer, error = %e, "reply write failed; closing connection");
                break;
            }
        }
    });

    let mut header_buf = vec![0u8; HEADER_SIZE];
    loop {
        let BufResult(r, buf) = reader.read_exact(header_buf).await;
        header_buf = buf;
        match r {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                tracing::debug!(%peer, error = %e, "connection read failed");
                break;
            }
        }
        if !MessageHeader::verify_header_checksum_raw(&header_buf) {
            tracing::warn!(%peer, "header checksum mismatch; closing connection");
            break;
        }
        let header = MessageHeader::decode(&header_buf);
        if header.get_size() < HEADER_SIZE {
            tracing::warn!(%peer, size = header.get_size(), "invalid frame size; closing connection");
            break;
        }
        let command = match Command::try_from(header.command) {
            Ok(command) => command,
            Err(_) => {
                tracing::warn!(%peer, command = header.command, "invalid command; closing connection");
                break;
            }
        };
        let body_size = header.get_body_size();
        if body_size > MAX_BODY {
            tracing::warn!(%peer, body_size, "oversized request; closing connection");
            break;
        }
        let body = if body_size > 0 {
            let BufResult(r, buf) = reader.read_exact(vec![0u8; body_size]).await;
            if let Err(e) = r {
                tracing::debug!(%peer, error = %e, "body read failed");
                break;
            }
            Bytes::from(buf)
        } else {
            Bytes::new()
        };
        if !header.verify_body_checksum(&body) {
            tracing::warn!(%peer, "body checksum mismatch; closing connection");
            break;
        }

        let connection_permit = match connection_limit.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break,
        };
        let global_permit = match gw.request_limit.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => break,
        };
        let gw = gw.clone();
        let mut tx = tx.clone();
        compio_runtime::spawn(async move {
            let _connection_permit = connection_permit;
            let _global_permit = global_permit;
            let trace_id = header.get_trace_id();
            let Some(resp_body) = gw.dispatch(command, body, &trace_id).await else {
                return;
            };
            let resp = reply_header(header.id, command, &trace_id, &resp_body);
            let _ = tx.send((resp, resp_body)).await;
        })
        .detach();
    }
    drop(tx);
    let _ = writer_task.await;
}

/// Accept loop for one worker thread; every worker binds the same port
/// with `SO_REUSEPORT` so the kernel spreads connections across them.
pub async fn run_worker(gw: Arc<Gateway>, worker_id: usize) -> std::io::Result<()> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), gw.config.port);
    let socket = TcpSocket::new_v4().await?;
    socket.set_reuseaddr(true)?;
    socket.set_reuseport(true)?;
    socket.bind(addr).await?;
    let listener: TcpListener = socket.listen(LISTEN_BACKLOG).await?;
    tracing::info!(worker_id, %addr, "fs_gateway listening");
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                tracing::warn!(error = %e, "accept failed");
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        tracing::debug!(worker_id, %peer, "connection accepted");
        compio_runtime::spawn(serve_connection(gw.clone(), stream, peer)).detach();
    }
}

#[cfg(test)]
mod validate_tests {
    use super::*;
    use data_types::DataBlobGuid;
    use data_types::object_layout::{ObjectCoreMetaData, ObjectMetaData};
    use data_types::ovr_map::{OVR_ROW_LEN, ovr_row_key};

    fn normal_layout(blob_id: uuid::Uuid, size: u64, blob_version: u64) -> ObjectLayout {
        ObjectLayout {
            timestamp: 0,
            version_id: ObjectLayout::gen_version_id(),
            block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
            blob_version,
            fs_ext: None,
            state: ObjectState::Normal(ObjectMetaData {
                blob_guid: DataBlobGuid {
                    blob_id,
                    volume_id: 1,
                },
                core_meta_data: ObjectCoreMetaData {
                    size,
                    ..Default::default()
                },
            }),
        }
    }

    fn encode(layout: &ObjectLayout) -> Vec<u8> {
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(layout, Vec::new())
            .expect("layout encodes")
    }

    #[test]
    fn valid_layout_yields_its_blob() {
        let blob_id = uuid::Uuid::new_v4();
        let layout = normal_layout(blob_id, 4096, 1);
        let blob = Gateway::validate_layout("/a", &encode(&layout)).expect("valid layout");
        assert_eq!(blob.map(|g| g.blob_id), Some(blob_id));
    }

    #[test]
    fn layout_invariants_are_enforced() {
        let blob_id = uuid::Uuid::new_v4();
        let mut odd_block_size = normal_layout(blob_id, 4096, 1);
        odd_block_size.block_size = 0;
        Gateway::validate_layout("/a", &encode(&odd_block_size)).expect_err("block_size 0");

        let zero_ceiling = normal_layout(blob_id, 4096, 0);
        Gateway::validate_layout("/a", &encode(&zero_ceiling)).expect_err("blob_version 0");

        let nil_blob_with_data = normal_layout(uuid::Uuid::nil(), 4096, 1);
        Gateway::validate_layout("/a", &encode(&nil_blob_with_data))
            .expect_err("nil blob with bytes");

        let empty = normal_layout(uuid::Uuid::nil(), 0, 1);
        assert!(
            Gateway::validate_layout("/a", &encode(&empty))
                .expect("empty file")
                .is_none()
        );

        Gateway::validate_layout("/a", b"not a layout").expect_err("garbage");
        Gateway::validate_layout(&format!("{BLOB_OWNER_PREFIX}00001/x"), &encode(&empty))
            .expect_err("reserved keyspace");
    }

    #[test]
    fn rows_must_decode() {
        let key = ovr_row_key(&uuid::Uuid::new_v4(), 3);
        Gateway::validate_layout(&key, &[0u8; OVR_ROW_LEN - 1]).expect_err("short row");
    }
}
