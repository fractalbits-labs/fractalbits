//! Listener, per-connection request loop, and the command dispatcher.
//!
//! The gateway holds no per-mount state: every request is authorized
//! by its session token, resolved to a bucket backend, executed, and
//! answered. Requests on one connection run concurrently; replies are
//! matched by request id on the client.
//!
//! Authorization is the namespace: a session may only name keys in its
//! bucket, and data is addressed by key and logical block. The client
//! never names a blob or a generation, so nothing it sends can reach
//! another bucket's storage.

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
use data_types::object_layout::{
    HARDLINK_PREFIX, InodeRecord, MpuState, ORPHAN_PREFIX, ObjectLayout, ObjectState, orphan_key,
    posix_only_moved,
};
use data_types::ovr_map::{OVR_GC_PREFIX, OVR_ROW_PREFIX};
use fs_gateway_codec::*;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use prost::Message;
use rpc_client_rss::RpcClientRss;
use rpc_codec_common::MessageHeaderTrait;
use uuid::Uuid;

use crate::auth::{Auth, Session};
use crate::backend::{BackendConfig, StorageBackend};
use crate::config::Config;
use crate::disk_cache::{DiskCache, MIRROR_BYTE_BUDGET, MirrorHandle, MirrorJob};
use crate::error::FsError;
use crate::flush;
use crate::prefetch;
use crate::resolve::BlockRead;
use crate::s3_volume::S3DataVolume;
use crate::sweep::{
    SweepCoordinator, scavenge_teardown_markers, teardown_value, write_conditional_marker,
};

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

/// Internal keyspaces the client may not address at all. `@hardlink/`
/// records and `@orphan/` keys are namespace state the client
/// legitimately reads and publishes to.
const RESERVED_PREFIXES: [&str; 2] = [OVR_ROW_PREFIX, OVR_GC_PREFIX];

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

fn reject_reserved_key(key: &str) -> Result<(), FsError> {
    if RESERVED_PREFIXES
        .iter()
        .any(|prefix| key.starts_with(prefix))
    {
        return Err(FsError::Unauthorized("reserved gateway key".into()));
    }
    Ok(())
}

/// Optional 16-byte layout version id on a read request.
fn expected_version(bytes: &[u8]) -> Result<Option<Uuid>, FsError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    Uuid::from_slice(bytes)
        .map(Some)
        .map_err(|_| FsError::InvalidState)
}

/// A value that names data on shared storage: the data binding the
/// client may carry forward but never author.
fn names_data(layout: &ObjectLayout) -> bool {
    match &layout.state {
        ObjectState::Normal(data) => !data.blob_guid.blob_id.is_nil(),
        ObjectState::Mpu(MpuState::Completed(_)) => true,
        _ => false,
    }
}

/// A client-supplied inode value, decoded by key shape.
#[derive(Debug)]
struct ClientValue {
    layout: ObjectLayout,
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
            discovered.caches = previous.caches.clone();
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
    pub(crate) fn mirror_insert(
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
        reject_reserved_key(&req.key)?;
        let expected = expected_version(&req.expected_version_id)?;
        match backend
            .read_block_at_key(
                &req.key,
                req.block_number,
                expected,
                self.disk_cache.as_ref(),
                trace_id,
            )
            .await?
        {
            BlockRead::Data(data) => Ok(read_block_response::Block { data, hole: false }),
            BlockRead::Hole => Ok(read_block_response::Block {
                data: Bytes::new(),
                hole: true,
            }),
        }
    }

    async fn probe_data_blocks(
        &self,
        req: ProbeDataBlocksRequest,
        trace_id: &TraceId,
    ) -> Result<probe_data_blocks_response::Blocks, FsError> {
        let (_, _, backend) = self.authorize(req.caller.as_ref(), false)?;
        reject_reserved_key(&req.key)?;
        let expected = expected_version(&req.expected_version_id)?;
        let data_blocks = backend
            .probe_data_blocks(
                &req.key,
                req.first_block,
                req.block_count,
                expected,
                trace_id,
            )
            .await?;
        Ok(probe_data_blocks_response::Blocks { data_blocks })
    }

    async fn prefetch_inode(
        &self,
        req: PrefetchInodeRequest,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let (_, _, backend) = self.authorize(req.caller.as_ref(), false)?;
        reject_reserved_key(&req.key)?;
        let Some(dc) = &self.disk_cache else {
            return Ok(());
        };
        let layout = backend
            .resolve_layout(&req.key, None, false, trace_id)
            .await?;
        let Ok(blob_guid) = layout.blob_guid() else {
            return Ok(());
        };
        let file_size = layout.size()?;
        if prefetch::cache_pressure_high(
            dc.current_usage(),
            dc.capacity_bytes(),
            self.config.prefetch_pressure_decline,
        ) || dc.is_complete(blob_guid, file_size)
        {
            return Ok(());
        }
        let rows = backend.row_map_for(&layout, trace_id).await?;
        let plan = prefetch::prefetch_plan(&layout, rows.as_deref());
        if plan.is_empty() {
            return Ok(());
        }
        let Ok(permit) = self.prefetch_limit.clone().try_acquire_owned() else {
            return Ok(());
        };
        let dc = Arc::clone(dc);
        compio_runtime::spawn(async move {
            let _permit = permit;
            prefetch::prefetch_blob(backend, dc, blob_guid, plan).await;
        })
        .detach();
        Ok(())
    }

    /// Semantic checks on a layout a client wants stored.
    fn validate_object_layout(layout: &ObjectLayout) -> Result<(), FsError> {
        if layout.block_size != ObjectLayout::DEFAULT_BLOCK_SIZE {
            return Err(FsError::InvalidState);
        }
        let ObjectState::Normal(data) = &layout.state else {
            return Ok(());
        };
        if data.blob_guid.blob_id.is_nil() {
            return if data.core_meta_data.size == 0 {
                Ok(())
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
        Ok(())
    }

    /// Decode a client-supplied inode value by key shape and check its
    /// invariants before it lands where the S3 gateway and other mounts
    /// will parse it.
    fn decode_client_value(key: &str, value: &[u8]) -> Result<ClientValue, FsError> {
        reject_reserved_key(key)?;
        let layout = if key.starts_with(HARDLINK_PREFIX) {
            let record = rkyv::from_bytes::<InodeRecord, rkyv::rancor::Error>(value)
                .map_err(|e| FsError::Deserialize(format!("inode record rejected: {e}")))?;
            if matches!(record.layout.state, ObjectState::Indirect(_)) {
                return Err(FsError::InvalidState);
            }
            record.layout
        } else {
            if key.starts_with('@') && !key.starts_with(ORPHAN_PREFIX) {
                return Err(FsError::Unauthorized("reserved gateway key".into()));
            }
            rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(value)
                .map_err(|e| FsError::Deserialize(format!("layout rejected: {e}")))?
        };
        Self::validate_object_layout(&layout)?;
        Ok(ClientValue { layout })
    }

    /// A stored value the client may copy its data binding from: the one
    /// its CAS replaces, or the one at a proof key.
    fn binding_carried(proof: Option<&ObjectLayout>, new: &ObjectLayout) -> Result<(), FsError> {
        match proof {
            Some(proof) if posix_only_moved(proof, new) => Ok(()),
            _ => Err(FsError::Unauthorized(
                "a data binding may only be carried forward, never authored".into(),
            )),
        }
    }

    /// Validate a CAS publish: a value that names data must carry the
    /// binding of the value it replaces, differing at most in posix.
    fn validate_cas(key: &str, value: &[u8], expected_old: &[u8]) -> Result<(), FsError> {
        let new = Self::decode_client_value(key, value)?;
        if !names_data(&new.layout) {
            return Ok(());
        }
        let proof = if expected_old.is_empty() {
            None
        } else {
            Some(Self::decode_client_value(key, expected_old)?.layout)
        };
        Self::binding_carried(proof.as_ref(), &new.layout)
    }

    /// Validate a blind put: a value that names data must carry the
    /// binding stored at `proof_key` in this bucket (hardlink promotion
    /// copies the source layout into its shared record).
    async fn validate_put(
        backend: &StorageBackend,
        key: &str,
        value: &[u8],
        proof_key: &str,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let new = Self::decode_client_value(key, value)?;
        if !names_data(&new.layout) {
            return Ok(());
        }
        if proof_key.is_empty() {
            return Self::binding_carried(None, &new.layout);
        }
        reject_reserved_key(proof_key)?;
        let proof = backend.layout_at(proof_key, trace_id).await?;
        Self::binding_carried(proof.as_ref(), &new.layout)
    }

    /// `true` for a value whose data an unlinked-but-open handle still
    /// needs a key for.
    fn is_data_value(value: &[u8]) -> bool {
        rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(value)
            .map(|layout| names_data(&layout))
            .unwrap_or(false)
    }

    async fn delete_inode(
        &self,
        req: DeleteInodeRequest,
        trace_id: &TraceId,
    ) -> Result<delete_inode_response::Deleted, FsError> {
        let (_, cfg, backend) = self.authorize(req.caller.as_ref(), true)?;
        reject_reserved_key(&req.key)?;
        let current = if req.orphan || req.teardown {
            match backend.get_inode_raw(&req.key, trace_id).await {
                Ok(bytes) => Some(bytes),
                Err(FsError::NotFound) => None,
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        // Unlinked while open: keep the value reachable under a hidden
        // key so the open handles can still address its data; the last
        // close deletes that key with teardown.
        if req.orphan
            && let Some(previous) = current.as_ref().filter(|v| Self::is_data_value(v))
        {
            let orphan = orphan_key(Uuid::new_v4());
            match backend
                .rename_file(&req.key, &orphan, false, trace_id)
                .await
            {
                Ok(_) => {
                    backend.forget_layout(&req.key);
                    return Ok(delete_inode_response::Deleted {
                        existed: true,
                        previous: previous.clone(),
                        orphan_key: orphan,
                    });
                }
                Err(FsError::NotFound) => {
                    return Ok(delete_inode_response::Deleted::default());
                }
                Err(error) => return Err(error),
            }
        }
        if req.teardown
            && let Some(current) = current.as_ref()
            && let Ok(layout) = rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(current)
        {
            write_conditional_marker(&backend, &layout, &req.key, trace_id).await?;
        }
        let previous = backend.delete_inode(&req.key, trace_id).await?;
        backend.forget_layout(&req.key);
        if req.teardown
            && let Some(previous) = &previous
        {
            teardown_value(&self.sweep, &cfg, &backend, &req.key, previous, trace_id).await;
        }
        Ok(delete_inode_response::Deleted {
            existed: previous.is_some(),
            previous: previous.unwrap_or_default(),
            orphan_key: String::new(),
        })
    }

    async fn rename_file(
        &self,
        req: RenameFileRequest,
        trace_id: &TraceId,
    ) -> Result<rename_file_response::Displaced, FsError> {
        let (_, cfg, backend) = self.authorize(req.caller.as_ref(), true)?;
        reject_reserved_key(&req.src_key)?;
        reject_reserved_key(&req.dst_key)?;
        // Record the teardown intent for the displaced blob's rows before
        // the swap makes its blob_id unrecoverable.
        if req.teardown_displaced && !req.orphan_displaced {
            match backend.get_inode_raw(&req.dst_key, trace_id).await {
                Ok(current) => {
                    if let Ok(layout) =
                        rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&current)
                    {
                        write_conditional_marker(&backend, &layout, &req.dst_key, trace_id).await?;
                    }
                }
                Err(FsError::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        let displaced = backend
            .rename_file(&req.src_key, &req.dst_key, req.force_overwrite, trace_id)
            .await?;
        backend.forget_layout(&req.src_key);
        backend.forget_layout(&req.dst_key);
        if displaced.is_empty() {
            return Ok(rename_file_response::Displaced::default());
        }
        // The replaced destination is still open somewhere: republish it
        // under a hidden key so those handles keep a name to address it
        // by. The blob has no key for the few milliseconds in between,
        // and nothing tears down a blob without an explicit request.
        if req.orphan_displaced && Self::is_data_value(&displaced) {
            let orphan = orphan_key(Uuid::new_v4());
            backend
                .put_inode(&orphan, displaced.clone(), trace_id)
                .await?;
            return Ok(rename_file_response::Displaced {
                previous: displaced,
                orphan_key: orphan,
            });
        }
        if req.teardown_displaced {
            teardown_value(
                &self.sweep,
                &cfg,
                &backend,
                &req.dst_key,
                &displaced,
                trace_id,
            )
            .await;
        }
        Ok(rename_file_response::Displaced {
            previous: displaced,
            orphan_key: String::new(),
        })
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
                    reject_reserved_key(&req.key)?;
                    backend.get_inode_raw(&req.key, trace_id).await
                };
                respond!(GetInodeResponse, get_inode_response, run.await)
            }
            Command::ListInodes => {
                let req = decode!(ListInodesRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), false)?;
                    reject_reserved_key(&req.prefix)?;
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
                            .filter(|(key, _)| reject_reserved_key(key).is_ok())
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
                    Self::validate_put(&backend, &req.key, &req.value, &req.proof_key, trace_id)
                        .await?;
                    let previous = backend.put_inode(&req.key, req.value, trace_id).await?;
                    backend.forget_layout(&req.key);
                    Ok::<_, FsError>(previous)
                };
                respond!(PutInodeResponse, put_inode_response, run.await)
            }
            Command::PutInodeCas => {
                let req = decode!(PutInodeCasRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), true)?;
                    Self::validate_cas(&req.key, &req.value, &req.expected_old_value)?;
                    let previous = backend
                        .put_inode_cas(&req.key, req.value, req.expected_old_value, trace_id)
                        .await?;
                    backend.forget_layout(&req.key);
                    Ok::<_, FsError>(previous)
                };
                respond!(PutInodeCasResponse, put_inode_cas_response, run.await)
            }
            Command::DeleteInode => {
                let req = decode!(DeleteInodeRequest);
                respond!(
                    DeleteInodeResponse,
                    delete_inode_response,
                    self.delete_inode(req, trace_id).await
                )
            }
            Command::RenameFile => {
                let req = decode!(RenameFileRequest);
                respond!(
                    RenameFileResponse,
                    rename_file_response,
                    self.rename_file(req, trace_id).await
                )
            }
            Command::RenameFolder => {
                let req = decode!(RenameFolderRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), true)?;
                    reject_reserved_key(&req.src_key)?;
                    reject_reserved_key(&req.dst_key)?;
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
                    reject_reserved_key(&req.key)?;
                    backend.put_dir_marker(&req.key, trace_id).await
                };
                respond!(PutDirMarkerResponse, put_dir_marker_response, run.await)
            }
            Command::ListMpuParts => {
                let req = decode!(ListMpuPartsRequest);
                let run = async {
                    let (_, _, backend) = self.authorize(req.caller.as_ref(), false)?;
                    reject_reserved_key(&req.key)?;
                    let upload_id = uuid::Uuid::from_slice(&req.upload_id)
                        .map_err(|e| FsError::Internal(format!("invalid upload id: {e}")))?;
                    let parts = backend
                        .list_mpu_parts(&req.key, upload_id, trace_id)
                        .await?;
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
            Command::ReadBlock => {
                let req = decode!(ReadBlockRequest);
                respond!(
                    ReadBlockResponse,
                    read_block_response,
                    self.read_block(req, trace_id).await
                )
            }
            Command::ProbeDataBlocks => {
                let req = decode!(ProbeDataBlocksRequest);
                respond!(
                    ProbeDataBlocksResponse,
                    probe_data_blocks_response,
                    self.probe_data_blocks(req, trace_id).await
                )
            }
            Command::BeginFlush => {
                let req = decode!(BeginFlushRequest);
                let run = async {
                    let (session, _, backend) = self.authorize(req.caller.as_ref(), true)?;
                    reject_reserved_key(&req.key)?;
                    flush::begin(self, &backend, &session, req, trace_id).await
                };
                respond!(BeginFlushResponse, begin_flush_response, run.await)
            }
            Command::WriteFlushBlock => {
                let req = decode!(WriteFlushBlockRequest);
                let run = async {
                    let (session, _, backend) = self.authorize(req.caller.as_ref(), true)?;
                    flush::write_block(self, &backend, &session, req, trace_id).await
                };
                respond!(
                    WriteFlushBlockResponse,
                    write_flush_block_response,
                    run.await
                )
            }
            Command::CommitFlush => {
                let req = decode!(CommitFlushRequest);
                let run = async {
                    let (session, cfg, backend) = self.authorize(req.caller.as_ref(), true)?;
                    flush::commit(self, &cfg, &backend, &session, req, trace_id).await
                };
                respond!(CommitFlushResponse, commit_flush_response, run.await)
            }
            Command::AbortFlush => {
                let req = decode!(AbortFlushRequest);
                let run = async {
                    let (session, cfg, _) = self.authorize(req.caller.as_ref(), true)?;
                    flush::abort(self, &cfg, &session, req).await
                };
                respond!(AbortFlushResponse, abort_flush_response, run.await)
            }
            Command::PrefetchInode => {
                let req = decode!(PrefetchInodeRequest);
                respond!(
                    PrefetchInodeResponse,
                    prefetch_inode_response,
                    self.prefetch_inode(req, trace_id).await
                )
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
    use data_types::object_layout::{ObjectCoreMetaData, ObjectMetaData, PosixAttrs};

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
    fn layout_invariants_are_enforced() {
        let blob_id = uuid::Uuid::new_v4();
        let mut odd_block_size = normal_layout(blob_id, 4096, 1);
        odd_block_size.block_size = 0;
        Gateway::decode_client_value("/a", &encode(&odd_block_size)).expect_err("block_size 0");

        let zero_ceiling = normal_layout(blob_id, 4096, 0);
        Gateway::decode_client_value("/a", &encode(&zero_ceiling)).expect_err("blob_version 0");

        let nil_blob_with_data = normal_layout(uuid::Uuid::nil(), 4096, 1);
        Gateway::decode_client_value("/a", &encode(&nil_blob_with_data))
            .expect_err("nil blob with bytes");

        let empty = normal_layout(uuid::Uuid::nil(), 0, 1);
        Gateway::decode_client_value("/a", &encode(&empty)).expect("empty file");

        Gateway::decode_client_value("/a", b"not a layout").expect_err("garbage");
        Gateway::decode_client_value(&format!("{OVR_ROW_PREFIX}x/00000001"), &encode(&empty))
            .expect_err("reserved keyspace");
    }

    #[test]
    fn data_binding_is_carried_never_authored() {
        let blob_id = uuid::Uuid::new_v4();
        let stored = normal_layout(blob_id, 4096, 3);
        let mut chmodded = stored.clone();
        chmodded.set_fs_posix(Some(PosixAttrs {
            mode: 0o100600,
            ..PosixAttrs::default()
        }));
        Gateway::validate_cas("/a", &encode(&chmodded), &encode(&stored))
            .expect("posix-only change carries the binding");

        let mut grown = stored.clone();
        if let ObjectState::Normal(data) = &mut grown.state {
            data.core_meta_data.size = 8192;
        }
        Gateway::validate_cas("/a", &encode(&grown), &encode(&stored)).expect_err("size change");

        let foreign = normal_layout(uuid::Uuid::new_v4(), 4096, 3);
        Gateway::validate_cas("/a", &encode(&foreign), &encode(&stored)).expect_err("other blob");
        Gateway::validate_cas("/a", &encode(&stored), b"").expect_err("binding on create");

        let empty = normal_layout(uuid::Uuid::nil(), 0, 1);
        Gateway::validate_cas("/a", &encode(&empty), b"").expect("empty create");
        Gateway::validate_cas("/a", &encode(&empty), &encode(&stored))
            .expect("dropping a binding is allowed");
    }
}
