//! Storage backend over the `fs_gateway` protocol.
//!
//! Inode primitives are one round trip each. Data is addressed by inode
//! key and logical block: the gateway owns blob identity, generations,
//! the row map and reclamation, and this side never names any of them.
//! Every request carries a session token issued by `Mount`; a gateway
//! that no longer recognises it (restart, expiry, or load-balancer
//! routing) answers `Unauthorized` and that worker's connection re-mounts
//! once and retries.

use bytes::Bytes;
use data_types::TraceId;
use data_types::object_layout::{InodeRecord, ObjectLayout, PosixAttrs};
use fs_gateway_codec::*;
use parking_lot::RwLock;
use rpc_client_common::rpc_retry;
use rpc_client_fs::RpcClientFs;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::config::Config;
use crate::error::FsError;

/// One listing entry: an empty layout is a common prefix (directory).
pub struct ListEntry {
    pub key: String,
    pub layout: Option<ObjectLayout>,
}

/// Outcome of `BeginFlush`: the opaque ticket the later steps carry and,
/// for an overwrite, the prepared layout now stored at the key.
pub struct Prepared {
    pub ticket: Bytes,
    pub layout: Option<ObjectLayout>,
}

/// Outcome of a delete: the removed bytes, and the hidden key the value
/// was moved to instead when the caller asked to orphan it.
pub struct Deleted {
    pub previous: Option<Bytes>,
    pub orphan_key: Option<String>,
}

/// The value a rename displaced at its destination, if any.
pub struct Displaced {
    pub previous: Bytes,
    pub orphan_key: Option<String>,
}

/// Mount capabilities shared by every worker thread.
pub struct Session {
    initial_token: Bytes,
    pub supports_s3_volume: bool,
}

/// Mount-time configuration shared across threads.
pub struct BackendConfig {
    pub config: Config,
    pub session: Arc<Session>,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn decode_layout(bytes: &[u8]) -> Result<ObjectLayout, FsError> {
    rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(bytes)
        .map_err(|e| FsError::Deserialize(e.to_string()))
}

fn encode_layout(layout: &ObjectLayout) -> Result<Bytes, FsError> {
    Ok(
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(layout, Vec::new())
            .map_err(FsError::from)?
            .into(),
    )
}

fn version_bytes(version_id: Option<Uuid>) -> Bytes {
    version_id
        .map(|id| Bytes::copy_from_slice(id.as_bytes()))
        .unwrap_or_default()
}

async fn mount(client: &RpcClientFs, config: &Config) -> Result<mount_response::Session, FsError> {
    let trace_id = TraceId::new();
    let timestamp_ms = unix_ms();
    let nonce: [u8; 16] = rand::random();
    let signature = if config.api_key_id.is_empty() {
        Vec::new()
    } else {
        mount_signature(
            config.api_key_secret.as_bytes(),
            &config.api_key_id,
            &config.bucket_name,
            timestamp_ms,
            &nonce,
        )
    };
    let req = MountRequest {
        bucket: config.bucket_name.clone(),
        read_write: config.read_write,
        api_key_id: config.api_key_id.clone(),
        timestamp_ms,
        nonce: Bytes::copy_from_slice(&nonce),
        signature: Bytes::from(signature),
    };
    let resp = rpc_retry!(
        "fs_gateway",
        client,
        mount(req.clone(), Some(config.rpc_request_timeout()), &trace_id)
    )
    .await?;
    match resp.result {
        Some(mount_response::Result::Ok(session)) => Ok(session),
        Some(mount_response::Result::Err(e)) => Err(FsError::from(e)),
        None => Err(FsError::Internal("empty mount response".into())),
    }
}

impl BackendConfig {
    /// Connect to the gateway and establish the mount session. Fails,
    /// rather than mounting partially, when the gateway refuses.
    pub async fn discover(config: &Config) -> Result<Self, String> {
        let client = RpcClientFs::new_from_addresses(
            config.gateway_addrs.clone(),
            config.rpc_connection_timeout(),
        );
        let session = mount(&client, config)
            .await
            .map_err(|e| format!("mount '{}' via gateway failed: {e}", config.bucket_name))?;
        tracing::info!(
            bucket = %config.bucket_name,
            gateway = ?config.gateway_addrs,
            read_write = config.read_write,
            supports_s3_volume = session.supports_s3_volume,
            "mounted via fs_gateway"
        );
        Ok(Self {
            config: config.clone(),
            session: Arc::new(Session {
                initial_token: session.token,
                supports_s3_volume: session.supports_s3_volume,
            }),
        })
    }
}

/// Per-thread gateway client. Created once per compio thread.
pub struct StorageBackend {
    client: RpcClientFs,
    session: Arc<Session>,
    token: RwLock<Bytes>,
    config: Config,
}

/// One gateway round trip with transport retry and a single transparent
/// re-mount when the session token is no longer accepted.
macro_rules! gateway_call {
    ($self:expr, $method:ident, $module:ident, $trace_id:expr, |$token:ident| $req:expr) => {
        async {
            let mut remounted = false;
            loop {
                let $token = $self.token();
                let resp = rpc_retry!(
                    "fs_gateway",
                    $self.client,
                    $method($req, Some($self.config.rpc_request_timeout()), $trace_id)
                )
                .await
                .map_err(FsError::Rpc)?;
                match resp.result {
                    Some($module::Result::Ok(v)) => break Ok(v),
                    Some($module::Result::Err(e))
                        if e.error_kind() == ErrorKind::Unauthorized && !remounted =>
                    {
                        tracing::info!(reason = %e.message, "session rejected; re-mounting");
                        $self.remount().await?;
                        remounted = true;
                    }
                    Some($module::Result::Err(e)) => break Err(FsError::from(e)),
                    None => {
                        break Err(FsError::Internal(concat!(
                            "empty ",
                            stringify!($method),
                            " response"
                        )
                        .into()));
                    }
                }
            }
        }
    };
}

/// Mutation call whose return value cannot be reconstructed after a lost
/// reply. Authentication rejection is safe to remount and retry because the
/// gateway rejects it before touching storage; transport failures surface.
macro_rules! gateway_call_once {
    ($self:expr, $method:ident, $module:ident, $trace_id:expr, |$token:ident| $req:expr) => {
        async {
            let mut remounted = false;
            loop {
                let $token = $self.token();
                let resp = $self
                    .client
                    .$method($req, Some($self.config.rpc_request_timeout()), $trace_id, 0)
                    .await
                    .map_err(FsError::Rpc)?;
                match resp.result {
                    Some($module::Result::Ok(v)) => break Ok(v),
                    Some($module::Result::Err(e))
                        if e.error_kind() == ErrorKind::Unauthorized && !remounted =>
                    {
                        tracing::info!(reason = %e.message, "session rejected; re-mounting");
                        $self.remount().await?;
                        remounted = true;
                    }
                    Some($module::Result::Err(e)) => break Err(FsError::from(e)),
                    None => {
                        break Err(FsError::Internal(concat!(
                            "empty ",
                            stringify!($method),
                            " response"
                        )
                        .into()));
                    }
                }
            }
        }
    };
}

impl StorageBackend {
    pub fn new(backend_config: &BackendConfig) -> Result<Self, String> {
        let client = RpcClientFs::new_from_addresses(
            backend_config.config.gateway_addrs.clone(),
            backend_config.config.rpc_connection_timeout(),
        );
        Ok(Self {
            client,
            session: backend_config.session.clone(),
            token: RwLock::new(backend_config.session.initial_token.clone()),
            config: backend_config.config.clone(),
        })
    }

    fn token(&self) -> Bytes {
        self.token.read().clone()
    }

    fn caller(&self, token: Bytes) -> Option<Caller> {
        Some(Caller { token })
    }

    async fn remount(&self) -> Result<(), FsError> {
        let session = mount(&self.client, &self.config).await?;
        *self.token.write() = session.token;
        Ok(())
    }

    /// Whether S3-resident blobs can be served by this mount.
    pub fn supports_s3_volume(&self) -> bool {
        self.session.supports_s3_volume
    }

    /// Fetch and decode an inode layout. The key has no trailing NUL.
    pub async fn get_inode(&self, key: &str, trace_id: &TraceId) -> Result<ObjectLayout, FsError> {
        let bytes = self.get_inode_raw(key, trace_id).await?;
        decode_layout(&bytes)
    }

    /// Raw value fetch: the stored bytes without the `ObjectLayout`
    /// decode (`@hardlink/` records are a different shape).
    pub async fn get_inode_raw(&self, key: &str, trace_id: &TraceId) -> Result<Bytes, FsError> {
        gateway_call!(self, get_inode, get_inode_response, trace_id, |token| {
            GetInodeRequest {
                caller: self.caller(token.clone()),
                key: key.to_string(),
            }
        })
        .await
    }

    /// List inodes under a prefix. An empty value is a common prefix (directory).
    pub async fn list_inodes(
        &self,
        prefix: &str,
        delimiter: &str,
        start_after: &str,
        max_keys: u32,
        trace_id: &TraceId,
    ) -> Result<Vec<ListEntry>, FsError> {
        let page = gateway_call!(self, list_inodes, list_inodes_response, trace_id, |token| {
            ListInodesRequest {
                caller: self.caller(token.clone()),
                prefix: prefix.to_string(),
                delimiter: delimiter.to_string(),
                start_after: start_after.to_string(),
                max_keys,
            }
        })
        .await?;
        let mut entries = Vec::with_capacity(page.entries.len());
        for entry in page.entries {
            let layout = if entry.value.is_empty() {
                None
            } else {
                Some(decode_layout(&entry.value).inspect_err(|e| {
                    tracing::error!(key = %entry.key, value_len = entry.value.len(), error = %e,
                        "list entry: rkyv deserialization failed");
                })?)
            };
            entries.push(ListEntry {
                key: entry.key,
                layout,
            });
        }
        Ok(entries)
    }

    /// List MPU parts for a completed multipart upload.
    pub async fn list_mpu_parts(
        &self,
        key: &str,
        upload_id: Uuid,
        trace_id: &TraceId,
    ) -> Result<Vec<(String, ObjectLayout)>, FsError> {
        let parts = gateway_call!(
            self,
            list_mpu_parts,
            list_mpu_parts_response,
            trace_id,
            |token| {
                ListMpuPartsRequest {
                    caller: self.caller(token.clone()),
                    key: key.to_string(),
                    upload_id: Bytes::copy_from_slice(upload_id.as_bytes()),
                }
            }
        )
        .await?;
        parts
            .parts
            .into_iter()
            .map(|p| decode_layout(&p.layout).map(|l| (p.key, l)))
            .collect()
    }

    /// Read logical block `block_number` of the file at `key` at its
    /// committed content, or `None` for a block that reads as zeros.
    /// `expected_version_id` is the layout the caller read against; the
    /// gateway answers `StaleLayout` if the key has moved past it.
    pub async fn read_block(
        &self,
        key: &str,
        block_number: u32,
        expected_version_id: Option<Uuid>,
        trace_id: &TraceId,
    ) -> Result<Option<Bytes>, FsError> {
        let block = gateway_call!(self, read_block, read_block_response, trace_id, |token| {
            ReadBlockRequest {
                caller: self.caller(token.clone()),
                key: key.to_string(),
                block_number,
                expected_version_id: version_bytes(expected_version_id),
            }
        })
        .await?;
        Ok((!block.hole).then_some(block.data))
    }

    /// Blocks of `[first_block, first_block + block_count)` holding data.
    pub async fn probe_data_blocks(
        &self,
        key: &str,
        first_block: u32,
        block_count: u32,
        expected_version_id: Option<Uuid>,
        trace_id: &TraceId,
    ) -> Result<Vec<u32>, FsError> {
        let blocks = gateway_call!(
            self,
            probe_data_blocks,
            probe_data_blocks_response,
            trace_id,
            |token| ProbeDataBlocksRequest {
                caller: self.caller(token.clone()),
                key: key.to_string(),
                first_block,
                block_count,
                expected_version_id: version_bytes(expected_version_id),
            }
        )
        .await?;
        Ok(blocks.data_blocks)
    }

    /// Prepare a flush of `key`. `expected` is the committed layout the
    /// dirty buffer was built against (`None` for a create).
    #[allow(clippy::too_many_arguments)]
    pub async fn begin_flush(
        &self,
        key: &str,
        expected: Option<&ObjectLayout>,
        file_size: u64,
        rewrites: &[u32],
        punched: &[u32],
        eof_low_watermark: Option<u32>,
        trim_upper: Option<u32>,
        trace_id: &TraceId,
    ) -> Result<Prepared, FsError> {
        let expected_layout = match expected {
            Some(layout) => encode_layout(layout)?,
            None => Bytes::new(),
        };
        let prepared = gateway_call!(self, begin_flush, begin_flush_response, trace_id, |token| {
            BeginFlushRequest {
                caller: self.caller(token.clone()),
                key: key.to_string(),
                expected_layout: expected_layout.clone(),
                file_size,
                rewrites: rewrites.to_vec(),
                punched: punched.to_vec(),
                eof_low_watermark,
                trim_upper,
            }
        })
        .await?;
        let layout = if prepared.layout.is_empty() {
            None
        } else {
            Some(decode_layout(&prepared.layout)?)
        };
        Ok(Prepared {
            ticket: prepared.ticket,
            layout,
        })
    }

    /// Ship one dirty block of a prepared flush.
    pub async fn write_flush_block(
        &self,
        ticket: &Bytes,
        block_number: u32,
        data: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        gateway_call!(
            self,
            write_flush_block,
            write_flush_block_response,
            trace_id,
            |token| WriteFlushBlockRequest {
                caller: self.caller(token.clone()),
                ticket: ticket.clone(),
                block_number,
                data: data.clone(),
            }
        )
        .await
    }

    /// Commit a prepared flush once every block is acknowledged. Returns
    /// the committed layout.
    pub async fn commit_flush(
        &self,
        ticket: &Bytes,
        rewrites: &[u32],
        punched: &[u32],
        posix: PosixAttrs,
        trace_id: &TraceId,
    ) -> Result<ObjectLayout, FsError> {
        let layout = gateway_call!(
            self,
            commit_flush,
            commit_flush_response,
            trace_id,
            |token| CommitFlushRequest {
                caller: self.caller(token.clone()),
                ticket: ticket.clone(),
                rewrites: rewrites.to_vec(),
                punched: punched.to_vec(),
                posix: Some(posix.into()),
            }
        )
        .await?;
        decode_layout(&layout)
    }

    /// Give up a prepared flush. Best effort: the gateway reclaims a
    /// create's fresh blob after the grace; an overwrite needs nothing.
    pub async fn abort_flush(&self, ticket: &Bytes) {
        let trace_id = TraceId::new();
        let result = gateway_call!(
            self,
            abort_flush,
            abort_flush_response,
            &trace_id,
            |token| {
                AbortFlushRequest {
                    caller: self.caller(token.clone()),
                    ticket: ticket.clone(),
                }
            }
        )
        .await;
        if let Err(e) = result {
            tracing::debug!(error = %e, "flush abort hint failed");
        }
    }

    /// Put (create/update) an inode. Returns the previous object bytes
    /// (empty if this is a new object). A value that names data must
    /// copy the binding stored at `proof_key`.
    pub async fn put_inode(
        &self,
        key: &str,
        value: Bytes,
        proof_key: Option<&str>,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        gateway_call!(self, put_inode, put_inode_response, trace_id, |token| {
            PutInodeRequest {
                caller: self.caller(token.clone()),
                key: key.to_string(),
                value: value.clone(),
                proof_key: proof_key.unwrap_or_default().to_string(),
            }
        })
        .await
    }

    /// Compare-and-swap publish: installs `value` at `key` only if the bytes
    /// currently stored match `expected_old_value` byte-for-byte (pass an
    /// empty `Bytes` to require absence). Returns the previous value bytes on
    /// success, or `FsError::CasConflict` when the guard fails.
    pub async fn put_inode_cas(
        &self,
        key: &str,
        value: Bytes,
        expected_old_value: Bytes,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        gateway_call!(
            self,
            put_inode_cas,
            put_inode_cas_response,
            trace_id,
            |token| {
                PutInodeCasRequest {
                    caller: self.caller(token.clone()),
                    key: key.to_string(),
                    value: value.clone(),
                    expected_old_value: expected_old_value.clone(),
                }
            }
        )
        .await
    }

    /// Fetch the `InodeRecord` backing a hardlink-promoted inode from its
    /// `@hardlink/<inode_id>` key.
    pub async fn get_inode_record(
        &self,
        inode_id: Uuid,
        trace_id: &TraceId,
    ) -> Result<InodeRecord, FsError> {
        let key = InodeRecord::key_for(inode_id);
        let bytes = self.get_inode_raw(&key, trace_id).await?;
        rkyv::from_bytes::<InodeRecord, rkyv::rancor::Error>(&bytes)
            .map_err(|e| FsError::Internal(format!("InodeRecord deserialization: {e}")))
    }

    /// Persist the `InodeRecord` for a hardlink-promoted inode. The
    /// record copies the layout published at `source_key`, which the
    /// gateway checks before accepting the data binding.
    pub async fn put_inode_record(
        &self,
        inode_id: Uuid,
        record: &InodeRecord,
        source_key: &str,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let key = InodeRecord::key_for(inode_id);
        let bytes: Bytes =
            rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(record, Vec::new())
                .map_err(FsError::from)?
                .into();
        self.put_inode(&key, bytes, Some(source_key), trace_id)
            .await?;
        Ok(())
    }

    /// Delete the `InodeRecord` for a hardlink inode whose last name was
    /// removed (nlink reached 0), reclaiming the shared blob.
    pub async fn delete_inode_record(
        &self,
        inode_id: Uuid,
        teardown: bool,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let key = InodeRecord::key_for(inode_id);
        self.delete_inode(&key, teardown, false, trace_id).await?;
        Ok(())
    }

    /// Delete an inode. `teardown` reclaims the data the value named;
    /// `orphan` moves a data-bearing value to a hidden key instead, for a
    /// file that is still open.
    pub async fn delete_inode(
        &self,
        key: &str,
        teardown: bool,
        orphan: bool,
        trace_id: &TraceId,
    ) -> Result<Deleted, FsError> {
        let deleted = gateway_call_once!(
            self,
            delete_inode,
            delete_inode_response,
            trace_id,
            |token| {
                DeleteInodeRequest {
                    caller: self.caller(token.clone()),
                    key: key.to_string(),
                    teardown,
                    orphan,
                }
            }
        )
        .await?;
        Ok(Deleted {
            previous: deleted.existed.then_some(deleted.previous),
            orphan_key: (!deleted.orphan_key.is_empty()).then_some(deleted.orphan_key),
        })
    }

    /// Rename a file (object). When `force_overwrite` is set and the
    /// destination exists, the gateway atomically replaces it and hands
    /// back the displaced value, reclaimed or orphaned as requested.
    pub async fn rename_file(
        &self,
        src_key: &str,
        dst_key: &str,
        force_overwrite: bool,
        teardown_displaced: bool,
        orphan_displaced: bool,
        trace_id: &TraceId,
    ) -> Result<Displaced, FsError> {
        let displaced =
            gateway_call_once!(self, rename_file, rename_file_response, trace_id, |token| {
                RenameFileRequest {
                    caller: self.caller(token.clone()),
                    src_key: src_key.to_string(),
                    dst_key: dst_key.to_string(),
                    force_overwrite,
                    teardown_displaced,
                    orphan_displaced,
                }
            })
            .await?;
        Ok(Displaced {
            previous: displaced.previous,
            orphan_key: (!displaced.orphan_key.is_empty()).then_some(displaced.orphan_key),
        })
    }

    /// Rename a folder (directory prefix).
    pub async fn rename_folder(
        &self,
        src_key: &str,
        dst_key: &str,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        gateway_call_once!(
            self,
            rename_folder,
            rename_folder_response,
            trace_id,
            |token| {
                RenameFolderRequest {
                    caller: self.caller(token.clone()),
                    src_key: src_key.to_string(),
                    dst_key: dst_key.to_string(),
                }
            }
        )
        .await
    }

    /// Create a directory marker.
    pub async fn put_dir_marker(&self, key: &str, trace_id: &TraceId) -> Result<(), FsError> {
        gateway_call!(
            self,
            put_dir_marker,
            put_dir_marker_response,
            trace_id,
            |token| {
                PutDirMarkerRequest {
                    caller: self.caller(token.clone()),
                    key: key.to_string(),
                }
            }
        )
        .await
    }

    /// Best-effort hint: warm the gateway disk cache with the file at
    /// `key`. Errors are logged, never surfaced.
    pub async fn prefetch_inode(&self, key: &str) {
        let trace_id = TraceId::new();
        let result = gateway_call!(
            self,
            prefetch_inode,
            prefetch_inode_response,
            &trace_id,
            |token| PrefetchInodeRequest {
                caller: self.caller(token.clone()),
                key: key.to_string(),
            }
        )
        .await;
        if let Err(e) = result {
            tracing::debug!(%key, error = %e, "prefetch hint failed");
        }
    }
}
