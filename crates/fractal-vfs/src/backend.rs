//! Storage backend over the `fs_gateway` protocol.
//!
//! The method set is the one `VfsCore` consumed from the in-cluster
//! backend; each call is one round trip to the gateway, which owns the
//! cluster clients, the disk cache, prefetch and reclamation. Every
//! request carries the session token issued by `Mount`; a gateway that
//! no longer recognises it (restart, expiry) answers `Unauthorized` and
//! the call transparently re-mounts once and retries.

use bytes::Bytes;
use data_types::object_layout::{InodeRecord, ObjectLayout};
use data_types::{DataBlobGuid, TraceId};
use fs_gateway_codec::prefetch_blob_request::PrefetchBlock;
use fs_gateway_codec::sweep_blob_request::BlockFloor;
use fs_gateway_codec::*;
use parking_lot::RwLock;
use rpc_client_common::rpc_retry;
use rpc_client_fs::RpcClientFs;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::Config;
use crate::error::FsError;

/// One listing entry: an empty layout is a common prefix (directory).
pub struct ListEntry {
    pub key: String,
    pub layout: Option<ObjectLayout>,
}

/// One stored generation of a block, as enumerated by the gateway.
#[derive(Debug, Clone, Copy)]
pub struct BlobBlockEntry {
    pub block_number: u32,
    pub version: u64,
}

/// Mount session shared by every worker thread.
pub struct Session {
    token: RwLock<Bytes>,
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
                token: RwLock::new(session.token),
                supports_s3_volume: session.supports_s3_volume,
            }),
        })
    }
}

/// Per-thread gateway client. Created once per compio thread.
pub struct StorageBackend {
    client: RpcClientFs,
    session: Arc<Session>,
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

impl StorageBackend {
    pub fn new(backend_config: &BackendConfig) -> Result<Self, String> {
        let client = RpcClientFs::new_from_addresses(
            backend_config.config.gateway_addrs.clone(),
            backend_config.config.rpc_connection_timeout(),
        );
        Ok(Self {
            client,
            session: backend_config.session.clone(),
            config: backend_config.config.clone(),
        })
    }

    fn token(&self) -> Bytes {
        self.session.token.read().clone()
    }

    fn caller(&self, token: Bytes) -> Option<Caller> {
        Some(Caller { token })
    }

    async fn remount(&self) -> Result<(), FsError> {
        let session = mount(&self.client, &self.config).await?;
        *self.session.token.write() = session.token;
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
    /// decode (the `@ovr/` row path stores non-layout values).
    pub async fn get_inode_raw(&self, key: &str, trace_id: &TraceId) -> Result<Bytes, FsError> {
        gateway_call!(self, get_inode, get_inode_response, trace_id, |token| {
            GetInodeRequest {
                caller: self.caller(token.clone()),
                key: key.to_string(),
            }
        })
        .await
    }

    async fn list_page(
        &self,
        prefix: &str,
        delimiter: &str,
        start_after: &str,
        max_keys: u32,
        trace_id: &TraceId,
    ) -> Result<(Vec<(String, Bytes)>, bool), FsError> {
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
        Ok((
            page.entries.into_iter().map(|e| (e.key, e.value)).collect(),
            page.has_more,
        ))
    }

    /// One raw listing page: `(key, value)` pairs plus the has_more flag.
    /// Callers own pagination via `start_after`; the server page clamp makes
    /// ignoring `has_more` a silent-truncation bug.
    pub async fn list_inodes_raw_page(
        &self,
        prefix: &str,
        start_after: &str,
        max_keys: u32,
        trace_id: &TraceId,
    ) -> Result<(Vec<(String, Bytes)>, bool), FsError> {
        self.list_page(prefix, "", start_after, max_keys, trace_id)
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
        let (page, _has_more) = self
            .list_page(prefix, delimiter, start_after, max_keys, trace_id)
            .await?;
        let mut entries = Vec::with_capacity(page.len());
        for (key, value) in page {
            let layout = if value.is_empty() {
                None
            } else {
                Some(decode_layout(&value).inspect_err(|e| {
                    tracing::error!(%key, value_len = value.len(), error = %e,
                        "list entry: rkyv deserialization failed");
                })?)
            };
            entries.push(ListEntry { key, layout });
        }
        Ok(entries)
    }

    /// List MPU parts for a completed multipart upload.
    pub async fn list_mpu_parts(
        &self,
        key: &str,
        upload_id: uuid::Uuid,
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

    /// Read one block at its exact committed generation. The gateway
    /// serves it from its disk cache when present.
    pub async fn read_block(
        &self,
        blob_guid: DataBlobGuid,
        version: u64,
        block_number: u32,
        content_len: usize,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        let block = gateway_call!(self, read_block, read_block_response, trace_id, |token| {
            ReadBlockRequest {
                caller: self.caller(token.clone()),
                blob: Some(BlobGuid::from(blob_guid)),
                block_number,
                version,
                content_len: content_len as u32,
            }
        })
        .await?;
        Ok(block.data)
    }

    /// Mint a fresh data blob GUID on the gateway's data volume.
    pub async fn create_blob_guid(&self, trace_id: &TraceId) -> Result<DataBlobGuid, FsError> {
        let guid = gateway_call!(
            self,
            allocate_blob_guid,
            allocate_blob_guid_response,
            trace_id,
            |token| AllocateBlobGuidRequest {
                caller: self.caller(token.clone()),
            }
        )
        .await?;
        DataBlobGuid::try_from(&guid).map_err(FsError::Internal)
    }

    /// Write a single block at a specific version. Override-style flush
    /// passes the bumped `blob_version`; initial-create passes `1`.
    pub async fn write_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        version: u64,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        gateway_call!(self, write_block, write_block_response, trace_id, |token| {
            WriteBlockRequest {
                caller: self.caller(token.clone()),
                blob: Some(BlobGuid::from(blob_guid)),
                block_number,
                version,
                data: body.clone(),
            }
        })
        .await
    }

    async fn list_blocks(
        &self,
        blob_guid: DataBlobGuid,
        first_block: u32,
        block_count: u32,
        all_nodes: bool,
        trace_id: &TraceId,
    ) -> Result<Vec<BlobBlockEntry>, FsError> {
        let entries = gateway_call!(
            self,
            list_blob_blocks,
            list_blob_blocks_response,
            trace_id,
            |token| ListBlobBlocksRequest {
                caller: self.caller(token.clone()),
                blob: Some(BlobGuid::from(blob_guid)),
                first_block,
                block_count,
                all_nodes,
            }
        )
        .await?;
        Ok(entries
            .entries
            .into_iter()
            .map(|e| BlobBlockEntry {
                block_number: e.block_number,
                version: e.version,
            })
            .collect())
    }

    /// Enumerate the committed block entries for one blob over
    /// `[first_block, first_block + block_count)`. Absent blocks are holes.
    pub async fn list_blob_blocks(
        &self,
        blob_guid: DataBlobGuid,
        first_block: u32,
        block_count: u32,
        trace_id: &TraceId,
    ) -> Result<Vec<BlobBlockEntry>, FsError> {
        self.list_blocks(blob_guid, first_block, block_count, false, trace_id)
            .await
    }

    /// Enumerate every physical entry for a blob from every placement node.
    pub async fn list_all_blob_blocks(
        &self,
        blob_guid: DataBlobGuid,
        trace_id: &TraceId,
    ) -> Result<Vec<BlobBlockEntry>, FsError> {
        self.list_blocks(blob_guid, 0, u32::MAX, true, trace_id)
            .await
    }

    /// Put (create/update) an inode. Returns the previous object bytes
    /// (empty if this is a new object).
    pub async fn put_inode(
        &self,
        key: &str,
        value: Bytes,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        gateway_call!(self, put_inode, put_inode_response, trace_id, |token| {
            PutInodeRequest {
                caller: self.caller(token.clone()),
                key: key.to_string(),
                value: value.clone(),
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
        inode_id: uuid::Uuid,
        trace_id: &TraceId,
    ) -> Result<InodeRecord, FsError> {
        let key = InodeRecord::key_for(inode_id);
        let bytes = self.get_inode_raw(&key, trace_id).await?;
        rkyv::from_bytes::<InodeRecord, rkyv::rancor::Error>(&bytes)
            .map_err(|e| FsError::Internal(format!("InodeRecord deserialization: {e}")))
    }

    /// Persist the `InodeRecord` for a hardlink-promoted inode.
    pub async fn put_inode_record(
        &self,
        inode_id: uuid::Uuid,
        record: &InodeRecord,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let key = InodeRecord::key_for(inode_id);
        let bytes: Bytes =
            rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(record, Vec::new())
                .map_err(FsError::from)?
                .into();
        self.put_inode(&key, bytes, trace_id).await?;
        Ok(())
    }

    /// Delete the `InodeRecord` for a hardlink inode whose last name was
    /// removed (nlink reached 0).
    pub async fn delete_inode_record(
        &self,
        inode_id: uuid::Uuid,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let key = InodeRecord::key_for(inode_id);
        self.delete_inode(&key, trace_id).await?;
        Ok(())
    }

    /// Delete an inode. Returns the previous object bytes, or None
    /// if the object was not found / already deleted.
    pub async fn delete_inode(
        &self,
        key: &str,
        trace_id: &TraceId,
    ) -> Result<Option<Bytes>, FsError> {
        let deleted = gateway_call!(
            self,
            delete_inode,
            delete_inode_response,
            trace_id,
            |token| {
                DeleteInodeRequest {
                    caller: self.caller(token.clone()),
                    key: key.to_string(),
                }
            }
        )
        .await?;
        Ok(deleted.existed.then_some(deleted.previous))
    }

    /// Rename a file (object). When `force_overwrite` is set and
    /// the destination already exists, the gateway atomically replaces it and
    /// returns the prior dst value (otherwise empty).
    pub async fn rename_file(
        &self,
        src_key: &str,
        dst_key: &str,
        force_overwrite: bool,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        gateway_call!(self, rename_file, rename_file_response, trace_id, |token| {
            RenameFileRequest {
                caller: self.caller(token.clone()),
                src_key: src_key.to_string(),
                dst_key: dst_key.to_string(),
                force_overwrite,
            }
        })
        .await
    }

    /// Rename a folder (directory prefix).
    pub async fn rename_folder(
        &self,
        src_key: &str,
        dst_key: &str,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        gateway_call!(
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

    /// Delete a single data block at its exact version.
    pub async fn delete_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        gateway_call!(
            self,
            delete_block,
            delete_block_response,
            trace_id,
            |token| {
                DeleteBlockRequest {
                    caller: self.caller(token.clone()),
                    blob: Some(BlobGuid::from(blob_guid)),
                    block_number,
                    version,
                }
            }
        )
        .await
    }

    /// Enumerate and delete every exact data or reservation key for a blob.
    pub async fn delete_blob_blocks(
        &self,
        blob_guid: DataBlobGuid,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        gateway_call!(
            self,
            delete_blob_blocks,
            delete_blob_blocks_response,
            trace_id,
            |token| DeleteBlobBlocksRequest {
                caller: self.caller(token.clone()),
                blob: Some(BlobGuid::from(blob_guid)),
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

    /// Best-effort hint: warm the gateway disk cache with these exact
    /// block identities. Errors are logged, never surfaced.
    pub async fn prefetch_blob(
        &self,
        blob_guid: DataBlobGuid,
        file_size: u64,
        blocks: Vec<PrefetchBlock>,
    ) {
        let trace_id = TraceId::new();
        let result = gateway_call!(
            self,
            prefetch_blob,
            prefetch_blob_response,
            &trace_id,
            |token| PrefetchBlobRequest {
                caller: self.caller(token.clone()),
                blob: Some(BlobGuid::from(blob_guid)),
                file_size,
                blocks: blocks.clone(),
            }
        )
        .await;
        if let Err(e) = result {
            tracing::debug!(%blob_guid, error = %e, "prefetch hint failed");
        }
    }

    /// Hand reclamation work to the gateway. Best effort: the durable
    /// `@ovr-gc/` markers cover a lost teardown; a lost superseded-block
    /// sweep leaks invisible garbage until the block is rewritten.
    #[allow(clippy::too_many_arguments)]
    pub async fn sweep_blob(
        &self,
        blob_guid: DataBlobGuid,
        victims: Vec<(u32, u64)>,
        below: Vec<(u32, u64)>,
        delete_all_blocks: bool,
        delete_rows: bool,
        with_grace: bool,
        marker_data_pending_unix_ms: Option<u64>,
    ) {
        let trace_id = TraceId::new();
        let victims: Vec<BlockIdentity> = victims
            .into_iter()
            .map(|(block_number, version)| BlockIdentity {
                block_number,
                version,
            })
            .collect();
        let below: Vec<BlockFloor> = below
            .into_iter()
            .map(|(block_number, keep_from)| BlockFloor {
                block_number,
                keep_from,
            })
            .collect();
        let result = gateway_call!(self, sweep_blob, sweep_blob_response, &trace_id, |token| {
            SweepBlobRequest {
                caller: self.caller(token.clone()),
                blob: Some(BlobGuid::from(blob_guid)),
                victims: victims.clone(),
                below: below.clone(),
                delete_all_blocks,
                delete_rows,
                with_grace,
                marker_data_pending_unix_ms,
            }
        })
        .await;
        if let Err(e) = result {
            tracing::warn!(%blob_guid, error = %e, "sweep hint failed; garbage may remain");
        }
    }
}
