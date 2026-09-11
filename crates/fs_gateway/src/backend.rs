#![allow(clippy::await_holding_refcell_ref)]

use bytes::Bytes;
use data_types::{Bucket, DataBlobGuid, DataVgInfo, RoutingKey, TraceId};
use file_ops::{
    create_dir_marker_layout, mpu_get_part_prefix, parse_delete_inode, parse_list_inodes,
    parse_list_inodes_raw, parse_mpu_parts, parse_put_inode, parse_put_inode_cas,
};
use futures::{StreamExt, stream};
use rpc_client_common::RpcError;
use rpc_client_common::nss_rpc_retry;
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcClientRss;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::sync::Arc;
use uuid::Uuid;
use volume_group_proxy::{DataVgProxy, PutBlobOutcome};

use crate::config::Config;
use crate::error::FsError;
use crate::resolve::ResolveCaches;
use crate::s3_volume::S3DataVolume;
use data_types::object_layout::{HARDLINK_PREFIX, InodeRecord, ObjectLayout, ObjectState};

fn exact_blob_identities(
    entries: Vec<bss_codec::list_blob_blocks_response::BlobBlockEntry>,
) -> BTreeSet<(u32, u64)> {
    entries
        .into_iter()
        .map(|entry| (entry.block_number, entry.version))
        .collect()
}

/// Discovered configuration from RSS (shared across threads).
pub struct BackendConfig {
    pub bucket_name: String,
    /// Bumped per `Mount`; per-thread backends are rebuilt when it moves
    /// so a new mount starts with fresh RPC clients and circuit breakers,
    /// as a freshly started fs_server process used to.
    pub generation: u64,
    pub nss_address: String,
    pub data_vg_info: DataVgInfo,
    pub root_blob_name: String,
    pub routing_key: RoutingKey,
    pub config: Config,
    /// Present when an S3 bucket is configured; shared by every thread.
    pub s3: Option<Arc<S3DataVolume>>,
    /// Layout and row-map caches, carried across re-mounts of the same
    /// root blob.
    pub caches: Arc<ResolveCaches>,
}

impl BackendConfig {
    /// Perform one-time initialization: discover bucket info, NSS address, DataVgInfo from RSS.
    /// This runs on a compio runtime and creates temporary RPC connections.
    pub async fn discover(
        config: &Config,
        bucket_name: &str,
        s3: Option<Arc<S3DataVolume>>,
        generation: u64,
    ) -> Result<Self, String> {
        let trace_id = TraceId::new();

        // 1. Create RSS client
        let rss_client = RpcClientRss::new_from_addresses(
            config.rss_addrs.clone(),
            config.rpc_connection_timeout(),
        );

        // 2. Resolve bucket -> root_blob_name, routing_key. We fetch the
        //    bucket first so the NSS address lookup below can use the bucket's
        //    routing_key.
        let bucket_key = format!("bucket:{bucket_name}");
        let (_version, bucket_json) = rss_client
            .get(&bucket_key, Some(config.rss_rpc_timeout()), &trace_id, 0)
            .await
            .map_err(|e| format!("Failed to get bucket '{bucket_name}': {e}"))?;

        let bucket: Bucket = serde_json::from_str(&bucket_json)
            .map_err(|e| format!("Failed to parse bucket JSON: {e}"))?;
        tracing::info!(
            "Resolved bucket '{bucket_name}' -> root_blob_name '{}' routing_key {}",
            bucket.root_blob_name,
            bucket.routing_key
        );

        // 3. Get active NSS address from RSS for this bucket's routing_key
        let nss_addr = rss_client
            .get_active_nss_address(
                bucket.routing_key.as_bytes(),
                Some(config.rss_rpc_timeout()),
                &trace_id,
                0,
            )
            .await
            .map_err(|e| format!("Failed to get NSS address from RSS: {e}"))?;
        tracing::info!("Got NSS address: {nss_addr}");

        // 4. Get DataVgInfo from RSS
        let data_vg_info = rss_client
            .get_data_vg_info(Some(config.rss_rpc_timeout()), &trace_id)
            .await
            .map_err(|e| format!("Failed to get DataVgInfo from RSS: {e}"))?;
        tracing::info!("Got DataVgInfo with {} volumes", data_vg_info.volumes.len());

        Ok(Self {
            bucket_name: bucket_name.to_string(),
            generation,
            nss_address: nss_addr,
            data_vg_info,
            root_blob_name: bucket.root_blob_name,
            routing_key: bucket.routing_key,
            config: config.clone(),
            s3,
            caches: Arc::new(ResolveCaches::default()),
        })
    }
}

/// Per-thread storage backend using compio-native RPC clients.
/// Created once per compio thread via thread_local.
/// Safety: compio is single-threaded, so RefCell borrows across await are safe.
pub struct StorageBackend {
    rss_client: RpcClientRss,
    nss_client: RefCell<RpcClientNss>,
    nss_address: RefCell<String>,
    data_vg_proxy: DataVgProxy,
    s3: Option<Arc<S3DataVolume>>,
    root_blob_name: String,
    routing_key: RoutingKey,
    config: Config,
    pub(crate) caches: Arc<ResolveCaches>,
}

impl StorageBackend {
    /// Create a per-thread backend from discovered configuration.
    pub fn new(backend_config: &BackendConfig) -> Result<Self, String> {
        let conn_timeout = backend_config.config.rpc_connection_timeout();
        let nss_client =
            RpcClientNss::new_from_address(backend_config.nss_address.clone(), conn_timeout);
        let rss_client =
            RpcClientRss::new_from_addresses(backend_config.config.rss_addrs.clone(), conn_timeout);
        let data_vg_proxy = DataVgProxy::new(
            backend_config.data_vg_info.clone(),
            backend_config.config.rpc_request_timeout(),
            conn_timeout,
        )
        .map_err(|e| e.to_string())?
        .with_ec_hedge_delay(backend_config.config.ec_read_hedge_delay());

        Ok(Self {
            rss_client,
            nss_client: RefCell::new(nss_client),
            nss_address: RefCell::new(backend_config.nss_address.clone()),
            data_vg_proxy,
            s3: backend_config.s3.clone(),
            root_blob_name: backend_config.root_blob_name.clone(),
            routing_key: backend_config.routing_key,
            config: backend_config.config.clone(),
            caches: backend_config.caches.clone(),
        })
    }

    /// The layout published at `key`, following one hardlink redirect.
    /// `None` when nothing is stored there.
    pub async fn layout_at(
        &self,
        key: &str,
        trace_id: &TraceId,
    ) -> Result<Option<ObjectLayout>, FsError> {
        let bytes = match self.get_inode_raw(key, trace_id).await {
            Ok(bytes) => bytes,
            Err(FsError::NotFound) => return Ok(None),
            Err(error) => return Err(error),
        };
        if key.starts_with(HARDLINK_PREFIX) {
            let record = rkyv::from_bytes::<InodeRecord, rkyv::rancor::Error>(&bytes)?;
            return Ok(Some(record.layout));
        }
        let layout = rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&bytes)?;
        let ObjectState::Indirect(redirect) = &layout.state else {
            return Ok(Some(layout));
        };
        match self
            .get_inode_raw(&InodeRecord::key_for(redirect.inode_id), trace_id)
            .await
        {
            Ok(bytes) => Ok(Some(
                rkyv::from_bytes::<InodeRecord, rkyv::rancor::Error>(&bytes)?.layout,
            )),
            Err(FsError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// `Some` when the blob lives on the S3 volume; `Err` inside when this
    /// mount has no S3 access. `None` routes to the data volume group.
    fn s3_volume(&self, blob_guid: DataBlobGuid) -> Option<Result<&S3DataVolume, FsError>> {
        if blob_guid.volume_id != DataBlobGuid::S3_VOLUME {
            return None;
        }
        Some(self.s3.as_deref().ok_or(FsError::InvalidState))
    }

    /// Returns a borrow of the NSS client.
    pub async fn get_nss_rpc_client(&self) -> Result<std::cell::Ref<'_, RpcClientNss>, FsError> {
        Ok(self.nss_client.borrow())
    }

    /// Try to refresh NSS address from RSS when connection fails.
    pub async fn try_refresh_nss_address(&self, trace_id: &TraceId) -> bool {
        let current_addr = self.nss_address.borrow().clone();

        match self
            .rss_client
            .get_active_nss_address(
                self.routing_key.as_bytes(),
                Some(self.config.rss_rpc_timeout()),
                trace_id,
                0,
            )
            .await
        {
            Ok(new_addr) => {
                if current_addr != new_addr {
                    tracing::info!("NSS address changed: {} -> {}", current_addr, new_addr);
                    let new_client = RpcClientNss::new_from_address(
                        new_addr.clone(),
                        self.config.rpc_connection_timeout(),
                    );
                    *self.nss_address.borrow_mut() = new_addr;
                    *self.nss_client.borrow_mut() = new_client;
                    true
                } else {
                    false
                }
            }
            Err(e) => {
                tracing::warn!("Failed to refresh NSS address: {e}");
                false
            }
        }
    }

    /// Raw NSS value fetch: the stored bytes without the `ObjectLayout`
    /// decode. The `@ovr/` row path must bypass `parse_get_inode`, which
    /// hard-errors on non-layout values.
    pub async fn get_inode_raw(&self, key: &str, trace_id: &TraceId) -> Result<Bytes, FsError> {
        let resp = nss_rpc_retry!(
            self.nss_client.borrow(),
            get_inode(
                &self.root_blob_name,
                key,
                Some(self.config.rpc_request_timeout()),
                trace_id
            ),
            self,
            trace_id
        )
        .await?;
        match resp.result {
            Some(nss_codec::get_inode_response::Result::Ok(bytes)) => Ok(bytes),
            Some(nss_codec::get_inode_response::Result::ErrNotFound(()))
            | Some(nss_codec::get_inode_response::Result::ErrNoSuchRootBlob(())) => {
                Err(FsError::NotFound)
            }
            Some(nss_codec::get_inode_response::Result::ErrOther(e)) => Err(FsError::Internal(e)),
            None => Err(FsError::Internal("empty GetInodeResponse".into())),
        }
    }

    /// One raw listing page: `(key, value)` pairs plus the has_more flag.
    /// Bypasses `parse_list_inodes`, whose `ObjectLayout` decode
    /// hard-errors on raw `@ovr/` row values. Callers own pagination via
    /// `start_after`; the NSS page clamp makes ignoring `has_more` a
    /// silent-truncation bug.
    pub async fn list_inodes_raw_page(
        &self,
        prefix: &str,
        start_after: &str,
        max_keys: u32,
        trace_id: &TraceId,
    ) -> Result<(Vec<(String, Bytes)>, bool), FsError> {
        let resp = nss_rpc_retry!(
            self.nss_client.borrow(),
            list_inodes(
                &self.root_blob_name,
                max_keys,
                prefix,
                "",
                start_after,
                true,
                Some(self.config.rpc_request_timeout()),
                trace_id
            ),
            self,
            trace_id
        )
        .await?;
        match parse_list_inodes_raw(resp) {
            Ok(page) => Ok(page),
            Err(file_ops::NssError::NoSuchRootBlob) => Err(FsError::NotFound),
            Err(e) => Err(e.into()),
        }
    }

    /// One raw listing page with a delimiter: `(key, value)` pairs (an
    /// empty value is a common prefix) plus the has_more flag. The client
    /// decodes layouts; the gateway never interprets the values.
    pub async fn list_inodes_page(
        &self,
        prefix: &str,
        delimiter: &str,
        start_after: &str,
        max_keys: u32,
        trace_id: &TraceId,
    ) -> Result<(Vec<(String, Bytes)>, bool), FsError> {
        let resp = nss_rpc_retry!(
            self.nss_client.borrow(),
            list_inodes(
                &self.root_blob_name,
                max_keys,
                prefix,
                delimiter,
                start_after,
                true,
                Some(self.config.rpc_request_timeout()),
                trace_id
            ),
            self,
            trace_id
        )
        .await?;
        match parse_list_inodes_raw(resp) {
            Ok(page) => Ok(page),
            Err(file_ops::NssError::NoSuchRootBlob) => Err(FsError::NotFound),
            Err(e) => Err(e.into()),
        }
    }

    /// List MPU parts for a completed multipart upload
    pub async fn list_mpu_parts(
        &self,
        key: &str,
        upload_id: uuid::Uuid,
        trace_id: &TraceId,
    ) -> Result<Vec<(String, ObjectLayout)>, FsError> {
        let mpu_prefix = mpu_get_part_prefix(key.to_string(), upload_id, 0);
        let resp = nss_rpc_retry!(
            self.nss_client.borrow(),
            list_inodes(
                &self.root_blob_name,
                10000,
                &mpu_prefix,
                "",
                "",
                false,
                Some(self.config.rpc_request_timeout()),
                trace_id
            ),
            self,
            trace_id
        )
        .await?;

        Ok(parse_mpu_parts(parse_list_inodes(resp)?)?)
    }

    /// Read one exact generation `(blob_guid, block_number, version)` via
    /// DataVgProxy. The version comes from the committed block map (or 1
    /// for unmapped blocks), so there is no version selection: any replica
    /// holding the write-once key answers authoritatively.
    /// Returns `(data, xxh3_64_checksum)`.
    pub async fn read_block(
        &self,
        blob_guid: DataBlobGuid,
        version: u64,
        block_number: u32,
        content_len: usize,
        trace_id: &TraceId,
    ) -> Result<(Bytes, u64), FsError> {
        let mut body = Bytes::new();
        if let Some(s3) = self.s3_volume(blob_guid) {
            body = s3?.read_block(blob_guid, block_number, version).await?;
        } else {
            self.data_vg_proxy
                .get_blob(
                    blob_guid,
                    block_number,
                    version,
                    content_len,
                    &mut body,
                    trace_id,
                )
                .await?;
        }
        let checksum = xxhash_rust::xxh3::xxh3_64(&body);
        Ok((body, checksum))
    }

    /// Create a new data blob GUID on the configured data volume.
    pub fn create_blob_guid(&self) -> DataBlobGuid {
        if self.config.data_volume_is_s3() {
            return DataBlobGuid {
                blob_id: Uuid::now_v7(),
                volume_id: DataBlobGuid::S3_VOLUME,
            };
        }
        self.data_vg_proxy.create_data_blob_guid()
    }

    /// Write a single block to a data blob via DataVgProxy at a specific
    /// version. Override-style flush passes the bumped `blob_version`;
    /// initial-create passes `1`. Returns `false` when the write-once key
    /// was already present somewhere: the write still counts as done, but
    /// `body` is not known to be what is stored.
    pub async fn write_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        version: u64,
        trace_id: &TraceId,
    ) -> Result<bool, FsError> {
        if let Some(s3) = self.s3_volume(blob_guid) {
            return s3?
                .write_block(blob_guid, block_number, body, version)
                .await;
        }
        let outcome = self
            .data_vg_proxy
            .put_blob(blob_guid, block_number, body, version, trace_id)
            .await?;
        Ok(outcome == PutBlobOutcome::Stored)
    }

    /// Enumerate the BSS-visible block entries for one blob over
    /// `[first_block, first_block + block_count)`. Absent blocks are holes.
    /// Used by lseek(SEEK_DATA/SEEK_HOLE).
    pub async fn list_blob_blocks(
        &self,
        blob_guid: DataBlobGuid,
        first_block: u32,
        block_count: u32,
        trace_id: &TraceId,
    ) -> Result<Vec<bss_codec::list_blob_blocks_response::BlobBlockEntry>, FsError> {
        if let Some(s3) = self.s3_volume(blob_guid) {
            let end = first_block.saturating_add(block_count);
            let mut entries = s3?.list_blob_blocks(blob_guid).await?;
            entries.retain(|e| e.block_number >= first_block && e.block_number < end);
            return Ok(entries);
        }
        Ok(self
            .data_vg_proxy
            .list_blob_blocks(blob_guid, first_block, block_count, trace_id)
            .await?)
    }

    /// Enumerate every physical entry for a blob from every placement node.
    /// Whole-blob reclamation uses this stronger listing so abandoned keys
    /// that never reached a write quorum are not missed.
    pub async fn list_all_blob_blocks(
        &self,
        blob_guid: DataBlobGuid,
        trace_id: &TraceId,
    ) -> Result<Vec<bss_codec::list_blob_blocks_response::BlobBlockEntry>, FsError> {
        if let Some(s3) = self.s3_volume(blob_guid) {
            return s3?.list_blob_blocks(blob_guid).await;
        }
        Ok(self
            .data_vg_proxy
            .list_all_blob_blocks(blob_guid, trace_id)
            .await?)
    }

    /// Put (create/update) an inode in NSS. Returns the previous object bytes
    /// (empty if this is a new object).
    pub async fn put_inode(
        &self,
        key: &str,
        value: Bytes,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        let resp = nss_rpc_retry!(
            self.nss_client.borrow(),
            put_inode(
                &self.root_blob_name,
                key,
                value.clone(),
                Some(self.config.rpc_request_timeout()),
                trace_id
            ),
            self,
            trace_id
        )
        .await?;

        Ok(parse_put_inode(resp)?)
    }

    /// Compare-and-swap publish: installs `value` at `key` only if the bytes
    /// currently stored match `expected_old_value` byte-for-byte (pass an
    /// empty `Bytes` to require absence). Returns the previous value bytes on
    /// success, or `FsError::CasConflict` when the guard fails: the
    /// override-flush path uses that typed error to forward-retry against the
    /// winning snapshot instead of clobbering it.
    pub async fn put_inode_cas(
        &self,
        key: &str,
        value: Bytes,
        expected_old_value: Bytes,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        let resp = nss_rpc_retry!(
            self.nss_client.borrow(),
            put_inode_cas(
                &self.root_blob_name,
                key,
                value.clone(),
                expected_old_value.clone(),
                Some(self.config.rpc_request_timeout()),
                trace_id
            ),
            self,
            trace_id
        )
        .await?;

        Ok(parse_put_inode_cas(resp)?)
    }

    /// Delete an inode from NSS. Returns the previous object bytes, or None
    /// if the object was not found / already deleted.
    pub async fn delete_inode(
        &self,
        key: &str,
        trace_id: &TraceId,
    ) -> Result<Option<Bytes>, FsError> {
        let resp = nss_rpc_retry!(
            self.nss_client.borrow(),
            delete_inode(
                &self.root_blob_name,
                key,
                Some(self.config.rpc_request_timeout()),
                trace_id
            ),
            self,
            trace_id
        )
        .await?;

        Ok(parse_delete_inode(resp)?)
    }

    /// Rename an object (file) in NSS.
    /// Rename a file (object) in NSS. When `force_overwrite` is set and
    /// the destination already exists, NSS atomically replaces it and
    /// returns the prior dst value (otherwise empty) so the caller can
    /// GC the now-orphaned blob.
    pub async fn rename_file(
        &self,
        src_key: &str,
        dst_key: &str,
        force_overwrite: bool,
        trace_id: &TraceId,
    ) -> Result<Bytes, FsError> {
        let result = nss_rpc_retry!(
            self.nss_client.borrow(),
            rename_object(
                &self.root_blob_name,
                src_key,
                dst_key,
                force_overwrite,
                Some(self.config.rpc_request_timeout()),
                trace_id
            ),
            self,
            trace_id
        )
        .await;

        match result {
            Ok(old_bytes) => Ok(old_bytes),
            Err(RpcError::NotFound) => Err(FsError::NotFound),
            Err(RpcError::AlreadyExists) => Err(FsError::AlreadyExists),
            Err(e) => Err(e.into()),
        }
    }

    /// Rename a folder (directory prefix) in NSS.
    pub async fn rename_folder(
        &self,
        src_key: &str,
        dst_key: &str,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let result = nss_rpc_retry!(
            self.nss_client.borrow(),
            rename_folder(
                &self.root_blob_name,
                src_key,
                dst_key,
                Some(self.config.rpc_request_timeout()),
                trace_id
            ),
            self,
            trace_id
        )
        .await;

        match result {
            Ok(()) => Ok(()),
            Err(RpcError::NotFound) => Err(FsError::NotFound),
            Err(RpcError::AlreadyExists) => Err(FsError::AlreadyExists),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete a single data block at its exact version. Used by background
    /// cleanup after the inode CAS makes that generation unreachable.
    pub async fn delete_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        if let Some(s3) = self.s3_volume(blob_guid) {
            return s3?.delete_block(blob_guid, block_number, version).await;
        }
        self.data_vg_proxy
            .delete_blob(blob_guid, block_number, version, trace_id)
            .await?;
        Ok(())
    }

    /// Enumerate and delete every exact data or reservation key for a blob.
    /// This is proportional to allocated keys, not logical file size, and
    /// includes orphaned generations that no block map row references.
    pub async fn delete_blob_blocks(
        &self,
        blob_guid: DataBlobGuid,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let entries = self.list_all_blob_blocks(blob_guid, trace_id).await?;
        let identities = exact_blob_identities(entries);
        let results = stream::iter(identities)
            .map(|(block_number, version)| async move {
                self.delete_block(blob_guid, block_number, version, trace_id)
                    .await
            })
            .buffer_unordered(32)
            .collect::<Vec<_>>()
            .await;
        if let Some(error) = results.into_iter().find_map(Result::err) {
            return Err(error);
        }
        Ok(())
    }

    /// Create a directory marker in NSS.
    /// Stores a minimal ObjectLayout with size=0 because NSS rejects empty values.
    pub async fn put_dir_marker(&self, key: &str, trace_id: &TraceId) -> Result<(), FsError> {
        let layout = create_dir_marker_layout();
        let value: Vec<u8> =
            rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())?;
        self.put_inode(key, Bytes::from(value), trace_id).await?;
        Ok(())
    }
}
