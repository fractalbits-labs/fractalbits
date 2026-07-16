#![allow(clippy::await_holding_refcell_ref)]

use bytes::Bytes;
use data_types::{Bucket, DataBlobGuid, DataVgInfo, RoutingKey, TraceId};
use file_ops::{
    ListEntry, create_dir_marker_layout, mpu_get_part_prefix, parse_delete_inode, parse_get_inode,
    parse_list_inodes, parse_mpu_parts, parse_put_inode, parse_put_inode_cas,
};
use rpc_client_common::RpcError;
use rpc_client_common::nss_rpc_retry;
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcClientRss;
use std::cell::RefCell;
use volume_group_proxy::DataVgProxy;

use crate::config::Config;
use crate::error::FsError;
use data_types::object_layout::{InodeRecord, ObjectLayout};

/// Discovered configuration from RSS (shared across threads).
pub struct BackendConfig {
    pub nss_address: String,
    pub data_vg_info: DataVgInfo,
    pub root_blob_name: String,
    pub routing_key: RoutingKey,
    pub config: Config,
}

impl BackendConfig {
    /// Perform one-time initialization: discover bucket info, NSS address, DataVgInfo from RSS.
    /// This runs on a compio runtime and creates temporary RPC connections.
    pub async fn discover(config: &Config) -> Result<Self, String> {
        let trace_id = TraceId::new();

        // 1. Create RSS client
        let rss_client = RpcClientRss::new_from_addresses(
            config.rss_addrs.clone(),
            config.rpc_connection_timeout(),
        );

        // 2. Resolve bucket -> root_blob_name, routing_key. We fetch the
        //    bucket first so the NSS address lookup below can use the bucket's
        //    routing_key.
        let bucket_key = format!("bucket:{}", config.bucket_name);
        let (_version, bucket_json) = rss_client
            .get(&bucket_key, Some(config.rss_rpc_timeout()), &trace_id, 0)
            .await
            .map_err(|e| format!("Failed to get bucket '{}': {e}", config.bucket_name))?;

        let bucket: Bucket = serde_json::from_str(&bucket_json)
            .map_err(|e| format!("Failed to parse bucket JSON: {e}"))?;
        tracing::info!(
            "Resolved bucket '{}' -> root_blob_name '{}' routing_key {}",
            config.bucket_name,
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
            nss_address: nss_addr,
            data_vg_info,
            root_blob_name: bucket.root_blob_name,
            routing_key: bucket.routing_key,
            config: config.clone(),
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
    root_blob_name: String,
    routing_key: RoutingKey,
    config: Config,
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
        .map_err(|e| e.to_string())?;

        Ok(Self {
            rss_client,
            nss_client: RefCell::new(nss_client),
            nss_address: RefCell::new(backend_config.nss_address.clone()),
            data_vg_proxy,
            root_blob_name: backend_config.root_blob_name.clone(),
            routing_key: backend_config.routing_key,
            config: backend_config.config.clone(),
        })
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

    /// Get inode from NSS. The key should NOT have the trailing \0
    /// (the NSS client adds it).
    pub async fn get_inode(&self, key: &str, trace_id: &TraceId) -> Result<ObjectLayout, FsError> {
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

        Ok(parse_get_inode(resp)?)
    }

    /// Fetch the raw bytes of an NSS record at `key` without decoding them
    /// as an `ObjectLayout`. Used for side records in the NSS keyspace that
    /// carry their own schema (`#bmap/...` block-map chunks).
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
            Some(nss_codec::get_inode_response::Result::Ok(b)) => Ok(b),
            Some(nss_codec::get_inode_response::Result::ErrNotFound(()))
            | Some(nss_codec::get_inode_response::Result::ErrNoSuchRootBlob(())) => {
                Err(FsError::NotFound)
            }
            Some(nss_codec::get_inode_response::Result::ErrOther(e)) => Err(FsError::Internal(e)),
            None => Err(FsError::Internal("empty GetInodeResponse".into())),
        }
    }

    /// List inodes from NSS. Returns (key, Option<ObjectLayout>).
    /// Empty inode data means common prefix (directory).
    pub async fn list_inodes(
        &self,
        prefix: &str,
        delimiter: &str,
        start_after: &str,
        max_keys: u32,
        trace_id: &TraceId,
    ) -> Result<Vec<ListEntry>, FsError> {
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

        Ok(parse_list_inodes(resp)?.entries)
    }

    /// List only the KEYS under an NSS prefix, without decoding values as
    /// `ObjectLayout` (side records like `#bmap/...` chunks carry their own
    /// schema and would fail the typed list parse).
    pub async fn list_inode_keys(
        &self,
        prefix: &str,
        max_keys: u32,
        trace_id: &TraceId,
    ) -> Result<Vec<String>, FsError> {
        let resp = nss_rpc_retry!(
            self.nss_client.borrow(),
            list_inodes(
                &self.root_blob_name,
                max_keys,
                prefix,
                "",
                "",
                true,
                Some(self.config.rpc_request_timeout()),
                trace_id
            ),
            self,
            trace_id
        )
        .await?;
        match resp.result {
            Some(nss_codec::list_inodes_response::Result::Ok(res)) => Ok(res
                .inodes
                .into_iter()
                .map(|i| i.key.trim_end_matches('\0').to_string())
                .collect()),
            Some(nss_codec::list_inodes_response::Result::ErrNoSuchRootBlob(())) => {
                Err(FsError::NotFound)
            }
            Some(nss_codec::list_inodes_response::Result::ErrOther(e)) => Err(FsError::Internal(e)),
            None => Err(FsError::Internal("empty ListInodesResponse".into())),
        }
    }

    /// List MPU parts for a completed multipart upload
    pub async fn list_mpu_parts(
        &self,
        key: &str,
        trace_id: &TraceId,
    ) -> Result<Vec<(String, ObjectLayout)>, FsError> {
        let mpu_prefix = mpu_get_part_prefix(key.to_string(), 0);
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
    /// holding the key answers authoritatively.
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
        let checksum = xxhash_rust::xxh3::xxh3_64(&body);
        Ok((body, checksum))
    }

    /// Create a new data blob GUID via DataVgProxy.
    pub fn create_blob_guid(&self) -> DataBlobGuid {
        self.data_vg_proxy.create_data_blob_guid()
    }

    /// Write a single block to a data blob via DataVgProxy at a specific
    /// version. Override-style flush passes the bumped `blob_version`;
    /// initial-create passes `1`.
    pub async fn write_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        version: u64,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        self.data_vg_proxy
            .put_blob(blob_guid, block_number, body, version, trace_id)
            .await?;
        Ok(())
    }

    /// Reserve a single block (single-op, no batch) at `version`. Used by
    /// fallocate; EC volumes treat it as a no-op.
    pub async fn reserve_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        block_size: u32,
        version: u64,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        self.data_vg_proxy
            .reserve_blob(blob_guid, block_number, block_size, version, trace_id)
            .await?;
        Ok(())
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
        Ok(self
            .data_vg_proxy
            .list_blob_blocks(blob_guid, first_block, block_count, trace_id)
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

    /// Fetch the `InodeRecord` backing a hardlink-promoted inode from its
    /// `#hardlink/<inode_id>` NSS key. Uses the raw `get_inode` RPC and
    /// decodes the bytes as an `InodeRecord` (rather than `ObjectLayout`).
    ///
    /// Callers that CAS-update a record re-serialize the value returned here
    /// as `expected_old_value`. That is sound because rkyv output is
    /// deterministic for these map-free layout types; the override-flush's
    /// own s3_key CAS already re-serializes a fetched `ObjectLayout` the same
    /// way, so a separate exact-bytes fetch is unnecessary.
    pub async fn get_inode_record(
        &self,
        inode_id: uuid::Uuid,
        trace_id: &TraceId,
    ) -> Result<InodeRecord, FsError> {
        let key = InodeRecord::key_for(inode_id);
        let resp = nss_rpc_retry!(
            self.nss_client.borrow(),
            get_inode(
                &self.root_blob_name,
                &key,
                Some(self.config.rpc_request_timeout()),
                trace_id
            ),
            self,
            trace_id
        )
        .await?;
        let bytes: Bytes = match resp.result {
            Some(nss_codec::get_inode_response::Result::Ok(b)) => b,
            Some(nss_codec::get_inode_response::Result::ErrNotFound(()))
            | Some(nss_codec::get_inode_response::Result::ErrNoSuchRootBlob(())) => {
                return Err(FsError::NotFound);
            }
            Some(nss_codec::get_inode_response::Result::ErrOther(e)) => {
                return Err(FsError::Internal(e));
            }
            None => return Err(FsError::Internal("empty GetInodeResponse".into())),
        };
        rkyv::from_bytes::<InodeRecord, rkyv::rancor::Error>(&bytes)
            .map_err(|e| FsError::Internal(format!("InodeRecord deserialization: {e}")))
    }

    /// Persist the `InodeRecord` for a hardlink-promoted inode at its
    /// `#hardlink/<inode_id>` NSS key.
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

    /// Delete a single data block at a specific version. Used by the
    /// override flush to trim blocks past a shrunk EOF: the block lives on
    /// the file's stable blob_guid, and deleting it at the bumped
    /// `blob_version` lets BSS's version-guarded delete drop the older
    /// block. Best-effort; logs and swallows errors like
    /// `delete_blob_blocks`.
    pub async fn delete_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        trace_id: &TraceId,
    ) {
        if let Err(e) = self
            .data_vg_proxy
            .delete_blob(blob_guid, block_number, version, trace_id)
            .await
        {
            tracing::warn!(
                %blob_guid,
                block_number,
                version,
                error = %e,
                "Failed to delete trimmed blob block"
            );
        }
    }

    /// Delete blob blocks for a given ObjectLayout. Fire-and-forget: logs
    /// warnings on failure but does not return errors.
    /// Delete every committed generation of a blob: each in-size block at
    /// the exact identity its map resolves (or version 1 when unmapped).
    /// Map holes have no key; superseded/orphaned generations not referenced
    /// by the map are not enumerable here and leak until a scanner exists
    /// (invisible garbage; the blob_guid is never reused).
    pub async fn delete_blob_blocks(
        &self,
        layout: &ObjectLayout,
        map: Option<&data_types::block_map::BlockMap>,
        trace_id: &TraceId,
    ) {
        let Ok(blob_guid) = layout.blob_guid() else {
            return;
        };
        let num_blocks = layout.num_blocks().unwrap_or(0) as u32;
        for block_number in 0..num_blocks {
            use data_types::block_map::RangeState;
            let version = match map {
                None => Some(1),
                Some(m) => match m.lookup(block_number) {
                    Some(RangeState::Written(v)) | Some(RangeState::Reserved(v)) => Some(v),
                    Some(RangeState::Hole) => None,
                    None => Some(1),
                },
            };
            let Some(version) = version else {
                continue;
            };
            if let Err(e) = self
                .data_vg_proxy
                .delete_blob(blob_guid, block_number, version, trace_id)
                .await
            {
                tracing::warn!(
                    %blob_guid,
                    block_number,
                    version,
                    error = %e,
                    "Failed to delete blob block"
                );
            }
        }
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
