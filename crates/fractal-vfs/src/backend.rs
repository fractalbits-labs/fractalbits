#![allow(clippy::await_holding_refcell_ref)]

use bytes::Bytes;
use data_types::{Bucket, DataBlobGuid, DataVgInfo, RoutingKey, TraceId};
use file_ops::{
    ListEntry, blob_blocks_to_delete, create_dir_marker_layout, mpu_get_part_prefix,
    parse_delete_inode, parse_get_inode, parse_get_inode_with_bytes, parse_list_inodes,
    parse_mpu_parts, parse_put_inode, parse_put_inode_cas,
};
use rpc_client_common::RpcError;
use rpc_client_common::nss_rpc_retry;
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcClientRss;
use std::cell::RefCell;
use volume_group_proxy::DataVgProxy;

use crate::config::Config;
use crate::error::FsError;
use data_types::object_layout::ObjectLayout;
use data_types::parent_inode::{PARENT_INODE_BLOCK_NUMBER, ParentInodeMeta};
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

    /// Variant of [`Self::get_inode`] that also returns the raw stored
    /// bytes alongside the parsed `ObjectLayout`. The caller can stash
    /// the bytes for a later `put_inode_cas` guard so the override flush
    /// has a definitive snapshot to compare against without re-fetching.
    pub async fn get_inode_with_bytes(
        &self,
        key: &str,
        trace_id: &TraceId,
    ) -> Result<(ObjectLayout, Bytes), FsError> {
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

        Ok(parse_get_inode_with_bytes(resp)?)
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

    /// Read a single block from a data blob via DataVgProxy.
    /// Returns `(data, xxh3_64_checksum)`.
    pub async fn read_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        content_len: usize,
        trace_id: &TraceId,
    ) -> Result<(Bytes, u64), FsError> {
        let mut body = Bytes::new();
        self.data_vg_proxy
            .get_blob(blob_guid, block_number, content_len, &mut body, trace_id)
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

    /// Compare-and-swap variant of [`Self::put_inode`].
    ///
    /// The put lands only when the bytes currently stored at `key` are
    /// byte-equal to `expected_old_value`. Pass an empty `Bytes` to
    /// require the slot to be empty (initial-create semantics under
    /// CAS, intended for safe replays of a new file). On a guard
    /// mismatch this returns `FsError::CasConflict` so the caller can
    /// surface a stale-handle error to userspace; the conflict bytes
    /// from the server are dropped at this layer because the override
    /// flush path cannot recover by silently adopting the winner's
    /// state -- the in-memory write buffer is provably stale.
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
    pub async fn rename_file(
        &self,
        src_key: &str,
        dst_key: &str,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let result = nss_rpc_retry!(
            self.nss_client.borrow(),
            rename_object(
                &self.root_blob_name,
                src_key,
                dst_key,
                false,
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

    /// Delete blob blocks for a given ObjectLayout. Fire-and-forget: logs
    /// warnings on failure but does not return errors.
    pub async fn delete_blob_blocks(&self, layout: &ObjectLayout, trace_id: &TraceId) {
        for (blob_guid, block_number) in blob_blocks_to_delete(layout) {
            if let Err(e) = self
                .data_vg_proxy
                .delete_blob(blob_guid, block_number, layout.blob_version, trace_id)
                .await
            {
                tracing::warn!(
                    %blob_guid,
                    block_number,
                    error = %e,
                    "Failed to delete blob block"
                );
            }
        }
    }

    /// Delete a specific (blob_guid, block_number) at a given version.
    /// Used by override flush to drop blocks past the new EOF.
    pub async fn delete_block(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        self.data_vg_proxy
            .delete_blob(blob_guid, block_number, version, trace_id)
            .await?;
        Ok(())
    }

    /// Write the parent inode meta record for `blob_guid` at the
    /// given version. Goes through the same `put_blob` path as a
    /// regular block write, but addresses the no-suffix key form via
    /// the `PARENT_INODE_BLOCK_NUMBER` sentinel that the BSS server
    /// recognises.
    pub async fn write_parent_inode(
        &self,
        blob_guid: DataBlobGuid,
        meta: ParentInodeMeta,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let body = Bytes::copy_from_slice(&meta.to_bytes());
        self.data_vg_proxy
            .put_blob(
                blob_guid,
                PARENT_INODE_BLOCK_NUMBER,
                body,
                meta.version,
                trace_id,
            )
            .await?;
        Ok(())
    }

    /// Read the parent inode meta record for `blob_guid`. Returns
    /// `Ok(None)` when the parent inode does not exist (older blobs
    /// written before parent-inode support, or a transient
    /// missing-replica situation that the regular hole-tolerant read
    /// path treats the same way).
    pub async fn read_parent_inode(
        &self,
        blob_guid: DataBlobGuid,
        trace_id: &TraceId,
    ) -> Result<Option<ParentInodeMeta>, FsError> {
        match self
            .read_block(
                blob_guid,
                PARENT_INODE_BLOCK_NUMBER,
                ParentInodeMeta::WIRE_LEN,
                trace_id,
            )
            .await
        {
            Ok((body, _)) => {
                let meta = ParentInodeMeta::from_bytes(&body)
                    .map_err(|e| FsError::Internal(format!("invalid ParentInodeMeta: {e}")))?;
                Ok(Some(meta))
            }
            Err(FsError::DataVg(volume_group_proxy::DataVgError::BlockNotFound)) => Ok(None),
            Err(FsError::Rpc(RpcError::NotFound)) => Ok(None),
            Err(e) => Err(e),
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
