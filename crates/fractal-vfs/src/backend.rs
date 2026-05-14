#![allow(clippy::await_holding_refcell_ref)]

use bytes::Bytes;
use data_types::{Bucket, DataBlobGuid, DataVgInfo, RoutingKey, TraceId};
use file_ops::{
    GetInodeAnyOk, ListEntry, blob_blocks_to_delete, mpu_get_part_prefix, parse_delete_inode,
    parse_get_inode, parse_get_inode_any, parse_get_inode_with_bytes, parse_list_inodes,
    parse_mpu_parts, parse_put_inode,
};
use rpc_client_bss::BssBatchSubOp;
use rpc_client_common::RpcError;
use rpc_client_common::nss_rpc_retry;
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcClientRss;
use std::cell::RefCell;
use volume_group_proxy::DataVgProxy;

use crate::config::Config;
use crate::error::FsError;
use data_types::object_layout::ObjectLayout;
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

    /// Combined file/dir lookup. `key` is the bare path with no trailing
    /// slash; NSS server-side probes both `/foo` and `/foo/` and the
    /// response says which (if any) matched. One round trip instead of
    /// two for the common FUSE_LOOKUP-on-miss path.
    pub async fn get_inode_any(
        &self,
        key: &str,
        trace_id: &TraceId,
    ) -> Result<GetInodeAnyOk, FsError> {
        let resp = nss_rpc_retry!(
            self.nss_client.borrow(),
            get_inode_any(
                &self.root_blob_name,
                key,
                Some(self.config.rpc_request_timeout()),
                trace_id
            ),
            self,
            trace_id
        )
        .await?;
        Ok(parse_get_inode_any(resp)?)
    }

    /// Variant of [`Self::get_inode`] that also returns the raw stored
    /// bytes alongside the parsed `ObjectLayout`. The caller can stash
    /// the bytes for a later CAS guard so the override flush has a
    /// definitive snapshot to compare against without re-fetching.
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
    ///
    /// `blob_version` is the file-level `ObjectLayout.blob_version` and
    /// drives the read strategy:
    ///
    /// - `<= 1`: never overridden. Only stored version anywhere is 1,
    ///   so a stale replica returns `BlockNotFound` (the fallback loop
    ///   handles that) and there are no version-mismatch reads to
    ///   worry about. Use the existing single-replica fast path.
    /// - `> 1`: has been overridden at least once. A
    ///   partition-then-rejoin replica might silently hold pre-bump
    ///   bytes for blocks the writer last wrote at an earlier version,
    ///   so dispatch into the fan-out + max-version + inline-repair
    ///   path. See `DataVgProxy::get_blob_with_quorum_check`.
    pub async fn read_block(
        &self,
        blob_guid: DataBlobGuid,
        blob_version: u64,
        block_number: u32,
        content_len: usize,
        trace_id: &TraceId,
    ) -> Result<(Bytes, u64), FsError> {
        let mut body = Bytes::new();
        if blob_version <= 1 {
            self.data_vg_proxy
                .get_blob(blob_guid, block_number, content_len, &mut body, trace_id)
                .await?;
        } else {
            self.data_vg_proxy
                .get_blob_with_quorum_check(
                    blob_guid,
                    block_number,
                    content_len,
                    &mut body,
                    trace_id,
                )
                .await?;
        }
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

    /// Batched inode mutations -- ship N entries in one RPC. Server
    /// processes entries in order; per-entry results come back in
    /// `InodeBatchResponse.results`. Used by the writeback worker to
    /// drain PublishLayout as one RPC instead of N round-trips.
    pub async fn inode_batch(
        &self,
        entries: Vec<nss_codec::InodeBatchEntry>,
        trace_id: &TraceId,
    ) -> Result<Vec<nss_codec::InodeEntryResult>, FsError> {
        let resp = nss_rpc_retry!(
            self.nss_client.borrow(),
            inode_batch(
                &self.root_blob_name,
                entries.clone(),
                Some(self.config.rpc_request_timeout()),
                trace_id
            ),
            self,
            trace_id
        )
        .await?;
        Ok(resp.results)
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

    /// Fetch the `InodeRecord` for a hardlink-promoted inode. Uses the
    /// generic NSS key/value RPC against the `#hardlink/<inode_id>`
    /// key (`InodeRecord::key_for`), then deserialises as
    /// `InodeRecord` instead of `ObjectLayout`. See
    /// `misc/docs/017-fs/TBR/20-fs-symlinks-and-hardlinks-design.md`
    /// section 4.2 for the keyspace rationale.
    pub async fn get_inode_record(
        &self,
        inode_id: uuid::Uuid,
        trace_id: &TraceId,
    ) -> Result<data_types::object_layout::InodeRecord, FsError> {
        use nss_codec::{GetInodeResponse, get_inode_response};
        let key = data_types::object_layout::InodeRecord::key_for(inode_id);
        let resp: GetInodeResponse = nss_rpc_retry!(
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
        let bytes = match resp.result.unwrap() {
            get_inode_response::Result::Ok(b) => b,
            get_inode_response::Result::ErrNotFound(())
            | get_inode_response::Result::ErrNoSuchRootBlob(()) => return Err(FsError::NotFound),
            get_inode_response::Result::ErrOther(e) => return Err(FsError::Internal(e)),
        };
        rkyv::from_bytes::<data_types::object_layout::InodeRecord, rkyv::rancor::Error>(&bytes)
            .map_err(|e| FsError::Internal(format!("InodeRecord deserialization: {e}")))
    }

    /// Persist the `InodeRecord` for a hardlink-promoted inode at the
    /// `#hardlink/<inode_id>` key. Uses unconditional put; the
    /// per-record CAS slot model from doc 20 section 4.5 is deferred
    /// to the post-MVP step.
    pub async fn put_inode_record(
        &self,
        inode_id: uuid::Uuid,
        record: &data_types::object_layout::InodeRecord,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let key = data_types::object_layout::InodeRecord::key_for(inode_id);
        let bytes: Bytes = rkyv::to_bytes::<rkyv::rancor::Error>(record)
            .map_err(|e| FsError::Internal(format!("InodeRecord serialization: {e}")))?
            .to_vec()
            .into();
        self.put_inode(&key, bytes, trace_id).await?;
        Ok(())
    }

    /// Delete the `#hardlink/<inode_id>` keyspace entry. Called once
    /// `nlink` reaches 0 and no local fhs hold the inode open. See
    /// `vfs_unlink` for the surrounding GC.
    pub async fn delete_inode_record(
        &self,
        inode_id: uuid::Uuid,
        trace_id: &TraceId,
    ) -> Result<Option<Bytes>, FsError> {
        let key = data_types::object_layout::InodeRecord::key_for(inode_id);
        self.delete_inode(&key, trace_id).await
    }

    /// Rename an object (file) in NSS. When `force_overwrite` is set
    /// and the destination already exists, the rename atomically
    /// replaces it and the prior dst value bytes are returned;
    /// callers use those bytes to GC the orphaned blob. When dst
    /// didn't exist (or `force_overwrite=false` and dst was free),
    /// the returned `Bytes` is empty.
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
            Ok(old_value) => Ok(old_value),
            Err(RpcError::NotFound) => Err(FsError::NotFound),
            Err(RpcError::AlreadyExists) => Err(FsError::AlreadyExists),
            Err(e) => Err(e.into()),
        }
    }

    /// Rename a folder (directory prefix) in NSS. When `force_overwrite=true`
    /// and the destination prefix already exists, NSS atomically replaces
    /// it with `src_key`'s subtree (the orphaned dst subtree is leaked
    /// pending NSS-side blob reclamation).
    pub async fn rename_folder(
        &self,
        src_key: &str,
        dst_key: &str,
        force_overwrite: bool,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let result = nss_rpc_retry!(
            self.nss_client.borrow(),
            rename_folder(
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
            Ok(()) => Ok(()),
            Err(RpcError::NotFound) => Err(FsError::NotFound),
            Err(RpcError::AlreadyExists) => Err(FsError::AlreadyExists),
            Err(e) => Err(e.into()),
        }
    }

    /// Delete blob blocks for a given ObjectLayout. Fire-and-forget: logs
    /// warnings on failure but does not return errors.
    ///
    /// Uses `list_blob_blocks` to enumerate the actually-allocated
    /// blocks (Data + Reserved entries) and only issues a delete for
    /// those, so cleanup is O(allocated_blocks) instead of
    /// O(logical_blocks). Sparse-file unlinks no longer round-trip a
    /// delete RPC for every hole. Falls back to the dense
    /// `0..num_blocks` walk when the listing call fails -- the
    /// hole-tolerant per-block deletes keep that path correct.
    pub async fn delete_blob_blocks(&self, layout: &ObjectLayout, trace_id: &TraceId) {
        let blob_guid = match layout.blob_guid() {
            Ok(g) => g,
            Err(_) => return,
        };
        let num_blocks = match layout.num_blocks() {
            Ok(n) => n,
            Err(_) => return,
        };
        if num_blocks == 0 {
            return;
        }

        let blocks: Vec<u32> = match self
            .data_vg_proxy
            .list_blob_blocks(blob_guid, 0, num_blocks as u32, trace_id)
            .await
        {
            Ok(entries) => entries.into_iter().map(|e| e.block_number).collect(),
            Err(e) => {
                tracing::warn!(
                    %blob_guid,
                    error = %e,
                    "list_blob_blocks failed during cleanup; falling back to dense walk"
                );
                blob_blocks_to_delete(layout)
                    .into_iter()
                    .map(|(_, b)| b)
                    .collect()
            }
        };

        for block_number in blocks {
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

    /// Send N block-mutation sub-ops (writes / deletes / reserves)
    /// for a single blob as one batched RPC per replica (or per data
    /// BSS node on EC volumes), with quorum reduction per entry.
    ///
    /// `commit` publishes the blob's authoritative
    /// `(total_size, block_count)` at `blob_version`; BSS records it
    /// once all sub-ops resolve. Every batch publishes a commit;
    /// callers that don't change file size re-stamp the current
    /// `(total_size, block_count)` at the new `blob_version`.
    ///
    /// Returns one `Result<(), FsError>` per input sub-op, in the same
    /// order. Per-entry failures are surfaced inside the vector; the
    /// outer `Result` only fails when the whole batch couldn't be
    /// dispatched (e.g. transport, quorum unreachable).
    pub async fn flush_blocks_batched(
        &self,
        sub_ops: Vec<BssBatchSubOp>,
        commit: rpc_client_bss::BlobCommitInfo,
        trace_id: &TraceId,
    ) -> Result<Vec<Result<(), FsError>>, FsError> {
        let entry_results = self
            .data_vg_proxy
            .put_blocks_batched(sub_ops, commit, trace_id)
            .await?;
        Ok(entry_results
            .into_iter()
            .map(|r| r.map_err(FsError::from))
            .collect())
    }

    /// Enumerate the BSS-side block entries for `blob_guid` over the
    /// requested range. Each result is `(block_number, entry_type,
    /// version)` where `entry_type` is 0=Data, 1=Reserved. Absent
    /// indices are holes.
    pub async fn list_blob_blocks(
        &self,
        blob_guid: DataBlobGuid,
        first_block: u32,
        block_count: u32,
        trace_id: &TraceId,
    ) -> Result<Vec<bss_codec::list_blob_blocks_response::BlobBlockEntry>, FsError> {
        let entries = self
            .data_vg_proxy
            .list_blob_blocks(blob_guid, first_block, block_count, trace_id)
            .await?;
        Ok(entries)
    }

    /// Read the BSS-internal commit info for `blob_guid`. Returns
    /// the currently-recorded `(total_size, block_count, blob_version)`
    /// as the BSS replica with the highest version sees it.
    /// `expected_version` is the caller's last-known version (used
    /// by version-aware fan-out); pass 0 when no prior is known.
    /// Returns `Ok(None)` when no replica has a commit entry for the
    /// blob (new blob, or partition that missed every publish).
    pub async fn get_blob_info(
        &self,
        blob_guid: DataBlobGuid,
        expected_version: u64,
        trace_id: &TraceId,
    ) -> Result<Option<rpc_client_bss::BlobInfo>, FsError> {
        Ok(self
            .data_vg_proxy
            .get_blob_info(blob_guid, expected_version, trace_id)
            .await?)
    }
}
