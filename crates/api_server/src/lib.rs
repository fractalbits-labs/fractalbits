pub mod api_key_routes;
pub mod blob_client;
mod blob_storage;
mod config;
pub mod handler;
pub mod http_stats;
pub mod unified_stats;

pub use blob_client::BlobClient;
use blob_client::BlobDeletionRequest;
pub use config::{BlobStorageBackend, BlobStorageConfig, Config, S3HybridSingleAzConfig};
use data_types::{ApiKey, Bucket, RoutingKey, TraceId, Versioned};
use handler::common::s3_error::S3Error;
use metrics_wrapper::counter;
use moka::future::Cache;
use rpc_client_common::{RpcError, rss_rpc_retry};
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcClientRss;

use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{
    Mutex, OnceCell, RwLock, RwLockReadGuard,
    mpsc::{self, Receiver, Sender},
};
use tracing::debug;
pub type BlobId = uuid::Uuid;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub struct AppState {
    pub config: Arc<Config>,
    pub cache: Arc<Cache<String, Versioned<String>>>,
    pub worker_id: u16,

    // Per-routing-key NSS clients, lazily populated as buckets are resolved.
    // A single api_server instance can cache clients for multiple distinct
    // routing keys (each routing key maps to one NSS endpoint).
    nss_clients: Arc<RwLock<HashMap<RoutingKey, NssEntry>>>,
    rpc_client_rss: RpcClientRss,

    blob_client: OnceCell<Arc<BlobClient>>,
    blob_deletion_tx: Sender<BlobDeletionRequest>,
    blob_deletion_rx: Mutex<Option<Receiver<BlobDeletionRequest>>>,
}

pub struct NssEntry {
    address: String,
    client: RpcClientNss,
}

impl AppState {
    const PER_CORE_CACHE_CAPACITY: u64 = 10_000;

    pub fn new_per_core_sync(
        config: Arc<Config>,
        // Shared across all per-core AppStates (including the mgmt-only one)
        // so the S3 path doesn't redundantly refresh per-core on failover.
        nss_clients: Arc<RwLock<HashMap<RoutingKey, NssEntry>>>,
        worker_id: u16,
    ) -> Self {
        debug!("Initializing per-core AppState with lazy RPC client connections");

        let (tx, rx) = mpsc::channel(1024 * 1024);

        let cache = Arc::new(
            Cache::builder()
                .time_to_live(Duration::from_secs(300))
                .max_capacity(Self::PER_CORE_CACHE_CAPACITY)
                .build(),
        );

        debug!("Per-core AppState initialized with lazy BlobClient initialization");

        let rpc_client_rss = RpcClientRss::new_from_addresses(
            config.rss_addrs.clone(),
            config.rpc_connection_timeout(),
        );

        Self {
            config,
            nss_clients,
            rpc_client_rss,
            blob_client: OnceCell::new(),
            blob_deletion_tx: tx,
            blob_deletion_rx: Mutex::new(Some(rx)),
            cache,
            worker_id,
        }
    }

    /// Shared-map constructor for the shared NSS-client table used by
    /// `new_per_core_sync`. Call once at process startup and pass the same
    /// `Arc` into every per-core AppState (and the mgmt AppState).
    pub fn new_shared_nss_clients() -> Arc<RwLock<HashMap<RoutingKey, NssEntry>>> {
        Arc::new(RwLock::new(HashMap::new()))
    }

    /// Returns a read guard to the NSS client for the given routing_key,
    /// or ServiceUnavailable if no client is cached for that key.
    pub async fn get_nss_rpc_client(
        &self,
        routing_key: &RoutingKey,
    ) -> Result<RwLockReadGuard<'_, RpcClientNss>, S3Error> {
        let guard = self.nss_clients.read().await;
        RwLockReadGuard::try_map(guard, |map| map.get(routing_key).map(|e| &e.client))
            .map_err(|_| S3Error::ServiceUnavailable)
    }

    pub async fn update_nss_address(&self, routing_key: RoutingKey, new_address: String) {
        tracing::info!(
            "Updating NSS address for routing_key {} to: {}",
            routing_key,
            new_address
        );
        let new_client = RpcClientNss::new_from_address(
            new_address.clone(),
            self.config.rpc_connection_timeout(),
        );
        self.nss_clients.write().await.insert(
            routing_key,
            NssEntry {
                address: new_address,
                client: new_client,
            },
        );
        tracing::info!("NSS client updated successfully");
    }

    /// Get the cached NSS address for a routing_key (for comparison during refresh).
    pub async fn get_nss_address(&self, routing_key: &RoutingKey) -> Option<String> {
        self.nss_clients
            .read()
            .await
            .get(routing_key)
            .map(|e| e.address.clone())
    }

    /// Try to refresh NSS address from RSS for the given routing_key.
    /// Returns true if address was refreshed and caller should retry the operation.
    pub async fn try_refresh_nss_address(
        &self,
        routing_key: &RoutingKey,
        trace_id: &TraceId,
    ) -> bool {
        let current_addr = self.get_nss_address(routing_key).await;

        let rss_client = self.get_rss_rpc_client();
        match rss_rpc_retry!(
            rss_client,
            get_active_nss_address(
                routing_key.as_bytes(),
                Some(self.config.rss_rpc_timeout()),
                trace_id
            )
        )
        .await
        {
            Ok(new_addr) => {
                if current_addr.as_deref() != Some(&new_addr) {
                    tracing::info!(
                        "NSS address changed during refresh for routing_key {}: {:?} -> {}",
                        routing_key,
                        current_addr,
                        new_addr
                    );
                    self.update_nss_address(*routing_key, new_addr).await;
                    true
                } else {
                    tracing::debug!(
                        "NSS address unchanged during refresh for routing_key {}: {:?}",
                        routing_key,
                        current_addr
                    );
                    false
                }
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to fetch NSS address from RSS during refresh for routing_key {}: {}",
                    routing_key,
                    e
                );
                false
            }
        }
    }

    /// Ensures NSS client is initialized for the given routing_key by fetching
    /// address from RSS if needed.
    pub async fn ensure_nss_client_initialized(
        &self,
        routing_key: &RoutingKey,
        trace_id: &TraceId,
    ) -> bool {
        // Fast path: check if already cached for this routing_key
        if self.get_nss_rpc_client(routing_key).await.is_ok() {
            return true;
        }

        tracing::info!(
            "NSS client not initialized for routing_key {}, fetching address from RSS",
            routing_key
        );
        let rss_client = self.get_rss_rpc_client();
        match rss_rpc_retry!(
            rss_client,
            get_active_nss_address(
                routing_key.as_bytes(),
                Some(self.config.rss_rpc_timeout()),
                trace_id
            )
        )
        .await
        {
            Ok(addr) => {
                if !addr.is_empty() {
                    self.update_nss_address(*routing_key, addr).await;
                    true
                } else {
                    tracing::warn!(
                        "RSS returned empty NSS address for routing_key {}",
                        routing_key
                    );
                    false
                }
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to fetch NSS address from RSS for routing_key {}: {}",
                    routing_key,
                    e
                );
                false
            }
        }
    }

    pub fn get_rss_rpc_client(&self) -> &RpcClientRss {
        &self.rpc_client_rss
    }

    pub async fn get_blob_client(
        &self,
        _routing_key: &RoutingKey,
    ) -> Result<Arc<BlobClient>, String> {
        self.blob_client
            .get_or_try_init(|| async {
                debug!("Creating per-worker BlobClient on-demand");

                let rx = self
                    .blob_deletion_rx
                    .lock()
                    .await
                    .take()
                    .ok_or_else(|| "BlobClient already initialized".to_string())?;

                debug!(
                    "Fetching DataVgInfo from RSS at {:?}",
                    self.config.rss_addrs
                );
                let rss_client = self.get_rss_rpc_client();

                let data_vg_info = rss_client
                    .get_data_vg_info(Some(self.config.rss_rpc_timeout()), &TraceId::new())
                    .await
                    .map_err(|e| format!("Failed to fetch DataVgInfo from RSS: {}", e))?;

                debug!(
                    "Successfully fetched DataVgInfo with {} volumes",
                    data_vg_info.volumes.len()
                );

                let blob_client = BlobClient::new_with_data_vg_info(
                    &self.config.blob_storage,
                    rx,
                    self.config.rss_rpc_timeout(),
                    self.config.rpc_connection_timeout(),
                    data_vg_info,
                )
                .await
                .map_err(|e| e.to_string())?;

                Ok(Arc::new(blob_client))
            })
            .await
            .cloned()
    }

    pub fn get_blob_deletion(&self) -> Sender<BlobDeletionRequest> {
        self.blob_deletion_tx.clone()
    }
}

// API Key operations
impl AppState {
    pub async fn get_api_key(
        &self,
        key_id: String,
        trace_id: &TraceId,
    ) -> Result<Versioned<ApiKey>, RpcError> {
        let full_key = format!("api_key:{key_id}");
        if let Some(json) = self.cache.get(&full_key).await {
            counter!("api_key_cache_hit").increment(1);
            tracing::debug!("get cached data with full_key: {full_key}");
            return Ok((
                json.version,
                serde_json::from_slice(json.data.as_bytes()).unwrap(),
            )
                .into());
        } else {
            counter!("api_key_cache_miss").increment(1);
        }

        let rss_client = self.get_rss_rpc_client();
        let (version, data) = rss_rpc_retry!(
            rss_client,
            get(&full_key, Some(self.config.rss_rpc_timeout()), trace_id)
        )
        .await?;
        let json = Versioned::new(version, data);
        self.cache.insert(full_key, json.clone()).await;
        Ok((
            json.version,
            serde_json::from_slice(json.data.as_bytes()).unwrap(),
        )
            .into())
    }

    pub async fn get_test_api_key(
        &self,
        trace_id: &TraceId,
    ) -> Result<Versioned<ApiKey>, RpcError> {
        self.get_api_key("test_api_key".into(), trace_id).await
    }

    /// Force-refetch an api_key from RSS, bypassing the local cache. The
    /// freshly-fetched `Versioned<ApiKey>` is also written back to the cache
    /// (so subsequent reads see it) and returned to the caller.
    ///
    /// Used by the authorization gate to handle the case where another
    /// api_server has just added or removed a bucket from this api_key's
    /// `authorized_buckets`: our cached copy says deny, but the source of
    /// truth says allow (or vice versa). One refresh on the cold path of a
    /// denial closes the staleness window without re-introducing recall.
    pub async fn refresh_api_key(
        &self,
        key_id: String,
        trace_id: &TraceId,
    ) -> Result<Versioned<ApiKey>, RpcError> {
        let full_key = format!("api_key:{key_id}");
        let rss_client = self.get_rss_rpc_client();
        let (version, data) = rss_rpc_retry!(
            rss_client,
            get(&full_key, Some(self.config.rss_rpc_timeout()), trace_id)
        )
        .await?;
        let json = Versioned::new(version, data);
        self.cache.insert(full_key, json.clone()).await;
        counter!("api_key_refresh_from_rss").increment(1);
        Ok((
            json.version,
            serde_json::from_slice(json.data.as_bytes()).unwrap(),
        )
            .into())
    }

    pub async fn put_api_key(
        &self,
        api_key: &Versioned<ApiKey>,
        trace_id: &TraceId,
    ) -> Result<(), RpcError> {
        let full_key = format!("api_key:{}", api_key.data.key_id);
        let data: String = serde_json::to_string(&api_key.data).unwrap();
        let versioned_data: Versioned<String> = (api_key.version, data).into();

        let rss_client = self.get_rss_rpc_client();
        rss_rpc_retry!(
            rss_client,
            put(
                versioned_data.version,
                &full_key,
                &versioned_data.data,
                Some(self.config.rss_rpc_timeout()),
                trace_id
            )
        )
        .await?;

        tracing::debug!("caching data with full_key: {full_key}");
        self.cache.insert(full_key, versioned_data).await;
        Ok(())
    }

    pub async fn delete_api_key(
        &self,
        api_key: &ApiKey,
        trace_id: &TraceId,
    ) -> Result<(), RpcError> {
        let full_key = format!("api_key:{}", api_key.key_id);
        let rss_client = self.get_rss_rpc_client();
        rss_rpc_retry!(
            rss_client,
            delete(&full_key, Some(self.config.rss_rpc_timeout()), trace_id)
        )
        .await?;
        // Drop our local cache entry. Other per-core caches and other
        // api_server instances fall back to TTL — see the
        // refresh-on-auth-deny path that picks up new versions on demand.
        self.cache.invalidate(&full_key).await;
        Ok(())
    }

    pub async fn list_api_keys(&self, trace_id: &TraceId) -> Result<Vec<ApiKey>, RpcError> {
        let prefix = "api_key:".to_string();
        let rss_client = self.get_rss_rpc_client();
        let kvs = rss_rpc_retry!(
            rss_client,
            list(&prefix, Some(self.config.rss_rpc_timeout()), trace_id)
        )
        .await?;
        Ok(kvs
            .iter()
            .map(|x| serde_json::from_slice(x.as_bytes()).unwrap())
            .collect())
    }
}

// Bucket operations
impl AppState {
    pub async fn get_bucket(
        &self,
        bucket_name: &str,
        trace_id: &TraceId,
    ) -> Result<Versioned<Bucket>, RpcError> {
        let full_key = format!("bucket:{bucket_name}");
        if let Some(json) = self.cache.get(&full_key).await {
            counter!("bucket_cache_hit").increment(1);
            tracing::debug!("get cached data with full_key: {full_key}");
            return Ok((
                json.version,
                serde_json::from_slice(json.data.as_bytes()).unwrap(),
            )
                .into());
        } else {
            counter!("bucket_cache_miss").increment(1);
        }

        let rss_client = self.get_rss_rpc_client();
        let (version, data) = rss_rpc_retry!(
            rss_client,
            get(&full_key, Some(self.config.rss_rpc_timeout()), trace_id)
        )
        .await?;
        let json = Versioned::new(version, data);
        self.cache.insert(full_key, json.clone()).await;
        Ok((
            json.version,
            serde_json::from_slice(json.data.as_bytes()).unwrap(),
        )
            .into())
    }

    /// Always read the bucket from RSS, bypassing the local cache. Updates
    /// the cache with the fresh value on success, and invalidates the cache
    /// entry on `RpcError::NotFound` (the bucket has been deleted upstream).
    /// Used by metadata-only endpoints (HEAD bucket) and as the
    /// authoritative second-look on suspected staleness.
    pub async fn fetch_bucket_no_cache(
        &self,
        bucket_name: &str,
        trace_id: &TraceId,
    ) -> Result<Versioned<Bucket>, RpcError> {
        let full_key = format!("bucket:{bucket_name}");
        let rss_client = self.get_rss_rpc_client();
        let result = rss_rpc_retry!(
            rss_client,
            get(&full_key, Some(self.config.rss_rpc_timeout()), trace_id)
        )
        .await;
        match result {
            Ok((version, data)) => {
                let json = Versioned::new(version, data);
                self.cache.insert(full_key, json.clone()).await;
                counter!("bucket_refresh_from_rss").increment(1);
                Ok((
                    json.version,
                    serde_json::from_slice(json.data.as_bytes()).unwrap(),
                )
                    .into())
            }
            Err(RpcError::NotFound) => {
                self.cache.invalidate(&full_key).await;
                counter!("bucket_refresh_from_rss_gone").increment(1);
                Err(RpcError::NotFound)
            }
            Err(e) => Err(e),
        }
    }

    /// Drop the cache entry for a bucket. Used after an NSS op surfaces
    /// `NoSuchRootBlob`, which means the bucket has been deleted upstream
    /// and our cached entry (if any) is stale. The next `get_bucket` call
    /// will re-fetch from RSS and observe the deletion authoritatively.
    pub async fn invalidate_bucket_cache(&self, bucket_name: &str) {
        let full_key = format!("bucket:{bucket_name}");
        self.cache.invalidate(&full_key).await;
    }

    pub async fn create_bucket(
        &self,
        bucket_name: &str,
        api_key_id: &str,
        trace_id: TraceId,
    ) -> Result<(), RpcError> {
        let rss_client = self.get_rss_rpc_client();
        rss_rpc_retry!(
            rss_client,
            create_bucket(
                bucket_name,
                api_key_id,
                Some(self.config.rss_rpc_timeout()),
                &trace_id
            )
        )
        .await?;

        // Drop our local api_key cache entry; other workers / api_servers
        // pick up the new authorized_buckets via refresh-on-auth-deny or TTL.
        self.cache
            .invalidate(&format!("api_key:{api_key_id}"))
            .await;
        Ok(())
    }

    pub async fn delete_bucket(
        &self,
        bucket_name: &str,
        api_key_id: &str,
        trace_id: TraceId,
    ) -> Result<(), RpcError> {
        let rss_client = self.get_rss_rpc_client();
        rss_rpc_retry!(
            rss_client,
            delete_bucket(
                bucket_name,
                api_key_id,
                Some(self.config.rss_rpc_timeout()),
                &trace_id
            )
        )
        .await?;

        // Drop our local bucket and api_key cache entries; other workers /
        // api_servers fall back to TTL or to NoSuchRootBlob from NSS on the
        // next op against the deleted bucket.
        self.cache
            .invalidate(&format!("bucket:{bucket_name}"))
            .await;
        self.cache
            .invalidate(&format!("api_key:{api_key_id}"))
            .await;
        Ok(())
    }

    pub async fn list_buckets(&self, trace_id: TraceId) -> Result<Vec<Bucket>, RpcError> {
        let prefix = "bucket:".to_string();
        let rss_client = self.get_rss_rpc_client();
        let kvs = rss_rpc_retry!(
            rss_client,
            list(&prefix, Some(self.config.rss_rpc_timeout()), &trace_id)
        )
        .await?;
        Ok(kvs
            .iter()
            .map(|x| serde_json::from_slice(x.as_bytes()).unwrap())
            .collect())
    }
}
