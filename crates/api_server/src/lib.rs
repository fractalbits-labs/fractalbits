pub mod api_key_routes;
pub mod blob_client;
mod blob_storage;
pub mod cache_mgmt;
mod cache_registry;
mod config;
pub mod handler;
pub mod http_stats;
pub mod unified_stats;

pub use blob_client::BlobClient;
use blob_client::BlobDeletionRequest;
pub use config::{BlobStorageBackend, BlobStorageConfig, Config, S3HybridSingleAzConfig};
pub use data_blob_tracking::{DataBlobTracker, DataBlobTrackingError};
use data_types::{ApiKey, Bucket, TraceId, Versioned};
use handler::common::s3_error::S3Error;
use metrics_wrapper::counter;
use moka::future::Cache;
use rpc_client_common::{RpcError, rss_rpc_retry};
use rpc_client_nss::RpcClientNss;
use rpc_client_rss::RpcClientRss;

pub use cache_registry::CacheCoordinator;
use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
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
    pub cache_coordinator: Arc<CacheCoordinator<Versioned<String>>>,
    pub az_status_coordinator: Arc<CacheCoordinator<String>>,
    pub az_status_enabled: AtomicBool,
    pub worker_id: u16,

    // NSS client is lazily initialized - set when we receive address from RSS
    // We store both the client and the address for refresh detection
    rpc_client_nss: Arc<RwLock<Option<RpcClientNss>>>,
    nss_address: Arc<RwLock<Option<String>>>,
    rpc_client_rss: RpcClientRss,

    blob_client: OnceCell<Arc<BlobClient>>,
    blob_deletion_tx: Sender<BlobDeletionRequest>,
    blob_deletion_rx: Mutex<Option<Receiver<BlobDeletionRequest>>>,
    pub data_blob_tracker: OnceCell<Arc<DataBlobTracker>>,
}

impl AppState {
    const PER_CORE_CACHE_CAPACITY: u64 = 10_000;

    pub fn new_per_core_sync(
        config: Arc<Config>,
        cache_coordinator: Arc<CacheCoordinator<Versioned<String>>>,
        az_status_coordinator: Arc<CacheCoordinator<String>>,
        worker_id: u16,
    ) -> Self {
        debug!("Initializing per-core AppState with lazy RPC client connections");

        let (tx, rx) = mpsc::channel(1024 * 1024);

        let cache = Arc::new(
            Cache::builder()
                .time_to_idle(Duration::from_secs(300))
                .max_capacity(Self::PER_CORE_CACHE_CAPACITY)
                .build(),
        );
        cache_coordinator.register_cache(cache.clone());

        debug!("Per-core AppState initialized with lazy BlobClient initialization");

        // NSS client starts uninitialized - will be set when we receive address from RSS
        let rpc_client_nss = Arc::new(RwLock::new(None));
        let nss_address = Arc::new(RwLock::new(None));
        let rpc_client_rss = RpcClientRss::new_from_addresses(
            config.rss_addrs.clone(),
            config.rpc_connection_timeout(),
        );

        Self {
            config,
            rpc_client_nss,
            nss_address,
            rpc_client_rss,
            blob_client: OnceCell::new(),
            blob_deletion_tx: tx,
            blob_deletion_rx: Mutex::new(Some(rx)),
            cache,
            cache_coordinator,
            az_status_coordinator,
            az_status_enabled: AtomicBool::new(false),
            worker_id,
            data_blob_tracker: OnceCell::new(),
        }
    }

    /// Returns a read guard to the NSS client, or ServiceUnavailable error if not initialized
    pub async fn get_nss_rpc_client(&self) -> Result<RwLockReadGuard<'_, RpcClientNss>, S3Error> {
        let guard = self.rpc_client_nss.read().await;
        RwLockReadGuard::try_map(guard, |opt| opt.as_ref()).map_err(|_| S3Error::ServiceUnavailable)
    }

    pub async fn update_nss_address(&self, new_address: String) {
        tracing::info!("Updating NSS address to: {}", new_address);
        let new_client = RpcClientNss::new_from_address(
            new_address.clone(),
            self.config.rpc_connection_timeout(),
        );
        *self.nss_address.write().await = Some(new_address);
        *self.rpc_client_nss.write().await = Some(new_client);
        tracing::info!("NSS client updated successfully");
    }

    /// Get the current NSS address (for comparison during refresh)
    pub async fn get_nss_address(&self) -> Option<String> {
        self.nss_address.read().await.clone()
    }

    /// Try to refresh NSS address from RSS when connection fails.
    /// Returns true if address was refreshed and caller should retry the operation.
    pub async fn try_refresh_nss_address(&self, trace_id: &TraceId) -> bool {
        let current_addr = self.get_nss_address().await;

        // Fetch latest NSS address from RSS
        let rss_client = self.get_rss_rpc_client();
        match rss_rpc_retry!(
            rss_client,
            get_active_nss_address(Some(self.config.rss_rpc_timeout()), trace_id)
        )
        .await
        {
            Ok(new_addr) => {
                if current_addr.as_deref() != Some(&new_addr) {
                    tracing::info!(
                        "NSS address changed during refresh: {:?} -> {}",
                        current_addr,
                        new_addr
                    );
                    self.update_nss_address(new_addr).await;
                    true
                } else {
                    tracing::debug!("NSS address unchanged during refresh: {:?}", current_addr);
                    false
                }
            }
            Err(e) => {
                tracing::warn!("Failed to fetch NSS address from RSS during refresh: {}", e);
                false
            }
        }
    }

    /// Ensures NSS client is initialized by fetching address from RSS if needed
    pub async fn ensure_nss_client_initialized(&self, trace_id: &TraceId) -> bool {
        // Fast path: check if already initialized
        if self.get_nss_rpc_client().await.is_ok() {
            return true;
        }

        // Fetch NSS address from RSS
        tracing::info!("NSS client not initialized, fetching address from RSS");
        let rss_client = self.get_rss_rpc_client();
        match rss_rpc_retry!(
            rss_client,
            get_active_nss_address(Some(self.config.rss_rpc_timeout()), trace_id)
        )
        .await
        {
            Ok(addr) => {
                if !addr.is_empty() {
                    self.update_nss_address(addr).await;
                    true
                } else {
                    tracing::warn!("RSS returned empty NSS address");
                    false
                }
            }
            Err(e) => {
                tracing::warn!("Failed to fetch NSS address from RSS: {}", e);
                false
            }
        }
    }

    pub fn get_rss_rpc_client(&self) -> &RpcClientRss {
        &self.rpc_client_rss
    }

    pub async fn get_blob_client(&self) -> Result<Arc<BlobClient>, String> {
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

                let (blob_client, az_status_cache) = BlobClient::new_with_data_vg_info(
                    &self.config.blob_storage,
                    rx,
                    self.config.rss_rpc_timeout(),
                    self.config.rpc_connection_timeout(),
                    None,
                    data_vg_info,
                )
                .await
                .map_err(|e| e.to_string())?;

                // Register az_status_cache if present (only for Multi-AZ)
                if let Some(cache) = az_status_cache {
                    self.az_status_coordinator.register_cache(cache);
                }

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
        self.cache_coordinator.invalidate_entry(&full_key).await;
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

    pub async fn create_bucket(
        &self,
        bucket_name: &str,
        api_key_id: &str,
        is_multi_az: bool,
        trace_id: TraceId,
    ) -> Result<(), RpcError> {
        let rss_client = self.get_rss_rpc_client();
        rss_rpc_retry!(
            rss_client,
            create_bucket(
                bucket_name,
                api_key_id,
                is_multi_az,
                Some(self.config.rss_rpc_timeout()),
                &trace_id
            )
        )
        .await?;

        // Invalidate API key cache across all workers since it now has new bucket permissions
        self.cache_coordinator
            .invalidate_entry(&format!("api_key:{api_key_id}"))
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

        // Invalidate both bucket and API key cache across all workers
        self.cache_coordinator
            .invalidate_entry(&format!("bucket:{bucket_name}"))
            .await;
        self.cache_coordinator
            .invalidate_entry(&format!("api_key:{api_key_id}"))
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
