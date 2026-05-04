use serde::Deserialize;
use std::time::Duration;

/// Writeback-cache durability mode.
///
/// `Strict` is the legacy synchronous path: every FUSE op blocks until
/// the corresponding NSS / BSS RPC completes. `Default` enables the
/// writeback fast path for the enabled operation slice and falls back
/// to strict for the rest. `Loose` is a future full-target option not
/// enabled today.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WritebackMode {
    #[default]
    Strict,
    Default,
}

#[allow(dead_code)]
impl WritebackMode {
    pub fn from_str_lossy(s: &str) -> Self {
        match s {
            "default" => WritebackMode::Default,
            _ => WritebackMode::Strict,
        }
    }

    pub fn is_strict(&self) -> bool {
        matches!(self, WritebackMode::Strict)
    }
}

/// Writeback knobs. All fields have safe defaults that keep the queue
/// in strict mode until the workload validation gate passes.
#[allow(dead_code)]
#[derive(Deserialize, Debug, Clone)]
pub struct WritebackConfig {
    /// Mode string; `strict` (default) or `default`. Loose mode is
    /// not exposed today.
    #[serde(default = "default_writeback_mode")]
    pub mode: String,
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: u32,
    #[serde(default = "default_max_batch_wait_ms")]
    pub max_batch_wait_ms: u32,
    #[serde(default = "default_max_retry_duration_ms")]
    pub max_retry_duration_ms: u32,
    #[serde(default = "default_max_inflight_age_ms")]
    pub max_inflight_age_ms: u32,
    #[serde(default = "default_worker_pool_size")]
    pub worker_pool_size: u32,
    #[serde(default = "default_queue_capacity_mb")]
    pub queue_capacity_mb: u32,
}

fn default_writeback_mode() -> String {
    "strict".to_string()
}
fn default_max_batch_size() -> u32 {
    1024
}
fn default_max_batch_wait_ms() -> u32 {
    50
}
fn default_max_retry_duration_ms() -> u32 {
    30_000
}
fn default_max_inflight_age_ms() -> u32 {
    60_000
}
fn default_worker_pool_size() -> u32 {
    8
}
fn default_queue_capacity_mb() -> u32 {
    16
}

fn default_prefetch_full_threshold_mb() -> u64 {
    256
}

fn default_prefetch_partial_threshold_mb() -> u64 {
    4096
}

fn default_prefetch_pressure_decline() -> f64 {
    0.90
}

#[allow(dead_code)]
impl WritebackConfig {
    pub fn parsed_mode(&self) -> WritebackMode {
        WritebackMode::from_str_lossy(&self.mode)
    }
}

impl Default for WritebackConfig {
    fn default() -> Self {
        Self {
            mode: default_writeback_mode(),
            max_batch_size: default_max_batch_size(),
            max_batch_wait_ms: default_max_batch_wait_ms(),
            max_retry_duration_ms: default_max_retry_duration_ms(),
            max_inflight_age_ms: default_max_inflight_age_ms(),
            worker_pool_size: default_worker_pool_size(),
            queue_capacity_mb: default_queue_capacity_mb(),
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub rss_addrs: Vec<String>,
    pub bucket_name: String,
    pub mount_point: String,

    pub nfs_port: u16,

    pub rpc_request_timeout_seconds: u64,
    pub rpc_connection_timeout_seconds: u64,
    pub rss_rpc_timeout_seconds: u64,
    pub worker_threads: usize,
    pub allow_other: bool,
    #[allow(dead_code)]
    pub auto_unmount: bool,

    pub dir_cache_ttl_seconds: u64,
    #[allow(dead_code)]
    pub attr_cache_ttl_seconds: u64,
    #[allow(dead_code)]
    pub block_cache_size_mb: u64,
    pub read_write: bool,

    pub disk_cache_enabled: bool,
    pub disk_cache_path: String,
    pub disk_cache_size_gb: u64,
    pub passthrough_enabled: bool,
    pub passthrough_max_object_size_gb: u64,

    /// Open-time whole-blob prefetch threshold. Files at or below this
    /// size always prefetch on open. Default 256 MiB.
    #[serde(default = "default_prefetch_full_threshold_mb")]
    pub prefetch_full_threshold_mb: u64,
    /// Larger files prefetch only when the kernel sets `FOPEN_KEEP_CACHE`
    /// (a sequential / bulk-read hint) and the file is at or below this
    /// size. Default 4096 MiB.
    #[serde(default = "default_prefetch_partial_threshold_mb")]
    pub prefetch_partial_threshold_mb: u64,
    /// Per-volume opt-in: always prefetch regardless of size hints.
    /// Suitable for log / training / backup workloads.
    #[serde(default)]
    pub workload_bulk_read: bool,
    /// Decline prefetch when current disk-cache usage is at or above
    /// this fraction of capacity (0.0-1.0). Default 0.90.
    #[serde(default = "default_prefetch_pressure_decline")]
    pub prefetch_pressure_decline: f64,

    #[serde(default)]
    #[allow(dead_code)]
    pub writeback: WritebackConfig,
}

impl Config {
    pub fn rpc_request_timeout(&self) -> Duration {
        Duration::from_secs(self.rpc_request_timeout_seconds)
    }

    pub fn rpc_connection_timeout(&self) -> Duration {
        Duration::from_secs(self.rpc_connection_timeout_seconds)
    }

    pub fn rss_rpc_timeout(&self) -> Duration {
        Duration::from_secs(self.rss_rpc_timeout_seconds)
    }

    pub fn dir_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.dir_cache_ttl_seconds)
    }

    #[allow(dead_code)]
    pub fn attr_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.attr_cache_ttl_seconds)
    }

    /// Override config fields from FS_SERVER_* environment variables.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("FS_SERVER_BUCKET_NAME") {
            self.bucket_name = v;
        }
        if let Ok(v) = std::env::var("FS_SERVER_MOUNT_POINT") {
            self.mount_point = v;
        }
        if let Ok(v) = std::env::var("FS_SERVER_READ_WRITE") {
            self.read_write = v.parse().unwrap_or(self.read_write);
        }
        if let Ok(v) = std::env::var("FS_SERVER_DISK_CACHE_ENABLED") {
            self.disk_cache_enabled = v.parse().unwrap_or(self.disk_cache_enabled);
        }
        if let Ok(v) = std::env::var("FS_SERVER_DISK_CACHE_PATH") {
            self.disk_cache_path = v;
        }
        if let Ok(v) = std::env::var("FS_SERVER_DISK_CACHE_SIZE_GB") {
            self.disk_cache_size_gb = v.parse().unwrap_or(self.disk_cache_size_gb);
        }
        if let Ok(v) = std::env::var("FS_SERVER_NFS_PORT") {
            self.nfs_port = v.parse().unwrap_or(self.nfs_port);
        }
        if let Ok(v) = std::env::var("FS_SERVER_WORKER_THREADS") {
            self.worker_threads = v.parse().unwrap_or(self.worker_threads);
        }
        if let Ok(v) = std::env::var("FS_SERVER_WRITEBACK_MODE") {
            self.writeback.mode = v;
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rss_addrs: vec!["127.0.0.1:8086".to_string()],
            bucket_name: "default".to_string(),
            mount_point: "/mnt/fractalbits".to_string(),
            rpc_request_timeout_seconds: 30,
            rpc_connection_timeout_seconds: 5,
            rss_rpc_timeout_seconds: 30,
            worker_threads: 2,
            allow_other: false,
            auto_unmount: false,
            dir_cache_ttl_seconds: 5,
            attr_cache_ttl_seconds: 5,
            block_cache_size_mb: 256,
            nfs_port: 2049,
            read_write: false,
            disk_cache_enabled: false,
            disk_cache_path: "/var/cache/fractalbits/".to_string(),
            disk_cache_size_gb: 50,
            passthrough_enabled: false,
            passthrough_max_object_size_gb: 10,
            prefetch_full_threshold_mb: default_prefetch_full_threshold_mb(),
            prefetch_partial_threshold_mb: default_prefetch_partial_threshold_mb(),
            workload_bulk_read: false,
            prefetch_pressure_decline: default_prefetch_pressure_decline(),
            writeback: WritebackConfig::default(),
        }
    }
}
