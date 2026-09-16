use serde::Deserialize;
use std::time::Duration;
use strum::EnumString;

/// Writeback-cache durability mode.
///
/// `Strict` is the legacy synchronous path: every FUSE op blocks until
/// the corresponding gateway RPC completes. `Default` enables the
/// writeback fast path for the enabled operation slice and falls back
/// to strict for the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, EnumString)]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum WritebackMode {
    #[default]
    Strict,
    Default,
}

fn default_writeback_mode() -> String {
    "default".to_string()
}
fn default_writeback_poll_ms() -> u32 {
    // Tight by default: the metadata path issues one put_inode per intent
    // (no batching yet), so a large poll interval just adds latency that
    // drain_inode_to_barrier (every unlink/rmdir/close) then waits out. An
    // operator can still raise this to widen the batch-accumulation window.
    2
}

fn default_prefetch_full_threshold_mb() -> u64 {
    256
}

fn default_prefetch_partial_threshold_mb() -> u64 {
    4096
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    /// `host:port` of one or more fs_gateway instances (tried in order).
    pub gateway_addrs: Vec<String>,
    pub bucket_name: String,
    pub mount_point: String,
    /// API key used to sign `Mount`. Empty only for local development
    /// against a gateway that does not require authentication.
    #[serde(default)]
    pub api_key_id: String,
    #[serde(default)]
    pub api_key_secret: String,

    pub rpc_request_timeout_seconds: u64,
    pub rpc_connection_timeout_seconds: u64,
    pub worker_threads: usize,
    pub allow_other: bool,
    pub auto_unmount: bool,

    pub dir_cache_ttl_seconds: u64,
    pub attr_cache_ttl_seconds: u64,
    pub read_write: bool,

    /// Open-time whole-blob prefetch threshold. Files at or below this
    /// size always ask the gateway to prefetch on open. Default 256 MiB.
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

    /// Writeback durability mode; `default` (cache on) or `strict`.
    #[serde(default = "default_writeback_mode")]
    pub writeback_mode: String,
    /// Writeback worker poll interval in ms (default 2); the drainer polls
    /// this often. Clamped to 1..=1000 at startup.
    #[serde(default = "default_writeback_poll_ms")]
    pub writeback_poll_ms: u32,
}

impl Config {
    pub fn rpc_request_timeout(&self) -> Duration {
        Duration::from_secs(self.rpc_request_timeout_seconds)
    }

    pub fn rpc_connection_timeout(&self) -> Duration {
        Duration::from_secs(self.rpc_connection_timeout_seconds)
    }

    pub fn dir_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.dir_cache_ttl_seconds)
    }

    pub fn attr_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.attr_cache_ttl_seconds)
    }

    /// Start this mount's connections at a random gateway. Every worker
    /// thread connects to the first address that accepts it and stays on
    /// that connection, so without this every mount on a cluster lands on
    /// the first listed gateway; with it, mounts spread while each mount
    /// still keeps one gateway, and its caches, until that one fails.
    pub fn spread_gateways(&mut self) {
        if self.gateway_addrs.len() > 1 {
            let start = rand::random_range(0..self.gateway_addrs.len());
            self.rotate_gateways(start);
        }
    }

    fn rotate_gateways(&mut self, start: usize) {
        self.gateway_addrs.rotate_left(start);
    }

    /// Override config fields from FS_MOUNT_* environment variables.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("FS_MOUNT_GATEWAY_ADDRS") {
            self.gateway_addrs = v.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Ok(v) = std::env::var("FS_MOUNT_BUCKET_NAME") {
            self.bucket_name = v;
        }
        if let Ok(v) = std::env::var("FS_MOUNT_MOUNT_POINT") {
            self.mount_point = v;
        }
        if let Ok(v) = std::env::var("FS_MOUNT_API_KEY_ID") {
            self.api_key_id = v;
        }
        if let Ok(v) = std::env::var("FS_MOUNT_API_KEY_SECRET") {
            self.api_key_secret = v;
        }
        if let Ok(v) = std::env::var("FS_MOUNT_READ_WRITE") {
            self.read_write = v.parse().unwrap_or(self.read_write);
        }
        if let Ok(v) = std::env::var("FS_MOUNT_WORKER_THREADS") {
            self.worker_threads = v.parse().unwrap_or(self.worker_threads);
        }
        if let Ok(v) = std::env::var("FS_MOUNT_WRITEBACK_MODE") {
            self.writeback_mode = v;
        }
        if let Ok(v) = std::env::var("FS_MOUNT_WRITEBACK_POLL_MS") {
            self.writeback_poll_ms = v.parse().unwrap_or(self.writeback_poll_ms);
        }
        if let Ok(v) = std::env::var("FS_MOUNT_ALLOW_OTHER") {
            self.allow_other = v.parse().unwrap_or(self.allow_other);
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            gateway_addrs: vec!["127.0.0.1:8180".to_string()],
            bucket_name: "default".to_string(),
            mount_point: "/mnt/fractalbits".to_string(),
            api_key_id: String::new(),
            api_key_secret: String::new(),
            rpc_request_timeout_seconds: 30,
            rpc_connection_timeout_seconds: 5,
            worker_threads: 2,
            allow_other: false,
            auto_unmount: false,
            dir_cache_ttl_seconds: 5,
            attr_cache_ttl_seconds: 5,
            read_write: false,
            prefetch_full_threshold_mb: default_prefetch_full_threshold_mb(),
            prefetch_partial_threshold_mb: default_prefetch_partial_threshold_mb(),
            workload_bulk_read: false,
            writeback_mode: default_writeback_mode(),
            writeback_poll_ms: default_writeback_poll_ms(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_keeps_every_gateway_and_only_reorders() {
        let mut cfg = Config {
            gateway_addrs: vec!["a".into(), "b".into(), "c".into()],
            ..Default::default()
        };
        cfg.rotate_gateways(1);
        assert_eq!(cfg.gateway_addrs, vec!["b", "c", "a"], "rotated by one");
        cfg.rotate_gateways(2);
        assert_eq!(cfg.gateway_addrs, vec!["a", "b", "c"], "rotated back");
        for _ in 0..20 {
            cfg.spread_gateways();
            let mut sorted = cfg.gateway_addrs.clone();
            sorted.sort();
            assert_eq!(sorted, vec!["a", "b", "c"], "a spread is a rotation");
        }
        let mut single = Config {
            gateway_addrs: vec!["only".into()],
            ..Default::default()
        };
        single.spread_gateways();
        assert_eq!(
            single.gateway_addrs,
            vec!["only"],
            "one address is left alone"
        );
    }
}
