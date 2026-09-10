use serde::Deserialize;
use std::time::Duration;
use volume_group_proxy::DEFAULT_EC_HEDGE_DELAY;

fn default_ec_read_hedge_delay_ms() -> u64 {
    DEFAULT_EC_HEDGE_DELAY.as_millis() as u64
}
fn default_data_volume() -> String {
    "bss".to_string()
}
fn default_s3_host() -> String {
    "http://127.0.0.1".to_string()
}
fn default_s3_port() -> u16 {
    9000
}
fn default_s3_region() -> String {
    "localdev".to_string()
}
fn default_prefetch_pressure_decline() -> f64 {
    0.90
}
fn default_true() -> bool {
    true
}
fn default_token_ttl_seconds() -> u64 {
    3600
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub rss_addrs: Vec<String>,
    /// Client-facing listen port (all interfaces).
    pub port: u16,
    pub worker_threads: usize,

    pub rpc_request_timeout_seconds: u64,
    pub rpc_connection_timeout_seconds: u64,
    pub rss_rpc_timeout_seconds: u64,
    /// EC read grace period before parity shards are requested.
    #[serde(default = "default_ec_read_hedge_delay_ms")]
    pub ec_read_hedge_delay_ms: u64,

    /// Where new file data goes: `bss` (data volume group) or `s3`.
    #[serde(default = "default_data_volume")]
    pub data_volume: String,
    /// Bucket holding S3-resident blocks. Empty disables S3 access, in
    /// which case S3-resident files are rejected by the client.
    #[serde(default)]
    pub s3_bucket: String,
    #[serde(default = "default_s3_host")]
    pub s3_host: String,
    #[serde(default = "default_s3_port")]
    pub s3_port: u16,
    #[serde(default = "default_s3_region")]
    pub s3_region: String,

    pub disk_cache_enabled: bool,
    pub disk_cache_path: String,
    pub disk_cache_size_gb: u64,
    /// Decline a prefetch when disk-cache usage is at or above this
    /// fraction of capacity (0.0-1.0).
    #[serde(default = "default_prefetch_pressure_decline")]
    pub prefetch_pressure_decline: f64,

    /// Verify the API-key signature on Mount and the bucket permissions
    /// it grants. Off only for local development.
    #[serde(default = "default_true")]
    pub auth_required: bool,
    /// Session token lifetime; clients re-mount transparently on expiry.
    #[serde(default = "default_token_ttl_seconds")]
    pub token_ttl_seconds: u64,
}

impl Config {
    pub fn rpc_request_timeout(&self) -> Duration {
        Duration::from_secs(self.rpc_request_timeout_seconds)
    }

    pub fn ec_read_hedge_delay(&self) -> Duration {
        Duration::from_millis(self.ec_read_hedge_delay_ms)
    }

    pub fn rpc_connection_timeout(&self) -> Duration {
        Duration::from_secs(self.rpc_connection_timeout_seconds)
    }

    pub fn rss_rpc_timeout(&self) -> Duration {
        Duration::from_secs(self.rss_rpc_timeout_seconds)
    }

    pub fn token_ttl(&self) -> Duration {
        Duration::from_secs(self.token_ttl_seconds)
    }

    pub fn s3_enabled(&self) -> bool {
        !self.s3_bucket.is_empty()
    }

    pub fn data_volume_is_s3(&self) -> bool {
        self.data_volume == "s3"
    }

    /// Override config fields from FS_GATEWAY_* environment variables.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("FS_GATEWAY_RSS_ADDRS") {
            self.rss_addrs = v.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Ok(v) = std::env::var("FS_GATEWAY_PORT") {
            self.port = v.parse().unwrap_or(self.port);
        }
        if let Ok(v) = std::env::var("FS_GATEWAY_WORKER_THREADS") {
            self.worker_threads = v.parse().unwrap_or(self.worker_threads);
        }
        if let Ok(v) = std::env::var("FS_GATEWAY_DATA_VOLUME") {
            self.data_volume = v;
        }
        if let Ok(v) = std::env::var("FS_GATEWAY_S3_BUCKET") {
            self.s3_bucket = v;
        }
        if let Ok(v) = std::env::var("FS_GATEWAY_S3_HOST") {
            self.s3_host = v;
        }
        if let Ok(v) = std::env::var("FS_GATEWAY_S3_PORT") {
            self.s3_port = v.parse().unwrap_or(self.s3_port);
        }
        if let Ok(v) = std::env::var("FS_GATEWAY_S3_REGION") {
            self.s3_region = v;
        }
        if let Ok(v) = std::env::var("FS_GATEWAY_DISK_CACHE_ENABLED") {
            self.disk_cache_enabled = v.parse().unwrap_or(self.disk_cache_enabled);
        }
        if let Ok(v) = std::env::var("FS_GATEWAY_DISK_CACHE_PATH") {
            self.disk_cache_path = v;
        }
        if let Ok(v) = std::env::var("FS_GATEWAY_DISK_CACHE_SIZE_GB") {
            self.disk_cache_size_gb = v.parse().unwrap_or(self.disk_cache_size_gb);
        }
        if let Ok(v) = std::env::var("FS_GATEWAY_AUTH_REQUIRED") {
            self.auth_required = v.parse().unwrap_or(self.auth_required);
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rss_addrs: vec!["127.0.0.1:8086".to_string()],
            port: 8180,
            worker_threads: 2,
            rpc_request_timeout_seconds: 30,
            rpc_connection_timeout_seconds: 5,
            rss_rpc_timeout_seconds: 30,
            ec_read_hedge_delay_ms: default_ec_read_hedge_delay_ms(),
            data_volume: default_data_volume(),
            s3_bucket: String::new(),
            s3_host: default_s3_host(),
            s3_port: default_s3_port(),
            s3_region: default_s3_region(),
            disk_cache_enabled: false,
            disk_cache_path: "/var/cache/fractalbits/".to_string(),
            disk_cache_size_gb: 50,
            prefetch_pressure_decline: default_prefetch_pressure_decline(),
            auth_required: true,
            token_ttl_seconds: default_token_ttl_seconds(),
        }
    }
}
