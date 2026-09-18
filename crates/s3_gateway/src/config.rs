use crate::blob_storage::S3RetryConfig;
use serde::Deserialize;
use std::time::Duration;
use volume_group_proxy::DEFAULT_EC_HEDGE_DELAY;

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum BlobStorageBackend {
    S3HybridSingleAz,
    /// Every data blob lives in S3; BSS only serves the journal and metadata volumes.
    DataInS3,
    #[default]
    AllInBssSingleAz,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BlobStorageConfig {
    pub backend: BlobStorageBackend,

    pub s3_hybrid_single_az: Option<S3HybridSingleAzConfig>,

    /// S3 endpoint for the `DataInS3` backend (same shape as the hybrid config).
    #[serde(default)]
    pub data_in_s3: Option<S3HybridSingleAzConfig>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RatelimitConfig {
    pub enabled: bool,
    pub put_qps: u32,
    pub get_qps: u32,
    pub delete_qps: u32,
}

impl Default for RatelimitConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Default to disabled for local testing
            put_qps: 7000,
            get_qps: 10000,
            delete_qps: 5000,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct HttpsConfig {
    pub enabled: bool,
    pub port: u16,
    pub cert_file: String,
    pub key_file: String,
    pub force_http1_only: bool,
}

impl Default for HttpsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8443,
            cert_file: "data/etc/cert.pem".to_string(),
            key_file: "data/etc/key.pem".to_string(),
            force_http1_only: false,
        }
    }
}

fn default_fs_control_port() -> u16 {
    8181
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct Config {
    pub rss_addrs: Vec<String>,

    pub port: u16,
    pub mgmt_port: u16,
    /// ARTFS control plane (`/v1`), the only listener safe to expose:
    /// it carries signed routes and nothing else.
    #[serde(default = "default_fs_control_port")]
    pub fs_control_port: u16,
    pub https: HttpsConfig,
    pub region: String,
    pub root_domain: String,
    pub with_metrics: bool,
    pub http_request_timeout_seconds: u64,
    pub rpc_request_timeout_seconds: u64,
    pub rpc_connection_timeout_seconds: u64,
    pub rss_rpc_timeout_seconds: u64,
    pub client_request_timeout_seconds: u64,
    /// EC read grace period before parity shards are requested.
    #[serde(default = "default_ec_read_hedge_delay_ms")]
    pub ec_read_hedge_delay_ms: u64,
    pub stats_dir: String,
    pub enable_stats_writer: bool,
    pub blob_storage: BlobStorageConfig,
    pub allow_missing_or_bad_signature: bool,
    /// Require the `FBSIG1` signature on the management `/v1` routes. Off
    /// only for local development; see `mgmt_auth`.
    #[serde(default = "default_true")]
    pub mgmt_auth_required: bool,
    pub worker_threads: usize,
    pub set_thread_affinity: bool,
}

fn default_true() -> bool {
    true
}

fn default_ec_read_hedge_delay_ms() -> u64 {
    DEFAULT_EC_HEDGE_DELAY.as_millis() as u64
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

    pub fn http_request_timeout(&self) -> Duration {
        Duration::from_secs(self.http_request_timeout_seconds)
    }

    pub fn client_request_timeout(&self) -> Duration {
        Duration::from_secs(self.client_request_timeout_seconds)
    }
}

#[derive(serde::Deserialize, Debug, Clone)]
pub struct S3HybridSingleAzConfig {
    pub s3_host: String,
    pub s3_port: u16,
    pub s3_region: String,
    pub s3_bucket: String,
    #[serde(default)]
    pub ratelimit: RatelimitConfig,
    #[serde(default)]
    pub retry_config: S3RetryConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self::all_in_bss_single_az()
    }
}

impl Config {
    pub fn data_in_s3() -> Self {
        let mut config = Self::s3_hybrid_single_az();
        config.blob_storage = BlobStorageConfig {
            backend: BlobStorageBackend::DataInS3,
            data_in_s3: config.blob_storage.s3_hybrid_single_az.take(),
            s3_hybrid_single_az: None,
        };
        config
    }

    pub fn s3_hybrid_single_az() -> Self {
        Self {
            rss_addrs: vec!["127.0.0.1:8086".to_string()],
            port: 8080,
            mgmt_port: 18080,
            fs_control_port: default_fs_control_port(),
            https: HttpsConfig::default(),
            region: "localdev".into(),
            root_domain: ".localhost".into(),
            with_metrics: false,
            http_request_timeout_seconds: 120,
            rpc_request_timeout_seconds: 30,
            rpc_connection_timeout_seconds: 5,
            rss_rpc_timeout_seconds: 30,
            client_request_timeout_seconds: 120,
            ec_read_hedge_delay_ms: default_ec_read_hedge_delay_ms(),
            stats_dir: "data/s3-gateway/local/stats".into(),
            enable_stats_writer: false,
            blob_storage: BlobStorageConfig {
                backend: BlobStorageBackend::S3HybridSingleAz,
                s3_hybrid_single_az: Some(S3HybridSingleAzConfig {
                    s3_host: "http://127.0.0.1".into(),
                    s3_port: 9000,
                    s3_region: "localdev".into(),
                    s3_bucket: "fractalbits-bucket".into(),
                    ratelimit: RatelimitConfig::default(),
                    retry_config: S3RetryConfig::default(),
                }),
                data_in_s3: None,
            },
            allow_missing_or_bad_signature: false,
            mgmt_auth_required: true,
            worker_threads: 2,
            set_thread_affinity: false,
        }
    }

    pub fn all_in_bss_single_az() -> Self {
        Self {
            rss_addrs: vec!["127.0.0.1:8086".to_string()],
            port: 8080,
            mgmt_port: 18080,
            fs_control_port: default_fs_control_port(),
            https: HttpsConfig::default(),
            region: "localdev".into(),
            root_domain: ".localhost".into(),
            with_metrics: false,
            http_request_timeout_seconds: 120,
            rpc_request_timeout_seconds: 30,
            rpc_connection_timeout_seconds: 5,
            rss_rpc_timeout_seconds: 30,
            client_request_timeout_seconds: 120,
            ec_read_hedge_delay_ms: default_ec_read_hedge_delay_ms(),
            stats_dir: "data/s3-gateway/local/stats".into(),
            enable_stats_writer: false,
            blob_storage: BlobStorageConfig {
                backend: BlobStorageBackend::AllInBssSingleAz,
                s3_hybrid_single_az: None,
                data_in_s3: None,
            },
            allow_missing_or_bad_signature: false,
            mgmt_auth_required: true,
            worker_threads: 2,
            set_thread_affinity: false,
        }
    }
}
