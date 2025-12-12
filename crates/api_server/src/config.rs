use crate::blob_storage::S3RetryConfig;
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub enum BlobStorageBackend {
    #[default]
    S3HybridSingleAz,
    S3ExpressMultiAz,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BlobStorageConfig {
    pub backend: BlobStorageBackend,

    pub s3_hybrid_single_az: Option<S3HybridSingleAzConfig>,
    pub s3_express_multi_az: Option<S3ExpressMultiAzConfig>,
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
pub struct S3ExpressMultiAzConfig {
    pub local_az_host: String,
    pub local_az_port: u16,
    pub s3_region: String,
    pub local_az_bucket: String,
    pub remote_az_bucket: String,
    pub remote_az_host: String,
    pub remote_az_port: u16,
    pub local_az: String,
    pub remote_az: String,
    pub ratelimit: RatelimitConfig,
    pub retry_config: S3RetryConfig,
}

impl Default for S3ExpressMultiAzConfig {
    fn default() -> Self {
        Self {
            local_az_host: "http://127.0.0.1".into(),
            local_az_port: 9001,
            s3_region: "localdev".into(),
            local_az_bucket: "fractalbits-localdev-az1-data-bucket".into(),
            remote_az_bucket: "fractalbits-localdev-az2-data-bucket".into(),
            remote_az_host: "http://127.0.0.1".into(),
            remote_az_port: 9002,
            local_az: "localdev-az1".into(),
            remote_az: "localdev-az2".into(),
            ratelimit: RatelimitConfig::default(),
            retry_config: S3RetryConfig::default(),
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

#[derive(serde::Deserialize, Debug, Clone)]
pub struct Config {
    pub nss_addr: String,
    pub rss_addrs: Vec<String>,

    pub port: u16,
    pub mgmt_port: u16,
    pub https: HttpsConfig,
    pub region: String,
    pub root_domain: String,
    pub with_metrics: bool,
    pub http_request_timeout_seconds: u64,
    pub rpc_request_timeout_seconds: u64,
    pub rpc_connection_timeout_seconds: u64,
    pub rss_rpc_timeout_seconds: u64,
    pub client_request_timeout_seconds: u64,
    pub stats_dir: String,
    pub enable_stats_writer: bool,
    pub blob_storage: BlobStorageConfig,
    pub allow_missing_or_bad_signature: bool,
    pub worker_threads: usize,
    pub set_thread_affinity: bool,
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
        Self::s3_hybrid_single_az()
    }
}

impl Config {
    pub fn s3_express_multi_az() -> Self {
        Self {
            nss_addr: "127.0.0.1:8087".to_string(),
            rss_addrs: vec!["127.0.0.1:8086".to_string()],
            port: 8080,
            mgmt_port: 18080,
            https: HttpsConfig::default(),
            region: "localdev".into(),
            root_domain: ".localhost".into(),
            with_metrics: false,
            http_request_timeout_seconds: 120,
            rpc_request_timeout_seconds: 30,
            rpc_connection_timeout_seconds: 5,
            rss_rpc_timeout_seconds: 30,
            client_request_timeout_seconds: 120,
            stats_dir: "data/api-server/local/stats".into(),
            enable_stats_writer: false,
            blob_storage: BlobStorageConfig {
                backend: BlobStorageBackend::S3ExpressMultiAz,
                s3_hybrid_single_az: None,
                s3_express_multi_az: Some(S3ExpressMultiAzConfig::default()),
            },
            allow_missing_or_bad_signature: false,
            worker_threads: 2,
            set_thread_affinity: false,
        }
    }

    pub fn s3_hybrid_single_az() -> Self {
        Self {
            nss_addr: "127.0.0.1:8087".to_string(),
            rss_addrs: vec!["127.0.0.1:8086".to_string()],
            port: 8080,
            mgmt_port: 18080,
            https: HttpsConfig::default(),
            region: "localdev".into(),
            root_domain: ".localhost".into(),
            with_metrics: false,
            http_request_timeout_seconds: 120,
            rpc_request_timeout_seconds: 30,
            rpc_connection_timeout_seconds: 5,
            rss_rpc_timeout_seconds: 30,
            client_request_timeout_seconds: 120,
            stats_dir: "data/api-server/local/stats".into(),
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
                s3_express_multi_az: None,
            },
            allow_missing_or_bad_signature: false,
            worker_threads: 2,
            set_thread_affinity: false,
        }
    }
}
