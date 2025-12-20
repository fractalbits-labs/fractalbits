use cmd_lib::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Error;
use std::time::{Duration, Instant};

use crate::common::{
    BOOTSTRAP_CONFIG, ETC_PATH, download_from_s3, get_builds_bucket, get_instance_id,
};

const CONFIG_RETRY_TIMEOUT_SECS: u64 = 120;

#[derive(Debug, Deserialize)]
pub struct BootstrapConfig {
    pub global: GlobalConfig,
    #[serde(default)]
    pub aws: Option<AwsConfig>,
    pub endpoints: EndpointsConfig,
    pub resources: ResourcesConfig,
    #[serde(default)]
    pub etcd: Option<EtcdConfig>,
    #[serde(default)]
    pub instances: HashMap<String, InstanceConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum JournalType {
    #[default]
    Ebs,
    Nvme,
}

#[derive(Debug, Deserialize)]
pub struct GlobalConfig {
    pub for_bench: bool,
    pub data_blob_storage: String,
    pub rss_ha_enabled: bool,
    #[serde(default)]
    pub rss_backend: String,
    #[serde(default)]
    pub journal_type: JournalType,
    #[serde(default)]
    pub num_bss_nodes: Option<usize>,
    #[serde(default)]
    pub meta_stack_testing: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EtcdConfig {
    pub enabled: bool,
    pub cluster_id: String,
    pub cluster_size: usize,
    pub s3_bucket: String,
}

#[derive(Debug, Deserialize)]
pub struct AwsConfig {
    pub bucket: String,
    #[allow(dead_code)]
    pub local_az: String,
    #[serde(default)]
    pub remote_az: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct EndpointsConfig {
    pub nss_endpoint: String,
    #[serde(default)]
    pub mirrord_endpoint: Option<String>,
    #[serde(default)]
    pub api_server_endpoint: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ResourcesConfig {
    pub nss_a_id: String,
    #[serde(default)]
    pub nss_b_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InstanceConfig {
    pub service_type: String,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub leader_id: Option<String>,
    #[serde(default)]
    pub volume_id: Option<String>,
    #[serde(default)]
    pub bench_client_num: Option<usize>,
}

impl BootstrapConfig {
    pub fn download_and_parse() -> Result<Self, Error> {
        let builds_bucket = get_builds_bucket()?;
        let s3_path = format!("{builds_bucket}/{BOOTSTRAP_CONFIG}");
        let local_path = format!("{ETC_PATH}{BOOTSTRAP_CONFIG}");
        let instance_id = get_instance_id()?;

        let start_time = Instant::now();
        let timeout = Duration::from_secs(CONFIG_RETRY_TIMEOUT_SECS);
        loop {
            // Retry download if the file doesn't exist yet (race condition with CDK deployment)
            match download_from_s3(&s3_path, &local_path) {
                Ok(_) => {}
                Err(e) => {
                    if start_time.elapsed() > timeout {
                        return Err(Error::other(format!(
                            "Failed to download bootstrap config after {CONFIG_RETRY_TIMEOUT_SECS}s: {e}"
                        )));
                    }
                    info!("Download failed ({e}), waiting 5s and retrying...");
                    std::thread::sleep(Duration::from_secs(5));
                    continue;
                }
            }

            let content = std::fs::read_to_string(&local_path)?;
            let config: BootstrapConfig = toml::from_str(&content)
                .map_err(|e| Error::other(format!("TOML parse error: {e}")))?;

            if config.instances.contains_key(&instance_id) {
                info!("Found instance {instance_id} in bootstrap config");
                return Ok(config);
            }

            if start_time.elapsed() > timeout {
                info!(
                    "Instance {instance_id} not in config after {CONFIG_RETRY_TIMEOUT_SECS}s, proceeding anyway"
                );
                return Ok(config);
            }

            info!("Instance {instance_id} not yet in config, waiting 5s and retrying...");
            std::thread::sleep(Duration::from_secs(5));
        }
    }

    pub fn is_etcd_backend(&self) -> bool {
        self.global.rss_backend == "etcd"
    }
}
