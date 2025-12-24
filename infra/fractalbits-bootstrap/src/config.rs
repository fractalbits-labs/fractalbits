use cmd_lib::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Error;
use std::time::{Duration, Instant};

use crate::common::{
    BOOTSTRAP_CLUSTER_CONFIG, ETC_PATH, download_from_s3, get_bootstrap_bucket, get_instance_id,
};

const CONFIG_RETRY_TIMEOUT_SECS: u64 = 120;

#[derive(Debug, Deserialize)]
pub struct BootstrapConfig {
    pub global: GlobalConfig,
    #[serde(default)]
    pub aws: Option<AwsConfig>,
    pub endpoints: EndpointsConfig,
    #[serde(default)]
    pub resources: Option<ResourcesConfig>,
    #[serde(default)]
    pub etcd: Option<EtcdConfig>,
    #[serde(default)]
    pub nodes: Vec<NodeConfig>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeployTarget {
    #[default]
    Aws,
    OnPrem,
}

#[derive(Debug, Deserialize)]
pub struct GlobalConfig {
    #[serde(default)]
    pub deploy_target: DeployTarget,
    pub region: String,
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
    #[serde(default)]
    pub cpu_target: Option<String>,
    #[serde(default)]
    pub workflow_cluster_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EtcdConfig {
    pub enabled: bool,
    pub cluster_size: usize,
    #[serde(default)]
    pub endpoints: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AwsConfig {
    #[serde(default)]
    pub data_blob_bucket: Option<String>,
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

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ResourcesConfig {
    #[serde(default)]
    pub nss_a_id: String,
    #[serde(default)]
    pub nss_b_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    pub id: String,
    pub service_type: String,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub volume_id: Option<String>,
    #[serde(default)]
    pub bench_client_num: Option<usize>,
    #[serde(default)]
    pub private_ip: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InstanceConfig {
    pub service_type: String,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub volume_id: Option<String>,
    #[serde(default)]
    pub bench_client_num: Option<usize>,
    #[serde(default)]
    pub private_ip: Option<String>,
}

impl BootstrapConfig {
    pub fn download_and_parse() -> Result<Self, Error> {
        let bootstrap_bucket = get_bootstrap_bucket();
        let s3_path = format!("{bootstrap_bucket}/{BOOTSTRAP_CLUSTER_CONFIG}");
        let local_path = format!("{ETC_PATH}{BOOTSTRAP_CLUSTER_CONFIG}");

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

            let instance_id = match config.global.deploy_target {
                DeployTarget::OnPrem => run_fun!(hostname)?,
                DeployTarget::Aws => get_instance_id()?,
            };

            if config.contains_instance(&instance_id) {
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

    pub fn get_instance(&self, id: &str) -> Option<InstanceConfig> {
        if let Some(node) = self.nodes.iter().find(|n| n.id == id) {
            return Some(InstanceConfig {
                service_type: node.service_type.clone(),
                role: node.role.clone(),
                volume_id: node.volume_id.clone(),
                bench_client_num: node.bench_client_num,
                private_ip: node.private_ip.clone(),
            });
        }
        self.instances.get(id).cloned()
    }

    pub fn contains_instance(&self, id: &str) -> bool {
        self.nodes.iter().any(|n| n.id == id) || self.instances.contains_key(id)
    }

    pub fn get_resources(&self) -> ResourcesConfig {
        self.resources.clone().unwrap_or_default()
    }
}
