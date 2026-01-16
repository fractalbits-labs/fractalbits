use crate::CmdResult;
use cmd_lib::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Error;
use std::path::Path;
use xtask_common::{
    BOOTSTRAP_CLUSTER_CONFIG, BootstrapClusterConfig, ClusterEndpointsConfig, ClusterEtcdConfig,
    ClusterGlobalConfig, DataBlobStorage, DeployTarget, JournalType, NodeEntry, RssBackend,
};

use super::upload::upload_with_endpoint;

#[derive(Debug, Deserialize)]
pub struct InputClusterGlobal {
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub for_bench: bool,
    #[serde(default)]
    pub rss_ha_enabled: bool,
    #[serde(default = "default_num_bss_nodes")]
    pub num_bss_nodes: usize,
    #[serde(default)]
    pub num_api_servers: Option<usize>,
    #[serde(default)]
    pub num_bench_clients: Option<usize>,
    #[serde(default)]
    pub cpu_target: Option<String>,
}

fn default_region() -> String {
    "on-prem".to_string()
}

fn default_num_bss_nodes() -> usize {
    6
}

#[derive(Debug, Deserialize)]
pub struct InputClusterEndpoints {
    #[serde(default)]
    pub nss_endpoint: Option<String>,
    #[serde(default)]
    pub api_server_endpoint: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct InputNodeEntry {
    pub ip: String,
    #[serde(default)]
    pub hostname: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub volume_id: Option<String>,
    #[serde(default)]
    pub bench_client_num: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct InputClusterConfig {
    pub global: InputClusterGlobal,
    #[serde(default)]
    pub endpoints: Option<InputClusterEndpoints>,
    pub nodes: HashMap<String, Vec<InputNodeEntry>>,
}

impl InputClusterConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let content = std::fs::read_to_string(&path).map_err(|e| {
            Error::other(format!(
                "Failed to read cluster config from {}: {}",
                path.as_ref().display(),
                e
            ))
        })?;

        toml::from_str(&content).map_err(|e| {
            Error::other(format!(
                "Failed to parse cluster config from {}: {}",
                path.as_ref().display(),
                e
            ))
        })
    }

    pub fn to_bootstrap_cluster_toml(&self) -> Result<String, Error> {
        let cluster_id = format!(
            "fractalbits-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        // On-prem always uses these fixed settings
        let global = ClusterGlobalConfig {
            deploy_target: DeployTarget::OnPrem,
            region: self.global.region.clone(),
            for_bench: self.global.for_bench,
            data_blob_storage: DataBlobStorage::AllInBssSingleAz,
            rss_ha_enabled: self.global.rss_ha_enabled,
            rss_backend: RssBackend::Etcd,
            journal_type: JournalType::Nvme,
            num_bss_nodes: Some(self.global.num_bss_nodes),
            num_api_servers: self.global.num_api_servers,
            num_bench_clients: self.global.num_bench_clients,
            cpu_target: self.global.cpu_target.clone(),
            workflow_cluster_id: Some(cluster_id),
            meta_stack_testing: false,
        };

        let nss_endpoint = self
            .endpoints
            .as_ref()
            .and_then(|e| e.nss_endpoint.clone())
            .or_else(|| {
                self.nodes
                    .get("nss_server")
                    .and_then(|nodes| nodes.first())
                    .map(|n| n.ip.clone())
            })
            .unwrap_or_default();

        let endpoints = ClusterEndpointsConfig {
            nss_endpoint,
            api_server_endpoint: self
                .endpoints
                .as_ref()
                .and_then(|e| e.api_server_endpoint.clone()),
        };

        // On-prem always uses etcd
        let etcd = Some(ClusterEtcdConfig {
            enabled: true,
            cluster_size: self.global.num_bss_nodes,
            endpoints: None,
        });

        // Convert input nodes to output format (already grouped by service_type)
        let nodes: HashMap<String, Vec<NodeEntry>> = self
            .nodes
            .iter()
            .map(|(service_type, entries)| {
                let node_entries: Vec<NodeEntry> = entries
                    .iter()
                    .map(|node| NodeEntry {
                        id: node.hostname.clone().unwrap_or_else(|| node.ip.clone()),
                        private_ip: Some(node.ip.clone()),
                        role: node.role.clone(),
                        volume_id: node.volume_id.clone(),
                        bench_client_num: node.bench_client_num,
                    })
                    .collect();
                (service_type.clone(), node_entries)
            })
            .collect();

        let config = BootstrapClusterConfig {
            global,
            aws: None,
            endpoints,
            resources: None,
            etcd,
            nodes,
            bootstrap_bucket: "fractalbits-bootstrap".to_string(),
        };

        config
            .to_toml()
            .map_err(|e| Error::other(format!("Failed to serialize bootstrap_cluster.toml: {}", e)))
    }
}

pub fn create_cluster(
    cluster_config_path: &str,
    bootstrap_s3_url: &str,
    watch_bootstrap: bool,
    skip_upload: bool,
) -> CmdResult {
    if !skip_upload {
        info!("Uploading binaries to S3 bootstrap bucket...");
        upload_with_endpoint(DeployTarget::OnPrem, Some(bootstrap_s3_url))?;
    }

    let config = InputClusterConfig::from_file(cluster_config_path)?;

    let total_nodes: usize = config.nodes.values().map(|v| v.len()).sum();
    info!(
        "Creating cluster with {} nodes, bootstrap S3 URL: {}",
        total_nodes, bootstrap_s3_url
    );

    let bootstrap_toml = config.to_bootstrap_cluster_toml()?;
    info!(
        "Generated {}:\n{}",
        BOOTSTRAP_CLUSTER_CONFIG, bootstrap_toml
    );

    info!("Uploading {} to S3...", BOOTSTRAP_CLUSTER_CONFIG);
    let s3_key = format!("s3://fractalbits-bootstrap/{}", BOOTSTRAP_CLUSTER_CONFIG);
    run_cmd!(
        echo $bootstrap_toml |
            AWS_DEFAULT_REGION=localdev
            AWS_ENDPOINT_URL_S3=http://$bootstrap_s3_url
            AWS_ACCESS_KEY_ID=test_api_key
            AWS_SECRET_ACCESS_KEY=test_api_secret
            aws s3 cp --no-progress - $s3_key
    )
    .map_err(|e| {
        Error::other(format!(
            "Failed to upload {} to S3: {}",
            BOOTSTRAP_CLUSTER_CONFIG, e
        ))
    })?;

    info!("{} uploaded successfully", BOOTSTRAP_CLUSTER_CONFIG);

    for (service_type, nodes) in &config.nodes {
        for node in nodes {
            let node_ip = &node.ip;
            info!("Bootstrapping node {} (service: {})", node_ip, service_type);

            let bootstrap_cmd = format!(
                "export AWS_DEFAULT_REGION=localdev && \
                 export AWS_ENDPOINT_URL_S3=http://{bootstrap_s3_url} && \
                 export AWS_ACCESS_KEY_ID=test_api_key && \
                 export AWS_SECRET_ACCESS_KEY=test_api_secret && \
                 aws s3 cp --no-progress s3://fractalbits-bootstrap/bootstrap.sh - | sh"
            );

            run_cmd!(ssh $node_ip $bootstrap_cmd).map_err(|e| {
                Error::other(format!("Failed to bootstrap node {}: {}", node_ip, e))
            })?;

            info!("Node {} bootstrapped successfully", node_ip);
        }
    }

    info!("Cluster creation completed for {} nodes", total_nodes);

    if watch_bootstrap {
        super::bootstrap::show_progress(xtask_common::DeployTarget::OnPrem)?;
    } else {
        info!("To monitor bootstrap progress, run:");
        info!("  cargo xtask deploy bootstrap-progress --vpc-target on-prem");
    }

    Ok(())
}
