//! S3-based workflow barrier system for bootstrap coordination.
//!
//! Services progress through well-defined stages, writing stage completion
//! objects to S3. This provides clear dependency ordering and visibility
//! into bootstrap progress.

use crate::common::{get_instance_id, get_private_ip};
use crate::config::BootstrapConfig;
use cmd_lib::*;
use serde::{Deserialize, Serialize};
use std::io::Error;
use std::time::{Duration, Instant};

/// Poll interval when waiting for barriers
const POLL_INTERVAL_SECS: u64 = 2;

/// Service types for workflow barriers
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowServiceType {
    Rss,
    Nss,
    Bss,
    Api,
    Bench,
}

impl WorkflowServiceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Rss => "root_server",
            Self::Nss => "nss_server",
            Self::Bss => "bss_server",
            Self::Api => "api_server",
            Self::Bench => "bench",
        }
    }
}

/// Stage completion object written to S3
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageCompletion {
    pub instance_id: String,
    pub service_type: String,
    pub timestamp: String,
    #[serde(default)]
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// Workflow barrier for coordinating bootstrap stages via S3
pub struct WorkflowBarrier {
    bucket: String,
    cluster_id: String,
    instance_id: String,
    service_type: String,
}

impl WorkflowBarrier {
    /// Create a new workflow barrier
    pub fn new(bucket: &str, cluster_id: &str, instance_id: &str, service_type: &str) -> Self {
        Self {
            bucket: bucket.to_string(),
            cluster_id: cluster_id.to_string(),
            instance_id: instance_id.to_string(),
            service_type: service_type.to_string(),
        }
    }

    /// Create a workflow barrier from bootstrap config
    pub fn from_config(
        config: &BootstrapConfig,
        service_type: WorkflowServiceType,
    ) -> Result<Self, Error> {
        let cluster_id = config
            .global
            .workflow_cluster_id
            .as_ref()
            .ok_or_else(|| Error::other("workflow_cluster_id not configured"))?;

        let bucket = &config.bootstrap_bucket;
        let instance_id = get_instance_id()?;
        Ok(Self::new(
            bucket,
            cluster_id,
            &instance_id,
            service_type.as_str(),
        ))
    }

    /// Get the S3 prefix for workflow data
    fn workflow_prefix(&self) -> String {
        format!("s3://{}/workflow/{}", self.bucket, self.cluster_id)
    }

    /// Get the S3 path for a stage directory or file
    fn stage_path(&self, stage: &str) -> String {
        format!("{}/stages/{}", self.workflow_prefix(), stage)
    }

    /// Get the S3 path for this instance's stage completion
    fn instance_stage_path(&self, stage: &str) -> String {
        format!("{}/{}.json", self.stage_path(stage), self.instance_id)
    }

    /// Write stage completion marker to S3 (per-node stage)
    pub fn complete_stage(&self, stage: &str, metadata: Option<serde_json::Value>) -> CmdResult {
        let completion = StageCompletion {
            instance_id: self.instance_id.clone(),
            service_type: self.service_type.clone(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            version: "1.0".to_string(),
            metadata,
        };

        let json = serde_json::to_string(&completion)
            .map_err(|e| Error::other(format!("Failed to serialize stage completion: {e}")))?;

        let s3_path = self.instance_stage_path(stage);
        info!("Completing stage '{stage}' at {s3_path}");
        run_cmd!(echo $json | aws s3 cp - $s3_path --quiet)?;

        Ok(())
    }

    /// Write a global stage completion marker (single file, not per-node)
    pub fn complete_global_stage(
        &self,
        stage: &str,
        metadata: Option<serde_json::Value>,
    ) -> CmdResult {
        let completion = StageCompletion {
            instance_id: self.instance_id.clone(),
            service_type: self.service_type.clone(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            version: "1.0".to_string(),
            metadata,
        };

        let json = serde_json::to_string(&completion)
            .map_err(|e| Error::other(format!("Failed to serialize stage completion: {e}")))?;

        let s3_path = format!("{}.json", self.stage_path(stage));
        info!("Completing global stage '{stage}' at {s3_path}");
        run_cmd!(echo $json | aws s3 cp - $s3_path --quiet)?;

        Ok(())
    }

    /// Wait for a global stage (single file, no node suffix)
    pub fn wait_for_global(&self, stage: &str, timeout_secs: u64) -> CmdResult {
        let bucket = &self.bucket;
        let key = format!("workflow/{}/stages/{}.json", self.cluster_id, stage);
        info!("Waiting for global stage '{stage}' (head-object {bucket}/{key})");

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        loop {
            if start.elapsed() > timeout {
                return Err(Error::other(format!(
                    "Timeout waiting for global stage '{stage}' after {timeout_secs}s"
                )));
            }

            // Check if the file exists
            let result = run_fun!(aws s3api head-object --bucket $bucket --key $key 2>/dev/null);
            if result.is_ok() {
                info!("Global stage '{stage}' is complete");
                return Ok(());
            }

            std::thread::sleep(Duration::from_secs(POLL_INTERVAL_SECS));
        }
    }

    /// Wait for N nodes to complete a per-node stage
    pub fn wait_for_nodes(
        &self,
        stage: &str,
        expected: usize,
        timeout_secs: u64,
    ) -> Result<Vec<StageCompletion>, Error> {
        info!("Waiting for {expected} nodes to complete stage '{stage}'");

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        loop {
            if start.elapsed() > timeout {
                return Err(Error::other(format!(
                    "Timeout waiting for {expected} nodes at stage '{stage}' after {timeout_secs}s"
                )));
            }

            let completions = self.get_stage_completions(stage)?;
            info!(
                "Stage '{stage}': {} of {expected} nodes complete",
                completions.len()
            );

            if completions.len() >= expected {
                info!("Stage '{stage}' complete with {} nodes", completions.len());
                return Ok(completions);
            }

            std::thread::sleep(Duration::from_secs(POLL_INTERVAL_SECS));
        }
    }

    /// List all completions for a per-node stage
    pub fn get_stage_completions(&self, stage: &str) -> Result<Vec<StageCompletion>, Error> {
        let stage_prefix = format!("{}/", self.stage_path(stage));

        let output = run_fun!(aws s3 ls $stage_prefix 2>/dev/null).unwrap_or_default();
        if output.trim().is_empty() {
            return Ok(Vec::new());
        }

        let mut completions = Vec::new();
        for line in output.lines() {
            let parts: Vec<_> = line.split_whitespace().collect();
            if parts.len() >= 4 {
                let filename = parts[3];
                if filename.ends_with(".json") {
                    let s3_path = format!("{}{}", stage_prefix, filename);
                    match run_fun!(aws s3 cp $s3_path - 2>/dev/null) {
                        Ok(content) => {
                            if let Ok(completion) =
                                serde_json::from_str::<StageCompletion>(&content)
                            {
                                completions.push(completion);
                            }
                        }
                        Err(_) => continue,
                    }
                }
            }
        }

        Ok(completions)
    }

    /// Get the etcd nodes prefix for this cluster (unified path)
    pub fn etcd_nodes_prefix(&self) -> String {
        format!("{}/etcd/nodes/", self.workflow_prefix())
    }

    /// Register an etcd node in the workflow S3 structure
    pub fn register_etcd_node(&self) -> CmdResult {
        let my_ip = get_private_ip()?;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let node_info = serde_json::json!({
            "ip": my_ip,
            "timestamp": timestamp,
            "instance_id": self.instance_id,
        });

        let json = serde_json::to_string(&node_info)
            .map_err(|e| Error::other(format!("Failed to serialize node info: {e}")))?;

        let s3_path = format!("{}{}.json", self.etcd_nodes_prefix(), my_ip);
        info!("Registering etcd node at {s3_path}");
        run_cmd!(echo $json | aws s3 cp - $s3_path --quiet)?;

        Ok(())
    }

    /// Get registered etcd nodes from the workflow S3 structure
    pub fn get_etcd_nodes(&self) -> Result<Vec<EtcdNodeInfo>, Error> {
        let prefix = self.etcd_nodes_prefix();

        let output = run_fun!(aws s3 ls $prefix 2>/dev/null).unwrap_or_default();
        if output.trim().is_empty() {
            return Ok(Vec::new());
        }

        let mut nodes = Vec::new();
        for line in output.lines() {
            let parts: Vec<_> = line.split_whitespace().collect();
            if parts.len() >= 4 {
                let filename = parts[3];
                if filename.ends_with(".json") {
                    let s3_path = format!("{}{}", prefix, filename);
                    match run_fun!(aws s3 cp $s3_path - 2>/dev/null) {
                        Ok(content) => {
                            if let Ok(node_info) = serde_json::from_str::<EtcdNodeInfo>(&content) {
                                nodes.push(node_info);
                            }
                        }
                        Err(_) => continue,
                    }
                }
            }
        }

        nodes.sort_by(|a, b| a.ip.cmp(&b.ip));
        Ok(nodes)
    }

    /// Wait for etcd cluster nodes to register
    pub fn wait_for_etcd_nodes(
        &self,
        expected: usize,
        timeout_secs: u64,
    ) -> Result<Vec<EtcdNodeInfo>, Error> {
        info!("Waiting for {expected} etcd nodes to register");

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        loop {
            if start.elapsed() > timeout {
                return Err(Error::other(format!(
                    "Timeout waiting for {expected} etcd nodes after {timeout_secs}s"
                )));
            }

            let nodes = self.get_etcd_nodes()?;
            info!("Found {} of {expected} etcd nodes", nodes.len());

            if nodes.len() >= expected {
                return Ok(nodes);
            }

            std::thread::sleep(Duration::from_secs(POLL_INTERVAL_SECS));
        }
    }
}

/// etcd node registration info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtcdNodeInfo {
    pub ip: String,
    pub timestamp: u64,
    #[serde(default)]
    pub instance_id: Option<String>,
}

impl EtcdNodeInfo {
    /// Generate etcd initial-cluster string from nodes
    pub fn generate_initial_cluster(nodes: &[EtcdNodeInfo]) -> String {
        const ETCD_PEER_PORT: u16 = 2380;
        nodes
            .iter()
            .map(|node| {
                let member_name = format!("bss-{}", node.ip.replace('.', "-"));
                format!("{}=http://{}:{}", member_name, node.ip, ETCD_PEER_PORT)
            })
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Stage constants for type safety
pub mod stages {
    pub const INSTANCES_READY: &str = "00-instances-ready";
    pub const ETCD_READY: &str = "10-etcd-ready";
    pub const RSS_INITIALIZED: &str = "20-rss-initialized";
    pub const METADATA_VG_READY: &str = "25-metadata-vg-ready";
    pub const NSS_FORMATTED: &str = "30-nss-formatted";
    pub const MIRRORD_READY: &str = "35-mirrord-ready";
    pub const NSS_JOURNAL_READY: &str = "40-nss-journal-ready";
    pub const BSS_CONFIGURED: &str = "50-bss-configured";
    pub const SERVICES_READY: &str = "60-services-ready";
}

/// Timeout constants for each stage (in seconds)
#[allow(dead_code)]
pub mod timeouts {
    pub const INSTANCES_READY: u64 = 120;
    pub const ETCD_READY: u64 = 300;
    pub const RSS_INITIALIZED: u64 = 300;
    pub const METADATA_VG_READY: u64 = 300;
    pub const NSS_FORMATTED: u64 = 300;
    pub const MIRRORD_READY: u64 = 120;
    pub const NSS_JOURNAL_READY: u64 = 120;
    pub const BSS_CONFIGURED: u64 = 300;
    pub const SERVICES_READY: u64 = 60;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_initial_cluster() {
        let nodes = vec![
            EtcdNodeInfo {
                ip: "10.0.1.5".to_string(),
                timestamp: 1234567890,
                instance_id: None,
            },
            EtcdNodeInfo {
                ip: "10.0.1.6".to_string(),
                timestamp: 1234567891,
                instance_id: None,
            },
        ];

        let result = EtcdNodeInfo::generate_initial_cluster(&nodes);
        assert_eq!(
            result,
            "bss-10-0-1-5=http://10.0.1.5:2380,bss-10-0-1-6=http://10.0.1.6:2380"
        );
    }
}
