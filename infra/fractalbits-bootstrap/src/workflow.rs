//! Cloud-storage-based workflow barrier system for bootstrap coordination.
//!
//! Services progress through well-defined stages, writing stage completion
//! objects to cloud storage (AWS S3 or GCS). This provides clear dependency
//! ordering and visibility into bootstrap progress.

use crate::common::get_instance_id;
use crate::config::{BootstrapConfig, DeployTarget};
use cmd_lib::*;
use serde::{Deserialize, Serialize};
use std::io::Error;
use std::time::{Duration, Instant};
use xtask_common::cloud_storage;
use xtask_common::stages::StageDef;

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

/// Stage completion object written to cloud storage
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

impl StageCompletion {
    /// Extract a string field from metadata across multiple completions, sorted.
    /// Useful for collecting per-node data (e.g. IPs) written via stage metadata.
    pub fn extract_metadata_field(completions: &[Self], field: &str) -> Vec<String> {
        let mut values: Vec<String> = completions
            .iter()
            .filter_map(|c| c.metadata.as_ref()?.get(field)?.as_str().map(String::from))
            .collect();
        values.sort();
        values
    }
}

/// Workflow barrier for coordinating bootstrap stages via cloud storage
pub struct WorkflowBarrier {
    bucket: String,
    cluster_id: String,
    instance_id: String,
    service_type: String,
    deploy_target: DeployTarget,
}

impl WorkflowBarrier {
    /// Create a new workflow barrier
    pub fn new(
        bucket: &str,
        cluster_id: &str,
        instance_id: &str,
        service_type: &str,
        deploy_target: DeployTarget,
    ) -> Self {
        Self {
            bucket: bucket.to_string(),
            cluster_id: cluster_id.to_string(),
            instance_id: instance_id.to_string(),
            service_type: service_type.to_string(),
            deploy_target,
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
        let instance_id = get_instance_id(config.global.deploy_target)?;
        Ok(Self::new(
            bucket,
            cluster_id,
            &instance_id,
            service_type.as_str(),
            config.global.deploy_target,
        ))
    }

    /// Key prefix for workflow data (no URI scheme)
    fn workflow_prefix(&self) -> String {
        format!("workflow/{}", self.cluster_id)
    }

    /// Key for a stage directory
    fn stage_key(&self, stage: &str) -> String {
        format!("{}/stages/{}", self.workflow_prefix(), stage)
    }

    /// Key for this instance's stage completion
    fn instance_stage_key(&self, stage: &str) -> String {
        format!("{}/{}.json", self.stage_key(stage), self.instance_id)
    }

    /// Write stage completion marker (per-node stage)
    pub fn complete_stage(
        &self,
        stage: StageDef,
        metadata: Option<serde_json::Value>,
    ) -> CmdResult {
        let key_name = stage.key_name();
        let stage = stage.name;
        let completion = StageCompletion {
            instance_id: self.instance_id.clone(),
            service_type: self.service_type.clone(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            version: "1.0".to_string(),
            metadata,
        };

        let json = serde_json::to_string(&completion)
            .map_err(|e| Error::other(format!("Failed to serialize stage completion: {e}")))?;

        let key = self.instance_stage_key(&key_name);
        info!("Completing stage '{stage}' at {}/{key}", self.bucket);
        cloud_storage::upload_string(
            &json,
            &cloud_storage::object_uri(&self.bucket, &key, self.deploy_target),
        )
    }

    /// Write a global stage completion marker (single file, not per-node)
    pub fn complete_global_stage(
        &self,
        stage: StageDef,
        metadata: Option<serde_json::Value>,
    ) -> CmdResult {
        let key_name = stage.key_name();
        let stage = stage.name;
        let completion = StageCompletion {
            instance_id: self.instance_id.clone(),
            service_type: self.service_type.clone(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            version: "1.0".to_string(),
            metadata,
        };

        let json = serde_json::to_string(&completion)
            .map_err(|e| Error::other(format!("Failed to serialize stage completion: {e}")))?;

        let key = format!("{}.json", self.stage_key(&key_name));
        info!("Completing global stage '{stage}' at {}/{key}", self.bucket);
        cloud_storage::upload_string(
            &json,
            &cloud_storage::object_uri(&self.bucket, &key, self.deploy_target),
        )
    }

    /// Wait for a global stage (single file, no node suffix)
    pub fn wait_for_global(&self, stage: StageDef, timeout_secs: u64) -> CmdResult {
        let key_name = stage.key_name();
        let stage = stage.name;
        let key = format!("{}/stages/{}.json", self.workflow_prefix(), key_name);
        info!("Waiting for global stage '{stage}' ({}/{key})", self.bucket);

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        loop {
            if start.elapsed() > timeout {
                return Err(Error::other(format!(
                    "Timeout waiting for global stage '{stage}' after {timeout_secs}s"
                )));
            }

            if cloud_storage::head_object(&self.bucket, &key, self.deploy_target) {
                info!("Global stage '{stage}' is complete");
                return Ok(());
            }

            std::thread::sleep(Duration::from_secs(POLL_INTERVAL_SECS));
        }
    }

    /// Wait for N nodes to complete a per-node stage
    pub fn wait_for_nodes(
        &self,
        stage: StageDef,
        expected: usize,
        timeout_secs: u64,
    ) -> Result<Vec<StageCompletion>, Error> {
        let key_name = stage.key_name();
        let stage = stage.name;
        info!("Waiting for {expected} nodes to complete stage '{stage}'");

        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs);

        loop {
            if start.elapsed() > timeout {
                return Err(Error::other(format!(
                    "Timeout waiting for {expected} nodes at stage '{stage}' after {timeout_secs}s"
                )));
            }

            let completions = self.get_stage_completions(&key_name)?;
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
        let prefix = format!("{}/", self.stage_key(stage));
        let output = cloud_storage::list_objects(&self.bucket, &prefix, self.deploy_target)
            .unwrap_or_default();
        if output.trim().is_empty() {
            return Ok(Vec::new());
        }

        let mut completions = Vec::new();
        for filename in parse_listing_filenames(&output, self.deploy_target) {
            if filename.ends_with(".json") {
                let key = format!("{prefix}{filename}");
                if let Ok(content) = cloud_storage::cat(&cloud_storage::object_uri(
                    &self.bucket,
                    &key,
                    self.deploy_target,
                )) && let Ok(completion) = serde_json::from_str::<StageCompletion>(&content)
                {
                    completions.push(completion);
                }
            }
        }

        Ok(completions)
    }
}

/// Parse filenames from cloud storage listing output.
/// AWS `aws s3 ls` returns lines like: `2024-01-01 00:00:00    123 filename.json`
/// GCS `gcloud storage ls` returns full paths like: `gs://bucket/prefix/filename.json`
fn parse_listing_filenames(output: &str, deploy_target: DeployTarget) -> Vec<String> {
    match deploy_target {
        DeployTarget::Gcp => output
            .lines()
            .filter_map(|line| {
                let line = line.trim();
                if line.is_empty() {
                    return None;
                }
                // GCS returns full gs:// paths; extract just the filename
                line.rsplit('/').next().map(|s| s.to_string())
            })
            .filter(|s| !s.is_empty())
            .collect(),
        _ => {
            // AWS S3 listing: "2024-01-01 00:00:00    123 filename.json"
            output
                .lines()
                .filter_map(|line| {
                    let parts: Vec<_> = line.split_whitespace().collect();
                    if parts.len() >= 4 {
                        Some(parts[3].to_string())
                    } else {
                        None
                    }
                })
                .collect()
        }
    }
}

pub use xtask_common::stages;

/// Timeout constants for each stage (in seconds)
#[allow(dead_code)]
pub mod timeouts {
    pub const INSTANCES_READY: u64 = 120;
    pub const ETCD_READY: u64 = 600;
    pub const RSS_INITIALIZED: u64 = 600;
    pub const METADATA_VG_READY: u64 = 600;
    pub const NSS_FORMATTED: u64 = 600;
    pub const MIRRORD_READY: u64 = 120;
    pub const NSS_JOURNAL_READY: u64 = 600;
    pub const BSS_CONFIGURED: u64 = 1200; // 20 min: storage_alloc_mode=.write_zero is slower
    pub const SERVICES_READY: u64 = 60;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_metadata_field() {
        let completions = vec![
            StageCompletion {
                instance_id: "i-1".to_string(),
                service_type: "bss_server".to_string(),
                timestamp: "2024-01-01T00:00:00Z".to_string(),
                version: "1.0".to_string(),
                metadata: Some(serde_json::json!({"ip": "10.0.1.6"})),
            },
            StageCompletion {
                instance_id: "i-2".to_string(),
                service_type: "bss_server".to_string(),
                timestamp: "2024-01-01T00:00:01Z".to_string(),
                version: "1.0".to_string(),
                metadata: Some(serde_json::json!({"ip": "10.0.1.5"})),
            },
            StageCompletion {
                instance_id: "i-3".to_string(),
                service_type: "bss_server".to_string(),
                timestamp: "2024-01-01T00:00:02Z".to_string(),
                version: "1.0".to_string(),
                metadata: None,
            },
        ];

        let ips = StageCompletion::extract_metadata_field(&completions, "ip");
        assert_eq!(ips, vec!["10.0.1.5", "10.0.1.6"]); // sorted
    }

    #[test]
    fn test_parse_listing_filenames_aws() {
        let output =
            "2024-01-01 00:00:00    123 node1.json\n2024-01-01 00:00:01    456 node2.json\n";
        let filenames = parse_listing_filenames(output, DeployTarget::Aws);
        assert_eq!(filenames, vec!["node1.json", "node2.json"]);
    }

    #[test]
    fn test_parse_listing_filenames_gcp() {
        let output = "gs://bucket/workflow/123/stages/00-instances-ready/node1.json\ngs://bucket/workflow/123/stages/00-instances-ready/node2.json\n";
        let filenames = parse_listing_filenames(output, DeployTarget::Gcp);
        assert_eq!(filenames, vec!["node1.json", "node2.json"]);
    }
}
