use crate::common::*;
use cmd_lib::*;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Error;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const ETCD_PEER_PORT: u16 = 2380;
const QUORUM_WAIT_TIMEOUT_SECS: u64 = 300;
const POLL_INTERVAL_SECS: u64 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BssNodeInfo {
    pub ip: String,
    pub score: u64,
    pub timestamp: u64,
}

pub struct EtcdClusterCoordinator {
    cluster_id: String,
    s3_bucket: String,
    quorum_size: usize,
    my_ip: String,
    my_score: u64,
}

impl EtcdClusterCoordinator {
    pub fn new(cluster_id: &str, s3_bucket: &str, quorum_size: usize) -> Result<Self, Error> {
        let my_ip = get_private_ip()?;
        let my_score = Self::calculate_bootstrap_score(cluster_id, &my_ip);

        Ok(Self {
            cluster_id: cluster_id.to_string(),
            s3_bucket: s3_bucket.to_string(),
            quorum_size,
            my_ip,
            my_score,
        })
    }

    pub fn calculate_bootstrap_score(cluster_id: &str, ip: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        cluster_id.hash(&mut hasher);
        ip.hash(&mut hasher);
        hasher.finish()
    }

    pub fn register_node(&self) -> CmdResult {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let node_info = BssNodeInfo {
            ip: self.my_ip.clone(),
            score: self.my_score,
            timestamp,
        };

        let json = serde_json::to_string(&node_info)
            .map_err(|e| Error::other(format!("Failed to serialize node info: {e}")))?;

        let s3_path = format!(
            "s3://{}/bootstrap/{}/nodes/{}.json",
            self.s3_bucket, self.cluster_id, self.my_ip
        );

        info!("Registering node at {s3_path}");
        run_cmd!(echo $json | aws s3 cp - $s3_path --quiet)?;

        Ok(())
    }

    pub fn wait_for_quorum(&self) -> Result<Vec<BssNodeInfo>, Error> {
        info!("Waiting for {} nodes to register in S3", self.quorum_size);

        let start = Instant::now();
        let timeout = Duration::from_secs(QUORUM_WAIT_TIMEOUT_SECS);

        loop {
            if start.elapsed() > timeout {
                return Err(Error::other(format!(
                    "Timeout waiting for {} nodes after {}s",
                    self.quorum_size, QUORUM_WAIT_TIMEOUT_SECS
                )));
            }

            let nodes = self.get_registered_nodes()?;
            info!("Found {} registered nodes", nodes.len());

            if nodes.len() >= self.quorum_size {
                let mut sorted_nodes = nodes;
                sorted_nodes.sort_by_key(|n| n.score);
                return Ok(sorted_nodes);
            }

            std::thread::sleep(Duration::from_secs(POLL_INTERVAL_SECS));
        }
    }

    fn get_registered_nodes(&self) -> Result<Vec<BssNodeInfo>, Error> {
        let s3_prefix = format!(
            "s3://{}/bootstrap/{}/nodes/",
            self.s3_bucket, self.cluster_id
        );

        let output = run_fun!(aws s3 ls $s3_prefix 2>/dev/null).unwrap_or_default();
        if output.trim().is_empty() {
            return Ok(Vec::new());
        }

        let mut nodes = Vec::new();
        for line in output.lines() {
            let parts: Vec<_> = line.split_whitespace().collect();
            if parts.len() >= 4 {
                let filename = parts[3];
                if filename.ends_with(".json") {
                    let s3_path = format!("{}{}", s3_prefix, filename);
                    match run_fun!(aws s3 cp $s3_path - 2>/dev/null) {
                        Ok(content) => {
                            if let Ok(node_info) = serde_json::from_str::<BssNodeInfo>(&content) {
                                nodes.push(node_info);
                            }
                        }
                        Err(_) => continue,
                    }
                }
            }
        }

        Ok(nodes)
    }

    pub fn generate_initial_cluster(&self, nodes: &[BssNodeInfo]) -> String {
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

pub fn get_cluster_state_s3(bucket: &str, cluster_id: &str) -> Result<String, Error> {
    let s3_path = format!(
        "s3://{}/bootstrap/{}/cluster-state.json",
        bucket, cluster_id
    );

    let output = run_fun!(aws s3 cp $s3_path - 2>/dev/null)?;
    if output.trim().is_empty() {
        return Ok("pre-bootstrap".to_string());
    }

    #[derive(Deserialize)]
    struct ClusterState {
        state: String,
    }

    let state: ClusterState = serde_json::from_str(&output)
        .map_err(|e| Error::other(format!("Failed to parse cluster state: {e}")))?;

    Ok(state.state)
}

pub fn set_cluster_state_s3(bucket: &str, cluster_id: &str, state: &str) -> CmdResult {
    let s3_path = format!(
        "s3://{}/bootstrap/{}/cluster-state.json",
        bucket, cluster_id
    );

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let json = format!(r#"{{"state":"{}","timestamp":{}}}"#, state, timestamp);

    info!("Setting cluster state to '{state}' at {s3_path}");
    run_cmd!(echo $json | aws s3 cp - $s3_path --quiet)?;

    Ok(())
}

pub fn get_all_registered_bss_nodes_s3(
    bucket: &str,
    cluster_id: &str,
) -> Result<Vec<BssNodeInfo>, Error> {
    let s3_prefix = format!("s3://{}/bootstrap/{}/nodes/", bucket, cluster_id);

    let output = run_fun!(aws s3 ls $s3_prefix 2>/dev/null).unwrap_or_default();
    if output.trim().is_empty() {
        return Ok(Vec::new());
    }

    let mut nodes = Vec::new();
    for line in output.lines() {
        let parts: Vec<_> = line.split_whitespace().collect();
        if parts.len() >= 4 {
            let filename = parts[3];
            if filename.ends_with(".json") {
                let s3_path = format!("{}{}", s3_prefix, filename);
                match run_fun!(aws s3 cp $s3_path - 2>/dev/null) {
                    Ok(content) => {
                        if let Ok(node_info) = serde_json::from_str::<BssNodeInfo>(&content) {
                            nodes.push(node_info);
                        }
                    }
                    Err(_) => continue,
                }
            }
        }
    }

    nodes.sort_by_key(|n| n.score);
    Ok(nodes)
}
