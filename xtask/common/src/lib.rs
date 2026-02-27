use cmd_lib::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tracing::info;
use uuid::Uuid;

pub const BOOTSTRAP_CLUSTER_CONFIG: &str = "bootstrap_cluster.toml";

pub mod workflow_stages;
pub use workflow_stages::{STAGES, StageInfo};

/// AWS credentials + endpoint for DynamoDB Local (used in tests and local development)
pub const LOCAL_DDB_ENVS: &[&str] = &[
    "AWS_DEFAULT_REGION=fakeRegion",
    "AWS_ACCESS_KEY_ID=fakeMyKeyId",
    "AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey",
    "AWS_ENDPOINT_URL_DYNAMODB=http://localhost:8000",
];

/// AWS credentials + endpoint for DynamoDB Local in systemd Environment format
pub const LOCAL_DDB_ENVS_SYSTEMD: &str = r#"
Environment="AWS_DEFAULT_REGION=fakeRegion"
Environment="AWS_ACCESS_KEY_ID=fakeMyKeyId"
Environment="AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey"
Environment="AWS_ENDPOINT_URL_DYNAMODB=http://localhost:8000""#;

#[derive(Clone, Copy, PartialEq, Eq, Default, Debug, clap::ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeployTarget {
    OnPrem,
    #[default]
    Aws,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum JournalType {
    #[default]
    Ebs,
    Nvme,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DataBlobStorage {
    #[default]
    AllInBssSingleAz,
    S3HybridSingleAz,
    S3ExpressMultiAz,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RssBackend {
    Etcd,
    #[default]
    Ddb,
}

/// Node entry within a service type group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeEntry {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_ip: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_uuid: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bench_client_num: Option<usize>,
}

/// Node config with service_type (for iteration/lookup convenience)
#[derive(Debug, Clone)]
pub struct ClusterNodeConfig {
    pub id: String,
    pub service_type: String,
    pub private_ip: Option<String>,
    pub role: Option<String>,
    pub volume_id: Option<String>,
    pub journal_uuid: Option<String>,
    pub bench_client_num: Option<usize>,
}

impl ClusterNodeConfig {
    pub fn from_entry(service_type: &str, entry: &NodeEntry) -> Self {
        Self {
            id: entry.id.clone(),
            service_type: service_type.to_string(),
            private_ip: entry.private_ip.clone(),
            role: entry.role.clone(),
            volume_id: entry.volume_id.clone(),
            journal_uuid: entry.journal_uuid.clone(),
            bench_client_num: entry.bench_client_num,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterGlobalConfig {
    #[serde(default)]
    pub deploy_target: DeployTarget,
    pub region: String,
    pub for_bench: bool,
    pub data_blob_storage: DataBlobStorage,
    pub rss_ha_enabled: bool,
    #[serde(default)]
    pub rss_backend: RssBackend,
    #[serde(default)]
    pub journal_type: JournalType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_bss_nodes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_api_servers: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_bench_clients: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_cluster_id: Option<String>,
    #[serde(default)]
    pub meta_stack_testing: bool,
    #[serde(default)]
    pub use_generic_binaries: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterAwsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_blob_bucket: Option<String>,
    pub local_az: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_az: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterEndpointsConfig {
    pub nss_endpoint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_server_endpoint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterResourcesConfig {
    pub nss_a_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nss_b_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_a_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_b_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterEtcdConfig {
    pub enabled: bool,
    pub cluster_size: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoints: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapClusterConfig {
    pub global: ClusterGlobalConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aws: Option<ClusterAwsConfig>,
    pub endpoints: ClusterEndpointsConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ClusterResourcesConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etcd: Option<ClusterEtcdConfig>,
    #[serde(default)]
    pub nodes: HashMap<String, Vec<NodeEntry>>,
    #[serde(default)]
    pub bootstrap_bucket: String,
}

impl BootstrapClusterConfig {
    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }

    pub fn from_toml(s: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(s)
    }

    pub fn is_etcd_backend(&self) -> bool {
        self.global.rss_backend == RssBackend::Etcd
    }

    pub fn get_bootstrap_bucket(&self) -> String {
        format!("s3://{}", self.bootstrap_bucket)
    }

    /// Get all nodes as a flat list with service_type
    pub fn all_nodes(&self) -> Vec<ClusterNodeConfig> {
        self.nodes
            .iter()
            .flat_map(|(service_type, entries)| {
                entries
                    .iter()
                    .map(|e| ClusterNodeConfig::from_entry(service_type, e))
            })
            .collect()
    }

    /// Get nodes by service type
    pub fn get_nodes(&self, service_type: &str) -> Vec<ClusterNodeConfig> {
        self.nodes
            .get(service_type)
            .map(|entries| {
                entries
                    .iter()
                    .map(|e| ClusterNodeConfig::from_entry(service_type, e))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get node entries by service type (without cloning)
    pub fn get_node_entries(&self, service_type: &str) -> Option<&Vec<NodeEntry>> {
        self.nodes.get(service_type)
    }

    /// Get instance config by ID or private IP (searches all service types)
    pub fn get_instance(&self, id: &str) -> Option<ClusterNodeConfig> {
        for (service_type, entries) in &self.nodes {
            // First try exact ID match
            if let Some(entry) = entries.iter().find(|e| e.id == id) {
                return Some(ClusterNodeConfig::from_entry(service_type, entry));
            }
            // Then try matching by private_ip
            if let Some(entry) = entries.iter().find(|e| e.private_ip.as_deref() == Some(id)) {
                return Some(ClusterNodeConfig::from_entry(service_type, entry));
            }
        }
        None
    }

    /// Check if instance exists in any service type (by ID or private_ip)
    pub fn contains_instance(&self, id: &str) -> bool {
        self.nodes.values().any(|entries| {
            entries
                .iter()
                .any(|e| e.id == id || e.private_ip.as_deref() == Some(id))
        })
    }

    pub fn get_resources(&self) -> ClusterResourcesConfig {
        self.resources.clone().unwrap_or_default()
    }

    /// Add a node to a service type group
    pub fn add_node(&mut self, service_type: &str, entry: NodeEntry) {
        self.nodes
            .entry(service_type.to_string())
            .or_default()
            .push(entry);
    }
}

// Support GenUuids only for now
pub fn gen_uuids(num: usize, file: &str) -> CmdResult {
    info!("Generating {num} uuids into file {file}");
    let num_threads = num_cpus::get();
    let num_uuids = num / num_threads;
    let last_num_uuids = num - num_uuids * (num_threads - 1);

    let uuids = Arc::new(Mutex::new(Vec::new()));
    (0..num_threads).into_par_iter().for_each(|i| {
        let mut uuids_str = String::new();
        let n = if i == num_threads - 1 {
            last_num_uuids
        } else {
            num_uuids
        };
        for _ in 0..n {
            uuids_str += &Uuid::now_v7().to_string();
            uuids_str += "\n";
        }
        uuids.lock().unwrap().push(uuids_str);
    });

    let dir = run_fun!(dirname $file)?;
    run_cmd! {
        mkdir -p $dir;
        echo -n > $file;
    }?;
    for uuid in uuids.lock().unwrap().iter() {
        run_cmd!(echo -n $uuid >> $file)?;
    }
    info!("File {file} is ready");
    Ok(())
}

pub fn dump_vg_config(localdev: bool) -> CmdResult {
    // AWS cli environment variables based on localdev flag
    let env_vars: &[&str] = if localdev { LOCAL_DDB_ENVS } else { &[] };

    // Query BSS data volume group configuration
    let data_vg_result = run_fun! {
        $[env_vars]
        aws dynamodb get-item
            --table-name "fractalbits-service-discovery"
            --key "{\"service_id\": {\"S\": \"bss-data-vg-config\"}}"
            --query "Item.value.S"
            --output text
    };

    // Query BSS metadata volume group configuration
    let metadata_vg_result = run_fun! {
        $[env_vars]
        aws dynamodb get-item
            --table-name "fractalbits-service-discovery"
            --key "{\"service_id\": {\"S\": \"bss-metadata-vg-config\"}}"
            --query "Item.value.S"
            --output text
    };

    // JSON output - output raw JSON strings that can be used as environment variables
    let mut output = serde_json::Map::new();

    // Add data VG config if available
    if let Ok(json_str) = data_vg_result
        && !json_str.trim().is_empty()
        && json_str.trim() != "None"
    {
        match serde_json::from_str::<serde_json::Value>(&json_str) {
            Ok(json_value) => {
                output.insert("data_vg_config".to_string(), json_value);
            }
            Err(e) => {
                error!("Failed to parse data VG config JSON: {}", e);
            }
        }
    }

    // Add metadata VG config if available
    if let Ok(json_str) = metadata_vg_result
        && !json_str.trim().is_empty()
        && json_str.trim() != "None"
    {
        match serde_json::from_str::<serde_json::Value>(&json_str) {
            Ok(json_value) => {
                output.insert("metadata_vg_config".to_string(), json_value);
            }
            Err(e) => {
                error!("Failed to parse metadata VG config JSON: {}", e);
            }
        }
    }

    // Output the combined JSON
    let combined_json = serde_json::Value::Object(output);
    match serde_json::to_string(&combined_json) {
        Ok(json_string) => println!("{}", json_string),
        Err(e) => error!("Failed to serialize combined JSON: {}", e),
    }

    Ok(())
}

/// Check if a TCP port is ready for connections
pub fn check_port_ready(port: u16) -> bool {
    TcpStream::connect_timeout(
        &format!("127.0.0.1:{}", port).parse().unwrap(),
        Duration::from_secs(1),
    )
    .is_ok()
}

/// Create directories for BSS server
pub fn create_bss_dirs(data_dir: &Path, bss_id: u32, bss_count: u32) -> CmdResult {
    info!("Creating directories for bss{} server", bss_id);

    let bss_dir = data_dir.join(format!("bss{}", bss_id));
    fs::create_dir_all(bss_dir.join("local/stats"))?;
    fs::create_dir_all(bss_dir.join("local/blobs"))?;

    // Data volume: EC-only for 6-node, replicated otherwise
    if bss_count == 6 {
        // EC volume: all 6 nodes get data_volume32768 (0x8000)
        let ec_volume_id: u16 = 0x8000;
        let data_vol_dir = bss_dir.join(format!("local/blobs/data_volume{}", ec_volume_id));
        fs::create_dir_all(&data_vol_dir)?;
        for i in 0..256 {
            fs::create_dir_all(data_vol_dir.join(format!("{}", i)))?;
        }
    } else {
        let data_volume_id = match bss_count {
            1 => 1,
            _ => (bss_id / 3) + 1, // DATA_VG_QUORUM_N = 3
        };
        let data_vol_dir = bss_dir.join(format!("local/blobs/data_volume{}", data_volume_id));
        fs::create_dir_all(&data_vol_dir)?;
        for i in 0..256 {
            fs::create_dir_all(data_vol_dir.join(format!("{}", i)))?;
        }
    }

    // Metadata volume
    let metadata_volume_id = match bss_count {
        1 => 1,
        _ => (bss_id / 6) + 1, // METADATA_VG_QUORUM_N = 6
    };
    let meta_vol_dir = bss_dir.join(format!("local/blobs/metadata_volume{}", metadata_volume_id));
    fs::create_dir_all(&meta_vol_dir)?;
    for i in 0..256 {
        fs::create_dir_all(meta_vol_dir.join(format!("{}", i)))?;
    }

    Ok(())
}

/// Create directories for NSS server
/// - is_ebs_journal: true for EBS journal, false for NVMe journal
/// - For EBS: journal at data/ebs/<journal_uuid>/
/// - For NVMe: journal at data/<dir_name>/local/journal/<journal_uuid>/
pub fn create_nss_dirs(
    data_dir: &Path,
    dir_name: &str,
    is_ebs_journal: bool,
    journal_uuid: Option<&str>,
) -> CmdResult {
    info!("Creating directories for {} server", dir_name);

    let nss_dir = data_dir.join(dir_name);

    // Always create local/journal directory (needed for fbs.state and unit tests)
    fs::create_dir_all(nss_dir.join("local/journal"))?;

    // Create journal directory based on journal type
    if is_ebs_journal {
        if let Some(uuid) = journal_uuid {
            let ebs_journal_dir = data_dir.join("ebs").join(uuid);
            fs::create_dir_all(&ebs_journal_dir)?;
            info!("Created EBS journal directory: {:?}", ebs_journal_dir);
        }
    } else {
        // NVMe journal
        if let Some(uuid) = journal_uuid {
            let local_journal_dir = nss_dir.join("local/journal").join(uuid);
            fs::create_dir_all(&local_journal_dir)?;
            info!("Created NVMe journal directory: {:?}", local_journal_dir);
        }
    }

    fs::create_dir_all(nss_dir.join("local/stats"))?;
    fs::create_dir_all(nss_dir.join("local/meta_cache/blobs"))?;

    for i in 0..256 {
        fs::create_dir_all(nss_dir.join(format!("local/meta_cache/blobs/{}", i)))?;
    }

    Ok(())
}

/// Generate data volume group configuration JSON (unified format with per-volume mode)
fn generate_data_vg_replicated_config(bss_count: u32, n: u32, r: u32, w: u32) -> String {
    let num_volumes = bss_count / n;
    let mut volumes = Vec::new();

    for vol_idx in 0..num_volumes {
        let start_idx = vol_idx * n;
        let end_idx = start_idx + n;

        let nodes: Vec<String> = (start_idx..end_idx)
            .map(|i| {
                format!(
                    r#"{{"node_id":"bss{i}","ip":"127.0.0.1","port":{}}}"#,
                    8088 + i
                )
            })
            .collect();

        volumes.push(format!(
            r#"{{"volume_id":{},"bss_nodes":[{}],"mode":{{"type":"replicated","n":{n},"r":{r},"w":{w}}}}}"#,
            vol_idx + 1,
            nodes.join(",")
        ));
    }

    format!(r#"{{"volumes":[{}]}}"#, volumes.join(","))
}

/// Generate EC-only data volume group config for 6-node cluster
fn generate_ec_volume_group_config(bss_count: u32) -> String {
    let ec_volume_id: u16 = 0x8000; // Volume::EC_VOLUME_ID_BASE
    let data_shards: u32 = 4;
    let parity_shards: u32 = 2;
    let total_shards = data_shards + parity_shards;

    let nodes: Vec<String> = (0..total_shards)
        .map(|i| {
            format!(
                r#"{{"node_id":"bss{i}","ip":"127.0.0.1","port":{}}}"#,
                8088 + i
            )
        })
        .collect();

    assert_eq!(bss_count, total_shards);

    format!(
        r#"{{"volumes":[{{"volume_id":{},"bss_nodes":[{}],"mode":{{"type":"erasure_coded","data_shards":{},"parity_shards":{}}}}}]}}"#,
        ec_volume_id,
        nodes.join(","),
        data_shards,
        parity_shards,
    )
}

/// Generate BSS data volume group config for given bss_count
pub fn generate_bss_data_vg_config(bss_count: u32) -> String {
    match bss_count {
        1 => generate_data_vg_replicated_config(1, 1, 1, 1),
        6 => generate_ec_volume_group_config(6),
        _ => generate_data_vg_replicated_config(1, 1, 1, 1),
    }
}

/// Generate metadata volume group configuration JSON (old format with top-level quorum,
/// consumed by the Zig NSS server)
pub fn generate_metadata_vg_config(bss_count: u32, n: u32, r: u32, w: u32) -> String {
    let num_volumes = bss_count / n;
    let mut volumes = Vec::new();

    for vol_idx in 0..num_volumes {
        let start_idx = vol_idx * n;
        let end_idx = start_idx + n;

        let nodes: Vec<String> = (start_idx..end_idx)
            .map(|i| {
                format!(
                    r#"{{"node_id":"bss{i}","ip":"127.0.0.1","port":{}}}"#,
                    8088 + i
                )
            })
            .collect();

        volumes.push(format!(
            r#"{{"volume_id":{},"bss_nodes":[{}]}}"#,
            vol_idx + 1,
            nodes.join(",")
        ));
    }

    format!(
        r#"{{"volumes":[{}],"quorum":{{"n":{n},"r":{r},"w":{w}}}}}"#,
        volumes.join(","),
    )
}

/// Generate BSS metadata volume group config for given bss_count
pub fn generate_bss_metadata_vg_config(bss_count: u32) -> String {
    const METADATA_VG_QUORUM_N: u32 = 6;
    const METADATA_VG_QUORUM_R: u32 = 4;
    const METADATA_VG_QUORUM_W: u32 = 4;

    match bss_count {
        1 => generate_metadata_vg_config(1, 1, 1, 1),
        6 => generate_metadata_vg_config(
            6,
            METADATA_VG_QUORUM_N,
            METADATA_VG_QUORUM_R,
            METADATA_VG_QUORUM_W,
        ),
        _ => generate_metadata_vg_config(1, 1, 1, 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ec_config_for_6_nodes() {
        let config = generate_bss_data_vg_config(6);
        let parsed: serde_json::Value = serde_json::from_str(&config).unwrap();

        let volumes = parsed["volumes"].as_array().unwrap();
        assert_eq!(volumes.len(), 1);

        let ec = &volumes[0];
        assert_eq!(ec["volume_id"].as_u64().unwrap(), 0x8000);
        assert_eq!(ec["bss_nodes"].as_array().unwrap().len(), 6);

        let mode = &ec["mode"];
        assert_eq!(mode["type"].as_str().unwrap(), "erasure_coded");
        assert_eq!(mode["data_shards"].as_u64().unwrap(), 4);
        assert_eq!(mode["parity_shards"].as_u64().unwrap(), 2);
    }

    #[test]
    fn replicated_config_for_1_node() {
        let config = generate_bss_data_vg_config(1);
        let parsed: serde_json::Value = serde_json::from_str(&config).unwrap();

        let volumes = parsed["volumes"].as_array().unwrap();
        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0]["volume_id"].as_u64().unwrap(), 1);

        let mode = &volumes[0]["mode"];
        assert_eq!(mode["type"].as_str().unwrap(), "replicated");
        assert_eq!(mode["n"].as_u64().unwrap(), 1);
        assert_eq!(mode["r"].as_u64().unwrap(), 1);
        assert_eq!(mode["w"].as_u64().unwrap(), 1);
    }

    #[test]
    fn ec_config_nodes_have_correct_ports() {
        let config = generate_bss_data_vg_config(6);
        let parsed: serde_json::Value = serde_json::from_str(&config).unwrap();
        let nodes = parsed["volumes"][0]["bss_nodes"].as_array().unwrap();

        for (i, node) in nodes.iter().enumerate() {
            assert_eq!(node["node_id"].as_str().unwrap(), format!("bss{}", i));
            assert_eq!(node["ip"].as_str().unwrap(), "127.0.0.1");
            assert_eq!(node["port"].as_u64().unwrap(), 8088 + i as u64);
        }
    }

    #[test]
    fn metadata_config_unchanged_for_6_nodes() {
        let config = generate_bss_metadata_vg_config(6);
        let parsed: serde_json::Value = serde_json::from_str(&config).unwrap();

        let volumes = parsed["volumes"].as_array().unwrap();
        assert_eq!(volumes.len(), 1);
        assert_eq!(volumes[0]["bss_nodes"].as_array().unwrap().len(), 6);

        let quorum = &parsed["quorum"];
        assert_eq!(quorum["n"].as_u64().unwrap(), 6);
        assert_eq!(quorum["r"].as_u64().unwrap(), 4);
        assert_eq!(quorum["w"].as_u64().unwrap(), 4);
    }
}
