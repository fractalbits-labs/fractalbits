use std::collections::HashMap;
use std::io::Error;

use chrono::Utc;
use uuid::Uuid;
use xtask_common::{
    BootstrapClusterConfig, ClusterEndpointsConfig, ClusterGcpConfig, ClusterGlobalConfig,
    ClusterResourcesConfig, DataBlobStorage, NodeEntry,
};

pub struct GcpDeployParams<'a> {
    pub outputs: &'a HashMap<String, String>,
    pub project_id: &'a str,
    pub zone: &'a str,
    pub rss_backend: xtask_common::RssBackend,
    pub rss_ha_enabled: bool,
    pub num_bss_nodes: usize,
    pub num_api_servers: usize,
    pub with_bench: bool,
}

/// Generate a BootstrapClusterConfig from Terraform outputs.
///
/// Parses `terraform output -json` into a cluster config suitable for
/// the fractalbits-bootstrap binary on GCP.
#[allow(dead_code)]
pub fn generate_bootstrap_config(
    params: &GcpDeployParams,
) -> Result<BootstrapClusterConfig, Error> {
    let outputs = params.outputs;
    let region = outputs
        .get("region")
        .ok_or_else(|| Error::other("Terraform output 'region' not found"))?
        .clone();

    let workflow_cluster_id = Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let journal_uuid = Uuid::now_v7().to_string();

    // Instance names and IPs from Terraform outputs
    let rss_a_name = get_output(outputs, "rss_a_name")?;
    let rss_a_ip = get_output(outputs, "rss_a_ip")?;
    let nss_a_name = get_output(outputs, "nss_a_name")?;
    let nss_a_ip = get_output(outputs, "nss_a_ip")?;
    let nss_b_name = outputs.get("nss_b_name").filter(|v| !v.is_empty()).cloned();
    let nss_b_ip = outputs.get("nss_b_ip").filter(|v| !v.is_empty()).cloned();

    let rss_b_name = outputs.get("rss_b_name").filter(|v| !v.is_empty()).cloned();
    let rss_b_ip = outputs.get("rss_b_ip").filter(|v| !v.is_empty()).cloned();

    let nss_endpoint = nss_a_ip.clone();

    // Build nodes map
    let mut nodes: HashMap<String, Vec<NodeEntry>> = HashMap::new();

    // RSS nodes
    nodes
        .entry("root_server".to_string())
        .or_default()
        .push(NodeEntry {
            id: rss_a_name.clone(),
            private_ip: Some(rss_a_ip),
            role: Some("leader".to_string()),
            volume_id: None,
            journal_uuid: None,
            bench_client_num: None,
        });
    if let Some(rss_b) = &rss_b_name {
        nodes
            .entry("root_server".to_string())
            .or_default()
            .push(NodeEntry {
                id: rss_b.clone(),
                private_ip: rss_b_ip,
                role: Some("follower".to_string()),
                volume_id: None,
                journal_uuid: None,
                bench_client_num: None,
            });
    }

    // NSS nodes (pd-ssd journal on GCP persistent disk)
    // volume_id uses "gcp:" prefix for GCP device path resolution
    let nss_volume_id = Some("gcp:nss-journal".to_string());
    nodes
        .entry("nss_server".to_string())
        .or_default()
        .push(NodeEntry {
            id: nss_a_name.clone(),
            private_ip: Some(nss_a_ip),
            role: None,
            volume_id: nss_volume_id.clone(),
            journal_uuid: Some(journal_uuid.clone()),
            bench_client_num: None,
        });
    if let (Some(name), Some(ip)) = (&nss_b_name, &nss_b_ip) {
        nodes
            .entry("nss_server".to_string())
            .or_default()
            .push(NodeEntry {
                id: name.clone(),
                private_ip: Some(ip.clone()),
                role: None,
                volume_id: nss_volume_id,
                journal_uuid: Some(journal_uuid),
                bench_client_num: None,
            });
    }

    // BSS nodes from Terraform MIG
    for i in 0..params.num_bss_nodes {
        let name_key = format!("bss_{i}_name");
        let ip_key = format!("bss_{i}_ip");
        if let (Some(name), Some(ip)) = (outputs.get(&name_key), outputs.get(&ip_key)) {
            nodes
                .entry("bss_server".to_string())
                .or_default()
                .push(NodeEntry {
                    id: name.clone(),
                    private_ip: Some(ip.clone()),
                    role: None,
                    volume_id: None,
                    journal_uuid: None,
                    bench_client_num: None,
                });
        }
    }

    // API server nodes from Terraform MIG
    for i in 0..params.num_api_servers {
        let name_key = format!("api_{i}_name");
        let ip_key = format!("api_{i}_ip");
        if let (Some(name), Some(ip)) = (outputs.get(&name_key), outputs.get(&ip_key)) {
            nodes
                .entry("api_server".to_string())
                .or_default()
                .push(NodeEntry {
                    id: name.clone(),
                    private_ip: Some(ip.clone()),
                    role: None,
                    volume_id: None,
                    journal_uuid: None,
                    bench_client_num: None,
                });
        }
    }

    // Bench server
    if let Some(bench_name) = outputs.get("bench_server_name") {
        let bench_ip = get_output(outputs, "bench_server_ip")?;
        nodes
            .entry("bench_server".to_string())
            .or_default()
            .push(NodeEntry {
                id: bench_name.clone(),
                private_ip: Some(bench_ip),
                role: None,
                volume_id: None,
                journal_uuid: None,
                bench_client_num: Some(0),
            });
    }

    let network = outputs
        .get("network_name")
        .cloned()
        .unwrap_or_else(|| "fractalbits-vpc".to_string());
    let subnetwork = outputs
        .get("subnetwork_name")
        .cloned()
        .unwrap_or_else(|| "fractalbits-subnet".to_string());
    let service_account = outputs
        .get("service_account_email")
        .cloned()
        .unwrap_or_default();

    let firestore_database = if params.rss_backend == xtask_common::RssBackend::Firestore {
        outputs.get("firestore_database_id").cloned()
    } else {
        None
    };

    let api_endpoint = outputs.get("api_lb_ip").cloned();

    let gcp_config = ClusterGcpConfig {
        project_id: params.project_id.to_string(),
        zone: params.zone.to_string(),
        remote_zone: None,
        network,
        subnetwork,
        service_account,
        firestore_database,
    };

    let config = BootstrapClusterConfig {
        global: ClusterGlobalConfig {
            deploy_target: xtask_common::DeployTarget::Gcp,
            region,
            for_bench: params.with_bench,
            data_blob_storage: DataBlobStorage::AllInBssSingleAz,
            rss_ha_enabled: params.rss_ha_enabled,
            rss_backend: params.rss_backend,
            journal_type: match outputs.get("journal_type").map(|s| s.as_str()) {
                Some("local_ssd") => xtask_common::JournalType::Nvme,
                _ => xtask_common::JournalType::Ebs, // pd_ssd uses EBS-style journal
            },
            num_bss_nodes: Some(params.num_bss_nodes),
            num_api_servers: Some(params.num_api_servers),
            num_bench_clients: None,
            workflow_cluster_id: Some(workflow_cluster_id),
            meta_stack_testing: false,
            use_generic_binaries: true,
        },
        aws: None,
        gcp: Some(gcp_config),
        endpoints: ClusterEndpointsConfig {
            nss_endpoint,
            api_server_endpoint: api_endpoint,
        },
        resources: Some(ClusterResourcesConfig {
            nss_a_id: nss_a_name,
            nss_b_id: nss_b_name,
            volume_a_id: None,
            volume_b_id: None,
        }),
        etcd: None,
        nodes,
        bootstrap_bucket: format!("{}-deploy-staging", params.project_id),
    };

    Ok(config)
}

#[allow(dead_code)]
fn get_output(outputs: &HashMap<String, String>, key: &str) -> Result<String, Error> {
    outputs
        .get(key)
        .filter(|v| !v.is_empty())
        .cloned()
        .ok_or_else(|| Error::other(format!("Terraform output '{}' not found", key)))
}
