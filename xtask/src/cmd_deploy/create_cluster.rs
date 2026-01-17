use crate::CmdResult;
use cmd_lib::*;
use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Error;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use uuid::Uuid;
use xtask_common::{
    BOOTSTRAP_CLUSTER_CONFIG, BootstrapClusterConfig, ClusterEndpointsConfig, ClusterEtcdConfig,
    ClusterGlobalConfig, ClusterResourcesConfig, DataBlobStorage, DeployTarget, JournalType,
    NodeEntry, RssBackend,
};

const SSH_TUNNEL_LOCAL_PORT: u16 = 8080;

fn parse_ssh_config_for_instance_ids(
    ssh_config_path: &str,
) -> Result<HashMap<String, String>, Error> {
    let content = std::fs::read_to_string(ssh_config_path).map_err(|e| {
        Error::other(format!(
            "Failed to read SSH config from {}: {}",
            ssh_config_path, e
        ))
    })?;

    let mut ip_to_instance: HashMap<String, String> = HashMap::new();
    let host_re = Regex::new(r"Host\s+(\d+\.\d+\.\d+\.\d+)").unwrap();
    let proxy_re = Regex::new(r"--target\s+(i-[a-f0-9]+)").unwrap();

    let mut current_ip: Option<String> = None;
    for line in content.lines() {
        if let Some(caps) = host_re.captures(line) {
            current_ip = Some(caps[1].to_string());
        } else if let Some(caps) = proxy_re.captures(line)
            && let Some(ip) = current_ip.take()
        {
            ip_to_instance.insert(ip, caps[1].to_string());
        }
    }

    Ok(ip_to_instance)
}

fn push_ssh_key(instance_id: &str) -> CmdResult {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/home/ec2-user".to_string());
    let pub_key_arg = format!("file://{home}/.ssh/id_ed25519.pub");

    run_cmd!(
        aws ec2-instance-connect send-ssh-public-key
            --instance-id $instance_id
            --instance-os-user ec2-user
            --ssh-public-key $pub_key_arg
    )
    .map_err(|e| Error::other(format!("Failed to push SSH key to {}: {}", instance_id, e)))?;

    Ok(())
}

fn start_ssh_tunnel(
    ssh_config_path: &str,
    rss_ip: &str,
    instance_id: &str,
) -> Result<Child, Error> {
    push_ssh_key(instance_id)?;

    let tunnel_spec = format!("{}:localhost:8080", SSH_TUNNEL_LOCAL_PORT);
    let child = Command::new("ssh")
        .args([
            "-F",
            ssh_config_path,
            "-N",
            "-L",
            &tunnel_spec,
            "-o",
            "ExitOnForwardFailure=yes",
            rss_ip,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| Error::other(format!("Failed to start SSH tunnel: {}", e)))?;

    std::thread::sleep(Duration::from_secs(3));

    let port = SSH_TUNNEL_LOCAL_PORT;
    let health_check = run_fun!(
        AWS_DEFAULT_REGION=localdev
        AWS_ACCESS_KEY_ID=test_api_key
        AWS_SECRET_ACCESS_KEY=test_api_secret
        aws s3 --endpoint-url "http://localhost:$port" ls 2>&1
    );

    if health_check.is_err() {
        return Err(Error::other(
            "SSH tunnel started but S3 endpoint health check failed. \
             Verify the bootstrap container is running on RSS.",
        ));
    }

    Ok(child)
}

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

        let api_server_endpoint = self
            .endpoints
            .as_ref()
            .and_then(|e| e.api_server_endpoint.clone())
            .or_else(|| {
                self.nodes
                    .get("api_server")
                    .and_then(|nodes| nodes.first())
                    .map(|n| n.ip.clone())
            });

        let endpoints = ClusterEndpointsConfig {
            nss_endpoint,
            api_server_endpoint,
        };

        // On-prem always uses etcd
        let etcd = Some(ClusterEtcdConfig {
            enabled: true,
            cluster_size: self.global.num_bss_nodes,
            endpoints: None,
        });

        // Generate a shared journal UUID for NSS nodes (used by both nss_server and mirrord)
        let shared_journal_uuid = Uuid::new_v4().to_string();

        // Extract NSS node IDs based on role for resources config
        let nss_nodes = self.nodes.get("nss_server");
        let nss_a_id = nss_nodes
            .and_then(|nodes| {
                nodes
                    .iter()
                    .find(|n| n.role.as_deref() == Some("active"))
                    .or_else(|| nodes.first())
            })
            .map(|n| n.hostname.clone().unwrap_or_else(|| n.ip.clone()));
        let nss_b_id = nss_nodes.and_then(|nodes| {
            nodes
                .iter()
                .find(|n| n.role.as_deref() == Some("standby"))
                .map(|n| n.hostname.clone().unwrap_or_else(|| n.ip.clone()))
        });

        // Build resources config if we have NSS nodes
        let resources = nss_a_id.map(|nss_a| ClusterResourcesConfig {
            nss_a_id: nss_a,
            nss_b_id,
            volume_a_id: None,
            volume_b_id: None,
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
                        // Assign shared journal UUID to NSS nodes for NVMe journal coordination
                        journal_uuid: if service_type == "nss_server" {
                            Some(shared_journal_uuid.clone())
                        } else {
                            None
                        },
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
            resources,
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
    bootstrap_s3_url: Option<&str>,
    watch_bootstrap: bool,
    ssh_config: Option<&str>,
) -> CmdResult {
    let config = InputClusterConfig::from_file(cluster_config_path)?;

    // Extract RSS IP from cluster config for remote bootstrap commands.
    let rss_ip = config
        .nodes
        .get("root_server")
        .and_then(|nodes| nodes.first())
        .map(|n| &n.ip)
        .ok_or_else(|| Error::other("No root_server found in cluster config"))?;
    let remote_s3_url = format!("{}:8080", rss_ip);

    // Parse SSH config for IP->instance mapping if provided
    let ip_to_instance = if let Some(config_path) = ssh_config {
        parse_ssh_config_for_instance_ids(config_path)?
    } else {
        HashMap::new()
    };

    // Determine bootstrap S3 URL and optionally start tunnel
    let mut tunnel_child: Option<Child> = None;
    let local_s3_url: String = if let Some(url) = bootstrap_s3_url {
        url.to_string()
    } else if let Some(ssh_config_path) = ssh_config {
        // Auto-establish tunnel when ssh_config is provided
        let rss_instance_id = ip_to_instance
            .get(rss_ip)
            .ok_or_else(|| Error::other("RSS IP not found in SSH config"))?;

        info!("Establishing SSH tunnel to RSS for uploads...");
        let child = start_ssh_tunnel(ssh_config_path, rss_ip, rss_instance_id)?;
        tunnel_child = Some(child);
        info!(
            "SSH tunnel established: localhost:{} -> {}:8080",
            SSH_TUNNEL_LOCAL_PORT, rss_ip
        );

        format!("localhost:{}", SSH_TUNNEL_LOCAL_PORT)
    } else {
        return Err(Error::other(
            "Either --bootstrap-s3-url or --ssh-config must be provided",
        ));
    };

    let total_nodes: usize = config.nodes.values().map(|v| v.len()).sum();
    info!(
        "Creating cluster with {} nodes, local S3 URL: {}, remote S3 URL: {}",
        total_nodes, local_s3_url, remote_s3_url
    );

    // Note: Binaries are pre-populated in the Docker image via 'deploy build --for-on-prem'.
    // We only need to upload the cluster config file.

    let bootstrap_toml = config.to_bootstrap_cluster_toml()?;
    info!(
        "Generated {}:\n{}",
        BOOTSTRAP_CLUSTER_CONFIG, bootstrap_toml
    );

    info!("Uploading {} to S3...", BOOTSTRAP_CLUSTER_CONFIG);
    let s3_key = format!("s3://fractalbits-bootstrap/{}", BOOTSTRAP_CLUSTER_CONFIG);
    let s3_endpoint_url = format!("http://{}", local_s3_url);
    run_cmd!(
        echo $bootstrap_toml |
            AWS_DEFAULT_REGION=localdev
            AWS_ENDPOINT_URL_S3=$s3_endpoint_url
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

    // Kill tunnel after uploads are done - nodes access RSS directly
    if let Some(mut child) = tunnel_child {
        info!("Closing SSH tunnel (nodes will access RSS directly)...");
        let _ = child.kill();
        let _ = child.wait();
    }

    // Bootstrap all nodes in parallel - the workflow stages act as barriers to coordinate
    let mut handles = Vec::new();
    for (service_type, nodes) in &config.nodes {
        for node in nodes {
            let node_ip = node.ip.clone();
            let service = service_type.clone();
            let remote_url = remote_s3_url.clone();
            let ssh_config_path = ssh_config.map(|s| s.to_string());
            let instance_id = ip_to_instance.get(&node_ip).cloned();

            info!(
                "Starting bootstrap for node {} (service: {})",
                node_ip, service
            );

            // Push SSH key before spawning thread
            if let Some(ref id) = instance_id {
                push_ssh_key(id)?;
            }

            let handle = std::thread::spawn(move || -> Result<(), String> {
                let bootstrap_cmd = format!(
                    "export AWS_DEFAULT_REGION=localdev && \
                     export AWS_ENDPOINT_URL_S3=http://{} && \
                     export AWS_ACCESS_KEY_ID=test_api_key && \
                     export AWS_SECRET_ACCESS_KEY=test_api_secret && \
                     aws s3 cp --no-progress s3://fractalbits-bootstrap/bootstrap.sh - | sudo -E sh",
                    remote_url
                );

                let result = if let Some(config_path) = ssh_config_path {
                    std::process::Command::new("ssh")
                        .args(["-F", &config_path, &node_ip, &bootstrap_cmd])
                        .status()
                } else {
                    std::process::Command::new("ssh")
                        .args([&node_ip, &bootstrap_cmd])
                        .status()
                };

                match result {
                    Ok(status) if status.success() => Ok(()),
                    Ok(status) => Err(format!(
                        "Bootstrap failed for {} with exit code {:?}",
                        node_ip,
                        status.code()
                    )),
                    Err(e) => Err(format!("Failed to bootstrap {}: {}", node_ip, e)),
                }
            });

            handles.push((node.ip.clone(), service_type.clone(), handle));
        }
    }

    // Wait for all bootstrap threads to complete
    let mut errors = Vec::new();
    for (node_ip, service, handle) in handles {
        match handle.join() {
            Ok(Ok(())) => info!("Node {} ({}) bootstrapped successfully", node_ip, service),
            Ok(Err(e)) => errors.push(e),
            Err(_) => errors.push(format!("Bootstrap thread panicked for {}", node_ip)),
        }
    }

    if !errors.is_empty() {
        return Err(Error::other(format!(
            "Bootstrap failed for some nodes:\n{}",
            errors.join("\n")
        )));
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
