use crate::CmdResult;
use cmd_lib::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Error;
use std::time::{Duration, Instant};

use super::upload::get_bootstrap_bucket_name;
use xtask_common::DeployTarget;

const CDK_OUTPUTS_PATH: &str = "/tmp/cdk-outputs.json";
const SSM_POLL_INTERVAL_SECS: u64 = 5;
const SSM_TIMEOUT_SECS: u64 = 600;
const SSM_AGENT_READY_TIMEOUT_SECS: u64 = 300;

#[derive(Debug, Deserialize)]
struct CdkOutputs {
    #[serde(rename = "FractalbitsVpcStack")]
    stack: HashMap<String, String>,
}

pub fn setup_on_prem_simulation() -> CmdResult {
    info!("Setting up on-prem simulation...");

    // 1. Parse CDK outputs
    let outputs = parse_cdk_outputs()?;

    // 2. Get instance IDs and wait for SSM readiness
    let rss_a_id = outputs
        .get("rssAId")
        .ok_or_else(|| Error::other("rss-A instance ID not found"))?;
    let nss_a_id = outputs
        .get("nssAId")
        .ok_or_else(|| Error::other("nss-A instance ID not found"))?;
    // nss-B is created for NVMe journal type (active/standby mode)
    let nss_b_id = outputs.get("nssBId");

    let api_asg = outputs.get("apiServerAsgName");
    let bss_asg = outputs.get("bssAsgName");
    let bench_client_asg = outputs.get("benchClientAsgName");
    let bench_server_id = outputs.get("benchserverId");

    // Collect all instance IDs
    let mut all_ids = vec![rss_a_id.clone(), nss_a_id.clone()];
    if let Some(nss_b) = nss_b_id {
        all_ids.push(nss_b.clone());
    }
    let mut api_instance_ids = Vec::new();
    let mut bss_instance_ids = Vec::new();
    let mut bench_client_ids = Vec::new();

    if let Some(asg) = api_asg
        && !asg.is_empty()
    {
        api_instance_ids = get_asg_instance_ids(asg)?;
        all_ids.extend(api_instance_ids.clone());
    }
    if let Some(asg) = bss_asg
        && !asg.is_empty()
    {
        bss_instance_ids = get_asg_instance_ids(asg)?;
        all_ids.extend(bss_instance_ids.clone());
    }
    if let Some(asg) = bench_client_asg
        && !asg.is_empty()
    {
        bench_client_ids = get_asg_instance_ids(asg)?;
        all_ids.extend(bench_client_ids.clone());
    }
    if let Some(id) = bench_server_id {
        all_ids.push(id.clone());
    }

    // 3. Wait for SSM agent
    wait_for_ssm_agent_ready(&all_ids)?;

    // 4. Get private IPs and hostnames for all instances
    let rss = NodeInfo {
        ip: get_instance_private_ip(rss_a_id)?,
        hostname: get_instance_hostname(rss_a_id)?,
    };
    let nss_a = NodeInfo {
        ip: get_instance_private_ip(nss_a_id)?,
        hostname: get_instance_hostname(nss_a_id)?,
    };
    let nss_b_id = nss_b_id.ok_or_else(|| {
        Error::other("nss-B instance ID not found - required for simulate-on-prem")
    })?;
    let nss_b = NodeInfo {
        ip: get_instance_private_ip(nss_b_id)?,
        hostname: get_instance_hostname(nss_b_id)?,
    };
    let api_nodes: Vec<NodeInfo> = api_instance_ids
        .iter()
        .map(|id| {
            Ok(NodeInfo {
                ip: get_instance_private_ip(id)?,
                hostname: get_instance_hostname(id)?,
            })
        })
        .collect::<Result<_, Error>>()?;
    let bss_nodes: Vec<NodeInfo> = bss_instance_ids
        .iter()
        .map(|id| {
            Ok(NodeInfo {
                ip: get_instance_private_ip(id)?,
                hostname: get_instance_hostname(id)?,
            })
        })
        .collect::<Result<_, Error>>()?;
    let bench_client_nodes: Vec<NodeInfo> = bench_client_ids
        .iter()
        .map(|id| {
            Ok(NodeInfo {
                ip: get_instance_private_ip(id)?,
                hostname: get_instance_hostname(id)?,
            })
        })
        .collect::<Result<_, Error>>()?;
    let bench_server_node: Option<NodeInfo> = bench_server_id
        .map(|id| -> Result<NodeInfo, Error> {
            Ok(NodeInfo {
                ip: get_instance_private_ip(id)?,
                hostname: get_instance_hostname(id)?,
            })
        })
        .transpose()?;

    // 5. Set up Docker bootstrap service on rss-A
    let aws_bucket = get_bootstrap_bucket_name(DeployTarget::Aws)?;
    setup_docker_bootstrap_service(rss_a_id, &aws_bucket)?;

    // 6. Generate cluster.toml
    let cluster_toml = generate_cluster_toml(
        &rss,
        &nss_a,
        &nss_b,
        &bss_nodes,
        &api_nodes,
        &bench_client_nodes,
        bench_server_node.as_ref(),
    );
    let toml_path = save_cluster_toml(&cluster_toml)?;

    // 7. Build instance ID to IP mapping for SSH config
    let mut instance_ip_map = vec![
        (rss_a_id.clone(), rss.ip.clone()),
        (nss_a_id.clone(), nss_a.ip.clone()),
        (nss_b_id.clone(), nss_b.ip.clone()),
    ];
    for (id, node) in api_instance_ids.iter().zip(api_nodes.iter()) {
        instance_ip_map.push((id.clone(), node.ip.clone()));
    }
    for (id, node) in bss_instance_ids.iter().zip(bss_nodes.iter()) {
        instance_ip_map.push((id.clone(), node.ip.clone()));
    }
    for (id, node) in bench_client_ids.iter().zip(bench_client_nodes.iter()) {
        instance_ip_map.push((id.clone(), node.ip.clone()));
    }
    if let (Some(id), Some(node)) = (bench_server_id, &bench_server_node) {
        instance_ip_map.push((id.clone(), node.ip.clone()));
    }

    // 8. Generate and save SSH config
    let ssh_config = generate_ssh_config(&instance_ip_map);
    let ssh_config_path = save_ssh_config(&ssh_config)?;

    // 9. Print instructions
    info!("Simulate-on-prem setup complete!");
    info!("Generated cluster.toml at: {}", toml_path);
    info!("Generated SSH config at: {}", ssh_config_path);
    info!("");
    info!("To complete cluster setup, run:");
    println!("  cargo xtask deploy create-cluster \\");
    println!("    --config {} \\", toml_path);
    println!("    --ssh-config {}", ssh_config_path);

    Ok(())
}

fn setup_docker_bootstrap_service(rss_a_id: &str, aws_bucket: &str) -> CmdResult {
    // Create setup script that loads pre-built Docker image from S3
    let setup_script = format!(
        r#"#!/bin/bash
set -ex

# Install and start Docker
sudo yum install -y docker
sudo systemctl start docker
sudo systemctl enable docker

# Download and load pre-built Docker image from S3
aws s3 cp --no-progress s3://{aws_bucket}/docker/fractalbits-image.tar.gz /tmp/
sudo docker load < <(gunzip -c /tmp/fractalbits-image.tar.gz)
rm /tmp/fractalbits-image.tar.gz

# Run the fractalbits bootstrap container
sudo docker run -d --privileged --name fractalbits-bootstrap \
    -p 8080:8080 -p 18080:18080 \
    -v fractalbits-data:/data \
    fractalbits:latest

# Wait for container to be ready
for i in {{1..60}}; do
    if curl -sf --max-time 5 http://localhost:18080/mgmt/health; then
        echo ""
        echo "Docker bootstrap container is ready"
        exit 0
    fi
    echo "Health check attempt $i/60..."
    sleep 5
done
echo "Timed out waiting for Docker container"
sudo docker logs fractalbits-bootstrap 2>&1 | tail -50
exit 1
"#
    );

    info!("Uploading Docker setup script to S3...");
    run_cmd!(echo $setup_script | aws s3 cp - "s3://$aws_bucket/simulate-on-prem-docker-setup.sh")?;

    info!("Setting up Docker bootstrap service on rss-A...");
    let ssm_command = format!(
        "aws s3 cp --no-progress s3://{}/simulate-on-prem-docker-setup.sh - | bash 2>&1",
        aws_bucket
    );
    ssm_run_command(rss_a_id, &ssm_command, "Setup Docker bootstrap service")?;
    Ok(())
}

struct NodeInfo {
    ip: String,
    hostname: String,
}

fn generate_cluster_toml(
    rss: &NodeInfo,
    nss_a: &NodeInfo,
    nss_b: &NodeInfo,
    bss_nodes: &[NodeInfo],
    api_nodes: &[NodeInfo],
    bench_client_nodes: &[NodeInfo],
    bench_server_node: Option<&NodeInfo>,
) -> String {
    let for_bench = !bench_client_nodes.is_empty();
    let num_bench_clients = bench_client_nodes.len();

    let mut toml = format!(
        r#"[global]
num_bss_nodes = {}
for_bench = {}
num_bench_clients = {}

[[nodes.root_server]]
ip = "{}"
hostname = "{}"
role = "leader"

[[nodes.nss_server]]
ip = "{}"
hostname = "{}"
role = "active"

[[nodes.nss_server]]
ip = "{}"
hostname = "{}"
role = "standby"
"#,
        bss_nodes.len(),
        for_bench,
        num_bench_clients,
        rss.ip,
        rss.hostname,
        nss_a.ip,
        nss_a.hostname,
        nss_b.ip,
        nss_b.hostname
    );

    for node in bss_nodes {
        toml.push_str(&format!(
            r#"
[[nodes.bss_server]]
ip = "{}"
hostname = "{}"
"#,
            node.ip, node.hostname
        ));
    }

    for node in api_nodes {
        toml.push_str(&format!(
            r#"
[[nodes.api_server]]
ip = "{}"
hostname = "{}"
"#,
            node.ip, node.hostname
        ));
    }

    for (i, node) in bench_client_nodes.iter().enumerate() {
        toml.push_str(&format!(
            r#"
[[nodes.bench_client]]
ip = "{}"
hostname = "{}"
bench_client_num = {}
"#,
            node.ip, node.hostname, i
        ));
    }

    // Add bench_server if we have a dedicated bench_server node
    if let Some(bench_server) = bench_server_node {
        toml.push_str(&format!(
            r#"
[[nodes.bench_server]]
ip = "{}"
hostname = "{}"
bench_client_num = {}
"#,
            bench_server.ip,
            bench_server.hostname,
            bench_client_nodes.len()
        ));
    }

    toml
}

fn parse_cdk_outputs() -> Result<HashMap<String, String>, Error> {
    let content = std::fs::read_to_string(CDK_OUTPUTS_PATH).map_err(|e| {
        Error::other(format!(
            "Failed to read CDK outputs from {}: {}",
            CDK_OUTPUTS_PATH, e
        ))
    })?;

    let outputs: CdkOutputs = serde_json::from_str(&content).map_err(|e| {
        Error::other(format!(
            "Failed to parse CDK outputs from {}: {}",
            CDK_OUTPUTS_PATH, e
        ))
    })?;

    Ok(outputs.stack)
}

fn get_asg_instance_ids(asg_name: &str) -> Result<Vec<String>, Error> {
    let output = run_fun!(
        aws autoscaling describe-auto-scaling-groups
            --auto-scaling-group-names $asg_name
            --query "AutoScalingGroups[0].Instances[?LifecycleState=='InService'].InstanceId"
            --output text
    )
    .map_err(|e| {
        Error::other(format!(
            "Failed to get ASG instances for {}: {}",
            asg_name, e
        ))
    })?;

    Ok(output.split_whitespace().map(String::from).collect())
}

fn wait_for_ssm_agent_ready(instance_ids: &[String]) -> Result<(), Error> {
    let start_time = Instant::now();
    let timeout = Duration::from_secs(SSM_AGENT_READY_TIMEOUT_SECS);

    info!(
        "Waiting for SSM agent to be ready on {} instances...",
        instance_ids.len()
    );

    loop {
        if start_time.elapsed() > timeout {
            return Err(Error::other(format!(
                "Timed out waiting for SSM agent after {}s",
                SSM_AGENT_READY_TIMEOUT_SECS
            )));
        }

        let instance_ids_str = instance_ids.join(",");
        let output = run_fun!(
            aws ssm describe-instance-information
                --filters "Key=InstanceIds,Values=$instance_ids_str"
                --query "InstanceInformationList[].InstanceId"
                --output text
                2>/dev/null
        )
        .unwrap_or_default();

        let online_instances: Vec<&str> = output.split_whitespace().collect();
        let online_count = online_instances.len();

        if online_count >= instance_ids.len() {
            info!(
                "All {} instances are SSM-managed and ready",
                instance_ids.len()
            );
            return Ok(());
        }

        info!(
            "Waiting for SSM agent: {}/{} instances ready, retrying in {}s...",
            online_count,
            instance_ids.len(),
            SSM_POLL_INTERVAL_SECS
        );

        std::thread::sleep(Duration::from_secs(SSM_POLL_INTERVAL_SECS));
    }
}

fn get_instance_private_ip(instance_id: &str) -> Result<String, Error> {
    let output = run_fun!(
        aws ec2 describe-instances
            --instance-ids $instance_id
            --query "Reservations[0].Instances[0].PrivateIpAddress"
            --output text
    )
    .map_err(|e| {
        Error::other(format!(
            "Failed to get private IP for instance {}: {}",
            instance_id, e
        ))
    })?;

    let ip = output.trim().to_string();
    if ip.is_empty() || ip == "None" {
        return Err(Error::other(format!(
            "No private IP found for instance {}",
            instance_id
        )));
    }

    Ok(ip)
}

fn get_instance_hostname(instance_id: &str) -> Result<String, Error> {
    let output = run_fun!(
        aws ec2 describe-instances
            --instance-ids $instance_id
            --query "Reservations[0].Instances[0].PrivateDnsName"
            --output text
    )
    .map_err(|e| {
        Error::other(format!(
            "Failed to get hostname for instance {}: {}",
            instance_id, e
        ))
    })?;

    let hostname = output.trim().to_string();
    if hostname.is_empty() || hostname == "None" {
        return Err(Error::other(format!(
            "No hostname found for instance {}",
            instance_id
        )));
    }

    Ok(hostname)
}

fn ssm_run_command(instance_id: &str, command: &str, description: &str) -> CmdResult {
    let command_id = run_fun!(
        aws ssm send-command
            --document-name "AWS-RunShellScript"
            --targets "Key=InstanceIds,Values=$instance_id"
            --parameters commands="$command"
            --timeout-seconds 600
            --comment $description
            --query "Command.CommandId"
            --output text
    )
    .map_err(|e| Error::other(format!("Failed to send SSM command: {}", e)))?;

    let command_id = command_id.trim();
    info!("SSM command sent with ID: {}", command_id);

    // Wait for command to complete
    wait_for_ssm_command(command_id, instance_id)?;

    Ok(())
}

fn wait_for_ssm_command(command_id: &str, instance_id: &str) -> Result<(), Error> {
    let start_time = Instant::now();
    let timeout = Duration::from_secs(SSM_TIMEOUT_SECS);

    info!(
        "Waiting for SSM command {} to complete on instance {}...",
        command_id, instance_id
    );

    loop {
        if start_time.elapsed() > timeout {
            return Err(Error::other(format!(
                "SSM command {} timed out after {}s",
                command_id, SSM_TIMEOUT_SECS
            )));
        }

        let status = run_fun!(
            aws ssm get-command-invocation
                --command-id $command_id
                --instance-id $instance_id
                --query "Status"
                --output text
                2>/dev/null
        )
        .unwrap_or_else(|_| "Pending".to_string());

        let status = status.trim();

        match status {
            "Success" => {
                info!("SSM command {} completed successfully", command_id);
                return Ok(());
            }
            "Failed" | "Cancelled" | "TimedOut" => {
                // Get the error output for debugging
                let error_output = run_fun!(
                    aws ssm get-command-invocation
                        --command-id $command_id
                        --instance-id $instance_id
                        --query "StandardErrorContent"
                        --output text
                        2>/dev/null
                )
                .unwrap_or_default();

                return Err(Error::other(format!(
                    "SSM command {} failed with status {}: {}",
                    command_id, status, error_output
                )));
            }
            _ => {
                // Still in progress
            }
        }

        std::thread::sleep(Duration::from_secs(SSM_POLL_INTERVAL_SECS));
    }
}

fn save_cluster_toml(content: &str) -> Result<String, Error> {
    let dir = std::env::current_dir()
        .map_err(|e| Error::other(format!("Failed to get current directory: {}", e)))?
        .join("data/etc");

    std::fs::create_dir_all(&dir)
        .map_err(|e| Error::other(format!("Failed to create data/etc directory: {}", e)))?;

    let path = dir.join("cluster.toml");
    std::fs::write(&path, content)
        .map_err(|e| Error::other(format!("Failed to write cluster.toml: {}", e)))?;

    Ok(path.to_string_lossy().to_string())
}

fn generate_ssh_config(instance_ip_map: &[(String, String)]) -> String {
    let mut config = String::from("# Auto-generated SSH config for simulate-on-prem\n");
    config.push_str("# Usage: ssh -F <this-file> <private-ip>\n");
    config.push_str("# Note: Run 'aws ec2-instance-connect send-ssh-public-key' before SSH\n\n");

    for (instance_id, ip) in instance_ip_map {
        config.push_str(&format!(
            r#"Host {}
    ProxyCommand aws ssm start-session --target {} --document-name AWS-StartSSHSession --parameters portNumber=%p
    User ec2-user
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    IdentityFile ~/.ssh/id_ed25519

"#,
            ip, instance_id
        ));
    }

    config
}

fn save_ssh_config(content: &str) -> Result<String, Error> {
    let dir = std::env::current_dir()
        .map_err(|e| Error::other(format!("Failed to get current directory: {}", e)))?
        .join("data/etc");

    std::fs::create_dir_all(&dir)
        .map_err(|e| Error::other(format!("Failed to create data/etc directory: {}", e)))?;

    let path = dir.join("ssh-config.conf");
    std::fs::write(&path, content)
        .map_err(|e| Error::other(format!("Failed to write SSH config: {}", e)))?;

    Ok(path.to_string_lossy().to_string())
}
