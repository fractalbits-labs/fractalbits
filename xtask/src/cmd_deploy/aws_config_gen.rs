use std::collections::HashMap;
use std::io::Error;

use chrono::Utc;
use uuid::Uuid;
use xtask_common::{
    BootstrapClusterConfig, ClusterAwsConfig, ClusterEndpointsConfig, ClusterEtcdConfig,
    ClusterGlobalConfig, ClusterResourcesConfig, DataBlobStorage, NodeEntry,
};

use super::common::VpcConfig;
use super::ssm_utils;

fn to_common_rss_backend(backend: crate::RssBackend) -> xtask_common::RssBackend {
    match backend {
        crate::RssBackend::Etcd => xtask_common::RssBackend::Etcd,
        crate::RssBackend::Ddb => xtask_common::RssBackend::Ddb,
    }
}

fn to_common_journal_type(jt: crate::JournalType) -> xtask_common::JournalType {
    match jt {
        crate::JournalType::Ebs => xtask_common::JournalType::Ebs,
        crate::JournalType::Nvme => xtask_common::JournalType::Nvme,
    }
}

/// Generate a BootstrapClusterConfig from CDK outputs and VPC deployment config.
///
/// This replaces the CDK-side toml-config-builder.ts, moving config generation
/// to xtask so it can be uploaded to Docker S3 at deploy time.
pub fn generate_bootstrap_config(
    outputs: &HashMap<String, String>,
    vpc_config: &VpcConfig,
) -> Result<BootstrapClusterConfig, Error> {
    let region = outputs
        .get("region")
        .ok_or_else(|| Error::other("CDK output 'region' not found"))?
        .clone();

    let workflow_cluster_id = Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let journal_uuid = Uuid::now_v7().to_string();

    // Look up instance private IPs from CDK instance IDs
    let rss_a_id = get_output(outputs, "rssAId")?;
    let nss_a_id = get_output(outputs, "nssAId")?;
    let nss_b_id = get_output(outputs, "nssBId")?;
    let rss_b_id = outputs.get("rssBId").cloned();

    let rss_a_ip = ssm_utils::get_instance_private_ip(&rss_a_id)?;
    let nss_a_ip = ssm_utils::get_instance_private_ip(&nss_a_id)?;
    let nss_b_ip = ssm_utils::get_instance_private_ip(&nss_b_id)?;

    let is_ebs = vpc_config.journal_type == crate::JournalType::Ebs;

    // NSS endpoint: use nss-A private IP directly (single-AZ mode)
    let nss_endpoint = nss_a_ip.clone();

    // API server endpoint from NLB
    let api_endpoint = outputs.get("ApiNLBDnsName").cloned();

    // EBS volume IDs (only for EBS journal type)
    let volume_a_id = if is_ebs {
        outputs.get("VolumeAId").cloned()
    } else {
        None
    };
    let volume_b_id = if is_ebs {
        outputs.get("VolumeBId").cloned()
    } else {
        None
    };

    // Build nodes map
    let mut nodes: HashMap<String, Vec<NodeEntry>> = HashMap::new();

    // RSS nodes
    nodes
        .entry("root_server".to_string())
        .or_default()
        .push(NodeEntry {
            id: rss_a_id.clone(),
            private_ip: Some(rss_a_ip),
            role: Some("leader".to_string()),
            volume_id: None,
            journal_uuid: None,
            bench_client_num: None,
        });
    if let Some(rss_b) = &rss_b_id {
        let rss_b_ip = ssm_utils::get_instance_private_ip(rss_b)?;
        nodes
            .entry("root_server".to_string())
            .or_default()
            .push(NodeEntry {
                id: rss_b.clone(),
                private_ip: Some(rss_b_ip),
                role: Some("follower".to_string()),
                volume_id: None,
                journal_uuid: None,
                bench_client_num: None,
            });
    }

    // NSS nodes
    let nss_a_volume = if is_ebs { volume_a_id.clone() } else { None };
    let nss_journal = if is_ebs {
        Some(journal_uuid.clone())
    } else {
        None
    };
    nodes
        .entry("nss_server".to_string())
        .or_default()
        .push(NodeEntry {
            id: nss_a_id.clone(),
            private_ip: Some(nss_a_ip),
            role: None,
            volume_id: nss_a_volume,
            journal_uuid: nss_journal.clone(),
            bench_client_num: None,
        });

    let nss_b_volume = if is_ebs {
        volume_b_id.clone().or(volume_a_id.clone())
    } else {
        None
    };
    nodes
        .entry("nss_server".to_string())
        .or_default()
        .push(NodeEntry {
            id: nss_b_id.clone(),
            private_ip: Some(nss_b_ip),
            role: None,
            volume_id: nss_b_volume,
            journal_uuid: nss_journal,
            bench_client_num: None,
        });

    // BSS nodes (from ASG)
    if let Some(bss_asg) = outputs.get("bssAsgName")
        && !bss_asg.is_empty()
    {
        let bss_ids = ssm_utils::get_asg_instance_ids(bss_asg)?;
        for id in &bss_ids {
            let ip = ssm_utils::get_instance_private_ip(id)?;
            nodes
                .entry("bss_server".to_string())
                .or_default()
                .push(NodeEntry {
                    id: id.clone(),
                    private_ip: Some(ip),
                    role: None,
                    volume_id: None,
                    journal_uuid: None,
                    bench_client_num: None,
                });
        }
    }

    // API server nodes (from ASG) - no individual entries needed,
    // they discover via DynamoDB service discovery

    // Bench server
    if let Some(bench_id) = outputs.get("benchserverId") {
        let bench_ip = ssm_utils::get_instance_private_ip(bench_id)?;
        let bench_client_asg = outputs.get("benchClientAsgName");
        let num_bench_clients = if let Some(asg) = bench_client_asg
            && !asg.is_empty()
        {
            ssm_utils::get_asg_instance_ids(asg)?.len()
        } else {
            0
        };
        nodes
            .entry("bench_server".to_string())
            .or_default()
            .push(NodeEntry {
                id: bench_id.clone(),
                private_ip: Some(bench_ip),
                role: None,
                volume_id: None,
                journal_uuid: None,
                bench_client_num: Some(num_bench_clients),
            });
    }

    // Bench client nodes (from ASG)
    if let Some(bench_asg) = outputs.get("benchClientAsgName")
        && !bench_asg.is_empty()
    {
        let bench_ids = ssm_utils::get_asg_instance_ids(bench_asg)?;
        for (i, id) in bench_ids.iter().enumerate() {
            let ip = ssm_utils::get_instance_private_ip(id)?;
            nodes
                .entry("bench_client".to_string())
                .or_default()
                .push(NodeEntry {
                    id: id.clone(),
                    private_ip: Some(ip),
                    role: None,
                    volume_id: None,
                    journal_uuid: None,
                    bench_client_num: Some(i),
                });
        }
    }

    // GUI server
    if let Some(gui_id) = outputs.get("guiserverId") {
        let gui_ip = ssm_utils::get_instance_private_ip(gui_id)?;
        nodes
            .entry("gui_server".to_string())
            .or_default()
            .push(NodeEntry {
                id: gui_id.clone(),
                private_ip: Some(gui_ip),
                role: None,
                volume_id: None,
                journal_uuid: None,
                bench_client_num: None,
            });
    }

    // Determine local AZ from the region (use first AZ)
    let local_az = run_fun_trimmed(
        "aws",
        &[
            "ec2",
            "describe-availability-zones",
            "--region",
            &region,
            "--query",
            "AvailabilityZones[0].ZoneName",
            "--output",
            "text",
        ],
    )?;

    let aws_config = ClusterAwsConfig {
        data_blob_bucket: outputs.get("DataBlobBucketName").cloned(),
        local_az,
        remote_az: None,
    };

    let config = BootstrapClusterConfig {
        global: ClusterGlobalConfig {
            deploy_target: xtask_common::DeployTarget::Aws,
            region,
            for_bench: vpc_config.with_bench,
            data_blob_storage: DataBlobStorage::AllInBssSingleAz,
            rss_ha_enabled: vpc_config.root_server_ha,
            rss_backend: to_common_rss_backend(vpc_config.rss_backend),
            journal_type: to_common_journal_type(vpc_config.journal_type),
            num_bss_nodes: Some(vpc_config.num_bss_nodes as usize),
            num_api_servers: Some(vpc_config.num_api_servers as usize),
            num_bench_clients: if vpc_config.with_bench {
                Some(vpc_config.num_bench_clients as usize)
            } else {
                None
            },
            workflow_cluster_id: Some(workflow_cluster_id),
            meta_stack_testing: false,
            use_generic_binaries: vpc_config.use_generic_binaries,
        },
        aws: Some(aws_config),
        endpoints: ClusterEndpointsConfig {
            nss_endpoint,
            api_server_endpoint: api_endpoint,
        },
        resources: Some(ClusterResourcesConfig {
            nss_a_id,
            nss_b_id: Some(nss_b_id),
            volume_a_id,
            volume_b_id,
        }),
        etcd: if vpc_config.rss_backend == crate::RssBackend::Etcd {
            Some(ClusterEtcdConfig {
                enabled: true,
                cluster_size: vpc_config.num_bss_nodes as usize,
                endpoints: None,
            })
        } else {
            None
        },
        nodes,
        bootstrap_bucket: "fractalbits-bootstrap".to_string(),
    };

    Ok(config)
}

fn get_output(outputs: &HashMap<String, String>, key: &str) -> Result<String, Error> {
    outputs
        .get(key)
        .filter(|v| !v.is_empty())
        .cloned()
        .ok_or_else(|| Error::other(format!("CDK output '{}' not found", key)))
}

fn run_fun_trimmed(cmd: &str, args: &[&str]) -> Result<String, Error> {
    use std::process::Command;
    let output = Command::new(cmd)
        .args(args)
        .output()
        .map_err(|e| Error::other(format!("Failed to run {} {:?}: {}", cmd, args, e)))?;
    if !output.status.success() {
        return Err(Error::other(format!(
            "{} {:?} failed: {}",
            cmd,
            args,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}
