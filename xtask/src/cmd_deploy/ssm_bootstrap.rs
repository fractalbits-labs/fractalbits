use crate::CmdResult;
use cmd_lib::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Error;
use std::time::{Duration, Instant};

use super::common::{DeployTarget, get_bootstrap_bucket_name};

const CDK_OUTPUTS_PATH: &str = "/tmp/cdk-outputs.json";
const SSM_POLL_INTERVAL_SECS: u64 = 5;
const SSM_TIMEOUT_SECS: u64 = 600;
const SSM_AGENT_READY_TIMEOUT_SECS: u64 = 300;

#[derive(Debug, Deserialize)]
struct CdkOutputs {
    #[serde(rename = "FractalbitsVpcStack")]
    stack: HashMap<String, String>,
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

fn ssm_send_command(
    instance_ids: &[String],
    bucket_name: &str,
    description: &str,
) -> Result<String, Error> {
    if instance_ids.is_empty() {
        return Err(Error::other("No instance IDs provided"));
    }

    let instance_ids_str = instance_ids.join(",");
    let bootstrap_command = format!(
        "aws s3 cp --no-progress s3://{bucket_name}/bootstrap.sh - | sh 2>&1 | tee -a /var/log/cloud-init-output.log"
    );

    let command_id = run_fun!(
        aws ssm send-command
            --document-name "AWS-RunShellScript"
            --targets "Key=InstanceIds,Values=$instance_ids_str"
            --parameters commands="$bootstrap_command"
            --timeout-seconds 600
            --comment $description
            --query "Command.CommandId"
            --output text
    )
    .map_err(|e| Error::other(format!("Failed to send SSM command: {}", e)))?;

    Ok(command_id.trim().to_string())
}

fn wait_for_ssm_command(command_id: &str, instance_ids: &[String]) -> Result<(), Error> {
    let start_time = Instant::now();
    let timeout = Duration::from_secs(SSM_TIMEOUT_SECS);

    info!(
        "Waiting for SSM command {} to complete on {} instances...",
        command_id,
        instance_ids.len()
    );

    loop {
        if start_time.elapsed() > timeout {
            return Err(Error::other(format!(
                "SSM command {} timed out after {}s",
                command_id, SSM_TIMEOUT_SECS
            )));
        }

        let mut all_complete = true;
        let mut failed_instances: Vec<String> = Vec::new();

        for instance_id in instance_ids {
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
                "Success" => {}
                "Failed" | "Cancelled" | "TimedOut" => {
                    failed_instances.push(format!("{}:{}", instance_id, status));
                }
                _ => {
                    all_complete = false;
                }
            }
        }

        if !failed_instances.is_empty() {
            return Err(Error::other(format!(
                "SSM command failed on instances: {}",
                failed_instances.join(", ")
            )));
        }

        if all_complete {
            info!("SSM command {} completed successfully", command_id);
            return Ok(());
        }

        std::thread::sleep(Duration::from_secs(SSM_POLL_INTERVAL_SECS));
    }
}

pub fn ssm_bootstrap_cluster() -> CmdResult {
    let bucket_name = get_bootstrap_bucket_name(DeployTarget::Aws)?;

    info!("Parsing CDK outputs from {}...", CDK_OUTPUTS_PATH);
    let outputs = parse_cdk_outputs()?;

    let mut all_instance_ids: Vec<String> = Vec::new();

    // Collect static instance IDs (keys are from CDK outputs with special chars removed)
    let static_instance_keys = [
        "rssAId",
        "rssBId",
        "nssAId",
        "nssBId",
        "benchserverId",
        "guiserverId",
    ];

    for key in &static_instance_keys {
        if let Some(instance_id) = outputs.get(*key)
            && !instance_id.is_empty()
        {
            info!("Found static instance {}: {}", key, instance_id);
            all_instance_ids.push(instance_id.clone());
        }
    }

    // Collect ASG instance IDs
    let asg_keys = ["apiServerAsgName", "bssAsgName", "benchClientAsgName"];

    for key in &asg_keys {
        if let Some(asg_name) = outputs.get(*key)
            && !asg_name.is_empty()
        {
            info!("Getting instances from ASG {}: {}", key, asg_name);
            match get_asg_instance_ids(asg_name) {
                Ok(ids) => {
                    info!("Found {} instances in ASG {}", ids.len(), asg_name);
                    all_instance_ids.extend(ids);
                }
                Err(e) => {
                    warn!("Failed to get instances from ASG {}: {}", asg_name, e);
                }
            }
        }
    }

    if all_instance_ids.is_empty() {
        return Err(Error::other("No instances found to bootstrap"));
    }

    info!(
        "Bootstrapping {} instances via SSM: {:?}",
        all_instance_ids.len(),
        all_instance_ids
    );

    // Wait for SSM agent to be ready on all instances
    wait_for_ssm_agent_ready(&all_instance_ids)?;

    // AWS SSM has a limit of 50 instances per send-command, so batch them
    const SSM_BATCH_SIZE: usize = 50;
    let batches: Vec<&[String]> = all_instance_ids.chunks(SSM_BATCH_SIZE).collect();

    info!(
        "Sending SSM commands in {} batches (max {} instances per batch)",
        batches.len(),
        SSM_BATCH_SIZE
    );

    let mut command_results: Vec<(String, Vec<String>)> = Vec::new();

    for (batch_idx, batch) in batches.iter().enumerate() {
        let batch_vec: Vec<String> = batch.to_vec();
        let command_id = ssm_send_command(
            &batch_vec,
            &bucket_name,
            &format!(
                "Bootstrap fractalbits instances (batch {}/{})",
                batch_idx + 1,
                batches.len()
            ),
        )?;
        info!(
            "SSM command sent for batch {}/{} with ID: {} ({} instances)",
            batch_idx + 1,
            batches.len(),
            command_id,
            batch.len()
        );
        command_results.push((command_id, batch_vec));
    }

    // Wait for all commands to complete
    for (command_id, instance_ids) in &command_results {
        wait_for_ssm_command(command_id, instance_ids)?;
    }

    info!("All instances bootstrapped successfully via SSM");

    Ok(())
}
