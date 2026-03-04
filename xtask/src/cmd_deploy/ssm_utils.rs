use crate::CmdResult;
use cmd_lib::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Error;
use std::time::{Duration, Instant};

pub const CDK_OUTPUTS_PATH: &str = "/tmp/cdk-outputs.json";
const SSM_POLL_INTERVAL_SECS: u64 = 5;
const SSM_TIMEOUT_SECS: u64 = 600;
const SSM_AGENT_READY_TIMEOUT_SECS: u64 = 300;

#[derive(Debug, Deserialize)]
struct CdkOutputs {
    #[serde(rename = "FractalbitsVpcStack")]
    stack: HashMap<String, String>,
}

pub fn parse_cdk_outputs() -> Result<HashMap<String, String>, Error> {
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

pub fn get_asg_instance_ids(asg_name: &str) -> Result<Vec<String>, Error> {
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

pub fn wait_for_ssm_agent_ready(instance_ids: &[String]) -> Result<(), Error> {
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

/// Send SSM command to multiple instances with a bootstrap command
pub fn ssm_send_command(
    instance_ids: &[String],
    command: &str,
    description: &str,
) -> Result<String, Error> {
    if instance_ids.is_empty() {
        return Err(Error::other("No instance IDs provided"));
    }

    let instance_ids_str = instance_ids.join(",");

    let command_id = run_fun!(
        aws ssm send-command
            --document-name "AWS-RunShellScript"
            --targets "Key=InstanceIds,Values=$instance_ids_str"
            --parameters commands="$command"
            --timeout-seconds 600
            --comment $description
            --query "Command.CommandId"
            --output text
    )
    .map_err(|e| Error::other(format!("Failed to send SSM command: {}", e)))?;

    Ok(command_id.trim().to_string())
}

/// Wait for SSM command to complete on multiple instances
pub fn wait_for_ssm_command(command_id: &str, instance_ids: &[String]) -> Result<(), Error> {
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

/// Send SSM command to a single instance and wait for completion
pub fn ssm_run_command(instance_id: &str, command: &str, description: &str) -> CmdResult {
    ssm_run_command_with_timeout(instance_id, command, description, SSM_TIMEOUT_SECS)
}

pub fn ssm_run_command_with_timeout(
    instance_id: &str,
    command: &str,
    description: &str,
    timeout_secs: u64,
) -> CmdResult {
    // Write command to a temp file as JSON to avoid shell quoting issues
    let params_json = serde_json::json!({"commands": [command]});
    let params_file = "/tmp/ssm-command-params.json";
    std::fs::write(params_file, params_json.to_string())
        .map_err(|e| Error::other(format!("Failed to write SSM params file: {}", e)))?;

    let ssm_timeout = timeout_secs.to_string();
    let params_arg = format!("file://{}", params_file);
    let command_id = run_fun!(
        aws ssm send-command
            --document-name "AWS-RunShellScript"
            --targets "Key=InstanceIds,Values=$instance_id"
            --parameters $params_arg
            --timeout-seconds $ssm_timeout
            --comment $description
            --query "Command.CommandId"
            --output text
    )
    .map_err(|e| Error::other(format!("Failed to send SSM command: {}", e)))?;

    let command_id = command_id.trim();
    info!("SSM command sent with ID: {}", command_id);

    wait_for_single_instance_command(command_id, instance_id, timeout_secs)?;

    Ok(())
}

/// Wait for SSM command on a single instance (with error output on failure)
fn wait_for_single_instance_command(
    command_id: &str,
    instance_id: &str,
    timeout_secs: u64,
) -> Result<(), Error> {
    let start_time = Instant::now();
    let timeout = Duration::from_secs(timeout_secs);

    info!(
        "Waiting for SSM command {} to complete on instance {}...",
        command_id, instance_id
    );

    loop {
        if start_time.elapsed() > timeout {
            return Err(Error::other(format!(
                "SSM command {} timed out after {}s",
                command_id, timeout_secs
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
            _ => {}
        }

        std::thread::sleep(Duration::from_secs(SSM_POLL_INTERVAL_SECS));
    }
}

pub fn get_instance_private_ip(instance_id: &str) -> Result<String, Error> {
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

/// Collect all instance IDs from CDK outputs (static instances + ASG instances)
pub fn collect_all_instance_ids(outputs: &HashMap<String, String>) -> Result<Vec<String>, Error> {
    let mut all_instance_ids: Vec<String> = Vec::new();

    // Static instance keys from CDK outputs
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

    // ASG instance keys from CDK outputs
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
        return Err(Error::other("No instances found in CDK outputs"));
    }

    Ok(all_instance_ids)
}
