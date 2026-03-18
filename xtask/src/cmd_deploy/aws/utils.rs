use cmd_lib::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Error;

pub const CDK_OUTPUTS_PATH: &str = "/tmp/cdk-outputs.json";

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

/// Get the bootstrap bucket name for AWS: fractalbits-bootstrap-{region}-{account}
pub fn get_aws_bootstrap_bucket() -> Result<String, Error> {
    let region = run_fun!(aws configure get region)?;
    let account_id = run_fun!(aws sts get-caller-identity --query Account --output text)?;
    Ok(format!(
        "fractalbits-bootstrap-{}-{}",
        region.trim(),
        account_id.trim()
    ))
}
