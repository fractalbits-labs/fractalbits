use crate::CmdResult;
use cmd_lib::*;
use std::io::Error;

use super::ssm_utils;

pub struct DockerHostInfo {
    pub instance_id: String,
    pub private_ip: String,
}

/// Create a temporary ARM64 EC2 instance for running the Docker bootstrap container
pub fn create_docker_host(
    subnet_id: &str,
    sg_id: &str,
    role_name: &str,
) -> Result<DockerHostInfo, Error> {
    info!("Creating temporary Docker host instance...");

    // Look up the IAM instance profile name from the role name
    let instance_profile_name = run_fun!(
        aws iam list-instance-profiles-for-role
            --role-name $role_name
            --query "InstanceProfiles[0].InstanceProfileName"
            --output text
    )
    .map_err(|e| {
        Error::other(format!(
            "Failed to get instance profile for role {}: {}",
            role_name, e
        ))
    })?;
    let instance_profile_name = instance_profile_name.trim();

    // Get latest AL2023 ARM64 AMI
    let ami_id = run_fun!(
        aws ssm get-parameter
            --name "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-6.12-arm64"
            --query "Parameter.Value"
            --output text
    )
    .map_err(|e| Error::other(format!("Failed to get ARM64 AMI: {}", e)))?;
    let ami_id = ami_id.trim();

    let instance_id = run_fun!(
        aws ec2 run-instances
            --instance-type "c7g.medium"
            --image-id $ami_id
            --subnet-id $subnet_id
            --security-group-ids $sg_id
            --iam-instance-profile "Name=$instance_profile_name"
            --tag-specifications
                "ResourceType=instance,Tags=[{Key=Name,Value=fractalbits-docker-host},{Key=fractalbits:Role,Value=docker-host}]"
            --query "Instances[0].InstanceId"
            --output text
    )
    .map_err(|e| Error::other(format!("Failed to create Docker host instance: {}", e)))?;
    let instance_id = instance_id.trim().to_string();

    info!("Docker host instance created: {}", instance_id);

    // Wait for instance to reach running state
    info!("Waiting for instance to reach running state...");
    run_cmd!(
        aws ec2 wait instance-running --instance-ids $instance_id
    )
    .map_err(|e| {
        Error::other(format!(
            "Failed waiting for instance {}: {}",
            instance_id, e
        ))
    })?;

    let private_ip = ssm_utils::get_instance_private_ip(&instance_id)?;
    info!("Docker host running at {}", private_ip);

    Ok(DockerHostInfo {
        instance_id,
        private_ip,
    })
}

/// Install Docker and start the bootstrap container on the Docker host
pub fn setup_docker_on_host(
    instance_id: &str,
    aws_bucket: &str,
    docker_variant: &str,
) -> CmdResult {
    info!("Setting up Docker on host {}...", instance_id);

    ssm_utils::wait_for_ssm_agent_ready(&[instance_id.to_string()])?;

    let setup_script = format!(
        r#"#!/bin/bash
set -ex

# Install and start Docker
yum install -y docker
systemctl start docker
systemctl enable docker

# Detect architecture and download correct image
ARCH=$(arch)
echo "Detected architecture: $ARCH"

aws s3 cp --no-progress \
    "s3://{aws_bucket}/docker/fractalbits-{docker_variant}-$ARCH.tar.gz" /tmp/fractalbits.tar.gz
docker load < <(gunzip -c /tmp/fractalbits.tar.gz)
rm /tmp/fractalbits.tar.gz

docker tag "fractalbits-{docker_variant}:$ARCH" fractalbits:latest

docker run -d --privileged --name fractalbits-bootstrap \
    -p 8080:8080 -p 18080:18080 \
    -v fractalbits-data:/data \
    fractalbits:latest

# Wait for container to be ready (up to 5 min)
for i in $(seq 1 60); do
    if curl -sf --max-time 5 http://localhost:18080/mgmt/health; then
        echo ""
        echo "Docker bootstrap container is ready"
        exit 0
    fi
    echo "Health check attempt $i/60..."
    sleep 5
done
echo "Timed out waiting for Docker container"
docker logs fractalbits-bootstrap 2>&1 | tail -50
exit 1
"#
    );

    ssm_utils::ssm_run_command(instance_id, &setup_script, "Setup Docker bootstrap")?;
    info!("Docker bootstrap container is running on {}", instance_id);
    Ok(())
}

/// Upload bootstrap cluster config to the Docker S3 service
pub fn upload_config_to_docker_s3(docker_host_id: &str, config_toml: &str) -> CmdResult {
    info!("Uploading bootstrap config to Docker S3...");

    // Escape single quotes in the config for safe embedding in shell script
    let escaped_config = config_toml.replace('\'', "'\\''");

    let upload_script = format!(
        r#"#!/bin/bash
set -ex
export AWS_DEFAULT_REGION=localdev
export AWS_ENDPOINT_URL_S3=http://localhost:8080
export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret
echo '{escaped_config}' | aws s3 cp - s3://fractalbits-bootstrap/bootstrap_cluster.toml
echo "Bootstrap config uploaded successfully"
"#
    );

    ssm_utils::ssm_run_command(docker_host_id, &upload_script, "Upload bootstrap config")?;
    info!("Bootstrap config uploaded to Docker S3");
    Ok(())
}

/// Terminate the Docker host instance
pub fn terminate_docker_host(instance_id: &str) -> CmdResult {
    info!("Terminating Docker host instance {}...", instance_id);
    run_cmd!(
        aws ec2 terminate-instances --instance-ids $instance_id
    )
    .map_err(|e| {
        Error::other(format!(
            "Failed to terminate instance {}: {}",
            instance_id, e
        ))
    })?;
    info!("Docker host terminated");
    Ok(())
}
