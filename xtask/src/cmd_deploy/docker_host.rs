use crate::CmdResult;
use cmd_lib::*;
use std::io::Error;

use super::ssm_utils;

pub struct DockerHostInfo {
    pub instance_id: String,
    pub private_ip: String,
}

/// Install Docker, start the bootstrap container, and upload binaries on rss-A.
///
/// Downloads the slim Docker image and binaries bundle from AWS S3,
/// starts the container, then uploads binaries to the container's S3 service.
pub fn setup_docker_on_host(
    instance_id: &str,
    aws_bucket: &str,
    deploy_variant: &str,
) -> Result<DockerHostInfo, Error> {
    let private_ip = ssm_utils::get_instance_private_ip(instance_id)?;
    info!("Setting up Docker on {instance_id} ({private_ip})...");

    let setup_script = format!(
        r#"#!/bin/bash
set -ex

# Clean up any previous container
docker stop fractalbits-bootstrap 2>/dev/null || true
docker rm fractalbits-bootstrap 2>/dev/null || true

# Install and start Docker
yum install -y docker
systemctl start docker

# Detect architecture and download correct image + binaries
ARCH=$(arch)
echo "Detected architecture: $ARCH"

aws s3 cp --no-progress \
    "s3://{aws_bucket}/docker/fractalbits-$ARCH.tar.gz" /tmp/fractalbits.tar.gz
aws s3 cp --no-progress \
    "s3://{aws_bucket}/docker/binaries-{deploy_variant}.tar.gz" /tmp/binaries.tar.gz

# Load slim Docker image
docker load < <(gunzip -c /tmp/fractalbits.tar.gz)
rm /tmp/fractalbits.tar.gz
docker tag "fractalbits:$ARCH" fractalbits:latest

# Start container
docker run -d --privileged --name fractalbits-bootstrap \
    -p 8080:8080 -p 18080:18080 \
    -v fractalbits-data:/data \
    fractalbits:latest

# Wait for container S3 to be ready (up to 5 min)
for i in $(seq 1 60); do
    if curl -sf --max-time 5 http://localhost:18080/mgmt/health; then
        echo ""
        echo "Container is ready"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "Timed out waiting for container"
        docker logs fractalbits-bootstrap 2>&1 | tail -50
        exit 1
    fi
    echo "Health check attempt $i/60..."
    sleep 5
done

# Extract binaries bundle and upload to container S3
mkdir -p /tmp/binaries
tar -xzf /tmp/binaries.tar.gz -C /tmp/binaries
rm /tmp/binaries.tar.gz

export AWS_DEFAULT_REGION=localdev
export AWS_ENDPOINT_URL_S3=http://localhost:8080
export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret

BUCKET=fractalbits-bootstrap
aws s3 mb "s3://$BUCKET" 2>/dev/null || true

# Generate and upload bootstrap.sh
# bootstrap.sh runs with Docker S3 env vars already set by the SSM command,
# so it downloads fractalbits-bootstrap from Docker S3 (not AWS S3).
cat > /tmp/bootstrap.sh << 'BOOTEOF'
#!/bin/bash
set -ex
exec > >(tee -a /var/log/fractalbits-bootstrap.log) 2>&1
echo "=== Bootstrap started at $(date) ==="
mkdir -p /opt/fractalbits/bin
aws s3 cp --no-progress s3://fractalbits-bootstrap/$(arch)/fractalbits-bootstrap /opt/fractalbits/bin/fractalbits-bootstrap
chmod +x /opt/fractalbits/bin/fractalbits-bootstrap
/opt/fractalbits/bin/fractalbits-bootstrap fractalbits-bootstrap
echo "=== Bootstrap completed at $(date) ==="
BOOTEOF
aws s3 cp /tmp/bootstrap.sh "s3://$BUCKET/bootstrap.sh"
"#
    );

    // Generate the upload commands based on deploy variant
    let upload_commands = match deploy_variant {
        "aws" => generate_aws_upload_commands(),
        _ => generate_onprem_upload_commands(),
    };

    let full_script = format!("{}\n{}", setup_script, upload_commands);

    // 1200s timeout: download + docker load + health check + binary upload
    ssm_utils::ssm_run_command_with_timeout(
        instance_id,
        &full_script,
        "Setup Docker and upload binaries",
        1200,
    )
    .map_err(|e| Error::other(format!("Failed to setup Docker on {instance_id}: {e}")))?;

    info!("Docker bootstrap container running with binaries on {instance_id}");
    Ok(DockerHostInfo {
        instance_id: instance_id.to_string(),
        private_ip,
    })
}

fn generate_aws_upload_commands() -> String {
    let mut cmds = String::new();

    // Sync shared binaries (bootstrap, etcd, warp) from generic to s3://{bucket}/{arch}/
    for arch in ["x86_64", "aarch64"] {
        cmds.push_str(&format!(
            r#"
echo "Syncing shared binaries for {arch}..."
aws s3 sync /tmp/binaries/generic/{arch}/ "s3://$BUCKET/{arch}/" \
    --exclude "*" \
    --include "fractalbits-bootstrap" \
    --include "etcd" \
    --include "etcdctl" \
    --include "warp"
"#
        ));
    }

    // Sync CPU-specific binaries
    let cpu_targets = [
        ("x86_64", &["broadwell", "skylake"][..]),
        ("aarch64", &["neoverse-n1", "neoverse-n2"][..]),
    ];
    for (arch, cpus) in cpu_targets {
        for cpu in cpus {
            cmds.push_str(&format!(
                r#"
echo "Syncing AWS {cpu} binaries for {arch}..."
aws s3 sync /tmp/binaries/aws/{arch}/{cpu}/ "s3://$BUCKET/{arch}/{cpu}/"
"#
            ));
        }
    }

    // Sync UI
    cmds.push_str(
        r#"
if [ -d /tmp/binaries/ui ]; then
    echo "Syncing UI..."
    aws s3 sync /tmp/binaries/ui/ "s3://$BUCKET/ui/"
fi

rm -rf /tmp/binaries
echo "All binaries uploaded to container S3"
"#,
    );

    cmds
}

fn generate_onprem_upload_commands() -> String {
    let mut cmds = String::new();

    for arch in ["x86_64", "aarch64"] {
        cmds.push_str(&format!(
            r#"
echo "Syncing generic binaries for {arch}..."
aws s3 sync /tmp/binaries/generic/{arch}/ "s3://$BUCKET/{arch}/"
"#
        ));
    }

    cmds.push_str(
        r#"
if [ -d /tmp/binaries/ui ]; then
    echo "Syncing UI..."
    aws s3 sync /tmp/binaries/ui/ "s3://$BUCKET/ui/"
fi

rm -rf /tmp/binaries
echo "All binaries uploaded to container S3"
"#,
    );

    cmds
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

/// Clean up Docker on rss-A (best-effort, before VPC destruction).
pub fn cleanup_docker_host(instance_id: &str) -> CmdResult {
    info!("Cleaning up Docker on {instance_id}...");

    let cleanup_script = r#"#!/bin/bash
set -ex
docker stop fractalbits-bootstrap 2>/dev/null || true
docker rm fractalbits-bootstrap 2>/dev/null || true
systemctl stop docker 2>/dev/null || true
"#;
    let _ = ssm_utils::ssm_run_command(instance_id, cleanup_script, "Cleanup Docker");

    info!("Docker cleanup complete");
    Ok(())
}
