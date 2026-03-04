use crate::*;

use super::build::DOCKER_OUTPUT_DIR;
use super::common::{DeployTarget, get_bootstrap_bucket_name};

pub fn upload(deploy_target: DeployTarget) -> CmdResult {
    upload_with_endpoint(deploy_target, None)
}

/// Build slim Docker images and binaries bundle, then upload to AWS S3.
pub fn upload_docker_images(deploy_target: DeployTarget) -> CmdResult {
    super::build::build_docker_images()?;
    super::build::pack_binaries(Some(deploy_target))?;

    let bucket_name = get_bootstrap_bucket_name(DeployTarget::Aws)?;

    // Create bucket if it doesn't exist
    let bucket_exists = run_cmd!(aws s3api head-bucket --bucket $bucket_name &>/dev/null).is_ok();
    if !bucket_exists {
        run_cmd! {
            info "Creating bucket $bucket_name";
            aws s3 mb "s3://$bucket_name";
        }?;
    }

    // Upload slim Docker images (one per arch)
    for arch in ["aarch64", "x86_64"] {
        let image_path = format!("{}/fractalbits-{}.tar.gz", DOCKER_OUTPUT_DIR, arch);
        let s3_path = format!("s3://{}/docker/fractalbits-{}.tar.gz", bucket_name, arch);
        info!("Uploading {} Docker image to S3...", arch);
        run_cmd!(aws s3 cp $image_path $s3_path)?;
    }

    // Upload binaries bundle
    let variant = match deploy_target {
        DeployTarget::Aws => "aws",
        DeployTarget::OnPrem => "onprem",
    };
    let binaries_path = format!("{}/binaries-{}.tar.gz", DOCKER_OUTPUT_DIR, variant);
    let s3_path = format!("s3://{}/docker/binaries-{}.tar.gz", bucket_name, variant);
    info!("Uploading {} binaries bundle to S3...", variant);
    run_cmd!(aws s3 cp $binaries_path $s3_path)?;

    Ok(())
}

pub fn upload_with_endpoint(deploy_target: DeployTarget, s3_endpoint: Option<&str>) -> CmdResult {
    // Docker S3 always uses the simple bucket name (no region/account suffix).
    // AWS S3 uses the qualified name to avoid cross-account collisions.
    let bucket_name = if s3_endpoint.is_some() {
        get_bootstrap_bucket_name(DeployTarget::OnPrem)?
    } else {
        get_bootstrap_bucket_name(DeployTarget::Aws)?
    };

    // Build environment variables for S3 access as a vector
    let endpoint_env = s3_endpoint.map(|e| format!("AWS_ENDPOINT_URL_S3=http://{}", e));
    let env_vars = match &endpoint_env {
        Some(endpoint_var) => &vec![
            "AWS_DEFAULT_REGION=localdev",
            endpoint_var.as_str(),
            "AWS_ACCESS_KEY_ID=test_api_key",
            "AWS_SECRET_ACCESS_KEY=test_api_secret",
        ],
        None => &vec![],
    };

    // Check if the bucket exists; create if it doesn't
    let bucket_exists =
        run_cmd!($[env_vars] aws s3api head-bucket --bucket $bucket_name &>/dev/null).is_ok();
    if !bucket_exists {
        run_cmd! {
            info "Creating bucket $bucket_name";
            $[env_vars] aws s3 mb "s3://$bucket_name";
        }?;
    }

    let boostrap_script_content = format!(
        r#"#!/bin/bash
set -ex
exec > >(tee -a /var/log/fractalbits-bootstrap.log) 2>&1
echo "=== Bootstrap started at $(date) ==="
aws s3 cp --no-progress s3://{bucket_name}/$(arch)/fractalbits-bootstrap /opt/fractalbits/bin/fractalbits-bootstrap
chmod +x /opt/fractalbits/bin/fractalbits-bootstrap
/opt/fractalbits/bin/fractalbits-bootstrap {bucket_name}
echo "=== Bootstrap completed at $(date) ==="
"#
    );

    // Upload bootstrap script and sync binaries
    run_cmd! {
        echo $boostrap_script_content | $[env_vars] aws s3 cp - "s3://$bucket_name/bootstrap.sh";
    }?;

    // Sync binaries based on deploy target
    match deploy_target {
        DeployTarget::OnPrem => {
            // On-prem: sync only generic binaries (baseline CPU)
            for arch in ["x86_64", "aarch64"] {
                run_cmd! {
                    info "Syncing generic binaries for $arch to S3 bucket $bucket_name";
                    $[env_vars] aws s3 sync prebuilt/deploy/generic/$arch "s3://$bucket_name/$arch";
                }?;
            }
        }
        DeployTarget::Aws => {
            // AWS: sync generic (for bootstrap/etcd/warp) to s3://{bucket}/{arch}/
            // and CPU-specific binaries to s3://{bucket}/{arch}/{cpu}/
            let cpu_targets = [
                ("x86_64", vec!["broadwell", "skylake"]),
                ("aarch64", vec!["neoverse-n1", "neoverse-n2"]),
            ];

            for (arch, cpus) in cpu_targets {
                // Sync shared binaries (bootstrap, etcd, warp) from generic to s3://{bucket}/{arch}/
                run_cmd! {
                    info "Syncing shared binaries for $arch to S3 bucket $bucket_name";
                    $[env_vars] aws s3 sync prebuilt/deploy/generic/$arch "s3://$bucket_name/$arch"
                        --exclude "*"
                        --include "fractalbits-bootstrap"
                        --include "etcd"
                        --include "etcdctl"
                        --include "warp";
                }?;

                // Sync CPU-specific binaries to s3://{bucket}/{arch}/{cpu}/
                for cpu in cpus {
                    let aws_cpu_path = format!("prebuilt/deploy/aws/{}/{}", arch, cpu);
                    if std::path::Path::new(&aws_cpu_path).exists() {
                        run_cmd! {
                            info "Syncing AWS $cpu binaries for $arch to S3 bucket $bucket_name/$arch/$cpu";
                            $[env_vars] aws s3 sync $aws_cpu_path "s3://$bucket_name/$arch/$cpu";
                        }?;
                    }
                }
            }
        }
    }

    // Sync UI if it exists
    if std::path::Path::new("prebuilt/deploy/ui").exists() {
        run_cmd! {
            info "Syncing UI to S3 bucket $bucket_name";
            $[env_vars] aws s3 sync prebuilt/deploy/ui "s3://$bucket_name/ui";
        }?;
    }

    info!("Syncing all binaries is done");
    Ok(())
}
