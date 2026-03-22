use crate::*;
use xtask_common::cloud_storage;

use super::common::{DeployTarget, get_bootstrap_bucket_name};

pub fn upload(deploy_target: DeployTarget) -> CmdResult {
    match deploy_target {
        DeployTarget::Gcp => {
            let project_id = super::gcp::resolve_gcp_project(None)?;
            upload_gcp(&project_id)
        }
        DeployTarget::Aws | DeployTarget::OnPrem => upload_to_aws(deploy_target),
    }
}

/// Upload binaries directly to AWS S3 (no Docker images needed).
pub fn upload_to_aws(deploy_target: DeployTarget) -> CmdResult {
    let bucket_name = get_bootstrap_bucket_name(DeployTarget::Aws)?;
    let target = xtask_common::DeployTarget::Aws;

    cloud_storage::ensure_bucket(&bucket_name, target)?;

    // Sync binaries based on deploy target
    match deploy_target {
        DeployTarget::OnPrem | DeployTarget::Gcp => {
            // On-prem/GCP: sync only generic binaries (baseline CPU)
            for arch in ["x86_64", "aarch64"] {
                let src = format!("prebuilt/deploy/generic/{arch}");
                let dst = format!("s3://{bucket_name}/{arch}");
                info!("Syncing generic binaries for {arch} to {dst}");
                cloud_storage::sync_up(&src, &dst)?;
            }
        }
        DeployTarget::Aws => {
            // AWS: sync shared binaries to s3://{bucket}/{arch}/
            // and CPU-specific binaries to s3://{bucket}/{arch}/{cpu}/
            let cpu_targets = [
                ("x86_64", vec!["broadwell", "skylake"]),
                ("aarch64", vec!["neoverse-n1", "neoverse-n2"]),
            ];

            for (arch, cpus) in cpu_targets {
                // Sync shared binaries (bootstrap, etcd, warp) from generic
                let src = format!("prebuilt/deploy/generic/{arch}");
                let dst = format!("s3://{bucket_name}/{arch}");
                info!("Syncing shared binaries for {arch} to {dst}");
                cloud_storage::sync_up_filtered(
                    &src,
                    &dst,
                    &["fractalbits-bootstrap", "etcd", "etcdctl", "warp"],
                    &["*"],
                )?;

                // Sync CPU-specific binaries
                for cpu in &cpus {
                    let aws_cpu_path = format!("prebuilt/deploy/aws/{arch}/{cpu}");
                    if std::path::Path::new(&aws_cpu_path).exists() {
                        let cpu_dst = format!("s3://{bucket_name}/{arch}/{cpu}");
                        info!("Syncing AWS {cpu} binaries for {arch} to {cpu_dst}");
                        cloud_storage::sync_up(&aws_cpu_path, &cpu_dst)?;
                    }
                }
            }
        }
    }

    // Sync UI if it exists
    if std::path::Path::new("prebuilt/deploy/ui").exists() {
        let ui_dst = format!("s3://{bucket_name}/ui");
        info!("Syncing UI to {ui_dst}");
        cloud_storage::sync_up("prebuilt/deploy/ui", &ui_dst)?;
    }

    info!("Syncing all binaries is done");
    Ok(())
}

/// Upload binaries directly to GCS.
pub fn upload_gcp(project_id: &str) -> CmdResult {
    let bucket_name = format!("{project_id}-deploy-staging");
    let target = xtask_common::DeployTarget::Gcp;

    cloud_storage::ensure_bucket(&bucket_name, target)?;

    // GCP: sync generic binaries per arch
    for arch in ["x86_64", "aarch64"] {
        let src = format!("prebuilt/deploy/generic/{arch}");
        if std::path::Path::new(&src).exists() {
            let dst = format!("gs://{bucket_name}/{arch}");
            info!("Syncing generic binaries for {arch} to {dst}");
            cloud_storage::sync_up(&src, &dst)?;
        }
    }

    // Sync UI if it exists
    if std::path::Path::new("prebuilt/deploy/ui").exists() {
        let ui_dst = format!("gs://{bucket_name}/ui");
        info!("Syncing UI to {ui_dst}");
        cloud_storage::sync_up("prebuilt/deploy/ui", &ui_dst)?;
    }

    info!("Syncing all binaries to GCS is done");
    Ok(())
}

/// Upload binaries to on-prem Docker S3 endpoint.
#[allow(dead_code)] // Used by on-prem Docker S3 path
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

    let bucket_uri = format!("s3://{bucket_name}");
    let boostrap_script_content = format!(
        r#"#!/bin/bash
set -ex
exec > >(tee -a /var/log/fractalbits-bootstrap.log) 2>&1
echo "=== Bootstrap started at $(date) ==="
aws s3 cp --no-progress s3://{bucket_name}/$(arch)/fractalbits-bootstrap /opt/fractalbits/bin/fractalbits-bootstrap
chmod +x /opt/fractalbits/bin/fractalbits-bootstrap
/opt/fractalbits/bin/fractalbits-bootstrap {bucket_uri}
echo "=== Bootstrap completed at $(date) ==="
"#
    );

    // Upload bootstrap script and sync binaries
    run_cmd! {
        echo $boostrap_script_content | $[env_vars] aws s3 cp - "s3://$bucket_name/bootstrap.sh";
    }?;

    // Sync binaries based on deploy target
    match deploy_target {
        DeployTarget::OnPrem | DeployTarget::Gcp => {
            // On-prem/GCP: sync only generic binaries (baseline CPU)
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
