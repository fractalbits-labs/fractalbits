use crate::*;

use super::common::{DeployTarget, get_bootstrap_bucket_name};

pub fn upload(deploy_target: DeployTarget) -> CmdResult {
    upload_with_endpoint(deploy_target, None)
}

/// Upload Docker images to AWS S3 (for simulate-on-prem mode)
/// Uploads both architecture-specific images (aarch64 and x86_64)
pub fn upload_docker_image_only() -> CmdResult {
    let bucket_name = get_bootstrap_bucket_name(DeployTarget::Aws)?;

    // Upload both architecture-specific images
    for arch in ["aarch64", "x86_64"] {
        let image_path = format!("target/on-prem/fractalbits-{}.tar.gz", arch);
        if std::path::Path::new(&image_path).exists() {
            let s3_path = format!("s3://{}/docker/fractalbits-{}.tar.gz", bucket_name, arch);
            info!("Uploading {} Docker image to S3...", arch);
            run_cmd!(aws s3 cp $image_path $s3_path)?;
        } else {
            return Err(std::io::Error::other(format!(
                "Docker image for {} not found at {}. Run 'just deploy build --for-on-prem' first.",
                arch, image_path
            )));
        }
    }

    Ok(())
}

pub fn upload_with_endpoint(deploy_target: DeployTarget, s3_endpoint: Option<&str>) -> CmdResult {
    let bucket_name = if s3_endpoint.is_some() {
        get_bootstrap_bucket_name(deploy_target)?
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

    // Determine target-specific directory based on deploy target
    let target_dir = match deploy_target {
        DeployTarget::Aws => "aws",
        DeployTarget::OnPrem => "on_prem",
    };

    // Upload bootstrap script and sync binaries
    // First sync generic binaries, then overlay with target-specific Zig binaries
    run_cmd! {
        echo $boostrap_script_content | $[env_vars] aws s3 cp - "s3://$bucket_name/bootstrap.sh";
    }?;

    for arch in ["x86_64", "aarch64"] {
        run_cmd! {
            info "Syncing generic binaries for $arch to S3 bucket $bucket_name";
            $[env_vars] aws s3 sync prebuilt/deploy/generic/$arch "s3://$bucket_name/$arch";
        }?;
        run_cmd! {
            info "Syncing $target_dir Zig binaries for $arch to S3 bucket $bucket_name";
            $[env_vars] aws s3 sync prebuilt/deploy/$target_dir/$arch "s3://$bucket_name/$arch";
        }?;
    }

    // Sync UI if it exists
    if std::path::Path::new("prebuilt/deploy/ui").exists() {
        run_cmd! {
            info "Syncing UI to S3 bucket $bucket_name";
            $[env_vars] aws s3 sync prebuilt/deploy/ui "s3://$bucket_name/ui";
        }?;
    }

    // Upload Docker images if they exist (both architectures)
    if deploy_target == DeployTarget::Aws {
        for arch in ["aarch64", "x86_64"] {
            let image_path = format!("target/on-prem/fractalbits-{}.tar.gz", arch);
            if std::path::Path::new(&image_path).exists() {
                let s3_path = format!("s3://{}/docker/fractalbits-{}.tar.gz", bucket_name, arch);
                info!("Uploading {} Docker image to S3...", arch);
                run_cmd!($[env_vars] aws s3 cp $image_path $s3_path)?;
            }
        }
    }

    info!("Syncing all binaries is done");
    Ok(())
}
