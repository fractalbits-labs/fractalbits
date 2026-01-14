use crate::*;

use super::common::DeployTarget;

pub fn upload(deploy_target: DeployTarget) -> CmdResult {
    // Check/create S3 bucket and sync
    let bucket_name = get_bootstrap_bucket_name(deploy_target)?;

    // Check if the bucket exists; create if it doesn't
    let bucket_exists = run_cmd!(aws s3api head-bucket --bucket $bucket_name &>/dev/null).is_ok();
    if !bucket_exists {
        run_cmd! {
            info "Creating bucket $bucket_name";
            aws s3 mb "s3://$bucket_name";
        }?;
    }

    let boostrap_script_content = format!(
        r#"#!/bin/bash
set -ex
aws s3 cp --no-progress s3://{bucket_name}/$(arch)/fractalbits-bootstrap /opt/fractalbits/bin/fractalbits-bootstrap
chmod +x /opt/fractalbits/bin/fractalbits-bootstrap
/opt/fractalbits/bin/fractalbits-bootstrap {bucket_name}"#
    );

    run_cmd! {
        echo $boostrap_script_content | aws s3 cp - "s3://$bucket_name/bootstrap.sh";
        info "Syncing all binaries to S3 bucket $bucket_name";
        aws s3 sync prebuilt/deploy "s3://$bucket_name";
        info "Syncing all binaries is done";
    }?;
    Ok(())
}

pub fn get_bootstrap_bucket_name(deploy_target: DeployTarget) -> FunResult {
    match deploy_target {
        DeployTarget::OnPrem => Ok("fractalbits-bootstrap".to_string()),
        DeployTarget::Aws => {
            let region = run_fun!(aws configure get region)?;
            let account_id = run_fun!(aws sts get-caller-identity --query Account --output text)?;
            Ok(format!("fractalbits-bootstrap-{region}-{account_id}"))
        }
    }
}
