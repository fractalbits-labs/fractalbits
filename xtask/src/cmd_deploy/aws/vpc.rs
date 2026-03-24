use crate::*;
use colored::*;
use dialoguer::Input;
use std::path::Path;

use super::super::bootstrap_progress;
use super::super::common::cloud_storage;
use super::super::common::{
    DeployTarget, VpcConfig, get_bootstrap_bucket_name, upload_config_and_blueprint,
};
use super::super::upload;
use super::config_gen;

pub fn create_vpc(mut config: VpcConfig) -> CmdResult {
    apply_template_defaults(&mut config);
    // 1. Upload binaries directly to AWS S3
    let aws_bucket = get_bootstrap_bucket_name(DeployTarget::Aws)?;
    if !config.skip_upload {
        info!("Uploading binaries to AWS S3...");
        upload::upload(DeployTarget::Aws)?;
    }

    // Delete any stale bootstrap_cluster.toml from S3 before CDK deploy.
    // This prevents a leftover MetaStack config (which has no instance entries) from being
    // served to VpcStack instances while they wait for the correct config to be uploaded
    // after CDK finishes.
    cloud_storage::delete_stale_bootstrap_config(&aws_bucket, DeployTarget::Aws)?;

    // 2. CDK deploy (instances self-bootstrap via UserData)
    let cdk_dir = "infra/fractalbits-cdk";

    // Check if node_modules exists, if not run npm install
    let node_modules_path = format!("{}/node_modules/", cdk_dir);
    if !Path::new(&node_modules_path).exists() {
        run_cmd! {
            info "Node modules not found. Installing dependencies...";
            cd $cdk_dir;
            npm install &>/dev/null;

            info "Disabling CDK collecting telemetry data...";
            npx cdk acknowledge 34892 &>/dev/null; // https://github.com/aws/aws-cdk/issues/34892
            npx cdk cli-telemetry --disable;
        }?;
    }

    // Check if CDK has been bootstrapped
    let bootstrap_cdk_exists = run_cmd! {
        aws cloudformation describe-stacks
            --stack-name CDKToolkit &>/dev/null
    }
    .is_ok();
    if !bootstrap_cdk_exists {
        run_cmd! {
            info "CDK bootstrap stack not found. Running CDK bootstrap...";
            cd $cdk_dir;
            npx cdk bootstrap 2>&1;
            info "CDK bootstrap completed successfully";
        }?;
    }

    // Build CDK context parameters
    let context_params = build_cdk_context(&config);

    // Generate and upload bootstrap config BEFORE CDK deploy so instances find it immediately on boot
    info!("Generating bootstrap config (pre-deploy)...");
    let bootstrap_config = config_gen::generate_bootstrap_config(&config)?;
    let config_toml = bootstrap_config
        .to_toml()
        .map_err(|e| std::io::Error::other(format!("Failed to serialize config: {}", e)))?;
    let s3_bucket = format!("s3://{aws_bucket}");
    upload_config_and_blueprint(&s3_bucket, &config_toml, &bootstrap_config)?;
    info!("Bootstrap config uploaded. Starting CDK deploy...");

    let outputs_file = "/tmp/cdk-outputs.json";
    run_cmd!(
        cd $cdk_dir;
        npx cdk deploy FractalbitsVpcStack $[context_params]
            --outputs-file $outputs_file --require-approval never 2>&1
    )?;
    info!("VPC deployment completed successfully");

    // 3. Instances self-bootstrap via UserData (all nodes download binary from S3)

    // 4. Optionally watch bootstrap progress inline
    if config.watch_bootstrap {
        bootstrap_progress::show_progress(DeployTarget::Aws, None)?;
    } else {
        info!("To monitor bootstrap progress: just deploy bootstrap-progress");
    }

    info!("View your deployed stack with: just describe-stack");

    Ok(())
}

pub fn destroy_vpc() -> CmdResult {
    // Display warning message
    warn!("This will permanently destroy the VPC and all associated resources!");
    warn!("This action cannot be undone.");

    // Require user to type exact confirmation text
    let _confirmation: String = Input::new()
        .with_prompt(format!(
            "Type {} to confirm VPC destruction",
            "permanent destroy".bold()
        ))
        .validate_with(|input: &String| -> Result<(), String> {
            if input == "permanent destroy" {
                Ok(())
            } else {
                Err(format!(
                    "You must type {} exactly to confirm",
                    "permanent destroy".bold()
                ))
            }
        })
        .interact_text()
        .map_err(|e| std::io::Error::other(format!("Failed to read confirmation: {e}")))?;

    // First destroy the CDK stack
    run_cmd! {
        info "Destroying CDK stack...";
        cd infra/fractalbits-cdk;
        npx cdk destroy FractalbitsVpcStack 2>&1;
        info "CDK stack destroyed successfully";
    }?;

    // Then cleanup S3 bucket
    cleanup_bootstrap_bucket()?;

    info!("VPC destruction completed successfully");
    Ok(())
}

fn build_cdk_context(config: &VpcConfig) -> Vec<String> {
    let mut params = Vec::new();
    let mut add = |key: &str, value: String| {
        params.push("--context".to_string());
        params.push(format!("{}={}", key, value));
    };

    add("numApiServers", config.num_api_servers.to_string());
    add("numBenchClients", config.num_bench_clients.to_string());
    add("numBssNodes", config.num_bss_nodes.to_string());
    add("bssInstanceTypes", config.bss_instance_type.clone());
    add("nssInstanceType", config.nss_instance_type.clone());
    add(
        "apiServerInstanceType",
        config.api_server_instance_type.clone(),
    );
    add(
        "benchClientInstanceType",
        config.bench_client_instance_type.clone(),
    );
    add("ebsVolumeSize", config.ebs_volume_size.to_string());
    add("ebsVolumeIops", config.ebs_volume_iops.to_string());
    if config.with_bench {
        add("benchType", "external".to_string());
    }
    if let Some(ref az_val) = config.az {
        add("az", az_val.clone());
    }
    if config.root_server_ha {
        add("rootServerHa", "true".to_string());
    }
    add("rssBackend", config.rss_backend.as_ref().to_string());
    add("journalType", config.journal_type.as_ref().to_string());
    if config.use_generic_binaries {
        add("useGenericBinaries", "true".to_string());
    }
    add("deployOS", config.deploy_os.as_ref().to_string());

    params
}

/// Apply template defaults to VpcConfig fields before CDK deploy.
/// Templates are shortcuts that set a group of related fields to preset values.
/// Applying them in Rust ensures the resolved values are used for TOML generation
/// and passed explicitly to CDK, rather than having CDK derive them independently.
fn apply_template_defaults(config: &mut VpcConfig) {
    use crate::VpcTemplate;
    match config.template {
        Some(VpcTemplate::Mini) => {
            config.nss_instance_type = "r7g.xlarge".to_string();
            config.bss_instance_type = "i8g.xlarge".to_string();
            config.ebs_volume_size = 5;
            config.ebs_volume_iops = 1000;
            config.root_server_ha = false;
            config.num_api_servers = 1;
            config.num_bss_nodes = 1;
            config.num_bench_clients = 1;
        }
        Some(VpcTemplate::PerfDemo) => {
            config.nss_instance_type = "r7g.4xlarge".to_string();
            config.ebs_volume_size = 20;
            config.ebs_volume_iops = 10000;
            config.root_server_ha = true;
            config.num_api_servers = 14;
            config.num_bss_nodes = 6;
            config.num_bench_clients = 42;
        }
        None => {}
    }
}

fn cleanup_bootstrap_bucket() -> CmdResult {
    let bucket_name = get_bootstrap_bucket_name(DeployTarget::Aws)?;
    let bucket = format!("s3://{bucket_name}");

    let bucket_exists = run_cmd!(aws s3api head-bucket --bucket $bucket_name &>/dev/null).is_ok();
    if !bucket_exists {
        info!("Bucket {bucket} does not exist, nothing to clean up");
        return Ok(());
    }

    run_cmd! {
        info "Emptying bucket $bucket (delete all objects)";
        aws s3 rm $bucket --recursive;

        info "Deleting bucket $bucket";
        aws s3 rb $bucket;
        info "Successfully cleaned up builds bucket: $bucket";
    }?;

    Ok(())
}
