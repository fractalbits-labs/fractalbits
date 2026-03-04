use crate::*;
use colored::*;
use dialoguer::Input;
use std::path::Path;

use super::aws_config_gen;
use super::bootstrap_progress;
use super::common::{DeployTarget, VpcConfig, get_bootstrap_bucket_name};
use super::docker_host;
use super::ssm_bootstrap;
use super::ssm_utils;
use super::upload;

pub fn create_vpc(config: VpcConfig) -> CmdResult {
    // 1. Upload Docker images to AWS S3
    if !config.skip_upload {
        info!("Uploading Docker images to AWS S3...");
        upload::upload_docker_images(DeployTarget::Aws)?;
    }

    // 2. CDK deploy (no UserData, all bootstrap via SSM)
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

    info!("Deploying FractalbitsVpcStack...");
    let outputs_file = "/tmp/cdk-outputs.json";
    run_cmd!(
        cd $cdk_dir;
        npx cdk deploy FractalbitsVpcStack $[context_params]
            --outputs-file $outputs_file --require-approval never 2>&1
    )?;
    info!("VPC deployment completed successfully");

    // 3. Parse CDK outputs
    let outputs = ssm_utils::parse_cdk_outputs()?;

    // 4. Create temp Docker host
    let subnet_id = outputs
        .get("privateSubnetId")
        .ok_or_else(|| std::io::Error::other("CDK output 'privateSubnetId' not found"))?;
    let sg_id = outputs
        .get("privateSgId")
        .ok_or_else(|| std::io::Error::other("CDK output 'privateSgId' not found"))?;
    let role_name = outputs
        .get("instanceProfileName")
        .ok_or_else(|| std::io::Error::other("CDK output 'instanceProfileName' not found"))?;

    let docker_host = docker_host::create_docker_host(subnet_id, sg_id, role_name)?;

    // 5. Setup Docker on host (downloads AWS image from S3, starts container)
    let aws_bucket = get_bootstrap_bucket_name(DeployTarget::Aws)?;
    docker_host::setup_docker_on_host(&docker_host.instance_id, &aws_bucket, "aws")?;

    // 6. Generate and upload bootstrap config to Docker S3
    let bootstrap_config = aws_config_gen::generate_bootstrap_config(&outputs, &config)?;
    let config_toml = bootstrap_config
        .to_toml()
        .map_err(|e| std::io::Error::other(format!("Failed to serialize config: {}", e)))?;
    docker_host::upload_config_to_docker_s3(&docker_host.instance_id, &config_toml)?;

    // 7. Bootstrap all nodes via SSM (pointing at Docker S3)
    let instance_ids = ssm_utils::collect_all_instance_ids(&outputs)?;
    ssm_utils::wait_for_ssm_agent_ready(&instance_ids)?;
    ssm_bootstrap::ssm_bootstrap_from_docker(&instance_ids, &docker_host.private_ip)?;

    // 8. Wait for bootstrap, then terminate Docker host
    if config.watch_bootstrap {
        bootstrap_progress::show_progress_from_docker(&docker_host.private_ip)?;
    } else {
        info!("To monitor bootstrap progress, run:");
        info!("  cargo xtask deploy bootstrap-progress");
    }

    docker_host::terminate_docker_host(&docker_host.instance_id)?;

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
    add(
        "apiServerInstanceType",
        config.api_server_instance_type.clone(),
    );
    add(
        "benchClientInstanceType",
        config.bench_client_instance_type.clone(),
    );
    if config.with_bench {
        add("benchType", "external".to_string());
    }
    if let Some(ref template_val) = config.template {
        add("vpcTemplate", template_val.as_ref().to_string());
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
