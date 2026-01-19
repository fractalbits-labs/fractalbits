use crate::*;
use colored::*;
use dialoguer::Input;
use std::path::Path;

use super::bootstrap;
use super::common::{DeployTarget, VpcConfig, get_bootstrap_bucket_name};
use super::simulate_on_prem;
use super::ssm_bootstrap;
use super::upload::upload;

pub fn create_vpc(config: VpcConfig) -> CmdResult {
    let VpcConfig {
        template,
        num_api_servers,
        num_bench_clients,
        num_bss_nodes,
        with_bench,
        bss_instance_type,
        api_server_instance_type,
        bench_client_instance_type,
        az,
        root_server_ha,
        rss_backend,
        ssm_bootstrap,
        journal_type,
        watch_bootstrap,
        skip_upload,
        simulate_on_prem,
    } = config;

    // Override settings for simulate-on-prem mode
    let (rss_backend, journal_type) = if simulate_on_prem {
        (crate::RssBackend::Etcd, crate::JournalType::Nvme)
    } else {
        (rss_backend, journal_type)
    };

    if !skip_upload {
        if simulate_on_prem {
            info!("Uploading Docker image to S3 bootstrap bucket (simulate-on-prem mode)...");
            super::upload::upload_docker_image_only()?;
        } else {
            info!("Uploading binaries to S3 bootstrap bucket...");
            upload(DeployTarget::Aws)?;
        }
    }

    // Note: Template-based configuration is handled in CDK (vpc/fractalbits-cdk/bin/fractalbits-vpc.ts)
    // The values passed here may be overridden by the template in CDK
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

    // Build CDK context parameters (each --context flag and value must be separate arguments)
    let mut context_params = Vec::new();
    let mut add_context = |key: &str, value: String| {
        context_params.push("--context".to_string());
        context_params.push(format!("{}={}", key, value));
    };

    add_context("numApiServers", num_api_servers.to_string());
    add_context("numBenchClients", num_bench_clients.to_string());
    add_context("numBssNodes", num_bss_nodes.to_string());
    add_context("bssInstanceTypes", bss_instance_type);
    add_context("apiServerInstanceType", api_server_instance_type);
    add_context("benchClientInstanceType", bench_client_instance_type);
    if with_bench {
        add_context("benchType", "external".to_string());
    }
    if let Some(template_val) = template {
        add_context("vpcTemplate", template_val.as_ref().to_string());
    }
    if let Some(az_val) = az {
        add_context("az", az_val);
    }
    if root_server_ha {
        add_context("rootServerHa", "true".to_string());
    }
    add_context("rssBackend", rss_backend.as_ref().to_string());
    add_context("journalType", journal_type.as_ref().to_string());

    // Add skipUserData context for SSM-based bootstrap
    if ssm_bootstrap {
        add_context("skipUserData", "true".to_string());
    }

    // Add simulate-on-prem context parameters
    if simulate_on_prem {
        add_context("simulateOnPrem", "true".to_string());
        add_context("skipUserData", "true".to_string()); // Skip normal bootstrap
    }

    // Deploy the VPC stack
    if simulate_on_prem {
        run_cmd! {
            info "Deploying FractalbitsVpcStack (simulate-on-prem mode)...";
            cd $cdk_dir;
            npx cdk deploy FractalbitsVpcStack
                $[context_params]
                --outputs-file /tmp/cdk-outputs.json
                --require-approval never 2>&1;
            info "VPC deployment completed successfully";
        }?;

        // Set up on-prem simulation
        simulate_on_prem::setup_on_prem_simulation()?;
        return Ok(());
    } else if ssm_bootstrap {
        run_cmd! {
            info "Deploying FractalbitsVpcStack (SSM bootstrap mode)...";
            cd $cdk_dir;
            npx cdk deploy FractalbitsVpcStack
                $[context_params]
                --outputs-file /tmp/cdk-outputs.json
                --require-approval never 2>&1;
            info "VPC deployment completed successfully";
        }?;

        // Bootstrap via SSM
        ssm_bootstrap::ssm_bootstrap_cluster()?;
    } else {
        run_cmd! {
            info "Deploying FractalbitsVpcStack...";
            cd $cdk_dir;
            npx cdk deploy FractalbitsVpcStack
                $[context_params]
                --require-approval never 2>&1;
            info "VPC deployment completed successfully";
        }?;
    }

    if watch_bootstrap {
        bootstrap::show_progress(DeployTarget::Aws)?;
    } else {
        info!("To monitor bootstrap progress, run:");
        info!("  cargo xtask deploy bootstrap-progress");
    }

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

fn cleanup_bootstrap_bucket() -> CmdResult {
    let bucket_name = get_bootstrap_bucket_name(DeployTarget::Aws)?;
    let bucket = format!("s3://{bucket_name}");

    // Check if the bucket exists
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
