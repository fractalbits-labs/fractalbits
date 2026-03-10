use crate::*;
use std::collections::HashMap;

use super::common::{
    DeployTarget, VpcConfig, get_bootstrap_bucket_name, resolve_gcp_project, resolve_gcp_zone,
};
use super::{bootstrap_progress, gcp_config_gen, gcp_ssh, upload};

const TERRAFORM_DIR: &str = "infra/fractalbits-terraform";

pub fn create_gcp_vpc(config: VpcConfig) -> CmdResult {
    let project_id = resolve_gcp_project(config.gcp_project.as_deref())?;
    let zone = resolve_gcp_zone(config.gcp_zone.as_deref());
    let region = zone
        .rsplit_once('-')
        .map(|(r, _)| r)
        .unwrap_or("us-central1");

    // 1. Clear stale Firestore data from previous deployments BEFORE creating instances
    // This must happen before terraform apply so that newly bootstrapping instances
    // don't have their Firestore data (VG configs, API keys) deleted mid-bootstrap
    clear_firestore_stale_data(&project_id)?;

    // 2. Upload Docker images to bootstrap bucket (via Docker S3 on local)
    if !config.skip_upload {
        info!("Uploading Docker images...");
        upload::upload_docker_images_gcp(&project_id)?;
    }

    // 3. Terraform init + apply
    let tf_vars = build_terraform_vars(&config, &project_id, &zone, region);
    let tf_state_bucket =
        std::env::var("GCP_TF_STATE_BUCKET").unwrap_or_else(|_| format!("{project_id}-tf-state"));
    info!("Running Terraform apply...");
    run_cmd!(
        cd $TERRAFORM_DIR;
        terraform init
            -backend-config="bucket=$tf_state_bucket"
            -backend-config="prefix=vpc"
            -input=false 2>&1
    )?;
    run_cmd!(
        cd $TERRAFORM_DIR;
        terraform apply $[tf_vars] -auto-approve 2>&1
    )?;
    info!("Terraform apply completed");

    // 4. Parse Terraform outputs
    let outputs = gcp_ssh::parse_terraform_outputs(TERRAFORM_DIR)?;

    // 5. Setup Docker on RSS-A
    let rss_a_name = outputs
        .get("rss_a_name")
        .ok_or_else(|| std::io::Error::other("Terraform output 'rss_a_name' not found"))?;
    let rss_a_ip = gcp_ssh::get_instance_private_ip(rss_a_name, &zone, &project_id)?;

    gcp_ssh::wait_for_ssh_ready(rss_a_name, &zone, &project_id)?;
    let bucket_name = get_bootstrap_bucket_name(DeployTarget::Gcp)?;
    setup_docker_on_gcp_host(rss_a_name, &zone, &project_id, &bucket_name)?;

    // 6. Generate and upload bootstrap config
    let params = gcp_config_gen::GcpDeployParams {
        outputs: &outputs,
        project_id: &project_id,
        zone: &zone,
        rss_backend: to_common_rss_backend(config.rss_backend),
        rss_ha_enabled: config.root_server_ha,
        num_bss_nodes: config.num_bss_nodes as usize,
        num_api_servers: config.num_api_servers as usize,
        with_bench: config.with_bench,
    };
    let bootstrap_config = gcp_config_gen::generate_bootstrap_config(&params)?;
    let config_toml = bootstrap_config
        .to_toml()
        .map_err(|e| std::io::Error::other(format!("Failed to serialize config: {e}")))?;
    upload_config_to_gcp_docker_s3(rss_a_name, &zone, &project_id, &config_toml)?;

    // 7. Bootstrap all instances
    // RSS-A uses localhost since Docker S3 is local
    info!("Triggering bootstrap on RSS-A...");
    trigger_bootstrap_on_gcp(rss_a_name, &zone, &project_id, "localhost")?;

    // Other instances pull from Docker S3 on RSS-A via its private IP
    let instance_names = collect_instance_names(&outputs);
    for name in &instance_names {
        if name == rss_a_name {
            continue;
        }
        gcp_ssh::wait_for_ssh_ready(name, &zone, &project_id)?;
        trigger_bootstrap_on_gcp(name, &zone, &project_id, &rss_a_ip)?;
    }

    // 8. Watch bootstrap progress via IAP tunnel to Docker S3
    if config.watch_bootstrap {
        bootstrap_progress::show_progress_from_gcp_docker(rss_a_name, &zone, &project_id)?;
    } else {
        info!("To monitor bootstrap progress, run:");
        info!("  cargo xtask deploy bootstrap-progress --deploy-target gcp");
    }

    Ok(())
}

pub fn destroy_gcp_vpc(
    gcp_project: Option<String>,
    gcp_zone: Option<String>,
    delete_project: bool,
) -> CmdResult {
    use colored::*;
    use dialoguer::Input;

    let project_id = resolve_gcp_project(gcp_project.as_deref())?;
    let _zone = resolve_gcp_zone(gcp_zone.as_deref());

    if delete_project {
        warn!("This will DELETE the entire GCP project '{project_id}' and ALL its resources!");
        warn!("This includes VMs, disks, buckets, Firestore, IAM, and everything else.");
        warn!("This action cannot be undone.");

        let _confirmation: String = Input::new()
            .with_prompt(format!(
                "Type {} to confirm project deletion",
                project_id.bold()
            ))
            .validate_with(|input: &String| -> Result<(), String> {
                if input == &project_id {
                    Ok(())
                } else {
                    Err(format!(
                        "You must type {} exactly to confirm",
                        project_id.bold()
                    ))
                }
            })
            .interact_text()
            .map_err(|e| std::io::Error::other(format!("Failed to read confirmation: {e}")))?;

        run_cmd! {
            info "Deleting GCP project '$project_id'...";
            gcloud projects delete $project_id --quiet 2>&1;
            info "GCP project '$project_id' deleted";
        }?;
    } else {
        warn!("This will permanently destroy the GCP VPC and all associated resources!");
        warn!("This action cannot be undone.");

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

        run_cmd! {
            info "Destroying Terraform resources...";
            cd $TERRAFORM_DIR;
            terraform destroy -auto-approve 2>&1;
            info "Terraform destroy completed";
        }?;

        info!("GCP VPC destruction completed");
    }

    Ok(())
}

fn build_terraform_vars(
    config: &VpcConfig,
    project_id: &str,
    zone: &str,
    region: &str,
) -> Vec<String> {
    let mut vars = Vec::new();
    let mut add = |key: &str, value: &str| {
        vars.push("-var".to_string());
        vars.push(format!("{key}={value}"));
    };

    // Reuse existing cluster_id from terraform state if available,
    // otherwise generate a new one.
    let cluster_id = std::env::var("GCP_CLUSTER_ID").unwrap_or_else(|_| {
        let existing = cmd_lib::run_fun!(
            cd $TERRAFORM_DIR;
            terraform output -raw cluster_id 2>/dev/null
        );
        existing
            .ok()
            .filter(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'))
            .unwrap_or_else(|| {
                format!(
                    "{}-{}",
                    chrono::Local::now().format("%Y%m%d-%H%M%S"),
                    &project_id[..project_id.len().min(8)]
                )
            })
    });
    add("project_id", project_id);
    add("cluster_id", &cluster_id);
    add("region", region);
    add("zone_a", zone);
    add("num_api_servers", &config.num_api_servers.to_string());
    add("num_bss_nodes", &config.num_bss_nodes.to_string());
    add("root_server_ha", &config.root_server_ha.to_string());
    add(
        "rss_backend",
        match config.rss_backend {
            RssBackend::Etcd => "etcd",
            RssBackend::Ddb => "ddb",
            RssBackend::Firestore => "firestore",
        },
    );
    if config.with_bench {
        add("with_bench", "true");
    }
    if let Some(ref template_val) = config.template {
        add("vpc_template", template_val.as_ref());
    }

    vars
}

fn to_common_rss_backend(backend: RssBackend) -> xtask_common::RssBackend {
    match backend {
        RssBackend::Etcd => xtask_common::RssBackend::Etcd,
        RssBackend::Ddb => xtask_common::RssBackend::Ddb,
        RssBackend::Firestore => xtask_common::RssBackend::Firestore,
    }
}

fn setup_docker_on_gcp_host(
    instance_name: &str,
    zone: &str,
    project: &str,
    bucket_name: &str,
) -> CmdResult {
    info!("Setting up Docker on {instance_name}...");

    let gcs_bucket = format!("{project}-deploy-staging");

    let setup_script = format!(
        r#"#!/bin/bash
set -ex

# Clean up any previous container
docker stop fractalbits-bootstrap 2>/dev/null || true
docker rm fractalbits-bootstrap 2>/dev/null || true

# Install Docker and AWS CLI (for Docker S3 operations)
apt-get update -qq && apt-get install -y docker.io
systemctl start docker

# Enable IP forwarding so Docker bridge networking works for external traffic
# Persist across reboots and sysctl reloads (bootstrap may reset sysctl)
sysctl -w net.ipv4.ip_forward=1
echo 'net.ipv4.ip_forward=1' > /etc/sysctl.d/99-ip-forward.conf

# Flush nftables raw table to remove Docker's anti-spoof rules that block
# external access to container ports via DNAT
nft flush table ip raw 2>/dev/null || true

# Install AWS CLI (for Docker S3 operations)
command -v aws >/dev/null 2>&1 || snap install aws-cli --classic

# Download Docker image from GCS and load into Docker
ARCH=$(arch)
echo "Detected architecture: $ARCH"
gcloud storage cp "gs://{gcs_bucket}/docker/fractalbits-$ARCH.tar.gz" /tmp/fractalbits.tar.gz
docker load < /tmp/fractalbits.tar.gz
rm -f /tmp/fractalbits.tar.gz
docker tag "fractalbits:$ARCH" fractalbits:latest

# Download binaries bundle from GCS
gcloud storage cp "gs://{gcs_bucket}/docker/binaries-gcp.tar.gz" /tmp/binaries-gcp.tar.gz

# Start container (Docker S3 endpoint) with port mapping
# Only expose S3 ports (8080, 18080) to avoid port conflicts with host services
docker run -d --privileged --name fractalbits-bootstrap \
    -p 8080:8080 -p 18080:18080 \
    -v fractalbits-data:/data \
    fractalbits:latest

# Wait for container S3 to be ready
for i in $(seq 1 60); do
    if curl -sf --max-time 5 http://localhost:18080/mgmt/health; then
        echo ""
        echo "Container is ready"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "Timed out waiting for container"
        docker logs fractalbits-bootstrap 2>&1 | tail -50
        exit 1
    fi
    echo "Health check attempt $i/60..."
    sleep 5
done

# Set up Docker S3 credentials (S3 protocol on RSS-A)
export AWS_DEFAULT_REGION=localdev
export AWS_ENDPOINT_URL_S3=http://localhost:8080
export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret

BUCKET={bucket_name}
aws s3 mb "s3://$BUCKET" 2>/dev/null || true

# Upload binaries from GCS bundle to Docker S3
# tar contains generic/<arch>/* so strip the generic/ prefix
mkdir -p /tmp/binaries-unpack
tar -xzf /tmp/binaries-gcp.tar.gz --strip-components=1 -C /tmp/binaries-unpack generic/
aws s3 sync /tmp/binaries-unpack "s3://$BUCKET/"
rm -rf /tmp/binaries-unpack /tmp/binaries-gcp.tar.gz

# Upload bootstrap script to Docker S3
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

echo "Docker setup complete"
"#
    );

    gcp_ssh::gcp_run_command(
        instance_name,
        zone,
        project,
        &format!("sudo bash -c '{}'", setup_script.replace('\'', "'\\''")),
        "Setup Docker and bootstrap container",
    )
}

fn upload_config_to_gcp_docker_s3(
    instance_name: &str,
    zone: &str,
    project: &str,
    config_toml: &str,
) -> CmdResult {
    info!("Uploading bootstrap config to Docker S3 on {instance_name}...");

    // Write config to a temp file locally, SCP it to the instance, then upload to Docker S3.
    // This avoids shell escaping issues with nested single/double quotes in TOML content.
    let tmp_path = "/tmp/fractalbits_bootstrap_cluster.toml";
    std::fs::write(tmp_path, config_toml)?;

    run_cmd!(
        gcloud compute scp $tmp_path $instance_name:/tmp/bootstrap_cluster.toml
            --zone $zone
            --project $project
            --tunnel-through-iap
            --strict-host-key-checking=no 2>&1
    )?;
    std::fs::remove_file(tmp_path).ok();

    gcp_ssh::gcp_run_command(
        instance_name,
        zone,
        project,
        "export AWS_DEFAULT_REGION=localdev && export AWS_ENDPOINT_URL_S3=http://localhost:8080 && export AWS_ACCESS_KEY_ID=test_api_key && export AWS_SECRET_ACCESS_KEY=test_api_secret && aws s3 cp /tmp/bootstrap_cluster.toml s3://fractalbits-bootstrap/bootstrap_cluster.toml && rm -f /tmp/bootstrap_cluster.toml && echo 'Bootstrap config uploaded'",
        "Upload bootstrap config",
    )
}

fn trigger_bootstrap_on_gcp(
    instance_name: &str,
    zone: &str,
    project: &str,
    rss_a_ip: &str,
) -> CmdResult {
    let bootstrap_cmd = format!(
        r#"sudo bash -c '
command -v aws >/dev/null 2>&1 || snap install aws-cli --classic
export AWS_DEFAULT_REGION=localdev
export AWS_ENDPOINT_URL_S3=http://{rss_a_ip}:8080
export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret
export DOCKER_S3_AUTH=1
nohup bash -c "aws s3 cp --no-progress s3://fractalbits-bootstrap/bootstrap.sh - | bash" \
    </dev/null >/var/log/fractalbits-bootstrap.log 2>&1 &
echo "Bootstrap triggered in background (PID=$!)"
'"#
    );

    gcp_ssh::gcp_run_command(
        instance_name,
        zone,
        project,
        &bootstrap_cmd,
        "Trigger bootstrap",
    )
}

fn clear_firestore_stale_data(project_id: &str) -> CmdResult {
    info!("Clearing stale Firestore data from previous deployments...");
    let database_id = "fractalbits";
    let token = run_fun!(gcloud auth print-access-token)?;

    // Clear all collections that persist across deployments
    for collection in [
        "fractalbits-service-discovery",
        "fractalbits-api-keys-and-buckets",
    ] {
        // List all documents in the collection
        let list_url = format!(
            "https://firestore.googleapis.com/v1/projects/{project_id}/databases/{database_id}/documents/{collection}"
        );
        let list_result = run_fun!(
            curl -sf $list_url -H "Authorization: Bearer $token"
        );
        if let Ok(json_str) = list_result {
            // Parse document names and delete each one
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&json_str)
                && let Some(docs) = parsed.get("documents").and_then(|d| d.as_array())
            {
                for doc in docs {
                    if let Some(name) = doc.get("name").and_then(|n| n.as_str()) {
                        let delete_url = format!("https://firestore.googleapis.com/v1/{name}");
                        let _ = run_cmd!(
                            curl -sf -X DELETE $delete_url -H "Authorization: Bearer $token"
                        );
                    }
                }
                if !docs.is_empty() {
                    info!("Cleared {} documents from {collection}", docs.len());
                }
            }
        }
    }

    info!("Firestore stale data cleared");
    Ok(())
}

fn collect_instance_names(outputs: &HashMap<String, String>) -> Vec<String> {
    let mut names = Vec::new();
    for key in [
        "rss_a_name",
        "rss_b_name",
        "nss_a_name",
        "nss_b_name",
        "bench_server_name",
    ] {
        if let Some(name) = outputs.get(key).filter(|v| !v.is_empty()) {
            names.push(name.clone());
        }
    }
    // Collect MIG instances (BSS and API) by scanning sequential keys
    for prefix in ["bss", "api"] {
        for i in 0.. {
            match outputs.get(&format!("{prefix}_{i}_name")) {
                Some(name) => names.push(name.clone()),
                None => break,
            }
        }
    }
    names
}
