use crate::*;

use super::super::common::{DeployTarget, VpcConfig, upload_config_and_blueprint};
use super::super::{bootstrap_progress, upload};
use super::config_gen;
const TERRAFORM_DIR: &str = "infra/gcp-terraform";

pub fn create_vpc(config: VpcConfig) -> CmdResult {
    let project_id = super::resolve_gcp_project(config.gcp_project.as_deref())?;
    let zone = super::resolve_gcp_zone(config.gcp_zone.as_deref());
    let region = zone
        .rsplit_once('-')
        .map(|(r, _)| r)
        .unwrap_or("us-central1");

    // 1. Clear stale Firestore data from previous deployments
    clear_firestore_stale_data(&project_id)?;

    // 2. Upload binaries directly to GCS
    let gcs_bucket = format!("{project_id}-deploy-staging");
    if !config.skip_upload {
        info!("Uploading binaries to GCS...");
        upload::upload_gcp(&project_id)?;
    }

    // 3. Generate and upload bootstrap config BEFORE Terraform so instances find it immediately on boot
    info!("Generating bootstrap config (pre-deploy)...");
    let params = config_gen::GcpDeployParams {
        project_id: &project_id,
        zone: &zone,
        region,
        rss_backend: config.rss_backend,
        rss_ha_enabled: config.root_server_ha,
        num_bss_nodes: config.num_bss_nodes as usize,
        num_api_servers: config.num_api_servers as usize,
        num_bench_clients: config.num_bench_clients as usize,
        with_bench: config.with_bench,
        use_generic_binaries: config.use_generic_binaries,
        bss_storage_alloc_mode: config.bss_storage_alloc_mode,
    };
    let bootstrap_config = config_gen::generate_bootstrap_config(&params)?;
    let config_toml = bootstrap_config
        .to_toml()
        .map_err(|e| std::io::Error::other(format!("Failed to serialize config: {e}")))?;
    let gs_bucket = format!("gs://{gcs_bucket}");
    upload_config_and_blueprint(&gs_bucket, &config_toml, &bootstrap_config)?;
    info!("Bootstrap config uploaded. Starting Terraform apply...");

    // 4. Terraform init + apply
    let tf_vars = build_terraform_vars(&config, &project_id, &zone, region);
    let tf_state_bucket =
        std::env::var("GCP_TF_STATE_BUCKET").unwrap_or_else(|_| format!("{project_id}-tf-state"));
    run_cmd!(
        cd $TERRAFORM_DIR;
        terraform init
            -backend-config="bucket=$tf_state_bucket"
            -backend-config="prefix=vpc"
            -reconfigure
            -input=false 2>&1
    )?;
    run_cmd!(
        cd $TERRAFORM_DIR;
        terraform apply $[tf_vars] -auto-approve 2>&1
    )?;
    info!("Terraform apply completed");

    // 5. Instances self-bootstrap via startup scripts (download binary from GCS)

    // 6. Watch bootstrap progress via GCS
    if config.watch_bootstrap {
        bootstrap_progress::show_progress_with_bucket(DeployTarget::Gcp, None, Some(&gcs_bucket))?;
    } else {
        info!("To monitor bootstrap progress, run: just deploy bootstrap-progress --target gcp");
    }

    Ok(())
}

pub fn destroy_vpc(
    gcp_project: Option<String>,
    gcp_zone: Option<String>,
    delete_project: bool,
) -> CmdResult {
    use colored::*;
    use dialoguer::Input;

    let project_id = super::resolve_gcp_project(gcp_project.as_deref())?;
    let _zone = super::resolve_gcp_zone(gcp_zone.as_deref());

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
        add("num_bench_clients", &config.num_bench_clients.to_string());
    }
    if let Some(ref template_val) = config.template {
        add("vpc_template", template_val.as_ref());
    }

    vars
}

fn clear_firestore_stale_data(project_id: &str) -> CmdResult {
    info!("Clearing stale Firestore data from previous deployments...");
    let database_id = "fractalbits";
    let token = run_fun!(gcloud auth print-access-token)?;

    // Discover every top-level collection instead of hard-coding names. Each
    // service self-registers into its own collection
    // (`fractalbits-service-discovery-<service>`), so a per-service node from a
    // prior cluster that didn't deregister cleanly will otherwise leak into
    // `resolve_nss`/`resolve_rss` lookups and break `running_nss_id` in
    // `journal-configs`. Wiping every fractalbits collection at deploy start
    // guarantees a clean slate.
    let list_url = format!(
        "https://firestore.googleapis.com/v1/projects/{project_id}/databases/{database_id}/documents:listCollectionIds"
    );
    let list_result = run_fun!(
        curl -sf -X POST $list_url
            -H "Authorization: Bearer $token"
            -H "Content-Type: application/json"
            -d "{}"
    );
    let collections: Vec<String> = list_result
        .ok()
        .and_then(|json_str| serde_json::from_str::<serde_json::Value>(&json_str).ok())
        .and_then(|parsed| {
            parsed
                .get("collectionIds")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .filter(|name| name.starts_with("fractalbits-"))
                        .collect()
                })
        })
        .unwrap_or_default();

    for collection in collections {
        let docs_url = format!(
            "https://firestore.googleapis.com/v1/projects/{project_id}/databases/{database_id}/documents/{collection}"
        );
        let docs_result = run_fun!(
            curl -sf $docs_url -H "Authorization: Bearer $token"
        );
        if let Ok(json_str) = docs_result
            && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&json_str)
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

    info!("Firestore stale data cleared");
    Ok(())
}
