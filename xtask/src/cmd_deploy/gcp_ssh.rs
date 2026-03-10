use cmd_lib::*;
use std::io::Error;

/// Run a command on a GCP instance via IAP tunnel SSH.
pub fn gcp_run_command(
    instance_name: &str,
    zone: &str,
    project: &str,
    command: &str,
    description: &str,
) -> Result<(), Error> {
    info!("[{instance_name}] {description}");
    run_cmd!(
        gcloud compute ssh $instance_name
            --zone $zone
            --project $project
            --tunnel-through-iap
            --command $command
            --strict-host-key-checking=no 2>&1
    )
    .map_err(|e| Error::other(format!("SSH to {instance_name} failed: {e}")))
}

/// Get the private IP of a GCP instance.
pub fn get_instance_private_ip(
    instance_name: &str,
    zone: &str,
    project: &str,
) -> Result<String, Error> {
    let ip = run_fun!(
        gcloud compute instances describe $instance_name
            --zone $zone
            --project $project
            --format "get(networkInterfaces[0].networkIP)"
    )?;
    let ip = ip.trim().to_string();
    if ip.is_empty() {
        return Err(Error::other(format!(
            "No private IP for instance {instance_name}"
        )));
    }
    Ok(ip)
}

/// Wait for SSH to be reachable on a GCP instance via IAP tunnel.
pub fn wait_for_ssh_ready(instance_name: &str, zone: &str, project: &str) -> Result<(), Error> {
    info!("Waiting for SSH on {instance_name}...");
    for i in 1..=30 {
        let result = run_cmd!(
            gcloud compute ssh $instance_name
                --zone $zone
                --project $project
                --tunnel-through-iap
                --command "echo ready"
                --strict-host-key-checking=no --ssh-flag="-o ConnectTimeout=10" 2>/dev/null
        );
        if result.is_ok() {
            info!("SSH ready on {instance_name}");
            return Ok(());
        }
        if i % 5 == 0 {
            info!("SSH not ready on {instance_name}, attempt {i}/30...");
        }
        std::thread::sleep(std::time::Duration::from_secs(10));
    }
    Err(Error::other(format!(
        "Timed out waiting for SSH on {instance_name}"
    )))
}

/// Parse Terraform outputs from JSON format.
pub fn parse_terraform_outputs(
    terraform_dir: &str,
) -> Result<std::collections::HashMap<String, String>, Error> {
    let output = run_fun!(
        cd $terraform_dir;
        terraform output -json
    )?;

    let parsed: serde_json::Value =
        serde_json::from_str(&output).map_err(|e| Error::other(format!("JSON parse: {e}")))?;

    let mut outputs = std::collections::HashMap::new();
    if let Some(obj) = parsed.as_object() {
        for (key, val) in obj {
            if let Some(v) = val.get("value").and_then(|v| v.as_str()) {
                outputs.insert(key.clone(), v.to_string());
            }
        }
    }

    // Populate MIG instance names and IPs (BSS and API servers)
    let project_id = outputs.get("project_id").cloned().unwrap_or_default();
    let zone = outputs.get("zone_a").cloned().unwrap_or_default();
    if !project_id.is_empty() && !zone.is_empty() {
        populate_mig_instances(&mut outputs, &project_id, &zone, "bss")?;
        populate_mig_instances(&mut outputs, &project_id, &zone, "api")?;
    }

    Ok(outputs)
}

/// Query MIG instances and add their names/IPs to the outputs map.
/// Retries until instances are found (MIG instances with local SSDs take longer to provision).
fn populate_mig_instances(
    outputs: &mut std::collections::HashMap<String, String>,
    project_id: &str,
    zone: &str,
    prefix: &str,
) -> Result<(), Error> {
    let cluster_id = outputs.get("cluster_id").cloned().unwrap_or_default();
    let mig_name = format!("{prefix}-servers-{cluster_id}");

    let mut instance_list = String::new();
    for attempt in 0..12 {
        let result = run_fun!(
            gcloud compute instance-groups managed list-instances $mig_name
                --zone $zone
                --project $project_id
                --format "csv[no-heading](name)"
        );
        match result {
            Ok(list) if !list.trim().is_empty() => {
                instance_list = list;
                break;
            }
            _ => {
                if attempt == 0 {
                    info!("Waiting for {prefix} MIG instances to be provisioned...");
                }
                std::thread::sleep(std::time::Duration::from_secs(5));
            }
        }
    }

    for (i, line) in instance_list.lines().enumerate() {
        let instance_name = line.trim();
        if instance_name.is_empty() {
            continue;
        }
        outputs.insert(format!("{prefix}_{i}_name"), instance_name.to_string());

        // Get the private IP
        if let Ok(ip) = get_instance_private_ip(instance_name, zone, project_id) {
            outputs.insert(format!("{prefix}_{i}_ip"), ip);
        }
    }
    Ok(())
}
