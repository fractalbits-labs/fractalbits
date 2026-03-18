use crate::common::{BIN_PATH, DDB_SERVICE_DISCOVERY_TABLE, ETC_PATH};
use crate::config::BootstrapConfig;
use cmd_lib::*;
use std::io::Error;
use std::time::{Duration, Instant};

pub fn get_gcp_instance_id() -> FunResult {
    // Query GCP instance metadata service for the instance name
    run_fun!(
        curl -sf -H "Metadata-Flavor: Google"
            "http://metadata.google.internal/computeMetadata/v1/instance/name"
    )
}

pub fn get_gcp_private_ip() -> FunResult {
    run_fun!(
        curl -sf -H "Metadata-Flavor: Google"
            "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip"
    )
}

/// Get a GCP access token from the instance metadata service.
fn get_gcp_access_token() -> FunResult {
    run_fun!(
        curl -sf -H "Metadata-Flavor: Google"
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
            | jq -r ".access_token"
    )
}

/// Write a document to Firestore via REST API.
/// Used during bootstrap to initialize service discovery state before RSS starts.
pub fn firestore_put_document(
    config: &BootstrapConfig,
    collection: &str,
    doc_id: &str,
    fields_json: &str,
) -> CmdResult {
    let gcp = config.gcp.as_ref().ok_or_else(|| {
        Error::other("GCP config missing from bootstrap config for Firestore write")
    })?;
    let project_id = &gcp.project_id;
    let database_id = gcp.firestore_database.as_deref().unwrap_or("fractalbits");
    let token = get_gcp_access_token()?;
    let url = format!(
        "https://firestore.googleapis.com/v1/projects/{project_id}/databases/{database_id}/documents/{collection}/{doc_id}"
    );

    run_cmd! {
        curl -sf -X PATCH $url
            -H "Authorization: Bearer $token"
            -H "Content-Type: application/json"
            -d $fields_json
    }
}

pub(crate) fn create_firestore_register_and_deregister_service(
    config: &BootstrapConfig,
    service_id: &str,
) -> CmdResult {
    create_firestore_register_service(config, service_id)?;
    create_firestore_deregister_service(config, service_id)?;
    Ok(())
}

fn create_firestore_register_service(config: &BootstrapConfig, service_id: &str) -> CmdResult {
    let gcp = config.gcp.as_ref().ok_or_else(|| {
        Error::other("GCP config missing from bootstrap config for Firestore registration")
    })?;
    let project_id = &gcp.project_id;
    let database_id = gcp.firestore_database.as_deref().unwrap_or("fractalbits");
    let firestore_register_script = format!("{BIN_PATH}firestore-register.sh");

    let systemd_unit_content = format!(
        r##"[Unit]
Description=Firestore Service Registration
After=network-online.target

[Service]
Type=oneshot
ExecStart={firestore_register_script}

[Install]
WantedBy=multi-user.target
"##
    );

    // Use subcollection: fractalbits-service-discovery/{service_id}/instances/{instance_id}
    // with a field "ip" containing the private IP
    let register_script_content = format!(
        r###"#!/bin/bash
set -e
service_id={service_id}
project_id="{project_id}"
database_id="{database_id}"
instance_id=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/name")
private_ip=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip")

echo "Registering itself ($instance_id,$private_ip) to Firestore with service_id $service_id" >&2

MAX_RETRIES=30
retry_count=0
success=false

while [ $retry_count -lt $MAX_RETRIES ] && [ "$success" = "false" ]; do
    retry_count=$((retry_count + 1))
    token=$(curl -sf -H "Metadata-Flavor: Google" \
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
        | jq -r ".access_token")

    url="https://firestore.googleapis.com/v1/projects/$project_id/databases/$database_id/documents/{DDB_SERVICE_DISCOVERY_TABLE}-$service_id/$instance_id"
    fields_json=$(printf '{{"fields":{{"ip":{{"stringValue":"%s"}}}}}}' "$private_ip")

    if curl -sf -X PATCH "$url" \
        -H "Authorization: Bearer $token" \
        -H "Content-Type: application/json" \
        -d "$fields_json" >/dev/null; then
        echo "Registered service on attempt $retry_count" >&2
        success=true
    else
        echo "Registration failed on attempt $retry_count, retrying..." >&2
        sleep 2
    fi
done

if [ "$success" = "false" ]; then
    echo "FATAL: Failed to register service $service_id after $MAX_RETRIES attempts" >&2
    exit 1
fi

echo "Done" >&2
"###
    );

    run_cmd! {
        echo $register_script_content > $firestore_register_script;
        chmod +x $firestore_register_script;

        echo $systemd_unit_content > ${ETC_PATH}firestore-register.service;
        systemctl enable --now ${ETC_PATH}firestore-register.service;
    }?;
    Ok(())
}

fn create_firestore_deregister_service(config: &BootstrapConfig, service_id: &str) -> CmdResult {
    let gcp = config.gcp.as_ref().ok_or_else(|| {
        Error::other("GCP config missing from bootstrap config for Firestore deregistration")
    })?;
    let project_id = &gcp.project_id;
    let database_id = gcp.firestore_database.as_deref().unwrap_or("fractalbits");
    let firestore_deregister_script = format!("{BIN_PATH}firestore-deregister.sh");

    let systemd_unit_content = format!(
        r##"[Unit]
Description=Firestore Service Deregistration
After=network-online.target
Before=reboot.target halt.target poweroff.target kexec.target

DefaultDependencies=no

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart={firestore_deregister_script}

[Install]
WantedBy=reboot.target halt.target poweroff.target kexec.target
"##
    );

    let deregister_script_content = format!(
        r###"#!/bin/bash
set -e
service_id={service_id}
project_id="{project_id}"
database_id="{database_id}"
instance_id=$(curl -sf -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/name")

echo "Deregistering itself ($instance_id) from Firestore with service_id $service_id" >&2

token=$(curl -sf -H "Metadata-Flavor: Google" \
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token" \
    | jq -r ".access_token")

url="https://firestore.googleapis.com/v1/projects/$project_id/databases/$database_id/documents/{DDB_SERVICE_DISCOVERY_TABLE}-$service_id/$instance_id"

curl -sf -X DELETE "$url" \
    -H "Authorization: Bearer $token" 2>/dev/null || true

echo "Done" >&2
"###
    );

    run_cmd! {
        echo $deregister_script_content > $firestore_deregister_script;
        chmod +x $firestore_deregister_script;

        echo $systemd_unit_content > ${ETC_PATH}firestore-deregister.service;
        systemctl enable ${ETC_PATH}firestore-deregister.service;
    }?;
    Ok(())
}

pub(crate) fn get_service_ips_firestore(
    config: &BootstrapConfig,
    service_id: &str,
    expected_min_count: usize,
) -> Vec<String> {
    info!("Waiting for {expected_min_count} {service_id} service(s) via Firestore");
    let gcp = config
        .gcp
        .as_ref()
        .expect("GCP config required for Firestore service discovery");
    let project_id = &gcp.project_id;
    let database_id = gcp.firestore_database.as_deref().unwrap_or("fractalbits");

    let start_time = Instant::now();
    let timeout = Duration::from_secs(600);
    let collection = format!("{DDB_SERVICE_DISCOVERY_TABLE}-{service_id}");

    loop {
        if start_time.elapsed() > timeout {
            cmd_die!("Timeout waiting for {service_id} service(s) via Firestore");
        }

        let token_result = get_gcp_access_token();
        if let Ok(token) = token_result {
            let url = format!(
                "https://firestore.googleapis.com/v1/projects/{project_id}/databases/{database_id}/documents/{collection}"
            );

            let res = run_fun!(
                curl -sf $url
                    -H "Authorization: Bearer $token"
            );

            if let Ok(output) = res
                && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&output)
            {
                let ips: Vec<String> = parsed["documents"]
                    .as_array()
                    .map(|docs| {
                        docs.iter()
                            .filter_map(|doc| {
                                doc["fields"]["ip"]["stringValue"]
                                    .as_str()
                                    .map(|s| s.to_string())
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                if ips.len() >= expected_min_count {
                    info!("Found a list of {service_id} services via Firestore: {ips:?}");
                    return ips;
                }
                info!(
                    "Found {} of {} {service_id} services, waiting...",
                    ips.len(),
                    expected_min_count
                );
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
}
