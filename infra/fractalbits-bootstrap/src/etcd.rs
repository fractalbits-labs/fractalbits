use crate::common::{BIN_PATH, ETC_PATH};
use crate::config::{BootstrapConfig, DeployTarget};
use cmd_lib::*;
use std::io::Error;
use std::time::{Duration, Instant};

pub fn get_etcd_endpoints(config: &BootstrapConfig) -> FunResult {
    // First try static endpoints from config (for on-prem/static etcd)
    if let Some(etcd_config) = &config.etcd
        && let Some(endpoints) = &etcd_config.endpoints
    {
        return Ok(endpoints.clone());
    }

    // Fall back to workflow barrier discovery (for dynamic BSS etcd cluster)
    let barrier = crate::workflow::WorkflowBarrier::from_config(
        config,
        crate::workflow::WorkflowServiceType::Bss,
    )?;
    let completions = barrier
        .get_stage_completions(&crate::workflow::stages::ETCD_NODES_REGISTERED.key_name())?;
    let bss_ips = crate::workflow::StageCompletion::extract_metadata_field(&completions, "ip");

    if bss_ips.is_empty() {
        return Err(Error::other(
            "No BSS nodes registered in workflow for etcd endpoints",
        ));
    }

    Ok(bss_ips
        .iter()
        .map(|ip| format!("http://{ip}:2379"))
        .collect::<Vec<_>>()
        .join(","))
}

pub(crate) fn create_etcd_register_and_deregister_service(
    config: &BootstrapConfig,
    service_id: &str,
) -> CmdResult {
    let endpoints = get_etcd_endpoints(config)?;
    create_etcd_register_service(config, &endpoints, service_id)?;
    create_etcd_deregister_service(config, &endpoints, service_id)?;
    Ok(())
}

fn create_etcd_register_service(
    config: &BootstrapConfig,
    endpoints: &str,
    service_id: &str,
) -> CmdResult {
    let etcd_register_script = format!("{BIN_PATH}etcd-register.sh");

    let get_instance_id_cmd = match config.global.deploy_target {
        DeployTarget::OnPrem => "hostname".to_string(),
        DeployTarget::Aws => "ec2-metadata -i | awk '{print $2}'".to_string(),
        DeployTarget::Gcp => "curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/name".to_string(),
    };

    let get_private_ip_cmd = match config.global.deploy_target {
        DeployTarget::OnPrem => "hostname -I | awk '{print $1}'".to_string(),
        DeployTarget::Aws => "ec2-metadata -o | awk '{print $2}'".to_string(),
        DeployTarget::Gcp => "curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip".to_string(),
    };

    let systemd_unit_content = format!(
        r##"[Unit]
Description=etcd Service Registration
After=network-online.target etcd.service

[Service]
Type=oneshot
ExecStart={etcd_register_script}

[Install]
WantedBy=multi-user.target
"##
    );

    // Use individual keys per instance: /fractalbits-service-discovery/<service_id>/<instance_id> -> <ip>
    // This allows simple put/del operations and prefix scanning for discovery
    let register_script_content = format!(
        r###"#!/bin/bash
set -e
service_id={service_id}
endpoints="{endpoints}"
instance_id=$({get_instance_id_cmd})
private_ip=$({get_private_ip_cmd})
etcd_key="/fractalbits-service-discovery/$service_id/$instance_id"

echo "Registering itself ($instance_id,$private_ip) to etcd at $etcd_key" >&2

MAX_RETRIES=30
retry_count=0
success=false

while [ $retry_count -lt $MAX_RETRIES ] && [ "$success" = "false" ]; do
    retry_count=$((retry_count + 1))

    if {BIN_PATH}etcdctl --endpoints="$endpoints" put "$etcd_key" "$private_ip" 2>/dev/null; then
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
        echo $register_script_content > $etcd_register_script;
        chmod +x $etcd_register_script;

        echo $systemd_unit_content > ${ETC_PATH}etcd-register.service;
        systemctl enable --now ${ETC_PATH}etcd-register.service;
    }?;
    Ok(())
}

fn create_etcd_deregister_service(
    config: &BootstrapConfig,
    endpoints: &str,
    service_id: &str,
) -> CmdResult {
    let etcd_deregister_script = format!("{BIN_PATH}etcd-deregister.sh");

    let get_instance_id_cmd = match config.global.deploy_target {
        DeployTarget::OnPrem => "hostname".to_string(),
        DeployTarget::Aws => "ec2-metadata -i | awk '{print $2}'".to_string(),
        DeployTarget::Gcp => "curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/name".to_string(),
    };

    let systemd_unit_content = format!(
        r##"[Unit]
Description=etcd Service Deregistration
After=network-online.target
Before=reboot.target halt.target poweroff.target kexec.target

DefaultDependencies=no

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart={etcd_deregister_script}

[Install]
WantedBy=reboot.target halt.target poweroff.target kexec.target
"##
    );

    // Use individual keys per instance - just delete the key
    let deregister_script_content = format!(
        r###"#!/bin/bash
set -e
service_id={service_id}
endpoints="{endpoints}"
instance_id=$({get_instance_id_cmd})
etcd_key="/fractalbits-service-discovery/$service_id/$instance_id"

echo "Deregistering itself ($instance_id) from etcd at $etcd_key" >&2
{BIN_PATH}etcdctl --endpoints="$endpoints" del "$etcd_key" 2>/dev/null || true
echo "Done" >&2
"###
    );

    run_cmd! {
        echo $deregister_script_content > $etcd_deregister_script;
        chmod +x $etcd_deregister_script;

        echo $systemd_unit_content > ${ETC_PATH}etcd-deregister.service;
        systemctl enable ${ETC_PATH}etcd-deregister.service;
    }?;
    Ok(())
}

pub(crate) fn get_service_ips_etcd(
    endpoints: &str,
    service_id: &str,
    expected_min_count: usize,
) -> Vec<String> {
    info!("Waiting for {expected_min_count} {service_id} service(s) via etcd");
    let start_time = Instant::now();
    let timeout = Duration::from_secs(300);
    let etcdctl = format!("{BIN_PATH}etcdctl");
    // Use prefix scan: /fractalbits-service-discovery/<service_id>/<instance_id> -> <ip>
    let etcd_prefix = format!("/fractalbits-service-discovery/{service_id}/");
    loop {
        if start_time.elapsed() > timeout {
            cmd_die!("Timeout waiting for ${service_id} service(s) via etcd");
        }

        let res: Result<String, _> = run_fun! {
            $etcdctl --endpoints=$endpoints get $etcd_prefix --prefix --print-value-only 2>/dev/null
        };

        match res {
            Ok(ref output) if !output.trim().is_empty() => {
                // Each line is an IP address
                let ips: Vec<String> = output
                    .lines()
                    .filter(|line| !line.is_empty())
                    .map(|s| s.to_string())
                    .collect();

                if ips.len() >= expected_min_count {
                    info!("Found a list of {service_id} services via etcd: {ips:?}");
                    return ips;
                }
                info!(
                    "Found {} of {} {service_id} services, waiting...",
                    ips.len(),
                    expected_min_count
                );
            }
            _ => {}
        }
        std::thread::sleep(std::time::Duration::from_secs(2));
    }
}
