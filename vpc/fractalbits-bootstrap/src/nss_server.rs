pub mod ebs;

use super::common::*;
use crate::config::BootstrapConfig;
use cmd_lib::*;
use rayon::prelude::*;
use std::io::{self, Error};

const BLOB_DRAM_MEM_PERCENT: f64 = 0.8;
const NSS_META_CACHE_SHARDS: usize = 256;

pub fn bootstrap(config: &BootstrapConfig, volume_id: &str, for_bench: bool) -> CmdResult {
    let mirrord_endpoint = config.endpoints.mirrord_endpoint.as_deref();
    let rss_ha_enabled = config.global.rss_ha_enabled;
    let meta_stack_testing = config.global.meta_stack_testing;

    install_rpms(&["nvme-cli", "mdadm"])?;
    if meta_stack_testing || for_bench {
        download_binaries(&["test_fractal_art", "rewrk_rpc"])?;
    }
    format_local_nvme_disks(false)?;
    download_binaries(&["nss_server", "nss_role_agent"])?;
    setup_configs(volume_id, "nss", mirrord_endpoint, rss_ha_enabled)?;

    // For meta stack testing, format NSS immediately (no root_server to trigger via SSM)
    if meta_stack_testing {
        let volume_dev = ebs::get_volume_dev(volume_id);
        ebs::format_nss_internal(&volume_dev)?;
    }

    Ok(())
}

fn setup_configs(
    volume_id: &str,
    service_name: &str,
    mirrord_endpoint: Option<&str>,
    rss_ha_enabled: bool,
) -> CmdResult {
    let volume_dev = ebs::get_volume_dev(volume_id);
    create_nss_config(&volume_dev)?;
    create_mirrord_config(&volume_dev)?;
    create_mount_unit(&volume_dev, "/data/ebs", "ext4")?;
    ebs::create_ebs_udev_rule(volume_id, "nss_role_agent")?;
    create_coredump_config()?;
    create_nss_role_agent_config(mirrord_endpoint, rss_ha_enabled)?;
    create_systemd_unit_file("nss_role_agent", false)?;
    create_systemd_unit_file("mirrord", false)?;
    create_systemd_unit_file(service_name, false)?;
    create_logrotate_for_stats()?;
    create_ena_irq_affinity_service()?;
    create_nvme_tuning_service()?;
    Ok(())
}

fn create_nss_config(volume_dev: &str) -> CmdResult {
    // Get total memory in kilobytes from /proc/meminfo
    let total_mem_kb_str = run_fun!(cat /proc/meminfo | grep MemTotal | awk r"{print $2}")?;
    let total_mem_kb = total_mem_kb_str
        .trim()
        .parse::<u64>()
        .map_err(|_| Error::other(format!("invalid total_mem_kb: {total_mem_kb_str}")))?;

    // Calculate total memory for blob_dram_kilo_bytes
    let blob_dram_kilo_bytes = (total_mem_kb as f64 * BLOB_DRAM_MEM_PERCENT) as u64;

    // Calculate fa_journal_segment_size based on EBS volume size
    let fa_journal_segment_size = ebs::calculate_fa_journal_segment_size(volume_dev)?;

    let num_cores = num_cpus()?;
    let net_worker_thread_count = num_cores / 2;
    let fa_thread_dataop_count = num_cores / 2;
    let fa_thread_count = fa_thread_dataop_count + 4;

    let config_content = format!(
        r##"working_dir = "/data"
server_port = 8088
health_port = 19999
net_worker_thread_count = {net_worker_thread_count}
fa_thread_count = {fa_thread_count}
fa_thread_dataop_count = {fa_thread_dataop_count}
blob_dram_kilo_bytes = {blob_dram_kilo_bytes}
fa_journal_segment_size = {fa_journal_segment_size}
log_level = "info"
mirrord_port = 9999
meta_cache_shards = {NSS_META_CACHE_SHARDS}
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$NSS_SERVER_CONFIG
    }?;
    Ok(())
}

fn create_mirrord_config(volume_dev: &str) -> CmdResult {
    let num_cores = run_fun!(nproc)?;
    // Calculate fa_journal_segment_size based on EBS volume size (same as nss_server)
    let fa_journal_segment_size = ebs::calculate_fa_journal_segment_size(volume_dev)?;
    let config_content = format!(
        r##"working_dir = "/data"
server_port = 9999
health_port = 19999
num_threads = {num_cores}
log_level = "info"
fa_journal_segment_size = {fa_journal_segment_size}
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$MIRRORD_CONFIG
    }?;
    Ok(())
}

fn create_nss_role_agent_config(mirrord_endpoint: Option<&str>, rss_ha_enabled: bool) -> CmdResult {
    let instance_id = get_instance_id()?;

    // Query DDB for RSS instance IPs
    let expected_rss_count = if rss_ha_enabled { 2 } else { 1 };
    let rss_ips = get_service_ips("root-server", expected_rss_count);
    let rss_addrs_toml = rss_ips
        .iter()
        .map(|ip| format!("\"{}:8088\"", ip))
        .collect::<Vec<_>>()
        .join(", ");

    let mirrord_configs = if let Some(mirrord_endpoint) = mirrord_endpoint {
        format!(
            r##"
mirrord_endpoint = "{mirrord_endpoint}"
mirrord_port = 9999
"##
        )
    } else {
        "".to_string()
    };
    let config_content = format!(
        r##"rss_addrs = [{rss_addrs_toml}]
rpc_timeout_seconds = 4
heartbeat_interval_seconds = 10
state_check_interval_seconds = 1
instance_id = "{instance_id}"
service_type = "unknown"
nss_port = 8088
rpc_server_port = 8077
restart_limit_burst = 3
restart_limit_interval_seconds = 600
{mirrord_configs}
"##
    );

    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$NSS_ROLE_AGENT_CONFIG
    }?;
    Ok(())
}
