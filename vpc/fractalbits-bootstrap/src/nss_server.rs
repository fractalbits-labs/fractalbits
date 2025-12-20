pub mod ebs_journal;
pub mod nvme_journal;

use super::common::*;
use crate::config::{BootstrapConfig, JournalType};
use cmd_lib::*;
use rayon::prelude::*;
use std::io::{self, Error};

const BLOB_DRAM_MEM_PERCENT: f64 = 0.8;
const NSS_META_CACHE_SHARDS: usize = 256;

pub fn bootstrap(config: &BootstrapConfig, volume_id: Option<&str>, for_bench: bool) -> CmdResult {
    let mirrord_endpoint = config.endpoints.mirrord_endpoint.as_deref();
    let rss_ha_enabled = config.global.rss_ha_enabled;
    let meta_stack_testing = config.global.meta_stack_testing;
    let journal_type = config.global.journal_type;

    install_rpms(&["nvme-cli", "mdadm"])?;
    if meta_stack_testing || for_bench {
        download_binaries(&["test_fractal_art", "rewrk_rpc"])?;
    }
    format_local_nvme_disks(false)?;
    download_binaries(&["nss_server", "nss_role_agent"])?;

    setup_configs(
        journal_type,
        volume_id,
        "nss",
        mirrord_endpoint,
        rss_ha_enabled,
    )?;

    // For meta stack testing, format NSS immediately (no root_server to trigger via SSM)
    if meta_stack_testing {
        let volume_id =
            volume_id.ok_or_else(|| Error::other("volume_id required for ebs journal type"))?;
        let volume_dev = ebs_journal::get_volume_dev(volume_id);
        ebs_journal::format_internal(&volume_dev)?;
        return Ok(());
    }

    match journal_type {
        JournalType::Nvme => {
            nvme_journal::format()?;
            // NVMe mode: root_server will trigger nss_role_agent via SSM after metadata VG is configured
        }
        JournalType::Ebs => {
            // EBS mode: format called from main() when triggered via SSM
            // service started by udev rule when volume is attached
        }
    }

    Ok(())
}

fn setup_configs(
    journal_type: JournalType,
    volume_id: Option<&str>,
    service_name: &str,
    mirrord_endpoint: Option<&str>,
    rss_ha_enabled: bool,
) -> CmdResult {
    // Journal-type specific config paths
    let (volume_dev, shared_dir) = match journal_type {
        JournalType::Ebs => {
            let vid = volume_id.ok_or_else(|| Error::other("volume_id required for EBS"))?;
            (Some(ebs_journal::get_volume_dev(vid)), "ebs")
        }
        JournalType::Nvme => (None, "local/journal"),
    };

    create_nss_config(volume_dev.as_deref(), shared_dir)?;
    create_mirrord_config(volume_dev.as_deref(), shared_dir)?;

    // EBS-specific: mount unit and udev rule
    if let (JournalType::Ebs, Some(vid), Some(vdev)) = (journal_type, volume_id, &volume_dev) {
        create_mount_unit(vdev, "/data/ebs", "ext4")?;
        ebs_journal::create_ebs_udev_rule(vid, "nss_role_agent")?;
    }

    // Common configs
    create_coredump_config()?;
    create_nss_role_agent_config(mirrord_endpoint, rss_ha_enabled)?;
    create_systemd_unit_file("nss_role_agent", false)?;

    // Systemd units - NVMe needs journal_type for local mount dependency
    create_systemd_unit_file_with_journal_type("mirrord", false, Some(journal_type))?;
    create_systemd_unit_file_with_journal_type(service_name, false, Some(journal_type))?;

    create_logrotate_for_stats()?;
    create_ena_irq_affinity_service()?;
    create_nvme_tuning_service()?;
    Ok(())
}

fn create_nss_config(volume_dev: Option<&str>, shared_dir: &str) -> CmdResult {
    // Get total memory in kilobytes from /proc/meminfo
    let total_mem_kb_str = run_fun!(cat /proc/meminfo | grep MemTotal | awk r"{print $2}")?;
    let total_mem_kb = total_mem_kb_str
        .trim()
        .parse::<u64>()
        .map_err(|_| Error::other(format!("invalid total_mem_kb: {total_mem_kb_str}")))?;

    // Calculate total memory for blob_dram_kilo_bytes
    let blob_dram_kilo_bytes = (total_mem_kb as f64 * BLOB_DRAM_MEM_PERCENT) as u64;

    // Calculate fa_journal_segment_size based on storage device size
    let fa_journal_segment_size = if let Some(dev) = volume_dev {
        ebs_journal::calculate_fa_journal_segment_size(dev)?
    } else {
        nvme_journal::calculate_fa_journal_segment_size()?
    };

    let num_cores = num_cpus()?;
    let net_worker_thread_count = num_cores / 2;
    let fa_thread_dataop_count = num_cores / 2;
    let fa_thread_count = fa_thread_dataop_count + 4;

    let config_content = format!(
        r##"working_dir = "/data"
shared_dir = "{shared_dir}"
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

fn create_mirrord_config(volume_dev: Option<&str>, shared_dir: &str) -> CmdResult {
    let num_cores = run_fun!(nproc)?;
    // Calculate fa_journal_segment_size based on storage device size
    let fa_journal_segment_size = if let Some(dev) = volume_dev {
        ebs_journal::calculate_fa_journal_segment_size(dev)?
    } else {
        nvme_journal::calculate_fa_journal_segment_size()?
    };
    let config_content = format!(
        r##"working_dir = "/data"
shared_dir = "{shared_dir}"
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

/// Wait for /data/local mount, create common directories, and run nss_server format.
/// If `create_journal_dir` is true, also creates /data/local/journal (for nvme mode).
pub(crate) fn wait_and_format_nss(create_journal_dir: bool) -> CmdResult {
    let mut wait_secs = 0;
    while run_cmd!(mountpoint -q "/data/local").is_err() {
        wait_secs += 1;
        info!("Waiting for /data/local to be mounted ({wait_secs}s)");
        std::thread::sleep(std::time::Duration::from_secs(1));
        if wait_secs >= 120 {
            cmd_die!("Timeout when waiting for /data/local to be mounted (120s)");
        }
    }

    if create_journal_dir {
        run_cmd! {
            info "Creating directories for nss_server";
            mkdir -p /data/local/journal;
            mkdir -p /data/local/stats;
            mkdir -p /data/local/meta_cache/blobs;
        }?;
    } else {
        run_cmd! {
            info "Creating directories for nss_server";
            mkdir -p /data/local/stats;
            mkdir -p /data/local/meta_cache/blobs;
        }?;
    }

    info!(
        "Creating {} meta cache shard directories in parallel",
        NSS_META_CACHE_SHARDS
    );
    let shards: Vec<usize> = (0..NSS_META_CACHE_SHARDS).collect();
    shards.par_iter().try_for_each(|&i| {
        let shard_dir = format!("/data/local/meta_cache/blobs/{}", i);
        std::fs::create_dir(&shard_dir)
            .map_err(|e| Error::other(format!("Failed to create {}: {}", shard_dir, e)))
    })?;

    run_cmd! {
        info "Syncing file system changes";
        sync;
    }?;

    run_cmd! {
        info "Running format for nss_server";
        /opt/fractalbits/bin/nss_server format -c ${ETC_PATH}${NSS_SERVER_CONFIG};
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
