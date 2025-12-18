use super::common::*;
use crate::config::BootstrapConfig;
use cmd_lib::*;
use rayon::prelude::*;
use std::io::Error;

const BLOB_DRAM_MEM_PERCENT: f64 = 0.8;
// AWS EBS has 500 IOPS/GB limit, so we need to have 20GB
// space for 10K IOPS. but journal size is much smaller.
const EBS_SPACE_PERCENT: f64 = 0.2;

const NSS_META_CACHE_SHARDS: usize = 256;

/// Calculate fa_journal_segment_size based on EBS volume size
fn calculate_fa_journal_segment_size(volume_dev: &str) -> Result<u64, Error> {
    // Get total size of volume_dev in bytes
    let ebs_blockdev_size_str = run_fun!(blockdev --getsize64 ${volume_dev})?;
    let ebs_blockdev_size = ebs_blockdev_size_str.trim().parse::<u64>().map_err(|_| {
        Error::other(format!(
            "invalid ebs blockdev size: {ebs_blockdev_size_str}"
        ))
    })?;
    let ebs_blockdev_mb = ebs_blockdev_size / 1024 / 1024;
    let fa_journal_segment_size = (ebs_blockdev_mb as f64 * EBS_SPACE_PERCENT) as u64 * 1024 * 1024;
    Ok(fa_journal_segment_size)
}

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
        let volume_dev = get_volume_dev(volume_id);
        format_nss(volume_dev)?;
    }

    Ok(())
}

fn setup_configs(
    volume_id: &str,
    service_name: &str,
    mirrord_endpoint: Option<&str>,
    rss_ha_enabled: bool,
) -> CmdResult {
    let volume_dev = get_volume_dev(volume_id);
    create_nss_config(&volume_dev)?;
    create_mirrord_config(&volume_dev)?;
    create_mount_unit(&volume_dev, "/data/ebs", "ext4")?;
    create_ebs_udev_rule(volume_id, "nss_role_agent")?;
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
    let fa_journal_segment_size = calculate_fa_journal_segment_size(volume_dev)?;

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
    let fa_journal_segment_size = calculate_fa_journal_segment_size(volume_dev)?;
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

fn create_ebs_udev_rule(volume_id: &str, service_name: &str) -> CmdResult {
    let content = format!(
        r##"KERNEL=="nvme*n*", SUBSYSTEM=="block", ENV{{ID_SERIAL}}=="Amazon_Elastic_Block_Store_{}_1", TAG+="systemd", ENV{{SYSTEMD_WANTS}}="{service_name}.service""##,
        volume_id.replace("-", "")
    );
    run_cmd! {
        echo $content > $ETC_PATH/99-ebs.rules;
        ln -s $ETC_PATH/99-ebs.rules /etc/udev/rules.d/;
    }?;

    Ok(())
}

pub fn format_nss(ebs_dev: String) -> CmdResult {
    run_cmd! {
        info "Disabling udev rules for EBS";
        ln -sf /dev/null /etc/udev/rules.d/99-ebs.rules;

        info "Formatting $ebs_dev to ext4 file system";
        mkfs.ext4 -O bigalloc -C 16384 $ebs_dev &>/dev/null;

        info "Mounting $ebs_dev to /data/ebs";
        mkdir -p /data/ebs;
        mount $ebs_dev /data/ebs;
    }?;

    let mut wait_secs = 0;
    while run_cmd!(mountpoint -q "/data/local").is_err() {
        wait_secs += 1;
        info!("Waiting for /data/local to be mounted ({wait_secs}s)");
        std::thread::sleep(std::time::Duration::from_secs(1));
        if wait_secs >= 120 {
            cmd_die!("Timeout when waiting for /data/local to be mounted (120s)");
        }
    }

    run_cmd! {
        info "Creating directories for nss_server";
        mkdir -p /data/local/stats;
        mkdir -p /data/local/meta_cache/blobs;
    }?;

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

    run_cmd! {
        info "Enabling udev rules for EBS";
        ln -sf /opt/fractalbits/etc/99-ebs.rules /etc/udev/rules.d/99-ebs.rules;
        udevadm control --reload-rules;
        udevadm trigger;

        info "${ebs_dev} is formatted successfully.";
    }?;

    Ok(())
}
