pub mod ebs_journal;
pub mod nvme_journal;

use super::common::*;
use crate::config::{BootstrapConfig, DeployTarget, JournalType};
use crate::workflow::{WorkflowBarrier, WorkflowServiceType, stages, timeouts};
use cmd_lib::*;
use rayon::prelude::*;
use std::io::Error;

const BLOB_DRAM_MEM_PERCENT: f64 = 0.8;
const NSS_META_CACHE_SHARDS: usize = 256;

pub fn bootstrap(config: &BootstrapConfig, volume_id: Option<&str>, for_bench: bool) -> CmdResult {
    let meta_stack_testing = config.global.meta_stack_testing;
    let journal_type = config.global.journal_type;

    // Determine the peer's endpoint for this NSS instance
    // For HA failover to work, each NSS needs to know the OTHER node's IP as mirrord_endpoint
    let mirrord_endpoint: Option<&str> = if journal_type == JournalType::Nvme {
        let resources = config.get_resources();
        let instance_id = get_instance_id_from_config(config)?;
        let is_standby = resources.nss_b_id.as_ref() == Some(&instance_id);

        if is_standby {
            // nss-B (standby): peer is nss-A, use nss_endpoint as mirrord target for failover
            Some(&config.endpoints.nss_endpoint)
        } else {
            // nss-A (active): peer is nss-B, use mirrord_endpoint
            config.endpoints.mirrord_endpoint.as_deref()
        }
    } else {
        // Non-NVMe journal: no HA, no mirrord
        None
    };

    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Nss)?;

    // Complete instances-ready stage
    barrier.complete_stage(stages::INSTANCES_READY, None)?;

    install_rpms(&["nvme-cli", "mdadm"])?;
    if meta_stack_testing || for_bench {
        let _ = download_binaries(config, &["test_fractal_art", "rewrk_rpc"]);
    }
    format_local_nvme_disks(false)?;

    let mut binaries = vec!["nss_server", "nss_role_agent"];
    if config.is_etcd_backend() {
        binaries.push("etcdctl");
    }
    // Download mirrord for NVMe journal type (active/standby mode)
    if journal_type == JournalType::Nvme {
        binaries.push("mirrord");
    }
    download_binaries(config, &binaries)?;

    // When using etcd backend, wait for etcd cluster to be ready first
    if config.is_etcd_backend() {
        info!("Waiting for etcd cluster to be ready...");
        barrier.wait_for_global(stages::ETCD_READY, timeouts::ETCD_READY)?;
        info!("etcd cluster is ready");
    }

    // Wait for RSS to initialize - RSS will have registered with service discovery by then
    // This must happen before setup_configs because create_nss_role_agent_config needs RSS IPs
    info!("Waiting for RSS to initialize...");
    barrier.wait_for_global(stages::RSS_INITIALIZED, timeouts::RSS_INITIALIZED)?;

    setup_configs(config, journal_type, volume_id, "nss", mirrord_endpoint)?;

    // Format journal based on type
    match journal_type {
        JournalType::Nvme => {
            nvme_journal::format()?;
        }
        JournalType::Ebs => {
            let volume_id =
                volume_id.ok_or_else(|| Error::other("volume_id required for ebs journal type"))?;
            ebs_journal::format_with_volume_id(volume_id)?;
        }
    }

    // Signal that formatting is complete
    barrier.complete_stage(stages::NSS_FORMATTED, None)?;

    // For NVMe journal type, coordinate active/standby startup
    // Standby (nss_b) must start mirrord first, then active (nss_a) can start nss_server
    if journal_type == JournalType::Nvme {
        let resources = config.get_resources();
        let instance_id = get_instance_id_from_config(config)?;
        let is_standby = resources.nss_b_id.as_ref() == Some(&instance_id);

        if is_standby {
            // Standby: start mirrord first
            info!("Starting as standby NSS (mirrord)");
            run_cmd!(systemctl start nss_role_agent.service)?;

            // Wait for mirrord to be ready
            wait_for_service_ready("mirrord", 9999, 120)?;

            // Signal that mirrord is ready
            barrier.complete_stage(stages::MIRRORD_READY, None)?;
            info!("Mirrord is ready, signaled MIRRORD_READY");

            // Complete services-ready stage
            barrier.complete_stage(stages::SERVICES_READY, None)?;
        } else {
            // Active: wait for mirrord to be ready first
            info!("Starting as active NSS, waiting for mirrord to be ready...");
            barrier.wait_for_nodes(stages::MIRRORD_READY, 1, timeouts::MIRRORD_READY)?;
            info!("Mirrord is ready, starting nss_role_agent");

            run_cmd!(systemctl start nss_role_agent.service)?;

            // Wait for nss_server to be ready before signaling
            wait_for_service_ready("nss_server", 8088, 120)?;

            // Signal that journal is ready and nss_server is accepting connections
            barrier.complete_stage(stages::NSS_JOURNAL_READY, None)?;

            // Complete services-ready stage
            barrier.complete_stage(stages::SERVICES_READY, None)?;
        }
    } else {
        // EBS journal type: no active/standby coordination needed
        run_cmd!(systemctl start nss_role_agent.service)?;

        // Wait for nss_server to be ready before signaling
        wait_for_service_ready("nss_server", 8088, 120)?;

        // Signal that journal is ready and nss_server is accepting connections
        barrier.complete_stage(stages::NSS_JOURNAL_READY, None)?;

        // Complete services-ready stage
        barrier.complete_stage(stages::SERVICES_READY, None)?;
    }

    Ok(())
}

fn setup_configs(
    config: &BootstrapConfig,
    journal_type: JournalType,
    volume_id: Option<&str>,
    service_name: &str,
    mirrord_endpoint: Option<&str>,
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

    // EBS-specific: mount unit
    if let (JournalType::Ebs, Some(vdev)) = (journal_type, &volume_dev) {
        create_mount_unit(vdev, "/data/ebs", "ext4")?;
    }

    // Common configs
    create_coredump_config()?;
    create_nss_role_agent_config(config, mirrord_endpoint)?;
    create_systemd_unit_file("nss_role_agent", false)?;

    // Systemd units - NVMe needs journal_type for local mount dependency
    create_systemd_unit_file_with_journal_type("mirrord", false, Some(journal_type))?;
    create_systemd_unit_file_with_journal_type(service_name, false, Some(journal_type))?;

    create_logrotate_for_stats()?;
    if config.global.deploy_target == DeployTarget::Aws {
        create_ena_irq_affinity_service()?;
    }
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

/// Create common directories and run nss_server format.
/// If `create_journal_dir` is true, also creates /data/local/journal (for nvme mode).
/// Note: /data/local is already mounted by format_local_nvme_disks() earlier in bootstrap.
pub(crate) fn format_nss(create_journal_dir: bool) -> CmdResult {
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

fn create_nss_role_agent_config(
    config: &BootstrapConfig,
    mirrord_endpoint: Option<&str>,
) -> CmdResult {
    let rss_ha_enabled = config.global.rss_ha_enabled;
    let instance_id = get_instance_id_from_config(config)?;
    let private_ip = get_private_ip()?;
    let nss_port = 8088;

    // Query service discovery for RSS instance IPs
    let expected_rss_count = if rss_ha_enabled { 2 } else { 1 };
    let rss_ips = get_service_ips_with_backend(config, "root-server", expected_rss_count);
    let rss_addrs_toml = rss_ips
        .iter()
        .map(|ip| format!("\"{}:8088\"", ip))
        .collect::<Vec<_>>()
        .join(", ");

    // mirrord_endpoint is only set in HA mode (NVMe journal with active/standby)
    let mirrord_endpoint_line = mirrord_endpoint
        .map(|ep| format!("mirrord_endpoint = \"{ep}\"\n"))
        .unwrap_or_default();

    let config_content = format!(
        r##"# NSS Role Agent Configuration
# Role is fetched from RSS at startup, not configured here

rss_addrs = [{rss_addrs_toml}]
instance_id = "{instance_id}"
network_address = "{private_ip}:{nss_port}"
{mirrord_endpoint_line}"##
    );

    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$NSS_ROLE_AGENT_CONFIG
    }?;
    Ok(())
}
