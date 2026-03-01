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

pub fn bootstrap(
    config: &BootstrapConfig,
    volume_id: Option<&str>,
    journal_uuid: Option<&str>,
    for_bench: bool,
) -> CmdResult {
    let meta_stack_testing = config.global.meta_stack_testing;
    let journal_type = config.global.journal_type;

    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Nss)?;

    // Complete instances-ready stage
    barrier.complete_stage(stages::INSTANCES_READY, None)?;

    install_packages(&["nvme-cli", "mdadm"])?;
    if meta_stack_testing || for_bench {
        let _ = download_binaries(config, &["rewrk_rpc"]);
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

    if !meta_stack_testing {
        // Wait for RSS to initialize - RSS will have registered with service discovery by then
        // This must happen before setup_configs because create_nss_role_agent_config needs RSS IPs
        info!("Waiting for RSS to initialize...");
        barrier.wait_for_global(stages::RSS_INITIALIZED, timeouts::RSS_INITIALIZED)?;
    } else {
        info!("Meta-stack testing mode: skipping RSS wait");
    }

    // Determine if this node is the EBS HA standby (nss-B with EBS journal)
    // EBS standby skips format/mount since it doesn't have the volume attached
    let resources = config.get_resources();
    let instance_id = get_instance_id_from_config(config)?;
    let is_standby = resources.nss_b_id.as_ref() == Some(&instance_id);
    let is_ha_mode = resources.nss_b_id.is_some();
    let is_ebs_standby = journal_type == JournalType::Ebs && is_standby;

    setup_configs(config, journal_type, volume_id, journal_uuid, "nss")?;

    if is_ebs_standby {
        // EBS HA standby: skip format/mount entirely, start role_agent idle
        info!("Starting as EBS HA standby NSS (idle)");

        // Create local directories (stats, meta_cache) needed when standby becomes active
        prepare_local_dirs()?;

        // Signal formatting complete (standby has nothing to format)
        barrier.complete_stage(stages::NSS_FORMATTED, None)?;

        // Start role_agent in standby mode (it will be idle)
        run_cmd!(systemctl start nss_role_agent.service)?;

        // Complete services-ready stage
        barrier.complete_stage(stages::SERVICES_READY, None)?;

        return Ok(());
    }

    // Format journal based on type (active or solo nodes only)
    match journal_type {
        JournalType::Nvme => {
            nvme_journal::format()?;
        }
        JournalType::Ebs => {
            let volume_id =
                volume_id.ok_or_else(|| Error::other("volume_id required for ebs journal type"))?;
            let journal_uuid = journal_uuid
                .ok_or_else(|| Error::other("journal_uuid required for ebs journal type"))?;
            ebs_journal::format_with_volume_id(volume_id, journal_uuid)?;
        }
    }

    // Signal that formatting is complete
    barrier.complete_stage(stages::NSS_FORMATTED, None)?;

    // For NVMe journal type, coordinate active/standby startup
    // Standby (nss_b) must start mirrord first, then active (nss_a) can start nss_server
    // In solo mode (no nss_b), just start nss_server directly without mirrord coordination
    if journal_type == JournalType::Nvme {
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
        } else if !is_ha_mode {
            // Solo mode: no mirrord coordination needed, just start nss_server directly
            info!("Starting as solo NSS (no mirrord coordination)");

            if !meta_stack_testing {
                info!("Waiting for metadata VG configuration...");
                barrier.wait_for_global(stages::METADATA_VG_READY, timeouts::METADATA_VG_READY)?;
            }

            run_cmd!(systemctl start nss_role_agent.service)?;

            // Wait for nss_server to be ready before signaling
            wait_for_service_ready("nss_server", 8088, 360)?;

            // Signal that journal is ready and nss_server is accepting connections
            barrier.complete_stage(stages::NSS_JOURNAL_READY, None)?;

            // Complete services-ready stage
            barrier.complete_stage(stages::SERVICES_READY, None)?;
        } else {
            // Active (HA mode): wait for mirrord to be ready first
            info!("Starting as active NSS, waiting for mirrord to be ready...");
            barrier.wait_for_nodes(stages::MIRRORD_READY, 1, timeouts::MIRRORD_READY)?;
            info!("Mirrord is ready");

            if !meta_stack_testing {
                info!("Waiting for metadata VG configuration...");
                barrier.wait_for_global(stages::METADATA_VG_READY, timeouts::METADATA_VG_READY)?;
            }

            info!("Starting nss_role_agent");
            run_cmd!(systemctl start nss_role_agent.service)?;

            // Wait for nss_server to be ready before signaling
            wait_for_service_ready("nss_server", 8088, 360)?;

            // Signal that journal is ready and nss_server is accepting connections
            barrier.complete_stage(stages::NSS_JOURNAL_READY, None)?;

            // Complete services-ready stage
            barrier.complete_stage(stages::SERVICES_READY, None)?;
        }
    } else {
        // EBS journal type: active or solo (standby already handled above)
        info!(
            "Starting as EBS {} NSS",
            if is_ha_mode { "HA active" } else { "solo" }
        );

        if !meta_stack_testing {
            info!("Waiting for metadata VG configuration...");
            barrier.wait_for_global(stages::METADATA_VG_READY, timeouts::METADATA_VG_READY)?;
        }

        run_cmd!(systemctl start nss_role_agent.service)?;

        // Wait for nss_server to be ready before signaling
        wait_for_service_ready("nss_server", 8088, 360)?;

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
    journal_uuid: Option<&str>,
    service_name: &str,
) -> CmdResult {
    // Journal-type specific config paths
    // For EBS: journal at /data/ebs/{uuid}/ (mount handled by nss_role_agent)
    // For NVMe: journal at /data/local/journal/
    let (volume_dev, shared_dir) = match journal_type {
        JournalType::Ebs => {
            let vid = volume_id.ok_or_else(|| Error::other("volume_id required for EBS"))?;
            let uuid = journal_uuid.ok_or_else(|| Error::other("journal_uuid required for EBS"))?;
            // shared_dir is relative to /data, so "ebs/{uuid}" means /data/ebs/{uuid}/
            (
                Some(ebs_journal::get_volume_dev(vid)),
                format!("ebs/{uuid}"),
            )
        }
        JournalType::Nvme => (None, "local/journal".to_string()),
    };

    create_nss_config(volume_dev.as_deref(), &shared_dir, journal_uuid)?;
    create_mirrord_config(volume_dev.as_deref(), &shared_dir)?;

    // Common configs
    create_coredump_config()?;
    if !config.global.meta_stack_testing {
        create_nss_role_agent_config(config)?;
    }
    create_systemd_unit_file("nss_role_agent", false)?;

    // Systemd units - NVMe needs journal_type for local mount dependency, EBS needs journal_uuid
    create_systemd_unit_file_with_journal_type("mirrord", false, Some(journal_type), journal_uuid)?;
    create_systemd_unit_file_with_journal_type(
        service_name,
        false,
        Some(journal_type),
        journal_uuid,
    )?;

    create_logrotate_for_stats()?;
    if config.global.deploy_target == DeployTarget::Aws {
        create_ena_irq_affinity_service()?;
    }
    create_nvme_tuning_service()?;
    Ok(())
}

fn create_nss_config(
    volume_dev: Option<&str>,
    shared_dir: &str,
    journal_uuid: Option<&str>,
) -> CmdResult {
    // Get total memory in kilobytes from /proc/meminfo
    let total_mem_kb_str = run_fun!(cat /proc/meminfo | grep MemTotal | awk r"{print $2}")?;
    let total_mem_kb = total_mem_kb_str
        .trim()
        .parse::<u64>()
        .map_err(|_| Error::other(format!("invalid total_mem_kb: {total_mem_kb_str}")))?;

    // Calculate total memory for blob_dram_kilo_bytes
    let blob_dram_kilo_bytes = (total_mem_kb as f64 * BLOB_DRAM_MEM_PERCENT) as u64;

    let fa_journal_segment_size = if volume_dev.is_some() {
        ebs_journal::FA_JOURNAL_SEGMENT_SIZE
    } else {
        nvme_journal::FA_JOURNAL_SEGMENT_SIZE
    };

    let num_cores = num_cpus()?;
    let net_worker_thread_count = num_cores / 2;
    let fa_thread_dataop_count = num_cores / 2;
    let fa_thread_count = fa_thread_dataop_count + 4;

    // Include journal_uuid in config if provided
    let journal_uuid_line = match journal_uuid {
        Some(uuid) => format!("journal_uuid = \"{uuid}\"\n"),
        None => String::new(),
    };

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
{journal_uuid_line}"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$NSS_SERVER_CONFIG
    }?;
    Ok(())
}

fn create_mirrord_config(volume_dev: Option<&str>, shared_dir: &str) -> CmdResult {
    let num_cores = run_fun!(nproc)?;
    let fa_journal_segment_size = if volume_dev.is_some() {
        ebs_journal::FA_JOURNAL_SEGMENT_SIZE
    } else {
        nvme_journal::FA_JOURNAL_SEGMENT_SIZE
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

/// Prepare local directories for nss_server (stats, meta_cache).
/// Note: /data/local is already mounted by format_local_nvme_disks() earlier in bootstrap.
/// This is called for both active and standby nodes.
fn prepare_local_dirs() -> CmdResult {
    run_cmd! {
        info "Creating local directories for nss_server";
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

    Ok(())
}

/// Prepare local directories and run nss_server format.
/// If `create_journal_dir` is true, also creates /data/local/journal (for nvme mode).
pub(crate) fn format_nss(create_journal_dir: bool) -> CmdResult {
    if create_journal_dir {
        run_cmd! {
            info "Creating journal directory for nvme mode";
            mkdir -p /data/local/journal;
        }?;
    }

    prepare_local_dirs()?;

    run_cmd! {
        info "Running format for nss_server";
        /opt/fractalbits/bin/nss_server format -c ${ETC_PATH}${NSS_SERVER_CONFIG};
    }?;

    Ok(())
}

fn create_nss_role_agent_config(config: &BootstrapConfig) -> CmdResult {
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

    // Build EBS failover config section if applicable
    let ebs_failover_section = if config.global.journal_type == JournalType::Ebs {
        let resources = config.get_resources();
        let nss_nodes = config.get_node_entries("nss_server");

        // Get the shared EBS volume_id and journal_uuid from nss-A's node entry
        let nss_a_entry =
            nss_nodes.and_then(|nodes| nodes.iter().find(|n| n.id == resources.nss_a_id));
        let ebs_volume_id = nss_a_entry
            .and_then(|n| n.volume_id.as_deref())
            .unwrap_or("");
        let ebs_journal_uuid = nss_a_entry
            .and_then(|n| n.journal_uuid.as_deref())
            .unwrap_or("");

        // Determine peer instance ID: A's peer is B, B's peer is A
        let peer_instance_id = if let Some(ref nss_b_id) = resources.nss_b_id {
            if instance_id == *nss_b_id {
                resources.nss_a_id.clone()
            } else {
                nss_b_id.clone()
            }
        } else {
            String::new()
        };

        let region = &config.global.region;

        let mut section = format!(
            r##"
journal_type = "ebs"
ebs_volume_id = "{ebs_volume_id}"
ebs_journal_uuid = "{ebs_journal_uuid}"
aws_region = "{region}"
"##
        );

        if !peer_instance_id.is_empty() {
            section.push_str(&format!("peer_instance_id = \"{peer_instance_id}\"\n"));
        }

        section
    } else {
        String::new()
    };

    // mirrord_endpoint is fetched from RSS at runtime, not configured here
    let config_content = format!(
        r##"# NSS Role Agent Configuration
# Role and mirrord_endpoint are fetched from RSS at startup

rss_addrs = [{rss_addrs_toml}]
instance_id = "{instance_id}"
network_address = "{private_ip}:{nss_port}"
{ebs_failover_section}"##
    );

    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$NSS_ROLE_AGENT_CONFIG
    }?;
    Ok(())
}
