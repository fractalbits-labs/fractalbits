use super::common::*;
use crate::config::{BootstrapConfig, DeployTarget};
use crate::stage_helpers::{CommonServicesReadyStage, InstancesReadyStage};
use crate::workflow::{WorkflowBarrier, WorkflowServiceType, stages};
use cmd_lib::*;
use std::io::Error;
use xtask_common::stages::{VerifiedGlobalDep, VerifiedNodeStage};

const BLOB_DRAM_MEM_PERCENT: f64 = 0.8;

struct NssFormattedStage;

impl NssFormattedStage {
    const STAGE: VerifiedNodeStage = const { stages::NSS_FORMATTED.node_stage() };
    const ETCD_READY: VerifiedGlobalDep = const { stages::NSS_FORMATTED.global_dep("etcd-ready") };
    const RSS_INITIALIZED: VerifiedGlobalDep =
        const { stages::NSS_FORMATTED.global_dep("rss-initialized") };

    fn wait_for_etcd_ready(barrier: &WorkflowBarrier) -> CmdResult {
        barrier.wait_for_global(Self::ETCD_READY)
    }

    fn wait_for_rss_initialized(barrier: &WorkflowBarrier) -> CmdResult {
        barrier.wait_for_global(Self::RSS_INITIALIZED)
    }

    fn complete(barrier: &WorkflowBarrier) -> CmdResult {
        barrier.complete_node_stage(Self::STAGE, None)
    }
}

struct NssJournalReadyStage;

impl NssJournalReadyStage {
    const STAGE: VerifiedNodeStage = const { stages::NSS_JOURNAL_READY.node_stage() };
    const METADATA_VG_READY: VerifiedGlobalDep =
        const { stages::NSS_JOURNAL_READY.global_dep("metadata-vg-ready") };

    fn wait_for_metadata_vg_ready(barrier: &WorkflowBarrier) -> CmdResult {
        barrier.wait_for_global(Self::METADATA_VG_READY)
    }

    fn complete(barrier: &WorkflowBarrier, metadata: serde_json::Value) -> CmdResult {
        barrier.complete_node_stage(Self::STAGE, Some(metadata))
    }
}

pub fn bootstrap(
    config: &BootstrapConfig,
    journal_uuid: Option<&str>,
    is_standby: bool,
    for_bench: bool,
) -> CmdResult {
    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Nss)?;
    let meta_stack_testing = config.global.meta_stack_testing;

    // Resolve journal_uuid: prefer CLI/NodeEntry value, fall back to global config
    let global_journal_uuid;
    let journal_uuid: &str = if let Some(uuid) = journal_uuid {
        uuid
    } else {
        global_journal_uuid = config.global.journal_uuid.clone();
        global_journal_uuid
            .as_deref()
            .ok_or_else(|| Error::other("journal_uuid is required"))?
    };

    // Get private IP for stage completion metadata (used by RSS to discover NSS IP)
    let private_ip = crate::common::get_private_ip(config.global.deploy_target).unwrap_or_default();
    let nss_role = if is_standby { "standby" } else { "primary" };
    let instances_ready_meta = serde_json::json!({
        "private_ip": private_ip,
        "role": nss_role,
    });

    // Complete instances-ready stage
    InstancesReadyStage::complete_with_metadata(&barrier, instances_ready_meta)?;

    if meta_stack_testing || for_bench {
        let _ = download_binaries(config, &["rewrk_rpc"]);
    }

    let mut binaries = vec!["nss_server", "nss_role_agent"];
    if config.is_etcd_backend() {
        binaries.push("etcdctl");
    }
    download_binaries(config, &binaries)?;

    // When using etcd backend, wait for etcd cluster to be ready first
    if config.is_etcd_backend() {
        info!("Waiting for etcd cluster to be ready...");
        NssFormattedStage::wait_for_etcd_ready(&barrier)?;
        info!("etcd cluster is ready");
    }

    if !meta_stack_testing {
        // Wait for RSS to initialize - RSS will have registered with service discovery by then
        // This must happen before setup_configs because create_nss_role_agent_config needs RSS IPs
        info!("Waiting for RSS to initialize...");
        NssFormattedStage::wait_for_rss_initialized(&barrier)?;
    } else {
        info!("Meta-stack testing mode: skipping RSS wait");
    }

    // Determine HA mode: if this node is the standby, we're definitely in HA mode.
    // For the primary node, check resources (TOML path) to see if a standby exists.
    let is_ha_mode = if is_standby {
        true
    } else {
        config.get_resources().nss_b_id.is_some()
    };
    setup_configs(config, journal_uuid, "nss")?;

    if is_standby {
        // HA standby: skip format/mount entirely, start role_agent idle
        info!("Starting as HA standby NSS (idle)");

        // Create local directories needed when standby becomes active
        prepare_local_dirs()?;

        // Signal formatting complete (standby has nothing to format)
        NssFormattedStage::complete(&barrier)?;

        // Start role_agent in standby mode (it will be idle)
        run_cmd!(systemctl start nss_role_agent.service)?;

        // Complete services-ready stage
        CommonServicesReadyStage::complete(&barrier)?;
        return Ok(());
    }

    // Wait for metadata VG configuration before format, since nss_server format
    // needs BSS addresses to initialize the buffer_manager state.
    if !meta_stack_testing {
        info!("Waiting for metadata VG configuration...");
        NssJournalReadyStage::wait_for_metadata_vg_ready(&barrier)?;
    }
    let metadata_vg_config = get_service_discovery_value(config, BSS_METADATA_VG_CONFIG_KEY)?;
    let journal_vg_config = get_service_discovery_value(config, BSS_JOURNAL_VG_CONFIG_KEY)?;
    let journal_config = get_service_discovery_value(config, "journal-config")?;
    info!("Fetched metadata VG, journal VG, and journal configs from service discovery");

    // Format journal via quorum journal (BSS storage)
    format_journal(&metadata_vg_config, &journal_vg_config, &journal_config)?;

    // Signal that formatting is complete
    NssFormattedStage::complete(&barrier)?;

    // Active or solo path
    let nss_mode = if is_ha_mode { "HA active" } else { "solo" };
    info!("Starting as {nss_mode} NSS");

    run_cmd!(systemctl start nss_role_agent.service)?;

    // Wait for nss_server to be ready before signaling
    wait_for_service_ready("nss_server", 8088, 360)?;

    // Signal that journal is ready and nss_server is accepting connections
    let journal_ready_meta = serde_json::json!({
        "private_ip": private_ip,
        "role": nss_role,
    });
    NssJournalReadyStage::complete(&barrier, journal_ready_meta)?;

    // Complete services-ready stage
    CommonServicesReadyStage::complete(&barrier)?;
    Ok(())
}

fn setup_configs(config: &BootstrapConfig, journal_uuid: &str, service_name: &str) -> CmdResult {
    create_nss_config(journal_uuid)?;

    // Common configs
    create_coredump_config()?;
    if !config.global.meta_stack_testing {
        create_nss_role_agent_config(config)?;
    }
    create_systemd_unit_file("nss_role_agent", false)?;
    create_systemd_unit_file(service_name, false)?;

    create_logrotate_for_stats()?;
    if config.global.deploy_target == DeployTarget::Aws {
        create_ena_irq_affinity_service()?;
    }
    create_nvme_tuning_service()?;
    Ok(())
}

fn create_nss_config(journal_uuid: &str) -> CmdResult {
    // Get total memory in kilobytes from /proc/meminfo
    let total_mem_kb_str = run_fun!(cat /proc/meminfo | grep MemTotal | awk r"{print $2}")?;
    let total_mem_kb = total_mem_kb_str
        .trim()
        .parse::<u64>()
        .map_err(|_| Error::other(format!("invalid total_mem_kb: {total_mem_kb_str}")))?;

    // Calculate total memory for blob_dram_kilo_bytes
    let blob_dram_kilo_bytes = (total_mem_kb as f64 * BLOB_DRAM_MEM_PERCENT) as u64;

    // 4GB journal segment size for quorum journal
    let fa_journal_segment_size: u64 = 4 * 1024 * 1024 * 1024;

    let num_cores = num_cpus()?;
    let net_worker_thread_count = num_cores / 2;
    let fa_thread_dataop_count = num_cores / 2;
    let fa_thread_count = fa_thread_dataop_count + 4;

    let journal_uuid_line = format!("journal_uuid = \"{journal_uuid}\"\n");

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
{journal_uuid_line}"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$NSS_SERVER_CONFIG
    }?;
    Ok(())
}

/// Prepare local directories for nss_server (stats).
/// This is called for both active and standby nodes.
fn prepare_local_dirs() -> CmdResult {
    run_cmd! {
        info "Creating local directories for nss_server";
        mkdir -p /data/local/stats;
    }?;

    run_cmd! {
        info "Syncing file system changes";
        sync;
    }?;

    Ok(())
}

/// Prepare local directories and run nss_server format.
/// `metadata_vg_config` provides BSS addresses for buffer_manager initialization.
/// `journal_config` provides journal UUID, device ID, size, and fence token.
fn format_journal(
    metadata_vg_config: &str,
    journal_vg_config: &str,
    journal_config: &str,
) -> CmdResult {
    prepare_local_dirs()?;

    run_cmd! {
        info "Running format for nss_server";
        METADATA_VG_CONFIG=$metadata_vg_config JOURNAL_VG_CONFIG=$journal_vg_config JOURNAL_CONFIG=$journal_config
        /opt/fractalbits/bin/nss_server format -c ${ETC_PATH}${NSS_SERVER_CONFIG};
    }?;

    Ok(())
}

fn create_nss_role_agent_config(config: &BootstrapConfig) -> CmdResult {
    let rss_ha_enabled = config.global.rss_ha_enabled;
    let instance_id = get_instance_id(config.global.deploy_target)?;
    let private_ip = get_private_ip_from_config(config, &instance_id)?;
    let nss_port = 8088;

    // Query service discovery for RSS instance IPs
    let expected_rss_count = if rss_ha_enabled { 2 } else { 1 };
    let rss_ips = get_service_ips_with_backend(config, "root-server", expected_rss_count);
    let rss_addrs_toml = rss_ips
        .iter()
        .map(|ip| format!("\"{}:8088\"", ip))
        .collect::<Vec<_>>()
        .join(", ");

    let config_content = format!(
        r##"# NSS Role Agent Configuration
# Role is fetched from RSS at startup

rss_addrs = [{rss_addrs_toml}]
instance_id = "{instance_id}"
network_address = "{private_ip}:{nss_port}"
journal_type = "remote"
"##
    );

    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$NSS_ROLE_AGENT_CONFIG
    }?;
    Ok(())
}
