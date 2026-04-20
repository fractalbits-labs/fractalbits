use super::common::*;
use crate::config::{BootstrapConfig, DeployTarget};
use crate::stage_helpers::{CommonServicesReadyStage, InstancesReadyStage};
use crate::workflow::{WorkflowBarrier, WorkflowServiceType, stages};
use cmd_lib::*;
use std::io::Error;
use xtask_common::stages::{
    VerifiedGlobalDep, VerifiedGlobalStage, VerifiedNodeDep, VerifiedNodeStage,
};

const BLOB_DRAM_MEM_PERCENT: f64 = 0.8;

struct NssConfiguredStage;

impl NssConfiguredStage {
    const STAGE: VerifiedNodeStage = const { stages::NSS_CONFIGURED.node_stage() };
    const ETCD_READY: VerifiedGlobalDep = const { stages::NSS_CONFIGURED.global_dep("etcd-ready") };
    const RSS_INITIALIZED: VerifiedGlobalDep =
        const { stages::NSS_CONFIGURED.global_dep("rss-initialized") };

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

struct JournalFormattedStage;

impl JournalFormattedStage {
    const STAGE: VerifiedGlobalStage = const { stages::JOURNAL_FORMATTED.global_stage() };
    const METADATA_VG_READY: VerifiedGlobalDep =
        const { stages::JOURNAL_FORMATTED.global_dep("metadata-vg-ready") };
    const BSS_CONFIGURED: VerifiedNodeDep =
        const { stages::JOURNAL_FORMATTED.node_dep("bss-configured") };
    const BSS_READY: VerifiedNodeDep = const { stages::JOURNAL_FORMATTED.node_dep("bss-ready") };

    fn wait_for_metadata_vg_ready(barrier: &WorkflowBarrier) -> CmdResult {
        barrier.wait_for_global(Self::METADATA_VG_READY)
    }

    fn wait_for_bss_configured(barrier: &WorkflowBarrier, expected: usize) -> CmdResult {
        barrier.wait_for_nodes(Self::BSS_CONFIGURED, expected)?;
        Ok(())
    }

    fn wait_for_bss_ready(barrier: &WorkflowBarrier, expected: usize) -> CmdResult {
        barrier.wait_for_nodes(Self::BSS_READY, expected)?;
        Ok(())
    }

    fn complete(barrier: &WorkflowBarrier) -> CmdResult {
        barrier.complete_global_stage(Self::STAGE, None)
    }
}

struct NssJournalReadyStage;

impl NssJournalReadyStage {
    const STAGE: VerifiedNodeStage = const { stages::NSS_JOURNAL_READY.node_stage() };
    const JOURNAL_FORMATTED: VerifiedGlobalDep =
        const { stages::NSS_JOURNAL_READY.global_dep("journal-formatted") };

    fn wait_for_journal_formatted(barrier: &WorkflowBarrier) -> CmdResult {
        barrier.wait_for_global(Self::JOURNAL_FORMATTED)
    }

    fn complete(barrier: &WorkflowBarrier, metadata: serde_json::Value) -> CmdResult {
        barrier.complete_node_stage(Self::STAGE, Some(metadata))
    }
}

pub fn bootstrap(
    config: &BootstrapConfig,
    journal_uuid: Option<&str>,
    for_bench: bool,
) -> CmdResult {
    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Nss)?;

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
    let instances_ready_meta = serde_json::json!({
        "private_ip": private_ip,
        "role": "primary",
    });

    // Complete instances-ready stage
    InstancesReadyStage::complete_with_metadata(&barrier, instances_ready_meta)?;

    if for_bench {
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
        NssConfiguredStage::wait_for_etcd_ready(&barrier)?;
        info!("etcd cluster is ready");
    }

    // Register NSS in service discovery before waiting for RSS. With NSS
    // running in an ASG/MIG, instance IDs/IPs are not known at synthesis
    // time, so RSS leader discovers the NSS endpoint by polling this
    // registry entry rather than via injected CLI args.
    register_service(config, "nss-server")?;

    // Wait for RSS to initialize - RSS will have registered with service discovery by then
    // This must happen before setup_configs because create_nss_role_agent_config needs RSS IPs
    info!("Waiting for RSS to initialize...");
    NssConfiguredStage::wait_for_rss_initialized(&barrier)?;

    setup_configs(config, journal_uuid, "nss")?;
    prepare_local_dirs()?;

    // Signal NSS is configured (configs written, local dirs prepared)
    NssConfiguredStage::complete(&barrier)?;

    // Wait for metadata VG configuration before format, since nss_server format
    // needs BSS addresses to initialize the buffer_manager state.
    info!("Waiting for metadata VG configuration...");
    JournalFormattedStage::wait_for_metadata_vg_ready(&barrier)?;

    // Read journal-configs to discover the journal owner (running_nss_id). In
    // a multi-NSS deployment only the owner formats the journal and runs
    // nss_server; the rest stay idle (role_agent fetches its role from RSS).
    // Concurrent formats from multiple NSS nodes would corrupt the journal.
    let journal_configs_json = get_service_discovery_value(config, "journal-configs")?;
    let journal_configs: Vec<serde_json::Value> = serde_json::from_str(&journal_configs_json)
        .map_err(|e| Error::other(format!("Failed to parse journal-configs: {e}")))?;
    let journal_config = journal_configs
        .first()
        .ok_or_else(|| Error::other("journal-configs list is empty"))?;
    let running_nss_id = journal_config
        .get("running_nss_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::other("journal-configs entry missing running_nss_id"))?;
    let my_instance_id = get_instance_id(config.global.deploy_target)?;
    let is_journal_owner = my_instance_id == running_nss_id;

    if is_journal_owner {
        info!("This NSS ({my_instance_id}) owns the journal; will format");
        let metadata_vg_config = get_service_discovery_value(config, BSS_METADATA_VG_CONFIG_KEY)?;
        let journal_vg_config = get_service_discovery_value(config, BSS_JOURNAL_VG_CONFIG_KEY)?;
        let journal_config_str = journal_config.to_string();

        // Format issues quorum writes to BSS. Wait first for every BSS node
        // to finish its (slow, zero-write) format — that's the BSS_CONFIGURED
        // wait and uses BSS_CONFIGURED's long timeout. Then wait for each BSS
        // to actually bind its port (BSS_READY, short timeout). Otherwise
        // MultiBssObjectWriter quorum fails with NotEnoughReplicas.
        let total_bss_nodes = config
            .global
            .num_bss_nodes
            .ok_or_else(|| Error::other("global.num_bss_nodes is required for NSS bootstrap"))?;
        info!("Waiting for {total_bss_nodes} BSS node(s) to complete format...");
        JournalFormattedStage::wait_for_bss_configured(&barrier, total_bss_nodes)?;
        info!("All BSS nodes have completed format");
        info!("Waiting for {total_bss_nodes} BSS node(s) to be serving...");
        JournalFormattedStage::wait_for_bss_ready(&barrier, total_bss_nodes)?;
        info!("All BSS nodes are serving");

        format_journal(&metadata_vg_config, &journal_vg_config, &journal_config_str)?;
        JournalFormattedStage::complete(&barrier)?;
    } else {
        info!(
            "This NSS ({my_instance_id}) is idle (owner is {running_nss_id}); \
             waiting for journal format to complete"
        );
        NssJournalReadyStage::wait_for_journal_formatted(&barrier)?;
    }

    info!("Starting nss_role_agent");
    run_cmd!(systemctl start nss_role_agent.service)?;

    if is_journal_owner {
        // Journal owner: nss_server is brought up by role_agent. Wait for it
        // and signal that the journal is ready and nss_server is accepting
        // connections. Idle nodes skip this — their role_agent stays idle.
        wait_for_service_ready("nss_server", 8088, 360)?;
        let journal_ready_meta = serde_json::json!({
            "private_ip": private_ip,
            "role": "primary",
        });
        NssJournalReadyStage::complete(&barrier, journal_ready_meta)?;
    }

    // Complete services-ready stage
    CommonServicesReadyStage::complete(&barrier)?;
    Ok(())
}

fn setup_configs(config: &BootstrapConfig, journal_uuid: &str, service_name: &str) -> CmdResult {
    create_nss_config(journal_uuid)?;

    // Common configs
    create_coredump_config()?;
    create_nss_role_agent_config(config)?;
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

/// Run nss_server format against the quorum journal.
/// `metadata_vg_config` provides BSS addresses for buffer_manager initialization.
/// `journal_config` provides journal UUID, device ID, size, and fence token.
fn format_journal(
    metadata_vg_config: &str,
    journal_vg_config: &str,
    journal_config: &str,
) -> CmdResult {
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
