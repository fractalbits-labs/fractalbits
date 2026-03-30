use super::common::*;
use crate::config::{BootstrapConfig, DeployTarget};
use crate::workflow::{StageCompletion, WorkflowBarrier, WorkflowServiceType, stages, timeouts};
use cmd_lib::*;
use std::io::Error;

const BLOB_DRAM_MEM_PERCENT: f64 = 0.8;
const FA_JOURNAL_SEGMENT_SIZE: u64 = 2 * 1024 * 1024 * 1024;
const FLAG_STORAGE_SIZE_PERCENT: f64 = 0.9;

pub fn bootstrap(config: &BootstrapConfig, for_bench: bool) -> CmdResult {
    let for_bench = for_bench || config.global.for_bench;
    let meta_stack_testing = config.global.meta_stack_testing;
    let use_etcd = config.is_etcd_backend();

    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Bss)?;

    // Complete instances-ready stage
    barrier.complete_stage(stages::INSTANCES_READY, None)?;

    install_packages(&["nvme-cli", "mdadm"])?;
    format_local_nvme_disks(false)?; // no twp support since experiment is done

    let mut binaries = vec!["bss_server", "test_bss_storage_engine", "fbs"];
    if use_etcd {
        binaries.push("etcdctl");
    }
    download_binaries(config, &binaries)?;

    create_coredump_config()?;

    info!("Creating directories for bss_server");
    run_cmd! {
        mkdir -p "/data/local/stats";
        mkdir -p "/data/local/journal";
        mkdir -p "/data/local/storage";
        mkdir -p "/data/local/storage/meta_blobs";
    }?;

    if meta_stack_testing || for_bench {
        let _ = download_binaries(config, &["rewrk_rpc"]); // i3, i3en may not compile rewrk_rpc tool
    }

    create_logrotate_for_stats()?;
    if config.global.deploy_target == DeployTarget::Aws {
        create_ena_irq_affinity_service()?;
    }
    create_nvme_tuning_service()?;

    // Start etcd using workflow-based cluster discovery
    // BSS nodes coordinate via S3 to form etcd cluster
    if let Some(etcd_config) = &config.etcd
        && etcd_config.enabled
    {
        info!("Starting etcd bootstrap with workflow-based cluster discovery");
        bootstrap_etcd(config, &barrier, etcd_config)?;
    }

    // Register BSS service AFTER etcd is bootstrapped (if using etcd backend)
    // This ensures etcd endpoints are available for registration
    register_service(config, "bss-server")?;

    if !meta_stack_testing {
        // Wait for RSS to initialize and publish volume configs
        info!("Waiting for RSS to initialize...");
        barrier.wait_for_global(stages::RSS_INITIALIZED, timeouts::RSS_INITIALIZED)?;
    }

    create_bss_config()?;
    format_bss(&config.global.storage_alloc_mode)?;
    create_systemd_unit_file("bss", true)?;

    run_cmd! {
        info "Syncing file system changes";
        sync;
    }?;

    // Signal that BSS is configured and ready
    barrier.complete_stage(stages::BSS_CONFIGURED, None)?;
    barrier.complete_stage(stages::SERVICES_READY, None)?;

    Ok(())
}

fn bootstrap_etcd(
    config: &BootstrapConfig,
    barrier: &WorkflowBarrier,
    etcd_config: &crate::config::EtcdConfig,
) -> CmdResult {
    let cluster_size = etcd_config.cluster_size;

    // REGISTER: Write node IP to cloud storage via generic stage mechanism
    let my_ip = get_private_ip(config.global.deploy_target)?;
    info!("Registering etcd node (IP: {my_ip}) via workflow stage");
    barrier.complete_stage(
        stages::ETCD_NODES_REGISTERED,
        Some(serde_json::json!({"ip": my_ip})),
    )?;

    // DISCOVER: Wait for all nodes to register
    info!("Waiting for {cluster_size} etcd nodes to register");
    let completions = barrier.wait_for_nodes(
        stages::ETCD_NODES_REGISTERED,
        cluster_size,
        timeouts::ETCD_READY,
    )?;
    let ips = StageCompletion::extract_metadata_field(&completions, "ip");
    info!("Found {} nodes: {ips:?}", ips.len());

    // ELECTION: All nodes have same view, generate initial-cluster
    let initial_cluster = generate_initial_cluster(&ips);
    info!("Generated initial-cluster: {initial_cluster}");

    // START: All nodes start etcd together with initial-cluster-state: new
    super::etcd_server::bootstrap_new_cluster(config, &initial_cluster)?;

    // Signal that etcd cluster is ready (any node can do this, idempotent)
    // Only one node needs to signal, but it's safe for all to try
    barrier.complete_global_stage(stages::ETCD_READY, None)?;

    Ok(())
}

/// Generate etcd initial-cluster string from node IPs
fn generate_initial_cluster(ips: &[String]) -> String {
    const ETCD_PEER_PORT: u16 = 2380;
    ips.iter()
        .map(|ip| {
            let member_name = format!("bss-{}", ip.replace('.', "-"));
            format!("{member_name}=http://{ip}:{ETCD_PEER_PORT}")
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn format_bss(alloc_mode: &xtask_common::StorageAllocMode) -> CmdResult {
    let mode = alloc_mode.to_string();
    run_cmd! {
        info "Running format for bss_server (storage_alloc_mode=${mode})";
        ${BIN_PATH}bss_server format -c ${ETC_PATH}${BSS_SERVER_CONFIG} --storage-alloc-mode $mode;
    }?;

    Ok(())
}

fn create_bss_config() -> CmdResult {
    // Get total memory in kilobytes from /proc/meminfo
    let total_mem_kb_str = run_fun!(cat /proc/meminfo | grep MemTotal | awk r"{print $2}")?;
    let total_mem_kb = total_mem_kb_str
        .trim()
        .parse::<u64>()
        .map_err(|_| Error::other(format!("invalid total_mem_kb: {total_mem_kb_str}")))?;

    let blob_dram_kilo_bytes = (total_mem_kb as f64 * BLOB_DRAM_MEM_PERCENT) as u64;

    let num_cores = num_cpus()?;
    let net_worker_thread_count = num_cores / 2;
    let fa_thread_dataop_count = num_cores / 2;
    let fa_thread_count = fa_thread_dataop_count + 4;

    let fa_journal_segment_size = FA_JOURNAL_SEGMENT_SIZE;

    // Get /data/local volume size in 1K blocks and compute flag_storage_size as 90% of it
    let data_size_kb_str = run_fun!(df -k /data/local | awk r"NR==2 {print $2}")?;
    let data_size_kb = data_size_kb_str.trim().parse::<u64>().map_err(|_| {
        Error::other(format!(
            "invalid /data/local volume size: {data_size_kb_str}"
        ))
    })?;
    let flag_storage_size = (data_size_kb as f64 * 1024.0 * FLAG_STORAGE_SIZE_PERCENT) as u64;
    info!(
        "/data/local volume size: {} KB, flag_storage_size: {} bytes ({} GB)",
        data_size_kb,
        flag_storage_size,
        flag_storage_size / (1024 * 1024 * 1024)
    );

    let config_content = format!(
        r##"working_dir = "/data"
shared_dir = "local/journal"
server_port = 8088
health_port = 19998
net_worker_thread_count = {net_worker_thread_count}
fa_thread_count = {fa_thread_count}
fa_thread_dataop_count = {fa_thread_dataop_count}
blob_dram_kilo_bytes = {blob_dram_kilo_bytes}
io_concurrency = 256
flag_storage_size = {flag_storage_size}
fa_journal_segment_size = {fa_journal_segment_size}
log_level = "info"
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$BSS_SERVER_CONFIG;
    }?;

    Ok(())
}
