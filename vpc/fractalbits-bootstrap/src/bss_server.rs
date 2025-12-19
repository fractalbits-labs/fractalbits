use super::common::*;
use crate::config::BootstrapConfig;
use crate::etcd_cluster::{
    EtcdClusterCoordinator, get_all_registered_bss_nodes_s3, set_cluster_state_s3,
};
use cmd_lib::*;
use rayon::prelude::*;
use std::fs;
use std::io::Error;

const BSS_DATA_VOLUME_SHARDS: usize = 65536;
const BSS_METADATA_VOLUME_SHARDS: usize = 256;

#[derive(Debug, Clone, Copy)]
enum VolumeType {
    Data,
    Metadata,
}

impl VolumeType {
    fn as_str(&self) -> &'static str {
        match self {
            VolumeType::Data => "data",
            VolumeType::Metadata => "metadata",
        }
    }

    fn shard_count(&self) -> usize {
        match self {
            VolumeType::Data => BSS_DATA_VOLUME_SHARDS,
            VolumeType::Metadata => BSS_METADATA_VOLUME_SHARDS,
        }
    }
}

#[derive(Debug, Default)]
struct VolumeAssignments {
    data_volume: Option<usize>,
    metadata_volume: Option<usize>,
}

impl VolumeAssignments {
    fn has_assignments(&self) -> bool {
        self.data_volume.is_some() || self.metadata_volume.is_some()
    }
}

pub fn bootstrap(config: &BootstrapConfig, for_bench: bool) -> CmdResult {
    let for_bench = for_bench || config.global.for_bench;
    let meta_stack_testing = config.global.meta_stack_testing;
    let use_etcd = config.is_etcd_backend();

    install_rpms(&["nvme-cli", "mdadm"])?;
    format_local_nvme_disks(false)?; // no twp support since experiment is done
    create_ddb_register_and_deregister_service("bss-server")?;
    let mut binaries = vec!["bss_server"];
    if use_etcd {
        binaries.push("etcdctl");
    }
    download_binaries(&binaries)?;

    create_coredump_config()?;

    info!("Creating directories for bss_server");
    run_cmd!(mkdir -p "/data/local/stats")?;

    if meta_stack_testing || for_bench {
        let _ = download_binaries(&["rewrk_rpc"]); // i3, i3en may not compile rewrk_rpc tool
    }

    create_logrotate_for_stats()?;
    create_ena_irq_affinity_service()?;
    create_nvme_tuning_service()?;

    // Start etcd BEFORE setup_volume_directories
    // BSS nodes coordinate via S3 to form etcd cluster, then RSS writes volume configs to etcd
    if let Some(etcd_config) = &config.etcd
        && etcd_config.enabled
    {
        info!("Starting etcd bootstrap with S3-based cluster discovery");
        bootstrap_etcd(etcd_config)?;
    }

    setup_volume_directories(config, use_etcd)?;
    create_bss_config()?;
    create_systemd_unit_file("bss", true)?;

    run_cmd! {
        info "Syncing file system changes";
        sync;
    }?;

    Ok(())
}

fn bootstrap_etcd(etcd_config: &crate::config::EtcdConfig) -> CmdResult {
    let cluster_id = &etcd_config.cluster_id;
    let s3_bucket = &etcd_config.s3_bucket;
    let quorum_size = etcd_config.quorum_size;

    let coordinator = EtcdClusterCoordinator::new(cluster_id, s3_bucket, quorum_size)?;

    // REGISTER: Write node info to S3
    info!("Registering node in S3 for cluster {cluster_id}");
    coordinator.register_node()?;

    // DISCOVER: Wait for all nodes to register
    info!("Waiting for {quorum_size} nodes to register");
    let nodes = coordinator.wait_for_quorum()?;
    info!(
        "Found {} nodes: {:?}",
        nodes.len(),
        nodes.iter().map(|n| &n.ip).collect::<Vec<_>>()
    );

    // ELECTION: All nodes have same view, generate initial-cluster
    let initial_cluster = coordinator.generate_initial_cluster(&nodes);
    info!("Generated initial-cluster: {initial_cluster}");

    // START: All nodes start etcd together with initial-cluster-state: new
    super::etcd_server::bootstrap_new_cluster(&initial_cluster)?;

    // Mark cluster as active in S3 (any node can do this, idempotent)
    set_cluster_state_s3(s3_bucket, cluster_id, "active")?;

    Ok(())
}

fn setup_volume_directories(config: &BootstrapConfig, use_etcd: bool) -> CmdResult {
    let instance_id = get_instance_id()?;
    info!("BSS instance ID: {instance_id}");

    let assignments = match wait_for_volume_configs(config, use_etcd) {
        Ok(()) => match get_volume_assignments(config, &instance_id, use_etcd) {
            Ok(assignments) => {
                info!("Volume assignments for {instance_id}: {assignments:?}");

                if assignments.has_assignments() {
                    assignments
                } else {
                    cmd_die!("No volume assignments found for $instance_id");
                }
            }
            Err(e) => {
                cmd_die!("Failed to get volume assignments: $e");
            }
        },
        Err(e) => {
            cmd_die!("Get volume assignments failed: $e");
        }
    };

    create_volume_directories(&assignments)?;
    Ok(())
}

fn create_bss_config() -> CmdResult {
    let num_threads = run_fun!(nproc)?;
    let config_content = format!(
        r##"working_dir = "/data"
server_port = 8088
num_threads = {num_threads}
log_level = "warn"
use_direct_io = true
io_concurrency = 256
data_volume_shards = {BSS_DATA_VOLUME_SHARDS}
metadata_volume_shards = {BSS_METADATA_VOLUME_SHARDS}
set_thread_affinity = true
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$BSS_SERVER_CONFIG;
    }?;

    Ok(())
}

fn wait_for_volume_configs(config: &BootstrapConfig, use_etcd: bool) -> CmdResult {
    const TIMEOUT_SECS: u64 = 300;
    const POLL_INTERVAL_SECS: u64 = 1;

    let backend_name = if use_etcd { "etcd" } else { "DDB" };
    info!("Waiting for BSS volume configurations in {backend_name}...");
    let start = std::time::Instant::now();

    while start.elapsed().as_secs() < TIMEOUT_SECS {
        // Check if both configs exist AND contain valid JSON
        let data_result = if use_etcd {
            get_etcd_config(config, BSS_DATA_VG_CONFIG_KEY)
        } else {
            get_ddb_config(BSS_DATA_VG_CONFIG_KEY)
        };

        let metadata_result = if use_etcd {
            get_etcd_config(config, BSS_METADATA_VG_CONFIG_KEY)
        } else {
            get_ddb_config(BSS_METADATA_VG_CONFIG_KEY)
        };

        if let (Ok(data_config), Ok(metadata_config)) = (data_result, metadata_result) {
            // Verify the configs contain valid JSON
            if serde_json::from_str::<serde_json::Value>(&data_config).is_ok()
                && serde_json::from_str::<serde_json::Value>(&metadata_config).is_ok()
            {
                info!("Volume configurations found in {backend_name}");
                return Ok(());
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECS));
    }

    Err(Error::other(format!(
        "Timeout waiting for volume configs in {backend_name}"
    )))
}

fn get_ddb_config(service_key: &str) -> FunResult {
    let key = format!(r#"{{"service_id":{{"S":"{}"}}}}"#, service_key);
    let result = run_fun!(
        aws dynamodb get-item
            --table-name $DDB_SERVICE_DISCOVERY_TABLE
            --key $key
            --query "Item.value.S"
            --output text
            2>/dev/null
    )?;

    if result.is_empty() || result == "None" || result == "null" {
        return Err(Error::other(format!(
            "Config {} not found in DDB",
            service_key
        )));
    }

    Ok(result)
}

fn get_etcd_config(config: &BootstrapConfig, service_key: &str) -> FunResult {
    let etcd_config = config
        .etcd
        .as_ref()
        .ok_or_else(|| Error::other("etcd config missing"))?;

    let nodes = get_all_registered_bss_nodes_s3(&etcd_config.s3_bucket, &etcd_config.cluster_id)?;
    let etcd_endpoints = nodes
        .iter()
        .map(|node| format!("http://{}:2379", node.ip))
        .collect::<Vec<_>>()
        .join(",");

    let etcdctl = format!("{BIN_PATH}etcdctl");
    let key = format!("/fractalbits-service-discovery/{}", service_key);

    let result =
        run_fun!($etcdctl --endpoints=$etcd_endpoints get $key --print-value-only 2>/dev/null)?;

    if result.is_empty() {
        return Err(Error::other(format!(
            "Config {} not found in etcd",
            service_key
        )));
    }

    Ok(result.trim().to_string())
}

fn get_volume_assignments(
    config: &BootstrapConfig,
    instance_id: &str,
    use_etcd: bool,
) -> Result<VolumeAssignments, Error> {
    // For etcd backend, configs use IP addresses as identifiers
    // For DDB backend, configs use instance IDs
    let my_ip = if use_etcd {
        Some(get_private_ip()?)
    } else {
        None
    };

    let find_volume = |config_json: &str| -> Result<Option<usize>, Error> {
        let config: serde_json::Value = serde_json::from_str(config_json)
            .map_err(|e| Error::other(format!("Failed to parse config: {}", e)))?;

        if let Some(volumes) = config["volumes"].as_array() {
            for volume in volumes {
                if let Some(nodes) = volume["bss_nodes"].as_array() {
                    for node in nodes {
                        // For DDB backend, match by node_id (instance ID)
                        // For etcd backend, match by IP address
                        let matches = if let Some(ref ip) = my_ip {
                            node["ip"].as_str() == Some(ip.as_str())
                        } else {
                            node["node_id"].as_str() == Some(instance_id)
                        };
                        if matches {
                            return Ok(volume["volume_id"].as_u64().map(|v| v as usize));
                        }
                    }
                }
            }
        }

        Ok(None)
    };

    let mut assignments = VolumeAssignments::default();

    let data_config = if use_etcd {
        get_etcd_config(config, BSS_DATA_VG_CONFIG_KEY)?
    } else {
        get_ddb_config(BSS_DATA_VG_CONFIG_KEY)?
    };
    assignments.data_volume = find_volume(&data_config)?;

    let metadata_config = if use_etcd {
        get_etcd_config(config, BSS_METADATA_VG_CONFIG_KEY)?
    } else {
        get_ddb_config(BSS_METADATA_VG_CONFIG_KEY)?
    };
    assignments.metadata_volume = find_volume(&metadata_config)?;

    Ok(assignments)
}

fn create_volume_directories(assignments: &VolumeAssignments) -> CmdResult {
    let create_dirs = |volume_type: VolumeType, volume_id: usize| -> CmdResult {
        let type_str = volume_type.as_str();
        let volume_dir = format!("/data/local/blobs/{}_volume{}", type_str, volume_id);

        info!("Creating {type_str} volume{volume_id} directories");
        fs::create_dir_all(&volume_dir)
            .map_err(|e| Error::other(format!("Failed to create {volume_dir}: {e}")))?;

        let shard_count = volume_type.shard_count();
        let shards: Vec<usize> = (0..shard_count).collect();

        shards.par_iter().try_for_each(|&i| {
            let shard_dir = format!("{}/{}", volume_dir, i);
            fs::create_dir(&shard_dir)
                .map_err(|e| Error::other(format!("Failed to create {shard_dir}: {e}")))
        })?;

        info!("Created {shard_count} {type_str} volume{volume_id} shard directories");
        Ok(())
    };

    if let Some(vol_id) = assignments.data_volume {
        create_dirs(VolumeType::Data, vol_id)?;
    }

    if let Some(vol_id) = assignments.metadata_volume {
        create_dirs(VolumeType::Metadata, vol_id)?;
    }

    run_cmd! {
        info "Syncing filesystem";
        sync;
    }?;

    Ok(())
}
