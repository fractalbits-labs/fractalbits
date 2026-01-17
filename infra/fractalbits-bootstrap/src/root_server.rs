use super::common::*;
use crate::config::{BootstrapConfig, DeployTarget, JournalType};
use crate::workflow::{WorkflowBarrier, WorkflowServiceType, stages, timeouts};
use cmd_lib::*;
use std::io::Error;

const POLL_INTERVAL_SECONDS: u64 = 1;
const MAX_POLL_ATTEMPTS: u64 = 300;

// Volume group quorum vpc configuration constants
const TOTAL_BSS_NODES: usize = 6;
const DATA_VG_QUORUM_N: usize = 3;
const DATA_VG_QUORUM_R: usize = 2;
const DATA_VG_QUORUM_W: usize = 2;
const META_DATA_VG_QUORUM_N: usize = 6;
const META_DATA_VG_QUORUM_R: usize = 4;
const META_DATA_VG_QUORUM_W: usize = 4;

const BOOTSTRAP_GRACE_PERIOD_SECS: u64 = 120;

pub fn bootstrap(config: &BootstrapConfig, is_leader: bool, for_bench: bool) -> CmdResult {
    let nss_endpoint = &config.endpoints.nss_endpoint;
    let resources = config.get_resources();
    let nss_a_id = &resources.nss_a_id;
    let nss_b_id = resources.nss_b_id.as_deref();
    let remote_az = config.aws.as_ref().and_then(|aws| aws.remote_az.as_deref());
    let num_bss_nodes = config.global.num_bss_nodes;
    let ha_enabled = config.global.rss_ha_enabled;

    if is_leader {
        bootstrap_leader(
            config,
            nss_endpoint,
            nss_a_id,
            nss_b_id,
            remote_az,
            num_bss_nodes,
            ha_enabled,
            for_bench,
        )
    } else {
        bootstrap_follower(config, nss_endpoint, ha_enabled)
    }
}

fn bootstrap_follower(config: &BootstrapConfig, nss_endpoint: &str, ha_enabled: bool) -> CmdResult {
    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Rss)?;

    // Complete instances-ready stage
    barrier.complete_stage(stages::INSTANCES_READY, None)?;

    let mut binaries = vec!["rss_admin", "root_server"];
    if config.is_etcd_backend() {
        binaries.push("etcdctl");
    }
    download_binaries(config, &binaries)?;

    // Wait for leader to initialize RSS
    info!("Follower waiting for RSS leader to initialize...");
    barrier.wait_for_global(stages::RSS_INITIALIZED, timeouts::RSS_INITIALIZED)?;

    create_rss_config(config, nss_endpoint, ha_enabled)?;
    create_rss_bootstrap_env()?;
    create_systemd_unit_file("rss", true)?; // Start immediately
    register_service(config, "root-server")?;

    // Complete services-ready stage
    barrier.complete_stage(stages::SERVICES_READY, None)?;

    // Clear bootstrap env so restarts use default grace period
    clear_rss_bootstrap_env()?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn bootstrap_leader(
    config: &BootstrapConfig,
    nss_endpoint: &str,
    nss_a_id: &str,
    nss_b_id: Option<&str>,
    remote_az: Option<&str>,
    num_bss_nodes: Option<usize>,
    ha_enabled: bool,
    for_bench: bool,
) -> CmdResult {
    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Rss)?;

    // Complete instances-ready stage
    barrier.complete_stage(stages::INSTANCES_READY, None)?;

    // Wait for etcd cluster if using etcd backend
    if config.is_etcd_backend() {
        info!("Waiting for etcd cluster to be ready...");
        barrier.wait_for_global(stages::ETCD_READY, timeouts::ETCD_READY)?;
    }

    let mut binaries = vec!["rss_admin", "root_server"];
    if config.is_etcd_backend() {
        binaries.push("etcdctl");
    }
    download_binaries(config, &binaries)?;

    // Initialize AZ status if this is a multi-AZ deployment (AWS only)
    if let Some(remote_az) = remote_az
        && config.global.deploy_target == DeployTarget::Aws
    {
        initialize_az_status(config, remote_az)?;
    }

    // Initialize NSS role states in service discovery BEFORE starting RSS
    // This ensures the observer state exists when RSS starts
    initialize_observer_state(config, nss_a_id, nss_b_id)?;

    create_rss_config(config, nss_endpoint, ha_enabled)?;
    create_rss_bootstrap_env()?;
    create_systemd_unit_file("rss", true)?;
    register_service(config, "root-server")?;

    // Wait for RSS to be ready before signaling RSS_INITIALIZED
    if ha_enabled {
        wait_for_leadership()?;
    } else {
        wait_for_service_ready("root_server", 8088, 300)?;
    }

    // Create S3 Express buckets if remote_az is provided (AWS only)
    if let Some(remote_az) = remote_az
        && config.global.deploy_target == DeployTarget::Aws
    {
        let local_az = get_current_aws_az_id()?;
        create_s3_express_bucket(&local_az, S3EXPRESS_LOCAL_BUCKET_CONFIG)?;
        create_s3_express_bucket(remote_az, S3EXPRESS_REMOTE_BUCKET_CONFIG)?;
    }

    // Complete RSS initialized stage - signals NSS and other services can proceed
    barrier.complete_global_stage(stages::RSS_INITIALIZED, None)?;

    // Initialize BSS volume group configurations in service discovery (only for single-AZ mode)
    if remote_az.is_none() {
        let total_bss_nodes = num_bss_nodes.unwrap_or(TOTAL_BSS_NODES);
        initialize_bss_volume_groups(config, &barrier, total_bss_nodes)?;
    }

    // Wait for NSS formatting to complete via workflow barriers
    let expected_nss = if nss_b_id.is_some() { 2 } else { 1 };
    info!("Waiting for {expected_nss} NSS instance(s) to complete formatting...");
    barrier.wait_for_nodes(stages::NSS_FORMATTED, expected_nss, timeouts::NSS_FORMATTED)?;
    info!("All NSS instances have completed formatting");

    // Wait for NSS journal to be ready via workflow barriers
    // For NVMe journal, only active (nss-A) publishes journal-ready; standby runs mirrord
    let expected_journal_ready = if config.global.journal_type == JournalType::Nvme {
        1
    } else {
        expected_nss
    };
    info!("Waiting for {expected_journal_ready} NSS journal(s) to be ready...");
    barrier.wait_for_nodes(
        stages::NSS_JOURNAL_READY,
        expected_journal_ready,
        timeouts::NSS_JOURNAL_READY,
    )?;

    if for_bench {
        run_cmd!($BIN_PATH/rss_admin --rss-addr=127.0.0.1:8088 api-key init-test)?;
    }

    // Complete services-ready stage
    barrier.complete_stage(stages::SERVICES_READY, None)?;

    // Clear bootstrap env so restarts use default grace period
    clear_rss_bootstrap_env()?;

    Ok(())
}

fn initialize_observer_state(
    config: &BootstrapConfig,
    nss_a_id: &str,
    nss_b_id: Option<&str>,
) -> CmdResult {
    info!("Initializing observer state in service discovery");

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);

    // Get shared journal_uuid from the primary NSS (nss-A) in config
    // Both NSS and mirrord use the same journal_uuid
    let nss_nodes = config.get_node_entries("nss_server");
    let shared_journal_uuid = nss_nodes
        .and_then(|nodes| nodes.iter().find(|n| n.id == nss_a_id))
        .and_then(|n| n.journal_uuid.as_deref());

    let journal_uuid_json = shared_journal_uuid
        .map(|u| format!("\"{u}\""))
        .unwrap_or_else(|| "null".to_string());

    // Create ObserverPersistentState JSON
    let observer_state_json = if let Some(nss_b_id) = nss_b_id {
        // HA mode: active/standby - both machines share the same journal_uuid
        info!("HA mode: {nss_a_id} as active NSS, {nss_b_id} as standby mirrord");
        format!(
            r#"{{"observer_state":"active_standby","nss_machine":{{"machine_id":"{nss_a_id}","running_service":"nss","expected_role":"active","network_address":null,"journal_uuid":{journal_uuid_json}}},"mirrord_machine":{{"machine_id":"{nss_b_id}","running_service":"mirrord","expected_role":"standby","network_address":null,"journal_uuid":{journal_uuid_json}}},"last_updated":{timestamp},"version":1}}"#
        )
    } else {
        // Solo mode: single NSS
        info!("Solo mode: {nss_a_id} as solo NSS");
        format!(
            r#"{{"observer_state":"solo","nss_machine":{{"machine_id":"{nss_a_id}","running_service":"nss","expected_role":"solo","network_address":null,"journal_uuid":{journal_uuid_json}}},"mirrord_machine":{{"machine_id":"","running_service":"mirrord","expected_role":"","network_address":null,"journal_uuid":null}},"last_updated":{timestamp},"version":1}}"#
        )
    };

    if config.is_etcd_backend() {
        let etcdctl = format!("{BIN_PATH}etcdctl");
        let etcd_endpoints = get_etcd_endpoints_from_workflow(config)?;
        let key = "/fractalbits-service-discovery/observer_state";
        run_cmd!($etcdctl --endpoints=$etcd_endpoints put $key $observer_state_json >/dev/null)?;
    } else {
        let region = get_current_aws_region()?;
        // Escape JSON for DynamoDB attribute value
        let escaped_json = observer_state_json.replace('"', r#"\""#);
        let observer_state_item = format!(
            r#"{{"service_id":{{"S":"observer_state"}},"state":{{"S":"{escaped_json}"}},"version":{{"N":"1"}}}}"#
        );

        run_cmd! {
            aws dynamodb put-item
                --table-name $DDB_SERVICE_DISCOVERY_TABLE
                --item $observer_state_item
                --region $region
        }?;
    }

    info!("Observer state initialized in service discovery");
    Ok(())
}

fn initialize_bss_volume_groups(
    config: &BootstrapConfig,
    barrier: &WorkflowBarrier,
    total_bss_nodes: usize,
) -> CmdResult {
    info!("Initializing BSS volume group configurations...");

    let bss_addresses: Vec<(String, String)> = if config.is_etcd_backend() {
        info!("Getting BSS nodes from workflow barrier...");
        let bss_nodes = barrier.get_etcd_nodes()?;

        if bss_nodes.len() < total_bss_nodes {
            return Err(Error::other(format!(
                "Not enough BSS nodes registered: {} < {}",
                bss_nodes.len(),
                total_bss_nodes
            )));
        }

        bss_nodes
            .iter()
            .enumerate()
            .map(|(i, node)| (format!("bss-{}", i + 1), node.ip.clone()))
            .collect()
    } else {
        let region = get_current_aws_region()?;
        info!("Waiting for all BSS nodes to register in service discovery...");
        wait_for_all_bss_nodes(&region, total_bss_nodes)?;
        let bss_instances = get_all_bss_addresses(&region)?;
        let mut sorted_instances: Vec<_> = bss_instances.into_iter().collect();
        sorted_instances.sort_by(|a, b| a.0.cmp(&b.0));
        sorted_instances
    };

    for (instance_id, address) in bss_addresses.iter() {
        info!("BSS node: {} at {}", instance_id, address);
    }

    info!("All BSS nodes available. Initializing volume group configurations...");

    // Adjust quorum settings for single BSS node deployments
    let (data_vg_quorum_n, data_vg_quorum_r, data_vg_quorum_w) = match total_bss_nodes {
        1 => (1, 1, 1),
        n if n % DATA_VG_QUORUM_N == 0 => (DATA_VG_QUORUM_N, DATA_VG_QUORUM_R, DATA_VG_QUORUM_W),
        _ => cmd_die!(
            "Unsupported number of bss nodes (1 or $DATA_VG_QUORUM_N}*k ): $total_bss_nodes"
        ),
    };

    let (metadata_vg_quorum_n, metadata_vg_quorum_r, metadata_vg_quorum_w) = match total_bss_nodes {
        1 => (1, 1, 1),
        n if n % META_DATA_VG_QUORUM_N == 0 => (
            META_DATA_VG_QUORUM_N,
            META_DATA_VG_QUORUM_R,
            META_DATA_VG_QUORUM_W,
        ),
        n if n % DATA_VG_QUORUM_N == 0 => (DATA_VG_QUORUM_N, DATA_VG_QUORUM_R, DATA_VG_QUORUM_W),
        _ => cmd_die!(
            "Unsupported number of bss nodes (1 or $META_DATA_VG_QUORUM_N}*k ): $total_bss_nodes"
        ),
    };

    let bss_data_vg_config_json = build_volume_group_config(
        &bss_addresses,
        data_vg_quorum_n,
        data_vg_quorum_r,
        data_vg_quorum_w,
    );

    let bss_metadata_vg_config_json = build_volume_group_config(
        &bss_addresses,
        metadata_vg_quorum_n,
        metadata_vg_quorum_r,
        metadata_vg_quorum_w,
    );

    if config.is_etcd_backend() {
        let etcdctl = format!("{BIN_PATH}etcdctl");
        let etcd_endpoints = get_etcd_endpoints_from_workflow(config)?;
        let data_key = "/fractalbits-service-discovery/bss-data-vg-config";
        let metadata_key = "/fractalbits-service-discovery/bss-metadata-vg-config";
        run_cmd! {
            $etcdctl --endpoints=$etcd_endpoints put $data_key $bss_data_vg_config_json >/dev/null;
            $etcdctl --endpoints=$etcd_endpoints put $metadata_key $bss_metadata_vg_config_json >/dev/null;
        }?;
    } else {
        let region = get_current_aws_region()?;
        let bss_data_vg_config_item = format!(
            r#"{{"service_id":{{"S":"{}"}},"value":{{"S":"{}"}}}}"#,
            BSS_DATA_VG_CONFIG_KEY,
            bss_data_vg_config_json
                .replace('"', r#"\""#)
                .replace('\n', "")
        );

        run_cmd! {
            aws dynamodb put-item
                --table-name $DDB_SERVICE_DISCOVERY_TABLE
                --item $bss_data_vg_config_item
                --region $region
        }?;

        let bss_metadata_vg_config_item = format!(
            r#"{{"service_id":{{"S":"{}"}},"value":{{"S":"{}"}}}}"#,
            BSS_METADATA_VG_CONFIG_KEY,
            bss_metadata_vg_config_json
                .replace('"', r#"\""#)
                .replace('\n', "")
        );

        run_cmd! {
            aws dynamodb put-item
                --table-name $DDB_SERVICE_DISCOVERY_TABLE
                --item $bss_metadata_vg_config_item
                --region $region
        }?;
    }

    info!("BSS volume group configurations initialized in service discovery");
    Ok(())
}

fn build_volume_group_config(
    bss_addresses: &[(String, String)],
    quorum_n: usize,
    quorum_r: usize,
    quorum_w: usize,
) -> String {
    let num_volumes = bss_addresses.len() / quorum_n;

    let mut volumes = Vec::new();
    for vol_id_idx in 0..num_volumes {
        let start_idx = vol_id_idx * quorum_n;
        let end_idx = start_idx + quorum_n;

        let nodes: Vec<String> = (start_idx..end_idx)
            .map(|i| {
                format!(
                    r#"{{"node_id":"{}","ip":"{}","port":8088}}"#,
                    bss_addresses[i].0, bss_addresses[i].1
                )
            })
            .collect();

        volumes.push(format!(
            r#"{{"volume_id":{},"bss_nodes":[{}]}}"#,
            vol_id_idx + 1,
            nodes.join(",")
        ));
    }

    format!(
        r#"{{"volumes":[{}],"quorum":{{"n":{quorum_n},"r":{quorum_r},"w":{quorum_w}}}}}"#,
        volumes.join(",")
    )
}

fn wait_for_all_bss_nodes(region: &str, expected_count: usize) -> CmdResult {
    let mut i = 0;

    loop {
        i += 1;

        // Query the service discovery table to check how many BSS nodes are registered
        let result = run_fun! {
            aws dynamodb get-item
                --table-name $DDB_SERVICE_DISCOVERY_TABLE
                --key "{\"service_id\": {\"S\": \"$BSS_SERVER_KEY\"}}"
                --region $region
                2>/dev/null | jq -r ".Item.instances.M | length // 0"
        };

        match result {
            Ok(ref count_str) => {
                let count: usize = count_str.trim().parse().unwrap_or(0);
                info!("BSS nodes registered: {}/{}", count, expected_count);

                if count >= expected_count {
                    info!("All {} BSS nodes have registered", expected_count);
                    return Ok(());
                }
            }
            Err(_) => {
                info!("No BSS nodes registered yet");
            }
        }

        if i >= MAX_POLL_ATTEMPTS {
            cmd_die!("Timed out waiting for all BSS nodes to register in service discovery");
        }

        std::thread::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECONDS));
    }
}

fn get_all_bss_addresses(
    region: &str,
) -> Result<std::collections::HashMap<String, String>, std::io::Error> {
    let result = run_fun! {
        aws dynamodb get-item
            --table-name $DDB_SERVICE_DISCOVERY_TABLE
            --key "{\"service_id\": {\"S\": \"$BSS_SERVER_KEY\"}}"
            --region $region
            2>/dev/null | jq -r ".Item.instances.M | to_entries | map(\"\\(.key)=\\(.value.S)\") | .[]"
    }?;

    let mut addresses = std::collections::HashMap::new();
    for line in result.lines() {
        if let Some((instance_id, address)) = line.split_once('=') {
            addresses.insert(instance_id.to_string(), address.to_string());
        }
    }

    Ok(addresses)
}

fn initialize_az_status(config: &BootstrapConfig, remote_az: &str) -> CmdResult {
    let local_az = get_current_aws_az_id()?;

    info!("Initializing AZ status in service discovery");
    info!("Setting {local_az} and {remote_az} to Normal");

    if config.is_etcd_backend() {
        let etcdctl = format!("{BIN_PATH}etcdctl");
        let etcd_endpoints = get_etcd_endpoints_from_workflow(config)?;
        let key = "/fractalbits-service-discovery/az_status";
        let az_status_json =
            format!(r#"{{"status":{{"{local_az}":"Normal","{remote_az}":"Normal"}}}}"#);
        run_cmd!($etcdctl --endpoints=$etcd_endpoints put $key $az_status_json >/dev/null)?;
    } else {
        let region = get_current_aws_region()?;
        let az_status_item = format!(
            r#"{{"service_id":{{"S":"{}"}},"status":{{"M":{{"{local_az}":{{"S":"Normal"}},"{remote_az}":{{"S":"Normal"}}}}}}}}"#,
            AZ_STATUS_KEY
        );

        run_cmd! {
            aws dynamodb put-item
                --table-name $DDB_SERVICE_DISCOVERY_TABLE
                --item $az_status_item
                --region $region
        }?;
    }

    info!("AZ status initialized in service discovery ({local_az}: Normal, {remote_az}: Normal)");
    Ok(())
}

fn get_etcd_endpoints_from_workflow(config: &BootstrapConfig) -> Result<String, Error> {
    // First try config endpoints (for on-prem/static etcd)
    if let Ok(endpoints) = get_etcd_endpoints(config) {
        return Ok(endpoints);
    }

    // Fall back to workflow barrier discovery (for dynamic BSS etcd cluster)
    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Rss)?;
    let bss_nodes = barrier.get_etcd_nodes()?;

    if bss_nodes.is_empty() {
        return Err(Error::other("No BSS nodes registered in workflow"));
    }

    Ok(bss_nodes
        .iter()
        .map(|node| format!("http://{}:2379", node.ip))
        .collect::<Vec<_>>()
        .join(","))
}

fn wait_for_leadership() -> CmdResult {
    info!("Waiting for local root_server to become leader...");
    let mut i = 0;
    const HEALTH_PORT: u16 = 18088;

    loop {
        i += 1;

        let health_url = format!("http://localhost:{HEALTH_PORT}");
        let result = run_fun!(curl -s $health_url 2>/dev/null | jq -r ".is_leader");

        match result {
            Ok(ref response) if response.trim() == "true" => {
                info!("Local root_server has become the leader");
                break;
            }
            Ok(ref response) => {
                if i % 10 == 0 {
                    info!(
                        "Root_server not yet leader (is_leader: {}), waiting...",
                        response.trim()
                    );
                }
            }
            Err(_) => {
                if i % 10 == 0 {
                    info!("Health endpoint not yet responding, waiting...");
                }
            }
        }

        if i >= MAX_POLL_ATTEMPTS {
            cmd_die!("Timed out waiting for root_server to become leader");
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    Ok(())
}

fn create_rss_config(config: &BootstrapConfig, nss_endpoint: &str, ha_enabled: bool) -> CmdResult {
    let region = &config.global.region;
    let instance_id = get_instance_id_from_config(config)?;

    let backend = if config.is_etcd_backend() {
        "etcd"
    } else {
        "ddb"
    };

    let etcd_endpoints_line = if config.is_etcd_backend() {
        // Use workflow barrier to get etcd nodes
        let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Rss)?;
        let bss_nodes = barrier.get_etcd_nodes()?;
        if bss_nodes.is_empty() {
            return Err(Error::other(
                "No BSS nodes registered in workflow for etcd endpoints",
            ));
        }
        let endpoints: Vec<String> = bss_nodes
            .iter()
            .map(|node| format!("http://{}:2379", node.ip))
            .collect();
        format!(
            "\n# etcd endpoints for cluster connection\netcd_endpoints = {:?}",
            endpoints
        )
    } else {
        String::new()
    };

    let config_content = format!(
        r##"# Root Server Configuration

# AWS region
region = "{region}"

# Server port
server_port = 8088

# Server health port
health_port = 18088

# Metrics port
metrics_port = 18087

# API Server management port
api_server_mgmt_port = 18088

# Nss server rpc server address
nss_addr = "{nss_endpoint}:8088"

# Backend storage (ddb or etcd)
backend = "{backend}"{etcd_endpoints_line}

# Leader Election Configuration (uses the same backend as RSS: ddb or etcd)
[leader_election]
# Whether leader election is enabled
enabled = {ha_enabled}

# Instance ID for this root server
instance_id = "{instance_id}"

# Table name (for DDB) or key prefix (for etcd) for leader election
table_name = "fractalbits-leader-election"

# Key used to identify this leader election group
leader_key = "root-server-leader"

# How long a leader holds the lease before it expires (in seconds)
lease_duration_secs = 60

# How often to send heartbeats and check leadership status (in seconds)
heartbeat_interval_secs = 15

# Maximum number of retry attempts for leader election operations
max_retry_attempts = 5

# Enable monitoring and metrics collection
enable_monitoring = true

# Observer Configuration
[observer]
# Grace period (in seconds) before observer starts making state transitions
# During bootstrap, this is overridden via env var to 120s
initial_grace_period_secs = 2.0

# How often to check health and evaluate state transitions (in seconds)
heartbeat_interval_secs = 0.5

# Health data older than this threshold is considered stale (in seconds)
health_stale_threshold_secs = 5.0
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$ROOT_SERVER_CONFIG;
    }?;
    Ok(())
}

fn create_rss_bootstrap_env() -> CmdResult {
    let grace_period = BOOTSTRAP_GRACE_PERIOD_SECS;
    let content = format!("OBSERVER_INITIAL_GRACE_PERIOD_SECS={grace_period}");
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $content > ${ETC_PATH}rss.env;
    }?;
    info!("Created RSS bootstrap env file with grace period {grace_period}s");
    Ok(())
}

fn clear_rss_bootstrap_env() -> CmdResult {
    run_cmd!(echo -n "" > ${ETC_PATH}rss.env)?;
    info!("Cleared RSS bootstrap env file");
    Ok(())
}
