use super::common::*;
use crate::config::{BootstrapConfig, JournalType};
use crate::etcd_cluster::{get_cluster_state_s3, get_registered_nodes};
use cmd_lib::*;
use std::io::Error;

const COMMAND_TIMEOUT_SECONDS: u64 = 300;
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

pub fn bootstrap(
    config: &BootstrapConfig,
    is_leader: bool,
    follower_id: Option<String>,
    for_bench: bool,
) -> CmdResult {
    let nss_endpoint = &config.endpoints.nss_endpoint;
    let nss_a_id = &config.resources.nss_a_id;
    let nss_b_id = config.resources.nss_b_id.as_deref();
    let remote_az = config.aws.as_ref().and_then(|aws| aws.remote_az.as_deref());
    let num_bss_nodes = config.global.num_bss_nodes;
    let ha_enabled = config.global.rss_ha_enabled;

    if is_leader {
        bootstrap_leader(
            config,
            nss_endpoint,
            nss_a_id,
            nss_b_id,
            follower_id.as_deref(),
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
    let mut binaries = vec!["rss_admin", "root_server"];
    if config.is_etcd_backend() {
        binaries.push("etcdctl");
    }
    download_binaries(&binaries)?;
    create_rss_config(config, nss_endpoint, ha_enabled)?;
    create_systemd_unit_file("rss", false)?;
    create_ddb_register_and_deregister_service("root-server")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn bootstrap_leader(
    config: &BootstrapConfig,
    nss_endpoint: &str,
    nss_a_id: &str,
    nss_b_id: Option<&str>,
    follower_id: Option<&str>,
    remote_az: Option<&str>,
    num_bss_nodes: Option<usize>,
    ha_enabled: bool,
    for_bench: bool,
) -> CmdResult {
    let mut binaries = vec!["rss_admin", "root_server"];
    if config.is_etcd_backend() {
        binaries.push("etcdctl");
    }
    download_binaries(&binaries)?;

    // Initialize AZ status if this is a multi-AZ deployment
    if let Some(remote_az) = remote_az {
        initialize_az_status(config, remote_az)?;
    }

    create_rss_config(config, nss_endpoint, ha_enabled)?;
    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("rss", !ha_enabled || follower_id.is_some())?;
    create_ddb_register_and_deregister_service("root-server")?;

    // Initialize NSS formatting and root server startup
    // Create S3 Express buckets if remote_az is provided
    if let Some(remote_az) = remote_az {
        // Create local S3 Express bucket
        let local_az = get_current_aws_az_id()?;
        create_s3_express_bucket(&local_az, S3EXPRESS_LOCAL_BUCKET_CONFIG)?;

        // Create remote S3 Express bucket
        create_s3_express_bucket(remote_az, S3EXPRESS_REMOTE_BUCKET_CONFIG)?;
    }

    // Initialize NSS role states in service discovery
    initialize_nss_roles(config, nss_a_id, nss_b_id)?;

    // Initialize BSS volume group configurations in service discovery (only for single-AZ mode)
    if remote_az.is_none() {
        let total_bss_nodes = num_bss_nodes.unwrap_or(TOTAL_BSS_NODES);
        initialize_bss_volume_groups(config, total_bss_nodes)?;
    }

    // For nvme journal type, NSS formats locally during bootstrap - no SSM needed
    // For ebs journal type, we need to trigger NSS formatting via SSM
    if config.global.journal_type == JournalType::Ebs {
        // Format nss-B first if it exists, then nss-A
        // NSS discovers its EBS device from bootstrap config
        let bootstrap_bin = "/opt/fractalbits/bin/fractalbits-bootstrap";
        if let Some(nss_b_id) = nss_b_id {
            info!("Formatting NSS instance {nss_b_id} (standby)");
            wait_for_ssm_ready(nss_b_id);
            run_cmd_with_ssm(
                nss_b_id,
                &format!(r##"sudo bash -c "{bootstrap_bin} --format-ebs-journal &>>{CLOUD_INIT_LOG}""##),
                false,
            )?;
            info!("Successfully formatted {nss_b_id} (standby)");
        }

        // Always format nss-A
        let role = if nss_b_id.is_some() { "active" } else { "solo" };
        info!("Formatting NSS instance {nss_a_id} ({role})");
        wait_for_ssm_ready(nss_a_id);
        run_cmd_with_ssm(
            nss_a_id,
            &format!(r##"sudo bash -c "{bootstrap_bin} --format-ebs-journal &>>{CLOUD_INIT_LOG}""##),
            false,
        )?;
        info!("Successfully formatted {nss_a_id} ({role})");
    } else {
        info!("Journal type is nvme - NSS formats locally, skipping SSM formatting");
    }

    if ha_enabled {
        wait_for_leadership()?;
    } else {
        // For non-HA deployments, wait for root server to be ready
        wait_for_rss_ready()?;
    }

    // For NVMe journal type, start nss_role_agent via SSM after metadata VG is configured
    // For EBS, nss_role_agent is started by udev rule when volume is attached after format
    if config.global.journal_type == JournalType::Nvme {
        info!("Starting nss_role_agent on NSS instances via SSM (NVMe mode)");
        wait_for_ssm_ready(nss_a_id);
        run_cmd_with_ssm(
            nss_a_id,
            "sudo systemctl start nss_role_agent.service",
            false,
        )?;
        info!("Successfully started nss_role_agent on {nss_a_id}");

        if let Some(nss_b_id) = nss_b_id {
            wait_for_ssm_ready(nss_b_id);
            run_cmd_with_ssm(
                nss_b_id,
                "sudo systemctl start nss_role_agent.service",
                false,
            )?;
            info!("Successfully started nss_role_agent on {nss_b_id}");
        }
    }

    if for_bench {
        run_cmd!($BIN_PATH/rss_admin --rss-addr=127.0.0.1:8088 api-key init-test)?;
    }

    // Start follower root server if follower_id is provided
    if let Some(follower_id) = follower_id {
        start_follower_root_server(follower_id)?;

        // Only bootstrap ebs_failover service if nss_b_id exists
        // if let Some(nss_b_id) = nss_b_id {
        //     bootstrap_ebs_failover_service(nss_a_id, nss_b_id, volume_a_id)?;
        // }
    }
    Ok(())
}

fn initialize_nss_roles(
    config: &BootstrapConfig,
    nss_a_id: &str,
    nss_b_id: Option<&str>,
) -> CmdResult {
    info!("Initializing NSS role states in service discovery");

    let nss_roles_json = if let Some(nss_b_id) = nss_b_id {
        info!("Setting {nss_a_id} as active");
        info!("Setting {nss_b_id} as standby");
        format!(r#"{{"states":{{"{nss_a_id}":"active","{nss_b_id}":"standby"}}}}"#)
    } else {
        info!("Setting {nss_a_id} as solo");
        format!(r#"{{"states":{{"{nss_a_id}":"solo"}}}}"#)
    };

    if config.is_etcd_backend() {
        wait_for_etcd_cluster(config)?;
        let etcdctl = format!("{BIN_PATH}etcdctl");
        let etcd_endpoints = get_etcd_endpoints(config)?;
        let key = "/fractalbits-service-discovery/nss_roles";
        run_cmd!($etcdctl --endpoints=$etcd_endpoints put $key $nss_roles_json >/dev/null)?;
    } else {
        let region = get_current_aws_region()?;
        let nss_roles_item = if let Some(nss_b_id) = nss_b_id {
            format!(
                r#"{{"service_id":{{"S":"{}"}},"states":{{"M":{{"{nss_a_id}":{{"S":"active"}},"{nss_b_id}":{{"S":"standby"}}}}}}}}"#,
                NSS_ROLES_KEY
            )
        } else {
            format!(
                r#"{{"service_id":{{"S":"{}"}},"states":{{"M":{{"{nss_a_id}":{{"S":"solo"}}}}}}}}"#,
                NSS_ROLES_KEY
            )
        };

        run_cmd! {
            aws dynamodb put-item
                --table-name $DDB_SERVICE_DISCOVERY_TABLE
                --item $nss_roles_item
                --region $region
        }?;
    }

    info!("NSS roles initialized in service discovery");
    Ok(())
}

fn initialize_bss_volume_groups(config: &BootstrapConfig, total_bss_nodes: usize) -> CmdResult {
    info!("Initializing BSS volume group configurations...");

    let bss_addresses: Vec<(String, String)> = if config.is_etcd_backend() {
        let etcd_config = config
            .etcd
            .as_ref()
            .ok_or_else(|| Error::other("etcd config missing for etcd backend"))?;

        info!("Waiting for etcd cluster to become active via S3...");
        wait_for_etcd_cluster_active_s3(&etcd_config.s3_bucket, &etcd_config.cluster_id)?;

        info!("Getting BSS nodes from S3 registry...");
        let bss_nodes = get_registered_nodes(&etcd_config.s3_bucket, &etcd_config.cluster_id)?;

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
        let etcd_endpoints = get_etcd_endpoints(config)?;
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
        wait_for_etcd_cluster(config)?;
        let etcdctl = format!("{BIN_PATH}etcdctl");
        let etcd_endpoints = get_etcd_endpoints(config)?;
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

fn wait_for_etcd_cluster(config: &BootstrapConfig) -> CmdResult {
    let etcd_config = config
        .etcd
        .as_ref()
        .ok_or_else(|| Error::other("etcd config missing"))?;

    wait_for_etcd_cluster_active_s3(&etcd_config.s3_bucket, &etcd_config.cluster_id)?;

    info!("Waiting for etcd cluster to be healthy...");
    let etcd_endpoints = get_etcd_endpoints(config)?;
    let etcdctl = format!("{BIN_PATH}etcdctl");
    let mut i = 0;

    loop {
        i += 1;

        let result = run_cmd!($etcdctl --endpoints=$etcd_endpoints endpoint health 2>/dev/null);
        if result.is_ok() {
            info!("etcd cluster is healthy");
            return Ok(());
        }

        if i % 10 == 0 {
            info!("etcd cluster not yet healthy, waiting... (attempt {i})");
        }

        if i >= MAX_POLL_ATTEMPTS {
            cmd_die!("Timed out waiting for etcd cluster to be healthy");
        }

        std::thread::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECONDS));
    }
}

fn wait_for_etcd_cluster_active_s3(bucket: &str, cluster_id: &str) -> CmdResult {
    info!("Waiting for etcd cluster to become active via S3...");
    let mut i = 0;

    loop {
        i += 1;

        match get_cluster_state_s3(bucket, cluster_id) {
            Ok(state) if state == "active" => {
                info!("etcd cluster is active");
                return Ok(());
            }
            Ok(state) => {
                if i % 10 == 0 {
                    info!("etcd cluster state: {state}, waiting... (attempt {i})");
                }
            }
            Err(_) => {
                if i % 10 == 0 {
                    info!("etcd cluster state not available, waiting... (attempt {i})");
                }
            }
        }

        if i >= MAX_POLL_ATTEMPTS {
            cmd_die!("Timed out waiting for etcd cluster to become active");
        }

        std::thread::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECONDS));
    }
}

fn get_etcd_endpoints(config: &BootstrapConfig) -> Result<String, Error> {
    let etcd_config = config
        .etcd
        .as_ref()
        .ok_or_else(|| Error::other("etcd config missing"))?;

    let bss_nodes = get_registered_nodes(&etcd_config.s3_bucket, &etcd_config.cluster_id)?;
    if bss_nodes.is_empty() {
        return Err(Error::other("No BSS nodes registered in S3"));
    }

    Ok(bss_nodes
        .iter()
        .map(|node| format!("http://{}:2379", node.ip))
        .collect::<Vec<_>>()
        .join(","))
}

fn wait_for_rss_ready() -> CmdResult {
    info!("Waiting for root_server to be ready...");
    let mut i = 0;
    const RSS_PORT: u16 = 8088;

    loop {
        i += 1;
        if check_port_ready("localhost", RSS_PORT) {
            info!("Root_server is ready (port {} responding)", RSS_PORT);
            return Ok(());
        }

        if i % 10 == 0 {
            info!("Root_server not yet ready, waiting... (attempt {i})");
        }

        if i >= MAX_POLL_ATTEMPTS {
            cmd_die!("Timed out waiting for root_server to be ready");
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn wait_for_leadership() -> CmdResult {
    info!("Waiting for local root_server to become leader...");
    let mut i = 0;
    const HEALTH_PORT: u16 = 18088;

    loop {
        i += 1;

        // Check if the health endpoint is responding and reports leadership
        let health_url = format!("http://localhost:{HEALTH_PORT}");
        let result = run_fun!(curl -s $health_url 2>/dev/null | jq -r ".is_leader");

        match result {
            Ok(ref response) if response.trim() == "true" => {
                info!("Local root_server has become the leader");
                break;
            }
            Ok(ref response) => {
                info!(
                    "Root_server not yet leader (is_leader: {}), waiting...",
                    response.trim()
                );
            }
            Err(_) => {
                info!("Health endpoint not yet responding, waiting...");
            }
        }

        if i >= MAX_POLL_ATTEMPTS {
            cmd_die!("Timed out waiting for root_server to become leader");
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    Ok(())
}

fn start_follower_root_server(follower_id: &str) -> CmdResult {
    info!("Starting rss service on follower instance {follower_id}");
    wait_for_ssm_ready(follower_id);

    // The follower instance should have already run its own bootstrap process
    // (with no follower_id parameter) to set up configs and systemd unit file
    // We just need to start the service
    run_cmd_with_ssm(follower_id, "sudo systemctl start rss.service", false)?;

    info!("Successfully started rss service on follower {follower_id}");
    Ok(())
}

fn wait_for_ssm_ready(instance_id: &str) {
    let mut i = 0;
    loop {
        i += 1;
        let result = run_fun! {
            aws ssm describe-instance-information
                --filters "Key=InstanceIds,Values=$instance_id"

                --output json | jq -r ".InstanceInformationList[0].PingStatus"
        };
        info!("Ping {instance_id} status: {result:?}");
        match result {
            Ok(ref s) if s == "Online" => break,
            _ => std::thread::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECONDS)),
        };

        if i >= MAX_POLL_ATTEMPTS {
            cmd_die!("Timed out while waiting for SSM after $MAX_POLL_ATTEMPTS attempts.");
        }
    }

    info!("Waiting for {instance_id} cloud init to be done");
    let mut i = 0;
    loop {
        i += 1;
        let result = run_cmd_with_ssm(instance_id, &format!("test -f {BOOTSTRAP_DONE_FILE}"), true);
        match result {
            Ok(()) => break,
            _ => {
                std::thread::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECONDS));
            }
        }

        if i >= MAX_POLL_ATTEMPTS {
            cmd_die!("Timed out while waiting for cloud init after $MAX_POLL_ATTEMPTS attempts.");
        }
    }
}

fn run_cmd_with_ssm(instance_id: &str, cmd: &str, quiet: bool) -> CmdResult {
    if !quiet {
        info!("Running {cmd} on {instance_id} with SSM");
    };
    let command_id = run_fun! {
        aws ssm send-command
            --instance-ids "$instance_id"
            --document-name "AWS-RunShellScript"
            --parameters "commands=[\'$cmd\']"
            --timeout-seconds "$COMMAND_TIMEOUT_SECONDS"
            --query "Command.CommandId"
            --output text
    }?;
    if !quiet {
        info!(
            "Command sent to {instance_id} successfully. Command ID: {command_id}. Polling for results..."
        );
    }
    let mut i = 0;
    loop {
        i += 1;
        let invocation_json = run_fun! {
            aws ssm get-command-invocation
                --command-id "$command_id"
                --instance-id "$instance_id"
        }?;
        let status = run_fun!(echo $invocation_json | jq -r .Status)?;

        if !quiet {
            info!("Command status from {instance_id} is: {status}");
        }
        match status.as_ref() {
            "Success" => break,
            "Failed" => {
                if !quiet {
                    warn!("Command execution failed on the remote instance.");
                }
                let error_output = run_fun!(echo $invocation_json | jq -r .StandardErrorContent)?;
                return Err(Error::other(format!(
                    "Remote Error Output:\n---\n{error_output}---"
                )));
            }
            "TimedOut" => {
                return Err(Error::other(format!(
                    "Command timed out on the remote instance after {COMMAND_TIMEOUT_SECONDS} seconds."
                )));
            }
            _ => {
                // Status is Pending, InProgress, Cancelling, etc.
                std::thread::sleep(std::time::Duration::from_secs(POLL_INTERVAL_SECONDS));
            }
        }
        if i >= MAX_POLL_ATTEMPTS {
            return Err(Error::other(format!(
                "Timed out polling for command result after {MAX_POLL_ATTEMPTS} attempts."
            )));
        }
    }

    Ok(())
}

#[allow(unused)]
fn bootstrap_ebs_failover_service(nss_a_id: &str, nss_b_id: &str, volume_id: &str) -> CmdResult {
    let service_name = "ebs-failover";

    let config_content = format!(
        r##"nss_a_id = "{nss_a_id}"    # Primary instance ID
nss_b_id = "{nss_b_id}"  # Secondary instance ID
volume_id = "{volume_id}"            # EBS volume ID
device_name = "/dev/xvdf"                      # Device name for OS

dynamodb_table_name = "ebs-failover-state"     # Name of the pre-created DynamoDB table

check_interval_seconds = 30                    # How often to perform health checks
health_check_timeout_seconds = 15              # Timeout for each health check attempt
post_failover_delay_seconds = 60               # Pause after a failover before resuming checks

enable_fencing = true                          # Set to true to enable STONITH
fencing_action = "stop"                        # "stop" or "terminate"
fencing_timeout_seconds = 300                  # Max time to wait for instance to stop/terminate
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/${service_name}-config.toml;
    }?;

    create_systemd_unit_file(service_name, true)?;
    Ok(())
}

fn create_rss_config(config: &BootstrapConfig, nss_endpoint: &str, ha_enabled: bool) -> CmdResult {
    let region = get_current_aws_region()?;
    let instance_id = get_instance_id()?;

    let backend = if config.is_etcd_backend() {
        "etcd"
    } else {
        "ddb"
    };

    let etcd_endpoints_line = if let Some(etcd_config) = &config.etcd {
        if etcd_config.enabled {
            info!("Waiting for etcd cluster to become active before creating RSS config...");
            wait_for_etcd_cluster_active_s3(&etcd_config.s3_bucket, &etcd_config.cluster_id)?;

            let bss_nodes = get_registered_nodes(&etcd_config.s3_bucket, &etcd_config.cluster_id)?;
            if bss_nodes.is_empty() {
                return Err(Error::other(
                    "No BSS nodes registered in S3 for etcd endpoints",
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
        }
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
# Increased to 60s for better stability against transient network issues
lease_duration_secs = 60

# How often to send heartbeats and check leadership status (in seconds)
# Set to 15s for less aggressive polling while maintaining responsiveness
# With 50% renewal threshold, renewal happens at 30s, giving 30s buffer
heartbeat_interval_secs = 15

# Maximum number of retry attempts for leader election operations
# Increased to 5 for better startup resilience
max_retry_attempts = 5

# Enable monitoring and metrics collection
enable_monitoring = true
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$ROOT_SERVER_CONFIG;
    }?;
    Ok(())
}
