use super::common::*;
use crate::config::{BootstrapConfig, DeployTarget};
use crate::stage_helpers::{InstancesReadyStage, ServicesReadyStageDef};
use crate::workflow::{StageCompletion, WorkflowBarrier, WorkflowServiceType, stages};
use cmd_lib::*;
use std::io::Error;
use xtask_common::stages::{VerifiedGlobalDep, VerifiedGlobalStage, VerifiedNodeDep};

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

const BOOTSTRAP_GRACE_PERIOD_SECS: u64 = 300;

struct ServicesReadyStage;

impl ServicesReadyStage {
    const RSS_INITIALIZED: VerifiedGlobalDep =
        const { stages::SERVICES_READY.global_dep("rss-initialized") };
    const NSS_FORMATTED: VerifiedNodeDep =
        const { stages::SERVICES_READY.node_dep("nss-formatted") };
    const NSS_JOURNAL_READY: VerifiedNodeDep =
        const { stages::SERVICES_READY.node_dep("nss-journal-ready") };

    fn wait_for_rss_initialized(barrier: &WorkflowBarrier) -> CmdResult {
        ServicesReadyStageDef::wait_for_global_dep(barrier, Self::RSS_INITIALIZED)
    }

    fn wait_for_nss_formatted(
        barrier: &WorkflowBarrier,
        expected: usize,
    ) -> Result<Vec<StageCompletion>, Error> {
        ServicesReadyStageDef::wait_for_node_dep(barrier, Self::NSS_FORMATTED, expected)
    }

    fn wait_for_nss_journal_ready(
        barrier: &WorkflowBarrier,
        expected: usize,
    ) -> Result<Vec<StageCompletion>, Error> {
        ServicesReadyStageDef::wait_for_node_dep(barrier, Self::NSS_JOURNAL_READY, expected)
    }

    fn complete(barrier: &WorkflowBarrier) -> CmdResult {
        ServicesReadyStageDef::complete(barrier)
    }
}

struct RssInitializedStage;

impl RssInitializedStage {
    const ETCD_READY: VerifiedGlobalDep =
        const { stages::RSS_INITIALIZED.global_dep("etcd-ready") };
    const STAGE: VerifiedGlobalStage = const { stages::RSS_INITIALIZED.global_stage() };

    fn wait_for_etcd_ready(barrier: &WorkflowBarrier) -> CmdResult {
        barrier.wait_for_global(Self::ETCD_READY)
    }

    fn complete(barrier: &WorkflowBarrier) -> CmdResult {
        barrier.complete_global_stage(Self::STAGE, None)
    }
}

struct MetadataVgReadyStage;

impl MetadataVgReadyStage {
    const INSTANCES_READY: VerifiedNodeDep =
        const { stages::METADATA_VG_READY.node_dep("instances-ready") };
    const STAGE: VerifiedGlobalStage = const { stages::METADATA_VG_READY.global_stage() };

    fn wait_for_instances_ready(
        barrier: &WorkflowBarrier,
        expected: usize,
    ) -> Result<Vec<StageCompletion>, Error> {
        barrier.wait_for_nodes(Self::INSTANCES_READY, expected)
    }

    fn complete(barrier: &WorkflowBarrier) -> CmdResult {
        barrier.complete_global_stage(Self::STAGE, None)
    }
}

pub fn bootstrap(
    config: &BootstrapConfig,
    is_leader: bool,
    for_bench: bool,
    cli_nss_a_id: Option<&str>,
    cli_nss_b_id: Option<&str>,
    cli_nss_a_ip: Option<&str>,
) -> CmdResult {
    // CLI-provided IP takes precedence (injected via UserData on cloud deployments
    // where the instance IP is known at CDK/Terraform synthesis time but not in
    // the bootstrap config, which is generated before instances are created).
    let nss_endpoint = cli_nss_a_ip
        .filter(|s| !s.is_empty())
        .or_else(|| {
            config
                .endpoints
                .as_ref()
                .and_then(|e| e.nss_endpoint.as_deref())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or("");
    let resources = config.get_resources();
    // CLI-provided IDs take precedence (AWS cloud path); fall back to TOML resources (on-prem path)
    let nss_a_id_owned;
    let nss_a_id = if let Some(id) = cli_nss_a_id {
        id
    } else {
        nss_a_id_owned = resources.nss_a_id.clone();
        &nss_a_id_owned
    };
    let nss_b_id = cli_nss_b_id.or(resources.nss_b_id.as_deref());
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
        )?;
        Ok(())
    } else {
        bootstrap_follower(config, nss_endpoint, ha_enabled)
    }
}

fn bootstrap_follower(config: &BootstrapConfig, nss_endpoint: &str, ha_enabled: bool) -> CmdResult {
    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Rss)?;

    // Complete instances-ready stage
    InstancesReadyStage::complete(&barrier)?;

    let mut binaries = vec!["rss_admin", "root_server"];
    if config.is_etcd_backend() {
        binaries.push("etcdctl");
    }
    download_binaries(config, &binaries)?;

    // Wait for leader to initialize RSS
    info!("Follower waiting for RSS leader to initialize...");
    ServicesReadyStage::wait_for_rss_initialized(&barrier)?;

    create_rss_config(config, nss_endpoint, ha_enabled)?;
    create_rss_bootstrap_env()?;
    create_systemd_unit_file("rss", true)?; // Start immediately
    register_service(config, "root-server")?;

    // Complete services-ready stage
    ServicesReadyStage::complete(&barrier)?;

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
    InstancesReadyStage::complete(&barrier)?;

    // Wait for etcd cluster if using etcd backend
    if config.is_etcd_backend() {
        info!("Waiting for etcd cluster to be ready...");
        RssInitializedStage::wait_for_etcd_ready(&barrier)?;
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
    initialize_observer_state(config, nss_a_id, nss_endpoint)?;

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
    RssInitializedStage::complete(&barrier)?;

    // Initialize BSS volume group configurations in service discovery (only for single-AZ mode)
    if remote_az.is_none() {
        let total_bss_nodes = num_bss_nodes.unwrap_or(TOTAL_BSS_NODES);
        initialize_bss_volume_groups(config, &barrier, total_bss_nodes)?;
    }

    // Signal metadata VG ready - NSS active nodes wait for this before starting nss_role_agent
    // This ensures metadata_vg_config is available when nss_role_agent calls wait_for_metadata_vg_ready()
    MetadataVgReadyStage::complete(&barrier)?;

    // Wait for SERVICES_READY dependencies
    // Wait for NSS formatting to complete via workflow barriers
    let expected_nss = if nss_b_id.is_some() { 2 } else { 1 };
    info!("Waiting for {expected_nss} NSS instance(s) to complete formatting...");
    ServicesReadyStage::wait_for_nss_formatted(&barrier, expected_nss)?;
    info!("All NSS instances have completed formatting");

    // Wait for NSS journal to be ready via workflow barriers
    // For NVMe HA: only active (nss-0) signals journal-ready; standby is idle
    // For EBS HA: only active (nss-0) signals journal-ready; standby is idle
    // For solo (any journal type): the single node signals
    let expected_journal_ready = if nss_b_id.is_some() {
        1 // HA mode: only active node signals journal-ready
    } else {
        expected_nss // Solo: the single node signals
    };
    info!("Waiting for {expected_journal_ready} NSS journal(s) to be ready...");
    ServicesReadyStage::wait_for_nss_journal_ready(&barrier, expected_journal_ready)?;

    if for_bench {
        run_cmd!($BIN_PATH/rss_admin --rss-addr=127.0.0.1:8088 api-key init-test)?;
    }

    // Complete services-ready stage
    ServicesReadyStage::complete(&barrier)?;

    // Clear bootstrap env so restarts use default grace period
    clear_rss_bootstrap_env()?;

    Ok(())
}

fn initialize_observer_state(
    config: &BootstrapConfig,
    nss_a_id: &str,
    nss_endpoint: &str,
) -> CmdResult {
    info!("Initializing journal config and nss-store in service discovery");

    // Get shared journal_uuid: prefer per-node entry (on-prem TOML path), fall back to global config
    let nss_nodes = config.get_node_entries("nss_server");
    let node_journal_uuid = nss_nodes
        .and_then(|nodes| nodes.iter().find(|n| n.id == nss_a_id))
        .and_then(|n| n.journal_uuid.clone());
    let shared_journal_uuid = node_journal_uuid
        .as_deref()
        .or(config.global.journal_uuid.as_deref());

    // Initialize journal config in service discovery with running_nss_id set to nss_a_id
    if let Some(journal_uuid) = shared_journal_uuid {
        let journal_size: u64 = 4 * 1024 * 1024 * 1024; // 4GB for cloud deployment
        let journal_config_json = format!(
            r#"[{{"journal_uuid":"{}","device_id":1,"journal_size":{},"version":1,"running_nss_id":"{}"}}]"#,
            journal_uuid, journal_size, nss_a_id
        );

        if config.is_etcd_backend() {
            let etcdctl = format!("{BIN_PATH}etcdctl");
            let etcd_endpoints = get_etcd_endpoints_from_workflow(config)?;
            let key = "/fractalbits-service-discovery/journal-configs";
            run_cmd!($etcdctl --endpoints=$etcd_endpoints put $key $journal_config_json >/dev/null)?;
        } else if config.is_firestore_backend() {
            let escaped = journal_config_json.replace('"', r#"\""#);
            let fields_json = format!(
                r#"{{"fields":{{"value":{{"stringValue":"{escaped}"}},"version":{{"integerValue":"1"}}}}}}"#
            );
            firestore_put_document(
                config,
                "fractalbits-service-discovery",
                "journal-configs",
                &fields_json,
            )?;
        } else {
            let region = get_current_aws_region()?;
            let escaped = journal_config_json.replace('"', r#"\""#);
            let journal_config_item = format!(
                r#"{{"service_id":{{"S":"journal-configs"}},"value":{{"S":"{escaped}"}}}}"#
            );
            run_cmd! {
                aws dynamodb put-item
                    --table-name $DDB_SERVICE_DISCOVERY_TABLE
                    --item $journal_config_item
                    --region $region
            }?;
        }
        info!("Journal config initialized in service discovery");
    }

    // Initialize nss-store with the NSS address
    if !nss_endpoint.is_empty() {
        let nss_store_json =
            format!(r#"{{"nodes":{{"{nss_a_id}":{{"network_address":"{nss_endpoint}:8088"}}}}}}"#);

        if config.is_etcd_backend() {
            let etcdctl = format!("{BIN_PATH}etcdctl");
            let etcd_endpoints = get_etcd_endpoints_from_workflow(config)?;
            let key = "/fractalbits-service-discovery/nss-store";
            run_cmd!($etcdctl --endpoints=$etcd_endpoints put $key $nss_store_json >/dev/null)?;
        } else if config.is_firestore_backend() {
            let escaped = nss_store_json.replace('"', r#"\""#);
            let fields_json = format!(
                r#"{{"fields":{{"value":{{"stringValue":"{escaped}"}},"version":{{"integerValue":"1"}}}}}}"#
            );
            firestore_put_document(
                config,
                "fractalbits-service-discovery",
                "nss-store",
                &fields_json,
            )?;
        } else {
            let region = get_current_aws_region()?;
            let escaped = nss_store_json.replace('"', r#"\""#);
            let nss_store_item =
                format!(r#"{{"service_id":{{"S":"nss-store"}},"value":{{"S":"{escaped}"}}}}"#);
            run_cmd! {
                aws dynamodb put-item
                    --table-name $DDB_SERVICE_DISCOVERY_TABLE
                    --item $nss_store_item
                    --region $region
            }?;
        }
        info!("NSS store initialized in service discovery");
    }

    // Initialize observer leader fence token
    if config.is_etcd_backend() {
        let etcdctl = format!("{BIN_PATH}etcdctl");
        let etcd_endpoints = get_etcd_endpoints_from_workflow(config)?;
        let key = "/fractalbits-service-discovery/observer-leader-fence";
        run_cmd!($etcdctl --endpoints=$etcd_endpoints put $key "0" >/dev/null)?;
    } else if config.is_firestore_backend() {
        let fields_json = r#"{"fields":{"value":{"integerValue":"0"}}}"#;
        firestore_put_document(
            config,
            "fractalbits-service-discovery",
            "observer-leader-fence",
            fields_json,
        )?;
    } else {
        let region = get_current_aws_region()?;
        let fence_item =
            r#"{"service_id":{"S":"observer-leader-fence"},"value":{"N":"0"}}"#;
        run_cmd! {
            aws dynamodb put-item
                --table-name $DDB_SERVICE_DISCOVERY_TABLE
                --item $fence_item
                --region $region
        }?;
    }
    info!("Observer leader fence token initialized in service discovery");

    Ok(())
}

fn initialize_bss_volume_groups(
    config: &BootstrapConfig,
    barrier: &WorkflowBarrier,
    total_bss_nodes: usize,
) -> CmdResult {
    info!("Initializing BSS volume group configurations...");

    let bss_addresses: Vec<(String, String)> = if config.is_etcd_backend() {
        info!("Getting BSS nodes from workflow stage completions...");
        let completions =
            barrier.get_stage_completions(&stages::ETCD_NODES_REGISTERED.key_name())?;
        let bss_ips = StageCompletion::extract_metadata_field(&completions, "ip");

        if bss_ips.len() < total_bss_nodes {
            return Err(Error::other(format!(
                "Not enough BSS nodes registered: {} < {}",
                bss_ips.len(),
                total_bss_nodes
            )));
        }

        bss_ips
            .iter()
            .enumerate()
            .map(|(i, ip)| (format!("bss-{}", i + 1), ip.clone()))
            .collect()
    } else if config.is_firestore_backend() {
        info!("Getting BSS nodes from bootstrap config...");
        // Wait for BSS nodes to complete instances-ready stage via workflow
        info!("Waiting for {total_bss_nodes} BSS node(s) to be ready...");
        MetadataVgReadyStage::wait_for_instances_ready(barrier, total_bss_nodes + 1)
            .unwrap_or_default(); // Best effort - RSS already counted
        let bss_ips = get_service_ips_with_backend(config, "bss-server", total_bss_nodes);
        bss_ips
            .into_iter()
            .enumerate()
            .map(|(i, ip)| (format!("bss-{}", i + 1), ip))
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

    let bss_data_vg_config_json = build_data_volume_group_config(
        &bss_addresses,
        data_vg_quorum_n,
        data_vg_quorum_r,
        data_vg_quorum_w,
    );

    let bss_metadata_vg_config_json = build_metadata_volume_group_config(
        &bss_addresses,
        metadata_vg_quorum_n,
        metadata_vg_quorum_r,
        metadata_vg_quorum_w,
    );

    // Journal VG uses the same topology as metadata VG
    let bss_journal_vg_config_json = build_metadata_volume_group_config(
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
        let journal_key = "/fractalbits-service-discovery/bss-journal-vg-config";
        run_cmd! {
            $etcdctl --endpoints=$etcd_endpoints put $data_key $bss_data_vg_config_json >/dev/null;
            $etcdctl --endpoints=$etcd_endpoints put $metadata_key $bss_metadata_vg_config_json >/dev/null;
            $etcdctl --endpoints=$etcd_endpoints put $journal_key $bss_journal_vg_config_json >/dev/null;
        }?;
    } else if config.is_firestore_backend() {
        let data_escaped = bss_data_vg_config_json
            .replace('"', r#"\""#)
            .replace('\n', "");
        let data_fields = format!(r#"{{"fields":{{"value":{{"stringValue":"{data_escaped}"}}}}}}"#);
        firestore_put_document(
            config,
            "fractalbits-service-discovery",
            BSS_DATA_VG_CONFIG_KEY,
            &data_fields,
        )?;

        let meta_escaped = bss_metadata_vg_config_json
            .replace('"', r#"\""#)
            .replace('\n', "");
        let meta_fields = format!(r#"{{"fields":{{"value":{{"stringValue":"{meta_escaped}"}}}}}}"#);
        firestore_put_document(
            config,
            "fractalbits-service-discovery",
            BSS_METADATA_VG_CONFIG_KEY,
            &meta_fields,
        )?;

        let journal_escaped = bss_journal_vg_config_json
            .replace('"', r#"\""#)
            .replace('\n', "");
        let journal_fields =
            format!(r#"{{"fields":{{"value":{{"stringValue":"{journal_escaped}"}}}}}}"#);
        firestore_put_document(
            config,
            "fractalbits-service-discovery",
            BSS_JOURNAL_VG_CONFIG_KEY,
            &journal_fields,
        )?;
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

        let bss_journal_vg_config_item = format!(
            r#"{{"service_id":{{"S":"{}"}},"value":{{"S":"{}"}}}}"#,
            BSS_JOURNAL_VG_CONFIG_KEY,
            bss_journal_vg_config_json
                .replace('"', r#"\""#)
                .replace('\n', "")
        );

        run_cmd! {
            aws dynamodb put-item
                --table-name $DDB_SERVICE_DISCOVERY_TABLE
                --item $bss_journal_vg_config_item
                --region $region
        }?;
    }

    info!("BSS volume group configurations initialized in service discovery");
    Ok(())
}

fn build_data_volume_group_config(
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
            r#"{{"volume_id":{},"bss_nodes":[{}],"mode":{{"type":"replicated","n":{quorum_n},"r":{quorum_r},"w":{quorum_w}}}}}"#,
            vol_id_idx + 1,
            nodes.join(",")
        ));
    }

    format!(r#"{{"volumes":[{}]}}"#, volumes.join(","))
}

fn build_metadata_volume_group_config(
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
    let completions = barrier.get_stage_completions(&stages::ETCD_NODES_REGISTERED.key_name())?;
    let bss_ips = StageCompletion::extract_metadata_field(&completions, "ip");

    if bss_ips.is_empty() {
        return Err(Error::other("No BSS nodes registered in workflow"));
    }

    Ok(bss_ips
        .iter()
        .map(|ip| format!("http://{ip}:2379"))
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
    let instance_id = get_instance_id(config.global.deploy_target)?;

    let backend = if config.is_etcd_backend() {
        "etcd"
    } else if config.is_firestore_backend() {
        "firestore"
    } else {
        "ddb"
    };

    let firestore_config_lines = if config.is_firestore_backend() {
        let gcp = config
            .gcp
            .as_ref()
            .expect("GCP config required for Firestore backend");
        let project_id = &gcp.project_id;
        let database_id = gcp.firestore_database.as_deref().unwrap_or("fractalbits");
        format!(
            "\n# Firestore configuration\nfirestore_project_id = \"{project_id}\"\nfirestore_database_id = \"{database_id}\""
        )
    } else {
        String::new()
    };

    let etcd_endpoints_line = if config.is_etcd_backend() {
        // Use workflow stage completions to get etcd node IPs
        let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Rss)?;
        let completions =
            barrier.get_stage_completions(&stages::ETCD_NODES_REGISTERED.key_name())?;
        let bss_ips = StageCompletion::extract_metadata_field(&completions, "ip");
        if bss_ips.is_empty() {
            return Err(Error::other(
                "No BSS nodes registered in workflow for etcd endpoints",
            ));
        }
        let endpoints: Vec<String> = bss_ips
            .iter()
            .map(|ip| format!("http://{ip}:2379"))
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

# Backend storage (ddb, etcd, or firestore)
backend = "{backend}"{etcd_endpoints_line}{firestore_config_lines}

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
