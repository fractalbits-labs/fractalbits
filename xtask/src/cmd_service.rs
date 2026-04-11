use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

use crate::InitConfig;
use crate::etcd_utils::{ensure_etcd_local, resolve_etcd_bin};
use crate::firestore_utils;
use crate::*;
use colored::*;
use uuid::Uuid;
use xtask_common::{
    LOCAL_DDB_ENVS, LOCAL_DDB_ENVS_SYSTEMD, create_bss_dirs, create_nss_dirs,
    generate_bss_data_vg_config, generate_bss_journal_vg_config, generate_bss_metadata_vg_config,
};

pub fn init_service(
    service: ServiceName,
    build_mode: BuildMode,
    init_config: &InitConfig,
) -> CmdResult {
    stop_service(service)?;

    // We are using minio to test large blob IO
    ensure_minio()?;

    // Create systemd unit files for the services being initialized
    create_systemd_unit_files_for_init(service, build_mode, init_config)?;

    let init_ddb_local = || -> CmdResult {
        ensure_dynamodb_local()?;
        run_cmd! {
            rm -f data/rss/shared-local-instance.db;
            mkdir -p data/rss;
        }?;
        start_service(ServiceName::DdbLocal)?;

        // Create main keys-and-buckets table
        const DDB_TABLE_NAME: &str = "fractalbits-api-keys-and-buckets";
        run_cmd! {
            info "Initializing table: $DDB_TABLE_NAME ...";
            $[LOCAL_DDB_ENVS]
            aws dynamodb create-table
                --table-name $DDB_TABLE_NAME
                --attribute-definitions AttributeName=id,AttributeType=S
                --key-schema AttributeName=id,KeyType=HASH
                --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 >/dev/null;
        }?;

        // Create leader election table for root server
        const LEADER_TABLE_NAME: &str = "fractalbits-leader-election";
        run_cmd! {
            info "Initializing leader election table: $LEADER_TABLE_NAME ...";
            $[LOCAL_DDB_ENVS]
            aws dynamodb create-table
                --table-name $LEADER_TABLE_NAME
                --attribute-definitions AttributeName=key,AttributeType=S
                --key-schema AttributeName=key,KeyType=HASH
                --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 >/dev/null;
        }?;

        // Create observer leader election table
        const OBSERVER_LEADER_TABLE_NAME: &str = "fractalbits-leader-election-observer";
        run_cmd! {
            info "Initializing observer leader election table: $OBSERVER_LEADER_TABLE_NAME ...";
            $[LOCAL_DDB_ENVS]
            aws dynamodb create-table
                --table-name $OBSERVER_LEADER_TABLE_NAME
                --attribute-definitions AttributeName=key,AttributeType=S
                --key-schema AttributeName=key,KeyType=HASH
                --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 >/dev/null;
        }?;

        // Create service-discovery table for NSS role states
        const SERVICE_DISCOVERY_TABLE: &str = "fractalbits-service-discovery";
        run_cmd! {
            info "Creating service-discovery table: $SERVICE_DISCOVERY_TABLE ...";
            $[LOCAL_DDB_ENVS]
            aws dynamodb create-table
                --table-name $SERVICE_DISCOVERY_TABLE
                --attribute-definitions AttributeName=service_id,AttributeType=S
                --key-schema AttributeName=service_id,KeyType=HASH
                --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 >/dev/null;
        }?;

        // Initialize observer_state in service-discovery table.
        // active/standby with idle standby: role agent reports standby health.
        // Fields match root_server's ObserverPersistentState and MachineState structs.
        let observer_state_json = r#"{"observer_state":"active_standby","nss_machine":{"machine_id":"nss-0","running_service":"nss","expected_role":"active","network_address":"127.0.0.1:8087"},"standby_machine":{"machine_id":"nss-1","running_service":"noop","expected_role":"standby","network_address":null},"version":1,"last_updated":0,"nss_node_map":{"nss-0":1,"nss-1":2},"next_nss_node_id":3}"#;
        let observer_state_item = format!(
            r#"{{"service_id":{{"S":"observer_state"}},"state":{{"S":"{}"}}}}"#,
            observer_state_json.replace('"', "\\\"")
        );

        run_cmd! {
            info "Initializing observer_state in service-discovery table ...";
            $[LOCAL_DDB_ENVS]
            aws dynamodb put-item
                --table-name $SERVICE_DISCOVERY_TABLE
                --item $observer_state_item >/dev/null;
        }?;

        // Initialize AZ status in service-discovery table (using mock AZ names for local testing)
        let az_status_item = r#"{"service_id":{"S":"az_status"},"status":{"M":{"localdev-az1":{"S":"Normal"},"localdev-az2":{"S":"Normal"}}}}"#;

        run_cmd! {
            info "Initializing AZ status in service-discovery table ...";
            $[LOCAL_DDB_ENVS]
            aws dynamodb put-item
                --table-name $SERVICE_DISCOVERY_TABLE
                --item $az_status_item >/dev/null;
        }?;

        // Initialize BSS data volume group configuration in service-discovery table
        let bss_data_vg_config_json = generate_bss_data_vg_config(init_config.bss_count);
        let bss_data_vg_config_item = format!(
            r#"{{"service_id":{{"S":"bss-data-vg-config"}},"value":{{"S":"{}"}}}}"#,
            bss_data_vg_config_json
                .replace('"', r#"\""#)
                .replace('\n', "")
        );

        run_cmd! {
            info "Initializing BSS data volume group configuration in service-discovery table ...";
            $[LOCAL_DDB_ENVS]
            aws dynamodb put-item
                --table-name $SERVICE_DISCOVERY_TABLE
                --item $bss_data_vg_config_item >/dev/null;
        }?;

        // Initialize BSS metadata volume group configuration in service-discovery table
        let bss_metadata_vg_config_json = generate_bss_metadata_vg_config(init_config.bss_count);
        let bss_metadata_vg_config_item = format!(
            r#"{{"service_id":{{"S":"bss-metadata-vg-config"}},"value":{{"S":"{}"}}}}"#,
            bss_metadata_vg_config_json
                .replace('"', r#"\""#)
                .replace('\n', "")
        );

        run_cmd! {
            info "Initializing BSS metadata volume group configuration in service-discovery table ...";
            $[LOCAL_DDB_ENVS]
            aws dynamodb put-item
                --table-name $SERVICE_DISCOVERY_TABLE
                --item $bss_metadata_vg_config_item >/dev/null;
        }?;

        // Initialize BSS journal volume group configuration in service-discovery table
        let bss_journal_vg_config_json = generate_bss_journal_vg_config(init_config.bss_count);
        let bss_journal_vg_config_item = format!(
            r#"{{"service_id":{{"S":"bss-journal-vg-config"}},"value":{{"S":"{}"}}}}"#,
            bss_journal_vg_config_json
                .replace('"', r#"\""#)
                .replace('\n', "")
        );

        run_cmd! {
            info "Initializing BSS journal volume group configuration in service-discovery table ...";
            $[LOCAL_DDB_ENVS]
            aws dynamodb put-item
                --table-name $SERVICE_DISCOVERY_TABLE
                --item $bss_journal_vg_config_item >/dev/null;
        }?;

        // Initialize shared journal UUID in service-discovery table
        let journal_uuid = get_or_create_shared_journal_uuid()?;
        let journal_uuid_item = format!(
            r#"{{"service_id":{{"S":"journal-uuid"}},"value":{{"S":"{}"}}}}"#,
            journal_uuid
        );

        run_cmd! {
            info "Initializing shared journal UUID in service-discovery table ...";
            $[LOCAL_DDB_ENVS]
            aws dynamodb put-item
                --table-name $SERVICE_DISCOVERY_TABLE
                --item $journal_uuid_item >/dev/null;
        }?;

        Ok(())
    };
    let init_minio = |data_dir: &str| -> CmdResult { run_cmd!(mkdir -p $data_dir) };
    let init_etcd = || -> CmdResult {
        ensure_etcd_local()?;
        // Clean existing etcd data to prevent stale keys from previous runs
        run_cmd! {
            rm -rf data/etcd;
            mkdir -p data/etcd;
        }?;
        start_service(ServiceName::Etcd)?;

        // Initialize service-discovery keys using etcdctl
        let etcdctl = resolve_etcd_bin("etcdctl");

        // Always use observer_state for role management.
        // active/standby with idle standby: role agent reports standby health.
        // Fields match root_server's ObserverPersistentState and MachineState structs.
        let observer_state_json = r#"{"observer_state":"active_standby","nss_machine":{"machine_id":"nss-0","running_service":"nss","expected_role":"active","network_address":"127.0.0.1:8087"},"standby_machine":{"machine_id":"nss-1","running_service":"noop","expected_role":"standby","network_address":null},"version":1,"last_updated":0,"nss_node_map":{"nss-0":1,"nss-1":2},"next_nss_node_id":3}"#;

        let az_status_json = r#"{"status":{"localdev-az1":"Normal","localdev-az2":"Normal"}}"#;
        let bss_data_vg_config = generate_bss_data_vg_config(init_config.bss_count);
        let bss_metadata_vg_config = generate_bss_metadata_vg_config(init_config.bss_count);
        let bss_journal_vg_config = generate_bss_journal_vg_config(init_config.bss_count);

        run_cmd! {
            info "Initializing etcd service-discovery keys...";
            $etcdctl put /fractalbits-service-discovery/observer_state $observer_state_json >/dev/null;
            $etcdctl put /fractalbits-service-discovery/az_status $az_status_json >/dev/null;
            $etcdctl put /fractalbits-service-discovery/bss-data-vg-config $bss_data_vg_config >/dev/null;
            $etcdctl put /fractalbits-service-discovery/bss-metadata-vg-config $bss_metadata_vg_config >/dev/null;
            $etcdctl put /fractalbits-service-discovery/bss-journal-vg-config $bss_journal_vg_config >/dev/null;
        }?;

        // Initialize shared journal UUID
        let journal_uuid = get_or_create_shared_journal_uuid()?;
        run_cmd! {
            info "Initializing shared journal UUID in etcd ...";
            $etcdctl put /fractalbits-service-discovery/journal-uuid $journal_uuid >/dev/null;
        }?;

        stop_service(ServiceName::Etcd)?;
        Ok(())
    };
    let init_firestore = || -> CmdResult {
        firestore_utils::ensure_firestore_emulator()?;
        start_service(ServiceName::FirestoreEmulator)?;
        seed_firestore_emulator()?;
        Ok(())
    };
    let init_rss = || -> CmdResult {
        // Start backend service (ddb_local or etcd) based on config
        match init_config.rss_backend {
            RssBackend::Ddb => {
                if run_cmd!(systemctl --user is-active --quiet ddb_local.service).is_err() {
                    init_ddb_local()?;
                }
            }
            RssBackend::Etcd => {
                if run_cmd!(systemctl --user is-active --quiet etcd.service).is_err() {
                    init_etcd()?;
                }
            }
            RssBackend::Firestore => {
                if run_cmd!(systemctl --user is-active --quiet firestore_emulator.service).is_err()
                {
                    init_firestore()?;
                }
            }
        }

        // Start RSS service since admin now connects via RPC
        start_service(ServiceName::Rss)?;

        // Initialize api key for testing using RSS RPC
        let rss_admin_path = resolve_binary_path("rss_admin", build_mode);
        run_cmd! {
            $rss_admin_path --rss-addr=127.0.0.1:8086 api-key init-test;
        }?;

        // Stop services after initialization
        stop_service(ServiceName::Rss)?;
        match init_config.rss_backend {
            RssBackend::Ddb => stop_service(ServiceName::DdbLocal)?,
            RssBackend::Etcd => stop_service(ServiceName::Etcd)?,
            RssBackend::Firestore => stop_service(ServiceName::FirestoreEmulator)?,
        }
        Ok(())
    };
    let init_all_bss = |count: u32| -> CmdResult {
        create_bss_service_symlinks(count)?;
        for id in 0..count {
            create_bss_dirs(Path::new("data"), id)?;
        }

        for id in 0..count {
            format_bss_instance(id, build_mode)?;
        }
        Ok(())
    };
    let init_nss = || -> CmdResult {
        // nss now requires bss to store metadata blobs
        init_all_bss(init_config.bss_count)?;
        start_service(ServiceName::Bss)?;

        let format_log = "data/logs/format.log";
        let journal_uuid = get_or_create_shared_journal_uuid()?;
        let journal_type = init_config.journal_type;
        let is_ebs_journal = journal_type == JournalType::Ebs;
        create_dirs_for_nss_server(is_ebs_journal, &journal_uuid)?;
        let nss_binary = resolve_binary_path("nss_server", build_mode);
        let metadata_vg = generate_bss_metadata_vg_config(init_config.bss_count);
        let journal_vg = generate_bss_journal_vg_config(init_config.bss_count);

        // Compute shared_dir based on journal type (relative to working_dir which is ./data/nss-0)
        // For EBS: shared_dir is "../ebs/<journal_uuid>" to reach ./data/ebs/<uuid>
        // For NVMe: shared_dir is "local/journal/<journal_uuid>"
        let shared_dir = match journal_type {
            JournalType::Ebs => format!("../ebs/{}", journal_uuid),
            JournalType::Nvme => format!("local/journal/{}", journal_uuid),
        };

        match build_mode {
            BuildMode::Debug => run_cmd! {
                info "formatting nss_server with default configs";
                SHARED_DIR=$shared_dir JOURNAL_UUID=$journal_uuid METADATA_VG_CONFIG=$metadata_vg JOURNAL_VG_CONFIG=$journal_vg
                    $nss_binary format --init_test_tree
                    |& ts -m $TS_FMT >$format_log;
            }?,
            BuildMode::Release => run_cmd! {
                info "formatting nss_server for benchmarking";
                SHARED_DIR=$shared_dir JOURNAL_UUID=$journal_uuid METADATA_VG_CONFIG=$metadata_vg JOURNAL_VG_CONFIG=$journal_vg
                    $nss_binary format --init_test_tree
                    |& ts -m $TS_FMT >$format_log;
            }?,
        }

        stop_service(ServiceName::Bss)?;
        Ok(())
    };
    match service {
        ServiceName::ApiServer => {
            if init_config.with_https {
                generate_https_certificates()?;
            }
        }
        ServiceName::DdbLocal => init_ddb_local()?,
        ServiceName::Minio => init_minio("data/s3")?,
        ServiceName::MinioAz1 => init_minio("data/s3-localdev-az1")?,
        ServiceName::MinioAz2 => init_minio("data/s3-localdev-az2")?,
        ServiceName::Bss => {
            init_all_bss(init_config.bss_count)?;
        }
        ServiceName::Rss => init_rss()?,
        ServiceName::Nss => init_nss()?,
        ServiceName::NssRoleAgent => {}
        ServiceName::Etcd => init_etcd()?,
        ServiceName::FirestoreEmulator => firestore_utils::ensure_firestore_emulator()?,
        ServiceName::FsServer => {}
        ServiceName::All => {
            if init_config.with_https {
                generate_https_certificates()?;
            }
            init_rss()?;
            init_nss()?; // bss is initialized inside
            init_minio("data/s3")?;
            init_minio("data/s3-localdev-az1")?;
            init_minio("data/s3-localdev-az2")?;
        }
    }

    run_cmd! {
        info "systemctl daemon-reload";
        systemctl --user daemon-reload;
        systemctl --user reset-failed;
    }?;

    info!("All services are initialized successfully!");
    Ok(())
}

fn ensure_dynamodb_local() -> CmdResult {
    let dynamodb_file = "dynamodb_local_latest.tar.gz";
    let dynamodb_path = format!("third_party/{dynamodb_file}");
    let dynamodb_dir = "third_party/dynamodb_local";

    // Check if DynamoDB Local is already extracted and ready
    if Path::new(&format!("{dynamodb_dir}/DynamoDBLocal.jar")).exists() {
        return Ok(());
    }

    let download_url = "https://d1ni2b6xgvw0s0.cloudfront.net/v2.x/dynamodb_local_latest.tar.gz";

    // Check if already downloaded
    if !Path::new(&dynamodb_path).exists() {
        run_cmd! {
            info "Downloading DynamoDB Local...";
            curl -sL -o $dynamodb_path $download_url;
        }?;
    }

    run_cmd! {
        cd third_party;
        info "Extracting DynamoDB Local...";
        mkdir -p dynamodb_local;
        cd dynamodb_local;
        tar -xzf ../$dynamodb_file;
    }?;

    Ok(())
}

fn ensure_minio() -> CmdResult {
    let minio_dir = "third_party/minio";
    let minio_path = format!("{minio_dir}/minio");

    if run_cmd!(bash -c "command -v minio" &>/dev/null).is_ok() || Path::new(&minio_path).exists() {
        return Ok(());
    }

    let download_url = "https://dl.min.io/server/minio/release/linux-amd64/minio";
    run_cmd! {
        info "Downloading minio binary for testing since command not found";
        mkdir -p $minio_dir;
        curl -L -o $minio_path $download_url 2>&1;
        chmod +x $minio_path;
    }?;

    Ok(())
}

/// Seed the Firestore emulator with initial data (observer state, AZ status, VG configs, journal UUID).
/// The emulator must already be running on port 8282.
/// This is called both during init and during start (since the emulator is in-memory only).
fn seed_firestore_emulator() -> CmdResult {
    let bss_count = get_bss_count_from_config();

    let observer_state_json = r#"{"observer_state":"active_standby","nss_machine":{"machine_id":"nss-0","running_service":"nss","expected_role":"active","network_address":"127.0.0.1:8087"},"standby_machine":{"machine_id":"nss-1","running_service":"noop","expected_role":"standby","network_address":null},"version":1,"last_updated":0,"nss_node_map":{"nss-0":1,"nss-1":2},"next_nss_node_id":3}"#;
    let bss_data_vg_config = generate_bss_data_vg_config(bss_count);
    let bss_metadata_vg_config = generate_bss_metadata_vg_config(bss_count);
    let bss_journal_vg_config = generate_bss_journal_vg_config(bss_count);
    let journal_uuid = get_or_create_shared_journal_uuid()?;

    let firestore_put = |collection: &str, doc_id: &str, fields_json: &str| -> CmdResult {
        let url = format!(
            "http://localhost:8282/v1/projects/test-project/databases/fractalbits/documents/{collection}/{doc_id}"
        );
        run_cmd!(
            curl -sf -X PATCH $url
                -H "Content-Type: application/json"
                -d $fields_json >/dev/null
        )
    };

    let escaped_observer = observer_state_json.replace('"', r#"\""#);
    let observer_fields = format!(
        r#"{{"fields":{{"state":{{"stringValue":"{escaped_observer}"}},"version":{{"integerValue":"1"}}}}}}"#
    );
    info!("Seeding observer_state in Firestore...");
    firestore_put(
        "fractalbits-service-discovery",
        "observer_state",
        &observer_fields,
    )?;

    let az_status_json = r#"{"status":{"localdev-az1":"Normal","localdev-az2":"Normal"}}"#;
    let escaped_az = az_status_json.replace('"', r#"\""#);
    let az_fields = format!(
        r#"{{"fields":{{"value":{{"stringValue":"{escaped_az}"}},"version":{{"integerValue":"1"}}}}}}"#
    );
    info!("Seeding AZ status in Firestore...");
    firestore_put("fractalbits-service-discovery", "az_status", &az_fields)?;

    let escaped_data_vg = bss_data_vg_config.replace('"', r#"\""#).replace('\n', "");
    let data_vg_fields = format!(
        r#"{{"fields":{{"value":{{"stringValue":"{escaped_data_vg}"}},"version":{{"integerValue":"1"}}}}}}"#
    );
    info!("Seeding BSS data VG config in Firestore...");
    firestore_put(
        "fractalbits-service-discovery",
        "bss-data-vg-config",
        &data_vg_fields,
    )?;

    let escaped_meta_vg = bss_metadata_vg_config
        .replace('"', r#"\""#)
        .replace('\n', "");
    let meta_vg_fields = format!(
        r#"{{"fields":{{"value":{{"stringValue":"{escaped_meta_vg}"}},"version":{{"integerValue":"1"}}}}}}"#
    );
    info!("Seeding BSS metadata VG config in Firestore...");
    firestore_put(
        "fractalbits-service-discovery",
        "bss-metadata-vg-config",
        &meta_vg_fields,
    )?;

    let escaped_journal_vg = bss_journal_vg_config
        .replace('"', r#"\""#)
        .replace('\n', "");
    let journal_vg_fields = format!(
        r#"{{"fields":{{"value":{{"stringValue":"{escaped_journal_vg}"}},"version":{{"integerValue":"1"}}}}}}"#
    );
    info!("Seeding BSS journal VG config in Firestore...");
    firestore_put(
        "fractalbits-service-discovery",
        "bss-journal-vg-config",
        &journal_vg_fields,
    )?;

    let journal_fields = format!(
        r#"{{"fields":{{"value":{{"stringValue":"{journal_uuid}"}},"version":{{"integerValue":"1"}}}}}}"#
    );
    info!("Seeding journal UUID in Firestore...");
    firestore_put(
        "fractalbits-service-discovery",
        "journal-uuid",
        &journal_fields,
    )?;

    Ok(())
}

/// Re-initialize the test API key in RSS after a fresh Firestore emulator start.
fn reinit_firestore_api_key() -> CmdResult {
    let rss_admin_path = resolve_binary_path("rss_admin", BuildMode::Debug);
    info!("Re-initializing API key for Firestore emulator...");
    run_cmd! {
        $rss_admin_path --rss-addr=127.0.0.1:8086 api-key init-test;
    }
}

fn get_bss_count_from_config() -> u32 {
    // Count existing BSS service symlinks using glob pattern
    match glob::glob("data/etc/bss@[0-9]*.service") {
        Ok(paths) => paths.filter_map(Result::ok).count() as u32,
        Err(_) => 0,
    }
}

fn get_bss_service_names() -> Vec<String> {
    // Get all BSS service names (bss@0, bss@1, etc.)
    let bss_count = get_bss_count_from_config();
    (0..bss_count).map(|id| format!("bss@{}", id)).collect()
}

fn for_each_bss_service<F>(mut func: F) -> CmdResult
where
    F: FnMut(&str) -> CmdResult,
{
    // Apply a function to each BSS service instance
    for service_name in get_bss_service_names() {
        func(&service_name)?;
    }
    Ok(())
}

fn get_bss_service_status(service_name: &str) -> String {
    // Get status for a single BSS service instance
    match run_fun!(systemctl --user is-active $service_name.service 2>/dev/null) {
        Ok(output) => match output.trim() {
            "active" => "active".green().to_string(),
            status => status.yellow().to_string(),
        },
        Err(_) => {
            if run_cmd!(systemctl --user is-failed --quiet $service_name.service).is_ok() {
                "failed".red().to_string()
            } else {
                "inactive (dead)".bright_black().to_string()
            }
        }
    }
}

fn get_nss_service_names() -> Vec<&'static str> {
    vec!["nss@0", "nss@1"]
}

fn get_nss_service_status(service_name: &str) -> String {
    match run_fun!(systemctl --user is-active $service_name.service 2>/dev/null) {
        Ok(output) => match output.trim() {
            "active" => "active".green().to_string(),
            status => status.yellow().to_string(),
        },
        Err(_) => {
            if run_cmd!(systemctl --user is-failed --quiet $service_name.service).is_ok() {
                "failed".red().to_string()
            } else {
                "inactive (dead)".bright_black().to_string()
            }
        }
    }
}

/// Default nss_role_agent instance count at init time. Today this is 2 because
/// NSS runs as an active/standby pair; each role agent supervises one NSS node.
/// As the NSS topology grows, the count grows with it — one role agent per NSS.
const DEFAULT_NSS_ROLE_AGENT_COUNT: u32 = 2;

fn get_nss_role_agent_count_from_config() -> u32 {
    match glob::glob("data/etc/nss_role_agent@[0-9]*.service") {
        Ok(paths) => paths.filter_map(Result::ok).count() as u32,
        Err(_) => 0,
    }
}

fn get_nss_role_agent_service_names() -> Vec<String> {
    let count = get_nss_role_agent_count_from_config();
    (0..count)
        .map(|id| format!("nss_role_agent@{}", id))
        .collect()
}

fn get_nss_role_agent_service_status(service_name: &str) -> String {
    match run_fun!(systemctl --user is-active $service_name.service 2>/dev/null) {
        Ok(output) => match output.trim() {
            "active" => "active".green().to_string(),
            status => status.yellow().to_string(),
        },
        Err(_) => {
            if run_cmd!(systemctl --user is-failed --quiet $service_name.service).is_ok() {
                "failed".red().to_string()
            } else {
                "inactive (dead)".bright_black().to_string()
            }
        }
    }
}

fn create_nss_role_agent_service_symlinks(count: u32) -> CmdResult {
    // Remove any existing nss_role_agent instance symlinks
    if let Ok(paths) = glob::glob("data/etc/nss_role_agent@[0-9]*.service") {
        for path in paths.filter_map(Result::ok) {
            let _ = std::fs::remove_file(path);
        }
    }

    for id in 0..count {
        let service_file = format!("nss_role_agent@{}.service", id);
        let template_file = "nss_role_agent@.service";

        run_cmd! {
            info "Creating symlink for nss_role_agent instance $id";
            cd data/etc;
            ln -sf $template_file $service_file;
        }?;
    }
    Ok(())
}

pub fn start_nss_role_agent_instance(id: u32) -> CmdResult {
    let service_name = format!("nss_role_agent@{}", id);
    run_cmd!(systemctl --user start $service_name.service)?;

    // Today's 2-instance active/standby topology: instance 0 supervises the
    // active NSS (port 8087); instance 1 sits as an idle standby.
    if id == 0 {
        wait_for_port_ready(8087, 30)?;
    } else {
        // Idle standby (or future scaled instances): no managed service port.
        // Fall back to systemd's is-active check.
        use std::time::{Duration, Instant};
        let start = Instant::now();
        let timeout = Duration::from_secs(30);
        while start.elapsed() < timeout {
            if run_cmd!(systemctl --user is-active --quiet $service_name.service).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    info!("nss_role_agent instance {} started successfully", id);
    Ok(())
}

fn for_each_nss_role_agent_service<F>(mut func: F) -> CmdResult
where
    F: FnMut(&str) -> CmdResult,
{
    for service_name in get_nss_role_agent_service_names() {
        func(&service_name)?;
    }
    Ok(())
}

fn create_bss_service_symlinks(bss_count: u32) -> CmdResult {
    // Remove any existing BSS service symlinks using glob
    if let Ok(paths) = glob::glob("data/etc/bss@[0-9]*.service") {
        for path in paths.filter_map(Result::ok) {
            let _ = std::fs::remove_file(path);
        }
    }

    // Create symlinks for the specified BSS count
    for id in 0..bss_count {
        let service_file = format!("bss@{}.service", id);
        let template_file = "bss@.service";

        run_cmd! {
            info "Creating symlink for BSS instance $id";
            cd data/etc;
            ln -sf $template_file $service_file;
        }?;
    }
    Ok(())
}

pub fn start_bss_instance(id: u32) -> CmdResult {
    let service_name = format!("bss@{}", id);
    run_cmd!(systemctl --user start $service_name.service)?;

    // Wait for service to be ready
    let port = 8088 + id;
    wait_for_port_ready(port as u16, 30)?;

    info!("BSS instance {} (port {}) started successfully", id, port);
    Ok(())
}

pub fn wait_for_port_ready(port: u16, timeout_secs: u32) -> CmdResult {
    use std::time::{Duration, Instant};

    let start = Instant::now();
    let timeout = Duration::from_secs(timeout_secs as u64);

    info!(
        "Waiting for port {} to be ready (timeout: {}s)",
        port, timeout_secs
    );

    while start.elapsed() < timeout {
        if check_port_ready(port) {
            info!("Port {} is ready", port);
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    cmd_die!("Timeout waiting for port ${port} to be ready after ${timeout_secs}s")
}

pub fn stop_service(service: ServiceName) -> CmdResult {
    let services: Vec<ServiceName> = match service {
        ServiceName::All => all_services(
            get_data_blob_storage_setting(),
            get_rss_backend_setting(),
            false,
            false,
        ),
        single_service => vec![single_service],
    };

    for service in services {
        if service == ServiceName::Nss {
            let service_name = service.as_ref();
            cmd_die!(
                "$service_name is managed by nss_role_agent service - stop nss_role_agent instead"
            );
        } else if service == ServiceName::NssRoleAgent {
            // Handle nss_role_agent template instances
            for_each_nss_role_agent_service(|service_name| {
                if run_cmd!(systemctl --user is-active --quiet $service_name.service).is_err() {
                    return Ok(());
                }
                run_cmd! {
                    info "Stopping service: $service_name";
                    systemctl --user stop $service_name.service
                }?;

                if run_cmd!(systemctl --user is-active --quiet $service_name.service).is_ok() {
                    cmd_die!("Failed to stop $service_name: service is still running");
                }
                Ok(())
            })?;
        } else if service == ServiceName::Bss {
            // Handle BSS template instances using helper function
            for_each_bss_service(|service_name| {
                if run_cmd!(systemctl --user is-active --quiet $service_name.service).is_err() {
                    return Ok(());
                }
                run_cmd! {
                    info "Stopping service: $service_name";
                    systemctl --user stop $service_name.service
                }?;

                // make sure the process is really killed
                if run_cmd!(systemctl --user is-active --quiet $service_name.service).is_ok() {
                    cmd_die!("Failed to stop $service_name: service is still running");
                }
                Ok(())
            })?;
            // In case someone removes the whole data directory before issuing stop command
            run_cmd!(ignore killall bss_server &>/dev/null)?;
        } else {
            let service_name = service.as_ref();
            if run_cmd!(systemctl --user is-active --quiet $service_name.service).is_err() {
                continue;
            }

            run_cmd! {
                info "Stopping service: $service_name";
                systemctl --user stop $service_name.service
            }?;

            // make sure the process is really killed
            if run_cmd!(systemctl --user is-active --quiet $service_name.service).is_ok() {
                cmd_die!("Failed to stop $service_name: service is still running");
            }
        }
    }

    Ok(())
}

fn all_services(
    data_blob_storage: DataBlobStorage,
    rss_backend: RssBackend,
    with_managed_service: bool,
    sort: bool,
) -> Vec<ServiceName> {
    let rss_backend_service = match rss_backend {
        RssBackend::Ddb => ServiceName::DdbLocal,
        RssBackend::Etcd => ServiceName::Etcd,
        RssBackend::Firestore => ServiceName::FirestoreEmulator,
    };

    let mut services = match data_blob_storage {
        DataBlobStorage::S3HybridSingleAz => {
            let mut services = vec![
                ServiceName::ApiServer,
                ServiceName::NssRoleAgent,
                ServiceName::Bss,
                ServiceName::Rss,
                ServiceName::Minio,
            ];
            if with_managed_service {
                services.push(ServiceName::Nss);
            }
            services
        }
        DataBlobStorage::S3ExpressMultiAz => {
            let mut services = vec![
                ServiceName::ApiServer,
                ServiceName::NssRoleAgent,
                ServiceName::Bss,
                ServiceName::Rss,
                ServiceName::Minio,
                ServiceName::MinioAz1,
                ServiceName::MinioAz2,
            ];
            if with_managed_service {
                services.push(ServiceName::Nss);
            }
            services
        }
        DataBlobStorage::AllInBssSingleAz => {
            let mut services = vec![
                ServiceName::ApiServer,
                ServiceName::NssRoleAgent,
                ServiceName::Bss,
                ServiceName::Rss,
            ];
            if with_managed_service {
                services.push(ServiceName::Nss);
            }
            services
        }
    };
    services.push(rss_backend_service);
    if sort {
        services.sort_by_key(|s| s.as_ref().to_string());
    }
    services
}

fn get_rss_backend_setting() -> RssBackend {
    if run_cmd!(grep -q "RSS_BACKEND=etcd" data/etc/rss.service &>/dev/null).is_ok() {
        RssBackend::Etcd
    } else if run_cmd!(grep -q "RSS_BACKEND=firestore" data/etc/rss.service &>/dev/null).is_ok() {
        RssBackend::Firestore
    } else {
        RssBackend::Ddb
    }
}

fn get_data_blob_storage_setting() -> DataBlobStorage {
    if run_cmd!(grep -q multi_az data/etc/api_server.service &>/dev/null).is_ok() {
        DataBlobStorage::S3ExpressMultiAz
    } else if run_cmd!(grep -q s3_hybrid_single_az data/etc/api_server.service &>/dev/null).is_ok()
    {
        DataBlobStorage::S3HybridSingleAz
    } else {
        DataBlobStorage::AllInBssSingleAz
    }
}

pub fn show_service_status(service: ServiceName) -> CmdResult {
    match service {
        ServiceName::All => {
            println!("Service Status:");
            println!("─────────────────────────────────────");

            for svc in all_services(
                get_data_blob_storage_setting(),
                get_rss_backend_setting(),
                true,
                true,
            ) {
                if svc == ServiceName::Bss {
                    // Handle BSS template instances using helper functions
                    for bss_service_name in get_bss_service_names() {
                        let status = get_bss_service_status(&bss_service_name);
                        println!("{bss_service_name:<16}: {status}");
                    }
                } else if svc == ServiceName::Nss {
                    // Handle NSS template instances
                    for nss_service_name in get_nss_service_names() {
                        let status = get_nss_service_status(nss_service_name);
                        println!("{nss_service_name:<16}: {status}");
                    }
                } else if svc == ServiceName::NssRoleAgent {
                    // Handle nss_role_agent template instances
                    for service_name in get_nss_role_agent_service_names() {
                        let status = get_nss_role_agent_service_status(&service_name);
                        println!("{service_name:<16}: {status}");
                    }
                } else {
                    let service_name = svc.as_ref();
                    let status = if run_cmd!(systemctl --user list-unit-files --quiet $service_name.service | grep -q $service_name).is_ok() {
                        // Service exists, get its status
                        match run_fun!(systemctl --user is-active $service_name.service 2>/dev/null) {
                            Ok(output) => match output.trim() {
                                "active" => "active".green().to_string(),
                                status => status.yellow().to_string(),
                            },
                            Err(_) => {
                                // Command failed, try to get the actual status
                                if run_cmd!(systemctl --user is-failed --quiet $service_name.service).is_ok() {
                                    "failed".red().to_string()
                                } else {
                                    "inactive (dead)".bright_black().to_string()
                                }
                            }
                        }
                    } else {
                        "not installed".bright_black().to_string()
                    };

                    println!("{service_name:<16}: {status}");
                }
            }

            // fs_server is not part of all_services (standalone), show it separately
            let svc_name = ServiceName::FsServer.as_ref();
            let fs_status = match run_fun!(systemctl --user is-active $svc_name.service 2>/dev/null)
            {
                Ok(output) => match output.trim() {
                    "active" => "active".green().to_string(),
                    _ => "inactive (dead)".bright_black().to_string(),
                },
                Err(_) => "inactive (dead)".bright_black().to_string(),
            };
            println!("{svc_name:<16}: {fs_status}");
        }
        single_service => {
            if single_service == ServiceName::Bss {
                // Show all BSS template instances using helper functions
                let bss_services = get_bss_service_names();
                for (i, service_name) in bss_services.iter().enumerate() {
                    println!("=== {} ===", service_name);
                    run_cmd!(systemctl --user status $service_name.service --no-pager)?;
                    if i < bss_services.len() - 1 {
                        println!();
                    }
                }
            } else if single_service == ServiceName::Nss {
                // Show all NSS template instances
                let nss_services = get_nss_service_names();
                for (i, service_name) in nss_services.iter().enumerate() {
                    println!("=== {} ===", service_name);
                    run_cmd! { ignore systemctl --user status $service_name.service --no-pager; }?;
                    if i < nss_services.len() - 1 {
                        println!();
                    }
                }
            } else if single_service == ServiceName::NssRoleAgent {
                // Show all nss_role_agent template instances
                let services = get_nss_role_agent_service_names();
                for (i, service_name) in services.iter().enumerate() {
                    println!("=== {} ===", service_name);
                    run_cmd! { ignore systemctl --user status $service_name.service --no-pager; }?;
                    if i < services.len() - 1 {
                        println!();
                    }
                }
            } else {
                // Show detailed status for a single service
                let service_name = single_service.as_ref();
                run_cmd!(systemctl --user status $service_name.service --no-pager)?;
            }
        }
    }

    Ok(())
}

pub fn start_service(service: ServiceName) -> CmdResult {
    match service {
        ServiceName::All => start_all_services()?,
        ServiceName::Bss => {
            // Start all BSS template instances using helper function
            for_each_bss_service(|service_name| {
                let id: u32 = service_name.strip_prefix("bss@").unwrap().parse().unwrap();
                start_bss_instance(id)
            })?;

            info!("bss service started successfully");
        }
        ServiceName::NssRoleAgent => {
            // Start all nss_role_agent template instances in reverse order so the
            // standby-side agent is up before the active-side agent begins
            // managing NSS.
            let mut ids: Vec<u32> = (0..get_nss_role_agent_count_from_config()).collect();
            ids.reverse();
            for id in ids {
                start_nss_role_agent_instance(id)?;
            }

            info!("nss_role_agent service started successfully");
        }
        _ => {
            // Start the systemd service
            let service_name = service.as_ref();
            run_cmd!(systemctl --user start $service_name.service)?;

            // Wait for service to be ready
            wait_for_service_ready(service, 30)?;

            // Post-start actions
            match service {
                ServiceName::Minio => create_minio_bucket(9000, "fractalbits-bucket")?,
                ServiceName::MinioAz1 => {
                    create_minio_bucket(9001, "fractalbits-localdev-az1-data-bucket")?
                }
                ServiceName::MinioAz2 => {
                    create_minio_bucket(9002, "fractalbits-localdev-az2-data-bucket")?
                }
                ServiceName::ApiServer => register_local_api_server()?,
                _ => {}
            }

            info!("{service_name} service started successfully");
        }
    }
    Ok(())
}

fn create_minio_bucket(port: u16, bucket_name: &str) -> CmdResult {
    let minio_url = format!("http://localhost:{port}");
    let bucket = format!("s3://{bucket_name}");

    run_cmd! {
        info "Creating s3 bucket (\"$bucket_name\") ...";
        ignore AWS_DEFAULT_REGION=localdev AWS_ENDPOINT_URL_S3=$minio_url AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
            aws s3 mb $bucket --region localdev &>/dev/null;
    }?;

    let mut wait_new_bucket_secs = 0;
    const TIMEOUT_SECS: i32 = 5;
    loop {
        let bucket_ready = run_cmd! (
            AWS_DEFAULT_REGION=localdev AWS_ENDPOINT_URL_S3=$minio_url AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
            aws s3api head-bucket --bucket $bucket_name --region localdev &>/dev/null
        ).is_ok();

        if bucket_ready {
            break;
        }

        wait_new_bucket_secs += 1;
        if wait_new_bucket_secs >= TIMEOUT_SECS {
            cmd_die!("timeout waiting for newly created bucket ${bucket_name}");
        }

        info!("waiting for newly created bucket {bucket_name}: {wait_new_bucket_secs}s");
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    Ok(())
}

fn start_all_services() -> CmdResult {
    info!("Starting all services with systemd dependency management");

    // Start supporting services first based on backend configuration
    let rss_backend = get_rss_backend_setting();
    let data_blob_storage = get_data_blob_storage_setting();

    match rss_backend {
        RssBackend::Ddb => {
            info!("Starting supporting services (ddb_local)");
            start_service(ServiceName::DdbLocal)?;
        }
        RssBackend::Etcd => {
            info!("Starting supporting services (etcd)");
            start_service(ServiceName::Etcd)?;
        }
        RssBackend::Firestore => {
            info!("Starting supporting services (firestore emulator)");
            start_service(ServiceName::FirestoreEmulator)?;
            // Firestore emulator is in-memory only, so re-seed data on every start
            seed_firestore_emulator()?;
        }
    }

    // Start minio only for S3-based backends
    match data_blob_storage {
        DataBlobStorage::S3HybridSingleAz => {
            info!("Starting minio for S3HybridSingleAz");
            start_service(ServiceName::Minio)?;
        }
        DataBlobStorage::S3ExpressMultiAz => {
            info!("Starting minio instances for S3ExpressMultiAz");
            start_service(ServiceName::Minio)?;
            start_service(ServiceName::MinioAz1)?;
            start_service(ServiceName::MinioAz2)?;
        }
        DataBlobStorage::AllInBssSingleAz => {}
    }

    // For Firestore backend, re-init API key after RSS starts (emulator is in-memory)
    let reinit_api_key = matches!(rss_backend, RssBackend::Firestore);

    // Start all main services - systemd dependencies will handle ordering
    match data_blob_storage {
        DataBlobStorage::S3HybridSingleAz | DataBlobStorage::AllInBssSingleAz => {
            info!("Starting single_az services");
            start_service(ServiceName::Rss)?;
            if reinit_api_key {
                reinit_firestore_api_key()?;
            }
            // Start all BSS instances
            let bss_count = get_bss_count_from_config();
            for id in 0..bss_count {
                start_bss_instance(id)?;
            }
            // Start nss_role_agent instance 1 first (idle standby), then
            // instance 0 (active NSS).
            start_nss_role_agent_instance(1)?;
            start_nss_role_agent_instance(0)?;
            start_service(ServiceName::ApiServer)?;
        }
        DataBlobStorage::S3ExpressMultiAz => {
            info!("Starting multi_az services");
            // Start BSS instances (NSS requires BSS for metadata storage)
            let bss_count = get_bss_count_from_config();
            for id in 0..bss_count {
                start_bss_instance(id)?;
            }
            start_nss_role_agent_instance(1)?;
            start_service(ServiceName::Rss)?;
            if reinit_api_key {
                reinit_firestore_api_key()?;
            }
            start_nss_role_agent_instance(0)?;
            start_service(ServiceName::ApiServer)?;
        }
    }

    info!("All services are started successfully!");
    Ok(())
}

fn create_systemd_unit_files_for_init(
    service: ServiceName,
    build_mode: BuildMode,
    init_config: &InitConfig,
) -> CmdResult {
    match service {
        ServiceName::ApiServer => {
            create_systemd_unit_file(service, build_mode, init_config)?;
        }
        ServiceName::Bss
        | ServiceName::Nss
        | ServiceName::NssRoleAgent
        | ServiceName::Rss
        | ServiceName::DdbLocal
        | ServiceName::Minio
        | ServiceName::MinioAz1
        | ServiceName::MinioAz2
        | ServiceName::Etcd
        | ServiceName::FirestoreEmulator
        | ServiceName::FsServer => {
            create_systemd_unit_file(service, build_mode, init_config)?;
            if service == ServiceName::NssRoleAgent {
                create_nss_role_agent_service_symlinks(DEFAULT_NSS_ROLE_AGENT_COUNT)?;
            }
        }
        ServiceName::All => {
            let services = all_services(
                init_config.data_blob_storage,
                init_config.rss_backend,
                true,
                false,
            );
            for service in &services {
                create_systemd_unit_file(*service, build_mode, init_config)?;
            }
            create_nss_role_agent_service_symlinks(DEFAULT_NSS_ROLE_AGENT_COUNT)?;

            // Remove any stale mirrord unit left over from a previous init;
            // mirrord is no longer part of the service topology.
            run_cmd! {
                rm -f "data/etc/mirrord@.service";
            }?;
        }
    }
    Ok(())
}

fn create_systemd_unit_file(
    service: ServiceName,
    build_mode: BuildMode,
    init_config: &InitConfig,
) -> CmdResult {
    let pwd = run_fun!(pwd)?;
    let build = build_mode.as_ref();
    let service_name = service.as_ref();
    let mut env_settings = String::new();
    let mut managed_service = false;
    let env_rust_log = |build_mode: BuildMode| -> &'static str {
        match build_mode {
            BuildMode::Debug => {
                r##"
Environment="RUST_LOG=debug""##
            }
            BuildMode::Release => {
                r##"
Environment="RUST_LOG=warn""##
            }
        }
    };
    let minio_bin = match run_fun!(bash -c "command -v minio") {
        Ok(path) => path,
        Err(_) => run_fun!(realpath "third_party/minio/minio")?,
    };
    let exec_start = match service {
        ServiceName::Bss => {
            // Create template for BSS services using %i placeholder
            // Use bash arithmetic to calculate port dynamically: 8088 + instance_id
            env_settings += "\nEnvironment=\"WORKING_DIR=./bss%i\"";
            let bss_binary = resolve_binary_path("bss_server", build_mode);
            format!("/bin/bash -c 'SERVER_PORT=$((8088 + %i)) {bss_binary} serve'")
        }
        ServiceName::Nss => {
            managed_service = true;
            // Use template-based service with instance suffix (A or B)
            // WORKING_DIR, SHARED_DIR, JOURNAL_UUID, and HEALTH_PORT are set based on instance
            env_settings += "\nEnvironment=\"WORKING_DIR=./nss-%i\"";
            env_settings += &format!("\nEnvironmentFile=-{pwd}/data/etc/nss.env");
            let nss_binary = resolve_binary_path("nss_server", build_mode);
            // Read journal_uuid from shared file and compute SHARED_DIR based on journal type
            // For EBS: "../ebs/<uuid>" (relative to ./nss-%i to reach ./ebs/<uuid>)
            // For NVMe: "local/journal/<uuid>"
            let journal_type = init_config.journal_type;
            let shared_dir_prefix = match journal_type {
                JournalType::Ebs => "../ebs",
                JournalType::Nvme => "local/journal",
            };
            format!(
                r#"/bin/bash -c 'JOURNAL_UUID=$(cat ./etc/journal_uuid.txt); export JOURNAL_UUID; export SHARED_DIR="{shared_dir_prefix}/$JOURNAL_UUID"; if [ "%i" = "0" ]; then HEALTH_PORT=29999; else HEALTH_PORT=29998; fi; export HEALTH_PORT; if [ -n "$LOGS" ]; then {nss_binary} serve 2>&1 | ts "[%%Y-%%m-%%d %%H:%%M:%%S]" >> "$LOGS/nss-%i.log"; else exec {nss_binary} serve; fi'"#
            )
        }
        ServiceName::NssRoleAgent => {
            // Templated unit: %i is the instance index, INSTANCE_ID=nss-<index>
            env_settings += env_rust_log(build_mode);
            env_settings += "\nEnvironment=\"INSTANCE_ID=nss-%i\"";
            if init_config.nss_disable_restart_limit {
                env_settings += "\nEnvironment=\"NSS_DISABLE_RESTART_LIMIT=1\"";
            }
            if init_config.journal_type == JournalType::Ebs {
                env_settings += "\nEnvironment=\"APP_JOURNAL_TYPE=ebs\"";
            }
            resolve_binary_path("nss_role_agent", build_mode)
        }
        ServiceName::Rss => {
            env_settings = LOCAL_DDB_ENVS_SYSTEMD.to_string();
            env_settings += env_rust_log(build_mode);
            env_settings += &format!(
                "\nEnvironment=\"RSS_BACKEND={}\"",
                init_config.rss_backend.as_ref()
            );
            if init_config.rss_backend == RssBackend::Firestore {
                env_settings += "\nEnvironment=\"FIRESTORE_EMULATOR_HOST=localhost:8282\"";
                env_settings += "\nEnvironment=\"GCP_PROJECT_ID=test-project\"";
            }
            // Observer leader election configuration
            env_settings +=
                "\nEnvironment=\"LEADER_TABLE_NAME=fractalbits-leader-election-observer\"";
            env_settings += "\nEnvironment=\"INSTANCE_ID=rss-local\"";
            // Give services time to start before observer starts triggering failovers
            env_settings += "\nEnvironment=\"OBSERVER_INITIAL_GRACE_PERIOD_SECS=30\"";
            resolve_binary_path("root_server", build_mode)
        }
        ServiceName::ApiServer => {
            env_settings += env_rust_log(build_mode);
            env_settings += &format!(
                "\nEnvironment=\"APP_BLOB_STORAGE_BACKEND={}\"",
                init_config.data_blob_storage.as_ref()
            );
            if !init_config.with_https {
                env_settings += "\nEnvironment=\"HTTPS_DISABLED=1\"";
            }
            if init_config.for_gui {
                env_settings += r##"
Environment="GUI_WEB_ROOT=../ui/dist""##;
            }
            format!("{pwd}/target/{build}/api_server")
        }
        ServiceName::DdbLocal => {
            let java = run_fun!(bash -c "command -v java")?;
            let java_lib = format!("{pwd}/third_party/dynamodb_local/DynamoDBLocal_lib");
            format!(
                "{java} -Djava.library.path={java_lib} -jar {java_lib}/../DynamoDBLocal.jar -sharedDb -dbPath ./rss"
            )
        }
        ServiceName::Minio => {
            env_settings = r##"
Environment="MINIO_REGION=localdev""##
                .to_string();
            format!("{minio_bin} server --address :9000 s3/")
        }
        ServiceName::MinioAz1 => {
            env_settings = r##"
Environment="MINIO_REGION=localdev""##
                .to_string();
            format!("{minio_bin} server --address :9001 s3-localdev-az1/")
        }
        ServiceName::MinioAz2 => {
            env_settings = r##"
Environment="MINIO_REGION=localdev""##
                .to_string();
            format!("{minio_bin} server --address :9002 s3-localdev-az2/")
        }
        ServiceName::Etcd => {
            let etcd_bin = resolve_etcd_bin("etcd");
            format!(
                "{etcd_bin} --data-dir=./etcd --listen-client-urls=http://localhost:2379 --advertise-client-urls=http://localhost:2379"
            )
        }
        ServiceName::FirestoreEmulator => {
            let java = run_fun!(bash -c "command -v java")?;
            let jar = firestore_utils::resolve_firestore_jar();
            format!(
                "{java} -Duser.language=en -cp {jar} com.google.cloud.datastore.emulator.firestore.CloudFirestore start --host=localhost --port=8282 --database-mode=firestore-native"
            )
        }
        ServiceName::FsServer => {
            let fs = &init_config.fs_server;
            env_settings += &format!("\nEnvironment=\"FS_SERVER_BUCKET_NAME={}\"", fs.bucket_name);
            env_settings += &format!("\nEnvironment=\"FS_SERVER_MOUNT_POINT={}\"", fs.mount_point);
            if !fs.mode.is_empty() {
                env_settings += &format!("\nEnvironment=\"FS_SERVER_MODE={}\"", fs.mode);
            }
            env_settings += &format!("\nEnvironment=\"FS_SERVER_READ_WRITE={}\"", fs.read_write);
            if fs.disk_cache_enabled {
                env_settings += "\nEnvironment=\"FS_SERVER_DISK_CACHE_ENABLED=true\"";
                env_settings += &format!(
                    "\nEnvironment=\"FS_SERVER_DISK_CACHE_PATH={}\"",
                    fs.disk_cache_path
                );
                env_settings += &format!(
                    "\nEnvironment=\"FS_SERVER_DISK_CACHE_SIZE_GB={}\"",
                    fs.disk_cache_size_gb
                );
            }
            resolve_binary_path("fs_server", build_mode)
        }
        _ => unreachable!(),
    };
    let working_dir = format!("{}/data", run_fun!(realpath $pwd)?);

    // Add systemd dependencies based on service type
    let dependencies = match service {
        ServiceName::NssRoleAgent => {
            "After=rss.service\nWants=rss.service\n".to_string()
        }
        ServiceName::Rss => match init_config.rss_backend {
            RssBackend::Ddb => "After=ddb_local.service\nWants=ddb_local.service\n".to_string(),
            RssBackend::Etcd => "After=etcd.service\nWants=etcd.service\n".to_string(),
            RssBackend::Firestore => {
                "After=firestore_emulator.service\nWants=firestore_emulator.service\n".to_string()
            }
        },
        ServiceName::ApiServer => {
            match init_config.data_blob_storage {
                DataBlobStorage::AllInBssSingleAz => {
                    "After=rss.service nss_role_agent@0.service\nWants=rss.service nss_role_agent@0.service\n".to_string()
                }
                _ => {
                    "After=rss.service nss_role_agent@0.service minio.service\nWants=rss.service nss_role_agent@0.service minio.service\n".to_string()
                }
            }
        }
        ServiceName::FsServer => {
            "After=rss.service nss_role_agent@0.service\nWants=rss.service nss_role_agent@0.service\n".to_string()
        }
        _ => String::new(),
    };

    let (restart_settings, auto_restart) = if managed_service {
        ("", "")
    } else {
        (
            r##"# Limit to restarts within a 10-minute (600 second) interval
StartLimitIntervalSec=600
StartLimitBurst=100
        "##,
            "Restart=on-failure\nRestartSec=1",
        )
    };

    // Propagate LLVM_PROFILE_FILE to services for coverage instrumentation.
    // When running under `cargo llvm-cov`, this ensures service binaries write
    // profraw files to the location that `cargo llvm-cov report` discovers.
    // Replace `%Nm` (continuous mmap mode) with `%m` (merge-on-exit) since
    // mmap-based continuous profiling doesn't work reliably under systemd.
    // IMPORTANT: Escape `%` as `%%` for systemd unit files, since systemd
    // interprets `%` specifiers (e.g. `%p` = service prefix, `%m` = machine ID)
    // before passing them to the process. LLVM needs to see the raw `%p`/`%m`.
    if let Ok(profile_file) = std::env::var("LLVM_PROFILE_FILE") {
        let profile_file = regex::Regex::new(r"%\d+m").map_or(profile_file.clone(), |re| {
            re.replace(&profile_file, "%m").into_owned()
        });
        let profile_file = profile_file.replace('%', "%%");
        env_settings += &format!("\nEnvironment=\"LLVM_PROFILE_FILE={}\"", profile_file);
    }

    let systemd_unit_content = format!(
        r##"[Unit]
Description={service_name} Service
{dependencies}
{restart_settings}

[Service]
{auto_restart}
TimeoutStopSec=5
LimitNOFILE=1000000
LimitCORE=infinity
WorkingDirectory={working_dir}{env_settings}
ExecStart={exec_start}
SuccessExitStatus=143

[Install]
WantedBy=multi-user.target
"##
    );
    let service_file = match service {
        ServiceName::Bss => "bss@.service".to_string(),
        ServiceName::Nss => "nss@.service".to_string(),
        ServiceName::NssRoleAgent => "nss_role_agent@.service".to_string(),
        _ => format!("{service_name}.service"),
    };

    run_cmd! {
        mkdir -p $pwd/data/logs;
        mkdir -p $pwd/data/coredumps;
        mkdir -p data/etc;
        echo $systemd_unit_content > data/etc/$service_file;
        info "Linking ./data/etc/$service_file into ~/.config/systemd/user";
        systemctl --user link ./data/etc/$service_file --force --quiet;
    }?;
    Ok(())
}

fn create_dirs_for_nss_server(is_ebs_journal: bool, journal_uuid: &str) -> CmdResult {
    info!("Creating necessary directories for nss_server");
    run_cmd!(mkdir -p data/logs)?;
    // Create working directories for both the active (nss-0) and the standby
    // (nss-1) so the standby has a valid ./nss-1 path for when it gets promoted.
    create_nss_dirs(
        Path::new("data"),
        "nss-0",
        is_ebs_journal,
        Some(journal_uuid),
    )?;
    create_nss_dirs(
        Path::new("data"),
        "nss-1",
        is_ebs_journal,
        Some(journal_uuid),
    )
}

fn get_or_create_shared_journal_uuid() -> Result<String, std::io::Error> {
    let uuid_file = "data/etc/journal_uuid.txt";
    let uuid_path = Path::new(uuid_file);

    if uuid_path.exists() {
        let uuid = std::fs::read_to_string(uuid_path)?;
        let uuid = uuid.trim().to_string();
        info!("Using existing shared journal UUID: {}", uuid);
        return Ok(uuid);
    }

    // Generate a new UUID
    let uuid = Uuid::new_v4().to_string();

    // Ensure directory exists
    std::fs::create_dir_all("data/etc")?;

    // Save the UUID
    std::fs::write(uuid_path, &uuid)?;
    info!("Generated new shared journal UUID: {}", uuid);

    Ok(uuid)
}

pub fn wait_for_service_ready(service: ServiceName, timeout_secs: u32) -> CmdResult {
    use std::time::{Duration, Instant};

    let start = Instant::now();
    let timeout = Duration::from_secs(timeout_secs as u64);
    let service_name = service.as_ref();

    // Get port info for logging
    let (port_desc, ports): (&str, Vec<u16>) = match service {
        ServiceName::DdbLocal => ("port 8000", vec![8000]),
        ServiceName::Minio => ("port 9000", vec![9000]),
        ServiceName::MinioAz1 => ("port 9001", vec![9001]),
        ServiceName::MinioAz2 => ("port 9002", vec![9002]),
        ServiceName::Rss => ("port 8086", vec![8086]),
        ServiceName::Bss => {
            let bss_count = get_bss_count_from_config();
            let ports: Vec<u16> = (0..bss_count).map(|id| 8088 + id as u16).collect();
            ("BSS ports", ports)
        }
        ServiceName::Nss => ("port 8087", vec![8087]),
        ServiceName::ApiServer => ("port 8080", vec![8080]),
        ServiceName::NssRoleAgent => {
            unreachable!(
                "nss_role_agent is templated; start_service dispatches to start_nss_role_agent_instance"
            )
        }
        ServiceName::Etcd => ("port 2379", vec![2379]),
        ServiceName::FirestoreEmulator => ("port 8282", vec![8282]),
        ServiceName::FsServer => ("mountpoint check", vec![]),
        ServiceName::All => unreachable!("Should not check readiness for All"),
    };

    info!("Waiting for {service_name} to be ready ({port_desc}, timeout: {timeout_secs}s)");

    while start.elapsed() < timeout {
        // Check if systemd reports service as active
        if run_cmd!(systemctl --user is-active --quiet $service_name.service).is_ok() {
            // For network services, also check port availability
            let port_ready = if ports.is_empty() {
                true
            } else {
                ports.iter().all(|&port| check_port_ready(port))
            };

            if port_ready {
                info!("{service_name} is ready ({port_desc})");
                return Ok(());
            }
        }

        std::thread::sleep(Duration::from_millis(500));
    }

    cmd_die!("Timeout waiting for ${service_name} to be ready after ${timeout_secs}s")
}

pub fn check_port_ready(port: u16) -> bool {
    TcpStream::connect_timeout(
        &format!("127.0.0.1:{}", port).parse().unwrap(),
        Duration::from_secs(1),
    )
    .is_ok()
}

fn register_local_api_server() -> CmdResult {
    info!("Registering local api_server with service discovery");

    let backend = get_rss_backend_setting();
    match backend {
        RssBackend::Ddb => {
            // Create the JSON item for DynamoDB
            let item_json = r#"{
                "service_id": {"S": "api-server"},
                "instances": {
                    "M": {
                        "local-dev": {"S": "127.0.0.1:8080"}
                    }
                }
            }"#;

            // Try to update existing item first, if it doesn't exist, create it
            let key_json = "{\"service_id\": {\"S\": \"api-server\"}}";
            let attr_names = "{\"#instances\": \"instances\", \"#local\": \"local-dev\"}";
            let attr_values = "{\":ip\": {\"S\": \"127.0.0.1:8080\"}}";

            if run_cmd! {
                $[LOCAL_DDB_ENVS]
                aws dynamodb update-item
                    --table-name fractalbits-service-discovery
                    --key $key_json
                    --update-expression "SET #instances.#local = :ip"
                    --expression-attribute-names $attr_names
                    --expression-attribute-values $attr_values
                    --condition-expression "attribute_exists(service_id)" 2>/dev/null
            }
            .is_err()
            {
                // Item doesn't exist, create it
                run_cmd! {
                    $[LOCAL_DDB_ENVS]
                    aws dynamodb put-item
                        --table-name fractalbits-service-discovery
                        --item $item_json
                }?;
            }
        }
        RssBackend::Etcd => {
            // Use individual keys per instance: /fractalbits-service-discovery/api-server/<id> -> <ip>
            let etcdctl = resolve_etcd_bin("etcdctl");
            run_cmd!(
                $etcdctl put /fractalbits-service-discovery/api-server/local-dev "127.0.0.1" >/dev/null
            )?;
        }
        RssBackend::Firestore => {
            // Register api_server in Firestore service discovery
            let doc_json = r#"{"fields":{"ip":{"stringValue":"127.0.0.1"}}}"#;
            run_cmd!(
                curl -sf -X POST
                    "http://localhost:8282/v1/projects/test-project/databases/fractalbits/documents/fractalbits-service-discovery?documentId=api-server/local-dev"
                    -H "Content-Type: application/json"
                    -d $doc_json
                    >/dev/null
            ).ok(); // Ignore error if already exists
        }
    }

    info!("Local api_server registered in service discovery");
    Ok(())
}

pub fn format_bss_instance(id: u32, build_mode: BuildMode) -> CmdResult {
    let bss_binary = resolve_binary_path("bss_server", build_mode);
    let format_log = "data/logs/bss_format.log";
    let working_dir = format!("data/bss{id}");
    let port = 8088 + id;
    run_cmd! {
        info "formatting bss_server instance $id";
        WORKING_DIR=$working_dir
        SERVER_PORT=$port
        $bss_binary format --storage-alloc-mode sparse |& ts -m $TS_FMT >>$format_log;
    }?;
    Ok(())
}

pub(crate) fn resolve_binary_path(binary_name: &str, build_mode: BuildMode) -> String {
    let pwd = run_fun!(pwd).unwrap_or_else(|_| ".".to_string());
    let build = build_mode.as_ref();
    let arch = run_fun!(arch).unwrap_or_else(|_| "x86_64".to_string());

    // Check different locations based on binary type
    let candidates = match binary_name {
        "bss_server" | "nss_server" => {
            vec![
                format!("{pwd}/target/{build}/zig-out/bin/{binary_name}"),
                format!("{pwd}/{ZIG_DEBUG_OUT}/bin/{binary_name}"),
                format!("{pwd}/prebuilt/dev/{arch}/{binary_name}"),
            ]
        }
        _ => {
            vec![
                format!("{pwd}/target/{build}/{binary_name}"),
                format!("{pwd}/prebuilt/dev/{arch}/{binary_name}"),
            ]
        }
    };

    // Return first existing path, or default to target/build path
    for path in &candidates {
        if Path::new(path).exists() {
            return path.clone();
        }
    }

    // Default to first candidate (target/build path)
    candidates.into_iter().next().unwrap()
}

fn generate_https_certificates() -> CmdResult {
    info!("Generating HTTPS certificates for local development");

    // Check if certificates already exist
    if run_cmd!(test -f data/etc/cert.pem).is_ok() && run_cmd!(test -f data/etc/key.pem).is_ok() {
        info!("Certificates already exist, skipping generation");
        return Ok(());
    }

    run_cmd! {
        info "Running mkcert for trusted local certificates...";
        mkcert -install;
        mkdir -p data/etc;
        mkcert -key-file data/etc/key.pem -cert-file data/etc/cert.pem 127.0.0.1 localhost;
    }?;

    info!("HTTPS certificates generated successfully with mkcert:");
    info!("  Certificate: data/etc/cert.pem (trusted by system)");
    info!("  Private key: data/etc/key.pem (unencrypted)");
    Ok(())
}
