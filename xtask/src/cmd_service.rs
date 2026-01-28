use std::net::TcpStream;
use std::path::Path;
use std::time::Duration;

use crate::InitConfig;
use crate::etcd_utils::{ensure_etcd_local, resolve_etcd_bin};
use crate::*;
use colored::*;
use uuid::Uuid;
use xtask_common::{
    LOCAL_DDB_ENVS, LOCAL_DDB_ENVS_SYSTEMD, create_bss_dirs, create_nss_dirs,
    generate_bss_data_vg_config, generate_bss_metadata_vg_config,
};

pub fn init_service(
    service: ServiceName,
    build_mode: BuildMode,
    init_config: InitConfig,
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

        // Initialize observer_state in service-discovery table
        // Active/standby mode when JournalType::Nvme, solo mode otherwise
        // Fields match root_server's ObserverPersistentState and MachineState structs
        let observer_state_json = if init_config.journal_type == JournalType::Nvme {
            r#"{"observer_state":"active_standby","nss_machine":{"machine_id":"nss-A","running_service":"nss","expected_role":"active","network_address":"127.0.0.1:8087"},"mirrord_machine":{"machine_id":"nss-B","running_service":"mirrord","expected_role":"standby","network_address":"127.0.0.1:9999"},"version":1,"last_updated":0}"#
        } else {
            r#"{"observer_state":"solo","nss_machine":{"machine_id":"nss-A","running_service":"nss","expected_role":"solo","network_address":"127.0.0.1:8087"},"mirrord_machine":{"machine_id":"nss-B","running_service":"mirrord","expected_role":"","network_address":null},"version":1,"last_updated":0}"#
        };
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

        // Always use observer_state for role management
        // Active/standby mode when JournalType::Nvme, solo mode otherwise
        // Fields match root_server's ObserverPersistentState and MachineState structs
        let observer_state_json = if init_config.journal_type == JournalType::Nvme {
            r#"{"observer_state":"active_standby","nss_machine":{"machine_id":"nss-A","running_service":"nss","expected_role":"active","network_address":"127.0.0.1:8087"},"mirrord_machine":{"machine_id":"nss-B","running_service":"mirrord","expected_role":"standby","network_address":"127.0.0.1:9999"},"version":1,"last_updated":0}"#
        } else {
            r#"{"observer_state":"solo","nss_machine":{"machine_id":"nss-A","running_service":"nss","expected_role":"solo","network_address":"127.0.0.1:8087"},"mirrord_machine":{"machine_id":"nss-B","running_service":"mirrord","expected_role":"","network_address":null},"version":1,"last_updated":0}"#
        };

        let az_status_json = r#"{"status":{"localdev-az1":"Normal","localdev-az2":"Normal"}}"#;
        let bss_data_vg_config = generate_bss_data_vg_config(init_config.bss_count);
        let bss_metadata_vg_config = generate_bss_metadata_vg_config(init_config.bss_count);

        run_cmd! {
            info "Initializing etcd service-discovery keys...";
            $etcdctl put /fractalbits-service-discovery/observer_state $observer_state_json >/dev/null;
            $etcdctl put /fractalbits-service-discovery/az_status $az_status_json >/dev/null;
            $etcdctl put /fractalbits-service-discovery/bss-data-vg-config $bss_data_vg_config >/dev/null;
            $etcdctl put /fractalbits-service-discovery/bss-metadata-vg-config $bss_metadata_vg_config >/dev/null;
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
        }
        Ok(())
    };
    let init_all_bss = |count: u32| -> CmdResult {
        create_bss_service_symlinks(count)?;
        for id in 0..count {
            create_bss_dirs(Path::new("data"), id, count)?;
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

        // Compute shared_dir based on journal type (relative to working_dir which is ./data/nss-A)
        // For EBS: shared_dir is "../ebs/<journal_uuid>" to reach ./data/ebs/<uuid>
        // For NVMe: shared_dir is "local/journal/<journal_uuid>"
        let shared_dir = match journal_type {
            JournalType::Ebs => format!("../ebs/{}", journal_uuid),
            JournalType::Nvme => format!("local/journal/{}", journal_uuid),
        };

        match build_mode {
            BuildMode::Debug => run_cmd! {
                info "formatting nss_server with default configs";
                SHARED_DIR=$shared_dir JOURNAL_UUID=$journal_uuid $nss_binary format --init_test_tree
                    |& ts -m $TS_FMT >$format_log;
            }?,
            BuildMode::Release => run_cmd! {
                info "formatting nss_server for benchmarking";
                SHARED_DIR=$shared_dir JOURNAL_UUID=$journal_uuid $nss_binary format --init_test_tree
                    |& ts -m $TS_FMT >$format_log;
            }?,
        }

        stop_service(ServiceName::Bss)?;
        Ok(())
    };
    let init_mirrord = || -> CmdResult {
        let format_log = "data/logs/format_mirrord.log";
        let journal_type = init_config.journal_type;
        let journal_uuid = get_or_create_shared_journal_uuid()?;
        let is_ebs_journal = journal_type == JournalType::Ebs;
        create_dirs_for_mirrord_server(is_ebs_journal, &journal_uuid)?;
        let nss_binary = resolve_binary_path("nss_server", build_mode);
        info!(
            "Initializing mirrord with journal_type={:?}",
            journal_type.as_ref()
        );

        // Compute shared_dir based on journal type (relative to working_dir which is ./data/nss-B)
        // For EBS: shared_dir is "../ebs/<journal_uuid>" to reach ./data/ebs/<uuid>
        // For NVMe: shared_dir is "local/journal/<journal_uuid>"
        let shared_dir = match journal_type {
            JournalType::Ebs => format!("../ebs/{}", journal_uuid),
            JournalType::Nvme => format!("local/journal/{}", journal_uuid),
        };

        match build_mode {
            BuildMode::Debug => run_cmd! {
                info "formatting mirrord with default configs";
                WORKING_DIR="./data/nss-B" SHARED_DIR=$shared_dir JOURNAL_UUID=$journal_uuid $nss_binary format
                    |& ts -m $TS_FMT >$format_log;
            }?,
            BuildMode::Release => run_cmd! {
                info "formatting mirrord for benchmarking";
                WORKING_DIR="./data/nss-B" SHARED_DIR=$shared_dir JOURNAL_UUID=$journal_uuid $nss_binary format
                    |& ts -m $TS_FMT >$format_log;
            }?,
        }
        Ok(())
    };

    match service {
        ServiceName::ApiServer | ServiceName::GuiServer => {
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
        ServiceName::NssRoleAgentA | ServiceName::NssRoleAgentB => {}
        ServiceName::Mirrord => init_mirrord()?,
        ServiceName::Etcd => init_etcd()?,
        ServiceName::All => {
            if init_config.with_https {
                generate_https_certificates()?;
            }
            init_rss()?;
            init_nss()?; // bss is initialized inside
            init_mirrord()?;
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
    vec!["nss@A", "nss@B"]
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

fn get_mirrord_service_names() -> Vec<&'static str> {
    vec!["mirrord@A", "mirrord@B"]
}

fn get_mirrord_service_status(service_name: &str) -> String {
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
            get_journal_type_setting(),
            false,
            false,
        ),
        single_service => vec![single_service],
    };

    for service in services {
        if service == ServiceName::Nss || service == ServiceName::Mirrord {
            let service_name = service.as_ref();
            cmd_die!(
                "$service_name is managed by nss_role_agent service - stop nss_role_agent instead"
            );
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
    journal_type: JournalType,
    with_managed_service: bool,
    sort: bool,
) -> Vec<ServiceName> {
    let rss_backend_service = match rss_backend {
        RssBackend::Ddb => ServiceName::DdbLocal,
        RssBackend::Etcd => ServiceName::Etcd,
    };
    // When journal_type is Nvme, we always need NssRoleAgentB and Mirrord
    let with_mirrord =
        journal_type == JournalType::Nvme || data_blob_storage == DataBlobStorage::S3ExpressMultiAz;

    let mut services = match data_blob_storage {
        DataBlobStorage::S3HybridSingleAz => {
            let mut services = vec![
                ServiceName::ApiServer,
                ServiceName::NssRoleAgentA,
                ServiceName::Bss,
                ServiceName::Rss,
                rss_backend_service,
                ServiceName::Minio,
            ];
            if with_mirrord {
                services.push(ServiceName::NssRoleAgentB);
            }
            if with_managed_service {
                services.push(ServiceName::Nss);
                if with_mirrord {
                    services.push(ServiceName::Mirrord);
                }
            }
            services
        }
        DataBlobStorage::S3ExpressMultiAz => {
            let mut services = vec![
                ServiceName::ApiServer,
                ServiceName::NssRoleAgentA,
                ServiceName::NssRoleAgentB,
                ServiceName::Rss,
                rss_backend_service,
                ServiceName::Minio,
                ServiceName::MinioAz1,
                ServiceName::MinioAz2,
            ];
            if with_managed_service {
                services.push(ServiceName::Nss);
                services.push(ServiceName::Mirrord);
            }
            services
        }
        DataBlobStorage::AllInBssSingleAz => {
            let mut services = vec![
                ServiceName::ApiServer,
                ServiceName::NssRoleAgentA,
                ServiceName::Bss,
                ServiceName::Rss,
                rss_backend_service,
            ];
            if with_mirrord {
                services.push(ServiceName::NssRoleAgentB);
            }
            if with_managed_service {
                services.push(ServiceName::Nss);
                if with_mirrord {
                    services.push(ServiceName::Mirrord);
                }
            }
            services
        }
    };
    if sort {
        services.sort_by_key(|s| s.as_ref().to_string());
    }
    services
}

fn get_rss_backend_setting() -> RssBackend {
    if run_cmd!(grep -q "RSS_BACKEND=etcd" data/etc/rss.service &>/dev/null).is_ok() {
        RssBackend::Etcd
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

fn get_journal_type_setting() -> JournalType {
    // Check if nss_role_agent_b.service exists (indicates nvme/mirrord mode)
    if Path::new("data/etc/nss_role_agent_b.service").exists() {
        JournalType::Nvme
    } else {
        JournalType::Ebs
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
                get_journal_type_setting(),
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
                } else if svc == ServiceName::Mirrord {
                    // Handle Mirrord template instances
                    for mirrord_service_name in get_mirrord_service_names() {
                        let status = get_mirrord_service_status(mirrord_service_name);
                        println!("{mirrord_service_name:<16}: {status}");
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
                    let _ = run_cmd!(systemctl --user status $service_name.service --no-pager);
                    if i < nss_services.len() - 1 {
                        println!();
                    }
                }
            } else if single_service == ServiceName::Mirrord {
                // Show all Mirrord template instances
                let mirrord_services = get_mirrord_service_names();
                for (i, service_name) in mirrord_services.iter().enumerate() {
                    println!("=== {} ===", service_name);
                    let _ = run_cmd!(systemctl --user status $service_name.service --no-pager);
                    if i < mirrord_services.len() - 1 {
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
                ServiceName::ApiServer | ServiceName::GuiServer => register_local_api_server()?,
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
    let journal_type = get_journal_type_setting();

    // When journal_type is Nvme, we need mirrord running before nss active
    let with_mirrord =
        journal_type == JournalType::Nvme || data_blob_storage == DataBlobStorage::S3ExpressMultiAz;

    match rss_backend {
        RssBackend::Ddb => {
            info!("Starting supporting services (ddb_local)");
            start_service(ServiceName::DdbLocal)?;
        }
        RssBackend::Etcd => {
            info!("Starting supporting services (etcd)");
            start_service(ServiceName::Etcd)?;
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

    // Start all main services - systemd dependencies will handle ordering
    match data_blob_storage {
        DataBlobStorage::S3HybridSingleAz | DataBlobStorage::AllInBssSingleAz => {
            info!("Starting single_az services");
            start_service(ServiceName::Rss)?;
            // Start all BSS instances
            let bss_count = get_bss_count_from_config();
            for id in 0..bss_count {
                start_bss_instance(id)?;
            }
            // When using mirrord, start NssRoleAgentB first
            if with_mirrord {
                start_service(ServiceName::NssRoleAgentB)?;
            }
            start_service(ServiceName::NssRoleAgentA)?;
            start_service(ServiceName::ApiServer)?;
        }
        DataBlobStorage::S3ExpressMultiAz => {
            info!("Starting multi_az services (skipping BSS)");
            start_service(ServiceName::NssRoleAgentB)?;
            start_service(ServiceName::Rss)?;
            start_service(ServiceName::NssRoleAgentA)?;
            start_service(ServiceName::ApiServer)?;
        }
    }

    info!("All services are started successfully!");
    Ok(())
}

fn create_systemd_unit_files_for_init(
    service: ServiceName,
    build_mode: BuildMode,
    init_config: InitConfig,
) -> CmdResult {
    let api_server_service = if init_config.for_gui {
        ServiceName::GuiServer
    } else {
        ServiceName::ApiServer
    };
    match service {
        ServiceName::ApiServer | ServiceName::GuiServer => {
            create_systemd_unit_file(api_server_service, build_mode, init_config)?;
        }
        ServiceName::Bss
        | ServiceName::Nss
        | ServiceName::NssRoleAgentA
        | ServiceName::NssRoleAgentB
        | ServiceName::Mirrord
        | ServiceName::Rss
        | ServiceName::DdbLocal
        | ServiceName::Minio
        | ServiceName::MinioAz1
        | ServiceName::MinioAz2
        | ServiceName::Etcd => {
            create_systemd_unit_file(service, build_mode, init_config)?;
        }
        ServiceName::All => {
            let services = all_services(
                init_config.data_blob_storage,
                init_config.rss_backend,
                init_config.journal_type,
                true,
                false,
            );
            for service in &services {
                create_systemd_unit_file(*service, build_mode, init_config)?;
            }

            // Clean up service files that don't belong to the current configuration
            // This prevents stale service files from previous runs affecting startup
            let with_mirrord = init_config.journal_type == JournalType::Nvme
                || init_config.data_blob_storage == DataBlobStorage::S3ExpressMultiAz;
            if !with_mirrord {
                run_cmd! {
                    rm -f "data/etc/nss_role_agent_b.service";
                    rm -f "data/etc/mirrord@.service";
                }?;
            }
        }
    }
    Ok(())
}

fn create_systemd_unit_file(
    service: ServiceName,
    build_mode: BuildMode,
    init_config: InitConfig,
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
            env_settings += r##"
Environment="BSS_WORKING_DIR=./data/bss%i""##;
            let bss_binary = resolve_binary_path("bss_server", build_mode);
            format!("/bin/bash -c 'BSS_PORT=$((8088 + %i)) {bss_binary}'")
        }
        ServiceName::Nss => {
            managed_service = true;
            // Use template-based service with instance suffix (A or B)
            // WORKING_DIR, SHARED_DIR, JOURNAL_UUID, and HEALTH_PORT are set based on instance
            env_settings += "\nEnvironment=\"WORKING_DIR=./data/nss-%i\"";
            env_settings += &format!("\nEnvironmentFile=-{pwd}/data/etc/nss.env");
            let nss_binary = resolve_binary_path("nss_server", build_mode);
            // Read journal_uuid from shared file and compute SHARED_DIR based on journal type
            // For EBS: "../ebs/<uuid>" (relative to ./data/nss-%i to reach ./data/ebs/<uuid>)
            // For NVMe: "local/journal/<uuid>"
            let journal_type = init_config.journal_type;
            let shared_dir_prefix = match journal_type {
                JournalType::Ebs => "../ebs",
                JournalType::Nvme => "local/journal",
            };
            format!(
                r#"/bin/bash -c 'JOURNAL_UUID=$(cat ./data/etc/journal_uuid.txt); export JOURNAL_UUID; export SHARED_DIR="{shared_dir_prefix}/$JOURNAL_UUID"; if [ "%i" = "A" ]; then HEALTH_PORT=29999; else HEALTH_PORT=29998; fi; export HEALTH_PORT; if [ -n "$LOGS" ]; then {nss_binary} serve 2>&1 | ts "[%%Y-%%m-%%d %%H:%%M:%%S]" >> "$LOGS/nss-%i.log"; else exec {nss_binary} serve; fi'"#
            )
        }
        ServiceName::Mirrord => {
            managed_service = true;
            // Use template-based service with instance suffix (A or B)
            // WORKING_DIR, SHARED_DIR, JOURNAL_UUID, and HEALTH_PORT are set based on instance
            env_settings += "\nEnvironment=\"WORKING_DIR=./data/nss-%i\"";
            env_settings += &format!("\nEnvironmentFile=-{pwd}/data/etc/mirrord.env");
            let mirrord_binary = resolve_binary_path("mirrord", build_mode);
            // Read journal_uuid from shared file and compute SHARED_DIR based on journal type
            // For EBS: "../ebs/<uuid>" (relative to ./data/nss-%i to reach ./data/ebs/<uuid>)
            // For NVMe: "local/journal/<uuid>"
            let journal_type = init_config.journal_type;
            let shared_dir_prefix = match journal_type {
                JournalType::Ebs => "../ebs",
                JournalType::Nvme => "local/journal",
            };
            format!(
                r#"/bin/bash -c 'JOURNAL_UUID=$(cat ./data/etc/journal_uuid.txt); export JOURNAL_UUID; export SHARED_DIR="{shared_dir_prefix}/$JOURNAL_UUID"; if [ "%i" = "A" ]; then HEALTH_PORT=19999; else HEALTH_PORT=19998; fi; export HEALTH_PORT; if [ -n "$LOGS" ]; then {mirrord_binary} 2>&1 | ts "[%%Y-%%m-%%d %%H:%%M:%%S]" >> "$LOGS/mirrord-%i.log"; else exec {mirrord_binary}; fi'"#
            )
        }
        ServiceName::NssRoleAgentA => {
            env_settings += env_rust_log(build_mode);
            env_settings += "\nEnvironment=\"INSTANCE_ID=nss-A\"";
            env_settings += "\nEnvironment=\"RESTART_LOCKOUT_DIR=./data\"";
            if init_config.nss_disable_restart_limit {
                env_settings += "\nEnvironment=\"NSS_DISABLE_RESTART_LIMIT=1\"";
            }
            resolve_binary_path("nss_role_agent", build_mode)
        }
        ServiceName::NssRoleAgentB => {
            env_settings += env_rust_log(build_mode);
            env_settings += "\nEnvironment=\"INSTANCE_ID=nss-B\"";
            env_settings += "\nEnvironment=\"RESTART_LOCKOUT_DIR=./data\"";
            if init_config.nss_disable_restart_limit {
                env_settings += "\nEnvironment=\"NSS_DISABLE_RESTART_LIMIT=1\"";
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
            format!("{pwd}/target/{build}/api_server")
        }
        ServiceName::GuiServer => {
            env_settings += env_rust_log(build_mode);
            env_settings += &format!(
                "\nEnvironment=\"APP_BLOB_STORAGE_BACKEND={}\"",
                init_config.data_blob_storage.as_ref()
            );
            if !init_config.with_https {
                env_settings += "\nEnvironment=\"HTTPS_DISABLED=1\"";
            }
            env_settings += r##"
Environment="GUI_WEB_ROOT=ui/dist""##;
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
            format!("{minio_bin} server --address :9000 data/s3/")
        }
        ServiceName::MinioAz1 => {
            env_settings = r##"
Environment="MINIO_REGION=localdev""##
                .to_string();
            format!("{minio_bin} server --address :9001 data/s3-localdev-az1/")
        }
        ServiceName::MinioAz2 => {
            env_settings = r##"
Environment="MINIO_REGION=localdev""##
                .to_string();
            format!("{minio_bin} server --address :9002 data/s3-localdev-az2/")
        }
        ServiceName::Etcd => {
            let etcd_bin = resolve_etcd_bin("etcd");
            format!(
                "{etcd_bin} --data-dir=./data/etcd --listen-client-urls=http://localhost:2379 --advertise-client-urls=http://localhost:2379"
            )
        }
        _ => unreachable!(),
    };
    let working_dir = match service {
        ServiceName::DdbLocal => format!("{}/data", run_fun!(realpath $pwd)?),
        _ => run_fun!(realpath $pwd)?,
    };

    // Add systemd dependencies based on service type
    let dependencies = match service {
        ServiceName::NssRoleAgentA | ServiceName::NssRoleAgentB => {
            "After=rss.service\nWants=rss.service\n".to_string()
        }
        ServiceName::Rss => match init_config.rss_backend {
            RssBackend::Ddb => "After=ddb_local.service\nWants=ddb_local.service\n".to_string(),
            RssBackend::Etcd => "After=etcd.service\nWants=etcd.service\n".to_string(),
        },
        ServiceName::ApiServer | ServiceName::GuiServer => {
            match init_config.data_blob_storage {
                DataBlobStorage::AllInBssSingleAz => {
                    "After=rss.service nss_role_agent_a.service\nWants=rss.service nss_role_agent_a.service\n".to_string()
                }
                _ => {
                    "After=rss.service nss_role_agent_a.service minio.service\nWants=rss.service nss_role_agent_a.service minio.service\n".to_string()
                }
            }
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
        ServiceName::GuiServer => "api_server.service".to_string(),
        ServiceName::Bss => "bss@.service".to_string(),
        ServiceName::Nss => "nss@.service".to_string(),
        ServiceName::Mirrord => "mirrord@.service".to_string(),
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
    create_nss_dirs(
        Path::new("data"),
        "nss-A",
        is_ebs_journal,
        Some(journal_uuid),
    )
}

fn create_dirs_for_mirrord_server(is_ebs_journal: bool, journal_uuid: &str) -> CmdResult {
    info!("Creating necessary directories for mirrord");
    create_nss_dirs(
        Path::new("data"),
        "nss-B",
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
        ServiceName::Mirrord => ("port 9999", vec![9999]),
        ServiceName::ApiServer | ServiceName::GuiServer => ("port 8080", vec![8080]),
        ServiceName::NssRoleAgentA => ("managed nss port 8087", vec![8087]),
        ServiceName::NssRoleAgentB => ("managed mirrord port 9999", vec![9999]),
        ServiceName::Etcd => ("port 2379", vec![2379]),
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
    }

    info!("Local api_server registered in service discovery");
    Ok(())
}

fn resolve_binary_path(binary_name: &str, build_mode: BuildMode) -> String {
    let pwd = run_fun!(pwd).unwrap_or_else(|_| ".".to_string());
    let build = build_mode.as_ref();
    let arch = run_fun!(arch).unwrap_or_else(|_| "x86_64".to_string());

    // Check different locations based on binary type
    let candidates = match binary_name {
        "bss_server" | "nss_server" | "mirrord" => {
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
