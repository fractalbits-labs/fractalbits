mod api_server;
mod bench_client;
mod bench_server;
mod bss_server;
mod common;
mod config;
mod discovery;
mod etcd_server;
mod gui_server;
mod nss_server;
mod root_server;
mod workflow;

use cmd_lib::*;
use common::*;
use discovery::{ServiceType, discover_service_type};
use std::io::{self, Write};

#[cmd_lib::main]
fn main() -> CmdResult {
    env_logger::Builder::new()
        .format(|buf, record| {
            let timestamp = chrono::Local::now().format("%b %d %H:%M:%S").to_string();
            let process_name = std::env::current_exe()
                .ok()
                .and_then(|path| {
                    path.file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                })
                .unwrap_or_else(|| "fractalbits-bootstrap".to_string());
            let pid = std::process::id();
            writeln!(
                buf,
                "{} {}[{}]: {} {}",
                timestamp,
                process_name,
                pid,
                record.level(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info)
        .init();

    let main_build_info = option_env!("MAIN_BUILD_INFO").unwrap_or("unknown");
    let build_timestamp = option_env!("BUILD_TIMESTAMP").unwrap_or("unknown");
    let build_info = format!("{}, build time: {}", main_build_info, build_timestamp);
    eprintln!("build info: {}", build_info);

    generic_bootstrap()
}

fn generic_bootstrap() -> CmdResult {
    let args: Vec<String> = std::env::args().collect();
    let bucket_name = args
        .get(1)
        .ok_or_else(|| io::Error::other("Usage: fractalbits-bootstrap <bucket_name>"))?;

    info!("Starting config-based bootstrap mode (bucket: {bucket_name})");

    let config = config::download_and_parse(bucket_name)?;

    // Backup config to workflow directory for progress tracking
    if let Some(cluster_id) = &config.global.workflow_cluster_id {
        let _ = backup_config_to_workflow(&config, cluster_id);
    }

    let for_bench = config.global.for_bench;
    let service_type = discover_service_type(&config)?;

    common_setup(config.global.deploy_target)?;

    let service_name = match &service_type {
        ServiceType::RootServer { is_leader } => {
            root_server::bootstrap(&config, *is_leader, for_bench)?;
            "root_server"
        }
        ServiceType::NssServer {
            volume_id,
            journal_uuid,
        } => {
            nss_server::bootstrap(
                &config,
                volume_id.as_deref(),
                journal_uuid.as_deref(),
                for_bench,
            )?;
            "nss_server"
        }
        ServiceType::ApiServer => {
            api_server::bootstrap(&config, for_bench)?;
            "api_server"
        }
        ServiceType::BssServer => {
            bss_server::bootstrap(&config, for_bench)?;
            "bss_server"
        }
        ServiceType::GuiServer => {
            gui_server::bootstrap(&config)?;
            "gui_server"
        }
        ServiceType::BenchServer { bench_client_num } => {
            let api_endpoint = config
                .endpoints
                .api_server_endpoint
                .as_ref()
                .ok_or_else(|| io::Error::other("api_server_endpoint not set in config"))?;
            bench_server::bootstrap(&config, api_endpoint.clone(), *bench_client_num)?;
            "bench_server"
        }
        ServiceType::BenchClient => {
            bench_client::bootstrap(&config)?;
            "bench_client"
        }
    };

    run_cmd! {
        touch $BOOTSTRAP_DONE_FILE;
        sync;
        info "fractalbits-bootstrap $service_name is done";
    }?;
    Ok(())
}
