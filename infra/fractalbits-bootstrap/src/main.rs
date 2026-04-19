mod api_server;
mod aws;
mod bench_client;
mod bench_server;
mod bss_server;
mod common;
mod config;
mod discovery;
mod etcd;
mod etcd_server;
mod gcp;
mod gui_server;
mod nss_server;
mod root_server;
mod stage_helpers;
mod workflow;

use clap::Parser;
use cmd_lib::*;
use common::*;
use discovery::{CliArgs, ServiceType, discover_from_args, discover_service_type};
use std::io;

#[cmd_lib::main]
fn main() -> CmdResult {
    env_logger::Builder::new()
        .format(|buf, record| {
            use std::io::Write;
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

    let cli_args = CliArgs::parse();
    generic_bootstrap_with_args(cli_args)
}

pub fn generic_bootstrap_with_bucket(bucket_uri: &str) -> CmdResult {
    let cli_args = CliArgs::parse_from(["fractalbits-bootstrap", bucket_uri]);
    generic_bootstrap_with_args(cli_args)
}

fn generic_bootstrap_with_args(cli_args: CliArgs) -> CmdResult {
    let bucket_uri = &cli_args.bucket_uri;
    info!("Starting bootstrap (bucket: {bucket_uri})");

    let config = config::download_and_parse(bucket_uri)?;

    // Backup config to workflow directory for progress tracking
    if let Some(cluster_id) = &config.global.workflow_cluster_id {
        let _ = backup_config_to_workflow(&config, cluster_id);
    }

    let for_bench = config.global.for_bench;

    // If --role is provided, use it directly (cloud deployments).
    // Otherwise fall back to TOML-based discovery (on-prem / backward compat).
    let service_type = if cli_args.role.is_some() {
        info!("Using CLI-provided role: {:?}", cli_args.role);
        discover_from_args(&cli_args)?
    } else {
        info!("No --role arg, falling back to TOML-based discovery");
        discover_service_type(&config)?
    };

    common_setup(config.global.deploy_target)?;

    let service_name = match service_type {
        ServiceType::RootServer { is_leader } => {
            root_server::bootstrap(&config, is_leader, for_bench)?;
            "root_server"
        }
        ServiceType::NssServer { journal_uuid } => {
            nss_server::bootstrap(&config, journal_uuid.as_deref(), for_bench)?;
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
            let api_endpoint = cli_args
                .api_server_endpoint
                .as_deref()
                .or_else(|| {
                    config
                        .endpoints
                        .as_ref()
                        .and_then(|e| e.api_server_endpoint.as_deref())
                })
                .ok_or_else(|| io::Error::other("api_server_endpoint not set"))?;
            // bench_client_num is 0 for cloud (CLI) path; read from global config.
            // For on-prem (TOML) path, it comes from per-instance config.
            let actual_bench_client_num = if bench_client_num > 0 {
                bench_client_num
            } else {
                config.global.num_bench_clients.unwrap_or(1)
            };
            bench_server::bootstrap(&config, api_endpoint.to_string(), actual_bench_client_num)?;
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
