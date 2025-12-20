mod api_server;
mod bench_client;
mod bench_server;
mod bss_server;
mod common;
mod config;
mod discovery;
mod etcd_cluster;
mod etcd_server;
mod gui_server;
mod nss_server;
mod root_server;

use clap::Parser;
use cmd_lib::*;
use common::*;
use config::BootstrapConfig;
use discovery::{ServiceType, discover_service_type};
use std::io::{self, Write};

#[derive(Parser)]
#[clap(
    name = "fractalbits-bootstrap",
    about = "Bootstrap for fractalbits cluster"
)]
struct Opts {
    #[arg(
        long,
        help = "Format EBS journal for NSS instance (called via SSM from root_server)"
    )]
    format_ebs_journal: bool,
}

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

    let opts = Opts::parse();

    if opts.format_ebs_journal {
        nss_server::ebs_journal::format()?;
        info!("fractalbits-bootstrap --format-nss is done");
    } else {
        generic_bootstrap()?;
    }

    Ok(())
}

fn generic_bootstrap() -> CmdResult {
    info!("Starting config-based bootstrap mode");

    let config = BootstrapConfig::download_and_parse()?;
    let for_bench = config.global.for_bench;
    let service_type = discover_service_type(&config)?;

    common_setup()?;

    let service_name = match &service_type {
        ServiceType::RootServer {
            is_leader,
            follower_id,
        } => {
            root_server::bootstrap(&config, *is_leader, follower_id.clone(), for_bench)?;
            "root_server"
        }
        ServiceType::NssServer { volume_id } => {
            nss_server::bootstrap(&config, volume_id.as_deref(), for_bench)?;
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
            bench_server::bootstrap(api_endpoint.clone(), *bench_client_num)?;
            "bench_server"
        }
        ServiceType::BenchClient => {
            bench_client::bootstrap()?;
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
