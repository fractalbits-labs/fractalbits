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
use std::io::Write;

#[derive(Parser)]
#[clap(
    name = "fractalbits-bootstrap",
    about = "Bootstrap for cloud ec2 instances"
)]
struct Opts {
    #[arg(long, help = "Format NSS instance (called via SSM from root_server)")]
    format_nss: bool,
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

    if opts.format_nss {
        let ebs_dev = discover_ebs_device()?;
        nss_server::format_nss(ebs_dev)?;
        run_cmd!(info "fractalbits-bootstrap --format-nss is done")?;
    } else {
        config_based_bootstrap()?;
    }

    Ok(())
}

fn discover_ebs_device() -> Result<String, std::io::Error> {
    use config::BootstrapConfig;

    info!("Discovering EBS device from bootstrap config");

    let config = BootstrapConfig::download_and_parse()?;
    let instance_id = get_instance_id()?;

    let instance_config = config.instances.get(&instance_id).ok_or_else(|| {
        std::io::Error::other(format!("Instance {} not found in config", instance_id))
    })?;

    let volume_id = instance_config
        .volume_id
        .as_ref()
        .ok_or_else(|| std::io::Error::other("volume_id not set in instance config"))?;

    let ebs_dev = get_volume_dev(volume_id);
    info!("Discovered EBS device: {ebs_dev} for volume {volume_id}");
    Ok(ebs_dev)
}

fn config_based_bootstrap() -> CmdResult {
    use config::BootstrapConfig;
    use discovery::{ServiceType, discover_service_type};

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
            nss_server::bootstrap(&config, volume_id, for_bench)?;
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
                .ok_or_else(|| std::io::Error::other("api_server_endpoint not set in config"))?;
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
