use clap::Parser;
use log::info;
use std::io::Error;

use crate::common::get_instance_id;
use crate::config::{BootstrapConfig, InstanceConfig};

#[derive(Debug, Clone)]
pub enum ServiceType {
    RootServer {
        is_leader: bool,
    },
    NssServer {
        journal_uuid: Option<String>,
    },
    ApiServer,
    BssServer,
    GuiServer,
    BenchServer {
        bench_client_num: usize,
    },
    BenchClient,
}

/// CLI arguments passed to fractalbits-bootstrap for cloud deployments.
/// Each instance gets its role via `--role` (and optional sub-args) in UserData/startup-script.
#[derive(Debug, Parser)]
pub struct CliArgs {
    /// Bucket URI (s3:// or gs://) — positional, always first
    pub bucket_uri: String,

    /// Service role: root_server, nss_server, api_server, bss_server, gui_server,
    /// bench_server, bench_client
    #[clap(long)]
    pub role: Option<String>,

    /// RSS sub-role: leader or follower (for root_server)
    #[clap(long)]
    pub rss_role: Option<String>,

    /// NSS sub-role: primary or standby (for nss_server)
    #[clap(long)]
    pub nss_role: Option<String>,

    /// NSS instance ID (for root_server leader — used to initialize observer state)
    #[clap(long)]
    pub nss_id: Option<String>,

    /// NSS private IP (for root_server leader — injected via UserData so RSS config
    /// has the correct nss_addr from the start, without needing a post-start update)
    #[clap(long)]
    pub nss_ip: Option<String>,

    /// API server NLB endpoint (for bench_server — injected via UserData)
    #[clap(long)]
    pub api_server_endpoint: Option<String>,
}

/// Discover service type from CLI args (cloud deployments with `--role` arg).
pub fn discover_from_args(args: &CliArgs) -> Result<ServiceType, Error> {
    let role = args.role.as_deref().unwrap_or("");
    info!("Discovering service type from CLI args: role={role:?}");

    match role {
        "root_server" => {
            let is_leader = args.rss_role.as_deref().unwrap_or("leader") == "leader";
            Ok(ServiceType::RootServer { is_leader })
        }
        "nss_server" => {
            // journal_uuid is no longer per-node; it comes from config.global.journal_uuid
            Ok(ServiceType::NssServer {
                journal_uuid: None, // read from config.global.journal_uuid at bootstrap time
            })
        }
        "api_server" => Ok(ServiceType::ApiServer),
        "bss_server" => Ok(ServiceType::BssServer),
        "gui_server" => Ok(ServiceType::GuiServer),
        "bench_server" => Ok(ServiceType::BenchServer {
            bench_client_num: 0,
        }),
        "bench_client" => Ok(ServiceType::BenchClient),
        _ => Err(Error::other(format!(
            "Unknown --role value: {role:?}. Expected one of: root_server, nss_server, api_server, bss_server, gui_server, bench_server, bench_client"
        ))),
    }
}

pub fn discover_service_type(config: &BootstrapConfig) -> Result<ServiceType, Error> {
    let instance_id = get_instance_id(config.global.deploy_target)?;
    info!("Discovering service type for instance: {instance_id}");

    if let Some(instance_config) = config.get_instance(&instance_id) {
        info!(
            "Found instance config in TOML: {:?}",
            instance_config.service_type
        );
        return parse_instance_config(&instance_config);
    }

    // Instance must be in TOML config for all deploy targets
    Err(Error::other(format!(
        "Instance '{instance_id}' not found in bootstrap config. All instances must be listed in TOML."
    )))
}

fn parse_instance_config(instance_config: &InstanceConfig) -> Result<ServiceType, Error> {
    match instance_config.service_type.as_str() {
        "root_server" => {
            let role = instance_config.role.as_deref().unwrap_or("leader");
            let is_leader = role == "leader";
            Ok(ServiceType::RootServer { is_leader })
        }
        "nss_server" => {
            let journal_uuid = instance_config.journal_uuid.clone();
            if journal_uuid.is_none() {
                return Err(Error::other("NSS server config missing journal_uuid"));
            }
            Ok(ServiceType::NssServer { journal_uuid })
        }
        "api_server" => Ok(ServiceType::ApiServer),
        "bss_server" => Ok(ServiceType::BssServer),
        "gui_server" => Ok(ServiceType::GuiServer),
        "bench_server" => {
            let bench_client_num = instance_config
                .bench_client_num
                .ok_or_else(|| Error::other("Bench server config missing bench_client_num"))?;
            Ok(ServiceType::BenchServer { bench_client_num })
        }
        "bench_client" => Ok(ServiceType::BenchClient),
        _ => Err(Error::other(format!(
            "Unknown service type: {}",
            instance_config.service_type
        ))),
    }
}
