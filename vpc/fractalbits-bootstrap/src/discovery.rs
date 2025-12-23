use log::info;
use std::io::Error;

use crate::common::{get_current_aws_region, get_ec2_tag, get_instance_id_from_config};
use crate::config::{BootstrapConfig, InstanceConfig, JournalType, VpcTarget};

pub const SERVICE_TYPE_TAG: &str = "fractalbits:ServiceType";

#[derive(Debug, Clone)]
pub enum ServiceType {
    RootServer { is_leader: bool },
    NssServer { volume_id: Option<String> },
    ApiServer,
    BssServer,
    GuiServer,
    BenchServer { bench_client_num: usize },
    BenchClient,
}

pub fn discover_service_type(config: &BootstrapConfig) -> Result<ServiceType, Error> {
    let instance_id = get_instance_id_from_config(config)?;
    info!("Discovering service type for instance: {instance_id}");

    if let Some(instance_config) = config.instances.get(&instance_id) {
        info!(
            "Found instance config in TOML: {:?}",
            instance_config.service_type
        );
        return parse_instance_config(config, instance_config);
    }

    // For on-prem, instance MUST be in TOML config (no EC2 tag fallback)
    if config.global.target == VpcTarget::OnPrem {
        return Err(Error::other(format!(
            "Instance '{instance_id}' not found in config. On-prem requires all instances in TOML."
        )));
    }

    // AWS fallback: query EC2 tag
    info!("Instance not in TOML config, querying EC2 tag: {SERVICE_TYPE_TAG}");
    let region = get_current_aws_region()?;
    let service_type_tag = get_ec2_tag(&instance_id, &region, SERVICE_TYPE_TAG)?;
    info!("Found service type from EC2 tag: {service_type_tag}");

    match service_type_tag.as_str() {
        "api_server" => Ok(ServiceType::ApiServer),
        "bss_server" => Ok(ServiceType::BssServer),
        "bench_client" => Ok(ServiceType::BenchClient),
        _ => Err(Error::other(format!(
            "Unknown service type tag: {service_type_tag}"
        ))),
    }
}

fn parse_instance_config(
    config: &BootstrapConfig,
    instance_config: &InstanceConfig,
) -> Result<ServiceType, Error> {
    match instance_config.service_type.as_str() {
        "root_server" => {
            let role = instance_config.role.as_deref().unwrap_or("leader");
            let is_leader = role == "leader";
            Ok(ServiceType::RootServer { is_leader })
        }
        "nss_server" => {
            let volume_id = instance_config.volume_id.clone();
            // volume_id is required for ebs journal type
            if config.global.journal_type == JournalType::Ebs && volume_id.is_none() {
                return Err(Error::other(
                    "NSS server config missing volume_id for ebs journal type",
                ));
            }
            Ok(ServiceType::NssServer { volume_id })
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
