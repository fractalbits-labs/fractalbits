use super::common::*;
use crate::config::BootstrapConfig;
use crate::stage_helpers::{BenchServicesReadyStage, InstancesReadyStage};
use crate::workflow::{WorkflowBarrier, WorkflowServiceType};
use cmd_lib::*;

pub fn bootstrap(config: &BootstrapConfig) -> CmdResult {
    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Bench)?;
    InstancesReadyStage::complete(&barrier)?;

    let mut binaries = vec!["warp"];
    if config.is_etcd_backend() {
        binaries.push("etcdctl");
    }
    download_binaries(config, &binaries)?;
    setup_serial_console_password()?;
    create_systemd_unit_file("bench_client", true)?;

    // When using etcd backend, wait for etcd cluster to be ready before registering
    if config.is_etcd_backend() {
        info!("Waiting for etcd cluster to be ready...");
        BenchServicesReadyStage::wait_for_etcd_ready(&barrier)?;
        info!("etcd cluster is ready");
    }

    register_service(config, "bench-client")?;

    BenchServicesReadyStage::complete(&barrier)?;

    Ok(())
}
