use super::common::*;
use crate::config::BootstrapConfig;
use crate::workflow::{WorkflowBarrier, WorkflowServiceType, stages, timeouts};
use cmd_lib::*;

pub fn bootstrap(config: &BootstrapConfig) -> CmdResult {
    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Bench)?;
    barrier.complete_stage(stages::INSTANCES_READY, None)?;

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
        barrier.wait_for_global(stages::ETCD_READY, timeouts::ETCD_READY)?;
        info!("etcd cluster is ready");
    }

    register_service(config, "bench-client")?;

    barrier.complete_stage(stages::SERVICES_READY, None)?;

    Ok(())
}
