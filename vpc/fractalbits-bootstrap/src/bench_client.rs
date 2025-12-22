use super::common::*;
use crate::config::BootstrapConfig;
use crate::workflow::{WorkflowBarrier, WorkflowServiceType, stages};
use cmd_lib::*;

pub fn bootstrap(config: &BootstrapConfig) -> CmdResult {
    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Bench)?;
    barrier.complete_stage(stages::INSTANCES_READY, None)?;

    download_binaries(&["warp"])?;
    setup_serial_console_password()?;
    create_systemd_unit_file("bench_client", true)?;
    create_ddb_register_and_deregister_service("bench-client")?;

    barrier.complete_stage(stages::SERVICES_READY, None)?;

    Ok(())
}
