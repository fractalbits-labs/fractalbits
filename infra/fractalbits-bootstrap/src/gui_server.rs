use crate::api_server;
use crate::config::BootstrapConfig;
use crate::stage_helpers::{CommonServicesReadyStage, InstancesReadyStage};
use crate::workflow::{WorkflowBarrier, WorkflowServiceType};
use crate::*;
use xtask_common::cloud_storage;

pub fn bootstrap(config: &BootstrapConfig) -> CmdResult {
    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Api)?;
    InstancesReadyStage::complete(&barrier)?;

    download_binaries(config, &["api_server"])?;
    let bootstrap_bucket = config.get_bootstrap_bucket();
    let ui_uri = format!("{bootstrap_bucket}/ui");
    cloud_storage::sync_down(&ui_uri, GUI_WEB_ROOT)?;

    api_server::create_config(config)?;
    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("gui_server", true)?;

    CommonServicesReadyStage::complete(&barrier)?;

    Ok(())
}
