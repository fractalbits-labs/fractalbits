use crate::api_server;
use crate::config::BootstrapConfig;
use crate::workflow::{WorkflowBarrier, WorkflowServiceType, stages};
use crate::*;

pub fn bootstrap(config: &BootstrapConfig) -> CmdResult {
    let nss_endpoint = &config.endpoints.nss_endpoint;

    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Api)?;
    barrier.complete_stage(stages::INSTANCES_READY, None)?;

    download_binaries(config, &["api_server"])?;
    let bootstrap_bucket = get_bootstrap_bucket();
    run_cmd!(aws s3 cp --no-progress $bootstrap_bucket/ui $GUI_WEB_ROOT --recursive)?;

    api_server::create_config(config, nss_endpoint)?;
    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("gui_server", true)?;

    barrier.complete_stage(stages::SERVICES_READY, None)?;

    Ok(())
}
