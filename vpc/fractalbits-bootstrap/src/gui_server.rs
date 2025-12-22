use crate::api_server;
use crate::config::BootstrapConfig;
use crate::workflow::{WorkflowBarrier, WorkflowServiceType, stages};
use crate::*;

pub fn bootstrap(config: &BootstrapConfig) -> CmdResult {
    let data_blob_storage = &config.global.data_blob_storage;
    let data_blob_bucket = config.aws.as_ref().map(|aws| aws.data_blob_bucket.as_str());
    let nss_endpoint = &config.endpoints.nss_endpoint;
    let remote_az = config.aws.as_ref().and_then(|aws| aws.remote_az.as_deref());
    let rss_ha_enabled = config.global.rss_ha_enabled;

    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Api)?;
    barrier.complete_stage(stages::INSTANCES_READY, None)?;

    download_binaries(&["api_server"])?;
    let bootstrap_bucket = get_bootstrap_bucket();
    run_cmd!(aws s3 cp --no-progress $bootstrap_bucket/ui $GUI_WEB_ROOT --recursive)?;

    api_server::create_config(
        data_blob_storage,
        data_blob_bucket,
        nss_endpoint,
        remote_az,
        rss_ha_enabled,
    )?;
    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("gui_server", true)?;

    barrier.complete_stage(stages::SERVICES_READY, None)?;

    Ok(())
}
