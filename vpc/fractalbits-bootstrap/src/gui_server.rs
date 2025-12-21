use crate::api_server;
use crate::config::BootstrapConfig;
use crate::*;

pub fn bootstrap(config: &BootstrapConfig) -> CmdResult {
    let data_blob_storage = &config.global.data_blob_storage;
    let data_blob_bucket = config.aws.as_ref().map(|aws| aws.data_blob_bucket.as_str());
    let nss_endpoint = &config.endpoints.nss_endpoint;
    let remote_az = config.aws.as_ref().and_then(|aws| aws.remote_az.as_deref());
    let rss_ha_enabled = config.global.rss_ha_enabled;

    download_binaries(&["api_server"])?;
    let builds_bucket = get_bootstrap_bucket()?;
    run_cmd!(aws s3 cp --no-progress $builds_bucket/ui $GUI_WEB_ROOT --recursive)?;

    api_server::create_config(
        data_blob_storage,
        data_blob_bucket,
        nss_endpoint,
        remote_az,
        rss_ha_enabled,
    )?;
    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("gui_server", true)?;

    Ok(())
}
