use crate::api_server;
use crate::config::BootstrapConfig;
use crate::*;

pub fn bootstrap(config: &BootstrapConfig) -> CmdResult {
    let bucket = if config.is_multi_az() {
        None
    } else {
        Some(config.aws.bucket.as_str())
    };
    let nss_endpoint = &config.endpoints.nss_endpoint;
    let remote_az = config.aws.remote_az.as_deref();
    let rss_ha_enabled = config.global.rss_ha_enabled;

    download_binaries(&["api_server"])?;
    let builds_bucket = get_builds_bucket()?;
    run_cmd!(aws s3 cp --no-progress $builds_bucket/ui $GUI_WEB_ROOT --recursive)?;

    api_server::create_config(bucket, nss_endpoint, remote_az, rss_ha_enabled)?;
    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("gui_server", true)?;

    Ok(())
}
