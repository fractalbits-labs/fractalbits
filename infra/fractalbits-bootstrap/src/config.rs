use cmd_lib::*;
use std::io::Error;
use std::time::{Duration, Instant};

use crate::common::{BOOTSTRAP_CLUSTER_CONFIG, ETC_PATH, ensure_aws_cli, ensure_ec2_metadata};

// Re-export types from xtask_common
pub use xtask_common::{
    BootstrapClusterConfig, ClusterEtcdConfig, ClusterNodeConfig, DataBlobStorage, DeployTarget,
    cloud_storage,
};

// Type aliases for backwards compatibility
pub type BootstrapConfig = BootstrapClusterConfig;
pub type EtcdConfig = ClusterEtcdConfig;
pub type InstanceConfig = ClusterNodeConfig;

// CDK VPC deploy can take 10-15 minutes, and instances start booting during CDK deploy.
// With pre-deploy TOML upload, this should be much shorter — but keep a generous timeout
// as a safety net for slow S3 propagation or network issues.
const CONFIG_RETRY_TIMEOUT_SECS: u64 = 600;

/// Download and parse bootstrap config from cloud storage.
/// `bucket_uri` is a full URI like `s3://bucket-name` or `gs://bucket-name`.
pub fn download_and_parse(bucket_uri: &str) -> Result<BootstrapClusterConfig, Error> {
    let cloud_path = format!("{bucket_uri}/{BOOTSTRAP_CLUSTER_CONFIG}");
    let local_path = format!("{ETC_PATH}{BOOTSTRAP_CLUSTER_CONFIG}");

    if bucket_uri.starts_with("s3://") {
        ensure_aws_cli()?;
    }

    let start_time = Instant::now();
    let timeout = Duration::from_secs(CONFIG_RETRY_TIMEOUT_SECS);
    loop {
        // Retry download if the file doesn't exist yet (race condition with deploy)
        match cloud_storage::download_file(&cloud_path, &local_path) {
            Ok(_) => {}
            Err(e) => {
                if start_time.elapsed() > timeout {
                    return Err(Error::other(format!(
                        "Failed to download bootstrap config after {CONFIG_RETRY_TIMEOUT_SECS}s: {e}"
                    )));
                }
                info!("Download failed ({e}), waiting 5s and retrying...");
                std::thread::sleep(Duration::from_secs(5));
                continue;
            }
        }

        let content = std::fs::read_to_string(&local_path)?;
        let config: BootstrapClusterConfig =
            toml::from_str(&content).map_err(|e| Error::other(format!("TOML parse error: {e}")))?;

        if config.global.deploy_target == DeployTarget::Aws {
            ensure_ec2_metadata()?;
        }

        info!("Bootstrap config downloaded successfully");
        return Ok(config);
    }
}
