use cmd_lib::*;
use std::io::Error;
use std::time::{Duration, Instant};

use crate::common::{BOOTSTRAP_CLUSTER_CONFIG, ETC_PATH, download_from_s3, get_instance_id};

// Re-export types from xtask_common
pub use xtask_common::{
    BootstrapClusterConfig, ClusterEtcdConfig, ClusterNodeConfig, DataBlobStorage, DeployTarget,
    JournalType,
};

// Type aliases for backwards compatibility
pub type BootstrapConfig = BootstrapClusterConfig;
pub type EtcdConfig = ClusterEtcdConfig;
pub type InstanceConfig = ClusterNodeConfig;

const CONFIG_RETRY_TIMEOUT_SECS: u64 = 120;

pub fn download_and_parse(bucket_name: &str) -> Result<BootstrapClusterConfig, Error> {
    let bootstrap_bucket = format!("s3://{bucket_name}");
    let s3_path = format!("{bootstrap_bucket}/{BOOTSTRAP_CLUSTER_CONFIG}");
    let local_path = format!("{ETC_PATH}{BOOTSTRAP_CLUSTER_CONFIG}");

    let start_time = Instant::now();
    let timeout = Duration::from_secs(CONFIG_RETRY_TIMEOUT_SECS);
    loop {
        // Retry download if the file doesn't exist yet (race condition with CDK deployment)
        match download_from_s3(&s3_path, &local_path) {
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

        let instance_id = match config.global.deploy_target {
            DeployTarget::OnPrem => run_fun!(hostname)?,
            DeployTarget::Aws => get_instance_id()?,
        };

        if config.contains_instance(&instance_id) {
            info!("Found instance {instance_id} in bootstrap config");
            return Ok(config);
        }

        if start_time.elapsed() > timeout {
            info!(
                "Instance {instance_id} not in config after {CONFIG_RETRY_TIMEOUT_SECS}s, proceeding anyway"
            );
            return Ok(config);
        }

        info!("Instance {instance_id} not yet in config, waiting 5s and retrying...");
        std::thread::sleep(Duration::from_secs(5));
    }
}
