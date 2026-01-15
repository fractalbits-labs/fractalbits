use crate::config::{BootstrapConfig, DataBlobStorage, DeployTarget, JournalType};
use crate::workflow::{WorkflowBarrier, WorkflowServiceType, stages, timeouts};
use crate::*;
use std::io::Error;

pub fn bootstrap(config: &BootstrapConfig, for_bench: bool) -> CmdResult {
    let remote_az = config.aws.as_ref().and_then(|aws| aws.remote_az.as_deref());

    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Api)?;
    // Complete instances-ready stage
    barrier.complete_stage(stages::INSTANCES_READY, None)?;

    let mut binaries = vec!["api_server"];
    if config.is_etcd_backend() {
        binaries.push("etcdctl");
    }
    download_binaries(config, &binaries)?;

    // Wait for RSS to initialize before we can get RSS IPs
    info!("Waiting for RSS to initialize...");
    barrier.wait_for_global(stages::RSS_INITIALIZED, timeouts::RSS_INITIALIZED)?;

    // Wait for NSS journals to be ready before we can serve requests
    // For NVMe journal, only active (nss-A) publishes journal-ready; standby runs mirrord
    let expected_journal_ready =
        if remote_az.is_some() && config.global.journal_type != JournalType::Nvme {
            2
        } else {
            1
        };
    info!("Waiting for {expected_journal_ready} NSS journal(s) to be ready...");
    barrier.wait_for_nodes(
        stages::NSS_JOURNAL_READY,
        expected_journal_ready,
        timeouts::NSS_JOURNAL_READY,
    )?;

    create_config(config)?;

    info!("Creating directories for api_server");
    run_cmd!(mkdir -p "/data/local/stats")?;

    if for_bench {
        // Try to download tools for micro-benchmarking
        let _ = download_binaries(config, &["rewrk_rpc"]);
    }

    if config.global.deploy_target == DeployTarget::Aws {
        create_ena_irq_affinity_service()?;
    }

    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("api_server", true)?;
    register_service(config, "api-server")?;

    // Signal that API server is ready
    barrier.complete_stage(stages::SERVICES_READY, None)?;

    Ok(())
}

pub fn create_config(config: &BootstrapConfig) -> CmdResult {
    let data_blob_storage = &config.global.data_blob_storage;
    let data_blob_bucket = config
        .aws
        .as_ref()
        .and_then(|aws| aws.data_blob_bucket.as_deref());
    let remote_az = config.aws.as_ref().and_then(|aws| aws.remote_az.as_deref());
    let rss_ha_enabled = config.global.rss_ha_enabled;

    let region = &config.global.region;
    let num_cores = num_cpus()?;

    // Query service discovery for RSS instance IPs
    let expected_rss_count = if rss_ha_enabled { 2 } else { 1 };
    let rss_ips = get_service_ips_with_backend(config, "root-server", expected_rss_count);
    let rss_addrs_toml = rss_ips
        .iter()
        .map(|ip| format!("\"{}:8088\"", ip))
        .collect::<Vec<_>>()
        .join(", ");

    let config_content = if *data_blob_storage == DataBlobStorage::S3ExpressMultiAz {
        let remote_az =
            remote_az.ok_or_else(|| Error::other("remote_az required for s3_express_multi_az"))?;
        let aws_region = get_current_aws_region()?;
        // S3 Express Multi-AZ configuration
        let local_az = get_current_aws_az_id()?;
        let local_bucket = get_s3_express_bucket_name(&local_az)?;
        let remote_bucket = get_s3_express_bucket_name(remote_az)?;

        format!(
            r##"rss_addrs = [{rss_addrs_toml}]
region = "{aws_region}"
port = 80
mgmt_port = 18088
root_domain = ".localhost"
with_metrics = true
http_request_timeout_seconds = 100
rpc_request_timeout_seconds = 5
rpc_connection_timeout_seconds = 5
rss_rpc_timeout_seconds = 30
client_request_timeout_seconds = 10
stats_dir = "/data/local/stats"
enable_stats_writer = false
allow_missing_or_bad_signature = false
worker_threads = {num_cores}
set_thread_affinity = true

[https]
enabled = false
port = 443
cert_file = "/opt/fractalbits/etc/cert.pem"
key_file = "/opt/fractalbits/etc/key.pem"
force_http1_only = false

[blob_storage]
backend = "s3_express_multi_az"

[blob_storage.s3_express_multi_az]
local_az_host = "http://s3.{aws_region}.amazonaws.com"
local_az_port = 80
remote_az_host = "http://s3.{aws_region}.amazonaws.com"
remote_az_port = 80
s3_region = "{aws_region}"
local_az_bucket = "{local_bucket}"
remote_az_bucket = "{remote_bucket}"
local_az = "{local_az}"
remote_az = "{remote_az}"

[blob_storage.s3_express_multi_az.ratelimit]
enabled = false
put_qps = 7000
get_qps = 10000
delete_qps = 5000

[blob_storage.s3_express_multi_az.retry_config]
enabled = false
max_attempts = 15
initial_backoff_us = 50
max_backoff_us = 500
backoff_multiplier = 1.0
"##
        )
    } else if let Some(bucket_name) = data_blob_bucket {
        // S3 Hybrid single-az configuration (AWS only)
        let aws_region = get_current_aws_region()?;
        format!(
            r##"rss_addrs = [{rss_addrs_toml}]
region = "{aws_region}"
port = 80
mgmt_port = 18088
root_domain = ".localhost"
with_metrics = true
http_request_timeout_seconds = 100
rpc_request_timeout_seconds = 15
rpc_connection_timeout_seconds = 5
rss_rpc_timeout_seconds = 30
client_request_timeout_seconds = 10
stats_dir = "/data/local/stats"
enable_stats_writer = false
allow_missing_or_bad_signature = false
worker_threads = {num_cores}
set_thread_affinity = true

[https]
enabled = false
port = 443
cert_file = "/opt/fractalbits/etc/cert.pem"
key_file = "/opt/fractalbits/etc/key.pem"
force_http1_only = false

[blob_storage]
backend = "s3_hybrid_single_az"

[blob_storage.s3_hybrid_single_az]
s3_host = "http://s3.{aws_region}.amazonaws.com"
s3_port = 80
s3_region = "{aws_region}"
s3_bucket = "{bucket_name}"

[blob_storage.s3_hybrid_single_az.ratelimit]
enabled = false
put_qps = 7000
get_qps = 10000
delete_qps = 5000

[blob_storage.s3_hybrid_single_az.retry_config]
enabled = true
max_attempts = 8
initial_backoff_us = 15000
max_backoff_us = 2000000
backoff_multiplier = 1.8
"##
        )
    } else {
        // AllInBss single-az configuration
        format!(
            r##"rss_addrs = [{rss_addrs_toml}]
region = "{region}"
port = 80
mgmt_port = 18088
root_domain = ".localhost"
with_metrics = true
http_request_timeout_seconds = 100
rpc_request_timeout_seconds = 15
rpc_connection_timeout_seconds = 5
rss_rpc_timeout_seconds = 30
client_request_timeout_seconds = 10
stats_dir = "/data/local/stats"
enable_stats_writer = false
allow_missing_or_bad_signature = false
worker_threads = {num_cores}
set_thread_affinity = true

[https]
enabled = false
port = 443
cert_file = "/opt/fractalbits/etc/cert.pem"
key_file = "/opt/fractalbits/etc/key.pem"
force_http1_only = false

[blob_storage]
backend = "all_in_bss_single_az"
"##
        )
    };

    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$API_SERVER_CONFIG
    }?;
    Ok(())
}
