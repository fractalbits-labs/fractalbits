use crate::*;

pub fn bootstrap(
    bucket: Option<&str>,
    nss_endpoint: &str,
    remote_az: Option<&str>,
    rss_ha_enabled: bool,
    for_bench: bool,
) -> CmdResult {
    download_binaries(&["api_server"])?;
    create_config(bucket, nss_endpoint, remote_az, rss_ha_enabled)?;

    info!("Creating directories for api_server");
    run_cmd!(mkdir -p "/data/local/stats")?;

    if for_bench {
        // Try to download tools for micro-benchmarking
        download_binaries(&["rewrk_rpc", "test_fractal_art"])?;
    }

    create_ena_irq_affinity_service()?;

    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("api_server", true)?;
    create_ddb_register_and_deregister_service("api-server")?;

    Ok(())
}

pub fn create_config(
    bucket: Option<&str>,
    nss_endpoint: &str,
    remote_az: Option<&str>,
    rss_ha_enabled: bool,
) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let num_cores = num_cpus()?;

    // Query DDB for RSS instance IPs
    let expected_rss_count = if rss_ha_enabled { 2 } else { 1 };
    let rss_ips = get_service_ips("root-server", expected_rss_count);
    let rss_addrs_toml = rss_ips
        .iter()
        .map(|ip| format!("\"{}:8088\"", ip))
        .collect::<Vec<_>>()
        .join(", ");

    let config_content = if let Some(remote_az) = remote_az {
        // S3 Express Multi-AZ configuration
        let local_az = get_current_aws_az_id()?;
        let local_bucket = get_s3_express_bucket_name(&local_az)?;
        let remote_bucket = get_s3_express_bucket_name(remote_az)?;

        format!(
            r##"nss_addr = "{nss_endpoint}:8088"
rss_addrs = [{rss_addrs_toml}]
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
    } else {
        // Hybrid single az configuration
        let bucket_name =
            bucket.ok_or_else(|| std::io::Error::other("Bucket name required for hybrid mode"))?;
        format!(
            r##"nss_addr = "{nss_endpoint}:8088"
rss_addrs = [{rss_addrs_toml}]
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
    };

    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$API_SERVER_CONFIG
    }?;
    Ok(())
}
