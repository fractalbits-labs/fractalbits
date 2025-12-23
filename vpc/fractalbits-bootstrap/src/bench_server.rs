mod yaml_get;
mod yaml_mixed;
mod yaml_put;

use super::common::*;
use crate::config::{BootstrapConfig, VpcTarget};
use crate::workflow::{WorkflowBarrier, WorkflowServiceType, stages, timeouts};
use cmd_lib::*;
use std::time::Duration;
use {yaml_get::*, yaml_mixed::*, yaml_put::*};

struct WorkloadConfig {
    size_kb: usize,
    put_concurrent_ops: usize,
    get_concurrent_ops: usize,
    mixed_concurrent_ops: usize,
}

const WORKLOAD_CONFIGS: &[WorkloadConfig] = &[
    WorkloadConfig {
        size_kb: 4,
        put_concurrent_ops: 36,
        get_concurrent_ops: 72,
        mixed_concurrent_ops: 54,
    },
    WorkloadConfig {
        size_kb: 64,
        put_concurrent_ops: 6,
        get_concurrent_ops: 12,
        mixed_concurrent_ops: 9,
    },
];

pub fn bootstrap(
    config: &BootstrapConfig,
    api_server_endpoint: String,
    bench_client_num: usize,
) -> CmdResult {
    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Bench)?;
    barrier.complete_stage(stages::INSTANCES_READY, None)?;

    let mut binaries = vec!["warp"];
    if config.is_etcd_backend() {
        binaries.push("etcdctl");
    }
    download_binaries(config, &binaries)?;
    setup_serial_console_password()?;

    // When using etcd backend, wait for etcd cluster to be ready before service discovery
    if config.is_etcd_backend() {
        info!("Waiting for etcd cluster to be ready...");
        barrier.wait_for_global(stages::ETCD_READY, timeouts::ETCD_READY)?;
        info!("etcd cluster is ready");
    }

    let client_ips = get_service_ips_with_backend(config, "bench-client", bench_client_num);

    let region = match config.global.target {
        VpcTarget::Aws => get_current_aws_region()?,
        VpcTarget::OnPrem => "on-prem".to_string(),
    };
    let mut warp_client_ips = String::new();
    for ip in client_ips.iter() {
        warp_client_ips.push_str(&format!("  - {ip}:7761\n"));
    }

    for wl_config in WORKLOAD_CONFIGS {
        create_put_workload_config(
            &warp_client_ips,
            &region,
            &api_server_endpoint,
            "2m",
            wl_config.size_kb,
            wl_config.put_concurrent_ops,
        )?;
        create_get_workload_config(
            &warp_client_ips,
            &region,
            &api_server_endpoint,
            "2m",
            wl_config.size_kb,
            wl_config.get_concurrent_ops,
        )?;
        create_mixed_workload_config(
            &warp_client_ips,
            &region,
            &api_server_endpoint,
            "2m",
            wl_config.size_kb,
            wl_config.mixed_concurrent_ops,
        )?;
    }

    info!(
        "Waiting for api_server endpoint {} to be ready",
        api_server_endpoint
    );
    while !check_port_ready(&api_server_endpoint, 80) {
        std::thread::sleep(Duration::from_secs(1));
    }
    info!(
        "api_server endpoint {}:80 is reachable",
        api_server_endpoint
    );

    create_bench_start_script(&region, &api_server_endpoint)?;

    barrier.complete_stage(stages::SERVICES_READY, None)?;

    Ok(())
}

fn create_bench_start_script(region: &str, api_server_ip: &str) -> CmdResult {
    let script_content = format!(
        r##"#!/bin/bash

export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret

set -ex
export AWS_DEFAULT_REGION={region}
export AWS_ENDPOINT_URL_S3=http://{api_server_ip}
bench_bucket=warp-benchmark-bucket

if ! aws s3api head-bucket --bucket $bench_bucket &>/dev/null; then
  aws s3api create-bucket --bucket $bench_bucket
  aws s3api wait bucket-exists --bucket $bench_bucket
  aws s3 ls
fi

/opt/fractalbits/bin/warp run /opt/fractalbits/etc/bench_${{WORKLOAD:-put_4k}}.yml
"##
    );
    run_cmd! {
        mkdir -p $BIN_PATH;
        echo $script_content > $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
        chmod +x $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
    }?;
    Ok(())
}
