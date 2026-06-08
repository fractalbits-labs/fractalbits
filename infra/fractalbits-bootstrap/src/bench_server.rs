mod yaml_get;
mod yaml_mixed;
mod yaml_put;

use super::common::*;
use crate::config::BootstrapConfig;
use crate::stage_helpers::{BenchServicesReadyStage, InstancesReadyStage};
use crate::workflow::{WorkflowBarrier, WorkflowServiceType};
use cmd_lib::*;
use std::time::Duration;
use {yaml_get::*, yaml_mixed::*, yaml_put::*};

struct WorkloadConfig {
    size_kb: usize,
    put_concurrent_ops: usize,
    get_concurrent_ops: usize,
    mixed_concurrent_ops: usize,
}

// Round a queue depth up to the nearest multiple of 4, floored at 4.
fn round_up_to_4(n: usize) -> usize {
    n.div_ceil(4).max(1) * 4
}

// Per-client queue depth derived from cluster size. The /3 on puts is 3-way
// replication, dropped for a single bss.
//
//   4KB put = (80 * bss_count * 6 / 3) / bench_clients
//   4KB get = (96 * bss_count * 6)     / bench_clients
//   mixed   = average of put and get
//   64KB    = 4KB depth / 8
fn workload_configs(bss_count: usize, bench_clients: usize) -> Vec<WorkloadConfig> {
    let bench = bench_clients.max(1);
    let put_replication = if bss_count == 1 { 1 } else { 3 };
    let put_4k = (80 * bss_count * 6 / put_replication) / bench;
    let get_4k = (96 * bss_count * 6) / bench;
    let mixed_4k = (put_4k + get_4k) / 2;
    vec![
        WorkloadConfig {
            size_kb: 4,
            put_concurrent_ops: round_up_to_4(put_4k),
            get_concurrent_ops: round_up_to_4(get_4k),
            mixed_concurrent_ops: round_up_to_4(mixed_4k),
        },
        WorkloadConfig {
            size_kb: 64,
            put_concurrent_ops: round_up_to_4(put_4k / 8),
            get_concurrent_ops: round_up_to_4(get_4k / 8),
            mixed_concurrent_ops: round_up_to_4(mixed_4k / 8),
        },
    ]
}

pub fn bootstrap(
    config: &BootstrapConfig,
    api_server_endpoint: String,
    bench_client_num: usize,
) -> CmdResult {
    let barrier = WorkflowBarrier::from_config(config, WorkflowServiceType::Bench)?;
    InstancesReadyStage::complete(&barrier)?;

    let mut binaries = vec!["warp"];
    if config.is_etcd_backend() {
        binaries.push("etcdctl");
    }
    download_binaries(config, &binaries)?;
    setup_serial_console_password()?;

    // When using etcd backend, wait for etcd cluster to be ready before service discovery
    if config.is_etcd_backend() {
        info!("Waiting for etcd cluster to be ready...");
        BenchServicesReadyStage::wait_for_etcd_ready(&barrier)?;
        info!("etcd cluster is ready");
    }

    ensure_aws_cli()?;

    let client_ips = get_service_ips_with_backend(config, "bench-client", bench_client_num);

    let region = config.global.region.as_str();
    let mut warp_client_ips = String::new();
    for ip in client_ips.iter() {
        warp_client_ips.push_str(&format!("  - {ip}:7761\n"));
    }

    let bss_count = config.global.num_bss_nodes.unwrap_or(1);
    let workload_configs = workload_configs(bss_count, bench_client_num);
    for wl_config in &workload_configs {
        create_put_workload_config(
            &warp_client_ips,
            region,
            &api_server_endpoint,
            "2m",
            wl_config.size_kb,
            wl_config.put_concurrent_ops,
        )?;
        create_get_workload_config(
            &warp_client_ips,
            region,
            &api_server_endpoint,
            "2m",
            wl_config.size_kb,
            wl_config.get_concurrent_ops,
        )?;
        create_mixed_workload_config(
            &warp_client_ips,
            region,
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

    create_bench_start_script(region, &api_server_endpoint)?;

    BenchServicesReadyStage::complete(&barrier)?;

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
