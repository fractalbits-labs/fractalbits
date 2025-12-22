use crate::cmd_deploy::{VpcTarget, get_bootstrap_bucket_name};
use cmd_lib::*;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::Deserialize;
use std::collections::HashMap;
use std::io::Error;
use std::time::{Duration, Instant};

#[derive(Debug, Deserialize)]
struct BootstrapConfigGlobal {
    workflow_cluster_id: Option<String>,
    #[serde(default)]
    num_bss_nodes: Option<usize>,
    #[serde(default)]
    num_api_servers: Option<usize>,
    #[serde(default)]
    num_bench_clients: Option<usize>,
    #[serde(default)]
    rss_ha_enabled: bool,
    #[serde(default)]
    rss_backend: String,
}

#[derive(Debug, Deserialize)]
struct InstanceConfig {
    service_type: String,
}

#[derive(Debug, Deserialize)]
struct BootstrapConfig {
    global: BootstrapConfigGlobal,
    #[serde(default)]
    instances: HashMap<String, InstanceConfig>,
}

const POLL_INTERVAL_SECS: u64 = 2;
const TIMEOUT_SECS: u64 = 600; // 10 minutes

struct StageInfo {
    name: &'static str,
    desc: &'static str,
    is_global: bool,
}

const STAGES: &[StageInfo] = &[
    StageInfo {
        name: "00-instances-ready",
        desc: "Instances ready",
        is_global: false,
    },
    StageInfo {
        name: "10-etcd-ready",
        desc: "etcd cluster formed",
        is_global: true,
    },
    StageInfo {
        name: "20-rss-initialized",
        desc: "RSS config published",
        is_global: true,
    },
    StageInfo {
        name: "30-nss-formatted",
        desc: "NSS formatted",
        is_global: false,
    },
    StageInfo {
        name: "40-nss-journal-ready",
        desc: "NSS journal ready",
        is_global: false,
    },
    StageInfo {
        name: "50-bss-configured",
        desc: "BSS configured",
        is_global: false,
    },
    StageInfo {
        name: "60-services-ready",
        desc: "Services ready",
        is_global: false,
    },
];

struct WorkflowConfig {
    cluster_id: String,
    num_bss: usize,
    num_nss: usize,
    num_rss: usize,
    num_api: usize,
    num_bench: usize,
    use_etcd: bool,
}

fn parse_workflow_config(content: &str) -> Result<WorkflowConfig, Error> {
    let config: BootstrapConfig = toml::from_str(content)
        .map_err(|e| Error::other(format!("Failed to parse bootstrap.toml: {e}")))?;

    let cluster_id = config
        .global
        .workflow_cluster_id
        .ok_or_else(|| Error::other("workflow_cluster_id not found in config"))?;

    let num_bss = config.global.num_bss_nodes.unwrap_or(1);
    let num_rss = if config.global.rss_ha_enabled { 2 } else { 1 };
    let use_etcd = config.global.rss_backend == "etcd";

    // Get num_api from config, fallback to counting instances
    let num_api = config.global.num_api_servers.unwrap_or_else(|| {
        config
            .instances
            .values()
            .filter(|i| i.service_type == "api_server")
            .count()
    });

    // Get num_bench from config (bench clients + 1 bench server if present)
    let num_bench = config.global.num_bench_clients.map(|n| n + 1).unwrap_or(0);

    // Count NSS from instances
    let num_nss = config
        .instances
        .values()
        .filter(|i| i.service_type == "nss_server")
        .count()
        .max(1);

    Ok(WorkflowConfig {
        cluster_id,
        num_bss,
        num_nss,
        num_rss,
        num_api,
        num_bench,
        use_etcd,
    })
}

fn get_workflow_config(bucket: &str) -> Result<WorkflowConfig, Error> {
    let s3_path = format!("s3://{bucket}/bootstrap.toml");
    let content = run_fun!(aws s3 cp $s3_path - 2>/dev/null)
        .map_err(|e| Error::other(format!("Failed to download bootstrap.toml: {e}")))?;

    parse_workflow_config(&content)
}

#[allow(dead_code)]
fn get_workflow_config_by_id(bucket: &str, cluster_id: &str) -> Result<WorkflowConfig, Error> {
    let s3_path = format!("s3://{bucket}/workflow/{cluster_id}/bootstrap.toml");
    let content = run_fun!(aws s3 cp $s3_path - 2>/dev/null)
        .map_err(|e| Error::other(format!("Failed to download workflow config: {e}")))?;

    parse_workflow_config(&content)
}

fn count_stage_completions(bucket: &str, cluster_id: &str, stage: &str) -> usize {
    let prefix = format!("s3://{bucket}/workflow/{cluster_id}/stages/{stage}/");
    let output = run_fun!(aws s3 ls $prefix 2>/dev/null).unwrap_or_default();
    output.lines().filter(|l| l.ends_with(".json")).count()
}

fn check_global_stage(bucket: &str, cluster_id: &str, stage: &str) -> bool {
    let path = format!("s3://{bucket}/workflow/{cluster_id}/stages/{stage}.json");
    let result = run_fun!(aws s3 ls $path 2>/dev/null);
    result.is_ok() && !result.unwrap().trim().is_empty()
}

pub fn show_progress() -> CmdResult {
    let bucket = get_bootstrap_bucket_name(VpcTarget::Aws)?;

    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap(),
    );
    spinner.set_message("Waiting for bootstrap config...");
    spinner.enable_steady_tick(Duration::from_millis(100));

    let config = loop {
        match get_workflow_config(&bucket) {
            Ok(config) => break config,
            Err(_) => {
                std::thread::sleep(Duration::from_secs(POLL_INTERVAL_SECS));
            }
        }
    };
    spinner.finish_and_clear();

    let WorkflowConfig {
        cluster_id,
        num_bss,
        num_nss,
        num_rss,
        num_api,
        num_bench,
        use_etcd,
    } = config;
    let total_nodes = num_bss + num_nss + num_rss + num_api + num_bench;

    info!("Monitoring bootstrap progress (cluster_id: {cluster_id}, {total_nodes} total nodes)");

    let mp = MultiProgress::new();
    let start_time = Instant::now();
    let timeout = Duration::from_secs(TIMEOUT_SECS);

    // Create progress bars for each stage
    let style_pending = ProgressStyle::default_bar()
        .template("  {prefix:.dim} {msg}")
        .unwrap();
    let style_progress = ProgressStyle::default_bar()
        .template("  {prefix:.yellow} {msg} [{bar:20.yellow}] {pos}/{len}")
        .unwrap()
        .progress_chars("=> ");
    let style_done = ProgressStyle::default_bar()
        .template("  {prefix:.green} {msg}")
        .unwrap();
    let style_global_pending = ProgressStyle::default_bar()
        .template("  {prefix:.dim} {msg}")
        .unwrap();
    let style_global_progress = ProgressStyle::default_bar()
        .template("  {prefix:.yellow} {msg}")
        .unwrap();
    let style_global_done = ProgressStyle::default_bar()
        .template("  {prefix:.green} {msg}")
        .unwrap();

    let mut bars: Vec<(ProgressBar, &StageInfo, usize, bool)> = Vec::new();

    for stage in STAGES {
        if stage.name == "10-etcd-ready" && !use_etcd {
            continue;
        }

        let expected = if stage.is_global {
            1
        } else if stage.name == "30-nss-formatted" || stage.name == "40-nss-journal-ready" {
            num_nss
        } else if stage.name == "50-bss-configured" {
            num_bss
        } else {
            total_nodes
        };

        let pb = mp.add(ProgressBar::new(expected as u64));
        if stage.is_global {
            pb.set_style(style_global_pending.clone());
        } else {
            pb.set_style(style_pending.clone());
        }
        pb.set_prefix("[  ]");
        pb.set_message(stage.desc.to_string());
        bars.push((pb, stage, expected, false)); // false = not finished
    }

    loop {
        let mut all_complete = true;

        for (pb, stage, expected, finished) in &mut bars {
            if *finished {
                continue;
            }

            let desc = stage.desc;
            if stage.is_global {
                let complete = check_global_stage(&bucket, &cluster_id, stage.name);
                if complete {
                    pb.set_style(style_global_done.clone());
                    pb.set_prefix("[OK]");
                    pb.finish_with_message(desc.to_string());
                    *finished = true;
                } else {
                    all_complete = false;
                    pb.set_style(style_global_progress.clone());
                    pb.set_prefix("[..]");
                }
            } else {
                let count = count_stage_completions(&bucket, &cluster_id, stage.name);
                pb.set_position(count as u64);

                if count >= *expected {
                    pb.set_style(style_done.clone());
                    pb.set_prefix("[OK]");
                    pb.finish_with_message(format!("{desc}: {count}/{expected}"));
                    *finished = true;
                } else if count > 0 {
                    all_complete = false;
                    pb.set_style(style_progress.clone());
                    pb.set_prefix("[..]");
                } else {
                    all_complete = false;
                    pb.set_style(style_pending.clone());
                    pb.set_prefix("[  ]");
                    pb.set_message(format!("{desc}: {count}/{expected}"));
                }
            }
        }

        if all_complete {
            break;
        }

        if start_time.elapsed() > timeout {
            for (pb, _, _, _) in &bars {
                pb.abandon();
            }
            return Err(Error::other(format!(
                "Bootstrap timed out after {TIMEOUT_SECS} seconds"
            )));
        }

        std::thread::sleep(Duration::from_secs(POLL_INTERVAL_SECS));
    }

    info!("Bootstrap completed");

    Ok(())
}
