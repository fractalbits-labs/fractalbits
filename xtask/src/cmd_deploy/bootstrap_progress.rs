use super::common::{DeployTarget, get_bootstrap_bucket_name};
use super::ssm_utils;
use crate::CmdResult;
use cmd_lib::*;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::io::Error;
use std::time::{Duration, Instant};
use xtask_common::{STAGE_BLUEPRINT_FILE, StageBlueprint, StageBlueprintEntry};

const POLL_INTERVAL_SECS: u64 = 2;
const TIMEOUT_SECS: u64 = 600; // 10 minutes

/// S3 access configuration for fetching progress data
struct S3Access {
    bucket: String,
    env_vars: Vec<String>,
}

impl S3Access {
    fn for_aws(target: DeployTarget) -> Result<Self, Error> {
        Ok(Self {
            bucket: get_bootstrap_bucket_name(target)?,
            env_vars: vec![],
        })
    }

    fn for_local_tunnel(local_port: u16) -> Self {
        Self {
            bucket: "fractalbits-bootstrap".to_string(),
            env_vars: vec![
                "AWS_DEFAULT_REGION=localdev".to_string(),
                format!("AWS_ENDPOINT_URL_S3=http://localhost:{local_port}"),
                "AWS_ACCESS_KEY_ID=test_api_key".to_string(),
                "AWS_SECRET_ACCESS_KEY=test_api_secret".to_string(),
            ],
        }
    }

    fn env_refs(&self) -> Vec<&str> {
        self.env_vars.iter().map(|s| s.as_str()).collect()
    }
}

fn get_blueprint(access: &S3Access) -> Result<StageBlueprint, Error> {
    let bucket = &access.bucket;
    let s3_path = format!("s3://{bucket}/{STAGE_BLUEPRINT_FILE}");
    let env_vars = access.env_refs();
    let content = run_fun!($[env_vars] aws s3 cp $s3_path - 2>/dev/null)
        .map_err(|e| Error::other(format!("Failed to download {STAGE_BLUEPRINT_FILE}: {e}")))?;

    serde_json::from_str(&content)
        .map_err(|e| Error::other(format!("Failed to parse {STAGE_BLUEPRINT_FILE}: {e}")))
}

/// Cached S3 listing for all stages - avoids repeated S3 calls
struct StageCache {
    /// Lines from `aws s3 ls --recursive` output
    lines: Vec<String>,
}

impl StageCache {
    fn fetch(access: &S3Access, cluster_id: &str) -> Self {
        let bucket = &access.bucket;
        let prefix = format!("s3://{bucket}/workflow/{cluster_id}/stages/");
        let env_vars = access.env_refs();
        let output =
            run_fun!($[env_vars] aws s3 ls --recursive $prefix 2>/dev/null).unwrap_or_default();
        let lines = output.lines().map(|s| s.to_string()).collect();
        Self { lines }
    }

    fn count_stage_completions(&self, stage: &str) -> usize {
        let stage_prefix = format!("stages/{stage}/");
        self.lines
            .iter()
            .filter(|l| l.contains(&stage_prefix) && l.ends_with(".json"))
            .count()
    }

    fn check_global_stage(&self, stage: &str) -> bool {
        let stage_file = format!("stages/{stage}.json");
        self.lines.iter().any(|l| l.contains(&stage_file))
    }
}

pub fn show_progress(target: DeployTarget) -> CmdResult {
    let access = S3Access::for_aws(target)?;
    show_progress_inner(&access)
}

/// Monitor bootstrap progress via SSM port-forwarding tunnel to Docker S3.
///
/// Starts an SSM session that forwards a local port to port 8080 on the
/// Docker host instance, then polls the Docker S3 for workflow stage data.
pub fn show_progress_from_docker(docker_host_id: &str) -> CmdResult {
    let local_port = find_available_port()?;
    info!(
        "Starting SSM tunnel to Docker S3 on {} (local port {})...",
        docker_host_id, local_port
    );

    let mut tunnel = start_ssm_tunnel(docker_host_id, local_port)?;

    // Wait for tunnel to be ready
    wait_for_tunnel_ready(local_port)?;
    info!("SSM tunnel established");

    let access = S3Access::for_local_tunnel(local_port);
    let result = show_progress_inner(&access);

    // Clean up tunnel
    let _ = tunnel.kill();
    let _ = tunnel.wait();

    result
}

/// Show bootstrap progress using the rss-A instance from CDK outputs.
pub fn show_progress_from_cdk_outputs() -> CmdResult {
    let outputs = ssm_utils::parse_cdk_outputs()?;
    let rss_a_id = outputs
        .get("rssAId")
        .ok_or_else(|| Error::other("CDK output 'rssAId' not found"))?;
    show_progress_from_docker(rss_a_id)
}

fn show_progress_inner(access: &S3Access) -> CmdResult {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap(),
    );
    spinner.set_message("Waiting for stage blueprint...");
    spinner.enable_steady_tick(Duration::from_millis(100));

    let blueprint = loop {
        match get_blueprint(access) {
            Ok(bp) => break bp,
            Err(_) => {
                std::thread::sleep(Duration::from_secs(POLL_INTERVAL_SECS));
            }
        }
    };
    spinner.finish_and_clear();

    let cluster_id = &blueprint.cluster_id;

    info!(
        "Monitoring bootstrap progress (cluster_id: {cluster_id}, {} stages)",
        blueprint.stages.len()
    );

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

    let mut bars: Vec<(ProgressBar, &StageBlueprintEntry, bool)> = Vec::new();

    for stage in &blueprint.stages {
        let pb = mp.add(ProgressBar::new(stage.expected as u64));
        if stage.is_global {
            pb.set_style(style_global_pending.clone());
        } else {
            pb.set_style(style_pending.clone());
        }
        pb.set_prefix("[  ]");
        pb.set_message(stage.desc.clone());
        bars.push((pb, stage, false));
    }

    loop {
        // Single S3 call per iteration - fetch all stage data at once
        let cache = StageCache::fetch(access, cluster_id);
        let mut all_complete = true;

        for (pb, stage, finished) in &mut bars {
            if *finished {
                continue;
            }

            let desc = &stage.desc;
            let expected = stage.expected;

            if stage.is_global {
                let complete = cache.check_global_stage(&stage.name);
                if complete {
                    pb.set_style(style_global_done.clone());
                    pb.set_prefix("[OK]");
                    pb.finish_with_message(desc.clone());
                    *finished = true;
                } else {
                    all_complete = false;
                    pb.set_style(style_global_progress.clone());
                    pb.set_prefix("[..]");
                }
            } else {
                let count = cache.count_stage_completions(&stage.name);
                pb.set_position(count as u64);

                if count >= expected {
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
            for (pb, _, _) in &bars {
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

fn find_available_port() -> Result<u16, Error> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|e| Error::other(format!("Failed to find available port: {e}")))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn start_ssm_tunnel(instance_id: &str, local_port: u16) -> Result<std::process::Child, Error> {
    let port_str = local_port.to_string();
    std::process::Command::new("aws")
        .args([
            "ssm",
            "start-session",
            "--target",
            instance_id,
            "--document-name",
            "AWS-StartPortForwardingSession",
            "--parameters",
            &format!("{{\"portNumber\":[\"8080\"],\"localPortNumber\":[\"{port_str}\"]}}"),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|e| Error::other(format!("Failed to start SSM tunnel: {e}")))
}

fn wait_for_tunnel_ready(local_port: u16) -> Result<(), Error> {
    let start = Instant::now();
    let timeout = Duration::from_secs(30);

    while start.elapsed() < timeout {
        if std::net::TcpStream::connect_timeout(
            &format!("127.0.0.1:{local_port}").parse().unwrap(),
            Duration::from_millis(500),
        )
        .is_ok()
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    Err(Error::other(format!(
        "SSM tunnel not ready after {timeout:?}"
    )))
}
