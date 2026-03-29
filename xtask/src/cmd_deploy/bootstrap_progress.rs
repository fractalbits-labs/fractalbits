use super::common::DeployTarget;
use crate::CmdResult;
use cmd_lib::*;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::io::Error;
use std::time::{Duration, Instant};
use xtask_common::{STAGE_BLUEPRINT_FILE, StageBlueprint, StageBlueprintEntry};

const POLL_INTERVAL_SECS: u64 = 2;
const TIMEOUT_SECS: u64 = 1500; // 25 minutes (format_zero=true is slower)

/// Cloud storage access configuration for fetching progress data
enum CloudAccess {
    AwsS3 { bucket: String },
    Gcs { bucket: String },
}

impl CloudAccess {
    fn for_aws() -> Result<Self, Error> {
        let bucket = super::aws::get_aws_bootstrap_bucket()?;
        Ok(Self::AwsS3 { bucket })
    }

    fn for_gcp(gcp_project: Option<&str>) -> Result<Self, Error> {
        let project_id = super::gcp::resolve_gcp_project(gcp_project)?;
        Ok(Self::Gcs {
            bucket: format!("{project_id}-deploy-staging"),
        })
    }

    fn download(&self, key: &str) -> Result<String, Error> {
        match self {
            Self::AwsS3 { bucket } => {
                let s3_path = format!("s3://{bucket}/{key}");
                run_fun!(aws s3 cp $s3_path - 2>/dev/null)
            }
            Self::Gcs { bucket } => {
                let gs_path = format!("gs://{bucket}/{key}");
                run_fun!(gcloud storage cat $gs_path 2>/dev/null)
            }
        }
    }

    fn list_recursive(&self, prefix: &str) -> String {
        match self {
            Self::AwsS3 { bucket } => {
                let s3_prefix = format!("s3://{bucket}/{prefix}");
                run_fun!(aws s3 ls --recursive $s3_prefix 2>/dev/null).unwrap_or_default()
            }
            Self::Gcs { bucket } => {
                let gs_prefix = format!("gs://{bucket}/{prefix}**");
                run_fun!(gcloud storage ls $gs_prefix 2>/dev/null).unwrap_or_default()
            }
        }
    }
}

fn get_blueprint(access: &CloudAccess) -> Result<StageBlueprint, Error> {
    let content = access
        .download(STAGE_BLUEPRINT_FILE)
        .map_err(|e| Error::other(format!("Failed to download {STAGE_BLUEPRINT_FILE}: {e}")))?;

    serde_json::from_str(&content)
        .map_err(|e| Error::other(format!("Failed to parse {STAGE_BLUEPRINT_FILE}: {e}")))
}

/// Cached cloud storage listing for all stages
struct StageCache {
    lines: Vec<String>,
    is_gcs: bool,
}

impl StageCache {
    fn fetch(access: &CloudAccess, cluster_id: &str) -> Self {
        let prefix = format!("workflow/{cluster_id}/stages/");
        let output = access.list_recursive(&prefix);
        let is_gcs = matches!(access, CloudAccess::Gcs { .. });
        let lines = output.lines().map(|s| s.to_string()).collect();
        Self { lines, is_gcs }
    }

    fn count_stage_completions(&self, stage: &str) -> usize {
        if self.is_gcs {
            // GCS listing returns full paths like: gs://bucket/workflow/.../stages/00-instances-ready/node1.json
            let stage_suffix = format!("stages/{stage}/");
            self.lines
                .iter()
                .filter(|l| l.contains(&stage_suffix) && l.ends_with(".json"))
                .count()
        } else {
            let stage_prefix = format!("stages/{stage}/");
            self.lines
                .iter()
                .filter(|l| l.contains(&stage_prefix) && l.ends_with(".json"))
                .count()
        }
    }

    fn check_global_stage(&self, stage: &str) -> bool {
        let stage_file = format!("stages/{stage}.json");
        self.lines.iter().any(|l| l.contains(&stage_file))
    }
}

pub fn show_progress(target: DeployTarget, gcp_project: Option<&str>) -> CmdResult {
    show_progress_with_bucket(target, gcp_project, None)
}

pub fn show_progress_with_bucket(
    target: DeployTarget,
    gcp_project: Option<&str>,
    gcs_bucket: Option<&str>,
) -> CmdResult {
    let access = match target {
        DeployTarget::Aws => CloudAccess::for_aws()?,
        DeployTarget::Gcp => {
            if let Some(bucket) = gcs_bucket {
                CloudAccess::Gcs {
                    bucket: bucket.to_string(),
                }
            } else {
                CloudAccess::for_gcp(gcp_project)?
            }
        }
        DeployTarget::OnPrem => {
            return Err(Error::other(
                "OnPrem progress monitoring not supported via cloud storage",
            ));
        }
    };
    show_progress_inner(&access)
}

/// Show bootstrap progress using AWS S3 (persisted data).
pub fn show_progress_from_cdk_outputs() -> CmdResult {
    let access = CloudAccess::for_aws()?;
    show_progress_inner(&access)
}

fn show_progress_inner(access: &CloudAccess) -> CmdResult {
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
