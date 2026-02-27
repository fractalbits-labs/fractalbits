use super::common::{DeployTarget, get_bootstrap_bucket_name};
use crate::CmdResult;
use cmd_lib::*;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::io::Error;
use std::time::{Duration, Instant};
use xtask_common::{STAGE_BLUEPRINT_FILE, StageBlueprint, StageBlueprintEntry};

const POLL_INTERVAL_SECS: u64 = 2;
const TIMEOUT_SECS: u64 = 600; // 10 minutes

fn get_blueprint(bucket: &str) -> Result<StageBlueprint, Error> {
    let s3_path = format!("s3://{bucket}/{STAGE_BLUEPRINT_FILE}");
    let content = run_fun!(aws s3 cp $s3_path - 2>/dev/null)
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
    fn fetch(bucket: &str, cluster_id: &str) -> Self {
        let prefix = format!("s3://{bucket}/workflow/{cluster_id}/stages/");
        let output = run_fun!(aws s3 ls --recursive $prefix 2>/dev/null).unwrap_or_default();
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
    let bucket = get_bootstrap_bucket_name(target)?;

    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap(),
    );
    spinner.set_message("Waiting for stage blueprint...");
    spinner.enable_steady_tick(Duration::from_millis(100));

    let blueprint = loop {
        match get_blueprint(&bucket) {
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
        let cache = StageCache::fetch(&bucket, cluster_id);
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
