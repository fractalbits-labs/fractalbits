use crate::cmd_repo::{PREBUILT_REPO, all_repos, repo_has_changes};
use crate::{DeployTarget, PrebuiltCommand};
use cmd_lib::*;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Serialize)]
struct Manifest {
    build_time: String,
    repos: BTreeMap<String, RepoInfo>,
}

#[derive(Serialize)]
struct RepoInfo {
    commit: String,
    branch: String,
}

pub fn run_cmd_prebuilt(cmd: PrebuiltCommand) -> CmdResult {
    match cmd {
        PrebuiltCommand::Publish {
            skip_build,
            dry_run,
            allow_dirty,
        } => publish(skip_build, dry_run, allow_dirty),
        PrebuiltCommand::Update => update(),
    }
}

fn publish(skip_build: bool, dry_run: bool, allow_dirty: bool) -> CmdResult {
    info!("Starting prebuilt publish process...");

    // Verify prebuilt directory exists and is a git repo
    if !Path::new("prebuilt/.git").exists() {
        cmd_die!("prebuilt/ directory is not a git repository. Run 'cargo xtask repo init' first.");
    }

    // Verify all source repos are clean (unless --allow-dirty is set)
    if !allow_dirty {
        verify_repos_clean()?;
    } else {
        warn!("--allow-dirty: Skipping dirty check");
    }

    if !skip_build {
        // Build prebuilt binaries for x86_64 (Docker/local use)
        info!("Building prebuilt binaries for x86_64...");
        crate::cmd_build::build_prebuilt(true)?;

        // Build binaries for all CPU targets (deployment)
        info!("Building binaries for all CPU targets...");
        crate::cmd_deploy::build(DeployTarget::All, true, &[], &[])?;
    } else {
        info!("Skipping build step (--skip-build)");
    }

    // Generate manifest.json
    let manifest = generate_manifest()?;
    let manifest_json = serde_json::to_string_pretty(&manifest)
        .map_err(|e| std::io::Error::other(format!("Failed to serialize manifest: {e}")))?;

    if dry_run {
        info!("Dry run mode - not committing or pushing");
        info!("Would write manifest.json:");
        println!("{manifest_json}");
        return Ok(());
    }

    // Write manifest.json to prebuilt/
    let manifest_path = "prebuilt/manifest.json";
    fs::write(manifest_path, &manifest_json)?;
    info!("Wrote manifest to {manifest_path}");

    // Generate commit message
    let commit_message = generate_commit_message(&manifest);

    // Commit and push
    commit_and_push(&commit_message)?;

    info!("Prebuilt binaries published successfully!");
    Ok(())
}

fn verify_repos_clean() -> CmdResult {
    info!("Verifying all source repos are clean...");

    let mut dirty_repos = Vec::new();
    for repo in all_repos() {
        if repo_has_changes(repo.path) {
            dirty_repos.push(repo.path);
        }
    }

    if !dirty_repos.is_empty() {
        let dirty_list = dirty_repos.join(", ");
        cmd_die!("The following repos have uncommitted changes: $dirty_list");
    }

    info!("All repos are clean");
    Ok(())
}

fn generate_manifest() -> Result<Manifest, std::io::Error> {
    let build_time = run_fun!(date --utc "+%Y-%m-%dT%H:%M:%SZ")?;

    let mut repos = BTreeMap::new();
    for repo in all_repos() {
        let path = repo.path;
        let (commit, branch) = if path == "." {
            let commit = run_fun!(git rev-parse --short HEAD)?;
            let branch = run_fun!(git branch --show-current)?;
            (commit.trim().to_string(), branch.trim().to_string())
        } else {
            let commit = run_fun!(cd $path; git rev-parse --short HEAD)?;
            let branch = run_fun!(cd $path; git branch --show-current)?;
            (commit.trim().to_string(), branch.trim().to_string())
        };

        repos.insert(path.to_string(), RepoInfo { commit, branch });
    }

    Ok(Manifest { build_time, repos })
}

fn generate_commit_message(manifest: &Manifest) -> String {
    let mut msg = String::from("Update prebuilt binaries\n\n");

    // Find the max path length for alignment
    let max_len = manifest.repos.keys().map(|k| k.len()).max().unwrap_or(0);

    for (path, info) in &manifest.repos {
        msg.push_str(&format!(
            "{:width$} {}\n",
            path,
            info.commit,
            width = max_len
        ));
    }

    msg
}

fn commit_and_push(commit_message: &str) -> CmdResult {
    info!("Committing changes in prebuilt/...");

    // Stage all changes
    run_cmd! {
        cd prebuilt;
        git add -A;
    }?;

    // Check if there are any changes to commit
    let has_changes = run_cmd!(cd prebuilt; git diff-index --quiet --cached HEAD --).is_err();
    if !has_changes {
        info!("No changes to commit");
        return Ok(());
    }

    // Commit
    run_cmd! {
        cd prebuilt;
        git commit -m $commit_message;
    }?;

    // Push
    run_cmd! {
        info "Pushing to remote...";
        cd prebuilt;
        git push;
    }?;

    Ok(())
}

fn update() -> CmdResult {
    info!("Updating prebuilt binaries to latest version...");

    let path = PREBUILT_REPO.path;
    let url = PREBUILT_REPO.url;
    let branch = PREBUILT_REPO.branch;

    if Path::new(&format!("{path}/.git")).exists() {
        // Remove existing and re-clone with depth=1
        info!("Removing existing prebuilt directory...");
        run_cmd!(rm -rf $path)?;
    }

    run_cmd! {
        info "Cloning prebuilt repo (depth=1)...";
        git clone --depth=1 -b $branch $url $path;
    }?;

    info!("Prebuilt binaries updated successfully");
    Ok(())
}
