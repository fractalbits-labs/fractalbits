use crate::*;
use cmd_lib::*;
use comfy_table::{Cell, Color, Table, presets};
use std::path::Path;

#[derive(Clone)]
pub struct Repo {
    pub path: &'static str,
    pub url: &'static str,
    pub branch: &'static str,
}

// Define git repos as constants
const GIT_REPOS: &[Repo] = &[
    Repo {
        path: ".",
        url: "https://github.com/fractalbits-labs/fractalbits-main.git",
        branch: "main",
    },
    Repo {
        path: ZIG_REPO_PATH,
        url: "https://github.com/fractalbits-labs/fractalbits-core.git",
        branch: "main",
    },
    Repo {
        path: "crates/ha",
        url: "https://github.com/fractalbits-labs/fractalbits-ha.git",
        branch: "main",
    },
    Repo {
        path: "misc",
        url: "https://github.com/fractalbits-labs/fractalbits-misc.git",
        branch: "main",
    },
    Repo {
        path: "crates/root_server",
        url: "https://github.com/fractalbits-labs/fractalbits-root_server.git",
        branch: "main",
    },
    Repo {
        path: UI_REPO_PATH,
        url: "https://github.com/fractalbits-labs/fractalbits-ui.git",
        branch: "main",
    },
    PREBUILT_REPO,
];
pub const PREBUILT_REPO: Repo = Repo {
    path: "prebuilt",
    url: "https://github.com/fractalbits-labs/fractalbits-prebuilt.git",
    branch: "main",
};

pub fn run_cmd_repo(repo_cmd: RepoCommand) -> CmdResult {
    match repo_cmd {
        RepoCommand::List => list_repos()?,
        RepoCommand::Status => show_repos_status()?,
        RepoCommand::Init { all } => init_repos(all)?,
        RepoCommand::Foreach { command } => run_foreach_repo(&command)?,
    }
    Ok(())
}

fn list_repos() -> CmdResult {
    info!("Listing all repos ...");

    let mut table = Table::new();
    table.load_preset(presets::ASCII_BORDERS_ONLY_CONDENSED);
    table.set_header(vec!["Path", "URL", "Branch"]);

    for repo in all_repos() {
        table.add_row(vec![repo.path, repo.url, repo.branch]);
    }

    println!("{table}");
    Ok(())
}

pub fn all_repos() -> impl Iterator<Item = &'static Repo> {
    GIT_REPOS.iter().filter(|repo| {
        repo.path != "prebuilt" && Path::new(&format!("{}/.git/", repo.path)).exists()
    })
}

pub fn repo_has_changes(path: &str) -> bool {
    let has_staged_changes =
        |path: &str| run_cmd!(cd $path; git diff-index --quiet --cached HEAD --).is_err();
    let has_local_changes = |path: &str| run_cmd!(cd $path; git diff --quiet).is_err();
    has_staged_changes(path) || has_local_changes(path)
}

fn repo_has_unpushed_commits(path: &str) -> bool {
    let count = run_fun! {
        cd $path;
        git rev-list "@{u}..HEAD" --count 2>/dev/null
    }
    .unwrap_or_else(|_| "0".to_string());
    count.trim() != "0"
}

fn show_repos_status() -> CmdResult {
    info!("Checking repo status...");

    let mut table = Table::new();
    table.load_preset(presets::ASCII_BORDERS_ONLY_CONDENSED);
    table.set_header(vec![
        "Path", "User", "Branch", "Status", "Commit", "Message",
    ]);

    for repo in all_repos() {
        let path = repo.path;
        // Get current branch and commit
        let (branch, commit, message, status) = if path == "." {
            let branch = run_fun!(git branch --show-current)?;
            let commit = run_fun!(git rev-parse --short HEAD)?;
            let message = run_fun!(git log --oneline -1 --pretty=format:"%s")?;
            let status = if repo_has_changes(path) {
                "modified"
            } else if repo_has_unpushed_commits(path) {
                "committed"
            } else {
                "clean"
            };
            (
                branch.trim().to_string(),
                commit.trim().to_string(),
                message.trim().to_string(),
                status,
            )
        } else {
            let branch = run_fun! {
                cd $path;
                git branch --show-current
            }?;
            let commit = run_fun! {
                cd $path;
                git rev-parse --short HEAD
            }?;
            let message = run_fun! {
                cd $path;
                git log --oneline -1 --pretty=format:"%s"
            }?;
            let status = if repo_has_changes(path) {
                "modified"
            } else if repo_has_unpushed_commits(path) {
                "committed"
            } else {
                "clean"
            };
            (
                branch.trim().to_string(),
                commit.trim().to_string(),
                message.trim().to_string(),
                status,
            )
        };

        let username = run_fun! {
            cd $path;
            git config user.name
        }?;

        // Create cells with appropriate colors
        let status_cell = match status {
            "clean" => Cell::new(status).fg(Color::Green),
            "modified" => Cell::new(status).fg(Color::Cyan),
            "committed" => Cell::new(status).fg(Color::Yellow),
            _ => Cell::new(status),
        };

        let branch_cell = if branch != "main" {
            Cell::new(&branch).fg(Color::Yellow)
        } else {
            Cell::new(&branch)
        };

        table.add_row(vec![
            Cell::new(path),
            Cell::new(&username),
            branch_cell,
            status_cell,
            Cell::new(&commit),
            Cell::new(&message),
        ]);
    }

    println!("{table}");
    Ok(())
}

fn init_repos(all: bool) -> CmdResult {
    info!("Initializing repos ...");

    let all_repos = if all {
        &GIT_REPOS[1..] // Skip the main repo (.)
    } else {
        &[PREBUILT_REPO]
    };
    for repo in all_repos {
        let path = repo.path;
        let branch = repo.branch;
        let url = repo.url;

        if !Path::new(path).exists() {
            if path == "prebuilt" {
                run_cmd! {
                    info "Cloning repo: prebuilt (depth=1)";
                    git clone --depth=1 -b $branch $url $path;
                }?;
            } else {
                run_cmd! {
                    info "Cloning repo: $path";
                    git clone -b $branch $url $path;
                }?;
            }
        } else {
            info!("Git repo already exists: {}", path);
        }
    }

    info!("Repos initialized successfully");
    Ok(())
}

fn run_foreach_repo(command: &[String]) -> CmdResult {
    info!("Running command in each repo: {command:?} ...");

    for repo in all_repos() {
        let path = repo.path;
        run_cmd! {
            info "Running in repo: $path";
            cd $path;
            $[command] 2>&1;
        }?;
    }

    Ok(())
}
