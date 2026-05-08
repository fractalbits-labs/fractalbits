//! pjdfstest driver. Clones, bootstraps, and runs the POSIX
//! filesystem compliance suite against an fs_server FUSE mount.
//!
//! pjdfstest is a third-party C + Perl test suite that walks the
//! POSIX system-call surface (`chmod`, `chown`, `link`, `mkdir`,
//! `mkfifo`, `open`, `rename`, `rmdir`, `symlink`, `truncate`,
//! `unlink`, `chflags`, `granular`). Each `.t` file under `tests/` is
//! a TAP-format prove(1) script that calls the local `pjdfstest`
//! binary with a small fixed grammar.
//!
//! Failures are documented but not fatal: many subdirs assume
//! Linux/BSD-specific features (chflags, capabilities, ACLs) that
//! fs_server intentionally doesn't expose.

use crate::cmd_service;
use crate::{CmdResult, FsServerConfig, InitConfig, ServiceName};
use cmd_lib::run_cmd;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use super::MOUNT_POINT;

const PJDFSTEST_REPO: &str = "https://github.com/pjd/pjdfstest.git";
const PJDFSTEST_DIR: &str = "data/third_party/pjdfstest";

fn pjdfstest_path() -> PathBuf {
    let base = std::env::current_dir().expect("cwd");
    base.join(PJDFSTEST_DIR)
}

fn pjdfstest_binary() -> PathBuf {
    pjdfstest_path().join("pjdfstest")
}

fn require_build_tools() -> CmdResult {
    let needed = ["cc", "prove", "perl", "git"];
    let mut missing: Vec<&str> = Vec::new();
    for tool in &needed {
        let ok = std::process::Command::new("which")
            .arg(tool)
            .stdout(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if !ok {
            missing.push(*tool);
        }
    }
    if !missing.is_empty() {
        return Err(std::io::Error::other(format!(
            "Missing build tools required by pjdfstest: {missing:?}\n  \
             Install via: sudo apt install -y build-essential perl git"
        )));
    }
    Ok(())
}

/// Hand-rolled `config.h` for Linux glibc. pjdfstest's upstream uses
/// autoconf to discover which `*at` syscalls and stat-timespec field
/// shapes the host has; we know the answers for Linux, so we sidestep
/// the autotools chain entirely. BSD-only flags
/// (`chflags`, `lchmod`, etc.) stay undefined so pjdfstest skips
/// those code paths.
const LINUX_CONFIG_H: &str = r#"/* Hand-rolled config.h for Linux glibc. See
 * xtask/src/cmd_run_tests/fs_server/pjdfs.rs for the source.
 */
#define HAVE_FACCESSAT 1
#define HAVE_FCHMODAT 1
#define HAVE_FCHOWNAT 1
#define HAVE_FSTATAT 1
#define HAVE_LINKAT 1
#define HAVE_MKDIRAT 1
#define HAVE_MKFIFOAT 1
#define HAVE_MKNODAT 1
#define HAVE_OPENAT 1
#define HAVE_POSIX_FALLOCATE 1
#define HAVE_RENAMEAT 1
#define HAVE_SYMLINKAT 1
#define HAVE_UNLINKAT 1
#define HAVE_UTIMENSAT 1
#define HAVE_SYS_SYSMACROS_H 1
#define HAVE_STRUCT_STAT_ST_ATIM 1
#define HAVE_STRUCT_STAT_ST_CTIM 1
#define HAVE_STRUCT_STAT_ST_MTIM 1
"#;

fn ensure_pjdfstest_built() -> CmdResult {
    require_build_tools()?;
    let path = pjdfstest_path();
    let binary = pjdfstest_binary();
    if binary.exists() {
        println!("  pjdfstest already built at {}", binary.display());
        return Ok(());
    }
    let parent = path.parent().expect("parent of pjdfstest_dir");
    std::fs::create_dir_all(parent)?;

    let path_str = path.to_string_lossy().to_string();
    if !path.exists() {
        println!("  cloning pjdfstest into {path_str}");
        run_cmd! {
            git clone --depth 1 $PJDFSTEST_REPO $path_str;
        }?;
    }
    // Drop the hand-rolled config.h next to pjdfstest.c and compile
    // the single source file directly. Skips autotools so the build
    // works on a barebones host (just gcc + make).
    std::fs::write(path.join("config.h"), LINUX_CONFIG_H)?;
    println!("  compiling pjdfstest (single-source, hand-rolled config.h)");
    let path_for_cmd = path_str.clone();
    run_cmd! {
        cd $path_for_cmd;
        cc -Wall -include config.h -o pjdfstest pjdfstest.c;
    }?;
    if !binary.exists() {
        return Err(std::io::Error::other(format!(
            "pjdfstest build did not produce {}",
            binary.display()
        )));
    }
    Ok(())
}

fn disk_cache_path() -> String {
    let base = std::env::current_dir().expect("Failed to get cwd");
    base.join("data/fuse_test_disk_cache")
        .to_string_lossy()
        .to_string()
}

fn fs_cfg(bucket: &str) -> FsServerConfig {
    FsServerConfig {
        bucket_name: bucket.to_string(),
        mount_point: MOUNT_POINT.to_string(),
        read_write: true,
        disk_cache_enabled: false,
        disk_cache_path: disk_cache_path(),
        // pjdfstest is meant to run as root so it can `setuid(65534)`
        // and exercise cross-user EPERM paths. We shell out to
        // `sudo prove`, which means root needs to read / write the
        // FUSE mount the daemon (running as the user) just created.
        // FUSE only allows that if mounted with `allow_other`, and
        // the host needs `user_allow_other` enabled in
        // /etc/fuse.conf for the user to set that flag.
        allow_other: true,
        ..Default::default()
    }
}

fn mount_fuse_default(bucket: &str) -> CmdResult {
    let mount_point = MOUNT_POINT;
    run_cmd! {
        ignore fusermount3 -u $mount_point 2>/dev/null;
        ignore fusermount -u $mount_point 2>/dev/null;
    }?;
    run_cmd!(mkdir -p $mount_point)?;

    let cfg = fs_cfg(bucket);
    cmd_service::init_service(
        ServiceName::FsServer,
        crate::cmd_build::BuildMode::Debug,
        &InitConfig {
            fs_server: cfg,
            ..Default::default()
        },
    )?;
    cmd_service::start_service(ServiceName::FsServer)?;

    for _ in 0..40 {
        std::thread::sleep(Duration::from_millis(500));
        if std::process::Command::new("mountpoint")
            .arg("-q")
            .arg(mount_point)
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
        {
            return Ok(());
        }
    }
    Err(std::io::Error::other(format!(
        "FUSE mount at {mount_point} not ready after 20 seconds"
    )))
}

fn unmount() -> CmdResult {
    let mount_point = MOUNT_POINT;
    run_cmd! {
        ignore fusermount3 -u $mount_point 2>/dev/null;
        ignore fusermount -u $mount_point 2>/dev/null;
    }?;
    let _ = cmd_service::stop_service(ServiceName::FsServer);
    run_cmd! { ignore pkill -x fs_server 2>/dev/null; }?;
    std::thread::sleep(Duration::from_millis(500));
    Ok(())
}

pub async fn run_pjdfstest(subdir: Option<&str>) -> CmdResult {
    ensure_pjdfstest_built()?;

    // Use a dedicated bucket so prior runs don't pollute the namespace.
    let bucket_name = "fs-pjdfs";
    let _ctx = {
        let ctx = test_common::context();
        ctx.create_bucket(bucket_name).await;
        ctx
    };

    mount_fuse_default(bucket_name)?;
    println!("  FUSE mounted at {MOUNT_POINT} (writeback=default)");

    // pjdfstest expects to be run from a working dir that is itself
    // a writable test root. It creates files / dirs in `.` and
    // expects the local `pjdfstest` binary to be on PATH.
    let test_root = format!("{MOUNT_POINT}/pjdfstest-root");
    std::fs::create_dir_all(&test_root)?;

    let pjd_dir = pjdfstest_path();
    let bin = pjdfstest_binary();
    let bin_dir = bin
        .parent()
        .expect("binary parent")
        .to_string_lossy()
        .to_string();

    // Run the prove suite. The standard layout is
    // `tests/<group>/NN.t`. Pass `-r` to recurse, `-v` for verbose
    // (so failures land in our log). When a subdir is given, scope
    // to that one group; otherwise run everything.
    let prove_target = match subdir {
        Some(s) => format!("{}/tests/{}", pjd_dir.display(), s),
        None => format!("{}/tests", pjd_dir.display()),
    };
    println!("  running prove (as root, via sudo) against {prove_target}");

    // Inherit PATH+pjdfstest dir so the .t scripts find the binary.
    let path_env = std::env::var("PATH").unwrap_or_default();
    let path_with_pjd = format!("{bin_dir}:{path_env}");

    // pjdfstest's whole point is to fork + setuid(65534) and verify
    // the cross-user EPERM contract; running it as the unprivileged
    // user just hides those tests behind "EPERM expected, got 0".
    // We invoke `sudo -E` so the env (PATH + the pjdfstest binary
    // dir we just prepended) is preserved across the privilege drop.
    // Caller must have passwordless sudo configured for this session
    // (`sudo -v` once is enough); the host's /etc/fuse.conf must
    // also have `user_allow_other` so the FUSE mount is reachable
    // from root.
    let verbose = std::env::var("PJDFS_VERBOSE").is_ok();
    let mut args: Vec<&str> = vec!["-E", "prove"];
    if verbose {
        args.push("-v");
    }
    args.push("-r");
    args.push(&prove_target);
    let status = Command::new("sudo")
        .args(&args)
        .current_dir(&test_root)
        .env("PATH", &path_with_pjd)
        .status()?;

    unmount()?;

    if !status.success() {
        // Per-suite failures are common (chflags, capabilities, ACLs
        // that fs_server doesn't expose). Surface as a warning, not
        // a hard error, so the workload-validation flow stays unblocked.
        eprintln!(
            "  pjdfstest exited with {status:?} -- inspect the prove log \
             above for which subgroups failed."
        );
    } else {
        println!("  pjdfstest: all subgroups passed");
    }
    Ok(())
}
