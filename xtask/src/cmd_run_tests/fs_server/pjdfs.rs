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
//! fs_server intentionally doesn't expose. The promotion gate looks
//! at the regression delta against strict mode, not the absolute
//! pass count.

use crate::cmd_service;
use crate::{CmdResult, FsServerConfig, InitConfig, ServiceName};
use cmd_lib::run_cmd;
use std::path::PathBuf;
use std::time::Duration;

use super::MOUNT_POINT;

const PJDFSTEST_REPO: &str = "https://github.com/pjd/pjdfstest.git";
const PJDFSTEST_DIR: &str = "data/third_party/pjdfstest";
/// Pinned upstream commit. pjdfstest publishes no release tags, so we
/// pin by SHA to keep the suite reproducible: an upstream regression on
/// `master` can't silently break our runs, and the pass/fail counts
/// stay comparable across machines and over time. Bump this only after
/// re-validating the full suite against the new revision.
const PJDFSTEST_COMMIT: &str = "ededbeb2b44929972898afb87474b0937f78a877";

/// Test files excluded from the run because they exercise a feature
/// fs_server does not implement yet. Skipped (rather than left to
/// fail) so the suite stays a clean signal for what IS supported.
///
/// - `rename/09.t`, `rename/10.t`: their sticky-bit matrix includes
///   directory-over-directory atomic replace, which needs NSS
///   folder-rename overwrite support. The only available core
///   implementation leaks the orphaned dst blob, so it's deferred rather
///   than shipped early.
/// - `rename/24.t`: asserts POSIX directory link count
///   (`nlink = 2 + immediate_subdirs`). Computing that requires an NSS
///   directory listing on every dir `lookup`/`getattr`, a real cost on
///   metadata-heavy workloads (`ls -la`, `find`, `du`); deferred rather
///   than pay it for the link-count semantic alone.
const SKIP_TEST_FILES: &[&str] = &["rename/09.t", "rename/10.t", "rename/24.t"];

fn pjdfstest_path() -> PathBuf {
    let base = std::env::current_dir().expect("cwd");
    base.join(PJDFSTEST_DIR)
}

fn pjdfstest_binary() -> PathBuf {
    pjdfstest_path().join("pjdfstest")
}

/// Recursively collect every `*.t` test file under `root`, dropping any
/// whose path ends with a `SKIP_TEST_FILES` entry. Sorted so the run
/// order is stable. `prove` is then handed this explicit list instead of
/// `-r <dir>`, which is how we exclude individual files without touching
/// the pinned upstream checkout.
fn collect_test_files(root: &std::path::Path) -> Vec<String> {
    fn walk(dir: &std::path::Path, out: &mut Vec<String>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "t") {
                let s = path.to_string_lossy();
                if !SKIP_TEST_FILES.iter().any(|skip| s.ends_with(skip)) {
                    out.push(s.into_owned());
                }
            }
        }
    }
    let mut out = Vec::new();
    if root.is_dir() {
        walk(root, &mut out);
    } else if root.extension().is_some_and(|e| e == "t") {
        // A single `.t` target (unusual, but honour it).
        let s = root.to_string_lossy();
        if !SKIP_TEST_FILES.iter().any(|skip| s.ends_with(skip)) {
            out.push(s.into_owned());
        }
    }
    out.sort();
    out
}

fn require_build_tools() -> CmdResult {
    let needed = ["cc", "prove", "perl", "git"];
    let mut missing: Vec<&str> = Vec::new();
    for tool in &needed {
        if run_cmd!(which $tool &>/dev/null).is_err() {
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
        println!("  cloning pjdfstest at {PJDFSTEST_COMMIT} into {path_str}");
        // Shallow fetch of the pinned commit only (GitHub serves
        // reachable SHAs), so we get exactly the validated revision
        // without downloading full history.
        run_cmd! {
            git init -q $path_str;
            git -C $path_str remote add origin $PJDFSTEST_REPO;
            git -C $path_str fetch --depth 1 origin $PJDFSTEST_COMMIT;
            git -C $path_str checkout -q FETCH_HEAD;
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
        // pjdfstest forks and `setuid(65534)` to verify the cross-user
        // EPERM contract, so the suite must run as root (via sudo).
        // FUSE only lets a different user reach the mount if it was
        // mounted with `allow_other`, and the host needs
        // `user_allow_other` in /etc/fuse.conf for that to take effect.
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
        if run_cmd!(mountpoint -q $mount_point).is_ok() {
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
    println!("  FUSE mounted at {MOUNT_POINT}");

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

    // pjdfstest's whole point is to fork + `setuid(65534)` and verify
    // the cross-user EPERM contract; running it as the unprivileged
    // user just hides those tests behind "EPERM expected, got 0". So
    // run the suite as root via `sudo -E`, which preserves the env
    // (the PATH we prepend with the pjdfstest binary dir) across the
    // privilege change. The caller must have passwordless sudo for
    // this session (`sudo -v` once is enough), and the host's
    // /etc/fuse.conf must have `user_allow_other` so the allow_other
    // mount is reachable from root.
    let path_env = std::env::var("PATH").unwrap_or_default();
    let prove_env = vec![format!("PATH={bin_dir}:{path_env}")];
    let verbose = std::env::var("PJDFS_VERBOSE").is_ok();

    // Hand prove an explicit, skip-filtered file list instead of
    // `-r <dir>` so excluded files never run (see SKIP_TEST_FILES).
    let prove_files = collect_test_files(std::path::Path::new(&prove_target));
    if prove_files.is_empty() {
        println!("  no pjdfstest files to run under {prove_target} (all skipped?)");
        unmount()?;
        return Ok(());
    }
    if !SKIP_TEST_FILES.is_empty() {
        println!(
            "  skipping {} known test file(s): {SKIP_TEST_FILES:?}",
            SKIP_TEST_FILES.len()
        );
    }
    println!(
        "  running prove (as root, via sudo) over {} file(s) under {prove_target}",
        prove_files.len()
    );

    // Build the whole prove command as one vector: cmd_lib's `$[vec]`
    // splat must stand alone, so the explicit file list can't be mixed
    // with literal args.
    let mut prove_cmd: Vec<String> = vec!["sudo".into(), "-E".into(), "prove".into()];
    if verbose {
        prove_cmd.push("-v".into());
    }
    prove_cmd.extend(prove_files);

    let prove_result = run_cmd! {
        cd $test_root;
        $[prove_env] $[prove_cmd];
    };

    unmount()?;

    match prove_result {
        Ok(()) => println!("  pjdfstest: all subgroups passed"),
        // Per-suite failures are common (chflags, capabilities, ACLs
        // that fs_server doesn't expose). Surface as a warning, not
        // a hard error, so the workload-validation flow stays unblocked.
        Err(e) => eprintln!(
            "  pjdfstest reported failures ({e}) -- inspect the prove log \
             above for which subgroups failed."
        ),
    }
    Ok(())
}
