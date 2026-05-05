//! A/B benchmark for the writeback metadata cache promotion gate.
//! Untar a tarball into a freshly-created bucket mounted via FUSE in
//! strict mode, then again in default (writeback) mode, and report
//! wall-clock so a strict-vs-default speedup can be read off
//! directly.
//!
//! Strict is the synchronous path: every FUSE op blocks on its NSS /
//! BSS RPC. Default routes the enabled op slice through the
//! writeback queue, batched InodeBatch on Stage A, and batched
//! BssBatch on Stage B; everything else falls back to strict.

use crate::cmd_service;
use crate::{CmdResult, FsServerConfig, InitConfig, ServiceName};
use cmd_lib::run_cmd;
use std::io;
use std::process::Command;
use std::time::{Duration, Instant};

use super::{MOUNT_POINT, setup_test_bucket};

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
        ..Default::default()
    }
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

fn mount_with_mode(bucket: &str, writeback_mode: &str) -> CmdResult {
    let mount_point = MOUNT_POINT;
    run_cmd! {
        ignore fusermount3 -u $mount_point 2>/dev/null;
        ignore fusermount -u $mount_point 2>/dev/null;
    }?;
    run_cmd!(mkdir -p $mount_point)?;

    let mut cfg = fs_cfg(bucket);
    cfg.writeback_mode = writeback_mode.to_string();
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
        "FUSE mount at {mount_point} not ready after 20 seconds (mode={writeback_mode})"
    )))
}

async fn run_one(label: &str, mode: &str, tarball: &str) -> io::Result<Duration> {
    println!("\n=== Bench: tar -xf  (writeback={mode})  [{label}] ===");

    // Per-mode bucket so the second run starts cold (untar the
    // tarball into an empty namespace, mirroring the kernel-tar
    // shape).
    let bucket_name = format!("fs-bench-{mode}");
    let _ctx = {
        let ctx = test_common::context();
        ctx.create_bucket(&bucket_name).await;
        ctx
    };

    mount_with_mode(&bucket_name, mode)?;
    println!("  FUSE ready");

    let target_dir = format!("{MOUNT_POINT}/untar");
    std::fs::create_dir_all(&target_dir)?;

    let tar_start = Instant::now();
    // `--touch` skips the post-extract utime() call. fs_server today
    // does not preserve atime/mtime via SETATTR, so utime would surface
    // a stream of ENOENT-shaped warnings that bury the real workload
    // signal. Mtime preservation is orthogonal to this benchmark.
    let status = Command::new("tar")
        .args(["-xf", tarball, "--touch", "-C", &target_dir])
        .status()?;
    if !status.success() {
        unmount()?;
        return Err(std::io::Error::other(format!(
            "tar exit code {status:?} (mode={mode})"
        )));
    }
    let tar_elapsed = tar_start.elapsed();

    // Force any FUSE writeback queue to drain so the wall-clock
    // measurement captures all the work the workload triggered (not
    // just the ops that fired before tar exited). Strict mode treats
    // sync as a no-op (writes are already on disk); default-mode
    // sync waits for the queue to settle.
    let sync_start = Instant::now();
    let _ = Command::new("sync").status();
    let sync_elapsed = sync_start.elapsed();

    let elapsed = tar_elapsed + sync_elapsed;
    println!(
        "  tar: {:.2}s   sync: {:.2}s",
        tar_elapsed.as_secs_f64(),
        sync_elapsed.as_secs_f64()
    );
    println!("  wall: {:.2}s", elapsed.as_secs_f64());

    unmount()?;
    Ok(elapsed)
}

pub async fn run_fs_bench(tarball: &str) -> CmdResult {
    if !std::path::Path::new(tarball).exists() {
        return Err(std::io::Error::other(format!(
            "Tarball not found: {tarball} -- create one e.g. `tar -cf {tarball} -C /usr include/`"
        )));
    }

    let _ = setup_test_bucket().await; // warm S3 client / RSS state

    let strict_t = run_one("baseline", "strict", tarball).await?;
    let default_t = run_one("default", "default", tarball).await?;

    println!("\n=== A/B summary ===");
    println!("  strict   wall: {:.2}s", strict_t.as_secs_f64());
    println!("  default  wall: {:.2}s", default_t.as_secs_f64());
    if default_t.as_secs_f64() > 0.0 {
        let speedup = strict_t.as_secs_f64() / default_t.as_secs_f64();
        println!("  speedup:      {speedup:.2}x");
    }
    Ok(())
}
