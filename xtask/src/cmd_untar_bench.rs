use crate::CmdResult;
use crate::cmd_build::BuildMode;
use crate::cmd_run_tests::fs_server::{self, MOUNT_POINT};
use crate::cmd_service;
use crate::{DataBlobStorage, FsServerConfig, InitConfig, ServiceName};
use cmd_lib::*;
use std::time::Instant;

/// Untar the requested tarball onto a read-write FUSE mount and report
/// wall-clock time per iteration. Reuses the fs_server integration harness's
/// cluster + bucket + mount setup. Run `just build --release` first.
pub async fn run(disk_cache: bool, tarball: String, iterations: u32) -> CmdResult {
    let mode = BuildMode::Release;

    if !std::path::Path::new(&tarball).exists() {
        return Err(std::io::Error::other(format!(
            "tarball not found: {tarball}"
        )));
    }

    // Clean slate.
    let _ = cmd_service::stop_service(ServiceName::FsServer);
    cmd_service::stop_service(ServiceName::All)?;
    fs_server::ensure_fuse_uring()?;

    // Bring up the backend cluster in the requested build mode.
    cmd_service::init_service(
        ServiceName::All,
        mode,
        &InitConfig {
            data_blob_storage: DataBlobStorage::AllInBssSingleAz,
            bss_count: 6,
            ..Default::default()
        },
    )?;
    cmd_service::start_service(ServiceName::All)?;

    let (_ctx, bucket) = fs_server::setup_test_bucket().await;

    // Mount fs_server in normal strict mode.
    let mount_point = MOUNT_POINT;
    run_cmd! {
        ignore fusermount3 -u $mount_point 2>/dev/null;
        ignore fusermount -u $mount_point 2>/dev/null;
    }?;
    run_cmd!(mkdir -p $mount_point)?;

    let dc_path = format!("{}/data/untar_bench_disk_cache", run_fun!(pwd)?);
    let mut fs_cfg = FsServerConfig {
        bucket_name: bucket.clone(),
        mount_point: mount_point.to_string(),
        read_write: true,
        ..Default::default()
    };
    if disk_cache {
        run_cmd!(rm -rf $dc_path)?;
        run_cmd!(mkdir -p $dc_path)?;
        fs_cfg.disk_cache_enabled = true;
        fs_cfg.disk_cache_path = dc_path.clone();
        fs_cfg.disk_cache_size_gb = 20;
    }
    cmd_service::init_service(
        ServiceName::FsServer,
        mode,
        &InitConfig {
            fs_server: fs_cfg,
            ..Default::default()
        },
    )?;
    cmd_service::start_service(ServiceName::FsServer)?;
    cmd_service::wait_for_service_ready(ServiceName::FsServer, 15)?;

    println!(
        "=== untar bench: mode=strict disk_cache={disk_cache} tarball={tarball} iterations={iterations} ==="
    );
    for i in 0..iterations {
        let dest = format!("{mount_point}/untar{i}");
        run_cmd!(mkdir -p $dest)?;
        let start = Instant::now();
        run_cmd!(tar xf $tarball -C $dest)?;
        let elapsed = start.elapsed();
        let nfiles = run_fun!(find $dest -type f)?.lines().count();
        println!(
            "UNTAR_RESULT iter={i} secs={:.2} files={nfiles}",
            elapsed.as_secs_f64()
        );
    }

    // Teardown.
    run_cmd! {
        ignore fusermount3 -u $mount_point 2>/dev/null;
        ignore fusermount -u $mount_point 2>/dev/null;
    }?;
    let _ = cmd_service::stop_service(ServiceName::FsServer);
    cmd_service::stop_service(ServiceName::All)?;
    Ok(())
}
