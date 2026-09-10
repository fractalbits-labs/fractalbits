use crate::CmdResult;
use crate::cmd_build::BuildMode;
use crate::cmd_run_tests::fs_server::{self, MOUNT_POINT};
use crate::cmd_service;
use crate::{DataBlobStorage, FsMountConfig, InitConfig, ServiceName};
use cmd_lib::*;
use std::time::Instant;

/// Untar the requested tarball onto a read-write FUSE mount and report
/// wall-clock time per iteration. Reuses the FUSE integration harness's
/// cluster + bucket + mount setup. Run `just build --release` first.
pub async fn run(
    disk_cache: bool,
    tarball: String,
    iterations: u32,
    writeback_mode: &str,
) -> CmdResult {
    let mode = BuildMode::Release;

    if !std::path::Path::new(&tarball).exists() {
        return Err(std::io::Error::other(format!(
            "tarball not found: {tarball}"
        )));
    }

    // Clean slate.
    let _ = fs_server::unmount_fs(MOUNT_POINT);
    let _ = fs_server::stop_gateway();
    cmd_service::stop_service(ServiceName::All)?;
    fs_server::ensure_fuse_uring()?;

    // Bring up the backend cluster in the requested build mode.
    cmd_service::init_service(
        ServiceName::All,
        mode,
        &InitConfig {
            data_blob_storage: DataBlobStorage::AllInBssSingleAz,
            bss_count: 1,
            ..Default::default()
        },
    )?;
    cmd_service::start_service(ServiceName::All)?;

    let (_ctx, bucket) = fs_server::setup_test_bucket().await;

    // Mount through the gateway in the requested writeback mode.
    let mount_point = MOUNT_POINT;
    let dc_path = format!("{}/data/untar_bench_disk_cache", run_fun!(pwd)?);
    if disk_cache {
        run_cmd!(rm -rf $dc_path)?;
    }
    fs_server::ensure_gateway(mode, &fs_server::gateway_config(disk_cache, &dc_path, 20))?;
    let fs_cfg = FsMountConfig {
        bucket_name: bucket.clone(),
        mount_point: mount_point.to_string(),
        read_write: true,
        writeback_mode: writeback_mode.to_string(),
        ..Default::default()
    };
    fs_server::mount_fs(mode, &fs_cfg)?;

    println!(
        "=== untar bench: writeback_mode={writeback_mode} disk_cache={disk_cache} tarball={tarball} iterations={iterations} ==="
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
    let _ = fs_server::unmount_fs(mount_point);
    let _ = fs_server::stop_gateway();
    cmd_service::stop_service(ServiceName::All)?;
    Ok(())
}
