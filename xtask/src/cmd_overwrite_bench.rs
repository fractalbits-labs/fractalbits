use crate::cmd_build::BuildMode;
use crate::cmd_run_tests::fs_server::{self, MOUNT_POINT};
use crate::cmd_service;
use crate::{DataBlobStorage, FsServerConfig, InitConfig, ServiceName};
use cmd_lib::*;

/// Overwrite-heavy fio benchmark (VM-image / database profile) on a
/// read-write FUSE mount: preallocate a file, hammer it with random 4k
/// overwrites under periodic fdatasync, then remount and measure cold
/// random and sequential reads of the fragmented file. Reuses the
/// untar-bench cluster/bucket/mount setup, with 6 BSS nodes so the
/// data volume is erasure coded (4+2). Deterministic fio seeds keep
/// the offset sequence identical across runs and branches. Run
/// `just build --release` first.
pub async fn run(disk_cache: bool, file_mb: u32, write_secs: u32, read_secs: u32) -> CmdResult {
    let mode = BuildMode::Release;

    // Clean slate.
    let _ = cmd_service::stop_service(ServiceName::FsServer);
    cmd_service::stop_service(ServiceName::All)?;
    fs_server::ensure_fuse_uring()?;

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

    let mount_point = MOUNT_POINT;
    run_cmd! {
        ignore fusermount3 -u $mount_point 2>/dev/null;
        ignore fusermount -u $mount_point 2>/dev/null;
    }?;
    run_cmd!(mkdir -p $mount_point)?;

    let dc_path = format!("{}/data/owbench_disk_cache", run_fun!(pwd)?);
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

    let bench_file = format!("{mount_point}/owbench.bin");
    let size = format!("{file_mb}M");
    println!(
        "=== overwrite bench: file={file_mb}MiB overwrite={write_secs}s reads={read_secs}s disk_cache={disk_cache} ==="
    );

    // Phase 0: lay the file down sequentially and commit it.
    println!("--- PHASE prep: sequential write {size} ---");
    run_cmd!(
        fio --name=prep --filename=$bench_file --size=$size --rw=write --bs=1M
            --ioengine=psync --direct=0 --numjobs=1 --iodepth=1 --end_fsync=1
            --randrepeat=1 --allrandrepeat=1
    )?;

    // Phase 1: random 4k overwrites, one fdatasync per 32 writes, so
    // every flush publishes a batch of rewritten 128 KiB blocks.
    println!("--- PHASE overwrite: randwrite 4k fdatasync=32 {write_secs}s ---");
    run_cmd!(
        fio --name=overwrite --filename=$bench_file --size=$size --rw=randwrite --bs=4k
            --ioengine=psync --direct=0 --numjobs=1 --iodepth=1 --fdatasync=32
            --time_based --runtime=$write_secs --randrepeat=1 --allrandrepeat=1
    )?;

    // Cold-read phases, each behind a remount so neither the kernel page
    // cache nor any mount-local state carries over from the writer.
    for (name, rw, bs, time_based) in [
        ("randread-4k", "randread", "4k", true),
        ("randread-128k", "randread", "128k", true),
        ("seqread-1m", "read", "1M", false),
    ] {
        cmd_service::stop_service(ServiceName::FsServer)?;
        run_cmd! {
            ignore fusermount3 -u $mount_point 2>/dev/null;
            ignore fusermount -u $mount_point 2>/dev/null;
        }?;
        cmd_service::start_service(ServiceName::FsServer)?;
        cmd_service::wait_for_service_ready(ServiceName::FsServer, 15)?;

        println!("--- PHASE {name}: {rw} bs={bs} (cold mount) ---");
        if time_based {
            run_cmd!(
                fio --name=$name --filename=$bench_file --size=$size --rw=$rw --bs=$bs
                    --ioengine=psync --direct=0 --numjobs=1 --iodepth=1
                    --time_based --runtime=$read_secs --randrepeat=1 --allrandrepeat=1
            )?;
        } else {
            run_cmd!(
                fio --name=$name --filename=$bench_file --size=$size --rw=$rw --bs=$bs
                    --ioengine=psync --direct=0 --numjobs=1 --iodepth=1
                    --randrepeat=1 --allrandrepeat=1
            )?;
        }
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
