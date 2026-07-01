use crate::cmd_service;
use crate::{CmdResult, FsServerConfig, InitConfig, ServiceName};
use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use colored::*;
use std::path::Path;
use std::process::{Child, Command};
use std::time::Duration;

use super::{MOUNT_POINT, cleanup_objects, generate_test_data, setup_test_bucket};

const MOUNT_POINT_B: &str = "/tmp/fs_server_test_b";

fn disk_cache_path() -> String {
    let base = std::env::current_dir().expect("Failed to get cwd");
    base.join("data/fuse_test_disk_cache")
        .to_string_lossy()
        .to_string()
}

fn fs_server_config(bucket: &str, read_write: bool, disk_cache: bool) -> FsServerConfig {
    let mut cfg = FsServerConfig {
        bucket_name: bucket.to_string(),
        mount_point: MOUNT_POINT.to_string(),
        read_write,
        ..Default::default()
    };
    if disk_cache {
        cfg.disk_cache_enabled = true;
        cfg.disk_cache_path = disk_cache_path();
        cfg.disk_cache_size_gb = 1;
    }
    cfg
}

/// Same as `mount_fuse_with_opts` but sets `writeback_mode = "default"`
/// so the writeback queue / worker are active for this mount.
fn mount_fuse_writeback(bucket: &str, read_write: bool, disk_cache: bool) -> CmdResult {
    let mount_point = MOUNT_POINT;

    run_cmd! {
        ignore fusermount3 -u $mount_point 2>/dev/null;
        ignore fusermount -u $mount_point 2>/dev/null;
    }?;
    run_cmd!(mkdir -p $mount_point)?;
    if disk_cache {
        let dc_path = disk_cache_path();
        run_cmd!(mkdir -p $dc_path)?;
    }
    let mut fs_cfg = fs_server_config(bucket, read_write, disk_cache);
    fs_cfg.writeback_mode = "default".to_string();
    let init_config = InitConfig {
        fs_server: fs_cfg,
        ..Default::default()
    };
    cmd_service::init_service(
        ServiceName::FsServer,
        crate::cmd_build::BuildMode::Debug,
        &init_config,
    )?;
    cmd_service::start_service(ServiceName::FsServer)?;

    for i in 0..20 {
        std::thread::sleep(Duration::from_millis(500));
        if run_cmd!(mountpoint -q $mount_point).is_ok() {
            println!(
                "    FUSE (writeback=default) mounted at {} (after {}ms)",
                mount_point,
                (i + 1) * 500
            );
            return Ok(());
        }
    }

    Err(std::io::Error::other(format!(
        "FUSE mount at {} not ready after 10 seconds",
        mount_point
    )))
}

fn mount_fuse_ro(bucket: &str, disk_cache: bool) -> CmdResult {
    mount_fuse_with_opts(bucket, false, disk_cache)
}

fn mount_fuse_rw(bucket: &str, disk_cache: bool) -> CmdResult {
    mount_fuse_with_opts(bucket, true, disk_cache)
}

fn mount_fuse_with_opts(bucket: &str, read_write: bool, disk_cache: bool) -> CmdResult {
    let mount_point = MOUNT_POINT;

    // Clean up any stale FUSE mount (e.g. "Transport endpoint is not connected").
    run_cmd! {
        ignore fusermount3 -u $mount_point 2>/dev/null;
        ignore fusermount -u $mount_point 2>/dev/null;
    }?;
    run_cmd!(mkdir -p $mount_point)?;
    if disk_cache {
        let dc_path = disk_cache_path();
        run_cmd!(mkdir -p $dc_path)?;
    }
    // Explicit strict mode: the config default is writeback-on, so without
    // this the general FUSE suite (and this helper's callers) would never
    // exercise the strict synchronous publish path. Default-mode coverage
    // lives in the `mount_fuse_writeback` tests and pjdfstest.
    let mut fs_cfg = fs_server_config(bucket, read_write, disk_cache);
    fs_cfg.writeback_mode = "strict".to_string();
    let init_config = InitConfig {
        fs_server: fs_cfg,
        ..Default::default()
    };
    cmd_service::init_service(
        ServiceName::FsServer,
        crate::cmd_build::BuildMode::Debug,
        &init_config,
    )?;
    cmd_service::start_service(ServiceName::FsServer)?;

    // Wait for mount to appear (poll up to 10 seconds)
    for i in 0..20 {
        std::thread::sleep(Duration::from_millis(500));
        if run_cmd!(mountpoint -q $mount_point).is_ok() {
            println!(
                "    FUSE (writeback=strict) mounted at {} (after {}ms)",
                mount_point,
                (i + 1) * 500
            );
            return Ok(());
        }
    }

    Err(std::io::Error::other(format!(
        "FUSE mount at {} not ready after 10 seconds",
        mount_point
    )))
}

pub fn unmount_fuse() -> CmdResult {
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

// ── Second fs_server instance helpers ──────────────────────────────
//
// Spawns a second fs_server process directly (not via systemd) with
// a different mount point on the same bucket. Used for cross-instance
// cache invalidation tests.

fn spawn_second_fuse(bucket: &str, read_write: bool) -> std::io::Result<Child> {
    let mount_point = MOUNT_POINT_B;

    // Clean up any stale mount
    let _ = std::process::Command::new("fusermount3")
        .args(["-u", mount_point])
        .stderr(std::process::Stdio::null())
        .status();
    let _ = std::process::Command::new("fusermount")
        .args(["-u", mount_point])
        .stderr(std::process::Stdio::null())
        .status();
    std::fs::create_dir_all(mount_point)?;

    let binary = format!(
        "{}/target/debug/fs_server",
        std::env::current_dir()?.display()
    );
    let mut cmd = Command::new(&binary);
    cmd.env("FS_SERVER_BUCKET_NAME", bucket)
        .env("FS_SERVER_MOUNT_POINT", mount_point)
        .env("FS_SERVER_MODE", "fuse")
        .env("FS_SERVER_READ_WRITE", read_write.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    // Propagate LLVM_PROFILE_FILE for coverage instrumentation
    if let Ok(profile_file) = std::env::var("LLVM_PROFILE_FILE") {
        cmd.env("LLVM_PROFILE_FILE", profile_file);
    }
    let child = cmd.spawn()?;

    // Wait for mount to appear
    for i in 0..20 {
        std::thread::sleep(Duration::from_millis(500));
        if run_cmd!(mountpoint -q $mount_point).is_ok() {
            println!(
                "    Second FUSE mounted at {} (after {}ms)",
                mount_point,
                (i + 1) * 500
            );
            return Ok(child);
        }
    }

    Err(std::io::Error::other(format!(
        "Second FUSE mount at {} not ready after 10 seconds",
        mount_point
    )))
}

fn stop_second_fuse(mut child: Child) {
    let mount_point = MOUNT_POINT_B;
    let _ = std::process::Command::new("fusermount3")
        .args(["-u", mount_point])
        .stderr(std::process::Stdio::null())
        .status();
    let _ = std::process::Command::new("fusermount")
        .args(["-u", mount_point])
        .stderr(std::process::Stdio::null())
        .status();
    let _ = child.kill();
    let _ = child.wait();
}

pub async fn run_fuse_tests_with_disk_cache(disk_cache_only: bool) -> CmdResult {
    info!("Running FUSE integration tests...");

    if !disk_cache_only {
        println!(
            "\n{}",
            ">>> Running FUSE tests WITHOUT disk cache <<<".bold()
        );
        run_fuse_test_suite(false).await?;

        // Reinit services to clear stale state from the first suite.
        cmd_service::stop_service(ServiceName::All)?;
        cmd_service::init_service(
            ServiceName::All,
            crate::cmd_build::BuildMode::Debug,
            &crate::InitConfig::default(),
        )?;
        cmd_service::start_service(ServiceName::All)?;
    }

    println!("\n{}", ">>> Running FUSE tests WITH disk cache <<<".bold());
    run_fuse_test_suite(true).await?;

    println!("\n{}", "=== All FUSE Tests PASSED ===".green().bold());
    Ok(())
}

async fn run_fuse_test_suite(disk_cache: bool) -> CmdResult {
    let dc_label = if disk_cache { " [disk-cache]" } else { "" };

    // Start the disk-cache phase from a clean cache directory so the run is
    // deterministic. A real deployment uses unique (UUIDv7) blob ids per
    // object, so a persisted cache never collides across object lifetimes;
    // but repeated local suite runs share one fixed cache dir and accumulate
    // per-version entries from prior runs, which perturbs eviction/timing and
    // makes cache-mode tests (mmap, cross-instance overwrite) flaky. CI starts
    // from a fresh checkout (empty dir); mirror that here.
    if disk_cache {
        let dc_path = disk_cache_path();
        std::fs::remove_dir_all(&dc_path).ok();
    }

    macro_rules! run_test {
        ($name:expr, $func:ident) => {
            println!(
                "\n{}",
                format!("=== Test: {}{} ===", $name, dc_label).bold()
            );
            if let Err(e) = $func(disk_cache).await {
                eprintln!("{}: {}", "Test FAILED".red().bold(), e);
                return Err(e);
            }
        };
    }

    run_test!("Basic File Read", test_basic_file_read);
    run_test!("Directory Listing", test_directory_listing);
    run_test!("Large File Read", test_large_file_read);
    run_test!("Nested Directory Structure", test_nested_directories);
    run_test!("Create, Write, Read", test_create_write_read);
    run_test!("Large File Write", test_large_file_write);
    run_test!("Mkdir and Rmdir", test_mkdir_rmdir);
    run_test!("Unlink", test_unlink);
    run_test!(
        "Symlink (create / readlink / lstat / unlink)",
        test_symlink_basic
    );
    run_test!(
        "Writeback default mode (symlink commit via async worker)",
        test_writeback_default_mode_symlink
    );
    run_test!(
        "Hardlink: write after link is durable on both names",
        test_hardlink_write_visible
    );
    run_test!(
        "Hardlink: cold-Indirect write resolves to shared record",
        test_hardlink_write_cold_indirect
    );
    run_test!(
        "Hardlink: cross-alias chmod + write both survive",
        test_hardlink_chmod_then_write
    );
    run_test!(
        "Writeback default mode (async release of dirty file)",
        test_writeback_default_mode_async_release
    );
    run_test!(
        "Writeback default mode (5-level mkdir -p)",
        test_writeback_default_mode_mkdir
    );
    run_test!(
        "Writeback default mode (ancestor deps; 30 mkdirs)",
        test_writeback_default_mode_ancestor_deps
    );
    run_test!(
        "Writeback default mode (fsyncdir drains queue)",
        test_writeback_default_mode_fsyncdir
    );
    run_test!(
        "Writeback default mode (O_DSYNC per-write drain)",
        test_writeback_default_mode_o_sync
    );
    run_test!("Rename", test_rename);
    run_test!("Unlink with Open Handle", test_unlink_open_handle);
    run_test!("Overwrite Existing File", test_overwrite_existing);
    run_test!("Rename Atomic Replace", test_rename_atomic_replace);
    run_test!("Truncate Write", test_truncate_write);
    run_test!("Write in Subdirectory", test_write_in_subdirectory);
    run_test!("Rename Directory", test_rename_directory);
    run_test!("dd + fsync Write", test_dd_fsync);
    run_test!("mmap Write", test_mmap_write);
    run_test!("Fsync Persistence", test_fsync_persistence);
    run_test!("Truncate to Non-Zero Size", test_truncate_nonzero);

    // Sparse WriteBuffer + single-writer regression tests.
    run_test!("Sparse: Large Truncate", test_sparse_truncate_large);
    run_test!(
        "Sparse: Partial-Block Overwrite",
        test_sparse_partial_overwrite
    );
    run_test!(
        "Sparse: Dirty-Handle Read After Write",
        test_sparse_dirty_read_after_write
    );
    run_test!(
        "Sparse: Single-Writer EBUSY",
        test_sparse_single_writer_ebusy
    );
    run_test!(
        "Sparse: Override Flush Preserves Bytes",
        test_sparse_override_flush_persists
    );
    run_test!(
        "Sparse: Sparse File Round Trip Reads Zeros",
        test_sparse_sparse_file_round_trip
    );
    run_test!(
        "Sparse: Truncate-Then-Extend Reads Zeros",
        test_sparse_truncate_then_extend
    );
    run_test!(
        "Sparse: Shrink-Then-Grow Destroys Pre-Shrink Bytes",
        test_sparse_shrink_then_grow_destroys
    );

    // fallocate (sparse-file syscalls)
    run_test!("fallocate Extend Grows File", test_fallocate_extend);
    run_test!(
        "fallocate KEEP_SIZE Does Not Grow",
        test_fallocate_keep_size
    );
    run_test!(
        "fallocate PUNCH_HOLE Aligned Drops Block",
        test_fallocate_punch_hole_aligned
    );
    run_test!(
        "fallocate PUNCH_HOLE Edge Zeroes Bytes",
        test_fallocate_punch_hole_edge
    );
    run_test!(
        "fallocate PUNCH_HOLE Within Single Block",
        test_fallocate_punch_hole_single_block
    );
    run_test!(
        "lseek SEEK_DATA / SEEK_HOLE on Sparse File",
        test_lseek_seek_data_hole
    );
    run_test!("lseek SEEK_HOLE on Punched File", test_lseek_punched_hole);

    // Cache staleness tests: verify FUSE sees external S3 mutations after TTL
    run_test!(
        "External Create Visibility",
        test_external_create_visibility
    );
    run_test!(
        "External Overwrite Visibility",
        test_external_overwrite_visibility
    );
    run_test!(
        "External Delete Visibility",
        test_external_delete_visibility
    );
    run_test!(
        "External Rename Visibility",
        test_external_rename_visibility
    );

    // Cross-instance tests: two FUSE mounts on same bucket
    run_test!(
        "Cross-Instance Write Visibility",
        test_cross_instance_write_visibility
    );
    run_test!(
        "Cross-Instance Rename Visibility",
        test_cross_instance_rename_visibility
    );
    run_test!(
        "Cross-Instance Delete Visibility",
        test_cross_instance_delete_visibility
    );
    run_test!(
        "Cross-Instance Overwrite Visibility",
        test_cross_instance_overwrite_visibility
    );
    run_test!(
        "Cross-Instance Directory Owner After Listing",
        test_cross_instance_dir_owner_after_listing
    );

    // Destructive: stops/starts bss@0 to exercise override durability
    // across a replica partition+rejoin. Run it ONCE (no-cache phase) and
    // in isolation: the override write path is disk-cache-independent, and
    // running it in both phases double-cycles bss@0, racing the cluster's
    // recovery between phases. The reference branch invokes it via a separate
    // wrapper for the same reason.
    if !disk_cache {
        run_test!(
            "Override Survives BSS Partition-Rejoin",
            test_override_survives_bss_partition_rejoin
        );
    }

    // Disk-cache-specific tests (only run when disk_cache is enabled)
    if disk_cache {
        run_test!("Disk Cache Populates on Read", test_disk_cache_populates);
        run_test!("Disk Cache Hit on Re-read", test_disk_cache_hit_reread);
        run_test!(
            "Disk Cache Cold Start After Remount",
            test_disk_cache_cold_start
        );
        run_test!(
            "Disk Cache Survives Override (stable path + inline metadata)",
            test_disk_cache_survives_override
        );
        // Skip if fio isn't on PATH; the test is opt-in to the host
        // tooling rather than a hard dependency on the test fixture.
        if run_cmd!(bash -c "command -v fio").is_ok() {
            run_test!(
                "Disk Cache: qemu-style fio random-write workload",
                test_qemu_style_fio_workload
            );
        }
    }

    Ok(())
}

async fn test_basic_file_read(disk_cache: bool) -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload test objects via S3 API");
    let test_files: Vec<(&str, Vec<u8>)> = vec![
        ("hello.txt", b"Hello, FUSE!".to_vec()),
        ("numbers.dat", b"0123456789".to_vec()),
        ("empty.txt", b"".to_vec()),
    ];

    for (key, data) in &test_files {
        ctx.client
            .put_object()
            .bucket(&bucket)
            .key(*key)
            .body(ByteStream::from(data.clone()))
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));
        println!("    Uploaded: {} ({} bytes)", key, data.len());
    }

    println!("  Step 2: Mount FUSE filesystem");
    mount_fuse_ro(&bucket, disk_cache)?;

    println!("  Step 3: Read and verify files mount");
    for (key, expected_data) in &test_files {
        let fuse_path = format!("{}/{}", MOUNT_POINT, key);
        let actual_data =
            std::fs::read(&fuse_path).unwrap_or_else(|e| panic!("Failed to read {key}: {e}"));
        assert_eq!(actual_data, *expected_data, "{key}: data mismatch");
        println!("    {}: OK ({} bytes)", key, actual_data.len());
    }

    unmount_fuse()?;
    cleanup_objects(
        &ctx,
        &bucket,
        &test_files.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
    )
    .await;

    println!("{}", "SUCCESS: Basic file read test passed".green());
    Ok(())
}

async fn test_directory_listing(disk_cache: bool) -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload objects with directory structure");
    let keys = vec![
        "top-level.txt",
        "docs/readme.md",
        "docs/guide.md",
        "src/main.rs",
        "src/lib.rs",
        "src/util/helper.rs",
    ];

    for key in &keys {
        let data = format!("content of {key}");
        ctx.client
            .put_object()
            .bucket(&bucket)
            .key(*key)
            .body(ByteStream::from(data.into_bytes()))
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));
    }
    println!("    Uploaded {} objects", keys.len());

    println!("  Step 2: Mount FUSE filesystem");
    mount_fuse_ro(&bucket, disk_cache)?;

    println!("  Step 3: Verify root directory listing");
    let root_entries: Vec<String> = std::fs::read_dir(MOUNT_POINT)
        .expect("Failed to list root")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    println!("    Root entries: {:?}", root_entries);

    let expected_root = vec!["top-level.txt", "docs", "src"];
    for expected in &expected_root {
        assert!(
            root_entries.contains(&expected.to_string()),
            "Missing root entry: {expected}"
        );
        println!("    Found: {}", expected);
    }

    println!("  Step 4: Verify subdirectory listing");
    let docs_path = format!("{}/docs", MOUNT_POINT);
    let docs_entries: Vec<String> = std::fs::read_dir(&docs_path)
        .expect("Failed to list docs/")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    println!("    docs/ entries: {:?}", docs_entries);

    for expected in &["readme.md", "guide.md"] {
        assert!(
            docs_entries.contains(&expected.to_string()),
            "Missing docs/ entry: {expected}"
        );
    }

    println!("  Step 5: Verify file content in subdirectory");
    let readme_path = format!("{}/docs/readme.md", MOUNT_POINT);
    let content = std::fs::read_to_string(&readme_path).expect("Failed to read docs/readme.md");
    assert_eq!(
        content, "content of docs/readme.md",
        "Content mismatch for docs/readme.md"
    );
    println!("    docs/readme.md content: OK");

    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;

    println!("{}", "SUCCESS: Directory listing test passed".green());
    Ok(())
}

async fn test_large_file_read(disk_cache: bool) -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    let sizes: Vec<(&str, usize)> = vec![
        ("small-4k", 4 * 1024),
        ("medium-512k", 512 * 1024),
        ("large-2mb", 2 * 1024 * 1024),
    ];

    println!("  Step 1: Upload large test objects");
    let mut upload_keys = Vec::new();
    for (label, size) in &sizes {
        let key = format!("large-{label}");
        let data = generate_test_data(&key, *size);
        ctx.client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from(data))
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));
        upload_keys.push(key);
        println!("    Uploaded: {} ({} bytes)", label, size);
    }

    println!("  Step 2: Mount FUSE filesystem");
    mount_fuse_ro(&bucket, disk_cache)?;

    println!("  Step 3: Read and verify large files");
    for (i, (label, size)) in sizes.iter().enumerate() {
        let key = &upload_keys[i];
        let expected_data = generate_test_data(key, *size);
        let fuse_path = format!("{}/{}", MOUNT_POINT, key);
        let actual_data =
            std::fs::read(&fuse_path).unwrap_or_else(|e| panic!("Failed to read {key}: {e}"));
        assert_eq!(actual_data, expected_data, "{label}: data mismatch");
        println!("    {}: OK ({} bytes)", label, actual_data.len());
    }

    unmount_fuse()?;
    let key_refs: Vec<&str> = upload_keys.iter().map(|k| k.as_str()).collect();
    cleanup_objects(&ctx, &bucket, &key_refs).await;

    println!("{}", "SUCCESS: Large file read test passed".green());
    Ok(())
}

async fn test_nested_directories(disk_cache: bool) -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload deeply nested objects");
    let keys = vec!["a/b/c/deep.txt", "a/b/sibling.txt", "a/top.txt"];

    for key in &keys {
        let data = format!("nested:{key}");
        ctx.client
            .put_object()
            .bucket(&bucket)
            .key(*key)
            .body(ByteStream::from(data.into_bytes()))
            .send()
            .await
            .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));
    }
    println!("    Uploaded {} objects", keys.len());

    println!("  Step 2: Mount FUSE filesystem");
    mount_fuse_ro(&bucket, disk_cache)?;

    println!("  Step 3: Verify nested directory traversal");

    let a_path = format!("{}/a", MOUNT_POINT);
    assert!(Path::new(&a_path).is_dir(), "a/ should be a directory");
    println!("    a/ is a directory: OK");

    let top_path = format!("{}/a/top.txt", MOUNT_POINT);
    let content = std::fs::read_to_string(&top_path).expect("Failed to read a/top.txt");
    assert_eq!(content, "nested:a/top.txt", "a/top.txt content mismatch");
    println!("    a/top.txt content: OK");

    let deep_path = format!("{}/a/b/c/deep.txt", MOUNT_POINT);
    let content = std::fs::read_to_string(&deep_path).expect("Failed to read a/b/c/deep.txt");
    assert_eq!(
        content, "nested:a/b/c/deep.txt",
        "a/b/c/deep.txt content mismatch"
    );
    println!("    a/b/c/deep.txt content: OK");

    let ab_path = format!("{}/a/b", MOUNT_POINT);
    let ab_entries: Vec<String> = std::fs::read_dir(&ab_path)
        .expect("Failed to list a/b/")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    println!("    a/b/ entries: {:?}", ab_entries);

    for expected in &["c", "sibling.txt"] {
        assert!(
            ab_entries.contains(&expected.to_string()),
            "Missing a/b/ entry: {expected}"
        );
    }

    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;

    println!(
        "{}",
        "SUCCESS: Nested directory structure test passed".green()
    );
    Ok(())
}

async fn test_create_write_read(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create and write files");
    let test_data = b"Hello from FUSE write!";
    let fuse_path = format!("{}/write-test.txt", MOUNT_POINT);
    std::fs::write(&fuse_path, test_data).expect("Failed to write file");
    println!("    Written: write-test.txt ({} bytes)", test_data.len());

    println!("  Step 3: Read back and verify");
    let read_back = std::fs::read(&fuse_path).expect("Failed to read back");
    assert_eq!(read_back, test_data, "write-test.txt data mismatch");
    println!("    write-test.txt content: OK");

    println!("  Step 4: Write a larger file (64KB)");
    let large_data = generate_test_data("large-write", 64 * 1024);
    let large_path = format!("{}/large-write.bin", MOUNT_POINT);
    std::fs::write(&large_path, &large_data).expect("Failed to write large file");

    let large_read = std::fs::read(&large_path).expect("Failed to read back large file");
    assert_eq!(large_read, large_data, "large-write.bin data mismatch");
    println!("    large-write.bin (64KB): OK");

    println!("  Step 5: Verify files appear in listing");
    let entries: Vec<String> = std::fs::read_dir(MOUNT_POINT)
        .expect("Failed to list root")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        entries.contains(&"write-test.txt".to_string()),
        "write-test.txt not found in listing"
    );
    println!("    write-test.txt in listing: OK");

    unmount_fuse()?;
    println!("{}", "SUCCESS: Create/write/read test passed".green());
    Ok(())
}

async fn test_large_file_write(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    let sizes: Vec<(&str, usize)> = vec![
        ("small-4k", 4 * 1024),
        ("medium-512k", 512 * 1024),
        ("large-2mb", 2 * 1024 * 1024),
    ];

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Write large files via FUSE");
    let mut keys = Vec::new();
    for (label, size) in &sizes {
        let key = format!("fuse-write-{label}");
        let data = generate_test_data(&key, *size);
        let fuse_path = format!("{}/{}", MOUNT_POINT, key);
        std::fs::write(&fuse_path, &data).unwrap_or_else(|e| panic!("Failed to write {key}: {e}"));
        keys.push((key, data));
        println!("    Written: {} ({} bytes)", label, size);
    }

    println!("  Step 3: Read back and verify");
    for (i, (label, _)) in sizes.iter().enumerate() {
        let (key, expected_data) = &keys[i];
        let fuse_path = format!("{}/{}", MOUNT_POINT, key);
        let actual_data =
            std::fs::read(&fuse_path).unwrap_or_else(|e| panic!("Failed to read {key}: {e}"));
        assert_eq!(actual_data, *expected_data, "{label}: data mismatch");
        println!("    {}: OK ({} bytes)", label, actual_data.len());
    }

    unmount_fuse()?;

    println!("{}", "SUCCESS: Large file write test passed".green());
    Ok(())
}

async fn test_mkdir_rmdir(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create directory");
    let dir_path = format!("{}/testdir", MOUNT_POINT);
    std::fs::create_dir(&dir_path).expect("Failed to mkdir");
    println!("    Created: testdir/");

    assert!(Path::new(&dir_path).is_dir(), "testdir/ is not a directory");
    println!("    testdir/ is a directory: OK");

    println!("  Step 3: Remove empty directory");
    std::fs::remove_dir(&dir_path).expect("Failed to rmdir");
    println!("    Removed: testdir/");

    assert!(
        !Path::new(&dir_path).exists(),
        "testdir/ still exists after rmdir"
    );
    println!("    testdir/ gone: OK");

    println!("  Step 4: Verify non-empty rmdir fails");
    let dir2_path = format!("{}/testdir2", MOUNT_POINT);
    std::fs::create_dir(&dir2_path).expect("Failed to mkdir testdir2");
    let file_in_dir = format!("{}/testdir2/file.txt", MOUNT_POINT);
    std::fs::write(&file_in_dir, b"content").expect("Failed to write file in dir");

    let err = std::fs::remove_dir(&dir2_path).expect_err("Non-empty rmdir should fail");
    assert_eq!(err.raw_os_error(), Some(39), "Expected ENOTEMPTY");
    println!("    Non-empty rmdir correctly returned ENOTEMPTY");

    unmount_fuse()?;
    println!("{}", "SUCCESS: Mkdir/rmdir test passed".green());
    Ok(())
}

async fn test_unlink(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create file then unlink");
    let file_path = format!("{}/to-delete.txt", MOUNT_POINT);
    std::fs::write(&file_path, b"delete me").expect("Failed to write");
    println!("    Created: to-delete.txt");

    assert!(Path::new(&file_path).exists(), "to-delete.txt should exist");

    std::fs::remove_file(&file_path).expect("Failed to unlink");
    println!("    Unlinked: to-delete.txt");

    assert!(
        !Path::new(&file_path).exists(),
        "to-delete.txt still exists after unlink"
    );
    println!("    to-delete.txt gone: OK");

    unmount_fuse()?;
    println!("{}", "SUCCESS: Unlink test passed".green());
    Ok(())
}

/// Exercise symlink create / readlink / lstat / unlink end-to-end. The
/// kernel routes `symlink(2)` -> `FUSE_SYMLINK` -> our `vfs_symlink`,
/// stores a layout with `ObjectState::Symlink`, and a subsequent
/// `readlink(2)` round-trips the original target bytes back through
/// `vfs_readlink`. `lstat` must report the link mode (S_IFLNK) and the
/// target byte count as the size.
async fn test_symlink_basic(disk_cache: bool) -> CmdResult {
    use std::os::unix::fs::FileTypeExt;
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: symlink(target, link)");
    let link_path = format!("{}/my-link", MOUNT_POINT);
    let target = "../etc/hostname";
    std::os::unix::fs::symlink(target, &link_path)
        .expect("failed to create symlink via FUSE_SYMLINK");
    println!("    Created: my-link -> {}", target);

    println!("  Step 3: readlink should round-trip the target verbatim");
    let resolved = std::fs::read_link(&link_path).expect("read_link failed");
    assert_eq!(
        resolved.to_str().expect("non-utf8 target"),
        target,
        "readlink(my-link) returned wrong target"
    );

    println!("  Step 4: lstat reports S_IFLNK and size = target.len()");
    let meta = std::fs::symlink_metadata(&link_path).expect("symlink_metadata failed");
    assert!(
        meta.file_type().is_symlink(),
        "lstat did not report a symlink: file_type = {:?}",
        meta.file_type()
    );
    assert_eq!(
        meta.len(),
        target.len() as u64,
        "symlink lstat size mismatch: expected {}, got {}",
        target.len(),
        meta.len()
    );
    // Also assert the type is NOT a regular file or block device.
    assert!(
        !meta.file_type().is_file(),
        "symlink reported as regular file"
    );
    assert!(
        !meta.file_type().is_block_device(),
        "symlink reported as block device"
    );

    println!("  Step 5: unlink the symlink (no blob to clean up)");
    std::fs::remove_file(&link_path).expect("failed to unlink symlink");
    assert!(
        std::fs::symlink_metadata(&link_path).is_err(),
        "symlink still resolves after unlink"
    );

    println!("  Step 6: same name is reusable after unlink");
    std::os::unix::fs::symlink("/tmp/another", &link_path)
        .expect("failed to recreate symlink with same name");
    let resolved2 = std::fs::read_link(&link_path).expect("read_link after recreate failed");
    assert_eq!(resolved2.to_str().unwrap(), "/tmp/another");

    println!("  Step 7: symlinks coexist with regular files in the same dir");
    let regular = format!("{}/sibling.txt", MOUNT_POINT);
    std::fs::write(&regular, b"hello").expect("regular write failed");
    let regular_meta = std::fs::metadata(&regular).expect("metadata regular");
    assert!(regular_meta.file_type().is_file());
    assert_eq!(regular_meta.len(), 5);

    // Cleanup
    let _ = std::fs::remove_file(&link_path);
    let _ = std::fs::remove_file(&regular);

    unmount_fuse()?;
    println!("{}", "SUCCESS: symlink basic test passed".green());
    Ok(())
}

/// Exercise the writeback-default-mode path end-to-end. With
/// `writeback_mode = "default"`, vfs_symlink enqueues the
/// `InodeIntent::PutInode` instead of firing NSS synchronously; the
/// background worker drains the queue ~50ms later. The test:
///   1. Mounts FUSE in writeback default mode.
///   2. Creates a symlink (returns immediately to the kernel).
///   3. Verifies the local view (readlink) sees it instantly.
///   4. Polls until the kernel-side dir listing also shows it; this
///      proves the worker actually committed via NSS, not just the
///      local InodeTable.
///   5. Unmounts to flush the queue at destroy.
///   6. Re-mounts in *strict* mode and confirms the symlink is durable
///      (readlink succeeds against a freshly-spawned fs_server with no
///      InodeTable cache holdover).
async fn test_writeback_default_mode_symlink(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in writeback default mode");
    mount_fuse_writeback(&bucket, true, disk_cache)?;

    println!("  Step 2: Create a symlink via FUSE_SYMLINK");
    let link_path = format!("{}/wb-symlink", MOUNT_POINT);
    let target = "../etc/wb-target";
    std::os::unix::fs::symlink(target, &link_path).expect("FUSE_SYMLINK failed");
    println!("    enqueued: wb-symlink -> {}", target);

    println!("  Step 3: Local view must surface the symlink immediately");
    let resolved = std::fs::read_link(&link_path).expect("local readlink failed");
    assert_eq!(
        resolved.to_str().unwrap(),
        target,
        "local readlink saw stale target"
    );

    println!("  Step 4: Wait for the worker to commit to NSS (poll up to 5s)");
    let mount_point = MOUNT_POINT;
    let mut committed = false;
    for _ in 0..50 {
        std::thread::sleep(Duration::from_millis(100));
        // dir listing goes through the FUSE -> vfs_readdir -> NSS
        // ListInodes path; hitting a freshly-listed entry proves NSS
        // persisted the write.
        let entries: Vec<_> = std::fs::read_dir(mount_point)
            .expect("readdir failed")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        if entries.iter().any(|n| n == "wb-symlink") {
            committed = true;
            break;
        }
    }
    assert!(
        committed,
        "worker failed to commit symlink within 5s; default-mode pipeline broken"
    );
    println!("    NSS commit observed via dir listing");

    println!("  Step 5: Unmount (drains residual queue, blocks new enqueues)");
    unmount_fuse()?;

    println!("  Step 6: Re-mount; symlink must be durable");
    mount_fuse_rw(&bucket, disk_cache)?;
    let resolved2 = std::fs::read_link(&link_path).expect("post-remount readlink failed");
    assert_eq!(resolved2.to_str().unwrap(), target);

    let _ = std::fs::remove_file(&link_path);
    unmount_fuse()?;

    println!(
        "{}",
        "SUCCESS: writeback default mode symlink path passed".green()
    );
    Ok(())
}

/// Regression for the P0 where a write after creating a hardlink was
/// silently discarded: hardlink promotion sets `inode_id`, and the flush
/// then skipped publish entirely, never touching the shared blob or the
/// `#hardlink/<id>` InodeRecord. Writes must instead flush to the record
/// (record-aware CAS path) so both names observe the new bytes, before
/// and after a remount.
async fn test_hardlink_write_visible(disk_cache: bool) -> CmdResult {
    use std::os::unix::fs::MetadataExt;

    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE rw, create hl-a with initial bytes");
    mount_fuse_rw(&bucket, disk_cache)?;
    let a_path = format!("{}/hl-a", MOUNT_POINT);
    let b_path = format!("{}/hl-b", MOUNT_POINT);
    std::fs::write(&a_path, b"AAAAAAAA").expect("write hl-a failed");

    println!("  Step 2: hard_link(hl-a, hl-b) promotes the inode (nlink=2)");
    std::fs::hard_link(&a_path, &b_path).expect("hard_link failed");
    let nlink = std::fs::metadata(&b_path)
        .expect("stat hl-b failed")
        .nlink();
    assert_eq!(nlink, 2, "hardlink nlink should be 2, got {nlink}");

    println!("  Step 3: Write new bytes through hl-b, then fsync");
    {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&b_path)
            .expect("open hl-b for write failed");
        use std::io::Write;
        let mut f = f;
        f.write_all(b"ZZZZZZZZZZZZZZZZ").expect("write hl-b failed");
        f.sync_all().expect("fsync hl-b failed");
    }
    let want = b"ZZZZZZZZZZZZZZZZ".to_vec();

    println!("  Step 4: Both names must observe the new bytes pre-remount");
    let a_live = std::fs::read(&a_path).expect("read hl-a failed");
    let b_live = std::fs::read(&b_path).expect("read hl-b failed");
    assert_eq!(a_live, want, "hl-a stale before remount (write discarded)");
    assert_eq!(b_live, want, "hl-b stale before remount");

    println!("  Step 5: Remount; the write must be durable on both names");
    unmount_fuse()?;
    mount_fuse_rw(&bucket, disk_cache)?;
    let a_cold = std::fs::read(&a_path).expect("post-remount read hl-a failed");
    let b_cold = std::fs::read(&b_path).expect("post-remount read hl-b failed");
    assert_eq!(
        a_cold, want,
        "hl-a stale after remount (P0: old bytes remained)"
    );
    assert_eq!(b_cold, want, "hl-b stale after remount");

    let _ = std::fs::remove_file(&a_path);
    let _ = std::fs::remove_file(&b_path);
    unmount_fuse()?;

    println!(
        "{}",
        "SUCCESS: write after hardlink is durable on both names".green()
    );
    Ok(())
}

/// Cold-cache variant: after a remount the inode cache is empty, so an
/// enumeration (readdirplus) can cache the raw `Indirect` redirect with
/// no `inode_id`. A write must still resolve the redirect to the shared
/// record (persisting the resolved identity) rather than CAS a Normal
/// layout over the redirect. Exercises the P0 cold path that the
/// warm-cache test cannot.
async fn test_hardlink_write_cold_indirect(disk_cache: bool) -> CmdResult {
    use std::os::unix::fs::MetadataExt;

    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Create hl-c, link hl-d, commit, then remount cold");
    mount_fuse_rw(&bucket, disk_cache)?;
    let c_path = format!("{}/hl-c", MOUNT_POINT);
    let d_path = format!("{}/hl-d", MOUNT_POINT);
    std::fs::write(&c_path, b"AAAAAAAA").expect("write hl-c failed");
    std::fs::hard_link(&c_path, &d_path).expect("hard_link failed");
    unmount_fuse()?;
    mount_fuse_rw(&bucket, disk_cache)?;

    // Step 2: enumerate on the fresh mount. FUSE_READDIRPLUS_AUTO makes the
    // kernel use readdirplus for this first pass, which must resolve the
    // hardlink (Indirect) entries, both to size their attrs (an
    // unresolved redirect has no size, which previously EINVAL'd the whole
    // `ls`) and to report the shared record's true link count (nlink=2, not
    // the redirect's default 1). Enumerating + resolving here also caches
    // the inode_id, so the write below takes vfs_open's warm resolve path;
    // the pure-Indirect vfs_open branch is a defensive stateless/NFS path
    // not reachable once a lookup or readdirplus has run.
    println!("  Step 2: Enumerate (readdirplus); attrs resolve to the record");
    let (mut saw_c, mut saw_d) = (false, false);
    for e in std::fs::read_dir(MOUNT_POINT)
        .expect("readdir on hardlink dir failed (readdirplus EINVAL on Indirect?)")
    {
        let e = e.expect("dir entry failed");
        let name = e.file_name().to_string_lossy().into_owned();
        if name == "hl-c" || name == "hl-d" {
            let nlink = e.metadata().expect("entry metadata failed").nlink();
            assert_eq!(
                nlink, 2,
                "{name}: hardlink attr nlink should be 2, got {nlink}"
            );
            if name == "hl-c" {
                saw_c = true;
            } else {
                saw_d = true;
            }
        }
    }
    assert!(saw_c && saw_d, "both hardlink names must enumerate");

    println!("  Step 3: Write via hl-d on the cold cache, then fsync");
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&d_path)
            .expect("open hl-d for write failed");
        f.write_all(b"YYYYYYYYYYYY").expect("write hl-d failed");
        f.sync_all().expect("fsync hl-d failed");
    }
    let want = b"YYYYYYYYYYYY".to_vec();

    // The written name (hl-d) reflects the new bytes immediately. We do not
    // assert the *other* name (hl-c) here: Step 2's readdirplus cached its
    // attr at the pre-write size, and the two hardlink names are distinct
    // FUSE inodes with independent kernel attr caches, so hl-c's size stays
    // cached-stale until its TTL lapses. That cross-alias attr-cache lag is
    // a separate, pre-existing limitation; the P0 guarantee under test is
    // that the cold write reached the shared record (not a clobbered
    // redirect), which the post-remount cross-name check below proves.
    println!("  Step 4: The written name sees the new bytes immediately");
    assert_eq!(
        std::fs::read(&d_path).expect("read hl-d failed"),
        want,
        "hl-d stale right after its own write"
    );

    // Step 5 is the real P0 guard: had the cold write taken the wrong
    // (s3_key) path it would have CAS'd a Normal layout over hl-c's
    // redirect, so after a remount hl-c and hl-d would diverge (one the
    // clobbered standalone file, the other the untouched record). Both
    // resolving to the new bytes proves the redirect survived and the
    // shared record carries the write.
    println!("  Step 5: Remount; both links must still resolve to the new bytes");
    unmount_fuse()?;
    mount_fuse_rw(&bucket, disk_cache)?;
    assert_eq!(
        std::fs::read(&c_path).expect("post-remount read hl-c failed"),
        want,
        "hl-c stale after remount (redirect clobbered / write lost)"
    );
    assert_eq!(
        std::fs::read(&d_path).expect("post-remount read hl-d failed"),
        want,
        "hl-d stale after remount"
    );

    let _ = std::fs::remove_file(&c_path);
    let _ = std::fs::remove_file(&d_path);
    unmount_fuse()?;

    println!(
        "{}",
        "SUCCESS: cold-Indirect write resolves to the shared record".green()
    );
    Ok(())
}

/// Cross-alias metadata/data merge: a chmod via one hardlink name and a
/// write via the other must BOTH survive. Regression for the record-CAS
/// read-modify-write applying a stale whole-layout, which would either
/// revert the chmod's mode (write flush rebuilt from the pre-chmod posix
/// snapshot) or revert the write's size/version (setattr restored the
/// pre-write cached layout).
async fn test_hardlink_chmod_then_write(disk_cache: bool) -> CmdResult {
    use std::os::unix::fs::PermissionsExt;

    let (_ctx, bucket) = setup_test_bucket().await;
    mount_fuse_rw(&bucket, disk_cache)?;
    let e_path = format!("{}/hl-e", MOUNT_POINT);
    let f_path = format!("{}/hl-f", MOUNT_POINT);

    println!("  Step 1: create hl-e (mode 0644), link hl-f");
    std::fs::write(&e_path, b"AAAAAAAA").expect("write hl-e failed");
    std::fs::set_permissions(&e_path, std::fs::Permissions::from_mode(0o644))
        .expect("chmod 0644 failed");
    std::fs::hard_link(&e_path, &f_path).expect("hard_link failed");

    println!("  Step 2: chmod hl-e -> 0600, then truncating write+fsync via hl-f");
    std::fs::set_permissions(&e_path, std::fs::Permissions::from_mode(0o600))
        .expect("chmod 0600 failed");
    {
        use std::io::Write;
        let mut wf = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&f_path)
            .expect("open hl-f for write failed");
        wf.write_all(b"ZZZZZZZZZZZZ").expect("write hl-f failed");
        wf.sync_all().expect("fsync hl-f failed");
    }
    let want = b"ZZZZZZZZZZZZ".to_vec();

    println!("  Step 3: remount; content AND mode must be correct on both names");
    unmount_fuse()?;
    mount_fuse_rw(&bucket, disk_cache)?;
    for p in [e_path.as_str(), f_path.as_str()] {
        assert_eq!(
            std::fs::read(p).expect("post-remount read failed"),
            want,
            "{p}: content wrong (write lost, or chmod reverted size/version)"
        );
        let mode = std::fs::metadata(p)
            .expect("post-remount stat failed")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            mode, 0o600,
            "{p}: mode wrong (chmod undone by the write flush)"
        );
    }

    let _ = std::fs::remove_file(&e_path);
    let _ = std::fs::remove_file(&f_path);
    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: cross-alias chmod + write both survive".green()
    );
    Ok(())
}

async fn test_rename(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create file and rename");
    let src_path = format!("{}/original.txt", MOUNT_POINT);
    let dst_path = format!("{}/renamed.txt", MOUNT_POINT);
    let content = b"rename me";
    std::fs::write(&src_path, content).expect("Failed to write");
    println!("    Created: original.txt");

    std::fs::rename(&src_path, &dst_path).expect("Failed to rename");
    println!("    Renamed: original.txt -> renamed.txt");

    assert!(
        !Path::new(&src_path).exists(),
        "original.txt still exists after rename"
    );
    println!("    original.txt gone: OK");

    let read_back = std::fs::read(&dst_path).expect("Failed to read renamed file");
    assert_eq!(read_back, content, "renamed.txt content mismatch");
    println!("    renamed.txt content: OK");

    unmount_fuse()?;
    println!("{}", "SUCCESS: Rename test passed".green());
    Ok(())
}

async fn test_unlink_open_handle(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create file and open a read handle");
    let file_path = format!("{}/open-del.txt", MOUNT_POINT);
    let content = b"still readable after unlink";
    std::fs::write(&file_path, content).expect("Failed to write");
    println!("    Created: open-del.txt");

    let mut file = std::fs::File::open(&file_path).expect("Failed to open");
    println!("    Opened read handle");

    println!("  Step 3: Unlink while handle is open");
    std::fs::remove_file(&file_path).expect("Failed to unlink");
    println!("    Unlinked: open-del.txt");

    assert!(
        !Path::new(&file_path).exists(),
        "open-del.txt still exists after unlink"
    );
    println!("    Path is gone (ENOENT): OK");

    println!("  Step 4: Read from open handle after unlink");
    use std::io::Read;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .expect("Failed to read from open handle");
    assert_eq!(
        buf,
        content,
        "Content mismatch from open handle: expected {} bytes, got {}",
        content.len(),
        buf.len()
    );
    println!("    Read from open handle: OK ({} bytes)", buf.len());

    drop(file);
    println!("    Closed handle (deferred cleanup triggered)");

    unmount_fuse()?;
    println!("{}", "SUCCESS: Unlink with open handle test passed".green());
    Ok(())
}

async fn test_overwrite_existing(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create original file");
    let file_path = format!("{}/overwrite.txt", MOUNT_POINT);
    let original = b"0123456789";
    std::fs::write(&file_path, original).expect("Failed to write original");
    println!("    Created: overwrite.txt (10 bytes)");

    println!("  Step 3: Partial overwrite at offset 3");
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&file_path)
            .expect("Failed to open for write");
        file.seek(SeekFrom::Start(3)).expect("Failed to seek");
        file.write_all(b"XYZ").expect("Failed to write");
        file.flush().expect("Failed to flush");
    }
    println!("    Wrote 'XYZ' at offset 3");

    println!("  Step 4: Verify merged content");
    let result = std::fs::read(&file_path).expect("Failed to read back");
    let expected = b"012XYZ6789";
    assert_eq!(
        result,
        expected,
        "Content mismatch: expected {:?}, got {:?}",
        String::from_utf8_lossy(expected),
        String::from_utf8_lossy(&result)
    );
    println!(
        "    Content after overwrite: OK ({:?})",
        String::from_utf8_lossy(&result)
    );

    unmount_fuse()?;
    println!("{}", "SUCCESS: Overwrite existing file test passed".green());
    Ok(())
}

async fn test_rename_atomic_replace(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create source and destination files");
    let src_path = format!("{}/rename-src.txt", MOUNT_POINT);
    let dst_path = format!("{}/rename-dst.txt", MOUNT_POINT);
    std::fs::write(&src_path, b"source content").expect("Failed to write src");
    std::fs::write(&dst_path, b"destination content").expect("Failed to write dst");
    println!("    Created: rename-src.txt and rename-dst.txt");

    println!("  Step 3: rename(2) over an existing dst atomically replaces it");
    std::fs::rename(&src_path, &dst_path).expect("rename(src, dst) should succeed");
    println!("    Rename succeeded");

    println!("  Step 4: Verify src is gone and dst now has source content");
    let src_err = std::fs::read(&src_path).expect_err("src should no longer exist");
    assert_eq!(
        src_err.raw_os_error(),
        Some(libc::ENOENT),
        "Expected ENOENT"
    );
    let dst_data = std::fs::read(&dst_path).expect("dst should still exist");
    assert_eq!(
        dst_data, b"source content",
        "Destination should now hold source's content"
    );
    println!("    src: ENOENT, dst: holds source bytes");

    unmount_fuse()?;
    println!("{}", "SUCCESS: Rename atomic-replace test passed".green());
    Ok(())
}

async fn test_truncate_write(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create original file");
    let file_path = format!("{}/trunc.txt", MOUNT_POINT);
    std::fs::write(&file_path, b"original long content here").expect("Failed to write original");
    println!("    Created: trunc.txt (26 bytes)");

    println!("  Step 3: Overwrite with O_TRUNC (shorter content)");
    std::fs::write(&file_path, b"short").expect("Failed to truncate-write");
    println!("    Wrote 'short' with O_TRUNC");

    println!("  Step 4: Verify truncated content");
    let result = std::fs::read(&file_path).expect("Failed to read back");
    assert_eq!(result, b"short", "Content mismatch after truncate");
    println!(
        "    Content after truncate: OK ({:?})",
        String::from_utf8_lossy(&result)
    );

    unmount_fuse()?;
    println!("{}", "SUCCESS: Truncate write test passed".green());
    Ok(())
}

async fn test_write_in_subdirectory(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create subdirectory");
    let dir_path = format!("{}/subdir", MOUNT_POINT);
    std::fs::create_dir(&dir_path).expect("Failed to mkdir");
    println!("    Created: subdir/");

    println!("  Step 3: Write files into subdirectory");
    let file1 = format!("{}/subdir/file1.txt", MOUNT_POINT);
    let file2 = format!("{}/subdir/file2.txt", MOUNT_POINT);
    std::fs::write(&file1, b"content one").expect("Failed to write file1");
    std::fs::write(&file2, b"content two").expect("Failed to write file2");
    println!("    Written: subdir/file1.txt and subdir/file2.txt");

    println!("  Step 4: Read back and verify");
    let data1 = std::fs::read(&file1).expect("Failed to read file1");
    let data2 = std::fs::read(&file2).expect("Failed to read file2");
    assert_eq!(data1, b"content one", "file1 content mismatch");
    assert_eq!(data2, b"content two", "file2 content mismatch");
    println!("    Content verified: OK");

    println!("  Step 5: Remount and verify subdirectory listing");
    unmount_fuse()?;
    mount_fuse_rw(&bucket, disk_cache)?;
    let entries: Vec<String> = std::fs::read_dir(&dir_path)
        .expect("Failed to list subdir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    println!("    Listing: {:?}", entries);
    for expected in &["file1.txt", "file2.txt"] {
        assert!(
            entries.contains(&expected.to_string()),
            "Missing entry in subdir listing: {expected}"
        );
    }

    unmount_fuse()?;
    println!("{}", "SUCCESS: Write in subdirectory test passed".green());
    Ok(())
}

async fn test_rename_directory(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create directory with files");
    let src_dir = format!("{}/srcdir", MOUNT_POINT);
    std::fs::create_dir(&src_dir).expect("Failed to mkdir srcdir");
    let child_file = format!("{}/srcdir/child.txt", MOUNT_POINT);
    std::fs::write(&child_file, b"child content").expect("Failed to write child");
    println!("    Created: srcdir/child.txt");

    println!("  Step 3: Rename directory");
    let dst_dir = format!("{}/dstdir", MOUNT_POINT);
    std::fs::rename(&src_dir, &dst_dir).expect("Failed to rename dir");
    println!("    Renamed: srcdir/ -> dstdir/");

    assert!(
        !Path::new(&src_dir).exists(),
        "srcdir still exists after rename"
    );
    println!("    srcdir/ gone: OK");

    println!("  Step 4: Verify child at new path");
    let new_child = format!("{}/dstdir/child.txt", MOUNT_POINT);
    let data = std::fs::read(&new_child).expect("Failed to read dstdir/child.txt");
    assert_eq!(data, b"child content", "dstdir/child.txt content mismatch");
    println!("    dstdir/child.txt content: OK");

    unmount_fuse()?;
    println!("{}", "SUCCESS: Rename directory test passed".green());
    Ok(())
}

/// Test dd-style buffered write + fsync exercises the writeback cache path.
async fn test_dd_fsync(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: dd 400KB of zeros with conv=fsync");
    let dd_path = format!("{}/dd-test", MOUNT_POINT);
    run_cmd!(dd if=/dev/zero of=$dd_path bs=4096 count=100 conv=fsync 2>&1)?;

    println!("  Step 3: Verify file size");
    let meta = std::fs::metadata(&dd_path).expect("Failed to stat dd-test");
    assert_eq!(meta.len(), 409600, "dd-test size mismatch");
    println!("    dd-test size: OK (409600 bytes)");

    println!("  Step 4: Verify all bytes are zero");
    let data = std::fs::read(&dd_path).expect("Failed to read dd-test");
    assert!(
        data.iter().all(|&b| b == 0),
        "dd-test contains non-zero bytes"
    );
    println!("    dd-test content: OK (all zeros)");

    println!("  Step 5: Remount and verify persistence");
    unmount_fuse()?;
    mount_fuse_rw(&bucket, disk_cache)?;

    let persisted = std::fs::metadata(&dd_path).expect("dd-test gone after remount");
    assert_eq!(
        persisted.len(),
        409600,
        "dd-test size after remount mismatch"
    );
    let persisted_data = std::fs::read(&dd_path).expect("Failed to read dd-test after remount");
    assert!(
        persisted_data.iter().all(|&b| b == 0),
        "dd-test contains non-zero bytes after remount"
    );
    println!("    dd-test after remount: OK (409600 bytes, all zeros)");

    println!("  Step 6: dd with urandom pattern");
    let urandom_path = format!("{}/dd-urandom", MOUNT_POINT);
    run_cmd!(dd if=/dev/urandom of=$urandom_path bs=4096 count=10 conv=fsync 2>&1)?;

    let urandom_data = std::fs::read(&urandom_path).expect("Failed to read dd-urandom");
    assert_eq!(urandom_data.len(), 40960, "dd-urandom size mismatch");
    println!("    dd-urandom size: OK (40960 bytes)");

    unmount_fuse()?;
    println!("{}", "SUCCESS: dd + fsync write test passed".green());
    Ok(())
}

/// Test mmap write via libc exercises the writeback cache mmap path.
async fn test_mmap_write(disk_cache: bool) -> CmdResult {
    use std::os::unix::io::AsRawFd;

    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create file with known content (4096 bytes of 'A')");
    let file_path = format!("{}/mmap-test.bin", MOUNT_POINT);
    let size: usize = 4096;
    let original = vec![b'A'; size];
    std::fs::write(&file_path, &original).expect("Failed to write mmap-test.bin");
    println!("    Created: mmap-test.bin ({} bytes)", size);

    println!("  Step 3: mmap the file and modify bytes");
    {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("Failed to open for mmap");
        let fd = file.as_raw_fd();

        unsafe {
            let ptr = libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            );
            assert_ne!(
                ptr,
                libc::MAP_FAILED,
                "mmap failed: {}",
                std::io::Error::last_os_error()
            );

            // Write 'X' at offsets 0, 100, 1000, 4095
            let slice = std::slice::from_raw_parts_mut(ptr as *mut u8, size);
            slice[0] = b'X';
            slice[100] = b'X';
            slice[1000] = b'X';
            slice[4095] = b'X';

            let ret = libc::msync(ptr, size, libc::MS_SYNC);
            assert_eq!(ret, 0, "msync failed: {}", std::io::Error::last_os_error());
            println!("    msync: OK");

            libc::munmap(ptr, size);
        }
    }
    println!("    mmap write + msync + munmap: OK");

    println!("  Step 4: Read back and verify modifications");
    let readback = std::fs::read(&file_path).expect("Failed to read back mmap-test.bin");
    assert_eq!(readback.len(), size, "mmap-test.bin size mismatch");

    let modified_offsets = [0, 100, 1000, 4095];
    for &offset in &modified_offsets {
        assert_eq!(
            readback[offset], b'X',
            "mmap-test.bin[{}]: expected 'X' (0x58), got 0x{:02x}",
            offset, readback[offset]
        );
    }
    // Verify unmodified bytes are still 'A'
    for (i, &byte) in readback.iter().enumerate() {
        if !modified_offsets.contains(&i) {
            assert_eq!(
                byte, b'A',
                "mmap-test.bin[{}]: expected 'A' (0x41), got 0x{:02x}",
                i, byte
            );
        }
    }
    println!("    Readback verified: 4 bytes modified, rest unchanged");

    println!("  Step 5: Remount and verify persistence");
    unmount_fuse()?;
    mount_fuse_rw(&bucket, disk_cache)?;

    let persisted = std::fs::read(&file_path).expect("Failed to read after remount");
    assert_eq!(
        persisted.len(),
        size,
        "mmap-test.bin size after remount mismatch"
    );
    for &offset in &modified_offsets {
        assert_eq!(
            persisted[offset], b'X',
            "mmap-test.bin[{}] after remount: expected 'X', got 0x{:02x}",
            offset, persisted[offset]
        );
    }
    println!("    Post-remount: OK (modifications persisted)");

    unmount_fuse()?;
    println!("{}", "SUCCESS: mmap write test passed".green());
    Ok(())
}

/// Test that fsync flushes data to the backend so it survives a remount.
async fn test_fsync_persistence(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Write file and fsync");
    let file_path = format!("{}/fsync-test.txt", MOUNT_POINT);
    let content = b"fsync persisted data";
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)
            .expect("Failed to create file");
        file.write_all(content).expect("Failed to write");
        file.sync_all().expect("Failed to fsync");
        println!(
            "    Written and fsynced: fsync-test.txt ({} bytes)",
            content.len()
        );
    }

    println!("  Step 3: Verify file is readable before remount");
    let read_back = std::fs::read(&file_path).expect("Failed to read before remount");
    assert_eq!(read_back, content, "Pre-remount content mismatch");
    println!("    Pre-remount read: OK");

    println!("  Step 4: Remount and verify data persisted");
    unmount_fuse()?;
    mount_fuse_rw(&bucket, disk_cache)?;

    let persisted = std::fs::read(&file_path).expect("Failed to read after remount");
    assert_eq!(persisted, content, "Post-remount content mismatch");
    println!("    Post-remount read: OK ({} bytes)", persisted.len());

    println!("  Step 5: Test sync_data (fdatasync)");
    let file_path2 = format!("{}/fdatasync-test.txt", MOUNT_POINT);
    let content2 = b"fdatasync persisted data";
    {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path2)
            .expect("Failed to create file2");
        file.write_all(content2).expect("Failed to write file2");
        file.sync_data().expect("Failed to fdatasync");
        println!("    Written and fdatasynced: fdatasync-test.txt");
    }

    unmount_fuse()?;
    mount_fuse_rw(&bucket, disk_cache)?;

    let persisted2 =
        std::fs::read(&file_path2).expect("Failed to read fdatasync file after remount");
    assert_eq!(
        persisted2, content2,
        "fdatasync post-remount content mismatch"
    );
    println!("    fdatasync post-remount: OK");

    unmount_fuse()?;
    println!("{}", "SUCCESS: Fsync persistence test passed".green());
    Ok(())
}

// ── Cache staleness tests ───────────────────────────────────────────
//
// These tests verify that FUSE sees mutations made externally via the
// S3 API (bypassing FUSE). With TTL-based caching (FUSE entry TTL=1s,
// DirCache TTL=5s), we must wait for the cache to expire before the
// kernel re-issues LOOKUP/readdir. This is the expected behavior for
// TTL-based cache invalidation.

/// Time to wait for FUSE entry TTL + DirCache TTL to expire.
const CACHE_TTL_WAIT: Duration = Duration::from_secs(7);

/// Test that a file created externally via S3 becomes visible through FUSE.
async fn test_external_create_visibility(disk_cache: bool) -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-only mode");
    mount_fuse_ro(&bucket, disk_cache)?;

    println!("  Step 2: List root to populate DirCache");
    let entries_before: Vec<String> = std::fs::read_dir(MOUNT_POINT)
        .expect("Failed to list root")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    println!("    Root entries before: {:?}", entries_before);

    println!("  Step 3: Create file externally via S3 API");
    let key = "ext-created.txt";
    let data = b"created externally";
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data.to_vec()))
        .send()
        .await
        .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));
    println!("    Uploaded: {key}");

    println!("  Step 4: Wait for cache TTL to expire");
    std::thread::sleep(CACHE_TTL_WAIT);

    println!("  Step 5: Verify file is now visible through FUSE");
    let fuse_path = format!("{}/{}", MOUNT_POINT, key);
    let content =
        std::fs::read(&fuse_path).unwrap_or_else(|e| panic!("Failed to read {key} via FUSE: {e}"));
    assert_eq!(content, data, "{key}: content mismatch");
    println!("    {key} visible and content matches: OK");

    println!("  Step 6: Verify it appears in directory listing");
    let entries_after: Vec<String> = std::fs::read_dir(MOUNT_POINT)
        .expect("Failed to list root")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        entries_after.contains(&key.to_string()),
        "{key} not in directory listing: {:?}",
        entries_after
    );
    println!("    {key} in directory listing: OK");

    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &[key]).await;

    println!(
        "{}",
        "SUCCESS: External create visibility test passed".green()
    );
    Ok(())
}

/// Test that an externally overwritten file's new content is visible through FUSE.
async fn test_external_overwrite_visibility(disk_cache: bool) -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload initial file via S3 API");
    let key = "ext-overwrite.txt";
    let original = b"original content";
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(original.to_vec()))
        .send()
        .await
        .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));

    println!("  Step 2: Mount FUSE and read the file (cache it)");
    mount_fuse_ro(&bucket, disk_cache)?;

    let fuse_path = format!("{}/{}", MOUNT_POINT, key);
    let content = std::fs::read(&fuse_path).expect("Failed to read original");
    assert_eq!(content, original, "Original content mismatch");
    println!("    Original read: OK ({} bytes)", content.len());

    println!("  Step 3: Overwrite file externally via S3 API");
    let updated = b"updated content after overwrite";
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(updated.to_vec()))
        .send()
        .await
        .unwrap_or_else(|e| panic!("Failed to overwrite {key}: {e}"));
    println!("    Overwritten via S3 API");

    println!("  Step 4: Wait for cache TTL to expire");
    std::thread::sleep(CACHE_TTL_WAIT);

    println!("  Step 5: Verify updated content is visible through FUSE");
    let new_content = std::fs::read(&fuse_path).expect("Failed to read after overwrite");
    assert_eq!(new_content, updated, "Overwritten content mismatch");
    println!(
        "    Updated content visible: OK ({} bytes)",
        new_content.len()
    );

    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &[key]).await;

    println!(
        "{}",
        "SUCCESS: External overwrite visibility test passed".green()
    );
    Ok(())
}

/// Test that a file deleted externally via S3 becomes invisible through FUSE.
async fn test_external_delete_visibility(disk_cache: bool) -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload file via S3 API");
    let key = "ext-delete.txt";
    let data = b"to be deleted externally";
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data.to_vec()))
        .send()
        .await
        .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));

    println!("  Step 2: Mount FUSE and verify file exists");
    mount_fuse_ro(&bucket, disk_cache)?;

    let fuse_path = format!("{}/{}", MOUNT_POINT, key);
    let content = std::fs::read(&fuse_path).expect("Failed to read file");
    assert_eq!(content, data, "Initial content mismatch");
    println!("    File readable: OK");

    println!("  Step 3: Delete file externally via S3 API");
    ctx.client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap_or_else(|e| panic!("Failed to delete {key}: {e}"));
    println!("    Deleted via S3 API");

    println!("  Step 4: Wait for cache TTL to expire");
    std::thread::sleep(CACHE_TTL_WAIT);

    println!("  Step 5: Verify file is gone from directory listing");
    let entries: Vec<String> = std::fs::read_dir(MOUNT_POINT)
        .expect("Failed to list root")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        !entries.contains(&key.to_string()),
        "{key} still in directory listing after delete: {:?}",
        entries
    );
    println!("    {key} gone from listing: OK");

    println!("  Step 6: Verify direct access returns ENOENT");
    let err = std::fs::read(&fuse_path).expect_err("Read should fail after external delete");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::NotFound,
        "Expected NotFound, got: {err}"
    );
    println!("    Direct access returns ENOENT: OK");

    unmount_fuse()?;

    println!(
        "{}",
        "SUCCESS: External delete visibility test passed".green()
    );
    Ok(())
}

/// Test that a file renamed externally via S3 (delete old + create new)
/// becomes visible under the new name through FUSE.
async fn test_external_rename_visibility(disk_cache: bool) -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload file via S3 API");
    let old_key = "ext-rename-old.txt";
    let new_key = "ext-rename-new.txt";
    let data = b"content that gets renamed";
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(old_key)
        .body(ByteStream::from(data.to_vec()))
        .send()
        .await
        .unwrap_or_else(|e| panic!("Failed to put {old_key}: {e}"));

    println!("  Step 2: Mount FUSE and read the file (cache it)");
    mount_fuse_ro(&bucket, disk_cache)?;

    let old_path = format!("{}/{}", MOUNT_POINT, old_key);
    let content = std::fs::read(&old_path).expect("Failed to read old key");
    assert_eq!(content, data, "Original content mismatch");
    println!("    Old key readable: OK");

    println!("  Step 3: Simulate rename via S3 API (copy + delete)");
    // Create new key with same content
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(new_key)
        .body(ByteStream::from(data.to_vec()))
        .send()
        .await
        .unwrap_or_else(|e| panic!("Failed to put {new_key}: {e}"));
    // Delete old key
    ctx.client
        .delete_object()
        .bucket(&bucket)
        .key(old_key)
        .send()
        .await
        .unwrap_or_else(|e| panic!("Failed to delete {old_key}: {e}"));
    println!("    Renamed via S3 API: {old_key} -> {new_key}");

    println!("  Step 4: Wait for cache TTL to expire");
    std::thread::sleep(CACHE_TTL_WAIT);

    println!("  Step 5: Verify old name is gone");
    let entries: Vec<String> = std::fs::read_dir(MOUNT_POINT)
        .expect("Failed to list root")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        !entries.contains(&old_key.to_string()),
        "{old_key} still visible after rename: {:?}",
        entries
    );
    println!("    Old name gone from listing: OK");

    println!("  Step 6: Verify new name is visible with correct content");
    let new_path = format!("{}/{}", MOUNT_POINT, new_key);
    let new_content =
        std::fs::read(&new_path).unwrap_or_else(|e| panic!("Failed to read {new_key}: {e}"));
    assert_eq!(new_content, data, "Renamed content mismatch");
    println!("    New name readable with correct content: OK");

    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &[new_key]).await;

    println!(
        "{}",
        "SUCCESS: External rename visibility test passed".green()
    );
    Ok(())
}

// ── Cross-instance cache invalidation tests ────────────────────────
//
// These tests run two fs_server FUSE instances on the same bucket.
// Instance A (systemd) uses MOUNT_POINT, instance B (direct process)
// uses MOUNT_POINT_B. Mutations on one instance should become visible
// on the other after the DirCache TTL expires.
//
// TTL-based expiry (FUSE TTL=1s, DirCache TTL=5s) is the design
// choice for cache invalidation.

/// Test that a file written via FUSE on instance A becomes visible
/// on instance B after cache expiry.
async fn test_cross_instance_write_visibility(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount instance A (read-write)");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Spawn instance B (read-only) on same bucket");
    let child_b = spawn_second_fuse(&bucket, false)?;

    println!("  Step 3: Create file on instance A");
    let key = "cross-write.txt";
    let content = b"written on instance A";
    let path_a = format!("{}/{}", MOUNT_POINT, key);
    std::fs::write(&path_a, content).expect("Failed to write on A");
    println!("    Written: {key} on instance A");

    println!("  Step 4: Wait for cache TTL to expire on instance B");
    std::thread::sleep(CACHE_TTL_WAIT);

    println!("  Step 5: Verify file visible on instance B");
    let path_b = format!("{}/{}", MOUNT_POINT_B, key);
    let read_b =
        std::fs::read(&path_b).unwrap_or_else(|e| panic!("Failed to read {key} on B: {e}"));
    assert_eq!(read_b, content, "Content mismatch on instance B");
    println!("    {key} visible on instance B: OK");

    stop_second_fuse(child_b);
    unmount_fuse()?;

    println!(
        "{}",
        "SUCCESS: Cross-instance write visibility test passed".green()
    );
    Ok(())
}

/// Test that a file renamed via FUSE on instance A is reflected on
/// instance B (old name gone, new name visible).
async fn test_cross_instance_rename_visibility(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount instance A (read-write)");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create file on instance A");
    let old_key = "cross-rename-old.txt";
    let new_key = "cross-rename-new.txt";
    let content = b"rename me across instances";
    let old_path_a = format!("{}/{}", MOUNT_POINT, old_key);
    std::fs::write(&old_path_a, content).expect("Failed to write on A");
    println!("    Created: {old_key} on instance A");

    println!("  Step 3: Spawn instance B (read-only) on same bucket");
    let child_b = spawn_second_fuse(&bucket, false)?;

    println!("  Step 4: Verify file visible on instance B before rename");
    let old_path_b = format!("{}/{}", MOUNT_POINT_B, old_key);
    let read_b = std::fs::read(&old_path_b).expect("Failed to read old key on B");
    assert_eq!(read_b, content, "Pre-rename content mismatch on B");
    println!("    {old_key} visible on B: OK");

    println!("  Step 5: Rename file on instance A");
    let new_path_a = format!("{}/{}", MOUNT_POINT, new_key);
    std::fs::rename(&old_path_a, &new_path_a).expect("Failed to rename on A");
    println!("    Renamed: {old_key} -> {new_key} on A");

    println!("  Step 6: Wait for cache TTL to expire on instance B");
    std::thread::sleep(CACHE_TTL_WAIT);

    println!("  Step 7: Verify old name gone and new name visible on B");
    let entries_b: Vec<String> = std::fs::read_dir(MOUNT_POINT_B)
        .expect("Failed to list B")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        !entries_b.contains(&old_key.to_string()),
        "{old_key} still visible on B: {:?}",
        entries_b
    );
    println!("    {old_key} gone from B: OK");

    let new_path_b = format!("{}/{}", MOUNT_POINT_B, new_key);
    let new_read_b =
        std::fs::read(&new_path_b).unwrap_or_else(|e| panic!("Failed to read {new_key} on B: {e}"));
    assert_eq!(new_read_b, content, "Renamed content mismatch on B");
    println!("    {new_key} visible on B with correct content: OK");

    stop_second_fuse(child_b);
    unmount_fuse()?;

    println!(
        "{}",
        "SUCCESS: Cross-instance rename visibility test passed".green()
    );
    Ok(())
}

/// Test that a file deleted via FUSE on instance A disappears from
/// instance B after cache expiry.
async fn test_cross_instance_delete_visibility(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount instance A (read-write)");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create file on instance A");
    let key = "cross-delete.txt";
    let content = b"delete me across instances";
    let path_a = format!("{}/{}", MOUNT_POINT, key);
    std::fs::write(&path_a, content).expect("Failed to write on A");
    println!("    Created: {key} on instance A");

    println!("  Step 3: Spawn instance B (read-only) on same bucket");
    let child_b = spawn_second_fuse(&bucket, false)?;

    println!("  Step 4: Verify file visible on instance B");
    let path_b = format!("{}/{}", MOUNT_POINT_B, key);
    let read_b = std::fs::read(&path_b).expect("Failed to read on B");
    assert_eq!(read_b, content, "Pre-delete content mismatch on B");
    println!("    {key} visible on B: OK");

    println!("  Step 5: Delete file on instance A");
    std::fs::remove_file(&path_a).expect("Failed to delete on A");
    println!("    Deleted: {key} on A");

    println!("  Step 6: Wait for cache TTL to expire on instance B");
    std::thread::sleep(CACHE_TTL_WAIT);

    println!("  Step 7: Verify file gone from instance B");
    let entries_b: Vec<String> = std::fs::read_dir(MOUNT_POINT_B)
        .expect("Failed to list B")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        !entries_b.contains(&key.to_string()),
        "{key} still in listing on B: {:?}",
        entries_b
    );
    println!("    {key} gone from B listing: OK");

    let err = std::fs::read(&path_b).expect_err("Read should fail on B after delete");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::NotFound,
        "Expected NotFound on B, got: {err}"
    );
    println!("    Direct access on B returns ENOENT: OK");

    stop_second_fuse(child_b);
    unmount_fuse()?;

    println!(
        "{}",
        "SUCCESS: Cross-instance delete visibility test passed".green()
    );
    Ok(())
}

/// Test that overwriting a file on instance A causes instance B to see the
/// new *content* (not just a new dentry). This exercises `invalidate_inode`
/// which drops the kernel page cache, ensuring stale cached file data is
/// not served after a remote overwrite.
async fn test_cross_instance_overwrite_visibility(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount instance A (read-write)");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create file on instance A with initial content");
    let key = "cross-overwrite.txt";
    let content_v1 = b"version-1: original content";
    let path_a = format!("{}/{}", MOUNT_POINT, key);
    std::fs::write(&path_a, content_v1).expect("Failed to write v1 on A");
    println!("    Written v1: {key} ({} bytes)", content_v1.len());

    println!("  Step 3: Spawn instance B (read-only) on same bucket");
    let child_b = spawn_second_fuse(&bucket, false)?;

    println!("  Step 4: Read file on instance B to cache dentry + page cache");
    let path_b = format!("{}/{}", MOUNT_POINT_B, key);
    let read_v1 =
        std::fs::read(&path_b).unwrap_or_else(|e| panic!("Failed to read {key} on B: {e}"));
    assert_eq!(read_v1, content_v1, "Initial content mismatch on B");
    println!("    B cached v1: OK");

    println!("  Step 5: Overwrite file on instance A with new content");
    let content_v2 = b"version-2: updated content with different length!";
    std::fs::write(&path_a, content_v2).expect("Failed to write v2 on A");
    println!("    Written v2: {key} ({} bytes)", content_v2.len());

    println!("  Step 6: Wait for cache invalidation on instance B");
    std::thread::sleep(CACHE_TTL_WAIT);

    println!("  Step 7: Read file on instance B - should see v2 content");
    let read_v2 = std::fs::read(&path_b)
        .unwrap_or_else(|e| panic!("Failed to read {key} on B after overwrite: {e}"));
    assert_eq!(
        read_v2,
        content_v2,
        "Instance B still sees stale content after overwrite.\n  Expected: {:?}\n  Got:      {:?}",
        String::from_utf8_lossy(content_v2),
        String::from_utf8_lossy(&read_v2)
    );
    println!("    B sees v2: OK ({} bytes)", read_v2.len());

    stop_second_fuse(child_b);
    unmount_fuse()?;

    println!(
        "{}",
        "SUCCESS: Cross-instance overwrite visibility test passed".green()
    );
    Ok(())
}

/// Regression: a directory first materialised on a second instance via a
/// delimiter listing (readdir) must report its real owner, not the uid-0
/// placeholder the common-prefix listing seeds. Before the fix nothing
/// refreshed that placeholder, so `stat` reported uid 0 and a `chmod` /
/// `utime` by the true owner got EPERM. This is the cross-instance analogue
/// of the single-mount forget+relookup case (tar's dir-metadata pass
/// failing under kernel inode-cache pressure during a large untar).
async fn test_cross_instance_dir_owner_after_listing(disk_cache: bool) -> CmdResult {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount instance A (read-write)");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: mkdir on A (owned by the mounting user) with a child");
    let dir = "owned-dir";
    let path_a = format!("{}/{}", MOUNT_POINT, dir);
    std::fs::create_dir(&path_a).expect("mkdir on A");
    // A child key makes the dir surface as a listing common-prefix on B.
    std::fs::write(format!("{path_a}/child.txt"), b"x").expect("write child on A");

    println!("  Step 3: Spawn instance B (read-write) on same bucket");
    let child_b = spawn_second_fuse(&bucket, true)?;
    std::thread::sleep(CACHE_TTL_WAIT);

    println!("  Step 4: Materialise B's entry via a directory listing (readdir)");
    let names: Vec<String> = std::fs::read_dir(MOUNT_POINT_B)
        .expect("readdir B")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        names.contains(&dir.to_string()),
        "dir not listed on B: {names:?}"
    );

    println!("  Step 5: stat on B reports the real owner, not the uid-0 placeholder");
    let uid = unsafe { libc::getuid() };
    let path_b = format!("{}/{}", MOUNT_POINT_B, dir);
    let meta = std::fs::metadata(&path_b).expect("stat B");
    assert_eq!(
        meta.uid(),
        uid,
        "listing-materialised dir on B reports placeholder owner {} (expected {uid})",
        meta.uid()
    );

    println!("  Step 6: chmod on B by the owner succeeds (no EPERM)");
    std::fs::set_permissions(&path_b, std::fs::Permissions::from_mode(0o0755))
        .expect("chmod on B must not EPERM for the real owner");

    stop_second_fuse(child_b);
    unmount_fuse()?;

    println!(
        "{}",
        "SUCCESS: Cross-instance directory owner after listing test passed".green()
    );
    Ok(())
}

// ── Disk-cache-specific integration tests ──────────────────────────

/// Test that reading files via FUSE populates the disk cache directory.
async fn test_disk_cache_populates(disk_cache: bool) -> CmdResult {
    assert!(disk_cache, "this test requires disk cache");
    let (ctx, bucket) = setup_test_bucket().await;

    // Clean disk cache directory
    let dc_path = disk_cache_path();
    let _ = std::fs::remove_dir_all(&dc_path);

    println!("  Step 1: Upload test file via S3 API");
    let key = "dc-populate.bin";
    let data = generate_test_data(key, 64 * 1024);
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));
    println!("    Uploaded: {} ({} bytes)", key, data.len());

    println!("  Step 2: Mount FUSE with disk cache");
    mount_fuse_ro(&bucket, disk_cache)?;

    println!("  Step 3: Read file to populate cache");
    let fuse_path = format!("{}/{}", MOUNT_POINT, key);
    let actual = std::fs::read(&fuse_path).expect("Failed to read via FUSE");
    assert_eq!(actual, data, "data mismatch");
    println!("    Read: OK ({} bytes)", actual.len());

    println!("  Step 4: Verify cache files exist on disk");
    let cache_files: Vec<_> = std::fs::read_dir(&dc_path)
        .expect("Failed to list disk cache dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .collect();
    assert!(
        !cache_files.is_empty(),
        "disk cache should contain files after a read"
    );
    println!("    Disk cache files: {} (expected > 0)", cache_files.len());

    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &[key]).await;

    println!(
        "{}",
        "SUCCESS: Disk cache populates on read test passed".green()
    );
    Ok(())
}

/// Test that a second read of the same file is served from disk cache.
/// Verifies by reading twice and checking the file is readable both times.
async fn test_disk_cache_hit_reread(disk_cache: bool) -> CmdResult {
    assert!(disk_cache, "this test requires disk cache");
    let (ctx, bucket) = setup_test_bucket().await;

    // Clean disk cache directory
    let dc_path = disk_cache_path();
    let _ = std::fs::remove_dir_all(&dc_path);

    println!("  Step 1: Upload test file via S3 API");
    let key = "dc-reread.bin";
    let data = generate_test_data(key, 128 * 1024);
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));

    println!("  Step 2: Mount FUSE with disk cache");
    mount_fuse_ro(&bucket, disk_cache)?;

    println!("  Step 3: First read (populates cache)");
    let fuse_path = format!("{}/{}", MOUNT_POINT, key);
    let first_read = std::fs::read(&fuse_path).expect("Failed first read");
    assert_eq!(first_read, data, "first read data mismatch");
    println!("    First read: OK ({} bytes)", first_read.len());

    println!("  Step 4: Second read (should hit cache)");
    let second_read = std::fs::read(&fuse_path).expect("Failed second read");
    assert_eq!(second_read, data, "second read data mismatch");
    println!("    Second read: OK ({} bytes)", second_read.len());

    // Verify cache directory is non-empty
    let cache_file_count = std::fs::read_dir(&dc_path)
        .expect("Failed to list disk cache dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .count();
    assert!(
        cache_file_count > 0,
        "disk cache should have files after reads"
    );
    println!("    Cache files present: {}", cache_file_count);

    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &[key]).await;

    println!(
        "{}",
        "SUCCESS: Disk cache hit on re-read test passed".green()
    );
    Ok(())
}

/// Test that remounting with an existing disk cache directory performs
/// cold-start scan and serves reads from cache.
async fn test_disk_cache_cold_start(disk_cache: bool) -> CmdResult {
    assert!(disk_cache, "this test requires disk cache");
    let (ctx, bucket) = setup_test_bucket().await;

    // Clean disk cache directory
    let dc_path = disk_cache_path();
    let _ = std::fs::remove_dir_all(&dc_path);

    println!("  Step 1: Upload test file via S3 API");
    let key = "dc-coldstart.bin";
    let data = generate_test_data(key, 64 * 1024);
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(data.clone()))
        .send()
        .await
        .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));

    println!("  Step 2: Mount, read to populate cache, then unmount");
    mount_fuse_ro(&bucket, disk_cache)?;
    let fuse_path = format!("{}/{}", MOUNT_POINT, key);
    let first_read = std::fs::read(&fuse_path).expect("Failed to read");
    assert_eq!(first_read, data, "first read data mismatch");
    println!("    Read and cached: OK ({} bytes)", first_read.len());

    // Count cache files before unmount
    let cache_count_before = std::fs::read_dir(&dc_path)
        .expect("Failed to list disk cache dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .count();
    println!("    Cache files before unmount: {}", cache_count_before);
    assert!(cache_count_before > 0, "cache should have files");

    unmount_fuse()?;

    println!("  Step 3: Remount (cold-start scan should find cached files)");
    mount_fuse_ro(&bucket, disk_cache)?;

    // Verify cache files are still on disk (not cleaned up)
    let cache_count_after = std::fs::read_dir(&dc_path)
        .expect("Failed to list disk cache dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .count();
    println!("    Cache files after remount: {}", cache_count_after);
    assert_eq!(
        cache_count_before, cache_count_after,
        "cache file count changed after remount"
    );

    println!("  Step 4: Read file again (should use cached data)");
    let second_read = std::fs::read(&fuse_path).expect("Failed to read after remount");
    assert_eq!(second_read, data, "post-remount data mismatch");
    println!("    Post-remount read: OK ({} bytes)", second_read.len());

    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &[key]).await;

    println!(
        "{}",
        "SUCCESS: Disk cache cold start after remount test passed".green()
    );
    Ok(())
}

/// Test truncating a file to non-zero sizes (shrink and extend).
async fn test_truncate_nonzero(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create a file with known content");
    let file_path = format!("{}/trunc-size.txt", MOUNT_POINT);
    let original = b"0123456789ABCDEF";
    std::fs::write(&file_path, original).expect("Failed to write");
    println!("    Created: trunc-size.txt ({} bytes)", original.len());

    println!("  Step 3: Truncate to 10 bytes (shrink)");
    {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&file_path)
            .expect("Failed to open for truncate");
        file.set_len(10).expect("Failed to set_len(10)");
    }
    let data = std::fs::read(&file_path).expect("Failed to read after shrink");
    assert_eq!(data, b"0123456789", "Shrink content mismatch");
    println!(
        "    Shrink to 10 bytes: OK ({:?})",
        String::from_utf8_lossy(&data)
    );

    println!("  Step 4: Extend to 16 bytes (zero-filled)");
    {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&file_path)
            .expect("Failed to open for extend");
        file.set_len(16).expect("Failed to set_len(16)");
    }
    let data = std::fs::read(&file_path).expect("Failed to read after extend");
    assert_eq!(data.len(), 16, "Extend length mismatch");
    assert_eq!(
        data[..10],
        b"0123456789"[..],
        "Extend corrupted existing data"
    );
    assert_eq!(data[10..], [0u8; 6], "Extended region not zero-filled");
    println!("    Extend to 16 bytes: OK (first 10 preserved, last 6 zeroed)");

    println!("  Step 5: Truncate to zero");
    {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&file_path)
            .expect("Failed to open for truncate-zero");
        file.set_len(0).expect("Failed to set_len(0)");
    }
    let data = std::fs::read(&file_path).expect("Failed to read after truncate-zero");
    assert!(
        data.is_empty(),
        "Truncate-to-zero failed: got {} bytes",
        data.len()
    );
    println!("    Truncate to 0: OK (empty)");

    println!("  Step 6: Verify truncated file persists after remount");
    let file_path2 = format!("{}/trunc-persist.txt", MOUNT_POINT);
    std::fs::write(&file_path2, b"ABCDEFGHIJKLMNOP").expect("Failed to write persist file");
    {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&file_path2)
            .expect("Failed to open persist file");
        file.set_len(8).expect("Failed to truncate persist file");
        file.sync_all().expect("Failed to fsync persist file");
    }

    unmount_fuse()?;
    mount_fuse_rw(&bucket, disk_cache)?;

    let persisted = std::fs::read(&file_path2).expect("Failed to read persist file after remount");
    assert_eq!(
        persisted,
        b"ABCDEFGH",
        "Truncate+remount mismatch: expected 'ABCDEFGH', got {:?}",
        String::from_utf8_lossy(&persisted)
    );
    println!(
        "    Truncate+fsync+remount: OK ({:?})",
        String::from_utf8_lossy(&persisted)
    );

    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: Truncate to non-zero size test passed".green()
    );
    Ok(())
}

async fn test_sparse_truncate_large(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create a small file");
    let file_path = format!("{}/sparse-trunc-large.bin", MOUNT_POINT);
    std::fs::write(&file_path, b"hello").expect("Failed to write seed file");

    println!("  Step 3: ftruncate up to 256MB (sparse extend)");
    {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&file_path)
            .expect("Failed to open for truncate-large");
        // 256 MB is far larger than what the legacy BytesMut::resize
        // path would tolerate without obvious memory pressure, but
        // small enough to keep the test fast on a constrained box.
        file.set_len(256 * 1024 * 1024)
            .expect("ftruncate(256MB) failed -- sparse buffer regressed?");
    }

    println!("  Step 4: Verify stat() reports the buffered size");
    let meta = std::fs::metadata(&file_path).expect("Failed to stat");
    assert_eq!(
        meta.len(),
        256 * 1024 * 1024,
        "stat after ftruncate should report buffered size"
    );

    println!("  Step 5: Read first 5 bytes (existing data preserved)");
    let mut head = vec![0u8; 5];
    let f = std::fs::OpenOptions::new()
        .read(true)
        .open(&file_path)
        .expect("Failed to open for read");
    use std::os::unix::fs::FileExt;
    f.read_exact_at(&mut head, 0).expect("read_at(0..5) failed");
    assert_eq!(&head, b"hello", "Original bytes should survive ftruncate");

    println!("  Step 6: Read 4KB from a hole (returns zeros)");
    let mut hole = vec![0xffu8; 4096];
    f.read_exact_at(&mut hole, 1024 * 1024)
        .expect("read_at(1MB) failed");
    assert!(
        hole.iter().all(|&b| b == 0),
        "Hole region should read as zeros"
    );

    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: Sparse O(1) truncate large test passed".green()
    );
    Ok(())
}

// The sparse buffer lazy-loads only the touched blocks on a partial-
// block edit. A small write at a high offset must not disturb the
// surrounding bytes.
async fn test_sparse_partial_overwrite(disk_cache: bool) -> CmdResult {
    use aws_sdk_s3::primitives::ByteStream;
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload an existing 64KB file via S3");
    let key = "sparse-partial.bin";
    let original = generate_test_data(key, 64 * 1024);
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(original.clone()))
        .send()
        .await
        .expect("Failed to put existing object");

    println!("  Step 2: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 3: Open the file for RDWR and overwrite 16 bytes at offset 32KB");
    let file_path = format!("{}/{}", MOUNT_POINT, key);
    let patch = b"V1-PARTIAL-WRITE";
    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("Failed to open RDWR");
        f.write_all_at(patch, 32 * 1024)
            .expect("write_all_at failed");
        f.sync_all().expect("fsync failed");
    }

    println!("  Step 4: Verify the file is unchanged outside the patched region");
    let actual = std::fs::read(&file_path).expect("Failed to read after partial write");
    assert_eq!(actual.len(), original.len(), "Length must match original");
    assert_eq!(
        &actual[..32 * 1024],
        &original[..32 * 1024],
        "Pre-patch region corrupted"
    );
    assert_eq!(
        &actual[32 * 1024..32 * 1024 + patch.len()],
        patch,
        "Patch did not land"
    );
    assert_eq!(
        &actual[32 * 1024 + patch.len()..],
        &original[32 * 1024 + patch.len()..],
        "Post-patch region corrupted"
    );

    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &[key]).await;
    println!(
        "{}",
        "SUCCESS: Sparse partial-block overwrite test passed".green()
    );
    Ok(())
}

// A same-handle read after write must observe the just-written bytes
// via the per-block merge, including reads that span the transition
// between buffered and unbuffered blocks.
async fn test_sparse_dirty_read_after_write(disk_cache: bool) -> CmdResult {
    use aws_sdk_s3::primitives::ByteStream;
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload a 32KB seed file via S3");
    let key = "sparse-dirty-read.bin";
    let original = generate_test_data(key, 32 * 1024);
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(original.clone()))
        .send()
        .await
        .expect("Failed to put seed object");

    println!("  Step 2: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 3: Write at offset 1024, then read overlapping range without close");
    let file_path = format!("{}/{}", MOUNT_POINT, key);
    use std::os::unix::fs::FileExt;
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file_path)
        .expect("Failed to open RDWR");
    let patch = b"DIRTY-READ-AFTER-WRITE-CHECK";
    f.write_all_at(patch, 1024).expect("write_all_at failed");

    let mut readback = vec![0u8; patch.len()];
    f.read_exact_at(&mut readback, 1024)
        .expect("read_exact_at failed");
    assert_eq!(
        &readback, patch,
        "Dirty-handle read should see just-written bytes"
    );

    // Read across a buffered/unbuffered boundary too.
    let mut head = vec![0u8; 1024 + patch.len() + 32];
    f.read_exact_at(&mut head, 0)
        .expect("cross-boundary read failed");
    assert_eq!(
        &head[..1024],
        &original[..1024],
        "Pre-patch region should reflect committed bytes"
    );
    assert_eq!(
        &head[1024..1024 + patch.len()],
        patch,
        "Patch region should reflect buffered bytes"
    );
    assert_eq!(
        &head[1024 + patch.len()..],
        &original[1024 + patch.len()..1024 + patch.len() + 32],
        "Post-patch region should reflect committed bytes"
    );

    drop(f);
    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &[key]).await;
    println!(
        "{}",
        "SUCCESS: Sparse dirty-handle read-after-write test passed".green()
    );
    Ok(())
}

// The inode-scoped write lock must reject a second open(O_WRONLY) on
// the same inode while the first is live. Read-only opens are
// unaffected.
async fn test_sparse_single_writer_ebusy(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Create + flush a file so it exists in NSS");
    let file_path = format!("{}/sparse-busy.bin", MOUNT_POINT);
    std::fs::write(&file_path, b"seed").expect("seed write failed");

    println!("  Step 3: Open the file for write and hold the handle");
    let f1 = std::fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .expect("Failed to open first writer");

    println!("  Step 4: Second writer open must fail with EBUSY");
    let err = std::fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .expect_err("Second writer should be rejected");
    let raw = err.raw_os_error().unwrap_or(0);
    assert_eq!(
        raw,
        libc::EBUSY,
        "Expected EBUSY, got {} ({:?})",
        raw,
        err.kind()
    );
    println!("    Got EBUSY as expected");

    println!("  Step 5: A reader open on the same inode is unaffected");
    let reader = std::fs::OpenOptions::new()
        .read(true)
        .open(&file_path)
        .expect("Read open should succeed alongside the writer");
    drop(reader);

    println!("  Step 6: Closing the first writer releases the lock");
    drop(f1);
    let f2 = std::fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .expect("Second writer should succeed after first closes");
    drop(f2);

    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: Sparse single-writer EBUSY test passed".green()
    );
    Ok(())
}

// Shrink + grow inside one buffer session. A read of the regrown
// region must return zeros, not pre-shrink committed data. Exercises
// the wb.file_size logic and the shrink-clamp on per-block intents
// inside vfs_setattr_size.
// Shrink-then-grow within the SAME handle, same session. Seeds a
// 256KB file with a recognizable pattern, shrinks to 4KB, then writes
// past the old EOF (re-grows past the originally committed size).
// POSIX: bytes between the new EOF and the re-extended position must
// read as zeros, NOT the pre-shrink data. Without the
// `eof_low_watermark` guard, the lazy-load on the re-extended write
// would resurface the pre-shrink BSS bytes and the read would see the
// original pattern instead of zeros.
async fn test_sparse_shrink_then_grow_destroys(disk_cache: bool) -> CmdResult {
    use aws_sdk_s3::primitives::ByteStream;
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Seed a 256KB file via S3 with a recognizable pattern");
    let key = "shrink-grow-destroys.bin";
    let pattern: Vec<u8> = (0..256 * 1024).map(|i| (i % 251 + 1) as u8).collect();
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(pattern.clone()))
        .send()
        .await
        .expect("seed put failed");

    println!("  Step 2: Mount FUSE rw");
    mount_fuse_rw(&bucket, disk_cache)?;

    let file_path = format!("{}/{}", MOUNT_POINT, key);

    println!(
        "  Step 3: Open RDWR, shrink to 4KB, then write at offset 200KB, all on the same handle"
    );
    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("open rdwr failed");
        f.set_len(4096).expect("set_len(4096) failed");
        let marker = b"MARKER-AT-200K";
        f.write_all_at(marker, 200 * 1024)
            .expect("write past old EOF failed");
        f.sync_all().expect("sync_all failed");
    }

    println!("  Step 4: Re-open and read [4096..200KB) -- must be zeros, NOT the seed pattern");
    let read_handle = std::fs::File::open(&file_path).expect("open for read");
    use std::os::unix::fs::FileExt;
    let mut span = vec![0xffu8; 200 * 1024 - 4096];
    read_handle
        .read_exact_at(&mut span, 4096)
        .expect("span read failed");
    assert!(
        span.iter().all(|&b| b == 0),
        "destroyed-by-shrink range must read as zeros (POSIX shrink-destroys), \
         got first non-zero at offset {} (value {})",
        span.iter().position(|&b| b != 0).unwrap_or(usize::MAX),
        span.iter().find(|&&b| b != 0).copied().unwrap_or(0)
    );

    println!("  Step 5: Verify the marker at 200KB is intact");
    let mut marker_back = [0u8; 14];
    read_handle
        .read_exact_at(&mut marker_back, 200 * 1024)
        .expect("marker read failed");
    assert_eq!(&marker_back, b"MARKER-AT-200K");
    drop(read_handle);

    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &[key]).await;
    println!(
        "{}",
        "SUCCESS: Sparse shrink-then-grow destroys pre-shrink bytes test passed".green()
    );
    Ok(())
}

async fn test_sparse_truncate_then_extend(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 2: Write a small file, then ftruncate up");
    let file_path = format!("{}/sparse-shrink-grow.bin", MOUNT_POINT);
    std::fs::write(&file_path, b"abcdefghij").expect("seed write failed");
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&file_path)
        .expect("Failed to open for set_len");
    f.set_len(4096).expect("set_len(4096) failed");

    println!("  Step 3: Read [10..4096) -- must be zeros");
    use std::os::unix::fs::FileExt;
    let read_handle = std::fs::OpenOptions::new()
        .read(true)
        .open(&file_path)
        .expect("Failed to open for read");
    let mut tail = vec![0xffu8; 4096 - 10];
    read_handle
        .read_exact_at(&mut tail, 10)
        .expect("tail read failed");
    assert!(
        tail.iter().all(|&b| b == 0),
        "Extended region must read as zeros"
    );
    drop(read_handle);
    drop(f);

    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: Sparse truncate-then-extend test passed".green()
    );
    Ok(())
}

// Override flush preserves the surrounding bytes after a partial write
// + close + reopen + read. This exercises the path where flush keeps
// the existing blob_guid, bumps blob_version, and writes only the
// modified block at V+1; other blocks stay at their old version on
// disk and remain reachable through the new layout.
async fn test_sparse_override_flush_persists(disk_cache: bool) -> CmdResult {
    use aws_sdk_s3::primitives::ByteStream;
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload a 256KB file via S3 (spans 2 blocks)");
    let key = "sparse-override.bin";
    let original = generate_test_data(key, 256 * 1024);
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(original.clone()))
        .send()
        .await
        .expect("put failed");

    println!("  Step 2: Mount FUSE rw");
    mount_fuse_rw(&bucket, disk_cache)?;

    println!("  Step 3: Open RDWR, patch 32 bytes near the start of block 1, close");
    let file_path = format!("{}/{}", MOUNT_POINT, key);
    let patch = b"OVERRIDE-FLUSH-CHECK-32B-MARKER!";
    let patch_offset: u64 = 128 * 1024 + 16; // 16 bytes into block 1
    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("open rdwr failed");
        f.write_all_at(patch, patch_offset)
            .expect("write_all_at failed");
        f.sync_all().expect("sync_all failed");
    }

    println!("  Step 4: Reopen and read; verify patch landed and surroundings are intact");
    let actual = std::fs::read(&file_path).expect("read failed");
    assert_eq!(actual.len(), original.len(), "size changed unexpectedly");
    assert_eq!(
        &actual[..patch_offset as usize],
        &original[..patch_offset as usize],
        "block 0 (or pre-patch region of block 1) corrupted"
    );
    assert_eq!(
        &actual[patch_offset as usize..patch_offset as usize + patch.len()],
        patch,
        "patch did not land"
    );
    assert_eq!(
        &actual[patch_offset as usize + patch.len()..],
        &original[patch_offset as usize + patch.len()..],
        "post-patch tail of block 1 corrupted"
    );

    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &[key]).await;
    println!(
        "{}",
        "SUCCESS: Sparse override flush persists test passed".green()
    );
    Ok(())
}

// A sparse file written through the override flush path: ftruncate to a
// large size, write a single small chunk near the end, close. After
// reopen, the unwritten ranges read as zeros (BlockNotFound -> zeros)
// and the written chunk reads correctly. This is the round-trip
// version of test_sparse_truncate_large that actually flushes.
async fn test_sparse_sparse_file_round_trip(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE rw and create a fresh file");
    mount_fuse_rw(&bucket, disk_cache)?;
    let file_path = format!("{}/sparse-roundtrip.bin", MOUNT_POINT);

    println!("  Step 2: Write seed bytes, then ftruncate to 4MB, write a marker near 3MB, close");
    let marker = b"END-MARKER-32B-XXXXXXXXXXXXXXXXX";
    let marker_offset: u64 = 3 * 1024 * 1024;
    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&file_path)
            .expect("create failed");
        f.write_all_at(b"head!", 0).expect("write head failed");
        f.set_len(4 * 1024 * 1024).expect("set_len failed");
        f.write_all_at(marker, marker_offset)
            .expect("write marker failed");
        f.sync_all().expect("sync_all failed");
    }

    println!("  Step 3: Reopen; verify size, head, marker, and a hole region read zeros");
    let meta = std::fs::metadata(&file_path).expect("stat failed");
    assert_eq!(meta.len(), 4 * 1024 * 1024, "size mismatch after flush");

    use std::os::unix::fs::FileExt;
    let f = std::fs::OpenOptions::new()
        .read(true)
        .open(&file_path)
        .expect("open ro failed");

    let mut head = vec![0u8; 5];
    f.read_exact_at(&mut head, 0).expect("read head failed");
    assert_eq!(&head, b"head!", "head bytes lost");

    let mut hole = vec![0xffu8; 4096];
    f.read_exact_at(&mut hole, 1024 * 1024)
        .expect("read hole failed");
    assert!(hole.iter().all(|&b| b == 0), "hole did not read as zeros");

    let mut readback = vec![0u8; marker.len()];
    f.read_exact_at(&mut readback, marker_offset)
        .expect("read marker failed");
    assert_eq!(&readback, marker, "marker mismatch");

    drop(f);
    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: Sparse sparse-file round trip test passed".green()
    );
    Ok(())
}

// fallocate + lseek tests
//
// FUSE block_size used by fs_server is 128KB (`DEFAULT_BLOCK_SIZE`).
// Tests below assume that boundary so the aligned / edge / single-block
// PUNCH_HOLE shapes are exercised.

// Used by the fallocate / lseek tests (ported in a later phase).
#[allow(dead_code)]
const BLOCK_SIZE: u64 = 128 * 1024;

fn do_fallocate(fd: i32, mode: i32, offset: u64, length: u64) -> Result<(), i32> {
    let rc = unsafe { libc::fallocate(fd, mode, offset as libc::off_t, length as libc::off_t) };
    if rc < 0 {
        Err(std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO))
    } else {
        Ok(())
    }
}

/// Wrapper around libc::lseek(2) that returns the resulting offset or errno.
fn do_lseek(fd: i32, offset: i64, whence: i32) -> Result<i64, i32> {
    let rc = unsafe { libc::lseek(fd, offset as libc::off_t, whence) };
    if rc < 0 {
        Err(std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO))
    } else {
        Ok(rc as i64)
    }
}

async fn test_fallocate_extend(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;
    println!("  Step 1: Mount FUSE rw");
    mount_fuse_rw(&bucket, disk_cache)?;

    let file_path = format!("{}/fallocate-extend.bin", MOUNT_POINT);
    println!("  Step 2: Create file with a small seed write");
    std::fs::write(&file_path, b"hello").expect("seed write failed");

    println!("  Step 3: fallocate(mode=0, offset=0, length=8KB) extends to 8KB");
    {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("open rdwr failed");
        do_fallocate(f.as_raw_fd(), 0, 0, 8 * 1024).expect("fallocate failed");
        f.sync_all().expect("sync_all failed");
    }

    println!("  Step 4: Reopen and verify size grew to 8KB");
    let meta = std::fs::metadata(&file_path).expect("stat failed");
    assert_eq!(meta.len(), 8 * 1024, "fallocate did not grow the file");

    println!("  Step 5: Tail past the original write reads as zeros");
    use std::os::unix::fs::FileExt;
    let f = std::fs::File::open(&file_path).expect("open ro failed");
    let mut tail = vec![0xffu8; 8 * 1024 - 5];
    f.read_exact_at(&mut tail, 5).expect("tail read failed");
    assert!(
        tail.iter().all(|&b| b == 0),
        "fallocated tail should be zero-filled"
    );
    drop(f);

    unmount_fuse()?;
    println!("{}", "SUCCESS: fallocate extend test passed".green());
    Ok(())
}

async fn test_fallocate_keep_size(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;
    println!("  Step 1: Mount FUSE rw");
    mount_fuse_rw(&bucket, disk_cache)?;

    let file_path = format!("{}/fallocate-keep-size.bin", MOUNT_POINT);
    println!("  Step 2: Create a 4KB file");
    std::fs::write(&file_path, vec![b'x'; 4 * 1024]).expect("seed write failed");

    println!("  Step 3: fallocate(KEEP_SIZE) past EOF must NOT grow the file");
    {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("open rdwr failed");
        do_fallocate(f.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, 0, 64 * 1024)
            .expect("fallocate KEEP_SIZE failed");
        f.sync_all().expect("sync_all failed");
    }

    println!("  Step 4: Verify size is still 4KB");
    let meta = std::fs::metadata(&file_path).expect("stat failed");
    assert_eq!(meta.len(), 4 * 1024, "KEEP_SIZE must not change file size");

    unmount_fuse()?;
    println!("{}", "SUCCESS: fallocate KEEP_SIZE test passed".green());
    Ok(())
}

async fn test_fallocate_punch_hole_aligned(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;
    println!("  Step 1: Mount FUSE rw");
    mount_fuse_rw(&bucket, disk_cache)?;

    let file_path = format!("{}/fallocate-punch-aligned.bin", MOUNT_POINT);
    let total = 3 * BLOCK_SIZE as usize;
    println!(
        "  Step 2: Create a {}KB file with non-zero pattern",
        total / 1024
    );
    let pattern: Vec<u8> = (0..total).map(|i| (i % 251 + 1) as u8).collect();
    std::fs::write(&file_path, &pattern).expect("seed write failed");

    println!("  Step 3: PUNCH_HOLE the middle block (block-aligned)");
    {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("open rdwr failed");
        do_fallocate(
            f.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            BLOCK_SIZE,
            BLOCK_SIZE,
        )
        .expect("PUNCH_HOLE failed");
        f.sync_all().expect("sync_all failed");
    }

    println!("  Step 4: Verify punched block reads as zeros, neighbours intact");
    use std::os::unix::fs::FileExt;
    let f = std::fs::File::open(&file_path).expect("open ro failed");

    let mut head = vec![0u8; BLOCK_SIZE as usize];
    f.read_exact_at(&mut head, 0).expect("head read failed");
    assert_eq!(
        &head,
        &pattern[..BLOCK_SIZE as usize],
        "head block corrupted"
    );

    let mut hole = vec![0xffu8; BLOCK_SIZE as usize];
    f.read_exact_at(&mut hole, BLOCK_SIZE)
        .expect("hole read failed");
    assert!(
        hole.iter().all(|&b| b == 0),
        "punched block should be zeros"
    );

    let mut tail = vec![0u8; BLOCK_SIZE as usize];
    f.read_exact_at(&mut tail, 2 * BLOCK_SIZE)
        .expect("tail read failed");
    assert_eq!(
        &tail,
        &pattern[2 * BLOCK_SIZE as usize..],
        "tail block corrupted"
    );

    let meta = std::fs::metadata(&file_path).expect("stat failed");
    assert_eq!(meta.len(), total as u64, "PUNCH_HOLE must keep size");

    drop(f);
    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: fallocate PUNCH_HOLE aligned test passed".green()
    );
    Ok(())
}

async fn test_fallocate_punch_hole_edge(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;
    println!("  Step 1: Mount FUSE rw");
    mount_fuse_rw(&bucket, disk_cache)?;

    let file_path = format!("{}/fallocate-punch-edge.bin", MOUNT_POINT);
    let total = 3 * BLOCK_SIZE as usize;
    let pattern: Vec<u8> = (0..total).map(|i| (i % 251 + 1) as u8).collect();
    println!("  Step 2: Seed a {}KB file", total / 1024);
    std::fs::write(&file_path, &pattern).expect("seed write failed");

    let punch_offset: u64 = BLOCK_SIZE - 1024;
    let punch_len: u64 = 2 * 1024 + BLOCK_SIZE; // crosses two block boundaries
    println!(
        "  Step 3: PUNCH_HOLE crossing block boundaries (offset={}, len={})",
        punch_offset, punch_len
    );
    {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("open rdwr failed");
        do_fallocate(
            f.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            punch_offset,
            punch_len,
        )
        .expect("PUNCH_HOLE failed");
        f.sync_all().expect("sync_all failed");
    }

    println!("  Step 4: Verify [punch_offset..punch_offset+punch_len) reads as zeros");
    use std::os::unix::fs::FileExt;
    let f = std::fs::File::open(&file_path).expect("open ro failed");

    let mut hole = vec![0xffu8; punch_len as usize];
    f.read_exact_at(&mut hole, punch_offset)
        .expect("hole read failed");
    assert!(
        hole.iter().all(|&b| b == 0),
        "punched edge range should be zeros, first non-zero at {}",
        hole.iter().position(|&b| b != 0).unwrap_or(usize::MAX)
    );

    println!("  Step 5: Verify pre-punch and post-punch bytes are intact");
    let mut pre = vec![0u8; punch_offset as usize];
    f.read_exact_at(&mut pre, 0).expect("pre read failed");
    assert_eq!(
        &pre,
        &pattern[..punch_offset as usize],
        "pre-punch corrupted"
    );

    let post_offset = punch_offset + punch_len;
    let post_len = total as u64 - post_offset;
    let mut post = vec![0u8; post_len as usize];
    f.read_exact_at(&mut post, post_offset)
        .expect("post read failed");
    assert_eq!(
        &post,
        &pattern[post_offset as usize..],
        "post-punch corrupted"
    );

    drop(f);
    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: fallocate PUNCH_HOLE edge test passed".green()
    );
    Ok(())
}

async fn test_fallocate_punch_hole_single_block(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;
    println!("  Step 1: Mount FUSE rw");
    mount_fuse_rw(&bucket, disk_cache)?;

    let file_path = format!("{}/fallocate-punch-single.bin", MOUNT_POINT);
    let total: usize = 8 * 1024;
    let pattern: Vec<u8> = (0..total).map(|i| ((i % 250) + 5) as u8).collect();
    println!("  Step 2: Seed an 8KB file (single 128KB block)");
    std::fs::write(&file_path, &pattern).expect("seed write failed");

    let punch_offset: u64 = 1024;
    let punch_len: u64 = 2 * 1024;
    println!(
        "  Step 3: PUNCH_HOLE confined to one block ({}, {})",
        punch_offset, punch_len
    );
    {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("open rdwr failed");
        do_fallocate(
            f.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            punch_offset,
            punch_len,
        )
        .expect("PUNCH_HOLE failed");
        f.sync_all().expect("sync_all failed");
    }

    println!("  Step 4: Verify only [1024..3072) is zero, surroundings intact");
    use std::os::unix::fs::FileExt;
    let f = std::fs::File::open(&file_path).expect("open ro failed");

    let mut all = vec![0u8; total];
    f.read_exact_at(&mut all, 0).expect("read failed");
    assert_eq!(
        &all[..punch_offset as usize],
        &pattern[..punch_offset as usize],
        "head bytes corrupted"
    );
    assert!(
        all[punch_offset as usize..(punch_offset + punch_len) as usize]
            .iter()
            .all(|&b| b == 0),
        "punched range must be zero"
    );
    assert_eq!(
        &all[(punch_offset + punch_len) as usize..],
        &pattern[(punch_offset + punch_len) as usize..],
        "tail bytes corrupted"
    );

    drop(f);
    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: fallocate single-block PUNCH_HOLE test passed".green()
    );
    Ok(())
}

async fn test_lseek_seek_data_hole(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;
    println!("  Step 1: Mount FUSE rw");
    mount_fuse_rw(&bucket, disk_cache)?;

    let file_path = format!("{}/lseek-sparse.bin", MOUNT_POINT);

    // The replace-flush path that handles brand-new files writes every
    // logical block dense, so a fresh `create + set_len + write_at_3MB`
    // sequence ends up with no actual holes on disk. To exercise lseek
    // against real holes we seed the file first (one block becomes
    // committed), then re-open and let the override-flush path place
    // only the new tail block at offset 3MB. Everything between the
    // first block and the tail block stays unallocated in BSS.
    println!("  Step 2: Seed a tiny file so subsequent flushes use the override path");
    std::fs::write(&file_path, b"seed-data").expect("seed write failed");

    println!("  Step 3: Re-open, extend to 4MB, write at 3MB; sync");
    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("open rdwr failed");
        f.set_len(4 * 1024 * 1024).expect("set_len failed");
        f.write_all_at(b"data!", 3 * 1024 * 1024)
            .expect("data write failed");
        f.sync_all().expect("sync_all failed");
    }

    println!("  Step 4: SEEK_DATA from inside the hole must jump to the tail data block");
    use std::os::unix::io::AsRawFd;
    let f = std::fs::File::open(&file_path).expect("open ro failed");
    let fd = f.as_raw_fd();

    let data_off = do_lseek(fd, BLOCK_SIZE as i64, libc::SEEK_DATA)
        .expect("SEEK_DATA from inside hole failed");
    assert!(
        data_off >= BLOCK_SIZE as i64 && data_off <= 3 * 1024 * 1024,
        "SEEK_DATA returned {} (expected jump to the 3MB data block)",
        data_off
    );

    println!(
        "  Step 5: SEEK_HOLE from offset 0 (block 0 has the seed) should land in the hole region"
    );
    let hole_off = do_lseek(fd, 0, libc::SEEK_HOLE).expect("SEEK_HOLE from 0 failed");
    assert!(
        hole_off >= BLOCK_SIZE as i64 && hole_off < 3 * 1024 * 1024,
        "SEEK_HOLE returned {} (expected first hole offset between block 1 and 3MB)",
        hole_off
    );

    println!("  Step 6: SEEK_HOLE from offset 3MB (mid-data) should advance past the data block");
    let after_data =
        do_lseek(fd, 3 * 1024 * 1024, libc::SEEK_HOLE).expect("SEEK_HOLE from 3MB failed");
    assert!(
        after_data > 3 * 1024 * 1024,
        "SEEK_HOLE from 3MB returned {} (must be > 3MB)",
        after_data
    );

    println!("  Step 7: SEEK_DATA past EOF must return ENXIO");
    let err =
        do_lseek(fd, 4 * 1024 * 1024, libc::SEEK_DATA).expect_err("SEEK_DATA past EOF must fail");
    assert_eq!(err, libc::ENXIO, "expected ENXIO past EOF, got {}", err);

    drop(f);
    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: lseek SEEK_DATA / SEEK_HOLE on sparse file test passed".green()
    );
    Ok(())
}

async fn test_lseek_punched_hole(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;
    println!("  Step 1: Mount FUSE rw");
    mount_fuse_rw(&bucket, disk_cache)?;

    let file_path = format!("{}/lseek-punched.bin", MOUNT_POINT);
    let total = 3 * BLOCK_SIZE as usize;
    println!(
        "  Step 2: Seed a {}KB file with a non-zero pattern",
        total / 1024
    );
    let pattern: Vec<u8> = (0..total).map(|i| (i % 251 + 1) as u8).collect();
    std::fs::write(&file_path, &pattern).expect("seed write failed");

    println!("  Step 3: PUNCH_HOLE the middle aligned block");
    {
        use std::os::unix::io::AsRawFd;
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .expect("open rdwr failed");
        do_fallocate(
            f.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            BLOCK_SIZE,
            BLOCK_SIZE,
        )
        .expect("PUNCH_HOLE failed");
        f.sync_all().expect("sync_all failed");
    }

    println!("  Step 4: SEEK_HOLE from offset 0 must land at the punched block");
    use std::os::unix::io::AsRawFd;
    let f = std::fs::File::open(&file_path).expect("open ro failed");
    let fd = f.as_raw_fd();

    let hole_off = do_lseek(fd, 0, libc::SEEK_HOLE).expect("SEEK_HOLE failed");
    assert!(
        hole_off >= BLOCK_SIZE as i64 && hole_off < 2 * BLOCK_SIZE as i64,
        "SEEK_HOLE returned {}, expected within the punched range",
        hole_off
    );

    println!("  Step 5: SEEK_DATA from inside the hole must land in the trailing data block");
    let data_off =
        do_lseek(fd, BLOCK_SIZE as i64, libc::SEEK_DATA).expect("SEEK_DATA from hole failed");
    assert!(
        data_off >= 2 * BLOCK_SIZE as i64,
        "SEEK_DATA returned {}, expected the trailing data block",
        data_off
    );

    drop(f);
    unmount_fuse()?;
    println!("{}", "SUCCESS: lseek punched-hole test passed".green());
    Ok(())
}

async fn test_disk_cache_survives_override(disk_cache: bool) -> CmdResult {
    assert!(disk_cache, "this test requires disk cache");
    let (_ctx, bucket) = setup_test_bucket().await;

    let dc_path = disk_cache_path();
    let _ = std::fs::remove_dir_all(&dc_path);

    println!("  Step 1: Mount RW and create a file with original bytes");
    mount_fuse_rw(&bucket, disk_cache)?;
    let key = "dc-survives-override.bin";
    let fuse_path = format!("{}/{}", MOUNT_POINT, key);
    let original = generate_test_data(key, 64 * 1024);
    std::fs::write(&fuse_path, &original).expect("Failed to write original");
    println!("    Wrote: {} ({} bytes)", key, original.len());

    println!("  Step 2: Read once to populate the disk cache at V=1");
    let first_read = std::fs::read(&fuse_path).expect("Failed initial read");
    assert_eq!(first_read, original, "initial read mismatch");
    let cache_files_v1: Vec<_> = std::fs::read_dir(&dc_path)
        .expect("Failed to list disk cache dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        cache_files_v1.len(),
        1,
        "exactly one cache file at the stable path expected; got {:?}",
        cache_files_v1
    );
    let cache_name = cache_files_v1[0].clone();
    assert!(
        !cache_name.contains("_v"),
        "cache filename {} must not carry a version suffix under the new scheme",
        cache_name
    );
    println!("    Cache file: {}", cache_name);

    println!("  Step 3: Override-flush: rewrite the file with new bytes at V+1");
    let updated = {
        let mut v = original.clone();
        // Mutate every byte so a stale-cache hit produces a clearly
        // different result than the new content.
        for (i, b) in v.iter_mut().enumerate() {
            *b = b.wrapping_add(((i % 251) + 1) as u8);
        }
        v
    };
    std::fs::write(&fuse_path, &updated).expect("Failed to overwrite");
    println!("    Overwrote: {} bytes", updated.len());

    println!("  Step 4: Read after override; the disk cache must serve V+1 bytes");
    let post_read = std::fs::read(&fuse_path).expect("Failed post-override read");
    assert_eq!(
        post_read, updated,
        "stale-cache regression: read returned previous-version bytes"
    );

    println!("  Step 5: Cache file path is unchanged (no rename, no new file)");
    let cache_files_v2: Vec<_> = std::fs::read_dir(&dc_path)
        .expect("Failed to list disk cache dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        cache_files_v2.len(),
        1,
        "expected exactly one cache file post-override; got {:?}",
        cache_files_v2
    );
    assert_eq!(
        cache_files_v2[0], cache_name,
        "cache file path must be stable across the override flush"
    );
    println!("    Cache file (unchanged): {}", cache_files_v2[0]);

    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: Disk cache survives override (stable path) passed".green()
    );
    Ok(())
}

async fn test_qemu_style_fio_workload(disk_cache: bool) -> CmdResult {
    assert!(disk_cache, "this test requires disk cache");
    let (_ctx, bucket) = setup_test_bucket().await;
    let dc_path = disk_cache_path();
    let _ = std::fs::remove_dir_all(&dc_path);

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket, disk_cache)?;
    let key = "qemu-style.img";
    let fuse_path = format!("{}/{}", MOUNT_POINT, key);

    println!("  Step 2: Create a 64 MiB seed file (dd from /dev/urandom)");
    let seed_size: u64 = 64 * 1024 * 1024;
    run_cmd!(dd if=/dev/urandom of=$fuse_path bs=1M count=64 conv=fsync 2>/dev/null)?;
    let stat0 = std::fs::metadata(&fuse_path).expect("stat seed");
    assert_eq!(stat0.len(), seed_size, "seed file size");

    println!("  Step 3: Snapshot pre-fio cache state");
    let pre_count = std::fs::read_dir(&dc_path)
        .map(|d| {
            d.filter_map(|e| e.ok())
                .filter(|e| e.path().is_file())
                .count()
        })
        .unwrap_or(0);
    println!("    pre-fio cache files: {}", pre_count);

    println!("  Step 4: fio random 4 KiB writes for 10s");
    let fio_output = run_fun!(
        fio --name=qemu-style --filename=$fuse_path --rw=randwrite --bs=4k
            --size=64M --iodepth=1 --numjobs=1 --runtime=10 --time_based
            --minimal 2>&1
    )?;
    // The `--minimal` output is one CSV line per job; pick a couple of
    // numbers out for the log so we have evidence the workload actually
    // ran (some IOPS, non-zero bytes).
    let fields: Vec<&str> = fio_output.split(';').collect();
    if fields.len() > 30 {
        // Field positions for minimal output are documented in fio(1).
        // 7 = total bytes written (KB), 8 = bandwidth (KB/s),
        // 49 = total IO time (msec), best-effort label.
        let bytes_written = fields.get(48).copied().unwrap_or("?");
        let iops = fields.get(49).copied().unwrap_or("?");
        println!("    fio: writes_KB={} iops_avg={}", bytes_written, iops);
    } else {
        println!(
            "    fio: completed (minimal output had {} fields)",
            fields.len()
        );
    }

    println!("  Step 5: Sync + verify file size unchanged");
    run_cmd!(sync $fuse_path 2>/dev/null)?;
    let stat1 = std::fs::metadata(&fuse_path).expect("stat post-fio");
    assert_eq!(
        stat1.len(),
        seed_size,
        "file size must be stable across the random-write workload"
    );

    println!("  Step 6: Read every block back and assert population");
    let bytes = std::fs::read(&fuse_path).expect("read post-fio");
    assert_eq!(bytes.len(), seed_size as usize, "read length");
    let nonzero = bytes
        .chunks(4096)
        .filter(|b| b.iter().any(|&v| v != 0))
        .count();
    assert!(
        nonzero > 1000,
        "expected most 4 KiB blocks populated, got {}",
        nonzero
    );

    println!("  Step 7: Cache stability -- exactly one file per blob (stable-path invariant)");
    let post_files: Vec<_> = std::fs::read_dir(&dc_path)
        .expect("read disk cache dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    println!("    post-fio cache files: {:?}", post_files);
    let result = if post_files.len() == 1 {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "expected exactly 1 cache file under stable-path scheme; got {} ({:?})",
            post_files.len(),
            post_files,
        )))
    };
    unmount_fuse()?;
    result?;

    println!(
        "{}",
        "SUCCESS: qemu-style fio workload + stable-path cache invariant".green()
    );
    Ok(())
}

async fn test_override_survives_bss_partition_rejoin(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;
    let key = "partition-rejoin.bin";
    let fuse_path = format!("{}/{}", MOUNT_POINT, key);

    println!("  Step 1: Mount RW, create a 256 KiB file at V=1, fsync to drain to BSS");
    mount_fuse_rw(&bucket, disk_cache)?;
    let v1 = generate_test_data("partition-rejoin-v1", 256 * 1024);
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&fuse_path)
            .expect("open for v1");
        f.write_all(&v1).expect("write_all v1");
        // sync_all forces the writeback flush to land in NSS+BSS
        // before we stop node 0 in Step 2. std::fs::write does not
        // fsync, and default-mode release-flush is async (a spawned
        // task) that would be killed by the upcoming unmount.
        f.sync_all().expect("sync_all v1");
    }
    let meta = std::fs::metadata(&fuse_path).expect("stat v1");
    assert_eq!(meta.len(), v1.len() as u64, "v1 file_size after release");
    unmount_fuse()?;

    println!("  Step 2: Stop BSS node 0 (one of 3 replicas)");
    run_cmd!(systemctl --user stop bss@0.service)?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("  Step 3: Remount and override-flush at V=2 with node 0 down");
    mount_fuse_rw(&bucket, disk_cache)?;
    let v2 = generate_test_data("partition-rejoin-v2", 256 * 1024);
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&fuse_path)
            .expect("open for override-write");
        f.write_all(&v2).expect("write_all v2");
        f.sync_all().expect("sync_all v2");
    }
    unmount_fuse()?;

    println!("  Step 4: Restart BSS node 0 (rejoins with stale V=1 bytes)");
    run_cmd!(systemctl --user start bss@0.service)?;
    tokio::time::sleep(Duration::from_secs(2)).await;

    if disk_cache {
        println!("  Step 5: Wipe the disk cache so reads actually hit BSS");
        let _ = std::fs::remove_dir_all(disk_cache_path());
    }

    println!(
        "  Step 6: Remount read-only and read; expect V=2 bytes regardless of which replica answers"
    );
    mount_fuse_ro(&bucket, disk_cache)?;
    let post = std::fs::read(&fuse_path).expect("Failed post-rejoin read");
    let read_ok = post == v2;
    unmount_fuse()?;
    if !read_ok {
        return Err(std::io::Error::other(
            "stale-replica regression: post-rejoin read did not return V=2 content",
        ));
    }

    println!(
        "{}",
        "SUCCESS: Override survives BSS partition-rejoin (3-replica fan-out + inline-repair)"
            .green()
    );
    Ok(())
}

async fn test_writeback_default_mode_async_release(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE");
    mount_fuse_writeback(&bucket, true, disk_cache)?;

    println!("  Step 2: Create + write + close a file (close returns before NSS commit)");
    let key = "wb-async-release.bin";
    let fuse_path = format!("{}/{}", MOUNT_POINT, key);
    let payload = generate_test_data(key, 32 * 1024);
    std::fs::write(&fuse_path, &payload).expect("write+close failed");

    println!("  Step 3: Poll until NSS commit visible via dir listing (up to 5s)");
    let mount_point = MOUNT_POINT;
    let mut committed = false;
    for _ in 0..50 {
        std::thread::sleep(Duration::from_millis(100));
        let entries: Vec<_> = std::fs::read_dir(mount_point)
            .expect("readdir failed")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        if entries.iter().any(|n| n == key) {
            committed = true;
            break;
        }
    }
    assert!(
        committed,
        "async-release flush failed to commit within 5s; default-mode pipeline broken"
    );
    println!("    NSS commit observed via dir listing");

    println!("  Step 4: Read the file back; bytes must match");
    let read_back = std::fs::read(&fuse_path).expect("post-flush read failed");
    assert_eq!(
        read_back.len(),
        payload.len(),
        "post-flush size mismatch: expected {}, got {}",
        payload.len(),
        read_back.len()
    );
    assert_eq!(read_back, payload, "post-flush content mismatch");

    let _ = std::fs::remove_file(&fuse_path);
    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: writeback default mode async release passed".green()
    );
    Ok(())
}

async fn test_writeback_default_mode_mkdir(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in writeback default mode");
    mount_fuse_writeback(&bucket, true, disk_cache)?;

    println!("  Step 2: Create a 5-deep nested directory tree");
    let path_a = format!("{}/wb-mkdir-a", MOUNT_POINT);
    let path_b = format!("{}/b", path_a);
    let path_c = format!("{}/c", path_b);
    let path_d = format!("{}/d", path_c);
    let path_e = format!("{}/e", path_d);
    std::fs::create_dir_all(&path_e).expect("create_dir_all failed");
    println!("    enqueued nested mkdirs");

    println!("  Step 3: Poll until every level is visible (up to 5s)");
    let mut all_visible = false;
    for _ in 0..50 {
        std::thread::sleep(Duration::from_millis(100));
        let levels = [&path_a, &path_b, &path_c, &path_d, &path_e];
        let visible = levels
            .iter()
            .filter(|p| std::fs::metadata(p).is_ok())
            .count();
        if visible == levels.len() {
            all_visible = true;
            break;
        }
    }
    assert!(
        all_visible,
        "default-mode mkdir failed to commit all 5 levels within 5s"
    );

    println!("  Step 4: Drop a regular file in the deepest dir");
    let leaf_file = format!("{}/leaf.txt", path_e);
    std::fs::write(&leaf_file, b"hello deep").expect("write leaf failed");
    let mut leaf_committed = false;
    for _ in 0..50 {
        std::thread::sleep(Duration::from_millis(100));
        if std::fs::metadata(&leaf_file).is_ok() {
            leaf_committed = true;
            break;
        }
    }
    assert!(leaf_committed, "leaf file did not commit within 5s");
    let read_back = std::fs::read(&leaf_file).expect("read leaf failed");
    assert_eq!(&read_back[..], b"hello deep");

    // Cleanup: rm -rf the tree root takes leaf.txt and b/c/d/e with it.
    run_cmd!(ignore rm -rf $path_a)?;
    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: writeback default mode mkdir (5-level nested) passed".green()
    );
    Ok(())
}

async fn test_writeback_default_mode_ancestor_deps(disk_cache: bool) -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in writeback default mode");
    mount_fuse_writeback(&bucket, true, disk_cache)?;

    println!("  Step 2: Rapid-fire 30 (mkdir, mkdir/, ) pairs");
    let n = 30usize;
    for i in 0..n {
        let dir = format!("{}/wb-deps-d{:03}", MOUNT_POINT, i);
        std::fs::create_dir(&dir).expect("mkdir wb-deps dir failed");
    }
    println!("    enqueued {} mkdirs", n);

    println!("  Step 3: Poll until every dir is visible (up to 10s)");
    let mount_point = MOUNT_POINT;
    let mut all_dirs = false;
    for _ in 0..100 {
        std::thread::sleep(Duration::from_millis(100));
        let entries: Vec<_> = std::fs::read_dir(mount_point)
            .expect("readdir failed")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        let visible = (0..n)
            .filter(|i| entries.iter().any(|nm| nm == &format!("wb-deps-d{:03}", i)))
            .count();
        if visible == n {
            all_dirs = true;
            break;
        }
    }
    assert!(
        all_dirs,
        "default-mode mkdir burst failed to commit {} dirs",
        n
    );

    // Cleanup
    for i in 0..n {
        let _ = std::fs::remove_dir(format!("{}/wb-deps-d{:03}", MOUNT_POINT, i));
    }
    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: writeback default mode ancestor deps (30 mkdirs) passed".green()
    );
    Ok(())
}

async fn test_writeback_default_mode_fsyncdir(disk_cache: bool) -> CmdResult {
    use std::os::fd::AsRawFd;
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in writeback default mode");
    mount_fuse_writeback(&bucket, true, disk_cache)?;

    println!("  Step 2: Burst-create 20 files at the mount root");
    let n = 20usize;
    for i in 0..n {
        let path = format!("{}/wb-fsyncdir-{:03}.txt", MOUNT_POINT, i);
        std::fs::write(&path, format!("payload-{}", i).as_bytes())
            .expect("write wb-fsyncdir file failed");
    }
    println!("    enqueued {} files", n);

    println!("  Step 3: fsync the parent directory; must block until queue drains");
    let dir = std::fs::File::open(MOUNT_POINT).expect("open mount root");
    let dir_fd = dir.as_raw_fd();
    let r = unsafe { libc::fsync(dir_fd) };
    assert_eq!(
        r,
        0,
        "fsync(dir) failed: {}",
        std::io::Error::last_os_error()
    );
    drop(dir);

    println!("  Step 4: Without polling, every file must already be in NSS");
    let entries: Vec<_> = std::fs::read_dir(MOUNT_POINT)
        .expect("readdir failed")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    let visible = (0..n)
        .filter(|i| {
            entries
                .iter()
                .any(|name| name == &format!("wb-fsyncdir-{:03}.txt", i))
        })
        .count();
    assert_eq!(
        visible, n,
        "fsyncdir did not drain the queue: {}/{} files visible",
        visible, n
    );

    // Cleanup
    for i in 0..n {
        let _ = std::fs::remove_file(format!("{}/wb-fsyncdir-{:03}.txt", MOUNT_POINT, i));
    }
    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: writeback default mode fsyncdir drain passed".green()
    );
    Ok(())
}

async fn test_writeback_default_mode_o_sync(disk_cache: bool) -> CmdResult {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in writeback default mode");
    mount_fuse_writeback(&bucket, true, disk_cache)?;

    let path = format!("{}/wb-osync.txt", MOUNT_POINT);

    println!("  Step 2: Pre-create the file (sets up the inode + early-publish)");
    std::fs::write(&path, b"v0").expect("pre-create failed");
    // Wait for that initial create's queue cycle to drain (we expect
    // it to take one worker tick at most). Without this, the O_DSYNC
    // open below races with the create's own put_inode.
    std::thread::sleep(Duration::from_millis(200));

    println!("  Step 3: Open with O_DSYNC and write");
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .custom_flags(libc::O_DSYNC)
        .open(&path)
        .expect("open O_DSYNC failed");
    let payload = b"sync-payload";
    f.write_all(payload).expect("write failed");
    // Each write under O_SYNC drains the queue; close (which flushes
    // again) is then a no-op but kept for symmetry with userspace.
    drop(f);

    println!("  Step 4: Read back via a fresh fd; bytes must match");
    let read_back = std::fs::read(&path).expect("read failed");
    assert_eq!(
        &read_back[..],
        payload,
        "O_DSYNC write did not surface synchronously"
    );

    let _ = std::fs::remove_file(&path);
    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: writeback default mode O_DSYNC drain passed".green()
    );
    Ok(())
}
