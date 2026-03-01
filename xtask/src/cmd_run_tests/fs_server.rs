use crate::cmd_service;
use crate::{CmdResult, ServiceName};
use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use colored::*;
use std::path::Path;
use std::time::Duration;
use test_common::*;

const MOUNT_POINT: &str = "/tmp/fs_server_test";
const BUCKET_NAME: &str = "test-file-server";

pub async fn run_fs_server_tests() -> CmdResult {
    info!("Running fs_server integration tests...");

    println!("\n{}", "=== Test: Basic File Read ===".bold());
    if let Err(e) = test_basic_file_read().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Directory Listing ===".bold());
    if let Err(e) = test_directory_listing().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Large File Read ===".bold());
    if let Err(e) = test_large_file_read().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== Test: Nested Directory Structure ===".bold()
    );
    if let Err(e) = test_nested_directories().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    // Phase 2: Write tests
    println!("\n{}", "=== Test: Create, Write, Read ===".bold());
    if let Err(e) = test_create_write_read().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Mkdir and Rmdir ===".bold());
    if let Err(e) = test_mkdir_rmdir().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Unlink ===".bold());
    if let Err(e) = test_unlink().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Rename ===".bold());
    if let Err(e) = test_rename().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== Test: Unlink with Open Handle ===".bold()
    );
    if let Err(e) = test_unlink_open_handle().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== Test: Overwrite Existing File ===".bold()
    );
    if let Err(e) = test_overwrite_existing().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== Test: Rename No-Replace (EEXIST) ===".bold()
    );
    if let Err(e) = test_rename_noreplace().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Truncate Write ===".bold());
    if let Err(e) = test_truncate_write().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== Test: Write in Subdirectory ===".bold()
    );
    if let Err(e) = test_write_in_subdirectory().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Rename Directory ===".bold());
    if let Err(e) = test_rename_directory().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== All FS Server Tests PASSED ===".green().bold());
    Ok(())
}

/// Build the fs_server binary.
pub fn build_fs_server() -> CmdResult {
    run_cmd! {
        info "Building fs_server ...";
        cargo build -p fs_server;
    }
}

/// Enable FUSE io_uring support (requires kernel >= 6.14).
pub fn ensure_fuse_uring() -> CmdResult {
    #[rustfmt::skip]
    let kernel_version = run_fun!(uname -r)?;
    let parts: Vec<&str> = kernel_version.split('.').collect();
    let major: u32 = parts.first().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor: u32 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);

    if major < 6 || (major == 6 && minor < 14) {
        info!("Kernel {kernel_version} < 6.14, skipping FUSE io_uring enablement");
        return Ok(());
    }

    let current =
        std::fs::read_to_string("/sys/module/fuse/parameters/enable_uring").unwrap_or_default();
    if current.trim() != "N" {
        info!("FUSE io_uring already enabled (kernel {kernel_version})");
        return Ok(());
    }

    info!("Enabling FUSE io_uring support (kernel {kernel_version})");
    run_cmd!(sudo sh -c "echo Y > /sys/module/fuse/parameters/enable_uring")?;
    Ok(())
}

fn write_fs_server_env(bucket: &str, mount_point: &str, read_write: bool) -> CmdResult {
    let env_content = format!(
        "FUSE_BUCKET_NAME={bucket}\nFUSE_MOUNT_POINT={mount_point}\nFUSE_READ_WRITE={read_write}\n"
    );
    run_cmd!(mkdir -p data/etc)?;
    std::fs::write("data/etc/fs_server.env", env_content)?;
    Ok(())
}

fn mount_fuse(bucket: &str) -> CmdResult {
    mount_fuse_with_opts(bucket, false)
}

fn mount_fuse_rw(bucket: &str) -> CmdResult {
    mount_fuse_with_opts(bucket, true)
}

fn mount_fuse_with_opts(bucket: &str, read_write: bool) -> CmdResult {
    let mount_point = MOUNT_POINT;

    // Create mount point directory
    run_cmd!(mkdir -p $mount_point)?;

    // Write env file and start fs_server via systemd
    write_fs_server_env(bucket, mount_point, read_write)?;
    cmd_service::init_service(
        ServiceName::FsServer,
        crate::cmd_build::BuildMode::Debug,
        crate::InitConfig::default(),
    )?;
    cmd_service::start_service(ServiceName::FsServer)?;

    // Wait for mount to appear (poll up to 10 seconds)
    for i in 0..20 {
        std::thread::sleep(Duration::from_millis(500));
        let status = std::process::Command::new("mountpoint")
            .arg("-q")
            .arg(mount_point)
            .status();
        if let Ok(s) = status
            && s.success()
        {
            println!(
                "    FUSE mounted at {} (after {}ms)",
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

fn unmount_fuse() -> CmdResult {
    let mount_point = MOUNT_POINT;
    // Try fusermount3 first, then fusermount
    if run_cmd!(fusermount3 -u $mount_point 2>/dev/null).is_err() {
        let _ = run_cmd!(fusermount -u $mount_point 2>/dev/null);
    }
    // Stop fs_server systemd service
    let _ = cmd_service::stop_service(ServiceName::FsServer);
    // Kill any remaining fs_server processes
    let _ = run_cmd!(pkill -f "fs_server" 2>/dev/null);
    std::thread::sleep(Duration::from_millis(500));
    Ok(())
}

/// Generate deterministic test data from a key name.
fn generate_test_data(key: &str, size: usize) -> Vec<u8> {
    let pattern = format!("<<{key}>>");
    let pattern_bytes = pattern.as_bytes();
    let mut data = Vec::with_capacity(size);
    while data.len() < size {
        let remaining = size - data.len();
        let chunk = &pattern_bytes[..remaining.min(pattern_bytes.len())];
        data.extend_from_slice(chunk);
    }
    data
}

async fn setup_test_bucket() -> (Context, String) {
    let ctx = context();
    let bucket = ctx.create_bucket(BUCKET_NAME).await;
    (ctx, bucket)
}

async fn cleanup_objects(ctx: &Context, bucket: &str, keys: &[&str]) {
    for key in keys {
        let _ = ctx
            .client
            .delete_object()
            .bucket(bucket)
            .key(*key)
            .send()
            .await;
    }
}

async fn test_basic_file_read() -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    // Step 1: Upload test objects via S3 API
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
            .map_err(|e| std::io::Error::other(format!("Failed to put {key}: {e}")))?;
        println!("    Uploaded: {} ({} bytes)", key, data.len());
    }

    // Step 2: Mount FUSE
    println!("  Step 2: Mount FUSE filesystem");
    mount_fuse(&bucket)?;

    // Step 3: Read and verify files mount
    println!("  Step 3: Read and verify files mount");
    let mut passed = 0;
    let mut failed = 0;

    for (key, expected_data) in &test_files {
        let fuse_path = format!("{}/{}", MOUNT_POINT, key);
        match std::fs::read(&fuse_path) {
            Ok(actual_data) => {
                if actual_data == *expected_data {
                    println!("    {}: OK ({} bytes)", key, actual_data.len());
                    passed += 1;
                } else {
                    println!(
                        "    {}: {} (expected {} bytes, got {} bytes)",
                        key,
                        "DATA MISMATCH".red(),
                        expected_data.len(),
                        actual_data.len()
                    );
                    failed += 1;
                }
            }
            Err(e) => {
                println!("    {}: {} ({})", key, "READ FAILED".red(), e);
                failed += 1;
            }
        }
    }

    // Cleanup
    unmount_fuse()?;
    cleanup_objects(
        &ctx,
        &bucket,
        &test_files.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
    )
    .await;

    if failed > 0 {
        return Err(std::io::Error::other(format!(
            "{} of {} file reads failed",
            failed,
            passed + failed
        )));
    }

    println!("{}", "SUCCESS: Basic file read test passed".green());
    Ok(())
}

async fn test_directory_listing() -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    // Upload objects with directory structure
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
            .map_err(|e| std::io::Error::other(format!("Failed to put {key}: {e}")))?;
    }
    println!("    Uploaded {} objects", keys.len());

    // Mount
    println!("  Step 2: Mount FUSE filesystem");
    mount_fuse(&bucket)?;

    // Verify root directory listing
    println!("  Step 3: Verify root directory listing");
    let root_entries: Vec<String> = std::fs::read_dir(MOUNT_POINT)
        .map_err(|e| std::io::Error::other(format!("Failed to list root: {e}")))?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    println!("    Root entries: {:?}", root_entries);

    let expected_root = vec!["top-level.txt", "docs", "src"];
    for expected in &expected_root {
        if !root_entries.contains(&expected.to_string()) {
            unmount_fuse()?;
            cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;
            return Err(std::io::Error::other(format!(
                "Missing root entry: {expected}"
            )));
        }
        println!("    Found: {}", expected);
    }

    // Verify subdirectory listing
    println!("  Step 4: Verify subdirectory listing");
    let docs_path = format!("{}/docs", MOUNT_POINT);
    let docs_entries: Vec<String> = std::fs::read_dir(&docs_path)
        .map_err(|e| std::io::Error::other(format!("Failed to list docs/: {e}")))?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    println!("    docs/ entries: {:?}", docs_entries);

    for expected in &["readme.md", "guide.md"] {
        if !docs_entries.contains(&expected.to_string()) {
            unmount_fuse()?;
            cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;
            return Err(std::io::Error::other(format!(
                "Missing docs/ entry: {expected}"
            )));
        }
    }

    // Verify file content in subdirectory
    println!("  Step 5: Verify file content in subdirectory");
    let readme_path = format!("{}/docs/readme.md", MOUNT_POINT);
    let content = std::fs::read_to_string(&readme_path)
        .map_err(|e| std::io::Error::other(format!("Failed to read docs/readme.md: {e}")))?;
    if content != "content of docs/readme.md" {
        unmount_fuse()?;
        cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;
        return Err(std::io::Error::other(format!(
            "Content mismatch for docs/readme.md: got '{content}'"
        )));
    }
    println!("    docs/readme.md content: OK");

    // Cleanup
    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;

    println!("{}", "SUCCESS: Directory listing test passed".green());
    Ok(())
}

async fn test_large_file_read() -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    // Test with various sizes including ones that cross block boundaries
    // Default block size is ~1MB (1048320 = 1024*1024 - 256)
    let sizes: Vec<(&str, usize)> = vec![
        ("small-4k", 4 * 1024),
        ("medium-512k", 512 * 1024),
        ("large-2mb", 2 * 1024 * 1024),
    ];

    // Upload
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
            .map_err(|e| std::io::Error::other(format!("Failed to put {key}: {e}")))?;
        upload_keys.push(key);
        println!("    Uploaded: {} ({} bytes)", label, size);
    }

    // Mount
    println!("  Step 2: Mount FUSE filesystem");
    mount_fuse(&bucket)?;

    // Read and verify
    println!("  Step 3: Read and verify large files");
    let mut passed = 0;
    let mut failed = 0;

    for (i, (label, size)) in sizes.iter().enumerate() {
        let key = &upload_keys[i];
        let expected_data = generate_test_data(key, *size);
        let fuse_path = format!("{}/{}", MOUNT_POINT, key);

        match std::fs::read(&fuse_path) {
            Ok(actual_data) => {
                if actual_data == expected_data {
                    println!("    {}: OK ({} bytes)", label, actual_data.len());
                    passed += 1;
                } else {
                    let first_diff = actual_data
                        .iter()
                        .zip(expected_data.iter())
                        .position(|(a, b)| a != b);
                    println!(
                        "    {}: {} (expected {} bytes, got {}, first diff at {:?})",
                        label,
                        "DATA MISMATCH".red(),
                        expected_data.len(),
                        actual_data.len(),
                        first_diff,
                    );
                    failed += 1;
                }
            }
            Err(e) => {
                println!("    {}: {} ({})", label, "READ FAILED".red(), e);
                failed += 1;
            }
        }
    }

    // Cleanup
    unmount_fuse()?;
    let key_refs: Vec<&str> = upload_keys.iter().map(|k| k.as_str()).collect();
    cleanup_objects(&ctx, &bucket, &key_refs).await;

    if failed > 0 {
        return Err(std::io::Error::other(format!(
            "{} of {} large file reads failed",
            failed,
            passed + failed
        )));
    }

    println!("{}", "SUCCESS: Large file read test passed".green());
    Ok(())
}

async fn test_nested_directories() -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    // Create deeply nested structure
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
            .map_err(|e| std::io::Error::other(format!("Failed to put {key}: {e}")))?;
    }
    println!("    Uploaded {} objects", keys.len());

    // Mount
    println!("  Step 2: Mount FUSE filesystem");
    mount_fuse(&bucket)?;

    // Walk the tree
    println!("  Step 3: Verify nested directory traversal");

    // Check a/ exists
    let a_path = format!("{}/a", MOUNT_POINT);
    if !Path::new(&a_path).is_dir() {
        unmount_fuse()?;
        cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;
        return Err(std::io::Error::other("a/ should be a directory"));
    }
    println!("    a/ is a directory: OK");

    // Check a/top.txt
    let top_path = format!("{}/a/top.txt", MOUNT_POINT);
    let content = std::fs::read_to_string(&top_path)
        .map_err(|e| std::io::Error::other(format!("Failed to read a/top.txt: {e}")))?;
    if content != "nested:a/top.txt" {
        unmount_fuse()?;
        cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;
        return Err(std::io::Error::other(format!(
            "a/top.txt content mismatch: '{content}'"
        )));
    }
    println!("    a/top.txt content: OK");

    // Check a/b/c/deep.txt
    let deep_path = format!("{}/a/b/c/deep.txt", MOUNT_POINT);
    let content = std::fs::read_to_string(&deep_path)
        .map_err(|e| std::io::Error::other(format!("Failed to read a/b/c/deep.txt: {e}")))?;
    if content != "nested:a/b/c/deep.txt" {
        unmount_fuse()?;
        cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;
        return Err(std::io::Error::other(format!(
            "a/b/c/deep.txt content mismatch: '{content}'"
        )));
    }
    println!("    a/b/c/deep.txt content: OK");

    // Check a/b/ listing
    let ab_path = format!("{}/a/b", MOUNT_POINT);
    let ab_entries: Vec<String> = std::fs::read_dir(&ab_path)
        .map_err(|e| std::io::Error::other(format!("Failed to list a/b/: {e}")))?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    println!("    a/b/ entries: {:?}", ab_entries);

    for expected in &["c", "sibling.txt"] {
        if !ab_entries.contains(&expected.to_string()) {
            unmount_fuse()?;
            cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;
            return Err(std::io::Error::other(format!(
                "Missing a/b/ entry: {expected}"
            )));
        }
    }

    // Cleanup
    unmount_fuse()?;
    cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;

    println!(
        "{}",
        "SUCCESS: Nested directory structure test passed".green()
    );
    Ok(())
}

// Phase 2: Write tests

async fn test_create_write_read() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

    // Step 2: Create and write files
    println!("  Step 2: Create and write files");
    let test_data = b"Hello from FUSE write!";
    let fuse_path = format!("{}/write-test.txt", MOUNT_POINT);
    std::fs::write(&fuse_path, test_data)
        .map_err(|e| std::io::Error::other(format!("Failed to write file: {e}")))?;
    println!("    Written: write-test.txt ({} bytes)", test_data.len());

    // Step 3: Read back and verify
    println!("  Step 3: Read back and verify");
    let read_back = std::fs::read(&fuse_path)
        .map_err(|e| std::io::Error::other(format!("Failed to read back: {e}")))?;
    if read_back != test_data {
        unmount_fuse()?;
        return Err(std::io::Error::other(format!(
            "Data mismatch: expected {} bytes, got {}",
            test_data.len(),
            read_back.len()
        )));
    }
    println!("    write-test.txt content: OK");

    // Step 4: Write a larger file
    println!("  Step 4: Write a larger file (64KB)");
    let large_data = generate_test_data("large-write", 64 * 1024);
    let large_path = format!("{}/large-write.bin", MOUNT_POINT);
    std::fs::write(&large_path, &large_data)
        .map_err(|e| std::io::Error::other(format!("Failed to write large file: {e}")))?;

    let large_read = std::fs::read(&large_path)
        .map_err(|e| std::io::Error::other(format!("Failed to read back large file: {e}")))?;
    if large_read != large_data {
        unmount_fuse()?;
        return Err(std::io::Error::other(format!(
            "Large file data mismatch: expected {} bytes, got {}",
            large_data.len(),
            large_read.len()
        )));
    }
    println!("    large-write.bin (64KB): OK");

    // Step 5: Verify file appears in directory listing
    println!("  Step 5: Verify files appear in listing");
    let entries: Vec<String> = std::fs::read_dir(MOUNT_POINT)
        .map_err(|e| std::io::Error::other(format!("Failed to list root: {e}")))?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    if !entries.contains(&"write-test.txt".to_string()) {
        unmount_fuse()?;
        return Err(std::io::Error::other("write-test.txt not found in listing"));
    }
    println!("    write-test.txt in listing: OK");

    unmount_fuse()?;
    println!("{}", "SUCCESS: Create/write/read test passed".green());
    Ok(())
}

async fn test_mkdir_rmdir() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

    // Create directory
    println!("  Step 2: Create directory");
    let dir_path = format!("{}/testdir", MOUNT_POINT);
    std::fs::create_dir(&dir_path)
        .map_err(|e| std::io::Error::other(format!("Failed to mkdir: {e}")))?;
    println!("    Created: testdir/");

    // Verify it appears
    if !Path::new(&dir_path).is_dir() {
        unmount_fuse()?;
        return Err(std::io::Error::other("testdir/ is not a directory"));
    }
    println!("    testdir/ is a directory: OK");

    // Rmdir
    println!("  Step 3: Remove empty directory");
    std::fs::remove_dir(&dir_path)
        .map_err(|e| std::io::Error::other(format!("Failed to rmdir: {e}")))?;
    println!("    Removed: testdir/");

    // Verify it's gone
    if Path::new(&dir_path).exists() {
        unmount_fuse()?;
        return Err(std::io::Error::other("testdir/ still exists after rmdir"));
    }
    println!("    testdir/ gone: OK");

    // Test non-empty rmdir should fail
    println!("  Step 4: Verify non-empty rmdir fails");
    let dir2_path = format!("{}/testdir2", MOUNT_POINT);
    std::fs::create_dir(&dir2_path)
        .map_err(|e| std::io::Error::other(format!("Failed to mkdir testdir2: {e}")))?;
    let file_in_dir = format!("{}/testdir2/file.txt", MOUNT_POINT);
    std::fs::write(&file_in_dir, b"content")
        .map_err(|e| std::io::Error::other(format!("Failed to write file in dir: {e}")))?;

    match std::fs::remove_dir(&dir2_path) {
        Err(e) if e.raw_os_error() == Some(39) => {
            // ENOTEMPTY = 39 on Linux
            println!("    Non-empty rmdir correctly returned ENOTEMPTY");
        }
        Err(e) => {
            println!(
                "    Non-empty rmdir returned error: {} (expected ENOTEMPTY)",
                e
            );
        }
        Ok(()) => {
            unmount_fuse()?;
            return Err(std::io::Error::other(
                "Non-empty rmdir should have failed but succeeded",
            ));
        }
    }

    unmount_fuse()?;
    println!("{}", "SUCCESS: Mkdir/rmdir test passed".green());
    Ok(())
}

async fn test_unlink() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

    // Create file
    println!("  Step 2: Create file then unlink");
    let file_path = format!("{}/to-delete.txt", MOUNT_POINT);
    std::fs::write(&file_path, b"delete me")
        .map_err(|e| std::io::Error::other(format!("Failed to write: {e}")))?;
    println!("    Created: to-delete.txt");

    // Verify exists
    if !Path::new(&file_path).exists() {
        unmount_fuse()?;
        return Err(std::io::Error::other("to-delete.txt should exist"));
    }

    // Unlink
    std::fs::remove_file(&file_path)
        .map_err(|e| std::io::Error::other(format!("Failed to unlink: {e}")))?;
    println!("    Unlinked: to-delete.txt");

    // Verify gone
    if Path::new(&file_path).exists() {
        unmount_fuse()?;
        return Err(std::io::Error::other(
            "to-delete.txt still exists after unlink",
        ));
    }
    println!("    to-delete.txt gone: OK");

    unmount_fuse()?;
    println!("{}", "SUCCESS: Unlink test passed".green());
    Ok(())
}

async fn test_rename() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

    // Create file
    println!("  Step 2: Create file and rename");
    let src_path = format!("{}/original.txt", MOUNT_POINT);
    let dst_path = format!("{}/renamed.txt", MOUNT_POINT);
    let content = b"rename me";
    std::fs::write(&src_path, content)
        .map_err(|e| std::io::Error::other(format!("Failed to write: {e}")))?;
    println!("    Created: original.txt");

    // Rename
    std::fs::rename(&src_path, &dst_path)
        .map_err(|e| std::io::Error::other(format!("Failed to rename: {e}")))?;
    println!("    Renamed: original.txt -> renamed.txt");

    // Verify old path gone
    if Path::new(&src_path).exists() {
        unmount_fuse()?;
        return Err(std::io::Error::other(
            "original.txt still exists after rename",
        ));
    }
    println!("    original.txt gone: OK");

    // Verify new path has correct content
    let read_back = std::fs::read(&dst_path)
        .map_err(|e| std::io::Error::other(format!("Failed to read renamed file: {e}")))?;
    if read_back != content {
        unmount_fuse()?;
        return Err(std::io::Error::other("renamed.txt content mismatch"));
    }
    println!("    renamed.txt content: OK");

    unmount_fuse()?;
    println!("{}", "SUCCESS: Rename test passed".green());
    Ok(())
}

async fn test_unlink_open_handle() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

    // Create file
    println!("  Step 2: Create file and open a read handle");
    let file_path = format!("{}/open-del.txt", MOUNT_POINT);
    let content = b"still readable after unlink";
    std::fs::write(&file_path, content)
        .map_err(|e| std::io::Error::other(format!("Failed to write: {e}")))?;
    println!("    Created: open-del.txt");

    // Open the file and keep the handle
    let mut file = std::fs::File::open(&file_path)
        .map_err(|e| std::io::Error::other(format!("Failed to open: {e}")))?;
    println!("    Opened read handle");

    // Unlink while fd is open
    println!("  Step 3: Unlink while handle is open");
    std::fs::remove_file(&file_path)
        .map_err(|e| std::io::Error::other(format!("Failed to unlink: {e}")))?;
    println!("    Unlinked: open-del.txt");

    // Verify path is gone
    if Path::new(&file_path).exists() {
        drop(file);
        unmount_fuse()?;
        return Err(std::io::Error::other(
            "open-del.txt still exists after unlink",
        ));
    }
    println!("    Path is gone (ENOENT): OK");

    // Read from still-open handle - blob data should still be accessible
    println!("  Step 4: Read from open handle after unlink");
    use std::io::Read;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .map_err(|e| std::io::Error::other(format!("Failed to read from open handle: {e}")))?;
    if buf != content {
        drop(file);
        unmount_fuse()?;
        return Err(std::io::Error::other(format!(
            "Content mismatch from open handle: expected {} bytes, got {}",
            content.len(),
            buf.len()
        )));
    }
    println!("    Read from open handle: OK ({} bytes)", buf.len());

    // Close handle (triggers deferred blob cleanup)
    drop(file);
    println!("    Closed handle (deferred cleanup triggered)");

    unmount_fuse()?;
    println!("{}", "SUCCESS: Unlink with open handle test passed".green());
    Ok(())
}

async fn test_overwrite_existing() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

    // Create original file
    println!("  Step 2: Create original file");
    let file_path = format!("{}/overwrite.txt", MOUNT_POINT);
    let original = b"0123456789";
    std::fs::write(&file_path, original)
        .map_err(|e| std::io::Error::other(format!("Failed to write original: {e}")))?;
    println!("    Created: overwrite.txt (10 bytes)");

    // Open for write without truncate, write at offset 3
    println!("  Step 3: Partial overwrite at offset 3");
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&file_path)
            .map_err(|e| std::io::Error::other(format!("Failed to open for write: {e}")))?;
        file.seek(SeekFrom::Start(3))
            .map_err(|e| std::io::Error::other(format!("Failed to seek: {e}")))?;
        file.write_all(b"XYZ")
            .map_err(|e| std::io::Error::other(format!("Failed to write: {e}")))?;
        file.flush()
            .map_err(|e| std::io::Error::other(format!("Failed to flush: {e}")))?;
    }
    println!("    Wrote 'XYZ' at offset 3");

    // Read back and verify
    println!("  Step 4: Verify merged content");
    let result = std::fs::read(&file_path)
        .map_err(|e| std::io::Error::other(format!("Failed to read back: {e}")))?;
    let expected = b"012XYZ6789";
    if result != expected {
        unmount_fuse()?;
        return Err(std::io::Error::other(format!(
            "Content mismatch: expected {:?}, got {:?}",
            String::from_utf8_lossy(expected),
            String::from_utf8_lossy(&result)
        )));
    }
    println!(
        "    Content after overwrite: OK ({:?})",
        String::from_utf8_lossy(&result)
    );

    unmount_fuse()?;
    println!("{}", "SUCCESS: Overwrite existing file test passed".green());
    Ok(())
}

/// Design doc test 7: rename when destination exists should return EEXIST.
async fn test_rename_noreplace() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

    // Create source and destination files
    println!("  Step 2: Create source and destination files");
    let src_path = format!("{}/rename-src.txt", MOUNT_POINT);
    let dst_path = format!("{}/rename-dst.txt", MOUNT_POINT);
    std::fs::write(&src_path, b"source content")
        .map_err(|e| std::io::Error::other(format!("Failed to write src: {e}")))?;
    std::fs::write(&dst_path, b"destination content")
        .map_err(|e| std::io::Error::other(format!("Failed to write dst: {e}")))?;
    println!("    Created: rename-src.txt and rename-dst.txt");

    // Rename should fail with EEXIST since destination exists
    println!("  Step 3: Rename with existing destination should return EEXIST");
    match std::fs::rename(&src_path, &dst_path) {
        Err(e) if e.raw_os_error() == Some(17) => {
            // EEXIST = 17 on Linux
            println!("    Rename correctly returned EEXIST");
        }
        Err(e) => {
            unmount_fuse()?;
            return Err(std::io::Error::other(format!(
                "Expected EEXIST, got error: {} (raw_os_error={:?})",
                e,
                e.raw_os_error()
            )));
        }
        Ok(()) => {
            unmount_fuse()?;
            return Err(std::io::Error::other(
                "Rename should have failed with EEXIST but succeeded",
            ));
        }
    }

    // Verify both files are unchanged
    println!("  Step 4: Verify both files are unchanged");
    let src_data = std::fs::read(&src_path)
        .map_err(|e| std::io::Error::other(format!("Failed to read src: {e}")))?;
    let dst_data = std::fs::read(&dst_path)
        .map_err(|e| std::io::Error::other(format!("Failed to read dst: {e}")))?;
    if src_data != b"source content" {
        unmount_fuse()?;
        return Err(std::io::Error::other("Source file content changed"));
    }
    if dst_data != b"destination content" {
        unmount_fuse()?;
        return Err(std::io::Error::other("Destination file content changed"));
    }
    println!("    Both files unchanged: OK");

    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: Rename no-replace (EEXIST) test passed".green()
    );
    Ok(())
}

/// Test O_TRUNC: writing to existing file with truncation replaces content.
async fn test_truncate_write() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

    // Create original file with some content
    println!("  Step 2: Create original file");
    let file_path = format!("{}/trunc.txt", MOUNT_POINT);
    std::fs::write(&file_path, b"original long content here")
        .map_err(|e| std::io::Error::other(format!("Failed to write original: {e}")))?;
    println!("    Created: trunc.txt (26 bytes)");

    // Overwrite with shorter content using std::fs::write (which uses O_TRUNC)
    println!("  Step 3: Overwrite with O_TRUNC (shorter content)");
    std::fs::write(&file_path, b"short")
        .map_err(|e| std::io::Error::other(format!("Failed to truncate-write: {e}")))?;
    println!("    Wrote 'short' with O_TRUNC");

    // Read back and verify only new content is present
    println!("  Step 4: Verify truncated content");
    let result = std::fs::read(&file_path)
        .map_err(|e| std::io::Error::other(format!("Failed to read back: {e}")))?;
    if result != b"short" {
        unmount_fuse()?;
        return Err(std::io::Error::other(format!(
            "Content mismatch: expected 'short', got {:?} ({} bytes)",
            String::from_utf8_lossy(&result),
            result.len()
        )));
    }
    println!(
        "    Content after truncate: OK ({:?})",
        String::from_utf8_lossy(&result)
    );

    unmount_fuse()?;
    println!("{}", "SUCCESS: Truncate write test passed".green());
    Ok(())
}

/// Test creating files inside a mkdir'd subdirectory.
async fn test_write_in_subdirectory() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

    // Create a subdirectory
    println!("  Step 2: Create subdirectory");
    let dir_path = format!("{}/subdir", MOUNT_POINT);
    std::fs::create_dir(&dir_path)
        .map_err(|e| std::io::Error::other(format!("Failed to mkdir: {e}")))?;
    println!("    Created: subdir/");

    // Write files into subdirectory
    println!("  Step 3: Write files into subdirectory");
    let file1 = format!("{}/subdir/file1.txt", MOUNT_POINT);
    let file2 = format!("{}/subdir/file2.txt", MOUNT_POINT);
    std::fs::write(&file1, b"content one")
        .map_err(|e| std::io::Error::other(format!("Failed to write file1: {e}")))?;
    std::fs::write(&file2, b"content two")
        .map_err(|e| std::io::Error::other(format!("Failed to write file2: {e}")))?;
    println!("    Written: subdir/file1.txt and subdir/file2.txt");

    // Read back and verify
    println!("  Step 4: Read back and verify");
    let data1 = std::fs::read(&file1)
        .map_err(|e| std::io::Error::other(format!("Failed to read file1: {e}")))?;
    let data2 = std::fs::read(&file2)
        .map_err(|e| std::io::Error::other(format!("Failed to read file2: {e}")))?;
    if data1 != b"content one" || data2 != b"content two" {
        unmount_fuse()?;
        return Err(std::io::Error::other("Subdirectory file content mismatch"));
    }
    println!("    Content verified: OK");

    // Remount to clear all kernel and FUSE-side caches, then verify listing
    println!("  Step 5: Remount and verify subdirectory listing");
    unmount_fuse()?;
    mount_fuse_rw(&bucket)?;
    let entries: Vec<String> = std::fs::read_dir(&dir_path)
        .map_err(|e| std::io::Error::other(format!("Failed to list subdir: {e}")))?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    println!("    Listing: {:?}", entries);
    for expected in &["file1.txt", "file2.txt"] {
        if !entries.contains(&expected.to_string()) {
            unmount_fuse()?;
            return Err(std::io::Error::other(format!(
                "Missing entry in subdir listing: {expected}"
            )));
        }
    }

    unmount_fuse()?;
    println!("{}", "SUCCESS: Write in subdirectory test passed".green());
    Ok(())
}

/// Test renaming a directory (rename_folder path).
async fn test_rename_directory() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

    // Create directory with files
    println!("  Step 2: Create directory with files");
    let src_dir = format!("{}/srcdir", MOUNT_POINT);
    std::fs::create_dir(&src_dir)
        .map_err(|e| std::io::Error::other(format!("Failed to mkdir srcdir: {e}")))?;
    let child_file = format!("{}/srcdir/child.txt", MOUNT_POINT);
    std::fs::write(&child_file, b"child content")
        .map_err(|e| std::io::Error::other(format!("Failed to write child: {e}")))?;
    println!("    Created: srcdir/child.txt");

    // Rename directory
    println!("  Step 3: Rename directory");
    let dst_dir = format!("{}/dstdir", MOUNT_POINT);
    std::fs::rename(&src_dir, &dst_dir)
        .map_err(|e| std::io::Error::other(format!("Failed to rename dir: {e}")))?;
    println!("    Renamed: srcdir/ -> dstdir/");

    // Verify old path gone
    if Path::new(&src_dir).exists() {
        unmount_fuse()?;
        return Err(std::io::Error::other("srcdir still exists after rename"));
    }
    println!("    srcdir/ gone: OK");

    // Verify child accessible at new path
    println!("  Step 4: Verify child at new path");
    let new_child = format!("{}/dstdir/child.txt", MOUNT_POINT);
    let data = std::fs::read(&new_child)
        .map_err(|e| std::io::Error::other(format!("Failed to read dstdir/child.txt: {e}")))?;
    if data != b"child content" {
        unmount_fuse()?;
        return Err(std::io::Error::other("dstdir/child.txt content mismatch"));
    }
    println!("    dstdir/child.txt content: OK");

    unmount_fuse()?;
    println!("{}", "SUCCESS: Rename directory test passed".green());
    Ok(())
}
