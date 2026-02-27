use crate::CmdResult;
use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use colored::*;
use std::path::Path;
use std::time::Duration;
use test_common::*;

const MOUNT_POINT: &str = "/tmp/fuse_client_test";
const BUCKET_NAME: &str = "test-fuse";

pub async fn run_fuse_client_tests() -> CmdResult {
    info!("Running FUSE client integration tests...");

    println!("\n{}", "=== Test: Basic File Read via FUSE ===".bold());
    if let Err(e) = test_basic_file_read().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Directory Listing via FUSE ===".bold());
    if let Err(e) = test_directory_listing().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Large File Read via FUSE ===".bold());
    if let Err(e) = test_large_file_read().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== Test: Nested Directory Structure via FUSE ===".bold()
    );
    if let Err(e) = test_nested_directories().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== All FUSE Client Tests PASSED ===".green().bold()
    );
    Ok(())
}

fn fuse_binary_path() -> String {
    format!(
        "{}/target/debug/fuse_client",
        env!("CARGO_MANIFEST_DIR").trim_end_matches("/xtask")
    )
}

fn mount_fuse(bucket: &str) -> CmdResult {
    let binary = fuse_binary_path();
    let mount_point = MOUNT_POINT;

    // Create mount point directory
    run_cmd!(mkdir -p $mount_point)?;

    // Start fuse_client as a background process
    std::process::Command::new(&binary)
        .args(["-b", bucket, "-m", mount_point])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|e| std::io::Error::other(format!("Failed to spawn fuse_client: {e}")))?;

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
    // Kill any remaining fuse_client processes
    let _ = run_cmd!(pkill -f "fuse_client.*test-fuse" 2>/dev/null);
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

    // Step 3: Read and verify files via FUSE mount
    println!("  Step 3: Read and verify files via FUSE mount");
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
