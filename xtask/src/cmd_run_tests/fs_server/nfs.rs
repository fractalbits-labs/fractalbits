use crate::cmd_service;
use crate::{CmdResult, ServiceName};
use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use colored::*;
use std::time::Duration;

use super::{cleanup_objects, generate_test_data, setup_test_bucket};

const NFS_MOUNT_POINT: &str = "/tmp/nfs_server_test";

fn write_nfs_server_env(bucket: &str, read_write: bool) -> CmdResult {
    let env_content =
        format!("FUSE_BUCKET_NAME={bucket}\nFS_SERVER_MODE=nfs\nFUSE_READ_WRITE={read_write}\n");
    run_cmd!(mkdir -p data/etc)?;
    std::fs::write("data/etc/fs_server.env", env_content)?;
    Ok(())
}

fn mount_nfs(bucket: &str) -> CmdResult {
    mount_nfs_with_opts(bucket, false)
}

#[allow(dead_code)]
fn mount_nfs_rw(bucket: &str) -> CmdResult {
    mount_nfs_with_opts(bucket, true)
}

fn mount_nfs_with_opts(bucket: &str, read_write: bool) -> CmdResult {
    let mount_point = NFS_MOUNT_POINT;

    run_cmd!(mkdir -p $mount_point)?;

    write_nfs_server_env(bucket, read_write)?;
    cmd_service::init_service(
        ServiceName::FsServer,
        crate::cmd_build::BuildMode::Debug,
        crate::InitConfig::default(),
    )?;
    cmd_service::start_service(ServiceName::FsServer)?;

    // Wait for NFS server to start listening
    std::thread::sleep(Duration::from_secs(2));

    // Mount via NFS (port/mountport bypass rpcbind; our server handles both on one port)
    if let Err(e) = run_cmd!(
        sudo mount -t nfs -o vers=3,tcp,nolock,soft,timeo=50,port=2049,mountport=2049 "localhost:/" $mount_point
    ) {
        let _ = cmd_service::stop_service(ServiceName::FsServer);
        return Err(e);
    }

    // Verify mount
    for i in 0..10 {
        std::thread::sleep(Duration::from_millis(500));
        let status = std::process::Command::new("mountpoint")
            .arg("-q")
            .arg(mount_point)
            .status();
        if let Ok(s) = status
            && s.success()
        {
            println!(
                "    NFS mounted at {} (after {}ms)",
                mount_point,
                2000 + (i + 1) * 500
            );
            return Ok(());
        }
    }

    let _ = run_cmd!(sudo umount $mount_point 2>/dev/null);
    let _ = cmd_service::stop_service(ServiceName::FsServer);
    Err(std::io::Error::other(format!(
        "NFS mount at {} not ready after 7 seconds",
        mount_point
    )))
}

fn unmount_nfs() -> CmdResult {
    let mount_point = NFS_MOUNT_POINT;
    let _ = run_cmd!(sudo umount $mount_point 2>/dev/null);
    let _ = cmd_service::stop_service(ServiceName::FsServer);
    let _ = run_cmd!(pkill -f "fs_server" 2>/dev/null);
    std::thread::sleep(Duration::from_millis(500));
    Ok(())
}

pub async fn run_nfs_tests() -> CmdResult {
    info!("Running NFS integration tests...");

    println!("\n{}", "=== NFS Test: Basic File Read ===".bold());
    if let Err(e) = test_nfs_basic_file_read().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== NFS Test: Directory Listing ===".bold());
    if let Err(e) = test_nfs_directory_listing().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== NFS Test: Large File Read ===".bold());
    if let Err(e) = test_nfs_large_file_read().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== All NFS Tests PASSED ===".green().bold());
    Ok(())
}

async fn test_nfs_basic_file_read() -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload test objects via S3 API");
    let test_files: Vec<(&str, Vec<u8>)> = vec![
        ("hello.txt", b"Hello, NFS!".to_vec()),
        ("numbers.dat", b"0123456789".to_vec()),
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

    println!("  Step 2: Mount NFS filesystem");
    mount_nfs(&bucket)?;

    println!("  Step 3: Read and verify files");
    let mut passed = 0;
    let mut failed = 0;

    for (key, expected_data) in &test_files {
        let nfs_path = format!("{}/{}", NFS_MOUNT_POINT, key);
        match std::fs::read(&nfs_path) {
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

    unmount_nfs()?;
    cleanup_objects(
        &ctx,
        &bucket,
        &test_files.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
    )
    .await;

    if failed > 0 {
        return Err(std::io::Error::other(format!(
            "{} of {} NFS file reads failed",
            failed,
            passed + failed
        )));
    }

    println!("{}", "SUCCESS: NFS basic file read test passed".green());
    Ok(())
}

async fn test_nfs_directory_listing() -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Upload objects with directory structure");
    let keys = vec![
        "top-level.txt",
        "docs/readme.md",
        "docs/guide.md",
        "src/main.rs",
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

    println!("  Step 2: Mount NFS filesystem");
    mount_nfs(&bucket)?;

    println!("  Step 3: Verify root directory listing");
    let root_entries: Vec<String> = std::fs::read_dir(NFS_MOUNT_POINT)
        .map_err(|e| std::io::Error::other(format!("Failed to list root: {e}")))?
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    println!("    Root entries: {:?}", root_entries);

    let expected_root = vec!["top-level.txt", "docs", "src"];
    for expected in &expected_root {
        if !root_entries.contains(&expected.to_string()) {
            unmount_nfs()?;
            cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;
            return Err(std::io::Error::other(format!(
                "Missing root entry: {expected}"
            )));
        }
        println!("    Found: {}", expected);
    }

    println!("  Step 4: Verify file content");
    let readme_path = format!("{}/docs/readme.md", NFS_MOUNT_POINT);
    let content = std::fs::read_to_string(&readme_path)
        .map_err(|e| std::io::Error::other(format!("Failed to read docs/readme.md: {e}")))?;
    if content != "content of docs/readme.md" {
        unmount_nfs()?;
        cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;
        return Err(std::io::Error::other(format!(
            "Content mismatch for docs/readme.md: got '{content}'"
        )));
    }
    println!("    docs/readme.md content: OK");

    unmount_nfs()?;
    cleanup_objects(&ctx, &bucket, &keys.to_vec()).await;

    println!("{}", "SUCCESS: NFS directory listing test passed".green());
    Ok(())
}

async fn test_nfs_large_file_read() -> CmdResult {
    let (ctx, bucket) = setup_test_bucket().await;

    let sizes: Vec<(&str, usize)> = vec![
        ("small-4k", 4 * 1024),
        ("medium-512k", 512 * 1024),
        ("large-2mb", 2 * 1024 * 1024),
    ];

    println!("  Step 1: Upload large test objects");
    let mut upload_keys = Vec::new();
    for (label, size) in &sizes {
        let key = format!("nfs-large-{label}");
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

    println!("  Step 2: Mount NFS filesystem");
    mount_nfs(&bucket)?;

    println!("  Step 3: Read and verify large files");
    let mut passed = 0;
    let mut failed = 0;

    for (i, (label, size)) in sizes.iter().enumerate() {
        let key = &upload_keys[i];
        let expected_data = generate_test_data(key, *size);
        let nfs_path = format!("{}/{}", NFS_MOUNT_POINT, key);

        match std::fs::read(&nfs_path) {
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

    unmount_nfs()?;
    let key_refs: Vec<&str> = upload_keys.iter().map(|k| k.as_str()).collect();
    cleanup_objects(&ctx, &bucket, &key_refs).await;

    if failed > 0 {
        return Err(std::io::Error::other(format!(
            "{} of {} NFS large file reads failed",
            failed,
            passed + failed
        )));
    }

    println!("{}", "SUCCESS: NFS large file read test passed".green());
    Ok(())
}
