use crate::cmd_service;
use crate::{CmdResult, ServiceName};
use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use colored::*;
use std::path::Path;
use std::time::Duration;

use super::{cleanup_objects, generate_test_data, setup_test_bucket};

const NFS_MOUNT_POINT: &str = "/tmp/nfs_server_test";

fn write_nfs_server_env(bucket: &str, read_write: bool) -> CmdResult {
    let env_content = format!(
        "FS_SERVER_BUCKET_NAME={bucket}\nFS_SERVER_MODE=nfs\nFS_SERVER_READ_WRITE={read_write}\n"
    );
    run_cmd!(mkdir -p data/etc)?;
    std::fs::write("data/etc/fs_server.env", env_content)?;
    Ok(())
}

fn mount_nfs(bucket: &str) -> CmdResult {
    mount_nfs_with_opts(bucket, false)
}

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

    run_cmd! { ignore sudo umount $mount_point 2>/dev/null; }?;
    let _ = cmd_service::stop_service(ServiceName::FsServer);
    Err(std::io::Error::other(format!(
        "NFS mount at {} not ready after 7 seconds",
        mount_point
    )))
}

fn unmount_nfs() -> CmdResult {
    let mount_point = NFS_MOUNT_POINT;
    run_cmd! { ignore sudo umount $mount_point 2>/dev/null; }?;
    let _ = cmd_service::stop_service(ServiceName::FsServer);
    run_cmd! { ignore pkill -x fs_server 2>/dev/null; }?;
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

    println!("\n{}", "=== NFS Test: Create, Write, Read ===".bold());
    if let Err(e) = test_nfs_create_write_read().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== NFS Test: Large File Write ===".bold());
    if let Err(e) = test_nfs_large_file_write().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== NFS Test: Mkdir and Rmdir ===".bold());
    if let Err(e) = test_nfs_mkdir_rmdir().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== NFS Test: Unlink ===".bold());
    if let Err(e) = test_nfs_unlink().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== NFS Test: Rename ===".bold());
    if let Err(e) = test_nfs_rename().await {
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
            .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));
        println!("    Uploaded: {} ({} bytes)", key, data.len());
    }

    println!("  Step 2: Mount NFS filesystem");
    mount_nfs(&bucket)?;

    println!("  Step 3: Read and verify files");
    for (key, expected_data) in &test_files {
        let nfs_path = format!("{}/{}", NFS_MOUNT_POINT, key);
        let actual_data =
            std::fs::read(&nfs_path).unwrap_or_else(|e| panic!("Failed to read {key}: {e}"));
        assert_eq!(actual_data, *expected_data, "{key}: data mismatch");
        println!("    {}: OK ({} bytes)", key, actual_data.len());
    }

    unmount_nfs()?;
    cleanup_objects(
        &ctx,
        &bucket,
        &test_files.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
    )
    .await;

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
            .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));
    }
    println!("    Uploaded {} objects", keys.len());

    println!("  Step 2: Mount NFS filesystem");
    mount_nfs(&bucket)?;

    println!("  Step 3: Verify root directory listing");
    let root_entries: Vec<String> = std::fs::read_dir(NFS_MOUNT_POINT)
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

    println!("  Step 4: Verify file content");
    let readme_path = format!("{}/docs/readme.md", NFS_MOUNT_POINT);
    let content = std::fs::read_to_string(&readme_path).expect("Failed to read docs/readme.md");
    assert_eq!(
        content, "content of docs/readme.md",
        "Content mismatch for docs/readme.md"
    );
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
            .unwrap_or_else(|e| panic!("Failed to put {key}: {e}"));
        upload_keys.push(key);
        println!("    Uploaded: {} ({} bytes)", label, size);
    }

    println!("  Step 2: Mount NFS filesystem");
    mount_nfs(&bucket)?;

    println!("  Step 3: Read and verify large files");
    for (i, (label, size)) in sizes.iter().enumerate() {
        let key = &upload_keys[i];
        let expected_data = generate_test_data(key, *size);
        let nfs_path = format!("{}/{}", NFS_MOUNT_POINT, key);
        let actual_data =
            std::fs::read(&nfs_path).unwrap_or_else(|e| panic!("Failed to read {key}: {e}"));
        assert_eq!(actual_data, expected_data, "{label}: data mismatch");
        println!("    {}: OK ({} bytes)", label, actual_data.len());
    }

    unmount_nfs()?;
    let key_refs: Vec<&str> = upload_keys.iter().map(|k| k.as_str()).collect();
    cleanup_objects(&ctx, &bucket, &key_refs).await;

    println!("{}", "SUCCESS: NFS large file read test passed".green());
    Ok(())
}

async fn test_nfs_create_write_read() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount NFS in read-write mode");
    mount_nfs_rw(&bucket)?;

    println!("  Step 2: Create and write files");
    let test_data = b"Hello from NFS write!";
    let nfs_path = format!("{}/nfs-write-test.txt", NFS_MOUNT_POINT);
    std::fs::write(&nfs_path, test_data).expect("Failed to write file");
    println!(
        "    Written: nfs-write-test.txt ({} bytes)",
        test_data.len()
    );

    println!("  Step 3: Read back and verify");
    let read_back = std::fs::read(&nfs_path).expect("Failed to read back");
    assert_eq!(read_back, test_data, "nfs-write-test.txt data mismatch");
    println!("    nfs-write-test.txt content: OK");

    println!("  Step 4: Write a larger file (64KB)");
    let large_data = generate_test_data("nfs-large-write", 64 * 1024);
    let large_path = format!("{}/nfs-large-write.bin", NFS_MOUNT_POINT);
    std::fs::write(&large_path, &large_data).expect("Failed to write large file");

    let large_read = std::fs::read(&large_path).expect("Failed to read back large file");
    assert_eq!(large_read, large_data, "nfs-large-write.bin data mismatch");
    println!("    nfs-large-write.bin (64KB): OK");

    println!("  Step 5: Verify files appear in listing");
    let entries: Vec<String> = std::fs::read_dir(NFS_MOUNT_POINT)
        .expect("Failed to list root")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert!(
        entries.contains(&"nfs-write-test.txt".to_string()),
        "nfs-write-test.txt not found in listing"
    );
    println!("    nfs-write-test.txt in listing: OK");

    unmount_nfs()?;
    println!("{}", "SUCCESS: NFS create/write/read test passed".green());
    Ok(())
}

async fn test_nfs_large_file_write() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    let sizes: Vec<(&str, usize)> = vec![
        ("small-4k", 4 * 1024),
        ("medium-512k", 512 * 1024),
        ("large-2mb", 2 * 1024 * 1024),
    ];

    println!("  Step 1: Mount NFS in read-write mode");
    mount_nfs_rw(&bucket)?;

    println!("  Step 2: Write large files via NFS");
    let mut keys = Vec::new();
    for (label, size) in &sizes {
        let key = format!("nfs-write-{label}");
        let data = generate_test_data(&key, *size);
        let nfs_path = format!("{}/{}", NFS_MOUNT_POINT, key);
        std::fs::write(&nfs_path, &data).unwrap_or_else(|e| panic!("Failed to write {key}: {e}"));
        keys.push((key, data));
        println!("    Written: {} ({} bytes)", label, size);
    }

    println!("  Step 3: Read back and verify");
    for (i, (label, _)) in sizes.iter().enumerate() {
        let (key, expected_data) = &keys[i];
        let nfs_path = format!("{}/{}", NFS_MOUNT_POINT, key);
        let actual_data =
            std::fs::read(&nfs_path).unwrap_or_else(|e| panic!("Failed to read {key}: {e}"));
        assert_eq!(actual_data, *expected_data, "{label}: data mismatch");
        println!("    {}: OK ({} bytes)", label, actual_data.len());
    }

    unmount_nfs()?;

    println!("{}", "SUCCESS: NFS large file write test passed".green());
    Ok(())
}

async fn test_nfs_mkdir_rmdir() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount NFS in read-write mode");
    mount_nfs_rw(&bucket)?;

    println!("  Step 2: Create directory");
    let dir_path = format!("{}/nfs-testdir", NFS_MOUNT_POINT);
    std::fs::create_dir(&dir_path).expect("Failed to mkdir");
    println!("    Created: nfs-testdir/");

    assert!(
        Path::new(&dir_path).is_dir(),
        "nfs-testdir/ is not a directory"
    );
    println!("    nfs-testdir/ is a directory: OK");

    println!("  Step 3: Create file in directory");
    let file_path = format!("{}/nfs-testdir/file.txt", NFS_MOUNT_POINT);
    std::fs::write(&file_path, b"content in dir").expect("Failed to write file in dir");
    println!("    Created: nfs-testdir/file.txt");

    println!("  Step 4: Remove file then directory");
    std::fs::remove_file(&file_path).expect("Failed to unlink file");
    println!("    Removed: nfs-testdir/file.txt");

    std::fs::remove_dir(&dir_path).expect("Failed to rmdir");
    println!("    Removed: nfs-testdir/");

    assert!(
        !Path::new(&dir_path).exists(),
        "nfs-testdir/ still exists after rmdir"
    );
    println!("    nfs-testdir/ gone: OK");

    unmount_nfs()?;
    println!("{}", "SUCCESS: NFS mkdir/rmdir test passed".green());
    Ok(())
}

async fn test_nfs_unlink() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount NFS in read-write mode");
    mount_nfs_rw(&bucket)?;

    println!("  Step 2: Create file then unlink");
    let file_path = format!("{}/nfs-to-delete.txt", NFS_MOUNT_POINT);
    std::fs::write(&file_path, b"delete me via NFS").expect("Failed to write");
    println!("    Created: nfs-to-delete.txt");

    assert!(
        Path::new(&file_path).exists(),
        "nfs-to-delete.txt should exist"
    );

    std::fs::remove_file(&file_path).expect("Failed to unlink");
    println!("    Unlinked: nfs-to-delete.txt");

    assert!(
        !Path::new(&file_path).exists(),
        "nfs-to-delete.txt still exists after unlink"
    );
    println!("    nfs-to-delete.txt gone: OK");

    unmount_nfs()?;
    println!("{}", "SUCCESS: NFS unlink test passed".green());
    Ok(())
}

async fn test_nfs_rename() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount NFS in read-write mode");
    mount_nfs_rw(&bucket)?;

    println!("  Step 2: Create file and rename");
    let src_path = format!("{}/nfs-original.txt", NFS_MOUNT_POINT);
    let dst_path = format!("{}/nfs-renamed.txt", NFS_MOUNT_POINT);
    let content = b"rename me via NFS";
    std::fs::write(&src_path, content).expect("Failed to write");
    println!("    Created: nfs-original.txt");

    std::fs::rename(&src_path, &dst_path).expect("Failed to rename");
    println!("    Renamed: nfs-original.txt -> nfs-renamed.txt");

    assert!(
        !Path::new(&src_path).exists(),
        "nfs-original.txt still exists after rename"
    );
    println!("    nfs-original.txt gone: OK");

    let read_back = std::fs::read(&dst_path).expect("Failed to read renamed file");
    assert_eq!(read_back, content, "nfs-renamed.txt content mismatch");
    println!("    nfs-renamed.txt content: OK");

    unmount_nfs()?;
    println!("{}", "SUCCESS: NFS rename test passed".green());
    Ok(())
}
