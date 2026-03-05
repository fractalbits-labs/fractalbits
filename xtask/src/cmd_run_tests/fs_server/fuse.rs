use crate::cmd_service;
use crate::{CmdResult, ServiceName};
use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use colored::*;
use std::path::Path;
use std::time::Duration;

use super::{MOUNT_POINT, cleanup_objects, generate_test_data, setup_test_bucket};

fn write_fs_server_env(bucket: &str, mount_point: &str, read_write: bool) -> CmdResult {
    let env_content = format!(
        "FS_SERVER_BUCKET_NAME={bucket}\nFS_SERVER_MOUNT_POINT={mount_point}\nFS_SERVER_READ_WRITE={read_write}\n"
    );
    run_cmd! {
        mkdir -p data/etc;
        echo $env_content > data/etc/fs_server.env;
    }
}

pub fn mount_fuse_ro(bucket: &str) -> CmdResult {
    mount_fuse_with_opts(bucket, false)
}

fn mount_fuse_rw(bucket: &str) -> CmdResult {
    mount_fuse_with_opts(bucket, true)
}

fn mount_fuse_with_opts(bucket: &str, read_write: bool) -> CmdResult {
    let mount_point = MOUNT_POINT;

    run_cmd!(mkdir -p $mount_point)?;
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

pub fn unmount_fuse() -> CmdResult {
    let mount_point = MOUNT_POINT;
    run_cmd! {
        ignore fusermount3 -u $mount_point 2>/dev/null;
        ignore fusermount -u $mount_point 2>/dev/null;
    }?;
    let _ = cmd_service::stop_service(ServiceName::FsServer);
    run_cmd! { ignore pkill -f "fs_server" 2>/dev/null; }?;
    std::thread::sleep(Duration::from_millis(500));
    Ok(())
}

pub async fn run_fuse_tests() -> CmdResult {
    info!("Running FUSE integration tests...");

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

    println!("\n{}", "=== Test: Nested Directory Structure ===".bold());
    if let Err(e) = test_nested_directories().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Create, Write, Read ===".bold());
    if let Err(e) = test_create_write_read().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Large File Write ===".bold());
    if let Err(e) = test_large_file_write().await {
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

    println!("\n{}", "=== Test: Unlink with Open Handle ===".bold());
    if let Err(e) = test_unlink_open_handle().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Overwrite Existing File ===".bold());
    if let Err(e) = test_overwrite_existing().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Rename No-Replace (EEXIST) ===".bold());
    if let Err(e) = test_rename_noreplace().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Truncate Write ===".bold());
    if let Err(e) = test_truncate_write().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Write in Subdirectory ===".bold());
    if let Err(e) = test_write_in_subdirectory().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Rename Directory ===".bold());
    if let Err(e) = test_rename_directory().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: dd + fsync Write ===".bold());
    if let Err(e) = test_dd_fsync().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: mmap Write ===".bold());
    if let Err(e) = test_mmap_write().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Fsync Persistence ===".bold());
    if let Err(e) = test_fsync_persistence().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test: Truncate to Non-Zero Size ===".bold());
    if let Err(e) = test_truncate_nonzero().await {
        eprintln!("{}: {}", "Test FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== All FUSE Tests PASSED ===".green().bold());
    Ok(())
}

async fn test_basic_file_read() -> CmdResult {
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
    mount_fuse_ro(&bucket)?;

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

async fn test_directory_listing() -> CmdResult {
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
    mount_fuse_ro(&bucket)?;

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

async fn test_large_file_read() -> CmdResult {
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
    mount_fuse_ro(&bucket)?;

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

async fn test_nested_directories() -> CmdResult {
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
    mount_fuse_ro(&bucket)?;

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

async fn test_create_write_read() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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

async fn test_large_file_write() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    let sizes: Vec<(&str, usize)> = vec![
        ("small-4k", 4 * 1024),
        ("medium-512k", 512 * 1024),
        ("large-2mb", 2 * 1024 * 1024),
    ];

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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

async fn test_mkdir_rmdir() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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

async fn test_unlink() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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

async fn test_rename() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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

async fn test_unlink_open_handle() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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

async fn test_overwrite_existing() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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

async fn test_rename_noreplace() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

    println!("  Step 2: Create source and destination files");
    let src_path = format!("{}/rename-src.txt", MOUNT_POINT);
    let dst_path = format!("{}/rename-dst.txt", MOUNT_POINT);
    std::fs::write(&src_path, b"source content").expect("Failed to write src");
    std::fs::write(&dst_path, b"destination content").expect("Failed to write dst");
    println!("    Created: rename-src.txt and rename-dst.txt");

    println!("  Step 3: Rename with existing destination should return EEXIST");
    let err =
        std::fs::rename(&src_path, &dst_path).expect_err("Rename should have failed with EEXIST");
    assert_eq!(err.raw_os_error(), Some(17), "Expected EEXIST");
    println!("    Rename correctly returned EEXIST");

    println!("  Step 4: Verify both files are unchanged");
    let src_data = std::fs::read(&src_path).expect("Failed to read src");
    let dst_data = std::fs::read(&dst_path).expect("Failed to read dst");
    assert_eq!(src_data, b"source content", "Source file content changed");
    assert_eq!(
        dst_data, b"destination content",
        "Destination file content changed"
    );
    println!("    Both files unchanged: OK");

    unmount_fuse()?;
    println!(
        "{}",
        "SUCCESS: Rename no-replace (EEXIST) test passed".green()
    );
    Ok(())
}

async fn test_truncate_write() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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

async fn test_write_in_subdirectory() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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
    mount_fuse_rw(&bucket)?;
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

async fn test_rename_directory() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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
async fn test_dd_fsync() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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
    mount_fuse_rw(&bucket)?;

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
async fn test_mmap_write() -> CmdResult {
    use std::os::unix::io::AsRawFd;

    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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
    mount_fuse_rw(&bucket)?;

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
async fn test_fsync_persistence() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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
    mount_fuse_rw(&bucket)?;

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
    mount_fuse_rw(&bucket)?;

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

/// Test truncating a file to non-zero sizes (shrink and extend).
async fn test_truncate_nonzero() -> CmdResult {
    let (_ctx, bucket) = setup_test_bucket().await;

    println!("  Step 1: Mount FUSE in read-write mode");
    mount_fuse_rw(&bucket)?;

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
    mount_fuse_rw(&bucket)?;

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
