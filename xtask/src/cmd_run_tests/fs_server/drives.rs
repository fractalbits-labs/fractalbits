//! Drive CRUD end to end through both protocols: create over `/v1`,
//! write over S3, mount, subtree mounts, force delete.

use crate::cmd_build::BuildMode;
use crate::{CmdResult, FsMountConfig};
use aws_sdk_s3::primitives::ByteStream;
use colored::*;
use serde_json::json;
use std::time::Duration;
use test_common::mgmt::MgmtClient;
use test_common::{Context, context};

use super::fuse::{MOUNT_POINT_B, spawn_second_fuse_at, stop_second_fuse};
use super::{MOUNT_POINT, ensure_gateway, gateway_config, mount_fs, unmount_fs};

const DRIVE: &str = "drv-e2e";

fn mount_cfg(read_write: bool, prefix: &str) -> FsMountConfig {
    FsMountConfig {
        bucket_name: DRIVE.to_string(),
        mount_point: MOUNT_POINT.to_string(),
        read_write,
        prefix: prefix.to_string(),
        ..Default::default()
    }
}

async fn put(ctx: &Context, key: &str, body: &'static [u8]) -> CmdResult {
    ctx.client
        .put_object()
        .bucket(DRIVE)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .map_err(|e| std::io::Error::other(format!("put {key}: {e}")))?;
    Ok(())
}

async fn head(ctx: &Context, key: &str) -> bool {
    ctx.client
        .head_object()
        .bucket(DRIVE)
        .key(key)
        .send()
        .await
        .is_ok()
}

fn names(dir: &str) -> std::io::Result<Vec<String>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        out.push(entry?.file_name().to_string_lossy().to_string());
    }
    out.sort();
    Ok(out)
}

pub async fn run_drive_e2e(disk_cache: bool) -> CmdResult {
    println!("\n{}", "=== Test: Drive CRUD end to end ===".bold());
    let mgmt = MgmtClient::new();
    let ctx = context();
    ensure_gateway(BuildMode::Debug, &gateway_config(disk_cache, "", 1))?;

    println!("  Step 0: Clean slate");
    let _ = mgmt.delete_drive(DRIVE, true).await;
    assert!(
        mgmt.wait_deleted(DRIVE, Duration::from_secs(60)).await,
        "previous drive gone"
    );

    println!("  Step 1: Create the drive over /v1");
    let (status, body) = mgmt
        .create_drive(DRIVE, json!({ "suite": "fs-server" }))
        .await;
    assert_eq!(status.as_u16(), 200, "create: {body}");

    println!("  Step 2: Put over S3, read through a read-write mount, write back");
    put(&ctx, "in/hello.txt", b"from s3").await?;
    mount_fs(BuildMode::Debug, &mount_cfg(true, ""))?;
    assert_eq!(
        std::fs::read(format!("{MOUNT_POINT}/in/hello.txt"))?,
        b"from s3",
        "S3 object visible through the mount"
    );
    std::fs::create_dir_all(format!("{MOUNT_POINT}/out"))?;
    std::fs::write(format!("{MOUNT_POINT}/out/result.txt"), b"from fuse")?;
    unmount_fs(MOUNT_POINT)?;
    assert!(
        head(&ctx, "out/result.txt").await,
        "mount write visible over S3"
    );

    println!("  Step 3: Read-only mount refuses writes");
    mount_fs(BuildMode::Debug, &mount_cfg(false, ""))?;
    let err = std::fs::write(format!("{MOUNT_POINT}/out/nope.txt"), b"x")
        .expect_err("write on a read-only mount");
    assert_eq!(err.raw_os_error(), Some(libc::EROFS), "EROFS: {err}");
    unmount_fs(MOUNT_POINT)?;

    println!("  Step 4: Two subtree mounts, each sees only its own");
    mount_fs(BuildMode::Debug, &mount_cfg(true, "/agents/a/"))?;
    let child_b = spawn_second_fuse_at(DRIVE, true, "/agents/b/")?;
    std::fs::write(format!("{MOUNT_POINT}/a.txt"), b"a")?;
    std::fs::write(format!("{MOUNT_POINT_B}/b.txt"), b"b")?;
    assert_eq!(names(MOUNT_POINT)?, vec!["a.txt"], "a sees only a");
    assert_eq!(names(MOUNT_POINT_B)?, vec!["b.txt"], "b sees only b");
    stop_second_fuse(child_b);
    unmount_fs(MOUNT_POINT)?;
    assert!(
        head(&ctx, "agents/a/a.txt").await,
        "a at its full path over S3"
    );
    assert!(
        head(&ctx, "agents/b/b.txt").await,
        "b at its full path over S3"
    );

    println!("  Step 5: Force delete, then a fresh mount is refused");
    let (status, body) = mgmt.delete_drive(DRIVE, true).await;
    assert_eq!(status.as_u16(), 202, "force delete: {body}");
    assert!(
        mgmt.wait_deleted(DRIVE, Duration::from_secs(120)).await,
        "force delete finished"
    );
    assert!(!head(&ctx, "in/hello.txt").await, "objects gone over S3");
    assert!(
        mount_fs(BuildMode::Debug, &mount_cfg(true, "")).is_err(),
        "mount of a deleted drive fails"
    );
    let _ = unmount_fs(MOUNT_POINT);

    println!("{}", "SUCCESS: Drive CRUD end to end passed".green());
    Ok(())
}
