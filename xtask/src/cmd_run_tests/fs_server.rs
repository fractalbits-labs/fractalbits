pub mod fuse;
pub mod nfs;

use crate::CmdResult;
use cmd_lib::*;
use test_common::*;

pub const MOUNT_POINT: &str = "/tmp/fs_server_test";
const BUCKET_NAME: &str = "test-file-server";

pub async fn run_fs_server_tests(run_fuse: bool, run_nfs: bool, disk_cache: bool) -> CmdResult {
    info!("Running fs_server integration tests...");
    if run_fuse {
        fuse::run_fuse_tests_with_disk_cache(disk_cache).await?;
    }
    if run_nfs {
        nfs::run_nfs_tests().await?;
    }
    Ok(())
}

/// Build the fs_server binary using isolated COMPIO_TARGET_DIR
/// to prevent workspace feature unification from enabling tokio-runtime.
pub fn build_fs_server() -> CmdResult {
    let compio_target = crate::cmd_build::COMPIO_TARGET_DIR;
    run_cmd! {
        info "Building fs_server [compio-runtime] ...";
        CARGO_TARGET_DIR=$compio_target cargo build -p fs_server;
        mkdir -p target/debug;
        cp -f $compio_target/debug/fs_server target/debug/;
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

/// Generate deterministic test data from a key name.
pub fn generate_test_data(key: &str, size: usize) -> Vec<u8> {
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

pub async fn setup_test_bucket() -> (Context, String) {
    let ctx = context();
    let bucket = ctx.create_bucket(BUCKET_NAME).await;
    (ctx, bucket)
}

pub async fn cleanup_objects(ctx: &Context, bucket: &str, keys: &[&str]) {
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
