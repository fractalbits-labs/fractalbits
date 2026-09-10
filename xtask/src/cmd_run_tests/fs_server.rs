pub mod fuse;
pub mod pjdfs;

use crate::cmd_build::BuildMode;
use crate::{CmdResult, FsGatewayConfig, FsMountConfig, InitConfig, ServiceName, cmd_service};
use cmd_lib::*;
use std::sync::Mutex;
use std::time::Duration;
use test_common::*;

pub const MOUNT_POINT: &str = "/tmp/fs_server_test";
/// Where `fractalbits-mount` reaches the local `fs_gateway` unit.
pub const GATEWAY_ADDR: &str = "127.0.0.1:8180";
const BUCKET_NAME: &str = "test-file-server";

pub async fn run_fs_server_tests(disk_cache: bool) -> CmdResult {
    info!("Running fs_server integration tests...");
    fuse::run_fuse_tests_with_disk_cache(disk_cache).await
}

/// Build `fs_gateway` and `fractalbits-mount` using the isolated
/// COMPIO_TARGET_DIR to prevent workspace feature unification from
/// enabling tokio-runtime on their compio-only RPC deps.
pub fn build_fs_binaries() -> CmdResult {
    let compio_target_dir = crate::cmd_build::COMPIO_TARGET_DIR;
    run_cmd! {
        info "Building fs_gateway + fractalbits-mount (isolated compio build) ...";
        CARGO_TARGET_DIR=$compio_target_dir cargo build -p fs_gateway -p fs_client;
        rm -f target/debug/fs_gateway target/debug/fractalbits-mount;
                cp $compio_target_dir/debug/fs_gateway target/debug/fs_gateway;
        cp $compio_target_dir/debug/fractalbits-mount target/debug/fractalbits-mount;
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

/// Gateway config for a test run; the disk cache lives on the gateway.
pub fn gateway_config(disk_cache: bool, disk_cache_path: &str, size_gb: u64) -> FsGatewayConfig {
    let mut cfg = FsGatewayConfig::default();
    if disk_cache {
        cfg.disk_cache_enabled = true;
        cfg.disk_cache_path = disk_cache_path.to_string();
        cfg.disk_cache_size_gb = size_gb;
    }
    cfg
}

/// The gateway config currently running under systemd, if this process
/// started it. Lets a suite that mounts per test restart the gateway
/// only when its config actually changes (disk cache on/off).
static RUNNING_GATEWAY: Mutex<Option<FsGatewayConfig>> = Mutex::new(None);

/// Start (or restart on a config change) the local `fs_gateway` unit.
pub fn ensure_gateway(mode: BuildMode, cfg: &FsGatewayConfig) -> CmdResult {
    let mut running = RUNNING_GATEWAY.lock().expect("gateway state lock");
    let service_name = ServiceName::FsGateway.as_ref();
    let active = run_cmd!(systemctl --user is-active --quiet $service_name.service).is_ok();
    if active && running.as_ref() == Some(cfg) {
        return Ok(());
    }
    let _ = cmd_service::stop_service(ServiceName::FsGateway);
    if cfg.disk_cache_enabled {
        let dc_path = &cfg.disk_cache_path;
        run_cmd!(mkdir -p $dc_path)?;
    }
    cmd_service::init_service(
        ServiceName::FsGateway,
        mode,
        &InitConfig {
            fs_gateway: cfg.clone(),
            ..Default::default()
        },
    )?;
    cmd_service::start_service(ServiceName::FsGateway)?;
    *running = Some(cfg.clone());
    Ok(())
}

pub fn stop_gateway() -> CmdResult {
    let mut running = RUNNING_GATEWAY.lock().expect("gateway state lock");
    *running = None;
    let _ = cmd_service::stop_service(ServiceName::FsGateway);
    run_cmd! { ignore pkill -x fs_gateway 2>/dev/null; }
}

/// Mount `cfg.bucket_name` at `cfg.mount_point` through the running
/// gateway with the `fs_mount` unit, waiting for the mountpoint.
pub fn mount_fs(mode: BuildMode, cfg: &FsMountConfig) -> CmdResult {
    let mount_point = cfg.mount_point.clone();
    // Clean up any stale FUSE mount (e.g. "Transport endpoint is not connected").
    run_cmd! {
        ignore fusermount3 -u $mount_point 2>/dev/null;
        ignore fusermount -u $mount_point 2>/dev/null;
    }?;
    run_cmd!(mkdir -p $mount_point)?;
    cmd_service::init_service(
        ServiceName::FsMount,
        mode,
        &InitConfig {
            fs_mount: cfg.clone(),
            ..Default::default()
        },
    )?;
    cmd_service::start_service(ServiceName::FsMount)?;

    for i in 0..40 {
        std::thread::sleep(Duration::from_millis(500));
        if run_cmd!(mountpoint -q $mount_point).is_ok() {
            let mode = if cfg.writeback_mode.is_empty() {
                "default"
            } else {
                cfg.writeback_mode.as_str()
            };
            println!(
                "    FUSE (writeback={mode}) mounted at {mount_point} (after {}ms)",
                (i + 1) * 500
            );
            return Ok(());
        }
    }
    Err(std::io::Error::other(format!(
        "FUSE mount at {mount_point} not ready after 20 seconds"
    )))
}

/// Unmount and stop the `fs_mount` unit. The gateway keeps running.
pub fn unmount_fs(mount_point: &str) -> CmdResult {
    run_cmd! {
        ignore fusermount3 -u $mount_point 2>/dev/null;
        ignore fusermount -u $mount_point 2>/dev/null;
    }?;
    let _ = cmd_service::stop_service(ServiceName::FsMount);
    run_cmd! { ignore pkill -f "/fractalbits-mount" 2>/dev/null; }?;
    std::thread::sleep(Duration::from_millis(500));
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
