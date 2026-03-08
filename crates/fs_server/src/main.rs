mod backend;
mod cache;
mod config;
mod disk_cache;
mod error;
mod fuse_server;
mod inode;
mod nfs_server;
mod slice_mut;
mod vfs;
mod watch;

use clap::Parser;
use fractal_fuse::MountOptions;
use fractal_fuse::Session;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::{Config, ServerMode};

#[derive(Parser)]
#[clap(name = "fs_server", about = "FUSE/NFS file server for FractalBits S3")]
struct Opt {
    #[clap(short = 'c', long = "config", help = "Config file path")]
    config_file: Option<PathBuf>,

    #[clap(
        long = "mode",
        default_value = "fuse",
        help = "Server mode: fuse or nfs"
    )]
    mode: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let third_party_filter = "hyper_util=warn,aws_smithy=warn,aws_sdk=warn,h2=warn";
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .map(|filter| {
                    format!("{filter},{third_party_filter}")
                        .parse()
                        .unwrap_or(filter)
                })
                .unwrap_or_else(|_| format!("info,{third_party_filter}").into()),
        )
        .with({
            let is_terminal = std::io::stdout().is_terminal();
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_ansi(false)
                .with_level(is_terminal)
                .with_target(is_terminal)
        })
        .init();

    let opt = Opt::parse();
    let mode_str = std::env::var("FS_SERVER_MODE").unwrap_or(opt.mode);
    let server_mode = match mode_str.as_str() {
        "nfs" => ServerMode::Nfs,
        _ => ServerMode::Fuse,
    };

    let mut cfg: Config = match opt.config_file {
        Some(config_file) => ::config::Config::builder()
            .add_source(::config::File::from(config_file).required(true))
            .build()?
            .try_deserialize()?,
        None => Config::default(),
    };
    cfg.apply_env_overrides();

    let mount_point = cfg.mount_point.clone();
    let read_write = cfg.read_write;

    tracing::info!(
        bucket = %cfg.bucket_name,
        mode = ?server_mode,
        read_write = read_write,
        "Starting fs_server"
    );

    // Discover backend configuration (NSS address, DataVgInfo, bucket) via RSS.
    // This runs on a temporary compio runtime since we need async RPC.
    let backend_config = {
        let cfg_ref = &cfg;
        compio_runtime::Runtime::new()
            .expect("Failed to create compio runtime for discovery")
            .block_on(backend::BackendConfig::discover(cfg_ref))
            .map_err(|e| std::io::Error::other(format!("Backend discovery failed: {e}")))?
    };
    let backend_config = Arc::new(backend_config);

    let inodes = Arc::new(inode::InodeTable::new());
    let vfs_core = Arc::new(vfs::VfsCore::new(
        backend_config.clone(),
        inodes,
        read_write,
    ));

    match server_mode {
        ServerMode::Fuse => {
            tracing::info!(mount_point = %mount_point, "Starting FUSE client");

            // Spawn background watch loop for cross-instance cache invalidation
            let shutdown = Arc::new(AtomicBool::new(false));
            let watch_handle =
                watch::spawn_watch_loop(vfs_core.clone(), backend_config, shutdown.clone());

            let fuse_fs = fuse_server::FuseServer::new(vfs_core);

            let mount_options = MountOptions::default()
                .fs_name("fractalbits")
                .read_only(!read_write)
                .allow_other(cfg.allow_other)
                .write_back(read_write && !cfg.passthrough_enabled)
                .passthrough(cfg.passthrough_enabled);

            Session::new(mount_options).run(fuse_fs, Path::new(&mount_point))?;

            // Signal watch loop to stop and wait for it
            shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = watch_handle.join();
            tracing::info!("FUSE client exited");
        }
        ServerMode::Nfs => {
            tracing::info!(port = cfg.nfs_port, "Starting NFS server");
            let nfs_adapter = nfs_server::NfsAdapter::new(vfs_core, 1);

            let nfs_config = fractal_nfs::NfsServerConfig {
                port: cfg.nfs_port,
                ..Default::default()
            };

            fractal_nfs::run(nfs_adapter, nfs_config)?;
            tracing::info!("NFS server exited");
        }
    }

    Ok(())
}
