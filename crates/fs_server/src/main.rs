mod fuse_server;

use clap::Parser;
use fractal_fuse::MountOptions;
use fractal_fuse::Session;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use fractal_vfs::backend;
use fractal_vfs::config::Config;
use fractal_vfs::inode;
use fractal_vfs::vfs::VfsCore;

use crate::fuse_server::FuseServer;

#[derive(Parser)]
#[clap(name = "fs_server", about = "FUSE file server for FractalBits S3")]
struct Opt {
    #[clap(short = 'c', long = "config", help = "Config file path")]
    config_file: Option<PathBuf>,
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
        read_write = read_write,
        "Starting fs_server"
    );

    // Discover backend configuration (NSS address, DataVgInfo, bucket) via RSS.
    let backend_config = {
        let cfg_ref = &cfg;
        compio_runtime::Runtime::new()
            .expect("Failed to create compio runtime for discovery")
            .block_on(backend::BackendConfig::discover(cfg_ref))
            .map_err(|e| std::io::Error::other(format!("Backend discovery failed: {e}")))?
    };
    let backend_config = Arc::new(backend_config);

    let inodes = Arc::new(inode::InodeTable::new());
    let vfs_core = VfsCore::new(backend_config, inodes, read_write);

    tracing::info!(mount_point = %mount_point, "Starting FUSE client");

    let mount_options = MountOptions::default()
        .fs_name("fractalbits")
        .read_only(!read_write)
        .allow_other(cfg.allow_other)
        // When allow_other is on (e.g. the pjdfstest harness mounts as
        // the daemon user but drives the suite as root), turn on the
        // kernel's default_permissions check so it enforces the mode /
        // uid / gid bits we report. Without it the kernel skips access
        // gating and the cross-user EPERM / EACCES contract can't be
        // exercised.
        .default_permissions(cfg.allow_other)
        .write_back(read_write && !cfg.passthrough_enabled)
        .passthrough(cfg.passthrough_enabled);

    let session =
        Session::new(mount_point.into(), mount_options)?.with_worker_count(cfg.worker_threads);
    let vfs_core = Arc::new(vfs_core.with_fuse_fd(session.fuse_fd()));
    session.run(FuseServer::new(vfs_core))?;
    tracing::info!("FUSE server exited");

    Ok(())
}
