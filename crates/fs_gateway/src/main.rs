mod auth;
mod backend;
mod config;
mod disk_cache;
mod error;
mod flush;
mod prefetch;
mod resolve;
mod s3_volume;
mod server;
mod sweep;

use clap::Parser;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use tokio::signal::unix::{SignalKind, signal};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::Config;
use crate::server::Gateway;

#[derive(Parser)]
#[clap(
    name = "fs_gateway",
    about = "Stateless storage gateway for artfs-mount"
)]
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
    let worker_threads = cfg.worker_threads.max(1);
    tracing::info!(
        port = cfg.port,
        worker_threads,
        auth_required = cfg.auth_required,
        data_volume = %cfg.data_volume,
        "Starting fs_gateway"
    );

    let gateway = Arc::new(Gateway::new(Arc::new(cfg)));

    // Reclamation and cache eviction live on their own runtime so a
    // request worker never hosts long-lived background work.
    {
        let gw = gateway.clone();
        thread::Builder::new()
            .name("fs-gw-sweep".to_string())
            .spawn(move || {
                let rt = compio_runtime::Runtime::new().expect("sweep runtime");
                rt.block_on(async move {
                    if let Some(dc) = &gw.disk_cache {
                        dc.spawn_evictor();
                    }
                    sweep::ensure_sweep_worker_started(&gw.sweep);
                    std::future::pending::<()>().await;
                });
            })?;
    }

    for worker_id in 0..worker_threads {
        let gw = gateway.clone();
        thread::Builder::new()
            .name(format!("fs-gw-w{worker_id}"))
            .spawn(move || {
                let rt = compio_runtime::Runtime::new().expect("worker runtime");
                if let Err(e) = rt.block_on(server::run_worker(gw, worker_id)) {
                    tracing::error!(worker_id, error = %e, "worker exited");
                    std::process::exit(1);
                }
            })?;
    }

    // Stateless: a signal just stops the process. Queued reclamation is
    // resumed by the durable `@ovr-gc/` markers on the next mount.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigint = signal(SignalKind::interrupt())?;
        let signal_name = tokio::select! {
            _ = sigterm.recv() => "SIGTERM",
            _ = sigint.recv() => "SIGINT",
        };
        tracing::info!(signal = signal_name, "received signal, shutting down");
        Ok::<(), std::io::Error>(())
    })?;
    Ok(())
}
