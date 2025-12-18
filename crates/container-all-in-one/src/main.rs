use clap::Parser;
use std::io;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod orchestrator;
use orchestrator::Orchestrator;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[clap(
    name = "container-all-in-one",
    about = "All-in-one container orchestrator for fractalbits services"
)]
struct Opt {
    #[clap(
        long,
        default_value = "/opt/fractalbits/bin",
        help = "Directory containing service binaries"
    )]
    pub bin_dir: std::path::PathBuf,

    #[clap(
        long,
        default_value = "/data",
        help = "Data directory for all services"
    )]
    pub data_dir: std::path::PathBuf,

    #[clap(long, default_value = "8080", help = "API server port")]
    pub api_port: u16,

    #[clap(long, default_value = "2379", help = "etcd client port")]
    pub etcd_port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_writer(io::stderr),
        )
        .init();

    let main_build_info = option_env!("MAIN_BUILD_INFO").unwrap_or("unknown");
    let build_timestamp = option_env!("BUILD_TIMESTAMP").unwrap_or("unknown");
    let build_info = format!("{}, build time: {}", main_build_info, build_timestamp);
    eprintln!("build info: {}", build_info);

    let opt = Opt::parse();
    info!("Starting container-all-in-one orchestrator");
    info!("  bin_dir: {:?}", opt.bin_dir);
    info!("  data_dir: {:?}", opt.data_dir);
    info!("  api_port: {}", opt.api_port);
    info!("  etcd_port: {}", opt.etcd_port);

    let mut orchestrator =
        Orchestrator::new(opt.bin_dir, opt.data_dir, opt.api_port, opt.etcd_port);

    // Setup signal handlers for graceful shutdown
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let mut sigint = signal(SignalKind::interrupt()).unwrap();

    // Start all services
    if let Err(e) = orchestrator.start_all().await {
        error!("Failed to start services: {}", e);
        orchestrator.shutdown();
        return Err(e);
    }

    info!("All services running. Press Ctrl+C to stop.");

    // Wait for shutdown signal
    tokio::select! {
        _ = sigterm.recv() => {
            info!("Received SIGTERM, shutting down gracefully");
        }
        _ = sigint.recv() => {
            info!("Received SIGINT, shutting down gracefully");
        }
    }

    orchestrator.shutdown();
    info!("Shutdown complete");

    Ok(())
}
