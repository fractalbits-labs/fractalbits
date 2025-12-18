use clap::Parser;
use cmd_lib::*;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tokio::signal::unix::{SignalKind, signal};
use tokio::time::sleep;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use xtask_tools::{
    check_port_ready, create_bss_dirs, create_nss_dirs, generate_bss_data_vg_config,
    generate_bss_metadata_vg_config,
};

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[clap(
    name = "docker-all-in-one",
    about = "All-in-one container orchestrator for fractalbits services"
)]
struct Opt {
    #[clap(
        long,
        default_value = "/opt/fractalbits/bin",
        help = "Directory containing service binaries"
    )]
    pub bin_dir: PathBuf,

    #[clap(
        long,
        default_value = "/data",
        help = "Data directory for all services"
    )]
    pub data_dir: PathBuf,

    #[clap(long, default_value = "8080", help = "API server port")]
    pub api_port: u16,

    #[clap(long, default_value = "2379", help = "etcd client port")]
    pub etcd_port: u16,
}

struct Orchestrator {
    bin_dir: PathBuf,
    data_dir: PathBuf,
    api_port: u16,
    etcd_port: u16,
    children: Vec<(&'static str, Child)>,
}

impl Orchestrator {
    fn new(opt: &Opt) -> Self {
        Self {
            bin_dir: opt.bin_dir.clone(),
            data_dir: opt.data_dir.clone(),
            api_port: opt.api_port,
            etcd_port: opt.etcd_port,
            children: Vec::new(),
        }
    }

    async fn start_all(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Initializing directories");
        self.init_directories()?;

        // Start etcd
        info!("Starting etcd");
        self.start_etcd()?;
        self.wait_for_port(self.etcd_port, 30).await?;

        // Initialize etcd keys
        info!("Initializing etcd service-discovery keys");
        self.init_etcd_keys()?;

        // Start BSS
        info!("Starting bss_server");
        self.start_bss()?;
        self.wait_for_port(8088, 30).await?;

        // Start RSS
        info!("Starting root_server");
        self.start_rss()?;
        self.wait_for_port(8086, 30).await?;

        // Initialize test API key
        info!("Initializing test API key");
        self.init_test_api_key()?;

        // Format and start NSS
        info!("Formatting nss_server");
        self.format_nss()?;

        info!("Starting nss_server");
        self.start_nss()?;
        self.wait_for_port(8087, 30).await?;

        // Start api_server
        info!("Starting api_server");
        self.start_api_server()?;
        self.wait_for_port(self.api_port, 30).await?;

        info!("All services started successfully");
        Ok(())
    }

    fn init_directories(&self) -> Result<(), Box<dyn std::error::Error>> {
        fs::create_dir_all(self.data_dir.join("etcd"))?;
        fs::create_dir_all(self.data_dir.join("logs"))?;

        create_bss_dirs(&self.data_dir, 0, 1)?;
        create_nss_dirs(&self.data_dir, "nss-A")?;

        Ok(())
    }

    fn start_etcd(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let child = Command::new(self.bin_dir.join("etcd"))
            .arg("--data-dir")
            .arg(self.data_dir.join("etcd"))
            .arg("--listen-client-urls")
            .arg(format!("http://0.0.0.0:{}", self.etcd_port))
            .arg("--advertise-client-urls")
            .arg(format!("http://127.0.0.1:{}", self.etcd_port))
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;

        self.children.push(("etcd", child));
        Ok(())
    }

    fn init_etcd_keys(&self) -> Result<(), Box<dyn std::error::Error>> {
        let etcdctl = self.bin_dir.join("etcdctl");

        let nss_roles_json = r#"{"states":{"nss-A":"solo"}}"#;
        let az_status_json = r#"{"status":{"docker-az1":"Normal"}}"#;
        let bss_data_vg = generate_bss_data_vg_config(1);
        let bss_metadata_vg = generate_bss_metadata_vg_config(1);

        run_cmd! {
            $etcdctl put /fractalbits-service-discovery/nss_roles $nss_roles_json;
            $etcdctl put /fractalbits-service-discovery/az_status $az_status_json;
            $etcdctl put /fractalbits-service-discovery/bss-data-vg-config $bss_data_vg;
            $etcdctl put /fractalbits-service-discovery/bss-metadata-vg-config $bss_metadata_vg;
        }?;

        Ok(())
    }

    fn start_bss(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let child = Command::new(self.bin_dir.join("bss_server"))
            .env("BSS_WORKING_DIR", self.data_dir.join("bss0"))
            .env("BSS_ID", "bss0")
            .env("BSS_PORT", "8088")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;

        self.children.push(("bss_server", child));
        Ok(())
    }

    fn start_rss(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let child = Command::new(self.bin_dir.join("root_server"))
            .env("RSS_BACKEND", "etcd")
            .env("ETCD_ENDPOINTS", format!("http://127.0.0.1:{}", self.etcd_port))
            .env("RUST_LOG", "info")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;

        self.children.push(("root_server", child));
        Ok(())
    }

    fn init_test_api_key(&self) -> Result<(), Box<dyn std::error::Error>> {
        let rss_admin = self.bin_dir.join("rss_admin");

        run_cmd! {
            $rss_admin --rss-addr=127.0.0.1:8086 api-key init-test;
        }?;

        Ok(())
    }

    fn format_nss(&self) -> Result<(), Box<dyn std::error::Error>> {
        let nss_bin = self.bin_dir.join("nss_server");
        let working_dir = self.data_dir.join("nss-A");

        run_cmd! {
            WORKING_DIR=$working_dir $nss_bin format --init_test_tree;
        }?;

        Ok(())
    }

    fn start_nss(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let child = Command::new(self.bin_dir.join("nss_server"))
            .arg("serve")
            .env("WORKING_DIR", self.data_dir.join("nss-A"))
            .env("NSS_ROLE", "solo")
            .env("METADATA_VG_CONFIG", generate_bss_metadata_vg_config(1))
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;

        self.children.push(("nss_server", child));
        Ok(())
    }

    fn start_api_server(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let child = Command::new(self.bin_dir.join("api_server"))
            .env("RUST_LOG", "info")
            .env("HTTPS_DISABLED", "1")
            .env("APP_BLOB_STORAGE_BACKEND", "all_in_bss_single_az")
            .env("APP_STATS_DIR", self.data_dir.join("api-server/local/stats"))
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;

        self.children.push(("api_server", child));
        Ok(())
    }

    async fn wait_for_port(
        &self,
        port: u16,
        timeout_secs: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        let timeout = Duration::from_secs(timeout_secs as u64);

        info!(
            "Waiting for port {} to be ready (timeout: {}s)",
            port, timeout_secs
        );

        while start.elapsed() < timeout {
            if check_port_ready(port) {
                info!("Port {} is ready", port);
                return Ok(());
            }
            sleep(Duration::from_millis(500)).await;
        }

        Err(format!(
            "Timeout waiting for port {} to be ready after {}s",
            port, timeout_secs
        )
        .into())
    }

    fn shutdown(&mut self) {
        info!("Shutting down services in reverse order");

        while let Some((name, mut child)) = self.children.pop() {
            info!("Stopping {}", name);
            if let Err(e) = child.kill() {
                warn!("Failed to kill {}: {}", name, e);
            }
            if let Err(e) = child.wait() {
                warn!("Failed to wait for {}: {}", name, e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
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
    info!("Starting docker-all-in-one orchestrator");
    info!("  bin_dir: {:?}", opt.bin_dir);
    info!("  data_dir: {:?}", opt.data_dir);
    info!("  api_port: {}", opt.api_port);
    info!("  etcd_port: {}", opt.etcd_port);

    let mut orchestrator = Orchestrator::new(&opt);

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
