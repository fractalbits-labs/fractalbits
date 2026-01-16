use anyhow::{Context, Result, bail};
use cmd_lib::*;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{error, info, warn};
use xtask_common::{
    check_port_ready, create_bss_dirs, create_nss_dirs, generate_bss_data_vg_config,
    generate_bss_metadata_vg_config,
};

pub struct Orchestrator {
    bin_dir: PathBuf,
    data_dir: PathBuf,
    api_port: u16,
    etcd_port: u16,
    children: Vec<(&'static str, Child)>,
    stream_tasks: Vec<JoinHandle<()>>,
}

impl Orchestrator {
    pub fn new(bin_dir: PathBuf, data_dir: PathBuf, api_port: u16, etcd_port: u16) -> Self {
        Self {
            bin_dir,
            data_dir,
            api_port,
            etcd_port,
            children: Vec::new(),
            stream_tasks: Vec::new(),
        }
    }

    pub async fn start_all(&mut self) -> Result<()> {
        let total_start = Instant::now();

        info!("Initializing directories");
        self.init_directories()?;

        // Phase 1: Start etcd (must be first)
        let phase_start = Instant::now();
        info!("Starting etcd");
        self.start_etcd()?;
        self.wait_for_port(self.etcd_port, 30).await?;
        info!("Phase 1 (etcd ready): {:?}", phase_start.elapsed());

        info!("Initializing etcd service-discovery keys");
        self.init_etcd_keys()?;

        // Phase 2: Start BSS and RSS in parallel (both depend on etcd)
        let phase_start = Instant::now();
        info!("Starting bss_server and root_server in parallel");
        self.start_bss()?;
        self.start_rss()?;

        let (bss_result, rss_result) =
            tokio::join!(wait_for_port_async(8088, 30), wait_for_port_async(8086, 30));
        bss_result?;
        rss_result?;
        info!("Phase 2 (bss+rss ready): {:?}", phase_start.elapsed());

        // Phase 3: Init API key and format+start NSS in parallel
        // - init_test_api_key needs RSS
        // - format_nss + start_nss_role_agent needs BSS
        let phase_start = Instant::now();
        info!("Initializing API key and starting nss_role_agent in parallel");

        let bin_dir = self.bin_dir.clone();
        let api_key_task = tokio::task::spawn_blocking(move || init_test_api_key_static(&bin_dir));

        let format_start = Instant::now();
        self.format_nss()?;
        info!("format_nss completed in {:?}", format_start.elapsed());

        self.start_nss_role_agent()?;

        let (api_key_res, nss_result) = tokio::join!(api_key_task, wait_for_port_async(8087, 120));
        api_key_res.context("API key init task panicked")??;
        nss_result?;
        info!("Phase 3 (api_key+nss ready): {:?}", phase_start.elapsed());

        // Phase 4: Start api_server (depends on RSS and NSS)
        let phase_start = Instant::now();
        info!("Starting api_server");
        self.start_api_server()?;
        self.wait_for_port(self.api_port, 30).await?;
        info!("Phase 4 (api_server ready): {:?}", phase_start.elapsed());

        info!(
            "All services started successfully in {:?}",
            total_start.elapsed()
        );
        Ok(())
    }

    fn init_directories(&self) -> Result<()> {
        let data_dir = &self.data_dir;
        run_cmd! {
            mkdir -p $data_dir/etcd;
            chmod 700 $data_dir/etcd; // Set restrictive permissions (700) to avoid etcd warning
            mkdir -p $data_dir/logs;
        }?;

        create_bss_dirs(&self.data_dir, 0, 1)?;
        create_nss_dirs(&self.data_dir, "nss-A")?;

        Ok(())
    }

    fn start_etcd(&mut self) -> Result<()> {
        let child = Command::new(self.bin_dir.join("etcd"))
            .arg("--name")
            .arg("fractalbits-etcd")
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

    fn init_etcd_keys(&self) -> Result<()> {
        let etcdctl = self.bin_dir.join("etcdctl");

        let nss_roles_json = r#"{"states":{"nss-A":"solo"}}"#;
        let az_status_json = r#"{"status":{"docker-az1":"Normal"}}"#;
        let bss_data_vg = generate_bss_data_vg_config(1);
        let bss_metadata_vg = generate_bss_metadata_vg_config(1);

        // Initialize observer_state for solo mode (single NSS, no mirrord)
        let observer_state_json = r#"{"observer_state":"solo","nss_machine":{"machine_id":"nss-A","running_service":"nss","expected_role":"solo","network_address":"127.0.0.1:8087"},"mirrord_machine":{"machine_id":"","running_service":"mirrord","expected_role":"","network_address":null},"last_updated":0.0,"version":0}"#;

        run_cmd! {
            $etcdctl put /fractalbits-service-discovery/nss_roles $nss_roles_json >/dev/null;
            $etcdctl put /fractalbits-service-discovery/az_status $az_status_json >/dev/null;
            $etcdctl put /fractalbits-service-discovery/bss-data-vg-config $bss_data_vg >/dev/null;
            $etcdctl put /fractalbits-service-discovery/bss-metadata-vg-config $bss_metadata_vg >/dev/null;
            $etcdctl put /fractalbits-service-discovery/observer_state $observer_state_json >/dev/null;
        }?;

        Ok(())
    }

    fn start_bss(&mut self) -> Result<()> {
        let mut child = Command::new(self.bin_dir.join("bss_server"))
            .env("BSS_WORKING_DIR", self.data_dir.join("bss0"))
            .env("BSS_ID", "bss0")
            .env("BSS_PORT", "8088")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        if let Some(stdout) = child.stdout.take() {
            self.spawn_output_streamer("bss_server", stdout);
        }
        if let Some(stderr) = child.stderr.take() {
            self.spawn_output_streamer("bss_server", stderr);
        }

        self.children.push(("bss_server", child));
        Ok(())
    }

    fn start_rss(&mut self) -> Result<()> {
        let mut child = Command::new(self.bin_dir.join("root_server"))
            .env("RSS_BACKEND", "etcd")
            .env(
                "ETCD_ENDPOINTS",
                format!("http://127.0.0.1:{}", self.etcd_port),
            )
            .env("RUST_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        if let Some(stdout) = child.stdout.take() {
            self.spawn_output_streamer("root_server", stdout);
        }
        if let Some(stderr) = child.stderr.take() {
            self.spawn_output_streamer("root_server", stderr);
        }

        self.children.push(("root_server", child));
        Ok(())
    }

    fn format_nss(&self) -> Result<()> {
        let nss_bin = self.bin_dir.join("nss_server");
        let working_dir = self.data_dir.join("nss-A");

        // Skip formatting if journal file already exists (data already formatted)
        let journal_file = working_dir.join("local/journal/journal.data");
        if journal_file.exists() {
            info!("NSS data already formatted, skipping format_nss");
            return Ok(());
        }

        run_cmd! {
            WORKING_DIR=$working_dir $nss_bin format;
        }?;

        Ok(())
    }

    fn start_nss_role_agent(&mut self) -> Result<()> {
        let mut child = Command::new(self.bin_dir.join("nss_role_agent"))
            .env("INSTANCE_ID", "nss-A")
            .env("APP_SERVICE_MANAGER_BACKEND", "direct")
            .env("APP_WORKING_DIR", self.data_dir.join("nss-A"))
            .env("RUST_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        if let Some(stdout) = child.stdout.take() {
            self.spawn_output_streamer("nss_role_agent", stdout);
        }
        if let Some(stderr) = child.stderr.take() {
            self.spawn_output_streamer("nss_role_agent", stderr);
        }

        self.children.push(("nss_role_agent", child));
        Ok(())
    }

    fn start_api_server(&mut self) -> Result<()> {
        let mut child = Command::new(self.bin_dir.join("api_server"))
            .env("RUST_LOG", "info")
            .env("HTTPS_DISABLED", "1")
            .env("APP_BLOB_STORAGE_BACKEND", "all_in_bss_single_az")
            .env(
                "APP_STATS_DIR",
                self.data_dir.join("api-server/local/stats"),
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        if let Some(stdout) = child.stdout.take() {
            self.spawn_output_streamer("api_server", stdout);
        }
        if let Some(stderr) = child.stderr.take() {
            self.spawn_output_streamer("api_server", stderr);
        }

        self.children.push(("api_server", child));
        Ok(())
    }

    async fn wait_for_port(&mut self, port: u16, timeout_secs: u32) -> Result<()> {
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

            if let Some(crashed) = self.check_for_crashed_service() {
                bail!(self.format_service_crash_error(&crashed.0, crashed.1));
            }

            sleep(Duration::from_millis(500)).await;
        }

        if let Some(crashed) = self.check_for_crashed_service() {
            bail!(self.format_service_crash_error(&crashed.0, crashed.1));
        }

        bail!(
            "Timeout waiting for port {} to be ready after {}s",
            port,
            timeout_secs
        );
    }

    fn check_for_crashed_service(&mut self) -> Option<(String, ExitStatus)> {
        for (name, child) in &mut self.children {
            if let Ok(Some(status)) = child.try_wait() {
                return Some((name.to_string(), status));
            }
        }
        None
    }

    fn format_service_crash_error(&self, service_name: &str, status: ExitStatus) -> String {
        error!(
            "Service '{}' exited unexpectedly with status: {}",
            service_name, status
        );

        let is_storage_service = service_name == "bss_server" || service_name == "nss_server";

        let mut msg = format!(
            "Service '{}' crashed with exit status: {}",
            service_name, status
        );

        if is_storage_service {
            msg.push_str("\n\nThis is likely because io_uring requires elevated privileges.");
            msg.push_str("\nPlease run the container with --privileged flag:");
            msg.push_str("\n\n  docker run --rm --privileged -p 8080:8080 <image>");
            msg.push_str(
                "\n\nAlternatively, use 'just docker run' which handles this automatically.",
            );
        }

        msg
    }

    fn spawn_output_streamer<R>(&mut self, logger: &'static str, reader: R)
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let handle = tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                info!(logger = logger, "{}", line);
            }
        });
        self.stream_tasks.push(handle);
    }

    pub async fn shutdown(&mut self) {
        info!("Shutting down services in reverse order");

        // Abort all output streaming tasks
        for handle in self.stream_tasks.drain(..) {
            handle.abort();
        }

        while let Some((name, mut child)) = self.children.pop() {
            info!("Stopping {}", name);
            if let Err(e) = child.start_kill() {
                warn!("Failed to kill {}: {}", name, e);
            }
            if let Err(e) = child.wait().await {
                warn!("Failed to wait for {}: {}", name, e);
            }
        }
    }
}

fn init_test_api_key_static(bin_dir: &Path) -> Result<()> {
    let rss_admin = bin_dir.join("rss_admin");

    run_cmd! {
        $rss_admin --rss-addr=127.0.0.1:8086 api-key init-test;
    }?;

    Ok(())
}

async fn wait_for_port_async(port: u16, timeout_secs: u32) -> Result<()> {
    let start = Instant::now();
    let timeout = Duration::from_secs(timeout_secs as u64);

    while start.elapsed() < timeout {
        if check_port_ready(port) {
            info!("Port {} is ready", port);
            return Ok(());
        }
        sleep(Duration::from_millis(500)).await;
    }

    bail!("Timeout waiting for port {} after {}s", port, timeout_secs);
}
