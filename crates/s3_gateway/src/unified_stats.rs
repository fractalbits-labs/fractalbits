use std::io::Write;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

pub struct UnifiedStatsWriter {
    stats_file_path: String,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    task_handle: Option<JoinHandle<()>>,
}

impl UnifiedStatsWriter {
    pub fn new(stats_dir: String) -> Self {
        let stats_file_path = format!("{}/s3_gateway.stats", stats_dir);
        Self {
            stats_file_path,
            shutdown_tx: None,
            task_handle: None,
        }
    }

    pub async fn start(&mut self) -> Result<(), std::io::Error> {
        std::fs::create_dir_all(
            std::path::Path::new(&self.stats_file_path)
                .parent()
                .unwrap(),
        )?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let stats_file_path = self.stats_file_path.clone();
        let task_handle = tokio::spawn(async move {
            if let Err(e) = Self::stats_writer_task(stats_file_path, shutdown_rx).await {
                error!("Unified stats writer task failed: {}", e);
            }
        });

        self.task_handle = Some(task_handle);
        debug!(
            "Unified stats writer started, writing to {}",
            self.stats_file_path
        );
        Ok(())
    }

    async fn stats_writer_task(
        stats_file_path: String,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), std::io::Error> {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&stats_file_path)?;

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut lines = 0usize;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let bss_stats = rpc_client_bss::get_global_bss_stats();
                    let nss_stats = rpc_client_nss::get_global_nss_stats();
                    let http_stats = crate::http_stats::get_global_http_stats();

                    if lines.is_multiple_of(30) {
                        let mut header = format!("{:<22} ", "Time");

                        header.push_str("nss_get_inode    ");
                        header.push_str("nss_put_inode    ");
                        header.push_str("nss_delete_inode ");
                        header.push_str("nss_other        ");

                        header.push_str("bss_get_blob     ");
                        header.push_str("bss_put_blob     ");
                        header.push_str("bss_delete_blob  ");

                        header.push_str("http_get_obj     ");
                        header.push_str("http_put_obj     ");
                        header.push_str("http_delete_obj  ");
                        header.push_str("http_other       ");

                        writeln!(file, "{}", header.trim_end())?;
                        file.flush()?;
                    }

                    lines += 1;

                    let now = chrono::Local::now();
                    let timestamp = now.format("%Y-%m-%dT%H:%M:%S");

                    let mut line = format!("{:<22} ", timestamp);

                    line.push_str(&format!("{:<17}", nss_stats.get_count(rpc_client_nss::OperationType::GetInode)));
                    line.push_str(&format!("{:<17}", nss_stats.get_count(rpc_client_nss::OperationType::PutInode)));
                    line.push_str(&format!("{:<17}", nss_stats.get_count(rpc_client_nss::OperationType::DeleteInode)));
                    line.push_str(&format!("{:<17}", nss_stats.get_count(rpc_client_nss::OperationType::Other)));

                    line.push_str(&format!("{:<17}", bss_stats.get_count(rpc_client_bss::OperationType::GetData)));
                    line.push_str(&format!("{:<17}", bss_stats.get_count(rpc_client_bss::OperationType::PutData)));
                    line.push_str(&format!("{:<17}", bss_stats.get_count(rpc_client_bss::OperationType::DeleteData)));

                    line.push_str(&format!("{:<17}", http_stats.get_count(crate::http_stats::HttpEndpointType::GetObj)));
                    line.push_str(&format!("{:<17}", http_stats.get_count(crate::http_stats::HttpEndpointType::PutObj)));
                    line.push_str(&format!("{:<17}", http_stats.get_count(crate::http_stats::HttpEndpointType::DeleteObj)));
                    line.push_str(&format!("{:<17}", http_stats.get_count(crate::http_stats::HttpEndpointType::Other)));

                    writeln!(file, "{}", line.trim_end())?;
                }
                _ = &mut shutdown_rx => {
                    debug!("Unified stats writer shutting down");
                    break;
                }
            }
        }

        file.flush()?;
        Ok(())
    }

    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.task_handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for UnifiedStatsWriter {
    fn drop(&mut self) {
        if self.shutdown_tx.is_some() {
            warn!("UnifiedStatsWriter dropped without calling stop()");
        }
    }
}

pub async fn init_unified_stats_writer(
    stats_dir: String,
) -> Result<UnifiedStatsWriter, std::io::Error> {
    let mut writer = UnifiedStatsWriter::new(stats_dir);
    writer.start().await?;
    Ok(writer)
}
