use std::process::Command;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use bss_repair::DataRepairReport;
use bytes::Bytes;
use cmd_lib::*;
use colored::*;
use data_types::{DataBlobGuid, TraceId};
use rpc_client_bss::RpcClientBss;
use tokio::time::sleep;
use uuid::Uuid;

use crate::CmdResult;
use crate::etcd_utils::resolve_etcd_bin;

type TestResult<T = ()> = Result<T, BssRepairTestError>;
static TEST_VOLUMES: OnceLock<TestVolumes> = OnceLock::new();

#[derive(Clone, Copy)]
struct TestVolumes {
    scan: u16,
    split_brain: u16,
    majority: u16,
    degraded_scan: u16,
}

struct BssRestartGuard {
    instance: u8,
    needs_restart: bool,
}

impl BssRestartGuard {
    fn new(instance: u8) -> Self {
        Self {
            instance,
            needs_restart: true,
        }
    }

    fn disarm(&mut self) {
        self.needs_restart = false;
    }
}

impl Drop for BssRestartGuard {
    fn drop(&mut self) {
        if !self.needs_restart {
            return;
        }

        let unit = format!("bss@{}.service", self.instance);
        let _ = Command::new("systemctl")
            .args(["--user", "start", &unit])
            .status();
    }
}

#[derive(Debug, thiserror::Error)]
enum BssRepairTestError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Rpc(#[from] rpc_client_bss::RpcErrorBss),

    #[error("command failed: {0}")]
    CommandFailed(String),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl From<BssRepairTestError> for std::io::Error {
    fn from(value: BssRepairTestError) -> Self {
        std::io::Error::other(value.to_string())
    }
}

pub async fn run_bss_repair_tests() -> CmdResult {
    run_bss_repair_tests_inner().await.map_err(Into::into)
}

async fn run_bss_repair_tests_inner() -> TestResult {
    info!("Running BSS repair tests...");
    let volumes = *TEST_VOLUMES.get_or_init(new_test_volumes);
    install_test_data_vg_config(volumes)?;

    println!(
        "\n{}",
        "=== Test: Scan-Only Finds Under-Replicated Blob ==="
            .bold()
            .green()
    );
    test_scan_only_detects_under_replicated_without_repair().await?;

    println!(
        "\n{}",
        "=== Test: Repair Mode Heals Multiple Blobs And Leaves Healthy Volume Clean ==="
            .bold()
            .green()
    );
    test_repair_mode_heals_multiple_blobs_and_healthy_followup().await?;

    println!(
        "\n{}",
        "=== Test: Majority Repair Fixes Outlier Replica ==="
            .bold()
            .green()
    );
    test_majority_repair_fixes_outlier_replica().await?;

    println!(
        "\n{}",
        "=== Test: Split-Brain Is Reported As Failed Volume ==="
            .bold()
            .green()
    );
    test_split_brain_is_reported_as_failed_volume().await?;

    println!(
        "\n{}",
        "=== Test: Degraded Scan Continues With Quorum ==="
            .bold()
            .green()
    );
    test_degraded_scan_continues_with_quorum().await?;

    println!("\n{}", "=== All BSS Repair Tests PASSED ===".green().bold());
    Ok(())
}

async fn test_scan_only_detects_under_replicated_without_repair() -> TestResult {
    let volumes = *TEST_VOLUMES.get().expect("test volumes initialized");
    let blob_guid = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: volumes.scan,
    };
    let body = Bytes::from_static(b"bss-repair-scan-only");
    write_blob_to_two_nodes(blob_guid, 0, body.clone()).await?;

    let volume_id = volumes.scan.to_string();
    let report = run_bss_repair_json(&["scan-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(report.scanned_volumes, 1, "expected one scanned volume");
    assert_eq!(report.failed_volumes, 0, "scan-only should not fail");
    assert_eq!(report.scanned_blobs, 1, "expected one scanned blob");
    assert_eq!(report.repair_candidates, 1, "expected one repair candidate");
    assert_eq!(report.repaired_blobs, 0, "scan-only must not repair");

    let node2_entries = list_keys_on_node("127.0.0.1:8090", volumes.scan).await?;
    assert!(
        node2_entries.is_empty(),
        "scan-only should not populate missing node"
    );

    println!("  OK: scan-only detected the missing replica and left data untouched");
    Ok(())
}

async fn test_repair_mode_heals_multiple_blobs_and_healthy_followup() -> TestResult {
    let volumes = *TEST_VOLUMES.get().expect("test volumes initialized");
    let body_a = Bytes::from_static(b"bss-repair-body-a");
    let body_b = Bytes::from_static(b"bss-repair-body-b");
    let blob_a = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: volumes.scan,
    };
    let blob_b = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: volumes.scan,
    };

    write_blob_to_two_nodes(blob_a, 0, body_a.clone()).await?;
    write_blob_to_two_nodes(blob_b, 0, body_b.clone()).await?;

    let volume_id = volumes.scan.to_string();
    let report = run_bss_repair_json(&["repair-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(report.failed_volumes, 0, "repair should succeed");
    assert_eq!(report.scanned_volumes, 1, "expected one scanned volume");
    assert_eq!(
        report.repair_candidates, 3,
        "expected the previously scanned blob plus two new blobs"
    );
    assert_eq!(report.repaired_blobs, 3, "expected three repaired blobs");

    assert_eq!(
        read_blob_from_node("127.0.0.1:8090", blob_a, 0, body_a.len()).await?,
        body_a,
        "node2 should receive repaired blob A"
    );
    assert_eq!(
        read_blob_from_node("127.0.0.1:8090", blob_b, 0, body_b.len()).await?,
        body_b,
        "node2 should receive repaired blob B"
    );

    let post_repair = run_bss_repair_json(&["scan-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(post_repair.failed_volumes, 0, "healthy scan should succeed");
    assert_eq!(
        post_repair.repair_candidates, 0,
        "healthy volume should be clean"
    );

    println!("  OK: repair mode healed all missing replicas and follow-up scan was clean");
    Ok(())
}

async fn test_majority_repair_fixes_outlier_replica() -> TestResult {
    let volumes = *TEST_VOLUMES.get().expect("test volumes initialized");
    let blob_guid = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: volumes.majority,
    };
    let canonical_body = Bytes::from_static(b"majority-body");
    let outlier_body = Bytes::from_static(b"outlier-body");

    write_blob_to_two_nodes(blob_guid, 0, canonical_body.clone()).await?;
    put_blob("127.0.0.1:8090", blob_guid, 0, outlier_body).await?;

    let volume_id = volumes.majority.to_string();
    let report = run_bss_repair_json(&["repair-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(report.scanned_volumes, 1, "expected one scanned volume");
    assert_eq!(report.failed_volumes, 0, "majority repair should succeed");
    assert_eq!(report.repair_candidates, 1, "expected one repair candidate");
    assert_eq!(report.repaired_blobs, 1, "expected one repaired blob");
    assert_eq!(
        read_blob_from_node("127.0.0.1:8090", blob_guid, 0, canonical_body.len()).await?,
        canonical_body,
        "outlier replica should be overwritten with canonical body"
    );

    println!("  OK: majority replicas repaired the outlier node");
    Ok(())
}

async fn test_split_brain_is_reported_as_failed_volume() -> TestResult {
    let volumes = *TEST_VOLUMES.get().expect("test volumes initialized");
    let blob_guid = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: volumes.split_brain,
    };
    put_blob(
        "127.0.0.1:8088",
        blob_guid,
        0,
        Bytes::from_static(b"mismatch-a"),
    )
    .await?;
    put_blob(
        "127.0.0.1:8089",
        blob_guid,
        0,
        Bytes::from_static(b"mismatch-b"),
    )
    .await?;

    let volume_id = volumes.split_brain.to_string();
    let report =
        run_bss_repair_json_expect_failure(&["scan-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(report.scanned_volumes, 1, "expected one scanned volume");
    assert_eq!(
        report.failed_volumes, 1,
        "mismatch should mark the volume failed"
    );
    assert_eq!(report.volume_reports.len(), 1, "expected one volume report");
    let error = report.volume_reports[0]
        .error
        .as_deref()
        .unwrap_or("<missing error>");
    assert!(
        error.contains("no authoritative replica"),
        "unexpected volume error: {error}"
    );

    println!("  OK: split-brain replicas were surfaced as a failed volume report");
    Ok(())
}

async fn test_degraded_scan_continues_with_quorum() -> TestResult {
    let volumes = *TEST_VOLUMES.get().expect("test volumes initialized");
    let mut restart_guard = BssRestartGuard::new(2);
    let status = Command::new("systemctl")
        .args(["--user", "stop", "bss@2.service"])
        .status()?;
    assert!(status.success(), "failed to stop bss@2.service");
    sleep(Duration::from_secs(2)).await;

    let active_status = Command::new("systemctl")
        .args(["--user", "is-active", "--quiet", "bss@2.service"])
        .status()?;
    assert!(
        !active_status.success(),
        "bss@2.service should be stopped for degraded scan test"
    );

    let volume_id = volumes.degraded_scan.to_string();
    let report = run_bss_repair_json(&["scan-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(report.scanned_volumes, 1, "expected one scanned volume");
    assert_eq!(report.failed_volumes, 0, "degraded scan should not fail");
    assert_eq!(report.degraded_volumes, 1, "expected one degraded volume");
    assert_eq!(report.volume_reports.len(), 1, "expected one volume report");
    assert!(
        report.volume_reports[0].degraded,
        "volume should be marked degraded"
    );
    assert_eq!(
        report.volume_reports[0].failed_nodes,
        vec!["bss2".to_string()],
        "expected node bss2 to be recorded as failed"
    );

    start_bss_instance(2).await?;
    restart_guard.disarm();

    println!("  OK: scan continued after ListBlobs failure while quorum remained");
    Ok(())
}

fn install_test_data_vg_config(volumes: TestVolumes) -> CmdResult {
    let data_vg_config = format!(
        r#"{{"volumes":[
{{"volume_id":{},"bss_nodes":[{{"node_id":"bss0","ip":"127.0.0.1","port":8088}},{{"node_id":"bss1","ip":"127.0.0.1","port":8089}},{{"node_id":"bss2","ip":"127.0.0.1","port":8090}}],"mode":{{"type":"replicated","n":3,"r":2,"w":2}}}},
{{"volume_id":{},"bss_nodes":[{{"node_id":"bss0","ip":"127.0.0.1","port":8088}},{{"node_id":"bss1","ip":"127.0.0.1","port":8089}},{{"node_id":"bss2","ip":"127.0.0.1","port":8090}}],"mode":{{"type":"replicated","n":3,"r":2,"w":2}}}},
{{"volume_id":{},"bss_nodes":[{{"node_id":"bss0","ip":"127.0.0.1","port":8088}},{{"node_id":"bss1","ip":"127.0.0.1","port":8089}},{{"node_id":"bss2","ip":"127.0.0.1","port":8090}}],"mode":{{"type":"replicated","n":3,"r":2,"w":2}}}},
{{"volume_id":{},"bss_nodes":[{{"node_id":"bss0","ip":"127.0.0.1","port":8088}},{{"node_id":"bss1","ip":"127.0.0.1","port":8089}},{{"node_id":"bss2","ip":"127.0.0.1","port":8090}}],"mode":{{"type":"replicated","n":3,"r":2,"w":2}}}}
]}}"#,
        volumes.scan, volumes.split_brain, volumes.majority, volumes.degraded_scan
    );
    let etcdctl = resolve_etcd_bin("etcdctl");

    run_cmd! {
        info "Installing test data vg config into etcd";
        $etcdctl put /fractalbits-service-discovery/bss-data-vg-config $data_vg_config >/dev/null;
    }?;

    Ok(())
}

fn new_test_volumes() -> TestVolumes {
    let seed = (Uuid::now_v7().as_u128() % 10_000) as u16;
    let base = 10_000 + seed * 5;
    TestVolumes {
        scan: base,
        split_brain: base + 1,
        majority: base + 2,
        degraded_scan: base + 3,
    }
}

async fn start_bss_instance(instance: u8) -> TestResult {
    let unit = format!("bss@{instance}.service");
    let port = 8088 + instance as u16;

    let status = Command::new("systemctl")
        .args(["--user", "start", &unit])
        .status()?;
    assert!(status.success(), "failed to start {unit}");

    sleep(Duration::from_secs(2)).await;

    let active_status = Command::new("systemctl")
        .args(["--user", "is-active", "--quiet", &unit])
        .status()?;
    assert!(
        active_status.success(),
        "{unit} should be active after start"
    );

    let client = Arc::new(RpcClientBss::new_from_address(
        format!("127.0.0.1:{port}"),
        Duration::from_secs(5),
    ));
    for _ in 0..30 {
        if client
            .list_data_blobs(
                1,
                "/d1/",
                "",
                1,
                Some(Duration::from_secs(2)),
                &TraceId::new(),
                0,
            )
            .await
            .is_ok()
        {
            return Ok(());
        }
        sleep(Duration::from_millis(500)).await;
    }
    Err(BssRepairTestError::CommandFailed(format!(
        "{unit} did not become ready after start"
    )))
}

fn run_bss_repair_json(args: &[&str]) -> TestResult<DataRepairReport> {
    let output = Command::new("./target/debug/bss_repair")
        .args(["--rss-addrs", "127.0.0.1:8086"])
        .args(args)
        .output()?;

    if !output.status.success() {
        return Err(BssRepairTestError::CommandFailed(format!(
            "stdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    Ok(serde_json::from_slice::<DataRepairReport>(&output.stdout)?)
}

fn run_bss_repair_json_expect_failure(args: &[&str]) -> TestResult<DataRepairReport> {
    let output = Command::new("./target/debug/bss_repair")
        .args(["--rss-addrs", "127.0.0.1:8086"])
        .args(args)
        .output()?;

    if output.status.success() {
        return Err(BssRepairTestError::CommandFailed(
            "expected bss_repair command to fail".to_string(),
        ));
    }

    Ok(serde_json::from_slice::<DataRepairReport>(&output.stdout)?)
}

async fn write_blob_to_two_nodes(
    blob_guid: DataBlobGuid,
    block_number: u32,
    body: Bytes,
) -> TestResult {
    put_blob("127.0.0.1:8088", blob_guid, block_number, body.clone()).await?;
    put_blob("127.0.0.1:8089", blob_guid, block_number, body).await?;
    Ok(())
}

async fn put_blob(
    addr: &str,
    blob_guid: DataBlobGuid,
    block_number: u32,
    body: Bytes,
) -> TestResult {
    let client = Arc::new(RpcClientBss::new_from_address(
        addr.to_string(),
        Duration::from_secs(5),
    ));
    let checksum = xxhash_rust::xxh3::xxh3_64(&body);
    client
        .put_data_blob(
            blob_guid,
            block_number,
            body,
            checksum,
            Some(Duration::from_secs(5)),
            &TraceId::new(),
            0,
        )
        .await?;
    Ok(())
}

async fn read_blob_from_node(
    addr: &str,
    blob_guid: DataBlobGuid,
    block_number: u32,
    content_len: usize,
) -> TestResult<Bytes> {
    let client = Arc::new(RpcClientBss::new_from_address(
        addr.to_string(),
        Duration::from_secs(5),
    ));
    let mut body = Bytes::new();
    client
        .get_data_blob(
            blob_guid,
            block_number,
            &mut body,
            content_len,
            Some(Duration::from_secs(5)),
            &TraceId::new(),
            0,
        )
        .await?;
    Ok(body)
}

async fn list_keys_on_node(addr: &str, volume_id: u16) -> TestResult<Vec<String>> {
    let client = Arc::new(RpcClientBss::new_from_address(
        addr.to_string(),
        Duration::from_secs(5),
    ));
    let page = client
        .list_data_blobs(
            volume_id,
            &format!("/d{volume_id}/"),
            "",
            100,
            Some(Duration::from_secs(5)),
            &TraceId::new(),
            0,
        )
        .await?;
    Ok(page.blobs.into_iter().map(|entry| entry.key).collect())
}
