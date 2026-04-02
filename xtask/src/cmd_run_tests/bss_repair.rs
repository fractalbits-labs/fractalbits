use std::process::Command;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use bytes::Bytes;
use cmd_lib::*;
use colored::*;
use data_types::ec_utils::{ec_padded_len, ec_rotation, node_index};
use data_types::{DataBlobGuid, TraceId};
use data_types::{DataRepairReport, MetaRepairReport};
use reed_solomon_simd::encode as rs_encode;
use rpc_client_bss::{RpcClientBss, RpcErrorBss};
use tokio::time::sleep;
use uuid::Uuid;

use crate::CmdResult;
use crate::cmd_build::BuildMode;
use crate::cmd_service::resolve_binary_path;
use crate::etcd_utils::resolve_etcd_bin;

type TestResult<T = ()> = Result<T, BssRepairTestError>;
static TEST_VOLUMES: OnceLock<TestVolumes> = OnceLock::new();
static META_TEST_VOLUMES: OnceLock<MetaTestVolumes> = OnceLock::new();
static EC_TEST_VOLUMES: OnceLock<EcTestVolumes> = OnceLock::new();
const META_FENCE_TOKEN: u64 = 9999;
const EC_K: usize = 4;
const EC_M: usize = 2;
const EC_TOTAL: usize = EC_K + EC_M;

#[derive(Clone, Copy)]
struct TestVolumes {
    scan: u16,
    split_brain: u16,
    majority: u16,
    degraded_scan: u16,
    delete_repair: u16,
}

#[derive(Clone, Copy)]
struct MetaTestVolumes {
    version_skew: u16,
    missing_blob: u16,
    anomaly: u16,
    tombstone: u16,
}

#[derive(Clone, Copy)]
struct EcTestVolumes {
    scan: u16,
    repair: u16,
    unrecoverable: u16,
    delete_repair: u16,
    corrupt: u16,
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

    println!(
        "\n{}",
        "=== Test: Repair Skips Partially-Deleted Blobs ==="
            .bold()
            .green()
    );
    test_repair_skips_partially_deleted_blobs().await?;

    // --- Metadata repair tests ---

    let meta_volumes = *META_TEST_VOLUMES.get_or_init(new_meta_test_volumes);
    install_test_metadata_vg_config(meta_volumes)?;
    setup_metadata_fence_token().await?;

    println!(
        "\n{}",
        "=== Test: Meta Scan Detects Version Skew Without Repair ==="
            .bold()
            .green()
    );
    test_meta_scan_detects_version_skew().await?;

    println!(
        "\n{}",
        "=== Test: Meta Repair Heals Version Skew ==="
            .bold()
            .green()
    );
    test_meta_repair_heals_version_skew().await?;

    println!(
        "\n{}",
        "=== Test: Meta Repair Propagates Missing Blob ==="
            .bold()
            .green()
    );
    test_meta_repair_propagates_missing_blob().await?;

    println!(
        "\n{}",
        "=== Test: Meta Scan Reports Same-Version Anomaly ==="
            .bold()
            .green()
    );
    test_meta_scan_reports_anomaly().await?;

    println!(
        "\n{}",
        "=== Test: Meta Repair Propagates Tombstone To Stale Node ==="
            .bold()
            .green()
    );
    test_meta_repair_propagates_tombstone().await?;

    println!(
        "\n{}",
        "=== Test: Meta Delete Is Rejected When Target Has Newer Version ==="
            .bold()
            .green()
    );
    test_meta_delete_version_guard().await?;

    // --- EC repair tests ---

    let ec_volumes = *EC_TEST_VOLUMES.get_or_init(new_ec_test_volumes);
    install_test_ec_data_vg_config(ec_volumes)?;

    println!(
        "\n{}",
        "=== Test: EC Scan Detects Missing Shards ==="
            .bold()
            .green()
    );
    test_ec_scan_detects_missing_shards().await?;

    println!(
        "\n{}",
        "=== Test: EC Repair Reconstructs Missing Shards ==="
            .bold()
            .green()
    );
    test_ec_repair_reconstructs_missing_shards().await?;

    println!(
        "\n{}",
        "=== Test: EC Scan Reports Unrecoverable Blob ==="
            .bold()
            .green()
    );
    test_ec_scan_reports_unrecoverable_blob().await?;

    println!(
        "\n{}",
        "=== Test: EC Repair Skips Tombstoned Blob ==="
            .bold()
            .green()
    );
    test_ec_repair_skips_tombstoned_blob().await?;

    println!(
        "\n{}",
        "=== Test: EC Repair Overwrites Corrupt Shard ==="
            .bold()
            .green()
    );
    test_ec_repair_overwrites_corrupt_shard().await?;

    println!(
        "\n{}",
        "=== Test: EC Scan Followup Clean After Repair ==="
            .bold()
            .green()
    );
    test_ec_scan_followup_clean_after_repair().await?;

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

async fn test_repair_skips_partially_deleted_blobs() -> TestResult {
    let volumes = *TEST_VOLUMES.get().expect("test volumes initialized");
    let blob_guid = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: volumes.delete_repair,
    };
    let body = Bytes::from_static(b"bss-repair-delete-test");

    // Write blob to all 3 nodes
    put_blob("127.0.0.1:8088", blob_guid, 0, body.clone()).await?;
    put_blob("127.0.0.1:8089", blob_guid, 0, body.clone()).await?;
    put_blob("127.0.0.1:8090", blob_guid, 0, body).await?;

    // Delete from node 2 only (simulate partial delete failure)
    delete_blob_from_node("127.0.0.1:8090", blob_guid, 0).await?;

    // Scan should NOT report the deleted blob as a repair candidate
    let volume_id = volumes.delete_repair.to_string();
    let report = run_bss_repair_json(&["scan-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(report.scanned_volumes, 1, "expected one scanned volume");
    assert_eq!(report.failed_volumes, 0, "scan should not fail");
    assert_eq!(
        report.repair_candidates, 0,
        "partially-deleted blob must not be a repair candidate"
    );

    // Repair should also NOT resurrect the blob
    let report = run_bss_repair_json(&["repair-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(report.failed_volumes, 0, "repair should not fail");
    assert_eq!(report.repaired_blobs, 0, "no blobs should be repaired");

    // Verify node 2 still does NOT have the blob (not resurrected)
    let node2_entries = list_keys_on_node("127.0.0.1:8090", volumes.delete_repair).await?;
    assert!(
        node2_entries.is_empty(),
        "deleted blob must not be resurrected on node 2"
    );

    println!("  OK: repair correctly skipped the partially-deleted blob");
    Ok(())
}

async fn delete_blob_from_node(
    addr: &str,
    blob_guid: DataBlobGuid,
    block_number: u32,
) -> TestResult {
    let client = Arc::new(RpcClientBss::new_from_address(
        addr.to_string(),
        Duration::from_secs(5),
    ));
    client
        .delete_data_blob(
            blob_guid,
            block_number,
            Some(Duration::from_secs(5)),
            &TraceId::new(),
            0,
        )
        .await?;
    Ok(())
}

fn install_test_data_vg_config(volumes: TestVolumes) -> CmdResult {
    let data_vg_config = format!(
        r#"{{"volumes":[
{{"volume_id":{},"bss_nodes":[{{"node_id":"bss0","ip":"127.0.0.1","port":8088}},{{"node_id":"bss1","ip":"127.0.0.1","port":8089}},{{"node_id":"bss2","ip":"127.0.0.1","port":8090}}],"mode":{{"type":"replicated","n":3,"r":2,"w":2}}}},
{{"volume_id":{},"bss_nodes":[{{"node_id":"bss0","ip":"127.0.0.1","port":8088}},{{"node_id":"bss1","ip":"127.0.0.1","port":8089}},{{"node_id":"bss2","ip":"127.0.0.1","port":8090}}],"mode":{{"type":"replicated","n":3,"r":2,"w":2}}}},
{{"volume_id":{},"bss_nodes":[{{"node_id":"bss0","ip":"127.0.0.1","port":8088}},{{"node_id":"bss1","ip":"127.0.0.1","port":8089}},{{"node_id":"bss2","ip":"127.0.0.1","port":8090}}],"mode":{{"type":"replicated","n":3,"r":2,"w":2}}}},
{{"volume_id":{},"bss_nodes":[{{"node_id":"bss0","ip":"127.0.0.1","port":8088}},{{"node_id":"bss1","ip":"127.0.0.1","port":8089}},{{"node_id":"bss2","ip":"127.0.0.1","port":8090}}],"mode":{{"type":"replicated","n":3,"r":2,"w":2}}}},
{{"volume_id":{},"bss_nodes":[{{"node_id":"bss0","ip":"127.0.0.1","port":8088}},{{"node_id":"bss1","ip":"127.0.0.1","port":8089}},{{"node_id":"bss2","ip":"127.0.0.1","port":8090}}],"mode":{{"type":"replicated","n":3,"r":2,"w":2}}}}
]}}"#,
        volumes.scan,
        volumes.split_brain,
        volumes.majority,
        volumes.degraded_scan,
        volumes.delete_repair
    );
    let etcdctl = resolve_etcd_bin("etcdctl");

    run_cmd! {
        info "Installing test data vg config into etcd";
        $etcdctl put /fractalbits-service-discovery/bss-data-vg-config $data_vg_config >/dev/null;
    }?;

    Ok(())
}

fn new_test_volumes() -> TestVolumes {
    let seed = (Uuid::now_v7().as_u128() % 9_000) as u16;
    let base = 10_000 + seed * 6;
    TestVolumes {
        scan: base,
        split_brain: base + 1,
        majority: base + 2,
        degraded_scan: base + 3,
        delete_repair: base + 4,
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
                false,
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
    let bss_repair_bin = resolve_binary_path("bss_repair", BuildMode::Debug);
    let output = Command::new(&bss_repair_bin)
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
    let bss_repair_bin = resolve_binary_path("bss_repair", BuildMode::Debug);
    let output = Command::new(&bss_repair_bin)
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
            false,
        )
        .await?;
    Ok(page.blobs.into_iter().map(|entry| entry.key).collect())
}

// --- Metadata repair helpers ---

fn new_meta_test_volumes() -> MetaTestVolumes {
    let seed = (Uuid::now_v7().as_u128() % 9_000) as u16;
    let base = 20_000 + seed * 5;
    MetaTestVolumes {
        version_skew: base,
        missing_blob: base + 1,
        anomaly: base + 2,
        tombstone: base + 3,
    }
}

fn install_test_metadata_vg_config(volumes: MetaTestVolumes) -> CmdResult {
    let metadata_vg_config = format!(
        r#"{{"volumes":[
{{"volume_id":{},"bss_nodes":[{{"node_id":"bss0","ip":"127.0.0.1","port":8088}},{{"node_id":"bss1","ip":"127.0.0.1","port":8089}},{{"node_id":"bss2","ip":"127.0.0.1","port":8090}}]}},
{{"volume_id":{},"bss_nodes":[{{"node_id":"bss0","ip":"127.0.0.1","port":8088}},{{"node_id":"bss1","ip":"127.0.0.1","port":8089}},{{"node_id":"bss2","ip":"127.0.0.1","port":8090}}]}},
{{"volume_id":{},"bss_nodes":[{{"node_id":"bss0","ip":"127.0.0.1","port":8088}},{{"node_id":"bss1","ip":"127.0.0.1","port":8089}},{{"node_id":"bss2","ip":"127.0.0.1","port":8090}}]}},
{{"volume_id":{},"bss_nodes":[{{"node_id":"bss0","ip":"127.0.0.1","port":8088}},{{"node_id":"bss1","ip":"127.0.0.1","port":8089}},{{"node_id":"bss2","ip":"127.0.0.1","port":8090}}]}}
],"quorum":{{"n":3,"r":2,"w":2}}}}"#,
        volumes.version_skew, volumes.missing_blob, volumes.anomaly, volumes.tombstone,
    );
    let etcdctl = resolve_etcd_bin("etcdctl");

    run_cmd! {
        info "Installing test metadata vg config into etcd";
        $etcdctl put /fractalbits-service-discovery/bss-metadata-vg-config $metadata_vg_config >/dev/null;
    }?;

    Ok(())
}

/// Parse a MetaBlobGuid key (Zig extern struct format) back into 16-byte blob_id.
/// Key format: /m{volume_id}/{device_id:x8}-{uuid:x16}-{volume_id:x4}-{salt:x4}
fn parse_meta_blob_id_from_key(key: &str, volume_id: u16) -> [u8; 16] {
    let key = key.trim_end_matches('\0');
    let prefix = format!("/m{volume_id}/");
    let suffix = key
        .strip_prefix(&prefix)
        .expect("key should have volume prefix");
    let parts: Vec<&str> = suffix.split('-').collect();
    assert_eq!(
        parts.len(),
        4,
        "MetaBlobGuid should have 4 dash-separated hex parts"
    );

    let device_id = u32::from_str_radix(parts[0], 16).expect("valid device_id hex");
    let uuid_val = u64::from_str_radix(parts[1], 16).expect("valid uuid hex");
    let vol_id = u16::from_str_radix(parts[2], 16).expect("valid volume_id hex");
    let salt = u16::from_str_radix(parts[3], 16).expect("valid salt hex");

    let mut blob_id = [0u8; 16];
    blob_id[0..8].copy_from_slice(&uuid_val.to_le_bytes());
    blob_id[8..12].copy_from_slice(&device_id.to_le_bytes());
    blob_id[12..14].copy_from_slice(&vol_id.to_le_bytes());
    blob_id[14..16].copy_from_slice(&salt.to_le_bytes());
    blob_id
}

async fn setup_metadata_fence_token() -> TestResult {
    let timeout = Some(Duration::from_secs(5));
    let trace_id = TraceId::new();
    for port in [8088, 8089, 8090] {
        let client = Arc::new(RpcClientBss::new_from_address(
            format!("127.0.0.1:{port}"),
            Duration::from_secs(5),
        ));
        client
            .handshake(META_FENCE_TOKEN, timeout, &trace_id, 0)
            .await?;
    }
    Ok(())
}

async fn put_meta_blob_on_node(
    addr: &str,
    blob_id: [u8; 16],
    volume_id: u16,
    body: Bytes,
    version: u64,
    is_new: bool,
) -> TestResult {
    let client = Arc::new(RpcClientBss::new_from_address(
        addr.to_string(),
        Duration::from_secs(5),
    ));
    let checksum = xxhash_rust::xxh3::xxh3_64(&body);
    client
        .put_metadata_blob(
            blob_id,
            volume_id,
            body,
            checksum,
            version,
            is_new,
            META_FENCE_TOKEN,
            Some(Duration::from_secs(5)),
            &TraceId::new(),
            0,
        )
        .await?;
    Ok(())
}

async fn get_meta_blob_from_node(
    addr: &str,
    blob_id: [u8; 16],
    volume_id: u16,
    content_len: usize,
) -> TestResult<Bytes> {
    let client = Arc::new(RpcClientBss::new_from_address(
        addr.to_string(),
        Duration::from_secs(5),
    ));
    let body = client
        .get_metadata_blob(
            blob_id,
            volume_id,
            content_len,
            META_FENCE_TOKEN,
            Some(Duration::from_secs(5)),
            &TraceId::new(),
            0,
        )
        .await?;
    Ok(body)
}

async fn delete_meta_blob_on_node(
    addr: &str,
    blob_id: [u8; 16],
    volume_id: u16,
    version: u64,
) -> TestResult {
    let client = Arc::new(RpcClientBss::new_from_address(
        addr.to_string(),
        Duration::from_secs(5),
    ));
    client
        .delete_metadata_blob(
            blob_id,
            volume_id,
            version,
            META_FENCE_TOKEN,
            Some(Duration::from_secs(5)),
            &TraceId::new(),
            0,
        )
        .await?;
    Ok(())
}

async fn list_meta_keys_on_node(addr: &str, volume_id: u16) -> TestResult<Vec<String>> {
    let client = Arc::new(RpcClientBss::new_from_address(
        addr.to_string(),
        Duration::from_secs(5),
    ));
    let page = client
        .list_data_blobs(
            volume_id,
            &format!("/m{volume_id}/"),
            "",
            100,
            Some(Duration::from_secs(5)),
            &TraceId::new(),
            0,
            false,
        )
        .await?;
    Ok(page.blobs.into_iter().map(|entry| entry.key).collect())
}

fn run_bss_repair_meta_json(args: &[&str]) -> TestResult<MetaRepairReport> {
    let bss_repair_bin = resolve_binary_path("bss_repair", BuildMode::Debug);
    let output = Command::new(&bss_repair_bin)
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

    Ok(serde_json::from_slice::<MetaRepairReport>(&output.stdout)?)
}

// --- Metadata test cases ---

async fn test_meta_scan_detects_version_skew() -> TestResult {
    let volumes = *META_TEST_VOLUMES
        .get()
        .expect("meta test volumes initialized");
    let blob_id = *Uuid::now_v7().as_bytes();
    let body = Bytes::from_static(b"meta-version-skew-test");
    let body_old = Bytes::from_static(b"meta-old-version");

    // Put v=5 on nodes 0,1; v=3 on node 2
    put_meta_blob_on_node(
        "127.0.0.1:8088",
        blob_id,
        volumes.version_skew,
        body.clone(),
        5,
        true,
    )
    .await?;
    put_meta_blob_on_node(
        "127.0.0.1:8089",
        blob_id,
        volumes.version_skew,
        body.clone(),
        5,
        true,
    )
    .await?;
    put_meta_blob_on_node(
        "127.0.0.1:8090",
        blob_id,
        volumes.version_skew,
        body_old,
        3,
        true,
    )
    .await?;

    let volume_id = volumes.version_skew.to_string();
    let fence_token = META_FENCE_TOKEN.to_string();
    let report = run_bss_repair_meta_json(&[
        "scan-meta",
        "--volume-id",
        &volume_id,
        "--fence-token",
        &fence_token,
        "--json",
    ])?;

    assert_eq!(report.scanned_volumes, 1, "expected one scanned volume");
    assert_eq!(report.failed_volumes, 0, "scan-only should not fail");
    assert_eq!(report.scanned_blobs, 1, "expected one scanned blob");
    assert_eq!(report.repair_candidates, 1, "expected one repair candidate");
    assert_eq!(report.repaired_blobs, 0, "scan-only must not repair");

    println!("  OK: scan-meta detected version skew without repairing");
    Ok(())
}

async fn test_meta_repair_heals_version_skew() -> TestResult {
    let volumes = *META_TEST_VOLUMES
        .get()
        .expect("meta test volumes initialized");
    let body = Bytes::from_static(b"meta-version-skew-test");

    let volume_id = volumes.version_skew.to_string();
    let fence_token = META_FENCE_TOKEN.to_string();
    let report = run_bss_repair_meta_json(&[
        "repair-meta",
        "--volume-id",
        &volume_id,
        "--fence-token",
        &fence_token,
        "--json",
    ])?;

    assert_eq!(report.failed_volumes, 0, "repair should succeed");
    assert_eq!(report.repair_candidates, 1, "expected one repair candidate");
    assert_eq!(report.repaired_blobs, 1, "expected one repaired blob");

    // Verify node 2 now has the latest version's content
    let keys = list_meta_keys_on_node("127.0.0.1:8090", volumes.version_skew).await?;
    assert_eq!(keys.len(), 1, "node 2 should have the blob");

    // Read and verify body from node 2
    let blob_id = parse_meta_blob_id_from_key(&keys[0], volumes.version_skew);
    let repaired_body =
        get_meta_blob_from_node("127.0.0.1:8090", blob_id, volumes.version_skew, body.len())
            .await?;
    assert_eq!(repaired_body, body, "repaired blob should match source");

    // Follow-up scan should be clean
    let post_repair = run_bss_repair_meta_json(&[
        "scan-meta",
        "--volume-id",
        &volume_id,
        "--fence-token",
        &fence_token,
        "--json",
    ])?;
    assert_eq!(
        post_repair.repair_candidates, 0,
        "healthy volume should be clean after repair"
    );

    println!("  OK: repair-meta healed version skew and follow-up scan was clean");
    Ok(())
}

async fn test_meta_repair_propagates_missing_blob() -> TestResult {
    let volumes = *META_TEST_VOLUMES
        .get()
        .expect("meta test volumes initialized");
    let blob_id = *Uuid::now_v7().as_bytes();
    let body = Bytes::from_static(b"meta-missing-blob-test");

    // Put v=5 on nodes 0,1 only (node 2 is missing)
    put_meta_blob_on_node(
        "127.0.0.1:8088",
        blob_id,
        volumes.missing_blob,
        body.clone(),
        5,
        true,
    )
    .await?;
    put_meta_blob_on_node(
        "127.0.0.1:8089",
        blob_id,
        volumes.missing_blob,
        body.clone(),
        5,
        true,
    )
    .await?;

    let volume_id = volumes.missing_blob.to_string();
    let fence_token = META_FENCE_TOKEN.to_string();
    let report = run_bss_repair_meta_json(&[
        "repair-meta",
        "--volume-id",
        &volume_id,
        "--fence-token",
        &fence_token,
        "--json",
    ])?;

    assert_eq!(report.failed_volumes, 0, "repair should succeed");
    assert_eq!(report.repair_candidates, 1, "expected one repair candidate");
    assert_eq!(report.repaired_blobs, 1, "expected one repaired blob");

    // Verify node 2 now has the blob
    let repaired_body =
        get_meta_blob_from_node("127.0.0.1:8090", blob_id, volumes.missing_blob, body.len())
            .await?;
    assert_eq!(repaired_body, body, "repaired blob should match source");

    println!("  OK: repair-meta propagated missing metadata blob to node 2");
    Ok(())
}

async fn test_meta_scan_reports_anomaly() -> TestResult {
    let volumes = *META_TEST_VOLUMES
        .get()
        .expect("meta test volumes initialized");
    let blob_id = *Uuid::now_v7().as_bytes();
    let body_a = Bytes::from_static(b"anomaly-body-aaa");
    let body_b = Bytes::from_static(b"anomaly-body-bbb");

    // Put same version (v=5) with different bodies on nodes 0 and 1
    put_meta_blob_on_node("127.0.0.1:8088", blob_id, volumes.anomaly, body_a, 5, true).await?;
    put_meta_blob_on_node("127.0.0.1:8089", blob_id, volumes.anomaly, body_b, 5, true).await?;

    let volume_id = volumes.anomaly.to_string();
    let fence_token = META_FENCE_TOKEN.to_string();
    let report = run_bss_repair_meta_json(&[
        "scan-meta",
        "--volume-id",
        &volume_id,
        "--fence-token",
        &fence_token,
        "--json",
    ])?;

    assert_eq!(report.scanned_volumes, 1, "expected one scanned volume");
    assert_eq!(report.failed_volumes, 0, "anomaly should not fail the scan");
    assert_eq!(report.anomalies, 1, "expected one anomaly");
    assert_eq!(
        report.repair_candidates, 0,
        "anomalies must not become repair candidates"
    );

    println!("  OK: scan-meta reported same-version checksum divergence as anomaly");
    Ok(())
}

async fn test_meta_repair_propagates_tombstone() -> TestResult {
    let volumes = *META_TEST_VOLUMES
        .get()
        .expect("meta test volumes initialized");
    let blob_id = *Uuid::now_v7().as_bytes();
    let body = Bytes::from_static(b"meta-tombstone-test");

    // Put v=5 on all 3 nodes
    for port in [8088, 8089, 8090] {
        put_meta_blob_on_node(
            &format!("127.0.0.1:{port}"),
            blob_id,
            volumes.tombstone,
            body.clone(),
            5,
            true,
        )
        .await?;
    }

    // Update to v=7 on nodes 0,1 only, then delete on nodes 0,1.
    // This leaves: nodes 0,1 have tombstone at v=7, node 2 has live v=5.
    for port in [8088, 8089] {
        put_meta_blob_on_node(
            &format!("127.0.0.1:{port}"),
            blob_id,
            volumes.tombstone,
            body.clone(),
            7,
            false,
        )
        .await?;
        delete_meta_blob_on_node(&format!("127.0.0.1:{port}"), blob_id, volumes.tombstone, 7)
            .await?;
    }

    let volume_id = volumes.tombstone.to_string();
    let fence_token = META_FENCE_TOKEN.to_string();

    // Scan should detect 1 repair candidate (node 2 is stale at v=5)
    let scan_report = run_bss_repair_meta_json(&[
        "scan-meta",
        "--volume-id",
        &volume_id,
        "--fence-token",
        &fence_token,
        "--json",
    ])?;
    assert_eq!(
        scan_report.repair_candidates, 1,
        "stale node 2 should be a repair candidate"
    );

    // Repair should propagate delete to node 2
    let repair_report = run_bss_repair_meta_json(&[
        "repair-meta",
        "--volume-id",
        &volume_id,
        "--fence-token",
        &fence_token,
        "--json",
    ])?;
    assert_eq!(repair_report.failed_volumes, 0, "repair should succeed");
    assert_eq!(
        repair_report.repaired_blobs, 1,
        "expected one repaired blob"
    );

    // Follow-up scan should be clean (tombstone propagated, no more stale nodes)
    let post_repair = run_bss_repair_meta_json(&[
        "scan-meta",
        "--volume-id",
        &volume_id,
        "--fence-token",
        &fence_token,
        "--json",
    ])?;
    assert_eq!(
        post_repair.repair_candidates, 0,
        "volume should be clean after tombstone repair"
    );
    assert_eq!(post_repair.anomalies, 0, "no anomalies expected");

    println!("  OK: repair-meta propagated tombstone to stale node and converged");
    Ok(())
}

async fn test_meta_delete_version_guard() -> TestResult {
    let volumes = *META_TEST_VOLUMES
        .get()
        .expect("meta test volumes initialized");
    let blob_id = *Uuid::now_v7().as_bytes();
    let body = Bytes::from_static(b"meta-delete-guard-test");

    // Put v=10 on node 0
    put_meta_blob_on_node(
        "127.0.0.1:8088",
        blob_id,
        volumes.tombstone,
        body.clone(),
        10,
        true,
    )
    .await?;

    // Attempt to delete with version=5 (stale). The server's erase_check_fn
    // should reject this because the existing version (10) > request version (5).
    let client = Arc::new(RpcClientBss::new_from_address(
        "127.0.0.1:8088".to_string(),
        Duration::from_secs(5),
    ));
    let result = client
        .delete_metadata_blob(
            blob_id,
            volumes.tombstone,
            5, // stale version
            META_FENCE_TOKEN,
            Some(Duration::from_secs(5)),
            &TraceId::new(),
            0,
        )
        .await;

    assert!(
        matches!(result, Err(RpcErrorBss::VersionSkipped)),
        "delete with stale version should be rejected with VersionSkipped, got: {result:?}"
    );

    // Verify the blob is still alive at v=10
    let fetched =
        get_meta_blob_from_node("127.0.0.1:8088", blob_id, volumes.tombstone, body.len()).await?;
    assert_eq!(
        fetched, body,
        "blob should be unchanged after rejected delete"
    );

    println!("  OK: metadata delete with stale version was rejected (VersionSkipped)");
    Ok(())
}

// --- EC repair helpers ---

fn new_ec_test_volumes() -> EcTestVolumes {
    let seed = (Uuid::now_v7().as_u128() % 900) as u16;
    let base = 0x8000 + seed * 6;
    EcTestVolumes {
        scan: base,
        repair: base + 1,
        unrecoverable: base + 2,
        delete_repair: base + 3,
        corrupt: base + 4,
    }
}

fn install_test_ec_data_vg_config(volumes: EcTestVolumes) -> CmdResult {
    let node_list = (0..EC_TOTAL as u16)
        .map(|i| {
            format!(
                r#"{{"node_id":"bss{i}","ip":"127.0.0.1","port":{}}}"#,
                8088 + i
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let mode = format!(
        r#"{{"type":"erasure_coded","data_shards":{},"parity_shards":{}}}"#,
        EC_K, EC_M
    );

    let ec_volumes: Vec<String> = [
        volumes.scan,
        volumes.repair,
        volumes.unrecoverable,
        volumes.delete_repair,
        volumes.corrupt,
    ]
    .iter()
    .map(|vid| {
        format!(
            r#"{{"volume_id":{},"bss_nodes":[{}],"mode":{}}}"#,
            vid, node_list, mode
        )
    })
    .collect();

    let data_vg_config = format!(r#"{{"volumes":[{}]}}"#, ec_volumes.join(","));
    let etcdctl = resolve_etcd_bin("etcdctl");

    run_cmd! {
        info "Installing test EC data vg config into etcd";
        $etcdctl put /fractalbits-service-discovery/bss-data-vg-config $data_vg_config >/dev/null;
    }?;

    Ok(())
}

/// Encode body with Reed-Solomon and write individual shards to the 6 BSS nodes.
/// `skip_shard_indices` allows skipping specific shard indices to simulate missing shards.
/// Returns the shard size for later verification.
async fn write_ec_shards_partial(
    blob_guid: DataBlobGuid,
    block_number: u32,
    body: &[u8],
    skip_shard_indices: &[usize],
) -> TestResult<usize> {
    let padded_len = ec_padded_len(body.len(), EC_K);
    let mut padded = vec![0u8; padded_len];
    padded[..body.len()].copy_from_slice(body);

    let shard_size = padded_len / EC_K;
    let data_shards: Vec<&[u8]> = padded.chunks(shard_size).collect();
    let parity_shards = rs_encode(EC_K, EC_M, &data_shards)
        .map_err(|e| BssRepairTestError::CommandFailed(format!("RS encode failed: {e}")))?;

    // Build all shard data: first k data shards, then m parity shards
    let mut all_shards: Vec<Vec<u8>> = Vec::with_capacity(EC_TOTAL);
    for chunk in padded.chunks(shard_size) {
        all_shards.push(chunk.to_vec());
    }
    for parity in &parity_shards {
        all_shards.push(parity.clone());
    }
    assert_eq!(all_shards.len(), EC_TOTAL);

    // Determine rotation and write each shard to its assigned node
    let rotation = ec_rotation(&blob_guid.blob_id, EC_TOTAL as u32);

    for (shard_idx, shard) in all_shards.iter().enumerate() {
        if skip_shard_indices.contains(&shard_idx) {
            continue;
        }
        let ni = node_index(shard_idx, rotation, EC_TOTAL);
        let addr = format!("127.0.0.1:{}", 8088 + ni as u16);
        let shard_data = Bytes::from(shard.clone());
        put_blob(&addr, blob_guid, block_number, shard_data).await?;
    }

    Ok(shard_size)
}

/// Write all shards (convenience wrapper).
async fn write_ec_shards(
    blob_guid: DataBlobGuid,
    block_number: u32,
    body: &[u8],
) -> TestResult<usize> {
    write_ec_shards_partial(blob_guid, block_number, body, &[]).await
}

fn ec_node_clients() -> Vec<Arc<RpcClientBss>> {
    (0..EC_TOTAL)
        .map(|i| {
            Arc::new(RpcClientBss::new_from_address(
                format!("127.0.0.1:{}", 8088 + i),
                Duration::from_secs(5),
            ))
        })
        .collect()
}

async fn list_ec_keys_on_node(addr: &str, volume_id: u16) -> TestResult<Vec<String>> {
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
            false,
        )
        .await?;
    Ok(page.blobs.into_iter().map(|entry| entry.key).collect())
}

/// Delete a blob from a specific node (used to simulate shard loss)
async fn delete_ec_shard(
    blob_guid: DataBlobGuid,
    block_number: u32,
    node_idx: usize,
) -> TestResult {
    let addr = format!("127.0.0.1:{}", 8088 + node_idx as u16);
    delete_blob_from_node(&addr, blob_guid, block_number).await
}

// --- EC test cases ---

async fn test_ec_scan_detects_missing_shards() -> TestResult {
    let volumes = *EC_TEST_VOLUMES.get().expect("ec test volumes initialized");
    let blob_guid = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: volumes.scan,
    };
    let body = b"ec-scan-detect-missing-shards-test-body-padding-data-here";

    // Write only 4 of 6 shards (skip shard indices 0 and 1)
    let _shard_size = write_ec_shards_partial(blob_guid, 0, body, &[0, 1]).await?;

    let volume_id = volumes.scan.to_string();
    let report = run_bss_repair_json(&["scan-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(report.scanned_volumes, 1, "expected one scanned volume");
    assert_eq!(report.failed_volumes, 0, "scan-only should not fail");
    assert_eq!(report.scanned_blobs, 1, "expected one scanned blob");
    assert_eq!(report.repair_candidates, 1, "expected one repair candidate");
    assert_eq!(report.repaired_blobs, 0, "scan-only must not repair");

    println!("  OK: EC scan detected missing shards without repairing");
    Ok(())
}

async fn test_ec_repair_reconstructs_missing_shards() -> TestResult {
    let volumes = *EC_TEST_VOLUMES.get().expect("ec test volumes initialized");
    let blob_guid = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: volumes.repair,
    };
    let body = b"ec-repair-reconstruct-missing-shards-test-body-padding";

    // Write only 4 of 6 shards (skip shard indices 0 and 1)
    let shard_size = write_ec_shards_partial(blob_guid, 0, body, &[0, 1]).await?;

    let volume_id = volumes.repair.to_string();
    let report = run_bss_repair_json(&["repair-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(report.failed_volumes, 0, "repair should succeed");
    assert_eq!(report.repair_candidates, 1, "expected one repair candidate");
    assert_eq!(report.repaired_blobs, 1, "expected one repaired blob");
    assert_eq!(report.failed_repairs, 0, "expected no failed repairs");

    // Verify all 6 shards exist and are readable
    let clients = ec_node_clients();
    for (ni, client) in clients.iter().enumerate() {
        let mut shard_body = Bytes::new();
        client
            .get_data_blob(
                blob_guid,
                0,
                &mut shard_body,
                shard_size,
                Some(Duration::from_secs(5)),
                &TraceId::new(),
                0,
            )
            .await
            .unwrap_or_else(|e| panic!("should read shard from node {ni}: {e}"));
        assert_eq!(
            shard_body.len(),
            shard_size,
            "shard on node {ni} has wrong size"
        );
    }

    println!("  OK: EC repair reconstructed missing shards and all 6 shards verified");
    Ok(())
}

async fn test_ec_scan_reports_unrecoverable_blob() -> TestResult {
    let volumes = *EC_TEST_VOLUMES.get().expect("ec test volumes initialized");
    let blob_guid = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: volumes.unrecoverable,
    };
    let body = b"ec-unrecoverable-blob-test-body-padding-data-here-xxx";

    // Write only 3 of 6 shards (skip 3, exceeds m=2, so unrecoverable)
    let _shard_size = write_ec_shards_partial(blob_guid, 0, body, &[0, 1, 2]).await?;

    let volume_id = volumes.unrecoverable.to_string();
    let report = run_bss_repair_json(&["scan-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(report.scanned_volumes, 1, "expected one scanned volume");
    assert_eq!(report.failed_volumes, 0, "scan should not fail");
    assert_eq!(
        report.unrecoverable_blobs, 1,
        "expected one unrecoverable blob"
    );
    assert_eq!(
        report.repair_candidates, 0,
        "unrecoverable blob should not be a repair candidate"
    );

    println!("  OK: EC scan correctly reported unrecoverable blob (3 missing > m=2)");
    Ok(())
}

async fn test_ec_repair_skips_tombstoned_blob() -> TestResult {
    let volumes = *EC_TEST_VOLUMES.get().expect("ec test volumes initialized");
    let blob_guid = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: volumes.delete_repair,
    };
    let body = b"ec-tombstone-skip-test-body-padding-data-here-xxxx";

    let _shard_size = write_ec_shards(blob_guid, 0, body).await?;

    // Delete blob on 2 nodes (creating is_deleted tombstones)
    let rotation = ec_rotation(&blob_guid.blob_id, EC_TOTAL as u32);
    let target_node_0 = node_index(0, rotation, EC_TOTAL);
    let target_node_1 = node_index(1, rotation, EC_TOTAL);
    delete_ec_shard(blob_guid, 0, target_node_0).await?;
    delete_ec_shard(blob_guid, 0, target_node_1).await?;

    // The k-way merge will see: 4 live + 2 deleted nodes.
    // With k+1=5 scan quorum, it will see enough to proceed.
    // The 2 deleted entries should prevent repair (tombstone = skip).
    let volume_id = volumes.delete_repair.to_string();
    let report = run_bss_repair_json(&["repair-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(report.failed_volumes, 0, "repair should not fail");
    assert_eq!(
        report.repaired_blobs, 0,
        "tombstoned blob should not be repaired"
    );

    // Verify deleted nodes still don't have the blob
    let keys_0 = list_ec_keys_on_node(
        &format!("127.0.0.1:{}", 8088 + target_node_0 as u16),
        volumes.delete_repair,
    )
    .await?;
    assert!(
        keys_0.is_empty(),
        "deleted shard on node {target_node_0} must not be resurrected"
    );

    println!("  OK: EC repair correctly skipped tombstoned blob");
    Ok(())
}

async fn test_ec_repair_overwrites_corrupt_shard() -> TestResult {
    let volumes = *EC_TEST_VOLUMES.get().expect("ec test volumes initialized");
    let blob_guid = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: volumes.corrupt,
    };
    let body = b"ec-corrupt-shard-repair-test-body-padding-data-xxxx";

    // Write 5 good shards, deliberately skip shard index 0 so we can write
    // garbage to that slot as a fresh put (no overwrite conflict with BSS).
    let shard_size = write_ec_shards_partial(blob_guid, 0, body, &[0]).await?;

    // Put a wrong-size shard to the node for shard index 0 (simulates corruption).
    // Because shard 0 was never written, this is a fresh write — no BSS overwrite
    // check applies. The scanner will see this shard's size differs from the
    // majority and classify it as corrupt.
    let rotation = ec_rotation(&blob_guid.blob_id, EC_TOTAL as u32);
    let corrupt_node = node_index(0, rotation, EC_TOTAL);
    let corrupt_addr = format!("127.0.0.1:{}", 8088 + corrupt_node as u16);
    let garbage = Bytes::from(vec![0xFFu8; shard_size + 1]);
    put_blob(&corrupt_addr, blob_guid, 0, garbage).await?;

    let volume_id = volumes.corrupt.to_string();
    let report = run_bss_repair_json(&["repair-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(report.failed_volumes, 0, "repair should succeed");
    assert_eq!(report.repaired_blobs, 1, "expected one repaired blob");
    assert_eq!(report.failed_repairs, 0, "expected no failed repairs");

    // Verify the corrupt shard was fixed: read it back and check size
    let mut repaired_shard = Bytes::new();
    let client = Arc::new(RpcClientBss::new_from_address(
        corrupt_addr,
        Duration::from_secs(5),
    ));
    client
        .get_data_blob(
            blob_guid,
            0,
            &mut repaired_shard,
            shard_size,
            Some(Duration::from_secs(5)),
            &TraceId::new(),
            0,
        )
        .await
        .expect("should read repaired shard");
    assert_eq!(
        repaired_shard.len(),
        shard_size,
        "repaired shard wrong size"
    );
    println!("  OK: EC repair overwrote corrupt shard with correct data");
    Ok(())
}

async fn test_ec_scan_followup_clean_after_repair() -> TestResult {
    let volumes = *EC_TEST_VOLUMES.get().expect("ec test volumes initialized");
    let blob_guid = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: volumes.repair,
    };
    let body = b"ec-followup-clean-test-body-padding-data-here-xxxxx";

    // Write 5 of 6 shards (skip shard index 0)
    let _shard_size = write_ec_shards_partial(blob_guid, 0, body, &[0]).await?;

    // Repair
    let volume_id = volumes.repair.to_string();
    let report = run_bss_repair_json(&["repair-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(report.failed_volumes, 0, "repair should succeed");
    assert!(
        report.repaired_blobs >= 1,
        "expected at least one repaired blob"
    );

    // Follow-up scan should be clean
    let post_repair = run_bss_repair_json(&["scan-data", "--volume-id", &volume_id, "--json"])?;
    assert_eq!(post_repair.failed_volumes, 0, "scan should succeed");
    assert_eq!(
        post_repair.repair_candidates, 0,
        "volume should be clean after repair"
    );

    println!("  OK: EC follow-up scan was clean after repair");
    Ok(())
}
