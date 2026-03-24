use std::sync::Arc;
use std::time::Duration;

use bss_repair::{DataVolumeScanner, RepairConfig};
use bytes::Bytes;
use cmd_lib::*;
use colored::*;
use data_types::{BssNode, DataBlobGuid, TraceId, Volume, VolumeMode};
use rpc_client_bss::RpcClientBss;
use uuid::Uuid;

use crate::CmdResult;

type TestResult<T = ()> = Result<T, BssRepairTestError>;

#[derive(Debug, thiserror::Error)]
enum BssRepairTestError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Rpc(#[from] rpc_client_bss::RpcErrorBss),

    #[error(transparent)]
    Repair(#[from] bss_repair::RepairError),
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

    println!(
        "\n{}",
        "=== Test: Replicated Data Volume Scan And Repair ==="
            .bold()
            .green()
    );
    test_replicated_data_volume_scan_and_repair().await?;

    println!("\n{}", "=== All BSS Repair Tests PASSED ===".green().bold());
    Ok(())
}

async fn test_replicated_data_volume_scan_and_repair() -> TestResult {
    let volume_id = 77;
    let volume = test_volume(volume_id);
    let config = RepairConfig {
        scan_batch_size: 16,
        rpc_connection_timeout: Duration::from_secs(5),
        rpc_request_timeout: Duration::from_secs(5),
        list_retry_count: 0,
    };
    let scanner = DataVolumeScanner::new(config, volume.clone())?;

    let blob_guid = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id,
    };
    let block_number = 0;
    let body = Bytes::from_static(b"bss-repair-e2e-body");
    let checksum = xxhash_rust::xxh3::xxh3_64(&body);
    let trace_id = TraceId::new();

    let node0 = Arc::new(RpcClientBss::new_from_address(
        "127.0.0.1:8088".to_string(),
        Duration::from_secs(5),
    ));
    let node1 = Arc::new(RpcClientBss::new_from_address(
        "127.0.0.1:8089".to_string(),
        Duration::from_secs(5),
    ));
    let node2 = Arc::new(RpcClientBss::new_from_address(
        "127.0.0.1:8090".to_string(),
        Duration::from_secs(5),
    ));

    node0
        .put_data_blob(
            blob_guid,
            block_number,
            body.clone(),
            checksum,
            Some(Duration::from_secs(5)),
            &trace_id,
            0,
        )
        .await?;
    node1
        .put_data_blob(
            blob_guid,
            block_number,
            body.clone(),
            checksum,
            Some(Duration::from_secs(5)),
            &trace_id,
            0,
        )
        .await?;

    let scan_result = scanner.scan_once().await?;
    assert_eq!(
        scan_result.scanned_blobs, 1,
        "expected exactly one scanned blob"
    );
    assert_eq!(
        scan_result.repair_candidates.len(),
        1,
        "expected one under-replicated blob"
    );
    let candidate = &scan_result.repair_candidates[0];
    assert_eq!(candidate.present_on_nodes, vec!["bss0", "bss1"]);
    assert_eq!(candidate.missing_on_nodes, vec!["bss2"]);

    let repair_result = scanner.scan_and_repair().await?;
    assert_eq!(
        repair_result.scanned_blobs, 1,
        "repair scan should see one blob"
    );
    assert_eq!(
        repair_result.repaired_blobs, 1,
        "expected one repaired blob"
    );

    let mut repaired_body = Bytes::new();
    node2
        .get_data_blob(
            blob_guid,
            block_number,
            &mut repaired_body,
            body.len(),
            Some(Duration::from_secs(5)),
            &TraceId::new(),
            0,
        )
        .await?;
    assert_eq!(repaired_body, body, "repaired replica content mismatch");

    let post_repair = scanner.scan_once().await?;
    assert_eq!(
        post_repair.scanned_blobs, 1,
        "post-repair scan should still see one blob"
    );
    assert!(
        post_repair.repair_candidates.is_empty(),
        "post-repair scan should find no missing replicas"
    );

    println!("  OK: under-replicated blob was detected and repaired end-to-end");
    Ok(())
}

fn test_volume(volume_id: u16) -> Volume {
    Volume {
        volume_id,
        bss_nodes: vec![
            BssNode {
                node_id: "bss0".to_string(),
                ip: "127.0.0.1".to_string(),
                port: 8088,
            },
            BssNode {
                node_id: "bss1".to_string(),
                ip: "127.0.0.1".to_string(),
                port: 8089,
            },
            BssNode {
                node_id: "bss2".to_string(),
                ip: "127.0.0.1".to_string(),
                port: 8090,
            },
        ],
        mode: VolumeMode::Replicated { n: 3, r: 2, w: 2 },
    }
}
