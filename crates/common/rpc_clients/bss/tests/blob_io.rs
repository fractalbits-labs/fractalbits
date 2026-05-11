use bytes::Bytes;
use data_types::{DataBlobGuid, TraceId};
use fake::Fake;
use rpc_client_bss::*;
use std::time::Duration;
use tracing_test::traced_test;
use uuid::Uuid;

async fn is_server_reachable(url: &str) -> bool {
    tokio::net::TcpStream::connect(url).await.is_ok()
}

#[tokio::test]
#[traced_test]
async fn test_basic_blob_io_with_fixed_bytes() {
    let url = "127.0.0.1:9225";
    tracing::debug!(%url);

    if !is_server_reachable(url).await {
        tracing::info!("Blob storage server not reachable at {url}, skipping test");
        return;
    }

    let rpc_client = RpcClientBss::new_from_address(url.to_string(), Duration::from_secs(5));

    for _ in 0..1 {
        let blob_guid = DataBlobGuid {
            blob_id: Uuid::now_v7(),
            volume_id: 1,
        };
        let content: Bytes = vec![0xff; 1024 * 1024 - 256].into();
        let body_checksum = xxhash_rust::xxh3::xxh3_64(&content);
        let mut readback_content = Bytes::new();
        rpc_client
            .put_data_blob(
                blob_guid,
                0,
                content.clone(),
                body_checksum,
                1,
                None,
                &TraceId::new(),
                0,
            )
            .await
            .unwrap();

        rpc_client
            .get_data_blob(
                blob_guid,
                0,
                &mut readback_content,
                content.len(),
                None,
                &TraceId::new(),
                0,
            )
            .await
            .unwrap();
        assert_eq!(content, readback_content);
    }
}

#[tokio::test]
#[traced_test]
async fn test_bss_batch_put_delete_reserve() {
    // 9225 is the legacy probe port; 8088 is the local `just service`
    // default. Try both so this test runs both in the legacy harness
    // and against `just service start` without manual config.
    let url = if is_server_reachable("127.0.0.1:9225").await {
        "127.0.0.1:9225"
    } else if is_server_reachable("127.0.0.1:8088").await {
        "127.0.0.1:8088"
    } else {
        tracing::info!("No BSS reachable at 9225 or 8088; skipping test");
        return;
    };
    let rpc_client = RpcClientBss::new_from_address(url.to_string(), Duration::from_secs(5));

    // Single-blob batch with all three sub-op kinds at distinct
    // block_numbers exercises the dispatcher's per-op fan-out.
    // BssBatch is per-blob: every sub-op shares the envelope
    // commit_blob_id.
    let blob_guid = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: 1,
    };
    let put_body: Bytes = vec![0xab; 4096].into();
    let put_chk = xxhash_rust::xxh3::xxh3_64(&put_body);

    // Pre-populate the delete-target block so the batch sub-Delete
    // actually tombstones a real entry rather than getting NotFound.
    rpc_client
        .put_data_blob(
            blob_guid,
            1,
            Bytes::from(vec![0xcd; 4096]),
            xxhash_rust::xxh3::xxh3_64(&[0xcd; 4096]),
            1,
            None,
            &TraceId::new(),
            0,
        )
        .await
        .expect("seed delete-target");

    let sub_ops = vec![
        BssBatchSubOp::PutDataBlob {
            blob_guid,
            block_number: 0,
            body: put_body.clone(),
            body_checksum: put_chk,
            version: 2,
        },
        BssBatchSubOp::DeleteDataBlob {
            blob_guid,
            block_number: 1,
            version: 2,
        },
        BssBatchSubOp::ReserveBlocks {
            blob_guid,
            block_number: 2,
            block_size: 4096,
            expected_version: 2,
        },
    ];

    let commit = rpc_client_bss::BlobCommitInfo {
        blob_guid,
        blob_version: 2,
        total_size: 4096,
        block_count: 1,
    };
    let results = rpc_client
        .put_bss_batch(sub_ops, commit, None, &TraceId::new(), 0)
        .await
        .expect("batch send");

    assert_eq!(results.len(), 3, "one result per sub-op");
    assert!(
        results[0].status.is_ok(),
        "Put sub failed: {:?}",
        results[0].status
    );
    assert!(
        results[2].status.is_ok(),
        "Reserve sub failed: {:?}",
        results[2].status
    );

    // Verify the Put landed by reading it back.
    let mut readback = Bytes::new();
    rpc_client
        .get_data_blob(
            blob_guid,
            0,
            &mut readback,
            put_body.len(),
            None,
            &TraceId::new(),
            0,
        )
        .await
        .expect("readback put");
    assert_eq!(readback, put_body);
}

#[tokio::test]
#[traced_test]
async fn test_bss_batch_burst_puts() {
    let url = if is_server_reachable("127.0.0.1:9225").await {
        "127.0.0.1:9225"
    } else if is_server_reachable("127.0.0.1:8088").await {
        "127.0.0.1:8088"
    } else {
        tracing::info!("No BSS reachable at 9225 or 8088; skipping test");
        return;
    };
    let rpc_client = RpcClientBss::new_from_address(url.to_string(), Duration::from_secs(10));

    // Burst of 16 single-blob puts (distinct block_numbers) in one
    // batch -- stresses sub-blob-buf pool and per-sub response
    // accumulation. BssBatch is per-blob.
    const N: usize = 16;
    let blob_guid = DataBlobGuid {
        blob_id: Uuid::now_v7(),
        volume_id: 1,
    };
    let mut sub_ops: Vec<BssBatchSubOp> = Vec::with_capacity(N);
    let mut expected: Vec<(u32, Bytes)> = Vec::with_capacity(N);
    for i in 0..N {
        let body: Bytes = vec![(i as u8).wrapping_mul(7); 4096].into();
        let chk = xxhash_rust::xxh3::xxh3_64(&body);
        sub_ops.push(BssBatchSubOp::PutDataBlob {
            blob_guid,
            block_number: i as u32,
            body: body.clone(),
            body_checksum: chk,
            version: 1,
        });
        expected.push((i as u32, body));
    }
    let commit = rpc_client_bss::BlobCommitInfo {
        blob_guid,
        blob_version: 1,
        total_size: (N as u64) * 4096,
        block_count: N as u32,
    };
    let results = rpc_client
        .put_bss_batch(sub_ops, commit, None, &TraceId::new(), 0)
        .await
        .expect("burst batch send");
    assert_eq!(results.len(), N);
    for (i, r) in results.iter().enumerate() {
        assert!(r.status.is_ok(), "burst sub[{i}] failed: {:?}", r.status);
    }

    // Spot-check: read back two of the puts to verify data really
    // landed (not just acked with errno=0).
    for &idx in &[0usize, N - 1] {
        let (block_num, body) = &expected[idx];
        let mut readback = Bytes::new();
        rpc_client
            .get_data_blob(
                blob_guid,
                *block_num,
                &mut readback,
                body.len(),
                None,
                &TraceId::new(),
                0,
            )
            .await
            .expect("readback burst put");
        assert_eq!(&readback, body, "burst sub[{idx}] readback mismatch");
    }
}

#[tokio::test]
#[traced_test]
async fn test_basic_blob_io_with_random_bytes() {
    let url = "127.0.0.1:9225";
    tracing::debug!(%url);

    if !is_server_reachable(url).await {
        tracing::info!("Blob storage server not reachable at {url}, skipping test");
        return;
    }

    let rpc_client = RpcClientBss::new_from_address(url.to_string(), Duration::from_secs(5));

    for _ in 0..1 {
        let blob_guid = DataBlobGuid {
            blob_id: Uuid::now_v7(),
            volume_id: 1,
        };
        let content = Bytes::from((4096..1024 * 1024 - 256).fake::<String>());
        let body_checksum = xxhash_rust::xxh3::xxh3_64(&content);
        let mut readback_content = Bytes::new();
        rpc_client
            .put_data_blob(
                blob_guid,
                0,
                content.clone(),
                body_checksum,
                1,
                None,
                &TraceId::new(),
                0,
            )
            .await
            .unwrap();

        rpc_client
            .get_data_blob(
                blob_guid,
                0,
                &mut readback_content,
                content.len(),
                None,
                &TraceId::new(),
                0,
            )
            .await
            .unwrap();
        assert_eq!(content, readback_content);
    }
}
