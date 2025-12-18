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
