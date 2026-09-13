//! `/v1/drives` end to end against a running local cluster.

use aws_sdk_s3::primitives::ByteStream;
use data_types::TraceId;
use reqwest::{Method, StatusCode};
use rpc_client_common::{RpcError, rss_rpc_retry};
use rpc_client_rss::RpcClientRss;
use serde_json::json;
use std::time::Duration;
use test_common::mgmt::MgmtClient;
use test_common::*;

const RSS_ADDR: &str = "127.0.0.1:8086";

/// Another worker may still hold the bucket record from before a
/// delete-and-recreate of the same name; its first request after that
/// answers NoSuchBucket and drops the entry, so one retry covers the
/// window.
async fn put_object(ctx: &Context, bucket: &str, key: &str) {
    let mut last = None;
    for attempt in 0..5 {
        let res = ctx
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await;
        match res {
            Ok(_) => return,
            Err(e) => {
                eprintln!("put {bucket}/{key} attempt {attempt}: {e}");
                last = Some(e);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    assert!(
        last.is_none(),
        "put object {bucket}/{key} failed after 5 attempts: {last:?}"
    );
}

/// A second API key with no bucket permissions, minted through /api_keys.
async fn other_key() -> MgmtClient {
    let resp = reqwest::Client::new()
        .post(format!("{}/api_keys/", MgmtClient::new().base))
        .json(&json!({ "name": "drives-test-other" }))
        .send()
        .await
        .expect("create api key");
    assert_eq!(resp.status(), StatusCode::OK, "create api key");
    let key: serde_json::Value = resp.json().await.expect("key json");
    MgmtClient::with_key(
        key["key_id"].as_str().expect("key_id"),
        key["secret_key"].as_str().expect("secret_key"),
    )
}

#[tokio::test]
async fn test_drive_crud() {
    let mgmt = MgmtClient::new();
    let name = "drv-crud";
    // Clean slate if a previous run died.
    let _ = mgmt.delete_drive(name, true).await;
    assert!(
        mgmt.wait_deleted(name, Duration::from_secs(30)).await,
        "clean slate"
    );

    let (status, body) = mgmt.create_drive(name, json!({ "run": "r1" })).await;
    assert_eq!(status, StatusCode::OK, "create: {body}");
    assert_eq!(body["name"], name, "name");
    assert_eq!(body["status"], "ready", "status");
    assert_eq!(body["labels"]["run"], "r1", "labels");
    let id = body["id"].as_str().expect("id").to_string();

    let (status, again) = mgmt.create_drive(name, json!({ "run": "r1" })).await;
    assert_eq!(status, StatusCode::OK, "idempotent create: {again}");
    assert_eq!(again["id"], id, "same drive");

    let (status, body) = mgmt.create_drive(name, json!({ "run": "r2" })).await;
    assert_eq!(status, StatusCode::CONFLICT, "different metadata: {body}");
    assert_eq!(body["code"], "already_exists", "code");

    let (status, body) = mgmt.get_drive(name).await;
    assert_eq!(status, StatusCode::OK, "get by name: {body}");
    let (status, body) = mgmt.get_drive(&id).await;
    assert_eq!(status, StatusCode::OK, "get by id: {body}");
    assert_eq!(body["name"], name, "id resolves to the name");

    let (status, body) = mgmt
        .request(Method::GET, "/v1/drives?limit=200", None)
        .await;
    assert_eq!(status, StatusCode::OK, "list: {body}");
    let names: Vec<&str> = body["data"]
        .as_array()
        .expect("data")
        .iter()
        .map(|d| d["name"].as_str().expect("name"))
        .collect();
    assert!(names.contains(&name), "listed: {names:?}");
    assert!(body["meta"]["hasMore"].is_boolean(), "meta");

    // The drive is the bucket: visible over S3 by the same name.
    let ctx = context();
    let buckets = ctx.list_buckets().await;
    assert!(
        buckets.buckets().iter().any(|b| b.name() == Some(name)),
        "bucket listed over S3"
    );

    let (status, body) = mgmt.delete_drive(name, false).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "delete empty: {body}");
    let (status, _) = mgmt.get_drive(name).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "gone");
    let (status, _) = mgmt.delete_drive(name, false).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "delete again");
}

#[tokio::test]
async fn test_drive_validation_and_auth() {
    let mgmt = MgmtClient::new();
    let (status, body) = mgmt.create_drive("AB", json!({})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "bad name: {body}");
    assert_eq!(body["code"], "invalid_name", "code");

    // Unsigned and badly signed requests are refused.
    let resp = reqwest::Client::new()
        .get(format!("{}/v1/drives", mgmt.base))
        .send()
        .await
        .expect("unsigned");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "unsigned");
    let bad = MgmtClient::with_key(TEST_KEY, "wrong-secret");
    let (status, body) = bad.request(Method::GET, "/v1/drives", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "bad secret: {body}");
    let unknown = MgmtClient::with_key("no-such-key", "x");
    let (status, _) = unknown.request(Method::GET, "/v1/drives", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "unknown key");

    // A signature over the bare path must not authorize the same request
    // with `force=true` appended: the query string is signed.
    let header = mgmt.fbsig1_header(&Method::DELETE, "/v1/drives/drv-crud", "", b"");
    let resp = reqwest::Client::new()
        .delete(format!("{}/v1/drives/drv-crud?force=true", mgmt.base))
        .header("authorization", header)
        .send()
        .await
        .expect("forced delete with an unforced signature");
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "query not covered");
}

/// A drive record names its bucket incarnation: once the bucket is deleted
/// and recreated under the same name over S3, the old record is stale and
/// must not attach its id to the new bucket.
#[tokio::test]
async fn test_drive_stale_record_after_recreate() {
    let mgmt = MgmtClient::new();
    let ctx = context();
    let name = "drv-stale";
    let _ = mgmt.delete_drive(name, true).await;
    assert!(
        mgmt.wait_deleted(name, Duration::from_secs(30)).await,
        "clean slate"
    );
    let (status, body) = mgmt.create_drive(name, json!({ "gen": "1" })).await;
    assert_eq!(status, StatusCode::OK, "create: {body}");
    let old_id = body["id"].as_str().expect("id").to_string();

    // Through the S3 door, without the control plane noticing.
    ctx.delete_bucket(name).await;
    ctx.create_bucket(name).await;

    let (status, body) = mgmt
        .request(Method::GET, "/v1/drives?limit=200", None)
        .await;
    assert_eq!(status, StatusCode::OK, "list: {body}");
    let listed: Vec<&str> = body["data"]
        .as_array()
        .expect("data")
        .iter()
        .map(|d| d["name"].as_str().expect("name"))
        .collect();
    assert!(
        !listed.contains(&name),
        "stale record is not listed: {listed:?}"
    );
    let (status, _) = mgmt.get_drive(name).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "old record is stale, not served"
    );
    let (status, _) = mgmt.get_drive(&old_id).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "old id never resolves to the new bucket"
    );
    let (status, body) = mgmt.create_drive(name, json!({ "gen": "2" })).await;
    assert_eq!(status, StatusCode::OK, "adopt the new incarnation: {body}");
    assert_ne!(body["id"], old_id, "a new drive, not the old id");
    assert_eq!(
        body["labels"]["gen"], "2",
        "new metadata, no conflict with the old"
    );
    let (status, _) = mgmt.delete_drive(name, false).await;
    assert_eq!(status, StatusCode::NO_CONTENT, "delete");
}

/// A revoked key cannot start a force sweep from a gateway that still
/// caches it: destructive requests re-read the key from RSS.
#[tokio::test]
async fn test_drive_force_delete_with_revoked_key() {
    let owner = other_key().await;
    let name = format!(
        "drv-revoked-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis()
            % 1_000_000
    );
    let (status, body) = owner.create_drive(&name, json!({})).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "create with the key to be revoked: {body}"
    );
    // Warm the gateway's key cache, then revoke through /api_keys.
    let (status, _) = owner.get_drive(&name).await;
    assert_eq!(status, StatusCode::OK, "cached");
    let resp = reqwest::Client::new()
        .delete(format!("{}/api_keys/{}", owner.base, owner.key_id))
        .send()
        .await
        .expect("delete api key");
    assert_eq!(resp.status(), StatusCode::NO_CONTENT, "revoke");
    let (status, body) = owner.delete_drive(&name, true).await;
    assert!(
        status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN,
        "force with a revoked key: {status} {body}"
    );
}

#[tokio::test]
async fn test_drive_adoption_and_ownership() {
    let mgmt = MgmtClient::new();
    let ctx = context();
    let name = "drv-adopt";
    let _ = mgmt.delete_drive(name, true).await;
    assert!(
        mgmt.wait_deleted(name, Duration::from_secs(30)).await,
        "clean slate"
    );

    // A bucket made over S3 is not a drive until the owner adopts it.
    ctx.create_bucket(name).await;
    let (status, _) = mgmt.get_drive(name).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "not a drive yet");
    let other = other_key().await;
    let (status, body) = other.create_drive(name, json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT, "adopt by non-owner: {body}");
    let (status, body) = mgmt.create_drive(name, json!({})).await;
    assert_eq!(status, StatusCode::OK, "adopt by owner: {body}");

    // The non-owner cannot see or delete it.
    let (status, _) = other.get_drive(name).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unreadable is not found");
    let (status, body) = other.request(Method::GET, "/v1/drives", None).await;
    assert_eq!(status, StatusCode::OK, "list as other: {body}");
    assert!(
        body["data"].as_array().expect("data").is_empty(),
        "other lists nothing"
    );
    let (status, body) = other.delete_drive(name, false).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "delete by non-owner: {body}");

    // With an object present, plain delete refuses and force empties.
    put_object(&ctx, name, "a/b").await;
    let (status, body) = mgmt.delete_drive(name, false).await;
    assert_eq!(status, StatusCode::CONFLICT, "not empty: {body}");
    assert_eq!(body["code"], "not_empty", "code");
    let (status, body) = mgmt.delete_drive(name, true).await;
    assert_eq!(status, StatusCode::ACCEPTED, "force: {body}");
    assert_eq!(body["status"], "deleting", "status");
    assert!(
        mgmt.wait_deleted(name, Duration::from_secs(60)).await,
        "force delete finished"
    );
    let head = ctx.client.head_bucket().bucket(name).send().await;
    assert!(head.is_err(), "bucket gone over S3");
}

#[tokio::test]
async fn test_drive_force_delete_pages() {
    let mgmt = MgmtClient::new();
    let ctx = context();
    let name = "drv-force";
    let _ = mgmt.delete_drive(name, true).await;
    assert!(
        mgmt.wait_deleted(name, Duration::from_secs(60)).await,
        "clean slate"
    );
    let (status, body) = mgmt.create_drive(name, json!({})).await;
    assert_eq!(status, StatusCode::OK, "create: {body}");

    // Three list pages: two full and one partial.
    let total = 2500;
    for chunk in (0..total).collect::<Vec<u32>>().chunks(100) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("obj/{i:05}")).collect();
        futures::future::join_all(keys.iter().map(|k| put_object(&ctx, name, k))).await;
    }

    let (a, b) = tokio::join!(mgmt.delete_drive(name, true), mgmt.delete_drive(name, true));
    assert_eq!(a.0, StatusCode::ACCEPTED, "force: {}", a.1);
    assert_eq!(b.0, StatusCode::ACCEPTED, "concurrent force: {}", b.1);
    // The status flip is a compare-and-set on the stored record: the record
    // must never still read `ready` once a force delete was accepted.
    let (status, body) = mgmt.get_drive(name).await;
    assert!(
        status == StatusCode::NOT_FOUND || body["status"] == "deleting",
        "record after force: {status} {body}"
    );
    let (status, body) = mgmt.create_drive(name, json!({})).await;
    assert!(
        status == StatusCode::CONFLICT || status == StatusCode::OK,
        "create while deleting or after: {status} {body}"
    );
    assert!(
        mgmt.wait_deleted(name, Duration::from_secs(120)).await,
        "force delete finished"
    );
    let head = ctx.client.head_bucket().bucket(name).send().await;
    assert!(head.is_err(), "bucket gone over S3");
}

/// Timing aid for the `delete_object_by_key` extraction, not a test. Uses
/// only S3 calls so it runs against a gateway without `/v1`:
/// `cargo test -p s3_gateway --test drives bench_delete -- --ignored --nocapture`.
#[tokio::test]
#[ignore]
async fn bench_delete_objects() {
    let ctx = context();
    let name = "bench-del";
    ctx.create_bucket(name).await;
    let total = 3000u32;
    for chunk in (0..total).collect::<Vec<u32>>().chunks(100) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("b/{i:05}")).collect();
        futures::future::join_all(keys.iter().map(|k| put_object(&ctx, name, k))).await;
    }
    let start = std::time::Instant::now();
    for i in 0..total {
        ctx.client
            .delete_object()
            .bucket(name)
            .key(format!("b/{i:05}"))
            .send()
            .await
            .expect("delete object");
    }
    let elapsed = start.elapsed();
    println!(
        "delete_object x{total}: {:?} total, {:?} avg",
        elapsed,
        elapsed / total
    );
    ctx.delete_bucket(name).await;
}

/// The RSS fences the drive code relies on: a version-conditioned record
/// delete that refuses a replaced record, and a bucket delete fenced on the
/// bucket incarnation. Talks to RSS directly, so it runs only where RSS is
/// addressable: the DDB and etcd phases of precheckin, not the Docker
/// phase, whose container maps only the S3 and management ports.
#[tokio::test]
async fn test_rss_fenced_deletes() {
    let reachable = tokio::time::timeout(
        Duration::from_secs(2),
        tokio::net::TcpStream::connect(RSS_ADDR),
    )
    .await;
    if !matches!(reachable, Ok(Ok(_))) {
        eprintln!("RSS at {RSS_ADDR} is not reachable from this test run; fence test skipped");
        return;
    }
    let rss = RpcClientRss::new_from_addresses(vec![RSS_ADDR.to_string()], Duration::from_secs(5));
    let t = TraceId::new();
    // Outside the drive: prefix, so a concurrent listDrives never sees it.
    let key = "fence-test:drives";
    let _ = rss_rpc_retry!(rss, delete(key, 0, None, &t)).await;

    // Read version N, replace the record (N+1), delete with N: refused,
    // and the replacement stays.
    let written = rss_rpc_retry!(rss, put(0, key, "v1", None, &t))
        .await
        .expect("create");
    let (n, v1) = rss_rpc_retry!(rss, get(key, None, &t))
        .await
        .expect("get v1");
    assert_eq!(v1, "v1", "first value");
    assert_eq!(written, n, "put returns the version get reports");
    let written = rss_rpc_retry!(rss, put(n, key, "v2", None, &t))
        .await
        .expect("replace");
    let (n2, v2) = rss_rpc_retry!(rss, get(key, None, &t))
        .await
        .expect("get v2");
    assert_eq!(v2, "v2", "replaced value");
    assert_eq!(written, n2, "replacement's version comes back from put");
    let stale = rss_rpc_retry!(rss, delete(key, n, None, &t)).await;
    assert!(
        matches!(stale, Err(RpcError::Retry)),
        "stale delete: {stale:?}"
    );
    let (_, still) = rss_rpc_retry!(rss, get(key, None, &t))
        .await
        .expect("still there");
    assert_eq!(still, "v2", "replacement untouched");
    rss_rpc_retry!(rss, delete(key, n2, None, &t))
        .await
        .expect("current delete");
    let gone = rss_rpc_retry!(rss, get(key, None, &t)).await;
    assert!(matches!(gone, Err(RpcError::NotFound)), "gone: {gone:?}");

    // A bucket delete fenced on the wrong root deletes nothing.
    let ctx = context();
    let name = "drv-fence";
    ctx.create_bucket(name).await;
    let (_, bucket) = rss_rpc_retry!(rss, get(&format!("bucket:{name}"), None, &t))
        .await
        .expect("bucket record");
    let bucket: serde_json::Value = serde_json::from_str(&bucket).expect("bucket json");
    let root = bucket["root_blob_name"].as_str().expect("root").to_string();
    let wrong = rss_rpc_retry!(
        rss,
        delete_bucket(name, TEST_KEY, "not-this-root", None, &t)
    )
    .await;
    assert!(
        matches!(wrong, Err(RpcError::IncarnationMismatch)),
        "wrong root: {wrong:?}"
    );
    ctx.client
        .head_bucket()
        .bucket(name)
        .send()
        .await
        .expect("bucket survives a mismatched fence");
    rss_rpc_retry!(rss, delete_bucket(name, TEST_KEY, &root, None, &t))
        .await
        .expect("delete with the right root");
    let head = ctx.client.head_bucket().bucket(name).send().await;
    assert!(head.is_err(), "bucket gone");
}
