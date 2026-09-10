use std::time::Duration;

use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use test_common::*;

#[tokio::test]
async fn test_presigned_get() {
    let ctx = context();
    let bucket = ctx.create_bucket("presigned-get-bucket").await;

    let key = "presigned-test-file.txt";
    let value = b"hello from presigned URL";
    let data = ByteStream::from_static(value);

    // PUT normally
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(data)
        .send()
        .await
        .expect("put_object failed");

    // Generate pre-signed GET URL
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(3600))
        .build()
        .expect("presigning config build failed");

    let presigned_request = ctx
        .client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .expect("presigning failed");

    // GET via pre-signed URL with reqwest (no credentials)
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("reqwest client build failed");

    let response = http_client
        .get(presigned_request.uri())
        .send()
        .await
        .expect("presigned GET failed");

    assert_eq!(
        response.status(),
        200,
        "presigned GET returned {}",
        response.status()
    );

    let body = response.bytes().await.expect("reading body failed");
    assert_eq!(body.as_ref(), value);

    // Cleanup
    ctx.client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("delete_object failed");

    ctx.delete_bucket(&bucket).await;
}

#[tokio::test]
async fn test_presigned_put() {
    let ctx = context();
    let bucket = ctx.create_bucket("presigned-put-bucket").await;

    let key = "presigned-put-file.txt";
    let value = b"uploaded via presigned PUT";

    // Generate pre-signed PUT URL
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(3600))
        .build()
        .expect("presigning config build failed");

    let presigned_request = ctx
        .client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .expect("presigning failed");

    // PUT via pre-signed URL with reqwest (no credentials)
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("reqwest client build failed");

    let response = http_client
        .put(presigned_request.uri())
        .body(value.to_vec())
        .send()
        .await
        .expect("presigned PUT failed");

    assert_eq!(
        response.status(),
        200,
        "presigned PUT returned {}",
        response.status()
    );

    // GET normally to verify the upload
    let get_res = ctx
        .client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("get_object failed");

    assert_bytes_eq!(get_res.body, value);

    // Cleanup
    ctx.client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("delete_object failed");

    ctx.delete_bucket(&bucket).await;
}

#[tokio::test]
async fn test_presigned_head() {
    let ctx = context();
    let bucket = ctx.create_bucket("presigned-head-bucket").await;

    let key = "presigned-head-file.txt";
    let value = b"head test content";
    let data = ByteStream::from_static(value);

    // PUT normally
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(data)
        .send()
        .await
        .expect("put_object failed");

    // Generate pre-signed HEAD URL
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(3600))
        .build()
        .expect("presigning config build failed");

    let presigned_request = ctx
        .client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .expect("presigning failed");

    // HEAD via pre-signed URL
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("reqwest client build failed");

    let response = http_client
        .head(presigned_request.uri())
        .send()
        .await
        .expect("presigned HEAD failed");

    assert_eq!(
        response.status(),
        200,
        "presigned HEAD returned {}",
        response.status()
    );

    assert_eq!(
        response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok()),
        Some(value.len()),
    );

    // Cleanup
    ctx.client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("delete_object failed");

    ctx.delete_bucket(&bucket).await;
}

#[tokio::test]
async fn test_presigned_delete() {
    let ctx = context();
    let bucket = ctx.create_bucket("presigned-delete-bucket").await;

    let key = "presigned-delete-file.txt";
    let value = b"delete me";
    let data = ByteStream::from_static(value);

    // PUT normally
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(data)
        .send()
        .await
        .expect("put_object failed");

    // Generate pre-signed DELETE URL
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(3600))
        .build()
        .expect("presigning config build failed");

    let presigned_request = ctx
        .client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .expect("presigning failed");

    // DELETE via pre-signed URL
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("reqwest client build failed");

    let response = http_client
        .delete(presigned_request.uri())
        .send()
        .await
        .expect("presigned DELETE failed");

    assert!(
        response.status().is_success(),
        "presigned DELETE returned {}",
        response.status()
    );

    // Verify object is deleted
    let get_res = ctx
        .client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await;

    assert!(get_res.is_err(), "object should be deleted");

    ctx.delete_bucket(&bucket).await;
}

#[tokio::test]
async fn test_presigned_expired_url() {
    let ctx = context();
    let bucket = ctx.create_bucket("presigned-expired-bucket").await;

    let key = "presigned-expired-file.txt";
    let value = b"expires soon";
    let data = ByteStream::from_static(value);

    // PUT normally
    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(data)
        .send()
        .await
        .expect("put_object failed");

    // Generate pre-signed GET URL with 1 second expiry
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(1))
        .build()
        .expect("presigning config build failed");

    let presigned_request = ctx
        .client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .expect("presigning failed");

    // Wait for expiration
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Try GET with expired URL
    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("reqwest client build failed");

    let response = http_client
        .get(presigned_request.uri())
        .send()
        .await
        .expect("request failed");

    // Should be 403 Forbidden (AccessDenied)
    assert_eq!(
        response.status(),
        403,
        "expired presigned URL should return 403, got {}",
        response.status()
    );

    // Cleanup
    ctx.client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("delete_object failed");

    ctx.delete_bucket(&bucket).await;
}
