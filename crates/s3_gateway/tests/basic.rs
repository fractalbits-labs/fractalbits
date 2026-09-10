use aws_sdk_s3::primitives::ByteStream;
use test_common::*;

#[tokio::test]
async fn test_basic_object_apis() {
    let ctx = context();
    let bucket = ctx.create_bucket("my-bucket1").await;

    let key = "hello";
    let value = b"42";
    let data = ByteStream::from_static(value);

    ctx.client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(data)
        .send()
        .await
        .unwrap();

    let res = ctx
        .client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();

    assert_bytes_eq!(res.body, value);

    ctx.client
        .delete_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();

    ctx.delete_bucket(&bucket).await;
}

#[tokio::test]
async fn test_basic_bucket_apis() {
    let ctx = context();
    let bucket = ctx.create_bucket("my-bucket2").await;

    let buckets = ctx.list_buckets().await.buckets.unwrap();
    // Note we may have concurrent tests running, so just do basic testing here
    assert!(!buckets.is_empty());
    ctx.delete_bucket(&bucket).await;
}
