// convert from aws's s3 rust sdk tests

use aws_sdk_s3::{
    operation::get_object::GetObjectOutput, primitives::ByteStream, types::ChecksumAlgorithm,
    types::ChecksumMode,
};
use ntex::http::header::HeaderValue;
use rstest::rstest;
use test_common::{Context, assert_bytes_eq, context};

// The test structure is identical for all supported checksum algorithms
#[allow(clippy::too_many_arguments)]
async fn test_checksum(
    ctx: &Context,
    bucket: &str,
    key: &str,
    value: &'static [u8],
    expected_decoded_content_length: usize,
    expected_encoded_content_length: usize,
    checksum_algorithm: ChecksumAlgorithm,
    streaming: bool,
) -> GetObjectOutput {
    // ByteStreams created from a file are streaming and have a known size
    let mut file = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    file.write_all(value).unwrap();

    let body = if streaming {
        ByteStream::from_path(file.path()).await.unwrap()
    } else {
        ByteStream::from_static(value)
    };

    ctx.client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .checksum_algorithm(checksum_algorithm)
        .customize()
        // we actually only do `inspect_request` here
        .mutate_request(move |request| {
            if streaming {
                let x_amz_decoded_content_length = request
                    .headers()
                    .get("x-amz-decoded-content-length")
                    .expect("x-amz-decoded-content-length header exists");
                // The length of the string "Hello world"
                assert_eq!(
                    HeaderValue::from_str(&expected_decoded_content_length.to_string()).unwrap(),
                    x_amz_decoded_content_length,
                    "decoded content length was wrong"
                );
            }
            let content_length = request
                .headers()
                .get("Content-Length")
                .expect("Content-Length header exists");

            // The sum of the length of the original body, chunk markers, and trailers
            assert_eq!(
                HeaderValue::from_str(&expected_encoded_content_length.to_string()).unwrap(),
                content_length,
                "content-length was expected to be {expected_encoded_content_length} but was {content_length} instead"
            );
        })
        .send()
        .await
        .unwrap_or_else(|_| panic!("put_object failed: (bucket={bucket}, key={key})"));

    ctx.client
        .get_object()
        .bucket(bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap()
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn test_crc32_checksum(#[case] streaming: bool) {
    let (ctx, bucket, key) = setup(&format!("crc32-checksum-on-streaming-{streaming}")).await;

    let expected_encoded_content_length = if streaming {
        b"B\r\nHello world\r\n0\r\nx-amz-checksum-crc32:i9aeUg==\r\n\r\n".len()
    } else {
        b"Hello world".len()
    };
    let res = test_checksum(
        &ctx,
        &bucket,
        &key,
        b"Hello world",
        11,
        expected_encoded_content_length,
        ChecksumAlgorithm::Crc32,
        streaming,
    )
    .await;
    // Header checksums are base64 encoded
    assert_eq!(res.checksum_crc32(), Some("i9aeUg=="));
    assert_bytes_eq!(res.body, b"Hello world");

    cleanup(&ctx, &bucket, &key).await;
}

// This test isn't a duplicate. It tests CRC32C (note the C) checksum request validation
#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn test_crc32c_checksum(#[case] streaming: bool) {
    let (ctx, bucket, key) = setup(&format!("crc32c-checksum-on-streaming-{streaming}")).await;

    let expected_encoded_content_length = if streaming {
        b"B\r\nHello world\r\n0\r\nx-amz-checksum-crc32c:crUfeA==\r\n\r\n".len()
    } else {
        b"Hello world".len()
    };
    let res = test_checksum(
        &ctx,
        &bucket,
        &key,
        b"Hello world",
        11,
        expected_encoded_content_length,
        ChecksumAlgorithm::Crc32C,
        streaming,
    )
    .await;
    // Header checksums are base64 encoded
    assert_eq!(res.checksum_crc32_c(), Some("crUfeA=="));
    assert_bytes_eq!(res.body, b"Hello world");

    cleanup(&ctx, &bucket, &key).await;
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn test_sha1_checksum(#[case] streaming: bool) {
    let (ctx, bucket, key) = setup(&format!("sha1-checksum-on-streaming-{streaming}")).await;

    let expected_encoded_content_length = if streaming {
        b"B\r\nHello world\r\n0\r\nx-amz-checksum-sha1:e1AsOh9IyGCa4hLN+2Od7jlnP14=\r\n\r\n".len()
    } else {
        b"Hello world".len()
    };
    let res = test_checksum(
        &ctx,
        &bucket,
        &key,
        b"Hello world",
        11,
        expected_encoded_content_length,
        ChecksumAlgorithm::Sha1,
        streaming,
    )
    .await;
    // Header checksums are base64 encoded
    assert_eq!(res.checksum_sha1(), Some("e1AsOh9IyGCa4hLN+2Od7jlnP14="));
    assert_bytes_eq!(res.body, b"Hello world");

    cleanup(&ctx, &bucket, &key).await;
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn test_sha256_checksum(#[case] streaming: bool) {
    let (ctx, bucket, key) = setup(&format!("sha256-checksum-on-streaming-{streaming}")).await;

    let expected_encoded_content_length = if streaming {
        b"B\r\nHello world\r\n0\r\nx-amz-checksum-sha256:ZOyIygCyaOW6GjVnihtTFtIS9PNmskdyMlNKiuyjfzw=\r\n\r\n".len()
    } else {
        b"Hello world".len()
    };
    let res = test_checksum(
        &ctx,
        &bucket,
        &key,
        b"Hello world",
        11,
        expected_encoded_content_length,
        ChecksumAlgorithm::Sha256,
        streaming,
    )
    .await;
    // Header checksums are base64 encoded
    assert_eq!(
        res.checksum_sha256(),
        Some("ZOyIygCyaOW6GjVnihtTFtIS9PNmskdyMlNKiuyjfzw=")
    );
    assert_bytes_eq!(res.body, b"Hello world");

    cleanup(&ctx, &bucket, &key).await;
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test]
async fn test_crc64nvme_checksum(#[case] streaming: bool) {
    let (ctx, bucket, key) = setup(&format!("crc64nvme-checksum-on-streaming-{streaming}")).await;

    let expected_encoded_content_length = if streaming {
        b"B\r\nHello world\r\n0\r\nx-amz-checksum-crc64nvme:OOJZ0D8xKts=\r\n\r\n".len()
    } else {
        b"Hello world".len()
    };
    let res = test_checksum(
        &ctx,
        &bucket,
        &key,
        b"Hello world",
        11,
        expected_encoded_content_length,
        ChecksumAlgorithm::Crc64Nvme,
        streaming,
    )
    .await;
    // Header checksums are base64 encoded
    assert_eq!(res.checksum_crc64_nvme(), Some("OOJZ0D8xKts="));
    assert_bytes_eq!(res.body, b"Hello world");

    cleanup(&ctx, &bucket, &key).await;
}

async fn setup(bucket: &str) -> (Context, String, String) {
    let ctx = context();
    let bucket = ctx.create_bucket(bucket).await;
    let key = "test.txt".to_string();
    (ctx, bucket, key)
}

async fn cleanup(ctx: &Context, bucket: &str, key: &str) {
    ctx.client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    ctx.delete_bucket(bucket).await;
}
