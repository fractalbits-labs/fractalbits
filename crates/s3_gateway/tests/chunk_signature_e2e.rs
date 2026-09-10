use aws_sdk_s3::types::ChecksumMode;
use aws_signature::{
    STREAMING_PAYLOAD,
    sigv4::{SigningParams, sign_request},
    streaming::{SignatureError, create_chunk_signature_context, create_streaming_body},
};
use chrono::Utc;
use reqwest::{Client, Method};
use std::collections::{BTreeMap, BTreeSet};
use test_common::build_client;

/// Simple S3 test client using reqwest and the aws_signature common crate
pub struct S3TestClient {
    client: Client,
    params: SigningParams,
    endpoint: String,
}

impl S3TestClient {
    pub fn new(
        access_key_id: String,
        secret_access_key: String,
        region: String,
        endpoint: String,
    ) -> Self {
        Self {
            client: Client::new(),
            params: SigningParams {
                access_key_id,
                secret_access_key,
                region,
                service: "s3".to_string(),
                datetime: Utc::now(),
            },
            endpoint,
        }
    }

    pub async fn put_object_chunked(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        chunk_size: usize,
        headers: Option<BTreeMap<String, String>>,
    ) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/{}/{}", self.endpoint, bucket, key);
        let datetime = Utc::now();

        // Update signing params with current time
        let mut params = self.params.clone();
        params.datetime = datetime;

        // Create chunk signature context
        let chunk_context =
            create_chunk_signature_context(datetime, &params.secret_access_key, &params.region)?;

        // Create headers
        let mut request_headers = BTreeMap::new();
        request_headers.insert("host".to_string(), self.extract_host(&url)?);
        request_headers.insert(
            "x-amz-date".to_string(),
            datetime.format("%Y%m%dT%H%M%SZ").to_string(),
        );
        request_headers.insert(
            "x-amz-content-sha256".to_string(),
            STREAMING_PAYLOAD.to_string(),
        );
        request_headers.insert("content-encoding".to_string(), "aws-chunked".to_string());
        request_headers.insert(
            "x-amz-decoded-content-length".to_string(),
            body.len().to_string(),
        );

        // Add custom headers
        if let Some(custom_headers) = headers {
            for (key, value) in custom_headers {
                request_headers.insert(key, value);
            }
        }

        // Calculate seed signature (signature of the initial request)
        let signed_headers: BTreeSet<String> = request_headers.keys().cloned().collect();
        let uri_path = format!("/{}/{}", bucket, key);
        let query_params = BTreeMap::new();

        // Build canonical headers from request headers
        let mut canonical_headers = Vec::new();
        for header_name in &signed_headers {
            if let Some(header_value) = request_headers.get(header_name) {
                canonical_headers.push(format!("{}:{}", header_name, header_value.trim()));
            }
        }

        let authorization = sign_request(
            &params,
            "PUT",
            &uri_path,
            &query_params,
            &canonical_headers,
            &signed_headers,
            STREAMING_PAYLOAD,
        )
        .map_err(|e| format!("Failed to sign request: {}", e))?;

        // Extract seed signature from authorization header
        let seed_signature = self.extract_seed_signature(&authorization)?;

        // Create streaming body with chunk signatures
        let streaming_body =
            create_streaming_body(&body, chunk_size, &chunk_context, &seed_signature)?;

        // Add content length for the streaming body (including chunk headers)
        let final_headers = {
            let mut headers = request_headers.clone();
            headers.insert(
                "content-length".to_string(),
                streaming_body.len().to_string(),
            );
            headers.insert("authorization".to_string(), authorization);
            headers
        };

        // Convert to reqwest headers
        let mut reqwest_headers = reqwest::header::HeaderMap::new();
        for (name, value) in final_headers {
            reqwest_headers.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes())?,
                reqwest::header::HeaderValue::from_str(&value)?,
            );
        }

        // Send the request
        let response = self
            .client
            .request(Method::PUT, &url)
            .headers(reqwest_headers)
            .body(streaming_body)
            .send()
            .await?;

        Ok(response)
    }

    pub async fn put_object_chunked_invalid_signature(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        chunk_size: usize,
    ) -> Result<reqwest::Response, Box<dyn std::error::Error + Send + Sync>> {
        // Same as put_object_chunked but with corrupted chunk signatures
        let url = format!("{}/{}/{}", self.endpoint, bucket, key);
        let datetime = Utc::now();

        let mut params = self.params.clone();
        params.datetime = datetime;

        let mut request_headers = BTreeMap::new();
        request_headers.insert("host".to_string(), self.extract_host(&url)?);
        request_headers.insert(
            "x-amz-date".to_string(),
            datetime.format("%Y%m%dT%H%M%SZ").to_string(),
        );
        request_headers.insert(
            "x-amz-content-sha256".to_string(),
            STREAMING_PAYLOAD.to_string(),
        );
        request_headers.insert("content-encoding".to_string(), "aws-chunked".to_string());
        request_headers.insert(
            "x-amz-decoded-content-length".to_string(),
            body.len().to_string(),
        );

        let signed_headers: BTreeSet<String> = request_headers.keys().cloned().collect();
        let uri_path = format!("/{}/{}", bucket, key);
        let query_params = BTreeMap::new();

        // Build canonical headers from request headers
        let mut canonical_headers = Vec::new();
        for header_name in &signed_headers {
            if let Some(header_value) = request_headers.get(header_name) {
                canonical_headers.push(format!("{}:{}", header_name, header_value.trim()));
            }
        }

        let authorization = sign_request(
            &params,
            "PUT",
            &uri_path,
            &query_params,
            &canonical_headers,
            &signed_headers,
            STREAMING_PAYLOAD,
        )
        .map_err(|e| format!("Failed to sign request: {}", e))?;

        // Create streaming body with INVALID chunk signatures
        let streaming_body = self.create_invalid_streaming_body(&body, chunk_size)?;

        let final_headers = {
            let mut headers = request_headers.clone();
            headers.insert(
                "content-length".to_string(),
                streaming_body.len().to_string(),
            );
            headers.insert("authorization".to_string(), authorization);
            headers
        };

        let mut reqwest_headers = reqwest::header::HeaderMap::new();
        for (name, value) in final_headers {
            reqwest_headers.insert(
                reqwest::header::HeaderName::from_bytes(name.as_bytes())?,
                reqwest::header::HeaderValue::from_str(&value)?,
            );
        }

        let response = self
            .client
            .request(Method::PUT, &url)
            .headers(reqwest_headers)
            .body(streaming_body)
            .send()
            .await?;

        Ok(response)
    }

    fn extract_host(&self, url: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let parsed_url = url::Url::parse(url)?;
        let host = parsed_url.host_str().unwrap_or("localhost");
        if let Some(port) = parsed_url.port() {
            Ok(format!("{}:{}", host, port))
        } else {
            Ok(host.to_string())
        }
    }

    fn extract_seed_signature(
        &self,
        authorization: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        // Extract the signature from "AWS4-HMAC-SHA256 ... Signature=<sig>"
        if let Some(signature_part) = authorization.split("Signature=").nth(1) {
            Ok(signature_part.to_string())
        } else {
            Err("Failed to extract signature from authorization header".into())
        }
    }

    fn create_invalid_streaming_body(
        &self,
        body: &[u8],
        chunk_size: usize,
    ) -> Result<Vec<u8>, SignatureError> {
        let mut result = Vec::new();

        // Create chunks with invalid signatures
        for chunk in body.chunks(chunk_size) {
            let invalid_signature = "invalidchunksignature1234567890abcdef1234567890abcdef12345678";
            let chunk_header = format!(
                "{:x};chunk-signature={}\r\n",
                chunk.len(),
                invalid_signature
            );
            result.extend_from_slice(chunk_header.as_bytes());
            result.extend_from_slice(chunk);
            result.extend_from_slice(b"\r\n");
        }

        // Add final zero-length chunk with invalid signature
        let final_invalid_sig = "invalidfinalsignature1234567890abcdef1234567890abcdef1234567890";
        let final_header = format!("0;chunk-signature={}\r\n\r\n", final_invalid_sig);
        result.extend_from_slice(final_header.as_bytes());

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_ACCESS_KEY: &str = "test_api_key";
    const TEST_SECRET_KEY: &str = "test_api_secret";
    const TEST_REGION: &str = "localdev";
    const TEST_ENDPOINT: &str = "http://localhost:8080";

    fn create_test_client() -> S3TestClient {
        S3TestClient::new(
            TEST_ACCESS_KEY.to_string(),
            TEST_SECRET_KEY.to_string(),
            TEST_REGION.to_string(),
            TEST_ENDPOINT.to_string(),
        )
    }

    #[tokio::test]
    async fn test_client_creation() {
        let client = create_test_client();
        assert_eq!(client.params.access_key_id, TEST_ACCESS_KEY);
        assert_eq!(client.params.secret_access_key, TEST_SECRET_KEY);
        assert_eq!(client.params.region, TEST_REGION);
        assert_eq!(client.endpoint, TEST_ENDPOINT);
    }

    #[tokio::test]
    async fn test_extract_host() {
        let client = create_test_client();
        let host = client
            .extract_host("http://localhost:8080/bucket/key")
            .unwrap();
        assert_eq!(host, "localhost:8080");
    }

    #[tokio::test]
    async fn test_extract_seed_signature() {
        let client = create_test_client();
        let auth = "AWS4-HMAC-SHA256 Credential=test/scope, SignedHeaders=host, Signature=abcd1234";
        let sig = client.extract_seed_signature(auth).unwrap();
        assert_eq!(sig, "abcd1234");
    }

    // Integration test - runs with server
    #[tokio::test]
    async fn test_put_object_chunked_valid() {
        // Create bucket using standard test context first
        let ctx = test_common::context();
        let bucket_name = ctx.create_bucket("chunk-test-bucket").await;

        let client = create_test_client();
        let original_body = b"Hello, chunked world!".to_vec();
        let key_name = "test-key";

        // Upload using custom chunked client
        let response = client
            .put_object_chunked(&bucket_name, key_name, original_body.clone(), 8, None)
            .await;

        let response = response.expect("Chunked upload request should not fail");
        let status = response.status();

        if !status.is_success() {
            let body_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Could not read response body".to_string());
            panic!(
                "Chunked upload should succeed with valid signatures, got status: {}, body: {}",
                status, body_text
            );
        }

        // Verify upload by downloading with AWS SDK client
        let aws_client = build_client();
        let get_response = aws_client
            .get_object()
            .bucket(&bucket_name)
            .key(key_name)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .expect("Failed to download uploaded object for verification");

        // Extract checksums before consuming the body
        let sha256 = get_response.checksum_sha256().map(|s| s.to_string());
        let sha1 = get_response.checksum_sha1().map(|s| s.to_string());

        // Verify content matches
        let downloaded_body = get_response
            .body
            .collect()
            .await
            .expect("Failed to collect downloaded body")
            .into_bytes();

        assert_eq!(
            downloaded_body.as_ref(),
            original_body.as_slice(),
            "Downloaded content does not match uploaded content"
        );

        // Assert expected checksum values for "Hello, chunked world!" (21 bytes)
        if let Some(sha256) = sha256 {
            assert_eq!(
                sha256, "hnTYyCIGBcfMtsp4C+9kYj0W9w+m+KfFzTieYy1ug4I=",
                "SHA256 checksum mismatch"
            );
        }

        if let Some(sha1) = sha1 {
            assert_eq!(
                sha1, "HxLv1JzcrwUtpKN3Kiegu4Ut7RU=",
                "SHA1 checksum mismatch"
            );
        }

        // Cleanup: delete the object first, then the bucket
        aws_client
            .delete_object()
            .bucket(&bucket_name)
            .key(key_name)
            .send()
            .await
            .expect("Failed to delete test object");

        ctx.delete_bucket(&bucket_name).await;
    }

    // Integration test - runs with server
    #[tokio::test]
    async fn test_put_object_chunked_invalid() {
        // Create bucket using standard test context first
        let ctx = test_common::context();
        let bucket_name = ctx.create_bucket("chunk-test-bucket-invalid").await;

        let client = create_test_client();
        let body = b"Hello, chunked world!".to_vec();
        let key_name = "test-key-invalid";

        let response = client
            .put_object_chunked_invalid_signature(&bucket_name, key_name, body, 8)
            .await;

        let response = response.expect("Invalid chunk signature request should not fail to send");
        let status = response.status();
        assert!(
            status.is_server_error(),
            "Invalid chunk signature upload should be rejected with server error, got status: {}",
            status
        );

        // Verify that no object was created by attempting download
        let aws_client = build_client();
        aws_client
            .get_object()
            .bucket(&bucket_name)
            .key(key_name)
            .send()
            .await
            .expect_err("Object should not exist after invalid chunk signature upload");

        // Cleanup: try to delete the object if it exists (it shouldn't, but just in case)
        let _ = aws_client
            .delete_object()
            .bucket(&bucket_name)
            .key(key_name)
            .send()
            .await; // Ignore error if object doesn't exist

        ctx.delete_bucket(&bucket_name).await;
    }
}
