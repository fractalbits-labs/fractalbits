//! A signed client for the s3_gateway ARTFS control plane (`/v1`), shared
//! by the S3 API tests and the FUSE suite in xtask.

use data_types::mgmt_sig::mgmt_signature;
use reqwest::{Client, Method, StatusCode};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

/// The private mgmt port: health and the unauthenticated `/api_keys`.
pub const DEFAULT_MGMT_PORT: u16 = 18080;
/// The exposable control port: only the signed `/v1` routes.
pub const DEFAULT_FS_CONTROL_PORT: u16 = 8181;

pub fn mgmt_base() -> String {
    format!("http://127.0.0.1:{DEFAULT_MGMT_PORT}")
}

pub struct ControlClient {
    pub base: String,
    pub key_id: String,
    pub secret: String,
    http: Client,
}

impl ControlClient {
    pub fn new() -> Self {
        Self::with_key(crate::TEST_KEY, crate::TEST_SECRET)
    }

    pub fn with_key(key_id: &str, secret: &str) -> Self {
        Self {
            base: format!("http://127.0.0.1:{DEFAULT_FS_CONTROL_PORT}"),
            key_id: key_id.to_string(),
            secret: secret.to_string(),
            http: Client::new(),
        }
    }

    /// `query` is the raw query string, without the `?`, empty when absent.
    pub fn fbsig1_header(&self, method: &Method, path: &str, query: &str, body: &[u8]) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis() as u64;
        let nonce: [u8; 8] = rand_bytes();
        let body_sha = hex::encode(Sha256::digest(body));
        let sig = mgmt_signature(
            self.secret.as_bytes(),
            &self.key_id,
            ts,
            &nonce,
            method.as_str(),
            path,
            query,
            &body_sha,
        );
        format!(
            "FBSIG1 key_id={}, ts={ts}, nonce={}, sig={}",
            self.key_id,
            hex::encode(nonce),
            hex::encode(sig)
        )
    }

    /// The path and the query string are both signed.
    pub async fn request(
        &self,
        method: Method,
        path_and_query: &str,
        body: Option<&Value>,
    ) -> (StatusCode, Value) {
        let (path, query) = path_and_query
            .split_once('?')
            .unwrap_or((path_and_query, ""));
        let bytes = body
            .map(|b| serde_json::to_vec(b).expect("json"))
            .unwrap_or_default();
        let auth = self.fbsig1_header(&method, path, query, &bytes);
        let mut req = self
            .http
            .request(method, format!("{}{}", self.base, path_and_query))
            .header("authorization", auth);
        if body.is_some() {
            req = req.header("content-type", "application/json").body(bytes);
        }
        let resp = req.send().await.expect("control request");
        let status = resp.status();
        let text = resp.text().await.expect("body");
        let json = if text.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        };
        (status, json)
    }

    pub async fn create_drive(&self, name: &str, labels: Value) -> (StatusCode, Value) {
        let body = serde_json::json!({ "name": name, "labels": labels });
        self.request(Method::POST, "/v1/drives", Some(&body)).await
    }

    pub async fn get_drive(&self, name: &str) -> (StatusCode, Value) {
        self.request(Method::GET, &format!("/v1/drives/{name}"), None)
            .await
    }

    pub async fn delete_drive(&self, name: &str, force: bool) -> (StatusCode, Value) {
        let q = if force { "?force=true" } else { "" };
        self.request(Method::DELETE, &format!("/v1/drives/{name}{q}"), None)
            .await
    }

    /// Poll `get` until it answers 404, the end of a `force` delete.
    pub async fn wait_deleted(&self, name: &str, timeout: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if self.get_drive(name).await.0 == StatusCode::NOT_FOUND {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        false
    }
}

impl Default for ControlClient {
    fn default() -> Self {
        Self::new()
    }
}

fn rand_bytes<const N: usize>() -> [u8; N] {
    let mut out = [0u8; N];
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    for (i, b) in out.iter_mut().enumerate() {
        *b = (seed >> (8 * (i % 16))) as u8 ^ (i as u8).wrapping_mul(31);
    }
    out
}
