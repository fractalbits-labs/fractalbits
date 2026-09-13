//! The management API's `FBSIG1` request signature, shared by the gateway
//! that verifies it and the clients that produce it.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::time::Duration;

/// Replay window for the `FBSIG1` timestamp, the same as the mount skew.
pub const MGMT_SKEW: Duration = Duration::from_secs(300);

/// HMAC-SHA256 over `key_id | timestamp_ms | nonce | method | path | query
/// | body_sha256_hex` under the API key's secret. Same key and separator
/// convention as the mount signature in `fs_gateway_codec`, so one client
/// routine signs both. `query` is the raw query string as sent, empty when
/// there is none; it is covered because `force=true` turns a delete
/// destructive, so it must not be attachable to a signed request.
#[allow(clippy::too_many_arguments)]
pub fn mgmt_signature(
    secret_key: &[u8],
    key_id: &str,
    timestamp_ms: u64,
    nonce: &[u8],
    method: &str,
    path: &str,
    query: &str,
    body_sha256_hex: &str,
) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key).expect("hmac accepts any key length");
    mac.update(key_id.as_bytes());
    mac.update(b"\n");
    mac.update(&timestamp_ms.to_le_bytes());
    mac.update(nonce);
    mac.update(b"\n");
    mac.update(method.as_bytes());
    mac.update(b"\n");
    mac.update(path.as_bytes());
    mac.update(b"\n");
    mac.update(query.as_bytes());
    mac.update(b"\n");
    mac.update(body_sha256_hex.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
