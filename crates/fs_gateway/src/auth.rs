//! Mount authentication and session tokens.
//!
//! `Mount` is verified against the API key stored in RSS (the same key
//! the S3 gateway uses). Every later request carries a session token:
//! an HMAC over `(bucket, read_write, expiry)` under a per-process
//! secret, so verification is a hash and never a lookup. A token from
//! another gateway process fails verification and the client re-mounts.

use bytes::{BufMut, Bytes, BytesMut};
use data_types::{ApiKey, TraceId};
use fs_gateway_codec::MountRequest;
use hmac::{Hmac, Mac};
use rpc_client_rss::RpcClientRss;
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::config::Config;
use crate::error::FsError;

type HmacSha256 = Hmac<Sha256>;

const TAG_LEN: usize = 32;
/// Mount timestamps older or newer than this are rejected (replay window).
const MOUNT_SKEW: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
pub struct Session {
    pub bucket: String,
    pub read_write: bool,
    /// The mounting process, stable across its re-mounts.
    pub instance: Uuid,
}

pub struct Auth {
    secret: [u8; 32],
    required: bool,
    token_ttl: Duration,
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn hmac_tag(secret: &[u8], payload: &[u8]) -> [u8; TAG_LEN] {
    let mut mac = HmacSha256::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(payload);
    mac.finalize().into_bytes().into()
}

impl Auth {
    pub fn new(config: &Config) -> Self {
        Self {
            secret: rand::random(),
            required: config.auth_required,
            token_ttl: config.token_ttl(),
        }
    }

    /// Check the Mount signature and the key's bucket permissions.
    pub async fn verify_mount(
        &self,
        req: &MountRequest,
        rss: &RpcClientRss,
        rss_timeout: Duration,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        if !self.required {
            return Ok(());
        }
        let now = unix_ms();
        let skew = MOUNT_SKEW.as_millis() as u64;
        if req.timestamp_ms.abs_diff(now) > skew {
            return Err(FsError::Unauthorized(
                "mount timestamp outside window".into(),
            ));
        }
        if req.api_key_id.is_empty() {
            return Err(FsError::Unauthorized("missing api key id".into()));
        }
        let full_key = format!("api_key:{}", req.api_key_id);
        let (_version, json) = rss
            .get(&full_key, Some(rss_timeout), trace_id, 0)
            .await
            .map_err(|e| FsError::Unauthorized(format!("api key lookup failed: {e}")))?;
        let api_key: ApiKey = serde_json::from_str(&json)
            .map_err(|e| FsError::Unauthorized(format!("api key decode failed: {e}")))?;
        if api_key.is_deleted {
            return Err(FsError::Unauthorized("api key deleted".into()));
        }
        let expected = fs_gateway_codec::mount_signature(
            api_key.secret_key.as_bytes(),
            &req.api_key_id,
            &req.bucket,
            req.timestamp_ms,
            &req.nonce,
        );
        if !constant_time_eq(&expected, &req.signature) {
            return Err(FsError::Unauthorized("bad mount signature".into()));
        }
        if !api_key.allow_read(&req.bucket) {
            return Err(FsError::Unauthorized("no read permission on bucket".into()));
        }
        if req.read_write && !api_key.allow_write(&req.bucket) {
            return Err(FsError::Unauthorized(
                "no write permission on bucket".into(),
            ));
        }
        Ok(())
    }

    /// Token layout: `[u8 read_write][u64 expiry_ms][16-byte instance]
    /// [bucket][32-byte tag]`.
    pub fn issue_token(&self, session: &Session) -> Bytes {
        let expiry = unix_ms().saturating_add(self.token_ttl.as_millis() as u64);
        let mut payload = BytesMut::with_capacity(25 + session.bucket.len() + TAG_LEN);
        payload.put_u8(session.read_write as u8);
        payload.put_u64_le(expiry);
        payload.put_slice(session.instance.as_bytes());
        payload.put_slice(session.bucket.as_bytes());
        let tag = hmac_tag(&self.secret, &payload);
        payload.put_slice(&tag);
        payload.freeze()
    }

    pub fn verify_token(&self, token: &[u8]) -> Result<Session, FsError> {
        if token.len() < 25 + TAG_LEN {
            return Err(FsError::Unauthorized("malformed session token".into()));
        }
        let (payload, tag) = token.split_at(token.len() - TAG_LEN);
        if !constant_time_eq(&hmac_tag(&self.secret, payload), tag) {
            return Err(FsError::Unauthorized("unknown session token".into()));
        }
        let read_write = payload[0] != 0;
        let expiry = u64::from_le_bytes(payload[1..9].try_into().expect("8 bytes"));
        if unix_ms() > expiry {
            return Err(FsError::Unauthorized("session token expired".into()));
        }
        let instance = Uuid::from_slice(&payload[9..25]).expect("16 bytes");
        let bucket = std::str::from_utf8(&payload[25..])
            .map_err(|_| FsError::Unauthorized("malformed session token".into()))?
            .to_string();
        Ok(Session {
            bucket,
            read_write,
            instance,
        })
    }
}

impl Auth {
    /// Sign an opaque payload under the process secret: `payload || tag`.
    pub fn sign(&self, payload: &[u8]) -> Bytes {
        let mut out = BytesMut::with_capacity(payload.len() + TAG_LEN);
        out.put_slice(payload);
        out.put_slice(&hmac_tag(&self.secret, payload));
        out.freeze()
    }

    /// The payload of a value produced by `sign`, if the tag verifies.
    pub fn verify_signed<'a>(&self, signed: &'a [u8], what: &str) -> Result<&'a [u8], FsError> {
        if signed.len() < TAG_LEN {
            return Err(FsError::Unauthorized(format!("malformed {what}")));
        }
        let (payload, tag) = signed.split_at(signed.len() - TAG_LEN);
        if !constant_time_eq(&hmac_tag(&self.secret, payload), tag) {
            return Err(FsError::Unauthorized(format!("unknown {what}")));
        }
        Ok(payload)
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
