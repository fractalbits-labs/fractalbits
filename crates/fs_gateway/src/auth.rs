//! Mount authentication and session tokens.
//!
//! `Mount` is verified against the API key stored in RSS (the same key
//! the S3 gateway uses). Every later request carries a session token:
//! an HMAC over `(bucket, scope, read_write, owner, expiry)` under a
//! per-process secret, so verification is a hash and never a lookup. A
//! token from another gateway process fails verification and the client
//! re-mounts.

use bytes::{BufMut, Bytes, BytesMut};
use data_types::{ApiKey, TraceId, mgmt_sig::constant_time_eq, scope::in_scope};
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
    /// The key had `allow_owner` on the bucket at `Mount`. Read by nobody
    /// until claims land; carried now so the token layout is final.
    pub owner: bool,
    /// The mounting process, stable across its re-mounts.
    pub instance: Uuid,
    /// Directory key the session is confined to, `/` for the whole bucket.
    pub scope: String,
}

impl Session {
    pub fn in_scope(&self, key: &str) -> bool {
        in_scope(&self.scope, key)
    }
}

const FLAG_READ_WRITE: u8 = 1;
const FLAG_OWNER: u8 = 2;

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
    /// Returns whether the key owns the bucket.
    pub async fn verify_mount(
        &self,
        req: &MountRequest,
        rss: &RpcClientRss,
        rss_timeout: Duration,
        trace_id: &TraceId,
    ) -> Result<bool, FsError> {
        if !self.required {
            return Ok(false);
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
            &req.prefix,
            "",
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
        Ok(api_key.allow_owner(&req.bucket))
    }

    /// Token layout: `[u8 flags][u64 expiry_ms][16-byte instance]
    /// [u16 len][bucket][u16 len][prefix][u16 len][holder][32-byte tag]`.
    /// Flag bit 0 is `read_write`, bit 1 `owner`. The holder is empty
    /// until claims land; the layout already has its slot.
    pub fn issue_token(&self, session: &Session) -> Bytes {
        let expiry = unix_ms().saturating_add(self.token_ttl.as_millis() as u64);
        let mut payload =
            BytesMut::with_capacity(31 + session.bucket.len() + session.scope.len() + TAG_LEN);
        let mut flags = 0u8;
        if session.read_write {
            flags |= FLAG_READ_WRITE;
        }
        if session.owner {
            flags |= FLAG_OWNER;
        }
        payload.put_u8(flags);
        payload.put_u64_le(expiry);
        payload.put_slice(session.instance.as_bytes());
        for field in [session.bucket.as_str(), session.scope.as_str(), ""] {
            payload.put_u16_le(field.len() as u16);
            payload.put_slice(field.as_bytes());
        }
        let tag = hmac_tag(&self.secret, &payload);
        payload.put_slice(&tag);
        payload.freeze()
    }

    pub fn verify_token(&self, token: &[u8]) -> Result<Session, FsError> {
        let malformed = || FsError::Unauthorized("malformed session token".into());
        if token.len() < 31 + TAG_LEN {
            return Err(malformed());
        }
        let (payload, tag) = token.split_at(token.len() - TAG_LEN);
        if !constant_time_eq(&hmac_tag(&self.secret, payload), tag) {
            return Err(FsError::Unauthorized("unknown session token".into()));
        }
        let flags = payload[0];
        let expiry = u64::from_le_bytes(payload[1..9].try_into().expect("8 bytes"));
        if unix_ms() > expiry {
            return Err(FsError::Unauthorized("session token expired".into()));
        }
        let instance = Uuid::from_slice(&payload[9..25]).expect("16 bytes");
        let mut rest = &payload[25..];
        let mut next = || -> Result<String, FsError> {
            if rest.len() < 2 {
                return Err(malformed());
            }
            let len = u16::from_le_bytes([rest[0], rest[1]]) as usize;
            rest = &rest[2..];
            if rest.len() < len {
                return Err(malformed());
            }
            let (field, tail) = rest.split_at(len);
            rest = tail;
            std::str::from_utf8(field)
                .map(str::to_string)
                .map_err(|_| malformed())
        };
        let bucket = next()?;
        let scope = next()?;
        let _holder = next()?;
        if !rest.is_empty() {
            return Err(malformed());
        }
        Ok(Session {
            bucket,
            read_write: flags & FLAG_READ_WRITE != 0,
            owner: flags & FLAG_OWNER != 0,
            instance,
            scope,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_auth() -> Auth {
        Auth {
            secret: [7u8; 32],
            required: true,
            token_ttl: Duration::from_secs(60),
        }
    }

    #[test]
    fn token_round_trip() {
        let auth = test_auth();
        for (scope, read_write, owner) in [
            ("/", false, false),
            ("/repo/src/", true, true),
            ("/donn\u{e9}es/\u{1f5c4}/", true, false),
        ] {
            let session = Session {
                bucket: "proj".into(),
                read_write,
                owner,
                instance: Uuid::new_v4(),
                scope: scope.to_string(),
            };
            let token = auth.issue_token(&session);
            let back = auth.verify_token(&token).expect("verify");
            assert_eq!(back.bucket, session.bucket, "bucket");
            assert_eq!(back.scope, session.scope, "scope");
            assert_eq!(back.read_write, read_write, "read_write");
            assert_eq!(back.owner, owner, "owner");
            assert_eq!(back.instance, session.instance, "instance");
        }
    }

    #[test]
    fn token_tamper_and_truncation_fail() {
        let auth = test_auth();
        let session = Session {
            bucket: "proj".into(),
            read_write: true,
            owner: false,
            instance: Uuid::new_v4(),
            scope: "/a/".into(),
        };
        let token = auth.issue_token(&session);
        let mut flipped = token.to_vec();
        flipped[30] ^= 1;
        auth.verify_token(&flipped).expect_err("flipped byte");
        auth.verify_token(&token[..token.len() - 1])
            .expect_err("truncated");
        auth.verify_token(b"").expect_err("empty");
        let other = Auth {
            secret: [9u8; 32],
            ..test_auth()
        };
        other
            .verify_token(&token)
            .expect_err("other process secret");
    }

    #[test]
    fn scope_membership() {
        let session = Session {
            bucket: "proj".into(),
            read_write: true,
            owner: false,
            instance: Uuid::new_v4(),
            scope: "/repo/".into(),
        };
        assert!(session.in_scope("/repo/"), "scope dir");
        assert!(session.in_scope("/repo/x"), "inside");
        assert!(!session.in_scope("/repository/x"), "sibling");
        assert!(!session.in_scope("/x"), "outside");
    }
}
