//! `FBSIG1` request signatures for the control-plane `/v1` routes.
//!
//! `Authorization: FBSIG1 key_id=<id>, ts=<unix ms>, nonce=<hex>, sig=<hex>`
//! where `sig` is `data_types::mgmt_sig::mgmt_signature` over the method,
//! path, query string and body under the API key's secret, so the secret
//! never travels and no parameter such as `force` can be added later.
//! Verification mirrors `fs_gateway`'s mount check: replay window, key
//! lookup through the existing cache, constant-time compare.

use actix_web::{
    Error, HttpMessage, HttpResponse,
    body::MessageBody,
    dev::{Payload, ServiceRequest, ServiceResponse},
    http::StatusCode,
    middleware::Next,
    web::{Bytes, Data},
};
use data_types::{
    ApiKey, TraceId, Versioned,
    drive::unix_ms,
    mgmt_sig::{MGMT_SKEW, constant_time_eq, mgmt_signature},
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tracing::debug;

use crate::AppState;

/// Largest signed body accepted; drive metadata is far below this.
pub const MAX_BODY: usize = 64 * 1024;

/// The verified key, inserted into request extensions for the handlers.
#[derive(Clone)]
pub struct AuthedKey(pub Versioned<ApiKey>);

/// The one error shape every `/v1` response uses on failure.
#[derive(Serialize)]
pub struct ApiError<'a> {
    pub code: &'a str,
    pub message: String,
}

pub fn api_error(status: StatusCode, code: &str, message: impl Into<String>) -> HttpResponse {
    HttpResponse::build(status).json(ApiError {
        code,
        message: message.into(),
    })
}

#[derive(Debug, PartialEq, Eq)]
pub struct FbSig1Header {
    pub key_id: String,
    pub timestamp_ms: u64,
    pub nonce: Vec<u8>,
    pub signature: Vec<u8>,
}

/// Parse the header value after the scheme has been checked.
pub fn parse_fbsig1_header(value: &str) -> Option<FbSig1Header> {
    let rest = value.strip_prefix("FBSIG1 ")?;
    let (mut key_id, mut ts, mut nonce, mut sig) = (None, None, None, None);
    for part in rest.split(',') {
        let (k, v) = part.trim().split_once('=')?;
        match k {
            "key_id" => key_id = Some(v.to_string()),
            "ts" => ts = v.parse().ok(),
            "nonce" => nonce = hex::decode(v).ok(),
            "sig" => sig = hex::decode(v).ok(),
            _ => return None,
        }
    }
    Some(FbSig1Header {
        key_id: key_id?,
        timestamp_ms: ts?,
        nonce: nonce?,
        signature: sig?,
    })
}

pub fn body_sha256_hex(body: &[u8]) -> String {
    hex::encode(Sha256::digest(body))
}

/// The pure check, so it can be tested without a gateway. `required`
/// false skips the signature but still needs a live key.
#[allow(clippy::too_many_arguments)]
pub fn check_fbsig1(
    hdr: &FbSig1Header,
    api_key: &ApiKey,
    method: &str,
    path: &str,
    query: &str,
    body_sha256_hex: &str,
    now_ms: u64,
    required: bool,
) -> Result<(), &'static str> {
    if api_key.is_deleted {
        return Err("api key deleted");
    }
    if !required {
        return Ok(());
    }
    if hdr.timestamp_ms.abs_diff(now_ms) > MGMT_SKEW.as_millis() as u64 {
        return Err("timestamp outside window");
    }
    let expected = mgmt_signature(
        api_key.secret_key.as_bytes(),
        &hdr.key_id,
        hdr.timestamp_ms,
        &hdr.nonce,
        method,
        path,
        query,
        body_sha256_hex,
    );
    if !constant_time_eq(&expected, &hdr.signature) {
        return Err("bad signature");
    }
    Ok(())
}

fn unauthorized(req: ServiceRequest, why: &str) -> Result<ServiceResponse, Error> {
    debug!("mgmt auth refused: {why}");
    Ok(req.into_response(api_error(StatusCode::UNAUTHORIZED, "unauthorized", why)))
}

/// `actix_web::middleware::from_fn` body for the `/v1` scope.
pub async fn fbsig1_auth(
    req: ServiceRequest,
    next: Next<impl MessageBody + 'static>,
) -> Result<ServiceResponse, Error> {
    let header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_fbsig1_header);
    let Some(hdr) = header else {
        return unauthorized(req, "missing or malformed FBSIG1 authorization");
    };
    let Some(app) = req.app_data::<Data<Arc<AppState>>>().cloned() else {
        return Ok(req.into_response(api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "app state missing",
        )));
    };

    let (http_req, mut payload) = req.into_parts();
    let body = match <Bytes as actix_web::FromRequest>::from_request(&http_req, &mut payload).await
    {
        Ok(body) if body.len() <= MAX_BODY => body,
        Ok(_) => {
            return Ok(ServiceResponse::new(
                http_req,
                api_error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "too_large",
                    "body over limit",
                ),
            ));
        }
        Err(e) => {
            return Ok(ServiceResponse::new(
                http_req,
                api_error(StatusCode::BAD_REQUEST, "bad_body", e.to_string()),
            ));
        }
    };
    let method = http_req.method().as_str().to_string();
    let path = http_req.path().to_string();
    let query = http_req.query_string().to_string();
    let req = ServiceRequest::from_parts(http_req, Payload::from(body.clone()));

    let trace_id = TraceId::new();
    let api_key = match app.get_api_key(hdr.key_id.clone(), &trace_id).await {
        Ok(key) => key,
        Err(e) => return unauthorized(req, &format!("api key lookup failed: {e}")),
    };
    if let Err(why) = check_fbsig1(
        &hdr,
        &api_key.data,
        &method,
        &path,
        &query,
        &body_sha256_hex(&body),
        unix_ms(),
        app.config.mgmt_auth_required,
    ) {
        return unauthorized(req, why);
    }
    req.extensions_mut().insert(AuthedKey(api_key));
    let res = next.call(req).await?;
    Ok(res.map_into_boxed_body())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signed(
        key: &ApiKey,
        ts: u64,
        method: &str,
        path: &str,
        query: &str,
        body: &[u8],
    ) -> FbSig1Header {
        let nonce = vec![1, 2, 3, 4];
        let signature = mgmt_signature(
            key.secret_key.as_bytes(),
            &key.key_id,
            ts,
            &nonce,
            method,
            path,
            query,
            &body_sha256_hex(body),
        );
        FbSig1Header {
            key_id: key.key_id.clone(),
            timestamp_ms: ts,
            nonce,
            signature,
        }
    }

    #[test]
    fn header_round_trip() {
        let key = ApiKey::new_for_test();
        let hdr = signed(&key, 1_000, "POST", "/v1/drives", "", b"{}");
        let text = format!(
            "FBSIG1 key_id={}, ts={}, nonce={}, sig={}",
            hdr.key_id,
            hdr.timestamp_ms,
            hex::encode(&hdr.nonce),
            hex::encode(&hdr.signature)
        );
        assert_eq!(parse_fbsig1_header(&text), Some(hdr), "parse back");
        assert!(parse_fbsig1_header("Bearer x").is_none(), "wrong scheme");
        assert!(
            parse_fbsig1_header("FBSIG1 key_id=a, ts=1").is_none(),
            "missing fields"
        );
        assert!(
            parse_fbsig1_header("FBSIG1 key_id=a, ts=1, nonce=zz, sig=00").is_none(),
            "bad hex"
        );
    }

    #[test]
    fn signature_checks() {
        let key = ApiKey::new_for_test();
        let now = 1_000_000;
        let body = b"{\"name\":\"d1\"}";
        let hdr = signed(&key, now, "POST", "/v1/drives", "", body);
        let sha = body_sha256_hex(body);
        assert_eq!(
            check_fbsig1(&hdr, &key, "POST", "/v1/drives", "", &sha, now, true),
            Ok(()),
            "good"
        );
        assert_eq!(
            check_fbsig1(
                &hdr,
                &key,
                "POST",
                "/v1/drives",
                "",
                &body_sha256_hex(b"{}"),
                now,
                true,
            ),
            Err("bad signature"),
            "tampered body"
        );
        assert_eq!(
            check_fbsig1(&hdr, &key, "DELETE", "/v1/drives", "", &sha, now, true),
            Err("bad signature"),
            "different method"
        );
        assert_eq!(
            check_fbsig1(
                &hdr,
                &key,
                "POST",
                "/v1/drives",
                "force=true",
                &sha,
                now,
                true
            ),
            Err("bad signature"),
            "query added after signing"
        );
        let forced = signed(&key, now, "DELETE", "/v1/drives/d1", "force=true", b"");
        assert_eq!(
            check_fbsig1(
                &forced,
                &key,
                "DELETE",
                "/v1/drives/d1",
                "force=true",
                &body_sha256_hex(b""),
                now,
                true
            ),
            Ok(()),
            "signed query verifies"
        );
        assert_eq!(
            check_fbsig1(
                &forced,
                &key,
                "DELETE",
                "/v1/drives/d1",
                "",
                &body_sha256_hex(b""),
                now,
                true
            ),
            Err("bad signature"),
            "query removed after signing"
        );
        let skew = MGMT_SKEW.as_millis() as u64 + 1;
        assert_eq!(
            check_fbsig1(&hdr, &key, "POST", "/v1/drives", "", &sha, now + skew, true),
            Err("timestamp outside window"),
            "stale"
        );
        let mut other = ApiKey::new_for_test();
        other.secret_key = "other".into();
        assert_eq!(
            check_fbsig1(&hdr, &other, "POST", "/v1/drives", "", &sha, now, true),
            Err("bad signature"),
            "wrong secret"
        );
        let mut deleted = ApiKey::new_for_test();
        deleted.is_deleted = true;
        assert_eq!(
            check_fbsig1(&hdr, &deleted, "POST", "/v1/drives", "", &sha, now, false),
            Err("api key deleted"),
            "deleted key even when not required"
        );
        assert_eq!(
            check_fbsig1(&hdr, &other, "POST", "/v1/drives", "", &sha, now, false),
            Ok(()),
            "not required skips the signature"
        );
    }
}
