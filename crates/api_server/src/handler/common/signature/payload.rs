use aws_signature::sigv4::uri_encode;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use crate::{
    AppState,
    handler::common::{
        request::extract::{AuthSource, Authentication, CanonicalRequestHasher},
        signature::SignatureError,
    },
};
use aws_signature::{get_signing_key_cached, verify_signature};
use data_types::{ApiKey, TraceId, Versioned};
use ntex::http::header::HOST;
use ntex::web::{HttpRequest, types::Query};

pub async fn check_signature_impl(
    app: Rc<AppState>,
    auth: &Authentication<'_>,
    request: &HttpRequest,
    trace_id: &TraceId,
) -> Result<Versioned<ApiKey>, SignatureError> {
    match auth.source {
        AuthSource::Header => check_header_based_signature(app, auth, request, trace_id).await,
        AuthSource::QueryParam => check_query_based_signature(app, auth, request, trace_id).await,
    }
}

async fn check_header_based_signature(
    app: Rc<AppState>,
    authentication: &Authentication<'_>,
    request: &HttpRequest,
    trace_id: &TraceId,
) -> Result<Versioned<ApiKey>, SignatureError> {
    let canonical_hash = hash_canonical_request_streaming(
        request,
        &authentication.signed_headers,
        &authentication.content_sha256,
    )?;

    let string_to_sign = build_string_to_sign(
        &authentication.formatted_date,
        &authentication.scope_string,
        &canonical_hash,
    );

    tracing::trace!(?authentication, %canonical_hash, %string_to_sign);

    let key = verify_v4(app, authentication, &string_to_sign, trace_id).await?;
    Ok(key)
}

async fn check_query_based_signature(
    app: Rc<AppState>,
    authentication: &Authentication<'_>,
    request: &HttpRequest,
    trace_id: &TraceId,
) -> Result<Versioned<ApiKey>, SignatureError> {
    // Build canonical query string excluding X-Amz-Signature
    let canonical_query = build_canonical_query_string_filtered(request.query_string());

    let canonical_hash = hash_canonical_request_for_presigned(
        request,
        &authentication.signed_headers,
        &canonical_query,
    )?;

    let string_to_sign = build_string_to_sign(
        &authentication.formatted_date,
        &authentication.scope_string,
        &canonical_hash,
    );

    tracing::trace!(?authentication, %canonical_hash, %string_to_sign);

    let key = verify_v4(app, authentication, &string_to_sign, trace_id).await?;
    Ok(key)
}

/// Stream canonical request directly to hasher (header-based auth)
fn hash_canonical_request_streaming(
    request: &HttpRequest,
    signed_headers: &BTreeSet<Cow<'_, str>>,
    payload_hash: &str,
) -> Result<String, SignatureError> {
    let mut hasher = CanonicalRequestHasher::new();

    // Stream HTTP method
    hasher.add_method(request.method().as_str());

    // Stream URI
    hasher.add_uri(request.path());

    // Build and stream canonical query string
    let query_str = build_canonical_query_string(request.query_string());
    hasher.add_query(&query_str);

    // Stream canonical headers
    let header_pairs = collect_signed_headers(request, signed_headers)?;

    // Headers are already sorted because signed_headers is a BTreeSet
    hasher.add_headers(header_pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())));

    // Stream signed headers list
    hasher.add_signed_headers(signed_headers.iter().map(|s| s.as_ref()));

    // Stream payload hash
    hasher.add_payload_hash(payload_hash);

    // Return hex hash (no canonical request string ever created!)
    Ok(hasher.finalize())
}

/// Hash canonical request for pre-signed URL verification
fn hash_canonical_request_for_presigned(
    request: &HttpRequest,
    signed_headers: &BTreeSet<Cow<'_, str>>,
    canonical_query: &str,
) -> Result<String, SignatureError> {
    let mut hasher = CanonicalRequestHasher::new();

    // Stream HTTP method
    hasher.add_method(request.method().as_str());

    // Stream URI
    hasher.add_uri(request.path());

    // Use the pre-filtered canonical query string
    hasher.add_query(canonical_query);

    // Stream canonical headers
    let header_pairs = collect_signed_headers(request, signed_headers)?;
    hasher.add_headers(header_pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())));

    // Stream signed headers list
    hasher.add_signed_headers(signed_headers.iter().map(|s| s.as_ref()));

    // Pre-signed URLs always use UNSIGNED-PAYLOAD
    hasher.add_payload_hash("UNSIGNED-PAYLOAD");

    Ok(hasher.finalize())
}

/// Collect signed header values from request
fn collect_signed_headers(
    request: &HttpRequest,
    signed_headers: &BTreeSet<Cow<'_, str>>,
) -> Result<Vec<(String, String)>, SignatureError> {
    let headers = request.headers();
    let mut header_pairs: Vec<(String, String)> = Vec::with_capacity(signed_headers.len());

    for header_name in signed_headers {
        let value_str = if header_name.as_ref() == "host" && !headers.contains_key(HOST) {
            // For HTTP/2, get host from connection info (:authority pseudo-header)
            let connection_info = request.connection_info();
            let host_value = connection_info.host();
            tracing::debug!("Using host from connection info for HTTP/2: {}", host_value);
            host_value.to_string()
        } else if let Some(header_value) = headers.get(header_name.as_ref()) {
            if let Ok(s) = header_value.to_str() {
                s.trim().to_string()
            } else {
                // For non-ASCII headers, convert bytes to UTF-8 lossy
                String::from_utf8_lossy(header_value.as_bytes())
                    .trim()
                    .to_string()
            }
        } else {
            return Err(SignatureError::Other(format!(
                "signed header `{}` is not present",
                header_name
            )));
        };
        header_pairs.push((header_name.to_string(), value_str));
    }

    Ok(header_pairs)
}

fn build_canonical_query_string(query_str: &str) -> String {
    if query_str.is_empty() {
        return String::new();
    }
    let query_params = Query::<BTreeMap<String, String>>::from_query(query_str)
        .unwrap_or_else(|_| Query(Default::default()))
        .into_inner();
    let mut items = String::with_capacity(query_str.len() * 2);
    for (key, value) in query_params.iter() {
        if !items.is_empty() {
            items.push('&');
        }
        items.push_str(&uri_encode(key, true));
        items.push('=');
        items.push_str(&uri_encode(value, true));
    }
    items
}

/// Build canonical query string for pre-signed URLs, excluding X-Amz-Signature
fn build_canonical_query_string_filtered(query_str: &str) -> String {
    if query_str.is_empty() {
        return String::new();
    }
    let query_params = Query::<BTreeMap<String, String>>::from_query(query_str)
        .unwrap_or_else(|_| Query(Default::default()))
        .into_inner();
    let mut items = String::with_capacity(query_str.len() * 2);
    for (key, value) in query_params.iter() {
        // Exclude X-Amz-Signature from canonical query string
        if key == "X-Amz-Signature" {
            continue;
        }
        if !items.is_empty() {
            items.push('&');
        }
        items.push_str(&uri_encode(key, true));
        items.push('=');
        items.push_str(&uri_encode(value, true));
    }
    items
}

fn build_string_to_sign(
    formatted_datetime: &str,
    credential_scope: &str,
    canonical_hash: &str,
) -> String {
    const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";
    let mut result = String::with_capacity(
        AWS4_HMAC_SHA256.len() + 1 + formatted_datetime.len() + 1 + credential_scope.len() + 1 + 64,
    );
    result.push_str(AWS4_HMAC_SHA256);
    result.push('\n');
    result.push_str(formatted_datetime);
    result.push('\n');
    result.push_str(credential_scope);
    result.push('\n');
    result.push_str(canonical_hash);
    result
}

async fn verify_v4(
    app: Rc<AppState>,
    auth: &Authentication<'_>,
    string_to_sign: &str,
    trace_id: &TraceId,
) -> Result<Versioned<ApiKey>, SignatureError> {
    let key = app.get_api_key(auth.key_id.to_string(), trace_id).await?;

    let signing_key =
        get_signing_key_cached(auth.date, &key.data.secret_key, &app.config.region)
            .map_err(|e| SignatureError::Other(format!("Unable to build signing key: {}", e)))?;

    if !verify_signature(&signing_key, string_to_sign, &auth.signature)? {
        return Err(SignatureError::Other("signature mismatch".into()));
    }

    Ok(key)
}
