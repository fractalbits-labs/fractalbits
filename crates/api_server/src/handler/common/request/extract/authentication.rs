use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap};

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use ntex::http::header::{AUTHORIZATION, ToStrError};
use ntex::web::HttpRequest;
use sha2::{Digest, Sha256};
use thiserror::Error;

use url::form_urlencoded;

use crate::handler::common::{
    s3_error::S3Error,
    time::{LONG_DATETIME, SHORT_DATE},
};

const SCOPE_ENDING: &str = "aws4_request";

#[derive(Error, Debug)]
pub enum AuthError {
    #[error(transparent)]
    ToStrError(#[from] ToStrError),
    #[error("invalid format: {0}")]
    Invalid(String),
    #[error("expired pre-signed URL")]
    Expired,
    #[error("invalid query parameters: {0}")]
    InvalidQueryParam(String),
}

impl From<AuthError> for S3Error {
    fn from(value: AuthError) -> Self {
        tracing::error!("AuthError: {value}");
        match value {
            AuthError::Expired => S3Error::AccessDenied,
            AuthError::InvalidQueryParam(_) => S3Error::AuthorizationQueryParametersError,
            _ => S3Error::AuthorizationHeaderMalformed,
        }
    }
}

/// Tracks whether authentication came from header or query parameters
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthSource {
    Header,
    QueryParam,
}

#[derive(Debug)]
pub struct Authentication<'a> {
    pub key_id: Cow<'a, str>,
    pub scope: Scope<'a>,
    pub signed_headers: BTreeSet<Cow<'a, str>>,
    pub signature: Cow<'a, str>,
    pub content_sha256: Cow<'a, str>,
    pub date: DateTime<Utc>,
    pub scope_string: String,
    pub formatted_date: String,
    pub source: AuthSource,
    /// For pre-signed URLs: the expiration time
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct Scope<'a> {
    pub date: Cow<'a, str>,
    pub region: Cow<'a, str>,
    pub service: Cow<'a, str>,
}

/// Extract authentication from request with zero-copy optimization
pub fn extract_authentication(req: &HttpRequest) -> Result<Option<Authentication<'_>>, AuthError> {
    // Try header-based extraction first (existing path, unchanged hot path)
    if let Some(auth) = extract_header_authentication(req)? {
        return Ok(Some(auth));
    }

    // If no header, try query parameter based authentication (pre-signed URLs)
    if let Some(auth) = extract_query_authentication(req)? {
        return Ok(Some(auth));
    }

    // Neither header nor query params -> anonymous request
    Ok(None)
}

/// Extract authentication from Authorization header (zero-copy)
fn extract_header_authentication(
    req: &HttpRequest,
) -> Result<Option<Authentication<'_>>, AuthError> {
    const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";

    // Note The name of the standard header is unfortunate because it carries authentication
    // information, not authorization.
    // https://docs.aws.amazon.com/AmazonS3/latest/API/RESTAuthentication.html#ConstructingTheAuthenticationHeader
    let authentication = match req.headers().get(AUTHORIZATION) {
        Some(auth) => auth
            .to_str()
            .map_err(|e| AuthError::Invalid(format!("Header error: {e}")))?,
        None => return Ok(None),
    };

    let (auth_kind, rest) = authentication
        .split_once(' ')
        .ok_or(AuthError::Invalid("Authorization field too short".into()))?;

    if auth_kind != AWS4_HMAC_SHA256 {
        return Err(AuthError::Invalid(
            "Unsupported authorization method".into(),
        ));
    }

    let mut auth_params = HashMap::new();
    for auth_part in rest.split(',') {
        let auth_part = auth_part.trim();
        let eq = auth_part
            .find('=')
            .ok_or(AuthError::Invalid("missing =".into()))?;
        let (key, value) = auth_part.split_at(eq);
        auth_params.insert(key, value.trim_start_matches('='));
    }

    let cred = auth_params.get("Credential").ok_or(AuthError::Invalid(
        "Could not find Credential in Authorization field".into(),
    ))?;
    let signed_headers = auth_params
        .get("SignedHeaders")
        .ok_or(AuthError::Invalid(
            "Could not find SignedHeaders in Authorization field".into(),
        ))?
        .split(';')
        .map(Cow::Borrowed)
        .collect();
    let signature = auth_params.get("Signature").ok_or(AuthError::Invalid(
        "Could not find Signature in Authorization field".into(),
    ))?;

    let content_sha256 = req
        .headers()
        .get("x-amz-content-sha256")
        .ok_or(AuthError::Invalid(
            "Missing x-amz-content-sha256 field".into(),
        ))?
        .to_str()
        .map_err(|e| AuthError::Invalid(format!("Header error: {e}")))?;

    let date = req
        .headers()
        .get("x-amz-date")
        .ok_or(AuthError::Invalid("Missing x-amz-date field".into()))?
        .to_str()
        .map_err(|e| AuthError::Invalid(format!("Header error: {e}")))?;
    let date = parse_date(date)?;

    if (Utc::now() - date).num_hours() > 24 {
        return Err(AuthError::Invalid("Date is too old".into()));
    }
    let (key_id, scope) = parse_credential(cred)?;
    if *scope.date != format!("{}", date.format(SHORT_DATE)) {
        return Err(AuthError::Invalid("Date mismatch".into()));
    }

    let scope_string =
        aws_signature::sigv4::format_scope_string(&date, &scope.region, &scope.service);
    let formatted_date = format!("{}", date.format("%Y%m%dT%H%M%SZ"));

    let auth = Authentication {
        key_id: Cow::Borrowed(key_id),
        scope,
        signed_headers,
        signature: Cow::Borrowed(signature),
        content_sha256: Cow::Borrowed(content_sha256),
        date,
        scope_string,
        formatted_date,
        source: AuthSource::Header,
        expires_at: None,
    };
    Ok(Some(auth))
}

/// Extract authentication from query parameters (pre-signed URLs)
fn extract_query_authentication(
    req: &HttpRequest,
) -> Result<Option<Authentication<'_>>, AuthError> {
    let query_str = req.query_string();
    if query_str.is_empty() {
        return Ok(None);
    }

    // Parse query parameters
    let params: HashMap<String, String> = form_urlencoded::parse(query_str.as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();

    // Check for X-Amz-Algorithm to determine if this is a pre-signed URL
    let algorithm = match params.get("X-Amz-Algorithm") {
        Some(alg) => alg,
        None => return Ok(None),
    };

    if algorithm != "AWS4-HMAC-SHA256" {
        return Err(AuthError::InvalidQueryParam(format!(
            "Unsupported algorithm: {}",
            algorithm
        )));
    }

    // Extract required query parameters
    let credential = params
        .get("X-Amz-Credential")
        .ok_or_else(|| AuthError::InvalidQueryParam("Missing X-Amz-Credential".into()))?;

    let date_str = params
        .get("X-Amz-Date")
        .ok_or_else(|| AuthError::InvalidQueryParam("Missing X-Amz-Date".into()))?;

    let expires_str = params
        .get("X-Amz-Expires")
        .ok_or_else(|| AuthError::InvalidQueryParam("Missing X-Amz-Expires".into()))?;

    let signed_headers_str = params
        .get("X-Amz-SignedHeaders")
        .ok_or_else(|| AuthError::InvalidQueryParam("Missing X-Amz-SignedHeaders".into()))?;

    let signature = params
        .get("X-Amz-Signature")
        .ok_or_else(|| AuthError::InvalidQueryParam("Missing X-Amz-Signature".into()))?;

    // Parse date
    let date = parse_date(date_str)?;

    // Parse expires and check expiration (fail fast before crypto)
    let expires_seconds: i64 = expires_str.parse().map_err(|_| {
        AuthError::InvalidQueryParam(format!("Invalid X-Amz-Expires: {}", expires_str))
    })?;

    // AWS allows 1 second to 7 days (604800 seconds)
    if !(1..=604800).contains(&expires_seconds) {
        return Err(AuthError::InvalidQueryParam(format!(
            "X-Amz-Expires must be between 1 and 604800, got {}",
            expires_seconds
        )));
    }

    let expires_at = date + chrono::Duration::seconds(expires_seconds);
    if Utc::now() > expires_at {
        return Err(AuthError::Expired);
    }

    // Parse credential
    let (key_id, scope) = parse_credential_owned(credential)?;
    if *scope.date != format!("{}", date.format(SHORT_DATE)) {
        return Err(AuthError::Invalid("Date mismatch".into()));
    }

    let scope_string =
        aws_signature::sigv4::format_scope_string(&date, &scope.region, &scope.service);
    let formatted_date = format!("{}", date.format("%Y%m%dT%H%M%SZ"));

    let signed_headers = signed_headers_str
        .split(';')
        .map(|s| Cow::Owned(s.to_string()))
        .collect();

    let auth = Authentication {
        key_id: Cow::Owned(key_id),
        scope,
        signed_headers,
        signature: Cow::Owned(signature.clone()),
        content_sha256: Cow::Borrowed("UNSIGNED-PAYLOAD"),
        date,
        scope_string,
        formatted_date,
        source: AuthSource::QueryParam,
        expires_at: Some(expires_at),
    };

    Ok(Some(auth))
}

fn parse_date(date: &str) -> Result<DateTime<Utc>, AuthError> {
    let date: NaiveDateTime = NaiveDateTime::parse_from_str(date, LONG_DATETIME)
        .map_err(|_| AuthError::Invalid("Invalid date".into()))?;
    Ok(Utc.from_utc_datetime(&date))
}

fn parse_credential(cred: &str) -> Result<(&str, Scope<'_>), AuthError> {
    let parts: Vec<&str> = cred.split('/').collect();
    if parts.len() != 5 || parts[4] != SCOPE_ENDING {
        return Err(AuthError::Invalid("wrong scope format".into()));
    }

    let scope = Scope {
        date: Cow::Borrowed(parts[1]),
        region: Cow::Borrowed(parts[2]),
        service: Cow::Borrowed(parts[3]),
    };
    Ok((parts[0], scope))
}

/// Parse credential string into owned values (for query param auth)
fn parse_credential_owned(cred: &str) -> Result<(String, Scope<'static>), AuthError> {
    let parts: Vec<&str> = cred.split('/').collect();
    if parts.len() != 5 || parts[4] != SCOPE_ENDING {
        return Err(AuthError::InvalidQueryParam(
            "wrong credential scope format".into(),
        ));
    }

    let scope = Scope {
        date: Cow::Owned(parts[1].to_string()),
        region: Cow::Owned(parts[2].to_string()),
        service: Cow::Owned(parts[3].to_string()),
    };
    Ok((parts[0].to_string(), scope))
}

/// Zero-copy SigV4 canonical request hasher
/// Streams bytes directly into SHA-256 without intermediate buffers
pub struct CanonicalRequestHasher {
    hasher: Sha256,
}

impl CanonicalRequestHasher {
    pub fn new() -> Self {
        Self {
            hasher: Sha256::new(),
        }
    }

    #[inline]
    fn write_str(&mut self, s: &str) {
        self.hasher.update(s.as_bytes());
    }

    #[inline]
    fn write_byte(&mut self, b: u8) {
        self.hasher.update([b]);
    }

    /// Add HTTP method to canonical request
    #[inline]
    pub fn add_method(&mut self, method: &str) {
        self.write_str(method);
        self.write_byte(b'\n');
    }

    /// Add URI path to canonical request
    #[inline]
    pub fn add_uri(&mut self, uri: &str) {
        self.write_str(uri);
        self.write_byte(b'\n');
    }

    /// Add query string to canonical request
    #[inline]
    pub fn add_query(&mut self, query: &str) {
        self.write_str(query);
        self.write_byte(b'\n');
    }

    /// Add canonical headers in sorted order
    /// Headers should be pre-sorted by the caller
    pub fn add_headers<'a, I>(&mut self, headers: I)
    where
        I: Iterator<Item = (&'a str, &'a str)>,
    {
        for (name, value) in headers {
            self.write_str(name);
            self.write_byte(b':');
            self.write_str(value);
            self.write_byte(b'\n');
        }
        self.write_byte(b'\n');
    }

    /// Add signed headers list
    pub fn add_signed_headers<'a, I>(&mut self, signed_headers: I)
    where
        I: Iterator<Item = &'a str>,
    {
        let mut first = true;
        for header in signed_headers {
            if !first {
                self.write_byte(b';');
            }
            self.write_str(header);
            first = false;
        }
        self.write_byte(b'\n');
    }

    /// Add payload hash
    pub fn add_payload_hash(&mut self, hash: &str) {
        self.write_str(hash);
    }

    /// Finalize and return the hex-encoded hash
    pub fn finalize(self) -> String {
        let result = self.hasher.finalize();
        hex::encode(result)
    }

    /// Finalize and return raw hash bytes
    pub fn finalize_bytes(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }
}

impl Default for CanonicalRequestHasher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike};
    use ntex::web::test::TestRequest;

    #[test]
    fn test_extract_auth_none() {
        let (req, _) = TestRequest::get().uri("/obj1").to_http_parts();
        let auth = extract_authentication(&req).unwrap();
        assert!(auth.is_none());
    }

    #[test]
    fn test_parse_credential() {
        let cred = "AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request";
        let (key_id, scope) = parse_credential(cred).unwrap();

        assert_eq!(key_id, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(scope.date, "20230101");
        assert_eq!(scope.region, "us-east-1");
        assert_eq!(scope.service, "s3");
        // Verify the scope can be formatted correctly
        let expected_scope = format!(
            "{}/{}/{}/{}",
            scope.date, scope.region, scope.service, SCOPE_ENDING
        );
        assert_eq!(expected_scope, "20230101/us-east-1/s3/aws4_request");
    }

    #[test]
    fn test_parse_credential_invalid_format() {
        let cred = "invalid/format";
        assert!(parse_credential(cred).is_err());

        let cred = "key/date/region/service/wrong_ending";
        assert!(parse_credential(cred).is_err());
    }

    #[test]
    fn test_parse_date() {
        let date_str = "20230101T120000Z";
        let parsed = parse_date(date_str).unwrap();

        assert_eq!(parsed.year(), 2023);
        assert_eq!(parsed.month(), 1);
        assert_eq!(parsed.day(), 1);
        assert_eq!(parsed.hour(), 12);
        assert_eq!(parsed.minute(), 0);
        assert_eq!(parsed.second(), 0);
    }

    #[test]
    fn test_parse_date_invalid() {
        let date_str = "invalid_date";
        assert!(parse_date(date_str).is_err());

        let date_str = "2023-01-01T12:00:00Z"; // Wrong format
        assert!(parse_date(date_str).is_err());
    }

    #[test]
    fn test_canonical_request_hasher() {
        let mut hasher = CanonicalRequestHasher::new();
        hasher.add_method("GET");
        hasher.add_uri("/test");
        hasher.add_query("");
        hasher.add_headers(
            [("host", "example.com"), ("x-amz-date", "20230101T120000Z")]
                .iter()
                .copied(),
        );
        hasher.add_signed_headers(["host", "x-amz-date"].iter().copied());
        hasher.add_payload_hash("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

        let hash = hasher.finalize();
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64);
    }
}
