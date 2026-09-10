mod error;
pub use error::SignatureError;
pub mod payload;

// Re-export from common crate
pub use aws_signature::streaming::{
    ChunkSignature, ChunkSignatureContext, parse_chunk_signature, verify_chunk_signature,
};

use crate::{
    AppState,
    handler::common::{request::extract::Authentication, s3_error::S3Error},
};
use actix_web::HttpRequest;
use data_types::{ApiKey, TraceId, Versioned};
use std::sync::Arc;
use tracing::{debug, error, warn};

pub async fn check_signature(
    app: Arc<AppState>,
    request: &HttpRequest,
    auth: Option<&Authentication<'_>>,
    trace_id: &TraceId,
) -> Result<Versioned<ApiKey>, S3Error> {
    let allow_missing_or_bad_signature = app.config.allow_missing_or_bad_signature;
    debug!(%allow_missing_or_bad_signature, ?auth, "starting signature verification");

    if let Some(auth) = auth {
        match payload::check_signature_impl(app.clone(), auth, request, trace_id).await {
            Ok(verified) => Ok(verified),
            Err(e) => {
                if allow_missing_or_bad_signature {
                    warn!(?auth, error = ?e, "allowed bad signature");
                    app.get_test_api_key(trace_id)
                        .await
                        .map_err(|_| S3Error::InvalidAccessKeyId)
                } else {
                    error!(?auth, error = ?e, "verifying request failed");
                    Err(S3Error::from(e))
                }
            }
        }
    } else if allow_missing_or_bad_signature {
        warn!("allowing anonymous access");
        app.get_test_api_key(trace_id)
            .await
            .map_err(|_| S3Error::InvalidAccessKeyId)
    } else {
        error!("no valid authentication found");
        Err(S3Error::InvalidSignature)
    }
}
