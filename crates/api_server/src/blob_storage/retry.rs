use aws_sdk_s3::error::{ProvideErrorMetadata, SdkError};
use metrics_wrapper::{counter, histogram};
use rand::RngExt;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Configuration for retry behavior
#[derive(Clone, Debug, serde::Deserialize)]
pub struct S3RetryConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub max_attempts: u32,
    pub initial_backoff_us: u64,
    pub max_backoff_us: u64,
    pub backoff_multiplier: f64,
}

fn default_enabled() -> bool {
    true
}

impl Default for S3RetryConfig {
    // "standard" mode
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: 8,
            initial_backoff_us: 15_000,
            max_backoff_us: 2_000_000,
            backoff_multiplier: 1.8,
        }
    }
}

impl S3RetryConfig {
    /// Configuration optimized for rate-limited S3 operations
    pub fn rate_limited() -> Self {
        Self {
            enabled: true,
            max_attempts: 15,
            initial_backoff_us: 50,  // 50 microseconds
            max_backoff_us: 500,     // 500 microseconds (0.5ms cap)
            backoff_multiplier: 1.0, // no exponential growth
        }
    }

    /// Configuration for disabled retries (single attempt only)
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            max_attempts: 1,
            initial_backoff_us: 0,
            max_backoff_us: 0,
            backoff_multiplier: 1.0,
        }
    }
}

/// Determines if an error is retryable
pub fn is_retryable_error<E>(error: &SdkError<E>) -> bool
where
    E: ProvideErrorMetadata,
{
    // First check SdkError variants for structural error classification
    match error {
        // Timeout errors are always retryable
        SdkError::TimeoutError(_) => return true,

        // Dispatch failures (network issues) are retryable
        SdkError::DispatchFailure(_) => return true,

        // Service errors need deeper inspection
        SdkError::ServiceError(service_error) => {
            let error_metadata = service_error.err();

            // Check HTTP status code if available
            if let Some(status) = error_metadata.code() {
                // 5xx server errors are generally retryable
                if status.starts_with("5") {
                    return true;
                }
                // 408 Request Timeout
                if status == "408" {
                    return true;
                }
                // 429 Too Many Requests
                if status == "429" {
                    return true;
                }
                // 503 Service Unavailable
                if status == "503" {
                    return true;
                }
            }

            // Check specific AWS error codes
            if let Some(error_code) = error_metadata.code() {
                match error_code {
                    "SlowDown" | "Throttling" | "ThrottlingException" => return true,
                    "RequestTimeout" | "RequestTimeoutException" => return true,
                    "ServiceUnavailable" | "ServiceUnavailableException" => return true,
                    "InternalError" | "InternalServerError" => return true,
                    "TooManyRequests" => return true,
                    _ => {}
                }
            }
        }

        // Construction and response errors are generally not retryable
        SdkError::ConstructionFailure(_) | SdkError::ResponseError { .. } => return false,

        // Handle any other variants
        _ => {}
    }

    // Fallback to string-based checks for edge cases
    let error_str = error.to_string();

    // SlowDown errors (throttling)
    if error_str.contains("SlowDown") || error_str.contains("503") {
        return true;
    }

    // RequestTimeout errors
    if error_str.contains("RequestTimeout") || error_str.contains("408") {
        return true;
    }

    // ServiceUnavailable errors
    if error_str.contains("ServiceUnavailable") {
        return true;
    }

    // TooManyRequests errors (429)
    if error_str.contains("TooManyRequests") || error_str.contains("429") {
        return true;
    }

    // Connection errors
    if error_str.contains("connection") || error_str.contains("timed out") {
        return true;
    }

    // Generic service errors (common transient issues)
    if error_str.contains("service error") {
        return true;
    }

    false
}

/// Extract concise error information for logging
fn get_error_info<E>(error: &SdkError<E>) -> String
where
    E: ProvideErrorMetadata,
{
    match error {
        SdkError::ServiceError(service_error) => {
            let error_metadata = service_error.err();
            let error_code = error_metadata.code().unwrap_or("Unknown");
            let message = error_metadata.message().unwrap_or("No message");
            format!("ServiceError: Code={error_code}, Message={message}")
        }
        SdkError::TimeoutError(_) => "TimeoutError".to_string(),
        SdkError::DispatchFailure(dispatch_failure) => {
            format!("DispatchFailure: {dispatch_failure:?}")
        }
        SdkError::ConstructionFailure(_) => "ConstructionFailure".to_string(),
        SdkError::ResponseError { .. } => "ResponseError".to_string(),
        _ => format!("UnknownError: {error}"),
    }
}

/// Calculate backoff with jitter
pub fn calculate_backoff(attempt: u32, config: &S3RetryConfig) -> Duration {
    let base_backoff =
        config.initial_backoff_us as f64 * config.backoff_multiplier.powi(attempt as i32 - 1);
    let capped_backoff = base_backoff.min(config.max_backoff_us as f64);

    // Add jitter (0.8x to 1.2x for tighter control)
    let jitter = rand::rng().random_range(0.8..1.2);
    let final_backoff = (capped_backoff * jitter) as u64;

    Duration::from_micros(final_backoff)
}

/// Execute an S3 operation with retry logic
pub async fn retry_s3_operation<F, Fut, T, E>(
    operation_name: &str,
    storage_type: &str,
    bucket: &str,
    config: &S3RetryConfig,
    mut operation: F,
) -> Result<T, SdkError<E>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, SdkError<E>>>,
    E: std::error::Error + Send + Sync + 'static + ProvideErrorMetadata,
{
    let start_time = Instant::now();
    let mut attempt = 0;
    let mut total_backoff_ms = 0u64;

    // Convert string references to owned strings for use in metrics
    let operation_name = operation_name.to_string();
    let _storage_type = storage_type.to_string();
    let _bucket = bucket.to_string();

    loop {
        attempt += 1;
        let attempt_start = Instant::now();

        debug!(
            "Attempting {} operation (attempt {}/{})",
            operation_name, attempt, config.max_attempts
        );

        match operation().await {
            Ok(result) => {
                // Record success metrics
                if attempt > 1 {
                    counter!(
                        "s3_operation_retry_success",
                        "operation" => operation_name.clone(),
                        "storage" => _storage_type.clone(),
                        "bucket" => _bucket.clone()
                    )
                    .increment(1);

                    histogram!(
                        "s3_operation_retry_attempts",
                        "operation" => operation_name.clone(),
                        "storage" => _storage_type.clone(),
                        "bucket" => _bucket.clone()
                    )
                    .record(attempt as f64);

                    histogram!(
                        "s3_operation_retry_total_duration_ms",
                        "operation" => operation_name.clone(),
                        "storage" => _storage_type.clone(),
                        "bucket" => _bucket.clone()
                    )
                    .record(start_time.elapsed().as_millis() as f64);

                    histogram!(
                        "s3_operation_retry_backoff_ms",
                        "operation" => operation_name.clone(),
                        "storage" => _storage_type.clone(),
                        "bucket" => _bucket.clone()
                    )
                    .record(total_backoff_ms as f64);

                    debug!(
                        "S3 {} operation succeeded after {} attempts ({:.2}ms total, {:.2}ms in backoff)",
                        operation_name,
                        attempt,
                        start_time.elapsed().as_millis(),
                        total_backoff_ms
                    );
                }

                return Ok(result);
            }
            Err(err) => {
                let attempt_duration = attempt_start.elapsed();

                // Record attempt metrics
                counter!(
                    "s3_operation_attempt_failed",
                    "operation" => operation_name.clone(),
                    "storage" => _storage_type.clone(),
                    "bucket" => _bucket.clone(),
                    "attempt" => attempt.to_string()
                )
                .increment(1);

                histogram!(
                    "s3_operation_attempt_duration_ms",
                    "operation" => operation_name.clone(),
                    "storage" => _storage_type.clone(),
                    "bucket" => _bucket.clone(),
                    "attempt" => attempt.to_string()
                )
                .record(attempt_duration.as_millis() as f64);

                // Check if error is retryable and we have attempts left
                if !is_retryable_error(&err) {
                    let error_info = get_error_info(&err);
                    warn!(
                        "S3 {} operation failed with non-retryable error after {} attempts: {}",
                        operation_name, attempt, error_info
                    );

                    counter!(
                        "s3_operation_failed_non_retryable",
                        "operation" => operation_name.clone(),
                        "storage" => _storage_type.clone(),
                        "bucket" => _bucket.clone()
                    )
                    .increment(1);

                    return Err(err);
                }

                if attempt >= config.max_attempts {
                    warn!(
                        "S3 {} operation failed after {} attempts (max reached): {}",
                        operation_name, attempt, err
                    );

                    counter!(
                        "s3_operation_failed_max_attempts",
                        "operation" => operation_name.clone(),
                        "storage" => _storage_type.clone(),
                        "bucket" => _bucket.clone()
                    )
                    .increment(1);

                    histogram!(
                        "s3_operation_failed_total_duration_ms",
                        "operation" => operation_name.clone(),
                        "storage" => _storage_type.clone(),
                        "bucket" => _bucket.clone()
                    )
                    .record(start_time.elapsed().as_millis() as f64);

                    return Err(err);
                }

                // Calculate backoff and wait
                let backoff = calculate_backoff(attempt, config);
                let backoff_ms = backoff.as_millis() as u64;
                total_backoff_ms += backoff_ms;

                // Check for specific throttling errors and record them
                let error_str = err.to_string();
                if error_str.contains("SlowDown") {
                    counter!(
                        "s3_operation_throttled",
                        "operation" => operation_name.clone(),
                        "storage" => _storage_type.clone(),
                        "bucket" => _bucket.clone()
                    )
                    .increment(1);
                }

                let error_info = get_error_info(&err);
                debug!(
                    "S3 {} operation attempt {} failed, retrying after {}ms: {}",
                    operation_name, attempt, backoff_ms, error_info
                );

                ntex::time::sleep(backoff).await;
            }
        }
    }
}
