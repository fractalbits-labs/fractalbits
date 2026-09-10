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
