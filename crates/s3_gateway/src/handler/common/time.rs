use std::time::{Duration, UNIX_EPOCH};

use chrono::DateTime;

pub const SHORT_DATE: &str = "%Y%m%d";
pub const LONG_DATETIME: &str = "%Y%m%dT%H%M%SZ";

pub fn format_timestamp(timestamp: u64) -> String {
    let dt = DateTime::from_timestamp_millis(timestamp as i64).unwrap();
    dt.to_rfc3339()
}

pub fn format_http_date(timestamp: u64) -> String {
    let date = UNIX_EPOCH + Duration::from_millis(timestamp);
    httpdate::fmt_http_date(date)
}
