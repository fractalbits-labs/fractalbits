//! The ARTFS drive record.
//!
//! A drive is a bucket, one to one and same name; this record is the
//! control-plane metadata beside the bucket record, stored in RSS at
//! `drive:{name}` and never read on a data path.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

pub const DRIVE_PREFIX: &str = "drive:";
pub const MAX_DRIVE_LABELS: usize = 32;
/// A force-delete sweep whose heartbeat is older than this is dead: a new
/// `force` request may resume it, and until then repeated requests only
/// answer 202 without starting a second sweeper.
pub const SWEEP_LEASE_MS: u64 = 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriveStatus {
    Ready,
    Deleting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Drive {
    /// Stable reference, survives a later rename of the bucket.
    pub id: String,
    /// The bucket name.
    pub name: String,
    #[serde(default)]
    pub display_name: String,
    /// `BTreeMap` so equal label sets serialise identically.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub status: DriveStatus,
    pub created_at_ms: u64,
    /// Api key id that created the drive.
    pub created_by: String,
    /// `root_blob_name` of the bucket incarnation this record describes. A
    /// bucket deleted and recreated under the same name gets a new root, so
    /// a record whose root no longer matches is stale and never attaches its
    /// id to the new bucket.
    pub bucket_root: String,
    /// Set when `status` became `Deleting`; zero otherwise.
    #[serde(default)]
    pub deleting_since_ms: u64,
    /// Last progress mark of the running force-delete sweep, see
    /// `SWEEP_LEASE_MS`.
    #[serde(default)]
    pub sweep_heartbeat_ms: u64,
    /// Identity of the lease holder, minted when the lease is taken and
    /// kept across its renewals. A lease write whose response was lost is
    /// recognised as landed by this token, not by the record's name.
    #[serde(default)]
    pub sweep_token: String,
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl Drive {
    pub fn new(
        name: &str,
        display_name: &str,
        labels: BTreeMap<String, String>,
        created_by: &str,
        bucket_root: &str,
    ) -> Self {
        Self {
            id: Uuid::now_v7().to_string(),
            name: name.to_string(),
            display_name: display_name.to_string(),
            labels,
            status: DriveStatus::Ready,
            created_at_ms: unix_ms(),
            created_by: created_by.to_string(),
            bucket_root: bucket_root.to_string(),
            deleting_since_ms: 0,
            sweep_heartbeat_ms: 0,
            sweep_token: String::new(),
        }
    }

    /// True while a sweeper is making progress; a stale one may be resumed.
    pub fn sweep_alive(&self, now_ms: u64) -> bool {
        self.status == DriveStatus::Deleting
            && now_ms.saturating_sub(self.sweep_heartbeat_ms) < SWEEP_LEASE_MS
    }

    pub fn kv_key(name: &str) -> String {
        format!("{DRIVE_PREFIX}{name}")
    }

    /// True when the caller-supplied metadata matches, the idempotent
    /// create case.
    pub fn same_metadata(&self, display_name: &str, labels: &BTreeMap<String, String>) -> bool {
        self.display_name == display_name && &self.labels == labels
    }
}

// Check if a bucket name is valid.
//
// The requirements are listed here:
// <https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html>
pub fn is_valid_bucket_name(n: &str) -> bool {
    // Bucket names must be between 3 and 63 characters
    n.len() >= 3 && n.len() <= 63
    // Bucket names must be composed of lowercase letters, numbers,
    // dashes and dots
    && n.chars().all(|c| matches!(c, '.' | '-' | 'a'..='z' | '0'..='9'))
    // Bucket names must start and end with a letter or a number
    && !n.starts_with(&['-', '.'][..])
    && !n.ends_with(&['-', '.'][..])
    // Bucket names must not be formatted as an IP address
    && n.parse::<std::net::IpAddr>().is_err()
    // Bucket names must not start with "xn--"
    && !n.starts_with("xn--")
    && !n.contains(".xn--")
    // Bucket names must not end with "-s3alias"
    && !n.ends_with("-s3alias")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_name_rules() {
        for ok in ["abc", "my-bucket.v2", "a1b", "task-4711"] {
            assert!(is_valid_bucket_name(ok), "{ok} should be valid");
        }
        for bad in [
            "ab",
            "-abc",
            "abc.",
            "ABC",
            "a_b",
            "192.168.0.1",
            "xn--abc",
            "abc-s3alias",
            &"a".repeat(64),
        ] {
            assert!(!is_valid_bucket_name(bad), "{bad} should be invalid");
        }
    }

    #[test]
    fn record_round_trip_with_defaults() {
        let drive = Drive::new("d1", "", BTreeMap::new(), "key1", "root-1");
        let json = serde_json::to_string(&drive).expect("serialise");
        let back: Drive = serde_json::from_str(&json).expect("deserialise");
        assert_eq!(back, drive, "round trip");

        // A record written without the defaulted fields still loads.
        let minimal = r#"{"id":"x","name":"d1","status":"ready","created_at_ms":1,"created_by":"k","bucket_root":"r"}"#;
        let back: Drive = serde_json::from_str(minimal).expect("minimal record");
        assert_eq!(back.status, DriveStatus::Ready, "status");
        assert!(back.labels.is_empty(), "labels default");
        assert_eq!(back.deleting_since_ms, 0, "deleting_since default");
        assert!(!back.sweep_alive(1), "no sweep on a ready record");
        let mut deleting = back.clone();
        deleting.status = DriveStatus::Deleting;
        deleting.sweep_heartbeat_ms = 1_000;
        assert!(
            deleting.sweep_alive(1_000 + SWEEP_LEASE_MS - 1),
            "fresh heartbeat"
        );
        assert!(
            !deleting.sweep_alive(1_000 + SWEEP_LEASE_MS),
            "stale heartbeat"
        );
        assert_eq!(Drive::kv_key("d1"), "drive:d1", "kv key");
    }
}
