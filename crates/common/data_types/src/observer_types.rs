use crate::VolumeRef;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceStatus {
    Solo,
    Failure,
}

impl fmt::Display for ServiceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceStatus::Solo => write!(f, "solo"),
            ServiceStatus::Failure => write!(f, "failure"),
        }
    }
}

impl ServiceStatus {
    pub fn from_role(role: &str) -> Option<ServiceStatus> {
        match role {
            "solo" => Some(ServiceStatus::Solo),
            "failure" => Some(ServiceStatus::Failure),
            _ => None,
        }
    }
}

/// Per-journal configuration stored as a standalone entry in service discovery.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JournalConfig {
    /// UUID of the journal
    pub journal_uuid: String,
    /// Incremental device ID starting from 1, assigned at journal creation, never changes
    pub device_id: u32,
    /// Journal size in bytes (default: 1GB)
    pub journal_size: u64,
    /// Config version, also used as fence token
    pub version: u64,
    /// Which NSS instance is currently running with this journal
    #[serde(default)]
    pub running_nss_id: Option<String>,
    /// Journal volumes this device writes to, referencing the global pool
    /// `bss-journal-vg-config`. Shared: journals may overlap and use only a
    /// subset. The effective JOURNAL_VG_CONFIG is the pool filtered to these,
    /// with each uuid verified. (Metadata VG stays cluster-global.)
    pub journal_volumes: Vec<VolumeRef>,
}

impl JournalConfig {
    /// Validate the journal config for correctness.
    pub fn validate(&self) -> Result<(), String> {
        if self.device_id == 0 {
            return Err("device_id must be >= 1".to_string());
        }
        if self.journal_volumes.is_empty() {
            return Err("journal config has empty journal_volumes".to_string());
        }
        Ok(())
    }
}

/// Per-NSS instance state, persisted in service discovery under key "nss-store".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NssStore {
    /// Map of instance_id -> per-NSS state
    pub nodes: HashMap<String, NssNodeState>,
}

/// Per-NSS instance persistent state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NssNodeState {
    /// Network address (ip:port) for the NSS service
    pub network_address: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_status_from_role() {
        assert_eq!(ServiceStatus::from_role("solo"), Some(ServiceStatus::Solo));
        assert_eq!(
            ServiceStatus::from_role("failure"),
            Some(ServiceStatus::Failure)
        );
        assert_eq!(ServiceStatus::from_role("invalid"), None);
    }

    #[test]
    fn test_journal_config_serialization() {
        let config = JournalConfig {
            journal_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            device_id: 1,
            journal_size: 1024 * 1024 * 1024,
            version: 1,
            running_nss_id: Some("nss-0".to_string()),
            journal_volumes: vec![VolumeRef {
                volume_id: 1,
                uuid: "u-1".to_string(),
            }],
        };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: JournalConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, config);
    }

    #[test]
    fn test_journal_config_default_running_nss_id() {
        let json = r#"{"journal_uuid":"test-uuid","device_id":1,"journal_size":0,"version":1,"journal_volumes":[{"volume_id":1,"uuid":"u-1"}]}"#;
        let config: JournalConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.running_nss_id, None);
    }

    #[test]
    fn test_journal_config_validate_ok() {
        let config = JournalConfig {
            journal_uuid: "test-uuid".to_string(),
            device_id: 1,
            journal_size: 0,
            version: 1,
            running_nss_id: None,
            journal_volumes: vec![VolumeRef {
                volume_id: 1,
                uuid: "u-1".to_string(),
            }],
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_journal_config_validate_zero_device_id() {
        let config = JournalConfig {
            journal_uuid: "test-uuid".to_string(),
            device_id: 0,
            journal_size: 0,
            version: 1,
            running_nss_id: None,
            journal_volumes: vec![VolumeRef {
                volume_id: 1,
                uuid: "u-1".to_string(),
            }],
        };
        assert!(config.validate().is_err());
    }
}
