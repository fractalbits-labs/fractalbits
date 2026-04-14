use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverState {
    /// Single NSS instance running solo (no HA)
    Solo,
    /// Normal HA: NSS active, standby idle
    ActiveStandby,
    /// Failover mode: NSS solo, standby degraded (recovering)
    SoloDegraded,
    /// Recovery mode: NSS active, standby degraded/syncing
    ActiveDegraded,
}

impl fmt::Display for ObserverState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObserverState::Solo => write!(f, "solo"),
            ObserverState::ActiveStandby => write!(f, "active_standby"),
            ObserverState::SoloDegraded => write!(f, "solo_degraded"),
            ObserverState::ActiveDegraded => write!(f, "active_degraded"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceType {
    Nss,
    Noop,
}

impl fmt::Display for ServiceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceType::Nss => write!(f, "nss"),
            ServiceType::Noop => write!(f, "noop"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceStatus {
    Active,
    Solo,
    Standby,
    Degraded,
    Failure,
}

impl fmt::Display for ServiceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceStatus::Active => write!(f, "active"),
            ServiceStatus::Solo => write!(f, "solo"),
            ServiceStatus::Standby => write!(f, "standby"),
            ServiceStatus::Degraded => write!(f, "degraded"),
            ServiceStatus::Failure => write!(f, "failure"),
        }
    }
}

impl ServiceStatus {
    pub fn from_role(role: &str) -> Option<ServiceStatus> {
        match role {
            "active" => Some(ServiceStatus::Active),
            "solo" => Some(ServiceStatus::Solo),
            "standby" => Some(ServiceStatus::Standby),
            "degraded" => Some(ServiceStatus::Degraded),
            "failure" => Some(ServiceStatus::Failure),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResponse {
    pub status: ServiceStatus,
    pub service_type: ServiceType,
    #[serde(default)]
    pub uptime_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MachineState {
    pub machine_id: String,
    pub running_service: ServiceType,
    pub expected_role: String,
    #[serde(default)]
    pub network_address: Option<String>,
    #[serde(default)]
    pub journal_uuid: Option<String>,
}

impl MachineState {
    pub fn new(machine_id: String, running_service: ServiceType, expected_role: String) -> Self {
        Self {
            machine_id,
            running_service,
            expected_role,
            network_address: None,
            journal_uuid: None,
        }
    }

    pub fn with_network_address(mut self, address: Option<String>) -> Self {
        self.network_address = address;
        self
    }

    pub fn with_journal_uuid(mut self, journal_uuid: Option<String>) -> Self {
        self.journal_uuid = journal_uuid;
        self
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
    /// Journal volume IDs this journal can write to (no dups, must be valid)
    pub journal_volume_ids: Vec<u16>,
    /// Metadata volume IDs for NSS running with this journal (no dups, must be valid)
    pub metadata_volume_ids: Vec<u16>,
    /// Which NSS instance is currently running with this journal
    #[serde(default)]
    pub running_nss_id: Option<String>,
}

impl JournalConfig {
    /// Validate the journal config for correctness.
    pub fn validate(&self) -> Result<(), String> {
        if self.device_id == 0 {
            return Err("device_id must be >= 1".to_string());
        }

        let mut seen = HashSet::new();
        for &id in &self.journal_volume_ids {
            if !seen.insert(id) {
                return Err(format!("duplicate journal_volume_id: {id}"));
            }
        }

        let mut seen = HashSet::new();
        for &id in &self.metadata_volume_ids {
            if !seen.insert(id) {
                return Err(format!("duplicate metadata_volume_id: {id}"));
            }
        }

        Ok(())
    }
}

/// Full observer persistent state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObserverPersistentState {
    pub observer_state: ObserverState,
    pub nss_machine: MachineState,
    pub standby_machine: MachineState,
    #[serde(default = "default_timestamp")]
    pub last_updated: f64,
    /// Version counter, incremented on each persist.
    /// Used as fencing token for BSS metadata volume operations.
    #[serde(default)]
    pub version: u64,
    /// Mapping from instance_id to NVMe reservation node sequence ID.
    /// Monotonically increasing, never reused.
    #[serde(default)]
    pub nss_node_map: HashMap<String, u64>,
    /// Next node sequence ID to assign.
    #[serde(default)]
    pub next_nss_node_id: u64,
}

fn default_timestamp() -> f64 {
    0.0
}

impl ObserverPersistentState {
    pub fn new(
        observer_state: ObserverState,
        nss_machine: MachineState,
        standby_machine: MachineState,
    ) -> Self {
        let mut nss_node_map = HashMap::new();
        let mut next_id = 1u64;
        if !nss_machine.machine_id.is_empty() {
            nss_node_map.insert(nss_machine.machine_id.clone(), next_id);
            next_id += 1;
        }
        if !standby_machine.machine_id.is_empty() {
            nss_node_map.insert(standby_machine.machine_id.clone(), next_id);
            next_id += 1;
        }
        Self {
            observer_state,
            nss_machine,
            standby_machine,
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0),
            version: 0,
            nss_node_map,
            next_nss_node_id: next_id,
        }
    }

    /// Get or assign a node sequence ID for the given instance.
    /// Assigns the next available ID if not present.
    pub fn ensure_nss_node_id(&mut self, instance_id: &str) -> u64 {
        if let Some(&id) = self.nss_node_map.get(instance_id) {
            return id;
        }
        let id = self.next_nss_node_id;
        self.nss_node_map.insert(instance_id.to_string(), id);
        self.next_nss_node_id += 1;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observer_state_serialization() {
        let state = ObserverState::ActiveStandby;
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, "\"active_standby\"");

        let parsed: ObserverState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, ObserverState::ActiveStandby);
    }

    #[test]
    fn test_service_status_from_role() {
        assert_eq!(
            ServiceStatus::from_role("active"),
            Some(ServiceStatus::Active)
        );
        assert_eq!(
            ServiceStatus::from_role("standby"),
            Some(ServiceStatus::Standby)
        );
        assert_eq!(ServiceStatus::from_role("invalid"), None);
    }

    #[test]
    fn test_machine_state_serialization() {
        let machine =
            MachineState::new("nss-0".to_string(), ServiceType::Nss, "active".to_string());
        let json = serde_json::to_string(&machine).unwrap();
        let parsed: MachineState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.machine_id, "nss-0");
        assert_eq!(parsed.expected_role, "active");
    }

    #[test]
    fn test_persistent_state_serialization() {
        let state = ObserverPersistentState::new(
            ObserverState::ActiveStandby,
            MachineState::new("nss-0".to_string(), ServiceType::Nss, "active".to_string()),
            MachineState::new(
                "nss-1".to_string(),
                ServiceType::Noop,
                "standby".to_string(),
            ),
        );
        let json = serde_json::to_string(&state).unwrap();
        let parsed: ObserverPersistentState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.observer_state, ObserverState::ActiveStandby);
        assert_eq!(parsed.nss_machine.machine_id, "nss-0");
        assert_eq!(parsed.standby_machine.machine_id, "nss-1");
        assert_eq!(parsed.nss_node_map.get("nss-0"), Some(&1));
        assert_eq!(parsed.nss_node_map.get("nss-1"), Some(&2));
        assert_eq!(parsed.next_nss_node_id, 3);
    }

    #[test]
    fn test_nss_node_map_solo_mode() {
        let state = ObserverPersistentState::new(
            ObserverState::Solo,
            MachineState::new("nss-0".to_string(), ServiceType::Nss, "solo".to_string()),
            MachineState::new(String::new(), ServiceType::Noop, String::new()),
        );
        assert_eq!(state.nss_node_map.len(), 1);
        assert_eq!(state.nss_node_map.get("nss-0"), Some(&1));
        assert_eq!(state.next_nss_node_id, 2);
    }

    #[test]
    fn test_ensure_nss_node_id() {
        let mut state = ObserverPersistentState::new(
            ObserverState::ActiveStandby,
            MachineState::new("nss-0".to_string(), ServiceType::Nss, "active".to_string()),
            MachineState::new(
                "nss-1".to_string(),
                ServiceType::Noop,
                "standby".to_string(),
            ),
        );
        assert_eq!(state.ensure_nss_node_id("nss-0"), 1);
        assert_eq!(state.ensure_nss_node_id("nss-1"), 2);
        assert_eq!(state.ensure_nss_node_id("nss-2"), 3);
        assert_eq!(state.next_nss_node_id, 4);
        // Calling again returns same ID
        assert_eq!(state.ensure_nss_node_id("nss-2"), 3);
        assert_eq!(state.next_nss_node_id, 4);
    }

    #[test]
    fn test_noop_service_type_serialization() {
        let machine = MachineState::new(
            "nss-1".to_string(),
            ServiceType::Noop,
            "standby".to_string(),
        );
        let json = serde_json::to_string(&machine).unwrap();
        assert!(json.contains("\"noop\""));
        let parsed: MachineState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.running_service, ServiceType::Noop);
    }

    #[test]
    fn test_journal_config_serialization() {
        let config = JournalConfig {
            journal_uuid: "550e8400-e29b-41d4-a716-446655440000".to_string(),
            device_id: 1,
            journal_size: 1024 * 1024 * 1024,
            version: 1,
            journal_volume_ids: vec![1, 2, 3],
            metadata_volume_ids: vec![10, 20],
            running_nss_id: Some("nss-0".to_string()),
        };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: JournalConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, config);
    }

    #[test]
    fn test_journal_config_default_running_nss_id() {
        let json = r#"{"journal_uuid":"test-uuid","device_id":1,"journal_size":0,"version":1,"journal_volume_ids":[],"metadata_volume_ids":[]}"#;
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
            journal_volume_ids: vec![1, 2, 3],
            metadata_volume_ids: vec![10, 20],
            running_nss_id: None,
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
            journal_volume_ids: vec![],
            metadata_volume_ids: vec![],
            running_nss_id: None,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_journal_config_validate_dup_journal_volume_ids() {
        let config = JournalConfig {
            journal_uuid: "test-uuid".to_string(),
            device_id: 1,
            journal_size: 0,
            version: 1,
            journal_volume_ids: vec![1, 2, 1],
            metadata_volume_ids: vec![],
            running_nss_id: None,
        };
        let err = config.validate().expect_err("should fail on dup");
        assert!(err.contains("duplicate journal_volume_id"));
    }

    #[test]
    fn test_journal_config_validate_dup_metadata_volume_ids() {
        let config = JournalConfig {
            journal_uuid: "test-uuid".to_string(),
            device_id: 1,
            journal_size: 0,
            version: 1,
            journal_volume_ids: vec![],
            metadata_volume_ids: vec![5, 5],
            running_nss_id: None,
        };
        let err = config.validate().expect_err("should fail on dup");
        assert!(err.contains("duplicate metadata_volume_id"));
    }
}
