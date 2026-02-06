use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObserverState {
    /// Single NSS instance running solo (no HA, no mirrord)
    Solo,
    /// Normal HA: NSS active, Mirrord standby
    ActiveStandby,
    /// Failover mode: NSS solo, Mirrord degraded (recovering)
    SoloDegraded,
    /// Recovery mode: NSS active, Mirrord degraded/syncing
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
    Mirrord,
    Noop,
}

impl fmt::Display for ServiceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceType::Nss => write!(f, "nss"),
            ServiceType::Mirrord => write!(f, "mirrord"),
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
            MachineState::new("nss-A".to_string(), ServiceType::Nss, "active".to_string());
        let json = serde_json::to_string(&machine).unwrap();
        let parsed: MachineState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.machine_id, "nss-A");
        assert_eq!(parsed.expected_role, "active");
    }

    #[test]
    fn test_persistent_state_serialization() {
        let state = ObserverPersistentState::new(
            ObserverState::ActiveStandby,
            MachineState::new("nss-A".to_string(), ServiceType::Nss, "active".to_string()),
            MachineState::new(
                "nss-B".to_string(),
                ServiceType::Mirrord,
                "standby".to_string(),
            ),
        );
        let json = serde_json::to_string(&state).unwrap();
        let parsed: ObserverPersistentState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.observer_state, ObserverState::ActiveStandby);
        assert_eq!(parsed.nss_machine.machine_id, "nss-A");
        assert_eq!(parsed.standby_machine.machine_id, "nss-B");
        assert_eq!(parsed.nss_node_map.get("nss-A"), Some(&1));
        assert_eq!(parsed.nss_node_map.get("nss-B"), Some(&2));
        assert_eq!(parsed.next_nss_node_id, 3);
    }

    #[test]
    fn test_nss_node_map_solo_mode() {
        let state = ObserverPersistentState::new(
            ObserverState::Solo,
            MachineState::new("nss-A".to_string(), ServiceType::Nss, "solo".to_string()),
            MachineState::new(String::new(), ServiceType::Mirrord, String::new()),
        );
        assert_eq!(state.nss_node_map.len(), 1);
        assert_eq!(state.nss_node_map.get("nss-A"), Some(&1));
        assert_eq!(state.next_nss_node_id, 2);
    }

    #[test]
    fn test_ensure_nss_node_id() {
        let mut state = ObserverPersistentState::new(
            ObserverState::ActiveStandby,
            MachineState::new("nss-A".to_string(), ServiceType::Nss, "active".to_string()),
            MachineState::new(
                "nss-B".to_string(),
                ServiceType::Mirrord,
                "standby".to_string(),
            ),
        );
        assert_eq!(state.ensure_nss_node_id("nss-A"), 1);
        assert_eq!(state.ensure_nss_node_id("nss-B"), 2);
        assert_eq!(state.ensure_nss_node_id("nss-C"), 3);
        assert_eq!(state.next_nss_node_id, 4);
        // Calling again returns same ID
        assert_eq!(state.ensure_nss_node_id("nss-C"), 3);
        assert_eq!(state.next_nss_node_id, 4);
    }

    #[test]
    fn test_backward_compat_missing_nss_node_map() {
        // Old JSON without nss_node_map should deserialize with defaults
        let json = r#"{"observer_state":"active_standby","nss_machine":{"machine_id":"nss-A","running_service":"nss","expected_role":"active"},"standby_machine":{"machine_id":"nss-B","running_service":"mirrord","expected_role":"standby"},"last_updated":0.0,"version":1}"#;
        let parsed: ObserverPersistentState = serde_json::from_str(json).unwrap();
        assert!(parsed.nss_node_map.is_empty());
        assert_eq!(parsed.next_nss_node_id, 0);
    }

    #[test]
    fn test_noop_service_type_serialization() {
        let machine = MachineState::new(
            "nss-B".to_string(),
            ServiceType::Noop,
            "standby".to_string(),
        );
        let json = serde_json::to_string(&machine).unwrap();
        assert!(json.contains("\"noop\""));
        let parsed: MachineState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.running_service, ServiceType::Noop);
    }

    #[test]
    fn test_backward_compat_mirrord_machine_field() {
        // Verify that JSON with old "mirrord_machine" field name can still be deserialized
        // This is NOT supported - we use a clean rename. This test documents the break.
        let json = r#"{"observer_state":"active_standby","nss_machine":{"machine_id":"nss-A","running_service":"nss","expected_role":"active"},"mirrord_machine":{"machine_id":"nss-B","running_service":"mirrord","expected_role":"standby"},"last_updated":0.0,"version":0}"#;
        let result = serde_json::from_str::<ObserverPersistentState>(json);
        assert!(
            result.is_err(),
            "Old mirrord_machine field should not deserialize - clean rename"
        );
    }
}
