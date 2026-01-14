use serde::{Deserialize, Serialize};
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
}

impl fmt::Display for ServiceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceType::Nss => write!(f, "nss"),
            ServiceType::Mirrord => write!(f, "mirrord"),
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
}

impl MachineState {
    pub fn new(machine_id: String, running_service: ServiceType, expected_role: String) -> Self {
        Self {
            machine_id,
            running_service,
            expected_role,
            network_address: None,
        }
    }

    pub fn with_network_address(mut self, address: Option<String>) -> Self {
        self.network_address = address;
        self
    }
}

/// Full observer persistent state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObserverPersistentState {
    pub observer_state: ObserverState,
    pub nss_machine: MachineState,
    pub mirrord_machine: MachineState,
    #[serde(default = "default_timestamp")]
    pub last_updated: f64,
    /// Version counter, incremented on each persist.
    /// Used as fencing token for BSS metadata volume operations.
    #[serde(default)]
    pub version: u64,
}

fn default_timestamp() -> f64 {
    0.0
}

impl ObserverPersistentState {
    pub fn new(
        observer_state: ObserverState,
        nss_machine: MachineState,
        mirrord_machine: MachineState,
    ) -> Self {
        Self {
            observer_state,
            nss_machine,
            mirrord_machine,
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0),
            version: 0,
        }
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
        assert_eq!(parsed.mirrord_machine.machine_id, "nss-B");
    }
}
