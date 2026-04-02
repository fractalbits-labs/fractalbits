use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataVolumeReport {
    pub volume_id: u16,
    pub scanned_blobs: u64,
    pub repair_candidates: u64,
    pub repaired_blobs: u64,
    pub failed_repairs: u64,
    pub unrecoverable_blobs: u64,
    pub degraded: bool,
    pub failed_nodes: Vec<String>,
    pub repair_failed_nodes: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataRepairReport {
    pub scanned_volumes: u64,
    pub degraded_volumes: u64,
    pub failed_volumes: u64,
    pub scanned_blobs: u64,
    pub repair_candidates: u64,
    pub repaired_blobs: u64,
    pub failed_repairs: u64,
    pub unrecoverable_blobs: u64,
    pub volume_reports: Vec<DataVolumeReport>,
}

impl DataRepairReport {
    pub fn has_failures(&self) -> bool {
        self.failed_volumes > 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaVolumeReport {
    pub volume_id: u16,
    pub scanned_blobs: u64,
    pub repair_candidates: u64,
    pub repaired_blobs: u64,
    pub failed_repairs: u64,
    pub anomalies: u64,
    pub degraded: bool,
    pub failed_nodes: Vec<String>,
    pub repair_failed_nodes: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaRepairReport {
    pub scanned_volumes: u64,
    pub degraded_volumes: u64,
    pub failed_volumes: u64,
    pub scanned_blobs: u64,
    pub repair_candidates: u64,
    pub repaired_blobs: u64,
    pub failed_repairs: u64,
    pub anomalies: u64,
    pub volume_reports: Vec<MetaVolumeReport>,
}

impl MetaRepairReport {
    pub fn has_failures(&self) -> bool {
        self.failed_volumes > 0
    }
}
