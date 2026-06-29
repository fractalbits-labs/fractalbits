#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataVgInfo {
    pub volumes: Vec<Volume>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Volume {
    pub volume_id: u16,
    /// Stable volume identity. `volume_id` routes; `uuid` is a safety tag to
    /// detect a `volume_id` that no longer refers to the expected volume.
    pub uuid: String,
    pub bss_nodes: Vec<BssNode>,
    pub mode: VolumeMode,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum VolumeMode {
    #[serde(rename = "replicated")]
    Replicated { n: u32, r: u32, w: u32 },
    #[serde(rename = "erasure_coded")]
    ErasureCoded {
        data_shards: u32,
        parity_shards: u32,
    },
}

impl Volume {
    pub const EC_VOLUME_ID_BASE: u16 = 0x8000;

    pub fn is_ec_volume_id(volume_id: u16) -> bool {
        volume_id >= Self::EC_VOLUME_ID_BASE && volume_id != u16::MAX
    }

    pub fn is_ec(&self) -> bool {
        matches!(self.mode, VolumeMode::ErasureCoded { .. })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BssNode {
    pub node_id: String,
    pub ip: String,
    pub port: u16,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetadataVgInfo {
    pub volumes: Vec<MetadataVolume>,
    pub quorum: MetadataQuorum,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetadataVolume {
    pub volume_id: u16,
    /// Stable volume identity (see `Volume::uuid`).
    pub uuid: String,
    pub bss_nodes: Vec<BssNode>,
}

/// Reference to a pool volume: `volume_id` routes, `uuid` is verified at
/// resolution. Pairing them prevents id/uuid misalignment.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VolumeRef {
    pub volume_id: u16,
    pub uuid: String,
}

impl MetadataVgInfo {
    pub fn volume_refs(&self) -> Vec<VolumeRef> {
        self.volumes
            .iter()
            .map(|v| VolumeRef {
                volume_id: v.volume_id,
                uuid: v.uuid.clone(),
            })
            .collect()
    }
}

pub fn pool_volume_refs(pool_json: &str) -> Result<Vec<VolumeRef>, String> {
    let pool: MetadataVgInfo = serde_json::from_str(pool_json)
        .map_err(|e| format!("failed to parse journal VG pool: {e}"))?;
    Ok(pool.volume_refs())
}

#[cfg(test)]
mod tests {
    use super::*;

    const POOL: &str = r#"{
        "volumes":[
            {"volume_id":1,"uuid":"u-1","bss_nodes":[{"node_id":"bss-0","ip":"127.0.0.1","port":8088}]},
            {"volume_id":2,"uuid":"u-2","bss_nodes":[{"node_id":"bss-1","ip":"127.0.0.1","port":8089}]}
        ],
        "quorum":{"n":3,"r":2,"w":2}
    }"#;

    #[test]
    fn pool_volume_refs_extracts_pairs() {
        let refs = pool_volume_refs(POOL).expect("parse");
        assert_eq!(
            refs,
            vec![
                VolumeRef {
                    volume_id: 1,
                    uuid: "u-1".to_string()
                },
                VolumeRef {
                    volume_id: 2,
                    uuid: "u-2".to_string()
                },
            ]
        );
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct MetadataQuorum {
    pub n: u32,
    pub r: u32,
    pub w: u32,
}
