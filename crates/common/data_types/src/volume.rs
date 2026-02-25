#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataVgInfo {
    pub volumes: Vec<Volume>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Volume {
    pub volume_id: u16,
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
