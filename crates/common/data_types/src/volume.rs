#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataVgInfo {
    pub volumes: Vec<DataVolume>,
    pub quorum: Option<QuorumConfig>,
    #[serde(default)]
    pub ec_volumes: Vec<EcVolume>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EcVolume {
    pub volume_id: u16,
    pub data_shards: u32,
    pub parity_shards: u32,
    pub bss_nodes: Vec<BssNode>,
}

impl EcVolume {
    pub const EC_VOLUME_ID_BASE: u16 = 0x8000;

    pub fn is_ec_volume_id(volume_id: u16) -> bool {
        volume_id >= Self::EC_VOLUME_ID_BASE && volume_id != u16::MAX
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataVolume {
    pub volume_id: u16,
    pub bss_nodes: Vec<BssNode>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BssNode {
    pub node_id: String,
    pub ip: String,
    pub port: u16,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QuorumConfig {
    pub n: u32,
    pub r: u32,
    pub w: u32,
}
