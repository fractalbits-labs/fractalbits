use crate::DataBlobGuid;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum ObjectLayoutError {
    #[error("invalid object state")]
    InvalidState,
}

pub type HeaderList = Vec<(String, String)>;

/// Specifies where a blob should be stored/retrieved from
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BlobLocation {
    /// Small blobs stored in DataVgProxy
    DataVgProxy,
    /// Large blobs stored in S3
    S3,
}

#[derive(Archive, Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct ObjectLayout {
    pub timestamp: u64,
    pub version_id: Uuid, // v4
    pub block_size: u32,
    pub state: ObjectState,
}

impl ObjectLayout {
    pub const DEFAULT_BLOCK_SIZE: u32 = 1024 * 1024 - 256;

    pub fn gen_version_id() -> Uuid {
        Uuid::new_v4()
    }

    /// Returns true if the object is in a final state and can be listed/returned.
    /// Objects in Mpu(Uploading) or Mpu(Aborted) states are not listable.
    #[inline]
    pub fn is_listable(&self) -> bool {
        matches!(
            &self.state,
            ObjectState::Normal(_) | ObjectState::Mpu(MpuState::Completed(_))
        )
    }

    #[inline]
    pub fn get_blob_location(&self) -> Result<BlobLocation, ObjectLayoutError> {
        let blob_guid = self.blob_guid()?;
        if blob_guid.volume_id == DataBlobGuid::S3_VOLUME {
            Ok(BlobLocation::S3)
        } else {
            Ok(BlobLocation::DataVgProxy)
        }
    }

    #[inline]
    pub fn blob_guid(&self) -> Result<DataBlobGuid, ObjectLayoutError> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.blob_guid),
            _ => Err(ObjectLayoutError::InvalidState),
        }
    }

    #[inline]
    pub fn size(&self) -> Result<u64, ObjectLayoutError> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.core_meta_data.size),
            ObjectState::Mpu(MpuState::Completed(ref core_meta_data)) => Ok(core_meta_data.size),
            _ => Err(ObjectLayoutError::InvalidState),
        }
    }

    #[inline]
    pub fn etag(&self) -> Result<String, ObjectLayoutError> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.core_meta_data.etag.clone()),
            ObjectState::Mpu(MpuState::Completed(ref core_meta_data)) => {
                Ok(core_meta_data.etag.clone())
            }
            _ => Err(ObjectLayoutError::InvalidState),
        }
    }

    #[inline]
    pub fn num_blocks(&self) -> Result<usize, ObjectLayoutError> {
        Ok(self.size()?.div_ceil(self.block_size as u64) as usize)
    }

    #[inline]
    pub fn checksum(&self) -> Result<Option<ChecksumValue>, ObjectLayoutError> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.core_meta_data.checksum),
            ObjectState::Mpu(MpuState::Completed(ref core_meta_data)) => {
                Ok(core_meta_data.checksum)
            }
            _ => Err(ObjectLayoutError::InvalidState),
        }
    }

    #[inline]
    pub fn headers(&self) -> Result<&HeaderList, ObjectLayoutError> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(&data.core_meta_data.headers),
            ObjectState::Mpu(MpuState::Completed(ref core_meta_data)) => {
                Ok(&core_meta_data.headers)
            }
            _ => Err(ObjectLayoutError::InvalidState),
        }
    }
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub enum ObjectState {
    Normal(ObjectMetaData),
    Mpu(MpuState),
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub enum MpuState {
    Uploading,
    Aborted,
    Completed(ObjectCoreMetaData),
}

/// Data stored in normal object or mpu parts
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub struct ObjectMetaData {
    pub blob_guid: DataBlobGuid,
    pub core_meta_data: ObjectCoreMetaData,
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub struct ObjectCoreMetaData {
    pub size: u64,
    pub etag: String,
    pub headers: HeaderList,
    pub checksum: Option<ChecksumValue>,
}

#[derive(
    PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, serde::Serialize, serde::Deserialize,
)]
pub enum ChecksumAlgorithm {
    Crc32,
    Crc32c,
    Crc64Nvme,
    Sha1,
    Sha256,
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Copy,
    Debug,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum ChecksumValue {
    Crc32(#[serde(with = "serde_bytes")] [u8; 4]),
    Crc32c(#[serde(with = "serde_bytes")] [u8; 4]),
    Crc64Nvme(#[serde(with = "serde_bytes")] [u8; 8]),
    Sha1(#[serde(with = "serde_bytes")] [u8; 20]),
    Sha256(#[serde(with = "serde_bytes")] [u8; 32]),
}

impl ChecksumValue {
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        match self {
            ChecksumValue::Crc32(_) => ChecksumAlgorithm::Crc32,
            ChecksumValue::Crc32c(_) => ChecksumAlgorithm::Crc32c,
            ChecksumValue::Crc64Nvme(_) => ChecksumAlgorithm::Crc64Nvme,
            ChecksumValue::Sha1(_) => ChecksumAlgorithm::Sha1,
            ChecksumValue::Sha256(_) => ChecksumAlgorithm::Sha256,
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        match self {
            ChecksumValue::Crc32(bytes) => bytes,
            ChecksumValue::Crc32c(bytes) => bytes,
            ChecksumValue::Crc64Nvme(bytes) => bytes,
            ChecksumValue::Sha1(bytes) => bytes,
            ChecksumValue::Sha256(bytes) => bytes,
        }
    }
}
