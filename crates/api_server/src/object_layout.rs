use crate::blob_storage::BlobLocation;
use crate::handler::common::{checksum::ChecksumValue, s3_error::S3Error};
use data_types::DataBlobGuid;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

pub type HeaderList = Vec<(String, String)>;

#[derive(Archive, Deserialize, Serialize, PartialEq, Debug)]
pub struct ObjectLayout {
    pub timestamp: u64,
    pub version_id: Uuid, // v4
    pub block_size: u32,
    pub state: ObjectState,
}

pub fn gen_version_id() -> Uuid {
    Uuid::new_v4()
}

impl ObjectLayout {
    pub const DEFAULT_BLOCK_SIZE: u32 = 1024 * 1024 - 256;

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
    pub fn get_blob_location(&self) -> Result<BlobLocation, S3Error> {
        let blob_guid = self.blob_guid()?;
        if blob_guid.volume_id == DataBlobGuid::S3_VOLUME {
            Ok(BlobLocation::S3)
        } else {
            Ok(BlobLocation::DataVgProxy)
        }
    }

    #[inline]
    pub fn blob_guid(&self) -> Result<DataBlobGuid, S3Error> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.blob_guid),
            _ => Err(S3Error::InvalidObjectState),
        }
    }

    #[inline]
    pub fn size(&self) -> Result<u64, S3Error> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.core_meta_data.size),
            ObjectState::Mpu(MpuState::Completed(ref core_meta_data)) => Ok(core_meta_data.size),
            _ => Err(S3Error::InvalidObjectState),
        }
    }

    #[inline]
    pub fn etag(&self) -> Result<String, S3Error> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.core_meta_data.etag.clone()),
            ObjectState::Mpu(MpuState::Completed(ref core_meta_data)) => {
                Ok(core_meta_data.etag.clone())
            }
            _ => Err(S3Error::InvalidObjectState),
        }
    }

    #[inline]
    pub fn num_blocks(&self) -> Result<usize, S3Error> {
        Ok(self.size()?.div_ceil(self.block_size as u64) as usize)
    }

    #[inline]
    pub fn checksum(&self) -> Result<Option<ChecksumValue>, S3Error> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.core_meta_data.checksum),
            ObjectState::Mpu(MpuState::Completed(ref core_meta_data)) => {
                Ok(core_meta_data.checksum)
            }
            _ => Err(S3Error::InvalidObjectState),
        }
    }

    #[inline]
    pub fn headers(&self) -> Result<&HeaderList, S3Error> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(&data.core_meta_data.headers),
            ObjectState::Mpu(MpuState::Completed(ref core_meta_data)) => {
                Ok(&core_meta_data.headers)
            }
            _ => Err(S3Error::InvalidObjectState),
        }
    }
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq)]
pub enum ObjectState {
    Normal(ObjectMetaData),
    Mpu(MpuState),
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq)]
pub enum MpuState {
    Uploading,
    Aborted,
    Completed(ObjectCoreMetaData),
}

/// Data stored in normal object or mpu parts
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq)]
pub struct ObjectMetaData {
    pub blob_guid: DataBlobGuid,
    pub core_meta_data: ObjectCoreMetaData,
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq)]
pub struct ObjectCoreMetaData {
    pub size: u64,
    pub etag: String,
    pub headers: HeaderList,
    pub checksum: Option<ChecksumValue>,
}
