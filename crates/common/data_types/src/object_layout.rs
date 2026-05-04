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
    /// Monotonic version for in-place block override (V1 sparse + override).
    /// Incremented by `PutInodeCas` on every flush that has pending work.
    /// `0` is reserved for legacy / pre-V1 layouts; new flushes start at `1`.
    pub blob_version: u64,
    pub state: ObjectState,
}

impl ObjectLayout {
    pub const DEFAULT_BLOCK_SIZE: u32 = 128 * 1024;

    pub fn gen_version_id() -> Uuid {
        Uuid::new_v4()
    }

    /// Returns true if the object is in a final state and can be listed/returned.
    /// Objects in Mpu(Uploading) state are not listable.
    #[inline]
    pub fn is_listable(&self) -> bool {
        matches!(
            &self.state,
            ObjectState::Normal(_)
                | ObjectState::Mpu(MpuState::Completed(_))
                | ObjectState::Symlink(_)
                | ObjectState::Indirect(_)
        )
    }

    /// `true` when this layout describes a symbolic link.
    #[inline]
    pub fn is_symlink(&self) -> bool {
        matches!(&self.state, ObjectState::Symlink(_))
    }

    /// Borrow the symlink target bytes when this layout is a symlink.
    #[inline]
    pub fn symlink_target(&self) -> Option<&[u8]> {
        match &self.state {
            ObjectState::Symlink(data) => Some(&data.target),
            _ => None,
        }
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
            // POSIX: a symlink's stat size is the length of its target.
            ObjectState::Symlink(ref data) => Ok(data.target.len() as u64),
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
            ObjectState::Symlink(ref data) => Ok(data.core_meta_data.etag.clone()),
            _ => Err(ObjectLayoutError::InvalidState),
        }
    }

    /// Number of data blocks for non-symlink objects. Symlinks have no
    /// BSS blob and report 0; Indirect entries have no inline state.
    #[inline]
    pub fn num_blocks(&self) -> Result<usize, ObjectLayoutError> {
        match self.state {
            ObjectState::Symlink(_) => Ok(0),
            _ => Ok(self.size()?.div_ceil(self.block_size as u64) as usize),
        }
    }

    #[inline]
    pub fn checksum(&self) -> Result<Option<ChecksumValue>, ObjectLayoutError> {
        match self.state {
            ObjectState::Normal(ref data) => Ok(data.core_meta_data.checksum),
            ObjectState::Mpu(MpuState::Completed(ref core_meta_data)) => {
                Ok(core_meta_data.checksum)
            }
            ObjectState::Symlink(ref data) => Ok(data.core_meta_data.checksum),
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
            ObjectState::Symlink(ref data) => Ok(&data.core_meta_data.headers),
            _ => Err(ObjectLayoutError::InvalidState),
        }
    }
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub enum ObjectState {
    Normal(ObjectMetaData),
    Mpu(MpuState),
    /// Symbolic link. The body is the raw target path the kernel
    /// returns from `readlink(2)`. No BSS blob is allocated.
    Symlink(SymlinkData),
    /// Hardlink redirect. The real layout lives at a separate
    /// inode-keyed entry and must be resolved before any read /
    /// write op can run. Schema-only today; no VFS handler creates
    /// or follows these. Reserved as the Phase-1 placeholder for the
    /// lazy-promotion hardlink design.
    Indirect(IndirectEntry),
}

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub enum MpuState {
    Uploading,
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

/// Body of an `ObjectState::Symlink` layout. `target` is the raw bytes
/// the kernel returns from `readlink(2)`. `core_meta_data` carries the
/// usual stat fields so the symlink itself answers `lstat` correctly.
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub struct SymlinkData {
    pub target: Vec<u8>,
    pub core_meta_data: ObjectCoreMetaData,
}

/// Schema-only placeholder for hardlink indirection. A name whose
/// layout has `state == Indirect(entry)` is a redirect: the real
/// layout lives at a separate inode-keyed entry. No VFS handler
/// constructs or follows these today; reserved for a future
/// lazy-promotion hardlink implementation.
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub struct IndirectEntry {
    pub inode_id: Uuid,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn core_meta(size: u64) -> ObjectCoreMetaData {
        ObjectCoreMetaData {
            size,
            etag: "etag".to_string(),
            headers: vec![],
            checksum: None,
        }
    }

    fn symlink_layout(target: &[u8]) -> ObjectLayout {
        ObjectLayout {
            timestamp: 0,
            version_id: ObjectLayout::gen_version_id(),
            block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
            blob_version: 0,
            state: ObjectState::Symlink(SymlinkData {
                target: target.to_vec(),
                core_meta_data: core_meta(target.len() as u64),
            }),
        }
    }

    fn indirect_layout() -> ObjectLayout {
        ObjectLayout {
            timestamp: 0,
            version_id: ObjectLayout::gen_version_id(),
            block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
            blob_version: 0,
            state: ObjectState::Indirect(IndirectEntry {
                inode_id: Uuid::new_v4(),
            }),
        }
    }

    #[test]
    fn symlink_size_matches_target_length() {
        let layout = symlink_layout(b"../etc/hostname");
        assert_eq!(
            layout.size().expect("size"),
            b"../etc/hostname".len() as u64
        );
    }

    #[test]
    fn symlink_is_listable_and_is_symlink() {
        let layout = symlink_layout(b"a/b/c");
        assert!(layout.is_listable(), "symlink must be listable");
        assert!(layout.is_symlink());
        assert_eq!(layout.symlink_target(), Some(b"a/b/c".as_slice()));
    }

    #[test]
    fn symlink_has_no_blob_guid() {
        let layout = symlink_layout(b"target");
        assert!(matches!(
            layout.blob_guid(),
            Err(ObjectLayoutError::InvalidState)
        ));
    }

    #[test]
    fn symlink_reports_zero_blocks() {
        let layout = symlink_layout(b"abc");
        assert_eq!(layout.num_blocks().expect("num_blocks"), 0);
    }

    #[test]
    fn indirect_is_listable_but_not_a_symlink() {
        let layout = indirect_layout();
        assert!(layout.is_listable());
        assert!(!layout.is_symlink());
        assert!(layout.symlink_target().is_none());
    }

    #[test]
    fn indirect_layout_has_no_inline_state() {
        let layout = indirect_layout();
        assert!(matches!(
            layout.size(),
            Err(ObjectLayoutError::InvalidState)
        ));
        assert!(matches!(
            layout.blob_guid(),
            Err(ObjectLayoutError::InvalidState)
        ));
        assert!(matches!(
            layout.etag(),
            Err(ObjectLayoutError::InvalidState)
        ));
        assert!(matches!(
            layout.checksum(),
            Err(ObjectLayoutError::InvalidState)
        ));
    }

    #[test]
    fn symlink_round_trips_through_rkyv() {
        let layout = symlink_layout(b"/tmp/target");
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&layout).expect("serialize");
        let parsed: ObjectLayout =
            rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&bytes).expect("deserialize");
        assert_eq!(parsed, layout);
        assert_eq!(parsed.symlink_target(), Some(b"/tmp/target".as_slice()));
    }
}
