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

    /// `true` when this layout should appear as an entry in an
    /// `S3 ListObjectsV2` reply. Mpu(Uploading) objects are not
    /// listable (they're still being assembled). Directory inodes
    /// and Special inodes (fifo / block / char / socket) are
    /// filesystem-only concepts and never surface through the
    /// S3 listing API; the listing path emits directories as
    /// `CommonPrefixes` and skips Special entries entirely.
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

    /// `true` when this layout should be exposed by the filesystem
    /// (FUSE / NFS) lookup and readdir paths. Includes everything
    /// `is_listable()` does plus `Special` (fifo / block / char /
    /// socket): the S3 listing API hides those, but the filesystem
    /// must expose them or `chmod` / `unlink` against a freshly
    /// created fifo returns ENOENT once the dentry's TTL expires
    /// and the kernel re-issues FUSE_LOOKUP.
    #[inline]
    pub fn is_fs_visible(&self) -> bool {
        matches!(
            &self.state,
            ObjectState::Normal(_)
                | ObjectState::Mpu(MpuState::Completed(_))
                | ObjectState::Symlink(_)
                | ObjectState::Indirect(_)
                | ObjectState::Special(_)
        )
    }

    /// `true` when this layout describes a symbolic link.
    #[inline]
    pub fn is_symlink(&self) -> bool {
        matches!(&self.state, ObjectState::Symlink(_))
    }

    /// `true` when this layout describes a fifo / block / char /
    /// unix-socket inode.
    #[inline]
    pub fn is_special(&self) -> bool {
        matches!(&self.state, ObjectState::Special(_))
    }

    /// Borrow the Special body when this layout is a
    /// fifo / device / socket inode.
    #[inline]
    pub fn special(&self) -> Option<&SpecialData> {
        match &self.state {
            ObjectState::Special(data) => Some(data),
            _ => None,
        }
    }

    /// `true` when this layout describes a directory inode.
    #[inline]
    pub fn is_directory(&self) -> bool {
        matches!(&self.state, ObjectState::Directory(_))
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
            // POSIX: special files (fifo / device / socket) and
            // directory inodes report size = 0 from stat(2). The
            // FUSE / NFS getattr path discards this and reports its
            // own value anyway, but a callable accessor is more
            // ergonomic than the InvalidState the catch-all would
            // otherwise return.
            ObjectState::Special(_) | ObjectState::Directory(_) => Ok(0),
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

    /// Number of data blocks for non-symlink objects. Symlinks and
    /// directories have no BSS blob and report 0; Indirect entries
    /// have no inline state.
    #[inline]
    pub fn num_blocks(&self) -> Result<usize, ObjectLayoutError> {
        match self.state {
            ObjectState::Symlink(_) | ObjectState::Directory(_) => Ok(0),
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
    /// FIFO / block / char / unix-socket inode. Stat returns the
    /// matching `S_IFMT` bit and (for block / char) the persisted
    /// `rdev`; the kernel handles all I/O against the open fd
    /// itself (pipes, device drivers, AF_UNIX), so fs_server only
    /// has to round-trip the metadata.
    Special(SpecialData),
    /// Directory marker. Persisted at the trailing-`/` NSS key as a
    /// "this prefix is a directory" sentinel that
    /// `vfs_lookup`'s list-fallback path checks for. Does NOT carry
    /// a `blob_guid`, size, etag, headers, or checksum (none of
    /// those are meaningful for a directory inode) but does carry
    /// `PosixAttrs` so chmod/chown/utime against directories survive
    /// the close-time round-trip the same way file flushes do.
    Directory(DirectoryData),
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

#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone, Default)]
pub struct ObjectCoreMetaData {
    pub size: u64,
    pub etag: String,
    pub headers: HeaderList,
    pub checksum: Option<ChecksumValue>,
    /// POSIX attrs the FUSE / NFS layer reads back via `stat(2)`, stored
    /// behind a relative pointer so a pure-S3 object (which never sets
    /// them) costs only the `None` niche (~5 archived bytes) instead of
    /// the full inline `PosixAttrs` (28 bytes). rkyv is a zero-copy
    /// format: an inline `Option<PosixAttrs>` would still reserve the
    /// 28 bytes, so the `Box` is what moves the payload out-of-line and
    /// makes the absent case cheap. `None` means "uninitialised": the
    /// runtime falls back to the mount-default mode/uid/gid and
    /// synthesises mtime/ctime from the layout `timestamp`. Only an
    /// explicit chmod/chown/utimensat (or an FS-created inode) stores
    /// `Some`.
    pub posix: Option<Box<PosixAttrs>>,
}

/// Persisted POSIX attrs for a regular file, directory, or symlink
/// inode. Lives inside `ObjectCoreMetaData` so every layout shape
/// (Normal, Mpu Completed, Symlink) inherits them. Times are stored
/// in nanoseconds since the Unix epoch.
///
/// `atime` is intentionally absent: fs_server never updates it on
/// `read(2)` (the equivalent of mounting noatime), so persisting a
/// per-inode `atime_ns` adds 8 bytes per NSS layout for a value
/// that only ever advances via `utimensat`. The stat-time atime is
/// synthesised from `mtime_ns` instead, a noatime/strictatime
/// fallback that keeps the contract `atime != 0 after create` that
/// pjdfstest verifies, while saving the per-inode storage cost.
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Eq, Clone, Copy, Default)]
pub struct PosixAttrs {
    /// Permission bits + file-type bits (`S_IFREG`, `S_IFDIR`,
    /// `S_IFLNK`). `0` means "uninitialised".
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime_ns: u64,
    pub ctime_ns: u64,
}

/// Body of an `ObjectState::Symlink` layout. `target` is the raw bytes
/// the kernel returns from `readlink(2)`. `core_meta_data` carries the
/// usual stat fields so the symlink itself answers `lstat` correctly.
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub struct SymlinkData {
    pub target: Vec<u8>,
    pub core_meta_data: ObjectCoreMetaData,
}

/// Body of an `ObjectState::Special` layout. `kind` discriminates
/// fifo / block / char / socket; `rdev` is the kernel's device
/// number (only meaningful for `BlockDevice` and `CharDevice`,
/// fifos and sockets store 0). `core_meta_data.posix` carries the
/// stat fields the FUSE layer surfaces.
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub struct SpecialData {
    pub kind: SpecialKind,
    pub rdev: u32,
    pub core_meta_data: ObjectCoreMetaData,
}

/// Discriminator for the `ObjectState::Special` variant.
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub enum SpecialKind {
    Fifo,
    BlockDevice,
    CharDevice,
    Socket,
}

/// Body of an `ObjectState::Directory` layout. Carries only the POSIX
/// attrs the FUSE layer needs for `stat(2)` on the directory itself.
/// No `blob_guid`, no `size`, no `etag`, no `headers`, no `checksum`:
/// none of those are meaningful for directory inodes, and storing
/// them on every NSS round-trip was burning ~50 wire bytes per dir
/// for no reader.
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Eq, Clone, Copy, Default)]
pub struct DirectoryData {
    pub posix: PosixAttrs,
}

/// Hardlink indirection. A name whose layout has
/// `state == Indirect(entry)` is a redirect: the real layout lives at
/// the `#hardlink/<inode_id>` keyspace entry. The other `ObjectLayout`
/// fields on a redirect (`timestamp`, `version_id`, `block_size`,
/// `blob_version`) are sentinel placeholders; the authoritative values
/// live in the `InodeRecord`.
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub struct IndirectEntry {
    pub inode_id: Uuid,
}

/// The `#hardlink/<inode_id>` keyspace entry that backs every
/// `ObjectState::Indirect` redirect. Holds the real `ObjectLayout`
/// (whose `state` is one of `Normal | Mpu | Symlink | Special`, never
/// `Indirect`), the persisted link count, and an `orphan_since`
/// timestamp set when `nlink` drops to zero while open file handles
/// keep the inode alive.
///
/// `#` is encoding-reserved (every user-facing s3_key starts with
/// `/`), so the keyspace cannot collide with a path-derived name.
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub struct InodeRecord {
    pub layout: ObjectLayout,
    pub nlink: u32,
    /// Wall-clock nanoseconds since the Unix epoch when `nlink` first
    /// reached 0; `None` while the inode still has at least one name.
    /// Reserved for scan/repair orphan finalisation; the inline GC at
    /// `vfs_unlink` covers the common single-instance case.
    pub orphan_since: Option<u64>,
}

impl InodeRecord {
    /// Build the `#hardlink/<inode_id>` NSS key for an inode.
    pub fn key_for(inode_id: Uuid) -> String {
        format!("#hardlink/{inode_id}")
    }
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
            ..Default::default()
        }
    }

    fn normal_layout(core: ObjectCoreMetaData) -> ObjectLayout {
        ObjectLayout {
            timestamp: 1,
            version_id: ObjectLayout::gen_version_id(),
            block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
            blob_version: 1,
            state: ObjectState::Normal(ObjectMetaData {
                blob_guid: DataBlobGuid {
                    blob_id: Uuid::nil(),
                    volume_id: 0,
                },
                core_meta_data: core,
            }),
        }
    }

    #[test]
    fn absent_posix_serializes_smaller_than_present() {
        // Pure-S3 object: posix is None (the common case). An FS inode or
        // a chmod'd object carries Some(Box). The None case must be the
        // cheaper one; that is the whole point of the Box indirection
        // (an inline Option<PosixAttrs> would not save anything in rkyv).
        let mut s3 = core_meta(123);
        s3.posix = None;
        let mut fs = core_meta(123);
        fs.posix = Some(Box::new(PosixAttrs {
            mode: 0o100644,
            uid: 1000,
            gid: 1000,
            mtime_ns: 42,
            ctime_ns: 42,
        }));
        let s3_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&normal_layout(s3)).expect("ser none");
        let fs_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&normal_layout(fs)).expect("ser some");
        assert!(
            s3_bytes.len() < fs_bytes.len(),
            "None-posix layout ({} bytes) must be smaller than Some-posix ({} bytes)",
            s3_bytes.len(),
            fs_bytes.len()
        );
    }

    #[test]
    fn posix_option_box_round_trips() {
        // None.
        let mut none = core_meta(7);
        none.posix = None;
        let l = normal_layout(none);
        let b = rkyv::to_bytes::<rkyv::rancor::Error>(&l).expect("ser");
        let back: ObjectLayout =
            rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&b).expect("de");
        match back.state {
            ObjectState::Normal(d) => assert!(d.core_meta_data.posix.is_none()),
            _ => unreachable!(),
        }
        // Some.
        let p = PosixAttrs {
            mode: 0o100600,
            uid: 5,
            gid: 6,
            mtime_ns: 9,
            ctime_ns: 10,
        };
        let mut some = core_meta(7);
        some.posix = Some(Box::new(p));
        let l = normal_layout(some);
        let b = rkyv::to_bytes::<rkyv::rancor::Error>(&l).expect("ser");
        let back: ObjectLayout =
            rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&b).expect("de");
        match back.state {
            ObjectState::Normal(d) => {
                assert_eq!(d.core_meta_data.posix.as_deref().copied(), Some(p))
            }
            _ => unreachable!(),
        }
    }

    fn special_layout(kind: SpecialKind, rdev: u32) -> ObjectLayout {
        ObjectLayout {
            timestamp: 0,
            version_id: ObjectLayout::gen_version_id(),
            block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
            blob_version: 0,
            state: ObjectState::Special(SpecialData {
                kind,
                rdev,
                core_meta_data: core_meta(0),
            }),
        }
    }

    fn directory_layout(mode: u32) -> ObjectLayout {
        ObjectLayout {
            timestamp: 0,
            version_id: ObjectLayout::gen_version_id(),
            block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
            blob_version: 0,
            state: ObjectState::Directory(DirectoryData {
                posix: PosixAttrs {
                    mode,
                    ..Default::default()
                },
            }),
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

    #[test]
    fn posix_default_mode_is_uninitialised_sentinel() {
        let p = PosixAttrs::default();
        assert_eq!(p.mode, 0, "mode 0 is the uninitialised sentinel");
        assert_eq!((p.uid, p.gid, p.mtime_ns, p.ctime_ns), (0, 0, 0, 0));
    }

    #[test]
    fn special_is_fs_visible_but_not_listable() {
        let layout = special_layout(SpecialKind::Fifo, 0);
        assert!(layout.is_special());
        assert!(layout.special().is_some());
        assert!(
            layout.is_fs_visible(),
            "fifo must be visible to the filesystem"
        );
        assert!(
            !layout.is_listable(),
            "special files never surface through the S3 listing API"
        );
        assert_eq!(layout.size().expect("size"), 0);
        assert_eq!(layout.num_blocks().expect("num_blocks"), 0);
    }

    #[test]
    fn special_round_trips_rdev_through_rkyv() {
        let layout = special_layout(SpecialKind::CharDevice, 0x0103);
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&layout).expect("serialize");
        let parsed: ObjectLayout =
            rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&bytes).expect("deserialize");
        assert_eq!(parsed, layout);
        let data = parsed.special().expect("special");
        assert_eq!(data.kind, SpecialKind::CharDevice);
        assert_eq!(data.rdev, 0x0103);
    }

    #[test]
    fn directory_carries_posix_but_is_not_listable() {
        let layout = directory_layout(0o755);
        assert!(layout.is_directory());
        assert!(
            !layout.is_listable(),
            "directories surface as CommonPrefixes"
        );
        assert!(
            !layout.is_fs_visible(),
            "directories use the list-fallback path"
        );
        assert_eq!(layout.size().expect("size"), 0);
        assert_eq!(layout.num_blocks().expect("num_blocks"), 0);
    }

    #[test]
    fn directory_round_trips_through_rkyv() {
        let layout = directory_layout(0o2775);
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&layout).expect("serialize");
        let parsed: ObjectLayout =
            rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&bytes).expect("deserialize");
        assert_eq!(parsed, layout);
        assert!(parsed.is_directory());
    }
}
