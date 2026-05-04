//! Parent inode metadata stored in BSS at the bare blob key
//! `/d{vol}/{uuid}` (no `-p{N}` block suffix). Written alongside the
//! per-block writes during a flush; readable as a sanity check on the
//! committed `total_size` and `block_count` for a blob.
//!
//! The parent inode key sorts lexicographically before every
//! per-block key for the same blob, so a single-pass scan of
//! `/d{vol}/` always encounters the parent record first for each
//! blob.
//!
//! At the BSS RPC layer, the parent inode is addressed by passing
//! [`PARENT_INODE_BLOCK_NUMBER`] as `block_number` on a regular
//! `PutDataBlob` / `GetDataBlob` / `DeleteDataBlob` request -- the
//! BSS server's key builder maps that sentinel to the no-suffix key
//! form.

/// Sentinel block_number value that selects the parent inode key
/// (`/d{vol}/{uuid}`, no `-pN` suffix) on data-blob RPCs.
///
/// Mirrors the `parent_inode_block_number` constant on the Zig side;
/// keep them in sync.
pub const PARENT_INODE_BLOCK_NUMBER: u32 = u32::MAX;

/// Plain-old-data wire format for the parent inode body.
///
/// `repr(C)` + fixed-size fields + manual `from_le_bytes` (no
/// alignment-aware borrow tricks) so this can be sent over the BSS
/// blob channel as raw bytes without a serialisation framework.
///
/// Layout:
///
///   offset  size  field
///   ------  ----  -----
///   0       8     version           u64 (LE)
///   8       8     total_size        u64 (LE)
///   16      4     block_count       u32 (LE) — ceil(total_size / block_size)
///   20      4     _reserved         u32 (LE) — must be 0
///
/// The serialised body is always exactly 24 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParentInodeMeta {
    /// Blob version this parent record corresponds to. Matches
    /// `ObjectLayout.blob_version` for the same flush.
    pub version: u64,
    /// File size in bytes. Authoritative for the data read path:
    /// `block_content_len = min(block_size, total_size - block_start)`.
    pub total_size: u64,
    /// `ceil(total_size / block_size)`. Stored explicitly so a scanner
    /// can tell in-range blocks (`block_num < block_count`) apart from
    /// orphaned tail blocks without reaching for the layout's
    /// `block_size`.
    pub block_count: u32,
    /// Reserved for future use; must be 0 on write.
    pub _reserved: u32,
}

impl ParentInodeMeta {
    /// On-the-wire size in bytes. The body length BSS receives /
    /// returns is always exactly this many bytes -- no padding.
    pub const WIRE_LEN: usize = 24;

    /// Compute `block_count` from a file size + block size pair.
    /// `block_size` of zero produces a `block_count` of zero (treated
    /// as an empty file).
    pub fn block_count_from(total_size: u64, block_size: u32) -> u32 {
        if block_size == 0 {
            0
        } else {
            total_size.div_ceil(block_size as u64) as u32
        }
    }

    pub fn new(version: u64, total_size: u64, block_size: u32) -> Self {
        Self {
            version,
            total_size,
            block_count: Self::block_count_from(total_size, block_size),
            _reserved: 0,
        }
    }

    pub fn to_bytes(&self) -> [u8; Self::WIRE_LEN] {
        let mut buf = [0u8; Self::WIRE_LEN];
        buf[0..8].copy_from_slice(&self.version.to_le_bytes());
        buf[8..16].copy_from_slice(&self.total_size.to_le_bytes());
        buf[16..20].copy_from_slice(&self.block_count.to_le_bytes());
        buf[20..24].copy_from_slice(&self._reserved.to_le_bytes());
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ParentInodeError> {
        if bytes.len() < Self::WIRE_LEN {
            return Err(ParentInodeError::TooShort {
                got: bytes.len(),
                want: Self::WIRE_LEN,
            });
        }
        let version = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let total_size = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let block_count = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
        let reserved = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
        if reserved != 0 {
            return Err(ParentInodeError::ReservedBytesNonZero(reserved));
        }
        Ok(Self {
            version,
            total_size,
            block_count,
            _reserved: 0,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParentInodeError {
    #[error("ParentInodeMeta body too short: got {got} bytes, need {want}")]
    TooShort { got: usize, want: usize },
    #[error("ParentInodeMeta reserved field is non-zero: {0}")]
    ReservedBytesNonZero(u32),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_simple() {
        let m = ParentInodeMeta::new(7, 100_000, 128 * 1024);
        let bytes = m.to_bytes();
        assert_eq!(bytes.len(), ParentInodeMeta::WIRE_LEN);
        let back = ParentInodeMeta::from_bytes(&bytes).expect("decode");
        assert_eq!(back, m);
        assert_eq!(back.block_count, 1);
    }

    #[test]
    fn block_count_aligned() {
        let m = ParentInodeMeta::new(1, 256 * 1024, 128 * 1024);
        assert_eq!(m.block_count, 2);
    }

    #[test]
    fn block_count_zero_size() {
        let m = ParentInodeMeta::new(1, 0, 128 * 1024);
        assert_eq!(m.block_count, 0);
    }

    #[test]
    fn block_count_zero_block_size_is_safe() {
        let m = ParentInodeMeta::new(1, 100, 0);
        assert_eq!(m.block_count, 0);
    }

    #[test]
    fn from_bytes_rejects_short_buffer() {
        let err = ParentInodeMeta::from_bytes(&[0u8; 16]).expect_err("too short");
        assert!(matches!(
            err,
            ParentInodeError::TooShort { got: 16, want: 24 }
        ));
    }

    #[test]
    fn from_bytes_rejects_nonzero_reserved() {
        let mut bytes = ParentInodeMeta::new(1, 100, 128 * 1024).to_bytes();
        // Stomp the reserved field.
        bytes[20..24].copy_from_slice(&42u32.to_le_bytes());
        let err = ParentInodeMeta::from_bytes(&bytes).expect_err("reserved");
        assert!(matches!(err, ParentInodeError::ReservedBytesNonZero(42)));
    }
}
