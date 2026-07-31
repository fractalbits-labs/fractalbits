//! Per-block overwrite rows: the lazy block-version map.
//!
//! One NSS row per block that was ever overwritten, punched, or trimmed,
//! keyed `@ovr/{blob_id:hex32}/{block:08x}` with a fixed 20-byte value.
//! Absence of a row means the block is at the base version (1), so a
//! file that is only created or appended carries no rows at all.
//!
//! A row holds two (state, version) slots. The two slots exist for
//! atomicity, not history: a flush pre-stages its rows with
//! `cur_version` above the committed ceiling, and the commit CAS that
//! raises the ceiling switches the whole flush's rows on at once. While
//! `cur_version > ceiling`, `prev` is exactly the resolution a reader
//! at the current committed ceiling computes, which holds because at
//! most one flush is committable at a time (the most recent preparer;
//! see the flush's prepare CAS).

use std::collections::BTreeMap;

use bytes::Bytes;
use uuid::Uuid;

/// Fixed encoded size of one row value. NSS rejects empty values, so
/// there is no zero-length encoding to consider.
pub const OVR_ROW_LEN: usize = 20;

const OVR_CODEC_TAG: u8 = 0x01;
const STATE_WRITTEN: u8 = 1;
const STATE_HOLE: u8 = 2;
const PREV_BASE: u8 = 0;

pub const OVR_ROW_PREFIX: &str = "@ovr/";
pub const OVR_GC_PREFIX: &str = "@ovr-gc/";

/// State of one committed slot of a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowState {
    /// The block's content is the BSS generation named by the slot.
    Written,
    /// The block reads as zeros with no BSS access (punch or trim).
    /// Crucially there is no BSS tombstone behind a `Hole`.
    Hole,
}

/// The pre-flush slot of a row. `Base` means "resolve as if there were
/// no row": base version 1, miss tolerated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrevSlot {
    Base,
    Slot(RowState, u64),
}

/// One decoded `@ovr/` row. Field invariants (enforced by `decode`):
/// `cur_version > 0`, and `prev` version is nonzero and strictly below
/// `cur_version` when present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OvrRow {
    pub cur_state: RowState,
    pub cur_version: u64,
    pub prev: PrevSlot,
}

fn state_tag(state: RowState) -> u8 {
    match state {
        RowState::Written => STATE_WRITTEN,
        RowState::Hole => STATE_HOLE,
    }
}

fn tag_state(tag: u8) -> Option<RowState> {
    match tag {
        STATE_WRITTEN => Some(RowState::Written),
        STATE_HOLE => Some(RowState::Hole),
        _ => None,
    }
}

impl OvrRow {
    /// Hand-encoded fixed layout: byte determinism is load-bearing for
    /// the row CAS, and a hand codec avoids rkyv root-offset and
    /// padding surprises.
    pub fn encode(&self) -> [u8; OVR_ROW_LEN] {
        let mut out = [0u8; OVR_ROW_LEN];
        out[0] = OVR_CODEC_TAG;
        out[1] = state_tag(self.cur_state);
        let (prev_state, prev_version) = match self.prev {
            PrevSlot::Base => (PREV_BASE, 0),
            PrevSlot::Slot(state, version) => (state_tag(state), version),
        };
        out[2] = prev_state;
        // out[3]: flags, reserved, must be 0.
        out[4..12].copy_from_slice(&self.cur_version.to_le_bytes());
        out[12..20].copy_from_slice(&prev_version.to_le_bytes());
        out
    }

    pub fn decode(buf: &[u8]) -> Option<OvrRow> {
        if buf.len() != OVR_ROW_LEN || buf[0] != OVR_CODEC_TAG || buf[3] != 0 {
            return None;
        }
        let cur_state = tag_state(buf[1])?;
        let cur_version = u64::from_le_bytes(buf[4..12].try_into().ok()?);
        let prev_version = u64::from_le_bytes(buf[12..20].try_into().ok()?);
        let prev = match buf[2] {
            PREV_BASE => {
                if prev_version != 0 {
                    return None;
                }
                PrevSlot::Base
            }
            tag => {
                let state = tag_state(tag)?;
                if prev_version == 0 || prev_version >= cur_version {
                    return None;
                }
                PrevSlot::Slot(state, prev_version)
            }
        };
        if cur_version == 0 {
            return None;
        }
        Some(OvrRow {
            cur_state,
            cur_version,
            prev,
        })
    }
}

/// How a reader must fetch one mapped-file block below EOF (blocks at
/// or beyond EOF read as zeros without consulting anything).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockResolution {
    /// Zeros with zero RPCs (a committed `Hole`).
    Hole,
    /// One exact `get_blob` at `version`. A total miss is detected data
    /// loss (the map committed this generation), never a sparse hole.
    Exact { version: u64 },
    /// One exact `get_blob` at the base version 1. A total miss is a
    /// tolerated sparse hole (subject to the stale-layout revalidation
    /// protocol).
    Base,
    /// Both slots sit above the reader's ceiling: the layout snapshot
    /// predates a commit that happened between the layout fetch and the
    /// row's last write. Refresh the layout and the row, then retry.
    Stale,
}

/// The reader decision procedure for one block of a mapped blob, given
/// the layout's committed ceiling. `None` means the row is absent from
/// a complete row snapshot. An unmapped blob (`map_epoch == 0`) skips
/// rows entirely and resolves every block to `Base`.
pub fn resolve_row(row: Option<&OvrRow>, ceiling: u64) -> BlockResolution {
    let Some(row) = row else {
        return BlockResolution::Base;
    };
    let slot = if row.cur_version <= ceiling {
        (row.cur_state, row.cur_version)
    } else {
        match row.prev {
            PrevSlot::Base => return BlockResolution::Base,
            PrevSlot::Slot(state, version) if version <= ceiling => (state, version),
            PrevSlot::Slot(_, _) => return BlockResolution::Stale,
        }
    };
    match slot {
        (RowState::Hole, _) => BlockResolution::Hole,
        (RowState::Written, version) => BlockResolution::Exact { version },
    }
}

/// How to fetch one resolved block from BSS. This owns the fetch-side
/// consequences of a resolution so every reader path (FUSE read, flush
/// RMW load, prefetch, S3 GET) shares one copy of the two invariants:
/// the storage-format rule that burned generations (version > 1) are
/// stored zero-padded to `block_size` (constant EC shard size) while
/// base-version blocks keep their natural length, and the durability
/// rule that a row-committed generation missing on every replica is
/// detected data loss, never a sparse hole.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockFetchPlan {
    /// Serve zeros with no BSS access (a committed `Hole`).
    Zeros,
    /// Both row slots sit above the reader's ceiling: refresh the
    /// layout and the row, then retry.
    Stale,
    /// One exact `get_blob` at `version`, requesting `read_len` bytes
    /// and clamping the returned body to the logical content length.
    Fetch {
        version: u64,
        read_len: usize,
        /// A total miss is detected data loss when true (the row
        /// committed this exact generation), a tolerated sparse hole
        /// when false (base version).
        miss_is_loss: bool,
    },
}

/// Fetch plan for one block of a (possibly unmapped) blob at the
/// layout's committed ceiling. See `BlockFetchPlan`.
pub fn block_fetch_plan(
    rows: Option<&OvrRowMap>,
    block: u32,
    ceiling: u64,
    block_size: usize,
    content_len: usize,
) -> BlockFetchPlan {
    let resolution = match rows {
        Some(rows) => rows.resolve(block, ceiling),
        None => BlockResolution::Base,
    };
    let (version, miss_is_loss) = match resolution {
        BlockResolution::Hole => return BlockFetchPlan::Zeros,
        BlockResolution::Stale => return BlockFetchPlan::Stale,
        BlockResolution::Exact { version } => (version, true),
        BlockResolution::Base => (1, false),
    };
    let read_len = if version > 1 {
        block_size.max(content_len)
    } else {
        content_len
    };
    BlockFetchPlan::Fetch {
        version,
        read_len,
        miss_is_loss,
    }
}

/// Shared all-zero block backing `zeros`; sized to the default block
/// size, which every layout uses today.
static ZERO_BLOCK: [u8; 128 * 1024] = [0u8; 128 * 1024];

/// A zero buffer of `len` bytes. Sliced from one static block (a
/// refcount bump, no allocation or memset) whenever it fits; hole and
/// sparse reads hit this once per block.
pub fn zeros(len: usize) -> Bytes {
    if len <= ZERO_BLOCK.len() {
        Bytes::from_static(&ZERO_BLOCK[..len])
    } else {
        Bytes::from(vec![0u8; len])
    }
}

/// The monotone row-CAS merge with conditional promotion.
/// `base_ceiling` is the committed ceiling the flush prepared against;
/// it cannot move before this flush commits or aborts (the most recent
/// preparer is the only possible committer), which is what makes the
/// promotion sound. Returns `None` when the write must be skipped
/// (`version` no newer than the stored row).
pub fn merge_row_for_write(
    old: Option<&OvrRow>,
    new_state: RowState,
    version: u64,
    base_ceiling: u64,
) -> Option<OvrRow> {
    let prev = match old {
        None => PrevSlot::Base,
        Some(old) => {
            if version <= old.cur_version {
                return None;
            }
            if old.cur_version <= base_ceiling {
                // The stored cur is committed: it becomes the reader's
                // fallback while this flush's version is above the
                // ceiling, and the exact identity the sweep may reclaim
                // once the commit lands.
                PrevSlot::Slot(old.cur_state, old.cur_version)
            } else {
                // The stored cur belongs to an attempt that never
                // committed (ours or an abandoned one): never promote a
                // version no reader could have observed.
                old.prev
            }
        }
    };
    Some(OvrRow {
        cur_state: new_state,
        cur_version: version,
        prev,
    })
}

/// NSS key of one block's row.
pub fn ovr_row_key(blob_id: &Uuid, block: u32) -> String {
    format!("{}{}/{:08x}", OVR_ROW_PREFIX, blob_id.as_simple(), block)
}

/// Prefix of every row of a blob. The block number is fixed-width hex,
/// so lexicographic order under this prefix equals numeric block order
/// (range prefetch and `start_after` pagination depend on that).
pub fn ovr_row_prefix(blob_id: &Uuid) -> String {
    format!("{}{}/", OVR_ROW_PREFIX, blob_id.as_simple())
}

/// Teardown intent marker: written before the inode that names
/// `blob_id` is deleted, removed after the row sweep completes. Without
/// it a crash mid-teardown leaks the rows permanently, since no
/// surviving key can recover the blob_id.
pub fn ovr_gc_key(blob_id: &Uuid) -> String {
    format!("{}{}", OVR_GC_PREFIX, blob_id.as_simple())
}

/// Block number of a row key returned by a prefix listing (with or
/// without the ART's trailing NUL).
pub fn parse_ovr_row_block(key: &str) -> Option<u32> {
    let key = key.trim_end_matches('\0');
    let rest = key.strip_prefix(OVR_ROW_PREFIX)?;
    let (_, block_hex) = rest.split_once('/')?;
    if block_hex.len() != 8 {
        return None;
    }
    u32::from_str_radix(block_hex, 16).ok()
}

/// Blob id of a `@ovr-gc/` marker key.
pub fn parse_ovr_gc_blob_id(key: &str) -> Option<Uuid> {
    let key = key.trim_end_matches('\0');
    Uuid::try_parse(key.strip_prefix(OVR_GC_PREFIX)?).ok()
}

/// A complete row snapshot for one blob, tagged with the `map_epoch`
/// it was loaded under. Validity rule: a snapshot at epoch M serves any
/// read whose layout carries the same M, however far the ceiling has
/// advanced, including the negative knowledge that a row was absent.
#[derive(Debug, Clone, Default)]
pub struct OvrRowMap {
    pub epoch: u64,
    rows: BTreeMap<u32, OvrRow>,
}

impl OvrRowMap {
    pub fn new(epoch: u64) -> Self {
        Self {
            epoch,
            rows: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, block: u32, row: OvrRow) {
        self.rows.insert(block, row);
    }

    pub fn get(&self, block: u32) -> Option<&OvrRow> {
        self.rows.get(&block)
    }

    pub fn resolve(&self, block: u32, ceiling: u64) -> BlockResolution {
        resolve_row(self.get(block), ceiling)
    }

    /// Rows intersecting `[start, end_excl)`, in block order.
    pub fn range(&self, start: u32, end_excl: u32) -> impl Iterator<Item = (u32, &OvrRow)> {
        self.rows
            .range(start..end_excl)
            .map(|(block, row)| (*block, row))
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(cur_state: RowState, cur_version: u64, prev: PrevSlot) -> OvrRow {
        OvrRow {
            cur_state,
            cur_version,
            prev,
        }
    }

    #[test]
    fn codec_round_trips_every_shape() {
        let shapes = [
            row(RowState::Written, 7, PrevSlot::Base),
            row(RowState::Hole, 9, PrevSlot::Slot(RowState::Written, 3)),
            row(
                RowState::Written,
                u64::MAX,
                PrevSlot::Slot(RowState::Hole, 1),
            ),
        ];
        for shape in shapes {
            let bytes = shape.encode();
            assert_eq!(bytes.len(), OVR_ROW_LEN);
            assert_eq!(OvrRow::decode(&bytes), Some(shape), "shape {shape:?}");
        }
    }

    #[test]
    fn codec_rejects_malformed_rows() {
        let good = row(RowState::Written, 7, PrevSlot::Slot(RowState::Hole, 3)).encode();

        let mut bad_tag = good;
        bad_tag[0] = 0x02;
        assert_eq!(OvrRow::decode(&bad_tag), None);

        let mut bad_state = good;
        bad_state[1] = 3;
        assert_eq!(OvrRow::decode(&bad_state), None);

        let mut bad_flags = good;
        bad_flags[3] = 1;
        assert_eq!(OvrRow::decode(&bad_flags), None);

        // prev_version must be 0 iff prev_state is Base.
        let mut base_with_version = row(RowState::Written, 7, PrevSlot::Base).encode();
        base_with_version[12] = 1;
        assert_eq!(OvrRow::decode(&base_with_version), None);
        let mut slot_without_version = good;
        slot_without_version[12..20].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(OvrRow::decode(&slot_without_version), None);

        // prev strictly below cur; cur nonzero.
        let mut inverted = good;
        inverted[12..20].copy_from_slice(&7u64.to_le_bytes());
        assert_eq!(OvrRow::decode(&inverted), None);
        let mut zero_cur = row(RowState::Written, 1, PrevSlot::Base).encode();
        zero_cur[4..12].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(OvrRow::decode(&zero_cur), None);

        assert_eq!(OvrRow::decode(&good[..19]), None);
    }

    #[test]
    fn reader_procedure_covers_every_case() {
        let ceiling = 5;
        // Case 2: absent row resolves to base, miss tolerated.
        assert_eq!(resolve_row(None, ceiling), BlockResolution::Base);
        // Case 3: committed cur wins; miss is loss.
        assert_eq!(
            resolve_row(Some(&row(RowState::Written, 5, PrevSlot::Base)), ceiling),
            BlockResolution::Exact { version: 5 }
        );
        assert_eq!(
            resolve_row(
                Some(&row(
                    RowState::Hole,
                    4,
                    PrevSlot::Slot(RowState::Written, 2)
                )),
                ceiling
            ),
            BlockResolution::Hole
        );
        // Case 4: staged cur above the ceiling falls back to base.
        assert_eq!(
            resolve_row(Some(&row(RowState::Written, 9, PrevSlot::Base)), ceiling),
            BlockResolution::Base
        );
        // Case 5: staged cur falls back to the committed prev slot.
        assert_eq!(
            resolve_row(
                Some(&row(
                    RowState::Written,
                    9,
                    PrevSlot::Slot(RowState::Written, 4)
                )),
                ceiling
            ),
            BlockResolution::Exact { version: 4 }
        );
        assert_eq!(
            resolve_row(
                Some(&row(
                    RowState::Written,
                    9,
                    PrevSlot::Slot(RowState::Hole, 3)
                )),
                ceiling
            ),
            BlockResolution::Hole
        );
        // Case 6: both slots above the ceiling force a refresh.
        assert_eq!(
            resolve_row(
                Some(&row(
                    RowState::Written,
                    9,
                    PrevSlot::Slot(RowState::Written, 7)
                )),
                ceiling
            ),
            BlockResolution::Stale
        );
    }

    #[test]
    fn merge_promotes_only_committed_versions() {
        let base_ceiling = 5;
        // Fresh block: prev is Base.
        assert_eq!(
            merge_row_for_write(None, RowState::Written, 7, base_ceiling),
            Some(row(RowState::Written, 7, PrevSlot::Base))
        );
        // Committed old cur is promoted into prev.
        let committed = row(RowState::Written, 4, PrevSlot::Slot(RowState::Hole, 2));
        assert_eq!(
            merge_row_for_write(Some(&committed), RowState::Hole, 7, base_ceiling),
            Some(row(RowState::Hole, 7, PrevSlot::Slot(RowState::Written, 4)))
        );
        // A never-committed cur (above the base ceiling) is never
        // promoted: readers keep resolving the last committed slot.
        let staged = row(RowState::Written, 7, PrevSlot::Slot(RowState::Written, 4));
        assert_eq!(
            merge_row_for_write(Some(&staged), RowState::Written, 9, base_ceiling),
            Some(row(
                RowState::Written,
                9,
                PrevSlot::Slot(RowState::Written, 4)
            ))
        );
        // Monotone: a version at or below the stored cur is skipped.
        assert_eq!(
            merge_row_for_write(Some(&staged), RowState::Written, 7, base_ceiling),
            None
        );
        assert_eq!(
            merge_row_for_write(Some(&staged), RowState::Written, 6, base_ceiling),
            None
        );
    }

    #[test]
    fn repeated_failed_attempts_never_move_prev() {
        // V1 < V2 < V3 burn without committing: each CAS advances cur,
        // prev stays at the committed resolution throughout.
        let base_ceiling = 3;
        let committed = row(RowState::Written, 3, PrevSlot::Base);
        let mut current = committed;
        for attempt in [5, 6, 9] {
            current = merge_row_for_write(Some(&current), RowState::Written, attempt, base_ceiling)
                .expect("newer version must supersede");
            assert_eq!(current.prev, PrevSlot::Slot(RowState::Written, 3));
            assert_eq!(
                resolve_row(Some(&current), base_ceiling),
                BlockResolution::Exact { version: 3 },
                "readers keep the committed bytes"
            );
        }
    }

    #[test]
    fn fetch_plan_owns_padding_and_loss_semantics() {
        let bs = 128 * 1024;
        let mut map = OvrRowMap::new(4);
        map.insert(1, row(RowState::Written, 3, PrevSlot::Base));
        map.insert(2, row(RowState::Hole, 4, PrevSlot::Base));
        map.insert(
            3,
            row(RowState::Written, 9, PrevSlot::Slot(RowState::Written, 7)),
        );

        // Unmapped blob / absent row: base version, natural length,
        // miss tolerated.
        assert_eq!(
            block_fetch_plan(None, 0, 4, bs, 17),
            BlockFetchPlan::Fetch {
                version: 1,
                read_len: 17,
                miss_is_loss: false
            }
        );
        // Burned generation: padded request, miss is loss.
        assert_eq!(
            block_fetch_plan(Some(&map), 1, 4, bs, 17),
            BlockFetchPlan::Fetch {
                version: 3,
                read_len: bs,
                miss_is_loss: true
            }
        );
        assert_eq!(
            block_fetch_plan(Some(&map), 2, 4, bs, 17),
            BlockFetchPlan::Zeros
        );
        assert_eq!(
            block_fetch_plan(Some(&map), 3, 4, bs, 17),
            BlockFetchPlan::Stale
        );
        // A content length above block_size (single-block small file)
        // never shrinks the request.
        assert_eq!(
            block_fetch_plan(Some(&map), 1, 4, bs, bs + 5),
            BlockFetchPlan::Fetch {
                version: 3,
                read_len: bs + 5,
                miss_is_loss: true
            }
        );
    }

    #[test]
    fn zeros_are_shared_and_unbounded() {
        assert_eq!(zeros(0).len(), 0);
        let block = zeros(128 * 1024);
        assert_eq!(block.len(), 128 * 1024);
        assert!(block.iter().all(|byte| *byte == 0));
        let big = zeros(128 * 1024 + 1);
        assert_eq!(big.len(), 128 * 1024 + 1);
        assert!(big.iter().all(|byte| *byte == 0));
    }

    #[test]
    fn keys_are_fixed_width_and_parse_back() {
        let blob_id = Uuid::from_u128(0x1234_5678_9abc_def0_1122_3344_5566_7788);
        let key = ovr_row_key(&blob_id, 0x2a);
        assert_eq!(key, "@ovr/123456789abcdef01122334455667788/0000002a");
        assert!(key.starts_with(&ovr_row_prefix(&blob_id)));
        assert_eq!(parse_ovr_row_block(&key), Some(0x2a));
        assert_eq!(parse_ovr_row_block(&format!("{key}\0")), Some(0x2a));
        assert_eq!(parse_ovr_row_block("@ovr/xyz"), None);

        // Fixed-width hex: lexicographic order equals numeric order.
        assert!(ovr_row_key(&blob_id, 9) < ovr_row_key(&blob_id, 10));
        assert!(ovr_row_key(&blob_id, 255) < ovr_row_key(&blob_id, 256));

        let gc = ovr_gc_key(&blob_id);
        assert_eq!(gc, "@ovr-gc/123456789abcdef01122334455667788");
        assert_eq!(parse_ovr_gc_blob_id(&gc), Some(blob_id));
    }

    #[test]
    fn row_map_resolves_ranges_and_absences() {
        let mut map = OvrRowMap::new(4);
        map.insert(2, row(RowState::Written, 3, PrevSlot::Base));
        map.insert(
            7,
            row(RowState::Hole, 4, PrevSlot::Slot(RowState::Written, 2)),
        );

        assert_eq!(map.resolve(2, 4), BlockResolution::Exact { version: 3 });
        assert_eq!(map.resolve(7, 4), BlockResolution::Hole);
        assert_eq!(map.resolve(5, 4), BlockResolution::Base);
        assert_eq!(map.range(0, 5).count(), 1);
        assert_eq!(map.range(0, 8).count(), 2);
        assert_eq!(map.len(), 2);
    }
}
