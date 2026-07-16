//! Per-block version map for data-volume blobs.
//!
//! Sparse, range-encoded record of which BSS generation holds each block of
//! a blob. Blocks not covered by any range are at version 1 if they were
//! ever written (the map exists only when overwrite, punch, poison, or
//! fallocate happened). The map is stored as immutable chunk records in the
//! NSS keyspace at `#bmap/{blob_id}/{map_id}-{chunk_no}`; a flush that
//! changes the map writes a complete new chunk set under a fresh `map_id`
//! and the inode CAS flips the `BlockMapRef` pointer.

use crate::DataBlobGuid;
use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

/// State of one block range.
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub enum RangeState {
    /// Committed generation: the block's body is exactly
    /// `(blob_guid, block, version)` and must exist on the read rule.
    Written(u64),
    /// Punched hole or poisoned aborted-append range: reads as zeros with
    /// no BSS access; writers must never write these blocks at version 1.
    Hole,
    /// fallocate claim at the given version: reads as zeros; the BSS holds
    /// a space reservation at `(blob_guid, block, version)`.
    Reserved(u64),
}

/// Inclusive block range with a uniform state.
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Eq, Clone, Copy)]
pub struct BlockRange {
    pub start: u32,
    pub end: u32, // inclusive
    pub state: RangeState,
}

/// One persisted chunk of a map (must serialize under the NSS value cap).
#[derive(Debug, Archive, Deserialize, Serialize, PartialEq, Clone)]
pub struct BlockMapChunk {
    pub ranges: Vec<BlockRange>,
}

/// Cap chosen so a full chunk serializes well under the 8 KiB NSS value
/// limit (a BlockRange archives to ~16 bytes).
pub const MAX_RANGES_PER_CHUNK: usize = 300;

/// NSS key of one map chunk. Keyed by the stable `blob_id` (not the s3
/// key) so the map survives rename and serves hardlinked files unchanged;
/// `#` is encoding-reserved so the keyspace cannot collide with names.
pub fn bmap_chunk_key(blob_guid: &DataBlobGuid, map_id: Uuid, chunk_no: u32) -> String {
    format!("#bmap/{}/{}-{:04}", blob_guid.blob_id, map_id, chunk_no)
}

/// Prefix of every chunk of every map version of a blob (unlink teardown).
pub fn bmap_prefix(blob_guid: &DataBlobGuid) -> String {
    format!("#bmap/{}/", blob_guid.blob_id)
}

/// In-memory per-block version map: sorted, non-overlapping, adjacent
/// equal-state ranges merged.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BlockMap {
    ranges: Vec<BlockRange>,
}

impl BlockMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn range_count(&self) -> usize {
        self.ranges.len()
    }

    pub fn ranges(&self) -> &[BlockRange] {
        &self.ranges
    }

    /// Resolve one block. `None` means "not covered": version 1 if the
    /// block was ever written, else a sparse hole.
    pub fn lookup(&self, block: u32) -> Option<RangeState> {
        let idx = self.ranges.partition_point(|r| r.end < block);
        match self.ranges.get(idx) {
            Some(r) if r.start <= block => Some(r.state),
            _ => None,
        }
    }

    /// The committed version a reader must fetch for `block`, or `None`
    /// for zeros-without-RPC (hole / reserved).
    pub fn read_version(&self, block: u32) -> Option<u64> {
        match self.lookup(block) {
            Some(RangeState::Written(v)) => Some(v),
            Some(RangeState::Hole) | Some(RangeState::Reserved(_)) => None,
            None => Some(1),
        }
    }

    /// Overlay `[start, end]` (inclusive) with `state`: clip every existing
    /// range against the span (keep the outside fragments), insert the new
    /// range, then merge touching equal-state neighbors.
    pub fn overlay(&mut self, start: u32, end: u32, state: RangeState) {
        assert!(start <= end, "overlay range inverted: {start}..{end}");
        let mut out: Vec<BlockRange> = Vec::with_capacity(self.ranges.len() + 2);
        for r in self.ranges.drain(..) {
            if r.end < start || r.start > end {
                out.push(r);
                continue;
            }
            // The `start > 0` / `end < u32::MAX` bounds are implied by the
            // fragment conditions, so the +-1 cannot overflow.
            if r.start < start {
                out.push(BlockRange {
                    start: r.start,
                    end: start - 1,
                    state: r.state,
                });
            }
            if r.end > end {
                out.push(BlockRange {
                    start: end + 1,
                    end: r.end,
                    state: r.state,
                });
            }
        }
        out.push(BlockRange { start, end, state });
        out.sort_by_key(|r| r.start);
        let mut merged: Vec<BlockRange> = Vec::with_capacity(out.len());
        for r in out {
            match merged.last_mut() {
                Some(last)
                    if last.state == r.state && last.end != u32::MAX && last.end + 1 >= r.start =>
                {
                    last.end = last.end.max(r.end);
                }
                _ => merged.push(r),
            }
        }
        self.ranges = merged;
    }

    /// Remove coverage at and above `first_dropped_block` (truncate down).
    pub fn clip_from(&mut self, first_dropped_block: u32) {
        self.ranges.retain_mut(|r| {
            if r.start >= first_dropped_block {
                return false;
            }
            if r.end >= first_dropped_block {
                r.end = first_dropped_block - 1;
            }
            true
        });
    }

    /// Serialize into chunk records, each under the NSS value cap.
    pub fn to_chunks(&self) -> Result<Vec<Vec<u8>>, rkyv::rancor::Error> {
        let mut chunks = Vec::new();
        for ranges in self.ranges.chunks(MAX_RANGES_PER_CHUNK) {
            let chunk = BlockMapChunk {
                ranges: ranges.to_vec(),
            };
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&chunk)?;
            chunks.push(bytes.to_vec());
        }
        Ok(chunks)
    }

    /// Rebuild from chunk records fetched in order.
    pub fn from_chunks(chunks: &[impl AsRef<[u8]>]) -> Result<Self, rkyv::rancor::Error> {
        let mut ranges = Vec::new();
        for c in chunks {
            let chunk = rkyv::from_bytes::<BlockMapChunk, rkyv::rancor::Error>(c.as_ref())?;
            ranges.extend(chunk.ranges);
        }
        Ok(Self { ranges })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(ranges: &[(u32, u32, RangeState)]) -> BlockMap {
        let mut m = BlockMap::new();
        for (s, e, st) in ranges {
            m.overlay(*s, *e, *st);
        }
        m
    }

    #[test]
    fn lookup_empty_map() {
        let m = BlockMap::new();
        assert_eq!(m.lookup(0), None);
        assert_eq!(m.read_version(7), Some(1), "uncovered block is v1");
    }

    #[test]
    fn overlay_disjoint_and_lookup() {
        let m = map(&[(10, 19, RangeState::Written(5)), (30, 30, RangeState::Hole)]);
        assert_eq!(m.range_count(), 2);
        assert_eq!(m.lookup(9), None);
        assert_eq!(m.lookup(10), Some(RangeState::Written(5)));
        assert_eq!(m.lookup(19), Some(RangeState::Written(5)));
        assert_eq!(m.lookup(20), None);
        assert_eq!(m.lookup(30), Some(RangeState::Hole));
        assert_eq!(m.read_version(30), None, "hole reads zeros without RPC");
    }

    #[test]
    fn overlay_replaces_covered_span() {
        let mut m = map(&[(0, 99, RangeState::Written(2))]);
        m.overlay(40, 59, RangeState::Written(3));
        assert_eq!(
            m.ranges(),
            &[
                BlockRange {
                    start: 0,
                    end: 39,
                    state: RangeState::Written(2)
                },
                BlockRange {
                    start: 40,
                    end: 59,
                    state: RangeState::Written(3)
                },
                BlockRange {
                    start: 60,
                    end: 99,
                    state: RangeState::Written(2)
                },
            ]
        );
    }

    #[test]
    fn overlay_merges_equal_state_neighbors() {
        let mut m = map(&[
            (0, 9, RangeState::Written(4)),
            (20, 29, RangeState::Written(4)),
        ]);
        m.overlay(10, 19, RangeState::Written(4));
        assert_eq!(
            m.ranges(),
            &[BlockRange {
                start: 0,
                end: 29,
                state: RangeState::Written(4)
            }]
        );
    }

    #[test]
    fn overlay_partial_overlap_both_sides() {
        let mut m = map(&[
            (10, 19, RangeState::Written(2)),
            (25, 34, RangeState::Written(3)),
        ]);
        m.overlay(15, 29, RangeState::Hole);
        assert_eq!(
            m.ranges(),
            &[
                BlockRange {
                    start: 10,
                    end: 14,
                    state: RangeState::Written(2)
                },
                BlockRange {
                    start: 15,
                    end: 29,
                    state: RangeState::Hole
                },
                BlockRange {
                    start: 30,
                    end: 34,
                    state: RangeState::Written(3)
                },
            ]
        );
    }

    #[test]
    fn whole_file_rewrite_collapses_to_one_range() {
        let mut m = map(&[
            (3, 7, RangeState::Written(2)),
            (9, 9, RangeState::Hole),
            (12, 40, RangeState::Written(6)),
        ]);
        m.overlay(0, 99, RangeState::Written(9));
        assert_eq!(m.range_count(), 1);
        assert_eq!(m.lookup(50), Some(RangeState::Written(9)));
    }

    #[test]
    fn clip_from_truncates_coverage() {
        let mut m = map(&[
            (0, 9, RangeState::Written(2)),
            (20, 29, RangeState::Written(3)),
        ]);
        m.clip_from(5);
        assert_eq!(
            m.ranges(),
            &[BlockRange {
                start: 0,
                end: 4,
                state: RangeState::Written(2)
            }]
        );
        m.clip_from(0);
        assert!(m.is_empty());
    }

    #[test]
    fn chunk_round_trip() {
        let mut m = BlockMap::new();
        // Non-mergeable ranges (alternating versions) to force many entries.
        for i in 0..(MAX_RANGES_PER_CHUNK as u32 * 2 + 17) {
            m.overlay(i * 2, i * 2, RangeState::Written(2 + (i as u64 % 3)));
        }
        let chunks = m.to_chunks().expect("to_chunks");
        assert!(chunks.len() >= 3, "expected multiple chunks");
        for c in &chunks {
            assert!(c.len() < 7 * 1024, "chunk must fit the NSS value cap");
        }
        let back = BlockMap::from_chunks(&chunks).expect("from_chunks");
        assert_eq!(back, m);
    }

    #[test]
    fn overlay_at_block_zero_and_u32_edges() {
        let mut m = BlockMap::new();
        m.overlay(0, 0, RangeState::Written(2));
        assert_eq!(m.lookup(0), Some(RangeState::Written(2)));
        m.overlay(0, 5, RangeState::Written(3));
        assert_eq!(m.range_count(), 1);
        assert_eq!(m.lookup(3), Some(RangeState::Written(3)));
    }

    #[test]
    fn reserved_reads_as_zeros() {
        let m = map(&[(4, 8, RangeState::Reserved(7))]);
        assert_eq!(m.read_version(5), None);
        assert_eq!(m.lookup(5), Some(RangeState::Reserved(7)));
    }
}
