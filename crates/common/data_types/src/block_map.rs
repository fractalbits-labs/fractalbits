//! Per-block version map for data-volume blobs.
//!
//! Sparse, range-encoded record of which BSS generation holds each block of
//! a blob. Blocks not covered by any range are at version 1 if they were
//! ever written (the map exists once an existing blob is mutated or
//! fallocate is used). The map is stored as immutable chunk records in the
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
    /// Punched, trimmed, or conversion-safe hole: reads as zeros with no BSS
    /// access.
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

/// Cap chosen so a full chunk serializes well under the 8 KiB NSS value
/// limit: the worst-case encoded range is 21 bytes (two 5-byte u32
/// varints, the state tag, one 10-byte u64 varint), so a full chunk
/// stays under 6.4 KiB; typical ranges cost 5-8 bytes.
pub const MAX_RANGES_PER_CHUNK: usize = 300;

/// Chunk wire format version. The chunk value is a compact delta+varint
/// encoding (the same family BlueStore uses for its extent maps) rather
/// than a general-purpose compressor: chunks live uncompressed in NSS
/// memory, and domain encoding beats lz4 on these tiny structured
/// records while staying deterministic and dependency-free.
///
/// Layout: `[version byte][varint range_count]` then per range, in block
/// order: `[varint gap]` (start minus the previous range's end+1;
/// absolute start for the first range), `[varint length-1]` (inclusive),
/// `[state tag byte]`, and for Written/Reserved `[varint version]`.
const CHUNK_CODEC_V1: u8 = 1;
const STATE_TAG_WRITTEN: u8 = 0;
const STATE_TAG_HOLE: u8 = 1;
const STATE_TAG_RESERVED: u8 = 2;

#[derive(Debug, thiserror::Error)]
pub enum BlockMapCodecError {
    #[error("unknown chunk codec version {0}")]
    UnknownCodec(u8),
    #[error("truncated chunk")]
    Truncated,
    #[error("trailing bytes after {0} ranges")]
    TrailingBytes(u64),
    #[error("invalid state tag {0}")]
    InvalidStateTag(u8),
    #[error("range exceeds the u32 block space")]
    Overflow,
    #[error("ranges out of order across chunks")]
    Unordered,
}

fn push_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn read_varint(bytes: &[u8], pos: &mut usize) -> Result<u64, BlockMapCodecError> {
    let mut v: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let byte = *bytes.get(*pos).ok_or(BlockMapCodecError::Truncated)?;
        *pos += 1;
        if shift == 63 && byte > 1 {
            return Err(BlockMapCodecError::Overflow);
        }
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(v);
        }
        shift += 7;
        if shift > 63 {
            return Err(BlockMapCodecError::Overflow);
        }
    }
}

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

/// Prefix for active reader leases that pin this blob's generations.
pub fn block_reader_prefix(blob_guid: &DataBlobGuid) -> String {
    format!("#bmap-reader/{}/", blob_guid.blob_id)
}

/// NSS key for one active reader lease.
pub fn block_reader_key(blob_guid: &DataBlobGuid, reader_id: Uuid) -> String {
    format!("{}{reader_id}", block_reader_prefix(blob_guid))
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

    /// Serialize into chunk records, each under the NSS value cap. Cannot
    /// fail: the in-memory invariant (sorted, non-overlapping ranges)
    /// guarantees every delta is representable.
    pub fn to_chunks(&self) -> Vec<Vec<u8>> {
        let mut chunks = Vec::new();
        for ranges in self.ranges.chunks(MAX_RANGES_PER_CHUNK) {
            let mut out = Vec::with_capacity(2 + ranges.len() * 8);
            out.push(CHUNK_CODEC_V1);
            push_varint(&mut out, ranges.len() as u64);
            // Next block not yet covered by an encoded range; deltas from
            // it keep typical gaps and lengths in one or two bytes.
            let mut cursor: u64 = 0;
            for r in ranges {
                push_varint(&mut out, u64::from(r.start) - cursor);
                push_varint(&mut out, u64::from(r.end - r.start));
                match r.state {
                    RangeState::Written(v) => {
                        out.push(STATE_TAG_WRITTEN);
                        push_varint(&mut out, v);
                    }
                    RangeState::Hole => out.push(STATE_TAG_HOLE),
                    RangeState::Reserved(v) => {
                        out.push(STATE_TAG_RESERVED);
                        push_varint(&mut out, v);
                    }
                }
                cursor = u64::from(r.end) + 1;
            }
            chunks.push(out);
        }
        chunks
    }

    /// Rebuild from chunk records fetched in order.
    pub fn from_chunks(chunks: &[impl AsRef<[u8]>]) -> Result<Self, BlockMapCodecError> {
        let mut ranges: Vec<BlockRange> = Vec::new();
        for c in chunks {
            let bytes = c.as_ref();
            let mut pos = 0usize;
            let codec = *bytes.first().ok_or(BlockMapCodecError::Truncated)?;
            pos += 1;
            if codec != CHUNK_CODEC_V1 {
                return Err(BlockMapCodecError::UnknownCodec(codec));
            }
            let count = read_varint(bytes, &mut pos)?;
            let mut cursor: u64 = 0;
            for _ in 0..count {
                let gap = read_varint(bytes, &mut pos)?;
                let len_minus_1 = read_varint(bytes, &mut pos)?;
                let start = cursor + gap;
                let end = start + len_minus_1;
                if end > u64::from(u32::MAX) {
                    return Err(BlockMapCodecError::Overflow);
                }
                let tag = *bytes.get(pos).ok_or(BlockMapCodecError::Truncated)?;
                pos += 1;
                let state = match tag {
                    STATE_TAG_WRITTEN => RangeState::Written(read_varint(bytes, &mut pos)?),
                    STATE_TAG_HOLE => RangeState::Hole,
                    STATE_TAG_RESERVED => RangeState::Reserved(read_varint(bytes, &mut pos)?),
                    other => return Err(BlockMapCodecError::InvalidStateTag(other)),
                };
                if let Some(prev) = ranges.last()
                    && start <= u64::from(prev.end)
                {
                    return Err(BlockMapCodecError::Unordered);
                }
                ranges.push(BlockRange {
                    start: start as u32,
                    end: end as u32,
                    state,
                });
                cursor = end + 1;
            }
            if pos != bytes.len() {
                return Err(BlockMapCodecError::TrailingBytes(count));
            }
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
        let chunks = m.to_chunks();
        assert!(chunks.len() >= 3, "expected multiple chunks");
        for c in &chunks {
            assert!(c.len() < 7 * 1024, "chunk must fit the NSS value cap");
        }
        let back = BlockMap::from_chunks(&chunks).expect("from_chunks");
        assert_eq!(back, m);
    }

    #[test]
    fn chunk_codec_edge_values_round_trip() {
        let m = map(&[
            (0, 0, RangeState::Written(1)),
            (1, 1, RangeState::Hole),
            (2, 7, RangeState::Reserved(u64::MAX)),
            (u32::MAX - 1, u32::MAX, RangeState::Written(u64::MAX)),
        ]);
        let chunks = m.to_chunks();
        assert_eq!(chunks.len(), 1);
        let back = BlockMap::from_chunks(&chunks).expect("from_chunks");
        assert_eq!(back, m);

        let empty = BlockMap::new();
        assert!(empty.to_chunks().is_empty());
        assert!(
            BlockMap::from_chunks(&Vec::<Vec<u8>>::new())
                .expect("empty")
                .is_empty()
        );
    }

    #[test]
    fn chunk_codec_typical_range_costs_few_bytes() {
        // Dense alternating-version single-block ranges: gap 1B, len 1B,
        // tag 1B, version 1B. The old rkyv encoding paid 17 bytes here.
        let mut m = BlockMap::new();
        for i in 0..MAX_RANGES_PER_CHUNK as u32 {
            m.overlay(i * 2, i * 2, RangeState::Written(2 + (i as u64 % 3)));
        }
        let chunks = m.to_chunks();
        assert_eq!(chunks.len(), 1);
        let per_range = chunks[0].len() / MAX_RANGES_PER_CHUNK;
        assert!(
            per_range <= 6,
            "expected <= 6 bytes per typical range, got {per_range}"
        );
    }

    #[test]
    fn chunk_codec_worst_case_fits_value_cap() {
        // Worst case: huge gaps (5-byte u32 varints) and max versions
        // (10-byte u64 varints). A full chunk must stay under the 8 KiB
        // NSS value cap.
        let mut m = BlockMap::new();
        let stride = u32::MAX / (MAX_RANGES_PER_CHUNK as u32 + 1);
        for i in 0..MAX_RANGES_PER_CHUNK as u32 {
            let start = (i + 1) * stride;
            m.overlay(start, start, RangeState::Written(u64::MAX - u64::from(i)));
        }
        let chunks = m.to_chunks();
        assert_eq!(chunks.len(), 1);
        assert!(
            chunks[0].len() < 7 * 1024,
            "worst-case chunk {} bytes exceeds budget",
            chunks[0].len()
        );
        assert_eq!(BlockMap::from_chunks(&chunks).expect("rt"), m);
    }

    #[test]
    fn chunk_codec_rejects_malformed_input() {
        let m = map(&[(3, 9, RangeState::Written(4))]);
        let mut chunks = m.to_chunks();

        let truncated = &chunks[0][..chunks[0].len() - 1];
        assert!(matches!(
            BlockMap::from_chunks(&[truncated]),
            Err(BlockMapCodecError::Truncated)
        ));

        let mut trailing = chunks[0].clone();
        trailing.push(0);
        assert!(matches!(
            BlockMap::from_chunks(&[trailing]),
            Err(BlockMapCodecError::TrailingBytes(_))
        ));

        let mut bad_codec = chunks[0].clone();
        bad_codec[0] = 9;
        assert!(matches!(
            BlockMap::from_chunks(&[bad_codec]),
            Err(BlockMapCodecError::UnknownCodec(9))
        ));

        // Duplicate chunk: the second copy restarts at block 3, behind the
        // first copy's end, which must surface as cross-chunk disorder.
        let dup = chunks[0].clone();
        chunks.push(dup);
        assert!(matches!(
            BlockMap::from_chunks(&chunks),
            Err(BlockMapCodecError::Unordered)
        ));

        assert!(matches!(
            BlockMap::from_chunks(&[Vec::<u8>::new()]),
            Err(BlockMapCodecError::Truncated)
        ));
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
