//! Per-handle sparse write buffer: the in-memory staging area a write-mode
//! handle accumulates into before the override flush publishes it.

use bytes::Bytes;

/// Per-block content intent for the sparse WriteBuffer.
///
/// Blocks NOT in the map are implicitly "Keep": no buffered work, BSS is
/// authoritative. The override flush uploads only `Rewrite` blocks (in
/// place at the bumped blob_version), replays `Delete` intents as
/// versioned block deletes, and never touches "Keep"/absent blocks. The
/// sparse buffer keeps in-memory ops O(1), avoids whole-file preload on
/// open, and serves dirty-handle reads per block.
#[derive(Debug, Clone)]
pub(crate) enum BlockState {
    /// Definitive new bytes for this block. Origin: `vfs_write`, a shrink
    /// tail-zero, or a punch-hole partial edge. The override flush uploads
    /// these (zero-padded to block_size) at the new blob_version.
    Rewrite(Bytes),
    /// PUNCH_HOLE intent: the override flush schedules a versioned
    /// `delete_block` so the BSS entry is dropped at the new blob_version.
    /// Reads (dirty-handle merge and post-flush via `BlockNotFound`) treat
    /// the block as zeros. Distinguished from a plain hole because a
    /// punched block sits inside the file's logical range and the deletion
    /// must be replayed on flush even with no `Rewrite` content.
    Delete,
}

pub(crate) struct WriteBuffer {
    /// Logical file size (includes holes). Authoritative within this
    /// handle session for stat / read clamping until flush commits.
    pub(crate) file_size: u64,
    /// True if `file_size` differs from the committed layout size at open
    /// time, or any block intent was buffered. Flush-eligibility predicate.
    pub(crate) size_changed: bool,
    /// Blob guid of the file at open time; used to lazy-load committed
    /// bytes for partial-block edits and dirty reads, and reused by the
    /// override flush. `None` for brand-new files.
    pub(crate) existing_blob_guid: Option<data_types::DataBlobGuid>,
    /// Block size copied from the committed layout (or DEFAULT for new
    /// files).
    pub(crate) block_size: u32,
    /// Per-block content intents, keyed by block index.
    pub(crate) blocks: std::collections::BTreeMap<u32, BlockState>,
    /// True if any flush-worthy work is buffered.
    pub(crate) dirty: bool,
    /// Smallest `ceil(new_size / block_size)` reached by any shrink in this
    /// session. Blocks at index `>= eof_low_watermark` had their committed
    /// BSS data logically destroyed by the shrink and must read as zeros
    /// until the flush trim deletes them, even if a later grow brings the
    /// index back into the file. Reset to `None` only on a successful
    /// flush. Without it, `truncate(small); write(past old EOF)` would
    /// lazy-load pre-shrink bytes and resurrect data POSIX requires zeroed.
    pub(crate) eof_low_watermark: Option<u32>,
    /// `committed_block_count` pinned at the FIRST shrink this session.
    /// Pairs with `eof_low_watermark` to bound the EOF-trim across
    /// post-CAS-failure retries: the flush promotes the committed size to
    /// the smaller new size, so recomputing the upper bound from the layout
    /// on retry would lose the original committed bound. Reset on flush.
    pub(crate) trim_upper: Option<u32>,
    /// Block indices fallocate has reserved. On flush these become
    /// `ReserveBlocks` (single-op, no batch) for blocks not superseded by a
    /// `Rewrite`/`Delete`. Reads and `lseek(SEEK_DATA)` treat reserved
    /// blocks as logical-data per Linux convention even before flush.
    pub(crate) pending_reservations: std::collections::BTreeSet<u32>,
    /// Committed fallocate claims this handle published: block ->
    /// reserved version. Lets a later fallocate skip re-claiming a block
    /// this handle already holds a claim for. A write of the block takes
    /// a fresh burned identity; the superseded claim is reclaimed by the
    /// touched-block sweep.
    pub(crate) committed_reservations: std::collections::BTreeMap<u32, u64>,
}

impl WriteBuffer {
    pub(crate) fn new(
        existing_blob_guid: Option<data_types::DataBlobGuid>,
        file_size: u64,
        block_size: u32,
    ) -> Self {
        Self {
            file_size,
            size_changed: false,
            existing_blob_guid,
            block_size,
            blocks: std::collections::BTreeMap::new(),
            dirty: false,
            eof_low_watermark: None,
            trim_upper: None,
            pending_reservations: std::collections::BTreeSet::new(),
            committed_reservations: std::collections::BTreeMap::new(),
        }
    }

    /// Drop per-block intents and reservations past the new EOF (shrink).
    pub(crate) fn drop_blocks_past(&mut self, new_last_block_excl: u32) {
        self.blocks.retain(|b, _| *b < new_last_block_excl);
        self.pending_reservations
            .retain(|b| *b < new_last_block_excl);
    }

    /// True when block `b` sits in a range whose committed BSS bytes were
    /// destroyed by a shrink earlier this session; lazy-load and
    /// dirty-read paths must return zeros for such blocks.
    pub(crate) fn block_destroyed_by_shrink(&self, b: u32) -> bool {
        self.eof_low_watermark.is_some_and(|low| b >= low)
    }
}
