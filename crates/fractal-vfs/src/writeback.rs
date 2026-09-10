//! Writeback metadata cache.
//!
//! Removes the per-FUSE-op RPC round-trip from the create / metadata
//! write path. Each inbound op updates `InodeTable` / `DirCache`
//! immediately, enqueues an intent (see `InodeOp` for the kinds), and
//! returns success to the kernel. A background worker drains the queue
//! with one metadata-store publish per intent, so single-threaded create storms
//! (tar -xf, cp -r) pipeline their metadata-store round-trips instead of
//! serialising one per file.
//!
//! Scope: metadata mutations (the `InodeOp` kinds) on exclusive-writer
//! regular files and directories. Default mode only; strict
//! mode routes every op through the synchronous path and never touches
//! this queue. One active generation per inode; a publish CAS conflict
//! taints the inode (deferred EIO on the next fsync / open) rather than
//! rebasing.
//!
//! The queue tracks two things: the pending `InodeOp` intents, and a
//! per-inode "cycle" whose stage lets fsync / unlink / rename / open
//! barriers wait until an inode's dirty publishes have committed.

use bytes::Bytes;
use data_types::object_layout::PosixAttrs;
use fractal_fuse::InodeId;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Pipeline generation number, monotonic per inode. Identifies the cycle
/// a queued intent belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Generation(pub u64);

/// S3 key (the path, e.g. `/foo/bar/baz`). Owned because intents outlive
/// the FUSE op that produced them.
pub type S3Key = String;

/// Intent lifecycle. An intent is removed from the queue once it commits
/// or fails, so only these two live states are ever stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntentState {
    Pending,
    InFlight,
}

/// Inode namespace mutation captured by an intent.
#[derive(Debug, Clone)]
pub enum InodeOp {
    /// Brand-new entry publish (mkdir / symlink / mknod), guarded on
    /// absence so a same-name peer create is not overwritten.
    PutInode {
        parent_key: S3Key,
        name: String,
        layout_bytes: Bytes,
    },
    /// Posix-only update (chmod / chown / utimensat) on an existing
    /// entry. Published via CAS: the fast path guards on the layout
    /// snapshot taken at enqueue, and a conflict re-fetches and folds
    /// `posix` onto the fresh layout, so this can never overwrite a
    /// concurrent data publish with stale blob state. Note the fold
    /// replaces the whole PosixAttrs blob, so two independent writers
    /// racing disjoint posix fields (mode vs uid) is last-writer-wins;
    /// that is out of scope here (exclusive-writer inodes only) and
    /// matches the strict/synchronous path's granularity.
    SetPosix {
        posix: PosixAttrs,
        expected_layout_bytes: Bytes,
        layout_bytes: Bytes,
    },
}

/// One queued namespace mutation.
#[derive(Debug, Clone)]
struct InodeIntent {
    op: InodeOp,
    inode: InodeId,
    state: IntentState,
}

/// State of a per-inode commit cycle. Each cycle is a single publish,
/// so barriers only ever care whether a cycle is still `Dirty` or has
/// reached `Done` (via the worker's publish, or the async close-flush
/// that commits the layout inline in `flush_write_buffer`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileCommitStage {
    Dirty,
    Done,
}

/// Outcome of an `upsert_inode_intent` call. Lets tests and the FUSE-op
/// layer assert how an op composed against existing state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoalesceOutcome {
    /// Brand-new intent installed.
    Inserted,
    /// An existing Pending intent's body was replaced in place.
    ReplacedInPlace,
    /// The queue is closed to new enqueues (unmount drain in progress);
    /// the caller must publish synchronously instead.
    Blocked,
}

/// A pending intent the worker has popped and is about to ship to the metadata store.
/// Carries everything needed to fire the RPC and route the result back
/// via `mark_committed` / `mark_failed`.
#[derive(Debug, Clone)]
pub struct DrainableInodeIntent {
    pub s3_key: S3Key,
    pub inode: InodeId,
    pub generation: Generation,
    pub op: InodeOp,
}

/// The writeback queue. Wrapped behind a `Mutex` so it can be shared
/// across compio tasks; the critical sections are short (a HashMap
/// upsert plus a cycle-stage update).
pub struct WritebackQueue {
    inner: Mutex<QueueInner>,
}

struct QueueInner {
    /// Pending / in-flight intents, keyed by (key, inode, generation).
    /// The inode is part of the key: a FORGET + relookup can hand the
    /// same key to a fresh inode whose generation counter restarts, and
    /// a (key, generation) collision would let the old inode's in-flight
    /// completion silently delete the new inode's pending intent.
    inode_intents: HashMap<(S3Key, InodeId, Generation), InodeIntent>,
    /// Live-intent count per `S3Key`, mirroring `inode_intents` so
    /// `has_pending_intent_for_key` is O(1) instead of scanning every
    /// intent on the hot `vfs_lookup` path.
    intent_key_refs: HashMap<S3Key, u32>,
    /// Per-inode cycle stage, keyed by inode then generation.
    file_pipeline: HashMap<InodeId, BTreeMap<Generation, FileCommitStage>>,
    /// Active generation per inode (the gen new writes route to).
    active_generation: HashMap<InodeId, Generation>,
    /// Inodes tainted by a publish failure; deferred EIO on next fsync.
    tainted: HashSet<InodeId>,
    /// Count of pending + in-flight intents.
    depth: u64,
    /// Set by unmount: `upsert_inode_intent` returns `Blocked` so the
    /// caller publishes synchronously and the drain sees a queue whose
    /// depth is monotonically decreasing.
    enqueue_blocked: bool,
    /// Key prefixes with async enqueues temporarily blocked. Set across a
    /// directory rename so a create racing under the moved subtree falls
    /// back to a synchronous publish (a narrow window) instead of leaving
    /// an intent the worker commits at the pre-rename key much later,
    /// resurrecting a ghost under the old path. Usually empty, so the hot
    /// enqueue path only pays a scan when a rename is in flight.
    blocked_prefixes: Vec<String>,
    /// Inodes the kernel FORGOT while dirty writeback state pinned
    /// their `InodeTable` entry; moved to `reapable` once drained.
    forgotten: HashSet<InodeId>,
    /// Drained `forgotten` inodes awaiting InodeTable removal.
    reapable: Vec<InodeId>,
}

impl Default for WritebackQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl WritebackQueue {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(QueueInner {
                inode_intents: HashMap::new(),
                intent_key_refs: HashMap::new(),
                file_pipeline: HashMap::new(),
                active_generation: HashMap::new(),
                tainted: HashSet::new(),
                depth: 0,
                enqueue_blocked: false,
                blocked_prefixes: Vec::new(),
                forgotten: HashSet::new(),
                reapable: Vec::new(),
            }),
        }
    }

    /// Active generation for `inode`, or `Generation(0)` if none opened.
    pub fn active_generation(&self, inode: InodeId) -> Generation {
        let inner = self.inner.lock();
        inner
            .active_generation
            .get(&inode)
            .copied()
            .unwrap_or(Generation(0))
    }

    /// Open a new `Dirty` cycle for `inode` at `generation` and mark it
    /// the inode's active generation.
    pub fn open_cycle(&self, inode: InodeId, generation: Generation) {
        let mut inner = self.inner.lock();
        inner
            .file_pipeline
            .entry(inode)
            .or_default()
            .insert(generation, FileCommitStage::Dirty);
        inner.active_generation.insert(inode, generation);
    }

    /// Atomically allocate and open the next cycle for `inode`.
    /// Callers that publish real writeback work must use this instead of
    /// reading `active_generation` and then opening a cycle separately.
    ///
    /// Generations are monotonic across the inode's lifetime: pruning a
    /// fully-drained pipeline keeps `active_generation`, so a re-publish
    /// (chmod after create, second close) always opens a *higher*
    /// generation than any barrier a concurrent fsync/unlink captured.
    /// The counter resets only when the inode is forgotten.
    pub fn open_next_cycle(&self, inode: InodeId) -> Generation {
        let mut inner = self.inner.lock();
        let generation = Generation(
            inner
                .active_generation
                .get(&inode)
                .copied()
                .unwrap_or(Generation(0))
                .0
                + 1,
        );
        inner
            .file_pipeline
            .entry(inode)
            .or_default()
            .insert(generation, FileCommitStage::Dirty);
        inner.active_generation.insert(inode, generation);
        generation
    }

    /// Upsert an inode intent. A same-(key, inode, generation) Pending
    /// intent has its body replaced in place (latest wins); otherwise a
    /// fresh intent is installed. Returns `Blocked` without enqueueing
    /// once the unmount drain has closed the queue.
    pub fn upsert_inode_intent(
        &self,
        key: S3Key,
        inode: InodeId,
        generation: Generation,
        op: InodeOp,
    ) -> CoalesceOutcome {
        let mut inner = self.inner.lock();
        if inner.enqueue_blocked || inner.blocked_prefixes.iter().any(|p| key.starts_with(p)) {
            return CoalesceOutcome::Blocked;
        }
        if let Some(existing) = inner
            .inode_intents
            .get_mut(&(key.clone(), inode, generation))
            && existing.state == IntentState::Pending
        {
            existing.op = op;
            return CoalesceOutcome::ReplacedInPlace;
        }
        let prev = inner.inode_intents.insert(
            (key.clone(), inode, generation),
            InodeIntent {
                op,
                inode,
                state: IntentState::Pending,
            },
        );
        if prev.is_none() {
            inner.depth = inner.depth.saturating_add(1);
            *inner.intent_key_refs.entry(key).or_insert(0) += 1;
        }
        CoalesceOutcome::Inserted
    }

    /// Snapshot the count of pending + in-flight intents.
    pub fn depth(&self) -> u64 {
        self.inner.lock().depth
    }

    /// `true` iff any not-yet-committed intent exists for `key`.
    /// `vfs_lookup` uses this to decide whether a metadata-store miss is
    /// authoritative: a key with a pending intent is an entry the worker
    /// has not drained yet (serve read-your-writes from cache), whereas a
    /// metadata-store miss with no pending intent is genuinely gone and must surface
    /// ENOENT.
    pub fn has_pending_intent_for_key(&self, key: &str) -> bool {
        let inner = self.inner.lock();
        inner.intent_key_refs.contains_key(key)
    }

    /// `true` iff a not-yet-committed child create exists under
    /// `parent_key`. Directory emptiness checks use this before
    /// consulting the metadata store: a successful FUSE create has already made the child
    /// visible to the caller even when the worker has not published the
    /// child layout yet.
    pub fn has_pending_child_put_inode_for_parent(&self, parent_key: &str) -> bool {
        let inner = self.inner.lock();
        inner.inode_intents.values().any(|intent| {
            matches!(
                &intent.op,
                InodeOp::PutInode {
                    parent_key: intent_parent,
                    ..
                } if intent_parent == parent_key
            )
        })
    }

    /// Pop up to `max_batch` Pending intents, mark them InFlight, and
    /// return drainable snapshots for the worker to ship. At most one
    /// pending generation per inode is drained at a time, and only after
    /// lower generations on that inode have reached `Done`.
    pub fn drain_pending(&self, max_batch: usize) -> Vec<DrainableInodeIntent> {
        let mut inner = self.inner.lock();

        let mut by_inode: BTreeMap<InodeId, (S3Key, InodeId, Generation)> = BTreeMap::new();
        for ((key, inode, generation), intent) in inner
            .inode_intents
            .iter()
            .filter(|(_, intent)| intent.state == IntentState::Pending)
        {
            let lower_dirty = inner
                .file_pipeline
                .get(&intent.inode)
                .is_some_and(|cycles| {
                    cycles
                        .iter()
                        .any(|(g, stage)| *g < *generation && *stage != FileCommitStage::Done)
                });
            if lower_dirty {
                continue;
            }

            by_inode
                .entry(intent.inode)
                .and_modify(|existing| {
                    if (*generation, key.as_str()) < (existing.2, existing.0.as_str()) {
                        *existing = (key.clone(), *inode, *generation);
                    }
                })
                .or_insert_with(|| (key.clone(), *inode, *generation));
        }

        let candidates: Vec<(S3Key, InodeId, Generation)> =
            by_inode.into_values().take(max_batch).collect();

        let mut out = Vec::new();
        for key in candidates {
            let Some(intent) = inner.inode_intents.get_mut(&key) else {
                continue;
            };
            if intent.state != IntentState::Pending {
                continue;
            }
            intent.state = IntentState::InFlight;
            out.push(DrainableInodeIntent {
                s3_key: key.0.clone(),
                inode: intent.inode,
                generation: key.2,
                op: intent.op.clone(),
            });
        }

        out
    }

    /// Apply a successful publish result: remove the intent and mark the
    /// owning cycle `Done`.
    pub fn mark_committed(&self, key: &str, generation: Generation, inode: InodeId) {
        let mut inner = self.inner.lock();
        if inner
            .inode_intents
            .remove(&(key.to_string(), inode, generation))
            .is_some()
        {
            inner.depth = inner.depth.saturating_sub(1);
            Self::decr_intent_key_ref(&mut inner, key);
        }

        if let Some(cycle) = inner
            .file_pipeline
            .get_mut(&inode)
            .and_then(|c| c.get_mut(&generation))
        {
            *cycle = FileCommitStage::Done;
        }
        Self::prune_done_cycles_locked(&mut inner, inode);
    }

    /// Apply a failed publish result: remove the intent, taint the inode
    /// so a subsequent fsync / open surfaces EIO, and short-circuit the
    /// cycle to `Done`. The application is expected to close and reopen on
    /// the remote winner.
    pub fn mark_failed(&self, key: &str, generation: Generation, inode: InodeId) {
        let mut inner = self.inner.lock();
        if inner
            .inode_intents
            .remove(&(key.to_string(), inode, generation))
            .is_some()
        {
            inner.depth = inner.depth.saturating_sub(1);
            Self::decr_intent_key_ref(&mut inner, key);
        }

        inner.tainted.insert(inode);

        if let Some(cycle) = inner
            .file_pipeline
            .get_mut(&inode)
            .and_then(|c| c.get_mut(&generation))
        {
            *cycle = FileCommitStage::Done;
        }
        Self::prune_done_cycles_locked(&mut inner, inode);
    }

    /// Mark a cycle `Done`. The async close-flush (FUSE_RELEASE) runs the
    /// layout CAS + block writes as one synchronous unit inside
    /// `flush_write_buffer`, so its cycle goes straight from `Dirty` to
    /// `Done`. Idempotent; a missing cycle is a no-op.
    pub fn advance_to_done(&self, inode: InodeId, generation: Generation) {
        let mut inner = self.inner.lock();
        if let Some(cycle) = inner
            .file_pipeline
            .get_mut(&inode)
            .and_then(|c| c.get_mut(&generation))
        {
            *cycle = FileCommitStage::Done;
        }
        Self::prune_done_cycles_locked(&mut inner, inode);
    }

    /// Capture a fsync barrier: the highest dirty (non-`Done`) generation
    /// for `inode`, or `None` when no cycle is dirty (fsync on an idle
    /// inode is a no-op).
    pub fn fsync_barrier(&self, inode: InodeId) -> Option<Generation> {
        let inner = self.inner.lock();
        inner
            .file_pipeline
            .get(&inode)?
            .iter()
            .filter(|(_, stage)| **stage != FileCommitStage::Done)
            .map(|(generation, _)| *generation)
            .max()
    }

    /// `true` iff every cycle on `inode` at generation `<= barrier` has
    /// reached `Done`. The fsync drain loop polls this; a true return
    /// means the drain is complete.
    pub fn cycles_at_or_below_drained(&self, inode: InodeId, barrier: Generation) -> bool {
        let inner = self.inner.lock();
        let Some(cycles) = inner.file_pipeline.get(&inode) else {
            return true;
        };
        cycles
            .iter()
            .filter(|(g, _)| **g <= barrier)
            .all(|(_, stage)| *stage == FileCommitStage::Done)
    }

    /// Snapshot every inode/generation pair that still has an uncommitted
    /// (non-`Done`) cycle. Used by the mount-wide `fsyncdir` drain barrier
    /// to learn what to wait on.
    pub fn snapshot_dirty_cycles(&self) -> Vec<(InodeId, Generation)> {
        let inner = self.inner.lock();
        let mut out = Vec::new();
        for (ino, cycles) in inner.file_pipeline.iter() {
            for (generation, stage) in cycles.iter() {
                if *stage != FileCommitStage::Done {
                    out.push((*ino, *generation));
                }
            }
        }
        out
    }

    /// Record a deferred error against `inode`: taint it so a subsequent
    /// fsync / close surfaces EIO.
    pub fn record_failure(&self, inode: InodeId) {
        let mut inner = self.inner.lock();
        inner.tainted.insert(inode);
    }

    /// `true` iff a publish failure has tainted this inode. Tainted inodes
    /// must close-and-reopen to observe the remote winner.
    pub fn is_tainted(&self, inode: InodeId) -> bool {
        let inner = self.inner.lock();
        inner.tainted.contains(&inode)
    }

    /// Consume a taint: returns `true` and clears it if set. Deferred
    /// writeback errors are reported once (like the kernel's errseq
    /// semantics); leaving the taint in place would EIO every later
    /// open/fsync of the inode forever with no recovery path.
    pub fn take_taint(&self, inode: InodeId) -> bool {
        let mut inner = self.inner.lock();
        let removed = inner.tainted.remove(&inode);
        if removed {
            Self::queue_reapable_if_idle_locked(&mut inner, inode);
        }
        removed
    }

    /// Snapshot the currently-tainted inodes without consuming them.
    /// The fsyncdir sweep uses this to find taints under a directory
    /// prefix; consumption stays with `take_taint`.
    pub fn tainted_inodes(&self) -> Vec<InodeId> {
        let inner = self.inner.lock();
        let mut out: Vec<InodeId> = inner.tainted.iter().copied().collect();
        out.sort_unstable();
        out
    }

    /// Consume every deferred publish error. Mount-wide barriers use this
    /// after all dirty cycles have drained, including failures whose cycles
    /// were already pruned before the barrier started.
    pub fn take_all_taints(&self) -> Vec<InodeId> {
        let mut inner = self.inner.lock();
        let mut tainted: Vec<InodeId> = inner.tainted.drain().collect();
        tainted.sort_unstable();
        for inode in &tainted {
            Self::queue_reapable_if_idle_locked(&mut inner, *inode);
        }
        tainted
    }

    /// Inodes with a live (pending or in-flight) intent for `key`. The
    /// delete/rename drains use this in addition to the InodeTable
    /// lookup: an intent outlives its entry when a FORGET raced the
    /// enqueue, and skipping the drain would let the worker resurrect
    /// the name after the delete.
    pub fn intent_inodes_for_key(&self, key: &str) -> Vec<InodeId> {
        let inner = self.inner.lock();
        let mut out: Vec<InodeId> = inner
            .inode_intents
            .keys()
            .filter(|(k, _, _)| k == key)
            .map(|(_, inode, _)| *inode)
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Inodes with live intents whose key is inside `prefix`.
    /// Directory rename uses this to drain child publishes before moving the
    /// subtree, so a queued child cannot commit at the old prefix after the
    /// rename.
    pub fn intent_inodes_for_key_prefix(&self, prefix: &str) -> Vec<InodeId> {
        let inner = self.inner.lock();
        let mut out: Vec<InodeId> = inner
            .inode_intents
            .keys()
            .filter(|(k, _, _)| k.starts_with(prefix))
            .map(|(_, inode, _)| *inode)
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// `true` iff the queue still tracks dirty state for `inode` (any
    /// non-pruned cycle; every live intent implies one). Used to pin
    /// the InodeTable entry across a kernel FORGET.
    pub fn has_live_state(&self, inode: InodeId) -> bool {
        let inner = self.inner.lock();
        Self::inode_has_live_state_locked(&inner, inode)
    }

    /// Record that the kernel forgot `inode` while writeback state was
    /// still pinning its InodeTable entry. Once the last cycle drains,
    /// the inode surfaces via `take_reapable` for deferred removal. An
    /// already-drained inode is queued immediately.
    pub fn mark_forgotten(&self, inode: InodeId) {
        let mut inner = self.inner.lock();
        if Self::inode_has_live_state_locked(&inner, inode) {
            inner.forgotten.insert(inode);
        } else {
            inner.reapable.push(inode);
        }
    }

    /// Take the inodes whose deferred FORGET is ready to finish (their
    /// writeback state drained after `mark_forgotten`).
    pub fn take_reapable(&self) -> Vec<InodeId> {
        let mut inner = self.inner.lock();
        std::mem::take(&mut inner.reapable)
    }

    /// Clear the taint. The unlink / rmdir path calls this once the
    /// name is being removed: the failed publish is moot (the inode is
    /// going away) and the deferred EIO must not block the delete.
    /// Returns `true` if a taint was cleared, so the delete path knows
    /// the entry's create publish failed (the metadata store has nothing) and a metadata-store
    /// miss must not surface as ENOENT for a locally-visible name.
    pub fn clear_taint(&self, inode: InodeId) -> bool {
        let mut inner = self.inner.lock();
        let removed = inner.tainted.remove(&inode);
        if removed {
            Self::queue_reapable_if_idle_locked(&mut inner, inode);
        }
        removed
    }

    /// Block (or unblock) new enqueues. The unmount path sets this before
    /// draining so the queue depth is monotonically decreasing.
    pub fn set_enqueue_blocked(&self, blocked: bool) {
        let mut inner = self.inner.lock();
        inner.enqueue_blocked = blocked;
    }

    /// Block async enqueues for keys under `prefix`. Held across a
    /// directory rename so a concurrent create under the moved subtree
    /// takes the synchronous publish fallback instead of an async intent
    /// the worker would later commit at the stale pre-rename key. Paired
    /// with `unblock_prefix`; safe to nest (each blocker pushes its own).
    pub fn block_prefix(&self, prefix: &str) {
        let mut inner = self.inner.lock();
        inner.blocked_prefixes.push(prefix.to_string());
    }

    /// Release one `block_prefix` hold for `prefix`.
    pub fn unblock_prefix(&self, prefix: &str) {
        let mut inner = self.inner.lock();
        if let Some(pos) = inner.blocked_prefixes.iter().position(|p| p == prefix) {
            inner.blocked_prefixes.remove(pos);
        }
    }

    /// Drop retained state after VFS has released an idle inode.
    pub fn prune_inode_if_idle(&self, inode: InodeId) {
        let mut inner = self.inner.lock();
        Self::prune_done_cycles_locked(&mut inner, inode);
        if !Self::inode_has_live_state_locked(&inner, inode) {
            inner.active_generation.remove(&inode);
            inner.forgotten.remove(&inode);
        }
    }

    fn inode_has_live_state_locked(inner: &QueueInner, inode: InodeId) -> bool {
        inner.file_pipeline.contains_key(&inode) || inner.tainted.contains(&inode)
    }

    fn queue_reapable_if_idle_locked(inner: &mut QueueInner, inode: InodeId) {
        if !Self::inode_has_live_state_locked(inner, inode) && inner.forgotten.remove(&inode) {
            inner.reapable.push(inode);
        }
    }

    fn prune_done_cycles_locked(inner: &mut QueueInner, inode: InodeId) {
        let remove_pipeline = if let Some(cycles) = inner.file_pipeline.get_mut(&inode) {
            cycles.retain(|_, stage| *stage != FileCommitStage::Done);
            cycles.is_empty()
        } else {
            false
        };
        if remove_pipeline {
            inner.file_pipeline.remove(&inode);
            // Keep `active_generation` so `open_next_cycle` stays monotonic
            // across the inode's lifetime: a re-publish after every cycle
            // drained must open a *higher* generation, never reuse a number
            // an in-flight fsync/unlink barrier already captured. It is
            // reclaimed only when the inode is forgotten
            // (`prune_inode_if_idle`).
        }
        // A deferred FORGET waits on the pipeline; drained means the
        // InodeTable entry can go.
        Self::queue_reapable_if_idle_locked(inner, inode);
    }

    fn decr_intent_key_ref(inner: &mut QueueInner, key: &str) {
        if let Some(count) = inner.intent_key_refs.get_mut(key) {
            *count -= 1;
            if *count == 0 {
                inner.intent_key_refs.remove(key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(name: &str) -> InodeOp {
        put_under("/", name)
    }

    fn put_under(parent: &str, name: &str) -> InodeOp {
        InodeOp::PutInode {
            parent_key: parent.to_string(),
            name: name.to_string(),
            layout_bytes: Bytes::from_static(b"layout"),
        }
    }

    #[test]
    fn open_cycle_sets_active_generation_and_dirty_stage() {
        let q = WritebackQueue::new();
        assert_eq!(q.active_generation(InodeId(7)), Generation(0));
        q.open_cycle(InodeId(7), Generation(1));
        assert_eq!(q.active_generation(InodeId(7)), Generation(1));
        // A freshly opened cycle is dirty (non-Done), so it is its own
        // fsync barrier.
        assert_eq!(q.fsync_barrier(InodeId(7)), Some(Generation(1)));
    }

    #[test]
    fn open_next_cycle_allocates_under_the_queue_lock() {
        let q = WritebackQueue::new();
        assert_eq!(q.open_next_cycle(InodeId(7)), Generation(1));
        assert_eq!(q.open_next_cycle(InodeId(7)), Generation(2));
        assert_eq!(q.active_generation(InodeId(7)), Generation(2));
        assert_eq!(q.fsync_barrier(InodeId(7)), Some(Generation(2)));
    }

    #[test]
    fn generation_stays_monotonic_after_a_cycle_fully_drains() {
        let q = WritebackQueue::new();
        let g1 = q.open_next_cycle(InodeId(1));
        q.advance_to_done(InodeId(1), g1);
        // Pipeline is now pruned, but the next cycle must not reuse g1.
        let g2 = q.open_next_cycle(InodeId(1));
        assert_eq!(g2, Generation(2), "generation must not reset on prune");
        assert_eq!(q.fsync_barrier(InodeId(1)), Some(Generation(2)));
        // A forget reclaims the counter.
        q.advance_to_done(InodeId(1), g2);
        q.prune_inode_if_idle(InodeId(1));
        assert_eq!(q.open_next_cycle(InodeId(1)), Generation(1));
    }

    #[test]
    fn clear_taint_unblocks_the_inode() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        q.upsert_inode_intent("/a".into(), InodeId(1), Generation(1), put("a"));
        q.drain_pending(16);
        q.mark_failed("/a", Generation(1), InodeId(1));
        assert!(q.is_tainted(InodeId(1)));
        q.clear_taint(InodeId(1));
        assert!(!q.is_tainted(InodeId(1)));
    }

    #[test]
    fn has_pending_intent_tracks_multiple_generations_per_key() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        q.open_cycle(InodeId(1), Generation(2));
        q.upsert_inode_intent("/a".into(), InodeId(1), Generation(1), put("a"));
        q.upsert_inode_intent("/a".into(), InodeId(1), Generation(2), put("a"));
        assert!(q.has_pending_intent_for_key("/a"));
        // Committing one generation must not clear the key while the other
        // intent is still live.
        q.mark_committed("/a", Generation(1), InodeId(1));
        assert!(q.has_pending_intent_for_key("/a"));
        q.mark_committed("/a", Generation(2), InodeId(1));
        assert!(!q.has_pending_intent_for_key("/a"));
    }

    #[test]
    fn upsert_inserts_then_replaces_pending_in_place() {
        let q = WritebackQueue::new();
        assert_eq!(
            q.upsert_inode_intent("/a".into(), InodeId(1), Generation(1), put("a")),
            CoalesceOutcome::Inserted
        );
        assert_eq!(q.depth(), 1);
        // Same (key, gen) while Pending: replace body, depth unchanged.
        assert_eq!(
            q.upsert_inode_intent("/a".into(), InodeId(1), Generation(1), put("a")),
            CoalesceOutcome::ReplacedInPlace
        );
        assert_eq!(q.depth(), 1);
    }

    #[test]
    fn drain_pops_pending_and_marks_intent_in_flight() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        q.upsert_inode_intent("/a".into(), InodeId(1), Generation(1), put("a"));

        let drained = q.drain_pending(16);
        assert_eq!(drained.len(), 1, "one pending intent should drain");
        assert_eq!(drained[0].s3_key, "/a");
        assert_eq!(drained[0].inode, InodeId(1));
        assert_eq!(drained[0].generation, Generation(1));

        // A second drain sees nothing new (the intent is now InFlight).
        assert!(q.drain_pending(16).is_empty());
    }

    #[test]
    fn drain_pending_drains_one_generation_per_inode_in_order() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        q.open_cycle(InodeId(1), Generation(2));
        q.upsert_inode_intent("/a".into(), InodeId(1), Generation(2), put("a"));
        q.upsert_inode_intent("/a".into(), InodeId(1), Generation(1), put("a"));

        let first = q.drain_pending(16);
        assert_eq!(first.len(), 1, "only the oldest generation should drain");
        assert_eq!(first[0].generation, Generation(1));
        assert!(
            q.drain_pending(16).is_empty(),
            "newer generation must wait while the older one is in flight"
        );

        q.mark_committed("/a", Generation(1), InodeId(1));
        let second = q.drain_pending(16);
        assert_eq!(second.len(), 1, "next generation should drain after commit");
        assert_eq!(second[0].generation, Generation(2));
    }

    #[test]
    fn mark_committed_drives_cycle_to_done_and_drops_depth() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        q.upsert_inode_intent("/a".into(), InodeId(1), Generation(1), put("a"));
        q.drain_pending(16);
        assert_eq!(q.depth(), 1);

        q.mark_committed("/a", Generation(1), InodeId(1));
        assert_eq!(q.depth(), 0);
        assert!(
            q.cycles_at_or_below_drained(InodeId(1), Generation(1)),
            "committed cycle should read as drained"
        );
        assert_eq!(
            q.fsync_barrier(InodeId(1)),
            None,
            "no dirty cycle after commit"
        );
    }

    #[test]
    fn mark_failed_taints_inode_and_drives_to_done() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        q.upsert_inode_intent("/a".into(), InodeId(1), Generation(1), put("a"));
        q.drain_pending(16);

        assert!(!q.is_tainted(InodeId(1)));
        q.mark_failed("/a", Generation(1), InodeId(1));
        assert!(q.is_tainted(InodeId(1)));
        assert!(q.cycles_at_or_below_drained(InodeId(1), Generation(1)));
        assert_eq!(q.depth(), 0);
    }

    #[test]
    fn advance_to_done_clears_the_barrier() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        assert_eq!(q.fsync_barrier(InodeId(1)), Some(Generation(1)));
        q.advance_to_done(InodeId(1), Generation(1));
        assert_eq!(q.fsync_barrier(InodeId(1)), None);
    }

    #[test]
    fn fsync_barrier_returns_max_dirty_generation() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        q.open_cycle(InodeId(1), Generation(2));
        assert_eq!(q.fsync_barrier(InodeId(1)), Some(Generation(2)));
        // Draining the newer gen leaves the older one as the barrier.
        q.advance_to_done(InodeId(1), Generation(2));
        assert_eq!(q.fsync_barrier(InodeId(1)), Some(Generation(1)));
    }

    #[test]
    fn has_pending_intent_for_key_tracks_lifecycle() {
        let q = WritebackQueue::new();
        assert!(!q.has_pending_intent_for_key("/a"));
        q.open_cycle(InodeId(1), Generation(1));
        q.upsert_inode_intent("/a".into(), InodeId(1), Generation(1), put("a"));
        assert!(q.has_pending_intent_for_key("/a"));
        // Still tracked while InFlight.
        q.drain_pending(16);
        assert!(q.has_pending_intent_for_key("/a"));
        // Cleared once committed.
        q.mark_committed("/a", Generation(1), InodeId(1));
        assert!(!q.has_pending_intent_for_key("/a"));
    }

    #[test]
    fn pending_child_marks_parent_non_empty() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(2), Generation(1));
        q.upsert_inode_intent(
            "/dir/child".into(),
            InodeId(2),
            Generation(1),
            put_under("/dir/", "child"),
        );
        assert!(q.has_pending_child_put_inode_for_parent("/dir/"));
        assert!(!q.has_pending_child_put_inode_for_parent("/other/"));
        q.drain_pending(16);
        q.mark_committed("/dir/child", Generation(1), InodeId(2));
        assert!(!q.has_pending_child_put_inode_for_parent("/dir/"));
    }

    #[test]
    fn snapshot_dirty_cycles_lists_only_non_done() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        q.open_cycle(InodeId(2), Generation(1));
        q.advance_to_done(InodeId(2), Generation(1));
        let dirty = q.snapshot_dirty_cycles();
        assert_eq!(dirty, vec![(InodeId(1), Generation(1))]);
    }

    #[test]
    fn enqueue_blocked_rejects_new_intents() {
        let q = WritebackQueue::new();
        q.set_enqueue_blocked(true);
        assert_eq!(
            q.upsert_inode_intent("/a".into(), InodeId(1), Generation(1), put("a")),
            CoalesceOutcome::Blocked
        );
        assert_eq!(q.depth(), 0, "blocked upsert must not enqueue");
        q.set_enqueue_blocked(false);
        assert_eq!(
            q.upsert_inode_intent("/a".into(), InodeId(1), Generation(1), put("a")),
            CoalesceOutcome::Inserted
        );
    }

    #[test]
    fn blocked_prefix_rejects_only_matching_keys() {
        let q = WritebackQueue::new();
        q.block_prefix("/dir/");
        // A create under the blocked subtree is rejected (falls back to a
        // synchronous publish at the caller).
        assert_eq!(
            q.upsert_inode_intent("/dir/sub".into(), InodeId(1), Generation(1), put("sub")),
            CoalesceOutcome::Blocked
        );
        // A create outside the subtree is unaffected.
        assert_eq!(
            q.upsert_inode_intent("/other".into(), InodeId(2), Generation(1), put("other")),
            CoalesceOutcome::Inserted
        );
        // Releasing the hold re-opens the subtree.
        q.unblock_prefix("/dir/");
        assert_eq!(
            q.upsert_inode_intent("/dir/sub2".into(), InodeId(3), Generation(1), put("sub2")),
            CoalesceOutcome::Inserted
        );
    }

    #[test]
    fn take_taint_reports_once() {
        let q = WritebackQueue::new();
        q.record_failure(InodeId(1));
        assert!(q.is_tainted(InodeId(1)));
        assert!(q.take_taint(InodeId(1)), "first consumer sees the taint");
        assert!(!q.take_taint(InodeId(1)), "taint is report-once");
        assert!(!q.is_tainted(InodeId(1)));
    }

    #[test]
    fn taint_counts_as_live_state_until_reported() {
        let q = WritebackQueue::new();
        assert!(!q.has_live_state(InodeId(1)));
        q.record_failure(InodeId(1));
        assert!(
            q.has_live_state(InodeId(1)),
            "deferred EIO must pin the inode"
        );
        q.mark_forgotten(InodeId(1));
        assert!(
            q.take_reapable().is_empty(),
            "tainted forgotten inode is not reapable yet"
        );
        assert!(q.take_taint(InodeId(1)), "reporting EIO consumes the taint");
        assert_eq!(q.take_reapable(), vec![InodeId(1)]);
        assert!(!q.has_live_state(InodeId(1)));
    }

    #[test]
    fn clear_taint_releases_forgotten_inode() {
        let q = WritebackQueue::new();
        q.record_failure(InodeId(1));
        q.mark_forgotten(InodeId(1));
        q.clear_taint(InodeId(1));
        assert_eq!(q.take_reapable(), vec![InodeId(1)]);
        assert!(!q.has_live_state(InodeId(1)));
    }

    #[test]
    fn take_all_taints_reports_all_once_and_reaps() {
        let q = WritebackQueue::new();
        q.record_failure(InodeId(2));
        q.record_failure(InodeId(1));
        q.mark_forgotten(InodeId(1));

        assert_eq!(q.take_all_taints(), vec![InodeId(1), InodeId(2)]);
        assert!(q.take_all_taints().is_empty());
        assert_eq!(q.take_reapable(), vec![InodeId(1)]);
        assert!(!q.has_live_state(InodeId(1)));
        assert!(!q.has_live_state(InodeId(2)));
    }

    #[test]
    fn same_key_generation_from_different_inodes_coexist() {
        // FORGET + relookup can restart the generation counter on a
        // fresh inode for the same key; the old inode's completion must
        // not delete the new inode's intent.
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        q.upsert_inode_intent("/a".into(), InodeId(1), Generation(1), put("a"));
        q.drain_pending(16);
        q.open_cycle(InodeId(2), Generation(1));
        assert_eq!(
            q.upsert_inode_intent("/a".into(), InodeId(2), Generation(1), put("a")),
            CoalesceOutcome::Inserted,
            "new inode's intent must not clobber the in-flight one"
        );
        assert_eq!(q.depth(), 2);
        q.mark_committed("/a", Generation(1), InodeId(1));
        assert_eq!(q.depth(), 1, "only inode 1's intent is removed");
        assert!(q.has_pending_intent_for_key("/a"));
        let drained = q.drain_pending(16);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].inode, InodeId(2));
    }

    #[test]
    fn intent_inodes_for_key_lists_owners() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        q.upsert_inode_intent("/a".into(), InodeId(1), Generation(1), put("a"));
        assert_eq!(q.intent_inodes_for_key("/a"), vec![InodeId(1)]);
        assert!(q.intent_inodes_for_key("/b").is_empty());
        // Still listed while InFlight.
        q.drain_pending(16);
        assert_eq!(q.intent_inodes_for_key("/a"), vec![InodeId(1)]);
        q.mark_committed("/a", Generation(1), InodeId(1));
        assert!(q.intent_inodes_for_key("/a").is_empty());
    }

    #[test]
    fn intent_inodes_for_key_prefix_lists_children() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        q.open_cycle(InodeId(2), Generation(1));
        q.open_cycle(InodeId(3), Generation(1));
        q.upsert_inode_intent(
            "/dir/a".into(),
            InodeId(1),
            Generation(1),
            put_under("/dir/", "a"),
        );
        q.upsert_inode_intent(
            "/dir/sub/b".into(),
            InodeId(2),
            Generation(1),
            put_under("/dir/sub/", "b"),
        );
        q.upsert_inode_intent(
            "/other/c".into(),
            InodeId(3),
            Generation(1),
            put_under("/other/", "c"),
        );

        assert_eq!(
            q.intent_inodes_for_key_prefix("/dir/"),
            vec![InodeId(1), InodeId(2)]
        );
        q.drain_pending(16);
        assert_eq!(
            q.intent_inodes_for_key_prefix("/dir/"),
            vec![InodeId(1), InodeId(2)],
            "in-flight intents must still be drained"
        );
        q.mark_committed("/dir/a", Generation(1), InodeId(1));
        assert_eq!(q.intent_inodes_for_key_prefix("/dir/"), vec![InodeId(2)]);
    }

    #[test]
    fn forgotten_inode_becomes_reapable_after_drain() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        assert!(q.has_live_state(InodeId(1)));
        q.mark_forgotten(InodeId(1));
        assert!(q.take_reapable().is_empty(), "still pinned by the cycle");
        q.advance_to_done(InodeId(1), Generation(1));
        assert_eq!(q.take_reapable(), vec![InodeId(1)]);
        assert!(q.take_reapable().is_empty(), "reap list is consumed");
        // An idle inode is reapable immediately.
        q.mark_forgotten(InodeId(2));
        assert_eq!(q.take_reapable(), vec![InodeId(2)]);
    }

    #[test]
    fn cycles_at_or_below_drained_ignores_higher_generations() {
        let q = WritebackQueue::new();
        q.open_cycle(InodeId(1), Generation(1));
        q.open_cycle(InodeId(1), Generation(2));
        q.advance_to_done(InodeId(1), Generation(1));
        // Gen 1 is done; barrier at gen 1 is satisfied even though gen 2
        // is still dirty.
        assert!(q.cycles_at_or_below_drained(InodeId(1), Generation(1)));
        assert!(!q.cycles_at_or_below_drained(InodeId(1), Generation(2)));
    }
}
