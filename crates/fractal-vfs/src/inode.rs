use dashmap::DashMap;
use data_types::object_layout::{ObjectLayout, ObjectState, PosixAttrs};
use fractal_fuse::InodeId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use uuid::Uuid;

pub const ROOT_INODE: InodeId = InodeId(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryType {
    File,
    Directory,
}

/// Pull the embedded `PosixAttrs` out of an `ObjectLayout`. Returns the
/// zero value for layout shapes that don't carry one (Indirect, or
/// Mpu(Uploading)) so callers can treat that as the
/// "uninitialised, fall back to defaults" sentinel.
pub fn layout_posix(layout: &ObjectLayout) -> PosixAttrs {
    match &layout.state {
        ObjectState::Normal(data) => data
            .core_meta_data
            .posix
            .as_deref()
            .copied()
            .unwrap_or_default(),
        ObjectState::Mpu(data_types::object_layout::MpuState::Completed(core)) => {
            core.posix.as_deref().copied().unwrap_or_default()
        }
        ObjectState::Symlink(data) => data
            .core_meta_data
            .posix
            .as_deref()
            .copied()
            .unwrap_or_default(),
        ObjectState::Special(data) => data
            .core_meta_data
            .posix
            .as_deref()
            .copied()
            .unwrap_or_default(),
        ObjectState::Directory(data) => data.posix,
        _ => PosixAttrs::default(),
    }
}

/// Set the embedded `PosixAttrs` of an `ObjectLayout`, returning the
/// updated layout. No-op for shapes that don't carry posix
/// (Indirect, Mpu(Uploading)); used by `vfs_setattr_posix`'s
/// queue-side persistence path so a standalone chmod / chown / utime
/// against a file with no pending flush still survives a
/// forget+relookup.
pub fn layout_with_posix(mut layout: ObjectLayout, new_posix: PosixAttrs) -> ObjectLayout {
    use data_types::object_layout::MpuState;
    match &mut layout.state {
        ObjectState::Normal(data) => data.core_meta_data.posix = Some(Box::new(new_posix)),
        ObjectState::Mpu(MpuState::Completed(core)) => core.posix = Some(Box::new(new_posix)),
        ObjectState::Symlink(data) => data.core_meta_data.posix = Some(Box::new(new_posix)),
        ObjectState::Special(data) => data.core_meta_data.posix = Some(Box::new(new_posix)),
        ObjectState::Directory(data) => data.posix = new_posix,
        _ => {}
    }
    layout
}

pub struct InodeEntry {
    pub s3_key: String,
    pub entry_type: EntryType,
    pub layout: Option<ObjectLayout>,
    pub cache_expiry: Instant,
    /// In-memory POSIX attrs. On lookup we seed it from the layout's
    /// embedded `PosixAttrs`; setattr mutates this directly. The next
    /// flush reads it back and folds it into the layout it serialises
    /// so the changes survive the close-time round-trip.
    pub posix: PosixAttrs,
    /// `false` when `posix` is a placeholder default, not the inode's
    /// authoritative owner/mode. A directory materialised from a
    /// delimiter listing (readdir common-prefix, lookup prefix-listing
    /// fallback) has no layout to seed from, so its `posix` defaults to
    /// uid 0 / mode 0; trusting that default makes the setattr owner
    /// check reject the real owner with EPERM. The async attr paths
    /// (`vfs_getattr`, `lookup_or_insert` when a marker arrives) refresh
    /// `posix` from the NSS marker and flip this true.
    pub posix_known: bool,
    /// `true` once unlink/rmdir has removed the name mapping for this
    /// inode and issued the NSS delete. The kernel's dcache may still
    /// hold a stale dentry pointing at this inode; subsequent FUSE
    /// SETATTR / WRITE / RELEASE ops via that dentry must NOT write
    /// the inode's bytes back to NSS, otherwise the unlinked file
    /// resurrects (deterministic EEXIST on the next create at the same
    /// name). Cleared on lookup_or_insert when a new inode is
    /// allocated for the same key.
    pub name_removed: bool,
    /// In-memory atime override in nanoseconds since the Unix epoch.
    /// `0` means "no explicit atime set; mirror mtime in stat replies".
    /// Persisted `PosixAttrs` deliberately omits atime (we never bump
    /// it on `read(2)`), so this field carries the explicit value an
    /// `utimensat(2)` user supplied. Volatile across forget+relookup,
    /// which matches POSIX's latitude and is enough for the
    /// stat-immediately-after contract.
    pub atime_ns: u64,
    /// `Some(uuid)` once this inode has been promoted to a hardlink:
    /// its real layout lives in the `#hardlink/<uuid>` `InodeRecord`,
    /// and `layout` caches the resolved real layout (never an
    /// `Indirect` redirect). `None` for an ordinary single-named file.
    pub inode_id: Option<Uuid>,
    refcount: AtomicU64,
}

impl InodeEntry {
    fn new(s3_key: String, entry_type: EntryType, layout: Option<ObjectLayout>) -> Self {
        let posix = layout.as_ref().map(layout_posix).unwrap_or_default();
        // Authoritative only when seeded from a layout; a `None` seed
        // leaves `posix` at its default placeholder.
        let posix_known = layout.is_some();
        Self {
            s3_key,
            entry_type,
            layout,
            cache_expiry: Instant::now(),
            posix,
            posix_known,
            name_removed: false,
            atime_ns: 0,
            inode_id: None,
            refcount: AtomicU64::new(1),
        }
    }

    pub fn increment_ref(&self) {
        self.refcount.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements refcount by nlookup. Returns true if entry should be removed.
    pub fn forget(&self, nlookup: u64) -> bool {
        let prev = self.refcount.fetch_sub(nlookup, Ordering::Relaxed);
        prev <= nlookup
    }
}

/// Outcome of an `InodeTable::forget` refcount decrement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForgetOutcome {
    /// Refcount still positive; entry stays.
    Live,
    /// Refcount hit zero and the entry was removed.
    Removed,
    /// Refcount hit zero but a pin kept the entry (`keep_if_zero`).
    KeptZeroed,
}

pub struct InodeTable {
    map: DashMap<InodeId, InodeEntry>,
    next_ino: AtomicU64,
    // Reverse map: (s3_key, entry_type) -> inode for lookup dedup.
    // EntryType is included to avoid aliasing between files and directories
    // with the same key (e.g., a file at "dir/" vs a directory prefix "dir/").
    key_to_ino: DashMap<(String, EntryType), InodeId>,
}

impl Default for InodeTable {
    fn default() -> Self {
        Self::new()
    }
}

impl InodeTable {
    pub fn new() -> Self {
        let table = Self {
            map: DashMap::new(),
            next_ino: AtomicU64::new(2), // 1 is root
            key_to_ino: DashMap::new(),
        };
        // Insert root inode. Root key is "/" matching NSS key convention
        // where all keys are stored with a leading "/".
        table.map.insert(
            ROOT_INODE,
            InodeEntry {
                s3_key: "/".to_string(),
                entry_type: EntryType::Directory,
                layout: None,
                cache_expiry: Instant::now(),
                posix: PosixAttrs::default(),
                // Root has no NSS marker; make_dir_attr special-cases it
                // (mode 0o777) and it is never owner-checked, so treat its
                // placeholder posix as authoritative to skip marker fetches.
                posix_known: true,
                name_removed: false,
                atime_ns: 0,
                inode_id: None,
                refcount: AtomicU64::new(u64::MAX), // root never gets forgotten
            },
        );
        table
            .key_to_ino
            .insert(("/".to_string(), EntryType::Directory), ROOT_INODE);
        table
    }

    /// Look up or insert an inode for a given s3_key. Returns (ino, is_new).
    pub fn lookup_or_insert(
        &self,
        s3_key: &str,
        entry_type: EntryType,
        layout: Option<ObjectLayout>,
    ) -> (InodeId, bool) {
        let dedup_key = (s3_key.to_string(), entry_type);
        // Check if we already have this key
        if let Some(existing_ino) = self.key_to_ino.get(&dedup_key) {
            let ino = *existing_ino;
            if let Some(entry) = self.map.get(&ino) {
                entry.increment_ref();
                // Update layout if provided
                drop(entry);
                if let Some(new_layout) = layout
                    && let Some(mut entry) = self.map.get_mut(&ino)
                {
                    // Seed authoritative posix into an entry whose owner/mode
                    // is still a listing-materialised placeholder. Guarded on
                    // `!posix_known` so a real (possibly locally-mutated,
                    // not-yet-flushed) posix is never clobbered by a stale
                    // marker.
                    if !entry.posix_known {
                        entry.posix = layout_posix(&new_layout);
                        entry.posix_known = true;
                    }
                    entry.layout = Some(new_layout);
                    entry.cache_expiry = Instant::now();
                }
                return (ino, false);
            }
        }

        let ino = InodeId(self.next_ino.fetch_add(1, Ordering::Relaxed));
        self.map
            .insert(ino, InodeEntry::new(s3_key.to_string(), entry_type, layout));
        self.key_to_ino.insert(dedup_key, ino);
        (ino, true)
    }

    pub fn get(&self, ino: InodeId) -> Option<dashmap::mapref::one::Ref<'_, InodeId, InodeEntry>> {
        self.map.get(&ino)
    }

    pub fn get_mut(
        &self,
        ino: InodeId,
    ) -> Option<dashmap::mapref::one::RefMut<'_, InodeId, InodeEntry>> {
        self.map.get_mut(&ino)
    }

    pub fn get_s3_key(&self, ino: InodeId) -> Option<String> {
        self.map.get(&ino).map(|e| e.s3_key.clone())
    }

    /// Read-only lookup by key without creating entries or incrementing refcount.
    pub fn find_ino_by_key(&self, s3_key: &str, entry_type: EntryType) -> Option<InodeId> {
        self.key_to_ino
            .get(&(s3_key.to_string(), entry_type))
            .map(|r| *r)
    }

    /// Remove name mapping for an inode (used during unlink/rmdir).
    /// Removes the reverse map entry but keeps the inode in the map for open
    /// file handles. The inode will be fully removed when refcount reaches 0.
    pub fn remove_name_mapping(&self, ino: InodeId) {
        if ino == ROOT_INODE {
            return;
        }
        if let Some(mut entry) = self.map.get_mut(&ino) {
            self.key_to_ino
                .remove(&(entry.s3_key.clone(), entry.entry_type));
            // Mark the inode so any in-flight FUSE op via a now-stale
            // dentry stops re-publishing to NSS and resurrecting the
            // deleted name.
            entry.name_removed = true;
        }
    }

    /// Register an additional name -> inode mapping without disturbing
    /// the inode's primary `s3_key`. Used by `vfs_link` so a hardlink's
    /// new name resolves to the same inode (and the same `inode_id`
    /// resolution cache) as the original.
    pub fn add_alias(&self, s3_key: &str, entry_type: EntryType, ino: InodeId) {
        self.key_to_ino
            .insert((s3_key.to_string(), entry_type), ino);
    }

    /// Drop a single name -> inode mapping without touching the
    /// `InodeEntry` (so the inode and its other hardlink aliases stay
    /// live). Used by `vfs_unlink` when one of several hardlink names
    /// goes away but the inode still has links.
    pub fn remove_alias(&self, s3_key: &str, entry_type: EntryType) {
        self.key_to_ino.remove(&(s3_key.to_string(), entry_type));
    }

    /// Update the s3_key for an inode (used during rename).
    /// Updates both the inode entry and the reverse map.
    pub fn update_s3_key(&self, ino: InodeId, new_key: &str) {
        if let Some(mut entry) = self.map.get_mut(&ino) {
            let old_key = (entry.s3_key.clone(), entry.entry_type);
            self.key_to_ino.remove(&old_key);
            entry.s3_key = new_key.to_string();
            self.key_to_ino
                .insert((new_key.to_string(), entry.entry_type), ino);
        }
    }

    /// Update s3_keys for all child inodes under old_prefix to use new_prefix.
    /// The directory inode itself should already have been updated via
    /// `update_s3_key()` before calling this.
    pub fn rename_children(&self, old_prefix: &str, new_prefix: &str) {
        let children: Vec<InodeId> = self
            .map
            .iter()
            .filter(|e| {
                e.value().s3_key.starts_with(old_prefix)
                    && *e.key() != ROOT_INODE
                    && e.value().s3_key != old_prefix
            })
            .map(|e| *e.key())
            .collect();
        for ino in children {
            if let Some(mut entry) = self.map.get_mut(&ino) {
                let old_key = (entry.s3_key.clone(), entry.entry_type);
                let new_key = format!("{}{}", new_prefix, &entry.s3_key[old_prefix.len()..]);
                self.key_to_ino.remove(&old_key);
                entry.s3_key = new_key.clone();
                self.key_to_ino.insert((new_key, entry.entry_type), ino);
            }
        }
    }

    /// Forget an inode (decrement refcount). Removes the entry when the
    /// refcount reaches 0, unless `keep_if_zero` pins it (an async
    /// release flush or a queued writeback intent still needs the entry;
    /// the caller reaps it later via `remove_if_unreferenced`). Root is
    /// never removed.
    pub fn forget(&self, ino: InodeId, nlookup: u64, keep_if_zero: bool) -> ForgetOutcome {
        if ino == ROOT_INODE {
            return ForgetOutcome::Live;
        }

        let zeroed = self
            .map
            .get(&ino)
            .map(|entry| entry.forget(nlookup))
            .unwrap_or(false);
        if !zeroed {
            return ForgetOutcome::Live;
        }
        if keep_if_zero {
            return ForgetOutcome::KeptZeroed;
        }

        if let Some((_, entry)) = self.map.remove(&ino) {
            // Guard on the mapping still pointing here: after an unlink +
            // recreate the key maps to a NEWER inode, and a blind remove
            // would orphan the live entry.
            self.key_to_ino
                .remove_if(&(entry.s3_key.clone(), entry.entry_type), |_, mapped| {
                    *mapped == ino
                });
            return ForgetOutcome::Removed;
        }
        ForgetOutcome::Live
    }

    /// `true` iff the entry exists with a zero refcount, i.e. a FORGET
    /// zeroed it but a pin kept it alive.
    pub fn is_unreferenced(&self, ino: InodeId) -> bool {
        self.map
            .get(&ino)
            .map(|e| e.refcount.load(Ordering::Relaxed) == 0)
            .unwrap_or(false)
    }

    /// Finish a deferred FORGET: remove the entry iff its refcount is
    /// still 0 (a lookup that revived it in the meantime keeps it).
    /// Returns `true` when the entry was removed.
    pub fn remove_if_unreferenced(&self, ino: InodeId) -> bool {
        if ino == ROOT_INODE {
            return false;
        }
        let mut removed_key = None;
        let removed = self
            .map
            .remove_if(&ino, |_, e| {
                if e.refcount.load(Ordering::Relaxed) == 0 {
                    removed_key = Some((e.s3_key.clone(), e.entry_type));
                    true
                } else {
                    false
                }
            })
            .is_some();
        if let Some(key) = removed_key {
            // The reaped entry's key can be stale (unlink + recreate while
            // the deferred FORGET was pinned): only drop the mapping if it
            // still points at this inode, not at a newer one.
            self.key_to_ino.remove_if(&key, |_, mapped| *mapped == ino);
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::object_layout::DirectoryData;

    fn dir_layout(uid: u32, gid: u32, mode: u32) -> ObjectLayout {
        ObjectLayout {
            timestamp: 0,
            version_id: ObjectLayout::gen_version_id(),
            block_size: 4096,
            blob_version: 1,
            data_write_token: 0,
            next_data_write_token: 0,
            pending_data_write: None,
            state: ObjectState::Directory(DirectoryData {
                posix: PosixAttrs {
                    mode,
                    uid,
                    gid,
                    mtime_ns: 0,
                    ctime_ns: 0,
                },
            }),
        }
    }

    #[test]
    fn none_seed_dir_is_posix_unknown_then_refreshes_from_marker() {
        let table = InodeTable::new();
        let key = "d/";

        // readdir common-prefix / lookup prefix-fallback: no layout, so the
        // owner is a placeholder and must be flagged not-authoritative.
        let (ino, is_new) = table.lookup_or_insert(key, EntryType::Directory, None);
        assert!(is_new, "first insert allocates a new inode");
        {
            let e = table.get(ino).expect("entry present");
            assert!(!e.posix_known, "None-seed dir must be posix-unknown");
            assert_eq!(e.posix.uid, 0, "placeholder owner is uid 0");
        }

        // A later lookup that reads the authoritative marker seeds the real
        // owner into the existing (poisoned) entry and marks it known.
        let (ino2, is_new2) = table.lookup_or_insert(
            key,
            EntryType::Directory,
            Some(dir_layout(1000, 1001, 0o755)),
        );
        assert_eq!(ino2, ino, "same key resolves to the same inode");
        assert!(!is_new2, "second lookup reuses the entry");
        let e = table.get(ino).expect("entry present");
        assert!(e.posix_known, "marker lookup marks posix authoritative");
        assert_eq!(e.posix.uid, 1000, "owner refreshed from the marker");
        assert_eq!(e.posix.gid, 1001, "group refreshed from the marker");
    }

    #[test]
    fn known_posix_is_not_clobbered_by_a_later_marker() {
        // A dir seeded from its marker (known), then locally chmod'd but not
        // yet flushed, must not be reverted by a subsequent marker-bearing
        // lookup carrying the stale mode.
        let table = InodeTable::new();
        let key = "d/";
        let (ino, _) = table.lookup_or_insert(
            key,
            EntryType::Directory,
            Some(dir_layout(1000, 1000, 0o755)),
        );
        {
            let mut e = table.get_mut(ino).expect("entry present");
            e.posix.mode = 0o700; // unflushed local chmod
        }
        table.lookup_or_insert(
            key,
            EntryType::Directory,
            Some(dir_layout(1000, 1000, 0o755)),
        );
        let e = table.get(ino).expect("entry present");
        assert_eq!(
            e.posix.mode, 0o700,
            "known (locally mutated) posix must survive a stale marker lookup"
        );
    }
}
