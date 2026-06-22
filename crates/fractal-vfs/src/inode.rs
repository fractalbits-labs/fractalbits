use dashmap::DashMap;
use data_types::object_layout::{ObjectLayout, ObjectState, PosixAttrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use uuid::Uuid;

pub const ROOT_INODE: u64 = 1;

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
        Self {
            s3_key,
            entry_type,
            layout,
            cache_expiry: Instant::now(),
            posix,
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

pub struct InodeTable {
    map: DashMap<u64, InodeEntry>,
    next_ino: AtomicU64,
    // Reverse map: (s3_key, entry_type) -> inode for lookup dedup.
    // EntryType is included to avoid aliasing between files and directories
    // with the same key (e.g., a file at "dir/" vs a directory prefix "dir/").
    key_to_ino: DashMap<(String, EntryType), u64>,
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
    ) -> (u64, bool) {
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
                    entry.layout = Some(new_layout);
                    entry.cache_expiry = Instant::now();
                }
                return (ino, false);
            }
        }

        let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
        self.map
            .insert(ino, InodeEntry::new(s3_key.to_string(), entry_type, layout));
        self.key_to_ino.insert(dedup_key, ino);
        (ino, true)
    }

    pub fn get(&self, ino: u64) -> Option<dashmap::mapref::one::Ref<'_, u64, InodeEntry>> {
        self.map.get(&ino)
    }

    pub fn get_mut(&self, ino: u64) -> Option<dashmap::mapref::one::RefMut<'_, u64, InodeEntry>> {
        self.map.get_mut(&ino)
    }

    pub fn get_s3_key(&self, ino: u64) -> Option<String> {
        self.map.get(&ino).map(|e| e.s3_key.clone())
    }

    /// Read-only lookup by key without creating entries or incrementing refcount.
    pub fn find_ino_by_key(&self, s3_key: &str, entry_type: EntryType) -> Option<u64> {
        self.key_to_ino
            .get(&(s3_key.to_string(), entry_type))
            .map(|r| *r)
    }

    /// Remove name mapping for an inode (used during unlink/rmdir).
    /// Removes the reverse map entry but keeps the inode in the map for open
    /// file handles. The inode will be fully removed when refcount reaches 0.
    pub fn remove_name_mapping(&self, ino: u64) {
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
    pub fn add_alias(&self, s3_key: &str, entry_type: EntryType, ino: u64) {
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
    pub fn update_s3_key(&self, ino: u64, new_key: &str) {
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
        let children: Vec<u64> = self
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

    /// Forget an inode (decrement refcount). Removes entry when refcount reaches 0.
    /// Root inode is never removed.
    pub fn forget(&self, ino: u64, nlookup: u64) {
        if ino == ROOT_INODE {
            return;
        }

        let should_remove = self
            .map
            .get(&ino)
            .map(|entry| entry.forget(nlookup))
            .unwrap_or(false);

        if should_remove && let Some((_, entry)) = self.map.remove(&ino) {
            self.key_to_ino
                .remove(&(entry.s3_key.clone(), entry.entry_type));
        }
    }

    /// Evict inodes that haven't been accessed within `ttl`. Returns the set of
    /// evicted inode numbers so the caller can skip any that have open handles.
    /// Used in NFS mode where there is no FUSE FORGET to drive cleanup.
    pub fn evict_stale(&self, ttl: Duration) -> Vec<u64> {
        let cutoff = Instant::now() - ttl;
        let stale: Vec<u64> = self
            .map
            .iter()
            .filter(|e| *e.key() != ROOT_INODE && e.value().cache_expiry < cutoff)
            .map(|e| *e.key())
            .collect();

        let mut evicted = Vec::new();
        for ino in stale {
            if let Some((_, entry)) = self.map.remove(&ino) {
                self.key_to_ino
                    .remove(&(entry.s3_key.clone(), entry.entry_type));
                evicted.push(ino);
            }
        }
        evicted
    }
}
