//! Directory listing and statfs.

use data_types::TraceId;
use data_types::object_layout::{InodeRecord, ObjectState};
use fractal_fuse::{FileHandleId, InodeId};
use std::sync::Arc;

use crate::cache::{DirEntry, DirEntryKind};
use crate::config::WritebackMode;
use crate::error::FsError;
use crate::inode::{EntryType, ROOT_INODE};
use crate::vfs::{DEFAULT_BLOCK_SIZE, VfsCore, VfsDirEntry, VfsDirEntryPlus, VfsStatfs};

impl VfsCore {
    pub(crate) async fn fetch_dir_entries(
        &self,
        parent: InodeId,
        prefix: &str,
    ) -> Result<Arc<Vec<DirEntry>>, FsError> {
        if let Some(cached) = self.dir_cache.get(prefix) {
            let stale = cached
                .iter()
                .any(|entry| self.inodes.get(InodeId(entry.ino)).is_none());
            if !stale {
                return Ok(cached);
            }
            tracing::debug!(%prefix, "Directory cache contains stale inode(s), rebuilding");
            self.dir_cache.invalidate(prefix);
        }

        // A cold listing races the async worker: a queued create
        // (mkdir/symlink/mknod PutInode) or an in-flight release publish
        // may not be in NSS yet, and the incomplete listing would then be
        // cached until the TTL, hiding an entry the caller already saw
        // created. Wait for those cycles to commit first. Taints are left
        // in place (readdir is not an error-reporting point); a failed
        // create is simply absent from the listing.
        if self.writeback_mode == WritebackMode::Default {
            // Flush still-dirty open handles first: a file in the
            // close(2)-to-FUSE_RELEASE window has no registered cycle yet,
            // so draining only known cycles/intents would list NSS without
            // it and cache the incomplete listing. Flushing registers the
            // cycle so the wait below blocks on it. Mirrors
            // drain_writeback_under_prefix / vfs_fsyncdir.
            self.flush_dirty_handles_under_prefix(prefix).await?;
            for ino in self.writeback_drain_targets_under_prefix(prefix) {
                if let Some(barrier) = self.writeback.fsync_barrier(ino) {
                    self.wait_cycles_drained(ino, barrier).await?;
                }
            }
        }

        let trace_id = TraceId::new();
        let mut all_entries = Vec::new();

        // Resolve parent-of-parent inode for ".." entry.
        // For root ("/") or top-level dirs, parent-of-parent is root.
        let dotdot_ino = if parent == ROOT_INODE {
            ROOT_INODE
        } else {
            let trimmed = prefix.trim_end_matches('/');
            match trimmed.rfind('/') {
                Some(pos) => {
                    let parent_key = &prefix[..=pos];
                    if parent_key == "/" {
                        ROOT_INODE
                    } else {
                        let (ino, _) =
                            self.inodes
                                .lookup_or_insert(parent_key, EntryType::Directory, None);
                        ino
                    }
                }
                None => ROOT_INODE,
            }
        };

        all_entries.push(DirEntry {
            name: ".".to_string(),
            ino: parent.0,
            kind: DirEntryKind::Directory,
        });
        all_entries.push(DirEntry {
            name: "..".to_string(),
            ino: dotdot_ino.0,
            kind: DirEntryKind::Directory,
        });

        let mut start_after = String::new();
        loop {
            let entries = self
                .backend()
                .list_inodes(prefix, "/", &start_after, 1000, &trace_id)
                .await?;

            if entries.is_empty() {
                break;
            }

            let last_key = entries.last().map(|e| e.key.clone());

            for entry in entries {
                let raw_key = &entry.key;

                let name = if raw_key.len() >= prefix.len() {
                    &raw_key[prefix.len()..]
                } else {
                    raw_key.as_str()
                };

                if let Some(layout) = entry.layout.as_ref() {
                    // File - backend already stripped trailing \0 from keys
                    if !layout.is_listable() {
                        continue;
                    }
                    if name.is_empty() {
                        continue;
                    }
                    let kind = Self::dir_entry_kind_from_layout(layout);
                    let (ino, _) =
                        self.inodes
                            .lookup_or_insert(raw_key, EntryType::File, entry.layout);
                    all_entries.push(DirEntry {
                        name: name.to_string(),
                        ino: ino.0,
                        kind,
                    });
                } else {
                    // Directory (common prefix)
                    let dir_name = name.trim_end_matches('/');
                    if dir_name.is_empty() {
                        continue;
                    }
                    let dir_key = raw_key.clone();
                    let (ino, _) =
                        self.inodes
                            .lookup_or_insert(&dir_key, EntryType::Directory, None);
                    all_entries.push(DirEntry {
                        name: dir_name.to_string(),
                        ino: ino.0,
                        kind: DirEntryKind::Directory,
                    });
                }
            }

            if let Some(last) = last_key {
                start_after = last;
            } else {
                break;
            }
        }

        Ok(self.dir_cache.insert(prefix.to_string(), all_entries))
    }

    pub fn vfs_opendir(&self, inode: InodeId) -> Result<FileHandleId, FsError> {
        if inode != ROOT_INODE {
            // Drop the read guard before `drop_cached_layout` takes a
            // write guard on the same inode: holding both on one shard
            // self-deadlocks.
            {
                let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
                if entry.entry_type != EntryType::Directory {
                    return Err(FsError::NotDir);
                }
            }
            if self.writeback.take_taint(inode) {
                self.drop_cached_layout(inode);
                return Err(FsError::Internal("writeback drain".to_string()));
            }
        }

        Ok(self.alloc_fh())
    }

    pub async fn vfs_readdir(
        &self,
        parent: InodeId,
        offset: u64,
    ) -> Result<Vec<VfsDirEntry>, FsError> {
        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        let dir_entries = self.fetch_dir_entries(parent, &prefix).await?;

        let offset = offset as usize;
        let entries = dir_entries
            .iter()
            .skip(offset)
            .enumerate()
            .map(|(idx, entry)| VfsDirEntry {
                ino: entry.ino,
                kind: entry.kind,
                name: entry.name.clone(),
                offset: (offset + idx + 1) as u64,
            })
            .collect();

        Ok(entries)
    }

    pub async fn vfs_readdirplus(
        &self,
        parent: InodeId,
        offset: u64,
    ) -> Result<Vec<VfsDirEntryPlus>, FsError> {
        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        let dir_entries = self.fetch_dir_entries(parent, &prefix).await?;

        let offset = offset as usize;

        // A subdirectory row comes from the delimiter listing as a
        // common-prefix with no posix, so its entry carries the uid-0
        // placeholder. Seed the real owner from each marker before building
        // attrs, or readdirplus emits uid 0 and the kernel caches it (a
        // later stat/chmod then sees the placeholder owner). Concurrent to
        // bound the cost on a cold `ls` of a many-subdir directory; a
        // posix-known entry is skipped, so repeat listings pay nothing.
        let unknown_dirs: Vec<InodeId> = dir_entries
            .iter()
            .skip(offset)
            .filter(|e| e.kind.is_dir())
            .map(|e| InodeId(e.ino))
            .filter(|&ino| {
                self.inodes
                    .get(ino)
                    .map(|e| !e.posix_known)
                    .unwrap_or(false)
            })
            .collect();
        if !unknown_dirs.is_empty() {
            futures::future::join_all(
                unknown_dirs
                    .into_iter()
                    .map(|ino| self.refresh_dir_posix_if_unknown(ino)),
            )
            .await;
        }

        let trace_id = TraceId::new();
        let mut entries: Vec<VfsDirEntryPlus> =
            Vec::with_capacity(dir_entries.len().saturating_sub(offset));
        // Per-page cache so a directory holding many aliases of one hardlink
        // resolves the shared InodeRecord once, not once per name (otherwise
        // a single readdirplus fans out into N identical record RPCs).
        let mut record_cache: std::collections::HashMap<uuid::Uuid, InodeRecord> =
            std::collections::HashMap::new();
        for (idx, entry) in dir_entries.iter().skip(offset).enumerate() {
            let attr = if entry.kind.is_dir() {
                self.make_dir_attr(InodeId(entry.ino))
            } else {
                // Clone the cached layout out (dropping the map guard before
                // any await), then resolve a hardlink redirect to the shared
                // record's real layout: make_file_attr needs a sized layout,
                // and an `Indirect` redirect has none; `layout.size()`
                // would error and fail the whole readdirplus, surfacing as
                // EINVAL on the first `ls` of a directory holding a hardlink.
                let (cached_layout, cached_id) = self
                    .inodes
                    .get(InodeId(entry.ino))
                    .map(|e| (e.layout.clone(), e.inode_id))
                    .unwrap_or((None, None));
                match cached_layout {
                    Some(l) => {
                        // A hardlink alias either already carries the record
                        // id on its entry (a prior pass replaced the Indirect
                        // redirect with the record's normal layout) or still
                        // has the Indirect redirect cached. Either way resolve
                        // through the per-page record cache.
                        let id_opt = cached_id.or(match &l.state {
                            ObjectState::Indirect(redir) => Some(redir.inode_id),
                            _ => None,
                        });
                        let (resolved, resolved_id, nlink) = if let Some(id) = id_opt {
                            let rec = match record_cache.get(&id) {
                                Some(r) => r.clone(),
                                None => {
                                    let r = self.backend().get_inode_record(id, &trace_id).await?;
                                    record_cache.insert(id, r.clone());
                                    r
                                }
                            };
                            (rec.layout, Some(id), rec.nlink)
                        } else {
                            (l, None, 1)
                        };
                        // Persist the resolved hardlink identity + real
                        // layout + record posix so later lookups/opens/
                        // flushes target the shared record, and so the attr
                        // below reports the record's mode/uid/gid/times
                        // rather than stale cached defaults.
                        if let Some(id) = resolved_id
                            && let Some(mut e) = self.inodes.get_mut(InodeId(entry.ino))
                        {
                            e.inode_id = Some(id);
                            e.posix = crate::inode::layout_posix(&resolved);
                            e.layout = Some(resolved.clone());
                        }
                        let mut attr = self.make_file_attr(InodeId(entry.ino), &resolved)?;
                        // resolve_indirect returns the record's true link
                        // count; the redirect layout carries none.
                        attr.nlink = nlink;
                        attr
                    }
                    None => self.make_default_file_attr(InodeId(entry.ino)),
                }
            };
            entries.push(VfsDirEntryPlus {
                ino: entry.ino,
                kind: entry.kind,
                name: entry.name.clone(),
                offset: (offset + idx + 1) as u64,
                attr,
            });
        }

        Ok(entries)
    }

    pub fn vfs_statfs(&self) -> VfsStatfs {
        VfsStatfs {
            blocks: 1024 * 1024,
            bfree: if self.read_write { 512 * 1024 } else { 0 },
            bavail: if self.read_write { 512 * 1024 } else { 0 },
            files: 1024 * 1024,
            ffree: if self.read_write { 512 * 1024 } else { 0 },
            bsize: DEFAULT_BLOCK_SIZE,
            // POSIX NAME_MAX; Linux's VFS hard-caps any path
            // component at 255 regardless of what FUSE advertises, so
            // anything larger here just makes pjdfstest pick a name
            // the kernel will reject before we ever see it.
            namelen: 255,
            frsize: DEFAULT_BLOCK_SIZE,
        }
    }
}
