//! Attribute reads and posix publication.

use bytes::Bytes;
use data_types::TraceId;
use data_types::object_layout::{ObjectLayout, ObjectState, PosixAttrs, SpecialKind};
use fractal_fuse::{FileHandleId, InodeId};
use rkyv::api::high::to_bytes_in;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::WritebackMode;
use crate::error::FsError;
use crate::inode::{EntryType, ROOT_INODE};
use crate::vfs::publish::{publish_set_posix, put_inode_create_idempotent};
use crate::vfs::{DEFAULT_BLOCK_SIZE, VfsAttr, VfsCore, dir_mode, file_mode, now_ns, symlink_mode};
use crate::writeback::{CoalesceOutcome, InodeOp as WbInodeOp};

impl VfsCore {
    pub(crate) fn file_perm(&self) -> u16 {
        if self.read_write { 0o644 } else { 0o444 }
    }

    pub(crate) fn dir_perm(&self) -> u16 {
        if self.read_write { 0o755 } else { 0o555 }
    }

    pub(crate) fn make_file_attr(
        &self,
        ino: InodeId,
        layout: &ObjectLayout,
    ) -> Result<VfsAttr, FsError> {
        let size = layout.size()?;
        let ts = layout.timestamp / 1000;
        // Symlinks share the regular-file attribute path but report
        // S_IFLNK + 0 blocks. The kernel uses the mode bit to decide
        // whether to call FUSE_READLINK or FUSE_OPEN on a lookup.
        let is_symlink = layout.is_symlink();
        // Special inodes (fifo / block / char / unix-socket) share the
        // same attribute path; the kernel uses the S_IFMT bit and
        // `rdev` to dispatch I/O to its own pipe / device / socket
        // layer rather than calling FUSE_READ / FUSE_WRITE.
        let special = layout.special();
        // Prefer the in-memory posix from the inode entry: it tracks
        // unflushed setattr changes that haven't yet been folded into
        // a layout. Falls back to layout-embedded posix and finally to
        // synthesised defaults when neither has been initialised.
        let posix = self
            .inodes
            .get(ino)
            .map(|e| e.posix)
            .unwrap_or_else(|| crate::inode::layout_posix(layout));
        let default_mode = if is_symlink {
            symlink_mode(0o777)
        } else if let Some(s) = special {
            let ifmt = match s.kind {
                SpecialKind::Fifo => libc::S_IFIFO,
                SpecialKind::BlockDevice => libc::S_IFBLK,
                SpecialKind::CharDevice => libc::S_IFCHR,
                SpecialKind::Socket => libc::S_IFSOCK,
            };
            ifmt | (self.file_perm() as u32 & !libc::S_IFMT)
        } else {
            file_mode(self.file_perm())
        };
        // posix.mode may be a raw permission-bits value coming from a
        // chmod that didn't include S_IFMT. Re-stamp the file-type
        // bits from `default_mode` so the kernel sees a valid mode_t.
        let ifmt_mask = libc::S_IFMT;
        let mode = if posix.mode != 0 {
            (posix.mode & !ifmt_mask) | (default_mode & ifmt_mask)
        } else {
            default_mode
        };
        let rdev = special.map(|s| s.rdev).unwrap_or(0);
        let (mtime_secs, mtime_ns_part) = if posix.mtime_ns != 0 {
            (
                posix.mtime_ns / 1_000_000_000,
                (posix.mtime_ns % 1_000_000_000) as u32,
            )
        } else {
            (ts, 0u32)
        };
        let (ctime_secs, ctime_ns_part) = if posix.ctime_ns != 0 {
            (
                posix.ctime_ns / 1_000_000_000,
                (posix.ctime_ns % 1_000_000_000) as u32,
            )
        } else {
            (ts, 0u32)
        };
        let attr = VfsAttr {
            ino: ino.0,
            size,
            blocks: if is_symlink || special.is_some() {
                0
            } else {
                size.div_ceil(512)
            },
            // PosixAttrs intentionally drops the per-inode atime; we
            // mirror mtime so a freshly created inode reports a
            // non-zero atime. apply_atime_override layers any
            // utimensat-set atime on top after this builds.
            atime_secs: mtime_secs,
            mtime_secs,
            ctime_secs,
            atime_ns_part: mtime_ns_part,
            mtime_ns_part,
            ctime_ns_part,
            mode,
            nlink: 1,
            uid: posix.uid,
            gid: posix.gid,
            rdev,
            blksize: DEFAULT_BLOCK_SIZE,
        };
        Ok(self.apply_atime_override(ino, attr))
    }

    /// Fallback file attr when layout is unavailable (e.g., inode evicted
    /// between fetch_dir_entries and readdirplus iteration). Uses correct
    /// kind=RegularFile to avoid on-wire inconsistency.
    pub(crate) fn make_default_file_attr(&self, ino: InodeId) -> VfsAttr {
        VfsAttr {
            ino: ino.0,
            size: 0,
            blocks: 0,
            atime_secs: 0,
            mtime_secs: 0,
            ctime_secs: 0,
            atime_ns_part: 0,
            mtime_ns_part: 0,
            ctime_ns_part: 0,
            mode: file_mode(self.file_perm()),
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        }
    }

    pub(crate) fn make_dir_attr(&self, ino: InodeId) -> VfsAttr {
        let posix = self.inodes.get(ino).map(|e| e.posix).unwrap_or_default();
        // FUSE root inode reports mode 0o777 unconditionally so the
        // kernel's permission check lets every caller into the mount;
        // sub-directory inodes honour their persisted mode normally.
        let default_mode = if ino == ROOT_INODE {
            dir_mode(0o777)
        } else {
            dir_mode(self.dir_perm())
        };
        let ifmt_mask = libc::S_IFMT;
        let mode = if posix.mode != 0 && ino != ROOT_INODE {
            (posix.mode & !ifmt_mask) | (default_mode & ifmt_mask)
        } else {
            default_mode
        };
        let mtime_secs = posix.mtime_ns / 1_000_000_000;
        let mtime_ns_part = (posix.mtime_ns % 1_000_000_000) as u32;
        let ctime_secs = posix.ctime_ns / 1_000_000_000;
        let ctime_ns_part = (posix.ctime_ns % 1_000_000_000) as u32;
        let attr = VfsAttr {
            ino: ino.0,
            size: 0,
            blocks: 0,
            atime_secs: mtime_secs,
            mtime_secs,
            ctime_secs,
            atime_ns_part: mtime_ns_part,
            mtime_ns_part,
            ctime_ns_part,
            mode,
            // We do not maintain the traditional `2 + immediate_subdirs`
            // directory link count (it would cost an NSS listing per
            // stat), so report `1` (the btrfs convention) instead of a
            // constant `2`. A constant `nlink == 2` falsely tells
            // `find`/`du`/`fts` the directory has zero subdirectories, so
            // their leaf optimisation can skip recursing into real
            // children. A count below 2 is the standard "link count not
            // tracked, scan every entry" signal. POSIX permits nlink=1
            // for directories; the `2 + subdirs` scheme is a
            // traditional-FS convention, not a requirement.
            nlink: 1,
            uid: posix.uid,
            gid: posix.gid,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        };
        self.apply_atime_override(ino, attr)
    }

    pub(crate) fn make_new_file_attr(&self, ino: InodeId, size: u64) -> VfsAttr {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let posix = self.inodes.get(ino).map(|e| e.posix).unwrap_or_default();
        let default_mode = file_mode(self.file_perm());
        let ifmt_mask = libc::S_IFMT;
        let mode = if posix.mode != 0 {
            (posix.mode & !ifmt_mask) | (default_mode & ifmt_mask)
        } else {
            default_mode
        };
        let (mtime_secs, mtime_ns_part) = if posix.mtime_ns != 0 {
            (
                posix.mtime_ns / 1_000_000_000,
                (posix.mtime_ns % 1_000_000_000) as u32,
            )
        } else {
            (now_secs, 0u32)
        };
        let (ctime_secs, ctime_ns_part) = if posix.ctime_ns != 0 {
            (
                posix.ctime_ns / 1_000_000_000,
                (posix.ctime_ns % 1_000_000_000) as u32,
            )
        } else {
            (now_secs, 0u32)
        };
        let attr = VfsAttr {
            ino: ino.0,
            size,
            blocks: size.div_ceil(512),
            atime_secs: mtime_secs,
            mtime_secs,
            ctime_secs,
            atime_ns_part: mtime_ns_part,
            mtime_ns_part,
            ctime_ns_part,
            mode,
            nlink: 1,
            uid: posix.uid,
            gid: posix.gid,
            rdev: 0,
            blksize: DEFAULT_BLOCK_SIZE,
        };
        self.apply_atime_override(ino, attr)
    }

    /// Layer an explicit `utimensat`-set atime (held in
    /// `InodeEntry.atime_ns`, volatile) on top of the mtime-mirrored
    /// atime the builders emit. No-op when no override is set.
    pub(crate) fn apply_atime_override(&self, ino: InodeId, mut attr: VfsAttr) -> VfsAttr {
        if let Some(entry) = self.inodes.get(ino)
            && entry.atime_ns != 0
        {
            attr.atime_secs = entry.atime_ns / 1_000_000_000;
            attr.atime_ns_part = (entry.atime_ns % 1_000_000_000) as u32;
        }
        attr
    }

    pub async fn vfs_getattr(
        &self,
        inode: InodeId,
        fh: Option<FileHandleId>,
    ) -> Result<VfsAttr, FsError> {
        if inode == ROOT_INODE {
            return Ok(self.make_dir_attr(ROOT_INODE));
        }

        // If there's an open write handle with a dirty buffer, report its size
        if let Some(fh_id) = fh
            && let Some(handle) = self.file_handles.get(&fh_id)
            && let Some(ref wb) = handle.write_buf
            && wb.dirty
        {
            return Ok(self.make_new_file_attr(inode, wb.file_size));
        }

        // A directory materialised from a delimiter listing carries only
        // placeholder posix (uid 0 / mode 0); fetch its marker so stat and
        // the setattr owner check see the real owner. No-op for files or an
        // already-authoritative entry.
        self.refresh_dir_posix_if_unknown(inode).await;

        let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;

        match entry.entry_type {
            EntryType::Directory => Ok(self.make_dir_attr(inode)),
            EntryType::File => {
                let inode_id = entry.inode_id;
                let name_removed = entry.name_removed;
                if let Some(ref layout) = entry.layout {
                    let layout = layout.clone();
                    drop(entry);
                    if let Some(id) = inode_id {
                        // Hardlink: the authoritative layout (mode / uid /
                        // gid / times) AND nlink live in the shared
                        // record, and may have changed via another name
                        // (chmod/chown/unlink-ctime-bump). Refetch the
                        // record rather than trusting the cached layout
                        // (unlink/00.t ctime checks, link/00.t chmod).
                        // `make_file_attr` reads times/mode from
                        // `entry.posix`, so refresh that from the record
                        // BEFORE building the attr; a stale posix would
                        // otherwise mask the just-bumped ctime.
                        let trace_id = TraceId::new();
                        if let Ok(record) = self.backend().get_inode_record(id, &trace_id).await {
                            if let Some(mut e) = self.inodes.get_mut(inode) {
                                e.posix = crate::inode::layout_posix(&record.layout);
                                e.layout = Some(record.layout.clone());
                            }
                            let mut attr = self.make_file_attr(inode, &record.layout)?;
                            attr.nlink = record.nlink;
                            return Ok(attr);
                        }
                    }
                    let mut attr = self.make_file_attr(inode, &layout)?;
                    // Cross-instance size authority: this entry's cached layout
                    // (size + blob_version) may lag a peer instance's most
                    // recent overwrite, so make_file_attr's size can be stale.
                    // Re-read the authoritative geometry sentinel from BSS via a
                    // max-version quorum read, which reflects the latest
                    // published override regardless of our cached layout
                    // version. Skips symlinks/special files (they report their
                    // own size and have no data blob). getattr is gated by the
                    // 1s FUSE attr TTL, so this BSS read happens at most about
                    // once/sec/inode: a bounded, throttled extra read.
                    if !layout.is_symlink()
                        && layout.special().is_none()
                        && let Ok(geom_guid) = layout.blob_guid()
                    {
                        let trace_id = TraceId::new();
                        match self.backend().get_blob_info(geom_guid, &trace_id).await {
                            // Only let the sentinel move size FORWARD: apply it
                            // when it is at least as new as our cached layout
                            // (vfs_lookup refreshes the cached layout from NSS,
                            // so a stale sentinel must never downgrade a fresh
                            // size back to an older value).
                            Ok(Some(info)) if info.blob_version >= layout.blob_version => {
                                attr.size = info.total_size;
                                // make_file_attr derives st_blocks from size
                                // (512-byte units) for regular files; keep it
                                // consistent with the refreshed size.
                                attr.blocks = info.total_size.div_ceil(512);
                            }
                            // Sentinel older than our cached layout: keep the
                            // (fresher) cached-layout size.
                            Ok(Some(_)) => {}
                            // No sentinel yet: keep the cached-layout size.
                            Ok(None) => {}
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    "getattr get_blob_info failed; using cached size"
                                );
                            }
                        }
                    }
                    if name_removed {
                        // POSIX: an open-but-unlinked file with no
                        // remaining links reports nlink=0 (unlink/14.t).
                        attr.nlink = 0;
                    }
                    Ok(attr)
                } else {
                    let key = entry.s3_key.clone();
                    drop(entry);
                    let trace_id = TraceId::new();
                    match self.backend().get_inode(&key, &trace_id).await {
                        Ok(layout) => {
                            let (real_layout, resolved_id, nlink) =
                                self.resolve_indirect(layout, &trace_id).await?;
                            let mut attr = self.make_file_attr(inode, &real_layout)?;
                            attr.nlink = nlink;
                            if let Some(mut entry) = self.inodes.get_mut(inode) {
                                entry.layout = Some(real_layout);
                                if let Some(id) = resolved_id {
                                    entry.inode_id = Some(id);
                                }
                            }
                            Ok(attr)
                        }
                        // A freshly created file that hasn't flushed to NSS
                        // yet has no committed layout, so it isn't resolvable
                        // by key. It still exists in memory behind an open
                        // write handle; synthesize its attr from the cached
                        // posix + the largest open write-buffer size. Without
                        // this, an fd-based stat/utimes before the first flush
                        // (tar -x does openat(O_CREAT) then futimens(fd)
                        // before close, and the kernel may not forward the fh
                        // on SETATTR) would wrongly return ENOENT.
                        Err(FsError::NotFound) if self.has_open_handles_for_inode(inode, None) => {
                            let size = self
                                .file_handles
                                .iter()
                                .filter(|e| e.value().ino == inode)
                                .filter_map(|e| e.value().write_buf.as_ref().map(|wb| wb.file_size))
                                .max()
                                .unwrap_or(0);
                            Ok(self.make_new_file_attr(inode, size))
                        }
                        Err(e) => Err(e),
                    }
                }
            }
        }
    }

    /// In-memory-only attributes: like `vfs_getattr` but never touches
    /// the backend. Serves uid/gid/mode/times from the inode entry's
    /// `posix` and size/type from the cached `layout` (which the flush
    /// keeps current under the single-writer lock). Used on the setattr
    /// path (both the permission precheck and the post-mutation reply),
    /// so a `chmod`/`chown`/`utimensat` does not pay the two
    /// cross-instance coherency round-trips `vfs_getattr` makes
    /// (`get_inode` on a cold layout, `get_blob_info` size sentinel).
    /// This is the dominant per-file cost on create-heavy workloads
    /// (tar -xf issues one `utimensat` per file). Cross-instance
    /// freshness is still provided by the 1s FUSE attr TTL, after which
    /// the kernel re-issues a full `getattr`.
    ///
    /// True if the inode is a promoted hardlink (its `nlink` and shared
    /// posix live in the NSS `InodeRecord`, not the in-memory entry). The
    /// in-memory attr fast path below can't see that nlink, so a caller
    /// that replies an attr to the kernel must resolve the record for
    /// these (otherwise it clobbers the kernel's cached link count to 1).
    pub fn is_hardlink(&self, inode: InodeId) -> bool {
        self.inodes
            .get(inode)
            .map(|e| e.inode_id.is_some())
            .unwrap_or(false)
    }

    pub fn is_dir(&self, inode: InodeId) -> bool {
        self.inodes
            .get(inode)
            .map(|e| e.entry_type == EntryType::Directory)
            .unwrap_or(false)
    }

    /// Seed authoritative posix into a directory entry whose owner/mode is
    /// still a listing-materialised placeholder (`posix_known == false`),
    /// by reading its NSS marker. No-op for files, the root, an entry with
    /// known posix, or a marker that has no directory layout (a legacy
    /// Normal marker / implicit directory keeps its default). Guarded on
    /// `!posix_known` again after the fetch so a concurrent local posix
    /// mutation is never clobbered.
    pub(crate) async fn refresh_dir_posix_if_unknown(&self, inode: InodeId) {
        let dir_key = match self.inodes.get(inode) {
            Some(e) if e.entry_type == EntryType::Directory && !e.posix_known => e.s3_key.clone(),
            _ => return,
        };
        let trace_id = TraceId::new();
        if let Ok(layout) = self.backend().get_inode(&dir_key, &trace_id).await
            && layout.is_directory()
            && let Some(mut e) = self.inodes.get_mut(inode)
            && !e.posix_known
        {
            e.posix = crate::inode::layout_posix(&layout);
            e.posix_known = true;
        }
    }

    pub fn vfs_getattr_inmem(
        &self,
        inode: InodeId,
        fh: Option<FileHandleId>,
    ) -> Result<VfsAttr, FsError> {
        if inode == ROOT_INODE {
            return Ok(self.make_dir_attr(ROOT_INODE));
        }
        // An open write handle's dirty buffer is the authoritative size.
        if let Some(fh_id) = fh
            && let Some(handle) = self.file_handles.get(&fh_id)
            && let Some(ref wb) = handle.write_buf
            && wb.dirty
        {
            return Ok(self.make_new_file_attr(inode, wb.file_size));
        }
        let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
        match entry.entry_type {
            EntryType::Directory => Ok(self.make_dir_attr(inode)),
            EntryType::File => match entry.layout.as_ref() {
                // `make_file_attr` preserves size + S_IFMT (symlink /
                // device) from the layout and reads mode/uid/gid/times
                // from `entry.posix`, all in-memory, no round-trip.
                Some(layout) => {
                    let layout = layout.clone();
                    drop(entry);
                    self.make_file_attr(inode, &layout)
                }
                // No cached layout yet (a brand-new file whose flush has
                // not landed): report a zero-size regular file from the
                // in-memory posix. setattr changes mode/owner/times (all
                // in posix), not size, so this is correct for the reply;
                // the TTL-bounded next getattr fills in the real size.
                None => {
                    drop(entry);
                    Ok(self.make_new_file_attr(inode, 0))
                }
            },
        }
    }

    /// Persist a freshly-built inode layout at `key`. Metadata
    /// publishes (symlink / special-file create, chmod / chown /
    /// utimensat, directory create) honour the writeback mode: `Strict`
    /// writes through synchronously; `Default` enqueues a `PutInode`
    /// intent so the worker commits it asynchronously, which is what
    /// makes the metadata cache a cache. Correctness against a
    /// follow-up unlink / lookup is provided by `drain_inode_to_barrier`
    /// on unlink/rmdir (so a delete can't race a not-yet-drained
    /// publish) and by `vfs_lookup`'s in-memory read-your-writes
    /// fallback (so a pending create is still visible), not by
    /// forcing every publish through NSS. `rmdir` additionally checks
    /// the queue for pending child creates before trusting the NSS
    /// emptiness probe.
    pub(crate) async fn publish_inode_layout(
        &self,
        ino: InodeId,
        key: &str,
        parent_key: &str,
        name: &str,
        layout_bytes: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        match self.writeback_mode {
            WritebackMode::Strict => {
                // Guard on absence (empty expected bytes): a brand-new
                // create must not blind-overwrite a peer that won the
                // name between the caller's absence check and this
                // publish. A lost race is reported as EEXIST, mirroring
                // the hardlink publish path. Idempotent under an
                // internally-retried RPC (lost reply after commit).
                match put_inode_create_idempotent(self.backend(), key, layout_bytes, trace_id).await
                {
                    Ok(()) => {}
                    Err(FsError::CasConflict) => return Err(FsError::AlreadyExists),
                    Err(e) => return Err(e),
                }
            }
            WritebackMode::Default => {
                self.ensure_writeback_worker_started();
                let generation = self.writeback.open_next_cycle(ino);
                let outcome = self.writeback.upsert_inode_intent(
                    key.to_string(),
                    ino,
                    generation,
                    WbInodeOp::PutInode {
                        parent_key: parent_key.to_string(),
                        name: name.to_string(),
                        layout_bytes: layout_bytes.clone(),
                    },
                );
                if outcome == CoalesceOutcome::Blocked {
                    // Unmount drain in progress: publish synchronously
                    // so the metadata isn't dropped on the floor. Guard
                    // on absence (empty expected bytes) for the same
                    // reason the async worker does: a brand-new create
                    // must not clobber a peer that won the name.
                    match put_inode_create_idempotent(self.backend(), key, layout_bytes, trace_id)
                        .await
                    {
                        Ok(()) => self.writeback.advance_to_done(ino, generation),
                        // A peer won the name. The error is delivered
                        // synchronously as EEXIST, so resolve the cycle
                        // cleanly instead of tainting (there is no
                        // deferred error to surface later).
                        Err(FsError::CasConflict) => {
                            self.writeback.advance_to_done(ino, generation);
                            return Err(FsError::AlreadyExists);
                        }
                        Err(e) => {
                            self.writeback.mark_failed(key, generation, ino);
                            return Err(e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Persist a posix-only update (chmod / chown / utimensat) at `key`.
    /// `Strict` writes through synchronously. `Default` enqueues a
    /// `SetPosix` intent the worker applies via CAS (guarding on the
    /// layout snapshot taken here, re-fetching and re-folding on
    /// conflict), so this metadata publish can never blind-put a stale
    /// data layout over a concurrent flush's CAS publish.
    pub(crate) async fn publish_posix_update(
        &self,
        ino: InodeId,
        key: &str,
        posix: PosixAttrs,
        expected_layout_bytes: Bytes,
        layout_bytes: Bytes,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        match self.writeback_mode {
            WritebackMode::Strict => {
                publish_set_posix(
                    self.backend(),
                    key,
                    &posix,
                    &expected_layout_bytes,
                    &layout_bytes,
                    trace_id,
                )
                .await?;
            }
            WritebackMode::Default => {
                self.ensure_writeback_worker_started();
                let generation = self.writeback.open_next_cycle(ino);
                let outcome = self.writeback.upsert_inode_intent(
                    key.to_string(),
                    ino,
                    generation,
                    WbInodeOp::SetPosix {
                        posix,
                        expected_layout_bytes: expected_layout_bytes.clone(),
                        layout_bytes: layout_bytes.clone(),
                    },
                );
                if outcome == CoalesceOutcome::Blocked {
                    match publish_set_posix(
                        self.backend(),
                        key,
                        &posix,
                        &expected_layout_bytes,
                        &layout_bytes,
                        trace_id,
                    )
                    .await
                    {
                        Ok(_) => self.writeback.advance_to_done(ino, generation),
                        Err(e) => {
                            self.writeback.mark_failed(key, generation, ino);
                            return Err(e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn publish_posix_catchup_after_flush(
        &self,
        ino: InodeId,
        key: &str,
        layout: &ObjectLayout,
        trace_id: &TraceId,
    ) -> Result<Option<ObjectLayout>, FsError> {
        let Some(current_posix) = self.inodes.get(ino).map(|e| e.posix) else {
            return Ok(None);
        };
        if current_posix == crate::inode::layout_posix(layout) {
            return Ok(None);
        }

        let updated_layout = crate::inode::layout_with_posix(layout.clone(), current_posix);
        let expected_layout_bytes: Bytes =
            to_bytes_in::<_, rkyv::rancor::Error>(layout, Vec::new())
                .map_err(FsError::from)?
                .into();
        let updated_layout_bytes: Bytes =
            to_bytes_in::<_, rkyv::rancor::Error>(&updated_layout, Vec::new())
                .map_err(FsError::from)?
                .into();

        publish_set_posix(
            self.backend(),
            key,
            &current_posix,
            &expected_layout_bytes,
            &updated_layout_bytes,
            trace_id,
        )
        .await?;

        Ok(Some(updated_layout))
    }

    /// Apply a chmod / chown / utimensat to an inode. Each field is
    /// optional; `mode == Some(0)` is treated as "unset" (the kernel
    /// never sends a real mode of 0). The change is applied to the
    /// in-memory `entry.posix` immediately (so a getattr within the
    /// attr-cache TTL reflects it) and folded into the cached layout,
    /// which is then routed through the writeback / strict publish
    /// path so it survives a forget+relookup.
    #[allow(clippy::too_many_arguments)]
    pub async fn vfs_setattr_posix(
        &self,
        inode: InodeId,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        atime_ns: Option<u64>,
        mtime_ns: Option<u64>,
        ctime_ns: Option<u64>,
    ) -> Result<(), FsError> {
        self.ensure_writeback_worker_started();

        // Phase 1: mutate entry.posix under the guard, snapshot what we
        // need to persist, drop the guard before any await.
        let (s3_key, base_layout, updated_layout, new_posix, name_removed, inode_id) = {
            let mut entry = self.inodes.get_mut(inode).ok_or(FsError::NotFound)?;
            let mode_set = matches!(mode, Some(m) if m != 0);
            let uid_set = uid.is_some();
            let gid_set = gid.is_some();
            let atime_set = atime_ns.is_some();
            let mtime_set = mtime_ns.is_some();
            if mode_set {
                entry.posix.mode = mode.unwrap();
            }
            if let Some(u) = uid {
                entry.posix.uid = u;
            }
            if let Some(g) = gid {
                entry.posix.gid = g;
            }
            if let Some(at) = atime_ns {
                entry.atime_ns = at;
            }
            if let Some(m) = mtime_ns {
                entry.posix.mtime_ns = m;
            }
            if let Some(c) = ctime_ns {
                entry.posix.ctime_ns = c;
            } else if mode_set || uid_set || gid_set || atime_set || mtime_set {
                // POSIX: any of these changes bumps ctime to now unless
                // the caller set ctime explicitly.
                entry.posix.ctime_ns = now_ns();
            }
            let new_posix = entry.posix;
            // Fold the new posix into the cached layout when we have
            // one. With no cached layout we can't synthesise one
            // without an NSS round-trip; the in-memory mutation still
            // stands and the next op picks it up. The unfolded base is
            // kept too: the worker CAS-guards its publish on it.
            let base_layout = entry.layout.clone();
            let updated_layout = base_layout
                .as_ref()
                .map(|l| crate::inode::layout_with_posix(l.clone(), new_posix));
            let s3_key = entry.s3_key.clone();
            let name_removed = entry.name_removed;
            // Derive the hardlink id from a cached Indirect redirect when the
            // entry's `inode_id` was never set (e.g. the layout was cached by
            // a plain readdir that did not resolve it). Without this the
            // metadata update would take the non-hardlink path and overwrite
            // the redirect with a normal layout.
            let inode_id = entry.inode_id.or_else(|| match entry.layout.as_ref() {
                Some(l) => match &l.state {
                    ObjectState::Indirect(redir) => Some(redir.inode_id),
                    _ => None,
                },
                None => None,
            });
            (
                s3_key,
                base_layout,
                updated_layout,
                new_posix,
                name_removed,
                inode_id,
            )
        };

        // The dentry was unlinked; skip the NSS publish so we don't
        // resurrect the deleted file. The in-memory mutation already
        // happened, which is the right semantic for a still-open fd.
        if name_removed {
            return Ok(());
        }

        if let Some(layout) = updated_layout {
            // Hardlink: the shared metadata (mode/uid/gid/times) lives in
            // the `@hardlink/<inode_id>` InodeRecord, not at this name's
            // redirect. Fold the new posix into the record's layout so
            // every name observes the chmod/chown/utimes; nlink and
            // orphan_since are preserved.
            if let Some(id) = inode_id {
                let trace_id = TraceId::new();
                // Apply only the requested posix deltas to the FRESHLY
                // fetched record layout inside the CAS. Replacing the whole
                // layout with the snapshot-derived `layout` would restore a
                // stale size/blob_version if a hardlink-write flush bumped
                // the record between our snapshot and this CAS; and merging
                // field-by-field (rather than overwriting posix wholesale)
                // preserves a concurrent change to fields this call does not
                // touch.
                let committed = self
                    .cas_mutate_inode_record(id, &trace_id, |r| {
                        let mut p = crate::inode::layout_posix(&r.layout);
                        if let Some(m) = mode
                            && m != 0
                        {
                            p.mode = m;
                        }
                        if let Some(u) = uid {
                            p.uid = u;
                        }
                        if let Some(g) = gid {
                            p.gid = g;
                        }
                        if let Some(mt) = mtime_ns {
                            p.mtime_ns = mt;
                        }
                        if let Some(c) = ctime_ns {
                            p.ctime_ns = c;
                        } else if mode.is_some_and(|m| m != 0)
                            || uid.is_some()
                            || gid.is_some()
                            || atime_ns.is_some()
                            || mtime_ns.is_some()
                        {
                            p.ctime_ns = now_ns();
                        }
                        r.layout = crate::inode::layout_with_posix(r.layout.clone(), p);
                        Ok(())
                    })
                    .await?;
                // Reflect the committed record (our deltas + any concurrent
                // flush's size/version) into the local cache. Persist the
                // hardlink id too: when it was derived from a cached Indirect
                // redirect rather than `entry.inode_id`, the committed layout
                // we cache is the record's normal layout, so without setting
                // inode_id a second setattr would see a normal layout with no
                // id, take the non-hardlink path, and clobber the redirect.
                if let Some(mut e) = self.inodes.get_mut(inode) {
                    e.inode_id = Some(id);
                    e.posix = crate::inode::layout_posix(&committed.layout);
                    e.layout = Some(committed.layout.clone());
                }
                return Ok(());
            }

            let layout_bytes: Bytes =
                match to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new()) {
                    Ok(v) => v.into(),
                    Err(e) => {
                        tracing::warn!(error = %e, "vfs_setattr_posix: layout serialise failed");
                        return Ok(());
                    }
                };
            let expected_layout_bytes: Bytes = match to_bytes_in::<_, rkyv::rancor::Error>(
                &base_layout.expect("updated_layout implies a base layout"),
                Vec::new(),
            ) {
                Ok(v) => v.into(),
                Err(e) => {
                    tracing::warn!(error = %e, "vfs_setattr_posix: layout serialise failed");
                    return Ok(());
                }
            };
            // Keep the cached layout in sync with the bytes we publish so
            // a follow-up op reads the new posix from entry.layout (and a
            // follow-up setattr CAS-chains off it).
            if let Some(mut e) = self.inodes.get_mut(inode) {
                e.layout = Some(layout);
            }
            let trace_id = TraceId::new();
            self.publish_posix_update(
                inode,
                &s3_key,
                new_posix,
                expected_layout_bytes,
                layout_bytes,
                &trace_id,
            )
            .await?;
        }
        Ok(())
    }
}
