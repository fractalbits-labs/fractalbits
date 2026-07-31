//! Namei operations: lookup, link, create, unlink, rename, directories as names.

use bytes::Bytes;
use data_types::TraceId;
use data_types::object_layout::{
    DirectoryData, IndirectEntry, InodeRecord, MpuState, ObjectCoreMetaData, ObjectLayout,
    ObjectState, PosixAttrs, SpecialData, SpecialKind, SymlinkData,
};
use fractal_fuse::{FileHandleId, InodeId};
use rkyv::api::high::to_bytes_in;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cache::DirEntryKind;
use crate::config::WritebackMode;
use crate::error::FsError;
use crate::inode::{EntryType, ForgetOutcome, ROOT_INODE};
use crate::vfs::FileHandle;
use crate::vfs::write_buffer::WriteBuffer;
use crate::vfs::{DEFAULT_BLOCK_SIZE, VfsAttr, VfsCore, now_ns, symlink_mode};
use crate::writeback::WritebackQueue;

impl VfsCore {
    /// POSIX `NAME_MAX = 255`. Linux's general VFS enforces this at
    /// the kernel level for native filesystems but FUSE callers have
    /// to enforce it themselves; pjdfstest's `02.t` boundary tests
    /// (chmod/02.t, mkdir/02.t, etc.) pick a 256-byte component and
    /// expect ENAMETOOLONG.
    #[inline]
    pub(crate) fn check_name_max(name: &str) -> Result<(), FsError> {
        if name.len() > 255 {
            return Err(FsError::NameTooLong);
        }
        Ok(())
    }

    /// PATH_MAX boundary guard, separate from `check_name_max`. The
    /// kernel enforces PATH_MAX on the path the syscall receives
    /// before forwarding to FUSE; what reaches us is the
    /// bucket-relative key (`prefix + name`). NSS keys cap at 8 KiB
    /// (see `core/nss_server/configs.zig` user_max_key_size), so the
    /// only thing we guard here is a key that would overflow the NSS
    /// protocol cap.
    #[inline]
    pub(crate) fn check_path_max(prefix: &str, name: &str) -> Result<(), FsError> {
        if prefix.len() + name.len() > 8192 {
            return Err(FsError::NameTooLong);
        }
        Ok(())
    }

    /// POSIX: creating or removing an entry in a directory marks that
    /// directory's mtime and ctime for update (pjdfstest mkdir/00.t,
    /// unlink/00.t, etc.). We bump the parent's in-memory posix only:
    /// the immediately-following getattr reads it from the cached
    /// entry, and the parent's persisted layout is unaffected. Root
    /// has no inode entry of its own, so skip it.
    pub(crate) fn touch_parent_times(&self, parent: InodeId) {
        if parent == ROOT_INODE {
            return;
        }
        let now = now_ns();
        if let Some(mut entry) = self.inodes.get_mut(parent) {
            entry.posix.mtime_ns = now;
            entry.posix.ctime_ns = now;
        }
    }

    /// If `layout` is an `Indirect` hardlink redirect, fetch its
    /// `InodeRecord` and return the resolved `(real_layout, inode_id,
    /// nlink)`. For any non-redirect layout, return it unchanged with
    /// `nlink = 1` and no `inode_id`.
    pub(crate) async fn resolve_indirect(
        &self,
        layout: ObjectLayout,
        trace_id: &TraceId,
    ) -> Result<(ObjectLayout, Option<uuid::Uuid>, u32), FsError> {
        if let ObjectState::Indirect(redirect) = &layout.state {
            let inode_id = redirect.inode_id;
            let record = self.backend().get_inode_record(inode_id, trace_id).await?;
            Ok((record.layout, Some(inode_id), record.nlink))
        } else {
            Ok((layout, None, 1))
        }
    }

    /// Read-modify-write an `InodeRecord` under the same byte-equality CAS
    /// the record-aware flush uses, retrying on conflict. Without this,
    /// link / setattr / unlink would read-modify-write the record
    /// unconditionally and could clobber a concurrent flush that bumped the
    /// shared blob's version/size (and vice versa). Returns the committed
    /// record. `NotFound` propagates (the caller decides whether a vanished
    /// record is an error).
    pub(crate) async fn cas_mutate_inode_record(
        &self,
        inode_id: uuid::Uuid,
        trace_id: &TraceId,
        mut mutate: impl FnMut(&mut InodeRecord) -> Result<(), FsError>,
    ) -> Result<InodeRecord, FsError> {
        const MAX_RETRIES: u32 = 5;
        let key = InodeRecord::key_for(inode_id);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut record = self.backend().get_inode_record(inode_id, trace_id).await?;
            // Re-serialize the fetched record as the CAS guard. rkyv output is
            // deterministic for these map-free layout types, and the override
            // flush's own CAS already relies on exactly that, so this matches
            // the stored bytes without a separate raw-bytes fetch.
            let old_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&record, Vec::new())
                .map_err(FsError::from)?
                .into();
            // A fallible mutate lets the caller abort against the freshly
            // fetched record (e.g. `link` refusing to revive a record whose
            // last link is already gone) without publishing anything.
            mutate(&mut record)?;
            let new_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&record, Vec::new())
                .map_err(FsError::from)?
                .into();
            match self
                .backend()
                .put_inode_cas(&key, new_bytes, old_bytes, trace_id)
                .await
            {
                Ok(_) => return Ok(record),
                Err(FsError::CasConflict) if attempt < MAX_RETRIES => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Undo the nlink increment a `link` made when its destination publish
    /// then failed, so a failed first link can't strand a record at an
    /// inflated count (which would block its eventual reclamation). The
    /// decrement is itself a retrying CAS (`cas_mutate_inode_record`); if it
    /// still fails it is surfaced loudly; the residual case needs the same
    /// orphan-reconcile sweep as the unlink path.
    pub(crate) async fn compensate_link_increment(&self, inode_id: uuid::Uuid, trace_id: &TraceId) {
        if let Err(e) = self
            .cas_mutate_inode_record(inode_id, trace_id, |r| {
                r.nlink = r.nlink.saturating_sub(1);
                Ok(())
            })
            .await
        {
            tracing::warn!(
                %inode_id, error = %e,
                "link: could not compensate nlink after a failed destination \
                 publish; link count may be inflated until reconciled"
            );
        }
    }

    /// Create a hardlink `new_parent/new_name` to the file at `inode`.
    ///
    /// The first link promotes the file: its real layout is moved into a
    /// `@hardlink/<uuid>` `InodeRecord` (nlink=2) and both the original
    /// name and the new name become `Indirect(uuid)` redirects to it.
    /// A subsequent link to an already-promoted inode just bumps nlink
    /// and writes another redirect. Hardlinks to directories are EPERM
    /// (EISDIR here).
    pub async fn vfs_link(
        &self,
        inode: InodeId,
        new_parent: InodeId,
        new_name: &str,
    ) -> Result<VfsAttr, FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(new_name)?;
        self.ensure_writeback_worker_started();

        // Source key + cached inode_id (Some once already promoted).
        let (src_key, entry_type, cached_inode_id) = self
            .inodes
            .get(inode)
            .map(|e| (e.s3_key.clone(), e.entry_type, e.inode_id))
            .ok_or(FsError::NotFound)?;

        if entry_type == EntryType::Directory {
            return Err(FsError::IsDir);
        }

        let new_prefix = self.dir_prefix(new_parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&new_prefix, new_name)?;
        let new_key = format!("{}{}", new_prefix, new_name);

        let trace_id = TraceId::new();

        // Drain any pending publish for the destination name before the
        // EEXIST probe, including the queue's per-key inode records: a
        // FORGET can evict the InodeTable entry while a create intent is
        // still queued, and draining only the table's inode would let that
        // create commit after the link and clobber it. Mirrors the
        // unlink / rmdir / rename drains.
        let dst_ino = self.inodes.find_ino_by_key(&new_key, EntryType::File);
        for ino in self.writeback_drain_targets(&new_key, dst_ino) {
            self.flush_dirty_handles_for_inode(ino).await?;
            self.drain_inode_to_barrier(ino).await?;
        }

        // EEXIST if the destination name already exists. This also
        // subsumes the `link(a, a)` case (the source name is live, so
        // get_inode returns it), without a separate `new_key ==
        // src_key` guard, which would misfire for a promoted inode whose
        // cached `s3_key` is a since-unlinked alias (link/02.t,
        // link/03.t re-link a freed long name).
        match self.backend().get_inode(&new_key, &trace_id).await {
            Ok(_) => return Err(FsError::AlreadyExists),
            Err(FsError::NotFound) => {}
            Err(e) => return Err(e),
        }

        // Drain a pending publish for the source so we promote against
        // the post-flush layout, not a stale placeholder.
        self.flush_dirty_handles_for_inode(inode).await?;
        self.drain_inode_to_barrier(inode).await?;

        let now = now_ns();

        // POSIX: link(2) bumps the file's ctime. Stamp it into the record's
        // layout so a later lookup repopulating posix from the record sees
        // it (the in-memory mutation alone would be lost).
        let bump_link = |r: &mut InodeRecord| -> Result<(), FsError> {
            // Refuse to revive a record whose last link is already gone
            // (nlink == 0, awaiting reclaim by a concurrent unlink). Once
            // nlink hits 0 it stays there, so the unlink's post-commit
            // reclaim is safe: a racing link either commits its bump before
            // the decrement (the decrement then observes nlink > 0 and skips
            // reclaim) or observes nlink == 0 here and fails with ENOENT.
            if r.nlink == 0 {
                return Err(FsError::NotFound);
            }
            r.nlink = r.nlink.saturating_add(1);
            let mut p = crate::inode::layout_posix(&r.layout);
            p.ctime_ns = now;
            r.layout = crate::inode::layout_with_posix(r.layout.clone(), p);
            Ok(())
        };

        // The Indirect redirect bytes written at a promoted name (non-state
        // fields are placeholders; the record is authoritative).
        let make_redirect_bytes = |id: uuid::Uuid| -> Result<Bytes, FsError> {
            let l = ObjectLayout {
                timestamp: now / 1_000_000,
                version_id: ObjectLayout::gen_version_id(),
                block_size: DEFAULT_BLOCK_SIZE,
                blob_version: 0,
                fs_ext: None,
                state: ObjectState::Indirect(IndirectEntry { inode_id: id }),
            };
            let b: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&l, Vec::new())
                .map_err(FsError::from)?
                .into();
            Ok(b)
        };

        // Resolve to a shared inode_id, joining or creating the record.
        //   - cached inode_id: already promoted; bump nlink under CAS.
        //   - src layout Indirect: promoted, cache cold; follow + bump.
        //   - fresh normal source: promote ATOMICALLY: mint a record then
        //     CAS the source's NSS row from its exact normal bytes to an
        //     Indirect redirect. If that CAS loses (another client promoted
        //     first), discard our orphan record, re-read the now-Indirect
        //     redirect, and join the winner's record via the bump path, so
        //     concurrent first links converge on one record instead of each
        //     minting a divergent one and clobbering the source redirect.
        let (inode_id, record) = if let Some(inode_id) = cached_inode_id {
            let record = self
                .cas_mutate_inode_record(inode_id, &trace_id, bump_link)
                .await?;
            (inode_id, record)
        } else {
            // Promote a fresh source. A source-promotion CAS conflict does
            // NOT necessarily mean another linker won: a concurrent ordinary
            // write/chmod can also rewrite a still-normal source. So loop
            // (bounded): re-read the source each time and either join a
            // winner's record (now Indirect) or re-promote from the fresh
            // normal bytes (still Normal). One minted record id is reused
            // across attempts and dropped if we end up joining.
            let new_id = uuid::Uuid::new_v4();
            let mut record_created = false;
            const MAX_PROMOTE_RETRIES: u32 = 5;
            let mut attempt = 0;
            loop {
                attempt += 1;
                let src_layout = self.backend().get_inode(&src_key, &trace_id).await?;
                match &src_layout.state {
                    ObjectState::Indirect(redirect) => {
                        let id = redirect.inode_id;
                        if id == new_id {
                            // An earlier ambiguous CAS (e.g. a timeout) had
                            // actually installed our redirect. Recover it as a
                            // successful promotion rather than deleting new_id
                            // and dangling the source.
                            let record = self.backend().get_inode_record(new_id, &trace_id).await?;
                            break (new_id, record);
                        }
                        // Another linker won; the source points elsewhere, so
                        // our CAS never landed; drop our orphan and join theirs.
                        if record_created {
                            let _ = self.backend().delete_inode_record(new_id, &trace_id).await;
                        }
                        let record = self
                            .cas_mutate_inode_record(id, &trace_id, bump_link)
                            .await?;
                        break (id, record);
                    }
                    ObjectState::Directory(_) | ObjectState::Mpu(MpuState::Uploading) => {
                        // Source is not Indirect -> our CAS never landed.
                        if record_created {
                            let _ = self.backend().delete_inode_record(new_id, &trace_id).await;
                        }
                        return Err(FsError::IsDir);
                    }
                    ObjectState::Normal(_)
                    | ObjectState::Mpu(MpuState::Completed(_))
                    | ObjectState::Symlink(_)
                    | ObjectState::Special(_) => {
                        if attempt > MAX_PROMOTE_RETRIES {
                            // Still normal after all retries -> our CAS never
                            // landed -> new_id is a true orphan.
                            if record_created {
                                let _ = self.backend().delete_inode_record(new_id, &trace_id).await;
                            }
                            return Err(FsError::CasConflict);
                        }
                        let record = InodeRecord {
                            layout: crate::inode::layout_with_posix(src_layout.clone(), {
                                let mut p = crate::inode::layout_posix(&src_layout);
                                p.ctime_ns = now;
                                p
                            }),
                            nlink: 2,
                            orphan_since: None,
                        };
                        // (Re)seed the record from the current bytes, then
                        // flip the source row guarded on those exact bytes
                        // (the current normal layout re-serialized). On ANY CAS
                        // failure, conflict OR ambiguous (timeout), do NOT
                        // delete here: loop and re-read. The next iteration
                        // recovers (Indirect == new_id), joins (Indirect !=
                        // new_id), or re-promotes (still Normal).
                        let src_bytes: Bytes =
                            to_bytes_in::<_, rkyv::rancor::Error>(&src_layout, Vec::new())
                                .map_err(FsError::from)?
                                .into();
                        self.backend()
                            .put_inode_record(new_id, &record, &trace_id)
                            .await?;
                        record_created = true;
                        if self
                            .backend()
                            .put_inode_cas(
                                &src_key,
                                make_redirect_bytes(new_id)?,
                                src_bytes,
                                &trace_id,
                            )
                            .await
                            .is_ok()
                        {
                            break (new_id, record);
                        }
                    }
                }
            }
        };

        // Persist the source's resolved hardlink identity NOW, before the
        // destination write. If the destination absence-CAS below fails
        // (EEXIST), the source must not be left cached as a normal layout
        // with inode_id == None; a later setattr would then take the
        // non-hardlink path and publish that stale layout over the source's
        // Indirect redirect.
        if let Some(mut e) = self.inodes.get_mut(inode) {
            e.layout = Some(record.layout.clone());
            e.posix = crate::inode::layout_posix(&record.layout);
            e.inode_id = Some(inode_id);
            e.cache_expiry = std::time::Instant::now();
        }

        // Create the destination redirect with an absence CAS (empty
        // expected_old requires the key to be absent). Two concurrent links
        // to the same new name, or different sources racing the same name,
        // can both pass the earlier existence check; the absence CAS lets
        // only one win.
        //
        // Reconcile the outcome carefully so a failed publish never strands
        // the record at an inflated nlink, and, more importantly, never
        // *under*-counts a live destination (which would let a later source
        // unlink drive nlink to 0 and reclaim a still-referenced record):
        //   - Ok: our redirect landed -> success.
        //   - CasConflict: the name is taken -> EEXIST + compensate.
        //   - other (ambiguous, e.g. timeout): re-read the name's exact
        //     bytes. Only if they equal the exact redirect WE wrote did our
        //     publish land (matching inode_id alone is insufficient: two
        //     concurrent links to the same destination share it); then it is
        //     success. If the name holds other bytes -> EEXIST + compensate.
        //     If it is confirmed absent -> the publish did not land ->
        //     surface the error + compensate. If the re-read itself fails we
        //     CANNOT confirm absence, so we do NOT compensate (an inflated
        //     count merely leaks; under-counting a live link loses data).
        let dst_redirect = make_redirect_bytes(inode_id)?;
        match self
            .backend()
            .put_inode_cas(&new_key, dst_redirect.clone(), Bytes::new(), &trace_id)
            .await
        {
            Ok(_) => {}
            Err(FsError::CasConflict) => {
                self.compensate_link_increment(inode_id, &trace_id).await;
                return Err(FsError::AlreadyExists);
            }
            Err(e) => match self.backend().get_inode(&new_key, &trace_id).await {
                Ok(l) => {
                    // Re-serialize and compare to the exact redirect we wrote:
                    // equal iff our (ambiguous) CAS actually landed. rkyv is
                    // deterministic for these types.
                    let raw: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&l, Vec::new())
                        .map_err(FsError::from)?
                        .into();
                    if raw != dst_redirect {
                        // Name occupied by something we did not write.
                        self.compensate_link_increment(inode_id, &trace_id).await;
                        return Err(FsError::AlreadyExists);
                    }
                    // Our redirect is present -> the ambiguous CAS landed ->
                    // success (fall through).
                }
                Err(FsError::NotFound) => {
                    // Confirmed absent -> publish did not land.
                    self.compensate_link_increment(inode_id, &trace_id).await;
                    return Err(e);
                }
                Err(_reread_err) => {
                    // Indeterminate: the publish may have committed. Leave
                    // nlink as-is rather than risk under-counting a live link.
                    return Err(e);
                }
            },
        }

        // Map the new name to the inode and refresh dir caches/times.
        self.inodes.add_alias(&new_key, EntryType::File, inode);

        self.cache_dir_entry(&new_prefix, new_name, inode, DirEntryKind::RegularFile);
        self.touch_parent_times(new_parent);

        let mut attr = self.make_file_attr(inode, &record.layout)?;
        attr.nlink = record.nlink;
        Ok(attr)
    }

    pub async fn vfs_lookup(&self, parent: InodeId, name: &str) -> Result<VfsAttr, FsError> {
        Self::check_name_max(name)?;
        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;

        let full_key = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}{}", prefix, name)
        };
        let dir_key = format!("{}/", full_key);

        // Directory membership survives FUSE_FORGET. Local mutations
        // invalidate this snapshot, and its TTL bounds peer changes.
        if let Some(false) = self.dir_cache.contains_name(&prefix, name) {
            return Err(FsError::NotFound);
        }

        let trace_id = TraceId::new();

        // Read-your-writes before the NSS probe only when local writeback
        // proves this name has an in-flight publish. This closes the race
        // where NSS returns NotFound and the worker commits before the later
        // fallback check, without serving arbitrary stale cached entries.
        if let Some(ino) = self.inodes.find_ino_by_key(&full_key, EntryType::File)
            && let Some(entry) = self.inodes.get(ino)
            && !entry.name_removed
            && (self.writeback.has_pending_intent_for_key(&full_key)
                || self.writeback.fsync_barrier(ino).is_some())
        {
            let layout = entry.layout.clone();
            drop(entry);
            // Decide what to serve BEFORE taking a refcount, so a layout we
            // cannot resolve locally falls through to the NSS resolve path
            // instead of leaking the kernel-lookup count on an error reply.
            let ryw_attr = if let Some(size) = self.dirty_write_buffer_size(ino) {
                // Fresh create whose first flush hasn't landed, or an
                // in-flight overwrite: the live write buffer is the
                // authoritative local size. A cached layout (if any) still
                // holds the stale pre-flush committed size, so prefer the
                // buffer. This also keeps the async close-flush window from
                // caching a negative dentry for a file that exists.
                Some(self.make_new_file_attr(ino, size))
            } else {
                match &layout {
                    // An Indirect hardlink redirect cached by a plain readdir
                    // has no servable size() (InvalidState). The alias already
                    // exists in NSS (link publishes it synchronously), so
                    // there is no negative-dentry race: fall through to the
                    // NSS resolve path, which follows the redirect correctly.
                    Some(l) if matches!(l.state, ObjectState::Indirect(_)) => None,
                    Some(l) => Some(self.make_file_attr(ino, l)?),
                    None => Some(self.make_new_file_attr(ino, 0)),
                }
            };
            if let Some(attr) = ryw_attr {
                // This LOOKUP reply resolves the inode without going through
                // `lookup_or_insert`, so bump the kernel-lookup refcount here
                // or a later FORGET under-counts and evicts a live inode.
                if let Some(e) = self.inodes.get(ino) {
                    e.increment_ref();
                }
                return Ok(attr);
            }
        }
        if let Some(ino) = self.inodes.find_ino_by_key(&dir_key, EntryType::Directory)
            && let Some(entry) = self.inodes.get(ino)
            && !entry.name_removed
        {
            let has_pending = self.writeback.has_pending_intent_for_key(&dir_key);
            let is_tainted = self.writeback.is_tainted(ino);
            if has_pending || is_tainted {
                drop(entry);
                if is_tainted {
                    self.drain_inode_to_barrier(ino).await?;
                }
                let entry = self.inodes.get(ino).ok_or(FsError::NotFound)?;
                if entry.name_removed {
                    return Err(FsError::NotFound);
                }
                entry.increment_ref();
                drop(entry);
                return Ok(self.make_dir_attr(ino));
            }
        }

        // Try as file first. Use is_fs_visible (not is_listable) so
        // special files (fifo / device / socket), which the S3 listing
        // API hides, are still resolvable through FUSE lookup.
        match self.backend().get_inode(&full_key, &trace_id).await {
            Ok(layout) => {
                if !layout.is_fs_visible() {
                    return Err(FsError::NotFound);
                }
                // Follow an Indirect hardlink redirect to its real
                // layout + nlink, caching the inode_id on the entry.
                let (real_layout, inode_id, nlink) =
                    self.resolve_indirect(layout, &trace_id).await?;
                let (ino, _) = self.inodes.lookup_or_insert(
                    &full_key,
                    EntryType::File,
                    Some(real_layout.clone()),
                );
                if let Some(mut e) = self.inodes.get_mut(ino) {
                    // Cross-instance coherency: lookup_or_insert leaves an
                    // EXISTING entry's layout untouched, so a peer instance's
                    // override (new blob_version + size) would otherwise stay
                    // masked behind our stale cached layout; getattr reads
                    // size from entry.layout, so a follow-up stat (after the
                    // 1s lookup-attr TTL) would report the old size even
                    // though this lookup already fetched the fresh one from
                    // NSS. Refresh the cached layout to the just-read
                    // authoritative one. (Local unflushed writes live in the
                    // handle's write_buf, and unflushed setattr in
                    // entry.posix, so neither is clobbered here.)
                    e.layout = Some(real_layout.clone());
                    if let Some(id) = inode_id {
                        // Hardlink: also refresh the cached posix from the
                        // shared record so a chmod/chown/unlink-ctime-bump
                        // made via another name isn't masked by stale posix
                        // (make_file_attr reads posix for mode/times).
                        e.inode_id = Some(id);
                        e.posix = crate::inode::layout_posix(&real_layout);
                    }
                }
                let mut attr = self.make_file_attr(ino, &real_layout)?;
                attr.nlink = nlink;
                // Size authority: the NSS layout size is a lazy copy that can
                // lag a peer instance's most recent override, so the dentry
                // attr this LOOKUP installs (and the i_size the kernel derives
                // from it) would otherwise be stale; a follow-up read clamps
                // to the old size. Override with the authoritative geometry
                // sentinel so cross-instance stat/read see the latest EOF.
                let auth_size = self.authoritative_file_size(&real_layout).await?;
                if auth_size != attr.size {
                    attr.size = auth_size;
                    attr.blocks = auth_size.div_ceil(512);
                }
                return Ok(attr);
            }
            Err(FsError::NotFound) => {}
            Err(e) => return Err(e),
        }

        // Try as directory. Read the directory's own layout when a
        // marker is present so its persisted posix (mode/uid/gid/times)
        // seeds the inode entry; a Directory layout carries posix, a
        // legacy Normal marker does not (defaults apply).
        match self.backend().get_inode(&dir_key, &trace_id).await {
            Ok(layout) => {
                let seed = if layout.is_directory() {
                    Some(layout)
                } else {
                    None
                };
                let (ino, _) = self
                    .inodes
                    .lookup_or_insert(&dir_key, EntryType::Directory, seed);
                return Ok(self.make_dir_attr(ino));
            }
            Err(FsError::NotFound) => {}
            Err(e) => return Err(e),
        }

        // Read-your-writes: a just-created entry NSS doesn't have yet must
        // still resolve from the in-memory inode, but only when there's a
        // genuine in-flight reason it's missing from NSS, NOT for any stale
        // cached entry. Otherwise an entry deleted by another instance (NSS
        // says gone, but our cache still holds it because it was never
        // FUSE-unlinked here) would be resurrected and a follow-up read
        // would EIO on the deleted blocks instead of returning ENOENT.
        //
        // "In-flight" means either a pending writeback intent (async
        // metadata create/chmod/mkdir/symlink/mknod not yet drained) or an
        // open file handle (a regular-file create whose close-time flush
        // hasn't published to NSS yet). When neither holds, NSS's miss is
        // authoritative.
        if let Some(ino) = self.inodes.find_ino_by_key(&full_key, EntryType::File)
            && let Some(entry) = self.inodes.get(ino)
            && !entry.name_removed
            && (self.writeback.has_pending_intent_for_key(&full_key)
                || self.has_open_handles_for_inode(ino, None)
                // A tainted inode had its publish fail: NSS has nothing,
                // but the name must stay resolvable so the deferred EIO
                // is reachable through the next open instead of the file
                // silently vanishing as ENOENT.
                || self.writeback.is_tainted(ino))
        {
            let layout = entry.layout.clone();
            entry.increment_ref();
            drop(entry);
            return match layout {
                Some(layout) => self.make_file_attr(ino, &layout),
                None => Ok(self.make_new_file_attr(ino, self.dirty_buffer_size(ino))),
            };
        }
        if let Some(ino) = self.inodes.find_ino_by_key(&dir_key, EntryType::Directory)
            && let Some(entry) = self.inodes.get(ino)
            && !entry.name_removed
        {
            let has_pending = self.writeback.has_pending_intent_for_key(&dir_key);
            let is_tainted = self.writeback.is_tainted(ino);
            if has_pending || is_tainted {
                drop(entry);
                if is_tainted {
                    self.drain_inode_to_barrier(ino).await?;
                }
                let entry = self.inodes.get(ino).ok_or(FsError::NotFound)?;
                if entry.name_removed {
                    return Err(FsError::NotFound);
                }
                entry.increment_ref();
                drop(entry);
                return Ok(self.make_dir_attr(ino));
            }
        }

        // Fall back to a prefix listing for implicit directories that
        // have children but no marker inode of their own.
        let entries = self
            .backend()
            .list_inodes(&dir_key, "/", "", 1, &trace_id)
            .await;

        match entries {
            Ok(entries) if !entries.is_empty() => {
                let (ino, _) = self
                    .inodes
                    .lookup_or_insert(&dir_key, EntryType::Directory, None);
                Ok(self.make_dir_attr(ino))
            }
            _ => Err(FsError::NotFound),
        }
    }

    pub fn vfs_forget(&self, inode: InodeId, nlookup: u64) {
        // Pin the entry while an open handle (async release flush) or
        // queued writeback state still needs it: a flush whose entry
        // vanished mid-flight would publish default posix (mode 0,
        // uid 0), and a queued intent would lose its read-your-writes
        // anchor and its delete-drain identity. The pin is reaped once
        // the flush / worker drains (`reap_forgotten_inode`).
        let pin =
            self.has_open_handles_for_inode(inode, None) || self.writeback.has_live_state(inode);
        match self.inodes.forget(inode, nlookup, pin) {
            ForgetOutcome::Removed => self.writeback.prune_inode_if_idle(inode),
            ForgetOutcome::KeptZeroed => self.writeback.mark_forgotten(inode),
            ForgetOutcome::Live => {}
        }
        // Sweep entries whose pin drained after their FORGET.
        for ino in self.writeback.take_reapable() {
            self.reap_forgotten_inode(ino);
        }
    }

    /// Finish a FORGET that was deferred because writeback state or an
    /// open handle pinned the entry. No-op when the kernel still
    /// references the inode or a lookup revived it; re-queued when the
    /// pin is still live.
    pub(crate) fn reap_forgotten_inode(&self, ino: InodeId) {
        if !self.inodes.is_unreferenced(ino) {
            return;
        }
        if self.has_open_handles_for_inode(ino, None) || self.writeback.has_live_state(ino) {
            self.writeback.mark_forgotten(ino);
            return;
        }
        if self.inodes.remove_if_unreferenced(ino) {
            self.writeback.prune_inode_if_idle(ino);
        }
    }

    /// Create a fifo / block / char / unix-socket inode (the kernel
    /// routes both `mknod(2)` and `mkfifo(2)` here). fs_server only
    /// round-trips the metadata; the kernel owns all I/O against the
    /// open fd.
    pub async fn vfs_mknod(
        &self,
        parent: InodeId,
        name: &str,
        kind: SpecialKind,
        rdev: u32,
        init_posix: PosixAttrs,
    ) -> Result<VfsAttr, FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;
        self.ensure_writeback_worker_started();

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}", prefix, name);

        let trace_id = TraceId::new();
        match self.backend().get_inode(&key, &trace_id).await {
            Ok(_) => return Err(FsError::AlreadyExists),
            Err(FsError::NotFound) => {}
            Err(e) => return Err(e),
        }

        let ifmt = match kind {
            SpecialKind::Fifo => libc::S_IFIFO,
            SpecialKind::BlockDevice => libc::S_IFBLK,
            SpecialKind::CharDevice => libc::S_IFCHR,
            SpecialKind::Socket => libc::S_IFSOCK,
        };
        let mut posix = init_posix;
        // Re-stamp the right S_IFMT bits even if the caller passed only
        // permission bits, so a cross-instance stat sees the right kind.
        if posix.mode != 0 {
            posix.mode = (posix.mode & !libc::S_IFMT) | ifmt;
        }

        let layout = ObjectLayout {
            version_id: ObjectLayout::gen_version_id(),
            block_size: DEFAULT_BLOCK_SIZE,
            timestamp: now_ns() / 1_000_000,
            blob_version: 0,
            fs_ext: ObjectLayout::fs_ext_from(Some(posix)),
            state: ObjectState::Special(SpecialData {
                kind,
                rdev,
                core_meta_data: ObjectCoreMetaData {
                    size: 0,
                    etag: String::new(),
                    headers: vec![],
                    checksum: None,
                },
            }),
        };

        let layout_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())
            .map_err(FsError::from)?
            .into();

        // Cache the inode (and its layout) before publishing so the
        // async path has an `ino` to open a cycle against and a
        // read-your-writes lookup can serve the not-yet-committed entry.
        let (ino, _) = self
            .inodes
            .lookup_or_insert(&key, EntryType::File, Some(layout.clone()));

        self.publish_inode_layout(ino, &key, &prefix, name, layout_bytes, &trace_id)
            .await?;

        self.cache_dir_entry(
            &prefix,
            name,
            ino,
            Self::dir_entry_kind_from_layout(&layout),
        );
        self.touch_parent_times(parent);

        self.make_file_attr(ino, &layout)
    }

    pub async fn vfs_create(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<(VfsAttr, FileHandleId), FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}", prefix, name);

        let (ino, _) = self.inodes.lookup_or_insert(&key, EntryType::File, None);

        // Seed the in-memory posix from the create mode + caller ids so
        // the file reports the right st_mode/uid/gid before its first
        // flush; the flush folds this into the persisted layout.
        let now = now_ns();
        if let Some(mut entry) = self.inodes.get_mut(ino) {
            entry.posix = PosixAttrs {
                mode: (mode & !libc::S_IFMT) | libc::S_IFREG,
                uid,
                gid,
                mtime_ns: now,
                ctime_ns: now,
            };
            entry.name_removed = false;
            entry.atime_ns = 0;
        }

        let fh = self.alloc_fh();
        // vfs_create implicitly opens the new file for writing,
        // so it must obey the inode-scoped write lock. A re-create on an
        // inode that already has a live write handle returns EBUSY.
        self.acquire_write_lock_retry(ino, fh).await?;
        self.file_handles.insert(
            fh,
            FileHandle {
                ino,
                s3_key: key,
                layout: None,
                write_buf: Some({
                    // Fresh empty file; dirty so the close-time flush
                    // publishes the 0-byte inode.
                    let mut wb = WriteBuffer::new(None, 0, DEFAULT_BLOCK_SIZE);
                    wb.dirty = true;
                    wb.size_changed = true;
                    wb
                }),
                backing_id: None,
            },
        );

        let attr = self.make_new_file_attr(ino, 0);

        self.cache_dir_entry(&prefix, name, ino, DirEntryKind::RegularFile);
        self.touch_parent_times(parent);

        Ok((attr, fh))
    }

    /// Create a symbolic link at `(parent, name)` whose body is
    /// `target`. The layout is published to NSS via an unconditional
    /// `put_inode` (this is a brand-new entry), no BSS blob is
    /// allocated, and the parent dir cache is invalidated so the new
    /// name shows up in listings. Existing entries at the same name
    /// fail the create with `AlreadyExists`.
    pub async fn vfs_symlink(
        &self,
        parent: InodeId,
        name: &str,
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> Result<VfsAttr, FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;
        self.ensure_writeback_worker_started();

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}", prefix, name);

        let trace_id = TraceId::new();

        // Reject if a name already exists at this path.
        match self.backend().get_inode(&key, &trace_id).await {
            Ok(_) => return Err(FsError::AlreadyExists),
            Err(FsError::NotFound) => {}
            Err(e) => return Err(e),
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Symlink permission bits are conventionally 0777 and ignored
        // by the kernel; uid/gid come from the caller so lchown can
        // adjust them.
        let now = now_ns();
        let posix = PosixAttrs {
            mode: symlink_mode(0o777),
            uid,
            gid,
            mtime_ns: now,
            ctime_ns: now,
        };

        let layout = ObjectLayout {
            version_id: ObjectLayout::gen_version_id(),
            block_size: DEFAULT_BLOCK_SIZE,
            timestamp,
            blob_version: 0,
            fs_ext: ObjectLayout::fs_ext_from(Some(posix)),
            state: ObjectState::Symlink(SymlinkData {
                target: target.to_vec(),
                core_meta_data: ObjectCoreMetaData {
                    size: target.len() as u64,
                    etag: String::new(),
                    headers: vec![],
                    checksum: None,
                },
            }),
        };

        let layout_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())
            .map_err(FsError::from)?
            .into();

        // Cache the inode (and layout) before publishing so the async
        // path has an `ino` for its cycle and a read-your-writes lookup
        // can serve the not-yet-committed symlink.
        let (ino, _) = self
            .inodes
            .lookup_or_insert(&key, EntryType::File, Some(layout.clone()));

        self.publish_inode_layout(ino, &key, &prefix, name, layout_bytes, &trace_id)
            .await?;

        self.cache_dir_entry(&prefix, name, ino, DirEntryKind::Symlink);
        self.touch_parent_times(parent);

        self.make_file_attr(ino, &layout)
    }

    /// Return the bytes a `readlink(2)` should hand back. Returns
    /// `InvalidArgument` (EINVAL) when the inode is not a symlink,
    /// matching the `readlink(2)` errno for non-symlink targets.
    pub async fn vfs_readlink(&self, inode: InodeId) -> Result<Vec<u8>, FsError> {
        let (key, cached_target, known_non_symlink) = {
            let entry = self.inodes.get(inode).ok_or(FsError::NotFound)?;
            if entry.entry_type != EntryType::File {
                return Err(FsError::InvalidArg);
            }
            let cached_target = entry
                .layout
                .as_ref()
                .and_then(|layout| layout.symlink_target().map(|target| target.to_vec()));
            let known_non_symlink = entry
                .layout
                .as_ref()
                .is_some_and(|layout| cached_target.is_none() && !layout.is_symlink());
            (entry.s3_key.clone(), cached_target, known_non_symlink)
        };

        if let Some(target) = cached_target {
            // The cached target is authoritative for read-your-writes, so
            // don't block on the async publish barrier. Still surface a
            // deferred publish failure once (errseq-style) so a lost symlink
            // create isn't silently hidden behind a stale local target.
            if self.writeback.take_taint(inode) {
                self.drop_cached_layout(inode);
                return Err(FsError::Internal("writeback drain".to_string()));
            }
            return Ok(target);
        }
        if known_non_symlink {
            return Err(FsError::InvalidArg);
        }

        // Cold path: re-fetch from NSS. This handles the case where
        // the inode entry was created by lookup but the layout was
        // dropped (memory pressure / eviction).
        if self.writeback.has_pending_intent_for_key(&key) || self.writeback.is_tainted(inode) {
            self.drain_inode_to_barrier(inode).await?;
        }

        let trace_id = TraceId::new();
        let layout = self.backend().get_inode(&key, &trace_id).await?;

        if let Some(target) = layout.symlink_target() {
            // Cache the layout for future lookups on this inode.
            if let Some(mut e) = self.inodes.get_mut(inode) {
                e.layout = Some(layout.clone());
            }
            Ok(target.to_vec())
        } else {
            Err(FsError::InvalidArg)
        }
    }

    /// Clean up the value that previously lived at `key` after it was
    /// unlinked or replaced by a rename. Handles every layout shape:
    ///   - `Normal`: GC the blob blocks (deferred when a handle is still
    ///     open so reads against the open fd keep working).
    ///   - `Mpu(Completed)`: GC each part blob and delete the part inodes.
    ///   - `Indirect`: decrement the shared `InodeRecord`'s nlink, bumping
    ///     the surviving file's ctime; when nlink reaches 0 delete the
    ///     record and GC the real blob (or stamp `orphan_since` if a
    ///     handle is still open). A redirect shares its blob with other
    ///     names, so it is never deferred as a whole-blob cleanup.
    pub(crate) async fn cleanup_orphaned_value(
        &self,
        key: &str,
        ino_hint: Option<InodeId>,
        old_bytes: Bytes,
        trace_id: &TraceId,
    ) {
        if old_bytes.is_empty() {
            return;
        }
        if let Some(ino) = ino_hint
            && self.has_open_handles_for_inode(ino, None)
            && !matches!(
                rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
                    .ok()
                    .as_ref()
                    .map(|l| &l.state),
                Some(ObjectState::Indirect(_))
            )
        {
            self.deferred_blob_cleanup.insert(ino, old_bytes);
            return;
        }
        let Ok(old_layout) = rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
        else {
            return;
        };
        match &old_layout.state {
            ObjectState::Normal(_) => {
                if let Ok(blob_guid) = old_layout.blob_guid() {
                    let _ = self.backend().delete_blob_blocks(blob_guid, trace_id).await;
                }
            }
            ObjectState::Mpu(MpuState::Completed(_)) => {
                if let Ok(parts) = self.backend().list_mpu_parts(key, trace_id).await {
                    for (part_key, part_layout) in &parts {
                        if let Ok(blob_guid) = part_layout.blob_guid() {
                            let _ = self.backend().delete_blob_blocks(blob_guid, trace_id).await;
                        }
                        let _ = self.backend().delete_inode(part_key, trace_id).await;
                    }
                }
            }
            ObjectState::Indirect(redirect) => {
                let inode_id = redirect.inode_id;
                // Whether an open fd still references the inode is
                // independent of nlink; decide it up front so the CAS
                // mutation can fold orphan-marking into the same write.
                let still_open = ino_hint
                    .map(|i| self.has_open_handles_for_inode(i, None))
                    .unwrap_or(false);
                // CAS-decrement so a concurrent record-aware flush isn't
                // clobbered (and vice versa); on nlink>0 stamp the surviving
                // file's ctime, on the last link mark orphan if a handle
                // still holds it.
                let committed = self
                    .cas_mutate_inode_record(inode_id, trace_id, |r| {
                        r.nlink = r.nlink.saturating_sub(1);
                        if r.nlink > 0 {
                            let mut p = crate::inode::layout_posix(&r.layout);
                            p.ctime_ns = now_ns();
                            r.layout = crate::inode::layout_with_posix(r.layout.clone(), p);
                        } else if still_open {
                            r.orphan_since = Some(now_ns());
                        }
                        Ok(())
                    })
                    .await;
                match committed {
                    Ok(record) if record.nlink == 0 && !still_open => {
                        // Reclaim the shared blob + record. This is safe
                        // against a racing link: `bump_link` refuses to
                        // revive an nlink==0 record, so a link can only have
                        // committed *before* our decrement (then we observe
                        // nlink>0 above and skip), never after. The re-read
                        // confirms nlink is still 0 before deleting.
                        if let Ok(fresh) = self.backend().get_inode_record(inode_id, trace_id).await
                            && fresh.nlink == 0
                        {
                            if let Ok(blob_guid) = fresh.layout.blob_guid() {
                                let _ =
                                    self.backend().delete_blob_blocks(blob_guid, trace_id).await;
                            }
                            let _ = self.backend().delete_inode_record(inode_id, trace_id).await;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        // The name is already removed but the shared link
                        // count could not be decremented (e.g. CAS retries
                        // exhausted under sustained contention). Surface it
                        // rather than silently leaving st_nlink too high /
                        // leaking the blob; a record repair/GC sweep would
                        // reconcile.
                        tracing::warn!(
                            %inode_id, error = %e,
                            "unlink: failed to decrement hardlink record nlink; \
                             link count may be stale until reconciled"
                        );
                    }
                }
            }
            _ => {}
        }
    }

    pub async fn vfs_unlink(&self, parent: InodeId, name: &str) -> Result<(), FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}", prefix, name);

        let trace_id = TraceId::new();

        // With async metadata, a just-created (or just-chmod'd) inode
        // may still have a PutInode queued. Drain it before the delete
        // so (a) the delete sees the entry in NSS instead of racing to
        // a spurious ENOENT, and (b) the worker can't re-publish it
        // after the delete and resurrect the name. The queue's own
        // per-key inode records are drained too: a FORGET can evict the
        // InodeTable entry while its intent is still queued.
        let ino = self.inodes.find_ino_by_key(&key, EntryType::File);
        let mut tainted_delete = false;
        if self.writeback_mode == WritebackMode::Default {
            for target in self.writeback_drain_targets(&key, ino) {
                // Best-effort: the buffered data is discarded with the name
                // anyway (see drain_inode_for_delete's taint-tolerant
                // contract), so a failed publish must not wedge the delete
                // in permanent EIO. Blocks from a partial flush are
                // reconciled by GC.
                if let Err(e) = self.flush_dirty_handles_for_inode(target).await {
                    tracing::warn!(
                        target = target.0, key = %key, error = %e,
                        "unlink: pre-delete flush failed; proceeding with delete"
                    );
                }
                tainted_delete |= self.drain_inode_for_delete(target).await?;
            }
        }

        // Delete the inode from NSS
        let old_bytes = self.backend().delete_inode(&key, &trace_id).await?;

        let old_bytes = match old_bytes {
            Some(bytes) => bytes,
            // A tainted target's create publish failed: NSS has nothing,
            // but the name is still locally visible (lookup keeps a tainted
            // name resolvable). Finish the delete locally instead of
            // failing a visible name with ENOENT.
            None if tainted_delete => {
                if let Some(ino) = ino {
                    self.inodes.remove_name_mapping(ino);
                }
                self.dir_cache.invalidate(&prefix);
                self.touch_parent_times(parent);
                return Ok(());
            }
            // Return ENOENT if file didn't exist
            None => return Err(FsError::NotFound),
        };

        // Drop this name from the inode table. A hardlink redirect keeps
        // the inode (and its other names) live, so only its alias goes;
        // a single-named file is marked `name_removed` so a still-open fd
        // reports nlink=0 and any in-flight setattr/flush skips re-publish.
        let is_indirect = rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
            .map(|l| matches!(l.state, ObjectState::Indirect(_)))
            .unwrap_or(false);
        if is_indirect {
            self.inodes.remove_alias(&key, EntryType::File);
        } else if let Some(ino) = ino {
            self.inodes.remove_name_mapping(ino);
        }

        // GC the value (blob blocks, or a hardlink nlink decrement).
        self.cleanup_orphaned_value(&key, ino, old_bytes, &trace_id)
            .await;

        // Invalidate dir cache for parent
        self.dir_cache.invalidate(&prefix);
        self.touch_parent_times(parent);

        Ok(())
    }

    pub async fn vfs_mkdir(
        &self,
        parent: InodeId,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<VfsAttr, FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}/", prefix, name);

        let trace_id = TraceId::new();

        // Persist a Directory layout carrying the requested mode + caller
        // ids (instead of the plain marker) so chmod/chown/utime against
        // the directory survive a forget+relookup.
        let now = now_ns();
        let posix = PosixAttrs {
            mode: (mode & !libc::S_IFMT) | libc::S_IFDIR,
            uid,
            gid,
            mtime_ns: now,
            ctime_ns: now,
        };
        let layout = ObjectLayout {
            version_id: ObjectLayout::gen_version_id(),
            block_size: DEFAULT_BLOCK_SIZE,
            timestamp: now / 1_000_000,
            blob_version: 1,
            fs_ext: None,
            state: ObjectState::Directory(DirectoryData { posix }),
        };
        let layout_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&layout, Vec::new())
            .map_err(FsError::from)?
            .into();

        let (ino, _) =
            self.inodes
                .lookup_or_insert(&key, EntryType::Directory, Some(layout.clone()));

        self.publish_inode_layout(ino, &key, &prefix, name, layout_bytes, &trace_id)
            .await?;

        self.cache_dir_entry(&prefix, name, ino, DirEntryKind::Directory);
        self.dir_cache
            .insert_empty_dir(key.clone(), ino.0, parent.0);
        self.touch_parent_times(parent);

        Ok(self.make_dir_attr(ino))
    }

    pub async fn vfs_rmdir(&self, parent: InodeId, name: &str) -> Result<(), FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;

        let prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&prefix, name)?;
        let key = format!("{}{}/", prefix, name);

        let trace_id = TraceId::new();

        // Drain a pending async directory publish before the existence /
        // emptiness probe, so a just-created dir is committed to NSS and
        // the worker can't re-publish it after the delete. Includes the
        // queue's per-key inode records: a FORGET can evict the
        // InodeTable entry while its intent is still queued.
        let ino = self.inodes.find_ino_by_key(&key, EntryType::Directory);
        let mut tainted_delete = false;
        if self.writeback_mode == WritebackMode::Default {
            for target in self.writeback_drain_targets(&key, ino) {
                tainted_delete |= self.drain_inode_for_delete(target).await?;
            }
        }

        // A child create may have returned to the caller while its
        // default-mode PutInode is still queued or in flight. NSS
        // listing alone can miss that child, so preserve the POSIX
        // non-empty contract from the in-memory writeback queue first.
        if self.writeback_mode == WritebackMode::Default
            && self.writeback.has_pending_child_put_inode_for_parent(&key)
        {
            return Err(FsError::NotEmpty);
        }

        // Regular-file creates publish their final layout on close, so the
        // writeback queue child check above cannot see an open or async-
        // closing FILE child. A locally cached file child is already
        // visible to this mount and must keep rmdir from winning the race.
        // Only files, not dirs: a cached dir child can be a phantom (a
        // tombstoned subtree still emits a CommonPrefix into the readdir
        // cache), so dir emptiness is decided by the tombstone-filtering
        // no-delimiter NSS list below, not this cache (pjdfstest
        // mkdir/03.t, rmdir/03.t: rm -rf of a deep tree after a
        // mkdir+rmdir of the leaf).
        if self.dir_cache.has_file_children(&key) == Some(true) {
            return Err(FsError::NotEmpty);
        }

        // The dir_cache check above only sees children this mount cached a
        // listing for. A file child created and released under `key` while
        // the parent listing was absent or invalidated publishes its layout
        // via an async release cycle (not a PutInode intent), so it is
        // invisible to both checks above and not yet in NSS. Consult local
        // open-handle / in-flight-cycle state so rmdir can't delete the
        // directory out from under it.
        if self.writeback_mode == WritebackMode::Default && self.has_local_file_child_under(&key) {
            return Err(FsError::NotEmpty);
        }

        // List to check existence and emptiness. Use NO delimiter so
        // NSS walks leaves directly and filters tombstones: the list
        // path only drops tombstoned entries on the LEAF branch. With
        // delimiter "/" a fully-tombstoned subtree still emits a
        // CommonPrefix entry, so `rm -rf` of a deep tree would see a
        // phantom child here and fail with ENOTEMPTY even though every
        // descendant is already deleted (pjdfstest chmod/03.t). Without
        // a delimiter we read raw leaves with tombstones filtered: the
        // dir marker itself plus any live descendant. max_keys=2 is
        // enough; anything other than the marker means non-empty.
        let entries = self
            .backend()
            .list_inodes(&key, "", "", 2, &trace_id)
            .await?;

        // If no entries at all, directory doesn't exist. Exception: a
        // tainted target's mkdir publish failed, so NSS has no marker but
        // the name is still locally visible (lookup keeps a tainted dir
        // resolvable). Finish the delete locally instead of failing the
        // visible name with ENOENT.
        if entries.is_empty() {
            if tainted_delete {
                if let Some(ino) = ino {
                    self.inodes.remove_name_mapping(ino);
                }
                self.dir_cache.invalidate(&prefix);
                self.dir_cache.invalidate(&key);
                self.touch_parent_times(parent);
                return Ok(());
            }
            return Err(FsError::NotFound);
        }

        let has_children = entries.iter().any(|e| e.key != key);
        if has_children {
            return Err(FsError::NotEmpty);
        }

        // Delete the directory marker
        self.backend().delete_inode(&key, &trace_id).await?;

        // Remove from inode table (marks name_removed, no refcount leak)
        if let Some(ino) = ino {
            self.inodes.remove_name_mapping(ino);
        }

        // Invalidate dir cache for parent and self
        self.dir_cache.invalidate(&prefix);
        self.dir_cache.invalidate(&key);
        self.touch_parent_times(parent);

        Ok(())
    }

    pub async fn vfs_rename(
        &self,
        parent: InodeId,
        name: &str,
        new_parent: InodeId,
        new_name: &str,
    ) -> Result<(), FsError> {
        self.check_write_enabled()?;
        Self::check_name_max(name)?;
        Self::check_name_max(new_name)?;

        let src_prefix = self.dir_prefix(parent).ok_or(FsError::NotFound)?;
        let dst_prefix = self.dir_prefix(new_parent).ok_or(FsError::NotFound)?;
        Self::check_path_max(&src_prefix, name)?;
        Self::check_path_max(&dst_prefix, new_name)?;

        let src_key = format!("{}{}", src_prefix, name);
        let dst_key = format!("{}{}", dst_prefix, new_name);

        let trace_id = TraceId::new();

        let src_file_ino = self.inodes.find_ino_by_key(&src_key, EntryType::File);
        for ino in self.writeback_drain_targets(&src_key, src_file_ino) {
            self.flush_dirty_handles_for_inode(ino).await?;
            self.drain_inode_to_barrier(ino).await?;
        }
        let dst_ino_before = self.inodes.find_ino_by_key(&dst_key, EntryType::File);
        for ino in self.writeback_drain_targets(&dst_key, dst_ino_before) {
            self.flush_dirty_handles_for_inode(ino).await?;
            self.drain_inode_to_barrier(ino).await?;
        }
        // A just-created directory publishes its marker via an async
        // PutInode (Default writeback mode), so the NSS probe + rename below
        // would otherwise miss it and ENOENT, and a queued publish could
        // resurrect the old name after the rename. Drain the source (and a
        // replaced destination) directory barrier first, mirroring the file
        // drains above (pjdfstest rename/21.t renames a just-mkdir'd dir).
        // Like the delete drains, the queue's per-key inode records are
        // included so a FORGET-evicted entry can't skip the drain.
        let src_dir_probe = format!("{}/", src_key);
        let src_dir_ino = self
            .inodes
            .find_ino_by_key(&src_dir_probe, EntryType::Directory);
        for ino in self.writeback_drain_targets(&src_dir_probe, src_dir_ino) {
            self.drain_inode_to_barrier(ino).await?;
        }
        let dst_dir_probe = format!("{}/", dst_key);
        let dst_dir_ino = self
            .inodes
            .find_ino_by_key(&dst_dir_probe, EntryType::Directory);
        for ino in self.writeback_drain_targets(&dst_dir_probe, dst_dir_ino) {
            self.drain_inode_to_barrier(ino).await?;
        }

        // Determine type by probing NSS backend directly (no inode side effects)
        let is_dir = match self.backend().get_inode(&src_key, &trace_id).await {
            Ok(_) => false,
            Err(FsError::NotFound) => true,
            Err(e) => return Err(e),
        };

        if is_dir {
            let src_dir_key = format!("{}/", src_key);
            let dst_dir_key = format!("{}/", dst_key);

            // Block async enqueues under the source subtree across the drain
            // + rename. The kernel locks only the rename's parents, not the
            // moved directory, so a create racing in under it (e.g. mkdir
            // dir/sub during mv dir dir2) would otherwise leave an intent the
            // worker commits at the stale pre-rename key long after
            // rename_folder ran, resurrecting a ghost under the old path.
            // Blocking first forces such a create onto the synchronous
            // publish fallback, narrowing the residual race to the strict-mode
            // window. The guard releases the block on every exit path.
            self.writeback.block_prefix(&src_dir_key);
            let _block_guard = PrefixBlockGuard {
                writeback: Arc::clone(&self.writeback),
                prefix: src_dir_key.clone(),
            };

            self.drain_writeback_under_prefix(&src_dir_key).await?;

            self.backend()
                .rename_folder(&src_dir_key, &dst_dir_key, &trace_id)
                .await?;

            // Update the directory inode's s3_key since the kernel still
            // holds a reference to it after rename.
            if let Some(ino) = self
                .inodes
                .find_ino_by_key(&src_dir_key, EntryType::Directory)
            {
                self.inodes.update_s3_key(ino, &dst_dir_key);
            }

            // Update cached child inodes to reflect the new prefix so the
            // kernel's existing inode references remain valid.
            self.inodes.rename_children(&src_dir_key, &dst_dir_key);

            self.dir_cache.invalidate(&src_prefix);
            self.dir_cache.invalidate(&dst_prefix);
            self.dir_cache.invalidate(&src_dir_key);
            self.touch_parent_times(parent);
            if new_parent != parent {
                self.touch_parent_times(new_parent);
            }
        } else {
            // Drain pending writeback on src AND dst before the NSS
            // rename so we operate on the post-flush layout and a queued
            // publish can't resurrect either name after the atomic
            // replace (create+close returns to userspace before the
            // close-time publish lands in NSS; rename/09.t / 10.t fire
            // the rename immediately after).
            // POSIX rename(2) atomically replaces an existing
            // regular-file dst. NSS does the swap via
            // `force_overwrite=true` and hands back the prior dst value
            // so we can GC the orphaned blob.
            let old_bytes = self
                .backend()
                .rename_file(&src_key, &dst_key, true, &trace_id)
                .await?;

            // Drop the replaced dst's name from the inode table. A
            // hardlink redirect keeps its inode (other names) live; a
            // single-named file is marked removed so a still-open dst fd
            // won't republish the now-overwritten name.
            let dst_was_indirect =
                rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&old_bytes)
                    .map(|l| matches!(l.state, ObjectState::Indirect(_)))
                    .unwrap_or(false);
            if dst_was_indirect {
                self.inodes.remove_alias(&dst_key, EntryType::File);
            } else if let Some(dst_ino) = dst_ino_before {
                self.inodes.remove_name_mapping(dst_ino);
            }

            // GC the value the rename displaced: a blob for a Normal/Mpu
            // file, or an nlink decrement for a hardlink redirect (so a
            // rename over a multiply-linked file leaves the survivors at
            // the right count, rename/23.t).
            self.cleanup_orphaned_value(&dst_key, dst_ino_before, old_bytes, &trace_id)
                .await;

            // Update inode s3_key if cached (read-only lookup, no refcount leak)
            if let Some(ino) = self.inodes.find_ino_by_key(&src_key, EntryType::File) {
                self.inodes.update_s3_key(ino, &dst_key);
            }

            // Update any open file handles to reflect the new key
            for mut fh_entry in self.file_handles.iter_mut() {
                if fh_entry.value().s3_key == src_key {
                    fh_entry.value_mut().s3_key = dst_key.clone();
                }
            }

            self.dir_cache.invalidate(&src_prefix);
            self.dir_cache.invalidate(&dst_prefix);
            self.touch_parent_times(parent);
            if new_parent != parent {
                self.touch_parent_times(new_parent);
            }
        }

        Ok(())
    }
}

/// Releases a `block_prefix` hold when a directory rename returns or
/// unwinds, so a blocked prefix never outlives the rename that set it
/// (e.g. an early `?` on a drain or `rename_folder` error, or future
/// cancellation at unmount).
struct PrefixBlockGuard {
    writeback: Arc<WritebackQueue>,
    prefix: String,
}

impl Drop for PrefixBlockGuard {
    fn drop(&mut self) {
        self.writeback.unblock_prefix(&self.prefix);
    }
}
