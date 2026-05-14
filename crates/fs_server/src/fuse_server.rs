use fractal_fuse::*;
use std::ffi::OsStr;
use std::sync::Arc;

use fractal_vfs::error::FsError;
use fractal_vfs::vfs::{TTL, VfsAttr, VfsCore};

pub struct FuseServer {
    vfs: Arc<VfsCore>,
}

impl FuseServer {
    pub fn new(vfs: Arc<VfsCore>) -> Self {
        Self { vfs }
    }
}

fn to_file_attr(va: &VfsAttr) -> FileAttr {
    FileAttr {
        ino: va.ino,
        size: va.size,
        blocks: va.blocks,
        atime: Timestamp::new(va.atime_secs, va.atime_ns_part),
        mtime: Timestamp::new(va.mtime_secs, va.mtime_ns_part),
        ctime: Timestamp::new(va.ctime_secs, va.ctime_ns_part),
        mode: va.mode,
        nlink: va.nlink,
        uid: va.uid,
        gid: va.gid,
        rdev: va.rdev,
        blksize: va.blksize,
    }
}

fn fs_err(e: FsError) -> Errno {
    e.into()
}

impl Filesystem for FuseServer {
    async fn init(&self, _req: Request) -> FsResult<ReplyInit> {
        self.vfs.vfs_init();
        Ok(ReplyInit {
            max_write: 1024 * 1024,
            ..Default::default()
        })
    }

    async fn destroy(&self) {
        self.vfs.vfs_destroy();
    }

    async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> FsResult<ReplyEntry> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        match self.vfs.vfs_lookup(parent, name_str).await {
            Ok(attr) => Ok(ReplyEntry {
                ttl: TTL,
                attr: to_file_attr(&attr),
                generation: 0,
            }),
            // Negative-dentry caching: tell the kernel "this name does
            // not exist" with `nodeid = 0` and a non-zero entry TTL so
            // the next LOOKUP for the same (parent, name) -- e.g.
            // tar's CREATE-precheck -- is served from the dentry cache
            // and never reaches us. The CREATE itself is the only
            // userspace round trip. Safe in 1W:NR: only the writer
            // mutates the namespace, and the writer invalidates the
            // local dentry cache on every CREATE/MKDIR success.
            Err(FsError::NotFound) => Ok(ReplyEntry {
                ttl: TTL,
                attr: to_file_attr(&VfsAttr::negative_dentry()),
                generation: 0,
            }),
            Err(e) => Err(fs_err(e)),
        }
    }

    fn forget(&self, _req: Request, inode: u64, nlookup: u64) {
        self.vfs.vfs_forget(inode, nlookup);
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: u64,
        fh: Option<u64>,
        _flags: u32,
    ) -> FsResult<ReplyAttr> {
        let attr = self.vfs.vfs_getattr(inode, fh).await.map_err(fs_err)?;
        // When the inode has any writeable open handle, drop the
        // FUSE attr cache TTL to zero so a stat-immediately-after-
        // write reflects the in-memory mode -- vfs_write clears
        // SUID/SGID on the in-memory posix and the kernel-level
        // FUSE_NOTIFY_INVAL_INODE we send catches the inode cache,
        // but the per-fh cache the kernel checks for `fstat(fd)` is
        // not invalidated by inval_inode. TTL=0 forces the kernel
        // to ask us back. Read-only handles keep TTL so pure-read
        // workloads (find / ls -l) don't incur the round-trip.
        let ttl = if self.vfs.inode_has_writeable_handle(inode) {
            std::time::Duration::ZERO
        } else {
            TTL
        };
        Ok(ReplyAttr {
            ttl,
            attr: to_file_attr(&attr),
        })
    }

    async fn setattr(
        &self,
        req: Request,
        inode: u64,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> FsResult<ReplyAttr> {
        // POSIX setattr permission rules. We can't pass the FUSE
        // `default_permissions` flag (it interferes with the
        // allow_other test path), so the kernel forwards every
        // setattr regardless of caller privilege and we enforce the
        // EPERM contract ourselves:
        //   - chmod / utime / chown gid: caller must be root or the
        //     file's owner.
        //   - chown to a different uid: caller must be root.
        // req.uid == 0 short-circuits the whole block (root bypass);
        // matches Linux's CAP_FOWNER / CAP_CHOWN semantics close
        // enough for the pjdfstest contract suite.
        if req.uid != 0 {
            let cur = self.vfs.vfs_getattr(inode, fh).await.map_err(fs_err)?;
            let is_owner = cur.uid == req.uid;
            // FATTR_KILL_SUIDGID is the kernel-driven clear of the
            // suid/sgid bits on a write by a non-owner. POSIX
            // requires it; we must NOT enforce the "non-owner cannot
            // chmod" EPERM contract on this specific change because
            // the kernel already validated the underlying op.
            if set_attr.mode.is_some() && !is_owner && !set_attr.kill_suidgid {
                return Err(libc::EPERM);
            }
            if let Some(new_uid) = set_attr.uid
                && new_uid != cur.uid
            {
                return Err(libc::EPERM);
            }
            if let Some(new_gid) = set_attr.gid
                && new_gid != cur.gid
                && !is_owner
            {
                return Err(libc::EPERM);
            }
            // utimensat-style atime/mtime updates by a non-owner are
            // governed by POSIX utimensat(2): the owner may set any
            // time, a non-owner with write permission on the file
            // may update times (POSIX restricts non-owner to
            // UTIME_NOW, but Linux's kernel resolves UTIME_NOW to a
            // concrete current_time before forwarding to FUSE so we
            // can't tell UTIME_NOW from a specific timestamp at this
            // layer -- the kernel's own MAY_WRITE check via
            // `inode_permission` already filtered out the
            // no-write-perm case before we got here, so a non-owner
            // reaching this point with write perm against the file
            // is the legitimate UTIME_NOW path).
            //
            // The fh.is_none() escape stays: the kernel's writeback
            // cache flushes buffered mtime/ctime updates as
            // setattr-with-fh after a write returns. The fh-bearing
            // setattr came from a fd that already passed the
            // open-time write permission check, so accept it --
            // otherwise sparse-overwrite-then-fsync on a file the
            // caller doesn't own (S3-uploaded, owner=root) returns
            // EPERM from FUSE_FSYNC.
            if (set_attr.atime.is_some() || set_attr.mtime.is_some()) && !is_owner && fh.is_none() {
                let mode = cur.mode;
                let has_write = if cur.uid == req.uid {
                    mode & libc::S_IWUSR != 0
                } else if cur.gid == req.gid {
                    mode & libc::S_IWGRP != 0
                } else {
                    mode & libc::S_IWOTH != 0
                };
                if !has_write {
                    return Err(libc::EPERM);
                }
            }
        }

        // Apply size first so the dirty-handle path in vfs_getattr
        // observes the updated wb.file_size when it picks the attrs to
        // reply with. truncate(2) is path-based and the kernel does not
        // pass an fh -- open the inode internally, run the size update
        // through the writeback path, and release so the publish lands
        // before we reply.
        if let Some(new_size) = set_attr.size {
            match fh {
                Some(fh_id) => {
                    self.vfs
                        .vfs_setattr_size(inode, fh_id, new_size)
                        .await
                        .map_err(fs_err)?;
                }
                None => {
                    let internal_fh = self
                        .vfs
                        .vfs_open(inode, libc::O_WRONLY as u32)
                        .await
                        .map_err(fs_err)?;
                    let size_res = self
                        .vfs
                        .vfs_setattr_size(inode, internal_fh, new_size)
                        .await;
                    let release_res = self.vfs.vfs_release(internal_fh).await;
                    size_res.map_err(fs_err)?;
                    release_res.map_err(fs_err)?;
                }
            }
        }

        // POSIX: a non-root chown that successfully changes uid or gid
        // clears S_ISUID and (for group-executable files) S_ISGID.
        // Linux signals this via FATTR_KILL_SUIDGID. The kernel does NOT
        // pre-compute the mode (FATTR_MODE may be absent), so when
        // kill_suidgid is set without mode, fetch the current mode and
        // inject the cleared value into the same vfs_setattr_posix
        // call. This keeps the chown + suid/sgid clear atomic from the
        // FUSE handler's point of view.
        // POSIX: a successful non-root chown(2) that changes uid or
        // gid clears S_ISUID and (for group-executable files)
        // S_ISGID. The Linux kernel signals this to FUSE via
        // FATTR_KILL_SUIDGID *only* when the daemon advertises
        // FUSE_HANDLE_KILLPRIV / V2; this server's FUSE INIT
        // reply turns on neither (the kernel doesn't expose them on
        // 7.44 io_uring + writeback_cache mode), so the kernel
        // never lifts the clear up to us. Detect the chown locally
        // -- non-root caller, uid or gid being set -- and inject the
        // cleared mode into the same `vfs_setattr_posix` call.
        // Fixes pjdfstest chown/00.t failures 599, 603, 614-615 etc.
        let mut effective_mode = set_attr.mode;
        let needs_suid_clear = effective_mode.is_none()
            && (set_attr.kill_suidgid
                || (req.uid != 0 && (set_attr.uid.is_some() || set_attr.gid.is_some())));
        if needs_suid_clear {
            let cur = self.vfs.vfs_getattr(inode, fh).await.map_err(fs_err)?;
            let mut m = cur.mode;
            if m & libc::S_ISUID != 0 {
                m &= !libc::S_ISUID;
            }
            if m & libc::S_ISGID != 0 && m & libc::S_IXGRP != 0 {
                m &= !libc::S_ISGID;
            }
            if m != cur.mode {
                effective_mode = Some(m);
            }
        }
        // Apply mode / uid / gid / times to the in-memory inode entry.
        // No NSS round-trip here -- the next flush carries the new
        // values forward.
        let needs_posix = effective_mode.is_some()
            || set_attr.uid.is_some()
            || set_attr.gid.is_some()
            || set_attr.atime.is_some()
            || set_attr.mtime.is_some()
            || set_attr.ctime.is_some();
        if needs_posix {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            let resolve_time = |t: Option<SetAttrTime>| -> Option<u64> {
                t.map(|st| match st {
                    SetAttrTime::Now => now_ns,
                    SetAttrTime::Specific(ts) => {
                        ts.sec.saturating_mul(1_000_000_000) + ts.nsec as u64
                    }
                })
            };
            let ctime_ns = set_attr
                .ctime
                .map(|ts| ts.sec.saturating_mul(1_000_000_000) + ts.nsec as u64);
            self.vfs
                .vfs_setattr_posix(
                    inode,
                    effective_mode,
                    set_attr.uid,
                    set_attr.gid,
                    resolve_time(set_attr.atime),
                    resolve_time(set_attr.mtime),
                    ctime_ns,
                )
                .await
                .map_err(fs_err)?;
        }

        // Always reply through vfs_getattr so the size field reflects
        // an in-flight wb buffer (the truncate target lives in
        // wb.file_size, not in the layout cached on the inode).
        let attr = self.vfs.vfs_getattr(inode, fh).await.map_err(fs_err)?;
        Ok(ReplyAttr {
            ttl: TTL,
            attr: to_file_attr(&attr),
        })
    }

    async fn open(&self, req: Request, inode: u64, flags: u32) -> FsResult<ReplyOpen> {
        let fh = self.vfs.vfs_open(inode, flags).await.map_err(fs_err)?;

        // POSIX: a successful write(2) by a non-owner clears
        // S_ISUID and (for group-execute files) S_ISGID. The kernel
        // normally invokes notify_change(ATTR_KILL_SUID|ATTR_KILL_SGID)
        // around the write so the FS handles it; with
        // FUSE_WRITEBACK_CACHE the cache absorbs the write and the
        // kill never reaches userspace. We anticipate the rule at
        // open(O_WRONLY|O_RDWR|O_APPEND|O_TRUNC) time: if the caller
        // is non-root, non-owner, and the file has any of those
        // bits, clear them now so a subsequent stat (kernel-cached or
        // not) reflects the post-write semantics that pjdfstest
        // chmod/12.t verifies. Strip S_ISGID only when the file is
        // group-executable, matching the kernel's
        // setattr_should_drop_sgid contract.
        let write_flags = libc::O_WRONLY as u32
            | libc::O_RDWR as u32
            | libc::O_APPEND as u32
            | libc::O_TRUNC as u32;
        if req.uid != 0 && (flags & write_flags) != 0 {
            let attr = self
                .vfs
                .vfs_getattr(inode, Some(fh))
                .await
                .map_err(fs_err)?;
            let has_setuid = attr.mode & libc::S_ISUID != 0;
            let has_setgid_exec = attr.mode & libc::S_ISGID != 0 && attr.mode & libc::S_IXGRP != 0;
            if attr.uid != req.uid && (has_setuid || has_setgid_exec) {
                let new_mode = attr.mode & !(libc::S_ISUID | libc::S_ISGID);
                if let Err(e) = self
                    .vfs
                    .vfs_setattr_posix(inode, Some(new_mode), None, None, None, None, None)
                    .await
                {
                    tracing::warn!(inode, error = %e, "open: kill_suidgid setattr failed");
                }
            }
        }

        // Try passthrough for fully-cached read-only files
        let (open_flags, backing_id) = if flags & (libc::O_WRONLY as u32 | libc::O_RDWR as u32) == 0
        {
            self.vfs.try_passthrough_for_fh(fh).unwrap_or((0, 0))
        } else {
            (0, 0)
        };

        Ok(ReplyOpen {
            fh,
            flags: open_flags,
            backing_id,
        })
    }

    async fn read(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        offset: u64,
        buf: &mut [u8],
    ) -> FsResult<usize> {
        self.vfs.vfs_read(fh, offset, buf).await.map_err(fs_err)
    }

    async fn write(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        flags: u32,
    ) -> FsResult<usize> {
        let written = self.vfs.vfs_write(fh, offset, data).await.map_err(fs_err)?;

        // O_SYNC / O_DSYNC: every write is durability-tied, drain
        // the queue before the FUSE reply so the kernel sees the
        // same synchronous guarantee userspace asked for.
        if (flags & (libc::O_SYNC as u32 | libc::O_DSYNC as u32)) != 0 {
            self.vfs.vfs_flush(fh).await.map_err(fs_err)?;
        }

        Ok(written as usize)
    }

    async fn flush(&self, _req: Request, _inode: u64, _fh: u64, _lock_owner: u64) -> FsResult<()> {
        // FUSE_FLUSH fires on every close(2). It is *not* a
        // durability request -- POSIX only requires errors-on-close
        // to propagate, and the spawned vfs_release path is what
        // actually carries the put_inode_via_queue wait. Doing the
        // flush_write_buffer here forces every close to await one
        // worker tick for the queue commit, which would turn the
        // writeback win into a 10x regression on tar / cp
        // create-heavy workloads. Skip the work entirely and let
        // vfs_release do it off the FUSE worker thread.
        Ok(())
    }

    async fn fsync(&self, _req: Request, _inode: u64, fh: u64, _datasync: bool) -> FsResult<()> {
        self.vfs.vfs_flush(fh).await.map_err(fs_err)
    }

    async fn release(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        _flock_release: bool,
    ) -> FsResult<()> {
        self.vfs.release_passthrough(fh);

        // Spawn the synchronous release/flush work in the background
        // and return to the kernel immediately. A pending-flush
        // cycle is registered in the queue so vfs_fsync and the
        // FUSE_FSYNC drain path can wait for it.
        //
        // We deliberately keep the inode write lock held until the
        // spawn completes -- a subsequent vfs_open of the same inode
        // sees EBUSY, which is the correct "still being flushed"
        // semantic. Crash mid-flush after close means the data is
        // lost (POSIX legal without fsync); the loss bound is the
        // worker queue depth + the flush RPC chain.
        if let Some((ino, has_dirty, file_size)) = self.vfs.peek_release_state(fh)
            && has_dirty
        {
            let queue = self.vfs.writeback_queue().clone();
            let generation = self.vfs.allocate_flush_generation(ino);
            queue.open_cycle(ino, generation, file_size, 0);
            let _ = queue.advance_stage(
                ino,
                generation,
                crate::writeback::FileCommitStage::PublishLayoutQueued,
            );
            let vfs = self.vfs.clone();
            compio_runtime::spawn(async move {
                match vfs.vfs_release(fh).await {
                    Ok(()) => {
                        queue.advance_to_done(ino, generation);
                    }
                    Err(e) => {
                        tracing::warn!(
                            fh,
                            ino,
                            generation = generation.0,
                            error = %e,
                            "writeback release flush failed; tainting inode"
                        );
                        queue.record_failure(ino, generation);
                        queue.advance_to_done(ino, generation);
                    }
                }
            })
            .detach();
            return Ok(());
        }

        self.vfs.vfs_release(fh).await.map_err(fs_err)
    }

    async fn create(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _flags: u32,
    ) -> FsResult<ReplyCreate> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        // Seed the new inode's posix from the kernel's create() args:
        // the requesting uid/gid become owner/group; the mode the
        // kernel picked (after umask) becomes the inode mode. ctime /
        // mtime get `now` so stat-after-create reflects the create
        // time rather than the synthesised default. atime is
        // synthesised from mtime at stat time -- see PosixAttrs.
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let init_posix = data_types::object_layout::PosixAttrs {
            mode,
            uid: req.uid,
            gid: req.gid,
            mtime_ns: now_ns,
            ctime_ns: now_ns,
        };
        let (attr, fh) = self
            .vfs
            .vfs_create(parent, name_str, Some(init_posix))
            .await
            .map_err(fs_err)?;
        Ok(ReplyCreate {
            ttl: TTL,
            attr: to_file_attr(&attr),
            generation: 0,
            fh,
            flags: 0,
        })
    }

    async fn unlink(&self, req: Request, parent: u64, name: &OsStr) -> FsResult<()> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        self.vfs
            .vfs_unlink(parent, name_str, req.uid)
            .await
            .map_err(fs_err)
    }

    async fn mknod(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> FsResult<ReplyEntry> {
        use data_types::object_layout::{PosixAttrs, SpecialKind};
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        // Pick the kind from the S_IFMT bits in `mode`. The kernel
        // sends FIFO (S_IFIFO), block (S_IFBLK), char (S_IFCHR),
        // socket (S_IFSOCK), or regular (S_IFREG, when called from
        // mknod(2) with S_IFREG); only the four "special" cases
        // round-trip through this path.
        let kind = match mode & libc::S_IFMT {
            x if x == libc::S_IFIFO => SpecialKind::Fifo,
            x if x == libc::S_IFBLK => SpecialKind::BlockDevice,
            x if x == libc::S_IFCHR => SpecialKind::CharDevice,
            x if x == libc::S_IFSOCK => SpecialKind::Socket,
            // mknod(S_IFREG) is legal POSIX but unusual; we don't
            // map it onto vfs_create today (that path also does the
            // open-for-write dance) so reject for now.
            _ => return Err(libc::EINVAL),
        };
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let init_posix = PosixAttrs {
            mode,
            uid: req.uid,
            gid: req.gid,
            mtime_ns: now_ns,
            ctime_ns: now_ns,
        };
        let attr = self
            .vfs
            .vfs_mknod(parent, name_str, kind, rdev, init_posix)
            .await
            .map_err(fs_err)?;
        Ok(ReplyEntry {
            ttl: TTL,
            attr: to_file_attr(&attr),
            generation: 0,
        })
    }

    async fn symlink(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        link: &OsStr,
    ) -> FsResult<ReplyEntry> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        // The symlink target is uninterpreted bytes -- pass it through
        // verbatim so non-UTF-8 targets round-trip correctly.
        let target_bytes = link.as_encoded_bytes();
        let attr = self
            .vfs
            .vfs_symlink(parent, name_str, target_bytes)
            .await
            .map_err(fs_err)?;
        Ok(ReplyEntry {
            ttl: TTL,
            attr: to_file_attr(&attr),
            generation: 0,
        })
    }

    async fn readlink(&self, _req: Request, inode: u64) -> FsResult<ReplyReadlink> {
        let data = self.vfs.vfs_readlink(inode).await.map_err(fs_err)?;
        Ok(ReplyReadlink { data })
    }

    async fn link(
        &self,
        _req: Request,
        inode: u64,
        new_parent: u64,
        new_name: &OsStr,
    ) -> FsResult<ReplyEntry> {
        let name_str = new_name.to_str().ok_or(libc::EINVAL)?;
        let attr = self
            .vfs
            .vfs_link(inode, new_parent, name_str)
            .await
            .map_err(fs_err)?;
        Ok(ReplyEntry {
            ttl: TTL,
            attr: to_file_attr(&attr),
            generation: 0,
        })
    }

    async fn mkdir(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> FsResult<ReplyEntry> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        // Seed the new dir's posix from the kernel's mkdir() args.
        // The kernel already applied umask; mode arrives without
        // file-type bits.
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let init_posix = data_types::object_layout::PosixAttrs {
            mode,
            uid: req.uid,
            gid: req.gid,
            mtime_ns: now_ns,
            ctime_ns: now_ns,
        };
        let attr = self
            .vfs
            .vfs_mkdir(parent, name_str, Some(init_posix))
            .await
            .map_err(fs_err)?;
        Ok(ReplyEntry {
            ttl: TTL,
            attr: to_file_attr(&attr),
            generation: 0,
        })
    }

    async fn rmdir(&self, req: Request, parent: u64, name: &OsStr) -> FsResult<()> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        self.vfs
            .vfs_rmdir(parent, name_str, req.uid)
            .await
            .map_err(fs_err)
    }

    async fn rename(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        _flags: u32,
    ) -> FsResult<()> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        let new_name_str = new_name.to_str().ok_or(libc::EINVAL)?;
        self.vfs
            .vfs_rename(parent, name_str, new_parent, new_name_str, req.uid, req.gid)
            .await
            .map_err(fs_err)
    }

    async fn opendir(&self, _req: Request, inode: u64, _flags: u32) -> FsResult<ReplyOpen> {
        let fh = self.vfs.vfs_opendir(inode).map_err(fs_err)?;
        Ok(ReplyOpen {
            fh,
            flags: 0,
            backing_id: 0,
        })
    }

    async fn readdir(
        &self,
        _req: Request,
        parent: u64,
        _fh: u64,
        offset: u64,
        _size: u32,
    ) -> FsResult<Vec<DirectoryEntry>> {
        let entries = self.vfs.vfs_readdir(parent, offset).await.map_err(fs_err)?;
        Ok(entries
            .into_iter()
            .map(|e| DirectoryEntry {
                ino: e.ino,
                kind: if e.is_dir {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                },
                name: e.name.into_bytes(),
                offset: e.offset,
            })
            .collect())
    }

    async fn readdirplus(
        &self,
        _req: Request,
        parent: u64,
        _fh: u64,
        offset: u64,
        _size: u32,
    ) -> FsResult<Vec<DirectoryEntryPlus>> {
        let entries = self
            .vfs
            .vfs_readdirplus(parent, offset)
            .await
            .map_err(fs_err)?;
        Ok(entries
            .into_iter()
            .map(|e| DirectoryEntryPlus {
                ino: e.ino,
                generation: 0,
                kind: if e.is_dir {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                },
                name: e.name.into_bytes(),
                offset: e.offset,
                attr: to_file_attr(&e.attr),
                entry_ttl: TTL,
            })
            .collect())
    }

    async fn releasedir(&self, _req: Request, _inode: u64, _fh: u64, _flags: u32) -> FsResult<()> {
        Ok(())
    }

    async fn fsyncdir(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _datasync: bool,
    ) -> FsResult<()> {
        // Drain every dirty writeback cycle the queue currently
        // knows about. Cheap mount-wide barrier; a true
        // subtree-scoped variant is a future optimization.
        self.vfs.drain_all_dirty_cycles().await.map_err(fs_err)?;
        Ok(())
    }

    async fn fallocate(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        offset: u64,
        length: u64,
        mode: u32,
    ) -> FsResult<()> {
        self.vfs
            .vfs_fallocate(fh, offset, length, mode)
            .await
            .map_err(fs_err)
    }

    async fn lseek(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        offset: u64,
        whence: u32,
    ) -> FsResult<u64> {
        self.vfs.vfs_lseek(fh, offset, whence).await.map_err(fs_err)
    }

    async fn statfs(&self, _req: Request, _inode: u64) -> FsResult<ReplyStatfs> {
        let s = self.vfs.vfs_statfs();
        Ok(ReplyStatfs {
            blocks: s.blocks,
            bfree: s.bfree,
            bavail: s.bavail,
            files: s.files,
            ffree: s.ffree,
            bsize: s.bsize,
            namelen: s.namelen,
            frsize: s.frsize,
        })
    }
}
