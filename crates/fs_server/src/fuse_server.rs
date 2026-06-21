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
        let attr = self
            .vfs
            .vfs_lookup(parent, name_str)
            .await
            .map_err(fs_err)?;
        Ok(ReplyEntry {
            ttl: TTL,
            attr: to_file_attr(&attr),
            generation: 0,
        })
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
        Ok(ReplyAttr {
            ttl: TTL,
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
        // POSIX setattr permission rules. We don't pass the FUSE
        // `default_permissions` flag, so the kernel forwards every
        // setattr regardless of caller privilege and we enforce the
        // EPERM contract ourselves. root (uid 0) bypasses the whole
        // block.
        if req.uid != 0 {
            let cur = self.vfs.vfs_getattr(inode, fh).await.map_err(fs_err)?;
            let is_owner = cur.uid == req.uid;
            // chmod by a non-owner is EPERM. In writeback-cache mode the
            // kernel never forwards a suid-clear-on-write as a setattr
            // (the cache absorbs the write), so there is no kernel-driven
            // mode change to exempt here -- the open() handler does the
            // anticipatory clear instead.
            if set_attr.mode.is_some() && !is_owner {
                return Err(libc::EPERM);
            }
            // Only root may change the owning uid.
            if let Some(new_uid) = set_attr.uid
                && new_uid != cur.uid
            {
                return Err(libc::EPERM);
            }
            // chgrp to a different gid requires ownership.
            if let Some(new_gid) = set_attr.gid
                && new_gid != cur.gid
                && !is_owner
            {
                return Err(libc::EPERM);
            }
            // A non-owner setting a specific atime/mtime needs write
            // permission on the file (the legitimate UTIME_NOW path the
            // kernel already filtered for write access). The fh-bearing
            // case came from a fd that passed the open-time check, so
            // accept it.
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

        // Apply size first so the dirty-handle path in vfs_getattr sees
        // the updated buffer size. truncate(2) is path-based with no fh,
        // so open the inode internally and release after the update.
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

        // POSIX: a successful non-root chown that changes uid or gid
        // clears S_ISUID and (for group-executable files) S_ISGID. The
        // kernel does not lift this up to us here, so detect the chown
        // locally and inject the cleared mode into the same posix call.
        let mut effective_mode = set_attr.mode;
        let needs_suid_clear = effective_mode.is_none()
            && req.uid != 0
            && (set_attr.uid.is_some() || set_attr.gid.is_some());
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

        // Reply through vfs_getattr so the size field reflects an
        // in-flight write buffer.
        let attr = self.vfs.vfs_getattr(inode, fh).await.map_err(fs_err)?;
        Ok(ReplyAttr {
            ttl: TTL,
            attr: to_file_attr(&attr),
        })
    }

    async fn open(&self, req: Request, inode: u64, flags: u32) -> FsResult<ReplyOpen> {
        let fh = self.vfs.vfs_open(inode, flags).await.map_err(fs_err)?;

        // POSIX: a write(2) by a non-owner clears S_ISUID and (for
        // group-executable files) S_ISGID. With FUSE_WRITEBACK_CACHE the
        // cache absorbs the write and the kill never reaches userspace,
        // so anticipate it at open(O_WRONLY|O_RDWR|O_APPEND|O_TRUNC)
        // time: clear the bits now if the caller is non-root, non-owner,
        // and the file carries them.
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
        _flags: u32,
    ) -> FsResult<usize> {
        let written = self.vfs.vfs_write(fh, offset, data).await.map_err(fs_err)?;
        Ok(written as usize)
    }

    async fn flush(&self, _req: Request, _inode: u64, fh: u64, _lock_owner: u64) -> FsResult<()> {
        // FUSE_FLUSH fires on every close(2). It is not a durability
        // request -- POSIX only requires errors-on-close to propagate --
        // so use the no-drain variant to keep writeback cycles async and
        // avoid serialising create-heavy workloads against the worker.
        self.vfs.vfs_flush_no_drain(fh).await.map_err(fs_err)
    }

    async fn fsync(&self, _req: Request, _inode: u64, fh: u64, _datasync: bool) -> FsResult<()> {
        // fsync(2) is a durability request: drain the writeback barrier.
        self.vfs.vfs_flush(fh).await.map_err(fs_err)
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
        // Seed the new inode from the kernel's create() args: the
        // requesting uid/gid become owner/group and the (umask-applied)
        // mode becomes the inode mode.
        let (attr, fh) = self
            .vfs
            .vfs_create(parent, name_str, mode, req.uid, req.gid)
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

    async fn unlink(&self, _req: Request, parent: u64, name: &OsStr) -> FsResult<()> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        self.vfs.vfs_unlink(parent, name_str).await.map_err(fs_err)
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
        // Pick the kind from the S_IFMT bits. Only fifo / block / char /
        // socket round-trip here; mknod(S_IFREG) is legal POSIX but we
        // don't map it onto the create-for-write path today.
        let kind = match mode & libc::S_IFMT {
            x if x == libc::S_IFIFO => SpecialKind::Fifo,
            x if x == libc::S_IFBLK => SpecialKind::BlockDevice,
            x if x == libc::S_IFCHR => SpecialKind::CharDevice,
            x if x == libc::S_IFSOCK => SpecialKind::Socket,
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
        req: Request,
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
            .vfs_symlink(parent, name_str, target_bytes, req.uid, req.gid)
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
        // The kernel already applied umask; mode arrives without
        // file-type bits.
        let attr = self
            .vfs
            .vfs_mkdir(parent, name_str, mode, req.uid, req.gid)
            .await
            .map_err(fs_err)?;
        Ok(ReplyEntry {
            ttl: TTL,
            attr: to_file_attr(&attr),
            generation: 0,
        })
    }

    async fn rmdir(&self, _req: Request, parent: u64, name: &OsStr) -> FsResult<()> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        self.vfs.vfs_rmdir(parent, name_str).await.map_err(fs_err)
    }

    async fn rename(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
        _flags: u32,
    ) -> FsResult<()> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        let new_name_str = new_name.to_str().ok_or(libc::EINVAL)?;
        self.vfs
            .vfs_rename(parent, name_str, new_parent, new_name_str)
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
        Ok(())
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
