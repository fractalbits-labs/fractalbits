use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use fractal_nfs::Nfs3Filesystem;
use fractal_nfs::nfs3_types::*;
use fractal_nfs::nfs3_wire;
use fractal_nfs::xdr::XdrWriter;

use crate::error::FsError;
use crate::vfs::{VfsAttr, VfsCore};

const EVICTION_TTL: Duration = Duration::from_secs(300);
const EVICTION_INTERVAL_OPS: u64 = 1000;
const READDIR_MAX_ENTRIES: usize = 128;

pub struct NfsAdapter {
    vfs: VfsCore,
    fsid: u64,
    op_counter: AtomicU64,
}

impl NfsAdapter {
    pub fn new(vfs: VfsCore, fsid: u64) -> Self {
        Self {
            vfs,
            fsid,
            op_counter: AtomicU64::new(0),
        }
    }

    fn maybe_evict(&self) {
        let count = self.op_counter.fetch_add(1, Ordering::Relaxed);
        if count > 0 && count.is_multiple_of(EVICTION_INTERVAL_OPS) {
            self.vfs.vfs_evict_stale_inodes(EVICTION_TTL);
        }
    }
}

fn vfs_attr_to_fattr3(attr: &VfsAttr, fsid: u64) -> Fattr3 {
    Fattr3 {
        ftype: Ftype3::from_mode(attr.mode),
        mode: attr.mode & 0o7777,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        size: attr.size,
        used: attr.blocks * 512,
        rdev: Specdata3 {
            specdata1: 0,
            specdata2: 0,
        },
        fsid,
        fileid: attr.ino,
        atime: Nfstime3 {
            seconds: attr.atime_secs as u32,
            nseconds: 0,
        },
        mtime: Nfstime3 {
            seconds: attr.mtime_secs as u32,
            nseconds: 0,
        },
        ctime: Nfstime3 {
            seconds: attr.ctime_secs as u32,
            nseconds: 0,
        },
    }
}

fn fs_err_to_nfs(e: FsError) -> Nfsstat3 {
    match e {
        FsError::NotFound => Nfsstat3::Noent,
        FsError::AlreadyExists => Nfsstat3::Exist,
        FsError::NotEmpty => Nfsstat3::Notempty,
        FsError::IsDir => Nfsstat3::Isdir,
        FsError::NotDir => Nfsstat3::Notdir,
        FsError::ReadOnly => Nfsstat3::Rofs,
        FsError::BadFd => Nfsstat3::Badhandle,
        FsError::Rpc(_) => Nfsstat3::ServerFault,
        FsError::DataVg(_) => Nfsstat3::Io,
        FsError::InvalidState => Nfsstat3::Io,
        FsError::Deserialize(_) => Nfsstat3::Io,
        FsError::Internal(_) => Nfsstat3::ServerFault,
    }
}

const WRITE_VERF: [u8; 8] = [0; 8];

impl Nfs3Filesystem for NfsAdapter {
    async fn getattr(&self, fh: &NfsFh3, w: &mut XdrWriter) {
        self.maybe_evict();
        match self.vfs.vfs_getattr(fh.ino(), None).await {
            Ok(attr) => {
                let fattr = vfs_attr_to_fattr3(&attr, self.fsid);
                nfs3_wire::encode_getattr_ok(w, &fattr);
            }
            Err(e) => {
                nfs3_wire::encode_getattr_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn setattr(&self, fh: &NfsFh3, attrs: &Sattr3, w: &mut XdrWriter) {
        if let Some(0) = attrs.size {
            // Truncate to zero
            let open_fh = match self
                .vfs
                .vfs_open(fh.ino(), libc::O_WRONLY as u32 | libc::O_TRUNC as u32)
                .await
            {
                Ok(fh) => fh,
                Err(e) => {
                    nfs3_wire::encode_setattr_err(w, fs_err_to_nfs(e));
                    return;
                }
            };
            let result = self.vfs.vfs_setattr_truncate(fh.ino(), open_fh).await;
            let _ = self.vfs.vfs_release(open_fh).await;
            match result {
                Ok(attr) => {
                    let fattr = vfs_attr_to_fattr3(&attr, self.fsid);
                    nfs3_wire::encode_setattr_ok(w, &fattr);
                }
                Err(e) => {
                    nfs3_wire::encode_setattr_err(w, fs_err_to_nfs(e));
                }
            }
        } else {
            // Other setattr: just return current attrs
            match self.vfs.vfs_getattr(fh.ino(), None).await {
                Ok(attr) => {
                    let fattr = vfs_attr_to_fattr3(&attr, self.fsid);
                    nfs3_wire::encode_setattr_ok(w, &fattr);
                }
                Err(e) => {
                    nfs3_wire::encode_setattr_err(w, fs_err_to_nfs(e));
                }
            }
        }
    }

    async fn lookup(&self, dir_fh: &NfsFh3, name: &str, w: &mut XdrWriter) {
        match self.vfs.vfs_lookup(dir_fh.ino(), name).await {
            Ok(attr) => {
                let child_fh = NfsFh3::new(attr.ino, self.fsid);
                let fattr = vfs_attr_to_fattr3(&attr, self.fsid);
                nfs3_wire::encode_lookup_ok(w, &child_fh, &fattr, None);
            }
            Err(e) => {
                nfs3_wire::encode_lookup_err(w, fs_err_to_nfs(e), None);
            }
        }
    }

    async fn access(&self, fh: &NfsFh3, access: u32, _uid: u32, _gid: u32, w: &mut XdrWriter) {
        match self.vfs.vfs_getattr(fh.ino(), None).await {
            Ok(attr) => {
                let fattr = vfs_attr_to_fattr3(&attr, self.fsid);
                nfs3_wire::encode_access_ok(w, &fattr, access);
            }
            Err(e) => {
                nfs3_wire::encode_access_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn read(&self, fh: &NfsFh3, offset: u64, count: u32, w: &mut XdrWriter) {
        match self.vfs.vfs_read_by_ino(fh.ino(), offset, count).await {
            Ok(data) => {
                let eof = (data.len() as u32) < count;
                match self.vfs.vfs_getattr(fh.ino(), None).await {
                    Ok(attr) => {
                        let fattr = vfs_attr_to_fattr3(&attr, self.fsid);
                        nfs3_wire::encode_read_ok(w, &fattr, &data, eof);
                    }
                    Err(_) => {
                        // Return data even without post-op attrs
                        Nfsstat3::Ok.encode(w);
                        encode_post_op_attr(w, None);
                        w.write_u32(data.len() as u32);
                        w.write_bool(eof);
                        w.write_opaque(&data);
                    }
                }
            }
            Err(e) => {
                nfs3_wire::encode_read_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn write(
        &self,
        fh: &NfsFh3,
        offset: u64,
        data: &[u8],
        _stable: StableHow,
        w: &mut XdrWriter,
    ) {
        match self.vfs.vfs_write_by_ino(fh.ino(), offset, data).await {
            Ok(written) => {
                match self.vfs.vfs_getattr(fh.ino(), None).await {
                    Ok(attr) => {
                        let fattr = vfs_attr_to_fattr3(&attr, self.fsid);
                        nfs3_wire::encode_write_ok(
                            w,
                            &fattr,
                            written,
                            StableHow::FileSync,
                            &WRITE_VERF,
                        );
                    }
                    Err(_) => {
                        // Return success even without post-op attrs
                        Nfsstat3::Ok.encode(w);
                        encode_wcc_data(w, None, None);
                        w.write_u32(written);
                        w.write_u32(StableHow::FileSync as u32);
                        w.write_opaque_fixed(&WRITE_VERF);
                    }
                }
            }
            Err(e) => {
                nfs3_wire::encode_write_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn create(&self, dir_fh: &NfsFh3, name: &str, w: &mut XdrWriter) {
        match self.vfs.vfs_create(dir_fh.ino(), name).await {
            Ok((attr, fuse_fh)) => {
                // Release the FUSE file handle immediately (NFS is stateless)
                let _ = self.vfs.vfs_release(fuse_fh).await;
                let child_fh = NfsFh3::new(attr.ino, self.fsid);
                let fattr = vfs_attr_to_fattr3(&attr, self.fsid);
                nfs3_wire::encode_create_ok(w, &child_fh, &fattr, None);
            }
            Err(e) => {
                nfs3_wire::encode_create_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn mkdir(&self, dir_fh: &NfsFh3, name: &str, w: &mut XdrWriter) {
        match self.vfs.vfs_mkdir(dir_fh.ino(), name).await {
            Ok(attr) => {
                let child_fh = NfsFh3::new(attr.ino, self.fsid);
                let fattr = vfs_attr_to_fattr3(&attr, self.fsid);
                nfs3_wire::encode_mkdir_ok(w, &child_fh, &fattr, None);
            }
            Err(e) => {
                nfs3_wire::encode_mkdir_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn remove(&self, dir_fh: &NfsFh3, name: &str, w: &mut XdrWriter) {
        match self.vfs.vfs_unlink(dir_fh.ino(), name).await {
            Ok(()) => {
                nfs3_wire::encode_remove_ok(w, None);
            }
            Err(e) => {
                nfs3_wire::encode_remove_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn rmdir(&self, dir_fh: &NfsFh3, name: &str, w: &mut XdrWriter) {
        match self.vfs.vfs_rmdir(dir_fh.ino(), name).await {
            Ok(()) => {
                nfs3_wire::encode_remove_ok(w, None);
            }
            Err(e) => {
                nfs3_wire::encode_remove_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn rename(
        &self,
        from_dir: &NfsFh3,
        from_name: &str,
        to_dir: &NfsFh3,
        to_name: &str,
        w: &mut XdrWriter,
    ) {
        match self
            .vfs
            .vfs_rename(from_dir.ino(), from_name, to_dir.ino(), to_name)
            .await
        {
            Ok(()) => {
                nfs3_wire::encode_rename_ok(w, None, None);
            }
            Err(e) => {
                nfs3_wire::encode_rename_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn readdir(&self, dir_fh: &NfsFh3, cookie: u64, _count: u32, w: &mut XdrWriter) {
        match self.vfs.vfs_readdir(dir_fh.ino(), cookie).await {
            Ok(entries) => {
                let eof = entries.len() <= READDIR_MAX_ENTRIES;
                let nfs_entries: Vec<Entry3> = entries
                    .iter()
                    .take(READDIR_MAX_ENTRIES)
                    .map(|e| Entry3 {
                        fileid: e.ino,
                        name: e.name.clone(),
                        cookie: e.offset,
                    })
                    .collect();
                let cookieverf = [0u8; 8];
                nfs3_wire::encode_readdir_ok(w, None, &cookieverf, &nfs_entries, eof);
            }
            Err(e) => {
                nfs3_wire::encode_readdir_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn readdirplus(&self, dir_fh: &NfsFh3, cookie: u64, _maxcount: u32, w: &mut XdrWriter) {
        match self.vfs.vfs_readdirplus(dir_fh.ino(), cookie).await {
            Ok(entries) => {
                let eof = entries.len() <= READDIR_MAX_ENTRIES;
                let nfs_entries: Vec<Entryplus3> = entries
                    .iter()
                    .take(READDIR_MAX_ENTRIES)
                    .map(|e| {
                        let fh = NfsFh3::new(e.ino, self.fsid);
                        Entryplus3 {
                            fileid: e.ino,
                            name: e.name.clone(),
                            cookie: e.offset,
                            attr: Some(vfs_attr_to_fattr3(&e.attr, self.fsid)),
                            fh: Some(fh),
                        }
                    })
                    .collect();
                let cookieverf = [0u8; 8];
                nfs3_wire::encode_readdirplus_ok(w, None, &cookieverf, &nfs_entries, eof);
            }
            Err(e) => {
                nfs3_wire::encode_readdirplus_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn fsstat(&self, fh: &NfsFh3, w: &mut XdrWriter) {
        let stat = self.vfs.vfs_statfs();
        match self.vfs.vfs_getattr(fh.ino(), None).await {
            Ok(attr) => {
                let fattr = vfs_attr_to_fattr3(&attr, self.fsid);
                nfs3_wire::encode_fsstat_ok(
                    w,
                    &fattr,
                    stat.blocks * stat.bsize as u64,
                    stat.bfree * stat.bsize as u64,
                    stat.bavail * stat.bsize as u64,
                    stat.files,
                    stat.ffree,
                    stat.ffree,
                    0,
                );
            }
            Err(e) => {
                nfs3_wire::encode_fsstat_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn fsinfo(&self, fh: &NfsFh3, w: &mut XdrWriter) {
        let max_rw: u32 = 1024 * 1024;
        match self.vfs.vfs_getattr(fh.ino(), None).await {
            Ok(attr) => {
                let fattr = vfs_attr_to_fattr3(&attr, self.fsid);
                nfs3_wire::encode_fsinfo_ok(
                    w,
                    &fattr,
                    max_rw,
                    max_rw,
                    4096,
                    max_rw,
                    max_rw,
                    4096,
                    4096,
                    u64::MAX,
                    FSF3_HOMOGENEOUS | FSF3_CANSETTIME,
                );
            }
            Err(e) => {
                nfs3_wire::encode_fsinfo_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn pathconf(&self, fh: &NfsFh3, w: &mut XdrWriter) {
        match self.vfs.vfs_getattr(fh.ino(), None).await {
            Ok(attr) => {
                let fattr = vfs_attr_to_fattr3(&attr, self.fsid);
                nfs3_wire::encode_pathconf_ok(w, &fattr, 32767, 255);
            }
            Err(e) => {
                nfs3_wire::encode_pathconf_err(w, fs_err_to_nfs(e));
            }
        }
    }

    async fn commit(&self, fh: &NfsFh3, _offset: u64, _count: u32, w: &mut XdrWriter) {
        // All writes are FILE_SYNC (committed before reply), so COMMIT is a no-op.
        match self.vfs.vfs_getattr(fh.ino(), None).await {
            Ok(attr) => {
                let fattr = vfs_attr_to_fattr3(&attr, self.fsid);
                nfs3_wire::encode_commit_ok(w, &fattr, &WRITE_VERF);
            }
            Err(e) => {
                nfs3_wire::encode_commit_err(w, fs_err_to_nfs(e));
            }
        }
    }
}
