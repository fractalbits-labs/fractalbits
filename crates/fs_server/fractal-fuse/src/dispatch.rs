use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

use tracing::{debug, warn};

use crate::abi::*;
use crate::filesystem::Filesystem;
use crate::ring::RingEntry;
use crate::types::*;

/// Dispatch a FUSE request to the filesystem and return the response.
/// Returns the serialized response length written to the entry's header/payload,
/// or None if no response should be sent (e.g., FORGET).
pub async fn dispatch<F: Filesystem>(fs: &F, entry: &mut RingEntry) -> Option<()> {
    let header = entry.header();
    let in_hdr = header.in_header();
    let opcode = in_hdr.opcode;
    let nodeid = in_hdr.nodeid;
    let unique = in_hdr.unique;

    let req = Request {
        unique,
        uid: in_hdr.uid,
        gid: in_hdr.gid,
        pid: in_hdr.pid,
    };

    match opcode {
        FUSE_FORGET => {
            let arg: &fuse_forget_in = header.op_in_as();
            fs.forget(req, nodeid, arg.nlookup);
            return None;
        }
        FUSE_BATCH_FORGET => {
            let arg: &fuse_batch_forget_in = header.op_in_as();
            let count = arg.count as usize;
            let payload = entry.payload();
            let entry_size = std::mem::size_of::<fuse_forget_one>();
            let mut inodes = Vec::with_capacity(count);
            for i in 0..count {
                let offset = i * entry_size;
                if offset + entry_size > payload.len() {
                    break;
                }
                let one = unsafe { &*(payload.as_ptr().add(offset) as *const fuse_forget_one) };
                inodes.push((one.nodeid, one.nlookup));
            }
            fs.batch_forget(req, &inodes);
            return None;
        }
        _ => {}
    }

    let result = dispatch_with_reply(fs, entry, req, opcode, nodeid).await;
    debug!(
        "dispatch: opcode={} nodeid={} unique={}",
        opcode, nodeid, unique
    );
    serialize_response(entry, unique, opcode, result);
    Some(())
}

async fn dispatch_with_reply<F: Filesystem>(
    fs: &F,
    entry: &mut RingEntry,
    req: Request,
    opcode: u32,
    nodeid: u64,
) -> DispatchResult {
    match opcode {
        FUSE_LOOKUP => {
            let name = extract_name_from_payload(entry);
            match fs.lookup(req, nodeid, OsStr::from_bytes(&name)).await {
                Ok(reply) => DispatchResult::Entry(reply),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_GETATTR => {
            let arg: &fuse_getattr_in = entry.header().op_in_as();
            let fh = if arg.getattr_flags & FUSE_GETATTR_FH != 0 {
                Some(arg.fh)
            } else {
                None
            };
            match fs.getattr(req, nodeid, fh, arg.getattr_flags).await {
                Ok(reply) => DispatchResult::Attr(reply),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_SETATTR => {
            let arg: &fuse_setattr_in = entry.header().op_in_as();
            let fh = if arg.valid & FATTR_FH != 0 {
                Some(arg.fh)
            } else {
                None
            };
            let set_attr = SetAttr::from_raw(arg);
            match fs.setattr(req, nodeid, fh, set_attr).await {
                Ok(reply) => DispatchResult::Attr(reply),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_READLINK => match fs.readlink(req, nodeid).await {
            Ok(reply) => DispatchResult::Readlink(reply),
            Err(e) => DispatchResult::Error(e),
        },
        FUSE_SYMLINK => {
            let payload = entry.payload();
            let (name, link) = parse_two_names(payload);
            match fs
                .symlink(
                    req,
                    nodeid,
                    OsStr::from_bytes(name),
                    OsStr::from_bytes(link),
                )
                .await
            {
                Ok(reply) => DispatchResult::Entry(reply),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_MKNOD => {
            let arg: &fuse_mknod_in = entry.header().op_in_as();
            let name = extract_name_from_payload(entry);
            match fs
                .mknod(req, nodeid, OsStr::from_bytes(&name), arg.mode, arg.rdev)
                .await
            {
                Ok(reply) => DispatchResult::Entry(reply),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_MKDIR => {
            let arg: &fuse_mkdir_in = entry.header().op_in_as();
            let name = extract_name_from_payload(entry);
            match fs
                .mkdir(req, nodeid, OsStr::from_bytes(&name), arg.mode, arg.umask)
                .await
            {
                Ok(reply) => DispatchResult::Entry(reply),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_UNLINK => {
            let name = extract_name_from_payload(entry);
            match fs.unlink(req, nodeid, OsStr::from_bytes(&name)).await {
                Ok(()) => DispatchResult::Empty,
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_RMDIR => {
            let name = extract_name_from_payload(entry);
            match fs.rmdir(req, nodeid, OsStr::from_bytes(&name)).await {
                Ok(()) => DispatchResult::Empty,
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_RENAME => {
            let arg: &fuse_rename_in = entry.header().op_in_as();
            let payload = entry.payload();
            let (old_name, new_name) = parse_two_names(payload);
            match fs
                .rename(
                    req,
                    nodeid,
                    OsStr::from_bytes(old_name),
                    arg.newdir,
                    OsStr::from_bytes(new_name),
                    0,
                )
                .await
            {
                Ok(()) => DispatchResult::Empty,
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_RENAME2 => {
            let arg: &fuse_rename2_in = entry.header().op_in_as();
            let payload = entry.payload();
            let (old_name, new_name) = parse_two_names(payload);
            match fs
                .rename(
                    req,
                    nodeid,
                    OsStr::from_bytes(old_name),
                    arg.newdir,
                    OsStr::from_bytes(new_name),
                    arg.flags,
                )
                .await
            {
                Ok(()) => DispatchResult::Empty,
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_LINK => {
            let arg: &fuse_link_in = entry.header().op_in_as();
            let name = extract_name_from_payload(entry);
            match fs
                .link(req, arg.oldnodeid, nodeid, OsStr::from_bytes(&name))
                .await
            {
                Ok(reply) => DispatchResult::Entry(reply),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_OPEN => {
            let arg: &fuse_open_in = entry.header().op_in_as();
            match fs.open(req, nodeid, arg.flags).await {
                Ok(reply) => DispatchResult::Open(reply),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_READ => {
            let arg: &fuse_read_in = entry.header().op_in_as();
            let fh = arg.fh;
            let read_offset = arg.offset;
            let read_size_raw = arg.size;
            debug!(
                "READ request: nodeid={} fh={} offset={} size={}",
                nodeid, fh, read_offset, read_size_raw
            );
            let read_size = (read_size_raw as usize).min(entry.payload_len());
            match fs
                .read(
                    req,
                    nodeid,
                    fh,
                    read_offset,
                    &mut entry.payload_mut()[..read_size],
                )
                .await
            {
                Ok(n) => DispatchResult::Data(n),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_WRITE => {
            let arg: &fuse_write_in = entry.header().op_in_as();
            let payload = entry.payload();
            let data = &payload[..std::cmp::min(arg.size as usize, payload.len())];
            match fs
                .write(
                    req,
                    nodeid,
                    arg.fh,
                    arg.offset,
                    data,
                    arg.write_flags,
                    arg.flags,
                )
                .await
            {
                Ok(reply) => DispatchResult::Write(reply),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_STATFS => match fs.statfs(req, nodeid).await {
            Ok(reply) => DispatchResult::Statfs(reply),
            Err(e) => DispatchResult::Error(e),
        },
        FUSE_RELEASE => {
            let arg: &fuse_release_in = entry.header().op_in_as();
            let flush = arg.release_flags & 1 != 0; // FUSE_RELEASE_FLUSH
            match fs
                .release(req, nodeid, arg.fh, arg.flags, arg.lock_owner, flush)
                .await
            {
                Ok(()) => DispatchResult::Empty,
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_FSYNC => {
            let arg: &fuse_fsync_in = entry.header().op_in_as();
            let datasync = arg.fsync_flags & 1 != 0;
            match fs.fsync(req, nodeid, arg.fh, datasync).await {
                Ok(()) => DispatchResult::Empty,
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_FLUSH => {
            let arg: &fuse_flush_in = entry.header().op_in_as();
            match fs.flush(req, nodeid, arg.fh, arg.lock_owner).await {
                Ok(()) => DispatchResult::Empty,
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_OPENDIR => {
            let arg: &fuse_open_in = entry.header().op_in_as();
            match fs.opendir(req, nodeid, arg.flags).await {
                Ok(reply) => DispatchResult::Open(reply),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_READDIR => {
            let arg: &fuse_read_in = entry.header().op_in_as();
            match fs.readdir(req, nodeid, arg.fh, arg.offset, arg.size).await {
                Ok(entries) => DispatchResult::Readdir(entries, arg.size),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_READDIRPLUS => {
            let arg: &fuse_read_in = entry.header().op_in_as();
            match fs
                .readdirplus(req, nodeid, arg.fh, arg.offset, arg.size)
                .await
            {
                Ok(entries) => DispatchResult::Readdirplus(entries, arg.size),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_RELEASEDIR => {
            let arg: &fuse_release_in = entry.header().op_in_as();
            match fs.releasedir(req, nodeid, arg.fh, arg.flags).await {
                Ok(()) => DispatchResult::Empty,
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_FSYNCDIR => {
            let arg: &fuse_fsync_in = entry.header().op_in_as();
            let datasync = arg.fsync_flags & 1 != 0;
            match fs.fsyncdir(req, nodeid, arg.fh, datasync).await {
                Ok(()) => DispatchResult::Empty,
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_ACCESS => {
            let arg: &fuse_access_in = entry.header().op_in_as();
            match fs.access(req, nodeid, arg.mask).await {
                Ok(()) => DispatchResult::Empty,
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_CREATE => {
            let arg: &fuse_create_in = entry.header().op_in_as();
            let name = extract_name_from_payload(entry);
            match fs
                .create(req, nodeid, OsStr::from_bytes(&name), arg.mode, arg.flags)
                .await
            {
                Ok(reply) => DispatchResult::Create(reply),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_FALLOCATE => {
            let arg: &fuse_fallocate_in = entry.header().op_in_as();
            match fs
                .fallocate(req, nodeid, arg.fh, arg.offset, arg.length, arg.mode)
                .await
            {
                Ok(()) => DispatchResult::Empty,
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_LSEEK => {
            let arg: &fuse_lseek_in = entry.header().op_in_as();
            match fs.lseek(req, nodeid, arg.fh, arg.offset, arg.whence).await {
                Ok(offset) => DispatchResult::Lseek(offset),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_COPY_FILE_RANGE => {
            let arg: &fuse_copy_file_range_in = entry.header().op_in_as();
            match fs
                .copy_file_range(
                    req,
                    nodeid,
                    arg.fh_in,
                    arg.off_in,
                    arg.nodeid_out,
                    arg.fh_out,
                    arg.off_out,
                    arg.len,
                    arg.flags,
                )
                .await
            {
                Ok(reply) => DispatchResult::Write(reply),
                Err(e) => DispatchResult::Error(e),
            }
        }
        FUSE_DESTROY => {
            fs.destroy().await;
            DispatchResult::Empty
        }
        FUSE_STATX => {
            let arg: &fuse_statx_in = entry.header().op_in_as();
            let fh = if arg.getattr_flags & FUSE_GETATTR_FH != 0 {
                Some(arg.fh)
            } else {
                None
            };
            match fs.statx(req, nodeid, fh, arg.sx_flags, arg.sx_mask).await {
                Ok(reply) => DispatchResult::Statx(reply),
                Err(e) => DispatchResult::Error(e),
            }
        }
        _ => {
            warn!("unsupported FUSE opcode: {}", opcode);
            DispatchResult::Error(ENOSYS)
        }
    }
}

enum DispatchResult {
    Empty,
    Error(Errno),
    Entry(ReplyEntry),
    Attr(ReplyAttr),
    Statx(ReplyStatx),
    Open(ReplyOpen),
    /// Data already written directly into the payload buffer by read.
    /// The usize is the number of valid bytes in the payload.
    Data(usize),
    Write(usize),
    Create(ReplyCreate),
    Statfs(ReplyStatfs),
    Readdir(Vec<DirectoryEntry>, u32),
    Readdirplus(Vec<DirectoryEntryPlus>, u32),
    Readlink(ReplyReadlink),
    Lseek(u64),
}

/// Serialize a FUSE response into the ring entry buffers.
///
/// For io_uring FUSE, the kernel reads responses as follows (matching libfuse):
/// - `fuse_out_header` from the `in_out` area of the header buffer
/// - Response body (e.g., `fuse_attr_out`) from the **payload** buffer
/// - Body size from `ring_ent_in_out.payload_sz`
fn serialize_response(entry: &mut RingEntry, unique: u64, _opcode: u32, result: DispatchResult) {
    match result {
        DispatchResult::Error(errno) => {
            write_out_header(entry, unique, -errno, 0);
            entry.header_mut().ring_ent_in_out.payload_sz = 0;
        }
        DispatchResult::Empty => {
            write_out_header(entry, unique, 0, 0);
            entry.header_mut().ring_ent_in_out.payload_sz = 0;
        }
        DispatchResult::Entry(reply) => {
            debug!(
                "ENTRY reply: ino={} size={} mode=0o{:o}",
                reply.attr.ino, reply.attr.size, reply.attr.mode
            );
            let out = fuse_entry_out {
                nodeid: reply.attr.ino,
                generation: reply.generation,
                entry_valid: reply.ttl.as_secs(),
                attr_valid: reply.ttl.as_secs(),
                entry_valid_nsec: reply.ttl.subsec_nanos(),
                attr_valid_nsec: reply.ttl.subsec_nanos(),
                attr: reply.attr.to_fuse_attr(),
            };
            write_payload_struct(entry, unique, &out);
        }
        DispatchResult::Attr(reply) => {
            debug!(
                "ATTR reply: ino={} size={} mode=0o{:o}",
                reply.attr.ino, reply.attr.size, reply.attr.mode
            );
            let out = fuse_attr_out {
                attr_valid: reply.ttl.as_secs(),
                attr_valid_nsec: reply.ttl.subsec_nanos(),
                dummy: 0,
                attr: reply.attr.to_fuse_attr(),
            };
            write_payload_struct(entry, unique, &out);
        }
        DispatchResult::Statx(reply) => {
            debug!(
                "STATX reply: ino={} size={} mode=0o{:o}",
                reply.stat.ino, reply.stat.size, reply.stat.mode
            );
            let out = fuse_statx_out {
                attr_valid: reply.ttl.as_secs(),
                attr_valid_nsec: reply.ttl.subsec_nanos(),
                flags: reply.flags,
                stat: reply.stat.into(),
                ..Default::default()
            };
            write_payload_struct(entry, unique, &out);
        }
        DispatchResult::Open(reply) => {
            let out = fuse_open_out {
                fh: reply.fh,
                open_flags: reply.flags,
                backing_id: reply.backing_id,
            };
            write_payload_struct(entry, unique, &out);
        }
        DispatchResult::Data(n) => {
            // Data was already written into the payload by read
            let data_len = n.min(entry.payload_len());
            debug!("DATA reply (direct): {} bytes", data_len);
            write_out_header(entry, unique, 0, data_len as u32);
            entry.header_mut().ring_ent_in_out.payload_sz = data_len as u32;
        }
        DispatchResult::Write(written) => {
            let out = fuse_write_out {
                size: written as u32,
                padding: 0,
            };
            write_payload_struct(entry, unique, &out);
        }
        DispatchResult::Create(reply) => {
            let entry_out = fuse_entry_out {
                nodeid: reply.attr.ino,
                generation: reply.generation,
                entry_valid: reply.ttl.as_secs(),
                attr_valid: reply.ttl.as_secs(),
                entry_valid_nsec: reply.ttl.subsec_nanos(),
                attr_valid_nsec: reply.ttl.subsec_nanos(),
                attr: reply.attr.to_fuse_attr(),
            };
            let open_out = fuse_open_out {
                fh: reply.fh,
                open_flags: reply.flags,
                backing_id: 0,
            };
            // CREATE response is entry_out + open_out concatenated in the payload
            let entry_bytes = unsafe {
                std::slice::from_raw_parts(
                    &entry_out as *const _ as *const u8,
                    std::mem::size_of::<fuse_entry_out>(),
                )
            };
            let open_bytes = unsafe {
                std::slice::from_raw_parts(
                    &open_out as *const _ as *const u8,
                    std::mem::size_of::<fuse_open_out>(),
                )
            };
            let total = entry_bytes.len() + open_bytes.len();
            write_out_header(entry, unique, 0, total as u32);
            let payload = entry.payload_mut();
            payload[..entry_bytes.len()].copy_from_slice(entry_bytes);
            payload[entry_bytes.len()..total].copy_from_slice(open_bytes);
            entry.header_mut().ring_ent_in_out.payload_sz = total as u32;
        }
        DispatchResult::Statfs(reply) => {
            let out = fuse_statfs_out {
                st: fuse_kstatfs {
                    blocks: reply.blocks,
                    bfree: reply.bfree,
                    bavail: reply.bavail,
                    files: reply.files,
                    ffree: reply.ffree,
                    bsize: reply.bsize,
                    namelen: reply.namelen,
                    frsize: reply.frsize,
                    padding: 0,
                    spare: [0; 6],
                },
            };
            write_payload_struct(entry, unique, &out);
        }
        DispatchResult::Readdir(entries, max_size) => {
            let mut offset = 0usize;
            let payload = entry.payload_mut();
            let max = (max_size as usize).min(payload.len());

            for de in &entries {
                let name_len = de.name.len();
                let ent_size = fuse_dirent_size(name_len);
                if offset + ent_size > max {
                    break;
                }
                let dirent = fuse_dirent {
                    ino: de.ino,
                    off: de.offset,
                    namelen: name_len as u32,
                    typ: de.kind.to_dirent_type(),
                };
                let dirent_bytes = unsafe {
                    std::slice::from_raw_parts(
                        &dirent as *const _ as *const u8,
                        std::mem::size_of::<fuse_dirent>(),
                    )
                };
                payload[offset..offset + dirent_bytes.len()].copy_from_slice(dirent_bytes);
                offset += dirent_bytes.len();
                payload[offset..offset + name_len].copy_from_slice(&de.name);
                offset += name_len;
                // Pad to 8-byte alignment
                let aligned = fuse_dirent_align(offset);
                for b in &mut payload[offset..aligned] {
                    *b = 0;
                }
                offset = aligned;
            }

            write_out_header(entry, unique, 0, offset as u32);
            entry.header_mut().ring_ent_in_out.payload_sz = offset as u32;
        }
        DispatchResult::Readdirplus(entries, max_size) => {
            let mut offset = 0usize;
            let payload = entry.payload_mut();
            let max = (max_size as usize).min(payload.len());

            for de in &entries {
                let name_len = de.name.len();
                let ent_size = fuse_direntplus_size(name_len);
                if offset + ent_size > max {
                    break;
                }

                let entry_out = fuse_entry_out {
                    nodeid: de.ino,
                    generation: de.generation,
                    entry_valid: de.entry_ttl.as_secs(),
                    attr_valid: de.entry_ttl.as_secs(),
                    entry_valid_nsec: de.entry_ttl.subsec_nanos(),
                    attr_valid_nsec: de.entry_ttl.subsec_nanos(),
                    attr: de.attr.to_fuse_attr(),
                };
                let dirent = fuse_dirent {
                    ino: de.ino,
                    off: de.offset,
                    namelen: name_len as u32,
                    typ: de.kind.to_dirent_type(),
                };

                let entry_bytes = unsafe {
                    std::slice::from_raw_parts(
                        &entry_out as *const _ as *const u8,
                        std::mem::size_of::<fuse_entry_out>(),
                    )
                };
                let dirent_bytes = unsafe {
                    std::slice::from_raw_parts(
                        &dirent as *const _ as *const u8,
                        std::mem::size_of::<fuse_dirent>(),
                    )
                };

                payload[offset..offset + entry_bytes.len()].copy_from_slice(entry_bytes);
                offset += entry_bytes.len();
                payload[offset..offset + dirent_bytes.len()].copy_from_slice(dirent_bytes);
                offset += dirent_bytes.len();
                payload[offset..offset + name_len].copy_from_slice(&de.name);
                offset += name_len;
                let aligned = fuse_dirent_align(offset);
                for b in &mut payload[offset..aligned] {
                    *b = 0;
                }
                offset = aligned;
            }

            write_out_header(entry, unique, 0, offset as u32);
            entry.header_mut().ring_ent_in_out.payload_sz = offset as u32;
        }
        DispatchResult::Readlink(reply) => {
            let data = &reply.data;
            let data_len = data.len().min(entry.payload_len());
            write_out_header(entry, unique, 0, data_len as u32);
            entry.payload_mut()[..data_len].copy_from_slice(&data[..data_len]);
            entry.header_mut().ring_ent_in_out.payload_sz = data_len as u32;
        }
        DispatchResult::Lseek(offset) => {
            let out = fuse_lseek_out { offset };
            write_payload_struct(entry, unique, &out);
        }
    }
}

fn write_out_header(entry: &mut RingEntry, unique: u64, error: i32, len: u32) {
    let hdr = entry.header_mut().out_header_mut();
    hdr.len = len;
    hdr.error = error;
    hdr.unique = unique;
}

/// Write a struct response body to the payload buffer and set payload_sz.
fn write_payload_struct<T>(entry: &mut RingEntry, unique: u64, data: &T) {
    let size = std::mem::size_of::<T>();
    let bytes = unsafe { std::slice::from_raw_parts(data as *const T as *const u8, size) };
    write_out_header(entry, unique, 0, size as u32);
    entry.payload_mut()[..size].copy_from_slice(bytes);
    entry.header_mut().ring_ent_in_out.payload_sz = size as u32;
}

/// Extract a NUL-terminated name from the payload buffer.
fn extract_name_from_payload(entry: &RingEntry) -> Vec<u8> {
    let payload = entry.payload();
    let len = payload
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(payload.len());
    payload[..len].to_vec()
}

/// Parse two NUL-separated names from the payload (e.g., for rename, symlink).
fn parse_two_names(payload: &[u8]) -> (&[u8], &[u8]) {
    let first_end = payload
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(payload.len());
    let second_start = first_end + 1;
    if second_start >= payload.len() {
        return (&payload[..first_end], &[]);
    }
    let second = &payload[second_start..];
    let second_end = second.iter().position(|&b| b == 0).unwrap_or(second.len());
    (&payload[..first_end], &second[..second_end])
}
