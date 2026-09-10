use std::io;
use std::mem::size_of;
use std::ptr;

use compio_driver::{OpCode, OpEntry};
use io_uring::opcode::UringCmd80;
use io_uring::squeue::Entry128;
use io_uring::types::Fixed;

use crate::abi::*;

/// Default queue depth (ring entries per CPU core).
pub const DEFAULT_QUEUE_DEPTH: u16 = 256;

/// Fixed file index for the registered FUSE fd.
const FUSE_FD_INDEX: u32 = 0;

// Page size for buffer alignment
fn page_size() -> usize {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize }
}

fn round_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

/// A single ring entry with its pre-allocated, page-aligned buffers.
///
/// Each entry owns:
/// - A header buffer (`fuse_uring_req_header`, 288 bytes, page-aligned)
/// - A payload buffer (max_write bytes, page-aligned)
/// - An iovec array pointing to these two buffers
pub struct RingEntry {
    /// Page-aligned header buffer
    header_ptr: *mut u8,
    header_len: usize,
    /// Page-aligned payload buffer
    payload_ptr: *mut u8,
    payload_len: usize,
    /// iovec array (2 entries: header, payload)
    iov: [libc::iovec; 2],
}

unsafe impl Send for RingEntry {}
unsafe impl Sync for RingEntry {}

impl RingEntry {
    /// Allocate a new ring entry with page-aligned buffers.
    pub fn new(max_payload: usize) -> io::Result<Self> {
        let page_sz = page_size();
        let header_len = round_up(std::mem::size_of::<fuse_uring_req_header>(), page_sz);
        let payload_len = round_up(max_payload, page_sz);

        let header_ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                header_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_POPULATE,
                -1,
                0,
            )
        };
        if header_ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        let payload_ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                payload_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_POPULATE,
                -1,
                0,
            )
        };
        if payload_ptr == libc::MAP_FAILED {
            unsafe {
                libc::munmap(header_ptr, header_len);
            }
            return Err(io::Error::last_os_error());
        }

        let header_ptr = header_ptr as *mut u8;
        let payload_ptr = payload_ptr as *mut u8;

        let iov = [
            libc::iovec {
                iov_base: header_ptr as *mut libc::c_void,
                iov_len: header_len,
            },
            libc::iovec {
                iov_base: payload_ptr as *mut libc::c_void,
                iov_len: payload_len,
            },
        ];

        Ok(Self {
            header_ptr,
            header_len,
            payload_ptr,
            payload_len,
            iov,
        })
    }

    pub fn header(&self) -> &fuse_uring_req_header {
        unsafe { &*(self.header_ptr as *const fuse_uring_req_header) }
    }

    pub fn header_mut(&mut self) -> &mut fuse_uring_req_header {
        unsafe { &mut *(self.header_ptr as *mut fuse_uring_req_header) }
    }

    pub fn payload(&self) -> &[u8] {
        let payload_sz = self.header().ring_ent_in_out.payload_sz as usize;
        let len = payload_sz.min(self.payload_len);
        unsafe { std::slice::from_raw_parts(self.payload_ptr, len) }
    }

    pub fn payload_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.payload_ptr, self.payload_len) }
    }

    pub fn payload_len(&self) -> usize {
        self.payload_len
    }

    pub fn iov_ptr(&self) -> *const libc::iovec {
        self.iov.as_ptr()
    }

    /// Extract the commit_id from the kernel-filled header.
    pub fn commit_id(&self) -> u64 {
        self.header().ring_ent_in_out.commit_id
    }
}

impl Drop for RingEntry {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.header_ptr as *mut libc::c_void, self.header_len);
            libc::munmap(self.payload_ptr as *mut libc::c_void, self.payload_len);
        }
    }
}

/// Allocate all ring entries for a single queue.
pub fn allocate_ring_entries(queue_depth: u16, max_payload: usize) -> io::Result<Vec<RingEntry>> {
    let mut entries = Vec::with_capacity(queue_depth as usize);
    for _ in 0..queue_depth {
        entries.push(RingEntry::new(max_payload)?);
    }
    Ok(entries)
}

// ---------- OpCode implementations ----------

/// Build the 80-byte cmd area for a FUSE io_uring SQE.
fn build_cmd(qid: u16, commit_id: u64) -> [u8; 80] {
    let req = fuse_uring_cmd_req {
        flags: 0,
        commit_id,
        qid,
        padding: [0; 6],
    };
    let mut cmd = [0u8; 80];
    let src = unsafe {
        std::slice::from_raw_parts(
            &req as *const fuse_uring_cmd_req as *const u8,
            std::mem::size_of::<fuse_uring_cmd_req>(),
        )
    };
    cmd[..src.len()].copy_from_slice(src);
    cmd
}

/// FUSE REGISTER operation: registers a ring entry's buffers with the kernel
/// and fetches the first request.
pub struct FuseRegister {
    iov_ptr: u64,
    qid: u16,
}

impl FuseRegister {
    pub fn new(entry: &RingEntry, qid: u16) -> Self {
        Self {
            iov_ptr: entry.iov_ptr() as u64,
            qid,
        }
    }
}

/// Patch the `len` field (offset 24) in the raw SQE inside an Entry128.
///
/// The upstream io-uring crate does not expose a setter for `sqe.len` on
/// `UringCmd80` (unlike `addr`, which gained a builder in 0.7.12). The field
/// sits at a fixed kernel-ABI offset (24 bytes into `io_uring_sqe`), so we
/// write it directly.
unsafe fn set_sqe_len(entry: &mut Entry128, len: u32) {
    const _: () = assert!(size_of::<Entry128>() == 128);
    let ptr = entry as *mut Entry128 as *mut u8;
    unsafe { ptr.add(24).cast::<u32>().write(len) };
}

unsafe impl OpCode for FuseRegister {
    // No self-references are held across the operation; the iovec pointer
    // refers to externally-owned, pinned `RingEntry` memory.
    type Control = ();

    fn create_entry(&mut self, _: &mut Self::Control) -> OpEntry {
        let cmd = build_cmd(self.qid, 0);

        // `addr` carries the iovec pointer; builder setter added in io-uring 0.7.12.
        let mut entry = UringCmd80::new(Fixed(FUSE_FD_INDEX), FUSE_IO_URING_CMD_REGISTER)
            .cmd(cmd)
            .addr(Some(self.iov_ptr))
            .build();

        // Set iovec count (2) via raw SQE poke -- upstream has no .len() setter
        unsafe { set_sqe_len(&mut entry, 2) };

        entry.into()
    }
}

/// FUSE COMMIT_AND_FETCH operation: sends a response to the kernel and
/// atomically fetches the next request into the same buffers.
pub struct FuseCommitAndFetch {
    qid: u16,
    commit_id: u64,
}

impl FuseCommitAndFetch {
    pub fn new(qid: u16, commit_id: u64) -> Self {
        Self { qid, commit_id }
    }
}

unsafe impl OpCode for FuseCommitAndFetch {
    type Control = ();

    fn create_entry(&mut self, _: &mut Self::Control) -> OpEntry {
        let cmd = build_cmd(self.qid, self.commit_id);

        UringCmd80::new(Fixed(FUSE_FD_INDEX), FUSE_IO_URING_CMD_COMMIT_AND_FETCH)
            .cmd(cmd)
            .build()
            .into()
    }
}
