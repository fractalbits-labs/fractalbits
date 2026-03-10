use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    PutInode,
    GetInode,
    DeleteInode,
    Other,
}

impl OperationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationType::PutInode => "put_inode",
            OperationType::GetInode => "get_inode",
            OperationType::DeleteInode => "delete_inode",
            OperationType::Other => "other",
        }
    }

    pub fn all() -> [OperationType; 4] {
        [
            OperationType::GetInode,
            OperationType::PutInode,
            OperationType::DeleteInode,
            OperationType::Other,
        ]
    }

    pub fn from_operation(op: NssOperation) -> Self {
        match op {
            NssOperation::PutInode => OperationType::PutInode,
            NssOperation::GetInode => OperationType::GetInode,
            NssOperation::DeleteInode => OperationType::DeleteInode,
            NssOperation::ListInodes
            | NssOperation::CreateRootInode
            | NssOperation::DeleteRootInode
            | NssOperation::RenameFolder
            | NssOperation::RenameObject => OperationType::Other,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum NssOperation {
    PutInode,
    GetInode,
    ListInodes,
    DeleteInode,
    CreateRootInode,
    DeleteRootInode,
    RenameFolder,
    RenameObject,
}

pub struct NssStats {
    get_inode: AtomicU64,
    put_inode: AtomicU64,
    delete_inode: AtomicU64,
    other: AtomicU64,
}

impl Default for NssStats {
    fn default() -> Self {
        Self::new()
    }
}

impl NssStats {
    pub fn new() -> Self {
        Self {
            get_inode: AtomicU64::new(0),
            put_inode: AtomicU64::new(0),
            delete_inode: AtomicU64::new(0),
            other: AtomicU64::new(0),
        }
    }

    pub fn increment(&self, op: NssOperation) {
        let op_type = OperationType::from_operation(op);
        match op_type {
            OperationType::GetInode => self.get_inode.fetch_add(1, Ordering::Relaxed),
            OperationType::PutInode => self.put_inode.fetch_add(1, Ordering::Relaxed),
            OperationType::DeleteInode => self.delete_inode.fetch_add(1, Ordering::Relaxed),
            OperationType::Other => self.other.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn decrement(&self, op: NssOperation) {
        let op_type = OperationType::from_operation(op);
        match op_type {
            OperationType::GetInode => self.get_inode.fetch_sub(1, Ordering::Relaxed),
            OperationType::PutInode => self.put_inode.fetch_sub(1, Ordering::Relaxed),
            OperationType::DeleteInode => self.delete_inode.fetch_sub(1, Ordering::Relaxed),
            OperationType::Other => self.other.fetch_sub(1, Ordering::Relaxed),
        };
    }

    pub fn get_count(&self, op: OperationType) -> u64 {
        match op {
            OperationType::GetInode => self.get_inode.load(Ordering::Relaxed),
            OperationType::PutInode => self.put_inode.load(Ordering::Relaxed),
            OperationType::DeleteInode => self.delete_inode.load(Ordering::Relaxed),
            OperationType::Other => self.other.load(Ordering::Relaxed),
        }
    }
}

static GLOBAL_NSS_STATS: OnceLock<NssStats> = OnceLock::new();

pub fn get_global_nss_stats() -> &'static NssStats {
    GLOBAL_NSS_STATS.get_or_init(NssStats::new)
}
