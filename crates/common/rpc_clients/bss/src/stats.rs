use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    PutData,
    GetData,
    DeleteData,
}

impl OperationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationType::PutData => "put_blob",
            OperationType::GetData => "get_blob",
            OperationType::DeleteData => "delete_blob",
        }
    }

    pub fn all() -> [OperationType; 3] {
        [
            OperationType::GetData,
            OperationType::PutData,
            OperationType::DeleteData,
        ]
    }
}

pub struct BssStats {
    get_blob: AtomicU64,
    put_blob: AtomicU64,
    delete_blob: AtomicU64,
}

impl Default for BssStats {
    fn default() -> Self {
        Self::new()
    }
}

impl BssStats {
    pub fn new() -> Self {
        Self {
            get_blob: AtomicU64::new(0),
            put_blob: AtomicU64::new(0),
            delete_blob: AtomicU64::new(0),
        }
    }

    pub fn increment(&self, op: OperationType) {
        match op {
            OperationType::GetData => self.get_blob.fetch_add(1, Ordering::Relaxed),
            OperationType::PutData => self.put_blob.fetch_add(1, Ordering::Relaxed),
            OperationType::DeleteData => self.delete_blob.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn decrement(&self, op: OperationType) {
        match op {
            OperationType::GetData => self.get_blob.fetch_sub(1, Ordering::Relaxed),
            OperationType::PutData => self.put_blob.fetch_sub(1, Ordering::Relaxed),
            OperationType::DeleteData => self.delete_blob.fetch_sub(1, Ordering::Relaxed),
        };
    }

    pub fn get_count(&self, op: OperationType) -> u64 {
        match op {
            OperationType::GetData => self.get_blob.load(Ordering::Relaxed),
            OperationType::PutData => self.put_blob.load(Ordering::Relaxed),
            OperationType::DeleteData => self.delete_blob.load(Ordering::Relaxed),
        }
    }
}

/// In-flight counter guard: decrements on drop so a cancelled RPC
/// future does not leak the count.
pub struct BssStatsGuard {
    op: OperationType,
}

impl BssStatsGuard {
    pub fn new(op: OperationType) -> Self {
        get_global_bss_stats().increment(op);
        Self { op }
    }
}

impl Drop for BssStatsGuard {
    fn drop(&mut self) {
        get_global_bss_stats().decrement(self.op);
    }
}

static GLOBAL_BSS_STATS: OnceLock<BssStats> = OnceLock::new();

pub fn get_global_bss_stats() -> &'static BssStats {
    GLOBAL_BSS_STATS.get_or_init(BssStats::new)
}
