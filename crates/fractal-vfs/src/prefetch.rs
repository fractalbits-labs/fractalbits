//! Open-time whole-blob prefetch hint.
//!
//! The mount has no cache of its own; the disk cache lives on the
//! gateway. What stays here is the decision: on a read `vfs_open`, a few
//! cheap comparisons against the file size and the kernel's
//! `FOPEN_KEEP_CACHE` hint decide whether the file is worth warming, and
//! `prefetch_plan` resolves each block's exact committed identity from
//! the row map so the gateway can fetch without interpreting layouts.
//! The fetch loop and the cache-pressure check run on the gateway.

use data_types::object_layout::ObjectLayout;
use data_types::ovr_map::{BlockFetchPlan, OvrRowMap, block_fetch_plan};
use fs_gateway_codec::prefetch_blob_request::PrefetchBlock;

use crate::config::Config;

/// Tunable thresholds and opt-ins for `should_prefetch`. Built once
/// from `Config` at startup so the hot decision path doesn't reparse
/// strings or re-multiply MB-to-bytes per open.
#[derive(Debug, Clone, Copy)]
pub struct PrefetchPolicy {
    pub full_threshold_bytes: u64,
    pub partial_threshold_bytes: u64,
    pub workload_bulk_read: bool,
}

impl PrefetchPolicy {
    pub fn from_config(cfg: &Config) -> Self {
        const MIB: u64 = 1024 * 1024;
        Self {
            full_threshold_bytes: cfg.prefetch_full_threshold_mb.saturating_mul(MIB),
            partial_threshold_bytes: cfg.prefetch_partial_threshold_mb.saturating_mul(MIB),
            workload_bulk_read: cfg.workload_bulk_read,
        }
    }
}

/// `true` if `vfs_open` should send a whole-blob prefetch hint for this
/// file. The rule, in priority order:
///
/// 1. Empty files do not prefetch (nothing to do).
/// 2. Files at or below `full_threshold_bytes` always prefetch.
/// 3. Files at or below `partial_threshold_bytes` prefetch only when
///    the kernel sets `FOPEN_KEEP_CACHE`, the kernel's signal that
///    the application expects to read sequentially.
/// 4. Volumes flagged `workload_bulk_read=true` prefetch
///    unconditionally for any non-empty file.
pub fn should_prefetch(file_size: u64, fopen_keep_cache: bool, policy: &PrefetchPolicy) -> bool {
    if file_size == 0 {
        return false;
    }
    if file_size <= policy.full_threshold_bytes {
        return true;
    }
    if file_size <= policy.partial_threshold_bytes && fopen_keep_cache {
        return true;
    }
    policy.workload_bulk_read
}

/// Resolve every block of `layout` to the exact committed identity the
/// gateway should fetch. Holes and stale resolutions are skipped: the
/// former need no fetch, the latter mean the layout snapshot is older
/// than the row and the read path will refresh it.
pub fn prefetch_plan(layout: &ObjectLayout, rows: Option<&OvrRowMap>) -> Vec<PrefetchBlock> {
    let Ok(file_size) = layout.size() else {
        return Vec::new();
    };
    let block_size = layout.block_size as u64;
    if file_size == 0 || block_size == 0 {
        return Vec::new();
    }
    let ceiling = layout.blob_version;
    let last_block = ((file_size - 1) / block_size) as u32;
    let mut plan = Vec::with_capacity(last_block as usize + 1);
    for block_number in 0..=last_block {
        let block_start = block_number as u64 * block_size;
        let content_len = std::cmp::min(block_size, file_size - block_start) as usize;
        if let BlockFetchPlan::Fetch {
            version, read_len, ..
        } = block_fetch_plan(
            rows,
            block_number,
            ceiling,
            layout.block_size as usize,
            content_len,
        ) {
            plan.push(PrefetchBlock {
                block_number,
                version,
                read_len: read_len as u32,
                content_len: content_len as u32,
            });
        }
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy_default() -> PrefetchPolicy {
        PrefetchPolicy {
            full_threshold_bytes: 256 * 1024 * 1024,
            partial_threshold_bytes: 4096 * 1024 * 1024,
            workload_bulk_read: false,
        }
    }

    #[test]
    fn empty_file_never_prefetches() {
        assert!(!should_prefetch(0, true, &policy_default()));
        assert!(!should_prefetch(
            0,
            false,
            &PrefetchPolicy {
                workload_bulk_read: true,
                ..policy_default()
            }
        ));
    }

    #[test]
    fn small_file_always_prefetches() {
        let p = policy_default();
        // 100 MiB <= 256 MiB full threshold.
        assert!(should_prefetch(100 * 1024 * 1024, false, &p));
        assert!(should_prefetch(100 * 1024 * 1024, true, &p));
    }

    #[test]
    fn boundary_at_full_threshold_inclusive() {
        let p = policy_default();
        assert!(should_prefetch(p.full_threshold_bytes, false, &p));
        assert!(!should_prefetch(p.full_threshold_bytes + 1, false, &p));
    }

    #[test]
    fn medium_file_prefetches_only_with_keep_cache_hint() {
        let p = policy_default();
        // 1 GiB > full but <= partial.
        let size = 1024 * 1024 * 1024;
        assert!(!should_prefetch(size, false, &p));
        assert!(should_prefetch(size, true, &p));
    }

    #[test]
    fn medium_file_at_partial_threshold_inclusive() {
        let p = policy_default();
        assert!(should_prefetch(p.partial_threshold_bytes, true, &p));
        assert!(!should_prefetch(p.partial_threshold_bytes, false, &p));
        assert!(!should_prefetch(p.partial_threshold_bytes + 1, true, &p));
    }

    #[test]
    fn large_file_only_prefetches_if_workload_opt_in() {
        let mut p = policy_default();
        // 10 GiB.
        let size = 10u64 * 1024 * 1024 * 1024;
        assert!(!should_prefetch(size, false, &p));
        assert!(!should_prefetch(size, true, &p));
        p.workload_bulk_read = true;
        assert!(should_prefetch(size, false, &p));
    }
}
