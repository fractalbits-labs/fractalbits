//! Open-time whole-blob prefetch policy.
//!
//! On `vfs_open`, the policy decides whether to spawn a background task
//! that reads every block of the file into the disk cache. Once
//! complete, subsequent opens of the same blob can arm
//! `FUSE_PASSTHROUGH` and serve reads directly from NVMe with zero
//! FUSE crossing.
//!
//! The decision is intentionally cheap: a few comparisons against the
//! file size and the kernel's `FOPEN_KEEP_CACHE` hint. The heavy lifting
//! (block fetches, cache-pressure decline) is `prefetch_blob` at the
//! bottom of this file, which `vfs_open` spawns as a detached task.

use data_types::TraceId;
use data_types::object_layout::ObjectLayout;
use std::sync::Arc;

use crate::backend::{BackendConfig, StorageBackend};
use crate::config::Config;
use crate::disk_cache::DiskCache;

/// Tunable thresholds and opt-ins for `should_prefetch`. Built once
/// from `Config` at startup so the hot decision path doesn't reparse
/// strings or re-multiply MB-to-bytes per open.
#[derive(Debug, Clone, Copy)]
pub struct PrefetchPolicy {
    pub full_threshold_bytes: u64,
    pub partial_threshold_bytes: u64,
    pub workload_bulk_read: bool,
    pub pressure_decline: f64,
}

impl PrefetchPolicy {
    pub fn from_config(cfg: &Config) -> Self {
        const MIB: u64 = 1024 * 1024;
        Self {
            full_threshold_bytes: cfg.prefetch_full_threshold_mb.saturating_mul(MIB),
            partial_threshold_bytes: cfg.prefetch_partial_threshold_mb.saturating_mul(MIB),
            workload_bulk_read: cfg.workload_bulk_read,
            // Clamp into a usable range so a misconfiguration never
            // triggers prefetches when the cache is full.
            pressure_decline: cfg.prefetch_pressure_decline.clamp(0.0, 1.0),
        }
    }
}

/// `true` if `vfs_open` should spawn a whole-blob prefetch for this
/// file. The rule, in priority order:
///
/// 1. Empty files do not prefetch (nothing to do, and zero size makes
///    `is_complete` always `false` so passthrough cannot arm anyway).
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

/// `true` if the disk cache is too full to absorb a whole-blob prefetch
/// without immediately racing the evictor. Keeps prefetch from
/// contributing to thrash under capacity pressure.
pub fn cache_pressure_high(usage_bytes: u64, capacity_bytes: u64, policy: &PrefetchPolicy) -> bool {
    if capacity_bytes == 0 {
        return true;
    }
    let frac = usage_bytes as f64 / capacity_bytes as f64;
    frac >= policy.pressure_decline
}

/// Background whole-blob prefetch. Walks every block of `layout`,
/// fetches it from BSS, and inserts it into the disk cache. Each
/// per-block fetch goes through the same path as a read miss
/// (`backend.read_block` + `dc.insert`) so block_id, version, and
/// checksum semantics stay identical between prefetch-warmed entries
/// and lazy-warmed ones.
///
/// Errors are logged and ignored: a prefetch is best-effort, and a
/// transient failure is acceptable; the kernel's block-on-demand
/// path still serves the read.
pub(crate) async fn prefetch_blob(
    backend_cfg: Arc<BackendConfig>,
    disk_cache: Arc<DiskCache>,
    layout: ObjectLayout,
    rows: Option<Arc<data_types::ovr_map::OvrRowMap>>,
) {
    let Ok(file_size) = layout.size() else {
        return;
    };
    if file_size == 0 {
        return;
    }
    let Ok(blob_guid) = layout.blob_guid() else {
        return;
    };
    let block_size = layout.block_size as u64;
    if block_size == 0 {
        return;
    }
    // Re-check pressure: an unrelated workload may have filled the
    // cache between the open-time decision and the task starting.
    let policy = PrefetchPolicy {
        full_threshold_bytes: u64::MAX,
        partial_threshold_bytes: u64::MAX,
        workload_bulk_read: false,
        // Reuse the cache's high-watermark fraction for the in-task
        // pressure decline.
        pressure_decline: 0.95,
    };
    if cache_pressure_high(
        disk_cache.current_usage(),
        disk_cache.capacity_bytes(),
        &policy,
    ) {
        return;
    }

    let backend = match StorageBackend::new(&backend_cfg) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "prefetch: failed to construct backend");
            return;
        }
    };

    let last_block = ((file_size - 1) / block_size) as u32;
    let trace_id = TraceId::new();

    let ceiling = layout.blob_version;
    for block_num in 0..=last_block {
        let block_start = block_num as u64 * block_size;
        let block_content_len = std::cmp::min(block_size, file_size - block_start) as usize;

        // Resolve the block's exact committed identity from the rows;
        // holes need no fetch (and any stale cached entry for them is
        // superseded by the row).
        let (version, read_len, miss_is_loss) = match data_types::ovr_map::block_fetch_plan(
            rows.as_deref(),
            block_num,
            ceiling,
            layout.block_size as usize,
            block_content_len,
        ) {
            data_types::ovr_map::BlockFetchPlan::Fetch {
                version,
                read_len,
                miss_is_loss,
            } => (version, read_len, miss_is_loss),
            data_types::ovr_map::BlockFetchPlan::Zeros
            | data_types::ovr_map::BlockFetchPlan::Stale => continue,
        };

        // If another path has already populated this block (e.g. a
        // racing read), only the exact committed identity can
        // short-circuit the BSS round trip.
        if disk_cache
            .get_block_exact(blob_guid, block_num, version, block_content_len)
            .await
            .is_some()
        {
            continue;
        }

        let (mut data, _checksum) = match backend
            .read_block(blob_guid, version, block_num, read_len, &trace_id)
            .await
        {
            Ok(r) => r,
            Err(e) if e.is_block_missing() && !miss_is_loss => {
                // Sparse hole; intentionally not cached. The
                // block-on-demand path treats missing blocks as zeros.
                continue;
            }
            Err(e) => {
                tracing::debug!(
                    %blob_guid, block_num, version, error = %e,
                    "prefetch block fetch failed; abandoning prefetch"
                );
                return;
            }
        };
        if data.len() > block_content_len {
            data = data.slice(0..block_content_len);
        }

        let _ = disk_cache
            .insert_block(blob_guid, block_num, version, &data)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy_default() -> PrefetchPolicy {
        PrefetchPolicy {
            full_threshold_bytes: 256 * 1024 * 1024,
            partial_threshold_bytes: 4096 * 1024 * 1024,
            workload_bulk_read: false,
            pressure_decline: 0.90,
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
        assert!(!should_prefetch(size, true, &p));
        assert!(!should_prefetch(size, false, &p));
        p.workload_bulk_read = true;
        assert!(should_prefetch(size, false, &p));
    }

    #[test]
    fn workload_bulk_read_does_not_resurrect_empty_files() {
        let p = PrefetchPolicy {
            workload_bulk_read: true,
            ..policy_default()
        };
        assert!(!should_prefetch(0, true, &p));
    }

    #[test]
    fn cache_pressure_high_thresholds_correctly() {
        let p = policy_default();
        assert!(!cache_pressure_high(0, 1000, &p));
        assert!(!cache_pressure_high(800, 1000, &p));
        assert!(cache_pressure_high(900, 1000, &p));
        assert!(cache_pressure_high(1000, 1000, &p));
    }

    #[test]
    fn cache_pressure_high_zero_capacity_is_full() {
        let p = policy_default();
        assert!(cache_pressure_high(0, 0, &p));
    }

    #[test]
    fn pressure_decline_clamped_to_unit_interval() {
        let cfg_low = crate::config::Config {
            prefetch_pressure_decline: -0.5,
            ..Default::default()
        };
        let cfg_high = crate::config::Config {
            prefetch_pressure_decline: 1.5,
            ..Default::default()
        };
        let p_low = PrefetchPolicy::from_config(&cfg_low);
        let p_high = PrefetchPolicy::from_config(&cfg_high);
        assert!((0.0..=1.0).contains(&p_low.pressure_decline));
        assert!((0.0..=1.0).contains(&p_high.pressure_decline));
    }

    #[test]
    fn from_config_converts_mib_to_bytes() {
        let cfg = crate::config::Config {
            prefetch_full_threshold_mb: 100,
            prefetch_partial_threshold_mb: 2000,
            ..Default::default()
        };
        let p = PrefetchPolicy::from_config(&cfg);
        assert_eq!(p.full_threshold_bytes, 100 * 1024 * 1024);
        assert_eq!(p.partial_threshold_bytes, 2000u64 * 1024 * 1024);
    }
}
