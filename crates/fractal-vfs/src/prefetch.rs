//! Open-time whole-blob prefetch policy.
//!
//! On `vfs_open`, the policy decides whether to spawn a background task
//! that reads every block of the file into the disk cache. Once
//! complete, subsequent opens of the same blob can arm
//! `FUSE_PASSTHROUGH` and serve reads directly from NVMe with zero
//! FUSE crossing.
//!
//! The decision is intentionally cheap: a few comparisons against the
//! file size and the kernel's `FOPEN_KEEP_CACHE` hint. Heavy lifting
//! (block fetches, parallel scheduling, cache-pressure decline) lives
//! in the prefetch task itself.

#![allow(dead_code)] // Wired into `vfs_open` opt-in by config.

use crate::config::Config;

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
///    the kernel sets `FOPEN_KEEP_CACHE` -- the kernel's signal that
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
