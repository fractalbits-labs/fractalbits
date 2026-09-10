//! Whole-blob warm of the disk cache, driven by a client hint.
//!
//! The client resolves each block's exact committed identity from its
//! row map and ships the plan; the gateway only fetches and inserts, so
//! prefetch-warmed entries are byte-identical to lazily warmed ones.

use std::rc::Rc;
use std::sync::Arc;

use data_types::{DataBlobGuid, TraceId};
use fs_gateway_codec::prefetch_blob_request::PrefetchBlock;

use crate::backend::StorageBackend;
use crate::disk_cache::DiskCache;

/// `true` if the disk cache is too full to absorb a whole-blob prefetch
/// without immediately racing the evictor.
pub fn cache_pressure_high(usage_bytes: u64, capacity_bytes: u64, decline: f64) -> bool {
    if capacity_bytes == 0 {
        return true;
    }
    let frac = usage_bytes as f64 / capacity_bytes as f64;
    frac >= decline.clamp(0.0, 1.0)
}

/// Best-effort: a transient failure abandons the prefetch and the
/// block-on-demand path still serves the read.
pub async fn prefetch_blob(
    backend: Rc<StorageBackend>,
    disk_cache: Arc<DiskCache>,
    blob_guid: DataBlobGuid,
    blocks: Vec<PrefetchBlock>,
) {
    let trace_id = TraceId::new();
    for plan in blocks {
        // Re-check pressure: an unrelated workload may have filled the
        // cache since the hint was accepted.
        if cache_pressure_high(
            disk_cache.current_usage(),
            disk_cache.capacity_bytes(),
            0.95,
        ) {
            return;
        }
        let content_len = plan.content_len as usize;
        if disk_cache
            .get_block_exact(blob_guid, plan.block_number, plan.version, content_len)
            .await
            .is_some()
        {
            continue;
        }
        let (mut data, _checksum) = match backend
            .read_block(
                blob_guid,
                plan.version,
                plan.block_number,
                plan.read_len as usize,
                &trace_id,
            )
            .await
        {
            Ok(r) => r,
            Err(e) if e.is_block_missing() => {
                // Sparse hole; intentionally not cached.
                continue;
            }
            Err(e) => {
                tracing::debug!(
                    %blob_guid, block = plan.block_number, version = plan.version, error = %e,
                    "prefetch block fetch failed; abandoning prefetch"
                );
                return;
            }
        };
        if data.len() > content_len {
            data = data.slice(0..content_len);
        }
        let _ = disk_cache
            .insert_block(blob_guid, plan.block_number, plan.version, &data)
            .await;
    }
}
