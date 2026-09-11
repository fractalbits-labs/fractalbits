//! Whole-file warm of the disk cache, driven by a client hint that names
//! only the inode key. The gateway resolves each block's exact committed
//! identity from the layout and row map, so prefetch-warmed entries are
//! byte-identical to lazily warmed ones.

use std::rc::Rc;
use std::sync::Arc;

use data_types::TraceId;
use data_types::object_layout::ObjectLayout;
use data_types::ovr_map::{BlockFetchPlan, OvrRowMap, block_fetch_plan};

use crate::backend::StorageBackend;
use crate::disk_cache::DiskCache;

/// One block to fetch at its exact committed identity.
#[derive(Debug, Clone, Copy)]
pub struct PrefetchBlock {
    pub block_number: u32,
    pub version: u64,
    pub read_len: usize,
    pub content_len: usize,
}

/// `true` if the disk cache is too full to absorb a whole-blob prefetch
/// without immediately racing the evictor.
pub fn cache_pressure_high(usage_bytes: u64, capacity_bytes: u64, decline: f64) -> bool {
    if capacity_bytes == 0 {
        return true;
    }
    let frac = usage_bytes as f64 / capacity_bytes as f64;
    frac >= decline.clamp(0.0, 1.0)
}

/// Resolve every block of `layout` to the exact committed identity to
/// fetch. Holes and stale resolutions are skipped: the former need no
/// fetch, the latter mean the layout snapshot is older than the row and
/// the read path will refresh it.
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
                read_len,
                content_len,
            });
        }
    }
    plan
}

/// Best-effort: a transient failure abandons the prefetch and the
/// block-on-demand path still serves the read.
pub async fn prefetch_blob(
    backend: Rc<StorageBackend>,
    disk_cache: Arc<DiskCache>,
    blob_guid: data_types::DataBlobGuid,
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
        if disk_cache
            .get_block_exact(blob_guid, plan.block_number, plan.version, plan.content_len)
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
                plan.read_len,
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
        if data.len() > plan.content_len {
            data = data.slice(0..plan.content_len);
        }
        let _ = disk_cache
            .insert_block(blob_guid, plan.block_number, plan.version, &data)
            .await;
    }
}
