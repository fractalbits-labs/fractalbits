//! Key-addressed data resolution: the layout published at an inode key,
//! the blob's committed `@ovr/` row snapshot, and the exact generation a
//! logical block resolves to. The client never sees a blob id or a
//! version; it names `(key, block)` and the gateway resolves the rest.
//!
//! Both caches are optimizations, never correctness state: a layout is
//! validated against the client's expected version id and a short TTL,
//! a row snapshot is keyed by the `map_epoch` every row-changing commit
//! bumps, so a miss costs one NSS round trip and a hit can never serve a
//! resolution the committed layout would not.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use data_types::TraceId;
use data_types::object_layout::{ObjectLayout, ObjectState};
use data_types::ovr_map::{
    BlockFetchPlan, BlockResolution, OVR_ABORT_VALUE, OvrRow, OvrRowMap, block_fetch_plan,
    ovr_row_prefix, parse_ovr_abort_range, parse_ovr_row_block,
};
use lru::LruCache;
use uuid::Uuid;

use crate::backend::StorageBackend;
use crate::disk_cache::DiskCache;
use crate::error::FsError;

/// Rows loaded per NSS listing page. Fixed-width block keys make one
/// page cover a contiguous block range; the has_more loop covers the
/// rest (the NSS clamp must never silently truncate a snapshot).
const ROW_LOAD_PAGE: u32 = 1000;
const LAYOUT_CACHE_CAP: usize = 1 << 16;
const ROW_MAP_CACHE_CAP: usize = 4096;
/// A cached layout served without the client naming a version id is
/// refreshed on the same TTL the mount uses for attributes.
const LAYOUT_TTL: Duration = Duration::from_secs(1);
/// Bound on layout refreshes within one read before giving up.
const RESOLVE_ATTEMPTS: u32 = 4;

struct CachedLayout {
    layout: ObjectLayout,
    fetched_at: Instant,
}

/// Per-bucket caches shared by every gateway thread.
pub struct ResolveCaches {
    layouts: parking_lot::Mutex<LruCache<String, CachedLayout>>,
    rows: parking_lot::Mutex<LruCache<(Uuid, u64), Arc<OvrRowMap>>>,
}

impl Default for ResolveCaches {
    fn default() -> Self {
        Self {
            layouts: parking_lot::Mutex::new(LruCache::new(
                NonZeroUsize::new(LAYOUT_CACHE_CAP).expect("non-zero cap"),
            )),
            rows: parking_lot::Mutex::new(LruCache::new(
                NonZeroUsize::new(ROW_MAP_CACHE_CAP).expect("non-zero cap"),
            )),
        }
    }
}

/// One resolved logical block.
pub enum BlockRead {
    Data(Bytes),
    /// Reads as zeros: a committed hole, a sparse base miss, or a block
    /// at or beyond the committed size.
    Hole,
}

impl StorageBackend {
    /// The committed layout at `key`, with a hardlink redirect followed.
    /// `expected_version_id` is the layout the client read against: a
    /// cached entry with another version id is refetched before it can
    /// be reported stale. `bypass` forces a fetch.
    pub async fn resolve_layout(
        &self,
        key: &str,
        expected_version_id: Option<Uuid>,
        bypass: bool,
        trace_id: &TraceId,
    ) -> Result<ObjectLayout, FsError> {
        if !bypass
            && let Some(cached) = self.caches.layouts.lock().get(key)
            && cached.fetched_at.elapsed() < LAYOUT_TTL
            && expected_version_id.is_none_or(|expected| cached.layout.version_id == expected)
        {
            return Ok(cached.layout.clone());
        }
        let layout = self
            .layout_at(key, trace_id)
            .await?
            .ok_or(FsError::NotFound)?;
        self.cache_layout(key, layout.clone());
        Ok(layout)
    }

    /// Remember the layout now published at `key` (a fetch, or a commit
    /// this gateway just made).
    pub fn cache_layout(&self, key: &str, layout: ObjectLayout) {
        self.caches.layouts.lock().put(
            key.to_string(),
            CachedLayout {
                layout,
                fetched_at: Instant::now(),
            },
        );
    }

    pub fn forget_layout(&self, key: &str) {
        self.caches.layouts.lock().pop(key);
    }

    /// The blob's committed row snapshot for `layout`, or `None` for an
    /// unmapped blob (`map_epoch == 0`), which by definition has no
    /// committed row.
    pub async fn row_map_for(
        &self,
        layout: &ObjectLayout,
        trace_id: &TraceId,
    ) -> Result<Option<Arc<OvrRowMap>>, FsError> {
        if !layout.is_mapped() {
            return Ok(None);
        }
        let blob_id = layout.blob_guid()?.blob_id;
        Ok(Some(
            self.row_map_at(blob_id, layout.map_epoch(), trace_id)
                .await?,
        ))
    }

    /// Load (or serve cached) the row snapshot for `blob_id` at
    /// `map_epoch`. A snapshot at epoch M serves any read whose layout
    /// carries the same M; a delayed aborted row can land at that epoch
    /// but resolves identically through its committed fallback.
    pub async fn row_map_at(
        &self,
        blob_id: Uuid,
        map_epoch: u64,
        trace_id: &TraceId,
    ) -> Result<Arc<OvrRowMap>, FsError> {
        if let Some(cached) = self.caches.rows.lock().get(&(blob_id, map_epoch)) {
            return Ok(cached.clone());
        }
        let map = Arc::new(self.load_row_snapshot(blob_id, map_epoch, trace_id).await?);
        self.install_row_map(blob_id, map.clone());
        Ok(map)
    }

    /// Install a snapshot: a fresh load, or the committing flush's
    /// write-through so it never reloads its own rows.
    pub fn install_row_map(&self, blob_id: Uuid, map: Arc<OvrRowMap>) {
        self.caches.rows.lock().put((blob_id, map.epoch), map);
    }

    /// Full-prefix snapshot load, paginated past the NSS clamp.
    async fn load_row_snapshot(
        &self,
        blob_id: Uuid,
        map_epoch: u64,
        trace_id: &TraceId,
    ) -> Result<OvrRowMap, FsError> {
        let prefix = ovr_row_prefix(&blob_id);
        let mut map = OvrRowMap::new(map_epoch);
        let mut start_after = String::new();
        loop {
            let (page, has_more) = self
                .list_inodes_raw_page(&prefix, &start_after, ROW_LOAD_PAGE, trace_id)
                .await?;
            let Some(last_key) = page.last().map(|(key, _)| key.clone()) else {
                return Ok(map);
            };
            for (key, value) in page {
                if let Some(block) = parse_ovr_row_block(&key) {
                    let Some(row) = OvrRow::decode(&value) else {
                        return Err(FsError::Internal(format!("malformed @ovr row at {key}")));
                    };
                    map.insert(block, row);
                } else if let Some((lo, hi)) = parse_ovr_abort_range(&key) {
                    if value.as_ref() != OVR_ABORT_VALUE {
                        return Err(FsError::Internal(format!(
                            "malformed aborted generation record at {key}"
                        )));
                    }
                    map.add_aborted_range(lo, hi);
                } else {
                    return Err(FsError::Internal(format!("malformed @ovr key {key}")));
                }
            }
            if !has_more {
                return Ok(map);
            }
            start_after = last_key;
        }
    }

    /// Read logical block `block` of the file at `key` at its committed
    /// content. Resolution is retried against a fresh layout when the
    /// rows prove the cached one stale, and a base-version miss is a
    /// sparse hole only once the layout is confirmed unchanged; a
    /// row-committed miss is detected data loss, never a hole.
    pub async fn read_block_at_key(
        &self,
        key: &str,
        block: u32,
        expected_version_id: Option<Uuid>,
        disk_cache: Option<&Arc<DiskCache>>,
        trace_id: &TraceId,
    ) -> Result<BlockRead, FsError> {
        let mut bypass = false;
        for _ in 0..RESOLVE_ATTEMPTS {
            let layout = self
                .resolve_layout(key, expected_version_id, bypass, trace_id)
                .await?;
            if let Some(expected) = expected_version_id
                && layout.version_id != expected
            {
                if !bypass {
                    bypass = true;
                    continue;
                }
                return Err(FsError::StaleLayout);
            }
            let ObjectState::Normal(_) = &layout.state else {
                return Err(FsError::InvalidState);
            };
            let blob_guid = layout.blob_guid()?;
            let size = layout.size()?;
            let block_size = layout.block_size as usize;
            let block_start = block as u64 * layout.block_size as u64;
            if block_start >= size {
                return Ok(BlockRead::Hole);
            }
            let content_len = std::cmp::min(layout.block_size as u64, size - block_start) as usize;
            let rows = self.row_map_for(&layout, trace_id).await?;
            let (version, read_len, miss_is_loss) = match block_fetch_plan(
                rows.as_deref(),
                block,
                layout.blob_version,
                block_size,
                content_len,
            ) {
                BlockFetchPlan::Zeros => return Ok(BlockRead::Hole),
                // Both row slots sit above this layout's ceiling: a commit
                // landed after the layout was fetched. Refresh and retry.
                BlockFetchPlan::Stale => {
                    bypass = true;
                    continue;
                }
                BlockFetchPlan::Fetch {
                    version,
                    read_len,
                    miss_is_loss,
                } => (version, read_len, miss_is_loss),
            };
            if let Some(dc) = disk_cache
                && let Some(cached) = dc
                    .get_block_exact(blob_guid, block, version, content_len)
                    .await
            {
                return Ok(BlockRead::Data(cached));
            }
            match self
                .read_block(blob_guid, version, block, read_len, trace_id)
                .await
            {
                Ok((mut data, _checksum)) => {
                    if data.len() > content_len {
                        data = data.slice(0..content_len);
                    }
                    // Cold fill inline so a re-read right after this one is
                    // already a disk hit.
                    if let Some(dc) = disk_cache {
                        let _ = dc.insert_block(blob_guid, block, version, &data).await;
                    }
                    return Ok(BlockRead::Data(data));
                }
                Err(e) if e.is_block_missing() => {
                    if miss_is_loss {
                        tracing::error!(
                            %blob_guid,
                            block,
                            version,
                            "DATA LOSS: row-committed generation missing on every replica"
                        );
                        return Err(FsError::Corrupted);
                    }
                    // A base-version miss is zeros only if the key still
                    // publishes the very layout that issued the read.
                    let fresh = self.resolve_layout(key, None, true, trace_id).await?;
                    if fresh.version_id == layout.version_id {
                        return Ok(BlockRead::Hole);
                    }
                    bypass = true;
                }
                Err(e) => return Err(e),
            }
        }
        Err(FsError::StaleLayout)
    }

    /// Blocks in `[first_block, first_block + block_count)` that hold
    /// data (lseek SEEK_DATA / SEEK_HOLE): a row resolving to a written
    /// generation, or, for unmapped blocks, a stored base-version entry.
    pub async fn probe_data_blocks(
        &self,
        key: &str,
        first_block: u32,
        block_count: u32,
        expected_version_id: Option<Uuid>,
        trace_id: &TraceId,
    ) -> Result<Vec<u32>, FsError> {
        let layout = self
            .resolve_layout(key, expected_version_id, false, trace_id)
            .await?;
        if let Some(expected) = expected_version_id
            && layout.version_id != expected
        {
            return Err(FsError::StaleLayout);
        }
        let ObjectState::Normal(_) = &layout.state else {
            return Err(FsError::InvalidState);
        };
        if block_count == 0 {
            return Ok(Vec::new());
        }
        let blob_guid = layout.blob_guid()?;
        let rows = self.row_map_for(&layout, trace_id).await?;
        let ceiling = layout.blob_version;
        let end = first_block.saturating_add(block_count);
        let mut data_blocks = BTreeSet::new();
        let mut needs_probe = false;
        for block in first_block..end {
            match rows.as_deref().map(|rows| rows.resolve(block, ceiling)) {
                Some(BlockResolution::Exact { .. }) => {
                    data_blocks.insert(block);
                }
                Some(BlockResolution::Hole) => {}
                Some(BlockResolution::Base) | Some(BlockResolution::Stale) | None => {
                    needs_probe = true;
                }
            }
        }
        if needs_probe {
            let entries = self
                .list_blob_blocks(blob_guid, first_block, block_count, trace_id)
                .await?;
            for entry in entries {
                let resolution = rows
                    .as_deref()
                    .map(|rows| rows.resolve(entry.block_number, ceiling));
                let unmapped = matches!(
                    resolution,
                    None | Some(BlockResolution::Base) | Some(BlockResolution::Stale)
                );
                // Listed entries at other versions are pre-sweep garbage
                // of row-covered blocks and must not override the rows.
                if unmapped && entry.version == 1 {
                    data_blocks.insert(entry.block_number);
                }
            }
        }
        Ok(data_blocks.into_iter().collect())
    }
}
