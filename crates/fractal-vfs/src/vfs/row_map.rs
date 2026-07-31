//! `@ovr/` row-map plumbing: bulk snapshot loads keyed by `map_epoch`,
//! and the flush-side row writer. A block's row CAS is issued only
//! after that block's BSS body write is acknowledged; rows still
//! pipeline across blocks.

use std::sync::Arc;

use bytes::Bytes;
use data_types::ovr_map::{
    OvrRow, OvrRowMap, RowState, merge_row_for_write, ovr_row_key, ovr_row_prefix,
    parse_ovr_row_block,
};
use data_types::{DataBlobGuid, TraceId};
use futures::{StreamExt, stream};
use uuid::Uuid;

use crate::error::FsError;
use crate::vfs::VfsCore;
use data_types::object_layout::ObjectLayout;

/// Rows loaded per NSS listing page. Fixed-width block keys make one
/// page cover a contiguous block range; the has_more loop covers the
/// rest (the NSS clamp must never silently truncate a snapshot).
const ROW_LOAD_PAGE: u32 = 1000;
/// Concurrent row CASes per flush.
const ROW_WRITE_CONCURRENCY: usize = 16;
/// Bound on CAS retries for one row. A row that exhausts its budget
/// fails the flush before the commit CAS, never after.
const ROW_CAS_RETRIES: u32 = 16;

/// One row write a flush stages before its commit CAS.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RowWrite {
    pub(crate) block: u32,
    pub(crate) state: RowState,
}

/// What the row CAS displaced, handed to the sweep as exact victims.
#[derive(Debug, Default)]
pub(crate) struct RowWriteOutcome {
    /// Superseded `Written` identities: the committed generation this
    /// flush replaces, plus any orphan fragment an abandoned attempt
    /// left in the `cur` slot.
    pub(crate) victims: Vec<(u32, u64)>,
    /// The rows as stored after this flush's CAS, for the write-through
    /// into the cached snapshot on commit.
    pub(crate) rows: Vec<(u32, OvrRow)>,
}

impl VfsCore {
    /// The blob's committed row snapshot for a layout, or `None` for an
    /// unmapped blob (`map_epoch == 0`), which by definition has no
    /// committed row. Serves from the per-blob cache when the cached
    /// epoch matches; otherwise reloads the whole prefix.
    pub(crate) async fn layout_row_map(
        &self,
        layout: &ObjectLayout,
    ) -> Result<Option<Arc<OvrRowMap>>, FsError> {
        if !layout.is_mapped() {
            return Ok(None);
        }
        let blob_guid = layout.blob_guid().map_err(|_| FsError::InvalidState)?;
        Ok(Some(
            self.load_row_map(blob_guid, layout.map_epoch()).await?,
        ))
    }

    /// The row snapshot and committed ceiling for an optional layout:
    /// the pair every RMW / dirty-read / lazy-load path feeds into
    /// block resolution. `(None, 0)` for a brand-new file.
    pub(crate) async fn rows_and_ceiling(
        &self,
        layout: Option<&ObjectLayout>,
    ) -> Result<(Option<Arc<OvrRowMap>>, u64), FsError> {
        match layout {
            Some(layout) => Ok((self.layout_row_map(layout).await?, layout.blob_version)),
            None => Ok((None, 0)),
        }
    }

    /// Load (or serve cached) the row snapshot for `blob_guid` at
    /// `map_epoch`. Validity rule: a snapshot at epoch M serves any read
    /// whose layout carries the same M, regardless of ceiling movement,
    /// including the negative knowledge that a row was absent.
    pub(crate) async fn load_row_map(
        &self,
        blob_guid: DataBlobGuid,
        map_epoch: u64,
    ) -> Result<Arc<OvrRowMap>, FsError> {
        if let Some(cached) = self.row_maps.lock().get(&blob_guid.blob_id)
            && cached.epoch == map_epoch
        {
            return Ok(cached.clone());
        }
        let trace_id = TraceId::new();
        let map = Arc::new(
            load_row_snapshot(self.backend(), blob_guid.blob_id, map_epoch, &trace_id).await?,
        );
        self.install_row_map(blob_guid.blob_id, map.clone());
        Ok(map)
    }

    /// Install a snapshot (a fresh load, or the committing writer's
    /// write-through so it never reloads its own rows). The LRU evicts
    /// one cold entry at a time.
    pub(crate) fn install_row_map(&self, blob_id: Uuid, map: Arc<OvrRowMap>) {
        self.row_maps.lock().push(blob_id, map);
    }

    /// Stage this flush's rows: a monotone CAS that promotes the stored
    /// `cur` into `prev` only when that `cur` was committed at
    /// `base_ceiling`. Every row carries
    /// `cur_version = version > base_ceiling`, so nothing becomes
    /// visible before the commit CAS. Returns the exact identities the
    /// CASes displaced. Any error must fail the flush: a missing row
    /// after commit would hide acknowledged data.
    pub(crate) async fn write_rows_for_flush(
        &self,
        blob_guid: DataBlobGuid,
        writes: &[RowWrite],
        version: u64,
        base_ceiling: u64,
        base_map: Option<&OvrRowMap>,
        trace_id: &TraceId,
    ) -> Result<RowWriteOutcome, FsError> {
        let backend = self.backend();
        let results = stream::iter(writes.iter().copied())
            .map(|write| async move {
                let hint = base_map.and_then(|map| map.get(write.block)).copied();
                write_one_row(
                    backend,
                    blob_guid.blob_id,
                    write,
                    version,
                    base_ceiling,
                    hint,
                    trace_id,
                )
                .await
            })
            .buffer_unordered(ROW_WRITE_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        let mut outcome = RowWriteOutcome::default();
        for result in results {
            let (block, stored, victim) = result?;
            outcome.rows.push((block, stored));
            if let Some(identity) = victim {
                outcome.victims.push(identity);
            }
        }
        Ok(outcome)
    }
}

/// CAS-install one row. `hint` seeds the expected-old bytes from the
/// flush's base snapshot; a conflict refetches and recomputes, bounded
/// by `ROW_CAS_RETRIES`. Returns the stored row and the displaced
/// `Written` identity (if any).
async fn write_one_row(
    backend: &crate::backend::StorageBackend,
    blob_id: Uuid,
    write: RowWrite,
    version: u64,
    base_ceiling: u64,
    hint: Option<OvrRow>,
    trace_id: &TraceId,
) -> Result<(u32, OvrRow, Option<(u32, u64)>), FsError> {
    let key = ovr_row_key(&blob_id, write.block);
    let mut current: Option<OvrRow> = hint;
    for _ in 0..ROW_CAS_RETRIES {
        let Some(merged) =
            merge_row_for_write(current.as_ref(), write.state, version, base_ceiling)
        else {
            // The stored cur is already at or above our version: an
            // idempotent replay of this flush's own row. Nothing is
            // displaced that this attempt did not already report.
            let stored = current.expect("skip implies a stored row");
            return Ok((write.block, stored, None));
        };
        let expected_old = match current {
            Some(row) => Bytes::copy_from_slice(&row.encode()),
            None => Bytes::new(),
        };
        let new_bytes = Bytes::copy_from_slice(&merged.encode());
        match backend
            .put_inode_cas(&key, new_bytes, expected_old, trace_id)
            .await
        {
            Ok(_) => {
                // The displaced Written cur is an exact sweep victim: the
                // committed generation this flush supersedes (promoted
                // into prev until the commit lands), or an abandoned
                // attempt's orphan fragment (cur above the base ceiling).
                let victim = match current {
                    Some(OvrRow {
                        cur_state: RowState::Written,
                        cur_version,
                        ..
                    }) if cur_version != version => Some((write.block, cur_version)),
                    _ => None,
                };
                return Ok((write.block, merged, victim));
            }
            Err(FsError::CasConflict) => {
                // Self-heal from the stored bytes and retry. A conflict
                // with a verified-current snapshot means a foreign row
                // writer is live, which the doomed-preparer property
                // makes harmless: the monotone merge still applies.
                current = match backend.get_inode_raw(&key, trace_id).await {
                    Ok(bytes) => Some(OvrRow::decode(&bytes).ok_or_else(|| {
                        FsError::Internal(format!("malformed @ovr row at {key}"))
                    })?),
                    Err(FsError::NotFound) => None,
                    Err(error) => return Err(error),
                };
            }
            Err(error) => return Err(error),
        }
    }
    Err(FsError::Internal(format!(
        "row CAS budget exhausted for {key}"
    )))
}

/// Full-prefix snapshot load, paginated past the NSS clamp.
async fn load_row_snapshot(
    backend: &crate::backend::StorageBackend,
    blob_id: Uuid,
    map_epoch: u64,
    trace_id: &TraceId,
) -> Result<OvrRowMap, FsError> {
    let prefix = ovr_row_prefix(&blob_id);
    let mut map = OvrRowMap::new(map_epoch);
    let mut start_after = String::new();
    loop {
        let (page, has_more) = match backend
            .list_inodes_raw_page(&prefix, &start_after, ROW_LOAD_PAGE, trace_id)
            .await
        {
            Ok(page) => page,
            // No rows at all: legal for a freshly mapped blob whose
            // first row-writing commit only produced Hole rows that a
            // later teardown already removed, and for lagging caches.
            Err(FsError::NotFound) => return Ok(map),
            Err(error) => return Err(error),
        };
        let Some(last_key) = page.last().map(|(key, _)| key.clone()) else {
            return Ok(map);
        };
        for (key, value) in page {
            let Some(block) = parse_ovr_row_block(&key) else {
                return Err(FsError::Internal(format!("malformed @ovr row key {key}")));
            };
            let Some(row) = OvrRow::decode(&value) else {
                return Err(FsError::Internal(format!("malformed @ovr row at {key}")));
            };
            map.insert(block, row);
        }
        if !has_more {
            return Ok(map);
        }
        start_after = last_key;
    }
}
