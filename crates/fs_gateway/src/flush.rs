//! The three-step flush: the gateway-side owner of blob identity and
//! block generations. A client hands over its dirty blocks and size for
//! one inode key; everything below (minting the blob, burning the next
//! generation, the `@ovr/` rows, the commit CAS, reclamation) happens
//! here, so the client never names a blob or a version.
//!
//! The protocol over versioned write-once block keys and `@ovr/` rows:
//!
//! 1. Classify dirty blocks. First writes beyond the committed EOF are
//!    version-1 append territory: they take no row and no burned
//!    version, which is what keeps the map O(overwrites). Everything
//!    else (a row-covered block, a block below the committed EOF, or a
//!    block inside an interrupted append's `pending_append` range) lands
//!    at one freshly burned generation with a row.
//! 2. Prepare CAS (`BeginFlush`): durably burn the generation and record
//!    the version-1 append range before any data I/O. Once it wins,
//!    every skipped generation between the committed ceiling and this
//!    attempt is permanently rejected by an immutable abort-range record.
//! 3. Bodies (`WriteFlushBlock`), then rows (`CommitFlush`). A block's
//!    row CAS is issued only after every body is acknowledged, or a
//!    crash could leave a row naming a version with no data. Rows carry
//!    `cur_version` above the ceiling: invisible until commit.
//! 4. Commit CAS: the ceiling advances to the burned generation, every
//!    staged row becomes visible atomically, `pending_append` clears,
//!    and `map_epoch` bumps when rows or their interpretation changed.
//! 5. The superseded exact identities go to the background sweep, after
//!    a reader grace.
//!
//! The ticket returned by `BeginFlush` carries the prepared state signed
//! under the process secret, so each later step is self-contained: the
//! gateway keeps no per-flush memory, only caches.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use data_types::object_layout::{
    HARDLINK_PREFIX, InodeRecord, ObjectCoreMetaData, ObjectLayout, ObjectMetaData, ObjectState,
    PosixAttrs, layout_posix, posix_only_moved, same_committed_data,
};
use data_types::ovr_map::{
    BlockResolution, OVR_ABORT_VALUE, OvrRow, OvrRowMap, PrevSlot, RowState,
    merge_row_for_write_in_map, ovr_abort_key, ovr_row_key,
};
use data_types::{DataBlobGuid, TraceId};
use futures::{StreamExt, stream};
use prost::Message;
use rkyv::api::high::to_bytes_in;
use uuid::Uuid;

use crate::auth::{Auth, Session};
use crate::backend::{BackendConfig, StorageBackend};
use crate::error::FsError;
use crate::server::Gateway;
use crate::sweep::{SweepHint, enqueue_sweep};
use fs_gateway_codec::{
    AbortFlushRequest, BeginFlushRequest, CommitFlushRequest, FlushTicket, WriteFlushBlockRequest,
    begin_flush_response,
};

/// Concurrent row CASes per commit.
const ROW_WRITE_CONCURRENCY: usize = 16;
/// Bound on CAS retries for one row. A row that exhausts its budget
/// fails the flush before the commit CAS, never after.
const ROW_CAS_RETRIES: u32 = 16;
/// Bound on prepare/commit CAS retries that lose only to a posix
/// republish (a chmod or utimensat racing this flush); a pathological
/// utimensat storm still errors out.
const MAX_POSIX_REBASE_ATTEMPTS: u32 = 16;
/// A ticket older than this is refused; a flush never takes that long
/// and a stale ticket must not resurrect an abandoned attempt.
const TICKET_TTL: Duration = Duration::from_secs(3600);

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn encode_layout(layout: &ObjectLayout) -> Result<Bytes, FsError> {
    Ok(to_bytes_in::<_, rkyv::rancor::Error>(layout, Vec::new())
        .map_err(FsError::from)?
        .into())
}

/// Merge two inclusive block ranges into their covering span.
fn union_ranges(a: Option<(u32, u32)>, b: Option<(u32, u32)>) -> Option<(u32, u32)> {
    match (a, b) {
        (Some((alo, ahi)), Some((blo, bhi))) => Some((alo.min(blo), ahi.max(bhi))),
        (range @ Some(_), None) | (None, range @ Some(_)) => range,
        (None, None) => None,
    }
}

fn in_range(range: Option<(u32, u32)>, block: u32) -> bool {
    range.is_some_and(|(lo, hi)| lo <= block && block <= hi)
}

fn aborted_generation_range(
    base_ceiling: u64,
    version: u64,
) -> Result<Option<(u64, u64)>, FsError> {
    let first_skipped = base_ceiling
        .checked_add(1)
        .ok_or_else(|| FsError::Internal("block version exhausted".into()))?;
    Ok((version > first_skipped).then_some((first_skipped, version - 1)))
}

fn committed_map_epoch(
    base_epoch: u64,
    version: u64,
    rows_written: bool,
    aborted_range: Option<(u64, u64)>,
) -> u64 {
    if rows_written || aborted_range.is_some() {
        version
    } else {
        base_epoch
    }
}

/// Whether a row commits this block to an exact generation or a hole at
/// the current ceiling. Deliberately NOT "a row exists": a
/// straggler-staged row that resolves to Base must keep base-version
/// semantics everywhere (version-1 classification, trim probes, v1
/// sweep victims), or it shields the block's real state from the flush.
fn row_resolves(rows: Option<&OvrRowMap>, block: u32, ceiling: u64) -> bool {
    rows.is_some_and(|rows| {
        !matches!(
            rows.resolve(block, ceiling),
            BlockResolution::Base | BlockResolution::Stale
        )
    })
}

/// Committed geometry a flush classifies its blocks against.
#[derive(Clone, Copy)]
struct BaseGeometry {
    ceiling: u64,
    committed_block_count: u32,
    abandoned: Option<(u32, u32)>,
}

impl BaseGeometry {
    /// Version-1 append territory: a first write beyond the committed
    /// EOF, outside any interrupted append's range. A recorded range is
    /// contested at version 1, so every block inside it must be
    /// re-attempted at the burned generation.
    fn is_v1_append(&self, rows: Option<&OvrRowMap>, block: u32) -> bool {
        !row_resolves(rows, block, self.ceiling)
            && block >= self.committed_block_count
            && !in_range(self.abandoned, block)
    }

    fn v1_append_blocks(
        &self,
        rows: Option<&OvrRowMap>,
        rewrites: &BTreeSet<u32>,
    ) -> BTreeSet<u32> {
        rewrites
            .iter()
            .copied()
            .filter(|block| self.is_v1_append(rows, *block))
            .collect()
    }
}

fn span(blocks: &BTreeSet<u32>) -> Option<(u32, u32)> {
    match (blocks.first(), blocks.last()) {
        (Some(lo), Some(hi)) => Some((*lo, *hi)),
        _ => None,
    }
}

/// Verdict on a create publish whose CAS did not return success, derived
/// from a probe of the published key. Deleting the fresh blob requires
/// PROOF of non-publication: if the CAS landed (lost reply) the inode
/// references the blob and a whole-blob deletion would destroy
/// acknowledged data after the grace.
#[derive(Debug, PartialEq, Eq)]
enum CreatePublish {
    Landed,
    /// No proof either way: leak, never delete. Absence or another blob
    /// at the path is insufficient because a successful create may
    /// already have been renamed or hardlinked elsewhere.
    Ambiguous,
}

fn classify_create_publish(
    probe: &Result<Option<ObjectLayout>, FsError>,
    ours: DataBlobGuid,
) -> CreatePublish {
    if probe
        .as_ref()
        .ok()
        .and_then(|current| current.as_ref())
        .and_then(|current| current.blob_guid().ok())
        == Some(ours)
    {
        CreatePublish::Landed
    } else {
        CreatePublish::Ambiguous
    }
}

/// What is stored at a publish key: the layout, its raw bytes (the CAS
/// guard), and the record fields when the key is a `@hardlink/` record.
struct Stored {
    layout: ObjectLayout,
    raw: Bytes,
    record: Option<(u32, Option<u64>)>,
}

fn is_record_key(key: &str) -> bool {
    key.starts_with(HARDLINK_PREFIX)
}

async fn load_stored(
    backend: &StorageBackend,
    key: &str,
    trace_id: &TraceId,
) -> Result<Option<Stored>, FsError> {
    let raw = match backend.get_inode_raw(key, trace_id).await {
        Ok(raw) => raw,
        Err(FsError::NotFound) => return Ok(None),
        Err(error) => return Err(error),
    };
    if is_record_key(key) {
        let record = rkyv::from_bytes::<InodeRecord, rkyv::rancor::Error>(&raw)?;
        return Ok(Some(Stored {
            layout: record.layout,
            raw,
            record: Some((record.nlink, record.orphan_since)),
        }));
    }
    let layout = rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&raw)?;
    if matches!(layout.state, ObjectState::Indirect(_)) {
        // A promoted inode publishes through its record, never its name.
        return Err(FsError::InvalidState);
    }
    Ok(Some(Stored {
        layout,
        raw,
        record: None,
    }))
}

/// Serialize `layout` as the publish value for a key: bare, or wrapped
/// in the shared record with the current nlink and orphan stamp.
fn wrap_for_publish(
    record: Option<(u32, Option<u64>)>,
    layout: &ObjectLayout,
) -> Result<Bytes, FsError> {
    match record {
        Some((nlink, orphan_since)) => {
            let record = InodeRecord {
                layout: layout.clone(),
                nlink,
                orphan_since,
            };
            Ok(to_bytes_in::<_, rkyv::rancor::Error>(&record, Vec::new())
                .map_err(FsError::from)?
                .into())
        }
        None => encode_layout(layout),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_layout(
    blob_guid: DataBlobGuid,
    blob_version: u64,
    next_version: u64,
    pending_append: Option<(u32, u32)>,
    map_epoch: u64,
    file_size: u64,
    block_size: u32,
    posix: PosixAttrs,
) -> ObjectLayout {
    let mut layout = ObjectLayout {
        version_id: ObjectLayout::gen_version_id(),
        block_size,
        timestamp: unix_ms(),
        blob_version,
        fs_ext: ObjectLayout::fs_ext_from(Some(posix)),
        state: ObjectState::Normal(ObjectMetaData {
            blob_guid,
            core_meta_data: ObjectCoreMetaData {
                size: file_size,
                etag: blob_guid.blob_id.simple().to_string(),
                headers: vec![],
                checksum: None,
            },
        }),
    };
    layout.set_next_version(next_version);
    layout.set_pending_append(pending_append);
    layout.set_map_epoch(map_epoch);
    layout
}

// ---------- ticket ----------

fn issue_ticket(auth: &Auth, ticket: &FlushTicket) -> Bytes {
    let mut buf = BytesMut::with_capacity(ticket.encoded_len());
    ticket
        .encode(&mut buf)
        .expect("BytesMut encode is infallible");
    auth.sign(&buf)
}

fn open_ticket(auth: &Auth, session: &Session, signed: &[u8]) -> Result<FlushTicket, FsError> {
    let payload = auth.verify_signed(signed, "flush ticket")?;
    let ticket = FlushTicket::decode(payload)
        .map_err(|e| FsError::Unauthorized(format!("malformed flush ticket: {e}")))?;
    if ticket.bucket != session.bucket {
        return Err(FsError::Unauthorized(
            "flush ticket for another bucket".into(),
        ));
    }
    // The `BeginFlush` scope check, carried forward through the ticket.
    crate::server::check_key(session, &ticket.key)?;
    if unix_ms().saturating_sub(ticket.issued_ms) > TICKET_TTL.as_millis() as u64 {
        return Err(FsError::Unauthorized("flush ticket expired".into()));
    }
    if ticket.block_size != ObjectLayout::DEFAULT_BLOCK_SIZE || ticket.version == 0 {
        return Err(FsError::Unauthorized("malformed flush ticket".into()));
    }
    Ok(ticket)
}

fn ticket_blob(ticket: &FlushTicket) -> Result<DataBlobGuid, FsError> {
    let blob_id = Uuid::from_slice(&ticket.blob_id)
        .map_err(|e| FsError::Internal(format!("ticket blob id: {e}")))?;
    let volume_id = u16::try_from(ticket.volume_id)
        .map_err(|_| FsError::Internal("ticket volume id out of range".into()))?;
    Ok(DataBlobGuid { blob_id, volume_id })
}

fn ticket_geometry(ticket: &FlushTicket) -> BaseGeometry {
    BaseGeometry {
        ceiling: ticket.base_ceiling,
        committed_block_count: ticket.committed_size.div_ceil(ticket.block_size as u64) as u32,
        abandoned: match (ticket.abandoned_lo, ticket.abandoned_hi) {
            (Some(lo), Some(hi)) => Some((lo, hi)),
            _ => None,
        },
    }
}

async fn ticket_rows(
    backend: &StorageBackend,
    ticket: &FlushTicket,
    blob_guid: DataBlobGuid,
    trace_id: &TraceId,
) -> Result<Option<Arc<OvrRowMap>>, FsError> {
    if ticket.base_epoch == 0 {
        return Ok(None);
    }
    Ok(Some(
        backend
            .row_map_at(blob_guid.blob_id, ticket.base_epoch, trace_id)
            .await?,
    ))
}

// ---------- rows ----------

/// One row write a commit stages before its CAS.
#[derive(Debug, Clone, Copy)]
struct RowWrite {
    block: u32,
    state: RowState,
}

/// What the row CAS displaced, handed to the sweep as exact victims.
#[derive(Debug, Default)]
struct RowWriteOutcome {
    /// Superseded `Written` identities: the committed fallback this
    /// flush replaces, plus any orphan fragment an abandoned attempt
    /// left in `cur`.
    victims: Vec<(u32, u64)>,
    /// The rows as stored after this flush's CAS, for the write-through
    /// into the cached snapshot on commit.
    rows: Vec<(u32, OvrRow)>,
}

/// Stage this flush's rows: a monotone CAS that promotes the stored
/// `cur` into `prev` only when that `cur` was committed at
/// `base_ceiling`. Every row carries `cur_version = version >
/// base_ceiling`, so nothing becomes visible before the commit CAS. Any
/// error must fail the flush: a missing row after commit would hide
/// acknowledged data.
async fn write_rows_for_flush(
    backend: &StorageBackend,
    blob_id: Uuid,
    writes: &[RowWrite],
    version: u64,
    base_ceiling: u64,
    base_map: Option<&OvrRowMap>,
    trace_id: &TraceId,
) -> Result<RowWriteOutcome, FsError> {
    let results = stream::iter(writes.iter().copied())
        .map(|write| async move {
            write_one_row(
                backend,
                blob_id,
                write,
                version,
                base_ceiling,
                base_map,
                trace_id,
            )
            .await
        })
        .buffer_unordered(ROW_WRITE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;

    let mut outcome = RowWriteOutcome::default();
    for result in results {
        let (block, stored, victims) = result?;
        outcome.rows.push((block, stored));
        outcome.victims.extend(victims);
    }
    Ok(outcome)
}

/// CAS-install one row. The flush's base snapshot seeds the expected-old
/// bytes; a conflict refetches and recomputes, bounded by
/// `ROW_CAS_RETRIES`. Returns the stored row and the displaced `Written`
/// identities.
async fn write_one_row(
    backend: &StorageBackend,
    blob_id: Uuid,
    write: RowWrite,
    version: u64,
    base_ceiling: u64,
    base_map: Option<&OvrRowMap>,
    trace_id: &TraceId,
) -> Result<(u32, OvrRow, Vec<(u32, u64)>), FsError> {
    let key = ovr_row_key(&blob_id, write.block);
    let mut current = base_map.and_then(|map| map.get(write.block)).copied();
    for _ in 0..ROW_CAS_RETRIES {
        let Some(merged) = merge_row_for_write_in_map(
            current.as_ref(),
            write.state,
            version,
            base_ceiling,
            base_map,
        ) else {
            // The stored cur is already at or above our version: an
            // idempotent replay of this flush's own row. Nothing is
            // displaced that this attempt did not already report.
            let stored = current.expect("skip implies a stored row");
            return Ok((write.block, stored, Vec::new()));
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
                let mut victims = Vec::new();
                if let Some(identity) = match current {
                    Some(OvrRow {
                        cur_state: RowState::Written,
                        cur_version,
                        ..
                    }) if cur_version != version => Some((write.block, cur_version)),
                    _ => None,
                } {
                    victims.push(identity);
                }
                if let PrevSlot::Slot(RowState::Written, prev_version) = merged.prev {
                    victims.push((write.block, prev_version));
                }
                victims.sort_unstable();
                victims.dedup();
                return Ok((write.block, merged, victims));
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

/// Persist proof that every generation in `[lo, hi]` was doomed by this
/// flush's successful prepare CAS. The immutable record lands before any
/// commit can advance the ceiling across the range, so a delayed row CAS
/// from an older attempt remains permanently non-readable. A lost
/// successful reply is recovered by comparing the deterministic value.
async fn record_aborted_versions(
    backend: &StorageBackend,
    blob_id: Uuid,
    lo: u64,
    hi: u64,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    if lo == 0 || lo > hi {
        return Err(FsError::Internal(format!(
            "invalid aborted generation range {lo}..={hi}"
        )));
    }
    let key = ovr_abort_key(&blob_id, lo, hi);
    let value = Bytes::from_static(&OVR_ABORT_VALUE);
    match backend
        .put_inode_cas(&key, value.clone(), Bytes::new(), trace_id)
        .await
    {
        Ok(_) => Ok(()),
        Err(error) => match backend.get_inode_raw(&key, trace_id).await {
            Ok(stored) if stored.as_ref() == OVR_ABORT_VALUE => Ok(()),
            Ok(_) => Err(FsError::Internal(format!(
                "malformed aborted generation record at {key}"
            ))),
            Err(FsError::NotFound) => Err(error),
            Err(probe_error) => {
                tracing::warn!(%key, %probe_error, "abort record write outcome is ambiguous");
                Err(error)
            }
        },
    }
}

/// Whether the CAS bytes `wanted` are what the store currently holds (a
/// lost-reply idempotency probe after a failed CAS).
async fn publish_landed(
    backend: &StorageBackend,
    key: &str,
    wanted: &Bytes,
    trace_id: &TraceId,
) -> bool {
    matches!(backend.get_inode_raw(key, trace_id).await, Ok(current) if current == *wanted)
}

// ---------- steps ----------

/// `BeginFlush`: mint the blob for a create, or prepare the next
/// generation against the stored layout and hand back a ticket.
pub async fn begin(
    gw: &Gateway,
    backend: &StorageBackend,
    session: &Session,
    req: BeginFlushRequest,
    trace_id: &TraceId,
) -> Result<begin_flush_response::Prepared, FsError> {
    let block_size = ObjectLayout::DEFAULT_BLOCK_SIZE;
    if req.file_size.div_ceil(block_size as u64) > u32::MAX as u64 {
        return Err(FsError::InvalidState);
    }
    let stored = load_stored(backend, &req.key, trace_id).await?;

    if req.expected_layout.is_empty() {
        // Create: no committed base. A fresh blob is minted per attempt,
        // so no key this attempt writes can ever collide with another
        // attempt's bytes: everything lands at version 1, unpadded, with
        // no rows. The commit requires the key to still be absent.
        if stored.is_some() {
            return Err(FsError::CasConflict);
        }
        if is_record_key(&req.key) {
            return Err(FsError::InvalidState);
        }
        let blob_guid = backend.create_blob_guid();
        let ticket = FlushTicket {
            bucket: session.bucket.clone(),
            key: req.key,
            blob_id: Bytes::copy_from_slice(blob_guid.blob_id.as_bytes()),
            volume_id: blob_guid.volume_id as u32,
            create: true,
            version: 1,
            base_ceiling: 0,
            base_epoch: 0,
            committed_size: 0,
            file_size: req.file_size,
            block_size,
            abandoned_lo: None,
            abandoned_hi: None,
            eof_low_watermark: None,
            trim_upper: None,
            issued_ms: unix_ms(),
        };
        return Ok(begin_flush_response::Prepared {
            ticket: issue_ticket(&gw.auth, &ticket),
            layout: Bytes::new(),
        });
    }

    let expected = rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&req.expected_layout)?;
    // The key vanished, or another writer changed the committed data the
    // client buffered its edits against: a hard conflict either way.
    let Some(mut current) = stored else {
        return Err(FsError::CasConflict);
    };
    if !same_committed_data(&expected, &current.layout) {
        return Err(FsError::CasConflict);
    }
    if current.layout.block_size != block_size {
        return Err(FsError::InvalidState);
    }
    let rewrites: BTreeSet<u32> = req.rewrites.iter().copied().collect();

    let mut rebase_attempts = 0u32;
    let (prepare, version, geometry, base_epoch, blob_guid) = loop {
        let base = current.layout.clone();
        let blob_guid = base.blob_guid()?;
        let geometry = BaseGeometry {
            ceiling: base.blob_version,
            committed_block_count: base.size()?.div_ceil(block_size as u64) as u32,
            abandoned: base.pending_append(),
        };
        let version = base.next_burn_version();
        let base_rows = backend.row_map_for(&base, trace_id).await?;
        // The prepared record carries the union of the abandoned range
        // and this attempt's own version-1 span: if this attempt also
        // dies, the next one sees the whole contested territory. The
        // commit clears it, having resolved every block in the union.
        let v1_span = span(&geometry.v1_append_blocks(base_rows.as_deref(), &rewrites));
        let pending_union = union_ranges(geometry.abandoned, v1_span);

        // Prepare CAS. Durably burns `version` and records the version-1
        // territory before any data I/O. `blob_version` stays at the
        // reader-visible ceiling until commit. The doomed-preparer
        // property starts here: any older prepared flush can no longer
        // commit.
        let mut prepare = base.clone();
        prepare.set_next_version(version + 1);
        prepare.set_pending_append(pending_union);
        let new_bytes = wrap_for_publish(current.record, &prepare)?;
        match backend
            .put_inode_cas(&req.key, new_bytes.clone(), current.raw.clone(), trace_id)
            .await
        {
            Ok(_) => break (prepare, version, geometry, base.map_epoch(), blob_guid),
            Err(error) => {
                if publish_landed(backend, &req.key, &new_bytes, trace_id).await {
                    break (prepare, version, geometry, base.map_epoch(), blob_guid);
                }
                // A chmod or utimensat republished the row under us: rebase
                // over it. Any other divergence is a foreign writer.
                let fresh = load_stored(backend, &req.key, trace_id).await?;
                match fresh {
                    Some(fresh)
                        if rebase_attempts < MAX_POSIX_REBASE_ATTEMPTS
                            && posix_only_moved(&base, &fresh.layout) =>
                    {
                        rebase_attempts += 1;
                        current = fresh;
                    }
                    _ => return Err(error),
                }
            }
        }
    };

    // The successful prepare permanently doomed every skipped generation.
    // Publish that fact before any commit can advance the ceiling across
    // the gap. Delayed row CASes may still land, but every reader rejects
    // their versions.
    if let Some((lo, hi)) = aborted_generation_range(geometry.ceiling, version)? {
        record_aborted_versions(backend, blob_guid.blob_id, lo, hi, trace_id).await?;
    }
    backend.cache_layout(&req.key, prepare.clone());

    let committed_size = prepare.size()?;
    let ticket = FlushTicket {
        bucket: session.bucket.clone(),
        key: req.key,
        blob_id: Bytes::copy_from_slice(blob_guid.blob_id.as_bytes()),
        volume_id: blob_guid.volume_id as u32,
        create: false,
        version,
        base_ceiling: geometry.ceiling,
        base_epoch,
        committed_size,
        file_size: req.file_size,
        block_size,
        abandoned_lo: geometry.abandoned.map(|(lo, _)| lo),
        abandoned_hi: geometry.abandoned.map(|(_, hi)| hi),
        eof_low_watermark: req.eof_low_watermark,
        trim_upper: req.trim_upper,
        issued_ms: unix_ms(),
    };
    Ok(begin_flush_response::Prepared {
        ticket: issue_ticket(&gw.auth, &ticket),
        layout: encode_layout(&prepare)?,
    })
}

/// `WriteFlushBlock`: one body at the generation the ticket's
/// classification assigns it. Burned generations are padded to a full
/// block (constant EC shard size); version-1 bodies keep their length.
pub async fn write_block(
    gw: &Gateway,
    backend: &StorageBackend,
    session: &Session,
    req: WriteFlushBlockRequest,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    let ticket = open_ticket(&gw.auth, session, &req.ticket)?;
    let block_size = ticket.block_size as usize;
    // BSS rejects anything above one shard; enforce it here so a bad
    // client cannot reach that check on a shared node.
    if req.data.len() > block_size {
        return Err(FsError::InvalidState);
    }
    let blob_guid = ticket_blob(&ticket)?;
    let write_version = if ticket.create {
        1
    } else {
        let rows = ticket_rows(backend, &ticket, blob_guid, trace_id).await?;
        if ticket_geometry(&ticket).is_v1_append(rows.as_deref(), req.block_number) {
            1
        } else {
            ticket.version
        }
    };
    let body = if write_version > 1 && req.data.len() < block_size {
        let mut buf = BytesMut::with_capacity(block_size);
        buf.extend_from_slice(&req.data);
        buf.resize(block_size, 0);
        buf.freeze()
    } else {
        req.data
    };
    let stored = backend
        .write_block(
            blob_guid,
            req.block_number,
            body.clone(),
            write_version,
            trace_id,
        )
        .await?;
    // A write-once key that already existed kept its original bytes;
    // mirroring the caller's copy would poison the disk cache.
    if stored {
        gw.mirror_insert(blob_guid, req.block_number, write_version, body);
    }
    Ok(())
}

/// `CommitFlush`: stage the rows, advance the ceiling, hand the
/// superseded generations to the sweep. Returns the committed layout.
pub async fn commit(
    gw: &Gateway,
    cfg: &Arc<BackendConfig>,
    backend: &StorageBackend,
    session: &Session,
    req: CommitFlushRequest,
    trace_id: &TraceId,
) -> Result<Bytes, FsError> {
    let ticket = open_ticket(&gw.auth, session, &req.ticket)?;
    let blob_guid = ticket_blob(&ticket)?;
    let block_size = ticket.block_size;
    let bsz_u64 = block_size as u64;
    let file_size = ticket.file_size;
    let rewrites: BTreeSet<u32> = req.rewrites.iter().copied().collect();
    let punched: BTreeSet<u32> = req.punched.iter().copied().collect();
    let key = ticket.key.as_str();

    if ticket.create {
        let posix = req.posix.as_ref().map(PosixAttrs::from).unwrap_or_default();
        let layout = build_layout(blob_guid, 1, 0, None, 0, file_size, block_size, posix);
        let bytes = encode_layout(&layout)?;
        if let Err(error) = backend
            .put_inode_cas(key, bytes.clone(), Bytes::new(), trace_id)
            .await
        {
            // The CAS may have landed with a lost reply (an internal
            // retry then sees our own row and reports CasConflict), and
            // a transport error may or may not have been applied. Probe
            // by blob IDENTITY, not bytes: a concurrent posix republish
            // already changes the bytes of a landed publish. Never
            // delete on ambiguity: a truly unpublished blob leaks
            // instead, which scrub reconciles.
            let probe = backend.layout_at(key, trace_id).await;
            if classify_create_publish(&probe, blob_guid) != CreatePublish::Landed {
                return Err(error);
            }
        }
        backend.cache_layout(key, layout);
        return Ok(bytes);
    }

    // Overwrite: the key must still hold this flush's prepare. A later
    // preparer doomed this attempt; its bodies are invisible without
    // rows and its burned generation is recorded aborted by the winner.
    let Some(mut current) = load_stored(backend, key, trace_id).await? else {
        return Err(FsError::CasConflict);
    };
    if current.layout.blob_guid().ok() != Some(blob_guid) {
        return Err(FsError::CasConflict);
    }
    // A retried commit whose first attempt landed with a lost reply.
    if current.layout.blob_version == ticket.version {
        return encode_layout(&current.layout);
    }
    if current.layout.blob_version != ticket.base_ceiling
        || current.layout.next_burn_version() != ticket.version + 1
    {
        return Err(FsError::CasConflict);
    }
    let geometry = ticket_geometry(&ticket);
    let version = ticket.version;
    let base_ceiling = geometry.ceiling;
    let committed_bc = geometry.committed_block_count;
    let abandoned = geometry.abandoned;
    let new_num_blocks = file_size.div_ceil(bsz_u64) as u32;
    // Hold the exact base snapshot through commit so LRU eviction by
    // another reader cannot make write-through construct a partial map.
    let base_rows = ticket_rows(backend, &ticket, blob_guid, trace_id).await?;
    let base_rows_ref = base_rows.as_deref();
    let resolves = |block: u32| row_resolves(base_rows_ref, block, base_ceiling);
    let dirty = |block: u32| rewrites.contains(&block) || punched.contains(&block);

    let v1_append_blocks = geometry.v1_append_blocks(base_rows_ref, &rewrites);
    // Rewrites at the burned generation: everything dirty that is not
    // version-1 territory.
    let burned_rewrites: BTreeSet<u32> = rewrites
        .iter()
        .copied()
        .filter(|block| !v1_append_blocks.contains(block))
        .collect();

    let mut sweep_victims: Vec<(u32, u64)> = Vec::new();
    let mut sweep_below: Vec<(u32, u64)> = Vec::new();

    // Trim range: blocks logically destroyed by a shrink must read zeros
    // even while their superseded generations await the sweep, and must
    // never resurrect on a regrow, so they get Hole rows. One bounded
    // listing finds which unmapped blocks actually hold physical keys,
    // so a sparse trim rows nothing.
    let trim_lo = std::cmp::min(
        new_num_blocks,
        ticket.eof_low_watermark.unwrap_or(new_num_blocks),
    );
    let trim_hi = committed_bc.max(ticket.trim_upper.unwrap_or(0));
    let mut trim_hole_blocks: BTreeSet<u32> = BTreeSet::new();
    if trim_lo < trim_hi {
        if let Some(rows) = base_rows_ref {
            for (block, row) in rows.range(trim_lo, trim_hi) {
                if dirty(block) {
                    continue;
                }
                // Classify by the committed RESOLUTION, not the raw cur: a
                // straggler-staged cur above the ceiling would otherwise
                // shield its committed prev (and the block's v1 body, via
                // the probe skip below) from the Hole conversion.
                match rows.resolve(block, base_ceiling) {
                    BlockResolution::Exact { version } => {
                        trim_hole_blocks.insert(block);
                        sweep_victims.push((block, version));
                    }
                    // A committed Hole needs no new row.
                    BlockResolution::Hole => {}
                    // Base-resolving row: the ROW must still be superseded
                    // by a Hole; the probe below finds any v1 body.
                    BlockResolution::Base | BlockResolution::Stale => {
                        trim_hole_blocks.insert(block);
                    }
                }
                if row.cur_version > base_ceiling && row.cur_state == RowState::Written {
                    // Straggler orphan body at the staged generation. This
                    // trim's row CAS supersedes its metadata; the abort
                    // record keeps any still-delayed copy non-readable.
                    sweep_victims.push((block, row.cur_version));
                }
            }
        }
        let entries = backend
            .list_blob_blocks(blob_guid, trim_lo, trim_hi - trim_lo, trace_id)
            .await?;
        for entry in entries {
            let block = entry.block_number;
            if dirty(block) || resolves(block) {
                continue;
            }
            if entry.version == 1 {
                // Committed (or claimed) base-version content: must become
                // a Hole row or a regrow resurrects it.
                trim_hole_blocks.insert(block);
            }
            // Every listed generation of an unmapped trimmed block is
            // garbage once the shrink commits; orphans above the ceiling
            // included.
            if entry.version != version {
                sweep_victims.push((block, entry.version));
            }
        }
    }

    // The abandoned range's unresolved remainder becomes Hole rows at
    // `version`, so the contested version-1 fragments can never surface
    // where zeros are required (a later regrow included).
    let mut remainder_holes: BTreeSet<u32> = BTreeSet::new();
    if let Some((lo, hi)) = abandoned {
        for block in lo..=hi {
            if !burned_rewrites.contains(&block) && !punched.contains(&block) {
                remainder_holes.insert(block);
            }
        }
    }

    // Rows, issued only now that every body this flush names is
    // acknowledged. All staged rows carry cur_version = `version` above
    // the ceiling: invisible until the commit, all-or-nothing at commit.
    let mut row_writes: Vec<RowWrite> = burned_rewrites
        .iter()
        .map(|block| RowWrite {
            block: *block,
            state: RowState::Written,
        })
        .collect();
    row_writes.extend(
        punched
            .iter()
            .chain(trim_hole_blocks.iter())
            .chain(remainder_holes.iter())
            .map(|block| RowWrite {
                block: *block,
                state: RowState::Hole,
            }),
    );
    row_writes.sort_by_key(|write| write.block);
    row_writes.dedup_by_key(|write| write.block);
    let rows_written = !row_writes.is_empty();
    let mut committed_rows: Vec<(u32, OvrRow)> = Vec::new();
    if rows_written {
        let outcome = write_rows_for_flush(
            backend,
            blob_guid.blob_id,
            &row_writes,
            version,
            base_ceiling,
            base_rows_ref,
            trace_id,
        )
        .await?;
        sweep_victims.extend(outcome.victims);
        committed_rows = outcome.rows;
    }
    // Superseded base-version identities of unmapped territory: a burned
    // rewrite or punch below the committed EOF replaces whatever sat at
    // version 1 (data or a claim; a miss is an idempotent no-op).
    for block in burned_rewrites.iter().chain(punched.iter()) {
        if !resolves(*block) && *block < committed_bc {
            sweep_victims.push((*block, 1));
        }
        if in_range(abandoned, *block) {
            // Re-attempted at the burned version instead of v1, so the
            // contested version-1 fragment this block may carry is
            // abandoned; reclaim it.
            sweep_victims.push((*block, 1));
        }
    }
    // A burned rewrite over a Hole row may be superseding a claim at an
    // unknown burned generation (a filled fallocate over a punched
    // block): resolve those via one listing in the sweep.
    for block in burned_rewrites.iter() {
        if matches!(
            base_rows_ref.map(|rows| rows.resolve(*block, base_ceiling)),
            Some(BlockResolution::Hole)
        ) {
            sweep_below.push((*block, version));
        }
    }

    // Commit CAS. The ceiling advances to `version`, every staged row
    // becomes visible together, the version-1 territory is resolved
    // (pending_append clears), and map_epoch bumps when this commit
    // wrote a row or changed row interpretation by crossing an aborted
    // generation. A chmod or utimensat can win after prepare: the
    // candidate is rebuilt from the current guard on every retry so the
    // data commit never restores stale POSIX metadata.
    let aborted_range = aborted_generation_range(base_ceiling, version)?;
    let map_epoch = committed_map_epoch(ticket.base_epoch, version, rows_written, aborted_range);
    let mut rebase_attempts = 0u32;
    let final_layout = loop {
        let candidate = build_layout(
            blob_guid,
            version,
            version + 1,
            None,
            map_epoch,
            file_size,
            block_size,
            layout_posix(&current.layout),
        );
        let new_bytes = wrap_for_publish(current.record, &candidate)?;
        let Err(error) = backend
            .put_inode_cas(key, new_bytes.clone(), current.raw.clone(), trace_id)
            .await
        else {
            break candidate;
        };
        if publish_landed(backend, key, &new_bytes, trace_id).await {
            break candidate;
        }
        let fresh = load_stored(backend, key, trace_id).await?;
        match fresh {
            Some(fresh)
                if rebase_attempts < MAX_POSIX_REBASE_ATTEMPTS
                    && posix_only_moved(&current.layout, &fresh.layout) =>
            {
                rebase_attempts += 1;
                current = fresh;
            }
            _ => return Err(error),
        }
    };

    // Write-through the committed rows into the cached snapshot so this
    // gateway never reloads rows it just wrote: clone the base snapshot,
    // overlay the stored rows, retag at the new epoch.
    if !committed_rows.is_empty() || aborted_range.is_some() {
        let mut fresh = base_rows_ref.cloned().unwrap_or_default();
        fresh.epoch = final_layout.map_epoch();
        if let Some((lo, hi)) = aborted_range {
            fresh.add_aborted_range(lo, hi);
        }
        for (block, row) in committed_rows {
            fresh.insert(block, row);
        }
        backend.install_row_map(blob_guid.blob_id, Arc::new(fresh));
    }
    backend.cache_layout(key, final_layout.clone());

    // Reclaim what this commit superseded, after the reader grace. The
    // sweep re-derives reclaimability from the bucket's rows and layout
    // before it deletes anything.
    if !sweep_victims.is_empty() || !sweep_below.is_empty() {
        enqueue_sweep(
            &gw.sweep,
            cfg.clone(),
            blob_guid,
            SweepHint {
                key: key.to_string(),
                victims: sweep_victims,
                below: sweep_below,
                with_grace: true,
                ..Default::default()
            },
        );
    }
    encode_layout(&final_layout)
}

/// `AbortFlush`: the client gave up after `BeginFlush`. A create's fresh
/// blob is unreachable and is torn down after the grace (the sweep
/// confirms the key does not publish it). An overwrite's burned
/// generation needs nothing: its bodies are invisible without rows and
/// the next preparer records it aborted.
pub async fn abort(
    gw: &Gateway,
    cfg: &Arc<BackendConfig>,
    session: &Session,
    req: AbortFlushRequest,
) -> Result<(), FsError> {
    let ticket = open_ticket(&gw.auth, session, &req.ticket)?;
    if !ticket.create {
        return Ok(());
    }
    let blob_guid = ticket_blob(&ticket)?;
    enqueue_sweep(
        &gw.sweep,
        cfg.clone(),
        blob_guid,
        SweepHint {
            key: ticket.key,
            delete_all_blocks: true,
            with_grace: true,
            ..Default::default()
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::object_layout::IndirectEntry;

    #[test]
    fn pending_append_union_spans() {
        assert_eq!(union_ranges(None, None), None);
        assert_eq!(union_ranges(Some((3, 5)), None), Some((3, 5)));
        assert_eq!(union_ranges(None, Some((7, 9))), Some((7, 9)));
        assert_eq!(union_ranges(Some((3, 5)), Some((7, 9))), Some((3, 9)));
        assert_eq!(union_ranges(Some((7, 9)), Some((3, 5))), Some((3, 9)));
        assert!(in_range(Some((3, 5)), 3));
        assert!(in_range(Some((3, 5)), 5));
        assert!(!in_range(Some((3, 5)), 6));
        assert!(!in_range(None, 0));
    }

    #[test]
    fn row_free_recovery_bumps_map_epoch() {
        // Version 2 was prepared and abandoned. A row-free version-3
        // commit must publish the abort interpretation through epoch 3.
        let aborted = aborted_generation_range(1, 3).expect("range");
        assert_eq!(aborted, Some((2, 2)));
        assert_eq!(committed_map_epoch(0, 3, false, aborted), 3);

        // The ordinary consecutive row-free commit changes no row
        // interpretation and retains the existing cache epoch.
        let consecutive = aborted_generation_range(3, 4).expect("range");
        assert_eq!(consecutive, None);
        assert_eq!(committed_map_epoch(3, 4, false, consecutive), 3);
        assert_eq!(committed_map_epoch(3, 4, true, consecutive), 4);
    }

    fn guid(n: u128) -> DataBlobGuid {
        DataBlobGuid {
            blob_id: Uuid::from_u128(n),
            volume_id: 1,
        }
    }

    #[test]
    fn only_identity_match_proves_create_landed() {
        let ours = guid(7);
        let landed = build_layout(
            ours,
            1,
            0,
            None,
            0,
            0,
            ObjectLayout::DEFAULT_BLOCK_SIZE,
            PosixAttrs::default(),
        );
        assert_eq!(
            classify_create_publish(&Ok(Some(landed)), ours),
            CreatePublish::Landed
        );
        let other = build_layout(
            guid(8),
            1,
            0,
            None,
            0,
            0,
            ObjectLayout::DEFAULT_BLOCK_SIZE,
            PosixAttrs::default(),
        );
        assert_eq!(
            classify_create_publish(&Ok(Some(other)), ours),
            CreatePublish::Ambiguous
        );
        assert_eq!(
            classify_create_publish(&Ok(None), ours),
            CreatePublish::Ambiguous
        );
        assert_eq!(
            classify_create_publish(&Err(FsError::CasConflict), ours),
            CreatePublish::Ambiguous
        );
        let mut indirect = build_layout(
            ours,
            1,
            0,
            None,
            0,
            0,
            ObjectLayout::DEFAULT_BLOCK_SIZE,
            PosixAttrs::default(),
        );
        indirect.state = ObjectState::Indirect(IndirectEntry {
            inode_id: Uuid::from_u128(3),
        });
        assert_eq!(
            classify_create_publish(&Ok(Some(indirect)), ours),
            CreatePublish::Ambiguous
        );
    }

    #[test]
    fn v1_territory_excludes_committed_and_abandoned_blocks() {
        let geometry = BaseGeometry {
            ceiling: 4,
            committed_block_count: 3,
            abandoned: Some((5, 6)),
        };
        let mut rows = OvrRowMap::new(4);
        rows.insert(
            7,
            OvrRow {
                cur_state: RowState::Written,
                cur_version: 3,
                prev: PrevSlot::Base,
            },
        );
        let rewrites: BTreeSet<u32> = [1, 3, 4, 5, 7, 9].into_iter().collect();
        let v1 = geometry.v1_append_blocks(Some(&rows), &rewrites);
        assert_eq!(v1, [3, 4, 9].into_iter().collect::<BTreeSet<u32>>());
        assert_eq!(span(&v1), Some((3, 9)));
        assert_eq!(span(&BTreeSet::new()), None);
    }

    #[test]
    fn same_data_but_moved_posix_is_not_a_conflict() {
        let ours = guid(9);
        let base = build_layout(
            ours,
            4,
            5,
            None,
            4,
            4096,
            ObjectLayout::DEFAULT_BLOCK_SIZE,
            PosixAttrs::default(),
        );
        let mut chmodded = base.clone();
        chmodded.set_fs_posix(Some(PosixAttrs {
            mode: 0o100600,
            ..PosixAttrs::default()
        }));
        assert!(posix_only_moved(&base, &chmodded));
        assert!(same_committed_data(&base, &chmodded));
        let mut prepared = base.clone();
        prepared.set_next_version(7);
        prepared.set_pending_append(Some((2, 3)));
        assert!(!posix_only_moved(&base, &prepared));
        assert!(same_committed_data(&base, &prepared));
        let mut committed = base.clone();
        committed.blob_version = 5;
        assert!(!same_committed_data(&base, &committed));
    }
}
