//! NSS publish primitives and the long-running writeback worker that
//! drives them. The `publish_*` helpers are shared: the worker calls them
//! for queued intents, and the metadata paths in `vfs::attr` call the same
//! functions inline when a publish must land synchronously.

use bytes::Bytes;
use data_types::TraceId;
use rkyv::api::high::to_bytes_in;
use std::sync::Arc;
use std::time::Duration;

use crate::backend::{BackendConfig, StorageBackend};
use crate::error::FsError;
use crate::writeback::{DrainableInodeIntent, InodeOp as WbInodeOp, WritebackQueue};
use data_types::object_layout::{InodeRecord, ObjectState, PosixAttrs};

/// Max concurrent `put_inode` RPCs per drained batch. Intents in a batch
/// are on distinct inodes (see `drain_pending`), so they publish in
/// parallel; the cap bounds in-flight RPCs against NSS.
const PUBLISH_CONCURRENCY: usize = 32;

/// Long-running writeback worker. Polls the queue every `poll_ms`,
/// drains pending intents, and fires NSS `put_inode` for each.
/// Spawned at FUSE init when `WritebackMode::Default` is configured;
/// runs until the process exits. Each intent ships as a single-op
/// `put_inode` RPC; the pipelining win comes from overlapping many such
/// round-trips concurrently, not from coalescing them.
pub(crate) fn spawn_writeback_worker(
    backend_cfg: Arc<BackendConfig>,
    queue: Arc<WritebackQueue>,
    poll_ms: u32,
) {
    let poll_dur = Duration::from_millis(poll_ms.max(1) as u64);
    compio_runtime::spawn(async move {
        // One backend per concurrent publish lane. StorageBackend has
        // RefCell-backed clients so independent futures must not share one
        // instance across awaits, especially when failover refresh mutates the
        // cached NSS client.
        let mut backends = Vec::with_capacity(PUBLISH_CONCURRENCY);
        for lane in 0..PUBLISH_CONCURRENCY {
            match StorageBackend::new(&backend_cfg) {
                Ok(b) => backends.push(b),
                Err(e) => {
                    tracing::warn!(
                        lane,
                        error = %e,
                        "writeback worker: failed to init backend; aborting"
                    );
                    return;
                }
            }
        }

        loop {
            compio_runtime::time::sleep(poll_dur).await;

            // Drain a batch of pending intents. The drainer flips them
            // to InFlight before returning so concurrent enqueues fall
            // into the next-cycle / backpressure path.
            let drained = queue.drain_pending(1024);
            if drained.is_empty() {
                continue;
            }

            // Publish independent intents concurrently. `drain_pending`
            // returns at most one generation per inode, so no two intents in
            // the batch touch the same inode; they are order-independent and
            // safe to fire together. Bounded chunks cap the fan-out on NSS so
            // a large batch cannot open thousands of in-flight RPCs at once.
            let queue = &queue;
            for chunk in drained.chunks(PUBLISH_CONCURRENCY) {
                futures::future::join_all(chunk.iter().enumerate().map(|(lane, intent)| {
                    let backend = &backends[lane];
                    async move {
                        let inode = intent.inode;
                        match publish_intent_with_retry(backend, intent).await {
                            Ok(_) => {
                                queue.mark_committed(&intent.s3_key, intent.generation, inode);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    key = %intent.s3_key,
                                    generation = intent.generation.0,
                                    error = %e,
                                    "writeback publish failed"
                                );
                                queue.mark_failed(&intent.s3_key, intent.generation, inode);
                            }
                        }
                    }
                }))
                .await;
            }
        }
    })
    .detach();
}

/// Absence-guarded create that tolerates an internally-retried RPC whose
/// first attempt committed but whose reply was lost. A blind `put_inode`
/// was idempotent under such a retry; the CAS-on-absence is not: the
/// re-sent attempt sees the key present and returns `CasConflict` against
/// the mount's own committed layout. On `CasConflict`, re-fetch and
/// compare bytes: if the stored inode byte-equals what we are publishing it
/// is our own commit (success); otherwise a peer won the name (a real
/// `CasConflict`). A peer's create never matches because the layout carries
/// a per-publish `version_id`.
pub(crate) async fn put_inode_create_idempotent(
    backend: &StorageBackend,
    key: &str,
    layout_bytes: Bytes,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    match backend
        .put_inode_cas(key, layout_bytes.clone(), Bytes::new(), trace_id)
        .await
    {
        Ok(_) => Ok(()),
        Err(FsError::CasConflict) => match backend.get_inode(key, trace_id).await {
            Ok(cur) => {
                let cur_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&cur, Vec::new())
                    .map(Bytes::from)
                    .map_err(FsError::from)?;
                if cur_bytes == layout_bytes {
                    Ok(())
                } else {
                    Err(FsError::CasConflict)
                }
            }
            // The key vanished between the CAS and this fetch (a concurrent
            // delete): treat as a lost race, not our own commit.
            Err(FsError::NotFound) => Err(FsError::CasConflict),
            Err(e) => Err(e),
        },
        Err(e) => Err(e),
    }
}

/// Ship one intent to NSS with bounded retries, so a transient backend
/// blip doesn't taint the inode and silently drop metadata the caller
/// already saw succeed.
async fn publish_intent_with_retry(
    backend: &StorageBackend,
    intent: &DrainableInodeIntent,
) -> Result<(), FsError> {
    const MAX_ATTEMPTS: u32 = 3;
    let mut result = Ok(());
    for attempt in 1..=MAX_ATTEMPTS {
        let trace_id = TraceId::new();
        result = match &intent.op {
            // Brand-new entry create. Guard on absence (empty expected
            // bytes) so a peer that created the same name during the
            // async window is not blindly overwritten; a lost race
            // surfaces as CasConflict, taints the inode, and the caller
            // re-looks-up the winner.
            WbInodeOp::PutInode { layout_bytes, .. } => {
                put_inode_create_idempotent(
                    backend,
                    &intent.s3_key,
                    layout_bytes.clone(),
                    &trace_id,
                )
                .await
            }
            WbInodeOp::SetPosix {
                posix,
                expected_layout_bytes,
                layout_bytes,
            } => {
                publish_set_posix(
                    backend,
                    &intent.s3_key,
                    posix,
                    expected_layout_bytes,
                    layout_bytes,
                    &trace_id,
                )
                .await
            }
        };
        match &result {
            Ok(()) => return Ok(()),
            // An absence-guarded create that hits CasConflict lost the
            // name to a peer; that is terminal (retrying can only lose
            // again), so surface it now to taint and re-lookup. SetPosix
            // keeps the outer retry: its own fold loop re-fetches fresh
            // state, so a later attempt can still win a bursty conflict.
            Err(FsError::CasConflict) if matches!(intent.op, WbInodeOp::PutInode { .. }) => {
                return result;
            }
            Err(e) if attempt < MAX_ATTEMPTS => {
                tracing::warn!(
                    key = %intent.s3_key,
                    attempt,
                    error = %e,
                    "writeback publish retrying"
                );
                compio_runtime::time::sleep(Duration::from_millis(20 * attempt as u64)).await;
            }
            Err(_) => {}
        }
    }
    result
}

/// Apply a posix-only update via CAS. Fast path: one `put_inode_cas`
/// guarded on the layout snapshot taken at enqueue. On conflict the
/// fresh layout is fetched and the posix folded onto it, so a
/// concurrent data publish (close-flush CAS) is never rolled back to
/// the enqueue-time blob state. A missing key means the entry was
/// deleted after the enqueue; the update is moot.
pub(crate) async fn publish_set_posix(
    backend: &StorageBackend,
    key: &str,
    posix: &PosixAttrs,
    expected: &Bytes,
    folded: &Bytes,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    match backend
        .put_inode_cas(key, folded.clone(), expected.clone(), trace_id)
        .await
    {
        Ok(_) => return Ok(()),
        Err(FsError::CasConflict) => {}
        Err(FsError::NotFound) => return Ok(()),
        Err(e) => return Err(e),
    }
    const MAX_CAS_RETRIES: u32 = 4;
    for _ in 0..MAX_CAS_RETRIES {
        let cur = match backend.get_inode(key, trace_id).await {
            Ok(l) => l,
            Err(FsError::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        };
        // A concurrent hardlink promotion moved the posix into the
        // shared record; follow the redirect and publish there instead
        // of folding metadata into the redirect row.
        if let ObjectState::Indirect(redirect) = &cur.state {
            return publish_set_posix_record(backend, redirect.inode_id, posix, trace_id).await;
        }
        let cur_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&cur, Vec::new())
            .map_err(FsError::from)?
            .into();
        let new_layout = crate::inode::layout_with_posix(cur, *posix);
        let new_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&new_layout, Vec::new())
            .map_err(FsError::from)?
            .into();
        match backend
            .put_inode_cas(key, new_bytes, cur_bytes, trace_id)
            .await
        {
            Ok(_) => return Ok(()),
            Err(FsError::CasConflict) => continue,
            Err(FsError::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
    Err(FsError::CasConflict)
}

async fn publish_set_posix_record(
    backend: &StorageBackend,
    inode_id: uuid::Uuid,
    posix: &PosixAttrs,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    const MAX_CAS_RETRIES: u32 = 4;
    let key = InodeRecord::key_for(inode_id);
    for _ in 0..MAX_CAS_RETRIES {
        let mut record = match backend.get_inode_record(inode_id, trace_id).await {
            Ok(record) => record,
            Err(FsError::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        };
        let old_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&record, Vec::new())
            .map_err(FsError::from)?
            .into();
        record.layout = crate::inode::layout_with_posix(record.layout.clone(), *posix);
        let new_bytes: Bytes = to_bytes_in::<_, rkyv::rancor::Error>(&record, Vec::new())
            .map_err(FsError::from)?
            .into();
        match backend
            .put_inode_cas(&key, new_bytes, old_bytes, trace_id)
            .await
        {
            Ok(_) => return Ok(()),
            Err(FsError::CasConflict) => continue,
            Err(FsError::NotFound) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
    Err(FsError::CasConflict)
}
