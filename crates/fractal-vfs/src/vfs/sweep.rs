//! Reclamation hand-off: the commit and teardown paths decide what a
//! change made unreachable and ship that to the gateway as `SweepBlob`.
//! The gateway owns the reader grace, retries, and the teardown marker
//! replay; only the marker writes that must precede a namespace
//! mutation stay inline here.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use data_types::object_layout::ObjectLayout;
use data_types::ovr_map::{
    OvrGcMarker, encode_ovr_gc_data_pending, ovr_gc_key, parse_ovr_gc_marker,
};
use data_types::{DataBlobGuid, TraceId};
use rpc_client_common::reclamation_grace;

use crate::backend::StorageBackend;
use crate::error::FsError;
use crate::vfs::VfsCore;

const TEARDOWN_MARKER_CAS_RETRIES: u32 = 16;

#[derive(Clone)]
enum TeardownMarkerWrite {
    Conditional(Bytes),
    DataPending {
        volume_id: u16,
        not_before_unix_ms: u64,
    },
}

impl TeardownMarkerWrite {
    fn value(&self) -> Bytes {
        match self {
            Self::Conditional(value) => value.clone(),
            Self::DataPending {
                volume_id,
                not_before_unix_ms,
            } => {
                Bytes::copy_from_slice(&encode_ovr_gc_data_pending(*volume_id, *not_before_unix_ms))
            }
        }
    }

    fn satisfied_by(&self, current: Option<&[u8]>) -> bool {
        let Some(current) = current else {
            return false;
        };
        match self {
            Self::Conditional(_) => true,
            Self::DataPending { volume_id, .. } => matches!(
                parse_ovr_gc_marker(current),
                OvrGcMarker::DataPending {
                    volume_id: stored,
                    ..
                } | OvrGcMarker::RowsReady { volume_id: stored }
                    if stored == *volume_id
            ),
        }
    }
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn unix_deadline_after(duration: Duration) -> u64 {
    let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    unix_time_millis().saturating_add(millis)
}

/// Reclamation work derived by one commit or teardown, in the shape the
/// gateway consumes. `key` is the inode the blob was published under;
/// the gateway re-derives reclaimability from it.
#[derive(Debug, Default)]
struct SweepWork {
    key: String,
    victims: Vec<(u32, u64)>,
    below: Vec<(u32, u64)>,
    delete_all_blocks: bool,
    delete_rows: bool,
    marker_data_pending: Option<u64>,
}

async fn write_teardown_marker_cas(
    backend: &StorageBackend,
    blob_guid: DataBlobGuid,
    target: TeardownMarkerWrite,
    trace_id: &TraceId,
) -> Result<(), FsError> {
    let key = ovr_gc_key(&blob_guid.blob_id);
    let value = target.value();
    let mut current: Option<Bytes> = None;
    for _ in 0..TEARDOWN_MARKER_CAS_RETRIES {
        if target.satisfied_by(current.as_deref()) {
            return Ok(());
        }
        let expected = current.clone().unwrap_or_default();
        match backend
            .put_inode_cas(&key, value.clone(), expected, trace_id)
            .await
        {
            Ok(_) => return Ok(()),
            Err(FsError::CasConflict) => {
                current = match backend.get_inode_raw(&key, trace_id).await {
                    Ok(stored) => Some(stored),
                    Err(FsError::NotFound) => None,
                    Err(error) => return Err(error),
                };
            }
            Err(error) => {
                let probe = backend.get_inode_raw(&key, trace_id).await;
                if target.satisfied_by(probe.as_ref().ok().map(Bytes::as_ref)) {
                    return Ok(());
                }
                return Err(error);
            }
        }
    }
    Err(FsError::Internal(format!(
        "teardown marker CAS budget exhausted for {key}"
    )))
}

impl VfsCore {
    fn marker_deadline(&self) -> u64 {
        unix_deadline_after(reclamation_grace(
            self.backend_config.config.rpc_request_timeout(),
        ))
    }

    /// Ship one unit of reclamation work to the gateway off the caller's
    /// path. `sweep_inflight` lets `destroy` wait for the hand-off itself
    /// (not the reclamation) before the process exits.
    fn spawn_sweep(&self, blob_guid: DataBlobGuid, work: SweepWork) {
        let backend = self.backend();
        let inflight = Arc::clone(&self.sweep_inflight);
        inflight.fetch_add(1, Ordering::AcqRel);
        compio_runtime::spawn(async move {
            backend
                .sweep_blob(
                    &work.key,
                    blob_guid,
                    work.victims,
                    work.below,
                    work.delete_all_blocks,
                    work.delete_rows,
                    true,
                    work.marker_data_pending,
                )
                .await;
            inflight.fetch_sub(1, Ordering::AcqRel);
        })
        .detach();
    }

    /// Queue reclamation of the identities a commit superseded. `victims`
    /// come exactly from the row CAS (the displaced `cur` slots plus the
    /// v1 generation of rewritten unmapped blocks); `below` covers blocks
    /// whose stale claims have no row record and need one listing.
    pub(crate) fn enqueue_superseded_sweep(
        &self,
        key: &str,
        blob_guid: DataBlobGuid,
        victims: Vec<(u32, u64)>,
        below: Vec<(u32, u64)>,
    ) {
        if victims.is_empty() && below.is_empty() {
            return;
        }
        self.spawn_sweep(
            blob_guid,
            SweepWork {
                key: key.to_string(),
                victims,
                below,
                ..Default::default()
            },
        );
    }

    /// Reclaim the fragments of a create attempt whose publish never
    /// landed. The blob_guid was freshly minted and is unreachable, so
    /// tear everything down after the grace. `key` is where the publish
    /// would have landed; nothing there can name this blob.
    pub(crate) async fn cleanup_unpublished_blob(
        &self,
        key: &str,
        blob_guid: DataBlobGuid,
        identities: Vec<(u32, u64)>,
    ) {
        self.spawn_sweep(
            blob_guid,
            SweepWork {
                key: key.to_string(),
                victims: identities,
                delete_all_blocks: true,
                ..Default::default()
            },
        );
    }

    /// Tear down every exact data/reservation key and every `@ovr/` row
    /// belonging to a blob, after the reclamation grace. `key` is the
    /// name the blob was published under, already unlinked or renamed
    /// over. This promotes any pre-mutation `@ovr-gc/` intent to a
    /// committed marker; the gateway removes it when the rows are gone,
    /// and its init-time scavenger replays committed markers a crash
    /// left behind.
    pub(crate) async fn teardown_blob(&self, key: &str, layout: &ObjectLayout) {
        let Ok(blob_guid) = layout.blob_guid() else {
            return;
        };
        let marker_required = layout.may_have_ovr_records();
        let mut work = SweepWork {
            key: key.to_string(),
            delete_all_blocks: true,
            delete_rows: marker_required,
            ..Default::default()
        };
        let marker_deadline = self.marker_deadline();
        // Promote any pre-mutation intent to an unconditional teardown
        // record only after the caller established that the blob is
        // unreachable. The durable deadline lets a restarted scavenger
        // preserve the reader grace before replaying data deletion.
        if marker_required {
            let trace_id = TraceId::new();
            let target = TeardownMarkerWrite::DataPending {
                volume_id: blob_guid.volume_id,
                not_before_unix_ms: marker_deadline,
            };
            if let Err(error) =
                write_teardown_marker_cas(self.backend(), blob_guid, target, &trace_id).await
            {
                work.marker_data_pending = Some(marker_deadline);
                tracing::warn!(%blob_guid, %error, "teardown marker promotion failed; queued retry");
            }
        }
        self.spawn_sweep(blob_guid, work);
    }

    /// Durable `@ovr-gc/{blob_id}` teardown marker. Written
    /// before the inode delete so a crash mid-teardown cannot leak the
    /// blob's rows forever (the blob_id is unrecoverable from any
    /// surviving key once the inode is gone). Layouts that cannot have
    /// overwrite rows need no marker.
    ///
    /// `doomed_key` is the metadata-store key whose deletion this marker
    /// anticipates. It MUST be passed when the marker is written before
    /// the namespace mutation (unlink, rename-over): the mutation can
    /// still fail, leaving a live blob under a standing death warrant,
    /// so the scavenger must distinguish it from a committed teardown.
    /// Conditional markers are never replayed. `None` means the blob is
    /// already unreachable and the marker may be replayed unconditionally.
    pub(crate) async fn write_teardown_marker(
        &self,
        layout: &ObjectLayout,
        doomed_key: Option<&str>,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        if !layout.may_have_ovr_records() {
            return Ok(());
        }
        let Ok(blob_guid) = layout.blob_guid() else {
            return Ok(());
        };
        let target = match doomed_key {
            Some(doomed) => TeardownMarkerWrite::Conditional(Bytes::from(doomed.to_owned())),
            None => TeardownMarkerWrite::DataPending {
                volume_id: blob_guid.volume_id,
                not_before_unix_ms: self.marker_deadline(),
            },
        };
        write_teardown_marker_cas(self.backend(), blob_guid, target, trace_id).await
    }

    /// Release mount-local writer state and enqueue cleanup that was
    /// waiting for the final open handle. Runs after dirty handles and
    /// metadata have drained, when no request worker can create another
    /// handle.
    pub async fn prepare_sweep_shutdown(&self) {
        let handle_ids = self
            .file_handles
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for fh in handle_ids {
            let Some((_, handle)) = self.file_handles.remove(&fh) else {
                continue;
            };
            if handle.write_buf.is_some() {
                self.release_write_lock(handle.ino, fh);
            }
        }
        self.inode_write_owner.clear();

        let trace_id = TraceId::new();
        let deferred_inodes = self
            .deferred_blob_cleanup
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for ino in deferred_inodes {
            if let Some((_, (key, old_bytes))) = self.deferred_blob_cleanup.remove(&ino) {
                self.cleanup_orphaned_value(&key, Some(ino), old_bytes, &trace_id)
                    .await;
            }
        }
    }

    /// Wait until every queued sweep hand-off has reached the gateway.
    /// The reclamation itself continues there after this mount is gone.
    pub async fn drain_sweep_work(&self) {
        while self.sweep_inflight.load(Ordering::Acquire) > 0 {
            compio_runtime::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Report sweep hand-offs still pending when the bounded shutdown
    /// drain expires. Namespace visibility is already gone; the gateway's
    /// marker scavenger recovers teardowns, superseded-block work is lost.
    pub fn log_incomplete_sweep_work(&self) {
        let pending = self.sweep_inflight.load(Ordering::Acquire);
        if pending == 0 {
            return;
        }
        tracing::error!(
            pending_sweeps = pending,
            open_handles = self.file_handles.len(),
            deferred_blobs = self.deferred_blob_cleanup.len(),
            "destroy: reclamation hand-off incomplete; invisible physical garbage may remain"
        );
    }
}

#[cfg(test)]
mod sweep_tests {
    use super::*;
    use data_types::ovr_map::encode_ovr_gc_rows_ready;

    #[test]
    fn marker_writes_are_monotonic() {
        let conditional = TeardownMarkerWrite::Conditional(Bytes::from_static(b"/doomed"));
        let data_pending = TeardownMarkerWrite::DataPending {
            volume_id: 7,
            not_before_unix_ms: 123,
        };
        let pending_value = encode_ovr_gc_data_pending(7, 123);
        let ready_value = encode_ovr_gc_rows_ready(7);

        assert!(!conditional.satisfied_by(None));
        assert!(conditional.satisfied_by(Some(b"/other")));
        assert!(conditional.satisfied_by(Some(&pending_value)));
        assert!(conditional.satisfied_by(Some(&ready_value)));

        assert!(!data_pending.satisfied_by(None));
        assert!(!data_pending.satisfied_by(Some(b"/doomed")));
        assert!(!data_pending.satisfied_by(Some(data_types::ovr_map::OVR_GC_LEGACY_VALUE)));
        assert!(data_pending.satisfied_by(Some(&pending_value)));
        assert!(data_pending.satisfied_by(Some(&ready_value)));
    }
}
