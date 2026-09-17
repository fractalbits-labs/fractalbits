//! The `force` delete sweep: empty a drive's bucket, then tear it down.
//!
//! Runs on the management server's runtime for the gateway that took the
//! request. Every object goes through `delete_object_by_key`, the path
//! `DeleteObjects` uses, so blob generations and multipart parts are
//! handled exactly as an S3 delete.
//!
//! One sweeper per drive: the record's `sweep_heartbeat_ms` is a lease.
//! Objects are deleted in batches of `BATCH` keys; the lease is renewed by
//! compare-and-set before each batch and, while a batch runs, every
//! `RENEW_EVERY` by a renewer that races the batch. A renewal that loses
//! the race means another gateway took the lease: the batch in flight is
//! allowed to settle and nothing further starts. A per-process set covers
//! repeated requests to the same gateway. Any failure leaves the record in
//! `deleting` with a stale heartbeat; the next `force` delete resumes from
//! the remaining keys. Authorization is re-read from RSS before every page
//! and once more before the final bucket delete, which is fenced on the
//! bucket incarnation the record names.
//!
//! A lease write is a compare-and-set whose reply can be lost in transit;
//! the retry then carries a stale expected version and reports a lost
//! race for a write that landed, or ends in a transport error that says
//! nothing either way. Every lease write therefore carries the holder's
//! `sweep_token`, and any failed write is reconciled by reading the record
//! back: a record bearing this token and this heartbeat is this write, and
//! its version becomes the handle; any other content is a real loss. When
//! the read-back fails too the outcome is unknown, and the worker slot
//! already registered for the drive stays responsible for settling it,
//! retrying the read until RSS answers, before it either sweeps or exits.

use data_types::{Bucket, Drive, DriveStatus, TraceId, Versioned, drive::unix_ms};
use futures::future::{Either, select};
use futures::{StreamExt, stream};
use rpc_client_common::RpcError;
use std::collections::HashSet;
use std::pin::pin;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    AppState, drive_routes,
    handler::{common::list_raw_objects, common::s3_error::S3Error, delete_object_by_key},
};

const PAGE: u32 = 1000;
/// Objects per lease renewal.
const BATCH: usize = 100;
/// Renewal period while a batch is in flight, well inside `SWEEP_LEASE_MS`.
const RENEW_EVERY: Duration = Duration::from_secs(15);
/// Bounded so a sweep cannot starve foreground S3 traffic on this gateway.
pub const SWEEP_CONCURRENCY: usize = 8;

/// Drives this process is sweeping right now.
static RUNNING: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

pub enum Start {
    /// This request took the lease and a worker runs with this record.
    Spawned(Box<Versioned<Drive>>),
    /// A worker in this process is registered for the drive, or another
    /// request took the lease first; nothing was started here.
    Busy,
    /// The lease write's outcome is unknown; a worker holds the slot and
    /// settles it before sweeping or exiting.
    Settling,
}

/// What a lease write turned out to be once reconciled.
#[derive(Debug, PartialEq, Eq)]
pub enum LeaseWrite {
    Landed,
    Lost,
    /// The read-back failed as well; nothing is known yet.
    Unknown,
}

fn register(name: &str) -> bool {
    RUNNING.lock().expect("sweep set").insert(name.to_string())
}

fn release(name: &str) {
    RUNNING.lock().expect("sweep set").remove(name);
}

/// Take the lease and start a worker for it, in that order of commitment:
/// the worker slot in this process is registered first and released again
/// if the lease is not obtained, so a lease is never held by this process
/// without a sweeper behind it. The lease write returns the version it
/// stored, so the worker runs on exactly the incarnation that took the
/// lease; nothing is read back by name in between.
pub async fn start(
    app: Arc<AppState>,
    mut drive: Versioned<Drive>,
    key_id: String,
    trace_id: &TraceId,
) -> Result<Start, RpcError> {
    let name = drive.data.name.clone();
    if !register(&name) {
        return Ok(Start::Busy);
    }
    let now = unix_ms();
    drive.data.status = DriveStatus::Deleting;
    if drive.data.deleting_since_ms == 0 {
        drive.data.deleting_since_ms = now;
    }
    drive.data.sweep_heartbeat_ms = now;
    drive.data.sweep_token = Uuid::new_v4().to_string();
    match write_lease(&app, &mut drive, trace_id).await {
        LeaseWrite::Landed => {
            actix_web::rt::spawn(run(app, drive.clone(), key_id, false));
            Ok(Start::Spawned(Box::new(drive)))
        }
        LeaseWrite::Lost => {
            release(&name);
            Ok(Start::Busy)
        }
        // The slot is kept: the worker settles the outcome first, and a
        // lease that did land is never left without a sweeper.
        LeaseWrite::Unknown => {
            actix_web::rt::spawn(run(app, drive, key_id, true));
            Ok(Start::Settling)
        }
    }
}

/// The stored record, if it is the lease write `written` describes: same
/// drive, same token, same heartbeat. Its version is then the handle for
/// the next write.
fn reconcile_lease(
    written: &Versioned<Drive>,
    stored: Option<Versioned<Drive>>,
) -> Option<Versioned<Drive>> {
    stored.filter(|stored| {
        stored.data.id == written.data.id
            && stored.data.sweep_token == written.data.sweep_token
            && stored.data.sweep_heartbeat_ms == written.data.sweep_heartbeat_ms
    })
}

/// Classify a failed lease write from a read-back of the record. Any
/// failure, a CAS rejection or a transport error, may hide a write that
/// landed before its reply was lost, so every one is reconciled the same
/// way; a failed read-back leaves the outcome unknown.
fn after_uncertain_write(
    written: &mut Versioned<Drive>,
    read: Result<Option<Versioned<Drive>>, RpcError>,
) -> LeaseWrite {
    match read {
        Ok(stored) => match reconcile_lease(written, stored) {
            Some(landed) => {
                *written = landed;
                LeaseWrite::Landed
            }
            None => LeaseWrite::Lost,
        },
        Err(_) => LeaseWrite::Unknown,
    }
}

/// Write the lease in `drive` by compare-and-set and reconcile any failure.
async fn write_lease(
    app: &AppState,
    drive: &mut Versioned<Drive>,
    trace_id: &TraceId,
) -> LeaseWrite {
    match app.put_drive(drive, trace_id).await {
        Ok(()) => LeaseWrite::Landed,
        Err(_) => {
            let read = app.get_drive(&drive.data.name, trace_id).await;
            after_uncertain_write(drive, read)
        }
    }
}

/// Resolve an unknown outcome by reading back until RSS answers. The
/// caller holds the worker slot and must not give it up before this
/// returns; if the write did land, other gateways see a live heartbeat
/// meanwhile, and if RSS stays away past the lease they may take over,
/// which the read-back then reports as a loss.
async fn settle_lease(app: &AppState, drive: &mut Versioned<Drive>, trace_id: &TraceId) -> bool {
    let mut backoff = Duration::from_secs(1);
    loop {
        let read = app.get_drive(&drive.data.name, trace_id).await;
        match after_uncertain_write(drive, read) {
            LeaseWrite::Landed => return true,
            LeaseWrite::Lost => return false,
            LeaseWrite::Unknown => {
                warn!(
                    "drive {}: lease outcome still unknown, RSS unreachable",
                    drive.data.name
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(10));
            }
        }
    }
}

async fn run(app: Arc<AppState>, mut drive: Versioned<Drive>, key_id: String, settle: bool) {
    let name = drive.data.name.clone();
    let trace_id = TraceId::new();
    if settle && !settle_lease(&app, &mut drive, &trace_id).await {
        info!("drive {name}: the uncertain lease write had not landed, no sweep");
        release(&name);
        return;
    }
    match sweep(&app, drive, &key_id, &trace_id).await {
        Ok(Outcome::Done(deleted)) => {
            info!("drive {name} force delete done, {deleted} objects removed")
        }
        Ok(Outcome::LeaseLost(deleted)) => {
            info!("drive {name} sweep lease taken by another gateway after {deleted} objects")
        }
        Ok(Outcome::Stale) => info!("drive {name} record was stale, dropped without a sweep"),
        Err(e) => error!("drive {name} force delete stopped, record left deleting: {e}"),
    }
    release(&name);
}

enum Outcome {
    Done(usize),
    LeaseLost(usize),
    Stale,
}

/// The key must still exist and own the bucket, read fresh from RSS.
async fn still_authorized(app: &AppState, key_id: &str, name: &str, trace_id: &TraceId) -> bool {
    match app.refresh_api_key(key_id.to_string(), trace_id).await {
        Ok(key) => !key.data.is_deleted && key.data.allow_owner(name),
        Err(_) => false,
    }
}

/// Renew the lease; `false` when another sweeper took it. The write
/// updates `drive.version`, so the next renewal is fenced on this one.
async fn renew(
    app: &AppState,
    drive: &mut Versioned<Drive>,
    trace_id: &TraceId,
) -> Result<bool, S3Error> {
    // A renewal in the same millisecond as the previous write would be
    // indistinguishable from it when reconciling; step past it.
    drive.data.sweep_heartbeat_ms = unix_ms().max(drive.data.sweep_heartbeat_ms + 1);
    match write_lease(app, drive, trace_id).await {
        LeaseWrite::Landed => Ok(true),
        LeaseWrite::Lost => Ok(false),
        LeaseWrite::Unknown => Ok(settle_lease(app, drive, trace_id).await),
    }
}

/// Delete one batch while renewing the lease every `RENEW_EVERY`. Returns
/// the batch's results and whether the lease is still ours; on a lost lease
/// the RPCs already issued settle, and the caller starts no new batch.
async fn delete_batch(
    app: &Arc<AppState>,
    bucket: &Bucket,
    drive: &mut Versioned<Drive>,
    keys: Vec<String>,
    trace_id: &TraceId,
) -> Result<(Vec<Result<(), S3Error>>, bool), S3Error> {
    let batch = stream::iter(keys)
        .map(|key| async move {
            match delete_object_by_key(app, bucket, &key, trace_id).await {
                Ok(()) | Err(S3Error::NoSuchKey) => Ok(()),
                Err(e) => Err(e),
            }
        })
        .buffer_unordered(SWEEP_CONCURRENCY)
        .collect::<Vec<_>>();
    let mut batch = pin!(batch);
    loop {
        let timer = pin!(tokio::time::sleep(RENEW_EVERY));
        match select(batch.as_mut(), timer).await {
            Either::Left((results, _)) => return Ok((results, true)),
            Either::Right(((), _)) => {
                if !renew(app, drive, trace_id).await? {
                    return Ok((batch.await, false));
                }
            }
        }
    }
}

async fn sweep(
    app: &Arc<AppState>,
    mut drive: Versioned<Drive>,
    key_id: &str,
    trace_id: &TraceId,
) -> Result<Outcome, S3Error> {
    let name = drive.data.name.clone();
    let bucket: Bucket = match app.fetch_bucket_no_cache(&name, trace_id).await {
        Ok(b) => b.data,
        // Already gone through the S3 door; only the record is left.
        Err(RpcError::NotFound) => {
            drive_routes::drop_stale_record(app, drive, trace_id).await?;
            return Ok(Outcome::Stale);
        }
        Err(e) => return Err(e.into()),
    };
    // The bucket was recreated under the same name since this record was
    // written: the record is stale and the new bucket is not ours to empty.
    if bucket.root_blob_name != drive.data.bucket_root {
        warn!("drive {name}: record is for an earlier bucket incarnation, dropping it");
        drive_routes::drop_stale_record(app, drive, trace_id).await?;
        return Ok(Outcome::Stale);
    }
    // As the S3 handlers do before their first NSS call for a bucket.
    if !app
        .ensure_nss_client_initialized(&bucket.routing_key, trace_id)
        .await
    {
        return Err(S3Error::ServiceUnavailable);
    }

    let mut deleted = 0usize;
    let mut start_after = String::new();
    loop {
        if !still_authorized(app, key_id, &name, trace_id).await {
            return Err(S3Error::AccessDenied);
        }
        // User keys all start with "/"; the "@" keyspaces are the teardown's.
        let page = list_raw_objects(
            app,
            &bucket.routing_key,
            &bucket.root_blob_name,
            PAGE,
            "/",
            "",
            &start_after,
            false,
            trace_id,
        )
        .await?;
        let Some((last, _)) = page.last() else {
            break;
        };
        start_after = last.clone();
        let keys: Vec<String> = page.into_iter().map(|(key, _)| key).collect();
        for batch in keys.chunks(BATCH) {
            if !renew(app, &mut drive, trace_id).await? {
                return Ok(Outcome::LeaseLost(deleted));
            }
            let (results, kept) =
                delete_batch(app, &bucket, &mut drive, batch.to_vec(), trace_id).await?;
            for r in results {
                r?;
                deleted += 1;
            }
            if !kept {
                return Ok(Outcome::LeaseLost(deleted));
            }
        }
    }

    // Final fence: still authorized, still the lease holder, and only the
    // bucket incarnation this record names.
    if !still_authorized(app, key_id, &name, trace_id).await {
        return Err(S3Error::AccessDenied);
    }
    if !renew(app, &mut drive, trace_id).await? {
        return Ok(Outcome::LeaseLost(deleted));
    }
    match app
        .delete_bucket(&name, key_id, &drive.data.bucket_root, *trace_id)
        .await
    {
        Ok(()) => {}
        Err(RpcError::IncarnationMismatch) => {
            warn!("drive {name}: bucket replaced during the sweep, dropping the record");
            drive_routes::drop_stale_record(app, drive, trace_id).await?;
            return Ok(Outcome::Stale);
        }
        Err(e) => {
            error!("drive {name}: delete bucket after sweep: {e}");
            return Err(e.into());
        }
    }
    drive_routes::drop_stale_record(app, drive, trace_id).await?;
    Ok(Outcome::Done(deleted))
}

#[cfg(test)]
mod lease_tests {
    use super::*;
    use std::collections::BTreeMap;

    fn record(token: &str, heartbeat: u64, version: i64) -> Versioned<Drive> {
        let mut drive = Drive::new("d1", "", BTreeMap::new(), "k", "root-1");
        drive.id = "id-1".into();
        drive.status = DriveStatus::Deleting;
        drive.sweep_token = token.into();
        drive.sweep_heartbeat_ms = heartbeat;
        Versioned::new(version, drive)
    }

    #[test]
    fn a_landed_write_is_recognised_by_its_token() {
        let written = record("tok-a", 1_000, 3);
        let landed = reconcile_lease(&written, Some(record("tok-a", 1_000, 4)))
            .expect("same token and heartbeat is our write");
        assert_eq!(landed.version, 4, "the stored version becomes the handle");
    }

    #[test]
    fn any_failed_write_is_classified_by_the_read_back() {
        let mut written = record("tok-a", 1_000, 3);
        assert_eq!(
            after_uncertain_write(&mut written, Ok(Some(record("tok-a", 1_000, 4)))),
            LeaseWrite::Landed,
            "landed before the reply was lost"
        );
        assert_eq!(written.version, 4, "handle adopted");
        let mut written = record("tok-a", 1_000, 3);
        assert_eq!(
            after_uncertain_write(&mut written, Ok(Some(record("tok-b", 1_000, 4)))),
            LeaseWrite::Lost,
            "another holder"
        );
        assert_eq!(
            after_uncertain_write(&mut written, Ok(None)),
            LeaseWrite::Lost,
            "record gone"
        );
        let transport = Err(RpcError::IoError(std::io::Error::other("rss unreachable")));
        assert_eq!(
            after_uncertain_write(&mut written, transport),
            LeaseWrite::Unknown,
            "read-back failed: nothing known, the worker keeps settling"
        );
        assert_eq!(written.version, 3, "handle untouched while unknown");
    }

    #[test]
    fn anything_else_is_a_real_loss() {
        let written = record("tok-a", 1_000, 3);
        assert!(reconcile_lease(&written, None).is_none(), "record gone");
        assert!(
            reconcile_lease(&written, Some(record("tok-b", 1_000, 4))).is_none(),
            "another holder's token"
        );
        assert!(
            reconcile_lease(&written, Some(record("tok-a", 999, 4))).is_none(),
            "our token but an earlier write"
        );
        let mut other_drive = record("tok-a", 1_000, 4);
        other_drive.data.id = "id-2".into();
        assert!(
            reconcile_lease(&written, Some(other_drive)).is_none(),
            "a replacement record"
        );
    }
}
