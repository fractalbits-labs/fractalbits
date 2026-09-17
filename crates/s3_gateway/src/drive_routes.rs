//! `/v1/drives`: create, list, get, delete.
//!
//! A drive is a bucket one to one; these handlers wrap `CreateBucket` and
//! `DeleteBucket` and keep the `drive:` record beside the bucket record.
//! Permissions are the bucket's, read through the key the `FBSIG1`
//! middleware verified.

use actix_web::{
    HttpMessage, HttpRequest, HttpResponse,
    http::StatusCode,
    web::{Data, Json, Path, Query},
};
use chrono::DateTime;
use data_types::{
    ApiKey, Bucket, Drive, DriveStatus, TraceId, Versioned,
    drive::{MAX_DRIVE_LABELS, is_valid_bucket_name, unix_ms},
};
use rpc_client_common::RpcError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    AppState, drive_sweep,
    mgmt_auth::{AuthedKey, api_error},
};

const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateDriveRequest {
    pub name: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Deserialize)]
pub struct ListQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Deserialize)]
pub struct DeleteQuery {
    #[serde(default)]
    pub force: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DriveResponse {
    pub id: String,
    pub name: String,
    pub display_name: String,
    pub labels: BTreeMap<String, String>,
    pub status: DriveStatus,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleting_since: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub stale_deleting: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PageMeta {
    pub has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Serialize)]
pub struct Page {
    pub data: Vec<DriveResponse>,
    pub meta: PageMeta,
}

fn rfc3339(ms: u64) -> String {
    DateTime::from_timestamp_millis(ms as i64)
        .map(|t| t.to_rfc3339())
        .unwrap_or_default()
}

impl From<Drive> for DriveResponse {
    fn from(d: Drive) -> Self {
        let deleting = d.status == DriveStatus::Deleting && d.deleting_since_ms > 0;
        // No sweeper is renewing the lease; a `force` delete resumes it.
        let stale_deleting = deleting && !d.sweep_alive(unix_ms());
        Self {
            id: d.id,
            name: d.name,
            display_name: d.display_name,
            labels: d.labels,
            status: d.status,
            created_at: rfc3339(d.created_at_ms),
            deleting_since: deleting.then(|| rfc3339(d.deleting_since_ms)),
            stale_deleting,
        }
    }
}

fn internal(what: &str, e: impl std::fmt::Display) -> HttpResponse {
    error!("{what}: {e}");
    api_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal",
        format!("{what}: {e}"),
    )
}

fn not_found() -> HttpResponse {
    api_error(StatusCode::NOT_FOUND, "not_found", "no such drive")
}

fn authed_key(req: &HttpRequest) -> Option<Versioned<ApiKey>> {
    req.extensions().get::<AuthedKey>().map(|k| k.0.clone())
}

/// A permission check that refreshes the key once on deny, the same
/// staleness rule the S3 handlers apply: another gateway may have just
/// granted this key the bucket.
async fn allowed(
    app: &AppState,
    key: &mut Versioned<ApiKey>,
    bucket: &str,
    pred: fn(&ApiKey, &str) -> bool,
) -> bool {
    if pred(&key.data, bucket) {
        return true;
    }
    let trace_id = TraceId::new();
    if let Ok(fresh) = app
        .refresh_api_key(key.data.key_id.clone(), &trace_id)
        .await
    {
        *key = fresh;
    }
    pred(&key.data, bucket)
}

/// Remove a stale record, but only the incarnation the caller read. A lost
/// race means the record changed under us: re-read, and if it now
/// describes another drive (different id or bucket root) leave it alone;
/// only the same incarnation is retried. Either way the caller's drive is
/// gone, and the replacement is never handed to a caller who addressed
/// the old one.
pub(crate) async fn drop_stale_record(
    app: &AppState,
    mut drive: Versioned<Drive>,
    trace_id: &TraceId,
) -> Result<(), RpcError> {
    for _ in 0..3 {
        match app.delete_drive_record_if_version(&drive, trace_id).await {
            Ok(()) => return Ok(()),
            Err(RpcError::Retry) => {}
            Err(e) => return Err(e),
        }
        match app.get_drive(&drive.data.name, trace_id).await? {
            None => return Ok(()),
            Some(current)
                if current.data.id != drive.data.id
                    || current.data.bucket_root != drive.data.bucket_root =>
            {
                return Ok(());
            }
            Some(current) => drive = current,
        }
    }
    // The same incarnation kept changing under us; do not claim it is gone.
    Err(RpcError::Retry)
}

/// The bucket a record describes, if that incarnation still exists. A record
/// whose bucket is gone, or was recreated under the same name (new root),
/// is stale: it is dropped so its id never attaches to the new bucket, and
/// the caller treats the drive as absent.
async fn live_bucket(
    app: &AppState,
    drive: &Versioned<Drive>,
    trace_id: &TraceId,
) -> Result<Option<Bucket>, RpcError> {
    let stale = match app.fetch_bucket_no_cache(&drive.data.name, trace_id).await {
        Ok(bucket) if bucket.data.root_blob_name == drive.data.bucket_root => {
            return Ok(Some(bucket.data));
        }
        Ok(_) => "recreated",
        Err(RpcError::NotFound) => "deleted",
        Err(e) => return Err(e),
    };
    info!(
        "drive {}: bucket {stale} since the record was written, dropping it",
        drive.data.name
    );
    drop_stale_record(app, drive.clone(), trace_id).await?;
    Ok(None)
}

/// Resolve a path segment as an id (by scanning the listing) and then as
/// a name; a uuid satisfies the bucket name rules, so id goes first.
async fn resolve_ref(
    app: &AppState,
    r: &str,
    trace_id: &TraceId,
) -> Result<Option<Versioned<Drive>>, RpcError> {
    if Uuid::parse_str(r).is_ok()
        && let Some(d) = app
            .list_drives(trace_id)
            .await?
            .into_iter()
            .find(|d| d.id == r)
    {
        // Never answer with a replacement record when the caller named an id.
        return Ok(app
            .get_drive(&d.name, trace_id)
            .await?
            .filter(|current| current.data.id == r));
    }
    if !is_valid_bucket_name(r) {
        return Ok(None);
    }
    app.get_drive(r, trace_id).await
}

pub async fn create_drive(
    app: Data<Arc<AppState>>,
    req: HttpRequest,
    body: Json<CreateDriveRequest>,
) -> HttpResponse {
    let Some(mut key) = authed_key(&req) else {
        return api_error(StatusCode::UNAUTHORIZED, "unauthorized", "no key");
    };
    let body = body.into_inner();
    if !is_valid_bucket_name(&body.name) {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_name",
            "not a valid bucket name",
        );
    }
    if body.labels.len() > MAX_DRIVE_LABELS {
        return api_error(
            StatusCode::BAD_REQUEST,
            "invalid_labels",
            format!("at most {MAX_DRIVE_LABELS} labels"),
        );
    }
    let trace_id = TraceId::new();
    let name = body.name.as_str();
    info!("create drive {name}");

    // Existing record: idempotent when the caller owns it and the metadata matches.
    let answer_existing = |existing: Drive, key_owns: bool| -> HttpResponse {
        if !key_owns {
            return api_error(StatusCode::CONFLICT, "already_exists", "drive exists");
        }
        match existing.status {
            DriveStatus::Deleting => {
                api_error(StatusCode::CONFLICT, "deleting", "drive is being deleted")
            }
            DriveStatus::Ready if existing.same_metadata(&body.display_name, &body.labels) => {
                HttpResponse::Ok().json(DriveResponse::from(existing))
            }
            DriveStatus::Ready => api_error(
                StatusCode::CONFLICT,
                "already_exists",
                "drive exists with different metadata",
            ),
        }
    };

    match app.get_drive(name, &trace_id).await {
        Ok(Some(existing)) => match live_bucket(&app, &existing, &trace_id).await {
            Ok(Some(_)) => {
                let owns = allowed(&app, &mut key, name, ApiKey::allow_owner).await;
                return answer_existing(existing.data, owns);
            }
            // A stale record was dropped; this is a fresh create or adoption.
            Ok(None) => {}
            Err(e) => return internal("get bucket", e),
        },
        Ok(None) => {}
        Err(e) => return internal("get drive", e),
    }

    if !key.data.allow_create_bucket {
        return api_error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "key may not create buckets",
        );
    }
    match app.create_bucket(name, &key.data.key_id, trace_id).await {
        // A fresh bucket, or adoption of one this key already owns.
        Ok(()) | Err(RpcError::BucketAlreadyOwnedByYou) => {}
        Err(RpcError::AlreadyExists) => {
            return api_error(StatusCode::CONFLICT, "already_exists", "drive exists");
        }
        Err(RpcError::InternalResponseError(msg)) if msg.contains("API key not found") => {
            return api_error(StatusCode::FORBIDDEN, "forbidden", msg);
        }
        Err(e) => return internal("create bucket", e),
    }

    // The record names the bucket incarnation it belongs to.
    let bucket = match app.fetch_bucket_no_cache(name, &trace_id).await {
        Ok(bucket) => bucket.data,
        Err(e) => return internal("get bucket after create", e),
    };
    let drive = Drive::new(
        name,
        &body.display_name,
        body.labels.clone(),
        &key.data.key_id,
        &bucket.root_blob_name,
    );
    for _ in 0..3 {
        match app
            .put_drive(&mut Versioned::new(0, drive.clone()), &trace_id)
            .await
        {
            Ok(()) => return HttpResponse::Ok().json(DriveResponse::from(drive)),
            Err(RpcError::Retry) => {}
            Err(e) => return internal("put drive", e),
        }
        // Lost the race for the record. A concurrent create of this same
        // bucket incarnation won: answer from it. A record for an earlier
        // incarnation reappeared: it is stale, drop it and try again.
        let existing = match app.get_drive(name, &trace_id).await {
            Ok(Some(existing)) => existing,
            Ok(None) => continue,
            Err(e) => return internal("get drive", e),
        };
        if existing.data.bucket_root == bucket.root_blob_name {
            return answer_existing(existing.data, true);
        }
        if let Err(e) = drop_stale_record(&app, existing, &trace_id).await {
            return internal("drop stale drive record", e);
        }
    }
    internal("put drive", "record kept changing under the create")
}

pub async fn list_drives(
    app: Data<Arc<AppState>>,
    req: HttpRequest,
    query: Query<ListQuery>,
) -> HttpResponse {
    let Some(key) = authed_key(&req) else {
        return api_error(StatusCode::UNAUTHORIZED, "unauthorized", "no key");
    };
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let trace_id = TraceId::new();
    let mut drives = match app.list_drives(&trace_id).await {
        Ok(d) => d,
        Err(e) => return internal("list drives", e),
    };
    // One bucket scan beside the drive scan, so a record whose bucket was
    // deleted or recreated is not listed; repair happens on get and create.
    let roots: HashMap<String, String> = match app.list_buckets(trace_id).await {
        Ok(buckets) => buckets
            .into_iter()
            .map(|b| (b.bucket_name, b.root_blob_name))
            .collect(),
        Err(e) => return internal("list buckets", e),
    };
    drives.retain(|d| roots.get(&d.name) == Some(&d.bucket_root));
    drives.retain(|d| key.data.allow_read(&d.name));
    drives.sort_by(|a, b| a.name.cmp(&b.name));
    if let Some(cursor) = &query.cursor {
        drives.retain(|d| d.name.as_str() > cursor.as_str());
    }
    let has_more = drives.len() > limit;
    drives.truncate(limit);
    let next_cursor = has_more
        .then(|| drives.last().map(|d| d.name.clone()))
        .flatten();
    HttpResponse::Ok().json(Page {
        data: drives.into_iter().map(DriveResponse::from).collect(),
        meta: PageMeta {
            has_more,
            next_cursor,
        },
    })
}

pub async fn get_drive(
    app: Data<Arc<AppState>>,
    req: HttpRequest,
    path: Path<String>,
) -> HttpResponse {
    let Some(mut key) = authed_key(&req) else {
        return api_error(StatusCode::UNAUTHORIZED, "unauthorized", "no key");
    };
    let trace_id = TraceId::new();
    let drive = match resolve_ref(&app, &path, &trace_id).await {
        Ok(Some(d)) => d,
        Ok(None) => return not_found(),
        Err(e) => return internal("get drive", e),
    };
    // Unreadable is not found, so names are not enumerable.
    if !allowed(&app, &mut key, &drive.data.name, ApiKey::allow_read).await {
        return not_found();
    }
    match live_bucket(&app, &drive, &trace_id).await {
        Ok(Some(_)) => HttpResponse::Ok().json(DriveResponse::from(drive.data)),
        Ok(None) => not_found(),
        Err(e) => internal("get bucket", e),
    }
}

pub async fn delete_drive(
    app: Data<Arc<AppState>>,
    req: HttpRequest,
    path: Path<String>,
    query: Query<DeleteQuery>,
) -> HttpResponse {
    let Some(mut key) = authed_key(&req) else {
        return api_error(StatusCode::UNAUTHORIZED, "unauthorized", "no key");
    };
    let trace_id = TraceId::new();
    let drive = match resolve_ref(&app, &path, &trace_id).await {
        Ok(Some(d)) => d,
        Ok(None) => return not_found(),
        Err(e) => return internal("get drive", e),
    };
    let name = drive.data.name.clone();
    // Unreadable is not found, as in `get`; a reader that is not an owner is refused.
    if !allowed(&app, &mut key, &name, ApiKey::allow_read).await {
        return not_found();
    }
    if !allowed(&app, &mut key, &name, ApiKey::allow_owner).await {
        return api_error(
            StatusCode::FORBIDDEN,
            "forbidden",
            "not an owner of this drive",
        );
    }
    match live_bucket(&app, &drive, &trace_id).await {
        Ok(Some(_)) => {}
        Ok(None) => return not_found(),
        Err(e) => return internal("get bucket", e),
    }
    info!("delete drive {name}, force {}", query.force);

    if query.force {
        // Destructive: authorize against RSS, not the cached key, so a
        // revoked key cannot start a sweep from this gateway's cache.
        match app
            .refresh_api_key(key.data.key_id.clone(), &trace_id)
            .await
        {
            Ok(fresh) if !fresh.data.is_deleted && fresh.data.allow_owner(&name) => {}
            Ok(_) => {
                return api_error(
                    StatusCode::FORBIDDEN,
                    "forbidden",
                    "key no longer owns this drive",
                );
            }
            Err(e) => return api_error(StatusCode::UNAUTHORIZED, "unauthorized", e.to_string()),
        }
        if drive.data.sweep_alive(unix_ms()) {
            // A sweeper is renewing the lease; do not start a second one.
            return HttpResponse::Accepted().json(DriveResponse::from(drive.data));
        }
        // Registers a worker in this process before taking the lease, so a
        // lease is never held here without a sweeper behind it.
        return match drive_sweep::start(
            app.get_ref().clone(),
            drive.clone(),
            key.data.key_id.clone(),
            &trace_id,
        )
        .await
        {
            Ok(drive_sweep::Start::Spawned(current)) => {
                HttpResponse::Accepted().json(DriveResponse::from(current.data))
            }
            // A worker in this process, or a request elsewhere, holds it; or
            // this request's worker is settling an unknown lease outcome.
            Ok(drive_sweep::Start::Busy | drive_sweep::Start::Settling) => {
                HttpResponse::Accepted().json(DriveResponse::from(drive.data))
            }
            Err(e) => internal("start sweep", e),
        };
    }

    // Fenced on the incarnation this record describes: a bucket recreated
    // under the same name since the check above is never the one deleted.
    match app
        .delete_bucket(&name, &key.data.key_id, &drive.data.bucket_root, trace_id)
        .await
    {
        Ok(()) => {}
        Err(RpcError::InternalResponseError(msg)) if msg.contains("not empty") => {
            return api_error(
                StatusCode::CONFLICT,
                "not_empty",
                "drive has objects; use force",
            );
        }
        // The caller's incarnation is gone either way: deleted through the
        // S3 door, or replaced. Only the record is left to drop.
        Err(RpcError::IncarnationMismatch) => {
            if let Err(e) = drop_stale_record(&app, drive, &trace_id).await {
                return internal("drop stale drive record", e);
            }
            return not_found();
        }
        Err(RpcError::InternalResponseError(msg)) if msg.contains("not found") => {}
        Err(e) => return internal("delete bucket", e),
    }
    match drop_stale_record(&app, drive, &trace_id).await {
        Ok(()) => HttpResponse::NoContent().finish(),
        Err(e) => internal("delete drive record", e),
    }
}
