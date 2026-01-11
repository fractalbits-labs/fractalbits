use actix_web::{
    HttpResponse, Result,
    web::{Data, Json, Path},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::{info, warn};

use crate::AppState;

#[derive(Debug, Serialize, Deserialize)]
pub struct CacheInvalidationResponse {
    pub status: String,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AzStatusUpdateRequest {
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NssAddressUpdateRequest {
    pub address: String,
}

/// Invalidate a specific bucket from the cache
pub async fn invalidate_bucket(
    app: Data<Arc<AppState>>,
    path: Path<String>,
) -> Result<HttpResponse> {
    let bucket_name = path.into_inner();
    info!("Invalidating bucket cache for: {}", bucket_name);

    let cache_key = format!("bucket:{}", bucket_name);
    app.cache_coordinator.invalidate_entry(&cache_key).await;

    let response = CacheInvalidationResponse {
        status: "success".to_string(),
        message: format!("Bucket '{}' cache invalidated", bucket_name),
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Invalidate a specific API key from the cache
pub async fn invalidate_api_key(
    app: Data<Arc<AppState>>,
    path: Path<String>,
) -> Result<HttpResponse> {
    let key_id = path.into_inner();
    info!("Invalidating API key cache for: {}", key_id);

    let cache_key = format!("api_key:{}", key_id);
    app.cache_coordinator.invalidate_entry(&cache_key).await;

    let response = CacheInvalidationResponse {
        status: "success".to_string(),
        message: format!("API key '{}' cache invalidated", key_id),
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Update az_status cache for a specific AZ with new status value
pub async fn update_az_status(
    app: Data<Arc<AppState>>,
    path: Path<String>,
    request: Json<AzStatusUpdateRequest>,
) -> Result<HttpResponse> {
    let az_id = path.into_inner();
    info!(
        "Updating az_status cache for: {} with status: {}",
        az_id, request.status
    );

    if !app.az_status_enabled.load(Ordering::Acquire) {
        let response = CacheInvalidationResponse {
            status: "error".to_string(),
            message: "AZ status cache not available for this storage backend".to_string(),
        };

        return Ok(HttpResponse::NotFound().json(response));
    }

    let cache_key = format!("az_status:{}", az_id);
    app.az_status_coordinator
        .insert(&cache_key, request.status.clone())
        .await;

    let response = CacheInvalidationResponse {
        status: "success".to_string(),
        message: format!(
            "AZ status cache for '{}' updated to '{}'",
            az_id, request.status
        ),
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Clear the entire cache
pub async fn clear_cache(app: Data<Arc<AppState>>) -> Result<HttpResponse> {
    warn!("Clearing entire cache");

    // Invalidate all entries in the cache
    app.cache_coordinator.invalidate_all();
    if app.az_status_enabled.load(Ordering::Acquire) {
        app.az_status_coordinator.invalidate_all();
    }

    let response = CacheInvalidationResponse {
        status: "success".to_string(),
        message: "All cache entries cleared".to_string(),
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Update NSS address for this api_server instance
pub async fn update_nss_address(
    app: Data<Arc<AppState>>,
    request: Json<NssAddressUpdateRequest>,
) -> Result<HttpResponse> {
    info!("Received NSS address update request: {}", request.address);

    app.update_nss_address(request.address.clone()).await;

    let response = CacheInvalidationResponse {
        status: "success".to_string(),
        message: format!("NSS address updated to '{}'", request.address),
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Health check endpoint for management API
pub async fn mgmt_health(app: Data<Arc<AppState>>) -> Result<HttpResponse> {
    let trace_id = data_types::TraceId::new();
    let nss_ready = app.ensure_nss_client_initialized(&trace_id).await;

    if nss_ready {
        Ok(HttpResponse::Ok().json(serde_json::json!({
            "status": "healthy",
            "service": "api_server",
            "nss_connected": true
        })))
    } else {
        Ok(HttpResponse::ServiceUnavailable().json(serde_json::json!({
            "status": "unhealthy",
            "service": "api_server",
            "nss_connected": false,
            "message": "NSS client not initialized"
        })))
    }
}
