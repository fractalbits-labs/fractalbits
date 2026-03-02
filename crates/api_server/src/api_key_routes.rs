use ntex::web::HttpResponse;
use ntex::web::types::{Json, Path, State};
use serde::{Deserialize, Serialize};
use std::rc::Rc;
use tracing::{error, info};

use crate::AppState;
use crate::handler::common::s3_error::S3Error;
use data_types::{ApiKey, TraceId, Versioned};

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct ApiKeyResponse {
    pub key_id: String,
    pub secret_key: String,
    pub name: String,
}

impl From<ApiKey> for ApiKeyResponse {
    fn from(api_key: ApiKey) -> Self {
        ApiKeyResponse {
            key_id: api_key.key_id,
            secret_key: api_key.secret_key,
            name: api_key.name,
        }
    }
}

pub async fn create_api_key(
    app: State<Rc<AppState>>,
    payload: Json<CreateApiKeyRequest>,
) -> Result<HttpResponse, S3Error> {
    info!("Creating API key with name: {}", payload.name);
    let trace_id = TraceId::new();
    let api_key = Versioned::new(0, ApiKey::new(&payload.name));
    let _key_id = api_key.data.key_id.clone();
    let _serialized_api_key = serde_json::to_string(&api_key.data).map_err(|e| {
        error!("Failed to serialize API key: {:?}", e);
        S3Error::InternalError
    })?;

    app.put_api_key(&api_key, &trace_id).await.map_err(|e| {
        error!("Failed to put API key to RSS: {:?}", e);
        S3Error::InternalError
    })?;

    Ok(HttpResponse::Ok().json(&api_key.data))
}

pub async fn delete_api_key(
    app: State<Rc<AppState>>,
    path: Path<String>,
) -> Result<HttpResponse, S3Error> {
    let key_id = path.into_inner().trim_start_matches("api_key:").to_string();
    info!("Deleting API key with key_id: {}", key_id);
    let trace_id = TraceId::new();
    let api_key = app.get_api_key(key_id, &trace_id).await.map_err(|e| {
        error!("Failed to get API key from RSS: {e:?}");
        S3Error::InternalError
    })?;
    app.delete_api_key(&api_key.data, &trace_id)
        .await
        .map_err(|e| {
            error!("Failed to delete API key from RSS: {e:?}");
            S3Error::InternalError
        })?;

    Ok(HttpResponse::NoContent().finish())
}

pub async fn list_api_keys(app: State<Rc<AppState>>) -> Result<HttpResponse, S3Error> {
    info!("Listing API keys");
    let trace_id = TraceId::new();
    let api_keys = app.list_api_keys(&trace_id).await.map_err(|e| {
        error!("Failed to list API keys from RSS: {:?}", e);
        S3Error::InternalError
    })?;

    let mut api_key_responses: Vec<ApiKeyResponse> = Vec::new();
    for api_key in api_keys {
        api_key_responses.push(api_key.into());
    }

    Ok(HttpResponse::Ok().json(&api_key_responses))
}
