use crate::handler::{ObjectRequestContext, common::s3_error::S3Error};
use actix_web::{HttpResponse, web::Query};
use rpc_client_common::nss_rpc_retry;
use serde::Deserialize;
use tracing::{error, info};

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
struct QueryOpts {
    src_path: String,
}

pub async fn rename_object_handler(ctx: ObjectRequestContext) -> Result<HttpResponse, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let QueryOpts { src_path } = Query::<QueryOpts>::from_query(ctx.request.query_string())
        .unwrap_or_else(|_| Query(Default::default()))
        .into_inner();
    let dst_path = ctx.key;
    assert!(!src_path.ends_with('/'));
    assert!(!dst_path.ends_with('/'));

    info!(bucket=%bucket.bucket_name, %src_path, %dst_path, "renaming object in bucket");

    let nss_client = ctx.app.get_nss_rpc_client();
    nss_rpc_retry!(
        nss_client,
        rename_object(
            &bucket.root_blob_name,
            &src_path,
            &dst_path,
            Some(ctx.app.config.rpc_request_timeout()),
            &ctx.trace_id
        )
    )
    .await
    .map_err(|e| {
        error!(bucket=%bucket.bucket_name, %src_path, %dst_path, error=%e, "failed to rename object");
        S3Error::InternalError
    })?;

    Ok(HttpResponse::Ok().finish())
}
