use crate::handler::{
    ObjectRequestContext,
    common::{get_raw_object, object_headers, s3_error::S3Error},
    get::{GetObjectHeaderOpts, GetObjectQueryOpts, override_headers},
};
use actix_web::{HttpResponse, web::Query};
use futures_util::{StreamExt as _, stream};

pub async fn head_object_handler(ctx: ObjectRequestContext) -> Result<HttpResponse, S3Error> {
    let bucket = ctx.resolve_bucket().await?;
    let query_opts = Query::<GetObjectQueryOpts>::from_query(ctx.request.query_string())
        .map_err(|_| S3Error::UnsupportedArgument)?;

    // Extract header options from headers
    let header_opts = GetObjectHeaderOpts::from_headers(ctx.request.headers())?;
    let checksum_mode_enabled = header_opts.x_amz_checksum_mode_enabled;

    // Get the raw object
    let obj = get_raw_object(
        &ctx.app,
        &bucket.routing_key,
        &bucket.root_blob_name,
        &ctx.key,
        &ctx.trace_id,
    )
    .await?;

    // Build the response with proper headers
    let mut response = HttpResponse::Ok();
    object_headers(&mut response, &obj, checksum_mode_enabled)?;
    override_headers(&mut response, &query_opts)?;

    let object_size = obj.size()?;
    Ok(response
        .no_chunking(object_size)
        .body(actix_web::body::SizedStream::new(
            object_size,
            stream::empty::<Result<_, std::io::Error>>().boxed_local(),
        )))
}
