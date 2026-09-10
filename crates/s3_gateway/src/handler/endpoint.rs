use super::{
    bucket::BucketEndpoint,
    common::{
        authorization::Authorization,
        request::extract::{ApiCommand, ApiSignature},
        s3_error::S3Error,
    },
    delete::DeleteEndpoint,
    get::GetEndpoint,
    head::HeadEndpoint,
    post::PostEndpoint,
    put::PutEndpoint,
};
use actix_web::http::Method;

pub enum Endpoint {
    Get(GetEndpoint),
    Put(PutEndpoint),
    Delete(DeleteEndpoint),
    Bucket(BucketEndpoint),
    Post(PostEndpoint),
    Head(HeadEndpoint),
}

impl Endpoint {
    pub fn from_extractors(
        request: &actix_web::HttpRequest,
        bucket_name: &str,
        key: &str,
        api_cmd: Option<ApiCommand>,
        api_sig: ApiSignature,
    ) -> Result<Endpoint, S3Error> {
        // Handle bucket related apis at first
        if key == "/" {
            match *request.method() {
                Method::HEAD => return Ok(Endpoint::Bucket(BucketEndpoint::HeadBucket)),
                Method::PUT => return Ok(Endpoint::Bucket(BucketEndpoint::CreateBucket)),
                Method::DELETE => return Ok(Endpoint::Bucket(BucketEndpoint::DeleteBucket)),
                Method::GET => {
                    // Or it will be list_objects* api, which will be handled in later endpoints
                    if bucket_name.is_empty() {
                        return Ok(Endpoint::Bucket(BucketEndpoint::ListBuckets));
                    }
                }
                Method::POST if api_cmd == Some(ApiCommand::Delete) => {
                    // Handled in later endpoint
                    // (PostEndpoint::DeleteObjects)
                }
                _ => return Err(S3Error::NotImplemented),
            }
        }

        match *request.method() {
            Method::HEAD => Ok(Endpoint::Head(HeadEndpoint::HeadObject)),
            Method::GET => Self::get_endpoint(api_cmd, api_sig, key),
            Method::PUT => Self::put_endpoint(api_cmd, api_sig),
            Method::POST => Self::post_endpoint(api_cmd, api_sig),
            Method::DELETE => Self::delete_endpoint(api_sig),
            _ => Err(S3Error::MethodNotAllowed),
        }
    }

    fn get_endpoint(
        api_cmd: Option<ApiCommand>,
        api_sig: ApiSignature,
        key: &str,
    ) -> Result<Endpoint, S3Error> {
        match (api_cmd, key) {
            (None, "/") => {
                if api_sig.list_type.is_some() {
                    Ok(Endpoint::Get(GetEndpoint::ListObjectsV2))
                } else {
                    Ok(Endpoint::Get(GetEndpoint::ListObjects))
                }
            }
            (None, _) => {
                if api_sig.upload_id.is_some() {
                    Ok(Endpoint::Get(GetEndpoint::ListParts))
                } else {
                    Ok(Endpoint::Get(GetEndpoint::GetObject))
                }
            }
            (Some(ApiCommand::Attributes), _) => {
                Ok(Endpoint::Get(GetEndpoint::GetObjectAttributes))
            }
            (Some(ApiCommand::Uploads), "/") => {
                Ok(Endpoint::Get(GetEndpoint::ListMultipartUploads))
            }
            (Some(_), _) => Err(S3Error::NotImplemented),
        }
    }

    fn put_endpoint(
        api_cmd: Option<ApiCommand>,
        api_sig: ApiSignature,
    ) -> Result<Endpoint, S3Error> {
        match (
            api_cmd,
            api_sig.part_number,
            api_sig.upload_id,
            api_sig.x_amz_copy_source,
        ) {
            (Some(ApiCommand::RenameFolder), None, None, None) => {
                Ok(Endpoint::Put(PutEndpoint::RenameFolder))
            }
            (Some(ApiCommand::RenameObject), None, None, None) => {
                Ok(Endpoint::Put(PutEndpoint::RenameObject))
            }
            (None, Some(part_number), Some(upload_id), None) => Ok(Endpoint::Put(
                PutEndpoint::UploadPart(part_number, upload_id),
            )),
            (None, None, None, Some(_)) => Ok(Endpoint::Put(PutEndpoint::CopyObject)),
            (None, None, None, None) => Ok(Endpoint::Put(PutEndpoint::PutObject)),
            _ => Err(S3Error::NotImplemented),
        }
    }

    fn post_endpoint(
        api_cmd: Option<ApiCommand>,
        api_sig: ApiSignature,
    ) -> Result<Endpoint, S3Error> {
        match (api_cmd, api_sig.upload_id) {
            (Some(ApiCommand::Delete), None) => Ok(Endpoint::Post(PostEndpoint::DeleteObjects)),
            (Some(ApiCommand::Uploads), None) => {
                Ok(Endpoint::Post(PostEndpoint::CreateMultipartUpload))
            }
            (None, Some(upload_id)) => Ok(Endpoint::Post(PostEndpoint::CompleteMultipartUpload(
                upload_id,
            ))),
            (_, _) => Err(S3Error::NotImplemented),
        }
    }

    fn delete_endpoint(api_sig: ApiSignature) -> Result<Endpoint, S3Error> {
        match api_sig.upload_id {
            Some(upload_id) => Ok(Endpoint::Delete(DeleteEndpoint::AbortMultipartUpload(
                upload_id,
            ))),
            None => Ok(Endpoint::Delete(DeleteEndpoint::DeleteObject)),
        }
    }

    /// Get the kind of authorization which is required to perform the operation.
    pub fn authorization_type(&self) -> Authorization {
        match self {
            Endpoint::Get(get_endpoint) => get_endpoint.authorization_type(),
            Endpoint::Put(put_endpoint) => put_endpoint.authorization_type(),
            Endpoint::Delete(delete_endpoint) => delete_endpoint.authorization_type(),
            Endpoint::Bucket(bucket_endpoint) => bucket_endpoint.authorization_type(),
            Endpoint::Post(post_endpoint) => post_endpoint.authorization_type(),
            Endpoint::Head(head_endpoint) => head_endpoint.authorization_type(),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Endpoint::Get(get_endpoint) => get_endpoint.as_str(),
            Endpoint::Put(put_endpoint) => put_endpoint.as_str(),
            Endpoint::Delete(delete_endpoint) => delete_endpoint.as_str(),
            Endpoint::Bucket(bucket_endpoint) => bucket_endpoint.as_str(),
            Endpoint::Post(post_endpoint) => post_endpoint.as_str(),
            Endpoint::Head(head_endpoint) => head_endpoint.as_str(),
        }
    }
}
