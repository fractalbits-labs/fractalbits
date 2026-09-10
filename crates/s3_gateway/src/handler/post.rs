mod complete_multipart_upload;
mod create_multipart_upload;
mod delete_objects;

pub use complete_multipart_upload::complete_multipart_upload_handler;
pub use create_multipart_upload::create_multipart_upload_handler;
pub use delete_objects::delete_objects_handler;

use super::common::authorization::Authorization;

pub enum PostEndpoint {
    CompleteMultipartUpload(String),
    CreateMultipartUpload,
    DeleteObjects,
}

impl PostEndpoint {
    pub fn authorization_type(&self) -> Authorization {
        match self {
            PostEndpoint::CompleteMultipartUpload(_) => Authorization::Write,
            PostEndpoint::CreateMultipartUpload => Authorization::Write,
            PostEndpoint::DeleteObjects => Authorization::Write,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            PostEndpoint::CompleteMultipartUpload(_) => "CompleteMultipartUpload",
            PostEndpoint::CreateMultipartUpload => "CreateMultipartUpload",
            PostEndpoint::DeleteObjects => "DeleteObjects",
        }
    }
}
