mod abort_multipart_upload;
mod delete_object;

pub use abort_multipart_upload::abort_multipart_upload_handler;
pub use delete_object::delete_object_handler;

use super::common::authorization::Authorization;

pub enum DeleteEndpoint {
    AbortMultipartUpload(String),
    DeleteObject,
}

impl DeleteEndpoint {
    pub fn authorization_type(&self) -> Authorization {
        match self {
            DeleteEndpoint::AbortMultipartUpload(_) => Authorization::Write,
            DeleteEndpoint::DeleteObject => Authorization::Write,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DeleteEndpoint::AbortMultipartUpload(_) => "AbortMultipartUpload",
            DeleteEndpoint::DeleteObject => "DeleteObject",
        }
    }
}
