mod block_data_stream;
mod copy_object;
mod put_object;
mod rename_folder;
mod rename_object;
mod s3_streaming;
mod upload_part;

pub use copy_object::copy_object_handler;
pub use put_object::put_object_handler;
pub use rename_folder::rename_folder_handler;
pub use rename_object::rename_object_handler;
pub use upload_part::upload_part_handler;

use super::common::authorization::Authorization;

pub enum PutEndpoint {
    CopyObject,
    PutObject,
    UploadPart(u64, String),
    RenameFolder,
    RenameObject,
}

impl PutEndpoint {
    pub fn authorization_type(&self) -> Authorization {
        match self {
            PutEndpoint::CopyObject => Authorization::Write,
            PutEndpoint::PutObject => Authorization::Write,
            PutEndpoint::RenameFolder => Authorization::Write,
            PutEndpoint::RenameObject => Authorization::Write,
            PutEndpoint::UploadPart(..) => Authorization::Write,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            PutEndpoint::CopyObject => "CopyObject",
            PutEndpoint::PutObject => "PutObject",
            PutEndpoint::RenameFolder => "RenameFolder",
            PutEndpoint::RenameObject => "RenameObject",
            PutEndpoint::UploadPart(..) => "UploadPart",
        }
    }
}
