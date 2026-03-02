use bytes::Bytes;
use data_types::object_layout::ObjectLayout;
use nss_codec::{
    DeleteInodeResponse, GetInodeResponse, ListInodesResponse, PutInodeResponse,
    delete_inode_response, get_inode_response, list_inodes_response, put_inode_response,
};

#[derive(Debug)]
pub enum NssError {
    NotFound,
    AlreadyExists,
    Internal(String),
    Deserialization(String),
}

impl std::fmt::Display for NssError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NssError::NotFound => write!(f, "not found"),
            NssError::AlreadyExists => write!(f, "already exists"),
            NssError::Internal(e) => write!(f, "internal error: {e}"),
            NssError::Deserialization(e) => write!(f, "deserialization error: {e}"),
        }
    }
}

impl std::error::Error for NssError {}

pub struct ListEntry {
    pub key: String,
    pub layout: Option<ObjectLayout>,
}

pub struct ListInodesResult {
    pub entries: Vec<ListEntry>,
    pub has_more: bool,
}

pub fn parse_get_inode(resp: GetInodeResponse) -> Result<ObjectLayout, NssError> {
    let object_bytes = match resp.result.unwrap() {
        get_inode_response::Result::Ok(res) => res,
        get_inode_response::Result::ErrNotFound(()) => {
            return Err(NssError::NotFound);
        }
        get_inode_response::Result::ErrOther(e) => {
            tracing::error!("NSS get_inode error: {e}");
            return Err(NssError::Internal(e));
        }
    };

    rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&object_bytes)
        .map_err(|e| NssError::Deserialization(e.to_string()))
}

pub fn parse_list_inodes(resp: ListInodesResponse) -> Result<ListInodesResult, NssError> {
    let (inodes, has_more) = match resp.result.unwrap() {
        list_inodes_response::Result::Ok(res) => (res.inodes, res.has_more),
        list_inodes_response::Result::Err(e) => {
            tracing::error!("NSS list_inodes error: {e}");
            return Err(NssError::Internal(e));
        }
    };

    let mut entries = Vec::with_capacity(inodes.len());
    for inode in inodes {
        if inode.inode.is_empty() {
            entries.push(ListEntry {
                key: inode.key,
                layout: None,
            });
        } else {
            match rkyv::from_bytes::<ObjectLayout, rkyv::rancor::Error>(&inode.inode) {
                Ok(object) => {
                    let key = inode.key.trim_end_matches('\0').to_string();
                    entries.push(ListEntry {
                        key,
                        layout: Some(object),
                    });
                }
                Err(e) => {
                    tracing::error!(
                        key = %inode.key,
                        inode_len = inode.inode.len(),
                        error = %e,
                        "list entry: rkyv deserialization failed"
                    );
                    return Err(NssError::Deserialization(e.to_string()));
                }
            }
        }
    }
    Ok(ListInodesResult { entries, has_more })
}

pub fn parse_put_inode(resp: PutInodeResponse) -> Result<Bytes, NssError> {
    match resp.result.unwrap() {
        put_inode_response::Result::Ok(res) => Ok(res),
        put_inode_response::Result::Err(e) => {
            tracing::error!("NSS put_inode error: {e}");
            Err(NssError::Internal(e))
        }
    }
}

pub fn parse_delete_inode(resp: DeleteInodeResponse) -> Result<Option<Bytes>, NssError> {
    match resp.result.unwrap() {
        delete_inode_response::Result::Ok(res) => Ok(Some(res)),
        delete_inode_response::Result::ErrNotFound(()) => Ok(None),
        delete_inode_response::Result::ErrAlreadyDeleted(()) => Ok(None),
        delete_inode_response::Result::ErrOther(e) => {
            tracing::error!("NSS delete_inode error: {e}");
            Err(NssError::Internal(e))
        }
    }
}

pub fn mpu_get_part_prefix(mut key: String, part_number: u64) -> String {
    key.push('#');
    // if part number is 0, we treat it as object key
    if part_number != 0 {
        // part numbers range is [1, 10000], which can be encoded as 4 digits
        // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
        let part_str = format!("{:04}", part_number - 1);
        key.push_str(&part_str);
    }
    key
}
