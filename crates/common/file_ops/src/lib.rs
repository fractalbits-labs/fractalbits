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

/// Extract (key, ObjectLayout) pairs from a ListInodesResult,
/// requiring all entries to have non-empty inode data (layouts).
pub fn parse_mpu_parts(result: ListInodesResult) -> Result<Vec<(String, ObjectLayout)>, NssError> {
    let mut parts = Vec::with_capacity(result.entries.len());
    for entry in result.entries {
        match entry.layout {
            Some(layout) => parts.push((entry.key, layout)),
            None => {
                return Err(NssError::Internal(
                    "MPU part has empty inode data".to_string(),
                ));
            }
        }
    }
    Ok(parts)
}

/// Create a minimal directory marker ObjectLayout with size=0.
/// NSS rejects empty values, so we store this sentinel layout for directories.
pub fn create_dir_marker_layout() -> ObjectLayout {
    use data_types::DataBlobGuid;
    use data_types::object_layout::{ObjectCoreMetaData, ObjectMetaData, ObjectState};

    ObjectLayout {
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        version_id: ObjectLayout::gen_version_id(),
        block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
        state: ObjectState::Normal(ObjectMetaData {
            blob_guid: DataBlobGuid {
                blob_id: uuid::Uuid::nil(),
                volume_id: 0,
            },
            core_meta_data: ObjectCoreMetaData {
                size: 0,
                etag: String::new(),
                headers: vec![],
                checksum: None,
            },
        }),
    }
}

/// Enumerate (blob_guid, block_number) pairs that should be deleted for a given ObjectLayout.
/// Returns an empty vec if the layout has no valid blob_guid or num_blocks.
pub fn blob_blocks_to_delete(layout: &ObjectLayout) -> Vec<(data_types::DataBlobGuid, u32)> {
    let blob_guid = match layout.blob_guid() {
        Ok(g) => g,
        Err(_) => return vec![],
    };
    let num_blocks = match layout.num_blocks() {
        Ok(n) => n,
        Err(_) => return vec![],
    };
    (0..num_blocks).map(|i| (blob_guid, i as u32)).collect()
}
