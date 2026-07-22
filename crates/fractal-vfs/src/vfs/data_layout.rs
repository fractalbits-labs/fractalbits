//! Data-layout support checks shared by open and namespace paths.

#[allow(unused_imports)]
use super::*;

impl VfsCore {
    /// Reject layouts whose data lives on the S3 hybrid volume: the FUSE
    /// data path only speaks the BSS block protocol. Indirect (hardlink)
    /// layouts are resolved to the shared record first.
    pub(crate) async fn ensure_data_layout_supported(
        &self,
        _key: &str,
        layout: &ObjectLayout,
        trace_id: &TraceId,
    ) -> Result<(), FsError> {
        let resolved_layout;
        let layout = if let ObjectState::Indirect(redirect) = &layout.state {
            resolved_layout = self
                .backend()
                .get_inode_record(redirect.inode_id, trace_id)
                .await?
                .layout;
            &resolved_layout
        } else {
            layout
        };
        if let ObjectState::Normal(_) = &layout.state
            && layout.blob_guid()?.volume_id == DataBlobGuid::S3_VOLUME
        {
            return Err(FsError::InvalidState);
        }
        Ok(())
    }
}
