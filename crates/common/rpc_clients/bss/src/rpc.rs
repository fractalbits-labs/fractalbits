use std::sync::Arc;
use std::time::Duration;

use crate::client::RpcClient;
use crate::stats::OperationType;
use bss_codec::{
    Command, ListBlobBlocksRequest, ListBlobBlocksResponse, ListBlobsRequest, ListBlobsResponse,
    MessageHeader, list_blob_blocks_response, list_blobs_response,
};
use bytes::Bytes;
use data_types::{DataBlobGuid, TraceId};
use rpc_client_common::MessageFrame;
use rpc_client_common::{InflightRpcGuard, ProtobufRequestHeader, ProtobufRpc, RpcError, rpc_ctx};
use tracing::error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataBlobMetadata {
    pub version: u64,
    /// Zero denotes a non-EC entry.
    pub cohort_tag: u64,
}

/// Check the errno field in the response header and return appropriate error
pub(crate) fn check_response_errno(header: &MessageHeader) -> Result<(), RpcError> {
    // errno codes from core/common/rpc/rpc_error.zig
    match header.errno {
        0 => Ok(()), // OK
        1 => Err(RpcError::InternalResponseError(
            "BSS returned InternalError".to_string(),
        )),
        2 => Err(RpcError::NotFound),
        3 => Err(RpcError::ChecksumMismatch), // Corrupted
        4 => Err(RpcError::Retry),            // SlowDown
        5 => Err(RpcError::InternalResponseError(
            "BSS returned ShutDown".to_string(),
        )),
        6 => Err(RpcError::InternalResponseError(
            "BSS returned TokenExpired".to_string(),
        )),
        7 => Err(RpcError::InternalResponseError(
            "BSS returned DeviceMismatch".to_string(),
        )),
        8 => Err(RpcError::VersionSkipped), // Write skipped due to version check
        10 => Err(RpcError::NoSpace),       // Space-map allocation failed
        11 => Err(RpcError::Mismatch),      // Write-once key violation
        code => Err(RpcError::InternalResponseError(format!(
            "Unknown BSS error code: {}",
            code
        ))),
    }
}

fn parse_list_blobs_response(
    resp: ListBlobsResponse,
) -> Result<list_blobs_response::Blobs, RpcError> {
    match resp.result {
        Some(list_blobs_response::Result::Ok(blobs)) => Ok(blobs),
        Some(list_blobs_response::Result::Err(err)) => Err(RpcError::InternalResponseError(err)),
        None => Err(RpcError::InternalResponseError(
            "BSS ListBlobs response missing result".to_string(),
        )),
    }
}

pub struct BlobListStream {
    client: Arc<RpcClient>,
    volume_id: u16,
    prefix: String,
    marker: String,
    max_keys: u32,
    include_deleted: bool,
    done: bool,
}

impl BlobListStream {
    pub fn new(
        client: Arc<RpcClient>,
        volume_id: u16,
        prefix: impl Into<String>,
        start_after: impl Into<String>,
        max_keys: u32,
        include_deleted: bool,
    ) -> Self {
        Self {
            client,
            volume_id,
            prefix: prefix.into(),
            marker: start_after.into(),
            max_keys,
            include_deleted,
            done: false,
        }
    }

    pub async fn next_batch(
        &mut self,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<Option<list_blobs_response::Blobs>, RpcError> {
        if self.done {
            return Ok(None);
        }

        let page = self
            .client
            .list_data_blobs(
                self.volume_id,
                &self.prefix,
                &self.marker,
                self.max_keys,
                timeout,
                trace_id,
                retry_count,
                self.include_deleted,
            )
            .await?;

        if let Some(last) = page.blobs.last() {
            self.marker = last.key.clone();
        }
        self.done = !page.has_more;
        Ok(Some(page))
    }
}

impl RpcClient {
    #[allow(clippy::too_many_arguments)]
    pub async fn list_data_blobs(
        &self,
        volume_id: u16,
        prefix: &str,
        start_after: &str,
        max_keys: u32,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
        include_deleted: bool,
    ) -> Result<list_blobs_response::Blobs, RpcError> {
        let body = ListBlobsRequest {
            max_keys,
            prefix: prefix.to_string(),
            start_after: start_after.to_string(),
            include_deleted,
        };
        let header = MessageHeader {
            volume_id,
            ..Default::default()
        };
        let resp: ListBlobsResponse = self
            .call_with_header(
                header,
                Command::ListBlobs as i32,
                "list_data_blobs",
                body,
                timeout,
                trace_id,
                retry_count,
                rpc_ctx!(volume_id, prefix),
            )
            .await?;
        parse_list_blobs_response(resp)
    }

    /// Enumerate the BSS-visible block entries for one blob over
    /// `[first_block, first_block + block_count)`. Absent blocks are holes.
    pub async fn list_blob_blocks(
        &self,
        blob_guid: DataBlobGuid,
        first_block: u32,
        block_count: u32,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<Vec<list_blob_blocks_response::BlobBlockEntry>, RpcError> {
        let header = MessageHeader {
            blob_id: blob_guid.blob_id.into_bytes(),
            volume_id: blob_guid.volume_id,
            ..Default::default()
        };
        let mut marker = String::new();
        let mut entries = Vec::new();
        loop {
            let body = ListBlobBlocksRequest {
                first_block,
                block_count,
                marker: marker.clone(),
            };
            let resp: ListBlobBlocksResponse = self
                .call_with_header(
                    header,
                    Command::ListBlobBlocks as i32,
                    "list_blob_blocks",
                    body,
                    timeout,
                    trace_id,
                    retry_count,
                    rpc_ctx!(blob_guid, first_block, block_count),
                )
                .await?;
            let blocks = match resp.result {
                Some(list_blob_blocks_response::Result::Ok(blocks)) => blocks,
                Some(list_blob_blocks_response::Result::Err(err)) => {
                    return Err(RpcError::InternalResponseError(err));
                }
                None => {
                    return Err(RpcError::InternalResponseError(
                        "BSS ListBlobBlocks response missing result".to_string(),
                    ));
                }
            };
            entries.extend(blocks.blocks);
            if !blocks.has_more {
                return Ok(entries);
            }
            if blocks.next_marker.is_empty() || blocks.next_marker == marker {
                return Err(RpcError::InternalResponseError(
                    "BSS ListBlobBlocks pagination made no progress".to_string(),
                ));
            }
            marker = blocks.next_marker;
        }
    }

    /// Raw-header round trip shared by the data and metadata blob RPCs. The caller fills the
    /// addressing and body fields; this fills the shared request fields, sends, and checks the
    /// response errno. `ctx` is only invoked when a non-retryable failure is logged.
    #[allow(clippy::too_many_arguments)]
    async fn send_raw(
        &self,
        mut header: MessageHeader,
        command: Command,
        name: &'static str,
        body: Vec<Bytes>,
        op: Option<OperationType>,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
        ctx: impl FnOnce() -> String,
    ) -> Result<MessageFrame<MessageHeader>, RpcError> {
        let _guard = InflightRpcGuard::new(Self::RPC_TYPE, name);
        let request_id = self.gen_request_id();
        header.set_request(request_id, command as i32, retry_count as u8, trace_id);
        let body_len: usize = body.iter().map(|c| c.len()).sum();
        header.size = (size_of::<MessageHeader>() + body_len) as u32;
        let frame = MessageFrame::new(header, body);
        let resp_frame = self
            .send_request_vectored(frame, timeout, op)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc = %name, %request_id, ctx = %ctx(), error = ?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
        Ok(resp_frame)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn put_data_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        body_checksum: u64,
        version: u64,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        self.put_data_blob_inner(
            blob_guid,
            block_number,
            body,
            body_checksum,
            version,
            timeout,
            trace_id,
            retry_count,
            0,
        )
        .await
    }

    /// Store one EC shard with the cohort shared by its stripe.
    #[allow(clippy::too_many_arguments)]
    pub async fn put_data_blob_with_cohort(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        body_checksum: u64,
        version: u64,
        cohort_tag: u64,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        self.put_data_blob_inner(
            blob_guid,
            block_number,
            body,
            body_checksum,
            version,
            timeout,
            trace_id,
            retry_count,
            cohort_tag,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn put_data_blob_inner(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: Bytes,
        body_checksum: u64,
        version: u64,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
        cohort_tag: u64,
    ) -> Result<(), RpcError> {
        let mut header = MessageHeader {
            blob_id: blob_guid.blob_id.into_bytes(),
            volume_id: blob_guid.volume_id,
            block_number,
            body_len: body.len() as u32,
            checksum_body: body_checksum,
            version,
            ..Default::default()
        };
        header.set_data_cohort_tag(cohort_tag);
        self.send_raw(
            header,
            Command::PutDataBlob,
            "put_data_blob",
            vec![body],
            Some(OperationType::PutData),
            timeout,
            trace_id,
            retry_count,
            rpc_ctx!(blob_guid, block_number),
        )
        .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn put_data_blob_vectored(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        chunks: Vec<Bytes>,
        body_checksum: u64,
        version: u64,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let header = MessageHeader {
            blob_id: blob_guid.blob_id.into_bytes(),
            volume_id: blob_guid.volume_id,
            block_number,
            body_len: chunks.iter().map(|c| c.len()).sum::<usize>() as u32,
            checksum_body: body_checksum,
            version,
            ..Default::default()
        };
        self.send_raw(
            header,
            Command::PutDataBlob,
            "put_data_blob_vectored",
            chunks,
            Some(OperationType::PutData),
            timeout,
            trace_id,
            retry_count,
            rpc_ctx!(blob_guid, block_number),
        )
        .await?;
        Ok(())
    }

    /// Issue a GetDataBlob RPC and return the BSS-reported `version` of the
    /// returned block alongside the body. Callers that need read-side
    /// version arbitration (see `DataVgProxy::get_blob`) compare this
    /// against an expected version to detect lagging-replica reads.
    /// Exact-identity read: data keys are versioned
    /// (`/d{vol}/{uuid}-p{block}-v{version}`), so the request must name the
    /// generation to fetch; there is no "latest version" read.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_data_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        body: &mut Bytes,
        content_len: usize,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<DataBlobMetadata, RpcError> {
        let header = MessageHeader {
            blob_id: blob_guid.blob_id.into_bytes(),
            volume_id: blob_guid.volume_id,
            block_number,
            body_len: content_len as u32,
            version,
            ..Default::default()
        };
        let resp_frame = self
            .send_raw(
                header,
                Command::GetDataBlob,
                "get_data_blob",
                Vec::new(),
                Some(OperationType::GetData),
                timeout,
                trace_id,
                retry_count,
                rpc_ctx!(blob_guid, block_number),
            )
            .await?;
        let metadata = DataBlobMetadata {
            version: resp_frame.header.version,
            cohort_tag: resp_frame.header.data_cohort_tag(),
        };
        *body = resp_frame.body;
        // Block-size padding (override flush) stores every block at full
        // block_size, so a reader that knows the logical content length
        // gets a body that is >= what it asked for and clamps locally.
        // Strict equality would reject that padded view; only a body
        // strictly shorter than requested is a real underread (BSS lost
        // bytes). content_len == 0 means "give me whatever you have".
        if content_len != 0 && body.len() < content_len {
            return Err(RpcError::InternalResponseError(format!(
                "BSS returned body length {} but client expected at least {}",
                body.len(),
                content_len
            )));
        }
        Ok(metadata)
    }

    pub async fn delete_data_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let header = MessageHeader {
            blob_id: blob_guid.blob_id.into_bytes(),
            volume_id: blob_guid.volume_id,
            block_number,
            version,
            ..Default::default()
        };
        self.send_raw(
            header,
            Command::DeleteDataBlob,
            "delete_data_blob",
            Vec::new(),
            Some(OperationType::DeleteData),
            timeout,
            trace_id,
            retry_count,
            rpc_ctx!(blob_guid, block_number),
        )
        .await?;
        Ok(())
    }

    pub async fn get_metadata_blob(
        &self,
        blob_id: [u8; 16],
        volume_id: u16,
        content_len: usize,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<Bytes, RpcError> {
        let header = MessageHeader {
            blob_id,
            volume_id,
            skip_fence_token: 1,
            body_len: content_len as u32,
            ..Default::default()
        };
        let resp_frame = self
            .send_raw(
                header,
                Command::GetMetadataBlob,
                "get_metadata_blob",
                Vec::new(),
                None,
                timeout,
                trace_id,
                retry_count,
                rpc_ctx!(volume_id),
            )
            .await?;
        let body = resp_frame.body;
        if content_len != body.len() {
            return Err(RpcError::InternalResponseError(format!(
                "BSS returned body length {} but client expected {}",
                body.len(),
                content_len
            )));
        }
        Ok(body)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn put_metadata_blob(
        &self,
        blob_id: [u8; 16],
        volume_id: u16,
        body: Bytes,
        body_checksum: u64,
        version: u64,
        is_new: bool,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let header = MessageHeader {
            blob_id,
            volume_id,
            body_len: body.len() as u32,
            version,
            is_new: if is_new { 1 } else { 0 },
            skip_fence_token: 1,
            checksum_body: body_checksum,
            ..Default::default()
        };
        self.send_raw(
            header,
            Command::PutMetadataBlob,
            "put_metadata_blob",
            vec![body],
            None,
            timeout,
            trace_id,
            retry_count,
            rpc_ctx!(volume_id),
        )
        .await?;
        Ok(())
    }

    pub async fn delete_metadata_blob(
        &self,
        blob_id: [u8; 16],
        volume_id: u16,
        version: u64,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let header = MessageHeader {
            blob_id,
            volume_id,
            is_deleted: 1,
            version,
            skip_fence_token: 1,
            ..Default::default()
        };
        self.send_raw(
            header,
            Command::DeleteMetadataBlob,
            "delete_metadata_blob",
            Vec::new(),
            None,
            timeout,
            trace_id,
            retry_count,
            rpc_ctx!(volume_id),
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::BlobListStream;
    use crate::client::RpcClient;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn blob_list_stream_tracks_done_for_empty_terminal_page() {
        let client = Arc::new(RpcClient::new_from_address(
            "127.0.0.1:1".to_string(),
            Duration::from_secs(1),
        ));
        let stream = BlobListStream::new(client, 1, "/d1/", "", 1000, false);

        assert_eq!(stream.marker, "");
        assert!(!stream.done);
        assert_eq!(stream.prefix, "/d1/");
        assert_eq!(stream.max_keys, 1000);
    }
}
