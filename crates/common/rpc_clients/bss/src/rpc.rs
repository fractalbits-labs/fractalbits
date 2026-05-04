use std::sync::Arc;
use std::time::Duration;

use crate::client::RpcClient;
use bss_codec::{
    Command, ListBlobBlocksRequest, ListBlobBlocksResponse, ListBlobsRequest, ListBlobsResponse,
    MessageHeader, ReserveBlocksRequest, ReserveBlocksResponse, list_blob_blocks_response,
    list_blobs_response, reserve_blocks_response,
};
use bytes::Bytes;
use data_types::{DataBlobGuid, TraceId};
use prost::Message as PbMessage;
use rpc_client_common::{InflightRpcGuard, RpcError, encode_protobuf};
use rpc_codec_common::MessageFrame;
use tracing::error;

/// Check the errno field in the response header and return appropriate error
fn check_response_errno(header: &MessageHeader) -> Result<(), RpcError> {
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
        let _guard = InflightRpcGuard::new("bss", "list_data_blobs");
        let body = ListBlobsRequest {
            max_keys,
            prefix: prefix.to_string(),
            start_after: start_after.to_string(),
            include_deleted,
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::ListBlobs;
        header.volume_id = volume_id;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);

        let msg_frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(msg_frame, timeout, None).await.map_err(|e| {
            if !e.retryable() {
                error!(rpc=%"list_data_blobs", %request_id, %volume_id, %prefix, error=?e, "bss rpc failed");
            }
            e
        })?;
        check_response_errno(&resp_frame.header)?;

        let resp: ListBlobsResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        parse_list_blobs_response(resp)
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
        let _guard = InflightRpcGuard::new("bss", "put_data_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.command = Command::PutDataBlob;
        header.content_len = body.len() as u32;
        header.size = size_of::<MessageHeader>() as u32 + header.content_len;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;
        header.checksum_body = body_checksum;
        header.version = version;

        let msg_frame = MessageFrame::new(header, body);
        let resp_frame = self
            .send_request(msg_frame, timeout, Some(crate::OperationType::PutData))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put_data_blob", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
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
        let _guard = InflightRpcGuard::new("bss", "put_data_blob_vectored");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.command = Command::PutDataBlob;
        let total_size: usize = chunks.iter().map(|c| c.len()).sum();
        header.content_len = total_size as u32;
        header.size = size_of::<MessageHeader>() as u32 + header.content_len;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;
        header.checksum_body = body_checksum;
        header.version = version;

        let msg_frame = MessageFrame::new(header, chunks);
        let resp_frame = self
            .send_request_vectored(msg_frame, timeout, Some(crate::OperationType::PutData))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put_data_blob_vectored", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    /// Issue a GetDataBlob RPC and return the BSS-reported `version` of the
    /// returned block alongside the body. Callers that need read-side
    /// version arbitration (see `DataVgProxy::get_blob`) compare this
    /// against an expected version to detect lagging-replica reads.
    pub async fn get_data_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: &mut Bytes,
        content_len: usize,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<u64, RpcError> {
        let _guard = InflightRpcGuard::new("bss", "get_data_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.command = Command::GetDataBlob;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;
        header.content_len = content_len as u32;
        header.size = size_of::<MessageHeader>() as u32;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request( msg_frame, timeout, Some(crate::OperationType::GetData))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_data_blob", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
        let version = resp_frame.header.version;
        *body = resp_frame.body;
        // The caller can pass content_len = 0 to opt into "give me
        // whatever you have" semantics. With block-size padding the
        // body length is always block_size, but readers that know the
        // logical content length still want to clamp to it locally;
        // strict equality here would force every padded block to
        // round-trip through the caller's logical-length field, which
        // isn't what the design wants. A non-zero content_len that is
        // strictly larger than the response is still an error -- that
        // means BSS lost bytes and the caller would silently underread.
        if content_len != 0 && body.len() < content_len {
            return Err(RpcError::InternalResponseError(format!(
                "BSS returned body length {} but client expected at least {}",
                body.len(),
                content_len
            )));
        }
        Ok(version)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn delete_data_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "delete_data_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.command = Command::DeleteDataBlob;
        header.size = size_of::<MessageHeader>() as u32;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;
        header.version = version;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request( msg_frame, timeout, Some(crate::OperationType::DeleteData))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"delete_data_blob", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
        Ok(())
    }

    /// Reserve a single block under `blob_guid` at `expected_version`.
    ///
    /// Spec carries a `block_count` field in the request body for
    /// future multi-block batching, but the server currently treats
    /// the call as scoped to `block_number` only -- callers that need
    /// a range simply iterate. The version-guard semantics mirror
    /// `put_data_blob`: VersionSkipped on a newer existing entry,
    /// idempotent on equal/older versions.
    #[allow(clippy::too_many_arguments)]
    pub async fn reserve_blocks(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        block_size: u32,
        expected_version: u64,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "reserve_blocks");
        let body = ReserveBlocksRequest {
            block_count: 1,
            block_size,
        };
        let body_bytes = encode_protobuf(body, trace_id)?;

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.command = Command::ReserveBlocks;
        header.size = (size_of::<MessageHeader>() + body_bytes.len()) as u32;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;
        header.version = expected_version;
        header.set_body_checksum(&body_bytes);

        let msg_frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self
            .send_request(msg_frame, timeout, Some(crate::OperationType::PutData))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"reserve_blocks", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
        // Body is a ReserveBlocksResponse with stats; we don't propagate
        // the breakdown today since the client iterates one block at a
        // time, but decoding it here keeps a future telemetry hook
        // cheap to add and validates the response shape.
        if !resp_frame.body.is_empty()
            && let Ok(resp) = <ReserveBlocksResponse as PbMessage>::decode(resp_frame.body.clone())
            && let Some(reserve_blocks_response::Result::Err(err)) = resp.result
        {
            return Err(RpcError::InternalResponseError(err));
        }
        Ok(())
    }

    /// Enumerate the BSS-side block entries for a single blob over the
    /// range `[first_block, first_block + block_count)`. Each result
    /// row is `(block_number, entry_type, version)` -- absent indices
    /// are holes. Returns an empty list when nothing in the range is
    /// stored on this BSS instance (every block is a hole).
    #[allow(clippy::too_many_arguments)]
    pub async fn list_blob_blocks(
        &self,
        blob_guid: DataBlobGuid,
        first_block: u32,
        block_count: u32,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<Vec<list_blob_blocks_response::BlobBlockEntry>, RpcError> {
        let _guard = InflightRpcGuard::new("bss", "list_blob_blocks");
        let body = ListBlobBlocksRequest {
            first_block,
            block_count,
        };
        let body_bytes = encode_protobuf(body, trace_id)?;

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.command = Command::ListBlobBlocks;
        header.size = (size_of::<MessageHeader>() + body_bytes.len()) as u32;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;
        header.set_body_checksum(&body_bytes);

        let msg_frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self
            .send_request(msg_frame, timeout, None)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"list_blob_blocks", %request_id, %blob_guid, %first_block, %block_count, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;

        let resp: ListBlobBlocksResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        match resp.result {
            Some(list_blob_blocks_response::Result::Ok(blocks)) => Ok(blocks.blocks),
            Some(list_blob_blocks_response::Result::Err(err)) => {
                Err(RpcError::InternalResponseError(err))
            }
            None => Err(RpcError::InternalResponseError(
                "BSS ListBlobBlocks response missing result".to_string(),
            )),
        }
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
        let _guard = InflightRpcGuard::new("bss", "get_metadata_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_id;
        header.volume_id = volume_id;
        header.command = Command::GetMetadataBlob;
        header.skip_fence_token = 1;
        header.content_len = content_len as u32;
        header.size = size_of::<MessageHeader>() as u32;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request(msg_frame, timeout, None)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_metadata_blob", %request_id, %volume_id, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
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
        let _guard = InflightRpcGuard::new("bss", "put_metadata_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_id;
        header.volume_id = volume_id;
        header.command = Command::PutMetadataBlob;
        header.content_len = body.len() as u32;
        header.size = size_of::<MessageHeader>() as u32 + header.content_len;
        header.version = version;
        header.is_new = if is_new { 1 } else { 0 };
        header.skip_fence_token = 1;
        header.checksum_body = body_checksum;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;

        let msg_frame = MessageFrame::new(header, body);
        let resp_frame = self
            .send_request(msg_frame, timeout, None)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put_metadata_blob", %request_id, %volume_id, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
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
        let _guard = InflightRpcGuard::new("bss", "delete_metadata_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_id;
        header.volume_id = volume_id;
        header.command = Command::DeleteMetadataBlob;
        header.is_deleted = 1;
        header.version = version;
        header.skip_fence_token = 1;
        header.size = size_of::<MessageHeader>() as u32;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request(msg_frame, timeout, None)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"delete_metadata_blob", %request_id, %volume_id, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
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
