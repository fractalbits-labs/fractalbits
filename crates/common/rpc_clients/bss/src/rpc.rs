use std::sync::Arc;
use std::time::Duration;

use crate::client::RpcClient;
use bss_codec::{
    Command, GetBlobAtOrBeforeRequest, ListBlobBlocksRequest, ListBlobBlocksResponse,
    ListBlobsRequest, ListBlobsResponse, MessageHeader, ReserveBlocksRequest,
    ReserveBlocksResponse, list_blob_blocks_response, list_blobs_response, reserve_blocks_response,
};
use bytes::Bytes;
use data_types::{DataBlobGuid, TraceId};
use prost::Message as PbMessage;
use rpc_client_common::{InflightRpcGuard, RpcError, encode_protobuf};
use rpc_codec_common::MessageFrame;
use tracing::error;

/// Classification of the generation a GetBlobAtOrBefore read selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtOrBeforeEntry {
    Data,
    /// Fallocate claim: reads as zeros.
    Reserved,
    /// Punch/trim tombstone: an authoritative hole.
    Tombstone,
}

/// One node's answer to a GetBlobAtOrBefore read.
#[derive(Debug, Clone)]
pub struct AtOrBeforeBlob {
    pub version: u64,
    pub entry: AtOrBeforeEntry,
    /// Exact stored payload length (0 for Reserved/Tombstone bodies).
    pub total_bytes: u32,
    pub body_checksum: u64,
    pub body: Bytes,
}

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

    /// Reserve a single block (single-op; no batch) at `expected_version`.
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
        if !resp_frame.body.is_empty()
            && let Ok(resp) = <ReserveBlocksResponse as PbMessage>::decode(resp_frame.body.clone())
            && let Some(reserve_blocks_response::Result::Err(err)) = resp.result
        {
            return Err(RpcError::InternalResponseError(err));
        }
        Ok(())
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
        let _guard = InflightRpcGuard::new("bss", "list_blob_blocks");
        let mut marker = String::new();
        let mut entries = Vec::new();
        loop {
            let body = ListBlobBlocksRequest {
                first_block,
                block_count,
                marker: marker.clone(),
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

            let resp: ListBlobBlocksResponse = PbMessage::decode(resp_frame.body)
                .map_err(|e| RpcError::DecodeError(e.to_string()))?;
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
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "put_data_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.command = Command::PutDataBlob;
        header.body_len = body.len() as u32;
        header.size = size_of::<MessageHeader>() as u32 + header.body_len;
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

    /// Write a punch/trim tombstone generation: an empty write-once body
    /// with the deleted flag set. An authoritative hole for at-or-before
    /// reads; replicated and repaired like data.
    pub async fn put_data_blob_tombstone(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        version: u64,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "put_data_blob_tombstone");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.command = Command::PutDataBlob;
        header.body_len = 0;
        header.size = size_of::<MessageHeader>() as u32;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;
        header.version = version;
        header.is_deleted = 1;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request(msg_frame, timeout, Some(crate::OperationType::PutData))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put_data_blob_tombstone", %request_id, %blob_guid, %block_number, error=?e, "bss rpc failed");
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
        header.body_len = total_size as u32;
        header.size = size_of::<MessageHeader>() as u32 + header.body_len;
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
        header.body_len = content_len as u32;
        header.version = version;
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
        Ok(version)
    }

    /// At-or-before read: the newest generation of `block_number` with
    /// version <= `ceiling_version`. NotFound means no eligible
    /// generation exists on this node. `header_only` returns the
    /// identity without the body (the EC selection phase).
    #[allow(clippy::too_many_arguments)]
    pub async fn get_data_blob_at_or_before(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        ceiling_version: u64,
        header_only: bool,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<AtOrBeforeBlob, RpcError> {
        let _guard = InflightRpcGuard::new("bss", "get_data_blob_at_or_before");
        let body = GetBlobAtOrBeforeRequest {};
        let body_bytes = encode_protobuf(body, trace_id)?;

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_guid.blob_id.into_bytes();
        header.volume_id = blob_guid.volume_id;
        header.block_number = block_number;
        header.command = Command::GetBlobAtOrBefore;
        header.version = ceiling_version;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;
        header.reserve1[0] = u8::from(header_only);
        header.size = (size_of::<MessageHeader>() + body_bytes.len()) as u32;
        header.set_body_checksum(&body_bytes);

        let msg_frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self
            .send_request(msg_frame, timeout, Some(crate::OperationType::GetData))
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_data_blob_at_or_before", %request_id, %blob_guid, %block_number, %ceiling_version, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;

        let entry = if resp_frame.header.is_deleted != 0 {
            AtOrBeforeEntry::Tombstone
        } else {
            match resp_frame.header.reserve1[0] {
                0 => AtOrBeforeEntry::Data,
                1 => AtOrBeforeEntry::Reserved,
                other => {
                    return Err(RpcError::InternalResponseError(format!(
                        "BSS GetBlobAtOrBefore returned unknown entry type {other}"
                    )));
                }
            }
        };
        Ok(AtOrBeforeBlob {
            version: resp_frame.header.version,
            entry,
            total_bytes: resp_frame.header.body_len,
            body_checksum: resp_frame.header.checksum_body,
            body: resp_frame.body,
        })
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
        header.body_len = content_len as u32;
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
        header.body_len = body.len() as u32;
        header.size = size_of::<MessageHeader>() as u32 + header.body_len;
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
