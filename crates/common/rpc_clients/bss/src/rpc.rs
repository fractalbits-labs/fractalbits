use std::sync::Arc;
use std::time::Duration;

use crate::client::RpcClient;
use bss_codec::{Command, ListBlobsRequest, ListBlobsResponse, MessageHeader, list_blobs_response};
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
        header.version = 1;

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
        header.version = 1;

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
    pub async fn get_data_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
        body: &mut Bytes,
        content_len: usize,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
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
        *body = resp_frame.body;
        if content_len != body.len() {
            return Err(RpcError::InternalResponseError(format!(
                "BSS returned body length {} but client expected {}",
                body.len(),
                content_len
            )));
        }
        Ok(())
    }

    pub async fn delete_data_blob(
        &self,
        blob_guid: DataBlobGuid,
        block_number: u32,
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
        header.version = 1;

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

    pub async fn handshake(
        &self,
        fence_token: u64,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "handshake");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::Handshake;
        header.fence_token = fence_token;
        header.size = size_of::<MessageHeader>() as u32;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request(msg_frame, timeout, None)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"handshake", %request_id, error=?e, "bss rpc failed");
                }
                e
            })?;
        check_response_errno(&resp_frame.header)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_metadata_blob(
        &self,
        blob_id: [u8; 16],
        volume_id: u16,
        content_len: usize,
        fence_token: u64,
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
        header.fence_token = fence_token;
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
        fence_token: u64,
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
        header.fence_token = fence_token;
        header.checksum_body = body_checksum;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;

        let msg_frame = MessageFrame::new(header, body);
        let resp_frame = self.send_request(msg_frame, timeout, None).await.map_err(|e| {
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
        fence_token: u64,
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
        header.fence_token = fence_token;
        header.size = size_of::<MessageHeader>() as u32;
        header.retry_count = retry_count as u8;
        header.trace_id = trace_id.0;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self.send_request(msg_frame, timeout, None).await.map_err(|e| {
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
