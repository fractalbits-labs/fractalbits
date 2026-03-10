use std::time::Duration;

use crate::client::RpcClient;
use bytes::Bytes;
use data_types::TraceId;
use nss_codec::*;
use prost::Message as PbMessage;
use rpc_client_common::{InflightRpcGuard, RpcError, encode_protobuf};
use rpc_codec_common::MessageFrame;
use tracing::error;

impl RpcClient {
    pub async fn put_inode(
        &self,
        root_blob_name: &str,
        key: &str,
        value: Bytes,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<PutInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "put_inode");
        let mut nss_key = key.to_string();
        nss_key.push('\0');
        let body = PutInodeRequest {
            root_blob_name: root_blob_name.to_string(),
            key: nss_key,
            value,
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::PutInode;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self
            .send_request(frame, timeout, crate::NssOperation::PutInode)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put_inode", %request_id, %root_blob_name, %key, error=?e, "nss rpc failed");
                }
                e
            })?;
        let resp: PutInodeResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        Ok(resp)
    }

    pub async fn get_inode(
        &self,
        root_blob_name: &str,
        key: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<GetInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "get_inode");
        let mut nss_key = key.to_string();
        nss_key.push('\0');
        let body = GetInodeRequest {
            root_blob_name: root_blob_name.to_string(),
            key: nss_key,
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::GetInode;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self
            .send_request(frame, timeout, crate::NssOperation::GetInode)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_inode", %request_id, %root_blob_name, %key, error=?e, "nss rpc failed");
                }
                e
            })?;
        let resp: GetInodeResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        Ok(resp)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn list_inodes(
        &self,
        root_blob_name: &str,
        max_keys: u32,
        prefix: &str,
        delimiter: &str,
        start_after: &str,
        skip_mpu_parts: bool,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<ListInodesResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "list_inodes");
        let mut start_after_owned = start_after.to_string();
        if !start_after_owned.ends_with("/") {
            start_after_owned.push('\0');
        }
        let body = ListInodesRequest {
            root_blob_name: root_blob_name.to_string(),
            max_keys,
            prefix: prefix.to_string(),
            delimiter: delimiter.to_string(),
            start_after: start_after_owned,
            skip_mpu_parts,
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::ListInodes;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self
            .send_request(frame, timeout, crate::NssOperation::ListInodes)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"list_inodes", %request_id, %root_blob_name, %prefix, error=?e, "nss rpc failed");
                }
                e
            })?;
        let resp: ListInodesResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        Ok(resp)
    }

    pub async fn delete_inode(
        &self,
        root_blob_name: &str,
        key: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<DeleteInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "delete_inode");
        let mut nss_key = key.to_string();
        nss_key.push('\0');
        let body = DeleteInodeRequest {
            root_blob_name: root_blob_name.to_string(),
            key: nss_key,
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::DeleteInode;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self
            .send_request(frame, timeout, crate::NssOperation::DeleteInode)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"delete_inode", %request_id, %root_blob_name, %key, error=?e, "nss rpc failed");
                }
                e
            })?;
        let resp: DeleteInodeResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        Ok(resp)
    }

    pub async fn create_root_inode(
        &self,
        bucket: &str,
        az_mirroring: bool,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<CreateRootInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "create_root_inode");
        let body = CreateRootInodeRequest {
            bucket: bucket.to_string(),
            az_mirroring,
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::CreateRootInode;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self
            .send_request(frame, timeout, crate::NssOperation::CreateRootInode)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"create_root_inode", %request_id, %bucket, error=?e, "nss rpc failed");
                }
                e
            })?;
        let resp: CreateRootInodeResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        Ok(resp)
    }

    pub async fn delete_root_inode(
        &self,
        root_blob_name: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<DeleteRootInodeResponse, RpcError> {
        let _guard = InflightRpcGuard::new("nss", "delete_root_inode");
        let body = DeleteRootInodeRequest {
            root_blob_name: root_blob_name.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::DeleteRootInode;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self
            .send_request(frame, timeout, crate::NssOperation::DeleteRootInode)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"delete_root_inode", %request_id, %root_blob_name, error=?e, "nss rpc failed");
                }
                e
            })?;
        let resp: DeleteRootInodeResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        Ok(resp)
    }

    pub async fn rename_folder(
        &self,
        root_blob_name: &str,
        src_path: &str,
        dst_path: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("nss", "rename_folder");
        let body = RenameRequest {
            root_blob_name: root_blob_name.to_string(),
            src_path: src_path.to_string(),
            dst_path: dst_path.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::Rename;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self
            .send_request(frame, timeout, crate::NssOperation::RenameFolder)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"rename_folder", %request_id, %root_blob_name, %src_path, %dst_path, error=?e, "nss rpc failed");
                }
                e
            })?;
        let resp: RenameResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        match resp.result.unwrap() {
            nss_codec::rename_response::Result::Ok(_) => Ok(()),
            nss_codec::rename_response::Result::ErrSrcNonexisted(_) => Err(RpcError::NotFound),
            nss_codec::rename_response::Result::ErrDstExisted(_) => Err(RpcError::AlreadyExists),
            nss_codec::rename_response::Result::ErrOther(e) => {
                Err(RpcError::InternalResponseError(e))
            }
        }
    }

    pub async fn rename_object(
        &self,
        root_blob_name: &str,
        src_path: &str,
        dst_path: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let mut nss_src_path = src_path.to_string();
        nss_src_path.push('\0');
        let mut nss_dst_path = dst_path.to_string();
        nss_dst_path.push('\0');

        let _guard = InflightRpcGuard::new("nss", "rename_object");
        let body = RenameRequest {
            root_blob_name: root_blob_name.to_string(),
            src_path: nss_src_path,
            dst_path: nss_dst_path,
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::Rename;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self
            .send_request(frame, timeout, crate::NssOperation::RenameObject)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"rename_object", %request_id, %root_blob_name, %src_path, %dst_path, error=?e, "nss rpc failed");
                }
                e
            })?;
        let resp: RenameResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        match resp.result.unwrap() {
            nss_codec::rename_response::Result::Ok(_) => Ok(()),
            nss_codec::rename_response::Result::ErrSrcNonexisted(_) => Err(RpcError::NotFound),
            nss_codec::rename_response::Result::ErrDstExisted(_) => Err(RpcError::AlreadyExists),
            nss_codec::rename_response::Result::ErrOther(e) => {
                Err(RpcError::InternalResponseError(e))
            }
        }
    }
}
