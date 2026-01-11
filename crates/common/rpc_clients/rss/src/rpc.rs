use std::time::{Duration, Instant};

use crate::client::RpcClient;
use bytes::Bytes;
use data_types::{DataVgInfo, TraceId};
use metrics_wrapper::histogram;
use prost::Message as PbMessage;
use rpc_client_common::{InflightRpcGuard, RpcError, encode_protobuf};
use rpc_codec_common::MessageFrame;
use rss_codec::*;
use tracing::{error, warn};

impl RpcClient {
    pub async fn put(
        &self,
        version: i64,
        key: &str,
        value: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "put");
        let start = Instant::now();
        let body = PutRequest {
            version,
            key: key.to_string(),
            value: value.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::Put;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame, timeout).await.map_err(|e| {
            if !e.retryable() {
                error!(rpc=%"put", %request_id, %key, error=?e, "rss rpc failed");
            }
            e
        })?;
        let resp: PutResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::put_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "Put_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            rss_codec::put_response::Result::ErrOther(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Put_ErrOther")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"put", %key, "rss rpc failed: {resp}");
                Err(RpcError::InternalResponseError(resp))
            }
            rss_codec::put_response::Result::ErrRetry(()) => {
                histogram!("rss_rpc_nanos", "status" => "Put_ErrRetry")
                    .record(duration.as_nanos() as f64);
                warn!(rpc=%"put", %key, "rss rpc failed, retry needed");
                Err(RpcError::Retry)
            }
        }
    }

    pub async fn get(
        &self,
        key: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(i64, String), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "get");
        let start = Instant::now();
        let body = GetRequest {
            key: key.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::Get;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame, timeout).await.map_err(|e| {
            if !e.retryable() {
                error!(rpc=%"get", %request_id, %key, error=?e, "rss rpc failed");
            }
            e
        })?;
        let resp: GetResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::get_response::Result::Ok(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Get_Ok")
                    .record(duration.as_nanos() as f64);
                Ok((resp.version, resp.value))
            }
            rss_codec::get_response::Result::ErrNotFound(_resp) => {
                histogram!("rss_rpc_nanos", "status" => "Get_ErrNotFound")
                    .record(duration.as_nanos() as f64);
                warn!(rpc=%"get", %key, "could not find entry");
                Err(RpcError::NotFound)
            }
            rss_codec::get_response::Result::ErrOther(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Get_ErrOther")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"get", %key, "rss rpc failed: {resp}");
                Err(RpcError::InternalResponseError(resp))
            }
        }
    }

    pub async fn delete(
        &self,
        key: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "delete");
        let start = Instant::now();
        let body = DeleteRequest {
            key: key.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::Delete;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame, timeout).await.map_err(|e| {
            error!(rpc=%"delete", %request_id, %key, error=?e, "rss rpc failed");
            e
        })?;
        let resp: DeleteResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::delete_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "Delete_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            rss_codec::delete_response::Result::Err(resp) => {
                histogram!("rss_rpc_nanos", "status" => "Delete_Err")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"delete", %key, "rss rpc failed: {resp}");
                Err(RpcError::InternalResponseError(resp))
            }
        }
    }

    pub async fn get_nss_role(
        &self,
        instance_id: &str,
        health_report: Option<NssAgentHealthReport>,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(String, u64), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "get_nss_role");
        let start = Instant::now();
        let body = GetNssRoleRequest {
            instance_id: instance_id.to_string(),
            health_report,
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::GetNssRole;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame, timeout).await.map_err(|e| {
            if !e.retryable() {
                error!(rpc=%"get_nss_role", %request_id, %instance_id, error=?e, "rss rpc failed");
            }
            e
        })?;
        let resp: GetNssRoleResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        let version = resp.version;
        match resp.result.unwrap() {
            rss_codec::get_nss_role_response::Result::Role(role) => {
                histogram!("rss_rpc_nanos", "status" => "GetNssRole_Ok")
                    .record(duration.as_nanos() as f64);
                Ok((role, version))
            }
            rss_codec::get_nss_role_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "GetNssRole_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"get_nss_role", %instance_id, "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    pub async fn list(
        &self,
        prefix: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<Vec<String>, RpcError> {
        let _guard = InflightRpcGuard::new("rss", "list");
        let start = Instant::now();
        let body = ListRequest {
            prefix: prefix.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::List;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame, timeout).await.map_err(|e| {
            if !e.retryable() {
                error!(rpc=%"list", %request_id, %prefix, error=?e, "rss rpc failed");
            }
            e
        })?;
        let resp: ListResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::list_response::Result::Ok(resp) => {
                histogram!("rss_rpc_nanos", "status" => "List_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(resp.kvs)
            }
            rss_codec::list_response::Result::Err(resp) => {
                histogram!("rss_rpc_nanos", "status" => "List_Err")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"list", %prefix, "rss rpc failed: {resp}");
                Err(RpcError::InternalResponseError(resp))
            }
        }
    }

    pub async fn get_az_status(
        &self,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<AzStatusMap, RpcError> {
        let _guard = InflightRpcGuard::new("rss", "get_az_status");
        let start = Instant::now();

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::GetAzStatus;
        header.size = size_of::<MessageHeader>() as u32;
        header.retry_count = retry_count as u8;
        header.set_body_checksum(&[]);
        header.set_trace_id(trace_id);

        let frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self.send_request(frame, timeout).await.map_err(|e| {
            if !e.retryable() {
                error!(rpc=%"get_az_status", %request_id, error=?e, "rss rpc failed");
            }
            e
        })?;
        let resp: GetAzStatusResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::get_az_status_response::Result::StatusMap(status_map) => {
                histogram!("rss_rpc_nanos", "status" => "GetAzStatus_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(status_map)
            }
            rss_codec::get_az_status_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "GetAzStatus_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"get_az_status", "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    pub async fn set_az_status(
        &self,
        az_id: &str,
        status: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "set_az_status");
        let start = Instant::now();
        let body = SetAzStatusRequest {
            az_id: az_id.to_string(),
            status: status.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::SetAzStatus;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self
            .send_request( frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"set_az_status", %request_id, %az_id, %status, error=?e, "rss rpc failed");
                }
                e
            })?;
        let resp: SetAzStatusResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::set_az_status_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "SetAzStatus_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            rss_codec::set_az_status_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "SetAzStatus_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"set_az_status", "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    pub async fn create_bucket(
        &self,
        bucket_name: &str,
        api_key_id: &str,
        is_multi_az: bool,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "create_bucket");
        let start = Instant::now();
        let body = CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
            enable_versioning: false,
            api_key_id: api_key_id.to_string(),
            is_multi_az,
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::CreateBucket;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame, timeout).await.map_err(|e| {
            if !e.retryable() {
                error!(rpc=%"create_bucket", %request_id, %bucket_name, error=?e, "rss rpc failed");
            }
            e
        })?;
        let resp: CreateBucketResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::create_bucket_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "CreateBucket_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            rss_codec::create_bucket_response::Result::ErrBucketAlreadyExists(()) => {
                histogram!("rss_rpc_nanos", "status" => "CreateBucket_AlreadyExists")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"create_bucket", %bucket_name, "Bucket already exists");
                Err(RpcError::AlreadyExists)
            }
            rss_codec::create_bucket_response::Result::ErrBucketAlreadyOwnedByYou(()) => {
                histogram!("rss_rpc_nanos", "status" => "CreateBucket_AlreadyOwnedByYou")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"create_bucket", %bucket_name, "Bucket already owned by you");
                Err(RpcError::BucketAlreadyOwnedByYou)
            }
            rss_codec::create_bucket_response::Result::ErrOther(err) => {
                histogram!("rss_rpc_nanos", "status" => "CreateBucket_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"create_bucket", %bucket_name, "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    pub async fn delete_bucket(
        &self,
        bucket_name: &str,
        api_key_id: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("rss", "delete_bucket");
        let start = Instant::now();
        let body = DeleteBucketRequest {
            bucket_name: bucket_name.to_string(),
            api_key_id: api_key_id.to_string(),
        };

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::DeleteBucket;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame, timeout).await.map_err(|e| {
            if !e.retryable() {
                error!(rpc=%"delete_bucket", %request_id, %bucket_name, error=?e, "rss rpc failed");
            }
            e
        })?;
        let resp: DeleteBucketResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::delete_bucket_response::Result::Ok(()) => {
                histogram!("rss_rpc_nanos", "status" => "DeleteBucket_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(())
            }
            rss_codec::delete_bucket_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "DeleteBucket_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"delete_bucket", %bucket_name, "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    pub async fn get_data_vg_info(
        &self,
        timeout: Option<Duration>,
        trace_id: &TraceId,
    ) -> Result<DataVgInfo, RpcError> {
        let _guard = InflightRpcGuard::new("rss", "get_data_vg_info");
        let start = Instant::now();
        let body = GetDataVgInfoRequest {};

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::GetDataVgInfo;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame, timeout).await.map_err(|e| {
            if !e.retryable() {
                error!(rpc=%"get_data_vg_info", %request_id, error=?e, "rss rpc failed");
            }
            e
        })?;
        let resp: GetDataVgInfoResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::get_data_vg_info_response::Result::InfoJson(info_json) => {
                match serde_json::from_str::<DataVgInfo>(&info_json) {
                    Ok(info) => {
                        histogram!("rss_rpc_nanos", "status" => "GetDataVgInfo_Ok")
                            .record(duration.as_nanos() as f64);
                        Ok(info)
                    }
                    Err(e) => {
                        histogram!("rss_rpc_nanos", "status" => "GetDataVgInfo_ParseError")
                            .record(duration.as_nanos() as f64);
                        error!(rpc=%"get_data_vg_info", "failed to parse JSON response: {e}");
                        Err(RpcError::DecodeError(format!(
                            "Failed to parse JSON response: {}",
                            e
                        )))
                    }
                }
            }
            rss_codec::get_data_vg_info_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "GetDataVgInfo_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"get_data_vg_info", "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    /// Get metadata VG info as raw JSON string for forwarding to NSS
    pub async fn get_metadata_vg_info_json(
        &self,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<String, RpcError> {
        let _guard = InflightRpcGuard::new("rss", "get_metadata_vg_info_json");
        let start = Instant::now();
        let body = GetMetadataVgInfoRequest {};
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::GetMetadataVgInfo;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame, timeout).await.map_err(|e| {
            if !e.retryable() {
                error!(rpc=%"get_metadata_vg_info_json", %request_id, error=?e, "rss rpc failed");
            }
            e
        })?;
        let resp: GetMetadataVgInfoResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::get_metadata_vg_info_response::Result::InfoJson(info_json) => {
                histogram!("rss_rpc_nanos", "status" => "GetMetadataVgInfoJson_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(info_json)
            }
            rss_codec::get_metadata_vg_info_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "GetMetadataVgInfoJson_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"get_metadata_vg_info_json", "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    pub async fn get_active_nss_address(
        &self,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<String, RpcError> {
        let _guard = InflightRpcGuard::new("rss", "get_active_nss_address");
        let start = Instant::now();
        let body = GetActiveNssAddressRequest {};

        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = Command::GetActiveNssAddress;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame, timeout).await.map_err(|e| {
            if !e.retryable() {
                error!(rpc=%"get_active_nss_address", %request_id, error=?e, "rss rpc failed");
            }
            e
        })?;
        let resp: GetActiveNssAddressResponse =
            PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::get_active_nss_address_response::Result::Address(addr) => {
                histogram!("rss_rpc_nanos", "status" => "GetActiveNssAddress_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(addr)
            }
            rss_codec::get_active_nss_address_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "GetActiveNssAddress_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"get_active_nss_address", "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }
}
