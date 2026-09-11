use std::time::{Duration, Instant};

use crate::client::RpcClient;
use data_types::{DataVgInfo, TraceId};
use metrics_wrapper::histogram;
use rpc_client_common::{ProtobufRpc, RpcError, rpc_ctx};
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
        let start = Instant::now();
        let body = PutRequest {
            version,
            key: key.to_string(),
            value: value.to_string(),
        };
        let resp: PutResponse = self
            .call(
                Command::Put as i32,
                "put",
                body,
                timeout,
                trace_id,
                retry_count,
                rpc_ctx!(key),
            )
            .await?;
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
        let start = Instant::now();
        let body = GetRequest {
            key: key.to_string(),
        };
        let resp: GetResponse = self
            .call(
                Command::Get as i32,
                "get",
                body,
                timeout,
                trace_id,
                retry_count,
                rpc_ctx!(key),
            )
            .await?;
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
        let start = Instant::now();
        let body = DeleteRequest {
            key: key.to_string(),
        };
        let resp: DeleteResponse = self
            .call(
                Command::Delete as i32,
                "delete",
                body,
                timeout,
                trace_id,
                retry_count,
                rpc_ctx!(key),
            )
            .await?;
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

    /// Returns (role, journal_config_json)
    pub async fn get_nss_role(
        &self,
        instance_id: &str,
        health_report: Option<NssAgentHealthReport>,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(String, Option<String>), RpcError> {
        let start = Instant::now();
        let body = GetNssRoleRequest {
            instance_id: instance_id.to_string(),
            health_report,
        };
        let resp: GetNssRoleResponse = self
            .call(
                Command::GetNssRole as i32,
                "get_nss_role",
                body,
                timeout,
                trace_id,
                retry_count,
                rpc_ctx!(instance_id),
            )
            .await?;
        let duration = start.elapsed();
        let journal_config_json = resp.journal_config_json;
        match resp.result.unwrap() {
            rss_codec::get_nss_role_response::Result::Role(role) => {
                histogram!("rss_rpc_nanos", "status" => "GetNssRole_Ok")
                    .record(duration.as_nanos() as f64);
                Ok((role, journal_config_json))
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
        let start = Instant::now();
        let body = ListRequest {
            prefix: prefix.to_string(),
        };
        let resp: ListResponse = self
            .call(
                Command::List as i32,
                "list",
                body,
                timeout,
                trace_id,
                retry_count,
                rpc_ctx!(prefix),
            )
            .await?;
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

    pub async fn create_bucket(
        &self,
        bucket_name: &str,
        api_key_id: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<(), RpcError> {
        let start = Instant::now();
        let body = CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
            enable_versioning: false,
            api_key_id: api_key_id.to_string(),
        };
        let resp: CreateBucketResponse = self
            .call(
                Command::CreateBucket as i32,
                "create_bucket",
                body,
                timeout,
                trace_id,
                retry_count,
                rpc_ctx!(bucket_name),
            )
            .await?;
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
        let start = Instant::now();
        let body = DeleteBucketRequest {
            bucket_name: bucket_name.to_string(),
            api_key_id: api_key_id.to_string(),
        };
        let resp: DeleteBucketResponse = self
            .call(
                Command::DeleteBucket as i32,
                "delete_bucket",
                body,
                timeout,
                trace_id,
                retry_count,
                rpc_ctx!(bucket_name),
            )
            .await?;
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
        let start = Instant::now();
        let resp: GetDataVgInfoResponse = self
            .call(
                Command::GetDataVgInfo as i32,
                "get_data_vg_info",
                GetDataVgInfoRequest {},
                timeout,
                trace_id,
                0,
                String::new,
            )
            .await?;
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

    pub async fn get_metadata_vg_info(
        &self,
        timeout: Option<Duration>,
        trace_id: &TraceId,
    ) -> Result<data_types::MetadataVgInfo, RpcError> {
        let json = self.get_metadata_vg_info_json(timeout, trace_id, 0).await?;
        serde_json::from_str(&json).map_err(|e| {
            error!(rpc=%"get_metadata_vg_info", "failed to parse JSON response: {e}");
            RpcError::DecodeError(format!("Failed to parse metadata VG JSON: {e}"))
        })
    }

    /// Get metadata VG info as raw JSON string for forwarding to NSS
    pub async fn get_metadata_vg_info_json(
        &self,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<String, RpcError> {
        let start = Instant::now();
        let resp: GetMetadataVgInfoResponse = self
            .call(
                Command::GetMetadataVgInfo as i32,
                "get_metadata_vg_info_json",
                GetMetadataVgInfoRequest {},
                timeout,
                trace_id,
                retry_count,
                String::new,
            )
            .await?;
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

    /// Get journal VG info as raw JSON string for forwarding to NSS
    pub async fn get_journal_vg_info_json(
        &self,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<String, RpcError> {
        let start = Instant::now();
        let resp: GetJournalVgInfoResponse = self
            .call(
                Command::GetJournalVgInfo as i32,
                "get_journal_vg_info_json",
                GetJournalVgInfoRequest {},
                timeout,
                trace_id,
                retry_count,
                String::new,
            )
            .await?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::get_journal_vg_info_response::Result::InfoJson(info_json) => {
                histogram!("rss_rpc_nanos", "status" => "GetJournalVgInfoJson_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(info_json)
            }
            rss_codec::get_journal_vg_info_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "GetJournalVgInfoJson_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"get_journal_vg_info_json", "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    /// Get journal config as raw JSON string for forwarding to NSS
    pub async fn get_journal_config_json(
        &self,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<String, RpcError> {
        let start = Instant::now();
        let resp: GetJournalConfigResponse = self
            .call(
                Command::GetJournalConfig as i32,
                "get_journal_config_json",
                GetJournalConfigRequest {},
                timeout,
                trace_id,
                retry_count,
                String::new,
            )
            .await?;
        let duration = start.elapsed();
        match resp.result.unwrap() {
            rss_codec::get_journal_config_response::Result::ConfigJson(config_json) => {
                histogram!("rss_rpc_nanos", "status" => "GetJournalConfigJson_Ok")
                    .record(duration.as_nanos() as f64);
                Ok(config_json)
            }
            rss_codec::get_journal_config_response::Result::Error(err) => {
                histogram!("rss_rpc_nanos", "status" => "GetJournalConfigJson_Error")
                    .record(duration.as_nanos() as f64);
                error!(rpc=%"get_journal_config_json", "rss rpc failed: {err}");
                Err(RpcError::InternalResponseError(err))
            }
        }
    }

    pub async fn get_active_nss_address(
        &self,
        routing_key: &[u8],
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<String, RpcError> {
        let start = Instant::now();
        let body = GetActiveNssAddressRequest {
            routing_key: bytes::Bytes::copy_from_slice(routing_key),
        };
        let resp: GetActiveNssAddressResponse = self
            .call(
                Command::GetActiveNssAddress as i32,
                "get_active_nss_address",
                body,
                timeout,
                trace_id,
                retry_count,
                String::new,
            )
            .await?;
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
