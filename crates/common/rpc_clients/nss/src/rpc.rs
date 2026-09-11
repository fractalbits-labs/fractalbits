use std::time::Duration;

use crate::client::RpcClient;
use crate::stats::{NssOperation, NssStatsGuard};
use bytes::Bytes;
use data_types::TraceId;
use nss_codec::*;
use prost::Message as PbMessage;
use rpc_client_common::{ProtobufRpc, RpcError, rpc_ctx};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TeardownFence {
    pub token: u64,
    pub session: u64,
    pub generation: u64,
}

/// NSS keys are NUL-terminated on the wire.
fn nul_terminated(key: &str) -> String {
    let mut nss_key = String::with_capacity(key.len() + 1);
    nss_key.push_str(key);
    nss_key.push('\0');
    nss_key
}

fn map_rename_result(resp: RenameResponse) -> Result<Bytes, RpcError> {
    match resp.result.unwrap() {
        rename_response::Result::Ok(old_value) => Ok(old_value),
        rename_response::Result::ErrSrcNonexisted(_) => Err(RpcError::NotFound),
        rename_response::Result::ErrDstExisted(_) => Err(RpcError::AlreadyExists),
        rename_response::Result::ErrNoSuchRootBlob(_) => Err(RpcError::NoSuchRootBlob),
        rename_response::Result::ErrOther(e) => Err(RpcError::InternalResponseError(e)),
    }
}

impl RpcClient {
    #[allow(clippy::too_many_arguments)]
    async fn call_with_stats<Req: PbMessage, Resp: PbMessage + Default>(
        &self,
        command: Command,
        op: NssOperation,
        body: Req,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
        ctx: impl FnOnce() -> String,
    ) -> Result<Resp, RpcError> {
        let _stats = NssStatsGuard::new(op);
        self.call(
            command as i32,
            op.as_str(),
            body,
            timeout,
            trace_id,
            retry_count,
            ctx,
        )
        .await
    }

    pub async fn put_inode(
        &self,
        root_blob_name: &str,
        key: &str,
        value: Bytes,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<PutInodeResponse, RpcError> {
        let body = PutInodeRequest {
            root_blob_name: root_blob_name.to_string(),
            key: nul_terminated(key),
            value,
        };
        self.call_with_stats(
            Command::PutInode,
            NssOperation::PutInode,
            body,
            timeout,
            trace_id,
            retry_count,
            rpc_ctx!(root_blob_name, key),
        )
        .await
    }

    /// Compare-and-swap variant of `put_inode`. The server installs `value`
    /// at `key` only if the currently stored bytes match `expected_old_value`
    /// exactly; pass an empty `Bytes` to require the slot to be empty.
    /// On mismatch the response carries the current bytes via `Conflict`,
    /// letting callers fail forward without guessing the winner's state.
    #[allow(clippy::too_many_arguments)]
    pub async fn put_inode_cas(
        &self,
        root_blob_name: &str,
        key: &str,
        value: Bytes,
        expected_old_value: Bytes,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<PutInodeCasResponse, RpcError> {
        let body = PutInodeCasRequest {
            root_blob_name: root_blob_name.to_string(),
            key: nul_terminated(key),
            value,
            expected_old_value,
        };
        self.call_with_stats(
            Command::PutInodeCas,
            NssOperation::PutInodeCas,
            body,
            timeout,
            trace_id,
            retry_count,
            rpc_ctx!(root_blob_name, key),
        )
        .await
    }

    pub async fn get_inode(
        &self,
        root_blob_name: &str,
        key: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<GetInodeResponse, RpcError> {
        let body = GetInodeRequest {
            root_blob_name: root_blob_name.to_string(),
            key: nul_terminated(key),
        };
        self.call_with_stats(
            Command::GetInode,
            NssOperation::GetInode,
            body,
            timeout,
            trace_id,
            retry_count,
            rpc_ctx!(root_blob_name, key),
        )
        .await
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
        let start_after = if start_after.ends_with('/') {
            start_after.to_string()
        } else {
            nul_terminated(start_after)
        };
        let body = ListInodesRequest {
            root_blob_name: root_blob_name.to_string(),
            max_keys,
            prefix: prefix.to_string(),
            delimiter: delimiter.to_string(),
            start_after,
            skip_mpu_parts,
        };
        self.call_with_stats(
            Command::ListInodes,
            NssOperation::ListInodes,
            body,
            timeout,
            trace_id,
            retry_count,
            rpc_ctx!(root_blob_name, prefix),
        )
        .await
    }

    pub async fn delete_inode(
        &self,
        root_blob_name: &str,
        key: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<DeleteInodeResponse, RpcError> {
        self.delete_inode_with_teardown(
            root_blob_name,
            key,
            TeardownFence::default(),
            timeout,
            trace_id,
            retry_count,
        )
        .await
    }

    pub async fn delete_inode_with_teardown(
        &self,
        root_blob_name: &str,
        key: &str,
        teardown: TeardownFence,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<DeleteInodeResponse, RpcError> {
        let body = DeleteInodeRequest {
            root_blob_name: root_blob_name.to_string(),
            key: nul_terminated(key),
            teardown_token: teardown.token,
            teardown_session: teardown.session,
            teardown_generation: teardown.generation,
        };
        self.call_with_stats(
            Command::DeleteInode,
            NssOperation::DeleteInode,
            body,
            timeout,
            trace_id,
            retry_count,
            rpc_ctx!(root_blob_name, key),
        )
        .await
    }

    pub async fn create_root_inode(
        &self,
        bucket: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<CreateRootInodeResponse, RpcError> {
        let body = CreateRootInodeRequest {
            bucket: bucket.to_string(),
        };
        self.call_with_stats(
            Command::CreateRootInode,
            NssOperation::CreateRootInode,
            body,
            timeout,
            trace_id,
            retry_count,
            rpc_ctx!(bucket),
        )
        .await
    }

    pub async fn delete_root_inode(
        &self,
        root_blob_name: &str,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<DeleteRootInodeResponse, RpcError> {
        self.delete_root_inode_with_teardown(
            root_blob_name,
            TeardownFence::default(),
            false,
            timeout,
            trace_id,
            retry_count,
        )
        .await
    }

    pub async fn delete_root_inode_with_teardown(
        &self,
        root_blob_name: &str,
        teardown: TeardownFence,
        abort_teardown: bool,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<DeleteRootInodeResponse, RpcError> {
        let body = DeleteRootInodeRequest {
            root_blob_name: root_blob_name.to_string(),
            teardown_token: teardown.token,
            abort_teardown,
            teardown_session: teardown.session,
            teardown_generation: teardown.generation,
        };
        self.call_with_stats(
            Command::DeleteRootInode,
            NssOperation::DeleteRootInode,
            body,
            timeout,
            trace_id,
            retry_count,
            rpc_ctx!(root_blob_name),
        )
        .await
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
        let body = RenameRequest {
            root_blob_name: root_blob_name.to_string(),
            src_path: src_path.to_string(),
            dst_path: dst_path.to_string(),
            force_overwrite: false,
        };
        let resp: RenameResponse = self
            .call_with_stats(
                Command::Rename,
                NssOperation::RenameFolder,
                body,
                timeout,
                trace_id,
                retry_count,
                rpc_ctx!(root_blob_name, src_path, dst_path),
            )
            .await?;
        map_rename_result(resp).map(|_| ())
    }

    /// On a force_overwrite that replaced an existing dst, the returned bytes are the prior dst
    /// value so the caller can GC the now-orphaned blob; on a non-overwriting rename they are
    /// empty.
    #[allow(clippy::too_many_arguments)]
    pub async fn rename_object(
        &self,
        root_blob_name: &str,
        src_path: &str,
        dst_path: &str,
        force_overwrite: bool,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<Bytes, RpcError> {
        let body = RenameRequest {
            root_blob_name: root_blob_name.to_string(),
            src_path: nul_terminated(src_path),
            dst_path: nul_terminated(dst_path),
            force_overwrite,
        };
        let resp: RenameResponse = self
            .call_with_stats(
                Command::Rename,
                NssOperation::RenameObject,
                body,
                timeout,
                trace_id,
                retry_count,
                rpc_ctx!(root_blob_name, src_path, dst_path),
            )
            .await?;
        map_rename_result(resp)
    }
}
