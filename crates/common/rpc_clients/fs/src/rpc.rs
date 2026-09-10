use std::time::Duration;

use crate::client::RpcClient;
use data_types::TraceId;
use fs_gateway_codec::*;
use prost::Message as PbMessage;
use rpc_client_common::{InflightRpcGuard, RpcError, encode_protobuf};
use rpc_codec_common::MessageFrame;
use tracing::error;

impl RpcClient {
    /// One request / response round trip. Transport failures surface as
    /// `RpcError`; application outcomes stay inside the decoded response
    /// so the caller can map them without losing the error kind.
    async fn call<Req: PbMessage, Resp: PbMessage + Default>(
        &self,
        command: Command,
        name: &'static str,
        body: Req,
        timeout: Option<Duration>,
        trace_id: &TraceId,
        retry_count: u32,
    ) -> Result<Resp, RpcError> {
        let _guard = InflightRpcGuard::new("fs_gateway", name);
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.command = command as i32;
        header.size = (size_of::<MessageHeader>() + body.encoded_len()) as u32;
        header.retry_count = retry_count as u8;
        header.set_trace_id(trace_id);

        let body_bytes = encode_protobuf(body, trace_id)?;
        header.set_body_checksum(&body_bytes);
        let frame = MessageFrame::new(header, body_bytes);
        let resp_frame = self.send_request(frame, timeout).await.map_err(|e| {
            if !e.retryable() {
                error!(rpc = %name, %request_id, error = ?e, "fs_gateway rpc failed");
            }
            e
        })?;
        PbMessage::decode(resp_frame.body).map_err(|e| RpcError::DecodeError(e.to_string()))
    }
}

macro_rules! rpc_method {
    ($name:ident, $command:ident, $req:ty, $resp:ty) => {
        impl RpcClient {
            pub async fn $name(
                &self,
                req: $req,
                timeout: Option<Duration>,
                trace_id: &TraceId,
                retry_count: u32,
            ) -> Result<$resp, RpcError> {
                self.call(
                    Command::$command,
                    stringify!($name),
                    req,
                    timeout,
                    trace_id,
                    retry_count,
                )
                .await
            }
        }
    };
}

rpc_method!(mount, Mount, MountRequest, MountResponse);
rpc_method!(get_inode, GetInode, GetInodeRequest, GetInodeResponse);
rpc_method!(
    list_inodes,
    ListInodes,
    ListInodesRequest,
    ListInodesResponse
);
rpc_method!(put_inode, PutInode, PutInodeRequest, PutInodeResponse);
rpc_method!(
    put_inode_cas,
    PutInodeCas,
    PutInodeCasRequest,
    PutInodeCasResponse
);
rpc_method!(
    delete_inode,
    DeleteInode,
    DeleteInodeRequest,
    DeleteInodeResponse
);
rpc_method!(
    rename_file,
    RenameFile,
    RenameFileRequest,
    RenameFileResponse
);
rpc_method!(
    rename_folder,
    RenameFolder,
    RenameFolderRequest,
    RenameFolderResponse
);
rpc_method!(
    put_dir_marker,
    PutDirMarker,
    PutDirMarkerRequest,
    PutDirMarkerResponse
);
rpc_method!(
    list_mpu_parts,
    ListMpuParts,
    ListMpuPartsRequest,
    ListMpuPartsResponse
);
rpc_method!(
    allocate_blob_guid,
    AllocateBlobGuid,
    AllocateBlobGuidRequest,
    AllocateBlobGuidResponse
);
rpc_method!(read_block, ReadBlock, ReadBlockRequest, ReadBlockResponse);
rpc_method!(
    write_block,
    WriteBlock,
    WriteBlockRequest,
    WriteBlockResponse
);
rpc_method!(
    list_blob_blocks,
    ListBlobBlocks,
    ListBlobBlocksRequest,
    ListBlobBlocksResponse
);
rpc_method!(
    prefetch_blob,
    PrefetchBlob,
    PrefetchBlobRequest,
    PrefetchBlobResponse
);
rpc_method!(sweep_blob, SweepBlob, SweepBlobRequest, SweepBlobResponse);
