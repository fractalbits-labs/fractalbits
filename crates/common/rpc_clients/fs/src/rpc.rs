use std::time::Duration;

use crate::client::RpcClient;
use data_types::TraceId;
use fs_gateway_codec::*;
use rpc_client_common::{ProtobufRpc, RpcError};

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
                    Command::$command as i32,
                    stringify!($name),
                    req,
                    timeout,
                    trace_id,
                    retry_count,
                    String::new,
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
rpc_method!(read_block, ReadBlock, ReadBlockRequest, ReadBlockResponse);
rpc_method!(
    probe_data_blocks,
    ProbeDataBlocks,
    ProbeDataBlocksRequest,
    ProbeDataBlocksResponse
);
rpc_method!(
    begin_flush,
    BeginFlush,
    BeginFlushRequest,
    BeginFlushResponse
);
rpc_method!(
    write_flush_block,
    WriteFlushBlock,
    WriteFlushBlockRequest,
    WriteFlushBlockResponse
);
rpc_method!(
    commit_flush,
    CommitFlush,
    CommitFlushRequest,
    CommitFlushResponse
);
rpc_method!(
    abort_flush,
    AbortFlush,
    AbortFlushRequest,
    AbortFlushResponse
);
rpc_method!(
    prefetch_inode,
    PrefetchInode,
    PrefetchInodeRequest,
    PrefetchInodeResponse
);
