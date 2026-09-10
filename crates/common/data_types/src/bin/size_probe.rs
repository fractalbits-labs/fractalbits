use data_types::DataBlobGuid;
use data_types::object_layout::{
    ObjectCoreMetaData, ObjectLayout, ObjectMetaData, ObjectState, PosixAttrs,
};
use uuid::Uuid;

fn base_layout() -> ObjectLayout {
    ObjectLayout {
        timestamp: 1,
        version_id: Uuid::new_v4(),
        block_size: ObjectLayout::DEFAULT_BLOCK_SIZE,
        blob_version: 1,
        fs_ext: None,
        state: ObjectState::Normal(ObjectMetaData {
            blob_guid: DataBlobGuid {
                blob_id: Uuid::new_v4(),
                volume_id: 1,
            },
            core_meta_data: ObjectCoreMetaData {
                size: 4096,
                etag: "0123456789abcdef0123456789abcdef".to_string(),
                headers: vec![],
                checksum: None,
            },
        }),
    }
}

fn size_of(l: &ObjectLayout) -> usize {
    rkyv::to_bytes::<rkyv::rancor::Error>(l).unwrap().len()
}

fn main() {
    println!("s3 row (fs_ext None):        {}", size_of(&base_layout()));

    let mut fs = base_layout();
    fs.set_fs_posix(Some(PosixAttrs {
        mode: 0o100644,
        uid: 1000,
        gid: 1000,
        mtime_ns: 42,
        ctime_ns: 42,
    }));
    println!("fs row (posix only):         {}", size_of(&fs));

    fs.set_next_version(9);
    println!("fs row (posix + nv):         {}", size_of(&fs));
}
