use data_types::DataBlobGuid;
use data_types::block_map::{BlockMap, RangeState};
use data_types::object_layout::{
    BlockMapRef, ObjectCoreMetaData, ObjectLayout, ObjectMetaData, ObjectState, PosixAttrs,
    PreparedWrite,
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

    fs.set_block_map(Some(BlockMapRef {
        map_id: Uuid::new_v4(),
        chunk_count: 1,
    }));
    println!("fs row (posix + map):        {}", size_of(&fs));

    fs.set_prepared_write(Some(PreparedWrite {
        version: 2,
        append_range: Some((0, 1)),
        reservation_abort_map: None,
    }));
    println!("fs row (posix + map + pw):   {}", size_of(&fs));

    let mut m = BlockMap::new();
    let mut block = 0u32;
    for i in 0..300u32 {
        m.overlay(block, block + 3, RangeState::Written(2 + (i as u64 % 7)));
        block += 6;
    }
    let chunks = m.to_chunks();
    println!(
        "chunk of 300 ranges:         {} bytes ({} / range)",
        chunks[0].len(),
        chunks[0].len() / 300
    );
}
