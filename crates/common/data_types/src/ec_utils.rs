use uuid::Uuid;

/// Compute shard-to-node rotation for EC volumes.
/// rotation = crc32(blob_id bytes) % total_shards
pub fn ec_rotation(blob_id: &Uuid, total_shards: u32) -> usize {
    let hash = crc32fast::hash(blob_id.as_bytes());
    (hash % total_shards) as usize
}

/// Compute padded length for RS encoding to ensure even shard sizes.
/// The stripe must be a multiple of `data_shards * 2` for reed-solomon-simd.
pub fn ec_padded_len(content_len: usize, data_shards: usize) -> usize {
    let stripe_size = data_shards * 2;
    if content_len.is_multiple_of(stripe_size) {
        content_len
    } else {
        content_len + (stripe_size - content_len % stripe_size)
    }
}

/// Compute shard index from node position and rotation.
/// Given node_idx in the volume's bss_nodes list, returns the logical shard
/// index (0..k are data shards, k..k+m are parity shards).
pub fn shard_index(node_idx: usize, rotation: usize, total: usize) -> usize {
    (node_idx + total - rotation) % total
}

/// Compute node index from shard index and rotation.
/// Given a logical shard index, returns the node position in bss_nodes.
pub fn node_index(shard_idx: usize, rotation: usize, total: usize) -> usize {
    (shard_idx + rotation) % total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_deterministic() {
        let blob_id = Uuid::parse_str("01234567-89ab-cdef-0123-456789abcdef").unwrap();
        let r1 = ec_rotation(&blob_id, 6);
        let r2 = ec_rotation(&blob_id, 6);
        assert_eq!(r1, r2);
        assert!(r1 < 6);
    }

    #[test]
    fn rotation_varies_by_blob_id() {
        let mut rotations = std::collections::HashSet::new();
        for i in 0..100u128 {
            let blob_id = Uuid::from_u128(i);
            let r = ec_rotation(&blob_id, 6);
            assert!(r < 6);
            rotations.insert(r);
        }
        assert!(rotations.len() >= 3);
    }

    #[test]
    fn padded_len_multiple() {
        assert_eq!(ec_padded_len(8, 4), 8);
        assert_eq!(ec_padded_len(9, 4), 16);
        assert_eq!(ec_padded_len(0, 4), 0);
        assert_eq!(ec_padded_len(99, 4), 104);
    }

    #[test]
    fn shard_node_roundtrip() {
        let total = 6;
        for rotation in 0..total {
            for si in 0..total {
                let ni = node_index(si, rotation, total);
                let back = shard_index(ni, rotation, total);
                assert_eq!(back, si, "rotation={rotation} shard={si} node={ni}");
            }
        }
    }
}
