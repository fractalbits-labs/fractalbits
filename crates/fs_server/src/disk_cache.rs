use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use uuid::Uuid;

/// Local NVMe disk cache for block data.
///
/// Each S3 object maps to a sparse cache file at `{cache_dir}/{blob_id}_{volume_id}`.
/// Blocks are written at their natural offset (`block_number * block_size`).
/// An xxHash3-64 checksum region is appended after `content_length`.
///
/// Populated blocks are detected via `SEEK_DATA`/`SEEK_HOLE` (ext4/xfs extent tree),
/// avoiding any in-memory bitmap.
#[allow(dead_code)]
pub struct DiskCache {
    cache_dir: PathBuf,
    max_size_bytes: u64,
    block_size: u64,
}

#[allow(dead_code)]
impl DiskCache {
    /// Create a new DiskCache. Creates the cache directory if needed and
    /// verifies the filesystem supports SEEK_DATA (ext4/xfs).
    pub fn new(
        cache_dir: impl Into<PathBuf>,
        max_size_gb: u64,
        block_size: u64,
    ) -> io::Result<Self> {
        let cache_dir = cache_dir.into();
        fs::create_dir_all(&cache_dir)?;

        // Verify filesystem type supports sparse file hole detection
        verify_filesystem(&cache_dir)?;

        Ok(Self {
            cache_dir,
            max_size_bytes: max_size_gb * 1024 * 1024 * 1024,
            block_size,
        })
    }

    /// Read a cached block. Returns None if the block is not cached or
    /// if checksum verification fails.
    pub fn get(&self, blob_id: Uuid, vol: u16, block: u32, content_length: u64) -> Option<Bytes> {
        let path = self.cache_file_path(blob_id, vol);
        let file = File::open(&path).ok()?;
        let fd = std::os::fd::AsRawFd::as_raw_fd(&file);

        // Check if block is populated (data, not a hole)
        if !is_block_populated(fd, block, self.block_size) {
            return None;
        }

        // Compute block data range
        let block_offset = block as u64 * self.block_size;
        let block_end = std::cmp::min(block_offset + self.block_size, content_length);
        let block_len = (block_end - block_offset) as usize;

        // Read block data
        let mut data = vec![0u8; block_len];
        if file.read_exact_at(&mut data, block_offset).is_err() {
            return None;
        }

        // Read checksum from checksum region
        let checksum_offset = content_length + block as u64 * 8;
        let mut checksum_buf = [0u8; 8];
        if file
            .read_exact_at(&mut checksum_buf, checksum_offset)
            .is_err()
        {
            return None;
        }
        let stored_checksum = u64::from_le_bytes(checksum_buf);

        // Verify checksum
        let computed = xxhash_rust::xxh3::xxh3_64(&data);
        if computed != stored_checksum {
            tracing::warn!(
                %blob_id, vol, block,
                "disk cache checksum mismatch, deleting cache file"
            );
            let _ = fs::remove_file(&path);
            return None;
        }

        Some(Bytes::from(data))
    }

    /// Write a block to cache. Creates the cache file if it doesn't exist.
    pub fn insert(
        &self,
        blob_id: Uuid,
        vol: u16,
        block: u32,
        content_length: u64,
        data: &[u8],
        checksum: u64,
    ) {
        let path = self.cache_file_path(blob_id, vol);
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
        {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(%blob_id, vol, block, error = %e, "failed to open cache file");
                return;
            }
        };

        let block_offset = block as u64 * self.block_size;

        // Write block data
        if let Err(e) = file.write_all_at(data, block_offset) {
            tracing::warn!(%blob_id, vol, block, error = %e, "failed to write cache data");
            return;
        }

        // Write checksum in checksum region
        let checksum_offset = content_length + block as u64 * 8;
        let checksum_bytes = checksum.to_le_bytes();
        if let Err(e) = file.write_all_at(&checksum_bytes, checksum_offset) {
            tracing::warn!(%blob_id, vol, block, error = %e, "failed to write cache checksum");
            return;
        }

        // Ensure data + checksum are persisted
        if let Err(e) = file.sync_data() {
            tracing::warn!(%blob_id, vol, block, error = %e, "failed to sync cache file");
        }
    }

    /// Check if all blocks of an object are populated (ready for passthrough).
    pub fn is_complete(&self, blob_id: Uuid, vol: u16, content_length: u64) -> bool {
        if content_length == 0 {
            return false;
        }

        let path = self.cache_file_path(blob_id, vol);
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(_) => return false,
        };
        let fd = std::os::fd::AsRawFd::as_raw_fd(&file);

        // Scan [0, content_length) for holes using SEEK_DATA/SEEK_HOLE.
        // If the first data region starts at 0 and the first hole is at or
        // beyond content_length, the file is fully populated.
        let data_start = unsafe { libc::lseek(fd, 0, libc::SEEK_DATA) };
        if data_start != 0 {
            return false;
        }

        let hole_start = unsafe { libc::lseek(fd, 0, libc::SEEK_HOLE) };
        if hole_start < 0 {
            return false;
        }

        hole_start as u64 >= content_length
    }

    /// Get the cache file path for an object.
    pub fn cache_file_path(&self, blob_id: Uuid, vol: u16) -> PathBuf {
        self.cache_dir
            .join(format!("{}_{}", blob_id.as_simple(), vol))
    }

    /// Evict LRU cache files until usage is at or below `target_bytes`.
    pub fn evict_to(&self, target_bytes: u64) {
        let entries = match fs::read_dir(&self.cache_dir) {
            Ok(e) => e,
            Err(_) => return,
        };

        // Collect (path, disk_usage, atime)
        let mut files: Vec<(PathBuf, u64, i64)> = Vec::new();
        let mut total_usage: u64 = 0;

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if let Ok(meta) = fs::metadata(&path) {
                let disk_bytes = meta.blocks() * 512;
                let atime = meta.atime();
                total_usage += disk_bytes;
                files.push((path, disk_bytes, atime));
            }
        }

        if total_usage <= target_bytes {
            return;
        }

        // Sort by atime ascending (oldest first = LRU)
        files.sort_by_key(|f| f.2);

        for (path, disk_bytes, _) in &files {
            if total_usage <= target_bytes {
                break;
            }
            if fs::remove_file(path).is_ok() {
                total_usage = total_usage.saturating_sub(*disk_bytes);
                tracing::info!(path = %path.display(), "evicted cache file");
            }
        }
    }

    /// Get current disk usage of the cache directory in bytes.
    pub fn current_usage(&self) -> u64 {
        let entries = match fs::read_dir(&self.cache_dir) {
            Ok(e) => e,
            Err(_) => return 0,
        };

        let mut total: u64 = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file()
                && let Ok(meta) = fs::metadata(&path)
            {
                total += meta.blocks() * 512;
            }
        }
        total
    }
}

/// Check if a block is populated (data, not a hole) in the cache file.
fn is_block_populated(fd: i32, block: u32, block_size: u64) -> bool {
    let offset = block as i64 * block_size as i64;
    let result = unsafe { libc::lseek(fd, offset, libc::SEEK_DATA) };
    result == offset
}

/// Verify that the cache directory is on ext4 or xfs (required for SEEK_DATA/SEEK_HOLE
/// to distinguish written zeros from holes).
fn verify_filesystem(path: &Path) -> io::Result<()> {
    use nix::sys::statfs::statfs;

    let stat = statfs(path).map_err(io::Error::other)?;
    let fs_type = stat.filesystem_type();

    // ext4: EXT4_SUPER_MAGIC = 0xEF53
    // xfs:  XFS_SUPER_MAGIC  = 0x58465342
    // tmpfs: TMPFS_MAGIC     = 0x01021994 (for tests)
    const EXT4_MAGIC: i64 = 0xEF53;
    const XFS_MAGIC: i64 = 0x58465342;
    const TMPFS_MAGIC: i64 = 0x0102_1994;

    let magic = fs_type.0 as i64;
    if magic != EXT4_MAGIC && magic != XFS_MAGIC && magic != TMPFS_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "disk cache requires ext4 or xfs filesystem (got type 0x{:X})",
                magic
            ),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    fn test_cache_dir() -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("test_disk_cache_{}_{}", std::process::id(), id));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_insert_and_get() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let blob_id = Uuid::new_v4();
        let vol = 1u16;
        let data = vec![42u8; 1024];
        let checksum = xxhash_rust::xxh3::xxh3_64(&data);
        let content_length = 4096u64; // 4 blocks of 1024

        cache.insert(blob_id, vol, 0, content_length, &data, checksum);

        let result = cache.get(blob_id, vol, 0, content_length);
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_ref(), &data[..]);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_get_missing_block() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let blob_id = Uuid::new_v4();
        let result = cache.get(blob_id, 1, 0, 4096);
        assert!(result.is_none());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_get_hole_block() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let blob_id = Uuid::new_v4();
        let vol = 1u16;
        let data = vec![55u8; 1024];
        let checksum = xxhash_rust::xxh3::xxh3_64(&data);
        let content_length = 8192u64;

        // Insert block 5, try to get block 3 (should be a hole)
        cache.insert(blob_id, vol, 5, content_length, &data, checksum);

        let result = cache.get(blob_id, vol, 3, content_length);
        assert!(result.is_none());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_checksum_corruption() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let blob_id = Uuid::new_v4();
        let vol = 1u16;
        let data = vec![99u8; 1024];
        let checksum = xxhash_rust::xxh3::xxh3_64(&data);
        let content_length = 4096u64;

        cache.insert(blob_id, vol, 0, content_length, &data, checksum);

        // Corrupt data on disk
        let path = cache.cache_file_path(blob_id, vol);
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&[0u8; 10], 0).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // get should return None and delete the file
        let result = cache.get(blob_id, vol, 0, content_length);
        assert!(result.is_none());
        assert!(!path.exists());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_is_complete() {
        let dir = test_cache_dir();
        // Use block_size >= fs block size (4096) so SEEK_DATA/SEEK_HOLE
        // can distinguish individual blocks
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();

        let blob_id = Uuid::new_v4();
        let vol = 1u16;
        let content_length = 3 * block_size; // 3 blocks

        // Insert all 3 blocks
        for block in 0..3 {
            let data = vec![block as u8; block_size as usize];
            let checksum = xxhash_rust::xxh3::xxh3_64(&data);
            cache.insert(blob_id, vol, block, content_length, &data, checksum);
        }

        assert!(cache.is_complete(blob_id, vol, content_length));

        // Partial: block 0 and block 2 only (gap at block 1)
        let blob_id2 = Uuid::new_v4();
        for block in [0, 2] {
            let data = vec![block as u8; block_size as usize];
            let checksum = xxhash_rust::xxh3::xxh3_64(&data);
            cache.insert(blob_id2, vol, block, content_length, &data, checksum);
        }

        assert!(!cache.is_complete(blob_id2, vol, content_length));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_evict() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        // Insert 3 cache files
        for i in 0..3 {
            let blob_id = Uuid::from_u128(i as u128 + 1);
            let data = vec![i as u8; 4096];
            let checksum = xxhash_rust::xxh3::xxh3_64(&data);
            cache.insert(blob_id, 1, 0, 8192, &data, checksum);
        }

        // Evict to 0 bytes -- should remove everything
        cache.evict_to(0);

        let remaining: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();
        assert!(remaining.is_empty());

        let _ = fs::remove_dir_all(&dir);
    }
}
