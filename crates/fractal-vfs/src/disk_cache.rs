use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use compio_buf::BufResult;
use compio_fs::{File, OpenOptions};
use compio_io::{AsyncReadAt, AsyncWriteAt};

use crate::slice_mut::SliceMut;
use lru::LruCache;
use uuid::Uuid;

/// How often the background evictor checks disk usage.
const EVICTION_INTERVAL: Duration = Duration::from_secs(60);

/// Start evicting when usage exceeds this fraction of max_size_bytes.
const HIGH_WATERMARK: f64 = 0.95;

/// Evict down to this fraction of max_size_bytes.
const LOW_WATERMARK: f64 = 0.90;

// ── In-memory LRU tracker ──────────────────────────────────────────

/// Mutable inner state of the cache tracker, protected by a Mutex.
struct TrackerInner {
    /// LRU map from (blob_id, vol) -> approximate disk_bytes.
    /// The LRU ordering is maintained automatically by `get`/`push`.
    lru: LruCache<(Uuid, u16), u64>,
    /// Approximate total disk usage in bytes.
    total_usage: u64,
}

/// Tracks cache file access order and approximate total disk usage.
///
/// Uses `lru::LruCache` (linked HashMap) for O(1) touch/insert/pop_lru.
/// All operations hold a `Mutex` for a short critical section (~50ns
/// of pointer updates), which is negligible at FUSE request rates.
struct CacheTracker {
    inner: Mutex<TrackerInner>,
}

impl CacheTracker {
    fn new() -> Self {
        Self {
            inner: Mutex::new(TrackerInner {
                lru: LruCache::unbounded(),
                total_usage: 0,
            }),
        }
    }

    /// Record an access to a cache file (promotes to MRU).
    fn touch(&self, blob_id: Uuid, vol: u16) {
        let mut inner = self.inner.lock().expect("tracker lock poisoned");
        // `get` promotes the entry to the most-recently-used position.
        let _ = inner.lru.get(&(blob_id, vol));
    }

    /// Record a new block insertion. Returns the new total_usage.
    fn record_insert(&self, blob_id: Uuid, vol: u16, added_bytes: u64) -> u64 {
        let mut inner = self.inner.lock().expect("tracker lock poisoned");
        // `get` promotes to MRU and returns mutable ref to the value.
        if let Some(disk_bytes) = inner.lru.get_mut(&(blob_id, vol)) {
            *disk_bytes += added_bytes;
        } else {
            inner.lru.push((blob_id, vol), added_bytes);
        }
        inner.total_usage += added_bytes;
        inner.total_usage
    }

    fn current_usage(&self) -> u64 {
        self.inner
            .lock()
            .expect("tracker lock poisoned")
            .total_usage
    }

    /// Remove a file from tracking. Subtracts tracked bytes from total_usage.
    fn remove(&self, blob_id: Uuid, vol: u16) {
        let mut inner = self.inner.lock().expect("tracker lock poisoned");
        if let Some(disk_bytes) = inner.lru.pop(&(blob_id, vol)) {
            inner.total_usage = inner.total_usage.saturating_sub(disk_bytes);
        }
    }

    /// Pop the least-recently-used entry. Returns its key and tracked bytes.
    fn pop_lru(&self) -> Option<((Uuid, u16), u64)> {
        let mut inner = self.inner.lock().expect("tracker lock poisoned");
        let ((blob_id, vol), disk_bytes) = inner.lru.pop_lru()?;
        inner.total_usage = inner.total_usage.saturating_sub(disk_bytes);
        Some(((blob_id, vol), disk_bytes))
    }

    /// Insert a cold-start entry (appended at LRU end, i.e. oldest).
    fn insert_cold(&self, blob_id: Uuid, vol: u16, disk_bytes: u64) {
        let mut inner = self.inner.lock().expect("tracker lock poisoned");
        inner.lru.push((blob_id, vol), disk_bytes);
        // Demote to LRU position (oldest). `demote` moves to the back
        // of the internal list, which is the LRU end.
        inner.lru.demote(&(blob_id, vol));
        inner.total_usage += disk_bytes;
    }

    /// Number of tracked files.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().expect("tracker lock poisoned").lru.len()
    }

    /// Peek at the LRU ordering (oldest first) without modifying it.
    /// Used only in tests.
    #[cfg(test)]
    fn peek_lru_order(&self) -> Vec<(Uuid, u16)> {
        let inner = self.inner.lock().expect("tracker lock poisoned");
        // `iter()` returns entries from most-recently-used to least.
        // Reverse to get oldest-first.
        inner
            .lru
            .iter()
            .rev()
            .map(|(&(blob_id, vol), _)| (blob_id, vol))
            .collect()
    }
}

// ── DiskCache ──────────────────────────────────────────────────────

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
    high_bytes: u64,
    low_bytes: u64,
    tracker: Arc<CacheTracker>,
}

#[allow(dead_code)]
impl DiskCache {
    /// Create a new DiskCache. Creates the cache directory if needed,
    /// verifies the filesystem supports SEEK_DATA (ext4/xfs), and
    /// performs a cold-start scan to populate the in-memory tracker.
    pub fn new(
        cache_dir: impl Into<PathBuf>,
        max_size_gb: u64,
        block_size: u64,
    ) -> io::Result<Self> {
        let cache_dir = cache_dir.into();
        std::fs::create_dir_all(&cache_dir)?;

        // Verify filesystem type supports sparse file hole detection
        verify_filesystem(&cache_dir)?;

        let max_size_bytes = max_size_gb * 1024 * 1024 * 1024;
        let tracker = Arc::new(CacheTracker::new());

        // Cold-start: populate tracker from existing cache files
        cold_start_scan(&cache_dir, &tracker);

        Ok(Self {
            cache_dir,
            max_size_bytes,
            block_size,
            high_bytes: (max_size_bytes as f64 * HIGH_WATERMARK) as u64,
            low_bytes: (max_size_bytes as f64 * LOW_WATERMARK) as u64,
            tracker,
        })
    }

    /// Spawn a background evictor task that checks usage every 60s.
    ///
    /// The periodic check is O(1) (atomic read of total_usage). Eviction
    /// only runs when usage exceeds the high watermark (95%), evicting
    /// down to the low watermark (90%).
    pub fn spawn_evictor(&self) {
        let cache_dir = self.cache_dir.clone();
        let tracker = self.tracker.clone();
        let high = self.high_bytes;
        let low = self.low_bytes;

        compio_runtime::spawn(async move {
            loop {
                compio_runtime::time::sleep(EVICTION_INTERVAL).await;
                if tracker.current_usage() > high {
                    let dir = cache_dir.clone();
                    let t = tracker.clone();
                    let _ = compio_runtime::spawn_blocking(move || {
                        run_eviction(&dir, &t, low);
                    })
                    .await;
                }
            }
        })
        .detach();
    }

    /// Fire-and-forget an urgent eviction (e.g. after ENOSPC).
    fn request_eviction(&self) {
        let cache_dir = self.cache_dir.clone();
        let tracker = self.tracker.clone();
        let low = self.low_bytes;
        compio_runtime::spawn(async move {
            let _ = compio_runtime::spawn_blocking(move || {
                run_eviction(&cache_dir, &tracker, low);
            })
            .await;
        })
        .detach();
    }

    /// Read a cached block. Returns None if the block is not cached or
    /// if checksum verification fails.
    pub async fn get(
        &self,
        blob_id: Uuid,
        vol: u16,
        block: u32,
        content_length: u64,
    ) -> Option<Bytes> {
        let path = self.cache_file_path(blob_id, vol);
        let file = File::open(&path).await.ok()?;
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
        let buf = vec![0u8; block_len];
        let BufResult(r, data) = file.read_at(buf, block_offset).await;
        if r.ok()? != block_len {
            return None;
        }

        // Read checksum from checksum region
        let checksum_offset = content_length + block as u64 * 8;
        let buf = vec![0u8; 8];
        let BufResult(r, checksum_buf) = file.read_at(buf, checksum_offset).await;
        if r.ok()? != 8 {
            return None;
        }

        let stored_checksum = u64::from_le_bytes(checksum_buf[..8].try_into().ok()?);

        // Verify checksum
        let computed = xxhash_rust::xxh3::xxh3_64(&data);
        if computed != stored_checksum {
            tracing::warn!(
                %blob_id, vol, block,
                "disk cache checksum mismatch, deleting cache file"
            );
            self.tracker.remove(blob_id, vol);
            let _ = compio_fs::remove_file(&path).await;
            return None;
        }

        self.tracker.touch(blob_id, vol);
        Some(Bytes::from(data))
    }

    /// Read a cached block directly into a caller-provided buffer (zero-copy path).
    ///
    /// Returns `Some(bytes_read)` on hit, `None` on miss or checksum failure.
    /// The first `bytes_read` bytes of `buf` contain the block data.
    pub async fn get_into(
        &self,
        blob_id: Uuid,
        vol: u16,
        block: u32,
        content_length: u64,
        buf: &mut [u8],
    ) -> Option<usize> {
        let path = self.cache_file_path(blob_id, vol);
        let file = File::open(&path).await.ok()?;
        let fd = std::os::fd::AsRawFd::as_raw_fd(&file);

        if !is_block_populated(fd, block, self.block_size) {
            return None;
        }

        let block_offset = block as u64 * self.block_size;
        let block_end = std::cmp::min(block_offset + self.block_size, content_length);
        let block_len = (block_end - block_offset) as usize;

        if block_len > buf.len() {
            return None;
        }

        // Read block data directly into the caller's buffer
        let slice_buf = unsafe { SliceMut::new(buf.as_mut_ptr(), block_len) };
        let BufResult(r, _) = file.read_at(slice_buf, block_offset).await;
        if r.ok()? != block_len {
            return None;
        }

        // Read checksum from checksum region
        let checksum_offset = content_length + block as u64 * 8;
        let checksum_vec = vec![0u8; 8];
        let BufResult(r, checksum_buf) = file.read_at(checksum_vec, checksum_offset).await;
        if r.ok()? != 8 {
            return None;
        }

        let stored_checksum = u64::from_le_bytes(checksum_buf[..8].try_into().ok()?);

        // Verify checksum against data already in the caller's buffer
        let computed = xxhash_rust::xxh3::xxh3_64(&buf[..block_len]);
        if computed != stored_checksum {
            tracing::warn!(
                %blob_id, vol, block,
                "disk cache checksum mismatch, deleting cache file"
            );
            self.tracker.remove(blob_id, vol);
            let _ = compio_fs::remove_file(&path).await;
            return None;
        }

        self.tracker.touch(blob_id, vol);
        Some(block_len)
    }

    /// Write a block to cache. Creates the cache file if it doesn't exist.
    pub async fn insert(
        &self,
        blob_id: Uuid,
        vol: u16,
        block: u32,
        content_length: u64,
        data: &[u8],
        checksum: u64,
    ) {
        let path = self.cache_file_path(blob_id, vol);
        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(%blob_id, vol, block, error = %e, "failed to open cache file");
                return;
            }
        };

        // Check if this block is already cached (avoid double-counting usage).
        let fd = std::os::fd::AsRawFd::as_raw_fd(&file);
        let new_block = !is_block_populated(fd, block, self.block_size);

        let block_offset = block as u64 * self.block_size;

        // Write block data
        let BufResult(r, _) = file.write_at(data.to_vec(), block_offset).await;
        if let Err(e) = r {
            if e.kind() == io::ErrorKind::StorageFull {
                self.request_eviction();
            }
            tracing::warn!(%blob_id, vol, block, error = %e, "failed to write cache data");
            return;
        }

        // Write checksum in checksum region
        let checksum_offset = content_length + block as u64 * 8;
        let BufResult(r, _) = file
            .write_at(checksum.to_le_bytes().to_vec(), checksum_offset)
            .await;
        if let Err(e) = r {
            if e.kind() == io::ErrorKind::StorageFull {
                self.request_eviction();
            }
            tracing::warn!(%blob_id, vol, block, error = %e, "failed to write cache checksum");
            return;
        }

        // Ensure data + checksum are persisted
        if let Err(e) = file.sync_data().await {
            tracing::warn!(%blob_id, vol, block, error = %e, "failed to sync cache file");
        }

        // Update tracker after successful write
        if new_block {
            let new_total = self.tracker.record_insert(blob_id, vol, data.len() as u64);
            if new_total > self.high_bytes {
                self.request_eviction();
            }
        } else {
            self.tracker.touch(blob_id, vol);
        }
    }

    /// Check if all blocks of an object are populated (ready for passthrough).
    pub fn is_complete(&self, blob_id: Uuid, vol: u16, content_length: u64) -> bool {
        if content_length == 0 {
            return false;
        }

        let path = self.cache_file_path(blob_id, vol);
        let file = match std::fs::File::open(&path) {
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
    /// Runs on a blocking thread pool. The returned handle can be awaited
    /// when the caller needs to know eviction finished.
    pub fn evict_to(&self, target_bytes: u64) -> compio_runtime::JoinHandle<()> {
        let cache_dir = self.cache_dir.clone();
        let tracker = self.tracker.clone();
        compio_runtime::spawn_blocking(move || {
            run_eviction(&cache_dir, &tracker, target_bytes);
        })
    }

    /// Get current approximate disk usage of the cache in bytes (O(1)).
    pub fn current_usage(&self) -> u64 {
        self.tracker.current_usage()
    }

    /// Number of files tracked by the in-memory tracker.
    #[cfg(test)]
    fn tracked_file_count(&self) -> usize {
        self.tracker.len()
    }
}

// ── Helpers ────────────────────────────────────────────────────────

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

/// Populate the tracker from existing cache files on startup.
/// Cold-start files are inserted at the LRU end (oldest = first eviction candidates).
fn cold_start_scan(cache_dir: &Path, tracker: &CacheTracker) {
    let entries = match std::fs::read_dir(cache_dir) {
        Ok(e) => e,
        Err(_) => return,
    };

    let mut count = 0u32;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some((blob_id, vol)) = parse_cache_filename(&path)
            && let Ok(meta) = std::fs::metadata(&path)
        {
            let disk_bytes = meta.blocks() * 512;
            tracker.insert_cold(blob_id, vol, disk_bytes);
            count += 1;
        }
    }
    if count > 0 {
        tracing::info!(
            count,
            total_bytes = tracker.current_usage(),
            "disk cache cold-start scan complete"
        );
    }
}

/// Parse a cache filename (`{uuid_simple}_{vol}`) into its components.
fn parse_cache_filename(path: &Path) -> Option<(Uuid, u16)> {
    let name = path.file_name()?.to_str()?;
    let (blob_str, vol_str) = name.rsplit_once('_')?;
    let blob_id = Uuid::parse_str(blob_str).ok()?;
    let vol = vol_str.parse::<u16>().ok()?;
    Some((blob_id, vol))
}

/// Evict LRU cache files until usage drops to `target_bytes` or below.
/// Pops entries from the LRU end in O(1) per file. Each `pop_lru` call
/// holds the tracker Mutex only for pointer updates (~50ns), then releases
/// it before the blocking `remove_file` syscall.
fn run_eviction(cache_dir: &Path, tracker: &CacheTracker, target_bytes: u64) {
    if tracker.current_usage() <= target_bytes {
        return;
    }

    tracing::info!(
        current_usage = tracker.current_usage(),
        target_bytes,
        "disk cache eviction started"
    );

    let mut evicted = 0u64;

    while tracker.current_usage() > target_bytes {
        let Some(((blob_id, vol), _disk_bytes)) = tracker.pop_lru() else {
            break;
        };

        let path = cache_dir.join(format!("{}_{}", blob_id.as_simple(), vol));
        let _ = std::fs::remove_file(&path);
        evicted += 1;
    }

    tracing::info!(
        evicted_files = evicted,
        remaining_bytes = tracker.current_usage(),
        "disk cache eviction complete"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::FileExt;
    use std::sync::atomic::{AtomicU32, Ordering};

    static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

    fn test_cache_dir() -> PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("test_disk_cache_{}_{}", std::process::id(), id));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[compio_macros::test]
    async fn test_insert_and_get() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let blob_id = Uuid::new_v4();
        let vol = 1u16;
        let data = vec![42u8; 1024];
        let checksum = xxhash_rust::xxh3::xxh3_64(&data);
        let content_length = 4096u64; // 4 blocks of 1024

        cache
            .insert(blob_id, vol, 0, content_length, &data, checksum)
            .await;

        let result = cache.get(blob_id, vol, 0, content_length).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_ref(), &data[..]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_get_into() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let blob_id = Uuid::new_v4();
        let vol = 1u16;
        let data = vec![42u8; 1024];
        let checksum = xxhash_rust::xxh3::xxh3_64(&data);
        let content_length = 4096u64;

        cache
            .insert(blob_id, vol, 0, content_length, &data, checksum)
            .await;

        // Read into a pre-allocated buffer
        let mut buf = vec![0u8; 1024];
        let result = cache
            .get_into(blob_id, vol, 0, content_length, &mut buf)
            .await;
        assert_eq!(result, Some(1024));
        assert_eq!(&buf[..], &data[..]);

        // Miss: wrong block
        let mut buf2 = vec![0u8; 1024];
        let result = cache
            .get_into(blob_id, vol, 1, content_length, &mut buf2)
            .await;
        assert!(result.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_get_missing_block() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let blob_id = Uuid::new_v4();
        let result = cache.get(blob_id, 1, 0, 4096).await;
        assert!(result.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_get_hole_block() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let blob_id = Uuid::new_v4();
        let vol = 1u16;
        let data = vec![55u8; 1024];
        let checksum = xxhash_rust::xxh3::xxh3_64(&data);
        let content_length = 8192u64;

        // Insert block 5, try to get block 3 (should be a hole)
        cache
            .insert(blob_id, vol, 5, content_length, &data, checksum)
            .await;

        let result = cache.get(blob_id, vol, 3, content_length).await;
        assert!(result.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_checksum_corruption() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let blob_id = Uuid::new_v4();
        let vol = 1u16;
        let data = vec![99u8; 1024];
        let checksum = xxhash_rust::xxh3::xxh3_64(&data);
        let content_length = 4096u64;

        cache
            .insert(blob_id, vol, 0, content_length, &data, checksum)
            .await;

        // Corrupt data on disk (use std::fs for direct corruption)
        let path = cache.cache_file_path(blob_id, vol);
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&[0u8; 10], 0).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // get should return None and delete the file
        let result = cache.get(blob_id, vol, 0, content_length).await;
        assert!(result.is_none());
        assert!(!path.exists());

        // Tracker should also be cleaned up
        assert_eq!(cache.tracked_file_count(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_is_complete() {
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
            cache
                .insert(blob_id, vol, block, content_length, &data, checksum)
                .await;
        }

        assert!(cache.is_complete(blob_id, vol, content_length));

        // Partial: block 0 and block 2 only (gap at block 1)
        let blob_id2 = Uuid::new_v4();
        for block in [0, 2] {
            let data = vec![block as u8; block_size as usize];
            let checksum = xxhash_rust::xxh3::xxh3_64(&data);
            cache
                .insert(blob_id2, vol, block, content_length, &data, checksum)
                .await;
        }

        assert!(!cache.is_complete(blob_id2, vol, content_length));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_evict() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        // Insert 3 cache files
        for i in 0..3 {
            let blob_id = Uuid::from_u128(i as u128 + 1);
            let data = vec![i as u8; 4096];
            let checksum = xxhash_rust::xxh3::xxh3_64(&data);
            cache.insert(blob_id, 1, 0, 8192, &data, checksum).await;
        }

        assert_eq!(cache.tracked_file_count(), 3);
        assert!(cache.current_usage() > 0);

        // Evict to 0 bytes -- should remove everything.
        // Await the handle so the test can verify the result.
        cache.evict_to(0).await.unwrap();

        let remaining: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();
        assert!(remaining.is_empty());

        // Tracker should be empty too
        assert_eq!(cache.tracked_file_count(), 0);
        assert_eq!(cache.current_usage(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_tracker_touch_updates_lru() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();
        let content_length = 3 * block_size;

        // Insert 3 files: blob1, blob2, blob3 (one block each)
        let blobs: Vec<Uuid> = (1..=3).map(Uuid::from_u128).collect();
        for (i, blob_id) in blobs.iter().enumerate() {
            let data = vec![i as u8 + 1; block_size as usize];
            let checksum = xxhash_rust::xxh3::xxh3_64(&data);
            cache
                .insert(*blob_id, 1, 0, content_length, &data, checksum)
                .await;
        }

        // Access blob1 via get(), making it most recently used
        let _ = cache.get(blobs[0], 1, 0, content_length).await;

        // LRU order should have blob2 and blob3 first (older access)
        let lru = cache.tracker.peek_lru_order();
        assert_eq!(lru.len(), 3);
        // blob1 should be last (most recently accessed)
        assert_eq!(lru[2].0, blobs[0]);
        // blob2 and blob3 should be first two (order between them
        // depends on insert order, but both before blob1)
        let first_two: Vec<Uuid> = lru[..2].iter().map(|e| e.0).collect();
        assert!(first_two.contains(&blobs[1]));
        assert!(first_two.contains(&blobs[2]));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_cold_start_scan() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let content_length = 3 * block_size;

        // Phase 1: Create cache files with a DiskCache instance
        {
            let cache = DiskCache::new(&dir, 1, block_size).unwrap();
            for i in 0..3 {
                let blob_id = Uuid::from_u128(i as u128 + 100);
                let data = vec![i as u8 + 1; block_size as usize];
                let checksum = xxhash_rust::xxh3::xxh3_64(&data);
                cache
                    .insert(blob_id, 1, 0, content_length, &data, checksum)
                    .await;
            }
            assert_eq!(cache.tracked_file_count(), 3);
            assert!(cache.current_usage() > 0);
        }

        // Phase 2: Create a new DiskCache over the same directory.
        // The cold-start scan should find the 3 existing files.
        let cache2 = DiskCache::new(&dir, 1, block_size).unwrap();
        assert_eq!(cache2.tracked_file_count(), 3);
        assert!(cache2.current_usage() > 0);

        // Cold-start files should have last_access = 0, making them
        // oldest in LRU order. Verify by inserting a new file and
        // checking it's last in LRU.
        let new_blob = Uuid::from_u128(999);
        let data = vec![77u8; block_size as usize];
        let checksum = xxhash_rust::xxh3::xxh3_64(&data);
        cache2
            .insert(new_blob, 1, 0, content_length, &data, checksum)
            .await;

        let lru = cache2.tracker.peek_lru_order();
        assert_eq!(lru.len(), 4);
        // The new blob should be last (highest access counter)
        assert_eq!(lru[3].0, new_blob);
        // All cold-start blobs should be first (last_access = 0)
        for entry in &lru[..3] {
            assert_ne!(entry.0, new_blob);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_insert_tracks_usage() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();
        let content_length = 3 * block_size;

        assert_eq!(cache.current_usage(), 0);

        let blob_id = Uuid::new_v4();
        let data = vec![42u8; block_size as usize];
        let checksum = xxhash_rust::xxh3::xxh3_64(&data);
        cache
            .insert(blob_id, 1, 0, content_length, &data, checksum)
            .await;

        // Usage should be approximately block_size bytes
        assert!(cache.current_usage() >= block_size);
        assert_eq!(cache.tracked_file_count(), 1);

        // Insert another block in the same file
        let data2 = vec![43u8; block_size as usize];
        let checksum2 = xxhash_rust::xxh3::xxh3_64(&data2);
        cache
            .insert(blob_id, 1, 1, content_length, &data2, checksum2)
            .await;

        // Usage should increase (still 1 file, but more bytes)
        assert!(cache.current_usage() >= 2 * block_size);
        assert_eq!(cache.tracked_file_count(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_reinsert_same_block_no_double_count() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();
        let content_length = 3 * block_size;

        let blob_id = Uuid::new_v4();
        let data = vec![42u8; block_size as usize];
        let checksum = xxhash_rust::xxh3::xxh3_64(&data);

        cache
            .insert(blob_id, 1, 0, content_length, &data, checksum)
            .await;
        let usage_after_first = cache.current_usage();

        // Re-insert the same block -- should NOT double-count
        cache
            .insert(blob_id, 1, 0, content_length, &data, checksum)
            .await;
        let usage_after_second = cache.current_usage();

        assert_eq!(usage_after_first, usage_after_second);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_evict_respects_lru_order() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();
        let content_length = 3 * block_size;

        // Insert blob1, blob2, blob3 in order
        let blob1 = Uuid::from_u128(1);
        let blob2 = Uuid::from_u128(2);
        let blob3 = Uuid::from_u128(3);

        for blob_id in [blob1, blob2, blob3] {
            let data = vec![0u8; block_size as usize];
            let checksum = xxhash_rust::xxh3::xxh3_64(&data);
            cache
                .insert(blob_id, 1, 0, content_length, &data, checksum)
                .await;
        }

        // Touch blob1 to make it most recently used
        let _ = cache.get(blob1, 1, 0, content_length).await;

        // Evict enough to remove 1-2 files but not all.
        // Set target to keep only ~1 file worth of data.
        let one_file_bytes = cache.current_usage() / 3;
        cache.evict_to(one_file_bytes).await.unwrap();

        // blob1 should survive (most recently accessed)
        assert!(
            cache.cache_file_path(blob1, 1).exists(),
            "blob1 should survive eviction (most recently accessed)"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_parse_cache_filename() {
        let uuid = Uuid::from_u128(0x12345678_1234_1234_1234_123456789abc);
        let path = PathBuf::from(format!("/tmp/cache/{}_{}", uuid.as_simple(), 42));
        let result = parse_cache_filename(&path);
        assert_eq!(result, Some((uuid, 42)));

        // Invalid filenames
        assert_eq!(
            parse_cache_filename(&PathBuf::from("/tmp/cache/invalid")),
            None
        );
        assert_eq!(
            parse_cache_filename(&PathBuf::from("/tmp/cache/abc_def")),
            None
        );
    }
}
