use parking_lot::Mutex;
use std::io;
use std::num::NonZeroUsize;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use compio_buf::BufResult;
use compio_fs::{File, OpenOptions};
use compio_io::{AsyncReadAt, AsyncWriteAt};
use data_types::DataBlobGuid;

use crate::slice_mut::SliceMut;
use lru::LruCache;
use uuid::Uuid;

// On-disk layout
//
// Each blob has a single sparse cache file at
// `{cache_dir}/{blob_id_simple}_{vol}` independent of `blob_version`.
//
// Region map:
//
//   [0, META_OFFSET)                  data region: block bytes at
//                                     natural offsets (block * block_size).
//
//   [META_OFFSET, META_OFFSET+32)     CacheHeader (file-level state, see
//                                     `CacheHeader` below).
//
//   [META_OFFSET+32, ...)             per-block metadata array, indexed
//                                     directly by block number. Each
//                                     entry is `BlockMeta` (16 bytes:
//                                     `block_version: u64`, `checksum: u64`).
//                                     `block_version == 0` is the
//                                     never-cached / hole sentinel.
//
// The file is sparse: `META_OFFSET` sits well above any plausible
// addressable data, so the metadata region's physical extents are
// disjoint from the data region's and the disk footprint stays
// proportional to actually-populated blocks plus the metadata
// entries for those blocks.
//
// Concretely we set `META_OFFSET = 1 << 40` (1 TiB). ext4's max
// file size at 4 KiB block size is 16 TiB; sitting at 1 TiB leaves
// 15 TiB for sparse data extents while staying well inside the
// filesystem's per-file ceiling. Per-block metadata at 16 B per
// block supports up to 2^28 blocks (256 M) per cache file.

const META_OFFSET: u64 = 1 << 40;
const HEADER_SIZE: u64 = 32;
const BLOCK_META_SIZE: u64 = 16;
const CACHE_MAGIC: u32 = 0x4642_4443; // "FBDC"
const CACHE_FORMAT_VERSION: u32 = 1;

/// File-level cache header. `commit_epoch` is the newest file commit or
/// mirror job reconciled with this cache file. It orders whole-file mirror
/// jobs and is independent of the exact generation stored for each block.
/// A different instance bumping past this value is the cue to invalidate the
/// file on next open.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
struct CacheHeader {
    magic: u32,
    format_version: u32,
    block_size: u32,
    block_count: u32,
    commit_epoch: u64,
    flags: u32,
    _reserved: u32,
}

impl CacheHeader {
    fn new(block_size: u32, commit_epoch: u64) -> Self {
        Self {
            magic: CACHE_MAGIC,
            format_version: CACHE_FORMAT_VERSION,
            block_size,
            block_count: 0,
            commit_epoch,
            flags: 0,
            _reserved: 0,
        }
    }

    fn to_bytes(self) -> [u8; HEADER_SIZE as usize] {
        let mut out = [0u8; HEADER_SIZE as usize];
        out[0..4].copy_from_slice(&self.magic.to_le_bytes());
        out[4..8].copy_from_slice(&self.format_version.to_le_bytes());
        out[8..12].copy_from_slice(&self.block_size.to_le_bytes());
        out[12..16].copy_from_slice(&self.block_count.to_le_bytes());
        out[16..24].copy_from_slice(&self.commit_epoch.to_le_bytes());
        out[24..28].copy_from_slice(&self.flags.to_le_bytes());
        out[28..32].copy_from_slice(&self._reserved.to_le_bytes());
        out
    }

    fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < HEADER_SIZE as usize {
            return None;
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().ok()?);
        if magic != CACHE_MAGIC {
            return None;
        }
        let format_version = u32::from_le_bytes(buf[4..8].try_into().ok()?);
        if format_version != CACHE_FORMAT_VERSION {
            return None;
        }
        Some(Self {
            magic,
            format_version,
            block_size: u32::from_le_bytes(buf[8..12].try_into().ok()?),
            block_count: u32::from_le_bytes(buf[12..16].try_into().ok()?),
            commit_epoch: u64::from_le_bytes(buf[16..24].try_into().ok()?),
            flags: u32::from_le_bytes(buf[24..28].try_into().ok()?),
            _reserved: u32::from_le_bytes(buf[28..32].try_into().ok()?),
        })
    }
}

/// Per-block metadata entry. `block_version == 0` is the
/// "never cached / hole" sentinel (real BSS versions start at 1).
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
struct BlockMeta {
    block_version: u64,
    checksum: u64,
}

impl BlockMeta {
    fn to_bytes(self) -> [u8; BLOCK_META_SIZE as usize] {
        let mut out = [0u8; BLOCK_META_SIZE as usize];
        out[0..8].copy_from_slice(&self.block_version.to_le_bytes());
        out[8..16].copy_from_slice(&self.checksum.to_le_bytes());
        out
    }

    fn from_bytes(buf: &[u8]) -> Option<Self> {
        if buf.len() < BLOCK_META_SIZE as usize {
            return None;
        }
        Some(Self {
            block_version: u64::from_le_bytes(buf[0..8].try_into().ok()?),
            checksum: u64::from_le_bytes(buf[8..16].try_into().ok()?),
        })
    }

    fn is_cached(&self) -> bool {
        self.block_version != 0
    }
}

fn block_meta_offset(block: u32) -> u64 {
    META_OFFSET + HEADER_SIZE + (block as u64) * BLOCK_META_SIZE
}

const EVICTION_INTERVAL: Duration = Duration::from_secs(60);
const HIGH_WATERMARK: f64 = 0.95;
const LOW_WATERMARK: f64 = 0.90;

// In-memory LRU tracker

/// Mutable inner state of the cache tracker, protected by a Mutex.
struct TrackerInner {
    /// LRU map from `(blob_id, vol) -> approximate disk_bytes`.
    /// One entry per cache file. Eviction unlinks the whole file.
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
        let mut inner = self.inner.lock();
        let _ = inner.lru.get(&(blob_id, vol));
    }

    /// Record a new block insertion. Returns the new total_usage.
    fn record_insert(&self, blob_id: Uuid, vol: u16, added_bytes: u64) -> u64 {
        let mut inner = self.inner.lock();
        if let Some(disk_bytes) = inner.lru.get_mut(&(blob_id, vol)) {
            *disk_bytes += added_bytes;
        } else {
            inner.lru.push((blob_id, vol), added_bytes);
        }
        inner.total_usage += added_bytes;
        inner.total_usage
    }

    fn current_usage(&self) -> u64 {
        self.inner.lock().total_usage
    }

    /// Remove a file from tracking. Subtracts tracked bytes from total_usage.
    fn remove(&self, blob_id: Uuid, vol: u16) {
        let mut inner = self.inner.lock();
        if let Some(disk_bytes) = inner.lru.pop(&(blob_id, vol)) {
            inner.total_usage = inner.total_usage.saturating_sub(disk_bytes);
        }
    }

    /// Pop the least-recently-used entry. Returns its key and tracked bytes.
    fn pop_lru(&self) -> Option<((Uuid, u16), u64)> {
        let mut inner = self.inner.lock();
        let ((blob_id, vol), disk_bytes) = inner.lru.pop_lru()?;
        inner.total_usage = inner.total_usage.saturating_sub(disk_bytes);
        Some(((blob_id, vol), disk_bytes))
    }

    /// Insert a cold-start entry (appended at LRU end, i.e. oldest).
    fn insert_cold(&self, blob_id: Uuid, vol: u16, disk_bytes: u64) {
        let mut inner = self.inner.lock();
        inner.lru.push((blob_id, vol), disk_bytes);
        inner.lru.demote(&(blob_id, vol));
        inner.total_usage += disk_bytes;
    }

    /// Number of tracked files.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().lru.len()
    }

    /// Peek at the LRU ordering (oldest first) without modifying it.
    /// Used only in tests.
    #[cfg(test)]
    fn peek_lru_order(&self) -> Vec<(Uuid, u16)> {
        let inner = self.inner.lock();
        inner.lru.iter().rev().map(|(&k, _)| k).collect()
    }
}

// DiskCache

/// Local NVMe disk cache for block data.
///
/// Each blob maps to a sparse cache file at
/// `{cache_dir}/{blob_id}_{volume_id}` (stable across version bumps).
/// Block bytes live at their natural offset; per-block metadata
/// (version + checksum) and a file-level header live in a metadata
/// region far above the data region (see "On-disk layout" at top of
/// file).
///
/// Override flush updates the cache file in place via
/// `sync_after_flush`: rewrites land on the data region, deletes
/// punch holes, and the per-block version + checksum are bumped in
/// the metadata region. A reader that hits a block whose checksum
/// doesn't match the cached bytes (e.g. torn-write crash recovery)
/// treats it as a miss and refetches.
///
/// Cross-instance staleness is detected at open time via
/// `reconcile_on_open`: if the cache file's commit epoch lags the inode's
/// `layout.blob_version` (another instance has committed a newer layout), the
/// whole cache file is unlinked so the next read cold-fetches from BSS.
#[allow(dead_code)]
/// Number of stripe locks serializing per-blob cache-file mutations.
/// Fixed (not per-blob) to bound memory; distinct blobs that collide on a
/// stripe only see rare, brief contention.
const MIRROR_LOCK_STRIPES: usize = 256;
/// Process-local commit epochs fence delayed mirror jobs after their cache file
/// has been dropped. Bound the map so a long-lived process that overrides many
/// distinct blobs cannot grow it forever.
const COMMIT_EPOCH_CAP: usize = 65_536;

pub struct DiskCache {
    cache_dir: PathBuf,
    max_size_bytes: u64,
    block_size: u64,
    high_bytes: u64,
    low_bytes: u64,
    tracker: Arc<CacheTracker>,
    /// Highest file commit epoch this process has observed for each cache
    /// file. Kept outside the cache file so dropping the file does not let a
    /// delayed older mirror job recreate stale cache contents.
    commit_epochs: Arc<Mutex<LruCache<(Uuid, u16), u64>>>,
    /// Serializes cache writers for the same blob so they cannot race on the
    /// cache file or its commit epoch. Striped to bound memory. With the
    /// inline mirror this was guaranteed by the single-writer-per-inode
    /// lock; the async create mirror runs on its own thread, so the
    /// serialization is reestablished here.
    mirror_locks: Vec<futures::lock::Mutex<()>>,
}

impl DiskCache {
    /// Create a new DiskCache. Creates the cache directory if needed,
    /// verifies the filesystem supports SEEK_DATA (ext4/xfs), and
    /// performs a cold-start scan to populate the in-memory tracker.
    pub fn new(
        cache_dir: impl Into<PathBuf>,
        max_size_gb: u64,
        block_size: u64,
    ) -> io::Result<Self> {
        Self::new_with_commit_epoch_cap(
            cache_dir,
            max_size_gb,
            block_size,
            NonZeroUsize::new(COMMIT_EPOCH_CAP).expect("commit epoch cap is nonzero"),
        )
    }

    fn new_with_commit_epoch_cap(
        cache_dir: impl Into<PathBuf>,
        max_size_gb: u64,
        block_size: u64,
        commit_epoch_cap: NonZeroUsize,
    ) -> io::Result<Self> {
        let cache_dir = cache_dir.into();
        std::fs::create_dir_all(&cache_dir)?;

        // Verify filesystem type supports sparse file hole detection
        verify_filesystem(&cache_dir)?;

        let max_size_bytes = max_size_gb * 1024 * 1024 * 1024;
        let tracker = Arc::new(CacheTracker::new());
        let commit_epochs = Arc::new(Mutex::new(LruCache::new(commit_epoch_cap)));

        // Cold-start: populate tracker from existing cache files
        cold_start_scan(&cache_dir, &tracker);

        let mirror_locks = (0..MIRROR_LOCK_STRIPES)
            .map(|_| futures::lock::Mutex::new(()))
            .collect();

        Ok(Self {
            cache_dir,
            max_size_bytes,
            block_size,
            high_bytes: (max_size_bytes as f64 * HIGH_WATERMARK) as u64,
            low_bytes: (max_size_bytes as f64 * LOW_WATERMARK) as u64,
            tracker,
            commit_epochs,
            mirror_locks,
        })
    }

    /// The stripe lock guarding cache-file mutations for `blob_guid`.
    fn mirror_lock(&self, blob_guid: DataBlobGuid) -> &futures::lock::Mutex<()> {
        let idx = (blob_guid.blob_id.as_u128() as usize) % self.mirror_locks.len();
        &self.mirror_locks[idx]
    }

    fn memory_commit_epoch(&self, blob_id: Uuid, vol: u16) -> u64 {
        self.commit_epochs
            .lock()
            .get(&(blob_id, vol))
            .copied()
            .unwrap_or(0)
    }

    fn record_commit_epoch(&self, blob_id: Uuid, vol: u16, commit_epoch: u64) {
        if commit_epoch <= 1 {
            return;
        }
        let mut epochs = self.commit_epochs.lock();
        if let Some(entry) = epochs.get_mut(&(blob_id, vol)) {
            if commit_epoch > *entry {
                *entry = commit_epoch;
            }
        } else {
            epochs.push((blob_id, vol), commit_epoch);
        }
    }

    /// Spawn a background evictor task that checks usage every 60s.
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

    /// Cache file path. Stable across version bumps for a given blob.
    pub fn cache_file_path(&self, blob_id: Uuid, vol: u16) -> PathBuf {
        self.cache_dir
            .join(format!("{}_{}", blob_id.as_simple(), vol))
    }

    /// Read a cached block without checking its generation. Committed-layout
    /// callers should use `get_block_exact`. Returns None on miss (block never
    /// cached or checksum mismatch, both treated identically by callers).
    ///
    /// The cache always stores blocks at `self.block_size` bytes
    /// (zero-padded by writers; BSS pads identically on the
    /// network), and the stored checksum is over those `block_size`
    /// bytes. We read+checksum the full block, then truncate the
    /// returned bytes to `block_content_len` for the caller. This
    /// keeps the on-disk representation size-invariant across file_size
    /// changes (a `truncate` that extends the file doesn't invalidate
    /// previously-cached blocks).
    pub async fn get_block(
        &self,
        blob_guid: DataBlobGuid,
        block: u32,
        block_content_len: usize,
    ) -> Option<Bytes> {
        self.get_block_with_version(blob_guid, block, None, block_content_len)
            .await
    }

    /// Read a cached block only when its metadata carries `expected_version`.
    /// The version and checksum are validated from the same metadata record,
    /// so a caller never needs a separate version probe before reading bytes.
    pub async fn get_block_exact(
        &self,
        blob_guid: DataBlobGuid,
        block: u32,
        expected_version: u64,
        block_content_len: usize,
    ) -> Option<Bytes> {
        self.get_block_with_version(blob_guid, block, Some(expected_version), block_content_len)
            .await
    }

    async fn get_block_with_version(
        &self,
        blob_guid: DataBlobGuid,
        block: u32,
        expected_version: Option<u64>,
        block_content_len: usize,
    ) -> Option<Bytes> {
        let blob_id = blob_guid.blob_id;
        let vol = blob_guid.volume_id;
        let path = self.cache_file_path(blob_id, vol);
        let file = File::open(&path).await.ok()?;
        let fd = std::os::fd::AsRawFd::as_raw_fd(&file);

        // Block must be populated (data, not a sparse hole).
        if !is_block_populated(fd, block, self.block_size) {
            return None;
        }

        let meta = read_block_meta(&file, block).await?;
        if !meta.is_cached()
            || expected_version.is_some_and(|version| meta.block_version != version)
        {
            return None;
        }

        // Read the full block.
        let block_offset = block as u64 * self.block_size;
        let bsz = self.block_size as usize;
        let buf = vec![0u8; bsz];
        let BufResult(r, data) = file.read_at(buf, block_offset).await;
        if r.ok()? != bsz {
            return None;
        }

        // xxhash gate over the full block. Torn-write between body
        // and metadata (mid-flush crash) produces a mismatch here ->
        // treat as miss. The stale metadata stays in place; the next
        // `insert_block` overwrites it with the freshly-fetched
        // bytes' checksum.
        let computed = xxhash_rust::xxh3::xxh3_64(&data);
        if computed != meta.checksum {
            tracing::warn!(
                %blob_id, vol, block,
                "disk cache checksum mismatch, treating as miss",
            );
            return None;
        }

        self.tracker.touch(blob_id, vol);
        // Truncate to what the caller asked for.
        let take = std::cmp::min(block_content_len, bsz);
        Some(Bytes::from(data).slice(..take))
    }

    /// Read a cached block into a caller-provided buffer without checking its
    /// generation. Committed-layout callers should use `get_block_into_exact`.
    /// Returns Some(bytes_read) on hit, None on miss.
    ///
    /// The on-disk block is `self.block_size` bytes long and the
    /// checksum is over the full block, so for a partial last block
    /// (where the caller's `block_content_len < self.block_size`) we
    /// must read+checksum the full block in a temporary buffer
    /// rather than the truncated zero-copy slice. The caller's
    /// buffer receives only the first `block_content_len` bytes.
    /// Whole-block reads (`block_content_len == self.block_size`)
    /// stay zero-copy.
    pub async fn get_block_into(
        &self,
        blob_guid: DataBlobGuid,
        block: u32,
        block_content_len: usize,
        buf: &mut [u8],
    ) -> Option<usize> {
        self.get_block_into_with_version(blob_guid, block, None, block_content_len, buf)
            .await
    }

    /// Read a cached block into `buf` only when its metadata carries
    /// `expected_version`. The version and checksum are validated from the
    /// same metadata record before the buffer is accepted as a cache hit.
    pub async fn get_block_into_exact(
        &self,
        blob_guid: DataBlobGuid,
        block: u32,
        expected_version: u64,
        block_content_len: usize,
        buf: &mut [u8],
    ) -> Option<usize> {
        self.get_block_into_with_version(
            blob_guid,
            block,
            Some(expected_version),
            block_content_len,
            buf,
        )
        .await
    }

    async fn get_block_into_with_version(
        &self,
        blob_guid: DataBlobGuid,
        block: u32,
        expected_version: Option<u64>,
        block_content_len: usize,
        buf: &mut [u8],
    ) -> Option<usize> {
        let blob_id = blob_guid.blob_id;
        let vol = blob_guid.volume_id;
        let path = self.cache_file_path(blob_id, vol);
        let file = File::open(&path).await.ok()?;
        let fd = std::os::fd::AsRawFd::as_raw_fd(&file);

        if !is_block_populated(fd, block, self.block_size) {
            return None;
        }

        let meta = read_block_meta(&file, block).await?;
        if !meta.is_cached()
            || expected_version.is_some_and(|version| meta.block_version != version)
        {
            return None;
        }

        if block_content_len > buf.len() {
            return None;
        }

        let block_offset = block as u64 * self.block_size;
        let bsz = self.block_size as usize;

        if block_content_len == bsz {
            // Whole-block zero-copy fast path.
            let slice_buf = unsafe { SliceMut::new(buf.as_mut_ptr(), bsz) };
            let BufResult(r, _) = file.read_at(slice_buf, block_offset).await;
            if r.ok()? != bsz {
                return None;
            }
            let computed = xxhash_rust::xxh3::xxh3_64(&buf[..bsz]);
            if computed != meta.checksum {
                tracing::warn!(
                    %blob_id, vol, block,
                    "disk cache checksum mismatch, treating as miss",
                );
                return None;
            }
        } else {
            // Partial-block path: read full block to a temp,
            // checksum-validate, then copy out the prefix the
            // caller asked for.
            let tmp_buf = vec![0u8; bsz];
            let BufResult(r, data) = file.read_at(tmp_buf, block_offset).await;
            if r.ok()? != bsz {
                return None;
            }
            let computed = xxhash_rust::xxh3::xxh3_64(&data);
            if computed != meta.checksum {
                tracing::warn!(
                    %blob_id, vol, block,
                    "disk cache checksum mismatch, treating as miss",
                );
                return None;
            }
            buf[..block_content_len].copy_from_slice(&data[..block_content_len]);
        }

        self.tracker.touch(blob_id, vol);
        Some(block_content_len)
    }

    /// Read just the per-block version (without fetching the body).
    /// Useful for diagnostics and tests. Returns None on miss.
    pub async fn get_block_version(&self, blob_guid: DataBlobGuid, block: u32) -> Option<u64> {
        let path = self.cache_file_path(blob_guid.blob_id, blob_guid.volume_id);
        let file = File::open(&path).await.ok()?;
        let meta = read_block_meta(&file, block).await?;
        if meta.is_cached() {
            Some(meta.block_version)
        } else {
            None
        }
    }

    /// Populate the cache with bytes fetched from BSS. Creates the
    /// cache file (and initializes the header) if it doesn't exist.
    pub async fn insert_block(
        &self,
        blob_guid: DataBlobGuid,
        block: u32,
        block_version: u64,
        bytes: &[u8],
    ) -> io::Result<()> {
        if block_version == 0 {
            // Sentinel value: refuse to store as a "cached" entry.
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block_version=0 is reserved as the not-cached sentinel",
            ));
        }
        let blob_id = blob_guid.blob_id;
        let vol = blob_guid.volume_id;
        let path = self.cache_file_path(blob_id, vol);

        // Serialize against drop_blob and sync_after_flush for this blob so a
        // mirror job cannot race the block write or header initialization.
        let _guard = self.mirror_lock(blob_guid).lock().await;
        let memory_commit_epoch = self.memory_commit_epoch(blob_id, vol);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .await
            .inspect_err(|e| {
                tracing::warn!(%blob_id, vol, block, error = %e, "failed to open cache file");
            })?;

        // The header epoch orders whole-file mirror jobs only. Exact block
        // generations are independent: a committed layout at epoch 2 can
        // still reference an unchanged version-1 block, so an exact v1 cold
        // fill must be admitted without lowering the epoch. A new or malformed
        // header inherits the process-local epoch when available; otherwise it
        // starts at the initial commit epoch.
        match read_header(&file).await {
            Some(mut hdr) => {
                let commit_epoch = memory_commit_epoch.max(hdr.commit_epoch);
                if commit_epoch > hdr.commit_epoch {
                    hdr.commit_epoch = commit_epoch;
                    write_header(&mut file, &hdr).await?;
                }
                self.record_commit_epoch(blob_id, vol, commit_epoch);
            }
            None => {
                let commit_epoch = memory_commit_epoch.max(1);
                let header = CacheHeader::new(self.block_size as u32, commit_epoch);
                write_header(&mut file, &header).await?;
                self.record_commit_epoch(blob_id, vol, commit_epoch);
            }
        }

        // Check if this block was already cached (avoid double-counting).
        let fd = std::os::fd::AsRawFd::as_raw_fd(&file);
        let new_block = !is_block_populated(fd, block, self.block_size);

        // Pad to block_size before write so the on-disk
        // representation and the stored checksum are size-invariant.
        // BSS pads identically on the wire, so a cache populated
        // from a BSS fetch and a cache populated from a flushed
        // writer end up identical for the same content.
        let bsz = self.block_size as usize;
        let padded = pad_to_block_size_owned(bytes, bsz);
        let checksum = xxhash_rust::xxh3::xxh3_64(&padded);

        // Write body.
        let block_offset = block as u64 * self.block_size;
        let BufResult(r, _) = file.write_at(padded, block_offset).await;
        if let Err(e) = r {
            if e.kind() == io::ErrorKind::StorageFull {
                self.request_eviction();
            }
            tracing::warn!(%blob_id, vol, block, error = %e, "failed to write cache data");
            return Err(e);
        }

        // Write metadata entry (checksum over the full block).
        let meta = BlockMeta {
            block_version,
            checksum,
        };
        write_block_meta(&mut file, block, &meta).await?;

        // Persist body + metadata.
        if let Err(e) = file.sync_data().await {
            tracing::warn!(%blob_id, vol, block, error = %e, "failed to sync cache file");
        }

        // Update tracker.
        if new_block {
            let new_total = self.tracker.record_insert(blob_id, vol, bsz as u64);
            if new_total > self.high_bytes {
                self.request_eviction();
            }
        } else {
            self.tracker.touch(blob_id, vol);
        }

        Ok(())
    }

    /// Mark a single block as not-cached. Clears the metadata entry;
    /// the sparse data extent is reclaimable on next overwrite or
    /// whole-file eviction.
    pub async fn invalidate_block(&self, blob_guid: DataBlobGuid, block: u32) -> io::Result<()> {
        let path = self.cache_file_path(blob_guid.blob_id, blob_guid.volume_id);
        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .truncate(false)
            .open(&path)
            .await
        {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        clear_block_meta(&file, block).await
    }

    /// The newest file commit epoch recorded on disk or in this process.
    /// This value orders mirror jobs and does not constrain which exact block
    /// generations may be cached.
    pub async fn commit_epoch(&self, blob_guid: DataBlobGuid) -> Option<u64> {
        let memory_epoch = self.memory_commit_epoch(blob_guid.blob_id, blob_guid.volume_id);
        let path = self.cache_file_path(blob_guid.blob_id, blob_guid.volume_id);
        let file_epoch = match File::open(&path).await {
            Ok(file) => read_header(&file).await.map(|h| h.commit_epoch),
            Err(_) => None,
        };
        match (memory_epoch, file_epoch) {
            (0, None) => None,
            (epoch, None) => Some(epoch),
            (epoch, Some(file_epoch)) => Some(epoch.max(file_epoch)),
        }
    }

    /// Coherence-safe fallback: drop the entire cache file for a blob so
    /// every block misses and cold-fetches the authoritative bytes from
    /// BSS. Used when an override mirror fails partway: the superseded
    /// block body+metadata could otherwise remain a valid
    /// populated+checksum hit, so an override flush must not report success
    /// with that file still active. Holds the stripe lock so it cannot race a
    /// concurrent write for the same blob.
    pub async fn drop_blob(&self, blob_guid: DataBlobGuid, commit_epoch: u64) {
        let _guard = self.mirror_lock(blob_guid).lock().await;
        let blob_id = blob_guid.blob_id;
        let vol = blob_guid.volume_id;
        let path = self.cache_file_path(blob_id, vol);
        self.record_commit_epoch(blob_id, vol, commit_epoch);
        self.tracker.remove(blob_id, vol);
        let _ = compio_fs::remove_file(&path).await;
    }

    /// Post-flush wrapper for commits whose rewritten blocks all use the
    /// commit epoch as their exact generation.
    pub async fn sync_after_flush(
        &self,
        blob_guid: DataBlobGuid,
        commit_epoch: u64,
        rewrites: &[(u32, Bytes)],
        deletes: &[u32],
    ) -> io::Result<()> {
        let exact_rewrites: Vec<(u32, u64, Bytes)> = rewrites
            .iter()
            .map(|(block, bytes)| (*block, commit_epoch, bytes.clone()))
            .collect();
        self.sync_after_flush_exact(blob_guid, commit_epoch, &exact_rewrites, deletes)
            .await
    }

    /// Post-flush hook for exact per-block generations. `commit_epoch` orders
    /// whole-file mirror jobs, while each rewrite carries the generation that
    /// must be stamped into that block's metadata.
    pub async fn sync_after_flush_exact(
        &self,
        blob_guid: DataBlobGuid,
        commit_epoch: u64,
        rewrites: &[(u32, u64, Bytes)],
        deletes: &[u32],
    ) -> io::Result<()> {
        if commit_epoch == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "commit_epoch=0 is reserved",
            ));
        }
        if rewrites
            .iter()
            .any(|(_, block_version, _)| *block_version == 0)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block_version=0 is reserved as the not-cached sentinel",
            ));
        }
        let blob_id = blob_guid.blob_id;
        let vol = blob_guid.volume_id;
        let path = self.cache_file_path(blob_id, vol);

        // Serialize against concurrent cache writers for the same blob, and
        // read the commit epoch they may have set.
        let _guard = self.mirror_lock(blob_guid).lock().await;
        let memory_commit_epoch = self.memory_commit_epoch(blob_id, vol);
        if commit_epoch < memory_commit_epoch {
            return Ok(());
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .await?;

        // If a newer commit already advanced the file epoch, this mirror is a
        // stale straggler and must be skipped in full. Per-block generations
        // do not participate in this ordering because one committed layout can
        // legitimately reference blocks from several generations.
        match read_header(&file).await {
            Some(mut hdr) => {
                let current_epoch = memory_commit_epoch.max(hdr.commit_epoch);
                if commit_epoch < current_epoch {
                    return Ok(());
                }
                let new_epoch = current_epoch.max(commit_epoch);
                if new_epoch > hdr.commit_epoch {
                    hdr.commit_epoch = new_epoch;
                    write_header(&mut file, &hdr).await?;
                }
                self.record_commit_epoch(blob_id, vol, new_epoch);
            }
            None => {
                let new_epoch = memory_commit_epoch.max(commit_epoch);
                let hdr = CacheHeader::new(self.block_size as u32, new_epoch);
                write_header(&mut file, &hdr).await?;
                self.record_commit_epoch(blob_id, vol, new_epoch);
            }
        }

        let fd = std::os::fd::AsRawFd::as_raw_fd(&file);
        let bsz = self.block_size as usize;
        let mut added_bytes: u64 = 0;

        // Rewrites: write block_size-padded bytes and stamp each block's exact
        // generation. Padding and checksumming over the full block keep the
        // cache consistent across file_size changes.
        for (block_num, block_version, bytes) in rewrites {
            let was_populated = is_block_populated(fd, *block_num, self.block_size);
            let block_offset = (*block_num as u64) * self.block_size;
            let padded = pad_to_block_size_owned(bytes, bsz);
            let checksum = xxhash_rust::xxh3::xxh3_64(&padded);
            let BufResult(r, _) = file.write_at(padded, block_offset).await;
            if let Err(e) = r {
                if e.kind() == io::ErrorKind::StorageFull {
                    self.request_eviction();
                }
                return Err(e);
            }
            let meta = BlockMeta {
                block_version: *block_version,
                checksum,
            };
            write_block_meta(&mut file, *block_num, &meta).await?;
            if !was_populated {
                added_bytes += bsz as u64;
            }
        }

        // Deletes: punch hole in the data region, clear metadata.
        for block_num in deletes {
            punch_block_hole(fd, *block_num, self.block_size);
            clear_block_meta(&file, *block_num).await?;
        }

        // No per-file fdatasync here. The disk cache is a non-
        // authoritative read cache: durability is owned by BSS+NSS,
        // which the writeback flush has already published before this
        // mirror runs. A per-file fdatasync on a create-heavy workload
        // (one device barrier per file, 83k files on a linux untar)
        // dominates wall time (~51s) for zero correctness value.
        // The data stays in the page cache and the OS writes it back
        // lazily; on a crash a torn block fails its per-block checksum
        // on read and cold-fetches from BSS, and reconcile_on_open
        // drops a cache file whose commit epoch lags the layout.
        // Eviction/teardown can force a single bulk sync
        // if cache persistence across reboot is ever required.

        if added_bytes > 0 {
            let new_total = self.tracker.record_insert(blob_id, vol, added_bytes);
            if new_total > self.high_bytes {
                self.request_eviction();
            }
        } else {
            self.tracker.touch(blob_id, vol);
        }

        Ok(())
    }

    /// Open-time staleness check. If another instance has bumped
    /// `blob_version` past what the cache file's header records,
    /// unlink the file so the next read cold-fetches from BSS.
    pub async fn reconcile_on_open(
        &self,
        blob_guid: DataBlobGuid,
        layout_commit_epoch: u64,
    ) -> io::Result<()> {
        if layout_commit_epoch > 1 {
            self.record_commit_epoch(blob_guid.blob_id, blob_guid.volume_id, layout_commit_epoch);
        }
        let blob_id = blob_guid.blob_id;
        let vol = blob_guid.volume_id;
        let path = self.cache_file_path(blob_id, vol);

        let _guard = self.mirror_lock(blob_guid).lock().await;
        let file = match File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        let header = match read_header(&file).await {
            Some(h) => h,
            None => {
                // No header (or malformed): treat as stale and clear.
                drop(file);
                self.tracker.remove(blob_id, vol);
                let _ = compio_fs::remove_file(&path).await;
                return Ok(());
            }
        };
        if header.commit_epoch < layout_commit_epoch {
            tracing::info!(
                %blob_id, vol,
                cache_epoch = header.commit_epoch,
                layout_epoch = layout_commit_epoch,
                "disk cache stale (cross-instance bump), unlinking",
            );
            drop(file);
            self.tracker.remove(blob_id, vol);
            let _ = compio_fs::remove_file(&path).await;
        }
        Ok(())
    }

    /// Check if all blocks of an object are populated (ready for
    /// passthrough). A block is "populated" iff the data extent is
    /// present AND the metadata entry is non-sentinel.
    pub fn is_complete(&self, blob_guid: DataBlobGuid, content_length: u64) -> bool {
        if content_length == 0 {
            return false;
        }
        let path = self.cache_file_path(blob_guid.blob_id, blob_guid.volume_id);
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => return false,
        };
        let fd = std::os::fd::AsRawFd::as_raw_fd(&file);

        // Quick SEEK_DATA/SEEK_HOLE check on the data region first.
        let data_start = unsafe { libc::lseek(fd, 0, libc::SEEK_DATA) };
        if data_start != 0 {
            return false;
        }
        let hole_start = unsafe { libc::lseek(fd, 0, libc::SEEK_HOLE) };
        if hole_start < 0 {
            return false;
        }
        if (hole_start as u64) < content_length {
            return false;
        }
        // Data extent looks complete. Validate per-block metadata too:
        // a block with bytes on disk but a stale (sentinel) metadata
        // entry is not "complete": passthrough would serve bytes the
        // cache can no longer attest to.
        let block_count = content_length.div_ceil(self.block_size) as u32;
        for block in 0..block_count {
            let off = block_meta_offset(block);
            let mut buf = [0u8; BLOCK_META_SIZE as usize];
            use std::os::unix::fs::FileExt;
            if file.read_exact_at(&mut buf, off).is_err() {
                return false;
            }
            let Some(m) = BlockMeta::from_bytes(&buf) else {
                return false;
            };
            if !m.is_cached() {
                return false;
            }
        }
        true
    }

    /// Evict LRU cache files until usage is at or below `target_bytes`.
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

    /// Configured capacity in bytes.
    pub fn capacity_bytes(&self) -> u64 {
        self.max_size_bytes
    }

    /// Promote a cache entry to most-recently-used. Called from
    /// `vfs_open` so passthrough-served files (which bypass the
    /// per-block touch path inside `get_block`) keep their LRU
    /// position fresh.
    pub fn touch_blob(&self, blob_guid: DataBlobGuid) {
        self.tracker.touch(blob_guid.blob_id, blob_guid.volume_id);
    }

    /// Number of files tracked by the in-memory tracker.
    #[cfg(test)]
    fn tracked_file_count(&self) -> usize {
        self.tracker.len()
    }
}

// On-disk helpers

async fn read_header(file: &File) -> Option<CacheHeader> {
    let buf = vec![0u8; HEADER_SIZE as usize];
    let BufResult(r, data) = file.read_at(buf, META_OFFSET).await;
    let n = r.ok()?;
    if n < HEADER_SIZE as usize {
        return None;
    }
    CacheHeader::from_bytes(&data)
}

async fn write_header(file: &mut File, header: &CacheHeader) -> io::Result<()> {
    let bytes = header.to_bytes().to_vec();
    let BufResult(r, _) = file.write_at(bytes, META_OFFSET).await;
    r.map(|_| ())
}

async fn read_block_meta(file: &File, block: u32) -> Option<BlockMeta> {
    let buf = vec![0u8; BLOCK_META_SIZE as usize];
    let BufResult(r, data) = file.read_at(buf, block_meta_offset(block)).await;
    let n = r.ok()?;
    if n < BLOCK_META_SIZE as usize {
        return None;
    }
    BlockMeta::from_bytes(&data)
}

async fn write_block_meta(file: &mut File, block: u32, meta: &BlockMeta) -> io::Result<()> {
    let bytes = meta.to_bytes().to_vec();
    let BufResult(r, _) = file.write_at(bytes, block_meta_offset(block)).await;
    r.map(|_| ())
}

async fn clear_block_meta(file: &File, block: u32) -> io::Result<()> {
    // Punch the per-block metadata slot to zero. We use the
    // sparse-aware FALLOC_FL_PUNCH_HOLE; on FS that supports it this
    // is exactly what we want (returns zeros on read = sentinel
    // BlockMeta). For broader portability we could fall back to
    // pwrite of zeros, for ext4/xfs (which DiskCache::verify_filesystem
    // already requires), this works.
    let fd = std::os::fd::AsRawFd::as_raw_fd(file);
    let off = block_meta_offset(block) as i64;
    let result = unsafe {
        libc::fallocate(
            fd,
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            off,
            BLOCK_META_SIZE as i64,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Copy `bytes` into a fresh `Vec<u8>` zero-padded out to `block_size`.
/// If `bytes` is already >= `block_size` we still copy (we could
/// short-circuit, but the call sites here pass partial buffers that
/// the cache needs to own, so a copy is unavoidable).
fn pad_to_block_size_owned(bytes: &[u8], block_size: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(block_size);
    let take = std::cmp::min(bytes.len(), block_size);
    out.extend_from_slice(&bytes[..take]);
    out.resize(block_size, 0);
    out
}

fn punch_block_hole(fd: i32, block: u32, block_size: u64) {
    let off = block as i64 * block_size as i64;
    unsafe {
        libc::fallocate(
            fd,
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            off,
            block_size as i64,
        );
    }
}

/// Check if a block is populated (data, not a hole) in the cache file.
fn is_block_populated(fd: i32, block: u32, block_size: u64) -> bool {
    let offset = block as i64 * block_size as i64;
    let result = unsafe { libc::lseek(fd, offset, libc::SEEK_DATA) };
    result == offset
}

/// Verify that the cache directory is on ext4 or xfs (required for
/// SEEK_DATA/SEEK_HOLE to distinguish written zeros from holes, and
/// FALLOC_FL_PUNCH_HOLE for invalidation).
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
        let Some((blob_id, vol)) = parse_cache_filename(&path) else {
            continue;
        };
        let Ok(meta) = std::fs::metadata(&path) else {
            continue;
        };
        // Approximate on-disk footprint. The metadata region's
        // physical blocks contribute too; that's the right number
        // for eviction accounting.
        let disk_bytes = meta.blocks() * 512;
        tracker.insert_cold(blob_id, vol, disk_bytes);
        count += 1;
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

/// A best-effort disk-cache mirror write, handed to the dedicated mirror
/// thread so the local-cache I/O + checksum never run on a FUSE worker.
pub(crate) struct MirrorJob {
    pub(crate) blob_guid: data_types::DataBlobGuid,
    pub(crate) blob_version: u64,
    pub(crate) rewrites: Vec<(u32, Bytes)>,
    pub(crate) deletes: Vec<u32>,
    /// Retained `rewrites` payload size, used to keep `mirror_queued_bytes`
    /// balanced (added on enqueue, subtracted once the job is processed).
    pub(crate) byte_len: usize,
}

/// Sender + the shared queued-byte counter for the mirror channel.
pub(crate) struct MirrorHandle {
    pub(crate) tx: futures::channel::mpsc::Sender<MirrorJob>,
    pub(crate) queued_bytes: Arc<AtomicUsize>,
}

/// Bound on queued mirror jobs by count. The dedicated thread drains local
/// page-cache writes far faster than the network publish feeds it, so this
/// rarely fills; when it does, `try_send` drops the job (best-effort; the
/// block cold-fills from BSS on the next read) instead of blocking a FUSE
/// worker.
const MIRROR_QUEUE_CAP: usize = 4096;

/// Byte bound on the mirror queue. A job retains its rewritten `Bytes`
/// until the mirror thread writes them, so a slow cache device could
/// otherwise pin unbounded flushed write buffers (one large-file override
/// flush is a single job but many MiB). When the in-flight payload exceeds
/// this, new jobs are dropped (best-effort) before their `Bytes` are
/// retained. 256 MiB caps memory while staying far above the steady-state
/// backlog of an 83k-file untar.
pub(crate) const MIRROR_BYTE_BUDGET: usize = 256 * 1024 * 1024;

/// Spawn the dedicated disk-cache mirror thread. It owns its own compio
/// runtime (separate io_uring) and drains the job channel, so a
/// create-heavy workload's cache writes never steal cycles from the FUSE
/// worker threads. Returns the handle, or `None` if the runtime could not
/// be built (mirror then silently disabled; the cache still serves
/// reads via cold-fill, just not write-populated).
pub(crate) fn spawn_mirror_worker(dc: Arc<DiskCache>) -> Option<MirrorHandle> {
    let (tx, mut rx) = futures::channel::mpsc::channel::<MirrorJob>(MIRROR_QUEUE_CAP);
    let queued_bytes = Arc::new(AtomicUsize::new(0));
    let worker_bytes = queued_bytes.clone();
    let spawned = std::thread::Builder::new()
        .name("fb-disk-mirror".to_string())
        .spawn(move || {
            let rt = match compio_runtime::Runtime::builder().build() {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::warn!(error = %e, "disk-cache mirror thread: runtime build failed");
                    return;
                }
            };
            rt.block_on(async move {
                use futures::StreamExt;
                while let Some(job) = rx.next().await {
                    if let Err(e) = dc
                        .sync_after_flush(
                            job.blob_guid,
                            job.blob_version,
                            &job.rewrites,
                            &job.deletes,
                        )
                        .await
                    {
                        tracing::warn!(
                            blob_version = job.blob_version,
                            error = %e,
                            "disk cache mirror write failed (best-effort)"
                        );
                    }
                    worker_bytes.fetch_sub(job.byte_len, Ordering::Relaxed);
                }
            });
        });
    match spawned {
        Ok(_) => Some(MirrorHandle { tx, queued_bytes }),
        Err(e) => {
            tracing::warn!(error = %e, "failed to spawn disk-cache mirror thread");
            None
        }
    }
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
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn guid_with(blob_id: Uuid, vol: u16) -> DataBlobGuid {
        DataBlobGuid {
            blob_id,
            volume_id: vol,
        }
    }

    #[compio_macros::test]
    async fn test_insert_and_get_block() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let data = vec![42u8; 1024];

        cache
            .insert_block(guid, 0, /* block_version */ 7, &data)
            .await
            .unwrap();

        let result = cache.get_block(guid, 0, 1024).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_ref(), &data[..]);

        // get_block_version reflects the stamp we passed.
        assert_eq!(cache.get_block_version(guid, 0).await, Some(7));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_get_block_exact_rejects_other_generation() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let version_7 = vec![0x77u8; 1024];
        cache.insert_block(guid, 0, 7, &version_7).await.unwrap();

        assert_eq!(cache.get_block_exact(guid, 0, 6, 1024).await, None);
        assert_eq!(
            cache
                .get_block_exact(guid, 0, 7, 1024)
                .await
                .expect("exact generation should hit")
                .as_ref(),
            &version_7[..]
        );

        let version_8 = vec![0x88u8; 1024];
        cache.insert_block(guid, 0, 8, &version_8).await.unwrap();

        assert_eq!(cache.get_block_exact(guid, 0, 7, 1024).await, None);
        assert_eq!(
            cache
                .get_block_exact(guid, 0, 8, 1024)
                .await
                .expect("replacement generation should hit")
                .as_ref(),
            &version_8[..]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_get_block_into() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let data = vec![42u8; 1024];
        cache.insert_block(guid, 0, 1, &data).await.unwrap();

        let mut buf = vec![0u8; 1024];
        let result = cache.get_block_into(guid, 0, 1024, &mut buf).await;
        assert_eq!(result, Some(1024));
        assert_eq!(&buf[..], &data[..]);

        // Miss on a different (uncached) block.
        let mut buf2 = vec![0u8; 1024];
        let result = cache.get_block_into(guid, 1, 1024, &mut buf2).await;
        assert!(result.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_get_block_into_exact_rejects_other_generation() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let data = vec![0x55u8; 1024];
        cache.insert_block(guid, 0, 4, &data).await.unwrap();

        let mut rejected = vec![0xAAu8; 1024];
        assert_eq!(
            cache
                .get_block_into_exact(guid, 0, 3, 1024, &mut rejected)
                .await,
            None
        );
        assert!(
            rejected.iter().all(|byte| *byte == 0xAA),
            "rejected generation must not alter the destination buffer"
        );

        let mut accepted = vec![0u8; 1024];
        assert_eq!(
            cache
                .get_block_into_exact(guid, 0, 4, 1024, &mut accepted)
                .await,
            Some(1024)
        );
        assert_eq!(accepted, data);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_get_missing_block() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let result = cache.get_block(guid, 0, 1024).await;
        assert!(result.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_get_hole_block() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let data = vec![55u8; 1024];
        cache.insert_block(guid, 5, 1, &data).await.unwrap();

        let result = cache.get_block(guid, 3, 1024).await;
        assert!(result.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Torn-write recovery: on-disk bytes don't match the recorded
    /// xxh3. The read treats it as a miss and clears the metadata.
    #[compio_macros::test]
    async fn test_checksum_corruption() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let data = vec![99u8; 1024];
        cache.insert_block(guid, 0, 7, &data).await.unwrap();

        // Corrupt the body bytes via std::fs (synchronous direct write
        // bypassing the cache API).
        use std::os::unix::fs::FileExt;
        let path = cache.cache_file_path(guid.blob_id, guid.volume_id);
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_all_at(&[0u8; 10], 0).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // get_block returns None on checksum mismatch. The stale
        // metadata entry stays in place; the next insert_block
        // overwrites it with the fresh checksum.
        let result = cache.get_block(guid, 0, 1024).await;
        assert!(result.is_none());

        // Repopulate the entry; subsequent reads succeed.
        let fresh = vec![123u8; 1024];
        cache.insert_block(guid, 0, 8, &fresh).await.unwrap();
        let after = cache.get_block(guid, 0, 1024).await;
        assert_eq!(after.unwrap().as_ref(), &fresh[..]);
        assert_eq!(cache.get_block_version(guid, 0).await, Some(8));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `sync_after_flush_exact` updates the in-place file: same path, new
    /// bytes and exact generations at rewritten offsets, and an advanced
    /// commit epoch in the header.
    #[compio_macros::test]
    async fn test_sync_after_flush_in_place() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let v1_data_0 = vec![1u8; block_size as usize];
        let v1_data_1 = vec![2u8; block_size as usize];
        let v1_data_2 = vec![3u8; block_size as usize];
        cache.insert_block(guid, 0, 1, &v1_data_0).await.unwrap();
        cache.insert_block(guid, 1, 1, &v1_data_1).await.unwrap();
        cache.insert_block(guid, 2, 1, &v1_data_2).await.unwrap();

        let path_before = cache.cache_file_path(guid.blob_id, guid.volume_id);
        assert!(path_before.exists());

        // Commit epoch 2 rewrites block 1 at generation 2 and mirrors a
        // version-1 append independently.
        let new_block_1 = vec![22u8; block_size as usize];
        let appended_block_3 = vec![33u8; block_size as usize];
        cache
            .sync_after_flush_exact(
                guid,
                2,
                &[
                    (1, 2, Bytes::from(new_block_1.clone())),
                    (3, 1, Bytes::from(appended_block_3.clone())),
                ],
                &[],
            )
            .await
            .unwrap();

        // Same path, no new file.
        let path_after = cache.cache_file_path(guid.blob_id, guid.volume_id);
        assert_eq!(path_before, path_after);
        let files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();
        assert_eq!(files.len(), 1, "exactly one cache file post-override");

        // The commit epoch and exact per-block generations remain independent.
        assert_eq!(cache.get_block_version(guid, 0).await, Some(1));
        assert_eq!(cache.get_block_version(guid, 1).await, Some(2));
        assert_eq!(cache.get_block_version(guid, 2).await, Some(1));
        assert_eq!(cache.get_block_version(guid, 3).await, Some(1));
        assert_eq!(cache.commit_epoch(guid).await, Some(2));

        // Reading returns the new bytes for block 1 and the old bytes
        // for blocks 0 and 2.
        assert_eq!(
            cache
                .get_block(guid, 0, block_size as usize)
                .await
                .unwrap()
                .as_ref(),
            &v1_data_0[..]
        );
        assert_eq!(
            cache
                .get_block(guid, 1, block_size as usize)
                .await
                .unwrap()
                .as_ref(),
            &new_block_1[..]
        );
        assert_eq!(
            cache
                .get_block(guid, 2, block_size as usize)
                .await
                .unwrap()
                .as_ref(),
            &v1_data_2[..]
        );
        assert_eq!(
            cache
                .get_block(guid, 3, block_size as usize)
                .await
                .unwrap()
                .as_ref(),
            &appended_block_3[..]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Shrink-flush punches holes for deleted blocks and clears their
    /// metadata entries.
    #[compio_macros::test]
    async fn test_sync_after_flush_deletes() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let data = vec![7u8; block_size as usize];
        for b in 0..3 {
            cache.insert_block(guid, b, 1, &data).await.unwrap();
        }

        cache.sync_after_flush(guid, 2, &[], &[1, 2]).await.unwrap();

        assert_eq!(cache.get_block_version(guid, 0).await, Some(1));
        assert_eq!(cache.get_block_version(guid, 1).await, None);
        assert_eq!(cache.get_block_version(guid, 2).await, None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Cross-instance bump unlinks the cache on reconcile.
    #[compio_macros::test]
    async fn test_reconcile_on_open_unlinks_stale() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let data = vec![9u8; block_size as usize];
        cache
            .sync_after_flush_exact(guid, 5, &[(0, 5, Bytes::from(data))], &[])
            .await
            .unwrap();
        let path = cache.cache_file_path(guid.blob_id, guid.volume_id);
        assert!(path.exists());

        // Layout reports a strictly higher blob_version than the cache.
        cache.reconcile_on_open(guid, 6).await.unwrap();
        assert!(!path.exists(), "stale cache file unlinked");
        assert_eq!(cache.tracked_file_count(), 0);
        assert_eq!(cache.commit_epoch(guid).await, Some(6));

        // No-op when cache file is missing.
        cache.reconcile_on_open(guid, 6).await.unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Reconcile leaves the cache alone when its commit epoch is at least the
    /// layout's commit epoch.
    #[compio_macros::test]
    async fn test_reconcile_keeps_fresh_cache() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let data = vec![9u8; block_size as usize];
        cache.reconcile_on_open(guid, 10).await.unwrap();
        cache.insert_block(guid, 0, 1, &data).await.unwrap();
        let path = cache.cache_file_path(guid.blob_id, guid.volume_id);
        assert_eq!(cache.commit_epoch(guid).await, Some(10));

        cache.reconcile_on_open(guid, 10).await.unwrap();
        assert!(path.exists(), "cache file kept when up to date");
        cache.reconcile_on_open(guid, 9).await.unwrap();
        assert!(path.exists(), "cache file kept when ahead");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A newer mirror commit overwrites the block bytes in place and raises
    /// the commit epoch. A delayed older mirror commit is fenced without
    /// reviving the superseded bytes.
    #[compio_macros::test]
    async fn test_sync_after_flush_exact_fences_stale_commit() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let bsz = block_size as usize;
        let v1 = vec![0xAAu8; bsz];

        cache.insert_block(guid, 0, 1, &v1).await.unwrap();
        assert_eq!(
            cache.get_block(guid, 0, bsz).await.as_deref(),
            Some(&v1[..])
        );

        // Commit epoch 2 rewrites block 0 at exact generation 2.
        let v2 = vec![0xBBu8; bsz];
        cache
            .sync_after_flush_exact(guid, 2, &[(0, 2, Bytes::from(v2.clone()))], &[])
            .await
            .unwrap();
        assert_eq!(
            cache.get_block(guid, 0, bsz).await.as_deref(),
            Some(&v2[..]),
            "override bytes visible immediately"
        );
        assert_eq!(cache.commit_epoch(guid).await, Some(2));

        // A delayed commit-epoch-1 mirror must not revive its old bytes.
        cache
            .sync_after_flush_exact(guid, 1, &[(0, 1, Bytes::from(vec![0xDDu8; bsz]))], &[])
            .await
            .unwrap();
        assert_eq!(
            cache.get_block(guid, 0, bsz).await.as_deref(),
            Some(&v2[..]),
            "stale mirror commit must be fenced"
        );
        assert_eq!(cache.commit_epoch(guid).await, Some(2));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Exact block generations do not inherit the file commit epoch. A layout
    /// committed at epoch 2 can still reference an unchanged version-1 block.
    #[compio_macros::test]
    async fn test_insert_block_accepts_generation_below_commit_epoch() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let bsz = block_size as usize;

        // Establish commit epoch 2 with a generation-2 rewrite.
        cache
            .sync_after_flush_exact(guid, 2, &[(0, 2, Bytes::from(vec![0xBBu8; bsz]))], &[])
            .await
            .unwrap();
        assert_eq!(cache.commit_epoch(guid).await, Some(2));

        // The committed layout still references block 1 at exact generation 1.
        let v1 = vec![0xDDu8; bsz];
        cache.insert_block(guid, 1, 1, &v1).await.unwrap();
        assert_eq!(
            cache.get_block(guid, 1, bsz).await.as_deref(),
            Some(&v1[..]),
            "exact generation-1 cold-fill must land"
        );
        assert_eq!(cache.get_block_version(guid, 1).await, Some(1));
        assert_eq!(
            cache.commit_epoch(guid).await,
            Some(2),
            "cold-fill must not lower the commit epoch"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `drop_blob` removes the whole cache file while preserving the commit
    /// epoch. Delayed older mirrors stay fenced, but exact older block
    /// generations may cold-fill again when a committed layout references them.
    #[compio_macros::test]
    async fn test_drop_blob_removes_cache_file() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();

        let guid = guid_with(Uuid::new_v4(), 1);
        let bsz = block_size as usize;
        cache
            .insert_block(guid, 0, 1, &vec![0xAAu8; bsz])
            .await
            .unwrap();
        cache
            .insert_block(guid, 1, 1, &vec![0xBBu8; bsz])
            .await
            .unwrap();
        let path = cache.cache_file_path(guid.blob_id, guid.volume_id);
        assert!(path.exists());
        assert!(cache.get_block(guid, 0, bsz).await.is_some());

        cache.drop_blob(guid, 2).await;
        assert!(!path.exists(), "cache file removed");
        assert!(
            cache.get_block(guid, 0, bsz).await.is_none(),
            "every block misses after drop"
        );
        assert!(cache.get_block(guid, 1, bsz).await.is_none());
        assert_eq!(cache.commit_epoch(guid).await, Some(2));

        cache
            .sync_after_flush_exact(guid, 1, &[(0, 1, Bytes::from(vec![0xCCu8; bsz]))], &[])
            .await
            .unwrap();
        assert!(!path.exists(), "stale mirror must not recreate the file");

        let exact_v1 = vec![0xDDu8; bsz];
        cache.insert_block(guid, 0, 1, &exact_v1).await.unwrap();
        assert_eq!(
            cache.get_block(guid, 0, bsz).await.as_deref(),
            Some(&exact_v1[..]),
            "exact generation-1 cold-fill must be admitted"
        );
        assert_eq!(cache.commit_epoch(guid).await, Some(2));

        // Drop on an absent blob is a no-op.
        cache.drop_blob(guid_with(Uuid::new_v4(), 1), 2).await;

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The in-process commit-epoch map is bounded. Evicting an old entry is
    /// allowed because it only drops protection against a very old delayed
    /// mirror after the cache file was removed.
    #[compio_macros::test]
    async fn test_commit_epoch_map_is_lru_capped() {
        let dir = test_cache_dir();
        let cache = DiskCache::new_with_commit_epoch_cap(
            &dir,
            1,
            8192,
            NonZeroUsize::new(2).expect("test cap is nonzero"),
        )
        .unwrap();

        let guid1 = guid_with(Uuid::new_v4(), 1);
        let guid2 = guid_with(Uuid::new_v4(), 1);
        let guid3 = guid_with(Uuid::new_v4(), 1);

        cache.drop_blob(guid1, 2).await;
        cache.drop_blob(guid2, 3).await;
        assert_eq!(cache.commit_epoch(guid1).await, Some(2));

        cache.drop_blob(guid3, 4).await;

        assert_eq!(
            cache.commit_epoch(guid1).await,
            Some(2),
            "recently touched epoch retained"
        );
        assert_eq!(
            cache.commit_epoch(guid2).await,
            None,
            "least recently used epoch evicted"
        );
        assert_eq!(cache.commit_epoch(guid3).await, Some(4));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `is_complete` requires both data populated and metadata
    /// entries non-sentinel.
    #[compio_macros::test]
    async fn test_is_complete() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();
        let content_length = 3 * block_size;

        let guid = guid_with(Uuid::new_v4(), 1);
        for block in 0..3 {
            let data = vec![block as u8; block_size as usize];
            cache.insert_block(guid, block, 1, &data).await.unwrap();
        }

        assert!(cache.is_complete(guid, content_length));

        // Partial coverage: missing the middle block.
        let guid2 = guid_with(Uuid::new_v4(), 1);
        for block in [0u32, 2] {
            let data = vec![block as u8; block_size as usize];
            cache.insert_block(guid2, block, 1, &data).await.unwrap();
        }
        assert!(!cache.is_complete(guid2, content_length));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_evict_unlinks_whole_files() {
        let dir = test_cache_dir();
        let cache = DiskCache::new(&dir, 1, 1024).unwrap();

        for i in 0..3 {
            let guid = guid_with(Uuid::from_u128(i as u128 + 1), 1);
            let data = vec![i as u8; 4096];
            cache.insert_block(guid, 0, 1, &data).await.unwrap();
        }

        assert_eq!(cache.tracked_file_count(), 3);
        assert!(cache.current_usage() > 0);

        cache.evict_to(0).await.unwrap();

        let remaining: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();
        assert!(remaining.is_empty());
        assert_eq!(cache.tracked_file_count(), 0);
        assert_eq!(cache.current_usage(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// LRU order: blob touched via `get_block` is preserved as MRU.
    #[compio_macros::test]
    async fn test_tracker_touch_updates_lru() {
        let dir = test_cache_dir();
        let block_size = 8192u64;
        let cache = DiskCache::new(&dir, 1, block_size).unwrap();
        let content_length = 3 * block_size;
        let _ = content_length;

        let blobs: Vec<DataBlobGuid> = (1..=3)
            .map(|i| guid_with(Uuid::from_u128(i as u128), 1))
            .collect();
        for (i, g) in blobs.iter().enumerate() {
            let data = vec![i as u8 + 1; block_size as usize];
            cache.insert_block(*g, 0, 1, &data).await.unwrap();
        }

        let _ = cache.get_block(blobs[0], 0, block_size as usize).await;

        let lru = cache.tracker.peek_lru_order();
        assert_eq!(lru.len(), 3);
        assert_eq!(lru[2].0, blobs[0].blob_id, "blob[0] is MRU");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio_macros::test]
    async fn test_cold_start_scan() {
        let dir = test_cache_dir();
        let block_size = 8192u64;

        {
            let cache = DiskCache::new(&dir, 1, block_size).unwrap();
            for i in 0..3 {
                let guid = guid_with(Uuid::from_u128(i as u128 + 100), 1);
                let data = vec![i as u8 + 1; block_size as usize];
                cache.insert_block(guid, 0, 1, &data).await.unwrap();
            }
            assert_eq!(cache.tracked_file_count(), 3);
            assert!(cache.current_usage() > 0);
        }

        let cache2 = DiskCache::new(&dir, 1, block_size).unwrap();
        assert_eq!(cache2.tracked_file_count(), 3);
        assert!(cache2.current_usage() > 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_parse_cache_filename() {
        let uuid = Uuid::from_u128(0x12345678_1234_1234_1234_123456789abc);
        let path = PathBuf::from(format!("/tmp/cache/{}_{}", uuid.as_simple(), 42));
        assert_eq!(parse_cache_filename(&path), Some((uuid, 42)));

        // Bogus inputs reject cleanly.
        assert_eq!(
            parse_cache_filename(&PathBuf::from("/tmp/cache/invalid")),
            None,
        );
        assert_eq!(
            parse_cache_filename(&PathBuf::from(format!(
                "/tmp/cache/{}_notanumber",
                uuid.as_simple(),
            ))),
            None,
        );
    }

    #[test]
    fn test_header_roundtrip() {
        let h = CacheHeader {
            magic: CACHE_MAGIC,
            format_version: CACHE_FORMAT_VERSION,
            block_size: 65536,
            block_count: 1024,
            commit_epoch: 12345,
            flags: 0,
            _reserved: 0,
        };
        let bytes = h.to_bytes();
        let back = CacheHeader::from_bytes(&bytes).unwrap();
        assert_eq!(h, back);

        // Wrong magic.
        let mut bad = bytes;
        bad[0] = 0xFF;
        assert_eq!(CacheHeader::from_bytes(&bad), None);
    }

    #[test]
    fn test_block_meta_roundtrip() {
        let m = BlockMeta {
            block_version: 7,
            checksum: 0xdead_beef_cafe_babe,
        };
        let bytes = m.to_bytes();
        let back = BlockMeta::from_bytes(&bytes).unwrap();
        assert_eq!(m, back);

        // The default (zeroed) BlockMeta is the not-cached sentinel.
        let zero = BlockMeta::from_bytes(&[0u8; BLOCK_META_SIZE as usize]).unwrap();
        assert!(!zero.is_cached());
    }
}
