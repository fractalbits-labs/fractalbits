use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use data_types::TraceId;
use nss_codec::change_event::ChangeType;

use crate::backend::{BackendConfig, StorageBackend};
use crate::vfs::VfsCore;

const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Run the background watch loop that polls NSS for mutation events
/// and invalidates the local FUSE kernel cache accordingly.
pub fn spawn_watch_loop(
    vfs: Arc<VfsCore>,
    backend_config: Arc<BackendConfig>,
    shutdown: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("fuse-watch".to_string())
        .spawn(move || {
            let rt = compio_runtime::Runtime::new()
                .expect("cannot create compio runtime for watch loop");
            rt.block_on(watch_loop(vfs, backend_config, shutdown));
        })
        .expect("cannot spawn watch loop thread")
}

async fn watch_loop(
    vfs: Arc<VfsCore>,
    backend_config: Arc<BackendConfig>,
    shutdown: Arc<AtomicBool>,
) {
    let backend = match StorageBackend::new(&backend_config) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(error = %e, "watch loop: failed to create backend");
            return;
        }
    };

    let mut last_seq = 0u64;

    tracing::info!("watch loop started, polling every {:?}", POLL_INTERVAL);

    while !shutdown.load(Ordering::Relaxed) {
        let trace_id = TraceId::new();
        match backend.watch_changes(last_seq, &trace_id).await {
            Ok(resp) => {
                if resp.truncated {
                    tracing::warn!(
                        last_seq,
                        new_seq = resp.seq,
                        "watch loop: change log truncated, some events lost"
                    );
                }
                last_seq = resp.seq;
                for event in &resp.events {
                    apply_invalidation(&vfs, event);
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "watch_changes failed, retrying");
                compio_runtime::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        }
        compio_runtime::time::sleep(POLL_INTERVAL).await;
    }

    tracing::info!("watch loop stopped");
}

fn apply_invalidation(vfs: &VfsCore, event: &nss_codec::ChangeEvent) {
    let change_type = event.change_type();
    let key = &event.key;
    let old_key = &event.old_key;

    match change_type {
        ChangeType::Put => {
            let (parent, name) = split_parent_name(key);
            vfs.invalidate_entry(parent, name);
            // Also invalidate inode page cache so stale file content
            // is dropped. Without this, a remotely overwritten file
            // would still serve old data from kernel page cache.
            vfs.invalidate_inode(key);
        }
        ChangeType::Delete => {
            let (parent, name) = split_parent_name(key);
            vfs.invalidate_entry(parent, name);
        }
        ChangeType::Rename => {
            // Invalidate both old and new entries
            let (old_parent, old_name) = split_parent_name(old_key);
            let (new_parent, new_name) = split_parent_name(key);
            vfs.invalidate_entry(old_parent, old_name);
            vfs.invalidate_entry(new_parent, new_name);
            // If parents differ, invalidate both dir listings
            if old_parent != new_parent {
                vfs.invalidate_dir(old_parent);
            }
            vfs.invalidate_dir(new_parent);
        }
    }
}

/// Split an S3-style key into (parent_prefix, basename).
/// e.g. "dir/subdir/file.txt\0" -> ("dir/subdir/", "file.txt")
/// e.g. "file.txt\0" -> ("", "file.txt")
fn split_parent_name(key: &str) -> (&str, &str) {
    // Strip trailing NUL if present (NSS keys have trailing \0 for files)
    let key = key.trim_end_matches('\0');
    // Strip trailing / for directories
    let key = key.trim_end_matches('/');

    match key.rfind('/') {
        Some(pos) => (&key[..=pos], &key[pos + 1..]),
        None => ("", key),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_parent_name() {
        assert_eq!(split_parent_name("dir/file.txt\0"), ("dir/", "file.txt"));
        assert_eq!(split_parent_name("file.txt\0"), ("", "file.txt"));
        assert_eq!(
            split_parent_name("a/b/c/file.txt\0"),
            ("a/b/c/", "file.txt")
        );
        assert_eq!(split_parent_name("dir/subdir/"), ("dir/", "subdir"));
        assert_eq!(split_parent_name("top_level"), ("", "top_level"));
    }
}
