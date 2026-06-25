use moka::sync::Cache;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct DirEntry {
    pub name: String,
    pub ino: u64,
    pub is_dir: bool,
}

pub struct DirCache {
    inner: Cache<String, Arc<RwLock<Vec<DirEntry>>>>,
}

impl DirCache {
    pub fn new(ttl: Duration) -> Self {
        let cache = Cache::builder()
            .max_capacity(10_000)
            .time_to_live(ttl)
            .build();
        Self { inner: cache }
    }

    pub fn get(&self, prefix: &str) -> Option<Arc<Vec<DirEntry>>> {
        let cached = self.inner.get(prefix)?;
        let entries = cached.read();
        Some(Arc::new(entries.clone()))
    }

    pub fn contains_name(&self, prefix: &str, name: &str) -> Option<bool> {
        let cached = self.inner.get(prefix)?;
        let entries = cached.read();
        Some(entries.iter().any(|entry| entry.name == name))
    }

    pub fn has_children(&self, prefix: &str) -> Option<bool> {
        let cached = self.inner.get(prefix)?;
        let entries = cached.read();
        Some(
            entries
                .iter()
                .any(|entry| entry.name != "." && entry.name != ".."),
        )
    }

    /// Like `has_children`, but counts only non-directory (file) children.
    /// A directory child can be a phantom: readdir lists with a "/"
    /// delimiter, and a fully-tombstoned subtree still emits a CommonPrefix
    /// entry that lands in this cache, so a dir child here is not proof of
    /// non-emptiness (rmdir's no-delimiter NSS list, which filters
    /// tombstones, is authoritative for those, and a still-queued dir
    /// create is caught by the writeback-intent check). A file child,
    /// however, is a real local create not yet in NSS and must keep rmdir
    /// from winning the race.
    pub fn has_file_children(&self, prefix: &str) -> Option<bool> {
        let cached = self.inner.get(prefix)?;
        let entries = cached.read();
        Some(
            entries
                .iter()
                .any(|entry| !entry.is_dir && entry.name != "." && entry.name != ".."),
        )
    }

    pub fn insert(&self, prefix: String, entries: Vec<DirEntry>) -> Arc<Vec<DirEntry>> {
        let snapshot = Arc::new(entries.clone());
        self.inner.insert(prefix, Arc::new(RwLock::new(entries)));
        snapshot
    }

    pub fn upsert(&self, prefix: &str, entry: DirEntry) {
        let Some(cached) = self.inner.get(prefix) else {
            return;
        };
        {
            let mut entries = cached.write();
            if let Some(existing) = entries
                .iter_mut()
                .find(|existing| existing.name == entry.name)
            {
                *existing = entry;
            } else {
                entries.push(entry);
            }
        }
        self.inner.insert(prefix.to_string(), cached);
    }

    pub fn insert_empty_dir(&self, prefix: String, ino: u64, parent: u64) {
        self.inner.insert(
            prefix,
            Arc::new(RwLock::new(vec![
                DirEntry {
                    name: ".".to_string(),
                    ino,
                    is_dir: true,
                },
                DirEntry {
                    name: "..".to_string(),
                    ino: parent,
                    is_dir: true,
                },
            ])),
        );
    }

    pub fn invalidate(&self, prefix: &str) {
        self.inner.invalidate(prefix);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_preserves_empty_directory_entries() {
        let cache = DirCache::new(Duration::from_secs(1));
        cache.insert_empty_dir("dir/".to_string(), 2, 1);
        cache.upsert(
            "dir/",
            DirEntry {
                name: "child".to_string(),
                ino: 3,
                is_dir: false,
            },
        );
        cache.upsert(
            "dir/",
            DirEntry {
                name: "child".to_string(),
                ino: 4,
                is_dir: true,
            },
        );

        let entries = cache.get("dir/").expect("cached directory missing");
        assert_eq!(entries.len(), 3);
        let child = entries
            .iter()
            .find(|entry| entry.name == "child")
            .expect("cached child missing");
        assert_eq!(child.ino, 4);
        assert!(child.is_dir);
        assert_eq!(cache.contains_name("dir/", "child"), Some(true));
        assert_eq!(cache.contains_name("dir/", "missing"), Some(false));
        assert_eq!(cache.has_children("dir/"), Some(true));
    }
}
