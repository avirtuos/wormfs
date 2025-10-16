//! Inode allocation and caching for FileSystemService.
//!
//! This module provides thread-safe inode allocation and an LRU cache
//! for file metadata to optimize FUSE operations.

use crate::metadata_store::{FileId, FileMetadata};
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Root directory inode number (FUSE standard)
pub const ROOT_INODE: u64 = 1;

/// Root directory file ID (deterministic UUID)
/// Using UUID v5 (namespace-based) with DNS namespace and "wormfs:root"
pub const ROOT_FILE_ID: FileId = FileId(Uuid::from_bytes([
    0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8,
]));

/// Allocates unique inode numbers for new files.
///
/// Uses an atomic counter to ensure thread-safe allocation without locks.
/// Inodes start at 2 (ROOT_INODE is 1).
pub struct InodeAllocator {
    next_inode: AtomicU64,
}

impl InodeAllocator {
    /// Create a new InodeAllocator starting at inode 2.
    pub fn new() -> Self {
        Self {
            // Start at 2 because 1 is reserved for root
            next_inode: AtomicU64::new(ROOT_INODE + 1),
        }
    }

    /// Allocate the next available inode number.
    ///
    /// This method is thread-safe and lock-free.
    pub fn allocate(&self) -> u64 {
        self.next_inode.fetch_add(1, Ordering::SeqCst)
    }

    /// Get the current next inode (without allocating).
    pub fn peek(&self) -> u64 {
        self.next_inode.load(Ordering::SeqCst)
    }

    /// Set the next inode to allocate (used for recovery).
    ///
    /// This should only be called during initialization when recovering
    /// the highest inode from MetadataStore.
    pub fn set_next(&self, inode: u64) {
        self.next_inode.store(inode, Ordering::SeqCst);
    }
}

impl Default for InodeAllocator {
    fn default() -> Self {
        Self::new()
    }
}

/// Cached inode entry with TTL.
#[derive(Debug, Clone)]
pub struct CachedInode {
    /// File metadata
    pub file_id: FileId,
    /// File metadata
    pub metadata: FileMetadata,
    /// When this entry was cached
    pub cached_at: Instant,
}

impl CachedInode {
    /// Check if this cache entry has expired.
    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
}

/// LRU cache for inode metadata with TTL expiration.
///
/// Caches file metadata to avoid repeated MetadataStore queries for hot files.
/// Uses a combination of LRU eviction and TTL-based expiration.
pub struct InodeCache {
    cache: RwLock<LruCache<u64, CachedInode>>,
    ttl: Duration,
}

impl InodeCache {
    /// Create a new InodeCache with the given capacity and TTL.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum number of entries to cache
    /// * `ttl` - Time-to-live for cache entries
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        let capacity = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1000).unwrap());
        Self {
            cache: RwLock::new(LruCache::new(capacity)),
            ttl,
        }
    }

    /// Get a cached inode entry if it exists and hasn't expired.
    ///
    /// # Arguments
    ///
    /// * `inode` - The inode number to look up
    ///
    /// # Returns
    ///
    /// `Some(CachedInode)` if found and not expired, `None` otherwise.
    pub fn get(&self, inode: u64) -> Option<CachedInode> {
        let mut cache = self.cache.write().ok()?;

        // Get entry and update LRU order
        let entry = cache.get(&inode)?;

        // Check if expired
        if entry.is_expired(self.ttl) {
            // Remove expired entry
            cache.pop(&inode);
            return None;
        }

        Some(entry.clone())
    }

    /// Insert or update a cache entry.
    ///
    /// # Arguments
    ///
    /// * `inode` - The inode number
    /// * `file_id` - The file ID
    /// * `metadata` - The file metadata
    pub fn insert(&self, inode: u64, file_id: FileId, metadata: FileMetadata) {
        if let Ok(mut cache) = self.cache.write() {
            cache.put(
                inode,
                CachedInode {
                    file_id,
                    metadata,
                    cached_at: Instant::now(),
                },
            );
        }
    }

    /// Invalidate a specific inode entry.
    ///
    /// # Arguments
    ///
    /// * `inode` - The inode number to invalidate
    pub fn invalidate(&self, inode: u64) {
        if let Ok(mut cache) = self.cache.write() {
            cache.pop(&inode);
        }
    }

    /// Clear all cache entries.
    pub fn clear(&self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.clear();
        }
    }

    /// Get the number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.cache.read().map(|c| c.len()).unwrap_or(0)
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove all expired entries from the cache.
    ///
    /// This is useful for periodic cleanup to free memory.
    pub fn cleanup_expired(&self) {
        if let Ok(mut cache) = self.cache.write() {
            let expired_keys: Vec<u64> = cache
                .iter()
                .filter(|(_, entry)| entry.is_expired(self.ttl))
                .map(|(k, _)| *k)
                .collect();

            for key in expired_keys {
                cache.pop(&key);
            }
        }
    }
}

/// Shared inode management state.
///
/// Combines allocator and cache for efficient inode management.
pub struct InodeManager {
    allocator: InodeAllocator,
    cache: Arc<InodeCache>,
}

impl InodeManager {
    /// Create a new InodeManager.
    ///
    /// # Arguments
    ///
    /// * `cache_capacity` - Maximum cache entries
    /// * `cache_ttl` - Cache entry TTL
    pub fn new(cache_capacity: usize, cache_ttl: Duration) -> Self {
        Self {
            allocator: InodeAllocator::new(),
            cache: Arc::new(InodeCache::new(cache_capacity, cache_ttl)),
        }
    }

    /// Get the inode allocator.
    pub fn allocator(&self) -> &InodeAllocator {
        &self.allocator
    }

    /// Get the inode cache.
    pub fn cache(&self) -> Arc<InodeCache> {
        Arc::clone(&self.cache)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_inode_allocator_basic() {
        let allocator = InodeAllocator::new();

        assert_eq!(allocator.allocate(), 2);
        assert_eq!(allocator.allocate(), 3);
        assert_eq!(allocator.allocate(), 4);
    }

    #[test]
    fn test_inode_allocator_thread_safety() {
        let allocator = Arc::new(InodeAllocator::new());
        let mut handles = vec![];

        // Spawn 10 threads, each allocating 100 inodes
        for _ in 0..10 {
            let allocator = Arc::clone(&allocator);
            let handle = thread::spawn(move || {
                let mut inodes = vec![];
                for _ in 0..100 {
                    inodes.push(allocator.allocate());
                }
                inodes
            });
            handles.push(handle);
        }

        // Collect all allocated inodes
        let mut all_inodes = vec![];
        for handle in handles {
            all_inodes.extend(handle.join().unwrap());
        }

        // Should have 1000 unique inodes (100 * 10)
        all_inodes.sort();
        all_inodes.dedup();
        assert_eq!(all_inodes.len(), 1000);
        assert_eq!(*all_inodes.first().unwrap(), 2);
        assert_eq!(*all_inodes.last().unwrap(), 1001);
    }

    #[test]
    fn test_inode_allocator_set_next() {
        let allocator = InodeAllocator::new();

        allocator.set_next(100);
        assert_eq!(allocator.peek(), 100);
        assert_eq!(allocator.allocate(), 100);
        assert_eq!(allocator.allocate(), 101);
    }

    #[test]
    fn test_inode_cache_basic() {
        let cache = InodeCache::new(10, Duration::from_secs(60));
        let file_id = FileId::generate();
        let metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::RegularFile,
            size: 1024,
            permissions: 0o644,
            uid: 1000,
            gid: 1000,
            created_at: std::time::SystemTime::now(),
            modified_at: std::time::SystemTime::now(),
            accessed_at: std::time::SystemTime::now(),
        };

        // Cache miss
        assert!(cache.get(100).is_none());

        // Insert
        cache.insert(100, file_id, metadata.clone());

        // Cache hit
        let cached = cache.get(100).unwrap();
        assert_eq!(cached.file_id, file_id);
        assert_eq!(cached.metadata.size, 1024);
    }

    #[test]
    fn test_inode_cache_ttl_expiration() {
        let cache = InodeCache::new(10, Duration::from_millis(50));
        let file_id = FileId::generate();
        let metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::RegularFile,
            size: 1024,
            permissions: 0o644,
            uid: 1000,
            gid: 1000,
            created_at: std::time::SystemTime::now(),
            modified_at: std::time::SystemTime::now(),
            accessed_at: std::time::SystemTime::now(),
        };

        cache.insert(100, file_id, metadata);

        // Should be cached
        assert!(cache.get(100).is_some());

        // Wait for expiration
        thread::sleep(Duration::from_millis(60));

        // Should be expired
        assert!(cache.get(100).is_none());
    }

    #[test]
    fn test_inode_cache_lru_eviction() {
        let cache = InodeCache::new(3, Duration::from_secs(60));
        let metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::RegularFile,
            size: 1024,
            permissions: 0o644,
            uid: 1000,
            gid: 1000,
            created_at: std::time::SystemTime::now(),
            modified_at: std::time::SystemTime::now(),
            accessed_at: std::time::SystemTime::now(),
        };

        // Fill cache to capacity
        cache.insert(1, FileId::generate(), metadata.clone());
        cache.insert(2, FileId::generate(), metadata.clone());
        cache.insert(3, FileId::generate(), metadata.clone());

        assert_eq!(cache.len(), 3);

        // Insert one more - should evict oldest (1)
        cache.insert(4, FileId::generate(), metadata);

        assert_eq!(cache.len(), 3);
        assert!(cache.get(1).is_none()); // Evicted
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());
        assert!(cache.get(4).is_some());
    }

    #[test]
    fn test_inode_cache_invalidation() {
        let cache = InodeCache::new(10, Duration::from_secs(60));
        let metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::RegularFile,
            size: 1024,
            permissions: 0o644,
            uid: 1000,
            gid: 1000,
            created_at: std::time::SystemTime::now(),
            modified_at: std::time::SystemTime::now(),
            accessed_at: std::time::SystemTime::now(),
        };

        cache.insert(100, FileId::generate(), metadata);
        assert!(cache.get(100).is_some());

        cache.invalidate(100);
        assert!(cache.get(100).is_none());
    }

    #[test]
    fn test_inode_cache_cleanup_expired() {
        let cache = InodeCache::new(10, Duration::from_millis(50));
        let metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::RegularFile,
            size: 1024,
            permissions: 0o644,
            uid: 1000,
            gid: 1000,
            created_at: std::time::SystemTime::now(),
            modified_at: std::time::SystemTime::now(),
            accessed_at: std::time::SystemTime::now(),
        };

        // Insert 5 entries
        for i in 1..=5 {
            cache.insert(i, FileId::generate(), metadata.clone());
        }

        assert_eq!(cache.len(), 5);

        // Wait for expiration
        thread::sleep(Duration::from_millis(60));

        // Cleanup expired entries
        cache.cleanup_expired();

        assert_eq!(cache.len(), 0);
    }
}
