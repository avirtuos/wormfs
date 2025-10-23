//! Caching layer for MetadataStore to reduce SQLite query overhead.
//!
//! This module provides in-memory caching for stripe records and chunk lists,
//! with a secondary index for efficient offset-based lookups.

use super::{ChunkRecord, FileId, StripeId, StripeRecord};
use moka::future::Cache;
use moka::notification::RemovalCause;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Configuration for metadata caches.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Stripe record cache size in MB (default: 64MB)
    pub stripe_cache_size_mb: usize,

    /// Stripe cache time-to-live in seconds (default: 10 seconds)
    /// Entries expire after this duration regardless of access frequency
    pub stripe_cache_ttl_secs: u64,

    /// Stripe cache time-to-idle in seconds (default: 5 seconds)
    /// Entries expire if not accessed within this duration
    pub stripe_cache_tti_secs: u64,

    /// Chunk list cache size in MB (default: 64MB)
    pub chunk_cache_size_mb: usize,

    /// Chunk cache time-to-live in seconds (default: 10 seconds)
    pub chunk_cache_ttl_secs: u64,

    /// Chunk cache time-to-idle in seconds (default: 5 seconds)
    pub chunk_cache_tti_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            stripe_cache_size_mb: 64,
            stripe_cache_ttl_secs: 10, // Short TTL to prevent stale data
            stripe_cache_tti_secs: 5,

            chunk_cache_size_mb: 64,
            chunk_cache_ttl_secs: 10,
            chunk_cache_tti_secs: 5,
        }
    }
}

/// Unified metadata cache holding both stripe records and chunk lists.
///
/// This cache uses Arc wrapping for zero-copy sharing (similar to FileStore's StripeCache)
/// and maintains a secondary index for efficient offset-based stripe lookups.
pub struct MetadataCache {
    /// Primary cache for stripe metadata: StripeId → Arc<StripeRecord>
    stripe_records: Cache<StripeId, Arc<StripeRecord>>,

    /// Primary cache for chunk lists: StripeId → Arc<Vec<ChunkRecord>>
    chunk_lists: Cache<StripeId, Arc<Vec<ChunkRecord>>>,

    /// Secondary index for offset lookups: FileId → (offset → StripeId)
    /// Automatically maintained via eviction listeners
    stripe_offset_index: Arc<RwLock<HashMap<FileId, BTreeMap<u64, StripeId>>>>,
}

impl MetadataCache {
    /// Create a new MetadataCache with the given configuration.
    pub fn new(config: &CacheConfig) -> Self {
        // Shared reference to index for eviction listener
        let index: Arc<RwLock<HashMap<FileId, BTreeMap<u64, StripeId>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let index_clone = Arc::clone(&index);

        // Build stripe records cache with eviction listener
        let stripe_records = Cache::builder()
            .max_capacity((config.stripe_cache_size_mb * 1_024 * 1_024) as u64)
            .weigher(|_key: &StripeId, value: &Arc<StripeRecord>| -> u32 {
                // Rough size estimate for StripeRecord
                (std::mem::size_of::<StripeRecord>() + 64)
                    .try_into()
                    .unwrap_or(u32::MAX)
            })
            .time_to_live(Duration::from_secs(config.stripe_cache_ttl_secs))
            .time_to_idle(Duration::from_secs(config.stripe_cache_tti_secs))
            .eviction_listener(
                move |_key: Arc<StripeId>, value: Arc<StripeRecord>, cause: RemovalCause| {
                    // Automatically clean up secondary index when stripe is evicted
                    // This keeps the index synchronized with the cache

                    // Don't clean index if entry is being replaced (new entry will update it)
                    if matches!(cause, RemovalCause::Replaced) {
                        return;
                    }

                    let mut idx = index_clone.write().unwrap();
                    if let Some(file_index) = idx.get_mut(&value.file_id) {
                        file_index.remove(&value.offset);
                        if file_index.is_empty() {
                            idx.remove(&value.file_id);
                        }
                    }
                },
            )
            .build();

        // Build chunk lists cache
        let chunk_lists = Cache::builder()
            .max_capacity((config.chunk_cache_size_mb * 1_024 * 1_024) as u64)
            .weigher(|_key: &StripeId, value: &Arc<Vec<ChunkRecord>>| -> u32 {
                (value.len() * std::mem::size_of::<ChunkRecord>())
                    .try_into()
                    .unwrap_or(u32::MAX)
            })
            .time_to_live(Duration::from_secs(config.chunk_cache_ttl_secs))
            .time_to_idle(Duration::from_secs(config.chunk_cache_tti_secs))
            .build();

        Self {
            stripe_records,
            chunk_lists,
            stripe_offset_index: index,
        }
    }

    // ===== Stripe Record Operations =====

    /// Get stripe record by StripeId (zero-copy via Arc).
    ///
    /// Returns `None` if not in cache (caller should read-through to database).
    pub async fn get_stripe_by_id(&self, stripe_id: &StripeId) -> Option<Arc<StripeRecord>> {
        self.stripe_records.get(stripe_id).await
    }

    /// Get stripe record by file offset (zero-copy via Arc).
    ///
    /// Uses secondary index to find which stripe contains the given offset,
    /// then returns the cached stripe record if it exists.
    ///
    /// Returns `None` if not in cache (caller should read-through to database).
    pub async fn get_stripe_by_offset(
        &self,
        file_id: FileId,
        offset: u64,
    ) -> Option<Arc<StripeRecord>> {
        // Step 1: Find candidate StripeId from secondary index
        let maybe_stripe_id = {
            let index = self.stripe_offset_index.read().unwrap();
            if let Some(file_index) = index.get(&file_id) {
                // Find stripe whose start offset is ≤ target offset
                // BTreeMap::range is efficient: O(log n)
                file_index
                    .range(..=offset)
                    .next_back()
                    .map(|(_, &stripe_id)| stripe_id)
            } else {
                None
            }
        };

        // Step 2: Get record and verify offset is within stripe bounds
        if let Some(stripe_id) = maybe_stripe_id {
            if let Some(record_arc) = self.stripe_records.get(&stripe_id).await {
                // Check if offset falls within [stripe.offset, stripe.offset + stripe.size)
                if offset >= record_arc.offset && offset < record_arc.offset + record_arc.size {
                    return Some(record_arc);
                }
            }
        }

        None
    }

    /// Insert stripe record into cache and update secondary index.
    ///
    /// The record is wrapped in Arc for zero-copy sharing.
    pub async fn insert_stripe(&self, record: StripeRecord) {
        let stripe_id = record.stripe_id;
        let file_id = record.file_id;
        let offset = record.offset;

        // Wrap in Arc and insert into primary cache
        self.stripe_records
            .insert(stripe_id, Arc::new(record))
            .await;

        // Update secondary index
        let mut index = self.stripe_offset_index.write().unwrap();
        index
            .entry(file_id)
            .or_insert_with(BTreeMap::new)
            .insert(offset, stripe_id);
    }

    // ===== Chunk List Operations =====

    /// Get chunk list for a stripe (zero-copy via Arc).
    ///
    /// Returns `None` if not in cache (caller should read-through to database).
    pub async fn get_chunks(&self, stripe_id: &StripeId) -> Option<Arc<Vec<ChunkRecord>>> {
        self.chunk_lists.get(stripe_id).await
    }

    /// Insert chunk list into cache.
    ///
    /// The chunk list is wrapped in Arc for zero-copy sharing.
    pub async fn insert_chunks(&self, stripe_id: StripeId, chunks: Vec<ChunkRecord>) {
        self.chunk_lists.insert(stripe_id, Arc::new(chunks)).await;
    }

    // ===== Invalidation Operations =====

    /// Invalidate specific stripe (both record and chunk list).
    ///
    /// The eviction listener will automatically clean up the secondary index.
    pub async fn invalidate_stripe(&self, stripe_id: &StripeId) {
        self.stripe_records.invalidate(stripe_id).await;
        self.chunk_lists.invalidate(stripe_id).await;
    }

    /// Invalidate all stripes for a file.
    ///
    /// This is useful when a file is deleted or truncated.
    pub async fn invalidate_file(&self, file_id: &FileId) {
        // Collect all StripeIds for this file from the secondary index
        let stripe_ids: Vec<StripeId> = {
            let index = self.stripe_offset_index.read().unwrap();
            if let Some(file_index) = index.get(file_id) {
                file_index.values().copied().collect()
            } else {
                Vec::new()
            }
        };

        // Invalidate each stripe (eviction listener cleans index)
        for stripe_id in stripe_ids {
            self.invalidate_stripe(&stripe_id).await;
        }
    }

    /// Invalidate entire cache (all stripes and chunks).
    ///
    /// This is a nuclear option rarely needed in production.
    pub async fn invalidate_all(&self) {
        self.stripe_records.invalidate_all();
        self.chunk_lists.invalidate_all();

        // Clear secondary index
        let mut index = self.stripe_offset_index.write().unwrap();
        index.clear();
    }

    // ===== Statistics =====

    /// Get stripe cache statistics.
    pub fn stripe_stats(&self) -> CacheStats {
        CacheStats {
            entry_count: self.stripe_records.entry_count(),
            weighted_size: self.stripe_records.weighted_size(),
        }
    }

    /// Get chunk cache statistics.
    pub fn chunk_stats(&self) -> CacheStats {
        CacheStats {
            entry_count: self.chunk_lists.entry_count(),
            weighted_size: self.chunk_lists.weighted_size(),
        }
    }
}

/// Cache statistics for monitoring.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of entries in the cache
    pub entry_count: u64,

    /// Total weighted size of cache entries in bytes
    pub weighted_size: u64,
}

#[cfg(test)]
#[path = "cache_tests.rs"]
mod tests;
