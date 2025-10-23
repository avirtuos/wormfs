//! Per-file-handle write buffering that coalesces metadata and data changes.
//!
//! BufferedFileHandle replaces the problematic StripeCache with a simpler, per-file-handle
//! write buffering mechanism. Each file handle maintains its own isolated buffer of uncommitted
//! metadata and data changes, eliminating cross-file race conditions.
//!
//! # Key Design Principles
//!
//! 1. **One buffer per file handle** - Eliminates cross-file race conditions
//! 2. **Metadata and data together** - Prevents consistency issues
//! 3. **Simple flush semantics** - Only full flushes make data visible
//! 4. **Memory-pressure driven** - Flush when buffers get too large
//! 5. **Read-through caching** - Reads see buffered writes immediately
//!
//! # Usage
//!
//! ```no_run
//! # use wormfs::filesystem_service::buffered_file_handle::*;
//! # use wormfs::file_store::types::*;
//! # use std::sync::Arc;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create handle (normally done by FileSystemService on file open)
//! let config = BufferedFileHandleConfig::default();
//! // let handle = BufferedFileHandle::new(...);
//!
//! // Write data (buffered, not immediately visible)
//! // handle.write(0, &data).await?;
//!
//! // Read sees buffered writes
//! // let data = handle.read(0, 1024).await?;
//!
//! // Flush makes changes visible
//! // handle.full_flush().await?;
//! # Ok(())
//! # }
//! ```

use crate::file_store::{
    types::{ChunkMetadata, FileId, StoragePolicy, StripeId, StripeMetadata},
    FileStore, StripeBuilder,
};
use crate::filesystem_service::types::{Error, FileAttr};
use crate::metadata_store::{MetadataStore, MetadataStoreImpl};
use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tracing::trace;

/// Configuration for BufferedFileHandle behavior.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BufferedFileHandleConfig {
    /// Maximum memory per handle before triggering partial flush (bytes)
    pub max_memory_bytes: usize,

    /// Maximum time between full flushes (in seconds when serialized)
    #[serde(with = "serde_duration_seconds")]
    pub max_flush_interval: Duration,

    /// Maximum writes before forcing full flush
    pub max_writes_before_flush: usize,

    /// Stripe size (from FileStore config, typically chunk_size * data_shards)
    pub max_stripe_size: usize,
}

/// Serde helper module for Duration serialization/deserialization as seconds.
mod serde_duration_seconds {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

impl Default for BufferedFileHandleConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 20 * 1024 * 1024, // 20MB
            max_flush_interval: Duration::from_secs(10),
            max_writes_before_flush: 10000,
            max_stripe_size: 4 * 1024 * 1024, // 4MB
        }
    }
}

/// Simplified Raft interface for BufferedFileHandle.
///
/// In Phase 3, this will be replaced with real StorageRaftMember integration.
/// For now, it provides a mockable interface for testing.
#[async_trait::async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait RaftClient: Send + Sync {
    /// Propose a batch of stripe operations atomically.
    ///
    /// All operations succeed or fail together (single Raft log entry).
    async fn propose_stripe_batch(&self, operations: Vec<StripeOperation>) -> Result<(), Error>;
}

/// Operations on stripe metadata that can be batched.
#[derive(Debug, Clone)]
pub enum StripeOperation {
    /// Create a new stripe
    Create {
        file_id: FileId,
        stripe: StripeMetadata,
    },
    /// Update an existing stripe
    Update {
        file_id: FileId,
        stripe: StripeMetadata,
    },
    /// Delete a stripe
    Delete { stripe_id: StripeId },
    /// Update file attributes
    UpdateAttributes {
        file_id: FileId,
        inode: u64,
        attributes: FileAttr,
    },
}

/// Tracks where a stripe originated from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StripeOrigin {
    /// Stripe was loaded from MetadataStore (needs tombstoning when replaced)
    FromMetadataStore,
    /// Stripe was created in this session (can be replaced without tombstoning)
    CreatedInSession,
}

/// Represents a stripe in BufferedFileHandle's in-memory state.
#[derive(Debug, Clone)]
struct BufferedStripeMetadata {
    /// The actual stripe metadata
    metadata: StripeMetadata,

    /// Tracks where this stripe came from
    origin: StripeOrigin,

    /// Tracks whether this is a dirty update that needs flushing
    dirty: bool,
}

/// Inner state of BufferedFileHandle (protected by single Mutex).
struct BufferedFileHandleInner {
    /// File this handle is for
    file_id: FileId,
    inode: u64,

    /// Configuration
    config: BufferedFileHandleConfig,

    /// In-memory file attributes (may be stale + buffered changes)
    attributes: FileAttr,

    /// Map of stripe_index -> StripeMetadata
    /// Includes both committed (from MetadataStore) and buffered stripes
    /// IndexMap preserves insertion order for flush
    stripes: IndexMap<u32, BufferedStripeMetadata>,

    /// Stripes marked for deletion (tombstones)
    /// These were in MetadataStore but should be deleted on flush
    tombstones: HashSet<StripeId>,

    /// Tombstones tracked by stripe index for coordinated flushing
    /// Only commit tombstone when the replacement stripe is flushed
    /// Key: stripe_index, Value: old StripeId to tombstone
    tombstones_by_stripe: HashMap<u32, StripeId>,

    /// Active stripe builders (unbuffered data)
    /// Key: stripe_index
    builders: HashMap<u32, StripeBuilder>,

    /// Dirty tracking
    dirty_metadata: bool,
    dirty_data: bool,

    /// Memory accounting - only complete stripes trigger memory pressure
    complete_stripe_bytes: usize,

    /// Memory accounting - partial stripes tracked but don't trigger pressure
    partial_stripe_bytes: usize,

    /// Total buffered bytes (for diagnostics)
    buffered_bytes: usize,

    /// Last full flush time
    last_flush: Option<Instant>,

    /// Number of writes since last full flush
    writes_since_flush: usize,

    /// Storage policy for this file
    storage_policy: Arc<StoragePolicy>,
}

impl BufferedFileHandleInner {
    /// Recalculate memory usage from all builders.
    ///
    /// This ensures accurate memory accounting by iterating through all builders
    /// and calculating complete_stripe_bytes, partial_stripe_bytes, and buffered_bytes
    /// from scratch. This is more reliable than incremental updates which can drift
    /// due to overwrites, partial-to-complete transitions, and flush operations.
    fn recalculate_memory_usage(&mut self) {
        let mut complete = 0;
        let mut partial = 0;
        let mut total = 0;

        let max_stripe_size = self.config.max_stripe_size as usize;

        for builder in self.builders.values() {
            let size = builder.size();
            total += size;

            // A stripe is "complete" when it reaches exactly max_stripe_size
            if size == max_stripe_size {
                complete += size;
            } else {
                partial += size;
            }
        }

        self.complete_stripe_bytes = complete;
        self.partial_stripe_bytes = partial;
        self.buffered_bytes = total;
    }

    /// Calculate stripe file offset with overflow checking.
    ///
    /// Returns the byte offset in the logical file where this stripe begins.
    /// Protects against integer overflow for very large files.
    fn checked_stripe_offset(&self, stripe_idx: u32) -> Result<u64, Error> {
        let stripe_size = self.config.max_stripe_size as u64;
        let stripe_idx_u64 = stripe_idx as u64;
        stripe_idx_u64.checked_mul(stripe_size).ok_or_else(|| {
            Error::Internal(format!(
                "Stripe offset calculation overflow: stripe_idx={} * stripe_size={}",
                stripe_idx, stripe_size
            ))
        })
    }
}

/// Trait for reporting BufferedFileHandle metrics to parent service.
///
/// This allows BufferedFileHandle to report flush events and write patterns
/// without direct coupling to the metrics system.
pub trait BufferedMetricsReporter: Send + Sync {
    /// Report a flush operation.
    ///
    /// # Arguments
    /// * `is_full` - true for full flush (force=true), false for partial flush (force=false)
    /// * `latency_secs` - Duration of the flush operation in seconds
    fn report_flush(&self, is_full: bool, latency_secs: f64);

    /// Report a coalesced write (write that reused an existing stripe builder).
    fn report_write_coalesced(&self);

    /// Report a flush triggered by memory pressure.
    fn report_memory_pressure_flush(&self);

    /// Report an inform() call by operation type.
    ///
    /// # Arguments
    /// * `op_type` - The type of operation that triggered the inform() call
    fn report_inform(&self, op_type: OperationType);
}

/// Per-file-handle write buffer that coalesces metadata and data changes.
///
/// Maintains an in-memory snapshot of file state plus uncommitted changes.
/// All mutations stay local until flush().
///
/// # Concurrency
///
/// Wrapped in Mutex for simplicity. POSIX doesn't define behavior for concurrent
/// writes to same file descriptor, so serialization is acceptable.
pub struct BufferedFileHandle {
    /// All state protected by single mutex (simpler than RwLock hierarchy)
    inner: Arc<Mutex<BufferedFileHandleInner>>,

    /// Dependencies (outside mutex to avoid lock during async calls)
    metadata_store: Arc<MetadataStoreImpl>,
    file_store: Arc<dyn FileStore + Send + Sync>,
    raft_client: Arc<dyn RaftClient>,

    /// Optional metrics reporter for observability
    metrics_reporter: Option<Arc<dyn BufferedMetricsReporter>>,
}

// Manual Debug implementation since RaftClient is a trait object
impl std::fmt::Debug for BufferedFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferedFileHandle")
            .field("has_raft_client", &true)
            .finish_non_exhaustive()
    }
}

impl BufferedFileHandle {
    /// Create a new BufferedFileHandle for a file.
    ///
    /// # Arguments
    ///
    /// * `file_id` - File identifier
    /// * `inode` - Inode number
    /// * `attributes` - Initial file attributes
    /// * `storage_policy` - Storage policy for this file
    /// * `config` - Buffer configuration
    /// * `metadata_store` - MetadataStore for reading committed state
    /// * `file_store` - FileStore for writing stripe data
    /// * `raft_client` - Raft client for metadata updates (use MockRaftClient for testing)
    /// * `metrics_reporter` - Optional metrics reporter for observability
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        file_id: FileId,
        inode: u64,
        attributes: FileAttr,
        storage_policy: Arc<StoragePolicy>,
        config: BufferedFileHandleConfig,
        metadata_store: Arc<MetadataStoreImpl>,
        file_store: Arc<dyn FileStore + Send + Sync>,
        raft_client: Arc<dyn RaftClient>,
        metrics_reporter: Option<Arc<dyn BufferedMetricsReporter>>,
    ) -> Self {
        let inner = BufferedFileHandleInner {
            file_id,
            inode,
            config,
            attributes,
            stripes: IndexMap::new(),
            tombstones: HashSet::new(),
            tombstones_by_stripe: HashMap::new(),
            builders: HashMap::new(),
            dirty_metadata: false,
            dirty_data: false,
            complete_stripe_bytes: 0,
            partial_stripe_bytes: 0,
            buffered_bytes: 0,
            last_flush: None,
            writes_since_flush: 0,
            storage_policy,
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
            metadata_store,
            file_store,
            raft_client,
            metrics_reporter,
        }
    }

    /// Write data to the file at the specified offset.
    ///
    /// Data is buffered in-memory and not immediately visible to other handles.
    /// May trigger partial or full flush if memory pressure or write count thresholds met.
    ///
    /// # Arguments
    ///
    /// * `offset` - Byte offset in file where write begins
    /// * `data` - Data to write
    ///
    /// # Returns
    ///
    /// Number of bytes written.
    pub async fn write(&self, offset: u64, data: &[u8]) -> Result<u32, Error> {
        let mut bytes_written = 0;
        let mut remaining = data;
        let mut current_offset = offset;
        let mut had_coalescence = false;

        let config = {
            let inner = self.inner.lock().expect(
                "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
            );
            inner.config.clone()
        };

        while !remaining.is_empty() {
            // Calculate which stripe this write affects
            let stripe_idx = (current_offset / config.max_stripe_size as u64) as u32;
            let offset_in_stripe = (current_offset % config.max_stripe_size as u64) as usize;

            trace!(
                stripe_idx = %stripe_idx,
                offset_in_stripe = %offset_in_stripe,
                remaining_bytes = %remaining.len(),
                current_offset = %current_offset,
                "Write loop iteration"
            );

            // Check if we need to load an existing stripe for read-modify-write
            let needs_load = {
                let inner = self.inner.lock()
                .expect("BufferedFileHandle inner lock poisoned - indicates panic during file operation");
                let has_builder = inner.builders.contains_key(&stripe_idx);
                trace!(
                    stripe_idx = %stripe_idx,
                    has_builder = %has_builder,
                    "Checking if need to load existing stripe"
                );
                !has_builder
            };

            if needs_load {
                trace!(stripe_idx = %stripe_idx, "Calling get_or_load_existing_stripe");
                // Check for existing stripe that needs read-modify-write
                if let Some((existing_stripe, existing_data_arc)) =
                    self.get_or_load_existing_stripe(stripe_idx).await?
                {
                    // Clone the Arc contents to get a mutable copy for modification
                    // This is a one-time cost for writes; reads get zero-copy benefits
                    let mut existing_data = (*existing_data_arc).clone();

                    // Calculate how much to overwrite in this stripe
                    let bytes_to_write = remaining
                        .len()
                        .min(config.max_stripe_size - offset_in_stripe);

                    // Ensure the buffer is large enough to accommodate the write
                    // Extend buffer if we're writing past its current end
                    let required_size = offset_in_stripe + bytes_to_write;
                    if existing_data.len() < required_size {
                        existing_data.resize(required_size, 0);
                    }

                    // Overwrite the specific range in the existing data
                    existing_data[offset_in_stripe..offset_in_stripe + bytes_to_write]
                        .copy_from_slice(&remaining[..bytes_to_write]);

                    trace!(
                        stripe_idx = %stripe_idx,
                        offset_in_stripe = %offset_in_stripe,
                        bytes_written = %bytes_to_write,
                        original_size = %existing_stripe.size,
                        new_size = %existing_data.len(),
                        "Read-modify-write: Overwrote data in existing stripe"
                    );

                    let mut inner = self.inner.lock()
                    .expect("BufferedFileHandle inner lock poisoned - indicates panic during file operation");
                    let file_id = inner.file_id;
                    let storage_policy = Arc::clone(&inner.storage_policy);
                    let stripe_file_offset = inner.checked_stripe_offset(stripe_idx)?;

                    // Create new builder with NEW StripeId (immutability)
                    let mut new_builder = StripeBuilder::new(
                        file_id,
                        stripe_idx,
                        stripe_file_offset,
                        config.max_stripe_size,
                        storage_policy,
                    );

                    trace!(
                        stripe_idx = %stripe_idx,
                        new_stripe_id = ?new_builder.stripe_id(),
                        existing_stripe_id = ?existing_stripe.stripe_id,
                        "Read-modify-write: Creating new builder to replace existing stripe"
                    );

                    // Initialize new builder with the MODIFIED data
                    new_builder.append(&existing_data).map_err(|e| {
                        Error::DataFailed(format!("Failed to copy modified stripe data: {}", e))
                    })?;

                    // Track the tombstone BY STRIPE INDEX
                    // This tombstone only gets committed when this stripe is flushed
                    inner
                        .tombstones_by_stripe
                        .insert(stripe_idx, existing_stripe.stripe_id);

                    trace!(
                        stripe_idx = %stripe_idx,
                        old_stripe_id = ?existing_stripe.stripe_id,
                        "Marked old stripe for tombstone"
                    );

                    // Cache the metadata for the new stripe being built
                    inner.stripes.insert(
                        stripe_idx,
                        BufferedStripeMetadata {
                            metadata: StripeMetadata {
                                stripe_id: new_builder.stripe_id(),
                                file_id,
                                offset: stripe_file_offset,
                                size: existing_data.len() as u64,
                                checksum: existing_stripe.checksum,
                                chunks: vec![], // Will be filled during flush
                            },
                            origin: StripeOrigin::CreatedInSession,
                            dirty: true,
                        },
                    );

                    inner.builders.insert(stripe_idx, new_builder);

                    // Update loop variables to reflect what we just wrote
                    bytes_written += bytes_to_write as u32;
                    remaining = &remaining[bytes_to_write..];
                    current_offset += bytes_to_write as u64;

                    // Skip the normal append logic below since we already handled this write
                    continue;
                }
            }

            // Get or create builder for this stripe
            let (written, is_new_builder) = {
                let mut inner = self.inner.lock()
                    .expect("BufferedFileHandle inner lock poisoned - indicates panic during file operation");

                // Need to extract these before calling or_insert_with to avoid borrow issues
                let file_id = inner.file_id;
                let storage_policy = Arc::clone(&inner.storage_policy);

                let is_new_builder = !inner.builders.contains_key(&stripe_idx);

                // Check if we're trying to overwrite in the middle of an existing builder
                let needs_overwrite =
                    if let Some(existing_builder) = inner.builders.get(&stripe_idx) {
                        let current_size = existing_builder.size();
                        // If write offset is before the current end, we need to overwrite
                        offset_in_stripe < current_size
                    } else {
                        false
                    };

                // Calculate stripe offset with overflow check before or_insert_with
                let stripe_file_offset = inner.checked_stripe_offset(stripe_idx)?;

                let builder = inner.builders.entry(stripe_idx).or_insert_with(|| {
                    let new_builder = StripeBuilder::new(
                        file_id,
                        stripe_idx,
                        stripe_file_offset,
                        config.max_stripe_size,
                        storage_policy,
                    );
                    trace!(
                        stripe_idx = %stripe_idx,
                        stripe_id = ?new_builder.stripe_id(),
                        file_offset = %stripe_file_offset,
                        "Creating new StripeBuilder"
                    );
                    new_builder
                });

                // Either overwrite or append depending on offset
                let written = if needs_overwrite {
                    trace!(
                        stripe_idx = %stripe_idx,
                        offset_in_stripe = %offset_in_stripe,
                        current_size = %builder.size(),
                        data_len = %remaining.len(),
                        "Overwriting data in existing builder"
                    );
                    builder
                        .overwrite(offset_in_stripe, remaining)
                        .map_err(|e| {
                            Error::DataFailed(format!("Failed to overwrite stripe builder: {}", e))
                        })?
                } else {
                    builder.append(remaining).map_err(|e| {
                        Error::DataFailed(format!("Failed to append to stripe builder: {}", e))
                    })?
                };

                trace!(
                    action = if is_new_builder { "Created and appended" } else { "Appended" },
                    written_bytes = %written,
                    stripe_idx = %stripe_idx,
                    stripe_id = ?builder.stripe_id(),
                    offset_in_stripe = %offset_in_stripe,
                    total_stripe_size = %builder.size(),
                    "Write operation to stripe"
                );

                // Recalculate memory accounting from all builders
                // This ensures accuracy even with overwrites and partial-to-complete transitions
                inner.recalculate_memory_usage();

                (written, is_new_builder)
            };

            // Track if any coalescence occurred (metric reported after loop)
            if !is_new_builder {
                had_coalescence = true;
            }

            bytes_written += written as u32;
            remaining = &remaining[written..];
            current_offset += written as u64;

            // Check if need full flush due to memory pressure (outside lock)
            if self.needs_memory_flush() {
                trace!("Triggering full_flush due to memory pressure (complete stripes only)");

                // Report memory pressure flush before executing
                if let Some(reporter) = &self.metrics_reporter {
                    reporter.report_memory_pressure_flush();
                }

                self.full_flush(false).await?; // false = only flush complete stripes
            }
        }

        // Report write coalescence metric once per write operation if any coalescence occurred
        if had_coalescence {
            if let Some(reporter) = &self.metrics_reporter {
                reporter.report_write_coalesced();
            }
        }

        // Update file attributes
        {
            let mut inner = self.inner.lock().expect(
                "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
            );
            let new_size = (offset + bytes_written as u64).max(inner.attributes.size);
            inner.attributes.size = new_size;
            inner.attributes.mtime = SystemTime::now();
            inner.attributes.atime = SystemTime::now();

            inner.dirty_data = true;
            inner.dirty_metadata = true;
            inner.writes_since_flush += 1;
        }

        // Check if need full flush
        if self.needs_full_flush() {
            trace!("Triggering auto-flush after write (writes_since_flush >= threshold or time elapsed)");
            //self.full_flush(true).await?; // true = flush everything (time/count based flush)
        }

        Ok(bytes_written)
    }

    /// Check if a stripe already exists and load it if necessary.
    ///
    /// Returns Some((metadata, data)) if stripe exists in cache or MetadataStore.
    /// Returns None if no existing stripe found (safe to create new one).
    ///
    /// # Arguments
    ///
    /// * `stripe_idx` - Stripe index to check
    ///
    /// # Returns
    ///
    /// Option with (StripeMetadata, Arc<Vec<u8>>) if stripe exists, None otherwise (Arc enables zero-copy sharing).
    async fn get_or_load_existing_stripe(
        &self,
        stripe_idx: u32,
    ) -> Result<Option<(StripeMetadata, Arc<Vec<u8>>)>, Error> {
        trace!(stripe_idx = %stripe_idx, "get_or_load_existing_stripe: Entry");

        let (file_id, max_stripe_size, cached_stripe) = {
            let inner = self.inner.lock().expect(
                "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
            );

            trace!(
                stripe_idx = %stripe_idx,
                has_builder = %inner.builders.contains_key(&stripe_idx),
                has_stripe_metadata = %inner.stripes.contains_key(&stripe_idx),
                "get_or_load_existing_stripe: Locked inner"
            );

            // Check 1: Active builder exists - don't load, we're already writing to it
            if inner.builders.contains_key(&stripe_idx) {
                trace!(stripe_idx = %stripe_idx, "get_or_load_existing_stripe: Builder exists, returning None");
                return Ok(None);
            }

            // Check 2: Cached stripe metadata
            let cached = inner.stripes.get(&stripe_idx).map(|s| s.metadata.clone());

            (inner.file_id, inner.config.max_stripe_size, cached)
        };

        // If we have cached metadata, load the stripe data
        if let Some(stripe_meta) = cached_stripe {
            trace!(
                stripe_idx = %stripe_idx,
                stripe_id = ?stripe_meta.stripe_id,
                "get_or_load_existing_stripe: Found cached stripe, loading from FileStore"
            );

            // Get chunks from MetadataStore
            let chunk_records = self
                .metadata_store
                .get_stripe_chunks(stripe_meta.stripe_id)
                .await
                .map_err(|e| {
                    Error::DataFailed(format!("Failed to get chunks for stripe: {}", e))
                })?;

            // Convert to ChunkMetadata
            let chunks: Vec<ChunkMetadata> = chunk_records
                .iter()
                .map(|r| ChunkMetadata {
                    chunk_id: r.chunk_id,
                    chunk_index: r.chunk_index,
                    node_id: r.node_id,
                    disk_id: r.disk_id,
                })
                .collect();

            // Read stripe data from FileStore (Arc-wrapped for zero-copy sharing)
            let data: Arc<Vec<u8>> = self
                .file_store
                .read_stripe(file_id, stripe_meta.stripe_id, chunks)
                .await
                .map_err(|e| Error::DataFailed(format!("Failed to read stripe: {}", e)))?;

            return Ok(Some((stripe_meta, data)));
        }

        // Check 3: Query MetadataStore for stripe at this offset
        let stripe_offset = stripe_idx as u64 * max_stripe_size as u64;
        let stripe_result = self
            .metadata_store
            .get_stripe_at_offset(file_id, stripe_offset)
            .await;

        if let Ok(stripe_record) = stripe_result {
            trace!(
                stripe_idx = %stripe_idx,
                "Found stripe in MetadataStore, loading from FileStore"
            );

            // Get chunks
            let chunk_records = self
                .metadata_store
                .get_stripe_chunks(stripe_record.stripe_id)
                .await
                .map_err(|e| {
                    Error::DataFailed(format!("Failed to get chunks for stripe: {}", e))
                })?;

            // Convert to ChunkMetadata
            let chunks: Vec<ChunkMetadata> = chunk_records
                .iter()
                .map(|r| ChunkMetadata {
                    chunk_id: r.chunk_id,
                    chunk_index: r.chunk_index,
                    node_id: r.node_id,
                    disk_id: r.disk_id,
                })
                .collect();

            // Read stripe data (Arc-wrapped for zero-copy sharing)
            let data: Arc<Vec<u8>> = self
                .file_store
                .read_stripe(file_id, stripe_record.stripe_id, chunks)
                .await
                .map_err(|e| Error::DataFailed(format!("Failed to read stripe: {}", e)))?;

            // Convert StripeRecord to StripeMetadata
            let stripe_meta = StripeMetadata {
                stripe_id: stripe_record.stripe_id,
                file_id,
                offset: stripe_record.offset,
                size: stripe_record.size,
                checksum: stripe_record.checksum,
                chunks: chunk_records
                    .iter()
                    .map(|r| ChunkMetadata {
                        chunk_id: r.chunk_id,
                        chunk_index: r.chunk_index,
                        node_id: r.node_id,
                        disk_id: r.disk_id,
                    })
                    .collect(),
            };

            return Ok(Some((stripe_meta, data)));
        }

        // No existing stripe found
        Ok(None)
    }

    /// Read data from the file at the specified offset.
    ///
    /// Reads see buffered writes from this handle (read-your-writes guarantee).
    /// For non-buffered data, reads through to MetadataStore/FileStore.
    ///
    /// # Arguments
    ///
    /// * `offset` - Byte offset in file where read begins
    /// * `size` - Number of bytes to read
    ///
    /// # Returns
    ///
    /// Data read from file (may be less than requested if EOF reached).
    pub async fn read(&self, offset: u64, size: u32) -> Result<Vec<u8>, Error> {
        let mut result = Vec::with_capacity(size as usize);
        let config = {
            let inner = self.inner.lock().expect(
                "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
            );
            inner.config.clone()
        };

        let end_offset = offset.saturating_add(size as u64);

        // Calculate stripe range
        let start_stripe = (offset / config.max_stripe_size as u64) as u32;
        let end_stripe = if end_offset == 0 {
            0
        } else {
            ((end_offset - 1) / config.max_stripe_size as u64) as u32
        };

        for stripe_idx in start_stripe..=end_stripe {
            // Calculate read range within this stripe
            let stripe_start_offset = stripe_idx as u64 * config.max_stripe_size as u64;
            let stripe_end_offset = stripe_start_offset + config.max_stripe_size as u64;

            let read_start = offset.max(stripe_start_offset);
            let read_end = end_offset.min(stripe_end_offset);

            if read_start >= read_end {
                continue;
            }

            let offset_in_stripe = (read_start - stripe_start_offset) as usize;
            let bytes_to_read = (read_end - read_start) as usize;

            // Check if data is in a builder
            let builder_data = {
                let inner = self.inner.lock()
                .expect("BufferedFileHandle inner lock poisoned - indicates panic during file operation");
                inner.builders.get(&stripe_idx).map(|builder| {
                    let data = builder.data();
                    let available = data.len().saturating_sub(offset_in_stripe);
                    let to_copy = bytes_to_read.min(available);
                    data[offset_in_stripe..offset_in_stripe + to_copy].to_vec()
                })
            };

            if let Some(data) = builder_data {
                result.extend_from_slice(&data);
                if data.len() < bytes_to_read {
                    // Partial data in builder, pad with zeros or read rest from storage
                    let remaining = bytes_to_read - data.len();
                    result.extend(vec![0u8; remaining]);
                }
                continue;
            }

            // Check buffered stripes (metadata that's been flushed or is pending flush)
            let buffered_stripe = {
                let inner = self.inner.lock()
                .expect("BufferedFileHandle inner lock poisoned - indicates panic during file operation");
                inner.stripes.get(&stripe_idx).cloned()
            };

            if let Some(buffered) = buffered_stripe {
                // Read from FileStore using the stripe metadata
                let file_id = {
                    let inner = self.inner.lock()
                .expect("BufferedFileHandle inner lock poisoned - indicates panic during file operation");
                    inner.file_id
                };

                // Arc-wrapped stripe data enables zero-copy sharing across reads
                let stripe_data: Arc<Vec<u8>> = self
                    .file_store
                    .read_stripe(
                        file_id,
                        buffered.metadata.stripe_id,
                        buffered.metadata.chunks.clone(),
                    )
                    .await
                    .map_err(|e| Error::DataFailed(format!("Failed to read stripe: {}", e)))?;

                // Arc<Vec<u8>> derefs to &[u8], so slicing works transparently
                let available = stripe_data.len().saturating_sub(offset_in_stripe);
                let to_copy = bytes_to_read.min(available);
                result
                    .extend_from_slice(&stripe_data[offset_in_stripe..offset_in_stripe + to_copy]);

                // Pad with zeros if needed
                if to_copy < bytes_to_read {
                    result.extend(vec![0u8; bytes_to_read - to_copy]);
                }
            } else {
                // Stripe not found in buffer - fetch from MetadataStore
                let (file_id, stripe_file_offset) = {
                    let inner = self.inner.lock()
                .expect("BufferedFileHandle inner lock poisoned - indicates panic during file operation");
                    (inner.file_id, inner.checked_stripe_offset(stripe_idx)?)
                };

                // Query MetadataStore for stripe at this file offset
                trace!(
                    stripe_idx = %stripe_idx,
                    file_id = ?file_id,
                    offset = %stripe_file_offset,
                    "Stripe not in buffer, querying MetadataStore"
                );

                match self
                    .metadata_store
                    .get_stripe_at_offset(file_id, stripe_file_offset)
                    .await
                {
                    Ok(stripe_metadata) => {
                        trace!(
                            stripe_id = ?stripe_metadata.stripe_id,
                            size = %stripe_metadata.size,
                            "Found stripe in MetadataStore"
                        );
                        // Found in MetadataStore - fetch chunks and read from FileStore
                        let chunk_records = self
                            .metadata_store
                            .get_stripe_chunks(stripe_metadata.stripe_id)
                            .await
                            .map_err(|e| {
                                Error::Internal(format!("Failed to get stripe chunks: {}", e))
                            })?;

                        // Convert to ChunkMetadata
                        let chunks: Vec<_> = chunk_records
                            .iter()
                            .map(|r| ChunkMetadata {
                                chunk_id: r.chunk_id,
                                node_id: r.node_id,
                                disk_id: r.disk_id,
                                chunk_index: r.chunk_index,
                            })
                            .collect();

                        // Arc-wrapped stripe data for zero-copy sharing
                        let stripe_data: Arc<Vec<u8>> = self
                            .file_store
                            .read_stripe(file_id, stripe_metadata.stripe_id, chunks)
                            .await
                            .map_err(|e| {
                                Error::DataFailed(format!("Failed to read stripe: {}", e))
                            })?;

                        // Arc derefs to &[u8] for transparent slicing
                        let available = stripe_data.len().saturating_sub(offset_in_stripe);
                        let to_copy = bytes_to_read.min(available);
                        result.extend_from_slice(
                            &stripe_data[offset_in_stripe..offset_in_stripe + to_copy],
                        );

                        // Pad with zeros if needed
                        if to_copy < bytes_to_read {
                            result.extend(vec![0u8; bytes_to_read - to_copy]);
                        }
                    }
                    Err(e) => {
                        trace!(
                            error = %e,
                            zero_bytes = %bytes_to_read,
                            "Stripe not in MetadataStore (sparse region), returning zeros"
                        );
                        // Stripe not in MetadataStore - sparse region, return zeros
                        result.extend(vec![0u8; bytes_to_read]);
                    }
                }
            }
        }

        Ok(result)
    }

    /// Perform a full flush to make all changes visible.
    ///
    /// Writes all buffered data to FileStore and updates MetadataStore atomically.
    /// After successful flush, all changes are visible to other handles.
    ///
    /// This is called automatically when write count or time thresholds are met,
    /// or can be called explicitly (e.g., on file close or fsync).
    pub async fn full_flush(&self, force: bool) -> Result<(), Error> {
        // Start timing for metrics
        let start_time = std::time::Instant::now();

        // 1. Flush complete builders to FileStore (or all if force=true)
        let (builders_to_flush, config) = {
            let mut inner = self.inner.lock().expect(
                "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
            );
            let max_stripe_size = inner.config.max_stripe_size;

            // Partition builders into complete and partial
            let (complete, partial): (Vec<_>, Vec<_>) = inner
                .builders
                .iter()
                .partition(|(_, builder)| builder.size() == max_stripe_size as usize);

            trace!(
                force = %force,
                complete_builders = %complete.len(),
                partial_builders = %partial.len(),
                "full_flush: evaluating builders"
            );

            if !force && complete.is_empty() {
                // Nothing to flush - all stripes are partial
                trace!("No complete stripes to flush (force=false)");
                return Ok(());
            }

            // Determine which builders to flush
            let indices_to_flush: Vec<u32> = if force {
                // Flush everything
                inner.builders.keys().cloned().collect()
            } else {
                // Only flush complete stripes
                complete.iter().map(|(idx, _)| **idx).collect()
            };

            // Extract builders to flush
            let mut to_flush = Vec::new();
            for idx in &indices_to_flush {
                if let Some(builder) = inner.builders.remove(idx) {
                    to_flush.push((*idx, builder));
                }
            }

            (to_flush, inner.config.clone())
        };

        // Collect indices before consuming builders
        let flushed_indices_vec: Vec<u32> = builders_to_flush.iter().map(|(idx, _)| *idx).collect();

        let mut flushed_count = 0;
        for (stripe_idx, builder) in builders_to_flush {
            if builder.is_empty() {
                trace!(
                    stripe_idx = %stripe_idx,
                    stripe_id = ?builder.stripe_id(),
                    "Skipping empty builder"
                );
                continue;
            }

            trace!(
                stripe_idx = %stripe_idx,
                stripe_id = ?builder.stripe_id(),
                size = %builder.size(),
                offset = %builder.stripe_offset(),
                "Flushing stripe to FileStore"
            );

            // Extract values before consuming builder
            let file_id = builder.file_id();
            let stripe_id = builder.stripe_id();
            let stripe_offset = builder.stripe_offset();
            let policy = (*builder.policy()).clone();
            let data = builder.into_data();

            // Write stripe to FileStore
            let metadata = self
                .file_store
                .write_stripe(file_id, stripe_id, stripe_offset, data, policy)
                .await
                .map_err(|e| {
                    Error::DataFailed(format!("Failed to write stripe during full flush: {}", e))
                })?;

            // Add to buffered metadata
            let mut inner = self.inner.lock().expect(
                "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
            );

            // If there's already a stripe at this index, tombstone the old one ONLY if it came from MetadataStore
            // (This happens when partial_flush wrote a smaller version and now we're writing a larger one)
            // We can't tombstone stripes created in session because they don't exist in the database yet
            let old_stripe_id_to_tombstone = inner
                .stripes
                .get(&stripe_idx)
                .filter(|old_stripe| {
                    old_stripe.metadata.stripe_id != stripe_id
                        && old_stripe.origin == StripeOrigin::FromMetadataStore
                })
                .map(|old_stripe| old_stripe.metadata.stripe_id);

            if let Some(old_stripe_id) = old_stripe_id_to_tombstone {
                trace!(
                    old_stripe_id = ?old_stripe_id,
                    stripe_idx = %stripe_idx,
                    new_stripe_id = ?stripe_id,
                    "Tombstoning old committed stripe (replacing with new stripe)"
                );
                inner.tombstones.insert(old_stripe_id);
            } else if inner
                .stripes
                .get(&stripe_idx)
                .map(|s| s.metadata.stripe_id != stripe_id)
                .unwrap_or(false)
            {
                trace!(
                    stripe_idx = %stripe_idx,
                    "Replacing uncommitted stripe (old stripe will be garbage in FileStore)"
                );
                // The old stripe was written to FileStore but never committed to MetadataStore
                // It will remain as orphaned data in FileStore until garbage collection
            }

            inner.stripes.insert(
                stripe_idx,
                BufferedStripeMetadata {
                    metadata,
                    origin: StripeOrigin::CreatedInSession, // New stripe created in this session
                    dirty: false,
                },
            );
            flushed_count += 1;
        }

        trace!(
            flushed_count = %flushed_count,
            "Flushed stripes to FileStore"
        );

        // 2. Build batch of operations for Raft (only for flushed stripes)
        let (operations, flushed_indices) = {
            let mut inner = self.inner.lock().expect(
                "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
            );
            let mut ops = Vec::new();

            // Track which stripe indices were flushed
            let flushed_set: std::collections::HashSet<u32> =
                flushed_indices_vec.iter().cloned().collect();

            trace!(
                flushed_stripes = %flushed_set.len(),
                "Building Raft operations for flushed stripes"
            );

            // Add ONLY stripes that were flushed
            for (stripe_idx, buffered_stripe) in inner.stripes.iter() {
                // Skip stripes that weren't flushed in this call
                if !flushed_set.contains(stripe_idx) {
                    continue;
                }

                if buffered_stripe.origin == StripeOrigin::CreatedInSession {
                    trace!(
                        stripe_idx = %stripe_idx,
                        stripe_id = ?buffered_stripe.metadata.stripe_id,
                        offset = %buffered_stripe.metadata.offset,
                        size = %buffered_stripe.metadata.size,
                        "Adding CREATE operation"
                    );
                    ops.push(StripeOperation::Create {
                        file_id: inner.file_id,
                        stripe: buffered_stripe.metadata.clone(),
                    });
                } else if buffered_stripe.dirty {
                    ops.push(StripeOperation::Update {
                        file_id: inner.file_id,
                        stripe: buffered_stripe.metadata.clone(),
                    });
                }
            }

            // Add tombstones ONLY for flushed stripes
            for (stripe_idx, old_stripe_id) in &inner.tombstones_by_stripe {
                if flushed_set.contains(stripe_idx) {
                    trace!(
                        old_stripe_id = ?old_stripe_id,
                        stripe_idx = %stripe_idx,
                        "Adding DELETE operation for tombstoned stripe"
                    );
                    ops.push(StripeOperation::Delete {
                        stripe_id: *old_stripe_id,
                    });
                }
            }

            // Also add any global tombstones (from old tombstones set)
            for stripe_id in &inner.tombstones {
                ops.push(StripeOperation::Delete {
                    stripe_id: *stripe_id,
                });
            }

            // Update file attributes
            ops.push(StripeOperation::UpdateAttributes {
                file_id: inner.file_id,
                inode: inner.inode,
                attributes: inner.attributes.clone(),
            });

            (ops, flushed_set)
        };

        // 3. Submit atomically via Raft
        self.raft_client
            .propose_stripe_batch(operations)
            .await
            .map_err(|e| Error::Internal(format!("Failed to propose stripe batch: {}", e)))?;

        // 4. Update origin and clear dirty flags ONLY for flushed stripes
        {
            let mut inner = self.inner.lock().expect(
                "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
            );

            // Update origin for flushed stripes
            for stripe_idx in &flushed_indices {
                if let Some(buffered_stripe) = inner.stripes.get_mut(stripe_idx) {
                    buffered_stripe.origin = StripeOrigin::FromMetadataStore;
                    buffered_stripe.dirty = false;
                }
            }

            // Remove tombstones ONLY for flushed stripes
            for stripe_idx in &flushed_indices {
                inner.tombstones_by_stripe.remove(stripe_idx);
            }

            // Clear global tombstones (these were all committed)
            inner.tombstones.clear();

            // Note: builders were already removed when we extracted them for flushing
            // Partial builders (if any) remain in the builders map ONLY if force=false

            // Update memory accounting by recalculating from all remaining builders
            // This works for both force=true (no builders left) and force=false (partial builders remain)
            inner.recalculate_memory_usage();

            // Only fully clean state if this was a forced flush
            if force {
                inner.dirty_metadata = false;
                inner.dirty_data = false;
                inner.writes_since_flush = 0;
            }

            inner.last_flush = Some(Instant::now());
        }

        // Report flush metrics
        if let Some(reporter) = &self.metrics_reporter {
            let latency_secs = start_time.elapsed().as_secs_f64();
            reporter.report_flush(force, latency_secs);
        }

        Ok(())
    }

    /// Inform the handle of an impending operation that may require flush.
    ///
    /// This allows FileSystemService to give BufferedFileHandle a heads-up before
    /// complex operations (like truncate, setattr, rename) so it can choose to flush
    /// for simpler implementation.
    ///
    /// # Arguments
    ///
    /// * `op_type` - Type of operation about to be performed
    pub async fn inform(&self, op_type: OperationType) -> Result<(), Error> {
        // Report inform call to metrics
        if let Some(reporter) = &self.metrics_reporter {
            reporter.report_inform(op_type);
        }

        match op_type {
            OperationType::Truncate => self.full_flush(true).await, // true = flush everything before truncate
            OperationType::Setattr => {
                if self.has_buffered_data() {
                    self.full_flush(true).await // true = flush everything before setattr
                } else {
                    Ok(())
                }
            }
            OperationType::Flush => self.full_flush(false).await, // false = flush complete stripes only
            OperationType::Fsync => self.full_flush(true).await, // true = flush everything including partial
            OperationType::Release => self.full_flush(true).await, // true = flush everything on close
            OperationType::Rename => Ok(()),
            OperationType::Lock => Ok(()),
        }
    }

    /// Check if there's any buffered data.
    fn has_buffered_data(&self) -> bool {
        let inner = self.inner.lock().expect(
            "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
        );
        inner.dirty_data
    }

    /// Check if memory flush is needed.
    fn needs_memory_flush(&self) -> bool {
        let inner = self.inner.lock().expect(
            "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
        );
        // Only count complete stripes toward memory pressure
        // Partial stripes don't trigger flush (prevents data loss)
        inner.complete_stripe_bytes > inner.config.max_memory_bytes
    }

    /// Check if full flush is needed due to time/write count.
    fn needs_full_flush(&self) -> bool {
        let inner = self.inner.lock().expect(
            "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
        );

        if inner.writes_since_flush >= inner.config.max_writes_before_flush {
            return true;
        }

        if let Some(last) = inner.last_flush {
            last.elapsed() > inner.config.max_flush_interval
        } else {
            false
        }
    }

    /// Get current memory usage in bytes.
    /// Returns the sum of complete and partial stripe bytes.
    pub fn memory_bytes(&self) -> usize {
        let inner = self.inner.lock().expect(
            "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
        );
        inner.complete_stripe_bytes + inner.partial_stripe_bytes
    }

    /// Get detailed memory usage breakdown.
    /// Returns (complete_stripe_bytes, partial_stripe_bytes, total_bytes).
    pub fn memory_usage_detailed(&self) -> (usize, usize, usize) {
        let inner = self.inner.lock().expect(
            "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
        );
        let total = inner.complete_stripe_bytes + inner.partial_stripe_bytes;
        (
            inner.complete_stripe_bytes,
            inner.partial_stripe_bytes,
            total,
        )
    }

    /// Get file attributes (may include buffered changes).
    pub fn attributes(&self) -> FileAttr {
        let inner = self.inner.lock().expect(
            "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
        );
        inner.attributes.clone()
    }

    /// Get file metadata by inode (cached, includes buffered changes).
    ///
    /// This provides a fast path for metadata lookups that avoids hitting MetadataStore.
    /// The returned FileAttr includes any buffered size/mtime changes that haven't been flushed yet.
    ///
    /// # Returns
    ///
    /// Cached file attributes for this handle's inode.
    pub fn get_file_by_inode(&self) -> FileAttr {
        // Return cached attributes (same as attributes() but named to match MetadataStore API)
        let inner = self.inner.lock().expect(
            "BufferedFileHandle inner lock poisoned - indicates panic during file operation",
        );
        inner.attributes.clone()
    }

    /// Update cached file attributes.
    ///
    /// This allows external code (like setattr()) to update the cached attributes
    /// when metadata changes occur outside of normal write operations.
    ///
    /// If the file size has decreased, this will also invalidate cached stripe
    /// metadata beyond the new file size to prevent reading stale data.
    ///
    /// # Arguments
    ///
    /// * `attrs` - The new file attributes to cache
    pub fn update_attributes(&self, attrs: FileAttr) {
        let mut inner = self.inner.lock().unwrap();
        let old_size = inner.attributes.size;
        let new_size = attrs.size;

        // Update attributes first
        inner.attributes = attrs;

        // If file size decreased, invalidate cached stripes beyond the new size
        if new_size < old_size {
            let stripe_size = inner.config.max_stripe_size as u64;

            // Calculate the last valid stripe index
            let last_valid_stripe_idx = if new_size == 0 {
                0
            } else {
                ((new_size - 1) / stripe_size) as u32
            };

            // Remove cached stripes beyond the new size
            inner
                .stripes
                .retain(|&stripe_idx, _| stripe_idx <= last_valid_stripe_idx);

            // Remove builders beyond the new size
            inner
                .builders
                .retain(|&stripe_idx, _| stripe_idx <= last_valid_stripe_idx);

            trace!(
                old_size = %old_size,
                new_size = %new_size,
                last_valid_stripe = %last_valid_stripe_idx,
                "Invalidated cached stripes beyond new file size"
            );
        }
    }
}

/// Types of operations that may require pre-flush or cache invalidation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    /// File truncation
    Truncate,
    /// Set file attributes
    Setattr,
    /// File rename
    Rename,
    /// Lock acquisition
    Lock,
    /// Flush file data
    Flush,
    /// Synchronize file data to storage
    Fsync,
    /// Release (close) file handle
    Release,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_store::types::CompressionAlgorithm;
    use crate::file_store::types::{
        ChunkCacheEntry, ChunkData, ChunkId, ChunkMetadata, DiskId, DiskStats,
        Error as FileStoreError, PrefetchPolicy, PrepareVote, RebuildResult, VerificationResult,
    };
    use crate::filesystem_service::types::FileType;
    use crate::metadata_store::{
        types::{IsolationLevel, SynchronousMode},
        Config as MetadataConfig, MetadataStoreFactory,
    };
    use async_trait::async_trait;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use uuid::Uuid;

    // Simple stub FileStore for testing
    struct StubFileStore;

    #[async_trait]
    impl FileStore for StubFileStore {
        fn new(_config: crate::file_store::types::Config) -> Result<Self, FileStoreError> {
            Ok(Self)
        }

        async fn write_stripe(
            &self,
            file_id: FileId,
            stripe_id: StripeId,
            stripe_offset: u64,
            data: Vec<u8>,
            _policy: StoragePolicy,
        ) -> Result<StripeMetadata, FileStoreError> {
            Ok(StripeMetadata {
                stripe_id,
                file_id,
                offset: stripe_offset,
                size: data.len() as u64,
                checksum: 0,
                chunks: vec![],
            })
        }

        async fn read_stripe(
            &self,
            _file_id: FileId,
            _stripe_id: StripeId,
            _chunks: Vec<ChunkMetadata>,
        ) -> Result<Arc<Vec<u8>>, FileStoreError> {
            Ok(Arc::new(vec![]))
        }

        async fn update_stripe_partial(
            &self,
            _file_id: FileId,
            _stripe_id: StripeId,
            _stripe_offset: u64,
            _existing_chunks: Vec<ChunkMetadata>,
            _offset: u64,
            _new_data: Vec<u8>,
            _policy: StoragePolicy,
        ) -> Result<StripeMetadata, FileStoreError> {
            unimplemented!()
        }

        async fn stage_chunk(&self, _chunk_data: ChunkData) -> Result<ChunkId, FileStoreError> {
            unimplemented!()
        }

        async fn activate_chunk(&self, _chunk_id: ChunkId) -> Result<(), FileStoreError> {
            unimplemented!()
        }

        async fn discard_staged_chunk(&self, _chunk_id: ChunkId) -> Result<(), FileStoreError> {
            unimplemented!()
        }

        async fn write_chunk_local(
            &self,
            _chunk_id: ChunkId,
            _chunk_data: ChunkData,
        ) -> Result<(), FileStoreError> {
            unimplemented!()
        }

        async fn read_chunk_local(&self, _chunk_id: ChunkId) -> Result<ChunkData, FileStoreError> {
            unimplemented!()
        }

        async fn verify_chunk(
            &self,
            _chunk_id: ChunkId,
        ) -> Result<VerificationResult, FileStoreError> {
            unimplemented!()
        }

        async fn rebuild_stripe(
            &self,
            _file_id: FileId,
            _stripe_id: StripeId,
        ) -> Result<RebuildResult, FileStoreError> {
            unimplemented!()
        }

        fn get_disk_stats(&self) -> Vec<DiskStats> {
            vec![]
        }

        async fn add_disk(&mut self, _path: PathBuf) -> Result<DiskId, FileStoreError> {
            unimplemented!()
        }

        async fn remove_disk(&mut self, _disk_id: DiskId) -> Result<(), FileStoreError> {
            unimplemented!()
        }

        async fn cache_chunk(
            &self,
            _chunk_id: ChunkId,
            _data: Vec<u8>,
        ) -> Result<(), FileStoreError> {
            unimplemented!()
        }

        async fn get_cached_chunk(
            &self,
            _chunk_id: ChunkId,
        ) -> Result<Option<ChunkCacheEntry>, FileStoreError> {
            Ok(None)
        }

        async fn prefetch_stripe_chunks(
            &self,
            _file_id: FileId,
            _stripe_id: StripeId,
            _policy: PrefetchPolicy,
        ) -> Result<(), FileStoreError> {
            Ok(())
        }
    }

    // Mock RaftClient for tests that always succeeds
    struct MockRaftClient;

    #[async_trait::async_trait]
    impl RaftClient for MockRaftClient {
        async fn propose_stripe_batch(
            &self,
            _operations: Vec<StripeOperation>,
        ) -> Result<(), crate::filesystem_service::types::Error> {
            // Mock implementation - always succeeds
            Ok(())
        }
    }

    async fn create_test_handle() -> BufferedFileHandle {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let metadata_config = MetadataConfig {
            database_path: db_path,
            read_pool_size: 4,
            enable_wal: true,
            cache_size_mb: 64,
            enable_foreign_keys: false,
            synchronous: SynchronousMode::Normal,
            transaction_isolation: IsolationLevel::ReadCommitted,
            enable_prepared_statements: true,
            read_pool_timeout_secs: 30,
        };

        let metadata_store = Arc::new(
            MetadataStoreFactory::create_concrete(metadata_config)
                .await
                .unwrap(),
        );

        let file_store: Arc<dyn FileStore + Send + Sync> = Arc::new(StubFileStore);

        let file_id = FileId::new(Uuid::new_v4());
        let inode = 1;
        let attributes = FileAttr {
            ino: inode,
            size: 0,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        };

        let storage_policy = Arc::new(StoragePolicy::new(
            2,
            1,
            1024 * 1024,
            CompressionAlgorithm::None,
        ));

        let config = BufferedFileHandleConfig::default();

        BufferedFileHandle::new(
            file_id,
            inode,
            attributes,
            storage_policy,
            config,
            metadata_store,
            file_store,
            Arc::new(MockRaftClient),
            None, // No metrics reporter for unit tests
        )
    }

    #[tokio::test]
    async fn test_buffered_file_handle_new() {
        let handle = create_test_handle().await;
        let attrs = handle.attributes();
        assert_eq!(attrs.size, 0);
        assert_eq!(handle.memory_bytes(), 0);
    }

    #[tokio::test]
    async fn test_write_buffers_data() {
        let handle = create_test_handle().await;

        let data = vec![0xAA; 1024];
        let written = handle.write(0, &data).await.unwrap();

        assert_eq!(written, 1024);
        assert_eq!(handle.attributes().size, 1024);
        assert!(handle.memory_bytes() > 0);
    }

    #[tokio::test]
    async fn test_read_buffered_data() {
        let handle = create_test_handle().await;

        // Write some data
        let write_data = vec![0xBB; 512];
        handle.write(0, &write_data).await.unwrap();

        // Read it back
        let read_data = handle.read(0, 512).await.unwrap();
        assert_eq!(read_data.len(), 512);
        assert_eq!(read_data, write_data);
    }

    #[tokio::test]
    async fn test_multiple_writes() {
        let handle = create_test_handle().await;

        // Write in multiple chunks
        handle.write(0, &vec![0x11; 256]).await.unwrap();
        handle.write(256, &vec![0x22; 256]).await.unwrap();
        handle.write(512, &vec![0x33; 256]).await.unwrap();

        assert_eq!(handle.attributes().size, 768);

        // Read back different sections
        let data1 = handle.read(0, 256).await.unwrap();
        assert_eq!(data1, vec![0x11; 256]);

        let data2 = handle.read(256, 256).await.unwrap();
        assert_eq!(data2, vec![0x22; 256]);
    }

    #[tokio::test]
    async fn test_full_flush() {
        let handle = create_test_handle().await;

        // Write some data
        handle.write(0, &vec![0xCC; 1024]).await.unwrap();

        // Explicit flush
        handle.full_flush(true).await.unwrap(); // force=true for test

        // After flush, buffered bytes should be zero
        assert_eq!(handle.memory_bytes(), 0);
    }

    #[tokio::test]
    async fn test_raft_batching() {
        use crate::filesystem_service::raft_commands::{RaftClientImpl, StorageRaftMemberStub};

        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        let metadata_config = MetadataConfig {
            database_path: db_path,
            read_pool_size: 4,
            enable_wal: true,
            cache_size_mb: 64,
            enable_foreign_keys: false,
            synchronous: SynchronousMode::Normal,
            transaction_isolation: IsolationLevel::ReadCommitted,
            enable_prepared_statements: true,
            read_pool_timeout_secs: 30,
        };

        let metadata_store = Arc::new(
            MetadataStoreFactory::create_concrete(metadata_config)
                .await
                .unwrap(),
        );

        // Initialize schema
        metadata_store.initialize_schema().await.unwrap();

        let file_store: Arc<dyn FileStore + Send + Sync> = Arc::new(StubFileStore);

        // Create Raft client
        let raft_stub = Arc::new(StorageRaftMemberStub::new((*metadata_store).clone()));
        let raft_client: Arc<dyn RaftClient> = Arc::new(RaftClientImpl::new(raft_stub));

        let file_id = FileId::new(Uuid::new_v4());
        let inode = 1;
        let attributes = FileAttr {
            ino: inode,
            size: 0,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        };

        let storage_policy = Arc::new(StoragePolicy::new(
            2,
            1,
            1024 * 1024,
            CompressionAlgorithm::None,
        ));

        let config = BufferedFileHandleConfig::default();

        // Create handle WITH Raft client
        let handle = BufferedFileHandle::new(
            file_id,
            inode,
            attributes,
            storage_policy,
            config,
            metadata_store.clone(),
            file_store,
            raft_client,
            None, // No metrics reporter for unit tests
        );

        // Write some data
        handle.write(0, &vec![0xAA; 1024]).await.unwrap();
        handle.write(1024, &vec![0xBB; 1024]).await.unwrap();

        // Full flush should commit to MetadataStore via Raft
        handle.full_flush(true).await.unwrap(); // force=true for test

        // Verify stripes were committed to MetadataStore
        let file_stripes = metadata_store.get_file_stripes(file_id).await.unwrap();
        assert!(
            !file_stripes.is_empty(),
            "Stripes should be committed after full flush"
        );

        println!(
            "✅ Raft batching test passed - {} stripes committed",
            file_stripes.len()
        );
    }
}
