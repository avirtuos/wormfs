//! # FileStore Component
//!
//! FileStore manages erasure coding, chunk storage, and chunk placement for file data.
//!
//! ## Responsibilities
//!
//! - Applying Reed-Solomon erasure coding to file stripes
//! - Coordinating chunk placement across storage nodes and disks
//! - Handling chunk read and write operations
//! - Managing chunk verification and integrity checking
//! - Enforcing storage policies (stripe size, data/parity shards)
//! - Implementing chunk placement rules (blast radius limitations)
//! - Providing chunk reconstruction from available shards
//! - Managing local chunk storage on backing filesystems
//!
//! ## Stripe Processing
//!
//! FileStore processes file data in stripes:
//!
//! ### Write Pipeline
//! 1. Receive stripe data (up to stripe_size bytes)
//! 2. Apply Reed-Solomon encoding
//! 3. Generate k data shards and m parity shards
//! 4. Calculate checksums (stripe + individual chunks)
//! 5. Select chunk placement (nodes + disks)
//! 6. Create chunk headers with metadata
//! 7. Write chunks to storage nodes
//! 8. Update metadata via Raft
//!
//! ### Read Pipeline
//! 1. Query metadata for chunk locations
//! 2. Request chunks from storage nodes
//! 3. Verify chunk checksums
//! 4. If missing/corrupt: reconstruct from k available shards
//! 5. Apply Reed-Solomon decoding
//! 6. Verify stripe checksum
//! 7. Return stripe data
//!
//! ## Two-Phase Commit Support
//!
//! FileStore participates in distributed transactions coordinated by StorageRaftMember:
//!
//! ### Phase 1: PREPARE
//! - `prepare_chunk()` writes chunk with state="preparing" and fsyncs
//! - Returns vote (COMMIT or ABORT) based on success
//! - Chunk is durable but not yet visible to readers
//!
//! ### Phase 2: COMMIT/ABORT
//! - `commit_chunk()` transitions chunk to state="active" (visible)
//! - `abort_chunk()` deletes the preparing chunk
//! - Orphan cleanup handles crashed transactions
//!
//! ## Chunk File Format
//!
//! Each chunk is stored with a self-describing header:
//! - Magic bytes ("WORM")
//! - Format version
//! - Chunk and stripe checksums
//! - Chunk ID, Stripe ID, File ID
//! - Erasure coding parameters
//! - Chunk state (preparing/active)
//! - Transaction ID (if preparing)
//!
//! ## Storage Organization
//!
//! Chunks are organized in folders using hash bucketing:
//! ```text
//! /data/disks/disk1/
//!   ├── 1/abc123def45/chunk_0
//!   ├── 2/xyz789ghi01/chunk_3
//!   └── ...
//! ```

pub mod chunk_client;
pub mod erasure_coding;
pub mod implementation;
pub mod stripe_builder;
pub mod types;

use async_trait::async_trait;
pub use chunk_client::{ChunkClient, ChunkClientConfig, ChunkClientPool};
pub use implementation::FileStoreImpl;
pub use placement::{ChunkPlacement, PlacementConfig, PlacementEngine};
use std::path::{Path, PathBuf};
use std::sync::Arc;
pub use stripe_builder::StripeBuilder;
pub use types::{
    ChunkCacheEntry, ChunkData, ChunkHeader, ChunkId, ChunkMetadata, CompressionAlgorithm, Config,
    DiskId, DiskStats, ErasureAlgorithm, Error, FileId, NodeId, PrefetchPolicy, PrepareVote,
    RebuildResult, StoragePolicy, StripeId, StripeMetadata, TxId, VerificationResult,
};

/// FileStore trait defines the interface for chunk storage and erasure coding.
///
/// Implementations handle the conversion of file data into erasure-coded chunks
/// distributed across the storage cluster.
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock)]
#[async_trait]
pub trait FileStore: Send + Sync {
    /// Create a new FileStore instance.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration including disk paths and storage policies
    ///
    /// # Returns
    ///
    /// A new FileStore instance ready to handle chunk operations.
    fn new(config: Config) -> Result<Self, Error>
    where
        Self: Sized;

    /// Write a stripe to storage (applies erasure coding and distributes chunks).
    ///
    /// This is the high-level write method that handles the complete stripe
    /// processing pipeline.
    ///
    /// # Arguments
    ///
    /// * `file_id` - File this stripe belongs to
    /// * `stripe_id` - Unique stripe identifier
    /// * `stripe_offset` - Byte offset where this stripe starts in the file
    /// * `data` - Raw stripe data (up to stripe_size bytes)
    /// * `policy` - Storage policy (data shards, parity shards, stripe size)
    ///
    /// # Returns
    ///
    /// Metadata describing the created stripe and chunk locations.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Erasure encoding fails
    /// - Insufficient storage nodes/disks available
    /// - Chunk write operations fail
    async fn write_stripe(
        &self,
        file_id: FileId,
        stripe_id: StripeId,
        stripe_offset: u64,
        data: Vec<u8>,
        policy: StoragePolicy,
    ) -> Result<StripeMetadata, Error>;

    /// Read a stripe from storage (retrieves and reconstructs if necessary).
    ///
    /// This method handles the complete stripe read pipeline including
    /// reconstruction from available chunks if some are missing or corrupt.
    ///
    /// # Arguments
    ///
    /// * `file_id` - File this stripe belongs to
    /// * `stripe_id` - Stripe identifier
    /// * `chunks` - Chunk location metadata provided by caller (from MetadataStore query)
    ///
    /// # Returns
    ///
    /// The reconstructed stripe data.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Insufficient chunks available for reconstruction
    /// - All chunks are corrupt or missing
    /// - Disk not found for chunk location
    async fn read_stripe(
        &self,
        file_id: FileId,
        stripe_id: StripeId,
        chunks: Vec<ChunkMetadata>,
    ) -> Result<Arc<Vec<u8>>, Error>;

    /// Update a portion of an existing stripe.
    ///
    /// This performs a read-modify-write operation:
    /// 1. Read existing stripe data using provided chunk locations
    /// 2. Apply modifications at specified offset
    /// 3. Re-encode stripe with new data
    /// 4. Write NEW chunks with NEW ChunkIds
    /// 5. Return StripeMetadata with new ChunkIds
    ///
    /// The old chunks remain on disk for transaction safety.
    /// Caller must persist new metadata via Raft, then trigger
    /// cleanup of orphaned chunks.
    ///
    /// # Arguments
    ///
    /// * `file_id` - File this stripe belongs to
    /// * `stripe_id` - Stripe identifier (reused for new version)
    /// * `stripe_offset` - Byte offset where this stripe starts in the file
    /// * `existing_chunks` - Current chunk locations from metadata
    /// * `offset` - Byte offset **within the stripe** where update begins
    /// * `new_data` - Data to write at offset
    /// * `policy` - Storage policy for re-encoding
    ///
    /// # Returns
    ///
    /// StripeMetadata containing NEW ChunkIds for the updated stripe.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Reading existing stripe fails
    /// - Re-encoding fails
    /// - Writing new chunks fails
    async fn update_stripe_partial(
        &self,
        file_id: FileId,
        stripe_id: StripeId,
        stripe_offset: u64,
        existing_chunks: Vec<ChunkMetadata>,
        offset: u64,
        new_data: Vec<u8>,
        policy: StoragePolicy,
    ) -> Result<StripeMetadata, Error>;

    // ===== Two-Phase Commit Operations =====

    /// Stage a chunk on local disk without metadata tracking.
    ///
    /// Writes chunk data to disk in a "staged" state. The chunk is not visible
    /// in the filesystem and has no metadata record. Only the Leader tracks
    /// staged chunks in memory for the duration of the transaction.
    ///
    /// Staged chunks older than 1 hour are considered orphaned and will be
    /// cleaned up by StorageWatchdog.
    ///
    /// # Arguments
    ///
    /// * `chunk_data` - Chunk data including header and payload
    ///
    /// # Returns
    ///
    /// The ChunkId of the staged chunk.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Disk is full or unavailable
    /// - I/O errors occur during write
    /// - Chunk validation fails
    async fn stage_chunk(&self, chunk_data: ChunkData) -> Result<ChunkId, Error>;

    /// Activate a staged chunk after metadata commit.
    ///
    /// Transitions a staged chunk to "active" state, making it visible in
    /// the filesystem. This is called after Raft successfully commits the
    /// metadata operations that reference this chunk.
    ///
    /// # Arguments
    ///
    /// * `chunk_id` - Identifier of the staged chunk to activate
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Chunk is not found (may have been cleaned up)
    /// - State transition fails
    /// - I/O errors occur
    async fn activate_chunk(&self, chunk_id: ChunkId) -> Result<(), Error>;

    /// Discard a staged chunk after transaction failure.
    ///
    /// Deletes a staged chunk when the metadata transaction fails or is aborted.
    /// The chunk data is permanently removed from disk.
    ///
    /// # Arguments
    ///
    /// * `chunk_id` - Identifier of the staged chunk to discard
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Chunk is not found (may have already been cleaned up)
    /// - Deletion fails
    async fn discard_staged_chunk(&self, chunk_id: ChunkId) -> Result<(), Error>;

    // ===== Local Chunk Operations =====

    /// Write a chunk locally (called by remote nodes).
    ///
    /// # Arguments
    ///
    /// * `chunk_id` - Chunk identifier
    /// * `chunk_data` - Chunk data including header
    ///
    /// # Errors
    ///
    /// Returns an error if write fails or disk is full.
    async fn write_chunk_local(
        &self,
        chunk_id: ChunkId,
        chunk_data: ChunkData,
    ) -> Result<(), Error>;

    /// Read a chunk locally (called by remote nodes).
    ///
    /// # Arguments
    ///
    /// * `chunk_id` - Chunk identifier
    ///
    /// # Returns
    ///
    /// The chunk data including header.
    async fn read_chunk_local(&self, chunk_id: ChunkId) -> Result<ChunkData, Error>;

    /// Get the filesystem path for a disk by its ID.
    ///
    /// # Arguments
    ///
    /// * `disk_id` - The disk identifier
    ///
    /// # Returns
    ///
    /// The filesystem path to the disk
    ///
    /// # Errors
    ///
    /// Returns an error if disk not found.
    async fn get_disk_path(&self, disk_id: DiskId) -> Result<PathBuf, Error>;

    /// Read a chunk from disk given full path components.
    ///
    /// # Arguments
    ///
    /// * `disk_path` - Filesystem path to the disk
    /// * `file_id` - File identifier
    /// * `stripe_id` - Stripe identifier
    /// * `chunk_id` - Chunk identifier
    ///
    /// # Returns
    ///
    /// The chunk data including header
    ///
    /// # Errors
    ///
    /// Returns an error if chunk not found or read fails.
    async fn read_chunk_from_disk(
        &self,
        disk_path: &Path,
        file_id: FileId,
        stripe_id: StripeId,
        chunk_id: ChunkId,
    ) -> Result<ChunkData, Error>;

    /// Verify chunk integrity (checksum validation).
    ///
    /// # Arguments
    ///
    /// * `chunk_id` - Chunk identifier
    ///
    /// # Returns
    ///
    /// Verification result indicating if chunk is valid, corrupt, or missing.
    ///
    /// # Errors
    ///
    /// Returns an error if verification cannot be performed.
    async fn verify_chunk(&self, chunk_id: ChunkId) -> Result<VerificationResult, Error>;

    /// Rebuild corrupt or missing chunks for a stripe.
    ///
    /// This method reconstructs missing/corrupt chunks using available chunks
    /// and erasure coding, then writes the reconstructed chunks back to storage.
    ///
    /// # Arguments
    ///
    /// * `file_id` - File identifier
    /// * `stripe_id` - Stripe identifier
    ///
    /// # Returns
    ///
    /// Result describing which chunks were rebuilt.
    ///
    /// # Errors
    ///
    /// Returns an error if insufficient chunks available or rebuild fails.
    async fn rebuild_stripe(
        &self,
        file_id: FileId,
        stripe_id: StripeId,
    ) -> Result<RebuildResult, Error>;

    /// Get local disk statistics.
    ///
    /// # Returns
    ///
    /// Statistics for all locally managed disks (space usage, chunk counts, etc.).
    fn get_disk_stats(&self) -> Vec<DiskStats>;

    /// Add a new disk to local storage.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the disk mount point
    ///
    /// # Returns
    ///
    /// Identifier for the newly added disk.
    ///
    /// # Errors
    ///
    /// Returns an error if disk cannot be initialized or is already managed.
    async fn add_disk(&mut self, path: PathBuf) -> Result<DiskId, Error>;

    /// Remove a disk from local storage.
    ///
    /// This triggers migration of chunks to other disks before removal.
    ///
    /// # Arguments
    ///
    /// * `disk_id` - Disk identifier
    ///
    /// # Errors
    ///
    /// Returns an error if disk not found or has unmigrated chunks.
    async fn remove_disk(&mut self, disk_id: DiskId) -> Result<(), Error>;

    // === Chunk Caching Methods ===

    /// Cache a chunk to disk for later retrieval.
    ///
    /// Chunks are cached in their encoded form (not decoded) to save space.
    /// Cached chunks are used to speed up reads and prefetching.
    ///
    /// # Arguments
    ///
    /// * `chunk_id` - Chunk identifier
    /// * `data` - Encoded chunk data
    ///
    /// # Errors
    ///
    /// Returns an error if caching fails due to disk I/O issues.
    async fn cache_chunk(&self, chunk_id: ChunkId, data: Vec<u8>) -> Result<(), Error>;

    /// Retrieve a cached chunk from disk.
    ///
    /// # Arguments
    ///
    /// * `chunk_id` - Chunk identifier
    ///
    /// # Returns
    ///
    /// Cached chunk entry if found, None otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if cache lookup fails.
    async fn get_cached_chunk(&self, chunk_id: ChunkId) -> Result<Option<ChunkCacheEntry>, Error>;

    /// Prefetch chunks for a stripe without decoding them.
    ///
    /// This method fetches chunks and stores them in the cache for later use.
    /// Chunks are not decoded until actually needed for a read operation.
    ///
    /// # Arguments
    ///
    /// * `file_id` - File identifier
    /// * `stripe_id` - Stripe identifier
    /// * `policy` - Prefetch policy (e.g., next stripe, lookahead)
    ///
    /// # Errors
    ///
    /// Returns an error if prefetching fails. Errors are non-fatal and
    /// should not interrupt normal operations.
    async fn prefetch_stripe_chunks(
        &self,
        file_id: FileId,
        stripe_id: StripeId,
        policy: PrefetchPolicy,
    ) -> Result<(), Error>;
}

// =============================================================================
// Concrete Implementation (Placeholder)
// =============================================================================

// NOTE: FileStoreImpl will be added when we implement the FileStore component.
// For now, the trait definition provides the interface contract.
