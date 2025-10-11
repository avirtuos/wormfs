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

pub mod types;

use async_trait::async_trait;
use std::path::PathBuf;
use std::time::SystemTime;
pub use types::{
    ChunkData, ChunkHeader, ChunkId, ChunkLocation, CompressionAlgorithm, Config, DiskId,
    DiskStats, ErasureAlgorithm, Error, FileId, NodeId, PrepareVote, RebuildResult, StoragePolicy,
    StripeId, StripeMetadata, TxId, VerificationResult,
};

/// FileStore trait defines the interface for chunk storage and erasure coding.
///
/// Implementations handle the conversion of file data into erasure-coded chunks
/// distributed across the storage cluster.
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
    /// - Metadata lookup fails
    async fn read_stripe(&self, file_id: FileId, stripe_id: StripeId) -> Result<Vec<u8>, Error>;

    // ===== Two-Phase Commit Operations =====

    /// Prepare a chunk locally (Phase 1 of 2PC).
    ///
    /// Writes chunk with state="preparing" and fsyncs to disk. The chunk
    /// is durable but not yet visible to readers.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - Transaction identifier
    /// * `chunk_data` - Chunk data including header and payload
    ///
    /// # Returns
    ///
    /// Vote indicating whether the chunk was successfully prepared (COMMIT)
    /// or preparation failed (ABORT).
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails or I/O errors occur.
    async fn prepare_chunk(&self, tx_id: TxId, chunk_data: ChunkData)
        -> Result<PrepareVote, Error>;

    /// Commit a prepared chunk (Phase 2 of 2PC).
    ///
    /// Transitions chunk from "preparing" to "active" state, making it
    /// visible to readers.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - Transaction identifier
    /// * `chunk_id` - Chunk identifier
    ///
    /// # Errors
    ///
    /// Returns an error if the chunk cannot be found or state transition fails.
    async fn commit_chunk(&self, tx_id: TxId, chunk_id: ChunkId) -> Result<(), Error>;

    /// Abort a prepared chunk (Phase 2 of 2PC).
    ///
    /// Deletes a chunk in "preparing" state, rolling back the transaction.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - Transaction identifier
    /// * `chunk_id` - Chunk identifier
    ///
    /// # Errors
    ///
    /// Returns an error if the chunk cannot be found or deletion fails.
    async fn abort_chunk(&self, tx_id: TxId, chunk_id: ChunkId) -> Result<(), Error>;

    /// Cleanup orphaned preparing chunks (background task).
    ///
    /// Scans for chunks in "preparing" state older than the specified age
    /// and deletes them. This handles transactions that were interrupted
    /// by crashes.
    ///
    /// # Arguments
    ///
    /// * `older_than` - Delete preparing chunks older than this time
    ///
    /// # Returns
    ///
    /// Number of orphaned chunks cleaned up.
    async fn cleanup_orphaned_chunks(&self, older_than: SystemTime) -> Result<u64, Error>;

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
    ///
    /// # Errors
    ///
    /// Returns an error if chunk not found or read fails.
    async fn read_chunk_local(&self, chunk_id: ChunkId) -> Result<ChunkData, Error>;

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
}
