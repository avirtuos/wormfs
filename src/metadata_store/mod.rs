//! # MetadataStore Component
//!
//! MetadataStore is the SQLite-based metadata persistence layer that stores the
//! materialized state of the filesystem after Raft log entries are applied.
//!
//! ## Responsibilities
//!
//! - Storing file metadata (paths, permissions, size, inodes)
//! - Storing chunk locations (which node/disk has which chunks)
//! - Storing stripe mappings (stripe → chunks relationship)
//! - Managing file lock state (active read/write locks)
//! - Providing fast query access to metadata for filesystem operations
//! - Supporting transactional updates from Raft state machine
//! - Creating consistent snapshots for Raft log compaction
//! - Recovering from snapshots during node startup
//!
//! ## Database Schema
//!
//! MetadataStore maintains tables for:
//! - **Files**: File paths, permissions, inodes, sizes
//! - **Stripes**: Stripe metadata and checksums
//! - **Chunks**: Chunk locations and status
//! - **Locks**: Active read/write locks
//! - **Nodes**: Cluster node information
//! - **Disks**: Disk information and capacity
//! - **StoragePolicies**: Erasure coding configurations
//!
//! ## Architecture: Client Pattern
//!
//! MetadataStore uses a client pattern with interior mutability to support:
//! 1. **OpenRaft Compatibility**: Raft can own a cloned instance
//! 2. **Concurrent Access**: Multiple components can safely query metadata
//! 3. **Thread Safety**: RwLock ensures safe concurrent reads and exclusive writes
//!
//! ## Transaction Support
//!
//! MetadataStore provides transactional operations through SQLite's ACID guarantees.
//! All Raft-committed operations are applied atomically to ensure consistency.
//!
//! ## Locking
//!
//! The store manages distributed file locks:
//! - **Read Locks**: Multiple concurrent readers allowed
//! - **Write Locks**: Exclusive access, blocks all other locks
//! - **Lock Expiration**: Automatic cleanup of expired locks
//! - **Lock Extension**: Clients can extend lease duration

pub mod types;

use async_trait::async_trait;
use std::path::Path;
use std::time::SystemTime;
pub use types::{ChunkId, ClientId, Config, DiskId, Error, FileId, FileMetadata, NodeId, StripeId};

/// MetadataStore trait defines the interface for metadata persistence.
///
/// Implementations provide storage and retrieval of filesystem metadata,
/// chunk locations, and lock state.
///
/// Note: This trait cannot be automocked due to the Clone bound requirement.
/// Manual mocking or alternative testing strategies should be used.
#[async_trait]
pub trait MetadataStore: Send + Sync + Clone {
    /// Data types
    type FileRecord: Send + Sync;
    type StripeRecord: Send + Sync;
    type ChunkRecord: Send + Sync;
    type LockRecord: Send + Sync;
    type NodeRecord: Send + Sync;
    type DiskRecord: Send + Sync;

    /// Create a new MetadataStore.
    ///
    /// Returns a cheap-to-clone client handle that wraps the actual database
    /// connection with interior mutability.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration including database path and tuning parameters
    ///
    /// # Returns
    ///
    /// A cloneable MetadataStore handle.
    fn new(config: Config) -> Result<Self, Error>
    where
        Self: Sized;

    /// Initialize database schema.
    ///
    /// Creates all tables, indexes, and constraints. This method is idempotent
    /// and safe to call on existing databases.
    ///
    /// # Errors
    ///
    /// Returns an error if schema creation fails.
    async fn initialize_schema(&self) -> Result<(), Error>;

    // ===== File Operations =====

    /// Create a new file entry.
    ///
    /// # Arguments
    ///
    /// * `path` - File path
    /// * `inode` - Inode number
    /// * `metadata` - File metadata (permissions, size, timestamps, etc.)
    ///
    /// # Returns
    ///
    /// The newly created file's identifier.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - File already exists at path
    /// - Parent directory doesn't exist
    /// - Database constraint violation
    async fn create_file(
        &self,
        path: &Path,
        inode: u64,
        metadata: FileMetadata,
    ) -> Result<FileId, Error>;

    /// Get file metadata by path.
    async fn get_file_by_path(&self, path: &Path) -> Result<Self::FileRecord, Error>;

    /// Get file metadata by inode.
    async fn get_file_by_inode(&self, inode: u64) -> Result<Self::FileRecord, Error>;

    /// Get file metadata by file ID.
    async fn get_file(&self, file_id: FileId) -> Result<Self::FileRecord, Error>;

    /// Update file metadata.
    async fn update_file(&self, file_id: FileId, metadata: FileMetadata) -> Result<(), Error>;

    /// Delete a file.
    async fn delete_file(&self, file_id: FileId) -> Result<(), Error>;

    /// List files in a directory.
    async fn list_directory(&self, path: &Path) -> Result<Vec<Self::FileRecord>, Error>;

    // ===== Stripe Operations =====

    /// Allocate stripes for a file.
    async fn allocate_stripes(
        &self,
        file_id: FileId,
        stripes: Vec<Self::StripeRecord>,
    ) -> Result<(), Error>;

    /// Get stripe by ID.
    async fn get_stripe(&self, stripe_id: StripeId) -> Result<Self::StripeRecord, Error>;

    /// Get all stripes for a file.
    async fn get_file_stripes(&self, file_id: FileId) -> Result<Vec<Self::StripeRecord>, Error>;

    /// Get stripe at specific offset in file.
    async fn get_stripe_at_offset(
        &self,
        file_id: FileId,
        offset: u64,
    ) -> Result<Self::StripeRecord, Error>;

    // ===== Chunk Operations =====

    /// Allocate chunks for a stripe.
    async fn allocate_chunks(
        &self,
        stripe_id: StripeId,
        chunks: Vec<Self::ChunkRecord>,
    ) -> Result<(), Error>;

    /// Get chunk by ID.
    async fn get_chunk(&self, chunk_id: ChunkId) -> Result<Self::ChunkRecord, Error>;

    /// Get all chunks for a stripe.
    async fn get_stripe_chunks(&self, stripe_id: StripeId)
        -> Result<Vec<Self::ChunkRecord>, Error>;

    /// Update chunk location.
    async fn update_chunk_location(
        &self,
        chunk_id: ChunkId,
        node_id: NodeId,
        disk_id: DiskId,
    ) -> Result<(), Error>;

    /// Mark chunk as corrupt.
    async fn mark_chunk_corrupt(&self, chunk_id: ChunkId) -> Result<(), Error>;

    /// Update chunk verification time.
    async fn update_chunk_verification(
        &self,
        chunk_id: ChunkId,
        verified_at: SystemTime,
    ) -> Result<(), Error>;

    // ===== Lock Operations =====

    /// Acquire a read lock on a file.
    async fn acquire_read_lock(
        &self,
        file_id: FileId,
        client_id: ClientId,
        expires_at: SystemTime,
    ) -> Result<u64, Error>;

    /// Acquire a write lock on a file.
    async fn acquire_write_lock(
        &self,
        file_id: FileId,
        client_id: ClientId,
        expires_at: SystemTime,
    ) -> Result<u64, Error>;

    /// Release a lock.
    async fn release_lock(&self, file_id: FileId, client_id: ClientId) -> Result<(), Error>;

    /// Extend lock expiration time.
    async fn extend_lock(
        &self,
        file_id: FileId,
        client_id: ClientId,
        new_expiry: SystemTime,
    ) -> Result<(), Error>;

    /// Get active locks for a file.
    async fn get_file_locks(&self, file_id: FileId) -> Result<Vec<Self::LockRecord>, Error>;

    /// Clean up expired locks.
    async fn cleanup_expired_locks(&self) -> Result<u64, Error>;

    // ===== Snapshot Operations =====

    /// Create a consistent snapshot of the database.
    async fn create_snapshot(&self, snapshot_path: &Path) -> Result<(), Error>;

    /// Restore from a snapshot.
    async fn restore_from_snapshot(&self, snapshot_path: &Path) -> Result<(), Error>;
}
