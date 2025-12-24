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
//! ## Architecture: Read Pool + Single Writer Pattern
//!
//! MetadataStore uses a hybrid connection strategy for optimal performance:
//!
//! ### Connection Management
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │         MetadataStore (Cloneable Handle)            │
//! ├─────────────────────────────────────────────────────┤
//! │                                                       │
//! │  Write Path (Raft-aligned):                         │
//! │  ┌──────────────────────────────────────────┐       │
//! │  │ write_conn: Mutex<Connection>             │       │
//! │  │  • Single connection for all writes       │       │
//! │  │  • Serialized through Raft leader         │       │
//! │  │  • BEGIN IMMEDIATE transactions           │       │
//! │  └──────────────────────────────────────────┘       │
//! │                                                       │
//! │  Read Path (Concurrent):                            │
//! │  ┌──────────────────────────────────────────┐       │
//! │  │ read_pool: Pool<SqliteConnection>         │       │
//! │  │  • 4-8 connections (configurable)          │       │
//! │  │  • True concurrent reads                   │       │
//! │  │  • No Rust-level locking                   │       │
//! │  │  • WAL mode allows reads while writing     │       │
//! │  └──────────────────────────────────────────┘       │
//! │                                                       │
//! │  Cache (Optional):                                   │
//! │  ┌──────────────────────────────────────────┐       │
//! │  │ cache: RwLock<LruCache<...>>              │       │
//! │  │  • Hot metadata (file attrs, dirs)         │       │
//! │  │  • Write-through for consistency           │       │
//! │  │  • TTL-based invalidation                  │       │
//! │  └──────────────────────────────────────────┘       │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! ### Key Benefits
//!
//! 1. **True Read Concurrency**: Multiple threads read simultaneously without blocking
//! 2. **Raft-Aligned Writes**: Single writer matches Raft's leader-only write model
//! 3. **WAL Mode Utilization**: Fully leverages SQLite's concurrent reader support
//! 4. **FUSE Optimized**: Read-heavy workloads get maximum throughput
//! 5. **Bounded Resources**: Pool size limits connection overhead
//! 6. **OpenRaft Compatible**: Cloneable handle allows Raft to "own" an instance
//!
//! ### Implementation Structure
//!
//! ```ignore
//! struct MetadataStoreInner {
//!     write_conn: Mutex<rusqlite::Connection>,
//!     read_pool: Pool<SqliteConnectionManager>,
//!     cache: RwLock<LruCache<CacheKey, CachedValue>>,
//!     config: Config,
//! }
//!
//! #[derive(Clone)]
//! pub struct MetadataStoreImpl {
//!     inner: Arc<MetadataStoreInner>,
//! }
//! ```
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

pub mod cache;
pub mod factory;
pub mod implementation;
pub mod types;

#[cfg(test)]
mod tests;

// Re-export the concrete implementation
pub use implementation::MetadataStoreImpl;

use async_trait::async_trait;
use std::path::Path;
use std::time::SystemTime;

pub use factory::MetadataStoreFactory;
pub use types::{
    ChunkId, ChunkRecord, ChunkStatus, ClientId, Config, DiskId, DiskRecord, DiskStatus, Error,
    FileId, FileMetadata, FileRecord, FileType, LockRecord, LockType, NodeId, NodeRecord,
    NodeStatus, StripeId, StripeRecord,
};

/// MetadataStore trait defines the interface for metadata persistence.
///
/// Implementations provide storage and retrieval of filesystem metadata,
/// chunk locations, and lock state.
///
/// Note: This trait cannot be automocked due to the Clone bound requirement.
/// Manual mocking or alternative testing strategies should be used.
#[async_trait]
pub trait MetadataStore: Send + Sync + Clone {
    /// Initialize database schema.
    ///
    /// Creates all tables, indexes, and constraints. This method is idempotent
    /// and safe to call on existing databases.
    ///
    /// # Errors
    ///
    /// Returns an error if schema creation fails.
    async fn initialize_schema(&self) -> Result<(), Error>;

    /// Initialize default node and disks for Phase 1 single-node operation.
    ///
    /// This method ensures that the database has the necessary node and disk records
    /// for storing chunk metadata. It creates:
    /// - A single node with the specified node_id representing the local node
    /// - Disk records for each configured disk path
    ///
    /// This method is idempotent and safe to call multiple times.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The node ID to use for this storage node
    /// * `disk_paths` - List of disk paths to register
    ///
    /// # Errors
    ///
    /// Returns an error if node/disk initialization fails.
    async fn initialize_node_and_disks(
        &self,
        node_id: u64,
        disk_paths: &[std::path::PathBuf],
    ) -> Result<(), Error>;

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
    /// # Parameters
    ///
    /// * `file_id` - Caller-provided unique identifier (use `FileId::generate()`)
    /// * `path` - File path
    /// * `inode` - Inode number (from inode reservation system)
    /// * `metadata` - File metadata
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - File already exists at path or file_id
    /// - Parent directory doesn't exist
    /// - Database constraint violation
    async fn create_file(
        &self,
        file_id: FileId,
        path: &Path,
        inode: u64,
        metadata: FileMetadata,
    ) -> Result<(), Error>;

    /// Get file metadata by path.
    async fn get_file_by_path(&self, path: &Path) -> Result<FileRecord, Error>;

    /// Get file metadata by inode.
    async fn get_file_by_inode(&self, inode: u64) -> Result<FileRecord, Error>;

    /// Get file metadata by file ID.
    async fn get_file(&self, file_id: FileId) -> Result<FileRecord, Error>;

    /// Update file metadata.
    async fn update_file(&self, file_id: FileId, metadata: FileMetadata) -> Result<(), Error>;

    /// Delete a file.
    async fn delete_file(&self, file_id: FileId) -> Result<(), Error>;

    /// List files in a directory.
    async fn list_directory(&self, path: &Path) -> Result<Vec<FileRecord>, Error>;

    // ===== Stripe Operations =====

    /// Allocate stripes for a file.
    async fn allocate_stripes(
        &self,
        file_id: FileId,
        stripes: Vec<StripeRecord>,
    ) -> Result<(), Error>;

    /// Get stripe by ID.
    async fn get_stripe(&self, stripe_id: StripeId) -> Result<StripeRecord, Error>;

    /// Get all stripes for a file.
    async fn get_file_stripes(&self, file_id: FileId) -> Result<Vec<StripeRecord>, Error>;

    /// Get stripe at specific offset in file.
    async fn get_stripe_at_offset(
        &self,
        file_id: FileId,
        offset: u64,
    ) -> Result<StripeRecord, Error>;

    /// Delete a stripe and all its chunks.
    async fn delete_stripe(&self, stripe_id: StripeId) -> Result<(), Error>;

    // ===== Chunk Operations =====

    /// Allocate chunks for a stripe.
    async fn allocate_chunks(
        &self,
        stripe_id: StripeId,
        chunks: Vec<ChunkRecord>,
    ) -> Result<(), Error>;

    /// Get chunk by ID.
    async fn get_chunk(&self, chunk_id: ChunkId) -> Result<ChunkRecord, Error>;

    /// Get all chunks for a stripe.
    async fn get_stripe_chunks(&self, stripe_id: StripeId) -> Result<Vec<ChunkRecord>, Error>;

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
        node_id: u64,
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
    async fn get_file_locks(&self, file_id: FileId) -> Result<Vec<LockRecord>, Error>;

    /// Clean up expired locks.
    ///
    /// **IMPORTANT**: This method must be called periodically by the application to prevent
    /// unbounded growth of the locks table. Expired locks are not automatically removed.
    ///
    /// Recommended frequency: Every 5-60 minutes depending on lock duration and system load.
    ///
    /// Returns the number of locks removed.
    async fn cleanup_expired_locks(&self) -> Result<u64, Error>;

    // ===== Inode Reservation Operations =====

    /// Reserve an available inode for future use.
    ///
    /// Reserves an inode from the inode pool with a 1-hour expiration time.
    /// This is typically called by StorageRaftMember when preparing to create
    /// a new file, before the actual Raft transaction is committed.
    ///
    /// The reservation ensures that multiple concurrent file creation operations
    /// don't try to use the same inode. If the reservation expires (after 1 hour)
    /// without being confirmed, the inode becomes available for reuse.
    ///
    /// # Returns
    ///
    /// The reserved inode number.
    ///
    /// # Inode Limits
    ///
    /// While the API uses u64 for inodes, SQLite's INTEGER type is signed i64.
    /// The practical maximum is 2^63-1 (9,223,372,036,854,775,807) inodes.
    /// This is effectively unlimited for any real-world filesystem.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Inode space is exhausted (`Error::InodeSpaceExhausted`)
    /// - Database operation fails
    async fn reserve_inode(&self) -> Result<u64, Error>;

    /// Confirm an inode reservation and mark it as used.
    ///
    /// Called when a Raft transaction successfully commits and the inode
    /// is permanently assigned to a file. This marks the inode as used
    /// and prevents it from being reused.
    ///
    /// # Arguments
    ///
    /// * `inode` - The inode number to confirm
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Inode is not reserved (`Error::InodeNotReserved`)
    /// - Inode reservation has expired (`Error::InodeReservationExpired`)
    /// - Inode is already in use (`Error::InodeInUse`)
    async fn confirm_inode(&self, inode: u64) -> Result<(), Error>;

    /// Release an inode reservation.
    ///
    /// Called when a Raft transaction fails or is aborted, making the
    /// reserved inode available for reuse by other operations.
    ///
    /// # Arguments
    ///
    /// * `inode` - The inode number to release
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Inode is not reserved (`Error::InodeNotReserved`)
    /// - Database operation fails
    async fn release_inode(&self, inode: u64) -> Result<(), Error>;

    /// Clean up expired inode reservations.
    ///
    /// Scans for inode reservations older than 1 hour and releases them
    /// back to the available pool. This is typically called periodically
    /// by a background task.
    ///
    /// # Returns
    ///
    /// Number of expired reservations that were cleaned up.
    ///
    /// # Errors
    ///
    /// Returns an error if database operation fails.
    ///
    /// # TODO: Background Maintenance Task (Phase 2)
    ///
    /// This method needs to be called periodically by a background maintenance task.
    /// The task should:
    /// - Run every 10-15 minutes
    /// - Call `cleanup_expired_inode_reservations()` to release stale inode reservations
    /// - Call `cleanup_expired_locks()` to release expired file locks
    /// - Log statistics about cleaned up resources
    /// - Be implemented as part of StorageWatchdog or a dedicated MaintenanceService
    async fn cleanup_expired_inode_reservations(&self) -> Result<u64, Error>;

    // ===== Snapshot Operations =====

    /// Create a consistent snapshot of the database.
    async fn create_snapshot(&self, snapshot_path: &Path) -> Result<(), Error>;

    /// Restore from a snapshot.
    async fn restore_from_snapshot(&self, snapshot_path: &Path) -> Result<(), Error>;
}
