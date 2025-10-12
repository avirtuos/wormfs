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
pub use types::{
    ChunkData, ChunkHeader, ChunkId, ChunkLocation, CompressionAlgorithm, Config, DiskId,
    DiskStats, ErasureAlgorithm, Error, FileId, NodeId, PrepareVote, RebuildResult, StoragePolicy,
    StripeId, StripeMetadata, TxId, VerificationResult,
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

/// FuseFileSystem trait defines the interface for FUSE filesystem operations over gRPC.
///
/// This trait acts as an adapter between high-level FUSE filesystem operations and
/// WormFS's internal stripe/chunk/metadata architecture. Implementations translate
/// byte-oriented file operations into stripe-aligned chunk operations while maintaining
/// POSIX semantics.
///
/// ## Responsibilities
///
/// - Translating FUSE file operations to stripe/chunk operations
/// - Managing write buffering for stripe alignment
/// - Coordinating metadata updates through StorageRaftMember
/// - Handling partial reads/writes efficiently
/// - Supporting gRPC streaming for large file transfers
/// - Managing file handles with caching context
///
/// ## Architecture
///
/// ```text
/// FUSE Client (over gRPC)
///       │
///       ▼
/// FuseFileSystem (adapter layer)
///       │
///       ├──► FileStore (stripe/chunk operations)
///       ├──► MetadataStore (file metadata)
///       └──► StorageRaftMember (metadata writes)
/// ```
///
/// ## Write Buffering
///
/// Writes are buffered to align with stripe boundaries:
/// 1. Buffer partial stripe writes in memory
/// 2. When stripe is complete or flush requested, convert to stripe operation
/// 3. Apply erasure coding and distribute chunks
/// 4. Update metadata via Raft consensus
///
/// ## Read Optimization
///
/// Reads are optimized for common access patterns:
/// 1. Sequential reads prefetch next stripe
/// 2. Random reads cache recently accessed stripes
/// 3. Partial stripe reads only retrieve necessary chunks
///
/// ## gRPC Considerations
///
/// - Streaming support for large file transfers
/// - Efficient chunking for network transmission
/// - Client-side caching coordination
/// - Connection state management
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock(
    type FileHandle = u64;
    type DirHandle = u64;
))]
#[async_trait]
pub trait FuseFileSystem: Send + Sync {
    /// File handle type for open files
    type FileHandle: Send + Sync;
    /// Directory handle type for open directories
    type DirHandle: Send + Sync;

    /// Create a new file.
    ///
    /// Creates a new file with the specified attributes and returns a file handle.
    /// The file is initially empty with no stripes allocated.
    ///
    /// # Arguments
    ///
    /// * `parent_inode` - Parent directory inode
    /// * `name` - File name
    /// * `mode` - POSIX permissions (e.g., 0644)
    /// * `uid` - Owner user ID
    /// * `gid` - Owner group ID
    /// * `flags` - Open flags (O_RDWR, O_CREAT, etc.)
    ///
    /// # Returns
    ///
    /// A tuple of (file_handle, file_id, attributes) for the created file.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Parent directory doesn't exist
    /// - File already exists
    /// - Permission denied
    /// - Metadata commit fails
    async fn fuse_create(
        &self,
        parent_inode: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        flags: u32,
    ) -> Result<
        (
            Self::FileHandle,
            FileId,
            crate::filesystem_service::FileAttr,
        ),
        Error,
    >;

    /// Open an existing file.
    ///
    /// Opens an existing file and returns a file handle with caching context.
    /// Initializes read/write buffers based on access mode.
    ///
    /// # Arguments
    ///
    /// * `inode` - File inode
    /// * `flags` - Open flags (O_RDONLY, O_RDWR, O_APPEND, etc.)
    ///
    /// # Returns
    ///
    /// A tuple of (file_handle, file_id, attributes) for the opened file.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - File not found
    /// - Permission denied
    /// - Invalid open flags
    async fn fuse_open(
        &self,
        inode: u64,
        flags: u32,
    ) -> Result<
        (
            Self::FileHandle,
            FileId,
            crate::filesystem_service::FileAttr,
        ),
        Error,
    >;

    /// Read data from a file.
    ///
    /// Reads data from the specified byte range, handling stripe alignment internally.
    /// May read partial stripes and cache them for subsequent reads.
    ///
    /// # Arguments
    ///
    /// * `handle` - File handle from fuse_open()
    /// * `offset` - Byte offset in file
    /// * `size` - Number of bytes to read
    ///
    /// # Returns
    ///
    /// File data starting at the specified offset.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Handle is invalid
    /// - Read beyond file size
    /// - Chunk reconstruction fails
    async fn fuse_read(
        &self,
        handle: &Self::FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, Error>;

    /// Write data to a file.
    ///
    /// Writes data at the specified offset, buffering partial stripe writes.
    /// Full stripes are immediately converted to chunk operations.
    ///
    /// # Arguments
    ///
    /// * `handle` - File handle from fuse_open()
    /// * `offset` - Byte offset in file
    /// * `data` - Data to write
    ///
    /// # Returns
    ///
    /// Number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Handle is invalid or not opened for writing
    /// - Insufficient storage space
    /// - Stripe operation fails
    /// - Metadata commit fails
    async fn fuse_write(
        &self,
        handle: &mut Self::FileHandle,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<u32, Error>;

    /// Flush buffered writes.
    ///
    /// Forces all buffered partial stripe writes to be completed and persisted.
    /// This is called when the application calls flush() or before file release.
    ///
    /// # Arguments
    ///
    /// * `handle` - File handle from fuse_open()
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Handle is invalid
    /// - Stripe write fails
    /// - Metadata commit fails
    async fn fuse_flush(&self, handle: &mut Self::FileHandle) -> Result<(), Error>;

    /// Synchronize file data and metadata.
    ///
    /// Forces all buffered writes to disk and ensures metadata is persisted.
    /// Stronger guarantee than flush() - ensures durability.
    ///
    /// # Arguments
    ///
    /// * `handle` - File handle from fuse_open()
    /// * `datasync` - If true, only sync data; if false, sync data and metadata
    ///
    /// # Errors
    ///
    /// Returns an error if synchronization fails.
    async fn fuse_fsync(&self, handle: &mut Self::FileHandle, datasync: bool) -> Result<(), Error>;

    /// Truncate or extend a file.
    ///
    /// Changes the file size, allocating new stripes or deallocating old ones as needed.
    /// Handles partial stripe modifications efficiently.
    ///
    /// # Arguments
    ///
    /// * `inode` - File inode
    /// * `new_size` - New file size in bytes
    ///
    /// # Returns
    ///
    /// Updated file attributes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - File not found
    /// - Permission denied
    /// - Insufficient storage space (for extension)
    /// - Metadata commit fails
    async fn fuse_truncate(
        &self,
        inode: u64,
        new_size: u64,
    ) -> Result<crate::filesystem_service::FileAttr, Error>;

    /// Release (close) a file.
    ///
    /// Flushes any pending writes, releases file handle resources, and updates
    /// access time metadata.
    ///
    /// # Arguments
    ///
    /// * `handle` - File handle from fuse_open()
    ///
    /// # Errors
    ///
    /// Returns an error if flush or cleanup fails.
    async fn fuse_release(&self, handle: Self::FileHandle) -> Result<(), Error>;

    /// Delete a file.
    ///
    /// Removes the file and deallocates all associated stripes and chunks.
    /// Metadata removal is coordinated through Raft consensus.
    ///
    /// # Arguments
    ///
    /// * `parent_inode` - Parent directory inode
    /// * `name` - File name to delete
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - File not found
    /// - Permission denied
    /// - File is still open
    /// - Metadata commit fails
    async fn fuse_unlink(&self, parent_inode: u64, name: &str) -> Result<(), Error>;

    /// Rename/move a file.
    ///
    /// Moves a file from one directory to another and/or renames it.
    /// Metadata updates are atomic via Raft consensus.
    ///
    /// # Arguments
    ///
    /// * `parent_inode` - Current parent directory inode
    /// * `name` - Current file name
    /// * `new_parent_inode` - New parent directory inode
    /// * `new_name` - New file name
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Source or destination doesn't exist
    /// - Permission denied
    /// - Cross-filesystem move (not supported)
    /// - Metadata commit fails
    async fn fuse_rename(
        &self,
        parent_inode: u64,
        name: &str,
        new_parent_inode: u64,
        new_name: &str,
    ) -> Result<(), Error>;

    // ===== Directory Operations =====

    /// Create a directory.
    ///
    /// Creates a new directory with the specified attributes.
    ///
    /// # Arguments
    ///
    /// * `parent_inode` - Parent directory inode
    /// * `name` - Directory name
    /// * `mode` - POSIX permissions
    /// * `uid` - Owner user ID
    /// * `gid` - Owner group ID
    ///
    /// # Returns
    ///
    /// File attributes for the created directory.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Parent doesn't exist
    /// - Directory already exists
    /// - Permission denied
    /// - Metadata commit fails
    async fn fuse_mkdir(
        &self,
        parent_inode: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<crate::filesystem_service::FileAttr, Error>;

    /// Remove a directory.
    ///
    /// Removes an empty directory. Non-empty directories return an error.
    ///
    /// # Arguments
    ///
    /// * `parent_inode` - Parent directory inode
    /// * `name` - Directory name to remove
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Directory not found
    /// - Directory not empty
    /// - Permission denied
    /// - Metadata commit fails
    async fn fuse_rmdir(&self, parent_inode: u64, name: &str) -> Result<(), Error>;

    /// Open a directory for reading.
    ///
    /// Opens a directory and returns a handle for readdir operations.
    ///
    /// # Arguments
    ///
    /// * `inode` - Directory inode
    ///
    /// # Returns
    ///
    /// Directory handle for subsequent readdir calls.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Not a directory
    /// - Permission denied
    async fn fuse_opendir(&self, inode: u64) -> Result<Self::DirHandle, Error>;

    /// Read directory entries.
    ///
    /// Returns directory entries starting at the specified offset.
    /// Supports pagination for large directories.
    ///
    /// # Arguments
    ///
    /// * `handle` - Directory handle from fuse_opendir()
    /// * `offset` - Starting offset in directory listing
    ///
    /// # Returns
    ///
    /// Vector of directory entries.
    ///
    /// # Errors
    ///
    /// Returns an error if handle is invalid or read fails.
    async fn fuse_readdir(
        &self,
        handle: &Self::DirHandle,
        offset: i64,
    ) -> Result<Vec<crate::filesystem_service::DirEntry>, Error>;

    /// Release (close) a directory.
    ///
    /// Releases directory handle resources.
    ///
    /// # Arguments
    ///
    /// * `handle` - Directory handle from fuse_opendir()
    async fn fuse_releasedir(&self, handle: Self::DirHandle) -> Result<(), Error>;

    // ===== Metadata Operations =====

    /// Get file or directory attributes.
    ///
    /// Retrieves metadata including size, permissions, timestamps, etc.
    ///
    /// # Arguments
    ///
    /// * `inode` - File or directory inode
    ///
    /// # Returns
    ///
    /// File attributes.
    ///
    /// # Errors
    ///
    /// Returns an error if inode not found.
    async fn fuse_getattr(&self, inode: u64) -> Result<crate::filesystem_service::FileAttr, Error>;

    /// Set file or directory attributes.
    ///
    /// Updates metadata attributes. Only specified fields are updated.
    ///
    /// # Arguments
    ///
    /// * `inode` - File or directory inode
    /// * `mode` - New permissions (optional)
    /// * `uid` - New owner user ID (optional)
    /// * `gid` - New owner group ID (optional)
    /// * `size` - New size for truncate (optional)
    /// * `atime` - New access time (optional)
    /// * `mtime` - New modification time (optional)
    ///
    /// # Returns
    ///
    /// Updated file attributes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Inode not found
    /// - Permission denied
    /// - Metadata commit fails
    #[allow(clippy::too_many_arguments)]
    async fn fuse_setattr(
        &self,
        inode: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<std::time::SystemTime>,
        mtime: Option<std::time::SystemTime>,
    ) -> Result<crate::filesystem_service::FileAttr, Error>;

    /// Get filesystem statistics.
    ///
    /// Returns overall filesystem statistics including total/free space,
    /// inode counts, etc.
    ///
    /// # Returns
    ///
    /// Filesystem statistics structure.
    async fn fuse_statfs(&self) -> Result<crate::filesystem_service::FileAttr, Error>;

    // ===== Extended Attributes =====

    /// Get an extended attribute.
    ///
    /// Retrieves the value of a named extended attribute.
    ///
    /// # Arguments
    ///
    /// * `inode` - File or directory inode
    /// * `name` - Attribute name
    ///
    /// # Returns
    ///
    /// Attribute value as bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Inode not found
    /// - Attribute doesn't exist
    /// - Permission denied
    async fn fuse_getxattr(&self, inode: u64, name: &str) -> Result<Vec<u8>, Error>;

    /// Set an extended attribute.
    ///
    /// Sets or creates a named extended attribute.
    ///
    /// # Arguments
    ///
    /// * `inode` - File or directory inode
    /// * `name` - Attribute name
    /// * `value` - Attribute value
    /// * `flags` - Operation flags (create, replace)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Inode not found
    /// - Permission denied
    /// - Metadata commit fails
    async fn fuse_setxattr(
        &self,
        inode: u64,
        name: &str,
        value: Vec<u8>,
        flags: u32,
    ) -> Result<(), Error>;

    /// List all extended attributes.
    ///
    /// Returns the names of all extended attributes for an inode.
    ///
    /// # Arguments
    ///
    /// * `inode` - File or directory inode
    ///
    /// # Returns
    ///
    /// Vector of attribute names.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Inode not found
    /// - Permission denied
    async fn fuse_listxattr(&self, inode: u64) -> Result<Vec<String>, Error>;

    /// Remove an extended attribute.
    ///
    /// Deletes a named extended attribute.
    ///
    /// # Arguments
    ///
    /// * `inode` - File or directory inode
    /// * `name` - Attribute name to remove
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Inode not found
    /// - Attribute doesn't exist
    /// - Permission denied
    /// - Metadata commit fails
    async fn fuse_removexattr(&self, inode: u64, name: &str) -> Result<(), Error>;
}
