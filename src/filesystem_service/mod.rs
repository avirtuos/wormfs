//! # FileSystemService Component
//!
//! FileSystemService exposes FUSE-compatible APIs for client filesystems to interact with WormFS.
//!
//! ## Responsibilities
//!
//! - Providing FUSE filesystem operations (create, read, write, delete, etc.)
//! - Translating FUSE operations into metadata and data operations
//! - Interacting with StorageRaftMember for metadata write operations
//! - Querying MetadataStore directly for read operations
//! - Coordinating with FileStore for chunk data read/write operations
//! - Managing file locks for concurrent access control
//! - Handling POSIX semantics (permissions, ownership, timestamps)
//! - Providing directory operations (mkdir, rmdir, readdir)
//! - Supporting extended attributes (xattr) for metadata storage
//!
//! ## Operation Categories
//!
//! ### File Operations
//! - **create**: Create a new file
//! - **open**: Open a file for reading or writing
//! - **read**: Read file data at offset
//! - **write**: Write file data at offset
//! - **truncate**: Change file size
//! - **unlink**: Delete a file
//! - **rename**: Move/rename a file
//!
//! ### Directory Operations
//! - **mkdir**: Create directory
//! - **rmdir**: Remove directory
//! - **readdir**: List directory contents
//! - **lookup**: Resolve filename to inode
//!
//! ### Metadata Operations
//! - **getattr**: Get file attributes (stat)
//! - **setattr**: Set file attributes (chmod, chown, utime)
//! - **statfs**: Get filesystem statistics
//!
//! ### Lock Operations
//! - **acquire_lock**: Acquire read or write lock
//! - **release_lock**: Release lock
//! - **extend_lock**: Extend lock lease
//!
//! ## Operation Flow
//!
//! ### Write Operation Flow
//! ```text
//! Client write(file_id, offset, data)
//!      │
//!      ▼
//! FileSystemService
//!      │
//!      ├─── Check lock ownership
//!      │
//!      ├─── Calculate stripe layout
//!      │
//!      ├─── Propose transaction via StorageRaftMember ───► Raft Consensus
//!      │                                                       │
//!      │                                                       ▼
//!      │                                                   2PC Protocol
//!      │                                                       │
//!      ├─── Apply erasure coding ──────────────► FileStore ◄──┘
//!      │
//!      └─── Return success to client
//! ```
//!
//! ### Read Operation Flow
//! ```text
//! Client read(file_id, offset, size)
//!      │
//!      ▼
//! FileSystemService
//!      │
//!      ├─── Query metadata ──────────► MetadataStore
//!      │                                    │
//!      │                                    ▼
//!      │                               Get stripes
//!      │
//!      ├─── Request chunks ─────────► FileStore
//!      │                                    │
//!      │                                    ▼
//!      │                            Read & reconstruct
//!      │
//!      └─── Return data to client
//! ```
//!
//! ## Lock Management
//!
//! FileSystemService enforces distributed file locking:
//! - **Read locks**: Allow concurrent reads, block writes
//! - **Write locks**: Exclusive access, block all other locks
//! - **Lock leases**: Time-bound with automatic expiration
//! - **Lock extension**: Clients can extend before expiration

pub mod types;

use async_trait::async_trait;
use std::time::SystemTime;
pub use types::{ClientId, Config, DirEntry, Error, FileAttr, FileId, FileType, LockType};

/// FileSystemService trait defines the FUSE filesystem API.
///
/// Implementations provide filesystem operations that interact with
/// the underlying distributed storage system.
#[async_trait]
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock)]
pub trait FileSystemService: Send + Sync {
    /// Create a new FileSystemService.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration
    ///
    /// # Returns
    ///
    /// A new FileSystemService instance.
    fn new(config: Config) -> Result<Self, Error>
    where
        Self: Sized;

    // ===== File Operations =====

    /// Create a new file.
    ///
    /// # Arguments
    ///
    /// * `parent` - Parent directory inode
    /// * `name` - File name
    /// * `mode` - POSIX permissions
    /// * `uid` - Owner user ID
    /// * `gid` - Owner group ID
    /// * `client_id` - Client creating the file
    ///
    /// # Returns
    ///
    /// File attributes for the created file.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Parent directory doesn't exist
    /// - File already exists
    /// - Permission denied
    async fn create(
        &self,
        parent: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        client_id: ClientId,
    ) -> Result<FileAttr, Error>;

    /// Open a file.
    ///
    /// # Arguments
    ///
    /// * `inode` - File inode
    /// * `flags` - Open flags (read, write, append)
    /// * `client_id` - Client opening the file
    ///
    /// # Returns
    ///
    /// File handle and attributes.
    ///
    /// # Errors
    ///
    /// Returns an error if file not found or permission denied.
    async fn open(
        &self,
        inode: u64,
        flags: u32,
        client_id: ClientId,
    ) -> Result<(u64, FileAttr), Error>;

    /// Read file data.
    ///
    /// # Arguments
    ///
    /// * `inode` - File inode
    /// * `offset` - Byte offset in file
    /// * `size` - Number of bytes to read
    /// * `client_id` - Client reading the file
    ///
    /// # Returns
    ///
    /// File data read from the specified offset.
    ///
    /// # Errors
    ///
    /// Returns an error if read fails or permission denied.
    async fn read(
        &self,
        inode: u64,
        offset: u64,
        size: u32,
        client_id: ClientId,
    ) -> Result<Vec<u8>, Error>;

    /// Write file data.
    ///
    /// # Arguments
    ///
    /// * `inode` - File inode
    /// * `offset` - Byte offset in file
    /// * `data` - Data to write
    /// * `client_id` - Client writing the file
    ///
    /// # Returns
    ///
    /// Number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Write lock not held
    /// - Permission denied
    /// - Insufficient storage space
    async fn write(
        &self,
        inode: u64,
        offset: u64,
        data: Vec<u8>,
        client_id: ClientId,
    ) -> Result<u32, Error>;

    /// Delete a file.
    ///
    /// # Arguments
    ///
    /// * `parent` - Parent directory inode
    /// * `name` - File name to delete
    /// * `client_id` - Client deleting the file
    ///
    /// # Errors
    ///
    /// Returns an error if file not found or permission denied.
    async fn unlink(&self, parent: u64, name: &str, client_id: ClientId) -> Result<(), Error>;

    // ===== Directory Operations =====

    /// Create a directory.
    ///
    /// # Arguments
    ///
    /// * `parent` - Parent directory inode
    /// * `name` - Directory name
    /// * `mode` - POSIX permissions
    /// * `uid` - Owner user ID
    /// * `gid` - Owner group ID
    /// * `client_id` - Client creating the directory
    ///
    /// # Returns
    ///
    /// File attributes for the created directory.
    async fn mkdir(
        &self,
        parent: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        client_id: ClientId,
    ) -> Result<FileAttr, Error>;

    /// Remove a directory.
    ///
    /// # Arguments
    ///
    /// * `parent` - Parent directory inode
    /// * `name` - Directory name to remove
    /// * `client_id` - Client removing the directory
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Directory not empty
    /// - Directory not found
    /// - Permission denied
    async fn rmdir(&self, parent: u64, name: &str, client_id: ClientId) -> Result<(), Error>;

    /// Read directory contents.
    ///
    /// # Arguments
    ///
    /// * `inode` - Directory inode
    /// * `offset` - Offset in directory listing
    /// * `client_id` - Client reading the directory
    ///
    /// # Returns
    ///
    /// Vector of directory entries.
    ///
    /// # Errors
    ///
    /// Returns an error if not a directory or permission denied.
    async fn readdir(
        &self,
        inode: u64,
        offset: i64,
        client_id: ClientId,
    ) -> Result<Vec<DirEntry>, Error>;

    // ===== Metadata Operations =====

    /// Get file attributes.
    ///
    /// # Arguments
    ///
    /// * `inode` - File inode
    ///
    /// # Returns
    ///
    /// File attributes (size, permissions, timestamps, etc.).
    ///
    /// # Errors
    ///
    /// Returns an error if file not found.
    async fn getattr(&self, inode: u64) -> Result<FileAttr, Error>;

    /// Set file attributes.
    ///
    /// # Arguments
    ///
    /// * `inode` - File inode
    /// * `mode` - New permissions (optional)
    /// * `uid` - New owner user ID (optional)
    /// * `gid` - New owner group ID (optional)
    /// * `size` - New size (optional, for truncate)
    /// * `atime` - New access time (optional)
    /// * `mtime` - New modification time (optional)
    /// * `client_id` - Client setting attributes
    ///
    /// # Returns
    ///
    /// Updated file attributes.
    ///
    /// # Errors
    ///
    /// Returns an error if file not found or permission denied.
    #[allow(clippy::too_many_arguments)]
    async fn setattr(
        &self,
        inode: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
        client_id: ClientId,
    ) -> Result<FileAttr, Error>;

    // ===== Lock Operations =====

    /// Acquire a lock on a file.
    ///
    /// # Arguments
    ///
    /// * `inode` - File inode
    /// * `lock_type` - Read or write lock
    /// * `expires_at` - Lock expiration time
    /// * `client_id` - Client acquiring the lock
    ///
    /// # Returns
    ///
    /// Lock ID if acquired.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Lock conflict (another client holds incompatible lock)
    /// - File not found
    async fn acquire_lock(
        &self,
        inode: u64,
        lock_type: LockType,
        expires_at: SystemTime,
        client_id: ClientId,
    ) -> Result<u64, Error>;

    /// Release a lock on a file.
    ///
    /// # Arguments
    ///
    /// * `inode` - File inode
    /// * `client_id` - Client releasing the lock
    ///
    /// # Errors
    ///
    /// Returns an error if lock not held by this client.
    async fn release_lock(&self, inode: u64, client_id: ClientId) -> Result<(), Error>;

    /// Extend lock expiration time.
    ///
    /// # Arguments
    ///
    /// * `inode` - File inode
    /// * `new_expiry` - New expiration time
    /// * `client_id` - Client extending the lock
    ///
    /// # Errors
    ///
    /// Returns an error if lock not held by this client.
    async fn extend_lock(
        &self,
        inode: u64,
        new_expiry: SystemTime,
        client_id: ClientId,
    ) -> Result<(), Error>;
}
