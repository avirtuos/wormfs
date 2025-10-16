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

pub mod factory;
pub mod fuse_adapter;
pub mod implementation;
pub mod inode;
pub mod mount;
pub mod raft_commands;
pub mod raft_integration;
pub mod types;

use async_trait::async_trait;
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
pub use types::{
    CachedInode, ClientId, Config, DirEntry, Error, FileAttr, FileHandle, FileId, FileType,
    InodeCache, LockType, OpenFile, OpenFlags, SetAttr,
};

/// FileSystemService trait defines the FUSE filesystem API.
///
/// Implementations provide filesystem operations that interact with
/// the underlying distributed storage system.
///
/// Note: Construction is handled by FileSystemServiceFactory to allow
/// for clean dependency injection and testing.
#[async_trait]
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock)]
pub trait FileSystemService: Send + Sync {
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

// =============================================================================
// Concrete Implementation with Client Pattern
// =============================================================================

/// Inner state for FileSystemService with interior mutability.
struct FileSystemServiceInner {
    /// File handle map
    file_handles: RwLock<std::collections::HashMap<FileHandle, OpenFile>>,
    /// Inode cache for fast lookups
    inode_cache: RwLock<InodeCache>,
    /// Configuration
    config: Config,
}

/// Concrete FileSystemService implementation with client pattern.
///
/// This struct is cloneable and lightweight, wrapping shared state in Arc.
/// Multiple FUSE handler threads can hold clones that share the same underlying state.
///
/// The concrete types for StorageRaftMember, MetadataStore, and FileStore are determined
/// at compile time via the trait's associated types.
///
/// NOTE: This is a placeholder type definition. The actual implementation will be provided
/// when the concrete types for all dependencies are available.
#[allow(dead_code)]
struct FileSystemServiceImpl {
    /// Shared inner state
    inner: Arc<FileSystemServiceInner>,
    /// Reference to StorageRaftMember for metadata writes
    raft_member: Arc<crate::storage_raft_member::StorageRaftMemberImpl>,
    /// Reference to MetadataStore for metadata reads
    metadata_store: crate::metadata_store::MetadataStoreImpl,
    /// Reference to FileStore for chunk I/O - will be added when FileStoreImpl exists
    _file_store: (),
}

impl FileSystemServiceImpl {
    // === Internal Stripe Operations (stubs) ===

    /// Read one or more stripes and extract requested byte range.
    ///
    /// This is a stub - implementation will handle over-scanning.
    async fn read_stripes(
        &self,
        _file_id: FileId,
        _offset: u64,
        _length: usize,
    ) -> Result<Vec<u8>, Error> {
        unimplemented!("read_stripes will be implemented")
    }

    /// Write data across one or more stripes (read-modify-write for partial).
    ///
    /// This is a stub - implementation will handle buffering and RMW.
    async fn write_stripes(
        &self,
        _file_id: FileId,
        _offset: u64,
        _data: &[u8],
    ) -> Result<(), Error> {
        unimplemented!("write_stripes will be implemented")
    }

    /// Acquire file lock (delegated to RaftMember).
    ///
    /// This is a stub - implementation will delegate to StorageRaftMember.
    async fn acquire_lock_internal(
        &self,
        _file_id: FileId,
        _lock_type: LockType,
    ) -> Result<u64, Error> {
        unimplemented!("acquire_lock_internal will be implemented")
    }

    /// Release file lock.
    ///
    /// This is a stub - implementation will delegate to StorageRaftMember.
    async fn release_lock_internal(&self, _lock_id: u64) -> Result<(), Error> {
        unimplemented!("release_lock_internal will be implemented")
    }
}

// NOTE: Trait implementation is commented out until FileStoreImpl is available.
// Once all concrete types are implemented, uncomment and complete this implementation.
//
// #[async_trait]
// impl FileSystemService for FileSystemServiceImpl {
//     type RaftMember = crate::storage_raft_member::StorageRaftMemberImpl;
//     type MetadataStore = crate::metadata_store::MetadataStoreImpl;
//     type FileStore = crate::file_store::FileStoreImpl;
//
//     fn new(
//         config: Config,
//         raft_member: Arc<Self::RaftMember>,
//         metadata_store: Self::MetadataStore,
//         file_store: Arc<Self::FileStore>,
//     ) -> Result<Self, Error>
//     where
//         Self: Sized,
//     {
//         let inner = Arc::new(FileSystemServiceInner {
//             file_handles: RwLock::new(std::collections::HashMap::new()),
//             inode_cache: RwLock::new(InodeCache::new()),
//             config,
//         });
//
//         Ok(Self {
//             inner,
//             raft_member,
//             metadata_store,
//             _file_store: (),
//         })
//     }
//
//     // ... rest of trait implementation
// }

// The following methods would be part of the FileSystemService trait implementation:
/*
    async fn create(
        &self,
        _parent: u64,
        _name: &str,
        _mode: u32,
        _uid: u32,
        _gid: u32,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        unimplemented!("create will be implemented")
    }

    async fn open(
        &self,
        _inode: u64,
        _flags: u32,
        _client_id: ClientId,
    ) -> Result<(u64, FileAttr), Error> {
        unimplemented!("open will be implemented")
    }

    async fn read(
        &self,
        _inode: u64,
        _offset: u64,
        _size: u32,
        _client_id: ClientId,
    ) -> Result<Vec<u8>, Error> {
        unimplemented!("read will be implemented")
    }

    async fn write(
        &self,
        _inode: u64,
        _offset: u64,
        _data: Vec<u8>,
        _client_id: ClientId,
    ) -> Result<u32, Error> {
        unimplemented!("write will be implemented")
    }

    async fn unlink(&self, _parent: u64, _name: &str, _client_id: ClientId) -> Result<(), Error> {
        unimplemented!("unlink will be implemented")
    }

    async fn mkdir(
        &self,
        _parent: u64,
        _name: &str,
        _mode: u32,
        _uid: u32,
        _gid: u32,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        unimplemented!("mkdir will be implemented")
    }

    async fn rmdir(&self, _parent: u64, _name: &str, _client_id: ClientId) -> Result<(), Error> {
        unimplemented!("rmdir will be implemented")
    }

    async fn readdir(
        &self,
        _inode: u64,
        _offset: i64,
        _client_id: ClientId,
    ) -> Result<Vec<DirEntry>, Error> {
        unimplemented!("readdir will be implemented")
    }

    async fn getattr(&self, _inode: u64) -> Result<FileAttr, Error> {
        unimplemented!("getattr will be implemented")
    }

    async fn setattr(
        &self,
        _inode: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<SystemTime>,
        _mtime: Option<SystemTime>,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        unimplemented!("setattr will be implemented")
    }

    async fn acquire_lock(
        &self,
        _inode: u64,
        _lock_type: LockType,
        _expires_at: SystemTime,
        _client_id: ClientId,
    ) -> Result<u64, Error> {
        unimplemented!("acquire_lock will be implemented")
    }

    async fn release_lock(&self, _inode: u64, _client_id: ClientId) -> Result<(), Error> {
        unimplemented!("release_lock will be implemented")
    }

    async fn extend_lock(
        &self,
        _inode: u64,
        _new_expiry: SystemTime,
        _client_id: ClientId,
    ) -> Result<(), Error> {
        unimplemented!("extend_lock will be implemented")
    }
*/
