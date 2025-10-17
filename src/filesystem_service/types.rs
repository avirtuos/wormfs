//! Common types for the FileSystemService component.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;

// Re-export common ID types
pub use crate::file_store::types::FileId;
pub use crate::metadata_store::types::ClientId;

/// File handle opaque to FUSE.
pub type FileHandle = u64;

/// Configuration for FileSystemService.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    /// Node ID for this storage node (used in distributed lock tracking)
    pub node_id: u64,

    /// Client heartbeat timeout - how long before client considered dead (in seconds when serialized)
    /// Phase 1: Set to 24 hours (effectively infinite, no real heartbeats)
    /// Phase 2: Set to 30 seconds (with actual gRPC heartbeat endpoint)
    #[serde(with = "serde_duration_seconds")]
    pub client_heartbeat_timeout: Duration,

    /// Enable read lock enforcement
    pub enable_read_locks: bool,

    /// Lock timeout duration (in seconds when serialized)
    #[serde(with = "serde_duration_seconds")]
    pub lock_timeout: Duration,

    /// Lock extend interval for long-lived operations (in seconds when serialized)
    #[serde(with = "serde_duration_seconds")]
    pub lock_extend_interval: Duration,

    /// Maximum file handles per client
    pub max_file_handles: usize,

    /// Inode cache size (number of entries)
    pub inode_cache_size: usize,

    /// Inode cache TTL (in seconds when serialized)
    #[serde(with = "serde_duration_seconds")]
    pub inode_cache_ttl: Duration,

    /// Read buffer size (for stripe assembly)
    pub read_buffer_size: usize,

    /// Write buffer size
    pub write_buffer_size: usize,

    /// Enable write-through (no buffering)
    pub write_through: bool,

    /// Default file permissions
    pub default_file_mode: u32,

    /// Default directory permissions
    pub default_dir_mode: u32,

    /// Maximum file size
    pub max_file_size: u64,

    /// Enable extended attributes
    pub enable_xattr: bool,

    /// Default UID for filesystem operations
    pub uid: u32,

    /// Default GID for filesystem operations
    pub gid: u32,
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

impl Default for Config {
    fn default() -> Self {
        Self {
            node_id: 1,                                           // Single-node Phase 1 default
            client_heartbeat_timeout: Duration::from_secs(86400), // 24 hours (stub mode)
            enable_read_locks: true,
            lock_timeout: Duration::from_secs(10),
            lock_extend_interval: Duration::from_secs(5),
            max_file_handles: 10_000,
            inode_cache_size: 10_000,
            inode_cache_ttl: Duration::from_secs(60),
            read_buffer_size: 10 * 1024 * 1024,  // 10MB
            write_buffer_size: 10 * 1024 * 1024, // 10MB
            write_through: true,
            default_file_mode: 0o644,
            default_dir_mode: 0o755,
            // 16 EiB (exbibytes) = 16 * 1024^6 bytes
            // Still allows files 16,000x larger than current largest files (~1 PiB)
            // Leaves headroom to prevent offset + data.len() overflow
            max_file_size: 16 * 1024 * 1024 * 1024 * 1024 * 1024,
            enable_xattr: true,
            uid: 1000,
            gid: 1000,
        }
    }
}

/// Errors that can occur during FileSystemService operations.
#[derive(Error, Debug)]
pub enum Error {
    /// File not found
    #[error("File not found: inode {0}")]
    NotFound(u64),

    /// File already exists
    #[error("File already exists: {0}")]
    AlreadyExists(String),

    /// Permission denied
    #[error("Permission denied for operation on inode {0}")]
    PermissionDenied(u64),

    /// Directory not empty
    #[error("Directory not empty: inode {0}")]
    DirectoryNotEmpty(u64),

    /// Not a directory
    #[error("Not a directory: inode {0}")]
    NotADirectory(u64),

    /// Is a directory
    #[error("Is a directory: inode {0}")]
    IsADirectory(u64),

    /// Not a symbolic link
    #[error("Not a symbolic link: inode {0}")]
    NotASymlink(u64),

    /// Lock conflict
    #[error("Lock conflict: {0}")]
    LockConflict(String),

    /// Lock not held
    #[error("Lock not held: {0}")]
    LockNotHeld(String),

    /// Insufficient storage space
    #[error("Insufficient storage space")]
    NoSpace,

    /// Invalid argument
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Data operation failed
    #[error("Data operation failed: {0}")]
    DataFailed(String),

    /// Invalid file handle
    #[error("Invalid file handle: {0}")]
    InvalidFileHandle(FileHandle),

    /// Raft operation error
    #[error("Raft error: {0}")]
    RaftError(String),

    /// Metadata error
    #[error("Metadata error: {0}")]
    MetadataError(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Not supported
    #[error("Not supported: {0}")]
    NotSupported(String),
}

impl Error {
    /// Convert error to FUSE errno for kernel interface.
    ///
    /// This is a stub implementation - actual mapping will be done during implementation.
    pub fn to_errno(&self) -> i32 {
        match self {
            Self::NotFound(_) => libc::ENOENT,
            Self::PermissionDenied(_) => libc::EACCES,
            Self::AlreadyExists(_) => libc::EEXIST,
            Self::NotADirectory(_) => libc::ENOTDIR,
            Self::IsADirectory(_) => libc::EISDIR,
            Self::NotASymlink(_) => libc::EINVAL,
            Self::DirectoryNotEmpty(_) => libc::ENOTEMPTY,
            Self::InvalidFileHandle(_) => libc::EBADF,
            Self::LockConflict(_) => libc::ENOLCK,
            Self::LockNotHeld(_) => libc::ENOLCK,
            Self::NoSpace => libc::ENOSPC,
            Self::InvalidArgument(_) => libc::EINVAL,
            Self::Io(_) => libc::EIO,
            Self::DataFailed(_) => libc::EIO,
            Self::RaftError(_) | Self::MetadataError(_) | Self::Internal(_) => libc::EIO,
            Self::NotSupported(_) => libc::ENOSYS,
        }
    }
}

/// Type of file lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    /// Read lock (shared, allows concurrent reads)
    Read,
    /// Write lock (exclusive, blocks all other locks)
    Write,
}

/// File attributes (similar to POSIX stat).
#[derive(Debug, Clone)]
pub struct FileAttr {
    /// Inode number
    pub ino: u64,

    /// File size in bytes
    pub size: u64,

    /// Number of blocks
    pub blocks: u64,

    /// Access time
    pub atime: SystemTime,

    /// Modification time
    pub mtime: SystemTime,

    /// Change time
    pub ctime: SystemTime,

    /// Creation time
    pub crtime: SystemTime,

    /// File type
    pub kind: FileType,

    /// Permissions
    pub perm: u16,

    /// Number of hard links
    pub nlink: u32,

    /// Owner user ID
    pub uid: u32,

    /// Owner group ID
    pub gid: u32,

    /// Device ID (for special files)
    pub rdev: u32,

    /// Block size for filesystem I/O
    pub blksize: u32,

    /// Flags
    pub flags: u32,
}

/// Directory entry.
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// Inode number
    pub ino: u64,

    /// File name
    pub name: String,

    /// File type (regular, directory, symlink, etc.)
    pub kind: FileType,
}

/// File type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    /// Regular file
    RegularFile,
    /// Directory
    Directory,
    /// Symbolic link
    Symlink,
    /// Named pipe (FIFO)
    NamedPipe,
    /// Block device
    BlockDevice,
    /// Character device
    CharDevice,
    /// Socket
    Socket,
}

/// Represents an open file with its associated state.
#[derive(Debug)]
pub struct OpenFile {
    /// File identifier
    pub file_id: FileId,
    /// Inode number
    pub inode: u64,
    /// Client ID that opened the file
    pub client_id: ClientId,
    /// Lock ID if file is locked
    pub lock_id: Option<u64>,
    /// Open flags
    pub flags: OpenFlags,
    /// Current file offset (for operations that need it)
    pub offset: AtomicU64,
    /// Reference count for deferred deletion
    /// When a file is unlinked while still open, this tracks how many handles remain
    pub refcount: std::sync::atomic::AtomicU32,
}

/// Open flags parsed from FUSE.
#[derive(Debug, Clone, Copy)]
pub struct OpenFlags {
    pub read: bool,
    pub write: bool,
    pub append: bool,
    pub truncate: bool,
    pub create: bool,
    pub exclusive: bool,
}

impl OpenFlags {
    /// Create OpenFlags from FUSE flags.
    ///
    /// This is a stub implementation - actual parsing will be done during implementation.
    pub fn from_fuse(_flags: u32) -> Self {
        unimplemented!("OpenFlags::from_fuse will be implemented")
    }

    /// Determine the lock type needed for these flags.
    pub fn lock_type(&self) -> LockType {
        if self.write {
            LockType::Write
        } else {
            LockType::Read
        }
    }
}

/// Attributes to set via setattr operation.
#[derive(Debug, Default)]
pub struct SetAttr {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<SystemTime>,
    pub mtime: Option<SystemTime>,
}

/// Inode cache for fast path lookups.
#[derive(Debug)]
pub struct InodeCache {
    pub entries: HashMap<u64, CachedInode>,
    pub path_to_inode: HashMap<PathBuf, u64>,
}

impl InodeCache {
    /// Create a new empty inode cache.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            path_to_inode: HashMap::new(),
        }
    }
}

impl Default for InodeCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Cached inode entry.
#[derive(Debug, Clone)]
pub struct CachedInode {
    pub file_id: FileId,
    pub attrs: FileAttr,
    pub inserted_at: Instant,
}
