//! Common types for the FileSystemService component.

use std::time::SystemTime;
use thiserror::Error;

// Re-export common ID types
pub use crate::file_store::types::FileId;
pub use crate::metadata_store::types::ClientId;

/// Configuration for FileSystemService.
#[derive(Debug, Clone)]
pub struct Config {
    /// Default file permissions
    pub default_file_mode: u32,

    /// Default directory permissions
    pub default_dir_mode: u32,

    /// Maximum file size
    pub max_file_size: u64,

    /// Default lock timeout
    pub default_lock_timeout_secs: u64,

    /// Enable extended attributes
    pub enable_xattr: bool,
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

    /// Lock conflict
    #[error("Lock conflict: cannot acquire {lock_type:?} lock on inode {inode}")]
    LockConflict { inode: u64, lock_type: LockType },

    /// Lock not held
    #[error("Lock not held by client {client_id:?} on inode {inode}")]
    LockNotHeld { inode: u64, client_id: ClientId },

    /// Insufficient storage space
    #[error("Insufficient storage space")]
    NoSpace,

    /// Invalid argument
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Metadata operation failed
    #[error("Metadata operation failed: {0}")]
    MetadataFailed(String),

    /// Data operation failed
    #[error("Data operation failed: {0}")]
    DataFailed(String),
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
