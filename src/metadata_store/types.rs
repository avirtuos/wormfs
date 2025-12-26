//! Common types for the MetadataStore component.

use std::path::PathBuf;
use std::time::SystemTime;
use thiserror::Error;

// Re-export common ID types from file_store
pub use crate::file_store::types::{ChunkId, DiskId, FileId, NodeId, StripeId};

/// Unique identifier for a client connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(pub u64);

impl ClientId {
    /// Create a new ClientId.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the inner u64 value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Configuration for MetadataStore.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    /// Path to SQLite database file
    pub database_path: PathBuf,

    /// Number of read connections in pool (4-8 recommended)
    pub read_pool_size: usize,

    /// Enable WAL mode for better concurrent access
    pub enable_wal: bool,

    /// SQLite page cache size in MB (default: 10MB, maximum: 2047MB)
    ///
    /// Values above 2047 will cause initialization to fail with `Error::ConfigInvalid`
    /// to prevent integer overflow when converting to KB.
    pub cache_size_mb: usize,

    /// Enable foreign key constraints
    pub enable_foreign_keys: bool,

    /// Synchronous mode setting
    pub synchronous: SynchronousMode,

    /// Transaction isolation level
    pub transaction_isolation: IsolationLevel,

    /// Enable prepared statements for common queries
    pub enable_prepared_statements: bool,

    /// Timeout for acquiring connection from read pool (in seconds)
    pub read_pool_timeout_secs: u64,

    /// Stripe metadata cache size in MB (default: 64MB)
    pub stripe_cache_size_mb: usize,

    /// Stripe cache time-to-live in seconds (default: 10 seconds)
    /// Entries expire after this duration regardless of access frequency
    pub stripe_cache_ttl_secs: u64,

    /// Stripe cache time-to-idle in seconds (default: 5 seconds)
    /// Entries expire if not accessed within this duration
    pub stripe_cache_tti_secs: u64,

    /// Chunk list cache size in MB (default: 64MB)
    pub chunk_cache_size_mb: usize,

    /// Chunk cache time-to-live in seconds (default: 10 seconds)
    pub chunk_cache_ttl_secs: u64,

    /// Chunk cache time-to-idle in seconds (default: 5 seconds)
    pub chunk_cache_tti_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database_path: PathBuf::from("/var/lib/wormfs/metadata.db"),
            read_pool_size: 8,
            enable_wal: true,
            cache_size_mb: 10,
            enable_foreign_keys: true,
            synchronous: SynchronousMode::Normal,
            transaction_isolation: IsolationLevel::Serializable,
            enable_prepared_statements: true,
            read_pool_timeout_secs: 30,
            stripe_cache_size_mb: 64,
            stripe_cache_ttl_secs: 10,
            stripe_cache_tti_secs: 5,
            chunk_cache_size_mb: 64,
            chunk_cache_ttl_secs: 10,
            chunk_cache_tti_secs: 5,
        }
    }
}

/// SQLite synchronous mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SynchronousMode {
    /// No syncing (fastest, least safe)
    Off,
    /// Sync at critical moments only
    Normal,
    /// Full sync after each transaction (slowest, safest)
    Full,
}

/// Transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IsolationLevel {
    /// Read committed isolation
    ReadCommitted,
    /// Serializable isolation (strictest)
    Serializable,
}

/// Errors that can occur during MetadataStore operations.
#[derive(Error, Debug)]
pub enum Error {
    /// File already exists
    #[error("File already exists at path {0:?}")]
    FileAlreadyExists(PathBuf),

    /// File not found by path
    #[error("File not found at path: {0}")]
    FileNotFoundByPath(String),

    /// File not found by inode
    #[error("File not found with inode: {0}")]
    FileNotFoundByInode(u64),

    /// File not found by file ID
    #[error("File not found with file_id: {0:?}")]
    FileNotFoundByFileId(FileId),

    /// Parent directory not found
    #[error("Parent directory not found: {0:?}")]
    ParentNotFound(PathBuf),

    /// Stripe not found
    #[error("Stripe {0:?} not found")]
    StripeNotFound(StripeId),

    /// Chunk not found
    #[error("Chunk {0:?} not found")]
    ChunkNotFound(ChunkId),

    /// Lock conflict
    #[error("Lock conflict: cannot acquire {lock_type} lock on file {file_id:?}")]
    LockConflict { file_id: FileId, lock_type: String },

    /// Lock not found
    #[error("Lock not found for file {file_id:?} and client {client_id:?}")]
    LockNotFound {
        file_id: FileId,
        client_id: ClientId,
    },

    /// Database constraint violation
    #[error("Database constraint violation: {0}")]
    ConstraintViolation(String),

    /// Schema initialization failed
    #[error("Schema initialization failed: {0}")]
    SchemaInitFailed(String),

    /// Snapshot creation failed
    #[error("Snapshot creation failed: {0}")]
    SnapshotFailed(String),

    /// Snapshot restore failed
    #[error("Snapshot restore failed: {0}")]
    RestoreFailed(String),

    /// Query error
    #[error("Query error: {0}")]
    QueryError(String),

    /// Transaction error
    #[error("Transaction error: {0}")]
    TransactionError(String),

    /// Database connection error
    #[error("Database connection error: {0}")]
    ConnectionError(String),

    /// Inode space exhausted (reached maximum safe inode value)
    #[error("Inode space exhausted: cannot allocate more inodes (max: 2^63-1)")]
    InodeSpaceExhausted,

    /// Configuration validation error
    #[error("Invalid configuration: {0}")]
    ConfigInvalid(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Inode not available
    #[error("No available inodes")]
    NoAvailableInodes,

    /// Inode not reserved
    #[error("Inode {0} is not reserved")]
    InodeNotReserved(u64),

    /// Inode already in use
    #[error("Inode {0} is already in use")]
    InodeInUse(u64),

    /// Inode reservation expired
    #[error("Inode {0} reservation has expired")]
    InodeReservationExpired(u64),
}

/// File metadata structure.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// Type of file (regular file, directory, symlink)
    pub file_type: FileType,

    /// File size in bytes
    pub size: u64,

    /// POSIX permissions
    pub permissions: u32,

    /// Owner user ID
    pub uid: u32,

    /// Owner group ID
    pub gid: u32,

    /// Creation timestamp
    pub created_at: SystemTime,

    /// Last modification timestamp
    pub modified_at: SystemTime,

    /// Last access timestamp
    pub accessed_at: SystemTime,

    /// Symlink target path (only for FileType::Symlink)
    pub target: Option<String>,
}

// ===== Database Record Types =====

/// Type of file in the filesystem.
///
/// This enum represents the different types of files that can exist in the filesystem.
/// The integer values match the database representation for efficient storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum FileType {
    /// Regular file (data file)
    RegularFile = 0,
    /// Directory
    Directory = 1,
    /// Symbolic link (reserved for future use)
    Symlink = 2,
}

impl From<i32> for FileType {
    fn from(value: i32) -> Self {
        match value {
            0 => FileType::RegularFile,
            1 => FileType::Directory,
            2 => FileType::Symlink,
            _ => FileType::RegularFile, // Default to regular file for unknown values
        }
    }
}

impl From<FileType> for i32 {
    fn from(file_type: FileType) -> Self {
        file_type as i32
    }
}

/// File record from the database.
#[derive(Debug, Clone)]
pub struct FileRecord {
    pub file_id: FileId,
    pub inode: u64,
    pub path: PathBuf,
    pub parent_path: PathBuf,
    pub name: String,
    pub file_type: FileType,
    pub size: u64,
    pub permissions: u32,
    pub uid: u32,
    pub gid: u32,
    pub created_at: SystemTime,
    pub modified_at: SystemTime,
    pub accessed_at: SystemTime,
    pub storage_policy_id: u32,
    /// Symlink target path (only for FileType::Symlink)
    pub target: Option<String>,
}

/// Stripe record from the database.
#[derive(Debug, Clone)]
pub struct StripeRecord {
    pub stripe_id: StripeId,
    pub file_id: FileId,
    pub stripe_index: u32,
    pub offset: u64,
    pub size: u64,
    pub checksum: u32,
    pub created_at: SystemTime,
}

/// Chunk record from the database.
#[derive(Debug, Clone)]
pub struct ChunkRecord {
    pub chunk_id: ChunkId,
    pub stripe_id: StripeId,
    pub chunk_index: u8,
    pub node_id: NodeId,
    pub disk_id: DiskId,
    pub checksum: u32,
    pub status: ChunkStatus,
    pub created_at: SystemTime,
    pub last_verified: Option<SystemTime>,
}

/// Chunk status enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkStatus {
    Healthy,
    Corrupt,
    Missing,
    Rebuilding,
}

/// Lock record from the database.
#[derive(Debug, Clone)]
pub struct LockRecord {
    pub lock_id: u64,
    pub file_id: FileId,
    pub client_id: ClientId,
    pub lock_type: LockType,
    pub acquired_at: SystemTime,
    pub expires_at: SystemTime,
}

/// Lock type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    Read,
    Write,
}

/// Node record from the database.
#[derive(Debug, Clone)]
pub struct NodeRecord {
    pub node_id: NodeId,
    pub address: String,
    pub status: NodeStatus,
    pub last_seen: SystemTime,
}

/// Node status enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Online,
    Offline,
    Failed,
}

/// Disk record from the database.
#[derive(Debug, Clone)]
pub struct DiskRecord {
    pub disk_id: DiskId,
    pub node_id: NodeId,
    pub path: PathBuf,
    pub total_space: u64,
    pub free_space: u64,
    pub status: DiskStatus,
}

/// Disk status enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskStatus {
    Healthy,
    Degraded,
    Failed,
}

/// Proposal history record for AdminUI display.
///
/// Tracks Raft proposals applied through the state machine on this node.
/// Stored in the `proposal_history` table for persistence and troubleshooting.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProposalHistoryRecord {
    /// Database row ID
    pub id: u64,
    /// Raft log index (unique per proposal)
    pub log_index: u64,
    /// Raft term when committed
    pub log_term: u64,
    /// Node that was leader when proposed
    pub leader_node_id: u64,
    /// When this proposal was applied locally (Unix timestamp)
    pub applied_at: std::time::SystemTime,
    /// Operation type for display (e.g., "AtomicTransaction")
    pub operation_type: String,
    /// Transaction ID if applicable (hex string)
    pub tx_id: Option<String>,
    /// Number of sub-operations in the proposal
    pub operation_count: usize,
    /// Whether operation succeeded
    pub success: bool,
    /// Error message if failed
    pub error_message: Option<String>,
    /// Full JSON operation details (for click-through view)
    pub operation_details: String,
}
