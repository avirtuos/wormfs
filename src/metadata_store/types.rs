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
#[derive(Debug, Clone)]
pub struct Config {
    /// Path to SQLite database file
    pub database_path: PathBuf,

    /// Maximum number of database connections in pool
    pub max_connections: u32,

    /// Enable WAL mode for better concurrent access
    pub enable_wal: bool,

    /// SQLite cache size (in KB)
    pub cache_size_kb: u32,

    /// Enable foreign key constraints
    pub enable_foreign_keys: bool,

    /// Synchronous mode setting
    pub synchronous: SynchronousMode,
}

/// SQLite synchronous mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SynchronousMode {
    /// No syncing (fastest, least safe)
    Off,
    /// Sync at critical moments only
    Normal,
    /// Full sync after each transaction (slowest, safest)
    Full,
}

/// Errors that can occur during MetadataStore operations.
#[derive(Error, Debug)]
pub enum Error {
    /// File already exists
    #[error("File already exists at path {0:?}")]
    FileAlreadyExists(PathBuf),

    /// File not found
    #[error("File not found: {0}")]
    FileNotFound(String),

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

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// File metadata structure.
#[derive(Debug, Clone)]
pub struct FileMetadata {
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
}
