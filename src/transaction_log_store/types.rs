//! Common types for the TransactionLogStore component.

use std::path::PathBuf;
use thiserror::Error;

/// Configuration for TransactionLogStore.
#[derive(Debug, Clone)]
pub struct Config {
    /// Path to the transaction log database file
    pub log_path: PathBuf,

    /// Enable fsync for durability
    pub enable_fsync: bool,

    /// Cache size for redb (in bytes)
    pub cache_size_bytes: u64,

    /// Maximum log size before rotation (in bytes)
    pub max_log_size_bytes: u64,
}

/// Errors that can occur during TransactionLogStore operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Log entry not found
    #[error("Log entry {0} not found")]
    EntryNotFound(u64),

    /// Log is empty
    #[error("Log is empty")]
    LogEmpty,

    /// Write operation failed
    #[error("Failed to write log entry: {0}")]
    WriteFailed(String),

    /// Read operation failed
    #[error("Failed to read log entry: {0}")]
    ReadFailed(String),

    /// Fsync operation failed
    #[error("Fsync failed: {0}")]
    FsyncFailed(String),

    /// Disk full
    #[error("Disk full: cannot write log entry")]
    DiskFull,

    /// Trim operation failed
    #[error("Failed to trim log: {0}")]
    TrimFailed(String),

    /// Checksum mismatch
    #[error("Checksum mismatch for log entry {0}")]
    ChecksumMismatch(u64),

    /// Database corruption
    #[error("Database corruption detected: {0}")]
    Corruption(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Log entry structure.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Log entry index
    pub index: u64,

    /// Raft term
    pub term: u64,

    /// Entry data (serialized operation)
    pub data: Vec<u8>,

    /// Entry checksum
    pub checksum: u32,
}

/// Transaction log statistics.
#[derive(Debug, Clone)]
pub struct LogStats {
    /// First log index
    pub first_index: u64,

    /// Last log index
    pub last_index: u64,

    /// Total number of entries
    pub entry_count: u64,

    /// Total size of log in bytes
    pub total_size_bytes: u64,
}
