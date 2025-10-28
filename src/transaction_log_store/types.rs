//! Common types for the TransactionLogStore component.

use std::path::PathBuf;
use std::time::SystemTime;
use thiserror::Error;

/// Configuration for TransactionLogStore.
#[derive(Debug, Clone)]
pub struct TransactionLogConfig {
    /// Path to the transaction log database file
    pub db_path: PathBuf,

    /// Cache size for redb (in MB)
    pub cache_size_mb: usize,

    /// Compact database when log grows beyond this size (in MB)
    pub compact_threshold_mb: usize,

    /// Maximum log size before snapshot is recommended (in MB)
    pub max_log_size_mb: usize,

    /// Maximum log age before snapshot is recommended (in days)
    pub max_log_age_days: u32,
}

impl Default for TransactionLogConfig {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from("/var/lib/wormfs/transaction_log.redb"),
            cache_size_mb: 8,
            compact_threshold_mb: 100,
            max_log_size_mb: 128,
            max_log_age_days: 7,
        }
    }
}

/// Errors that can occur during TransactionLogStore operations.
#[derive(Error, Debug)]
pub enum LogError {
    /// Database error
    #[error("Database error: {0}")]
    DatabaseError(String),

    /// Log entry not found
    #[error("Entry not found at index {0}")]
    EntryNotFound(u64),

    /// Invalid log index
    #[error("Invalid log index: {0}")]
    InvalidIndex(u64),

    /// Invalid range provided
    #[error("Invalid range: {0}")]
    InvalidRange(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Checksum verification failed (data corruption detected)
    #[error("Checksum verification failed at index {0}: data is corrupted")]
    ChecksumFailed(u64),

    /// I/O error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Log entry structure.
///
/// Note: This stores serialized MetadataOperation(s) as bytes.
/// The actual MetadataOperation enum is defined in storage_raft_member::types.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// Log entry index
    pub index: u64,

    /// Raft term
    pub term: u64,

    /// Serialized operations (Vec<MetadataOperation>)
    pub operations: Vec<u8>,

    /// Timestamp when entry was created
    pub timestamp: SystemTime,
}

/// Transaction log statistics.
#[derive(Debug, Clone)]
pub struct LogStats {
    /// First log index
    pub first_index: Option<u64>,

    /// Last log index
    pub last_index: Option<u64>,

    /// Total number of entries
    pub entry_count: u64,

    /// Total size of log in bytes
    pub db_size_bytes: u64,

    /// Last compaction timestamp
    pub last_compaction: Option<SystemTime>,
}

/// Integrity report for log verification.
#[derive(Debug, Clone)]
pub struct IntegrityReport {
    /// Total number of entries checked
    pub total_entries: u64,

    /// Missing indices detected
    pub missing_indices: Vec<u64>,

    /// Whether the log is valid
    pub is_valid: bool,
}
