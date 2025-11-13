//! Common types for the SnapshotStore component.

use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use thiserror::Error;

/// Configuration for SnapshotStore.
#[derive(Debug, Clone)]
pub struct Config {
    /// Base directory for snapshot storage
    pub storage_dir: PathBuf,

    /// Retention policy
    pub retention_policy: RetentionPolicy,

    /// Compression algorithm (future use)
    pub compression: CompressionAlgorithm,

    /// Chunk size for streaming snapshots
    pub stream_chunk_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage_dir: PathBuf::from("/var/lib/wormfs/snapshots"),
            retention_policy: RetentionPolicy::default(),
            compression: CompressionAlgorithm::None,
            stream_chunk_size: 64 * 1024, // 64KB
        }
    }
}

/// Retention policy for snapshots.
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Maximum number of snapshots to keep
    pub max_snapshots: usize,

    /// Maximum age of snapshots to keep
    pub max_age: Duration,

    /// Always keep at least this many snapshots
    pub min_snapshots: usize,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_snapshots: 10,
            max_age: Duration::from_secs(30 * 24 * 60 * 60), // 30 days
            min_snapshots: 3,
        }
    }
}

/// Compression algorithm for snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// Zstd compression with specified level (1-22)
    Zstd { level: i32 },
}

/// Errors that can occur during SnapshotStore operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Snapshot not found
    #[error("Snapshot not found: {0}")]
    NotFound(u64),

    /// Invalid snapshot
    #[error("Invalid snapshot: {0}")]
    Invalid(String),

    /// Checksum mismatch
    #[error("Checksum mismatch")]
    ChecksumMismatch,

    /// I/O error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Registry error
    #[error("Registry error: {0}")]
    RegistryError(String),

    /// Corruption detected
    #[error("Corruption detected: {0}")]
    Corruption(String),

    /// Storage full
    #[error("Storage full")]
    StorageFull,

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Compression/decompression error
    #[error("Compression error: {0}")]
    CompressionError(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),
}

/// Snapshot information structure.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotInfo {
    /// Snapshot identifier
    pub snapshot_id: u64,

    /// Log index at snapshot time
    pub log_index: u64,

    /// Log term at snapshot time
    pub log_term: u64,

    /// When snapshot was created
    pub timestamp: SystemTime,

    /// Format version
    pub format_version: u16,

    /// Size of metadata database in bytes
    pub metadata_db_size: u64,

    /// Checksum of metadata database
    pub metadata_db_checksum: String,

    /// Compression algorithm used
    pub compression: CompressionAlgorithm,

    /// Node ID that created the snapshot
    pub node_id: String,

    /// Path to snapshot storage directory
    pub storage_path: PathBuf,

    /// Membership log index (index of the log entry that established the membership in this snapshot)
    /// None if membership was established before any logged changes
    pub membership_log_index: Option<u64>,

    /// Membership log term (term of the log entry that established the membership in this snapshot)
    /// None if membership was established before any logged changes
    pub membership_log_term: Option<u64>,

    /// Node ID of the leader that created this log entry (for membership log_id)
    /// None if membership was established before any logged changes
    pub membership_leader_node_id: Option<u64>,

    /// Serialized membership configuration (JSON format)
    /// Contains the voter and learner node configurations that were active at snapshot time
    pub membership_config: String,

    /// Node ID of the leader that created the last log entry in this snapshot
    pub snapshot_leader_node_id: u64,
}

impl SnapshotInfo {
    /// Get path to metadata database file.
    pub fn metadata_db_path(&self) -> PathBuf {
        self.storage_path.join("metadata.db")
    }

    /// Get path to metadata JSON file.
    pub fn metadata_json_path(&self) -> PathBuf {
        self.storage_path.join("metadata.json")
    }

    /// Get path to checksum file.
    pub fn checksum_path(&self) -> PathBuf {
        self.storage_path.join("checksum.sha256")
    }
}

/// Snapshot storage statistics.
#[derive(Debug, Clone)]
pub struct SnapshotStats {
    /// Total number of snapshots
    pub total_snapshots: usize,

    /// Total size of all snapshots in bytes
    pub total_size: u64,

    /// Oldest snapshot timestamp
    pub oldest_snapshot: Option<SystemTime>,

    /// Newest snapshot timestamp
    pub newest_snapshot: Option<SystemTime>,

    /// Disk usage in bytes
    pub disk_usage: u64,
}

/// Snapshot reader for accessing snapshot data.
#[derive(Debug)]
pub struct SnapshotReader {
    /// Snapshot ID
    snapshot_id: u64,

    /// Path to metadata database
    metadata_path: PathBuf,

    /// Snapshot information
    snapshot_info: SnapshotInfo,
}

impl SnapshotReader {
    /// Create a new snapshot reader.
    pub fn new(snapshot_id: u64, metadata_path: PathBuf, snapshot_info: SnapshotInfo) -> Self {
        Self {
            snapshot_id,
            metadata_path,
            snapshot_info,
        }
    }

    /// Get the path to the metadata database.
    pub fn get_metadata_db_path(&self) -> &PathBuf {
        &self.metadata_path
    }

    /// Get snapshot information.
    pub fn get_snapshot_info(&self) -> &SnapshotInfo {
        &self.snapshot_info
    }

    /// Get snapshot ID.
    pub fn snapshot_id(&self) -> u64 {
        self.snapshot_id
    }
}
