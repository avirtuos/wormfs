//! Common types for the SnapshotStore component.

use std::path::PathBuf;
use std::time::SystemTime;
use thiserror::Error;

/// Configuration for SnapshotStore.
#[derive(Debug, Clone)]
pub struct Config {
    /// Directory to store snapshots
    pub snapshot_dir: PathBuf,

    /// Maximum number of snapshots to retain
    pub max_snapshots: usize,

    /// Maximum age for snapshots before pruning
    pub max_snapshot_age_days: u64,

    /// Buffer size for snapshot file operations
    pub buffer_size: usize,
}

/// Errors that can occur during SnapshotStore operations.
#[derive(Error, Debug)]
pub enum Error {
    /// No snapshots available
    #[error("No snapshots available")]
    NoSnapshots,

    /// Snapshot not found
    #[error("Snapshot at index {0} not found")]
    SnapshotNotFound(u64),

    /// Snapshot ingestion failed
    #[error("Failed to ingest snapshot: {0}")]
    IngestFailed(String),

    /// Snapshot directory full
    #[error("Snapshot directory full: {0}")]
    DirectoryFull(String),

    /// Snapshot deletion failed
    #[error("Failed to delete snapshot {snapshot_id}: {reason}")]
    DeletionFailed { snapshot_id: u64, reason: String },

    /// Invalid snapshot file
    #[error("Invalid snapshot file: {0}")]
    InvalidSnapshot(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Snapshot metadata structure.
#[derive(Debug, Clone)]
pub struct SnapshotMetadata {
    /// Snapshot identifier
    pub snapshot_id: u64,

    /// Transaction log index at snapshot time
    pub tx_index: u64,

    /// When snapshot was created
    pub created_at: SystemTime,

    /// Size of snapshot file in bytes
    pub size_bytes: u64,

    /// Path to snapshot file
    pub file_path: PathBuf,
}

/// Snapshot storage statistics.
#[derive(Debug, Clone)]
pub struct SnapshotStats {
    /// Total number of snapshots
    pub snapshot_count: usize,

    /// Total size of all snapshots in bytes
    pub total_size_bytes: u64,

    /// Oldest snapshot timestamp
    pub oldest_snapshot: Option<SystemTime>,

    /// Newest snapshot timestamp
    pub newest_snapshot: Option<SystemTime>,
}
