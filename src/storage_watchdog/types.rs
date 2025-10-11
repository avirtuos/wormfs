//! Common types for the StorageWatchdog component.

use std::time::{Duration, SystemTime};
use thiserror::Error;

// Re-export common ID types from file_store
pub use crate::file_store::types::{ChunkId, FileId, NodeId, StripeId};
pub use crate::metadata_store::types::DiskId;

/// Configuration for StorageWatchdog.
#[derive(Debug, Clone)]
pub struct Config {
    /// Interval between shallow checks
    pub shallow_check_interval: Duration,

    /// Interval between deep checks
    pub deep_check_interval: Duration,

    /// Maximum concurrent checks
    pub max_concurrent_checks: usize,

    /// Check timeout duration
    pub check_timeout: Duration,

    /// Enable automatic repair of detected issues
    pub enable_auto_repair: bool,

    /// Rate limit for checks (checks per second)
    pub max_checks_per_second: f64,
}

/// Errors that can occur during StorageWatchdog operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Check operation failed
    #[error("Check failed: {0}")]
    CheckFailed(String),

    /// Watchdog not running
    #[error("Watchdog is not running")]
    NotRunning,

    /// Watchdog already running
    #[error("Watchdog is already running")]
    AlreadyRunning,

    /// Task start failed
    #[error("Failed to start watchdog task: {0}")]
    TaskStartFailed(String),

    /// Task stop failed
    #[error("Failed to stop watchdog task: {0}")]
    TaskStopFailed(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result of a watchdog check.
#[derive(Debug, Clone)]
pub struct CheckResult {
    /// Whether the check passed
    pub passed: bool,

    /// Number of items checked
    pub items_checked: u64,

    /// Number of issues found
    pub issues_found: u64,

    /// List of consistency events (issues)
    pub events: Vec<ConsistencyEventType>,

    /// Time taken to perform check
    pub duration: Duration,
}

/// Types of consistency events.
#[derive(Debug, Clone)]
pub enum ConsistencyEventType {
    /// Chunk file not found
    ChunkMissing { chunk_id: ChunkId, node_id: NodeId },

    /// Chunk checksum mismatch
    ChunkCorrupt { chunk_id: ChunkId, node_id: NodeId },

    /// Storage node unreachable
    NodeUnreachable { node_id: NodeId },

    /// Disk I/O errors
    DiskFailed { disk_id: DiskId, node_id: NodeId },

    /// Stripe cannot be reconstructed
    StripeUnrecoverable {
        stripe_id: StripeId,
        file_id: FileId,
    },
}

/// Watchdog statistics.
#[derive(Debug, Clone)]
pub struct WatchdogStats {
    /// Total shallow checks performed
    pub total_shallow_checks: u64,

    /// Total deep checks performed
    pub total_deep_checks: u64,

    /// Total issues found
    pub total_issues_found: u64,

    /// Last shallow check time
    pub last_shallow_check: Option<SystemTime>,

    /// Last deep check time
    pub last_deep_check: Option<SystemTime>,

    /// Average shallow check duration
    pub avg_shallow_check_duration: Duration,

    /// Average deep check duration
    pub avg_deep_check_duration: Duration,

    /// Checks per second (current rate)
    pub checks_per_second: f64,
}
