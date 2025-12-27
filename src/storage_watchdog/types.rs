//! Common types for the StorageWatchdog component.
#![allow(dead_code)]

use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use thiserror::Error;

// Re-export common ID types from file_store
pub use crate::file_store::types::{ChunkId, FileId, NodeId, StripeId};
pub use crate::metadata_store::types::DiskId;

/// Configuration for StorageWatchdog.
#[derive(Debug, Clone)]
pub struct Config {
    /// Interval between shallow checks (default: 1 week)
    pub shallow_check_interval: Duration,

    /// Interval between deep checks (default: 1 month)
    pub deep_check_interval: Duration,

    /// Maximum concurrent repair operations
    pub max_concurrent_repairs: usize,

    /// Maximum retries for failed repairs
    pub max_repair_retries: usize,

    /// Delay between repair retries
    pub repair_retry_delay: Duration,

    /// Batch size for shallow checks
    pub shallow_check_batch_size: usize,

    /// Batch size for deep checks
    pub deep_check_batch_size: usize,

    /// Path to verification state database
    pub verification_state_path: std::path::PathBuf,

    /// Memory limit for ext-sort operations (default: 10MB)
    pub sort_memory_limit: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            shallow_check_interval: Duration::from_secs(7 * 24 * 60 * 60), // 1 week
            deep_check_interval: Duration::from_secs(30 * 24 * 60 * 60),   // 1 month
            max_concurrent_repairs: 5,
            max_repair_retries: 3,
            repair_retry_delay: Duration::from_secs(10),
            shallow_check_batch_size: 100,
            deep_check_batch_size: 10,
            verification_state_path: std::path::PathBuf::from("verification_state.redb"),
            sort_memory_limit: 10 * 1024 * 1024, // 10MB
        }
    }
}

/// Errors that can occur during StorageWatchdog operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Watchdog not running
    #[error("Watchdog is not running")]
    NotRunning,

    /// Not the Raft leader
    #[error("Not the leader")]
    NotLeader,

    /// Repair operation failed
    #[error("Repair failed: {0}")]
    RepairFailed(String),

    /// Metadata error
    #[error("Metadata error: {0}")]
    MetadataError(String),

    /// FileStore error
    #[error("FileStore error: {0}")]
    FileStoreError(String),

    /// Network error
    #[error("Network error: {0}")]
    NetworkError(String),

    /// Database error
    #[error("Database error: {0}")]
    DatabaseError(String),

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
    pub events: Vec<ConsistencyEvent>,

    /// Time taken to perform check
    pub duration: Duration,
}

/// Types of consistency events.
#[derive(Debug, Clone)]
pub enum ConsistencyEvent {
    /// Chunk is missing (shallow check)
    ChunkMissing {
        file_id: FileId,
        stripe_id: StripeId,
        chunk_id: ChunkId,
        node_id: NodeId,
    },

    /// Chunk is corrupt (deep check)
    ChunkCorrupt {
        file_id: FileId,
        stripe_id: StripeId,
        chunk_id: ChunkId,
        node_id: NodeId,
        reason: String,
    },

    /// Stripe checksum mismatch (deep check)
    StripeCorrupt {
        file_id: FileId,
        stripe_id: StripeId,
        reason: String,
    },

    /// Node unreachable
    NodeUnreachable {
        node_id: NodeId,
        affected_chunks: Vec<ChunkId>,
    },

    /// Disk failure
    DiskFailure {
        node_id: NodeId,
        disk_id: DiskId,
        affected_chunks: Vec<ChunkId>,
    },
}

/// Repair request for a stripe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairRequest {
    pub file_id: FileId,
    pub stripe_id: StripeId,
    pub priority: RepairPriority,
    pub created_at: SystemTime,
    pub retry_count: u32,
}

/// Priority levels for repair operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RepairPriority {
    Critical = 0, // Multiple chunks missing
    High = 1,     // Single chunk missing
    Medium = 2,   // Corrupt chunk
    Low = 3,      // Periodic verification
}

/// Watchdog status information.
#[derive(Debug, Clone)]
pub struct WatchdogStatus {
    pub is_running: bool,
    pub is_leader: bool,
    pub shallow_checks_completed: u64,
    pub deep_checks_completed: u64,
    pub repairs_completed: u64,
    pub repairs_failed: u64,
    pub pending_repairs: usize,
    pub last_shallow_check: Option<SystemTime>,
    pub last_deep_check: Option<SystemTime>,
}

/// Verification progress tracking.
#[derive(Debug, Clone)]
pub struct VerificationProgress {
    pub shallow_check_progress: CheckProgress,
    pub deep_check_progress: CheckProgress,
}

/// Progress information for a check cycle.
#[derive(Debug, Clone)]
pub struct CheckProgress {
    pub total_files: u64,
    pub checked_files: u64,
    pub total_stripes: u64,
    pub checked_stripes: u64,
    pub issues_found: u64,
    pub started_at: Option<SystemTime>,
    pub estimated_completion: Option<SystemTime>,
}

/// Internal state for a repair operation.
#[derive(Debug, Clone)]
pub(crate) struct RepairStatus {
    pub request: RepairRequest,
    pub started_at: SystemTime,
    pub retry_count: u32,
}

/// Internal watchdog state.
#[derive(Debug)]
pub(crate) struct WatchdogState {
    pub is_running: bool,
    pub shallow_check_position: Option<FileId>,
    pub deep_check_position: Option<FileId>,
    pub last_shallow_check: Option<SystemTime>,
    pub last_deep_check: Option<SystemTime>,
    pub active_repairs: HashMap<StripeId, RepairStatus>,
}

/// Watchdog statistics (deprecated - use WatchdogStatus instead).
#[deprecated(note = "Use WatchdogStatus instead")]
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
