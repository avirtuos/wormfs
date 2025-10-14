//! # WormValidator Types
//!
//! Core data structures and error types for the WormValidator component.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Configuration for the WormValidator.
#[derive(Debug, Clone)]
pub struct ValidatorConfig {
    /// Temporary directory for test data
    pub temp_dir: PathBuf,
    /// Enable verbose logging
    pub verbose: bool,
    /// Keep test data after completion
    pub keep_data: bool,
    /// Specific scenarios to run (None = all)
    pub scenarios: Option<Vec<String>>,
    /// Path to write test report
    pub report_path: Option<PathBuf>,
    /// Enable benchmark mode
    pub benchmark_mode: bool,
    /// Cluster configuration
    pub cluster_config: ClusterConfig,
    /// Client configuration
    pub client_config: ClientConfig,
}

impl Default for ValidatorConfig {
    fn default() -> Self {
        Self {
            temp_dir: PathBuf::from("/tmp/wormfs-validator"),
            verbose: false,
            keep_data: false,
            scenarios: None,
            report_path: None,
            benchmark_mode: false,
            cluster_config: ClusterConfig::default(),
            client_config: ClientConfig::default(),
        }
    }
}

/// Configuration for the embedded storage cluster.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Raft heartbeat interval in milliseconds
    pub raft_heartbeat_ms: u64,
    /// Metadata store path (relative to temp_dir)
    pub metadata_store_path: PathBuf,
    /// File store path (relative to temp_dir)
    pub file_store_path: PathBuf,
    /// Snapshot store path (relative to temp_dir)
    pub snapshot_store_path: PathBuf,
    /// Transaction log path (relative to temp_dir)
    pub transaction_log_path: PathBuf,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            raft_heartbeat_ms: 100,
            metadata_store_path: PathBuf::from("metadata.db"),
            file_store_path: PathBuf::from("filestore"),
            snapshot_store_path: PathBuf::from("snapshots"),
            transaction_log_path: PathBuf::from("txlog"),
        }
    }
}

/// Configuration for the FUSE client simulator.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Endpoint address
    pub endpoint: String,
    /// Request timeout in seconds
    pub timeout_secs: u64,
    /// Maximum number of retries
    pub max_retries: u32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            endpoint: "127.0.0.1:7000".to_string(),
            timeout_secs: 30,
            max_retries: 3,
        }
    }
}

/// Results from a test run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TestResults {
    /// Total number of scenarios
    pub total_scenarios: usize,
    /// Number of passed scenarios
    pub passed: usize,
    /// Number of failed scenarios
    pub failed: usize,
    /// Number of skipped scenarios
    pub skipped: usize,
    /// Total duration of test run
    pub duration: Duration,
    /// Individual scenario results
    pub scenario_results: Vec<ScenarioResult>,
}

impl TestResults {
    /// Check if all tests passed.
    pub fn all_passed(&self) -> bool {
        self.failed == 0 && self.passed == self.total_scenarios
    }

    /// Get success rate as a percentage.
    pub fn success_rate(&self) -> f64 {
        if self.total_scenarios == 0 {
            return 0.0;
        }
        (self.passed as f64 / self.total_scenarios as f64) * 100.0
    }
}

/// Result from a single test scenario.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ScenarioResult {
    /// Scenario name
    pub name: String,
    /// Scenario category
    pub category: String,
    /// Test status
    pub status: TestStatus,
    /// Execution duration
    pub duration: Duration,
    /// Error message if failed
    pub error: Option<String>,
    /// Performance metrics
    pub metrics: HashMap<String, f64>,
}

/// Status of a test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TestStatus {
    /// Test passed successfully
    Passed,
    /// Test failed
    Failed,
    /// Test was skipped
    Skipped,
}

/// Errors that can occur in the validator.
#[derive(Debug, Error)]
pub enum ValidatorError {
    #[error("Cluster startup failed: {0}")]
    ClusterStartupFailed(String),

    #[error("Client connection failed: {0}")]
    ClientConnectionFailed(String),

    #[error("Test scenario failed: {0}")]
    TestScenarioFailed(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Cluster not started")]
    ClusterNotStarted,

    #[error("Cleanup failed: {0}")]
    CleanupFailed(String),
}

/// File handle type used by the client simulator.
pub type FileHandle = u64;

/// File ID type.
pub type FileId = String;

/// Lock ID type.
pub type LockId = String;

/// Lock type for file locking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    /// Shared read lock
    Read,
    /// Exclusive write lock
    Write,
}

/// File attributes for metadata operations.
#[derive(Debug, Clone)]
pub struct FileAttr {
    /// File size in bytes
    pub size: u64,
    /// File permissions (Unix mode)
    pub mode: u32,
    /// Owner user ID
    pub uid: u32,
    /// Owner group ID
    pub gid: u32,
    /// Access time (seconds since epoch)
    pub atime: u64,
    /// Modification time (seconds since epoch)
    pub mtime: u64,
    /// Creation time (seconds since epoch)
    pub ctime: u64,
}

/// Directory entry for readdir operations.
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// Entry name
    pub name: String,
    /// File ID
    pub file_id: FileId,
    /// Entry type
    pub entry_type: EntryType,
}

/// Type of directory entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    /// Regular file
    File,
    /// Directory
    Directory,
}
