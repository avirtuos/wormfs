//! # TransactionLogStore Component
//!
//! TransactionLogStore manages the persistent Raft transaction log for WormFS.
//!
//! ## Responsibilities
//!
//! - Durably storing Raft log entries (append operations)
//! - Providing log entry retrieval for replication and recovery
//! - Supporting log trimming after snapshot creation
//! - Maintaining log integrity with checksums
//! - Providing efficient sequential and random access to log entries
//! - Managing log file rotation and compaction
//!
//! ## Storage Backend
//!
//! TransactionLogStore uses `redb` (a pure Rust embedded database) for reliable,
//! high-performance log storage. redb provides:
//! - ACID transactions
//! - Zero-copy reads
//! - Crash safety with write-ahead logging
//! - Efficient range queries
//!
//! ## Log Operations
//!
//! ### Append
//! New log entries are appended sequentially with monotonically increasing indices.
//! Each append is immediately fsynced to ensure durability before acknowledging.
//!
//! ### Read
//! Log entries can be read by:
//! - Single index lookup
//! - Range query (start_index to end_index)
//! - Scanning from a given index forward
//!
//! ### Trim
//! After snapshot creation, old log entries can be trimmed to free space.
//! Trimming removes entries up to (but not including) the snapshot index.
//!
//! ## Log Entry Format
//!
//! Each log entry contains:
//! - Index: Monotonically increasing log position
//! - Term: Raft term when entry was created
//! - Data: Serialized operation (TransactionPrepare, TransactionCommit, etc.)
//! - Checksum: CRC32 of the entry for integrity verification
//!
//! ## File Organization
//!
//! ```text
//! /var/lib/wormfs/transaction_log/
//!   └── log.redb  (redb database file)
//! ```

pub mod factory;
pub mod implementation;
pub mod types;

use async_trait::async_trait;
pub use factory::TransactionLogStoreFactory;
pub use implementation::{TransactionLogStoreImpl, VoteData};
pub use types::{IntegrityReport, LogEntry, LogError, LogStats, TransactionLogConfig};

/// TransactionLogStore trait defines the interface for Raft log persistence.
///
/// Implementations provide durable storage and retrieval of Raft log entries.
#[async_trait]
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock)]
pub trait TransactionLogStore: Send + Sync {
    /// Create a new TransactionLogStore.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration including log file path
    ///
    /// # Returns
    ///
    /// A new TransactionLogStore instance.
    fn new(config: TransactionLogConfig) -> Result<Self, LogError>
    where
        Self: Sized;

    /// Append a log entry at a specific index.
    ///
    /// This method durably writes a log entry and fsyncs before returning.
    /// The caller (Raft leader) must specify the exact index. If an entry already
    /// exists at this index with a different term, it will be overwritten.
    ///
    /// # Arguments
    ///
    /// * `index` - Log index for this entry (must be specified by Raft leader)
    /// * `term` - Raft term
    /// * `data` - Entry data (serialized operation)
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Index is 0 (invalid)
    /// - Index creates unexpected gap in log
    /// - Write fails
    /// - Fsync fails
    /// - Disk is full
    async fn append(&self, index: u64, term: u64, data: Vec<u8>) -> Result<(), LogError>;

    /// Append multiple log entries atomically at specific indices.
    ///
    /// All entries are written in a single transaction and fsynced together.
    /// The caller (Raft leader) must specify exact indices for each entry.
    /// If entries already exist at these indices with different terms, they will be overwritten.
    ///
    /// # Arguments
    ///
    /// * `entries` - Vector of (index, term, data) tuples
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Batch is empty
    /// - Any index is 0 (invalid)
    /// - Indices create unexpected gaps
    /// - Any write fails (all entries are rolled back)
    async fn append_batch(&self, entries: Vec<(u64, u64, u64, Vec<u8>)>) -> Result<(), LogError>;

    /// Get a log entry by index.
    ///
    /// # Arguments
    ///
    /// * `index` - Log entry index
    ///
    /// # Returns
    ///
    /// The log entry if found.
    ///
    /// # Errors
    ///
    /// Returns an error if entry not found or read fails.
    async fn get_entry(&self, index: u64) -> Result<LogEntry, LogError>;

    /// Get a range of log entries.
    ///
    /// # Arguments
    ///
    /// * `start_index` - First index to retrieve (inclusive)
    /// * `end_index` - Last index to retrieve (inclusive)
    ///
    /// # Returns
    ///
    /// Vector of log entries in the range.
    ///
    /// # Errors
    ///
    /// Returns an error if read fails.
    async fn get_entries(
        &self,
        start_index: u64,
        end_index: u64,
    ) -> Result<Vec<LogEntry>, LogError>;

    /// Get the last log entry.
    ///
    /// # Returns
    ///
    /// The last log entry if log is not empty.
    ///
    /// # Errors
    ///
    /// Returns an error if log is empty.
    async fn get_last_entry(&self) -> Result<LogEntry, LogError>;

    /// Get the index of the last log entry.
    ///
    /// # Returns
    ///
    /// The last log index, or 0 if log is empty.
    fn get_last_index(&self) -> u64;

    /// Get the index of the first log entry.
    ///
    /// # Returns
    ///
    /// The first log index, or 0 if log is empty.
    fn get_first_index(&self) -> u64;

    /// Trim log entries up to (but not including) the specified index.
    ///
    /// This method is called after snapshot creation to free space.
    /// Entries before `up_to_index` are deleted.
    ///
    /// # Arguments
    ///
    /// * `up_to_index` - Trim entries before this index
    ///
    /// # Returns
    ///
    /// Number of entries trimmed.
    ///
    /// # Errors
    ///
    /// Returns an error if trim fails.
    async fn trim(&self, up_to_index: u64) -> Result<u64, LogError>;

    /// Delete log entries from the specified index onwards.
    ///
    /// This method is critical for Raft consensus correctness. When a follower
    /// receives log entries from a leader that conflict with its own log, it must
    /// delete all entries from the conflict point onwards before appending the
    /// leader's entries.
    ///
    /// This handles split-brain scenarios where a follower may have uncommitted
    /// entries from a previous leader that are now invalid.
    ///
    /// # Arguments
    ///
    /// * `from_index` - Delete entries from this index onwards (inclusive)
    ///
    /// # Returns
    ///
    /// Number of entries deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if deletion fails.
    ///
    /// # Example
    ///
    /// ```text
    /// Log state: [1, 2, 3, 4, 5]
    /// delete_from(3) => [1, 2]
    /// Returns: 3 (deleted entries 3, 4, 5)
    /// ```
    async fn delete_from(&self, from_index: u64) -> Result<u64, LogError>;

    /// Compact the transaction log database.
    ///
    /// This operation defragments the database file and reclaims space from
    /// deleted entries. Compaction is recommended after significant trim()
    /// or delete_from() operations, or when the database size exceeds the
    /// compact_threshold_mb configuration.
    ///
    /// The operation is performed online (database remains accessible) and
    /// updates the last_compaction timestamp in statistics.
    ///
    /// # Returns
    ///
    /// The amount of space reclaimed in bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if compaction fails.
    ///
    /// # Note
    ///
    /// Compaction can be expensive for large databases. Consider running
    /// it during low-traffic periods or when database size exceeds threshold.
    async fn compact(&self) -> Result<u64, LogError>;

    /// Verify the integrity of the transaction log.
    ///
    /// This operation scans the entire log to:
    /// - Detect gaps in the log sequence (missing indices)
    /// - Verify CRC32 checksums for all entries
    /// - Report overall log health
    ///
    /// Gaps in the log sequence are allowed (e.g., after trim operations)
    /// and are reported but don't make the log invalid. Checksum failures
    /// indicate corruption and make the log invalid.
    ///
    /// # Returns
    ///
    /// An IntegrityReport containing:
    /// - total_entries: Number of entries checked
    /// - missing_indices: Vec of gaps found in the sequence
    /// - is_valid: true if all checksums are valid
    ///
    /// # Errors
    ///
    /// Returns an error if the verification process itself fails
    /// (not if corruption is found - that's reported in the IntegrityReport).
    ///
    /// # Performance
    ///
    /// This is an expensive operation that reads every entry. Run it
    /// during maintenance windows or low-traffic periods.
    async fn verify_integrity(&self) -> Result<IntegrityReport, LogError>;

    /// Get transaction log statistics.
    ///
    /// # Returns
    ///
    /// Statistics about log storage usage.
    fn get_stats(&self) -> LogStats;
}
