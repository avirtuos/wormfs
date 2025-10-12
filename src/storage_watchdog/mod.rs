//! # StorageWatchdog Component
//!
//! StorageWatchdog continuously monitors the availability and durability of all files
//! stored in WormFS by validating chunks across the storage cluster.
//!
//! ## Responsibilities
//!
//! - Walking the MetadataStore to discover all files, stripes, and chunks
//! - Performing shallow checks (chunk presence verification)
//! - Performing deep checks (chunk integrity and stripe reconstruction)
//! - Detecting missing, corrupt, or inaccessible chunks
//! - Reporting consistency issues to StorageRaftMember
//! - Scheduling checks based on configurable intervals
//! - Prioritizing checks based on file importance and previous issues
//!
//! ## Check Types
//!
//! ### Shallow Check
//! Fast verification that only checks chunk presence:
//! - Query metadata for chunk location
//! - Verify chunk file exists on assigned node/disk
//! - Does NOT read or verify chunk contents
//! - Runs frequently (e.g., every 5 minutes)
//!
//! ### Deep Check
//! Thorough verification that validates data integrity:
//! - Read all chunks for a stripe
//! - Verify individual chunk checksums
//! - Reconstruct stripe using erasure coding
//! - Verify stripe checksum
//! - Runs infrequently (e.g., every 24 hours)
//!
//! ## Consistency Event Reporting
//!
//! When issues are detected, StorageWatchdog submits consistency events to
//! StorageRaftMember for coordinated recovery:
//!
//! - **ChunkMissing**: Chunk file not found on assigned storage
//! - **ChunkCorrupt**: Chunk checksum mismatch
//! - **NodeUnreachable**: Storage node not responding
//! - **DiskFailed**: Disk I/O errors detected
//! - **StripeUnrecoverable**: Insufficient chunks for reconstruction
//!
//! ## Check Scheduling
//!
//! StorageWatchdog maintains a work queue prioritized by:
//! 1. Files that previously had issues (highest priority)
//! 2. Files that haven't been checked recently
//! 3. Files with high importance/access frequency
//!
//! ## Performance Considerations
//!
//! - Checks run continuously in background threads
//! - Rate limiting prevents overwhelming storage nodes
//! - Batch operations for efficiency
//! - Checks can be paused during high load

pub mod types;

use async_trait::async_trait;
pub use types::{
    CheckResult, ChunkId, Config, ConsistencyEventType, Error, FileId, NodeId, StripeId,
    WatchdogStats,
};

/// StorageWatchdog trait defines the interface for data integrity monitoring.
///
/// Implementations continuously verify chunk availability and integrity
/// across the storage cluster.
#[async_trait]
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock(
    type ConsistencyEvent = ();
))]
pub trait StorageWatchdog: Send + Sync {
    /// Event type for consistency issues
    type ConsistencyEvent: Send + Sync;

    /// Create a new StorageWatchdog.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration including check intervals and thresholds
    ///
    /// # Returns
    ///
    /// A new StorageWatchdog instance.
    fn new(config: Config) -> Result<Self, Error>
    where
        Self: Sized;

    /// Start the watchdog monitoring loops.
    ///
    /// This method starts background tasks that continuously check data integrity.
    /// It returns immediately after starting the tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if tasks cannot be started.
    async fn start(&self) -> Result<(), Error>;

    /// Stop the watchdog monitoring loops.
    ///
    /// This method gracefully stops all background tasks and waits for
    /// in-flight checks to complete.
    ///
    /// # Errors
    ///
    /// Returns an error if tasks cannot be stopped cleanly.
    async fn stop(&self) -> Result<(), Error>;

    /// Pause watchdog operations.
    ///
    /// Temporarily pauses all checks without stopping background tasks.
    /// Useful during high load or maintenance windows.
    async fn pause(&self) -> Result<(), Error>;

    /// Resume watchdog operations after pause.
    async fn resume(&self) -> Result<(), Error>;

    /// Check if watchdog is currently running.
    ///
    /// # Returns
    ///
    /// `true` if watchdog is active, `false` otherwise.
    fn is_running(&self) -> bool;

    /// Perform a shallow check on a specific file.
    ///
    /// # Arguments
    ///
    /// * `file_id` - File to check
    ///
    /// # Returns
    ///
    /// Check result indicating any issues found.
    ///
    /// # Errors
    ///
    /// Returns an error if check cannot be performed.
    async fn shallow_check_file(&self, file_id: FileId) -> Result<CheckResult, Error>;

    /// Perform a deep check on a specific file.
    ///
    /// # Arguments
    ///
    /// * `file_id` - File to check
    ///
    /// # Returns
    ///
    /// Check result indicating any issues found.
    ///
    /// # Errors
    ///
    /// Returns an error if check cannot be performed.
    async fn deep_check_file(&self, file_id: FileId) -> Result<CheckResult, Error>;

    /// Perform a shallow check on a specific stripe.
    ///
    /// # Arguments
    ///
    /// * `stripe_id` - Stripe to check
    ///
    /// # Returns
    ///
    /// Check result indicating any issues found.
    async fn shallow_check_stripe(&self, stripe_id: StripeId) -> Result<CheckResult, Error>;

    /// Perform a deep check on a specific stripe.
    ///
    /// # Arguments
    ///
    /// * `stripe_id` - Stripe to check
    ///
    /// # Returns
    ///
    /// Check result indicating any issues found.
    async fn deep_check_stripe(&self, stripe_id: StripeId) -> Result<CheckResult, Error>;

    /// Get watchdog statistics.
    ///
    /// # Returns
    ///
    /// Statistics about checks performed and issues found.
    fn get_stats(&self) -> WatchdogStats;

    /// Cleanup orphaned staged chunks (background task).
    ///
    /// Scans for chunks in "staged" state that are older than 1 hour and not
    /// tracked in any metadata records. These are chunks that were staged for
    /// a write operation but the metadata transaction never completed (due to
    /// failures, crashes, or client abandonment).
    ///
    /// The 1-hour threshold ensures that no in-flight transactions are affected
    /// by the cleanup process. Since staged chunks are only tracked in the
    /// Leader's memory during a transaction, this cleanup must be conservative
    /// to avoid race conditions.
    ///
    /// This method is typically called periodically by a background task.
    ///
    /// # Returns
    ///
    /// Number of orphaned staged chunks that were deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Filesystem scan fails
    /// - Deletion operations fail
    async fn cleanup_orphaned_staged_chunks(&self) -> Result<u64, Error>;
}
