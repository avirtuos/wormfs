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
    CheckProgress, CheckResult, ChunkId, Config, ConsistencyEvent, Error, FileId, NodeId,
    RepairPriority, RepairRequest, StripeId, VerificationProgress, WatchdogStatus,
};

/// StorageWatchdog trait defines the interface for data integrity monitoring.
///
/// The watchdog runs only on the Raft leader node and performs:
/// - Shallow checks: Fast verification of chunk presence
/// - Deep checks: Thorough validation including checksums and stripe reconstruction
/// - Repair coordination: Managing the repair queue and executing repairs
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock)]
#[async_trait]
pub trait StorageWatchdog: Send + Sync {
    /// Create a new StorageWatchdog.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration including check intervals and repair settings
    ///
    /// # Returns
    ///
    /// A new StorageWatchdog instance.
    ///
    /// # Errors
    ///
    /// Returns an error if initialization fails.
    fn new(config: Config) -> Result<Self, Error>
    where
        Self: Sized;

    /// Start watchdog tasks.
    ///
    /// This method should be called when the node becomes the Raft leader.
    /// It starts background tasks for:
    /// - Shallow check loop
    /// - Deep check loop
    /// - Repair queue processing
    ///
    /// # Errors
    ///
    /// Returns an error if tasks cannot be started.
    async fn start(&self) -> Result<(), Error>;

    /// Stop watchdog tasks.
    ///
    /// This method should be called when the node loses Raft leadership.
    /// It gracefully stops all background tasks and saves state.
    ///
    /// # Errors
    ///
    /// Returns an error if tasks cannot be stopped cleanly.
    async fn stop(&self) -> Result<(), Error>;

    /// Submit a consistency event for processing.
    ///
    /// This method can be called by any node to report issues detected locally.
    /// The event will be queued for repair if the node is the leader.
    ///
    /// # Arguments
    ///
    /// * `event` - The consistency event to process
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be processed.
    async fn submit_event(&self, event: ConsistencyEvent) -> Result<(), Error>;

    /// Get watchdog status and metrics.
    ///
    /// # Returns
    ///
    /// Current status including check progress and repair queue size.
    fn get_status(&self) -> WatchdogStatus;

    /// Manually trigger a shallow check cycle.
    ///
    /// This forces a shallow check to start immediately rather than waiting
    /// for the scheduled interval. Useful for testing or manual verification.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Node is not the leader
    /// - Check cannot be started
    async fn trigger_shallow_check(&self) -> Result<(), Error>;

    /// Manually trigger a deep check cycle.
    ///
    /// This forces a deep check to start immediately rather than waiting
    /// for the scheduled interval. Useful for testing or manual verification.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Node is not the leader
    /// - Check cannot be started
    async fn trigger_deep_check(&self) -> Result<(), Error>;

    /// Get verification progress for ongoing checks.
    ///
    /// # Returns
    ///
    /// Progress information for both shallow and deep check cycles.
    fn get_verification_progress(&self) -> VerificationProgress;

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
