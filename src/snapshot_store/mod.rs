//! # SnapshotStore Component
//!
//! SnapshotStore manages metadata snapshots for Raft log compaction and recovery.
//!
//! ## Responsibilities
//!
//! - Ingesting (storing) metadata snapshots triggered by StorageRaftMember
//! - Managing snapshot lifecycle and retention policies
//! - Providing snapshot retrieval for node recovery and catch-up
//! - Pruning old snapshots based on age and transaction log coverage
//! - Tracking snapshot metadata (transaction ID, index, timestamp)
//! - Supporting efficient snapshot transfer between nodes
//!
//! ## Snapshot Workflow
//!
//! ### Creation Flow
//! 1. StorageRaftMember triggers snapshot creation
//! 2. MetadataStore creates transactionally consistent backup
//! 3. StorageRaftMember calls `ingest_snapshot()` with snapshot file
//! 4. SnapshotStore stores file and updates internal state
//!
//! ### Retrieval Flow
//! 1. Node needs to catch up (fallen behind on transaction log)
//! 2. Node requests latest snapshot via StorageEndpoint
//! 3. SnapshotStore provides snapshot file and metadata
//! 4. Node restores MetadataStore from snapshot
//! 5. Node replays transaction log from snapshot point forward
//!
//! ### Pruning Flow
//! 1. Leader determines snapshots to prune based on policy
//! 2. Leader triggers `prune_snapshots()` on all nodes
//! 3. SnapshotStore deletes old snapshot files from disk
//! 4. SnapshotStore updates internal state
//!
//! ## Snapshot Format
//!
//! Snapshots are stored as files with accompanying metadata:
//! - Snapshot file: SQLite database backup
//! - Metadata file: JSON with snapshot info (transaction ID, index, timestamp)
//!
//! ## Storage Organization
//!
//! ```text
//! /var/lib/wormfs/snapshots/
//!   ├── snapshot_001234.db
//!   ├── snapshot_001234.json
//!   ├── snapshot_005678.db
//!   └── snapshot_005678.json
//! ```

pub mod types;

use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
pub use types::{Config, Error, SnapshotMetadata, SnapshotStats};

/// SnapshotStore trait defines the interface for snapshot management.
///
/// Implementations handle storage, retrieval, and lifecycle management
/// of metadata snapshots for Raft log compaction.
#[async_trait]
pub trait SnapshotStore: Send + Sync {
    /// Create a new SnapshotStore.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration including snapshot directory path
    ///
    /// # Returns
    ///
    /// A new SnapshotStore instance.
    fn new(config: Config) -> Result<Self, Error>
    where
        Self: Sized;

    /// Ingest a metadata snapshot.
    ///
    /// This method is called by StorageRaftMember after a snapshot has been
    /// created. The snapshot file is copied to the snapshot directory and
    /// metadata is recorded.
    ///
    /// # Arguments
    ///
    /// * `snapshot_path` - Path to the snapshot file to ingest
    /// * `tx_index` - Transaction log index at snapshot time
    /// * `timestamp` - When snapshot was created
    ///
    /// # Returns
    ///
    /// Snapshot ID for the ingested snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Snapshot file cannot be read
    /// - Snapshot directory is full
    /// - I/O error occurs
    async fn ingest_snapshot(
        &self,
        snapshot_path: &Path,
        tx_index: u64,
        timestamp: SystemTime,
    ) -> Result<u64, Error>;

    /// Get the latest snapshot.
    ///
    /// # Returns
    ///
    /// Path to the latest snapshot file and its metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if no snapshots exist.
    async fn get_latest_snapshot(&self) -> Result<(PathBuf, SnapshotMetadata), Error>;

    /// Get a specific snapshot by transaction index.
    ///
    /// # Arguments
    ///
    /// * `tx_index` - Transaction log index
    ///
    /// # Returns
    ///
    /// Path to the snapshot file and its metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot not found.
    async fn get_snapshot_at_index(
        &self,
        tx_index: u64,
    ) -> Result<(PathBuf, SnapshotMetadata), Error>;

    /// List all available snapshots.
    ///
    /// # Returns
    ///
    /// Vector of snapshot metadata ordered by transaction index.
    async fn list_snapshots(&self) -> Result<Vec<SnapshotMetadata>, Error>;

    /// Prune old snapshots based on retention policy.
    ///
    /// This method deletes snapshots that are no longer needed based on:
    /// - Age threshold
    /// - Minimum number of snapshots to retain
    /// - Transaction log coverage
    ///
    /// # Arguments
    ///
    /// * `keep_latest` - Number of latest snapshots to always keep
    /// * `older_than` - Delete snapshots older than this time
    ///
    /// # Returns
    ///
    /// Number of snapshots pruned.
    ///
    /// # Errors
    ///
    /// Returns an error if deletion fails.
    async fn prune_snapshots(
        &self,
        keep_latest: usize,
        older_than: Option<SystemTime>,
    ) -> Result<u64, Error>;

    /// Get snapshot storage statistics.
    ///
    /// # Returns
    ///
    /// Statistics about snapshot storage usage.
    fn get_stats(&self) -> SnapshotStats;
}
