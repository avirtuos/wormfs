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

pub mod factory;
pub mod implementation;
pub mod types;

use async_trait::async_trait;
pub use factory::SnapshotStoreFactory;
pub use implementation::SnapshotStoreImpl;
use std::path::Path;
pub use types::{
    CompressionAlgorithm, Config, Error, RetentionPolicy, SnapshotInfo, SnapshotReader,
    SnapshotStats,
};

/// SnapshotStore trait defines the interface for snapshot management.
///
/// Implementations handle storage, retrieval, and lifecycle management
/// of metadata snapshots for Raft log compaction.
///
/// ## Architecture: Client Pattern with Interior Mutability
///
/// SnapshotStore uses a client/server pattern with interior mutability:
/// - Multiple components can hold cloned instances (cheap Arc clones)
/// - OpenRaft can "own" an instance while other components access concurrently
/// - Thread-safe via interior mutability (RwLock)
/// - Read-optimized for concurrent snapshot streaming
#[async_trait]
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock)]
pub trait SnapshotStore: Send + Sync {
    /// Initialize snapshot storage directory.
    ///
    /// Scans the snapshot directory on disk to rebuild the in-memory registry.
    /// This method should be called once during component initialization.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Storage directory cannot be created or accessed
    /// - Existing snapshots cannot be scanned or loaded
    async fn initialize(&self) -> Result<(), Error>;

    /// Ingest a new snapshot.
    ///
    /// Called by StorageRaftMember after MetadataStore creates a snapshot.
    /// The snapshot file is copied to the snapshot directory and metadata is recorded.
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - Sequential snapshot identifier
    /// * `log_index` - Raft log index at snapshot time
    /// * `log_term` - Raft term at snapshot time
    /// * `metadata_db_path` - Path to the snapshot file to ingest
    ///
    /// # Returns
    ///
    /// Snapshot information for the ingested snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Snapshot file cannot be read
    /// - Checksum calculation fails
    /// - Snapshot directory is full
    /// - I/O error occurs
    async fn ingest_snapshot(
        &self,
        snapshot_id: u64,
        log_index: u64,
        log_term: u64,
        snapshot_leader_node_id: u64,
        metadata_db_path: &Path,
        membership_log_index: Option<u64>,
        membership_log_term: Option<u64>,
        membership_leader_node_id: Option<u64>,
        membership_config: String,
    ) -> Result<SnapshotInfo, Error>;

    /// Get the latest snapshot.
    ///
    /// # Returns
    ///
    /// The latest snapshot information, or None if no snapshots exist.
    async fn get_latest_snapshot(&self) -> Result<Option<SnapshotInfo>, Error>;

    /// Get a specific snapshot by ID.
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - Snapshot identifier
    ///
    /// # Returns
    ///
    /// Snapshot information.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot not found.
    async fn get_snapshot(&self, snapshot_id: u64) -> Result<SnapshotInfo, Error>;

    /// Get snapshot at or before a specific log index.
    ///
    /// # Arguments
    ///
    /// * `log_index` - Raft log index
    ///
    /// # Returns
    ///
    /// The latest snapshot at or before the given index, or None if no such snapshot exists.
    async fn get_snapshot_at_index(&self, log_index: u64) -> Result<Option<SnapshotInfo>, Error>;

    /// List all available snapshots.
    ///
    /// # Returns
    ///
    /// Vector of snapshot information ordered by snapshot ID.
    async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>, Error>;

    /// Open a snapshot for reading.
    ///
    /// Returns a SnapshotReader that provides access to the snapshot's metadata database.
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - Snapshot identifier
    ///
    /// # Returns
    ///
    /// SnapshotReader for accessing snapshot data.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot not found or cannot be opened.
    async fn open_snapshot(&self, snapshot_id: u64) -> Result<SnapshotReader, Error>;

    /// Stream snapshot to a remote node.
    ///
    /// Streams the snapshot's metadata database to the provided sink.
    /// Used for transferring snapshots between nodes.
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - Snapshot identifier
    /// * `sink` - Async writer to stream snapshot data to
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Snapshot not found
    /// - I/O error during streaming
    async fn stream_snapshot(
        &self,
        snapshot_id: u64,
        sink: Box<dyn tokio::io::AsyncWrite + Unpin + Send>,
    ) -> Result<(), Error>;

    /// Receive and store a snapshot from a remote node.
    ///
    /// Receives snapshot data from the provided source, stores it,
    /// and updates the registry.
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - Snapshot identifier
    /// * `log_index` - Raft log index at snapshot time
    /// * `log_term` - Raft term at snapshot time
    /// * `source` - Async reader to receive snapshot data from
    ///
    /// # Returns
    ///
    /// Snapshot information for the received snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - I/O error during reception
    /// - Checksum validation fails
    async fn receive_snapshot(
        &self,
        snapshot_id: u64,
        log_index: u64,
        log_term: u64,
        source: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
    ) -> Result<SnapshotInfo, Error>;

    /// Verify snapshot integrity.
    ///
    /// Validates the snapshot's checksum against stored metadata.
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - Snapshot identifier
    ///
    /// # Returns
    ///
    /// True if snapshot is valid, false otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot not found.
    async fn verify_snapshot(&self, snapshot_id: u64) -> Result<bool, Error>;

    /// Prune old snapshots based on retention policy.
    ///
    /// Automatically triggered when new snapshots are created.
    /// Deletes snapshots that exceed retention policy while respecting minimum retention.
    ///
    /// # Returns
    ///
    /// Vector of snapshot IDs that were deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if deletion fails.
    async fn prune_snapshots(&self) -> Result<Vec<u64>, Error>;

    /// Delete a specific snapshot.
    ///
    /// Removes the snapshot and all associated files.
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - Snapshot identifier
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Snapshot not found
    /// - Deletion fails
    async fn delete_snapshot(&self, snapshot_id: u64) -> Result<(), Error>;

    /// Get snapshot storage statistics.
    ///
    /// # Returns
    ///
    /// Statistics about snapshot storage usage.
    fn get_stats(&self) -> SnapshotStats;
}
