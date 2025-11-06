//! Common types for the StorageRaftMember component.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use thiserror::Error;

use crate::storage_network::NetworkHandleTrait;

/// Unique identifier for a node in the Raft cluster.
///
/// NodeId is used to identify members of the Raft cluster and track
/// cluster membership changes.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
)]
pub struct NodeId(pub u64);

impl NodeId {
    /// Create a new NodeId from a u64 value.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the inner u64 value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node({})", self.0)
    }
}

/// ClusterManager configuration preset.
///
/// Defines the aggressiveness of automatic failure detection and recovery:
/// - Conservative: Slower to react, fewer false positives
/// - Moderate: Balanced approach (recommended for most deployments)
/// - Aggressive: Fast reaction, may have more false positives
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterManagerPreset {
    /// Conservative: 30s heartbeat timeout, 120s min membership change interval
    Conservative,
    /// Moderate: 15s heartbeat timeout, 60s min membership change interval (default)
    Moderate,
    /// Aggressive: 5s heartbeat timeout, 30s min membership change interval
    Aggressive,
}

impl Default for ClusterManagerPreset {
    fn default() -> Self {
        Self::Moderate
    }
}

/// Raft configuration options.
///
/// These parameters control Raft's behavior for elections, log replication,
/// snapshot management, and transaction coordination.
#[derive(Clone)]
pub struct Config {
    // === Election Configuration ===
    /// Interval between heartbeats from leader to followers
    pub heartbeat_interval: Duration,

    /// Minimum election timeout duration
    pub election_timeout_min: Duration,

    /// Maximum election timeout duration
    pub election_timeout_max: Duration,

    // === Log Replication Configuration ===
    /// Maximum number of log entries per AppendEntries RPC
    pub max_payload_entries: u64,

    /// Maximum number of in-flight AppendEntries RPCs per follower (pipeline optimization)
    pub max_in_flight_append_entries: usize,

    /// Threshold for considering a follower lagging
    pub replication_lag_threshold: u64,

    /// Maximum number of uncommitted log entries before rejecting new writes (backpressure)
    pub max_uncommitted_entries: u64,

    // === Snapshot Configuration ===
    /// Time threshold for triggering snapshot
    pub snapshot_time_threshold: Duration,

    /// Log size threshold (bytes) for triggering snapshot
    pub snapshot_log_size_threshold: u64,

    /// Enable zstd compression for snapshots
    pub enable_snapshot_compression: bool,

    /// Compression level for zstd (1-22, higher = better compression but slower)
    pub snapshot_compression_level: i32,

    // === Read Consistency Configuration ===
    /// Enable lease-based read optimization (currently unused, for future support)
    pub enable_lease_based_reads: bool,

    /// Duration of read lease (currently unused, for future support)
    pub lease_duration: Duration,

    /// Maximum staleness allowed for local reads (default: 120 seconds)
    pub max_read_staleness: Duration,

    // === Transaction Configuration ===
    /// Default timeout for transactions (can be overridden per-transaction)
    pub default_transaction_timeout: Duration,

    /// Maximum number of concurrent in-flight transactions
    pub max_concurrent_transactions: usize,

    /// Timeout for new leader to recover in-flight transactions after election
    pub transaction_recovery_timeout: Duration,

    // === Storage Dependencies ===
    /// Path to the transaction log database
    pub transaction_log_path: PathBuf,

    /// Path to the metadata database
    pub metadata_db_path: PathBuf,

    /// Directory for storing Raft snapshots
    pub snapshot_directory: PathBuf,

    /// Network address for this node (for peer communication)
    pub network_address: SocketAddr,

    /// Handle to the storage network for peer-to-peer communication
    /// This must be set before calling new() - there's no default
    /// Uses trait object to support both production (libp2p) and test (stub) networks
    pub storage_network: Option<Arc<dyn NetworkHandleTrait>>,

    // === Cluster Manager Configuration ===
    /// Enable ClusterManager for automatic failure detection and recovery (default: true)
    pub enable_cluster_manager: bool,

    /// ClusterManager configuration preset (default: Moderate)
    pub cluster_manager_preset: ClusterManagerPreset,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("election_timeout_min", &self.election_timeout_min)
            .field("election_timeout_max", &self.election_timeout_max)
            .field("max_payload_entries", &self.max_payload_entries)
            .field(
                "max_in_flight_append_entries",
                &self.max_in_flight_append_entries,
            )
            .field("replication_lag_threshold", &self.replication_lag_threshold)
            .field("max_uncommitted_entries", &self.max_uncommitted_entries)
            .field("snapshot_time_threshold", &self.snapshot_time_threshold)
            .field(
                "snapshot_log_size_threshold",
                &self.snapshot_log_size_threshold,
            )
            .field(
                "enable_snapshot_compression",
                &self.enable_snapshot_compression,
            )
            .field(
                "snapshot_compression_level",
                &self.snapshot_compression_level,
            )
            .field("enable_lease_based_reads", &self.enable_lease_based_reads)
            .field("lease_duration", &self.lease_duration)
            .field("max_read_staleness", &self.max_read_staleness)
            .field(
                "default_transaction_timeout",
                &self.default_transaction_timeout,
            )
            .field(
                "max_concurrent_transactions",
                &self.max_concurrent_transactions,
            )
            .field(
                "transaction_recovery_timeout",
                &self.transaction_recovery_timeout,
            )
            .field("transaction_log_path", &self.transaction_log_path)
            .field("metadata_db_path", &self.metadata_db_path)
            .field("snapshot_directory", &self.snapshot_directory)
            .field("network_address", &self.network_address)
            .field("storage_network", &self.storage_network.is_some())
            .field("enable_cluster_manager", &self.enable_cluster_manager)
            .field("cluster_manager_preset", &self.cluster_manager_preset)
            .finish()
    }
}

impl Default for Config {
    /// Create a default Raft configuration with values optimized for LAN deployments.
    ///
    /// These defaults are based on the design document (Section 2.4) and provide:
    /// - Fast leader election (<3 seconds)
    /// - Low-latency replication (<50ms)
    /// - Reasonable snapshot thresholds
    /// - Conservative transaction settings
    fn default() -> Self {
        Self {
            // Election Configuration (LAN-optimized)
            heartbeat_interval: Duration::from_millis(250),
            election_timeout_min: Duration::from_millis(1000),
            election_timeout_max: Duration::from_millis(2000),

            // Log Replication Configuration
            max_payload_entries: 1000,
            max_in_flight_append_entries: 10,
            replication_lag_threshold: 100,
            max_uncommitted_entries: 10000,

            // Snapshot Configuration
            snapshot_time_threshold: Duration::from_secs(24 * 3600), // 24 hours
            snapshot_log_size_threshold: 10 * 1024 * 1024,           // 10 MB
            enable_snapshot_compression: true,
            snapshot_compression_level: 3,

            // Read Consistency Configuration
            enable_lease_based_reads: false, // Not yet implemented
            lease_duration: Duration::from_secs(10),
            max_read_staleness: Duration::from_secs(120),

            // Transaction Configuration
            default_transaction_timeout: Duration::from_secs(300), // 5 minutes
            max_concurrent_transactions: 100,
            transaction_recovery_timeout: Duration::from_secs(60),

            // Storage Dependencies - placeholders that must be overridden
            transaction_log_path: PathBuf::from("/tmp/wormfs/tx_log.db"),
            metadata_db_path: PathBuf::from("/tmp/wormfs/metadata.db"),
            snapshot_directory: PathBuf::from("/tmp/wormfs/snapshots"),
            network_address: SocketAddr::from(([127, 0, 0, 1], 5000)),
            storage_network: None, // Must be set before calling new()

            // Cluster Manager Configuration
            enable_cluster_manager: true, // Enabled by default for automatic recovery
            cluster_manager_preset: ClusterManagerPreset::Moderate,
        }
    }
}

/// Errors that can occur during Raft operations.
#[derive(Error, Debug)]
pub enum Error {
    /// This node is not the leader
    #[error("Not the leader; try node {leader:?}")]
    NotLeader { leader: Option<NodeId> },

    /// Operation timed out
    #[error("Operation timed out after {timeout:?}")]
    Timeout { timeout: Duration },

    /// Insufficient nodes to form quorum
    #[error("No quorum available: {available} of {total} nodes")]
    NoQuorum { available: usize, total: usize },

    /// Node already exists in cluster
    #[error("Node {0:?} already exists in cluster")]
    NodeAlreadyExists(NodeId),

    /// Node not found in cluster
    #[error("Node {0:?} not found in cluster")]
    NodeNotFound(NodeId),

    /// Cannot remove node - would lose quorum
    #[error("Cannot remove node {0:?} - would lose quorum")]
    WouldLoseQuorum(NodeId),

    /// Local state incompatible with cluster
    #[error("Local state incompatible: {0}")]
    IncompatibleState(String),

    /// Failed to contact peers
    #[error("Failed to contact peers: {0}")]
    PeerContactFailed(String),

    /// Snapshot operation failed
    #[error("Snapshot failed: {0}")]
    SnapshotFailed(String),

    /// Membership change failed
    #[error("Membership change failed: {0}")]
    MembershipChangeFailed(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Storage error
    #[error("Storage error: {0}")]
    StorageError(String),

    /// Raft internal error
    #[error("Raft error: {0}")]
    RaftError(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Raft role in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole {
    /// This node is the current leader
    Leader,
    /// This node is a follower
    Follower,
    /// This node is a candidate in an election
    Candidate,
}

/// Information about a cluster member
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClusterMemberInfo {
    /// Node ID
    pub node_id: NodeId,

    /// Whether this node is a voter (true) or learner (false)
    pub is_voter: bool,
}

/// Raft metrics and status information.
#[derive(Debug, Clone)]
pub struct RaftMetrics {
    /// Current Raft term
    pub current_term: u64,

    /// Current role of this node
    pub role: RaftRole,

    /// Node ID of current leader (if known)
    pub leader_id: Option<NodeId>,

    /// Index of highest committed log entry
    pub commit_index: u64,

    /// Index of highest applied log entry
    pub last_applied: u64,

    /// Index of last log entry
    pub last_log_index: u64,

    /// Index of last snapshot
    pub snapshot_index: u64,

    /// Number of nodes in the cluster
    pub cluster_size: usize,

    /// List of all cluster members (voters and learners) - available on all nodes
    pub cluster_members: Vec<ClusterMemberInfo>,

    /// Replication lag per follower (leader only)
    pub replication_lag: HashMap<NodeId, u64>,

    /// Timestamp when leader last sent AppendEntries to each follower (leader only)
    pub heartbeat_sent: HashMap<NodeId, Instant>,

    /// Timestamp when leader received AppendEntriesResponse from each follower (leader only)
    pub heartbeat_acked: HashMap<NodeId, Instant>,
}

// ============================================================================
// Raft Operations and Transaction Types
// ============================================================================

/// Transaction ID for two-phase commit coordination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TxId(pub u64);

/// Operations that can be proposed through Raft consensus.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WormFsOperation {
    /// Prepare phase of two-phase commit transaction (legacy, prefer AtomicTransaction)
    TransactionPrepare {
        tx_id: TxId,
        metadata_ops: Option<Vec<MetadataOperation>>,
        command_ops: Option<Vec<CommandOperation>>,
        timeout: SystemTime,
    },
    /// Commit phase of two-phase commit transaction (legacy, prefer AtomicTransaction)
    TransactionCommit { tx_id: TxId },
    /// Abort phase of two-phase commit transaction (legacy, prefer AtomicTransaction)
    TransactionAbort { tx_id: TxId, reason: Option<String> },
    /// Atomic transaction - all operations commit together in a single Raft round
    ///
    /// This is the preferred way to execute transactions, as it leverages Raft's native
    /// atomicity guarantees without requiring separate prepare/commit phases. All operations
    /// in the list are applied atomically - either all succeed or the node panics to prevent
    /// state divergence.
    AtomicTransaction {
        /// Unique transaction identifier
        tx_id: TxId,
        /// List of metadata operations to apply atomically
        operations: Vec<MetadataOperation>,
        /// Transaction timeout (for cleanup if node crashes)
        timeout: SystemTime,
    },
}

/// Metadata operations that can be proposed through Raft.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum MetadataOperation {
    /// Create a new file
    FileCreate {
        path: PathBuf,
        inode: u64,
        metadata: FileMetadata,
        policy: StoragePolicy,
    },
    /// Update file metadata
    FileUpdate {
        file_id: FileId,
        inode: u64,
        metadata: FileMetadata,
        policy: StoragePolicy,
    },
    /// Delete a file
    FileDelete { file_id: FileId, inode: u64 },
    /// Create a stripe for a file
    CreateStripe {
        file_id: FileId,
        stripe_id: StripeId,
        stripe_index: u32,
        policy: StoragePolicy,
        offset: u64,
        size: u64,
        chunks: Vec<ChunkId>,
    },
    /// Delete a stripe
    DeleteStripe {
        stripe_id: StripeId,
        file_id: FileId,
    },
    /// Create a chunk location record
    CreateChunk {
        node_id: NodeId,
        disk: DiskId,
        chunk: ChunkId,
        chunk_index: ChunkIndex,
    },
    /// Move a chunk from one location to another
    MoveChunk {
        chunk_id: ChunkId,
        old_node: NodeId,
        new_node: NodeId,
        old_disk: DiskId,
        new_disk: DiskId,
    },
    /// Delete a chunk
    DeleteChunk {
        node_id: NodeId,
        disk_id: DiskId,
        chunk_id: ChunkId,
    },
    /// Acquire a read lock on a file
    AcquireReadLock {
        file_id: FileId,
        client_id: u64,
        expires_at: SystemTime,
    },
    /// Acquire a write lock on a file
    AcquireWriteLock {
        file_id: FileId,
        client_id: u64,
        node_id: u64,
        expires_at: SystemTime,
    },
    /// Release a lock
    ReleaseLock { file_id: FileId, client_id: u64 },
    /// Extend lock expiration time
    ExtendLock {
        file_id: FileId,
        client_id: u64,
        new_expiry: SystemTime,
    },
}

/// Command operations that can be proposed through Raft.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CommandOperation {
    /// Create a snapshot
    CreateSnapshot { snapshot_id: u64, index: u64 },
    /// Trim transaction log to a specific index
    TrimLog { trim_to_index: u64 },
    /// Add a new member to the cluster
    AddMember {
        node_id: NodeId,
        address: SocketAddr,
    },
    /// Remove a member from the cluster
    RemoveMember { node_id: NodeId },
}

// ============================================================================
// Metadata Change Subscription Types
// ============================================================================

#[derive(Debug, Clone)]
pub struct MetadataChangeEvent {
    pub committed_at: SystemTime,
    pub log_index: u64,
    pub changes: Vec<MetadataChange>,
}
/// Events that can be emitted when metadata changes are committed.
#[derive(Debug, Clone)]
pub enum MetadataChange {
    /// A new file was created
    FileCreated {
        file_id: FileId,
        inode: u64,
        path: PathBuf,
    },
    /// File attributes were updated
    FileUpdated {
        file_id: FileId,
        inode: u64,
        changed_attrs: FileAttributeChanges,
    },
    /// A file was deleted
    FileDeleted { file_id: FileId, inode: u64 },
    /// A new directory was created
    DirectoryCreated {
        inode: u64,
        path: PathBuf,
        parent_inode: u64,
    },
    /// A directory was deleted
    DirectoryDeleted { inode: u64, path: PathBuf },
    /// A new stripe was added to a file
    StripeCreated {
        file_id: FileId,
        stripe_id: StripeId,
        offset: u64,
        size: u64,
    },
    /// A stripe was deleted from a file
    StripeDeleted {
        file_id: FileId,
        stripe_id: StripeId,
    },
    /// A chunk was moved to a different location
    ChunkMoved {
        chunk_id: ChunkId,
        old_location: ChunkLocation,
        new_location: ChunkLocation,
    },
    /// A file lock was released or expired
    LockReleased { file_id: FileId, inode: u64 },
}

/// Types of metadata changes for subscription filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetadataChangeType {
    FileCreated,
    FileUpdated,
    FileDeleted,
    DirectoryCreated,
    DirectoryDeleted,
    StripeCreated,
    StripeDeleted,
    ChunkMoved,
    LockReleased,
}

/// Tracks which file attributes changed in an update.
#[derive(Debug, Clone)]
pub struct FileAttributeChanges {
    pub size: Option<u64>,
    pub mtime: Option<SystemTime>,
    pub atime: Option<SystemTime>,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
}

/// Location of a chunk on a specific node and disk.
#[derive(Debug, Clone)]
pub struct ChunkLocation {
    pub node_id: NodeId,
    pub disk_id: DiskId,
}

/// Trait for components that want to receive metadata change notifications.
pub trait MetadataChangeSubscriber: Send + Sync {
    /// Called when a metadata change event occurs.
    ///
    /// Implementations should not block as this is called from the Raft apply path.
    fn on_metadata_change(&self, event: MetadataChangeEvent);
}

// ============================================================================
// Re-export types from other modules
// ============================================================================

// Re-export ID types from file_store
pub use crate::file_store::types::{ChunkId, DiskId, FileId, NodeId as FileStoreNodeId, StripeId};

/// Chunk index within a stripe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChunkIndex(pub u32);

/// File metadata (simplified for Raft operations).
///
/// This is a serializable subset of metadata_store::FileMetadata
/// used for Raft log entries.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileMetadata {
    /// File size in bytes
    pub size: u64,
    /// Creation timestamp
    pub created: SystemTime,
    /// Last modified timestamp
    pub modified: SystemTime,
    /// File permissions (Unix-style)
    pub mode: u32,
}

impl From<FileMetadata> for crate::metadata_store::FileMetadata {
    fn from(fm: FileMetadata) -> Self {
        crate::metadata_store::FileMetadata {
            file_type: crate::metadata_store::FileType::RegularFile,
            size: fm.size,
            permissions: fm.mode,
            uid: 0, // Default values
            gid: 0,
            created_at: fm.created,
            modified_at: fm.modified,
            accessed_at: fm.modified,
            target: None,
        }
    }
}

/// Storage policy for erasure coding and replication.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoragePolicy {
    /// Data chunks per stripe
    pub data_chunks: u32,
    /// Parity chunks per stripe
    pub parity_chunks: u32,
    /// Replication factor
    pub replication_factor: u32,
}
