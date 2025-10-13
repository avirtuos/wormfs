//! Common types for the StorageRaftMember component.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use thiserror::Error;

/// Unique identifier for a node in the Raft cluster.
///
/// NodeId is used to identify members of the Raft cluster and track
/// cluster membership changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

/// Raft configuration options.
///
/// These parameters control Raft's behavior for elections, log replication,
/// snapshot management, and transaction coordination.
#[derive(Debug, Clone)]
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

    /// Replication lag per follower (leader only)
    pub replication_lag: HashMap<NodeId, u64>,
}

// ============================================================================
// Raft Operations and Transaction Types
// ============================================================================

/// Transaction ID for two-phase commit coordination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxId(pub u64);

/// Operations that can be proposed through Raft consensus.
#[derive(Debug, Clone)]
pub enum WormFsOperation {
    /// Prepare phase of two-phase commit transaction
    TransactionPrepare {
        tx_id: TxId,
        metadata_ops: Option<Vec<MetadataOperation>>,
        command_ops: Option<Vec<CommandOperation>>,
        timeout: SystemTime,
    },
    /// Commit phase of two-phase commit transaction
    TransactionCommit { tx_id: TxId },
    /// Abort phase of two-phase commit transaction
    TransactionAbort { tx_id: TxId, reason: Option<String> },
}

/// Metadata operations that can be proposed through Raft.
#[derive(Debug, Clone)]
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
        metadata: FileMetadata,
        policy: StoragePolicy,
    },
    /// Delete a file
    FileDelete { file_id: FileId },
    /// Create a stripe for a file
    CreateStripe {
        file_id: FileId,
        stripe_id: StripeId,
        policy: StoragePolicy,
        offset: u64,
        size: u64,
        chunks: Vec<ChunkId>,
    },
    /// Delete a stripe
    DeleteStripe { stripe_id: StripeId },
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
}

/// Command operations that can be proposed through Raft.
#[derive(Debug, Clone)]
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
// Placeholder types (to be defined in appropriate modules)
// ============================================================================

/// File identifier (placeholder).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileId(pub u64);

/// Stripe identifier (placeholder).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StripeId(pub u64);

/// Chunk identifier (placeholder).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChunkId(pub u64);

/// Disk identifier (placeholder).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DiskId(pub u64);

/// Chunk index within a stripe (placeholder).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkIndex(pub u32);

/// File metadata (placeholder).
#[derive(Debug, Clone)]
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

/// Storage policy for erasure coding and replication (placeholder).
#[derive(Debug, Clone)]
pub struct StoragePolicy {
    /// Data chunks per stripe
    pub data_chunks: u32,
    /// Parity chunks per stripe
    pub parity_chunks: u32,
    /// Replication factor
    pub replication_factor: u32,
}
