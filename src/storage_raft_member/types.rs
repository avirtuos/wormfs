//! Common types for the StorageRaftMember component.

use std::collections::HashMap;
use std::time::Duration;
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
/// and snapshot management.
#[derive(Debug, Clone)]
pub struct Config {
    /// Interval between heartbeats from leader to followers
    pub heartbeat_interval: Duration,

    /// Minimum election timeout duration
    pub election_timeout_min: Duration,

    /// Maximum election timeout duration
    pub election_timeout_max: Duration,

    /// Maximum number of log entries per AppendEntries RPC
    pub max_payload_entries: u64,

    /// Time threshold for triggering snapshot
    pub snapshot_time_threshold: Duration,

    /// Log size threshold (bytes) for triggering snapshot
    pub snapshot_log_size_threshold: u64,

    /// Threshold for considering a follower lagging
    pub replication_lag_threshold: u64,

    /// Maximum number of in-flight AppendEntries RPCs per follower
    pub max_in_flight_append_entries: usize,

    /// Enable lease-based read optimization
    pub enable_lease_based_reads: bool,

    /// Duration of read lease
    pub lease_duration: Duration,
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
