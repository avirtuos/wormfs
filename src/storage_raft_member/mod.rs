//! # StorageRaftMember Component
//!
//! StorageRaftMember implements the Raft consensus protocol for WormFS, ensuring strong
//! consistency of metadata operations across the distributed cluster.
//!
//! ## Responsibilities
//!
//! - Participating in Raft leader election and maintaining cluster membership
//! - Proposing and committing metadata write transactions through consensus
//! - Replicating transaction log entries to follower nodes
//! - Applying committed operations to the MetadataStore
//! - Coordinating metadata snapshots across the cluster
//! - Managing read leases for optimized read performance
//! - Handling node join/leave operations
//! - Detecting and recovering from split-brain scenarios
//!
//! ## Two-Phase Commit Protocol
//!
//! StorageRaftMember coordinates distributed transactions using a two-phase commit (2PC)
//! protocol implemented through Raft consensus:
//!
//! ### Phase 1: PREPARE
//! 1. Leader creates TransactionPrepare entry with metadata and chunk operations
//! 2. Prepare entry is replicated via Raft to all nodes
//! 3. Each node applies prepare locally:
//!    - Prepares metadata changes (not yet visible)
//!    - Writes chunks with state="preparing" and fsyncs
//!    - Votes PREPARED or ABORT
//! 4. Leader collects votes from all nodes
//!
//! ### Phase 2: COMMIT/ABORT
//! - **Commit**: If all voted PREPARED, leader proposes TransactionCommit
//!   - Metadata becomes visible in MetadataStore
//!   - Chunks transition to state="active"
//! - **Abort**: If any voted ABORT, leader proposes TransactionAbort
//!   - Metadata changes discarded
//!   - Preparing chunks deleted
//!
//! ## Read Consistency
//!
//! StorageRaftMember supports multiple read consistency levels:
//! - **Linearizable**: Forward to leader, read after commit index
//! - **Lease-based**: Leader serves reads from local state within lease duration
//! - **Stale**: Followers serve reads with bounded staleness
//!
//! ## Snapshot Coordination
//!
//! The leader periodically triggers coordinated snapshots:
//! 1. Leader sends snapshot proposal to all nodes
//! 2. Each node creates consistent MetadataStore snapshot
//! 3. Nodes report completion
//! 4. Leader updates cluster snapshot state
//! 5. All nodes trim TransactionLogStore to snapshot point

pub mod types;

use async_trait::async_trait;
use std::net::SocketAddr;
pub use types::{Config, Error, NodeId, RaftMetrics, RaftRole};

/// StorageRaftMember trait defines the interface for Raft consensus operations.
///
/// Implementations provide distributed consensus for metadata operations, ensuring
/// strong consistency across the storage cluster.
#[async_trait]
pub trait StorageRaftMember: Send + Sync {
    /// Metadata operation type
    type Operation: Send + Sync;
    /// Result type for operations
    type OperationResult: Send + Sync;

    /// Create a new Raft member with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `node_id` - Unique identifier for this node
    /// * `config` - Raft configuration (timeouts, log settings, etc.)
    ///
    /// # Returns
    ///
    /// A new RaftMember instance ready to join or create a cluster.
    async fn new(node_id: NodeId, config: Config) -> Result<Self, Error>
    where
        Self: Sized;

    /// Initialize Raft and join or create a cluster.
    ///
    /// If `peers` is empty, this node becomes the initial leader of a new cluster.
    /// Otherwise, it attempts to join the existing cluster formed by the peers.
    ///
    /// # Arguments
    ///
    /// * `peers` - List of peer node IDs to form/join cluster with
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Unable to contact any peers (when joining)
    /// - Cluster membership cannot be established
    /// - Local state is incompatible with cluster state
    async fn initialize(&mut self, peers: Vec<NodeId>) -> Result<(), Error>;

    /// Propose a metadata write operation through Raft consensus.
    ///
    /// This method submits an operation to be replicated and committed via Raft.
    /// The operation is only applied after achieving consensus from a quorum of nodes.
    ///
    /// # Arguments
    ///
    /// * `operation` - The metadata operation to propose (create file, allocate chunks, etc.)
    ///
    /// # Returns
    ///
    /// The result of applying the operation, available after commit.
    ///
    /// # Errors
    ///
    /// - `NotLeader`: This node is not the leader; caller should retry on the leader
    /// - `Timeout`: Operation did not achieve consensus within timeout
    /// - `NoQuorum`: Insufficient nodes available to form quorum
    async fn propose_operation(
        &self,
        operation: Self::Operation,
    ) -> Result<Self::OperationResult, Error>;

    /// Check if this node is currently the Raft leader.
    ///
    /// # Returns
    ///
    /// `true` if this node is the current leader, `false` otherwise.
    fn is_leader(&self) -> bool;

    /// Get current Raft metrics and status.
    ///
    /// Returns metrics including:
    /// - Current term and role
    /// - Commit and apply indices
    /// - Replication lag per follower
    /// - Snapshot status
    ///
    /// # Returns
    ///
    /// A metrics object containing current Raft state.
    fn get_metrics(&self) -> RaftMetrics;

    /// Manually trigger a metadata snapshot.
    ///
    /// This method forces a snapshot creation regardless of configured thresholds.
    /// It coordinates with all nodes to create consistent snapshots and trim logs.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - This node is not the leader
    /// - Snapshot creation fails on any node
    /// - Coordination timeout expires
    async fn trigger_snapshot(&self) -> Result<(), Error>;

    /// Add a new node to the Raft cluster.
    ///
    /// This method proposes a cluster membership change to add a new node.
    /// The operation goes through Raft consensus and requires a quorum.
    ///
    /// # Arguments
    ///
    /// * `node_id` - Identifier for the new node
    /// * `address` - Network address of the new node
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - This node is not the leader
    /// - Node is already a member
    /// - Membership change cannot achieve consensus
    async fn add_node(&self, node_id: NodeId, address: SocketAddr) -> Result<(), Error>;

    /// Remove a node from the Raft cluster.
    ///
    /// This method proposes a cluster membership change to remove a node.
    /// The operation goes through Raft consensus and requires a quorum.
    ///
    /// # Arguments
    ///
    /// * `node_id` - Identifier of the node to remove
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - This node is not the leader
    /// - Node is not a member
    /// - Removing the node would lose quorum
    async fn remove_node(&self, node_id: NodeId) -> Result<(), Error>;

    /// Step down from leader role for graceful shutdown.
    ///
    /// This method causes the current leader to relinquish leadership and
    /// trigger an election. Useful for graceful node shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if this node is not currently the leader.
    async fn step_down(&self) -> Result<(), Error>;
}
