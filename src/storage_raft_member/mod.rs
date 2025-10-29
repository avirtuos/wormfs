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
//! protocol implemented through Raft consensus. **All operations are metadata-only** - chunk
//! data is staged before Raft operations begin.
//!
//! ### Phase 0: CHUNK STAGING (Data Plane - Before Raft)
//! 1. Leader calculates stripe layout and chunk placement
//! 2. Leader stages chunks (either directly or via client upload tokens)
//! 3. Storage nodes write chunks to disk in "staged" state (not in metadata)
//! 4. Only Leader tracks staged chunks in memory during transaction
//!
//! ### Phase 1: PREPARE METADATA (Control Plane - Raft)
//! 1. Leader creates TransactionPrepare entry with **metadata operations only**
//! 2. Prepare entry is replicated via Raft to all nodes
//! 3. Each node applies prepare locally:
//!    - Stages metadata changes (not yet visible in MetadataStore)
//!    - Validates operations
//!    - Votes PREPARED or ABORT via Raft acknowledgement
//! 4. Leader collects votes from Raft acknowledgements (not separate RPCs)
//!
//! ### Phase 2: COMMIT/ABORT METADATA (Control Plane - Raft)
//! - **Commit**: If quorum voted PREPARED, leader proposes TransactionCommit
//!   - Metadata becomes visible in MetadataStore
//!   - Signals storage nodes to activate staged chunks
//! - **Abort**: If quorum failed, leader proposes TransactionAbort
//!   - Metadata changes discarded
//!   - Signals storage nodes to discard staged chunks
//!
//! ### Transaction Recovery
//! - **Leader Change**: New leader uses conservative timeout-based approach
//! - **Vote Persistence**: Votes kept in memory only during transaction
//! - **Orphaned Chunks**: StorageWatchdog cleans up chunks older than 1 hour
//!
//! ## Read Consistency
//!
//! StorageRaftMember supports local reads with bounded staleness:
//! - **Local Reads**: All nodes (leader and followers) serve reads from local MetadataStore
//! - **Bounded Staleness**: Reads may be stale up to configured threshold (default: 120 seconds)
//! - **Client Request Handling**: Non-leaders reject writes and inform client of current leader
//!
//! Future versions may support:
//! - Linearizable reads (forward to leader)
//! - Quorum reads (read from majority)
//!
//! ## Snapshot Coordination
//!
//! The leader periodically triggers coordinated snapshots with eventual consistency:
//! 1. Leader sends snapshot proposal to all nodes
//! 2. Each node creates consistent MetadataStore snapshot (with zstd compression)
//! 3. Snapshots are streamed to avoid holding entire snapshot in memory
//! 4. Nodes report completion
//! 5. Leader updates cluster snapshot state
//! 6. All nodes trim TransactionLogStore to snapshot point
//!
//! ## Performance Optimizations
//!
//! - **Pipeline Optimization**: Multiple AppendEntries RPCs in flight simultaneously
//! - **Batched AppendEntries**: Multiple log entries per RPC
//! - **Backpressure Handling**: Reject writes when log backlog exists
//! - **Single-Node Membership Changes**: Simpler than joint consensus
//!
//! ## Operational Features
//!
//! - **Transaction Timeouts**: Configurable per-transaction
//! - **Lock Expiration**: Leader detects expired locks and issues Raft transaction
//! - **Concurrent Transaction Limits**: Configurable maximum for resource management

pub mod implementation;
pub mod log_storage;
pub mod network_factory;
pub mod raft_config;
pub mod raft_member;
pub mod state_machine;
pub mod types;

use async_trait::async_trait;
pub use implementation::StorageRaftMemberImpl;
pub use log_storage::RaftLogStorageAdapter;
pub use network_factory::WormFsNetworkFactory;
pub use raft_member::RaftMember;
pub use state_machine::WormFsStateMachine;
use std::net::SocketAddr;
pub use types::{
    Config, Error, MetadataChangeEvent, MetadataChangeType, NodeId, RaftMetrics, RaftRole,
};

/// StorageRaftMember trait defines the interface for Raft consensus operations.
///
/// Implementations provide distributed consensus for metadata operations, ensuring
/// strong consistency across the storage cluster.
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock(
    type Operation = ();
    type OperationResult = ();
))]
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

    /// Subscribe to metadata change events.
    ///
    /// Returns a receiver channel for metadata change notifications.
    /// Events are sent when metadata operations are committed through Raft.
    ///
    /// # Arguments
    ///
    /// * `filter` - Optional list of event types to subscribe to. If None, all events are received.
    ///
    /// # Returns
    ///
    /// An unbounded receiver channel for `MetadataChangeEvent`.
    ///
    /// # Notes
    ///
    /// - Events are sent asynchronously and do not block Raft operations
    /// - Slow subscribers may experience channel capacity issues
    /// - At-most-once delivery semantics (events may be missed if channel is full)
    async fn subscribe_metadata_changes(
        &self,
        filter: Option<Vec<MetadataChangeType>>,
    ) -> tokio::sync::mpsc::UnboundedReceiver<MetadataChangeEvent>;

    /// Handle an incoming Raft RPC from a remote node.
    ///
    /// This method is called by StorageNetwork when it receives a Raft RPC over libp2p.
    /// It forwards the RPC to the underlying OpenRaft instance for processing.
    ///
    /// # Arguments
    ///
    /// * `request` - The Raft RPC request (Vote, AppendEntries, or InstallSnapshot)
    ///
    /// # Returns
    ///
    /// The Raft RPC response to send back to the requesting node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The RPC cannot be deserialized
    /// - The Raft instance rejects the RPC
    /// - Internal processing fails
    async fn handle_raft_rpc(&self, request: Vec<u8>) -> Result<Vec<u8>, Error>;
}
