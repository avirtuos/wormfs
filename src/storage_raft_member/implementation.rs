//! Concrete implementation of StorageRaftMember using OpenRaft.
//!
//! This module provides the actual implementation of the StorageRaftMember trait
//! using OpenRaft for distributed consensus. It follows the interior mutability pattern
//! to satisfy OpenRaft's ownership requirements.

use async_trait::async_trait;
use openraft::{Raft, RaftMetrics as OpenRaftMetrics};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use super::raft_config::{WormFsNode, WormFsTypeConfig};
use super::types::{
    Config, Error, MetadataChangeEvent, MetadataChangeType, NodeId, RaftMetrics, RaftRole, TxId,
    WormFsOperation,
};
use super::StorageRaftMember;

/// Inner state for StorageRaftMemberImpl, wrapped in Arc for interior mutability.
///
/// This struct contains all the mutable state and is shared across clones of
/// StorageRaftMemberImpl via Arc. This pattern is required by OpenRaft which needs
/// to "own" an instance while other components hold clones.
struct Inner {
    /// This node's ID
    node_id: NodeId,

    /// Raft configuration
    config: Config,

    /// The OpenRaft instance
    raft: Arc<Raft<WormFsTypeConfig>>,

    /// Whether this node is currently the leader
    is_leader: AtomicBool,

    /// Current leader's node ID (if known)
    current_leader: RwLock<Option<NodeId>>,

    /// In-flight transaction state for two-phase commit
    /// Maps transaction ID to transaction state
    pending_transactions: RwLock<HashMap<TxId, TransactionState>>,

    /// Next transaction ID to use
    next_tx_id: AtomicU64,

    /// Metadata change subscribers
    subscribers: RwLock<Vec<SubscriberHandle>>,
}

/// State of an in-flight transaction during two-phase commit.
#[derive(Debug, Clone)]
struct TransactionState {
    /// Transaction ID
    tx_id: TxId,

    /// The prepare operation
    operation: WormFsOperation,

    /// Votes collected from nodes during prepare phase
    /// Maps NodeId to their vote (true = prepared, false = abort)
    votes: HashMap<NodeId, bool>,

    /// Number of nodes that must vote to achieve quorum
    quorum_size: usize,

    /// Transaction timeout
    timeout: std::time::SystemTime,

    /// Whether the transaction has been decided (committed or aborted)
    decided: bool,
}

/// Handle for a metadata change subscriber.
struct SubscriberHandle {
    /// Channel to send events to subscriber
    sender: mpsc::UnboundedSender<MetadataChangeEvent>,

    /// Filter for event types (None = all events)
    filter: Option<Vec<MetadataChangeType>>,
}

/// Concrete implementation of StorageRaftMember using OpenRaft.
///
/// This struct uses the interior mutability pattern - it's cheap to clone (just clones the Arc)
/// and all clones share the same underlying state. This is required by OpenRaft's API.
pub struct StorageRaftMemberImpl {
    /// Inner state wrapped in Arc for interior mutability
    inner: Arc<Inner>,
}

impl Clone for StorageRaftMemberImpl {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl StorageRaftMemberImpl {
    /// Create a new StorageRaftMemberImpl with the given dependencies.
    ///
    /// This is an internal constructor. The public interface is via the trait's `new` method.
    ///
    /// # Arguments
    ///
    /// * `node_id` - Unique identifier for this node
    /// * `config` - Raft configuration
    /// * `raft` - Initialized OpenRaft instance
    pub(crate) fn new_with_raft(
        node_id: NodeId,
        config: Config,
        raft: Arc<Raft<WormFsTypeConfig>>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                node_id,
                config,
                raft,
                is_leader: AtomicBool::new(false),
                current_leader: RwLock::new(None),
                pending_transactions: RwLock::new(HashMap::new()),
                next_tx_id: AtomicU64::new(1),
                subscribers: RwLock::new(Vec::new()),
            }),
        }
    }

    /// Get the next transaction ID.
    fn next_tx_id(&self) -> TxId {
        let id = self.inner.next_tx_id.fetch_add(1, Ordering::SeqCst);
        TxId(id)
    }

    /// Update leadership state based on Raft metrics.
    async fn update_leadership(&self, metrics: &OpenRaftMetrics<NodeId, WormFsNode>) {
        let is_leader = matches!(metrics.state, openraft::ServerState::Leader);
        self.inner.is_leader.store(is_leader, Ordering::SeqCst);

        let mut current_leader = self.inner.current_leader.write().await;
        *current_leader = metrics.current_leader;
    }

    /// Notify subscribers of a metadata change event.
    async fn notify_subscribers(&self, event: MetadataChangeEvent) {
        let subscribers = self.inner.subscribers.read().await;

        for subscriber in subscribers.iter() {
            // Check if this event matches the subscriber's filter
            let should_send = if let Some(filter) = &subscriber.filter {
                event.changes.iter().any(|change| {
                    let change_type = match change {
                        super::types::MetadataChange::FileCreated { .. } => {
                            MetadataChangeType::FileCreated
                        }
                        super::types::MetadataChange::FileUpdated { .. } => {
                            MetadataChangeType::FileUpdated
                        }
                        super::types::MetadataChange::FileDeleted { .. } => {
                            MetadataChangeType::FileDeleted
                        }
                        super::types::MetadataChange::DirectoryCreated { .. } => {
                            MetadataChangeType::DirectoryCreated
                        }
                        super::types::MetadataChange::DirectoryDeleted { .. } => {
                            MetadataChangeType::DirectoryDeleted
                        }
                        super::types::MetadataChange::StripeCreated { .. } => {
                            MetadataChangeType::StripeCreated
                        }
                        super::types::MetadataChange::StripeDeleted { .. } => {
                            MetadataChangeType::StripeDeleted
                        }
                        super::types::MetadataChange::ChunkMoved { .. } => {
                            MetadataChangeType::ChunkMoved
                        }
                        super::types::MetadataChange::LockReleased { .. } => {
                            MetadataChangeType::LockReleased
                        }
                    };
                    filter.contains(&change_type)
                })
            } else {
                true // No filter means all events
            };

            if should_send {
                // Non-blocking send - if channel is full or closed, we drop the event
                // This implements at-most-once delivery semantics
                let _ = subscriber.sender.send(event.clone());
            }
        }
    }

    /// Convert OpenRaft metrics to WormFS RaftMetrics.
    fn convert_metrics(
        &self,
        openraft_metrics: OpenRaftMetrics<NodeId, WormFsNode>,
    ) -> RaftMetrics {
        let role = match openraft_metrics.state {
            openraft::ServerState::Leader => RaftRole::Leader,
            openraft::ServerState::Follower => RaftRole::Follower,
            openraft::ServerState::Candidate => RaftRole::Candidate,
            _ => RaftRole::Follower, // Learner treated as follower for now
        };

        // Extract replication lag from replication metrics
        let mut replication_lag = HashMap::new();
        if let Some(replication) = &openraft_metrics.replication {
            let last_log = openraft_metrics.last_log_index.unwrap_or(0);
            for (node_id, matched_opt) in replication.iter() {
                if let Some(matched) = matched_opt {
                    let lag = last_log.saturating_sub(matched.index);
                    replication_lag.insert(*node_id, lag);
                }
            }
        }

        RaftMetrics {
            current_term: openraft_metrics.current_term,
            role,
            leader_id: openraft_metrics.current_leader,
            commit_index: openraft_metrics.last_applied.map(|l| l.index).unwrap_or(0),
            last_applied: openraft_metrics.last_applied.map(|l| l.index).unwrap_or(0),
            last_log_index: openraft_metrics.last_log_index.unwrap_or(0),
            snapshot_index: openraft_metrics.snapshot.map(|s| s.index).unwrap_or(0),
            cluster_size: openraft_metrics
                .membership_config
                .membership()
                .voter_ids()
                .count(),
            replication_lag,
        }
    }
}

#[async_trait]
impl StorageRaftMember for StorageRaftMemberImpl {
    type Operation = WormFsOperation;
    type OperationResult = ();

    async fn new(_node_id: NodeId, _config: Config) -> Result<Self, Error>
    where
        Self: Sized,
    {
        // This will be implemented once we have the storage adapters ready
        // For now, this is a placeholder
        todo!("StorageRaftMemberImpl::new requires storage adapters to be implemented first")
    }

    async fn initialize(&mut self, _peers: Vec<NodeId>) -> Result<(), Error> {
        // This will be implemented in the next phase
        todo!("StorageRaftMemberImpl::initialize will be implemented with cluster initialization")
    }

    async fn propose_operation(
        &self,
        _operation: Self::Operation,
    ) -> Result<Self::OperationResult, Error> {
        // Check if this node is the leader
        if !self.is_leader() {
            let leader = self.inner.current_leader.read().await;
            return Err(Error::NotLeader { leader: *leader });
        }

        // This will be implemented in the next phase with full 2PC logic
        todo!("StorageRaftMemberImpl::propose_operation will be implemented with 2PC logic")
    }

    fn is_leader(&self) -> bool {
        self.inner.is_leader.load(Ordering::SeqCst)
    }

    fn get_metrics(&self) -> RaftMetrics {
        // Get metrics from OpenRaft
        let openraft_metrics = self.inner.raft.metrics().borrow().clone();
        self.convert_metrics(openraft_metrics)
    }

    async fn trigger_snapshot(&self) -> Result<(), Error> {
        // Check if this node is the leader
        if !self.is_leader() {
            let leader = self.inner.current_leader.read().await;
            return Err(Error::NotLeader { leader: *leader });
        }

        // This will be implemented in the next phase
        todo!("StorageRaftMemberImpl::trigger_snapshot will be implemented with snapshot coordination")
    }

    async fn add_node(&self, _node_id: NodeId, _address: SocketAddr) -> Result<(), Error> {
        // Check if this node is the leader
        if !self.is_leader() {
            let leader = self.inner.current_leader.read().await;
            return Err(Error::NotLeader { leader: *leader });
        }

        // This will be implemented in the next phase
        todo!("StorageRaftMemberImpl::add_node will be implemented with membership changes")
    }

    async fn remove_node(&self, _node_id: NodeId) -> Result<(), Error> {
        // Check if this node is the leader
        if !self.is_leader() {
            let leader = self.inner.current_leader.read().await;
            return Err(Error::NotLeader { leader: *leader });
        }

        // This will be implemented in the next phase
        todo!("StorageRaftMemberImpl::remove_node will be implemented with membership changes")
    }

    async fn step_down(&self) -> Result<(), Error> {
        // Check if this node is the leader
        if !self.is_leader() {
            return Err(Error::NotLeader { leader: None });
        }

        // Trigger Raft to step down by sending a heartbeat timeout
        // This will cause the leader to step down and trigger a new election
        self.inner
            .raft
            .trigger()
            .elect()
            .await
            .map_err(|e| Error::ConfigError(format!("Failed to step down: {:?}", e)))?;

        Ok(())
    }

    async fn subscribe_metadata_changes(
        &self,
        filter: Option<Vec<MetadataChangeType>>,
    ) -> tokio::sync::mpsc::UnboundedReceiver<MetadataChangeEvent> {
        let (sender, receiver) = mpsc::unbounded_channel();

        let subscriber = SubscriberHandle { sender, filter };

        let mut subscribers = self.inner.subscribers.write().await;
        subscribers.push(subscriber);

        receiver
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tx_id_generation() {
        // This test will be expanded once we have the full implementation
        let tx_id1 = TxId(1);
        let tx_id2 = TxId(2);
        assert_ne!(tx_id1, tx_id2);
    }

    #[test]
    fn test_subscriber_handle_creation() {
        let (sender, _receiver) = mpsc::unbounded_channel();
        let handle = SubscriberHandle {
            sender,
            filter: Some(vec![MetadataChangeType::FileCreated]),
        };
        assert!(handle.filter.is_some());
    }
}
