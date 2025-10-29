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
use tracing::{error, info};

use crate::metadata_store::{MetadataStoreFactory, MetadataStoreImpl};
use crate::transaction_log_store::{TransactionLogConfig, TransactionLogStoreImpl};

use super::log_storage::RaftLogStorageAdapter;
use super::network_factory::WormFsNetworkFactory;
use super::raft_config::{WormFsNode, WormFsTypeConfig};
use super::state_machine::WormFsStateMachine;
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

    async fn new(node_id: NodeId, config: Config) -> Result<Self, Error>
    where
        Self: Sized,
    {
        info!("Creating StorageRaftMember for node {:?}", node_id);

        // Get the storage network handle
        let storage_network = config.storage_network.clone().ok_or_else(|| {
            Error::ConfigError("storage_network must be set in Config".to_string())
        })?;

        // Create the TransactionLogStore
        info!(
            "Opening transaction log at {:?}",
            config.transaction_log_path
        );
        let log_config = TransactionLogConfig {
            db_path: config.transaction_log_path.clone(),
            cache_size_mb: 64,
            compact_threshold_mb: 100,
            max_log_size_mb: 1000,
            max_log_age_days: 7,
        };
        let log_store = TransactionLogStoreImpl::new(log_config)
            .map_err(|e| Error::StorageError(format!("Failed to open transaction log: {:?}", e)))?;

        // Create the MetadataStore
        info!("Opening metadata store at {:?}", config.metadata_db_path);
        let metadata_config = crate::metadata_store::Config {
            database_path: config.metadata_db_path.clone(),
            read_pool_size: 8,
            enable_wal: true,
            cache_size_mb: 10,
            enable_foreign_keys: true,
            synchronous: crate::metadata_store::types::SynchronousMode::Normal,
            transaction_isolation: crate::metadata_store::types::IsolationLevel::Serializable,
            enable_prepared_statements: true,
            read_pool_timeout_secs: 30,
            stripe_cache_size_mb: 64,
            stripe_cache_ttl_secs: 10,
            stripe_cache_tti_secs: 5,
            chunk_cache_size_mb: 64,
            chunk_cache_ttl_secs: 10,
            chunk_cache_tti_secs: 5,
        };
        let metadata_store = MetadataStoreFactory::create_concrete(metadata_config)
            .await
            .map_err(|e| Error::StorageError(format!("Failed to open metadata store: {:?}", e)))?;

        // Create the adapters
        let log_storage = RaftLogStorageAdapter::new(log_store);
        let state_machine =
            WormFsStateMachine::new(metadata_store, config.snapshot_directory.clone());
        let network_factory = WormFsNetworkFactory::new(storage_network);

        // Convert our Config to OpenRaft's config
        let raft_config = openraft::Config {
            heartbeat_interval: config.heartbeat_interval.as_millis() as u64,
            election_timeout_min: config.election_timeout_min.as_millis() as u64,
            election_timeout_max: config.election_timeout_max.as_millis() as u64,
            max_payload_entries: config.max_payload_entries,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
                config.snapshot_log_size_threshold / 1000, // Convert to entry count estimate
            ),
            ..Default::default()
        };

        // Validate the config (validate() consumes self and returns the validated config)
        let validated_config = raft_config
            .validate()
            .map_err(|e| Error::ConfigError(format!("Invalid Raft config: {:?}", e)))?;

        // Create the Raft instance
        info!("Creating Raft instance with node_id {:?}", node_id);
        let raft = Raft::new(
            node_id,
            Arc::new(validated_config),
            network_factory,
            log_storage,
            state_machine,
        )
        .await
        .map_err(|e| Error::RaftError(format!("Failed to create Raft instance: {:?}", e)))?;

        let raft = Arc::new(raft);

        // Create the implementation
        let impl_instance = Self::new_with_raft(node_id, config, raft.clone());

        // Start a background task to monitor Raft metrics and update leadership state
        let impl_clone = impl_instance.clone();
        tokio::spawn(async move {
            let mut metrics_rx = impl_clone.inner.raft.metrics();
            loop {
                tokio::select! {
                    _ = metrics_rx.changed() => {
                        let metrics = metrics_rx.borrow_and_update().clone();
                        impl_clone.update_leadership(&metrics).await;
                    }
                }
            }
        });

        info!(
            "StorageRaftMember created successfully for node {:?}",
            node_id
        );
        Ok(impl_instance)
    }

    async fn initialize(&mut self, peers: Vec<NodeId>) -> Result<(), Error> {
        info!(
            "Initializing Raft for node {:?} with peers: {:?}",
            self.inner.node_id, peers
        );

        // Check if already initialized
        let is_initialized =
            self.inner.raft.is_initialized().await.map_err(|e| {
                Error::RaftError(format!("Failed to check initialization: {:?}", e))
            })?;

        if is_initialized {
            info!("Node {:?} is already initialized", self.inner.node_id);
            return Ok(());
        }

        if peers.is_empty() {
            // Single-node cluster: initialize with just this node
            info!(
                "Creating single-node cluster for node {:?}",
                self.inner.node_id
            );

            // Create a WormFsNode for this node
            // We use a placeholder peer_id based on the node_id since we don't have
            // the actual libp2p PeerId yet. This will work for single-node clusters.
            let this_node = super::raft_config::WormFsNode {
                peer_id: format!("node-{}", self.inner.node_id.as_u64()),
                metadata: Some(super::raft_config::NodeMetadata {
                    name: Some(format!("node-{}", self.inner.node_id.as_u64())),
                    version: Some(env!("CARGO_PKG_VERSION").to_string()),
                }),
            };

            // Initialize as a single-node cluster
            let mut members = std::collections::BTreeMap::new();
            members.insert(self.inner.node_id, this_node);

            self.inner
                .raft
                .initialize(members)
                .await
                .map_err(|e| Error::RaftError(format!("Failed to initialize Raft: {:?}", e)))?;

            info!(
                "Successfully initialized single-node cluster for node {:?}",
                self.inner.node_id
            );
            Ok(())
        } else {
            // Multi-node cluster: joining an existing cluster
            //
            // NOTE: This requires more design work. The current interface only provides NodeIds,
            // but we need WormFsNode information (peer_ids) to construct the membership.
            //
            // Possible approaches:
            // 1. Change the interface to pass full node information (NodeId + PeerId)
            // 2. Have the new node wait to be added via add_node() by the leader
            // 3. Add a separate method for querying node information from peers
            //
            // For now, we return an error indicating this is not yet implemented.
            Err(Error::ConfigError(
                "Multi-node cluster initialization not yet implemented. \
                 Please initialize as a single-node cluster first, then use \
                 add_node() to add additional nodes."
                    .to_string(),
            ))
        }
    }

    async fn propose_operation(
        &self,
        operation: Self::Operation,
    ) -> Result<Self::OperationResult, Error> {
        info!(
            "Proposing operation through Raft for node {:?}",
            self.inner.node_id
        );

        // Submit the operation to Raft for replication and consensus
        let response = self.inner.raft.client_write(operation).await.map_err(|e| {
            // Convert OpenRaft errors to our Error type
            match e {
                openraft::error::RaftError::APIError(api_err) => {
                    use openraft::error::ClientWriteError;
                    match api_err {
                        ClientWriteError::ForwardToLeader(forward) => Error::NotLeader {
                            leader: forward.leader_id,
                        },
                        ClientWriteError::ChangeMembershipError(err) => {
                            Error::MembershipChangeFailed(format!("{:?}", err))
                        }
                    }
                }
                openraft::error::RaftError::Fatal(fatal) => {
                    Error::RaftError(format!("Fatal Raft error: {:?}", fatal))
                }
            }
        })?;

        info!(
            "Operation committed at log_id: {:?} for node {:?}",
            response.log_id, self.inner.node_id
        );

        // The operation has been committed through Raft consensus and applied to the state machine
        // For now, we just return success. In the future, we could return more detailed information
        // from the response.data field if needed.
        Ok(())
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
        info!("Triggering snapshot for node {:?}", self.inner.node_id);

        // Trigger OpenRaft to create a snapshot
        // Note: OpenRaft handles the snapshot creation through the state machine's
        // build_snapshot() method, which we've already implemented
        self.inner
            .raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| Error::SnapshotFailed(format!("Failed to trigger snapshot: {:?}", e)))?;

        info!(
            "Snapshot trigger completed for node {:?}",
            self.inner.node_id
        );
        Ok(())
    }

    async fn add_node(&self, node_id: NodeId, address: SocketAddr) -> Result<(), Error> {
        info!(
            "Adding node {:?} with address {:?} to cluster",
            node_id, address
        );

        // Check if this node is the leader
        if !self.is_leader() {
            let leader = self.inner.current_leader.read().await;
            return Err(Error::NotLeader { leader: *leader });
        }

        // Create node information
        let node = super::raft_config::WormFsNode {
            peer_id: address.to_string(), // Use address as peer_id for now
            metadata: Some(super::raft_config::NodeMetadata {
                name: Some(format!("node-{}", node_id.as_u64())),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
            }),
        };

        // Add the node as a learner first, then promote to voter
        // Step 1: Add as learner
        let mut nodes = std::collections::BTreeMap::new();
        nodes.insert(node_id, node);

        self.inner
            .raft
            .change_membership(openraft::ChangeMembers::AddNodes(nodes.clone()), false)
            .await
            .map_err(|e| {
                Error::MembershipChangeFailed(format!("Failed to add node as learner: {:?}", e))
            })?;

        // Step 2: Promote to voter
        let mut voter_ids = std::collections::BTreeSet::new();
        voter_ids.insert(node_id);

        self.inner
            .raft
            .change_membership(openraft::ChangeMembers::AddVoterIds(voter_ids), true)
            .await
            .map_err(|e| {
                Error::MembershipChangeFailed(format!("Failed to promote node to voter: {:?}", e))
            })?;

        info!("Successfully added node {:?} to cluster", node_id);
        Ok(())
    }

    async fn remove_node(&self, node_id: NodeId) -> Result<(), Error> {
        info!("Removing node {:?} from cluster", node_id);

        // Check if this node is the leader
        if !self.is_leader() {
            let leader = self.inner.current_leader.read().await;
            return Err(Error::NotLeader { leader: *leader });
        }

        // Remove the node from voters first, then from learners
        // Step 1: Remove from voters
        let mut voter_ids = std::collections::BTreeSet::new();
        voter_ids.insert(node_id);

        self.inner
            .raft
            .change_membership(openraft::ChangeMembers::RemoveVoters(voter_ids), true)
            .await
            .map_err(|e| {
                Error::MembershipChangeFailed(format!("Failed to remove node from voters: {:?}", e))
            })?;

        // Step 2: Remove from nodes
        let mut node_ids = std::collections::BTreeSet::new();
        node_ids.insert(node_id);

        self.inner
            .raft
            .change_membership(openraft::ChangeMembers::RemoveNodes(node_ids), true)
            .await
            .map_err(|e| {
                Error::MembershipChangeFailed(format!(
                    "Failed to remove node from cluster: {:?}",
                    e
                ))
            })?;

        info!("Successfully removed node {:?} from cluster", node_id);
        Ok(())
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
