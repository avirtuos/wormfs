//! Concrete implementation of StorageRaftMember using OpenRaft.
//!
//! This module provides the actual implementation of the StorageRaftMember trait
//! using OpenRaft for distributed consensus. It follows the interior mutability pattern
//! to satisfy OpenRaft's ownership requirements.

use async_trait::async_trait;
use futures::FutureExt;
use openraft::{Raft, RaftMetrics as OpenRaftMetrics};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, trace};

use crate::metadata_store::MetadataStoreImpl;
use crate::transaction_log_store::{TransactionLogConfig, TransactionLogStoreImpl};

use super::cluster_manager::{ClusterEvent, ClusterManager, ClusterManagerConfig};
use super::log_storage::RaftLogStorageAdapter;
use super::network_factory::WormFsNetworkFactory;
use super::raft_config::{WormFsNode, WormFsTypeConfig};
use super::state_machine::WormFsStateMachine;
use super::types::{
    Config, Error, MetadataChangeEvent, MetadataChangeType, NodeId, RaftMetrics, RaftRole, TxId,
    WormFsOperation,
};
use super::utils::current_time_ms;
use super::StorageRaftMember;

/// Inner state for StorageRaftMemberImpl, wrapped in Arc for interior mutability.
///
/// This struct contains all the mutable state and is shared across clones of
/// StorageRaftMemberImpl via Arc. This pattern is required by OpenRaft which needs
/// to "own" an instance while other components hold clones.
/// Inner state shared between Raft components
pub struct Inner {
    /// This node's ID
    pub node_id: NodeId,

    /// Raft configuration
    pub config: Config,

    /// The OpenRaft instance
    pub raft: Arc<Raft<WormFsTypeConfig>>,

    /// The state machine inner (for accessing subscriptions)
    state_machine_inner: Arc<RwLock<super::state_machine::StateMachineInner>>,

    /// Whether this node is currently the leader
    pub is_leader: AtomicBool,

    /// Current leader's node ID (if known)
    pub current_leader: RwLock<Option<NodeId>>,

    /// In-flight transaction state for two-phase commit
    /// Maps transaction ID to transaction state
    pending_transactions: RwLock<HashMap<TxId, TransactionState>>,

    /// Cluster manager for automatic failure detection and recovery
    /// Only active on the leader node
    cluster_manager: RwLock<Option<Arc<ClusterManager>>>,

    /// Channel for cluster events (sent by ClusterManager)
    cluster_event_sender: mpsc::UnboundedSender<ClusterEvent>,

    /// Channel receiver for cluster events
    cluster_event_receiver: RwLock<Option<mpsc::UnboundedReceiver<ClusterEvent>>>,

    /// Timestamp when leader last sent AppendEntries to each follower (leader only)
    pub heartbeat_sent: Arc<RwLock<HashMap<NodeId, std::time::Instant>>>,

    /// Timestamp when leader received AppendEntriesResponse from each follower (leader only)
    pub heartbeat_acked: Arc<RwLock<HashMap<NodeId, std::time::Instant>>>,

    /// Timestamp when this node started up (milliseconds since Unix epoch)
    pub startup_time: u64,

    /// History of recent proposals (last 5 proposals) for admin UI
    proposal_history: Arc<RwLock<std::collections::VecDeque<super::types::ProposalRecord>>>,
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

/// Concrete implementation of StorageRaftMember using OpenRaft.
///
/// This struct uses the interior mutability pattern - it's cheap to clone (just clones the Arc)
/// and all clones share the same underlying state. This is required by OpenRaft's API.
pub struct StorageRaftMemberImpl {
    /// Inner state wrapped in Arc for interior mutability
    inner: Arc<Inner>,
}

impl StorageRaftMemberImpl {
    /// Get this node's ID.
    pub fn node_id(&self) -> NodeId {
        self.inner.node_id
    }

    /// Get the last N proposals submitted through Raft (for admin UI).
    ///
    /// Returns proposals in chronological order (oldest first).
    pub async fn get_proposal_history(&self) -> Vec<super::types::ProposalRecord> {
        let history = self.inner.proposal_history.read().await;
        history.iter().cloned().collect()
    }
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
    /// * `heartbeat_sent` - Shared timing tracker for AppendEntries sent times
    /// * `heartbeat_acked` - Shared timing tracker for AppendEntries response times
    pub(crate) fn new_with_raft(
        node_id: NodeId,
        config: Config,
        raft: Arc<Raft<WormFsTypeConfig>>,
        state_machine_inner: Arc<RwLock<super::state_machine::StateMachineInner>>,
        heartbeat_sent: Arc<RwLock<HashMap<NodeId, std::time::Instant>>>,
        heartbeat_acked: Arc<RwLock<HashMap<NodeId, std::time::Instant>>>,
    ) -> Self {
        // Create event channel for cluster manager events
        let (cluster_event_sender, cluster_event_receiver) = mpsc::unbounded_channel();

        // We'll create the ClusterManager lazily when we become leader
        // For now, just store None

        // Record startup time for grace period tracking
        let startup_time = current_time_ms();

        Self {
            inner: Arc::new(Inner {
                node_id,
                config,
                raft,
                state_machine_inner,
                is_leader: AtomicBool::new(false),
                current_leader: RwLock::new(None),
                pending_transactions: RwLock::new(HashMap::new()),
                cluster_manager: RwLock::new(None),
                cluster_event_sender,
                cluster_event_receiver: RwLock::new(Some(cluster_event_receiver)),
                heartbeat_sent,
                heartbeat_acked,
                startup_time,
                proposal_history: Arc::new(RwLock::new(std::collections::VecDeque::new())),
            }),
        }
    }

    /// Update leadership state based on Raft metrics.
    async fn update_leadership(&self, metrics: &OpenRaftMetrics<NodeId, WormFsNode>) {
        let is_leader = matches!(metrics.state, openraft::ServerState::Leader);
        let was_leader = self.inner.is_leader.swap(is_leader, Ordering::SeqCst);

        // Only log when leadership actually changes (transition)
        if is_leader != was_leader {
            info!(
                "[Leadership] Node {:?} leadership changed: {} -> {}",
                self.inner.node_id,
                if was_leader {
                    "Leader"
                } else {
                    "Follower/Learner"
                },
                if is_leader {
                    "Leader"
                } else {
                    "Follower/Learner"
                }
            );
        }

        let mut current_leader = self.inner.current_leader.write().await;
        *current_leader = metrics.current_leader;

        // Handle ClusterManager lifecycle based on leadership changes
        if is_leader && !was_leader {
            // We just became the leader - start ClusterManager
            info!("Node became leader, starting ClusterManager");
            debug!(
                "[Leadership] Node {:?} became leader, starting ClusterManager",
                self.inner.node_id
            );
            self.start_cluster_manager().await;
        } else if !is_leader && was_leader {
            // We lost leadership - stop ClusterManager
            info!("Node lost leadership, stopping ClusterManager");
            debug!(
                "[Leadership] Node {:?} lost leadership, stopping ClusterManager",
                self.inner.node_id
            );
            self.stop_cluster_manager().await;
        }
    }

    /// Start the ClusterManager for automatic failure detection and recovery.
    ///
    /// This is called when this node becomes the leader.
    async fn start_cluster_manager(&self) {
        debug!(
            "[ClusterManager] start_cluster_manager() called for node {:?}",
            self.inner.node_id
        );

        // Check if cluster management is disabled in config
        if !self.inner.config.enable_cluster_manager {
            debug!("ClusterManager disabled in configuration");
            info!("[ClusterManager] ClusterManager disabled in configuration");
            return;
        }

        // Check if already running
        let mut manager_guard = self.inner.cluster_manager.write().await;
        if manager_guard.is_some() {
            debug!("ClusterManager already running");
            info!("[ClusterManager] ClusterManager already running");
            return;
        }

        // Select the appropriate configuration based on preset
        use super::types::ClusterManagerPreset;
        let cluster_config = Arc::new(match self.inner.config.cluster_manager_preset {
            ClusterManagerPreset::Conservative => ClusterManagerConfig::conservative(),
            ClusterManagerPreset::Moderate => ClusterManagerConfig::moderate(),
            ClusterManagerPreset::Aggressive => ClusterManagerConfig::aggressive(),
        });

        info!(
            "Starting ClusterManager with preset: {:?}",
            self.inner.config.cluster_manager_preset
        );
        debug!(
            "[ClusterManager] Starting ClusterManager with preset: {:?}",
            self.inner.config.cluster_manager_preset
        );

        // Create a new ClusterManager instance
        let self_clone = Arc::new(self.clone());
        let cluster_manager = Arc::new(ClusterManager::new(
            cluster_config,
            self_clone,
            self.inner.cluster_event_sender.clone(),
            None, // TODO: Pass heartbeat_tracker once we integrate it
        ));

        // Start the manager
        info!("[ClusterManager] Calling cluster_manager.start()...");
        if let Err(e) = cluster_manager.start().await {
            error!("Failed to start ClusterManager: {}", e);
            info!("[ClusterManager] Failed to start: {}", e);
            return;
        }

        // Store the manager
        *manager_guard = Some(cluster_manager);
        info!("ClusterManager started successfully");
        info!("[ClusterManager] ClusterManager started successfully");
    }

    /// Stop the ClusterManager.
    ///
    /// This is called when this node loses leadership.
    async fn stop_cluster_manager(&self) {
        let mut manager_guard = self.inner.cluster_manager.write().await;
        if let Some(manager) = manager_guard.take() {
            if let Err(e) = manager.stop().await {
                error!("Error stopping ClusterManager: {}", e);
            } else {
                info!("ClusterManager stopped successfully");
            }
        }
    }

    /// Subscribe to cluster events.
    ///
    /// Returns a receiver channel for cluster events emitted by the ClusterManager.
    /// Only one subscriber is supported - subsequent calls will return None.
    pub async fn subscribe_cluster_events(&self) -> Option<mpsc::UnboundedReceiver<ClusterEvent>> {
        self.inner.cluster_event_receiver.write().await.take()
    }

    /// Process cluster events from the ClusterManager.
    ///
    /// This method runs in a background task and processes events emitted by the
    /// ClusterManager. It logs all events for observability and could be extended
    /// to emit metrics or trigger additional actions.
    async fn process_cluster_events(self: Arc<Self>) {
        // Try to take ownership of the receiver
        let mut receiver_guard = self.inner.cluster_event_receiver.write().await;
        let receiver = match receiver_guard.take() {
            Some(rx) => rx,
            None => {
                error!("ClusterManager event receiver already taken");
                return;
            }
        };
        drop(receiver_guard);

        info!("ClusterManager event processing task started");

        // Process events until the channel is closed
        let mut receiver = receiver;
        while let Some(event) = receiver.recv().await {
            match event {
                ClusterEvent::NodeHealthChanged {
                    node_id,
                    old_health,
                    new_health,
                    reason,
                } => {
                    info!(
                        node_id = ?node_id,
                        old_health = ?old_health,
                        new_health = ?new_health,
                        reason = %reason,
                        "Cluster event: Node health changed"
                    );
                }
                ClusterEvent::FailureDetected {
                    node_id,
                    consecutive_failures,
                    time_since_heartbeat,
                } => {
                    error!(
                        node_id = ?node_id,
                        consecutive_failures = consecutive_failures,
                        time_since_heartbeat = ?time_since_heartbeat,
                        "Cluster event: Node failure detected"
                    );
                }
                ClusterEvent::RecoveryDetected {
                    node_id,
                    consecutive_successes,
                } => {
                    info!(
                        node_id = ?node_id,
                        consecutive_successes = consecutive_successes,
                        "Cluster event: Node recovery detected"
                    );
                }
                ClusterEvent::MembershipChangeInitiated {
                    node_id,
                    action,
                    reason,
                } => {
                    info!(
                        node_id = ?node_id,
                        action = ?action,
                        reason = %reason,
                        "Cluster event: Membership change initiated"
                    );
                }
                ClusterEvent::MembershipChangeCompleted { node_id, action } => {
                    info!(
                        node_id = ?node_id,
                        action = ?action,
                        "Cluster event: Membership change completed"
                    );
                }
                ClusterEvent::MembershipChangeFailed {
                    node_id,
                    action,
                    error,
                } => {
                    error!(
                        node_id = ?node_id,
                        action = ?action,
                        error = %error,
                        "Cluster event: Membership change failed"
                    );
                }
                ClusterEvent::QuorumPreservationBlocked {
                    node_id,
                    action,
                    reason,
                } => {
                    error!(
                        node_id = ?node_id,
                        action = ?action,
                        reason = %reason,
                        "Cluster event: Membership change blocked to preserve quorum"
                    );
                }
                ClusterEvent::RateLimitTriggered {
                    node_id,
                    action,
                    reason,
                } => {
                    debug!(
                        node_id = ?node_id,
                        action = ?action,
                        reason = %reason,
                        "Cluster event: Membership change rate-limited"
                    );
                }
            }
        }

        info!("ClusterManager event processing task stopped");
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

        // Clone the heartbeat timing maps (cheap since they're just timestamps)
        // Use try_read() to avoid blocking in async contexts - if lock is held, return empty maps
        let heartbeat_sent = self
            .inner
            .heartbeat_sent
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default();
        let heartbeat_acked = self
            .inner
            .heartbeat_acked
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or_default();

        // Extract cluster membership information (available on all nodes)
        let membership = openraft_metrics.membership_config.membership();
        let voter_ids: std::collections::HashSet<_> = membership.voter_ids().collect();
        let learner_ids: std::collections::HashSet<_> = membership.learner_ids().collect();

        let mut cluster_members = Vec::new();
        // Add voters
        for node_id in voter_ids.iter() {
            cluster_members.push(super::types::ClusterMemberInfo {
                node_id: *node_id,
                is_voter: true,
            });
        }
        // Add learners
        for node_id in learner_ids.iter() {
            cluster_members.push(super::types::ClusterMemberInfo {
                node_id: *node_id,
                is_voter: false,
            });
        }
        // Sort by node_id for consistent ordering
        cluster_members.sort_by_key(|m| m.node_id.as_u64());

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
            cluster_members,
            replication_lag,
            heartbeat_sent,
            heartbeat_acked,
        }
    }
}

#[async_trait]
impl StorageRaftMember for StorageRaftMemberImpl {
    type Operation = WormFsOperation;
    type OperationResult = ();

    async fn new(
        node_id: NodeId,
        config: Config,
        metadata_store: MetadataStoreImpl,
    ) -> Result<Self, Error>
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

        // Use the external MetadataStore passed in
        info!("Using external metadata store (already initialized by caller)");

        // Create shared timing trackers for heartbeat monitoring
        let heartbeat_sent = Arc::new(RwLock::new(HashMap::new()));
        let heartbeat_acked = Arc::new(RwLock::new(HashMap::new()));

        // Create the adapters
        let log_storage = RaftLogStorageAdapter::new(log_store);
        let state_machine = WormFsStateMachine::new_with_config(
            metadata_store,
            config.snapshot_directory.clone(),
            config.enable_snapshot_compression,
            config.snapshot_compression_level,
        );

        // Initialize the state machine (creates snapshot directory, scans existing snapshots)
        state_machine.initialize().await.map_err(|e| {
            Error::ConfigError(format!("Failed to initialize state machine: {:?}", e))
        })?;

        // Get a handle to the state machine's inner for subscription access
        let state_machine_inner = state_machine.inner_handle();

        let network_factory = WormFsNetworkFactory::new(
            storage_network,
            heartbeat_sent.clone(),
            heartbeat_acked.clone(),
        );

        // Convert our Config to OpenRaft's config
        let raft_config = openraft::Config {
            heartbeat_interval: config.heartbeat_interval.as_millis() as u64,
            election_timeout_min: config.election_timeout_min.as_millis() as u64,
            election_timeout_max: config.election_timeout_max.as_millis() as u64,
            max_payload_entries: config.max_payload_entries,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
                config.snapshot_log_size_threshold / 1000, // Convert to entry count estimate
            ),
            max_in_snapshot_log_to_keep: 5, // Keep only 5 logs after snapshot for aggressive purging
            replication_lag_threshold: 10,  // Send snapshot if follower >10 entries behind
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
        let impl_instance = Self::new_with_raft(
            node_id,
            config,
            raft.clone(),
            state_machine_inner,
            heartbeat_sent,
            heartbeat_acked,
        );

        // Start a background task to monitor Raft metrics and update leadership state
        let impl_clone = impl_instance.clone();
        tokio::spawn(async move {
            let mut metrics_rx = impl_clone.inner.raft.metrics();
            debug!(
                "[Metrics Monitor] Starting metrics monitoring task for node {:?}",
                impl_clone.inner.node_id
            );

            // Track previous state to only log actual changes
            let mut prev_state: Option<openraft::ServerState> = None;
            let mut prev_leader: Option<NodeId> = None;

            loop {
                tokio::select! {
                    _ = metrics_rx.changed() => {
                        let metrics = metrics_rx.borrow_and_update().clone();

                        // Only log if state or leader actually changed
                        if prev_state != Some(metrics.state) || prev_leader != metrics.current_leader {
                            debug!("[Metrics Monitor] Node {:?} metrics changed: state={:?}, leader={:?}",
                                impl_clone.inner.node_id, metrics.state, metrics.current_leader);
                            prev_state = Some(metrics.state);
                            prev_leader = metrics.current_leader;
                        }

                        impl_clone.update_leadership(&metrics).await;
                    }
                }
            }
        });

        // Start a background task to process cluster events from ClusterManager
        let impl_clone = Arc::new(impl_instance.clone());
        tokio::spawn(async move {
            impl_clone.process_cluster_events().await;
        });

        info!(
            "StorageRaftMember created successfully for node {:?}",
            node_id
        );
        Ok(impl_instance)
    }

    async fn initialize(&mut self, peers: Vec<(NodeId, String)>) -> Result<(), Error> {
        info!(
            "Initializing Raft for node {:?} with {} peers",
            self.inner.node_id,
            peers.len()
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

            debug!("Calling raft.initialize() with {} members", members.len());

            let init_result = self.inner.raft.initialize(members).await;

            match &init_result {
                Ok(_) => {
                    debug!("raft.initialize() returned Ok");
                    info!(
                        "Successfully initialized single-node cluster for node {:?}",
                        self.inner.node_id
                    );
                }
                Err(e) => {
                    debug!("raft.initialize() returned Err: {:?}", e);
                }
            }

            init_result
                .map_err(|e| Error::RaftError(format!("Failed to initialize Raft: {:?}", e)))?;

            // Give the Raft core task time to process the initialization
            // initialize() is async - it queues the request and returns, actual processing happens later
            debug!("Waiting for Raft core to process initialization...");
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;

            // Check state after giving core time to process
            let metrics_after_init = self.inner.raft.metrics().borrow().clone();
            debug!(
                "State after raft.initialize() + delay: state={:?}, term={}, current_leader={:?}",
                metrics_after_init.state,
                metrics_after_init.current_term,
                metrics_after_init.current_leader
            );

            // For single-node clusters, the node automatically becomes leader after initialization.
            // No need to trigger an election manually - the 200ms delay above is sufficient
            // for the Raft core task to complete the state transition to Leader.
            info!("Single-node cluster initialization complete");
            Ok(())
        } else {
            // Multi-node cluster: initialize all nodes together
            info!(
                "Creating multi-node cluster for node {:?} with {} total members",
                self.inner.node_id,
                peers.len()
            );

            // Build membership from peer list
            let mut members = std::collections::BTreeMap::new();
            for (node_id, peer_id) in peers {
                let node = super::raft_config::WormFsNode {
                    peer_id: peer_id.clone(),
                    metadata: Some(super::raft_config::NodeMetadata {
                        name: Some(format!("node-{}", node_id.as_u64())),
                        version: Some(env!("CARGO_PKG_VERSION").to_string()),
                    }),
                };
                members.insert(node_id, node);
            }

            debug!("Calling raft.initialize() with {} members", members.len());

            let init_result = self.inner.raft.initialize(members).await;

            match &init_result {
                Ok(_) => {
                    debug!("raft.initialize() returned Ok");
                    info!(
                        "Successfully initialized multi-node cluster for node {:?}",
                        self.inner.node_id
                    );
                }
                Err(e) => {
                    debug!("raft.initialize() returned Err: {:?}", e);
                }
            }

            init_result
                .map_err(|e| Error::RaftError(format!("Failed to initialize Raft: {:?}", e)))?;

            // Give the Raft core task time to process the initialization
            debug!("Waiting for Raft core to process initialization...");
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            // Check state after initialization
            let metrics_after_init = self.inner.raft.metrics().borrow().clone();
            debug!(
                "State after raft.initialize() + delay: state={:?}, term={}, current_leader={:?}",
                metrics_after_init.state,
                metrics_after_init.current_term,
                metrics_after_init.current_leader
            );

            info!("Multi-node cluster initialization complete");
            Ok(())
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

        debug!(
            "[propose_operation] Node {:?}: About to call client_write()",
            self.inner.node_id
        );

        // Extract operation details for tracking before moving into client_write
        let (operation_type, tx_id, operation_count) = match &operation {
            WormFsOperation::AtomicTransaction {
                tx_id, operations, ..
            } => (
                "AtomicTransaction".to_string(),
                Some(tx_id.to_hex_short()),
                operations.len(),
            ),
            WormFsOperation::TransactionPrepare {
                tx_id,
                metadata_ops,
                command_ops,
                ..
            } => {
                let count = metadata_ops.as_ref().map(|v| v.len()).unwrap_or(0)
                    + command_ops.as_ref().map(|v| v.len()).unwrap_or(0);
                (
                    "TransactionPrepare".to_string(),
                    Some(tx_id.to_hex_short()),
                    count,
                )
            }
            WormFsOperation::TransactionCommit { tx_id } => (
                "TransactionCommit".to_string(),
                Some(tx_id.to_hex_short()),
                1,
            ),
            WormFsOperation::TransactionAbort { tx_id, .. } => (
                "TransactionAbort".to_string(),
                Some(tx_id.to_hex_short()),
                1,
            ),
        };

        let timestamp = std::time::SystemTime::now();

        // Submit the operation to Raft for replication and consensus
        let result = self.inner.raft.client_write(operation).await;

        // Track the proposal result
        let proposal_result = match &result {
            Ok(response) => {
                debug!(
                    "[propose_operation] Node {:?}: client_write() returned successfully! log_id={:?}",
                    self.inner.node_id, response.log_id
                );
                info!(
                    "Operation committed at log_id: {:?} for node {:?}",
                    response.log_id, self.inner.node_id
                );
                super::types::ProposalResult::Success
            }
            Err(e) => {
                debug!(
                    "[propose_operation] Node {:?}: client_write() returned error: {:?}",
                    self.inner.node_id, e
                );
                super::types::ProposalResult::Error(format!("{:?}", e))
            }
        };

        // Record the proposal in history
        {
            let mut history = self.inner.proposal_history.write().await;
            history.push_back(super::types::ProposalRecord {
                timestamp,
                operation_type,
                tx_id,
                operation_count,
                result: proposal_result,
            });
            // Keep only last 5 proposals
            while history.len() > 5 {
                history.pop_front();
            }
        }

        // Convert OpenRaft errors to our Error type
        let response = result.map_err(|e| match e {
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
        })?;

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

    async fn trigger_election(&self) -> Result<(), Error> {
        info!("Triggering election for node {:?}", self.inner.node_id);
        debug!(
            "trigger_election() called for node {:?}",
            self.inner.node_id
        );

        // Check state BEFORE election trigger
        let metrics_before = self.inner.raft.metrics().borrow().clone();
        debug!(
            "State BEFORE elect(): state={:?}, term={}, current_leader={:?}",
            metrics_before.state, metrics_before.current_term, metrics_before.current_leader
        );

        // Trigger OpenRaft to start an election immediately
        let result = self.inner.raft.trigger().elect().await;

        match &result {
            Ok(_) => {
                debug!(
                    "trigger().elect() succeeded for node {:?}",
                    self.inner.node_id
                );

                // Check state immediately after election trigger
                let metrics = self.inner.raft.metrics().borrow().clone();
                debug!(
                    "State immediately after elect(): state={:?}, term={}, current_leader={:?}",
                    metrics.state, metrics.current_term, metrics.current_leader
                );

                info!(
                    "Election trigger completed for node {:?}",
                    self.inner.node_id
                );
            }
            Err(e) => {
                debug!(
                    "trigger().elect() FAILED for node {:?}: {:?}",
                    self.inner.node_id, e
                );
            }
        }

        result.map_err(|e| Error::RaftError(format!("Failed to trigger election: {:?}", e)))?;
        Ok(())
    }

    async fn add_node(
        &self,
        node_id: NodeId,
        address: SocketAddr,
        peer_id: String,
    ) -> Result<(), Error> {
        info!(
            "Adding node {:?} with address {:?} and peer_id {} to cluster",
            node_id, address, peer_id
        );

        // Check if this node is the leader
        if !self.is_leader() {
            let leader = self.inner.current_leader.read().await;
            return Err(Error::NotLeader { leader: *leader });
        }

        // Validate peer_id format by attempting to parse it
        libp2p::PeerId::from_str(&peer_id)
            .map_err(|e| Error::ConfigError(format!("Invalid peer_id format: {}", e)))?;

        // Clone peer_id for logging before moving it into node struct
        let peer_id_for_logging = peer_id.clone();

        // Create node information
        let node = super::raft_config::WormFsNode {
            peer_id,
            metadata: Some(super::raft_config::NodeMetadata {
                name: Some(format!("node-{}", node_id.as_u64())),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
            }),
        };

        // Two-step process: add as learner first, then promote to voter
        // This allows the node to catch up on the log before participating in consensus

        // Step 1: Add as learner using the dedicated add_learner API
        // Use blocking=true to wait for the learner to catch up before returning
        info!(
            "Step 1: Adding node {:?} (peer_id={}) as learner (blocking until caught up)",
            node_id, peer_id_for_logging
        );
        info!("Calling raft.add_learner() - this will block until learner catches up...");

        let add_learner_result = self
            .inner
            .raft
            .add_learner(node_id, node, true) // true = blocking (wait for learner to catch up)
            .await;

        info!("raft.add_learner() returned: {:?}", add_learner_result);

        add_learner_result.map_err(|e| {
            Error::MembershipChangeFailed(format!("Failed to add node as learner: {:?}", e))
        })?;

        info!("Learner {:?} has caught up with the log", node_id);

        info!("Step 2: Promoting node {:?} to voter", node_id);
        // Step 2: Promote learner to voter using change_membership
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
        use super::state_machine::Subscription;

        // Create a broadcast channel for this subscription
        let capacity = 100;
        let (sender, receiver) = tokio::sync::broadcast::channel(capacity);

        // Add subscription to the state machine's subscription list
        let mut inner = self.inner.state_machine_inner.write().await;
        inner.subscriptions.push(Subscription { sender, filter });

        // Create an unbounded channel to forward events to (for compatibility with existing API)
        let (tx, rx) = mpsc::unbounded_channel();

        // Spawn a task to forward events from broadcast to unbounded
        let mut broadcast_rx = receiver;
        tokio::spawn(async move {
            while let Ok(event) = broadcast_rx.recv().await {
                if tx.send(event).is_err() {
                    // Receiver dropped, exit the forwarding task
                    break;
                }
            }
        });

        rx
    }

    #[tracing::instrument(skip(self, request), fields(node_id = %self.inner.node_id.0))]
    async fn handle_raft_rpc(&self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        use super::raft_member::{RaftRpcMessage, RaftRpcResponse};

        // Deserialize the incoming RPC request
        let rpc_message: RaftRpcMessage = bincode::deserialize(&request)
            .map_err(|e| Error::RaftError(format!("Failed to deserialize Raft RPC: {:?}", e)))?;

        // Handle the RPC based on its type by calling the appropriate Raft method
        let response = match rpc_message {
            RaftRpcMessage::Vote(vote_req) => {
                debug!(
                    "[Node {:?}] Handling Vote RPC from term {}",
                    self.inner.node_id, vote_req.vote.committed
                );

                // Catch panics to prevent bad actors from crashing the leader with malformed Vote RPCs
                let result = std::panic::AssertUnwindSafe(self.inner.raft.vote(vote_req))
                    .catch_unwind()
                    .await;

                match result {
                    Ok(Ok(resp)) => RaftRpcResponse::Vote(resp),
                    Ok(Err(e)) => {
                        debug!("[Node {:?}] Vote RPC error: {:?}", self.inner.node_id, e);
                        return Err(Error::RaftError(format!("Vote RPC failed: {:?}", e)));
                    }
                    Err(panic_err) => {
                        debug!(
                            "[Node {:?}] Vote RPC caused panic (rejected): {:?}",
                            self.inner.node_id, panic_err
                        );
                        return Err(Error::RaftError(
                            "Vote RPC rejected due to internal error".to_string(),
                        ));
                    }
                }
            }
            RaftRpcMessage::AppendEntries(append_req) => {
                // Log current state to understand why we're panicking
                let metrics = self.inner.raft.metrics().borrow().clone();
                debug!("[Node {:?}] Handling AppendEntries RPC: term={}, prev_log_index={:?}, entries={}, leader_committed={:?}",
                         self.inner.node_id, append_req.vote.leader_id.term,
                         append_req.prev_log_id, append_req.entries.len(), append_req.leader_commit);
                debug!("[Node {:?}] Current state: last_log={:?}, last_applied={:?}, snapshot={:?}, state={:?}, is_initialized={}",
                         self.inner.node_id, metrics.last_log_index, metrics.last_applied,
                         metrics.snapshot, metrics.state, self.inner.raft.is_initialized().await.unwrap_or(false));

                let mut max_id = 0;
                // Log individual entries for detailed debugging
                for (idx, entry) in append_req.entries.iter().enumerate() {
                    trace!(
                        "[Node {:?}] AppendEntries entry[{}]: log_id={:?} (term={}, index={})",
                        self.inner.node_id,
                        idx,
                        entry.log_id,
                        entry.log_id.leader_id.term,
                        entry.log_id.index
                    );

                    if max_id < entry.log_id.index {
                        max_id = entry.log_id.index;
                    }
                }

                // Catch panics to prevent bad actors from crashing nodes with malformed AppendEntries
                let result =
                    std::panic::AssertUnwindSafe(self.inner.raft.append_entries(append_req))
                        .catch_unwind()
                        .await;

                match result {
                    Ok(Ok(resp)) => {
                        debug!(
                            "[Node {:?}] Sending AppendEntries response: success={:?} - {:?}",
                            self.inner.node_id,
                            resp.is_success(),
                            resp
                        );
                        RaftRpcResponse::AppendEntries(resp)
                    }
                    Ok(Err(e)) => {
                        debug!(
                            "[Node {:?}] AppendEntries error: {:?}",
                            self.inner.node_id, e
                        );
                        return Err(Error::RaftError(format!(
                            "AppendEntries RPC failed: {:?}",
                            e
                        )));
                    }
                    Err(panic_err) => {
                        debug!(
                            "[Node {:?}] AppendEntries caused panic (rejected): {:?}",
                            self.inner.node_id, panic_err
                        );
                        return Err(Error::RaftError(
                            "AppendEntries rejected due to internal error".to_string(),
                        ));
                    }
                }
            }
            RaftRpcMessage::InstallSnapshot(snapshot_req) => {
                debug!(
                    "[Node {:?}] Handling InstallSnapshot RPC: term={}, last_included={:?}",
                    self.inner.node_id,
                    snapshot_req.vote.leader_id.term,
                    snapshot_req.meta.last_log_id
                );

                // Catch panics to prevent bad actors from crashing nodes with malformed InstallSnapshot
                let result =
                    std::panic::AssertUnwindSafe(self.inner.raft.install_snapshot(snapshot_req))
                        .catch_unwind()
                        .await;

                match result {
                    Ok(Ok(resp)) => {
                        debug!(
                            "[Node {:?}] InstallSnapshot completed successfully",
                            self.inner.node_id
                        );
                        RaftRpcResponse::InstallSnapshot(resp)
                    }
                    Ok(Err(e)) => {
                        debug!(
                            "[Node {:?}] InstallSnapshot error: {:?}",
                            self.inner.node_id, e
                        );
                        return Err(Error::RaftError(format!(
                            "InstallSnapshot RPC failed: {:?}",
                            e
                        )));
                    }
                    Err(panic_err) => {
                        debug!(
                            "[Node {:?}] InstallSnapshot caused panic (rejected): {:?}",
                            self.inner.node_id, panic_err
                        );
                        return Err(Error::RaftError(
                            "InstallSnapshot rejected due to internal error".to_string(),
                        ));
                    }
                }
            }
        };

        // Serialize the response
        bincode::serialize(&response).map_err(|e| {
            Error::RaftError(format!("Failed to serialize Raft RPC response: {:?}", e))
        })
    }
}

impl StorageRaftMemberImpl {
    /// Get access to the inner state (for testing).
    ///
    /// This is primarily used by integration tests that need direct access
    /// to the OpenRaft instance for advanced initialization scenarios.
    pub fn inner(&self) -> &Inner {
        &self.inner
    }

    /// Start a background task to periodically update the network's heartbeat data
    /// with the current Raft state.
    ///
    /// This should be called after the Raft node is fully initialized and the
    /// network layer is running.
    ///
    /// # Arguments
    ///
    /// * `network` - The network handle to update with heartbeat data
    /// * `interval_secs` - How often to update the heartbeat data (defaults to 1 second)
    pub fn start_heartbeat_updater(
        &self,
        network: Arc<crate::storage_network::StorageNetworkHandle>,
        interval_secs: Option<u64>,
    ) {
        let member = self.clone();
        let interval = std::time::Duration::from_secs(interval_secs.unwrap_or(1));

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;

                // Get current Raft metrics
                let metrics = member.inner.raft.metrics().borrow().clone();

                // Extract Raft state information
                let raft_state = format!("{:?}", metrics.state);
                let raft_term = Some(metrics.current_term);
                let last_log_index = metrics.last_log_index;
                let last_log_term = metrics.last_applied.map(|log_id| log_id.leader_id.term);
                let current_leader = metrics.current_leader.map(|id| id.0); // Extract inner u64
                                                                            // Check if this node is a voter by seeing if it's in the voters set
                let voters = metrics
                    .membership_config
                    .membership()
                    .voter_ids()
                    .collect::<Vec<_>>();
                let is_voter = voters.contains(&&member.inner.node_id);
                let startup_time = Some(member.inner.startup_time);

                // Update the network layer's heartbeat data
                if let Err(e) = network
                    .update_raft_heartbeat_data(
                        Some(raft_state),
                        raft_term,
                        last_log_index,
                        last_log_term,
                        current_leader,
                        Some(is_voter),
                        startup_time,
                    )
                    .await
                {
                    error!("Failed to update heartbeat data: {}", e);
                }
            }
        });
    }
}

// Implement RaftRpcHandler trait so StorageRaftMemberImpl can be registered with the network
#[async_trait]
impl super::raft_member::RaftRpcHandler for StorageRaftMemberImpl {
    async fn handle_raft_rpc(&self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        // Delegate to the StorageRaftMember trait method
        <Self as StorageRaftMember>::handle_raft_rpc(self, request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tx_id_generation() {
        // Test that generated TxIds are unique
        let tx_id1 = TxId::generate();
        let tx_id2 = TxId::generate();
        assert_ne!(tx_id1, tx_id2);

        // Test that manual creation works for testing
        let tx_id3 = TxId::new(123);
        let tx_id4 = TxId::new(456);
        assert_ne!(tx_id3, tx_id4);
    }
}
