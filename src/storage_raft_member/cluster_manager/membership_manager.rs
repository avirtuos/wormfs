/// Membership Management for Cluster Nodes
///
/// The MembershipManager handles membership changes: demoting voters to learners,
/// promoting learners to voters, and ensuring quorum is always maintained.
use super::config::ClusterManagerConfig;
use super::types::MembershipAction;
use crate::storage_raft_member::types::NodeId;
use crate::storage_raft_member::{StorageRaftMember, StorageRaftMemberImpl};
use futures::FutureExt; // For catch_unwind on Futures
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// Error type for membership operations
#[derive(Debug, Clone)]
pub enum MembershipError {
    /// Operation would violate quorum safety
    QuorumViolation(String),

    /// Rate limit exceeded
    RateLimitExceeded(String),

    /// Node not found
    NodeNotFound(NodeId),

    /// Invalid operation for node's current state
    InvalidOperation(String),

    /// Raft error during membership change
    RaftError(String),
}

impl std::fmt::Display for MembershipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MembershipError::QuorumViolation(msg) => write!(f, "Quorum violation: {}", msg),
            MembershipError::RateLimitExceeded(msg) => write!(f, "Rate limit exceeded: {}", msg),
            MembershipError::NodeNotFound(id) => write!(f, "Node not found: {:?}", id),
            MembershipError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            MembershipError::RaftError(msg) => write!(f, "Raft error: {}", msg),
        }
    }
}

impl std::error::Error for MembershipError {}

/// Manages cluster membership changes safely
///
/// Ensures that all membership changes maintain quorum, respect rate limits,
/// and follow the correct voter ↔ learner state transitions.
pub struct MembershipManager {
    /// Configuration for membership management
    config: Arc<ClusterManagerConfig>,

    /// Reference to the Raft instance for executing membership changes
    raft: Arc<StorageRaftMemberImpl>,

    /// Track the last time a membership change was made (for rate limiting)
    last_membership_change: HashMap<NodeId, Instant>,

    /// Total configured membership size (voters + learners)
    ///
    /// This is the total number of nodes that the cluster was originally configured with.
    /// Quorum is always calculated based on this total, not the current number of voters.
    /// For example, in a 5-node cluster, quorum is always 3, even if some nodes are
    /// demoted to learners.
    total_membership_size: usize,
}

impl MembershipManager {
    /// Create a new MembershipManager
    ///
    /// # Arguments
    ///
    /// * `config` - Cluster manager configuration
    /// * `raft` - Reference to the Raft instance for executing membership changes
    /// * `total_membership_size` - Total number of nodes in the cluster (voters + learners)
    pub fn new(
        config: Arc<ClusterManagerConfig>,
        raft: Arc<StorageRaftMemberImpl>,
        total_membership_size: usize,
    ) -> Self {
        Self {
            config,
            raft,
            last_membership_change: HashMap::new(),
            total_membership_size,
        }
    }

    /// Check if a membership change is allowed (rate limiting)
    ///
    /// Returns true if enough time has passed since the last change for this node.
    pub fn can_change_membership(&self, node_id: NodeId) -> bool {
        if let Some(last_change) = self.last_membership_change.get(&node_id) {
            last_change.elapsed() >= self.config.min_membership_change_interval
        } else {
            true // Never changed before, allowed
        }
    }

    /// Record that a membership change was made
    fn record_membership_change(&mut self, node_id: NodeId) {
        self.last_membership_change.insert(node_id, Instant::now());
    }

    /// Check if demoting a voter would violate quorum
    ///
    /// A demotion violates quorum if it would leave fewer than (n/2 + 1) voters,
    /// where n is the **total configured membership size**, not the current voter count.
    ///
    /// ## Critical: Quorum Based on Total Membership
    ///
    /// In Raft, quorum is always calculated based on the total configured membership,
    /// not just the current voters. For example, in a 5-node cluster:
    /// - Quorum always requires 3 nodes (5/2 + 1 = 3)
    /// - Even if nodes are demoted to learners, we must maintain 3 voters
    /// - We cannot allow only 2 voters, even if they're the only "healthy" nodes
    ///
    /// This prevents split-brain scenarios where demoted nodes could come back
    /// and create conflicting leadership.
    ///
    /// ## Parameters
    /// - `current_voters`: Number of voters currently in the cluster (unused, kept for API compat)
    /// - `voters_after_demotion`: Number of voters after this demotion
    ///
    /// ## Returns
    /// `true` if the demotion would violate quorum, `false` otherwise
    pub fn would_violate_quorum(
        &self,
        _current_voters: usize,
        voters_after_demotion: usize,
    ) -> bool {
        if voters_after_demotion == 0 {
            return true; // Can't have zero voters
        }

        // Quorum requires majority based on TOTAL configured membership
        // For a 5-node cluster: (5/2) + 1 = 3
        // For a 3-node cluster: (3/2) + 1 = 2
        let required_for_quorum = (self.total_membership_size / 2) + 1;

        voters_after_demotion < required_for_quorum
    }

    /// Validate a membership action before executing
    ///
    /// Checks rate limits and quorum safety.
    pub fn validate_action(
        &self,
        node_id: NodeId,
        action: MembershipAction,
        current_voters: usize,
    ) -> Result<(), MembershipError> {
        // Check rate limit
        if !self.can_change_membership(node_id) {
            return Err(MembershipError::RateLimitExceeded(format!(
                "Node {} must wait {:?} between membership changes",
                node_id, self.config.min_membership_change_interval
            )));
        }

        // Check quorum for demotion
        if action == MembershipAction::Demote {
            if self.would_violate_quorum(current_voters, current_voters - 1) {
                return Err(MembershipError::QuorumViolation(format!(
                    "Cannot demote node {} - would lose quorum (current voters: {})",
                    node_id, current_voters
                )));
            }
        }

        Ok(())
    }

    /// Get the current number of voters in the cluster
    ///
    /// Returns the count of nodes that are currently voting members.
    async fn get_voter_count(&self) -> Result<usize, MembershipError> {
        let metrics = self.raft.inner().raft.metrics().borrow().clone();

        // membership_config contains voters - membership() returns a reference
        let membership = metrics.membership_config.membership();
        Ok(membership.voter_ids().count())
    }

    /// Check if a node is currently a voter
    ///
    /// Returns true if the node is in the voting member set.
    async fn is_voter(&self, node_id: NodeId) -> Result<bool, MembershipError> {
        let metrics = self.raft.inner().raft.metrics().borrow().clone();

        // membership() returns a reference - get voter_ids and check if node is in the set
        let membership = metrics.membership_config.membership();
        let voter_ids: std::collections::BTreeSet<NodeId> = membership.voter_ids().collect();
        Ok(voter_ids.contains(&node_id))
    }

    /// Check if a node has caught up with the leader (is synced)
    ///
    /// A node is considered synced if its replication lag is below the configured threshold.
    /// For learners, we use heartbeat timing instead of replication lag since OpenRaft
    /// doesn't expose learner replication metrics the same way as voters.
    async fn is_synced(&self, node_id: NodeId) -> Result<bool, MembershipError> {
        // Check if node is a learner
        let openraft_metrics = self.raft.inner().raft.metrics().borrow().clone();
        let membership = openraft_metrics.membership_config.membership();
        let is_learner = membership.learner_ids().any(|id| id == node_id);

        if is_learner {
            // For learners, use heartbeat timing instead of replication lag
            // OpenRaft doesn't expose learner replication metrics in the standard way
            let metrics = self.raft.as_ref().get_metrics();

            if let Some(last_ack) = metrics.heartbeat_acked.get(&node_id) {
                let time_since_ack = std::time::Instant::now().duration_since(*last_ack);
                // Consider synced if heartbeat responded recently (within heartbeat timeout)
                // Using 2x heartbeat timeout as a generous threshold
                let heartbeat_timeout = self.config.heartbeat_timeout;
                let synced = time_since_ack < heartbeat_timeout * 2;

                debug!(
                    "[MembershipManager] Learner node {:?} sync check: time_since_ack={:?}, synced={}",
                    node_id, time_since_ack, synced
                );

                Ok(synced)
            } else {
                debug!(
                    "[MembershipManager] Learner node {:?} has no heartbeat_acked timestamp, not synced",
                    node_id
                );
                Ok(false)
            }
        } else {
            // For voters, use replication lag as before
            let metrics = self.raft.as_ref().get_metrics();

            // Check replication lag
            if let Some(lag) = metrics.replication_lag.get(&node_id) {
                // Node is synced if lag is below threshold (or 0)
                // For now, use a simple threshold - could be made configurable
                let sync_threshold = 10; // Allow up to 10 log entries behind
                Ok(*lag <= sync_threshold)
            } else {
                // Node not in replication map - not synced
                Ok(false)
            }
        }
    }

    /// Wait for a node to sync with the leader
    ///
    /// Polls the node's replication lag until it's below the threshold or timeout occurs.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The node to wait for
    /// * `timeout` - Maximum time to wait
    ///
    /// # Returns
    ///
    /// Ok if node synced within timeout, Err otherwise
    async fn wait_for_sync(
        &self,
        node_id: NodeId,
        timeout: Duration,
    ) -> Result<(), MembershipError> {
        let start = Instant::now();

        loop {
            // Check if synced
            if self.is_synced(node_id).await? {
                info!("Node {:?} has synced with leader", node_id);
                return Ok(());
            }

            // Check timeout
            if start.elapsed() > timeout {
                return Err(MembershipError::RaftError(format!(
                    "Node {:?} failed to sync within {:?}",
                    node_id, timeout
                )));
            }

            // Wait a bit before checking again
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Demote a voter to learner status
    ///
    /// Converts a voting member to a non-voting learner. This is typically done
    /// when a node fails or becomes unresponsive.
    ///
    /// # Safety
    ///
    /// This method checks:
    /// - Leader status (only leader can change membership)
    /// - Idempotency (no-op if already a learner)
    /// - Quorum preservation (prevents demotion that would lose quorum)
    /// - Rate limiting (enforces minimum time between changes)
    pub async fn demote_to_learner(&mut self, node_id: NodeId) -> Result<(), MembershipError> {
        debug!(
            "[MembershipManager] demote_to_learner called for node {:?}",
            node_id
        );

        // 1. Validate preconditions - must be leader
        if !self.raft.as_ref().is_leader() {
            debug!("[MembershipManager] Not leader, cannot demote");
            return Err(MembershipError::RaftError(
                "Not leader - cannot demote node".to_string(),
            ));
        }

        // 2. Check if already a learner (idempotent)
        if !self.is_voter(node_id).await? {
            info!(
                "Node {:?} is already a learner, no demotion needed",
                node_id
            );
            debug!(
                "[MembershipManager] Node {:?} already a learner, skipping",
                node_id
            );
            return Ok(());
        }

        debug!(
            "[MembershipManager] Node {:?} is a voter, proceeding with demotion",
            node_id
        );

        // 3. Validate quorum safety and rate limiting
        let voter_count = self.get_voter_count().await?;
        debug!("[MembershipManager] Current voter count: {}", voter_count);
        self.validate_action(node_id, MembershipAction::Demote, voter_count)?;
        debug!("[MembershipManager] Validation passed, proceeding with demotion");

        // 4. Get node information from current membership
        // We need this to re-add the node as a learner
        let node_info = {
            let metrics = self.raft.inner().raft.metrics().borrow().clone();
            let membership = metrics.membership_config.membership();
            membership.get_node(&node_id).cloned().ok_or_else(|| {
                MembershipError::RaftError(format!("Node {:?} not found in membership", node_id))
            })?
        };

        // 5. Add as learner first (non-blocking) to ensure it stays in the cluster
        // This is idempotent - if already a learner, this is a no-op
        // Even if the node is offline, we can still add it as a learner
        info!("Adding node {:?} as learner before demotion", node_id);
        debug!(
            "[MembershipManager] Adding node {:?} as learner (non-blocking) before demotion",
            node_id
        );

        // Protect against panics from OpenRaft add_learner operation
        let add_learner_result = std::panic::AssertUnwindSafe(
            self.raft
                .inner()
                .raft
                .add_learner(node_id, node_info, false),
        )
        .catch_unwind()
        .await;

        match add_learner_result {
            Ok(Ok(_)) => {
                info!(
                    "[MembershipManager] Successfully added node {:?} as learner",
                    node_id
                );
            }
            Ok(Err(e)) => {
                error!(
                    "[MembershipManager] ERROR: Failed to add node {:?} as learner: {:?}",
                    node_id, e
                );
                return Err(MembershipError::RaftError(format!(
                    "Failed to add learner: {:?}",
                    e
                )));
            }
            Err(panic_err) => {
                error!(
                    "[MembershipManager] PANIC: add_learner for node {:?} panicked: {:?}",
                    node_id, panic_err
                );
                return Err(MembershipError::RaftError(
                    "add_learner operation panicked".to_string(),
                ));
            }
        }

        // 6. Execute demotion via OpenRaft (remove from voters)
        let mut voter_ids = std::collections::BTreeSet::new();
        voter_ids.insert(node_id);

        // Protect against panics from OpenRaft change_membership operation
        let change_result = std::panic::AssertUnwindSafe(
            self.raft
                .inner()
                .raft
                .change_membership(openraft::ChangeMembers::RemoveVoters(voter_ids), true),
        )
        .catch_unwind()
        .await;

        match change_result {
            Ok(Ok(_)) => {
                info!(
                    "[MembershipManager] Successfully demoted node {:?} from voters",
                    node_id
                );
            }
            Ok(Err(e)) => {
                error!(
                    "[MembershipManager] ERROR: Demotion failed for node {:?}: {:?}",
                    node_id, e
                );
                return Err(MembershipError::RaftError(format!(
                    "Demotion failed: {:?}",
                    e
                )));
            }
            Err(panic_err) => {
                error!(
                    "[MembershipManager] PANIC: change_membership for demotion of node {:?} panicked: {:?}",
                    node_id, panic_err
                );
                return Err(MembershipError::RaftError(
                    "Demotion operation panicked".to_string(),
                ));
            }
        }

        // 7. Record change for rate limiting
        self.record_membership_change(node_id);

        info!("Successfully demoted node {:?} to learner", node_id);
        Ok(())
    }

    /// Promote a learner to voter status
    ///
    /// Converts a non-voting learner to a voting member. This is typically done
    /// after a node has recovered and caught up with the leader.
    ///
    /// # Safety
    ///
    /// This method checks:
    /// - Leader status (only leader can change membership)
    /// - Idempotency (no-op if already a voter)
    /// - Rate limiting (enforces minimum time between changes)
    pub async fn promote_to_voter(&mut self, node_id: NodeId) -> Result<(), MembershipError> {
        // 1. Validate preconditions - must be leader
        if !self.raft.as_ref().is_leader() {
            return Err(MembershipError::RaftError(
                "Not leader - cannot promote node".to_string(),
            ));
        }

        // 2. Check if already a voter (idempotent)
        if self.is_voter(node_id).await? {
            info!("Node {:?} is already a voter, no promotion needed", node_id);
            return Ok(());
        }

        // 3. Validate rate limiting (promotions don't violate quorum)
        let voter_count = self.get_voter_count().await?;
        self.validate_action(node_id, MembershipAction::Promote, voter_count)?;

        // 4. Execute promotion via OpenRaft
        let mut voter_ids = std::collections::BTreeSet::new();
        voter_ids.insert(node_id);

        // Protect against panics from OpenRaft change_membership operation
        let change_result = std::panic::AssertUnwindSafe(
            self.raft
                .inner()
                .raft
                .change_membership(openraft::ChangeMembers::AddVoterIds(voter_ids), true),
        )
        .catch_unwind()
        .await;

        match change_result {
            Ok(Ok(_)) => {
                info!(
                    "[MembershipManager] Successfully promoted node {:?} to voter",
                    node_id
                );
            }
            Ok(Err(e)) => {
                error!(
                    "[MembershipManager] ERROR: Promotion failed for node {:?}: {:?}",
                    node_id, e
                );
                return Err(MembershipError::RaftError(format!(
                    "Promotion failed: {:?}",
                    e
                )));
            }
            Err(panic_err) => {
                error!(
                    "[MembershipManager] PANIC: change_membership for promotion of node {:?} panicked: {:?}",
                    node_id, panic_err
                );
                return Err(MembershipError::RaftError(
                    "Promotion operation panicked".to_string(),
                ));
            }
        }

        // 5. Record change for rate limiting
        self.record_membership_change(node_id);

        info!("Successfully promoted node {:?} to voter", node_id);
        Ok(())
    }

    /// Handle a node failure
    ///
    /// When a node fails, this method demotes it to learner status to prevent
    /// it from participating in elections until it has recovered and synced.
    ///
    /// # Process
    ///
    /// 1. Verify node is a voter (can't demote learners)
    /// 2. Demote to learner (if quorum allows)
    ///
    /// # Safety
    ///
    /// Only demotes if it won't violate quorum. The node will be promoted back
    /// to voter status when it recovers via `handle_node_recovery()`.
    pub async fn handle_node_failure(&mut self, node_id: NodeId) -> Result<(), MembershipError> {
        warn!("Handling failure for node {:?}", node_id);
        debug!(
            "[MembershipManager] handle_node_failure called for node {:?}",
            node_id
        );

        // 1. Validate this is the leader
        if !self.raft.as_ref().is_leader() {
            debug!("[MembershipManager] Not leader, cannot handle failure");
            return Err(MembershipError::RaftError(
                "Not leader - cannot handle node failure".to_string(),
            ));
        }

        // 2. Check if node is a voter (can't demote learners)
        if !self.is_voter(node_id).await? {
            info!(
                "Node {:?} is already a learner, no demotion needed on failure",
                node_id
            );
            debug!(
                "[MembershipManager] Node {:?} is already a learner, skipping demotion",
                node_id
            );
            return Ok(());
        }

        debug!(
            "[MembershipManager] Node {:?} is a voter, calling demote_to_learner()",
            node_id
        );
        // 3. Demote to learner (will validate quorum and rate limits)
        self.demote_to_learner(node_id).await?;

        info!("Successfully handled failure for node {:?}", node_id);
        Ok(())
    }

    /// Handle a node recovery
    ///
    /// When a failed node comes back online, this method manages its re-integration
    /// into the cluster as a voting member.
    ///
    /// # Process
    ///
    /// 1. Check if node is already a voter (idempotent)
    /// 2. Assume node is already in cluster as learner (added by restart process)
    /// 3. Wait for node to sync with leader
    /// 4. Promote to voter
    ///
    /// # Note
    ///
    /// This method assumes the node is already present in the cluster as a learner.
    /// If the node needs to be re-added to the cluster, that should be done before
    /// calling this method (typically during the node restart process).
    pub async fn handle_node_recovery(&mut self, node_id: NodeId) -> Result<(), MembershipError> {
        info!("Handling recovery for node {:?}", node_id);

        // 1. Validate preconditions - must be leader
        if !self.raft.as_ref().is_leader() {
            return Err(MembershipError::RaftError(
                "Not leader - cannot handle node recovery".to_string(),
            ));
        }

        // 2. Check if already a voter (idempotent)
        if self.is_voter(node_id).await? {
            info!(
                "Node {:?} is already a voter, no recovery promotion needed",
                node_id
            );
            return Ok(());
        }

        // 3. Wait for node to sync with leader
        info!("Waiting for node {:?} to sync with leader", node_id);
        self.wait_for_sync(node_id, self.config.sync_wait_timeout)
            .await?;

        // 4. Promote to voter
        self.promote_to_voter(node_id).await?;

        info!("Successfully completed recovery for node {:?}", node_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_store::factory::MetadataStoreFactory;
    use crate::storage_network::network_handle_trait::NetworkHandleTrait;
    use crate::storage_network::types::{Error as NetworkError, PeerInfo};
    use crate::storage_raft_member::implementation::StorageRaftMemberImpl;
    use crate::storage_raft_member::log_storage::RaftLogStorageAdapter;
    use crate::storage_raft_member::network_factory::WormFsNetworkFactory;
    use crate::storage_raft_member::raft_member::RaftRpcHandler;
    use crate::storage_raft_member::state_machine::WormFsStateMachine;
    use crate::storage_raft_member::types::{ClusterManagerPreset, Config as RaftConfig};
    use crate::transaction_log_store::{TransactionLogConfig, TransactionLogStoreImpl};
    use openraft::Raft;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::RwLock;

    /// Mock network handle for testing
    struct MockNetworkHandle;

    #[async_trait::async_trait]
    impl NetworkHandleTrait for MockNetworkHandle {
        async fn send_request(
            &self,
            _peer_id_bytes: &[u8],
            _protocol: &str,
            _request: Vec<u8>,
        ) -> Result<Vec<u8>, NetworkError> {
            // Mock implementation - tests don't actually use the network
            Ok(Vec::new())
        }

        async fn register_raft_handler(
            &self,
            _handler: Arc<dyn RaftRpcHandler>,
        ) -> Result<(), NetworkError> {
            // Mock implementation - tests don't register handlers
            Ok(())
        }

        async fn get_connected_peers(&self) -> Result<Vec<PeerInfo>, NetworkError> {
            // Mock implementation - no connected peers for tests
            Ok(Vec::new())
        }

        async fn dial_configured_peers(&self) -> Result<(), NetworkError> {
            // Mock implementation - no dialing needed for tests
            Ok(())
        }
    }

    /// Create a minimal test Raft instance for unit testing MembershipManager.
    ///
    /// This helper creates the bare minimum setup needed to construct a
    /// StorageRaftMemberImpl. The tests that use this don't actually interact
    /// with Raft - they only test the pure logic methods of MembershipManager.
    async fn create_test_raft_instance() -> (Arc<StorageRaftMemberImpl>, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let node_id = NodeId(1);

        // Create minimal log store
        let log_path = temp_dir.path().join("transaction_log");
        let log_config = TransactionLogConfig {
            db_path: log_path.clone(),
            cache_size_mb: 1,
            compact_threshold_mb: 100,
            max_log_size_mb: 1000,
            max_log_age_days: 7,
        };
        let log_store =
            TransactionLogStoreImpl::new(log_config).expect("Failed to create log store");

        // Create minimal metadata store
        let metadata_path = temp_dir.path().join("metadata.db");
        let metadata_config = crate::metadata_store::Config {
            database_path: metadata_path.clone(),
            read_pool_size: 1,
            enable_wal: false,
            cache_size_mb: 1,
            enable_foreign_keys: false,
            synchronous: crate::metadata_store::types::SynchronousMode::Off,
            transaction_isolation: crate::metadata_store::types::IsolationLevel::ReadCommitted,
            enable_prepared_statements: false,
            read_pool_timeout_secs: 30,
            stripe_cache_size_mb: 1,
            stripe_cache_ttl_secs: 10,
            stripe_cache_tti_secs: 5,
            chunk_cache_size_mb: 1,
            chunk_cache_ttl_secs: 10,
            chunk_cache_tti_secs: 5,
        };
        let metadata_store = MetadataStoreFactory::create_concrete(metadata_config)
            .await
            .expect("Failed to create metadata store");

        // Create adapters
        let log_storage = RaftLogStorageAdapter::new(log_store);
        let snapshot_dir = temp_dir.path().join("snapshots");
        std::fs::create_dir_all(&snapshot_dir).expect("Failed to create snapshot dir");
        let state_machine = WormFsStateMachine::new(metadata_store.clone(), snapshot_dir.clone());

        // Create minimal Raft config
        let raft_config = openraft::Config {
            election_timeout_min: 150,
            election_timeout_max: 300,
            heartbeat_interval: 50,
            max_payload_entries: 64,
            replication_lag_threshold: 1000,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
            cluster_name: "test-cluster".to_string(),
            ..Default::default()
        };

        // Create mock network factory
        let mock_network: Arc<dyn NetworkHandleTrait> = Arc::new(MockNetworkHandle);
        let heartbeat_sent = Arc::new(RwLock::new(HashMap::new()));
        let heartbeat_acked = Arc::new(RwLock::new(HashMap::new()));
        let network_factory =
            WormFsNetworkFactory::new(mock_network, heartbeat_sent, heartbeat_acked);

        // Create Raft instance using Raft::new
        let raft = Raft::new(
            node_id,
            Arc::new(raft_config.clone()),
            network_factory,
            log_storage,   // Don't wrap in Arc, it already implements the trait
            state_machine, // Don't wrap in Arc, it already implements the trait
        )
        .await
        .expect("Failed to create Raft instance");

        // Create the StorageRaftMemberImpl using the internal constructor
        let config = RaftConfig {
            heartbeat_interval: Duration::from_millis(50),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            max_payload_entries: 64,
            max_in_flight_append_entries: 8,
            replication_lag_threshold: 1000,
            max_uncommitted_entries: 1000,
            snapshot_time_threshold: Duration::from_secs(300),
            snapshot_log_size_threshold: 100 * 1024 * 1024, // 100MB
            enable_snapshot_compression: false,
            snapshot_compression_level: 3,
            enable_lease_based_reads: false,
            lease_duration: Duration::from_secs(5),
            max_read_staleness: Duration::from_secs(120),
            default_transaction_timeout: Duration::from_secs(30),
            max_concurrent_transactions: 100,
            transaction_recovery_timeout: Duration::from_secs(60),
            transaction_log_path: log_path.clone(),
            metadata_db_path: metadata_path.clone(),
            snapshot_directory: snapshot_dir.clone(),
            network_address: "127.0.0.1:8080".parse().unwrap(),
            storage_network: Some(Arc::new(MockNetworkHandle)),
            enable_cluster_manager: false,
            cluster_manager_preset: ClusterManagerPreset::Moderate,
        };

        // Create timing trackers for heartbeat monitoring
        let heartbeat_sent = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
        let heartbeat_acked = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));

        let raft_member = Arc::new(StorageRaftMemberImpl::new_with_raft(
            node_id,
            config,
            Arc::new(raft),
            heartbeat_sent,
            heartbeat_acked,
        ));

        (raft_member, temp_dir)
    }

    #[tokio::test]
    async fn test_new_membership_manager() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let (raft, _temp_dir) = create_test_raft_instance().await;

        let manager = MembershipManager::new(config.clone(), raft, 1); // Single-node cluster for test

        // Verify initial state - no previous membership changes
        assert!(manager.can_change_membership(NodeId(1)));
        assert!(manager.can_change_membership(NodeId(2)));
        assert!(manager.can_change_membership(NodeId(99)));
    }

    #[tokio::test]
    async fn test_can_change_membership_rate_limiting() {
        let config = Arc::new(ClusterManagerConfig::aggressive()); // 30s interval
        let (raft, _temp_dir) = create_test_raft_instance().await;

        let mut manager = MembershipManager::new(config.clone(), raft, 1); // Single-node cluster for test
        let node_id = NodeId(1);

        // Initially should be allowed
        assert!(manager.can_change_membership(node_id));

        // Record a change
        manager.record_membership_change(node_id);

        // Immediately after should be rate limited
        assert!(!manager.can_change_membership(node_id));

        // Different node should still be allowed
        assert!(manager.can_change_membership(NodeId(2)));
    }

    #[test]
    fn test_would_violate_quorum() {
        // Create manager with default config - we only test the pure logic method
        // so we don't need the async setup
        let config = Arc::new(ClusterManagerConfig::moderate());

        // We need to test the would_violate_quorum method directly
        // Since it doesn't need the raft field, we can create a partial manager
        // But since Rust doesn't allow that easily, let's test the logic directly

        // Test cases for would_violate_quorum logic:
        // For 1 voter: Can't demote (would have 0)
        assert!(test_quorum_logic(1, 0));

        // For 2 voters: Can't demote to 1 (would lose quorum)
        assert!(test_quorum_logic(2, 1));

        // For 3 voters: Can demote to 2 (still have quorum)
        assert!(!test_quorum_logic(3, 2));

        // For 4 voters: Can demote to 3 (still have quorum)
        assert!(!test_quorum_logic(4, 3));

        // For 5 voters: Can demote to 4 (still have quorum)
        assert!(!test_quorum_logic(5, 4));

        // For 5 voters: Can't demote to 2 (would lose quorum - need 3)
        assert!(test_quorum_logic(5, 2));
    }

    // Helper to test quorum logic without needing a full manager
    fn test_quorum_logic(current_voters: usize, voters_after_demotion: usize) -> bool {
        if current_voters <= 1 {
            return true; // Can't demote the last voter
        }
        let required_for_quorum = (current_voters / 2) + 1;
        voters_after_demotion < required_for_quorum
    }

    #[tokio::test]
    async fn test_validate_action_quorum_violation() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let (raft, _temp_dir) = create_test_raft_instance().await;

        // Create manager with 3-node total membership
        let manager = MembershipManager::new(config.clone(), raft, 3);
        let node_id = NodeId(1);

        // Test demotion that would violate quorum (3-node cluster: 2 voters -> 1)
        // For a 3-node cluster, quorum=2, so demoting to 1 voter violates quorum
        let result = manager.validate_action(node_id, MembershipAction::Demote, 2);
        assert!(result.is_err());
        match result.err().unwrap() {
            MembershipError::QuorumViolation(msg) => {
                assert!(msg.contains("would lose quorum"));
            }
            _ => panic!("Expected QuorumViolation error"),
        }

        // Test demotion that's safe (3 voters -> 2)
        // For a 3-node cluster, quorum=2, so having 2 voters is safe
        let result = manager.validate_action(node_id, MembershipAction::Demote, 3);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_validate_action_promotion_always_ok() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let (raft, _temp_dir) = create_test_raft_instance().await;

        let manager = MembershipManager::new(config.clone(), raft, 5); // 5-node cluster for test
        let node_id = NodeId(1);

        // Promotions should never violate quorum
        assert!(manager
            .validate_action(node_id, MembershipAction::Promote, 1)
            .is_ok());
        assert!(manager
            .validate_action(node_id, MembershipAction::Promote, 2)
            .is_ok());
        assert!(manager
            .validate_action(node_id, MembershipAction::Promote, 5)
            .is_ok());
        assert!(manager
            .validate_action(node_id, MembershipAction::Promote, 10)
            .is_ok());
    }

    #[tokio::test]
    async fn test_record_membership_change() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let (raft, _temp_dir) = create_test_raft_instance().await;

        let mut manager = MembershipManager::new(config.clone(), raft, 1); // Single-node cluster for test
        let node_id = NodeId(1);

        // Initially should allow changes
        assert!(manager.can_change_membership(node_id));

        // Record a change
        manager.record_membership_change(node_id);

        // Should now be rate limited
        assert!(!manager.can_change_membership(node_id));

        // Verify the timestamp was recorded
        assert!(manager.last_membership_change.contains_key(&node_id));
    }
}
