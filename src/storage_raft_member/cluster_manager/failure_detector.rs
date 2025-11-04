/// Failure Detection for Cluster Nodes
///
/// The FailureDetector monitors node health by tracking heartbeats, replication lag,
/// and other signals. It determines when nodes should be considered failed or recovering.
use super::config::ClusterManagerConfig;
use super::types::{ClusterEvent, NodeHealth, NodeState};
use crate::storage_raft_member::types::{NodeId, RaftMetrics};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// Detects failures and recoveries in cluster nodes
///
/// Monitors Raft metrics and maintains state for each node in the cluster.
/// Uses configurable thresholds to determine node health status.
pub struct FailureDetector {
    /// Configuration for failure detection
    config: Arc<ClusterManagerConfig>,

    /// State tracking for all nodes in the cluster
    node_states: HashMap<NodeId, NodeState>,

    /// Event transmission channel for emitting cluster events
    event_tx: Option<mpsc::UnboundedSender<ClusterEvent>>,
}

impl FailureDetector {
    /// Create a new FailureDetector
    pub fn new(config: Arc<ClusterManagerConfig>) -> Self {
        Self {
            config,
            node_states: HashMap::new(),
            event_tx: None,
        }
    }

    /// Create a FailureDetector with event emission enabled
    ///
    /// Returns a tuple of (FailureDetector, event receiver channel)
    pub fn with_events(
        config: Arc<ClusterManagerConfig>,
    ) -> (Self, mpsc::UnboundedReceiver<ClusterEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let detector = Self {
            config,
            node_states: HashMap::new(),
            event_tx: Some(tx),
        };
        (detector, rx)
    }

    /// Emit a cluster event if event channel is configured
    fn emit_event(&self, event: ClusterEvent) {
        if let Some(tx) = &self.event_tx {
            if let Err(e) = tx.send(event) {
                warn!("Failed to emit cluster event: {:?}", e);
            }
        }
    }

    /// Poll Raft metrics and update node health states
    ///
    /// This method should be called periodically (e.g., every health_check_interval)
    /// by the ClusterManager. It extracts replication lag and updates health states
    /// for all tracked nodes.
    ///
    /// # Arguments
    ///
    /// * `metrics` - Current Raft metrics from the leader
    /// * `self_node_id` - The ID of the current node (to avoid monitoring ourselves)
    pub fn poll_raft_metrics(&mut self, metrics: &RaftMetrics, self_node_id: NodeId) {
        debug!(
            "Polling Raft metrics: leader={:?}, role={:?}, {} followers",
            metrics.leader_id,
            metrics.role,
            metrics.replication_lag.len()
        );
        debug!(
            "[FailureDetector] poll_raft_metrics: replication_lag={:?}",
            metrics.replication_lag
        );
        debug!(
            "[FailureDetector] Tracked nodes: {:?}",
            self.node_states.keys().collect::<Vec<_>>()
        );

        // Update lag for all followers in the metrics
        for (node_id, lag) in &metrics.replication_lag {
            // Skip ourselves
            if *node_id == self_node_id {
                continue;
            }

            // If we're tracking this node, update it
            if let Some(state) = self.node_states.get_mut(node_id) {
                let previous_lag = state.replication_lag;

                // Check if this is a learner - learners may appear in replication_lag
                // but we should ignore lag values for them since OpenRaft doesn't
                // properly track learner replication the same way
                if !state.is_voter {
                    // This is a learner - use heartbeat-based tracking only, ignore lag
                    let is_responsive = if let Some(last_ack) = metrics.heartbeat_acked.get(node_id)
                    {
                        let time_since_ack = std::time::Instant::now().duration_since(*last_ack);
                        time_since_ack < self.config.heartbeat_timeout
                    } else {
                        false
                    };

                    if is_responsive {
                        // Reset lag to 0 for responsive learners
                        state.update_lag(0);
                        state.record_heartbeat();
                        debug!(
                            "[FailureDetector] Learner node {:?} heartbeat recorded (resetting lag to 0)",
                            node_id
                        );
                    } else {
                        state.record_failure();
                        debug!(
                            "[FailureDetector] Learner node {:?} heartbeat timeout (ignoring lag={})",
                            node_id, lag
                        );
                    }
                } else {
                    // This is a voter - use normal lag-based tracking
                    state.update_lag(*lag);

                    // Check heartbeat timing to determine if node is responsive
                    // We use three signals for robust failure detection:
                    // 1. Replication lag (from OpenRaft metrics)
                    // 2. Time since last heartbeat sent
                    // 3. Time since last heartbeat ack received
                    let is_responsive = if let Some(last_ack) = metrics.heartbeat_acked.get(node_id)
                    {
                        let time_since_ack = std::time::Instant::now().duration_since(*last_ack);
                        // Consider responsive if we got an ack within heartbeat_timeout
                        time_since_ack < self.config.heartbeat_timeout
                    } else {
                        // No ack timestamp - node has never successfully responded to heartbeats
                        // This means either:
                        // 1. Node just joined (hasn't completed first heartbeat)
                        // 2. Node went offline before any successful heartbeat exchange
                        // 3. Node went offline and heartbeat tracking was cleared
                        // We should consider it unresponsive and let consecutive_failures build up
                        // If it's truly a new node, it will respond soon and reset failures
                        false
                    };

                    if is_responsive {
                        state.record_heartbeat();
                        debug!(
                            "[FailureDetector] Node {:?} heartbeat recorded (lag={}, prev_lag={})",
                            node_id, lag, previous_lag
                        );
                    } else {
                        state.record_failure();
                        debug!(
                            "[FailureDetector] Node {:?} heartbeat timeout (lag={}, last_ack too old)",
                            node_id, lag
                        );
                    }
                }

                // Update health based on all signals
                self.update_node_health_state(*node_id);
            }
        }

        // Check for nodes we're tracking but aren't in the replication lag map
        // These could be learners (not in replication map) or unresponsive voters
        let tracked_nodes: Vec<NodeId> = self.node_states.keys().copied().collect();
        for node_id in tracked_nodes {
            if node_id != self_node_id && !metrics.replication_lag.contains_key(&node_id) {
                if let Some(state) = self.node_states.get_mut(&node_id) {
                    if state.is_voter {
                        // Voter not in replication map - definitely unresponsive
                        debug!(
                            "[FailureDetector] Voter node {:?} not in replication lag map - recording failure",
                            node_id
                        );
                        state.record_failure();
                    } else {
                        // Learner - check heartbeat timing instead of replication lag
                        // OpenRaft doesn't expose learner replication metrics the same way
                        let is_responsive =
                            if let Some(last_ack) = metrics.heartbeat_acked.get(&node_id) {
                                let time_since_ack =
                                    std::time::Instant::now().duration_since(*last_ack);
                                time_since_ack < self.config.heartbeat_timeout
                            } else {
                                false
                            };

                        if is_responsive {
                            // Reset lag to 0 for responsive learners (we can't measure actual lag)
                            state.update_lag(0);
                            state.record_heartbeat();
                            debug!(
                                "[FailureDetector] Learner node {:?} responsive (heartbeat OK)",
                                node_id
                            );
                        } else {
                            state.record_failure();
                            debug!(
                                "[FailureDetector] Learner node {:?} unresponsive (no heartbeat ack)",
                                node_id
                            );
                        }
                    }

                    // Always update health state
                    self.update_node_health_state(node_id);
                }
            }
        }
    }

    /// Update the health state of a node based on all available signals
    ///
    /// This implements the health state machine with hysteresis:
    /// - Healthy → Degraded: First failure or warning lag exceeded
    /// - Degraded → Failed: Max consecutive failures or critical lag
    /// - Failed → Recovering: Node becomes responsive again
    /// - Recovering → Healthy: Sustained health (consecutive successes)
    ///
    /// Includes flapping prevention: if a node is in backoff period, skip state transitions.
    fn update_node_health_state(&mut self, node_id: NodeId) {
        let state = match self.node_states.get(&node_id) {
            Some(s) => s.clone(), // Clone to avoid borrow checker issues
            None => return,
        };

        // Skip state transitions if node is in backoff period
        if state.is_in_backoff() {
            debug!(
                "Node {:?} in backoff period, skipping state transition",
                node_id
            );
            return;
        }

        let old_health = state.health;
        let mut new_health = old_health;

        // State machine transitions
        match old_health {
            NodeHealth::Healthy => {
                // Healthy → Degraded: First failure, warning lag, or heartbeat timeout
                let heartbeat_timed_out =
                    state.time_since_heartbeat() >= self.config.heartbeat_timeout;

                if state.consecutive_failures > 0
                    || state.replication_lag >= self.config.warning_lag_threshold
                    || heartbeat_timed_out
                {
                    new_health = NodeHealth::Degraded;
                    info!(
                        "Node {:?} degraded: failures={}, lag={}, heartbeat_timeout={}",
                        node_id,
                        state.consecutive_failures,
                        state.replication_lag,
                        heartbeat_timed_out
                    );
                    info!(
                        "[FailureDetector] Node {:?} Healthy → Degraded (failures={}, lag={}, time_since_heartbeat={:?}, timeout={:?})",
                        node_id, state.consecutive_failures, state.replication_lag,
                        state.time_since_heartbeat(), self.config.heartbeat_timeout
                    );
                }
            }
            NodeHealth::Degraded => {
                // Degraded → Failed: Max failures, critical lag, or sustained heartbeat timeout
                let heartbeat_timed_out =
                    state.time_since_heartbeat() >= self.config.heartbeat_timeout;

                if state.consecutive_failures >= self.config.max_consecutive_failures
                    || state.replication_lag >= self.config.critical_lag_threshold
                    || heartbeat_timed_out
                {
                    new_health = NodeHealth::Failed;
                    warn!(
                        "Node {:?} failed: failures={}, lag={}, heartbeat_timeout={}",
                        node_id,
                        state.consecutive_failures,
                        state.replication_lag,
                        heartbeat_timed_out
                    );
                    warn!(
                        "[FailureDetector] Node {:?} Degraded → Failed (failures={}, lag={}, time_since_heartbeat={:?}, timeout={:?})",
                        node_id, state.consecutive_failures, state.replication_lag,
                        state.time_since_heartbeat(), self.config.heartbeat_timeout
                    );

                    // Emit failure event
                    self.emit_event(ClusterEvent::FailureDetected {
                        node_id,
                        consecutive_failures: state.consecutive_failures,
                        time_since_heartbeat: state.time_since_heartbeat(),
                    });
                }
                // Degraded → Healthy: Consecutive successes (hysteresis)
                else if state.consecutive_successes >= self.config.max_consecutive_failures {
                    new_health = NodeHealth::Healthy;
                    info!(
                        "Node {:?} recovered to healthy: successes={}",
                        node_id, state.consecutive_successes
                    );
                }
            }
            NodeHealth::Failed => {
                // Failed → Recovering: Node becomes responsive
                // For learners, we don't check replication lag (not tracked by OpenRaft)
                let lag_check_passed = if state.is_voter {
                    state.replication_lag < self.config.critical_lag_threshold
                } else {
                    // Learners: skip lag check, use heartbeat responsiveness only
                    true
                };

                if state.consecutive_successes > 0 && lag_check_passed {
                    new_health = NodeHealth::Recovering;
                    info!(
                        "Node {:?} recovering from failure (is_voter={})",
                        node_id, state.is_voter
                    );
                    info!(
                        "[FailureDetector] Node {:?} Failed → Recovering (is_voter={}, consecutive_successes={})",
                        node_id, state.is_voter, state.consecutive_successes
                    );

                    // Emit recovery event
                    self.emit_event(ClusterEvent::RecoveryDetected {
                        node_id,
                        consecutive_successes: state.consecutive_successes,
                    });
                }
            }
            NodeHealth::Recovering => {
                // Recovering → Healthy: Sustained health (more successes required)
                // Hysteresis: require 2x the failure threshold to promote
                if state.consecutive_successes >= (self.config.max_consecutive_failures * 2) {
                    new_health = NodeHealth::Healthy;
                    info!(
                        "Node {:?} fully recovered: successes={}",
                        node_id, state.consecutive_successes
                    );
                    info!(
                        "[FailureDetector] Node {:?} Recovering → Healthy (is_voter={}, consecutive_successes={})",
                        node_id, state.is_voter, state.consecutive_successes
                    );

                    // Reset backoff on full recovery
                    if let Some(state) = self.node_states.get_mut(&node_id) {
                        state.reset_backoff();
                    }
                }
                // Recovering → Failed: Node degrades again
                // For learners, we don't check replication lag (not tracked by OpenRaft)
                else {
                    let lag_check_failed = if state.is_voter {
                        state.replication_lag >= self.config.critical_lag_threshold
                    } else {
                        // Learners: skip lag check, only use consecutive_failures
                        false
                    };

                    if state.consecutive_failures >= self.config.max_consecutive_failures
                        || lag_check_failed
                    {
                        new_health = NodeHealth::Failed;
                        warn!(
                            "Node {:?} failed again during recovery: failures={}, lag={}, is_voter={}",
                            node_id, state.consecutive_failures, state.replication_lag, state.is_voter
                        );
                        warn!(
                            "[FailureDetector] Node {:?} Recovering → Failed (is_voter={}, failures={}, lag={})",
                            node_id, state.is_voter, state.consecutive_failures, state.replication_lag
                        );
                    }
                }
            }
        }

        // Apply state change if needed
        if new_health != old_health {
            // Collect event data before emitting (to avoid borrow checker issues)
            let event_reason = if let Some(state) = self.node_states.get(&node_id) {
                format!(
                    "failures={}, successes={}, lag={}",
                    state.consecutive_failures, state.consecutive_successes, state.replication_lag
                )
            } else {
                String::from("unknown")
            };

            // Update state
            if let Some(state) = self.node_states.get_mut(&node_id) {
                state.update_health(new_health);
                state.record_state_change(old_health, new_health);
            }

            // Emit health change event (after releasing mutable borrow)
            self.emit_event(ClusterEvent::NodeHealthChanged {
                node_id,
                old_health,
                new_health,
                reason: event_reason,
            });
        }
    }

    /// Add a node to track
    pub fn add_node(&mut self, node_id: NodeId, is_voter: bool) {
        self.node_states
            .insert(node_id, NodeState::new(node_id, is_voter));
    }

    /// Remove a node from tracking
    pub fn remove_node(&mut self, node_id: NodeId) {
        self.node_states.remove(&node_id);
    }

    /// Check the health of a specific node
    ///
    /// Returns the current health status based on heartbeat timing,
    /// consecutive failures, and replication lag.
    pub fn check_node_health(&self, node_id: NodeId) -> Option<NodeHealth> {
        self.node_states.get(&node_id).map(|state| state.health)
    }

    /// Check if a node is responsive
    ///
    /// A node is considered responsive if it has received a heartbeat within
    /// the configured timeout window.
    pub fn is_node_responsive(&self, node_id: NodeId) -> bool {
        if let Some(state) = self.node_states.get(&node_id) {
            state.time_since_heartbeat() < self.config.heartbeat_timeout
        } else {
            false
        }
    }

    /// Get the current replication lag for a node
    pub fn get_replication_lag(&self, node_id: NodeId) -> Option<u64> {
        self.node_states
            .get(&node_id)
            .map(|state| state.replication_lag)
    }

    /// Determine if a node should be demoted based on health
    ///
    /// A node should be demoted if it:
    /// - Has exceeded the max consecutive failures threshold
    /// - Has replication lag exceeding the critical threshold
    /// - Is currently a voter (can't demote learners)
    pub fn should_demote_node(&self, node_id: NodeId) -> bool {
        if let Some(state) = self.node_states.get(&node_id) {
            if !state.is_voter {
                return false; // Can't demote non-voters
            }

            let failed_too_many_times =
                state.consecutive_failures >= self.config.max_consecutive_failures;
            let lag_too_high = state.replication_lag >= self.config.critical_lag_threshold;

            failed_too_many_times || lag_too_high
        } else {
            false
        }
    }

    /// Record a heartbeat from a node
    pub fn record_heartbeat(&mut self, node_id: NodeId) {
        if let Some(state) = self.node_states.get_mut(&node_id) {
            state.record_heartbeat();
        }
    }

    /// Record a failed health check for a node
    pub fn record_failure(&mut self, node_id: NodeId) {
        if let Some(state) = self.node_states.get_mut(&node_id) {
            state.record_failure();
        }
    }

    /// Update replication lag for a node
    pub fn update_replication_lag(&mut self, node_id: NodeId, lag: u64) {
        if let Some(state) = self.node_states.get_mut(&node_id) {
            state.update_lag(lag);
        }
    }

    /// Update health status for a node
    pub fn update_node_health(&mut self, node_id: NodeId, health: NodeHealth) {
        if let Some(state) = self.node_states.get_mut(&node_id) {
            state.update_health(health);
        }
    }

    /// Get the state for a specific node
    pub fn get_node_state(&self, node_id: NodeId) -> Option<&NodeState> {
        self.node_states.get(&node_id)
    }

    /// Get all tracked node IDs
    pub fn get_all_nodes(&self) -> Vec<NodeId> {
        self.node_states.keys().copied().collect()
    }

    /// Check if a node is currently being tracked
    pub fn is_tracking(&self, node_id: NodeId) -> bool {
        self.node_states.contains_key(&node_id)
    }

    /// Clear all tracked nodes.
    ///
    /// Used when reinitializing the detector (e.g., on leadership change).
    pub fn clear_all_nodes(&mut self) {
        self.node_states.clear();
    }

    /// Get the health status of all tracked nodes.
    ///
    /// Returns a map of NodeId to NodeHealth.
    pub fn get_all_node_health(&self) -> HashMap<NodeId, NodeHealth> {
        self.node_states
            .iter()
            .map(|(id, state)| (*id, state.health))
            .collect()
    }

    /// Update a node's voter/learner status.
    ///
    /// This should be called when a node is promoted to voter or demoted to learner.
    /// The voter status affects how health checks are performed (voters use replication lag,
    /// learners use heartbeat timing only).
    pub fn update_node_voter_status(&mut self, node_id: NodeId, is_voter: bool) {
        if let Some(state) = self.node_states.get_mut(&node_id) {
            debug!(
                "[FailureDetector] Updating node {:?} voter status: {} -> {}",
                node_id, state.is_voter, is_voter
            );
            state.is_voter = is_voter;
        } else {
            warn!(
                "Attempted to update voter status for non-tracked node {:?}",
                node_id
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_new_failure_detector() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let detector = FailureDetector::new(config);
        assert_eq!(detector.get_all_nodes().len(), 0);
    }

    #[test]
    fn test_add_remove_node() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config);

        detector.add_node(NodeId(1), true);
        assert_eq!(detector.get_all_nodes().len(), 1);

        detector.remove_node(NodeId(1));
        assert_eq!(detector.get_all_nodes().len(), 0);
    }

    #[test]
    fn test_check_node_health() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config);

        detector.add_node(NodeId(1), true);
        assert_eq!(
            detector.check_node_health(NodeId(1)),
            Some(NodeHealth::Healthy)
        );
    }

    #[test]
    fn test_is_node_responsive_fresh() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config);

        detector.add_node(NodeId(1), true);
        assert!(detector.is_node_responsive(NodeId(1)));
    }

    #[test]
    fn test_record_heartbeat() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config);

        detector.add_node(NodeId(1), true);
        detector.record_failure(NodeId(1));
        detector.record_failure(NodeId(1));

        let state = detector.get_node_state(NodeId(1)).unwrap();
        assert_eq!(state.consecutive_failures, 2);

        detector.record_heartbeat(NodeId(1));
        let state = detector.get_node_state(NodeId(1)).unwrap();
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.consecutive_successes, 1);
    }

    #[test]
    fn test_should_demote_node_consecutive_failures() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config.clone());

        detector.add_node(NodeId(1), true);

        // Not enough failures yet
        detector.record_failure(NodeId(1));
        assert!(!detector.should_demote_node(NodeId(1)));

        // Hit the threshold
        for _ in 0..config.max_consecutive_failures {
            detector.record_failure(NodeId(1));
        }
        assert!(detector.should_demote_node(NodeId(1)));
    }

    #[test]
    fn test_should_demote_node_high_lag() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config.clone());

        detector.add_node(NodeId(1), true);

        // Low lag - should not demote
        detector.update_replication_lag(NodeId(1), 100);
        assert!(!detector.should_demote_node(NodeId(1)));

        // High lag - should demote
        detector.update_replication_lag(NodeId(1), config.critical_lag_threshold + 1);
        assert!(detector.should_demote_node(NodeId(1)));
    }

    #[test]
    fn test_should_not_demote_learner() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config.clone());

        // Add as learner (not voter)
        detector.add_node(NodeId(1), false);

        // Trigger failure conditions
        for _ in 0..config.max_consecutive_failures + 1 {
            detector.record_failure(NodeId(1));
        }

        // Should not demote because it's already a learner
        assert!(!detector.should_demote_node(NodeId(1)));
    }

    // ========== Phase 2 Tests: Raft Metrics Integration and State Machine ==========

    #[test]
    fn test_with_events() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let (_detector, _rx) = FailureDetector::with_events(config);
        // Successfully created detector with event channel
    }

    #[test]
    fn test_poll_raft_metrics_updates_lag() {
        use crate::storage_raft_member::types::{RaftMetrics, RaftRole};
        use std::collections::HashMap;

        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config);

        detector.add_node(NodeId(2), true);
        detector.add_node(NodeId(3), true);

        // Create mock Raft metrics with replication lag
        let mut replication_lag = HashMap::new();
        replication_lag.insert(NodeId(2), 50);
        replication_lag.insert(NodeId(3), 150);

        let metrics = RaftMetrics {
            current_term: 1,
            role: RaftRole::Leader,
            leader_id: Some(NodeId(1)),
            commit_index: 100,
            last_applied: 100,
            last_log_index: 150,
            snapshot_index: 0,
            cluster_size: 3,
            replication_lag,
            heartbeat_sent: HashMap::new(),
            heartbeat_acked: HashMap::new(),
        };

        detector.poll_raft_metrics(&metrics, NodeId(1));

        // Check that lag was updated
        assert_eq!(detector.get_replication_lag(NodeId(2)), Some(50));
        assert_eq!(detector.get_replication_lag(NodeId(3)), Some(150));
    }

    #[test]
    fn test_health_state_transitions_healthy_to_degraded() {
        use crate::storage_raft_member::types::{RaftMetrics, RaftRole};
        use std::collections::HashMap;

        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config.clone());

        detector.add_node(NodeId(2), true);

        // Initially healthy
        assert_eq!(
            detector.check_node_health(NodeId(2)),
            Some(NodeHealth::Healthy)
        );

        // Create metrics with high warning lag (but below critical)
        let mut replication_lag = HashMap::new();
        replication_lag.insert(NodeId(2), config.warning_lag_threshold);

        let metrics = RaftMetrics {
            current_term: 1,
            role: RaftRole::Leader,
            leader_id: Some(NodeId(1)),
            commit_index: 100,
            last_applied: 100,
            last_log_index: 150,
            snapshot_index: 0,
            cluster_size: 2,
            replication_lag,
            heartbeat_sent: HashMap::new(),
            heartbeat_acked: HashMap::new(),
        };

        detector.poll_raft_metrics(&metrics, NodeId(1));

        // Should transition to Degraded
        assert_eq!(
            detector.check_node_health(NodeId(2)),
            Some(NodeHealth::Degraded)
        );
    }

    #[test]
    fn test_health_state_transitions_degraded_to_failed() {
        use crate::storage_raft_member::types::{RaftMetrics, RaftRole};
        use std::collections::HashMap;

        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config.clone());

        detector.add_node(NodeId(2), true);

        // Manually set to degraded
        detector.update_node_health(NodeId(2), NodeHealth::Degraded);

        // Record enough failures to trigger transition to Failed
        for _ in 0..config.max_consecutive_failures {
            detector.record_failure(NodeId(2));
        }

        // Create empty metrics (node not responding)
        let metrics = RaftMetrics {
            current_term: 1,
            role: RaftRole::Leader,
            leader_id: Some(NodeId(1)),
            commit_index: 100,
            last_applied: 100,
            last_log_index: 150,
            snapshot_index: 0,
            cluster_size: 2,
            replication_lag: HashMap::new(), // Node not in replication map
            heartbeat_sent: HashMap::new(),
            heartbeat_acked: HashMap::new(),
        };

        detector.poll_raft_metrics(&metrics, NodeId(1));

        // Should transition to Failed
        assert_eq!(
            detector.check_node_health(NodeId(2)),
            Some(NodeHealth::Failed)
        );
    }

    #[test]
    fn test_health_state_transitions_failed_to_recovering() {
        use crate::storage_raft_member::types::{RaftMetrics, RaftRole};
        use std::collections::HashMap;

        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config.clone());

        detector.add_node(NodeId(2), true);

        // Manually set to failed
        detector.update_node_health(NodeId(2), NodeHealth::Failed);

        // Create metrics showing node is back online with low lag
        let mut replication_lag = HashMap::new();
        replication_lag.insert(NodeId(2), 50); // Low lag

        let metrics = RaftMetrics {
            current_term: 1,
            role: RaftRole::Leader,
            leader_id: Some(NodeId(1)),
            commit_index: 100,
            last_applied: 100,
            last_log_index: 150,
            snapshot_index: 0,
            cluster_size: 2,
            replication_lag,
            heartbeat_sent: HashMap::new(),
            heartbeat_acked: HashMap::new(),
        };

        detector.poll_raft_metrics(&metrics, NodeId(1));

        // Should transition to Recovering
        assert_eq!(
            detector.check_node_health(NodeId(2)),
            Some(NodeHealth::Recovering)
        );
    }

    #[test]
    fn test_health_state_transitions_recovering_to_healthy_with_hysteresis() {
        use crate::storage_raft_member::types::{RaftMetrics, RaftRole};
        use std::collections::HashMap;

        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config.clone());

        detector.add_node(NodeId(2), true);

        // Manually set to recovering
        detector.update_node_health(NodeId(2), NodeHealth::Recovering);

        // Create metrics showing node is healthy
        let mut replication_lag = HashMap::new();
        replication_lag.insert(NodeId(2), 10);

        let metrics = RaftMetrics {
            current_term: 1,
            role: RaftRole::Leader,
            leader_id: Some(NodeId(1)),
            commit_index: 100,
            last_applied: 100,
            last_log_index: 150,
            snapshot_index: 0,
            cluster_size: 2,
            replication_lag,
            heartbeat_sent: HashMap::new(),
            heartbeat_acked: HashMap::new(),
        };

        // Poll metrics multiple times (need 2x the failure threshold for recovery)
        let required_successes = config.max_consecutive_failures * 2;
        for _ in 0..required_successes {
            detector.poll_raft_metrics(&metrics, NodeId(1));
        }

        // Should transition to Healthy after sustained recovery
        assert_eq!(
            detector.check_node_health(NodeId(2)),
            Some(NodeHealth::Healthy)
        );
    }

    #[test]
    fn test_event_emission_on_failure() {
        use crate::storage_raft_member::types::{RaftMetrics, RaftRole};
        use std::collections::HashMap;

        let config = Arc::new(ClusterManagerConfig::moderate());
        let (mut detector, mut rx) = FailureDetector::with_events(config.clone());

        detector.add_node(NodeId(2), true);
        detector.update_node_health(NodeId(2), NodeHealth::Degraded);

        // Record enough failures
        for _ in 0..config.max_consecutive_failures {
            detector.record_failure(NodeId(2));
        }

        // Trigger state update
        let metrics = RaftMetrics {
            current_term: 1,
            role: RaftRole::Leader,
            leader_id: Some(NodeId(1)),
            commit_index: 100,
            last_applied: 100,
            last_log_index: 150,
            snapshot_index: 0,
            cluster_size: 2,
            replication_lag: HashMap::new(),
            heartbeat_sent: HashMap::new(),
            heartbeat_acked: HashMap::new(),
        };

        detector.poll_raft_metrics(&metrics, NodeId(1));

        // Should receive failure event
        let event = rx.try_recv().ok();
        assert!(event.is_some());
        if let Some(ClusterEvent::FailureDetected { node_id, .. }) = event {
            assert_eq!(node_id, NodeId(2));
        } else {
            panic!("Expected FailureDetected event");
        }
    }

    #[test]
    fn test_event_emission_on_recovery() {
        use crate::storage_raft_member::types::{RaftMetrics, RaftRole};
        use std::collections::HashMap;

        let config = Arc::new(ClusterManagerConfig::moderate());
        let (mut detector, mut rx) = FailureDetector::with_events(config);

        detector.add_node(NodeId(2), true);
        detector.update_node_health(NodeId(2), NodeHealth::Failed);

        // Show node is back online
        let mut replication_lag = HashMap::new();
        replication_lag.insert(NodeId(2), 50);

        let metrics = RaftMetrics {
            current_term: 1,
            role: RaftRole::Leader,
            leader_id: Some(NodeId(1)),
            commit_index: 100,
            last_applied: 100,
            last_log_index: 150,
            snapshot_index: 0,
            cluster_size: 2,
            replication_lag,
            heartbeat_sent: HashMap::new(),
            heartbeat_acked: HashMap::new(),
        };

        detector.poll_raft_metrics(&metrics, NodeId(1));

        // Should receive recovery event
        let event = rx.try_recv().ok();
        assert!(event.is_some());
        if let Some(ClusterEvent::RecoveryDetected { node_id, .. }) = event {
            assert_eq!(node_id, NodeId(2));
        } else {
            panic!("Expected RecoveryDetected event");
        }
    }

    #[test]
    fn test_flapping_prevention_backoff() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config);

        detector.add_node(NodeId(2), true);

        // Simulate rapid state changes
        for _ in 0..3 {
            if let Some(state) = detector.node_states.get_mut(&NodeId(2)) {
                state.record_state_change(NodeHealth::Healthy, NodeHealth::Degraded);
            }
        }

        // Node should now be in backoff
        let state = detector.get_node_state(NodeId(2)).unwrap();
        assert!(state.is_in_backoff());
    }

    #[test]
    fn test_poll_metrics_skips_self_node() {
        use crate::storage_raft_member::types::{RaftMetrics, RaftRole};
        use std::collections::HashMap;

        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config);

        // Track node 1 (which is the self node)
        detector.add_node(NodeId(1), true);
        detector.add_node(NodeId(2), true);

        let mut replication_lag = HashMap::new();
        replication_lag.insert(NodeId(1), 50);
        replication_lag.insert(NodeId(2), 75);

        let metrics = RaftMetrics {
            current_term: 1,
            role: RaftRole::Leader,
            leader_id: Some(NodeId(1)),
            commit_index: 100,
            last_applied: 100,
            last_log_index: 150,
            snapshot_index: 0,
            cluster_size: 2,
            replication_lag,
            heartbeat_sent: HashMap::new(),
            heartbeat_acked: HashMap::new(),
        };

        detector.poll_raft_metrics(&metrics, NodeId(1));

        // Self node should not be updated
        // Node 2 should be updated
        assert_eq!(detector.get_replication_lag(NodeId(2)), Some(75));
    }
}
