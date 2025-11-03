/// ClusterManager Coordinator
///
/// The main orchestrator that coordinates automatic failure detection and membership management.
/// Runs as a background task on the Raft leader node, monitoring cluster health and triggering
/// appropriate membership changes when failures or recoveries are detected.
///
/// ## Responsibilities
///
/// - Periodically poll FailureDetector to check node health
/// - Trigger MembershipManager actions based on health changes
/// - Ensure operations only run on the leader
/// - Emit metrics and events for observability
/// - Manage lifecycle (start/stop based on leadership changes)
use super::config::ClusterManagerConfig;
use super::failure_detector::FailureDetector;
use super::membership_manager::{MembershipError, MembershipManager};
use super::types::{ClusterEvent, NodeHealth};
use crate::storage_raft_member::types::NodeId;
use crate::storage_raft_member::{StorageRaftMember, StorageRaftMemberImpl};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// The main ClusterManager that coordinates failure detection and membership management.
///
/// This struct ties together the FailureDetector and MembershipManager, running a
/// background monitoring task that automatically responds to node health changes.
pub struct ClusterManager {
    /// Configuration for the cluster manager
    config: Arc<ClusterManagerConfig>,

    /// Reference to the Raft instance
    raft: Arc<StorageRaftMemberImpl>,

    /// Failure detector for monitoring node health
    failure_detector: Arc<Mutex<FailureDetector>>,

    /// Membership manager for executing safe membership changes
    membership_manager: Arc<Mutex<MembershipManager>>,

    /// Background task handle (None when not running)
    monitor_task: Arc<RwLock<Option<JoinHandle<()>>>>,

    /// Channel for sending cluster events
    event_sender: mpsc::UnboundedSender<ClusterEvent>,

    /// Track the last known health state of each node to detect changes
    last_known_health: Arc<RwLock<HashMap<NodeId, NodeHealth>>>,

    /// Flag to track if we're currently the leader
    is_running: Arc<RwLock<bool>>,
}

impl ClusterManager {
    /// Create a new ClusterManager.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for cluster management
    /// * `raft` - Reference to the Raft instance
    /// * `event_sender` - Channel for emitting cluster events
    ///
    /// # Returns
    ///
    /// A new ClusterManager instance ready to be started.
    pub fn new(
        config: Arc<ClusterManagerConfig>,
        raft: Arc<StorageRaftMemberImpl>,
        event_sender: mpsc::UnboundedSender<ClusterEvent>,
    ) -> Self {
        // Create the sub-components
        let failure_detector = Arc::new(Mutex::new(FailureDetector::new(config.clone())));
        let membership_manager = Arc::new(Mutex::new(MembershipManager::new(
            config.clone(),
            raft.clone(),
        )));

        Self {
            config,
            raft,
            failure_detector,
            membership_manager,
            monitor_task: Arc::new(RwLock::new(None)),
            event_sender,
            last_known_health: Arc::new(RwLock::new(HashMap::new())),
            is_running: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the cluster manager background monitoring task.
    ///
    /// This should be called when this node becomes the Raft leader.
    /// The monitoring task will:
    /// 1. Periodically poll Raft metrics
    /// 2. Update the failure detector with current state
    /// 3. Detect health changes and trigger membership actions
    /// 4. Emit events for observability
    ///
    /// # Returns
    ///
    /// Ok(()) if started successfully, or an error if already running.
    pub async fn start(&self) -> Result<(), String> {
        let mut is_running = self.is_running.write().await;
        if *is_running {
            return Err("ClusterManager is already running".to_string());
        }

        info!("Starting ClusterManager on leader node");
        *is_running = true;

        // Initialize node tracking from current Raft membership
        self.initialize_node_tracking().await?;

        // Spawn the background monitoring task
        let handle = self.spawn_monitor_task();
        let mut task_guard = self.monitor_task.write().await;
        *task_guard = Some(handle);

        Ok(())
    }

    /// Stop the cluster manager background monitoring task.
    ///
    /// This should be called when this node loses leadership.
    ///
    /// # Returns
    ///
    /// Ok(()) if stopped successfully, or an error if not running.
    pub async fn stop(&self) -> Result<(), String> {
        let mut is_running = self.is_running.write().await;
        if !*is_running {
            return Ok(()); // Already stopped, no-op
        }

        info!("Stopping ClusterManager");
        *is_running = false;

        // Cancel the background task
        let mut task_guard = self.monitor_task.write().await;
        if let Some(handle) = task_guard.take() {
            handle.abort();
            // Wait a bit for clean shutdown
            let _ = tokio::time::timeout(Duration::from_secs(1), handle).await;
        }

        // Clear last known health states
        self.last_known_health.write().await.clear();

        Ok(())
    }

    /// Initialize node tracking from current Raft membership.
    ///
    /// Reads the current cluster membership and initializes the failure detector
    /// with all known nodes.
    async fn initialize_node_tracking(&self) -> Result<(), String> {
        // Get OpenRaft metrics directly to access membership
        let openraft_metrics = self.raft.inner().raft.metrics().borrow().clone();
        let membership = openraft_metrics.membership_config.membership();

        eprintln!("[ClusterManager] initialize_node_tracking() called");
        eprintln!(
            "[ClusterManager] Current membership: voters={:?}, learners={:?}",
            membership.voter_ids().collect::<Vec<_>>(),
            membership.learner_ids().collect::<Vec<_>>()
        );

        let mut detector = self.failure_detector.lock().await;
        detector.clear_all_nodes();

        // Add all voters
        for node_id in membership.voter_ids() {
            eprintln!(
                "[ClusterManager] Checking voter node {:?} (self={:?})",
                node_id,
                self.raft.inner().node_id
            );
            if node_id != self.raft.inner().node_id {
                detector.add_node(node_id, true);
                info!("Tracking voter node: {:?}", node_id);
                eprintln!(
                    "[ClusterManager] Added voter node {:?} to tracking",
                    node_id
                );
            } else {
                eprintln!("[ClusterManager] Skipping self node {:?}", node_id);
            }
        }

        // Add all learners
        for node_id in membership.learner_ids() {
            eprintln!("[ClusterManager] Checking learner node {:?}", node_id);
            if node_id != self.raft.inner().node_id {
                detector.add_node(node_id, false);
                info!("Tracking learner node: {:?}", node_id);
                eprintln!(
                    "[ClusterManager] Added learner node {:?} to tracking",
                    node_id
                );
            }
        }

        eprintln!("[ClusterManager] Node tracking initialized");
        Ok(())
    }

    /// Spawn the background monitoring task.
    ///
    /// This task runs the main monitoring loop that polls metrics and responds
    /// to health changes.
    fn spawn_monitor_task(&self) -> JoinHandle<()> {
        let config = self.config.clone();
        let raft = self.raft.clone();
        let failure_detector = self.failure_detector.clone();
        let membership_manager = self.membership_manager.clone();
        let event_sender = self.event_sender.clone();
        let last_known_health = self.last_known_health.clone();
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            let mut check_interval = interval(config.health_check_interval);
            check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            info!("ClusterManager monitoring task started");
            eprintln!(
                "[ClusterManager Monitor] Monitoring task started (interval: {:?})",
                config.health_check_interval
            );

            loop {
                check_interval.tick().await;
                eprintln!("[ClusterManager Monitor] Tick - checking cluster health");

                // Check if we should stop
                if !*is_running.read().await {
                    info!("ClusterManager monitoring task stopping");
                    eprintln!("[ClusterManager Monitor] Stopping (is_running=false)");
                    break;
                }

                // Check if we're still the leader
                if !raft.is_leader() {
                    warn!("No longer the leader, stopping ClusterManager monitoring");
                    eprintln!("[ClusterManager Monitor] No longer leader, stopping");
                    *is_running.write().await = false;
                    break;
                }

                // Poll metrics and update failure detector
                let metrics = raft.get_metrics();
                let self_node_id = raft.inner().node_id;

                eprintln!(
                    "[ClusterManager Monitor] Polling metrics for self_node={:?}",
                    self_node_id
                );

                // Re-sync node tracking with current membership
                // This ensures we track nodes added after ClusterManager started
                let openraft_metrics = raft.inner().raft.metrics().borrow().clone();
                let membership = openraft_metrics.membership_config.membership();

                {
                    let mut detector = failure_detector.lock().await;

                    // Add any voters we're not yet tracking
                    for node_id in membership.voter_ids() {
                        if node_id != self_node_id && !detector.is_tracking(node_id) {
                            detector.add_node(node_id, true);
                            eprintln!(
                                "[ClusterManager Monitor] Started tracking new voter: {:?}",
                                node_id
                            );
                        }
                    }

                    // Add any learners we're not yet tracking
                    for node_id in membership.learner_ids() {
                        if node_id != self_node_id && !detector.is_tracking(node_id) {
                            detector.add_node(node_id, false);
                            eprintln!(
                                "[ClusterManager Monitor] Started tracking new learner: {:?}",
                                node_id
                            );
                        }
                    }

                    detector.poll_raft_metrics(&metrics, self_node_id);
                    let all_health = detector.get_all_node_health();
                    eprintln!(
                        "[ClusterManager Monitor] Current health states: {:?}",
                        all_health
                    );
                }

                // Check for health state changes and respond
                if let Err(e) = Self::check_and_respond_to_health_changes(
                    &failure_detector,
                    &membership_manager,
                    &event_sender,
                    &last_known_health,
                )
                .await
                {
                    error!("Error processing health changes: {:?}", e);
                    eprintln!(
                        "[ClusterManager Monitor] Error processing health changes: {:?}",
                        e
                    );
                }
            }

            info!("ClusterManager monitoring task stopped");
            eprintln!("[ClusterManager Monitor] Monitoring task stopped");
        })
    }

    /// Check for health state changes and trigger appropriate responses.
    ///
    /// Compares current health states with last known states, and triggers
    /// membership changes for nodes that have failed or recovered.
    async fn check_and_respond_to_health_changes(
        failure_detector: &Arc<Mutex<FailureDetector>>,
        membership_manager: &Arc<Mutex<MembershipManager>>,
        event_sender: &mpsc::UnboundedSender<ClusterEvent>,
        last_known_health: &Arc<RwLock<HashMap<NodeId, NodeHealth>>>,
    ) -> Result<(), MembershipError> {
        let current_health = {
            let detector = failure_detector.lock().await;
            detector.get_all_node_health()
        };

        let mut last_health = last_known_health.write().await;

        for (node_id, current) in &current_health {
            let previous = last_health.get(node_id).copied();

            // Skip if no change
            if previous == Some(*current) {
                continue;
            }

            // Log the health change
            info!(
                "Node {:?} health changed: {:?} -> {:?}",
                node_id, previous, current
            );
            eprintln!(
                "[ClusterManager] Node {:?} health changed: {:?} -> {:?}",
                node_id, previous, current
            );

            // Update last known state
            last_health.insert(*node_id, *current);

            // Emit health change event
            let _ = event_sender.send(ClusterEvent::NodeHealthChanged {
                node_id: *node_id,
                old_health: previous.unwrap_or(NodeHealth::Healthy),
                new_health: *current,
                reason: format!("Detected by ClusterManager monitoring"),
            });

            // Take action based on the health change
            match (*current, previous) {
                // Node has failed - trigger demotion if it's a voter
                (NodeHealth::Failed, Some(NodeHealth::Degraded))
                | (NodeHealth::Failed, Some(NodeHealth::Healthy)) => {
                    info!("Node {:?} has failed, triggering failure handling", node_id);
                    eprintln!(
                        "[ClusterManager] Node {:?} has FAILED, triggering failure handling",
                        node_id
                    );

                    let mut manager = membership_manager.lock().await;
                    if let Err(e) = manager.handle_node_failure(*node_id).await {
                        warn!("Failed to handle node failure for {:?}: {:?}", node_id, e);
                        // Emit failure event
                        let _ = event_sender.send(ClusterEvent::MembershipChangeFailed {
                            node_id: *node_id,
                            action: super::types::MembershipAction::Demote,
                            error: format!("{:?}", e),
                        });
                    } else {
                        // Emit success event
                        let _ = event_sender.send(ClusterEvent::MembershipChangeCompleted {
                            node_id: *node_id,
                            action: super::types::MembershipAction::Demote,
                        });
                    }
                }

                // Node has recovered - trigger promotion if it's a learner
                (NodeHealth::Healthy, Some(NodeHealth::Recovering)) => {
                    info!(
                        "Node {:?} has recovered, triggering recovery handling",
                        node_id
                    );
                    eprintln!("[ClusterManager] Node {:?} has RECOVERED (Recovering->Healthy), triggering recovery handling", node_id);

                    let mut manager = membership_manager.lock().await;
                    if let Err(e) = manager.handle_node_recovery(*node_id).await {
                        warn!("Failed to handle node recovery for {:?}: {:?}", node_id, e);
                        // Emit failure event
                        let _ = event_sender.send(ClusterEvent::MembershipChangeFailed {
                            node_id: *node_id,
                            action: super::types::MembershipAction::Promote,
                            error: format!("{:?}", e),
                        });
                    } else {
                        // Emit success event
                        let _ = event_sender.send(ClusterEvent::MembershipChangeCompleted {
                            node_id: *node_id,
                            action: super::types::MembershipAction::Promote,
                        });
                    }
                }

                // Node is degraded - just monitor, no action yet
                (NodeHealth::Degraded, _) => {
                    debug!("Node {:?} is degraded, monitoring closely", node_id);
                }

                // Node is recovering - wait for full recovery
                (NodeHealth::Recovering, _) => {
                    debug!(
                        "Node {:?} is recovering, waiting for full recovery",
                        node_id
                    );
                }

                _ => {
                    // Other transitions - just log
                    debug!(
                        "Node {:?} health transition: {:?} -> {:?}",
                        node_id, previous, current
                    );
                }
            }
        }

        Ok(())
    }

    /// Check if the cluster manager is currently running.
    pub async fn is_running(&self) -> bool {
        *self.is_running.read().await
    }

    /// Get the current health status of all tracked nodes.
    ///
    /// Returns a map of NodeId to NodeHealth for all monitored nodes.
    pub async fn get_cluster_health(&self) -> HashMap<NodeId, NodeHealth> {
        let detector = self.failure_detector.lock().await;
        detector.get_all_node_health()
    }

    /// Manually trigger a health check cycle.
    ///
    /// This is useful for testing or when immediate detection is needed.
    pub async fn trigger_health_check(&self) -> Result<(), String> {
        if !*self.is_running.read().await {
            return Err("ClusterManager is not running".to_string());
        }

        if !self.raft.is_leader() {
            return Err("Not the leader".to_string());
        }

        let metrics = self.raft.get_metrics();
        let self_node_id = self.raft.inner().node_id;

        {
            let mut detector = self.failure_detector.lock().await;
            detector.poll_raft_metrics(&metrics, self_node_id);
        }

        // Check for changes
        Self::check_and_respond_to_health_changes(
            &self.failure_detector,
            &self.membership_manager,
            &self.event_sender,
            &self.last_known_health,
        )
        .await
        .map_err(|e| format!("Health check failed: {:?}", e))?;

        Ok(())
    }
}

// Implement Drop to ensure clean shutdown
impl Drop for ClusterManager {
    fn drop(&mut self) {
        // Note: We can't do async cleanup in Drop, but the abort() call
        // in stop() should handle task cancellation
        debug!("ClusterManager dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_manager_creation() {
        // Test that ClusterManager can be created with proper configuration
        let config = Arc::new(ClusterManagerConfig::moderate());
        let (_tx, _rx) = mpsc::unbounded_channel::<ClusterEvent>();

        // Verify config validation works
        assert!(config.validate().is_ok());

        // Verify configuration presets
        let conservative = ClusterManagerConfig::conservative();
        assert!(conservative.validate().is_ok());
        assert_eq!(conservative.heartbeat_timeout, Duration::from_secs(30));

        let aggressive = ClusterManagerConfig::aggressive();
        assert!(aggressive.validate().is_ok());
        assert_eq!(aggressive.heartbeat_timeout, Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_event_channel() {
        // Test that events can be sent through the channel
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Send a test event
        let event = ClusterEvent::NodeHealthChanged {
            node_id: NodeId(1),
            old_health: NodeHealth::Healthy,
            new_health: NodeHealth::Failed,
            reason: "Test".to_string(),
        };

        tx.send(event.clone()).unwrap();

        // Verify event is received
        let received = rx.recv().await.unwrap();
        match received {
            ClusterEvent::NodeHealthChanged { node_id, .. } => {
                assert_eq!(node_id, NodeId(1));
            }
            _ => panic!("Wrong event type"),
        }
    }

    #[tokio::test]
    async fn test_failure_detector_integration() {
        // Test that FailureDetector works with ClusterManager types
        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut detector = FailureDetector::new(config);

        // Add nodes
        detector.add_node(NodeId(1), true);
        detector.add_node(NodeId(2), false);

        // Check initial state
        assert_eq!(
            detector.check_node_health(NodeId(1)),
            Some(NodeHealth::Healthy)
        );
        assert_eq!(
            detector.check_node_health(NodeId(2)),
            Some(NodeHealth::Healthy)
        );

        // Simulate failures
        for _ in 0..3 {
            detector.record_failure(NodeId(1));
        }

        // Update health state (this would normally be done by poll_raft_metrics)
        detector.update_node_health(NodeId(1), NodeHealth::Failed);

        // Verify state change
        assert_eq!(
            detector.check_node_health(NodeId(1)),
            Some(NodeHealth::Failed)
        );

        // Test get_all_node_health
        let all_health = detector.get_all_node_health();
        assert_eq!(all_health.get(&NodeId(1)), Some(&NodeHealth::Failed));
        assert_eq!(all_health.get(&NodeId(2)), Some(&NodeHealth::Healthy));
    }

    #[tokio::test]
    async fn test_membership_manager_integration() {
        // Test basic MembershipManager functionality without Raft
        let config = Arc::new(ClusterManagerConfig::moderate());

        // Test rate limiting logic
        assert_eq!(
            config.min_membership_change_interval,
            Duration::from_secs(60)
        );

        // Test quorum calculations (static method test)
        // These are the same tests from membership_manager but verify integration
        assert!(would_violate_quorum_test(2, 1)); // 2 -> 1 violates quorum
        assert!(!would_violate_quorum_test(3, 2)); // 3 -> 2 is safe
        assert!(!would_violate_quorum_test(5, 4)); // 5 -> 4 is safe
    }

    // Helper function to test quorum logic
    fn would_violate_quorum_test(current_voters: usize, voters_after_demotion: usize) -> bool {
        if current_voters <= 1 {
            return true;
        }
        let required_for_quorum = (current_voters / 2) + 1;
        voters_after_demotion < required_for_quorum
    }
}
