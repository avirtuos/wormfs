/// Types for the Cluster Manager
///
/// This module defines the core types used by the cluster manager for tracking
/// node health, membership changes, and cluster events.
use crate::storage_raft_member::types::NodeId;
use std::time::{Duration, Instant};

/// Health status of a cluster node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeHealth {
    /// Node is healthy and keeping up with replication
    Healthy,

    /// Node is experiencing degraded performance (high lag but still responsive)
    Degraded,

    /// Node has failed (unresponsive or critically lagging)
    Failed,

    /// Node is recovering from failure (detected as back online, catching up)
    Recovering,
}

/// State tracking for a cluster node
#[derive(Debug, Clone)]
pub struct NodeState {
    /// Node ID
    pub node_id: NodeId,

    /// Current health status
    pub health: NodeHealth,

    /// Current replication lag in number of log entries
    pub replication_lag: u64,

    /// Last time we received a heartbeat/activity from this node
    pub last_heartbeat: Instant,

    /// Number of consecutive failed health checks
    pub consecutive_failures: u32,

    /// Number of consecutive successful health checks (for recovery)
    pub consecutive_successes: u32,

    /// Whether this node is currently a voter or learner
    pub is_voter: bool,

    /// Backoff expiry time for flapping prevention
    /// If set, don't perform state transitions until this time has passed
    pub backoff_until: Option<Instant>,

    /// Number of state changes in recent history (for flapping detection)
    pub recent_state_changes: u32,

    /// Last time the health state changed
    pub last_state_change: Option<Instant>,
}

impl NodeState {
    /// Create a new NodeState for a node
    pub fn new(node_id: NodeId, is_voter: bool) -> Self {
        Self {
            node_id,
            health: NodeHealth::Healthy,
            replication_lag: 0,
            last_heartbeat: Instant::now(),
            consecutive_failures: 0,
            consecutive_successes: 0,
            is_voter,
            backoff_until: None,
            recent_state_changes: 0,
            last_state_change: None,
        }
    }

    /// Record a successful heartbeat
    pub fn record_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
        self.consecutive_failures = 0;
        self.consecutive_successes = self.consecutive_successes.saturating_add(1);
    }

    /// Record a failed health check
    pub fn record_failure(&mut self) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.consecutive_successes = 0;
    }

    /// Get the time since last heartbeat
    pub fn time_since_heartbeat(&self) -> Duration {
        self.last_heartbeat.elapsed()
    }

    /// Update replication lag
    pub fn update_lag(&mut self, lag: u64) {
        self.replication_lag = lag;
    }

    /// Update health status
    pub fn update_health(&mut self, health: NodeHealth) {
        self.health = health;
    }

    /// Check if the node is in backoff period (for flapping prevention)
    pub fn is_in_backoff(&self) -> bool {
        if let Some(backoff_until) = self.backoff_until {
            Instant::now() < backoff_until
        } else {
            false
        }
    }

    /// Record a state change and apply exponential backoff if flapping detected
    pub fn record_state_change(&mut self, old_health: NodeHealth, new_health: NodeHealth) {
        // Track state change
        self.last_state_change = Some(Instant::now());
        self.recent_state_changes = self.recent_state_changes.saturating_add(1);

        // Apply exponential backoff for flapping (multiple state changes in short time)
        if self.recent_state_changes >= 3 {
            // Exponential backoff: 2^(changes - 2) seconds
            let backoff_secs = 1u64 << (self.recent_state_changes.saturating_sub(2).min(6)); // Cap at 64 seconds
            self.backoff_until = Some(Instant::now() + Duration::from_secs(backoff_secs));
        }

        // Reset state change counter if node has been stable for a while
        if let Some(last_change) = self.last_state_change {
            if last_change.elapsed() > Duration::from_secs(300) {
                // 5 minutes of stability
                self.recent_state_changes = 0;
                self.backoff_until = None;
            }
        }
    }

    /// Reset backoff state (called when stability is confirmed)
    pub fn reset_backoff(&mut self) {
        self.backoff_until = None;
        self.recent_state_changes = 0;
    }
}

/// Type of membership action to perform
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipAction {
    /// Demote a voter to learner (non-voting)
    Demote,

    /// Promote a learner to voter
    Promote,

    /// Remove a node from the cluster entirely
    Remove,

    /// Add a new node to the cluster as learner
    Add,
}

/// Cluster events for observability and audit logging
#[derive(Debug, Clone)]
pub enum ClusterEvent {
    /// Node health changed
    NodeHealthChanged {
        node_id: NodeId,
        old_health: NodeHealth,
        new_health: NodeHealth,
        reason: String,
    },

    /// Membership change initiated
    MembershipChangeInitiated {
        node_id: NodeId,
        action: MembershipAction,
        reason: String,
    },

    /// Membership change completed
    MembershipChangeCompleted {
        node_id: NodeId,
        action: MembershipAction,
    },

    /// Membership change failed
    MembershipChangeFailed {
        node_id: NodeId,
        action: MembershipAction,
        error: String,
    },

    /// Node failure detected
    FailureDetected {
        node_id: NodeId,
        consecutive_failures: u32,
        time_since_heartbeat: Duration,
    },

    /// Node recovery detected
    RecoveryDetected {
        node_id: NodeId,
        consecutive_successes: u32,
    },

    /// Rate limit triggered
    RateLimitTriggered {
        node_id: NodeId,
        action: MembershipAction,
        reason: String,
    },

    /// Quorum preservation prevented action
    QuorumPreservationBlocked {
        node_id: NodeId,
        action: MembershipAction,
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_state_new() {
        let state = NodeState::new(NodeId(1), true);
        assert_eq!(state.node_id, NodeId(1));
        assert_eq!(state.health, NodeHealth::Healthy);
        assert_eq!(state.replication_lag, 0);
        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.consecutive_successes, 0);
        assert!(state.is_voter);
    }

    #[test]
    fn test_node_state_record_heartbeat() {
        let mut state = NodeState::new(NodeId(1), true);
        state.consecutive_failures = 5;
        state.consecutive_successes = 0;

        state.record_heartbeat();

        assert_eq!(state.consecutive_failures, 0);
        assert_eq!(state.consecutive_successes, 1);
    }

    #[test]
    fn test_node_state_record_failure() {
        let mut state = NodeState::new(NodeId(1), true);
        state.consecutive_successes = 5;
        state.consecutive_failures = 0;

        state.record_failure();

        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.consecutive_successes, 0);
    }

    #[test]
    fn test_node_state_update_lag() {
        let mut state = NodeState::new(NodeId(1), true);
        state.update_lag(150);
        assert_eq!(state.replication_lag, 150);
    }

    #[test]
    fn test_node_health_equality() {
        assert_eq!(NodeHealth::Healthy, NodeHealth::Healthy);
        assert_ne!(NodeHealth::Healthy, NodeHealth::Failed);
    }

    #[test]
    fn test_membership_action_equality() {
        assert_eq!(MembershipAction::Demote, MembershipAction::Demote);
        assert_ne!(MembershipAction::Demote, MembershipAction::Promote);
    }
}
