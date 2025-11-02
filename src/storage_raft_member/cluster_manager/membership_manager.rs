/// Membership Management for Cluster Nodes
///
/// The MembershipManager handles membership changes: demoting voters to learners,
/// promoting learners to voters, and ensuring quorum is always maintained.
use super::config::ClusterManagerConfig;
use super::types::MembershipAction;
use crate::storage_raft_member::types::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

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

    /// Track the last time a membership change was made (for rate limiting)
    last_membership_change: HashMap<NodeId, Instant>,
}

impl MembershipManager {
    /// Create a new MembershipManager
    pub fn new(config: Arc<ClusterManagerConfig>) -> Self {
        Self {
            config,
            last_membership_change: HashMap::new(),
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
    /// A demotion violates quorum if it would leave fewer than (n/2 + 1) voters.
    ///
    /// ## Parameters
    /// - `current_voters`: Number of voters currently in the cluster
    /// - `voters_after_demotion`: Number of voters after this demotion
    ///
    /// ## Returns
    /// `true` if the demotion would violate quorum, `false` otherwise
    pub fn would_violate_quorum(
        &self,
        current_voters: usize,
        voters_after_demotion: usize,
    ) -> bool {
        if current_voters <= 1 {
            return true; // Can't demote the last voter
        }

        // Quorum requires majority: n/2 + 1
        // For 3 voters, quorum is 2
        // For 5 voters, quorum is 3
        let required_for_quorum = (current_voters / 2) + 1;

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

    /// Placeholder for actual demotion logic (to be implemented in Phase 3)
    pub async fn demote_to_learner(&mut self, node_id: NodeId) -> Result<(), MembershipError> {
        // TODO: Implement actual Raft membership change in Phase 3
        self.record_membership_change(node_id);
        Ok(())
    }

    /// Placeholder for actual promotion logic (to be implemented in Phase 3)
    pub async fn promote_to_voter(&mut self, node_id: NodeId) -> Result<(), MembershipError> {
        // TODO: Implement actual Raft membership change in Phase 3
        self.record_membership_change(node_id);
        Ok(())
    }

    /// Placeholder for failure handling logic (to be implemented in Phase 3)
    pub async fn handle_node_failure(&mut self, node_id: NodeId) -> Result<(), MembershipError> {
        // TODO: Implement 5-step recovery process in Phase 3
        self.record_membership_change(node_id);
        Ok(())
    }

    /// Placeholder for recovery handling logic (to be implemented in Phase 3)
    pub async fn handle_node_recovery(&mut self, node_id: NodeId) -> Result<(), MembershipError> {
        // TODO: Implement learner re-addition and promotion in Phase 3
        self.record_membership_change(node_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_membership_manager() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let manager = MembershipManager::new(config);
        assert!(manager.can_change_membership(NodeId(1)));
    }

    #[test]
    fn test_can_change_membership_initially_true() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let manager = MembershipManager::new(config);
        assert!(manager.can_change_membership(NodeId(1)));
    }

    #[test]
    fn test_would_violate_quorum() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let manager = MembershipManager::new(config);

        // 1 voter -> can't demote
        assert!(manager.would_violate_quorum(1, 0));

        // 3 voters -> can demote to 2 (quorum is 2)
        assert!(!manager.would_violate_quorum(3, 2));

        // 3 voters -> can't demote to 1 (quorum is 2)
        assert!(manager.would_violate_quorum(3, 1));

        // 5 voters -> can demote to 4 or 3 (quorum is 3)
        assert!(!manager.would_violate_quorum(5, 4));
        assert!(!manager.would_violate_quorum(5, 3));

        // 5 voters -> can't demote to 2 (quorum is 3)
        assert!(manager.would_violate_quorum(5, 2));
    }

    #[test]
    fn test_validate_action_quorum_violation() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let manager = MembershipManager::new(config);

        // 3 voters, trying to demote would leave 2 - this is OK
        assert!(manager
            .validate_action(NodeId(1), MembershipAction::Demote, 3)
            .is_ok());

        // 3 voters demoting to 1 - violates quorum
        // But validate_action only checks single demotion (3 -> 2)
        // Let's test single-node cluster
        let result = manager.validate_action(NodeId(1), MembershipAction::Demote, 1);
        assert!(result.is_err());
        if let Err(MembershipError::QuorumViolation(_)) = result {
            // Expected
        } else {
            panic!("Expected QuorumViolation error");
        }
    }

    #[test]
    fn test_validate_action_promotion_always_ok() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let manager = MembershipManager::new(config);

        // Promotions never violate quorum
        assert!(manager
            .validate_action(NodeId(1), MembershipAction::Promote, 3)
            .is_ok());
    }

    #[tokio::test]
    async fn test_record_membership_change() {
        let config = Arc::new(ClusterManagerConfig::moderate());
        let mut manager = MembershipManager::new(config);

        // Initially can change
        assert!(manager.can_change_membership(NodeId(1)));

        // After a change, record it
        manager.record_membership_change(NodeId(1));

        // Immediately after, still within rate limit window
        // (In practice there'd be a delay, but our config has 60s interval)
        // So immediately after, it should NOT be allowed
        // Actually, the test will pass because time hasn't elapsed
        // Let me verify the logic is correct
        assert!(!manager.can_change_membership(NodeId(1)));
    }
}
