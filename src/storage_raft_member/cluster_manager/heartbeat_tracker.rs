use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::super::utils::current_time_ms;

/// Callback type for new node discovery.
/// Arguments: (node_id, storage_endpoint_url)
pub type NodeDiscoveryCallback = Arc<dyn Fn(String, Option<String>) + Send + Sync>;

/// Information about a node's heartbeat
#[derive(Debug, Clone)]
pub struct NodeHeartbeat {
    pub node_id: String,
    pub last_seen: u64, // Timestamp in milliseconds
    pub sequence: u64,
    pub admin_url: Option<String>,
    pub storage_endpoint_url: Option<String>,

    // Raft state information
    pub raft_state: Option<String>,
    pub raft_term: Option<u64>,
    pub last_log_index: Option<u64>,
    pub last_log_term: Option<u64>,
    pub current_leader: Option<u64>,
    pub is_voter: Option<bool>,
    pub startup_time: Option<u64>,

    // Storage capacity information
    pub total_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
    pub chunk_count: Option<u64>,
}

impl NodeHeartbeat {
    /// Check if this heartbeat is stale (not updated recently)
    pub fn is_stale(&self, stale_threshold_ms: u64) -> bool {
        let now = current_time_ms();
        now.saturating_sub(self.last_seen) > stale_threshold_ms
    }

    /// Check if this node recently started (within the grace period)
    pub fn is_in_startup_grace_period(&self, grace_period_ms: u64) -> bool {
        if let Some(startup_time) = self.startup_time {
            let now = current_time_ms();
            now.saturating_sub(startup_time) < grace_period_ms
        } else {
            false
        }
    }

    /// Get the log lag relative to another node (positive means this node is behind)
    pub fn log_lag(&self, other: &NodeHeartbeat) -> Option<i64> {
        match (self.last_log_index, other.last_log_index) {
            (Some(my_index), Some(other_index)) => Some(other_index as i64 - my_index as i64),
            _ => None,
        }
    }
}

/// Tracks heartbeats from all nodes in the cluster
#[derive(Clone)]
pub struct HeartbeatTracker {
    heartbeats: Arc<RwLock<HashMap<String, NodeHeartbeat>>>,
    stale_threshold_ms: u64,
    startup_grace_period_ms: u64,
    /// Optional callback fired when a new node is discovered
    on_node_discovered: Arc<RwLock<Option<NodeDiscoveryCallback>>>,
}

impl HeartbeatTracker {
    /// Create a new heartbeat tracker
    pub fn new(stale_threshold_ms: u64, startup_grace_period_ms: u64) -> Self {
        Self {
            heartbeats: Arc::new(RwLock::new(HashMap::new())),
            stale_threshold_ms,
            startup_grace_period_ms,
            on_node_discovered: Arc::new(RwLock::new(None)),
        }
    }

    /// Set callback to be invoked when a new node is discovered
    pub fn set_on_node_discovered(&self, callback: NodeDiscoveryCallback) {
        *self.on_node_discovered.write() = Some(callback);
        info!("[HeartbeatTracker] Node discovery callback registered");
    }

    /// Record a heartbeat from a node
    pub fn record_heartbeat(
        &self,
        node_id: String,
        timestamp_ms: u64,
        sequence: u64,
        admin_url: Option<String>,
        storage_endpoint_url: Option<String>,
        raft_state: Option<String>,
        raft_term: Option<u64>,
        last_log_index: Option<u64>,
        last_log_term: Option<u64>,
        current_leader: Option<u64>,
        is_voter: Option<bool>,
        startup_time: Option<u64>,
        total_bytes: Option<u64>,
        available_bytes: Option<u64>,
        chunk_count: Option<u64>,
    ) {
        let mut heartbeats = self.heartbeats.write();
        let is_new = !heartbeats.contains_key(&node_id);

        if is_new {
            info!(
                "[HeartbeatTracker] Discovered new node via heartbeat: node_id={}, storage_endpoint_url={:?}, raft_state={:?}, term={:?}, log_index={:?}, is_voter={:?}",
                node_id,
                storage_endpoint_url,
                raft_state,
                raft_term,
                last_log_index,
                is_voter
            );

            // Fire callback if registered (before creating NodeHeartbeat which moves storage_endpoint_url)
            let callback_opt = self.on_node_discovered.read();
            if let Some(callback) = &*callback_opt {
                info!(
                    "[HeartbeatTracker] Firing node discovery callback for node {}",
                    node_id
                );
                callback(node_id.clone(), storage_endpoint_url.clone());
            } else {
                warn!(
                    "[HeartbeatTracker] No node discovery callback registered! Cannot register node {}",
                    node_id
                );
            }
        } else {
            debug!(
                "[HeartbeatTracker] Updated existing heartbeat for node {} (seq: {})",
                node_id, sequence
            );
        }

        let heartbeat = NodeHeartbeat {
            node_id: node_id.clone(),
            last_seen: timestamp_ms,
            sequence,
            admin_url,
            storage_endpoint_url,
            raft_state: raft_state.clone(),
            raft_term,
            last_log_index,
            last_log_term,
            current_leader,
            is_voter,
            startup_time,
            total_bytes,
            available_bytes,
            chunk_count,
        };

        heartbeats.insert(node_id, heartbeat);
    }

    /// Get heartbeat information for a specific node
    pub fn get_heartbeat(&self, node_id: &str) -> Option<NodeHeartbeat> {
        self.heartbeats.read().get(node_id).cloned()
    }

    /// Get all tracked heartbeats
    pub fn get_all_heartbeats(&self) -> Vec<NodeHeartbeat> {
        self.heartbeats.read().values().cloned().collect()
    }

    /// Get all active (non-stale) heartbeats
    pub fn get_active_heartbeats(&self) -> Vec<NodeHeartbeat> {
        self.heartbeats
            .read()
            .values()
            .filter(|hb| !hb.is_stale(self.stale_threshold_ms))
            .cloned()
            .collect()
    }

    /// Get nodes that are in startup grace period
    pub fn get_nodes_in_grace_period(&self) -> Vec<NodeHeartbeat> {
        self.heartbeats
            .read()
            .values()
            .filter(|hb| hb.is_in_startup_grace_period(self.startup_grace_period_ms))
            .cloned()
            .collect()
    }

    /// Get active nodes with at least the specified available storage capacity
    pub fn get_nodes_with_capacity(&self, min_available_bytes: u64) -> Vec<NodeHeartbeat> {
        self.heartbeats
            .read()
            .values()
            .filter(|hb| {
                !hb.is_stale(self.stale_threshold_ms)
                    && hb
                        .available_bytes
                        .map(|bytes| bytes >= min_available_bytes)
                        .unwrap_or(false)
            })
            .cloned()
            .collect()
    }

    /// Get the highest log index seen in the cluster
    pub fn get_highest_log_index(&self) -> Option<u64> {
        self.heartbeats
            .read()
            .values()
            .filter_map(|hb| hb.last_log_index)
            .max()
    }

    /// Get the highest term seen in the cluster
    pub fn get_highest_term(&self) -> Option<u64> {
        self.heartbeats
            .read()
            .values()
            .filter_map(|hb| hb.raft_term)
            .max()
    }

    /// Find the current leader based on heartbeats
    /// Returns the node_id that most nodes agree is the leader
    pub fn get_consensus_leader(&self) -> Option<u64> {
        let heartbeats = self.heartbeats.read();
        let active_hbs: Vec<_> = heartbeats
            .values()
            .filter(|hb| !hb.is_stale(self.stale_threshold_ms))
            .collect();

        if active_hbs.is_empty() {
            return None;
        }

        // Count votes for each leader
        let mut leader_votes: HashMap<u64, usize> = HashMap::new();
        for hb in &active_hbs {
            if let Some(leader) = hb.current_leader {
                *leader_votes.entry(leader).or_insert(0) += 1;
            }
        }

        // Return the leader with the most votes
        leader_votes
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(leader, _)| leader)
    }

    /// Check if a node is significantly behind the cluster
    /// Returns true if the node is behind by more than the specified threshold
    pub fn is_node_behind(&self, node_id: &str, lag_threshold: u64) -> bool {
        let heartbeats = self.heartbeats.read();

        if let Some(node_hb) = heartbeats.get(node_id) {
            if let Some(node_index) = node_hb.last_log_index {
                // Compare against the highest log index in the cluster
                let highest_index = heartbeats
                    .values()
                    .filter_map(|hb| hb.last_log_index)
                    .max()
                    .unwrap_or(0);

                let lag = highest_index.saturating_sub(node_index);
                return lag > lag_threshold;
            }
        }

        false
    }

    /// Remove stale heartbeats
    pub fn cleanup_stale_heartbeats(&self) {
        let mut heartbeats = self.heartbeats.write();
        let stale_threshold = self.stale_threshold_ms;

        heartbeats.retain(|node_id, hb| {
            let is_stale = hb.is_stale(stale_threshold);
            if is_stale {
                debug!(node_id = %node_id, "Removing stale heartbeat");
            }
            !is_stale
        });
    }

    /// Get summary statistics about the cluster
    pub fn get_cluster_summary(&self) -> ClusterSummary {
        let heartbeats = self.heartbeats.read();
        let active_hbs: Vec<_> = heartbeats
            .values()
            .filter(|hb| !hb.is_stale(self.stale_threshold_ms))
            .collect();

        let total_nodes = heartbeats.len();
        let active_nodes = active_hbs.len();
        let voters = active_hbs
            .iter()
            .filter(|hb| hb.is_voter.unwrap_or(false))
            .count();
        let learners = active_hbs
            .iter()
            .filter(|hb| !hb.is_voter.unwrap_or(true))
            .count();

        let highest_log_index = active_hbs.iter().filter_map(|hb| hb.last_log_index).max();

        let highest_term = active_hbs.iter().filter_map(|hb| hb.raft_term).max();

        let consensus_leader = self.get_consensus_leader();

        let grace_period_nodes = active_hbs
            .iter()
            .filter(|hb| hb.is_in_startup_grace_period(self.startup_grace_period_ms))
            .count();

        ClusterSummary {
            total_nodes,
            active_nodes,
            voters,
            learners,
            highest_log_index,
            highest_term,
            consensus_leader,
            grace_period_nodes,
        }
    }
}

/// Summary statistics about the cluster
#[derive(Debug, Clone)]
pub struct ClusterSummary {
    pub total_nodes: usize,
    pub active_nodes: usize,
    pub voters: usize,
    pub learners: usize,
    pub highest_log_index: Option<u64>,
    pub highest_term: Option<u64>,
    pub consensus_leader: Option<u64>,
    pub grace_period_nodes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_retrieve_heartbeat() {
        let tracker = HeartbeatTracker::new(5000, 60000);
        let now = current_time_ms();

        tracker.record_heartbeat(
            "node1".to_string(),
            now,
            1,
            Some("http://node1:8080".to_string()),
            None, // storage_endpoint_url
            Some("Leader".to_string()),
            Some(5),
            Some(100),
            Some(4),
            Some(1),
            Some(true),
            Some(now - 30000),
            None, // total_bytes
            None, // available_bytes
            None, // chunk_count
        );

        let hb = tracker.get_heartbeat("node1").unwrap();
        assert_eq!(hb.node_id, "node1");
        assert_eq!(hb.raft_term, Some(5));
        assert_eq!(hb.last_log_index, Some(100));
        assert_eq!(hb.is_voter, Some(true));
    }

    #[test]
    fn test_stale_detection() {
        let tracker = HeartbeatTracker::new(5000, 60000);
        let now = current_time_ms();

        // Recent heartbeat
        tracker.record_heartbeat(
            "node1".to_string(),
            now,
            1,
            None,
            None, // storage_endpoint_url
            Some("Leader".to_string()),
            Some(5),
            Some(100),
            None,
            Some(1),
            Some(true),
            Some(now),
            None, // total_bytes
            None, // available_bytes
            None, // chunk_count
        );

        // Old heartbeat
        tracker.record_heartbeat(
            "node2".to_string(),
            now - 10000,
            1,
            None,
            None, // storage_endpoint_url
            Some("Follower".to_string()),
            Some(5),
            Some(95),
            None,
            Some(1),
            Some(true),
            Some(now - 10000),
            None, // total_bytes
            None, // available_bytes
            None, // chunk_count
        );

        let active = tracker.get_active_heartbeats();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].node_id, "node1");
    }

    #[test]
    fn test_grace_period() {
        let tracker = HeartbeatTracker::new(5000, 60000);
        let now = current_time_ms();

        // Node that just started
        tracker.record_heartbeat(
            "node1".to_string(),
            now,
            1,
            None,
            None, // storage_endpoint_url
            Some("Follower".to_string()),
            Some(5),
            Some(0),
            None,
            None,
            Some(false),
            Some(now - 5000), // Started 5 seconds ago
            None,             // total_bytes
            None,             // available_bytes
            None,             // chunk_count
        );

        // Node that started long ago
        tracker.record_heartbeat(
            "node2".to_string(),
            now,
            1,
            None,
            None, // storage_endpoint_url
            Some("Leader".to_string()),
            Some(5),
            Some(100),
            None,
            Some(2),
            Some(true),
            Some(now - 120000), // Started 2 minutes ago
            None,               // total_bytes
            None,               // available_bytes
            None,               // chunk_count
        );

        let grace_nodes = tracker.get_nodes_in_grace_period();
        assert_eq!(grace_nodes.len(), 1);
        assert_eq!(grace_nodes[0].node_id, "node1");
    }

    #[test]
    fn test_highest_log_index() {
        let tracker = HeartbeatTracker::new(5000, 60000);
        let now = current_time_ms();

        tracker.record_heartbeat(
            "node1".to_string(),
            now,
            1,
            None,
            None, // storage_endpoint_url
            None,
            Some(5),
            Some(100),
            None,
            None,
            None,
            None,
            None, // total_bytes
            None, // available_bytes
            None, // chunk_count
        );
        tracker.record_heartbeat(
            "node2".to_string(),
            now,
            1,
            None,
            None, // storage_endpoint_url
            None,
            Some(5),
            Some(120),
            None,
            None,
            None,
            None,
            None, // total_bytes
            None, // available_bytes
            None, // chunk_count
        );
        tracker.record_heartbeat(
            "node3".to_string(),
            now,
            1,
            None,
            None, // storage_endpoint_url
            None,
            Some(5),
            Some(95),
            None,
            None,
            None,
            None,
            None, // total_bytes
            None, // available_bytes
            None, // chunk_count
        );

        assert_eq!(tracker.get_highest_log_index(), Some(120));
        assert_eq!(tracker.get_highest_term(), Some(5));
    }

    #[test]
    fn test_consensus_leader() {
        let tracker = HeartbeatTracker::new(5000, 60000);
        let now = current_time_ms();

        // Three nodes all agree on leader 2
        tracker.record_heartbeat(
            "node1".to_string(),
            now,
            1,
            None,
            None, // storage_endpoint_url
            None,
            Some(5),
            Some(100),
            None,
            Some(2),
            None,
            None,
            None, // total_bytes
            None, // available_bytes
            None, // chunk_count
        );
        tracker.record_heartbeat(
            "node2".to_string(),
            now,
            1,
            None,
            None, // storage_endpoint_url
            None,
            Some(5),
            Some(105),
            None,
            Some(2),
            None,
            None,
            None, // total_bytes
            None, // available_bytes
            None, // chunk_count
        );
        tracker.record_heartbeat(
            "node3".to_string(),
            now,
            1,
            None,
            None, // storage_endpoint_url
            None,
            Some(5),
            Some(98),
            None,
            Some(2),
            None,
            None,
            None, // total_bytes
            None, // available_bytes
            None, // chunk_count
        );

        assert_eq!(tracker.get_consensus_leader(), Some(2));
    }

    #[test]
    fn test_cluster_summary() {
        let tracker = HeartbeatTracker::new(5000, 60000);
        let now = current_time_ms();

        tracker.record_heartbeat(
            "node1".to_string(),
            now,
            1,
            None,
            None, // storage_endpoint_url
            Some("Leader".to_string()),
            Some(5),
            Some(100),
            None,
            Some(1),
            Some(true),
            Some(now - 120000),
            None, // total_bytes
            None, // available_bytes
            None, // chunk_count
        );
        tracker.record_heartbeat(
            "node2".to_string(),
            now,
            1,
            None,
            None, // storage_endpoint_url
            Some("Follower".to_string()),
            Some(5),
            Some(99),
            None,
            Some(1),
            Some(true),
            Some(now - 120000),
            None, // total_bytes
            None, // available_bytes
            None, // chunk_count
        );
        tracker.record_heartbeat(
            "node3".to_string(),
            now,
            1,
            None,
            None, // storage_endpoint_url
            Some("Learner".to_string()),
            Some(5),
            Some(50),
            None,
            Some(1),
            Some(false),
            Some(now - 5000),
            None, // total_bytes
            None, // available_bytes
            None, // chunk_count
        );

        let summary = tracker.get_cluster_summary();
        assert_eq!(summary.total_nodes, 3);
        assert_eq!(summary.active_nodes, 3);
        assert_eq!(summary.voters, 2);
        assert_eq!(summary.learners, 1);
        assert_eq!(summary.highest_log_index, Some(100));
        assert_eq!(summary.grace_period_nodes, 1);
    }
}
