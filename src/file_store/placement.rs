//! Chunk placement engine for distributing chunks across storage nodes.
//!
//! The PlacementEngine selects optimal storage nodes for chunk placement based on:
//! - Available storage capacity
//! - Node diversity (spreading chunks across different nodes)
//! - Load balancing

use std::sync::Arc;

use super::types::{Error, NodeId};
use crate::storage_raft_member::cluster_manager::heartbeat_tracker::{
    HeartbeatTracker, NodeHeartbeat,
};
use tracing::debug;

/// Configuration for chunk placement strategy
#[derive(Debug, Clone)]
pub struct PlacementConfig {
    /// Minimum number of different nodes for chunk placement
    /// Default: total number of chunks (maximum diversity)
    pub min_node_diversity: usize,

    /// Prefer placing chunks on the local node when possible
    pub prefer_local: bool,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            min_node_diversity: usize::MAX, // Use maximum diversity by default
            prefer_local: false,
        }
    }
}

/// Placement decision for a single chunk
#[derive(Debug, Clone)]
pub struct ChunkPlacement {
    /// Index of this chunk within the stripe
    pub chunk_index: u8,

    /// Target node for this chunk
    pub target_node_id: NodeId,

    /// Whether this chunk will be stored locally
    pub is_local: bool,
}

/// Engine for selecting chunk placement across the cluster
pub struct PlacementEngine {
    heartbeat_tracker: Arc<HeartbeatTracker>,
    my_node_id: NodeId,
    config: PlacementConfig,
}

impl PlacementEngine {
    /// Create a new PlacementEngine
    ///
    /// # Arguments
    ///
    /// * `heartbeat_tracker` - Tracker for monitoring active nodes
    /// * `my_node_id` - ID of the local node
    /// * `config` - Placement configuration
    pub fn new(
        heartbeat_tracker: Arc<HeartbeatTracker>,
        my_node_id: NodeId,
        config: PlacementConfig,
    ) -> Self {
        Self {
            heartbeat_tracker,
            my_node_id,
            config,
        }
    }

    /// Select target nodes for chunk placement
    ///
    /// # Arguments
    ///
    /// * `num_chunks` - Number of chunks to place
    ///
    /// # Returns
    ///
    /// A vector of chunk placements, one for each chunk. The placement includes
    /// the target node ID and whether it's a local placement.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Not enough active nodes with capacity
    /// - No suitable nodes found
    pub fn select_placements(&self, num_chunks: usize) -> Result<Vec<ChunkPlacement>, Error> {
        // Get active nodes with capacity
        let min_chunk_size = 1024 * 1024; // Assume 1MB minimum per chunk
        let candidate_nodes = self
            .heartbeat_tracker
            .get_nodes_with_capacity(min_chunk_size);

        if candidate_nodes.is_empty() {
            // Fallback to all active nodes (they may not have reported capacity yet)
            let all_active = self.heartbeat_tracker.get_active_heartbeats();
            if all_active.is_empty() {
                return Err(Error::InsufficientStorage {
                    needed: num_chunks,
                    available: 0,
                });
            }

            debug!(
                "No nodes with reported capacity, using {} active nodes",
                all_active.len()
            );

            return self.select_from_nodes(num_chunks, &all_active);
        }

        debug!(
            "Found {} candidate nodes with capacity for {} chunks",
            candidate_nodes.len(),
            num_chunks
        );

        self.select_from_nodes(num_chunks, &candidate_nodes)
    }

    /// Select chunk placements from a list of candidate nodes
    fn select_from_nodes(
        &self,
        num_chunks: usize,
        nodes: &[NodeHeartbeat],
    ) -> Result<Vec<ChunkPlacement>, Error> {
        let mut placements = Vec::with_capacity(num_chunks);
        let mut selected_nodes: Vec<String> = Vec::new();

        // Calculate target diversity (limit to number of chunks or available nodes)
        let target_diversity = self
            .config
            .min_node_diversity
            .min(num_chunks)
            .min(nodes.len());

        for chunk_index in 0..num_chunks {
            // For the first N chunks (where N = target_diversity), ensure unique nodes
            let node = if chunk_index < target_diversity {
                // Select best node not yet used
                self.select_best_unused_node(nodes, &selected_nodes)?
            } else {
                // After achieving target diversity, select best node overall
                self.select_best_node(nodes)?
            };

            let is_local = node.node_id == self.my_node_id.0.to_string();
            selected_nodes.push(node.node_id.clone());

            placements.push(ChunkPlacement {
                chunk_index: chunk_index as u8,
                target_node_id: NodeId(node.node_id.parse().unwrap_or(0)),
                is_local,
            });
        }

        debug!(
            "Selected {} placements with {} unique nodes",
            placements.len(),
            selected_nodes
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
        );

        Ok(placements)
    }

    /// Select the best unused node from candidates
    fn select_best_unused_node<'a>(
        &self,
        nodes: &'a [NodeHeartbeat],
        used_nodes: &[String],
    ) -> Result<&'a NodeHeartbeat, Error> {
        let unused: Vec<&NodeHeartbeat> = nodes
            .iter()
            .filter(|n| !used_nodes.contains(&n.node_id))
            .collect();

        if unused.is_empty() {
            // All nodes used, fall back to best node overall
            return self.select_best_node(nodes);
        }

        // If prefer_local is set and local node is available, use it
        if self.config.prefer_local {
            let my_node_str = self.my_node_id.0.to_string();
            if let Some(local) = unused.iter().find(|n| n.node_id == my_node_str) {
                return Ok(local);
            }
        }

        // Score all unused nodes and select the best
        let mut best_node = unused[0];
        let mut best_score = self.score_node(best_node);

        for node in &unused[1..] {
            let score = self.score_node(node);
            if score > best_score {
                best_score = score;
                best_node = node;
            }
        }

        Ok(best_node)
    }

    /// Select the best node from all candidates
    fn select_best_node<'a>(&self, nodes: &'a [NodeHeartbeat]) -> Result<&'a NodeHeartbeat, Error> {
        if nodes.is_empty() {
            return Err(Error::InsufficientStorage {
                needed: 1,
                available: 0,
            });
        }

        // If prefer_local is set and local node is available, use it
        if self.config.prefer_local {
            let my_node_str = self.my_node_id.0.to_string();
            if let Some(local) = nodes.iter().find(|n| n.node_id == my_node_str) {
                return Ok(local);
            }
        }

        // Score all nodes and select the best
        let mut best_node = &nodes[0];
        let mut best_score = self.score_node(best_node);

        for node in &nodes[1..] {
            let score = self.score_node(node);
            if score > best_score {
                best_score = score;
                best_node = node;
            }
        }

        Ok(best_node)
    }

    /// Score a node for placement desirability (higher is better)
    ///
    /// Scoring factors:
    /// - Available capacity (40% weight)
    /// - Chunk count / load (30% weight)
    /// - Local node preference (30% weight)
    fn score_node(&self, node: &NodeHeartbeat) -> f64 {
        let mut score = 0.0;

        // Factor 1: Available capacity (40% weight)
        if let (Some(total), Some(available)) = (node.total_bytes, node.available_bytes) {
            if total > 0 {
                let capacity_ratio = available as f64 / total as f64;
                score += 0.4 * capacity_ratio;
            }
        } else {
            // No capacity info, assume moderate score
            score += 0.2;
        }

        // Factor 2: Chunk count / load balancing (30% weight)
        // Lower chunk count is better
        if let Some(chunk_count) = node.chunk_count {
            // Normalize to 0-1 range (assume max 10000 chunks as reference)
            let load_factor = 1.0 - (chunk_count.min(10000) as f64 / 10000.0);
            score += 0.3 * load_factor;
        } else {
            // No chunk count info, assume moderate score
            score += 0.15;
        }

        // Factor 3: Local node preference (30% weight)
        if self.config.prefer_local && node.node_id == self.my_node_id.0.to_string() {
            score += 0.3;
        }

        score
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_heartbeat(
        node_id: &str,
        total_bytes: u64,
        available_bytes: u64,
        chunk_count: u64,
    ) -> NodeHeartbeat {
        NodeHeartbeat {
            node_id: node_id.to_string(),
            last_seen: 0,
            sequence: 0,
            admin_url: None,
            raft_state: None,
            raft_term: None,
            last_log_index: None,
            last_log_term: None,
            current_leader: None,
            is_voter: None,
            startup_time: None,
            total_bytes: Some(total_bytes),
            available_bytes: Some(available_bytes),
            chunk_count: Some(chunk_count),
        }
    }

    #[test]
    fn test_select_placements_basic() {
        use crate::storage_raft_member::utils::current_time_ms;

        let tracker = Arc::new(HeartbeatTracker::new(5000, 60000));
        let my_node_id = NodeId(1);
        let config = PlacementConfig::default();

        let engine = PlacementEngine::new(tracker.clone(), my_node_id, config);

        let now = current_time_ms();

        // Record some heartbeats with capacity
        tracker.record_heartbeat(
            "1".to_string(),
            now,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(1_000_000_000), // 1GB total
            Some(800_000_000),   // 800MB available
            Some(100),           // 100 chunks
        );
        tracker.record_heartbeat(
            "2".to_string(),
            now,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(1_000_000_000), // 1GB total
            Some(900_000_000),   // 900MB available
            Some(50),            // 50 chunks
        );

        let placements = engine.select_placements(3).unwrap();
        assert_eq!(placements.len(), 3);
    }

    #[test]
    fn test_node_diversity() {
        use crate::storage_raft_member::utils::current_time_ms;

        let tracker = Arc::new(HeartbeatTracker::new(5000, 60000));
        let my_node_id = NodeId(1);
        let config = PlacementConfig {
            min_node_diversity: 2,
            prefer_local: false,
        };

        let engine = PlacementEngine::new(tracker.clone(), my_node_id, config);

        let now = current_time_ms();

        // Add 2 nodes
        tracker.record_heartbeat(
            "1".to_string(),
            now,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(1_000_000_000),
            Some(800_000_000),
            Some(100),
        );
        tracker.record_heartbeat(
            "2".to_string(),
            now,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(1_000_000_000),
            Some(900_000_000),
            Some(50),
        );

        let placements = engine.select_placements(3).unwrap();

        // First 2 chunks should be on different nodes
        assert_ne!(placements[0].target_node_id, placements[1].target_node_id);
    }

    #[test]
    fn test_score_node() {
        let tracker = Arc::new(HeartbeatTracker::new(5000, 60000));
        let my_node_id = NodeId(1);
        let config = PlacementConfig::default();

        let engine = PlacementEngine::new(tracker, my_node_id, config);

        // Node with high available capacity and low chunk count should score high
        let good_node = create_test_heartbeat("1", 1_000_000_000, 900_000_000, 50);
        let good_score = engine.score_node(&good_node);

        // Node with low available capacity and high chunk count should score low
        let poor_node = create_test_heartbeat("2", 1_000_000_000, 100_000_000, 5000);
        let poor_score = engine.score_node(&poor_node);

        assert!(good_score > poor_score);
    }
}
