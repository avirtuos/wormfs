// Raft configuration module
//
// This module defines configuration structures for the Raft consensus layer.
// Configuration is loaded from storage_node.yaml and used to initialize the Raft node.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;

/// Errors that can occur during configuration
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Invalid configuration: {reason}")]
    Invalid { reason: String },

    #[error("Missing required field: {field}")]
    MissingField { field: String },

    #[error("Invalid duration: {reason}")]
    InvalidDuration { reason: String },
}

/// Result type for configuration operations
pub type ConfigResult<T> = Result<T, ConfigError>;

/// Raft consensus configuration
///
/// This structure contains all the timing and policy parameters for the Raft
/// consensus algorithm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Unique identifier for this node in the Raft cluster
    pub node_id: u64,

    /// Interval between heartbeat messages from the leader
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: Duration,

    /// Minimum election timeout duration
    #[serde(with = "humantime_serde")]
    pub election_timeout_min: Duration,

    /// Maximum election timeout duration
    /// Actual timeout is randomly chosen between min and max
    #[serde(with = "humantime_serde")]
    pub election_timeout_max: Duration,

    /// Interval between automatic snapshots (in hours)
    pub snapshot_interval_hours: u64,

    /// Trigger snapshot when log size exceeds this many MB
    pub snapshot_log_size_mb: u64,

    /// Enable lease-based reads for better read performance
    pub use_lease_reads: bool,

    /// Duration of the leader lease (only used if use_lease_reads is true)
    #[serde(with = "humantime_serde")]
    pub lease_duration: Duration,

    /// Path to the Raft log storage directory
    pub log_path: String,

    /// Path to the state machine (metadata) storage directory
    pub state_path: String,

    /// Maximum number of log entries to send in a single AppendEntries RPC
    #[serde(default = "default_max_payload_entries")]
    pub max_payload_entries: u64,

    /// Enable or disable snapshot installation
    #[serde(default = "default_enable_snapshot")]
    pub enable_snapshot: bool,

    /// Maximum number of logs to keep after a snapshot
    #[serde(default = "default_max_in_snapshot_log_to_keep")]
    pub max_in_snapshot_log_to_keep: u64,

    /// Enable or disable compaction
    #[serde(default = "default_enable_compaction")]
    pub enable_compaction: bool,
}

fn default_max_payload_entries() -> u64 {
    300
}

fn default_enable_snapshot() -> bool {
    true
}

fn default_max_in_snapshot_log_to_keep() -> u64 {
    1000
}

fn default_enable_compaction() -> bool {
    true
}

impl RaftConfig {
    /// Create a new RaftConfig with default values for testing
    pub fn new_for_test(node_id: u64) -> Self {
        Self {
            node_id,
            heartbeat_interval: Duration::from_millis(250),
            election_timeout_min: Duration::from_millis(1000),
            election_timeout_max: Duration::from_millis(2000),
            snapshot_interval_hours: 24,
            snapshot_log_size_mb: 10,
            use_lease_reads: true,
            lease_duration: Duration::from_secs(5),
            log_path: format!("./data/test_raft_log_{}", node_id),
            state_path: format!("./data/test_metadata_{}", node_id),
            max_payload_entries: 300,
            enable_snapshot: true,
            max_in_snapshot_log_to_keep: 1000,
            enable_compaction: true,
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> ConfigResult<()> {
        // Validate election timeout range
        if self.election_timeout_min >= self.election_timeout_max {
            return Err(ConfigError::Invalid {
                reason: "election_timeout_min must be less than election_timeout_max".to_string(),
            });
        }

        // Validate heartbeat interval is less than election timeout
        if self.heartbeat_interval >= self.election_timeout_min {
            return Err(ConfigError::Invalid {
                reason: "heartbeat_interval must be less than election_timeout_min".to_string(),
            });
        }

        // Validate snapshot parameters
        if self.snapshot_interval_hours == 0 && self.snapshot_log_size_mb == 0 {
            return Err(ConfigError::Invalid {
                reason: "at least one snapshot trigger must be enabled".to_string(),
            });
        }

        // Validate lease duration if lease reads are enabled
        if self.use_lease_reads && self.lease_duration < self.heartbeat_interval {
            return Err(ConfigError::Invalid {
                reason: "lease_duration must be greater than heartbeat_interval".to_string(),
            });
        }

        // Validate paths are not empty
        if self.log_path.is_empty() {
            return Err(ConfigError::MissingField {
                field: "log_path".to_string(),
            });
        }

        if self.state_path.is_empty() {
            return Err(ConfigError::MissingField {
                field: "state_path".to_string(),
            });
        }

        Ok(())
    }

    /// Convert to OpenRaft Config
    pub fn to_openraft_config(&self) -> openraft::Config {
        openraft::Config {
            heartbeat_interval: self.heartbeat_interval.as_millis() as u64,
            election_timeout_min: self.election_timeout_min.as_millis() as u64,
            election_timeout_max: self.election_timeout_max.as_millis() as u64,
            max_payload_entries: self.max_payload_entries,
            snapshot_policy: if self.enable_snapshot {
                openraft::SnapshotPolicy::LogsSinceLast(self.max_in_snapshot_log_to_keep)
            } else {
                openraft::SnapshotPolicy::Never
            },
            enable_tick: true,
            enable_heartbeat: true,
            enable_elect: true,
            ..Default::default()
        }
        .validate()
        .expect("Invalid OpenRaft configuration")
    }

    /// Calculate snapshot log size threshold in bytes
    pub fn snapshot_log_size_bytes(&self) -> u64 {
        self.snapshot_log_size_mb * 1024 * 1024
    }

    /// Calculate snapshot interval as a Duration
    pub fn snapshot_interval(&self) -> Duration {
        Duration::from_secs(self.snapshot_interval_hours * 3600)
    }
}

/// Cluster configuration
///
/// Defines the initial set of nodes in the Raft cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// List of nodes in the cluster
    pub nodes: Vec<NodeConfig>,
}

/// Configuration for a single node in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Node identifier
    pub node_id: u64,

    /// Address for Raft RPC communication (for future network implementation)
    pub address: String,
}

impl ClusterConfig {
    /// Create a new cluster configuration for testing with local nodes
    pub fn new_for_test(node_count: usize) -> Self {
        let nodes = (1..=node_count)
            .map(|i| NodeConfig {
                node_id: i as u64,
                address: format!("127.0.0.1:{}", 5000 + i),
            })
            .collect();

        Self { nodes }
    }

    /// Get node IDs from the cluster configuration
    pub fn node_ids(&self) -> Vec<u64> {
        self.nodes.iter().map(|n| n.node_id).collect()
    }

    /// Validate cluster configuration
    pub fn validate(&self) -> ConfigResult<()> {
        if self.nodes.is_empty() {
            return Err(ConfigError::Invalid {
                reason: "cluster must have at least one node".to_string(),
            });
        }

        // Check for duplicate node IDs
        let mut seen_ids = std::collections::HashSet::new();
        for node in &self.nodes {
            if !seen_ids.insert(node.node_id) {
                return Err(ConfigError::Invalid {
                    reason: format!("duplicate node_id: {}", node.node_id),
                });
            }
        }

        // Check for duplicate addresses
        let mut seen_addresses = std::collections::HashSet::new();
        for node in &self.nodes {
            if !seen_addresses.insert(&node.address) {
                return Err(ConfigError::Invalid {
                    reason: format!("duplicate address: {}", node.address),
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_config_creation() {
        let config = RaftConfig::new_for_test(1);
        assert_eq!(config.node_id, 1);
        assert_eq!(config.heartbeat_interval, Duration::from_millis(250));
        assert!(config.use_lease_reads);
    }

    #[test]
    fn test_raft_config_validation() {
        let mut config = RaftConfig::new_for_test(1);
        assert!(config.validate().is_ok());

        // Test invalid election timeout range
        config.election_timeout_min = Duration::from_millis(2000);
        config.election_timeout_max = Duration::from_millis(1000);
        assert!(config.validate().is_err());

        // Fix and test heartbeat interval validation
        config.election_timeout_min = Duration::from_millis(1000);
        config.election_timeout_max = Duration::from_millis(2000);
        config.heartbeat_interval = Duration::from_millis(1500);
        assert!(config.validate().is_err());

        // Fix and test snapshot parameters
        config.heartbeat_interval = Duration::from_millis(250);
        config.snapshot_interval_hours = 0;
        config.snapshot_log_size_mb = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_cluster_config_creation() {
        let cluster = ClusterConfig::new_for_test(3);
        assert_eq!(cluster.nodes.len(), 3);
        assert_eq!(cluster.nodes[0].node_id, 1);
        assert_eq!(cluster.nodes[1].node_id, 2);
        assert_eq!(cluster.nodes[2].node_id, 3);
    }

    #[test]
    fn test_cluster_config_validation() {
        let mut cluster = ClusterConfig::new_for_test(3);
        assert!(cluster.validate().is_ok());

        // Test duplicate node IDs
        cluster.nodes[1].node_id = cluster.nodes[0].node_id;
        assert!(cluster.validate().is_err());

        // Test duplicate addresses
        cluster.nodes[1].node_id = 2;
        cluster.nodes[1].address = cluster.nodes[0].address.clone();
        assert!(cluster.validate().is_err());
    }

    #[test]
    fn test_node_ids_extraction() {
        let cluster = ClusterConfig::new_for_test(3);
        let ids = cluster.node_ids();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_snapshot_calculations() {
        let config = RaftConfig::new_for_test(1);
        assert_eq!(config.snapshot_log_size_bytes(), 10 * 1024 * 1024);
        assert_eq!(config.snapshot_interval(), Duration::from_secs(24 * 3600));
    }

    #[test]
    fn test_openraft_config_conversion() {
        let config = RaftConfig::new_for_test(1);
        let openraft_config = config.to_openraft_config();
        assert_eq!(openraft_config.heartbeat_interval, 250);
        assert_eq!(openraft_config.election_timeout_min, 1000);
        assert_eq!(openraft_config.election_timeout_max, 2000);
    }
}
