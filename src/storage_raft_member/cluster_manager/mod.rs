/// Cluster Manager for Automatic Failure Detection and Recovery
///
/// This module provides production-grade cluster management for Raft, including:
/// - Automatic failure detection via heartbeat monitoring
/// - Automatic demotion of failed voters to learners
/// - Automatic promotion of recovered learners back to voters
/// - Quorum-safe membership management
/// - Rate limiting to prevent membership thrashing
///
/// ## Architecture
///
/// The cluster manager consists of three main components:
///
/// 1. **FailureDetector**: Monitors node health by tracking heartbeats, replication lag,
///    and consecutive failures. Determines when nodes have failed or recovered.
///
/// 2. **MembershipManager**: Executes safe membership changes (demote/promote),
///    enforces rate limits, and ensures quorum is always maintained.
///
/// 3. **ClusterManager**: Coordinates the failure detector and
///    membership manager in a background task running on the cluster leader.
///
/// ## Usage
///
/// ClusterManager is automatically integrated into StorageRaftMember and runs
/// when the node becomes the Raft leader. The example below shows how to configure it:
///
/// ```rust,no_run
/// use wormfs::storage_raft_member::{Config, ClusterManagerPreset};
///
/// let mut config = Config::default();
///
/// // Enable ClusterManager (enabled by default)
/// config.enable_cluster_manager = true;
///
/// // Select a configuration preset
/// config.cluster_manager_preset = ClusterManagerPreset::Moderate;
/// // Options: Conservative, Moderate, or Aggressive
/// ```
///
/// ## Configuration
///
/// The cluster manager behavior is controlled by `ClusterManagerConfig`, which provides
/// three preset configurations:
///
/// - `conservative()`: Slow but safe - reduces false positives
/// - `moderate()`: Balanced (default)
/// - `aggressive()`: Fast failover - higher false positive risk
///
/// All presets can be customized by modifying individual fields.
///
/// ## Safety Guarantees
///
/// The cluster manager enforces critical safety properties:
///
/// 1. **Quorum preservation**: Never demotes a voter if it would cause quorum loss
/// 2. **Rate limiting**: Prevents membership thrashing during cascading failures
/// 3. **Leader-only changes**: Only the current leader executes membership changes
/// 4. **Idempotency**: All operations can be safely retried
///
/// ## Future Work
///
/// Phase 2 (current): Foundation - types, config, skeleton components
/// Phase 3: Full implementation with Raft integration
/// Phase 4: Advanced features (predictive detection, geographic awareness)
pub mod cluster_manager;
pub mod config;
pub mod failure_detector;
pub mod heartbeat_tracker;
pub mod membership_manager;
pub mod types;

// Re-export main types for convenience
pub use cluster_manager::ClusterManager;
pub use config::{ClusterManagerConfig, ConfigError};
pub use failure_detector::FailureDetector;
pub use heartbeat_tracker::{ClusterSummary, HeartbeatTracker, NodeHeartbeat};
pub use membership_manager::{MembershipError, MembershipManager};
pub use types::{ClusterEvent, MembershipAction, NodeHealth, NodeState};
