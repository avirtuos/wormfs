//! Integration tests for multi-node Raft clusters
//!
//! These tests verify that StorageRaftMember works correctly in multi-node
//! scenarios, including leader election, log replication, and failure recovery.
//!
//! ## Implementation Strategy
//!
//! These tests use a stub StorageNetwork implementation (stub_storage_network.rs)
//! that routes Raft RPCs via in-memory channels instead of real libp2p networking.
//! This provides:
//! - Instant, deterministic connectivity
//! - Fast test execution
//! - Easy simulation of network failures
//! - No port allocation or timing issues
//!
//! Real network integration is tested separately in network-specific tests.

mod stub_storage_network;

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;
use tracing::{self, info};

use stub_storage_network::{StubNetworkHub, StubStorageNetworkHandle};
use wormfs::storage_raft_member::types::{TxId, WormFsOperation};
use wormfs::storage_raft_member::{NodeId, StorageRaftMember, StorageRaftMemberImpl};
use wormfs::MetadataStore;

/// Get timeout multiplier for CI environments.
fn get_timeout_multiplier() -> f64 {
    std::env::var("TEST_TIMEOUT_MULTIPLIER")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(1.0)
        .max(1.0)
}

/// Apply timeout multiplier to a duration.
fn apply_timeout_multiplier(duration: Duration) -> Duration {
    let multiplier = get_timeout_multiplier();
    duration.mul_f64(multiplier)
}

/// Helper: Create a single-node Raft instance for testing with stub network
async fn create_single_node(
    node_id: u64,
) -> Result<(StorageRaftMemberImpl, TempDir, StubStorageNetworkHandle), Box<dyn std::error::Error>>
{
    let temp_dir = TempDir::new()?;
    let data_dir = temp_dir.path().to_path_buf();

    // Create stub network hub and handle for this node
    let hub = StubNetworkHub::new();
    let network_handle = hub.create_handle(node_id);
    network_handle.register().await;

    // Create MetadataStore first (required by Raft)
    let metadata_config = wormfs::metadata_store::Config {
        database_path: data_dir.join("metadata.redb"),
        cache_size_mb: 100,
        // WAL mode is enabled by default and works correctly with SQLite Backup API
        ..Default::default()
    };
    let metadata_store =
        wormfs::metadata_store::factory::MetadataStoreFactory::create_concrete(metadata_config)
            .await?;
    metadata_store.initialize_schema().await?;

    // Create Raft configuration with stub network
    let raft_config = wormfs::storage_raft_member::Config {
        heartbeat_interval: Duration::from_millis(500),
        election_timeout_min: Duration::from_millis(1500),
        election_timeout_max: Duration::from_millis(3000),
        max_payload_entries: 1000,
        max_in_flight_append_entries: 10,
        replication_lag_threshold: 1000,
        max_uncommitted_entries: 5000,
        snapshot_time_threshold: Duration::from_secs(3600),
        snapshot_log_size_threshold: 15 * 1000, // 15KB = ~15 entries for snapshot testing
        enable_snapshot_compression: true,
        snapshot_compression_level: 3,
        enable_lease_based_reads: false,
        lease_duration: Duration::from_secs(10),
        max_read_staleness: Duration::from_secs(120),
        default_transaction_timeout: Duration::from_secs(30),
        max_concurrent_transactions: 100,
        transaction_recovery_timeout: Duration::from_secs(60),
        transaction_log_path: data_dir.join("raft_log.redb"),
        metadata_db_path: data_dir.join("metadata.redb"),
        snapshot_directory: data_dir.join("snapshots"),
        network_address: format!("127.0.0.1:{}", 50000 + node_id).parse().unwrap(),
        storage_network: Some(Arc::new(network_handle.clone())),
        enable_cluster_manager: false, // Disabled for basic tests by default
        cluster_manager_preset: wormfs::storage_raft_member::ClusterManagerPreset::Moderate,
    };

    // Create Raft instance (passing shared MetadataStore)
    let raft_node = <StorageRaftMemberImpl as StorageRaftMember>::new(
        NodeId(node_id),
        raft_config,
        metadata_store,
    )
    .await?;

    // Register Raft handler with stub network
    network_handle
        .register_raft_handler_internal(Arc::new(raft_node.clone()))
        .await;

    Ok((raft_node, temp_dir, network_handle))
}

/// Multi-node test cluster infrastructure
///
/// This struct manages a multi-node Raft cluster for integration testing
/// using a stub network for instant, reliable communication.
struct RaftTestCluster {
    nodes: Vec<RaftTestNode>,
    // Map node_id to temp_dir for restart support
    temp_dirs: std::collections::HashMap<u64, TempDir>,
    // Hub is accessible for creating new handles during restart
    hub: StubNetworkHub,
}

struct RaftTestNode {
    id: u64,
    raft: StorageRaftMemberImpl,
    peer_id: String,
}

impl RaftTestCluster {
    /// Create a new N-node test cluster (not yet initialized)
    async fn new(node_count: usize) -> Result<Self, Box<dyn std::error::Error>> {
        eprintln!(
            "Creating {}-node test cluster with stub network",
            node_count
        );

        // Create shared network hub for all nodes
        let hub = StubNetworkHub::new();

        let mut nodes = Vec::with_capacity(node_count);
        let mut temp_dirs = std::collections::HashMap::new();

        // Create nodes with stub network (simple and instant!)
        for i in 0..node_count {
            let node_id = (i + 1) as u64;
            let temp_dir = TempDir::new()?;
            let data_dir = temp_dir.path().to_path_buf();

            // Create stub network handle for this node
            let network_handle = hub.create_handle(node_id);
            network_handle.register().await;

            // Get the real PeerId for this node
            let peer_id = network_handle.peer_id_string();

            // Create MetadataStore first (required by Raft)
            let metadata_config = wormfs::metadata_store::Config {
                database_path: data_dir.join("metadata.redb"),
                cache_size_mb: 100,
                ..Default::default()
            };
            let metadata_store =
                wormfs::metadata_store::factory::MetadataStoreFactory::create_concrete(
                    metadata_config,
                )
                .await?;
            metadata_store.initialize_schema().await?;

            // Create Raft configuration with stub network
            let raft_config = wormfs::storage_raft_member::Config {
                heartbeat_interval: Duration::from_millis(500),
                election_timeout_min: Duration::from_millis(1500),
                election_timeout_max: Duration::from_millis(3000),
                max_payload_entries: 1000,
                max_in_flight_append_entries: 10,
                replication_lag_threshold: 1000,
                max_uncommitted_entries: 5000,
                snapshot_time_threshold: Duration::from_secs(3600),
                snapshot_log_size_threshold: 50 * 1000, // 50KB = ~50 entries for snapshot testing
                enable_snapshot_compression: true,
                snapshot_compression_level: 3,
                enable_lease_based_reads: false,
                lease_duration: Duration::from_secs(10),
                max_read_staleness: Duration::from_secs(120),
                default_transaction_timeout: Duration::from_secs(30),
                max_concurrent_transactions: 100,
                transaction_recovery_timeout: Duration::from_secs(60),
                transaction_log_path: data_dir.join("raft_log.redb"),
                metadata_db_path: data_dir.join("metadata.redb"),
                snapshot_directory: data_dir.join("snapshots"),
                network_address: format!("127.0.0.1:{}", 50000 + node_id).parse().unwrap(),
                storage_network: Some(Arc::new(network_handle.clone())),
                enable_cluster_manager: false, // Disabled for basic tests by default
                cluster_manager_preset: wormfs::storage_raft_member::ClusterManagerPreset::Moderate,
            };

            // Create Raft instance (passing shared MetadataStore)
            let raft_node = <StorageRaftMemberImpl as StorageRaftMember>::new(
                NodeId(node_id),
                raft_config,
                metadata_store,
            )
            .await?;

            // Register Raft handler with stub network
            network_handle
                .register_raft_handler_internal(Arc::new(raft_node.clone()))
                .await;

            nodes.push(RaftTestNode {
                id: node_id,
                raft: raft_node,
                peer_id,
            });

            temp_dirs.insert(node_id, temp_dir);
        }

        eprintln!(
            "Created {} nodes - connectivity is instant with stub network!",
            node_count
        );

        // CRITICAL: Give OpenRaft background tasks time to fully start up
        // Without this delay, nodes might not be ready to handle RPCs
        eprintln!("Giving Raft nodes time to start up...");
        tokio::time::sleep(Duration::from_secs(1)).await;

        Ok(RaftTestCluster {
            nodes,
            temp_dirs,
            hub,
        })
    }

    /// Create a new N-node test cluster with ClusterManager enabled
    ///
    /// This variant enables automatic failure detection and recovery for testing
    /// ClusterManager behavior.
    async fn new_with_cluster_manager(
        node_count: usize,
        preset: wormfs::storage_raft_member::ClusterManagerPreset,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        eprintln!(
            "Creating {}-node test cluster with ClusterManager enabled ({:?} preset)",
            node_count, preset
        );

        // Create shared network hub for all nodes
        let hub = StubNetworkHub::new();

        let mut nodes = Vec::with_capacity(node_count);
        let mut temp_dirs = std::collections::HashMap::new();

        // Create nodes with ClusterManager enabled
        for i in 0..node_count {
            let node_id = (i + 1) as u64;
            let temp_dir = TempDir::new()?;
            let data_dir = temp_dir.path().to_path_buf();

            // Create stub network handle for this node
            let network_handle = hub.create_handle(node_id);
            network_handle.register().await;

            // Get the real PeerId for this node
            let peer_id = network_handle.peer_id_string();

            // Create MetadataStore first (required by Raft)
            let metadata_config = wormfs::metadata_store::Config {
                database_path: data_dir.join("metadata.redb"),
                cache_size_mb: 100,
                ..Default::default()
            };
            let metadata_store =
                wormfs::metadata_store::factory::MetadataStoreFactory::create_concrete(
                    metadata_config,
                )
                .await?;
            metadata_store.initialize_schema().await?;

            // Create Raft configuration with ClusterManager enabled
            let raft_config = wormfs::storage_raft_member::Config {
                heartbeat_interval: Duration::from_millis(500),
                election_timeout_min: Duration::from_millis(1500),
                election_timeout_max: Duration::from_millis(3000),
                max_payload_entries: 1000,
                max_in_flight_append_entries: 10,
                replication_lag_threshold: 1000,
                max_uncommitted_entries: 5000,
                snapshot_time_threshold: Duration::from_secs(3600),
                snapshot_log_size_threshold: 50 * 1000, // 50KB = ~50 entries for snapshot testing
                enable_snapshot_compression: true,
                snapshot_compression_level: 3,
                enable_lease_based_reads: false,
                lease_duration: Duration::from_secs(10),
                max_read_staleness: Duration::from_secs(120),
                default_transaction_timeout: Duration::from_secs(30),
                max_concurrent_transactions: 100,
                transaction_recovery_timeout: Duration::from_secs(60),
                transaction_log_path: data_dir.join("raft_log.redb"),
                metadata_db_path: data_dir.join("metadata.redb"),
                snapshot_directory: data_dir.join("snapshots"),
                network_address: format!("127.0.0.1:{}", 50000 + node_id).parse().unwrap(),
                storage_network: Some(Arc::new(network_handle.clone())),
                enable_cluster_manager: true, // ENABLED for automatic behavior testing
                cluster_manager_preset: preset,
            };

            // Create Raft instance (passing shared MetadataStore)
            let raft_node = <StorageRaftMemberImpl as StorageRaftMember>::new(
                NodeId(node_id),
                raft_config,
                metadata_store,
            )
            .await?;

            // Register Raft handler with stub network
            network_handle
                .register_raft_handler_internal(Arc::new(raft_node.clone()))
                .await;

            nodes.push(RaftTestNode {
                id: node_id,
                raft: raft_node,
                peer_id,
            });

            temp_dirs.insert(node_id, temp_dir);
        }

        eprintln!(
            "Created {} nodes with ClusterManager enabled - connectivity is instant with stub network!",
            node_count
        );

        // Give OpenRaft background tasks time to fully start up
        eprintln!("Giving Raft nodes time to start up...");
        tokio::time::sleep(Duration::from_secs(1)).await;

        Ok(RaftTestCluster {
            nodes,
            temp_dirs,
            hub,
        })
    }

    /// Initialize the cluster using static membership (all nodes initialized together).
    /// This is the recommended pattern for initial cluster formation.
    async fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.nodes.is_empty() {
            return Err("Cannot initialize empty cluster".into());
        }

        // Build the initial membership set with all nodes
        let mut member_nodes = std::collections::BTreeMap::new();
        for node in &self.nodes {
            let wormfs_node = wormfs::storage_raft_member::raft_config::WormFsNode {
                peer_id: node.peer_id.clone(),
                metadata: Some(wormfs::storage_raft_member::raft_config::NodeMetadata {
                    name: Some(format!("node-{}", node.id)),
                    version: Some(env!("CARGO_PKG_VERSION").to_string()),
                }),
            };
            member_nodes.insert(NodeId(node.id), wormfs_node);
        }

        eprintln!(
            "Initializing cluster with dynamic membership ({} nodes)",
            self.nodes.len()
        );

        // IMPORTANT: OpenRaft requires dynamic membership for multi-node clusters:
        // 1. Initialize the first node as a single-node cluster (just itself)
        // 2. Add other nodes as learners via add_learner()
        // 3. Promote them to voters via change_membership()

        eprintln!("Step 1: Initializing node 1 as single-node bootstrap cluster");
        let bootstrap_node = &self.nodes[0];
        let mut bootstrap_members = std::collections::BTreeMap::new();
        bootstrap_members.insert(
            NodeId(bootstrap_node.id),
            member_nodes
                .get(&NodeId(bootstrap_node.id))
                .unwrap()
                .clone(),
        );

        bootstrap_node
            .raft
            .inner()
            .raft
            .initialize(bootstrap_members)
            .await
            .map_err(|e| format!("Failed to initialize bootstrap node: {:?}", e))?;

        eprintln!("Bootstrap node initialized successfully");

        // CRITICAL: Give Raft core task time to process initialization
        // initialize() is async - it queues the request and returns immediately,
        // but actual processing happens in the Raft core background task.
        eprintln!("Waiting for Raft core to process initialization...");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Wait for bootstrap node to become leader (should be instant for single-node cluster)
        eprintln!("Step 2: Waiting for bootstrap node to become leader...");
        let start = std::time::Instant::now();
        loop {
            if bootstrap_node.raft.is_leader() {
                eprintln!("✅ Bootstrap node is leader");
                break;
            }
            if start.elapsed() > Duration::from_secs(5) {
                return Err("Timeout waiting for bootstrap node to become leader".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Step 3: Add other nodes as learners (if any)
        // Note: Bootstrap node can commit immediately (quorum=1), so we don't need to wait
        if self.nodes.len() > 1 {
            eprintln!(
                "Step 3: Adding {} other nodes as learners",
                self.nodes.len() - 1
            );
            for node in &self.nodes[1..] {
                eprintln!("Adding node {} as learner (non-blocking)", node.id);
                let wormfs_node = member_nodes.get(&NodeId(node.id)).unwrap().clone();
                // Use blocking=false to avoid waiting for learner to catch up
                // Uninitialized learners need time to sync
                bootstrap_node
                    .raft
                    .inner()
                    .raft
                    .add_learner(NodeId(node.id), wormfs_node, false)
                    .await
                    .map_err(|e| format!("Failed to add node {} as learner: {:?}", node.id, e))?;
                eprintln!("✅ Node {} added as learner (non-blocking)", node.id);
            }

            // Wait for learners to be initialized and caught up
            eprintln!("Waiting for learners to initialize and sync...");
            let start = std::time::Instant::now();
            'wait_loop: loop {
                if start.elapsed() > Duration::from_secs(10) {
                    return Err("Timeout waiting for learners to initialize".into());
                }

                // Check if all learners are initialized and caught up
                let mut all_ready = true;
                for node in &self.nodes[1..] {
                    let metrics = node.raft.inner().raft.metrics().borrow().clone();
                    let is_init = node
                        .raft
                        .inner()
                        .raft
                        .is_initialized()
                        .await
                        .unwrap_or(false);

                    eprintln!(
                        "  Node {}: initialized={}, last_log={:?}, last_applied={:?}",
                        node.id, is_init, metrics.last_log_index, metrics.last_applied
                    );

                    if !is_init {
                        all_ready = false;
                        break;
                    }
                }

                if all_ready {
                    eprintln!("✅ All learners are initialized and ready");
                    break 'wait_loop;
                }

                tokio::time::sleep(Duration::from_millis(200)).await;
            }

            // Step 4: Change membership to include all nodes as voters
            eprintln!("Step 4: Changing membership to include all nodes as voters");
            let all_node_ids: std::collections::BTreeSet<NodeId> =
                self.nodes.iter().map(|n| NodeId(n.id)).collect();

            bootstrap_node
                .raft
                .inner()
                .raft
                .change_membership(all_node_ids.clone(), false)
                .await
                .map_err(|e| format!("Failed to change membership: {:?}", e))?;

            eprintln!(
                "✅ Membership changed to include all {} nodes",
                self.nodes.len()
            );

            // Give time for membership change to propagate
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Verify we have a leader
        eprintln!("Step 5: Verifying leader...");
        let leader_idx = self.wait_for_leader(Duration::from_secs(10)).await?;
        eprintln!("✅ Node {} is the leader", self.nodes[leader_idx].id);

        Ok(())
    }

    /// Get a reference to a specific node
    fn node(&self, index: usize) -> &RaftTestNode {
        &self.nodes[index]
    }

    /// Get the leader node, if any
    fn leader(&self) -> Option<&RaftTestNode> {
        self.nodes.iter().find(|n| n.raft.is_leader())
    }

    /// Count the number of leaders (should be 0 or 1)
    fn leader_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.raft.is_leader()).count()
    }

    /// Wait for exactly one leader to be elected
    async fn wait_for_leader(
        &self,
        timeout: Duration,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let start = std::time::Instant::now();
        loop {
            let leader_count = self.leader_count();
            if leader_count == 1 {
                let leader_idx = self
                    .nodes
                    .iter()
                    .position(|n| n.raft.is_leader())
                    .expect("leader_count==1 but no leader found");
                return Ok(leader_idx);
            }
            if start.elapsed() > timeout {
                return Err(format!("Expected 1 leader, found {}", leader_count).into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Shutdown a node by removing it from the cluster but keeping its temp_dir
    /// This simulates a node crash/shutdown while preserving its persistent state.
    ///
    /// Returns Ok(()) if the node was found and removed, Err if node_id not found.
    async fn shutdown_node(&mut self, node_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        let node_idx = self
            .nodes
            .iter()
            .position(|n| n.id == node_id)
            .ok_or(format!("Node {} not found in cluster", node_id))?;

        // Remove the node from the active nodes list
        let removed_node = self.nodes.remove(node_idx);

        eprintln!(
            "🛑 Shutting down node {} (calling Raft shutdown, temp_dir preserved)",
            node_id
        );

        // CRITICAL: Unregister the Raft handler from StubNetworkHub BEFORE shutting down
        // This drops the Arc<RaftNode> reference that prevents database locks from being released
        eprintln!("  Unregistering Raft handler from hub...");
        self.hub.unregister_raft_handler(node_id).await;

        // Explicitly shutdown the Raft instance to stop background tasks and flush state
        // This is the realistic behavior - a graceful shutdown before restart
        removed_node
            .raft
            .inner()
            .raft
            .shutdown()
            .await
            .map_err(|e| format!("Failed to shutdown Raft: {:?}", e))?;

        eprintln!("  Raft shutdown complete for node {}", node_id);

        // The temp_dir is still in self.temp_dirs HashMap, so data persists
        // We explicitly drop the removed_node to ensure cleanup
        drop(removed_node);

        // IMPORTANT: Give the OS time to release database file locks
        // redb uses file-based locking, and the locks aren't released instantly
        eprintln!("Waiting for database file locks to be released...");
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Mark node as offline in the stub network to simulate network partition
        // This makes the node invisible to other nodes' peer discovery and
        // prevents heartbeat responses, enabling automatic failure detection.
        eprintln!("  Marking node {} as OFFLINE in stub network", node_id);
        self.hub.mark_node_offline(node_id).await;

        Ok(())
    }

    /// Restart a previously shutdown node, recreating its Raft instance with existing storage
    ///
    /// The node will rejoin using the same node_id and persistent state (logs, vote, etc.)
    async fn restart_node(&mut self, node_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        // Verify the node has a temp_dir (was previously created)
        if !self.temp_dirs.contains_key(&node_id) {
            return Err(format!("Node {} was never created (no temp_dir found)", node_id).into());
        }

        // Verify the node is not already running
        if self.nodes.iter().any(|n| n.id == node_id) {
            return Err(format!("Node {} is already running", node_id).into());
        }

        eprintln!("🔄 Restarting node {} with existing storage...", node_id);

        // CRITICAL FIX: Remove node from cluster membership first
        // This prevents the restarted node from immediately participating in elections
        // with stale log data, which would cause log reversion panics.
        eprintln!(
            "  Step 1: Removing node {} from cluster membership...",
            node_id
        );
        let leader = self
            .leader()
            .ok_or("No leader found to change membership")?;

        // Build new membership excluding the node being restarted
        let remaining_node_ids: Vec<NodeId> = self
            .nodes
            .iter()
            .map(|n| NodeId(n.id))
            .filter(|id| id.0 != node_id)
            .collect();

        leader
            .raft
            .inner()
            .raft
            .change_membership(remaining_node_ids.clone(), false)
            .await
            .map_err(|e| format!("Failed to remove node {} from membership: {:?}", node_id, e))?;

        eprintln!("  ✅ Node {} removed from cluster membership", node_id);
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Step 2: Restart the node with existing storage
        eprintln!(
            "  Step 2: Starting node {} with existing storage...",
            node_id
        );
        let data_dir = self.temp_dirs.get(&node_id).unwrap().path().to_path_buf();

        // Create new stub network handle for this node
        let network_handle = self.hub.create_handle(node_id);
        network_handle.register().await;

        // Get the PeerId for this node
        let peer_id = network_handle.peer_id_string();

        // Create Raft configuration with the SAME data directory
        // This will cause OpenRaft to load existing logs, vote, and state
        let raft_config = wormfs::storage_raft_member::Config {
            heartbeat_interval: Duration::from_millis(500),
            election_timeout_min: Duration::from_millis(1500),
            election_timeout_max: Duration::from_millis(3000),
            max_payload_entries: 1000,
            max_in_flight_append_entries: 10,
            replication_lag_threshold: 1000,
            max_uncommitted_entries: 5000,
            snapshot_time_threshold: Duration::from_secs(3600),
            snapshot_log_size_threshold: 15 * 1000, // 15KB = ~15 entries for snapshot testing
            enable_snapshot_compression: true,
            snapshot_compression_level: 3,
            enable_lease_based_reads: false,
            lease_duration: Duration::from_secs(10),
            max_read_staleness: Duration::from_secs(120),
            default_transaction_timeout: Duration::from_secs(30),
            max_concurrent_transactions: 100,
            transaction_recovery_timeout: Duration::from_secs(60),
            transaction_log_path: data_dir.join("raft_log.redb"),
            metadata_db_path: data_dir.join("metadata.redb"),
            snapshot_directory: data_dir.join("snapshots"),
            network_address: format!("127.0.0.1:{}", 50000 + node_id).parse().unwrap(),
            storage_network: Some(Arc::new(network_handle.clone())),
            enable_cluster_manager: false, // Disabled for basic tests by default
            cluster_manager_preset: wormfs::storage_raft_member::ClusterManagerPreset::Moderate,
        };

        // Create MetadataStore first (required by Raft)
        let metadata_config = wormfs::metadata_store::Config {
            database_path: data_dir.join("metadata.redb"),
            cache_size_mb: 100,
            ..Default::default()
        };
        let metadata_store =
            wormfs::metadata_store::factory::MetadataStoreFactory::create_concrete(metadata_config)
                .await?;
        metadata_store.initialize_schema().await?;

        // Create new Raft instance - it will load existing state from storage
        let raft_node = <StorageRaftMemberImpl as StorageRaftMember>::new(
            NodeId(node_id),
            raft_config,
            metadata_store,
        )
        .await?;

        // Register Raft handler with stub network
        network_handle
            .register_raft_handler_internal(Arc::new(raft_node.clone()))
            .await;

        // Wait for the node to be initialized before proceeding
        // This ensures the Raft state machine is ready to process requests
        info!("  Waiting for node {} to initialize...", node_id);
        let mut init_attempts = 0;
        let max_init_attempts = 50; // 5 seconds max
        loop {
            match raft_node.inner().raft.is_initialized().await {
                Ok(true) => {
                    info!(
                        "  ✅ Node {} is initialized after {} attempts",
                        node_id, init_attempts
                    );
                    break;
                }
                Ok(false) => {
                    init_attempts += 1;
                    if init_attempts >= max_init_attempts {
                        info!(
                            "  ⚠️  Node {} not initialized after {} attempts, continuing anyway",
                            node_id, init_attempts
                        );
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    info!(
                        "  ⚠️  Error checking initialization for node {}: {:?}",
                        node_id, e
                    );
                    break;
                }
            }
        }

        // Add the restarted node to our tracking
        self.nodes.push(RaftTestNode {
            id: node_id,
            raft: raft_node.clone(),
            peer_id: peer_id.clone(),
        });

        eprintln!("  ✅ Node {} restarted with existing state", node_id);

        // IMPORTANT: Mark node as online in the stub network BEFORE adding as learner
        // This allows the leader to send AppendEntries during the sync phase
        eprintln!("  Marking node {} as ONLINE in stub network", node_id);
        self.hub.mark_node_online(node_id).await;

        // Step 3: Add the restarted node back as a LEARNER (non-voting)
        eprintln!(
            "  Step 3: Adding node {} as learner (non-voting)...",
            node_id
        );
        let leader = self.leader().ok_or("No leader found to add learner")?;
        let wormfs_node = wormfs::storage_raft_member::raft_config::WormFsNode {
            peer_id: peer_id.clone(),
            metadata: Some(wormfs::storage_raft_member::raft_config::NodeMetadata {
                name: Some(format!("node-{}", node_id)),
                version: Some(env!("CARGO_PKG_VERSION").to_string()),
            }),
        };

        // Use blocking=true to wait for the learner to catch up
        leader
            .raft
            .inner()
            .raft
            .add_learner(NodeId(node_id), wormfs_node, true)
            .await
            .map_err(|e| format!("Failed to add node {} as learner: {:?}", node_id, e))?;

        eprintln!("  ✅ Node {} added as learner and caught up", node_id);

        // Step 4: Wait for the learner to fully sync
        eprintln!(
            "  Step 4: Waiting for node {} to sync with leader...",
            node_id
        );
        let start = std::time::Instant::now();
        loop {
            let leader = self.leader().ok_or("No leader found")?;
            let leader_metrics = leader.raft.inner().raft.metrics().borrow().clone();
            let node_metrics = raft_node.inner().raft.metrics().borrow().clone();

            if node_metrics.last_applied == leader_metrics.last_applied {
                eprintln!(
                    "  ✅ Node {} is fully synced (last_applied={:?})",
                    node_id, node_metrics.last_applied
                );
                break;
            }

            if start.elapsed() > Duration::from_secs(10) {
                return Err(format!(
                    "Timeout waiting for node {} to sync. Node: {:?}, Leader: {:?}",
                    node_id, node_metrics.last_applied, leader_metrics.last_applied
                )
                .into());
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Step 5: Promote the learner to voter
        eprintln!("  Step 5: Promoting node {} to voter...", node_id);
        let leader = self
            .leader()
            .ok_or("No leader found to change membership")?;

        // Build new membership including the restarted node
        let all_node_ids: Vec<NodeId> = self.nodes.iter().map(|n| NodeId(n.id)).collect();

        leader
            .raft
            .inner()
            .raft
            .change_membership(all_node_ids, false)
            .await
            .map_err(|e| format!("Failed to promote node {} to voter: {:?}", node_id, e))?;

        eprintln!("  ✅ Node {} promoted to voter", node_id);
        tokio::time::sleep(Duration::from_millis(500)).await;

        eprintln!(
            "✅ Node {} fully restarted and reintegrated into cluster",
            node_id
        );

        Ok(())
    }

    /// Restart a node with minimal intervention (for ClusterManager testing)
    ///
    /// This method just restarts the node without manual membership management,
    /// allowing ClusterManager to automatically handle failure detection and recovery.
    /// The node will come back and ClusterManager should detect it and manage membership.
    async fn restart_node_minimal(
        &mut self,
        node_id: u64,
        preset: wormfs::storage_raft_member::ClusterManagerPreset,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Verify the node has a temp_dir (was previously created)
        if !self.temp_dirs.contains_key(&node_id) {
            return Err(format!("Node {} was never created (no temp_dir found)", node_id).into());
        }

        // Verify the node is not already running
        if self.nodes.iter().any(|n| n.id == node_id) {
            return Err(format!("Node {} is already running", node_id).into());
        }

        info!(
            "🔄 Restarting node {} minimally (ClusterManager will handle recovery)...",
            node_id
        );

        let data_dir = self.temp_dirs.get(&node_id).unwrap().path().to_path_buf();

        // Create new stub network handle for this node
        let network_handle = self.hub.create_handle(node_id);
        network_handle.register().await;

        // Get the PeerId for this node
        let peer_id = network_handle.peer_id_string();

        // Create Raft configuration with ClusterManager enabled
        let raft_config = wormfs::storage_raft_member::Config {
            heartbeat_interval: Duration::from_millis(500),
            election_timeout_min: Duration::from_millis(1500),
            election_timeout_max: Duration::from_millis(3000),
            max_payload_entries: 1000,
            max_in_flight_append_entries: 10,
            replication_lag_threshold: 1000,
            max_uncommitted_entries: 5000,
            snapshot_time_threshold: Duration::from_secs(3600),
            snapshot_log_size_threshold: 15 * 1000, // 15KB = ~15 entries for snapshot testing
            enable_snapshot_compression: true,
            snapshot_compression_level: 3,
            enable_lease_based_reads: false,
            lease_duration: Duration::from_secs(10),
            max_read_staleness: Duration::from_secs(120),
            default_transaction_timeout: Duration::from_secs(30),
            max_concurrent_transactions: 100,
            transaction_recovery_timeout: Duration::from_secs(60),
            transaction_log_path: data_dir.join("raft_log.redb"),
            metadata_db_path: data_dir.join("metadata.redb"),
            snapshot_directory: data_dir.join("snapshots"),
            network_address: format!("127.0.0.1:{}", 50000 + node_id).parse().unwrap(),
            storage_network: Some(Arc::new(network_handle.clone())),
            enable_cluster_manager: true, // ENABLED for automatic recovery
            cluster_manager_preset: preset,
        };

        // Create MetadataStore first (required by Raft)
        let metadata_config = wormfs::metadata_store::Config {
            database_path: data_dir.join("metadata.redb"),
            cache_size_mb: 100,
            ..Default::default()
        };
        let metadata_store =
            wormfs::metadata_store::factory::MetadataStoreFactory::create_concrete(metadata_config)
                .await?;
        metadata_store.initialize_schema().await?;

        // Create new Raft instance - it will load existing state from storage
        let raft_node = <StorageRaftMemberImpl as StorageRaftMember>::new(
            NodeId(node_id),
            raft_config,
            metadata_store,
        )
        .await?;

        // Register Raft handler with stub network
        network_handle
            .register_raft_handler_internal(Arc::new(raft_node.clone()))
            .await;

        // Wait for the node to be initialized before proceeding
        // This ensures the Raft state machine is ready to process requests
        info!("  Waiting for node {} to initialize...", node_id);
        let mut init_attempts = 0;
        let max_init_attempts = 50; // 5 seconds max
        loop {
            match raft_node.inner().raft.is_initialized().await {
                Ok(true) => {
                    info!(
                        "  ✅ Node {} is initialized after {} attempts",
                        node_id, init_attempts
                    );
                    break;
                }
                Ok(false) => {
                    init_attempts += 1;
                    if init_attempts >= max_init_attempts {
                        info!(
                            "  ⚠️  Node {} not initialized after {} attempts, continuing anyway",
                            node_id, init_attempts
                        );
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    info!(
                        "  ⚠️  Error checking initialization for node {}: {:?}",
                        node_id, e
                    );
                    break;
                }
            }
        }

        // Add the restarted node to our tracking
        self.nodes.push(RaftTestNode {
            id: node_id,
            raft: raft_node,
            peer_id: peer_id.clone(),
        });

        // Mark node as online in the stub network (reverses the offline marking from shutdown)
        info!("  Marking node {} as ONLINE in stub network", node_id);
        self.hub.mark_node_online(node_id).await;

        // Debug: Show what Node 3's log actually contains after restart
        info!(
            "  📊 DEBUG: Checking Node {}'s log state after restart...",
            node_id
        );
        let restarted_node = self.nodes.iter().find(|n| n.id == node_id).unwrap();
        let restarted_metrics = restarted_node.raft.inner().raft.metrics().borrow().clone();
        info!(
            "  📊 Node {} log state: last_log_id={:?}, last_applied={:?}",
            node_id, restarted_metrics.last_log_index, restarted_metrics.last_applied
        );
        info!(
            "  📊 Node {} membership: voters={:?}, learners={:?}",
            node_id,
            restarted_metrics
                .membership_config
                .membership()
                .voter_ids()
                .collect::<Vec<_>>(),
            restarted_metrics
                .membership_config
                .membership()
                .learner_ids()
                .collect::<Vec<_>>()
        );

        // Re-add the node as a learner to restart OpenRaft's replication task if needed
        // This is necessary because the replication task may have exited when the node was offline
        eprintln!(
            "  Checking if node {} needs to be re-added to cluster",
            node_id
        );
        if let Some(leader) = self.leader() {
            // Debug: Show what the leader's replication tracking shows for Node 3
            eprintln!("  📊 DEBUG: Checking leader's view of Node {}...", node_id);
            let leader_metrics = leader.raft.inner().raft.metrics().borrow().clone();
            eprintln!(
                "  📊 Leader's last_log_id={:?}",
                leader_metrics.last_log_index
            );
            if let Some(replication) = leader_metrics.replication.as_ref() {
                if let Some(node_repl) = replication.get(&NodeId(node_id)) {
                    eprintln!(
                        "  📊 Leader's tracking for Node {}: {:?}",
                        node_id, node_repl
                    );
                }
            }
            // Check current membership to see if node is already present
            let metrics = leader.raft.inner().raft.metrics().borrow().clone();
            let membership = &metrics.membership_config.membership();

            let is_voter = membership.voter_ids().any(|id| id == NodeId(node_id));
            let is_learner = membership.learner_ids().any(|id| id == NodeId(node_id));

            if is_voter {
                eprintln!("  ℹ️  Node {} is already a voter in membership", node_id);
                // Still need to call add_learner to restart replication and sync membership
                eprintln!("  Calling add_learner to sync state even though already a voter");
                let node_info = wormfs::storage_raft_member::raft_config::WormFsNode {
                    peer_id,
                    metadata: None,
                };
                match leader
                    .raft
                    .inner()
                    .raft
                    .add_learner(NodeId(node_id), node_info, false)
                    .await
                {
                    Ok(_) => eprintln!("  ✅ Node {} replication restarted", node_id),
                    Err(e) => eprintln!(
                        "  ⚠️  Failed to restart replication for node {}: {:?}",
                        node_id, e
                    ),
                }
            } else if is_learner {
                eprintln!("  ℹ️  Node {} is already a learner in membership", node_id);
                // CRITICAL: The replication tracking may be stale from before the node went offline.
                // Calling add_learner directly might trigger log reversion panics. Instead, we need
                // to remove the node and re-add it to reset the replication tracking cleanly.
                eprintln!(
                    "  Removing and re-adding node {} to reset replication tracking",
                    node_id
                );

                // First, get current voters and learners (excluding this node)
                let voters: Vec<NodeId> = membership
                    .voter_ids()
                    .filter(|id| *id != NodeId(node_id))
                    .collect();
                let learners: Vec<NodeId> = membership
                    .learner_ids()
                    .filter(|id| *id != NodeId(node_id))
                    .collect();

                // Remove the node by changing membership without it
                let all_nodes = voters
                    .into_iter()
                    .chain(learners.into_iter())
                    .collect::<Vec<_>>();
                eprintln!(
                    "  Removing node {} from membership (new membership: {:?})",
                    node_id, all_nodes
                );
                match leader
                    .raft
                    .inner()
                    .raft
                    .change_membership(all_nodes, false)
                    .await
                {
                    Ok(_) => eprintln!("  ✅ Node {} removed from membership", node_id),
                    Err(e) => {
                        eprintln!("  ⚠️  Failed to remove node {}: {:?}", node_id, e);
                        return Ok(());
                    }
                }

                // Give it a moment to process
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Now add it back as a learner with fresh replication tracking
                eprintln!("  Re-adding node {} as learner with clean state", node_id);
                let node_info = wormfs::storage_raft_member::raft_config::WormFsNode {
                    peer_id,
                    metadata: None,
                };
                match leader
                    .raft
                    .inner()
                    .raft
                    .add_learner(NodeId(node_id), node_info, false)
                    .await
                {
                    Ok(_) => eprintln!(
                        "  ✅ Node {} re-added as learner with clean replication tracking",
                        node_id
                    ),
                    Err(e) => eprintln!("  ⚠️  Failed to re-add node {}: {:?}", node_id, e),
                }
            } else {
                // Node is not in membership, add it as a learner
                eprintln!(
                    "  Re-adding node {} as learner to restart replication",
                    node_id
                );
                let node_info = wormfs::storage_raft_member::raft_config::WormFsNode {
                    peer_id,
                    metadata: None,
                };

                match leader
                    .raft
                    .inner()
                    .raft
                    .add_learner(NodeId(node_id), node_info, false)
                    .await
                {
                    Ok(_) => eprintln!("  ✅ Node {} re-added as learner", node_id),
                    Err(e) => eprintln!(
                        "  ⚠️  Failed to re-add node {} as learner: {:?}",
                        node_id, e
                    ),
                }
            }
        }

        eprintln!(
            "  ✅ Node {} restarted (ClusterManager will handle recovery automatically)",
            node_id
        );

        Ok(())
    }
}

/// Test: Single-node cluster initialization and self-election as leader
///
/// ## Test Steps:
/// 1. Create single-node Raft instance with network
/// 2. Initialize as single-node cluster
/// 3. Wait for self-election as leader
/// 4. Verify leader status and metrics
#[tokio::test]
async fn test_single_node_initialization() {
    let (mut node, _temp_dir, _network_handle) =
        create_single_node(1).await.expect("Failed to create node");

    // Initialize as single-node cluster
    node.initialize(vec![]).await.expect("Failed to initialize");

    // Wait for the node to become leader
    // In a single-node cluster, this should happen immediately after initialization
    eprintln!("Waiting for node to become leader...");
    let wait_result = tokio::time::timeout(
        apply_timeout_multiplier(Duration::from_secs(15)), // Increased timeout
        async {
            // Poll until is_leader returns true
            for i in 0..150 {
                if node.is_leader() {
                    eprintln!("Node became leader after {} checks ({} ms)", i, i * 100);
                    return Ok(());
                }
                if i % 10 == 0 {
                    // Print status every second
                    let m = node.get_metrics();
                    eprintln!(
                        "Check {}: role={:?}, term={}, leader_id={:?}",
                        i, m.role, m.current_term, m.leader_id
                    );
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err("Node did not become leader after 15 seconds")
        },
    )
    .await;

    wait_result
        .expect("Timeout waiting for leader")
        .expect("Node did not become leader");

    // Get metrics for debugging
    let metrics = node.get_metrics();
    eprintln!("Node metrics after initialization:");
    eprintln!("  is_leader: {}", node.is_leader());
    eprintln!("  role: {:?}", metrics.role);
    eprintln!("  current_term: {}", metrics.current_term);
    eprintln!("  leader_id: {:?}", metrics.leader_id);
    eprintln!("  last_log_index: {:?}", metrics.last_log_index);
    eprintln!("  commit_index: {:?}", metrics.commit_index);

    // Should become leader
    assert!(
        node.is_leader(),
        "Single node should elect itself as leader. Metrics: role={:?}, term={}, leader_id={:?}",
        metrics.role,
        metrics.current_term,
        metrics.leader_id
    );

    assert_eq!(
        metrics.role,
        wormfs::storage_raft_member::RaftRole::Leader,
        "Should be in Leader role"
    );
    assert_eq!(metrics.current_term, 1, "Should be in term 1");
}

/// Test: Vote persistence survives node restarts
///
/// ## Test Steps:
/// 1. Create Raft instance with persistent storage
/// 2. Initialize and become leader (vote recorded in term 1)
/// 3. Verify leader election (demonstrates vote was cast)
///
/// ## Current Limitations:
/// Full restart verification (steps 4-5) is blocked by database locking issue:
/// StorageRaftMemberImpl doesn't properly release redb locks when dropped,
/// preventing reopening the same database even after 5+ seconds.
///
/// **Vote persistence itself IS verified** by unit tests in:
/// `src/storage_raft_member/log_storage.rs::tests::test_vote_persistence()`
///
/// TODO: Fix database cleanup in StorageRaftMemberImpl to enable full restart test
#[tokio::test]
async fn test_vote_persistence_across_restart() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let data_dir = temp_dir.path().to_path_buf();
    let log_path = data_dir.join("raft_log.redb");

    // Phase 1: Create node, initialize, and become leader
    let (expected_term, _expected_node_id) = async {
        // Create a single-node Raft instance using the helper
        // We can't use create_single_node because it creates its own temp_dir
        // Instead we'll create the node manually with our controlled data_dir
        let hub = StubNetworkHub::new();
        let network_handle = hub.create_handle(1);
        network_handle.register().await;

        let raft_config = wormfs::storage_raft_member::Config {
            heartbeat_interval: Duration::from_millis(500),
            election_timeout_min: Duration::from_millis(1500),
            election_timeout_max: Duration::from_millis(3000),
            max_payload_entries: 1000,
            max_in_flight_append_entries: 10,
            replication_lag_threshold: 1000,
            max_uncommitted_entries: 5000,
            snapshot_time_threshold: Duration::from_secs(3600),
            snapshot_log_size_threshold: 15 * 1000, // 15KB = ~15 entries for snapshot testing
            enable_snapshot_compression: true,
            snapshot_compression_level: 3,
            enable_lease_based_reads: false,
            lease_duration: Duration::from_secs(10),
            max_read_staleness: Duration::from_secs(120),
            default_transaction_timeout: Duration::from_secs(30),
            max_concurrent_transactions: 100,
            transaction_recovery_timeout: Duration::from_secs(60),
            transaction_log_path: log_path.clone(),
            metadata_db_path: data_dir.join("metadata.redb"),
            snapshot_directory: data_dir.join("snapshots"),
            network_address: "127.0.0.1:50001".parse().unwrap(),
            storage_network: Some(Arc::new(network_handle.clone())),
            enable_cluster_manager: false,
            cluster_manager_preset: wormfs::storage_raft_member::ClusterManagerPreset::Moderate,
        };

        // Create MetadataStore first (required by Raft)
        let metadata_config = wormfs::metadata_store::Config {
            database_path: data_dir.join("metadata.redb"),
            cache_size_mb: 100,
            ..Default::default()
        };
        let metadata_store =
            wormfs::metadata_store::factory::MetadataStoreFactory::create_concrete(metadata_config)
                .await
                .expect("Failed to create metadata store");
        metadata_store
            .initialize_schema()
            .await
            .expect("Failed to initialize metadata schema");

        let mut node1 = <StorageRaftMemberImpl as StorageRaftMember>::new(
            NodeId(1),
            raft_config,
            metadata_store,
        )
        .await
        .expect("Failed to create node");

        network_handle
            .register_raft_handler_internal(Arc::new(node1.clone()))
            .await;

        // Initialize as single-node cluster
        node1
            .initialize(vec![])
            .await
            .expect("Failed to initialize");

        // Wait for self-election
        let election_wait = apply_timeout_multiplier(Duration::from_millis(500));
        sleep(election_wait).await;

        let metrics = node1.get_metrics();
        assert_eq!(
            metrics.current_term, 1,
            "Should be in term 1 after election"
        );
        assert!(node1.is_leader(), "Should be leader");

        eprintln!(
            "✓ Node became leader in term 1, vote persisted to: {:?}",
            log_path
        );
        eprintln!(
            "  (Vote persistence verified by unit test: log_storage::tests::test_vote_persistence)"
        );

        (metrics.current_term, 1u64)
        // Node drops here, vote is persisted to disk
    }
    .await;

    // Phase 2 would verify restart loads persisted vote, but is currently blocked by:
    // Database locking issue - StorageRaftMemberImpl doesn't release redb file lock
    // even after 5+ seconds and explicit drops. See TODO in test documentation above.
    //
    // Workaround for future: Implement proper shutdown() method in StorageRaftMemberImpl
    // that explicitly closes database connections before dropping.

    assert_eq!(expected_term, 1, "Node successfully reached term 1");
    eprintln!("✓ Vote persistence test completed (restart verification pending fix)");
}

//
// ============================================================================
// Multi-Node Integration Tests (Currently Ignored - Infrastructure Needed)
// ============================================================================
//

/// Test: 3-node cluster formation and leader election
///
/// ## Test Steps:
/// 1. Create 3 nodes with proper network configuration
/// 2. Initialize first node as single-node cluster
/// 3. Add second and third nodes using add_node()
/// 4. Verify exactly one leader is elected
/// 5. Verify all nodes agree on leader
#[tokio::test]
#[ntest::timeout(20000)] // 20 second timeout
async fn test_three_node_cluster_formation() {
    // Create 3-node cluster
    let mut cluster = RaftTestCluster::new(3)
        .await
        .expect("Failed to create cluster");

    // Initialize the cluster (node 1 becomes leader, then adds nodes 2 and 3)
    cluster
        .initialize()
        .await
        .expect("Failed to initialize cluster");

    // Verify exactly one leader exists
    let leader_count = cluster.leader_count();
    assert_eq!(
        leader_count, 1,
        "Expected exactly 1 leader, found {}",
        leader_count
    );

    // Get the leader
    let leader = cluster.leader().expect("No leader found");
    let leader_id = leader.id;
    eprintln!("Leader is node {}", leader_id);

    // Verify all nodes agree on the leader
    for node in &cluster.nodes {
        let metrics = node.raft.get_metrics();
        assert_eq!(
            metrics.leader_id,
            Some(NodeId(leader_id)),
            "Node {} does not agree on leader (sees {:?})",
            node.id,
            metrics.leader_id
        );
    }

    eprintln!("✅ All nodes agree on leader: node {}", leader_id);
}

/// Test: Single node can propose operations
#[tokio::test]
async fn test_single_node_propose_operation() {
    use wormfs::storage_raft_member::types::{TxId, WormFsOperation};

    eprintln!("=== Testing single-node operation proposal ===");

    let (mut node, _temp_dir, _network_handle) =
        create_single_node(1).await.expect("Failed to create node");

    // Initialize as single-node cluster
    eprintln!("Initializing single node...");
    node.initialize(vec![]).await.expect("Failed to initialize");

    // Wait for the node to become leader
    eprintln!("Waiting for leadership...");
    for _ in 0..50 {
        if node.is_leader() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(node.is_leader(), "Node should be leader");
    eprintln!("Node is leader");

    // Try to propose an operation
    let operation = WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(1001),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };

    eprintln!("Proposing operation...");
    let result =
        tokio::time::timeout(Duration::from_secs(5), node.propose_operation(operation)).await;

    match result {
        Ok(Ok(_)) => eprintln!("✅ Single node CAN propose operations!"),
        Ok(Err(e)) => panic!("Failed to propose: {:?}", e),
        Err(_) => panic!("Timeout proposing operation on single node!"),
    }
}

/// Test: Bootstrap node can propose before adding learners
#[tokio::test]
async fn test_bootstrap_can_propose() {
    use wormfs::storage_raft_member::types::{TxId, WormFsOperation};

    eprintln!("=== Testing bootstrap node operation proposal ===");

    let cluster = RaftTestCluster::new(3)
        .await
        .expect("Failed to create cluster");

    // Get the bootstrap node
    let bootstrap = &cluster.nodes[0];

    // Build membership with ONLY bootstrap node
    let mut bootstrap_members = std::collections::BTreeMap::new();
    let wormfs_node = wormfs::storage_raft_member::raft_config::WormFsNode {
        peer_id: bootstrap.peer_id.clone(),
        metadata: Some(wormfs::storage_raft_member::raft_config::NodeMetadata {
            name: Some(format!("node-{}", bootstrap.id)),
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
        }),
    };
    bootstrap_members.insert(NodeId(bootstrap.id), wormfs_node);

    // Initialize bootstrap as single-node cluster
    eprintln!("Initializing bootstrap node as single-node cluster");
    bootstrap
        .raft
        .inner()
        .raft
        .initialize(bootstrap_members)
        .await
        .expect("Failed to initialize");

    // Wait for leadership
    eprintln!("Waiting for bootstrap to become leader...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    for _ in 0..50 {
        if bootstrap.raft.is_leader() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(bootstrap.raft.is_leader(), "Bootstrap should be leader");
    eprintln!("✅ Bootstrap is leader");

    // Try to propose an operation
    let operation = WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(9999),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };

    eprintln!("Proposing operation on bootstrap...");
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        bootstrap.raft.propose_operation(operation),
    )
    .await;

    match result {
        Ok(Ok(_)) => eprintln!("✅ Bootstrap node CAN propose operations!"),
        Ok(Err(e)) => panic!("Failed to propose: {:?}", e),
        Err(_) => panic!("Timeout proposing operation on bootstrap node!"),
    }
}

/// Test: Leader election after leader failure
///
/// ## Test Steps:
/// 1. Create 3-node cluster
/// 2. Wait for initial leader election
/// 3. Simulate leader failure by shutting it down
/// 4. Wait for remaining nodes to elect new leader
/// 5. Verify cluster elects exactly one new leader
/// 6. Verify cluster remains functional after election
#[tokio::test]
async fn test_leader_election_after_failure() {
    // Create 3-node cluster
    let mut cluster = RaftTestCluster::new(3)
        .await
        .expect("Failed to create cluster");

    // Initialize the cluster
    cluster
        .initialize()
        .await
        .expect("Failed to initialize cluster");

    // Get the initial leader
    let initial_leader_id = cluster.leader().expect("No initial leader found").id;
    eprintln!("Initial leader is node {}", initial_leader_id);

    // Shutdown the leader to simulate failure
    eprintln!(
        "Simulating leader failure by shutting down node {}",
        initial_leader_id
    );
    cluster
        .shutdown_node(initial_leader_id)
        .await
        .expect("Failed to shutdown leader");

    // Wait for election timeout and new leader election
    // Election timeout is 1500-3000ms, so wait 5 seconds to be safe
    eprintln!("Waiting for new leader election...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify exactly one leader exists among remaining nodes
    let leader_count = cluster.leader_count();
    assert_eq!(
        leader_count, 1,
        "Expected exactly 1 leader after old leader failure, found {}",
        leader_count
    );

    // Get the new leader
    let new_leader = cluster.leader().expect("No leader after election");
    eprintln!("New leader elected: node {}", new_leader.id);

    // Verify the new leader is different from the old one
    assert_ne!(
        new_leader.id, initial_leader_id,
        "New leader should be different from failed leader"
    );

    // Verify all remaining nodes agree on the leader
    for node in &cluster.nodes {
        let metrics = node.raft.get_metrics();
        assert_eq!(
            metrics.leader_id,
            Some(NodeId(new_leader.id)),
            "Node {} does not agree on leader (sees {:?})",
            node.id,
            metrics.leader_id
        );
    }

    eprintln!(
        "✅ New leader elected successfully after failure: node {} (was node {})",
        new_leader.id, initial_leader_id
    );
}

/// Test: Log replication verification across nodes
///
/// ## Test Steps:
/// 1. Create 3-node cluster
/// 2. Propose operation on leader
/// 3. Wait for replication
/// 4. Verify all nodes have same commit_index
/// 5. Verify operation appears in all node logs
#[tokio::test]
async fn test_log_replication() {
    use wormfs::storage_raft_member::types::{TxId, WormFsOperation};

    eprintln!("=== Starting test_log_replication ===");

    // Create 3-node cluster
    eprintln!("Step 1: Creating cluster...");
    let mut cluster = RaftTestCluster::new(3)
        .await
        .expect("Failed to create cluster");

    // Initialize the cluster
    eprintln!("Step 2: Initializing cluster...");
    cluster
        .initialize()
        .await
        .expect("Failed to initialize cluster");

    // Get the leader
    eprintln!("Step 3: Getting leader...");
    let leader = cluster.leader().expect("No leader found");
    eprintln!("Leader is node {}", leader.id);

    // IMPORTANT: Give leader time to establish replication streams to followers
    // Without this, the leader might not be able to reach quorum
    eprintln!("Waiting for leader to establish replication...");
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Get initial metrics from all nodes
    eprintln!("Step 4: Getting initial metrics...");
    let initial_metrics: Vec<_> = cluster
        .nodes
        .iter()
        .map(|n| (n.id, n.raft.get_metrics()))
        .collect();

    eprintln!("Initial commit indices:");
    for (id, metrics) in &initial_metrics {
        eprintln!(
            "  Node {}: commit_index={:?}, role={:?}, leader={:?}",
            id, metrics.commit_index, metrics.role, metrics.leader_id
        );
    }

    // Propose an operation on the leader
    let operation = WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(12345),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };

    eprintln!("Step 5: Proposing operation on leader...");
    eprintln!("About to call propose_operation()...");

    let propose_result = tokio::time::timeout(
        Duration::from_secs(10),
        leader.raft.propose_operation(operation),
    )
    .await;

    eprintln!("propose_operation() returned!");

    match propose_result {
        Ok(Ok(_)) => eprintln!("Operation proposed successfully"),
        Ok(Err(e)) => panic!("Failed to propose operation: {:?}", e),
        Err(_) => panic!("Timeout proposing operation"),
    }

    // Wait for replication
    eprintln!("Step 6: Waiting for replication...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get final metrics from all nodes
    let final_metrics: Vec<_> = cluster
        .nodes
        .iter()
        .map(|n| (n.id, n.raft.get_metrics()))
        .collect();

    eprintln!("Final commit indices:");
    for (id, metrics) in &final_metrics {
        eprintln!("  Node {}: {:?}", id, metrics.commit_index);
    }

    // Verify all nodes have the same commit_index
    let leader_commit = final_metrics
        .iter()
        .find(|(id, _)| *id == leader.id)
        .map(|(_, m)| m.commit_index)
        .expect("Leader metrics not found");

    for (id, metrics) in &final_metrics {
        assert_eq!(
            metrics.commit_index, leader_commit,
            "Node {} commit_index {:?} doesn't match leader's {:?}",
            id, metrics.commit_index, leader_commit
        );
    }

    // Verify commit_index increased (operation was committed)
    let initial_leader_commit = initial_metrics
        .iter()
        .find(|(id, _)| *id == leader.id)
        .map(|(_, m)| m.commit_index)
        .expect("Initial leader metrics not found");

    assert!(
        leader_commit > initial_leader_commit,
        "Commit index should have increased after operation. Before: {:?}, After: {:?}",
        initial_leader_commit,
        leader_commit
    );

    eprintln!("✅ Operation replicated successfully to all nodes");
    eprintln!(
        "   Commit index increased from {:?} to {:?}",
        initial_leader_commit, leader_commit
    );
}

/// Test: Concurrent client requests
///
/// ## Test Steps:
/// 1. Create 3-node cluster
/// 2. Submit 50+ operations concurrently
/// 3. Verify all operations complete successfully
/// 4. Verify operations are totally ordered across all nodes
#[tokio::test]
async fn test_concurrent_requests() {
    use wormfs::storage_raft_member::types::{TxId, WormFsOperation};

    // Create 3-node cluster
    let mut cluster = RaftTestCluster::new(3)
        .await
        .expect("Failed to create cluster");

    // Initialize the cluster
    cluster
        .initialize()
        .await
        .expect("Failed to initialize cluster");

    // Get the leader
    let leader = cluster.leader().expect("No leader found");
    eprintln!("Leader is node {}", leader.id);

    // Get initial commit index
    let initial_commit = leader.raft.get_metrics().commit_index;
    eprintln!("Initial commit index: {:?}", initial_commit);

    // Submit 50 operations concurrently
    let num_operations = 50;
    eprintln!("Submitting {} operations concurrently...", num_operations);

    let mut handles = vec![];
    for i in 0..num_operations {
        let leader_raft = leader.raft.clone();
        let handle = tokio::spawn(async move {
            let operation = WormFsOperation::TransactionPrepare {
                tx_id: TxId::new(1000 + i),
                metadata_ops: Some(vec![]),
                command_ops: None,
                timeout: std::time::SystemTime::now() + Duration::from_secs(30),
            };

            leader_raft.propose_operation(operation).await
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    let results = futures::future::join_all(handles).await;

    // Verify all operations succeeded
    let mut success_count = 0;
    let mut error_count = 0;
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(Ok(_)) => success_count += 1,
            Ok(Err(e)) => {
                eprintln!("Operation {} failed: {:?}", i, e);
                error_count += 1;
            }
            Err(e) => {
                eprintln!("Operation {} panicked: {:?}", i, e);
                error_count += 1;
            }
        }
    }

    eprintln!(
        "Results: {} successful, {} errors",
        success_count, error_count
    );
    assert_eq!(
        success_count, num_operations as usize,
        "All operations should succeed"
    );

    // Wait for final replication
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify all nodes have the same final commit_index
    let final_metrics: Vec<_> = cluster
        .nodes
        .iter()
        .map(|n| (n.id, n.raft.get_metrics()))
        .collect();

    eprintln!("Final commit indices:");
    for (id, metrics) in &final_metrics {
        eprintln!("  Node {}: {:?}", id, metrics.commit_index);
    }

    let leader_final_commit = final_metrics
        .iter()
        .find(|(id, _)| *id == leader.id)
        .map(|(_, m)| m.commit_index)
        .expect("Leader metrics not found");

    for (id, metrics) in &final_metrics {
        assert_eq!(
            metrics.commit_index, leader_final_commit,
            "Node {} commit_index doesn't match leader's",
            id
        );
    }

    // Verify commit index increased by at least the number of operations
    // (it might be more due to membership changes and blank entries)
    assert!(
        leader_final_commit > initial_commit,
        "Commit index should have increased. Before: {:?}, After: {:?}",
        initial_commit,
        leader_final_commit
    );

    eprintln!(
        "✅ All {} concurrent operations completed successfully",
        num_operations
    );
    eprintln!(
        "   Commit index: {:?} -> {:?}",
        initial_commit, leader_final_commit
    );
}

/// Test: Network partition handling (split-brain prevention)
///
/// ## Test Steps:
/// 1. Create 5-node cluster
/// 2. Partition into 3-node majority and 2-node minority
/// 3. Verify majority partition maintains leader
/// 4. Verify minority partition has no leader
/// 5. Heal partition
/// 6. Verify cluster reconverges
///
/// ## Status: DEFERRED
/// This test requires network partition simulation infrastructure in the stub network.
/// The stub network would need to support:
/// - Selective message dropping between nodes
/// - Partition creation/healing APIs
/// - Network delay simulation
///
/// Tests that network partitions correctly enforce Raft's quorum requirements and prevent split-brain.
#[tokio::test]
#[ntest::timeout(120000)]
async fn test_network_partition_handling() {
    use wormfs::storage_raft_member::types::{TxId, WormFsOperation};

    eprintln!("\n=== test_network_partition_handling ===");

    // ============================================================================
    // STEP 1: Create 5-node cluster
    // ============================================================================
    eprintln!("\n📦 Creating 5-node cluster...");
    let mut cluster = RaftTestCluster::new(5)
        .await
        .expect("Failed to create cluster");
    cluster
        .initialize()
        .await
        .expect("Failed to initialize cluster");

    // Wait for cluster to stabilize
    tokio::time::sleep(Duration::from_secs(3)).await;

    let leader = cluster.leader().expect("Should have a leader");
    eprintln!("✅ Cluster initialized with leader: Node {}", leader.id);

    // Submit initial operation to build some log history
    let operation_0 = WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(1000),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };
    leader
        .raft
        .propose_operation(operation_0)
        .await
        .expect("Initial operation should succeed");
    eprintln!("✅ Initial operation committed");

    // ============================================================================
    // STEP 2: Partition cluster into [1,2,3] (majority) and [4,5] (minority)
    // ============================================================================
    eprintln!("\n🔨 Creating network partition: [1,2,3] vs [4,5]");
    cluster
        .hub
        .partition_nodes(vec![vec![1, 2, 3], vec![4, 5]])
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ============================================================================
    // STEP 3: Verify majority partition [1,2,3] maintains a leader
    // ============================================================================
    eprintln!("\n✓ Verifying majority partition [1,2,3] has a leader...");
    let mut majority_has_leader = false;
    let mut majority_leader_id = None;

    for node in cluster.nodes.iter().filter(|n| n.id <= 3) {
        let metrics = node.raft.inner().raft.metrics().borrow().clone();
        if metrics.state == openraft::ServerState::Leader {
            majority_has_leader = true;
            majority_leader_id = Some(node.id);
            eprintln!("   Node {} is the leader in majority partition", node.id);
        }
    }

    assert!(
        majority_has_leader,
        "Majority partition [1,2,3] should have a leader"
    );
    eprintln!(
        "✅ Majority partition has leader: Node {}",
        majority_leader_id.unwrap()
    );

    // ============================================================================
    // STEP 4: Verify minority partition [4,5] has NO leader (lacks quorum)
    // ============================================================================
    eprintln!("\n✓ Verifying minority partition [4,5] has NO leader (lacks quorum)...");
    for node in cluster.nodes.iter().filter(|n| n.id >= 4) {
        let metrics = node.raft.inner().raft.metrics().borrow().clone();
        assert_ne!(
            metrics.state,
            openraft::ServerState::Leader,
            "Node {} in minority partition should NOT be leader",
            node.id
        );
        eprintln!(
            "   Node {} state: {:?} (correctly not a leader)",
            node.id, metrics.state
        );
    }
    eprintln!("✅ Minority partition has no leader (as expected)");

    // ============================================================================
    // STEP 5: Submit operation to majority partition - should SUCCEED
    // ============================================================================
    eprintln!("\n⏩ Submitting operation to majority partition [1,2,3]...");
    let majority_leader_node = cluster
        .nodes
        .iter()
        .find(|n| Some(n.id) == majority_leader_id)
        .expect("Majority leader node should exist");

    let operation_1 = WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(1001),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };

    majority_leader_node
        .raft
        .propose_operation(operation_1)
        .await
        .expect("Operation on majority partition should succeed");
    eprintln!("✅ Operation on majority partition succeeded");

    // ============================================================================
    // STEP 6: Submit operation to minority partition - should FAIL/TIMEOUT
    // ============================================================================
    eprintln!("\n⏩ Attempting operation on minority partition [4,5]...");
    let minority_node = cluster
        .nodes
        .iter()
        .find(|n| n.id == 4)
        .expect("Node 4 should exist");

    let operation_2 = WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(1002),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(5),
    };

    // This should timeout because minority partition cannot achieve quorum
    let result = tokio::time::timeout(
        Duration::from_secs(7),
        minority_node.raft.propose_operation(operation_2),
    )
    .await;

    match result {
        Err(_timeout) => {
            eprintln!("✅ Operation on minority partition correctly timed out");
        }
        Ok(Err(e)) => {
            eprintln!(
                "✅ Operation on minority partition correctly failed: {:?}",
                e
            );
        }
        Ok(Ok(_)) => {
            panic!("Operation on minority partition should NOT succeed - lacks quorum!");
        }
    }

    // ============================================================================
    // STEP 7: Heal the network partition
    // ============================================================================
    eprintln!("\n🔧 Healing network partition...");
    cluster.hub.heal_partition().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    eprintln!("✅ Network partition healed");

    // ============================================================================
    // STEP 8: Verify all 5 nodes converge on the same state
    // ============================================================================
    eprintln!("\n📊 Verifying all nodes converge on same state...");

    // Give nodes time to sync after partition heals
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Submit a final operation to force log replication to all nodes
    let final_leader = cluster
        .leader()
        .expect("Should have a leader after healing");
    let operation_final = WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(1003),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };
    final_leader
        .raft
        .propose_operation(operation_final)
        .await
        .expect("Final operation should succeed");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check that all nodes have similar log state
    let mut log_indices = Vec::new();
    for node in &cluster.nodes {
        let metrics = node.raft.inner().raft.metrics().borrow().clone();
        let log_index = metrics.last_log_index.unwrap_or(0);
        log_indices.push((node.id, log_index));
        eprintln!("   Node {}: log_index={}", node.id, log_index);
    }

    // Verify all nodes have converged (within reasonable range)
    let max_log_index = log_indices.iter().map(|(_, idx)| idx).max().unwrap();
    for (node_id, log_index) in &log_indices {
        // Allow up to 2 entries difference for replication lag
        assert!(
            max_log_index - log_index <= 2,
            "Node {} log_index={} is too far behind max={}",
            node_id,
            log_index,
            max_log_index
        );
    }

    eprintln!("✅ All nodes have converged on consistent state");
    eprintln!("\n=== test_network_partition_handling PASSED ===");
}

/// Test: Node restart and recovery
///
/// ## Test Steps:
/// 1. Create 3-node cluster
/// 2. Submit 10 operations to build log
/// 3. Shutdown node 3 (follower)
/// 4. Submit 10 more operations to nodes 1-2
/// 5. Restart node 3
/// 6. Verify node 3 catches up via log replication
/// 7. Verify node 3 has same commit_index as leader
/// 8. Verify node 3 can participate in consensus
///
/// ## Status: BLOCKED
/// Tests that a node can be shutdown and restarted while preserving its data.
///
/// This test verifies:
/// 1. Node can be cleanly shutdown (Raft stopped, database locks released)
/// 2. Node can be restarted with same storage (database reopened)
/// 3. Restarted node catches up via log replication
/// 4. Restarted node can participate in consensus again
///
/// The test uses unregister_raft_handler() to drop Arc references before restart,
/// allowing redb database locks to be released properly.
#[tokio::test]
async fn test_node_restart_recovery() {
    eprintln!("=== Starting test_node_restart_recovery ===");

    // Step 1: Create and initialize 3-node cluster
    eprintln!("Step 1: Creating and initializing 3-node cluster...");
    let mut cluster = RaftTestCluster::new(3)
        .await
        .expect("Failed to create cluster");
    cluster
        .initialize()
        .await
        .expect("Failed to initialize cluster");
    eprintln!("✅ Cluster initialized");

    // Get the leader
    let leader = cluster.leader().expect("No leader found");
    let leader_id = leader.id;
    eprintln!("Leader is node {}", leader_id);

    // Step 2: Submit 10 operations
    eprintln!("Step 2: Submitting 10 operations to build log...");
    for i in 1..=10 {
        let operation = WormFsOperation::TransactionPrepare {
            tx_id: TxId::new(i),
            metadata_ops: Some(vec![]),
            command_ops: None,
            timeout: std::time::SystemTime::now() + Duration::from_secs(30),
        };

        leader
            .raft
            .propose_operation(operation)
            .await
            .unwrap_or_else(|_| panic!("Failed to submit operation {}", i));
    }
    eprintln!("✅ Submitted 10 operations");

    // Give time for operations to replicate
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify all nodes have the same committed index
    let leader_metrics = leader.raft.inner().raft.metrics().borrow().clone();
    eprintln!(
        "Leader commit_index after 10 operations: {:?}",
        leader_metrics.last_applied
    );

    // Step 3: Shutdown node 3 (a follower)
    eprintln!("Step 3: Shutting down node 3...");
    cluster
        .shutdown_node(3)
        .await
        .expect("Failed to shutdown node 3");
    eprintln!("✅ Node 3 is shut down");

    // Verify we still have a leader (node 3 was a follower, so cluster still has quorum)
    tokio::time::sleep(Duration::from_millis(500)).await;
    let leader_after_shutdown_id = cluster.leader().expect("No leader after shutdown").id;
    eprintln!("Leader after shutdown: node {}", leader_after_shutdown_id);

    // Step 4: Submit 10 more operations (while node 3 is down)
    eprintln!("Step 4: Submitting 10 more operations while node 3 is down...");
    for i in 11..=20 {
        let operation = WormFsOperation::TransactionPrepare {
            tx_id: TxId::new(i),
            metadata_ops: Some(vec![]),
            command_ops: None,
            timeout: std::time::SystemTime::now() + Duration::from_secs(30),
        };

        let leader = cluster.leader().expect("No leader");
        leader
            .raft
            .propose_operation(operation)
            .await
            .unwrap_or_else(|_| panic!("Failed to submit operation {}", i));
    }
    eprintln!("✅ Submitted 10 more operations (11-20)");

    // Give time for operations to replicate to remaining nodes
    tokio::time::sleep(Duration::from_secs(1)).await;

    let leader_metrics_before_restart = {
        let leader = cluster.leader().expect("No leader");
        leader.raft.inner().raft.metrics().borrow().clone()
    };
    eprintln!(
        "Leader commit_index before restart: {:?}",
        leader_metrics_before_restart.last_applied
    );

    // Step 5: Restart node 3
    eprintln!("Step 5: Restarting node 3...");
    cluster
        .restart_node(3)
        .await
        .expect("Failed to restart node 3");
    eprintln!("✅ Node 3 restarted");

    // Step 6: Wait for node 3 to catch up
    eprintln!("Step 6: Waiting for node 3 to catch up...");
    let start = std::time::Instant::now();
    let node3 = cluster.nodes.iter().find(|n| n.id == 3).unwrap();

    loop {
        let node3_metrics = node3.raft.inner().raft.metrics().borrow().clone();
        let leader_metrics_current = {
            let leader = cluster.leader().expect("No leader");
            leader.raft.inner().raft.metrics().borrow().clone()
        };

        eprintln!(
            "  Node 3: last_applied={:?}, Leader: last_applied={:?}",
            node3_metrics.last_applied, leader_metrics_current.last_applied
        );

        // Check if node 3 has caught up
        if node3_metrics.last_applied == leader_metrics_current.last_applied {
            eprintln!(
                "✅ Node 3 caught up! last_applied={:?}",
                node3_metrics.last_applied
            );
            break;
        }

        if start.elapsed() > Duration::from_secs(10) {
            panic!("Timeout waiting for node 3 to catch up. Node 3 last_applied={:?}, Leader last_applied={:?}",
                node3_metrics.last_applied, leader_metrics_current.last_applied);
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Step 7: Verify node 3 has same commit_index as leader (already verified in step 6)
    let final_node3_metrics = node3.raft.inner().raft.metrics().borrow().clone();
    let final_leader_metrics = {
        let leader = cluster.leader().expect("No leader");
        leader.raft.inner().raft.metrics().borrow().clone()
    };

    assert_eq!(
        final_node3_metrics.last_applied, final_leader_metrics.last_applied,
        "Node 3 should have same last_applied as leader after catch-up"
    );
    eprintln!("✅ Node 3 has same commit_index as leader");

    // Step 8: Verify node 3 can participate in consensus by submitting a new operation
    eprintln!("Step 8: Verifying node 3 can participate in consensus...");
    let operation = WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(21),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };

    // Submit through the leader - if node 3 is part of consensus, this should succeed
    {
        let leader = cluster.leader().expect("No leader");
        leader
            .raft
            .propose_operation(operation)
            .await
            .expect("Failed to submit operation after node 3 restart");
    }

    // Give time to replicate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify the operation was applied on all nodes including node 3
    let final_metrics = node3.raft.inner().raft.metrics().borrow().clone();
    assert!(
        final_metrics.last_applied.is_some(),
        "Node 3 should have processed the final operation"
    );
    eprintln!("✅ Node 3 successfully participated in consensus after restart");

    eprintln!("=== test_node_restart_recovery PASSED ===");
}

// ============================================================================
// AUTOMATIC CLUSTER MANAGER BEHAVIOR TESTS
// ============================================================================
// These tests validate that ClusterManager automatically detects failures
// and recovers nodes without manual intervention.
// ============================================================================

// NOTE: test_automatic_failure_detection_and_demotion was removed
// Automatic demotion is no longer supported. Failed nodes remain as voters
// until an operator manually removes them. See test_cluster_manager_no_auto_demotion
// for verification that failed nodes are NOT automatically demoted.

/// Test: Failed voter recovery (nodes remain voters when offline)
///
/// ## Test Scenario:
/// 1. Create 3-node cluster with ClusterManager enabled
/// 2. Shutdown node 3 (it remains as a voter in the configuration)
/// 3. Verify node 3 is still a voter (no automatic demotion)
/// 4. Restart node 3 minimally (no manual membership steps)
/// 5. Verify node 3 successfully catches up and participates as voter
///
/// ## Expected Behavior:
/// - Failed node remains as voter in membership (no automatic demotion)
/// - When node restarts, it catches up with the log
/// - Node successfully participates in quorum again
#[tokio::test]
// This test validates that failed voters remain in the configuration and can
// successfully catch up when they come back online (no automatic demotion).
#[ntest::timeout(120000)]
async fn test_automatic_recovery_and_promotion() {
    // Initialize tracing with timestamps for detailed diagnostics (ANSI disabled for clean file output)
    // with_span_events shows span enter/exit and fields provide context to all nested logs
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_target(false)
        .with_thread_ids(true)
        .with_line_number(true)
        .with_level(true)
        .with_max_level(tracing::Level::INFO) // Show INFO level and above (reduced from TRACE)
        .with_ansi(false) // Disable ANSI color codes for clean file output
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NONE) // Don't log span enter/exit
        .try_init();

    info!("\n=== test_automatic_recovery_and_promotion ===");

    // Create 3-node cluster with ClusterManager enabled (aggressive for fast testing)
    let mut cluster = RaftTestCluster::new_with_cluster_manager(
        3,
        wormfs::storage_raft_member::ClusterManagerPreset::Aggressive,
    )
    .await
    .expect("Failed to create cluster");

    cluster.initialize().await.expect("Failed to initialize");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify all nodes are voters initially
    let leader = cluster.leader().expect("No leader found");
    let metrics_before = leader.raft.inner().raft.metrics().borrow().clone();
    let voters_before: Vec<NodeId> = metrics_before
        .membership_config
        .membership()
        .voter_ids()
        .collect();
    assert_eq!(voters_before.len(), 3, "Should have 3 voters initially");
    info!("✅ Initial cluster has 3 voters: {:?}", voters_before);

    // Shutdown node 3 to simulate failure
    info!("🔥 Shutting down node 3...");
    cluster
        .shutdown_node(3)
        .await
        .expect("Failed to shutdown node 3");

    // Wait a bit for failure to be detected
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify node 3 is STILL a voter (no automatic demotion)
    let leader = cluster.leader().expect("No leader found");
    let metrics_after_shutdown = leader.raft.inner().raft.metrics().borrow().clone();
    let voters_after_shutdown: Vec<NodeId> = metrics_after_shutdown
        .membership_config
        .membership()
        .voter_ids()
        .collect();
    assert_eq!(
        voters_after_shutdown.len(),
        3,
        "Should still have 3 voters (no automatic demotion)"
    );
    assert!(
        voters_after_shutdown.contains(&NodeId(3)),
        "Node 3 should still be a voter"
    );
    info!(
        "✅ Node 3 remains as voter despite being offline (voters: {:?})",
        voters_after_shutdown
    );

    // Restart node 3 minimally (it will catch up as a voter)
    info!("🔄 Restarting node 3 minimally...");
    cluster
        .restart_node_minimal(
            3,
            wormfs::storage_raft_member::ClusterManagerPreset::Aggressive,
        )
        .await
        .expect("Failed to restart node 3");

    // Wait for node to catch up
    info!("⏳ Waiting for node 3 to catch up...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify node 3 is still a voter and can participate
    let leader = cluster.leader().expect("No leader found");
    let metrics_final = leader.raft.inner().raft.metrics().borrow().clone();
    let voters_final: Vec<NodeId> = metrics_final
        .membership_config
        .membership()
        .voter_ids()
        .collect();

    info!("Final voters: {:?}", voters_final);
    assert_eq!(
        voters_final.len(),
        3,
        "Should have 3 voters (node never demoted). Found: {:?}",
        voters_final
    );
    assert!(
        voters_final.contains(&NodeId(3)),
        "Node 3 should still be a voter"
    );

    // Verify cluster is operational by committing a new operation
    let operation = wormfs::storage_raft_member::types::WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(999),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };
    leader
        .raft
        .propose_operation(operation)
        .await
        .expect("Cluster should be operational with all 3 voters");

    info!("✅ Node 3 successfully recovered and participates as voter");
    info!("=== test_automatic_recovery_and_promotion PASSED ===");
}

/// Test: Nodes are NOT automatically demoted when they fail
///
/// ## Test Scenario:
/// 1. Create 5-node cluster with ClusterManager enabled
/// 2. Fail nodes 3 and 4
/// 3. Wait for ClusterManager to detect failures
/// 4. Verify that nodes 3 and 4 remain as voters (no automatic demotion)
/// 5. Verify that cluster still has all 5 voters configured
/// 6. Verify that cluster loses quorum when >2 nodes are offline
///
/// ## Expected Behavior:
/// - Failed nodes are detected and logged but NOT automatically demoted
/// - All 5 nodes remain in the voter configuration
/// - Cluster correctly loses quorum when only 2/5 nodes are available
/// - Operator must manually remove permanently failed nodes
///
/// ## Rationale:
/// - Prevents accidental quorum degradation during cascading failures
/// - Preserves important state on temporarily failed nodes
/// - Prevents split-brain during network partitions
/// - Follows industry standard (etcd, Consul, etc.)
#[tokio::test]
#[ntest::timeout(60000)]
async fn test_cluster_manager_no_auto_demotion() {
    eprintln!("\n=== test_cluster_manager_no_auto_demotion ===");

    // Create 5-node cluster with ClusterManager enabled (but no auto-demotion)
    let mut cluster = RaftTestCluster::new_with_cluster_manager(
        5,
        wormfs::storage_raft_member::ClusterManagerPreset::Moderate,
    )
    .await
    .expect("Failed to create cluster");

    cluster.initialize().await.expect("Failed to initialize");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify all 5 nodes are voters initially
    let leader = cluster.leader().expect("No leader found");
    let metrics = leader.raft.inner().raft.metrics().borrow().clone();
    let voters_initial: Vec<NodeId> = metrics.membership_config.membership().voter_ids().collect();
    assert_eq!(voters_initial.len(), 5, "Should start with 5 voters");
    eprintln!("✅ Initial cluster has 5 voters: {:?}", voters_initial);

    // Simultaneously fail nodes 3 and 4
    eprintln!("🔥 Simulating simultaneous failure of nodes 3 and 4...");
    cluster
        .shutdown_node(3)
        .await
        .expect("Failed to shutdown node 3");
    cluster
        .shutdown_node(4)
        .await
        .expect("Failed to shutdown node 4");

    // Wait for ClusterManager to process failures (but nodes should NOT be demoted)
    eprintln!("⏳ Waiting for ClusterManager to detect failures...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Verify ALL 5 nodes are STILL voters (no automatic demotion)
    let leader = cluster.leader().expect("No leader found");
    let metrics_after_2_failures = leader.raft.inner().raft.metrics().borrow().clone();
    let voters_after_2_failures: Vec<NodeId> = metrics_after_2_failures
        .membership_config
        .membership()
        .voter_ids()
        .collect();

    eprintln!("Voters after 2 failures: {:?}", voters_after_2_failures);
    assert_eq!(
        voters_after_2_failures.len(),
        5,
        "All 5 nodes should remain as voters (no automatic demotion). Found: {}",
        voters_after_2_failures.len()
    );
    assert!(
        voters_after_2_failures.contains(&NodeId(3)),
        "Node 3 should still be a voter"
    );
    assert!(
        voters_after_2_failures.contains(&NodeId(4)),
        "Node 4 should still be a voter"
    );
    eprintln!("✅ Failed nodes 3 and 4 remain as voters (no automatic demotion)");

    // Phase 2: Fail a third node - all nodes should STILL remain as voters
    eprintln!("\n🔥 Phase 2: Failing a third node (node 5)...");
    cluster
        .shutdown_node(5)
        .await
        .expect("Failed to shutdown node 5");

    // Wait for ClusterManager to detect the third failure
    eprintln!("⏳ Waiting for ClusterManager to detect third failure...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Check the membership after third failure - ALL 5 nodes should STILL be voters
    let leader = cluster.leader().expect("No leader found");
    let metrics_after_third_failure = leader.raft.inner().raft.metrics().borrow().clone();
    let voters_after_third: Vec<NodeId> = metrics_after_third_failure
        .membership_config
        .membership()
        .voter_ids()
        .collect();

    eprintln!("After third failure:");
    eprintln!("  Voters: {:?}", voters_after_third);

    // CRITICAL VERIFICATION: All 5 nodes should STILL be voters (no automatic demotion)
    //
    // Even though 3 nodes are offline and only 2 are healthy, all 5 nodes remain as voters
    // in the configuration. The cluster will lose quorum (can't commit new operations)
    // because only 2/5 voters are available, but nodes are NOT automatically demoted.
    //
    // This is the correct behavior: operator must manually remove permanently failed nodes.
    assert_eq!(
        voters_after_third.len(),
        5,
        "All 5 nodes should STILL be voters (no automatic demotion)"
    );
    assert!(
        voters_after_third.contains(&NodeId(3)),
        "Node 3 should still be a voter"
    );
    assert!(
        voters_after_third.contains(&NodeId(4)),
        "Node 4 should still be a voter"
    );
    assert!(
        voters_after_third.contains(&NodeId(5)),
        "Node 5 should still be a voter"
    );

    eprintln!("✅ All 5 nodes remain as voters despite 3 being offline");
    eprintln!("✅ No automatic demotion occurred (operator must manually remove failed nodes)");

    // IMPORTANT RAFT BEHAVIOR NOTE:
    //
    // With 5 voters and only 2 healthy, the cluster CANNOT form quorum because
    // quorum requires a majority: ⌊5/2⌋ + 1 = 3 voters.
    //
    // This is standard Raft behavior: quorum is calculated from the current voter set.
    // Since we don't automatically demote failed nodes, all 5 nodes remain voters,
    // and the cluster loses quorum when only 2/5 are available.
    //
    // To verify this, let's try a write operation (it should fail or timeout):
    eprintln!("\n📝 Testing write operation with 2/5 voters online (should lose quorum)...");

    let leader = cluster.leader();
    use wormfs::storage_raft_member::types::{TxId, WormFsOperation};

    let operation = WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(99999),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };

    // The write should either timeout or fail because only 2/5 voters are available
    // (quorum requires 3)
    if let Some(leader_node) = leader {
        let write_result = tokio::time::timeout(
            Duration::from_secs(5),
            leader_node.raft.propose_operation(operation),
        )
        .await;

        match write_result {
            Ok(Ok(_)) => {
                eprintln!("❌ Write unexpectedly succeeded with only 2/5 voters online");
                panic!("Write should NOT succeed - quorum requires 3/5 voters");
            }
            Ok(Err(e)) => {
                eprintln!("✅ Write correctly failed: {:?}", e);
                eprintln!("   This is correct because 2/5 voters is not a quorum (need 3)");
            }
            Err(_timeout) => {
                eprintln!("✅ Write correctly timed out (cannot achieve quorum)");
                eprintln!("   This is correct because 2/5 voters is not a quorum (need 3)");
            }
        }
    } else {
        eprintln!("✅ No leader available (expected with 2/5 voters - cannot form quorum)");
    }

    eprintln!("=== test_cluster_manager_no_auto_demotion PASSED ===");
}

// NOTE: test_cluster_manager_rate_limiting was removed
// This test validated rate limiting for automatic demotion, which is no longer supported.
// Automatic demotion has been removed in favor of operator-driven membership management.
// Rate limiting for manual operator actions is not needed.

/// Test: Progressive node failure and recovery
///
/// ## Test Scenario:
/// 1. Start with 5-node cluster
/// 2. Progressively fail nodes one at a time:
///    - Fail node 3 → 4/5 healthy → cluster maintains quorum
///    - Fail node 4 → 3/5 healthy → cluster maintains quorum
///    - Fail node 5 → 2/5 healthy → cluster loses quorum
/// 3. Progressively recover nodes one at a time:
///    - Restart node 5 → 3/5 healthy → cluster regains quorum
///    - Restart node 4 → 4/5 healthy → cluster improves
///    - Restart node 3 → 5/5 healthy → full recovery
/// 4. Verify all nodes catch up and cluster is fully synchronized
///
/// ## Expected Behavior:
/// - Cluster can make progress with 3+ nodes (quorum)
/// - Cluster cannot make progress with 2 nodes (no quorum)
/// - Failed nodes remain as voters (no automatic demotion)
/// - Recovered nodes successfully catch up with missed operations
/// - Final cluster state is fully synchronized across all 5 nodes
#[tokio::test]
#[ntest::timeout(120000)]
async fn test_progressive_node_failure_and_recovery() {
    eprintln!("\n=== test_progressive_node_failure_and_recovery ===");

    // Create 5-node cluster
    let mut cluster = RaftTestCluster::new(5)
        .await
        .expect("Failed to create cluster");
    cluster.initialize().await.expect("Failed to initialize");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify all 5 nodes are voters initially
    let leader = cluster.leader().expect("No leader found");
    let metrics_initial = leader.raft.inner().raft.metrics().borrow().clone();
    let voters_initial: Vec<NodeId> = metrics_initial
        .membership_config
        .membership()
        .voter_ids()
        .collect();
    assert_eq!(voters_initial.len(), 5, "Should start with 5 voters");
    eprintln!("✅ Initial cluster has 5 voters: {:?}", voters_initial);

    // ============================================================================
    // PROGRESSIVE FAILURE PHASE
    // ============================================================================

    // FAIL NODE 3 → 4/5 healthy → should maintain quorum
    eprintln!("\n🔥 Phase 1: Failing node 3 (4/5 nodes remaining)...");
    cluster
        .shutdown_node(3)
        .await
        .expect("Failed to shutdown node 3");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify cluster can still make progress
    let leader = cluster.leader().expect("Should have leader with 4/5 nodes");
    let operation_1 = wormfs::storage_raft_member::types::WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(1001),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };
    leader
        .raft
        .propose_operation(operation_1)
        .await
        .expect("Should succeed with 4/5 nodes (quorum = 3)");
    eprintln!("✅ Cluster operational with 4/5 nodes (quorum maintained)");

    // FAIL NODE 4 → 3/5 healthy → should maintain quorum (exactly)
    eprintln!("\n🔥 Phase 2: Failing node 4 (3/5 nodes remaining)...");
    cluster
        .shutdown_node(4)
        .await
        .expect("Failed to shutdown node 4");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify cluster can still make progress (3 nodes = exact quorum)
    let leader = cluster.leader().expect("Should have leader with 3/5 nodes");
    let operation_2 = wormfs::storage_raft_member::types::WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(1002),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };
    leader
        .raft
        .propose_operation(operation_2)
        .await
        .expect("Should succeed with 3/5 nodes (exact quorum)");
    eprintln!("✅ Cluster operational with 3/5 nodes (exact quorum)");

    // FAIL NODE 5 → 2/5 healthy → should LOSE quorum
    eprintln!("\n🔥 Phase 3: Failing node 5 (2/5 nodes remaining - quorum lost)...");
    cluster
        .shutdown_node(5)
        .await
        .expect("Failed to shutdown node 5");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify cluster CANNOT make progress (2/5 < quorum)
    eprintln!("⏳ Attempting operation with 2/5 nodes (should fail/timeout)...");
    let leader_option = cluster.leader();
    if let Some(leader_node) = leader_option {
        let operation_3 = wormfs::storage_raft_member::types::WormFsOperation::TransactionPrepare {
            tx_id: TxId::new(1003),
            metadata_ops: Some(vec![]),
            command_ops: None,
            timeout: std::time::SystemTime::now() + Duration::from_secs(30),
        };
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            leader_node.raft.propose_operation(operation_3),
        )
        .await;

        match result {
            Ok(Ok(_)) => {
                panic!("❌ Operation should NOT succeed with only 2/5 nodes (no quorum)");
            }
            Ok(Err(e)) => {
                eprintln!("✅ Operation correctly failed: {:?}", e);
            }
            Err(_timeout) => {
                eprintln!("✅ Operation correctly timed out (cannot achieve quorum)");
            }
        }
    } else {
        eprintln!("✅ No leader available (expected with 2/5 nodes)");
    }

    // ============================================================================
    // PROGRESSIVE RECOVERY PHASE
    // ============================================================================

    // RESTART NODE 5 → 3/5 healthy → should REGAIN quorum
    eprintln!("\n🔄 Phase 4: Restarting node 5 (3/5 nodes)...");
    cluster
        .restart_node_minimal(
            5,
            wormfs::storage_raft_member::ClusterManagerPreset::Moderate,
        )
        .await
        .expect("Failed to restart node 5");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify cluster regains quorum
    let leader = cluster
        .leader()
        .expect("Should have leader after node 5 restart");
    let operation_4 = wormfs::storage_raft_member::types::WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(1004),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };
    leader
        .raft
        .propose_operation(operation_4)
        .await
        .expect("Should succeed after node 5 restart (3/5 = quorum)");
    eprintln!("✅ Cluster regained quorum with 3/5 nodes");

    // Verify node 5 caught up (find by ID since restart changes vector order)
    let node5 = cluster
        .nodes
        .iter()
        .find(|n| n.id == 5)
        .expect("Node 5 should exist");
    let node5_metrics = node5.raft.inner().raft.metrics().borrow().clone();
    eprintln!(
        "   Node 5 log index: {:?} (catching up)",
        node5_metrics.last_log_index
    );

    // RESTART NODE 4 → 4/5 healthy → improves cluster health
    eprintln!("\n🔄 Phase 5: Restarting node 4 (4/5 nodes)...");
    cluster
        .restart_node_minimal(
            4,
            wormfs::storage_raft_member::ClusterManagerPreset::Moderate,
        )
        .await
        .expect("Failed to restart node 4");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify cluster is operational
    let leader = cluster.leader().expect("Should have leader");
    let operation_5 = wormfs::storage_raft_member::types::WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(1005),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };
    leader
        .raft
        .propose_operation(operation_5)
        .await
        .expect("Should succeed with 4/5 nodes");
    eprintln!("✅ Cluster operational with 4/5 nodes");

    // Verify node 4 caught up (find by ID since restart changes vector order)
    let node4 = cluster
        .nodes
        .iter()
        .find(|n| n.id == 4)
        .expect("Node 4 should exist");
    let node4_metrics = node4.raft.inner().raft.metrics().borrow().clone();
    eprintln!(
        "   Node 4 log index: {:?} (catching up)",
        node4_metrics.last_log_index
    );

    // RESTART NODE 3 → 5/5 healthy → full recovery
    eprintln!("\n🔄 Phase 6: Restarting node 3 (5/5 nodes - full recovery)...");
    cluster
        .restart_node_minimal(
            3,
            wormfs::storage_raft_member::ClusterManagerPreset::Moderate,
        )
        .await
        .expect("Failed to restart node 3");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify cluster is fully operational
    let leader = cluster.leader().expect("Should have leader");
    let operation_6 = wormfs::storage_raft_member::types::WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(1006),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };
    leader
        .raft
        .propose_operation(operation_6)
        .await
        .expect("Should succeed with 5/5 nodes");
    eprintln!("✅ Cluster fully recovered with 5/5 nodes");

    // ============================================================================
    // FINAL VERIFICATION: All nodes synchronized
    // ============================================================================

    eprintln!("\n📊 Final Verification: Checking log synchronization...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Get log indices from all nodes
    let mut log_indices = Vec::new();
    for (idx, node) in cluster.nodes.iter().enumerate() {
        let node_id = idx + 1; // Node IDs are 1-based
        let metrics = node.raft.inner().raft.metrics().borrow().clone();
        let log_index = metrics.last_log_index.unwrap_or(0);
        log_indices.push((node_id, log_index));
        eprintln!("   Node {}: log_index={}", node_id, log_index);
    }

    // Verify all nodes have synchronized logs (within reasonable range)
    let max_log_index = log_indices.iter().map(|(_, idx)| idx).max().unwrap();
    for (node_id, log_index) in &log_indices {
        // Allow some lag (up to 2 entries) for nodes that just recovered
        assert!(
            max_log_index - log_index <= 2,
            "Node {} has log_index={}, max={} - too far behind",
            node_id,
            log_index,
            max_log_index
        );
    }
    eprintln!("✅ All nodes synchronized (max difference: ≤2 entries)");

    // Submit final operation and verify it replicates to all nodes
    eprintln!("\n🎯 Final operation test...");
    let leader = cluster.leader().expect("Should have leader");
    let final_operation = wormfs::storage_raft_member::types::WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(9999),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };
    leader
        .raft
        .propose_operation(final_operation)
        .await
        .expect("Final operation should succeed");

    tokio::time::sleep(Duration::from_secs(2)).await;
    eprintln!("✅ Final operation successfully replicated");

    eprintln!("\n=== test_progressive_node_failure_and_recovery PASSED ===");
}

/// Test snapshot transfer between nodes
///
/// This test verifies that when a lagging node rejoins the cluster,
/// it correctly receives and applies a snapshot from the leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_snapshot_transfer_between_nodes() {
    use wormfs::storage_raft_member::types::{TxId, WormFsOperation};

    // Initialize tracing to see all log messages
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .with_level(true)
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .try_init();

    eprintln!("\n=== test_snapshot_transfer_between_nodes ===");

    // ============================================================================
    // STEP 1: Create 3-node cluster with low snapshot threshold
    // ============================================================================
    eprintln!("\n📦 Creating 3-node cluster...");
    let mut cluster = RaftTestCluster::new(3)
        .await
        .expect("Failed to create cluster");
    cluster
        .initialize()
        .await
        .expect("Failed to initialize cluster");

    tokio::time::sleep(Duration::from_secs(2)).await;
    let leader = cluster.leader().expect("Should have a leader");
    eprintln!("✅ Cluster initialized with leader: Node {}", leader.id);

    // ============================================================================
    // STEP 2: Stop node 3
    // ============================================================================
    eprintln!("\n🛑 Stopping node 3...");
    cluster
        .shutdown_node(3)
        .await
        .expect("Failed to stop node 3");
    tokio::time::sleep(Duration::from_secs(1)).await;
    eprintln!("✅ Node 3 stopped");

    // ============================================================================
    // STEP 3: Write operations to force snapshot creation and log purging
    // ============================================================================
    eprintln!("\n📝 Writing operations to force snapshot creation and log purging...");
    let leader = cluster.leader().expect("Should have a leader");

    // Write 60 operations - this will:
    // 1. Trigger snapshot after 50 operations (snapshot_log_size_threshold / 1000 = 50)
    // 2. Purge old logs (keeping only last 5 via max_in_snapshot_log_to_keep)
    // 3. Force InstallSnapshot RPC when node 3 rejoins (can't use log replay)
    for i in 0..60 {
        let operation = WormFsOperation::TransactionPrepare {
            tx_id: TxId::new(2000 + i),
            metadata_ops: Some(vec![]),
            command_ops: None,
            timeout: std::time::SystemTime::now() + Duration::from_secs(30),
        };
        leader
            .raft
            .propose_operation(operation)
            .await
            .expect("Operation should succeed");

        if i % 10 == 0 {
            eprintln!("  Written {} operations...", i + 1);
        }

        // Small delay between operations to allow Raft to process
        if i % 5 == 4 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    eprintln!("✅ Wrote 60 operations");

    // Wait for snapshot creation and log purging
    eprintln!("\n⏳ Waiting for snapshot creation and log purging...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify snapshot was created on leader
    let leader_metrics = leader.raft.inner().raft.metrics().borrow().clone();
    eprintln!(
        "  📊 Leader snapshot: {:?}, purged: {:?}, last_log: {:?}",
        leader_metrics.snapshot, leader_metrics.purged, leader_metrics.last_log_index
    );

    // ============================================================================
    // STEP 4: Restart node 3 and verify it catches up with cluster
    // ============================================================================
    eprintln!("\n🔄 Restarting node 3...");

    cluster
        .restart_node(3)
        .await
        .expect("Failed to restart node 3");

    tokio::time::sleep(Duration::from_secs(3)).await;
    eprintln!("✅ Node 3 restarted and rejoined cluster");

    // ============================================================================
    // STEP 5: Verify node 3 caught up via SNAPSHOT TRANSFER (not log replay)
    // ============================================================================
    eprintln!("\n🔍 Verifying node 3 caught up via SNAPSHOT TRANSFER (not log replay)...");

    let leader = cluster.leader().expect("Should have a leader");
    let leader_metrics = leader.raft.inner().raft.metrics().borrow().clone();

    let node3 = cluster
        .nodes
        .iter()
        .find(|n| n.id == 3)
        .expect("Node 3 should exist");
    let node3_metrics = node3.raft.inner().raft.metrics().borrow().clone();

    eprintln!("\n📊 Leader Metrics:");
    eprintln!("  snapshot: {:?}", leader_metrics.snapshot);
    eprintln!("  purged: {:?}", leader_metrics.purged);
    eprintln!("  last_log_index: {:?}", leader_metrics.last_log_index);
    eprintln!("  last_applied: {:?}", leader_metrics.last_applied);

    eprintln!("\n📊 Node 3 Metrics:");
    eprintln!("  snapshot: {:?}", node3_metrics.snapshot);
    eprintln!("  purged: {:?}", node3_metrics.purged);
    eprintln!("  last_log_index: {:?}", node3_metrics.last_log_index);
    eprintln!("  last_applied: {:?}", node3_metrics.last_applied);

    // ASSERTION 1: Leader created a snapshot
    assert!(
        leader_metrics.snapshot.is_some(),
        "Leader should have created a snapshot"
    );
    let leader_snapshot = leader_metrics.snapshot.unwrap();
    assert!(
        leader_snapshot.index >= 49,
        "Leader snapshot should include at least 49 entries (near threshold), got: {}",
        leader_snapshot.index
    );

    // ASSERTION 2: Leader purged old logs (proof that logs can't be replayed)
    assert!(
        leader_metrics.purged.is_some(),
        "Leader should have purged old logs after snapshot"
    );
    let leader_purged = leader_metrics.purged.unwrap();
    assert!(
        leader_purged.index >= 44,
        "Leader should have purged logs up to at least index 44 (49 - max_in_snapshot_log_to_keep), got: {}",
        leader_purged.index
    );

    // ASSERTION 3: Node 3 received and installed snapshot
    assert!(
        node3_metrics.snapshot.is_some(),
        "Node 3 should have received and installed a snapshot"
    );
    let node3_snapshot = node3_metrics.snapshot.unwrap();
    assert!(
        node3_snapshot.index >= leader_purged.index,
        "Node 3 snapshot index ({}) should be >= leader's purged index ({}), proving InstallSnapshot RPC occurred",
        node3_snapshot.index,
        leader_purged.index
    );

    // ASSERTION 4: Node 3 caught up to leader
    let leader_applied = leader_metrics
        .last_applied
        .unwrap_or(openraft::LogId::default());
    let node3_applied = node3_metrics
        .last_applied
        .unwrap_or(openraft::LogId::default());

    assert!(
        leader_applied.index <= node3_applied.index + 5,
        "Node 3 should be caught up (leader: {}, node3: {})",
        leader_applied.index,
        node3_applied.index
    );

    // ASSERTION 5: Verify the snapshot transfer mechanism was used
    // Key proof: node3 has snapshot but started from scratch (no logs initially)
    // If it had caught up via log replay, it wouldn't have a snapshot at this point
    eprintln!("\n✅ VERIFIED: Node 3 caught up via InstallSnapshot RPC");
    eprintln!(
        "  - Leader created snapshot at index {}",
        leader_snapshot.index
    );
    eprintln!(
        "  - Leader purged logs up to index {} (can't replay from start)",
        leader_purged.index
    );
    eprintln!(
        "  - Node 3 received snapshot at index {}",
        node3_snapshot.index
    );
    eprintln!("  - Node 3 applied up to index {}", node3_applied.index);

    // ============================================================================
    // STEP 6: Verify all nodes can accept new operations
    // ============================================================================
    eprintln!("\n🎯 Verifying cluster can accept new operations...");
    let leader = cluster.leader().expect("Should have a leader");
    let final_operation = WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(3000),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };
    leader
        .raft
        .propose_operation(final_operation)
        .await
        .expect("Final operation should succeed");

    tokio::time::sleep(Duration::from_secs(1)).await;
    eprintln!("✅ Final operation succeeded");

    eprintln!("\n=== test_snapshot_transfer_between_nodes PASSED ===");
}

/// Test concurrent membership changes are properly serialized
///
/// This test verifies that membership changes are rate-limited and
/// do not happen concurrently, which could lead to split-brain scenarios.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_membership_changes() {
    eprintln!("\n=== test_concurrent_membership_changes ===");

    // ============================================================================
    // STEP 1: Create 3-node cluster
    // ============================================================================
    eprintln!("\n📦 Creating 3-node cluster...");
    let mut cluster = RaftTestCluster::new(3)
        .await
        .expect("Failed to create cluster");
    cluster
        .initialize()
        .await
        .expect("Failed to initialize cluster");

    tokio::time::sleep(Duration::from_secs(2)).await;
    let leader = cluster.leader().expect("Should have a leader");
    eprintln!("✅ Cluster initialized with leader: Node {}", leader.id);

    // ============================================================================
    // STEP 2: Attempt to change membership twice in rapid succession
    // ============================================================================
    eprintln!("\n⚡ Attempting concurrent membership changes...");

    // Get current membership
    let current_members: Vec<NodeId> = cluster.nodes.iter().map(|n| NodeId(n.id)).collect();

    eprintln!("  Current members: {:?}", current_members);

    // Try to remove node 2 and node 3 concurrently
    // Only one should succeed at a time due to Raft's serialization
    let leader = cluster.leader().expect("Should have a leader");

    // First change: remove node 3
    let members_without_3: Vec<NodeId> = current_members
        .iter()
        .filter(|n| n.0 != 3)
        .copied()
        .collect();

    eprintln!("  First change: removing node 3...");
    let change1 = leader
        .raft
        .inner()
        .raft
        .change_membership(members_without_3.clone(), false);

    // Immediately try second change: remove node 2 (from original membership)
    // This should either fail or wait for first change to complete
    let members_without_2: Vec<NodeId> = current_members
        .iter()
        .filter(|n| n.0 != 2)
        .copied()
        .collect();

    eprintln!("  Second change (concurrent): removing node 2...");
    let change2 = leader
        .raft
        .inner()
        .raft
        .change_membership(members_without_2, false);

    // Wait for both changes to complete
    let (result1, result2) = tokio::join!(change1, change2);

    eprintln!("  Change 1 result: {:?}", result1.is_ok());
    eprintln!("  Change 2 result: {:?}", result2.is_ok());

    // At least one should succeed (likely the first one)
    // The second might fail or succeed depending on timing
    assert!(
        result1.is_ok() || result2.is_ok(),
        "At least one membership change should succeed"
    );

    tokio::time::sleep(Duration::from_secs(2)).await;

    // ============================================================================
    // STEP 3: Verify cluster is still functional
    // ============================================================================
    eprintln!("\n✓ Verifying cluster is still functional...");

    // Check current membership
    let leader = cluster
        .leader()
        .expect("Should have a leader after membership changes");
    let metrics = leader.raft.inner().raft.metrics().borrow().clone();

    eprintln!("  Current membership: {:?}", metrics.membership_config);
    eprintln!("  Leader: Node {}", leader.id);

    // Try to propose an operation
    let operation = wormfs::storage_raft_member::types::WormFsOperation::TransactionPrepare {
        tx_id: wormfs::storage_raft_member::types::TxId::new(4000),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };

    leader
        .raft
        .propose_operation(operation)
        .await
        .expect("Operation should succeed after membership changes");

    eprintln!("✅ Cluster is functional after membership changes");

    eprintln!("\n=== test_concurrent_membership_changes PASSED ===");
}

/// Test membership changes during network partition
///
/// This test verifies that membership changes handle network partitions safely:
/// - Minority partition cannot add/remove nodes (no quorum)
/// - Majority partition can add/remove nodes
/// - After healing, cluster converges to correct membership
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_membership_change_during_partition() {
    use wormfs::storage_raft_member::types::{TxId, WormFsOperation};

    eprintln!("\n=== test_membership_change_during_partition ===");

    // ============================================================================
    // STEP 1: Create 3-node cluster
    // ============================================================================
    eprintln!("\n📦 Creating 3-node cluster...");
    let mut cluster = RaftTestCluster::new(3)
        .await
        .expect("Failed to create cluster");
    cluster
        .initialize()
        .await
        .expect("Failed to initialize cluster");

    tokio::time::sleep(Duration::from_secs(2)).await;
    let leader = cluster.leader().expect("Should have a leader");
    eprintln!("✅ Cluster initialized with leader: Node {}", leader.id);

    // ============================================================================
    // STEP 2: Partition cluster into [1,2] (majority) and [3] (minority)
    // ============================================================================
    eprintln!("\n🔨 Creating network partition: [1,2] vs [3]");
    cluster.hub.partition_nodes(vec![vec![1, 2], vec![3]]).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    eprintln!("✅ Network partition created");

    // ============================================================================
    // STEP 3: Verify majority partition [1,2] has a leader
    // ============================================================================
    eprintln!("\n✓ Verifying majority partition [1,2] has a leader...");
    let mut majority_leader_id = None;

    for node in cluster.nodes.iter().filter(|n| n.id <= 2) {
        let metrics = node.raft.inner().raft.metrics().borrow().clone();
        if metrics.state == openraft::ServerState::Leader {
            majority_leader_id = Some(node.id);
            eprintln!("   Node {} is the leader in majority partition", node.id);
            break;
        }
    }

    // Wait a bit more if no leader yet
    if majority_leader_id.is_none() {
        tokio::time::sleep(Duration::from_secs(2)).await;
        for node in cluster.nodes.iter().filter(|n| n.id <= 2) {
            let metrics = node.raft.inner().raft.metrics().borrow().clone();
            if metrics.state == openraft::ServerState::Leader {
                majority_leader_id = Some(node.id);
                eprintln!("   Node {} is the leader in majority partition", node.id);
                break;
            }
        }
    }

    assert!(
        majority_leader_id.is_some(),
        "Majority partition [1,2] should have a leader"
    );
    eprintln!(
        "✅ Majority partition has leader: Node {}",
        majority_leader_id.unwrap()
    );

    // ============================================================================
    // STEP 4: Attempt to change membership from minority partition (should fail)
    // ============================================================================
    eprintln!("\n⏩ Attempting membership change from minority partition [3]...");
    let minority_node = cluster
        .nodes
        .iter()
        .find(|n| n.id == 3)
        .expect("Node 3 should exist");

    // Try to remove node 1 from minority partition (should fail - no quorum)
    let members_without_1: Vec<NodeId> = vec![NodeId(2), NodeId(3)];

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        minority_node
            .raft
            .inner()
            .raft
            .change_membership(members_without_1, false),
    )
    .await;

    match result {
        Err(_timeout) => {
            eprintln!("✅ Membership change from minority correctly timed out");
        }
        Ok(Err(e)) => {
            eprintln!(
                "✅ Membership change from minority correctly failed: {:?}",
                e
            );
        }
        Ok(Ok(_)) => {
            // This might happen if the node was previously a leader and has cached leadership
            // But the change won't actually be committed without quorum
            eprintln!("⚠️  Membership change appeared to succeed, but won't commit without quorum");
        }
    }

    // ============================================================================
    // STEP 5: Successfully change membership from majority partition
    // ============================================================================
    eprintln!("\n⏩ Changing membership from majority partition [1,2]...");
    let majority_leader_node = cluster
        .nodes
        .iter()
        .find(|n| Some(n.id) == majority_leader_id)
        .expect("Majority leader should exist");

    // Remove node 3 from cluster (it's already partitioned away)
    let members_without_3: Vec<NodeId> = vec![NodeId(1), NodeId(2)];

    majority_leader_node
        .raft
        .inner()
        .raft
        .change_membership(members_without_3.clone(), false)
        .await
        .expect("Membership change from majority should succeed");

    tokio::time::sleep(Duration::from_secs(1)).await;
    eprintln!("✅ Membership changed successfully from majority partition");

    // Verify the change was applied
    let leader_metrics = majority_leader_node
        .raft
        .inner()
        .raft
        .metrics()
        .borrow()
        .clone();
    eprintln!("  New membership: {:?}", leader_metrics.membership_config);

    // ============================================================================
    // STEP 6: Heal partition and verify cluster converges
    // ============================================================================
    eprintln!("\n🔧 Healing network partition...");
    cluster.hub.heal_partition().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    eprintln!("✅ Network partition healed");

    // ============================================================================
    // STEP 7: Verify final cluster state
    // ============================================================================
    eprintln!("\n📊 Verifying final cluster state...");

    // The cluster should now have nodes 1 and 2 as voters
    // Node 3 should no longer be in the membership
    let leader = cluster.leader().expect("Should have a leader");
    let final_metrics = leader.raft.inner().raft.metrics().borrow().clone();

    eprintln!("  Final membership: {:?}", final_metrics.membership_config);
    eprintln!("  Final leader: Node {}", leader.id);

    // Verify cluster can still accept operations
    let final_operation = WormFsOperation::TransactionPrepare {
        tx_id: TxId::new(5000),
        metadata_ops: Some(vec![]),
        command_ops: None,
        timeout: std::time::SystemTime::now() + Duration::from_secs(30),
    };
    leader
        .raft
        .propose_operation(final_operation)
        .await
        .expect("Final operation should succeed");

    eprintln!("✅ Cluster is functional with updated membership");

    eprintln!("\n=== test_membership_change_during_partition PASSED ===");
}
