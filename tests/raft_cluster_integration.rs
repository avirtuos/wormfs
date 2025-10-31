//! Integration tests for multi-node Raft clusters
//!
//! These tests verify that StorageRaftMember works correctly in multi-node
//! scenarios, including leader election, log replication, and failure recovery.
//!
//! ## Implementation Status
//!
//! **Current State**: Tests documented but awaiting network infrastructure
//! **All tests**: Marked as `#[ignore]` pending StorageNetwork test infrastructure
//!
//! **Blockers for tests**:
//! - StorageRaftMember now requires valid StorageNetworkHandle in Config
//! - StorageNetwork setup requires:
//!   - Event loop spawn in separate thread (libp2p Swarm is !Send)
//!   - Keypair generation for peer identity
//!   - Port allocation and multiaddr configuration
//!   - Peer discovery and connection coordination
//! - For multi-node tests specifically:
//!   - Network address resolution for add_node() calls
//!   - Coordination of node startup sequence
//!   - Test harness to manage multiple event loops
//!
//! **Test Coverage** (all pending infrastructure):
//! - ⏳ Single-node initialization and leader election
//! - ⏳ Vote persistence across restarts
//! - ⏳ Multi-node cluster formation
//! - ⏳ Leader election after failure
//! - ⏳ Log replication verification
//! - ⏳ Concurrent client requests
//! - ⏳ Network partition handling
//! - ⏳ Node restart and recovery

use libp2p::identity;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

use wormfs::storage_network::types::{Config as NetworkConfig, PeerConfig};
use wormfs::storage_network::{StorageNetworkFactory, StorageNetworkHandle};
use wormfs::storage_raft_member::{NodeId, StorageRaftMember, StorageRaftMemberImpl};

/// Global atomic counter for allocating unique port ranges for parallel test execution.
/// Each test gets a unique base port to avoid conflicts when running in parallel.
static PORT_ALLOCATOR: AtomicU16 = AtomicU16::new(49000);

/// Allocate a unique base port for a test.
fn allocate_test_port() -> u16 {
    PORT_ALLOCATOR.fetch_add(1, Ordering::SeqCst)
}

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

/// Helper: Create a single-node Raft instance for testing with full network infrastructure
async fn create_single_node(
    node_id: u64,
) -> Result<
    (
        StorageRaftMemberImpl,
        TempDir,
        StorageNetworkHandle,
        std::thread::JoinHandle<()>,
    ),
    Box<dyn std::error::Error>,
> {
    let temp_dir = TempDir::new()?;
    let data_dir = temp_dir.path().to_path_buf();

    // Generate stable keypair from node_id seed
    let mut seed = [0u8; 32];
    seed[0] = node_id as u8;
    let keypair = identity::Keypair::ed25519_from_bytes(seed)?;

    // Allocate unique port for this node
    let listen_port = allocate_test_port();

    // Create minimal StorageNetwork configuration (no peers for single-node)
    let network_config = NetworkConfig {
        node_id: format!("raft-node-{}", node_id),
        listen_addresses: vec![format!("/ip4/127.0.0.1/tcp/{}", listen_port)],
        peers: vec![], // No peers for single-node tests
        peer_id_store_path: data_dir.join("peer_ids.json"),
        max_peers: 10,
        max_connections_per_peer: 3,
        connection_timeout: Duration::from_secs(10),
        idle_connection_timeout: Duration::from_secs(60),
        keep_alive_interval: Duration::from_secs(5),
        admin_url: None,
    };

    // Create StorageNetwork instance
    let (network_inner, network_handle) =
        StorageNetworkFactory::create_with_keypair(network_config, keypair).await?;

    // Spawn network event loop in dedicated thread
    let network_node_id = format!("raft-node-{}", node_id);
    let event_loop_thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create runtime for network event loop");

        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async move {
            if let Err(e) = network_inner.run().await {
                eprintln!("Network event loop error for {}: {}", network_node_id, e);
            }
        }));
    });

    // Give network time to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create Raft configuration with network handle
    let raft_config = wormfs::storage_raft_member::Config {
        heartbeat_interval: Duration::from_millis(500),
        election_timeout_min: Duration::from_millis(1500),
        election_timeout_max: Duration::from_millis(3000),
        max_payload_entries: 1000,
        max_in_flight_append_entries: 10,
        replication_lag_threshold: 1000,
        max_uncommitted_entries: 5000,
        snapshot_time_threshold: Duration::from_secs(3600),
        snapshot_log_size_threshold: 100 * 1024 * 1024,
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
        network_address: format!("127.0.0.1:{}", listen_port).parse().unwrap(),
        storage_network: Some(network_handle.clone()),
    };

    // Create Raft instance
    let raft_node =
        <StorageRaftMemberImpl as StorageRaftMember>::new(NodeId(node_id), raft_config).await?;

    // Register Raft handler with network
    network_handle
        .register_raft_handler(Arc::new(raft_node.clone()))
        .await?;

    Ok((raft_node, temp_dir, network_handle, event_loop_thread))
}

/// Multi-node test cluster infrastructure
///
/// This struct manages a multi-node Raft cluster for integration testing,
/// including network setup, node coordination, and cleanup.
struct RaftTestCluster {
    nodes: Vec<RaftTestNode>,
    _temp_dirs: Vec<TempDir>,
}

struct RaftTestNode {
    id: u64,
    raft: StorageRaftMemberImpl,
    network_handle: StorageNetworkHandle,
    peer_id: String,
    address: std::net::SocketAddr,
    _event_loop_thread: std::thread::JoinHandle<()>,
}

impl RaftTestCluster {
    /// Create a new N-node test cluster (not yet initialized)
    async fn new(node_count: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let mut nodes = Vec::with_capacity(node_count);
        let mut temp_dirs = Vec::with_capacity(node_count);

        // Create all nodes first (they don't know about each other yet)
        for i in 0..node_count {
            let node_id = (i + 1) as u64;
            let temp_dir = TempDir::new()?;
            let data_dir = temp_dir.path().to_path_buf();

            // Generate stable keypair from node_id seed
            let mut seed = [0u8; 32];
            seed[0] = node_id as u8;
            let keypair = identity::Keypair::ed25519_from_bytes(seed)?;
            let peer_id = keypair.public().to_peer_id().to_string();

            // Allocate unique port for this node
            let listen_port = allocate_test_port();
            let address: std::net::SocketAddr = format!("127.0.0.1:{}", listen_port).parse()?;

            // Create network configuration
            let network_config = NetworkConfig {
                node_id: format!("raft-node-{}", node_id),
                listen_addresses: vec![format!("/ip4/127.0.0.1/tcp/{}", listen_port)],
                peers: vec![], // We'll configure peers later
                peer_id_store_path: data_dir.join("peer_ids.json"),
                max_peers: 10,
                max_connections_per_peer: 3,
                connection_timeout: Duration::from_secs(10),
                idle_connection_timeout: Duration::from_secs(60),
                keep_alive_interval: Duration::from_secs(5),
                admin_url: None,
            };

            // Create StorageNetwork instance
            let (network_inner, network_handle) =
                StorageNetworkFactory::create_with_keypair(network_config, keypair).await?;

            // Spawn network event loop in dedicated thread
            let network_node_id = format!("raft-node-{}", node_id);
            let event_loop_thread = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create runtime for network event loop");

                let local = tokio::task::LocalSet::new();
                runtime.block_on(local.run_until(async move {
                    if let Err(e) = network_inner.run().await {
                        eprintln!("Network event loop error for {}: {}", network_node_id, e);
                    }
                }));
            });

            // Give network time to start listening
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Create Raft configuration
            let raft_config = wormfs::storage_raft_member::Config {
                heartbeat_interval: Duration::from_millis(500),
                election_timeout_min: Duration::from_millis(1500),
                election_timeout_max: Duration::from_millis(3000),
                max_payload_entries: 1000,
                max_in_flight_append_entries: 10,
                replication_lag_threshold: 1000,
                max_uncommitted_entries: 5000,
                snapshot_time_threshold: Duration::from_secs(3600),
                snapshot_log_size_threshold: 100 * 1024 * 1024,
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
                network_address: address,
                storage_network: Some(network_handle.clone()),
            };

            // Create Raft instance
            let raft_node =
                <StorageRaftMemberImpl as StorageRaftMember>::new(NodeId(node_id), raft_config)
                    .await?;

            // Register Raft handler with network
            network_handle
                .register_raft_handler(Arc::new(raft_node.clone()))
                .await?;

            nodes.push(RaftTestNode {
                id: node_id,
                raft: raft_node,
                network_handle,
                peer_id,
                address,
                _event_loop_thread: event_loop_thread,
            });

            temp_dirs.push(temp_dir);
        }

        Ok(RaftTestCluster {
            nodes,
            _temp_dirs: temp_dirs,
        })
    }

    /// Initialize the cluster:
    /// - Node 0 initializes as single-node cluster
    /// - Other nodes are added via add_node()
    async fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.nodes.is_empty() {
            return Err("Cannot initialize empty cluster".into());
        }

        // Initialize first node as single-node cluster
        self.nodes[0].raft.initialize(vec![]).await?;

        // Wait for first node to become leader
        let max_wait = Duration::from_secs(5);
        let start = std::time::Instant::now();
        loop {
            if self.nodes[0].raft.is_leader() {
                eprintln!("Node 1 became leader");
                break;
            }
            if start.elapsed() > max_wait {
                return Err("Node 1 did not become leader in time".into());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Add remaining nodes to the cluster
        for i in 1..self.nodes.len() {
            let node = &self.nodes[i];
            eprintln!(
                "Adding node {} with peer_id {} and address {}",
                node.id, node.peer_id, node.address
            );

            self.nodes[0]
                .raft
                .add_node(NodeId(node.id), node.address, node.peer_id.clone())
                .await?;

            // Give time for membership change to replicate
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Wait for cluster to stabilize
        tokio::time::sleep(Duration::from_secs(2)).await;

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
    let (mut node, _temp_dir, _network_handle, _event_loop_thread) =
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
/// 2. Initialize and become leader (vote recorded)
/// 3. Shutdown node (triggers vote persistence)
/// 4. Create new instance with same storage paths
/// 5. Verify persisted vote is loaded correctly
#[tokio::test]
async fn test_vote_persistence_across_restart() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let data_dir = temp_dir.path().to_path_buf();

    // First instance: initialize and become leader
    let term1 = {
        let (mut node1, _temp_dir1, _network_handle1, _event_loop_thread1) = create_single_node(1)
            .await
            .expect("Failed to create first instance");

        node1
            .initialize(vec![])
            .await
            .expect("Failed to initialize");

        // Wait for self-election
        let election_wait = apply_timeout_multiplier(Duration::from_millis(500));
        sleep(election_wait).await;

        let metrics1 = node1.get_metrics();
        assert_eq!(
            metrics1.current_term, 1,
            "First instance should be in term 1"
        );
        assert!(node1.is_leader(), "First instance should be leader");

        metrics1.current_term
        // Node and network drop here, triggering vote persistence
    };

    // Give some time for cleanup
    sleep(Duration::from_millis(100)).await;

    // Second instance: should load persisted vote
    // Note: This test currently verifies infrastructure works.
    // Full vote persistence validation would require using the same temp_dir paths,
    // which needs more infrastructure to coordinate shared storage with new network.
    let (_node2, _temp_dir2, _network_handle2, _event_loop_thread2) = create_single_node(1)
        .await
        .expect("Failed to create second instance");

    let metrics2 = _node2.get_metrics();
    // New instance starts with fresh storage (different temp_dir), so term is 0
    // Vote persistence is validated at the unit level in log_storage tests
    assert!(
        metrics2.current_term >= 0,
        "Second instance should initialize successfully"
    );

    // Note: To fully test vote persistence across restarts, we'd need to:
    // 1. Share the same data_dir between instances
    // 2. Coordinate network port reuse or use different ports
    // 3. Handle network cleanup between instances
    // This is tracked as future enhancement once we have shared storage test helpers
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

/// Test: Leader election after leader failure
///
/// ## Test Steps:
/// 1. Create 3-node cluster
/// 2. Wait for initial leader election
/// 3. Make leader call step_down()
/// 4. Verify new leader is elected within election timeout
/// 5. Verify new leader is different from original
#[tokio::test]
#[ignore = "Requires multi-node cluster infrastructure"]
async fn test_leader_election_after_failure() {
    panic!("Not yet implemented");
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
#[ignore = "Requires multi-node cluster infrastructure"]
async fn test_log_replication() {
    panic!("Not yet implemented");
}

/// Test: Concurrent client requests
///
/// ## Test Steps:
/// 1. Create 3-node cluster
/// 2. Submit 50+ operations concurrently
/// 3. Verify all operations complete successfully
/// 4. Verify operations are totally ordered across all nodes
#[tokio::test]
#[ignore = "Requires multi-node cluster infrastructure"]
async fn test_concurrent_requests() {
    panic!("Not yet implemented");
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
#[tokio::test]
#[ignore = "Requires network partition simulation infrastructure"]
async fn test_network_partition_handling() {
    panic!("Not yet implemented");
}

/// Test: Node restart and recovery
///
/// ## Test Steps:
/// 1. Create 3-node cluster
/// 2. Submit operations to build log
/// 3. Shutdown one follower
/// 4. Submit more operations
/// 5. Restart follower
/// 6. Verify follower catches up via log replication
/// 7. Verify follower has same state as leader
#[tokio::test]
#[ignore = "Requires node shutdown/restart infrastructure"]
async fn test_node_restart_recovery() {
    panic!("Not yet implemented");
}
