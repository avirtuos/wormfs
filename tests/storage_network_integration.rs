//! Integration tests for StorageNetwork component
//!
//! These tests validate multi-node networking scenarios including:
//! - Cluster formation and peer connectivity
//! - Gossipsub message propagation
//! - Request-response protocol
//! - Peer validation (explicit and AutoId modes)
//! - Network recovery and reconnection
//! - Concurrent operations
//!
//! ## Test Infrastructure
//!
//! Tests use a `TestCluster` helper that manages multiple StorageNetwork nodes
//! with isolated configurations and automatic cleanup.

use libp2p::identity;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::timeout;
use tracing;
use tracing_subscriber;
use wormfs::storage_network::types::{Config, ConnectionState, PeerConfig, PeerId, PeerIdConfig};
use wormfs::storage_network::{StorageNetworkFactory, StorageNetworkHandle};

/// Test cluster managing multiple StorageNetwork nodes
struct TestCluster {
    nodes: Vec<NodeInstance>,
    _temp_dirs: Vec<TempDir>,
}

/// Single node instance in a test cluster
struct NodeInstance {
    handle: StorageNetworkHandle,
    node_id: String,
    listen_port: u16,
    _event_loop_thread: std::thread::JoinHandle<()>,
}

impl TestCluster {
    /// Create a new test cluster with the specified number of nodes.
    ///
    /// All nodes are configured in AutoId mode by default and form a full mesh.
    async fn new(num_nodes: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let mut nodes = Vec::new();
        let mut _temp_dirs: Vec<TempDir> = Vec::new();

        // First pass: Generate stable keypairs for all nodes
        // We generate keypairs first so we can use Explicit peer IDs in configuration
        let mut keypairs = Vec::new();
        let mut peer_ids = Vec::new();

        for i in 0..num_nodes {
            // Generate a stable keypair for this node using a deterministic seed
            let mut seed = [0u8; 32];
            seed[0] = i as u8;
            let keypair = identity::Keypair::ed25519_from_bytes(seed)
                .expect("Failed to create keypair from seed");

            // Extract the peer ID from the keypair
            let libp2p_peer_id = libp2p::PeerId::from(keypair.public());
            let peer_id_bytes = libp2p_peer_id.to_bytes();
            let peer_id = PeerId::new(peer_id_bytes);

            keypairs.push(keypair);
            peer_ids.push(peer_id);
        }

        // Second pass: create all nodes with fixed test ports to avoid conflicts
        // Using base port 45000 + index
        let mut node_configs = Vec::new();
        let base_port = 45000_u16;

        for i in 0..num_nodes {
            let temp_dir = TempDir::new()?;
            let peer_id_store = temp_dir.path().join("peer_ids.json");
            let listen_port = base_port + (i as u16);

            node_configs.push((
                format!("node-{}", i),
                listen_port,
                peer_id_store.clone(),
                temp_dir,
            ));
        }

        // Third pass: configure peers for full mesh with Explicit peer IDs
        // Each node gets configured with all other nodes' addresses and explicit peer IDs
        for (i, (node_id, listen_port, peer_id_store, _temp_dir)) in node_configs.iter().enumerate()
        {
            // Build peer list: all other nodes with proper multiaddrs and explicit peer IDs
            let mut peers = Vec::new();
            for (j, (_, other_port, _, _)) in node_configs.iter().enumerate() {
                if i != j {
                    peers.push(PeerConfig {
                        multiaddr: format!("/ip4/127.0.0.1/tcp/{}", other_port),
                        peer_id: PeerIdConfig::Explicit(peer_ids[j].clone()),
                    });
                }
            }

            let config = Config {
                node_id: node_id.clone(),
                listen_addresses: vec![format!("/ip4/127.0.0.1/tcp/{}", listen_port)],
                peers: peers.clone(),
                peer_id_store_path: peer_id_store.clone(),
                max_peers: num_nodes,
                max_connections_per_peer: 3, // Allow multiple connections per peer for mesh formation
                connection_timeout: Duration::from_secs(10),
                idle_connection_timeout: Duration::from_secs(60),
                keep_alive_interval: Duration::from_secs(5),
            };

            // Debug: print node configuration
            eprintln!(
                "Node {} listening on {} will dial peers:",
                node_id, listen_port
            );
            for peer in &peers {
                eprintln!("  -> {}", peer.multiaddr);
            }

            // Create the network instance with the pre-generated stable keypair
            let (inner, handle) =
                StorageNetworkFactory::create_with_keypair(config, keypairs[i].clone()).await?;

            // Spawn the event loop in its own thread with LocalSet
            // This allows libp2p's !Send Swarm to run while we control it via channels
            let node_id_for_thread = node_id.clone();
            let event_loop_thread = std::thread::spawn(move || {
                // Create a new tokio runtime for this thread
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create runtime");

                // Run the event loop in a LocalSet
                let local = tokio::task::LocalSet::new();
                runtime.block_on(local.run_until(async move {
                    if let Err(e) = inner.run().await {
                        eprintln!("Event loop error for {}: {}", node_id_for_thread, e);
                    }
                }));
            });

            nodes.push(NodeInstance {
                handle,
                node_id: node_id.clone(),
                listen_port: *listen_port,
                _event_loop_thread: event_loop_thread,
            });
        }

        // Give all event loops time to start and begin listening on their ports
        // This prevents "NoAddresses" errors when dialing peers that haven't started yet
        tokio::time::sleep(Duration::from_millis(500)).await;

        // After all nodes are created and listening, initiate peer dialing with jitter
        // Each node will apply random jitter and send dial commands to its event loop
        for (i, node) in nodes.iter().enumerate() {
            if let Err(e) = node.handle.dial_configured_peers().await {
                eprintln!("Node {} failed to dial peers: {}", i, e);
            }
        }

        Ok(Self {
            nodes,
            _temp_dirs: node_configs.into_iter().map(|(_, _, _, td)| td).collect(),
        })
    }

    /// Wait for all nodes to achieve full mesh connectivity.
    ///
    /// Returns an error if connectivity is not achieved within the timeout.
    async fn wait_for_connectivity(
        &self,
        timeout_duration: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected_peers = self.nodes.len() - 1;

        timeout(timeout_duration, async {
            loop {
                let mut all_connected = true;

                for node in &self.nodes {
                    let connected_peers = node.handle.get_connected_peers().await;
                    if connected_peers.len() < expected_peers {
                        all_connected = false;
                        break;
                    }
                }

                if all_connected {
                    return Ok(());
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await?
    }

    /// Gracefully shutdown all nodes in the cluster.
    async fn stop(self) -> Result<(), Box<dyn std::error::Error>> {
        for node in &self.nodes {
            node.handle.shutdown().await?;
        }

        // Give event loops time to complete shutdown
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(())
    }

    /// Get a reference to a node by index.
    fn node(&self, index: usize) -> &NodeInstance {
        &self.nodes[index]
    }

    /// Get the number of nodes in the cluster.
    fn len(&self) -> usize {
        self.nodes.len()
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        // Cleanup happens when temp_dirs are dropped
    }
}

/// Wait for a condition to become true, polling at regular intervals.
async fn wait_for_condition<F, Fut>(
    timeout_duration: Duration,
    mut condition: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    timeout(timeout_duration, async {
        loop {
            if condition().await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await?
}

/// Collect all messages from a receiver with a timeout.
async fn collect_all_messages<T>(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<T>,
    timeout_duration: Duration,
) -> Vec<T> {
    let mut messages = Vec::new();
    let deadline = tokio::time::Instant::now() + timeout_duration;

    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(msg)) => messages.push(msg),
            Ok(None) => break, // Channel closed
            Err(_) => break,   // Timeout
        }
    }

    messages
}

// ============================================================================
// PHASE 2: Multi-Node Network Formation Tests
// ============================================================================

#[tokio::test]
async fn test_two_node_cluster_formation() {
    let cluster = TestCluster::new(2).await.expect("Failed to create cluster");

    // Wait for nodes to connect
    cluster
        .wait_for_connectivity(Duration::from_secs(10))
        .await
        .expect("Nodes failed to connect");

    // Verify both nodes see each other
    let node0_peers = cluster.node(0).handle.get_connected_peers().await;
    let node1_peers = cluster.node(1).handle.get_connected_peers().await;

    assert_eq!(node0_peers.len(), 1, "Node 0 should be connected to 1 peer");
    assert_eq!(node1_peers.len(), 1, "Node 1 should be connected to 1 peer");

    // Verify connection state
    assert_eq!(node0_peers[0].state, ConnectionState::Connected);
    assert_eq!(node1_peers[0].state, ConnectionState::Connected);

    cluster.stop().await.expect("Failed to stop cluster");
}

#[tokio::test]
async fn test_three_node_full_mesh() {
    // Initialize logging to see what's happening
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    let cluster = TestCluster::new(3).await.expect("Failed to create cluster");

    // Give more time for 3-node mesh to form (libp2p can take longer with more nodes)
    // Also print diagnostic info during waiting
    let start = std::time::Instant::now();
    let timeout_duration = Duration::from_secs(30);

    loop {
        if start.elapsed() > timeout_duration {
            // Print final state before failing
            for i in 0..3 {
                let peers = cluster.node(i).handle.get_connected_peers().await;
                println!("Node {} has {} connected peers", i, peers.len());
            }
            panic!("Nodes failed to form mesh after {:?}", timeout_duration);
        }

        let mut all_connected = true;
        for i in 0..3 {
            let peers = cluster.node(i).handle.get_connected_peers().await;
            if peers.len() < 2 {
                all_connected = false;
                break;
            }
        }

        if all_connected {
            println!("All nodes connected after {:?}", start.elapsed());
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Each node should be connected to 2 peers
    for i in 0..3 {
        let peers = cluster.node(i).handle.get_connected_peers().await;
        assert_eq!(peers.len(), 2, "Node {} should be connected to 2 peers", i);

        // All should be in Connected state
        for peer in peers {
            assert_eq!(peer.state, ConnectionState::Connected);
        }
    }

    cluster.stop().await.expect("Failed to stop cluster");
}

// ============================================================================
// Placeholder tests - to be implemented
// ============================================================================

#[tokio::test]
#[ignore = "Not yet implemented"]
async fn test_peer_discovery_via_configuration() {
    // TODO: Implement
}

#[tokio::test]
#[ignore = "Not yet implemented"]
async fn test_connection_state_tracking() {
    // TODO: Implement
}

#[tokio::test]
#[ignore = "Not yet implemented"]
async fn test_autoid_mode_first_connection() {
    // TODO: Implement
}

#[tokio::test]
#[ignore = "Not yet implemented"]
async fn test_large_cluster_formation() {
    // TODO: Implement
}

// ============================================================================
// PHASE 3: Gossipsub Message Propagation Tests
// ============================================================================

#[tokio::test]
#[ignore = "Not yet implemented"]
async fn test_basic_broadcast_propagation() {
    // TODO: Implement
}

#[tokio::test]
#[ignore = "Not yet implemented"]
async fn test_topic_subscription_isolation() {
    // TODO: Implement
}

// ... Additional placeholder tests for remaining phases
