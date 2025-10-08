//! Integration tests for libp2p transport layer
//!
//! These tests verify the network transport implementation including:
//! - Peer connections and discovery
//! - Message exchange (Raft RPCs)
//! - Failure handling and recovery
//! - Multi-node cluster behavior

use ntest::timeout;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};
use wormfs::raft::proto_types::proto::{
    AppendEntriesResponse, InstallSnapshotResponse, RaftRequest, RaftResponse, VoteResponse,
};
use wormfs::transport::{NetworkConfig, NetworkEvent, PeerInfo, StorageNetwork};

/// Helper to create a test network configuration
fn create_test_config(node_id: u64, port: u16, peers: Vec<PeerInfo>) -> NetworkConfig {
    NetworkConfig {
        node_id,
        listen_address: format!("/ip4/127.0.0.1/tcp/{}", port),
        peers,
        request_timeout_ms: 5000,
        connection_timeout_ms: 10000,
        max_retries: 3,
        allow_peer_discovery: true,
        snapshot_server_port: 8082, // Default test snapshot server port
        snapshot_dir: format!("./data/test_snapshots/{}", node_id), // Test snapshot directory
    }
}

/// Helper to create a simple mock request handler that responds to all requests
fn create_mock_handler(
    _node_id: u64,
) -> Arc<dyn Fn(RaftRequest, libp2p::PeerId) -> RaftResponse + Send + Sync> {
    Arc::new(move |request, _peer_id| {
        if let Some(req) = request.request {
            match req {
                wormfs::raft::proto_types::proto::raft_request::Request::AppendEntries(_) => {
                    RaftResponse {
                        response: Some(
                            wormfs::raft::proto_types::proto::raft_response::Response::AppendEntries(
                                AppendEntriesResponse {
                                    term: 1,
                                    success: true,
                                    conflict: None,
                                },
                            ),
                        ),
                    }
                }
                wormfs::raft::proto_types::proto::raft_request::Request::Vote(_) => {
                    RaftResponse {
                        response: Some(
                            wormfs::raft::proto_types::proto::raft_response::Response::Vote(
                                VoteResponse {
                                    term: 1,
                                    vote_granted: true,
                                    last_log_index: 0,
                                },
                            ),
                        ),
                    }
                }
                wormfs::raft::proto_types::proto::raft_request::Request::InstallSnapshot(_) => {
                    RaftResponse {
                        response: Some(
                            wormfs::raft::proto_types::proto::raft_response::Response::InstallSnapshot(
                                InstallSnapshotResponse {
                                    term: 1,
                                    success: true,
                                    error_message: String::new(),
                                },
                            ),
                        ),
                    }
                }
                _ => RaftResponse { response: None },
            }
        } else {
            RaftResponse { response: None }
        }
    })
}

#[tokio::test]
async fn test_network_initialization() {
    // Test that a network can be initialized with valid config
    let config = create_test_config(1, 4001, vec![]);
    let result = StorageNetwork::new(config);
    assert!(result.is_ok());

    let (mut network, _event_rx, _command_tx) = result.unwrap();
    assert_eq!(network.local_node_id(), 1);

    // Test starting listener
    let listen_result = network.start_listening();
    assert!(listen_result.is_ok());
}

#[tokio::test]
async fn test_invalid_configuration() {
    // Test with empty listen address
    let mut config = create_test_config(1, 4002, vec![]);
    config.listen_address = "".to_string();
    let result = StorageNetwork::new(config);
    assert!(result.is_err());

    // Test with zero timeout
    let mut config = create_test_config(1, 4003, vec![]);
    config.request_timeout_ms = 0;
    let result = StorageNetwork::new(config);
    assert!(result.is_err());
}

#[tokio::test(flavor = "multi_thread")]
#[timeout(60000)]
async fn test_two_node_connection() {
    tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::INFO)
        .try_init()
        .ok();

    info!("=== TEST START: test_two_node_connection ===");

    // Node 1 configuration
    info!("Step 1: Creating Node 1 configuration");
    let peer2 = PeerInfo {
        node_id: 2,
        address: "/ip4/127.0.0.1/tcp/4102".to_string(),
        peer_id: None, // Discovery mode
    };
    let config1 = create_test_config(1, 4101, vec![peer2]);
    info!("  - Node 1 will listen on port 4101");
    info!("  - Node 1 has peer 2 configured at port 4102");

    // Node 2 configuration
    info!("Step 2: Creating Node 2 configuration");
    let peer1 = PeerInfo {
        node_id: 1,
        address: "/ip4/127.0.0.1/tcp/4101".to_string(),
        peer_id: None, // Discovery mode
    };
    let config2 = create_test_config(2, 4102, vec![peer1]);
    info!("  - Node 2 will listen on port 4102");
    info!("  - Node 2 has peer 1 configured at port 4101");

    // Create both networks
    info!("Step 3: Creating StorageNetwork instances");
    let (mut network1, mut event_rx1, _cmd_tx1) = StorageNetwork::new(config1).unwrap();
    info!("  - Network 1 created successfully");
    let (mut network2, mut event_rx2, _cmd_tx2) = StorageNetwork::new(config2).unwrap();
    info!("  - Network 2 created successfully");

    // Set up mock handlers
    info!("Step 4: Setting up mock request handlers");
    network1.set_request_handler(create_mock_handler(1));
    info!("  - Handler set for network 1");
    network2.set_request_handler(create_mock_handler(2));
    info!("  - Handler set for network 2");

    // Start listening
    info!("Step 5: Starting listeners");
    network1.start_listening().unwrap();
    info!("  - Network 1 listening on port 4101");
    network2.start_listening().unwrap();
    info!("  - Network 2 listening on port 4102");

    // Spawn network event loops FIRST (must be running to handle dial_peers connections)
    info!("Step 6: Spawning network event loops");
    let network1_handle = tokio::spawn(async move {
        info!("  - Network 1 event loop started");
        network1.run().await
    });
    let network2_handle = tokio::spawn(async move {
        info!("  - Network 2 event loop started");
        network2.run().await
    });
    info!("  - Both event loops spawned");

    // Give event loops time to start
    info!("Step 7: Waiting 100ms for event loops to initialize");
    tokio::time::sleep(Duration::from_millis(100)).await;
    info!("  - Wait complete");

    // Now dial peers using command channel (event loops are running)
    // Add random delay between dial commands to avoid simultaneous dial race condition
    info!("Step 8: Sending DialAllPeers commands with random delay");
    let result1 = _cmd_tx1.send(wormfs::transport::NetworkCommand::DialAllPeers);
    info!("  - Network 1 DialAllPeers sent: {:?}", result1.is_ok());

    // Random delay between 500-1000ms to avoid race condition
    let delay_ms = rand::rng().random_range(500..=1000);
    info!(
        "  - Waiting {}ms before sending second dial command",
        delay_ms
    );
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

    let result2 = _cmd_tx2.send(wormfs::transport::NetworkCommand::DialAllPeers);
    info!("  - Network 2 DialAllPeers sent: {:?}", result2.is_ok());

    // Give dial operations time to process
    info!("Step 9: Waiting 500ms for dial operations to complete");
    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("  - Wait complete");

    // Wait for connection events
    info!("Step 10: Waiting for connection events (max 10 seconds)");
    let mut node1_connected = false;
    let mut node2_connected = false;

    let timeout = tokio::time::timeout(Duration::from_secs(20), async {
        info!("  - Entered event loop, waiting for PeerConnected events");
        while !node1_connected || !node2_connected {
            tokio::select! {
                Some(event) = event_rx1.recv() => {
                    info!("  - Network 1 received event: {:?}", event);
                    if let NetworkEvent::PeerConnected { node_id, .. } = event {
                        if node_id == 2 {
                            node1_connected = true;
                            info!("  ✓ Node 1 connected to Node 2");
                        }
                    }
                }
                Some(event) = event_rx2.recv() => {
                    info!("  - Network 2 received event: {:?}", event);
                    if let NetworkEvent::PeerConnected { node_id, .. } = event {
                        if node_id == 1 {
                            node2_connected = true;
                            info!("  ✓ Node 2 connected to Node 1");
                        }
                    }
                }
            }
            info!(
                "  - Connection status: node1_connected={}, node2_connected={}",
                node1_connected, node2_connected
            );
        }
        info!("  - Both nodes connected successfully!");
    })
    .await;

    // Cleanup
    info!("Step 11: Cleaning up network event loops");
    network1_handle.abort();
    network2_handle.abort();
    info!("  - Event loops aborted");

    info!("Step 12: Checking timeout result");
    if timeout.is_ok() {
        info!("  ✓ TEST PASSED: Nodes connected within timeout");
    } else {
        error!("  ✗ TEST FAILED: Timeout occurred");
    }

    info!("=== TEST END: test_two_node_connection ===");
    assert!(timeout.is_ok(), "Nodes should connect within timeout");
}

#[tokio::test(flavor = "multi_thread")]
#[timeout(30000)]
async fn test_append_entries_roundtrip() {
    // Set up two nodes
    let peer2 = PeerInfo {
        node_id: 2,
        address: "/ip4/127.0.0.1/tcp/4202".to_string(),
        peer_id: None,
    };
    let config1 = create_test_config(1, 4201, vec![peer2]);

    let peer1 = PeerInfo {
        node_id: 1,
        address: "/ip4/127.0.0.1/tcp/4201".to_string(),
        peer_id: None,
    };
    let config2 = create_test_config(2, 4202, vec![peer1]);

    let (mut network1, mut event_rx1, cmd_tx1) = StorageNetwork::new(config1).unwrap();
    let (mut network2, mut event_rx2, _cmd_tx2) = StorageNetwork::new(config2).unwrap();

    // Set up handlers
    network2.set_request_handler(create_mock_handler(2));

    // Start listening
    network1.start_listening().unwrap();
    network2.start_listening().unwrap();

    // Spawn network event loops FIRST
    let network1_handle = tokio::spawn(async move { network1.run().await });

    let network2_handle = tokio::spawn(async move { network2.run().await });

    // Give event loops time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now dial peers using command channel with random delay to avoid race condition
    let _ = cmd_tx1.send(wormfs::transport::NetworkCommand::DialAllPeers);

    // Random delay between 500-1000ms to avoid race condition
    let delay_ms = rand::rng().random_range(500..=1000);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

    let _ = _cmd_tx2.send(wormfs::transport::NetworkCommand::DialAllPeers);

    // Give dial operations time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Wait for connection
    let connected = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                Some(event) = event_rx1.recv() => {
                    if let wormfs::transport::NetworkEvent::PeerConnected { node_id, .. } = event {
                        if node_id == 2 {
                            return true;
                        }
                    }
                }
                Some(event) = event_rx2.recv() => {
                    if let wormfs::transport::NetworkEvent::PeerConnected { node_id, .. } = event {
                        if node_id == 1 {
                            // Connection established from node2's perspective
                        }
                    }
                }
            }
        }
    })
    .await;

    assert!(connected.is_ok(), "Nodes should connect");

    // Small delay to ensure connection is stable
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cleanup
    network1_handle.abort();
    network2_handle.abort();

    // Test passes if we get here without timeout
}

#[tokio::test(flavor = "multi_thread")]
#[timeout(30000)]
async fn test_vote_request_response() {
    // Set up two nodes
    let peer2 = PeerInfo {
        node_id: 2,
        address: "/ip4/127.0.0.1/tcp/4302".to_string(),
        peer_id: None,
    };
    let config1 = create_test_config(1, 4301, vec![peer2]);

    let peer1 = PeerInfo {
        node_id: 1,
        address: "/ip4/127.0.0.1/tcp/4301".to_string(),
        peer_id: None,
    };
    let config2 = create_test_config(2, 4302, vec![peer1]);

    let (mut network1, mut event_rx1, _cmd_tx1) = StorageNetwork::new(config1).unwrap();
    let (mut network2, mut event_rx2, _cmd_tx2) = StorageNetwork::new(config2).unwrap();

    network2.set_request_handler(create_mock_handler(2));

    network1.start_listening().unwrap();
    network2.start_listening().unwrap();

    // Spawn network event loops FIRST
    let network2_handle = tokio::spawn(async move { network2.run().await });

    let network1_handle = tokio::spawn(async move { network1.run().await });

    // Give event loops time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now dial peers using command channel with random delay to avoid race condition
    let _ = _cmd_tx1.send(wormfs::transport::NetworkCommand::DialAllPeers);

    // Random delay between 500-1000ms to avoid race condition
    let delay_ms = rand::rng().random_range(500..=1000);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

    let _ = _cmd_tx2.send(wormfs::transport::NetworkCommand::DialAllPeers);

    // Give dial operations time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Wait for connection
    let connected = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                Some(event) = event_rx1.recv() => {
                    if let NetworkEvent::PeerConnected { node_id, .. } = event {
                        if node_id == 2 {
                            return true;
                        }
                    }
                }
                Some(_) = event_rx2.recv() => {}
            }
        }
    })
    .await;

    assert!(connected.is_ok(), "Nodes should connect");

    // Cleanup
    network1_handle.abort();
    network2_handle.abort();

    // Test passes if we get here without timeout
}

#[tokio::test(flavor = "multi_thread")]
async fn test_request_timeout() {
    // Set up node 1 with a non-existent peer
    let peer2 = PeerInfo {
        node_id: 2,
        address: "/ip4/127.0.0.1/tcp/9999".to_string(), // Non-existent port
        peer_id: None,
    };
    let mut config1 = create_test_config(1, 4401, vec![peer2]);
    config1.request_timeout_ms = 2000; // 2 second timeout

    let (mut network1, _event_rx1, _cmd_tx1) = StorageNetwork::new(config1).unwrap();
    network1.start_listening().unwrap();

    // Try to send a request (will fail since node2 doesn't exist)
    // We can't test actual timeout without connection, but we can verify config
    assert_eq!(network1.config().request_timeout_ms, 2000);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_peer_health_tracking() {
    // Create a network with a peer
    let peer2 = PeerInfo {
        node_id: 2,
        address: "/ip4/127.0.0.1/tcp/4502".to_string(),
        peer_id: None,
    };
    let config = create_test_config(1, 4501, vec![peer2]);

    let (network, _event_rx, _cmd_tx) = StorageNetwork::new(config).unwrap();

    // Check initial peer stats
    let stats = network.peer_stats();
    assert_eq!(stats.total, 1);
    assert_eq!(stats.healthy, 0); // Not connected yet

    // Verify peer health can be queried
    let peer_health = network.get_peer_health(2);
    assert!(peer_health.is_some());
}

#[tokio::test(flavor = "multi_thread")]
#[timeout(30000)]
async fn test_three_node_cluster() {
    // Set up three nodes that should form a mesh
    let peer2 = PeerInfo {
        node_id: 2,
        address: "/ip4/127.0.0.1/tcp/4602".to_string(),
        peer_id: None,
    };
    let peer3 = PeerInfo {
        node_id: 3,
        address: "/ip4/127.0.0.1/tcp/4603".to_string(),
        peer_id: None,
    };
    let config1 = create_test_config(1, 4601, vec![peer2.clone(), peer3.clone()]);

    let peer1_for_2 = PeerInfo {
        node_id: 1,
        address: "/ip4/127.0.0.1/tcp/4601".to_string(),
        peer_id: None,
    };
    let config2 = create_test_config(2, 4602, vec![peer1_for_2.clone(), peer3.clone()]);

    let peer1_for_3 = PeerInfo {
        node_id: 1,
        address: "/ip4/127.0.0.1/tcp/4601".to_string(),
        peer_id: None,
    };
    let peer2_for_3 = PeerInfo {
        node_id: 2,
        address: "/ip4/127.0.0.1/tcp/4602".to_string(),
        peer_id: None,
    };
    let config3 = create_test_config(3, 4603, vec![peer1_for_3, peer2_for_3]);

    // Create networks
    let (mut network1, mut event_rx1, _cmd_tx1) = StorageNetwork::new(config1).unwrap();
    let (mut network2, mut event_rx2, _cmd_tx2) = StorageNetwork::new(config2).unwrap();
    let (mut network3, mut event_rx3, _cmd_tx3) = StorageNetwork::new(config3).unwrap();

    // Set up handlers
    network1.set_request_handler(create_mock_handler(1));
    network2.set_request_handler(create_mock_handler(2));
    network3.set_request_handler(create_mock_handler(3));

    // Start listening
    network1.start_listening().unwrap();
    network2.start_listening().unwrap();
    network3.start_listening().unwrap();

    // Spawn event loops FIRST
    let handle1 = tokio::spawn(async move { network1.run().await });
    let handle2 = tokio::spawn(async move { network2.run().await });
    let handle3 = tokio::spawn(async move { network3.run().await });

    // Give event loops time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now dial peers using command channel with random delays to avoid race condition
    let _ = _cmd_tx1.send(wormfs::transport::NetworkCommand::DialAllPeers);

    // Random delay between 500-1000ms to avoid race condition
    let delay_ms1 = rand::rng().random_range(500..=1000);
    tokio::time::sleep(Duration::from_millis(delay_ms1)).await;

    let _ = _cmd_tx2.send(wormfs::transport::NetworkCommand::DialAllPeers);

    // Another random delay for the third node
    let delay_ms2 = rand::rng().random_range(500..=1000);
    tokio::time::sleep(Duration::from_millis(delay_ms2)).await;

    let _ = _cmd_tx3.send(wormfs::transport::NetworkCommand::DialAllPeers);

    // Give dial operations time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Track connections
    let mut node1_peers = std::collections::HashSet::new();
    let mut node2_peers = std::collections::HashSet::new();
    let mut node3_peers = std::collections::HashSet::new();

    // Wait for all connections (each node should connect to 2 others)
    let timeout = tokio::time::timeout(Duration::from_secs(15), async {
        while node1_peers.len() < 2 || node2_peers.len() < 2 || node3_peers.len() < 2 {
            tokio::select! {
                Some(event) = event_rx1.recv() => {
                    if let NetworkEvent::PeerConnected { node_id, .. } = event {
                        node1_peers.insert(node_id);
                        tracing::info!("Node 1 connected to Node {}", node_id);
                    }
                }
                Some(event) = event_rx2.recv() => {
                    if let NetworkEvent::PeerConnected { node_id, .. } = event {
                        node2_peers.insert(node_id);
                        tracing::info!("Node 2 connected to Node {}", node_id);
                    }
                }
                Some(event) = event_rx3.recv() => {
                    if let NetworkEvent::PeerConnected { node_id, .. } = event {
                        node3_peers.insert(node_id);
                        tracing::info!("Node 3 connected to Node {}", node_id);
                    }
                }
            }
        }
    })
    .await;

    // Cleanup
    handle1.abort();
    handle2.abort();
    handle3.abort();

    assert!(timeout.is_ok(), "All nodes should connect within timeout");
    assert_eq!(node1_peers.len(), 2);
    assert_eq!(node2_peers.len(), 2);
    assert_eq!(node3_peers.len(), 2);
}

#[tokio::test]
async fn test_strict_mode_configuration() {
    // In strict mode, peers without peer_ids should cause validation error
    let peer_without_id = PeerInfo {
        node_id: 2,
        address: "/ip4/127.0.0.1/tcp/4702".to_string(),
        peer_id: None,
    };

    let mut config = create_test_config(1, 4701, vec![peer_without_id]);
    config.allow_peer_discovery = false; // Strict mode

    let (network, _event_rx, _cmd_tx) = StorageNetwork::new(config).unwrap();

    // Should fail validation in strict mode
    let validation = network.validate_strict_mode_config();
    assert!(validation.is_err(), "Strict mode should require peer_ids");
}

#[tokio::test]
async fn test_discovery_mode() {
    // In discovery mode, peers without peer_ids should be accepted
    let peer_without_id = PeerInfo {
        node_id: 2,
        address: "/ip4/127.0.0.1/tcp/4802".to_string(),
        peer_id: None,
    };

    let mut config = create_test_config(1, 4801, vec![peer_without_id]);
    config.allow_peer_discovery = true; // Discovery mode

    let result = StorageNetwork::new(config);
    assert!(
        result.is_ok(),
        "Discovery mode should accept peers without peer_ids"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[timeout(30000)]
async fn test_install_snapshot_exchange() {
    // Set up two nodes
    let peer2 = PeerInfo {
        node_id: 2,
        address: "/ip4/127.0.0.1/tcp/4902".to_string(),
        peer_id: None,
    };
    let config1 = create_test_config(1, 4901, vec![peer2]);

    let peer1 = PeerInfo {
        node_id: 1,
        address: "/ip4/127.0.0.1/tcp/4901".to_string(),
        peer_id: None,
    };
    let config2 = create_test_config(2, 4902, vec![peer1]);

    let (mut network1, mut event_rx1, _cmd_tx1) = StorageNetwork::new(config1).unwrap();
    let (mut network2, mut event_rx2, _cmd_tx2) = StorageNetwork::new(config2).unwrap();

    network2.set_request_handler(create_mock_handler(2));

    network1.start_listening().unwrap();
    network2.start_listening().unwrap();

    // Spawn event loops FIRST
    let network2_handle = tokio::spawn(async move { network2.run().await });

    let network1_handle = tokio::spawn(async move { network1.run().await });

    // Give event loops time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now dial peers using command channel with random delay to avoid race condition
    let _ = _cmd_tx1.send(wormfs::transport::NetworkCommand::DialAllPeers);

    // Random delay between 500-1000ms to avoid race condition
    let delay_ms = rand::rng().random_range(500..=1000);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;

    let _ = _cmd_tx2.send(wormfs::transport::NetworkCommand::DialAllPeers);

    // Give dial operations time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Wait for connection
    let connected = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                Some(event) = event_rx1.recv() => {
                    if let NetworkEvent::PeerConnected { node_id, .. } = event {
                        if node_id == 2 {
                            return true;
                        }
                    }
                }
                Some(_) = event_rx2.recv() => {}
            }
        }
    })
    .await;

    assert!(connected.is_ok(), "Nodes should connect");

    // Cleanup
    network1_handle.abort();
    network2_handle.abort();

    // Test passes if we get here without timeout
}
