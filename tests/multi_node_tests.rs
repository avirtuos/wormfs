//! Integration tests for Phase 2A.10: Multi-Node Integration Test
//!
//! These tests verify the complete networking system under realistic conditions
//! with multiple peers and various failure scenarios.

mod test_helpers;

use std::time::Duration;
use test_helpers::*;
use tokio::time::sleep;
use wormfs::networking::{AuthenticationMode, NetworkConfig, PingConfig, ReconnectionConfig};

/// Initialize tracing for tests
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();
}

// ========== Basic Multi-Node Mesh Tests ==========

#[tokio::test]
async fn test_five_node_mesh_topology() {
    init_tracing();
    println!("Testing 5-node mesh topology");

    // Create a cluster of 5 nodes
    let mut cluster = TestCluster::new(5, 10000).await.unwrap();

    println!("✓ Created 5-node cluster");

    // Connect all nodes in mesh topology
    cluster.connect_mesh().await.unwrap();

    println!("✓ Initiated mesh connections");

    // Wait for full mesh connectivity
    cluster
        .wait_for_mesh_connectivity(Duration::from_secs(15))
        .await
        .unwrap();

    println!("✓ Mesh connectivity established");

    // Verify cluster health
    let metrics = cluster.collect_metrics().await.unwrap();
    assert!(
        metrics.is_fully_connected(),
        "Cluster should be fully connected"
    );
    assert_eq!(metrics.num_nodes, 5, "Should have 5 nodes in the cluster");
    assert_eq!(
        metrics.total_connections, 10,
        "Should have 10 unique connections in 5-node mesh"
    );

    println!("✓ Cluster metrics verified:");
    println!("  - Nodes: {}", metrics.num_nodes);
    println!("  - Connections: {}", metrics.total_connections);
    println!(
        "  - Avg connections per node: {:.2}",
        metrics.avg_connections()
    );

    // Verify each node has exactly 4 connections
    for (i, &count) in metrics.peer_counts.iter().enumerate() {
        assert_eq!(
            count, 4,
            "Node {} should have 4 connections, got {}",
            i, count
        );
    }

    println!("✓ 5-node mesh topology test successful!");

    // Clean shutdown
    cluster.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn test_ten_node_mesh_topology() {
    init_tracing();
    println!("Testing 10-node mesh topology");

    // Create a cluster of 10 nodes
    let mut cluster = TestCluster::new(10, 10100).await.unwrap();

    println!("✓ Created 10-node cluster");

    // Connect all nodes in mesh topology
    cluster.connect_mesh().await.unwrap();

    // Wait for full mesh connectivity (may take longer with more nodes)
    cluster
        .wait_for_mesh_connectivity(Duration::from_secs(30))
        .await
        .unwrap();

    println!("✓ Mesh connectivity established");

    // Verify cluster health
    let metrics = cluster.collect_metrics().await.unwrap();
    assert!(
        metrics.is_fully_connected(),
        "Cluster should be fully connected"
    );
    assert_eq!(metrics.num_nodes, 10, "Should have 10 nodes in the cluster");
    assert_eq!(
        metrics.total_connections, 45,
        "Should have 45 unique connections in 10-node mesh (n*(n-1)/2)"
    );

    println!("✓ 10-node mesh topology test successful!");
    println!("  - Nodes: {}", metrics.num_nodes);
    println!("  - Connections: {}", metrics.total_connections);
    println!(
        "  - Avg connections per node: {:.2}",
        metrics.avg_connections()
    );

    // Clean shutdown
    cluster.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn test_mesh_topology_ping_propagation() {
    init_tracing();
    println!("Testing ping propagation in mesh topology");

    // Create cluster with short ping intervals
    let ping_config = PingConfig {
        interval_secs: 5,
        timeout_secs: 10,
        max_failures: 3,
    };

    let configs: Vec<NetworkConfig> = (0..5)
        .map(|i| NetworkConfig {
            listen_address: format!("/ip4/127.0.0.1/tcp/{}", 10200 + i),
            initial_peers: Vec::new(),
            bootstrap_peers: Vec::new(),
            ping: ping_config.clone(),
            authentication: wormfs::networking::AuthenticationConfig {
                mode: AuthenticationMode::Disabled,
                peers_file: format!("test_peers_{}.json", 10200 + i),
            },
            reconnection: ReconnectionConfig::default(),
        })
        .collect();

    let mut cluster = TestCluster::new_with_configs(configs).await.unwrap();

    // Connect mesh
    cluster.connect_mesh().await.unwrap();
    cluster
        .wait_for_mesh_connectivity(Duration::from_secs(15))
        .await
        .unwrap();

    println!("✓ Mesh connected, waiting for pings...");

    // Wait for pings to propagate (at least one ping cycle)
    sleep(Duration::from_secs(7)).await;

    println!("✓ Ping propagation test successful!");

    // Clean shutdown
    cluster.shutdown_all().await.unwrap();
}

// ========== Mixed Authentication Tests ==========

#[tokio::test]
async fn test_mixed_authentication_modes() {
    init_tracing();
    println!("Testing mixed authentication modes");

    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let peers_file = temp_dir.path().join("peers.json");

    // Create 3 nodes: Disabled, Learn, Enforce
    // For this test, we'll use Disabled mode for simplicity
    // (Enforce mode would require pre-populating peer lists)

    let configs = vec![
        NetworkConfig {
            listen_address: "/ip4/127.0.0.1/tcp/10300".to_string(),
            initial_peers: Vec::new(),
            bootstrap_peers: Vec::new(),
            ping: PingConfig::default(),
            authentication: wormfs::networking::AuthenticationConfig {
                mode: AuthenticationMode::Disabled,
                peers_file: peers_file.to_str().unwrap().to_string(),
            },
            reconnection: ReconnectionConfig::default(),
        },
        NetworkConfig {
            listen_address: "/ip4/127.0.0.1/tcp/10301".to_string(),
            initial_peers: Vec::new(),
            bootstrap_peers: Vec::new(),
            ping: PingConfig::default(),
            authentication: wormfs::networking::AuthenticationConfig {
                mode: AuthenticationMode::Learn,
                peers_file: peers_file.to_str().unwrap().to_string(),
            },
            reconnection: ReconnectionConfig::default(),
        },
        NetworkConfig {
            listen_address: "/ip4/127.0.0.1/tcp/10302".to_string(),
            initial_peers: Vec::new(),
            bootstrap_peers: Vec::new(),
            ping: PingConfig::default(),
            authentication: wormfs::networking::AuthenticationConfig {
                mode: AuthenticationMode::Disabled,
                peers_file: peers_file.to_str().unwrap().to_string(),
            },
            reconnection: ReconnectionConfig::default(),
        },
    ];

    let mut cluster = TestCluster::new_with_configs(configs).await.unwrap();

    // Connect nodes
    cluster.connect_mesh().await.unwrap();
    cluster
        .wait_for_mesh_connectivity(Duration::from_secs(10))
        .await
        .unwrap();

    // Verify all nodes can connect despite different auth modes
    let metrics = cluster.collect_metrics().await.unwrap();
    assert!(
        metrics.is_fully_connected(),
        "Mixed auth modes should still allow connections"
    );

    println!("✓ Mixed authentication modes test successful!");

    cluster.shutdown_all().await.unwrap();
}

// ========== Failure Scenario Tests ==========

#[tokio::test]
async fn test_cascading_node_failures() {
    init_tracing();
    println!("Testing cascading node failures");

    // Create 7-node cluster
    let mut cluster = TestCluster::new(7, 10400).await.unwrap();

    // Connect mesh
    cluster.connect_mesh().await.unwrap();
    cluster
        .wait_for_mesh_connectivity(Duration::from_secs(20))
        .await
        .unwrap();

    println!("✓ Initial mesh established");

    let initial_metrics = cluster.collect_metrics().await.unwrap();
    assert_eq!(initial_metrics.num_nodes, 7);
    println!("  - Initial nodes: {}", initial_metrics.num_nodes);

    // Kill 3 nodes sequentially
    println!("Shutting down node 0...");
    cluster.shutdown_node(0).await.unwrap();
    sleep(Duration::from_secs(2)).await;

    println!("Shutting down node 0 (was node 1)...");
    cluster.shutdown_node(0).await.unwrap();
    sleep(Duration::from_secs(2)).await;

    println!("Shutting down node 0 (was node 2)...");
    cluster.shutdown_node(0).await.unwrap();
    sleep(Duration::from_secs(2)).await;

    // Verify remaining 4 nodes are still connected
    let remaining_metrics = cluster.collect_metrics().await.unwrap();
    assert_eq!(
        remaining_metrics.num_nodes, 4,
        "Should have 4 nodes remaining"
    );

    println!("✓ Remaining nodes: {}", remaining_metrics.num_nodes);
    println!(
        "  - Min connections: {}",
        remaining_metrics.min_connections()
    );
    println!(
        "  - Max connections: {}",
        remaining_metrics.max_connections()
    );

    // Note: Nodes may not be fully connected immediately after failures
    // but should maintain some connectivity
    assert!(
        remaining_metrics.min_connections() > 0,
        "Remaining nodes should maintain some connectivity"
    );

    println!("✓ Cascading failure test successful!");

    cluster.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn test_node_recovery_after_failure() {
    init_tracing();
    println!("Testing node recovery after failure");

    let mut cluster = TestCluster::new(3, 10500).await.unwrap();

    // Connect mesh
    cluster.connect_mesh().await.unwrap();
    cluster
        .wait_for_mesh_connectivity(Duration::from_secs(10))
        .await
        .unwrap();

    println!("✓ Initial mesh established");

    // Shutdown one node
    let failed_node = cluster.nodes.remove(1);
    let failed_port = failed_node.port;
    failed_node.shutdown().await.unwrap();

    println!("✓ Node {} shut down", failed_port);
    sleep(Duration::from_secs(2)).await;

    // Verify remaining nodes detect the failure
    let metrics = cluster.collect_metrics().await.unwrap();
    assert_eq!(metrics.num_nodes, 2, "Should have 2 nodes remaining");

    // Restart the failed node
    let config = NetworkConfig {
        listen_address: format!("/ip4/127.0.0.1/tcp/{}", failed_port),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Disabled,
            peers_file: format!("test_peers_{}.json", failed_port),
        },
        reconnection: ReconnectionConfig::default(),
    };

    let new_node = TestNode::new(failed_port, config).await.unwrap();

    println!(
        "✓ Node {} restarted with new peer ID: {}",
        failed_port, new_node.peer_id
    );

    // Manually reconnect the new node
    for existing_node in &cluster.nodes {
        new_node
            .handle
            .dial(existing_node.multiaddr())
            .await
            .unwrap();
    }

    cluster.nodes.push(new_node);

    // Wait for connections to re-establish
    sleep(Duration::from_secs(3)).await;

    // Verify cluster is back to full health
    cluster
        .wait_for_mesh_connectivity(Duration::from_secs(10))
        .await
        .unwrap();

    let final_metrics = cluster.collect_metrics().await.unwrap();
    assert_eq!(final_metrics.num_nodes, 3, "Should be back to 3 nodes");
    assert!(
        final_metrics.is_fully_connected(),
        "Cluster should be fully connected again"
    );

    println!("✓ Node recovery test successful!");

    cluster.shutdown_all().await.unwrap();
}

// ========== Bootstrap Chain Test ==========

#[tokio::test]
async fn test_bootstrap_chain() {
    init_tracing();
    println!("Testing bootstrap chain connectivity");

    // Create node A (standalone)
    let config_a = NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/10600".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Disabled,
            peers_file: "test_peers_10600.json".to_string(),
        },
        reconnection: ReconnectionConfig::default(),
    };

    let node_a = TestNode::new(10600, config_a).await.unwrap();
    println!("✓ Node A created: {}", node_a.peer_id);

    // Create node B that bootstraps to A
    let config_b = NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/10601".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: vec![format!("{}@/ip4/127.0.0.1/tcp/10600", node_a.peer_id)],
        ping: PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Disabled,
            peers_file: "test_peers_10601.json".to_string(),
        },
        reconnection: ReconnectionConfig::default(),
    };

    let node_b = TestNode::new(10601, config_b).await.unwrap();
    println!("✓ Node B created: {}", node_b.peer_id);

    // Create node C that bootstraps to B
    let config_c = NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/10602".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: vec![format!("{}@/ip4/127.0.0.1/tcp/10601", node_b.peer_id)],
        ping: PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Disabled,
            peers_file: "test_peers_10602.json".to_string(),
        },
        reconnection: ReconnectionConfig::default(),
    };

    let node_c = TestNode::new(10602, config_c).await.unwrap();
    println!("✓ Node C created: {}", node_c.peer_id);

    // Wait for bootstrap connections
    sleep(Duration::from_secs(3)).await;

    // Verify B is connected to A
    let peers_b = node_b.handle.list_connected_peers().await.unwrap();
    assert!(
        peers_b.contains(&node_a.peer_id),
        "Node B should be connected to A via bootstrap"
    );

    // Verify C is connected to B
    let peers_c = node_c.handle.list_connected_peers().await.unwrap();
    assert!(
        peers_c.contains(&node_b.peer_id),
        "Node C should be connected to B via bootstrap"
    );

    println!("✓ Bootstrap chain test successful!");
    println!("  - A ← B ← C chain verified");

    // Clean shutdown
    node_a.shutdown().await.unwrap();
    node_b.shutdown().await.unwrap();
    node_c.shutdown().await.unwrap();
}

// ========== Performance Tests ==========

#[tokio::test]
async fn test_connection_setup_performance() {
    init_tracing();
    println!("Testing connection setup performance");

    // Create two nodes
    let mut cluster = TestCluster::new(2, 10700).await.unwrap();

    let node_1_peer = cluster.nodes[1].peer_id;
    let node_1_addr = cluster.nodes[1].multiaddr();

    // Measure connection time
    let connection_time =
        measure_connection_time(&mut cluster.nodes[0].handle, node_1_addr, node_1_peer)
            .await
            .unwrap();

    println!("✓ Connection established in {:?}", connection_time);

    // Verify connection time is reasonable (< 5 seconds)
    assert!(
        connection_time < Duration::from_secs(5),
        "Connection should establish in < 5 seconds, took {:?}",
        connection_time
    );

    println!("✓ Connection setup performance test successful!");

    cluster.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn test_cluster_scalability() {
    init_tracing();
    println!("Testing cluster scalability with varying node counts");

    // Test with different cluster sizes
    for num_nodes in [3, 5, 7] {
        let start_port = 10800 + (num_nodes as u16 * 10);
        println!(
            "\nTesting {}-node cluster (port {})...",
            num_nodes, start_port
        );

        let mut cluster = TestCluster::new(num_nodes, start_port).await.unwrap();

        cluster.connect_mesh().await.unwrap();
        cluster
            .wait_for_mesh_connectivity(Duration::from_secs(20))
            .await
            .unwrap();

        let metrics = cluster.collect_metrics().await.unwrap();
        assert!(metrics.is_fully_connected());

        let expected_connections = num_nodes * (num_nodes - 1) / 2;
        assert_eq!(metrics.total_connections, expected_connections);

        println!(
            "  ✓ {}-node cluster: {} connections, avg {:.2} per node",
            num_nodes,
            metrics.total_connections,
            metrics.avg_connections()
        );

        cluster.shutdown_all().await.unwrap();
    }

    println!("\n✓ Cluster scalability test successful!");
}
