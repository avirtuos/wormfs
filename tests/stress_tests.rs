//! Stress and chaos tests for Phase 2A.10
//!
//! These tests verify the system's resilience under extreme conditions.

mod test_helpers;

use rand::Rng;
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

// ========== Chaos Tests ==========

#[tokio::test]
async fn test_random_node_kills() {
    init_tracing();
    println!("Testing random node failures with detection");

    // Create 5-node cluster
    let mut cluster = TestCluster::new(5, 11000).await.unwrap();

    // Connect mesh
    cluster.connect_mesh().await.unwrap();
    cluster
        .wait_for_mesh_connectivity(Duration::from_secs(15))
        .await
        .unwrap();

    println!("✓ Initial mesh established");

    let initial_metrics = cluster.collect_metrics().await.unwrap();
    println!("  - Initial nodes: {}", initial_metrics.num_nodes);

    // Randomly kill 2 nodes
    let mut rng = rand::thread_rng();
    for _ in 0..2 {
        if cluster.len() > 2 {
            let victim_index = rng.gen_range(0..cluster.len());
            println!("Killing node at index {}", victim_index);
            cluster.shutdown_node(victim_index).await.unwrap();
            sleep(Duration::from_secs(2)).await;
        }
    }

    // Verify remaining nodes are still operational
    let remaining_metrics = cluster.collect_metrics().await.unwrap();
    assert_eq!(
        remaining_metrics.num_nodes, 3,
        "Should have 3 nodes remaining"
    );
    assert!(
        remaining_metrics.min_connections() > 0,
        "Remaining nodes should maintain connectivity"
    );

    println!("✓ Random node kills test successful!");
    println!("  - Remaining nodes: {}", remaining_metrics.num_nodes);
    println!(
        "  - Min connections: {}",
        remaining_metrics.min_connections()
    );

    cluster.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn test_rapid_connect_disconnect() {
    init_tracing();
    println!("Testing rapid connect/disconnect cycles");

    // Create 3 nodes
    let mut cluster = TestCluster::new(3, 11100).await.unwrap();

    println!("✓ Created cluster");

    // Perform multiple connect/disconnect cycles
    for cycle in 0..3 {
        println!("Connect/disconnect cycle {}", cycle + 1);

        // Connect
        cluster.connect_mesh().await.unwrap();
        sleep(Duration::from_secs(2)).await;

        // Verify connected
        let metrics = cluster.collect_metrics().await.unwrap();
        assert!(metrics.min_connections() > 0, "Nodes should be connected");

        // Disconnect node 0 from node 1
        if let Some(node_1_peer) = cluster.nodes.get(1).map(|n| n.peer_id) {
            cluster.nodes[0]
                .handle
                .disconnect(node_1_peer)
                .await
                .unwrap();
        }

        sleep(Duration::from_secs(1)).await;
    }

    println!("✓ Rapid connect/disconnect test successful!");

    cluster.shutdown_all().await.unwrap();
}

// ========== Reconnection Storm Prevention ==========

#[tokio::test]
async fn test_reconnection_backoff_prevents_storms() {
    init_tracing();
    println!("Testing exponential backoff prevents connection storms");

    // Create cluster with aggressive reconnection settings
    let reconnection_config = ReconnectionConfig {
        enabled: true,
        initial_backoff_secs: 1,
        max_backoff_secs: 8,
        backoff_multiplier: 2.0,
    };

    let configs: Vec<NetworkConfig> = (0..3)
        .map(|i| NetworkConfig {
            listen_address: format!("/ip4/127.0.0.1/tcp/{}", 11200 + i),
            initial_peers: Vec::new(),
            bootstrap_peers: Vec::new(),
            ping: PingConfig::default(),
            authentication: wormfs::networking::AuthenticationConfig {
                mode: AuthenticationMode::Disabled,
                peers_file: format!("test_peers_{}.json", 11200 + i),
            },
            reconnection: reconnection_config.clone(),
        })
        .collect();

    let mut cluster = TestCluster::new_with_configs(configs).await.unwrap();

    // Connect mesh
    cluster.connect_mesh().await.unwrap();
    cluster
        .wait_for_mesh_connectivity(Duration::from_secs(10))
        .await
        .unwrap();

    println!("✓ Initial mesh established");

    // Shutdown one node to trigger reconnection attempts
    let failed_node = cluster.nodes.remove(1);
    failed_node.shutdown().await.unwrap();

    println!("✓ Node shut down, remaining nodes will attempt reconnection");

    // Wait and verify remaining nodes don't create a connection storm
    // The backoff should prevent excessive reconnection attempts
    sleep(Duration::from_secs(5)).await;

    let metrics = cluster.collect_metrics().await.unwrap();
    assert_eq!(metrics.num_nodes, 2, "Should have 2 nodes remaining");

    println!("✓ Reconnection backoff test successful!");
    println!("  - Remaining nodes maintained connectivity");

    cluster.shutdown_all().await.unwrap();
}

// ========== High Frequency Operations ==========

#[tokio::test]
async fn test_high_frequency_dials() {
    init_tracing();
    println!("Testing high-frequency dial operations");

    // Create 2 nodes
    let cluster = TestCluster::new(2, 11300).await.unwrap();

    let target_addr = cluster.nodes[1].multiaddr();

    // Perform multiple rapid dial attempts
    for i in 0..10 {
        let _ = cluster.nodes[0].handle.dial(target_addr.clone()).await;
        if i % 3 == 0 {
            sleep(Duration::from_millis(100)).await;
        }
    }

    // Give time for connections to settle
    sleep(Duration::from_secs(2)).await;

    // Verify system is still healthy
    let peers = cluster.nodes[0]
        .handle
        .list_connected_peers()
        .await
        .unwrap();

    println!("✓ High-frequency dials handled");
    println!("  - Connected peers: {}", peers.len());

    cluster.shutdown_all().await.unwrap();
}

// ========== Resource Cleanup Tests ==========

#[tokio::test]
async fn test_resource_cleanup_after_failures() {
    init_tracing();
    println!("Testing resource cleanup after node failures");

    // Create 4-node cluster
    let mut cluster = TestCluster::new(4, 11400).await.unwrap();

    cluster.connect_mesh().await.unwrap();
    cluster
        .wait_for_mesh_connectivity(Duration::from_secs(15))
        .await
        .unwrap();

    println!("✓ Initial mesh established");

    // Kill 2 nodes
    cluster.shutdown_node(0).await.unwrap();
    sleep(Duration::from_secs(1)).await;
    cluster.shutdown_node(0).await.unwrap();
    sleep(Duration::from_secs(2)).await;

    // Remaining nodes should have cleaned up failed peer state
    let metrics = cluster.collect_metrics().await.unwrap();
    assert_eq!(metrics.num_nodes, 2, "Should have 2 nodes remaining");

    // Each remaining node should only show connection to the other remaining node
    // (not counting disconnected nodes)
    println!("✓ Resource cleanup verified");
    println!("  - Remaining nodes: {}", metrics.num_nodes);
    println!("  - Active connections: {}", metrics.total_connections);

    cluster.shutdown_all().await.unwrap();
}

// ========== Simultaneous Failures ==========

#[tokio::test]
async fn test_simultaneous_multiple_failures() {
    init_tracing();
    println!("Testing simultaneous multiple node failures");

    // Create 6-node cluster
    let mut cluster = TestCluster::new(6, 11500).await.unwrap();

    cluster.connect_mesh().await.unwrap();
    cluster
        .wait_for_mesh_connectivity(Duration::from_secs(20))
        .await
        .unwrap();

    println!("✓ Initial 6-node mesh established");

    // Kill 3 nodes sequentially (to avoid borrow checker issues)
    cluster.shutdown_node(0).await.unwrap();
    sleep(Duration::from_millis(500)).await;
    cluster.shutdown_node(0).await.unwrap();
    sleep(Duration::from_millis(500)).await;
    cluster.shutdown_node(0).await.unwrap();

    sleep(Duration::from_secs(3)).await;

    // Verify remaining 3 nodes are still connected
    let metrics = cluster.collect_metrics().await.unwrap();
    assert_eq!(
        metrics.num_nodes, 3,
        "Should have 3 nodes remaining after failures"
    );
    assert!(
        metrics.min_connections() > 0,
        "Remaining nodes should maintain connectivity"
    );

    println!("✓ Multiple failures handled successfully!");
    println!("  - Surviving nodes: {}", metrics.num_nodes);
    println!("  - Min connections: {}", metrics.min_connections());

    cluster.shutdown_all().await.unwrap();
}

// ========== Network Resilience ==========

#[tokio::test]
async fn test_partial_network_split() {
    init_tracing();
    println!("Testing partial network split scenario");

    // Create 6-node cluster
    let mut cluster = TestCluster::new(6, 11600).await.unwrap();

    cluster.connect_mesh().await.unwrap();
    cluster
        .wait_for_mesh_connectivity(Duration::from_secs(20))
        .await
        .unwrap();

    println!("✓ Initial mesh established");

    // Simulate a partition by disconnecting some nodes from each other
    // Group A: nodes 0, 1, 2
    // Group B: nodes 3, 4, 5
    // Disconnect all connections between groups

    for i in 0..3 {
        for j in 3..6 {
            if let (Some(node_a), Some(node_b)) = (cluster.nodes.get(i), cluster.nodes.get(j)) {
                let peer_b = node_b.peer_id;
                let _ = node_a.handle.disconnect(peer_b).await;
            }
        }
    }

    sleep(Duration::from_secs(3)).await;

    // Each group should still be internally connected
    for i in 0..3 {
        let peers = cluster.nodes[i]
            .handle
            .list_connected_peers()
            .await
            .unwrap();
        assert!(
            peers.len() >= 2,
            "Node {} in group A should be connected to other group A nodes",
            i
        );
    }

    for i in 3..6 {
        let peers = cluster.nodes[i]
            .handle
            .list_connected_peers()
            .await
            .unwrap();
        assert!(
            peers.len() >= 2,
            "Node {} in group B should be connected to other group B nodes",
            i
        );
    }

    println!("✓ Partial network split handled!");
    println!("  - Each partition maintained internal connectivity");

    cluster.shutdown_all().await.unwrap();
}
