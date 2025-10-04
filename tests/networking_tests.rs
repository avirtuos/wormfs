//! Integration tests for the NetworkService
//!
//! This module contains comprehensive tests for Phase 2A.1 and 2A.2 including
//! connection, disconnection, and batch dialing tests.

mod test_helpers;

use libp2p::Multiaddr;
use std::time::Duration;
use test_helpers::*;
use tokio::time::{sleep, timeout};
use wormfs::networking::{NetworkConfig, NetworkEvent, NetworkService};

/// Initialize tracing for tests
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();
}

#[tokio::test]
async fn test_service_creation_and_startup() {
    init_tracing();

    let config = NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/4010".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig::default(),
    };

    // Test service creation
    let (mut service, handle) = NetworkService::new(config.clone()).unwrap();

    // Verify peer ID is valid
    let peer_id = handle.local_peer_id();
    assert!(!peer_id.to_string().is_empty());

    // Test service startup
    let start_result = service.start(config).await;
    assert!(start_result.is_ok());

    // Start the service in a separate task
    let service_handle = tokio::spawn(async move { service.run().await });

    // Let the service run for a bit
    sleep(Duration::from_millis(100)).await;

    // Shutdown the service
    handle.shutdown().unwrap();

    // Wait for service to shutdown
    let result = timeout(Duration::from_secs(5), service_handle).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_ok());
}

#[tokio::test]
async fn test_invalid_listen_address() {
    init_tracing();

    let config = NetworkConfig {
        listen_address: "invalid-address".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig::default(),
    };

    let (mut service, _handle) = NetworkService::new(config.clone()).unwrap();
    let result = service.start(config).await;

    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Invalid listen address"));
}

#[tokio::test]
async fn test_peer_id_consistency() {
    init_tracing();

    let config = NetworkConfig::default();
    let (_service, handle) = NetworkService::new(config).unwrap();

    let peer_id1 = handle.local_peer_id();
    let peer_id2 = handle.local_peer_id();

    // Peer ID should be consistent
    assert_eq!(peer_id1, peer_id2);

    // Should be a valid peer ID format (starts with common prefixes)
    let peer_str = peer_id1.to_string();
    assert!(peer_str.starts_with("12D3") || peer_str.starts_with("Qm"));
}

#[tokio::test]
async fn test_service_stability() {
    init_tracing();

    let (mut service, handle) = create_test_node(4011).await.unwrap();

    // Start the service
    let service_handle = tokio::spawn(async move { service.run().await });

    // Let it run for 2 seconds to test stability
    sleep(Duration::from_secs(2)).await;

    // Service should still be running
    assert!(!service_handle.is_finished());

    // Shutdown
    handle.shutdown().unwrap();
    let result = timeout(Duration::from_secs(5), service_handle).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_graceful_shutdown() {
    init_tracing();

    let (mut service, handle) = create_test_node(4012).await.unwrap();

    let start_time = std::time::Instant::now();

    // Start the service
    let service_handle = tokio::spawn(async move { service.run().await });

    // Let it run briefly
    sleep(Duration::from_millis(100)).await;

    // Request shutdown
    handle.shutdown().unwrap();

    // Wait for shutdown to complete
    let result = timeout(Duration::from_secs(5), service_handle).await;
    let shutdown_time = start_time.elapsed();

    assert!(result.is_ok());
    assert!(result.unwrap().is_ok());
    assert!(
        shutdown_time < Duration::from_secs(5),
        "Shutdown took too long: {:?}",
        shutdown_time
    );
}

#[tokio::test]
async fn test_two_node_communication() {
    init_tracing();

    // Create two nodes on different ports
    let (mut service_a, mut handle_a) = create_test_node(4020).await.unwrap();
    let (mut service_b, mut handle_b) = create_test_node(4021).await.unwrap();

    let peer_a_id = handle_a.local_peer_id();
    let peer_b_id = handle_b.local_peer_id();

    println!("Node A peer ID: {}", peer_a_id);
    println!("Node B peer ID: {}", peer_b_id);

    // Start both services
    let service_a_handle = tokio::spawn(async move { service_a.run().await });

    let service_b_handle = tokio::spawn(async move { service_b.run().await });

    // Give services time to start listening
    sleep(Duration::from_millis(500)).await;

    // Node A dials Node B
    let dial_addr: Multiaddr = "/ip4/127.0.0.1/tcp/4021".parse().unwrap();
    println!("Node A dialing Node B at: {}", dial_addr);

    let dial_result = handle_a.dial(dial_addr).await;
    assert!(dial_result.is_ok(), "Failed to dial: {:?}", dial_result);

    // Wait for both nodes to see the connection
    println!("Waiting for connection events...");

    let connection_timeout = Duration::from_secs(10);

    // Node A should see connection to Node B
    let a_sees_b = wait_for_connection_event(&mut handle_a, peer_b_id, connection_timeout);
    // Node B should see connection from Node A
    let b_sees_a = wait_for_connection_event(&mut handle_b, peer_a_id, connection_timeout);

    // Wait for both connection events
    let (a_result, b_result) = tokio::join!(a_sees_b, b_sees_a);

    assert!(
        a_result.is_ok(),
        "Node A didn't see connection to Node B: {:?}",
        a_result
    );
    assert!(
        b_result.is_ok(),
        "Node B didn't see connection from Node A: {:?}",
        b_result
    );

    // Verify both nodes show each other as connected
    let peers_a = handle_a.list_connected_peers().await.unwrap();
    let peers_b = handle_b.list_connected_peers().await.unwrap();

    assert!(
        peers_a.contains(&peer_b_id),
        "Node A doesn't list Node B as connected. Connected peers: {:?}",
        peers_a
    );
    assert!(
        peers_b.contains(&peer_a_id),
        "Node B doesn't list Node A as connected. Connected peers: {:?}",
        peers_b
    );

    println!("✓ Two-node communication test successful!");
    println!("Node A connected peers: {:?}", peers_a);
    println!("Node B connected peers: {:?}", peers_b);

    // Clean shutdown
    handle_a.shutdown().unwrap();
    handle_b.shutdown().unwrap();

    let shutdown_a = timeout(Duration::from_secs(5), service_a_handle);
    let shutdown_b = timeout(Duration::from_secs(5), service_b_handle);

    let (result_a, result_b) = tokio::join!(shutdown_a, shutdown_b);
    assert!(result_a.is_ok() && result_a.unwrap().is_ok());
    assert!(result_b.is_ok() && result_b.unwrap().is_ok());
}

#[tokio::test]
async fn test_dial_nonexistent_peer() {
    init_tracing();

    let (mut service, mut handle) = create_test_node(4013).await.unwrap();

    // Start the service
    let service_handle = tokio::spawn(async move { service.run().await });

    // Try to dial a non-existent address
    let bad_addr: Multiaddr = "/ip4/127.0.0.1/tcp/9999".parse().unwrap();
    let dial_result = handle.dial(bad_addr).await;

    // Dial should succeed (connection attempt starts) but connection will fail
    assert!(dial_result.is_ok());

    // We might receive a connection error event
    if let Some(NetworkEvent::Error { message }) =
        timeout(Duration::from_secs(2), handle.next_event())
            .await
            .ok()
            .flatten()
    {
        assert!(message.contains("connection") || message.contains("error"));
    }

    // Clean shutdown
    handle.shutdown().unwrap();
    let result = timeout(Duration::from_secs(5), service_handle).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_multiple_connections() {
    init_tracing();

    // Create three nodes
    let (mut service_a, handle_a) = create_test_node(4030).await.unwrap();
    let (mut service_b, handle_b) = create_test_node(4031).await.unwrap();
    let (mut service_c, handle_c) = create_test_node(4032).await.unwrap();

    let peer_b_id = handle_b.local_peer_id();
    let peer_c_id = handle_c.local_peer_id();

    // Start all services
    let service_a_handle = tokio::spawn(async move { service_a.run().await });
    let service_b_handle = tokio::spawn(async move { service_b.run().await });
    let service_c_handle = tokio::spawn(async move { service_c.run().await });

    // Give services time to start
    sleep(Duration::from_millis(500)).await;

    // Node A connects to both B and C
    let dial_b: Multiaddr = "/ip4/127.0.0.1/tcp/4031".parse().unwrap();
    let dial_c: Multiaddr = "/ip4/127.0.0.1/tcp/4032".parse().unwrap();

    let dial_b_result = handle_a.dial(dial_b).await;
    let dial_c_result = handle_a.dial(dial_c).await;

    assert!(dial_b_result.is_ok());
    assert!(dial_c_result.is_ok());

    // Wait a bit for connections to establish
    sleep(Duration::from_secs(2)).await;

    // Verify Node A is connected to both B and C
    let peers_a = handle_a.list_connected_peers().await.unwrap();
    assert!(
        peers_a.len() >= 2,
        "Node A should be connected to at least 2 peers, got: {:?}",
        peers_a
    );
    assert!(peers_a.contains(&peer_b_id));
    assert!(peers_a.contains(&peer_c_id));

    // Clean shutdown
    handle_a.shutdown().unwrap();
    handle_b.shutdown().unwrap();
    handle_c.shutdown().unwrap();

    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_a_handle),
        timeout(Duration::from_secs(5), service_b_handle),
        timeout(Duration::from_secs(5), service_c_handle)
    );
}

// ========== Phase 2A.2 Tests ==========

#[tokio::test]
async fn test_ping_between_nodes() {
    init_tracing();

    // Create two nodes
    let (mut service_a, mut handle_a) = create_test_node(4070).await.unwrap();
    let (mut service_b, handle_b) = create_test_node(4071).await.unwrap();

    let peer_b_id = handle_b.local_peer_id();

    println!("Testing ping functionality between two nodes");

    // Start both services
    let service_a_handle = tokio::spawn(async move { service_a.run().await });
    let service_b_handle = tokio::spawn(async move { service_b.run().await });

    sleep(Duration::from_millis(500)).await;

    // Connect A to B
    let dial_addr = localhost_multiaddr(4071);
    handle_a.dial(dial_addr).await.unwrap();

    // Wait for connection
    let connection_timeout = Duration::from_secs(5);
    wait_for_connection_event(&mut handle_a, peer_b_id, connection_timeout)
        .await
        .unwrap();

    println!("✓ Nodes connected, waiting for ping events...");

    // Wait for at least one ping success event (pings happen every 15s by default)
    // With default config, we should see pings within 20 seconds
    let ping_timeout = Duration::from_secs(20);
    let result: Result<Result<Duration, ()>, _> = timeout(ping_timeout, async {
        loop {
            if let Some(event) = handle_a.next_event().await {
                match event {
                    NetworkEvent::PingSuccess { peer_id, rtt } => {
                        println!("✓ Ping success to {}: {:?}", peer_id, rtt);
                        if peer_id == peer_b_id {
                            return Ok(rtt);
                        }
                    }
                    NetworkEvent::PingFailure { peer_id } => {
                        println!("⚠ Ping failure to {}", peer_id);
                    }
                    _ => {}
                }
            }
        }
    })
    .await;

    assert!(result.is_ok(), "Should receive ping success event");
    let rtt = result.unwrap().unwrap();
    println!("✓ Measured RTT: {:?}", rtt);

    // RTT should be reasonable (< 1 second for localhost)
    assert!(
        rtt < Duration::from_secs(1),
        "RTT too high for localhost: {:?}",
        rtt
    );

    println!("✓ Ping test successful!");

    // Clean shutdown
    handle_a.shutdown().unwrap();
    handle_b.shutdown().unwrap();

    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_a_handle),
        timeout(Duration::from_secs(5), service_b_handle)
    );
}

// ========== Phase 2A.3 Tests (Ping) ==========

// ========== Phase 2A.4 Tests (Peer State Tracking) ==========

#[tokio::test]
async fn test_peer_state_connected() {
    init_tracing();

    // Create two nodes
    let (mut service_a, handle_a) = create_test_node(4080).await.unwrap();
    let (mut service_b, handle_b) = create_test_node(4081).await.unwrap();

    let peer_b_id = handle_b.local_peer_id();

    println!("Testing peer state tracking - Connected state");

    // Start both services
    let service_a_handle = tokio::spawn(async move { service_a.run().await });
    let service_b_handle = tokio::spawn(async move { service_b.run().await });

    sleep(Duration::from_millis(500)).await;

    // Connect A to B
    let dial_addr = localhost_multiaddr(4081);
    handle_a.dial(dial_addr).await.unwrap();

    // Wait for connection
    sleep(Duration::from_secs(1)).await;

    // Check peer state - should be Connected
    let state = handle_a.get_peer_state(peer_b_id).await.unwrap();
    assert_eq!(
        state,
        Some(wormfs::networking::PeerState::Connected),
        "Peer should be in Connected state"
    );

    // Get full peer info
    let info = handle_a.get_peer_info(peer_b_id).await.unwrap();
    assert!(info.is_some(), "Peer info should exist");
    let info = info.unwrap();
    assert_eq!(info.state, wormfs::networking::PeerState::Connected);
    assert!(info.connected_at.is_some());
    assert!(info.disconnected_at.is_none());

    println!("✓ Peer state Connected verified!");

    // Clean shutdown
    handle_a.shutdown().unwrap();
    handle_b.shutdown().unwrap();

    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_a_handle),
        timeout(Duration::from_secs(5), service_b_handle)
    );
}

#[tokio::test]
async fn test_peer_state_disconnected() {
    init_tracing();

    // Create two nodes
    let (mut service_a, mut handle_a) = create_test_node(4082).await.unwrap();
    let (mut service_b, handle_b) = create_test_node(4083).await.unwrap();

    let peer_b_id = handle_b.local_peer_id();

    println!("Testing peer state tracking - Disconnected state");

    // Start both services
    let service_a_handle = tokio::spawn(async move { service_a.run().await });
    let service_b_handle = tokio::spawn(async move { service_b.run().await });

    sleep(Duration::from_millis(500)).await;

    // Connect A to B
    let dial_addr = localhost_multiaddr(4083);
    handle_a.dial(dial_addr).await.unwrap();

    // Wait for connection
    let connection_timeout = Duration::from_secs(5);
    wait_for_connection_event(&mut handle_a, peer_b_id, connection_timeout)
        .await
        .unwrap();

    // Verify Connected state
    let state = handle_a.get_peer_state(peer_b_id).await.unwrap();
    assert_eq!(state, Some(wormfs::networking::PeerState::Connected));

    // Disconnect
    handle_a.disconnect(peer_b_id).await.unwrap();

    // Wait for disconnection
    sleep(Duration::from_secs(1)).await;

    // Check peer state - should be Disconnected
    let state = handle_a.get_peer_state(peer_b_id).await.unwrap();
    assert_eq!(
        state,
        Some(wormfs::networking::PeerState::Disconnected),
        "Peer should be in Disconnected state"
    );

    // Get full peer info
    let info = handle_a.get_peer_info(peer_b_id).await.unwrap();
    assert!(
        info.is_some(),
        "Peer info should still exist after disconnect"
    );
    let info = info.unwrap();
    assert_eq!(info.state, wormfs::networking::PeerState::Disconnected);
    assert!(info.disconnected_at.is_some());

    println!("✓ Peer state Disconnected verified!");

    // Clean shutdown
    handle_a.shutdown().unwrap();
    handle_b.shutdown().unwrap();

    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_a_handle),
        timeout(Duration::from_secs(5), service_b_handle)
    );
}

#[tokio::test]
async fn test_peer_state_query_api() {
    init_tracing();

    // Create nodes
    let (mut service_a, handle_a) = create_test_node(4084).await.unwrap();
    let (mut service_b, handle_b) = create_test_node(4085).await.unwrap();
    let (mut service_c, handle_c) = create_test_node(4086).await.unwrap();

    let peer_b_id = handle_b.local_peer_id();
    let peer_c_id = handle_c.local_peer_id();

    println!("Testing peer state query API");

    // Start all services
    let service_a_handle = tokio::spawn(async move { service_a.run().await });
    let service_b_handle = tokio::spawn(async move { service_b.run().await });
    let service_c_handle = tokio::spawn(async move { service_c.run().await });

    sleep(Duration::from_millis(500)).await;

    // Connect A to B and C
    handle_a.dial(localhost_multiaddr(4085)).await.unwrap();
    handle_a.dial(localhost_multiaddr(4086)).await.unwrap();

    // Wait for connections
    sleep(Duration::from_secs(1)).await;

    // List peers by state - should have 2 Connected
    let connected = handle_a
        .list_peers_by_state(wormfs::networking::PeerState::Connected)
        .await
        .unwrap();
    assert_eq!(connected.len(), 2, "Should have 2 connected peers");
    assert!(connected.contains(&peer_b_id));
    assert!(connected.contains(&peer_c_id));

    // Disconnect from B
    handle_a.disconnect(peer_b_id).await.unwrap();
    sleep(Duration::from_secs(1)).await;

    // Now should have 1 Connected, 1 Disconnected
    let connected = handle_a
        .list_peers_by_state(wormfs::networking::PeerState::Connected)
        .await
        .unwrap();
    assert_eq!(connected.len(), 1, "Should have 1 connected peer");
    assert!(connected.contains(&peer_c_id));

    let disconnected = handle_a
        .list_peers_by_state(wormfs::networking::PeerState::Disconnected)
        .await
        .unwrap();
    assert_eq!(disconnected.len(), 1, "Should have 1 disconnected peer");
    assert!(disconnected.contains(&peer_b_id));

    println!("✓ Peer state query API verified!");

    // Clean shutdown
    handle_a.shutdown().unwrap();
    handle_b.shutdown().unwrap();
    handle_c.shutdown().unwrap();

    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_a_handle),
        timeout(Duration::from_secs(5), service_b_handle),
        timeout(Duration::from_secs(5), service_c_handle)
    );
}

// ========== Phase 2A.2 Tests (continued) ==========

#[tokio::test]
async fn test_explicit_disconnect() {
    init_tracing();

    // Create two nodes
    let (mut service_a, mut handle_a) = create_test_node(4040).await.unwrap();
    let (mut service_b, mut handle_b) = create_test_node(4041).await.unwrap();

    let peer_a_id = handle_a.local_peer_id();
    let peer_b_id = handle_b.local_peer_id();

    println!("Testing explicit disconnect");
    println!("Node A: {}", peer_a_id);
    println!("Node B: {}", peer_b_id);

    // Start both services
    let service_a_handle = tokio::spawn(async move { service_a.run().await });
    let service_b_handle = tokio::spawn(async move { service_b.run().await });

    sleep(Duration::from_millis(500)).await;

    // Connect A to B
    let dial_addr = localhost_multiaddr(4041);
    handle_a.dial(dial_addr).await.unwrap();

    // Wait for connection
    let connection_timeout = Duration::from_secs(5);
    wait_for_connection_event(&mut handle_a, peer_b_id, connection_timeout)
        .await
        .unwrap();
    wait_for_connection_event(&mut handle_b, peer_a_id, connection_timeout)
        .await
        .unwrap();

    // Verify connected
    assert_peer_connected(&handle_a, peer_b_id).await.unwrap();
    assert_peer_connected(&handle_b, peer_a_id).await.unwrap();

    println!("✓ Nodes connected successfully");

    // Explicitly disconnect from Node A's side
    println!("Disconnecting from Node A...");
    handle_a.disconnect(peer_b_id).await.unwrap();

    // Wait for both sides to see the disconnection
    let disconnect_timeout = Duration::from_secs(5);
    wait_for_disconnection_event(&mut handle_a, peer_b_id, disconnect_timeout)
        .await
        .unwrap();
    wait_for_disconnection_event(&mut handle_b, peer_a_id, disconnect_timeout)
        .await
        .unwrap();

    // Verify disconnected
    assert_peer_disconnected(&handle_a, peer_b_id)
        .await
        .unwrap();
    assert_peer_disconnected(&handle_b, peer_a_id)
        .await
        .unwrap();

    println!("✓ Explicit disconnect test successful!");

    // Clean shutdown
    handle_a.shutdown().unwrap();
    handle_b.shutdown().unwrap();

    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_a_handle),
        timeout(Duration::from_secs(5), service_b_handle)
    );
}

#[tokio::test]
async fn test_batch_dial() {
    init_tracing();

    // Create one central node and three target nodes
    let (mut service_main, handle_main) = create_test_node(4050).await.unwrap();
    let (mut service_1, handle_1) = create_test_node(4051).await.unwrap();
    let (mut service_2, handle_2) = create_test_node(4052).await.unwrap();
    let (mut service_3, handle_3) = create_test_node(4053).await.unwrap();

    let peer_1_id = handle_1.local_peer_id();
    let peer_2_id = handle_2.local_peer_id();
    let peer_3_id = handle_3.local_peer_id();

    println!("Testing batch dial to multiple peers");

    // Start all services
    let main_handle = tokio::spawn(async move { service_main.run().await });
    let s1_handle = tokio::spawn(async move { service_1.run().await });
    let s2_handle = tokio::spawn(async move { service_2.run().await });
    let s3_handle = tokio::spawn(async move { service_3.run().await });

    sleep(Duration::from_millis(500)).await;

    // Batch dial to all three nodes
    let addresses = vec![
        localhost_multiaddr(4051),
        localhost_multiaddr(4052),
        localhost_multiaddr(4053),
    ];

    println!("Dialing {} peers concurrently...", addresses.len());
    let dial_results = handle_main.dial_many(addresses).await.unwrap();

    // All dials should succeed
    for (i, result) in dial_results.iter().enumerate() {
        assert!(
            result.is_ok(),
            "Dial to peer {} failed: {:?}",
            i + 1,
            result
        );
    }

    println!("✓ All dial attempts succeeded");

    // Wait for connections to establish
    sleep(Duration::from_secs(2)).await;

    // Verify all three are connected
    let peers = handle_main.list_connected_peers().await.unwrap();
    assert!(
        peers.len() >= 3,
        "Expected at least 3 connections, got {}",
        peers.len()
    );
    assert!(peers.contains(&peer_1_id));
    assert!(peers.contains(&peer_2_id));
    assert!(peers.contains(&peer_3_id));

    println!(
        "✓ Batch dial test successful! Connected to {} peers",
        peers.len()
    );

    // Clean shutdown
    handle_main.shutdown().unwrap();
    handle_1.shutdown().unwrap();
    handle_2.shutdown().unwrap();
    handle_3.shutdown().unwrap();

    let _ = tokio::join!(
        timeout(Duration::from_secs(5), main_handle),
        timeout(Duration::from_secs(5), s1_handle),
        timeout(Duration::from_secs(5), s2_handle),
        timeout(Duration::from_secs(5), s3_handle)
    );
}

#[tokio::test]
async fn test_batch_dial_with_failures() {
    init_tracing();

    let (mut service, handle) = create_test_node(4060).await.unwrap();
    let service_handle = tokio::spawn(async move { service.run().await });

    sleep(Duration::from_millis(500)).await;

    // Try to dial a mix of valid and invalid addresses
    let addresses = vec![
        localhost_multiaddr(9991), // Invalid
        localhost_multiaddr(9992), // Invalid
        localhost_multiaddr(9993), // Invalid
    ];

    let dial_results = handle.dial_many(addresses).await.unwrap();

    // All should "succeed" in the sense that dial attempts were made
    // (actual connection will fail asynchronously)
    assert_eq!(dial_results.len(), 3);
    for result in &dial_results {
        assert!(result.is_ok(), "Dial attempt should succeed: {:?}", result);
    }

    println!("✓ Batch dial with invalid addresses handled correctly");

    // Clean shutdown
    handle.shutdown().unwrap();
    let _ = timeout(Duration::from_secs(5), service_handle).await;
}

// ========== Phase 2A.5 Tests (Peer Authentication) ==========

#[tokio::test]
async fn test_disabled_mode() {
    init_tracing();

    use tempfile::TempDir;
    use wormfs::networking::AuthenticationMode;

    let temp_dir = TempDir::new().unwrap();
    let peers_file = temp_dir.path().join("peers.json");

    // Create nodes with authentication disabled
    let config_a = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5000".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Disabled,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let config_b = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5001".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Disabled,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let (mut service_a, mut handle_a) =
        wormfs::networking::NetworkService::new(config_a.clone()).unwrap();
    let (mut service_b, handle_b) =
        wormfs::networking::NetworkService::new(config_b.clone()).unwrap();

    service_a.start(config_a).await.unwrap();
    service_b.start(config_b).await.unwrap();

    let peer_b_id = handle_b.local_peer_id();

    let service_a_handle = tokio::spawn(async move { service_a.run().await });
    let service_b_handle = tokio::spawn(async move { service_b.run().await });

    sleep(Duration::from_millis(500)).await;

    // Connect A to B - should work in disabled mode
    handle_a.dial(localhost_multiaddr(5001)).await.unwrap();

    wait_for_connection_event(&mut handle_a, peer_b_id, Duration::from_secs(5))
        .await
        .unwrap();

    println!("✓ Disabled mode: Connection accepted");

    // Clean shutdown
    handle_a.shutdown().unwrap();
    handle_b.shutdown().unwrap();
    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_a_handle),
        timeout(Duration::from_secs(5), service_b_handle)
    );
}

#[tokio::test]
async fn test_learn_mode_new_peer() {
    init_tracing();

    use tempfile::TempDir;
    use wormfs::networking::AuthenticationMode;

    let temp_dir = TempDir::new().unwrap();
    let peers_file = temp_dir.path().join("peers.json");

    let config_a = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5010".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Learn,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let config_b = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5011".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Learn,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let (mut service_a, mut handle_a) =
        wormfs::networking::NetworkService::new(config_a.clone()).unwrap();
    let (mut service_b, handle_b) =
        wormfs::networking::NetworkService::new(config_b.clone()).unwrap();

    service_a.start(config_a).await.unwrap();
    service_b.start(config_b).await.unwrap();

    let peer_b_id = handle_b.local_peer_id();

    let service_a_handle = tokio::spawn(async move { service_a.run().await });
    let service_b_handle = tokio::spawn(async move { service_b.run().await });

    sleep(Duration::from_millis(500)).await;

    // Connect - should be learned
    handle_a.dial(localhost_multiaddr(5011)).await.unwrap();

    wait_for_connection_event(&mut handle_a, peer_b_id, Duration::from_secs(5))
        .await
        .unwrap();

    println!("✓ Learn mode: New peer accepted and learned");

    // Clean shutdown
    handle_a.shutdown().unwrap();
    handle_b.shutdown().unwrap();
    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_a_handle),
        timeout(Duration::from_secs(5), service_b_handle)
    );
}

#[tokio::test]
async fn test_learn_mode_file_persistence() {
    init_tracing();

    use tempfile::TempDir;
    use wormfs::networking::AuthenticationMode;

    let temp_dir = TempDir::new().unwrap();
    let peers_file = temp_dir.path().join("peers.json");

    let config_a = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5020".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Learn,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let config_b = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5021".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Learn,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let (mut service_a, mut handle_a) =
        wormfs::networking::NetworkService::new(config_a.clone()).unwrap();
    let (mut service_b, handle_b) =
        wormfs::networking::NetworkService::new(config_b.clone()).unwrap();

    service_a.start(config_a).await.unwrap();
    service_b.start(config_b).await.unwrap();

    let peer_b_id = handle_b.local_peer_id();

    let service_a_handle = tokio::spawn(async move { service_a.run().await });
    let service_b_handle = tokio::spawn(async move { service_b.run().await });

    sleep(Duration::from_millis(500)).await;

    // Connect and learn
    handle_a.dial(localhost_multiaddr(5021)).await.unwrap();
    wait_for_connection_event(&mut handle_a, peer_b_id, Duration::from_secs(5))
        .await
        .unwrap();

    sleep(Duration::from_secs(1)).await;

    // Verify peers file was created
    assert!(peers_file.exists(), "Peers file should be created");

    let content = std::fs::read_to_string(&peers_file).unwrap();
    assert!(
        content.contains(&peer_b_id.to_string()),
        "Peer should be in file"
    );

    println!("✓ Learn mode: Peers file persisted correctly");

    // Clean shutdown
    handle_a.shutdown().unwrap();
    handle_b.shutdown().unwrap();
    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_a_handle),
        timeout(Duration::from_secs(5), service_b_handle)
    );
}

#[tokio::test]
async fn test_enforce_mode_allowed_peer() {
    init_tracing();

    use tempfile::TempDir;
    use wormfs::networking::AuthenticationMode;

    let temp_dir = TempDir::new().unwrap();
    let peers_file = temp_dir.path().join("peers.json");

    // First create a node in learn mode to populate the peers file
    let config_learn = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5030".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Learn,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let config_b = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5031".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Learn,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let (mut service_learn, mut handle_learn) =
        wormfs::networking::NetworkService::new(config_learn.clone()).unwrap();
    let (mut service_b, handle_b) =
        wormfs::networking::NetworkService::new(config_b.clone()).unwrap();

    service_learn.start(config_learn).await.unwrap();
    service_b.start(config_b).await.unwrap();

    let peer_b_id = handle_b.local_peer_id();

    let service_learn_handle = tokio::spawn(async move { service_learn.run().await });
    let service_b_handle = tokio::spawn(async move { service_b.run().await });

    sleep(Duration::from_millis(500)).await;

    // Learn the peer first
    handle_learn.dial(localhost_multiaddr(5031)).await.unwrap();
    wait_for_connection_event(&mut handle_learn, peer_b_id, Duration::from_secs(5))
        .await
        .unwrap();

    sleep(Duration::from_secs(1)).await;

    // Shutdown learn node
    handle_learn.shutdown().unwrap();
    let _ = timeout(Duration::from_secs(5), service_learn_handle).await;

    // Now create a node in enforce mode with the populated peers file
    let config_enforce = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5032".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Enforce,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let (mut service_enforce, mut handle_enforce) =
        wormfs::networking::NetworkService::new(config_enforce.clone()).unwrap();
    service_enforce.start(config_enforce).await.unwrap();
    let service_enforce_handle = tokio::spawn(async move { service_enforce.run().await });

    sleep(Duration::from_millis(500)).await;

    // Should be able to connect because peer is in file
    handle_enforce
        .dial(localhost_multiaddr(5031))
        .await
        .unwrap();
    wait_for_connection_event(&mut handle_enforce, peer_b_id, Duration::from_secs(5))
        .await
        .unwrap();

    println!("✓ Enforce mode: Allowed peer accepted");

    // Clean shutdown
    handle_enforce.shutdown().unwrap();
    handle_b.shutdown().unwrap();
    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_enforce_handle),
        timeout(Duration::from_secs(5), service_b_handle)
    );
}

#[tokio::test]
async fn test_enforce_mode_unknown_peer() {
    init_tracing();

    use tempfile::TempDir;
    use wormfs::networking::AuthenticationMode;

    let temp_dir = TempDir::new().unwrap();
    let peers_file = temp_dir.path().join("peers.json");

    // Create empty peers file
    std::fs::write(&peers_file, r#"{"version":"1.0","peers":[]}"#).unwrap();

    let config_a = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5040".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Enforce,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let config_b = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5041".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Enforce,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let (mut service_a, mut handle_a) =
        wormfs::networking::NetworkService::new(config_a.clone()).unwrap();
    let (mut service_b, handle_b) =
        wormfs::networking::NetworkService::new(config_b.clone()).unwrap();

    service_a.start(config_a).await.unwrap();
    service_b.start(config_b).await.unwrap();

    let peer_b_id = handle_b.local_peer_id();

    let service_a_handle = tokio::spawn(async move { service_a.run().await });
    let service_b_handle = tokio::spawn(async move { service_b.run().await });

    sleep(Duration::from_millis(500)).await;

    // Try to connect - should be rejected
    handle_a.dial(localhost_multiaddr(5041)).await.unwrap();

    // Wait for AuthenticationFailed event
    let result = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = handle_a.next_event().await {
                match event {
                    wormfs::networking::NetworkEvent::AuthenticationFailed { peer_id, reason } => {
                        assert_eq!(peer_id, peer_b_id);
                        assert!(reason.contains("not in authorized list"));
                        return;
                    }
                    _ => continue,
                }
            }
        }
    })
    .await;

    assert!(result.is_ok(), "Should receive AuthenticationFailed event");

    println!("✓ Enforce mode: Unknown peer rejected");

    // Clean shutdown
    handle_a.shutdown().unwrap();
    handle_b.shutdown().unwrap();
    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_a_handle),
        timeout(Duration::from_secs(5), service_b_handle)
    );
}

#[tokio::test]
async fn test_manual_peer_addition() {
    init_tracing();

    use tempfile::TempDir;
    use wormfs::networking::AuthenticationMode;

    let temp_dir = TempDir::new().unwrap();
    let peers_file = temp_dir.path().join("peers.json");

    // Create node B first to get its peer ID
    let config_b = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5051".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Disabled,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let (mut service_b, handle_b) =
        wormfs::networking::NetworkService::new(config_b.clone()).unwrap();
    service_b.start(config_b).await.unwrap();
    let peer_b_id = handle_b.local_peer_id();
    let service_b_handle = tokio::spawn(async move { service_b.run().await });

    // Manually create peers file with peer B
    let peers_json = format!(
        r#"{{"version":"1.0","peers":[{{"peer_id":"{}","ip_addresses":["127.0.0.1"],"first_seen":"2025-01-01T00:00:00Z","last_seen":"2025-01-01T00:00:00Z","connection_count":0,"source":"manual"}}]}}"#,
        peer_b_id
    );
    std::fs::write(&peers_file, peers_json).unwrap();

    // Create node A in enforce mode
    let config_a = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5050".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Enforce,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let (mut service_a, mut handle_a) =
        wormfs::networking::NetworkService::new(config_a.clone()).unwrap();
    service_a.start(config_a).await.unwrap();
    let service_a_handle = tokio::spawn(async move { service_a.run().await });

    sleep(Duration::from_millis(500)).await;

    // Should be able to connect with manually added peer
    handle_a.dial(localhost_multiaddr(5051)).await.unwrap();
    wait_for_connection_event(&mut handle_a, peer_b_id, Duration::from_secs(5))
        .await
        .unwrap();

    println!("✓ Manual peer addition: Connection successful");

    // Clean shutdown
    handle_a.shutdown().unwrap();
    handle_b.shutdown().unwrap();
    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_a_handle),
        timeout(Duration::from_secs(5), service_b_handle)
    );
}

#[tokio::test]
async fn test_mode_transition_learn_to_enforce() {
    init_tracing();

    use tempfile::TempDir;
    use wormfs::networking::AuthenticationMode;

    let temp_dir = TempDir::new().unwrap();
    let peers_file = temp_dir.path().join("peers.json");

    // Phase 1: Learn mode
    let config_learn = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5060".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Learn,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let config_b = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5061".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Disabled,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let (mut service_learn, mut handle_learn) =
        wormfs::networking::NetworkService::new(config_learn.clone()).unwrap();
    let (mut service_b, handle_b) =
        wormfs::networking::NetworkService::new(config_b.clone()).unwrap();

    service_learn.start(config_learn).await.unwrap();
    service_b.start(config_b).await.unwrap();

    let peer_b_id = handle_b.local_peer_id();

    let service_learn_handle = tokio::spawn(async move { service_learn.run().await });
    let service_b_handle = tokio::spawn(async move { service_b.run().await });

    sleep(Duration::from_millis(500)).await;

    // Learn the peer
    handle_learn.dial(localhost_multiaddr(5061)).await.unwrap();
    wait_for_connection_event(&mut handle_learn, peer_b_id, Duration::from_secs(5))
        .await
        .unwrap();

    sleep(Duration::from_secs(1)).await;

    // Shutdown learn node
    handle_learn.shutdown().unwrap();
    let _ = timeout(Duration::from_secs(5), service_learn_handle).await;

    // Phase 2: Restart in enforce mode
    let config_enforce = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5062".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Enforce,
            peers_file: peers_file.to_str().unwrap().to_string(),
        },
    };

    let (mut service_enforce, mut handle_enforce) =
        wormfs::networking::NetworkService::new(config_enforce.clone()).unwrap();
    service_enforce.start(config_enforce).await.unwrap();
    let service_enforce_handle = tokio::spawn(async move { service_enforce.run().await });

    sleep(Duration::from_millis(500)).await;

    // Should still be able to connect in enforce mode
    handle_enforce
        .dial(localhost_multiaddr(5061))
        .await
        .unwrap();
    wait_for_connection_event(&mut handle_enforce, peer_b_id, Duration::from_secs(5))
        .await
        .unwrap();

    println!("✓ Mode transition: Learn -> Enforce successful");

    // Clean shutdown
    handle_enforce.shutdown().unwrap();
    handle_b.shutdown().unwrap();
    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_enforce_handle),
        timeout(Duration::from_secs(5), service_b_handle)
    );
}

#[tokio::test]
async fn test_learn_mode_ip_mismatch_rejection() {
    init_tracing();

    use tempfile::TempDir;
    use wormfs::networking::AuthenticationMode;

    let temp_dir_a = TempDir::new().unwrap();
    let temp_dir_b = TempDir::new().unwrap();
    let peers_file_a = temp_dir_a.path().join("peers.json");
    let peers_file_b = temp_dir_b.path().join("peers.json");

    // Create node B with disabled authentication
    let config_b = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5071".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Disabled,
            peers_file: peers_file_b.to_str().unwrap().to_string(),
        },
    };

    let (mut service_b, handle_b) =
        wormfs::networking::NetworkService::new(config_b.clone()).unwrap();
    let peer_b_id = handle_b.local_peer_id();
    service_b.start(config_b).await.unwrap();

    // Manually create peers file for node A with peer B but wrong IP
    let peers_json = format!(
        r#"{{"version":"1.0","peers":[{{"peer_id":"{}","ip_addresses":["192.168.1.1"],"first_seen":"2025-01-01T00:00:00Z","last_seen":"2025-01-01T00:00:00Z","connection_count":1,"source":"learned"}}]}}"#,
        peer_b_id
    );
    std::fs::write(&peers_file_a, peers_json).unwrap();

    // Create node A in learn mode
    let config_a = wormfs::networking::NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/5070".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Learn,
            peers_file: peers_file_a.to_str().unwrap().to_string(),
        },
    };

    let (mut service_a, mut handle_a) =
        wormfs::networking::NetworkService::new(config_a.clone()).unwrap();
    service_a.start(config_a).await.unwrap();

    let service_a_handle = tokio::spawn(async move { service_a.run().await });
    let service_b_handle = tokio::spawn(async move { service_b.run().await });

    sleep(Duration::from_millis(500)).await;

    // Try to connect - should be rejected due to IP mismatch
    handle_a.dial(localhost_multiaddr(5071)).await.unwrap();

    // Wait for AuthenticationFailed event
    let result = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = handle_a.next_event().await {
                match event {
                    wormfs::networking::NetworkEvent::AuthenticationFailed { peer_id, reason } => {
                        assert_eq!(peer_id, peer_b_id);
                        assert!(reason.contains("IP mismatch"));
                        return;
                    }
                    _ => continue,
                }
            }
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "Should receive AuthenticationFailed event for IP mismatch"
    );

    println!("✓ Learn mode: IP mismatch rejection successful");

    // Clean shutdown
    handle_a.shutdown().unwrap();
    handle_b.shutdown().unwrap();
    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_a_handle),
        timeout(Duration::from_secs(5), service_b_handle)
    );
}

// ========== Phase 2A.6 Tests (Bootstrap Peer Discovery) ==========

#[tokio::test]
async fn test_bootstrap_peer_parsing() {
    init_tracing();

    // Test valid bootstrap peer format
    let valid = "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN@/ip4/192.168.1.100/tcp/4001";
    let result = wormfs::networking::BootstrapPeer::parse(valid);
    assert!(result.is_ok(), "Valid format should parse correctly");

    // Test invalid format - missing @
    let invalid_no_at =
        "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN/ip4/192.168.1.100/tcp/4001";
    let result = wormfs::networking::BootstrapPeer::parse(invalid_no_at);
    assert!(result.is_err(), "Should reject format without @");

    // Test invalid format - bad peer ID
    let invalid_peer_id = "invalid_peer_id@/ip4/192.168.1.100/tcp/4001";
    let result = wormfs::networking::BootstrapPeer::parse(invalid_peer_id);
    assert!(result.is_err(), "Should reject invalid peer ID");

    // Test invalid format - bad multiaddr
    let invalid_multiaddr = "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN@not-a-multiaddr";
    let result = wormfs::networking::BootstrapPeer::parse(invalid_multiaddr);
    assert!(result.is_err(), "Should reject invalid multiaddr");

    println!("✓ Bootstrap peer parsing tests passed");
}

#[tokio::test]
async fn test_bootstrap_peers_auto_dial() {
    init_tracing();

    // Create 3 nodes - one main node and two bootstrap targets
    let (mut service_b1, handle_b1) = create_test_node(6001).await.unwrap();
    let (mut service_b2, handle_b2) = create_test_node(6002).await.unwrap();

    let peer_b1_id = handle_b1.local_peer_id();
    let peer_b2_id = handle_b2.local_peer_id();

    println!("Bootstrap peer 1: {}", peer_b1_id);
    println!("Bootstrap peer 2: {}", peer_b2_id);

    // Start bootstrap nodes
    let service_b1_handle = tokio::spawn(async move { service_b1.run().await });
    let service_b2_handle = tokio::spawn(async move { service_b2.run().await });

    sleep(Duration::from_millis(500)).await;

    // Create main node with bootstrap_peers configured
    let bootstrap_peers = vec![
        format!("{}@/ip4/127.0.0.1/tcp/6001", peer_b1_id),
        format!("{}@/ip4/127.0.0.1/tcp/6002", peer_b2_id),
    ];

    let config_main = NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/6000".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers,
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: wormfs::networking::AuthenticationMode::Disabled,
            peers_file: "peers.json".to_string(),
        },
    };

    let (mut service_main, handle_main) =
        wormfs::networking::NetworkService::new(config_main.clone()).unwrap();
    service_main.start(config_main).await.unwrap();

    let service_main_handle = tokio::spawn(async move { service_main.run().await });

    // Wait for automatic connections to bootstrap peers
    println!("Waiting for automatic bootstrap connections...");
    sleep(Duration::from_secs(3)).await;

    // Verify both bootstrap peers are connected
    let peers = handle_main.list_connected_peers().await.unwrap();
    assert!(
        peers.contains(&peer_b1_id),
        "Should auto-connect to bootstrap peer 1"
    );
    assert!(
        peers.contains(&peer_b2_id),
        "Should auto-connect to bootstrap peer 2"
    );

    println!("✓ Bootstrap peers auto-dial test successful!");

    // Clean shutdown
    handle_main.shutdown().unwrap();
    handle_b1.shutdown().unwrap();
    handle_b2.shutdown().unwrap();

    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_main_handle),
        timeout(Duration::from_secs(5), service_b1_handle),
        timeout(Duration::from_secs(5), service_b2_handle)
    );
}

#[tokio::test]
async fn test_bootstrap_peers_with_invalid_addresses() {
    init_tracing();

    // Create one valid bootstrap node
    let (mut service_b1, handle_b1) = create_test_node(6101).await.unwrap();
    let peer_b1_id = handle_b1.local_peer_id();

    let service_b1_handle = tokio::spawn(async move { service_b1.run().await });
    sleep(Duration::from_millis(500)).await;

    // Configure with mix of valid and invalid bootstrap peers
    let bootstrap_peers = vec![
        format!("{}@/ip4/127.0.0.1/tcp/6101", peer_b1_id), // Valid
        "invalid_format".to_string(),                      // Invalid format
        "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN@/ip4/127.0.0.1/tcp/9999".to_string(), // Valid format, unreachable
    ];

    let config_main = NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/6100".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers,
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: wormfs::networking::AuthenticationMode::Disabled,
            peers_file: "peers.json".to_string(),
        },
    };

    let (mut service_main, handle_main) =
        wormfs::networking::NetworkService::new(config_main.clone()).unwrap();

    // Should not crash on invalid addresses
    let start_result = service_main.start(config_main).await;
    assert!(
        start_result.is_ok(),
        "Should handle invalid addresses gracefully"
    );

    let service_main_handle = tokio::spawn(async move { service_main.run().await });

    // Wait for connections
    sleep(Duration::from_secs(2)).await;

    // Should still connect to the valid bootstrap peer
    let peers = handle_main.list_connected_peers().await.unwrap();
    assert!(
        peers.contains(&peer_b1_id),
        "Should connect to valid bootstrap peer despite invalid entries"
    );

    println!("✓ Bootstrap peers with invalid addresses handled gracefully!");

    // Clean shutdown
    handle_main.shutdown().unwrap();
    handle_b1.shutdown().unwrap();

    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_main_handle),
        timeout(Duration::from_secs(5), service_b1_handle)
    );
}

#[tokio::test]
async fn test_bootstrap_peer_tracking() {
    init_tracing();

    // Create bootstrap node
    let (mut service_b1, handle_b1) = create_test_node(6201).await.unwrap();
    let peer_b1_id = handle_b1.local_peer_id();

    let service_b1_handle = tokio::spawn(async move { service_b1.run().await });
    sleep(Duration::from_millis(500)).await;

    // Create main node with bootstrap peer
    let bootstrap_peers = vec![format!("{}@/ip4/127.0.0.1/tcp/6201", peer_b1_id)];

    let config_main = NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/6200".to_string(),
        initial_peers: Vec::new(),
        bootstrap_peers,
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: wormfs::networking::AuthenticationMode::Disabled,
            peers_file: "peers.json".to_string(),
        },
    };

    let (mut service_main, handle_main) =
        wormfs::networking::NetworkService::new(config_main.clone()).unwrap();
    service_main.start(config_main).await.unwrap();

    // Check that bootstrap peer is tracked
    assert!(
        service_main.is_bootstrap_peer(&peer_b1_id),
        "Should track peer as bootstrap peer"
    );

    let bootstrap_list = service_main.get_bootstrap_peers();
    assert_eq!(bootstrap_list.len(), 1, "Should have 1 bootstrap peer");
    assert!(bootstrap_list.contains(&peer_b1_id));

    println!("✓ Bootstrap peer tracking test successful!");

    // Clean shutdown
    handle_main.shutdown().unwrap();
    handle_b1.shutdown().unwrap();

    let service_main_handle = tokio::spawn(async move { service_main.run().await });

    let _ = tokio::join!(
        timeout(Duration::from_secs(5), service_main_handle),
        timeout(Duration::from_secs(5), service_b1_handle)
    );
}
