//! Integration tests for the NetworkService
//!
//! This module contains comprehensive tests for Phase 2A.1 including the
//! two-node communication test requested by the user.

use anyhow::Result;
use libp2p::{Multiaddr, PeerId};
use std::time::Duration;
use tokio::time::{sleep, timeout};
use wormfs::networking::{NetworkConfig, NetworkEvent, NetworkService};

/// Initialize tracing for tests
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .try_init();
}

/// Helper function to create a test node with a specific port
async fn create_test_node(
    port: u16,
) -> Result<(NetworkService, wormfs::networking::NetworkServiceHandle)> {
    let config = NetworkConfig {
        listen_address: format!("/ip4/127.0.0.1/tcp/{}", port),
    };

    let (mut service, handle) = NetworkService::new(config.clone())?;
    service.start(config).await?;

    Ok((service, handle))
}

/// Helper function to wait for a connection event
async fn wait_for_connection_event(
    handle: &mut wormfs::networking::NetworkServiceHandle,
    expected_peer: PeerId,
    timeout_duration: Duration,
) -> Result<()> {
    let result = timeout(timeout_duration, async {
        loop {
            if let Some(event) = handle.next_event().await {
                match event {
                    NetworkEvent::ConnectionEstablished { peer_id } if peer_id == expected_peer => {
                        return Ok(());
                    }
                    NetworkEvent::Error { message } => {
                        return Err(anyhow::anyhow!("Network error: {}", message));
                    }
                    _ => continue,
                }
            }
        }
    })
    .await;

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err(anyhow::anyhow!("Timeout waiting for connection event")),
    }
}

#[tokio::test]
async fn test_service_creation_and_startup() {
    init_tracing();

    let config = NetworkConfig {
        listen_address: "/ip4/127.0.0.1/tcp/4010".to_string(),
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
