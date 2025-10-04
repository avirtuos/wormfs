//! Test helper functions for networking tests
//!
//! This module provides common utilities for testing the NetworkService

use anyhow::Result;
use libp2p::{Multiaddr, PeerId};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;
use wormfs::networking::{
    AuthenticationMode, NetworkConfig, NetworkEvent, NetworkService, NetworkServiceHandle,
    PeerEntry,
};
use wormfs::peer_authorizer::PeerAuthorizer;

/// Create a test node with a specific port
#[allow(dead_code)]
pub async fn create_test_node(port: u16) -> Result<(NetworkService, NetworkServiceHandle)> {
    let config = NetworkConfig {
        listen_address: format!("/ip4/127.0.0.1/tcp/{}", port),
        initial_peers: Vec::new(),
        bootstrap_peers: Vec::new(),
        ping: wormfs::networking::PingConfig::default(),
        authentication: wormfs::networking::AuthenticationConfig {
            mode: AuthenticationMode::Disabled,
            peers_file: format!("test_peers_{}.json", port),
        },
        reconnection: wormfs::networking::ReconnectionConfig::default(),
    };

    // Create a simple PeerAuthorizer for testing (no PeerManager needed in tests)
    let known_peers = Arc::new(RwLock::new(HashMap::<PeerId, PeerEntry>::new()));
    let authorizer = PeerAuthorizer::new(
        AuthenticationMode::Disabled,
        known_peers,
        PathBuf::from(format!("test_peers_{}.json", port)),
    );

    // Create NetworkService with the authorizer
    let (mut service, handle) = NetworkService::new(config.clone(), authorizer)?;
    service.start(config).await?;

    Ok((service, handle))
}

/// Wait for a connection established event from a specific peer
pub async fn wait_for_connection_event(
    handle: &mut NetworkServiceHandle,
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

/// Wait for a connection closed event from a specific peer
pub async fn wait_for_disconnection_event(
    handle: &mut NetworkServiceHandle,
    expected_peer: PeerId,
    timeout_duration: Duration,
) -> Result<()> {
    let result = timeout(timeout_duration, async {
        loop {
            if let Some(event) = handle.next_event().await {
                match event {
                    NetworkEvent::ConnectionClosed { peer_id } if peer_id == expected_peer => {
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
        Err(_) => Err(anyhow::anyhow!("Timeout waiting for disconnection event")),
    }
}

/// Assert that a peer is in the connected peers list
#[allow(dead_code)]
pub async fn assert_peer_connected(handle: &NetworkServiceHandle, peer_id: PeerId) -> Result<()> {
    let peers = handle.list_connected_peers().await?;
    if !peers.contains(&peer_id) {
        return Err(anyhow::anyhow!(
            "Expected peer {} to be connected, but it's not in list: {:?}",
            peer_id,
            peers
        ));
    }
    Ok(())
}

/// Assert that a peer is NOT in the connected peers list
#[allow(dead_code)]
pub async fn assert_peer_disconnected(
    handle: &NetworkServiceHandle,
    peer_id: PeerId,
) -> Result<()> {
    let peers = handle.list_connected_peers().await?;
    if peers.contains(&peer_id) {
        return Err(anyhow::anyhow!(
            "Expected peer {} to be disconnected, but it's still in list: {:?}",
            peer_id,
            peers
        ));
    }
    Ok(())
}

/// Create a multiaddr for localhost with the given port
pub fn localhost_multiaddr(port: u16) -> Multiaddr {
    format!("/ip4/127.0.0.1/tcp/{}", port)
        .parse()
        .expect("Invalid multiaddr")
}
