//! Test helper functions for networking tests
//!
//! This module provides common utilities for testing the NetworkService

#![allow(dead_code)]

use anyhow::Result;
use libp2p::{Multiaddr, PeerId};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use wormfs::networking::{
    AuthenticationMode, NetworkConfig, NetworkEvent, NetworkService, NetworkServiceHandle,
    PeerEntry, PingConfig, ReconnectionConfig,
};
use wormfs::peer_authorizer::PeerAuthorizer;

/// Create a test node with a specific port
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

/// Represents a managed test node
pub struct TestNode {
    pub peer_id: PeerId,
    pub port: u16,
    pub handle: NetworkServiceHandle,
    pub service_handle: JoinHandle<Result<()>>,
}

impl TestNode {
    /// Create a new test node with the given port and config
    pub async fn new(port: u16, config: NetworkConfig) -> Result<Self> {
        let known_peers = Arc::new(RwLock::new(HashMap::<PeerId, PeerEntry>::new()));
        let authorizer = PeerAuthorizer::new(
            config.authentication.mode.clone(),
            known_peers,
            PathBuf::from(&config.authentication.peers_file),
        );

        let (mut service, handle) = NetworkService::new(config.clone(), authorizer)?;
        let peer_id = handle.local_peer_id();

        service.start(config).await?;
        let service_handle = tokio::spawn(async move { service.run().await });

        Ok(TestNode {
            peer_id,
            port,
            handle,
            service_handle,
        })
    }

    /// Shutdown this test node
    pub async fn shutdown(self) -> Result<()> {
        self.handle.shutdown()?;
        timeout(Duration::from_secs(5), self.service_handle)
            .await??
            .map_err(|e| anyhow::anyhow!("Service error: {}", e))?;
        Ok(())
    }

    /// Get the multiaddr for this node
    pub fn multiaddr(&self) -> Multiaddr {
        localhost_multiaddr(self.port)
    }
}

/// Represents a cluster of test nodes
pub struct TestCluster {
    pub nodes: Vec<TestNode>,
    pub start_port: u16,
}

impl TestCluster {
    /// Create a new test cluster with the given number of nodes
    pub async fn new(num_nodes: usize, start_port: u16) -> Result<Self> {
        let mut nodes = Vec::new();

        for i in 0..num_nodes {
            let port = start_port + i as u16;
            let config = NetworkConfig {
                listen_address: format!("/ip4/127.0.0.1/tcp/{}", port),
                initial_peers: Vec::new(),
                bootstrap_peers: Vec::new(),
                ping: PingConfig::default(),
                authentication: wormfs::networking::AuthenticationConfig {
                    mode: AuthenticationMode::Disabled,
                    peers_file: format!("test_peers_{}.json", port),
                },
                reconnection: ReconnectionConfig::default(),
            };

            let node = TestNode::new(port, config).await?;
            nodes.push(node);
        }

        // Give nodes time to start
        sleep(Duration::from_millis(500)).await;

        Ok(TestCluster { nodes, start_port })
    }

    /// Create a test cluster with custom configurations
    pub async fn new_with_configs(configs: Vec<NetworkConfig>) -> Result<Self> {
        let mut nodes = Vec::new();

        // Extract the actual port from each config's listen address
        for config in configs.into_iter() {
            // Parse the port from the listen address (e.g., "/ip4/127.0.0.1/tcp/10200")
            let port = config
                .listen_address
                .rsplit('/')
                .next()
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(9000);

            let node = TestNode::new(port, config).await?;
            nodes.push(node);
        }

        sleep(Duration::from_millis(500)).await;

        // Use the first node's port as start_port for reference
        let start_port = nodes.first().map(|n| n.port).unwrap_or(9000);
        Ok(TestCluster { nodes, start_port })
    }

    /// Connect all nodes in a mesh topology
    pub async fn connect_mesh(&mut self) -> Result<()> {
        let num_nodes = self.nodes.len();

        for i in 0..num_nodes {
            for j in (i + 1)..num_nodes {
                let target_addr = self.nodes[j].multiaddr();
                self.nodes[i].handle.dial(target_addr).await?;
            }
        }

        Ok(())
    }

    /// Wait for all nodes to be connected to each other (mesh topology)
    pub async fn wait_for_mesh_connectivity(&mut self, timeout_duration: Duration) -> Result<()> {
        let start = Instant::now();
        let num_nodes = self.nodes.len();
        let expected_connections = num_nodes - 1;

        while start.elapsed() < timeout_duration {
            let mut all_connected = true;

            for node in &self.nodes {
                let peers = node.handle.list_connected_peers().await?;
                if peers.len() < expected_connections {
                    all_connected = false;
                    break;
                }
            }

            if all_connected {
                return Ok(());
            }

            sleep(Duration::from_millis(100)).await;
        }

        Err(anyhow::anyhow!(
            "Timeout waiting for mesh connectivity after {:?}",
            timeout_duration
        ))
    }

    /// Get a specific node by index
    pub fn get_node(&self, index: usize) -> Option<&TestNode> {
        self.nodes.get(index)
    }

    /// Get a mutable reference to a specific node
    pub fn get_node_mut(&mut self, index: usize) -> Option<&mut TestNode> {
        self.nodes.get_mut(index)
    }

    /// Get the number of nodes in the cluster
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if the cluster is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Shutdown all nodes in the cluster
    pub async fn shutdown_all(self) -> Result<()> {
        for node in self.nodes {
            node.shutdown().await?;
        }
        Ok(())
    }

    /// Shutdown a specific node by index
    pub async fn shutdown_node(&mut self, index: usize) -> Result<()> {
        if index >= self.nodes.len() {
            return Err(anyhow::anyhow!("Node index {} out of bounds", index));
        }

        let node = self.nodes.remove(index);
        node.shutdown().await
    }

    /// Collect connectivity metrics for the cluster
    pub async fn collect_metrics(&self) -> Result<ClusterMetrics> {
        let mut total_connections = 0;
        let mut peer_counts = Vec::new();

        for node in &self.nodes {
            let peers = node.handle.list_connected_peers().await?;
            peer_counts.push(peers.len());
            total_connections += peers.len();
        }

        // Each connection is counted twice (once per node)
        let unique_connections = total_connections / 2;

        Ok(ClusterMetrics {
            num_nodes: self.nodes.len(),
            total_connections: unique_connections,
            peer_counts,
        })
    }
}

/// Cluster metrics for monitoring
#[derive(Debug, Clone)]
pub struct ClusterMetrics {
    pub num_nodes: usize,
    pub total_connections: usize,
    pub peer_counts: Vec<usize>,
}

impl ClusterMetrics {
    /// Check if the cluster is fully connected (mesh topology)
    pub fn is_fully_connected(&self) -> bool {
        if self.num_nodes == 0 {
            return true;
        }

        let expected_connections_per_node = self.num_nodes - 1;
        self.peer_counts
            .iter()
            .all(|&count| count == expected_connections_per_node)
    }

    /// Get the minimum number of connections any node has
    pub fn min_connections(&self) -> usize {
        self.peer_counts.iter().copied().min().unwrap_or(0)
    }

    /// Get the maximum number of connections any node has
    pub fn max_connections(&self) -> usize {
        self.peer_counts.iter().copied().max().unwrap_or(0)
    }

    /// Get the average number of connections per node
    pub fn avg_connections(&self) -> f64 {
        if self.num_nodes == 0 {
            return 0.0;
        }
        self.peer_counts.iter().sum::<usize>() as f64 / self.num_nodes as f64
    }
}

/// Wait for a specific number of connections on a node
pub async fn wait_for_connection_count(
    handle: &NetworkServiceHandle,
    expected_count: usize,
    timeout_duration: Duration,
) -> Result<()> {
    let start = Instant::now();

    while start.elapsed() < timeout_duration {
        let peers = handle.list_connected_peers().await?;
        if peers.len() >= expected_count {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }

    Err(anyhow::anyhow!(
        "Timeout waiting for {} connections",
        expected_count
    ))
}

/// Measure the time it takes to establish a connection
pub async fn measure_connection_time(
    handle: &mut NetworkServiceHandle,
    target_addr: Multiaddr,
    expected_peer: PeerId,
) -> Result<Duration> {
    let start = Instant::now();

    handle.dial(target_addr).await?;
    wait_for_connection_event(handle, expected_peer, Duration::from_secs(10)).await?;

    Ok(start.elapsed())
}

/// Assert that the cluster is healthy (all nodes connected)
pub async fn assert_cluster_health(cluster: &TestCluster) -> Result<()> {
    let metrics = cluster.collect_metrics().await?;

    if !metrics.is_fully_connected() {
        return Err(anyhow::anyhow!(
            "Cluster not fully connected. Min: {}, Max: {}, Avg: {:.2}",
            metrics.min_connections(),
            metrics.max_connections(),
            metrics.avg_connections()
        ));
    }

    Ok(())
}
