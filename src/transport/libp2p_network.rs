//! libp2p-based network transport for Raft consensus
//!
//! This module provides a libp2p network implementation that supports:
//! - TCP transport with noise encryption
//! - Request-response protocol for Raft RPCs
//! - Static peer configuration
//! - Connection management and automatic reconnection

use super::{PeerInfo, Result, TransportError};
use std::collections::HashMap;
use std::time::Duration;

/// Network configuration for libp2p transport
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NetworkConfig {
    /// This node's ID
    pub node_id: u64,

    /// Listen address (e.g., "/ip4/0.0.0.0/tcp/3000")
    pub listen_address: String,

    /// Static peer list
    pub peers: Vec<PeerInfo>,

    /// Request timeout in milliseconds
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,

    /// Connection timeout in milliseconds
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_ms: u64,

    /// Maximum number of connection retries
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

fn default_request_timeout() -> u64 {
    5000 // 5 seconds
}

fn default_connection_timeout() -> u64 {
    10000 // 10 seconds
}

fn default_max_retries() -> u32 {
    3
}

impl NetworkConfig {
    /// Create a new network configuration
    pub fn new(node_id: u64, listen_address: String, peers: Vec<PeerInfo>) -> Self {
        Self {
            node_id,
            listen_address,
            peers,
            request_timeout_ms: default_request_timeout(),
            connection_timeout_ms: default_connection_timeout(),
            max_retries: default_max_retries(),
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.listen_address.is_empty() {
            return Err(TransportError::Config(
                "listen_address cannot be empty".to_string(),
            ));
        }

        if self.request_timeout_ms == 0 {
            return Err(TransportError::Config(
                "request_timeout_ms must be > 0".to_string(),
            ));
        }

        Ok(())
    }

    /// Get request timeout as Duration
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    /// Get connection timeout as Duration
    pub fn connection_timeout(&self) -> Duration {
        Duration::from_millis(self.connection_timeout_ms)
    }
}

/// libp2p network transport implementation
///
/// This will be implemented in subsequent iterations to provide:
/// - Raft RPC handling (AppendEntries, Vote, InstallSnapshot)
/// - Connection pooling and management
/// - Automatic peer discovery from static configuration
/// - Health monitoring and reconnection
pub struct Libp2pNetwork {
    config: NetworkConfig,
    _peer_addresses: HashMap<u64, String>,
}

impl Libp2pNetwork {
    /// Create a new libp2p network instance
    pub fn new(config: NetworkConfig) -> Result<Self> {
        config.validate()?;

        // Build peer address map
        let peer_addresses: HashMap<u64, String> = config
            .peers
            .iter()
            .map(|p| (p.node_id, p.address.clone()))
            .collect();

        Ok(Self {
            config,
            _peer_addresses: peer_addresses,
        })
    }

    /// Start the network transport
    pub async fn start(&mut self) -> Result<()> {
        // TODO: Initialize libp2p transport
        // TODO: Set up request-response protocol
        // TODO: Start listening on configured address
        // TODO: Connect to configured peers
        tracing::info!(
            "Starting libp2p network on {} for node {}",
            self.config.listen_address,
            self.config.node_id
        );

        Ok(())
    }

    /// Stop the network transport
    pub async fn stop(&mut self) -> Result<()> {
        tracing::info!("Stopping libp2p network for node {}", self.config.node_id);
        Ok(())
    }

    /// Get the network configuration
    pub fn config(&self) -> &NetworkConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_config_validation() {
        let config = NetworkConfig::new(1, "/ip4/127.0.0.1/tcp/3000".to_string(), vec![]);
        assert!(config.validate().is_ok());

        let invalid_config = NetworkConfig::new(1, "".to_string(), vec![]);
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_network_config_timeouts() {
        let config = NetworkConfig::new(1, "/ip4/127.0.0.1/tcp/3000".to_string(), vec![]);

        assert_eq!(config.request_timeout(), Duration::from_millis(5000));
        assert_eq!(config.connection_timeout(), Duration::from_millis(10000));
    }
}
