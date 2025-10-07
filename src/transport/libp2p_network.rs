//! libp2p-based network transport for Raft consensus
//!
//! This module provides a libp2p network implementation that supports:
//! - TCP transport with noise encryption
//! - Request-response protocol for Raft RPCs
//! - Static peer configuration
//! - Connection management and automatic reconnection

use super::protocol::{RaftCodec, RaftProtocol};
use super::{PeerInfo, Result, TransportError};
use crate::raft::proto_types::proto::{RaftRequest, RaftResponse};
use libp2p::{
    core::upgrade,
    identity::Keypair,
    noise,
    request_response::{self, ProtocolSupport, ResponseChannel},
    swarm::{NetworkBehaviour, Swarm},
    tcp, yamux, Multiaddr, PeerId, Transport,
};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;

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

/// Network behaviour for Raft consensus
#[derive(NetworkBehaviour)]
struct RaftBehaviour {
    request_response: request_response::Behaviour<RaftCodec>,
}

/// Network events for external communication
#[derive(Debug)]
pub enum NetworkEvent {
    /// Peer connected
    PeerConnected { peer_id: PeerId, node_id: u64 },
    /// Peer disconnected
    PeerDisconnected { peer_id: PeerId, node_id: u64 },
    /// Incoming request received
    IncomingRequest {
        peer_id: PeerId,
        request: RaftRequest,
        channel: ResponseChannel<RaftResponse>,
    },
}

/// Commands to control the network
#[derive(Debug)]
pub enum NetworkCommand {
    /// Dial a peer
    Dial { node_id: u64, address: Multiaddr },
    /// Send a request
    SendRequest {
        peer_id: PeerId,
        request: RaftRequest,
        response_tx: tokio::sync::oneshot::Sender<Result<RaftResponse>>,
    },
    /// Send a response
    SendResponse {
        channel: ResponseChannel<RaftResponse>,
        response: RaftResponse,
    },
}

/// libp2p network transport implementation
pub struct Libp2pNetwork {
    swarm: Swarm<RaftBehaviour>,
    local_peer_id: PeerId,
    config: NetworkConfig,
    peer_addresses: HashMap<u64, String>,
    peer_id_to_node_id: HashMap<PeerId, u64>,
    node_id_to_peer_id: HashMap<u64, PeerId>,
    event_tx: mpsc::UnboundedSender<NetworkEvent>,
    command_rx: mpsc::UnboundedReceiver<NetworkCommand>,
}

impl Libp2pNetwork {
    /// Create a new libp2p network instance
    pub fn new(
        config: NetworkConfig,
    ) -> Result<(
        Self,
        mpsc::UnboundedReceiver<NetworkEvent>,
        mpsc::UnboundedSender<NetworkCommand>,
    )> {
        config.validate()?;

        // Generate identity keypair
        let local_key = Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());

        tracing::info!(
            "Node {} initialized with PeerId: {}",
            config.node_id,
            local_peer_id
        );

        // Build peer address map
        let peer_addresses: HashMap<u64, String> = config
            .peers
            .iter()
            .map(|p| (p.node_id, p.address.clone()))
            .collect();

        // Create TCP transport with noise encryption and yamux multiplexing
        let tcp_transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true));

        let transport = tcp_transport
            .upgrade(upgrade::Version::V1)
            .authenticate(noise::Config::new(&local_key).expect("Failed to create noise config"))
            .multiplex(yamux::Config::default())
            .boxed();

        // Configure request-response behavior
        let request_response = request_response::Behaviour::new(
            [(RaftProtocol, ProtocolSupport::Full)],
            request_response::Config::default().with_request_timeout(config.request_timeout()),
        );

        // Create network behaviour
        let behaviour = RaftBehaviour { request_response };

        // Build swarm
        let swarm_config = libp2p::swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(Duration::from_secs(60));

        let swarm = Swarm::new(transport, behaviour, local_peer_id, swarm_config);

        // Create event and command channels
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let network = Self {
            swarm,
            local_peer_id,
            config,
            peer_addresses,
            peer_id_to_node_id: HashMap::new(),
            node_id_to_peer_id: HashMap::new(),
            event_tx,
            command_rx,
        };

        Ok((network, event_rx, command_tx))
    }

    /// Start listening on the configured address
    pub fn start_listening(&mut self) -> Result<()> {
        let addr: Multiaddr = self
            .config
            .listen_address
            .parse()
            .map_err(|e| TransportError::Config(format!("Invalid listen address: {}", e)))?;

        self.swarm
            .listen_on(addr.clone())
            .map_err(|e| TransportError::Network(format!("Failed to listen: {}", e)))?;

        tracing::info!(
            "Node {} listening on {} (PeerId: {})",
            self.config.node_id,
            self.config.listen_address,
            self.local_peer_id
        );

        Ok(())
    }

    /// Get the local peer ID
    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    /// Get the local node ID
    pub fn local_node_id(&self) -> u64 {
        self.config.node_id
    }

    /// Map NodeID to PeerID
    fn node_id_to_peer_id(&self, node_id: u64) -> Result<PeerId> {
        self.node_id_to_peer_id
            .get(&node_id)
            .copied()
            .ok_or_else(|| {
                TransportError::Network(format!("No PeerId mapping for node {}", node_id))
            })
    }

    /// Map PeerID to NodeID
    fn peer_id_to_node_id(&self, peer_id: &PeerId) -> Option<u64> {
        self.peer_id_to_node_id.get(peer_id).copied()
    }

    /// Register a peer ID mapping
    fn register_peer(&mut self, peer_id: PeerId, node_id: u64) {
        self.peer_id_to_node_id.insert(peer_id, node_id);
        self.node_id_to_peer_id.insert(node_id, peer_id);
        tracing::debug!(
            "Registered mapping: Node {} <-> PeerId {}",
            node_id,
            peer_id
        );
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
