//! libp2p-based network transport for Raft consensus
//!
//! This module provides a libp2p network implementation that supports:
//! - TCP transport with noise encryption
//! - Request-response protocol for Raft RPCs
//! - Static peer configuration
//! - Connection management and automatic reconnection

use super::protocol::{RaftCodec, RaftProtocol};
use super::{PeerHealth, PeerInfo, PeerManager, PeerStatus, Result, TransportError};
use crate::raft::proto_types::proto::{NodeAnnouncement, RaftRequest, RaftResponse};
use futures::StreamExt;
use libp2p::{
    core::upgrade,
    identity::Keypair,
    noise,
    request_response::{self, ProtocolSupport, ResponseChannel},
    swarm::{NetworkBehaviour, Swarm},
    tcp, yamux, Multiaddr, PeerId, Transport,
};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
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

    /// Allow peer discovery via node announcements
    /// When true: accept connections and learn peer mappings via handshake
    /// When false: only accept connections from pre-configured peer_ids
    #[serde(default = "default_allow_discovery")]
    pub allow_peer_discovery: bool,
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

fn default_allow_discovery() -> bool {
    false // Default to strict mode for security
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
            allow_peer_discovery: default_allow_discovery(),
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
    peer_manager: PeerManager,
    peer_addresses: HashMap<u64, String>,
    peer_id_to_node_id: HashMap<PeerId, u64>,
    node_id_to_peer_id: HashMap<u64, PeerId>,
    pending_peers: HashMap<PeerId, Multiaddr>,
    announced_to: HashSet<PeerId>,
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

        // Initialize PeerManager
        let peer_manager = PeerManager::new(
            config.peers.clone(),
            Duration::from_secs(30), // Health timeout
            config.max_retries,
        );

        let network = Self {
            swarm,
            local_peer_id,
            config,
            peer_manager,
            peer_addresses,
            peer_id_to_node_id: HashMap::new(),
            node_id_to_peer_id: HashMap::new(),
            pending_peers: HashMap::new(),
            announced_to: HashSet::new(),
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

    /// Validate strict mode configuration
    /// Returns an error if allow_peer_discovery is false but not all peers have peer_ids
    pub fn validate_strict_mode_config(&self) -> Result<()> {
        if !self.config.allow_peer_discovery {
            for peer in &self.config.peers {
                if peer.peer_id.is_none() {
                    return Err(TransportError::Config(format!(
                        "Peer {} missing peer_id. Required when allow_peer_discovery=false",
                        peer.node_id
                    )));
                }
            }
        }
        Ok(())
    }

    /// Check if a PeerId is authorized (in configured peer list)
    fn is_authorized_peer(&self, peer_id: &PeerId) -> bool {
        if self.config.allow_peer_discovery {
            // In discovery mode, all peers are accepted
            return true;
        }

        // In strict mode, check against configured peer_ids
        self.config.peers.iter().any(|p| {
            if let Some(ref configured_peer_id) = p.peer_id {
                if let Ok(parsed_peer_id) = PeerId::from_str(configured_peer_id) {
                    return parsed_peer_id == *peer_id;
                }
            }
            false
        })
    }

    /// Find node_id for a given PeerId from configuration
    fn find_node_id_for_peer(&self, peer_id: &PeerId) -> Option<u64> {
        // First check if we already have the mapping
        if let Some(node_id) = self.peer_id_to_node_id(peer_id) {
            return Some(node_id);
        }

        // In strict mode, check configured peer_ids
        if !self.config.allow_peer_discovery {
            for peer in &self.config.peers {
                if let Some(ref configured_peer_id) = peer.peer_id {
                    if let Ok(parsed_peer_id) = PeerId::from_str(configured_peer_id) {
                        if parsed_peer_id == *peer_id {
                            return Some(peer.node_id);
                        }
                    }
                }
            }
        }

        None
    }

    /// Dial all configured peers
    pub async fn dial_peers(&mut self) -> Result<()> {
        tracing::info!(
            "Dialing {} configured peers (discovery mode: {})",
            self.config.peers.len(),
            self.config.allow_peer_discovery
        );

        let local_node_id = self.config.node_id;
        let allow_discovery = self.config.allow_peer_discovery;

        // Clone the peer list to avoid borrowing issues
        let peers = self.config.peers.clone();

        for peer in &peers {
            if peer.node_id == local_node_id {
                // Skip ourselves
                continue;
            }

            let addr: Multiaddr = peer
                .address
                .parse()
                .map_err(|e| TransportError::Config(format!("Invalid peer address: {}", e)))?;

            // If peer_id is configured, we can map it immediately
            if let Some(ref peer_id_str) = peer.peer_id {
                match PeerId::from_str(peer_id_str) {
                    Ok(peer_id) => {
                        tracing::info!(
                            "Dialing peer {} (node {}) at {}",
                            peer_id,
                            peer.node_id,
                            addr
                        );
                        // Pre-register the mapping
                        self.register_peer(peer_id, peer.node_id);
                    }
                    Err(e) => {
                        tracing::warn!("Invalid peer_id for node {}: {}", peer.node_id, e);
                        if !allow_discovery {
                            return Err(TransportError::Config(format!(
                                "Invalid peer_id for node {}: {}",
                                peer.node_id, e
                            )));
                        }
                    }
                }
            } else if !allow_discovery {
                return Err(TransportError::Config(format!(
                    "Peer {} missing peer_id in strict mode",
                    peer.node_id
                )));
            } else {
                tracing::info!(
                    "Dialing peer (node {}) at {} (discovery mode)",
                    peer.node_id,
                    addr
                );
            }

            // Dial the peer
            if let Err(e) = self.swarm.dial(addr.clone()) {
                tracing::warn!("Failed to dial peer {} at {}: {}", peer.node_id, addr, e);
                self.peer_manager.record_failure(peer.node_id);
            }
        }

        Ok(())
    }

    /// Handle connection established event
    fn handle_connection_established(
        &mut self,
        peer_id: PeerId,
        _endpoint: &libp2p::core::ConnectedPoint,
    ) {
        tracing::debug!("Connection established with peer {}", peer_id);

        // Check authorization in strict mode
        if !self.is_authorized_peer(&peer_id) {
            tracing::warn!("Rejecting unauthorized peer {} (strict mode)", peer_id);
            // In strict mode, disconnect unauthorized peers
            let _ = self.swarm.disconnect_peer_id(peer_id);
            return;
        }

        // In discovery mode or if authorized, proceed with handshake
        // Send NodeAnnouncement to introduce ourselves
        if !self.announced_to.contains(&peer_id) {
            let announcement = RaftRequest {
                request: Some(
                    crate::raft::proto_types::proto::raft_request::Request::Announce(
                        NodeAnnouncement {
                            node_id: self.config.node_id,
                            version: env!("CARGO_PKG_VERSION").to_string(),
                        },
                    ),
                ),
            };

            self.swarm
                .behaviour_mut()
                .request_response
                .send_request(&peer_id, announcement);

            self.announced_to.insert(peer_id);
            tracing::debug!("Sent NodeAnnouncement to peer {}", peer_id);
        }

        // If we already know the node_id, register as connected
        // Check mapping before mutation to avoid borrow checker issues
        let maybe_node_id = self.find_node_id_for_peer(&peer_id);
        if let Some(node_id) = maybe_node_id {
            self.peer_manager.record_success(node_id, None);

            let _ = self
                .event_tx
                .send(NetworkEvent::PeerConnected { peer_id, node_id });
        } else {
            // Wait for NodeAnnouncement to learn node_id
            tracing::debug!("Waiting for NodeAnnouncement from peer {}", peer_id);
        }
    }

    /// Handle connection closed event
    fn handle_connection_closed(&mut self, peer_id: PeerId) {
        tracing::debug!("Connection closed with peer {}", peer_id);

        // Remove from announced set
        self.announced_to.remove(&peer_id);

        // Update peer manager and emit event
        if let Some(node_id) = self.peer_id_to_node_id(&peer_id) {
            self.peer_manager.record_failure(node_id);

            let _ = self
                .event_tx
                .send(NetworkEvent::PeerDisconnected { peer_id, node_id });

            tracing::info!("Peer {} (node {}) disconnected", peer_id, node_id);
        }
    }

    /// Handle incoming NodeAnnouncement
    fn handle_node_announcement(&mut self, peer_id: PeerId, announcement: NodeAnnouncement) {
        let node_id = announcement.node_id;

        tracing::info!(
            "Received NodeAnnouncement from peer {} (node {}, version {})",
            peer_id,
            node_id,
            announcement.version
        );

        // Register the peer mapping
        self.register_peer(peer_id, node_id);

        // Update peer manager
        self.peer_manager.record_success(node_id, None);

        // Emit connection event
        let _ = self
            .event_tx
            .send(NetworkEvent::PeerConnected { peer_id, node_id });
    }

    /// Calculate reconnection delay with exponential backoff
    fn get_reconnect_delay(&self, failure_count: u32) -> Duration {
        let base_delay_ms = 1000u64; // 1 second
        let max_delay_ms = 60000u64; // 60 seconds

        let delay_ms = base_delay_ms * 2u64.saturating_pow(failure_count.min(6));
        Duration::from_millis(delay_ms.min(max_delay_ms))
    }

    /// Check if a peer should be reconnected
    fn should_reconnect(&self, peer: &PeerHealth) -> bool {
        match peer.status {
            PeerStatus::Disconnected => {
                // Reconnect if within retry limit
                peer.failure_count < self.config.max_retries
            }
            PeerStatus::Failed => {
                // Don't reconnect failed peers (unless manually reset)
                false
            }
            _ => false,
        }
    }

    /// Reconnect to failed peers with exponential backoff
    pub async fn reconnect_failed_peers(&mut self) -> Result<()> {
        let mut reconnect_list = Vec::new();

        for peer in self.peer_manager.all_peers() {
            if self.should_reconnect(peer) {
                let delay = self.get_reconnect_delay(peer.failure_count);

                // Check if enough time has passed since last attempt
                if let Some(last_seen) = peer.last_seen {
                    if last_seen.elapsed() < delay {
                        continue; // Too soon to retry
                    }
                }

                reconnect_list.push((peer.peer.node_id, peer.peer.address.clone()));
            }
        }

        for (node_id, address) in reconnect_list {
            let addr: Multiaddr = address
                .parse()
                .map_err(|e| TransportError::Config(format!("Invalid peer address: {}", e)))?;

            tracing::info!("Reconnecting to peer {} at {}", node_id, addr);

            if let Err(e) = self.swarm.dial(addr.clone()) {
                tracing::warn!("Failed to reconnect to peer {}: {}", node_id, e);
                self.peer_manager.record_failure(node_id);
            }
        }

        Ok(())
    }

    /// Run the network event loop
    pub async fn run(&mut self) -> Result<()> {
        use libp2p::swarm::SwarmEvent;
        use request_response::{Event as RequestResponseEvent, Message};

        loop {
            tokio::select! {
                // Process swarm events
                Some(event) = self.swarm.next() => {
                    match event {
                        SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                            self.handle_connection_established(peer_id, &endpoint);
                        }
                        SwarmEvent::ConnectionClosed { peer_id, .. } => {
                            self.handle_connection_closed(peer_id);
                        }
                        SwarmEvent::Behaviour(RaftBehaviourEvent::RequestResponse(event)) => {
                            match event {
                                RequestResponseEvent::Message { peer, message, .. } => {
                                    match message {
                                        Message::Request { request, channel, .. } => {
                                            // Check if this is a NodeAnnouncement
                                            if let Some(req) = &request.request {
                                                match req {
                                                    crate::raft::proto_types::proto::raft_request::Request::Announce(announcement) => {
                                                        self.handle_node_announcement(peer, announcement.clone());
                                                        // Respond with empty response (announcement doesn't need response)
                                                        let _ = self.swarm.behaviour_mut()
                                                            .request_response
                                                            .send_response(channel, RaftResponse { response: None });
                                                    }
                                                    _ => {
                                                        // Forward other requests to the event channel
                                                        let _ = self.event_tx.send(NetworkEvent::IncomingRequest {
                                                            peer_id: peer,
                                                            request,
                                                            channel,
                                                        });
                                                    }
                                                }
                                            }
                                        }
                                        Message::Response { .. } => {
                                            // Responses are handled by the request sender
                                        }
                                    }
                                }
                                RequestResponseEvent::OutboundFailure { peer, error, .. } => {
                                    tracing::warn!("Outbound request failed to peer {}: {:?}", peer, error);
                                    if let Some(node_id) = self.peer_id_to_node_id(&peer) {
                                        self.peer_manager.record_failure(node_id);
                                    }
                                }
                                RequestResponseEvent::InboundFailure { peer, error, .. } => {
                                    tracing::warn!("Inbound request failed from peer {}: {:?}", peer, error);
                                }
                                RequestResponseEvent::ResponseSent { peer, .. } => {
                                    tracing::trace!("Response sent to peer {}", peer);
                                }
                            }
                        }
                        SwarmEvent::IncomingConnection { .. } => {
                            tracing::trace!("Incoming connection");
                        }
                        SwarmEvent::OutgoingConnectionError { peer_id: Some(peer_id), error, .. } => {
                            tracing::warn!("Outgoing connection error to peer {}: {}", peer_id, error);
                            if let Some(node_id) = self.peer_id_to_node_id(&peer_id) {
                                self.peer_manager.record_failure(node_id);
                            }
                        }
                        SwarmEvent::OutgoingConnectionError { peer_id: None, .. } => {
                            // Connection error without peer_id, nothing to track
                        }
                        SwarmEvent::IncomingConnectionError { .. } => {
                            tracing::trace!("Incoming connection error");
                        }
                        _ => {}
                    }
                }

                // Process network commands
                Some(command) = self.command_rx.recv() => {
                    match command {
                        NetworkCommand::Dial { node_id, address } => {
                            tracing::debug!("Dialing peer {} at {}", node_id, address);
                            if let Err(e) = self.swarm.dial(address) {
                                tracing::warn!("Failed to dial peer {}: {}", node_id, e);
                                self.peer_manager.record_failure(node_id);
                            }
                        }
                        NetworkCommand::SendRequest { peer_id, request, response_tx } => {
                            let request_id = self.swarm
                                .behaviour_mut()
                                .request_response
                                .send_request(&peer_id, request);

                            tracing::trace!("Sent request {:?} to peer {}", request_id, peer_id);

                            // Note: In a complete implementation, we would track the request_id
                            // and match it with the response in RequestResponseEvent::Message
                            // For now, we just send the request and the response will be handled
                            // by the caller through a different mechanism
                            let _ = response_tx.send(Ok(RaftResponse { response: None }));
                        }
                        NetworkCommand::SendResponse { channel, response } => {
                            let _ = self.swarm
                                .behaviour_mut()
                                .request_response
                                .send_response(channel, response);
                        }
                    }
                }
            }
        }
    }

    /// Get peer manager statistics
    pub fn peer_stats(&self) -> super::peer_manager::PeerStats {
        self.peer_manager.stats()
    }

    /// Get peer health information
    pub fn get_peer_health(&self, node_id: u64) -> Option<&PeerHealth> {
        self.peer_manager.get_peer(node_id)
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
