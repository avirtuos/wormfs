//! Networking module for WormFS
//!
//! This module provides the NetworkService for libp2p-based peer-to-peer networking.
//! Phase 2A.1 provides minimal libp2p setup with TCP transport and basic connection handling.

use anyhow::{anyhow, Result};
use libp2p::{
    core::upgrade,
    futures::StreamExt,
    identity, noise, ping,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, Transport,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

/// Configuration for ping/heartbeat behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingConfig {
    /// Interval between ping attempts (seconds)
    #[serde(default = "default_ping_interval")]
    pub interval_secs: u64,
    /// Ping timeout (seconds)
    #[serde(default = "default_ping_timeout")]
    pub timeout_secs: u64,
    /// Maximum consecutive failures before marking unresponsive
    #[serde(default = "default_max_failures")]
    pub max_failures: u32,
}

fn default_ping_interval() -> u64 {
    15
}

fn default_ping_timeout() -> u64 {
    20
}

fn default_max_failures() -> u32 {
    3
}

impl Default for PingConfig {
    fn default() -> Self {
        Self {
            interval_secs: default_ping_interval(),
            timeout_secs: default_ping_timeout(),
            max_failures: default_max_failures(),
        }
    }
}

/// Configuration for the networking service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// The multiaddr to listen on (e.g., "/ip4/0.0.0.0/tcp/4001")
    pub listen_address: String,
    /// List of peer addresses to dial on startup (optional)
    #[serde(default)]
    pub initial_peers: Vec<String>,
    /// Ping/heartbeat configuration
    #[serde(default)]
    pub ping: PingConfig,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            listen_address: "/ip4/0.0.0.0/tcp/4001".to_string(),
            initial_peers: Vec::new(),
            ping: PingConfig::default(),
        }
    }
}

/// Events emitted by the NetworkService
#[derive(Debug, Clone)]
pub enum NetworkEvent {
    /// A peer connection was established
    ConnectionEstablished { peer_id: PeerId },
    /// A peer connection was closed
    ConnectionClosed { peer_id: PeerId },
    /// A ping succeeded with the given RTT
    PingSuccess { peer_id: PeerId, rtt: StdDuration },
    /// A ping failed to a peer
    PingFailure { peer_id: PeerId },
    /// An error occurred in the network layer
    Error { message: String },
}

/// Commands that can be sent to the NetworkService
#[derive(Debug)]
pub enum NetworkCommand {
    /// Dial a peer at the given address
    Dial {
        address: Multiaddr,
        response: oneshot::Sender<Result<()>>,
    },
    /// Dial multiple peers concurrently
    DialMany {
        addresses: Vec<Multiaddr>,
        response: oneshot::Sender<Vec<Result<()>>>,
    },
    /// Disconnect from a specific peer
    Disconnect {
        peer_id: PeerId,
        response: oneshot::Sender<Result<()>>,
    },
    /// Get the list of connected peers
    ListPeers {
        response: oneshot::Sender<Vec<PeerId>>,
    },
    /// Shutdown the service
    Shutdown,
}

/// Statistics for a connected peer
#[derive(Debug, Clone)]
pub struct PeerStats {
    /// Last measured RTT
    pub last_rtt: Option<StdDuration>,
    /// Number of consecutive ping failures
    pub consecutive_failures: u32,
    /// Last time a successful ping was received
    pub last_seen: Instant,
}

impl PeerStats {
    fn new() -> Self {
        Self {
            last_rtt: None,
            consecutive_failures: 0,
            last_seen: Instant::now(),
        }
    }
}

/// Custom network behaviour with ping support
#[derive(NetworkBehaviour)]
pub struct WormFSBehaviour {
    ping: ping::Behaviour,
}

impl WormFSBehaviour {
    fn new(config: &PingConfig) -> Self {
        let ping_config = ping::Config::new()
            .with_interval(StdDuration::from_secs(config.interval_secs))
            .with_timeout(StdDuration::from_secs(config.timeout_secs));

        Self {
            ping: ping::Behaviour::new(ping_config),
        }
    }
}

/// The main networking service
pub struct NetworkService {
    /// The libp2p swarm
    swarm: Swarm<WormFSBehaviour>,
    /// Channel for receiving commands
    command_rx: mpsc::UnboundedReceiver<NetworkCommand>,
    /// Channel for sending events
    event_tx: mpsc::UnboundedSender<NetworkEvent>,
    /// Set of connected peers
    connected_peers: HashSet<PeerId>,
    /// Peer statistics for tracking RTT and failures
    peer_stats: HashMap<PeerId, PeerStats>,
    /// Local peer ID
    local_peer_id: PeerId,
    /// Ping configuration
    ping_config: PingConfig,
}

impl NetworkService {
    /// Create a new NetworkService
    pub fn new(config: NetworkConfig) -> Result<(Self, NetworkServiceHandle)> {
        // Generate identity keypair
        let local_key = identity::Keypair::generate_ed25519();
        let local_peer_id = PeerId::from(local_key.public());

        info!("Local peer ID: {}", local_peer_id);

        // Build transport with TCP + Noise + Yamux
        let transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true))
            .upgrade(upgrade::Version::V1)
            .authenticate(noise::Config::new(&local_key)?)
            .multiplex(yamux::Config::default())
            .boxed();

        // Create behaviour with ping
        let behaviour = WormFSBehaviour::new(&config.ping);
        let swarm_config = libp2p::swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(std::time::Duration::from_secs(60));
        let swarm = Swarm::new(transport, behaviour, local_peer_id, swarm_config);

        // Create channels for communication
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let service = NetworkService {
            swarm,
            command_rx,
            event_tx,
            connected_peers: HashSet::new(),
            peer_stats: HashMap::new(),
            local_peer_id,
            ping_config: config.ping,
        };

        let handle = NetworkServiceHandle {
            command_tx,
            event_rx,
            local_peer_id,
        };

        Ok((service, handle))
    }

    /// Start the service and begin listening
    pub async fn start(&mut self, config: NetworkConfig) -> Result<()> {
        // Parse listen address
        let listen_addr: Multiaddr = config
            .listen_address
            .parse()
            .map_err(|e| anyhow!("Invalid listen address '{}': {}", config.listen_address, e))?;

        // Start listening
        self.swarm.listen_on(listen_addr.clone())?;
        info!("NetworkService listening on: {}", listen_addr);

        Ok(())
    }

    /// Run the main event loop
    pub async fn run(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                // Handle swarm events
                event = self.swarm.next() => {
                    if let Some(event) = event {
                        self.handle_swarm_event(event).await;
                    }
                }
                // Handle commands
                command = self.command_rx.recv() => {
                    match command {
                        Some(cmd) => {
                            if !self.handle_command(cmd).await {
                                break; // Shutdown requested
                            }
                        }
                        None => {
                            warn!("Command channel closed, shutting down");
                            break;
                        }
                    }
                }
            }
        }

        info!("NetworkService shutting down");
        Ok(())
    }

    /// Handle swarm events
    async fn handle_swarm_event(
        &mut self,
        event: SwarmEvent<<WormFSBehaviour as NetworkBehaviour>::ToSwarm>,
    ) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on: {}", address);
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                info!("Connection established with peer: {}", peer_id);
                self.connected_peers.insert(peer_id);
                // Initialize peer stats
                self.peer_stats.insert(peer_id, PeerStats::new());
                let _ = self
                    .event_tx
                    .send(NetworkEvent::ConnectionEstablished { peer_id });
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                info!("Connection closed with peer: {}", peer_id);
                self.connected_peers.remove(&peer_id);
                // Remove peer stats
                self.peer_stats.remove(&peer_id);
                let _ = self
                    .event_tx
                    .send(NetworkEvent::ConnectionClosed { peer_id });
            }
            SwarmEvent::Behaviour(WormFSBehaviourEvent::Ping(ping_event)) => {
                // Handle ping events
                match ping_event {
                    ping::Event {
                        peer,
                        result: Ok(rtt),
                        ..
                    } => {
                        debug!("Ping success to {}: {:?}", peer, rtt);
                        // Update peer stats
                        if let Some(stats) = self.peer_stats.get_mut(&peer) {
                            stats.last_rtt = Some(rtt);
                            stats.consecutive_failures = 0;
                            stats.last_seen = Instant::now();
                        }
                        let _ = self
                            .event_tx
                            .send(NetworkEvent::PingSuccess { peer_id: peer, rtt });
                    }
                    ping::Event {
                        peer,
                        result: Err(err),
                        ..
                    } => {
                        warn!("Ping failure to {}: {:?}", peer, err);
                        // Update failure count
                        if let Some(stats) = self.peer_stats.get_mut(&peer) {
                            stats.consecutive_failures += 1;
                            if stats.consecutive_failures >= self.ping_config.max_failures {
                                info!(
                                    "Peer {} marked as unresponsive after {} failures",
                                    peer, stats.consecutive_failures
                                );
                            }
                        }
                        let _ = self
                            .event_tx
                            .send(NetworkEvent::PingFailure { peer_id: peer });
                    }
                }
            }
            SwarmEvent::IncomingConnection { .. } => {
                debug!("Incoming connection");
            }
            SwarmEvent::IncomingConnectionError { error, .. } => {
                warn!("Incoming connection error: {}", error);
                let _ = self.event_tx.send(NetworkEvent::Error {
                    message: format!("Incoming connection error: {}", error),
                });
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                warn!("Outgoing connection error to {:?}: {}", peer_id, error);
                let _ = self.event_tx.send(NetworkEvent::Error {
                    message: format!("Outgoing connection error: {}", error),
                });
            }
            SwarmEvent::ListenerClosed { .. } => {
                warn!("Listener closed");
            }
            SwarmEvent::ListenerError { error, .. } => {
                error!("Listener error: {}", error);
                let _ = self.event_tx.send(NetworkEvent::Error {
                    message: format!("Listener error: {}", error),
                });
            }
            _ => {
                // Ignore other events for now
                debug!("Unhandled swarm event: {:?}", event);
            }
        }
    }

    /// Handle commands, returns false if shutdown was requested
    async fn handle_command(&mut self, command: NetworkCommand) -> bool {
        match command {
            NetworkCommand::Dial { address, response } => {
                debug!("Dialing peer at: {}", address);
                match self.swarm.dial(address.clone()) {
                    Ok(_) => {
                        let _ = response.send(Ok(()));
                    }
                    Err(e) => {
                        warn!("Failed to dial {}: {}", address, e);
                        let _ = response.send(Err(anyhow!("Failed to dial {}: {}", address, e)));
                    }
                }
            }
            NetworkCommand::DialMany {
                addresses,
                response,
            } => {
                debug!("Dialing {} peers concurrently", addresses.len());
                let mut results = Vec::new();
                for address in addresses {
                    let result = match self.swarm.dial(address.clone()) {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            warn!("Failed to dial {}: {}", address, e);
                            Err(anyhow!("Failed to dial {}: {}", address, e))
                        }
                    };
                    results.push(result);
                }
                let _ = response.send(results);
            }
            NetworkCommand::Disconnect { peer_id, response } => {
                debug!("Disconnecting from peer: {}", peer_id);
                match self.swarm.disconnect_peer_id(peer_id) {
                    Ok(()) => {
                        let _ = response.send(Ok(()));
                    }
                    Err(()) => {
                        warn!("Failed to disconnect from {}", peer_id);
                        let _ = response.send(Err(anyhow!("Failed to disconnect from peer")));
                    }
                }
            }
            NetworkCommand::ListPeers { response } => {
                let peers: Vec<PeerId> = self.connected_peers.iter().cloned().collect();
                let _ = response.send(peers);
            }
            NetworkCommand::Shutdown => {
                info!("Shutdown command received");
                return false;
            }
        }
        true
    }

    /// Get the local peer ID
    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }
}

/// Handle for interacting with the NetworkService
#[derive(Debug)]
pub struct NetworkServiceHandle {
    command_tx: mpsc::UnboundedSender<NetworkCommand>,
    event_rx: mpsc::UnboundedReceiver<NetworkEvent>,
    local_peer_id: PeerId,
}

impl NetworkServiceHandle {
    /// Dial a peer at the given address
    pub async fn dial(&self, address: Multiaddr) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_tx.send(NetworkCommand::Dial {
            address,
            response: tx,
        })?;
        rx.await?
    }

    /// Dial multiple peers concurrently
    pub async fn dial_many(&self, addresses: Vec<Multiaddr>) -> Result<Vec<Result<()>>> {
        let (tx, rx) = oneshot::channel();
        self.command_tx.send(NetworkCommand::DialMany {
            addresses,
            response: tx,
        })?;
        Ok(rx.await?)
    }

    /// Disconnect from a specific peer
    pub async fn disconnect(&self, peer_id: PeerId) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_tx.send(NetworkCommand::Disconnect {
            peer_id,
            response: tx,
        })?;
        rx.await?
    }

    /// Get the list of connected peers
    pub async fn list_connected_peers(&self) -> Result<Vec<PeerId>> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(NetworkCommand::ListPeers { response: tx })?;
        Ok(rx.await?)
    }

    /// Shutdown the service
    pub fn shutdown(&self) -> Result<()> {
        self.command_tx.send(NetworkCommand::Shutdown)?;
        Ok(())
    }

    /// Receive network events
    pub async fn next_event(&mut self) -> Option<NetworkEvent> {
        self.event_rx.recv().await
    }

    /// Get the local peer ID
    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_service_creation() {
        let config = NetworkConfig::default();
        let result = NetworkService::new(config);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_invalid_listen_address() {
        let config = NetworkConfig {
            listen_address: "invalid-address".to_string(),
            initial_peers: Vec::new(),
            ping: PingConfig::default(),
        };
        let (mut service, _handle) = NetworkService::new(config.clone()).unwrap();
        let result = service.start(config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_local_peer_id() {
        let config = NetworkConfig::default();
        let (_service, handle) = NetworkService::new(config).unwrap();

        // Peer ID should be valid
        let peer_id = handle.local_peer_id();
        assert_ne!(peer_id.to_string(), "");
    }
}
