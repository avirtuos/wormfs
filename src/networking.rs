//! Networking module for WormFS
//!
//! This module provides the NetworkService for libp2p-based peer-to-peer networking.
//! Phase 2A.1 provides minimal libp2p setup with TCP transport and basic connection handling.

use anyhow::{anyhow, Result};
use libp2p::{
    core::upgrade,
    futures::StreamExt,
    identity, noise,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, Transport,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

/// Configuration for the networking service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// The multiaddr to listen on (e.g., "/ip4/0.0.0.0/tcp/4001")
    pub listen_address: String,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            listen_address: "/ip4/0.0.0.0/tcp/4001".to_string(),
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
    /// Get the list of connected peers
    ListPeers {
        response: oneshot::Sender<Vec<PeerId>>,
    },
    /// Shutdown the service
    Shutdown,
}

/// Empty network behaviour for minimal setup
/// Uses libp2p's dummy behaviour which provides the required ConnectionHandler
type EmptyBehaviour = libp2p::swarm::dummy::Behaviour;

/// The main networking service
pub struct NetworkService {
    /// The libp2p swarm
    swarm: Swarm<EmptyBehaviour>,
    /// Channel for receiving commands
    command_rx: mpsc::UnboundedReceiver<NetworkCommand>,
    /// Channel for sending events
    event_tx: mpsc::UnboundedSender<NetworkEvent>,
    /// Set of connected peers
    connected_peers: HashSet<PeerId>,
    /// Local peer ID
    local_peer_id: PeerId,
}

impl NetworkService {
    /// Create a new NetworkService
    pub fn new(_config: NetworkConfig) -> Result<(Self, NetworkServiceHandle)> {
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

        // Create swarm with empty behaviour
        let behaviour = libp2p::swarm::dummy::Behaviour;
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
            local_peer_id,
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
        event: SwarmEvent<<EmptyBehaviour as NetworkBehaviour>::ToSwarm>,
    ) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on: {}", address);
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                info!("Connection established with peer: {}", peer_id);
                self.connected_peers.insert(peer_id);
                let _ = self
                    .event_tx
                    .send(NetworkEvent::ConnectionEstablished { peer_id });
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                info!("Connection closed with peer: {}", peer_id);
                self.connected_peers.remove(&peer_id);
                let _ = self
                    .event_tx
                    .send(NetworkEvent::ConnectionClosed { peer_id });
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
