//! Networking module for WormFS
//!
//! This module provides the NetworkService for libp2p-based peer-to-peer networking.
//! Phase 2A.1 provides minimal libp2p setup with TCP transport and basic connection handling.

use crate::peer_authorizer::{AuthResult, PeerAuthorizer};
use anyhow::{anyhow, Result};
use libp2p::{
    core::{upgrade, ConnectedPoint},
    futures::StreamExt,
    identity,
    multiaddr::Protocol,
    noise, ping,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, Transport,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::IpAddr;
use std::time::{Duration as StdDuration, Instant, SystemTime};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

/// Authentication mode
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthenticationMode {
    /// Disabled - accept all connections (insecure, testing only)
    Disabled,
    /// Learn - accept new peers, record to peers_file, reject IP mismatches
    Learn,
    /// Enforce - only accept peers listed in peers_file
    Enforce,
}

impl Default for AuthenticationMode {
    fn default() -> Self {
        Self::Enforce // Secure by default
    }
}

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthenticationConfig {
    /// Authentication mode
    #[serde(default)]
    pub mode: AuthenticationMode,
    /// Path to peers file (used in both Learn and Enforce modes)
    #[serde(default = "default_peers_file")]
    pub peers_file: String,
}

fn default_peers_file() -> String {
    "peers.json".to_string()
}

impl Default for AuthenticationConfig {
    fn default() -> Self {
        Self {
            mode: AuthenticationMode::Enforce,
            peers_file: default_peers_file(),
        }
    }
}

/// Entry in the peers file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerEntry {
    pub peer_id: String,
    pub ip_addresses: Vec<String>,
    #[serde(with = "humantime_serde")]
    pub first_seen: SystemTime,
    #[serde(with = "humantime_serde")]
    pub last_seen: SystemTime,
    pub connection_count: u32,
    pub source: String, // "learned" or "manual"
}

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

/// Configuration for reconnection behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconnectionConfig {
    /// Whether reconnection is enabled
    #[serde(default = "default_reconnection_enabled")]
    pub enabled: bool,
    /// Initial backoff duration in seconds
    #[serde(default = "default_initial_backoff")]
    pub initial_backoff_secs: u64,
    /// Maximum backoff duration in seconds
    #[serde(default = "default_max_backoff")]
    pub max_backoff_secs: u64,
    /// Backoff multiplier (exponential backoff)
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
}

fn default_reconnection_enabled() -> bool {
    true
}

fn default_initial_backoff() -> u64 {
    5
}

fn default_max_backoff() -> u64 {
    300
}

fn default_backoff_multiplier() -> f64 {
    2.0
}

impl Default for ReconnectionConfig {
    fn default() -> Self {
        Self {
            enabled: default_reconnection_enabled(),
            initial_backoff_secs: default_initial_backoff(),
            max_backoff_secs: default_max_backoff(),
            backoff_multiplier: default_backoff_multiplier(),
        }
    }
}

/// Bootstrap peer address in format: peer_id@multiaddr
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapPeer {
    pub peer_id: PeerId,
    pub multiaddr: Multiaddr,
}

impl BootstrapPeer {
    /// Parse a bootstrap peer from the format: peer_id@multiaddr
    pub fn parse(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split('@').collect();
        if parts.len() != 2 {
            return Err(anyhow!(
                "Invalid bootstrap peer format '{}'. Expected: peer_id@multiaddr",
                s
            ));
        }

        let peer_id = parts[0]
            .parse::<PeerId>()
            .map_err(|e| anyhow!("Invalid peer ID '{}': {}", parts[0], e))?;

        let multiaddr = parts[1]
            .parse::<Multiaddr>()
            .map_err(|e| anyhow!("Invalid multiaddr '{}': {}", parts[1], e))?;

        Ok(Self { peer_id, multiaddr })
    }
}

impl fmt::Display for BootstrapPeer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.peer_id, self.multiaddr)
    }
}

/// Configuration for the networking service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// The multiaddr to listen on (e.g., "/ip4/0.0.0.0/tcp/4001")
    pub listen_address: String,
    /// List of peer addresses to dial on startup (optional, deprecated - use bootstrap_peers)
    #[serde(default)]
    pub initial_peers: Vec<String>,
    /// Bootstrap peers to connect to on startup (format: peer_id@multiaddr)
    #[serde(default)]
    pub bootstrap_peers: Vec<String>,
    /// Ping/heartbeat configuration
    #[serde(default)]
    pub ping: PingConfig,
    /// Authentication configuration
    #[serde(default)]
    pub authentication: AuthenticationConfig,
    /// Reconnection configuration
    #[serde(default)]
    pub reconnection: ReconnectionConfig,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            listen_address: "/ip4/0.0.0.0/tcp/4001".to_string(),
            initial_peers: Vec::new(),
            bootstrap_peers: Vec::new(),
            ping: PingConfig::default(),
            authentication: AuthenticationConfig::default(),
            reconnection: ReconnectionConfig::default(),
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
    /// A peer has become unresponsive (consecutive failures exceeded threshold)
    PeerUnresponsive {
        peer_id: PeerId,
        consecutive_failures: u32,
        time_since_last_seen: StdDuration,
    },
    /// Authentication failed for a peer
    AuthenticationFailed { peer_id: PeerId, reason: String },
    /// A new peer was learned (Learn mode only)
    PeerLearned { peer_id: PeerId, entry: PeerEntry },
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
    /// Get the state of a specific peer
    GetPeerState {
        peer_id: PeerId,
        response: oneshot::Sender<Option<PeerState>>,
    },
    /// Get complete info about a specific peer
    GetPeerInfo {
        peer_id: PeerId,
        response: oneshot::Sender<Option<PeerInfo>>,
    },
    /// List all peers with a specific state
    ListPeersByState {
        state: PeerState,
        response: oneshot::Sender<Vec<PeerId>>,
    },
    /// Shutdown the service
    Shutdown,
}

/// Peer connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerState {
    /// Peer is connected and responsive
    Connected,
    /// Peer was disconnected (cleanly or via network)
    Disconnected,
    /// Peer is connected but unresponsive (consecutive ping failures exceeded)
    Failed,
}

/// Complete information about a peer
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// The peer's ID
    pub peer_id: PeerId,
    /// Current state of the peer
    pub state: PeerState,
    /// Last measured RTT
    pub last_rtt: Option<StdDuration>,
    /// Number of consecutive ping failures
    pub consecutive_failures: u32,
    /// Last time a successful ping was received
    pub last_seen: Instant,
    /// When the peer connected (if currently connected)
    pub connected_at: Option<Instant>,
    /// When the peer disconnected (if currently disconnected)
    pub disconnected_at: Option<Instant>,
    /// Last successful multiaddr for this peer
    pub last_multiaddr: Option<Multiaddr>,
    /// Number of reconnection attempts
    pub retry_count: u32,
    /// Next scheduled reconnection time
    pub next_retry_time: Option<Instant>,
    /// Current backoff duration
    pub current_backoff: StdDuration,
}

impl PeerInfo {
    pub fn new(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            state: PeerState::Connected,
            last_rtt: None,
            consecutive_failures: 0,
            last_seen: Instant::now(),
            connected_at: Some(Instant::now()),
            disconnected_at: None,
            last_multiaddr: None,
            retry_count: 0,
            next_retry_time: None,
            current_backoff: StdDuration::from_secs(5), // Default initial backoff
        }
    }

    /// Update state with logging
    pub fn set_state(&mut self, new_state: PeerState) {
        if self.state != new_state {
            info!(
                "Peer {} state transition: {:?} -> {:?}",
                self.peer_id, self.state, new_state
            );
            self.state = new_state;

            match new_state {
                PeerState::Connected => {
                    self.connected_at = Some(Instant::now());
                    self.disconnected_at = None;
                }
                PeerState::Disconnected => {
                    self.disconnected_at = Some(Instant::now());
                }
                PeerState::Failed => {
                    // Failed state maintains connection timestamps
                }
            }
        }
    }

    /// Reset reconnection state after successful connection
    pub fn reset_reconnection(&mut self, initial_backoff_secs: u64) {
        self.retry_count = 0;
        self.next_retry_time = None;
        self.current_backoff = StdDuration::from_secs(initial_backoff_secs);
    }
}

/// Extract IP address from a ConnectedPoint
fn extract_ip_from_endpoint(endpoint: &ConnectedPoint) -> Option<IpAddr> {
    let addr = match endpoint {
        ConnectedPoint::Dialer { address, .. } => address,
        ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
    };

    // Parse the multiaddr to extract IP
    for proto in addr.iter() {
        match proto {
            Protocol::Ip4(ip) => return Some(IpAddr::V4(ip)),
            Protocol::Ip6(ip) => return Some(IpAddr::V6(ip)),
            _ => continue,
        }
    }

    None
}

/// Extract Multiaddr from a ConnectedPoint
fn extract_multiaddr_from_endpoint(endpoint: &ConnectedPoint) -> Multiaddr {
    match endpoint {
        ConnectedPoint::Dialer { address, .. } => address.clone(),
        ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr.clone(),
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
    /// Peer information for tracking state, RTT and failures
    peer_info: HashMap<PeerId, PeerInfo>,
    /// Local peer ID
    local_peer_id: PeerId,
    /// Ping configuration
    ping_config: PingConfig,
    /// Peer authorizer for authentication
    authorizer: PeerAuthorizer,
    /// Bootstrap peers (peers to connect to on startup)
    bootstrap_peers: HashSet<PeerId>,
    /// Reconnection configuration
    reconnection_config: ReconnectionConfig,
    /// Peers that should be reconnected to when disconnected
    reconnectable_peers: HashSet<PeerId>,
}

impl NetworkService {
    /// Create a new NetworkService with a PeerAuthorizer
    pub fn new(
        config: NetworkConfig,
        authorizer: PeerAuthorizer,
    ) -> Result<(Self, NetworkServiceHandle)> {
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
            peer_info: HashMap::new(),
            local_peer_id,
            ping_config: config.ping.clone(),
            authorizer,
            bootstrap_peers: HashSet::new(),
            reconnection_config: config.reconnection,
            reconnectable_peers: HashSet::new(),
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

        // Parse and dial bootstrap peers
        if !config.bootstrap_peers.is_empty() {
            info!(
                "Connecting to {} bootstrap peers",
                config.bootstrap_peers.len()
            );

            for peer_str in &config.bootstrap_peers {
                match BootstrapPeer::parse(peer_str) {
                    Ok(bootstrap_peer) => {
                        info!(
                            "Dialing bootstrap peer: {} at {}",
                            bootstrap_peer.peer_id, bootstrap_peer.multiaddr
                        );

                        // Track as bootstrap peer
                        self.bootstrap_peers.insert(bootstrap_peer.peer_id);

                        // Dial the peer
                        if let Err(e) = self.swarm.dial(bootstrap_peer.multiaddr.clone()) {
                            warn!(
                                "Failed to dial bootstrap peer {} at {}: {}",
                                bootstrap_peer.peer_id, bootstrap_peer.multiaddr, e
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Invalid bootstrap peer '{}': {}", peer_str, e);
                        // Continue with other bootstrap peers
                    }
                }
            }
        }

        Ok(())
    }

    /// Run the main event loop
    pub async fn run(&mut self) -> Result<()> {
        // Create a timer for checking reconnections every second
        let mut reconnection_timer = tokio::time::interval(StdDuration::from_secs(1));

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
                // Check for pending reconnections
                _ = reconnection_timer.tick() => {
                    if self.reconnection_config.enabled {
                        self.check_reconnections().await;
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
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                info!("Connection established with peer: {}", peer_id);

                // Authentication check
                let authenticated = self.check_peer_authentication(peer_id, &endpoint).await;

                if !authenticated {
                    // Authentication failed - disconnect immediately
                    warn!("Authentication failed for peer {}, disconnecting", peer_id);
                    let _ = self.swarm.disconnect_peer_id(peer_id);
                    return;
                }

                self.connected_peers.insert(peer_id);

                // Store the multiaddr for reconnection
                let multiaddr = extract_multiaddr_from_endpoint(&endpoint);

                // Mark peer as reconnectable if it's a bootstrap peer or reconnection is enabled
                if self.bootstrap_peers.contains(&peer_id) || self.reconnection_config.enabled {
                    self.reconnectable_peers.insert(peer_id);
                }

                // Initialize or update peer info with Connected state
                if let Some(info) = self.peer_info.get_mut(&peer_id) {
                    info.set_state(PeerState::Connected);
                    info.last_multiaddr = Some(multiaddr);
                    // Reset reconnection state on successful connection
                    info.reset_reconnection(self.reconnection_config.initial_backoff_secs);
                    info!(
                        "Reconnected to peer {} (retry count was: {})",
                        peer_id, info.retry_count
                    );
                } else {
                    let mut info = PeerInfo::new(peer_id);
                    info.set_state(PeerState::Connected);
                    info.last_multiaddr = Some(multiaddr);
                    info.current_backoff =
                        StdDuration::from_secs(self.reconnection_config.initial_backoff_secs);
                    self.peer_info.insert(peer_id, info);
                }

                let _ = self
                    .event_tx
                    .send(NetworkEvent::ConnectionEstablished { peer_id });
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                info!("Connection closed with peer: {}", peer_id);
                self.connected_peers.remove(&peer_id);

                // Update peer state to Disconnected and schedule reconnection if needed
                if let Some(info) = self.peer_info.get_mut(&peer_id) {
                    info.set_state(PeerState::Disconnected);

                    // Schedule reconnection if this is a reconnectable peer and reconnection is enabled
                    if self.reconnection_config.enabled
                        && self.reconnectable_peers.contains(&peer_id)
                    {
                        self.schedule_reconnection(peer_id);
                    }
                }

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
                        // Update peer info
                        if let Some(info) = self.peer_info.get_mut(&peer) {
                            info.last_rtt = Some(rtt);
                            info.consecutive_failures = 0;
                            info.last_seen = Instant::now();
                            // Reset to Connected if was Failed
                            if info.state == PeerState::Failed {
                                info.set_state(PeerState::Connected);
                            }
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
                        // Update failure count and possibly mark as Failed
                        if let Some(info) = self.peer_info.get_mut(&peer) {
                            let was_failed = info.state == PeerState::Failed;
                            info.consecutive_failures += 1;

                            // Check if peer has become unresponsive
                            if info.consecutive_failures >= self.ping_config.max_failures {
                                info.set_state(PeerState::Failed);

                                // Calculate time since last seen
                                let time_since_last_seen = info.last_seen.elapsed();

                                // Emit PeerUnresponsive event
                                // This emits on initial failure and continues to emit periodically
                                // while the peer remains unresponsive
                                let _ = self.event_tx.send(NetworkEvent::PeerUnresponsive {
                                    peer_id: peer,
                                    consecutive_failures: info.consecutive_failures,
                                    time_since_last_seen,
                                });

                                if was_failed {
                                    debug!(
                                        "Peer {} still unresponsive ({} consecutive failures, {:?} since last seen)",
                                        peer, info.consecutive_failures, time_since_last_seen
                                    );
                                } else {
                                    warn!(
                                        "Peer {} became unresponsive ({} consecutive failures)",
                                        peer, info.consecutive_failures
                                    );
                                }
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
            NetworkCommand::GetPeerState { peer_id, response } => {
                let state = self.peer_info.get(&peer_id).map(|info| info.state);
                let _ = response.send(state);
            }
            NetworkCommand::GetPeerInfo { peer_id, response } => {
                let info = self.peer_info.get(&peer_id).cloned();
                let _ = response.send(info);
            }
            NetworkCommand::ListPeersByState { state, response } => {
                let peers: Vec<PeerId> = self
                    .peer_info
                    .values()
                    .filter(|info| info.state == state)
                    .map(|info| info.peer_id)
                    .collect();
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

    /// Check if a peer is a bootstrap peer
    pub fn is_bootstrap_peer(&self, peer_id: &PeerId) -> bool {
        self.bootstrap_peers.contains(peer_id)
    }

    /// Get all bootstrap peer IDs
    pub fn get_bootstrap_peers(&self) -> Vec<PeerId> {
        self.bootstrap_peers.iter().cloned().collect()
    }

    /// Check peer authentication using PeerAuthorizer
    async fn check_peer_authentication(
        &mut self,
        peer_id: PeerId,
        endpoint: &ConnectedPoint,
    ) -> bool {
        let peer_ip = extract_ip_from_endpoint(endpoint);

        match self.authorizer.authorize_peer(peer_id, peer_ip).await {
            AuthResult::Authorized => true,
            AuthResult::Unauthorized { reason } => {
                let _ = self
                    .event_tx
                    .send(NetworkEvent::AuthenticationFailed { peer_id, reason });
                false
            }
            AuthResult::LearnPeer { entry } => {
                // Emit PeerLearned event for PeerManager to handle
                let _ = self
                    .event_tx
                    .send(NetworkEvent::PeerLearned { peer_id, entry });
                true
            }
        }
    }

    /// Schedule a reconnection attempt for a disconnected peer
    fn schedule_reconnection(&mut self, peer_id: PeerId) {
        if let Some(info) = self.peer_info.get_mut(&peer_id) {
            // Calculate next retry time with exponential backoff
            let backoff_secs = (info.current_backoff.as_secs() as f64
                * self.reconnection_config.backoff_multiplier)
                as u64;
            let backoff_secs = backoff_secs.min(self.reconnection_config.max_backoff_secs);

            info.current_backoff = StdDuration::from_secs(backoff_secs);
            info.next_retry_time = Some(Instant::now() + info.current_backoff);
            info.retry_count += 1;

            info!(
                "Scheduled reconnection to peer {} in {:?} (attempt #{}, backoff: {:?})",
                peer_id, info.current_backoff, info.retry_count, info.current_backoff
            );
        }
    }

    /// Check for peers that need reconnection and attempt to reconnect
    async fn check_reconnections(&mut self) {
        let now = Instant::now();
        let mut peers_to_reconnect = Vec::new();

        // Find peers that need reconnection
        for (peer_id, info) in &self.peer_info {
            if let Some(next_retry_time) = info.next_retry_time {
                if now >= next_retry_time && info.state == PeerState::Disconnected {
                    if let Some(multiaddr) = &info.last_multiaddr {
                        peers_to_reconnect.push((*peer_id, multiaddr.clone()));
                    }
                }
            }
        }

        // Attempt reconnections
        for (peer_id, multiaddr) in peers_to_reconnect {
            self.attempt_reconnection(peer_id, multiaddr).await;
        }
    }

    /// Attempt to reconnect to a peer
    async fn attempt_reconnection(&mut self, peer_id: PeerId, multiaddr: Multiaddr) {
        if let Some(info) = self.peer_info.get_mut(&peer_id) {
            info!(
                "Attempting reconnection to peer {} at {} (attempt #{})",
                peer_id, multiaddr, info.retry_count
            );

            // Clear the next_retry_time to prevent repeated attempts
            info.next_retry_time = None;

            // Attempt to dial the peer
            match self.swarm.dial(multiaddr.clone()) {
                Ok(_) => {
                    debug!("Reconnection dial initiated for peer {}", peer_id);
                }
                Err(e) => {
                    warn!("Failed to dial peer {} during reconnection: {}", peer_id, e);
                    // Reschedule for next attempt
                    self.schedule_reconnection(peer_id);
                }
            }
        }
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

    /// Get the state of a specific peer
    pub async fn get_peer_state(&self, peer_id: PeerId) -> Result<Option<PeerState>> {
        let (tx, rx) = oneshot::channel();
        self.command_tx.send(NetworkCommand::GetPeerState {
            peer_id,
            response: tx,
        })?;
        Ok(rx.await?)
    }

    /// Get complete information about a specific peer
    pub async fn get_peer_info(&self, peer_id: PeerId) -> Result<Option<PeerInfo>> {
        let (tx, rx) = oneshot::channel();
        self.command_tx.send(NetworkCommand::GetPeerInfo {
            peer_id,
            response: tx,
        })?;
        Ok(rx.await?)
    }

    /// List all peers in a specific state
    pub async fn list_peers_by_state(&self, state: PeerState) -> Result<Vec<PeerId>> {
        let (tx, rx) = oneshot::channel();
        self.command_tx.send(NetworkCommand::ListPeersByState {
            state,
            response: tx,
        })?;
        Ok(rx.await?)
    }

    /// Get the local peer ID
    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_service_creation() {
        let config = NetworkConfig::default();
        let known_peers = Arc::new(RwLock::new(HashMap::new()));
        let authorizer = PeerAuthorizer::new(
            AuthenticationMode::Disabled,
            known_peers,
            PathBuf::from("test_peers.json"),
        );
        let result = NetworkService::new(config, authorizer);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_invalid_listen_address() {
        let config = NetworkConfig {
            listen_address: "invalid-address".to_string(),
            initial_peers: Vec::new(),
            bootstrap_peers: Vec::new(),
            ping: PingConfig::default(),
            authentication: AuthenticationConfig::default(),
            reconnection: ReconnectionConfig::default(),
        };
        let known_peers = Arc::new(RwLock::new(HashMap::new()));
        let authorizer = PeerAuthorizer::new(
            AuthenticationMode::Disabled,
            known_peers,
            PathBuf::from("test_peers.json"),
        );
        let (mut service, _handle) = NetworkService::new(config.clone(), authorizer).unwrap();
        let result = service.start(config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_local_peer_id() {
        let config = NetworkConfig::default();
        let known_peers = Arc::new(RwLock::new(HashMap::new()));
        let authorizer = PeerAuthorizer::new(
            AuthenticationMode::Disabled,
            known_peers,
            PathBuf::from("test_peers.json"),
        );
        let (_service, handle) = NetworkService::new(config, authorizer).unwrap();

        // Peer ID should be valid
        let peer_id = handle.local_peer_id();
        assert_ne!(peer_id.to_string(), "");
    }
}
