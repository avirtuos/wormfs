//! Networking module for WormFS
//!
//! This module provides the NetworkService for libp2p-based peer-to-peer networking.
//! Phase 2A.1 provides minimal libp2p setup with TCP transport and basic connection handling.

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
use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
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

/// Peers file format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeersFileFormat {
    version: String,
    peers: Vec<PeerEntry>,
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
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            listen_address: "/ip4/0.0.0.0/tcp/4001".to_string(),
            initial_peers: Vec::new(),
            bootstrap_peers: Vec::new(),
            ping: PingConfig::default(),
            authentication: AuthenticationConfig::default(),
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
}

impl PeerInfo {
    fn new(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            state: PeerState::Connected,
            last_rtt: None,
            consecutive_failures: 0,
            last_seen: Instant::now(),
            connected_at: Some(Instant::now()),
            disconnected_at: None,
        }
    }

    /// Update state with logging
    fn set_state(&mut self, new_state: PeerState) {
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
}

/// Load peers from a JSON file
fn load_peers_file(path: &Path) -> Result<HashMap<PeerId, PeerEntry>> {
    if !path.exists() {
        info!(
            "Peers file does not exist: {:?}, starting with empty peers list",
            path
        );
        return Ok(HashMap::new());
    }

    let content = fs::read_to_string(path)
        .map_err(|e| anyhow!("Failed to read peers file {:?}: {}", path, e))?;

    let file_format: PeersFileFormat = serde_json::from_str(&content)
        .map_err(|e| anyhow!("Failed to parse peers file {:?}: {}", path, e))?;

    let mut peers = HashMap::new();
    for entry in file_format.peers {
        if let Ok(peer_id) = entry.peer_id.parse::<PeerId>() {
            peers.insert(peer_id, entry);
        } else {
            warn!("Invalid peer_id in peers file: {}", entry.peer_id);
        }
    }

    info!("Loaded {} peers from {:?}", peers.len(), path);
    Ok(peers)
}

/// Save peers to a JSON file atomically
fn save_peers_file(path: &Path, peers: &HashMap<PeerId, PeerEntry>) -> Result<()> {
    let file_format = PeersFileFormat {
        version: "1.0".to_string(),
        peers: peers.values().cloned().collect(),
    };

    let json = serde_json::to_string_pretty(&file_format)
        .map_err(|e| anyhow!("Failed to serialize peers: {}", e))?;

    // Atomic write: write to temp file, then rename
    let temp_path = path.with_extension("tmp");
    fs::write(&temp_path, json)
        .map_err(|e| anyhow!("Failed to write temp peers file {:?}: {}", temp_path, e))?;

    fs::rename(&temp_path, path).map_err(|e| {
        anyhow!(
            "Failed to rename temp peers file {:?} to {:?}: {}",
            temp_path,
            path,
            e
        )
    })?;

    debug!("Saved {} peers to {:?}", peers.len(), path);
    Ok(())
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
    /// Authentication configuration
    authentication_config: AuthenticationConfig,
    /// Known peers (loaded from peers file)
    known_peers: HashMap<PeerId, PeerEntry>,
    /// Path to peers file
    peers_file_path: PathBuf,
    /// Bootstrap peers (peers to connect to on startup)
    bootstrap_peers: HashSet<PeerId>,
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

        // Load peers file if authentication is enabled
        let peers_file_path = PathBuf::from(&config.authentication.peers_file);
        let known_peers = match config.authentication.mode {
            AuthenticationMode::Disabled => {
                info!("Authentication disabled, not loading peers file");
                HashMap::new()
            }
            AuthenticationMode::Learn | AuthenticationMode::Enforce => {
                load_peers_file(&peers_file_path).unwrap_or_else(|e| {
                    warn!("Failed to load peers file: {}", e);
                    HashMap::new()
                })
            }
        };

        let service = NetworkService {
            swarm,
            command_rx,
            event_tx,
            connected_peers: HashSet::new(),
            peer_info: HashMap::new(),
            local_peer_id,
            ping_config: config.ping.clone(),
            authentication_config: config.authentication,
            known_peers,
            peers_file_path,
            bootstrap_peers: HashSet::new(),
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
                // Initialize peer info with Connected state
                let mut info = PeerInfo::new(peer_id);
                info.set_state(PeerState::Connected);
                self.peer_info.insert(peer_id, info);
                let _ = self
                    .event_tx
                    .send(NetworkEvent::ConnectionEstablished { peer_id });
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                info!("Connection closed with peer: {}", peer_id);
                self.connected_peers.remove(&peer_id);
                // Update peer state to Disconnected
                if let Some(info) = self.peer_info.get_mut(&peer_id) {
                    info.set_state(PeerState::Disconnected);
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

    /// Check peer authentication based on the configured mode
    async fn check_peer_authentication(
        &mut self,
        peer_id: PeerId,
        endpoint: &ConnectedPoint,
    ) -> bool {
        match self.authentication_config.mode {
            AuthenticationMode::Disabled => {
                debug!("Authentication disabled, accepting peer {}", peer_id);
                true
            }
            AuthenticationMode::Learn => self.check_learn_mode(peer_id, endpoint).await,
            AuthenticationMode::Enforce => self.check_enforce_mode(peer_id, endpoint).await,
        }
    }

    /// Check authentication in Learn mode
    async fn check_learn_mode(&mut self, peer_id: PeerId, endpoint: &ConnectedPoint) -> bool {
        let peer_ip = match extract_ip_from_endpoint(endpoint) {
            Some(ip) => ip,
            None => {
                warn!("Could not extract IP from endpoint for peer {}", peer_id);
                return true; // Accept if we can't extract IP
            }
        };

        if let Some(entry) = self.known_peers.get_mut(&peer_id) {
            // Known peer - verify IP matches
            let ip_str = peer_ip.to_string();
            if entry.ip_addresses.contains(&ip_str) {
                info!(
                    "Learn mode: Known peer {} with matching IP {}",
                    peer_id, ip_str
                );
                entry.last_seen = SystemTime::now();
                entry.connection_count += 1;
                // Save updated peer info
                if let Err(e) = save_peers_file(&self.peers_file_path, &self.known_peers) {
                    warn!("Failed to save peers file: {}", e);
                }
                true
            } else {
                let reason = format!(
                    "IP mismatch - expected one of {:?}, got {}",
                    entry.ip_addresses, ip_str
                );
                warn!("Learn mode: Rejecting peer {} - {}", peer_id, reason);
                let _ = self
                    .event_tx
                    .send(NetworkEvent::AuthenticationFailed { peer_id, reason });
                false
            }
        } else {
            // New peer - learn it
            let ip_str = peer_ip.to_string();
            info!(
                "Learn mode: Learning new peer {} with IP {}",
                peer_id, ip_str
            );

            let entry = PeerEntry {
                peer_id: peer_id.to_string(),
                ip_addresses: vec![ip_str],
                first_seen: SystemTime::now(),
                last_seen: SystemTime::now(),
                connection_count: 1,
                source: "learned".to_string(),
            };

            self.known_peers.insert(peer_id, entry);

            // Save new peer to file
            if let Err(e) = save_peers_file(&self.peers_file_path, &self.known_peers) {
                warn!("Failed to save peers file: {}", e);
            }

            true
        }
    }

    /// Check authentication in Enforce mode
    async fn check_enforce_mode(&mut self, peer_id: PeerId, endpoint: &ConnectedPoint) -> bool {
        let peer_ip = match extract_ip_from_endpoint(endpoint) {
            Some(ip) => ip,
            None => {
                let reason = "Could not extract IP from endpoint".to_string();
                warn!("Enforce mode: Rejecting peer {} - {}", peer_id, reason);
                let _ = self
                    .event_tx
                    .send(NetworkEvent::AuthenticationFailed { peer_id, reason });
                return false;
            }
        };

        if let Some(entry) = self.known_peers.get_mut(&peer_id) {
            let ip_str = peer_ip.to_string();
            if entry.ip_addresses.contains(&ip_str) {
                info!(
                    "Enforce mode: Authorized peer {} with IP {}",
                    peer_id, ip_str
                );
                entry.last_seen = SystemTime::now();
                entry.connection_count += 1;
                // Save updated peer info
                if let Err(e) = save_peers_file(&self.peers_file_path, &self.known_peers) {
                    warn!("Failed to save peers file: {}", e);
                }
                true
            } else {
                let reason = format!(
                    "IP mismatch - expected one of {:?}, got {}",
                    entry.ip_addresses, ip_str
                );
                warn!("Enforce mode: Rejecting peer {} - {}", peer_id, reason);
                let _ = self
                    .event_tx
                    .send(NetworkEvent::AuthenticationFailed { peer_id, reason });
                false
            }
        } else {
            let reason = "Peer not in authorized list".to_string();
            warn!(
                "Enforce mode: Rejecting unknown peer {} with IP {}",
                peer_id, peer_ip
            );
            let _ = self
                .event_tx
                .send(NetworkEvent::AuthenticationFailed { peer_id, reason });
            false
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
            bootstrap_peers: Vec::new(),
            ping: PingConfig::default(),
            authentication: AuthenticationConfig::default(),
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
