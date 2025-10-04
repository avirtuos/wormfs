//! Peer Manager Module
//!
//! This module manages peer lifecycle, state tracking, and reconnection logic,
//! separating these concerns from the low-level networking layer.

use crate::networking::{
    AuthenticationMode, NetworkCommand, NetworkEvent, PeerEntry, PeerInfo, PeerState,
    ReconnectionConfig,
};
use crate::peer_authorizer::PeerAuthorizer;
use anyhow::{anyhow, Result};
use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

/// Peers file format
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeersFileFormat {
    version: String,
    peers: Vec<PeerEntry>,
}

/// Commands that can be sent to the PeerManager
#[derive(Debug)]
pub enum PeerManagerCommand {
    /// Get the state of a specific peer
    GetPeerState {
        peer_id: PeerId,
        response: tokio::sync::oneshot::Sender<Option<PeerState>>,
    },
    /// Get complete info about a specific peer
    GetPeerInfo {
        peer_id: PeerId,
        response: tokio::sync::oneshot::Sender<Option<PeerInfo>>,
    },
    /// List all peers with a specific state
    ListPeersByState {
        state: PeerState,
        response: tokio::sync::oneshot::Sender<Vec<PeerId>>,
    },
    /// Shutdown the peer manager
    Shutdown,
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

/// The peer manager handles peer lifecycle and reconnection logic
pub struct PeerManager {
    /// Peer information for tracking state, RTT and failures
    peer_info: HashMap<PeerId, PeerInfo>,
    /// Peers that should be reconnected to when disconnected
    reconnectable_peers: HashSet<PeerId>,
    /// Bootstrap peers (peers to connect to on startup)
    bootstrap_peers: HashSet<PeerId>,
    /// Reconnection configuration
    reconnection_config: ReconnectionConfig,
    /// Channel for sending commands to NetworkService
    network_command_tx: mpsc::UnboundedSender<NetworkCommand>,
    /// Channel for receiving events from NetworkService
    network_event_rx: mpsc::UnboundedReceiver<NetworkEvent>,
    /// Channel for receiving commands
    command_rx: mpsc::UnboundedReceiver<PeerManagerCommand>,
    /// Known peers (shared with PeerAuthorizer)
    known_peers: Arc<RwLock<HashMap<PeerId, PeerEntry>>>,
    /// Path to peers file
    peers_file_path: PathBuf,
}

impl PeerManager {
    /// Create a new PeerManager
    pub fn new(
        reconnection_config: ReconnectionConfig,
        bootstrap_peers: HashSet<PeerId>,
        network_command_tx: mpsc::UnboundedSender<NetworkCommand>,
        network_event_rx: mpsc::UnboundedReceiver<NetworkEvent>,
        peers_file_path: PathBuf,
        auth_mode: AuthenticationMode,
    ) -> Result<(Self, PeerManagerHandle, PeerAuthorizer)> {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        // Load peers from file if authentication is not disabled
        let loaded_peers = match auth_mode {
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

        // Create shared known_peers map
        let known_peers = Arc::new(RwLock::new(loaded_peers.clone()));

        // Create PeerAuthorizer
        let authorizer =
            PeerAuthorizer::new(auth_mode, Arc::clone(&known_peers), peers_file_path.clone());

        // Mark bootstrap and known peers as reconnectable
        let mut reconnectable_peers = HashSet::new();
        reconnectable_peers.extend(&bootstrap_peers);
        reconnectable_peers.extend(loaded_peers.keys());

        let manager = Self {
            peer_info: HashMap::new(),
            reconnectable_peers,
            bootstrap_peers,
            reconnection_config,
            network_command_tx,
            network_event_rx,
            command_rx,
            known_peers,
            peers_file_path,
        };

        let handle = PeerManagerHandle { command_tx };

        Ok((manager, handle, authorizer))
    }

    /// Run the main event loop
    pub async fn run(&mut self) {
        info!("PeerManager started");

        // Create a timer for checking reconnections every second
        let mut reconnection_timer = tokio::time::interval(StdDuration::from_secs(1));

        loop {
            tokio::select! {
                // Handle network events
                event = self.network_event_rx.recv() => {
                    match event {
                        Some(event) => self.handle_network_event(event).await,
                        None => {
                            warn!("Network event channel closed, shutting down PeerManager");
                            break;
                        }
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
                            warn!("Command channel closed, shutting down PeerManager");
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

        info!("PeerManager shutting down");
    }

    /// Handle network events from NetworkService
    async fn handle_network_event(&mut self, event: NetworkEvent) {
        match event {
            NetworkEvent::ConnectionEstablished { peer_id } => {
                self.handle_connection_established(peer_id);
            }
            NetworkEvent::ConnectionClosed { peer_id } => {
                self.handle_connection_closed(peer_id);
            }
            NetworkEvent::PingSuccess { peer_id, rtt } => {
                self.handle_ping_success(peer_id, rtt);
            }
            NetworkEvent::PingFailure { peer_id } => {
                self.handle_ping_failure(peer_id);
            }
            NetworkEvent::PeerUnresponsive {
                peer_id,
                consecutive_failures,
                time_since_last_seen,
            } => {
                debug!(
                    "Peer {} unresponsive: {} failures, {:?} since last seen",
                    peer_id, consecutive_failures, time_since_last_seen
                );
                // State already updated by NetworkService
            }
            NetworkEvent::AuthenticationFailed { peer_id, reason } => {
                debug!("Authentication failed for peer {}: {}", peer_id, reason);
                // No action needed - NetworkService handles disconnection
            }
            NetworkEvent::PeerLearned { peer_id, entry } => {
                self.handle_peer_learned(peer_id, entry).await;
            }
            NetworkEvent::Error { message } => {
                warn!("Network error: {}", message);
            }
        }
    }

    /// Handle a newly learned peer
    async fn handle_peer_learned(&mut self, peer_id: PeerId, entry: PeerEntry) {
        info!(
            "Learning new peer {} with IPs {:?}",
            peer_id, entry.ip_addresses
        );

        // Add to known_peers
        {
            let mut known_peers = self.known_peers.write().await;
            known_peers.insert(peer_id, entry);
        }

        // Save to peers file
        let known_peers = self.known_peers.read().await;
        if let Err(e) = save_peers_file(&self.peers_file_path, &known_peers) {
            warn!("Failed to save peers file after learning new peer: {}", e);
        }

        // Mark as reconnectable
        self.reconnectable_peers.insert(peer_id);
    }

    /// Handle connection established
    fn handle_connection_established(&mut self, peer_id: PeerId) {
        info!("PeerManager: Connection established with peer {}", peer_id);

        // Mark as reconnectable if bootstrap or already known
        if self.bootstrap_peers.contains(&peer_id) || self.reconnection_config.enabled {
            self.reconnectable_peers.insert(peer_id);
        }

        // Update or create peer info
        if let Some(info) = self.peer_info.get_mut(&peer_id) {
            info.set_state(PeerState::Connected);
            info.reset_reconnection(self.reconnection_config.initial_backoff_secs);
            debug!(
                "Reconnected to peer {} (retry count was: {})",
                peer_id, info.retry_count
            );
        } else {
            let mut info = PeerInfo::new(peer_id);
            info.set_state(PeerState::Connected);
            info.current_backoff =
                StdDuration::from_secs(self.reconnection_config.initial_backoff_secs);
            self.peer_info.insert(peer_id, info);
        }
    }

    /// Handle connection closed
    fn handle_connection_closed(&mut self, peer_id: PeerId) {
        info!("PeerManager: Connection closed with peer {}", peer_id);

        // Update peer state
        if let Some(info) = self.peer_info.get_mut(&peer_id) {
            info.set_state(PeerState::Disconnected);

            // Schedule reconnection if this is a reconnectable peer
            if self.reconnection_config.enabled && self.reconnectable_peers.contains(&peer_id) {
                self.schedule_reconnection(peer_id);
            }
        }
    }

    /// Handle ping success
    fn handle_ping_success(&mut self, peer_id: PeerId, rtt: StdDuration) {
        if let Some(info) = self.peer_info.get_mut(&peer_id) {
            info.last_rtt = Some(rtt);
            info.consecutive_failures = 0;
            info.last_seen = Instant::now();

            // Reset to Connected if was Failed
            if info.state == PeerState::Failed {
                info.set_state(PeerState::Connected);
            }
        }
    }

    /// Handle ping failure
    fn handle_ping_failure(&mut self, peer_id: PeerId) {
        if let Some(info) = self.peer_info.get_mut(&peer_id) {
            info.consecutive_failures += 1;
            debug!(
                "Peer {} ping failure count: {}",
                peer_id, info.consecutive_failures
            );
        }
    }

    /// Schedule a reconnection attempt for a disconnected peer
    fn schedule_reconnection(&mut self, peer_id: PeerId) {
        if let Some(info) = self.peer_info.get_mut(&peer_id) {
            // Only schedule if we have a last known address
            if info.last_multiaddr.is_none() {
                debug!(
                    "Cannot schedule reconnection for peer {} - no known address",
                    peer_id
                );
                return;
            }

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

            // Send dial command to NetworkService
            let (tx, rx) = tokio::sync::oneshot::channel();
            if let Err(e) = self.network_command_tx.send(NetworkCommand::Dial {
                address: multiaddr.clone(),
                response: tx,
            }) {
                warn!("Failed to send dial command: {}", e);
                self.schedule_reconnection(peer_id);
                return;
            }

            // Wait for dial result
            match rx.await {
                Ok(Ok(())) => {
                    debug!("Reconnection dial initiated for peer {}", peer_id);
                }
                Ok(Err(e)) => {
                    warn!("Failed to dial peer {} during reconnection: {}", peer_id, e);
                    self.schedule_reconnection(peer_id);
                }
                Err(e) => {
                    warn!("Failed to receive dial response: {}", e);
                    self.schedule_reconnection(peer_id);
                }
            }
        }
    }

    /// Handle commands, returns false if shutdown was requested
    async fn handle_command(&mut self, command: PeerManagerCommand) -> bool {
        match command {
            PeerManagerCommand::GetPeerState { peer_id, response } => {
                let state = self.peer_info.get(&peer_id).map(|info| info.state);
                let _ = response.send(state);
            }
            PeerManagerCommand::GetPeerInfo { peer_id, response } => {
                let info = self.peer_info.get(&peer_id).cloned();
                let _ = response.send(info);
            }
            PeerManagerCommand::ListPeersByState { state, response } => {
                let peers: Vec<PeerId> = self
                    .peer_info
                    .values()
                    .filter(|info| info.state == state)
                    .map(|info| info.peer_id)
                    .collect();
                let _ = response.send(peers);
            }
            PeerManagerCommand::Shutdown => {
                info!("PeerManager shutdown command received");
                return false;
            }
        }
        true
    }

    /// Mark a peer's multiaddr for reconnection
    pub fn set_peer_multiaddr(&mut self, peer_id: PeerId, multiaddr: Multiaddr) {
        if let Some(info) = self.peer_info.get_mut(&peer_id) {
            info.last_multiaddr = Some(multiaddr);
        }
    }
}

/// Handle for interacting with the PeerManager
#[derive(Debug, Clone)]
pub struct PeerManagerHandle {
    command_tx: mpsc::UnboundedSender<PeerManagerCommand>,
}

impl PeerManagerHandle {
    /// Get the state of a specific peer
    pub async fn get_peer_state(&self, peer_id: PeerId) -> anyhow::Result<Option<PeerState>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.command_tx.send(PeerManagerCommand::GetPeerState {
            peer_id,
            response: tx,
        })?;
        Ok(rx.await?)
    }

    /// Get complete information about a specific peer
    pub async fn get_peer_info(&self, peer_id: PeerId) -> anyhow::Result<Option<PeerInfo>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.command_tx.send(PeerManagerCommand::GetPeerInfo {
            peer_id,
            response: tx,
        })?;
        Ok(rx.await?)
    }

    /// List all peers in a specific state
    pub async fn list_peers_by_state(&self, state: PeerState) -> anyhow::Result<Vec<PeerId>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.command_tx.send(PeerManagerCommand::ListPeersByState {
            state,
            response: tx,
        })?;
        Ok(rx.await?)
    }

    /// Shutdown the peer manager
    pub fn shutdown(&self) -> anyhow::Result<()> {
        self.command_tx.send(PeerManagerCommand::Shutdown)?;
        Ok(())
    }
}
