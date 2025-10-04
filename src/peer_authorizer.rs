//! Peer Authorizer Module
//!
//! This module provides peer authentication and authorization functionality.
//! It is created by PeerManager and shared with NetworkService for connection authentication.

use crate::networking::{AuthenticationMode, PeerEntry};
use libp2p::PeerId;
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Result of a peer authorization check
#[derive(Debug, Clone)]
pub enum AuthResult {
    /// Peer is authorized to connect
    Authorized,
    /// Peer is not authorized (with reason)
    Unauthorized { reason: String },
    /// Peer is new and should be learned (Learn mode only)
    LearnPeer { entry: PeerEntry },
}

/// Peer authorizer for authentication checks
#[derive(Clone)]
pub struct PeerAuthorizer {
    /// Authentication mode
    mode: AuthenticationMode,
    /// Known peers (shared with PeerManager)
    known_peers: Arc<RwLock<HashMap<PeerId, PeerEntry>>>,
    /// Path to peers file
    peers_file_path: Arc<PathBuf>,
}

impl PeerAuthorizer {
    /// Create a new PeerAuthorizer
    pub fn new(
        mode: AuthenticationMode,
        known_peers: Arc<RwLock<HashMap<PeerId, PeerEntry>>>,
        peers_file_path: PathBuf,
    ) -> Self {
        Self {
            mode,
            known_peers,
            peers_file_path: Arc::new(peers_file_path),
        }
    }

    /// Authorize a peer connection
    pub async fn authorize_peer(&self, peer_id: PeerId, peer_ip: Option<IpAddr>) -> AuthResult {
        match self.mode {
            AuthenticationMode::Disabled => {
                debug!("Authentication disabled, accepting peer {}", peer_id);
                AuthResult::Authorized
            }
            AuthenticationMode::Learn => self.check_learn_mode(peer_id, peer_ip).await,
            AuthenticationMode::Enforce => self.check_enforce_mode(peer_id, peer_ip).await,
        }
    }

    /// Check authentication in Learn mode
    async fn check_learn_mode(&self, peer_id: PeerId, peer_ip: Option<IpAddr>) -> AuthResult {
        let peer_ip = match peer_ip {
            Some(ip) => ip,
            None => {
                warn!("Could not extract IP from endpoint for peer {}", peer_id);
                return AuthResult::Authorized; // Accept if we can't extract IP
            }
        };

        let known_peers = self.known_peers.read().await;

        if let Some(entry) = known_peers.get(&peer_id) {
            // Known peer - verify IP matches
            let ip_str = peer_ip.to_string();
            if entry.ip_addresses.contains(&ip_str) {
                info!(
                    "Learn mode: Known peer {} with matching IP {}",
                    peer_id, ip_str
                );
                // Note: We don't update the entry here - PeerManager will do that
                AuthResult::Authorized
            } else {
                let reason = format!(
                    "IP mismatch - expected one of {:?}, got {}",
                    entry.ip_addresses, ip_str
                );
                warn!("Learn mode: Rejecting peer {} - {}", peer_id, reason);
                AuthResult::Unauthorized { reason }
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

            AuthResult::LearnPeer { entry }
        }
    }

    /// Check authentication in Enforce mode
    async fn check_enforce_mode(&self, peer_id: PeerId, peer_ip: Option<IpAddr>) -> AuthResult {
        let peer_ip = match peer_ip {
            Some(ip) => ip,
            None => {
                let reason = "Could not extract IP from endpoint".to_string();
                warn!("Enforce mode: Rejecting peer {} - {}", peer_id, reason);
                return AuthResult::Unauthorized { reason };
            }
        };

        let known_peers = self.known_peers.read().await;

        if let Some(entry) = known_peers.get(&peer_id) {
            let ip_str = peer_ip.to_string();
            if entry.ip_addresses.contains(&ip_str) {
                info!(
                    "Enforce mode: Authorized peer {} with IP {}",
                    peer_id, ip_str
                );
                // Note: We don't update the entry here - PeerManager will do that
                AuthResult::Authorized
            } else {
                let reason = format!(
                    "IP mismatch - expected one of {:?}, got {}",
                    entry.ip_addresses, ip_str
                );
                warn!("Enforce mode: Rejecting peer {} - {}", peer_id, reason);
                AuthResult::Unauthorized { reason }
            }
        } else {
            let reason = "Peer not in authorized list".to_string();
            warn!(
                "Enforce mode: Rejecting unknown peer {} with IP {}",
                peer_id, peer_ip
            );
            AuthResult::Unauthorized { reason }
        }
    }

    /// Get the authentication mode
    pub fn mode(&self) -> AuthenticationMode {
        self.mode.clone()
    }

    /// Get the peers file path
    pub fn peers_file_path(&self) -> &PathBuf {
        &self.peers_file_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_disabled_mode() {
        let peers = Arc::new(RwLock::new(HashMap::new()));
        let authorizer = PeerAuthorizer::new(
            AuthenticationMode::Disabled,
            peers,
            PathBuf::from("test.json"),
        );

        let peer_id = PeerId::random();
        let result = authorizer.authorize_peer(peer_id, None).await;

        assert!(matches!(result, AuthResult::Authorized));
    }

    #[tokio::test]
    async fn test_enforce_mode_authorized() {
        let mut peers_map = HashMap::new();
        let peer_id = PeerId::random();
        let entry = PeerEntry {
            peer_id: peer_id.to_string(),
            ip_addresses: vec!["127.0.0.1".to_string()],
            first_seen: SystemTime::now(),
            last_seen: SystemTime::now(),
            connection_count: 1,
            source: "manual".to_string(),
        };
        peers_map.insert(peer_id, entry);

        let peers = Arc::new(RwLock::new(peers_map));
        let authorizer = PeerAuthorizer::new(
            AuthenticationMode::Enforce,
            peers,
            PathBuf::from("test.json"),
        );

        let result = authorizer
            .authorize_peer(peer_id, Some("127.0.0.1".parse().unwrap()))
            .await;

        assert!(matches!(result, AuthResult::Authorized));
    }

    #[tokio::test]
    async fn test_enforce_mode_unauthorized() {
        let peers = Arc::new(RwLock::new(HashMap::new()));
        let authorizer = PeerAuthorizer::new(
            AuthenticationMode::Enforce,
            peers,
            PathBuf::from("test.json"),
        );

        let peer_id = PeerId::random();
        let result = authorizer
            .authorize_peer(peer_id, Some("127.0.0.1".parse().unwrap()))
            .await;

        assert!(matches!(result, AuthResult::Unauthorized { .. }));
    }

    #[tokio::test]
    async fn test_learn_mode_new_peer() {
        let peers = Arc::new(RwLock::new(HashMap::new()));
        let authorizer =
            PeerAuthorizer::new(AuthenticationMode::Learn, peers, PathBuf::from("test.json"));

        let peer_id = PeerId::random();
        let result = authorizer
            .authorize_peer(peer_id, Some("192.168.1.1".parse().unwrap()))
            .await;

        assert!(matches!(result, AuthResult::LearnPeer { .. }));
    }
}
