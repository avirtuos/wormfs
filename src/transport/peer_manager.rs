//! Peer health monitoring and connection management
//!
//! This module tracks peer connectivity and health status,
//! enabling automatic reconnection and failover detection.

use super::PeerInfo;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Peer connection status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerStatus {
    /// Connected and healthy
    Connected,

    /// Disconnected but attempting reconnection
    Disconnected,

    /// Connection failed after max retries
    Failed,

    /// Not yet attempted connection
    Unknown,
}

/// Peer health information
#[derive(Debug, Clone)]
pub struct PeerHealth {
    /// Peer information
    pub peer: PeerInfo,

    /// Current connection status
    pub status: PeerStatus,

    /// Last successful communication timestamp
    pub last_seen: Option<Instant>,

    /// Number of consecutive failures
    pub failure_count: u32,

    /// Round-trip time in milliseconds
    pub rtt_ms: Option<u64>,
}

impl PeerHealth {
    /// Create a new peer health tracker
    pub fn new(peer: PeerInfo) -> Self {
        Self {
            peer,
            status: PeerStatus::Unknown,
            last_seen: None,
            failure_count: 0,
            rtt_ms: None,
        }
    }

    /// Check if the peer is healthy (connected recently)
    pub fn is_healthy(&self, max_age: Duration) -> bool {
        if self.status != PeerStatus::Connected {
            return false;
        }

        if let Some(last_seen) = self.last_seen {
            last_seen.elapsed() < max_age
        } else {
            false
        }
    }

    /// Mark peer as connected
    pub fn mark_connected(&mut self, rtt_ms: Option<u64>) {
        self.status = PeerStatus::Connected;
        self.last_seen = Some(Instant::now());
        self.failure_count = 0;
        self.rtt_ms = rtt_ms;
    }

    /// Mark peer as disconnected
    pub fn mark_disconnected(&mut self) {
        self.status = PeerStatus::Disconnected;
        self.failure_count += 1;
    }

    /// Mark peer as failed
    pub fn mark_failed(&mut self) {
        self.status = PeerStatus::Failed;
    }
}

/// Peer manager for tracking and monitoring peer health
pub struct PeerManager {
    /// Map of node_id to peer health
    peers: HashMap<u64, PeerHealth>,

    /// Maximum time since last seen before considering unhealthy
    health_timeout: Duration,

    /// Maximum connection failures before marking as failed
    max_failures: u32,
}

impl PeerManager {
    /// Create a new peer manager
    pub fn new(peers: Vec<PeerInfo>, health_timeout: Duration, max_failures: u32) -> Self {
        let peer_health: HashMap<u64, PeerHealth> = peers
            .into_iter()
            .map(|p| {
                let node_id = p.node_id;
                (node_id, PeerHealth::new(p))
            })
            .collect();

        Self {
            peers: peer_health,
            health_timeout,
            max_failures,
        }
    }

    /// Get peer health information
    pub fn get_peer(&self, node_id: u64) -> Option<&PeerHealth> {
        self.peers.get(&node_id)
    }

    /// Get mutable peer health information
    pub fn get_peer_mut(&mut self, node_id: u64) -> Option<&mut PeerHealth> {
        self.peers.get_mut(&node_id)
    }

    /// Get all peers
    pub fn all_peers(&self) -> impl Iterator<Item = &PeerHealth> {
        self.peers.values()
    }

    /// Get healthy peers
    pub fn healthy_peers(&self) -> impl Iterator<Item = &PeerHealth> {
        self.peers
            .values()
            .filter(|p| p.is_healthy(self.health_timeout))
    }

    /// Count healthy peers
    pub fn healthy_count(&self) -> usize {
        self.healthy_peers().count()
    }

    /// Update peer status after successful communication
    pub fn record_success(&mut self, node_id: u64, rtt_ms: Option<u64>) {
        if let Some(peer) = self.peers.get_mut(&node_id) {
            peer.mark_connected(rtt_ms);
            tracing::debug!("Peer {} marked as connected", node_id);
        }
    }

    /// Update peer status after failed communication
    pub fn record_failure(&mut self, node_id: u64) {
        if let Some(peer) = self.peers.get_mut(&node_id) {
            peer.mark_disconnected();

            if peer.failure_count >= self.max_failures {
                peer.mark_failed();
                tracing::warn!(
                    "Peer {} marked as failed after {} consecutive failures",
                    node_id,
                    peer.failure_count
                );
            } else {
                tracing::debug!("Peer {} failure count: {}", node_id, peer.failure_count);
            }
        }
    }

    /// Reset a failed peer to allow reconnection attempts
    pub fn reset_peer(&mut self, node_id: u64) {
        if let Some(peer) = self.peers.get_mut(&node_id) {
            peer.status = PeerStatus::Unknown;
            peer.failure_count = 0;
            tracing::info!("Peer {} reset for reconnection", node_id);
        }
    }

    /// Get peer statistics
    pub fn stats(&self) -> PeerStats {
        let total = self.peers.len();
        let connected = self
            .peers
            .values()
            .filter(|p| p.status == PeerStatus::Connected)
            .count();
        let disconnected = self
            .peers
            .values()
            .filter(|p| p.status == PeerStatus::Disconnected)
            .count();
        let failed = self
            .peers
            .values()
            .filter(|p| p.status == PeerStatus::Failed)
            .count();
        let unknown = self
            .peers
            .values()
            .filter(|p| p.status == PeerStatus::Unknown)
            .count();
        let healthy = self.healthy_count();

        PeerStats {
            total,
            connected,
            disconnected,
            failed,
            unknown,
            healthy,
        }
    }
}

/// Peer statistics
#[derive(Debug, Clone)]
pub struct PeerStats {
    pub total: usize,
    pub connected: usize,
    pub disconnected: usize,
    pub failed: usize,
    pub unknown: usize,
    pub healthy: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_peer(node_id: u64) -> PeerInfo {
        PeerInfo {
            node_id,
            address: format!("/ip4/127.0.0.1/tcp/{}", 3000 + node_id),
        }
    }

    #[test]
    fn test_peer_health_lifecycle() {
        let mut health = PeerHealth::new(create_test_peer(1));

        assert_eq!(health.status, PeerStatus::Unknown);
        assert!(!health.is_healthy(Duration::from_secs(10)));

        health.mark_connected(Some(50));
        assert_eq!(health.status, PeerStatus::Connected);
        assert!(health.is_healthy(Duration::from_secs(10)));
        assert_eq!(health.rtt_ms, Some(50));
        assert_eq!(health.failure_count, 0);

        health.mark_disconnected();
        assert_eq!(health.status, PeerStatus::Disconnected);
        assert_eq!(health.failure_count, 1);

        health.mark_failed();
        assert_eq!(health.status, PeerStatus::Failed);
    }

    #[test]
    fn test_peer_manager() {
        let peers = vec![
            create_test_peer(1),
            create_test_peer(2),
            create_test_peer(3),
        ];

        let mut manager = PeerManager::new(peers, Duration::from_secs(10), 3);

        assert_eq!(manager.all_peers().count(), 3);
        assert_eq!(manager.healthy_count(), 0);

        manager.record_success(1, Some(50));
        assert_eq!(manager.healthy_count(), 1);

        manager.record_failure(2);
        manager.record_failure(2);
        manager.record_failure(2);

        let stats = manager.stats();
        assert_eq!(stats.total, 3);
        assert_eq!(stats.connected, 1);
        assert_eq!(stats.failed, 1);
    }
}
