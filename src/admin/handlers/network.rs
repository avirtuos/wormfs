//! Network status handler for admin API endpoints.
//!
//! Provides handlers for viewing peer connectivity and network health.

use crate::storage_network::StorageNetworkHandle;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use std::sync::Arc;
use std::time::SystemTime;

/// Handler for `/api/network/status` endpoint.
///
/// Returns comprehensive network status including connected peers,
/// heartbeat information, and network statistics.
///
/// # Returns
///
/// JSON response with network status information:
/// - `local_node`: Information about this node
/// - `peers`: List of connected peers with heartbeat status
/// - `statistics`: Network-level statistics
pub async fn network_status_handler(
    State(network): State<Arc<StorageNetworkHandle>>,
) -> impl IntoResponse {
    // Get connected peers from the network
    let peers = network.get_connected_peers().await;

    // Convert peer info to JSON
    let peer_list: Vec<_> = peers
        .iter()
        .map(|peer| {
            let last_heartbeat = peer
                .last_heartbeat
                .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                .map(|d| chrono::DateTime::from_timestamp(d.as_secs() as i64, 0))
                .flatten()
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| "never".to_string());

            let connected_since = peer
                .connected_since
                .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                .map(|d| chrono::DateTime::from_timestamp(d.as_secs() as i64, 0))
                .flatten()
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| "unknown".to_string());

            json!({
                "node_id": peer.node_id.as_ref().unwrap_or(&"unknown".to_string()),
                "peer_id": format!("{:?}", peer.peer_id),
                "addresses": peer.addresses.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
                "connection_state": format!("{:?}", peer.state),
                "last_heartbeat": last_heartbeat,
                "heartbeat_sequence": peer.heartbeat_sequence.unwrap_or(0),
                "rtt_ms": peer.rtt.map(|d| d.as_millis()).unwrap_or(0),
                "connected_since": connected_since,
                "admin_url": peer.admin_url.clone()
            })
        })
        .collect();

    let status = json!({
        "local_node": {
            "node_id": network.config.node_id,
            "listen_addresses": network.config.listen_addresses,
            "peer_id": "local",
            "uptime_seconds": 0  // TODO: Track actual uptime
        },
        "peers": peer_list,
        "statistics": {
            "total_peers": peers.len(),
            "connected_peers": peers.len(),
            "messages_sent": 0,  // TODO: Track from metrics
            "messages_received": 0,
            "bytes_sent": 0,
            "bytes_received": 0
        }
    });

    (StatusCode::OK, Json(status))
}

/// Handler for `/api/network/peers` endpoint.
///
/// Returns detailed information about all known peers.
pub async fn peers_handler(State(network): State<Arc<StorageNetworkHandle>>) -> impl IntoResponse {
    // Get connected peers from the network
    let peers = network.get_connected_peers().await;

    // Convert peer info to JSON
    let peer_list: Vec<_> = peers
        .iter()
        .map(|peer| {
            let last_heartbeat = peer
                .last_heartbeat
                .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                .map(|d| chrono::DateTime::from_timestamp(d.as_secs() as i64, 0))
                .flatten()
                .map(|dt| dt.to_rfc3339())
                .unwrap_or_else(|| "never".to_string());

            json!({
                "node_id": peer.node_id.as_ref().unwrap_or(&"unknown".to_string()),
                "peer_id": format!("{:?}", peer.peer_id),
                "addresses": peer.addresses.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
                "connection_state": format!("{:?}", peer.state),
                "last_heartbeat": last_heartbeat,
                "heartbeat_sequence": peer.heartbeat_sequence.unwrap_or(0),
                "rtt_ms": peer.rtt.map(|d| d.as_millis()).unwrap_or(0),
                "admin_url": peer.admin_url.clone()
            })
        })
        .collect();

    let response = json!({
        "peers": peer_list
    });

    (StatusCode::OK, Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_network::{ConnectionState, PeerInfo};
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    /// Test helper to create PeerInfo instances
    fn create_test_peer_info(
        node_id: &str,
        peer_id_bytes: Vec<u8>,
        ip: IpAddr,
        state: ConnectionState,
    ) -> PeerInfo {
        PeerInfo {
            peer_id: crate::storage_network::PeerId::new(peer_id_bytes),
            node_id: Some(node_id.to_string()),
            addresses: vec![ip],
            state,
            connected_since: Some(SystemTime::now()),
            protocols: vec!["wormfs/1.0.0".to_string()],
            last_heartbeat: Some(SystemTime::now()),
            heartbeat_sequence: Some(42),
            rtt: Some(Duration::from_millis(15)),
            admin_url: Some(format!("http://{}:9090", ip)),
        }
    }

    #[test]
    fn test_peer_info_creation() {
        // Test that we can create PeerInfo instances correctly
        let peer = create_test_peer_info(
            "test-node",
            vec![1, 2, 3, 4],
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)),
            ConnectionState::Connected,
        );

        // Verify peer has expected fields
        assert_eq!(peer.node_id.unwrap(), "test-node");
        assert_eq!(peer.state, ConnectionState::Connected);
        assert!(peer.last_heartbeat.is_some());
        assert_eq!(peer.heartbeat_sequence.unwrap(), 42);
        assert_eq!(peer.rtt.unwrap(), Duration::from_millis(15));
        assert_eq!(peer.admin_url.unwrap(), "http://192.168.1.10:9090");
    }

    #[test]
    fn test_peer_info_with_different_states() {
        let connected_peer = create_test_peer_info(
            "node-1",
            vec![1, 2],
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            ConnectionState::Connected,
        );

        let connecting_peer = create_test_peer_info(
            "node-2",
            vec![3, 4],
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            ConnectionState::Connecting,
        );

        assert_eq!(connected_peer.state, ConnectionState::Connected);
        assert_eq!(connecting_peer.state, ConnectionState::Connecting);
    }

    #[test]
    fn test_peer_info_address_formatting() {
        let ipv4_peer = create_test_peer_info(
            "ipv4-node",
            vec![1],
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)),
            ConnectionState::Connected,
        );

        assert_eq!(ipv4_peer.addresses.len(), 1);
        assert_eq!(ipv4_peer.addresses[0].to_string(), "192.168.1.100");

        // Test admin URL is correctly formatted
        assert_eq!(ipv4_peer.admin_url.unwrap(), "http://192.168.1.100:9090");
    }

    // Note: Handler tests with real StorageNetworkHandle are in
    // tests/admin_network_integration.rs
    // These unit tests verify the helper functions work correctly
}
