//! Network status handler for admin API endpoints.
//!
//! Provides handlers for viewing peer connectivity and network health.

use crate::storage_network::{PeerInfo, StorageNetworkHandle};
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::warn;

/// Helper to convert SystemTime to RFC3339 string with error logging.
///
/// Returns "never" for last_heartbeat fields and "unknown" for other fields
/// when the timestamp cannot be converted.
///
/// # Arguments
/// * `time` - Optional SystemTime to convert
/// * `peer_id` - Peer identifier for logging
/// * `field_name` - Name of the field being converted for logging
fn convert_timestamp(time: Option<SystemTime>, peer_id: &str, field_name: &str) -> String {
    time.and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
        .and_then(|d| {
            chrono::DateTime::from_timestamp(d.as_secs() as i64, 0).or_else(|| {
                warn!(
                    "Invalid {} timestamp for peer {}: {} seconds",
                    field_name,
                    peer_id,
                    d.as_secs()
                );
                None
            })
        })
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| {
            if field_name == "last_heartbeat" {
                "never".to_string()
            } else {
                "unknown".to_string()
            }
        })
}

/// Convert a PeerInfo instance to JSON representation.
///
/// This helper centralizes the peer-to-JSON conversion logic used by both
/// network_status_handler and peers_handler, eliminating code duplication.
///
/// # Arguments
/// * `peer` - The peer information to convert
/// * `include_connected_since` - Whether to include the connected_since field
fn peer_to_json(peer: &PeerInfo, include_connected_since: bool) -> serde_json::Value {
    // Extract peer ID for logging
    let peer_id = peer
        .node_id
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or("unknown");

    // Convert last_heartbeat timestamp with error logging
    let last_heartbeat = convert_timestamp(peer.last_heartbeat, peer_id, "last_heartbeat");

    // Build base JSON object
    let mut json_obj = json!({
        "node_id": peer.node_id.as_ref().unwrap_or(&"unknown".to_string()),
        "peer_id": format!("{:?}", peer.peer_id),
        "addresses": peer.addresses.iter().map(|a| a.to_string()).collect::<Vec<_>>(),
        "connection_state": format!("{:?}", peer.state),
        "last_heartbeat": last_heartbeat,
        "heartbeat_sequence": peer.heartbeat_sequence.unwrap_or(0),
        "rtt_ms": peer.rtt.map(|d| d.as_millis()).unwrap_or(0),
        "admin_url": peer.admin_url.clone()
    });

    // Conditionally add connected_since field
    if include_connected_since {
        let connected_since = convert_timestamp(peer.connected_since, peer_id, "connected_since");
        json_obj["connected_since"] = json!(connected_since);
    }

    json_obj
}

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

    // Convert peer info to JSON (include connected_since for network status)
    let peer_list: Vec<_> = peers.iter().map(|peer| peer_to_json(peer, true)).collect();

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

    // Convert peer info to JSON (exclude connected_since for peers endpoint)
    let peer_list: Vec<_> = peers.iter().map(|peer| peer_to_json(peer, false)).collect();

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

    #[test]
    fn test_timestamp_conversion_valid() {
        // Test valid timestamp conversion
        let now = SystemTime::now();
        let duration = now.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let timestamp_secs = duration.as_secs() as i64;

        // This should succeed
        let dt = chrono::DateTime::from_timestamp(timestamp_secs, 0);
        assert!(dt.is_some(), "Valid timestamp should convert successfully");
    }

    #[test]
    fn test_timestamp_conversion_out_of_range() {
        // Test with extremely large timestamp (year 2262+)
        let far_future_secs = i64::MAX;

        // This should return None
        let dt = chrono::DateTime::from_timestamp(far_future_secs, 0);
        assert!(dt.is_none(), "Out of range timestamp should return None");
    }

    #[test]
    fn test_timestamp_conversion_negative() {
        // Test with negative timestamp (before Unix epoch)
        let negative_secs = -100_000_000_000i64; // Very far in the past

        // This might be valid or might return None depending on how far back
        // The important thing is it doesn't panic
        let _dt = chrono::DateTime::from_timestamp(negative_secs, 0);
        // No assertion needed - just verifying it doesn't panic
    }

    #[test]
    fn test_peer_info_with_none_timestamps() {
        // Create a peer with no timestamps
        let peer = PeerInfo {
            peer_id: crate::storage_network::PeerId::new(vec![1, 2, 3]),
            node_id: Some("test-node".to_string()),
            addresses: vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))],
            state: ConnectionState::Connected,
            connected_since: None,
            protocols: vec!["wormfs/1.0.0".to_string()],
            last_heartbeat: None,
            heartbeat_sequence: None,
            rtt: None,
            admin_url: None,
        };

        // Verify that None timestamps are handled correctly
        assert!(peer.last_heartbeat.is_none());
        assert!(peer.connected_since.is_none());
    }

    #[test]
    fn test_convert_timestamp_valid() {
        // Test with current time
        let now = SystemTime::now();
        let result = convert_timestamp(Some(now), "test-peer", "last_heartbeat");

        // Should return RFC3339 formatted string
        assert_ne!(result, "never");
        assert_ne!(result, "unknown");
        // Verify it looks like an RFC3339 timestamp
        assert!(result.contains('T'));
        assert!(result.contains('Z') || result.contains('+') || result.contains('-'));
    }

    #[test]
    fn test_convert_timestamp_none() {
        // Test with None - should return default based on field name
        let result_heartbeat = convert_timestamp(None, "test-peer", "last_heartbeat");
        assert_eq!(result_heartbeat, "never");

        let result_other = convert_timestamp(None, "test-peer", "connected_since");
        assert_eq!(result_other, "unknown");
    }

    #[test]
    fn test_peer_to_json_with_connected_since() {
        // Create a test peer
        let peer = create_test_peer_info(
            "node-1",
            vec![1, 2, 3],
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)),
            ConnectionState::Connected,
        );

        // Convert to JSON with connected_since
        let json = peer_to_json(&peer, true);

        // Verify all expected fields are present
        assert_eq!(json["node_id"], "node-1");
        assert!(json["peer_id"].is_string());
        assert!(json["addresses"].is_array());
        assert!(json["connection_state"].is_string());
        assert!(json["last_heartbeat"].is_string());
        assert_eq!(json["heartbeat_sequence"], 42);
        assert!(json["rtt_ms"].is_number());
        assert!(json["admin_url"].is_string());

        // Verify connected_since is included
        assert!(json["connected_since"].is_string());
        assert_ne!(json["connected_since"], serde_json::Value::Null);
    }

    #[test]
    fn test_peer_to_json_without_connected_since() {
        // Create a test peer
        let peer = create_test_peer_info(
            "node-2",
            vec![4, 5, 6],
            IpAddr::V4(Ipv4Addr::new(192, 168, 1, 20)),
            ConnectionState::Connected,
        );

        // Convert to JSON without connected_since
        let json = peer_to_json(&peer, false);

        // Verify all expected fields are present
        assert_eq!(json["node_id"], "node-2");
        assert!(json["last_heartbeat"].is_string());

        // Verify connected_since is NOT included
        assert_eq!(json.get("connected_since"), None);
    }

    #[test]
    fn test_peer_to_json_with_none_values() {
        // Create a peer with None values
        let peer = PeerInfo {
            peer_id: crate::storage_network::PeerId::new(vec![1, 2, 3]),
            node_id: None,
            addresses: vec![IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))],
            state: ConnectionState::Connecting,
            connected_since: None,
            protocols: vec![],
            last_heartbeat: None,
            heartbeat_sequence: None,
            rtt: None,
            admin_url: None,
        };

        // Convert to JSON with connected_since
        let json = peer_to_json(&peer, true);

        // Verify defaults are used
        assert_eq!(json["node_id"], "unknown");
        assert_eq!(json["last_heartbeat"], "never");
        assert_eq!(json["connected_since"], "unknown");
        assert_eq!(json["heartbeat_sequence"], 0);
        assert_eq!(json["rtt_ms"], 0);
        assert!(json["admin_url"].is_null());
    }

    // Note: Handler tests with real StorageNetworkHandle are in
    // tests/admin_network_integration.rs
    // These unit tests verify the helper functions and timestamp handling work correctly
}
