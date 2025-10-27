//! Network status handler for admin API endpoints.
//!
//! Provides handlers for viewing peer connectivity and network health.

use axum::{http::StatusCode, response::IntoResponse, Json};

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
pub async fn network_status_handler() -> impl IntoResponse {
    // TODO: Get actual network status from StorageNode
    // For now, return placeholder data for UI development
    let status = serde_json::json!({
        "local_node": {
            "node_id": "wormfs-node-001",
            "listen_address": "127.0.0.1:7000",
            "peer_id": "12D3KooWRandom123456",
            "uptime_seconds": 3600
        },
        "peers": [
            {
                "node_id": "wormfs-node-002",
                "peer_id": "12D3KooWRandom789012",
                "addresses": ["127.0.0.1:7001"],
                "connection_state": "Connected",
                "last_heartbeat": "2025-10-27T10:30:00Z",
                "heartbeat_sequence": 42,
                "rtt_ms": 5,
                "connected_since": "2025-10-27T10:00:00Z"
            }
        ],
        "statistics": {
            "total_peers": 1,
            "connected_peers": 1,
            "messages_sent": 150,
            "messages_received": 148,
            "bytes_sent": 1024000,
            "bytes_received": 998400
        }
    });

    (StatusCode::OK, Json(status))
}

/// Handler for `/api/network/peers` endpoint.
///
/// Returns detailed information about all known peers.
pub async fn peers_handler() -> impl IntoResponse {
    // TODO: Get actual peer list from StorageNode
    let peers = serde_json::json!({
        "peers": [
            {
                "node_id": "wormfs-node-002",
                "peer_id": "12D3KooWRandom789012",
                "addresses": ["127.0.0.1:7001"],
                "connection_state": "Connected",
                "last_heartbeat": "2025-10-27T10:30:00Z",
                "heartbeat_sequence": 42,
                "rtt_ms": 5
            }
        ]
    });

    (StatusCode::OK, Json(peers))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_network_status_handler() {
        let response = network_status_handler().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_peers_handler() {
        let response = peers_handler().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
