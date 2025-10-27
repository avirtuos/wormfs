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
                "connected_since": connected_since
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
                "rtt_ms": peer.rtt.map(|d| d.as_millis()).unwrap_or(0)
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
    // Note: Tests removed because they require a real StorageNetworkHandle.
    // Network handler tests should be done at the integration level with a real network.
}
