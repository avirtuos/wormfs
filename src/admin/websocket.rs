//! WebSocket support for streaming real-time metrics updates.
//!
//! Provides WebSocket endpoint for pushing live metrics updates to connected clients.

use crate::metric_service::{MetricService, MetricServiceImpl};
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::Response,
};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::broadcast;

/// Broadcast channel capacity for metrics updates
const BROADCAST_CAPACITY: usize = 100;

/// WebSocket state shared across connections
#[derive(Clone)]
pub struct WsState {
    pub metrics: Arc<MetricServiceImpl>,
    pub broadcast_tx: broadcast::Sender<String>,
    /// Shutdown signal sender (shared across clones)
    shutdown_tx: Arc<tokio::sync::broadcast::Sender<()>>,
    /// Background task handle (shared across clones, using parking_lot for sync Drop)
    task_handle: Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl WsState {
    /// Create a new WebSocket state
    pub fn new(metrics: Arc<MetricServiceImpl>) -> Self {
        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        tracing::debug!("Creating new WsState instance");

        Self {
            metrics,
            broadcast_tx,
            shutdown_tx: Arc::new(shutdown_tx),
            task_handle: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Start a background task that periodically broadcasts metrics
    pub fn start_broadcast_task(&self) {
        let metrics = self.metrics.clone();
        let tx = self.broadcast_tx.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tracing::info!("Starting WebSocket broadcast task (interval: 2s)");

        let task_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(2));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Get metrics snapshot
                        let snapshot = metrics.snapshot();
                        tracing::debug!("WebSocket broadcast tick - {} metrics available", snapshot.metrics.len());

                        // Convert to JSON
                        let mut metrics_json = serde_json::Map::new();
                        for (name, metric) in snapshot.metrics.iter() {
                            let metric_obj = serde_json::json!({
                                "value": metric.value,
                                "type": format!("{:?}", metric.metric_type),
                                "unit": format!("{:?}", metric.unit),
                                "timestamp": snapshot.timestamp
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                            });
                            metrics_json.insert(name.clone(), metric_obj);
                        }

                        let payload = serde_json::json!({
                            "metrics": metrics_json,
                            "timestamp": snapshot.timestamp
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                        });

                        if let Ok(json_str) = serde_json::to_string(&payload) {
                            // Send to all connected clients
                            let receiver_count = tx.receiver_count();
                            tracing::debug!(
                                "Broadcasting metrics to {} WebSocket clients ({} bytes)",
                                receiver_count,
                                json_str.len()
                            );

                            match tx.send(json_str) {
                                Ok(_) => {
                                    if receiver_count > 0 {
                                        tracing::trace!("Metrics broadcast sent successfully");
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to broadcast metrics: {}", e);
                                }
                            }
                        } else {
                            tracing::error!("Failed to serialize metrics to JSON");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        // Shutdown signal received
                        tracing::info!("WebSocket broadcast task stopped (shutdown signal)");
                        break;
                    }
                }
            }
        });

        // Store the handle
        *self.task_handle.lock() = Some(task_handle);
    }
}

impl Drop for WsState {
    fn drop(&mut self) {
        // Only send shutdown signal if this is the last clone
        // (Arc::strong_count returns the number of references)
        if Arc::strong_count(&self.shutdown_tx) == 1 {
            tracing::info!(
                "Last WsState instance dropped, sending shutdown signal to broadcast task"
            );
            let _ = self.shutdown_tx.send(());
        } else {
            tracing::debug!(
                "WsState clone dropped, {} references remaining",
                Arc::strong_count(&self.shutdown_tx) - 1
            );
        }
    }
}

/// Handler for WebSocket upgrade requests at `/ws/metrics`
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<WsState>) -> Response {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

/// Handle an individual WebSocket connection
async fn handle_socket(socket: WebSocket, state: WsState) {
    tracing::info!("New WebSocket connection established");

    let (mut sender, mut receiver) = socket.split();

    // Send initial metrics snapshot immediately upon connection
    let initial_snapshot = state.metrics.snapshot();
    let mut metrics_json = serde_json::Map::new();
    for (name, metric) in initial_snapshot.metrics.iter() {
        let metric_obj = serde_json::json!({
            "value": metric.value,
            "type": format!("{:?}", metric.metric_type),
            "unit": format!("{:?}", metric.unit),
            "timestamp": initial_snapshot.timestamp
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        });
        metrics_json.insert(name.clone(), metric_obj);
    }

    let initial_payload = serde_json::json!({
        "metrics": metrics_json,
        "timestamp": initial_snapshot.timestamp
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    });

    if let Ok(json_str) = serde_json::to_string(&initial_payload) {
        tracing::debug!(
            "Sending initial metrics snapshot ({} metrics, {} bytes)",
            initial_snapshot.metrics.len(),
            json_str.len()
        );
        if let Err(e) = sender.send(Message::Text(json_str.into())).await {
            tracing::error!("Failed to send initial snapshot: {}", e);
            return;
        }
    }

    // Subscribe to broadcast channel
    let mut rx = state.broadcast_tx.subscribe();
    tracing::debug!("WebSocket client subscribed to broadcast channel");

    // Spawn task to forward broadcast messages to this WebSocket
    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            tracing::trace!("Forwarding broadcast message to WebSocket client");
            if sender.send(Message::Text(msg.into())).await.is_err() {
                tracing::debug!("WebSocket send failed, client disconnected");
                break;
            }
        }
        tracing::debug!("WebSocket send task ending");
    });

    // Spawn task to handle incoming WebSocket messages (mostly pings/pongs)
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            match msg {
                Message::Close(_) => {
                    break;
                }
                Message::Ping(data) => {
                    // Echo pong back
                    tracing::debug!("Received WebSocket ping");
                    // Can't send directly here, would need channel to sender
                    let _ = data; // Suppress warning
                }
                _ => {
                    // Ignore other message types
                }
            }
        }
    });

    // Wait for either task to finish
    // When one finishes, abort the other
    tokio::select! {
        _ = (&mut send_task) => {
            recv_task.abort();
        }
        _ = (&mut recv_task) => {
            send_task.abort();
        }
    }

    tracing::info!("WebSocket connection closed");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric_service::{Config, MetricService};

    #[tokio::test]
    async fn test_ws_state_creation() {
        let config = Config {
            enabled: true,
            ..Default::default()
        };

        let metrics = Arc::new(MetricServiceImpl::new(config).expect("Failed to create metrics"));
        let ws_state = WsState::new(metrics);

        // Verify state was created
        assert_eq!(ws_state.broadcast_tx.receiver_count(), 0);
    }

    #[tokio::test]
    async fn test_broadcast_channel() {
        let config = Config {
            enabled: true,
            ..Default::default()
        };

        let metrics = Arc::new(MetricServiceImpl::new(config).expect("Failed to create metrics"));
        let ws_state = WsState::new(metrics);

        // Subscribe to broadcast
        let mut rx = ws_state.broadcast_tx.subscribe();

        // Send a test message
        let test_msg = "test message".to_string();
        ws_state.broadcast_tx.send(test_msg.clone()).unwrap();

        // Receive the message
        let received = rx.recv().await.unwrap();
        assert_eq!(received, test_msg);
    }
}
