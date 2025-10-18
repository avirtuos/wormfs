//! HTTP server for exposing metrics via HTTP/JSON endpoints.
//!
//! Provides a simple HTTP server that exposes metrics snapshots in JSON format,
//! suitable for consumption by monitoring tools, dashboards, and scripts.

use super::{MetricService, MetricServiceImpl};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use std::sync::Arc;

/// Start the HTTP metrics server.
///
/// This spawns a background task that runs an HTTP server on the specified port.
/// The server exposes two endpoints:
/// - GET `/metrics` - Returns JSON snapshot of all metrics
/// - GET `/health` - Returns simple health check
///
/// # Arguments
///
/// * `metrics` - MetricService instance to query for metrics
/// * `port` - Port to bind the HTTP server to
///
/// # Returns
///
/// A `tokio::task::JoinHandle` for the background server task.
pub fn start_metrics_server(
    metrics: Arc<MetricServiceImpl>,
    port: u16,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let app = create_router(metrics);
        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

        tracing::info!("Starting metrics HTTP server on http://{}", addr);

        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!("Failed to bind metrics server to {}: {}", addr, e);
                return;
            }
        };

        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("Metrics server error: {}", e);
        }
    })
}

/// Create the Axum router with all metrics endpoints.
fn create_router(metrics: Arc<MetricServiceImpl>) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .with_state(metrics)
}

/// Handler for `/metrics` endpoint.
///
/// Returns a JSON snapshot of all current metrics.
async fn metrics_handler(
    State(metrics): State<Arc<MetricServiceImpl>>,
) -> Result<Json<serde_json::Value>, MetricsError> {
    let snapshot = metrics.snapshot();

    // Convert snapshot to JSON-friendly format
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

    let response = serde_json::json!({
        "metrics": metrics_json,
        "timestamp": snapshot.timestamp
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    });

    Ok(Json(response))
}

/// Handler for `/health` endpoint.
///
/// Returns a simple health check response.
async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy",
        "service": "wormfs-metrics"
    }))
}

/// Error type for metrics endpoints.
#[derive(Debug)]
struct MetricsError(String);

impl IntoResponse for MetricsError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": self.0
            })),
        )
            .into_response()
    }
}

impl From<String> for MetricsError {
    fn from(s: String) -> Self {
        MetricsError(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric_service::{Config, MetricService, UnitType};

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let config = Config {
            enabled: true,
            ..Default::default()
        };

        let metrics = Arc::new(MetricServiceImpl::new(config).expect("Failed to create metrics"));
        let metrics_clone = metrics.clone();

        // Start aggregation loop
        tokio::spawn(async move {
            let _ = metrics_clone.run().await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Publish some test metrics
        metrics
            .publish_counter("test.requests", 42, UnitType::Requests)
            .expect("Failed to publish");

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Test the handler
        let response = metrics_handler(State(metrics.clone()))
            .await
            .expect("Handler failed");

        let json = response.0;
        assert!(json.get("metrics").is_some());
        assert!(json.get("timestamp").is_some());

        // Verify metric is present
        let metrics_obj = json.get("metrics").unwrap().as_object().unwrap();
        assert!(metrics_obj.contains_key("test.requests"));
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let response = health_handler().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
