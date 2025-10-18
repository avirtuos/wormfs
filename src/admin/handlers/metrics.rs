//! Metrics handler for admin API endpoints.
//!
//! Provides handlers for metrics endpoints that expose metrics snapshots
//! in JSON format for consumption by the UI and external tools.

use crate::metric_service::{MetricService, MetricServiceImpl};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use std::sync::Arc;

/// Handler for `/api/metrics` endpoint.
///
/// Returns a JSON snapshot of all current metrics.
pub async fn metrics_handler(
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

    // Extract dropped metrics count for top-level field
    let dropped_metrics = snapshot
        .metrics
        .get("_internal.metrics.dropped")
        .map(|m| m.value as u64)
        .unwrap_or(0);

    let response = serde_json::json!({
        "metrics": metrics_json,
        "timestamp": snapshot.timestamp
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        "dropped_metrics": dropped_metrics,
    });

    Ok(Json(response))
}

/// Handler for `/api/health` endpoint.
///
/// Returns a simple health check response.
pub async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy",
        "service": "wormfs-admin"
    }))
}

/// Error type for metrics endpoints.
#[derive(Debug)]
pub struct MetricsError(String);

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
    async fn test_metrics_handler() {
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
    async fn test_health_handler() {
        let response = health_handler().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
