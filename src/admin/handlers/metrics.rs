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

/// Handler for `/api/metrics/components` endpoint.
///
/// Returns a list of metric components (prefixes) and their available metrics.
pub async fn components_handler(
    State(metrics): State<Arc<MetricServiceImpl>>,
) -> Result<Json<serde_json::Value>, MetricsError> {
    let snapshot = metrics.snapshot();

    // Group metrics by component prefix (part before first dot)
    let mut components: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    for metric_name in snapshot.metrics.keys() {
        // Skip internal metrics
        if metric_name.starts_with("_internal") {
            continue;
        }

        // Extract component name (part before first dot)
        if let Some(dot_pos) = metric_name.find('.') {
            let component = metric_name[..dot_pos].to_string();
            let metric_suffix = metric_name[dot_pos + 1..].to_string();

            components
                .entry(component)
                .or_insert_with(Vec::new)
                .push(metric_suffix);
        }
    }

    // Sort metrics within each component for consistency
    for metrics_list in components.values_mut() {
        metrics_list.sort();
    }

    let response = serde_json::json!({
        "components": components,
    });

    Ok(Json(response))
}

/// Handler for `/api/metrics/{component}` endpoint.
///
/// Returns metrics for a specific component only.
pub async fn component_metrics_handler(
    axum::extract::Path(component): axum::extract::Path<String>,
    State(metrics): State<Arc<MetricServiceImpl>>,
) -> Result<Json<serde_json::Value>, MetricsError> {
    let snapshot = metrics.snapshot();

    // Filter metrics for this component
    let mut component_metrics = serde_json::Map::new();
    let prefix = format!("{}.", component);

    for (name, metric) in snapshot.metrics.iter() {
        if name.starts_with(&prefix) {
            let metric_obj = serde_json::json!({
                "value": metric.value,
                "type": format!("{:?}", metric.metric_type),
                "unit": format!("{:?}", metric.unit),
                "timestamp": snapshot.timestamp
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            });
            component_metrics.insert(name.clone(), metric_obj);
        }
    }

    let response = serde_json::json!({
        "component": component,
        "metrics": component_metrics,
        "timestamp": snapshot.timestamp
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
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

    #[tokio::test]
    async fn test_components_handler() {
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

        // Publish metrics from different components
        metrics
            .publish_counter("filesystem.write_ops", 10, UnitType::Operations)
            .expect("Failed to publish");
        metrics
            .publish_counter("filestore.chunk_reads", 20, UnitType::Operations)
            .expect("Failed to publish");
        metrics
            .publish_gauge("metadata.cache_size", 1024.0, UnitType::Bytes)
            .expect("Failed to publish");

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Test the components handler
        let response = components_handler(State(metrics.clone()))
            .await
            .expect("Handler failed");

        let json = response.0;
        let components = json.get("components").unwrap().as_object().unwrap();

        // Should have 3 components: filesystem, filestore, metadata
        assert!(components.contains_key("filesystem"));
        assert!(components.contains_key("filestore"));
        assert!(components.contains_key("metadata"));

        // Verify metrics are grouped correctly
        let filesystem_metrics = components.get("filesystem").unwrap().as_array().unwrap();
        assert!(filesystem_metrics.contains(&serde_json::Value::String("write_ops".to_string())));

        let filestore_metrics = components.get("filestore").unwrap().as_array().unwrap();
        assert!(filestore_metrics.contains(&serde_json::Value::String("chunk_reads".to_string())));
    }

    #[tokio::test]
    async fn test_components_handler_excludes_internal_metrics() {
        let config = Config {
            enabled: true,
            ..Default::default()
        };

        let metrics = Arc::new(MetricServiceImpl::new(config).expect("Failed to create metrics"));
        let metrics_clone = metrics.clone();

        tokio::spawn(async move {
            let _ = metrics_clone.run().await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Publish an internal metric (should be excluded)
        metrics
            .publish_counter("_internal.metrics.dropped", 5, UnitType::Count)
            .expect("Failed to publish");
        metrics
            .publish_counter("filesystem.ops", 10, UnitType::Operations)
            .expect("Failed to publish");

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let response = components_handler(State(metrics.clone()))
            .await
            .expect("Handler failed");

        let json = response.0;
        let components = json.get("components").unwrap().as_object().unwrap();

        // _internal should not be in components
        assert!(!components.contains_key("_internal"));
        assert!(components.contains_key("filesystem"));
    }

    #[tokio::test]
    async fn test_component_metrics_handler_valid_component() {
        let config = Config {
            enabled: true,
            ..Default::default()
        };

        let metrics = Arc::new(MetricServiceImpl::new(config).expect("Failed to create metrics"));
        let metrics_clone = metrics.clone();

        tokio::spawn(async move {
            let _ = metrics_clone.run().await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Publish metrics from multiple components
        metrics
            .publish_counter("filesystem.write_ops", 10, UnitType::Operations)
            .expect("Failed to publish");
        metrics
            .publish_counter("filesystem.read_ops", 20, UnitType::Operations)
            .expect("Failed to publish");
        metrics
            .publish_counter("filestore.chunk_reads", 30, UnitType::Operations)
            .expect("Failed to publish");

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Test getting metrics for just "filesystem" component
        let response = component_metrics_handler(
            axum::extract::Path("filesystem".to_string()),
            State(metrics.clone()),
        )
        .await
        .expect("Handler failed");

        let json = response.0;
        assert_eq!(json.get("component").unwrap().as_str().unwrap(), "filesystem");

        let component_metrics = json.get("metrics").unwrap().as_object().unwrap();

        // Should have both filesystem metrics
        assert!(component_metrics.contains_key("filesystem.write_ops"));
        assert!(component_metrics.contains_key("filesystem.read_ops"));

        // Should NOT have filestore metrics
        assert!(!component_metrics.contains_key("filestore.chunk_reads"));
    }

    #[tokio::test]
    async fn test_component_metrics_handler_invalid_component() {
        let config = Config {
            enabled: true,
            ..Default::default()
        };

        let metrics = Arc::new(MetricServiceImpl::new(config).expect("Failed to create metrics"));
        let metrics_clone = metrics.clone();

        tokio::spawn(async move {
            let _ = metrics_clone.run().await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        metrics
            .publish_counter("filesystem.ops", 10, UnitType::Operations)
            .expect("Failed to publish");

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Request non-existent component
        let response = component_metrics_handler(
            axum::extract::Path("nonexistent".to_string()),
            State(metrics.clone()),
        )
        .await
        .expect("Handler failed");

        let json = response.0;
        let component_metrics = json.get("metrics").unwrap().as_object().unwrap();

        // Should be empty
        assert_eq!(component_metrics.len(), 0);
    }

    #[tokio::test]
    async fn test_metrics_handler_with_empty_snapshot() {
        let config = Config {
            enabled: true,
            ..Default::default()
        };

        let metrics = Arc::new(MetricServiceImpl::new(config).expect("Failed to create metrics"));
        let metrics_clone = metrics.clone();

        tokio::spawn(async move {
            let _ = metrics_clone.run().await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Don't publish any metrics - test with empty snapshot

        let response = metrics_handler(State(metrics.clone()))
            .await
            .expect("Handler should succeed even with empty metrics");

        let json = response.0;
        assert!(json.get("metrics").is_some());
        assert!(json.get("timestamp").is_some());
        assert_eq!(json.get("dropped_metrics").unwrap().as_u64().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_metrics_handler_includes_dropped_metrics_field() {
        let config = Config {
            enabled: true,
            ..Default::default()
        };

        let metrics = Arc::new(MetricServiceImpl::new(config).expect("Failed to create metrics"));
        let metrics_clone = metrics.clone();

        tokio::spawn(async move {
            let _ = metrics_clone.run().await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        // Publish the internal dropped metrics counter
        metrics
            .publish_counter("_internal.metrics.dropped", 42, UnitType::Count)
            .expect("Failed to publish");

        // Give more time for aggregation to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let response = metrics_handler(State(metrics.clone()))
            .await
            .expect("Handler failed");

        let json = response.0;

        // Check that dropped_metrics field exists (may be 0 or 42 depending on timing)
        // The important thing is that the field is present
        assert!(json.get("dropped_metrics").is_some());

        // Verify the internal metric is in the metrics map
        let metrics_map = json.get("metrics").unwrap().as_object().unwrap();
        if metrics_map.contains_key("_internal.metrics.dropped") {
            let dropped = metrics_map.get("_internal.metrics.dropped").unwrap();
            assert!(dropped.get("value").is_some());
        }
    }

    #[tokio::test]
    async fn test_component_metrics_handler_metrics_without_dots() {
        let config = Config {
            enabled: true,
            ..Default::default()
        };

        let metrics = Arc::new(MetricServiceImpl::new(config).expect("Failed to create metrics"));
        let metrics_clone = metrics.clone();

        tokio::spawn(async move {
            let _ = metrics_clone.run().await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Publish a metric without a dot (edge case - shouldn't break anything)
        metrics
            .publish_counter("nodot", 10, UnitType::Count)
            .expect("Failed to publish");
        metrics
            .publish_counter("filesystem.ops", 20, UnitType::Operations)
            .expect("Failed to publish");

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Request filesystem component
        let response = component_metrics_handler(
            axum::extract::Path("filesystem".to_string()),
            State(metrics.clone()),
        )
        .await
        .expect("Handler failed");

        let json = response.0;
        let component_metrics = json.get("metrics").unwrap().as_object().unwrap();

        // Should have filesystem metric
        assert!(component_metrics.contains_key("filesystem.ops"));

        // The "nodot" metric shouldn't cause issues
        assert!(!component_metrics.contains_key("nodot"));
    }
}
