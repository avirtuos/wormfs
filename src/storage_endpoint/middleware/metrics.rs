//! Metrics collection middleware for gRPC requests.

use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error};

use crate::metric_service::{MetricService, UnitType};

/// Middleware for collecting request metrics.
///
/// Collects metrics for each gRPC request including:
/// - Request count per service/method
/// - Request latency per service/method
/// - Success/error counts per service/method
#[derive(Clone)]
pub struct MetricsMiddleware<M: MetricService> {
    metrics: M,
    enabled: bool,
}

impl<M: MetricService> MetricsMiddleware<M> {
    /// Create a new metrics middleware.
    ///
    /// # Arguments
    ///
    /// * `metrics` - MetricService instance for publishing metrics
    /// * `enabled` - Whether metrics collection is enabled
    pub fn new(metrics: M, enabled: bool) -> Self {
        Self { metrics, enabled }
    }

    /// Record request start (returns timestamp for latency calculation).
    ///
    /// # Arguments
    ///
    /// * `service` - Service name (e.g., "filesystem", "chunk")
    /// * `method` - Method name (e.g., "create_file", "read_chunk")
    ///
    /// # Returns
    ///
    /// Instant timestamp for latency calculation.
    pub fn request_start(&self, service: &str, method: &str) -> Instant {
        if !self.enabled {
            return Instant::now();
        }

        let metric_name = format!("grpc.{}.{}.requests.total", service, method);
        if let Err(e) = self
            .metrics
            .publish_counter(&metric_name, 1, UnitType::Count)
        {
            error!("Failed to publish request counter: {}", e);
        }

        debug!("gRPC request started: {}.{}", service, method);
        Instant::now()
    }

    /// Record request completion.
    ///
    /// # Arguments
    ///
    /// * `service` - Service name
    /// * `method` - Method name
    /// * `start` - Start timestamp from `request_start()`
    /// * `success` - Whether the request succeeded
    pub fn request_end(&self, service: &str, method: &str, start: Instant, success: bool) {
        if !self.enabled {
            return;
        }

        let latency = start.elapsed().as_secs_f64();

        // Record latency
        let latency_metric = format!("grpc.{}.{}.latency", service, method);
        if let Err(e) = self
            .metrics
            .publish_histogram(&latency_metric, latency, UnitType::Seconds)
        {
            error!("Failed to publish latency histogram: {}", e);
        }

        // Record success/error count
        let result_metric = if success {
            format!("grpc.{}.{}.success.total", service, method)
        } else {
            format!("grpc.{}.{}.errors.total", service, method)
        };

        if let Err(e) = self
            .metrics
            .publish_counter(&result_metric, 1, UnitType::Count)
        {
            error!("Failed to publish result counter: {}", e);
        }

        debug!(
            "gRPC request completed: {}.{} (success={}, latency={:.3}s)",
            service, method, success, latency
        );
    }

    /// Record request error with error type.
    ///
    /// # Arguments
    ///
    /// * `service` - Service name
    /// * `method` - Method name
    /// * `error_code` - gRPC error code
    pub fn record_error(&self, service: &str, method: &str, error_code: &str) {
        if !self.enabled {
            return;
        }

        let metric_name = format!("grpc.{}.{}.errors.{}", service, method, error_code);
        if let Err(e) = self
            .metrics
            .publish_counter(&metric_name, 1, UnitType::Count)
        {
            error!("Failed to publish error counter: {}", e);
        }
    }

    /// Record streaming request metrics.
    ///
    /// # Arguments
    ///
    /// * `service` - Service name
    /// * `method` - Method name
    /// * `chunks_sent` - Number of chunks sent in the stream
    /// * `bytes_sent` - Total bytes sent
    pub fn record_stream(
        &self,
        service: &str,
        method: &str,
        chunks_sent: usize,
        bytes_sent: usize,
    ) {
        if !self.enabled {
            return;
        }

        let chunks_metric = format!("grpc.{}.{}.stream.chunks", service, method);
        if let Err(e) =
            self.metrics
                .publish_counter(&chunks_metric, chunks_sent as u64, UnitType::Count)
        {
            error!("Failed to publish stream chunks counter: {}", e);
        }

        let bytes_metric = format!("grpc.{}.{}.stream.bytes", service, method);
        if let Err(e) =
            self.metrics
                .publish_counter(&bytes_metric, bytes_sent as u64, UnitType::Bytes)
        {
            error!("Failed to publish stream bytes counter: {}", e);
        }
    }

    /// Check if metrics are enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric_service::MockMetricService;
    use std::time::Duration;

    #[test]
    fn test_metrics_disabled() {
        let mock_metrics = MockMetricService::default();
        let middleware = MetricsMiddleware::new(mock_metrics, false);

        let start = middleware.request_start("test", "method");
        middleware.request_end("test", "method", start, true);

        // With metrics disabled, no calls should be made
        // This test just ensures it doesn't panic
    }

    #[tokio::test]
    async fn test_metrics_request_flow() {
        let mock_metrics = MockMetricService::default();
        let middleware = MetricsMiddleware::new(mock_metrics, true);

        let start = middleware.request_start("filesystem", "create_file");
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Should not panic - metrics are published internally
        middleware.request_end("filesystem", "create_file", start, true);
    }

    #[test]
    fn test_metrics_error_recording() {
        let mock_metrics = MockMetricService::default();
        let middleware = MetricsMiddleware::new(mock_metrics, true);

        // Should not panic - metrics are published internally
        middleware.record_error("chunk", "write_chunk", "permission_denied");
    }

    #[test]
    fn test_metrics_stream_recording() {
        let mock_metrics = MockMetricService::default();
        let middleware = MetricsMiddleware::new(mock_metrics, true);

        // Should not panic - metrics are published internally
        middleware.record_stream("filesystem", "read_file", 10, 65536);
    }
}
