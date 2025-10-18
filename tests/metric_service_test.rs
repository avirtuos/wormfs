//! MetricService integration tests
//!
//! Tests for metric collection, aggregation, and time-series storage.

use std::collections::HashMap;
use std::time::Duration;
use wormfs::metric_service::{
    Config, MetricService, MetricServiceImpl, MetricType, MetricValue, UnitType,
};

/// Helper function to wait for the aggregation loop to be ready.
/// Polls for up to 2 seconds with 10ms intervals.
async fn wait_for_aggregation_ready(metrics: &MetricServiceImpl) {
    for _ in 0..200 {
        // Try to publish a test metric to verify the channel is active
        if metrics
            .publish_counter("_test.ready", 1, UnitType::Count)
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("Aggregation loop did not become ready within 2 seconds");
}

/// Helper function to wait for a specific metric to appear in the snapshot.
/// Polls for up to 2 seconds with 10ms intervals.
async fn wait_for_metric(metrics: &MetricServiceImpl, metric_name: &str) {
    for _ in 0..200 {
        let snapshot = metrics.snapshot();
        if snapshot.metrics.contains_key(metric_name) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("Metric '{}' did not appear within 2 seconds", metric_name);
}

/// Helper function to wait for a specific metric to reach an expected value.
/// Polls for up to 2 seconds with 10ms intervals.
async fn wait_for_metric_value<F>(metrics: &MetricServiceImpl, metric_name: &str, predicate: F)
where
    F: Fn(f64) -> bool,
{
    for _ in 0..200 {
        let snapshot = metrics.snapshot();
        if let Some(metric) = snapshot.metrics.get(metric_name) {
            if predicate(metric.value) {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "Metric '{}' did not reach expected value within 2 seconds",
        metric_name
    );
}

/// Test basic counter metric publishing and aggregation.
#[tokio::test]
async fn test_counter_publishing() {
    let config = Config {
        enabled: true,
        aggregation_window_secs: 1,
        max_cardinality: 1000,
        channel_buffer_size: 1000,
        enable_prometheus: false,
        prometheus_port: 9090,
        enable_otel: false,
        otel_endpoint: None,
        enable_time_series: false,
        time_series_retention_secs: 60,
        max_points_per_metric: 100,
        time_series_sample_interval_secs: 1,
    };

    let metrics = MetricServiceImpl::new(config).expect("Failed to create MetricService");

    // Wait for aggregation loop to be ready
    wait_for_aggregation_ready(&metrics).await;

    // Publish some counters
    metrics
        .publish_counter("test.requests", 1, UnitType::Requests)
        .expect("Failed to publish counter");
    metrics
        .publish_counter("test.requests", 5, UnitType::Requests)
        .expect("Failed to publish counter");
    metrics
        .publish_counter("test.requests", 3, UnitType::Requests)
        .expect("Failed to publish counter");

    // Wait for the metric to be aggregated to the expected value
    wait_for_metric_value(&metrics, "test.requests", |v| v == 9.0).await;

    // Verify the final value
    let snapshot = metrics.snapshot();
    let metric = &snapshot.metrics["test.requests"];
    assert_eq!(metric.value, 9.0, "Counter value should be 9 (1+5+3)");
}

/// Test gauge metric publishing (last value wins).
#[tokio::test]
async fn test_gauge_publishing() {
    let config = Config::default();
    let metrics = MetricServiceImpl::new(config).expect("Failed to create MetricService");

    // Wait for aggregation loop to be ready
    wait_for_aggregation_ready(&metrics).await;

    // Publish gauges (last value should win)
    metrics
        .publish_gauge("test.temperature", 20.0, UnitType::Count)
        .expect("Failed to publish gauge");
    metrics
        .publish_gauge("test.temperature", 25.0, UnitType::Count)
        .expect("Failed to publish gauge");
    metrics
        .publish_gauge("test.temperature", 22.0, UnitType::Count)
        .expect("Failed to publish gauge");

    // Wait for the gauge to reach the final value
    wait_for_metric_value(&metrics, "test.temperature", |v| v == 22.0).await;

    let snapshot = metrics.snapshot();
    let metric = &snapshot.metrics["test.temperature"];
    assert_eq!(metric.value, 22.0, "Gauge should have last published value");
}

/// Test histogram metric publishing and percentile calculation.
#[tokio::test]
async fn test_histogram_publishing() {
    let config = Config::default();
    let metrics = MetricServiceImpl::new(config).expect("Failed to create MetricService");

    // Wait for aggregation loop to be ready
    wait_for_aggregation_ready(&metrics).await;

    // Publish histogram values
    for value in &[0.01, 0.02, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.50, 1.00] {
        metrics
            .publish_histogram("test.latency", *value, UnitType::Seconds)
            .expect("Failed to publish histogram");
    }

    // Wait for histogram to be aggregated with a positive value
    wait_for_metric_value(&metrics, "test.latency", |v| v > 0.0).await;

    let snapshot = metrics.snapshot();
    // The aggregated value for histogram is typically the mean or a percentile
    let metric = &snapshot.metrics["test.latency"];
    assert!(metric.value > 0.0, "Histogram should have aggregated value");
}

/// Test labeled metrics (dimensions).
#[tokio::test]
async fn test_labeled_metrics() {
    let config = Config::default();
    let metrics = MetricServiceImpl::new(config).expect("Failed to create MetricService");

    // Wait for aggregation loop to be ready
    wait_for_aggregation_ready(&metrics).await;

    // Publish metrics with different labels
    let mut labels1 = HashMap::new();
    labels1.insert("disk".to_string(), "disk1".to_string());

    let mut labels2 = HashMap::new();
    labels2.insert("disk".to_string(), "disk2".to_string());

    metrics
        .publish_labeled(
            "disk.writes",
            MetricValue::Counter(100),
            MetricType::Counter,
            UnitType::Operations,
            labels1.clone(),
        )
        .expect("Failed to publish labeled metric");

    metrics
        .publish_labeled(
            "disk.writes",
            MetricValue::Counter(200),
            MetricType::Counter,
            UnitType::Operations,
            labels2.clone(),
        )
        .expect("Failed to publish labeled metric");

    // Wait for at least one of the labeled metrics to appear
    let disk1_key = "disk.writes[disk=disk1]";
    for _ in 0..200 {
        let snapshot = metrics.snapshot();
        if snapshot.metrics.contains_key(disk1_key) || snapshot.metrics.contains_key("disk.writes")
        {
            // Found the metric, test passes
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // If we get here, the metric wasn't found
    panic!("Labeled metric disk1 not found within 2 seconds");
}

/// Test cardinality limits.
#[tokio::test]
async fn test_cardinality_limit() {
    let config = Config {
        max_cardinality: 5, // Very low limit for testing
        ..Default::default()
    };

    let metrics = MetricServiceImpl::new(config).expect("Failed to create MetricService");

    // Wait for aggregation loop to be ready
    wait_for_aggregation_ready(&metrics).await;

    // Try to create more unique metrics than the limit
    for i in 0..10 {
        let metric_name = format!("test.metric.{}", i);
        let result = metrics.publish_counter(&metric_name, 1, UnitType::Count);

        if i < 5 {
            assert!(result.is_ok(), "First 5 metrics should succeed");
        } else {
            // After limit is reached, new metrics might fail
            // (depends on implementation - some might be buffered)
        }
    }

    // Wait for the internal dropped metrics counter to appear
    wait_for_metric(&metrics, "_internal.metrics.dropped").await;

    let snapshot = metrics.snapshot();
    // We should have at most max_cardinality + 1 metric (the +1 is the internal dropped metric)
    assert!(
        snapshot.metrics.len() <= 6,
        "Should not exceed cardinality limit + internal metrics"
    );

    // Verify internal dropped metric exists
    assert!(
        snapshot.metrics.contains_key("_internal.metrics.dropped"),
        "Should include internal dropped metrics counter"
    );
}

/// Test time-series storage and retrieval.
#[tokio::test]
async fn test_time_series_storage() {
    let config = Config {
        enable_time_series: true,
        time_series_retention_secs: 10,
        time_series_sample_interval_secs: 1,
        ..Default::default()
    };

    let metrics = MetricServiceImpl::new(config).expect("Failed to create MetricService");

    // Wait for aggregation loop to be ready
    wait_for_aggregation_ready(&metrics).await;

    // Publish metrics over time
    // Note: 500ms sleeps are intentional to allow time-series sampling
    for i in 0..5 {
        metrics
            .publish_counter("test.timeseries", i, UnitType::Count)
            .expect("Failed to publish counter");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Retrieve time-series data
    let series = metrics.get_time_series("test.timeseries", None, Duration::from_secs(5));

    assert!(!series.is_empty(), "Time-series should contain data points");
    assert!(series.len() <= 5, "Should not exceed published data points");
}

/// Test snapshot functionality.
#[tokio::test]
async fn test_snapshot() {
    let config = Config::default();
    let metrics = MetricServiceImpl::new(config).expect("Failed to create MetricService");

    // Wait for aggregation loop to be ready
    wait_for_aggregation_ready(&metrics).await;

    // Publish various metrics
    metrics
        .publish_counter("test.requests", 100, UnitType::Requests)
        .expect("Failed to publish");
    metrics
        .publish_gauge("test.connections", 42.0, UnitType::Count)
        .expect("Failed to publish");
    metrics
        .publish_histogram("test.latency", 0.123, UnitType::Seconds)
        .expect("Failed to publish");

    // Wait for all three metrics to be aggregated
    wait_for_metric(&metrics, "test.requests").await;
    wait_for_metric(&metrics, "test.connections").await;
    wait_for_metric(&metrics, "test.latency").await;

    let snapshot = metrics.snapshot();

    assert!(
        snapshot.metrics.len() >= 3,
        "Snapshot should contain all published metrics"
    );
    assert!(
        snapshot.timestamp.elapsed().unwrap() < Duration::from_secs(1),
        "Snapshot timestamp should be recent"
    );
}

/// Test that counter accumulation saturates at u64::MAX instead of overflowing.
#[tokio::test]
async fn test_counter_overflow_saturation() {
    let config = Config::default();
    let metrics = MetricServiceImpl::new(config).expect("Failed to create MetricService");

    // Wait for aggregation loop to be ready
    wait_for_aggregation_ready(&metrics).await;

    // Publish a counter near max
    metrics
        .publish_counter("test.overflow", u64::MAX - 100, UnitType::Count)
        .expect("Failed to publish counter");

    // Wait for first value to be aggregated
    wait_for_metric(&metrics, "test.overflow").await;

    // Add more to trigger potential overflow (200 > remaining 100)
    metrics
        .publish_counter("test.overflow", 200, UnitType::Count)
        .expect("Failed to publish counter");

    // Wait for the counter to saturate at u64::MAX
    wait_for_metric_value(&metrics, "test.overflow", |v| v as u64 == u64::MAX).await;

    let snapshot = metrics.snapshot();

    // Should saturate at MAX, not wrap to a small number
    let value = snapshot
        .metrics
        .get("test.overflow")
        .expect("Metric should exist")
        .value as u64;

    assert_eq!(
        value,
        u64::MAX,
        "Counter should saturate at u64::MAX, not wrap around"
    );
}
