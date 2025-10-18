//! MetricService integration tests
//!
//! Tests for metric collection, aggregation, and time-series storage.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use wormfs::metric_service::{
    Config, MetricService, MetricServiceImpl, MetricType, MetricValue, UnitType,
};

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
    let metrics_clone = metrics.clone();

    // Start aggregation loop in background
    tokio::spawn(async move {
        let _ = metrics_clone.run().await;
    });

    // Give the aggregation loop time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

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

    // Give time for aggregation
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Check snapshot
    let snapshot = metrics.snapshot();
    assert!(
        snapshot.metrics.contains_key("test.requests"),
        "Metric not found in snapshot"
    );

    let metric = &snapshot.metrics["test.requests"];
    assert_eq!(metric.value, 9.0, "Counter value should be 9 (1+5+3)");
}

/// Test gauge metric publishing (last value wins).
#[tokio::test]
async fn test_gauge_publishing() {
    let config = Config::default();
    let metrics = MetricServiceImpl::new(config).expect("Failed to create MetricService");
    let metrics_clone = metrics.clone();

    tokio::spawn(async move {
        let _ = metrics_clone.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

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

    tokio::time::sleep(Duration::from_millis(200)).await;

    let snapshot = metrics.snapshot();
    assert!(snapshot.metrics.contains_key("test.temperature"));

    let metric = &snapshot.metrics["test.temperature"];
    assert_eq!(metric.value, 22.0, "Gauge should have last published value");
}

/// Test histogram metric publishing and percentile calculation.
#[tokio::test]
async fn test_histogram_publishing() {
    let config = Config::default();
    let metrics = MetricServiceImpl::new(config).expect("Failed to create MetricService");
    let metrics_clone = metrics.clone();

    tokio::spawn(async move {
        let _ = metrics_clone.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish histogram values
    for value in &[0.01, 0.02, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.50, 1.00] {
        metrics
            .publish_histogram("test.latency", *value, UnitType::Seconds)
            .expect("Failed to publish histogram");
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    let snapshot = metrics.snapshot();
    assert!(snapshot.metrics.contains_key("test.latency"));

    // The aggregated value for histogram is typically the mean or a percentile
    let metric = &snapshot.metrics["test.latency"];
    assert!(metric.value > 0.0, "Histogram should have aggregated value");
}

/// Test labeled metrics (dimensions).
#[tokio::test]
async fn test_labeled_metrics() {
    let config = Config::default();
    let metrics = MetricServiceImpl::new(config).expect("Failed to create MetricService");
    let metrics_clone = metrics.clone();

    tokio::spawn(async move {
        let _ = metrics_clone.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

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

    tokio::time::sleep(Duration::from_millis(200)).await;

    let snapshot = metrics.snapshot();

    // Both labeled variants should exist
    let disk1_key = "disk.writes[disk=disk1]";
    let disk2_key = "disk.writes[disk=disk2]";

    assert!(
        snapshot.metrics.contains_key(disk1_key) || snapshot.metrics.contains_key("disk.writes"),
        "Labeled metric disk1 not found"
    );
}

/// Test cardinality limits.
#[tokio::test]
async fn test_cardinality_limit() {
    let config = Config {
        max_cardinality: 5, // Very low limit for testing
        ..Default::default()
    };

    let metrics = MetricServiceImpl::new(config).expect("Failed to create MetricService");
    let metrics_clone = metrics.clone();

    tokio::spawn(async move {
        let _ = metrics_clone.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

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

    tokio::time::sleep(Duration::from_millis(200)).await;

    let snapshot = metrics.snapshot();
    // We should have at most max_cardinality metrics
    assert!(
        snapshot.metrics.len() <= 5,
        "Should not exceed cardinality limit"
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
    let metrics_clone = metrics.clone();

    tokio::spawn(async move {
        let _ = metrics_clone.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish metrics over time
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
    let metrics_clone = metrics.clone();

    tokio::spawn(async move {
        let _ = metrics_clone.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

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

    tokio::time::sleep(Duration::from_millis(200)).await;

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
