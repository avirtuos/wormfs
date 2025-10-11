//! Common types for the MetricService component.

use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;

/// Configuration for MetricService.
#[derive(Debug, Clone)]
pub struct Config {
    /// Enable metrics collection
    pub enabled: bool,

    /// Aggregation window duration (seconds)
    pub aggregation_window_secs: u64,

    /// Maximum metric cardinality (unique label combinations)
    pub max_cardinality: usize,

    /// Channel buffer size for metric events
    pub channel_buffer_size: usize,

    /// Enable Prometheus exporter
    pub enable_prometheus: bool,

    /// Prometheus export port
    pub prometheus_port: u16,

    /// Enable OpenTelemetry exporter
    pub enable_otel: bool,

    /// OpenTelemetry endpoint
    pub otel_endpoint: Option<String>,
}

/// Errors that can occur during MetricService operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Metric cardinality limit exceeded
    #[error("Metric cardinality limit exceeded: {0}")]
    CardinalityExceeded(usize),

    /// Channel send failed
    #[error("Failed to send metric: {0}")]
    SendFailed(String),

    /// Aggregation loop not running
    #[error("Aggregation loop is not running")]
    NotRunning,

    /// Aggregation loop failed
    #[error("Aggregation loop failed: {0}")]
    AggregationFailed(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),
}

/// Type of metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    /// Monotonically increasing counter
    Counter,
    /// Value that can increase or decrease
    Gauge,
    /// Distribution of values
    Histogram,
    /// Summary with quantiles
    Summary,
    /// Rate calculated over time window
    Rate,
}

/// Unit of measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum UnitType {
    // Counts
    Count,
    Requests,
    Operations,
    Events,

    // Data sizes
    Bytes,
    Kilobytes,
    Megabytes,
    Gigabytes,

    // Rates
    BytesPerSecond,
    RequestsPerSecond,
    OperationsPerSecond,

    // Time durations
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,

    // Percentages
    Percent,
}

/// Metric value (can be counter or gauge).
#[derive(Debug, Clone, Copy)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(f64),
}

/// Snapshot of all metrics at a point in time.
#[derive(Debug, Clone)]
pub struct MetricSnapshot {
    pub timestamp: SystemTime,
    pub metrics: HashMap<String, AggregatedMetric>,
}

/// Aggregated metric data.
#[derive(Debug, Clone)]
pub struct AggregatedMetric {
    pub metric_type: MetricType,
    pub unit: UnitType,
    pub value: f64,
    pub labels: HashMap<String, String>,
}
