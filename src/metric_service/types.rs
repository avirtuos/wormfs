//! Common types for the MetricService component.

use std::collections::HashMap;
use std::time::SystemTime;
use thiserror::Error;

/// Configuration for MetricService.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

    /// Enable time-series storage for graphing
    pub enable_time_series: bool,

    /// Retention window for time-series data (seconds)
    pub time_series_retention_secs: u64,

    /// Maximum data points per metric (memory limit)
    pub max_points_per_metric: usize,

    /// Sample interval for time-series (seconds)
    /// Metrics published more frequently will be downsampled
    pub time_series_sample_interval_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            aggregation_window_secs: 60,
            max_cardinality: 10_000,
            channel_buffer_size: 10_000,
            enable_prometheus: false,
            prometheus_port: 9090,
            enable_otel: false,
            otel_endpoint: None,
            enable_time_series: true,
            time_series_retention_secs: 3600, // 1 hour
            max_points_per_metric: 1000,
            time_series_sample_interval_secs: 1, // 1 second
        }
    }
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
    /// Time-series data if enabled (metric_name -> Vec<(timestamp, value)>)
    pub time_series: Option<HashMap<String, Vec<(SystemTime, f64)>>>,
}

/// Aggregated metric data.
#[derive(Debug, Clone)]
pub struct AggregatedMetric {
    pub metric_type: MetricType,
    pub unit: UnitType,
    pub value: f64,
    pub labels: HashMap<String, String>,
}
