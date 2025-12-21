//! # MetricService Component
//!
//! MetricService provides centralized metrics collection and aggregation for all WormFS components.
//!
//! ## Responsibilities
//!
//! - Accepting metric events from all components via low-overhead publish API
//! - Aggregating raw metric events into meaningful statistics (rates, totals, percentiles)
//! - Supporting multiple metric types (counters, gauges, histograms, summaries)
//! - Exporting metrics to various backends (Prometheus, OpenTelemetry, logs)
//! - Maintaining metric metadata (names, labels, units, descriptions)
//! - Managing metric cardinality to prevent memory exhaustion
//! - Providing query APIs for internal health checks and debugging
//! - Supporting metric sampling and aggregation windows
//!
//! ## Architecture: Client Pattern with Interior Mutability
//!
//! MetricService uses a channel-based pattern where:
//! 1. The outer `MetricService` struct is lightweight and cloneable
//! 2. Components send metrics through an unbounded channel
//! 3. A background aggregation loop processes metrics asynchronously
//! 4. No locks are held during metric publishing (lock-free from caller perspective)
//! 5. Aggregated metrics are stored for export
//!
//! ## Metric Types
//!
//! - **Counter**: Monotonically increasing value (e.g., total requests)
//! - **Gauge**: Value that can increase or decrease (e.g., active connections)
//! - **Histogram**: Distribution of values (e.g., request latency)
//! - **Summary**: Like histogram but with configurable quantiles
//! - **Rate**: Calculated over time window (e.g., requests per second)
//!
//! ## Publishing Flow
//!
//! ```text
//! Component Threads
//!      │
//!      ├─── publish_metric() ───┐
//!      │                        │
//!      └─── publish_metric() ───┤
//!                               │
//!                               ▼
//!                    MetricEvent Channel
//!                               │
//!                               ▼
//!                   Background Aggregation Loop
//!                               │
//!                               ▼
//!                         MetricRegistry
//!                               │
//!                               ▼
//!                  Exporters (Prometheus, OTEL, Logs)
//! ```
//!
//! ## Example Usage
//!
//! ```ignore
//! let metrics = MetricService::new(config)?;
//!
//! // Publish a counter
//! metrics.publish_counter("raft.proposals.total", 1, UnitType::Operations)?;
//!
//! // Publish a histogram (latency measurement)
//! metrics.publish_histogram("raft.proposal.latency", elapsed_secs, UnitType::Seconds)?;
//!
//! // Publish with labels
//! let mut labels = HashMap::new();
//! labels.insert("disk_id".to_string(), "disk1".to_string());
//! metrics.publish_labeled("filestore.chunk_writes", 1, MetricType::Counter, UnitType::Operations, labels)?;
//! ```

pub mod implementation;
pub mod types;

use async_trait::async_trait;
pub use implementation::MetricServiceImpl;
use std::collections::HashMap;
use std::time::Duration;
pub use types::{
    AggregatedMetric, Config, Error, MetricSnapshot, MetricType, MetricValue, UnitType,
};

/// MetricService trait defines the interface for metrics collection and aggregation.
///
/// Implementations provide lock-free metric publishing with background aggregation.
///
/// Note: This trait cannot be automocked due to the Clone bound requirement.
/// Manual mocking or alternative testing strategies should be used.
#[async_trait]
pub trait MetricService: Send + Sync + Clone {
    /// Create a new MetricService.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration including aggregation settings and exporters
    ///
    /// # Returns
    ///
    /// A cloneable MetricService handle that can be shared across components.
    fn new(config: Config) -> Result<Self, Error>
    where
        Self: Sized;

    /// Start the background aggregation loop.
    ///
    /// This method must be called once to start processing metric events.
    ///
    /// # Errors
    ///
    /// Returns an error if the aggregation loop cannot be started.
    async fn run(&self) -> Result<(), Error>;

    /// Publish a counter metric.
    ///
    /// Counters are monotonically increasing values like total requests.
    ///
    /// # Arguments
    ///
    /// * `name` - Metric name (e.g., "raft.proposals.total")
    /// * `value` - Counter increment value
    /// * `unit` - Unit of measurement
    ///
    /// # Errors
    ///
    /// Returns an error if publishing fails.
    fn publish_counter(&self, name: &str, value: u64, unit: UnitType) -> Result<(), Error>;

    /// Publish a gauge metric.
    ///
    /// Gauges are values that can increase or decrease like active connections.
    ///
    /// # Arguments
    ///
    /// * `name` - Metric name
    /// * `value` - Current gauge value
    /// * `unit` - Unit of measurement
    ///
    /// # Errors
    ///
    /// Returns an error if publishing fails.
    fn publish_gauge(&self, name: &str, value: f64, unit: UnitType) -> Result<(), Error>;

    /// Publish a histogram observation.
    ///
    /// Histograms track distributions like latency percentiles.
    ///
    /// # Arguments
    ///
    /// * `name` - Metric name
    /// * `value` - Observed value
    /// * `unit` - Unit of measurement
    ///
    /// # Errors
    ///
    /// Returns an error if publishing fails.
    fn publish_histogram(&self, name: &str, value: f64, unit: UnitType) -> Result<(), Error>;

    /// Publish a metric with labels.
    ///
    /// Labels allow dimensional metrics (e.g., per-disk statistics).
    ///
    /// # Arguments
    ///
    /// * `name` - Metric name
    /// * `value` - Metric value
    /// * `metric_type` - Type of metric
    /// * `unit` - Unit of measurement
    /// * `labels` - Key-value labels
    ///
    /// # Errors
    ///
    /// Returns an error if publishing fails or cardinality limit exceeded.
    fn publish_labeled(
        &self,
        name: &str,
        value: MetricValue,
        metric_type: MetricType,
        unit: UnitType,
        labels: HashMap<String, String>,
    ) -> Result<(), Error>;

    /// Get current snapshot of all metrics.
    ///
    /// # Returns
    ///
    /// Snapshot containing all aggregated metrics.
    fn snapshot(&self) -> MetricSnapshot;

    /// Get time-series data for a specific metric.
    ///
    /// # Arguments
    ///
    /// * `name` - Metric name
    /// * `labels` - Optional labels to filter by
    /// * `duration` - How far back to retrieve (from now)
    ///
    /// # Returns
    ///
    /// Vector of (timestamp, value) pairs ordered by time (oldest first).
    fn get_time_series(
        &self,
        name: &str,
        labels: Option<HashMap<String, String>>,
        duration: Duration,
    ) -> Vec<(std::time::SystemTime, f64)>;
}

#[cfg(test)]
pub mod mock {
    use super::*;

    /// Manual mock for MetricService (cannot use mockall due to Clone bound).
    #[derive(Clone, Default)]
    pub struct MockMetricService;

    impl MockMetricService {
        pub fn new() -> Self {
            Self
        }
    }

    #[async_trait]
    impl MetricService for MockMetricService {
        fn new(_config: Config) -> Result<Self, Error> {
            Ok(Self)
        }

        async fn run(&self) -> Result<(), Error> {
            Ok(())
        }

        fn publish_counter(&self, _name: &str, _value: u64, _unit: UnitType) -> Result<(), Error> {
            Ok(())
        }

        fn publish_gauge(&self, _name: &str, _value: f64, _unit: UnitType) -> Result<(), Error> {
            Ok(())
        }

        fn publish_histogram(
            &self,
            _name: &str,
            _value: f64,
            _unit: UnitType,
        ) -> Result<(), Error> {
            Ok(())
        }

        fn publish_labeled(
            &self,
            _name: &str,
            _value: MetricValue,
            _metric_type: MetricType,
            _unit: UnitType,
            _labels: HashMap<String, String>,
        ) -> Result<(), Error> {
            Ok(())
        }

        fn snapshot(&self) -> MetricSnapshot {
            MetricSnapshot {
                timestamp: std::time::SystemTime::now(),
                metrics: HashMap::new(),
                time_series: None,
            }
        }

        fn get_time_series(
            &self,
            _name: &str,
            _labels: Option<HashMap<String, String>>,
            _duration: Duration,
        ) -> Vec<(std::time::SystemTime, f64)> {
            Vec::new()
        }
    }
}

#[cfg(test)]
pub use mock::MockMetricService;
