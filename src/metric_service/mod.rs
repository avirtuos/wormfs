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

pub mod types;

use async_trait::async_trait;
use std::collections::HashMap;
pub use types::{
    AggregatedMetric, Config, Error, MetricSnapshot, MetricType, MetricValue, UnitType,
};

/// MetricService trait defines the interface for metrics collection and aggregation.
///
/// Implementations provide lock-free metric publishing with background aggregation.
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
}
