//! MetricService implementation with channel-based publishing and time-series storage.

use super::types::{
    AggregatedMetric, Config, Error, MetricSnapshot, MetricType, MetricValue, UnitType,
};
use super::MetricService;
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, RwLock};

/// Internal metric event sent through the channel.
#[derive(Debug, Clone)]
struct MetricEvent {
    timestamp: SystemTime,
    name: String,
    value: MetricValue,
    metric_type: MetricType,
    unit: UnitType,
    labels: HashMap<String, String>,
}

/// Unique key for a metric (name + labels).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MetricKey {
    name: String,
    labels: BTreeMap<String, String>,
}

impl MetricKey {
    fn new(name: String, labels: HashMap<String, String>) -> Self {
        Self {
            name,
            labels: labels.into_iter().collect(),
        }
    }

    fn to_string(&self) -> String {
        if self.labels.is_empty() {
            self.name.clone()
        } else {
            let labels_str: Vec<String> = self
                .labels
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            format!("{}[{}]", self.name, labels_str.join(","))
        }
    }
}

/// Registry storing current aggregated metrics.
struct MetricRegistry {
    counters: HashMap<MetricKey, u64>,
    gauges: HashMap<MetricKey, f64>,
    histograms: HashMap<MetricKey, Vec<f64>>,
    metadata: HashMap<MetricKey, (MetricType, UnitType)>,
    cardinality: usize,
    max_cardinality: usize,
}

impl MetricRegistry {
    fn new(max_cardinality: usize) -> Self {
        Self {
            counters: HashMap::new(),
            gauges: HashMap::new(),
            histograms: HashMap::new(),
            metadata: HashMap::new(),
            cardinality: 0,
            max_cardinality,
        }
    }

    fn update(&mut self, event: MetricEvent) -> Result<(), Error> {
        let key = MetricKey::new(event.name.clone(), event.labels.clone());

        // Check cardinality limit for new metrics
        if !self.metadata.contains_key(&key) {
            if self.cardinality >= self.max_cardinality {
                return Err(Error::CardinalityExceeded(self.max_cardinality));
            }
            self.metadata
                .insert(key.clone(), (event.metric_type, event.unit));
            self.cardinality += 1;
        }

        match event.value {
            MetricValue::Counter(value) => {
                *self.counters.entry(key).or_insert(0) += value;
            }
            MetricValue::Gauge(value) => {
                self.gauges.insert(key, value);
            }
            MetricValue::Histogram(value) => {
                self.histograms
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push(value);
            }
        }

        Ok(())
    }

    fn snapshot(&self) -> HashMap<String, AggregatedMetric> {
        let mut result = HashMap::new();

        // Add counters
        for (key, value) in &self.counters {
            if let Some((metric_type, unit)) = self.metadata.get(key) {
                result.insert(
                    key.to_string(),
                    AggregatedMetric {
                        metric_type: *metric_type,
                        unit: *unit,
                        value: *value as f64,
                        labels: key
                            .labels
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    },
                );
            }
        }

        // Add gauges
        for (key, value) in &self.gauges {
            if let Some((metric_type, unit)) = self.metadata.get(key) {
                result.insert(
                    key.to_string(),
                    AggregatedMetric {
                        metric_type: *metric_type,
                        unit: *unit,
                        value: *value,
                        labels: key
                            .labels
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    },
                );
            }
        }

        // Add histograms (compute average)
        for (key, values) in &self.histograms {
            if let Some((metric_type, unit)) = self.metadata.get(key) {
                let avg = if values.is_empty() {
                    0.0
                } else {
                    values.iter().sum::<f64>() / values.len() as f64
                };
                result.insert(
                    key.to_string(),
                    AggregatedMetric {
                        metric_type: *metric_type,
                        unit: *unit,
                        value: avg,
                        labels: key
                            .labels
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    },
                );
            }
        }

        result
    }
}

/// Data point in a time series.
#[derive(Debug, Clone)]
struct DataPoint {
    timestamp: SystemTime,
    value: f64,
}

/// Time-series store with circular buffers.
struct TimeSeriesStore {
    series: HashMap<MetricKey, VecDeque<DataPoint>>,
    max_points_per_metric: usize,
    retention_window: Duration,
    sample_interval: Duration,
}

impl TimeSeriesStore {
    fn new(max_points_per_metric: usize, retention_secs: u64, sample_interval_secs: u64) -> Self {
        Self {
            series: HashMap::new(),
            max_points_per_metric,
            retention_window: Duration::from_secs(retention_secs),
            sample_interval: Duration::from_secs(sample_interval_secs),
        }
    }

    fn append(&mut self, event: &MetricEvent) {
        let key = MetricKey::new(event.name.clone(), event.labels.clone());
        let series = self.series.entry(key).or_insert_with(VecDeque::new);

        // Check if we should sample this point (downsampling)
        if let Some(last) = series.back() {
            if let Ok(elapsed) = event.timestamp.duration_since(last.timestamp) {
                if elapsed < self.sample_interval {
                    // Skip this point (too soon after last sample)
                    return;
                }
            }
        }

        // Convert metric value to f64
        let value = match event.value {
            MetricValue::Counter(v) => v as f64,
            MetricValue::Gauge(v) => v,
            MetricValue::Histogram(v) => v,
        };

        // Add new data point
        series.push_back(DataPoint {
            timestamp: event.timestamp,
            value,
        });

        // Enforce max points limit (circular buffer behavior)
        while series.len() > self.max_points_per_metric {
            series.pop_front();
        }
    }

    fn evict_old_points(&mut self) {
        let cutoff = SystemTime::now()
            .checked_sub(self.retention_window)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        for series in self.series.values_mut() {
            while let Some(point) = series.front() {
                if point.timestamp < cutoff {
                    series.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    fn get_series(
        &self,
        name: &str,
        labels: Option<HashMap<String, String>>,
        duration: Duration,
    ) -> Vec<(SystemTime, f64)> {
        let cutoff = SystemTime::now()
            .checked_sub(duration)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let labels_map = labels.unwrap_or_default();
        let key = MetricKey::new(name.to_string(), labels_map);

        self.series
            .get(&key)
            .map(|series| {
                series
                    .iter()
                    .filter(|point| point.timestamp >= cutoff)
                    .map(|point| (point.timestamp, point.value))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn get_all_series(&self, duration: Duration) -> HashMap<String, Vec<(SystemTime, f64)>> {
        let cutoff = SystemTime::now()
            .checked_sub(duration)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let mut result = HashMap::new();

        for (key, series) in &self.series {
            let data: Vec<(SystemTime, f64)> = series
                .iter()
                .filter(|point| point.timestamp >= cutoff)
                .map(|point| (point.timestamp, point.value))
                .collect();

            if !data.is_empty() {
                result.insert(key.to_string(), data);
            }
        }

        result
    }
}

/// MetricService implementation with channel-based publishing.
///
/// This implementation uses an unbounded channel for lock-free metric publishing.
/// The channel and background aggregation loop are internal implementation details
/// not exposed through the trait interface.
#[derive(Clone)]
pub struct MetricServiceImpl {
    /// Channel sender for publishing metrics (private - not exposed in trait)
    sender: mpsc::UnboundedSender<MetricEvent>,

    /// Aggregated metrics registry (shared across clones)
    registry: Arc<RwLock<MetricRegistry>>,

    /// Time-series data store (shared across clones)
    time_series: Arc<RwLock<TimeSeriesStore>>,

    /// Configuration
    config: Config,
}

impl MetricServiceImpl {
    /// Create a new MetricService instance and start the background aggregation loop.
    ///
    /// This initializes the channel, shared state, and spawns the background task.
    fn new_internal(config: Config) -> Result<Self, Error> {
        if !config.enabled {
            return Err(Error::ConfigError("Metrics are disabled".into()));
        }

        let (sender, receiver) = mpsc::unbounded_channel();
        let registry = Arc::new(RwLock::new(MetricRegistry::new(config.max_cardinality)));
        let time_series = Arc::new(RwLock::new(TimeSeriesStore::new(
            config.max_points_per_metric,
            config.time_series_retention_secs,
            config.time_series_sample_interval_secs,
        )));

        // Spawn background aggregation loop
        let registry_clone = Arc::clone(&registry);
        let time_series_clone = Arc::clone(&time_series);
        let enable_time_series = config.enable_time_series;

        tokio::spawn(async move {
            Self::aggregation_loop(
                receiver,
                registry_clone,
                time_series_clone,
                enable_time_series,
            )
            .await;
        });

        Ok(Self {
            sender,
            registry,
            time_series,
            config,
        })
    }

    /// Background aggregation loop.
    ///
    /// Processes metric events from the channel and updates the registry and time-series store.
    async fn aggregation_loop(
        mut receiver: mpsc::UnboundedReceiver<MetricEvent>,
        registry: Arc<RwLock<MetricRegistry>>,
        time_series: Arc<RwLock<TimeSeriesStore>>,
        enable_time_series: bool,
    ) {
        tracing::info!("MetricService aggregation loop started");

        // Periodic eviction timer
        let mut eviction_interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            tokio::select! {
                Some(event) = receiver.recv() => {
                    // Update registry
                    {
                        let mut registry = registry.write().await;
                        if let Err(e) = registry.update(event.clone()) {
                            tracing::warn!("Failed to update metric registry: {}", e);
                        }
                    }

                    // Update time-series if enabled
                    if enable_time_series {
                        let mut ts = time_series.write().await;
                        ts.append(&event);
                    }
                }
                _ = eviction_interval.tick() => {
                    // Evict old time-series data
                    if enable_time_series {
                        let mut ts = time_series.write().await;
                        ts.evict_old_points();
                    }
                }
                else => {
                    // Channel closed
                    tracing::info!("MetricService aggregation loop stopped (channel closed)");
                    break;
                }
            }
        }
    }
}

#[async_trait]
impl MetricService for MetricServiceImpl {
    fn new(config: Config) -> Result<Self, Error> {
        Self::new_internal(config)
    }

    async fn run(&self) -> Result<(), Error> {
        // Background loop already started in new(), so this is a no-op
        // This method exists to satisfy the trait interface
        Ok(())
    }

    fn publish_counter(&self, name: &str, value: u64, unit: UnitType) -> Result<(), Error> {
        let event = MetricEvent {
            timestamp: SystemTime::now(),
            name: name.to_string(),
            value: MetricValue::Counter(value),
            metric_type: MetricType::Counter,
            unit,
            labels: HashMap::new(),
        };

        self.sender
            .send(event)
            .map_err(|_| Error::SendFailed("Channel send failed".into()))
    }

    fn publish_gauge(&self, name: &str, value: f64, unit: UnitType) -> Result<(), Error> {
        let event = MetricEvent {
            timestamp: SystemTime::now(),
            name: name.to_string(),
            value: MetricValue::Gauge(value),
            metric_type: MetricType::Gauge,
            unit,
            labels: HashMap::new(),
        };

        self.sender
            .send(event)
            .map_err(|_| Error::SendFailed("Channel send failed".into()))
    }

    fn publish_histogram(&self, name: &str, value: f64, unit: UnitType) -> Result<(), Error> {
        let event = MetricEvent {
            timestamp: SystemTime::now(),
            name: name.to_string(),
            value: MetricValue::Histogram(value),
            metric_type: MetricType::Histogram,
            unit,
            labels: HashMap::new(),
        };

        self.sender
            .send(event)
            .map_err(|_| Error::SendFailed("Channel send failed".into()))
    }

    fn publish_labeled(
        &self,
        name: &str,
        value: MetricValue,
        metric_type: MetricType,
        unit: UnitType,
        labels: HashMap<String, String>,
    ) -> Result<(), Error> {
        let event = MetricEvent {
            timestamp: SystemTime::now(),
            name: name.to_string(),
            value,
            metric_type,
            unit,
            labels,
        };

        self.sender
            .send(event)
            .map_err(|_| Error::SendFailed("Channel send failed".into()))
    }

    fn snapshot(&self) -> MetricSnapshot {
        // Use try_read to avoid blocking
        // If lock is held, return empty snapshot
        let metrics = self
            .registry
            .try_read()
            .map(|registry| registry.snapshot())
            .unwrap_or_default();

        let time_series = if self.config.enable_time_series {
            self.time_series.try_read().ok().map(|ts| {
                ts.get_all_series(Duration::from_secs(self.config.time_series_retention_secs))
            })
        } else {
            None
        };

        MetricSnapshot {
            timestamp: SystemTime::now(),
            metrics,
            time_series,
        }
    }

    fn get_time_series(
        &self,
        name: &str,
        labels: Option<HashMap<String, String>>,
        duration: Duration,
    ) -> Vec<(SystemTime, f64)> {
        if !self.config.enable_time_series {
            return Vec::new();
        }

        self.time_series
            .try_read()
            .ok()
            .map(|ts| ts.get_series(name, labels, duration))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_key_ordering() {
        let mut labels1 = HashMap::new();
        labels1.insert("b".to_string(), "2".to_string());
        labels1.insert("a".to_string(), "1".to_string());

        let mut labels2 = HashMap::new();
        labels2.insert("a".to_string(), "1".to_string());
        labels2.insert("b".to_string(), "2".to_string());

        let key1 = MetricKey::new("test".to_string(), labels1);
        let key2 = MetricKey::new("test".to_string(), labels2);

        // Keys should be equal despite different insertion order
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_metric_registry_counters() {
        let mut registry = MetricRegistry::new(1000);

        let event1 = MetricEvent {
            timestamp: SystemTime::now(),
            name: "test.counter".to_string(),
            value: MetricValue::Counter(5),
            metric_type: MetricType::Counter,
            unit: UnitType::Count,
            labels: HashMap::new(),
        };

        let event2 = MetricEvent {
            timestamp: SystemTime::now(),
            name: "test.counter".to_string(),
            value: MetricValue::Counter(3),
            metric_type: MetricType::Counter,
            unit: UnitType::Count,
            labels: HashMap::new(),
        };

        registry.update(event1).unwrap();
        registry.update(event2).unwrap();

        let snapshot = registry.snapshot();
        let metric = snapshot.get("test.counter").unwrap();
        assert_eq!(metric.value, 8.0); // 5 + 3
    }

    #[test]
    fn test_metric_registry_cardinality_limit() {
        let mut registry = MetricRegistry::new(2);

        for i in 0..5 {
            let event = MetricEvent {
                timestamp: SystemTime::now(),
                name: format!("test.metric.{}", i),
                value: MetricValue::Counter(1),
                metric_type: MetricType::Counter,
                unit: UnitType::Count,
                labels: HashMap::new(),
            };

            let result = registry.update(event);
            if i < 2 {
                assert!(result.is_ok());
            } else {
                assert!(result.is_err());
            }
        }
    }

    #[test]
    fn test_time_series_store_downsampling() {
        let mut store = TimeSeriesStore::new(1000, 3600, 5); // 5 second sample interval

        let base_time = SystemTime::now();

        // Add 10 points rapidly (should be downsampled)
        for i in 0..10 {
            let event = MetricEvent {
                timestamp: base_time + Duration::from_millis(i * 100), // 100ms apart
                name: "test.metric".to_string(),
                value: MetricValue::Counter(i),
                metric_type: MetricType::Counter,
                unit: UnitType::Count,
                labels: HashMap::new(),
            };

            store.append(&event);
        }

        // Should have only 1 point (all others filtered by downsampling)
        let series = store.get_series("test.metric", None, Duration::from_secs(3600));
        assert_eq!(series.len(), 1);
    }
}
