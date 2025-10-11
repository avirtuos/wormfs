# MetricService Component Design

## Purpose & Responsibilities

MetricService provides a centralized metrics collection and aggregation system for all WormFS components. Its responsibilities include:

- Accepting metric events from all components via a low-overhead publish API
- Aggregating raw metric events into meaningful statistics (rates, totals, percentiles)
- Supporting multiple metric types (counters, gauges, histograms, summaries)
- Exporting metrics to various backends (Prometheus, OpenTelemetry, logs)
- Maintaining metric metadata (names, labels, units, descriptions)
- Managing metric cardinality to prevent memory exhaustion
- Providing query APIs for internal health checks and debugging
- Supporting metric sampling and aggregation windows

## Architecture: Client Pattern with Interior Mutability

MetricService uses the client pattern with interior mutability to allow all components to publish metrics concurrently without ownership conflicts.

### Why This Pattern?

**Concurrent Publishing**: Every component in WormFS needs to publish metrics from various code paths, often from multiple threads simultaneously. The traditional `Arc<RwLock<MetricService>>` approach would create lock contention on hot paths.

**Solution**: We implement a channel-based pattern where:
1. The outer `MetricService` struct is lightweight and cloneable
2. Components send metrics through an unbounded channel
3. A background aggregation loop processes metrics asynchronously
4. No locks are held during metric publishing (lock-free from caller perspective)
5. Aggregated metrics are stored in an `Arc<RwLock<MetricRegistry>>` for export

### Structure

```rust
struct MetricServiceInner {
    registry: Arc<RwLock<MetricRegistry>>,
    config: MetricConfig,
    exporters: Vec<Box<dyn MetricExporter>>,
}

#[derive(Clone)]
pub struct MetricService {
    inner: Arc<MetricServiceInner>,
    event_tx: mpsc::UnboundedSender<MetricEvent>,
}
```

### Key Benefits

1. **Lock-Free Publishing**: Components publish metrics without blocking on lock acquisition
2. **Concurrent Access**: Multiple components can publish metrics simultaneously
3. **Batched Aggregation**: Background loop can batch process multiple events efficiently
4. **Flexible Export**: Exporters can be added/removed without affecting publishers
5. **Low Overhead**: Publishing is just a channel send (nanoseconds)

## Architecture & Design

### Metric Event Flow

```
┌────────────────────────────────────────────────────────────┐
│                    Component Threads                        │
│  (StorageRaftMember, FileStore, MetadataStore, etc.)       │
└─────┬──────────┬──────────┬──────────┬─────────────────────┘
      │          │          │          │
      │ publish_metric() calls (non-blocking)
      │          │          │          │
      ▼          ▼          ▼          ▼
┌────────────────────────────────────────────────────────────┐
│              MetricEvent Channel (unbounded)                │
│  Events: { value, metric_type, unit, labels, timestamp }  │
└──────────────────────┬─────────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────────┐
│            Background Aggregation Loop                      │
│  • Batch read events from channel                          │
│  • Update counters, gauges, histograms                     │
│  • Compute rates (per-second, per-minute)                  │
│  • Prune expired time-series                               │
│  • Trigger exports on interval                             │
└──────────────────────┬─────────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────────┐
│              MetricRegistry (RwLock)                        │
│  Metrics by name:                                          │
│  • "raft.proposals" -> Counter{value: 1234}               │
│  • "filestore.chunk_writes" -> Histogram{...}             │
│  • "network.bytes_sent" -> Counter{...}                   │
└──────────────────────┬─────────────────────────────────────┘
                       │
                       ├─────────────────┬──────────────────┐
                       ▼                 ▼                  ▼
              ┌──────────────┐  ┌──────────────┐  ┌───────────┐
              │ Prometheus   │  │ OpenTelemetry│  │  Logging  │
              │  Exporter    │  │   Exporter   │  │ Exporter  │
              └──────────────┘  └──────────────┘  └───────────┘
```

### Metric Types

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    /// Monotonically increasing counter (e.g., total requests)
    Counter,
    
    /// Value that can increase or decrease (e.g., active connections)
    Gauge,
    
    /// Distribution of values (e.g., request latency)
    Histogram,
    
    /// Like histogram but with configurable quantiles
    Summary,
    
    /// Rate calculated over time window (e.g., requests per second)
    Rate,
}

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
    
    // Custom
    Custom(&'static str),
}
```

## Interfaces

### Public API

```rust
#[derive(Clone)]
pub struct MetricService {
    inner: Arc<MetricServiceInner>,
    event_tx: mpsc::UnboundedSender<MetricEvent>,
}

impl MetricService {
    /// Create a new MetricService with configuration
    pub fn new(config: MetricConfig) -> Result<Self, MetricError>;
    
    /// Start the background aggregation loop
    pub async fn run(&self) -> Result<(), MetricError>;
    
    /// Publish a counter metric (u64 value)
    pub fn publish_counter(
        &self,
        name: &str,
        value: u64,
        unit: UnitType,
    ) -> Result<(), MetricError>;
    
    /// Publish a gauge metric (f64 value)
    pub fn publish_gauge(
        &self,
        name: &str,
        value: f64,
        unit: UnitType,
    ) -> Result<(), MetricError>;
    
    /// Publish a histogram observation
    pub fn publish_histogram(
        &self,
        name: &str,
        value: f64,
        unit: UnitType,
    ) -> Result<(), MetricError>;
    
    /// Publish a metric with labels
    pub fn publish_labeled(
        &self,
        name: &str,
        value: MetricValue,
        metric_type: MetricType,
        unit: UnitType,
        labels: HashMap<String, String>,
    ) -> Result<(), MetricError>;
    
    /// Generic publish method (used by helper methods above)
    pub fn publish_metric(
        &self,
        event: MetricEvent,
    ) -> Result<(), MetricError>;
    
    /// Register a new exporter
    pub fn add_exporter(&self, exporter: Box<dyn MetricExporter>) -> Result<(), MetricError>;
    
    /// Get current snapshot of all metrics (for debugging/queries)
    pub fn snapshot(&self) -> MetricSnapshot;
    
    /// Reset all metrics (primarily for testing)
    pub fn reset(&self) -> Result<(), MetricError>;
    
    /// Get specific metric by name
    pub fn get_metric(&self, name: &str) -> Option<Metric>;
}

/// Metric event sent through the channel
#[derive(Debug, Clone)]
pub struct MetricEvent {
    pub name: String,
    pub value: MetricValue,
    pub metric_type: MetricType,
    pub unit: UnitType,
    pub labels: HashMap<String, String>,
    pub timestamp: SystemTime,
}

#[derive(Debug, Clone)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(f64),
}

/// Aggregated metric stored in registry
#[derive(Debug, Clone)]
pub struct Metric {
    pub name: String,
    pub metric_type: MetricType,
    pub unit: UnitType,
    pub labels: HashMap<String, String>,
    pub data: MetricData,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

#[derive(Debug, Clone)]
pub enum MetricData {
    Counter {
        value: u64,
        rate_per_second: f64,
    },
    Gauge {
        value: f64,
        min: f64,
        max: f64,
        avg: f64,
    },
    Histogram {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
        buckets: Vec<HistogramBucket>,
        quantiles: Vec<Quantile>,
    },
}

#[derive(Debug, Clone)]
pub struct HistogramBucket {
    pub upper_bound: f64,
    pub count: u64,
}

#[derive(Debug, Clone)]
pub struct Quantile {
    pub quantile: f64, // e.g., 0.5, 0.95, 0.99
    pub value: f64,
}
```

### Configuration

```rust
pub struct MetricConfig {
    /// Maximum number of unique metrics (cardinality limit)
    pub max_metrics: usize,
    
    /// Maximum number of unique label combinations per metric
    pub max_label_combinations: usize,
    
    /// Histogram bucket boundaries (e.g., for latency: [0.001, 0.01, 0.1, 1.0, 10.0])
    pub histogram_buckets: Vec<f64>,
    
    /// Quantiles to compute for summaries (e.g., [0.5, 0.95, 0.99])
    pub summary_quantiles: Vec<f64>,
    
    /// Time window for rate calculations
    pub rate_window: Duration,
    
    /// Export interval (how often to push to exporters)
    pub export_interval: Duration,
    
    /// Metric retention (how long to keep inactive metrics)
    pub metric_retention: Duration,
    
    /// Channel buffer size (0 = unbounded)
    pub channel_buffer_size: usize,
    
    /// Enable sampling for high-cardinality metrics
    pub enable_sampling: bool,
    
    /// Sample rate (1 in N events)
    pub sample_rate: u32,
}

impl Default for MetricConfig {
    fn default() -> Self {
        Self {
            max_metrics: 10_000,
            max_label_combinations: 1_000,
            histogram_buckets: vec![
                0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0,
            ],
            summary_quantiles: vec![0.5, 0.9, 0.95, 0.99],
            rate_window: Duration::from_secs(60),
            export_interval: Duration::from_secs(10),
            metric_retention: Duration::from_secs(600),
            channel_buffer_size: 0, // unbounded
            enable_sampling: false,
            sample_rate: 1,
        }
    }
}
```

### Metric Exporters

```rust
pub trait MetricExporter: Send + Sync {
    /// Export all metrics to the backend
    fn export(&self, snapshot: &MetricSnapshot) -> Result<(), MetricError>;
    
    /// Name of the exporter (for logging)
    fn name(&self) -> &str;
}

/// Prometheus exporter (HTTP /metrics endpoint)
pub struct PrometheusExporter {
    listen_addr: SocketAddr,
}

impl PrometheusExporter {
    pub fn new(listen_addr: SocketAddr) -> Self;
}

/// OpenTelemetry exporter
pub struct OtelExporter {
    endpoint: String,
    headers: HashMap<String, String>,
}

impl OtelExporter {
    pub fn new(endpoint: String, headers: HashMap<String, String>) -> Self;
}

/// Logging exporter (writes metrics to logs)
pub struct LoggingExporter {
    level: tracing::Level,
}

impl LoggingExporter {
    pub fn new(level: tracing::Level) -> Self;
}

/// In-memory exporter (for testing)
pub struct InMemoryExporter {
    snapshots: Arc<Mutex<Vec<MetricSnapshot>>>,
}
```

## Dependencies

### Direct Dependencies
- None (MetricService is a leaf component that other components depend on)

### External Dependencies
- `tokio`: Async runtime for background aggregation loop
- `tracing`: Integration with logging system
- `serde`: Serialization for metric export formats
- `prometheus`: Prometheus client library (optional)
- `opentelemetry`: OpenTelemetry client library (optional)

## Data Structures

```rust
/// Snapshot of all metrics at a point in time
#[derive(Debug, Clone)]
pub struct MetricSnapshot {
    pub timestamp: SystemTime,
    pub metrics: Vec<Metric>,
}

/// Internal registry holding all metrics
struct MetricRegistry {
    metrics: HashMap<MetricKey, Metric>,
    cardinality_tracker: CardinalityTracker,
}

/// Unique identifier for a metric (name + labels)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MetricKey {
    name: String,
    labels: BTreeMap<String, String>, // Sorted for consistent hashing
}

/// Tracks cardinality to prevent unbounded growth
struct CardinalityTracker {
    metric_counts: HashMap<String, usize>,
    max_per_metric: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum MetricError {
    #[error("Channel send failed: {0}")]
    ChannelError(String),
    
    #[error("Metric cardinality limit exceeded for '{0}'")]
    CardinalityLimitExceeded(String),
    
    #[error("Invalid metric name: {0}")]
    InvalidMetricName(String),
    
    #[error("Invalid label key or value: {0}")]
    InvalidLabel(String),
    
    #[error("Exporter error: {0}")]
    ExporterError(String),
    
    #[error("Registry error: {0}")]
    RegistryError(String),
}
```

## Example Usage

```rust
// Initialize MetricService
let config = MetricConfig::default();
let metrics = MetricService::new(config)?;

// Add exporters
metrics.add_exporter(Box::new(PrometheusExporter::new(
    "0.0.0.0:9090".parse()?,
)))?;

// Start background aggregation loop
tokio::spawn(async move {
    metrics.run().await.expect("MetricService failed");
});

// Clone for use in StorageRaftMember
let raft_metrics = metrics.clone();

// Publish metrics from various components
impl StorageRaftMember {
    async fn propose_transaction(&self, tx: Transaction) -> Result<()> {
        let start = Instant::now();
        
        // Increment counter
        self.metrics.publish_counter(
            "raft.proposals.total",
            1,
            UnitType::Operations,
        )?;
        
        // ... execute proposal ...
        
        // Record latency
        let elapsed = start.elapsed().as_secs_f64();
        self.metrics.publish_histogram(
            "raft.proposal.latency",
            elapsed,
            UnitType::Seconds,
        )?;
        
        Ok(())
    }
}

// Publish with labels
impl FileStore {
    async fn write_chunk(&self, chunk: Chunk, disk_id: DiskId) -> Result<()> {
        let mut labels = HashMap::new();
        labels.insert("disk_id".to_string(), disk_id.to_string());
        labels.insert("operation".to_string(), "write".to_string());
        
        self.metrics.publish_labeled(
            "filestore.chunk_operations",
            MetricValue::Counter(1),
            MetricType::Counter,
            UnitType::Operations,
            labels,
        )?;
        
        Ok(())
    }
}
```

## Common Metrics

### StorageRaftMember Metrics
- `raft.proposals.total` (Counter, Operations) - Total Raft proposals
- `raft.proposal.latency` (Histogram, Seconds) - Proposal latency
- `raft.commits.total` (Counter, Operations) - Total commits
- `raft.leader_changes` (Counter, Events) - Leadership changes
- `raft.log_size` (Gauge, Entries) - Current log size

### FileStore Metrics
- `filestore.chunk_writes` (Counter, Operations) - Chunk writes
- `filestore.chunk_reads` (Counter, Operations) - Chunk reads
- `filestore.bytes_written` (Counter, Bytes) - Total bytes written
- `filestore.bytes_read` (Counter, Bytes) - Total bytes read
- `filestore.disk_usage` (Gauge, Percent) - Disk usage per disk

### StorageNetwork Metrics
- `network.bytes_sent` (Counter, Bytes) - Bytes sent
- `network.bytes_received` (Counter, Bytes) - Bytes received
- `network.messages_sent` (Counter, Count) - Messages sent
- `network.peer_connections` (Gauge, Count) - Active peer connections
- `network.connection_errors` (Counter, Errors) - Connection errors

### MetadataStore Metrics
- `metadata.queries.total` (Counter, Operations) - Total queries
- `metadata.query.latency` (Histogram, Milliseconds) - Query latency
- `metadata.db_size` (Gauge, Bytes) - Database size
- `metadata.table_rows` (Gauge, Count) - Rows per table

## Testing Strategy

### Unit Tests
- Metric aggregation logic (counters, gauges, histograms)
- Rate calculation correctness
- Cardinality limiting behavior
- Label normalization and validation
- Histogram bucket assignment

### Integration Tests
- Multi-component metric publishing
- Exporter integration (Prometheus, OpenTelemetry)
- High-cardinality metric handling
- Metric retention and pruning
- Concurrent publishing from many threads

### Performance Tests
- Publish overhead (latency per call)
- Throughput (events per second)
- Memory usage with many metrics
- Aggregation loop performance
- Export performance at scale

## Error Handling

### Channel Errors
- If the event channel is full (bounded mode), drop metrics and increment a `metrics.dropped` counter
- Log warning when dropping metrics

### Cardinality Limits
- Reject new label combinations when limit reached
- Log error with metric name and label set
- Increment `metrics.cardinality_limit_exceeded` counter

### Exporter Failures
- Log error but don't fail the aggregation loop
- Increment per-exporter error counters
- Retry on next export interval

## Open Questions

1. **Metric Retention**: Should inactive metrics be automatically pruned after `metric_retention` duration, or kept forever? Pruning saves memory but loses historical context.

2. **Sampling Strategy**: For high-cardinality metrics, should we use reservoir sampling, consistent hashing, or simple modulo sampling?

3. **Histogram vs Summary**: Should we support both histogram (pre-defined buckets) and summary (client-side quantiles), or just one? Summaries are more accurate but more expensive.

4. **Label Cardinality**: Should we automatically detect high-cardinality labels (e.g., user_id, request_id) and warn/reject them?

5. **Pre-Aggregation**: Should components pre-aggregate locally before publishing (e.g., batch 100 counter increments into one event), or rely on the aggregation loop?

6. **Export Buffering**: Should exporters buffer metrics and flush on interval, or export immediately on each aggregation cycle?

7. **Metric Naming Convention**: Should we enforce a naming convention (e.g., `component.subsystem.metric_name`) and validate it?

8. **Dynamic Configuration**: Should metric configuration (buckets, quantiles, retention) be updateable at runtime without restart?

9. **Distributed Aggregation**: For cluster-wide metrics (e.g., total cluster throughput), should MetricService aggregate across nodes, or should that be handled externally (e.g., Prometheus)?

10. **Metric Metadata**: Should we support metric metadata (description, help text) for self-documenting metrics in exporters?

11. **Alert Rules**: Should MetricService support defining alert rules (e.g., "alert if raft.proposal.latency p99 > 1s"), or defer that to external systems?

12. **Metric Dimensions**: Should we support hierarchical dimensions (e.g., `region.datacenter.node`) or just flat label sets?

13. **Counter Resets**: How should we handle counter resets on process restart? Should we persist counter state, or accept the reset?

14. **Clock Skew**: How should we handle clock skew when aggregating metrics with timestamps from different components?

15. **Export Format**: Should we support multiple export formats (JSON, Protocol Buffers, custom binary) or just standard Prometheus/OTEL formats?
