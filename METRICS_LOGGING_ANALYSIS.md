# SnapshotStore Metrics and Logging Gap Analysis

## Executive Summary

The SnapshotStore implementation has solid basic logging (21 log statements) but lacks comprehensive metrics instrumentation. Compared to similar components like FileStore and MetadataStore, SnapshotStore is missing:

- **0 metrics** (FileStore has ~15 metrics, MetadataStore has ~10 metrics)
- **Missing operational timing metrics** (compression, decompression, verification)
- **Missing compression ratio tracking**
- **Missing pruning operation metrics**
- **Incomplete error tracking metrics**

---

## Current Metrics Infrastructure

### MetricsService Interface

Located in: `/home/virtuoso/Documents/workspace/wormfs_v2/src/metric_service/`

#### Metric Types Supported

```rust
pub enum MetricType {
    Counter,    // Monotonically increasing (e.g., total requests)
    Gauge,      // Value that can increase/decrease (e.g., active connections)
    Histogram,  // Distribution of values (e.g., request latency)
    Summary,    // Histogram with configurable quantiles
    Rate,       // Calculated over time window
}
```

#### Unit Types Supported

```rust
pub enum UnitType {
    // Counts
    Count, Requests, Operations, Events,
    
    // Data sizes
    Bytes, Kilobytes, Megabytes, Gigabytes,
    
    // Rates
    BytesPerSecond, RequestsPerSecond, OperationsPerSecond,
    
    // Time durations
    Nanoseconds, Microseconds, Milliseconds, Seconds,
    
    // Percentages
    Percent,
}
```

#### Publishing API

```rust
// Counter: for monotonically increasing values
fn publish_counter(&self, name: &str, value: u64, unit: UnitType) -> Result<(), Error>

// Gauge: for values that can change up/down
fn publish_gauge(&self, name: &str, value: f64, unit: UnitType) -> Result<(), Error>

// Histogram: for distributions (latency, size, etc.)
fn publish_histogram(&self, name: &str, value: f64, unit: UnitType) -> Result<(), Error>
```

### Metric Naming Convention

Pattern: `component.subsystem.metric_name`

Examples from FileStore:
- `filestore.stripe_write.total` (Counter)
- `filestore.stripe_write.latency` (Histogram)
- `filestore.stripe_cache.hits` (Counter)
- `filestore.stripe_cache.bytes_read_from_cache` (Counter)

Examples from MetadataStore:
- `metadata_store.insert.total` (Counter)
- `metadata_store.insert.latency` (Histogram)
- `metadata_store.get_stripe_at_offset.cache_hit` (Counter)

---

## Current Logging in SnapshotStore

### Existing Log Points (21 total)

#### Initialization Phase
- **Line 114-117**: `info!` - Snapshot storage directory status
- **Line 136-141**: `warn!` - Snapshot directory missing metadata.json
- **Line 148-154**: `error!` - Failed to read file metadata
- **Line 169-178**: `error!` - Failed to parse metadata.json
- **Line 182-193**: `error!` - Failed to read metadata.json file
- **Line 203-207**: `warn!` - Found corrupted/invalid snapshots
- **Line 210-214**: `info!` - Summary of loaded snapshots

#### Snapshot Ingestion
- **Line 328-333**: `info!` - Ingesting snapshot
- **Line 399-401**: `info!` - Successfully ingested snapshot
- **Line 405-406**: `warn!` - Failed to prune snapshots after ingestion

#### Streaming
- **Line 500**: `info!` - Streamed snapshot with total bytes
- **Line 511**: `info!` - Receiving snapshot from remote
- **Line 536-538**: `info!` - Received snapshot with total bytes

#### Pruning
- **Line 592-596**: `debug!` - Skipping pruning (not enough snapshots)
- **Line 632**: `error!` - Failed to delete snapshot
- **Line 637-638**: `info!` - Pruned snapshots summary

#### Deletion
- **Line 656**: `info!` - Deleted snapshot

#### Initialization Summary
- **Line 301-303**: `info!` - Initializing SnapshotStore at path
- **Line 312**: `info!` - SnapshotStore initialized successfully

---

## Snapshot Operations Needing Metrics

### 1. **Snapshot Creation (Ingestion)**

#### Current Code Location
- Lines 316-410 in `implementation.rs`

#### Metrics Needed
| Metric Name | Type | Unit | Purpose |
|---|---|---|---|
| `snapshot.ingest.total` | Counter | Operations | Total snapshots ingested |
| `snapshot.ingest.latency` | Histogram | Milliseconds | Time to ingest snapshot |
| `snapshot.ingest.size` | Histogram | Bytes | Size of ingested snapshots |
| `snapshot.ingest.compression_ratio` | Gauge | Percent | (original / compressed) * 100 |
| `snapshot.ingest.failures` | Counter | Operations | Failed ingestion attempts |

#### Missing Logs
- [ ] Timestamp when ingestion starts
- [ ] Compression algorithm being used
- [ ] Original vs compressed size comparison
- [ ] Checksum calculation time
- [ ] Metadata write time
- [ ] Registry update status
- [ ] Pruning trigger event

### 2. **Snapshot Decompression**

#### Current Code Location
- Lines 251-295 in `implementation.rs`

#### Metrics Needed
| Metric Name | Type | Unit | Purpose |
|---|---|---|---|
| `snapshot.decompress.total` | Counter | Operations | Total decompressions |
| `snapshot.decompress.latency` | Histogram | Milliseconds | Time to decompress |
| `snapshot.decompress.ratio` | Gauge | Percent | Compression ratio (compressed/original) |
| `snapshot.decompress.errors` | Counter | Operations | Failed decompression attempts |

#### Missing Logs
- [ ] Decompression start/end
- [ ] File sizes before/after
- [ ] Compression algorithm details
- [ ] Error details with context

### 3. **Snapshot Verification**

#### Current Code Location
- Lines 566-580 in `implementation.rs`

#### Metrics Needed
| Metric Name | Type | Unit | Purpose |
|---|---|---|---|
| `snapshot.verify.total` | Counter | Operations | Total verifications |
| `snapshot.verify.latency` | Histogram | Milliseconds | Time to verify |
| `snapshot.verify.failures` | Counter | Operations | Checksum mismatches |
| `snapshot.verify.success_rate` | Gauge | Percent | % of successful verifications |

#### Missing Logs
- [ ] Verification start/end
- [ ] Expected vs actual checksum
- [ ] Mismatch details
- [ ] Verification timing

### 4. **Snapshot Pruning**

#### Current Code Location
- Lines 582-642 in `implementation.rs`

#### Metrics Needed
| Metric Name | Type | Unit | Purpose |
|---|---|---|---|
| `snapshot.prune.total` | Counter | Operations | Total prune operations |
| `snapshot.prune.deleted_count` | Counter | Count | Number of snapshots deleted |
| `snapshot.prune.freed_bytes` | Counter | Bytes | Storage freed by pruning |
| `snapshot.prune.latency` | Histogram | Milliseconds | Time to prune all snapshots |
| `snapshot.prune.duration_seconds` | Counter | Seconds | Total time spent in pruning |

#### Missing Logs
- [ ] Prune operation start
- [ ] Policy details (max_snapshots, max_age, min_snapshots)
- [ ] Number of candidates examined
- [ ] Deletion decisions (why each was deleted)
- [ ] Per-snapshot deletion timing
- [ ] Total freed space
- [ ] Prune operation completion time

### 5. **Snapshot Retrieval**

#### Current Code Location
- Lines 412-444 in `implementation.rs`

#### Metrics Needed
| Metric Name | Type | Unit | Purpose |
|---|---|---|---|
| `snapshot.retrieve.total` | Counter | Operations | Total retrieval operations |
| `snapshot.retrieve.latency` | Histogram | Milliseconds | Time to retrieve snapshot |
| `snapshot.retrieve.not_found` | Counter | Operations | Not found errors |
| `snapshot.retrieve.by_index` | Counter | Operations | Retrievals by log index |
| `snapshot.retrieve.latest` | Counter | Operations | Latest snapshot retrievals |

#### Missing Logs
- [ ] Retrieve operation details
- [ ] Registry lookup time
- [ ] Cache hit/miss information
- [ ] Not found details

### 6. **Snapshot Streaming**

#### Current Code Location
- Lines 472-502 and 504-564 in `implementation.rs`

#### Metrics Needed
| Metric Name | Type | Unit | Purpose |
|---|---|---|---|
| `snapshot.stream.total` | Counter | Operations | Total stream operations |
| `snapshot.stream.latency` | Histogram | Milliseconds | Time to stream snapshot |
| `snapshot.stream.bytes_sent` | Counter | Bytes | Total bytes streamed |
| `snapshot.stream.throughput` | Gauge | BytesPerSecond | Average streaming throughput |
| `snapshot.receive.total` | Counter | Operations | Total receive operations |
| `snapshot.receive.latency` | Histogram | Milliseconds | Time to receive |
| `snapshot.receive.bytes_received` | Counter | Bytes | Total bytes received |
| `snapshot.receive.errors` | Counter | Operations | Receive failures |

#### Missing Logs
- [ ] Stream start/end with node identification
- [ ] Throughput during streaming
- [ ] Chunk size and count
- [ ] Timing per chunk
- [ ] Receive progress milestones
- [ ] Network errors

### 7. **Storage Statistics**

#### Current Code Location
- Lines 660-682 in `implementation.rs`

#### Metrics Needed
| Metric Name | Type | Unit | Purpose |
|---|---|---|---|
| `snapshot.registry.count` | Gauge | Count | Current number of snapshots |
| `snapshot.storage.total_bytes` | Gauge | Bytes | Total storage used |
| `snapshot.storage.oldest_age` | Gauge | Seconds | Age of oldest snapshot |
| `snapshot.storage.newest_age` | Gauge | Seconds | Age of newest snapshot |
| `snapshot.registry.corrupted_count` | Gauge | Count | Number of corrupted snapshots |
| `snapshot.storage.utilization` | Gauge | Percent | Storage utilization % |

#### Missing Logs
- [ ] Storage initialization and scanning
- [ ] Registry size at startup
- [ ] Corrupted file details
- [ ] Periodic storage usage updates

---

## Missing Log Statements by Operation

### Initialization Phase
```
Missing Logs:
- [ ] Directory creation success/failure details
- [ ] Scan completion statistics
- [ ] Time taken to scan directory
- [ ] Corrupted snapshot handling details
- [ ] Registry rebuild statistics
```

### Ingestion Phase
```
Missing Logs:
- [ ] Ingest operation start timestamp
- [ ] Source file verification
- [ ] Compression algorithm selection
- [ ] Original file size
- [ ] Compressed file size
- [ ] Compression ratio
- [ ] Checksum calculation timing
- [ ] Metadata JSON serialization timing
- [ ] Registry insertion confirmation
- [ ] Pruning trigger outcome
- [ ] Any compression errors with context
```

### Pruning Phase (CRITICAL GAPS)
```
Missing Logs:
- [ ] Prune operation start (with timestamp)
- [ ] Retention policy details being applied
- [ ] Total snapshots found
- [ ] Snapshots not eligible (too new)
- [ ] Snapshots selected for deletion (with reason)
- [ ] Per-snapshot deletion timing
- [ ] Deletion failures with detailed error
- [ ] Pruning completion with statistics
  - Total candidates
  - Total deleted
  - Total storage freed
  - Total time taken
```

### Decompression Phase
```
Missing Logs:
- [ ] Decompression start (file path, algorithm)
- [ ] Input file size
- [ ] Output file size
- [ ] Decompression timing
- [ ] Decompression errors with context
```

### Verification Phase
```
Missing Logs:
- [ ] Verification start (snapshot_id)
- [ ] Checksum calculation start
- [ ] Expected checksum
- [ ] Calculated checksum
- [ ] Verification result
- [ ] Verification timing
```

---

## Metric Naming Convention Summary

For SnapshotStore, use prefix: `snapshot.`

### Recommended Metric Names

**Counter Metrics** (monotonically increasing):
- `snapshot.ingest.total` - Total ingestion operations
- `snapshot.ingest.failures` - Failed ingestions
- `snapshot.stream.total` - Total stream operations
- `snapshot.receive.total` - Total receive operations
- `snapshot.verify.total` - Total verifications
- `snapshot.prune.total` - Total prune operations
- `snapshot.prune.deleted_count` - Snapshots deleted
- `snapshot.retrieve.total` - Total retrievals
- `snapshot.retrieve.not_found` - Not found errors

**Histogram Metrics** (distributions/latencies):
- `snapshot.ingest.latency` - Ingestion time
- `snapshot.stream.latency` - Streaming time
- `snapshot.receive.latency` - Receive time
- `snapshot.decompress.latency` - Decompression time
- `snapshot.verify.latency` - Verification time
- `snapshot.prune.latency` - Pruning time
- `snapshot.retrieve.latency` - Retrieval time
- `snapshot.ingest.size` - Ingested snapshot sizes
- `snapshot.decompress.ratio` - Compression ratios

**Gauge Metrics** (current state):
- `snapshot.registry.count` - Current snapshot count
- `snapshot.storage.total_bytes` - Total storage used
- `snapshot.ingest.compression_ratio` - Compression ratio
- `snapshot.storage.oldest_age` - Oldest snapshot age
- `snapshot.storage.newest_age` - Newest snapshot age
- `snapshot.registry.corrupted_count` - Corrupted snapshots

---

## Project Guidelines Compliance

### CLAUDE.md Rule #7: Metrics and Logging

> "Add metrics (using MetricsService) and log statements (e.g. info!, error!) at key operational and troubleshooting points in the code."

**Current Compliance**: PARTIAL
- Logging: 21 statements (good baseline)
- Metrics: 0 metrics (critical gap)

**Required Changes**:
1. Add MetricsService to SnapshotStoreImpl
2. Implement all 30+ metrics listed above
3. Add logging at all major operational points
4. Ensure error conditions are tracked

---

## Code Examples from Other Components

### FileStore Metric Pattern

```rust
// Timing-based histogram
let start = Instant::now();
// ... operation ...
let elapsed = start.elapsed().as_secs_f64();
let _ = metrics.publish_histogram(
    "filestore.stripe_write.latency",
    elapsed,
    crate::metric_service::UnitType::Seconds,
);

// Size-based counter
let _ = metrics.publish_counter(
    "filestore.stripe_write.bytes_raw",
    data_size,
    crate::metric_service::UnitType::Bytes,
);

// Cache hit tracking
let _ = metrics.publish_counter(
    "filestore.stripe_cache.hits",
    1,
    crate::metric_service::UnitType::Operations,
);
```

### MetadataStore Metric Pattern

```rust
// Dynamic metric names
let operation_type = "insert"; // or "query", "update", etc.

let _ = metrics.publish_counter(
    &format!("metadata_store.{}.total", operation_type),
    1,
    crate::metric_service::UnitType::Operations,
);

let elapsed = start.elapsed().as_secs_f64();
let _ = metrics.publish_histogram(
    &format!("metadata_store.{}.latency", operation_type),
    elapsed,
    crate::metric_service::UnitType::Seconds,
);
```

---

## Integration Points

### 1. Add MetricsService to SnapshotStoreImpl

```rust
pub struct SnapshotStoreInner {
    config: Config,
    registry: RwLock<HashMap<u64, SnapshotInfo>>,
    node_id: String,
    corrupted_count: std::sync::atomic::AtomicUsize,
    metrics: Option<Arc<crate::metric_service::MetricServiceImpl>>, // ADD THIS
}
```

### 2. Update Constructor

```rust
pub fn with_metrics(
    config: Config,
    metrics: Option<Arc<crate::metric_service::MetricServiceImpl>>,
) -> Result<Self, Error> {
    // ... existing code ...
    metrics,  // Store in inner
}
```

### 3. Use in Methods

```rust
async fn ingest_snapshot(&self, ...) -> Result<SnapshotInfo, Error> {
    let start = Instant::now();
    
    // ... operation ...
    
    // Publish metrics
    if let Some(ref m) = self.metrics {
        let _ = m.publish_histogram(
            "snapshot.ingest.latency",
            start.elapsed().as_secs_f64(),
            UnitType::Seconds,
        );
        let _ = m.publish_counter(
            "snapshot.ingest.total",
            1,
            UnitType::Operations,
        );
    }
    
    Ok(info)
}
```

---

## Summary of Missing Metrics (30 Total)

### Counter Metrics (13)
1. `snapshot.ingest.total`
2. `snapshot.ingest.failures`
3. `snapshot.stream.total`
4. `snapshot.receive.total`
5. `snapshot.receive.errors`
6. `snapshot.verify.total`
7. `snapshot.verify.failures`
8. `snapshot.prune.total`
9. `snapshot.prune.deleted_count`
10. `snapshot.prune.freed_bytes`
11. `snapshot.retrieve.total`
12. `snapshot.retrieve.not_found`
13. `snapshot.registry.corrupted_count`

### Histogram Metrics (10)
1. `snapshot.ingest.latency`
2. `snapshot.ingest.size`
3. `snapshot.stream.latency`
4. `snapshot.receive.latency`
5. `snapshot.decompress.latency`
6. `snapshot.decompress.ratio`
7. `snapshot.verify.latency`
8. `snapshot.prune.latency`
9. `snapshot.retrieve.latency`
10. `snapshot.compression_ratio`

### Gauge Metrics (7)
1. `snapshot.registry.count`
2. `snapshot.storage.total_bytes`
3. `snapshot.storage.oldest_age`
4. `snapshot.storage.newest_age`
5. `snapshot.ingest.compression_ratio`
6. `snapshot.verify.success_rate`
7. `snapshot.storage.utilization`

---

## Missing Log Statements Summary

### Critical Gaps (Pruning Operations)
- [ ] Prune operation start/end with timing
- [ ] Retention policy details
- [ ] Per-snapshot deletion reason and timing
- [ ] Total operations statistics

### Important Gaps (Ingestion)
- [ ] Compression details (algorithm, original, compressed size)
- [ ] Checksum calculation timing
- [ ] Metadata serialization status

### Important Gaps (Streaming)
- [ ] Stream/receive progress indicators
- [ ] Throughput information
- [ ] Network error details

### Operational Gaps (General)
- [ ] Operation timing summaries
- [ ] Registry state changes
- [ ] Storage space calculations

---

## Recommendations

### Phase 1: Critical Metrics (Implement First)
1. `snapshot.ingest.total` (Counter)
2. `snapshot.ingest.latency` (Histogram)
3. `snapshot.prune.total` (Counter)
4. `snapshot.prune.deleted_count` (Counter)
5. `snapshot.registry.count` (Gauge)
6. `snapshot.storage.total_bytes` (Gauge)

### Phase 2: Important Metrics
7. `snapshot.ingest.failures` (Counter)
8. `snapshot.prune.latency` (Histogram)
9. `snapshot.verify.total` (Counter)
10. `snapshot.verify.latency` (Histogram)

### Phase 3: Complete Set
All remaining metrics listed above

### Logging Priority
1. **CRITICAL**: Add pruning operation logging
2. **HIGH**: Add compression details to ingestion
3. **HIGH**: Add verification details
4. **MEDIUM**: Add streaming progress
5. **MEDIUM**: Add operation timing summaries

