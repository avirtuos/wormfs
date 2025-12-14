# Quick Reference: Metrics and Logging Gaps

## File Locations (Absolute Paths)

### SnapshotStore Implementation
- Implementation: `/home/virtuoso/Documents/workspace/wormfs_v2/src/snapshot_store/implementation.rs`
- Types: `/home/virtuoso/Documents/workspace/wormfs_v2/src/snapshot_store/types.rs`
- Module: `/home/virtuoso/Documents/workspace/wormfs_v2/src/snapshot_store/mod.rs`

### MetricsService
- Mod: `/home/virtuoso/Documents/workspace/wormfs_v2/src/metric_service/mod.rs`
- Types: `/home/virtuoso/Documents/workspace/wormfs_v2/src/metric_service/types.rs`
- Implementation: `/home/virtuoso/Documents/workspace/wormfs_v2/src/metric_service/implementation.rs`

### Reference Implementations
- FileStore: `/home/virtuoso/Documents/workspace/wormfs_v2/src/file_store/implementation.rs`
- MetadataStore: `/home/virtuoso/Documents/workspace/wormfs_v2/src/metadata_store/implementation.rs`

### Design Documents
- SnapshotStore Design: `/home/virtuoso/Documents/workspace/wormfs_v2/docs/components/06_SnapshotStore.md`
- MetricsService Design: `/home/virtuoso/Documents/workspace/wormfs_v2/docs/components/10_MetricService.md`

### Full Analysis
- Comprehensive Analysis: `/home/virtuoso/Documents/workspace/wormfs_v2/METRICS_LOGGING_ANALYSIS.md`

---

## Key Findings

### Current State
- Logging: 21 statements (good baseline)
- Metrics: 0 metrics (critical gap - need 30+)
- Compliance: PARTIAL (Rule #7 of CLAUDE.md)

### Major Gaps

**Pruning Operations (CRITICAL)**
- No pruning timing metrics
- No deletion count tracking
- No storage freed tracking
- Minimal pruning logs

**Compression (HIGH)**
- No compression ratio metrics
- No compression timing metrics
- No decompression metrics
- Missing compression algorithm logs

**Verification (HIGH)**
- No verification metrics
- No verification timing logs
- No mismatch tracking

**Streaming (MEDIUM)**
- Limited streaming metrics
- No throughput tracking
- No receive progress logs

---

## 30 Missing Metrics Summary

### Counter Metrics (13)
```
snapshot.ingest.total
snapshot.ingest.failures
snapshot.stream.total
snapshot.receive.total
snapshot.receive.errors
snapshot.verify.total
snapshot.verify.failures
snapshot.prune.total
snapshot.prune.deleted_count
snapshot.prune.freed_bytes
snapshot.retrieve.total
snapshot.retrieve.not_found
snapshot.registry.corrupted_count
```

### Histogram Metrics (10)
```
snapshot.ingest.latency
snapshot.ingest.size
snapshot.stream.latency
snapshot.receive.latency
snapshot.decompress.latency
snapshot.decompress.ratio
snapshot.verify.latency
snapshot.prune.latency
snapshot.retrieve.latency
snapshot.compression_ratio
```

### Gauge Metrics (7)
```
snapshot.registry.count
snapshot.storage.total_bytes
snapshot.storage.oldest_age
snapshot.storage.newest_age
snapshot.ingest.compression_ratio
snapshot.verify.success_rate
snapshot.storage.utilization
```

---

## Code Examples

### Metric Publishing Pattern (from FileStore)

```rust
use crate::metric_service::UnitType;
use std::time::Instant;

let start = Instant::now();
// ... do work ...
let elapsed = start.elapsed().as_secs_f64();

// Publishing counter
let _ = metrics.publish_counter(
    "snapshot.ingest.total",
    1,
    UnitType::Operations,
);

// Publishing histogram (timing)
let _ = metrics.publish_histogram(
    "snapshot.ingest.latency",
    elapsed,
    UnitType::Seconds,
);

// Publishing gauge (current value)
let _ = metrics.publish_gauge(
    "snapshot.storage.total_bytes",
    total_size as f64,
    UnitType::Bytes,
);
```

### Adding Metrics to Component

```rust
// 1. Add to struct
pub struct SnapshotStoreInner {
    // ... existing fields ...
    metrics: Option<Arc<crate::metric_service::MetricServiceImpl>>,
}

// 2. Add constructor parameter
pub fn with_metrics(
    config: Config,
    metrics: Option<Arc<crate::metric_service::MetricServiceImpl>>,
) -> Result<Self, Error> { ... }

// 3. Use in methods
if let Some(ref m) = self.metrics {
    let _ = m.publish_counter("snapshot.ingest.total", 1, UnitType::Operations);
}
```

---

## Logging Gaps by Operation

### Pruning (CRITICAL)
Missing:
- Prune operation start/end timestamps
- Retention policy details being applied
- Candidates identified
- Deletion decisions and reasons
- Per-snapshot timing
- Total statistics (deleted, freed, time)

### Ingestion (HIGH)
Missing:
- Compression algorithm selection
- Original vs compressed size logs
- Checksum calculation timing
- Metadata serialization timing
- Compression ratio

### Verification (HIGH)
Missing:
- Expected vs actual checksums
- Verification timing
- Mismatch details

### Streaming (MEDIUM)
Missing:
- Progress indicators
- Throughput information
- Network error context

### Retrieval (MEDIUM)
Missing:
- Lookup timing
- Not found details

---

## Implementation Roadmap

### Phase 1: Critical Metrics
Focus on: Ingestion, Pruning, Storage stats
Time: ~2 hours
Metrics: 6 (3 counter, 2 histogram, 1 gauge)

### Phase 2: Important Metrics
Focus on: Verification, Retrieval, Error tracking
Time: ~2 hours
Metrics: 8 (4 counter, 3 histogram, 1 gauge)

### Phase 3: Complete Coverage
Focus on: Compression, Streaming, Edge cases
Time: ~2 hours
Metrics: 16 (remaining)

### Phase 4: Logging Enhancements
Focus on: Detailed operation logs at all stages
Time: ~2 hours
Logs: 30+ additional statements

---

## Testing Strategy

### New Metrics Tests
- Track that metrics are published for each operation
- Verify correct metric names and types
- Verify correct units
- Verify timing values are reasonable (>0, <timeout)

### New Logging Tests
- Capture log output for each operation
- Verify expected log statements present
- Check log levels appropriate
- Verify error logs on failures

### Integration Tests
- End-to-end snapshot lifecycle with metrics
- Concurrent operations with metrics
- Stress test metric publishing overhead

---

## Compliance Notes

### CLAUDE.md Rule #7 Requirement
"Add metrics (using MetricsService) and log statements (e.g. info!, error!) at key operational and troubleshooting points in the code."

**Current**: PARTIAL (good logging, no metrics)
**Target**: FULL (both comprehensive logging and metrics)

### Key Operational Points (Rule #7)
1. Snapshot ingestion (timing, size, compression)
2. Snapshot pruning (deleted count, freed space, timing)
3. Snapshot verification (result, timing, mismatches)
4. Snapshot streaming (throughput, timing)
5. Registry operations (count, size tracking)
6. Error conditions (all error paths)

