# BufferedFileHandle Design

**Status**: Draft
**Issue**: #101
**Related**: #97 (Data corruption with StripeCache)
**Author**: Claude Code
**Date**: 2025-10-19

## Table of Contents

1. [Overview](#overview)
2. [Problem Statement](#problem-statement)
3. [Architecture](#architecture)
4. [Component Design](#component-design)
5. [Data Structures](#data-structures)
6. [Flush Strategies](#flush-strategies)
7. [Read Path](#read-path)
8. [Write Path](#write-path)
9. [Memory Management](#memory-management)
10. [Concurrency Model](#concurrency-model)
11. [Open Questions](#open-questions)
12. [Implementation Phases](#implementation-phases)
13. [Migration Plan](#migration-plan)
14. [Testing Strategy](#testing-strategy)

---

## Overview

BufferedFileHandle replaces the problematic StripeCache with a simpler, per-file-handle write buffering mechanism. Instead of managing a global cache of stripes across all files, each file handle maintains its own isolated buffer of uncommitted metadata and data changes.

### Key Principles

1. **One buffer per file handle** - Eliminates cross-file race conditions
2. **Metadata and data together** - Prevents consistency issues
3. **Simple flush semantics** - Only full flushes make data visible
4. **Memory-pressure driven** - Flush when buffers get too large
5. **Read-through caching** - Reads see buffered writes immediately

### Goals

- **Primary**: Solve IO amplification for incremental writes (uploads, streaming)
- **Secondary**: Eliminate data corruption from StripeCache races
- **Non-goal**: Read caching/optimization (future work)

---

## Problem Statement

### Current Issues with StripeCache

1. **Wrong Abstraction**: Global cache across all files creates write_group confusion
2. **Race Conditions**: Partial flushes cause data loss
   - Dirty timeout can flush mid-write
   - Concurrent writes to same write_group get lost
   - Metadata and data updates not atomic
3. **Read-Your-Writes**: Writers don't always see their own unbuffered data
4. **Complex Eviction**: LRU eviction only works for clean entries
5. **Memory Accounting**: Can exceed limits when all entries are dirty

### Evidence from Recent Debugging

- 30MB file: ✅ Works (fits in 256MB cache)
- 300MB file: ❌ Data corruption (exceeds cache, triggers eviction/flush)
- Race condition: `flush_write_group` clones keys with read lock, concurrent writes add more stripes, those stripes get lost when write_group removed

---

## Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────┐
│ FileSystemService                                            │
│                                                              │
│  ┌──────────────┐         ┌─────────────────────────────┐  │
│  │ OpenFile     │────────▶│  BufferedFileHandle         │  │
│  │              │         │                             │  │
│  │ - inode      │         │  ┌──────────────────────┐  │  │
│  │ - fh         │         │  │  In-Memory Metadata  │  │  │
│  │ - buffer: Arc│         │  │  - FileAttr          │  │  │
│  └──────────────┘         │  │  - Stripes Map       │  │  │
│                           │  │  - Tombstones Set    │  │  │
│                           │  └──────────────────────┘  │  │
│                           │                             │  │
│                           │  ┌──────────────────────┐  │  │
│                           │  │  Buffered Data       │  │  │
│                           │  │  StripeBuilder Map   │  │  │
│                           │  └──────────────────────┘  │  │
│                           └─────────────────────────────┘  │
│                                      │                      │
│                                      │ flush()              │
│                                      ▼                      │
│                           ┌─────────────────────┐          │
│                           │ StorageRaftMember   │          │
│                           └─────────────────────┘          │
│                                      │                      │
│                           ┌──────────┴─────────┐          │
│                           ▼                     ▼           │
│                    ┌──────────────┐    ┌──────────────┐   │
│                    │ MetadataStore│    │  FileStore   │   │
│                    └──────────────┘    └──────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Component Relationships

- `OpenFile` owns an `Arc<BufferedFileHandle>` (shared between dup'd file handles)
- `BufferedFileHandle` owns `StripeBuilder` instances
- `StripeBuilder` (new utility) pre-allocates `StripeId` and buffers data
- Flushes go through Raft → MetadataStore + FileStore (atomic)

---

## Component Design

### StripeBuilder (New Utility in file_store)

```rust
/// Utility for building stripes incrementally before committing to storage.
///
/// Key features:
/// - Pre-allocates StripeId on construction
/// - Buffers stripe data without computing parity
/// - Stores StoragePolicy for later use
/// - Can be "built" into actual Stripe via FileStore
pub struct StripeBuilder {
    /// Pre-allocated stripe ID that will be used in FileStore
    stripe_id: StripeId,

    /// File this stripe belongs to
    file_id: FileId,

    /// Stripe index within file
    stripe_index: u32,

    /// Byte offset in file where this stripe starts
    stripe_offset: u64,

    /// Buffered data (not yet erasure-coded)
    data: Vec<u8>,

    /// Storage policy for this stripe
    policy: Arc<StoragePolicy>,

    /// When this builder was created
    created_at: Instant,
}

impl StripeBuilder {
    /// Create a new stripe builder with pre-allocated ID.
    pub fn new(
        file_id: FileId,
        stripe_index: u32,
        stripe_offset: u64,
        max_size: usize,
        policy: Arc<StoragePolicy>,
    ) -> Self {
        Self {
            stripe_id: StripeId::new(),  // Pre-allocated!
            file_id,
            stripe_index,
            stripe_offset,
            data: Vec::with_capacity(max_size),
            policy,
            created_at: Instant::now(),
        }
    }

    /// Append data to this stripe (up to max_chunk_size).
    pub fn append(&mut self, data: &[u8]) -> Result<usize, Error> {
        let available = self.remaining_capacity();
        let to_write = data.len().min(available);
        self.data.extend_from_slice(&data[..to_write]);
        Ok(to_write)
    }

    /// Get the pre-allocated stripe ID.
    pub fn stripe_id(&self) -> StripeId {
        self.stripe_id
    }

    /// Get current data size.
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Get memory footprint.
    pub fn memory_bytes(&self) -> usize {
        self.data.capacity()
    }
}
```

### BufferedFileHandle

```rust
/// Per-file-handle write buffer that coalesces metadata and data changes.
///
/// Maintains an in-memory snapshot of file state plus uncommitted changes.
/// All mutations stay local until flush().
///
/// CONCURRENCY: Wrapped in Mutex for simplicity. POSIX doesn't define behavior
/// for concurrent writes to same file descriptor, so serialization is acceptable.
pub struct BufferedFileHandle {
    /// All state protected by single mutex (simpler than RwLock hierarchy)
    inner: Arc<Mutex<BufferedFileHandleInner>>,

    /// Dependencies (outside mutex to avoid lock during async calls)
    metadata_store: Arc<MetadataStoreImpl>,
    file_store: Arc<dyn FileStore + Send + Sync>,
    raft_stub: Arc<StorageRaftMemberStub>,
    metrics: Option<Arc<MetricServiceImpl>>,
}

struct BufferedFileHandleInner {
    /// File this handle is for
    file_id: FileId,
    inode: u64,

    /// Configuration
    config: BufferedFileHandleConfig,

    /// In-memory file attributes (may be stale + buffered changes)
    attributes: FileAttr,

    /// Map of stripe_index -> StripeMetadata
    /// Includes both committed (from MetadataStore) and buffered stripes
    /// IndexMap preserves insertion order for flush
    stripes: IndexMap<u32, StripeMetadata>,

    /// Stripes marked for deletion (tombstones)
    /// These were in MetadataStore but should be deleted on flush
    tombstones: HashSet<StripeId>,

    /// Active stripe builders (unbuffered data)
    /// Key: stripe_index
    builders: HashMap<u32, StripeBuilder>,

    /// Dirty tracking
    dirty_metadata: bool,
    dirty_data: bool,

    /// Memory accounting
    buffered_bytes: usize,

    /// Last full flush time
    last_flush: Option<Instant>,

    /// Number of writes since last full flush
    writes_since_flush: usize,
}

pub struct BufferedFileHandleConfig {
    /// Maximum memory per handle before triggering partial flush
    max_memory_bytes: usize,

    /// Maximum time between full flushes
    max_flush_interval: Duration,

    /// Maximum writes before forcing full flush
    max_writes_before_flush: usize,

    /// Stripe size (from FileStore config)
    max_stripe_size: usize,
}
```

---

## Data Structures

### Stripe Metadata Representation

```rust
/// Represents a stripe in BufferedFileHandle.
#[derive(Clone)]
pub struct StripeMetadata {
    stripe_id: StripeId,
    stripe_index: u32,
    offset: u64,
    size: u64,
    checksum: u32,
    created_at: SystemTime,

    /// Tracks whether this stripe exists in MetadataStore
    committed: bool,

    /// Tracks whether this is a dirty update to committed stripe
    dirty: bool,
}
```

### Memory Accounting

```rust
impl BufferedFileHandle {
    /// Calculate current memory usage.
    fn calculate_memory_usage(&self) -> usize {
        let builders = self.builders.read().unwrap();
        builders.values()
            .map(|b| b.memory_bytes())
            .sum()
    }

    /// Check if we need to flush due to memory pressure.
    fn needs_memory_flush(&self) -> bool {
        self.buffered_bytes.load(Ordering::Relaxed) > self.config.max_memory_bytes
    }

    /// Check if we need to flush due to time/write count.
    fn needs_full_flush(&self) -> bool {
        let writes = self.writes_since_flush.load(Ordering::Relaxed);
        if writes >= self.config.max_writes_before_flush {
            return true;
        }

        if let Some(last) = *self.last_flush.read().unwrap() {
            last.elapsed() > self.config.max_flush_interval
        } else {
            false
        }
    }
}
```

---

## Flush Strategies

### Two Types of Flushes

#### 1. Partial Flush (Memory Pressure)

**Trigger**: `buffered_bytes > max_memory_bytes`

**Action**:
- Write `StripeBuilder` data to FileStore only
- Do NOT update MetadataStore
- Keep `StripeMetadata` in buffer
- Mark builder as "flushed to disk"
- Free builder memory

**Result**: Data is safe on disk but not visible via metadata

```rust
async fn partial_flush(&self) -> Result<(), Error> {
    // 1. Find builders consuming most memory
    let builders_to_flush = self.select_builders_for_flush();

    // 2. Write to FileStore (chunks only, no metadata update)
    for builder in builders_to_flush {
        let chunks = self.file_store.write_stripe_data_only(
            builder.file_id,
            builder.stripe_id,
            builder.stripe_offset,
            &builder.data,
            &builder.policy,
        ).await?;

        // 3. Record that data is on disk
        // metadata stays buffered
        builder.mark_flushed_to_disk(chunks);

        // 4. Free data buffer
        builder.clear_data();
    }

    Ok(())
}
```

#### 2. Full Flush (Periodic or on Close)

**Trigger**:
- `writes_since_flush >= max_writes_before_flush`
- `time_since_flush > max_flush_interval`
- File close/release
- Explicit `fsync()`

**Action**:
- Flush all remaining `StripeBuilder` data to FileStore
- Build consolidated metadata update
- Submit atomic Raft command with all changes:
  - New stripes
  - Updated stripes
  - Deleted stripes (tombstones)
  - File attribute updates (size, mtime)

```rust
async fn full_flush(&self) -> Result<(), Error> {
    let mut inner = self.inner.lock().await;

    // 1. Flush all remaining builders to FileStore
    for (stripe_idx, builder) in inner.builders.drain() {
        if !builder.is_flushed_to_disk() {
            // Write data if not already written
            let metadata = self.file_store.write_stripe(
                builder.file_id,
                builder.stripe_id,
                builder.stripe_offset,
                &builder.data,
                &builder.policy,
            ).await?;

            // Update buffered metadata with actual metadata
            inner.stripes.insert(stripe_idx, metadata);
        }
    }

    // 2. Build batch of MetadataOperations using EXISTING Raft batching
    let mut operations = Vec::new();

    // Add all new stripes
    for (_, stripe) in inner.stripes.iter().filter(|(_, s)| !s.committed) {
        operations.push(MetadataOperation::CreateStripe {
            file_id: inner.file_id,
            stripe: stripe.clone(),
        });
    }

    // Add all updated stripes
    for (_, stripe) in inner.stripes.iter().filter(|(_, s)| s.committed && s.dirty) {
        operations.push(MetadataOperation::UpdateStripe {
            file_id: inner.file_id,
            stripe: stripe.clone(),
        });
    }

    // Add all tombstoned stripes
    for stripe_id in &inner.tombstones {
        operations.push(MetadataOperation::DeleteStripe {
            stripe_id: *stripe_id,
        });
    }

    // Update file attributes
    operations.push(MetadataOperation::UpdateFileAttributes {
        file_id: inner.file_id,
        inode: inner.inode,
        attributes: inner.attributes.clone(),
    });

    // 3. Submit atomically via EXISTING Raft batching (single log entry!)
    self.raft_stub.propose_operations(operations).await?;

    // 4. Mark all as committed and clear dirty flags
    for stripe in inner.stripes.values_mut() {
        stripe.committed = true;
        stripe.dirty = false;
    }
    inner.tombstones.clear();
    inner.dirty_metadata = false;
    inner.dirty_data = false;
    inner.writes_since_flush = 0;
    inner.last_flush = Some(Instant::now());

    Ok(())
}
```

---

## Read Path

### Read Flow

```
read(inode, offset, size)
  │
  ├──▶ 1. Check BufferedFileHandle for affected stripes
  │        - Stripe in builders? → Return buffered data
  │        - Stripe tombstoned? → Return zeros/EOF
  │        - Stripe in metadata? → Note for read-through
  │
  └──▶ 2. Read-through to MetadataStore/FileStore for non-buffered
           - Skip tombstoned stripes
           - Merge buffered + committed data
```

### Implementation

```rust
async fn read(
    &self,
    offset: u64,
    size: u32,
) -> Result<Vec<u8>, Error> {
    let mut result = Vec::with_capacity(size as usize);
    let end_offset = offset + size as u64;

    // Calculate stripe range
    let start_stripe = (offset / self.config.max_stripe_size as u64) as u32;
    let end_stripe = ((end_offset - 1) / self.config.max_stripe_size as u64) as u32;

    for stripe_idx in start_stripe..=end_stripe {
        // Check if buffered
        let builders = self.builders.read().unwrap();
        if let Some(builder) = builders.get(&stripe_idx) {
            // Data in buffer - use it
            let stripe_data = self.read_from_builder(builder, offset, size)?;
            result.extend_from_slice(&stripe_data);
            continue;
        }
        drop(builders);

        // Check if tombstoned
        if self.tombstones.read().unwrap().contains(&stripe_id) {
            // Stripe deleted - return zeros
            let to_read = /* calculate */;
            result.extend(vec![0u8; to_read]);
            continue;
        }

        // Read through to storage
        let stripe_data = self.read_through_storage(stripe_idx, offset, size).await?;
        result.extend_from_slice(&stripe_data);
    }

    Ok(result)
}
```

---

## Write Path

### Write Flow

```
write(inode, offset, data)
  │
  ├──▶ 1. Find/create StripeBuilder for affected stripes
  │
  ├──▶ 2. Append data to builders
  │        - May span multiple stripes
  │        - Update in-memory FileAttr (size, mtime)
  │        - Mark dirty
  │
  ├──▶ 3. Check flush triggers
  │        - Memory pressure? → partial_flush()
  │        - Too many writes? → full_flush()
  │
  └──▶ 4. Return success (data buffered)
```

### Implementation

```rust
async fn write(
    &self,
    offset: u64,
    data: &[u8],
) -> Result<u32, Error> {
    let mut bytes_written = 0;
    let mut remaining = data;
    let mut current_offset = offset;

    while !remaining.is_empty() {
        // Find stripe for this offset
        let stripe_idx = (current_offset / self.config.max_stripe_size as u64) as u32;
        let stripe_offset = current_offset % self.config.max_stripe_size as u64;

        // Get or create builder
        let mut builders = self.builders.write().unwrap();
        let builder = builders.entry(stripe_idx).or_insert_with(|| {
            let stripe_file_offset = stripe_idx as u64 * self.config.max_stripe_size as u64;
            StripeBuilder::new(
                self.file_id,
                stripe_idx,
                stripe_file_offset,
                self.config.max_stripe_size,
                self.get_storage_policy(),
            )
        });

        // Append to builder
        let written = builder.append(remaining)?;
        bytes_written += written as u32;
        remaining = &remaining[written..];
        current_offset += written as u64;

        // Update memory accounting
        self.buffered_bytes.fetch_add(written, Ordering::Relaxed);

        drop(builders);

        // Check if need partial flush
        if self.needs_memory_flush() {
            self.partial_flush().await?;
        }
    }

    // Update file attributes
    {
        let mut attrs = self.attributes.write().unwrap();
        let new_size = (offset + bytes_written as u64).max(attrs.size);
        attrs.size = new_size;
        attrs.mtime = SystemTime::now();
        attrs.atime = SystemTime::now();
    }

    self.dirty_data.store(true, Ordering::Release);
    self.dirty_metadata.store(true, Ordering::Release);
    self.writes_since_flush.fetch_add(1, Ordering::Relaxed);

    // Check if need full flush
    if self.needs_full_flush() {
        self.full_flush().await?;
    }

    Ok(bytes_written)
}
```

---

## Memory Management

### Per-Handle vs Global Limits

**Decision**: Start with **per-handle limits**, add global accounting later if needed.

**Rationale**:
- Simpler implementation
- Natural back-pressure per file
- Avoids cross-file coordination
- Can add global limit as wrapper

### Memory Breakdown

```rust
struct MemoryUsage {
    // Stripe data in builders
    builder_data: usize,

    // In-memory metadata structures
    metadata_overhead: usize,

    // Total
    total: usize,
}

impl BufferedFileHandle {
    fn get_memory_usage(&self) -> MemoryUsage {
        let builder_data: usize = self.builders.read().unwrap()
            .values()
            .map(|b| b.memory_bytes())
            .sum();

        let metadata_overhead =
            self.stripes.read().unwrap().len() * size_of::<StripeMetadata>() +
            self.tombstones.read().unwrap().len() * size_of::<StripeId>();

        MemoryUsage {
            builder_data,
            metadata_overhead,
            total: builder_data + metadata_overhead,
        }
    }
}
```

### Partial Flush Selection

**Strategy**: Flush oldest builders first (FIFO)

```rust
fn select_builders_for_flush(&self) -> Vec<StripeBuilder> {
    let target_free = self.config.max_memory_bytes * 30 / 100; // Free 30%
    let mut to_flush = Vec::new();
    let mut freed = 0;

    let builders = self.builders.read().unwrap();

    // Sort by creation time (oldest first)
    let mut sorted: Vec<_> = builders.iter().collect();
    sorted.sort_by_key(|(_, b)| b.created_at);

    for (idx, builder) in sorted {
        let size = builder.memory_bytes();
        to_flush.push((*idx, builder.clone()));
        freed += size;

        if freed >= target_free {
            break;
        }
    }

    to_flush
}
```

---

## Concurrency Model

### Thread Safety

- `Arc<BufferedFileHandle>` shared between dup'd file descriptors
- All internal state protected by `RwLock` or `AtomicUsize`
- Reads can proceed concurrently
- Writes serialize at builder level

### Lock Ordering

To prevent deadlocks, always acquire locks in this order:

1. `attributes` (RwLock)
2. `stripes` (RwLock)
3. `builders` (RwLock)
4. `tombstones` (RwLock)

Never hold multiple write locks simultaneously.

### Flush Coordination

**Question**: What happens if `write()` is called during `full_flush()`?

**Option A**: Block writes during flush
- Hold `builders` write lock for entire flush
- Simple but high latency

**Option B**: Allow concurrent writes, flush snapshot
- Clone builders at flush start
- New writes go to new builders
- More complex but lower latency

**Recommendation**: Start with **Option A** for correctness, optimize to Option B later.

---

## Open Questions

### 1. Concurrent Access Patterns

**Q**: Can multiple threads call `write()` on the same `BufferedFileHandle` simultaneously?

**A**: Concurrent writes using the same file description (handle) are possible but their behavior is undefined so as long as we do not crash we are not violating any posix rules. Perhaps the best thing we can do is wrap BufferedFileHandle with a mutex.

**Follow-up**: Do we need finer-grained locking (per-stripe mutexes)? No.

### 2. Raft Command Batching

**Q**: The `BatchFileUpdate` command doesn't exist yet. Should we:
- Add new Raft command type?
- Issue multiple existing commands in sequence?
- Use some form of transaction?

**A**: We do not need a BatchFileUpdate command as StorageRaftMember already supports submitting mutliple MetadataOperations in a single proposal and that is essentially batching so we should simply use that facility to get atomicity.

### 3. Partial Flush Visibility

**Q**: After partial flush, stripe data is on disk but not visible. What if process crashes before full flush?

**Decision**: This is acceptable. Data is durable but not visible, matching POSIX semantics.

### 4. Tombstone Lifecycle

**Q**: When can we remove entries from the `tombstones` set?

**A**: Only after successful full flush that deletes them from MetadataStore. This is safe because when a Stripe is deleted and recreated like when a file shrinks and then re-expands, the new Stripe gets a new StripeId so the old Stripe that was previously deleted when the file shunk is truely dead forever because when the file expands again that will get a new Stripe Id even though it may cover a previously a previously present offset and range of bytes.

### 5. Read-Your-Writes Guarantee

**Q**: Must reads see buffered writes from same handle?

**A**: Yes, this is critical for correctness. Reads must check `builders` first.

**Q**: What about reads from different handles to same file?

**A**: Different handles have different `BufferedFileHandle` instances, so buffered writes are isolated until flush. This matches POSIX semantics (each FD has its own file offset).

### 6. Truncate Operation

**Q**: How does `truncate()` interact with buffered data?

**Cases**:
1. Truncate to 0 → Clear all builders, tombstone all stripes
2. Truncate to middle of file → Partial stripe adjustment
3. Truncate beyond EOF → Extend with zeros

**Recommendation**: Force full flush before truncate for simplicity. There may be many cases where forcing a flush is simpler so we should adopt the inform(OperationType) pattern we originally tried in StripeCache so that FileSystemService will give BufferedFileReader a heads up by informing it of an impending operations so that it can choose to flush in order to enable simpler implementation choices.

### 7. StripeBuilder Interface

**Q**: Should `StripeBuilder` be in `file_store` module or `filesystem_service`?

**A**: `file_store` is correct. It's a utility for building stripes, not filesystem logic.

**Q**: Should `StripeBuilder` expose erasure-coded chunks?

**A**: No. It's just a data buffer. `FileStore::write_stripe()` handles erasure coding.

### 8. Metrics and Observability

**Q**: What metrics do we need?

**Essential**:
- `buffered_file_handles.active` - Number of active handles
- `buffered_file_handles.memory_bytes` - Total buffered memory
- `buffered_file_handles.partial_flushes` - Partial flush count
- `buffered_file_handles.full_flushes` - Full flush count
- `buffered_file_handles.flush_latency` - Flush duration histogram
- `buffered_file_handles.writes_coalesced` - Writes that hit buffer

### 9. Configuration Tuning

**Q**: What are reasonable default values?

**Proposed**:
```rust
BufferedFileHandleConfig {
    max_memory_bytes: 20 * 1024 * 1024,  // 20MB per handle
    max_flush_interval: Duration::from_secs(5),
    max_writes_before_flush: 100,
}
```

**Rationale**:
- 20MB = ~5 full stripes (4MB each)
- Smaller buffer = more frequent flushes = lower memory footprint
- 100 writes balances metadata coalescing vs flush frequency

### 10. Backward Compatibility

**Q**: How do we migrate from StripeCache to BufferedFileHandle?

**A**: See Migration Plan below.

---

## Implementation Phases

### Phase 1: StripeBuilder Utility (Week 1)

**Goal**: Create and test `StripeBuilder` in isolation

**Tasks**:
1. Create `src/file_store/stripe_builder.rs`
2. Implement `StripeBuilder` with tests
3. Add `StripeId::new()` generation
4. Add unit tests for append, capacity, memory accounting

**Success Criteria**:
- All unit tests pass
- No changes to existing code yet

### Phase 2: BufferedFileHandle Core (Week 2)

**Goal**: Implement BufferedFileHandle without integration

**Tasks**:
1. Create `src/filesystem_service/buffered_file_handle.rs`
2. Implement data structures (stripes, builders, tombstones)
3. Implement `write()` and `read()` with buffering
4. Add memory accounting
5. Implement partial and full flush logic
6. Add unit tests with mock dependencies

**Success Criteria**:
- Can write and read back buffered data
- Partial flush frees memory
- Full flush would call correct Raft commands (mocked)

### Phase 3: Raft Integration (Week 3)

**Goal**: Verify existing Raft batching supports atomic updates

**Tasks**:
1. Verify `propose_operations(Vec<MetadataOperation>)` is atomic
2. Add test: batch of create/update/delete operations
3. Test crash recovery (operations all-or-nothing)
4. Add inform(OperationType) pattern to BufferedFileHandle

**Inform Pattern Implementation**:
```rust
pub enum OperationType {
    Truncate,
    Setattr,
    Rename,
    Lock,
}

impl BufferedFileHandle {
    pub async fn inform(&self, op: OperationType) -> Result<(), Error> {
        match op {
            OperationType::Truncate => self.full_flush().await,
            OperationType::Setattr if self.has_buffered_data() => self.full_flush().await,
            _ => Ok(()),
        }
    }
}
```

**Success Criteria**:
- Full flush updates MetadataStore atomically
- Can read flushed data through normal path
- Inform pattern simplifies complex operations

### Phase 4: FileSystemService Integration (Week 4)

**Goal**: Replace StripeCache usage with BufferedFileHandle

**Tasks**:
1. Add `buffered_handle: Arc<BufferedFileHandle>` to `OpenFile`
2. Update `write()` to delegate to handle
3. Update `read()` to check handle first
4. Update `release()` to full flush handle
5. Update `fsync()` to full flush handle
6. Run existing integration tests

**Success Criteria**:
- All existing tests pass
- 30MB test works
- 300MB test works (the failing one!)

### Phase 5: StripeCache Removal (Week 5)

**Goal**: Clean up old code using stripe_cache_removal_checklist.md as a guide.

**Tasks**:
1. Remove `src/filesystem_service/stripe_cache.rs`
2. Remove StripeCache from `FileSystemService` struct
3. Remove StripeCache tests
4. Remove StripeCache config options
5. Update documentation

**Success Criteria**:
- Clean build with no warnings
- All tests pass
- Code is simpler

### Phase 6: Metrics and AdminUI (Week 6)

**Goal**: Add observability

**Tasks**:
1. Add metrics to BufferedFileHandle
2. Create AdminUI section for BufferedFileHandle
3. Add to demo script output
4. Performance testing and tuning

**Success Criteria**:
- Metrics visible in AdminUI
- Can observe memory usage, flush frequency
- IO amplification ratios calculated correctly

---

## Migration Plan

### Removing StripeCache Integration Points

1. **FileSystemService struct** (`implementation.rs`):
   ```rust
   // Remove:
   stripe_cache: Arc<StripeCache<StripeWriteContext>>,

   // Add to OpenFile:
   buffered_handle: Arc<BufferedFileHandle>,
   ```

2. **Factory** (`factory.rs`):
   ```rust
   // Remove StripeCache creation
   // Add BufferedFileHandle creation per file open
   ```

3. **Write path** (`implementation.rs::write`):
   ```rust
   // Remove:
   self.stripe_cache.write_stripe(...).await?;

   // Replace with:
   open_file.buffered_handle.write(offset, data).await?;
   ```

4. **Release path** (`implementation.rs::release`):
   ```rust
   // Remove:
   self.stripe_cache.flush_write_group(...).await?;

   // Replace with:
   open_file.buffered_handle.full_flush().await?;
   ```

5. **Tests**:
   - Remove `tests/stripe_cache_test.rs`
   - Remove `tests/stripe_cache_integration_test.rs`
   - Update all other tests to not reference StripeCache

### Compatibility Considerations

- No disk format changes
- No protocol changes
- Metrics names will change (acceptable)
- Config options will change (document in migration guide)

---

## Testing Strategy

### Unit Tests

1. **StripeBuilder**:
   - Test append within capacity
   - Test append exceeding capacity
   - Test memory accounting
   - Test ID pre-allocation

2. **BufferedFileHandle**:
   - Test simple write → read
   - Test multi-stripe write
   - Test partial flush frees memory
   - Test full flush clears dirty flag
   - Test tombstone tracking
   - Test read-your-writes
   - Test concurrent writes to different stripes

### Integration Tests

1. **Small file (< max_stripe_size)**:
   - Single stripe, no flush needed
   - Verify read-back correct

2. **Medium file (few stripes)**:
   - Multiple stripes, fits in memory
   - Verify read-back correct

3. **Large file (> max_memory_bytes)**:
   - **This is the 300MB test that currently fails!**
   - Should trigger partial flushes
   - Should still read back correctly
   - Verify IO amplification is low

4. **Truncate scenarios**:
   - Truncate to 0
   - Truncate to middle
   - Truncate beyond EOF

5. **Concurrent access**:
   - Multiple threads writing different regions
   - Dup'd file descriptors
   - Read while writing

### Regression Tests

- All existing tests must continue to pass
- Verify IO amplification metrics improve

### Performance Tests

- Measure flush latency distribution
- Measure memory usage under load
- Compare with StripeCache baseline (should be better!)

---

## Success Metrics

### Functional

- ✅ 300MB file test passes (currently fails)
- ✅ All existing tests pass
- ✅ No data corruption
- ✅ Read-your-writes guaranteed

### Performance

- ✅ IO amplification < 2x for 2+1 erasure coding
- ✅ Memory usage bounded per handle
- ✅ Flush latency < 100ms for typical workloads

### Code Quality

- ✅ Simpler than StripeCache (fewer lines, clearer logic)
- ✅ No race conditions (verified by testing)
- ✅ Well documented
- ✅ Good test coverage (>80%)

---

## Appendix: Comparison with StripeCache

| Aspect | StripeCache | BufferedFileHandle |
|--------|-------------|-------------------|
| Scope | Global cache across all files | Per-file-handle buffer |
| Metadata | Separate from data | Together with data |
| Flush | Partial via write_groups | Full flush only (data can pre-flush) |
| Race Conditions | Many (write_group reuse, partial flush) | None (isolated per handle) |
| Memory Limits | Global, hard to enforce | Per-handle, natural |
| Read-your-writes | Not guaranteed | Always guaranteed |
| Eviction | LRU (complex) | FIFO builders (simple) |
| Code Complexity | ~1300 LOC | ~600 LOC (estimated) |

---

## References

- Issue #101: Add new FileSystemCache component
- Issue #97: Data corruption with StripeCache
- POSIX file semantics
- Raft consensus protocol
- Reed-Solomon erasure coding

---

**Next Steps**: Review this design document, answer open questions, then proceed with Phase 1 implementation.
