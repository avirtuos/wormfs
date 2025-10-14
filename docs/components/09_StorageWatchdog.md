# StorageWatchdog Component Design

## Purpose & Responsibilities

StorageWatchdog is the background monitoring and repair component that continuously validates data integrity and coordinates recovery operations. Its responsibilities include:

- Running shallow chunk checks to detect missing chunks quickly
- Running deep integrity checks to validate chunk and stripe checksums
- Detecting and reporting corrupt or missing chunks
- Coordinating stripe reconstruction through StorageRaftMember
- Monitoring disk health and chunk placement
- Tracking verification progress and metrics
- Scheduling periodic integrity checks
- Prioritizing repairs based on severity

## Architecture & Design

### Watchdog Task Loop

```
┌─────────────────────────────────────────────────────────┐
│              StorageWatchdog (Leader-only)               │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  Background Tasks (run on Raft leader only):             │
│                                                           │
│  ┌─────────────────────────────────────────────────┐   │
│  │  Shallow Check Task                             │   │
│  │  (every 1 week)                                 │   │
│  │  ┌──────────────────────────────────────────┐   │   │
│  │  │  1. Get all files from MetadataStore     │   │   │
│  │  │  2. For each file's stripes:             │   │   │
│  │  │     - Get chunk locations                │   │   │
│  │  │     - Call check_chunk on each node      │   │   │
│  │  │     - Record missing/unavailable chunks  │   │   │
│  │  │  3. Submit repair requests for failures  │   │   │
│  │  └──────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────┘   │
│                                                        │
│  ┌─────────────────────────────────────────────────┐   │
│  │  Deep Check Task                                │   │
│  │  (every 1 month)                                │   │
│  │  ┌──────────────────────────────────────────┐   │   │
│  │  │  1. Get all files from MetadataStore     │   │   │
│  │  │  2. For each file's stripes:             │   │   │
│  │  │     - Read all chunks                    │   │   │
│  │  │     - Verify chunk checksums             │   │   │
│  │  │     - Reconstruct stripe                 │   │   │
│  │  │     - Verify stripe checksum             │   │   │
│  │  │     - Record corrupt chunks              │   │   │
│  │  │  3. Submit repair requests for failures  │   │   │
│  │  └──────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  ┌─────────────────────────────────────────────────┐   │
│  │  Repair Task                                     │   │
│  │  (continuous)                                    │   │
│  │  ┌──────────────────────────────────────────┐   │   │
│  │  │  1. Dequeue repair request               │   │   │
│  │  │  2. Call FileStore.rebuild_stripe()      │   │   │
│  │  │  3. Update verification timestamp        │   │   │
│  │  │  4. Record repair metrics                │   │   │
│  │  └──────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  Repair Queue (priority queue):                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │  Priority 1: Multiple missing chunks (critical)  │   │
│  │  Priority 2: Single missing chunk (high)         │   │
│  │  Priority 3: Corrupt chunk detected (medium)     │   │
│  │  Priority 4: Periodic verification (low)         │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### WatchDog Coordination

```
All Nodes:
  1. Perform periodic "check" for locally stored chunks
  1a. Notify Leader of missing/damaged chunks
  
Leader Node:
  1. Perform periodic "deep check" for all Stripe/Chunks across all nodes.
  2. Queue repair of missing / damaged Stripes & Chunks
  3. The Watchdog on the Leader is also continuously processing the repair queue.
```

## Interfaces

### Public API

```rust
pub struct StorageWatchdog {
    config: WatchdogConfig,
    raft_member: Arc<StorageRaftMember>,
    file_store: Arc<FileStore>,
    metadata_store: Arc<MetadataStore>,
    storage_endpoint: Arc<StorageEndpoint>,
    repair_queue: Arc<Mutex<PriorityQueue<RepairRequest>>>,
    state: Arc<RwLock<WatchdogState>>,
}

impl StorageWatchdog {
    /// Create a new StorageWatchdog
    pub fn new(
        config: WatchdogConfig,
        raft_member: Arc<StorageRaftMember>,
        file_store: Arc<FileStore>,
        metadata_store: Arc<MetadataStore>,
        storage_endpoint: Arc<StorageEndpoint>,
    ) -> Self;
    
    /// Start watchdog tasks (called when node becomes leader)
    pub async fn start(&self) -> Result<(), WatchdogError>;
    
    /// Stop watchdog tasks (called when node loses leadership)
    pub async fn stop(&self) -> Result<(), WatchdogError>;
    
    /// Submit a consistency event for processing
    pub async fn submit_event(&self, event: ConsistencyEvent) -> Result<(), WatchdogError>;
    
    /// Get watchdog status and metrics
    pub fn get_status(&self) -> WatchdogStatus;
    
    /// Manually trigger a shallow check
    pub async fn trigger_shallow_check(&self) -> Result<(), WatchdogError>;
    
    /// Manually trigger a deep check
    pub async fn trigger_deep_check(&self) -> Result<(), WatchdogError>;
    
    /// Get verification progress
    pub fn get_verification_progress(&self) -> VerificationProgress;
}

/// Internal task implementations
impl StorageWatchdog {
    async fn shallow_check_task(&self);
    async fn deep_check_task(&self);
    async fn repair_task(&self);
    
    async fn check_file_shallow(&self, file_id: FileId) -> Result<Vec<ConsistencyEvent>, WatchdogError>;
    async fn check_file_deep(&self, file_id: FileId) -> Result<Vec<ConsistencyEvent>, WatchdogError>;
    async fn process_repair(&self, request: RepairRequest) -> Result<(), WatchdogError>;
}
```

### Data Structures

```rust
pub struct WatchdogConfig {
    /// Interval for shallow checks
    pub shallow_check_interval: Duration,
    
    /// Interval for deep checks
    pub deep_check_interval: Duration,
    
    /// Maximum concurrent repair operations
    pub max_concurrent_repairs: usize,
    
    /// Maximum retries for failed repairs
    pub max_repair_retries: usize,
    
    /// Delay between repair retries
    pub repair_retry_delay: Duration,
}

#[derive(Debug, Clone)]
pub enum ConsistencyEvent {
    /// Chunk is missing (shallow check)
    ChunkMissing {
        file_id: FileId,
        stripe_id: StripeId,
        chunk_id: ChunkId,
        node_id: NodeId,
    },
    
    /// Chunk is corrupt (deep check)
    ChunkCorrupt {
        file_id: FileId,
        stripe_id: StripeId,
        chunk_id: ChunkId,
        node_id: NodeId,
        reason: String,
    },
    
    /// Stripe checksum mismatch (deep check)
    StripeCorrupt {
        file_id: FileId,
        stripe_id: StripeId,
        reason: String,
    },
    
    /// Node unreachable
    NodeUnreachable {
        node_id: NodeId,
        affected_chunks: Vec<ChunkId>,
    },
    
    /// Disk failure
    DiskFailure {
        node_id: NodeId,
        disk_id: DiskId,
        affected_chunks: Vec<ChunkId>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairRequest {
    pub file_id: FileId,
    pub stripe_id: StripeId,
    pub priority: RepairPriority,
    pub created_at: SystemTime,
    pub retry_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RepairPriority {
    Critical = 0,  // Multiple chunks missing
    High = 1,      // Single chunk missing
    Medium = 2,    // Corrupt chunk
    Low = 3,       // Periodic verification
}

struct WatchdogState {
    is_running: bool,
    shallow_check_position: Option<FileId>,
    deep_check_position: Option<FileId>,
    last_shallow_check: Option<SystemTime>,
    last_deep_check: Option<SystemTime>,
    active_repairs: HashMap<StripeId, RepairStatus>,
}

#[derive(Debug, Clone)]
pub struct WatchdogStatus {
    pub is_running: bool,
    pub is_leader: bool,
    pub shallow_checks_completed: u64,
    pub deep_checks_completed: u64,
    pub repairs_completed: u64,
    pub repairs_failed: u64,
    pub pending_repairs: usize,
    pub last_shallow_check: Option<SystemTime>,
    pub last_deep_check: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct VerificationProgress {
    pub shallow_check_progress: CheckProgress,
    pub deep_check_progress: CheckProgress,
}

#[derive(Debug, Clone)]
pub struct CheckProgress {
    pub total_files: u64,
    pub checked_files: u64,
    pub total_stripes: u64,
    pub checked_stripes: u64,
    pub issues_found: u64,
    pub started_at: Option<SystemTime>,
    pub estimated_completion: Option<SystemTime>,
}

struct RepairStatus {
    request: RepairRequest,
    started_at: SystemTime,
    retry_count: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum WatchdogError {
    #[error("Watchdog not running")]
    NotRunning,
    
    #[error("Not the leader")]
    NotLeader,
    
    #[error("Repair failed: {0}")]
    RepairFailed(String),
    
    #[error("Metadata error: {0}")]
    MetadataError(String),
    
    #[error("FileStore error: {0}")]
    FileStoreError(String),
    
    #[error("Network error: {0}")]
    NetworkError(String),
}
```

## Dependencies

### Direct Dependencies
- **StorageRaftMember**: Check if leader, submit repair operations
- **FileStore**: Chunk verification and stripe reconstruction
- **MetadataStore**: Query file/stripe/chunk metadata
- **StorageEndpoint**: Check chunks on remote nodes (special local API)

### External Dependencies
- `tokio`: Async runtime and task scheduling
- `priority-queue`: Priority queue for repair requests
- `tracing`: Logging and metrics

## Configuration

```toml
[watchdog]
# Check intervals
shallow_check_interval_mins = 10080
deep_check_interval_hours = 43200

# Repair settings
max_concurrent_repairs = 5
max_repair_retries = 3
repair_retry_delay_secs = 10

# Performance tuning
shallow_check_batch_size = 100  # Files per batch
deep_check_batch_size = 10      # Files per batch (more expensive)
```

## Error Handling

### Check Failures
- Node unreachable: Mark chunks as offline, retry later. Offline is less severe than missing or corrupt as offline can be temporary where are missing or corrupt are likely terminal.
- Timeout: Skip for now, retry in next cycle
- Network errors: Log and continue with next check
- All failures tracked in metrics

### Repair Failures
- Insufficient shards: Mark as critical, alert operators
- Network failure: Retry with exponential backoff
- Max retries exceeded: Alert operators, require manual intervention
- All failures logged with full context

### Leader Changes
- Save state before stepping down
- Resume from last position on becoming leader
- Clear repair queue (will be rebuilt from metadata)

## Testing Strategy

### Unit Tests
- Shallow check logic
- Deep check logic
- Repair prioritization
- State save/restore
- Event processing

### Integration Tests
- Full check cycle with missing chunks
- Full check cycle with corrupt chunks
- Repair coordination with FileStore
- Leader election handling
- Concurrent checks and repairs

### Chaos Tests
- Random chunk deletion during checks
- Node failures during repairs
- Leader changes during checks
- Network partitions
- Disk failures

## Open Questions

1. **Repair Throttling**: Should we limit repair bandwidth to avoid impacting user operations? Answer: We don't need to worry about this for now but we should leave ourselves extension points where we can add this logic later so that repairs and checks can yield to client traffic so we don't starve either class of work.

6. **State Persistence**: Should verification state be persisted in MetadataStore or separate file? Answer: For each Stripe, we should store the time and result of the last shallow and deep verification. This should be stored in a dedicated redb instances. Since our list of stripes to check is driven by the list of Stripes in the MetadataStore, any Stripe that is present in this redb but hasn't had its last check updated in more than 3 intervals is likely no longer stored in the system and can safely be purged from redb by the watchdog.

6. **Alert Thresholds**: At what point should we alert operators vs. auto-repair? Answer: We should always auto-repair but we should be publishing appropriate metrics about the size of the repair queue, repair rate, etc so that an alerting mechanism can be built using the MetricService.

7. **Concurrency**: Should shallow and deep checks run concurrently or sequentially? Answer: They can run concurrently for now.

8. **Verification Window**: Should we support configurable time windows for checks (e.g., only during off-peak hours)? Answer: not right now, this might be a future enhancement though.

9. **Chunk Age**: Should we prioritize checking older chunks that haven't been verified recently? Answer: Yes, we should use the ext-sort crate to perform the sort of Stripes using the last check times from redb. ext-sort allows us to spill the sort to disk while using a configurable amount of memory (e.g. 10MB default limit). Then we can prioritize Stripes that haven't been checked recently.

10. **Repair Coordination**: Should multiple leaders coordinate repairs across the cluster, or rely on Raft leader only? Answer: Any node may request a Stripe repair be scheduled by the Leader but only the watchdog on the Leader will actually action repairs.

11. **Metrics Export**: What specific metrics should be exposed for monitoring? Answer: We can start with basic metrics like check rate, repair rate, failure rate, and repair queue size.

12. **Manual Intervention**: What APIs should be exposed for operators to manually trigger checks/repairs? Answer: A simple API that allows admins to request deep check loop be kicked off should be sufficient. The API can be exposed via StorageEndpoint. We do not need an API for triggering the shallow check for now.
