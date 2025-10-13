# TransactionLogStore Component Design

## Purpose & Responsibilities

TransactionLogStore is the redb-based persistent storage for the Raft transaction log, providing the source of truth for replication and recovery. Its responsibilities include:

- Durably storing Raft log entries (metadata operations)
- Supporting append-only log operations
- Providing log entry retrieval for replication
- Supporting log trimming after snapshot creation
- Ensuring log integrity and consistency
- Providing efficient sequential access for log replay
- Supporting range queries for log replication
- Managing log compaction and cleanup

## Architecture & Design

### redb Storage Structure

```
┌─────────────────────────────────────────────────────────┐
│            TransactionLogStore (redb)                    │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  Database File: /var/lib/wormfs/transaction_log.redb    │
│                                                           │
│  Tables:                                                 │
│  ┌─────────────────────────────────────────────────┐   │
│  │  log_entries:                                    │   │
│  │    key: log_index (u64)                          │   │
│  │    value: LogEntry {                             │   │
│  │      term: u64,                                  │   │
│  │      operation: MetadataOperation (bytes),       │   │
│  │      timestamp: u64,                             │   │
│  │      checksum: u32                               │   │
│  │    }                                             │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  ┌─────────────────────────────────────────────────┐   │
│  │  metadata:                                       │   │
│  │    key: string                                   │   │
│  │    value: bytes                                  │   │
│  │    - "first_index" -> u64                        │   │
│  │    - "last_index" -> u64                         │   │
│  │    - "last_snapshot_index" -> u64                │   │
│  │    - "format_version" -> u16                     │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  Log Operations:                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │  1. Append entry                                 │   │
│  │  2. Get entry by index                           │   │
│  │  3. Get range of entries                         │   │
│  │  4. Trim entries before index                    │   │
│  │  5. Get first/last index                         │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Log Entry Format

```rust
// Stored in redb as bincode-serialized bytes
struct LogEntry {
    term: u64,
    operations: Vec<MetadataOperation>,  // From StorageRaftMember
    timestamp: SystemTime,
}
```

## Interfaces

### Architecture: Client Pattern with Interior Mutability

To support OpenRaft's requirement for exclusive ownership while allowing other components concurrent access, TransactionLogStore uses a client/server pattern:

```rust
/// Internal implementation with interior mutability
struct TransactionLogStoreInner {
    db: RwLock<redb::Database>,
    config: TransactionLogConfig,
}

/// Cheap-to-clone client handle
/// Multiple components can hold cloned instances
#[derive(Clone)]
pub struct TransactionLogStore {
    inner: Arc<TransactionLogStoreInner>,
}
```

**Key Benefits:**
1. **OpenRaft Compatibility**: OpenRaft can "own" a TransactionLogStore instance (which clones cheaply)
2. **Concurrent Access**: StorageRaftMember, Watchdog, etc. can hold their own cloned instances
3. **Thread Safety**: Interior mutability via RwLock ensures safe concurrent access
4. **Optimized for Append**: Write-heavy operations use write lock, read operations use read lock

### Public API

```rust

impl TransactionLogStoreFactory {
    /// Create a new TransactionLogStore
    /// Returns a cheap-to-clone client handle
    pub fn new(config: TransactionLogConfig) -> Result<TransactionLogStore, LogError>;
}

impl TransactionLogStore {
    /// Initialize log tables
    /// Uses interior mutability for thread-safe access
    pub async fn initialize(&self) -> Result<(), LogError>;
    
    /// Append a single log entry
    pub async fn append_entry(
        &self,
        index: u64,
        term: u64,
        operation: MetadataOperation,
    ) -> Result<(), LogError>;
    
    /// Append multiple log entries atomically
    pub async fn append_entries(
        &self,
        entries: Vec<(u64, u64, MetadataOperation)>,
    ) -> Result<(), LogError>;
    
    /// Get a log entry by index
    pub async fn get_entry(&self, index: u64) -> Result<Option<LogEntry>, LogError>;
    
    /// Get a range of log entries [start, end)
    pub async fn get_entries(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<LogEntry>, LogError>;
    
    /// Get the oldest log index available
    pub async fn get_oldest_index(&self) -> Result<Option<u64>, LogError>;
    
    /// Get the most recent log index
    pub async fn get_tip_index(&self) -> Result<Option<u64>, LogError>;
    
    /// Get the term of a specific log entry
    pub async fn get_entry_term(&self, index: u64) -> Result<Option<u64>, LogError>;
    
    /// Trim log entries before the given index (after snapshot)
    pub async fn trim_before(&self, index: u64) -> Result<u64, LogError>;
    
    /// Delete entries from index onwards (for Raft log truncation)
    pub async fn delete_from(&self, index: u64) -> Result<u64, LogError>;
    
    /// Get log statistics
    pub fn get_stats(&self) -> LogStats;
    
    /// Verify log integrity
    pub async fn verify_integrity(&self) -> Result<IntegrityReport, LogError>;
    
    /// Compact the database (reclaim space)
    pub async fn compact(&self) -> Result<(), LogError>;
}
```

### OpenRaft Integration

```rust
/// Implement OpenRaft's RaftLogStorage trait
#[async_trait]
impl RaftLogStorage<WormFsTypeConfig> for TransactionLogStore {
    async fn append(
        &mut self,
        entries: &[&LogEntry<WormFsTypeConfig>],
    ) -> Result<(), StorageError> {
        let entries_to_append: Vec<_> = entries
            .iter()
            .map(|e| (e.log_id.index, e.log_id.term, e.payload.clone()))
            .collect();
        
        self.append_entries(entries_to_append).await?;
        Ok(())
    }
    
    async fn delete_conflict_entries_since(
        &mut self,
        log_id: LogId,
    ) -> Result<(), StorageError> {
        self.delete_from(log_id.index).await?;
        Ok(())
    }
    
    async fn purge_logs_upto(
        &mut self,
        log_id: LogId,
    ) -> Result<(), StorageError> {
        self.trim_before(log_id.index).await?;
        Ok(())
    }
    
    async fn get_log_entries(
        &mut self,
        range: Range<u64>,
    ) -> Result<Vec<LogEntry<WormFsTypeConfig>>, StorageError> {
        let entries = self.get_entries(range.start, range.end).await?;
        Ok(entries.into_iter().map(|e| e.into_raft_entry()).collect())
    }
    
    async fn get_log_state(&mut self) -> Result<LogState, StorageError> {
        let first = self.get_first_index().await?;
        let last = self.get_last_index().await?;
        
        Ok(LogState {
            last_purged_log_id: first.map(|i| LogId::new(0, i - 1)),
            last_log_id: last.map(|i| {
                let term = self.get_entry_term(i).await?.unwrap_or(0);
                LogId::new(term, i)
            }),
        })
    }
}
```

### Data Structures

```rust
pub struct TransactionLogConfig {
    /// Path to redb database file
    pub db_path: PathBuf,
    
    /// Cache size for redb (in MB)
    pub cache_size_mb: usize,
    
    /// Compact database when log grows beyond this size (in MB)
    pub compact_threshold_mb: usize,
    
    /// Maximum log size before snapshot is recommended (in MB)
    pub max_log_size_mb: usize,
    
    /// Maximum log age before snapshot is recommended (in days)
    pub max_log_age_days: u32,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub operations: Vec<MetadataOperation>,
    pub timestamp: SystemTime,
}

impl LogEntry {
    pub fn new(
        index: u64,
        term: u64,
        operations: Vec<MetadataOperation>,
    ) -> Self {
        Self {
            index,
            term,
            operations,
            timestamp: SystemTime::now(),
        }
    }
    
    pub fn into_raft_entry(self) -> openraft::Entry<WormFsTypeConfig> {
        openraft::Entry {
            log_id: LogId::new(self.term, self.index),
            payload: EntryPayload::Normal(self.operations),
        }
    }
}

pub struct LogStats {
    pub first_index: Option<u64>,
    pub last_index: Option<u64>,
    pub entry_count: u64,
    pub db_size_bytes: u64,
    pub last_compaction: Option<SystemTime>,
}

pub struct IntegrityReport {
    pub total_entries: u64,
    pub missing_indices: Vec<u64>,
    pub is_valid: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum LogError {
    #[error("Database error: {0}")]
    DatabaseError(String),
    
    #[error("Entry not found at index {0}")]
    EntryNotFound(u64),
    
    #[error("Invalid log index: {0}")]
    InvalidIndex(u64),
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}
```

## Dependencies

### Direct Dependencies
- **StorageRaftMember**: Consumer of transaction log (reads/writes)

### External Dependencies
- `redb`: Embedded key-value database
- `bincode`: Serialization for log entries
- `tokio`: Async runtime

## Configuration

```toml
[transaction_log]
db_path = "/var/lib/wormfs/transaction_log.redb"
cache_size_mb = 8
compact_threshold_mb = 100

# Log retention policy (triggers snapshot recommendation)
max_log_size_mb = 128
max_log_age_days = 7

# Automatic compaction
auto_compact = true
compact_interval_hours = 24
```

## Error Handling

### Append Failures
- If disk full: return error to RaftMember
- If serialization fails: return error (should not happen)
- If database corruption: attempt recovery or require snapshot install
- All failures logged with context

### Read Failures
- If entry not found: return None (valid for range queries)
- If database error: attempt retry, then return error

### Corruption Detection
- redb provides built-in integrity guarantees
- Missing indices detected during integrity checks (if implemented)
- Corruption triggers snapshot install from leader
- Operators alerted for investigation

### Database Corruption
- redb provides ACID guarantees, corruption rare
- If detected: attempt to recover last known good state
- May require snapshot install from cluster
- Backup transaction log before recovery attempts

## Testing Strategy

### Unit Tests
- Append and retrieve single entries
- Append and retrieve ranges
- Trim operations
- Metadata tracking (first/last index)
- Multiple operations per log entry

### Integration Tests
- Large log replay scenarios
- Concurrent append and read operations
- Log trimming after snapshot
- Corruption detection and handling
- Database compaction

### Performance Tests
- Append throughput (entries/sec)
- Range query performance
- Large log replay time
- Database compaction time
- Memory usage under load

### Durability Tests
- Crash recovery scenarios
- Partial write detection
- Concurrent access safety
- Disk full handling

## Open Questions

1. **Batch Size**: What's the optimal batch size for append_entries operations? Trade-off between latency and throughput. Answer: We need this log to be highly durable so we need to avoid batching if it will increase the potential for data loss.

2. **Compaction Strategy**: Should compaction be automatic (background task) or manual (admin triggered)? Answer: Compaction should be automatic.

3. **Cache Size**: How should we tune redb cache size for different workloads? Auto-tuning based on available memory? Answer: We can use a simple config to control cache size. A goo default is 8 MB.

5. **Read Optimization**: Should we cache recently accessed log entries in memory? Answer: No, lets keep it simple for now.

6. **Corruption Recovery**: Should we support automatic recovery from partial corruption, or always require snapshot install? Answer: We can keep things simple for now and rely on snapshots for recovery.

7. **Monitoring**: What metrics should we expose? Answer: We can start with basic metrics including: append latency, read latency, log size, and compaction reduction (bytes).

8. **Backup Strategy**: Should we support online backups of the transaction log for disaster recovery? Answer: We do not need any backup strategy for the transaction log.

9. **Compression**: Should we compress log entries to reduce storage? Trade-off with CPU overhead. Answer: No compression is needed at this time.

10. **Multi-File Support**: Should we support splitting the log across multiple files for easier management? Answer: No, we can keep the log simple for now.

11. **Encryption**: Should transaction log be encrypted at rest? Key management approach? Answer: No encryption is needed at this time, we can assume the disk is sufficiently secure.

12. **Snapshot Coordination**: How should we coordinate log trimming with snapshot creation to avoid trimming too aggressively? Answer: StorageRaftMember will handle this coordination by directing us to trim the log to a specific index.

13. **Index Gaps**: How should we handle gaps in log indices? Reject, fill with no-op entries, or allow? Answer: We should allow gaps.

14. **Retention Policy**: Should there be a maximum log size/age before forcing snapshot creation? Answer: Yes, this should be controlled by StorageRaftMember and be configured through a simple option in our configuration file. A good default is likely 128MB or X days, whichever comes first.

15. **Integrity Checks**: How frequently should we run integrity checks? On startup, periodic background, or manual? Answer: We do not need to run integrity checks of the transaction log for now. We may add it in the future.
