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
    operation: MetadataOperation,  // From StorageRaftMember
    timestamp: SystemTime,
    checksum: u32,  // CRC32 of operation bytes
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
impl TransactionLogStore {
    /// Create a new TransactionLogStore
    /// Returns a cheap-to-clone client handle
    pub fn new(config: TransactionLogConfig) -> Result<Self, LogError>;
    
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
    
    /// Get the first log index
    pub async fn get_first_index(&self) -> Result<Option<u64>, LogError>;
    
    /// Get the last log index
    pub async fn get_last_index(&self) -> Result<Option<u64>, LogError>;
    
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
    
    /// Cache size for redb
    pub cache_size_mb: usize,
    
    /// Enable checksums for log entries
    pub enable_checksums: bool,
    
    /// Compact database when log grows beyond this size
    pub compact_threshold_mb: usize,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub operation: MetadataOperation,
    pub timestamp: SystemTime,
    pub checksum: Option<u32>,
}

impl LogEntry {
    pub fn new(
        index: u64,
        term: u64,
        operation: MetadataOperation,
    ) -> Self {
        let checksum = Self::compute_checksum(&operation);
        Self {
            index,
            term,
            operation,
            timestamp: SystemTime::now(),
            checksum: Some(checksum),
        }
    }
    
    fn compute_checksum(operation: &MetadataOperation) -> u32 {
        // Serialize and checksum the operation
        let bytes = bincode::serialize(operation).unwrap();
        crc32fast::hash(&bytes)
    }
    
    pub fn verify_checksum(&self) -> bool {
        if let Some(expected) = self.checksum {
            let actual = Self::compute_checksum(&self.operation);
            expected == actual
        } else {
            true  // No checksum to verify
        }
    }
    
    pub fn into_raft_entry(self) -> openraft::Entry<WormFsTypeConfig> {
        openraft::Entry {
            log_id: LogId::new(self.term, self.index),
            payload: EntryPayload::Normal(self.operation),
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
    pub verified_entries: u64,
    pub corrupt_entries: Vec<u64>,
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
    
    #[error("Checksum mismatch at index {0}")]
    ChecksumMismatch(u64),
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Corruption detected: {0}")]
    Corruption(String),
}
```

## Dependencies

### Direct Dependencies
- **StorageRaftMember**: Consumer of transaction log (reads/writes)

### External Dependencies
- `redb`: Embedded key-value database
- `bincode`: Serialization for log entries
- `crc32fast`: Checksum calculation
- `tokio`: Async runtime

## Configuration

```toml
[transaction_log]
db_path = "/var/lib/wormfs/transaction_log.redb"
cache_size_mb = 128
enable_checksums = true
compact_threshold_mb = 100

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
- If checksum mismatch: log error and return corruption error
- If database error: attempt retry, then return error

### Corruption Detection
- Checksums detect data corruption
- Missing indices detected during integrity checks
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
- Checksum verification
- Metadata tracking (first/last index)

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

1. **Batch Size**: What's the optimal batch size for append_entries operations? Trade-off between latency and throughput.

2. **Compaction Strategy**: Should compaction be automatic (background task) or manual (admin triggered)?

3. **Checksum Algorithm**: Is CRC32 sufficient, or should we use a stronger hash like SHA-256?

4. **Cache Size**: How should we tune redb cache size for different workloads? Auto-tuning based on available memory?

5. **Read Optimization**: Should we cache recently accessed log entries in memory?

6. **Corruption Recovery**: Should we support automatic recovery from partial corruption, or always require snapshot install?

7. **Monitoring**: What metrics should we expose? Append latency, read latency, size growth rate, compaction frequency?

8. **Backup Strategy**: Should we support online backups of the transaction log for disaster recovery?

9. **Compression**: Should we compress log entries to reduce storage? Trade-off with CPU overhead.

10. **Multi-File Support**: Should we support splitting the log across multiple files for easier management?

11. **Encryption**: Should transaction log be encrypted at rest? Key management approach?

12. **Snapshot Coordination**: How should we coordinate log trimming with snapshot creation to avoid trimming too aggressively?

13. **Index Gaps**: How should we handle gaps in log indices? Reject, fill with no-op entries, or allow?

14. **Retention Policy**: Should there be a maximum log size/age before forcing snapshot creation?

15. **Integrity Checks**: How frequently should we run integrity checks? On startup, periodic background, or manual?
