# SnapshotStore Component Design

## Purpose & Responsibilities

SnapshotStore manages metadata snapshots for Raft log compaction, enabling nodes to catch up without replaying the entire transaction log. Its responsibilities include:

- Storing metadata snapshots triggered by StorageRaftMember
- Tracking snapshot metadata (snapshot ID, log index, timestamp)
- Providing snapshot retrieval for node recovery and catch-up
- Pruning old snapshots based on retention policy
- Verifying snapshot integrity
- Supporting incremental snapshot transfer for network efficiency
- Managing snapshot storage lifecycle

## Architecture & Design

### Snapshot Storage Layout

```
┌─────────────────────────────────────────────────────────┐
│                  SnapshotStore                           │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  Storage Directory:                                      │
│  /var/lib/wormfs/snapshots/                              │
│    ├── snapshot_registry.db (SQLite)                    │
│    ├── snapshot_000001/                                  │
│    │   ├── metadata.json                                │
│    │   ├── metadata.db (SQLite snapshot)                │
│    │   └── checksum.sha256                              │
│    ├── snapshot_000002/                                  │
│    │   ├── metadata.json                                │
│    │   ├── metadata.db                                  │
│    │   └── checksum.sha256                              │
│    └── snapshot_000003/                                  │
│        ├── metadata.json                                │
│        ├── metadata.db                                  │
│        └── checksum.sha256                              │
│                                                           │
│  Snapshot Lifecycle:                                     │
│  ┌─────────────────────────────────────────────────┐   │
│  │  1. Leader triggers snapshot creation            │   │
│  │  2. SnapshotStore copies MetadataStore DB        │   │
│  │  3. Calculate checksum                           │   │
│  │  4. Record snapshot metadata                     │   │
│  │  5. Update registry                              │   │
│  │  6. Notify RaftMember of completion              │   │
│  │  7. Prune old snapshots (if policy exceeded)     │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Snapshot Metadata

```json
{
  "snapshot_id": 123,
  "log_index": 54321,
  "log_term": 5,
  "timestamp": "2025-01-10T12:00:00Z",
  "format_version": 1,
  "metadata_db_size": 104857600,
  "metadata_db_checksum": "sha256:abc123...",
  "compression": "none",
  "node_id": "node-1"
}
```

## Interfaces

### Architecture: Client Pattern with Interior Mutability

To support OpenRaft's requirement for exclusive ownership while allowing other components concurrent access, SnapshotStore uses a client/server pattern:

```rust
/// Internal implementation with interior mutability
struct SnapshotStoreInner {
    registry: RwLock<SnapshotRegistry>,
    config: SnapshotStoreConfig,
    storage_dir: PathBuf,
}

/// Cheap-to-clone client handle
/// Multiple components can hold cloned instances
#[derive(Clone)]
pub struct SnapshotStore {
    inner: Arc<SnapshotStoreInner>,
}
```

**Key Benefits:**
1. **OpenRaft Compatibility**: OpenRaft can "own" a SnapshotStore instance (which clones cheaply)
2. **Concurrent Access**: Multiple nodes can read snapshots while writes are coordinated
3. **Thread Safety**: Interior mutability via RwLock ensures safe concurrent access
4. **Read-Optimized**: Multiple nodes can stream snapshots concurrently via read locks

### Public API

```rust
impl SnapshotStore {
    /// Create a new SnapshotStore
    /// Returns a cheap-to-clone client handle
    pub fn new(config: SnapshotStoreConfig) -> Result<Self, SnapshotError>;
    
    /// Initialize snapshot storage directory
    /// Uses interior mutability for thread-safe access
    pub async fn initialize(&self) -> Result<(), SnapshotError>;
    
    /// Ingest a new snapshot (called by RaftMember after MetadataStore creates snapshot)
    pub async fn ingest_snapshot(
        &self,
        snapshot_id: u64,
        log_index: u64,
        log_term: u64,
        metadata_db_path: &Path,
    ) -> Result<SnapshotInfo, SnapshotError>;
    
    /// Get the latest snapshot
    pub async fn get_latest_snapshot(&self) -> Result<Option<SnapshotInfo>, SnapshotError>;
    
    /// Get a specific snapshot by ID
    pub async fn get_snapshot(&self, snapshot_id: u64) -> Result<SnapshotInfo, SnapshotError>;
    
    /// Get snapshot at or before a specific log index
    pub async fn get_snapshot_at_index(
        &self,
        log_index: u64,
    ) -> Result<Option<SnapshotInfo>, SnapshotError>;
    
    /// List all available snapshots
    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>, SnapshotError>;
    
    /// Open a snapshot for reading (returns path to metadata.db)
    pub async fn open_snapshot(
        &self,
        snapshot_id: u64,
    ) -> Result<SnapshotReader, SnapshotError>;
    
    /// Stream snapshot to a remote node
    pub async fn stream_snapshot(
        &self,
        snapshot_id: u64,
        sink: impl AsyncWrite + Unpin,
    ) -> Result<(), SnapshotError>;
    
    /// Receive and store a snapshot from a remote node
    pub async fn receive_snapshot(
        &self,
        snapshot_id: u64,
        log_index: u64,
        log_term: u64,
        source: impl AsyncRead + Unpin,
    ) -> Result<SnapshotInfo, SnapshotError>;
    
    /// Verify snapshot integrity
    pub async fn verify_snapshot(&self, snapshot_id: u64) -> Result<bool, SnapshotError>;
    
    /// Prune old snapshots based on retention policy
    pub async fn prune_snapshots(&self) -> Result<Vec<u64>, SnapshotError>;
    
    /// Delete a specific snapshot
    pub async fn delete_snapshot(&self, snapshot_id: u64) -> Result<(), SnapshotError>;
    
    /// Get storage statistics
    pub fn get_stats(&self) -> SnapshotStats;
}

pub struct SnapshotReader {
    snapshot_id: u64,
    metadata_path: PathBuf,
    // Allow reading the snapshot database
}

impl SnapshotReader {
    pub fn get_metadata_db_path(&self) -> &Path;
    pub fn get_snapshot_info(&self) -> &SnapshotInfo;
}
```

### Snapshot Registry

```rust
/// SQLite database tracking all snapshots
struct SnapshotRegistry {
    conn: rusqlite::Connection,
}

impl SnapshotRegistry {
    fn new(path: &Path) -> Result<Self, SnapshotError>;
    
    fn register_snapshot(&mut self, info: &SnapshotInfo) -> Result<(), SnapshotError>;
    
    fn get_snapshot(&self, snapshot_id: u64) -> Result<Option<SnapshotInfo>, SnapshotError>;
    
    fn get_latest_snapshot(&self) -> Result<Option<SnapshotInfo>, SnapshotError>;
    
    fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>, SnapshotError>;
    
    fn delete_snapshot(&mut self, snapshot_id: u64) -> Result<(), SnapshotError>;
}

// Registry schema
const REGISTRY_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id INTEGER PRIMARY KEY,
    log_index INTEGER NOT NULL,
    log_term INTEGER NOT NULL,
    timestamp INTEGER NOT NULL,
    format_version INTEGER NOT NULL,
    metadata_db_size INTEGER NOT NULL,
    metadata_db_checksum TEXT NOT NULL,
    compression TEXT NOT NULL,
    node_id TEXT NOT NULL
);

CREATE INDEX idx_snapshots_log_index ON snapshots(log_index);
CREATE INDEX idx_snapshots_timestamp ON snapshots(timestamp);
"#;
```

### Data Structures

```rust
pub struct SnapshotStoreConfig {
    /// Base directory for snapshot storage
    pub storage_dir: PathBuf,
    
    /// Retention policy
    pub retention_policy: RetentionPolicy,
    
    /// Compression algorithm (future use)
    pub compression: CompressionAlgorithm,
    
    /// Chunk size for streaming snapshots
    pub stream_chunk_size: usize,
}

#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Maximum number of snapshots to keep
    pub max_snapshots: usize,
    
    /// Maximum age of snapshots to keep
    pub max_age: Duration,
    
    /// Always keep at least this many snapshots
    pub min_snapshots: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum CompressionAlgorithm {
    None,
    // Future: Gzip, Zstd, etc.
}

#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub snapshot_id: u64,
    pub log_index: u64,
    pub log_term: u64,
    pub timestamp: SystemTime,
    pub format_version: u16,
    pub metadata_db_size: u64,
    pub metadata_db_checksum: String,
    pub compression: CompressionAlgorithm,
    pub node_id: NodeId,
    pub storage_path: PathBuf,
}

impl SnapshotInfo {
    pub fn metadata_db_path(&self) -> PathBuf {
        self.storage_path.join("metadata.db")
    }
    
    pub fn metadata_json_path(&self) -> PathBuf {
        self.storage_path.join("metadata.json")
    }
    
    pub fn checksum_path(&self) -> PathBuf {
        self.storage_path.join("checksum.sha256")
    }
}

pub struct SnapshotStats {
    pub total_snapshots: usize,
    pub total_size: u64,
    pub oldest_snapshot: Option<SystemTime>,
    pub newest_snapshot: Option<SystemTime>,
    pub disk_usage: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("Snapshot not found: {0}")]
    NotFound(u64),
    
    #[error("Invalid snapshot: {0}")]
    Invalid(String),
    
    #[error("Checksum mismatch")]
    ChecksumMismatch,
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Registry error: {0}")]
    RegistryError(String),
    
    #[error("Corruption detected: {0}")]
    Corruption(String),
    
    #[error("Storage full")]
    StorageFull,
}
```

## Dependencies

### Direct Dependencies
- **MetadataStore**: Source of snapshot data (receives path to snapshot file)
- **StorageRaftMember**: Triggers snapshot creation and retrieval

### External Dependencies
- `rusqlite`: Snapshot registry database
- `tokio::fs`: Async file I/O
- `sha2`: Checksum calculation
- `serde_json`: Metadata serialization

## Configuration

```toml
[snapshot_store]
storage_dir = "/var/lib/wormfs/snapshots"

# Retention policy
[snapshot_store.retention]
max_snapshots = 10
max_age_days = 30
min_snapshots = 3

# Streaming
stream_chunk_size_kb = 64

# Compression (not yet implemented)
compression = "none"
```

## Error Handling

### Snapshot Creation Failures
- If metadata.db copy fails: abort and clean up partial files
- If checksum calculation fails: abort and clean up
- If registry update fails: clean up snapshot files
- All failures logged with detailed context

### Snapshot Corruption
- Detect via checksum validation
- Mark corrupt snapshots in registry
- Prevent serving corrupt snapshots
- Alert operators for investigation

### Storage Full
- Check available space before snapshot creation
- If insufficient space: attempt to prune old snapshots
- If still insufficient: return error to RaftMember
- RaftMember can retry later

### Network Transfer Failures
- Use chunked streaming for reliability
- Support resume/retry for large snapshots
- Verify checksum after complete transfer
- Clean up incomplete transfers

## Testing Strategy

### Unit Tests
- Snapshot ingestion and registration
- Snapshot retrieval by ID and log index
- Checksum calculation and verification
- Registry operations
- Retention policy enforcement

### Integration Tests
- Full snapshot lifecycle (create, retrieve, delete)
- Concurrent snapshot operations
- Snapshot streaming to/from remote nodes
- Recovery from corrupt snapshots
- Pruning with various retention policies

### Performance Tests
- Large snapshot handling (multi-GB databases)
- Streaming throughput
- Concurrent snapshot access
- Registry query performance

## Open Questions

1. **Snapshot Compression**: Should we implement compression in the initial version? Trade-off between storage space and CPU/time.

2. **Incremental Snapshots**: Should we support incremental snapshots (only changed data), or always full snapshots?

3. **Streaming Protocol**: Should we use custom protocol or leverage existing transfer mechanisms (rsync, HTTP range requests)?

4. **Snapshot Validation**: How frequently should we validate snapshot integrity? On creation only, or periodic background checks?

5. **Concurrent Access**: Should multiple nodes be able to read the same snapshot simultaneously via network streaming?

6. **Snapshot Encryption**: Should snapshots be encrypted at rest? If yes, what key management approach?

7. **Snapshot Deduplication**: Should we deduplicate identical snapshots across nodes to save space?

8. **Registry Replication**: Should the snapshot registry be replicated via Raft, or maintained independently on each node?

9. **Snapshot Naming**: Should we use sequential IDs, timestamps, or log indices as primary identifiers?

10. **Cleanup Strategy**: Should snapshot cleanup be automatic (background task) or manual (admin triggered)?

11. **Snapshot Format**: Should we support exporting snapshots to portable formats (SQL dump, JSON) for debugging?

12. **Multi-Disk Support**: Should snapshots be stored across multiple disks for redundancy?

13. **Snapshot Metadata**: What additional metadata should we track? (cluster size, node count, storage policy distribution?)

14. **Transfer Optimization**: Should we support parallel chunk transfers for faster snapshot distribution?

15. **Snapshot Versioning**: How should we handle schema changes between snapshot versions? Automatic migration or manual intervention?
