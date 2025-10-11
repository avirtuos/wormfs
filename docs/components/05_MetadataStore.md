# MetadataStore Component Design

## Purpose & Responsibilities

MetadataStore is the SQLite-based metadata persistence layer that stores the materialized state of the filesystem after Raft log entries are applied. Its responsibilities include:

- Storing file metadata (paths, permissions, size, inodes)
- Storing chunk locations (which node/disk has which chunks)
- Storing stripe mappings (stripe → chunks relationship)
- Managing file lock state (active read/write locks)
- Providing fast query access to metadata for filesystem operations
- Supporting transactional updates from Raft state machine
- Creating consistent snapshots for Raft log compaction
- Recovering from snapshots during node startup

## Architecture & Design

### Database Schema

```
┌─────────────────────────────────────────────────────────┐
│              MetadataStore (SQLite)                      │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  Files Table:                                            │
│  ┌─────────────────────────────────────────────────┐   │
│  │ file_id (UUID, PK)                               │   │
│  │ inode (INTEGER)                                  │   │
│  │ path (TEXT)                                      │   │
│  │ parent_path (TEXT)                               │   │
│  │ name (TEXT)                                      │   │
│  │ size (INTEGER)                                   │   │
│  │ permissions (INTEGER)                            │   │
│  │ uid (INTEGER)                                    │   │
│  │ gid (INTEGER)                                    │   │
│  │ created_at (INTEGER)                             │   │
│  │ modified_at (INTEGER)                            │   │
│  │ accessed_at (INTEGER)                            │   │
│  │ storage_policy_id (INTEGER, FK)                  │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  Stripes Table:                                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │ stripe_id (UUID, PK)                             │   │
│  │ file_id (UUID, FK)                               │   │
│  │ stripe_index (INTEGER)                           │   │
│  │ offset (INTEGER)                                 │   │
│  │ size (INTEGER)                                   │   │
│  │ checksum (INTEGER)                               │   │
│  │ created_at (INTEGER)                             │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  Chunks Table:                                           │
│  ┌─────────────────────────────────────────────────┐   │
│  │ chunk_id (UUID, PK)                              │   │
│  │ stripe_id (UUID, FK)                             │   │
│  │ chunk_index (INTEGER)                            │   │
│  │ node_id (TEXT)                                   │   │
│  │ disk_id (TEXT)                                   │   │
│  │ checksum (INTEGER)                               │   │
│  │ status (TEXT)                                    │   │
│  │ created_at (INTEGER)                             │   │
│  │ last_verified (INTEGER)                          │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  Locks Table:                                            │
│  ┌─────────────────────────────────────────────────┐   │
│  │ lock_id (INTEGER, PK)                            │   │
│  │ file_id (UUID, FK)                               │   │
│  │ client_id (TEXT)                                 │   │
│  │ lock_type (TEXT) -- 'read' or 'write'           │   │
│  │ acquired_at (INTEGER)                            │   │
│  │ expires_at (INTEGER)                             │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  StoragePolicies Table:                                  │
│  ┌─────────────────────────────────────────────────┐   │
│  │ policy_id (INTEGER, PK)                          │   │
│  │ name (TEXT)                                      │   │
│  │ data_shards (INTEGER)                            │   │
│  │ parity_shards (INTEGER)                          │   │
│  │ stripe_size (INTEGER)                            │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  Nodes Table:                                            │
│  ┌─────────────────────────────────────────────────┐   │
│  │ node_id (TEXT, PK)                               │   │
│  │ address (TEXT)                                   │   │
│  │ status (TEXT)                                    │   │
│  │ last_seen (INTEGER)                              │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  Disks Table:                                            │
│  ┌─────────────────────────────────────────────────┐   │
│  │ disk_id (TEXT, PK)                               │   │
│  │ node_id (TEXT, FK)                               │   │
│  │ path (TEXT)                                      │   │
│  │ total_space (INTEGER)                            │   │
│  │ free_space (INTEGER)                             │   │
│  │ status (TEXT)                                    │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Indexes

```sql
-- File lookup indexes
CREATE UNIQUE INDEX idx_files_path ON files(path);
CREATE INDEX idx_files_parent ON files(parent_path);
CREATE INDEX idx_files_inode ON files(inode);

-- Stripe lookup indexes
CREATE INDEX idx_stripes_file ON stripes(file_id);
CREATE INDEX idx_stripes_file_index ON stripes(file_id, stripe_index);

-- Chunk lookup indexes
CREATE INDEX idx_chunks_stripe ON chunks(stripe_id);
CREATE INDEX idx_chunks_node ON chunks(node_id);
CREATE INDEX idx_chunks_disk ON chunks(disk_id);
CREATE INDEX idx_chunks_status ON chunks(status);

-- Lock lookup indexes
CREATE INDEX idx_locks_file ON locks(file_id);
CREATE INDEX idx_locks_expires ON locks(expires_at);
CREATE INDEX idx_locks_client ON locks(client_id);

-- Disk lookup indexes
CREATE INDEX idx_disks_node ON disks(node_id);
```

## Interfaces

### Architecture: Client Pattern with Interior Mutability

To support OpenRaft's requirement for exclusive ownership while allowing other components concurrent access, MetadataStore uses a client/server pattern:

```rust
/// Internal implementation with interior mutability
struct MetadataStoreInner {
    conn: RwLock<rusqlite::Connection>,
    config: MetadataStoreConfig,
}

/// Cheap-to-clone client handle
/// Multiple components can hold cloned instances
#[derive(Clone)]
pub struct MetadataStore {
    inner: Arc<MetadataStoreInner>,
}
```

**Key Benefits:**
1. **OpenRaft Compatibility**: OpenRaft can "own" a MetadataStore instance (which clones cheaply)
2. **Concurrent Access**: StorageEndpoint, Watchdog, etc. can hold their own cloned instances
3. **Thread Safety**: Interior mutability via RwLock ensures safe concurrent access
4. **Familiar Pattern**: Similar to hyper::Client, tokio::runtime::Handle, etc.

### Public API

```rust
impl MetadataStore {
    /// Create a new MetadataStore
    /// Returns a cheap-to-clone client handle
    pub fn new(config: MetadataStoreConfig) -> Result<Self, MetadataError>;
    
    /// Initialize database schema
    /// Uses interior mutability for thread-safe access
    pub async fn initialize_schema(&self) -> Result<(), MetadataError>;
    
    // ===== File Operations =====
    
    /// Create a new file entry
    pub async fn create_file(
        &self,
        path: &Path,
        inode: u64,
        metadata: FileMetadata,
    ) -> Result<FileId, MetadataError>;
    
    /// Get file metadata by path
    pub async fn get_file_by_path(&self, path: &Path) -> Result<FileRecord, MetadataError>;
    
    /// Get file metadata by inode
    pub async fn get_file_by_inode(&self, inode: u64) -> Result<FileRecord, MetadataError>;
    
    /// Get file metadata by file ID
    pub async fn get_file(&self, file_id: FileId) -> Result<FileRecord, MetadataError>;
    
    /// Update file metadata
    pub async fn update_file(
        &self,
        file_id: FileId,
        metadata: FileMetadata,
    ) -> Result<(), MetadataError>;
    
    /// Delete a file
    pub async fn delete_file(&self, file_id: FileId) -> Result<(), MetadataError>;
    
    /// List files in a directory
    pub async fn list_directory(&self, path: &Path) -> Result<Vec<FileRecord>, MetadataError>;
    
    // ===== Stripe Operations =====
    
    /// Allocate stripes for a file
    pub async fn allocate_stripes(
        &self,
        file_id: FileId,
        stripes: Vec<StripeRecord>,
    ) -> Result<(), MetadataError>;
    
    /// Get stripe by ID
    pub async fn get_stripe(&self, stripe_id: StripeId) -> Result<StripeRecord, MetadataError>;
    
    /// Get all stripes for a file
    pub async fn get_file_stripes(&self, file_id: FileId) -> Result<Vec<StripeRecord>, MetadataError>;
    
    /// Get stripe at specific offset in file
    pub async fn get_stripe_at_offset(
        &self,
        file_id: FileId,
        offset: u64,
    ) -> Result<StripeRecord, MetadataError>;
    
    // ===== Chunk Operations =====
    
    /// Allocate chunks for a stripe
    pub async fn allocate_chunks(
        &self,
        stripe_id: StripeId,
        chunks: Vec<ChunkRecord>,
    ) -> Result<(), MetadataError>;
    
    /// Get chunk by ID
    pub async fn get_chunk(&self, chunk_id: ChunkId) -> Result<ChunkRecord, MetadataError>;
    
    /// Get all chunks for a stripe
    pub async fn get_stripe_chunks(&self, stripe_id: StripeId) -> Result<Vec<ChunkRecord>, MetadataError>;
    
    /// Update chunk location
    pub async fn update_chunk_location(
        &self,
        chunk_id: ChunkId,
        node_id: NodeId,
        disk_id: DiskId,
    ) -> Result<(), MetadataError>;
    
    /// Mark chunk as corrupt
    pub async fn mark_chunk_corrupt(&self, chunk_id: ChunkId) -> Result<(), MetadataError>;
    
    /// Update chunk verification time
    pub async fn update_chunk_verification(
        &self,
        chunk_id: ChunkId,
        verified_at: SystemTime,
    ) -> Result<(), MetadataError>;
    
    /// Get all chunks on a node
    pub async fn get_node_chunks(&self, node_id: NodeId) -> Result<Vec<ChunkRecord>, MetadataError>;
    
    /// Get all chunks on a disk
    pub async fn get_disk_chunks(&self, disk_id: DiskId) -> Result<Vec<ChunkRecord>, MetadataError>;
    
    // ===== Lock Operations =====
    
    /// Acquire a read lock
    pub async fn acquire_read_lock(
        &self,
        file_id: FileId,
        client_id: ClientId,
        expires_at: SystemTime,
    ) -> Result<LockId, MetadataError>;
    
    /// Acquire a write lock
    pub async fn acquire_write_lock(
        &self,
        file_id: FileId,
        client_id: ClientId,
        expires_at: SystemTime,
    ) -> Result<LockId, MetadataError>;
    
    /// Release a lock
    pub async fn release_lock(&self, file_id: FileId, client_id: ClientId) -> Result<(), MetadataError>;
    
    /// Extend lock expiration
    pub async fn extend_lock(
        &self,
        file_id: FileId,
        client_id: ClientId,
        new_expiry: SystemTime,
    ) -> Result<(), MetadataError>;
    
    /// Get active locks for a file
    pub async fn get_file_locks(&self, file_id: FileId) -> Result<Vec<LockRecord>, MetadataError>;
    
    /// Clean up expired locks
    pub async fn cleanup_expired_locks(&self) -> Result<u64, MetadataError>;
    
    // ===== Node & Disk Operations =====
    
    /// Register a node
    pub async fn register_node(
        &self,
        node_id: NodeId,
        address: SocketAddr,
    ) -> Result<(), MetadataError>;
    
    /// Update node status
    pub async fn update_node_status(
        &self,
        node_id: NodeId,
        status: NodeStatus,
    ) -> Result<(), MetadataError>;
    
    /// Get all nodes
    pub async fn get_nodes(&self) -> Result<Vec<NodeRecord>, MetadataError>;
    
    /// Register a disk
    pub async fn register_disk(
        &self,
        node_id: NodeId,
        disk: DiskRecord,
    ) -> Result<(), MetadataError>;
    
    /// Update disk statistics
    pub async fn update_disk_stats(
        &self,
        disk_id: DiskId,
        total_space: u64,
        free_space: u64,
    ) -> Result<(), MetadataError>;
    
    /// Get all disks for a node
    pub async fn get_node_disks(&self, node_id: NodeId) -> Result<Vec<DiskRecord>, MetadataError>;
    
    /// Get available disks for chunk placement
    pub async fn get_available_disks(&self) -> Result<Vec<DiskRecord>, MetadataError>;
    
    // ===== Snapshot Operations =====
    
    /// Create a consistent snapshot of the database
    pub async fn create_snapshot(&self, snapshot_path: &Path) -> Result<(), MetadataError>;
    
    /// Restore from a snapshot
    pub async fn restore_from_snapshot(&self, snapshot_path: &Path) -> Result<(), MetadataError>;
    
    // ===== Transaction Support =====
    
    /// Begin a transaction
    pub async fn begin_transaction(&self) -> Result<Transaction, MetadataError>;
}

/// Transaction handle for atomic operations
pub struct Transaction<'a> {
    // Wraps SQLite transaction
}
```

### Data Structures

```rust
pub struct MetadataStoreConfig {
    pub db_path: PathBuf,
    pub cache_size_mb: usize,
    pub wal_mode: bool,
    pub sync_mode: SyncMode,
}

#[derive(Debug, Clone, Copy)]
pub enum SyncMode {
    Full,    // Maximum durability
    Normal,  // Balance of safety and performance
    Off,     // Maximum performance (unsafe)
}

#[derive(Debug, Clone)]
pub struct FileRecord {
    pub file_id: FileId,
    pub inode: u64,
    pub path: PathBuf,
    pub parent_path: PathBuf,
    pub name: String,
    pub size: u64,
    pub permissions: u32,
    pub uid: u32,
    pub gid: u32,
    pub created_at: SystemTime,
    pub modified_at: SystemTime,
    pub accessed_at: SystemTime,
    pub storage_policy: StoragePolicy,
}

#[derive(Debug, Clone)]
pub struct StripeRecord {
    pub stripe_id: StripeId,
    pub file_id: FileId,
    pub stripe_index: u32,
    pub offset: u64,
    pub size: u64,
    pub checksum: u32,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ChunkRecord {
    pub chunk_id: ChunkId,
    pub stripe_id: StripeId,
    pub chunk_index: u8,
    pub node_id: NodeId,
    pub disk_id: DiskId,
    pub checksum: u32,
    pub status: ChunkStatus,
    pub created_at: SystemTime,
    pub last_verified: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkStatus {
    Healthy,
    Corrupt,
    Missing,
    Rebuilding,
}

#[derive(Debug, Clone)]
pub struct LockRecord {
    pub lock_id: u64,
    pub file_id: FileId,
    pub client_id: ClientId,
    pub lock_type: LockType,
    pub acquired_at: SystemTime,
    pub expires_at: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockType {
    Read,
    Write,
}

#[derive(Debug, Clone)]
pub struct NodeRecord {
    pub node_id: NodeId,
    pub address: SocketAddr,
    pub status: NodeStatus,
    pub last_seen: SystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Online,
    Offline,
    Failed,
}

#[derive(Debug, Clone)]
pub struct DiskRecord {
    pub disk_id: DiskId,
    pub node_id: NodeId,
    pub path: PathBuf,
    pub total_space: u64,
    pub free_space: u64,
    pub status: DiskStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskStatus {
    Healthy,
    Degraded,
    Failed,
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("Database error: {0}")]
    DatabaseError(String),
    
    #[error("File not found: {0}")]
    FileNotFound(PathBuf),
    
    #[error("File already exists: {0}")]
    FileExists(PathBuf),
    
    #[error("Stripe not found: {0}")]
    StripeNotFound(StripeId),
    
    #[error("Chunk not found: {0}")]
    ChunkNotFound(ChunkId),
    
    #[error("Lock conflict: {0}")]
    LockConflict(String),
    
    #[error("Node not found: {0}")]
    NodeNotFound(NodeId),
    
    #[error("Disk not found: {0}")]
    DiskNotFound(DiskId),
    
    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),
    
    #[error("Transaction error: {0}")]
    TransactionError(String),
}
```

## Dependencies

### Direct Dependencies
None (standalone component)

### External Dependencies
- `rusqlite`: SQLite database interface
- `tokio`: Async runtime (for task spawning)
- `serde`: Serialization for complex types
- `uuid`: ID generation

## Configuration

```toml
[metadata_store]
db_path = "/var/lib/wormfs/metadata.db"
cache_size_mb = 256
wal_mode = true
sync_mode = "normal"  # full, normal, or off

# SQLite pragmas
[metadata_store.pragmas]
journal_mode = "WAL"
synchronous = "NORMAL"
cache_size = -262144  # 256MB
mmap_size = 268435456  # 256MB
temp_store = "memory"
locking_mode = "normal"
```

## Error Handling

### Database Corruption
- Regular integrity checks via `PRAGMA integrity_check`
- Automatic recovery from WAL if possible
- Snapshot restoration if corruption detected
- Alert operators for manual intervention

### Lock Conflicts
- Write lock blocks all other locks
- Read locks allow concurrent reads
- Expired locks automatically cleaned up
- Lock timeout prevents deadlocks

### Constraint Violations
- Foreign key enforcement enabled
- Unique constraint on file paths
- Referential integrity for stripe/chunk relationships
- Transaction rollback on violation

## Testing Strategy

### Unit Tests
- CRUD operations for each entity type
- Lock acquisition and conflict scenarios
- Expired lock cleanup
- Query performance with indexes
- Transaction commit/rollback

### Integration Tests
- Concurrent access from multiple threads
- Snapshot creation and restoration
- Large dataset performance
- Foreign key cascade behavior

### Performance Tests
- Query latency benchmarks
- Index effectiveness
- Write throughput
- Concurrent transaction handling

## Open Questions

1. **Connection Pooling**: Should we use a connection pool for concurrent access, or rely on SQLite's WAL mode with single connection?

2. **Lock Cleanup**: Should expired lock cleanup run on a timer, or be triggered by lock operations?

3. **Cache Size**: What's the optimal cache size for typical workloads? Should it be auto-tuned based on available memory?

4. **Snapshot Format**: Should snapshots be raw SQLite files, or exported to a portable format?

5. **Query Optimization**: Should we use prepared statements for common queries, or rely on SQLite's query cache?

6. **Inode Allocation**: Should MetadataStore be responsible for inode generation, or should it come from the caller?

7. **Soft Deletes**: Should file deletion be soft (mark as deleted) or hard (remove from database)?

8. **Path Normalization**: Should paths be normalized (e.g., resolve "..", "//" ) before storage?

9. **Metadata Versioning**: Should we version metadata schema for future migrations?

10. **Audit Trail**: Should we maintain an audit log of metadata changes for debugging?

11. **Statistics**: Should we maintain aggregated statistics (file count, total size, etc.) or compute on-demand?

12. **Path Depth Limit**: Should we enforce a maximum path depth or length?

13. **Case Sensitivity**: Should file paths be case-sensitive or case-insensitive?

14. **Transaction Isolation**: What isolation level should we use for transactions? (READ COMMITTED, SERIALIZABLE)?

15. **Backup Strategy**: Should MetadataStore support online backups, or require snapshot creation?
