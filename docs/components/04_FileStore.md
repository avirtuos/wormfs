# FileStore Component Design

## Purpose & Responsibilities

FileStore manages the erasure coding, chunk storage, and chunk placement for file data in WormFS. Its responsibilities include:

- Applying Reed-Solomon erasure coding to file stripes
- Coordinating chunk placement across storage nodes and disks
- Handling chunk read and write operations
- Managing chunk verification and integrity checking
- Enforcing storage policies (stripe size, data/parity shards)
- Implementing chunk placement rules (blast radius limitations)
- Providing chunk reconstruction from available shards
- Managing local chunk storage on backing filesystems

## Architecture & Design

### Stripe Processing Pipeline

```
┌─────────────────────────────────────────────────────────┐
│                    FileStore                             │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  Write Pipeline:                                         │
│  ┌─────────────────────────────────────────────────┐   │
│  │  1. Receive stripe data                          │   │
│  │  2. Apply Reed-Solomon encoding                  │   │
│  │  3. Generate data shards (k chunks)              │   │
│  │  4. Generate parity shards (m chunks)            │   │
│  │  5. Calculate checksums (stripe + chunks)        │   │
│  │  6. Select chunk placement (nodes + disks)       │   │
│  │  7. Create chunk headers                         │   │
│  │  8. Write chunks to storage nodes                │   │
│  │  9. Update metadata via Raft                     │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  Read Pipeline:                                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │  1. Query metadata for chunk locations           │   │
│  │  2. Request chunks from storage nodes            │   │
│  │  3. Verify chunk checksums                       │   │
│  │  4. If missing/corrupt: reconstruct from k shards│   │
│  │  5. Apply Reed-Solomon decoding                  │   │
│  │  6. Verify stripe checksum                       │   │
│  │  7. Return stripe data                           │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  Local Chunk Storage:                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │  /data/disks/disk1/                              │   │
│  │    ├── 1/                                        │   │
│  │    │   └── abc123def45/ (chunk folder)          │   │
│  │    │       ├── index.json                        │   │
│  │    │       ├── chunk_0                           │   │
│  │    │       └── chunk_3                           │   │
│  │    ├── 2/                                        │   │
│  │    ...                                            │   │
│  │    └── 1000/                                     │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Chunk Placement Algorithm

```
For each stripe:
  1. Get storage policy (k data, m parity shards). This may come from the request or require lookup in the MetadataStore as this is can be set at the File level (for writes) or Stripe level (for reads while a File is transitioning between StoragePolicies)
  2. Query MetadataStore for available nodes/disks
  3. For each chunk (0 to k+m-1):
     a. Filter disks that already have a chunk from this stripe
     b. Select disk with most free space
     c. Assign chunk to that disk
     d. Record assignment
  4. Verify all chunks assigned successfully
  5. Return chunk placement map
```

### Chunk Staging Workflow

FileStore supports a data-plane-first approach where chunk data is written BEFORE
metadata operations go through Raft consensus though FileStore is mostly unaware of Files or their visibility as it only operates on Stripes and Chunks.

#### Orphaned Chunk Handling

Staged chunks that are older than 1 hour with no metadata record are considered
orphaned and are cleaned up by StorageWatchdog. This handles scenarios where:
- Client crashes before completing metadata transaction
- Leader crashes before committing metadata
- Network partitions prevent metadata commit

The 1-hour threshold ensures no in-flight transactions are affected by cleanup.

#### On Abort

When `abort_chunk()` is called (if Raft transaction fails):

1. **Locate Chunk**: Find chunk file by chunk_id
2. **Delete Chunk**:
   - Remove chunk file from disk
   - Free allocated space

#### Orphan Cleanup

Background task `cleanup_orphaned_chunks()` handles crash recovery:

1. **Scan Disks**: Find all chunks in PREPARING state
2. **Check Age**: Filter chunks older than timeout (e.g.,1 Hour)
3. **Purge Orphans**: Delete stale preparing chunks
4. **Log Results**: Record cleanup statistics for monitoring

**Rationale**: If coordinator crashes after PREPARE but before COMMIT/ABORT, orphaned chunks are eventually cleaned up automatically.


### Chunk File Format

```
┌──────────────────────────────────────────┐
│           Chunk File Layout              │
├──────────────────────────────────────────┤
│                                          │
│  Header (variable length):              │
│  ┌────────────────────────────────────┐ │
│  │ Magic bytes (4 bytes): "WORM"     │ │
│  │ Format version (2 bytes)           │ │
│  │ Header length (2 bytes)            │ │
│  │ Chunk checksum (4 bytes CRC32)     │ │
│  │ Chunk ID (16 bytes UUID)           │ │
│  │ Stripe ID (16 bytes UUID)          │ │
│  │ File ID (16 bytes UUID)            │ │
│  │ Stripe start offset (8 bytes)      │ │
│  │ Stripe end offset (8 bytes)        │ │
│  │ Chunk index (1 byte)               │ │
│  │ Data shards count (1 byte)         │ │
│  │ Parity shards count (1 byte)       │ │
│  │ Erasure algorithm (1 byte)         │ │
│  │ Compression algorithm (1 byte)     │ │
│  │ Stripe checksum (4 bytes CRC32)    │ │
│  │ Chunk state (1 byte)               │ │
│  │ Reserved (variable)                │ │
│  └────────────────────────────────────┘ │
│                                          │
│  Chunk Data:                             │
│  ┌────────────────────────────────────┐ │
│  │ Raw chunk data (variable length)   │ │
│  │ (potentially compressed)            │ │
│  └────────────────────────────────────┘ │
└──────────────────────────────────────────┘
```

## Interfaces

### Public API

```rust
pub struct FileStore {
    config: FileStoreConfig,
    local_disks: Vec<DiskInfo>,
    encoder: Arc<ReedSolomonEncoder>,
    network: Arc<StorageNetwork>,
    metadata_store: Arc<MetadataStore>,
}

impl FileStore {
    /// Create a new FileStore
    pub fn new(
        config: FileStoreConfig,
        network: Arc<StorageNetwork>,
        metadata_store: Arc<MetadataStore>,
    ) -> Result<Self, FileStoreError>;
    
    /// Write a stripe to storage (legacy - prefer transaction-based approach)
    pub async fn write_stripe(
        &self,
        file_id: FileId,
        stripe_id: StripeId,
        data: Vec<u8>,
        policy: StoragePolicy,
    ) -> Result<StripeMetadata, FileStoreError>;
    
    /// Read a stripe from storage
    pub async fn read_stripe(
        &self,
        file_id: FileId,
        stripe_id: StripeId,
    ) -> Result<Vec<u8>, FileStoreError>;
    
    // ===== Two-Phase Commit Transaction Support =====
    
    /// Prepare a chunk locally (Phase 1 of 2PC)
    /// Writes chunk with state="preparing" and fsyncs
    pub async fn prepare_chunk(
        &self,
        tx_id: TxId,
        chunk_data: ChunkData,
    ) -> Result<PrepareVote, FileStoreError>;
    
    /// Commit a prepared chunk (Phase 2 of 2PC)
    /// Changes chunk state from "preparing" to "active"
    pub async fn commit_chunk(
        &self,
        tx_id: TxId,
        chunk_id: ChunkId,
    ) -> Result<(), FileStoreError>;
    
    /// Abort a prepared chunk (Phase 2 of 2PC)
    /// Deletes chunk in "preparing" state
    pub async fn abort_chunk(
        &self,
        tx_id: TxId,
        chunk_id: ChunkId,
    ) -> Result<(), FileStoreError>;
    
    /// Cleanup orphaned preparing chunks (background task)
    pub async fn cleanup_orphaned_chunks(
        &self,
        older_than: SystemTime,
    ) -> Result<u64, FileStoreError>;
    
    // ===== Legacy Direct Chunk Operations =====
    
    /// Write a chunk locally (called by remote nodes)
    pub async fn write_chunk_local(
        &self,
        chunk_id: ChunkId,
        chunk_data: ChunkData,
    ) -> Result<(), FileStoreError>;
    
    /// Read a chunk locally (called by remote nodes)
    pub async fn read_chunk_local(
        &self,
        chunk_id: ChunkId,
    ) -> Result<ChunkData, FileStoreError>;
    
    /// Verify a chunk exists and is readable
    pub async fn check_chunk(
        &self,
        chunk_id: ChunkId,
    ) -> Result<ChunkStatus, FileStoreError>;
    
    /// Verify chunk integrity (checksum validation)
    pub async fn verify_chunk(
        &self,
        chunk_id: ChunkId,
    ) -> Result<VerificationResult, FileStoreError>;
    
    /// Reconstruct and rewrite corrupt/missing chunks for a stripe
    pub async fn rebuild_stripe(
        &self,
        file_id: FileId,
        stripe_id: StripeId,
    ) -> Result<RebuildResult, FileStoreError>;
    
    /// Get local disk statistics
    pub fn get_disk_stats(&self) -> Vec<DiskStats>;
    
    /// Add a new disk to local storage
    pub async fn add_disk(&mut self, path: PathBuf) -> Result<DiskId, FileStoreError>;
    
    /// Remove a disk from local storage
    pub async fn remove_disk(&mut self, disk_id: DiskId) -> Result<(), FileStoreError>;
}
```

### Storage Policy

```rust
pub struct StoragePolicy {
    pub parity_algo: ErasureAlgorithm,
    /// Number of data shards
    pub data_shards: u8,
    /// Number of parity shards
    pub parity_shards: u8,
    /// Stripe size in bytes
    pub stripe_size: u64,
    /// Compression algorithm (None for now)
    pub compression: CompressionAlgorithm,
}

impl StoragePolicy {
    /// Total number of chunks per stripe
    pub fn total_shards(&self) -> u8 {
        self.data_shards + self.parity_shards
    }
    
    /// Minimum shards needed to reconstruct
    pub fn min_shards_for_recovery(&self) -> u8 {
        self.data_shards
    }
    
    /// Maximum tolerable failures
    pub fn max_failures(&self) -> u8 {
        self.parity_shards
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CompressionAlgorithm {
    None,
    // Future: Lz4, Zstd, etc.
}

#[derive(Debug, Clone, Copy)]
pub enum ErasureAlgorithm {
    ReedSolomon,
    None, // For testing or special cases
}
```

### Chunk Data Structures

```rust
pub struct ChunkData {
    pub header: ChunkHeader,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkHeader {
    pub magic: [u8; 4], // "WORM"
    pub format_version: u16,
    pub chunk_checksum: u32,
    pub chunk_id: ChunkId,
    pub stripe_id: StripeId,
    pub file_id: FileId,
    pub stripe_start_offset: u64,
    pub stripe_end_offset: u64,
    pub chunk_index: u8,
    pub data_shards: u8,
    pub parity_shards: u8,
    pub erasure_algorithm: ErasureAlgorithm,
    pub compression_algorithm: CompressionAlgorithm,
    pub stripe_checksum: u32,
}

impl ChunkHeader {
    pub fn serialize(&self) -> Vec<u8>;
    pub fn deserialize(bytes: &[u8]) -> Result<Self, FileStoreError>;
    pub fn compute_checksum(&self, data: &[u8]) -> u32;
}

pub struct StripeMetadata {
    pub stripe_id: StripeId,
    pub file_id: FileId,
    pub offset: u64,
    pub size: u64,
    pub checksum: u32,
    pub chunks: Vec<ChunkLocation>,
}

pub struct ChunkLocation {
    pub chunk_id: ChunkId,
    pub node_id: NodeId,
    pub disk_id: DiskId,
    pub chunk_index: u8,
}
```

### Disk Management

```rust
pub struct DiskInfo {
    pub disk_id: DiskId,
    pub path: PathBuf,
    pub total_space: u64,
    pub free_space: u64,
    pub status: DiskStatus,
}

pub struct DiskStats {
    pub disk_id: DiskId,
    pub path: PathBuf,
    pub total_space: u64,
    pub free_space: u64,
    pub used_space: u64,
    pub chunk_count: u64,
    pub file_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskStatus {
    Healthy,
    Degraded,
    Failed,
    Removed,
}

pub struct ChunkFolder {
    pub path: PathBuf,
    pub file_id: FileId,
    pub index: ChunkFolderIndex,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChunkFolderIndex {
    pub file_id: FileId,
    pub file_path: PathBuf,
    pub chunk_count: u64,
    pub total_size: u64,
    pub created_at: SystemTime,
    pub last_modified: SystemTime,
}
```

### Erasure Coding

```rust
pub struct ReedSolomonEncoder {
    // Wraps reed-solomon-erasure crate
}

impl ReedSolomonEncoder {
    pub fn new() -> Self;
    
    /// Encode data into k data shards + m parity shards
    pub fn encode(
        &self,
        data: &[u8],
        data_shards: usize,
        parity_shards: usize,
    ) -> Result<Vec<Vec<u8>>, EncodingError>;
    
    /// Decode data from available shards (need at least k shards)
    pub fn decode(
        &self,
        shards: Vec<Option<Vec<u8>>>,
        data_shards: usize,
        parity_shards: usize,
    ) -> Result<Vec<u8>, EncodingError>;
    
    /// Verify shard integrity
    pub fn verify(
        &self,
        shards: &[Option<Vec<u8>>],
        data_shards: usize,
        parity_shards: usize,
    ) -> bool;
}
```

## Dependencies

### Direct Dependencies
- **StorageNetwork**: For remote chunk read/write operations
- **MetadataStore**: For querying chunk locations and updating chunk metadata
- **StorageRaftMember**: For proposing chunk allocation operations (indirect)

### External Dependencies
- `reed-solomon-erasure`: Reed-Solomon erasure coding implementation
- `crc32fast`: CRC32 checksum computation
- `tokio`: Async I/O for file operations
- `serde`: Chunk header serialization
- `uuid`: Chunk/stripe/file ID generation

## Data Structures

```rust
pub struct FileStoreConfig {
    /// Local disk paths
    pub disk_paths: Vec<PathBuf>,
    
    /// Default storage policy
    pub default_policy: StoragePolicy,
    
    /// Chunk write timeout
    pub chunk_write_timeout: Duration,
    
    /// Chunk read timeout
    pub chunk_read_timeout: Duration,
    
    /// Maximum concurrent chunk operations
    pub max_concurrent_operations: usize,
    
    /// Chunk folder hash buckets
    pub hash_buckets: u16, // Default: 1000
}

#[derive(Debug, thiserror::Error)]
pub enum FileStoreError {
    #[error("Encoding error: {0}")]
    EncodingError(String),
    
    #[error("Insufficient shards: need {needed}, have {available}")]
    InsufficientShards { needed: usize, available: usize },
    
    #[error("Chunk not found: {0}")]
    ChunkNotFound(ChunkId),
    
    #[error("Chunk corrupted: {0}")]
    ChunkCorrupted(ChunkId),
    
    #[error("Disk error: {0}")]
    DiskError(String),
    
    #[error("No available disk for chunk placement")]
    NoDiskAvailable,
    
    #[error("Network error: {0}")]
    NetworkError(String),
    
    #[error("Metadata error: {0}")]
    MetadataError(String),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

pub enum ChunkStatus {
    Present,
    Missing,
    Corrupted,
}

pub struct VerificationResult {
    pub chunk_id: ChunkId,
    pub status: ChunkStatus,
    pub checksum_valid: bool,
}

pub struct RebuildResult {
    pub stripe_id: StripeId,
    pub chunks_rebuilt: Vec<ChunkId>,
    pub chunks_verified: Vec<ChunkId>,
}

/// Two-Phase Commit vote result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareVote {
    pub tx_id: TxId,
    pub chunk_id: ChunkId,
    pub vote: Vote,
    pub disk_id: DiskId,
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Vote {
    Commit,  // Chunk successfully prepared
    Abort,   // Preparation failed
}

/// Chunk state for transaction management
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkState {
    Preparing,  // Written but not yet committed
    Active,     // Committed and readable
    Deleted,    // Marked for deletion
}
```

## Configuration

```toml
[filestore]
# Local disk paths
disk_paths = [
    "/data/disks/disk1",
    "/data/disks/disk2",
    "/data/disks/disk3"
]

# Default storage policy
[filestore.default_policy]
data_shards = 6
parity_shards = 3
stripe_size_mb = 1
compression = "none"

# Performance tuning
chunk_write_timeout_secs = 30
chunk_read_timeout_secs = 10
max_concurrent_operations = 100

# Storage organization
hash_buckets = 1000
```

## Error Handling

### Chunk Write Failures
- Retry on transient network errors (up to 3 attempts)
- If node unavailable: select alternate node and retry
- If disk full: select alternate disk and retry
- All failures logged and reported to monitoring

### Chunk Read Failures
- If chunk missing/corrupt: attempt reconstruction from other shards
- If reconstruction fails: return error to client
- Log all reconstruction events for analysis

### Reconstruction Failures
- If insufficient shards available: mark stripe as unrecoverable
- Trigger alert for operator intervention
- Log detailed reconstruction attempt information

### Disk Failures
- Mark disk as failed in metadata
- Trigger chunk migration to healthy disks
- Prevent new chunk allocation to failed disk

## Testing Strategy

### Unit Tests
- Reed-Solomon encoding/decoding correctness
- Chunk header serialization/deserialization
- Checksum computation and validation
- Chunk placement algorithm logic
- Chunk folder path generation

### Integration Tests
- Write and read stripe with various policies
- Stripe reconstruction with missing chunks
- Stripe reconstruction with corrupt chunks
- Multi-disk chunk placement
- Concurrent stripe operations

### Performance Tests
- Encoding/decoding throughput
- Chunk write latency
- Chunk read latency
- Reconstruction performance
- Concurrent operation scalability

### Failure Tests
- Disk failure during write
- Network failure during chunk transfer
- Corrupt chunk detection and recovery
- Insufficient shards scenario

## Open Questions

### Two-Phase Commit Protocol

1. **Transaction Timeout**: What should be the default timeout for orphaned chunk cleanup? (5 minutes, 10 minutes, 1 hour?) Answer: 1 hour

2. **Chunk Location Persistence**: Should we persist the tx_id → chunk_id mapping to disk for crash recovery, or rely on orphan cleanup? Answer: We do not need to keep track of a mapping and can instead rely on orpha clean up. In general, this is something that is tracked by the StorageRaftNode and TransactionLogStore if we want to do a better job of active clean up or carry out other processes that require knowledge of Chunks in a TX.

4. **Temporary File Location**: Should PREPARING chunks be written to a separate temp directory or in-place with a state marker? Answer: No, staged chunks can be written directly to their final location since StorageMetadata gates the visibility of these items.

5. **Commit Operation Atomicity**: Is file rename sufficient for commit atomicity, or do we need additional guarantees (e.g., journal)? Answer: We should not be renaming or moving files once their are staged. They should be stage in their final location as specified by the StoreChunk request.

6. **Partial Stripe Commit**: If some chunks fail to prepare, should we retry with alternate nodes or immediately abort? Answer: We should retry with alternate nodes until we either successfully stage all chunks, run out of nodes to try on (and thus are unable to meet the minimum requirements of the StoragePolicy), or we've exhausted all retries but have met the minimum requirements of the StoragePolicy (and can reply on background storage anti-entropy mechanism to replicate additional chunks)

7. **Orphan Cleanup Frequency**: How often should the background cleanup task run? (Every minute, every 5 minutes, on-demand?) Answer: This process should run once an hour.

8. **Vote Response Strategy**: Should nodes vote Commit optimistically (before fsync) or pessimistically (after fsync)? Trade-off between latency and safety. Answer: We do not need to block on any explicit fsync.

### General Storage Questions

9. **Compression Support**: Should we implement compression in the initial version, or defer to future iterations? If yes, which algorithm (lz4, zstd)? Answer: We can differ chunk compression to a future version

10. **Chunk Size**: Should chunk size be fixed (stripe_size / data_shards) or configurable independently? Answer: We should allow configuring of Chunk size only and allow Strip size to be a function of Chunk size and StoragePolicy (e.g. data + parity shard counts). We can start with a Chunk size of 1MB.

11. **Partial Stripe Writes**: How should we handle the last stripe of a file that's smaller than stripe_size? Pad or use variable-sized encoding? Answer: We should reduce the Chunk size such that we do not require more than 10% padding. This may require storing additional metadata in the ChunkHeader.

12. **Chunk Caching**: Should FileStore implement a chunk cache for frequently accessed data? What eviction policy? Answer: No Chunk caching is needed at this time. We may add it in a future release.

13. **Disk Balancing**: Should we implement active rebalancing when disk usage becomes imbalanced, or only balance new writes? Answer: Yes, this is one of the responsibilities we expect to be added to the StorageWatchdog's background anti-entropy and storage optimization processes.

14. **Chunk Migration**: Should chunk migration be automatic or require operator approval? What triggers migration? Answer: chunk migration should be automatic without operator intervention. The triggers will largely originate from the StorageWatchdog's background anti-entropy and optimization processes.

15. **Verification Frequency**: How often should we verify chunk checksums during normal reads vs. deep verification? Answer: We should have a configuration that controls the probability that we validate checksums as part of a normal read operation where as deep verification always validates checksums.

16. **Reconstruction Priority**: When multiple stripes need reconstruction, how should we prioritize? (FIFO, by file importance, by corruption severity?)? Answer: We should prioritize by corruption severity with Stripes that are missing more chunks having higher priority.

17. **Chunk Deduplication**: Should we implement chunk-level deduplication for identical data blocks? Answer: Not at this time. This is something we may add to the StorageWatchdog in the future.

18. **Storage Efficiency**: Should we optimize storage for small files (< stripe_size) with different encoding strategies? Answer: For now the only optimization of this type we want to include is the use of variable chunk size to avoid the need to use padding that is more than 10% of the Chunk size.

19. **Disk Hot-Swap**: How should we handle adding/removing disks while the system is running? Graceful migration required? Answer: We can not force graceful migration because some cases, such as disk failure, are not predictable but we should support an optional graceful migration where an operator can supply a setting in a disk's configuration that informs the system a disk is schedule for removal. The StorageWatchdog should see that setting and request ChunkMigration vis FileSystemService which will work with StorageRaftMember, FileStore, and StorageEndpoint to migrate affected Chunks to different disks and possibly different nodes.

20. **Chunk Versioning**: Should chunks be versioned to support file modifications, or always create new chunks? Answer: Chunks should be immutable so any modification means new chunks with new IDs. The same is true for Stripes. Files and Directories are the only mutable entities because they are purely metadata.

21. **Read Optimization**: Should we read from the "fastest" node/disk based on latency metrics, or always prefer local chunks? Answer: We do not need such optimizations at this time.

22. **Erasure Algorithm Flexibility**: Should we support multiple erasure algorithms, or strictly Reed-Solomon for simplicity? Answer: Only reed-solomon and None are required at this time.
