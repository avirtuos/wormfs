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
  1. Get storage policy (k data, m parity shards)
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
metadata operations go through Raft consensus:

#### Chunk State Machine

```
                ┌──────────────┐
                │              │
        stage_chunk()      ┌───▼────┐
                │          │ NULL   │
                │          └───┬────┘
                │              │
                │              │ Initial state (no chunk exists)
                │              │
                ▼              ▼
            ┌─────────────────────┐
            │    STAGED           │
            │  (on disk, not      │
            │   tracked in        │
            │   metadata)         │
            └─────────────────────┘
                    │
                    │ After Raft commits metadata
          ┌─────────┴──────────┐
          │                    │
   activate_chunk()    discard_staged_chunk()
          │                    │
          ▼                    ▼
    ┌──────────┐          ┌─────────┐
    │  ACTIVE  │          │ DELETED │
    │(readable)│          │ (purged)│
    └──────────┘          └─────────┘
```

#### Orphaned Chunk Handling

Staged chunks that are older than 1 hour with no metadata record are considered
orphaned and are cleaned up by StorageWatchdog. This handles scenarios where:
- Client crashes before completing metadata transaction
- Leader crashes before committing metadata
- Network partitions prevent metadata commit

The 1-hour threshold ensures no in-flight transactions are affected by cleanup.

#### Phase 1: Prepare

When `prepare_chunk()` is called by StorageRaftMember (acting on Raft leader's prepare directive):

1. **Validate Request**: Check tx_id, chunk_id, and available disk space
2. **Select Disk**: Choose target disk based on chunk assignment from leader
3. **Write Chunk**: 
   - Write chunk header with state=PREPARING
   - Write chunk data to temporary location
   - Compute and verify checksums
4. **Persist State**:
   - fsync() chunk file to disk
   - Record tx_id → chunk_id mapping in memory
5. **Return Vote**:
   - Vote::Commit if successful (chunk durably stored)
   - Vote::Abort if any failure occurred

**Key Properties**:
- Chunk is written to disk but NOT yet visible to readers
- State is durable (survives crashes)
- Vote response guarantees chunk can be committed or aborted

#### Phase 2a: Commit

When `commit_chunk()` is called (after Raft commits metadata):

1. **Locate Chunk**: Find chunk file by tx_id and chunk_id
2. **Verify State**: Ensure chunk is in PREPARING state
3. **Atomically Update**:
   - Rename chunk from temp location to final location
   - Update chunk header state to ACTIVE
   - fsync() to ensure durability
4. **Cleanup**: Remove tx_id → chunk_id mapping

**Result**: Chunk becomes visible and readable by clients

#### Phase 2b: Abort

When `abort_chunk()` is called (if Raft transaction fails):

1. **Locate Chunk**: Find chunk file by tx_id and chunk_id
2. **Verify State**: Ensure chunk is in PREPARING state
3. **Delete Chunk**:
   - Remove chunk file from disk
   - Free allocated space
4. **Cleanup**: Remove tx_id → chunk_id mapping

**Result**: Chunk is purged as if it never existed

#### Orphan Cleanup

Background task `cleanup_orphaned_chunks()` handles crash recovery:

1. **Scan Disks**: Find all chunks in PREPARING state
2. **Check Age**: Filter chunks older than timeout (e.g., 5 minutes)
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
│  │ Transaction ID (16 bytes, if PREPARING) │
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

1. **Transaction Timeout**: What should be the default timeout for orphaned chunk cleanup? (5 minutes, 10 minutes, 1 hour?)

2. **Prepare Vote Timeout**: How long should the leader wait for prepare votes before aborting? Should it be configurable per operation type?

3. **Chunk Location Persistence**: Should we persist the tx_id → chunk_id mapping to disk for crash recovery, or rely on orphan cleanup?

4. **Temporary File Location**: Should PREPARING chunks be written to a separate temp directory or in-place with a state marker?

5. **Commit Operation Atomicity**: Is file rename sufficient for commit atomicity, or do we need additional guarantees (e.g., journal)?

6. **Partial Stripe Commit**: If some chunks fail to prepare, should we retry with alternate nodes or immediately abort?

7. **Orphan Cleanup Frequency**: How often should the background cleanup task run? (Every minute, every 5 minutes, on-demand?)

8. **Vote Response Strategy**: Should nodes vote Commit optimistically (before fsync) or pessimistically (after fsync)? Trade-off between latency and safety.

### General Storage Questions

9. **Compression Support**: Should we implement compression in the initial version, or defer to future iterations? If yes, which algorithm (lz4, zstd)?

10. **Chunk Size**: Should chunk size be fixed (stripe_size / data_shards) or configurable independently?

11. **Partial Stripe Writes**: How should we handle the last stripe of a file that's smaller than stripe_size? Pad or use variable-sized encoding?

12. **Chunk Caching**: Should FileStore implement a chunk cache for frequently accessed data? What eviction policy?

13. **Disk Balancing**: Should we implement active rebalancing when disk usage becomes imbalanced, or only balance new writes?

14. **Chunk Migration**: Should chunk migration be automatic or require operator approval? What triggers migration?

15. **Verification Frequency**: How often should we verify chunk checksums during normal reads vs. deep verification?

16. **Reconstruction Priority**: When multiple stripes need reconstruction, how should we prioritize? (FIFO, by file importance, by corruption severity?)?

17. **Chunk Deduplication**: Should we implement chunk-level deduplication for identical data blocks?

18. **Storage Efficiency**: Should we optimize storage for small files (< stripe_size) with different encoding strategies?

19. **Disk Hot-Swap**: How should we handle adding/removing disks while the system is running? Graceful migration required?

20. **Chunk Versioning**: Should chunks be versioned to support file modifications, or always create new chunks?

21. **Read Optimization**: Should we read from the "fastest" node/disk based on latency metrics, or always prefer local chunks?

22. **Erasure Algorithm Flexibility**: Should we support multiple erasure algorithms, or strictly Reed-Solomon for simplicity?
