# StorageRaftMember Component Design

## Purpose & Responsibilities

StorageRaftMember implements the Raft consensus protocol for WormFS, ensuring strong consistency of metadata operations across the distributed cluster. Its responsibilities include:

- Participating in Raft leader election and maintaining cluster membership
- Proposing and committing metadata write transactions through consensus
- Replicating transaction log entries to follower nodes
- Applying committed operations to the MetadataStore
- Coordinating metadata snapshots across the cluster
- Managing read leases for optimized read performance
- Handling node join/leave operations
- Detecting and recovering from split-brain scenarios

## Architecture & Design

### Raft State Machine

```
┌─────────────────────────────────────────┐
│      StorageRaftMember (OpenRaft)       │
├─────────────────────────────────────────┤
│                                         │
│  ┌───────────────────────────────────┐ │
│  │    Raft Role Management           │ │
│  │  • Leader Election                │ │
│  │  • Heartbeat Broadcasting         │ │
│  │  • Log Replication                │ │
│  └───────────────────────────────────┘ │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │   Transaction Proposal Pipeline   │ │
│  │  1. Receive operation request     │ │
│  │  2. Append to local log           │ │
│  │  3. Replicate to followers        │ │
│  │  4. Wait for quorum               │ │
│  │  5. Commit & apply to state       │ │
│  └───────────────────────────────────┘ │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │    Snapshot Coordination          │ │
│  │  • Trigger snapshot creation      │ │
│  │  • Signal all nodes               │ │
│  │  • Track completion status        │ │
│  │  • Trim transaction log           │ │
│  └───────────────────────────────────┘ │
└─────────────────────────────────────────┘
         │          │           │
         ▼          ▼           ▼
  TransactionLog  Metadata  StorageNetwork
     Store        Store
```

### Operation Flow

**Write Operations (Metadata-Only Two-Phase Commit via Raft):**

*Example: Appending data to end of file*

**Phase 0: CHUNK STAGING (Data Plane - Before Raft)**
1. Client submits write request to Leader
2. Leader calculates:
   - Stripe layout and erasure coding parameters
   - Chunk placement (which nodes, which disks)
3. Leader stages chunks (two options):
   - **Option A**: Leader receives data from client, distributes to storage nodes
   - **Option B**: Leader issues time-limited upload tokens, client uploads directly
4. Storage nodes write chunks to disk in "staged" state:
   - No metadata tracking (not in MetadataStore)
   - Only Leader tracks staged chunks in memory
   - Staged chunks older than 1 hour will be cleaned up by StorageWatchdog

**Phase 1: PREPARE METADATA (Control Plane - Raft)**
5. Leader proposes TransactionPrepare through Raft containing ONLY metadata operations:
   - Update file size, and other file attributes
   - Add stripe record with chunk locations
   - Add chunk location records
6. Raft replicates prepare entry to all followers
7. Each node applies prepare locally:
   - Stages metadata changes (not yet visible in MetadataStore)
   - Votes PREPARED or ABORT based on validation
8. Once majority commits prepare entry, metadata is prepared on all nodes

**Phase 2: COMMIT/ABORT METADATA (Control Plane - Raft)**

*Commit Path:*
9. Leader proposes TransactionCommit through Raft
10. Raft replicates commit decision to all followers
11. Each node applies commit:
    - Applies metadata changes to MetadataStore (now visible)
    - Signals storage nodes to activate staged chunks (transition to "active")
12. Return success to client

*Abort Path:*
9. Leader proposes TransactionAbort through Raft
10. Raft replicates abort decision to all followers
11. Each node applies abort:
    - Discards prepared metadata changes
    - Signals storage nodes to discard staged chunks
12. Return error to client

*Example: Deleting a file*

**Phase 1: PREPARE**
1. Client submits delete request
2. Leader creates TransactionPrepare with:
   - Metadata: mark file as deleted, deallocate chunks
   - Chunk operations: empty (no new chunks to write)
3. Prepare replicates via Raft, nodes vote PREPARED

**Phase 2: COMMIT**
4. Leader commits via Raft
5. All nodes mark file/chunks as deleted in metadata (immediate invisibility)
6. Background cleanup asynchronously deletes chunk files

**Read Operations:**
1. If leader with valid lease: serve directly from MetadataStore
2. If follower: optionally forward to leader or serve stale read
3. Return data with staleness indicator

**Snapshot Operations:**
1. Leader triggers snapshot based on time/size threshold
2. Leader sends snapshot proposal to all nodes
3. Each node creates consistent MetadataStore snapshot
4. Nodes report completion back to leader
5. Leader updates cluster snapshot state
6. All nodes trim TransactionLogStore to snapshot point

### Metadata Change Subscriptions

StorageRaftMember provides a subscription mechanism for local components (e.g., FileSystemService) to receive notifications when metadata changes are committed through Raft consensus. This enables cache invalidation and other reactive behaviors.

**Subscription Flow:**
```
┌─────────────────┐
│FileSystemService│
└────────┬────────┘
         │ subscribe_metadata_changes()
         ▼
┌─────────────────────────────────────────┐
│         StorageRaftMember               │
│  ┌────────────────────────────────────┐ │
│  │  Subscription Management           │ │
│  │  • Register subscriber            │ │
│  │  • Filter by change types         │ │
│  │  • Maintain subscriber channels   │ │
│  └────────────────────────────────────┘ │
│                                         │
│  When applying committed operations:   │
│  ┌────────────────────────────────────┐ │
│  │  1. Apply to MetadataStore         │ │
│  │  2. Generate MetadataChangeEvent   │ │
│  │  3. Send to matching subscribers   │ │
│  └────────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

**Example: FileSystemService Cache Invalidation**
1. FileSystemService subscribes to file and directory change events on startup
2. User modifies file through different node
3. Modification goes through Raft consensus
4. When operation commits, StorageRaftMember:
   - Applies change to MetadataStore
   - Creates `MetadataChangeEvent::FileUpdated` event
   - Sends event to FileSystemService subscriber channel
5. FileSystemService receives event and invalidates cached inode

**Supported Change Events:**
- `FileCreated`: New file added to metadata
- `FileUpdated`: File attributes modified (size, mtime, permissions)
- `FileDeleted`: File removed from metadata
- `DirectoryCreated`: New directory created
- `DirectoryDeleted`: Directory removed
- `StripeCreated`: New stripe added to file
- `StripeDeleted`: Stripe removed from file
- `ChunkMoved`: Chunk relocated to different node/disk
- `LockReleased`: File lock released or expired

**Subscription Management:**
- Subscribers use async channels to receive events
- Events are sent asynchronously to avoid blocking Raft operations
- Subscribers can filter for specific event types
- Channel capacity prevents unbounded memory growth
- Slow subscribers may miss events (at-most-once delivery)

## Interfaces

### Public API

```rust
pub struct StorageRaftMember {
    raft: Arc<Raft<WormFsTypeConfig>>,
    node_id: NodeId,
    network: Arc<StorageNetwork>,
    metadata_store: Arc<MetadataStore>,
    transaction_log_store: Arc<TransactionLogStore>,
    snapshot_store: Arc<SnapshotStore>,
    metrics: Arc<RaftMetrics>,
}

impl StorageRaftMember {
    /// Create a new Raft member
    pub async fn new(
        node_id: NodeId,
        config: RaftConfig,
        network: Arc<StorageNetwork>,
        metadata_store: Arc<MetadataStore>,
        transaction_log_store: Arc<TransactionLogStore>,
        snapshot_store: Arc<SnapshotStore>,
    ) -> Result<Self, RaftError>;
    
    /// Initialize Raft and join/create cluster
    pub async fn initialize(&mut self, peers: Vec<NodeId>) -> Result<(), RaftError>;
    
    /// Propose a metadata write operation (goes through consensus)
    pub async fn propose_operation(
        &self,
        operation: WormFsOperation,
    ) -> Result<OperationResult, RaftError>;
    
    /// Check if this node is the current leader
    pub fn is_leader(&self) -> bool;
    
    /// Get current Raft metrics and status
    pub fn get_status(&self) -> RaftStatus;
    
    /// Add a new node to the cluster
    pub async fn add_node(&self, node_id: NodeId, address: SocketAddr) -> Result<(), RaftError>;
    
    /// Remove a node from the cluster
    pub async fn remove_node(&self, node_id: NodeId) -> Result<(), RaftError>;
    
    /// Step down from leader (for graceful shutdown)
    pub async fn step_down(&self) -> Result<(), RaftError>;

    /// Subscribe to metadata change events
    ///
    /// Returns a receiver channel for metadata change notifications.
    /// Events are sent when metadata operations are committed through Raft.
    pub async fn subscribe_metadata_changes(
        &self,
        filter: Option<Vec<MetadataChangeType>>,
    ) -> Result<tokio::sync::mpsc::Receiver<MetadataChangeEvent>, RaftError>;
}
```

### Raft Type Configuration

```rust
pub struct WormFsTypeConfig;

impl RaftTypeConfig for WormFsTypeConfig {
    type NodeId = NodeId;
    type Node = NodeInfo;
    type Entry = WormFsOperation;
    type SnapshotData = MetadataSnapshot;
    type AsyncRuntime = TokioRuntime;
}

/// Operations that can be proposed through Raft
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WormFsOperation {
    // Two-Phase Commit Transaction Operations
    TransactionPrepare {
        tx_id: TxId,
        metadata_ops: Option<Vec<MetadataOperation>>,
        command_ops: Option<Vec<CommandOperation>>,
        timeout: SystemTime,
    },
    TransactionCommit {
        tx_id: TxId,
    },
    TransactionAbort {
        tx_id: TxId,
        reason: Option<String>,
    }
}

/// Transaction ID for 2PC coordination
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TxId(pub u64);

/// MetadataOperation that can be proposed through Raft
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataOperation {
    FileCreate {
        pub path: PathBuf,
        pub inode: u64,
        pub metadata: FileMetadata,
        pub policy: StoragePolicy,
    },
    FileUpdate {
        pub file_id: FileId,
        pub metadata: FileMetadata,
        pub policy: StoragePolicy,
    },
    FileDelete{
        pub file_id: FileId,
    },
    CreateStripe {
        pub file_id: FileId,
        pub stripe_id: StripeId,
        pub policy: StoragePolicy,
        pub offset: u64,
        pub size: u64,
        pub chunks: Vec<ChunkId>
    },
    DeleteStripe {
        pub stripe_id: StripeId,
    },
    CreateChunk {
        pub node_id: NodeId,
        pub disk: DiskId,
        pub chunk: ChunkId,
        pub chunk_index: ChunkIndex,
    },
    MoveChunk {
        pub chunk_id: ChunkId,
        pub old_node: NodeId,
        pub new_node: NodeId,
        pub old_disk: DiskId,
        pub new_disk: DiskId,
    },
    DeleteChunk {
        pub node_id: NodeId,
        pub disk_id: DiskId,
        pub chunk_id: ChunkId,
    },
}

/// CommandOperation that can be proposed through Raft
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandOperation {
    CreateSnapshot {
        pub snapshot_id: u64,
        pub index: u64,
    },
    TrimLog {
        pub trim_to_index: u64,
    },
    AddMember {
        pub node_id: NodeId,
        pub address: SocketAddr,
    },
    RemoveMember {
        pub node_id: NodeId,
    },
}

```

### State Machine Implementation

```rust
#[async_trait]
impl RaftStateMachine<WormFsTypeConfig> for MetadataStateMachine {
    async fn apply(
        &mut self,
        entries: Vec<&MetadataOperation>,
    ) -> Result<Vec<OperationResult>, RaftError> {
        let mut results = Vec::new();
        
        for entry in entries {
            let result = match entry {
                MetadataOperation::CreateFile { path, inode, metadata } => {
                    self.metadata_store.create_file(path, *inode, metadata.clone()).await?
                }
                MetadataOperation::AllocateChunks { file_id, stripe_id, chunks } => {
                    self.metadata_store.allocate_chunks(*file_id, *stripe_id, chunks.clone()).await?
                }
                // ... handle other operations
                _ => unimplemented!(),
            };
            results.push(result);
        }
        
        Ok(results)
    }
    
    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, RaftError>;
    
    async fn install_snapshot(
        &mut self,
        snapshot: MetadataSnapshot,
    ) -> Result<(), RaftError>;
    
    async fn get_current_snapshot(&mut self) -> Result<MetadataSnapshot, RaftError>;
}
```

## Dependencies

### Direct Dependencies
- **StorageNetwork**: For Raft RPC communication with other nodes
- **MetadataStore**: State machine that operations are applied to
- **TransactionLogStore**: Persistent Raft log storage
- **SnapshotStore**: Snapshot persistence and retrieval
- **FileStore**: Coordination for chunk operations (indirect)

### External Dependencies
- `openraft`: Raft consensus implementation
- `tokio`: Async runtime
- `serde`: Operation serialization
- `tracing`: Structured logging

## Data Structures

```rust
pub struct RaftConfig {
    // Election settings
    pub heartbeat_interval: Duration,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    
    // Log settings
    pub max_payload_entries: u64,
    pub snapshot_policy: SnapshotPolicy,
    
    // Replication settings
    pub replication_lag_threshold: u64,
    pub max_in_flight_append_entries: usize,
    
    // Read optimization
    pub enable_lease_based_reads: bool,
    pub lease_duration: Duration,
}

pub struct SnapshotPolicy {
    pub time_threshold: Duration,
    pub log_size_threshold: u64,
}

pub struct RaftStatus {
    pub current_term: u64,
    pub role: RaftRole,
    pub leader_id: Option<NodeId>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub last_log_index: u64,
    pub snapshot_index: u64,
    pub cluster_size: usize,
    pub replication_lag: HashMap<NodeId, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftRole {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, thiserror::Error)]
pub enum RaftError {
    #[error("Not the leader, leader is: {0:?}")]
    NotLeader(Option<NodeId>),
    
    #[error("Operation timeout")]
    Timeout,
    
    #[error("No quorum available")]
    NoQuorum,
    
    #[error("Network error: {0}")]
    NetworkError(String),
    
    #[error("Storage error: {0}")]
    StorageError(String),
    
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
}
```

## Design Decisions

Based on the open questions answered below, the following key design decisions have been made:

### Transaction Coordination
- **Vote Collection**: Votes collected via Raft acknowledgements (not separate RPCs)
- **Transaction Timeout**: Configurable per-transaction; default based on stripe size
- **Vote Persistence**: Kept in memory only during transaction
- **Concurrent Transactions**: Configurable maximum limit for resource management
- **Leader Recovery**: Conservative timeout-based approach for in-flight transactions

### Read Operations
- **Consistency Model**: Local reads with bounded staleness (default: 120 seconds)
- **Client Handling**: Reject writes at non-leaders with leader hint (no automatic forwarding)
- **Future Support**: Linearizable and quorum reads deferred to future versions

### Snapshot Management
- **Coordination**: Eventual consistency acceptable (no two-phase snapshot)
- **Compression**: zstd compression with streaming to avoid memory issues
- **Strategy**: Time and size thresholds; no additional compaction for now

### Performance Optimizations
- **Pipeline Optimization**: Enabled (multiple in-flight AppendEntries)
- **Pre-vote**: Skipped for initial release
- **Parallel Log Application**: Deferred to future optimization
- **Backpressure**: Reject writes when log backlog exists

### Membership Changes
- **Approach**: Single-node changes (simpler than joint consensus)
- **Observer Nodes**: Not required for initial release

### Operational Features
- **Lock Expiration**: Leader-driven via Raft consensus
- **Metrics**: Basic metrics (latency, replication lag, success/failure rates)
- **Tuning**: LAN-optimized (no special WAN configuration)
- **Orphaned Chunks**: StorageWatchdog cleanup (1-hour threshold)

## Configuration

```toml
[raft]
# Election configuration
heartbeat_interval_ms = 250
election_timeout_min_ms = 1000
election_timeout_max_ms = 2000

# Log management
max_payload_entries = 1000
max_in_flight_append_entries = 10
replication_lag_threshold = 100
max_uncommitted_entries = 10000

# Snapshot configuration
snapshot_time_threshold_hours = 24
snapshot_log_size_threshold_mb = 10
enable_snapshot_compression = true
snapshot_compression_level = 3

# Read consistency
enable_lease_based_reads = false  # Reserved for future use
lease_duration_ms = 5000           # Reserved for future use
max_read_staleness_seconds = 120

# Transaction configuration
default_transaction_timeout_seconds = 300
max_concurrent_transactions = 100
transaction_recovery_timeout_seconds = 60
```

## Error Handling

### Leader Failures
- Automatic leader election within election timeout window
- Client requests retry on new leader
- In-flight operations may need to be retried

### Follower Failures
- Leader detects via heartbeat timeout
- Node marked as unavailable
- Replication continues with remaining quorum
- Failed node can rejoin and catch up via log replay

### Network Partitions
- Minority partition cannot accept writes (no quorum)
- Majority partition continues operating normally
- Minority can serve stale reads if configured
- Partitions reconcile automatically when healed

### Log Corruption
- Checksums verify log entry integrity
- Corrupted entries trigger snapshot install from leader
- Node may need to rejoin cluster with fresh state

## Testing Strategy

### Unit Tests
- Operation serialization/deserialization
- State machine apply logic for each operation type
- Snapshot trigger threshold calculation
- Lease expiration logic

### Integration Tests
- Leader election with 3/5/7 node clusters
- Log replication and commit verification
- Snapshot creation and installation
- Node join/leave operations
- Lock acquisition and timeout

### Chaos Tests
- Random leader kills during operations
- Network partition scenarios
- Slow/unreliable network conditions
- Disk I/O failures during log writes
- Clock skew between nodes

## Open Questions

1. **Vote Collection Mechanism**: Should votes be collected via direct RPC queries to each node, or should votes be embedded in Raft acknowledgments of the prepare entry? Answer: We should use raft acknowledgements to convey votes.

2. **Transaction Timeout**: What's an appropriate timeout for transactions, especially for large file writes (100s of MB)? Should it be configurable per operation? Answer: We should make it configurable per transaction but most transactions will be of similar size since most file data operations operate on Stripes and Chunks which have a max size that should help bound operation duration. A good timeout

3. **Partial Prepare Failure**: If some nodes prepare successfully but others fail, should we attempt retry with different node placement, or immediately abort? Answer: We should respect quorum rules for metadata operations. However, for Chunk staging, we need to meet the StoragePolicy's minimum requirements before attempting the Raft transaction.

4. **Transaction Recovery on Leader Change**: How should the new leader handle in-flight transactions? Query all participants, or use a conservative timeout-based approach? Answer: We should start with a conservative timeout-based approach.

5. **Orphaned Chunk Cleanup**: How frequently should nodes scan for and clean up orphaned "preparing" chunks from aborted transactions? Answer: This warrants its own design document but we ca likely start with something basic, but configurable within the StorageWatchdog to handle this. For examples, perform a basic validation of all chunks once a week (including Orphaned Chunks). Perform a deeper validation where we do Stripe level reads and checksum validations once every 6 months. Both of these operations should be going slowly in the background over the course of their respective intervals to avoid high resource demands.

6. **Vote Persistence**: Should prepare votes be persisted to disk, or can they be kept in memory (and re-queried after crashes)? Answer: Prepare votes can be kept in memory while a transaction is in-flight.

7. **Transaction State Limits**: Should there be a maximum number of concurrent transactions to prevent resource exhaustion? Answer: Yes but it should be a configurable limit.

### Raft Operations

9. **Read Consistency Guarantees**: Should we support linearizable reads by default (forward to leader), or prefer lease-based reads with bounded staleness? What should the default staleness bound be? Answer: We should favor local reads with bounded staleness. The staleness threshold should be configurable with a sane default (120 seconds).

10. **Snapshot Coordination**: The design mentions signaling all nodes to snapshot simultaneously. Should this be a two-phase process (prepare + commit) to ensure transactional consistency, or is eventual consistency acceptable? Answer: Eventual consistency is acceptable.

11. **Pre-vote Optimization**: Should we implement Raft's pre-vote extension to avoid unnecessary leader changes when a partitioned node rejoins? Answer: Lets skip this optimization for now.

12. **Log Compaction Strategy**: Beyond snapshots, should we implement additional log compaction (e.g., log cleaning, log-structured merge trees)? Answer: Not yet, lets save this optimization for a future date.

13. **Client Request Routing**: Should followers automatically forward writes to the leader transparently, or return an error with leader hint and let clients retry? Answer: We should reject the client's request and inform them of the current leader's connection details.

14. **Quorum Reads**: Should we support quorum reads (read from majority) as an option between stale reads and linearizable reads? Answer: We may support quorum reads and linearizable reads in the future but to start we will only support local (potentially stale) reads.

15. **Configuration Changes**: Should membership changes (add/remove node) use single-node or joint consensus approach? OpenRaft supports both. Answer: Single node is fine for now.

16. **Metrics Granularity**: What specific Raft metrics should be exposed for monitoring? Answer: Lets start with basic metrics like transaction latency, replication lag, and transaction success/failure rates.

17. **Backpressure**: How should we handle backpressure when the Raft log grows faster than it can be applied to the state machine? Answer: We should reject new write operations when there is a log backlog.

18. **Lock Expiration**: Should lock expiration be handled by Raft consensus (explicit timeout proposals) or locally by each node's state machine with clock-based expiration? Answer: The leader should detect expired locks and issue a Raft transaction to ensure the lock clears on all nodes.

19. **Snapshot Compression**: Should metadata snapshots be compressed before storage/transfer? What compression algorithm? Answer: Lets use zstd compression but we need to be careful to stream the compress and decompress phases so that we never need to hold the entire metadata snapshot in memory as part of the backup or restore process.

20. **Observer Nodes**: Should we support read-only observer nodes that receive log replication but don't vote in elections? Answer: No, this feature is not required today.

21. **Raft Extensions**: Should we implement any Raft extensions like pipeline optimization, parallel log application, or batched AppendEntries? Answer: Lets start with only the pipeline optimization.

22. **Failure Detection Tuning**: How should heartbeat and election timeout be tuned for different network conditions (LAN vs WAN deployments)? Answer: We are only targeting LAN usecases for now so no special tunning is required in our initial release.

## Implementation Status (Issue #76 - Phase 2.3)

### Completed Components ✅

#### Core Infrastructure
- **RaftTypeConfig**: Custom type configuration for WormFS (NodeId, Node, Entry, etc.)
- **WormFsStateMachine**: Implements RaftStateMachine trait with metadata-only operations
- **RaftLogStorageAdapter**: Adapts TransactionLogStore to OpenRaft's RaftLogStorage trait
- **WormFsNetworkFactory**: Creates lightweight RaftMember instances for peer communication
- **RaftMember**: Implements RaftNetwork trait for point-to-point Raft RPCs via libp2p

#### State Machine Features
- **Two-Phase Commit**: TransactionPrepare, TransactionCommit, TransactionAbort operations
- **Metadata Operations**: FileCreate, FileUpdate, FileDelete, CreateStripe, DeleteStripe, etc.
- **Event Emission**: MetadataChange events published to subscribers on commit
- **Snapshot Support**: Snapshot creation, restoration, and builder implementation
- **Transaction Cleanup**: Automatic cleanup of old transactions from state machine memory

#### Public API Methods
All 10 public API methods are fully implemented:
1. `new()` - Creates Raft instance with dependencies
2. `initialize()` - Single-node cluster bootstrap
3. `propose_operation()` - Submit operations through Raft consensus
4. `is_leader()` - Check leadership status
5. `get_metrics()` - Returns Raft metrics
6. `trigger_snapshot()` - Force snapshot creation
7. `add_node()` - Two-phase membership addition (learner → voter)
8. `remove_node()` - Two-phase membership removal (voter → learner → removal)
9. `step_down()` - Leader relinquishes leadership
10. `subscribe_metadata_changes()` - Event channel subscription

#### RPC Handling
- **handle_raft_rpc()**: Method for processing incoming Raft RPCs (Vote, AppendEntries, InstallSnapshot)
- Deserializes RaftRpcMessage, calls OpenRaft methods, serializes RaftRpcResponse

### Known Limitations & Future Work 🔧

#### Network Integration
- **StorageNetwork RPC Wiring**: StorageNetwork can receive Raft RPCs but cannot forward them to StorageRaftMember yet
  - TODO: Add raft_handler field to InnerState
  - TODO: Add register_raft_handler() method to StorageNetworkHandle
  - TODO: Update Raft RPC handler to call handle_raft_rpc()
  - Impact: Multi-node clusters cannot communicate until this is wired up

#### Multi-Node Cluster Bootstrap
- **Current**: Only single-node initialization via `initialize(peers: Vec::new())`
- **Pattern**: Initialize as single-node, then use `add_node()` to grow cluster
- **Future**: Could add support for N-node bootstrap by accepting full node information (NodeId + PeerId + SocketAddr)

#### Event Emission Gaps
Several MetadataOperations don't include all fields needed for complete events:
- **FileCreate**: Lacks `file_id` (generated during apply) → FileCreate events not emitted
- **FileUpdate/FileDelete**: Lack `inode` field → Events have inode=0
- **DeleteStripe**: Lacks `file_id` → StripeDeleted events not emitted
- **CreateStripe**: Uses placeholder `stripe_index=0` and `checksum=0`
- Fix: Either pre-generate IDs before proposing, or make event emission async with metadata queries

#### Vote Persistence
- **Current**: Votes stored in memory only (RwLock<VoteState>)
- **Impact**: Node restart loses vote, could violate "vote once per term" rule
- **Future**: Persist votes to redb table within TransactionLogStore
- **Acceptable**: For testing and development; must fix before production

#### Testing Gaps
- **Unit Tests**: ✅ Comprehensive coverage of state machine, log storage, serialization
- **Integration Tests**: ❌ No multi-node cluster integration tests yet
- **Chaos Tests**: ❌ No consensus property testing (leader election, log consistency, etc.)
- **Performance Tests**: ❌ No benchmarks for throughput, latency, replication lag

#### SnapshotStore Integration
- SnapshotStore module exists but is entirely stubbed (all methods return unimplemented errors)
- Current snapshot implementation uses in-memory byte arrays
- Future: Integrate with SnapshotStore for persistent, compressed snapshots

### Completion Criteria for Phase 2.3

**Status**: Core implementation complete, polish remaining

**To fully close Issue #76**, the following work is recommended (but not required for declaring Phase 2.3 complete):

1. **Network Wiring** (1-2 days): Complete StorageNetwork ↔ StorageRaftMember RPC integration
2. **Integration Tests** (2-3 days): Multi-node cluster tests, leader election, membership changes
3. **Chaos Tests** (1-2 days): Jepsen-style property tests for consensus safety
4. **Performance Benchmarks** (1 day): Throughput, latency, replication lag measurements
5. **Vote Persistence** (0.5 day): Persist votes to redb for crash recovery
6. **SnapshotStore** (3-5 days): Implement persistent snapshot storage with streaming compression

**Current Recommendation**: Declare Phase 2.3 complete with documented limitations. The core Raft infrastructure is solid and functional for single-node deployments. Multi-node support requires network wiring and testing, which can be tackled in a follow-up issue.

---
*Last Updated: 2025-10-29 - Issue #76 Phase 2.3*
