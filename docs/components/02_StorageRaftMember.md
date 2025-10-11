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

**Write Operations (Two-Phase Commit via Raft):**

*Example: Appending data to end of file*

**Phase 1: PREPARE (via Raft consensus)**
1. Client/Component submits write request to any node
2. If follower: forward to leader
3. Leader becomes 2PC coordinator:
   - Generates transaction ID (TxID)
   - Calculates stripe layout, erasure coding, and chunk placement
   - Creates unified TransactionPrepare entry containing:
     * Metadata changes (file size, chunk allocations)
     * Chunk assignments (which nodes get which chunk data)
4. Leader proposes TransactionPrepare through Raft
5. Raft replicates prepare entry to all followers
6. Each node (including leader) applies prepare locally:
   - Prepares metadata changes (not yet visible in MetadataStore)
   - If node has chunks assigned: writes chunks with state="preparing" and fsyncs
   - Votes PREPARED or ABORT based on success
   - Stores vote locally
7. Once majority commits prepare entry, leader collects votes from all nodes
8. Leader makes decision: COMMIT if all voted PREPARED, else ABORT

**Phase 2: COMMIT/ABORT (via Raft consensus)**

*Commit Path:*
9. Leader proposes TransactionCommit through Raft
10. Raft replicates commit decision to all followers
11. Each node applies commit:
    - Applies metadata changes to MetadataStore (chunks now visible)
    - Changes local preparing chunks to state="active"
12. Return success to client

*Abort Path:*
9. Leader proposes TransactionAbort through Raft
10. Raft replicates abort decision to all followers
11. Each node applies abort:
    - Discards metadata changes
    - Deletes local preparing chunks
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
        operation: MetadataOperation,
    ) -> Result<OperationResult, RaftError>;
    
    /// Read metadata (may serve stale reads on followers)
    pub async fn read_metadata(
        &self,
        query: MetadataQuery,
        allow_stale: bool,
    ) -> Result<MetadataResult, RaftError>;
    
    /// Check if this node is the current leader
    pub fn is_leader(&self) -> bool;
    
    /// Get current Raft metrics and status
    pub fn get_metrics(&self) -> RaftMetrics;
    
    /// Manually trigger a snapshot
    pub async fn trigger_snapshot(&self) -> Result<(), RaftError>;
    
    /// Add a new node to the cluster
    pub async fn add_node(&self, node_id: NodeId, address: SocketAddr) -> Result<(), RaftError>;
    
    /// Remove a node from the cluster
    pub async fn remove_node(&self, node_id: NodeId) -> Result<(), RaftError>;
    
    /// Step down from leader (for graceful shutdown)
    pub async fn step_down(&self) -> Result<(), RaftError>;
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
        chunk_ops: Option<Vec<ChunkDataOperation>>,
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

/// ChunkDataOperation that can be proposed through Raft
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChunkDataOperation {
    StoreChunk{
        pub node_id: NodeId,
        pub disk_id: DiskId,
        pub source: StorageEndpointUrl,
        pub chunk_id: ChunkId,
    },
    DeleteChunk{
        pub node_id: NodeId,
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

pub struct RaftMetrics {
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

pub struct ChunkAllocation {
    pub chunk_id: ChunkId,
    pub node_id: NodeId,
    pub disk_id: DiskId,
    pub chunk_index: u8,
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

## Configuration

```toml
[raft]
# Election configuration
heartbeat_interval_ms = 250
election_timeout_min_ms = 1000
election_timeout_max_ms = 2000

# Log management
max_payload_entries = 1000
snapshot_time_threshold_hours = 24
snapshot_log_size_threshold_mb = 10

# Replication
replication_lag_threshold = 100
max_in_flight_append_entries = 10

# Read optimization
enable_lease_based_reads = true
lease_duration_ms = 5000
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

### Two-Phase Commit Protocol

1. **Vote Collection Mechanism**: Should votes be collected via direct RPC queries to each node, or should votes be embedded in Raft acknowledgments of the prepare entry?

2. **Transaction Timeout**: What's an appropriate timeout for transactions, especially for large file writes (100s of MB)? Should it be configurable per operation?

3. **Partial Prepare Failure**: If some nodes prepare successfully but others fail, should we attempt retry with different node placement, or immediately abort?

4. **Transaction Recovery on Leader Change**: How should the new leader handle in-flight transactions? Query all participants, or use a conservative timeout-based approach?

5. **Orphaned Chunk Cleanup**: How frequently should nodes scan for and clean up orphaned "preparing" chunks from aborted transactions?

6. **Vote Persistence**: Should prepare votes be persisted to disk, or can they be kept in memory (and re-queried after crashes)?

7. **Transaction State Limits**: Should there be a maximum number of concurrent transactions to prevent resource exhaustion?

8. **Chunk Data in Raft Log**: Given that chunk data can be large (MBs), should we limit the size of individual TransactionPrepare entries, or handle large operations specially?

### Raft Operations

9. **Read Consistency Guarantees**: Should we support linearizable reads by default (forward to leader), or prefer lease-based reads with bounded staleness? What should the default staleness bound be?

10. **Snapshot Coordination**: The design mentions signaling all nodes to snapshot simultaneously. Should this be a two-phase process (prepare + commit) to ensure transactional consistency, or is eventual consistency acceptable?

11. **Pre-vote Optimization**: Should we implement Raft's pre-vote extension to avoid unnecessary leader changes when a partitioned node rejoins?

12. **Log Compaction Strategy**: Beyond snapshots, should we implement additional log compaction (e.g., log cleaning, log-structured merge trees)?

13. **Client Request Routing**: Should followers automatically forward writes to the leader transparently, or return an error with leader hint and let clients retry?

14. **Quorum Reads**: Should we support quorum reads (read from majority) as an option between stale reads and linearizable reads?

15. **Configuration Changes**: Should membership changes (add/remove node) use single-node or joint consensus approach? OpenRaft supports both.

16. **Metrics Granularity**: What specific Raft metrics should be exposed for monitoring? Per-operation latency, replication lag histograms, transaction success/failure rates?

17. **Backpressure**: How should we handle backpressure when the Raft log grows faster than it can be applied to the state machine?

18. **Lock Expiration**: Should lock expiration be handled by Raft consensus (explicit timeout proposals) or locally by each node's state machine with clock-based expiration?

19. **Snapshot Compression**: Should metadata snapshots be compressed before storage/transfer? What compression algorithm?

20. **Observer Nodes**: Should we support read-only observer nodes that receive log replication but don't vote in elections?

21. **Raft Extensions**: Should we implement any Raft extensions like pipeline optimization, parallel log application, or batched AppendEntries?

22. **Failure Detection Tuning**: How should heartbeat and election timeout be tuned for different network conditions (LAN vs WAN deployments)?
