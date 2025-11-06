# TransactionManager Component Design

## Purpose & Responsibilities

TransactionManager provides distributed transaction support for WormFS metadata operations, ensuring ACID guarantees across the cluster. Its responsibilities include:

- Managing transaction lifecycle (begin, commit, abort)
- Coordinating two-phase commit protocol through Raft consensus
- Tracking active transactions with timeout-based expiration
- Enforcing transaction limits and validating operations
- Providing metadata change subscriptions for real-time notifications
- Preventing deadlocks through timeout-based lock acquisition

## Architecture & Design

### Transaction Lifecycle

```
┌─────────────────────────────────────────┐
│      TransactionManager                 │
├─────────────────────────────────────────┤
│                                         │
│  ┌───────────────────────────────────┐ │
│  │   Transaction Lifecycle           │ │
│  │                                   │ │
│  │   1. begin() → TxId               │ │
│  │   2. add_operation() (repeat)     │ │
│  │   3. commit() or abort()          │ │
│  │                                   │ │
│  │   Active Transactions:            │ │
│  │   HashMap<TxId, TransactionBatch> │ │
│  └───────────────────────────────────┘ │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │   Two-Phase Commit via Raft      │ │
│  │                                   │ │
│  │   Phase 1: Prepare                │ │
│  │   - Validate operations           │ │
│  │   - Propose to Raft               │ │
│  │   - Wait for quorum               │ │
│  │                                   │ │
│  │   Phase 2: Commit                 │ │
│  │   - Apply to state machine        │ │
│  │   - Update metadata store         │ │
│  │   - Notify subscribers            │ │
│  └───────────────────────────────────┘ │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │   Cleanup & Monitoring            │ │
│  │  • Expire timed-out transactions  │ │
│  │  • Track transaction count        │ │
│  │  • Metrics collection             │ │
│  └───────────────────────────────────┘ │
└─────────────────────────────────────────┘
         │                    │
         ▼                    ▼
  StorageRaftMember      MetadataStore
```

### Operation Types

TransactionManager supports these high-level operations:

```rust
pub enum Operation {
    /// Create a new file
    CreateFile {
        file_id: FileId,
        path: PathBuf,
        inode: u64,
        metadata: FileMetadata,
        policy: StoragePolicy,
    },

    /// Update existing file metadata
    UpdateFile {
        file_id: FileId,
        inode: u64,
        metadata: FileMetadata,
        policy: StoragePolicy,
    },

    /// Delete a file
    DeleteFile {
        file_id: FileId,
        inode: u64,
    },

    /// Create a stripe for a file
    CreateStripe {
        file_id: FileId,
        stripe_id: StripeId,
        stripe_index: u32,
        policy: StoragePolicy,
        offset: u64,
        size: u64,
        chunks: Vec<ChunkId>,
    },

    /// Delete a stripe
    DeleteStripe {
        stripe_id: StripeId,
        file_id: FileId,
    },

    /// Acquire distributed locks
    AcquireReadLock { ... },
    AcquireWriteLock { ... },
    ReleaseLock { ... },
}
```

### Transaction Flow

**1. Begin Transaction**
```rust
let tx_id = tx_manager.begin(Duration::from_secs(30)).await?;
```
- Validates transaction limit not exceeded
- Generates unique transaction ID
- Records creation time and timeout
- Returns `TxId` for subsequent operations

**2. Add Operations**
```rust
tx_manager.add_operation(tx_id, Operation::CreateFile {
    file_id: FileId::generate(),
    path: PathBuf::from("/test.txt"),
    // ... other fields
}).await?;
```
- Validates transaction exists and hasn't expired
- Converts high-level `Operation` to `MetadataOperation`
- Appends to transaction's operation list
- No Raft interaction yet (local only)

**3. Commit (Two-Phase)**
```rust
tx_manager.commit(tx_id).await?;
```

**Phase 1: Prepare**
- Retrieve transaction batch from active transactions
- Create `WormFsOperation::TransactionPrepare` with all operations
- Propose to Raft for consensus
- Raft replicates to followers
- Each node applies prepare to state machine (validates operations)
- Wait for quorum acknowledgment

**Phase 2: Commit**
- Create `WormFsOperation::TransactionCommit` with tx_id
- Propose to Raft
- State machine applies operations to MetadataStore
- Emit subscription events for each operation
- Remove transaction from active set
- Return success to caller

### Subscription System

TransactionManager integrates with the WormFsStateMachine subscription system to provide real-time metadata change notifications:

```rust
// Subscribe to all metadata changes
let rx = storage_raft_member
    .subscribe_metadata_changes(None)
    .await;

// Or subscribe to specific change types
let rx = storage_raft_member
    .subscribe_metadata_changes(Some(vec![
        MetadataChangeType::FileCreated,
        MetadataChangeType::FileDeleted,
    ]))
    .await;

// Receive events
while let Some(event) = rx.recv().await {
    match event.changes[0] {
        MetadataChange::FileCreated { file_id, path, .. } => {
            println!("File created: {:?}", path);
        }
        // ... handle other events
    }
}
```

**Event Types:**
- `FileCreated` - New file created
- `FileUpdated` - File metadata modified
- `FileDeleted` - File removed
- `StripeCreated` - Stripe allocated for file
- `StripeDeleted` - Stripe removed

**Implementation Details:**
- Subscriptions are stored in `WormFsStateMachine::Inner`
- Events are broadcast via `tokio::sync::broadcast` channels
- Subscribers receive events after Raft commit (not during prepare)
- Supports filtering by event type
- Configurable subscriber limit (default: 100)

### Deadlock Prevention

WormFS uses timeout-based deadlock prevention rather than active deadlock detection:

- Lock operations include an `expires_at` timestamp
- If a lock cannot be acquired before expiration, the operation fails
- Transactions that fail during commit are aborted
- Default lock timeout: 10 seconds (configurable)

This approach is simpler than cycle detection and works well for the expected workload where locks are held briefly.

## Configuration

```toml
[transactions]
# Maximum number of active transactions (default: 1000)
max_active_transactions = 1000

# Timeout for transaction prepare phase in seconds (default: 30)
prepare_timeout_secs = 30

# Lock acquisition timeout in seconds (default: 10)
lock_timeout_secs = 10

# Deadlock detection interval in milliseconds (default: 100)
# Note: Currently we use timeout-based prevention, not active detection
deadlock_detection_interval_ms = 100

# Enable subscription system for metadata changes (default: true)
enable_subscriptions = true

# Maximum number of concurrent subscribers (default: 100)
max_subscribers = 100

# Interval for cleanup task to check for expired transactions (default: 1 second)
cleanup_interval_secs = 1
```

## ACID Guarantees

### Atomicity
All operations in a transaction either succeed together or fail together. This is guaranteed by:
- Two-phase commit protocol
- Raft consensus ensuring all nodes agree
- Prepare phase validates all operations before committing
- If any operation fails during prepare, the entire transaction is aborted

### Consistency
Database invariants are maintained:
- Unique constraints (e.g., no duplicate file paths)
- Referential integrity (e.g., stripes reference existing files)
- State machine validation during prepare phase

### Isolation
Transactions are isolated through Raft's linearizability:
- All operations serialized through Raft log
- Each transaction gets a unique log index
- Operations applied in strict log order
- No dirty reads, phantom reads, or write skew within Raft's guarantees

### Durability
Committed data survives crashes:
- Raft log persisted to disk (TransactionLogStore)
- MetadataStore uses SQLite with WAL mode
- Snapshots provide recovery points
- All operations replicated to quorum before commit

## Error Handling

```rust
pub enum Error {
    /// Transaction ID not found
    TransactionNotFound(TxId),

    /// Transaction has expired
    TransactionExpired(TxId),

    /// Too many active transactions
    TooManyTransactions(usize),

    /// Operation validation failed
    InvalidOperation(String),

    /// Raft proposal failed
    RaftError(String),

    /// Timeout exceeded
    InvalidTimeout(Duration, Duration),
}
```

## Testing

The TransactionManager has comprehensive test coverage:

### Unit Tests (`src/transaction_manager/tests.rs`)
- Transaction lifecycle (begin/commit/abort)
- Operation validation
- Timeout handling
- Transaction limits

### Integration Tests (`tests/transaction_manager_integration.rs`)
- Multi-node cluster transactions
- Concurrent transaction handling
- ACID property verification:
  - Atomicity: 5 files created in single transaction
  - Consistency: Data consistent across all nodes
  - Isolation: Concurrent transactions don't interfere
  - Durability: Data persists across operations
- Write skew detection
- Sequential lock acquisition
- Subscription event delivery

## Performance Characteristics

**Expected Performance:**
- Transaction prepare: <10ms (local validation)
- Transaction commit: <50ms (includes Raft round-trip)
- Lock acquisition: <1ms (Raft operation)
- Subscription notification: <5ms (async broadcast)
- Concurrent transactions: >100/second (with Raft batching)

**Resource Usage:**
- Memory: ~1KB per active transaction
- Disk I/O: Only during Raft log writes (sequential)
- Network: Minimal (only Raft consensus traffic)

## Dependencies

- `StorageRaftMember` - For Raft consensus and replication
- `MetadataStore` - For persistent metadata storage
- `MetricService` - For transaction metrics and monitoring
- `tokio` - Async runtime for transaction management
- `uuid` - Transaction ID generation (uses TxId = u64)

## Future Enhancements

Potential improvements for future phases:

1. **Active Deadlock Detection**: Implement wait-for graph analysis
2. **Lock Escalation**: Convert multiple row locks to table locks
3. **Backpressure for Subscribers**: Rate limiting when subscribers fall behind
4. **Read-Only Transactions**: Optimize for read-heavy workloads
5. **Distributed Transactions**: Coordinate with other services/systems
6. **Transaction Logging**: Detailed audit trail for compliance

## See Also

- [StorageRaftMember](02_StorageRaftMember.md) - Raft consensus implementation
- [MetadataStore](05_MetadataStore.md) - Persistent metadata storage
- [TransactionLogStore](07_TransactionLogStore.md) - Raft log persistence
- GitHub Issue #77 - Phase 2.4: MetadataStore Transaction Support
