# Phase 2B Detailed Implementation Plan: Metadata Gossip Protocol

## Overview

Phase 2B "Metadata Gossip Protocol" originally scoped as a 2-3 week phase, contains significant distributed systems complexity that makes it difficult to implement and test as a single unit. This document breaks Phase 2B into 10 smaller, manageable sub-phases that can be independently developed and tested.

## Rationale for Breaking Down Phase 2B

The original Phase 2B encompasses:
- Master election using libp2p consensus
- Metadata operation proposal and approval workflow
- Event broadcasting with sequence numbers
- Acknowledgment tracking and replay mechanisms
- Conflict resolution for metadata operations

This represents roughly 25-35 days of work when considering:
- Complexity of distributed consensus algorithms
- Testing distributed systems edge cases (network partitions, split-brain scenarios)
- Ensuring reliability under various failure modes
- Performance optimization for metadata operations
- Integration with existing networking layer from Phase 2A
- Learning curve for distributed systems concepts

By breaking this into smaller phases, we achieve:
- **Reduced Risk**: Each phase can be validated before proceeding
- **Faster Feedback**: Issues discovered early when they're easier to fix
- **Independent Testing**: Each component thoroughly tested in isolation
- **Progress Visibility**: Clear milestones and completion criteria
- **Team Flexibility**: Multiple developers can work on different phases
- **Incremental Complexity**: Start simple, add sophistication gradually

## Sub-Phase Breakdown

### **Phase 2B.1: Custom Protocol Definition (2-3 days)** COMPLETED

**Goal:** Define the protocol buffer messages for metadata synchronization

**Context:** Before implementing any distributed logic, we need a clear message format for all metadata operations. This provides the foundation for all subsequent communication.

**Deliverables:**
- Extend `proto/wormfs.proto` with metadata protocol messages:
  - `MetadataEvent` (FileCreated, FileDeleted, FileModified, ChunkPlaced, ChunkRemoved)
  - `MetadataProposal` (operation proposal from any node)
  - `MetadataResponse` (approval/rejection from master)
  - `MetadataAck` (acknowledgment from peers)
  - `MetadataRequest` (request for missing events)
- Message fields include:
  - Sequence numbers for ordering
  - Timestamps for conflict resolution
  - Peer IDs for attribution
  - Payload data (file/chunk metadata)
- Code generation for Rust structs
- Basic serialization/deserialization tests

**Success Criteria:**
- Complete protobuf schema for all metadata messages
- Generated Rust code compiles without errors
- Can serialize and deserialize all message types
- Message format is versioned for future compatibility
- Unit tests verify serialization round-trips
- No clippy errors or warnings
- No cargo formatting errors or warnings

**Test Strategy:**
- Unit test: Serialize/deserialize each message type
- Unit test: Invalid message format handling
- Unit test: Version compatibility checks
- Property test: Round-trip serialization preserves data
- Integration test: Message exchange between two nodes

**Files Modified:**
- `proto/wormfs.proto` - Add metadata protocol messages
- `src/metadata_protocol.rs` - New module for protocol handling
- `tests/metadata_protocol_tests.rs` - Protocol message tests
- `Cargo.toml` - Add prost build dependencies if needed

---

### **Phase 2B.2: Basic Custom Protocol Handler (2-3 days)** COMPLETED
**Goal:** Implement libp2p protocol handler for metadata messages

**Context:** Create a custom libp2p protocol that can send and receive metadata messages. This integrates with our existing NetworkService from Phase 2A.

**Deliverables:**
- `MetadataProtocol` implementing libp2p protocol traits
- Protocol identifier: `/wormfs/metadata/1.0.0`
- Request/response pattern for metadata operations
- Integration with NetworkService swarm
- Basic message routing to handlers
- Connection-based stream management
- Protocol negotiation handling

**Success Criteria:**
- Can establish metadata protocol streams between nodes
- Can send metadata messages between connected peers
- Protocol negotiation works correctly
- Multiple concurrent streams handled properly
- Stream errors handled gracefully
- Integration with existing NetworkService successful
- No clippy errors or warnings
- No cargo formatting errors or warnings

**Test Strategy:**
- Integration test: Establish protocol between two nodes
- Integration test: Send metadata message, verify receipt
- Integration test: Multiple concurrent messages
- Integration test: Handle protocol negotiation failure
- Error test: Malformed message handling
- Performance test: Message throughput baseline

**Files Modified:**
- `src/metadata_protocol.rs` - Protocol handler implementation
- `src/networking.rs` - Integrate metadata protocol into swarm
- `tests/metadata_protocol_tests.rs` - Protocol handler tests
- `src/lib.rs` - Export metadata protocol types

---

### **Phase 2B.3: Event Broadcasting Foundation (2-3 days)** COMPLETED
**Goal:** Implement basic event broadcasting to all connected peers

**Context:** Create the mechanism for distributing metadata events across the cluster. Start with simple fire-and-forget broadcasting before adding reliability.

**Deliverables:**
- `MetadataReplicator` component for event distribution
- Broadcast metadata events to all connected peers
- Event types: FileCreated, FileDeleted, ChunkCreated, ChunkRemoved, ChunkVerified, ChunkRepaired, ChunkMoved, FileUpdated, StripeCreated, StripeReplaced, StripeDeleted, ChunkDeleted.
- No ordering guarantees initially (just broadcast)
- No reliability guarantees initially (fire-and-forget)
- Basic event filtering (don't echo back to sender)
- Logging for debugging broadcast behavior

**Success Criteria:**
- Can broadcast events to all connected peers
- Events reach all peers in multi-node cluster
- Sender doesn't receive its own events
- Broadcast works with dynamic peer set (peers joining/leaving)
- Event delivery is best-effort (no guarantees yet)
- Integration test with 3+ nodes verifies broadcast
- No clippy errors or warnings
- No cargo formatting errors or warnings

**Test Strategy:**
- Integration test: Broadcast from Node A, verify Nodes B & C receive
- Integration test: Add Node D mid-test, verify it receives subsequent events
- Integration test: Remove Node B, verify broadcast continues to others
- Integration test: Verify sender doesn't receive echo
- Performance test: Broadcast latency measurement
- Scale test: Broadcast to 10+ nodes

**Files Modified:**
- `src/metadata_replicator.rs` - New broadcaster component
- `src/storage_node.rs` - Integrate broadcaster
- `tests/metadata_replicator_tests.rs` - Broadcasting tests
- `src/lib.rs` - Export broadcaster types

---

### **Phase 2B.4: Sequence Number Management (2-3 days)** COMPLETED
**Goal:** Add sequence numbers for event ordering and gap detection

**Context:** To build reliable distributed metadata, we need to detect missing events and order events correctly. Sequence numbers are the foundation for this.

**Deliverables:**
- Add sequence numbers to all metadata events
- Per-node sequence counter (monotonically increasing)
- Track last seen sequence number per peer
- Detect sequence gaps (missing events)
- Emit gap detection events for handling
- Sequence number persistence across restarts
- Handle sequence number rollovers gracefully

**Success Criteria:**
- Events numbered sequentially per originating node
- Can detect gaps in received sequences
- Gap detection works reliably in tests
- Sequence numbers persist across node restarts
- Handle large sequence numbers (u64 range)
- Integration test verifies gap detection
- No clippy errors or warnings
- No cargo formatting errors or warnings

**Test Strategy:**
- Unit test: Sequence number generation and increment
- Integration test: Receive events, verify sequential numbering
- Integration test: Inject gap (skip sequence), verify detection
- Integration test: Restart node, verify sequence continues
- Integration test: Multiple nodes, each with independent sequences
- Stress test: High-frequency events, verify no sequence errors
- Edge case test: Sequence number near u64::MAX

**Files Modified:**
- `src/metadata_protocol.rs` - Add sequence number fields
- `src/metadata_broadcaster.rs` - Sequence number assignment
- `src/sequence_tracker.rs` - New module for sequence management
- `src/metadata_store.rs` - Persist sequence numbers
- `tests/sequence_tests.rs` - Sequence number tests

---

### **Phase 2B.5: OpenRaft Integration & Leader Election (4-5 days)**
**Goal:** Integrate OpenRaft for distributed consensus instead of building custom master election

**Context:** Rather than building custom master election, proposal/approval workflow, acknowledgments, replay, and partition handling from scratch (which essentially describes the Raft consensus algorithm), we leverage the battle-tested OpenRaft crate. This gives us production-ready consensus with linearizable consistency guarantees while reducing implementation complexity by ~70%.

**Architectural Decision:**
Using OpenRaft provides:
- Leader election with automatic failover
- Replicated log with majority-based commits
- Split-brain prevention via quorum
- Automatic conflict resolution (linearizable operations)
- Network partition handling
- Log compaction and snapshots

**Deliverables:**

1. **TransactionLog Trait** (`src/transaction_log.rs`)
   - Abstraction for Raft log storage
   - Methods: `append()`, `get()`, `get_range()`, `truncate()`, `compact()`, `flush()`
   - Async trait compatible with tokio

2. **Redb Implementation** (`src/redb_transaction_log.rs`)
   - Durable transaction log using redb
   - Efficient sequential writes and zero-copy reads
   - Crash recovery support
   - Atomic operations for consistency

3. **Raft Storage** (`src/raft_storage.rs`)
   - Implements OpenRaft's `RaftLogStorage` trait
   - Uses TransactionLog for log entries
   - Stores Raft hard state (current term, voted_for)
   - Snapshot support for log compaction

4. **Raft Network** (`src/raft_network.rs`)
   - Implements OpenRaft's `RaftNetwork` trait
   - Routes Raft RPCs through libp2p NetworkService
   - Handles AppendEntries, RequestVote, InstallSnapshot

5. **Raft State Machine** (`src/raft_state_machine.rs`)
   - Implements OpenRaft's `RaftStateMachine` trait
   - Applies committed log entries to MetadataStore
   - Snapshot creation and restoration

6. **Raft Node** (`src/raft_node.rs`)
   - Main Raft coordination component
   - Initializes OpenRaft with custom implementations
   - Handles leader election and membership changes
   - Exposes API for submitting metadata operations

**Success Criteria:**
- Leader automatically elected on startup (< 2s)
- Metadata operations go through Raft log
- Operations replicated to majority quorum before commit
- Automatic failover on leader failure (< 10s)
- Transaction log survives crashes and recovers state
- Linearizable consistency for all metadata operations
- No clippy errors or warnings
- No cargo formatting errors or warnings

**Test Strategy:**
- Unit tests for TransactionLog trait implementations
- Integration test: 3-node cluster leader election
- Integration test: Metadata operation replication
- Integration test: Leader failure and re-election
- Integration test: Network partition (majority continues, minority blocks)
- Chaos test: Random node failures during operations
- Performance test: Operation throughput and latency

**Files to Create/Modify:**
- `src/transaction_log.rs` (new)
- `src/redb_transaction_log.rs` (new)
- `src/raft_storage.rs` (new)
- `src/raft_network.rs` (new)
- `src/raft_state_machine.rs` (new)
- `src/raft_node.rs` (new)
- `src/metadata_replicator.rs` (modify to use Raft)
- `src/storage_node.rs` (integrate Raft node)
- `src/lib.rs` (export new modules)
- `Cargo.toml` (add openraft and redb dependencies)
- `config/storage_node.yaml` (add Raft configuration)
- `tests/raft_consensus_tests.rs` (new)

**Configuration Example:**
```yaml
raft:
  data_dir: "./data/raft"
  
  transaction_log:
    type: "redb"
    path: "./data/raft/transaction.log"
    cache_size_mb: 128
    
  election:
    timeout_ms: 1500
    heartbeat_interval_ms: 500
    
  snapshot:
    enabled: true
    threshold_entries: 10000
    path: "./data/raft/snapshots"
```

**What This Phase Replaces:**
This single phase effectively replaces what would have been:
- Custom master election (old Phase 2B.5)
- Proposal/approval workflow (old Phase 2B.6)
- Acknowledgment system (old Phase 2B.7)
- Event replay mechanism (old Phase 2B.8)
- Conflict resolution (old Phase 2B.9)
- Partition handling (old Phase 2B.10)

All of these capabilities are provided automatically by OpenRaft's Raft implementation.

---

### **Phase 2B.6: Metadata Operation Workflow (3-4 days)** ⚠️ SUPERSEDED BY OPENRAFT

**Note:** This phase is **automatically handled by OpenRaft** (Phase 2B.5). Raft's log replication provides the proposal/approval workflow through its AppendEntries RPC. Operations are submitted to the leader, replicated to a majority quorum, and then committed. This provides stronger guarantees (linearizability) than a custom implementation would.

---

### **Phase 2B.7: Acknowledgment System (2-3 days)** ⚠️ SUPERSEDED BY OPENRAFT

**Note:** This phase is **automatically handled by OpenRaft** (Phase 2B.5). Raft's AppendEntries RPC includes built-in acknowledgments from followers. The leader tracks which followers have replicated each log entry and only commits entries once a majority has acknowledged. This is more robust than a custom acknowledgment system.

---

### **Phase 2B.8: Event Replay Mechanism (3-4 days)** ⚠️ SUPERSEDED BY OPENRAFT

**Note:** This phase is **automatically handled by OpenRaft** (Phase 2B.5). Raft automatically detects when followers fall behind and sends them missing log entries via AppendEntries RPCs. New nodes joining the cluster receive a snapshot (if far behind) followed by log entries to catch up. This is more efficient than custom replay logic.

---

### **Phase 2B.9: Conflict Resolution (3-4 days)** ⚠️ SUPERSEDED BY OPENRAFT

**Note:** This phase is **automatically handled by OpenRaft** (Phase 2B.5). Raft's linearizable consistency model prevents conflicts entirely - all operations are sequenced by the leader in a single log, eliminating concurrent conflicting operations. This is stronger than eventual consistency with conflict resolution.

---

### **Phase 2B.10: Network Partition Handling (3-4 days)** ⚠️ SUPERSEDED BY OPENRAFT

**Note:** This phase is **automatically handled by OpenRaft** (Phase 2B.5). Raft's linearizable consistency model prevents conflicts entirely - all operations are sequenced by the leader in a single log, eliminating concurrent conflicting operations. This is stronger than eventual consistency with conflict resolution.

---

### **Phase 2B.11: Raft Integration Testing (3-4 days)**
**Goal:** Comprehensive testing of OpenRaft-based metadata consensus system

**Context:** Validate the complete distributed metadata synchronization system under realistic conditions with multiple nodes and various failure scenarios.

**Deliverables:**
- Comprehensive integration test with 5+ nodes
- Test all failure scenarios (master failure, network partition, node crash)
- Test proposal workflow under load
- Test acknowledgment and replay under stress
- Performance baseline for metadata operations
- Stress testing framework for gossip protocol
- End-to-end workflow tests (file operations through gossip)

**Success Criteria:**
- All nodes maintain consistent metadata state
- Master election and failover work reliably
- Proposals processed within acceptable time (< 5s p99)
- Partitions handled without data corruption
- System stable under continuous load (>1 hour)
- Performance acceptable (>100 ops/sec cluster-wide)
- No clippy errors or warnings
- No cargo formatting errors or warnings

**Test Strategy:**
- Integration test: Start 5 nodes, verify mesh metadata synchronization
- Integration test: Perform file operations, verify metadata propagated
- Chaos test: Kill random nodes, verify recovery
- Chaos test: Create partitions, verify handling and recovery
- Stress test: High-frequency metadata operations
- Performance test: Measure operation latency and throughput
- Reliability test: Extended stability under normal operation
- Scale test: Test with 10+ nodes

**Files Modified:**
- `tests/metadata_gossip_tests.rs` - Comprehensive gossip tests
- `tests/test_helpers.rs` - Enhanced test utilities for gossip
- `benches/metadata_bench.rs` - Performance benchmarks
- `tests/chaos_tests.rs` - Chaos testing for gossip
- `src/storage_node.rs` - Final integration touches

---

### **Phase 2B.12: Transaction Log Compaction (2-3 days)**
**Goal:** Implement automatic log compaction to prevent unbounded growth of the Raft transaction log

**Context:** Without log compaction, the Raft transaction log would grow indefinitely, eventually consuming all disk space and increasing startup/recovery times. By implementing automatic snapshotting and log truncation, we can maintain bounded disk usage while preserving the ability to recover from crashes and catch up lagging peers.

**Deliverables:**

1. **Snapshot Manager** (`src/snapshot_manager.rs`)
   - Create snapshots of current metadata state
   - Store snapshots separately from transaction log
   - Versioned snapshot format for future compatibility
   - Atomic snapshot creation to prevent corruption

2. **Automatic Snapshot Triggers**
   - Trigger based on log entry count threshold (e.g., 10,000 entries)
   - Trigger based on log size threshold (e.g., 1GB)
   - Time-based triggers (e.g., every 6 hours)
   - Background task to monitor and trigger snapshots

3. **Log Compaction**
   - Truncate log entries covered by snapshot
   - Keep configurable buffer of recent entries (e.g., 100)
   - Atomic compaction operations
   - Space reclamation via redb compaction

4. **Snapshot-based Catch-up**
   - Send snapshot to far-behind peers
   - Follow with log entries after snapshot point
   - Much faster than replaying entire log history
   - Progress tracking and resumption on failure

5. **Multi-version Snapshots**
   - Keep last N snapshots for safety (e.g., 3)
   - Garbage collect old snapshots
   - Allow rollback if corruption detected

**Success Criteria:**
- Snapshots automatically created when thresholds exceeded
- Transaction log remains bounded in size
- Log compaction works without data loss
- Far-behind peers catch up via snapshot + log tail
- Snapshot creation doesn't block operations
- Old snapshots garbage collected properly
- System recovers correctly from snapshot on restart
- No clippy errors or warnings

**Test Strategy:**
- Unit test: Snapshot creation and restoration
- Unit test: Log compaction correctness
- Integration test: Generate 10k+ operations, verify log compaction
- Integration test: Stop node, compact, restart, verify state preserved
- Integration test: New node catches up via snapshot
- Integration test: Multiple concurrent snapshots handled correctly
- Performance test: Snapshot creation time for various metadata sizes
- Stress test: Continuous operations with periodic compaction

**Configuration:**
```yaml
raft:
  transaction_log:
    max_size_mb: 1024           # Trigger snapshot when log exceeds this
    max_entries: 10000          # Trigger snapshot after this many entries
    
  snapshot:
    enabled: true
    interval_hours: 6           # Take snapshot every 6 hours
    min_entries: 1000           # Don't snapshot if fewer than this
    keep_count: 3               # Keep last 3 snapshots
    
  compaction:
    enabled: true
    retention_entries: 100      # Keep this many entries after compaction
    run_interval_mins: 30       # Check for compaction every 30 minutes
```

**Files to Create/Modify:**
- `src/snapshot_manager.rs` (new)
- `src/log_compactor.rs` (new)
- `src/redb_transaction_log.rs` (add compaction support)
- `src/raft_storage.rs` (integrate snapshot creation/restoration)
- `src/raft_node.rs` (add compaction background task)
- `tests/log_compaction_tests.rs` (new)
- `config/storage_node.yaml` (add compaction config)

**Implementation Details:**

1. **Snapshot Creation:**
   ```rust
   pub async fn create_snapshot(&self, up_to_index: u64) -> Result<Snapshot> {
       // 1. Create snapshot of metadata store state
       let metadata_state = self.metadata_store.export_state()?;
       
       // 2. Write snapshot to disk atomically
       let snapshot_path = self.snapshot_path(up_to_index);
       write_snapshot_atomic(&snapshot_path, &metadata_state)?;
       
       // 3. Return snapshot metadata
       Ok(Snapshot {
           index: up_to_index,
           term: self.current_term,
           path: snapshot_path,
           size: metadata_state.len(),
           checksum: calculate_checksum(&metadata_state),
       })
   }
   ```

2. **Log Compaction:**
   ```rust
   pub async fn compact_log(&self, snapshot: &Snapshot) -> Result<()> {
       // 1. Delete all entries <= snapshot.index
       self.transaction_log.compact(snapshot.index).await?;
       
       // 2. Update first_index in log metadata
       self.transaction_log.set_first_index(snapshot.index + 1).await?;
       
       // 3. Trigger redb compaction to reclaim space
       self.transaction_log.flush().await?;
       
       Ok(())
   }
   ```

3. **Snapshot-based Recovery:**
   ```rust
   pub async fn recover_from_snapshot(&self) -> Result<u64> {
       // 1. Find latest valid snapshot
       let snapshot = self.find_latest_snapshot()?;
       
       // 2. Restore metadata store from snapshot
       self.metadata_store.import_state(&snapshot.data)?;
       
       // 3. Replay log entries after snapshot
       let entries = self.transaction_log
           .get_range((snapshot.index + 1)..)
           .await?;
       for entry in entries {
           self.apply_to_state_machine(entry)?;
       }
       
       Ok(snapshot.index)
   }
   ```

**Benefits:**
- ✅ Bounded disk usage (log doesn't grow forever)
- ✅ Faster startup/recovery (load snapshot + small log tail)
- ✅ Efficient catch-up for lagging peers (snapshot + recent entries)
- ✅ Configurable trade-offs (frequency vs performance)
- ✅ Safe operation (atomic operations, multiple snapshot versions)

**Edge Cases Handled:**
- Snapshot creation during active operations (use copy-on-write)
- Crash during snapshot creation (atomic writes)
- Crash during log compaction (transactional operations)
- Concurrent snapshot requests (serialize with mutex)
- Snapshot transfer failure (retry with exponential backoff)
- Corrupt snapshot detection (checksum validation)

---

## Testing Strategy Across All Sub-Phases

### Unit Tests
**Focus:** Individual components and algorithms
- Message serialization/deserialization
- Sequence number generation and tracking
- Conflict resolution algorithms
- Election logic and state transitions
- Acknowledgment tracking
- Partition detection logic

**Coverage Goal:** >90% for all metadata gossip modules

### Integration Tests
**Focus:** Component interactions and distributed behavior
- Two-node metadata synchronization
- Multi-node consensus scenarios
- Master election and failover
- Event broadcasting and replay
- Network partition handling
- End-to-end metadata operations

**Test Environment:** Docker containers with network control for partition simulation

### Chaos Tests
**Focus:** Resilience under adverse conditions
- Random node failures during operations
- Network partitions at critical moments
- Concurrent operations from multiple nodes
- Master failures during proposals
- Message loss and delays
- Clock skew between nodes

**Tools:** Custom chaos framework with controllable failure injection

### Performance Tests
**Focus:** Scalability and resource usage
- Proposal throughput (ops/sec)
- Event propagation latency (ms)
- Catchup time for lagging nodes
- Memory usage under load
- Network bandwidth utilization
- CPU usage during normal operation

### Consistency Tests
**Focus:** Distributed state correctness
- All nodes reach same final state
- No data loss under failures
- Causal ordering preserved
- Conflict resolution determinism
- Partition healing correctness

### Test Infrastructure

**Helper Functions:**
- `start_gossip_cluster(num_nodes)` - Start cluster with gossip enabled
- `partition_cluster(nodes_a, nodes_b)` - Create network partition
- `heal_partition()` - Restore network connectivity
- `propose_metadata_op(node, operation)` - Submit metadata operation
- `wait_for_consensus(expected_state, timeout)` - Wait for convergence
- `inject_failure(node, failure_type)` - Inject controlled failures
- `verify_metadata_consistency(nodes)` - Check all nodes have same state

**Test Utilities:**
- Docker-based network isolation and control
- Simulated clock for time-based testing
- Message delay and loss injection
- Log capture and assertion helpers
- Metadata state comparison tools
- Performance measurement utilities

**Continuous Integration:**
- All tests run on every commit
- Separate test jobs for unit/integration/chaos
- Performance regression detection
- Consistency verification on all tests
- Cross-platform testing (Linux primary)
- Memory leak and resource leak detection

---

## Implementation Order and Dependencies

The sub-phases are designed to be implemented sequentially, with each building on the previous:

```
2B.1 (Protocol Definition)
  ↓
2B.2 (Protocol Handler)
  ↓
2B.3 (Event Broadcasting)
  ↓
2B.4 (Sequence Numbers)
  ↓
2B.5 (Master Election)
  ↓
2B.6 (Proposal Workflow)
  ↓
2B.7 (Acknowledgments)
  ↓
2B.8 (Event Replay)
  ↓
2B.9 (Conflict Resolution)
  ↓
2B.10 (Partition Handling)
  ↓
2B.11 (Integration Testing)
```

**Dependencies:**
- Each phase requires the previous phase to be complete and tested
- Phases 2B.7-2B.10 may have some parallel work opportunities
- Testing infrastructure built incrementally
- Performance baselines established early (Phase 2B.3) and tracked

**Parallel Work Opportunities:**
- Test infrastructure development alongside core functionality
- Documentation writing during implementation
- Performance testing framework preparation
- Chaos testing framework development

---

## Time Estimates and Milestones

**Total Estimated Time:** 27-37 days (5-7 weeks)
- Individual phases: 2-4 days each
- Buffer time: 20% for unexpected issues
- Integration and testing: 25% of total time

**Weekly Milestones:**
- **Week 1:** Phases 2B.1-2B.3 (Protocol and basic broadcasting)
- **Week 2:** Phases 2B.4-2B.5 (Ordering and master election)
- **Week 3:** Phases 2B.6-2B.7 (Proposals and acknowledgments)
- **Week 4:** Phases 2B.8-2B.9 (Replay and conflict resolution)
- **Week 5:** Phases 2B.10-2B.11 (Partitions and integration testing)
- **Weeks 6-7:** Buffer time, comprehensive testing, and hardening

**Risk Mitigation:**
- Each phase has clear rollback points
- Early phases establish foundation for later work
- Comprehensive testing prevents regression
- Regular code reviews ensure quality
- Distributed systems expertise consulted as needed

---

## Success Metrics

**Functional Requirements:**
- Single master elected and maintained across cluster
- All metadata operations propagate to all nodes reliably
- Missed operations detected and replayed correctly
- Master failover works within 15 seconds
- Network partitions handled without data corruption
- Conflict resolution produces deterministic results

**Performance Requirements:**
- Metadata operation latency: < 5s p99
- Event propagation: < 2s to all nodes
- Master election: < 10s on startup
- Master failover: < 15s
- Catchup for lagging node: < 30s for 1000 events
- Throughput: > 100 ops/sec cluster-wide

**Quality Requirements:**
- Zero clippy warnings
- Zero formatting errors
- >90% test coverage
- <5 critical bugs per phase
- Clean architecture boundaries
- Well-documented APIs

**Reliability Requirements:**
- No data loss under any failure scenario
- Eventual consistency guaranteed
- No split-brain conditions
- Graceful degradation under partitions
- Automatic recovery after failures

---

## Consistency Model

The metadata gossip protocol implements **eventual consistency** with the following guarantees:

**Consistency Guarantees:**
- **Single Master Coordination:** All metadata operations coordinated through elected master
- **Causal Ordering:** Operations from same node maintain causal order
- **Conflict-Free Convergence:** All nodes eventually reach same state
- **Deterministic Conflict Resolution:** Same conflicts always resolve the same way
- **No Data Loss:** Under any failure scenario with majority partition

**Availability Guarantees:**
- **Majority Partition:** Continues processing operations
- **Minority Partition:** Read-only mode (no new operations)
- **Master Failure:** New master elected, operations resume within 15s
- **Network Healing:** Automatic reconciliation and convergence

**Partition Tolerance:**
- **Detection:** Network partitions detected within 30s
- **Handling:** Only majority partition processes operations
- **Recovery:** Automatic state reconciliation on healing
- **Safety:** No conflicting operations in different partitions

---

## Security Considerations

While security is primarily addressed in later phases, Phase 2B includes basic security measures:

**Authentication:**
- Leverage Phase 2A peer authentication
- Only authenticated peers participate in gossip
- Master validates identity of proposal originators

**Authorization:**
- Basic operation validation at master
- Prevent unauthorized metadata modifications
- Audit log of all metadata operations

**Integrity:**
- All messages include checksums
- Detect and reject tampered messages
- Validate sequence number continuity

**Future Enhancements (Post-Phase 2B):**
- Encrypted metadata payloads
- Fine-grained authorization policies
- Audit log signing and verification
- Byzantine fault tolerance

---

## Operational Considerations

**Monitoring:**
- Master election events
- Proposal throughput and latency
- Event propagation delays
- Acknowledgment success rates
- Catchup operations and duration
- Conflict resolution frequency
- Partition detection and healing

**Debugging:**
- Detailed event logging with sequence numbers
- State machine transition logging
- Network event correlation
- Metadata state snapshots
- Replay history for troubleshooting

**Configuration:**
- Proposal timeout (default: 5s)
- Acknowledgment timeout (default: 3s)
- Master heartbeat interval (default: 10s)
- Election timeout (default: 30s)
- Replay batch size (default: 100 events)
- Partition detection threshold (default: 30s)

---

## Comparison with Phase 2A

**Similarities:**
- Incremental complexity building
- Comprehensive testing at each step
- Clear dependencies and ordering
- Similar time estimates per phase
- Focus on distributed systems reliability

**Differences:**
- Phase 2B builds on Phase 2A networking foundation
- More complex distributed systems concepts (consensus, consistency)
- Higher testing complexity (consistency verification)
- More emphasis on failure scenarios and edge cases
- Requires deeper distributed systems understanding

**Integration Points:**
- Uses Phase 2A's NetworkService for transport
- Leverages Phase 2A's peer management
- Builds on Phase 2A's connection management
- Extends Phase 2A's authentication model

---

## Conclusion

This detailed breakdown transforms the original monolithic Phase 2B into manageable, testable components. Each sub-phase has clear goals, deliverables, and success criteria that can be independently verified.

The sequential approach ensures that each component is thoroughly tested before building the next layer. This reduces risk, improves quality, and provides clear progress indicators throughout the implementation.

The total time estimate of 5-7 weeks is more realistic than the original 2-3 week estimate, accounting for the inherent complexity of distributed consensus and metadata synchronization systems. This careful planning will result in a robust, reliable metadata gossip protocol that forms the foundation for WormFS's distributed operations.

The breakdown also provides natural points for code review, testing, and validation, ensuring that the final system is production-ready and maintainable.
