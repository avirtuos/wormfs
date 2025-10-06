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

### **Phase 2B.4: Sequence Number Management (2-3 days)**
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

### **Phase 2B.5: Simple Master Election (3-4 days)**
**Goal:** Implement basic leader election for metadata coordination

**Context:** Metadata operations need coordination to prevent conflicts. A master node will coordinate all metadata changes. Start with a simple deterministic election before adding sophistication.

**Deliverables:**
- Simple election algorithm: lowest peer ID becomes master
- Master announcement broadcasts
- Master heartbeat to maintain leadership
- Master failure detection by peers
- Automatic re-election on master failure
- Track current master in all nodes
- Master-only operation enforcement

**Success Criteria:**
- Consistent master elected across all nodes
- Master election completes within 10 seconds of startup
- Master failure detected within 30 seconds
- Re-election completes within 15 seconds
- All nodes agree on current master
- Split-brain prevented (only one master at a time)
- No clippy errors or warnings
- No cargo formatting errors or warnings

**Test Strategy:**
- Integration test: Start 3 nodes, verify same master elected
- Integration test: Kill master, verify re-election
- Integration test: Verify new master elected within timeout
- Integration test: All nodes agree on master after election
- Chaos test: Rapidly start/stop nodes, verify stable elections
- Partition test: Network partition, verify election in majority partition
- Scale test: Election with 10+ nodes

**Files Modified:**
- `src/master_election.rs` - New election implementation
- `src/metadata_coordinator.rs` - New coordinator using master
- `src/storage_node.rs` - Integrate election
- `tests/election_tests.rs` - Election tests
- `config/storage_node.yaml` - Election configuration

---

### **Phase 2B.6: Metadata Operation Workflow (3-4 days)**
**Goal:** Implement proposal/approval workflow for metadata operations

**Context:** With master election in place, implement the workflow where nodes propose metadata changes to the master, and the master approves and broadcasts them.

**Deliverables:**
- Proposal submission from any node to master
- Master validation of proposals
- Master approval and sequence number assignment
- Master broadcasts approved operations
- Response routing back to proposer
- Operation timeout handling
- Basic conflict detection at master

**Success Criteria:**
- Non-master nodes can propose operations
- Master receives and validates proposals
- Master assigns sequence numbers to approved operations
- Approved operations broadcast to all peers
- Proposer receives confirmation of approval/rejection
- Rejected operations include reason
- Operation lifecycle completes within 5 seconds
- No clippy errors or warnings
- No cargo formatting errors or warnings

**Test Strategy:**
- Integration test: Node A proposes, master approves, all receive
- Integration test: Invalid proposal rejected with reason
- Integration test: Concurrent proposals handled correctly
- Integration test: Proposal timeout handling
- Integration test: Master change during proposal in-flight
- Performance test: Proposal throughput measurement
- Stress test: High-frequency proposals from multiple nodes

**Files Modified:**
- `src/metadata_coordinator.rs` - Proposal workflow
- `src/metadata_protocol.rs` - Add proposal/response messages
- `src/storage_node.rs` - Proposal submission API
- `tests/proposal_tests.rs` - Proposal workflow tests
- `src/metadata_store.rs` - Apply approved operations

---

### **Phase 2B.7: Acknowledgment System (2-3 days)**
**Goal:** Track acknowledgments from peers for broadcast events

**Context:** To ensure all nodes have received and processed metadata events, implement an acknowledgment system that tracks which nodes have confirmed receipt.

**Deliverables:**
- Send acknowledgments for received events
- Track acknowledgments per event at master
- Configurable acknowledgment timeout
- Retry logic for missing acknowledgments
- Event considered committed when quorum acks received
- Handle nodes that never acknowledge
- Acknowledgment status API for monitoring

**Success Criteria:**
- Peers send acknowledgments for received events
- Master tracks acknowledgments per event
- Can determine when event is fully acknowledged
- Timeout for slow/failed peers works correctly
- Quorum-based commit detection (majority of nodes)
- Integration test verifies acknowledgment flow
- No clippy errors or warnings
- No cargo formatting errors or warnings

**Test Strategy:**
- Integration test: Broadcast event, verify all nodes acknowledge
- Integration test: Delay one node's ack, verify timeout handling
- Integration test: Kill one node, verify quorum still achieved
- Integration test: Verify event marked committed after quorum
- Performance test: Acknowledgment overhead measurement
- Chaos test: Random node failures during acknowledgment
- Scale test: Acknowledgments with 10+ nodes

**Files Modified:**
- `src/metadata_protocol.rs` - Add acknowledgment messages
- `src/metadata_coordinator.rs` - Track acknowledgments
- `src/ack_tracker.rs` - New acknowledgment tracking module
- `tests/ack_tests.rs` - Acknowledgment tests
- `config/storage_node.yaml` - Acknowledgment timeout config

---

### **Phase 2B.8: Event Replay Mechanism (3-4 days)**
**Goal:** Request and replay missed events to maintain consistency

**Context:** When nodes detect gaps in sequence numbers or join the cluster, they need to request and replay missed events to catch up with cluster state.

**Deliverables:**
- Gap detection triggers replay requests
- Request specific event ranges from peers
- Batch replay for efficiency
- Order preservation during replay
- Replay from multiple peers if needed
- Catchup for newly joined nodes
- Replay progress tracking and logging

**Success Criteria:**
- Detected gaps trigger automatic replay requests
- Can request and receive event ranges from peers
- Replayed events applied in correct order
- Newly joined nodes catch up to current state
- Replay completes within reasonable time (< 30s for 1000 events)
- Handle replay failures gracefully (retry with different peer)
- No clippy errors or warnings
- No cargo formatting errors or warnings

**Test Strategy:**
- Integration test: Pause node, generate events, resume, verify catchup
- Integration test: Start new node, verify it catches up
- Integration test: Request specific range, verify correct events received
- Integration test: Replay from multiple peers if one fails
- Performance test: Catchup time for various event counts
- Stress test: Catchup under continuous new events
- Scale test: Large event log replay (10k+ events)

**Files Modified:**
- `src/metadata_protocol.rs` - Add replay request/response messages
- `src/event_replay.rs` - New replay mechanism
- `src/metadata_coordinator.rs` - Integrate replay
- `src/metadata_store.rs` - Store events for replay
- `tests/replay_tests.rs` - Replay mechanism tests

---

### **Phase 2B.9: Conflict Resolution (3-4 days)**
**Goal:** Handle concurrent operations and resolve conflicts

**Context:** Even with master coordination, conflicts can occur during network partitions or rapid concurrent operations. Implement deterministic conflict resolution.

**Deliverables:**
- Timestamp-based conflict detection
- Last-write-wins (LWW) conflict resolution
- Handle concurrent file creation/deletion
- Handle concurrent chunk placement operations
- Conflict resolution during partition healing
- Maintain consistency guarantees (eventual consistency)
- Conflict event logging for debugging

**Success Criteria:**
- Concurrent operations resolved deterministically
- Same conflict always resolves the same way on all nodes
- Timestamps provide total ordering of operations
- No data loss during conflict resolution
- Conflicts logged clearly for debugging
- Integration test verifies conflict resolution
- No clippy errors or warnings
- No cargo formatting errors or warnings

**Test Strategy:**
- Integration test: Concurrent file creation from two nodes
- Integration test: Verify deterministic resolution
- Integration test: Partition cluster, create conflicts, heal, verify resolution
- Integration test: Same-timestamp operations handled correctly
- Unit test: Conflict resolution algorithm correctness
- Chaos test: High-conflict scenarios
- Consistency test: All nodes reach same final state

**Files Modified:**
- `src/conflict_resolver.rs` - New conflict resolution module
- `src/metadata_coordinator.rs` - Integrate conflict resolution
- `src/metadata_protocol.rs` - Add conflict resolution fields
- `tests/conflict_tests.rs` - Conflict resolution tests
- `src/metadata_store.rs` - Apply conflict resolution logic

---

### **Phase 2B.10: Network Partition Handling (3-4 days)**
**Goal:** Detect and handle network partitions gracefully

**Context:** Network partitions are inevitable in distributed systems. Implement detection and handling to prevent data corruption and ensure recovery when the partition heals.

**Deliverables:**
- Detect network partitions via heartbeat monitoring
- Prevent split-brain (only majority partition operates)
- Minority partition becomes read-only
- Track diverged state during partition
- Merge metadata after partition healing
- Prevent operations in minority partition
- Partition detection logging and alerting

**Success Criteria:**
- Partitions detected within 30 seconds
- Only majority partition continues metadata operations
- Minority partition rejects new operations
- Partition healing detected and reconciliation starts
- Metadata converges after partition heals
- No data corruption or loss during partition
- No clippy errors or warnings
- No cargo formatting errors or warnings

**Test Strategy:**
- Integration test: Create partition (2 nodes vs 1 node)
- Integration test: Verify majority continues, minority blocks
- Integration test: Heal partition, verify reconciliation
- Integration test: Verify all nodes reach consistent state after healing
- Chaos test: Multiple partitions in sequence
- Chaos test: Partition during active operations
- Edge case test: Exactly split cluster (2 vs 2)

**Files Modified:**
- `src/partition_detector.rs` - New partition detection module
- `src/metadata_coordinator.rs` - Partition handling logic
- `src/master_election.rs` - Partition-aware election
- `tests/partition_tests.rs` - Partition handling tests
- `config/storage_node.yaml` - Partition detection config

---

### **Phase 2B.11: Integration and Testing (3-4 days)**
**Goal:** Comprehensive testing of complete metadata gossip system

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
