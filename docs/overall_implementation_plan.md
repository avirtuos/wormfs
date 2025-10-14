# WormFS Overall Implementation Plan - Iterative Delivery Approach

## Overview
This plan outlines the recommended implementation order for WormFS components, prioritizing iterative delivery of a working end-to-end system. The approach focuses on building a minimal viable path through the system first, then adding complexity and robustness in subsequent phases.

## Implementation Phases

### Phase 1: Minimal Data Path (Weeks 1-3)
**Goal: Store and retrieve a single file without consensus or distribution**

1. **MetadataStore** (minimal)
   - Basic SQLite schema
   - File and stripe CRUD operations
   - No transactions or snapshots yet

2. **FileStore** (minimal)
   - Simple chunk storage to local disk
   - Basic erasure coding (Reed-Solomon)
   - Single node operation only
   - No 2PC or staging yet

3. **FileSystemService** (minimal)
   - Basic FUSE operations (create, read, write, stat)
   - Direct integration with MetadataStore and FileStore
   - No locking or concurrent access yet

4. **StorageNode** (minimal)
   - Wire up the three components
   - Basic configuration loading
   - Simple main() entry point

**Milestone**: Can mount filesystem and create/read/write files on a single node

---

### Phase 2: Add Consensus Layer (Weeks 4-6)
**Goal: Metadata consistency across multiple nodes**

5. **StorageNetwork**
   - libp2p setup for node discovery
   - Basic peer-to-peer communication
   - RPC foundation for Raft

6. **TransactionLogStore**
   - Raft log persistence with redb
   - Basic append and read operations

7. **StorageRaftMember**
   - OpenRaft integration
   - Leader election
   - Log replication
   - Metadata operations through consensus

8. **Update MetadataStore**
   - Add transaction support
   - Integrate with Raft state machine
   - Add prepare/commit/abort operations

**Milestone**: Multiple nodes maintain consistent metadata through Raft

---

### Phase 3: Distributed Storage (Weeks 7-9)
**Goal: Distribute chunks across nodes**

9. **StorageEndpoint**
   - gRPC server for client connections
   - Node-to-node chunk transfer APIs
   - Basic authentication

10. **Update FileStore**
    - Implement 2PC for chunk operations
    - Add chunk staging/activation
    - Remote chunk fetching
    - Chunk placement logic

11. **Update FileSystemService**
    - Add file locking
    - Stripe coordination across nodes
    - Read-modify-write for partial stripes

**Milestone**: Files are erasure-coded and distributed across cluster

---

### Phase 4: Robustness & Recovery (Weeks 10-11)
**Goal: Handle failures gracefully**

12. **SnapshotStore**
    - Metadata snapshot creation
    - Snapshot transfer between nodes
    - Log compaction support

13. **StorageWatchdog**
    - Chunk verification (shallow & deep)
    - Missing chunk detection
    - Basic repair coordination
    - Orphan cleanup

14. **Update StorageRaftMember**
    - Snapshot coordination
    - Membership changes (add/remove nodes)
    - Transaction recovery

**Milestone**: System recovers from node failures and maintains data integrity

---

### Phase 5: Observability & Testing (Week 12)
**Goal: Production readiness**

15. **MetricService**
    - Prometheus metrics
    - Health endpoints
    - Performance monitoring

16. **WormValidator**
    - End-to-end test scenarios
    - Chaos testing
    - Performance benchmarks

17. **StorageNode** (complete)
    - Graceful shutdown
    - Component lifecycle management
    - Production configuration

**Milestone**: System is observable, testable, and production-ready

---

## Key Implementation Principles

1. **Stub Early, Implement Later**: Use stub implementations for complex features initially
2. **Single Node First**: Get everything working on one node before adding distribution
3. **Metadata Before Data**: Focus on metadata consistency before chunk distribution
4. **Read Path Before Write Path**: Implement read operations before complex write paths
5. **Manual Before Automatic**: Manual recovery before automatic healing

## Alternative Paths to Consider

### If prioritizing early user feedback:
- Move StorageEndpoint earlier (after Phase 1) for basic client API
- Implement a simple CLI client alongside Phase 1

### If prioritizing data safety:
- Move SnapshotStore to Phase 2 (right after Raft)
- Implement basic StorageWatchdog verification in Phase 3

### If prioritizing performance testing:
- Implement basic MetricService in Phase 2
- Add simple benchmarking throughout each phase

## Minimizing Throwaway Work

This plan minimizes throwaway work by:
- Building on each component incrementally
- Using interfaces/traits defined in scaffolding
- Adding complexity gradually to working systems
- Keeping test coverage from day one

## Success Metrics per Phase

| Phase | Success Criteria |
|-------|-----------------|
| 1 | Single-node file operations work via FUSE mount |
| 2 | 3-node cluster maintains consistent metadata |
| 3 | Files distribute across nodes with erasure coding |
| 4 | System recovers from single node failure |
| 5 | 95% test coverage, <1s latency for small files |

## Risk Mitigation

### Technical Risks:
- **FUSE complexity**: Start with read-only operations if write proves difficult
- **Erasure coding performance**: Begin with replication, add erasure coding later
- **Raft integration**: Use raft-rs example code as reference implementation

### Schedule Risks:
- **12-week timeline aggressive**: Each phase has "must have" vs "nice to have" features
- **Testing may reveal issues**: Budget 20% time for bug fixes and refactoring
- **Integration complexity**: Maintain integration tests from Phase 1

## Next Steps

1. Create detailed task breakdown for Phase 1
2. Set up development environment with test infrastructure
3. Implement MetadataStore SQLite schema
4. Begin weekly progress reviews

## Notes

- This plan assumes full-time development effort
- Actual timeline may vary based on complexity discoveries
- Each phase should produce working software that can be demoed
- Documentation should be maintained throughout, not just at the end