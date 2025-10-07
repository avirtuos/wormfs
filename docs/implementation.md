
## Implementation Planning

### Development Philosophy

This implementation plan breaks down WormFS development into small, manageable phases of 1-3 weeks each. Each phase produces working, testable components with clear success criteria. This approach allows for:

- **Steady Progress**: Frequent wins and measurable advancement
- **Thorough Testing**: Each component validated before moving forward  
- **Flexibility**: Easy to pivot or adjust based on learnings
- **Maintainable Motivation**: Regular completions and working software

#### Coding Standards

- Ensure Clippy has no errors or warnings using `cargo clippy --all-targets --all-features -- -D warnings` command/.
- Ensure all tests past
- Ensure Cargo Formatting is applied using `cargo fmt`
- Ensure no cargo build errors or warnings

### Foundation Phases (0A-0D): Core Building Blocks

#### **Phase 0A: Chunk Format Foundation (1-2 weeks)** COMPLETED
**Goal:** Create the basic chunk file format with headers and validation

**Deliverables:**
- `chunk_format.rs` module with:
  - Variable-length binary header struct with CRC32 checksum, chunk_id, stripe_id, file_id, byte offsets, erasure details
  - Header serialization/deserialization (with versioning support)
  - Chunk write/read functions that handle header + data
  - Unit tests for header parsing and chunk integrity validation

**Success Criteria:**
- Can create a chunk file with proper header
- Can read chunk file and validate header integrity
- Can detect corrupted headers and data
- 100% test coverage on chunk format operations
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/chunk_format.rs`, `tests/chunk_format_tests.rs`

---

#### **Phase 0B: Erasure Coding Abstraction (1-2 weeks)** COMPLETED
**Goal:** Wrap reed-solomon-erasure crate with WormFS-specific interface

**Deliverables:**
- `erasure_coding.rs` module with:
  - Configuration struct (k data shards, m parity shards, stripe size)
  - `encode_stripe()` function: takes byte slice → returns Vec of chunk data
  - `decode_stripe()` function: takes partial chunks → reconstructs stripe
  - Error handling for insufficient chunks, corruption, etc.
  - Unit tests with various failure scenarios (missing chunks, corrupt chunks)

**Success Criteria:**
- Can encode test data into k+m chunks
- Can reconstruct original data from any k chunks
- Proper error handling when too few chunks available
- Benchmarks for encoding/decoding performance
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/erasure_coding.rs`, `tests/erasure_tests.rs`, `benches/erasure_bench.rs`

---

#### **Phase 0C: Metadata Storage Foundation (1-2 weeks)** COMPLETED
**Goal:** Create sqlite abstraction for WormFS metadata operations

**Deliverables:**
- `metadata_store.rs` module with:
  - File metadata CRUD operations (create, read, update, delete)
  - Chunk metadata CRUD operations (chunk location tracking)
  - Stripe metadata operations (stripe-to-chunks mapping)
  - Database schema versioning support
  - Unit tests with mock data

**Success Criteria:**
- Can store and retrieve file metadata (permissions, size, path)
- Can track which storage node has which chunks
- Can map stripes to their constituent chunks
- Database survives process restart with data intact
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/metadata_store.rs`, `tests/metadata_tests.rs`

---

#### **Phase 0D: Basic Storage Layout (1 week)** COMPLETED
**Goal:** Implement local chunk storage organization

**Deliverables:**
- `storage_layout.rs` module with:
  - Chunk folder hashing (10-char alphanumeric hash)
  - 1000 top-level folder distribution
  - Chunk index file format and operations
  - Basic chunk placement logic (single disk for now)
  - Directory creation and cleanup functions

**Success Criteria:**
- Can determine correct storage path for any file
- Can create chunk folders and index files
- Can list all chunks for a given file
- No hash collisions in test scenarios
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/storage_layout.rs`, `tests/storage_layout_tests.rs`

### Single Node Phases (1A-1C): Local Storage Operations

#### **Phase 1A: Single-Node Storage Node (2 weeks)** COMPLETED
**Goal:** Combine all Phase 0 components into basic storage node

**Deliverables:**
- `storage_node.rs` main structure with:
  - Configuration loading (YAML/TOML config file)
  - Metadata database initialization
  - Local chunk storage/retrieval operations
  - Basic stripe encode/decode/store workflows
  - CLI interface for basic operations (store file, retrieve file)

**Success Criteria:**
- Can store a file by breaking it into stripes and chunks
- Can retrieve and reconstruct complete file from chunks  
- Survives restart with metadata intact
- Command-line tools work for basic file operations
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/storage_node.rs`, `src/cli.rs`, `config/storage_node.yaml`

---

#### **Phase 1B: Local File Operations (1-2 weeks)** COMPLETED
**Goal:** Add comprehensive file management operations

**Deliverables:**
- Extend storage node with:
  - File deletion (remove chunks and metadata)
  - File listing and metadata queries
  - Basic integrity checking (validate chunk checksums)
  - Configuration for stripe size, k+m parameters
  - Logging and basic error handling

**Success Criteria:**
- Can delete files completely (chunks + metadata)
- Can list stored files with their metadata
- Can detect and report corrupted chunks
- Configurable erasure coding parameters work correctly
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/file_operations.rs`, `src/integrity_checker.rs`

---

#### **Phase 1C: Multi-Disk Support (1-2 weeks)** COMPLETED
**Goal:** Add support for multiple storage devices per node

**Deliverables:**
- Extend storage layout with:
  - Multi-disk configuration and detection
  - Chunk placement across disks (max 1 chunk per stripe per disk)
  - Disk space monitoring and balancing
  - Disk failure handling (mark disk offline, continue operations)

**Success Criteria:**
- Can utilize multiple disks for chunk storage
- Proper blast radius protection (no stripe has >1 chunk per disk)
- Balances storage across available disks by free space
- Gracefully handles individual disk failures
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/disk_manager.rs`, `src/chunk_placement.rs`

### Networking Phases (2A-2D): Distributed Communications with Raft

You can find examples of how to use openraft v0.9+ APIs in the crate's github repository at https://github.com/databendlabs/openraft/tree/release-0.9/examples/raft-kv-rocksdb

#### **Phase 2A: Raft Storage Components & Unit Testing (1 week)** COMPLETED
**Goal:** Implement and test Raft storage components independently, defer cluster testing to Phase 2B

**Background & Rationale:**
After reviewing OpenRaft v0.9 examples and API patterns, we determined that implementing a channel-based local network for testing is overly complex due to OpenRaft's specific error type requirements. The API uses nested generic error types with Node type parameters that make mocked network implementations difficult to maintain. Instead, Phase 2A focuses on validating storage correctness through unit tests, while cluster integration testing is deferred to Phase 2B when we implement actual network transport.

**Key Learnings from OpenRaft v0.9:**
- Use `openraft::declare_raft_types!` macro instead of manual RaftTypeConfig impl
- Error types use nested generics: `RaftError<NID, E>` and `RPCError<NID, Node, RaftError<E>>`
- Clear separation between RaftLogStorage (redb) and RaftStateMachine (SQLite) traits
- Real network transports are simpler than channel-based mocking
- Unit tests provide more value than complex integration mocks

**Deliverables:**
- `raft/` module structure with:
  - `types.rs`: Metadata operations and Raft type declarations using `declare_raft_types!` macro ✅
  - `log_store.rs`: RaftLogStorage implementation using redb ✅
  - `state_machine.rs`: RaftStateMachine integrated with MetadataStore
  - `config.rs`: Raft configuration structs ✅
  - `storage.rs`: Re-exports for storage components ✅
  - `mod.rs`: Module organization ✅
- MetadataStore integration in StateMachine with:
  - Actual database operations for all metadata ops
  - Snapshot creation from SQLite state
  - Proper error handling and transactions
- Comprehensive unit test suite (`tests/raft_unit_tests.rs`):
  - LogStore operations (append, truncate, purge, vote persistence)
  - StateMachine apply operations
  - Snapshot creation and restoration
  - Configuration validation
- Deferred to Phase 2B:
  - Network transport implementation (libp2p or TCP)
  - Node manager with cluster coordination
  - Multi-node integration tests
  - Leader election testing
  - Log replication across nodes

**Success Criteria:**
- LogStore passes all unit tests independently
- StateMachine correctly applies operations to MetadataStore
- Snapshot creation serializes SQLite state properly
- Snapshot restoration rebuilds state correctly
- All configuration validates properly
- 90%+ test coverage on Raft components
- No clippy errors or warnings
- No rust format errors or warnings

**Key Files:** `src/raft/mod.rs`, `src/raft/types.rs`, `src/raft/log_store.rs`, `src/raft/state_machine.rs`, `src/raft/config.rs`, `src/raft/storage.rs`, `tests/raft_unit_tests.rs`

**Raft Configuration (in storage_node.yaml):**
```yaml
raft:
  node_id: 1
  heartbeat_interval: 250ms
  election_timeout_min: 1000ms
  election_timeout_max: 2000ms
  snapshot_interval_hours: 24
  snapshot_log_size_mb: 10
  use_lease_reads: true
  lease_duration: 5s
  log_path: "./data/raft_log"
  state_path: "./data/metadata"
```

---

#### **Phase 2B: Network Transport & Cluster Testing (2-3 weeks)** *(Expanded)*
**Goal:** Implement network transport for Raft and validate cluster operations

**Deliverables:**
- `raft/node.rs` - Node manager with cluster coordination:
  - Raft instance initialization and lifecycle management
  - Message routing and handling
  - Leader discovery and client routing
  - Cluster membership management
- Network transport implementation:
  - `raft/libp2p_network.rs` - Full libp2p implementation:
    - Custom RaftNetwork trait implementation over libp2p
    - Request-response protocol for Raft RPCs
    - Peer discovery and connection management
    - Transport encryption using noise protocol
    - Connection pooling and automatic reconnection
- `raft/peer_manager.rs` for health monitoring and failover
- Integration tests with actual network transport (`tests/raft_integration_tests.rs`):
  - 3-node cluster formation
  - Leader election within 2 seconds
  - Log replication across nodes
  - Metadata operations end-to-end
  - Network partition scenarios
  - Node failure and recovery

**Success Criteria:**
- Can establish Raft cluster with 3+ nodes over real network
- Leader election completes within 2 seconds
- Metadata operations replicate to majority before commit
- Log replication works reliably across nodes
- Network partitions handled correctly (majority progresses, minority stalls)
- Automatic reconnection on connection failure
- All integration tests pass
- No clippy errors or warnings
- No rust format errors or warnings

**Key Files:** `src/raft/node.rs`, `src/raft/libp2p_network.rs` (or `tcp_network.rs`), `src/raft/peer_manager.rs`, `proto/wormfs.proto`, `tests/raft_integration_tests.rs`

**Protobuf Additions:**
```protobuf
message AppendEntriesRequest {
    uint64 term = 1;
    uint64 leader_id = 2;
    uint64 prev_log_index = 3;
    uint64 prev_log_term = 4;
    repeated LogEntry entries = 5;
    uint64 leader_commit = 6;
}

message VoteRequest {
    uint64 term = 1;
    uint64 candidate_id = 2;
    uint64 last_log_index = 3;
    uint64 last_log_term = 4;
}

message InstallSnapshotRequest {
    uint64 term = 1;
    uint64 leader_id = 2;
    uint64 last_included_index = 3;
    uint64 last_included_term = 4;
    bytes data = 5;
}
```

---

#### **Phase 2C: Metadata Operations via Raft (2 weeks)**
**Goal:** Route all metadata operations through Raft consensus with optimizations

**Deliverables:**
- `raft/client.rs` with proposal and query interface:
  - `propose()`: Submit metadata operations to Raft leader
  - `read()`: Linearizable reads through Raft
  - `read_lease()`: Optimized lease-based reads
  - `read_stale()`: Local reads for minority partitions
  - Leader discovery and automatic redirection
- Lease-based read optimization implementation
- Snapshot management with configurable triggers (time/size)
- Async snapshot creation to avoid blocking consensus
- Compressed snapshot transfer for new nodes
- End-to-end tests for all metadata operations

**Success Criteria:**
- All metadata operations go through Raft consensus
- Lease-based reads work without consensus overhead
- Minority partitions serve stale reads correctly
- Snapshots triggered by time (24h) and size (10MB) thresholds
- New nodes catch up via snapshot + log replay
- Write operations require majority quorum
- Linearizable consistency guaranteed for writes
- No clippy errors or warnings
- No rust format errors or warnings

**Key Files:** `src/raft/client.rs`, `src/raft/snapshot.rs`, `tests/raft_metadata_tests.rs`

**Read Mode Types:**
```rust
pub enum ReadMode {
    Linearizable,  // Goes through Raft, requires majority
    LeaseRead,     // Uses leader lease, requires majority  
    StaleRead,     // Local read, works in minority partition
}
```

---

#### **Phase 2D: Chunk Coordination with Raft (1 week)**
**Goal:** Coordinate chunk operations through Raft leader

**Deliverables:**
- Update chunk placement decisions to go through Raft proposals
- Leader-coordinated rebalancing operations
- Direct libp2p streams for chunk transfer (unchanged from original design)
- Integration with Raft client for chunk metadata updates
- Recovery operations initiated via Raft consensus

**Success Criteria:**
- Chunk placement decisions coordinated through Raft
- Direct chunk transfers work efficiently via libp2p
- Rebalancing operations proposed through Raft
- Chunk metadata updates replicated consistently
- Recovery operations coordinated by Raft leader
- No clippy errors or warnings
- No rust format errors or warnings

**Key Files:** `src/chunk_transfer.rs`, `src/distributed_storage.rs`, `tests/chunk_coordination_tests.rs`

**Note:** Chunk data transfer still uses direct libp2p streams for efficiency. Only chunk metadata operations (location tracking, placement decisions) go through Raft.

### Client Integration Phases (3A-3C): FUSE Filesystem

#### **Phase 3A: gRPC API Foundation (2 weeks)**
**Goal:** Implement client-storage communication protocol

**Deliverables:**
- gRPC service definitions with:
  - Filesystem operation APIs (create, read, write, delete, list)
  - Metadata operation APIs (permissions, attributes)
  - Configuration APIs (storage policies, cluster status)
  - Authentication using TLS 1.3 with PSK
  - Protocol buffer message definitions

**Success Criteria:**
- Complete gRPC API specification
- Authentication and encryption working
- All filesystem operations accessible via gRPC
- API documentation and examples
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `proto/wormfs.proto`, `src/grpc_server.rs`, `src/grpc_client.rs`

---

#### **Phase 3B: Basic FUSE Implementation (2-3 weeks)**
**Goal:** Create FUSE filesystem client

**Deliverables:**
- FUSE client with:
  - Basic filesystem operations (open, read, write, close)
  - Directory operations (list, create, delete)
  - File metadata operations (stat, chmod, etc.)
  - Connection management to storage nodes
  - Basic caching for metadata and small reads

**Success Criteria:**
- Can mount WormFS as a FUSE filesystem
- Basic file operations work through standard tools (ls, cat, cp)
- Directory operations function correctly
- Metadata operations reflect proper file attributes
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/fuse_client.rs`, `src/fuse_operations.rs`

---

#### **Phase 3C: Advanced FUSE Features (2 weeks)**
**Goal:** Add production-ready FUSE capabilities

**Deliverables:**
- Enhanced FUSE client with:
  - File locking (read/write locks with timeouts)
  - Advanced caching strategies
  - Concurrent operation handling
  - Error recovery and reconnection
  - Performance optimizations for large files

**Success Criteria:**
- File locking prevents concurrent write conflicts
- Good performance for typical file operations
- Handles storage node failures gracefully
- Concurrent operations work correctly
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/file_locking.rs`, `src/cache_manager.rs`, `src/performance_optimizer.rs`

### Advanced Features (4A+): Production Capabilities

#### **Phase 4A: Data Integrity & Recovery (2-3 weeks)**
**Goal:** Implement comprehensive data protection

**Deliverables:**
- Data integrity system with:
  - Shallow integrity checking (chunk existence verification)
  - Deep integrity checking (checksum validation and stripe reconstruction)
  - Automatic corruption detection and repair
  - Background scrubbing processes
  - Recovery coordination across cluster

**Success Criteria:**
- Detects missing or corrupted chunks quickly
- Automatically repairs recoverable data corruption
- Background scrubbing runs without impacting performance
- Recovery processes coordinate properly across nodes
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/integrity_checker.rs`, `src/data_recovery.rs`, `src/scrubber.rs`

---

#### **Phase 4B: Administrative Interface (2 weeks)**
**Goal:** Create web-based management interface

**Deliverables:**
- Web UI and REST API with:
  - Cluster status monitoring and visualization
  - Node management (add, remove, configure)
  - Storage policy management
  - Data integrity status and repair operations
  - Performance metrics and alerting

**Success Criteria:**
- Complete cluster visibility through web interface
- Can manage cluster configuration through UI
- Real-time monitoring of cluster health
- Administrative operations work reliably
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/web_ui.rs`, `src/rest_api.rs`, `web/` directory

---

#### **Phase 4C: Performance Optimization (2-3 weeks)**
**Goal:** Optimize system performance for production workloads

**Deliverables:**
- Performance enhancements with:
  - Advanced caching strategies (client and storage node)
  - Connection pooling and multiplexing
  - Parallel chunk operations
  - Memory usage optimization
  - Network bandwidth optimization

**Success Criteria:**
- Significant performance improvements in benchmarks
- Efficient memory usage under load
- Good scalability characteristics
- Network bandwidth used efficiently
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/performance/`, `benches/`, performance test suite


#### **Phase 5: Clean Ups & Optimizations (2-3 weeks)**
**Goal:** Adress clean up tasks identified along the way

**Deliverables:**
- Performance enhancements with:
  - MetadataStore::list_files() should support streaming or paginating results to reduce memory pressure to hold all files in memory at once. This is especially relevant for integrity check activities that walk all files.

**Success Criteria:**
- Its possible to process the list of all files in a paginated fashion without holding all files in memory at the same time and without running the sqlite query repeatedly.
- No clippy errors or warnings
- No rust format errors or warnings.


### Future Phases (6A+): Production Readiness

Additional phases will cover:
- **Security hardening** (authentication, authorization, audit logging)
- **Monitoring and observability** (metrics, tracing, alerting)
- **Deployment automation** (Docker, Kubernetes, configuration management)
- **Documentation and guides** (user manual, deployment guide, API docs)
- **Testing infrastructure** (integration tests, chaos testing, performance tests)

### Migration Strategy

**Note on Migration:** The OpenRaft integration represents a fundamental architectural change to how WormFS handles metadata consensus. This design decision was made early in the project's development (Phase 2), so no migration path from a previous gossip-based system is needed.

**For Future Reference:**
If migration becomes necessary in the future (e.g., from one Raft implementation to another), the following approach would be recommended:

1. **Snapshot-Based Migration:**
   - Create a final snapshot of the source system's metadata state
   - Initialize new Raft cluster with this snapshot as the initial state
   - Verify metadata integrity post-migration
   - Run both systems in parallel during transition period

2. **Compatibility Layers:**
   - Maintain read compatibility with old metadata format
   - Gradually transition write operations to new system
   - Provide rollback capability during migration window

3. **Testing Strategy:**
   - Full cluster migration testing in development environment
   - Gradual rollout with canary deployments
   - Comprehensive validation of metadata consistency
   - Performance benchmarking pre/post migration

**Current Implementation Approach:**
Since we're implementing Raft from the start of Phase 2:
- No legacy gossip protocol to migrate from
- Clean slate implementation of Raft consensus
- Focus on getting the Raft implementation right from the beginning
- Build Phase 2 components incrementally as outlined in the plan

### Development Guidelines

**Testing Strategy:**
- Unit tests for each module (aim for >90% coverage)
- Integration tests for multi-component interactions
- End-to-end tests for complete workflows
- Performance benchmarks for critical paths
- Raft-specific tests for consensus scenarios (leader election, log replication, network partitions)

**Code Quality:**
- Use `cargo clippy` for linting
- Use `cargo fmt` for consistent formatting
- Document all public APIs with rustdoc
- Follow Rust best practices and idioms

**Dependency Management:**
- Minimize external dependencies where possible
- Pin dependency versions for reproducible builds
- Regular security audits of dependencies
- Consider alternatives for heavy dependencies

**Raft Implementation Guidelines:**
- Keep Raft types simple and focused on metadata operations
- Separate concerns: log storage (redb) vs state machine (SQLite)
- Test with local channels first before adding network complexity
- Comprehensive testing of failure scenarios (node failures, partitions)
- Monitor performance impact of consensus operations
