
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

### Networking Phases (2A-2D): Distributed Communications

#### **Phase 2A: Basic libp2p Networking (2 weeks)** INPROGRESS - see phase_2a_detailed.md
**Goal:** Establish basic peer-to-peer networking foundation

**Deliverables:**
- `networking.rs` module with:
  - libp2p swarm setup with transport encryption
  - Peer discovery using configured peer IPs
  - Basic peer authentication using peer IDs
  - Heartbeat protocol for peer liveness detection
  - Connection management and reconnection logic

**Success Criteria:**
- Can establish encrypted connections between storage nodes
- Peer authentication works with configured peer IDs
- Heartbeat detection identifies offline peers within 30 seconds
- Handles network interruptions gracefully
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/networking.rs`, `src/peer_manager.rs`

---

#### **Phase 2B: Metadata Gossip Protocol (2-3 weeks)**
**Goal:** Implement distributed metadata synchronization

**Deliverables:**
- Metadata gossip system with:
  - Master election using libp2p consensus
  - Metadata operation proposal and approval workflow
  - Event broadcasting with sequence numbers
  - Acknowledgment tracking and replay mechanisms
  - Conflict resolution for metadata operations

**Success Criteria:**
- Single master elected and maintained across network partitions
- Metadata operations propagate to all nodes reliably
- Missed operations detected and replayed correctly
- Master failover works within 10 seconds
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/gossip_protocol.rs`, `src/master_election.rs`, `src/metadata_sync.rs`

---

#### **Phase 2C: Remote Chunk Transfer (2 weeks)**
**Goal:** Enable direct chunk operations between storage nodes

**Deliverables:**
- Chunk transfer protocol with:
  - Direct libp2p streams for chunk read/write
  - Chunk request/response message formats
  - Bulk chunk migration capabilities
  - Transfer progress tracking and error handling
  - Authentication for chunk operations

**Success Criteria:**
- Can read chunks from remote storage nodes
- Can write chunks to remote storage nodes
- Bulk transfers work reliably for rebalancing
- Proper error handling for network failures
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/chunk_transfer.rs`, `src/remote_operations.rs`

---

#### **Phase 2D: Multi-Node Storage Operations (2 weeks)**
**Goal:** Coordinate stripe operations across multiple nodes

**Deliverables:**
- Distributed storage coordination with:
  - Multi-node stripe encoding and placement
  - Distributed stripe reconstruction from remote chunks
  - Chunk placement optimization across cluster
  - Basic load balancing for chunk distribution

**Success Criteria:**
- Can store stripes across multiple storage nodes
- Can reconstruct files from chunks on different nodes
- Chunk placement follows blast radius rules across nodes
- Basic cluster-wide storage balancing works
- No clippy errors or warnings
- No rust format errors or warnings.

**Key Files:** `src/distributed_storage.rs`, `src/cluster_coordinator.rs`

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

### Development Guidelines

**Testing Strategy:**
- Unit tests for each module (aim for >90% coverage)
- Integration tests for multi-component interactions
- End-to-end tests for complete workflows
- Performance benchmarks for critical paths

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
