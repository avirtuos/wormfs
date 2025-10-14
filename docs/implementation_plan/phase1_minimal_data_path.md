# Phase 1: Minimal Data Path - Detailed Implementation Plan

## Overview
**Duration**: 3 weeks (15 working days)
**Goal**: Create a working single-node filesystem that can store and retrieve files using FUSE mount
**Success Criteria**: Can mount filesystem, create directories, write files, read files, and list contents on a single node

## Component Implementation Order

### MetadataStore (Days 1-5)

#### Step 1: SQLite Schema Design & Setup
**File**: `src/metadata_store/implementation.rs`

**Tasks**:
1. Design and implement SQLite schema
   ```sql
   -- Core tables needed for Phase 1
   CREATE TABLE files (
       file_id INTEGER PRIMARY KEY,
       inode INTEGER UNIQUE NOT NULL,
       parent_inode INTEGER,
       name TEXT NOT NULL,
       file_type INTEGER NOT NULL, -- 0=file, 1=directory
       size INTEGER NOT NULL,
       mode INTEGER NOT NULL,
       uid INTEGER NOT NULL,
       gid INTEGER NOT NULL,
       atime INTEGER NOT NULL,
       mtime INTEGER NOT NULL,
       ctime INTEGER NOT NULL,
       UNIQUE(parent_inode, name)
   );

   CREATE TABLE stripes (
       stripe_id INTEGER PRIMARY KEY,
       file_id INTEGER NOT NULL,
       stripe_index INTEGER NOT NULL,
       offset INTEGER NOT NULL,
       size INTEGER NOT NULL,
       FOREIGN KEY (file_id) REFERENCES files(file_id),
       UNIQUE(file_id, stripe_index)
   );

   CREATE TABLE chunks (
       chunk_id INTEGER PRIMARY KEY,
       stripe_id INTEGER NOT NULL,
       chunk_index INTEGER NOT NULL,
       node_id INTEGER NOT NULL,
       disk_id INTEGER NOT NULL,
       path TEXT NOT NULL,
       size INTEGER NOT NULL,
       checksum TEXT,
       FOREIGN KEY (stripe_id) REFERENCES stripes(stripe_id)
   );
   ```

2. Implement database connection pool using `sqlx`
3. Add migration support for schema versioning
4. Create test database for unit tests

**Deliverables**:
- Working SQLite database initialization
- Schema migration system
- Connection pool management

#### Step 2: Basic CRUD Operations
**Methods to implement**:
```rust
impl MetadataStore for MetadataStoreImpl {
    // File operations
    async fn create_file(&self, parent_inode: u64, name: &str, mode: u32) -> Result<FileMetadata>;
    async fn get_file_by_inode(&self, inode: u64) -> Result<FileMetadata>;
    async fn update_file(&self, inode: u64, updates: FileUpdates) -> Result<()>;
    async fn delete_file(&self, inode: u64) -> Result<()>;

    // Directory operations
    async fn create_directory(&self, parent_inode: u64, name: &str, mode: u32) -> Result<FileMetadata>;
    async fn list_directory(&self, inode: u64) -> Result<Vec<DirEntry>>;

    // Stripe operations
    async fn add_stripe(&self, file_id: FileId, stripe: StripeMetadata) -> Result<()>;
    async fn get_stripes(&self, file_id: FileId) -> Result<Vec<StripeMetadata>>;

    // Chunk operations (simplified for Phase 1)
    async fn add_chunk(&self, stripe_id: StripeId, chunk: ChunkMetadata) -> Result<()>;
    async fn get_chunks(&self, stripe_id: StripeId) -> Result<Vec<ChunkMetadata>>;
}
```

**Testing**:
- Unit tests for each CRUD operation
- Test parent-child relationships
- Test unique constraints

#### Step 3: Integration & Testing

**Tasks**:
1. Create comprehensive test suite
2. Add error handling for common cases
3. Performance testing with 1000+ files
4. Documentation of API

**Deliverables**:
- Fully tested MetadataStore with 90%+ coverage
- Performance benchmarks
- API documentation

---

### FileStore (Days 6-8)

#### Step 4: Local Chunk Storage
**File**: `src/file_store/implementation.rs`

**Tasks**:
1. Implement local disk storage layout
   ```
   /data/<disk_id>/Hash(file_id) % 2000/file_id/
   ├── stripe_0000/
   │   ├── chunk_0.dat
   │   ├── chunk_1.dat
   │   └── chunk_2.dat
   └── stripe_0001/
       ├── chunk_0.dat
       └── chunk_1.dat
   ```

2. Implement chunk write operations
   ```rust
   impl FileStore for FileStoreImpl {
       async fn write_chunk_local(&self, chunk_id: ChunkId, data: &[u8]) -> Result<ChunkLocation>;
       async fn read_chunk_local(&self, chunk_id: ChunkId) -> Result<Vec<u8>>;
       async fn delete_chunk_local(&self, chunk_id: ChunkId) -> Result<()>;
   }
   ```

3. Add checksum verification (CRC32)
4. Implement simple disk space management

**Deliverables**:
- Working local chunk storage
- Checksum verification on read/write
- Basic disk space tracking

#### Step 5: Basic Erasure Coding

**Tasks**:
1. Integrate `reed-solomon` crate
2. Implement stripe encoding
   ```rust
   async fn encode_stripe(&self, data: Vec<u8>, policy: StoragePolicy) -> Result<Vec<ChunkData>> {
       // For Phase 1: Simple 2+1 encoding (2 data, 1 parity)
       let encoder = ReedSolomon::new(2, 1)?;
       let chunks = encoder.encode(data)?;
       Ok(chunks)
   }
   ```

3. Implement stripe reconstruction
   ```rust
   async fn decode_stripe(&self, chunks: Vec<ChunkData>, policy: StoragePolicy) -> Result<Vec<u8>> {
       // Reconstruct from available chunks
       let decoder = ReedSolomon::new(2, 1)?;
       let data = decoder.decode(chunks)?;
       Ok(data)
   }
   ```

4. Handle partial stripe writes (read-modify-write)

**Deliverables**:
- Working Reed-Solomon encoding/decoding
- Configurable data/parity ratio
- Partial stripe update support

#### Step 6: FileStore Integration

**Tasks**:
1. Implement high-level stripe operations
   ```rust
   impl FileStore for FileStoreImpl {
       async fn write_stripe(&self, file_id: FileId, stripe_id: StripeId, data: Vec<u8>) -> Result<StripeMetadata>;
       async fn read_stripe(&self, file_id: FileId, stripe_id: StripeId) -> Result<Vec<u8>>;
   }
   ```

2. Coordinate with MetadataStore for chunk tracking
3. Add error handling and recovery for missing chunks
4. Create comprehensive test suite

**Deliverables**:
- Complete FileStore implementation
- Integration with MetadataStore
- Test coverage 85%+

---

### Week 2, Days 4-5 & Week 3, Days 1-2: FileSystemService (Days 9-12)

#### Step 7: FUSE Integration Setup
**File**: `src/filesystem_service/implementation.rs`

**Tasks**:
1. Set up `fuser` crate integration
2. Implement basic FUSE trait
   ```rust
   impl fuser::Filesystem for FileSystemServiceImpl {
       fn init(&mut self, _req: &Request) -> Result<(), c_int>;
       fn destroy(&mut self);
       fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry);
       fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr);
   }
   ```

3. Implement inode allocation and management
4. Create mount/unmount utilities

**Deliverables**:
- Basic FUSE mount working
- Can stat root directory
- Clean mount/unmount

#### Step 8: File Operations
**Tasks**:
1. Implement file creation and deletion
   ```rust
   fn create(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, flags: u32, reply: ReplyCreate);
   fn unlink(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty);
   ```

2. Implement file read operations
   ```rust
   fn read(&mut self, req: &Request, ino: u64, fh: u64, offset: i64, size: u32, reply: ReplyData);
   ```

3. Implement file write operations
   ```rust
   fn write(&mut self, req: &Request, ino: u64, fh: u64, offset: i64, data: &[u8], flags: u32, reply: ReplyWrite);
   ```

4. Handle file truncation and size changes

**Deliverables**:
- Can create and delete files
- Can read and write file contents
- Proper size tracking

#### Step 9: Directory Operations
**Tasks**:
1. Implement directory operations
   ```rust
   fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry);
   fn rmdir(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty);
   fn readdir(&mut self, req: &Request, ino: u64, fh: u64, offset: i64, reply: ReplyDirectory);
   ```

2. Implement path traversal and lookup
3. Add permission checking (basic)
4. Handle special entries (., ..)

**Deliverables**:
- Full directory support
- Can traverse directory tree
- Proper permission handling

---

### Week 3, Days 3-4: StorageNode Integration (Days 13-14)

#### Step 10: Component Wiring
**File**: `src/storage_node/mod.rs`

**Tasks**:
1. Create StorageNode struct with components
   ```rust
   pub struct StorageNode {
       metadata_store: MetadataStoreImpl,
       file_store: FileStoreImpl,
       filesystem_service: FileSystemServiceImpl,
       config: StorageNodeConfig,
   }
   ```

2. Implement initialization sequence
   ```rust
   impl StorageNode {
       pub async fn new(config: StorageNodeConfig) -> Result<Self> {
           // Initialize components in order
           let metadata_store = MetadataStoreImpl::new(&config.metadata)?;
           let file_store = FileStoreImpl::new(&config.file_store)?;
           let filesystem_service = FileSystemServiceImpl::new(
               metadata_store.clone(),
               file_store.clone(),
           )?;
           Ok(Self { ... })
       }
   }
   ```

3. Add configuration loading from TOML
4. Create main binary entry point

**Deliverables**:
- Working StorageNode initialization
- Configuration management
- Clean startup/shutdown

#### Step 11: Basic CLI and Configuration
**Tasks**:
1. Create command-line interface
   ```bash
   wormfs --config config.toml --mount-point /mnt/wormfs
   ```

2. Add configuration validation
3. Implement graceful shutdown (Ctrl+C)
4. Add basic logging with `tracing`

**Configuration file example**:
```toml
[metadata]
path = "/var/lib/wormfs/metadata.db"

[file_store]
data_path = "/var/lib/wormfs/chunks"
stripe_size = 1048576  # 1MB
data_shards = 2
parity_shards = 1

[filesystem]
mount_point = "/mnt/wormfs"
uid = 1000
gid = 1000
```

**Deliverables**:
- Complete CLI interface
- Configuration management
- Clean shutdown handling

---

### Step 12: Testing & Documentation (Day 15)

#### Integration Testing
**File**: `tests/integration/phase1_test.rs`

**Test Scenarios**:
1. Mount filesystem and perform basic operations
2. Create nested directory structure
3. Write and read files of various sizes
4. Verify erasure coding (corrupt a chunk, still readable)
5. Performance test: Write/read 100MB file
6. Stress test: Create 1000 files

**Test Script Example**:
```bash
#!/bin/bash
# Mount filesystem
./target/debug/wormfs --config test.toml --mount-point /tmp/wormfs &
PID=$!

# Test file operations
echo "Hello, WormFS!" > /tmp/wormfs/test.txt
cat /tmp/wormfs/test.txt
mkdir -p /tmp/wormfs/dir1/dir2
ls -la /tmp/wormfs

# Cleanup
kill $PID
```

#### Documentation
**Tasks**:
1. Create user guide for Phase 1 features
2. Document configuration options
3. Create troubleshooting guide
4. Update README with current status

**Deliverables**:
- Comprehensive test suite
- User documentation
- Performance benchmarks
- Known limitations documented

---

## Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Mount Success | 100% | Filesystem mounts without errors |
| File Operations | 100% | Create, read, write, delete work |
| Directory Operations | 100% | mkdir, rmdir, ls work |
| Data Integrity | 100% | Files match after write/read |
| Performance | <100ms | Small file operations latency |
| Test Coverage | >80% | Unit and integration tests |

## Risk Mitigation

### Technical Risks:
1. **FUSE Complexity**: Start with read-only if needed
2. **SQLite Performance**: Use WAL mode, prepared statements
3. **Erasure Coding**: Fall back to replication if issues

### Fallback Options:
- If FUSE proves difficult: Create simple HTTP API first
- If erasure coding is slow: Use simple replication
- If SQLite has issues: Use simple JSON files

## Dependencies

### External Crates:
- `sqlx` - SQLite async driver
- `fuser` - FUSE bindings
- `reed-solomon` - Erasure coding
- `tokio` - Async runtime
- `tracing` - Logging
- `clap` - CLI parsing
- `serde` / `toml` - Configuration

## Next Steps After Phase 1

Once Phase 1 is complete and tested:
1. Benchmark performance baseline
2. Identify bottlenecks for optimization
3. Prepare for Phase 2 (consensus layer)
4. Consider early user feedback incorporation

## Notes

- Keep all operations local to one node
- No network code in Phase 1
- No concurrency/locking needed yet
- Focus on correctness over performance
- Document all TODOs for later phases