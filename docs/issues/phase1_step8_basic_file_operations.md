# Phase 1, Step 8: Basic File Operations

**Status**: Not Started
**Phase**: 1 - Minimal Data Path
**Dependencies**: Phase 1.1 (Metadata Store), Phase 1.2 (Chunk Storage)
**Estimated Effort**: 5-7 days

## Overview

Implement the core file operations that enable basic read/write functionality in WormFS. This step integrates the metadata store (Phase 1.1) with the chunk storage layer (Phase 1.2) to provide a complete data path for file operations.

## Current State

The `FilesystemServiceImpl` in `src/filesystem_service/implementation.rs` currently has stub implementations for the following operations:

- `create()` - Returns stub "Operation not yet implemented"
- `open()` - Returns stub file handle 1
- `read()` - Returns empty data
- `write()` - Returns stub 0 bytes written
- `unlink()` - Returns stub success
- `setattr()` - Partially implemented (updates `modified_at` only)

The necessary building blocks are in place:
- ✅ MetadataStore fully implemented (Phase 1.1)
- ✅ FileStore with stripe read/write/update (Phase 1.2)
- ✅ InodeManager with allocation and caching
- ✅ FUSE adapter layer with proper error handling

## Objectives

Implement the six core file operations with full integration between metadata and data storage:

1. **File Creation** (`create`) - Create new files with proper metadata
2. **File Opening** (`open`) - Generate file handles and track open files
3. **File Reading** (`read`) - Read data through stripe-based storage
4. **File Writing** (`write`) - Write data with erasure coding
5. **File Deletion** (`unlink`) - Remove files and clean up storage
6. **Metadata Updates** (`setattr`) - Handle truncation, permissions, timestamps

## Detailed Implementation Plan

### 1. File Creation (`create`)

**Current State**: Returns stub error "Operation not yet implemented"

**Implementation Requirements**:
```rust
async fn create(
    &self,
    parent: u64,
    name: &str,
    mode: u32,
    flags: u32,
) -> Result<(FileAttr, u64), Error>
```

**Steps**:
1. **Reserve Inode**: Call `inode_manager.allocate()` to get new inode
2. **Construct Path**: Lookup parent directory, build full path
3. **Create Metadata Entry**: Call `metadata_store.create_file()` with:
   - Generated `FileId`
   - Full path
   - Reserved inode
   - Initial metadata (size=0, type=RegularFile, permissions from mode)
4. **Update Cache**: Call `inode_manager.cache_inode()` with new metadata
5. **Generate File Handle**: Allocate handle in `open_files` map
6. **Return Attributes**: Convert FileRecord to FileAttr

**Error Handling**:
- Parent inode not found → `Error::NotFound(parent)`
- File already exists → `Error::FileAlreadyExists`
- Inode allocation fails → `Error::NoAvailableInodes`

**Integration Points**:
- `MetadataStore::create_file()`
- `InodeManager::allocate()`
- `InodeManager::cache_inode()`

**Test Cases**:
- Create file in root directory
- Create file in nested directory
- Create file with custom permissions (0o644, 0o755, etc.)
- Verify metadata persists after creation
- Error: Create file with duplicate name
- Error: Create file in non-existent parent

---

### 2. File Opening (`open`)

**Current State**: Returns hardcoded handle 1

**Implementation Requirements**:
```rust
async fn open(&self, inode: u64, flags: u32) -> Result<u64, Error>
```

**Steps**:
1. **Verify File Exists**: Query `metadata_store.get_file_by_inode(inode)`
2. **Check Access Permissions**: Validate flags against file permissions
3. **Generate Unique Handle**: Use atomic counter for handle generation
4. **Track Open File**: Insert into `open_files: HashMap<u64, OpenFileState>`:
   ```rust
   struct OpenFileState {
       inode: u64,
       file_id: FileId,
       flags: u32,
       offset: AtomicU64,  // For sequential reads/writes
   }
   ```
5. **Update Access Time**: Call `metadata_store.update_file()` with new `accessed_at`
6. **Return Handle**: Return unique file handle

**Error Handling**:
- Inode not found → `Error::NotFound(inode)`
- Permission denied → `Error::PermissionDenied`

**Integration Points**:
- `MetadataStore::get_file_by_inode()`
- `MetadataStore::update_file()`

**Test Cases**:
- Open existing file with O_RDONLY
- Open existing file with O_RDWR
- Open file multiple times (different handles)
- Verify access time updated
- Error: Open non-existent inode
- Error: Open with incompatible flags (write to read-only file)

---

### 3. File Reading (`read`)

**Current State**: Returns empty vector

**Implementation Requirements**:
```rust
async fn read(
    &self,
    inode: u64,
    fh: u64,
    offset: i64,
    size: u32,
) -> Result<Vec<u8>, Error>
```

**Steps**:
1. **Validate File Handle**: Lookup `fh` in `open_files`, verify inode matches
2. **Get File Metadata**: Query `metadata_store.get_file_by_inode(inode)`
3. **Bounds Checking**:
   - If offset ≥ file size, return empty vector
   - Clamp read size to `min(size, file_size - offset)`
4. **Calculate Stripe Range**:
   ```rust
   let start_stripe = offset / STRIPE_SIZE;
   let end_stripe = (offset + size - 1) / STRIPE_SIZE;
   ```
5. **Read Stripes**: For each stripe in range:
   - Query `metadata_store.get_stripe_at_offset(file_id, stripe_offset)`
   - Call `file_store.read_stripe(stripe_id)` to get decoded data
   - Accumulate stripe data in buffer
6. **Extract Requested Range**: Slice buffer from offset within first stripe to end
7. **Update Access Time**: Call `metadata_store.update_file()` with new `accessed_at`
8. **Return Data**: Return extracted byte range

**Error Handling**:
- Invalid file handle → `Error::InvalidFileHandle`
- File deleted between open and read → `Error::NotFound(inode)`
- Stripe read failure → propagate `FileStoreError`
- Chunk unavailable → `Error::DataUnavailable`

**Integration Points**:
- `FileStore::read_stripe()`
- `MetadataStore::get_stripe_at_offset()`
- `MetadataStore::get_file_by_inode()`

**Performance Considerations**:
- Cache decoded stripes for sequential reads
- Prefetch next stripe when reading sequentially
- Use read-ahead for large sequential reads

**Test Cases**:
- Read entire small file (< 1 stripe)
- Read spanning multiple stripes
- Read with offset in middle of file
- Read beyond file size (returns partial/empty)
- Sequential reads (verify cache efficiency)
- Random access reads
- Concurrent reads to same file
- Error: Read with invalid file handle
- Error: Read from deleted file

---

### 4. File Writing (`write`)

**Current State**: Returns 0 bytes written

**Implementation Requirements**:
```rust
async fn write(
    &self,
    inode: u64,
    fh: u64,
    offset: i64,
    data: &[u8],
) -> Result<u32, Error>
```

**Steps**:
1. **Validate File Handle**: Lookup `fh` in `open_files`, verify write permission
2. **Get File Metadata**: Query `metadata_store.get_file_by_inode(inode)`
3. **Calculate Stripe Range**: Determine affected stripes
4. **Handle Partial Stripe Writes**:
   - **First Stripe (if partial)**:
     - Read existing stripe data
     - Merge with new data at correct offset
     - Call `file_store.update_stripe()`
   - **Middle Stripes (full stripes)**:
     - Call `file_store.write_stripe()` directly
   - **Last Stripe (if partial)**:
     - Read existing stripe data (if exists)
     - Merge with new data
     - Call `file_store.update_stripe()` or `write_stripe()`
5. **Update File Metadata**:
   - New size: `max(old_size, offset + data.len())`
   - Update `modified_at` timestamp
   - Call `metadata_store.update_file()`
6. **Invalidate Cache**: Call `inode_manager.invalidate(inode)`
7. **Return Written Bytes**: Return `data.len() as u32`

**Error Handling**:
- Invalid file handle → `Error::InvalidFileHandle`
- File opened read-only → `Error::PermissionDenied`
- File store write failure → propagate error
- Disk space exhausted → `Error::NoSpace`

**Integration Points**:
- `FileStore::write_stripe()`
- `FileStore::update_stripe()`
- `FileStore::read_stripe()` (for read-modify-write)
- `MetadataStore::update_file()`
- `InodeManager::invalidate()`

**Performance Considerations**:
- Buffer writes to accumulate full stripes when possible
- Defer metadata updates for burst writes
- Implement write-behind caching for hot files

**Write Patterns to Handle**:
1. **Sequential Write** (offset = current size): Append-only, no RMW
2. **Overwrite** (offset < current size): Read-modify-write existing stripes
3. **Sparse Write** (offset > current size): Create hole (zero-fill gap)
4. **Partial Stripe Write**: Requires read-modify-write cycle

**Test Cases**:
- Write to new file (sequential from offset 0)
- Append to existing file
- Overwrite middle of file
- Write spanning multiple stripes
- Write creating sparse file (offset > size)
- Write with partial stripe at start
- Write with partial stripe at end
- Concurrent writes to different files
- Sequential writes (verify buffering)
- Error: Write with invalid handle
- Error: Write to read-only file handle
- Error: Write with insufficient disk space

---

### 5. File Deletion (`unlink`)

**Current State**: Returns success without action

**Implementation Requirements**:
```rust
async fn unlink(&self, parent: u64, name: &str) -> Result<(), Error>
```

**Steps**:
1. **Construct Path**: Lookup parent directory, build full path
2. **Get File Metadata**: Query `metadata_store.get_file_by_path()`
3. **Check File Type**: Verify it's a regular file (not directory)
4. **Check Open Files**: Verify file is not currently open
   - If open, mark for deferred deletion
5. **Get File Stripes**: Query `metadata_store.get_file_stripes(file_id)`
6. **Delete Metadata**: Call `metadata_store.delete_file(file_id)`
7. **Queue Chunk Cleanup**: For each stripe:
   - Get chunks: `metadata_store.get_stripe_chunks(stripe_id)`
   - Queue async deletion via `file_store.delete_stripe(stripe_id)`
8. **Invalidate Cache**: Call `inode_manager.remove(inode)`
9. **Release Inode**: Mark inode as available for reuse

**Error Handling**:
- File not found → `Error::NotFound`
- File is directory → `Error::IsDirectory`
- Permission denied → `Error::PermissionDenied`
- File busy (if enforcing) → `Error::FileBusy`

**Integration Points**:
- `MetadataStore::delete_file()`
- `MetadataStore::get_file_stripes()`
- `FileStore::delete_stripe()`
- `InodeManager::remove()`

**Deferred Deletion**:
When a file is unlinked while still open:
1. Remove directory entry (metadata)
2. Keep inode alive until last handle closes
3. Implement reference counting in `OpenFileState`
4. Trigger cleanup in `release()` when refcount reaches 0

**Test Cases**:
- Delete simple file
- Delete file with multiple stripes
- Delete file then verify inode reused
- Delete then create file with same name
- Error: Delete non-existent file
- Error: Delete directory with unlink
- Error: Delete file while open (if enforced)
- Verify chunks actually deleted from storage nodes

---

### 6. Metadata Updates (`setattr`)

**Current State**: Only updates `modified_at` timestamp

**Implementation Requirements**:
```rust
async fn setattr(
    &self,
    inode: u64,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
    atime: Option<SystemTime>,
    mtime: Option<SystemTime>,
) -> Result<FileAttr, Error>
```

**Steps**:
1. **Get Current Metadata**: Query `metadata_store.get_file_by_inode(inode)`
2. **Apply Updates**: Update only provided fields:
   - `mode` → Update permissions
   - `uid`/`gid` → Update ownership
   - `size` → **Truncation** (special handling)
   - `atime`/`mtime` → Update timestamps
3. **Handle Truncation** (if `size` provided):
   - **Expand** (new_size > current_size):
     - Just update metadata (sparse file)
   - **Shrink** (new_size < current_size):
     - Calculate affected stripes
     - Delete stripes beyond new size
     - Truncate partial stripe at boundary
     - Update file size in metadata
4. **Persist Changes**: Call `metadata_store.update_file()`
5. **Invalidate Cache**: Call `inode_manager.invalidate(inode)`
6. **Return Updated Attrs**: Convert updated FileRecord to FileAttr

**Error Handling**:
- Inode not found → `Error::NotFound(inode)`
- Permission denied → `Error::PermissionDenied`
- Invalid size (negative) → `Error::InvalidArgument`

**Integration Points**:
- `MetadataStore::update_file()`
- `MetadataStore::get_file_stripes()`
- `FileStore::delete_stripe()` (for truncation)
- `FileStore::update_stripe()` (for partial truncation)
- `InodeManager::invalidate()`

**Truncation Edge Cases**:
1. **Truncate to 0**: Delete all stripes, keep metadata
2. **Truncate within stripe**: Read-modify-write last stripe
3. **Truncate at stripe boundary**: Clean stripe deletion
4. **Truncate beyond size**: No-op (sparse semantics)

**Test Cases**:
- Update permissions (chmod)
- Update ownership (chown)
- Update timestamps (touch)
- Truncate file to 0
- Truncate file to smaller size (within stripe)
- Truncate file to smaller size (across stripes)
- Truncate file to larger size (sparse)
- Update multiple attributes simultaneously
- Error: setattr on non-existent inode
- Error: setattr with invalid size

---

## Data Structures

### Open File Tracking

Add to `FilesystemServiceImpl`:
```rust
struct OpenFileState {
    inode: u64,
    file_id: FileId,
    flags: u32,           // Open flags (O_RDONLY, O_RDWR, etc.)
    offset: AtomicU64,    // Current offset for sequential access
    refcount: AtomicU32,  // Number of open handles (for deferred deletion)
}

struct FilesystemServiceImpl {
    // ... existing fields
    open_files: Arc<RwLock<HashMap<u64, Arc<OpenFileState>>>>,
    next_file_handle: AtomicU64,
}
```

### Write Buffer (Optional Enhancement)

For improved write performance:
```rust
struct WriteBuffer {
    data: Vec<u8>,
    dirty_ranges: Vec<(u64, u64)>,  // (offset, length) pairs
    last_flush: Instant,
}
```

---

## Testing Requirements

### Unit Tests

Create `src/filesystem_service/file_operations_tests.rs`:

1. **Creation Tests**:
   - `test_create_file_success()`
   - `test_create_duplicate_file_error()`
   - `test_create_in_nonexistent_parent()`

2. **Open Tests**:
   - `test_open_existing_file()`
   - `test_open_nonexistent_file_error()`
   - `test_open_multiple_handles()`

3. **Read Tests**:
   - `test_read_full_file()`
   - `test_read_partial_file()`
   - `test_read_multi_stripe()`
   - `test_read_beyond_eof()`
   - `test_read_invalid_handle()`

4. **Write Tests**:
   - `test_write_new_file()`
   - `test_write_append()`
   - `test_write_overwrite()`
   - `test_write_sparse()`
   - `test_write_multi_stripe()`
   - `test_write_partial_stripe()`

5. **Delete Tests**:
   - `test_unlink_file()`
   - `test_unlink_nonexistent()`
   - `test_unlink_verifies_cleanup()`

6. **Setattr Tests**:
   - `test_setattr_permissions()`
   - `test_setattr_truncate_grow()`
   - `test_setattr_truncate_shrink()`
   - `test_setattr_truncate_zero()`

### Integration Tests

Add to `tests/integration/file_operations_test.rs`:

1. **End-to-End Workflow**:
   ```rust
   #[tokio::test]
   async fn test_file_lifecycle() {
       // Create → Open → Write → Close → Open → Read → Verify → Unlink
   }
   ```

2. **Multi-Stripe Operations**:
   ```rust
   #[tokio::test]
   async fn test_large_file_write_read() {
       // Write 10MB file, read back in chunks, verify integrity
   }
   ```

3. **Concurrent Access**:
   ```rust
   #[tokio::test]
   async fn test_concurrent_reads() {
       // Multiple readers accessing same file
   }
   ```

4. **Error Recovery**:
   ```rust
   #[tokio::test]
   async fn test_write_failure_rollback() {
       // Simulate stripe write failure, verify metadata consistency
   }
   ```

### Performance Benchmarks

Add to `benches/file_operations_benchmarks.rs`:

1. **Sequential Write Performance**: Measure MB/s for large sequential writes
2. **Sequential Read Performance**: Measure MB/s for large sequential reads
3. **Random Write Performance**: 4KB random writes
4. **Random Read Performance**: 4KB random reads
5. **Metadata Operation Latency**: create/open/setattr/unlink times

**Target Metrics**:
- Sequential write: > 100 MB/s
- Sequential read: > 150 MB/s
- Random 4KB write: < 5ms latency
- Random 4KB read: < 2ms latency (cached), < 10ms (uncached)
- File creation: < 5ms
- File deletion: < 10ms

---

## Success Criteria

### Functional Requirements
- ✅ All six operations fully implemented with proper error handling
- ✅ Metadata and data remain consistent across operations
- ✅ Files persist correctly through mount/unmount cycles
- ✅ Multi-stripe files handled correctly
- ✅ Partial stripe reads/writes work correctly
- ✅ File truncation properly cleans up storage

### Testing Requirements
- ✅ All unit tests passing (target: 30+ tests)
- ✅ All integration tests passing (target: 10+ tests)
- ✅ Performance benchmarks meet targets
- ✅ No memory leaks in long-running tests
- ✅ No data corruption under concurrent access

### Code Quality
- ✅ Comprehensive error handling with proper context
- ✅ Clear documentation for all public methods
- ✅ Logging at appropriate levels (debug, info, warn, error)
- ✅ No panics in production code paths
- ✅ No unsafe code unless absolutely necessary (with justification)

---

## Implementation Phases

### Phase 1: Foundation (Days 1-2)
- [ ] Add `OpenFileState` and handle tracking infrastructure
- [ ] Implement `create()` operation
- [ ] Implement `open()` operation
- [ ] Write unit tests for create/open

### Phase 2: Data Operations (Days 3-4)
- [ ] Implement `read()` operation
- [ ] Implement `write()` operation with full stripe handling
- [ ] Write unit tests for read/write
- [ ] Add integration test for write→read round-trip

### Phase 3: Cleanup Operations (Day 5)
- [ ] Implement `unlink()` operation
- [ ] Implement `setattr()` with truncation
- [ ] Write unit tests for unlink/setattr
- [ ] Add integration test for file lifecycle

### Phase 4: Testing & Polish (Days 6-7)
- [ ] Add comprehensive integration tests
- [ ] Add performance benchmarks
- [ ] Performance optimization pass
- [ ] Documentation and code review
- [ ] End-to-end testing with FUSE mount

---

## Risks and Mitigations

### Risk 1: Data Corruption in Partial Stripe Writes
**Mitigation**:
- Implement careful read-modify-write logic
- Add extensive testing for edge cases
- Use checksums to detect corruption early

### Risk 2: Race Conditions in Concurrent Access
**Mitigation**:
- Use proper locking in `open_files` map
- Implement atomic metadata updates
- Add stress tests with concurrent operations

### Risk 3: Performance Degradation
**Mitigation**:
- Profile hot paths early
- Implement caching for decoded stripes
- Use write buffering for small writes
- Optimize metadata lookups

### Risk 4: Incomplete Cleanup on Failures
**Mitigation**:
- Implement transactional semantics where possible
- Add cleanup on error paths
- Log incomplete operations for debugging
- Add health checks to detect orphaned data

---

## Dependencies

### Required (Blocking)
- Phase 1.1: MetadataStore ✅ Complete
- Phase 1.2: Chunk Storage ✅ Complete

### Optional (Enhancement)
- Write-behind caching system
- Read-ahead prefetching
- Compression support

---

## References

- [Phase 1 Implementation Plan](../implementation_plan/phase1_minimal_data_path.md)
- [MetadataStore Documentation](../../src/metadata_store/mod.rs)
- [FileStore Documentation](../../src/file_store/mod.rs)
- [FUSE Operations](https://docs.rs/fuser/latest/fuser/trait.Filesystem.html)

---

## Notes

### Stripe Size Considerations
The default stripe size (4MB) affects write patterns:
- Small files (< 4MB): Single stripe, no splitting needed
- Large files: Multiple stripes, requires range calculation
- Random writes: May require read-modify-write cycles

### Future Enhancements
Items intentionally deferred to later phases:
- **Distributed Locking**: File-level locks across cluster (Phase 2)
- **Write Buffering**: Accumulate writes before stripe commit (Phase 3)
- **Read Caching**: Cache decoded stripes in memory (Phase 3)
- **Compression**: Compress stripes before erasure coding (Phase 4)
- **Deduplication**: Detect and eliminate duplicate chunks (Phase 5)

### Open Questions
1. Should we implement POSIX-style deferred deletion (unlink while open)?
2. What's the maximum reasonable file size we should support?
3. Should we enforce file locking, or rely on client coordination?

---

**Issue Created**: 2025-10-16
**Target Completion**: Phase 1 Sprint 2
