# FileSystemService Component Design

## Purpose & Responsibilities

FileSystemService provides the FUSE-compatible API layer that translates filesystem operations into WormFS storage operations. Its responsibilities include:

- Implementing FUSE filesystem operations (open, read, write, close, stat, etc.)
- Coordinating stripe-level read/write operations across multiple chunks
- Managing read-modify-write logic for partial stripe updates
- Handling over-scanning when reads span multiple stripes
- Delegating lock operations to StorageRaftMember
- Routing metadata operations to appropriate components (RaftMember for writes, MetadataStore for reads)
- Managing file handles and operation contexts
- Providing inode-to-path mapping and directory traversal
- Handling file permissions and access control

## Architecture: Client Pattern with Interior Mutability

FileSystemService uses the client pattern with interior mutability to allow concurrent filesystem operations from multiple FUSE client threads.

### Why This Pattern?

**Concurrent FUSE Operations**: FUSE filesystems receive operations from multiple threads simultaneously (one per active file operation). Each operation needs access to the FileSystem component to coordinate with storage components.

**Solution**: We implement a cloneable client handle pattern where:
1. The outer `FileSystemService` struct is lightweight and cloneable
2. Shared state lives in `Arc<FileSystemServiceInner>` with interior mutability
3. Each FUSE handler thread holds a cloned instancec
4. File handles and operation state use RwLock for concurrent access
5. No global locks block unrelated operations

### Structure

```rust
struct FileSystemServiceInner {
    file_handles: RwLock<HashMap<FileHandle, OpenFile>>,
    inode_cache: RwLock<InodeCache>,
    config: FileSystemServiceConfig,
}

#[derive(Clone)]
pub struct FileSystemService {
    inner: Arc<FileSystemServiceInner>,
    raft_member: Arc<StorageRaftMember>,
    metadata_store: MetadataStore,
    file_store: Arc<FileStore>,
}
```

### Key Benefits

1. **Concurrent Operations**: Multiple FUSE operations can execute simultaneously without blocking
2. **Isolated State**: File handles are independent, allowing concurrent access to different files
3. **Efficient Caching**: Inode and metadata caching reduces backend queries
4. **Clean Abstraction**: Encapsulates all filesystem semantics in one component

## Architecture & Design

### FUSE Operation Flow

```
┌─────────────────────────────────────────────────────────┐
│                  FUSE Client Kernel                      │
│  (User processes: cat, vim, cp, etc.)                   │
└──────────────────────┬──────────────────────────────────┘
                       │ FUSE Protocol
                       ▼
┌─────────────────────────────────────────────────────────┐
│              FileSystemService Component                 │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  FUSE Operation Handlers:                                │
│  ┌─────────────────────────────────────────────────┐   │
│  │ lookup()  → MetadataStore.get_by_path()        │   │
│  │ getattr() → MetadataStore.get_metadata()       │   │
│  │ readdir() → MetadataStore.list_directory()     │   │
│  │                                                  │   │
│  │ create()  → RaftMember.propose(CreateFile)     │   │
│  │ mkdir()   → RaftMember.propose(CreateDir)      │   │
│  │ unlink()  → RaftMember.propose(DeleteFile)     │   │
│  │                                                  │   │
│  │ open()    → acquire_lock() + create FileHandle │   │
│  │ read()    → read_stripes() + over_scan         │   │
│  │ write()   → read_modify_write() + write_stripe │   │
│  │ release() → release_lock() + cleanup           │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  Stripe I/O Coordination:                                │
│  ┌─────────────────────────────────────────────────┐   │
│  │ read_stripes(offset, len)                       │   │
│  │   → identify affected stripes                   │   │
│  │   → fetch stripe metadata from MetadataStore   │   │
│  │   → read stripe data from FileStore             │   │
│  │   → assemble contiguous buffer                  │   │
│  │   → return slice [offset:offset+len]            │   │
│  │                                                  │   │
│  │ write_stripes(offset, data)                     │   │
│  │   → identify affected stripes                   │   │
│  │   → for partial writes: read existing stripe   │   │
│  │   → apply write to stripe buffer                │   │
│  │   → propose stripe update via RaftMember        │   │
│  │   → update metadata (file size, mtime)          │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
         │           │            │
         ▼           ▼            ▼
    RaftMember  MetadataStore  FileStore
   (metadata    (metadata      (chunk
    writes)      reads)         I/O)
```


#### Write Operation

When a client (e.g. fuse client) attempts to write a new file, the FileSystem component receives the request via the StorageEndpoint the client was connected to. FileSystem then initiates the following flow, if and only if its node is the current Raft leader:

1. **Create Empty File**: (this step is only needed if we are creating a new file and writing to it in one step) Calls StorageRaftMember to create an empty file and lock it for writing by the peer that is coordinating the operation (itself). 
2. **Prepare Chunk Data**: FileSystem then directs data be transmitted to FileStore via StorageEndpoint by the initiating client where FileStore will stage the new Stripes and Chunks. If any Chunks exceed their max allowed retries, FileSystem makes new placement decisions for the affected Chunks and reattempt staging upto a maximum number of re-attempts. As long as the minimum requirements of the StoragePolicy are satisfied, Staging can be considered a success and FileSystem will move to the next step.
3. **Update File Metadata**: Initiate another transaction via StorageRaftMember to add/update/etc the new Stripes and Chunks, making them visible via FileSystem read operations. The FileSystem itself likely needs to participate in Voting so StorageRaftMember may need to ask FileSystem and Metastore (perhaps others) to weigh in on the vote before responding to proposals issued by the Master.
3a. **Transaction Vote Calculation**: When asked by StorageRaftMember, FileSystem should locate any chunk files which are expected to be local to the node before contributing to the vote.
4. **Return Result To Caller**: At this point we can notifier the caller (e.g. fuse client) that the operation has completed, either successfully or with errors.

**Key Properties**:
- Chunk data operations are purely a data plane (FileStore) process.
- FileSystem enforces WormFS File System semantics by coordinating Metadata and chunk data independently.
- State is durable (survives crashes)
- Vote response guarantees chunk can be committed or aborted

### Read Operation Example

```
User reads bytes 500,000 - 1,500,000 from file (1MB read)
  ↓
FileSystemService.read(fh=42, offset=500000, len=1000000)
  ↓
Identify affected stripes (assume stripe_size=1MB):
  - Stripe 0: bytes 0-1,048,575
  - Stripe 1: bytes 1,048,576-2,097,151
  ↓
Over-scan: Must read entire Stripe 0 and Stripe 1
  ↓
MetadataStore.get_stripe_metadata(stripe_0, stripe_1)
  → Returns chunk locations for both stripes
  ↓
FileStore.read_stripe(stripe_0)
  → Returns 1MB buffer
  ↓
FileStore.read_stripe(stripe_1)
  → Returns 1MB buffer
  ↓
Concatenate buffers: [stripe_0_data][stripe_1_data]
  ↓
Extract requested range: buffer[500000:1500000]
  ↓
Return to FUSE kernel
```

### Write Operation Example

```
User writes 512KB at offset 700,000
  ↓
FileSystemService.write(fh=42, offset=700000, data=512KB)
  ↓
Identify affected stripes (assume stripe_size=1MB):
  - Stripe 0: bytes 0-1,048,575 (partial write)
  ↓
Read-Modify-Write:
  1. FileStore.read_stripe(stripe_0)
     → Returns existing 1MB buffer
  2. Apply write: buffer[700000:1212000] = new_data
  3. RaftMember.propose(WriteStripe { stripe_0, buffer })
     → Triggers 2PC to distribute chunks
  4. Wait for commit
  ↓
Update metadata:
  RaftMember.propose(UpdateFileMetadata {
    file_id,
    size: max(old_size, offset + len),
    mtime: now(),
  })
  ↓
Return bytes_written to FUSE kernel
```

## Interfaces

### Public API

```rust
#[derive(Clone)]
pub struct FileSystemService {
    inner: Arc<FileSystemServiceInner>,
    raft_member: Arc<StorageRaftMember>,
    metadata_store: MetadataStore,
    file_store: Arc<FileStore>,
}

impl FileSystemService {
    /// Create a new FileSystemService instance
    pub fn new(
        config: FileSystemServiceConfig,
        raft_member: Arc<StorageRaftMember>,
        metadata_store: MetadataStore,
        file_store: Arc<FileStore>,
    ) -> Result<Self, FileSystemServiceError>;
    
    // === FUSE Metadata Operations ===
    
    /// Look up a directory entry by name
    pub async fn lookup(
        &self,
        parent_inode: u64,
        name: &OsStr,
    ) -> Result<FileAttr, FileSystemServiceError>;
    
    /// Get file attributes
    pub async fn getattr(
        &self,
        inode: u64,
    ) -> Result<FileAttr, FileSystemError>;
    
    /// Set file attributes (chmod, chown, truncate, utimens)
    pub async fn setattr(
        &self,
        inode: u64,
        attrs: SetAttr,
    ) -> Result<FileAttr, FileSystemError>;
    
    /// Read directory contents
    pub async fn readdir(
        &self,
        inode: u64,
        offset: i64,
    ) -> Result<Vec<DirectoryEntry>, FileSystemServiceError>;
    
    // === FUSE File Operations ===
    
    /// Create a new file
    pub async fn create(
        &self,
        parent_inode: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<(FileAttr, FileHandle), FileSystemServiceError>;
    
    /// Open an existing file
    pub async fn open(
        &self,
        inode: u64,
        flags: u32,
    ) -> Result<FileHandle, FileSystemServiceError>;
    
    /// Read data from file
    pub async fn read(
        &self,
        fh: FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, FileSystemServiceError>;
    
    /// Write data to file
    pub async fn write(
        &self,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
    ) -> Result<u32, FileSystemServiceError>;
    
    /// Flush pending writes
    pub async fn flush(
        &self,
        fh: FileHandle,
    ) -> Result<(), FileSystemServiceError>;
    
    /// Close file handle
    pub async fn release(
        &self,
        fh: FileHandle,
    ) -> Result<(), FileSystemError>;
    
    // === FUSE Directory Operations ===
    
    /// Create a directory
    pub async fn mkdir(
        &self,
        parent_inode: u64,
        name: &OsStr,
        mode: u32,
    ) -> Result<FileAttr, FileSystemError>;
    
    /// Remove a directory
    pub async fn rmdir(
        &self,
        parent_inode: u64,
        name: &OsStr,
    ) -> Result<(), FileSystemError>;
    
    /// Remove a file
    pub async fn unlink(
        &self,
        parent_inode: u64,
        name: &OsStr,
    ) -> Result<(), FileSystemError>;
    
    /// Rename a file or directory
    pub async fn rename(
        &self,
        old_parent: u64,
        old_name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> Result<(), FileSystemError>;
    
    // === Internal Stripe Operations ===
    
    /// Read one or more stripes and extract requested byte range
    async fn read_stripes(
        &self,
        file_id: FileId,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, FileSystemError>;
    
    /// Write data across one or more stripes (read-modify-write for partial)
    async fn write_stripes(
        &self,
        file_id: FileId,
        offset: u64,
        data: &[u8],
    ) -> Result<(), FileSystemError>;
    
    /// Acquire file lock (delegated to RaftMember)
    async fn acquire_lock(
        &self,
        file_id: FileId,
        lock_type: LockType,
    ) -> Result<LockId, FileSystemServiceError>;
    
    /// Release file lock
    async fn release_lock(
        &self,
        lock_id: LockId,
    ) -> Result<(), FileSystemError>;
}

struct FileSystemServiceInner {
    file_handles: RwLock<HashMap<FileHandle, OpenFile>>,
    inode_cache: RwLock<InodeCache>,
    config: FileSystemServiceConfig,
}

/// Represents an open file
struct OpenFile {
    file_id: FileId,
    inode: u64,
    lock_id: Option<LockId>,
    flags: OpenFlags,
    offset: AtomicU64,
}

/// File handle opaque to FUSE
pub type FileHandle = u64;

/// FUSE file attributes
#[derive(Debug, Clone)]
pub struct FileAttr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub kind: FileType,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
}

#[derive(Debug, Clone)]
pub enum FileType {
    RegularFile,
    Directory,
    Symlink,
}

/// Attributes to set
#[derive(Debug)]
pub struct SetAttr {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub size: Option<u64>,
    pub atime: Option<SystemTime>,
    pub mtime: Option<SystemTime>,
}

#[derive(Debug, Clone)]
pub struct DirectoryEntry {
    pub inode: u64,
    pub file_type: FileType,
    pub name: OsString,
    pub offset: i64,
}
```

### Configuration

```rust
pub struct FileSystemServiceConfig {
    /// Enable read lock enforcement
    pub enable_read_locks: bool,
    
    /// Lock timeout duration
    pub lock_timeout: Duration,
    
    /// Lock extend interval for long-lived operations
    pub lock_extend_interval: Duration,
    
    /// Maximum file handles per client
    pub max_file_handles: usize,
    
    /// Inode cache size (number of entries)
    pub inode_cache_size: usize,
    
    /// Inode cache TTL
    pub inode_cache_ttl: Duration,
    
    /// Read buffer size (for stripe assembly)
    pub read_buffer_size: usize,
    
    /// Write buffer size
    pub write_buffer_size: usize,
    
    /// Enable write-through (no buffering)
    pub write_through: bool,
    
    /// Default file permissions
    pub default_file_mode: u32,
    
    /// Default directory permissions
    pub default_dir_mode: u32,
}

impl Default for FileSystemServiceConfig {
    fn default() -> Self {
        Self {
            enable_read_locks: true,
            lock_timeout: Duration::from_secs(10),
            lock_extend_interval: Duration::from_secs(5),
            max_file_handles: 10_000,
            inode_cache_size: 10_000,
            inode_cache_ttl: Duration::from_secs(60),
            read_buffer_size: 10 * 1024 * 1024, // 10MB
            write_buffer_size: 10 * 1024 * 1024, // 10MB
            write_through: true,
            default_file_mode: 0o644,
            default_dir_mode: 0o755,
        }
    }
}
```

## Dependencies

### Direct Dependencies
- **StorageRaftMember**: For metadata write operations (create, delete, update)
- **MetadataStore**: For metadata read operations (stat, readdir, lookup)
- **FileStore**: For stripe read/write operations

### Dependents
- **StorageEndpoint**: Routes FUSE client requests to FileSystemService

### External Dependencies
- `fuser`: FUSE library for Rust
- `tokio`: Async runtime
- `tracing`: Logging and debugging

## Data Structures

```rust
/// Inode cache for fast path lookups
struct InodeCache {
    entries: HashMap<u64, CachedInode>,
    path_to_inode: HashMap<PathBuf, u64>,
}

struct CachedInode {
    file_id: FileId,
    attrs: FileAttr,
    inserted_at: Instant,
}

/// Open flags parsed from FUSE
#[derive(Debug, Clone, Copy)]
pub struct OpenFlags {
    pub read: bool,
    pub write: bool,
    pub append: bool,
    pub truncate: bool,
    pub create: bool,
    pub exclusive: bool,
}

impl OpenFlags {
    pub fn from_fuse(flags: u32) -> Self {
        let read = (flags & libc::O_RDONLY as u32) != 0 || (flags & libc::O_RDWR as u32) != 0;
        let write = (flags & libc::O_WRONLY as u32) != 0 || (flags & libc::O_RDWR as u32) != 0;
        let append = (flags & libc::O_APPEND as u32) != 0;
        let truncate = (flags & libc::O_TRUNC as u32) != 0;
        let create = (flags & libc::O_CREAT as u32) != 0;
        let exclusive = (flags & libc::O_EXCL as u32) != 0;
        
        Self {
            read,
            write,
            append,
            truncate,
            create,
            exclusive,
        }
    }
    
    pub fn lock_type(&self) -> LockType {
        if self.write {
            LockType::Write
        } else {
            LockType::Read
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FileSystemServiceError {
    #[error("File not found: {0}")]
    NotFound(String),
    
    #[error("Permission denied")]
    PermissionDenied,
    
    #[error("File already exists: {0}")]
    AlreadyExists(String),
    
    #[error("Not a directory: {0}")]
    NotADirectory(String),
    
    #[error("Is a directory: {0}")]
    IsADirectory(String),
    
    #[error("Directory not empty: {0}")]
    DirectoryNotEmpty(String),
    
    #[error("Invalid file handle: {0}")]
    InvalidFileHandle(FileHandle),
    
    #[error("Lock acquisition failed: {0}")]
    LockFailed(String),
    
    #[error("I/O error: {0}")]
    IoError(String),
    
    #[error("Metadata error: {0}")]
    MetadataError(#[from] MetadataStoreError),
    
    #[error("FileStore error: {0}")]
    FileStoreError(#[from] FileStoreError),
    
    #[error("Raft error: {0}")]
    RaftError(String),
}

impl FileSystemServiceError {
    /// Convert to FUSE errno
    pub fn to_errno(&self) -> i32 {
        match self {
            Self::NotFound(_) => libc::ENOENT,
            Self::PermissionDenied => libc::EACCES,
            Self::AlreadyExists(_) => libc::EEXIST,
            Self::NotADirectory(_) => libc::ENOTDIR,
            Self::IsADirectory(_) => libc::EISDIR,
            Self::DirectoryNotEmpty(_) => libc::ENOTEMPTY,
            Self::InvalidFileHandle(_) => libc::EBADF,
            Self::LockFailed(_) => libc::ENOLCK,
            Self::IoError(_) => libc::EIO,
            _ => libc::EIO,
        }
    }
}
```

## Stripe I/O Logic

### Read Algorithm

```rust
async fn read_stripes(
    &self,
    file_id: FileId,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, FileSystemServiceError> {
    // Get file metadata to determine stripe size
    let file_meta = self.metadata_store.get_file_metadata(file_id).await?;
    let stripe_size = file_meta.stripe_size;
    
    // Calculate affected stripe range
    let first_stripe = offset / stripe_size;
    let last_stripe = (offset + length as u64 - 1) / stripe_size;
    
    // Allocate buffer for over-scanned data
    let num_stripes = (last_stripe - first_stripe + 1) as usize;
    let mut buffer = Vec::with_capacity(num_stripes * stripe_size as usize);
    
    // Read each stripe
    for stripe_idx in first_stripe..=last_stripe {
        let stripe_id = StripeId::new(file_id, stripe_idx);
        let stripe_data = self.file_store.read_stripe(file_id, stripe_id).await?;
        buffer.extend_from_slice(&stripe_data);
    }
    
    // Extract requested range from over-scanned buffer
    let buffer_offset = (offset % stripe_size) as usize;
    let end = buffer_offset + length;
    
    Ok(buffer[buffer_offset..end].to_vec())
}
```

### Write Algorithm

```rust
async fn write_stripes(
    &self,
    file_id: FileId,
    offset: u64,
    data: &[u8],
) -> Result<(), FileSystemServiceError> {
    let file_meta = self.metadata_store.get_file_metadata(file_id).await?;
    let stripe_size = file_meta.stripe_size;
    
    let first_stripe = offset / stripe_size;
    let last_stripe = (offset + data.len() as u64 - 1) / stripe_size;
    
    let mut data_offset = 0;
    
    for stripe_idx in first_stripe..=last_stripe {
        let stripe_id = StripeId::new(file_id, stripe_idx);
        let stripe_start = stripe_idx * stripe_size;
        let stripe_end = stripe_start + stripe_size;
        
        // Calculate this stripe's write boundaries
        let write_start_in_stripe = if stripe_idx == first_stripe {
            (offset % stripe_size) as usize
        } else {
            0
        };
        
        let write_end_in_stripe = if stripe_idx == last_stripe {
            let remaining = data.len() - data_offset;
            write_start_in_stripe + remaining
        } else {
            stripe_size as usize
        };
        
        let write_len = write_end_in_stripe - write_start_in_stripe;
        
        // Determine if this is a full stripe write or partial
        let stripe_buffer = if write_start_in_stripe == 0 && write_len == stripe_size as usize {
            // Full stripe write - no read needed
            data[data_offset..data_offset + write_len].to_vec()
        } else {
            // Partial stripe write - read-modify-write
            let mut existing = self.file_store
                .read_stripe(file_id, stripe_id)
                .await
                .unwrap_or_else(|_| vec![0; stripe_size as usize]);
            
            existing[write_start_in_stripe..write_end_in_stripe]
                .copy_from_slice(&data[data_offset..data_offset + write_len]);
            
            existing
        };
        
        // Propose stripe write via Raft (triggers 2PC)
        let write_op = WormFsOperation::TransactionPrepare {
            tx_id: TransactionId::new(),
            metadata_ops: vec![
                MetadataOperation::UpdateStripe {
                    stripe_id,
                    size: stripe_buffer.len() as u64,
                    // ... other fields
                }
            ],
            chunk_ops: vec![
                ChunkDataOperation::WriteStripe {
                    stripe_id,
                    data: stripe_buffer,
                    policy: file_meta.storage_policy,
                }
            ],
        };
        
        self.raft_member.propose_operation(write_op).await?;
        
        data_offset += write_len;
    }
    
    // Update file size and mtime
    let new_size = std::cmp::max(file_meta.size, offset + data.len() as u64);
    self.raft_member.propose_operation(
        WormFsOperation::TransactionPrepare {
            tx_id: TransactionId::new(),
            metadata_ops: vec![
                MetadataOperation::UpdateFile {
                    file_id,
                    size: Some(new_size),
                    mtime: Some(SystemTime::now()),
                    ..Default::default()
                }
            ],
            chunk_ops: vec![],
        }
    ).await?;
    
    Ok(())
}
```

## Testing Strategy

### Unit Tests
- Stripe offset calculation (first_stripe, last_stripe, buffer_offset)
- OpenFlags parsing from FUSE flags
- Error code mapping (FileSystemError → errno)
- Inode cache hit/miss logic
- File handle allocation and cleanup

### Integration Tests
- End-to-end FUSE operations (create, read, write, delete)
- Multi-stripe read with over-scanning
- Partial stripe write with read-modify-write
- Lock acquisition and release
- Directory operations (mkdir, rmdir, readdir)
- Rename operations across directories

### Performance Tests
- Sequential read throughput
- Sequential write throughput
- Random read latency
- Random write latency
- Concurrent file access from multiple clients
- Large file operations (>10GB)

## Open Questions

1. **Write Buffering**: Should we buffer writes in memory and flush periodically, or always write-through to ensure durability? Buffering improves performance but risks data loss on crash. Answer: We can buffer writes to 1 Stripe per file until they exceed the Chunk/Stripe size, or start modifying a different Stripe.

2. **Prefetching**: Should we implement read-ahead prefetching for sequential access patterns? This could significantly improve streaming read performance. Answer: yea, we should fetch the Chunks for 1 Stripe after the requested Stripe but not decode them until they are actually needed to satisfy a read. All chunks when fetched should be cached on disk and only pulled into memory when decoding occurs.

3. **Stripe Size Changes**: If a file's stripe size changes (future feature), how should we handle reads/writes that span old and new stripe boundaries? Answer: When a file's stripe size changes, the change is only applied to new Stripe indexes (e.g. new Stripes at the end of a file). When an existing Stripe is replaced, for whatever reason it retains the size of the Stripe it replaced. In the future we may add a Watchdog optimizer task that will re-write files so they can fully adopt changes to Chunk size and StoragePolicy.

4. **Lock Timeout Handling**: When a lock expires, should we automatically try to re-acquire it, or fail the operation and force the client to retry? Answer: We should be extending locks before they expire, if a lock is expired or revoked for some reason we should fail the operation to the client.

5. **Inode Cache Invalidation**: How should we invalidate inode cache entries when metadata changes? Polling, TTL-based, or event-driven? Answer: We should be notified of changes via StorageRaftMember. Lets update the design of StorageRaftMember to reflect this requirement if it isn't already present. 

6. **Sparse Files**: Should we support sparse files by only writing non-zero stripes? This could save significant storage for large sparse files. Answer: For now we can skip this optimization.

7. **Permissions Caching**: Should we cache permission checks, or always query MetadataStore? Caching improves performance but may serve stale permissions. Answer: Yes, lets cache permissions.

8. **Directory Listing Order**: Should readdir() return entries in inode order, name order, or creation order? Different orders have different performance characteristics. Answer: Lets return them in whichever order is the most performant for the StorageNode

9. **Symlink Support**: Do we need to support symbolic links? If so, how should symlink targets be stored in metadata? Answer: Yes, they should be stored as a special type of File and include a redirect path for their target.

10. **Extended Attributes**: Should we support extended attributes (xattrs) for user metadata? This is commonly used by backup tools and OS features. Answer: Yes

11. **Hard Links**: Should we support hard links? This requires reference counting and complicates unlink operations. Answer: No, lets not support hard links for now.

12. **File Locking Semantics**: Should we support POSIX file locking (flock, fcntl locks) in addition to our custom lock system? Answer: Yes but lets map these onto our custom lock system.

13. **Concurrent Write Handling**: How should we handle concurrent writes to the same stripe from different clients? Currently undefined behavior. Answer: We should disallow concurrent writes. If two clients attempt to open a file for writing, at the same time, one should get an error. It might be simplest to accomplish this with locks but there may be other ways to accomplish this. We can allow concurrent readers which a file is being written and leave it up to the client application to handle any data irregularities.

14. **Write Amplification**: Read-modify-write for small writes causes write amplification. Should we implement write combining or log-structured updates? Answer: We can skip these optimizations for now as we do not expect this to be a common use-case.

15. **Metadata Refresh**: How frequently should we refresh cached metadata to detect external changes from other nodes? Answer: we expect these changes to be pushed to us via StorageRaftMember as FileSystemService participates in preparing and voting to accept Metadata update proposals.
