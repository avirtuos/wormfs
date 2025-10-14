# StorageEndpoint Component Design

## Purpose & Responsibilities

StorageEndpoint is the gRPC API server that exposes the storage node's functionality as well as its storage data plane to clients and other storage nodes. Its responsibilities include:

- Providing FUSE filesystem operations API for client nodes
- Exposing chunk read/write APIs for inter-node communication
- Providing snapshot transfer APIs for node recovery
- Exposing transaction log APIs for Raft replication
- Handling administrative operations (cluster management, monitoring)
- Managing client authentication and authorization
- Implementing request rate limiting and back-pressure
- Routing requests to appropriate internal components
- Support Stripe upload and download from FUSE clients

## Architecture & Design

### API Categories

```
┌─────────────────────────────────────────────────────────┐
│               StorageEndpoint (gRPC)                     │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  Client APIs (from FUSE clients):                        │
│  ┌─────────────────────────────────────────────────┐   │
│  │  • File operations (create, read, write, delete) │   │
│  │  • Directory operations (mkdir, list, rmdir)     │   │
│  │  • Metadata operations (stat, chmod, chown)      │   │
│  │  • Lock operations (acquire, release, extend)    │   │
│  │  • Stripe read/write operations                  │   │
│  └─────────────────────────────────────────────────┘   │
│                  ↓ Routes to FileSystemService           │
│                  (FileSystemService handles FUSE semantics) │
│                                                           │
│  Inter-Node APIs (from other storage nodes):             │
│  ┌─────────────────────────────────────────────────┐   │
│  │  • Chunk transfer (read/write chunks)            │   │
│  │  • Chunk verification (check_chunk)              │   │
│  │  • Snapshot transfer (stream snapshots)          │   │
│  │  • Transaction log queries                       │   │
│  └─────────────────────────────────────────────────┘   │
│                  ↓ Routes to FileStore/SnapshotStore/TxLog │
│                                                           │
│  Administrative APIs:                                    │
│  ┌─────────────────────────────────────────────────┐   │
│  │  • Cluster status and health                     │   │
│  │  • Node management (add, remove)                 │   │
│  │  • Storage policy management                     │   │
│  │  • Metrics and monitoring                        │   │
│  └─────────────────────────────────────────────────┘   │
│                  ↓ Routes to StorageRaftMember/StorageNode │
└─────────────────────────────────────────────────────────┘
```

### Request Flow

#### FUSE Client Write Operations

```
Client Write Request
     ↓
gRPC Server (tonic)
     ↓
Request Validation & Authentication
     ↓
FilesystemService → FileSystemService.write()
     ↓
FileSystemService handles:
  - Stripe identification
  - Read-modify-write logic
  - Lock management
  - Metadata updates
     ↓
FileSystemService delegates to:
  - RaftMember (metadata writes via 2PC)
  - FileStore (stripe I/O)
  - MetadataStore (metadata reads)
     ↓
Response to client
```

#### Inter-Node Write Operations (Transaction Protocol)

```
Inter-Node Write Request (any node)
     ↓
gRPC Server (tonic)
     ↓
Request Validation & Authentication
     ↓
Check if Local Node is Raft Leader
     ↓
┌─────────────────────────────────────┐
│ If Leader:                          │
│   → Process locally via RaftMember  │
│   → Coordinate 2PC                  │
│                                     │
│ If Follower:                        │
│   → Redirect caller to leader       │
└─────────────────────────────────────┘
```

#### FUSE Client Read Operations

```
Client Read Request
     ↓
gRPC Server (tonic)
     ↓
Request Validation & Authentication
     ↓
Rate Limiting / Backpressure Check
     ↓
FilesystemService → FileSystemService
     ↓
┌───────────────────┬───────────────────┬──────────────┐
│                   │                   │              │
Metadata Op         File Read          Directory Op   
     ↓                   ↓                   ↓              
FileSystemService.getattr  FileSystemService.read    FileSystemService.readdir
     ↓                   ↓                   ↓
MetadataStore       FileStore          MetadataStore
(local read)        (read_stripe)      (list_dir)
     ↓                   ↓                   ↓
Response            Response           Response
```

#### Inter-Node Read Operations

```
Inter-Node Read Request
     ↓
gRPC Server (tonic)
     ↓
Request Validation & Authentication
     ↓
Rate Limiting / Backpressure Check
     ↓
Route to Handler
     ↓
┌──────────────────┬──────────────────┬────────────────┐
│                  │                  │                │
Chunk Read         Snapshot Read      TxLog Read     Admin Op
     ↓                  ↓                  ↓                ↓
FileStore          SnapshotStore      TxLogStore     StorageNode
(read_chunk)       (stream_snapshot)  (get_entries)  (status)
     ↓                  ↓                  ↓                ↓
Response           Response           Response         Response
```

## Interfaces

### gRPC Service Definitions

```protobuf
syntax = "proto3";

package wormfs.storage;

// ===== Client Filesystem Service =====
service FilesystemService {
  // File operations
  rpc CreateFile(CreateFileRequest) returns (CreateFileResponse);
  rpc ReadFile(ReadFileRequest) returns (stream FileChunk);
  rpc WriteFile(stream FileChunk) returns (WriteFileResponse);
  rpc DeleteFile(DeleteFileRequest) returns (DeleteFileResponse);
  rpc GetFileMetadata(GetFileMetadataRequest) returns (FileMetadataResponse);
  
  // Directory operations
  rpc CreateDirectory(CreateDirectoryRequest) returns (CreateDirectoryResponse);
  rpc ListDirectory(ListDirectoryRequest) returns (ListDirectoryResponse);
  rpc DeleteDirectory(DeleteDirectoryRequest) returns (DeleteDirectoryResponse);
  
  // Lock operations
  rpc AcquireLock(AcquireLockRequest) returns (AcquireLockResponse);
  rpc ReleaseLock(ReleaseLockRequest) returns (ReleaseLockResponse);
  rpc ExtendLock(ExtendLockRequest) returns (ExtendLockResponse);
  
  // Stripe operations
  rpc ReadStripe(ReadStripeRequest) returns (ReadStripeResponse);
  rpc WriteStripe(WriteStripeRequest) returns (WriteStripeResponse);
}

// ===== Inter-Node Chunk Service =====
service ChunkService {
  rpc WriteChunk(WriteChunkRequest) returns (WriteChunkResponse);
  rpc ReadChunk(ReadChunkRequest) returns (ReadChunkResponse);
  rpc CheckChunk(CheckChunkRequest) returns (CheckChunkResponse);
  rpc VerifyChunk(VerifyChunkRequest) returns (VerifyChunkResponse);
  rpc DeleteChunk(DeleteChunkRequest) returns (DeleteChunkResponse);
}

// ===== Inter-Node Snapshot Service =====
service SnapshotService {
  rpc GetLatestSnapshot(GetLatestSnapshotRequest) returns (SnapshotInfo);
  rpc StreamSnapshot(StreamSnapshotRequest) returns (stream SnapshotChunk);
  rpc GetSnapshotInfo(GetSnapshotInfoRequest) returns (SnapshotInfo);
}

// ===== Inter-Node Transaction Log Service =====
service TransactionLogService {
  rpc GetLogEntries(GetLogEntriesRequest) returns (stream LogEntry);
  rpc GetLogState(GetLogStateRequest) returns (LogStateResponse);
}

// ===== Administrative Service =====
service AdminService {
  rpc GetClusterStatus(GetClusterStatusRequest) returns (ClusterStatusResponse);
  rpc GetNodeHealth(GetNodeHealthRequest) returns (NodeHealthResponse);
  rpc AddNode(AddNodeRequest) returns (AddNodeResponse);
  rpc RemoveNode(RemoveNodeRequest) returns (RemoveNodeResponse);
  rpc SetStoragePolicy(SetStoragePolicyRequest) returns (SetStoragePolicyResponse);
  rpc GetMetrics(GetMetricsRequest) returns (MetricsResponse);
}

// ===== Message Definitions =====

message FileMetadata {
  uint64 inode = 1;
  string path = 2;
  uint64 size = 3;
  uint32 permissions = 4;
  uint32 uid = 5;
  uint32 gid = 6;
  int64 created_at = 7;
  int64 modified_at = 8;
  int64 accessed_at = 9;
}

message CreateFileRequest {
  string path = 1;
  FileMetadata metadata = 2;
}

message CreateFileResponse {
  bytes file_id = 1;
  uint64 inode = 2;
}

message ReadStripeRequest {
  bytes file_id = 1;
  bytes stripe_id = 2;
}

message ReadStripeResponse {
  bytes data = 1;
}

message WriteStripeRequest {
  bytes file_id = 1;
  bytes stripe_id = 2;
  bytes data = 3;
  StoragePolicy policy = 4;
}

message WriteStripeResponse {
  bytes stripe_id = 1;
  repeated ChunkLocation chunks = 2;
}

message StoragePolicy {
  uint32 data_shards = 1;
  uint32 parity_shards = 2;
  uint64 stripe_size = 3;
}

message ChunkLocation {
  bytes chunk_id = 1;
  string node_id = 2;
  string disk_id = 3;
  uint32 chunk_index = 4;
}

message AcquireLockRequest {
  bytes file_id = 1;
  string client_id = 2;
  LockType lock_type = 3;
  int64 duration_secs = 4;
}

enum LockType {
  READ = 0;
  WRITE = 1;
}

message AcquireLockResponse {
  uint64 lock_id = 1;
  int64 expires_at = 2;
}
```

### Rust Implementation

```rust
pub struct StorageEndpoint {
    config: EndpointConfig,
    file_system: Arc<FileSystemService>,
    raft_member: Arc<StorageRaftMember>,
    file_store: Arc<FileStore>,
    metadata_store: Arc<MetadataStore>,
    snapshot_store: Arc<SnapshotStore>,
    transaction_log_store: Arc<TransactionLogStore>,
    storage_node: Arc<StorageNode>,
}

impl StorageEndpoint {
    pub fn new(
        config: EndpointConfig,
        file_system: Arc<FileSystemService>,
        raft_member: Arc<StorageRaftMember>,
        file_store: Arc<FileStore>,
        metadata_store: Arc<MetadataStore>,
        snapshot_store: Arc<SnapshotStore>,
        transaction_log_store: Arc<TransactionLogStore>,
        storage_node: Arc<StorageNode>,
    ) -> Self {
        Self {
            config,
            file_system,
            raft_member,
            file_store,
            metadata_store,
            snapshot_store,
            transaction_log_store,
            storage_node,
        }
    }
    
    /// Start the gRPC server
    pub async fn start(&self) -> Result<(), EndpointError> {
        let addr = self.config.listen_address.parse()?;
        
        Server::builder()
            .add_service(FilesystemServiceServer::new(
                FilesystemServiceImpl::new(
                    self.file_system.clone(),
                )
            ))
            .add_service(ChunkServiceServer::new(
                ChunkServiceImpl::new(self.file_store.clone())
            ))
            .add_service(SnapshotServiceServer::new(
                SnapshotServiceImpl::new(self.snapshot_store.clone())
            ))
            .add_service(TransactionLogServiceServer::new(
                TransactionLogServiceImpl::new(self.transaction_log_store.clone())
            ))
            .add_service(AdminServiceServer::new(
                AdminServiceImpl::new(
                    self.storage_node.clone(),
                    self.raft_member.clone(),
                )
            ))
            .serve(addr)
            .await?;
        
        Ok(())
    }
}

// ===== Filesystem Service Implementation =====

struct FilesystemServiceImpl {
    file_system: Arc<FileSystemService>,
}

#[tonic::async_trait]
impl FilesystemService for FilesystemServiceImpl {
    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>, Status> {
        let req = request.into_inner();
        
        // Parse metadata
        let metadata = req.metadata.ok_or(Status::invalid_argument("missing metadata"))?;
        
        // Delegate to FileSystemService
        let (attrs, _fh) = self.file_system
            .create(
                metadata.inode, // parent inode
                OsStr::new(&req.path),
                metadata.permissions,
                0, // flags
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(CreateFileResponse {
            file_id: attrs.ino.to_le_bytes().to_vec(),
            inode: attrs.ino,
        }))
    }
    
    async fn read_stripe(
        &self,
        request: Request<ReadStripeRequest>,
    ) -> Result<Response<ReadStripeResponse>, Status> {
        let req = request.into_inner();
        
        let file_id = FileId::from_bytes(&req.file_id)
            .map_err(|e| Status::invalid_argument(format!("invalid file_id: {}", e)))?;
        let stripe_id = StripeId::from_bytes(&req.stripe_id)
            .map_err(|e| Status::invalid_argument(format!("invalid stripe_id: {}", e)))?;
        
        // Delegate to FileSystemService which handles stripe I/O
        // Note: FileSystemService.read() expects inode, so we need to convert
        // This is a simplified example - actual implementation would need proper mapping
        let inode = file_id.as_u64(); // Simplified conversion
        let fh = 0; // Temporary file handle for read
        
        let data = self.file_system
            .read(fh, stripe_id.offset(), stripe_id.size() as u32)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(ReadStripeResponse { data }))
    }
    
    async fn write_stripe(
        &self,
        request: Request<WriteStripeRequest>,
    ) -> Result<Response<WriteStripeResponse>, Status> {
        let req = request.into_inner();
        
        let file_id = FileId::from_bytes(&req.file_id)?;
        let stripe_id = StripeId::from_bytes(&req.stripe_id)?;
        
        // Delegate to FileSystemService which handles stripe I/O and metadata updates
        let inode = file_id.as_u64(); // Simplified conversion
        let fh = 0; // Temporary file handle for write
        
        let bytes_written = self.file_system
            .write(fh, stripe_id.offset(), &req.data)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        // FileSystemService handles chunk metadata internally
        Ok(Response::new(WriteStripeResponse {
            stripe_id: stripe_id.as_bytes().to_vec(),
            chunks: vec![], // Chunks are managed internally by FileSystem
        }))
    }
    
    async fn acquire_lock(
        &self,
        request: Request<AcquireLockRequest>,
    ) -> Result<Response<AcquireLockResponse>, Status> {
        let req = request.into_inner();
        
        let file_id = FileId::from_bytes(&req.file_id)?;
        let lock_type = match req.lock_type() {
            LockType::Read => crate::LockType::Read,
            LockType::Write => crate::LockType::Write,
        };
        
        // Delegate to FileSystemService which handles lock acquisition via RaftMember
        let lock_id = self.file_system
            .acquire_lock(file_id, lock_type)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        let duration = Duration::from_secs(req.duration_secs as u64);
        let expires_at = SystemTime::now() + duration;
        
        Ok(Response::new(AcquireLockResponse {
            lock_id: lock_id.as_u64(),
            expires_at: expires_at.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
        }))
    }
    
    // ... other methods
}

// ===== Chunk Service Implementation =====

struct ChunkServiceImpl {
    file_store: Arc<FileStore>,
}

#[tonic::async_trait]
impl ChunkService for ChunkServiceImpl {
    async fn write_chunk(
        &self,
        request: Request<WriteChunkRequest>,
    ) -> Result<Response<WriteChunkResponse>, Status> {
        let req = request.into_inner();
        
        let chunk_id = ChunkId::from_bytes(&req.chunk_id)?;
        let chunk_data = ChunkData::from_proto(req.chunk_data.ok_or(Status::invalid_argument("missing chunk data"))?)?;
        
        self.file_store
            .write_chunk_local(chunk_id, chunk_data)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(WriteChunkResponse {}))
    }
    
    async fn read_chunk(
        &self,
        request: Request<ReadChunkRequest>,
    ) -> Result<Response<ReadChunkResponse>, Status> {
        let req = request.into_inner();
        
        let chunk_id = ChunkId::from_bytes(&req.chunk_id)?;
        
        let chunk_data = self.file_store
            .read_chunk_local(chunk_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(ReadChunkResponse {
            chunk_data: Some(chunk_data.to_proto()),
        }))
    }
    
    async fn check_chunk(
        &self,
        request: Request<CheckChunkRequest>,
    ) -> Result<Response<CheckChunkResponse>, Status> {
        let req = request.into_inner();
        
        let chunk_id = ChunkId::from_bytes(&req.chunk_id)?;
        
        let status = self.file_store
            .check_chunk(chunk_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        
        Ok(Response::new(CheckChunkResponse {
            status: status.to_proto(),
        }))
    }
    
    // ... other methods
}
```

## Dependencies

### Direct Dependencies
- **FileSystemService**: For all FUSE filesystem operations (delegates internally to RaftMember, MetadataStore, FileStore)
- **StorageRaftMember**: For internal metadata operations (primarily used by FileSystemService)
- **FileStore**: For inter-node chunk transfer operations
- **MetadataStore**: For internal metadata queries (primarily used by FileSystemService)
- **SnapshotStore**: For snapshot transfer to other nodes
- **TransactionLogStore**: For transaction log queries from other nodes
- **StorageNode**: For cluster management and health status

### External Dependencies
- `tonic`: gRPC framework
- `prost`: Protobuf serialization
- `tokio`: Async runtime
- `tower`: Middleware for rate limiting, auth
- `tracing`: Request tracing and logging

## Data Structures

```rust
pub struct EndpointConfig {
    pub listen_address: String,
    pub max_concurrent_requests: usize,
    pub request_timeout: Duration,
    pub enable_tls: bool,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub enable_auth: bool,
    pub auth_psk_path: Option<PathBuf>,
}

#[derive(Debug, thiserror::Error)]
pub enum EndpointError {
    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::transport::Error),
    
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    
    #[error("Authentication failed")]
    AuthenticationFailed,
    
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
    
    #[error("Internal error: {0}")]
    InternalError(String),
}
```

## Configuration

```toml
[endpoint]
listen_address = "0.0.0.0:7000"
max_concurrent_requests = 1000
request_timeout_secs = 30

# TLS PSK Authentication
enable_tls = true
enable_auth = true
identities_dir = "/etc/wormfs/identities/"
node_identity = "storage_node"  # Which PSK file in identities_dir to use for this node

# Rate limiting (two-level: per-client identity and overall)
[endpoint.rate_limit]
per_client_requests_per_second = 100
overall_requests_per_second = 1000
burst_size = 100
```

### PSK Identity System

WormFS uses TLS 1.3 with Pre-Shared Keys (PSK) for authentication:

- **Storage Nodes**: All storage nodes share the same PSK file (e.g., `storage_node`)
- **Clients**: Each client has its own PSK file for individual identity and permissions
- **PSK Storage**: All PSK files are stored in the `identities/` directory
- **File Format**: Each file contains one PSK, the filename represents the identity
- **Node Configuration**: The node's config specifies which PSK file to use for its identity

Example directory structure:
```
/etc/wormfs/identities/
  ├── storage_node  (shared by all storage nodes)
  ├── plex          (client identity for Plex)
  └── backup_client (client identity for backup jobs)
```

A PSK utility binary can be used to generate new PSK files:
```bash
wormfs-keygen --output /etc/wormfs/identities/new_client
```

## Error Handling

### Request Validation
- Invalid file IDs, stripe IDs, chunk IDs return InvalidArgument
- Missing required fields return InvalidArgument
- Malformed protobuf returns InvalidArgument

### Authentication Failures
- Missing credentials return Unauthenticated
- Invalid credentials return PermissionDenied
- Expired credentials return Unauthenticated

### Rate Limiting
- Exceeded rate limit returns ResourceExhausted
- Include retry-after header in response
- Log rate limit violations

### Internal Errors
- Component failures return Internal
- Include request ID for debugging
- Log full error context

## Testing Strategy

### Unit Tests
- Request parsing and validation
- Response serialization
- Error mapping
- Auth middleware

### Integration Tests
- End-to-end client requests
- Inter-node chunk transfer
- Snapshot streaming
- Lock acquisition/release

### Performance Tests
- Concurrent request handling
- Large file streaming
- Snapshot transfer throughput
- Request latency under load


## Design Decisions

Based on the requirements and constraints, the following key design decisions have been made:

### Transaction Protocol
- **Write Redirection**: Follower nodes reject write requests and redirect clients to the leader
- **Read Availability**: Reads can be performed against any node (with potential staleness)
- **Leader Discovery**: Clients use the `list_leaders()` API to discover current leader(s)
- **No Result Caching**: Transaction results are not cached on followers

### Authentication & Security  
- **TLS PSK**: Use TLS 1.3 with Pre-Shared Keys for authentication
- **Identity System**: Filename-based identity mapping in the `identities/` directory
- **Shared Node PSK**: All storage nodes share the same PSK file
- **Individual Client PSKs**: Each client has a unique PSK for identification

### Rate Limiting
- **Two-Level Limiting**: Per-client identity rate limits and overall node rate limits
- **Configuration**: Separately configurable limits for flexibility

### Features
- **Streaming**: Use streaming for large file reads
- **Health Checks**: Implement gRPC health check protocol
- **No Compression**: gRPC compression disabled initially
- **No Caching**: No metadata response caching
- **No Tracing**: OpenTelemetry tracing deferred to future versions
- **No Batching**: No batch operation support initially

### Backpressure
- **Component Overload**: Return ResourceExhausted status with retry-after headers
- **Queue Management**: Reject requests when internal queues are full
- **Connection Limits**: Enforce max_concurrent_requests limit
