# StorageEndpoint Component Design

## Purpose & Responsibilities

StorageEndpoint is the gRPC API server that exposes the storage node's functionality to clients and other storage nodes. Its responsibilities include:

- Providing FUSE filesystem operations API for client nodes
- Exposing chunk read/write APIs for inter-node communication
- Providing snapshot transfer APIs for node recovery
- Exposing transaction log APIs for Raft replication
- Handling administrative operations (cluster management, monitoring)
- Managing client authentication and authorization
- Implementing request rate limiting and backpressure
- Routing requests to appropriate internal components

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
│   → Forward to Raft Leader          │
│   → Wait for response               │
│   → Return result to client         │
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

#### Transaction Prepare/Commit/Abort (Inter-Node)

```
Raft Leader → Follower (via Raft consensus)
     ↓
TransactionService.PrepareChunk()
     ↓
FileStore.prepare_chunk()
     ↓
Return PrepareVote

     OR

Raft Leader → Follower (via Raft apply)
     ↓
TransactionService.CommitChunk() / AbortChunk()
     ↓
FileStore.commit_chunk() / abort_chunk()
     ↓
Return Success
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

# TLS configuration
enable_tls = true
tls_cert_path = "/etc/wormfs/server.crt"
tls_key_path = "/etc/wormfs/server.key"

# Authentication
enable_auth = true
auth_psk_path = "/etc/wormfs/client_keys/"

# Rate limiting
[endpoint.rate_limit]
requests_per_second = 1000
burst_size = 100
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

### Transaction Protocol Integration

The StorageEndpoint integrates with the two-phase commit protocol for write operations:

#### Leader Request Handling

When a write request arrives at the Raft leader node:

1. **Parse Request**: Extract file_id, stripe_id, data, and storage policy
2. **Apply Erasure Coding**: Encode data into k+m shards using FileStore encoder
3. **Compute Chunk Assignments**: Select target nodes/disks for each chunk
4. **Create Transaction**: Generate unique tx_id
5. **Propose via Raft**: Submit `TransactionPrepare` operation containing:
   - Transaction ID
   - Metadata changes (file size updates, etc.)
   - Complete chunk assignments with encoded data
6. **Wait for 2PC Completion**: Block until transaction commits or aborts
7. **Return Result**: Send chunk locations to client

#### Follower Request Handling

When a write request arrives at a Raft follower node:

1. **Detect Non-Leader**: Check Raft leader status
2. **Get Leader Address**: Query current leader from Raft state
3. **Forward Request**: Create gRPC client and forward entire request to leader
4. **Wait for Response**: Block until leader completes transaction
5. **Return Result**: Forward leader's response to client

**Note**: This design ensures all write coordination happens at the Raft leader, simplifying the transaction protocol and avoiding distributed coordination overhead.

#### Inter-Node Transaction RPC

For transaction prepare/commit/abort operations between nodes:

```rust
// Note: These are internal operations triggered by Raft, not exposed in public gRPC API

// Called by Raft leader on followers during PREPARE phase
async fn prepare_chunk_internal(tx_id: TxId, chunk_data: ChunkData) -> PrepareVote {
    self.file_store.prepare_chunk(tx_id, chunk_data).await
}

// Called by Raft apply on all nodes during COMMIT phase
async fn commit_chunk_internal(tx_id: TxId, chunk_id: ChunkId) {
    self.file_store.commit_chunk(tx_id, chunk_id).await
}

// Called by Raft apply on all nodes during ABORT phase
async fn abort_chunk_internal(tx_id: TxId, chunk_id: ChunkId) {
    self.file_store.abort_chunk(tx_id, chunk_id).await
}
```

These operations are triggered internally by the Raft state machine, not exposed as public gRPC endpoints.

## Open Questions

### Transaction Protocol Questions

1. **Leader Forwarding**: Should follower nodes always forward writes to leader, or should they be able to proxy the request while streaming data directly to chunk storage nodes?

2. **Client Retries**: If a client's request to a follower fails during forwarding, should the client automatically retry with the leader address, or always retry with the same node?

3. **Forwarding Timeout**: What timeout should we use for forwarding requests to the leader? Should it be longer than normal request timeout?

4. **Leader Discovery**: How should clients discover the current leader? Via DNS, via follower redirect, or via explicit leader query API?

5. **Transaction Result Caching**: Should we cache recent transaction results on followers to handle duplicate requests during network partitions?

### General Questions

6. **Authentication**: Should we use TLS 1.3 with PSK, mutual TLS, or both?

2. **Rate Limiting**: Should rate limits be per-client, per-endpoint, or global?

3. **Streaming**: Should large file reads/writes use streaming or chunked transfers?

4. **Compression**: Should we enable gRPC compression for all responses or selectively?

5. **Health Checks**: Should we implement gRPC health check protocol for load balancers?

6. **Metadata Caching**: Should we cache metadata responses to reduce Raft load?

7. **Request Tracing**: Should we implement OpenTelemetry for distributed tracing?

8. **Retries**: Should the server implement automatic retries for transient failures?

9. **Backpressure**: How should we handle backpressure when components are overloaded?

10. **API Versioning**: How should we version the gRPC API for compatibility?

11. **Batch Operations**: Should we support batching multiple operations in a single request?

12. **WebSocket Support**: Should we support WebSocket for real-time updates to clients?

13. **REST API**: Should we provide a REST API alongside gRPC for debugging/admin tools?

14. **Circuit Breakers**: Should we implement circuit breakers for downstream components?

15. **Request Prioritization**: Should we prioritize certain request types (admin, metadata, data)?
