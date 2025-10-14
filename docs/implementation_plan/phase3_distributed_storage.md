# Phase 3: Distributed Storage - Detailed Implementation Plan

## Overview
**Duration**: 3 weeks (15 working days)
**Goal**: Distribute chunk storage across multiple nodes using erasure coding
**Success Criteria**: Files are striped and distributed across 3+ node cluster with coordinated metadata and chunk operations
**Prerequisites**: Phase 2 complete (Raft consensus working for metadata)

## Component Implementation Order

### Week 1: StorageEndpoint & gRPC Infrastructure (Days 1-5)

#### Step 1: Protocol Buffers Definition
**File**: `proto/wormfs.proto`

**Tasks**:
1. Define gRPC service for filesystem operations
   ```protobuf
   syntax = "proto3";
   package wormfs.v1;

   service FilesystemService {
       // File operations
       rpc CreateFile(CreateFileRequest) returns (CreateFileResponse);
       rpc ReadFile(ReadFileRequest) returns (stream ReadFileResponse);
       rpc WriteFile(stream WriteFileRequest) returns (WriteFileResponse);
       rpc DeleteFile(DeleteFileRequest) returns (DeleteFileResponse);

       // Directory operations
       rpc CreateDirectory(CreateDirectoryRequest) returns (CreateDirectoryResponse);
       rpc ListDirectory(ListDirectoryRequest) returns (ListDirectoryResponse);
       rpc DeleteDirectory(DeleteDirectoryRequest) returns (DeleteDirectoryResponse);

       // Metadata operations
       rpc GetFileAttributes(GetFileAttributesRequest) returns (GetFileAttributesResponse);
       rpc SetFileAttributes(SetFileAttributesRequest) returns (SetFileAttributesResponse);

       // Lock operations
       rpc AcquireLock(AcquireLockRequest) returns (AcquireLockResponse);
       rpc ReleaseLock(ReleaseLockRequest) returns (ReleaseLockResponse);
       rpc ExtendLock(ExtendLockRequest) returns (ExtendLockResponse);
   }

   service ChunkService {
       // Inter-node chunk operations
       rpc ReadChunk(ReadChunkRequest) returns (ReadChunkResponse);
       rpc WriteChunk(WriteChunkRequest) returns (WriteChunkResponse);
       rpc VerifyChunk(VerifyChunkRequest) returns (VerifyChunkResponse);
       rpc StreamChunks(stream StreamChunksRequest) returns (stream StreamChunksResponse);

       // Stripe operations (for FUSE clients)
       rpc ReadStripe(ReadStripeRequest) returns (stream ReadStripeResponse);
       rpc WriteStripe(stream WriteStripeRequest) returns (WriteStripeResponse);
   }

   service AdminService {
       // Cluster management
       rpc GetClusterStatus(GetClusterStatusRequest) returns (GetClusterStatusResponse);
       rpc AddNode(AddNodeRequest) returns (AddNodeResponse);
       rpc RemoveNode(RemoveNodeRequest) returns (RemoveNodeResponse);

       // Storage management
       rpc GetStorageStats(GetStorageStatsRequest) returns (GetStorageStatsResponse);
       rpc AddDisk(AddDiskRequest) returns (AddDiskResponse);
       rpc RemoveDisk(RemoveDiskRequest) returns (RemoveDiskResponse);
   }
   ```

2. Define message types
   ```protobuf
   message CreateFileRequest {
       string path = 1;
       uint32 mode = 2;
       uint32 uid = 3;
       uint32 gid = 4;
   }

   message CreateFileResponse {
       uint64 file_id = 1;
       uint64 inode = 2;
       FileAttributes attributes = 3;
   }

   message WriteFileRequest {
       uint64 inode = 1;
       uint64 offset = 2;
       bytes data = 3;
       uint32 flags = 4;
   }

   message ReadChunkRequest {
       uint64 chunk_id = 1;
       bool verify_checksum = 2;
   }

   message ReadChunkResponse {
       bytes chunk_header = 1;
       bytes chunk_data = 2;
       uint32 checksum = 3;
   }
   ```

3. Add build script for protobuf compilation
   ```rust
   // build.rs
   fn main() -> Result<(), Box<dyn std::error::Error>> {
       tonic_build::configure()
           .build_server(true)
           .build_client(true)
           .compile(
               &["proto/wormfs.proto"],
               &["proto"],
           )?;
       Ok(())
   }
   ```

**Deliverables**:
- Complete protobuf service definitions
- Compiled Rust types and service traits
- Build integration working

#### Step 2: Basic gRPC Server Setup
**File**: `src/storage_endpoint/implementation.rs`

**Tasks**:
1. Implement StorageEndpoint with tonic
   ```rust
   use tonic::{transport::Server, Request, Response, Status};

   pub struct StorageEndpointImpl {
       config: EndpointConfig,
       filesystem_service: Arc<dyn FileSystemService>,
       file_store: Arc<dyn FileStore>,
       raft_member: Arc<dyn StorageRaftMember>,
   }

   impl StorageEndpointImpl {
       pub async fn new(
           config: EndpointConfig,
           filesystem_service: Arc<dyn FileSystemService>,
           file_store: Arc<dyn FileStore>,
           raft_member: Arc<dyn StorageRaftMember>,
       ) -> Result<Self, EndpointError> {
           Ok(Self {
               config,
               filesystem_service,
               file_store,
               raft_member,
           })
       }

       pub async fn serve(&self) -> Result<(), EndpointError> {
           let addr = self.config.listen_addr.parse()?;

           Server::builder()
               .add_service(FilesystemServiceServer::new(
                   FilesystemServiceHandler::new(self.filesystem_service.clone())
               ))
               .add_service(ChunkServiceServer::new(
                   ChunkServiceHandler::new(self.file_store.clone())
               ))
               .add_service(AdminServiceServer::new(
                   AdminServiceHandler::new(self.raft_member.clone())
               ))
               .serve(addr)
               .await?;

           Ok(())
       }
   }
   ```

2. Implement service handlers
   ```rust
   struct FilesystemServiceHandler {
       filesystem_service: Arc<dyn FileSystemService>,
   }

   #[tonic::async_trait]
   impl FilesystemService for FilesystemServiceHandler {
       async fn create_file(
           &self,
           request: Request<CreateFileRequest>,
       ) -> Result<Response<CreateFileResponse>, Status> {
           let req = request.into_inner();

           // Delegate to FileSystemService
           let result = self.filesystem_service
               .create(&req.path, req.mode, req.uid, req.gid)
               .await
               .map_err(|e| Status::internal(format!("Failed to create file: {}", e)))?;

           Ok(Response::new(CreateFileResponse {
               file_id: result.file_id.as_u64(),
               inode: result.inode,
               attributes: Some(to_proto_attributes(&result.attr)),
           }))
       }

       // Implement other filesystem operations...
   }
   ```

3. Add graceful shutdown
   ```rust
   impl StorageEndpointImpl {
       pub async fn serve_with_shutdown(
           &self,
           shutdown_rx: tokio::sync::oneshot::Receiver<()>,
       ) -> Result<(), EndpointError> {
           let addr = self.config.listen_addr.parse()?;

           Server::builder()
               .add_service(/* services */)
               .serve_with_shutdown(addr, async {
                   shutdown_rx.await.ok();
                   info!("Shutting down gRPC server");
               })
               .await?;

           Ok(())
       }
   }
   ```

**Deliverables**:
- gRPC server starting and accepting connections
- Basic request routing working
- Graceful shutdown support

#### Step 3: Authentication & Authorization
**File**: `src/storage_endpoint/auth.rs`

**Tasks**:
1. Implement authentication interceptor
   ```rust
   use tonic::service::interceptor::Interceptor;

   #[derive(Clone)]
   pub struct AuthInterceptor {
       valid_tokens: Arc<RwLock<HashSet<String>>>,
       node_certificates: Arc<RwLock<HashMap<NodeId, Certificate>>>,
   }

   impl Interceptor for AuthInterceptor {
       fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
           // Extract authorization header
           let token = req.metadata()
               .get("authorization")
               .ok_or_else(|| Status::unauthenticated("No auth token"))?
               .to_str()
               .map_err(|_| Status::unauthenticated("Invalid token format"))?;

           // Validate token
           if !self.valid_tokens.read().unwrap().contains(token) {
               return Err(Status::unauthenticated("Invalid token"));
           }

           // Add client identity to request extensions
           req.extensions_mut().insert(ClientIdentity {
               token: token.to_string(),
               node_id: None, // Parse from token
           });

           Ok(req)
       }
   }
   ```

2. Add per-node authentication for inter-node operations
   ```rust
   pub struct NodeAuthenticator {
       my_node_id: NodeId,
       node_certificates: Arc<RwLock<HashMap<NodeId, Certificate>>>,
   }

   impl NodeAuthenticator {
       pub fn verify_node(&self, request: &Request<()>) -> Result<NodeId, Status> {
           // Extract node certificate from TLS connection
           let peer_certs = request.peer_certs()
               .ok_or_else(|| Status::unauthenticated("No peer certificate"))?;

           // Verify certificate and extract node ID
           let node_id = self.extract_node_id(peer_certs)?;

           // Check if node is in cluster
           if !self.node_certificates.read().unwrap().contains_key(&node_id) {
               return Err(Status::permission_denied("Unknown node"));
           }

           Ok(node_id)
       }
   }
   ```

3. Add rate limiting
   ```rust
   pub struct RateLimiter {
       limits: Arc<RwLock<HashMap<ClientIdentity, TokenBucket>>>,
   }

   impl RateLimiter {
       pub fn check_rate_limit(&self, client: &ClientIdentity) -> Result<(), Status> {
           let mut limits = self.limits.write().unwrap();
           let bucket = limits.entry(client.clone())
               .or_insert_with(|| TokenBucket::new(100, Duration::from_secs(1)));

           if !bucket.consume(1) {
               return Err(Status::resource_exhausted("Rate limit exceeded"));
           }

           Ok(())
       }
   }
   ```

**Deliverables**:
- Token-based authentication for clients
- Certificate-based authentication for nodes
- Rate limiting middleware

#### Step 4: Chunk Transfer APIs
**File**: `src/storage_endpoint/chunk_handler.rs`

**Tasks**:
1. Implement chunk read handler
   ```rust
   struct ChunkServiceHandler {
       file_store: Arc<dyn FileStore>,
       node_auth: Arc<NodeAuthenticator>,
   }

   #[tonic::async_trait]
   impl ChunkService for ChunkServiceHandler {
       async fn read_chunk(
           &self,
           request: Request<ReadChunkRequest>,
       ) -> Result<Response<ReadChunkResponse>, Status> {
           // Authenticate node
           let requesting_node = self.node_auth.verify_node(&request)?;

           let req = request.into_inner();
           let chunk_id = ChunkId::new(req.chunk_id);

           // Read chunk from local storage
           let chunk_data = self.file_store
               .read_chunk_local(chunk_id)
               .await
               .map_err(|e| Status::not_found(format!("Chunk not found: {}", e)))?;

           // Verify checksum if requested
           if req.verify_checksum {
               let verification = self.file_store.verify_chunk(chunk_id).await
                   .map_err(|e| Status::internal(format!("Verification failed: {}", e)))?;

               if !verification.checksum_valid {
                   return Err(Status::data_loss("Chunk checksum mismatch"));
               }
           }

           Ok(Response::new(ReadChunkResponse {
               chunk_header: bincode::serialize(&chunk_data.header).unwrap(),
               chunk_data: chunk_data.data,
               checksum: chunk_data.header.chunk_checksum,
           }))
       }

       async fn write_chunk(
           &self,
           request: Request<WriteChunkRequest>,
       ) -> Result<Response<WriteChunkResponse>, Status> {
           // Authenticate node
           let requesting_node = self.node_auth.verify_node(&request)?;

           let req = request.into_inner();
           let chunk_id = ChunkId::new(req.chunk_id);

           // Deserialize chunk header
           let header: ChunkHeader = bincode::deserialize(&req.chunk_header)
               .map_err(|e| Status::invalid_argument(format!("Invalid header: {}", e)))?;

           // Verify checksum
           let computed_checksum = ChunkHeader::compute_checksum(&req.chunk_data);
           if computed_checksum != header.chunk_checksum {
               return Err(Status::invalid_argument("Checksum mismatch"));
           }

           // Write chunk locally
           let chunk_data = ChunkData {
               header,
               data: req.chunk_data,
           };

           self.file_store
               .write_chunk_local(chunk_id, chunk_data)
               .await
               .map_err(|e| Status::internal(format!("Write failed: {}", e)))?;

           Ok(Response::new(WriteChunkResponse {
               success: true,
           }))
       }

       async fn stream_chunks(
           &self,
           request: Request<tonic::Streaming<StreamChunksRequest>>,
       ) -> Result<Response<tonic::Streaming<StreamChunksResponse>>, Status> {
           let node_id = self.node_auth.verify_node(&request)?;
           let mut stream = request.into_inner();

           let (tx, rx) = mpsc::channel(10);

           tokio::spawn(async move {
               while let Some(req) = stream.next().await {
                   let req = match req {
                       Ok(r) => r,
                       Err(e) => {
                           let _ = tx.send(Err(e)).await;
                           break;
                       }
                   };

                   // Process chunk request
                   let result = self.handle_chunk_stream_request(req).await;
                   if tx.send(result).await.is_err() {
                       break;
                   }
               }
           });

           Ok(Response::new(ReceiverStream::new(rx)))
       }
   }
   ```

**Deliverables**:
- Chunk read/write APIs working
- Streaming chunk transfer
- Authentication and checksum verification

#### Step 5: Client Redirection
**File**: `src/storage_endpoint/redirection.rs`

**Tasks**:
1. Implement leader redirection for writes
   ```rust
   impl FilesystemServiceHandler {
       async fn ensure_leader(&self) -> Result<(), Status> {
           let metrics = self.raft_member.get_metrics().await;

           if metrics.role != RaftRole::Leader {
               let leader = metrics.current_leader
                   .ok_or_else(|| Status::unavailable("No leader elected"))?;

               let leader_addr = self.get_node_address(leader).await?;

               return Err(Status::failed_precondition(
                   format!("Not leader, redirect to: {}", leader_addr)
               ));
           }

           Ok(())
       }

       async fn create_file(
           &self,
           request: Request<CreateFileRequest>,
       ) -> Result<Response<CreateFileResponse>, Status> {
           // Check if we're the leader
           self.ensure_leader().await?;

           // Process request
           // ...
       }
   }
   ```

2. Add retry logic in gRPC client
   ```rust
   pub struct WormFsClient {
       current_leader: Arc<RwLock<Option<NodeId>>>,
       node_connections: Arc<RwLock<HashMap<NodeId, Channel>>>,
   }

   impl WormFsClient {
       pub async fn create_file_with_retry(
           &self,
           path: &str,
           mode: u32,
       ) -> Result<CreateFileResponse, ClientError> {
           let mut retries = 0;

           loop {
               let leader = self.current_leader.read().await.clone();
               let node = leader.unwrap_or_else(|| self.pick_random_node());

               let mut client = self.get_connection(node).await?;

               match client.create_file(CreateFileRequest {
                   path: path.to_string(),
                   mode,
                   uid: 1000,
                   gid: 1000,
               }).await {
                   Ok(resp) => return Ok(resp.into_inner()),
                   Err(status) if status.code() == Code::FailedPrecondition => {
                       // Extract leader from error message
                       if let Some(leader_addr) = self.parse_leader_redirect(&status) {
                           *self.current_leader.write().await = Some(leader_addr);
                       }

                       retries += 1;
                       if retries >= 3 {
                           return Err(ClientError::TooManyRedirects);
                       }
                   }
                   Err(e) => return Err(e.into()),
               }
           }
       }
   }
   ```

**Deliverables**:
- Write operations redirect to leader
- Client retry logic implemented
- Automatic leader discovery

---

### Week 2: Distributed FileStore Operations (Days 6-10)

#### Step 6: Chunk Placement Logic
**File**: `src/file_store/placement.rs`

**Tasks**:
1. Implement chunk placement algorithm
   ```rust
   pub struct ChunkPlacementPolicy {
       /// Minimum number of different nodes for chunk distribution
       min_node_diversity: usize,
       /// Minimum number of different disks per node
       min_disk_diversity: usize,
       /// Whether to avoid placing chunks on same rack
       rack_awareness: bool,
   }

   pub struct ChunkPlacer {
       policy: ChunkPlacementPolicy,
       cluster_state: Arc<RwLock<ClusterState>>,
   }

   impl ChunkPlacer {
       pub async fn place_chunks(
           &self,
           num_chunks: usize,
           stripe_id: StripeId,
       ) -> Result<Vec<ChunkPlacement>, PlacementError> {
           let cluster = self.cluster_state.read().await;

           // Get available nodes with capacity
           let mut available_nodes: Vec<NodeInfo> = cluster.nodes.values()
               .filter(|n| n.status == NodeStatus::Active)
               .filter(|n| n.has_capacity_for_chunk())
               .cloned()
               .collect();

           if available_nodes.len() < self.policy.min_node_diversity {
               return Err(PlacementError::InsufficientNodes);
           }

           let mut placements = Vec::new();

           // Simple round-robin with diversity constraints
           for chunk_index in 0..num_chunks {
               let node = self.select_node_for_chunk(
                   &available_nodes,
                   &placements,
                   chunk_index,
               )?;

               let disk = self.select_disk_on_node(&node)?;

               placements.push(ChunkPlacement {
                   chunk_index: chunk_index as u8,
                   node_id: node.node_id,
                   disk_id: disk.disk_id,
                   estimated_path: self.compute_chunk_path(stripe_id, chunk_index, disk.disk_id),
               });
           }

           Ok(placements)
       }

       fn select_node_for_chunk(
           &self,
           available: &[NodeInfo],
           existing: &[ChunkPlacement],
           chunk_index: usize,
       ) -> Result<NodeInfo, PlacementError> {
           // Count chunks per node in existing placements
           let mut node_counts: HashMap<NodeId, usize> = HashMap::new();
           for placement in existing {
               *node_counts.entry(placement.node_id).or_insert(0) += 1;
           }

           // Select node with fewest chunks, with tie-breaking by capacity
           available.iter()
               .min_by_key(|node| {
                   let count = node_counts.get(&node.node_id).copied().unwrap_or(0);
                   (count, node.total_capacity - node.used_capacity)
               })
               .cloned()
               .ok_or(PlacementError::NoAvailableNode)
       }
   }
   ```

2. Add blast radius protection
   ```rust
   impl ChunkPlacer {
       fn validate_placement(&self, placements: &[ChunkPlacement]) -> Result<(), PlacementError> {
           // Check node diversity
           let unique_nodes: HashSet<_> = placements.iter()
               .map(|p| p.node_id)
               .collect();

           if unique_nodes.len() < self.policy.min_node_diversity {
               return Err(PlacementError::InsufficientNodeDiversity);
           }

           // Check disk diversity per node
           let mut node_disks: HashMap<NodeId, HashSet<DiskId>> = HashMap::new();
           for placement in placements {
               node_disks.entry(placement.node_id)
                   .or_insert_with(HashSet::new)
                   .insert(placement.disk_id);
           }

           for (node_id, disks) in node_disks {
               if disks.len() < self.policy.min_disk_diversity {
                   return Err(PlacementError::InsufficientDiskDiversity { node_id });
               }
           }

           Ok(())
       }
   }
   ```

**Deliverables**:
- Chunk placement algorithm
- Node and disk diversity enforcement
- Blast radius protection

#### Step 7: Distributed Stripe Write
**File**: `src/file_store/distributed_write.rs`

**Tasks**:
1. Implement distributed stripe write with 2PC
   ```rust
   impl FileStoreImpl {
       pub async fn write_stripe_distributed(
           &self,
           file_id: FileId,
           stripe_id: StripeId,
           data: Vec<u8>,
           policy: StoragePolicy,
       ) -> Result<StripeMetadata, Error> {
           // Phase 0: Encode stripe locally
           let chunks = self.encode_stripe(data, &policy).await?;

           // Determine chunk placement
           let placements = self.chunk_placer
               .place_chunks(chunks.len(), stripe_id)
               .await?;

           // Phase 1: Stage chunks on target nodes
           let tx_id = TxId::new();
           let mut staged_chunks = Vec::new();

           for (chunk_data, placement) in chunks.into_iter().zip(placements.iter()) {
               let chunk_id = ChunkId::new(generate_chunk_id());

               // Stage chunk (local or remote)
               if placement.node_id == self.my_node_id {
                   self.stage_chunk(chunk_data).await?;
               } else {
                   self.stage_chunk_remote(
                       placement.node_id,
                       chunk_id,
                       chunk_data,
                   ).await?;
               }

               staged_chunks.push(ChunkMetadata {
                   chunk_id,
                   node_id: placement.node_id,
                   disk_id: placement.disk_id,
                   chunk_index: placement.chunk_index,
               });
           }

           // Phase 2: Commit metadata via Raft
           let stripe_metadata = StripeMetadata {
               stripe_id,
               file_id,
               offset: 0, // Caller provides
               size: data.len() as u64,
               checksum: crc32fast::hash(&data),
               chunks: staged_chunks.clone(),
           };

           // Propose through Raft (this triggers metadata 2PC)
           self.raft_member
               .propose_stripe_allocation(stripe_metadata.clone())
               .await
               .map_err(|e| {
                   // Rollback: discard staged chunks
                   self.cleanup_staged_chunks(&staged_chunks);
                   e
               })?;

           // Phase 3: Activate chunks after metadata commit
           for chunk_meta in &staged_chunks {
               if chunk_meta.node_id == self.my_node_id {
                   self.activate_chunk(chunk_meta.chunk_id).await?;
               } else {
                   self.activate_chunk_remote(
                       chunk_meta.node_id,
                       chunk_meta.chunk_id,
                   ).await?;
               }
           }

           Ok(stripe_metadata)
       }

       async fn stage_chunk_remote(
           &self,
           target_node: NodeId,
           chunk_id: ChunkId,
           chunk_data: ChunkData,
       ) -> Result<(), Error> {
           let mut client = self.get_node_client(target_node).await?;

           client.write_chunk(WriteChunkRequest {
               chunk_id: chunk_id.as_u64(),
               chunk_header: bincode::serialize(&chunk_data.header)?,
               chunk_data: chunk_data.data,
               staged: true, // Mark as staged
           }).await?;

           Ok(())
       }
   }
   ```

2. Handle write failures and cleanup
   ```rust
   impl FileStoreImpl {
       async fn cleanup_staged_chunks(&self, chunks: &[ChunkMetadata]) {
           for chunk_meta in chunks {
               if chunk_meta.node_id == self.my_node_id {
                   let _ = self.discard_staged_chunk(chunk_meta.chunk_id).await;
               } else {
                   let _ = self.discard_staged_chunk_remote(
                       chunk_meta.node_id,
                       chunk_meta.chunk_id,
                   ).await;
               }
           }
       }

       async fn discard_staged_chunk_remote(
           &self,
           target_node: NodeId,
           chunk_id: ChunkId,
       ) -> Result<(), Error> {
           let mut client = self.get_node_client(target_node).await?;

           client.delete_staged_chunk(DeleteStagedChunkRequest {
               chunk_id: chunk_id.as_u64(),
           }).await?;

           Ok(())
       }
   }
   ```

**Deliverables**:
- Distributed stripe write with 2PC
- Remote chunk staging
- Failure cleanup logic

#### Step 8: Distributed Stripe Read
**File**: `src/file_store/distributed_read.rs`

**Tasks**:
1. Implement distributed stripe read with reconstruction
   ```rust
   impl FileStoreImpl {
       pub async fn read_stripe_distributed(
           &self,
           file_id: FileId,
           stripe_id: StripeId,
       ) -> Result<Vec<u8>, Error> {
           // Get stripe metadata from MetadataStore
           let stripe_meta = self.metadata_store
               .get_stripe(stripe_id)
               .await?;

           // Fetch chunks in parallel
           let chunk_futures: Vec<_> = stripe_meta.chunks.iter()
               .map(|chunk_meta| {
                   self.fetch_chunk(chunk_meta.clone())
               })
               .collect();

           let chunk_results = futures::future::join_all(chunk_futures).await;

           // Collect successful chunks
           let mut available_chunks = Vec::new();
           let mut failed_chunks = Vec::new();

           for (idx, result) in chunk_results.into_iter().enumerate() {
               match result {
                   Ok(chunk_data) => available_chunks.push((idx, chunk_data)),
                   Err(e) => {
                       warn!("Failed to fetch chunk {}: {}", idx, e);
                       failed_chunks.push(idx);
                   }
               }
           }

           // Check if we have enough chunks for reconstruction
           let policy = StoragePolicy {
               data_shards: stripe_meta.chunks[0].data_shards,
               parity_shards: stripe_meta.chunks[0].parity_shards,
               stripe_size: stripe_meta.size,
               compression: CompressionAlgorithm::None,
           };

           if available_chunks.len() < policy.data_shards as usize {
               return Err(Error::InsufficientChunks {
                   needed: policy.data_shards as usize,
                   available: available_chunks.len(),
               });
           }

           // Reconstruct stripe using erasure coding
           let stripe_data = self.decode_stripe(available_chunks, &policy).await?;

           // Verify stripe checksum
           let computed_checksum = crc32fast::hash(&stripe_data);
           if computed_checksum != stripe_meta.checksum {
               return Err(Error::ChecksumMismatch {
                   expected: stripe_meta.checksum.to_string(),
                   actual: computed_checksum.to_string(),
               });
           }

           Ok(stripe_data)
       }

       async fn fetch_chunk(&self, chunk_meta: ChunkMetadata) -> Result<ChunkData, Error> {
           if chunk_meta.node_id == self.my_node_id {
               // Local read
               self.read_chunk_local(chunk_meta.chunk_id).await
           } else {
               // Remote read
               self.read_chunk_remote(chunk_meta.node_id, chunk_meta.chunk_id).await
           }
       }

       async fn read_chunk_remote(
           &self,
           target_node: NodeId,
           chunk_id: ChunkId,
       ) -> Result<ChunkData, Error> {
           let mut client = self.get_node_client(target_node).await?;

           let response = client.read_chunk(ReadChunkRequest {
               chunk_id: chunk_id.as_u64(),
               verify_checksum: true,
           }).await?;

           let resp = response.into_inner();
           let header: ChunkHeader = bincode::deserialize(&resp.chunk_header)?;

           Ok(ChunkData {
               header,
               data: resp.chunk_data,
           })
       }
   }
   ```

2. Add chunk caching for performance
   ```rust
   impl FileStoreImpl {
       async fn fetch_chunk_with_cache(
           &self,
           chunk_meta: ChunkMetadata,
       ) -> Result<ChunkData, Error> {
           // Check cache first
           if let Some(cached) = self.get_cached_chunk(chunk_meta.chunk_id).await? {
               return Ok(ChunkData {
                   header: cached.header,
                   data: cached.data,
               });
           }

           // Fetch from source
           let chunk_data = self.fetch_chunk(chunk_meta).await?;

           // Cache for future reads
           self.cache_chunk(chunk_meta.chunk_id, chunk_data.data.clone()).await?;

           Ok(chunk_data)
       }
   }
   ```

**Deliverables**:
- Distributed stripe read
- Parallel chunk fetching
- Erasure code reconstruction
- Chunk caching

#### Step 9: Coordination with Raft for Metadata
**File**: `src/storage_raft_member/stripe_operations.rs`

**Tasks**:
1. Add stripe allocation to Raft state machine
   ```rust
   // Define operation type
   pub enum MetadataOperation {
       CreateFile { path: String, inode: u64, metadata: FileMetadata },
       AllocateStripe { stripe_metadata: StripeMetadata },
       UpdateFileSize { file_id: FileId, new_size: u64 },
       DeleteFile { file_id: FileId },
       // ... other operations
   }

   impl StorageRaftMemberImpl {
       pub async fn propose_stripe_allocation(
           &self,
           stripe_metadata: StripeMetadata,
       ) -> Result<(), Error> {
           let operation = MetadataOperation::AllocateStripe { stripe_metadata };
           let data = bincode::serialize(&operation)?;

           // Propose through Raft
           let result = self.raft.client_write(data).await
               .map_err(|e| Error::RaftError(format!("Failed to replicate: {}", e)))?;

           Ok(())
       }
   }
   ```

2. Update state machine to handle stripe allocation
   ```rust
   impl StateMachine {
       async fn apply_stripe_allocation(
           &mut self,
           stripe_metadata: StripeMetadata,
       ) -> Result<(), Error> {
           // Store stripe metadata
           self.metadata_store
               .allocate_stripes(
                   stripe_metadata.file_id,
                   vec![stripe_metadata.into()],
               )
               .await?;

           Ok(())
       }

       async fn apply(&mut self, entry: &LogEntry) -> Result<Response, Error> {
           let operation: MetadataOperation = bincode::deserialize(&entry.data)?;

           match operation {
               MetadataOperation::AllocateStripe { stripe_metadata } => {
                   self.apply_stripe_allocation(stripe_metadata).await?;
                   Ok(Response::StripeAllocated)
               }
               // Handle other operations...
           }
       }
   }
   ```

**Deliverables**:
- Stripe allocation through Raft
- State machine integration
- Metadata consistency across nodes

#### Step 10: Testing Distributed Storage
**File**: `tests/integration/phase3_distributed_test.rs`

**Tasks**:
1. Test distributed stripe write
   ```rust
   #[tokio::test]
   async fn test_distributed_stripe_write() {
       let cluster = TestCluster::new(3).await;

       // Create file
       let file_id = cluster.leader()
           .create_file("/test.dat", 0o644)
           .await
           .unwrap();

       // Write stripe
       let data = vec![0xAB; 1024 * 1024]; // 1MB
       let stripe_metadata = cluster.leader()
           .write_stripe(file_id, data.clone())
           .await
           .unwrap();

       // Verify chunks distributed across nodes
       let node_ids: HashSet<_> = stripe_metadata.chunks.iter()
           .map(|c| c.node_id)
           .collect();
       assert!(node_ids.len() >= 2, "Chunks should be distributed");

       // Verify each node has its chunks
       for chunk_meta in &stripe_metadata.chunks {
           let node = cluster.get_node(chunk_meta.node_id);
           let chunk = node.file_store
               .read_chunk_local(chunk_meta.chunk_id)
               .await;
           assert!(chunk.is_ok());
       }
   }
   ```

2. Test read with reconstruction
   ```rust
   #[tokio::test]
   async fn test_read_with_node_failure() {
       let cluster = TestCluster::new(5).await;

       // Write file with 3+2 erasure coding
       let file_id = cluster.leader()
           .create_file("/test.dat", 0o644)
           .await
           .unwrap();

       let data = vec![0xCD; 2 * 1024 * 1024]; // 2MB
       cluster.leader()
           .write_stripe(file_id, data.clone())
           .await
           .unwrap();

       // Kill 2 nodes (within parity tolerance)
       let metadata = cluster.get_file_metadata(file_id).await.unwrap();
       let stripe = &metadata.stripes[0];
       let nodes_to_kill: Vec<_> = stripe.chunks.iter()
           .map(|c| c.node_id)
           .take(2)
           .collect();

       for node_id in nodes_to_kill {
           cluster.stop_node(node_id).await;
       }

       // Read should still succeed
       let read_data = cluster.any_node()
           .read_stripe(file_id, stripe.stripe_id)
           .await
           .unwrap();

       assert_eq!(read_data, data);
   }
   ```

**Deliverables**:
- Distributed write tests passing
- Read with reconstruction verified
- Node failure tolerance confirmed

---

### Week 3: FileSystemService Integration & Robustness (Days 11-15)

#### Step 11: Distributed File Locking
**File**: `src/filesystem_service/locking.rs`

**Tasks**:
1. Implement distributed lock acquisition
   ```rust
   impl FileSystemServiceImpl {
       pub async fn acquire_lock_distributed(
           &self,
           file_id: FileId,
           client_id: ClientId,
           lock_type: LockType,
       ) -> Result<u64, Error> {
           // Propose lock acquisition through Raft
           let operation = MetadataOperation::AcquireLock {
               file_id,
               client_id,
               lock_type,
               expires_at: SystemTime::now() + Duration::from_secs(30),
           };

           let lock_id = self.raft_member
               .propose_operation(operation)
               .await?;

           Ok(lock_id)
       }

       pub async fn release_lock_distributed(
           &self,
           file_id: FileId,
           client_id: ClientId,
       ) -> Result<(), Error> {
           let operation = MetadataOperation::ReleaseLock { file_id, client_id };

           self.raft_member
               .propose_operation(operation)
               .await?;

           Ok(())
       }
   }
   ```

2. Add lock conflict detection
   ```rust
   impl StateMachine {
       fn check_lock_conflict(
           &self,
           file_id: FileId,
           client_id: ClientId,
           lock_type: LockType,
       ) -> Result<(), Error> {
           let existing_locks = self.metadata_store
               .get_file_locks(file_id)
               .await?;

           for lock in existing_locks {
               if lock.client_id == client_id {
                   continue; // Same client
               }

               if lock.lock_type == LockType::Write || lock_type == LockType::Write {
                   return Err(Error::LockConflict);
               }
           }

           Ok(())
       }
   }
   ```

**Deliverables**:
- Distributed lock acquisition
- Lock conflict detection
- Lock expiration handling

#### Step 12: Partial Stripe Updates
**File**: `src/filesystem_service/partial_write.rs`

**Tasks**:
1. Implement read-modify-write for partial stripes
   ```rust
   impl FileSystemServiceImpl {
       pub async fn write_partial(
           &self,
           file_id: FileId,
           offset: u64,
           data: &[u8],
       ) -> Result<usize, Error> {
           // Determine affected stripes
           let stripe_size = self.get_stripe_size(file_id).await?;
           let start_stripe = offset / stripe_size;
           let end_stripe = (offset + data.len() as u64) / stripe_size;

           let mut bytes_written = 0;

           for stripe_index in start_stripe..=end_stripe {
               let stripe_offset = stripe_index * stripe_size;
               let write_offset_in_stripe = if stripe_index == start_stripe {
                   offset % stripe_size
               } else {
                   0
               };

               let write_end = if stripe_index == end_stripe {
                   (offset + data.len() as u64) % stripe_size
               } else {
                   stripe_size
               };

               let write_len = write_end - write_offset_in_stripe;

               // Read-modify-write for partial stripe
               let stripe_data = if write_offset_in_stripe != 0 || write_end != stripe_size {
                   // Partial write - need read-modify-write
                   let mut existing = self.file_store
                       .read_stripe(file_id, StripeId::new(stripe_index))
                       .await
                       .unwrap_or_else(|_| vec![0; stripe_size as usize]);

                   // Modify the relevant portion
                   let start = write_offset_in_stripe as usize;
                   let end = start + write_len as usize;
                   existing[start..end].copy_from_slice(
                       &data[bytes_written..bytes_written + write_len as usize]
                   );

                   existing
               } else {
                   // Full stripe write
                   data[bytes_written..bytes_written + stripe_size as usize].to_vec()
               };

               // Write modified stripe
               self.file_store
                   .write_stripe_distributed(
                       file_id,
                       StripeId::new(stripe_index),
                       stripe_data,
                       self.get_storage_policy(file_id).await?,
                   )
                   .await?;

               bytes_written += write_len as usize;
           }

           Ok(bytes_written)
       }
   }
   ```

2. Add concurrent write coordination
   ```rust
   impl FileSystemServiceImpl {
       pub async fn coordinated_write(
           &self,
           file_id: FileId,
           offset: u64,
           data: &[u8],
           client_id: ClientId,
       ) -> Result<usize, Error> {
           // Acquire write lock
           let _lock = self.acquire_lock_distributed(
               file_id,
               client_id,
               LockType::Write,
           ).await?;

           // Perform write with lock held
           let result = self.write_partial(file_id, offset, data).await;

           // Lock released automatically when _lock drops

           result
       }
   }
   ```

**Deliverables**:
- Partial stripe writes
- Read-modify-write logic
- Concurrent write coordination

#### Step 13: Update StorageNode Integration
**File**: `src/storage_node/mod.rs`

**Tasks**:
1. Integrate StorageEndpoint into StorageNode
   ```rust
   pub struct StorageNode {
       config: StorageNodeConfig,
       metadata_store: Arc<MetadataStoreImpl>,
       file_store: Arc<FileStoreImpl>,
       filesystem_service: Arc<FileSystemServiceImpl>,
       storage_network: Arc<StorageNetworkHandle>,
       raft_member: Arc<StorageRaftMemberImpl>,
       storage_endpoint: Arc<StorageEndpointImpl>,
   }

   impl StorageNode {
       pub async fn new(config: StorageNodeConfig) -> Result<Self, Error> {
           // Initialize components in order (from Phase 1 & 2)
           let metadata_store = Arc::new(MetadataStoreImpl::new(config.metadata.clone())?);
           let file_store = Arc::new(FileStoreImpl::new(config.file_store.clone())?);

           // Initialize network (Phase 2)
           let storage_network = Arc::new(
               StorageNetworkFactory::create(config.network.clone()).await?
           );

           // Initialize Raft (Phase 2)
           let raft_member = Arc::new(
               StorageRaftMemberImpl::new(
                   config.raft.clone(),
                   metadata_store.clone(),
                   storage_network.clone(),
               ).await?
           );

           // Initialize filesystem service (Phase 1, updated)
           let filesystem_service = Arc::new(
               FileSystemServiceImpl::new(
                   metadata_store.clone(),
                   file_store.clone(),
                   raft_member.clone(),
               )?
           );

           // Initialize storage endpoint (Phase 3)
           let storage_endpoint = Arc::new(
               StorageEndpointImpl::new(
                   config.endpoint.clone(),
                   filesystem_service.clone(),
                   file_store.clone(),
                   raft_member.clone(),
               ).await?
           );

           Ok(Self {
               config,
               metadata_store,
               file_store,
               filesystem_service,
               storage_network,
               raft_member,
               storage_endpoint,
           })
       }

       pub async fn start(&self) -> Result<(), Error> {
           // Start network event loop
           let network = self.storage_network.clone();
           tokio::spawn(async move {
               network.run().await;
           });

           // Start Raft
           self.raft_member.start().await?;

           // Start gRPC endpoint
           let endpoint = self.storage_endpoint.clone();
           tokio::spawn(async move {
               endpoint.serve().await;
           });

           info!("StorageNode started successfully");
           Ok(())
       }
   }
   ```

**Deliverables**:
- StorageEndpoint integrated
- All components wired together
- Startup sequence working

#### Step 14: End-to-End Testing
**File**: `tests/integration/phase3_e2e_test.rs`

**Tasks**:
1. Test complete distributed file operations
   ```rust
   #[tokio::test]
   async fn test_distributed_file_lifecycle() {
       let cluster = TestCluster::new(5).await;

       // Connect client to cluster
       let client = WormFsClient::connect(cluster.nodes()).await.unwrap();

       // Create file
       let file = client.create_file("/data/test.txt", 0o644).await.unwrap();

       // Write data (multiple stripes)
       let data = generate_test_data(10 * 1024 * 1024); // 10MB
       let written = client.write_file(file.inode, 0, &data).await.unwrap();
       assert_eq!(written, data.len());

       // Read back
       let read_data = client.read_file(file.inode, 0, data.len()).await.unwrap();
       assert_eq!(read_data, data);

       // Verify distribution
       let metadata = cluster.get_file_metadata_from_any_node(file.file_id).await.unwrap();
       let all_nodes: HashSet<_> = metadata.stripes.iter()
           .flat_map(|s| s.chunks.iter().map(|c| c.node_id))
           .collect();
       assert!(all_nodes.len() >= 3, "Data should be distributed across multiple nodes");

       // Test with node failures
       cluster.stop_node(all_nodes.iter().next().unwrap().clone()).await;

       // Read should still work
       let read_after_failure = client.read_file(file.inode, 0, data.len()).await.unwrap();
       assert_eq!(read_after_failure, data);

       // Delete file
       client.delete_file(file.inode).await.unwrap();
   }
   ```

2. Test concurrent operations
   ```rust
   #[tokio::test]
   async fn test_concurrent_writes() {
       let cluster = TestCluster::new(3).await;
       let client = Arc::new(WormFsClient::connect(cluster.nodes()).await.unwrap());

       // Create file
       let file = client.create_file("/concurrent.dat", 0o644).await.unwrap();

       // Concurrent writes to different offsets
       let mut handles = vec![];
       for i in 0..10 {
           let client = client.clone();
           let file = file.clone();
           handles.push(tokio::spawn(async move {
               let data = vec![i as u8; 1024 * 1024];
               let offset = i * 1024 * 1024;
               client.write_file(file.inode, offset, &data).await.unwrap();
           }));
       }

       // Wait for all writes
       for handle in handles {
           handle.await.unwrap();
       }

       // Verify all data
       for i in 0..10 {
           let offset = i * 1024 * 1024;
           let data = client.read_file(file.inode, offset, 1024 * 1024).await.unwrap();
           assert_eq!(data, vec![i as u8; 1024 * 1024]);
       }
   }
   ```

**Deliverables**:
- End-to-end distributed operations
- Concurrent access tests
- Node failure scenarios

#### Step 15: Performance Testing & Documentation
**File**: `tests/performance/phase3_benchmarks.rs`

**Tasks**:
1. Benchmark distributed operations
   ```rust
   #[tokio::test]
   async fn bench_distributed_write_throughput() {
       let cluster = TestCluster::new(5).await;
       let client = WormFsClient::connect(cluster.nodes()).await.unwrap();

       let file = client.create_file("/bench.dat", 0o644).await.unwrap();

       let data = vec![0u8; 10 * 1024 * 1024]; // 10MB
       let iterations = 10;

       let start = Instant::now();
       for i in 0..iterations {
           client.write_file(file.inode, i * data.len() as u64, &data)
               .await
               .unwrap();
       }
       let elapsed = start.elapsed();

       let total_bytes = (data.len() * iterations) as f64;
       let throughput = total_bytes / elapsed.as_secs_f64() / (1024.0 * 1024.0);

       println!("Write throughput: {:.2} MB/s", throughput);
       assert!(throughput > 10.0, "Write throughput should be > 10 MB/s");
   }

   #[tokio::test]
   async fn bench_distributed_read_throughput() {
       let cluster = TestCluster::new(5).await;
       let client = WormFsClient::connect(cluster.nodes()).await.unwrap();

       // Setup: write data
       let file = client.create_file("/bench_read.dat", 0o644).await.unwrap();
       let data = vec![0u8; 100 * 1024 * 1024]; // 100MB
       client.write_file(file.inode, 0, &data).await.unwrap();

       // Benchmark reads
       let start = Instant::now();
       let iterations = 10;
       for _ in 0..iterations {
           let _ = client.read_file(file.inode, 0, data.len()).await.unwrap();
       }
       let elapsed = start.elapsed();

       let total_bytes = (data.len() * iterations) as f64;
       let throughput = total_bytes / elapsed.as_secs_f64() / (1024.0 * 1024.0);

       println!("Read throughput: {:.2} MB/s", throughput);
       assert!(throughput > 50.0, "Read throughput should be > 50 MB/s");
   }
   ```

2. Create Phase 3 documentation
   - User guide for distributed storage
   - Chunk placement policies
   - Performance tuning guide
   - Troubleshooting common issues

**Deliverables**:
- Performance benchmarks
- Throughput measurements
- Complete documentation
- Phase 3 milestone achieved

---

## Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Chunk Distribution | >80% | Percentage of stripes with chunks on multiple nodes |
| Write Latency | <500ms | Average latency for 1MB write in 3-node cluster |
| Read Latency | <200ms | Average latency for 1MB read in 3-node cluster |
| Reconstruction Success | 100% | Read success rate with N-1 node failures (N=parity) |
| gRPC Throughput | >100 req/s | Requests per second for file operations |
| Test Coverage | >85% | Unit and integration tests for distributed storage |

## Risk Mitigation

### Technical Risks:
1. **Network Partitions**: Implement proper timeout and retry logic
2. **Chunk Placement Complexity**: Start simple, optimize later
3. **gRPC Performance**: Use streaming for large transfers
4. **Two-Phase Commit Failures**: Comprehensive cleanup logic

### Fallback Options:
- If gRPC issues: Use direct HTTP/2 with reqwest
- If placement is slow: Use simple round-robin initially
- If 2PC is complex: Simplify to single-phase for Phase 3

## Dependencies

### External Crates:
- `tonic` - gRPC server and client
- `prost` - Protocol Buffers
- `tokio` - Async runtime
- `futures` - Async utilities
- `reed-solomon-erasure` - Erasure coding (from Phase 1)

## Integration Points

### Phase 1 Components:
- **MetadataStore**: Query stripe metadata
- **FileStore**: Local chunk storage
- **FileSystemService**: FUSE operations

### Phase 2 Components:
- **StorageNetwork**: Node-to-node communication
- **StorageRaftMember**: Metadata consensus
- **TransactionLogStore**: No direct integration

### New in Phase 3:
- **StorageEndpoint**: gRPC API layer
- **Distributed FileStore operations**: Remote chunk access
- **Distributed locking**: Coordinated through Raft

## Next Steps After Phase 3

Once Phase 3 is complete and tested:
1. Benchmark cluster performance with varying node counts
2. Test with production-like workloads
3. Prepare for Phase 4 (robustness and recovery)
4. Consider adding chunk migration for rebalancing

## Notes

- Keep Phase 1 and Phase 2 functionality working
- Focus on correctness over optimization initially
- Document all distributed coordination logic
- Prepare comprehensive test scenarios for failure modes
- Monitor network bandwidth usage during testing