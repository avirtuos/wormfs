# Phase 4: Robustness & Recovery - Detailed Implementation Plan

## Overview
**Duration**: 2 weeks (10 working days)
**Goal**: Add robustness and self-healing capabilities to handle node failures and data integrity issues
**Success Criteria**: System automatically detects and repairs data integrity issues, recovers nodes efficiently from snapshots, and maintains availability during failures
**Prerequisites**: Phase 3 complete (distributed storage working across cluster)

## Component Implementation Order

### Week 1: SnapshotStore & Metadata Recovery (Days 1-5)

#### Step 1: Snapshot Creation & Storage
**File**: `src/snapshot_store/implementation.rs`

**Tasks**:
1. Implement snapshot ingestion
   ```rust
   use std::path::{Path, PathBuf};
   use tokio::fs;
   use serde::{Serialize, Deserialize};

   pub struct SnapshotStoreImpl {
       config: Config,
       snapshot_dir: PathBuf,
       snapshots: Arc<RwLock<BTreeMap<u64, SnapshotInfo>>>,
   }

   impl SnapshotStoreImpl {
       pub async fn new(config: Config) -> Result<Self, Error> {
           // Create snapshot directory
           fs::create_dir_all(&config.snapshot_dir).await?;

           let mut store = Self {
               config,
               snapshot_dir: config.snapshot_dir.clone(),
               snapshots: Arc::new(RwLock::new(BTreeMap::new())),
           };

           // Scan existing snapshots
           store.scan_snapshots().await?;

           Ok(store)
       }

       async fn scan_snapshots(&mut self) -> Result<(), Error> {
           let mut entries = fs::read_dir(&self.snapshot_dir).await?;

           while let Some(entry) = entries.next_entry().await? {
               let path = entry.path();
               if path.extension().and_then(|s| s.to_str()) == Some("json") {
                   let metadata = fs::read_to_string(&path).await?;
                   let info: SnapshotInfo = serde_json::from_str(&metadata)?;

                   self.snapshots.write().await.insert(info.snapshot_id, info);
               }
           }

           Ok(())
       }
   }
   ```

2. Implement snapshot ingestion
   ```rust
   #[async_trait]
   impl SnapshotStore for SnapshotStoreImpl {
       async fn ingest_snapshot(
           &self,
           snapshot_id: u64,
           log_index: u64,
           log_term: u64,
           source_path: &Path,
       ) -> Result<(), Error> {
           // Copy snapshot file to storage
           let snapshot_path = self.snapshot_dir.join(format!("snapshot_{:08}.db", snapshot_id));
           fs::copy(source_path, &snapshot_path).await?;

           // Compress if configured
           if self.config.compression != CompressionAlgorithm::None {
               let compressed_path = self.compress_snapshot(&snapshot_path).await?;
               fs::remove_file(&snapshot_path).await?;
               fs::rename(compressed_path, &snapshot_path).await?;
           }

           // Create metadata
           let info = SnapshotInfo {
               snapshot_id,
               log_index,
               log_term,
               created_at: SystemTime::now(),
               size_bytes: fs::metadata(&snapshot_path).await?.len(),
               compression: self.config.compression,
               checksum: self.compute_checksum(&snapshot_path).await?,
           };

           // Write metadata file
           let metadata_path = self.snapshot_dir.join(format!("snapshot_{:08}.json", snapshot_id));
           let metadata_json = serde_json::to_string_pretty(&info)?;
           fs::write(metadata_path, metadata_json).await?;

           // Update registry
           self.snapshots.write().await.insert(snapshot_id, info.clone());

           info!("Ingested snapshot {} at log index {}", snapshot_id, log_index);

           Ok(())
       }
   }
   ```

3. Add compression support
   ```rust
   impl SnapshotStoreImpl {
       async fn compress_snapshot(&self, path: &Path) -> Result<PathBuf, Error> {
           let compressed_path = path.with_extension("db.zst");

           let input = fs::read(path).await?;
           let compressed = tokio::task::spawn_blocking(move || {
               zstd::encode_all(&input[..], 3) // Compression level 3
           }).await??;

           fs::write(&compressed_path, compressed).await?;

           Ok(compressed_path)
       }

       async fn decompress_snapshot(&self, path: &Path) -> Result<Vec<u8>, Error> {
           let compressed = fs::read(path).await?;

           let decompressed = tokio::task::spawn_blocking(move || {
               zstd::decode_all(&compressed[..])
           }).await??;

           Ok(decompressed)
       }

       async fn compute_checksum(&self, path: &Path) -> Result<String, Error> {
           let data = fs::read(path).await?;
           let checksum = crc32fast::hash(&data);
           Ok(format!("{:08x}", checksum))
       }
   }
   ```

**Deliverables**:
- Snapshot ingestion working
- Compression with zstd
- Checksum verification
- Metadata tracking

#### Step 2: Snapshot Retrieval & Streaming
**File**: `src/snapshot_store/retrieval.rs`

**Tasks**:
1. Implement snapshot retrieval
   ```rust
   #[async_trait]
   impl SnapshotStore for SnapshotStoreImpl {
       async fn get_latest_snapshot(&self) -> Result<Option<SnapshotInfo>, Error> {
           let snapshots = self.snapshots.read().await;
           Ok(snapshots.values().last().cloned())
       }

       async fn get_snapshot(&self, snapshot_id: u64) -> Result<Option<SnapshotInfo>, Error> {
           let snapshots = self.snapshots.read().await;
           Ok(snapshots.get(&snapshot_id).cloned())
       }

       async fn open_snapshot(&self, snapshot_id: u64) -> Result<Box<dyn SnapshotReader>, Error> {
           let snapshots = self.snapshots.read().await;
           let info = snapshots.get(&snapshot_id)
               .ok_or(Error::SnapshotNotFound(snapshot_id))?
               .clone();

           let path = self.snapshot_dir.join(format!("snapshot_{:08}.db", snapshot_id));

           let reader: Box<dyn SnapshotReader> = if info.compression != CompressionAlgorithm::None {
               // Decompress and return reader
               let data = self.decompress_snapshot(&path).await?;
               Box::new(MemorySnapshotReader::new(data, info))
           } else {
               // Direct file reader
               Box::new(FileSnapshotReader::new(path, info).await?)
           };

           Ok(reader)
       }
   }
   ```

2. Implement streaming snapshot reader
   ```rust
   pub struct FileSnapshotReader {
       file: tokio::fs::File,
       info: SnapshotInfo,
       bytes_read: u64,
   }

   impl FileSnapshotReader {
       pub async fn new(path: PathBuf, info: SnapshotInfo) -> Result<Self, Error> {
           let file = tokio::fs::File::open(path).await?;
           Ok(Self {
               file,
               info,
               bytes_read: 0,
           })
       }
   }

   #[async_trait]
   impl SnapshotReader for FileSnapshotReader {
       async fn read_chunk(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
           use tokio::io::AsyncReadExt;

           let n = self.file.read(buf).await?;
           self.bytes_read += n as u64;
           Ok(n)
       }

       fn info(&self) -> &SnapshotInfo {
           &self.info
       }

       fn bytes_read(&self) -> u64 {
           self.bytes_read
       }

       fn progress(&self) -> f64 {
           self.bytes_read as f64 / self.info.size_bytes as f64
       }
   }
   ```

3. Add snapshot transfer via gRPC
   ```rust
   // In storage_endpoint/snapshot_handler.rs
   #[async_trait]
   impl SnapshotService for SnapshotServiceHandler {
       async fn stream_snapshot(
           &self,
           request: Request<StreamSnapshotRequest>,
       ) -> Result<Response<tonic::Streaming<StreamSnapshotResponse>>, Status> {
           let req = request.into_inner();
           let snapshot_id = req.snapshot_id;

           // Open snapshot for streaming
           let mut reader = self.snapshot_store
               .open_snapshot(snapshot_id)
               .await
               .map_err(|e| Status::not_found(format!("Snapshot not found: {}", e)))?;

           let (tx, rx) = mpsc::channel(10);

           tokio::spawn(async move {
               let mut buf = vec![0u8; 1024 * 1024]; // 1MB chunks

               loop {
                   match reader.read_chunk(&mut buf).await {
                       Ok(0) => break, // EOF
                       Ok(n) => {
                           let response = StreamSnapshotResponse {
                               data: buf[..n].to_vec(),
                               offset: reader.bytes_read() - n as u64,
                               total_size: reader.info().size_bytes,
                           };

                           if tx.send(Ok(response)).await.is_err() {
                               break;
                           }
                       }
                       Err(e) => {
                           let _ = tx.send(Err(Status::internal(format!("Read error: {}", e)))).await;
                           break;
                       }
                   }
               }
           });

           Ok(Response::new(ReceiverStream::new(rx)))
       }
   }
   ```

**Deliverables**:
- Snapshot retrieval APIs
- Streaming snapshot reader
- gRPC snapshot transfer
- Progress tracking

#### Step 3: Snapshot Retention & Pruning
**File**: `src/snapshot_store/retention.rs`

**Tasks**:
1. Implement retention policy
   ```rust
   #[derive(Debug, Clone)]
   pub struct RetentionPolicy {
       /// Keep at least this many snapshots
       pub min_snapshots: usize,
       /// Maximum age of snapshots to keep
       pub max_age: Duration,
       /// Maximum total storage for snapshots
       pub max_total_size: u64,
   }

   impl SnapshotStoreImpl {
       pub async fn apply_retention_policy(&self) -> Result<u64, Error> {
           let snapshots = self.snapshots.read().await;
           let snapshot_list: Vec<_> = snapshots.values().cloned().collect();
           drop(snapshots);

           let mut to_delete = Vec::new();
           let now = SystemTime::now();

           // Keep minimum number of snapshots
           if snapshot_list.len() <= self.config.retention_policy.min_snapshots {
               return Ok(0);
           }

           // Find snapshots to delete based on age
           for info in &snapshot_list[..snapshot_list.len() - self.config.retention_policy.min_snapshots] {
               let age = now.duration_since(info.created_at)
                   .unwrap_or(Duration::from_secs(0));

               if age > self.config.retention_policy.max_age {
                   to_delete.push(info.snapshot_id);
               }
           }

           // Check total size constraint
           let total_size: u64 = snapshot_list.iter().map(|s| s.size_bytes).sum();
           if total_size > self.config.retention_policy.max_total_size {
               // Delete oldest until under limit
               let mut current_size = total_size;
               for info in &snapshot_list {
                   if current_size <= self.config.retention_policy.max_total_size {
                       break;
                   }
                   if !to_delete.contains(&info.snapshot_id) {
                       to_delete.push(info.snapshot_id);
                       current_size -= info.size_bytes;
                   }
               }
           }

           // Delete identified snapshots
           let mut deleted_count = 0;
           for snapshot_id in to_delete {
               self.delete_snapshot(snapshot_id).await?;
               deleted_count += 1;
           }

           Ok(deleted_count)
       }

       async fn delete_snapshot(&self, snapshot_id: u64) -> Result<(), Error> {
           let snapshot_path = self.snapshot_dir.join(format!("snapshot_{:08}.db", snapshot_id));
           let metadata_path = self.snapshot_dir.join(format!("snapshot_{:08}.json", snapshot_id));

           // Delete files
           if snapshot_path.exists() {
               fs::remove_file(snapshot_path).await?;
           }
           if metadata_path.exists() {
               fs::remove_file(metadata_path).await?;
           }

           // Remove from registry
           self.snapshots.write().await.remove(&snapshot_id);

           info!("Deleted snapshot {}", snapshot_id);

           Ok(())
       }
   }
   ```

2. Add background pruning task
   ```rust
   impl SnapshotStoreImpl {
       pub async fn start_pruning_task(&self) -> JoinHandle<()> {
           let store = self.clone();

           tokio::spawn(async move {
               let mut interval = tokio::time::interval(Duration::from_hours(1));

               loop {
                   interval.tick().await;

                   match store.apply_retention_policy().await {
                       Ok(count) if count > 0 => {
                           info!("Pruned {} old snapshots", count);
                       }
                       Err(e) => {
                           error!("Snapshot pruning failed: {}", e);
                       }
                       _ => {}
                   }
               }
           })
       }
   }
   ```

**Deliverables**:
- Retention policy implementation
- Automatic pruning
- Age and size-based cleanup
- Background pruning task

#### Step 4: Node Recovery from Snapshot
**File**: `src/storage_node/recovery.rs`

**Tasks**:
1. Implement snapshot-based recovery
   ```rust
   impl StorageNode {
       pub async fn recover_from_snapshot(&self) -> Result<(), Error> {
           info!("Starting recovery from snapshot");

           // Get latest snapshot from any node
           let snapshot_info = self.fetch_latest_snapshot_info().await?;

           if let Some(info) = snapshot_info {
               info!("Found snapshot {} at log index {}", info.snapshot_id, info.log_index);

               // Stream snapshot from source node
               let snapshot_data = self.stream_snapshot(info.snapshot_id).await?;

               // Restore metadata store
               self.metadata_store.restore_from_snapshot(&snapshot_data).await?;

               // Update Raft state
               self.raft_member.set_snapshot_index(info.log_index).await?;

               info!("Successfully restored from snapshot {}", info.snapshot_id);

               // Replay transaction log from snapshot point
               self.replay_transaction_log(info.log_index).await?;
           } else {
               info!("No snapshot available, replaying full transaction log");
               self.replay_transaction_log(0).await?;
           }

           Ok(())
       }

       async fn fetch_latest_snapshot_info(&self) -> Result<Option<SnapshotInfo>, Error> {
           let peers = self.storage_network.get_peers().await;

           for peer in peers {
               match self.query_snapshot_info(peer).await {
                   Ok(Some(info)) => return Ok(Some(info)),
                   Ok(None) => continue,
                   Err(e) => {
                       warn!("Failed to query snapshot from peer {}: {}", peer, e);
                       continue;
                   }
               }
           }

           Ok(None)
       }

       async fn stream_snapshot(&self, snapshot_id: u64) -> Result<Vec<u8>, Error> {
           let peers = self.storage_network.get_peers().await;

           for peer in peers {
               let mut client = self.get_snapshot_client(peer).await?;

               let request = StreamSnapshotRequest { snapshot_id };
               let mut stream = match client.stream_snapshot(request).await {
                   Ok(response) => response.into_inner(),
                   Err(e) => {
                       warn!("Failed to start snapshot stream from {}: {}", peer, e);
                       continue;
                   }
               };

               let mut data = Vec::new();

               while let Some(chunk) = stream.next().await {
                   match chunk {
                       Ok(response) => {
                           data.extend_from_slice(&response.data);
                       }
                       Err(e) => {
                           warn!("Snapshot stream error from {}: {}", peer, e);
                           break;
                       }
                   }
               }

               if !data.is_empty() {
                   return Ok(data);
               }
           }

           Err(Error::SnapshotUnavailable)
       }

       async fn replay_transaction_log(&self, from_index: u64) -> Result<(), Error> {
           info!("Replaying transaction log from index {}", from_index);

           let entries = self.transaction_log_store.read_range(from_index, u64::MAX).await?;

           for entry in entries {
               let operation: MetadataOperation = bincode::deserialize(&entry.data)?;
               self.apply_operation(operation).await?;
           }

           info!("Completed transaction log replay");

           Ok(())
       }
   }
   ```

**Deliverables**:
- Snapshot-based node recovery
- Streaming snapshot fetch
- Transaction log replay
- Automated recovery workflow

#### Step 5: Raft Snapshot Integration
**File**: `src/storage_raft_member/snapshot_integration.rs`

**Tasks**:
1. Trigger snapshot creation
   ```rust
   impl StorageRaftMemberImpl {
       pub async fn trigger_snapshot(&self) -> Result<(), Error> {
           info!("Triggering metadata snapshot");

           let metrics = self.raft.metrics();
           let snapshot_index = metrics.last_applied_log;
           let snapshot_term = metrics.current_term;

           // Create snapshot ID
           let snapshot_id = SystemTime::now()
               .duration_since(SystemTime::UNIX_EPOCH)?
               .as_secs();

           // Create temporary snapshot file
           let temp_path = self.metadata_store
               .create_snapshot_to_file()
               .await?;

           // Ingest into SnapshotStore
           self.snapshot_store
               .ingest_snapshot(snapshot_id, snapshot_index, snapshot_term, &temp_path)
               .await?;

           // Cleanup temp file
           tokio::fs::remove_file(temp_path).await?;

           // Trim transaction log
           self.transaction_log_store.trim(snapshot_index).await?;

           info!("Snapshot {} created at log index {}", snapshot_id, snapshot_index);

           Ok(())
       }

       pub async fn start_snapshot_task(&self) -> JoinHandle<()> {
           let member = self.clone();

           tokio::spawn(async move {
               let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Every hour

               loop {
                   interval.tick().await;

                   if member.is_leader().await {
                       match member.trigger_snapshot().await {
                           Ok(_) => info!("Periodic snapshot created"),
                           Err(e) => error!("Snapshot creation failed: {}", e),
                       }
                   }
               }
           })
       }
   }
   ```

2. Implement OpenRaft snapshot support
   ```rust
   #[async_trait]
   impl RaftStorage<TypeConfig> for RaftStorageAdapter {
       async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError> {
           info!("Building Raft snapshot");

           // Trigger snapshot creation
           let snapshot_id = self.raft_member.trigger_snapshot().await
               .map_err(|e| StorageError::IO(format!("Snapshot creation failed: {}", e)))?;

           // Get snapshot info
           let snapshot_info = self.snapshot_store.get_snapshot(snapshot_id).await
               .map_err(|e| StorageError::IO(format!("Snapshot retrieval failed: {}", e)))?
               .ok_or_else(|| StorageError::IO("Snapshot not found".to_string()))?;

           // Create snapshot metadata
           let meta = SnapshotMeta {
               last_log_id: Some(LogId::new(
                   LeaderId::new(snapshot_info.log_term, NodeId::default()),
                   snapshot_info.log_index,
               )),
               last_membership: self.last_membership.clone(),
               snapshot_id: snapshot_id.to_string(),
           };

           // Open snapshot for reading
           let reader = self.snapshot_store.open_snapshot(snapshot_id).await
               .map_err(|e| StorageError::IO(format!("Failed to open snapshot: {}", e)))?;

           Ok(Snapshot {
               meta,
               snapshot: Box::new(reader),
           })
       }

       async fn begin_receiving_snapshot(&mut self) -> Result<Box<dyn SnapshotWriter>, StorageError> {
           info!("Beginning to receive snapshot");

           let temp_path = self.temp_dir.join(format!("snapshot_{}.tmp", uuid::Uuid::new_v4()));

           let writer = SnapshotFileWriter::new(temp_path).await
               .map_err(|e| StorageError::IO(format!("Failed to create snapshot writer: {}", e)))?;

           Ok(Box::new(writer))
       }

       async fn install_snapshot(
           &mut self,
           meta: &SnapshotMeta<TypeConfig>,
           snapshot: Box<dyn AsyncRead + AsyncSeek + Send + Unpin>,
       ) -> Result<(), StorageError> {
           info!("Installing snapshot");

           // Write snapshot to temp file
           let temp_path = self.temp_dir.join(format!("snapshot_{}.tmp", meta.snapshot_id));
           let mut file = tokio::fs::File::create(&temp_path).await?;
           let mut snapshot = snapshot;

           tokio::io::copy(&mut snapshot, &mut file).await?;
           file.sync_all().await?;

           // Restore metadata store
           self.metadata_store.restore_from_snapshot(&temp_path).await
               .map_err(|e| StorageError::IO(format!("Failed to restore metadata: {}", e)))?;

           // Ingest into snapshot store
           let snapshot_id = meta.snapshot_id.parse::<u64>()
               .map_err(|e| StorageError::IO(format!("Invalid snapshot ID: {}", e)))?;

           let log_id = meta.last_log_id.as_ref()
               .ok_or_else(|| StorageError::IO("Missing log ID".to_string()))?;

           self.snapshot_store.ingest_snapshot(
               snapshot_id,
               log_id.index,
               log_id.leader_id.term,
               &temp_path,
           ).await
               .map_err(|e| StorageError::IO(format!("Failed to ingest snapshot: {}", e)))?;

           // Cleanup temp file
           tokio::fs::remove_file(temp_path).await?;

           info!("Snapshot installed successfully");

           Ok(())
       }
   }
   ```

**Deliverables**:
- Raft snapshot creation
- Snapshot installation
- Log compaction integration
- Periodic snapshot triggers

---

### Week 2: StorageWatchdog & Integrity Monitoring (Days 6-10)

#### Step 6: Shallow Chunk Verification
**File**: `src/storage_watchdog/shallow_check.rs`

**Tasks**:
1. Implement shallow check task
   ```rust
   pub struct StorageWatchdogImpl {
       config: Config,
       metadata_store: Arc<dyn MetadataStore>,
       file_store: Arc<dyn FileStore>,
       storage_network: Arc<StorageNetworkHandle>,
       raft_member: Arc<dyn StorageRaftMember>,
       repair_queue: Arc<Mutex<PriorityQueue<RepairRequest, Priority>>>,
       state: Arc<RwLock<WatchdogState>>,
       shutdown_tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
   }

   impl StorageWatchdogImpl {
       async fn shallow_check_task(&self) {
           info!("Starting shallow check task");

           let mut interval = tokio::time::interval(self.config.shallow_check_interval);

           loop {
               interval.tick().await;

               if !self.is_running().await {
                   break;
               }

               match self.run_shallow_check().await {
                   Ok(events) => {
                       info!("Shallow check completed, found {} issues", events.len());
                       for event in events {
                           self.handle_consistency_event(event).await;
                       }
                   }
                   Err(e) => {
                       error!("Shallow check failed: {}", e);
                   }
               }
           }
       }

       async fn run_shallow_check(&self) -> Result<Vec<ConsistencyEvent>, Error> {
           let mut events = Vec::new();
           let mut state = self.state.write().await;

           // Get all files from metadata store
           let files = self.metadata_store.list_all_files().await?;

           let start_position = state.shallow_check_position.unwrap_or(FileId::new(0));
           let mut checked_count = 0;
           const BATCH_SIZE: usize = 100;

           for file in files.iter().skip_while(|f| f.file_id < start_position).take(BATCH_SIZE) {
               let file_events = self.check_file_shallow(file.file_id).await?;
               events.extend(file_events);
               checked_count += 1;

               state.shallow_check_position = Some(file.file_id);
           }

           // Reset position if we reached the end
           if checked_count < BATCH_SIZE {
               state.shallow_check_position = None;
               state.last_shallow_check = Some(SystemTime::now());
           }

           Ok(events)
       }

       async fn check_file_shallow(&self, file_id: FileId) -> Result<Vec<ConsistencyEvent>, Error> {
           let mut events = Vec::new();

           // Get file metadata
           let file_record = self.metadata_store.get_file(file_id).await?;

           // Get all stripes for file
           let stripes = self.metadata_store.get_file_stripes(file_id).await?;

           for stripe in stripes {
               // Get chunks for stripe
               let chunks = self.metadata_store.get_stripe_chunks(stripe.stripe_id).await?;

               for chunk in chunks {
                   // Check if chunk exists on target node
                   let exists = if chunk.node_id == self.get_my_node_id() {
                       // Local check
                       self.file_store.check_chunk(chunk.chunk_id).await.is_ok()
                   } else {
                       // Remote check
                       self.check_chunk_remote(chunk.node_id, chunk.chunk_id).await.is_ok()
                   };

                   if !exists {
                       events.push(ConsistencyEvent::ChunkMissing {
                           file_id,
                           stripe_id: stripe.stripe_id,
                           chunk_id: chunk.chunk_id,
                           node_id: chunk.node_id,
                       });
                   }
               }
           }

           Ok(events)
       }

       async fn check_chunk_remote(&self, node_id: NodeId, chunk_id: ChunkId) -> Result<(), Error> {
           let mut client = self.get_node_client(node_id).await?;

           let response = client.verify_chunk(VerifyChunkRequest {
               chunk_id: chunk_id.as_u64(),
               deep_check: false, // Shallow only
           }).await?;

           if response.into_inner().exists {
               Ok(())
           } else {
               Err(Error::ChunkNotFound(chunk_id))
           }
       }
   }
   ```

**Deliverables**:
- Shallow check loop implementation
- Chunk existence verification
- Remote chunk checking
- Progress tracking

#### Step 7: Deep Chunk Verification
**File**: `src/storage_watchdog/deep_check.rs`

**Tasks**:
1. Implement deep verification
   ```rust
   impl StorageWatchdogImpl {
       async fn deep_check_task(&self) {
           info!("Starting deep check task");

           let mut interval = tokio::time::interval(self.config.deep_check_interval);

           loop {
               interval.tick().await;

               if !self.is_running().await {
                   break;
               }

               match self.run_deep_check().await {
                   Ok(events) => {
                       info!("Deep check completed, found {} issues", events.len());
                       for event in events {
                           self.handle_consistency_event(event).await;
                       }
                   }
                   Err(e) => {
                       error!("Deep check failed: {}", e);
                   }
               }
           }
       }

       async fn run_deep_check(&self) -> Result<Vec<ConsistencyEvent>, Error> {
           let mut events = Vec::new();
           let mut state = self.state.write().await;

           let files = self.metadata_store.list_all_files().await?;
           let start_position = state.deep_check_position.unwrap_or(FileId::new(0));
           let mut checked_count = 0;
           const BATCH_SIZE: usize = 10; // Smaller batch for deep checks

           for file in files.iter().skip_while(|f| f.file_id < start_position).take(BATCH_SIZE) {
               let file_events = self.check_file_deep(file.file_id).await?;
               events.extend(file_events);
               checked_count += 1;

               state.deep_check_position = Some(file.file_id);
           }

           if checked_count < BATCH_SIZE {
               state.deep_check_position = None;
               state.last_deep_check = Some(SystemTime::now());
           }

           Ok(events)
       }

       async fn check_file_deep(&self, file_id: FileId) -> Result<Vec<ConsistencyEvent>, Error> {
           let mut events = Vec::new();
           let stripes = self.metadata_store.get_file_stripes(file_id).await?;

           for stripe in stripes {
               // Check stripe integrity
               match self.verify_stripe(file_id, stripe.stripe_id).await {
                   Ok(()) => {
                       // Stripe is valid
                   }
                   Err(VerificationError::ChunkCorrupt { chunk_id, node_id, reason }) => {
                       events.push(ConsistencyEvent::ChunkCorrupt {
                           file_id,
                           stripe_id: stripe.stripe_id,
                           chunk_id,
                           node_id,
                           reason,
                       });
                   }
                   Err(VerificationError::StripeCorrupt { reason }) => {
                       events.push(ConsistencyEvent::StripeCorrupt {
                           file_id,
                           stripe_id: stripe.stripe_id,
                           reason,
                       });
                   }
                   Err(e) => {
                       error!("Stripe verification failed: {}", e);
                   }
               }
           }

           Ok(events)
       }

       async fn verify_stripe(&self, file_id: FileId, stripe_id: StripeId) -> Result<(), VerificationError> {
           let chunks = self.metadata_store.get_stripe_chunks(stripe_id).await?;

           // Fetch all chunks
           let mut chunk_data = Vec::new();
           for chunk_meta in &chunks {
               let data = if chunk_meta.node_id == self.get_my_node_id() {
                   self.file_store.read_chunk_local(chunk_meta.chunk_id).await?
               } else {
                   self.read_chunk_remote(chunk_meta.node_id, chunk_meta.chunk_id).await?
               };

               // Verify individual chunk checksum
               let computed = ChunkHeader::compute_checksum(&data.data);
               if computed != data.header.chunk_checksum {
                   return Err(VerificationError::ChunkCorrupt {
                       chunk_id: chunk_meta.chunk_id,
                       node_id: chunk_meta.node_id,
                       reason: format!("Checksum mismatch: expected {}, got {}",
                           data.header.chunk_checksum, computed),
                   });
               }

               chunk_data.push((chunk_meta.chunk_index, data));
           }

           // Reconstruct stripe using erasure coding
           let policy = StoragePolicy {
               data_shards: chunks[0].data_shards,
               parity_shards: chunks[0].parity_shards,
               stripe_size: 0, // Not needed for decoding
               compression: CompressionAlgorithm::None,
           };

           let reconstructed = self.file_store.decode_stripe(chunk_data, &policy).await?;

           // Verify stripe checksum
           let stripe_record = self.metadata_store.get_stripe(stripe_id).await?;
           let computed = crc32fast::hash(&reconstructed);

           if computed != stripe_record.checksum {
               return Err(VerificationError::StripeCorrupt {
                   reason: format!("Stripe checksum mismatch: expected {}, got {}",
                       stripe_record.checksum, computed),
               });
           }

           Ok(())
       }
   }
   ```

**Deliverables**:
- Deep integrity verification
- Chunk checksum validation
- Stripe reconstruction
- Full integrity checks

#### Step 8: Repair Coordination
**File**: `src/storage_watchdog/repair.rs`

**Tasks**:
1. Implement repair queue
   ```rust
   impl StorageWatchdogImpl {
       async fn handle_consistency_event(&self, event: ConsistencyEvent) {
           let repair_request = match event {
               ConsistencyEvent::ChunkMissing { file_id, stripe_id, .. } => {
                   RepairRequest {
                       file_id,
                       stripe_id,
                       priority: RepairPriority::High,
                       created_at: SystemTime::now(),
                       retry_count: 0,
                   }
               }
               ConsistencyEvent::ChunkCorrupt { file_id, stripe_id, .. } => {
                   RepairRequest {
                       file_id,
                       stripe_id,
                       priority: RepairPriority::Medium,
                       created_at: SystemTime::now(),
                       retry_count: 0,
                   }
               }
               ConsistencyEvent::StripeCorrupt { file_id, stripe_id, .. } => {
                   RepairRequest {
                       file_id,
                       stripe_id,
                       priority: RepairPriority::Critical,
                       created_at: SystemTime::now(),
                       retry_count: 0,
                   }
               }
               _ => return,
           };

           // Add to repair queue
           self.repair_queue.lock().await.push(repair_request, repair_request.priority);
       }

       async fn repair_task(&self) {
           info!("Starting repair task");

           loop {
               if !self.is_running().await {
                   break;
               }

               // Get next repair request
               let request = {
                   let mut queue = self.repair_queue.lock().await;
                   queue.pop().map(|(req, _)| req)
               };

               if let Some(request) = request {
                   match self.process_repair(request.clone()).await {
                       Ok(()) => {
                           info!("Repair completed for stripe {}", request.stripe_id);
                           let mut state = self.state.write().await;
                           state.active_repairs.remove(&request.stripe_id);
                       }
                       Err(e) => {
                           error!("Repair failed for stripe {}: {}", request.stripe_id, e);

                           // Retry if below max retries
                           if request.retry_count < self.config.max_repair_retries {
                               let retry_request = RepairRequest {
                                   retry_count: request.retry_count + 1,
                                   ..request
                               };

                               tokio::time::sleep(self.config.repair_retry_delay).await;
                               self.repair_queue.lock().await.push(retry_request.clone(), retry_request.priority);
                           }
                       }
                   }
               } else {
                   // No work, sleep
                   tokio::time::sleep(Duration::from_secs(1)).await;
               }
           }
       }

       async fn process_repair(&self, request: RepairRequest) -> Result<(), Error> {
           info!("Processing repair for file {} stripe {}", request.file_id, request.stripe_id);

           // Track active repair
           {
               let mut state = self.state.write().await;
               state.active_repairs.insert(request.stripe_id, RepairStatus::InProgress);
           }

           // Get stripe metadata
           let stripe = self.metadata_store.get_stripe(request.stripe_id).await?;
           let chunks = self.metadata_store.get_stripe_chunks(request.stripe_id).await?;

           // Identify missing/corrupt chunks
           let mut available_chunks = Vec::new();
           let mut missing_chunks = Vec::new();

           for chunk_meta in &chunks {
               let result = if chunk_meta.node_id == self.get_my_node_id() {
                   self.file_store.verify_chunk(chunk_meta.chunk_id).await
               } else {
                   self.verify_chunk_remote(chunk_meta.node_id, chunk_meta.chunk_id).await
               };

               match result {
                   Ok(verification) if verification.checksum_valid => {
                       available_chunks.push(chunk_meta.clone());
                   }
                   _ => {
                       missing_chunks.push(chunk_meta.clone());
                   }
               }
           }

           // Check if we have enough chunks for reconstruction
           let policy = StoragePolicy {
               data_shards: chunks[0].data_shards,
               parity_shards: chunks[0].parity_shards,
               stripe_size: stripe.size,
               compression: CompressionAlgorithm::None,
           };

           if available_chunks.len() < policy.data_shards as usize {
               return Err(Error::InsufficientChunks {
                   needed: policy.data_shards as usize,
                   available: available_chunks.len(),
               });
           }

           // Reconstruct stripe
           let stripe_data = self.file_store.read_stripe_distributed(
               request.file_id,
               request.stripe_id,
           ).await?;

           // Re-encode and write missing chunks
           let chunks = self.file_store.encode_stripe(stripe_data, &policy).await?;

           for chunk_meta in missing_chunks {
               let chunk_data = &chunks[chunk_meta.chunk_index as usize];

               // Write chunk to target node
               if chunk_meta.node_id == self.get_my_node_id() {
                   self.file_store.write_chunk_local(chunk_meta.chunk_id, chunk_data.clone()).await?;
               } else {
                   self.write_chunk_remote(chunk_meta.node_id, chunk_meta.chunk_id, chunk_data.clone()).await?;
               }

               info!("Repaired chunk {} on node {}", chunk_meta.chunk_id, chunk_meta.node_id);
           }

           Ok(())
       }
   }
   ```

**Deliverables**:
- Repair queue with prioritization
- Chunk reconstruction and repair
- Remote chunk repair
- Retry logic

#### Step 9: Orphan Cleanup
**File**: `src/storage_watchdog/orphan_cleanup.rs`

**Tasks**:
1. Implement orphan detection and cleanup
   ```rust
   impl StorageWatchdogImpl {
       pub async fn cleanup_orphans(&self) -> Result<u64, Error> {
           info!("Starting orphan cleanup");

           let mut cleaned_count = 0;

           // Find chunks on local disk
           let local_chunks = self.scan_local_chunks().await?;

           // Find chunks in metadata
           let metadata_chunks = self.get_all_metadata_chunks().await?;

           let metadata_chunk_ids: HashSet<_> = metadata_chunks.into_iter().collect();

           // Identify orphans (on disk but not in metadata)
           let orphans: Vec<_> = local_chunks.into_iter()
               .filter(|chunk_id| !metadata_chunk_ids.contains(chunk_id))
               .collect();

           info!("Found {} orphan chunks", orphans.len());

           // Delete orphans older than threshold
           for chunk_id in orphans {
               if self.is_chunk_old_enough(chunk_id).await? {
                   self.file_store.delete_chunk_local(chunk_id).await?;
                   cleaned_count += 1;
               }
           }

           // Clean staged chunks older than 1 hour
           let staged_cleaned = self.file_store
               .cleanup_orphaned_chunks(
                   SystemTime::now() - Duration::from_secs(3600)
               )
               .await?;

           cleaned_count += staged_cleaned;

           info!("Cleaned {} orphan chunks", cleaned_count);

           Ok(cleaned_count)
       }

       async fn scan_local_chunks(&self) -> Result<Vec<ChunkId>, Error> {
           let mut chunk_ids = Vec::new();
           let disks = self.file_store.get_disk_stats();

           for disk in disks {
               // Walk disk directory
               let mut entries = tokio::fs::read_dir(&disk.path).await?;

               while let Some(entry) = entries.next_entry().await? {
                   if let Some(chunk_id) = self.extract_chunk_id_from_path(&entry.path()) {
                       chunk_ids.push(chunk_id);
                   }
               }
           }

           Ok(chunk_ids)
       }

       async fn get_all_metadata_chunks(&self) -> Result<Vec<ChunkId>, Error> {
           let mut chunk_ids = Vec::new();
           let files = self.metadata_store.list_all_files().await?;

           for file in files {
               let stripes = self.metadata_store.get_file_stripes(file.file_id).await?;
               for stripe in stripes {
                   let chunks = self.metadata_store.get_stripe_chunks(stripe.stripe_id).await?;
                   chunk_ids.extend(chunks.into_iter().map(|c| c.chunk_id));
               }
           }

           Ok(chunk_ids)
       }

       async fn is_chunk_old_enough(&self, chunk_id: ChunkId) -> Result<bool, Error> {
           // Check chunk age - only delete if older than 1 hour
           let chunk_path = self.file_store.get_chunk_path(chunk_id)?;
           let metadata = tokio::fs::metadata(&chunk_path).await?;
           let modified = metadata.modified()?;
           let age = SystemTime::now().duration_since(modified)?;

           Ok(age > Duration::from_secs(3600))
       }

       pub async fn start_orphan_cleanup_task(&self) -> JoinHandle<()> {
           let watchdog = self.clone();

           tokio::spawn(async move {
               let mut interval = tokio::time::interval(Duration::from_secs(86400)); // Daily

               loop {
                   interval.tick().await;

                   if watchdog.is_running().await {
                       match watchdog.cleanup_orphans().await {
                           Ok(count) => info!("Orphan cleanup removed {} chunks", count),
                           Err(e) => error!("Orphan cleanup failed: {}", e),
                       }
                   }
               }
           })
       }
   }
   ```

**Deliverables**:
- Orphan chunk detection
- Safe cleanup with age threshold
- Scheduled cleanup task
- Staged chunk cleanup

#### Step 10: Integration Testing & Documentation
**File**: `tests/integration/phase4_robustness_test.rs`

**Tasks**:
1. Test snapshot-based recovery
   ```rust
   #[tokio::test]
   async fn test_node_recovery_from_snapshot() {
       let cluster = TestCluster::new(3).await;

       // Create data
       let client = WormFsClient::connect(cluster.nodes()).await.unwrap();
       for i in 0..100 {
           let path = format!("/test_{}.dat", i);
           let file = client.create_file(&path, 0o644).await.unwrap();
           let data = vec![i as u8; 1024 * 1024];
           client.write_file(file.inode, 0, &data).await.unwrap();
       }

       // Trigger snapshot on leader
       cluster.leader().trigger_snapshot().await.unwrap();

       // Stop a follower node
       let follower_id = cluster.followers()[0];
       cluster.stop_node(follower_id).await;

       // Create more data
       for i in 100..200 {
           let path = format!("/test_{}.dat", i);
           let file = client.create_file(&path, 0o644).await.unwrap();
           let data = vec![i as u8; 1024 * 1024];
           client.write_file(file.inode, 0, &data).await.unwrap();
       }

       // Restart follower - should recover from snapshot
       cluster.start_node(follower_id).await;

       // Wait for recovery
       tokio::time::sleep(Duration::from_secs(10)).await;

       // Verify data consistency
       let follower = cluster.get_node(follower_id);
       for i in 0..200 {
           let path = format!("/test_{}.dat", i);
           let file = follower.metadata_store.get_file_by_path(&path).await;
           assert!(file.is_ok(), "File {} should exist", i);
       }
   }
   ```

2. Test watchdog repair
   ```rust
   #[tokio::test]
   async fn test_watchdog_chunk_repair() {
       let cluster = TestCluster::new(5).await;

       // Write file with 3+2 erasure coding
       let client = WormFsClient::connect(cluster.nodes()).await.unwrap();
       let file = client.create_file("/test.dat", 0o644).await.unwrap();
       let data = vec![0xAB; 10 * 1024 * 1024];
       client.write_file(file.inode, 0, &data).await.unwrap();

       // Get stripe metadata
       let metadata = cluster.leader().metadata_store
           .get_file(file.file_id).await.unwrap();
       let stripe = metadata.stripes[0].clone();

       // Corrupt a chunk
       let chunk_to_corrupt = &stripe.chunks[0];
       cluster.get_node(chunk_to_corrupt.node_id)
           .file_store
           .corrupt_chunk(chunk_to_corrupt.chunk_id)
           .await
           .unwrap();

       // Trigger deep check on leader's watchdog
       cluster.leader().watchdog.trigger_deep_check().await.unwrap();

       // Wait for repair
       tokio::time::sleep(Duration::from_secs(30)).await;

       // Verify chunk is repaired
       let verification = cluster.get_node(chunk_to_corrupt.node_id)
           .file_store
           .verify_chunk(chunk_to_corrupt.chunk_id)
           .await
           .unwrap();

       assert!(verification.checksum_valid);

       // Read file should still work
       let read_data = client.read_file(file.inode, 0, data.len()).await.unwrap();
       assert_eq!(read_data, data);
   }
   ```

3. Test orphan cleanup
   ```rust
   #[tokio::test]
   async fn test_orphan_chunk_cleanup() {
       let cluster = TestCluster::new(3).await;
       let node = cluster.get_node(cluster.leader_id());

       // Create some orphan chunks manually
       for i in 0..10 {
           let chunk_id = ChunkId::new(i);
           let chunk_data = ChunkData {
               header: create_test_chunk_header(chunk_id),
               data: vec![0u8; 1024],
           };
           node.file_store.write_chunk_local(chunk_id, chunk_data).await.unwrap();
       }

       // Wait for chunks to age
       tokio::time::sleep(Duration::from_secs(3700)).await;

       // Run orphan cleanup
       let cleaned = node.watchdog.cleanup_orphans().await.unwrap();
       assert_eq!(cleaned, 10);

       // Verify chunks are deleted
       for i in 0..10 {
           let chunk_id = ChunkId::new(i);
           let result = node.file_store.read_chunk_local(chunk_id).await;
           assert!(result.is_err());
       }
   }
   ```

4. Create documentation
   - Recovery procedures guide
   - Watchdog configuration tuning
   - Troubleshooting integrity issues
   - Snapshot management best practices

**Deliverables**:
- Snapshot recovery tests passing
- Watchdog repair verified
- Orphan cleanup validated
- Complete documentation
- Phase 4 milestone achieved

---

## Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Recovery Time | <5 min | Time to recover node from snapshot |
| Snapshot Size | <50% metadata | Compressed snapshot vs raw metadata size |
| Shallow Check Coverage | 100%/day | Percentage of chunks checked daily |
| Deep Check Coverage | 100%/week | Percentage of stripes verified weekly |
| Repair Success Rate | >99% | Successful repairs / total repairs |
| Orphan Cleanup | 100% | Orphans removed within 24 hours |

## Risk Mitigation

### Technical Risks:
1. **Snapshot Corruption**: Add checksum verification, keep multiple snapshots
2. **Repair Conflicts**: Use Raft coordination for repair operations
3. **Watchdog Overhead**: Add rate limiting, prioritization
4. **False Positives**: Add retry logic for transient failures

### Fallback Options:
- If snapshot is slow: Use incremental snapshots
- If watchdog impacts performance: Reduce check frequency
- If repair fails: Manual intervention with tools

## Dependencies

### External Crates:
- `zstd` - Snapshot compression
- `priority-queue` - Repair queue (already in deps)
- `ext-sort` - Large dataset sorting for watchdog (already in deps)

### Internal Dependencies:
- **MetadataStore**: Snapshot creation, consistency queries
- **FileStore**: Chunk verification, repair operations
- **StorageRaftMember**: Snapshot coordination, leadership detection
- **StorageNetwork**: Inter-node chunk verification
- **StorageEndpoint**: Snapshot transfer APIs

## Integration Points

### Phase 1-3 Components:
- **MetadataStore**: Create snapshots, restore from snapshots
- **FileStore**: Chunk verification and repair
- **StorageRaftMember**: Trigger snapshots, coordinate repairs
- **StorageNetwork**: Remote chunk operations

### New in Phase 4:
- **SnapshotStore**: Snapshot lifecycle management
- **StorageWatchdog**: Continuous integrity monitoring
- **Recovery workflows**: Automated node recovery

## Next Steps After Phase 4

Once Phase 4 is complete and tested:
1. Run long-term stability testing
2. Test with real failure scenarios
3. Prepare for Phase 5 (observability and production readiness)
4. Performance optimization based on monitoring

## Notes

- Watchdog runs only on Raft leader
- Snapshots enable fast node recovery
- Orphan cleanup prevents disk space leaks
- Repair is automatic but observable
- Focus on preventing data loss above all else
