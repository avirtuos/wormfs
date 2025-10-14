# Phase 2: Add Consensus Layer - Detailed Implementation Plan

## Overview
**Duration**: 3 weeks (15 working days)
**Goal**: Add Raft consensus to coordinate metadata operations across multiple nodes
**Success Criteria**: 3-node cluster maintains consistent metadata through leader election and log replication
**Prerequisites**: Phase 1 complete (single-node filesystem working)

## Component Implementation Order

### Week 1: StorageNetwork & TransactionLogStore (Days 1-5)

#### Step 1: libp2p Basic Setup
**File**: `src/storage_network/implementation.rs`

**Tasks**:
1. Set up basic libp2p swarm with TCP transport
   ```rust
   use libp2p::{
       identity, noise, tcp, yamux, PeerId, Swarm,
       swarm::{NetworkBehaviour, SwarmBuilder},
   };

   pub struct WormFsBehaviour {
       gossipsub: gossipsub::Behaviour,
       request_response: request_response::Behaviour<RaftCodec>,
       identify: identify::Behaviour,
       ping: ping::Behaviour,
   }
   ```

2. Implement peer discovery and connection management
   ```rust
   impl StorageNetwork {
       async fn connect_to_peer(&self, peer_addr: Multiaddr) -> Result<PeerId, Error> {
           // Parse peer ID from address
           // Dial peer
           // Verify connection
       }

       async fn discover_local_peers(&self) -> Result<Vec<PeerId>, Error> {
           // Use mDNS for local network discovery
       }
   }
   ```

3. Create basic event loop for swarm
   ```rust
   async fn run_event_loop(mut swarm: Swarm<WormFsBehaviour>) {
       loop {
           select! {
               event = swarm.next() => {
                   // Handle swarm events
               }
               cmd = cmd_rx.recv() => {
                   // Handle network commands
               }
           }
       }
   }
   ```

**Deliverables**:
- Basic peer-to-peer connectivity working
- Can discover and connect to peers on local network
- Event loop running in background task

#### Step 2: Topic-Based Communication
**File**: `src/storage_network/topics.rs`

**Tasks**:
1. Implement topic subscription system
   ```rust
   impl StorageNetwork {
       pub async fn join_topic(&self, topic: &str) -> Result<(TopicSender, TopicReceiver), Error> {
           // Subscribe to gossipsub topic
           // Create channels for topic messages
           // Return sender/receiver pair
       }

       pub async fn publish(&self, topic: &str, message: Vec<u8>) -> Result<(), Error> {
           // Publish message to gossipsub topic
       }
   }
   ```

2. Add request-response protocol for direct messaging
   ```rust
   // For Raft AppendEntries and RequestVote RPCs
   impl StorageNetwork {
       pub async fn send_request(&self, peer: PeerId, request: RaftMessage)
           -> Result<RaftResponse, Error> {
           // Send request and await response
       }
   }
   ```

3. Implement peer state tracking
   ```rust
   struct PeerState {
       peer_id: PeerId,
       addresses: Vec<Multiaddr>,
       connection_state: ConnectionState,
       last_seen: Instant,
       raft_role: Option<RaftRole>,
   }
   ```

**Deliverables**:
- Topic-based pub/sub working
- Direct request-response messaging
- Peer state tracking

#### Step 3: TransactionLogStore with redb
**File**: `src/transaction_log_store/implementation.rs`

**Tasks**:
1. Set up redb database schema
   ```rust
   use redb::{Database, TableDefinition};

   const LOG_ENTRIES: TableDefinition<u64, Vec<u8>> =
       TableDefinition::new("log_entries");
   const LOG_METADATA: TableDefinition<&str, Vec<u8>> =
       TableDefinition::new("log_metadata");

   pub struct TransactionLogStoreImpl {
       db: Arc<Database>,
       next_index: AtomicU64,
   }
   ```

2. Implement core log operations
   ```rust
   impl TransactionLogStore for TransactionLogStoreImpl {
       async fn append(&self, term: u64, data: Vec<u8>) -> Result<u64, LogError> {
           let index = self.next_index.fetch_add(1, Ordering::SeqCst);
           let entry = LogEntry { index, term, data, checksum: crc32(&data) };

           let write_txn = self.db.begin_write()?;
           {
               let mut table = write_txn.open_table(LOG_ENTRIES)?;
               table.insert(&index, &bincode::serialize(&entry)?)?;
           }
           write_txn.commit()?;
           Ok(index)
       }

       async fn read(&self, index: u64) -> Result<LogEntry, LogError> {
           let read_txn = self.db.begin_read()?;
           let table = read_txn.open_table(LOG_ENTRIES)?;
           let data = table.get(&index)?
               .ok_or(LogError::NotFound)?;
           Ok(bincode::deserialize(&data.value())?)
       }
   }
   ```

3. Add range queries and trimming
   ```rust
   async fn read_range(&self, start: u64, end: u64) -> Result<Vec<LogEntry>, LogError> {
       // Read entries in range [start, end)
   }

   async fn trim(&self, up_to_index: u64) -> Result<(), LogError> {
       // Remove entries with index < up_to_index
   }
   ```

**Deliverables**:
- Durable log storage with redb
- Append, read, range query operations
- Log trimming support

#### Step 4: Network Integration for Raft
**File**: `src/storage_network/raft_transport.rs`

**Tasks**:
1. Create Raft network adapter
   ```rust
   pub struct RaftNetworkAdapter {
       network: StorageNetworkHandle,
       node_id: NodeId,
   }

   impl RaftNetwork for RaftNetworkAdapter {
       async fn send_append_entries(&self, target: NodeId, req: AppendEntriesRequest)
           -> Result<AppendEntriesResponse, Error> {
           let peer_id = self.node_to_peer(target)?;
           self.network.send_request(peer_id, RaftMessage::AppendEntries(req)).await
       }

       async fn send_request_vote(&self, target: NodeId, req: RequestVoteRequest)
           -> Result<RequestVoteResponse, Error> {
           // Similar implementation
       }
   }
   ```

2. Handle incoming Raft RPCs
   ```rust
   async fn handle_raft_request(request: RaftMessage, raft: Arc<RaftNode>)
       -> Result<RaftResponse, Error> {
       match request {
           RaftMessage::AppendEntries(req) => {
               let resp = raft.append_entries(req).await?;
               Ok(RaftResponse::AppendEntries(resp))
           }
           RaftMessage::RequestVote(req) => {
               let resp = raft.vote(req).await?;
               Ok(RaftResponse::RequestVote(resp))
           }
       }
   }
   ```

**Deliverables**:
- Raft network adapter implemented
- RPC handling for AppendEntries and RequestVote
- Integration between StorageNetwork and Raft

#### Step 5: Testing Network & Log
**File**: `tests/integration/phase2_network_test.rs`

**Tasks**:
1. Test 3-node network formation
   ```rust
   #[tokio::test]
   async fn test_three_node_network() {
       let node1 = create_test_node(1).await;
       let node2 = create_test_node(2).await;
       let node3 = create_test_node(3).await;

       // Connect nodes
       node2.connect_to_peer(node1.address()).await.unwrap();
       node3.connect_to_peer(node1.address()).await.unwrap();

       // Verify all nodes see each other
       assert_eq!(node1.peer_count(), 2);
   }
   ```

2. Test topic messaging
   ```rust
   #[tokio::test]
   async fn test_topic_pubsub() {
       // Set up 3-node network
       // Subscribe all to "test" topic
       // Publish from node1
       // Verify node2 and node3 receive
   }
   ```

3. Test log operations
   ```rust
   #[tokio::test]
   async fn test_log_durability() {
       let log = TransactionLogStoreImpl::new(test_config()).unwrap();

       // Append entries
       let idx1 = log.append(1, b"entry1".to_vec()).await.unwrap();
       let idx2 = log.append(1, b"entry2".to_vec()).await.unwrap();

       // Read back
       let entry1 = log.read(idx1).await.unwrap();
       assert_eq!(entry1.data, b"entry1");

       // Test persistence (close and reopen)
       drop(log);
       let log = TransactionLogStoreImpl::new(test_config()).unwrap();
       let entry1 = log.read(idx1).await.unwrap();
       assert_eq!(entry1.data, b"entry1");
   }
   ```

**Deliverables**:
- Network connectivity tests passing
- Topic messaging verified
- Log durability confirmed

---

### Week 2: StorageRaftMember & Integration (Days 6-10)

#### Step 6: OpenRaft Integration
**File**: `src/storage_raft_member/implementation.rs`

**Tasks**:
1. Set up OpenRaft node
   ```rust
   use openraft::{Config, Raft, RaftStorage};

   pub struct StorageRaftMemberImpl {
       raft: Raft<TypeConfig>,
       metadata_store: Arc<MetadataStoreImpl>,
       log_store: Arc<TransactionLogStoreImpl>,
       network: Arc<RaftNetworkAdapter>,
   }

   impl StorageRaftMemberImpl {
       pub async fn new(config: Config) -> Result<Self, Error> {
           let raft_config = Arc::new(openraft::Config {
               election_timeout_min: 150,
               election_timeout_max: 300,
               heartbeat_interval: 50,
               ..Default::default()
           });

           let storage = Arc::new(RaftStorageAdapter::new(
               metadata_store.clone(),
               log_store.clone(),
           ));

           let raft = Raft::new(
               config.node_id,
               raft_config,
               network.clone(),
               storage,
           ).await?;

           Ok(Self { raft, metadata_store, log_store, network })
       }
   }
   ```

2. Implement RaftStorage adapter
   ```rust
   struct RaftStorageAdapter {
       metadata_store: Arc<MetadataStoreImpl>,
       log_store: Arc<TransactionLogStoreImpl>,
       state_machine: Arc<RwLock<StateMachine>>,
   }

   #[async_trait]
   impl RaftStorage<TypeConfig> for RaftStorageAdapter {
       async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError> {
           // Save vote to persistent storage
       }

       async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError> {
           // Read saved vote
       }

       async fn append_to_log(&mut self, entries: &[&Entry<TypeConfig>]) -> Result<(), StorageError> {
           for entry in entries {
               self.log_store.append(entry.log_id.term, entry.payload.clone()).await?;
           }
           Ok(())
       }
   }
   ```

3. Create state machine for metadata operations
   ```rust
   struct StateMachine {
       metadata_store: Arc<MetadataStoreImpl>,
       prepared_txns: HashMap<TxId, PreparedTransaction>,
   }

   impl StateMachine {
       async fn apply(&mut self, entry: &LogEntry) -> Result<Response, Error> {
           let operation = bincode::deserialize(&entry.data)?;
           match operation {
               Operation::CreateFile(req) => {
                   let file_id = self.metadata_store.create_file(
                       &req.path, req.inode, req.metadata
                   ).await?;
                   Ok(Response::FileCreated(file_id))
               }
               Operation::TransactionPrepare(txn) => {
                   // Store in prepared_txns
                   self.prepared_txns.insert(txn.id, txn);
                   Ok(Response::Prepared)
               }
               Operation::TransactionCommit(tx_id) => {
                   // Apply prepared transaction
                   let txn = self.prepared_txns.remove(&tx_id)
                       .ok_or(Error::TransactionNotFound)?;
                   self.apply_transaction(txn).await?;
                   Ok(Response::Committed)
               }
           }
       }
   }
   ```

**Deliverables**:
- OpenRaft node initialized
- RaftStorage adapter implemented
- State machine for metadata operations

#### Step 7: Leader Election & Log Replication
**File**: `src/storage_raft_member/consensus.rs`

**Tasks**:
1. Implement cluster initialization
   ```rust
   impl StorageRaftMemberImpl {
       pub async fn initialize_cluster(&self, members: Vec<NodeId>) -> Result<(), Error> {
           if members.len() == 1 {
               // Bootstrap single-node cluster
               self.raft.initialize(members.clone().into()).await?;
           } else {
               // Multi-node initialization
               let membership = members.into_iter().collect();
               self.raft.change_membership(membership, false).await?;
           }
           Ok(())
       }

       pub async fn add_learner(&self, node_id: NodeId, node_addr: String) -> Result<(), Error> {
           self.raft.add_learner(node_id, node_addr, true).await?;
           Ok(())
       }
   }
   ```

2. Implement operation proposal
   ```rust
   impl StorageRaftMember for StorageRaftMemberImpl {
       async fn propose_operation(&self, op: Self::Operation)
           -> Result<Self::OperationResult, Error> {
           // Check if leader
           if !self.is_leader().await {
               return Err(Error::NotLeader(self.get_leader().await));
           }

           // Serialize operation
           let data = bincode::serialize(&op)?;

           // Propose through Raft
           let result = self.raft.client_write(data).await?;

           // Deserialize response
           Ok(bincode::deserialize(&result.data)?)
       }
   }
   ```

3. Handle leadership changes
   ```rust
   async fn monitor_leadership(raft: Arc<Raft<TypeConfig>>) {
       let mut metrics_rx = raft.metrics();

       while let Some(metrics) = metrics_rx.recv().await {
           match metrics.state {
               ServerState::Leader => {
                   info!("Became leader, term: {}", metrics.current_term);
                   // Start lease management
                   // Resume pending transactions
               }
               ServerState::Follower => {
                   info!("Became follower, leader: {:?}", metrics.current_leader);
                   // Stop lease management
                   // Redirect clients to leader
               }
               ServerState::Candidate => {
                   info!("In election, term: {}", metrics.current_term);
               }
           }
       }
   }
   ```

**Deliverables**:
- Leader election working
- Operation proposal and replication
- Leadership change handling

#### Step 8: Two-Phase Commit Protocol
**File**: `src/storage_raft_member/transactions.rs`

**Tasks**:
1. Implement transaction coordinator (leader only)
   ```rust
   pub struct TransactionCoordinator {
       active_txns: HashMap<TxId, TransactionState>,
       raft: Arc<Raft<TypeConfig>>,
   }

   impl TransactionCoordinator {
       pub async fn begin_transaction(&mut self, ops: Vec<MetadataOp>)
           -> Result<TxId, Error> {
           let tx_id = TxId::new();

           // Phase 1: Prepare
           let prepare = Operation::TransactionPrepare(PrepareRequest {
               tx_id,
               operations: ops,
               timeout: Duration::from_secs(30),
           });

           let result = self.raft.client_write(prepare).await?;

           // Track transaction state
           self.active_txns.insert(tx_id, TransactionState::Prepared);

           Ok(tx_id)
       }

       pub async fn commit_transaction(&mut self, tx_id: TxId) -> Result<(), Error> {
           // Phase 2: Commit
           let commit = Operation::TransactionCommit(tx_id);
           self.raft.client_write(commit).await?;

           self.active_txns.remove(&tx_id);
           Ok(())
       }
   }
   ```

2. Handle transaction recovery after leader change
   ```rust
   async fn recover_transactions(raft: Arc<Raft<TypeConfig>>, log: Arc<TransactionLogStore>) {
       // Scan recent log entries for prepared but not committed transactions
       let recent_entries = log.read_range(last_committed - 1000, last_committed).await?;

       for entry in recent_entries {
           if let Operation::TransactionPrepare(txn) = deserialize(&entry.data)? {
               // Check if committed or aborted
               if !is_resolved(&txn.tx_id).await {
                   // Conservative: abort after timeout
                   if txn.created_at.elapsed() > Duration::from_secs(60) {
                       let abort = Operation::TransactionAbort(txn.tx_id);
                       raft.client_write(abort).await?;
                   }
               }
           }
       }
   }
   ```

**Deliverables**:
- 2PC coordinator implemented
- Transaction recovery after leader change
- Timeout-based abort logic

#### Step 9: Update MetadataStore for Consensus
**File**: `src/metadata_store/consensus_adapter.rs`

**Tasks**:
1. Add transaction support to MetadataStore
   ```rust
   impl MetadataStoreImpl {
       pub async fn prepare_transaction(&self, tx_id: TxId, ops: Vec<MetadataOp>)
           -> Result<PrepareVote, Error> {
           // Validate operations
           for op in &ops {
               if !self.validate_operation(op).await? {
                   return Ok(PrepareVote::Abort);
               }
           }

           // Stage changes (not visible yet)
           let mut staged = self.staged_transactions.write().await;
           staged.insert(tx_id, ops);

           Ok(PrepareVote::Commit)
       }

       pub async fn commit_transaction(&self, tx_id: TxId) -> Result<(), Error> {
           let mut staged = self.staged_transactions.write().await;
           let ops = staged.remove(&tx_id)
               .ok_or(Error::TransactionNotFound)?;

           // Apply operations to database
           let mut conn = self.write_conn.lock().await;
           let tx = conn.transaction()?;

           for op in ops {
               self.apply_operation(&tx, op).await?;
           }

           tx.commit()?;

           // Notify subscribers
           self.notify_change(MetadataChangeEvent {
               tx_id,
               committed_at: SystemTime::now(),
           }).await;

           Ok(())
       }
   }
   ```

2. Implement metadata change subscriptions
   ```rust
   impl MetadataStoreImpl {
       pub async fn subscribe_changes(&self, filter: MetadataChangeType)
           -> mpsc::Receiver<MetadataChangeEvent> {
           let (tx, rx) = mpsc::channel(100);
           self.subscribers.write().await.push((filter, tx));
           rx
       }

       async fn notify_change(&self, event: MetadataChangeEvent) {
           let subscribers = self.subscribers.read().await;
           for (filter, tx) in subscribers.iter() {
               if filter.matches(&event) {
                   let _ = tx.send(event.clone()).await;
               }
           }
       }
   }
   ```

**Deliverables**:
- Transaction support in MetadataStore
- Metadata change notifications
- Integration with Raft state machine

#### Step 10: Testing Consensus
**File**: `tests/integration/phase2_consensus_test.rs`

**Tasks**:
1. Test 3-node cluster formation
   ```rust
   #[tokio::test]
   async fn test_three_node_raft_cluster() {
       let cluster = TestCluster::new(3).await;

       // Wait for leader election
       tokio::time::sleep(Duration::from_secs(2)).await;

       // Verify one leader, two followers
       let leaders = cluster.get_leaders().await;
       assert_eq!(leaders.len(), 1);

       let leader_id = leaders[0];

       // Test operation through leader
       let result = cluster.nodes[leader_id]
           .propose_operation(Operation::CreateFile(...))
           .await;
       assert!(result.is_ok());

       // Verify replication to followers
       for node in &cluster.nodes {
           let file = node.metadata_store.get_file_by_path("/test").await;
           assert!(file.is_ok());
       }
   }
   ```

2. Test leader failover
   ```rust
   #[tokio::test]
   async fn test_leader_failover() {
       let cluster = TestCluster::new(3).await;
       let old_leader = cluster.get_leader().await;

       // Kill leader
       cluster.stop_node(old_leader).await;

       // Wait for new election
       tokio::time::sleep(Duration::from_secs(3)).await;

       // Verify new leader elected
       let new_leader = cluster.get_leader().await;
       assert_ne!(old_leader, new_leader);

       // Operations work through new leader
       let result = cluster.nodes[new_leader]
           .propose_operation(Operation::CreateFile(...))
           .await;
       assert!(result.is_ok());
   }
   ```

**Deliverables**:
- 3-node cluster test passing
- Leader failover verified
- Metadata consistency confirmed

---

### Week 3: Integration & Robustness (Days 11-15)

#### Step 11: Snapshot Support
**File**: `src/storage_raft_member/snapshot.rs`

**Tasks**:
1. Implement snapshot creation
   ```rust
   impl RaftStorageAdapter {
       async fn build_snapshot(&mut self) -> Result<Snapshot, StorageError> {
           // Create metadata snapshot
           let snapshot_data = self.metadata_store.create_snapshot().await?;

           let meta = SnapshotMeta {
               last_log_id: self.last_applied_log(),
               last_membership: self.last_membership(),
               snapshot_id: generate_snapshot_id(),
           };

           Ok(Snapshot {
               meta,
               data: Box::new(Cursor::new(snapshot_data)),
           })
       }

       async fn begin_receiving_snapshot(&mut self) -> Result<Box<SnapshotSink>, StorageError> {
           Ok(Box::new(SnapshotReceiver::new(
               self.metadata_store.clone()
           )))
       }
   }
   ```

2. Implement log compaction
   ```rust
   async fn compact_log(log_store: Arc<TransactionLogStore>, snapshot_index: u64) {
       // Trim log up to snapshot
       log_store.trim(snapshot_index).await?;

       info!("Compacted log up to index {}", snapshot_index);
   }
   ```

**Deliverables**:
- Snapshot creation and restoration
- Log compaction after snapshot
- Snapshot transfer between nodes

#### Step 12: Membership Changes
**File**: `src/storage_raft_member/membership.rs`

**Tasks**:
1. Implement node addition
   ```rust
   impl StorageRaftMemberImpl {
       pub async fn add_node(&self, node_id: NodeId, addr: SocketAddr)
           -> Result<(), Error> {
           // Add as learner first
           self.raft.add_learner(node_id, addr.to_string(), true).await?;

           // Wait for catch-up
           self.wait_for_learner_catchup(node_id).await?;

           // Promote to voter
           let membership = self.raft.membership_config();
           let mut new_membership = membership.clone();
           new_membership.add_voter(node_id);

           self.raft.change_membership(new_membership, false).await?;

           Ok(())
       }
   }
   ```

2. Implement node removal
   ```rust
   pub async fn remove_node(&self, node_id: NodeId) -> Result<(), Error> {
       let membership = self.raft.membership_config();
       let mut new_membership = membership.clone();
       new_membership.remove_node(node_id);

       self.raft.change_membership(new_membership, false).await?;

       Ok(())
   }
   ```

**Deliverables**:
- Dynamic node addition
- Safe node removal
- Membership reconfiguration

#### Step 13: Performance Optimization
**File**: `src/storage_raft_member/optimization.rs`

**Tasks**:
1. Implement pipeline optimization
   ```rust
   struct PipelinedReplication {
       max_in_flight: usize,
       in_flight: Arc<AtomicUsize>,
   }

   impl PipelinedReplication {
       async fn replicate_entries(&self, entries: Vec<LogEntry>) {
           let chunks = entries.chunks(10); // Batch size

           for chunk in chunks {
               while self.in_flight.load(Ordering::Relaxed) >= self.max_in_flight {
                   tokio::time::sleep(Duration::from_millis(10)).await;
               }

               self.in_flight.fetch_add(1, Ordering::Relaxed);

               tokio::spawn(async move {
                   // Send AppendEntries RPC
                   self.send_append_entries(chunk).await;
                   self.in_flight.fetch_sub(1, Ordering::Relaxed);
               });
           }
       }
   }
   ```

2. Add backpressure handling
   ```rust
   impl StorageRaftMemberImpl {
       async fn propose_with_backpressure(&self, op: Operation)
           -> Result<Response, Error> {
           // Check log lag
           let metrics = self.raft.metrics();
           let lag = metrics.last_log_index - metrics.last_applied;

           if lag > 1000 {
               return Err(Error::Backpressure);
           }

           self.propose_operation(op).await
       }
   }
   ```

**Deliverables**:
- Pipelined replication
- Batched AppendEntries
- Backpressure protection

#### Step 14: Monitoring & Metrics
**File**: `src/storage_raft_member/metrics.rs`

**Tasks**:
1. Expose Raft metrics
   ```rust
   impl StorageRaftMemberImpl {
       pub async fn get_metrics(&self) -> RaftMetrics {
           let metrics = self.raft.metrics();

           RaftMetrics {
               state: metrics.state,
               current_term: metrics.current_term,
               current_leader: metrics.current_leader,
               last_log_index: metrics.last_log_index,
               last_applied: metrics.last_applied_log,
               membership: metrics.membership_config.clone(),
           }
       }
   }
   ```

2. Add performance counters
   ```rust
   struct PerformanceCounters {
       proposals_total: AtomicU64,
       proposals_failed: AtomicU64,
       replication_latency: Histogram,
       election_count: AtomicU64,
   }
   ```

**Deliverables**:
- Raft metrics exposed
- Performance monitoring
- Health check endpoints

#### Step 15: Integration Testing & Documentation
**File**: `tests/integration/phase2_full_test.rs`

**Tasks**:
1. End-to-end consensus test
   ```rust
   #[tokio::test]
   async fn test_consensus_under_load() {
       let cluster = TestCluster::new(5).await;

       // Concurrent operations from multiple clients
       let mut handles = vec![];
       for i in 0..10 {
           let cluster = cluster.clone();
           handles.push(tokio::spawn(async move {
               for j in 0..100 {
                   let path = format!("/test/{}/{}", i, j);
                   cluster.create_file(&path).await.unwrap();
               }
           }));
       }

       // Wait for completion
       for handle in handles {
           handle.await.unwrap();
       }

       // Verify consistency
       for node in &cluster.nodes {
           let files = node.list_all_files().await.unwrap();
           assert_eq!(files.len(), 1000);
       }
   }
   ```

2. Network partition test
   ```rust
   #[tokio::test]
   async fn test_network_partition() {
       let cluster = TestCluster::new(5).await;

       // Create partition: [1,2] | [3,4,5]
       cluster.partition(vec![1,2], vec![3,4,5]).await;

       // Minority cannot make progress
       let result = cluster.nodes[0].create_file("/test").await;
       assert!(result.is_err());

       // Majority elects new leader and continues
       let result = cluster.nodes[3].create_file("/test2").await;
       assert!(result.is_ok());

       // Heal partition
       cluster.heal_partition().await;

       // Eventually consistent
       tokio::time::sleep(Duration::from_secs(5)).await;
       for node in &cluster.nodes {
           assert!(node.get_file("/test2").await.is_ok());
       }
   }
   ```

3. Create documentation
   - User guide for multi-node setup
   - Raft tuning parameters
   - Troubleshooting guide
   - Performance benchmarks

**Deliverables**:
- Load testing passed
- Partition tolerance verified
- Documentation complete
- Phase 2 milestone achieved

---

## Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Cluster Formation | 100% | 3+ nodes form cluster successfully |
| Leader Election | <3s | Time to elect leader after failure |
| Metadata Consistency | 100% | All nodes have same metadata after replication |
| Operation Latency | <100ms | Time to commit operation in 3-node cluster |
| Partition Tolerance | Pass | Majority partition continues operation |
| Test Coverage | >85% | Unit and integration tests for consensus |

## Risk Mitigation

### Technical Risks:
1. **OpenRaft Complexity**: Start with example code, gradually add features
2. **Network Issues**: Use reliable TCP, add retry logic
3. **State Machine Bugs**: Extensive testing, careful transaction handling
4. **Performance**: Start simple, optimize based on profiling

### Fallback Options:
- If OpenRaft issues: Consider simpler Raft library or basic implementation
- If libp2p complexity: Use simple TCP with tokio
- If redb issues: Fall back to append-only file

## Dependencies

### External Crates:
- `libp2p` - Peer-to-peer networking
- `openraft` - Raft consensus
- `redb` - Transaction log storage
- `tokio` - Async runtime
- `bincode` - Serialization
- `tracing` - Logging

## Integration Points with Phase 1

1. **MetadataStore**: Add transaction support while maintaining Phase 1 interface
2. **FileStore**: No changes needed for Phase 2
3. **FileSystemService**: Update to use consensus for writes
4. **StorageNode**: Add network and Raft initialization

## Next Steps After Phase 2

Once Phase 2 is complete and tested:
1. Benchmark consensus performance
2. Test with 5-7 node clusters
3. Prepare for Phase 3 (distributed storage)
4. Consider optimization opportunities

## Notes

- Focus on correctness over performance initially
- Keep Phase 1 functionality working throughout
- Document all consensus-related decisions
- Prepare for Phase 3 distributed operations