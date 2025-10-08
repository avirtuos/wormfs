# Phase 2B: Detailed Implementation Plan for libp2p Transport

## Overview

This document outlines the incremental implementation strategy for Phase 2B: Network Transport & Cluster Testing. The implementation is broken down into manageable chunks that can be implemented and tested independently.

## Architecture Overview

### Component Structure

```
src/transport/
├── mod.rs                    # Existing: Error types, PeerInfo, exports
├── libp2p_network.rs         # To expand: Main network implementation
├── peer_manager.rs           # Existing: Health monitoring
├── protocol.rs               # NEW: Raft RPC protocol definitions
└── codec.rs                  # NEW: Message serialization/deserialization
```

### Key Design Decisions

1. **Protocol Layer:** Use libp2p request-response protocol for Raft RPCs
2. **Serialization:** Protobuf for all message serialization
3. **Codec Separation:** Separate codec layer for clean serialization
4. **Architecture:** Event-driven with tokio channels
5. **Separation of Concerns:** Clear boundary between transport and Raft logic

## Implementation Chunks

### Chunk 1: Protocol Definition & Codec - COMPLETED

**Files:** 
- `src/transport/protocol.rs` (NEW)
- `src/transport/codec.rs` (NEW)

**Purpose:** Define the Raft RPC protocol for libp2p request-response and handle message serialization

**What to implement:**

#### protocol.rs
- Define `RaftProtocol` codec for request-response
- Implement `ProtocolName` trait
- Request/Response message types
- Protocol behavior configuration

```rust
pub struct RaftProtocol;

impl ProtocolName for RaftProtocol {
    fn protocol_name(&self) -> &[u8] {
        b"/wormfs/raft/1.0.0"
    }
}

pub struct RaftCodec;

impl RequestResponseCodec for RaftCodec {
    type Protocol = RaftProtocol;
    type Request = RaftRequest;   // From protobuf
    type Response = RaftResponse; // From protobuf
    
    async fn read_request<T>(...) -> io::Result<Self::Request>;
    async fn read_response<T>(...) -> io::Result<Self::Response>;
    async fn write_request<T>(...) -> io::Result<()>;
    async fn write_response<T>(...) -> io::Result<()>;
}
```

#### codec.rs
- Encode/decode functions for Raft messages
- Error handling for malformed messages
- Integration with prost

```rust
pub fn encode_raft_request(req: &RaftRequest) -> Result<Vec<u8>>;
pub fn decode_raft_request(bytes: &[u8]) -> Result<RaftRequest>;
pub fn encode_raft_response(resp: &RaftResponse) -> Result<Vec<u8>>;
pub fn decode_raft_response(bytes: &[u8]) -> Result<RaftResponse>;
```

**Why this first:** Establishes the communication contract before implementing networking

**Estimated Time:** 1-2 hours

---

### Chunk 2: Libp2p Swarm Setup (~200 lines) - COMPLETED

**File:** `src/transport/libp2p_network.rs` (expand existing)

**Purpose:** Initialize libp2p Swarm with proper configuration

**What to implement:**

```rust
use libp2p::{
    core::upgrade,
    noise,
    tcp,
    yamux,
    swarm::{Swarm, SwarmBuilder},
    request_response::{RequestResponse, RequestResponseConfig},
};

pub struct Libp2pNetwork {
    swarm: Swarm<RaftBehaviour>,
    local_peer_id: PeerId,
    config: NetworkConfig,
    peer_manager: PeerManager,
    // Event channels for async communication
    event_tx: mpsc::Sender<NetworkEvent>,
    event_rx: mpsc::Receiver<NetworkEvent>,
}

// NetworkBehaviour for Raft
#[derive(NetworkBehaviour)]
struct RaftBehaviour {
    request_response: RequestResponse<RaftCodec>,
}

impl Libp2pNetwork {
    pub fn new(config: NetworkConfig) -> Result<Self> {
        // 1. Set up TCP transport
        // 2. Add noise authentication
        // 3. Add yamux multiplexing
        // 4. Configure request-response behavior
        // 5. Build and return Swarm
    }
}
```

**Components:**
- TCP transport setup
- Noise authentication for encryption
- Yamux multiplexing
- Request-response behavior for Raft RPCs
- Swarm initialization
- Event channel creation

**Why this second:** Core infrastructure needed before connections

**Estimated Time:** 2-3 hours

---

### Chunk 3: Connection Management (~200 lines) - COMPLETED

**File:** `src/transport/libp2p_network.rs` (expand)

**Purpose:** Handle peer connections and lifecycle

**What to implement:**

```rust
impl Libp2pNetwork {
    /// Connect to all configured static peers
    pub async fn dial_peers(&mut self) -> Result<()> {
        for peer in &self.config.peers {
            let multiaddr = peer.address.parse()?;
            self.swarm.dial(multiaddr)?;
            tracing::info!("Dialing peer {} at {}", peer.node_id, peer.address);
        }
        Ok(())
    }
    
    /// Handle new connection established
    fn handle_connection_established(&mut self, peer_id: PeerId) {
        // Update peer manager
        // Log connection
        // Emit event
    }
    
    /// Handle connection closed
    fn handle_connection_closed(&mut self, peer_id: PeerId, cause: &ConnectionError) {
        // Update peer manager
        // Schedule reconnection if needed
        // Emit event
    }
    
    /// Automatic reconnection logic
    async fn reconnect_failed_peers(&mut self) {
        // Check peer manager for failed peers
        // Retry connection with backoff
    }
}
```

**Integration Points:**
- PeerManager for health tracking
- Automatic reconnection with exponential backoff
- Connection event handling
- Peer ID to NodeID mapping

**Why this third:** Enables actual peer-to-peer communication

**Estimated Time:** 2-3 hours

---

### Chunk 4: Message Sending (~150 lines) - COMPLETED

**File:** `src/transport/libp2p_network.rs` (expand)

**Purpose:** Implement outbound Raft RPC sending

**What to implement:**

```rust
impl Libp2pNetwork {
    /// Send AppendEntries RPC to target node
    pub async fn send_append_entries(
        &mut self,
        target: NodeId,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        let peer_id = self.node_id_to_peer_id(target)?;
        let raft_request = RaftRequest::AppendEntries(request);
        let response = self.send_request(peer_id, raft_request).await?;
        match response {
            RaftResponse::AppendEntries(resp) => Ok(resp),
            _ => Err(TransportError::Network("Unexpected response type".into())),
        }
    }
    
    /// Send Vote RPC to target node
    pub async fn send_vote(
        &mut self,
        target: NodeId,
        request: VoteRequest,
    ) -> Result<VoteResponse>;
    
    /// Send InstallSnapshot RPC to target node
    pub async fn send_install_snapshot(
        &mut self,
        target: NodeId,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse>;
    
    /// Generic request sender with timeout
    async fn send_request(
        &mut self,
        peer_id: PeerId,
        request: RaftRequest,
    ) -> Result<RaftResponse> {
        // Use request-response behavior
        // Apply timeout
        // Handle errors
        // Update peer manager on success/failure
    }
    
    /// Map NodeID to PeerID
    fn node_id_to_peer_id(&self, node_id: NodeId) -> Result<PeerId>;
}
```

**Features:**
- Type-safe RPC methods for each Raft message type
- Timeout handling per config
- Automatic peer health updates
- Error handling and retry logic

**Why this fourth:** Core Raft RPC functionality

**Estimated Time:** 1-2 hours

---

### Chunk 5: Message Receiving & Event Loop (~200 lines) - COMPLETED

**File:** `src/transport/libp2p_network.rs` (expand)

**Purpose:** Handle incoming messages and drive the event loop

**What to implement:**

```rust
/// Incoming message handler
pub type IncomingRequestHandler = Box<dyn Fn(RaftRequest) -> RaftResponse + Send>;

impl Libp2pNetwork {
    /// Main event loop
    pub async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                // Handle swarm events
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await?;
                }
                
                // Handle external commands
                cmd = self.event_rx.recv() => {
                    if let Some(cmd) = cmd {
                        self.handle_command(cmd).await?;
                    }
                }
                
                // Periodic maintenance
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    self.maintenance().await?;
                }
            }
        }
    }
    
    /// Handle incoming requests
    async fn handle_swarm_event(&mut self, event: SwarmEvent) -> Result<()> {
        match event {
            SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(event)) => {
                match event {
                    RequestResponseEvent::Message { peer, message } => {
                        self.handle_incoming_message(peer, message).await?;
                    }
                    RequestResponseEvent::OutboundFailure { peer, error, .. } => {
                        self.peer_manager.record_failure(peer);
                    }
                    RequestResponseEvent::InboundFailure { peer, error } => {
                        tracing::warn!("Inbound failure from {}: {:?}", peer, error);
                    }
                    _ => {}
                }
            }
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                self.handle_connection_established(peer_id);
            }
            SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                self.handle_connection_closed(peer_id, &cause);
            }
            _ => {}
        }
        Ok(())
    }
    
    /// Handle incoming Raft RPC
    async fn handle_incoming_message(
        &mut self,
        peer: PeerId,
        message: RequestResponseMessage<RaftRequest, RaftResponse>,
    ) -> Result<()> {
        match message {
            RequestResponseMessage::Request { request, channel, .. } => {
                // Call registered handler
                let response = (self.request_handler)(request);
                // Send response back
                self.swarm.behaviour_mut().request_response.send_response(channel, response)?;
            }
            RequestResponseMessage::Response { response, .. } => {
                // Handle response (already handled in send_request)
            }
        }
        Ok(())
    }
    
    /// Register handler for incoming requests
    pub fn set_request_handler(&mut self, handler: IncomingRequestHandler) {
        self.request_handler = handler;
    }
    
    /// Periodic maintenance tasks
    async fn maintenance(&mut self) -> Result<()> {
        // Reconnect to failed peers
        self.reconnect_failed_peers().await?;
        // Clean up old state
        Ok(())
    }
}
```

**Features:**
- Tokio-based async event loop
- Incoming request handling with callbacks
- Automatic reconnection on failures
- Health monitoring updates
- Command channel for external control

**Why this fifth:** Completes bidirectional communication

**Estimated Time:** 2-3 hours

---

### Chunk 6: Integration & Testing (~100 lines) - COMPLETED

**File:** `tests/transport_tests.rs` (NEW)

**Purpose:** Test transport layer independently

**What to implement:**

```rust
#[tokio::test]
async fn test_two_node_connection() {
    // Create two nodes with configs pointing to each other
    let node1 = Libp2pNetwork::new(config1).unwrap();
    let node2 = Libp2pNetwork::new(config2).unwrap();
    
    // Start both nodes
    tokio::spawn(async move { node1.run().await });
    tokio::spawn(async move { node2.run().await });
    
    // Wait for connection
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Verify connection established
    // Check peer manager shows connected
}

#[tokio::test]
async fn test_message_send_receive() {
    // Set up two connected nodes
    // Send a test message from node1 to node2
    // Verify node2 receives it
    // Verify response comes back
}

#[tokio::test]
async fn test_connection_failure_recovery() {
    // Set up two nodes
    // Simulate connection failure
    // Verify automatic reconnection
    // Verify peer manager updates correctly
}

#[tokio::test]
async fn test_multiple_peers() {
    // Set up 3-node cluster
    // Verify all nodes connect to each other
    // Test message passing between all pairs
}
```

**Test Coverage:**
- 2-node connection establishment
- Message send/receive roundtrip
- Connection failure and recovery
- Multi-node peer discovery
- Timeout handling
- Error scenarios

**Why this last:** Validates all components work together

**Estimated Time:** 1-2 hours

---

### Chunk 7: Efficient Snapshot Transfer (~300 lines)

**Files:**
- `proto/wormfs.proto` (modify)
- `src/transport/snapshot_transfer.rs` (NEW)
- `src/transport/libp2p_network.rs` (modify)

**Purpose:** Implement efficient out-of-band snapshot transfer to avoid pushing large snapshot data through Raft consensus

**Background:** Currently, `InstallSnapshotRequest` embeds snapshot data directly in the Raft message. This works for small metadata snapshots (<1MB) but becomes inefficient for larger snapshots. This chunk implements a two-phase approach: signaling through Raft, bulk transfer through HTTP/gRPC.

**What to implement:**

#### Phase 1: Modify Protobuf Definitions

```protobuf
// Modified InstallSnapshotRequest for signaling only:
message InstallSnapshotRequest {
  uint64 term = 1;
  uint64 leader_id = 2;
  uint64 last_included_index = 3;
  uint64 last_included_term = 4;
  
  // Removed: bytes data = 6;
  // Removed: uint64 offset = 5;
  // Removed: bool done = 7;
  
  // New: Provide access information instead
  string snapshot_url = 5;     // Where to download (e.g., "http://leader:8081/snapshots/abc123")
  string snapshot_id = 6;       // Unique snapshot identifier
  uint64 snapshot_size = 7;     // Total size in bytes for validation
  bytes snapshot_hash = 8;      // SHA256 hash for integrity verification
}

message InstallSnapshotResponse {
  uint64 term = 1;
  bool accepted = 2;    // Follower acknowledges and will download
  bool installed = 3;   // Follower has completed download and installation
}
```

#### Phase 2: HTTP Snapshot Transfer Server

```rust
// src/transport/snapshot_transfer.rs

use axum::{
    Router,
    extract::{Path, State},
    response::IntoResponse,
    http::StatusCode,
};
use tokio::fs::File;
use tokio_util::io::ReaderStream;

pub struct SnapshotTransferServer {
    snapshot_store: Arc<SnapshotStore>,
    listen_addr: SocketAddr,
}

impl SnapshotTransferServer {
    pub fn new(snapshot_store: Arc<SnapshotStore>, port: u16) -> Self {
        Self {
            snapshot_store,
            listen_addr: SocketAddr::from(([0, 0, 0, 0], port)),
        }
    }
    
    /// Start the HTTP server for snapshot transfers
    pub async fn run(self) -> Result<()> {
        let app = Router::new()
            .route("/snapshots/:snapshot_id", axum::routing::get(serve_snapshot))
            .with_state(self.snapshot_store);
        
        tracing::info!("Snapshot transfer server listening on {}", self.listen_addr);
        
        axum::Server::bind(&self.listen_addr)
            .serve(app.into_make_service())
            .await?;
        
        Ok(())
    }
}

/// Serve a snapshot file for download
async fn serve_snapshot(
    Path(snapshot_id): Path<String>,
    State(snapshot_store): State<Arc<SnapshotStore>>,
) -> Result<impl IntoResponse, StatusCode> {
    // Get snapshot path
    let snapshot_path = snapshot_store
        .get_snapshot_path(&snapshot_id)
        .ok_or(StatusCode::NOT_FOUND)?;
    
    // Open file for streaming
    let file = File::open(&snapshot_path)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    
    // Create streaming response
    let stream = ReaderStream::new(file);
    let body = axum::body::StreamBody::new(stream);
    
    Ok(body)
}
```

#### Phase 3: Snapshot Download Client

```rust
// src/transport/snapshot_transfer.rs

use reqwest;
use sha2::{Sha256, Digest};
use tokio::io::AsyncWriteExt;

pub struct SnapshotClient {
    http_client: reqwest::Client,
    snapshot_store: Arc<SnapshotStore>,
}

impl SnapshotClient {
    pub fn new(snapshot_store: Arc<SnapshotStore>) -> Self {
        Self {
            http_client: reqwest::Client::new(),
            snapshot_store,
        }
    }
    
    /// Download and install a snapshot from a peer
    pub async fn download_snapshot(
        &self,
        url: &str,
        snapshot_id: &str,
        expected_size: u64,
        expected_hash: &[u8],
    ) -> Result<()> {
        tracing::info!("Downloading snapshot {} from {}", snapshot_id, url);
        
        // Create temporary file
        let temp_path = self.snapshot_store.get_temp_path(snapshot_id);
        let mut file = tokio::fs::File::create(&temp_path).await?;
        
        // Download with streaming and hashing
        let mut response = self.http_client.get(url).send().await?;
        let mut hasher = Sha256::new();
        let mut downloaded = 0u64;
        
        while let Some(chunk) = response.chunk().await? {
            hasher.update(&chunk);
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
            
            // Progress logging
            if downloaded % (10 * 1024 * 1024) == 0 {  // Every 10MB
                tracing::info!(
                    "Snapshot download progress: {}/{}MB",
                    downloaded / 1024 / 1024,
                    expected_size / 1024 / 1024
                );
            }
        }
        
        file.flush().await?;
        drop(file);
        
        // Verify size
        if downloaded != expected_size {
            return Err(SnapshotError::SizeMismatch {
                expected: expected_size,
                actual: downloaded,
            });
        }
        
        // Verify hash
        let actual_hash = hasher.finalize();
        if actual_hash.as_slice() != expected_hash {
            return Err(SnapshotError::HashMismatch);
        }
        
        tracing::info!("Snapshot download verified, installing...");
        
        // Install snapshot (atomic move)
        self.snapshot_store.install_snapshot(snapshot_id, temp_path).await?;
        
        Ok(())
    }
}
```

#### Phase 4: Integration with Libp2pNetwork

```rust
// src/transport/libp2p_network.rs

impl Libp2pNetwork {
    /// Modified send_install_snapshot to use HTTP transfer
    pub async fn send_install_snapshot(
        &mut self,
        target: u64,
        snapshot_id: String,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<InstallSnapshotResponse> {
        let peer_id = self.node_id_to_peer_id(target)?;
        
        // Get snapshot metadata
        let snapshot_info = self.snapshot_store.get_info(&snapshot_id)?;
        
        // Build URL for HTTP transfer
        let snapshot_url = format!(
            "http://{}:{}/snapshots/{}",
            self.config.listen_address,
            self.config.snapshot_transfer_port,
            snapshot_id
        );
        
        // Send notification via Raft (lightweight)
        let raft_request = RaftRequest {
            request: Some(
                crate::raft::proto_types::proto::raft_request::Request::InstallSnapshot(
                    InstallSnapshotRequest {
                        term: self.current_term,
                        leader_id: self.config.node_id,
                        last_included_index,
                        last_included_term,
                        snapshot_url,
                        snapshot_id,
                        snapshot_size: snapshot_info.size,
                        snapshot_hash: snapshot_info.hash.to_vec(),
                    }
                )
            ),
        };
        
        let response = self.send_request(peer_id, raft_request).await?;
        
        match response.response {
            Some(crate::raft::proto_types::proto::raft_response::Response::InstallSnapshot(resp)) => {
                Ok(resp)
            }
            _ => Err(TransportError::Network(
                "Unexpected response type for InstallSnapshot".into(),
            )),
        }
    }
}
```

#### Phase 5: Follower Snapshot Installation

```rust
// In Raft node message handler

async fn handle_install_snapshot_request(
    &mut self,
    request: InstallSnapshotRequest,
) -> InstallSnapshotResponse {
    // Check term
    if request.term < self.current_term {
        return InstallSnapshotResponse {
            term: self.current_term,
            accepted: false,
            installed: false,
        };
    }
    
    // Accept the snapshot offer
    let snapshot_id = request.snapshot_id.clone();
    let response = InstallSnapshotResponse {
        term: self.current_term,
        accepted: true,
        installed: false,  // Will download in background
    };
    
    // Spawn background task to download
    let snapshot_client = self.snapshot_client.clone();
    let url = request.snapshot_url;
    let size = request.snapshot_size;
    let hash = request.snapshot_hash;
    
    tokio::spawn(async move {
        match snapshot_client.download_snapshot(&url, &snapshot_id, size, &hash).await {
            Ok(()) => {
                tracing::info!("Snapshot {} installed successfully", snapshot_id);
                // Send confirmation back to leader (optional)
            }
            Err(e) => {
                tracing::error!("Failed to download snapshot {}: {}", snapshot_id, e);
            }
        }
    });
    
    response
}
```

**Features:**
- HTTP-based snapshot transfer (port 8081)
- Streaming download with progress tracking
- SHA256 hash verification for integrity
- Atomic snapshot installation
- Background download (non-blocking)
- Resume capability (future enhancement)
- Compression support (future enhancement)

**Why this enhancement:**
- Prevents large snapshot data from congesting Raft protocol
- Enables efficient bulk transfer
- Supports standard HTTP infrastructure (CDN, caching)
- Better monitoring and debugging
- Scalable to large snapshots (GBs)

**Implementation Stages:**

1. **Stage 1 (Current - MVP)**: Keep embedded data approach
   - Works fine for small metadata snapshots (<1MB)
   - Good enough for initial testing
   
2. **Stage 2 (This Chunk)**: Add HTTP transfer server and client
   - Implement SnapshotTransferServer with axum
   - Implement SnapshotClient for downloads
   - Modify InstallSnapshot to use URLs
   
3. **Stage 3 (Future)**: Production optimizations
   - Add compression (zstd)
   - Implement resume capability (HTTP Range requests)
   - Support incremental snapshots
   - Enable P2P transfers between any nodes

**Dependencies:**
- Add to Cargo.toml:
  ```toml
  axum = "0.6"
  tokio-util = { version = "0.7", features = ["io"] }
  reqwest = { version = "0.11", features = ["stream"] }
  sha2 = "0.10"
  ```

**Testing:**
```rust
#[tokio::test]
async fn test_snapshot_http_transfer() {
    // Start snapshot server on node1
    let server = SnapshotTransferServer::new(snapshot_store.clone(), 8081);
    tokio::spawn(server.run());
    
    // Create test snapshot
    let snapshot_id = "test-snapshot-123";
    create_test_snapshot(snapshot_store.clone(), snapshot_id).await;
    
    // Download from node2
    let client = SnapshotClient::new(snapshot_store2.clone());
    client.download_snapshot(
        "http://localhost:8081/snapshots/test-snapshot-123",
        snapshot_id,
        expected_size,
        &expected_hash,
    ).await.unwrap();
    
    // Verify integrity
    assert_snapshot_installed(snapshot_store2, snapshot_id);
}
```

**Estimated Time:** 3-4 hours

---

## Implementation Timeline

### Estimated Time Per Chunk

1. **Chunk 1** (Protocol & Codec): 1-2 hours
2. **Chunk 2** (Swarm Setup): 2-3 hours
3. **Chunk 3** (Connections): 2-3 hours
4. **Chunk 4** (Send Messages): 1-2 hours
5. **Chunk 5** (Receive & Event Loop): 2-3 hours
6. **Chunk 6** (Testing): 1-2 hours

**Total:** 9-15 hours of implementation work

### Suggested Order

Implement in sequence 1→2→3→4→5→6, as each builds on the previous.

## After Transport Layer Completion

Once the transport layer is complete and tested, the next major components are:

### Raft Node Manager (`src/raft/node.rs`)
- Create Raft instance with storage components
- Implement RaftNetwork trait over libp2p transport
- Message routing for Raft operations
- Leader discovery and client routing
- Estimated: 400-600 lines, 4-6 hours

### 3-Node Cluster Integration Tests
- Full cluster setup with real networking
- Leader election validation (< 2s requirement)
- Log replication verification
- Network partition scenarios
- Failover testing
- Estimated: 200-300 lines, 3-4 hours

### Docker Compose Configuration
- Multi-node cluster setup
- Network configuration
- Volume management
- Environment variables
- Estimated: 1-2 hours

## Success Criteria

The libp2p transport implementation will be considered complete when:

1. ✅ Two nodes can connect to each other via libp2p
2. ✅ Nodes can send and receive Raft RPCs (AppendEntries, Vote, InstallSnapshot)
3. ✅ Connection failures are detected and automatic reconnection works
4. ✅ All transport tests pass
5. ✅ No memory leaks or connection leaks
6. ✅ Peer health monitoring correctly tracks connection status
7. ✅ Ready for integration with Raft node manager

## References

- [libp2p request-response documentation](https://docs.rs/libp2p-request-response/)
- [OpenRaft networking examples](https://github.com/databendlabs/openraft/tree/release-0.9/examples)
- WormFS protobuf definitions: `proto/wormfs.proto`
- Existing peer manager: `src/transport/peer_manager.rs`

## Notes

- All network code should be thoroughly async
- Use tracing for logging, not println
- Follow WormFS coding standards (clippy, rustfmt)
- Test coverage target: >90%
- Consider backpressure and flow control
- Handle edge cases: partial sends, connection drops, timeouts
