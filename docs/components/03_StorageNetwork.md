# StorageNetwork Component Design

## Purpose & Responsibilities

StorageNetwork provides the peer-to-peer networking layer for WormFS using libp2p. Its responsibilities include:

- Establishing and maintaining libp2p swarm connectivity between storage nodes
- Peer discovery and connection management
- Providing topic-based pub/sub channels for different subsystems
- Implementing secure, authenticated communication using libp2p's built-in encryption
- Managing peer identity validation (explicit peer IDs or auto-discovery mode)
- Handling network health monitoring and connection state
- Providing efficient direct chunk transfer streams between nodes
- Supporting multiple concurrent protocol handlers without deadlocks

## Architecture: Client Pattern with Interior Mutability

StorageNetwork uses the client pattern with interior mutability to satisfy OpenRaft's ownership requirements while allowing concurrent access from multiple components.

### Why This Pattern?

**OpenRaft Compatibility**: OpenRaft's API requires exclusive ownership of its storage components (via `impl RaftStorage` trait bounds). However, other components in WormFS (StorageEndpoint, Watchdog, etc.) need concurrent read access to network state and the ability to send messages. The traditional `Arc<RwLock<StorageNetwork>>` approach conflicts with OpenRaft's ownership model.

**Solution**: We implement a "client handle" pattern where:
1. The outer `StorageNetwork` struct is lightweight and cloneable
2. All shared state lives in an `Arc<NetworkInner>` 
3. Each component holds its own cloned instance of `StorageNetwork`
4. OpenRaft "owns" one instance, while other components hold clones
5. The event loop channel (`event_tx`) enables non-blocking command submission

### Structure

```rust
struct NetworkInner {
    swarm: RwLock<Swarm<WormFsBehaviour>>,
    peers: RwLock<HashMap<PeerId, PeerState>>,
    topics: RwLock<HashMap<String, TopicHandle>>,
    config: NetworkConfig,
}

#[derive(Clone)]
pub struct StorageNetwork {
    inner: Arc<NetworkInner>,
    event_tx: mpsc::UnboundedSender<NetworkCommand>,
}
```

### Key Benefits

1. **OpenRaft Compatibility**: OpenRaft can take ownership of a `StorageNetwork` instance without preventing other components from accessing network functionality

2. **Concurrent Access**: Multiple components can safely call methods concurrently. The RwLock allows multiple readers or one writer.

3. **Thread Safety**: All operations are synchronized through RwLock and the command channel, preventing race conditions.

4. **Non-Blocking Operations**: The event_tx channel allows components to submit network commands without blocking on lock acquisition.

5. **Event Loop Isolation**: The swarm event loop runs independently, processing both libp2p events and command channel messages.

## Architecture & Design

### Internal Mutability Pattern

```
┌─────────────────────────────────────────────────────────┐
│              StorageNetwork (Arc-friendly)               │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  Inner State (Mutex/RwLock)                              │
│  ┌─────────────────────────────────────────────────┐   │
│  │  • libp2p Swarm                                  │   │
│  │  • Active connections map                        │   │
│  │  │  • Peer validation state                       │   │
│  │  • Topic subscriptions                           │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  Public API (non-blocking)                               │
│  ┌─────────────────────────────────────────────────┐   │
│  │  join_topic(name) -> (Sender, Receiver)         │   │
│  │  send_to_peer(peer_id, data)                     │   │
│  │  open_stream(peer_id, protocol) -> Stream       │   │
│  │  get_peers() -> Vec<PeerInfo>                   │   │
│  └─────────────────────────────────────────────────┘   │
│                                                           │
│  Background Swarm Event Loop (exclusive)                 │
│  ┌─────────────────────────────────────────────────┐   │
│  │  loop {                                          │   │
│  │    swarm.select_next_some() => handle_event()  │   │
│  │    topic_rx.recv() => forward_to_swarm()       │   │
│  │  }                                               │   │
│  └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
         │           │            │
         ▼           ▼            ▼
     Raft RPC   Chunk Transfer  Gossip
```

### Component Usage Pattern

```rust
// Multiple components can share the network without ownership issues
let network = Arc::new(StorageNetwork::new(config)?);

// RaftMember joins the membership topic to elect leaders and monitor peer health
let (membership_tx, membership_rx) = network.join_topic("membership").await?;

// RaftMember joins the filesystem topic to propose and vote 
// on transactions against our distributed filesystem.
let (filesystem_tx, filesystem_rx) = network.join_topic("filesystem").await?;

// MetricService joins this topic to produce and consume metrics to/from other StorageNodes
let (stats_tx, stats_rx) = network.join_topic("stats").await?;

// Each component can send/receive independently
tokio::spawn(async move {
    while let Some(msg) = raft_rx.recv().await {
        // Process Raft messages
    }
});
```

### Peer Discovery & Validation

**Explicit Peer ID Mode:**
- Configuration specifies exact peer IDs for each IP
- Connections rejected if peer ID doesn't match
- Maximum security, requires pre-configuration

**Auto-ID Mode:**
- Accept any peer ID on first connection
- Store discovered peer ID durably
- Enforce same peer ID on subsequent connections
- Balance of security and ease of use

```
First Connection (auto_id):
  1. Peer connects from configured IP
  2. Extract and validate peer ID from libp2p handshake
  3. Store peer_id in durable config/database
  4. Accept connection

Subsequent Connections:
  1. Peer connects from configured IP
  2. Look up stored peer ID for this IP
  3. Validate against stored value
  4. Reject if mismatch, accept if match
```

## Interfaces

### Public API

struct NetworkInner {
    swarm: RwLock<Swarm<WormFsBehaviour>>,
    peers: RwLock<HashMap<PeerId, PeerState>>,
    topics: RwLock<HashMap<String, TopicHandle>>,
    config: NetworkConfig,
}

#[derive(Clone)]
pub struct StorageNetwork {
    inner: Arc<NetworkInner>,
    event_tx: mpsc::UnboundedSender<NetworkCommand>,
}

impl StorageNetwork {
    /// Create a new network instance
    pub async fn new(config: NetworkConfig) -> Result<Self, NetworkError>;
    
    /// Start the swarm event loop (must be called once)
    pub async fn run(&self) -> Result<(), NetworkError>;
    
    /// Join a topic and get channels for communication
    pub async fn join_topic(
        &self,
        topic_name: &str,
    ) -> Result<(TopicSender, TopicReceiver), NetworkError>;
    
    /// Send a message to a specific peer on a topic
    pub async fn send_to_peer(
        &self,
        peer_id: &PeerId,
        topic: &str,
        message: Vec<u8>,
    ) -> Result<(), NetworkError>;
    
    /// Broadcast a message to all peers on a topic
    pub async fn broadcast(
        &self,
        topic: &str,
        message: Vec<u8>,
    ) -> Result<(), NetworkError>;
    
    /// Open a direct stream to a peer for bulk data transfer
    pub async fn open_stream(
        &self,
        peer_id: &PeerId,
        protocol: &str,
    ) -> Result<Stream, NetworkError>;
    
    /// Get list of currently connected peers
    pub fn get_connected_peers(&self) -> Vec<PeerInfo>;
    
    /// Get detailed information about a specific peer
    pub fn get_peer_info(&self, peer_id: &PeerId) -> Option<PeerInfo>;
    
    /// Disconnect from a specific peer
    pub async fn disconnect_peer(&self, peer_id: &PeerId) -> Result<(), NetworkError>;
    
    /// Validate and potentially update stored peer ID (for auto_id mode)
    pub async fn validate_peer_id(
        &self,
        ip: IpAddr,
        peer_id: PeerId,
    ) -> Result<ValidationResult, NetworkError>;
}

/// Commands sent to the network event loop
enum NetworkCommand {
    JoinTopic {
        name: String,
        response: oneshot::Sender<Result<TopicHandle, NetworkError>>,
    },
    SendToPeer {
        peer_id: PeerId,
        topic: String,
        message: Vec<u8>,
    },
    Broadcast {
        topic: String,
        message: Vec<u8>,
    },
    OpenStream {
        peer_id: PeerId,
        protocol: String,
        response: oneshot::Sender<Result<Stream, NetworkError>>,
    },
}

pub struct TopicHandle {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    rx: mpsc::UnboundedReceiver<TopicMessage>,
}

pub struct TopicMessage {
    pub source: PeerId,
    pub data: Vec<u8>,
    pub timestamp: SystemTime,
}
```

### Configuration

```rust
pub struct NetworkConfig {
    /// This node's peer ID (derived from keypair)
    pub local_keypair: Keypair,
    
    /// libp2p listening addresses
    pub listen_addresses: Vec<Multiaddr>,
    
    /// Configured peers
    pub peers: Vec<PeerConfig>,
    
    /// Path to store discovered peer IDs
    pub peer_id_store_path: PathBuf,
    
    /// Connection limits
    pub max_peers: usize,
    pub max_connections_per_peer: usize,
    
    /// Timeouts
    pub connection_timeout: Duration,
    pub idle_connection_timeout: Duration,
    
    /// Keep-alive settings
    pub keep_alive_interval: Duration,
}

pub struct PeerConfig {
    pub ip_address: IpAddr,
    pub peer_id: PeerIdConfig,
}

pub enum PeerIdConfig {
    /// Exact peer ID required
    Explicit(PeerId),
    /// Accept and store peer ID on first connection
    AutoId,
}

pub struct PeerState {
    pub peer_id: PeerId,
    pub addresses: Vec<Multiaddr>,
    pub connection_state: ConnectionState,
    pub last_seen: SystemTime,
    pub validation_status: ValidationStatus,
}

#[derive(Debug, Clone, Copy)]
pub enum ConnectionState {
    Connected,
    Connecting,
    Disconnected,
    Failed,
}

pub enum ValidationStatus {
    Validated,
    AutoDiscovered,
    Pending,
    Failed(String),
}
```

### libp2p Behavior

```rust
#[derive(NetworkBehaviour)]
pub struct WormFsBehaviour {
    /// Gossipsub for topic-based messaging
    gossipsub: gossipsub::Behaviour,
    
    /// mDNS for local peer discovery (optional)
    mdns: mdns::tokio::Behaviour,
    
    /// Request-response for direct RPC
    request_response: request_response::Behaviour<WormFsCodec>,
    
    /// Keep-alive for connection maintenance
    keep_alive: keep_alive::Behaviour,
    
    /// Identify for peer info exchange
    identify: identify::Behaviour,
}
```

## Dependencies

### Direct Dependencies
- **PeerIdStore**: Persistent storage for auto-discovered peer IDs
- **Configuration**: Network settings and peer list

### External Dependencies
- `libp2p`: Core networking library
  - `libp2p-gossipsub`: Pub/sub messaging
  - `libp2p-request-response`: Direct RPC
  - `libp2p-noise`: Transport encryption
  - `libp2p-tcp`: TCP transport
  - `libp2p-mdns`: Local discovery (optional)
- `tokio`: Async runtime
- `serde`: Message serialization
- `tracing`: Structured logging

## Data Structures

```rust
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub addresses: Vec<Multiaddr>,
    pub connection_state: ConnectionState,
    pub latency: Option<Duration>,
    pub last_seen: SystemTime,
    pub protocol_version: String,
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("Failed to initialize swarm: {0}")]
    SwarmInitError(String),
    
    #[error("Peer not found: {0}")]
    PeerNotFound(PeerId),
    
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
    
    #[error("Send failed: {0}")]
    SendFailed(String),
    
    #[error("Peer validation failed: {0}")]
    ValidationFailed(String),
    
    #[error("Stream error: {0}")]
    StreamError(String),
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),
}

pub enum ValidationResult {
    /// Peer ID matches expected value
    Validated,
    /// First time seeing this peer, ID stored
    NewlyDiscovered(PeerId),
    /// Peer ID mismatch
    Rejected { expected: PeerId, actual: PeerId },
}
```

## Configuration

```toml
[network]
# libp2p configuration
listen_addresses = ["/ip4/0.0.0.0/tcp/7001"]
max_peers = 100
max_connections_per_peer = 3

# Timeouts
connection_timeout_secs = 30
idle_connection_timeout_secs = 600
keep_alive_interval_secs = 30

# Peer configuration
peer_id_store_path = "/var/lib/wormfs/peer_ids.db"

[[network.peers]]
ip = "10.0.0.2"
peer_id = "auto_id"

[[network.peers]]
ip = "10.0.0.3"
peer_id = "12D3KooWABC123..."  # Explicit peer ID

# Gossipsub tuning
[network.gossipsub]
heartbeat_interval_ms = 1000
history_length = 5
history_gossip = 3
mesh_n = 6
mesh_n_low = 4
mesh_n_high = 12
```

## Error Handling

### Connection Failures
- Automatic retry with exponential backoff
- Configurable max retry attempts
- Peer marked as failed after exhausting retries
- Health monitoring can trigger manual reconnection

### Peer Validation Failures
- Auto-ID mode: Store rejection reason, require manual intervention
- Explicit mode: Log error and reject connection
- Validation failures reported to monitoring system

### Network Partitions
- Gossipsub handles message routing around partitions
- Connection state tracked per peer
- Raft layer handles consensus implications

### Stream Errors
- Timeout on stream operations
- Automatic stream cleanup on errors
- Retry logic for transient failures
- Back-pressure handling for slow receivers

## Testing Strategy

### Unit Tests
- Peer ID validation logic (explicit vs auto-id)
- Topic subscription and message routing
- Connection state transitions
- Error handling and retry logic

### Integration Tests
- Multi-node swarm formation
- Topic-based messaging between nodes
- Stream-based bulk data transfer
- Peer disconnect and reconnect
- Auto-ID discovery and enforcement

### Network Tests
- Simulated network partitions
- High latency scenarios
- Packet loss conditions
- Bandwidth limitations
- Concurrent connection storms

## Open Questions

1. **Transport Selection**: Should we support QUIC in addition to TCP? QUIC offers better performance but requires UDP, which some networks block.

2. **Local Discovery**: Should we enable mDNS for automatic local peer discovery, or rely solely on configured peer lists?

3. **NAT Traversal**: Do we need to support NAT hole-punching (via libp2p-relay), or can we assume direct connectivity between storage nodes?

4. **Message Size Limits**: What should be the maximum message size for gossipsub? Large metadata operations might exceed default limits.

5. **Topic Security**: Should we implement topic-level access control, or rely on peer authentication being sufficient?

6. **Protocol Versioning**: How should we handle protocol version mismatches between nodes? Reject connection, downgrade, or maintain compatibility matrix?

7. **Bandwidth Throttling**: Should StorageNetwork implement bandwidth limiting for chunk transfers to prevent network saturation?

8. **Connection Pooling**: Should we maintain connection pools per protocol, or share connections across all protocols?

9. **Metrics Export**: What network metrics should be exposed? Connection count, bandwidth usage, message latency, error rates?

10. **Peer Reputation**: Should we implement a peer reputation system to deprioritize unreliable peers?

11. **Auto-ID Security**: In auto-id mode, should we require manual admin approval for newly discovered peers before trusting them?

12. **Encryption Options**: Should we support different encryption algorithms beyond libp2p-noise (e.g., TLS)?

13. **Topic Persistence**: Should topic subscriptions be persisted across node restarts, or re-established on startup?

14. **Network Topology**: Should we support different network topologies (mesh, star, hierarchical) or stick with full mesh?

15. **Compression**: Should we enable message compression for gossipsub and request-response protocols?
