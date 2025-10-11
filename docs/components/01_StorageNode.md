# StorageNode Component Design

## Purpose & Responsibilities

StorageNode is the top-level orchestrator component that represents a fully-featured WormFS storage node. It serves as the main entry point and dependency injection container, responsible for:

- Initializing and wiring together all subsystem components
- Managing the component lifecycle (startup, shutdown, graceful degradation)
- Loading and validating configuration
- Providing health checks and status reporting
- Coordinating graceful shutdown sequences

## Architecture & Design

### Component Initialization Flow

```
StorageNode::new(config)
  ├─> Load & validate configuration
  ├─> Initialize StorageNetwork (libp2p swarm)
  ├─> Initialize TransactionLogStore (redb)
  ├─> Initialize MetadataStore (SQLite)
  ├─> Initialize SnapshotStore
  ├─> Initialize FileStore
  ├─> Initialize StorageRaftMember (OpenRaft)
  ├─> Initialize FileSystemService (FUSE API layer)
  ├─> Initialize StorageEndpoint (gRPC server)
  └─> Initialize StorageWatchdog
```

### Ownership Model

StorageNode uses `Arc<T>` for shared ownership of components that need to be accessed from multiple places:
- Components are initialized in dependency order
- Shared components are wrapped in `Arc` for thread-safe reference counting
- Components requiring interior mutability use `Arc<RwLock<T>>` or `Arc<Mutex<T>>`

### Graceful Shutdown

StorageNode implements a coordinated shutdown sequence:
1. Stop accepting new client requests (shutdown StorageEndpoint)
2. Allow in-flight requests to complete (timeout: configurable, default 30s)
3. Step down Raft leadership if leader
4. Flush pending Raft log entries
5. Stop StorageWatchdog
6. Close MetadataStore and TransactionLogStore cleanly
7. Shutdown StorageNetwork

## Interfaces

### Public API

```rust
pub struct StorageNode {
    config: StorageNodeConfig,
    network: Arc<StorageNetwork>,
    raft_member: Arc<StorageRaftMember>,
    metadata_store: Arc<MetadataStore>,
    file_store: Arc<FileStore>,
    snapshot_store: Arc<SnapshotStore>,
    transaction_log_store: Arc<TransactionLogStore>,
    file_system: Arc<FileSystemService>,
    endpoint: Arc<StorageEndpoint>,
    watchdog: Arc<StorageWatchdog>,
    shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
}

impl StorageNode {
    /// Create and initialize a new StorageNode
    pub async fn new(config: StorageNodeConfig) -> Result<Self, StorageNodeError>;
    
    /// Start all components and begin serving requests
    pub async fn start(&mut self) -> Result<(), StorageNodeError>;
    
    /// Gracefully shutdown the storage node
    pub async fn shutdown(&mut self) -> Result<(), StorageNodeError>;
    
    /// Get current node status and health information
    pub fn get_status(&self) -> NodeStatus;
    
    /// Check if this node is the Raft leader
    pub fn is_leader(&self) -> bool;
    
    /// Get cluster membership information
    pub async fn get_cluster_info(&self) -> Result<ClusterInfo, StorageNodeError>;
}
```

### Configuration Structure

```rust
pub struct StorageNodeConfig {
    // Node identity
    pub node_id: NodeId,
    pub listen_address: SocketAddr,
    
    // Network configuration
    pub peer_ips: Vec<PeerConfig>,
    pub libp2p_listen_port: u16,
    
    // Storage paths
    pub data_dir: PathBuf,
    pub metadata_db_path: PathBuf,
    pub transaction_log_path: PathBuf,
    pub snapshot_dir: PathBuf,
    
    // Raft configuration
    pub raft_config: RaftConfig,
    
    // Storage policy defaults
    pub default_stripe_size: u64,
    pub default_data_shards: u8,
    pub default_parity_shards: u8,
    
    // Lock settings
    pub enable_read_locks: bool,
    pub lock_timeout: Duration,
    pub lock_extend_interval: Duration,
    
    // Watchdog configuration
    pub shallow_check_interval: Duration,
    pub deep_check_interval: Duration,
    
    // Snapshot configuration
    pub snapshot_interval: Duration,
    pub snapshot_log_size_threshold: u64,
}

pub struct PeerConfig {
    pub ip_address: IpAddr,
    pub peer_id: PeerIdConfig,
}

pub enum PeerIdConfig {
    Explicit(PeerId),
    AutoId, // Accept and store peer ID on first connection
}
```

## Dependencies

### Direct Dependencies
- **StorageNetwork**: Peer-to-peer communication layer
- **StorageRaftMember**: Consensus and metadata coordination
- **MetadataStore**: Metadata persistence
- **FileStore**: Chunk storage and erasure coding
- **SnapshotStore**: Snapshot management
- **TransactionLogStore**: Raft log persistence
- **FileSystemService**: FUSE filesystem API layer
- **StorageEndpoint**: gRPC API server
- **StorageWatchdog**: Data integrity monitoring

### External Dependencies
- `tokio`: Async runtime
- `serde`: Configuration serialization
- `tracing`: Structured logging

## Data Structures

```rust
pub struct NodeStatus {
    pub node_id: NodeId,
    pub role: RaftRole, // Leader, Follower, Candidate
    pub cluster_size: usize,
    pub is_healthy: bool,
    pub uptime: Duration,
    pub metadata_store_status: StoreStatus,
    pub file_store_status: StoreStatus,
    pub last_snapshot_time: Option<SystemTime>,
    pub watchdog_status: WatchdogStatus,
}

pub struct ClusterInfo {
    pub leader_id: Option<NodeId>,
    pub nodes: Vec<NodeInfo>,
    pub total_storage_capacity: u64,
    pub used_storage: u64,
}

pub struct NodeInfo {
    pub node_id: NodeId,
    pub address: SocketAddr,
    pub role: RaftRole,
    pub is_healthy: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum StorageNodeError {
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    #[error("Component initialization failed: {0}")]
    InitializationError(String),
    
    #[error("Shutdown error: {0}")]
    ShutdownError(String),
    
    #[error("Network error: {0}")]
    NetworkError(#[from] NetworkError),
    
    #[error("Storage error: {0}")]
    StorageError(String),
}
```

## Configuration

Configuration can be loaded from:
1. TOML configuration file (primary)
2. Environment variables (override specific values)
3. Command-line arguments (highest priority)

Example configuration file:
```toml
[node]
node_id = "node-1"
listen_address = "0.0.0.0:7000"
data_dir = "/var/lib/wormfs"

[network]
libp2p_listen_port = 7001
peers = [
    { ip = "10.0.0.2", peer_id = "auto_id" },
    { ip = "10.0.0.3", peer_id = "12D3KooWABC..." }
]

[storage]
default_stripe_size = 1048576  # 1MB
default_data_shards = 6
default_parity_shards = 3

[locks]
enable_read_locks = true
lock_timeout_secs = 10
lock_extend_interval_secs = 5

[raft]
heartbeat_interval_ms = 250
election_timeout_min_ms = 1000
election_timeout_max_ms = 2000

[watchdog]
shallow_check_interval_mins = 5
deep_check_interval_hours = 24
```

## Error Handling

### Initialization Errors
- If any component fails to initialize, StorageNode performs cleanup of already-initialized components
- Configuration validation errors are reported with specific field information
- Network binding failures include retry logic with exponential backoff

### Runtime Errors
- Component-level errors are logged and reported via health checks
- Critical errors (e.g., metadata corruption) trigger graceful shutdown
- Transient errors are retried with backoff

### Recovery Strategies
- On restart after crash, StorageNode performs recovery:
  1. Validate transaction log integrity
  2. Replay uncommitted log entries
  3. Verify metadata store consistency
  4. Rejoin Raft cluster

## Testing Strategy

### Unit Tests
- Configuration loading and validation
- Component initialization order
- Shutdown sequence correctness
- Error propagation from components

### Integration Tests
- Full node startup and shutdown
- Component interaction via dependency injection
- Configuration override precedence
- Graceful degradation scenarios

### End-to-End Tests
- Multi-node cluster formation
- Node failure and recovery
- Rolling restarts
- Configuration changes

## Open Questions

1. **Component Initialization Parallelization**: Should we initialize independent components in parallel, or is strict sequential initialization preferred for simplicity and debugging?

2. **Health Check Depth**: How detailed should the `get_status()` health checks be? Should we query each component for detailed status, or maintain a lightweight cached status?

3. **Dynamic Reconfiguration**: Should StorageNode support hot-reloading of configuration changes (e.g., changing watchdog intervals) without restart, or require full restart?

4. **Metrics and Observability**: What metrics should StorageNode expose directly vs. delegate to components? Should we use Prometheus metrics, OpenTelemetry, or both?

5. **Component Failure Isolation**: If a non-critical component (e.g., StorageWatchdog) fails, should the node continue operating in degraded mode or shut down completely?

6. **Bootstrap Mode**: Should there be a special bootstrap mode for initializing the first node in a cluster vs. joining an existing cluster?

7. **Disk Management**: Should StorageNode handle disk discovery and configuration, or should disk paths be explicitly configured in the config file?

8. **Admin Interface Integration**: Should StorageNode include the WebUI/REST API server directly, or should that be a separate process that connects to StorageNode via gRPC?

9. **Version Compatibility**: How should StorageNode handle version mismatches between nodes in the cluster? Should older nodes be rejected, or should there be a compatibility matrix?

10. **Resource Limits**: Should StorageNode enforce resource limits (memory, CPU, disk I/O) for components, or rely on OS-level controls?
