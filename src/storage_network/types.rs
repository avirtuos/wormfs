//! Common types for the StorageNetwork component.

use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use thiserror::Error;

/// Unique identifier for a peer in the network.
///
/// PeerId represents a libp2p peer identity in the distributed system.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PeerId(pub Vec<u8>);

impl PeerId {
    /// Create a new PeerId from bytes.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Get the inner bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Configuration for a peer in the network.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    /// Multiaddr string for the peer (e.g., "/ip4/127.0.0.1/tcp/4242/p2p/...")
    /// If the peer ID is not included in the multiaddr, AutoId mode is used.
    pub multiaddr: String,

    /// Peer ID configuration (explicit or auto-discover)
    /// This can be used to validate the peer ID from the multiaddr
    #[serde(default)]
    pub peer_id: PeerIdConfig,
}

impl Default for PeerIdConfig {
    fn default() -> Self {
        PeerIdConfig::AutoId
    }
}

/// Peer ID configuration mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum PeerIdConfig {
    /// Exact peer ID required - reject connections with mismatched IDs
    #[serde(skip)]
    Explicit(PeerId),

    /// Accept and store peer ID on first connection, enforce on subsequent connections
    AutoId,
}

/// Network configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Node ID for this node (used in heartbeat messages and identification)
    pub node_id: String,

    /// Listen addresses for libp2p
    pub listen_addresses: Vec<String>,

    /// Configured peers
    #[serde(default)]
    pub peers: Vec<PeerConfig>,

    /// Path to store discovered peer IDs (for auto-ID mode)
    pub peer_id_store_path: PathBuf,

    /// Maximum number of peers to maintain
    #[serde(default = "default_max_peers")]
    pub max_peers: usize,

    /// Maximum connections per peer
    #[serde(default = "default_max_connections_per_peer")]
    pub max_connections_per_peer: usize,

    /// Connection timeout in seconds
    #[serde(default = "default_connection_timeout_secs", with = "duration_serde")]
    pub connection_timeout: Duration,

    /// Idle connection timeout in seconds
    #[serde(
        default = "default_idle_connection_timeout_secs",
        with = "duration_serde"
    )]
    pub idle_connection_timeout: Duration,

    /// Keep-alive interval in seconds
    #[serde(default = "default_keep_alive_interval_secs", with = "duration_serde")]
    pub keep_alive_interval: Duration,

    /// Admin UI URL (optional, used in heartbeat messages)
    #[serde(default)]
    pub admin_url: Option<String>,
}

fn default_max_peers() -> usize {
    100
}

fn default_max_connections_per_peer() -> usize {
    3
}

fn default_connection_timeout_secs() -> Duration {
    Duration::from_secs(30)
}

fn default_idle_connection_timeout_secs() -> Duration {
    Duration::from_secs(600) // 10 minutes
}

fn default_keep_alive_interval_secs() -> Duration {
    Duration::from_secs(30)
}

mod duration_serde {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

/// State of a peer in the network.
#[derive(Debug, Clone)]
pub struct PeerState {
    /// Peer identifier
    pub peer_id: PeerId,

    /// Known addresses for this peer
    pub addresses: Vec<String>,

    /// Current connection state
    pub connection_state: ConnectionState,

    /// Last time we saw activity from this peer
    pub last_seen: SystemTime,

    /// Validation status for this peer
    pub validation_status: ValidationStatus,

    /// Last time we received a heartbeat from this peer
    pub last_heartbeat: Option<SystemTime>,

    /// WormFS node ID (from heartbeat messages)
    pub node_id: Option<String>,

    /// Last heartbeat sequence number received
    pub heartbeat_sequence: Option<u64>,

    /// Admin UI URL (from heartbeat messages)
    pub admin_url: Option<String>,
}

/// Peer validation status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationStatus {
    /// Peer has been validated against configured peer ID
    Validated,

    /// Peer was auto-discovered (first connection in auto-ID mode)
    AutoDiscovered,

    /// Validation is pending
    Pending,

    /// Validation failed
    Failed,
}

/// Handle for a topic subscription.
#[derive(Debug)]
pub struct TopicHandle {
    /// Sender for publishing messages to this topic
    pub tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,

    /// Receiver for consuming messages from this topic
    pub rx: tokio::sync::mpsc::UnboundedReceiver<TopicMessage>,
}

/// Commands sent to the network event loop.
pub enum NetworkCommand {
    /// Join a topic and get a handle for communication
    JoinTopic {
        /// Name of the topic to join
        name: String,
        /// Channel to send response back
        response: tokio::sync::oneshot::Sender<Result<TopicHandle, Error>>,
    },

    /// Send a message to a specific peer on a topic
    SendToPeer {
        /// Target peer identifier
        peer_id: PeerId,
        /// Topic name
        topic: String,
        /// Message bytes
        message: Vec<u8>,
    },

    /// Broadcast a message to all peers on a topic
    Broadcast {
        /// Topic name
        topic: String,
        /// Message bytes
        message: Vec<u8>,
    },

    /// Open a stream to a peer for direct communication
    OpenStream {
        /// Target peer identifier
        peer_id: PeerId,
        /// Protocol name
        protocol: String,
        /// Channel to send response back
        response: tokio::sync::oneshot::Sender<Result<(), Error>>,
    },

    /// Send a request to a peer and await response (request-response protocol)
    SendRequest {
        /// Target peer identifier
        peer_id: PeerId,
        /// Protocol name
        protocol: String,
        /// Request data
        request: Vec<u8>,
        /// Channel to send response back
        response: tokio::sync::oneshot::Sender<Result<Vec<u8>, Error>>,
    },

    /// Dial a peer at the given multiaddr
    DialPeer {
        /// Multiaddr to dial
        multiaddr: String,
    },

    /// Disconnect from a specific peer
    DisconnectPeer {
        /// Target peer identifier
        peer_id: PeerId,
        /// Channel to send response back
        response: tokio::sync::oneshot::Sender<Result<(), Error>>,
    },

    /// Shutdown the network event loop gracefully
    Shutdown {
        /// Channel to send response back when shutdown is complete
        response: tokio::sync::oneshot::Sender<Result<(), Error>>,
    },

    /// Get list of currently connected peers
    GetConnectedPeers {
        /// Channel to send response back
        response: tokio::sync::oneshot::Sender<Vec<PeerInfo>>,
    },

    /// Get detailed information about a specific peer
    GetPeerInfo {
        /// Peer ID to query
        peer_id: PeerId,
        /// Channel to send response back
        response: tokio::sync::oneshot::Sender<Option<PeerInfo>>,
    },

    /// Set the metrics service for tracking network operations
    SetMetrics {
        /// Metrics service implementation
        metrics: std::sync::Arc<crate::metric_service::MetricServiceImpl>,
    },

    /// Register a Raft handler for processing incoming Raft RPCs
    RegisterRaftHandler {
        /// The Raft handler to register (trait object for flexibility)
        handler: std::sync::Arc<dyn crate::storage_raft_member::RaftRpcHandler>,
    },

    /// Update Raft heartbeat data for inclusion in gossipsub heartbeats
    UpdateRaftHeartbeatData {
        /// Raft state: "Leader", "Follower", "Candidate", "Learner", "Shutdown"
        raft_state: Option<String>,
        /// Current Raft term
        raft_term: Option<u64>,
        /// Last log index
        last_log_index: Option<u64>,
        /// Last log term
        last_log_term: Option<u64>,
        /// Current leader node ID
        current_leader: Option<u64>,
        /// Whether this node is a voter
        is_voter: Option<bool>,
        /// Node startup time (milliseconds since Unix epoch)
        startup_time: Option<u64>,
    },
}

impl std::fmt::Debug for NetworkCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::JoinTopic { name, .. } => {
                f.debug_struct("JoinTopic").field("name", name).finish()
            }
            Self::SendToPeer {
                peer_id,
                topic,
                message,
            } => f
                .debug_struct("SendToPeer")
                .field("peer_id", peer_id)
                .field("topic", topic)
                .field("message_len", &message.len())
                .finish(),
            Self::Broadcast { topic, message } => f
                .debug_struct("Broadcast")
                .field("topic", topic)
                .field("message_len", &message.len())
                .finish(),
            Self::OpenStream {
                peer_id, protocol, ..
            } => f
                .debug_struct("OpenStream")
                .field("peer_id", peer_id)
                .field("protocol", protocol)
                .finish(),
            Self::SendRequest {
                peer_id,
                protocol,
                request,
                ..
            } => f
                .debug_struct("SendRequest")
                .field("peer_id", peer_id)
                .field("protocol", protocol)
                .field("request_len", &request.len())
                .finish(),
            Self::DialPeer { multiaddr } => f
                .debug_struct("DialPeer")
                .field("multiaddr", multiaddr)
                .finish(),
            Self::DisconnectPeer { peer_id, .. } => f
                .debug_struct("DisconnectPeer")
                .field("peer_id", peer_id)
                .finish(),
            Self::Shutdown { .. } => f.debug_struct("Shutdown").finish(),
            Self::GetConnectedPeers { .. } => f.debug_struct("GetConnectedPeers").finish(),
            Self::GetPeerInfo { peer_id, .. } => f
                .debug_struct("GetPeerInfo")
                .field("peer_id", peer_id)
                .finish(),
            Self::SetMetrics { .. } => f
                .debug_struct("SetMetrics")
                .field("metrics", &"<metrics>")
                .finish(),
            Self::RegisterRaftHandler { .. } => f
                .debug_struct("RegisterRaftHandler")
                .field("handler", &"<raft_handler>")
                .finish(),
            Self::UpdateRaftHeartbeatData {
                raft_state,
                raft_term,
                last_log_index,
                ..
            } => f
                .debug_struct("UpdateRaftHeartbeatData")
                .field("raft_state", raft_state)
                .field("raft_term", raft_term)
                .field("last_log_index", last_log_index)
                .finish(),
        }
    }
}

/// Errors that can occur during network operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Peer not connected
    #[error("Peer {0:?} is not connected")]
    PeerNotConnected(PeerId),

    /// Topic does not exist
    #[error("Topic '{0}' does not exist")]
    TopicNotFound(String),

    /// Failed to join topic
    #[error("Failed to join topic '{topic}': {reason}")]
    JoinTopicFailed { topic: String, reason: String },

    /// Protocol not supported
    #[error("Protocol '{0}' is not supported by peer")]
    ProtocolNotSupported(String),

    /// Stream establishment failed
    #[error("Failed to establish stream: {0}")]
    StreamFailed(String),

    /// Request timeout
    #[error("Request to peer {peer:?} timed out after {timeout_ms}ms")]
    RequestTimeout { peer: PeerId, timeout_ms: u64 },

    /// Request failed
    #[error("Request to peer {peer:?} failed: {reason}")]
    RequestFailed { peer: PeerId, reason: String },

    /// Send operation failed
    #[error("Failed to send message: {0}")]
    SendFailed(String),

    /// Invalid message format
    #[error("Invalid message format: {0}")]
    InvalidMessage(String),

    /// Broadcast failed
    #[error("Failed to broadcast on topic '{topic}': {reason}")]
    BroadcastFailed { topic: String, reason: String },

    /// Peer validation failed
    #[error("Peer validation failed: {0}")]
    ValidationFailed(String),

    /// Event loop already running
    #[error("Network event loop is already running")]
    AlreadyRunning,

    /// Event loop failed to start
    #[error("Failed to start event loop: {0}")]
    EventLoopFailed(String),

    /// Disconnection failed
    #[error("Failed to disconnect from peer {peer:?}: {reason}")]
    DisconnectFailed { peer: PeerId, reason: String },

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Information about a connected peer.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Peer identifier
    pub peer_id: PeerId,

    /// Peer's IP addresses
    pub addresses: Vec<IpAddr>,

    /// Connection state
    pub state: ConnectionState,

    /// Time when peer connected
    pub connected_since: Option<SystemTime>,

    /// Protocols supported by this peer
    pub protocols: Vec<String>,

    /// Round-trip time to peer (if measured)
    pub rtt: Option<Duration>,

    /// Last time we received a heartbeat from this peer
    pub last_heartbeat: Option<SystemTime>,

    /// WormFS node ID (from heartbeat messages)
    pub node_id: Option<String>,

    /// Last heartbeat sequence number received
    pub heartbeat_sequence: Option<u64>,

    /// Admin UI URL (from heartbeat messages)
    pub admin_url: Option<String>,
}

/// Peer connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Peer is connected
    Connected,

    /// Connection in progress
    Connecting,

    /// Peer is disconnected
    Disconnected,

    /// Connection failed
    Failed,
}

/// Peer validation result for auto-ID mode.
#[derive(Debug, Clone)]
pub enum ValidationResult {
    /// Peer ID matches expected value
    Validated,

    /// First time seeing this peer, ID has been stored
    NewlyDiscovered(PeerId),

    /// Peer ID mismatch - connection should be rejected
    Rejected {
        /// Expected peer ID (from stored config)
        expected: PeerId,
        /// Actual peer ID (from connection)
        actual: PeerId,
    },
}

/// Sender for publishing messages to a topic.
pub type TopicSender = tokio::sync::mpsc::UnboundedSender<Vec<u8>>;

/// Receiver for consuming messages from a topic.
pub type TopicReceiver = tokio::sync::mpsc::UnboundedReceiver<TopicMessage>;

/// Message received from a topic.
#[derive(Debug, Clone)]
pub struct TopicMessage {
    /// Peer that sent the message
    pub source: PeerId,

    /// Message data
    pub data: Vec<u8>,

    /// Timestamp when message was received
    pub timestamp: SystemTime,
}

/// Heartbeat message exchanged between peers.
///
/// Heartbeats are broadcast periodically on the heartbeat topic to indicate
/// peer liveness and allow other nodes to track connection health.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    /// Node ID of the sender
    pub node_id: String,

    /// Timestamp when the heartbeat was sent (milliseconds since Unix epoch)
    pub timestamp_ms: u64,

    /// Sequence number for this heartbeat (incremented on each send)
    pub sequence: u64,

    /// Admin UI URL of the sender (optional)
    #[serde(default)]
    pub admin_url: Option<String>,

    // ==== Raft-Specific Fields for Cluster Discovery ====
    /// Current Raft state: "Leader", "Follower", "Candidate", "Learner", "Shutdown"
    #[serde(default)]
    pub raft_state: Option<String>,

    /// Current Raft term number
    #[serde(default)]
    pub raft_term: Option<u64>,

    /// Index of the last log entry
    #[serde(default)]
    pub last_log_index: Option<u64>,

    /// Term of the last log entry
    #[serde(default)]
    pub last_log_term: Option<u64>,

    /// Node ID of the known current leader
    #[serde(default)]
    pub current_leader: Option<u64>,

    /// Whether this node is a voter (true) or learner (false)
    #[serde(default)]
    pub is_voter: Option<bool>,

    /// When this node started up (milliseconds since Unix epoch)
    /// Used to detect node restarts
    #[serde(default)]
    pub startup_time: Option<u64>,
}

impl HeartbeatMessage {
    /// Create a new heartbeat message.
    pub fn new(node_id: String, sequence: u64) -> Self {
        let timestamp_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            node_id,
            timestamp_ms,
            sequence,
            admin_url: None,
            raft_state: None,
            raft_term: None,
            last_log_index: None,
            last_log_term: None,
            current_leader: None,
            is_voter: None,
            startup_time: None,
        }
    }

    /// Create a new heartbeat message with admin URL.
    pub fn with_admin_url(node_id: String, sequence: u64, admin_url: Option<String>) -> Self {
        let timestamp_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            node_id,
            timestamp_ms,
            sequence,
            admin_url,
            raft_state: None,
            raft_term: None,
            last_log_index: None,
            last_log_term: None,
            current_leader: None,
            is_voter: None,
            startup_time: None,
        }
    }

    /// Create a heartbeat message with full Raft state information.
    /// This is the primary constructor used by Raft-aware nodes for cluster discovery.
    pub fn with_raft_state(
        node_id: String,
        sequence: u64,
        admin_url: Option<String>,
        raft_state: Option<String>,
        raft_term: Option<u64>,
        last_log_index: Option<u64>,
        last_log_term: Option<u64>,
        current_leader: Option<u64>,
        is_voter: Option<bool>,
        startup_time: Option<u64>,
    ) -> Self {
        let timestamp_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            node_id,
            timestamp_ms,
            sequence,
            admin_url,
            raft_state,
            raft_term,
            last_log_index,
            last_log_term,
            current_leader,
            is_voter,
            startup_time,
        }
    }

    /// Serialize the heartbeat message to bytes for transmission.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        bincode::serialize(self)
            .map_err(|e| Error::SendFailed(format!("Failed to serialize heartbeat: {}", e)))
    }

    /// Deserialize a heartbeat message from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        bincode::deserialize(bytes)
            .map_err(|e| Error::SendFailed(format!("Failed to deserialize heartbeat: {}", e)))
    }
}
