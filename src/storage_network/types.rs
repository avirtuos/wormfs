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
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// IP address of the peer
    pub ip_address: IpAddr,

    /// Peer ID configuration (explicit or auto-discover)
    pub peer_id: PeerIdConfig,
}

/// Peer ID configuration mode.
#[derive(Debug, Clone)]
pub enum PeerIdConfig {
    /// Exact peer ID required - reject connections with mismatched IDs
    Explicit(PeerId),

    /// Accept and store peer ID on first connection, enforce on subsequent connections
    AutoId,
}

/// Network configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Node ID for this node (used in heartbeat messages and identification)
    pub node_id: String,

    /// Listen addresses for libp2p
    pub listen_addresses: Vec<String>,

    /// Configured peers
    pub peers: Vec<PeerConfig>,

    /// Path to store discovered peer IDs (for auto-ID mode)
    pub peer_id_store_path: PathBuf,

    /// Maximum number of peers to maintain
    pub max_peers: usize,

    /// Maximum connections per peer
    pub max_connections_per_peer: usize,

    /// Connection timeout
    pub connection_timeout: Duration,

    /// Idle connection timeout
    pub idle_connection_timeout: Duration,

    /// Keep-alive interval
    pub keep_alive_interval: Duration,
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
#[derive(Debug)]
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
