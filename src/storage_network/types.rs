//! Common types for the StorageNetwork component.

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

/// Network configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Listen addresses for libp2p
    pub listen_addresses: Vec<String>,

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

    /// Path to store discovered peer IDs (for auto-ID mode)
    pub peer_id_store_path: Option<PathBuf>,
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
