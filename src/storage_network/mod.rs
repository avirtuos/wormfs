//! # StorageNetwork Component
//!
//! StorageNetwork provides the peer-to-peer networking layer for WormFS using libp2p.
//!
//! ## Responsibilities
//!
//! - Establishing and maintaining libp2p swarm connectivity between storage nodes
//! - Peer discovery and connection management
//! - Providing topic-based pub/sub channels for different subsystems
//! - Implementing secure, authenticated communication using libp2p's built-in encryption
//! - Managing peer identity validation (explicit peer IDs or auto-discovery mode)
//! - Handling network health monitoring and connection state
//! - Providing efficient direct chunk transfer streams between nodes
//! - Supporting multiple concurrent protocol handlers without deadlocks
//!
//! ## Architecture: Factory + Inner + Clone Pattern
//!
//! StorageNetwork uses a three-tier pattern to enable safe concurrent access:
//!
//! 1. **StorageNetworkFactory**: Creates and initializes the network instance
//! 2. **StorageNetworkInner**: Contains actual swarm and state (wrapped in Arc<RwLock>)
//! 3. **StorageNetwork**: Lightweight cloneable handle with command channel
//!
//! ### Structure
//!
//! ```ignore
//! struct StorageNetworkInner {
//!     swarm: RwLock<Swarm<WormFsBehaviour>>,
//!     peers: RwLock<HashMap<PeerId, PeerState>>,
//!     topics: RwLock<HashMap<String, TopicHandle>>,
//!     config: NetworkConfig,
//! }
//!
//! #[derive(Clone)]
//! pub struct StorageNetwork {
//!     inner: Arc<StorageNetworkInner>,
//!     event_tx: mpsc::UnboundedSender<NetworkCommand>,
//! }
//! ```
//!
//! ### Pattern Benefits
//!
//! 1. **OpenRaft Compatibility**: Raft can "own" a network instance while other components
//!    hold cloned instances
//! 2. **Concurrent Access**: Multiple threads can safely interact with the network
//! 3. **Interior Mutability**: RwLock and channel-based commands enable safe concurrent access
//! 4. **Event Loop Isolation**: The swarm runs in a dedicated event loop
//! 5. **Non-Blocking Operations**: Commands submitted via channel without lock acquisition
//!
//! ## Topic-Based Communication
//!
//! Components subscribe to topics and receive channels for communication:
//!
//! ```ignore
//! let (membership_tx, membership_rx) = network.join_topic("membership").await?;
//! let (filesystem_tx, filesystem_rx) = network.join_topic("filesystem").await?;
//! ```
//!
//! This allows clean separation of concerns while sharing a single network layer.
//!
//! ## Peer Validation
//!
//! StorageNetwork supports two peer validation modes:
//!
//! - **Explicit Peer ID**: Configuration specifies exact peer IDs; connections with
//!   mismatched IDs are rejected
//! - **Auto-ID Mode**: Accept any peer ID on first connection, store it durably,
//!   and enforce consistency on subsequent connections

pub mod types;

use async_trait::async_trait;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, RwLock};
pub use types::{
    Config, ConnectionState, Error, NetworkCommand, PeerId, PeerInfo, PeerState, TopicHandle,
    TopicMessage, TopicReceiver, TopicSender, ValidationResult,
};

/// Concrete implementation of StorageNetwork.
///
/// This is a lightweight cloneable handle that wraps the inner network state
/// and provides a command channel for non-blocking operations.
#[derive(Clone)]
#[allow(dead_code)] // TODO: Remove when implementation is complete
pub struct StorageNetworkHandle {
    /// Reference to inner network state
    inner: Arc<StorageNetworkInner>,

    /// Command channel for sending network commands to the event loop
    event_tx: tokio::sync::mpsc::UnboundedSender<NetworkCommand>,
}

/// Factory for creating StorageNetwork instances.
///
/// This factory is responsible for initializing the libp2p swarm and
/// creating the inner network state before returning a cloneable handle.
pub struct StorageNetworkFactory;

impl StorageNetworkFactory {
    /// Create a new StorageNetwork instance with the given configuration.
    ///
    /// This method initializes the libp2p swarm, sets up the event loop,
    /// and returns both the inner state and a cloneable network handle.
    ///
    /// # Arguments
    ///
    /// * `config` - Network configuration including peers, listen addresses, etc.
    ///
    /// # Returns
    ///
    /// A tuple of `(StorageNetworkInner, StorageNetworkHandle)` where:
    /// - `StorageNetworkInner` contains the actual swarm and should have `run()` called on it
    /// - `StorageNetworkHandle` is a cloneable handle for network operations
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Configuration is invalid
    /// - Swarm initialization fails
    /// - Event loop setup fails
    pub async fn create(
        _config: Config,
    ) -> Result<(StorageNetworkInner, StorageNetworkHandle), Error> {
        // TODO: Implement actual libp2p swarm initialization
        // For now, return placeholder error
        Err(Error::ConfigError(
            "StorageNetworkFactory not yet implemented".to_string(),
        ))
    }
}

/// Inner network state containing the actual libp2p swarm.
///
/// This struct holds all the mutable network state and is wrapped in Arc<RwLock>
/// to enable safe concurrent access from multiple components.
#[allow(dead_code)] // TODO: Remove when implementation is complete
pub struct StorageNetworkInner {
    /// libp2p swarm - protected by RwLock for concurrent access
    /// TODO: Replace () with actual Swarm<WormFsBehaviour> once libp2p behavior is implemented
    swarm: RwLock<()>,

    /// Active peer state tracking
    peers: RwLock<HashMap<PeerId, PeerState>>,

    /// Active topic subscriptions
    topics: RwLock<HashMap<String, TopicHandle>>,

    /// Network configuration
    config: Config,
}

impl StorageNetworkInner {
    /// Start the swarm event loop.
    ///
    /// This method must be called exactly once to start processing libp2p events
    /// and network commands. It runs indefinitely until shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if the event loop cannot be started or encounters a fatal error.
    pub async fn run(&self) -> Result<(), Error> {
        // TODO: Implement event loop that processes:
        // 1. libp2p swarm events
        // 2. NetworkCommand messages from the command channel
        // 3. Topic message routing
        Err(Error::EventLoopFailed(
            "Event loop not yet implemented".to_string(),
        ))
    }
}

/// StorageNetwork trait defines the interface for peer-to-peer networking.
///
/// Implementations provide libp2p-based networking with topic subscriptions,
/// direct streaming, and peer management.
///
/// Note: This trait is not mocked via automock due to Clone requirement.
/// Manual mock implementations can be created in test code as needed.
#[async_trait]
pub trait StorageNetwork: Send + Sync + Clone {
    /// Stream type for direct data transfer
    type Stream: Send + Sync;

    /// Create a new network instance with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Network configuration (listen addresses, peers, etc.)
    ///
    /// # Returns
    ///
    /// A new network instance that can be cloned and shared across components.
    async fn new(config: Config) -> Result<Self, Error>
    where
        Self: Sized;

    /// Start the swarm event loop.
    ///
    /// This method must be called exactly once to start the background event loop
    /// that processes libp2p events and handles network I/O.
    ///
    /// # Errors
    ///
    /// Returns an error if the event loop cannot be started or if it's already running.
    async fn run(&self) -> Result<(), Error>;

    /// Join a topic and get channels for pub/sub communication.
    ///
    /// This method subscribes to a gossipsub topic and returns sender/receiver
    /// channels for communication. Multiple components can join the same topic
    /// and will each receive all messages published to that topic.
    ///
    /// # Arguments
    ///
    /// * `topic_name` - Name of the topic to join (e.g., "filesystem", "membership")
    ///
    /// # Returns
    ///
    /// A tuple of `(sender, receiver)` channels for publishing and receiving messages.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic cannot be joined.
    async fn join_topic(&self, topic_name: &str) -> Result<(TopicSender, TopicReceiver), Error>;

    /// Send a message to a specific peer on a topic.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Target peer identifier
    /// * `topic` - Topic name
    /// * `message` - Message bytes to send
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Peer is not connected
    /// - Topic doesn't exist
    /// - Send operation fails
    async fn send_to_peer(
        &self,
        peer_id: &PeerId,
        topic: &str,
        message: Vec<u8>,
    ) -> Result<(), Error>;

    /// Broadcast a message to all peers on a topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name
    /// * `message` - Message bytes to broadcast
    ///
    /// # Errors
    ///
    /// Returns an error if the topic doesn't exist or broadcast fails.
    async fn broadcast(&self, topic: &str, message: Vec<u8>) -> Result<(), Error>;

    /// Open a direct stream to a peer for bulk data transfer.
    ///
    /// This method establishes a bidirectional stream for efficient large data
    /// transfers (e.g., chunk data) outside of the pub/sub system.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Target peer identifier
    /// * `protocol` - Protocol name (e.g., "/wormfs/chunk-transfer/1.0.0")
    ///
    /// # Returns
    ///
    /// A bidirectional stream for reading and writing data.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Peer is not connected
    /// - Protocol is not supported
    /// - Stream cannot be established
    async fn open_stream(&self, peer_id: &PeerId, protocol: &str) -> Result<Self::Stream, Error>;

    /// Get list of currently connected peers.
    ///
    /// # Returns
    ///
    /// A vector of peer information for all connected peers.
    fn get_connected_peers(&self) -> Vec<PeerInfo>;

    /// Get detailed information about a specific peer.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Peer identifier
    ///
    /// # Returns
    ///
    /// Peer information if the peer is known, `None` otherwise.
    fn get_peer_info(&self, peer_id: &PeerId) -> Option<PeerInfo>;

    /// Disconnect from a specific peer.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Identifier of peer to disconnect
    ///
    /// # Errors
    ///
    /// Returns an error if the peer is not connected or disconnection fails.
    async fn disconnect_peer(&self, peer_id: &PeerId) -> Result<(), Error>;

    /// Validate and potentially update stored peer ID for auto-ID mode.
    ///
    /// In auto-ID mode, this method is called on first connection to store
    /// the peer's ID, and on subsequent connections to validate consistency.
    ///
    /// # Arguments
    ///
    /// * `ip` - IP address of the peer
    /// * `peer_id` - Peer ID from libp2p handshake
    ///
    /// # Returns
    ///
    /// Validation result indicating if the peer was validated, newly discovered,
    /// or rejected due to ID mismatch.
    ///
    /// # Errors
    ///
    /// Returns an error if validation cannot be performed.
    async fn validate_peer_id(
        &self,
        ip: IpAddr,
        peer_id: PeerId,
    ) -> Result<ValidationResult, Error>;
}
