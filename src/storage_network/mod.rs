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

pub mod behaviour;
pub mod implementation;
pub mod peer_id_store;
pub mod types;

use async_trait::async_trait;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
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
    /// Reference to inner network state (contains swarm, peers, topics)
    pub(crate) inner: Arc<implementation::InnerState>,

    /// Command channel for sending network commands to the event loop
    pub(crate) event_tx: tokio::sync::mpsc::UnboundedSender<NetworkCommand>,
}

// Safety: StorageNetworkHandle is Send + Sync because:
// - Arc<InnerState> is Send + Sync (shared ownership)
// - UnboundedSender is Send + Sync
// - The Swarm inside InnerState is protected by RwLock which provides interior mutability
unsafe impl Send for StorageNetworkHandle {}

unsafe impl Sync for StorageNetworkHandle {}

impl StorageNetworkHandle {
    /// Join a topic and get channels for pub/sub communication.
    ///
    /// # Arguments
    ///
    /// * `topic_name` - Name of the topic to join
    ///
    /// # Returns
    ///
    /// A tuple of `(sender, receiver)` channels for publishing and receiving messages.
    pub async fn join_topic(
        &self,
        topic_name: &str,
    ) -> Result<(TopicSender, TopicReceiver), Error> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        self.event_tx
            .send(NetworkCommand::JoinTopic {
                name: topic_name.to_string(),
                response: response_tx,
            })
            .map_err(|_| Error::EventLoopFailed("Event loop is not running".to_string()))?;

        let topic_handle = response_rx
            .await
            .map_err(|_| Error::EventLoopFailed("Event loop dropped response".to_string()))??;

        Ok((topic_handle.tx, topic_handle.rx))
    }

    /// Broadcast a message to all peers on a topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name
    /// * `message` - Message bytes to broadcast
    pub async fn broadcast(&self, topic: &str, message: Vec<u8>) -> Result<(), Error> {
        self.event_tx
            .send(NetworkCommand::Broadcast {
                topic: topic.to_string(),
                message,
            })
            .map_err(|_| Error::EventLoopFailed("Event loop is not running".to_string()))?;

        Ok(())
    }

    /// Send a message to a specific peer on a topic.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Target peer identifier
    /// * `topic` - Topic name
    /// * `message` - Message bytes to send
    pub async fn send_to_peer(
        &self,
        peer_id: &PeerId,
        topic: &str,
        message: Vec<u8>,
    ) -> Result<(), Error> {
        self.event_tx
            .send(NetworkCommand::SendToPeer {
                peer_id: peer_id.clone(),
                topic: topic.to_string(),
                message,
            })
            .map_err(|_| Error::EventLoopFailed("Event loop is not running".to_string()))?;

        Ok(())
    }

    /// Get list of currently connected peers.
    pub async fn get_connected_peers(&self) -> Vec<PeerInfo> {
        let peers = self.inner.peers.read().await;

        peers
            .values()
            .filter(|state| state.connection_state == ConnectionState::Connected)
            .map(|state| PeerInfo {
                peer_id: state.peer_id.clone(),
                addresses: state
                    .addresses
                    .iter()
                    .filter_map(|s| s.parse().ok())
                    .collect(),
                state: state.connection_state,
                connected_since: Some(state.last_seen),
                protocols: vec![], // Day 3: Track protocols
                rtt: None,         // Day 3: Track RTT
                last_heartbeat: state.last_heartbeat,
            })
            .collect()
    }

    /// Get detailed information about a specific peer.
    pub async fn get_peer_info(&self, peer_id: &PeerId) -> Option<PeerInfo> {
        let peers = self.inner.peers.read().await;

        peers.get(peer_id).map(|state| PeerInfo {
            peer_id: state.peer_id.clone(),
            addresses: state
                .addresses
                .iter()
                .filter_map(|s| s.parse().ok())
                .collect(),
            state: state.connection_state,
            connected_since: Some(state.last_seen),
            protocols: vec![],
            rtt: None,
            last_heartbeat: state.last_heartbeat,
        })
    }

    /// Send a request to a specific peer and await response (request-response protocol).
    ///
    /// This method uses libp2p's request-response protocol for direct peer-to-peer RPC.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Target peer identifier
    /// * `protocol` - Protocol name (e.g., "/wormfs/rpc/1.0.0")
    /// * `request` - Request data bytes
    ///
    /// # Returns
    ///
    /// Response data bytes from the peer
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Peer is not connected
    /// - Request times out
    /// - Request fails for any reason
    pub async fn send_request(
        &self,
        peer_id: &PeerId,
        protocol: &str,
        request: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        self.event_tx
            .send(NetworkCommand::SendRequest {
                peer_id: peer_id.clone(),
                protocol: protocol.to_string(),
                request,
                response: response_tx,
            })
            .map_err(|_| Error::EventLoopFailed("Event loop is not running".to_string()))?;

        response_rx
            .await
            .map_err(|_| Error::EventLoopFailed("Event loop dropped response".to_string()))?
    }

    /// Set the metrics service for tracking network operations.
    ///
    /// # Arguments
    ///
    /// * `metrics` - MetricService implementation for recording metrics
    pub async fn set_metrics(
        &self,
        metrics: std::sync::Arc<crate::metric_service::MetricServiceImpl>,
    ) {
        *self.inner.metrics.write().await = Some(metrics);
    }

    /// Disconnect from a specific peer.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Identifier of peer to disconnect
    ///
    /// # Errors
    ///
    /// Returns an error if the peer is not connected or disconnection fails.
    pub async fn disconnect_peer(&self, peer_id: &PeerId) -> Result<(), Error> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        self.event_tx
            .send(NetworkCommand::DisconnectPeer {
                peer_id: peer_id.clone(),
                response: response_tx,
            })
            .map_err(|_| Error::EventLoopFailed("Event loop is not running".to_string()))?;

        response_rx
            .await
            .map_err(|_| Error::EventLoopFailed("Event loop dropped response".to_string()))?
    }

    /// Open a direct stream to a peer for bulk data transfer.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Target peer identifier
    /// * `protocol` - Protocol name
    ///
    /// # Errors
    ///
    /// Returns an error indicating streams are not yet implemented (Phase 3+).
    pub async fn open_stream(&self, _peer_id: &PeerId, _protocol: &str) -> Result<(), Error> {
        Err(Error::StreamFailed(
            "Stream opening not yet implemented (Phase 3+)".to_string(),
        ))
    }

    /// Validate and potentially update stored peer ID for auto-ID mode.
    ///
    /// This method validates a peer's ID against configuration. In auto-ID mode,
    /// it learns peer IDs on first connection and enforces them on subsequent connections.
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
    pub async fn validate_peer_id(
        &self,
        ip: IpAddr,
        peer_id: PeerId,
    ) -> Result<ValidationResult, Error> {
        // Access the inner's validate_peer_id method
        self.inner.validate_peer_id(ip, peer_id).await
    }

    /// Dial all configured peers with random jitter to avoid simultaneous connection attempts.
    ///
    /// This method reads the peer list from the configuration and sends dial commands
    /// with random delays (250-500ms) between each dial to prevent connection conflicts
    /// when multiple nodes start simultaneously.
    ///
    /// This should typically be called after starting the event loop.
    pub async fn dial_configured_peers(&self) -> Result<(), Error> {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        for peer_config in &self.inner.config.peers {
            // Add random jitter between 250ms and 500ms before each dial
            let jitter_ms = rng.gen_range(250..=500);
            tokio::time::sleep(Duration::from_millis(jitter_ms)).await;

            // Send dial command to event loop
            self.event_tx
                .send(NetworkCommand::DialPeer {
                    multiaddr: peer_config.multiaddr.clone(),
                })
                .map_err(|_| Error::EventLoopFailed("Event loop is not running".to_string()))?;
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        self.event_tx
            .send(NetworkCommand::Shutdown {
                response: response_tx,
            })
            .map_err(|_| Error::EventLoopFailed("Event loop is not running".to_string()))?;

        response_rx
            .await
            .map_err(|_| Error::EventLoopFailed("Event loop dropped response".to_string()))?
    }
}

/// Implementation of the StorageNetwork trait for StorageNetworkHandle.
#[async_trait]
impl StorageNetwork for StorageNetworkHandle {
    /// Stream type for direct data transfer.
    /// Currently using () as placeholder until Phase 3+ implements streaming.
    type Stream = ();

    /// Create a new network instance with the given configuration.
    ///
    /// **Important**: This method returns the handle immediately, but the network
    /// event loop must be started separately by calling `run()` on the returned
    /// `StorageNetworkInner` from `StorageNetworkFactory::create()`.
    ///
    /// Due to libp2p's architecture, the event loop cannot be automatically spawned
    /// here. Users should instead use `StorageNetworkFactory::create()` which returns
    /// both the `Inner` (for running) and `Handle` (for operations).
    async fn new(_config: Config) -> Result<Self, Error> {
        // Cannot use this method with the current architecture
        // Use StorageNetworkFactory::create() instead
        Err(Error::EventLoopFailed(
            "Use StorageNetworkFactory::create() to create network instances".to_string(),
        ))
    }

    /// Start the swarm event loop.
    ///
    /// This method is not applicable for `StorageNetworkHandle` because the event
    /// loop runs on `StorageNetworkInner`. Use `StorageNetworkFactory::create()`
    /// to get both components, then call `run()` on the Inner.
    async fn run(&self) -> Result<(), Error> {
        Err(Error::EventLoopFailed(
            "Call run() on StorageNetworkInner, not on StorageNetworkHandle".to_string(),
        ))
    }

    async fn join_topic(&self, topic_name: &str) -> Result<(TopicSender, TopicReceiver), Error> {
        StorageNetworkHandle::join_topic(self, topic_name).await
    }

    async fn send_to_peer(
        &self,
        peer_id: &PeerId,
        topic: &str,
        message: Vec<u8>,
    ) -> Result<(), Error> {
        StorageNetworkHandle::send_to_peer(self, peer_id, topic, message).await
    }

    async fn broadcast(&self, topic: &str, message: Vec<u8>) -> Result<(), Error> {
        StorageNetworkHandle::broadcast(self, topic, message).await
    }

    async fn open_stream(&self, peer_id: &PeerId, protocol: &str) -> Result<Self::Stream, Error> {
        StorageNetworkHandle::open_stream(self, peer_id, protocol).await
    }

    async fn get_connected_peers(&self) -> Vec<PeerInfo> {
        StorageNetworkHandle::get_connected_peers(self).await
    }

    async fn get_peer_info(&self, peer_id: &PeerId) -> Option<PeerInfo> {
        StorageNetworkHandle::get_peer_info(self, peer_id).await
    }

    async fn disconnect_peer(&self, peer_id: &PeerId) -> Result<(), Error> {
        StorageNetworkHandle::disconnect_peer(self, peer_id).await
    }

    async fn validate_peer_id(
        &self,
        ip: IpAddr,
        peer_id: PeerId,
    ) -> Result<ValidationResult, Error> {
        StorageNetworkHandle::validate_peer_id(self, ip, peer_id).await
    }
}

/// Factory for creating StorageNetwork instances.
///
/// This factory is responsible for initializing the libp2p swarm and
/// creating the inner network state before returning a cloneable handle.
pub struct StorageNetworkFactory;

// StorageNetworkFactory implementation is in implementation.rs

/// Inner network state containing the actual libp2p swarm.
///
/// This struct holds all the mutable network state and is wrapped in Arc
/// to enable safe concurrent access from multiple components.
#[allow(dead_code)] // TODO: Remove when implementation is complete
pub struct StorageNetworkInner {
    /// Reference to the inner state containing swarm and peer tracking.
    /// The actual InnerState is defined in implementation.rs.
    pub(crate) inner: Arc<implementation::InnerState>,
}

// StorageNetworkInner implementation is in implementation.rs

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
    async fn get_connected_peers(&self) -> Vec<PeerInfo>;

    /// Get detailed information about a specific peer.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Peer identifier
    ///
    /// # Returns
    ///
    /// Peer information if the peer is known, `None` otherwise.
    async fn get_peer_info(&self, peer_id: &PeerId) -> Option<PeerInfo>;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metric_service::MetricService;
    use std::time::Duration;

    /// Helper to create a test config
    fn test_config(node_id: &str) -> Config {
        Config {
            node_id: node_id.to_string(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![],
            peer_id_store_path: std::env::temp_dir().join(format!("test_{}.json", node_id)),
            max_peers: 10,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        }
    }

    #[tokio::test]
    async fn test_send_to_peer_command_queuing() {
        let config = test_config("send_to_peer_test");
        let (_inner, handle) = crate::storage_network::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        let peer_id = PeerId::new(vec![1, 2, 3, 4, 5]);
        let result = handle
            .send_to_peer(&peer_id, "test-topic", b"hello".to_vec())
            .await;

        // Should succeed at queuing the command even though event loop isn't running
        assert!(
            result.is_ok(),
            "Should be able to queue send_to_peer command"
        );
    }

    #[tokio::test]
    async fn test_join_topic_returns_channels() {
        let config = test_config("join_topic_test");
        let (_inner, handle) = crate::storage_network::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        // Join topic should timeout since event loop isn't running, but we can test the API
        let result =
            tokio::time::timeout(Duration::from_millis(100), handle.join_topic("test-topic")).await;

        // Should timeout waiting for event loop response
        assert!(result.is_err(), "Should timeout without running event loop");
    }

    #[tokio::test]
    async fn test_disconnect_peer_command_queuing() {
        let config = test_config("disconnect_test");
        let (_inner, handle) = crate::storage_network::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        let peer_id = PeerId::new(vec![1, 2, 3]);

        // Should timeout since event loop not running
        let result =
            tokio::time::timeout(Duration::from_millis(50), handle.disconnect_peer(&peer_id)).await;

        assert!(result.is_err(), "Should timeout waiting for event loop");
    }

    #[tokio::test]
    async fn test_open_stream_not_implemented() {
        let config = test_config("open_stream_test");
        let (_inner, handle) = crate::storage_network::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        let peer_id = PeerId::new(vec![4, 5, 6]);

        // open_stream should return error indicating it's not implemented
        let result = handle.open_stream(&peer_id, "/test/protocol").await;

        assert!(result.is_err(), "open_stream should return error");
        if let Err(e) = result {
            assert!(
                e.to_string().contains("not yet implemented")
                    || e.to_string().contains("not implemented"),
                "Error should indicate not implemented: {}",
                e
            );
        }
    }

    #[tokio::test]
    async fn test_set_metrics_updates_state() {
        let config = test_config("set_metrics_test");
        let (_inner, handle) = crate::storage_network::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        // Create a dummy metrics service
        let metrics = std::sync::Arc::new(
            crate::metric_service::MetricServiceImpl::new(crate::metric_service::Config::default())
                .expect("Should create metrics service"),
        );

        // Set metrics
        handle.set_metrics(metrics.clone()).await;

        // Verify metrics was stored (by checking internal state)
        let stored_metrics = handle.inner.metrics.read().await;
        assert!(stored_metrics.is_some(), "Metrics should be stored");
    }

    #[tokio::test]
    async fn test_get_connected_peers_empty_initially() {
        let config = test_config("get_peers_test");
        let (_inner, handle) = crate::storage_network::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        let peers = handle.get_connected_peers().await;
        assert_eq!(peers.len(), 0, "Should start with no peers");
    }

    #[tokio::test]
    async fn test_get_peer_info_nonexistent_peer() {
        let config = test_config("get_peer_info_test");
        let (_inner, handle) = crate::storage_network::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        let peer_id = PeerId::new(vec![9, 9, 9]);
        let info = handle.get_peer_info(&peer_id).await;

        assert!(info.is_none(), "Nonexistent peer should return None");
    }
}
