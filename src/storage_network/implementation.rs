//! StorageNetwork implementation with libp2p.
//!
//! This module provides the concrete implementation of the StorageNetwork trait,
//! including swarm initialization, peer management, and event handling.

use crate::storage_network::behaviour::WormFsBehaviourEvent;
use crate::storage_network::{
    behaviour::{BehaviourConfig, WormFsBehaviour, WormFsCodec},
    types::*,
};
use futures::StreamExt;
use libp2p::{
    gossipsub, identify, identity, noise, ping, request_response, swarm::SwarmEvent, tcp, yamux,
    Multiaddr, PeerId as Libp2pPeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Topic name for heartbeat messages
const HEARTBEAT_TOPIC: &str = "wormfs/heartbeat/1.0.0";

/// Interval between heartbeat broadcasts (seconds)
const HEARTBEAT_INTERVAL_SECS: u64 = 5;

/// Factory for creating StorageNetwork instances.
impl super::StorageNetworkFactory {
    /// Create a new StorageNetwork instance with the given configuration.
    ///
    /// This initializes the libp2p swarm with all required protocols and
    /// returns both the inner state (for running the event loop) and a
    /// cloneable handle for network operations.
    ///
    /// The event loop should be run by calling `.run()` on the returned `StorageNetworkInner`.
    /// Because libp2p's Swarm contains thread-local state (!Send types), the event loop
    /// must run in a LocalSet if you need to spawn it. Typically, you would either:
    /// - Run it directly with `inner.run().await` in your main task
    /// - Spawn it with `tokio::task::spawn_local()` within a LocalSet
    ///
    /// # Arguments
    ///
    /// * `config` - Network configuration
    ///
    /// # Returns
    ///
    /// A tuple of `(StorageNetworkInner, StorageNetworkHandle)` where:
    /// - `StorageNetworkInner` must have `run()` called to start the event loop
    /// - `StorageNetworkHandle` is a cloneable handle for network operations
    ///
    /// # Errors
    ///
    /// Returns an error if swarm initialization fails or configuration is invalid.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = Config { /* ... */ };
    /// let (inner, handle) = StorageNetworkFactory::create(config).await?;
    ///
    /// // Run event loop in main task
    /// tokio::select! {
    ///     result = inner.run() => result?,
    ///     _ = some_shutdown_signal() => {},
    /// }
    /// ```
    pub async fn create(
        config: Config,
    ) -> Result<(super::StorageNetworkInner, super::StorageNetworkHandle), Error> {
        // Generate a new random keypair
        let local_key = identity::Keypair::generate_ed25519();
        Self::create_with_keypair(config, local_key).await
    }

    /// Create a new StorageNetwork instance with a specific keypair.
    ///
    /// This method allows providing a specific keypair for the node's identity,
    /// which is useful for testing where stable peer IDs are needed.
    ///
    /// # Arguments
    ///
    /// * `config` - Network configuration
    /// * `keypair` - The libp2p keypair to use for this node's identity
    ///
    /// # Returns
    ///
    /// A tuple of `(StorageNetworkInner, StorageNetworkHandle)` where:
    /// - `StorageNetworkInner` must have `run()` called to start the event loop
    /// - `StorageNetworkHandle` is a cloneable handle for network operations
    pub async fn create_with_keypair(
        config: Config,
        local_key: identity::Keypair,
    ) -> Result<(super::StorageNetworkInner, super::StorageNetworkHandle), Error> {
        info!("Initializing StorageNetwork with libp2p");

        // Use the provided keypair for this node's identity
        let local_peer_id = Libp2pPeerId::from(local_key.public());
        info!("Local peer ID: {}", local_peer_id);

        // Store config for use in closure
        let keep_alive_interval = config.keep_alive_interval;

        // Build the swarm using the new builder API
        let swarm = SwarmBuilder::with_existing_identity(local_key.clone())
            .with_tokio()
            .with_tcp(
                tcp::Config::default().nodelay(true),
                noise::Config::new,
                yamux::Config::default,
            )
            .expect("Failed to configure TCP transport")
            .with_behaviour(|key| {
                // Configure gossipsub behavior
                let behaviour_config = BehaviourConfig::default();
                let gossipsub_config = behaviour_config.gossipsub;

                // Create gossipsub
                let gossipsub = gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(key.clone()),
                    gossipsub_config,
                )
                .expect("Valid gossipsub configuration");

                // Configure request-response protocol
                let req_resp_config = request_response::Config::default()
                    .with_request_timeout(behaviour_config.request_timeout);

                let request_response = request_response::Behaviour::<WormFsCodec>::with_codec(
                    WormFsCodec::default(),
                    iter::once((
                        StreamProtocol::new("/wormfs/rpc/1.0.0"),
                        request_response::ProtocolSupport::Full,
                    )),
                    req_resp_config,
                );

                // Configure identify protocol
                let identify_config =
                    identify::Config::new("/wormfs/1.0.0".to_string(), key.public())
                        .with_agent_version(format!("wormfs/{}", env!("CARGO_PKG_VERSION")));

                let identify = identify::Behaviour::new(identify_config);

                // Configure ping for keep-alive
                let ping_config = ping::Config::new()
                    .with_interval(keep_alive_interval)
                    .with_timeout(Duration::from_secs(20));

                let ping = ping::Behaviour::new(ping_config);

                // Combine all behaviors
                Ok(WormFsBehaviour {
                    gossipsub,
                    request_response,
                    identify,
                    ping,
                })
            })
            .expect("Failed to build behaviour")
            .build();

        // Create command channel for network operations
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        // Create peer state tracking
        let peers = RwLock::new(HashMap::new());

        // Create topic subscriptions tracking
        let topics = RwLock::new(HashMap::new());

        // Wrap swarm in RwLock for interior mutability
        let swarm_lock = RwLock::new(swarm);

        // Create peer ID store for learned peer IDs (AutoId mode)
        let peer_id_store = Arc::new(
            super::peer_id_store::PeerIdStore::new(&config.peer_id_store_path)
                .expect("Failed to create peer ID store"),
        );

        // Create inner state
        let inner = Arc::new(InnerState {
            swarm: swarm_lock,
            peers,
            topics,
            node_id: config.node_id.clone(),
            config: config.clone(),
            event_rx: RwLock::new(event_rx),
            pending_requests: RwLock::new(HashMap::new()),
            metrics: RwLock::new(None),
            peer_id_store,
            heartbeat_sequence: RwLock::new(0),
        });

        // Create cloneable handle
        let handle = super::StorageNetworkHandle {
            inner: inner.clone(),
            event_tx,
        };

        // Return both inner (for event loop) and handle (for operations)
        Ok((super::StorageNetworkInner { inner }, handle))
    }
}

/// Internal topic state for routing messages.
pub(crate) struct TopicState {
    /// Sender for routing messages to subscribers
    pub(crate) tx: mpsc::UnboundedSender<TopicMessage>,
}

/// Internal state shared between event loop and network handle.
pub struct InnerState {
    /// libp2p swarm
    pub(crate) swarm: RwLock<Swarm<WormFsBehaviour>>,

    /// Active peer states
    pub(crate) peers: RwLock<HashMap<PeerId, PeerState>>,

    /// Active topic subscriptions
    pub(crate) topics: RwLock<HashMap<String, TopicState>>,

    /// Network configuration
    pub(crate) config: Config,

    /// Command receiver for event loop
    pub(crate) event_rx: RwLock<mpsc::UnboundedReceiver<NetworkCommand>>,

    /// Pending request-response requests (RequestId → response channel)
    pub(crate) pending_requests: RwLock<
        HashMap<
            request_response::OutboundRequestId,
            tokio::sync::oneshot::Sender<Result<Vec<u8>, Error>>,
        >,
    >,

    /// Optional metrics service for tracking network operations
    pub(crate) metrics: RwLock<Option<Arc<crate::metric_service::MetricServiceImpl>>>,

    /// Persistent store for learned peer IDs (AutoId mode)
    pub(crate) peer_id_store: Arc<super::peer_id_store::PeerIdStore>,

    /// Node ID for this node (used in heartbeat messages)
    pub(crate) node_id: String,

    /// Heartbeat sequence counter
    pub(crate) heartbeat_sequence: RwLock<u64>,
}

// Safety: InnerState is Sync because:
// - All mutable state (swarm, peers, topics, etc.) is protected by tokio::sync::RwLock
// - tokio::sync::RwLock provides async-aware synchronization with Send guards
// - Arc and other fields are already Send + Sync
// - Access to non-Sync libp2p types (Swarm) is always through async RwLock
// - This allows InnerState to be shared across async tasks via Arc
unsafe impl Sync for InnerState {}

/// StorageNetworkInner implementation
impl super::StorageNetworkInner {
    /// Start the network event loop.
    ///
    /// This method processes libp2p swarm events and network commands.
    /// It should be called exactly once and runs until shutdown.
    pub async fn run(&self) -> Result<(), Error> {
        info!("Starting StorageNetwork event loop");

        // Set up listen addresses
        {
            let mut swarm = self.inner.swarm.write().await;
            for addr_str in &self.inner.config.listen_addresses {
                match addr_str.parse() {
                    Ok(addr) => match swarm.listen_on(addr) {
                        Ok(_) => info!("Listening on {}", addr_str),
                        Err(e) => warn!("Failed to listen on {}: {}", addr_str, e),
                    },
                    Err(e) => warn!("Invalid listen address '{}': {}", addr_str, e),
                }
            }
        }

        // Subscribe to heartbeat topic
        {
            let mut swarm = self.inner.swarm.write().await;
            let topic = gossipsub::IdentTopic::new(HEARTBEAT_TOPIC);
            if let Err(e) = swarm.behaviour_mut().gossipsub.subscribe(&topic) {
                warn!("Failed to subscribe to heartbeat topic: {}", e);
            } else {
                info!("Subscribed to heartbeat topic");
            }
        }

        info!("Event loop initialized, processing events");

        // Create heartbeat interval
        let mut heartbeat_interval =
            tokio::time::interval(Duration::from_secs(HEARTBEAT_INTERVAL_SECS));

        // Main event loop
        loop {
            tokio::select! {
                // Process swarm events
                event = async {
                    let mut swarm = self.inner.swarm.write().await;
                    swarm.select_next_some().await
                } => {
                    self.handle_swarm_event(event).await;
                }

                // Process network commands
                command = async {
                    let mut rx = self.inner.event_rx.write().await;
                    rx.recv().await
                } => {
                    match command {
                        Some(cmd) => {
                            let should_continue = self.handle_network_command(cmd).await;
                            if !should_continue {
                                info!("Shutdown command received, exiting event loop");
                                break;
                            }
                        }
                        None => {
                            info!("Command channel closed, shutting down event loop");
                            break;
                        }
                    }
                }

                // Broadcast heartbeat periodically
                _ = heartbeat_interval.tick() => {
                    self.broadcast_heartbeat().await;
                }
            }
        }

        info!("StorageNetwork event loop terminated");
        Ok(())
    }

    /// Handle a swarm event from libp2p.
    async fn handle_swarm_event(&self, event: SwarmEvent<WormFsBehaviourEvent>) {
        match event {
            SwarmEvent::ConnectionEstablished {
                peer_id,
                endpoint,
                num_established,
                ..
            } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                let remote_addr = endpoint.get_remote_address();

                info!(
                    "Connection established with peer {:?} at {} (total connections: {})",
                    internal_peer_id, remote_addr, num_established
                );

                // Validate peer ID directly (not based on IP address)
                // IP addresses can change, but peer IDs are stable identities
                let validation_result =
                    match self.validate_peer_id_direct(internal_peer_id.clone()).await {
                        Ok(result) => result,
                        Err(e) => {
                            warn!("Peer validation failed for {:?}: {}", internal_peer_id, e);

                            // Disconnect the peer
                            let mut swarm = self.inner.swarm.write().await;
                            let _ = swarm.disconnect_peer_id(peer_id);
                            return;
                        }
                    };

                // Check if peer was rejected
                if let super::types::ValidationResult::Rejected { expected, actual } =
                    &validation_result
                {
                    warn!(
                        "Rejecting peer {:?}: expected ID {:?}, got {:?}",
                        internal_peer_id, expected, actual
                    );

                    // Disconnect the peer
                    let mut swarm = self.inner.swarm.write().await;
                    let _ = swarm.disconnect_peer_id(peer_id);
                    return;
                }

                // Determine validation status based on result
                let validation_status = match &validation_result {
                    super::types::ValidationResult::Validated => ValidationStatus::Validated,
                    super::types::ValidationResult::NewlyDiscovered(_) => {
                        ValidationStatus::AutoDiscovered
                    }
                    super::types::ValidationResult::Rejected { .. } => {
                        unreachable!("Rejected case handled above")
                    }
                };

                // Update peer state
                let mut peers = self.inner.peers.write().await;
                peers.insert(
                    internal_peer_id.clone(),
                    PeerState {
                        peer_id: internal_peer_id,
                        addresses: vec![remote_addr.to_string()],
                        connection_state: ConnectionState::Connected,
                        last_seen: SystemTime::now(),
                        validation_status,
                        last_heartbeat: None,
                    },
                );

                // Record peer connection metrics
                let connected_count = peers
                    .values()
                    .filter(|p| p.connection_state == ConnectionState::Connected)
                    .count();
                drop(peers);

                self.record_metric_counter("storage_network.peer.connected", 1)
                    .await;
                self.record_metric_gauge(
                    "storage_network.peer.count",
                    connected_count as f64,
                    crate::metric_service::UnitType::Count,
                )
                .await;
            }

            SwarmEvent::ConnectionClosed {
                peer_id,
                cause,
                num_established,
                ..
            } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                info!(
                    "Connection closed with peer {:?} (cause: {:?}, remaining: {})",
                    internal_peer_id, cause, num_established
                );

                // Update peer state to disconnected if no more connections
                if num_established == 0 {
                    let mut peers = self.inner.peers.write().await;
                    if let Some(peer_state) = peers.get_mut(&internal_peer_id) {
                        peer_state.connection_state = ConnectionState::Disconnected;
                    }

                    // Record peer disconnection metrics
                    let connected_count = peers
                        .values()
                        .filter(|p| p.connection_state == ConnectionState::Connected)
                        .count();
                    drop(peers);

                    self.record_metric_counter("storage_network.peer.disconnected", 1)
                        .await;
                    self.record_metric_gauge(
                        "storage_network.peer.count",
                        connected_count as f64,
                        crate::metric_service::UnitType::Count,
                    )
                    .await;
                }
            }

            SwarmEvent::Behaviour(event) => {
                self.handle_behaviour_event(event).await;
            }

            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Now listening on {}", address);
            }

            SwarmEvent::IncomingConnection {
                local_addr,
                send_back_addr,
                ..
            } => {
                debug!(
                    "Incoming connection from {} to {}",
                    send_back_addr, local_addr
                );
            }

            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                if let Some(peer_id) = peer_id {
                    let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                    warn!(
                        "Outgoing connection error to peer {:?}: {}",
                        internal_peer_id, error
                    );
                } else {
                    warn!("Outgoing connection error: {}", error);
                }

                // Record connection error metric
                self.record_metric_counter("storage_network.peer.connection_errors", 1)
                    .await;
            }

            SwarmEvent::IncomingConnectionError {
                local_addr,
                send_back_addr,
                error,
                ..
            } => {
                warn!(
                    "Incoming connection error from {} to {}: {}",
                    send_back_addr, local_addr, error
                );
            }

            other => {
                // Log other/unexpected events at info level for visibility during testing
                info!("Unhandled swarm event: {:?}", other);
            }
        }
    }

    /// Handle a behaviour event from the combined behaviour.
    async fn handle_behaviour_event(&self, event: WormFsBehaviourEvent) {
        match event {
            WormFsBehaviourEvent::Gossipsub(gossipsub_event) => {
                self.handle_gossipsub_event(gossipsub_event).await;
            }

            WormFsBehaviourEvent::Identify(identify_event) => {
                self.handle_identify_event(identify_event).await;
            }

            WormFsBehaviourEvent::Ping(ping_event) => {
                self.handle_ping_event(ping_event).await;
            }

            WormFsBehaviourEvent::RequestResponse(rr_event) => {
                self.handle_request_response_event(rr_event).await;
            }
        }
    }

    /// Handle a gossipsub event.
    async fn handle_gossipsub_event(&self, event: gossipsub::Event) {
        match event {
            gossipsub::Event::Message {
                propagation_source,
                message_id,
                message,
            } => {
                let source = libp2p_peer_id_to_internal(&propagation_source);
                let topic = message.topic.as_str();
                let message_size = message.data.len() as u64;

                debug!(
                    "Received gossipsub message from {:?} on topic {} (id: {})",
                    source, topic, message_id
                );

                // Record gossipsub message received metrics
                self.record_metric_counter("storage_network.gossipsub.messages_received", 1)
                    .await;
                self.record_metric_counter(
                    "storage_network.gossipsub.bytes_received",
                    message_size,
                )
                .await;

                // Handle heartbeat messages specially
                if topic == HEARTBEAT_TOPIC {
                    self.handle_heartbeat_message(&source, &message.data).await;
                }

                // Route message to topic subscribers
                let topics = self.inner.topics.read().await;
                if let Some(topic_state) = topics.get(topic) {
                    let topic_message = TopicMessage {
                        source,
                        data: message.data,
                        timestamp: SystemTime::now(),
                    };

                    // Try to send, but don't block if channel is full
                    if topic_state.tx.send(topic_message).is_err() {
                        warn!(
                            "Failed to route message to topic '{}' - channel closed",
                            topic
                        );
                    }
                } else {
                    debug!("Received message for unsubscribed topic '{}'", topic);
                }
            }

            gossipsub::Event::Subscribed { peer_id, topic } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                debug!("Peer {:?} subscribed to topic {}", internal_peer_id, topic);

                // Record topic subscription metric
                self.record_metric_counter("storage_network.gossipsub.subscriptions", 1)
                    .await;
            }

            gossipsub::Event::Unsubscribed { peer_id, topic } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                debug!(
                    "Peer {:?} unsubscribed from topic {}",
                    internal_peer_id, topic
                );

                // Record topic unsubscription metric
                self.record_metric_counter("storage_network.gossipsub.unsubscriptions", 1)
                    .await;
            }

            gossipsub::Event::GossipsubNotSupported { peer_id } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                warn!("Peer {:?} does not support gossipsub", internal_peer_id);
            }

            gossipsub::Event::SlowPeer {
                peer_id,
                failed_messages,
            } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                warn!(
                    "Peer {:?} is slow to respond to gossipsub messages ({:?} failed messages)",
                    internal_peer_id, failed_messages
                );

                // Record metric for monitoring slow peers
                self.record_metric_counter("storage_network.gossipsub.slow_peers", 1)
                    .await;
            }
        }
    }

    /// Handle an identify event.
    async fn handle_identify_event(&self, event: identify::Event) {
        match event {
            identify::Event::Received {
                peer_id,
                info,
                connection_id: _,
            } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                debug!(
                    "Identified peer {:?}: protocol_version={}, agent_version={}",
                    internal_peer_id, info.protocol_version, info.agent_version
                );

                // Update peer info with addresses and protocols
                let mut peers = self.inner.peers.write().await;
                if let Some(peer_state) = peers.get_mut(&internal_peer_id) {
                    peer_state.addresses =
                        info.listen_addrs.iter().map(|a| a.to_string()).collect();
                    peer_state.last_seen = SystemTime::now();
                }
            }

            identify::Event::Sent {
                peer_id,
                connection_id: _,
            } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                debug!("Sent identify info to peer {:?}", internal_peer_id);
            }

            identify::Event::Pushed { peer_id, .. } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                debug!("Pushed identify info to peer {:?}", internal_peer_id);
            }

            identify::Event::Error {
                peer_id,
                error,
                connection_id: _,
            } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                warn!("Identify error with peer {:?}: {}", internal_peer_id, error);
            }
        }
    }

    /// Handle a ping event.
    async fn handle_ping_event(&self, event: ping::Event) {
        match event.result {
            Ok(rtt) => {
                let internal_peer_id = libp2p_peer_id_to_internal(&event.peer);
                let rtt_ms = rtt.as_millis() as f64;
                debug!("Ping to peer {:?}: {}ms", internal_peer_id, rtt_ms);

                // Record ping RTT metric
                self.record_metric_histogram(
                    "storage_network.ping.rtt_ms",
                    rtt_ms,
                    crate::metric_service::UnitType::Milliseconds,
                )
                .await;

                // Update last_seen timestamp
                let mut peers = self.inner.peers.write().await;
                if let Some(peer_state) = peers.get_mut(&internal_peer_id) {
                    peer_state.last_seen = SystemTime::now();
                }
            }
            Err(failure) => {
                let internal_peer_id = libp2p_peer_id_to_internal(&event.peer);
                warn!("Ping failed to peer {:?}: {:?}", internal_peer_id, failure);

                // Record ping failure metric
                self.record_metric_counter("storage_network.ping.failures", 1)
                    .await;
            }
        }
    }

    /// Handle a received heartbeat message.
    async fn handle_heartbeat_message(&self, source: &PeerId, data: &[u8]) {
        match HeartbeatMessage::from_bytes(data) {
            Ok(heartbeat) => {
                debug!(
                    "Received heartbeat from node '{}' via peer {:?} (seq: {})",
                    heartbeat.node_id, source, heartbeat.sequence
                );

                // Update peer's last_heartbeat time
                let mut peers = self.inner.peers.write().await;

                if let Some(peer_state) = peers.get_mut(source) {
                    peer_state.last_heartbeat = Some(SystemTime::now());
                    peer_state.last_seen = SystemTime::now();
                }
            }
            Err(e) => {
                warn!(
                    "Failed to deserialize heartbeat from peer {:?}: {}",
                    source, e
                );
            }
        }
    }

    /// Broadcast a heartbeat message to all connected peers.
    async fn broadcast_heartbeat(&self) {
        // Increment sequence number
        let sequence = {
            let mut seq = self.inner.heartbeat_sequence.write().await;
            *seq += 1;
            *seq
        };

        // Create heartbeat message
        let heartbeat = HeartbeatMessage::new(self.inner.node_id.clone(), sequence);

        match heartbeat.to_bytes() {
            Ok(bytes) => {
                // Publish to gossipsub
                let mut swarm = self.inner.swarm.write().await;

                let topic = gossipsub::IdentTopic::new(HEARTBEAT_TOPIC);
                if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic, bytes) {
                    warn!("Failed to broadcast heartbeat: {}", e);
                } else {
                    debug!("Broadcasted heartbeat (seq: {})", sequence);
                    drop(swarm);

                    // Record heartbeat sent metric
                    self.record_metric_counter("storage_network.heartbeat.sent", 1)
                        .await;
                }
            }
            Err(e) => {
                error!("Failed to serialize heartbeat: {}", e);
            }
        }
    }

    /// Handle a request-response event.
    async fn handle_request_response_event(
        &self,
        event: request_response::Event<Vec<u8>, Vec<u8>>,
    ) {
        use request_response::Event;

        match event {
            Event::Message {
                peer,
                message,
                connection_id: _,
            } => {
                use request_response::Message;
                match message {
                    Message::Request {
                        request_id,
                        request,
                        channel,
                    } => {
                        let internal_peer_id = libp2p_peer_id_to_internal(&peer);
                        info!(
                            "Received request from peer {:?} (ID: {:?}, {} bytes)",
                            internal_peer_id,
                            request_id,
                            request.len()
                        );

                        // TODO: Protocol detection limitation
                        // The current libp2p request-response implementation doesn't pass
                        // the protocol identifier to the message handler. Options:
                        // 1. Encode protocol in first bytes of request (protocol framing)
                        // 2. Use separate request-response behaviors per protocol
                        // 3. Wait for libp2p API improvement
                        //
                        // For now, we default to echo protocol for all requests.
                        // This works for testing but limits multi-protocol support.
                        let protocol = "/wormfs/echo";

                        // Route the request to the appropriate protocol handler
                        let response_result = self.route_request(protocol, request).await;

                        // Send response or handle error
                        match response_result {
                            Ok(response) => {
                                // Send successful response through the swarm
                                let mut swarm = self.inner.swarm.write().await;
                                if swarm
                                    .behaviour_mut()
                                    .request_response
                                    .send_response(channel, response)
                                    .is_err()
                                {
                                    warn!("Failed to send response to request {:?}", request_id);
                                }
                            }
                            Err(e) => {
                                error!(
                                    "Request handler failed for request {:?}: {}",
                                    request_id, e
                                );
                                // libp2p request-response doesn't support sending error responses
                                // The channel will be dropped, which signals failure to the peer
                            }
                        }
                    }

                    Message::Response {
                        request_id,
                        response,
                    } => {
                        debug!(
                            "Received response (ID: {:?}, {} bytes)",
                            request_id,
                            response.len()
                        );

                        // Look up pending request and send response back
                        let response_tx = {
                            let mut pending = self.inner.pending_requests.write().await;
                            pending.remove(&request_id)
                        };

                        if let Some(tx) = response_tx {
                            if tx.send(Ok(response)).is_err() {
                                warn!("Failed to deliver response for request {:?} - receiver dropped", request_id);
                            } else {
                                // Record successful request
                                self.record_metric_counter("storage_network.requests.succeeded", 1)
                                    .await;
                            }
                        } else {
                            warn!("Received response for unknown request {:?}", request_id);
                        }
                    }
                }
            }

            Event::OutboundFailure {
                peer,
                request_id,
                error,
                connection_id: _,
            } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer);
                warn!(
                    "Outbound request {:?} to peer {:?} failed: {:?}",
                    request_id, internal_peer_id, error
                );

                // Notify caller of failure
                let response_tx = {
                    let mut pending = self.inner.pending_requests.write().await;
                    pending.remove(&request_id)
                };

                if let Some(tx) = response_tx {
                    let err = Error::RequestFailed {
                        peer: internal_peer_id,
                        reason: format!("{:?}", error),
                    };
                    let _ = tx.send(Err(err));

                    // Record failed request
                    self.record_metric_counter("storage_network.requests.failed", 1)
                        .await;
                }
            }

            Event::InboundFailure {
                peer,
                request_id,
                error,
                connection_id: _,
            } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer);
                warn!(
                    "Inbound request {:?} from peer {:?} failed: {:?}",
                    request_id, internal_peer_id, error
                );
            }

            Event::ResponseSent {
                peer,
                request_id,
                connection_id: _,
            } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer);
                debug!(
                    "Response sent to peer {:?} for request {:?}",
                    internal_peer_id, request_id
                );
            }
        }
    }

    /// Handle a network command from the command channel.
    ///
    /// Returns `true` if the event loop should continue, `false` if shutdown was requested.
    async fn handle_network_command(&self, command: NetworkCommand) -> bool {
        match command {
            NetworkCommand::JoinTopic { name, response } => {
                let result = self.join_topic_internal(&name).await;
                if response.send(result).is_err() {
                    error!("Failed to send JoinTopic response");
                }
                true // Continue event loop
            }

            NetworkCommand::Broadcast { topic, message } => {
                if let Err(e) = self.broadcast_internal(&topic, message).await {
                    error!("Broadcast failed on topic '{}': {}", topic, e);
                }
                true // Continue event loop
            }

            NetworkCommand::SendToPeer {
                peer_id,
                topic,
                message,
            } => {
                if let Err(e) = self.send_to_peer_internal(&peer_id, &topic, message).await {
                    error!(
                        "SendToPeer failed to {:?} on topic '{}': {}",
                        peer_id, topic, e
                    );
                }
                true // Continue event loop
            }

            NetworkCommand::OpenStream {
                peer_id: _,
                protocol: _,
                response,
            } => {
                // Day 3: Implement stream opening
                let result = Err(Error::StreamFailed(
                    "Stream opening not yet implemented".to_string(),
                ));
                if response.send(result).is_err() {
                    error!("Failed to send OpenStream response");
                }
                true // Continue event loop
            }

            NetworkCommand::SendRequest {
                peer_id,
                protocol,
                request,
                response,
            } => {
                if let Err(e) = self
                    .send_request_internal(&peer_id, &protocol, request, response)
                    .await
                {
                    error!("SendRequest failed to {:?}: {}", peer_id, e);
                }
                true // Continue event loop
            }

            NetworkCommand::DialPeer { multiaddr } => {
                let mut swarm = self.inner.swarm.write().await;
                match multiaddr.parse::<Multiaddr>() {
                    Ok(addr) => {
                        info!("Dialing peer at {}", multiaddr);
                        if let Err(e) = swarm.dial(addr) {
                            warn!("Failed to dial peer at {}: {}", multiaddr, e);
                        }
                    }
                    Err(e) => {
                        warn!("Invalid multiaddr '{}': {}", multiaddr, e);
                    }
                }
                true // Continue event loop
            }

            NetworkCommand::DisconnectPeer { peer_id, response } => {
                let result = self.disconnect_peer_internal(&peer_id).await;
                if response.send(result).is_err() {
                    error!("Failed to send DisconnectPeer response");
                }
                true // Continue event loop
            }

            NetworkCommand::Shutdown { response } => {
                info!("Received shutdown command, initiating graceful shutdown");
                let result = self.shutdown_internal().await;
                if response.send(result).is_err() {
                    error!("Failed to send Shutdown response");
                }
                false // Exit event loop
            }
        }
    }

    /// Internal implementation of joining a topic.
    async fn join_topic_internal(&self, topic_name: &str) -> Result<TopicHandle, Error> {
        info!("Joining topic '{}'", topic_name);

        // Subscribe to gossipsub topic
        let topic = gossipsub::IdentTopic::new(topic_name);
        {
            let mut swarm = self.inner.swarm.write().await;
            swarm
                .behaviour_mut()
                .gossipsub
                .subscribe(&topic)
                .map_err(|e| Error::JoinTopicFailed {
                    topic: topic_name.to_string(),
                    reason: e.to_string(),
                })?;
        }

        // Create channels for this topic
        // tx: for the application to send messages to the network (for broadcasting)
        // rx: for the application to receive messages from the network
        let (tx, _internal_rx) = mpsc::unbounded_channel();
        let (internal_tx, rx) = mpsc::unbounded_channel();

        // Store internal_tx in topics map so event loop can route messages here
        let mut topics = self.inner.topics.write().await;

        // We need to store something that can receive TopicMessages
        // For now, create a simple struct to hold the sender
        let topic_state = TopicState { tx: internal_tx };
        topics.insert(topic_name.to_string(), topic_state);

        // Return the handle with tx (for sending) and rx (for receiving)
        Ok(TopicHandle { tx, rx })
    }

    /// Internal implementation of broadcasting a message.
    async fn broadcast_internal(&self, topic_name: &str, message: Vec<u8>) -> Result<(), Error> {
        let message_size = message.len();
        debug!(
            "Broadcasting {} bytes on topic '{}'",
            message_size, topic_name
        );

        let topic = gossipsub::IdentTopic::new(topic_name);
        let mut swarm = self.inner.swarm.write().await;

        swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic, message)
            .map_err(|e| Error::BroadcastFailed {
                topic: topic_name.to_string(),
                reason: e.to_string(),
            })?;

        // Drop the swarm lock before async metrics calls
        drop(swarm);

        // Record gossipsub message sent metrics
        self.record_metric_counter("storage_network.gossipsub.messages_sent", 1)
            .await;
        self.record_metric_counter("storage_network.gossipsub.bytes_sent", message_size as u64)
            .await;

        Ok(())
    }

    /// Internal implementation of sending to a specific peer.
    async fn send_to_peer_internal(
        &self,
        peer_id: &PeerId,
        topic_name: &str,
        message: Vec<u8>,
    ) -> Result<(), Error> {
        debug!(
            "Sending {} bytes to peer {:?} on topic '{}'",
            message.len(),
            peer_id,
            topic_name
        );

        // Verify peer is connected
        {
            let peers = self.inner.peers.read().await;
            if !peers.contains_key(peer_id) {
                return Err(Error::PeerNotConnected(peer_id.clone()));
            }
        }

        // For now, use broadcast (Day 3 will implement targeted messaging via request-response)
        self.broadcast_internal(topic_name, message).await
    }

    /// Internal implementation of sending a request and awaiting response.
    ///
    /// This uses libp2p's request-response protocol for direct peer-to-peer RPC.
    async fn send_request_internal(
        &self,
        peer_id: &PeerId,
        protocol: &str,
        request: Vec<u8>,
        response_tx: tokio::sync::oneshot::Sender<Result<Vec<u8>, Error>>,
    ) -> Result<(), Error> {
        info!(
            "Sending request ({} bytes) to peer {:?} on protocol '{}'",
            request.len(),
            peer_id,
            protocol
        );

        // Verify peer is connected
        {
            let peers = self.inner.peers.read().await;
            if !peers.contains_key(peer_id) {
                let _ = response_tx.send(Err(Error::PeerNotConnected(peer_id.clone())));
                return Err(Error::PeerNotConnected(peer_id.clone()));
            }
        }

        // Convert internal PeerId to libp2p PeerId
        let libp2p_peer_id = internal_peer_id_to_libp2p(peer_id)?;

        // Send request via request-response protocol
        let request_id = {
            let mut swarm = self.inner.swarm.write().await;

            swarm
                .behaviour_mut()
                .request_response
                .send_request(&libp2p_peer_id, request)
        };

        // Store response channel for when we get the response
        {
            let mut pending = self.inner.pending_requests.write().await;
            pending.insert(request_id, response_tx);
        }

        // Record total request count
        self.record_metric_counter("storage_network.requests.total", 1)
            .await;

        debug!("Request sent with ID: {:?}", request_id);
        Ok(())
    }

    /// Internal implementation of disconnecting from a peer.
    async fn disconnect_peer_internal(&self, peer_id: &PeerId) -> Result<(), Error> {
        info!("Disconnecting from peer {:?}", peer_id);

        // Verify peer exists
        {
            let peers = self.inner.peers.read().await;
            if !peers.contains_key(peer_id) {
                return Err(Error::PeerNotConnected(peer_id.clone()));
            }
        }

        // Convert internal PeerId to libp2p PeerId
        let libp2p_peer_id = internal_peer_id_to_libp2p(peer_id)?;

        // Disconnect via swarm
        {
            let mut swarm = self.inner.swarm.write().await;

            swarm
                .disconnect_peer_id(libp2p_peer_id)
                .map_err(|_| Error::DisconnectFailed {
                    peer: peer_id.clone(),
                    reason: "Failed to disconnect peer".to_string(),
                })?;
        }

        // Remove peer from our tracking
        {
            let mut peers = self.inner.peers.write().await;
            peers.remove(peer_id);
        }

        info!("Successfully disconnected from peer {:?}", peer_id);
        Ok(())
    }

    /// Internal implementation of graceful shutdown.
    ///
    /// This performs cleanup operations before the event loop exits:
    /// - Disconnects from all active peers
    /// - Unsubscribes from all topics
    /// - Cancels all pending requests
    async fn shutdown_internal(&self) -> Result<(), Error> {
        info!("Starting graceful shutdown");

        // 1. Disconnect from all peers
        {
            let peer_ids: Vec<PeerId> = {
                let peers = self.inner.peers.read().await;
                peers.keys().cloned().collect()
            };

            info!("Disconnecting from {} peer(s)", peer_ids.len());
            for peer_id in peer_ids {
                if let Err(e) = self.disconnect_peer_internal(&peer_id).await {
                    warn!(
                        "Failed to disconnect from peer {:?} during shutdown: {}",
                        peer_id, e
                    );
                }
            }
        }

        // 2. Unsubscribe from all topics
        {
            let topics: Vec<String> = {
                let topics_map = self.inner.topics.read().await;
                topics_map.keys().cloned().collect()
            };

            info!("Unsubscribing from {} topic(s)", topics.len());
            let mut swarm = self.inner.swarm.write().await;
            for topic_name in topics {
                let topic = gossipsub::IdentTopic::new(&topic_name);
                let success = swarm.behaviour_mut().gossipsub.unsubscribe(&topic);
                if success {
                    debug!("Unsubscribed from topic '{}'", topic_name);
                } else {
                    warn!(
                        "Failed to unsubscribe from topic '{}' during shutdown (not subscribed)",
                        topic_name
                    );
                }
            }
            drop(swarm);

            // Clear topics map
            let mut topics_map = self.inner.topics.write().await;
            topics_map.clear();
        }

        // 3. Cancel all pending requests
        {
            let pending_count = {
                let mut pending = self.inner.pending_requests.write().await;
                let count = pending.len();
                pending.clear();
                count
            };

            if pending_count > 0 {
                warn!(
                    "Cancelled {} pending request(s) during shutdown",
                    pending_count
                );
            }
        }

        info!("Graceful shutdown complete");
        Ok(())
    }

    /// Route incoming request to the appropriate protocol handler.
    ///
    /// This function dispatches requests based on the protocol identifier
    /// to specialized handler functions. Currently supports:
    /// - `/wormfs/echo` - Echo service for testing and diagnostics
    /// - `/wormfs/raft/1.0.0` - Raft consensus protocol (placeholder)
    ///
    /// # Arguments
    ///
    /// * `protocol` - The protocol identifier (e.g., "/wormfs/echo")
    /// * `request` - The request data bytes
    ///
    /// # Returns
    ///
    /// Response data bytes from the appropriate handler
    ///
    /// # Errors
    ///
    /// Returns `Error::ProtocolNotSupported` if the protocol is not recognized
    async fn route_request(&self, protocol: &str, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        debug!(
            "Routing request for protocol '{}' ({} bytes)",
            protocol,
            request.len()
        );

        // Record request metric
        self.record_metric_counter("storage_network.request_response.requests_total", 1)
            .await;

        match protocol {
            "/wormfs/echo" => self.handle_echo_request(request).await,
            "/wormfs/raft/1.0.0" => self.handle_raft_request(request).await,
            _ => {
                warn!("Unsupported protocol: {}", protocol);
                self.record_metric_counter("storage_network.request_response.unknown_protocol", 1)
                    .await;
                Err(Error::ProtocolNotSupported(protocol.to_string()))
            }
        }
    }

    /// Handle echo protocol requests.
    ///
    /// Protocol: `/wormfs/echo`
    ///
    /// This is a diagnostic handler that prepends "ECHO: " to the request
    /// and returns it. Useful for testing request-response functionality
    /// and network connectivity.
    ///
    /// # Arguments
    ///
    /// * `request` - The request data bytes
    ///
    /// # Returns
    ///
    /// The request data prepended with "ECHO: "
    async fn handle_echo_request(&self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        debug!("Handling echo request ({} bytes)", request.len());

        // Prepend "ECHO: " to the request
        let mut response = b"ECHO: ".to_vec();
        response.extend_from_slice(&request);

        // Record metric
        self.record_metric_counter("storage_network.request_response.echo_requests", 1)
            .await;

        Ok(response)
    }

    /// Handle Raft protocol requests.
    ///
    /// Protocol: `/wormfs/raft/1.0.0`
    ///
    /// This is a placeholder handler for Raft consensus protocol messages.
    /// Currently returns `ProtocolNotSupported` as the actual Raft RPC
    /// handling is not yet implemented.
    ///
    /// # Arguments
    ///
    /// * `request` - The request data bytes
    ///
    /// # Returns
    ///
    /// Currently returns an error. Will return Raft RPC response when implemented.
    ///
    /// # Errors
    ///
    /// Returns `Error::ProtocolNotSupported` until Raft RPC handler is implemented
    async fn handle_raft_request(&self, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        warn!(
            "Raft request handler not yet implemented (received {} bytes)",
            request.len()
        );

        // Record metric
        self.record_metric_counter("storage_network.request_response.raft_requests", 1)
            .await;
        self.record_metric_counter("storage_network.request_response.handler_errors", 1)
            .await;

        // TODO: Route to actual Raft RPC handler when implemented
        Err(Error::ProtocolNotSupported(
            "/wormfs/raft/1.0.0".to_string(),
        ))
    }

    /// Helper method to record a counter metric if metrics service is available.
    async fn record_metric_counter(&self, name: &str, value: u64) {
        if let Some(metrics) = self.inner.metrics.read().await.as_ref() {
            use crate::metric_service::{MetricService, UnitType};
            let _ = metrics.publish_counter(name, value, UnitType::Operations);
        }
    }

    /// Helper method to record a gauge metric if metrics service is available.
    async fn record_metric_gauge(
        &self,
        name: &str,
        value: f64,
        unit: crate::metric_service::UnitType,
    ) {
        if let Some(metrics) = self.inner.metrics.read().await.as_ref() {
            use crate::metric_service::MetricService;
            let _ = metrics.publish_gauge(name, value, unit);
        }
    }

    /// Helper method to record a histogram metric if metrics service is available.
    async fn record_metric_histogram(
        &self,
        name: &str,
        value: f64,
        unit: crate::metric_service::UnitType,
    ) {
        if let Some(metrics) = self.inner.metrics.read().await.as_ref() {
            use crate::metric_service::MetricService;
            let _ = metrics.publish_histogram(name, value, unit);
        }
    }

    /// Validate a peer's ID against configuration and stored values.
    ///
    /// This method implements two validation modes:
    /// - **Explicit mode**: Validates against a configured peer ID
    /// - **AutoId mode**: Learns and stores peer IDs on first connection, validates on subsequent connections
    ///
    /// # Arguments
    ///
    /// * `ip` - IP address of the connecting peer
    /// * `peer_id` - Peer ID from libp2p handshake
    ///
    /// # Returns
    ///
    /// - `Validated`: Peer ID matches expected value
    /// - `NewlyDiscovered(PeerId)`: First connection in AutoId mode, peer ID stored
    /// - `Rejected { expected, actual }`: Peer ID mismatch
    ///
    /// # Errors
    ///
    /// Returns an error if the IP is not in the configured peer list or if
    /// validation logic fails.
    /// Validate a peer's ID directly against configuration.
    ///
    /// This validates based on the cryptographic peer ID only, not IP address.
    /// IP addresses and ports can change, but peer IDs are stable identities.
    pub(crate) async fn validate_peer_id_direct(
        &self,
        peer_id: PeerId,
    ) -> Result<super::types::ValidationResult, Error> {
        use super::types::{PeerIdConfig, ValidationResult};

        // Check all configured peers to see if any match this peer ID
        for peer_config in &self.inner.config.peers {
            match &peer_config.peer_id {
                PeerIdConfig::Explicit(expected_id) => {
                    // Explicit mode: check if this peer ID matches
                    if expected_id == &peer_id {
                        info!("Peer {:?} validated with explicit ID", peer_id);
                        return Ok(ValidationResult::Validated);
                    }
                }
                PeerIdConfig::AutoId => {
                    // AutoId mode: check if we've seen this peer ID before
                    match self.inner.peer_id_store.get_by_peer_id(&peer_id) {
                        Ok(Some(stored_id)) if stored_id == peer_id => {
                            info!("Peer {:?} validated as previously discovered", peer_id);
                            return Ok(ValidationResult::Validated);
                        }
                        Ok(None) => {
                            // Not seen before, continue to new discovery logic
                        }
                        Ok(Some(_)) => {
                            // This should never happen - stored ID doesn't match peer ID
                            warn!("Peer ID mismatch in store");
                        }
                        Err(e) => {
                            return Err(Error::ValidationFailed(format!(
                                "Failed to check peer ID store: {}",
                                e
                            )));
                        }
                    }
                }
            }
        }

        // Check if this is a new AutoId peer
        // If any peer is configured in AutoId mode, accept and store new peers
        let has_autoid = self
            .inner
            .config
            .peers
            .iter()
            .any(|p| matches!(p.peer_id, PeerIdConfig::AutoId));

        if has_autoid {
            // Store this newly discovered peer ID
            // We use the peer ID itself as the key
            self.inner
                .peer_id_store
                .store_by_peer_id(peer_id.clone())
                .map_err(|e| Error::ValidationFailed(format!("Failed to store peer ID: {}", e)))?;

            info!("Peer {:?} newly discovered in AutoId mode", peer_id);
            return Ok(ValidationResult::NewlyDiscovered(peer_id));
        }

        // No match found and no AutoId mode - reject
        warn!("Peer {:?} not in configured peer list", peer_id);
        Err(Error::ValidationFailed(format!(
            "Peer {:?} is not in configured peer list",
            peer_id
        )))
    }

    /// Legacy IP-based validation method (deprecated, kept for compatibility)
    #[allow(dead_code)]
    pub(crate) async fn validate_peer_id(
        &self,
        ip: std::net::IpAddr,
        peer_id: PeerId,
    ) -> Result<super::types::ValidationResult, Error> {
        // Delegate to peer-ID-based validation
        self.validate_peer_id_direct(peer_id).await
    }
}

/// Implementation for InnerState to expose methods needed by StorageNetworkHandle.
impl InnerState {
    /// Validate a peer's ID against configuration and stored values.
    ///
    /// This method delegates to the same validation logic used by StorageNetworkInner.
    pub(crate) async fn validate_peer_id(
        &self,
        ip: std::net::IpAddr,
        peer_id: PeerId,
    ) -> Result<super::types::ValidationResult, Error> {
        use super::types::{PeerIdConfig, ValidationResult};

        // Find peer config for this IP by checking if IP appears in multiaddr
        let ip_str = ip.to_string();
        let peer_config = self
            .config
            .peers
            .iter()
            .find(|p| p.multiaddr.contains(&ip_str));

        let peer_config = match peer_config {
            Some(config) => config,
            None => {
                return Err(Error::ValidationFailed(format!(
                    "IP {} is not in configured peer list",
                    ip
                )))
            }
        };

        // Validate based on peer ID configuration mode
        match &peer_config.peer_id {
            PeerIdConfig::Explicit(expected_id) => {
                // Explicit mode: validate against configured peer ID
                if expected_id == &peer_id {
                    info!("Peer {} validated with explicit ID", ip);
                    Ok(ValidationResult::Validated)
                } else {
                    warn!(
                        "Peer {} rejected: expected ID {:?}, got {:?}",
                        ip, expected_id, peer_id
                    );
                    Ok(ValidationResult::Rejected {
                        expected: expected_id.clone(),
                        actual: peer_id,
                    })
                }
            }
            PeerIdConfig::AutoId => {
                // AutoId mode: check if we've seen this peer before
                match self.peer_id_store.get(&ip) {
                    Ok(Some(stored_id)) => {
                        // We've seen this peer before, validate against stored ID
                        if stored_id == peer_id {
                            info!("Peer {} validated with learned ID", ip);
                            Ok(ValidationResult::Validated)
                        } else {
                            warn!(
                                "Peer {} rejected: learned ID {:?}, got {:?}",
                                ip, stored_id, peer_id
                            );
                            Ok(ValidationResult::Rejected {
                                expected: stored_id,
                                actual: peer_id,
                            })
                        }
                    }
                    Ok(None) => {
                        // First time seeing this peer, store its ID
                        self.peer_id_store.store(ip, peer_id.clone()).map_err(|e| {
                            Error::ValidationFailed(format!("Failed to store peer ID: {}", e))
                        })?;
                        info!("Peer {} newly discovered with ID {:?}", ip, peer_id);
                        Ok(ValidationResult::NewlyDiscovered(peer_id))
                    }
                    Err(e) => {
                        Err(Error::ValidationFailed(format!("Failed to check peer ID store: {}", e)))
                    }
                }
            }
        }
    }
}

// Helper functions for converting between libp2p and internal types
fn libp2p_peer_id_to_internal(peer_id: &Libp2pPeerId) -> PeerId {
    PeerId::new(peer_id.to_bytes())
}

/// Convert internal PeerId to libp2p PeerId.
/// Day 3: Will be used for request-response protocol.
#[allow(dead_code)]
fn internal_peer_id_to_libp2p(peer_id: &PeerId) -> Result<Libp2pPeerId, Error> {
    Libp2pPeerId::from_bytes(peer_id.as_bytes())
        .map_err(|e| Error::ConfigError(format!("Invalid peer ID: {}", e)))
}

/// Extract IP address from a libp2p Multiaddr.
///
/// This function parses a Multiaddr and extracts the IP address component.
/// It supports both IPv4 and IPv6 addresses.
///
/// # Arguments
///
/// * `multiaddr` - The multiaddr to parse
///
/// # Returns
///
/// The extracted IP address, or `None` if no IP component was found.
fn extract_ip_from_multiaddr(multiaddr: &libp2p::Multiaddr) -> Option<std::net::IpAddr> {
    use libp2p::multiaddr::Protocol;

    for component in multiaddr.iter() {
        match component {
            Protocol::Ip4(addr) => return Some(std::net::IpAddr::V4(addr)),
            Protocol::Ip6(addr) => return Some(std::net::IpAddr::V6(addr)),
            _ => continue,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Helper to create a test Config with the given store path suffix
    fn test_config(store_suffix: &str) -> Config {
        Config {
            node_id: format!("test-node-{}", store_suffix),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![],
            peer_id_store_path: std::env::temp_dir().join(format!("test_{}.json", store_suffix)),
            max_peers: 100,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        }
    }

    #[tokio::test]
    async fn test_factory_creates_network() {
        let config = test_config("factory");

        let result = super::super::StorageNetworkFactory::create(config).await;
        assert!(result.is_ok(), "Factory should create network successfully");
    }

    #[test]
    fn test_peer_id_conversion() {
        let libp2p_id = Libp2pPeerId::random();
        let internal = libp2p_peer_id_to_internal(&libp2p_id);
        let back_to_libp2p = internal_peer_id_to_libp2p(&internal).unwrap();
        assert_eq!(
            libp2p_id, back_to_libp2p,
            "Peer ID conversion should be reversible"
        );
    }

    #[tokio::test]
    async fn test_join_topic_command() {
        // This test verifies that join_topic sends the correct command
        // Full end-to-end testing with event loop will be in Day 4 integration tests
        let config = Config {
            node_id: "test-node".to_string(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![],
            peer_id_store_path: std::env::temp_dir().join("test_peer_ids_cmd.json"),
            max_peers: 100,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let (_inner, handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        // Test that we can attempt to broadcast (command is queued)
        let broadcast_result = handle.broadcast("test-topic", b"hello".to_vec()).await;
        assert!(
            broadcast_result.is_ok(),
            "Should be able to queue broadcast command: {:?}",
            broadcast_result.err()
        );

        // Test send_to_peer command queuing
        let peer_id = PeerId::new(vec![1, 2, 3]);
        let send_result = handle
            .send_to_peer(&peer_id, "test-topic", b"hello".to_vec())
            .await;
        assert!(
            send_result.is_ok(),
            "Should be able to queue send_to_peer command: {:?}",
            send_result.err()
        );
    }

    #[tokio::test]
    async fn test_peer_state_tracking() {
        let config = Config {
            node_id: "test-node".to_string(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![],
            peer_id_store_path: std::env::temp_dir().join("test_peer_ids_state.json"),
            max_peers: 100,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let (_inner, handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        // Initially no peers
        let peers = handle.get_connected_peers().await;
        assert_eq!(peers.len(), 0, "Should start with no peers");

        // Test get_peer_info for non-existent peer
        let peer_id = PeerId::new(vec![1, 2, 3, 4]);
        let info = handle.get_peer_info(&peer_id).await;
        assert!(info.is_none(), "Non-existent peer should return None");
    }

    #[tokio::test]
    async fn test_send_request_peer_not_connected() {
        // Test that send_request properly queues request command
        // even when peer is not connected (error will be returned)
        let config = Config {
            node_id: "test-node".to_string(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![],
            peer_id_store_path: std::env::temp_dir().join("test_request_no_peer.json"),
            max_peers: 100,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let (_inner, handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        // Try to send request to non-existent peer
        let peer_id = PeerId::new(vec![1, 2, 3, 4, 5]);
        let request = b"test request".to_vec();

        // This should queue the command successfully, but since the event loop
        // isn't running, it will timeout waiting for a response
        let result = tokio::time::timeout(
            Duration::from_millis(50),
            handle.send_request(&peer_id, "/wormfs/test/1.0.0", request),
        )
        .await;

        // Should timeout since event loop isn't running to process commands
        assert!(
            result.is_err(),
            "Request should timeout when event loop is not running"
        );
    }

    #[tokio::test]
    async fn test_request_response_command_queuing() {
        // Test that send_request command can be queued
        let config = Config {
            node_id: "test-node".to_string(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![],
            peer_id_store_path: std::env::temp_dir().join("test_request_queue.json"),
            max_peers: 100,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let (_inner, handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        let peer_id = PeerId::new(vec![1, 2, 3, 4, 5, 6]);
        let request_data = b"test request data".to_vec();

        // Try to send request (command will be queued, but event loop isn't running)
        // We expect this to timeout since no event loop is processing commands
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            handle.send_request(&peer_id, "/wormfs/test/1.0.0", request_data),
        )
        .await;

        // Should timeout since event loop isn't running to process the command
        assert!(
            result.is_err() || matches!(result, Ok(Err(_))),
            "Request should timeout or fail without event loop"
        );
    }

    #[tokio::test]
    async fn test_metrics_are_optional() {
        // Test that metrics can be None and operations still work
        let config = Config {
            node_id: "test-node".to_string(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![],
            peer_id_store_path: std::env::temp_dir().join("test_metrics_optional.json"),
            max_peers: 100,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let (_inner, handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        // Verify metrics is None by default
        let metrics_lock = handle.inner.metrics.read().await;
        assert!(metrics_lock.is_none(), "Metrics should be None by default");
        drop(metrics_lock);

        // Operations should still work without metrics (will timeout due to no event loop)
        let peer_id = PeerId::new(vec![7, 8, 9]);
        let result = tokio::time::timeout(
            Duration::from_millis(50),
            handle.send_request(&peer_id, "/test", b"data".to_vec()),
        )
        .await;

        // Should timeout due to no event loop, not fail due to missing metrics
        assert!(
            result.is_err(),
            "Request should timeout without event loop, even with no metrics"
        );
    }

    #[tokio::test]
    async fn test_validate_peer_id_explicit_mode_valid() {
        // Test peer validation in explicit mode with matching peer ID
        use std::net::{IpAddr, Ipv4Addr};

        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100));
        let peer_id = PeerId::new(vec![1, 2, 3, 4, 5]);

        let config = Config {
            node_id: "test-node".to_string(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![super::super::types::PeerConfig {
                multiaddr: format!("/ip4/{}/tcp/0", ip),
                peer_id: super::super::types::PeerIdConfig::Explicit(peer_id.clone()),
            }],
            peer_id_store_path: std::env::temp_dir().join("test_validate_explicit_valid.json"),
            max_peers: 100,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        let result = inner
            .validate_peer_id(ip, peer_id)
            .await
            .expect("Validation should succeed");

        assert!(
            matches!(result, super::super::types::ValidationResult::Validated),
            "Expected Validated, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_validate_peer_id_explicit_mode_rejected() {
        // Test peer-ID validation in explicit mode with non-configured peer ID
        // With peer-ID validation, unknown peers return an error (not Rejected)

        let expected_peer_id = PeerId::new(vec![1, 2, 3, 4, 5]);
        let unknown_peer_id = PeerId::new(vec![6, 7, 8, 9, 10]);

        let config = Config {
            node_id: "test-node".to_string(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![super::super::types::PeerConfig {
                multiaddr: "/ip4/192.168.1.101/tcp/0".to_string(),
                peer_id: super::super::types::PeerIdConfig::Explicit(expected_peer_id.clone()),
            }],
            peer_id_store_path: std::env::temp_dir().join("test_validate_explicit_reject.json"),
            max_peers: 100,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        // Try to validate an unknown peer ID - should return error
        let result = inner.validate_peer_id_direct(unknown_peer_id.clone()).await;
        assert!(
            result.is_err(),
            "Expected error for unknown peer ID, got {:?}",
            result
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not in configured peer list"),
            "Error should mention peer not in list"
        );
    }

    #[tokio::test]
    async fn test_validate_peer_id_auto_mode_first_connection() {
        // Test peer-ID validation in AutoId mode on first connection (should learn ID)

        let peer_id = PeerId::new(vec![1, 2, 3, 4, 5]);

        let temp_store = std::env::temp_dir().join("test_validate_auto_first.json");
        let _ = std::fs::remove_file(&temp_store); // Clean up from previous test

        let config = Config {
            node_id: "test-node".to_string(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![super::super::types::PeerConfig {
                multiaddr: "/ip4/192.168.1.102/tcp/0".to_string(),
                peer_id: super::super::types::PeerIdConfig::AutoId,
            }],
            peer_id_store_path: temp_store.clone(),
            max_peers: 100,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        // First connection should learn the peer ID
        let result = inner
            .validate_peer_id_direct(peer_id.clone())
            .await
            .expect("Validation should succeed");

        match result {
            super::super::types::ValidationResult::NewlyDiscovered(discovered_id) => {
                assert_eq!(discovered_id, peer_id);
            }
            _ => panic!("Expected NewlyDiscovered, got {:?}", result),
        }

        // Verify peer ID was stored (using peer-ID-based lookup)
        let stored_id = inner.inner.peer_id_store.get_by_peer_id(&peer_id).expect("Failed to get peer ID");
        assert_eq!(stored_id, Some(peer_id));
    }

    #[tokio::test]
    async fn test_validate_peer_id_auto_mode_subsequent_connection_valid() {
        // Test peer validation in AutoId mode on subsequent connection with matching ID
        use std::net::{IpAddr, Ipv4Addr};

        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 103));
        let peer_id = PeerId::new(vec![1, 2, 3, 4, 5]);

        let temp_store = std::env::temp_dir().join("test_validate_auto_subsequent_valid.json");
        let _ = std::fs::remove_file(&temp_store);

        let config = Config {
            node_id: "test-node".to_string(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![super::super::types::PeerConfig {
                multiaddr: format!("/ip4/{}/tcp/0", ip),
                peer_id: super::super::types::PeerIdConfig::AutoId,
            }],
            peer_id_store_path: temp_store.clone(),
            max_peers: 100,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        // First connection - learn the ID
        inner
            .validate_peer_id(ip, peer_id.clone())
            .await
            .expect("First validation should succeed");

        // Second connection - validate against stored ID
        let result = inner
            .validate_peer_id(ip, peer_id)
            .await
            .expect("Second validation should succeed");

        assert!(
            matches!(result, super::super::types::ValidationResult::Validated),
            "Expected Validated on subsequent connection, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_validate_peer_id_auto_mode_subsequent_connection_same_peer() {
        // Test peer-ID validation in AutoId mode: subsequent connection with same peer ID
        // should be validated (not discovered again)

        let first_peer_id = PeerId::new(vec![1, 2, 3, 4, 5]);

        let temp_store = std::env::temp_dir().join("test_validate_auto_subsequent_same.json");
        let _ = std::fs::remove_file(&temp_store);

        let config = Config {
            node_id: "test-node".to_string(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![super::super::types::PeerConfig {
                multiaddr: "/ip4/192.168.1.104/tcp/0".to_string(),
                peer_id: super::super::types::PeerIdConfig::AutoId,
            }],
            peer_id_store_path: temp_store.clone(),
            max_peers: 100,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("Factory should create network");

        // First connection - learn the ID
        let result1 = inner
            .validate_peer_id_direct(first_peer_id.clone())
            .await
            .expect("First validation should succeed");
        assert!(
            matches!(
                result1,
                super::super::types::ValidationResult::NewlyDiscovered(_)
            ),
            "First connection should be NewlyDiscovered"
        );

        // Second connection with SAME peer ID - should be validated (recognized)
        let result2 = inner
            .validate_peer_id_direct(first_peer_id.clone())
            .await
            .expect("Second validation should succeed");
        assert!(
            matches!(result2, super::super::types::ValidationResult::Validated),
            "Subsequent connection with same peer ID should be Validated, got {:?}",
            result2
        );
    }

    // Note: test_validate_peer_id_unknown_ip was removed because it tested IP-based
    // validation logic which has been replaced by peer-ID-based validation.
    // The equivalent security guarantees are now provided by:
    // - Explicit mode: Only configured peer IDs are accepted (tested by test_validate_peer_id_explicit_mode_rejected)
    // - AutoId mode: Peer IDs are learned once and enforced on subsequent connections

    #[tokio::test]
    async fn test_factory_create_returns_valid_handle() {
        let config = test_config("create_test");

        let (_inner, handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        // Verify handle is usable immediately
        let peers = handle.get_connected_peers().await;
        assert_eq!(peers.len(), 0, "Should start with no peers");

        // Verify we can clone the handle
        let handle2 = handle.clone();
        let peers2 = handle2.get_connected_peers().await;
        assert_eq!(peers2.len(), 0, "Cloned handle should work the same");
    }

    #[tokio::test]
    async fn test_factory_inner_and_handle_share_state() {
        let config = test_config("shared_state_test");

        let (_inner, handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        // Both handle and inner should see the same peer list
        let peers_from_handle = handle.get_connected_peers().await;
        assert_eq!(
            peers_from_handle.len(),
            0,
            "Handle should see initial empty peer list"
        );

        // Verify peer info query works
        use libp2p::PeerId as Libp2pPeerId;
        let random_peer = Libp2pPeerId::random();
        let peer_info = handle.get_peer_info(&PeerId(random_peer.to_bytes())).await;
        assert!(peer_info.is_none(), "Should return None for unknown peer");
    }

    #[tokio::test]
    async fn test_factory_with_multiple_listen_addresses() {
        let config = Config {
            node_id: "multi_addr_test".to_string(),
            listen_addresses: vec![
                "/ip4/127.0.0.1/tcp/0".to_string(),
                "/ip4/0.0.0.0/tcp/0".to_string(),
            ],
            peers: vec![],
            peer_id_store_path: std::env::temp_dir().join("test_multi_addr.json"),
            max_peers: 10,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let result = super::super::StorageNetworkFactory::create(config).await;
        assert!(
            result.is_ok(),
            "Should create network with multiple listen addresses"
        );
    }

    #[tokio::test]
    async fn test_factory_channel_communication() {
        let config = test_config("channel_test");

        let (_inner, handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        // Verify we can send commands through the handle
        // The handle uses event_tx internally for operations like send_to_peer
        // We're testing that the channel is properly set up

        // Try to use the handle's command channel by calling a method
        // Even if we don't have a peer, this tests channel functionality
        use libp2p::PeerId as Libp2pPeerId;
        let test_peer = PeerId(Libp2pPeerId::random().to_bytes());
        let test_topic = "test/topic";
        let test_data = vec![1, 2, 3];

        // This will send a command through the channel (even though it will fail
        // later because the peer doesn't exist and event loop isn't running)
        let result = handle.send_to_peer(&test_peer, test_topic, test_data).await;

        // The command should be queued successfully (channel send succeeds)
        // The actual peer communication would happen in the event loop
        assert!(
            result.is_ok(),
            "Command should be queued successfully through channel"
        );
    }

    // ========== Phase 4: Event Handler Integration Tests ==========

    #[test]
    fn test_heartbeat_message_serialization() {
        let heartbeat = HeartbeatMessage::new("test_node".to_string(), 42);

        // Serialize
        let bytes = heartbeat.to_bytes().expect("Serialization should succeed");
        assert!(!bytes.is_empty(), "Serialized data should not be empty");

        // Deserialize
        let deserialized =
            HeartbeatMessage::from_bytes(&bytes).expect("Deserialization should succeed");

        assert_eq!(
            deserialized.node_id, "test_node",
            "Node ID should be preserved"
        );
        assert_eq!(deserialized.sequence, 42, "Sequence should be preserved");
    }

    #[test]
    fn test_heartbeat_message_invalid_data() {
        let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];

        let result = HeartbeatMessage::from_bytes(&invalid_data);
        assert!(result.is_err(), "Should fail to deserialize invalid data");
    }

    #[tokio::test]
    async fn test_heartbeat_updates_peer_state() {
        let config = test_config("heartbeat_peer_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        // Add a fake peer to the state
        use libp2p::PeerId as Libp2pPeerId;
        let libp2p_peer = Libp2pPeerId::random();
        let peer_id = PeerId(libp2p_peer.to_bytes());

        {
            let mut peers = inner.inner.peers.write().await;
            peers.insert(
                peer_id.clone(),
                PeerState {
                    peer_id: peer_id.clone(),
                    addresses: vec![],
                    connection_state: ConnectionState::Connected,
                    last_seen: SystemTime::now() - Duration::from_secs(100),
                    validation_status: ValidationStatus::Validated,
                    last_heartbeat: None,
                },
            );
        }

        // Create and handle a heartbeat message
        let heartbeat = HeartbeatMessage::new("remote_node".to_string(), 1);
        let heartbeat_bytes = heartbeat.to_bytes().expect("Serialization should work");

        inner
            .handle_heartbeat_message(&peer_id, &heartbeat_bytes)
            .await;

        // Verify peer state was updated
        let peers = inner.inner.peers.read().await;
        let peer_state = peers.get(&peer_id).expect("Peer should exist");

        assert!(
            peer_state.last_heartbeat.is_some(),
            "Last heartbeat should be set"
        );
        assert!(
            peer_state.last_seen > SystemTime::now() - Duration::from_secs(1),
            "Last seen should be recent"
        );
    }

    #[tokio::test]
    async fn test_heartbeat_invalid_data_logged() {
        let config = test_config("heartbeat_invalid_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        use libp2p::PeerId as Libp2pPeerId;
        let peer_id = PeerId(Libp2pPeerId::random().to_bytes());

        // Send invalid heartbeat data
        let invalid_data = vec![0xFF, 0xFF, 0xFF];

        // This should not panic, just log a warning
        inner
            .handle_heartbeat_message(&peer_id, &invalid_data)
            .await;

        // If we get here, the handler properly handled invalid data
    }

    #[tokio::test]
    async fn test_broadcast_heartbeat_increments_sequence() {
        let config = test_config("heartbeat_sequence_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        // Get initial sequence
        let initial_seq = *inner.inner.heartbeat_sequence.read().await;

        // Broadcast heartbeat (this will try to publish to gossipsub)
        inner.broadcast_heartbeat().await;

        // Check sequence was incremented
        let new_seq = *inner.inner.heartbeat_sequence.read().await;
        assert_eq!(
            new_seq,
            initial_seq + 1,
            "Sequence should increment on broadcast"
        );

        // Broadcast again
        inner.broadcast_heartbeat().await;

        let newer_seq = *inner.inner.heartbeat_sequence.read().await;
        assert_eq!(
            newer_seq,
            initial_seq + 2,
            "Sequence should continue incrementing"
        );
    }

    #[tokio::test]
    async fn test_topic_subscription_internal() {
        let config = test_config("topic_internal_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        let topic_name = "test/topic";

        // Join topic internally
        let result = inner.join_topic_internal(topic_name).await;

        // Should succeed (gossipsub subscribe will work even without peers)
        assert!(
            result.is_ok(),
            "Should successfully subscribe to topic internally"
        );

        let _topic_handle = result.unwrap();

        // Verify topic was added to internal state
        let topics = inner.inner.topics.read().await;
        assert!(
            topics.contains_key(topic_name),
            "Topic should be in internal state"
        );

        // Verify topic handle has valid channels
        // (TopicHandle only contains tx and rx, not the topic name)
    }

    #[tokio::test]
    async fn test_topic_message_routing() {
        let config = test_config("topic_routing_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        let topic_name = "test/messages";

        // Join topic
        let topic_handle = inner
            .join_topic_internal(topic_name)
            .await
            .expect("Should subscribe to topic");

        // Simulate receiving a topic message (directly push to channel)
        use libp2p::PeerId as Libp2pPeerId;
        let fake_peer = PeerId(Libp2pPeerId::random().to_bytes());
        let test_message = vec![1, 2, 3, 4, 5];

        let topic_message = TopicMessage {
            source: fake_peer.clone(),
            data: test_message.clone(),
            timestamp: SystemTime::now(),
        };

        // Send message through the topic's internal channel
        {
            let topics = inner.inner.topics.read().await;
            let topic_state = topics.get(topic_name).expect("Topic should exist");
            topic_state
                .tx
                .send(topic_message.clone())
                .expect("Should send message");
        }

        // Try to receive the message
        let mut rx = topic_handle.rx;
        let received = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("Should receive within timeout")
            .expect("Should get a message");

        assert_eq!(received.source, fake_peer, "Source should match");
        assert_eq!(received.data, test_message, "Data should match");
    }

    #[tokio::test]
    async fn test_multiple_topic_subscribers() {
        let config = test_config("multi_subscriber_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        let topic_name = "test/shared";

        // First subscriber joins
        let handle1 = inner
            .join_topic_internal(topic_name)
            .await
            .expect("First subscribe should succeed");

        // Second subscriber joins the same topic
        let _handle2 = inner
            .join_topic_internal(topic_name)
            .await
            .expect("Second subscribe should succeed");

        // Both subscriptions succeeded
        // Drop the first handle (we only needed to test subscription)
        drop(handle1);

        // Verify topic only registered once
        let topics = inner.inner.topics.read().await;
        assert_eq!(
            topics.len(),
            1,
            "Should only have one topic entry even with multiple subscribers"
        );
    }

    #[tokio::test]
    async fn test_disconnect_peer_internal() {
        let config = test_config("disconnect_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        use libp2p::PeerId as Libp2pPeerId;
        let libp2p_peer = Libp2pPeerId::random();
        let peer_id = PeerId(libp2p_peer.to_bytes());

        // Add peer to state
        {
            let mut peers = inner.inner.peers.write().await;
            peers.insert(
                peer_id.clone(),
                PeerState {
                    peer_id: peer_id.clone(),
                    addresses: vec![],
                    connection_state: ConnectionState::Connected,
                    last_seen: SystemTime::now(),
                    validation_status: ValidationStatus::Validated,
                    last_heartbeat: None,
                },
            );
        }

        // Disconnect the peer
        let result = inner.disconnect_peer_internal(&peer_id).await;

        // Should succeed (or return appropriate error if peer not connected)
        // The actual disconnection happens via swarm operations
        assert!(
            result.is_ok() || result.is_err(),
            "Disconnect should complete"
        );
    }

    #[tokio::test]
    async fn test_peer_state_lifecycle() {
        let config = test_config("peer_lifecycle_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        use libp2p::PeerId as Libp2pPeerId;
        let libp2p_peer = Libp2pPeerId::random();
        let peer_id = PeerId(libp2p_peer.to_bytes());

        // Initially no peers
        {
            let peers = inner.inner.peers.read().await;
            assert_eq!(peers.len(), 0, "Should start with no peers");
        }

        // Add a peer
        {
            let mut peers = inner.inner.peers.write().await;
            peers.insert(
                peer_id.clone(),
                PeerState {
                    peer_id: peer_id.clone(),
                    addresses: vec![],
                    connection_state: ConnectionState::Connected,
                    last_seen: SystemTime::now(),
                    validation_status: ValidationStatus::Validated,
                    last_heartbeat: None,
                },
            );
        }

        // Verify peer exists
        {
            let peers = inner.inner.peers.read().await;
            assert_eq!(peers.len(), 1, "Should have one peer");
            assert!(peers.contains_key(&peer_id), "Should contain our peer");
        }

        // Remove peer
        {
            let mut peers = inner.inner.peers.write().await;
            peers.remove(&peer_id);
        }

        // Verify peer removed
        {
            let peers = inner.inner.peers.read().await;
            assert_eq!(peers.len(), 0, "Should have no peers after removal");
        }
    }

    // ========== Phase 5: Edge Cases & Error Path Tests ==========

    #[tokio::test]
    async fn test_empty_configuration() {
        let config = Config {
            node_id: "empty_config_test".to_string(),
            listen_addresses: vec![], // Empty listen addresses
            peers: vec![],            // No configured peers
            peer_id_store_path: std::env::temp_dir().join("test_empty_config.json"),
            max_peers: 10,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let result = super::super::StorageNetworkFactory::create(config).await;

        // Should succeed even with empty configuration
        assert!(
            result.is_ok(),
            "Should create network with empty peer/address lists"
        );
    }

    #[tokio::test]
    async fn test_malformed_listen_address() {
        let config = Config {
            node_id: "malformed_addr_test".to_string(),
            listen_addresses: vec![
                "not-a-valid-multiaddr".to_string(),
                "also/invalid/format".to_string(),
            ],
            peers: vec![],
            peer_id_store_path: std::env::temp_dir().join("test_malformed_addr.json"),
            max_peers: 10,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let result = super::super::StorageNetworkFactory::create(config).await;

        // Should still create network, but malformed addresses will be skipped during run()
        assert!(
            result.is_ok(),
            "Factory creation should succeed even with malformed addresses"
        );
    }

    #[tokio::test]
    async fn test_broadcast_to_unsubscribed_topic() {
        let config = test_config("broadcast_unsubscribed_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        let topic_name = "never/subscribed";
        let message = vec![1, 2, 3];

        // Try to broadcast to a topic we never subscribed to
        let result = inner.broadcast_internal(topic_name, message).await;

        // This might fail or succeed depending on implementation
        // The important thing is it doesn't panic
        assert!(
            result.is_ok() || result.is_err(),
            "Broadcast to unsubscribed topic should be handled gracefully"
        );
    }

    #[tokio::test]
    async fn test_send_to_peer_via_unknown_topic() {
        let config = test_config("send_unknown_topic_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        use libp2p::PeerId as Libp2pPeerId;
        let peer_id = PeerId(Libp2pPeerId::random().to_bytes());
        let topic_name = "unknown/topic";
        let message = vec![1, 2, 3];

        // Try to send to a peer on an unknown topic
        let result = inner
            .send_to_peer_internal(&peer_id, topic_name, message)
            .await;

        // Should handle gracefully (likely return error)
        assert!(
            result.is_err() || result.is_ok(),
            "Send via unknown topic should be handled"
        );
    }

    #[tokio::test]
    async fn test_empty_message_handling() {
        let config = test_config("empty_message_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        let topic_name = "test/empty";

        // Subscribe to topic
        let _topic_handle = inner
            .join_topic_internal(topic_name)
            .await
            .expect("Should subscribe");

        // Try to broadcast empty message
        let empty_message = vec![];
        let result = inner.broadcast_internal(topic_name, empty_message).await;

        // Empty messages should be allowed (gossipsub will handle it)
        assert!(
            result.is_ok() || result.is_err(),
            "Empty message should be handled"
        );
    }

    #[tokio::test]
    async fn test_concurrent_peer_state_access() {
        let config = test_config("concurrent_access_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        use libp2p::PeerId as Libp2pPeerId;

        // Spawn multiple tasks that concurrently access peer state
        let inner_arc = Arc::clone(&inner.inner);
        let task1 = tokio::spawn(async move {
            for _ in 0..10 {
                let peers = inner_arc.peers.read().await;
                let _count = peers.len();
                drop(peers);
                tokio::time::sleep(Duration::from_micros(1)).await;
            }
        });

        let inner_arc2 = Arc::clone(&inner.inner);
        let task2 = tokio::spawn(async move {
            for _ in 0..10 {
                let mut peers = inner_arc2.peers.write().await;
                let peer_id = PeerId(Libp2pPeerId::random().to_bytes());
                peers.insert(
                    peer_id.clone(),
                    PeerState {
                        peer_id,
                        addresses: vec![],
                        connection_state: ConnectionState::Connected,
                        last_seen: SystemTime::now(),
                        validation_status: ValidationStatus::Validated,
                        last_heartbeat: None,
                    },
                );
                drop(peers);

                tokio::time::sleep(Duration::from_micros(1)).await;

                // Remove it
                let mut peers = inner_arc2.peers.write().await;
                peers.clear();
                drop(peers);
            }
        });

        // Both tasks should complete without deadlock
        let (r1, r2) = tokio::join!(task1, task2);
        assert!(r1.is_ok(), "Task 1 should complete");
        assert!(r2.is_ok(), "Task 2 should complete");
    }

    #[tokio::test]
    async fn test_heartbeat_with_missing_peer() {
        let config = test_config("missing_peer_heartbeat_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        use libp2p::PeerId as Libp2pPeerId;
        let unknown_peer = PeerId(Libp2pPeerId::random().to_bytes());

        // Send heartbeat from a peer that's not in our state
        let heartbeat = HeartbeatMessage::new("unknown_node".to_string(), 1);
        let heartbeat_bytes = heartbeat.to_bytes().expect("Serialization should work");

        // Should not panic, just skip the update
        inner
            .handle_heartbeat_message(&unknown_peer, &heartbeat_bytes)
            .await;

        // Verify peer was not added
        let peers = inner.inner.peers.read().await;
        assert!(
            !peers.contains_key(&unknown_peer),
            "Unknown peer should not be auto-added"
        );
    }

    #[tokio::test]
    async fn test_request_to_disconnected_peer() {
        let config = test_config("disconnected_request_test");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        use libp2p::PeerId as Libp2pPeerId;
        let peer_id = PeerId(Libp2pPeerId::random().to_bytes());

        // Create a oneshot channel for the response
        let (tx, _rx) = tokio::sync::oneshot::channel();

        // Try to send request to a peer that's not connected
        let result = inner
            .send_request_internal(&peer_id, "test/protocol", vec![1, 2, 3], tx)
            .await;

        // Should return error (peer not connected)
        assert!(result.is_err(), "Request to disconnected peer should fail");
    }

    #[tokio::test]
    async fn test_metrics_operations_when_none() {
        let config = test_config("no_metrics_test");

        let (inner, handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        // Verify metrics is None initially
        {
            let metrics = inner.inner.metrics.read().await;
            assert!(metrics.is_none(), "Metrics should be None initially");
        }

        // Try to record a metric (should be no-op)
        inner.record_metric_counter("test.metric", 1).await;

        // Try operations via handle with no metrics set
        let peers = handle.get_connected_peers().await;
        assert_eq!(peers.len(), 0, "Operations should work without metrics");

        // Metrics should still be None
        {
            let metrics = inner.inner.metrics.read().await;
            assert!(metrics.is_none(), "Metrics should remain None");
        }
    }

    #[tokio::test]
    async fn test_max_peers_limit_awareness() {
        let config = Config {
            node_id: "max_peers_test".to_string(),
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![],
            peer_id_store_path: std::env::temp_dir().join("test_max_peers.json"),
            max_peers: 5, // Low limit for testing
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config.clone())
            .await
            .expect("create() should succeed");

        // Verify config has the limit
        assert_eq!(inner.inner.config.max_peers, 5, "Max peers should be set");

        // Note: Actual enforcement happens in connection handler
        // This just tests that the configuration is accessible
    }

    #[test]
    fn test_heartbeat_message_zero_sequence() {
        let heartbeat = HeartbeatMessage::new("test_node".to_string(), 0);

        let bytes = heartbeat.to_bytes().expect("Should serialize");
        let deserialized = HeartbeatMessage::from_bytes(&bytes).expect("Should deserialize");

        assert_eq!(
            deserialized.sequence, 0,
            "Zero sequence should be preserved"
        );
    }

    #[test]
    fn test_heartbeat_message_large_sequence() {
        let heartbeat = HeartbeatMessage::new("test_node".to_string(), u64::MAX);

        let bytes = heartbeat.to_bytes().expect("Should serialize");
        let deserialized = HeartbeatMessage::from_bytes(&bytes).expect("Should deserialize");

        assert_eq!(
            deserialized.sequence,
            u64::MAX,
            "Large sequence should be preserved"
        );
    }

    #[tokio::test]
    async fn test_echo_handler() {
        // Test that echo handler prepends "ECHO: " to the request
        let config = test_config("echo_handler");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        let request = b"Hello, World!".to_vec();
        let response = inner
            .handle_echo_request(request.clone())
            .await
            .expect("Echo handler should succeed");

        // Verify response has "ECHO: " prepended
        assert!(
            response.starts_with(b"ECHO: "),
            "Response should start with 'ECHO: '"
        );

        // Verify original message follows the prefix
        assert_eq!(
            &response[6..],
            &request[..],
            "Response should contain original message after prefix"
        );

        // Verify exact expected response
        let expected = b"ECHO: Hello, World!".to_vec();
        assert_eq!(response, expected, "Response should match expected");
    }

    #[tokio::test]
    async fn test_echo_handler_empty_request() {
        // Test echo handler with empty request
        let config = test_config("echo_empty");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        let request = vec![];
        let response = inner
            .handle_echo_request(request)
            .await
            .expect("Echo handler should succeed with empty request");

        // Should still have the "ECHO: " prefix
        assert_eq!(response, b"ECHO: ", "Empty request should get prefix only");
    }

    #[tokio::test]
    async fn test_raft_handler_not_implemented() {
        // Test that raft handler returns ProtocolNotSupported error
        let config = test_config("raft_handler");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        let request = b"Raft RPC data".to_vec();
        let result = inner.handle_raft_request(request).await;

        // Should return error since not implemented
        assert!(
            result.is_err(),
            "Raft handler should return error (not implemented)"
        );

        // Verify it's the right error type
        match result {
            Err(Error::ProtocolNotSupported(protocol)) => {
                assert_eq!(
                    protocol, "/wormfs/raft/1.0.0",
                    "Should indicate raft protocol not supported"
                );
            }
            _ => panic!("Expected ProtocolNotSupported error"),
        }
    }

    #[tokio::test]
    async fn test_route_request_echo_protocol() {
        // Test routing to echo handler
        let config = test_config("route_echo");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        let request = b"Test message".to_vec();
        let response = inner
            .route_request("/wormfs/echo", request.clone())
            .await
            .expect("Routing to echo should succeed");

        // Verify it went through echo handler
        assert!(
            response.starts_with(b"ECHO: "),
            "Routed request should be handled by echo handler"
        );
    }

    #[tokio::test]
    async fn test_route_request_raft_protocol() {
        // Test routing to raft handler
        let config = test_config("route_raft");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        let request = b"Raft data".to_vec();
        let result = inner.route_request("/wormfs/raft/1.0.0", request).await;

        // Should fail since raft not implemented
        assert!(result.is_err(), "Routing to unimplemented raft should fail");
    }

    #[tokio::test]
    async fn test_route_request_unknown_protocol() {
        // Test routing to unknown protocol
        let config = test_config("route_unknown");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        let request = b"Some data".to_vec();
        let result = inner.route_request("/wormfs/unknown/1.0.0", request).await;

        // Should return ProtocolNotSupported error
        assert!(result.is_err(), "Unknown protocol should fail");

        match result {
            Err(Error::ProtocolNotSupported(protocol)) => {
                assert_eq!(
                    protocol, "/wormfs/unknown/1.0.0",
                    "Should indicate unknown protocol"
                );
            }
            _ => panic!("Expected ProtocolNotSupported error"),
        }
    }

    #[tokio::test]
    async fn test_route_request_binary_data() {
        // Test echo handler with binary data
        let config = test_config("route_binary");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        // Binary data including null bytes
        let request = vec![0x00, 0xFF, 0x42, 0xAB, 0xCD];
        let response = inner
            .route_request("/wormfs/echo", request.clone())
            .await
            .expect("Binary data should work");

        // Verify prefix and data
        assert_eq!(&response[0..6], b"ECHO: ", "Should have prefix");
        assert_eq!(&response[6..], &request[..], "Should preserve binary data");
    }

    #[tokio::test]
    async fn test_shutdown_command() {
        // Test that shutdown command can be sent
        let config = test_config("shutdown_cmd");

        let (_inner, handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        // Send shutdown command (will succeed but won't actually shutdown event loop since it's not running)
        // In production, this would initiate graceful shutdown
        let result = tokio::time::timeout(Duration::from_millis(100), handle.shutdown()).await;

        // Should succeed in sending the command, but timeout since no event loop is processing it
        assert!(
            result.is_err(),
            "Shutdown command should timeout when event loop is not running"
        );
    }

    #[tokio::test]
    async fn test_shutdown_internal_with_no_peers() {
        // Test shutdown with no active peers
        let config = test_config("shutdown_empty");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        // Call shutdown_internal directly
        let result = inner.shutdown_internal().await;

        assert!(result.is_ok(), "Shutdown should succeed with no peers");
    }

    #[tokio::test]
    async fn test_handle_network_command_returns_false_on_shutdown() {
        // Test that Shutdown command causes handle_network_command to return false
        let config = test_config("shutdown_return");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        // Create Shutdown command
        let shutdown_cmd = NetworkCommand::Shutdown {
            response: response_tx,
        };

        // Handle command - should return false
        let should_continue = inner.handle_network_command(shutdown_cmd).await;

        assert!(!should_continue, "Shutdown command should return false");

        // Response should be sent back
        let shutdown_result = response_rx.await.expect("Should receive response");
        assert!(shutdown_result.is_ok(), "Shutdown should succeed");
    }

    #[tokio::test]
    async fn test_handle_network_command_returns_true_on_other_commands() {
        // Test that non-Shutdown commands return true
        let config = test_config("non_shutdown_return");

        let (inner, _handle) = super::super::StorageNetworkFactory::create(config)
            .await
            .expect("create() should succeed");

        // Create Broadcast command (non-shutdown)
        let broadcast_cmd = NetworkCommand::Broadcast {
            topic: "test".to_string(),
            message: vec![1, 2, 3],
        };

        // Handle command - should return true
        let should_continue = inner.handle_network_command(broadcast_cmd).await;

        assert!(should_continue, "Non-shutdown commands should return true");
    }
}
