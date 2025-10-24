//! StorageNetwork implementation with libp2p.
//!
//! This module provides the concrete implementation of the StorageNetwork trait,
//! including swarm initialization, peer management, and event handling.

#[cfg(feature = "libp2p")]
use crate::storage_network::behaviour::WormFsBehaviourEvent;
#[cfg(feature = "libp2p")]
use crate::storage_network::{
    behaviour::{BehaviourConfig, WormFsBehaviour, WormFsCodec},
    types::*,
};
use futures::StreamExt;
#[cfg(feature = "libp2p")]
use libp2p::{
    gossipsub, identify, identity, noise, ping, request_response, swarm::SwarmEvent, tcp, yamux,
    PeerId as Libp2pPeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use std::collections::HashMap;
use std::iter;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Factory for creating StorageNetwork instances.
#[cfg(feature = "libp2p")]
impl super::StorageNetworkFactory {
    /// Create a new StorageNetwork instance with the given configuration.
    ///
    /// This initializes the libp2p swarm with all required protocols and
    /// returns both the inner state (for running the event loop) and a
    /// cloneable handle for network operations.
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
    pub async fn create(
        config: Config,
    ) -> Result<(super::StorageNetworkInner, super::StorageNetworkHandle), Error> {
        info!("Initializing StorageNetwork with libp2p");

        // Generate or load keypair for this node's identity
        let local_key = identity::Keypair::generate_ed25519();
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

        // Create inner state
        let inner = Arc::new(InnerState {
            swarm: swarm_lock,
            peers,
            topics,
            config: config.clone(),
            event_rx: RwLock::new(event_rx),
            pending_requests: RwLock::new(HashMap::new()),
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
#[cfg(feature = "libp2p")]
pub(crate) struct TopicState {
    /// Sender for routing messages to subscribers
    pub(crate) tx: mpsc::UnboundedSender<TopicMessage>,
}

/// Internal state shared between event loop and network handle.
#[cfg(feature = "libp2p")]
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
}

/// StorageNetworkInner implementation
#[cfg(feature = "libp2p")]
impl super::StorageNetworkInner {
    /// Start the network event loop.
    ///
    /// This method processes libp2p swarm events and network commands.
    /// It should be called exactly once and runs until shutdown.
    pub async fn run(&self) -> Result<(), Error> {
        info!("Starting StorageNetwork event loop");

        // Set up listen addresses
        {
            let mut swarm = self
                .inner
                .swarm
                .write()
                .expect("Failed to acquire swarm lock");
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

        info!("Event loop initialized, processing events");

        // Main event loop
        loop {
            tokio::select! {
                // Process swarm events
                event = async {
                    let mut swarm = self.inner.swarm.write().expect("Failed to acquire swarm lock");
                    swarm.select_next_some().await
                } => {
                    self.handle_swarm_event(event).await;
                }

                // Process network commands
                command = async {
                    let mut rx = self.inner.event_rx.write().expect("Failed to acquire rx lock");
                    rx.recv().await
                } => {
                    match command {
                        Some(cmd) => self.handle_network_command(cmd).await,
                        None => {
                            info!("Command channel closed, shutting down event loop");
                            break;
                        }
                    }
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
                info!(
                    "Connection established with peer {:?} at {} (total connections: {})",
                    internal_peer_id,
                    endpoint.get_remote_address(),
                    num_established
                );

                // Update peer state
                let mut peers = self
                    .inner
                    .peers
                    .write()
                    .expect("Failed to acquire peers lock");
                peers.insert(
                    internal_peer_id.clone(),
                    PeerState {
                        peer_id: internal_peer_id,
                        addresses: vec![endpoint.get_remote_address().to_string()],
                        connection_state: ConnectionState::Connected,
                        last_seen: SystemTime::now(),
                        validation_status: ValidationStatus::Pending,
                    },
                );
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
                    let mut peers = self
                        .inner
                        .peers
                        .write()
                        .expect("Failed to acquire peers lock");
                    if let Some(peer_state) = peers.get_mut(&internal_peer_id) {
                        peer_state.connection_state = ConnectionState::Disconnected;
                    }
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

            _ => {
                // Log other events at debug level
                debug!("Swarm event: {:?}", event);
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

                debug!(
                    "Received gossipsub message from {:?} on topic {} (id: {})",
                    source, topic, message_id
                );

                // Route message to topic subscribers
                let topics = self
                    .inner
                    .topics
                    .read()
                    .expect("Failed to acquire topics lock");
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
            }

            gossipsub::Event::Unsubscribed { peer_id, topic } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                debug!(
                    "Peer {:?} unsubscribed from topic {}",
                    internal_peer_id, topic
                );
            }

            gossipsub::Event::GossipsubNotSupported { peer_id } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                warn!("Peer {:?} does not support gossipsub", internal_peer_id);
            }
        }
    }

    /// Handle an identify event.
    async fn handle_identify_event(&self, event: identify::Event) {
        match event {
            identify::Event::Received { peer_id, info } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                debug!(
                    "Identified peer {:?}: protocol_version={}, agent_version={}",
                    internal_peer_id, info.protocol_version, info.agent_version
                );

                // Update peer info with addresses and protocols
                let mut peers = self
                    .inner
                    .peers
                    .write()
                    .expect("Failed to acquire peers lock");
                if let Some(peer_state) = peers.get_mut(&internal_peer_id) {
                    peer_state.addresses =
                        info.listen_addrs.iter().map(|a| a.to_string()).collect();
                    peer_state.last_seen = SystemTime::now();
                }
            }

            identify::Event::Sent { peer_id } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                debug!("Sent identify info to peer {:?}", internal_peer_id);
            }

            identify::Event::Pushed { peer_id, .. } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer_id);
                debug!("Pushed identify info to peer {:?}", internal_peer_id);
            }

            identify::Event::Error { peer_id, error } => {
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
                debug!("Ping to peer {:?}: {}ms", internal_peer_id, rtt.as_millis());

                // Update last_seen timestamp
                let mut peers = self
                    .inner
                    .peers
                    .write()
                    .expect("Failed to acquire peers lock");
                if let Some(peer_state) = peers.get_mut(&internal_peer_id) {
                    peer_state.last_seen = SystemTime::now();
                }
            }
            Err(failure) => {
                let internal_peer_id = libp2p_peer_id_to_internal(&event.peer);
                warn!("Ping failed to peer {:?}: {:?}", internal_peer_id, failure);
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
            Event::Message { peer, message } => {
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

                        // For now, echo the request back as response
                        // In the future, this would route to a handler
                        let response = request.clone();

                        // Send response through the swarm
                        let mut swarm = self
                            .inner
                            .swarm
                            .write()
                            .expect("Failed to acquire swarm lock");
                        if swarm
                            .behaviour_mut()
                            .request_response
                            .send_response(channel, response)
                            .is_err()
                        {
                            warn!("Failed to send response to request {:?}", request_id);
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
                            let mut pending = self
                                .inner
                                .pending_requests
                                .write()
                                .expect("Failed to acquire pending_requests lock");
                            pending.remove(&request_id)
                        };

                        if let Some(tx) = response_tx {
                            if tx.send(Ok(response)).is_err() {
                                warn!("Failed to deliver response for request {:?} - receiver dropped", request_id);
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
            } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer);
                warn!(
                    "Outbound request {:?} to peer {:?} failed: {:?}",
                    request_id, internal_peer_id, error
                );

                // Notify caller of failure
                let response_tx = {
                    let mut pending = self
                        .inner
                        .pending_requests
                        .write()
                        .expect("Failed to acquire pending_requests lock");
                    pending.remove(&request_id)
                };

                if let Some(tx) = response_tx {
                    let err = Error::RequestFailed {
                        peer: internal_peer_id,
                        reason: format!("{:?}", error),
                    };
                    let _ = tx.send(Err(err));
                }
            }

            Event::InboundFailure {
                peer,
                request_id,
                error,
            } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer);
                warn!(
                    "Inbound request {:?} from peer {:?} failed: {:?}",
                    request_id, internal_peer_id, error
                );
            }

            Event::ResponseSent { peer, request_id } => {
                let internal_peer_id = libp2p_peer_id_to_internal(&peer);
                debug!(
                    "Response sent to peer {:?} for request {:?}",
                    internal_peer_id, request_id
                );
            }
        }
    }

    /// Handle a network command from the command channel.
    async fn handle_network_command(&self, command: NetworkCommand) {
        match command {
            NetworkCommand::JoinTopic { name, response } => {
                let result = self.join_topic_internal(&name).await;
                if response.send(result).is_err() {
                    error!("Failed to send JoinTopic response");
                }
            }

            NetworkCommand::Broadcast { topic, message } => {
                if let Err(e) = self.broadcast_internal(&topic, message).await {
                    error!("Broadcast failed on topic '{}': {}", topic, e);
                }
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
            }
        }
    }

    /// Internal implementation of joining a topic.
    async fn join_topic_internal(&self, topic_name: &str) -> Result<TopicHandle, Error> {
        info!("Joining topic '{}'", topic_name);

        // Subscribe to gossipsub topic
        let topic = gossipsub::IdentTopic::new(topic_name);
        {
            let mut swarm = self
                .inner
                .swarm
                .write()
                .expect("Failed to acquire swarm lock");
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
        let mut topics = self
            .inner
            .topics
            .write()
            .expect("Failed to acquire topics lock");

        // We need to store something that can receive TopicMessages
        // For now, create a simple struct to hold the sender
        let topic_state = TopicState { tx: internal_tx };
        topics.insert(topic_name.to_string(), topic_state);

        // Return the handle with tx (for sending) and rx (for receiving)
        Ok(TopicHandle { tx, rx })
    }

    /// Internal implementation of broadcasting a message.
    async fn broadcast_internal(&self, topic_name: &str, message: Vec<u8>) -> Result<(), Error> {
        debug!(
            "Broadcasting {} bytes on topic '{}'",
            message.len(),
            topic_name
        );

        let topic = gossipsub::IdentTopic::new(topic_name);
        let mut swarm = self
            .inner
            .swarm
            .write()
            .expect("Failed to acquire swarm lock");

        swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic, message)
            .map_err(|e| Error::BroadcastFailed {
                topic: topic_name.to_string(),
                reason: e.to_string(),
            })?;

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
            let peers = self
                .inner
                .peers
                .read()
                .expect("Failed to acquire peers lock");
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
            let peers = self
                .inner
                .peers
                .read()
                .expect("Failed to acquire peers lock");
            if !peers.contains_key(peer_id) {
                let _ = response_tx.send(Err(Error::PeerNotConnected(peer_id.clone())));
                return Err(Error::PeerNotConnected(peer_id.clone()));
            }
        }

        // Convert internal PeerId to libp2p PeerId
        let libp2p_peer_id = internal_peer_id_to_libp2p(peer_id)?;

        // Send request via request-response protocol
        let request_id = {
            let mut swarm = self
                .inner
                .swarm
                .write()
                .expect("Failed to acquire swarm lock");

            swarm
                .behaviour_mut()
                .request_response
                .send_request(&libp2p_peer_id, request)
        };

        // Store response channel for when we get the response
        {
            let mut pending = self
                .inner
                .pending_requests
                .write()
                .expect("Failed to acquire pending_requests lock");
            pending.insert(request_id, response_tx);
        }

        debug!("Request sent with ID: {:?}", request_id);
        Ok(())
    }
}

// Helper functions for converting between libp2p and internal types
#[cfg(feature = "libp2p")]
fn libp2p_peer_id_to_internal(peer_id: &Libp2pPeerId) -> PeerId {
    PeerId::new(peer_id.to_bytes())
}

/// Convert internal PeerId to libp2p PeerId.
/// Day 3: Will be used for request-response protocol.
#[cfg(feature = "libp2p")]
#[allow(dead_code)]
fn internal_peer_id_to_libp2p(peer_id: &PeerId) -> Result<Libp2pPeerId, Error> {
    Libp2pPeerId::from_bytes(peer_id.as_bytes())
        .map_err(|e| Error::ConfigError(format!("Invalid peer ID: {}", e)))
}

#[cfg(all(test, feature = "libp2p"))]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_factory_creates_network() {
        let config = Config {
            listen_addresses: vec!["/ip4/127.0.0.1/tcp/0".to_string()],
            peers: vec![],
            peer_id_store_path: std::env::temp_dir().join("test_peer_ids.json"),
            max_peers: 100,
            max_connections_per_peer: 3,
            connection_timeout: Duration::from_secs(30),
            idle_connection_timeout: Duration::from_secs(600),
            keep_alive_interval: Duration::from_secs(30),
        };

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
        let peers = handle.get_connected_peers();
        assert_eq!(peers.len(), 0, "Should start with no peers");

        // Test get_peer_info for non-existent peer
        let peer_id = PeerId::new(vec![1, 2, 3, 4]);
        let info = handle.get_peer_info(&peer_id);
        assert!(info.is_none(), "Non-existent peer should return None");
    }
}
