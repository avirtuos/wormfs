//! StorageNetwork implementation with libp2p.
//!
//! This module provides the concrete implementation of the StorageNetwork trait,
//! including swarm initialization, peer management, and event handling.

#[cfg(feature = "libp2p")]
use crate::storage_network::{
    behaviour::{BehaviourConfig, WormFsBehaviour, WormFsCodec},
    types::*,
};
#[cfg(feature = "libp2p")]
use libp2p::{
    core::upgrade, gossipsub, identify, identity, noise, ping, request_response, tcp, yamux,
    PeerId as Libp2pPeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use std::collections::HashMap;
use std::iter;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info};

/// Factory for creating StorageNetwork instances.
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

/// Internal state shared between event loop and network handle.
pub struct InnerState {
    /// libp2p swarm
    pub(crate) swarm: RwLock<Swarm<WormFsBehaviour>>,

    /// Active peer states
    pub(crate) peers: RwLock<HashMap<PeerId, PeerState>>,

    /// Active topic subscriptions
    pub(crate) topics: RwLock<HashMap<String, TopicHandle>>,

    /// Network configuration
    pub(crate) config: Config,

    /// Command receiver for event loop
    pub(crate) event_rx: RwLock<mpsc::UnboundedReceiver<NetworkCommand>>,
}

/// StorageNetworkInner implementation
impl super::StorageNetworkInner {
    /// Start the network event loop.
    ///
    /// This method processes libp2p swarm events and network commands.
    /// It should be called exactly once and runs until shutdown.
    pub async fn run(&self) -> Result<(), Error> {
        info!("Starting StorageNetwork event loop");

        // TODO: Implement full event loop in Day 2
        // For now, just log that we're ready
        debug!("Event loop initialized, waiting for events");

        // Placeholder - actual implementation in Day 2
        Ok(())
    }
}

// Helper functions for converting between libp2p and internal types
fn libp2p_peer_id_to_internal(peer_id: &Libp2pPeerId) -> PeerId {
    PeerId::new(peer_id.to_bytes())
}

fn internal_peer_id_to_libp2p(peer_id: &PeerId) -> Result<Libp2pPeerId, Error> {
    Libp2pPeerId::from_bytes(peer_id.as_bytes())
        .map_err(|e| Error::ConfigError(format!("Invalid peer ID: {}", e)))
}

#[cfg(test)]
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
}
