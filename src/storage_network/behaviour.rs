//! libp2p network behavior for WormFS.
//!
//! This module defines the combined network behavior that integrates multiple
//! libp2p protocols for peer-to-peer communication in WormFS.

#[cfg(feature = "libp2p")]
use libp2p::swarm::NetworkBehaviour;
#[cfg(feature = "libp2p")]
use libp2p::{gossipsub, identify, ping, request_response, StreamProtocol};
use std::time::Duration;

/// Combined network behavior for WormFS.
///
/// This behavior integrates multiple libp2p protocols:
/// - Gossipsub: Topic-based pub/sub messaging for broadcasts
/// - RequestResponse: Direct RPC for point-to-point communication
/// - Identify: Peer information exchange and version negotiation
/// - Ping: Keep-alive and connection health monitoring
///
/// Note: mDNS is explicitly excluded per design decision to rely on
/// configured peer lists and gossip for peer discovery.
#[cfg(feature = "libp2p")]
#[derive(NetworkBehaviour)]
pub struct WormFsBehaviour {
    /// Gossipsub for topic-based pub/sub messaging.
    ///
    /// Used for broadcasting metadata updates, Raft heartbeats,
    /// and cluster membership changes.
    pub gossipsub: gossipsub::Behaviour,

    /// Request-response protocol for direct peer communication.
    ///
    /// Used for Raft RPCs (AppendEntries, RequestVote) and
    /// other point-to-point messaging that requires responses.
    pub request_response: request_response::Behaviour<WormFsCodec>,

    /// Identify protocol for peer info exchange.
    ///
    /// Enables nodes to exchange protocol versions, supported
    /// features, and peer addresses.
    pub identify: identify::Behaviour,

    /// Ping protocol for keep-alive and latency measurement.
    ///
    /// Maintains connections and provides RTT metrics for peers.
    pub ping: ping::Behaviour,
}

/// Codec for WormFS request-response protocol.
///
/// Handles serialization and deserialization of messages using bincode.
/// Implements size limits to prevent memory exhaustion from malicious peers.
#[cfg(feature = "libp2p")]
#[derive(Clone, Debug)]
pub struct WormFsCodec {
    /// Maximum message size (10MB default per design doc)
    max_message_size: usize,
}

#[cfg(feature = "libp2p")]
impl Default for WormFsCodec {
    fn default() -> Self {
        Self {
            // 10MB max message size per design doc
            max_message_size: 10 * 1024 * 1024,
        }
    }
}

#[cfg(feature = "libp2p")]
impl WormFsCodec {
    /// Create a new codec with the specified maximum message size.
    ///
    /// # Arguments
    ///
    /// * `max_message_size` - Maximum allowed message size in bytes
    pub fn new(max_message_size: usize) -> Self {
        Self { max_message_size }
    }

    /// Get the maximum message size.
    pub fn max_message_size(&self) -> usize {
        self.max_message_size
    }
}

// Placeholder for request-response codec implementation
// This will be fully implemented in Day 3
#[cfg(feature = "libp2p")]
#[async_trait::async_trait]
impl request_response::Codec for WormFsCodec {
    type Protocol = StreamProtocol;
    type Request = Vec<u8>;
    type Response = Vec<u8>;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: futures::AsyncRead + Unpin + Send,
    {
        use futures::AsyncReadExt;

        // Read message length (4 bytes)
        let mut len_bytes = [0u8; 4];
        io.read_exact(&mut len_bytes).await?;
        let len = u32::from_be_bytes(len_bytes) as usize;

        // Check size limit
        if len > self.max_message_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Message size {} exceeds limit {}",
                    len, self.max_message_size
                ),
            ));
        }

        // Read message data
        let mut data = vec![0u8; len];
        io.read_exact(&mut data).await?;
        Ok(data)
    }

    async fn read_response<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: futures::AsyncRead + Unpin + Send,
    {
        // Same implementation as read_request
        self.read_request(protocol, io).await
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> std::io::Result<()>
    where
        T: futures::AsyncWrite + Unpin + Send,
    {
        use futures::AsyncWriteExt;

        // Check size limit
        if req.len() > self.max_message_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Message size {} exceeds limit {}",
                    req.len(),
                    self.max_message_size
                ),
            ));
        }

        // Write message length
        let len = req.len() as u32;
        io.write_all(&len.to_be_bytes()).await?;

        // Write message data
        io.write_all(&req).await?;
        io.flush().await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> std::io::Result<()>
    where
        T: futures::AsyncWrite + Unpin + Send,
    {
        // Same implementation as write_request
        self.write_request(protocol, io, res).await
    }
}

/// Configuration for WormFS network behavior.
#[cfg(feature = "libp2p")]
pub struct BehaviourConfig {
    /// Gossipsub configuration
    pub gossipsub: gossipsub::Config,

    /// Request-response timeout
    pub request_timeout: Duration,

    /// Maximum request-response message size
    pub max_message_size: usize,
}

#[cfg(feature = "libp2p")]
impl Default for BehaviourConfig {
    fn default() -> Self {
        use std::hash::{Hash, Hasher};

        // Message ID function for deduplication
        let message_id_fn = |message: &gossipsub::Message| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            message.data.hash(&mut hasher);
            message.source.hash(&mut hasher);
            gossipsub::MessageId::from(hasher.finish().to_string())
        };

        Self {
            gossipsub: gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(1))
                .history_length(5)
                .history_gossip(3)
                .mesh_n(6)
                .mesh_n_low(4)
                .mesh_n_high(12)
                .message_id_fn(message_id_fn)
                .build()
                .expect("Valid gossipsub config"),
            request_timeout: Duration::from_secs(5),
            max_message_size: 10 * 1024 * 1024, // 10MB
        }
    }
}
