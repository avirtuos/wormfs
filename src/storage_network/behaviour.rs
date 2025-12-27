//! libp2p network behavior for WormFS.
//!
//! This module defines the combined network behavior that integrates multiple
//! libp2p protocols for peer-to-peer communication in WormFS.

use libp2p::swarm::NetworkBehaviour;
use libp2p::{gossipsub, identify, ping, request_response, StreamProtocol};
use std::time::Duration;

// Protocol Configuration Constants

/// Default maximum message size for request-response protocol (10MB).
///
/// This limit prevents memory exhaustion from malicious peers sending
/// excessively large messages. Based on WormFS design specification.
const DEFAULT_MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024;

/// Default request-response timeout in seconds.
///
/// Maximum time to wait for a response to a request before timing out.
const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 5;

// Gossipsub Protocol Constants

/// Gossipsub heartbeat interval in seconds.
///
/// How frequently the gossipsub protocol performs maintenance operations
/// like mesh management and sending control messages.
const GOSSIPSUB_HEARTBEAT_SECS: u64 = 1;

/// Gossipsub message history length.
///
/// Number of heartbeat intervals to retain message IDs for deduplication.
/// Higher values improve deduplication but increase memory usage.
const GOSSIPSUB_HISTORY_LENGTH: usize = 5;

/// Gossipsub history gossip rounds.
///
/// Number of past heartbeat intervals to gossip about during each heartbeat.
/// Helps with message propagation in case of network partitions.
const GOSSIPSUB_HISTORY_GOSSIP: usize = 3;

/// Gossipsub target mesh size (D parameter).
///
/// Target number of peers to maintain in the mesh for each topic.
/// Balances message redundancy with bandwidth usage.
const GOSSIPSUB_MESH_N: usize = 6;

/// Gossipsub minimum mesh size (D_lo parameter).
///
/// Minimum peers in mesh before grafting new connections.
/// Lower bound to maintain message delivery reliability.
const GOSSIPSUB_MESH_N_LOW: usize = 4;

/// Gossipsub maximum mesh size (D_hi parameter).
///
/// Maximum peers in mesh before pruning connections.
/// Upper bound to prevent excessive message duplication.
const GOSSIPSUB_MESH_N_HIGH: usize = 12;

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
#[derive(Clone, Debug)]
pub struct WormFsCodec {
    /// Maximum message size (10MB default per design doc)
    max_message_size: usize,
}

impl Default for WormFsCodec {
    fn default() -> Self {
        Self {
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }
}

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
pub struct BehaviourConfig {
    /// Gossipsub configuration
    pub gossipsub: gossipsub::Config,

    /// Request-response timeout
    pub request_timeout: Duration,

    /// Maximum request-response message size
    pub max_message_size: usize,
}

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
                .heartbeat_interval(Duration::from_secs(GOSSIPSUB_HEARTBEAT_SECS))
                .history_length(GOSSIPSUB_HISTORY_LENGTH)
                .history_gossip(GOSSIPSUB_HISTORY_GOSSIP)
                .mesh_n(GOSSIPSUB_MESH_N)
                .mesh_n_low(GOSSIPSUB_MESH_N_LOW)
                .mesh_n_high(GOSSIPSUB_MESH_N_HIGH)
                .max_transmit_size(DEFAULT_MAX_MESSAGE_SIZE) // Allow large messages up to 10MB
                .message_id_fn(message_id_fn)
                .build()
                .expect("Valid gossipsub config"),
            request_timeout: Duration::from_secs(DEFAULT_REQUEST_TIMEOUT_SECS),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::Cursor;
    use request_response::Codec;

    #[test]
    fn test_codec_default_max_size() {
        let codec = WormFsCodec::default();
        assert_eq!(
            codec.max_message_size(),
            DEFAULT_MAX_MESSAGE_SIZE,
            "Default max message size should be 10MB"
        );
    }

    #[test]
    fn test_codec_custom_max_size() {
        let custom_size = 5 * 1024 * 1024; // 5MB
        let codec = WormFsCodec::new(custom_size);
        assert_eq!(
            codec.max_message_size(),
            custom_size,
            "Custom max message size should be respected"
        );
    }

    #[tokio::test]
    async fn test_codec_read_write_roundtrip() {
        let mut codec = WormFsCodec::default();
        let test_data = vec![1, 2, 3, 4, 5, 42, 255, 0];

        // Write request
        let mut write_buf = Vec::new();
        codec
            .write_request(
                &StreamProtocol::new("/test"),
                &mut write_buf,
                test_data.clone(),
            )
            .await
            .expect("Write should succeed");

        // Verify format: 4 bytes length + data
        assert_eq!(write_buf.len(), 4 + test_data.len());
        assert_eq!(
            u32::from_be_bytes([write_buf[0], write_buf[1], write_buf[2], write_buf[3]]),
            test_data.len() as u32
        );

        // Read request back
        let mut read_cursor = Cursor::new(write_buf);
        let read_data = codec
            .read_request(&StreamProtocol::new("/test"), &mut read_cursor)
            .await
            .expect("Read should succeed");

        assert_eq!(read_data, test_data, "Roundtrip should preserve data");
    }

    #[tokio::test]
    async fn test_codec_size_limit_enforcement_write() {
        let mut codec = WormFsCodec::new(100); // 100 bytes max
        let oversized_data = vec![0u8; 101]; // 101 bytes

        let mut write_buf = Vec::new();
        let result = codec
            .write_request(
                &StreamProtocol::new("/test"),
                &mut write_buf,
                oversized_data,
            )
            .await;

        assert!(result.is_err(), "Should reject oversized write");
        assert!(
            result.unwrap_err().to_string().contains("exceeds limit"),
            "Error should mention size limit"
        );
    }

    #[tokio::test]
    async fn test_codec_size_limit_enforcement_read() {
        let mut codec = WormFsCodec::new(100); // 100 bytes max

        // Manually create a message that claims to be 101 bytes
        let mut malicious_data = Vec::new();
        malicious_data.extend_from_slice(&(101u32).to_be_bytes()); // Claim 101 bytes
        malicious_data.extend_from_slice(&[0u8; 101]); // Actual data

        let mut read_cursor = Cursor::new(malicious_data);
        let result = codec
            .read_request(&StreamProtocol::new("/test"), &mut read_cursor)
            .await;

        assert!(result.is_err(), "Should reject oversized read");
        assert!(
            result.unwrap_err().to_string().contains("exceeds limit"),
            "Error should mention size limit"
        );
    }

    #[tokio::test]
    async fn test_codec_empty_message() {
        let mut codec = WormFsCodec::default();
        let empty_data = vec![];

        // Write empty message
        let mut write_buf = Vec::new();
        codec
            .write_request(
                &StreamProtocol::new("/test"),
                &mut write_buf,
                empty_data.clone(),
            )
            .await
            .expect("Empty message write should succeed");

        // Read it back
        let mut read_cursor = Cursor::new(write_buf);
        let read_data = codec
            .read_request(&StreamProtocol::new("/test"), &mut read_cursor)
            .await
            .expect("Empty message read should succeed");

        assert_eq!(read_data, empty_data, "Empty message should roundtrip");
    }

    #[tokio::test]
    async fn test_codec_max_size_boundary() {
        let max_size = 1000;
        let mut codec = WormFsCodec::new(max_size);
        let boundary_data = vec![0u8; max_size]; // Exactly at limit

        // Write at boundary
        let mut write_buf = Vec::new();
        codec
            .write_request(
                &StreamProtocol::new("/test"),
                &mut write_buf,
                boundary_data.clone(),
            )
            .await
            .expect("Write at exact limit should succeed");

        // Read at boundary
        let mut read_cursor = Cursor::new(write_buf);
        let read_data = codec
            .read_request(&StreamProtocol::new("/test"), &mut read_cursor)
            .await
            .expect("Read at exact limit should succeed");

        assert_eq!(
            read_data.len(),
            max_size,
            "Should handle messages at exact size limit"
        );
    }

    #[test]
    fn test_behaviour_config_default() {
        let config = BehaviourConfig::default();

        // Verify request-response timeout
        assert_eq!(
            config.request_timeout,
            Duration::from_secs(DEFAULT_REQUEST_TIMEOUT_SECS),
            "Default request timeout should be 5 seconds"
        );

        // Verify max message size
        assert_eq!(
            config.max_message_size, DEFAULT_MAX_MESSAGE_SIZE,
            "Default max message size should be 10MB"
        );
    }

    #[test]
    fn test_behaviour_config_gossipsub_params() {
        let config = BehaviourConfig::default();

        // Verify gossipsub config was built successfully
        // Note: We can't easily inspect the gossipsub config directly,
        // but we can verify it was created without panicking
        assert!(
            config.gossipsub.heartbeat_interval() == Duration::from_secs(GOSSIPSUB_HEARTBEAT_SECS),
            "Gossipsub heartbeat should be 1 second"
        );
    }
}
