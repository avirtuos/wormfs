//! Network transport layer for WormFS
//!
//! This module provides the network transport implementation for Raft consensus
//! and other distributed operations. It uses libp2p for peer-to-peer networking
//! with support for:
//!
//! - TCP transport with noise encryption
//! - Static peer configuration
//! - Request-response protocol for Raft RPCs
//! - Peer health monitoring and connection management
//! - Automatic reconnection on failures

pub mod codec;
pub mod libp2p_network;
pub mod peer_manager;
pub mod protocol;

pub use codec::{
    decode_raft_request, decode_raft_response, encode_raft_request, encode_raft_response,
};
pub use libp2p_network::{Libp2pNetwork, NetworkConfig};
pub use peer_manager::{PeerHealth, PeerManager, PeerStatus};
pub use protocol::{RaftCodec, RaftProtocol};

use std::fmt;

/// Network transport error types
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Peer not found: {0}")]
    PeerNotFound(u64),

    #[error("Timeout waiting for response")]
    Timeout,

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, TransportError>;

/// Peer information for static configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerInfo {
    /// Node ID
    pub node_id: u64,

    /// Network address (e.g., "/ip4/127.0.0.1/tcp/3000")
    pub address: String,

    /// Optional libp2p PeerId (Base58 encoded)
    /// Required when allow_peer_discovery=false
    /// Optional when allow_peer_discovery=true
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<String>,
}

impl fmt::Display for PeerInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Peer {} @ {}", self.node_id, self.address)
    }
}
