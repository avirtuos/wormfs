//! Network handle trait for abstracting over real and test network implementations.
//!
//! This module provides the `NetworkHandleTrait` which allows the Raft implementation
//! to work with both production libp2p networking and test stub implementations.

use super::types::{Error, PeerInfo};
use crate::storage_raft_member::raft_member::RaftRpcHandler;
use async_trait::async_trait;
use std::sync::Arc;

/// Trait for network handles that can send Raft RPCs.
///
/// This trait is implemented by both:
/// - `StorageNetworkHandle` (production libp2p networking)
/// - `StubStorageNetworkHandle` (test in-memory channels)
///
/// By using this trait, the Raft implementation can work with either network type.
#[async_trait]
pub trait NetworkHandleTrait: Send + Sync {
    /// Send a request to a peer and wait for a response.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - The peer to send to (as bytes, libp2p compatible)
    /// * `protocol` - The protocol string (e.g., "/wormfs/raft/1.0.0")
    /// * `request` - The serialized request payload
    ///
    /// # Returns
    ///
    /// The serialized response payload, or an error.
    async fn send_request(
        &self,
        peer_id_bytes: &[u8],
        protocol: &str,
        request: Vec<u8>,
    ) -> Result<Vec<u8>, Error>;

    /// Register a Raft RPC handler for receiving RPCs.
    ///
    /// # Arguments
    ///
    /// * `handler` - The handler that will process incoming Raft RPCs
    async fn register_raft_handler(&self, handler: Arc<dyn RaftRpcHandler>) -> Result<(), Error>;

    /// Get the list of currently connected peers.
    ///
    /// # Returns
    ///
    /// A vector of peer information for all connected peers.
    async fn get_connected_peers(&self) -> Result<Vec<PeerInfo>, Error>;

    /// Attempt to dial configured peers.
    ///
    /// This triggers connection attempts to all configured peer addresses.
    async fn dial_configured_peers(&self) -> Result<(), Error>;
}
