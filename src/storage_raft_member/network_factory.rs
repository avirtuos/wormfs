//! RaftNetworkFactory implementation for creating RaftMember instances.
//!
//! This module implements OpenRaft's RaftNetworkFactory trait to create lightweight
//! RaftMember instances for communicating with specific Raft cluster members.
//!
//! ## Architecture
//!
//! The factory creates one RaftMember instance per target node. Each instance:
//! - Knows the target node's PeerId and NodeId
//! - Shares a reference to the StorageNetworkHandle (lightweight clone)
//! - Uses the existing libp2p request-response protocol
//!
//! This pattern allows OpenRaft to maintain per-node network connections while
//! reusing a single underlying libp2p network infrastructure.

use libp2p::PeerId;
use openraft::network::RaftNetworkFactory;
use tracing::debug;

use crate::storage_network::StorageNetworkHandle;

use super::raft_config::{WormFsNode, WormFsTypeConfig};
use super::raft_member::RaftMember;
use super::types::NodeId;

/// Factory for creating RaftMember instances.
///
/// This implements OpenRaft's RaftNetworkFactory trait, which creates lightweight
/// network client instances for communicating with specific Raft cluster members.
///
/// # Design
///
/// The factory holds a shared `StorageNetworkHandle` and creates `RaftMember`
/// instances that each know their target node. All instances share the same
/// underlying libp2p network, making this a very lightweight operation.
#[derive(Clone)]
pub struct WormFsNetworkFactory {
    /// Shared handle to the storage network
    network: StorageNetworkHandle,
}

impl WormFsNetworkFactory {
    /// Create a new network factory.
    ///
    /// # Arguments
    ///
    /// * `network` - The storage network handle to use for all network operations
    pub fn new(network: StorageNetworkHandle) -> Self {
        Self { network }
    }
}

impl RaftNetworkFactory<WormFsTypeConfig> for WormFsNetworkFactory {
    type Network = RaftMember;

    /// Create a new network client for communicating with a specific target node.
    ///
    /// # Arguments
    ///
    /// * `target` - The NodeId of the target Raft member
    /// * `node` - Node metadata (contains PeerId)
    ///
    /// # Returns
    ///
    /// A new `RaftMember` instance configured to communicate with the target node.
    ///
    /// # Panics
    ///
    /// Panics if the PeerId cannot be parsed. This should not happen in practice as
    /// PeerIds are validated when nodes join the cluster.
    async fn new_client(&mut self, target: NodeId, node: &WormFsNode) -> Self::Network {
        // Parse the PeerId
        let target_peer_id = node
            .peer_id
            .parse::<PeerId>()
            .expect("Invalid PeerId in node metadata");

        debug!(
            "Created RaftMember for target {:?} with PeerId {}",
            target, target_peer_id
        );

        RaftMember::new(target, target_peer_id, self.network.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_factory_creation() {
        // This test just verifies the factory can be created
        // Full testing requires a running StorageNetwork
        // which is covered by integration tests
    }
}
