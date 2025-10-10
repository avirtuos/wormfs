//! RaftNetwork adapter for integrating libp2p StorageNetwork with OpenRaft
//!
//! This module provides the bridge between OpenRaft's RaftNetwork trait and
//! our libp2p-based StorageNetwork implementation.

use async_trait::async_trait;
use openraft::{error::RPCError, RaftNetwork, RaftNetworkFactory};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::raft::proto_types::proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use crate::raft::types::WormFSTypeConfig;
use crate::transport::{StorageNetwork, TransportError};

/// Adapter that implements OpenRaft's RaftNetwork trait using our StorageNetwork
pub struct WormFSRaftNetwork {
    storage_network: Arc<Mutex<StorageNetwork>>,
    node_id: u64,
}

impl WormFSRaftNetwork {
    /// Create a new RaftNetwork adapter
    pub fn new(storage_network: Arc<Mutex<StorageNetwork>>, node_id: u64) -> Self {
        debug!("Creating WormFSRaftNetwork for node {}", node_id);
        Self {
            storage_network,
            node_id,
        }
    }

    /// Convert TransportError to RPCError
    fn convert_error<E>(error: TransportError) -> RPCError<u64, (), E> {
        warn!("Transport error in RaftNetwork: {}", error);
        match error {
            TransportError::Timeout => RPCError::Timeout(openraft::error::Timeout {
                action: openraft::error::RPCTypes::All,
                id: 0,
                target: 0,
                timeout: std::time::Duration::from_secs(5),
            }),
            TransportError::Network(msg) => {
                RPCError::Network(openraft::error::NetworkError::new(&msg))
            }
            TransportError::PeerNotFound { .. } => {
                RPCError::Network(openraft::error::NetworkError::new(&error.to_string()))
            }
            _ => RPCError::Network(openraft::error::NetworkError::new(&error.to_string())),
        }
    }
}

#[async_trait]
impl RaftNetwork<WormFSTypeConfig> for WormFSRaftNetwork {
    async fn append_entries(
        &mut self,
        target: u64,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RPCError<u64, (), openraft::error::Infallible>> {
        debug!(
            "Sending AppendEntries to node {} (term: {}, {} entries)",
            target,
            req.term,
            req.entries.len()
        );

        let network = self.storage_network.lock().await;
        network
            .send_append_entries(target, req)
            .await
            .map_err(Self::convert_error)
    }

    async fn install_snapshot(
        &mut self,
        target: u64,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RPCError<u64, (), openraft::error::Infallible>> {
        debug!(
            "Sending InstallSnapshot to node {} (snapshot: {})",
            target, req.snapshot_id
        );

        let network = self.storage_network.lock().await;
        network
            .send_install_snapshot(target, req)
            .await
            .map_err(Self::convert_error)
    }

    async fn vote(
        &mut self,
        target: u64,
        req: VoteRequest,
    ) -> Result<VoteResponse, RPCError<u64, (), openraft::error::Infallible>> {
        debug!(
            "Sending Vote request to node {} (term: {}, candidate: {})",
            target, req.term, req.candidate_id
        );

        let network = self.storage_network.lock().await;
        network
            .send_vote(target, req)
            .await
            .map_err(Self::convert_error)
    }
}

/// Factory for creating WormFSRaftNetwork instances
/// OpenRaft requires a factory pattern for creating network instances per peer
pub struct WormFSRaftNetworkFactory {
    storage_network: Arc<Mutex<StorageNetwork>>,
    node_id: u64,
}

impl WormFSRaftNetworkFactory {
    /// Create a new network factory
    pub fn new(storage_network: Arc<Mutex<StorageNetwork>>, node_id: u64) -> Self {
        Self {
            storage_network,
            node_id,
        }
    }
}

#[async_trait]
impl RaftNetworkFactory<WormFSTypeConfig> for WormFSRaftNetworkFactory {
    type Network = WormFSRaftNetwork;

    async fn new_client(&mut self, _target: u64, _node: &()) -> Self::Network {
        // OpenRaft calls this to create a network instance for communicating with a specific node
        // We use a shared StorageNetwork, so we just return a new adapter instance
        WormFSRaftNetwork::new(self.storage_network.clone(), self.node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::NetworkConfig;

    #[tokio::test]
    async fn test_network_creation() {
        let config = NetworkConfig {
            node_id: 1,
            listen_address: "/ip4/127.0.0.1/tcp/0".to_string(),
            peers: vec![],
            request_timeout: std::time::Duration::from_secs(5),
            max_retries: 3,
        };

        let (network, _rx, _tx) = StorageNetwork::new(config).unwrap();
        let raft_network = WormFSRaftNetwork::new(Arc::new(Mutex::new(network)), 1);

        assert_eq!(raft_network.node_id, 1);
    }

    #[tokio::test]
    async fn test_network_factory() {
        let config = NetworkConfig {
            node_id: 1,
            listen_address: "/ip4/127.0.0.1/tcp/0".to_string(),
            peers: vec![],
            request_timeout: std::time::Duration::from_secs(5),
            max_retries: 3,
        };

        let (network, _rx, _tx) = StorageNetwork::new(config).unwrap();
        let mut factory = WormFSRaftNetworkFactory::new(Arc::new(Mutex::new(network)), 1);

        let client = factory.new_client(2, &()).await;
        assert_eq!(client.node_id, 1);
    }
}
