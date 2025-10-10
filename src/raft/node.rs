//! RaftNode wrapper for managing OpenRaft lifecycle
//!
//! This module provides a high-level wrapper around the OpenRaft instance,
//! handling initialization, client operations, and lifecycle management.

use openraft::{Config, Raft};
use std::collections::BTreeSet;
use std::sync::Arc;
use tracing::{debug, info};

use crate::raft::config::RaftConfig;
use crate::raft::log_store::LogStore;
use crate::raft::network::WormFSRaftNetworkFactory;
use crate::raft::state_machine::StateMachine;
use crate::raft::types::WormFSTypeConfig;
use openraft::error::{ClientWriteError, InitializeError};
use openraft::raft::ClientWriteResponse;
use crate::raft::proto_types::proto::MetadataOp;

/// Wrapper around OpenRaft instance with lifecycle management
pub struct RaftNode {
    /// The underlying OpenRaft instance
    raft: Arc<Raft<WormFSTypeConfig>>,
    /// Node ID
    node_id: u64,
    /// Raft configuration
    config: RaftConfig,
}

impl RaftNode {
    /// Create a new RaftNode instance
    pub async fn new(
        node_id: u64,
        config: RaftConfig,
        log_store: Arc<LogStore>,
        state_machine: Arc<StateMachine>,
        network_factory: WormFSRaftNetworkFactory,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        info!("Creating RaftNode for node {}", node_id);

        // Convert our RaftConfig to OpenRaft's Config
        let openraft_config = config.to_openraft_config();

        // Validate configuration
        openraft_config
            .validate()
            .map_err(|e| format!("Invalid Raft config: {}", e))?;

        // Create the Raft instance
        let raft = Raft::new(
            node_id,
            Arc::new(openraft_config),
            network_factory,
            log_store,
            state_machine,
        )
        .await?;

        info!("RaftNode created successfully for node {}", node_id);

        Ok(Self {
            raft: Arc::new(raft),
            node_id,
            config,
        })
    }

    /// Initialize the Raft cluster with the given member set
    /// This should only be called once when bootstrapping a new cluster
    pub async fn initialize(&self, members: BTreeSet<u64>) -> Result<(), InitializeError> {
        info!("Initializing Raft cluster with members: {:?}", members);

        let mut member_nodes = BTreeSet::new();
        for member_id in members {
            member_nodes.insert((member_id, ()));
        }

        self.raft.initialize(member_nodes).await?;

        info!("Raft cluster initialized successfully");
        Ok(())
    }

    /// Check if this node is initialized (part of a cluster)
    pub async fn is_initialized(&self) -> Result<bool, Box<dyn std::error::Error>> {
        // Check if we have any log entries or if we're part of a membership
        let metrics = self.raft.metrics().borrow().clone();
        
        // If last_log_id is Some or membership_config is not empty, we're initialized
        Ok(metrics.last_log_index.is_some() 
            || !metrics.membership_config.membership().is_empty())
    }

    /// Get a reference to the underlying Raft instance
    pub fn raft(&self) -> &Arc<Raft<WormFSTypeConfig>> {
        &self.raft
    }

    /// Submit a client write request (metadata operation)
    pub async fn client_write(
        &self,
        op: MetadataOp,
    ) -> Result<ClientWriteResponse, ClientWriteError> {
        debug!("Submitting client write request");

        // Serialize the operation
        let payload = crate::raft::proto_types::serialize_metadata_op(&op)
            .map_err(|e| ClientWriteError::ForwardToLeader(
                openraft::error::ForwardToLeader {
                    leader_id: None,
                    leader_node: None,
                }
            ))?;

        // Submit to Raft
        let response = self.raft.client_write(payload).await?;

        debug!("Client write committed at log index: {:?}", response.log_id);
        Ok(response)
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics
            .current_leader
            .map(|leader| leader == self.node_id)
            .unwrap_or(false)
    }

    /// Get the current leader ID, if known
    pub async fn get_leader(&self) -> Option<u64> {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader
    }

    /// Get the current term
    pub async fn get_current_term(&self) -> u64 {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_term
    }

    /// Get cluster metrics for monitoring
    pub async fn get_metrics(&self) -> RaftNodeMetrics {
        let metrics = self.raft.metrics().borrow().clone();

        RaftNodeMetrics {
            node_id: self.node_id,
            current_term: metrics.current_term,
            current_leader: metrics.current_leader,
            last_log_index: metrics.last_log_index,
            last_applied: metrics.last_applied,
            state: format!("{:?}", metrics.state),
            membership: metrics
                .membership_config
                .membership()
                .nodes()
                .map(|(id, _)| *id)
                .collect(),
        }
    }

    /// Shutdown the Raft node gracefully
    pub async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Shutting down RaftNode {}", self.node_id);
        self.raft.shutdown().await?;
        info!("RaftNode {} shut down successfully", self.node_id);
        Ok(())
    }

    /// Get node ID
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Get configuration
    pub fn config(&self) -> &RaftConfig {
        &self.config
    }
}

/// Metrics for monitoring Raft node state
#[derive(Debug, Clone)]
pub struct RaftNodeMetrics {
    pub node_id: u64,
    pub current_term: u64,
    pub current_leader: Option<u64>,
    pub last_log_index: Option<u64>,
    pub last_applied: Option<openraft::LogId<u64>>,
    pub state: String,
    pub membership: Vec<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::log_store::LogStore;
    use crate::raft::network::WormFSRaftNetworkFactory;
    use crate::raft::state_machine::StateMachine;
    use crate::transport::{NetworkConfig, StorageNetwork};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_raft_node_creation() {
        let temp_dir = TempDir::new().unwrap();

        // Create storage components
        let log_path = temp_dir.path().join("log.db");
        let log_store = Arc::new(LogStore::new(log_path.to_str().unwrap()).unwrap());

        let state_machine = Arc::new(StateMachine::new().unwrap());

        // Create network factory
        let network_config = NetworkConfig {
            node_id: 1,
            listen_address: "/ip4/127.0.0.1/tcp/0".to_string(),
            peers: vec![],
            request_timeout: std::time::Duration::from_secs(5),
            max_retries: 3,
        };

        let (network, _rx, _tx) = StorageNetwork::new(network_config).unwrap();
        let network_factory = WormFSRaftNetworkFactory::new(Arc::new(Mutex::new(network)), 1);

        // Create Raft config
        let raft_config = RaftConfig::new_for_test(1);

        // Create RaftNode
        let raft_node = RaftNode::new(1, raft_config, log_store, state_machine, network_factory)
            .await
            .unwrap();

        assert_eq!(raft_node.node_id(), 1);
        assert!(!raft_node.is_leader().await);
    }

    #[tokio::test]
    async fn test_raft_node_initialization() {
        let temp_dir = TempDir::new().unwrap();

        // Create storage components
        let log_path = temp_dir.path().join("log.db");
        let log_store = Arc::new(LogStore::new(log_path.to_str().unwrap()).unwrap());

        let state_machine = Arc::new(StateMachine::new().unwrap());

        // Create network factory
        let network_config = NetworkConfig {
            node_id: 1,
            listen_address: "/ip4/127.0.0.1/tcp/0".to_string(),
            peers: vec![],
            request_timeout: std::time::Duration::from_secs(5),
            max_retries: 3,
        };

        let (network, _rx, _tx) = StorageNetwork::new(network_config).unwrap();
        let network_factory = WormFSRaftNetworkFactory::new(Arc::new(Mutex::new(network)), 1);

        // Create Raft config
        let raft_config = RaftConfig::new_for_test(1);

        // Create RaftNode
        let raft_node = RaftNode::new(1, raft_config, log_store, state_machine, network_factory)
            .await
            .unwrap();

        // Initialize as single-node cluster
        let mut members = BTreeSet::new();
        members.insert(1);

        raft_node.initialize(members).await.unwrap();

        // Should now be initialized
        assert!(raft_node.is_initialized().await.unwrap());
    }
}
