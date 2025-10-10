//! Request handler for incoming Raft RPCs
//!
//! This module provides the handler that processes incoming Raft requests
//! from peers via the libp2p network layer and routes them to the appropriate
//! OpenRaft handlers.

use libp2p::PeerId;
use openraft::{LogId, SnapshotMeta, StoredMembership};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, error, info, warn};

use crate::raft::node::RaftNode;
use crate::raft::proto_types::proto::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RaftRequest, RaftResponse, VoteRequest, VoteResponse,
};
use crate::raft::snapshot_store::{PersistedSnapshotMeta, SnapshotStore};
use crate::transport::SnapshotTransferClient;

/// Handler for incoming Raft requests
/// 
/// This handler receives RaftRequest messages from the libp2p network layer,
/// processes them, and returns appropriate RaftResponse messages.
pub struct RaftRequestHandler {
    /// The Raft node instance
    raft_node: Arc<RaftNode>,
    /// Snapshot store for managing snapshots
    snapshot_store: Arc<SnapshotStore>,
    /// Client for downloading snapshots from remote endpoints
    snapshot_client: Arc<SnapshotTransferClient>,
    /// Function to get this node's storage endpoint address
    endpoint_address_fn: Arc<dyn Fn() -> Option<String> + Send + Sync>,
}

impl RaftRequestHandler {
    /// Create a new request handler
    pub fn new(
        raft_node: Arc<RaftNode>,
        snapshot_store: Arc<SnapshotStore>,
        snapshot_client: Arc<SnapshotTransferClient>,
        endpoint_address_fn: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    ) -> Self {
        info!("Creating RaftRequestHandler for node {}", raft_node.node_id());
        Self {
            raft_node,
            snapshot_store,
            snapshot_client,
            endpoint_address_fn,
        }
    }

    /// Handle an incoming Raft request
    /// 
    /// This is the main entry point called by the libp2p network layer
    /// when a Raft RPC is received from a peer.
    pub fn handle_request(&self, request: RaftRequest, _peer: PeerId) -> RaftResponse {
        match request.request {
            Some(crate::raft::proto_types::proto::raft_request::Request::AppendEntries(req)) => {
                RaftResponse {
                    response: Some(
                        crate::raft::proto_types::proto::raft_response::Response::AppendEntries(
                            self.handle_append_entries(req),
                        ),
                    ),
                }
            }
            Some(crate::raft::proto_types::proto::raft_request::Request::Vote(req)) => {
                RaftResponse {
                    response: Some(crate::raft::proto_types::proto::raft_response::Response::Vote(
                        self.handle_vote(req),
                    )),
                }
            }
            Some(crate::raft::proto_types::proto::raft_request::Request::InstallSnapshot(req)) => {
                RaftResponse {
                    response: Some(
                        crate::raft::proto_types::proto::raft_response::Response::InstallSnapshot(
                            // InstallSnapshot requires async, but we're in a sync context
                            // We'll need to spawn a task and return immediately with a placeholder
                            // For now, return an error - this will be fixed when we properly integrate
                            self.handle_install_snapshot_sync(req),
                        ),
                    ),
                }
            }
            Some(crate::raft::proto_types::proto::raft_request::Request::Announce(_)) => {
                // NodeAnnouncement is handled separately by the network layer
                debug!("Received NodeAnnouncement, ignoring in RaftRequestHandler");
                RaftResponse { response: None }
            }
            None => {
                warn!("Received empty Raft request");
                RaftResponse { response: None }
            }
        }
    }

    /// Handle AppendEntries RPC
    /// 
    /// This is called when the leader sends log entries to replicate to this follower.
    fn handle_append_entries(&self, req: AppendEntriesRequest) -> AppendEntriesResponse {
        debug!(
            "Handling AppendEntries from leader {} (term: {}, {} entries)",
            req.leader_id,
            req.term,
            req.entries.len()
        );

        // For now, we return a placeholder response
        // The actual implementation would call into OpenRaft's append_entries handler
        // This requires the external API which we'll need to call via a channel or similar
        
        // TODO: Implement proper OpenRaft integration
        // For now, return a basic response
        AppendEntriesResponse {
            term: req.term,
            success: false,
            conflict: None,
        }
    }

    /// Handle Vote RPC
    /// 
    /// This is called when a candidate requests our vote during leader election.
    fn handle_vote(&self, req: VoteRequest) -> VoteResponse {
        debug!(
            "Handling Vote request from candidate {} (term: {})",
            req.candidate_id, req.term
        );

        // For now, we return a placeholder response
        // The actual implementation would call into OpenRaft's vote handler
        
        // TODO: Implement proper OpenRaft integration
        VoteResponse {
            term: req.term,
            vote_granted: false,
            last_log_index: 0,
        }
    }

    /// Handle InstallSnapshot RPC (synchronous wrapper)
    /// 
    /// This is a synchronous wrapper that will be replaced with proper async handling
    fn handle_install_snapshot_sync(&self, req: InstallSnapshotRequest) -> InstallSnapshotResponse {
        // Create a runtime to execute the async function
        // This is not ideal, but necessary for the synchronous handler interface
        let rt = tokio::runtime::Handle::try_current();
        
        match rt {
            Ok(handle) => {
                // We're already in a tokio runtime, spawn a blocking task
                let handler = self.clone_for_async();
                let req_clone = req.clone();
                
                // Spawn the async task but return immediately
                // The actual result will be processed asynchronously
                handle.spawn(async move {
                    let result = handler.handle_install_snapshot_async(req_clone).await;
                    if let Err(e) = result {
                        error!("InstallSnapshot async handler failed: {}", e);
                    }
                });
                
                // Return immediate acknowledgment
                InstallSnapshotResponse {
                    term: req.term,
                    success: true,
                    error_message: String::new(),
                }
            }
            Err(_) => {
                error!("No tokio runtime available for InstallSnapshot");
                InstallSnapshotResponse {
                    term: req.term,
                    success: false,
                    error_message: "No async runtime available".to_string(),
                }
            }
        }
    }

    /// Handle InstallSnapshot RPC (async implementation)
    /// 
    /// This implements the actual snapshot installation logic including:
    /// - Downloading the snapshot from the leader's StorageEndpoint via gRPC
    /// - Verifying the hash
    /// - Installing the snapshot atomically
    async fn handle_install_snapshot_async(
        &self,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, Box<dyn std::error::Error>> {
        info!(
            "Handling InstallSnapshot from leader {} (snapshot: {}, size: {} bytes)",
            req.leader_id, req.snapshot_id, req.size
        );

        // Extract leader's endpoint address (Task 6: use leader_address field)
        let leader_address = &req.leader_address;

        if leader_address.is_empty() {
            error!("Leader endpoint address not provided in InstallSnapshot request");
            return Ok(InstallSnapshotResponse {
                term: req.term,
                success: false,
                error_message: "Leader endpoint address not provided".to_string(),
            });
        }

        debug!("Downloading snapshot from leader endpoint: {}", leader_address);

        // Download snapshot using SnapshotTransferClient (Task 6: gRPC download)
        let snapshot_dir = self.snapshot_store.snapshot_dir();

        match self
            .snapshot_client
            .download_snapshot(
                leader_address,
                &req.snapshot_id,
                self.raft_node.node_id(),
                &req.hash,
                snapshot_dir,
            )
            .await
        {
            Ok(temp_path) => {
                info!(
                    "Successfully downloaded snapshot '{}', installing...",
                    req.snapshot_id
                );

                // Create persisted snapshot metadata
                let metadata = PersistedSnapshotMeta {
                    raft_meta: SnapshotMeta {
                        last_log_id: Some(LogId::new(
                            openraft::LeaderId::new(req.term, req.leader_id),
                            req.last_included_index,
                        )),
                        last_membership: StoredMembership::default(),
                        snapshot_id: req.snapshot_id.clone(),
                    },
                    data_checksum: req.hash.clone(),
                    data_size: req.size,
                    created_at: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                };

                // Install snapshot atomically (Task 6: atomic installation)
                if let Err(e) = self
                    .snapshot_store
                    .install_snapshot_from_temp(&req.snapshot_id, temp_path, metadata)
                    .await
                {
                    error!("Failed to install snapshot: {}", e);
                    return Ok(InstallSnapshotResponse {
                        term: req.term,
                        success: false,
                        error_message: format!("Failed to install snapshot: {}", e),
                    });
                }

                info!("Successfully installed snapshot '{}'", req.snapshot_id);

                // TODO: Notify OpenRaft that snapshot was installed
                // This would involve calling into the Raft state machine to load the snapshot

                Ok(InstallSnapshotResponse {
                    term: req.term,
                    success: true,
                    error_message: String::new(),
                })
            }
            Err(e) => {
                error!("Failed to download snapshot: {}", e);
                Ok(InstallSnapshotResponse {
                    term: req.term,
                    success: false,
                    error_message: format!("Download failed: {}", e),
                })
            }
        }
    }

    /// Clone the necessary components for async handling
    fn clone_for_async(&self) -> Self {
        Self {
            raft_node: self.raft_node.clone(),
            snapshot_store: self.snapshot_store.clone(),
            snapshot_client: self.snapshot_client.clone(),
            endpoint_address_fn: self.endpoint_address_fn.clone(),
        }
    }

    /// Get this node's storage endpoint address
    /// Used when sending InstallSnapshot requests to populate leader_address field
    pub fn get_endpoint_address(&self) -> Option<String> {
        (self.endpoint_address_fn)()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::config::RaftConfig;
    use crate::raft::log_store::LogStore;
    use crate::raft::network::WormFSRaftNetworkFactory;
    use crate::raft::state_machine::StateMachine;
    use crate::transport::{NetworkConfig, StorageNetwork};
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_request_handler_creation() {
        let temp_dir = TempDir::new().unwrap();

        // Create Raft components
        let log_path = temp_dir.path().join("log.db");
        let log_store = Arc::new(LogStore::new(log_path.to_str().unwrap()).unwrap());

        let state_machine = Arc::new(StateMachine::new().unwrap());
        let snapshot_store = state_machine.snapshot_store.clone();

        let network_config = NetworkConfig {
            node_id: 1,
            listen_address: "/ip4/127.0.0.1/tcp/0".to_string(),
            peers: vec![],
            request_timeout: std::time::Duration::from_secs(5),
            max_retries: 3,
        };

        let (network, _rx, _tx) = StorageNetwork::new(network_config).unwrap();
        let network_factory = WormFSRaftNetworkFactory::new(Arc::new(Mutex::new(network)), 1);

        let raft_config = RaftConfig::new_for_test(1);
        let raft_node =
            Arc::new(RaftNode::new(1, raft_config, log_store, state_machine, network_factory)
                .await
                .unwrap());

        let snapshot_client = Arc::new(SnapshotTransferClient::new(3));

        let endpoint_fn = Arc::new(|| Some("http://localhost:8082".to_string()))
            as Arc<dyn Fn() -> Option<String> + Send + Sync>;

        let handler = RaftRequestHandler::new(
            raft_node,
            snapshot_store,
            snapshot_client,
            endpoint_fn,
        );

        assert_eq!(
            handler.get_endpoint_address(),
            Some("http://localhost:8082".to_string())
        );
    }

    #[tokio::test]
    async fn test_handle_vote_request() {
        let temp_dir = TempDir::new().unwrap();

        // Create minimal setup
        let log_path = temp_dir.path().join("log.db");
        let log_store = Arc::new(LogStore::new(log_path.to_str().unwrap()).unwrap());

        let state_machine = Arc::new(StateMachine::new().unwrap());
        let snapshot_store = state_machine.snapshot_store.clone();

        let network_config = NetworkConfig {
            node_id: 1,
            listen_address: "/ip4/127.0.0.1/tcp/0".to_string(),
            peers: vec![],
            request_timeout: std::time::Duration::from_secs(5),
            max_retries: 3,
        };

        let (network, _rx, _tx) = StorageNetwork::new(network_config).unwrap();
        let network_factory = WormFSRaftNetworkFactory::new(Arc::new(Mutex::new(network)), 1);

        let raft_config = RaftConfig::new_for_test(1);
        let raft_node =
            Arc::new(RaftNode::new(1, raft_config, log_store, state_machine, network_factory)
                .await
                .unwrap());

        let snapshot_client = Arc::new(SnapshotTransferClient::new(3));
        let endpoint_fn = Arc::new(|| None) as Arc<dyn Fn() -> Option<String> + Send + Sync>;

        let handler = RaftRequestHandler::new(
            raft_node,
            snapshot_store,
            snapshot_client,
            endpoint_fn,
        );

        // Create a vote request
        let vote_req = VoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };

        let response = handler.handle_vote(vote_req);

        // Should return a response (even if vote not granted yet)
        assert_eq!(response.term, 1);
    }
}
