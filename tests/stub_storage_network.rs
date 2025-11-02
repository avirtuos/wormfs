//! Stub StorageNetwork implementation for testing using in-memory channels.
//!
//! This provides a fake network that allows multiple Raft nodes to communicate
//! via channels instead of real libp2p networking, making tests faster and more reliable.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use wormfs::storage_network::{Error, NetworkHandleTrait, PeerInfo};
use wormfs::storage_raft_member::raft_member::RaftRpcHandler;

/// A stub network that routes messages via in-memory channels
pub struct StubNetworkHub {
    /// Map of node_id -> message channel
    nodes: Arc<RwLock<HashMap<u64, mpsc::UnboundedSender<Vec<u8>>>>>,
    /// Map of node_id -> Raft handler
    raft_handlers: Arc<RwLock<HashMap<u64, Arc<dyn RaftRpcHandler>>>>,
    /// Map of PeerId bytes -> node_id (for routing RPCs by PeerId)
    peer_to_node: Arc<RwLock<HashMap<Vec<u8>, u64>>>,
}

impl StubNetworkHub {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            raft_handlers: Arc::new(RwLock::new(HashMap::new())),
            peer_to_node: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a handle for a specific node
    pub fn create_handle(&self, node_id: u64) -> StubStorageNetworkHandle {
        let (tx, rx) = mpsc::unbounded_channel();

        // Generate a real libp2p PeerId for this stub node
        let peer_id = libp2p::PeerId::random();

        StubStorageNetworkHandle {
            node_id,
            peer_id,
            tx,
            rx: Arc::new(RwLock::new(rx)),
            hub_nodes: self.nodes.clone(),
            hub_handlers: self.raft_handlers.clone(),
            hub_peer_to_node: self.peer_to_node.clone(),
        }
    }
}

/// Handle for a single node in the stub network
#[derive(Clone)]
pub struct StubStorageNetworkHandle {
    node_id: u64,
    /// The libp2p PeerId for this node (generated once during creation)
    peer_id: libp2p::PeerId,
    tx: mpsc::UnboundedSender<Vec<u8>>,
    rx: Arc<RwLock<mpsc::UnboundedReceiver<Vec<u8>>>>,
    hub_nodes: Arc<RwLock<HashMap<u64, mpsc::UnboundedSender<Vec<u8>>>>>,
    hub_handlers: Arc<RwLock<HashMap<u64, Arc<dyn RaftRpcHandler>>>>,
    hub_peer_to_node: Arc<RwLock<HashMap<Vec<u8>, u64>>>,
}

impl StubStorageNetworkHandle {
    /// Get this node's PeerId as a string (for use in Raft configuration)
    pub fn peer_id_string(&self) -> String {
        self.peer_id.to_string()
    }

    /// Register this node in the hub
    pub async fn register(&self) {
        let mut nodes = self.hub_nodes.write().await;
        nodes.insert(self.node_id, self.tx.clone());

        // Also register the PeerId -> node_id mapping
        let mut peer_map = self.hub_peer_to_node.write().await;
        peer_map.insert(self.peer_id.to_bytes(), self.node_id);
    }

    /// Send a Raft RPC to a target node and get response
    pub async fn send_raft_rpc(
        &self,
        target_node_id: u64,
        request: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        use wormfs::storage_network::types::PeerId as WormFsPeerId;
        use wormfs::storage_raft_member::raft_member::{RaftRpcMessage, RaftRpcResponse};

        // Deserialize request to log it
        let rpc_type = match bincode::deserialize::<RaftRpcMessage>(&request) {
            Ok(RaftRpcMessage::Vote(ref req)) => {
                format!(
                    "Vote(term={}, candidate={:?})",
                    req.vote.leader_id.term, req.vote.leader_id.node_id
                )
            }
            Ok(RaftRpcMessage::AppendEntries(ref req)) => {
                format!(
                    "AppendEntries(term={}, prev_log={:?}, entries={})",
                    req.vote.leader_id.term,
                    req.prev_log_id,
                    req.entries.len()
                )
            }
            Ok(RaftRpcMessage::InstallSnapshot(ref req)) => {
                format!(
                    "InstallSnapshot(term={}, last_included={:?})",
                    req.vote.leader_id.term, req.meta.last_log_id
                )
            }
            Err(_) => format!("Unknown({} bytes)", request.len()),
        };

        eprintln!(
            "[StubNetwork] Node {} → Node {}: {}",
            self.node_id, target_node_id, rpc_type
        );

        // Get the target node's Raft handler
        let handlers = self.hub_handlers.read().await;
        let handler = handlers.get(&target_node_id).ok_or_else(|| {
            eprintln!(
                "[StubNetwork] ERROR: Node {} not found in handlers map",
                target_node_id
            );
            // Create a PeerId for the error
            Error::PeerNotConnected(WormFsPeerId::new(target_node_id.to_le_bytes().to_vec()))
        })?;
        let handler = Arc::clone(handler);
        drop(handlers); // Release lock before async call

        // Call the handler directly (simulating network RPC)
        let result = handler.handle_raft_rpc(request).await.map_err(|e| {
            eprintln!(
                "[StubNetwork] ERROR: RPC to node {} failed: {:?}",
                target_node_id, e
            );
            Error::RequestFailed {
                peer: WormFsPeerId::new(target_node_id.to_le_bytes().to_vec()),
                reason: format!("Raft RPC failed: {:?}", e),
            }
        })?;

        // Deserialize response to log it
        let response_type = match bincode::deserialize::<RaftRpcResponse>(&result) {
            Ok(RaftRpcResponse::Vote(ref resp)) => {
                format!(
                    "VoteResp(granted={}, term={})",
                    resp.vote_granted, resp.vote.committed
                )
            }
            Ok(RaftRpcResponse::AppendEntries(ref resp)) => {
                // Use Debug formatting since AppendEntriesResponse fields vary by OpenRaft version
                format!("AppendResp({:?})", resp)
            }
            Ok(RaftRpcResponse::InstallSnapshot(ref resp)) => {
                format!("SnapshotResp({:?})", resp)
            }
            Err(_) => format!("Unknown({} bytes)", result.len()),
        };

        eprintln!(
            "[StubNetwork] Node {} ← Node {}: {}",
            self.node_id, target_node_id, response_type
        );
        Ok(result)
    }

    /// Register a Raft handler for this node
    pub async fn register_raft_handler_internal(&self, handler: Arc<dyn RaftRpcHandler>) {
        eprintln!(
            "[StubNetwork] Registering Raft handler for node {}",
            self.node_id
        );
        let mut handlers = self.hub_handlers.write().await;
        handlers.insert(self.node_id, handler);
        eprintln!(
            "[StubNetwork] Handler registered for node {}. Total handlers: {}",
            self.node_id,
            handlers.len()
        );
    }

    /// Get list of connected peers (all other registered nodes)
    pub async fn get_connected_peers_internal(&self) -> Vec<PeerInfo> {
        use wormfs::storage_network::types::{ConnectionState, PeerId as WormFsPeerId};

        let nodes = self.hub_nodes.read().await;
        nodes
            .keys()
            .filter(|&&id| id != self.node_id)
            .map(|&id| {
                // Create a libp2p PeerId for this stub node
                let peer_id_str = format!("stub-node-{}", id);
                let peer_id_libp2p: libp2p::PeerId = peer_id_str.parse().unwrap();

                PeerInfo {
                    peer_id: WormFsPeerId::new(peer_id_libp2p.to_bytes()),
                    addresses: vec![],
                    state: ConnectionState::Connected,
                    connected_since: Some(std::time::SystemTime::now()),
                    protocols: vec!["/wormfs/raft/1.0.0".to_string()],
                    rtt: Some(std::time::Duration::from_millis(1)), // Instant for stub
                    last_heartbeat: Some(std::time::SystemTime::now()),
                    node_id: Some(id.to_string()),
                    heartbeat_sequence: Some(0),
                    admin_url: None,
                }
            })
            .collect()
    }
}

// Implement NetworkHandleTrait so stub can be used wherever StorageNetworkHandle is expected
#[async_trait]
impl NetworkHandleTrait for StubStorageNetworkHandle {
    async fn send_request(
        &self,
        peer_id_bytes: &[u8],
        _protocol: &str,
        request: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        use wormfs::storage_network::types::PeerId as WormFsPeerId;

        // Look up the node_id for this PeerId
        let peer_map = self.hub_peer_to_node.read().await;
        let target_node_id = peer_map
            .get(peer_id_bytes)
            .copied()
            .ok_or_else(|| Error::PeerNotConnected(WormFsPeerId::new(peer_id_bytes.to_vec())))?;

        // Send RPC to the target node
        drop(peer_map); // Release lock before async call
        self.send_raft_rpc(target_node_id, request).await
    }

    async fn register_raft_handler(&self, handler: Arc<dyn RaftRpcHandler>) -> Result<(), Error> {
        self.register_raft_handler_internal(handler).await;
        Ok(())
    }

    async fn get_connected_peers(&self) -> Result<Vec<PeerInfo>, Error> {
        Ok(self.get_connected_peers_internal().await)
    }

    async fn dial_configured_peers(&self) -> Result<(), Error> {
        // No-op for stub - connections are instant
        Ok(())
    }
}
