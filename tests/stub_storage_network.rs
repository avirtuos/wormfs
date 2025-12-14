//! Stub StorageNetwork implementation for testing using in-memory channels.
//!
//! This provides a fake network that allows multiple Raft nodes to communicate
//! via channels instead of real libp2p networking, making tests faster and more reliable.

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info};
use wormfs::storage_network::{Error, NetworkHandleTrait, PeerInfo};
use wormfs::storage_raft_member::raft_member::RaftRpcHandler;

/// Derive a deterministic Ed25519 keypair from a node ID.
///
/// Uses SHA-256 to hash the full u64 node_id into a 32-byte seed,
/// ensuring unique keypairs for all possible node IDs. The domain separator
/// prevents collision with other uses of SHA-256 in the system.
///
/// # Arguments
/// * `node_id` - The u64 node identifier
///
/// # Returns
/// * `Ok(Keypair)` - The derived Ed25519 keypair
/// * `Err(String)` - Error message if keypair creation fails
fn derive_keypair_from_node_id(node_id: u64) -> Result<libp2p::identity::Keypair, String> {
    let mut hasher = Sha256::new();
    hasher.update(b"wormfs-node-keypair-v1:"); // Domain separator
    hasher.update(node_id.to_le_bytes());
    let hash = hasher.finalize();

    // hash is 32 bytes, exactly what ed25519_from_bytes needs
    let seed: [u8; 32] = hash.into();
    libp2p::identity::Keypair::ed25519_from_bytes(seed)
        .map_err(|e| format!("Failed to create keypair: {}", e))
}

/// A stub network that routes messages via in-memory channels
pub struct StubNetworkHub {
    /// Map of node_id -> message channel
    nodes: Arc<RwLock<HashMap<u64, mpsc::UnboundedSender<Vec<u8>>>>>,
    /// Map of node_id -> Raft handler
    raft_handlers: Arc<RwLock<HashMap<u64, Arc<dyn RaftRpcHandler>>>>,
    /// Map of PeerId bytes -> node_id (for routing RPCs by PeerId)
    peer_to_node: Arc<RwLock<HashMap<Vec<u8>, u64>>>,
    /// Set of nodes that are currently offline (for simulating node failures)
    offline_nodes: Arc<RwLock<HashSet<u64>>>,
    /// Network partition groups (nodes in different groups cannot communicate)
    partition_groups: Arc<RwLock<Vec<HashSet<u64>>>>,
}

impl StubNetworkHub {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            raft_handlers: Arc::new(RwLock::new(HashMap::new())),
            peer_to_node: Arc::new(RwLock::new(HashMap::new())),
            offline_nodes: Arc::new(RwLock::new(HashSet::new())),
            partition_groups: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Mark a node as offline (simulates node failure)
    pub async fn mark_node_offline(&self, node_id: u64) {
        info!("[StubNetwork] Marking node {} as OFFLINE", node_id);
        let mut offline = self.offline_nodes.write().await;
        offline.insert(node_id);
    }

    /// Mark a node as online (simulates node recovery)
    pub async fn mark_node_online(&self, node_id: u64) {
        info!("[StubNetwork] Marking node {} as ONLINE", node_id);
        let mut offline = self.offline_nodes.write().await;
        offline.remove(&node_id);
    }

    /// Check if a node is currently offline
    pub async fn is_node_offline(&self, node_id: u64) -> bool {
        let offline = self.offline_nodes.read().await;
        offline.contains(&node_id)
    }

    /// Create network partitions between groups of nodes
    ///
    /// Nodes in different groups cannot communicate with each other.
    /// Nodes within the same group can still communicate freely.
    ///
    /// # Example
    /// ```ignore
    /// // Partition into [1,2,3] and [4,5]
    /// hub.partition_nodes(vec![vec![1,2,3], vec![4,5]]).await;
    /// ```
    pub async fn partition_nodes(&self, groups: Vec<Vec<u64>>) {
        let mut partition_groups = self.partition_groups.write().await;
        partition_groups.clear();

        for group in groups {
            if !group.is_empty() {
                partition_groups.push(group.into_iter().collect());
            }
        }

        info!(
            "[StubNetwork] Created {} partition groups: {:?}",
            partition_groups.len(),
            partition_groups
        );
    }

    /// Heal all network partitions
    ///
    /// After calling this, all nodes can communicate with each other again.
    pub async fn heal_partition(&self) {
        let mut partition_groups = self.partition_groups.write().await;
        partition_groups.clear();
        info!("[StubNetwork] Healed all partitions");
    }

    /// Check if two nodes are in different partition groups (cannot communicate)
    async fn is_partitioned(&self, from_node: u64, to_node: u64) -> bool {
        let groups = self.partition_groups.read().await;

        // If no partitions exist, nodes can communicate
        if groups.is_empty() {
            return false;
        }

        // Find which group each node belongs to
        let from_group = groups.iter().position(|g| g.contains(&from_node));
        let to_group = groups.iter().position(|g| g.contains(&to_node));

        // If both nodes are in the same group, they can communicate
        // If they're in different groups (or not in any group), check accordingly
        match (from_group, to_group) {
            (Some(a), Some(b)) => a != b, // Partitioned if in different groups
            _ => false,                   // If node not in any group, allow communication
        }
    }

    /// Unregister a Raft handler for a node (for clean shutdown/restart)
    pub async fn unregister_raft_handler(&self, node_id: u64) {
        debug!(
            "[StubNetwork] Unregistering Raft handler for node {}",
            node_id
        );
        let mut handlers = self.raft_handlers.write().await;
        handlers.remove(&node_id);
        debug!(
            "[StubNetwork] Handler unregistered for node {}. Remaining handlers: {}",
            node_id,
            handlers.len()
        );
    }

    /// Create a handle for a specific node
    pub fn create_handle(&self, node_id: u64) -> StubStorageNetworkHandle {
        let (tx, rx) = mpsc::unbounded_channel();

        // Generate a deterministic libp2p PeerId for this stub node based on node_id
        // This ensures the same node_id always gets the same PeerId across restarts
        let keypair =
            derive_keypair_from_node_id(node_id).expect("Failed to create deterministic keypair");
        let peer_id = libp2p::PeerId::from(keypair.public());

        StubStorageNetworkHandle {
            node_id,
            peer_id,
            tx,
            rx: Arc::new(RwLock::new(rx)),
            hub_nodes: self.nodes.clone(),
            hub_handlers: self.raft_handlers.clone(),
            hub_peer_to_node: self.peer_to_node.clone(),
            hub_offline_nodes: self.offline_nodes.clone(),
            hub_partition_groups: self.partition_groups.clone(),
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
    hub_offline_nodes: Arc<RwLock<HashSet<u64>>>,
    hub_partition_groups: Arc<RwLock<Vec<HashSet<u64>>>>,
}

impl StubStorageNetworkHandle {
    /// Get this node's PeerId as a string (for use in Raft configuration)
    pub fn peer_id_string(&self) -> String {
        self.peer_id.to_string()
    }

    /// Check if two nodes are in different partition groups (cannot communicate)
    async fn is_partitioned_internal(&self, from_node: u64, to_node: u64) -> bool {
        let groups = self.hub_partition_groups.read().await;

        // If no partitions exist, nodes can communicate
        if groups.is_empty() {
            return false;
        }

        // Find which group each node belongs to
        let from_group = groups.iter().position(|g| g.contains(&from_node));
        let to_group = groups.iter().position(|g| g.contains(&to_node));

        // If both nodes are in the same group, they can communicate
        // If they're in different groups (or not in any group), check accordingly
        match (from_group, to_group) {
            (Some(a), Some(b)) => a != b, // Partitioned if in different groups
            _ => false,                   // If node not in any group, allow communication
        }
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

        // Check 1: Is there a network partition between sender and target?
        if self
            .is_partitioned_internal(self.node_id, target_node_id)
            .await
        {
            debug!(
                "[StubNetwork] Node {} → Node {}: PARTITIONED (different network segments)",
                self.node_id, target_node_id
            );
            // Simulate a realistic network timeout delay (100ms)
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            return Err(Error::PeerNotConnected(WormFsPeerId::new(
                target_node_id.to_le_bytes().to_vec(),
            )));
        }

        // Check 2: Is target node offline (simulates node failure)
        let offline_nodes = self.hub_offline_nodes.read().await;
        if offline_nodes.contains(&target_node_id) {
            drop(offline_nodes); // Release lock
            debug!(
                "[StubNetwork] Node {} → Node {}: OFFLINE (simulating network timeout)",
                self.node_id, target_node_id
            );
            // Simulate a realistic network timeout delay (100ms)
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            return Err(Error::PeerNotConnected(WormFsPeerId::new(
                target_node_id.to_le_bytes().to_vec(),
            )));
        }
        drop(offline_nodes); // Release lock

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

        debug!(
            "[StubNetwork] Node {} → Node {}: {}",
            self.node_id, target_node_id, rpc_type
        );

        // Get the target node's Raft handler
        let handlers = self.hub_handlers.read().await;
        let handler = handlers.get(&target_node_id).ok_or_else(|| {
            error!(
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
            error!(
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

        debug!(
            "[StubNetwork] Node {} ← Node {}: {}",
            self.node_id, target_node_id, response_type
        );
        Ok(result)
    }

    /// Register a Raft handler for this node
    pub async fn register_raft_handler_internal(&self, handler: Arc<dyn RaftRpcHandler>) {
        info!(
            "[StubNetwork] Registering Raft handler for node {}",
            self.node_id
        );
        let mut handlers = self.hub_handlers.write().await;
        handlers.insert(self.node_id, handler);
        info!(
            "[StubNetwork] Handler registered for node {}. Total handlers: {}",
            self.node_id,
            handlers.len()
        );
    }

    /// Get list of connected peers (all other registered nodes)
    /// NOTE: We don't filter offline nodes here because OpenRaft needs to know about
    /// all configured members. The offline check happens in send_raft_rpc() instead.
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
