//! RaftNetwork adapter for StorageNetwork.
//!
//! This module adapts StorageNetwork's libp2p-based networking to implement
//! OpenRaft's RaftNetwork trait, enabling Raft consensus over peer-to-peer communication.
//!
//! ## Architecture
//!
//! The adapter uses topic-based pub/sub for Raft RPCs:
//! - `raft.vote` topic: RequestVote RPCs
//! - `raft.append` topic: AppendEntries RPCs
//! - `raft.snapshot` topic: InstallSnapshot RPCs
//!
//! Each RPC type is sent via gossipsub and responses are correlated using request IDs.
//!
//! ## Design Decisions
//!
//! 1. **Topic-Based RPCs**: Each Raft RPC type gets a dedicated topic for clean separation
//! 2. **Request/Response Correlation**: Use unique request IDs to match responses
//! 3. **Timeout Handling**: Each RPC has configurable timeout with automatic retries
//! 4. **Backpressure**: Monitor pending request queue and reject new requests when overloaded
//!
//! ## Implementation Status
//!
//! This is currently a documented stub. Full implementation requires:
//! 1. Implementing OpenRaft's RaftNetwork trait with correct lifetimes
//! 2. Creating request/response serialization format
//! 3. Setting up topic subscriptions for Raft RPCs
//! 4. Implementing request correlation and timeout logic
//! 5. Adding backpressure monitoring

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::storage_network::StorageNetworkHandle;

use super::raft_config::WormFsNode;
use super::types::NodeId;

/// Request ID for correlating RPC requests and responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct RequestId(u64);

/// State of a pending RPC request.
#[derive(Debug)]
struct PendingRequest {
    /// When the request was sent
    sent_at: std::time::Instant,

    /// Timeout for this request
    timeout: Duration,

    /// Channel to send the response back
    response_tx: tokio::sync::oneshot::Sender<Vec<u8>>,
}

/// Inner state for the network adapter.
struct NetworkAdapterInner {
    /// The underlying storage network
    network: StorageNetworkHandle,

    /// Configuration for network operations
    config: NetworkAdapterConfig,

    /// Pending RPC requests waiting for responses
    pending_requests: RwLock<HashMap<RequestId, PendingRequest>>,

    /// Next request ID to use
    next_request_id: std::sync::atomic::AtomicU64,

    /// Topic handles for Raft RPCs (created on first use)
    vote_topic: RwLock<
        Option<(
            crate::storage_network::TopicSender,
            crate::storage_network::TopicReceiver,
        )>,
    >,
    append_topic: RwLock<
        Option<(
            crate::storage_network::TopicSender,
            crate::storage_network::TopicReceiver,
        )>,
    >,
    snapshot_topic: RwLock<
        Option<(
            crate::storage_network::TopicSender,
            crate::storage_network::TopicReceiver,
        )>,
    >,
}

/// Configuration for the network adapter.
#[derive(Debug, Clone)]
pub struct NetworkAdapterConfig {
    /// Default RPC timeout
    pub default_rpc_timeout: Duration,

    /// Maximum number of pending requests
    pub max_pending_requests: usize,

    /// Enable request pipelining
    pub enable_pipelining: bool,
}

impl Default for NetworkAdapterConfig {
    fn default() -> Self {
        Self {
            default_rpc_timeout: Duration::from_secs(5),
            max_pending_requests: 1000,
            enable_pipelining: true,
        }
    }
}

/// Network adapter that implements OpenRaft's RaftNetwork trait.
///
/// This adapter bridges StorageNetwork's libp2p-based communication
/// with OpenRaft's RPC requirements.
#[derive(Clone)]
pub struct RaftNetworkAdapter {
    inner: Arc<NetworkAdapterInner>,
}

impl RaftNetworkAdapter {
    /// Create a new network adapter.
    ///
    /// # Arguments
    ///
    /// * `network` - The storage network handle
    /// * `config` - Configuration for network adapter behavior
    pub fn new(network: StorageNetworkHandle, config: NetworkAdapterConfig) -> Self {
        Self {
            inner: Arc::new(NetworkAdapterInner {
                network,
                config,
                pending_requests: RwLock::new(HashMap::new()),
                next_request_id: std::sync::atomic::AtomicU64::new(1),
                vote_topic: RwLock::new(None),
                append_topic: RwLock::new(None),
                snapshot_topic: RwLock::new(None),
            }),
        }
    }

    /// Initialize the network adapter by subscribing to Raft topics.
    ///
    /// This should be called once before using the adapter for RPCs.
    pub async fn initialize(&self) -> Result<(), String> {
        info!("Initializing Raft network adapter");

        // Subscribe to vote topic
        let vote_handle = self
            .inner
            .network
            .join_topic("raft.vote")
            .await
            .map_err(|e| format!("Failed to join vote topic: {:?}", e))?;

        *self.inner.vote_topic.write().await = Some(vote_handle);
        debug!("Subscribed to raft.vote topic");

        // Subscribe to append entries topic
        let append_handle = self
            .inner
            .network
            .join_topic("raft.append")
            .await
            .map_err(|e| format!("Failed to join append topic: {:?}", e))?;

        *self.inner.append_topic.write().await = Some(append_handle);
        debug!("Subscribed to raft.append topic");

        // Subscribe to snapshot topic
        let snapshot_handle = self
            .inner
            .network
            .join_topic("raft.snapshot")
            .await
            .map_err(|e| format!("Failed to join snapshot topic: {:?}", e))?;

        *self.inner.snapshot_topic.write().await = Some(snapshot_handle);
        debug!("Subscribed to raft.snapshot topic");

        info!("Raft network adapter initialized successfully");
        Ok(())
    }

    /// Get the next request ID.
    fn next_request_id(&self) -> RequestId {
        let id = self
            .inner
            .next_request_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        RequestId(id)
    }

    /// Check if we can accept a new request (backpressure).
    async fn can_accept_request(&self) -> bool {
        let pending = self.inner.pending_requests.read().await;
        pending.len() < self.inner.config.max_pending_requests
    }

    /// Clean up timed-out requests.
    ///
    /// This should be called periodically to remove stale requests.
    pub async fn cleanup_timed_out_requests(&self) {
        let mut pending = self.inner.pending_requests.write().await;
        let now = std::time::Instant::now();

        let before_count = pending.len();

        pending.retain(|request_id, req| {
            let elapsed = now.duration_since(req.sent_at);
            if elapsed > req.timeout {
                warn!("Request {:?} timed out after {:?}", request_id, elapsed);
                false // Remove
            } else {
                true // Keep
            }
        });

        let after_count = pending.len();
        if before_count != after_count {
            debug!(
                "Cleaned up {} timed-out requests ({} -> {})",
                before_count - after_count,
                before_count,
                after_count
            );
        }
    }
}

// TODO: Implement OpenRaft's RaftNetwork trait
//
// The implementation needs to:
// 1. Implement RaftNetwork<WormFsTypeConfig> trait with correct lifetimes
// 2. Implement send_append_entries() - sends AppendEntries RPC to a target node
// 3. Implement send_vote() - sends RequestVote RPC to a target node
// 4. Implement send_install_snapshot() - sends InstallSnapshot RPC to a target node
//
// Example skeleton (needs correct lifetimes and serialization):
//
// #[async_trait]
// impl RaftNetwork<WormFsTypeConfig> for RaftNetworkAdapter {
//     async fn send_append_entries<'life0, 'async_trait>(
//         &'life0 mut self,
//         target: NodeId,
//         node: Option<WormFsNode>,
//         rpc: AppendEntriesRequest<WormFsTypeConfig>,
//     ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, WormFsNode>>
//     where
//         'life0: 'async_trait,
//         Self: 'async_trait,
//     {
//         // 1. Check backpressure
//         if !self.can_accept_request().await {
//             return Err(RPCError::Unreachable(Unreachable::new(&target)));
//         }
//
//         // 2. Generate request ID
//         let request_id = self.next_request_id();
//
//         // 3. Serialize RPC
//         let rpc_bytes = bincode::serialize(&rpc)
//             .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
//
//         // 4. Set up response channel
//         let (response_tx, response_rx) = oneshot::channel();
//
//         // 5. Store pending request
//         let pending_req = PendingRequest {
//             sent_at: Instant::now(),
//             timeout: self.inner.config.default_rpc_timeout,
//             response_tx,
//         };
//         self.inner.pending_requests.write().await.insert(request_id, pending_req);
//
//         // 6. Send via topic (need to include request_id in message)
//         let append_topic = self.inner.append_topic.read().await;
//         if let Some((tx, _rx)) = append_topic.as_ref() {
//             tx.send(rpc_bytes).await
//                 .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
//         }
//
//         // 7. Wait for response with timeout
//         let response_bytes = tokio::time::timeout(
//             self.inner.config.default_rpc_timeout,
//             response_rx,
//         )
//         .await
//         .map_err(|_| RPCError::Timeout(Timeout::new()))?
//         .map_err(|_| RPCError::Network(NetworkError::new(&"Response channel closed")))?;
//
//         // 8. Deserialize response
//         let response: AppendEntriesResponse<NodeId> = bincode::deserialize(&response_bytes)
//             .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
//
//         Ok(response)
//     }
//
//     // Similar implementations for send_vote() and send_install_snapshot()
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_generation() {
        let id1 = RequestId(1);
        let id2 = RequestId(2);
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_network_adapter_config_default() {
        let config = NetworkAdapterConfig::default();
        assert_eq!(config.default_rpc_timeout, Duration::from_secs(5));
        assert_eq!(config.max_pending_requests, 1000);
        assert!(config.enable_pipelining);
    }
}
