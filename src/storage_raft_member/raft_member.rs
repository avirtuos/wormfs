//! RaftMember: Network client for communicating with a specific Raft cluster member.
//!
//! This module provides the `RaftMember` type which implements OpenRaft's `RaftNetwork`
//! trait for point-to-point communication with a specific node in the Raft cluster.
//!
//! ## Architecture
//!
//! Each `RaftMember` instance:
//! - Represents communication to ONE specific cluster member
//! - Uses libp2p's request-response protocol for direct peer-to-peer messaging
//! - Shares the underlying StorageNetworkHandle (lightweight)
//! - Serializes Raft RPCs using bincode with serde
//!
//! ## Protocol
//!
//! All Raft RPCs are sent using the `/wormfs/raft/1.0.0` protocol over libp2p's
//! request-response mechanism. Messages are wrapped in `RaftRpcMessage` enums to
//! identify the RPC type on the receiving end.

use libp2p::PeerId;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, warn};

use crate::storage_network::types::PeerId as NetworkPeerId;
use crate::storage_network::StorageNetworkHandle;

use super::raft_config::{WormFsNode, WormFsTypeConfig};
use super::types::NodeId;

/// RPC message wrapper that identifies the type of Raft RPC.
///
/// This enum wraps the various Raft RPC request types so that the receiving
/// node can determine which Raft method to invoke.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRpcMessage {
    /// RequestVote RPC
    Vote(VoteRequest<NodeId>),
    /// AppendEntries RPC
    AppendEntries(AppendEntriesRequest<WormFsTypeConfig>),
    /// InstallSnapshot RPC
    InstallSnapshot(InstallSnapshotRequest<WormFsTypeConfig>),
}

/// RPC response wrapper that matches the request type.
///
/// This enum wraps the various Raft RPC response types for serialization.
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftRpcResponse {
    /// Response to RequestVote
    Vote(VoteResponse<NodeId>),
    /// Response to AppendEntries
    AppendEntries(AppendEntriesResponse<NodeId>),
    /// Response to InstallSnapshot
    InstallSnapshot(InstallSnapshotResponse<NodeId>),
}

/// Network client for communicating with a specific Raft cluster member.
///
/// Each instance is lightweight and represents communication to one target node.
/// Multiple instances share the same underlying libp2p network infrastructure.
#[derive(Clone)]
pub struct RaftMember {
    /// The NodeId of the target Raft member
    target_node_id: NodeId,
    /// The PeerId of the target for libp2p communication
    target_peer_id: PeerId,
    /// Shared handle to the storage network
    network: StorageNetworkHandle,
}

impl RaftMember {
    /// Create a new RaftMember for communicating with a specific cluster member.
    ///
    /// # Arguments
    ///
    /// * `target_node_id` - The NodeId of the target Raft member
    /// * `target_peer_id` - The PeerId of the target for libp2p communication
    /// * `network` - Shared handle to the storage network
    pub fn new(
        target_node_id: NodeId,
        target_peer_id: PeerId,
        network: StorageNetworkHandle,
    ) -> Self {
        Self {
            target_node_id,
            target_peer_id,
            network,
        }
    }

    /// Send a Raft RPC and wait for the response.
    ///
    /// This is a helper method that handles serialization, network communication,
    /// and deserialization for all Raft RPC types.
    async fn send_rpc<Req, Resp>(
        &self,
        rpc_message: RaftRpcMessage,
        rpc_name: &str,
    ) -> Result<Resp, RPCError<NodeId, WormFsNode, RaftError<NodeId>>>
    where
        Resp: for<'de> Deserialize<'de>,
    {
        // Serialize the request
        let request_bytes = bincode::serialize(&rpc_message).map_err(|e| {
            error!("Failed to serialize {} request: {:?}", rpc_name, e);
            RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Serialization error: {:?}", e),
            )))
        })?;

        debug!(
            "Sending {} to node {:?} (peer {})",
            rpc_name, self.target_node_id, self.target_peer_id
        );

        // Convert libp2p::PeerId to storage_network::types::PeerId
        let network_peer_id = NetworkPeerId::new(self.target_peer_id.to_bytes());

        // Send via request-response protocol
        let response_bytes = self
            .network
            .send_request(&network_peer_id, "/wormfs/raft/1.0.0", request_bytes)
            .await
            .map_err(|e| {
                warn!(
                    "Failed to send {} to node {:?}: {:?}",
                    rpc_name, self.target_node_id, e
                );
                // Convert StorageNetwork error to RPCError
                match e {
                    crate::storage_network::types::Error::PeerNotConnected(_) => {
                        RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                            std::io::ErrorKind::NotConnected,
                            "Peer not connected",
                        )))
                    }
                    crate::storage_network::types::Error::RequestTimeout { .. } => {
                        RPCError::Network(NetworkError::new(&std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "Request timed out",
                        )))
                    }
                    _ => RPCError::Network(NetworkError::new(&std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Network error: {:?}", e),
                    ))),
                }
            })?;

        // Deserialize the response
        let response: Resp = bincode::deserialize(&response_bytes).map_err(|e| {
            error!("Failed to deserialize {} response: {:?}", rpc_name, e);
            RPCError::Network(NetworkError::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Deserialization error: {:?}", e),
            )))
        })?;

        debug!(
            "Received {} response from node {:?}",
            rpc_name, self.target_node_id
        );

        Ok(response)
    }
}

impl RaftNetwork<WormFsTypeConfig> for RaftMember {
    /// Send an AppendEntries RPC to the target node.
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<WormFsTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, WormFsNode, RaftError<NodeId>>>
    {
        let message = RaftRpcMessage::AppendEntries(rpc);
        let response: RaftRpcResponse = self
            .send_rpc::<RaftRpcMessage, RaftRpcResponse>(message, "append_entries")
            .await?;

        match response {
            RaftRpcResponse::AppendEntries(resp) => Ok(resp),
            _ => {
                error!("Received unexpected response type for append_entries");
                Err(RPCError::Network(NetworkError::new(&std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Unexpected response type",
                ))))
            }
        }
    }

    /// Send a RequestVote RPC to the target node.
    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, WormFsNode, RaftError<NodeId>>> {
        let message = RaftRpcMessage::Vote(rpc);
        let response: RaftRpcResponse = self
            .send_rpc::<RaftRpcMessage, RaftRpcResponse>(message, "vote")
            .await?;

        match response {
            RaftRpcResponse::Vote(resp) => Ok(resp),
            _ => {
                error!("Received unexpected response type for vote");
                Err(RPCError::Network(NetworkError::new(&std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Unexpected response type",
                ))))
            }
        }
    }

    /// Send an InstallSnapshot RPC to the target node.
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<WormFsTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, WormFsNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let message = RaftRpcMessage::InstallSnapshot(rpc);

        // Call send_rpc and convert the error type
        let response_result = self
            .send_rpc::<RaftRpcMessage, RaftRpcResponse>(message, "install_snapshot")
            .await;
        let response: RaftRpcResponse = match response_result {
            Ok(r) => r,
            Err(e) => {
                return Err(match e {
                    RPCError::Unreachable(u) => RPCError::Unreachable(u),
                    RPCError::Network(n) => RPCError::Network(n),
                    RPCError::Timeout(t) => RPCError::Timeout(t),
                    RPCError::PayloadTooLarge(p) => RPCError::PayloadTooLarge(p),
                    // RemoteError contains RaftError<NodeId>, need to convert to RaftError<NodeId, InstallSnapshotError>
                    // For now, just convert to Network error since we can't easily convert the inner type
                    RPCError::RemoteError(_) => {
                        RPCError::Network(NetworkError::new(&std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Remote error during install_snapshot",
                        )))
                    }
                });
            }
        };

        match response {
            RaftRpcResponse::InstallSnapshot(resp) => Ok(resp),
            _ => {
                error!("Received unexpected response type for install_snapshot");
                Err(RPCError::Network(NetworkError::new(&std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Unexpected response type",
                ))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_message_serialization() {
        // Test that RaftRpcMessage can be serialized and deserialized
        let vote_req = VoteRequest {
            vote: openraft::Vote::new(1, NodeId(1)),
            last_log_id: None,
        };
        let message = RaftRpcMessage::Vote(vote_req);

        let serialized = bincode::serialize(&message).unwrap();
        let deserialized: RaftRpcMessage = bincode::deserialize(&serialized).unwrap();

        match deserialized {
            RaftRpcMessage::Vote(_) => {} // Success
            _ => panic!("Wrong RPC type after deserialization"),
        }
    }

    #[test]
    fn test_rpc_response_serialization() {
        // Test that RaftRpcResponse can be serialized and deserialized
        let vote_resp = VoteResponse {
            vote: openraft::Vote::new(1, NodeId(1)),
            vote_granted: true,
            last_log_id: None,
        };
        let response = RaftRpcResponse::Vote(vote_resp);

        let serialized = bincode::serialize(&response).unwrap();
        let deserialized: RaftRpcResponse = bincode::deserialize(&serialized).unwrap();

        match deserialized {
            RaftRpcResponse::Vote(_) => {} // Success
            _ => panic!("Wrong response type after deserialization"),
        }
    }
}
