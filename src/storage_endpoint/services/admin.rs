//! AdminService gRPC implementation.
//!
//! Handles cluster management operations including both read-only status queries
//! and mutation operations (add/remove nodes, trigger maintenance, etc.).
//!
//! Note: This duplicates some functionality from the HTTP Admin server, but that's intentional:
//! - gRPC API is for programmatic access (automation, CLIs, SDKs)
//! - HTTP API is for the web UI (browser-based monitoring)

use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use super::conversions::{raft_error_to_status, storage_node_error_to_status};
use crate::storage_endpoint::proto::wormfs::admin::admin_service_server::AdminService;
use crate::storage_endpoint::proto::wormfs::admin::*;
use crate::storage_node::StorageNode;
use crate::storage_raft_member::StorageRaftMember;

/// AdminService gRPC implementation.
///
/// Provides both read-only monitoring and cluster mutation operations.
pub struct AdminServiceImpl<R, N>
where
    R: StorageRaftMember,
    N: StorageNode,
{
    raft_member: Arc<R>,
    storage_node: Arc<N>,
}

impl<R, N> AdminServiceImpl<R, N>
where
    R: StorageRaftMember,
    N: StorageNode,
{
    /// Create a new AdminService.
    ///
    /// # Arguments
    ///
    /// * `raft_member` - StorageRaftMember for Raft operations
    /// * `storage_node` - StorageNode for node operations
    pub fn new(raft_member: Arc<R>, storage_node: Arc<N>) -> Self {
        Self {
            raft_member,
            storage_node,
        }
    }
}

#[tonic::async_trait]
impl<R, N> AdminService for AdminServiceImpl<R, N>
where
    R: StorageRaftMember + 'static,
    N: StorageNode + 'static,
{
    // ===== Read-only Operations =====

    async fn get_cluster_status(
        &self,
        _request: Request<GetClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        debug!("GetClusterStatus request");

        // Get Raft metrics for cluster info
        let raft_metrics = self.raft_member.get_metrics();

        // Build node status list from Raft members
        let mut nodes = Vec::new();

        // Add this node
        // TODO: Get actual node_id from StorageNode
        nodes.push(NodeStatus {
            node_id: "this_node".to_string(), // TODO: Get actual node ID
            address: String::new(),           // TODO: Get actual address from config
            state: format!("{:?}", raft_metrics.role),
            is_leader: self.raft_member.is_leader(),
        });

        // Add other nodes from Raft membership
        // TODO: Get actual node addresses from cluster membership

        let healthy_nodes = nodes.iter().filter(|n| !n.state.contains("Failed")).count();

        Ok(Response::new(ClusterStatusResponse {
            node_count: nodes.len() as u32,
            healthy_nodes: healthy_nodes as u32,
            leader_id: raft_metrics
                .leader_id
                .map(|id| format!("{}", id))
                .unwrap_or_default(),
            nodes,
        }))
    }

    async fn get_node_health(
        &self,
        _request: Request<GetNodeHealthRequest>,
    ) -> Result<Response<NodeHealthResponse>, Status> {
        debug!("GetNodeHealth request");

        // Get node status
        let status = self.storage_node.get_status();

        // TODO: Parse status to extract warnings and errors
        // For now, assume healthy

        Ok(Response::new(NodeHealthResponse {
            healthy: true,
            warnings: vec![],
            errors: vec![],
        }))
    }

    async fn list_leaders(
        &self,
        _request: Request<ListLeadersRequest>,
    ) -> Result<Response<ListLeadersResponse>, Status> {
        debug!("ListLeaders request");

        let raft_metrics = self.raft_member.get_metrics();
        let current_leader = raft_metrics
            .leader_id
            .map(|id| format!("{}", id))
            .unwrap_or_default();

        Ok(Response::new(ListLeadersResponse {
            leader_addresses: vec![current_leader.clone()],
            current_leader,
        }))
    }

    async fn get_storage_stats(
        &self,
        _request: Request<GetStorageStatsRequest>,
    ) -> Result<Response<GetStorageStatsResponse>, Status> {
        debug!("GetStorageStats request");

        // TODO: Get actual storage stats from FileStore
        // For now, return placeholder values

        Ok(Response::new(GetStorageStatsResponse {
            total_capacity: 0,
            used_capacity: 0,
            available_capacity: 0,
            total_chunks: 0,
            total_stripes: 0,
            node_stats: vec![],
        }))
    }

    async fn get_metrics(
        &self,
        _request: Request<GetMetricsRequest>,
    ) -> Result<Response<MetricsResponse>, Status> {
        debug!("GetMetrics request");

        let raft_metrics = self.raft_member.get_metrics();

        let mut metrics_map = HashMap::new();
        metrics_map.insert("raft_term".to_string(), raft_metrics.current_term as f64);
        metrics_map.insert(
            "raft_commit_index".to_string(),
            raft_metrics.commit_index as f64,
        );
        metrics_map.insert(
            "raft_last_applied".to_string(),
            raft_metrics.last_applied as f64,
        );
        metrics_map.insert(
            "raft_last_log_index".to_string(),
            raft_metrics.last_log_index as f64,
        );
        metrics_map.insert(
            "raft_is_leader".to_string(),
            if self.raft_member.is_leader() {
                1.0
            } else {
                0.0
            },
        );

        Ok(Response::new(MetricsResponse {
            metrics: metrics_map,
        }))
    }

    // ===== Cluster Mutation Operations =====

    async fn add_node(
        &self,
        request: Request<AddNodeRequest>,
    ) -> Result<Response<AddNodeResponse>, Status> {
        let req = request.into_inner();
        info!(
            "AddNode request: node_id={}, address={}",
            req.node_id, req.address
        );

        // Parse node_id to u64
        let node_id_val: u64 = req
            .node_id
            .parse()
            .map_err(|_| Status::invalid_argument("node_id must be a valid integer"))?;
        let node_id = crate::storage_raft_member::NodeId(node_id_val);

        // Parse address to SocketAddr
        let socket_addr: std::net::SocketAddr = req
            .address
            .parse()
            .map_err(|_| Status::invalid_argument("address must be a valid socket address"))?;

        // Add node via Raft
        // TODO: peer_id should come from request or be looked up
        let peer_id = format!("peer-{}", node_id_val);

        self.raft_member
            .add_node(node_id, socket_addr, peer_id)
            .await
            .map_err(raft_error_to_status)?;

        info!("Node {} added successfully", node_id_val);

        Ok(Response::new(AddNodeResponse { success: true }))
    }

    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<RemoveNodeResponse>, Status> {
        let req = request.into_inner();
        info!("RemoveNode request: node_id={}", req.node_id);

        // Parse node_id to u64
        let node_id_val: u64 = req
            .node_id
            .parse()
            .map_err(|_| Status::invalid_argument("node_id must be a valid integer"))?;
        let node_id = crate::storage_raft_member::NodeId(node_id_val);

        // Remove node via Raft
        self.raft_member
            .remove_node(node_id)
            .await
            .map_err(raft_error_to_status)?;

        info!("Node {} removed successfully", node_id_val);

        Ok(Response::new(RemoveNodeResponse { success: true }))
    }

    async fn set_storage_policy(
        &self,
        request: Request<SetStoragePolicyRequest>,
    ) -> Result<Response<SetStoragePolicyResponse>, Status> {
        let req = request.into_inner();
        error!(
            "SetStoragePolicy request: {:?} - NOT IMPLEMENTED",
            req.policy
        );

        // TODO: Implement storage policy update
        // This would need to propagate through Raft to update default policy
        Err(Status::unimplemented(
            "SetStoragePolicy not yet implemented",
        ))
    }

    async fn trigger_rebalance(
        &self,
        request: Request<TriggerRebalanceRequest>,
    ) -> Result<Response<TriggerRebalanceResponse>, Status> {
        let req = request.into_inner();
        error!(
            "TriggerRebalance request: force={}, targets={} - NOT IMPLEMENTED",
            req.force,
            req.target_nodes.len()
        );

        // TODO: Implement data rebalancing trigger
        // This would coordinate with StorageWatchdog or a dedicated rebalancer component
        Err(Status::unimplemented(
            "TriggerRebalance not yet implemented",
        ))
    }

    // ===== Maintenance Operations =====

    type StartScrubStream =
        tokio_stream::wrappers::ReceiverStream<Result<ScrubProgressResponse, Status>>;

    async fn start_scrub(
        &self,
        request: Request<StartScrubRequest>,
    ) -> Result<Response<Self::StartScrubStream>, Status> {
        let req = request.into_inner();
        info!(
            "StartScrub request: deep_scan={}, node_ids={}",
            req.deep_scan,
            req.node_ids.len()
        );

        let (tx, rx) = tokio::sync::mpsc::channel(32);

        // TODO: Implement data scrubbing with progress streaming
        // This would coordinate with StorageWatchdog's verification system
        tokio::spawn(async move {
            warn!("StartScrub not yet fully implemented - returning placeholder");
            let _ = tx
                .send(Ok(ScrubProgressResponse {
                    node_id: String::new(),
                    chunks_scanned: 0,
                    chunks_total: 0,
                    errors_found: 0,
                    errors_repaired: 0,
                    complete: true,
                }))
                .await;
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn create_snapshot(
        &self,
        request: Request<CreateSnapshotRequest>,
    ) -> Result<Response<CreateSnapshotResponse>, Status> {
        let req = request.into_inner();
        info!(
            "CreateSnapshot request: name={}, metadata_count={}",
            req.name,
            req.metadata.len()
        );

        // Trigger snapshot via Raft
        self.raft_member
            .trigger_snapshot()
            .await
            .map_err(raft_error_to_status)?;

        // TODO: Get actual snapshot ID from result
        let snapshot_id = 0;

        info!("Snapshot {} created successfully", snapshot_id);

        Ok(Response::new(CreateSnapshotResponse {
            snapshot_id,
            success: true,
        }))
    }

    type RestoreSnapshotStream =
        tokio_stream::wrappers::ReceiverStream<Result<RestoreProgressResponse, Status>>;

    async fn restore_snapshot(
        &self,
        request: Request<RestoreSnapshotRequest>,
    ) -> Result<Response<Self::RestoreSnapshotStream>, Status> {
        let req = request.into_inner();
        info!("RestoreSnapshot request: snapshot_id={}", req.snapshot_id);

        let (tx, rx) = tokio::sync::mpsc::channel(32);

        // TODO: Implement snapshot restoration with progress streaming
        // This would coordinate with SnapshotStore and MetadataStore
        tokio::spawn(async move {
            warn!("RestoreSnapshot not yet fully implemented - returning placeholder");
            let _ = tx
                .send(Ok(RestoreProgressResponse {
                    bytes_restored: 0,
                    bytes_total: 0,
                    progress_percent: 0.0,
                    complete: true,
                    error: None,
                }))
                .await;
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_node::MockStorageNode;
    use crate::storage_raft_member::MockStorageRaftMember;

    #[tokio::test]
    async fn test_add_node() {
        let mut mock_raft = MockStorageRaftMember::default();
        mock_raft.expect_add_node().returning(|_, _, _| Ok(()));

        let mock_node = MockStorageNode::default();
        let service = AdminServiceImpl::new(Arc::new(mock_raft), Arc::new(mock_node));

        let request = Request::new(AddNodeRequest {
            node_id: "2".to_string(),
            address: "127.0.0.1:7001".to_string(),
        });

        let response = service.add_node(request).await;
        assert!(response.is_ok());
        assert!(response.unwrap().into_inner().success);
    }

    #[tokio::test]
    async fn test_remove_node() {
        let mut mock_raft = MockStorageRaftMember::default();
        mock_raft.expect_remove_node().returning(|_| Ok(()));

        let mock_node = MockStorageNode::default();
        let service = AdminServiceImpl::new(Arc::new(mock_raft), Arc::new(mock_node));

        let request = Request::new(RemoveNodeRequest {
            node_id: "2".to_string(),
        });

        let response = service.remove_node(request).await;
        assert!(response.is_ok());
        assert!(response.unwrap().into_inner().success);
    }

    #[tokio::test]
    async fn test_create_snapshot() {
        let mut mock_raft = MockStorageRaftMember::default();
        mock_raft.expect_trigger_snapshot().returning(|| Ok(()));

        let mock_node = MockStorageNode::default();
        let service = AdminServiceImpl::new(Arc::new(mock_raft), Arc::new(mock_node));

        let request = Request::new(CreateSnapshotRequest {
            name: "test-snapshot".to_string(),
            metadata: HashMap::new(),
        });

        let response = service.create_snapshot(request).await;
        assert!(response.is_ok());
        assert!(response.unwrap().into_inner().success);
    }
}
