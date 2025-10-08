//! Snapshot transfer service implementation for StorageEndpoint
//!
//! This service provides gRPC-based snapshot streaming to peers for efficient
//! Raft snapshot distribution.

use tokio::io::AsyncReadExt;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

use crate::raft::proto_types::proto::{
    snapshot_transfer_service_server::SnapshotTransferService as SnapshotTransferServiceTrait,
    SnapshotChunk, SnapshotTransferRequest,
};
use crate::storage_endpoint::StorageEndpointConfig;

/// gRPC service implementation for serving snapshots to peers
pub struct SnapshotTransferServiceImpl {
    config: StorageEndpointConfig,
}

impl SnapshotTransferServiceImpl {
    /// Create a new snapshot transfer service
    pub fn new(config: StorageEndpointConfig) -> Self {
        Self { config }
    }

    /// Get the service configuration
    pub fn config(&self) -> &StorageEndpointConfig {
        &self.config
    }
}

#[tonic::async_trait]
impl SnapshotTransferServiceTrait for SnapshotTransferServiceImpl {
    type TransferSnapshotStream =
        tokio_stream::wrappers::ReceiverStream<Result<SnapshotChunk, Status>>;

    async fn transfer_snapshot(
        &self,
        request: Request<SnapshotTransferRequest>,
    ) -> Result<Response<Self::TransferSnapshotStream>, Status> {
        let req = request.into_inner();
        let snapshot_id = req.snapshot_id.clone();
        let requester_node_id = req.requester_node_id;

        info!(
            "Received snapshot transfer request for '{}' from node {}",
            snapshot_id, requester_node_id
        );

        // Build path to snapshot data file
        let data_path = self
            .config
            .snapshot_dir
            .join(format!("{}.data", snapshot_id));

        // Check if snapshot exists
        if !data_path.exists() {
            warn!("Snapshot not found: {}", snapshot_id);
            return Err(Status::not_found(format!(
                "Snapshot '{}' not found",
                snapshot_id
            )));
        }

        // Open the file
        let mut file = tokio::fs::File::open(&data_path).await.map_err(|e| {
            error!("Failed to open snapshot file {}: {}", snapshot_id, e);
            Status::internal(format!("Failed to open snapshot: {}", e))
        })?;

        // Get file size
        let metadata = file.metadata().await.map_err(|e| {
            error!("Failed to get file metadata {}: {}", snapshot_id, e);
            Status::internal(format!("Failed to get metadata: {}", e))
        })?;

        let total_size = metadata.len();
        info!(
            "Serving snapshot '{}' ({} bytes) to node {}",
            snapshot_id, total_size, requester_node_id
        );

        // Create channel for streaming
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        // Spawn task to stream file chunks
        tokio::spawn(async move {
            let chunk_size = 64 * 1024; // 64 KB chunks
            let mut buffer = vec![0u8; chunk_size];
            let mut offset = 0u64;

            loop {
                match file.read(&mut buffer).await {
                    Ok(0) => {
                        // EOF - send final chunk
                        let chunk = SnapshotChunk {
                            snapshot_id: snapshot_id.clone(),
                            offset,
                            data: vec![],
                            is_last: true,
                        };
                        let _ = tx.send(Ok(chunk)).await;
                        info!("Completed streaming snapshot '{}'", snapshot_id);
                        break;
                    }
                    Ok(n) => {
                        let chunk = SnapshotChunk {
                            snapshot_id: snapshot_id.clone(),
                            offset,
                            data: buffer[..n].to_vec(),
                            is_last: false,
                        };

                        if tx.send(Ok(chunk)).await.is_err() {
                            warn!("Client disconnected during snapshot transfer");
                            break;
                        }

                        offset += n as u64;
                    }
                    Err(e) => {
                        error!("Error reading snapshot file: {}", e);
                        let _ = tx
                            .send(Err(Status::internal(format!("Read error: {}", e))))
                            .await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_service_creation() {
        let config = StorageEndpointConfig::new(1, 8082, PathBuf::from("/tmp/snapshots"));
        let service = SnapshotTransferServiceImpl::new(config.clone());
        assert_eq!(service.config().node_id, 1);
        assert_eq!(
            service.config().snapshot_dir,
            PathBuf::from("/tmp/snapshots")
        );
    }
}
