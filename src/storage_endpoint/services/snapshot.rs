//! SnapshotService gRPC implementation.
//!
//! Handles Raft snapshot streaming between storage nodes,
//! delegating to the SnapshotStore component.

use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use super::conversions::snapshot_error_to_status;
use super::GRPC_STREAM_CHANNEL_BUFFER_SIZE;
use crate::snapshot_store::SnapshotStore;
use crate::storage_endpoint::proto::wormfs::snapshot::snapshot_service_server::SnapshotService;
use crate::storage_endpoint::proto::wormfs::snapshot::*;

/// SnapshotService gRPC implementation.
///
/// Delegates snapshot operations to the SnapshotStore component.
pub struct SnapshotServiceImpl<S: SnapshotStore> {
    snapshot_store: Arc<S>,
}

impl<S: SnapshotStore> SnapshotServiceImpl<S> {
    /// Create a new SnapshotService.
    ///
    /// # Arguments
    ///
    /// * `snapshot_store` - SnapshotStore instance for snapshot operations
    pub fn new(snapshot_store: Arc<S>) -> Self {
        Self { snapshot_store }
    }
}

#[tonic::async_trait]
impl<S: SnapshotStore + 'static> SnapshotService for SnapshotServiceImpl<S> {
    async fn get_latest_snapshot(
        &self,
        _request: Request<GetLatestSnapshotRequest>,
    ) -> Result<Response<SnapshotInfo>, Status> {
        debug!("GetLatestSnapshot request");

        let snapshot_info = self
            .snapshot_store
            .get_latest_snapshot()
            .await
            .map_err(snapshot_error_to_status)?;

        match snapshot_info {
            Some(info) => Ok(Response::new(SnapshotInfo {
                snapshot_id: info.snapshot_id,
                log_index: info.log_index,
                log_term: info.log_term,
                created_at: info
                    .timestamp
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
                size_bytes: 0, // TODO: Add size tracking to SnapshotInfo
            })),
            None => Err(Status::not_found("No snapshots available")),
        }
    }

    type StreamSnapshotStream =
        tokio_stream::wrappers::ReceiverStream<Result<SnapshotChunk, Status>>;

    async fn stream_snapshot(
        &self,
        request: Request<StreamSnapshotRequest>,
    ) -> Result<Response<Self::StreamSnapshotStream>, Status> {
        let req = request.into_inner();
        info!("StreamSnapshot request: snapshot_id={}", req.snapshot_id);

        let snapshot_store = self.snapshot_store.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(GRPC_STREAM_CHANNEL_BUFFER_SIZE);

        // Spawn task to stream snapshot data
        tokio::spawn(async move {
            // Create a pipe for streaming
            let (mut writer, mut reader) = tokio::io::duplex(64 * 1024);

            // Spawn snapshot streaming task
            let stream_task = tokio::spawn(async move {
                snapshot_store
                    .stream_snapshot(req.snapshot_id, Box::new(writer))
                    .await
            });

            // Stream chunks to client
            let mut offset = 0u64;
            let mut buffer = vec![0u8; 64 * 1024];

            loop {
                match tokio::io::AsyncReadExt::read(&mut reader, &mut buffer).await {
                    Ok(0) => {
                        // EOF - send final chunk
                        let _ = tx
                            .send(Ok(SnapshotChunk {
                                data: vec![],
                                offset,
                                last_chunk: true,
                            }))
                            .await;
                        break;
                    }
                    Ok(n) => {
                        let chunk = SnapshotChunk {
                            data: buffer[..n].to_vec(),
                            offset,
                            last_chunk: false,
                        };
                        offset += n as u64;

                        if tx.send(Ok(chunk)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("Stream error: {}", e))))
                            .await;
                        break;
                    }
                }
            }

            // Wait for stream task to complete
            if let Err(e) = stream_task.await {
                let _ = tx
                    .send(Err(Status::internal(format!(
                        "Snapshot stream failed: {}",
                        e
                    ))))
                    .await;
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn get_snapshot_info(
        &self,
        request: Request<GetSnapshotInfoRequest>,
    ) -> Result<Response<SnapshotInfo>, Status> {
        let req = request.into_inner();
        debug!("GetSnapshotInfo request: snapshot_id={}", req.snapshot_id);

        let snapshot_info = self
            .snapshot_store
            .get_snapshot(req.snapshot_id)
            .await
            .map_err(snapshot_error_to_status)?;

        Ok(Response::new(SnapshotInfo {
            snapshot_id: snapshot_info.snapshot_id,
            log_index: snapshot_info.log_index,
            log_term: snapshot_info.log_term,
            created_at: snapshot_info
                .timestamp
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            size_bytes: 0, // TODO: Add size tracking to SnapshotInfo
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot_store::MockSnapshotStore;

    #[tokio::test]
    async fn test_get_latest_snapshot_none() {
        let mut mock_store = MockSnapshotStore::default();
        mock_store
            .expect_get_latest_snapshot()
            .returning(|| Box::pin(async { Ok(None) }));

        let service = SnapshotServiceImpl::new(Arc::new(mock_store));
        let request = Request::new(GetLatestSnapshotRequest {});

        let response = service.get_latest_snapshot(request).await;
        assert!(response.is_err());
        assert_eq!(response.unwrap_err().code(), tonic::Code::NotFound);
    }
}
