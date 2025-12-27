//! ChunkService gRPC implementation.
//!
//! Handles chunk storage, retrieval, verification and repair operations,
//! delegating to the FileStore component.

use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use super::conversions::{
    bytes_to_chunk_id, bytes_to_file_id, bytes_to_stripe_id,
    filestore_error_to_status, proto_to_compression_algorithm, proto_to_erasure_algorithm,
};
use super::GRPC_STREAM_CHANNEL_BUFFER_SIZE;
use crate::file_store::{ChunkData, ChunkHeader, DiskId, FileStore};
use crate::storage_endpoint::proto::wormfs::chunk::chunk_service_server::ChunkService;
use crate::storage_endpoint::proto::wormfs::chunk::*;

/// ChunkService gRPC implementation.
///
/// Delegates chunk operations to the FileStore component.
pub struct ChunkServiceImpl<F: FileStore> {
    file_store: Arc<F>,
}

impl<F: FileStore> ChunkServiceImpl<F> {
    /// Create a new ChunkService.
    ///
    /// # Arguments
    ///
    /// * `file_store` - FileStore instance for chunk operations
    pub fn new(file_store: Arc<F>) -> Self {
        Self { file_store }
    }
}

#[tonic::async_trait]
impl<F: FileStore + 'static> ChunkService for ChunkServiceImpl<F> {
    async fn read_chunk(
        &self,
        request: Request<ReadChunkRequest>,
    ) -> Result<Response<ReadChunkResponse>, Status> {
        let req = request.into_inner();
        debug!("ReadChunk request");

        // Parse IDs from request
        let chunk_id = bytes_to_chunk_id(&req.chunk_id)?;
        let file_id = bytes_to_file_id(&req.file_id)?;
        let stripe_id = bytes_to_stripe_id(&req.stripe_id)?;
        let disk_id = DiskId(req.disk_id);

        // Resolve disk_id to disk_path
        let disk_path = self
            .file_store
            .get_disk_path(disk_id)
            .await
            .map_err(|e| Status::not_found(format!("Disk {} not found: {}", disk_id.0, e)))?;

        // Read chunk using the path components
        let chunk_data = self
            .file_store
            .read_chunk_from_disk(&disk_path, file_id, stripe_id, chunk_id)
            .await
            .map_err(filestore_error_to_status)?;

        // Serialize header for response
        let header_data = bincode::serialize(&chunk_data.header)
            .map_err(|e| Status::internal(format!("Failed to serialize header: {}", e)))?;

        Ok(Response::new(ReadChunkResponse {
            chunk_data: chunk_data.data,
            header_data,
        }))
    }

    async fn check_chunk(
        &self,
        request: Request<CheckChunkRequest>,
    ) -> Result<Response<CheckChunkResponse>, Status> {
        let req = request.into_inner();
        debug!("CheckChunk request");

        let chunk_id = bytes_to_chunk_id(&req.chunk_id)?;

        // Try to read the chunk locally to check if it exists
        match self.file_store.read_chunk_local(chunk_id).await {
            Ok(chunk_data) => {
                let size = chunk_data.data.len() as u64;
                Ok(Response::new(CheckChunkResponse { exists: true, size }))
            }
            Err(crate::file_store::Error::ChunkNotFound(_)) => {
                Ok(Response::new(CheckChunkResponse {
                    exists: false,
                    size: 0,
                }))
            }
            Err(e) => Err(Status::internal(format!("Failed to check chunk: {}", e))),
        }
    }

    async fn verify_chunk(
        &self,
        request: Request<VerifyChunkRequest>,
    ) -> Result<Response<VerifyChunkResponse>, Status> {
        let req = request.into_inner();
        debug!("VerifyChunk request");

        let chunk_id = bytes_to_chunk_id(&req.chunk_id)?;

        let verification_result = self
            .file_store
            .verify_chunk(chunk_id)
            .await
            .map_err(filestore_error_to_status)?;

        Ok(Response::new(VerifyChunkResponse {
            valid: verification_result.checksum_valid && verification_result.readable,
            checksum: verification_result.error.unwrap_or_default(),
        }))
    }

    async fn delete_chunk(
        &self,
        request: Request<DeleteChunkRequest>,
    ) -> Result<Response<DeleteChunkResponse>, Status> {
        let req = request.into_inner();
        debug!("DeleteChunk request");

        let chunk_id = bytes_to_chunk_id(&req.chunk_id)?;

        // Delete via discard_staged_chunk (works for both staged and active chunks)
        self.file_store
            .discard_staged_chunk(chunk_id)
            .await
            .map_err(filestore_error_to_status)?;

        debug!("Chunk deleted successfully");

        Ok(Response::new(DeleteChunkResponse { success: true }))
    }

    async fn store_chunk(
        &self,
        request: Request<StoreChunkRequest>,
    ) -> Result<Response<StoreChunkResponse>, Status> {
        let req = request.into_inner();
        debug!("StoreChunk request: is_staging={}", req.is_staging);

        let chunk = req
            .chunk
            .ok_or_else(|| Status::invalid_argument("chunk is required"))?;

        // Extract and validate all IDs
        let chunk_id = bytes_to_chunk_id(&chunk.chunk_id)?;
        let stripe_id = bytes_to_stripe_id(&chunk.stripe_id)?;
        let file_id = bytes_to_file_id(&chunk.file_id)?;

        // Convert algorithms from proto enums
        let erasure_algorithm = proto_to_erasure_algorithm(chunk.erasure_algorithm)?;
        let compression_algorithm = proto_to_compression_algorithm(chunk.compression_algorithm)?;

        // Create ChunkData with all fields from proto
        let chunk_data = ChunkData {
            header: ChunkHeader {
                magic: [b'W', b'O', b'R', b'M'],
                format_version: 1,
                chunk_checksum: chunk.chunk_checksum,
                chunk_id,
                stripe_id,
                file_id,
                stripe_start_offset: chunk.stripe_start_offset,
                stripe_end_offset: chunk.stripe_end_offset,
                chunk_index: chunk.chunk_index as u8,
                data_shards: chunk.data_shards as u8,
                parity_shards: chunk.parity_shards as u8,
                erasure_algorithm,
                compression_algorithm,
                stripe_checksum: chunk.stripe_checksum,
            },
            data: chunk.data,
        };

        if req.is_staging {
            self.file_store
                .stage_chunk(chunk_data)
                .await
                .map_err(filestore_error_to_status)?;
        } else {
            self.file_store
                .write_chunk_local(chunk_id, chunk_data)
                .await
                .map_err(filestore_error_to_status)?;
        }

        debug!("Chunk stored successfully");

        Ok(Response::new(StoreChunkResponse {
            success: true,
            error: None,
        }))
    }

    type RetrieveChunkStream =
        tokio_stream::wrappers::ReceiverStream<Result<RetrieveChunkResponse, Status>>;

    async fn retrieve_chunk(
        &self,
        request: Request<RetrieveChunkRequest>,
    ) -> Result<Response<Self::RetrieveChunkStream>, Status> {
        let req = request.into_inner();
        debug!("RetrieveChunk request");

        let _chunk_id = bytes_to_chunk_id(&req.chunk_id)?;

        // TODO: retrieve_chunk needs file_id/stripe_id/disk_id like read_chunk does
        // For now, return not implemented
        Err(Status::unimplemented(
            "retrieve_chunk requires file_id/stripe_id/disk_id parameters",
        ))
    }

    async fn batch_store(
        &self,
        request: Request<tonic::Streaming<BatchStoreRequest>>,
    ) -> Result<Response<BatchStoreResponse>, Status> {
        debug!("BatchStore request");

        let mut stream = request.into_inner();
        let mut chunks_stored = 0u32;
        let mut expected_count: Option<u32> = None;
        let mut transaction_id: Option<String> = None;
        let mut header_received = false;

        // Process each message in the stream
        while let Some(req) = stream.message().await? {
            // Handle oneof pattern
            match req.request {
                Some(batch_store_request::Request::Header(header)) => {
                    if header_received {
                        return Err(Status::invalid_argument("Duplicate header in batch"));
                    }
                    if chunks_stored > 0 {
                        return Err(Status::invalid_argument(
                            "Header must be first message in batch",
                        ));
                    }
                    debug!(
                        "Batch header: transaction_id={}, chunk_count={}",
                        header.transaction_id, header.chunk_count
                    );
                    header_received = true;
                    expected_count = Some(header.chunk_count);
                    transaction_id = Some(header.transaction_id);
                }
                Some(batch_store_request::Request::Chunk(chunk)) => {
                    if !header_received {
                        return Err(Status::invalid_argument(
                            "Header required before chunks in batch",
                        ));
                    }

                    let chunk_id = bytes_to_chunk_id(&chunk.chunk_id)?;
                    let stripe_id = bytes_to_stripe_id(&chunk.stripe_id)?;

                    let chunk_data = ChunkData {
                        header: ChunkHeader {
                            magic: [b'W', b'O', b'R', b'M'],
                            format_version: 1,
                            chunk_checksum: chunk.chunk_checksum,
                            chunk_id,
                            stripe_id,
                            file_id: bytes_to_file_id(&chunk.file_id)?,
                            stripe_start_offset: chunk.stripe_start_offset,
                            stripe_end_offset: chunk.stripe_end_offset,
                            chunk_index: chunk.chunk_index as u8,
                            data_shards: chunk.data_shards as u8,
                            parity_shards: chunk.parity_shards as u8,
                            erasure_algorithm: crate::file_store::ErasureAlgorithm::ReedSolomon,
                            compression_algorithm: crate::file_store::CompressionAlgorithm::None,
                            stripe_checksum: chunk.stripe_checksum,
                        },
                        data: chunk.data,
                    };

                    self.file_store
                        .write_chunk_local(chunk_id, chunk_data)
                        .await
                        .map_err(filestore_error_to_status)?;

                    chunks_stored += 1;
                }
                None => {
                    return Err(Status::invalid_argument("request field is required"));
                }
            }
        }

        // Validate chunk count matches declared count
        if let Some(expected) = expected_count {
            if chunks_stored != expected {
                warn!(
                    "Batch chunk count mismatch: expected {}, received {}",
                    expected, chunks_stored
                );
                return Err(Status::failed_precondition(format!(
                    "Chunk count mismatch: expected {}, received {}",
                    expected, chunks_stored
                )));
            }
        }

        if let Some(txn_id) = transaction_id {
            info!(
                "Batch store completed: transaction_id={}, chunks={}",
                txn_id, chunks_stored
            );
        } else {
            info!("Batch store completed: {} chunks", chunks_stored);
        }

        Ok(Response::new(BatchStoreResponse {
            chunks_stored,
            errors: vec![],
        }))
    }

    async fn list_chunks(
        &self,
        request: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResponse>, Status> {
        let req = request.into_inner();
        debug!("ListChunks request: page_size={}", req.page_size);

        // TODO: Implement chunk listing
        // This would require FileStore to maintain a chunk index
        warn!("ListChunks not yet fully implemented");

        Ok(Response::new(ListChunksResponse {
            chunks: vec![],
            next_page_token: String::new(),
        }))
    }

    async fn repair_chunk(
        &self,
        request: Request<RepairChunkRequest>,
    ) -> Result<Response<RepairChunkResponse>, Status> {
        let req = request.into_inner();
        let chunk_id = bytes_to_chunk_id(&req.chunk_id)?;
        let stripe_id = bytes_to_stripe_id(&req.stripe_id)?;

        error!(
            "RepairChunk request for chunk_id={:?}, stripe_id={:?} - NOT IMPLEMENTED",
            chunk_id, stripe_id
        );

        // TODO: Implement chunk repair
        // This would use rebuild_stripe to reconstruct the missing/corrupt chunk
        Err(Status::unimplemented("RepairChunk not yet implemented"))
    }

    type RebalanceChunksStream =
        tokio_stream::wrappers::ReceiverStream<Result<RebalanceResponse, Status>>;

    async fn rebalance_chunks(
        &self,
        request: Request<RebalanceRequest>,
    ) -> Result<Response<Self::RebalanceChunksStream>, Status> {
        let req = request.into_inner();
        debug!("RebalanceChunks request: force={}", req.force);

        let (tx, rx) = tokio::sync::mpsc::channel(GRPC_STREAM_CHANNEL_BUFFER_SIZE);

        // TODO: Implement chunk rebalancing with progress streaming
        tokio::spawn(async move {
            warn!("RebalanceChunks not yet fully implemented");
            let _ = tx
                .send(Ok(RebalanceResponse {
                    chunks_moved: 0,
                    chunks_remaining: 0,
                    progress_percent: 100.0,
                    complete: true,
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
    use crate::file_store::MockFileStore;

    #[tokio::test]
    async fn test_check_chunk_exists() {
        let mut mock_store = MockFileStore::default();

        let test_chunk_id = crate::file_store::ChunkId(uuid::Uuid::new_v4());
        let test_chunk_data = ChunkData {
            header: ChunkHeader {
                magic: [b'W', b'O', b'R', b'M'],
                format_version: 1,
                chunk_checksum: 0,
                chunk_id: test_chunk_id,
                stripe_id: crate::file_store::StripeId(uuid::Uuid::new_v4()),
                file_id: crate::file_store::FileId(uuid::Uuid::new_v4()),
                stripe_start_offset: 0,
                stripe_end_offset: 1024,
                chunk_index: 0,
                data_shards: 4,
                parity_shards: 2,
                erasure_algorithm: crate::file_store::ErasureAlgorithm::ReedSolomon,
                compression_algorithm: crate::file_store::CompressionAlgorithm::None,
                stripe_checksum: 0,
            },
            data: vec![1, 2, 3, 4],
        };

        mock_store
            .expect_read_chunk_local()
            .returning(move |_| Ok(test_chunk_data.clone()));

        let service = ChunkServiceImpl::new(Arc::new(mock_store));

        let request = Request::new(CheckChunkRequest {
            chunk_id: test_chunk_id.0.as_bytes().to_vec(),
        });

        let response = service.check_chunk(request).await;
        assert!(response.is_ok());
        let inner = response.unwrap().into_inner();
        assert!(inner.exists);
        assert_eq!(inner.size, 4);
    }

    #[tokio::test]
    async fn test_check_chunk_not_exists() {
        let mut mock_store = MockFileStore::default();

        mock_store
            .expect_read_chunk_local()
            .returning(|chunk_id| Err(crate::file_store::Error::ChunkNotFound(chunk_id)));

        let service = ChunkServiceImpl::new(Arc::new(mock_store));

        let request = Request::new(CheckChunkRequest {
            chunk_id: vec![0; 16],
        });

        let response = service.check_chunk(request).await;
        assert!(response.is_ok());
        let inner = response.unwrap().into_inner();
        assert!(!inner.exists);
        assert_eq!(inner.size, 0);
    }
}
