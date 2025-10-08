//! gRPC-based snapshot transfer for efficient Raft snapshot distribution
//!
//! This module provides:
//! - SnapshotTransferService implementation for serving snapshots
//! - SnapshotTransferClient for downloading snapshots
//!
//! Using gRPC for snapshot transfer has several benefits:
//! - Separation from Raft protocol (doesn't congest message channel)
//! - Efficient streaming with built-in flow control
//! - Type-safe API through protobuf definitions
//! - Consistent with overall architecture (using tonic for all services)

use sha2::{Digest, Sha256};
use std::io;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

use crate::raft::proto_types::proto::{
    snapshot_transfer_service_server::SnapshotTransferService as SnapshotTransferServiceTrait,
    SnapshotChunk, SnapshotTransferRequest,
};

/// Configuration for snapshot transfer server
#[derive(Debug, Clone)]
pub struct SnapshotTransferConfig {
    /// Port to listen on for gRPC requests
    pub port: u16,
    /// Local node ID (for authentication)
    pub node_id: u64,
    /// Directory where snapshots are stored
    pub snapshot_dir: PathBuf,
}

/// gRPC service implementation for serving snapshots to peers
pub struct SnapshotTransferServiceImpl {
    config: SnapshotTransferConfig,
}

impl SnapshotTransferServiceImpl {
    /// Create a new snapshot transfer service
    pub fn new(config: SnapshotTransferConfig) -> Self {
        Self { config }
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

/// Client for downloading snapshots from peers via gRPC
pub struct SnapshotTransferClient {
    max_retries: usize,
}

impl SnapshotTransferClient {
    /// Create a new snapshot transfer client
    pub fn new(max_retries: usize) -> Self {
        Self { max_retries }
    }

    /// Download a snapshot from a gRPC endpoint with retry logic
    ///
    /// Downloads to a temporary file, verifies the hash, then moves to final location
    pub async fn download_snapshot(
        &self,
        grpc_address: &str,
        snapshot_id: &str,
        requester_node_id: u64,
        expected_hash: &str,
        destination_dir: &std::path::Path,
    ) -> Result<PathBuf, SnapshotTransferError> {
        let mut last_error = None;

        for attempt in 1..=self.max_retries {
            match self
                .try_download(
                    grpc_address,
                    snapshot_id,
                    requester_node_id,
                    expected_hash,
                    destination_dir,
                )
                .await
            {
                Ok(path) => {
                    info!(
                        "Successfully downloaded snapshot '{}' (attempt {}/{})",
                        snapshot_id, attempt, self.max_retries
                    );
                    return Ok(path);
                }
                Err(e) => {
                    warn!(
                        "Failed to download snapshot '{}' (attempt {}/{}): {}",
                        snapshot_id, attempt, self.max_retries, e
                    );
                    last_error = Some(e);

                    if attempt < self.max_retries {
                        // Exponential backoff
                        let delay = std::time::Duration::from_secs(2u64.pow(attempt as u32 - 1));
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or(SnapshotTransferError::MaxRetriesExceeded))
    }

    /// Single attempt to download a snapshot
    async fn try_download(
        &self,
        grpc_address: &str,
        snapshot_id: &str,
        requester_node_id: u64,
        expected_hash: &str,
        destination_dir: &std::path::Path,
    ) -> Result<PathBuf, SnapshotTransferError> {
        use crate::raft::proto_types::proto::snapshot_transfer_service_client::SnapshotTransferServiceClient;

        info!(
            "Downloading snapshot '{}' from {}",
            snapshot_id, grpc_address
        );

        // Connect to gRPC server
        let mut client = SnapshotTransferServiceClient::connect(grpc_address.to_string())
            .await
            .map_err(|e| SnapshotTransferError::GrpcError(e.to_string()))?;

        // Create request
        let request = Request::new(SnapshotTransferRequest {
            snapshot_id: snapshot_id.to_string(),
            requester_node_id,
        });

        // Start streaming
        let mut stream = client
            .transfer_snapshot(request)
            .await
            .map_err(|e| SnapshotTransferError::GrpcError(e.to_string()))?
            .into_inner();

        // Download to temporary file
        let temp_path = destination_dir.join(format!("{}.tmp", snapshot_id));
        let mut temp_file = tokio::fs::File::create(&temp_path).await?;

        // Stream download and compute hash simultaneously
        let mut hasher = Sha256::new();
        let mut total_bytes = 0u64;

        while let Some(chunk) = stream
            .message()
            .await
            .map_err(|e| SnapshotTransferError::GrpcError(e.to_string()))?
        {
            if !chunk.data.is_empty() {
                hasher.update(&chunk.data);
                tokio::io::AsyncWriteExt::write_all(&mut temp_file, &chunk.data).await?;
                total_bytes += chunk.data.len() as u64;
            }

            if chunk.is_last {
                break;
            }
        }

        tokio::io::AsyncWriteExt::flush(&mut temp_file).await?;
        drop(temp_file);

        // Verify hash
        let computed_hash = format!("{:x}", hasher.finalize());
        if computed_hash != expected_hash {
            // Clean up temp file
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(SnapshotTransferError::HashMismatch {
                expected: expected_hash.to_string(),
                actual: computed_hash,
            });
        }

        info!(
            "Downloaded and verified snapshot '{}' ({} bytes)",
            snapshot_id, total_bytes
        );

        // Move to final location
        let final_path = destination_dir.join(format!("{}.data", snapshot_id));
        tokio::fs::rename(&temp_path, &final_path).await?;

        Ok(final_path)
    }
}

/// Errors that can occur during snapshot transfer
#[derive(Debug, thiserror::Error)]
pub enum SnapshotTransferError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("gRPC error: {0}")]
    GrpcError(String),

    #[error("Hash mismatch - expected: {expected}, got: {actual}")]
    HashMismatch { expected: String, actual: String },

    #[error("Max retries exceeded")]
    MaxRetriesExceeded,
}

/// Helper function to compute SHA256 hash of a file
pub async fn compute_file_hash(path: &std::path::Path) -> Result<String, io::Error> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 8192];

    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_client_creation() {
        let client = SnapshotTransferClient::new(3);
        assert_eq!(client.max_retries, 3);
    }

    #[tokio::test]
    async fn test_compute_file_hash() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.data");

        // Write test data
        tokio::fs::write(&test_file, b"test data").await.unwrap();

        // Compute hash
        let hash = compute_file_hash(&test_file).await.unwrap();

        // Verify it's a valid SHA256 hex string
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn test_hash_verification() {
        // Test that we can compute consistent hashes
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("test.data");

        tokio::fs::write(&test_file, b"test data").await.unwrap();

        let hash1 = compute_file_hash(&test_file).await.unwrap();
        let hash2 = compute_file_hash(&test_file).await.unwrap();

        assert_eq!(hash1, hash2);
    }
}
