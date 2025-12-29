//! ChunkClient trait and ChunkClientPool for managing gRPC connections to remote chunk services.
//!
//! This module provides a trait abstraction for chunk client operations and a concrete
//! implementation using gRPC. The trait enables mocking for tests.

use async_trait::async_trait;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, error, info, warn};

use super::types::{ChunkData, ChunkHeader, ChunkId, DiskId, Error, FileId, NodeId, StripeId};
use crate::storage_endpoint::proto::wormfs::chunk::{
    chunk_service_client::ChunkServiceClient, Chunk, DeleteChunkRequest, ReadChunkRequest,
    StoreChunkRequest,
};
use crate::storage_raft_member::cluster_manager::heartbeat_tracker::HeartbeatTracker;

/// Trait for chunk client operations.
///
/// This trait abstracts the operations for storing, reading, and deleting chunks
/// on remote nodes. It enables mocking for tests via automock.
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock)]
#[async_trait]
pub trait ChunkClient: Send + Sync {
    /// Store a chunk on a remote node.
    ///
    /// # Arguments
    ///
    /// * `target` - Target node ID
    /// * `chunk_data` - Chunk data to store
    /// * `is_staging` - Whether this is a staging write (2PC phase 1)
    /// * `transaction_id` - Transaction ID for 2PC coordination
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails after retries
    async fn store_chunk_remote(
        &self,
        target: NodeId,
        chunk_data: &ChunkData,
        is_staging: bool,
        transaction_id: &str,
    ) -> Result<(), Error>;

    /// Delete a chunk from a remote node.
    ///
    /// # Arguments
    ///
    /// * `target` - Target node ID
    /// * `chunk_id` - Chunk ID to delete
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails
    async fn delete_chunk_remote(&self, target: NodeId, chunk_id: ChunkId) -> Result<(), Error>;

    /// Read a chunk from a remote node.
    ///
    /// # Arguments
    ///
    /// * `source` - Source node ID
    /// * `chunk_id` - Chunk ID to read
    ///
    /// # Returns
    ///
    /// The chunk data
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails
    async fn read_chunk_remote(
        &self,
        source: NodeId,
        chunk_id: ChunkId,
        file_id: FileId,
        stripe_id: StripeId,
        disk_id: DiskId,
    ) -> Result<ChunkData, Error>;
}

/// Configuration for ChunkClientPool
#[derive(Debug, Clone)]
pub struct ChunkClientConfig {
    /// Connection timeout for gRPC clients
    pub connect_timeout: Duration,

    /// Request timeout for chunk operations
    pub request_timeout: Duration,

    /// Maximum number of retry attempts for failed operations
    pub max_retries: u32,

    /// Initial backoff duration for retries
    pub retry_backoff_ms: u64,
}

impl Default for ChunkClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_backoff_ms: 100,
        }
    }
}

/// Pool of ChunkService gRPC clients for remote chunk operations
pub struct ChunkClientPool {
    clients: RwLock<HashMap<NodeId, ChunkServiceClient<Channel>>>,
    heartbeat_tracker: Arc<HeartbeatTracker>,
    config: ChunkClientConfig,
}

impl ChunkClientPool {
    /// Create a new ChunkClientPool
    ///
    /// # Arguments
    ///
    /// * `heartbeat_tracker` - Tracker for discovering node addresses
    /// * `config` - Configuration for client connections
    pub fn new(heartbeat_tracker: Arc<HeartbeatTracker>, config: ChunkClientConfig) -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
            heartbeat_tracker,
            config,
        }
    }

    /// Get or create a ChunkService client for a node
    ///
    /// # Arguments
    ///
    /// * `node_id` - Target node ID
    ///
    /// # Returns
    ///
    /// A ChunkServiceClient connected to the target node
    ///
    /// # Errors
    ///
    /// Returns an error if the node is not found or connection fails
    pub async fn get_client(&self, node_id: NodeId) -> Result<ChunkServiceClient<Channel>, Error> {
        // Check if we already have a client
        {
            let clients = self.clients.read();
            if let Some(client) = clients.get(&node_id) {
                return Ok(client.clone());
            }
        }

        // Get node address from heartbeat tracker
        let node_id_str = node_id.0.to_string();
        let heartbeat = self
            .heartbeat_tracker
            .get_heartbeat(&node_id_str)
            .ok_or(Error::NodeNotFound(node_id))?;

        let storage_endpoint_url = heartbeat.storage_endpoint_url.ok_or_else(|| {
            Error::InvalidNodeAddress(format!("Node {} has no storage_endpoint_url", node_id.0))
        })?;

        debug!(
            "Creating gRPC client for node {} at {}",
            node_id.0, storage_endpoint_url
        );

        // Create new client
        let endpoint = Endpoint::from_shared(format!("http://{}", storage_endpoint_url))
            .map_err(|e| {
                Error::InvalidNodeAddress(format!(
                    "Invalid storage endpoint URL '{}': {}",
                    storage_endpoint_url, e
                ))
            })?
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.request_timeout);

        let channel = endpoint.connect().await.map_err(|e| {
            Error::ConnectionFailed(format!(
                "Failed to connect to node {} at {}: {}",
                node_id.0, storage_endpoint_url, e
            ))
        })?;

        let client = ChunkServiceClient::new(channel);

        // Cache the client
        {
            let mut clients = self.clients.write();
            clients.insert(node_id, client.clone());
        }

        info!(
            "Created gRPC client for node {} at {}",
            node_id.0, storage_endpoint_url
        );

        Ok(client)
    }

    /// Try to store a chunk (single attempt - private helper)
    async fn try_store_chunk(
        &self,
        target: NodeId,
        chunk_data: &ChunkData,
        is_staging: bool,
        transaction_id: &str,
    ) -> Result<(), Error> {
        let mut client = self.get_client(target).await?;

        let proto_chunk = Chunk {
            chunk_id: chunk_data.header.chunk_id.0.as_bytes().to_vec(),
            stripe_id: chunk_data.header.stripe_id.0.as_bytes().to_vec(),
            file_id: chunk_data.header.file_id.0.as_bytes().to_vec(),
            // Safe: u8 -> u32 conversion is always safe (0-255 fits in u32)
            chunk_index: chunk_data.header.chunk_index.into(),
            data: chunk_data.data.clone(),
            chunk_checksum: chunk_data.header.chunk_checksum,
            stripe_checksum: chunk_data.header.stripe_checksum,
            stripe_start_offset: chunk_data.header.stripe_start_offset,
            stripe_end_offset: chunk_data.header.stripe_end_offset,
            // Safe: u8 -> u32 conversions are always safe (0-255 fits in u32)
            data_shards: chunk_data.header.data_shards.into(),
            parity_shards: chunk_data.header.parity_shards.into(),
            erasure_algorithm: 0,     // ReedSolomon
            compression_algorithm: 0, // None
            // Safe: u8 -> usize conversions are always safe
            r#type: if usize::from(chunk_data.header.chunk_index)
                < usize::from(chunk_data.header.data_shards)
            {
                0 // DATA
            } else {
                1 // PARITY
            },
        };

        let request = StoreChunkRequest {
            chunk: Some(proto_chunk),
            is_staging,
            transaction_id: transaction_id.to_string(),
        };

        let response = client.store_chunk(request).await.map_err(|e| {
            Error::RemoteOperationFailed(format!(
                "StoreChunk RPC failed for node {}: {}",
                target.0, e
            ))
        })?;

        let inner = response.into_inner();
        if !inner.success {
            let error_msg = inner
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "Unknown error".to_string());
            return Err(Error::RemoteOperationFailed(format!(
                "StoreChunk failed on node {}: {}",
                target.0, error_msg
            )));
        }

        debug!(
            "Stored chunk {:?} on node {} (staging={})",
            chunk_data.header.chunk_id, target.0, is_staging
        );

        Ok(())
    }
}

// Trait implementation for ChunkClient
#[async_trait]
impl ChunkClient for ChunkClientPool {
    /// Store a chunk on a remote node with retry logic
    async fn store_chunk_remote(
        &self,
        target: NodeId,
        chunk_data: &ChunkData,
        is_staging: bool,
        transaction_id: &str,
    ) -> Result<(), Error> {
        let mut retries = 0;
        let mut backoff_ms = self.config.retry_backoff_ms;

        loop {
            match self
                .try_store_chunk(target, chunk_data, is_staging, transaction_id)
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if retries >= self.config.max_retries {
                        error!(
                            "Failed to store chunk on node {} after {} retries: {}",
                            target.0, retries, e
                        );
                        return Err(e);
                    }

                    warn!(
                        "Retry {}/{} for storing chunk on node {}: {}",
                        retries + 1,
                        self.config.max_retries,
                        target.0,
                        e
                    );

                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms *= 2; // Exponential backoff
                    retries += 1;
                }
            }
        }
    }

    /// Delete a chunk from a remote node
    ///
    /// # Arguments
    ///
    /// * `target` - Target node ID
    /// * `chunk_id` - Chunk ID to delete
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails
    async fn delete_chunk_remote(&self, target: NodeId, chunk_id: ChunkId) -> Result<(), Error> {
        let mut client = self.get_client(target).await?;

        let request = DeleteChunkRequest {
            chunk_id: chunk_id.0.as_bytes().to_vec(),
        };

        let response = client.delete_chunk(request).await.map_err(|e| {
            Error::RemoteOperationFailed(format!(
                "DeleteChunk RPC failed for node {}: {}",
                target.0, e
            ))
        })?;

        let inner = response.into_inner();
        if !inner.success {
            return Err(Error::RemoteOperationFailed(format!(
                "DeleteChunk failed on node {}",
                target.0
            )));
        }

        debug!("Deleted chunk {:?} from node {}", chunk_id, target.0);

        Ok(())
    }

    /// Read a chunk from a remote node
    ///
    /// # Arguments
    ///
    /// * `source` - Source node ID
    /// * `chunk_id` - Chunk ID to read
    ///
    /// # Returns
    ///
    /// The chunk data
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails
    async fn read_chunk_remote(
        &self,
        source: NodeId,
        chunk_id: ChunkId,
        file_id: FileId,
        stripe_id: StripeId,
        disk_id: DiskId,
    ) -> Result<ChunkData, Error> {
        let mut client = self.get_client(source).await?;

        let request = ReadChunkRequest {
            chunk_id: chunk_id.0.as_bytes().to_vec(),
            file_id: file_id.0.as_bytes().to_vec(),
            stripe_id: stripe_id.0.as_bytes().to_vec(),
            disk_id: disk_id.0,
        };

        let response = client.read_chunk(request).await.map_err(|e| {
            Error::RemoteOperationFailed(format!(
                "ReadChunk RPC failed for node {}: {}",
                source.0, e
            ))
        })?;

        let inner = response.into_inner();

        // Deserialize header from response
        let header: ChunkHeader = bincode::deserialize(&inner.header_data).map_err(|e| {
            Error::ChunkCorrupt(chunk_id, format!("Failed to deserialize header: {}", e))
        })?;

        Ok(ChunkData {
            header,
            data: inner.chunk_data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ChunkClientConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_backoff_ms, 100);
    }
}
