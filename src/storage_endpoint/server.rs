//! StorageEndpoint gRPC server implementation

use super::{Result, StorageEndpointConfig, StorageEndpointError};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{error, info};

/// StorageEndpoint gRPC server
///
/// This server manages the lifecycle of gRPC services for data transfer,
/// including snapshot transfer and (future) chunk data operations.
pub struct StorageEndpointServer {
    config: StorageEndpointConfig,
    server_handle: Option<JoinHandle<Result<()>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl StorageEndpointServer {
    /// Create a new StorageEndpoint server
    pub fn new(config: StorageEndpointConfig) -> Result<Self> {
        config.validate()?;

        Ok(Self {
            config,
            server_handle: None,
            shutdown_tx: None,
        })
    }

    /// Get the server configuration
    pub fn config(&self) -> &StorageEndpointConfig {
        &self.config
    }

    /// Get the public endpoint address for advertising to peers
    pub fn endpoint_address(&self, hostname: Option<&str>) -> String {
        self.config.public_address(hostname)
    }

    /// Check if the server is currently running
    pub fn is_running(&self) -> bool {
        self.server_handle.is_some()
    }

    /// Start the gRPC server
    ///
    /// This will be fully implemented in Task 3.
    /// For now, it's a placeholder that validates configuration.
    pub async fn start(&mut self) -> Result<String> {
        if self.is_running() {
            return Err(StorageEndpointError::Server(
                "Server is already running".to_string(),
            ));
        }

        info!(
            "Starting StorageEndpoint server at {} for node {}",
            self.config.server_address(),
            self.config.node_id
        );

        // TODO: Task 3 will implement:
        // 1. Build tonic server
        // 2. Register SnapshotTransferService
        // 3. Register future ChunkDataService
        // 4. Start server in background task
        // 5. Store server_handle and shutdown_tx

        // Placeholder validation
        self.config.validate()?;

        info!(
            "StorageEndpoint server started successfully at {}",
            self.config.server_address()
        );

        Ok(self.config.public_address(None))
    }

    /// Stop the gRPC server gracefully
    ///
    /// This will be fully implemented in Task 3.
    pub async fn stop(&mut self) -> Result<()> {
        if !self.is_running() {
            return Ok(());
        }

        info!(
            "Stopping StorageEndpoint server for node {}",
            self.config.node_id
        );

        // TODO: Task 3 will implement:
        // 1. Send shutdown signal via shutdown_tx
        // 2. Wait for server_handle to complete
        // 3. Clean up resources

        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        if let Some(handle) = self.server_handle.take() {
            match handle.await {
                Ok(Ok(())) => {
                    info!("StorageEndpoint server stopped successfully");
                }
                Ok(Err(e)) => {
                    error!("StorageEndpoint server stopped with error: {}", e);
                    return Err(e);
                }
                Err(e) => {
                    error!("Failed to join server task: {}", e);
                    return Err(StorageEndpointError::Server(format!(
                        "Failed to join server task: {}",
                        e
                    )));
                }
            }
        }

        Ok(())
    }
}

impl Drop for StorageEndpointServer {
    fn drop(&mut self) {
        if self.is_running() {
            // Attempt graceful shutdown
            if let Some(shutdown_tx) = self.shutdown_tx.take() {
                let _ = shutdown_tx.send(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_server_creation() {
        let config = StorageEndpointConfig::new(1, 8082, PathBuf::from("/tmp/snapshots"));
        let server = StorageEndpointServer::new(config);
        assert!(server.is_ok());
    }

    #[test]
    fn test_server_not_running_initially() {
        let config = StorageEndpointConfig::new(1, 8082, PathBuf::from("/tmp/snapshots"));
        let server = StorageEndpointServer::new(config).unwrap();
        assert!(!server.is_running());
    }

    #[test]
    fn test_endpoint_address() {
        let config = StorageEndpointConfig::new(1, 8082, PathBuf::from("/tmp/snapshots"));
        let server = StorageEndpointServer::new(config).unwrap();
        assert_eq!(
            server.endpoint_address(Some("example.com")),
            "http://example.com:8082"
        );
    }

    #[test]
    fn test_invalid_config() {
        let config = StorageEndpointConfig {
            bind_address: "".to_string(),
            port: 8082,
            snapshot_dir: PathBuf::from("/tmp"),
            node_id: 1,
        };
        let server = StorageEndpointServer::new(config);
        assert!(server.is_err());
    }
}
