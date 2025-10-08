//! StorageEndpoint gRPC server implementation

use super::{Result, SnapshotTransferServiceImpl, StorageEndpointConfig, StorageEndpointError};
use std::net::SocketAddr;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic_health::server::{health_reporter, HealthReporter};
use tracing::{error, info};

use crate::raft::proto_types::proto::snapshot_transfer_service_server::SnapshotTransferServiceServer;

/// StorageEndpoint gRPC server
///
/// This server manages the lifecycle of gRPC services for data transfer,
/// including snapshot transfer and (future) chunk data operations.
pub struct StorageEndpointServer {
    config: StorageEndpointConfig,
    server_handle: Option<JoinHandle<Result<()>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    health_reporter: Option<HealthReporter>,
}

impl StorageEndpointServer {
    /// Create a new StorageEndpoint server
    pub fn new(config: StorageEndpointConfig) -> Result<Self> {
        config.validate()?;

        Ok(Self {
            config,
            server_handle: None,
            shutdown_tx: None,
            health_reporter: None,
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

        // Parse bind address
        let addr: SocketAddr =
            self.config.server_address().parse().map_err(|e| {
                StorageEndpointError::Config(format!("Invalid server address: {}", e))
            })?;

        // Create snapshot transfer service
        let snapshot_service = SnapshotTransferServiceImpl::new(self.config.clone());

        // Create health reporter
        let (mut health_reporter, health_service) = health_reporter();

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Build and configure the gRPC server
        let server_future = Server::builder()
            .add_service(health_service)
            .add_service(SnapshotTransferServiceServer::new(snapshot_service))
            .serve_with_shutdown(addr, async {
                shutdown_rx.await.ok();
                info!("StorageEndpoint server received shutdown signal");
            });

        // Spawn server in background task
        let server_handle = tokio::spawn(async move {
            server_future
                .await
                .map_err(|e| StorageEndpointError::GrpcError(e.to_string()))
        });

        // Set services as serving
        health_reporter
            .set_serving::<SnapshotTransferServiceServer<SnapshotTransferServiceImpl>>()
            .await;

        // Store handles for lifecycle management
        self.server_handle = Some(server_handle);
        self.shutdown_tx = Some(shutdown_tx);
        self.health_reporter = Some(health_reporter);

        info!(
            "StorageEndpoint server started successfully at {}",
            self.config.server_address()
        );

        Ok(self.config.public_address(None))
    }

    /// Stop the gRPC server gracefully
    pub async fn stop(&mut self) -> Result<()> {
        if !self.is_running() {
            return Ok(());
        }

        info!(
            "Stopping StorageEndpoint server for node {}",
            self.config.node_id
        );

        // Mark services as not serving
        if let Some(mut reporter) = self.health_reporter.take() {
            reporter
                .set_not_serving::<SnapshotTransferServiceServer<SnapshotTransferServiceImpl>>()
                .await;
        }

        // Send shutdown signal
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }

        // Wait for server to complete
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
    use tempfile::TempDir;

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

    #[tokio::test]
    async fn test_server_lifecycle() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageEndpointConfig::new(1, 18082, temp_dir.path().to_path_buf());
        let mut server = StorageEndpointServer::new(config).unwrap();

        // Server should not be running initially
        assert!(!server.is_running());

        // Start the server
        let addr = server.start().await.unwrap();
        assert!(addr.contains("18082"));
        assert!(server.is_running());

        // Give server a moment to bind
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Stop the server
        server.stop().await.unwrap();
        assert!(!server.is_running());
    }

    #[tokio::test]
    async fn test_cannot_start_twice() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageEndpointConfig::new(1, 18083, temp_dir.path().to_path_buf());
        let mut server = StorageEndpointServer::new(config).unwrap();

        // Start the server
        server.start().await.unwrap();

        // Attempting to start again should fail
        let result = server.start().await;
        assert!(result.is_err());

        // Cleanup
        server.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_stop_when_not_running() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageEndpointConfig::new(1, 18084, temp_dir.path().to_path_buf());
        let mut server = StorageEndpointServer::new(config).unwrap();

        // Stopping a non-running server should succeed silently
        let result = server.stop().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_health_check_positive() {
        use tonic::transport::Channel;
        use tonic_health::pb::health_client::HealthClient;
        use tonic_health::pb::HealthCheckRequest;

        let temp_dir = TempDir::new().unwrap();
        let config = StorageEndpointConfig::new(1, 18085, temp_dir.path().to_path_buf());
        let mut server = StorageEndpointServer::new(config).unwrap();

        // Start the server
        server.start().await.unwrap();

        // Give server time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Connect to the health check endpoint
        let channel = Channel::from_static("http://127.0.0.1:18085")
            .connect()
            .await
            .unwrap();
        let mut health_client = HealthClient::new(channel);

        // Check health - should be SERVING
        let request = tonic::Request::new(HealthCheckRequest {
            service: "wormfs.SnapshotTransferService".to_string(),
        });
        let response = health_client.check(request).await.unwrap();
        let status = response.into_inner().status;

        // Status 1 = SERVING (from tonic_health::pb::health_check_response::ServingStatus)
        assert_eq!(status, 1, "Service should be SERVING");

        // Cleanup
        server.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_health_check_negative() {
        use tonic::transport::Channel;
        use tonic_health::pb::health_client::HealthClient;
        use tonic_health::pb::HealthCheckRequest;

        let temp_dir = TempDir::new().unwrap();
        let config = StorageEndpointConfig::new(1, 18086, temp_dir.path().to_path_buf());
        let mut server = StorageEndpointServer::new(config).unwrap();

        // Start the server
        server.start().await.unwrap();

        // Give server time to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Connect to the health check endpoint
        let channel = Channel::from_static("http://127.0.0.1:18086")
            .connect()
            .await
            .unwrap();
        let mut health_client = HealthClient::new(channel);

        // Stop the server (this should mark services as NOT_SERVING)
        server.stop().await.unwrap();

        // Give server time to stop
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Check health - connection should fail since server is stopped
        let request = tonic::Request::new(HealthCheckRequest {
            service: "wormfs.SnapshotTransferService".to_string(),
        });
        let response = health_client.check(request).await;

        // Should fail with transport error (connection refused/reset)
        assert!(
            response.is_err(),
            "Health check should fail when server is stopped"
        );
    }
}
