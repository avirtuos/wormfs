//! StorageEndpointImpl - Core gRPC server implementation.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tonic::transport::Server;
use tracing::{debug, error, info, warn};

use crate::file_store::{ChunkData, ChunkId, FileStore};
use crate::filesystem_service::FileSystemService;
use crate::metric_service::MetricService;
use crate::snapshot_store::SnapshotStore;
use crate::storage_node::StorageNode;
use crate::storage_raft_member::StorageRaftMember;
use crate::transaction_log_store::TransactionLogStore;

use super::middleware::{AuthInterceptor, MetricsMiddleware, RateLimiter};
use super::proto::wormfs::admin::admin_service_server::AdminServiceServer;
use super::proto::wormfs::chunk::chunk_service_server::ChunkServiceServer;
use super::proto::wormfs::filesystem::filesystem_service_server::FilesystemServiceServer;
use super::proto::wormfs::health::health_server::HealthServer;
use super::proto::wormfs::snapshot::snapshot_service_server::SnapshotServiceServer;
use super::proto::wormfs::transaction_log::transaction_log_service_server::TransactionLogServiceServer;
use super::services::*;
use super::types::{EndpointConfig, EndpointError};
use super::StorageEndpoint;

/// Internal state for StorageEndpoint with interior mutability.
struct StorageEndpointInner {
    /// Server state: is it currently serving requests?
    is_serving: RwLock<bool>,
    /// Bound address once server starts
    local_addr: RwLock<Option<SocketAddr>>,
    /// Shutdown signal sender
    shutdown_tx: broadcast::Sender<()>,
}

/// StorageEndpoint implementation providing gRPC APIs.
pub struct StorageEndpointImpl<FS, F, SS, TL, RM, SN, M>
where
    FS: FileSystemService,
    F: FileStore,
    SS: SnapshotStore,
    TL: TransactionLogStore,
    RM: StorageRaftMember,
    SN: StorageNode,
    M: MetricService,
{
    config: EndpointConfig,
    inner: Arc<StorageEndpointInner>,

    // Service dependencies
    file_system: Arc<FS>,
    file_store: Arc<F>,
    snapshot_store: Arc<SS>,
    transaction_log_store: Arc<TL>,
    raft_member: Arc<RM>,
    storage_node: Arc<SN>,
    metrics: M,

    // Middleware components
    auth_interceptor: AuthInterceptor,
    rate_limiter: Arc<RateLimiter>,
}

impl<FS, F, SS, TL, RM, SN, M> StorageEndpointImpl<FS, F, SS, TL, RM, SN, M>
where
    FS: FileSystemService + 'static,
    F: FileStore + 'static,
    SS: SnapshotStore + 'static,
    TL: TransactionLogStore + 'static,
    RM: StorageRaftMember + 'static,
    SN: StorageNode + 'static,
    M: MetricService + 'static,
{
    /// Create a new StorageEndpointImpl with all dependencies.
    ///
    /// This should typically be called via StorageEndpointFactory::create().
    pub fn new_with_dependencies(
        config: EndpointConfig,
        file_system: Arc<FS>,
        file_store: Arc<F>,
        snapshot_store: Arc<SS>,
        transaction_log_store: Arc<TL>,
        raft_member: Arc<RM>,
        storage_node: Arc<SN>,
        metrics: M,
        auth_interceptor: AuthInterceptor,
        rate_limiter: Arc<RateLimiter>,
    ) -> Result<Self, EndpointError> {
        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            config,
            inner: Arc::new(StorageEndpointInner {
                is_serving: RwLock::new(false),
                local_addr: RwLock::new(None),
                shutdown_tx,
            }),
            file_system,
            file_store,
            snapshot_store,
            transaction_log_store,
            raft_member,
            storage_node,
            metrics,
            auth_interceptor,
            rate_limiter,
        })
    }
}

#[async_trait::async_trait]
impl<FS, F, SS, TL, RM, SN, M> StorageEndpoint for StorageEndpointImpl<FS, F, SS, TL, RM, SN, M>
where
    FS: FileSystemService + 'static,
    F: FileStore + 'static,
    SS: SnapshotStore + 'static,
    TL: TransactionLogStore + 'static,
    RM: StorageRaftMember + 'static,
    SN: StorageNode + 'static,
    M: MetricService + 'static,
{
    fn new(_config: EndpointConfig) -> Result<Self, EndpointError>
    where
        Self: Sized,
    {
        Err(EndpointError::ConfigError(
            "Use StorageEndpointFactory::create() for proper initialization".to_string(),
        ))
    }

    async fn serve(&self) -> Result<(), EndpointError> {
        let addr = self.config.listen_address;

        // Create shutdown receiver
        let mut shutdown_rx = self.inner.shutdown_tx.subscribe();

        // Build service implementations
        let health_svc = HealthServiceImpl::new();
        let chunk_svc = ChunkServiceImpl::new(self.file_store.clone());
        let snapshot_svc = SnapshotServiceImpl::new(self.snapshot_store.clone());
        let txlog_svc = TransactionLogServiceImpl::new(self.transaction_log_store.clone());
        let admin_svc = AdminServiceImpl::new(self.raft_member.clone(), self.storage_node.clone());
        let filesystem_svc = FilesystemServiceImpl::new(self.file_system.clone());

        info!("Building gRPC server with all services");

        // Build server with all services
        // Note: Middleware like auth, rate limiting will be added in future iterations
        let server = Server::builder()
            .add_service(HealthServer::new(health_svc))
            .add_service(ChunkServiceServer::new(chunk_svc))
            .add_service(SnapshotServiceServer::new(snapshot_svc))
            .add_service(TransactionLogServiceServer::new(txlog_svc))
            .add_service(AdminServiceServer::new(admin_svc))
            .add_service(FilesystemServiceServer::new(filesystem_svc));

        // Update state
        *self.inner.is_serving.write().await = true;
        *self.inner.local_addr.write().await = Some(addr);

        info!("StorageEndpoint gRPC server starting on {}", addr);

        // Serve with graceful shutdown
        let serve_future = server.serve_with_shutdown(addr, async move {
            let _ = shutdown_rx.recv().await;
            info!("StorageEndpoint received shutdown signal");
        });

        match serve_future.await {
            Ok(()) => {
                info!("StorageEndpoint gRPC server stopped gracefully");
                *self.inner.is_serving.write().await = false;
                Ok(())
            }
            Err(e) => {
                error!("StorageEndpoint gRPC server error: {}", e);
                *self.inner.is_serving.write().await = false;
                Err(EndpointError::GrpcError(e.to_string()))
            }
        }
    }

    async fn shutdown(&self, timeout: Duration) -> Result<(), EndpointError> {
        info!("StorageEndpoint shutdown requested (timeout={:?})", timeout);

        if !self.is_serving() {
            return Err(EndpointError::NotServing);
        }

        // Send shutdown signal
        let _ = self.inner.shutdown_tx.send(());

        // Wait for server to stop with timeout
        let shutdown_complete = async {
            loop {
                if !*self.inner.is_serving.read().await {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        match tokio::time::timeout(timeout, shutdown_complete).await {
            Ok(()) => {
                info!("StorageEndpoint shutdown completed");
                Ok(())
            }
            Err(_) => {
                warn!("StorageEndpoint shutdown timed out after {:?}", timeout);
                Err(EndpointError::ShutdownTimeout(timeout))
            }
        }
    }

    fn local_addr(&self) -> SocketAddr {
        self.inner
            .local_addr
            .try_read()
            .map(|addr| *addr)
            .ok()
            .flatten()
            .unwrap_or(self.config.listen_address)
    }

    fn is_serving(&self) -> bool {
        self.inner
            .is_serving
            .try_read()
            .map(|serving| *serving)
            .unwrap_or(false)
    }

    async fn upload_chunk(
        &self,
        chunk_data: ChunkData,
        token: &str,
    ) -> Result<ChunkId, EndpointError> {
        debug!("Direct chunk upload with token");

        // TODO: Implement token validation and chunk upload
        warn!("upload_chunk not yet fully implemented");

        Err(EndpointError::InternalError(
            "Direct chunk upload not yet implemented".to_string(),
        ))
    }

    fn generate_upload_token(&self, chunk_id: ChunkId, valid_for: Duration) -> String {
        debug!(
            "Generating upload token for chunk_id (valid_for={:?})",
            valid_for
        );

        // TODO: Implement proper token generation with expiration
        // For now, return a simple hardcoded token
        warn!("generate_upload_token using hardcoded token - NOT SECURE");
        format!("upload-token-{:?}", chunk_id)
    }

    fn validate_upload_token(&self, chunk_id: ChunkId, token: &str) -> bool {
        debug!("Validating upload token for chunk_id");

        // TODO: Implement proper token validation
        warn!("validate_upload_token using hardcoded validation - NOT SECURE");
        token.starts_with("upload-token-")
    }

    async fn trigger_deep_check(&self) -> Result<(), EndpointError> {
        info!("Manual deep check triggered");

        // TODO: Implement deep check trigger via storage watchdog
        warn!("trigger_deep_check not yet fully implemented");

        Err(EndpointError::InternalError(
            "Deep check trigger not yet implemented".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_store::MockFileStore;
    use crate::filesystem_service::MockFileSystemService;
    use crate::metric_service::MockMetricService;
    use crate::snapshot_store::MockSnapshotStore;
    use crate::storage_node::MockStorageNode;
    use crate::storage_raft_member::MockStorageRaftMember;
    use crate::transaction_log_store::MockTransactionLogStore;

    async fn create_test_endpoint() -> Result<
        StorageEndpointImpl<
            MockFileSystemService,
            MockFileStore,
            MockSnapshotStore,
            MockTransactionLogStore,
            MockStorageRaftMember,
            MockStorageNode,
            MockMetricService,
        >,
        EndpointError,
    > {
        let config = EndpointConfig {
            listen_address: "127.0.0.1:0".parse().unwrap(),
            enable_auth: false,
            ..Default::default()
        };

        let auth = AuthInterceptor::new(false, None, None).await?;
        let rate_limiter = Arc::new(RateLimiter::new(Some(100), Some(1000), 100));

        StorageEndpointImpl::new_with_dependencies(
            config,
            Arc::new(MockFileSystemService::default()),
            Arc::new(MockFileStore::default()),
            Arc::new(MockSnapshotStore::default()),
            Arc::new(MockTransactionLogStore::default()),
            Arc::new(MockStorageRaftMember::default()),
            Arc::new(MockStorageNode::default()),
            MockMetricService::default(),
            auth,
            rate_limiter,
        )
    }

    #[tokio::test]
    async fn test_endpoint_creation() {
        let endpoint = create_test_endpoint().await;
        assert!(endpoint.is_ok());
    }

    #[tokio::test]
    async fn test_endpoint_not_serving_initially() {
        let endpoint = create_test_endpoint().await.unwrap();
        assert!(!endpoint.is_serving());
    }

    #[tokio::test]
    async fn test_shutdown_when_not_serving() {
        let endpoint = create_test_endpoint().await.unwrap();
        let result = endpoint.shutdown(Duration::from_secs(1)).await;
        assert!(result.is_err());
    }
}
