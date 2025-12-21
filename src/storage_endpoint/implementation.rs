//! StorageEndpointImpl - Core gRPC server implementation.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tonic::transport::Server;
use tower::limit::ConcurrencyLimitLayer;
use tracing::{debug, error, info, warn};

use crate::file_store::{ChunkData, ChunkId, FileStore};
use crate::filesystem_service::FileSystemService;
use crate::metric_service::MetricService;
use crate::snapshot_store::SnapshotStore;
use crate::storage_node::StorageNode;
use crate::storage_raft_member::StorageRaftMember;
use crate::transaction_log_store::TransactionLogStore;

use super::middleware::{
    AuthInterceptor, AuthLayer, MetricsLayer, MetricsMiddleware, RateLimitLayer, RateLimiter,
};
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

        // Build service implementations with message size limits applied
        let max_message_size = self.config.max_message_size;

        let health_svc = HealthServer::new(HealthServiceImpl::new());

        let chunk_svc = ChunkServiceServer::new(ChunkServiceImpl::new(self.file_store.clone()))
            .max_decoding_message_size(max_message_size)
            .max_encoding_message_size(max_message_size);

        let snapshot_svc =
            SnapshotServiceServer::new(SnapshotServiceImpl::new(self.snapshot_store.clone()))
                .max_decoding_message_size(max_message_size)
                .max_encoding_message_size(max_message_size);

        let txlog_svc = TransactionLogServiceServer::new(TransactionLogServiceImpl::new(
            self.transaction_log_store.clone(),
        ))
        .max_decoding_message_size(max_message_size)
        .max_encoding_message_size(max_message_size);

        let filesystem_svc =
            FilesystemServiceServer::new(FilesystemServiceImpl::new(self.file_system.clone()))
                .max_decoding_message_size(max_message_size)
                .max_encoding_message_size(max_message_size);

        info!(
            "Building gRPC server with all services and middleware (max_message_size={} bytes)",
            max_message_size
        );

        // Create middleware layers
        let auth_layer = AuthLayer::new(self.auth_interceptor.clone());
        let rate_limit_layer = RateLimitLayer::new((*self.rate_limiter).clone());
        let metrics_layer = MetricsLayer::new(MetricsMiddleware::new(
            self.metrics.clone(),
            self.config.enable_metrics,
        ));

        // Concurrency limit layer
        let concurrency_layer = ConcurrencyLimitLayer::new(self.config.max_concurrent_requests);

        // Build base server with timeout
        let server_builder = Server::builder().timeout(self.config.request_timeout);

        // TODO: TLS support requires enabling the "tls" feature on tonic dependency
        // See: https://github.com/hyperium/tonic/tree/master/examples/src/tls
        if self.config.enable_tls {
            warn!(
                "TLS is enabled in config but not yet implemented - requires tonic 'tls' feature"
            );
        }

        // Build server with middleware layers
        // Layer application order (outermost to innermost when processing request):
        //   1. concurrency_layer - limits concurrent requests
        //   2. auth_layer - authenticates request
        //   3. rate_limit_layer - rate limits per identity
        //   4. metrics_layer - records metrics
        //   5. service - actual service handler
        //
        // Note: Layers are added in reverse order (last added = outermost)
        // Note: Timeout is applied at server level, not as a layer
        // Note: Request logging is done via debug! logs in each service method

        if self.config.enable_logging {
            info!("Request logging enabled - service methods will log at debug level");
        }

        let server = server_builder
            .layer(metrics_layer)
            .layer(rate_limit_layer)
            .layer(auth_layer)
            .layer(concurrency_layer)
            .add_service(health_svc)
            .add_service(chunk_svc)
            .add_service(snapshot_svc)
            .add_service(txlog_svc)
            .add_service(filesystem_svc);

        // Update state
        *self.inner.is_serving.write().await = true;
        *self.inner.local_addr.write().await = Some(addr);

        info!(
            "StorageEndpoint gRPC server starting on {} (tls={}, logging={}, max_concurrent={}, timeout={:?})",
            addr,
            self.config.enable_tls,
            self.config.enable_logging,
            self.config.max_concurrent_requests,
            self.config.request_timeout
        );

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

    fn generate_upload_token(
        &self,
        _chunk_id: ChunkId,
        _valid_for: Duration,
    ) -> Result<String, EndpointError> {
        error!("generate_upload_token called but secure token generation is not yet implemented");
        Err(EndpointError::InternalError(
            "Secure upload token generation not yet implemented".to_string(),
        ))
    }

    fn validate_upload_token(&self, _chunk_id: ChunkId, _token: &str) -> Result<(), EndpointError> {
        error!("validate_upload_token called but secure token validation is not yet implemented");
        Err(EndpointError::InternalError(
            "Secure upload token validation not yet implemented".to_string(),
        ))
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
