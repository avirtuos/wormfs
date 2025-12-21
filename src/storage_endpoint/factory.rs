//! Factory for creating StorageEndpoint instances with dependency injection.

use std::sync::Arc;

use crate::file_store::FileStore;
use crate::filesystem_service::FileSystemService;
use crate::metric_service::MetricService;
use crate::snapshot_store::SnapshotStore;
use crate::storage_node::StorageNode;
use crate::storage_raft_member::StorageRaftMember;
use crate::transaction_log_store::TransactionLogStore;

use super::implementation::StorageEndpointImpl;
use super::middleware::{AuthInterceptor, RateLimiter};
use super::types::{EndpointConfig, EndpointError};

/// Factory for creating StorageEndpoint instances with proper dependency injection.
pub struct StorageEndpointFactory;

impl StorageEndpointFactory {
    /// Create a new StorageEndpoint with all required dependencies.
    ///
    /// # Arguments
    ///
    /// * `config` - StorageEndpoint configuration
    /// * `file_system` - FileSystemService for filesystem operations
    /// * `file_store` - FileStore for chunk storage
    /// * `snapshot_store` - SnapshotStore for Raft snapshots
    /// * `transaction_log_store` - TransactionLogStore for Raft logs
    /// * `raft_member` - StorageRaftMember for Raft operations
    /// * `storage_node` - StorageNode for node operations
    /// * `metrics` - MetricService for metrics collection
    ///
    /// # Returns
    ///
    /// A configured StorageEndpointImpl instance ready to serve.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Authentication configuration is invalid
    /// - Identities directory cannot be read
    /// - PSK files are malformed
    pub async fn create<FS, F, SS, TL, RM, SN, M>(
        config: EndpointConfig,
        file_system: Arc<FS>,
        file_store: Arc<F>,
        snapshot_store: Arc<SS>,
        transaction_log_store: Arc<TL>,
        raft_member: Arc<RM>,
        storage_node: Arc<SN>,
        metrics: M,
    ) -> Result<StorageEndpointImpl<FS, F, SS, TL, RM, SN, M>, EndpointError>
    where
        FS: FileSystemService + 'static,
        F: FileStore + 'static,
        SS: SnapshotStore + 'static,
        TL: TransactionLogStore + 'static,
        RM: StorageRaftMember + 'static,
        SN: StorageNode + 'static,
        M: MetricService + 'static,
    {
        // Create authentication interceptor
        let auth_interceptor = AuthInterceptor::new(
            config.enable_auth,
            config.identities_dir.clone(),
            config.node_identity.clone(),
        )
        .await?;

        // Create rate limiter
        let rate_limiter = Arc::new(RateLimiter::new(
            config.rate_limit_per_client,
            config.rate_limit_overall,
            config.rate_limit_burst_size,
        ));

        StorageEndpointImpl::new_with_dependencies(
            config,
            file_system,
            file_store,
            snapshot_store,
            transaction_log_store,
            raft_member,
            storage_node,
            metrics,
            auth_interceptor,
            rate_limiter,
        )
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

    #[tokio::test]
    async fn test_factory_create() {
        let config = EndpointConfig {
            listen_address: "127.0.0.1:0".parse().unwrap(),
            enable_auth: false,
            ..Default::default()
        };

        let result = StorageEndpointFactory::create(
            config,
            Arc::new(MockFileSystemService::default()),
            Arc::new(MockFileStore::default()),
            Arc::new(MockSnapshotStore::default()),
            Arc::new(MockTransactionLogStore::default()),
            Arc::new(MockStorageRaftMember::default()),
            Arc::new(MockStorageNode::default()),
            MockMetricService::default(),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_factory_auth_validation() {
        let config = EndpointConfig {
            listen_address: "127.0.0.1:0".parse().unwrap(),
            enable_auth: true,
            identities_dir: None, // Invalid: auth enabled but no identities dir
            ..Default::default()
        };

        let result = StorageEndpointFactory::create(
            config,
            Arc::new(MockFileSystemService::default()),
            Arc::new(MockFileStore::default()),
            Arc::new(MockSnapshotStore::default()),
            Arc::new(MockTransactionLogStore::default()),
            Arc::new(MockStorageRaftMember::default()),
            Arc::new(MockStorageNode::default()),
            MockMetricService::default(),
        )
        .await;

        assert!(result.is_err());
    }
}
