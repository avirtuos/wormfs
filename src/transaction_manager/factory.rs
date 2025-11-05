//! Factory for creating TransactionManager instances.

use super::implementation::TransactionManagerImpl;
use super::types::Config;
use super::TransactionManager;
use crate::metadata_store::MetadataStoreImpl;
use crate::metric_service::MetricServiceImpl;
use crate::storage_raft_member::{types::WormFsOperation, StorageRaftMember};
use std::sync::Arc;

/// Factory for creating TransactionManager instances.
///
/// This factory follows the same pattern as other components in WormFS,
/// providing a clean way to instantiate the transaction manager with
/// all required dependencies.
pub struct TransactionManagerFactory;

impl TransactionManagerFactory {
    /// Create a new TransactionManager instance.
    ///
    /// # Arguments
    ///
    /// * `raft_member` - StorageRaftMember for consensus operations
    /// * `metadata_store` - MetadataStore for validation queries
    /// * `config` - Configuration for the transaction manager
    /// * `metrics` - MetricService for observability
    ///
    /// # Returns
    ///
    /// An Arc-wrapped TransactionManager trait object
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use wormfs_v2::transaction_manager::{TransactionManagerFactory, Config};
    /// use std::sync::Arc;
    ///
    /// # async fn example(
    /// #     raft_member: Arc<dyn wormfs_v2::StorageRaftMember>,
    /// #     metadata_store: Arc<dyn wormfs_v2::MetadataStore>,
    /// #     metrics: Arc<wormfs_v2::MetricService>,
    /// # ) {
    /// let config = Config::default();
    ///
    /// let tx_manager = TransactionManagerFactory::create(
    ///     raft_member,
    ///     metadata_store,
    ///     config,
    ///     metrics,
    /// );
    /// # }
    /// ```
    pub fn create(
        raft_member: Arc<dyn StorageRaftMember<Operation = WormFsOperation, OperationResult = ()>>,
        metadata_store: MetadataStoreImpl,
        config: Config,
        metrics: MetricServiceImpl,
    ) -> Arc<dyn TransactionManager> {
        TransactionManagerImpl::new(raft_member, metadata_store, config, metrics)
    }

    /// Create a new TransactionManager with default configuration.
    ///
    /// This is a convenience method for common use cases where the default
    /// configuration is appropriate.
    ///
    /// # Arguments
    ///
    /// * `raft_member` - StorageRaftMember for consensus operations
    /// * `metadata_store` - MetadataStore implementation
    /// * `metrics` - MetricService implementation
    ///
    /// # Returns
    ///
    /// An Arc-wrapped TransactionManager trait object
    pub fn create_with_defaults(
        raft_member: Arc<dyn StorageRaftMember<Operation = WormFsOperation, OperationResult = ()>>,
        metadata_store: MetadataStoreImpl,
        metrics: MetricServiceImpl,
    ) -> Arc<dyn TransactionManager> {
        Self::create(raft_member, metadata_store, Config::default(), metrics)
    }
}
