//! Factory for creating FileSystemService instances.
//!
//! The factory pattern separates construction concerns from the service trait,
//! allowing the service trait to remain mockable while still providing type-safe
//! construction with concrete dependency types.

use super::implementation::FileSystemServiceImpl;
use super::raft_commands::StorageRaftMemberStub;
use super::{Config, Error};
use crate::file_store::FileStoreImpl;
use crate::metadata_store::MetadataStoreImpl;
use crate::metric_service::{MetricService, MetricServiceImpl};
use std::sync::Arc;

/// Concrete factory for creating FileSystemServiceImpl instances.
///
/// This factory handles the construction of FileSystemServiceImpl with all
/// its dependencies properly initialized.
///
/// # Phase 1 Implementation
///
/// In Phase 1, this factory uses:
/// - `StorageRaftMemberStub` - Stub that immediately returns success (no actual Raft consensus)
/// - `MetadataStoreImpl` - SQLite-based metadata storage
/// - `FileStoreImpl` - Erasure-coded chunk storage
///
/// # Phase 2 Migration
///
/// When migrating to Phase 2 with distributed Raft consensus:
/// 1. Replace `StorageRaftMemberStub` with `StorageRaftMemberImpl`
/// 2. Update the factory to accept a real Raft member instance
/// 3. All calling code remains unchanged (factory pattern benefit!)
pub struct FileSystemServiceImplFactory;

impl FileSystemServiceImplFactory {
    /// Create a new FileSystemServiceImpl instance.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for the filesystem service
    /// * `metadata_store` - MetadataStore instance for metadata operations
    /// * `file_store` - FileStore instance for chunk I/O operations
    /// * `metrics` - Optional MetricService for telemetry collection
    ///
    /// # Returns
    ///
    /// A fully initialized FileSystemServiceImpl instance.
    ///
    /// # Errors
    ///
    /// Returns an error if initialization fails (currently always succeeds).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = Config::default();
    /// let metadata_store = MetadataStoreFactory::create(metadata_config).await?;
    /// let file_store = Arc::new(FileStore::new(file_store_config)?);
    /// let metrics = Arc::new(MetricServiceImpl::new(metric_config)?);
    ///
    /// let service = FileSystemServiceImplFactory::create(
    ///     config,
    ///     metadata_store,
    ///     file_store,
    ///     Some(metrics),
    /// )?;
    /// ```
    pub fn create(
        config: Config,
        metadata_store: MetadataStoreImpl,
        file_store: Arc<FileStoreImpl>,
        metrics: Option<Arc<MetricServiceImpl>>,
    ) -> Result<FileSystemServiceImpl, Error> {
        // Create the service instance
        // Note: new() is pub(crate) so only callable from within filesystem_service module
        // The Raft stub is created internally with metadata_store
        let mut service = FileSystemServiceImpl::new(config, metadata_store, file_store.clone());

        // Inject metrics if provided
        if let Some(metrics_arc) = metrics {
            // Set metrics on FileSystemService
            service.set_metrics(metrics_arc.clone());

            // Set metrics on FileStore
            file_store.set_metrics(metrics_arc);
        }

        Ok(service)
    }
}
