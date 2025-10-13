//! Factory for creating SnapshotStore instances.

use super::implementation::SnapshotStoreImpl;
use super::types::{Config, Error};
use super::SnapshotStore;

/// Factory for creating SnapshotStore instances.
pub struct SnapshotStoreFactory;

impl SnapshotStoreFactory {
    /// Create a new SnapshotStore instance.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for the snapshot store
    ///
    /// # Returns
    ///
    /// A new SnapshotStore implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Configuration is invalid
    /// - Storage directory cannot be accessed
    pub fn create(config: Config) -> Result<impl SnapshotStore, Error> {
        SnapshotStoreImpl::new(config)
    }
}
