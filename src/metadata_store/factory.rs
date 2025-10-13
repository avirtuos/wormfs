//! Factory for creating MetadataStore instances.

use super::{Config, Error, MetadataStore};
use crate::metadata_store::implementation::MetadataStoreImpl;

/// Factory for creating MetadataStore instances.
///
/// This factory provides a clean separation between the MetadataStore trait
/// and its concrete implementation, making the code more testable and flexible.
pub struct MetadataStoreFactory;

impl MetadataStoreFactory {
    /// Create a new MetadataStore instance with the default implementation.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration including database path and tuning parameters
    ///
    /// # Returns
    ///
    /// A cloneable MetadataStore handle.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database file cannot be opened
    /// - Configuration is invalid
    /// - Connection pool initialization fails
    ///
    /// # Example
    ///
    /// ```ignore
    /// use wormfs::metadata_store::{MetadataStoreFactory, Config};
    ///
    /// let config = Config::default();
    /// let store = MetadataStoreFactory::create(config)?;
    /// store.initialize_schema().await?;
    /// ```
    pub fn create(config: Config) -> Result<impl MetadataStore, Error> {
        MetadataStoreImpl::new(config)
    }
}
