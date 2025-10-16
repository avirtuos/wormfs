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
    /// let store = MetadataStoreFactory::create(config).await?;
    /// store.initialize_schema().await?;
    /// ```
    pub async fn create(config: Config) -> Result<impl MetadataStore, Error> {
        MetadataStoreImpl::new(config).await
    }

    /// Create a new MetadataStore instance, returning the concrete type.
    ///
    /// This method is useful when you need the concrete `MetadataStoreImpl` type
    /// rather than the opaque `impl MetadataStore`. This is commonly needed when:
    /// - Passing to `FileSystemServiceImplFactory` which requires concrete types
    /// - Testing scenarios where you need access to implementation-specific methods
    /// - Avoiding unsafe transmute operations
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration including database path and tuning parameters
    ///
    /// # Returns
    ///
    /// A `MetadataStoreImpl` instance (concrete type, not opaque).
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
    /// let store = MetadataStoreFactory::create_concrete(config).await?;
    /// store.initialize_schema().await?;
    ///
    /// // Can now pass to FileSystemServiceImplFactory
    /// let service = FileSystemServiceImplFactory::create(fs_config, store, file_store)?;
    /// ```
    pub async fn create_concrete(config: Config) -> Result<MetadataStoreImpl, Error> {
        MetadataStoreImpl::new(config).await
    }
}
