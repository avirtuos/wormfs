//! Factory for creating StorageNode instances.

use super::implementation::StorageNodeImpl;
use super::{Config, Error, StorageNode};

/// Factory for creating StorageNode instances.
///
/// This factory provides a clean separation between the StorageNode trait
/// and its concrete implementation, supporting dependency injection and testing.
pub struct StorageNodeFactory;

impl StorageNodeFactory {
    /// Create a new StorageNode instance with the Phase 1 implementation.
    ///
    /// This method initializes all components in the correct dependency order:
    /// 1. MetadataStore (database)
    /// 2. FileStore (chunk storage)
    /// 3. FileSystemService (FUSE layer)
    ///
    /// # Arguments
    ///
    /// * `config` - Complete configuration for the storage node
    ///
    /// # Returns
    ///
    /// A StorageNode instance ready to be started.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Configuration is invalid
    /// - Any component fails to initialize
    /// - Required directories cannot be created
    /// - Database initialization fails
    ///
    /// # Example
    ///
    /// ```ignore
    /// use wormfs::storage_node::{StorageNodeFactory, Config};
    ///
    /// let config = Config::default();
    /// let node = StorageNodeFactory::create(config).await?;
    /// node.start().await?;
    /// ```
    pub async fn create(config: Config) -> Result<impl StorageNode, Error> {
        StorageNodeImpl::new_internal(config).await
    }

    /// Create a new StorageNode instance, returning the concrete type.
    ///
    /// This is useful when you need access to implementation-specific methods
    /// that aren't part of the StorageNode trait.
    ///
    /// # Arguments
    ///
    /// * `config` - Complete configuration for the storage node
    ///
    /// # Returns
    ///
    /// A StorageNodeImpl instance (concrete type, not opaque).
    ///
    /// # Errors
    ///
    /// Same error conditions as `create()`.
    pub async fn create_concrete(config: Config) -> Result<StorageNodeImpl, Error> {
        StorageNodeImpl::new_internal(config).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Test that factory can create a StorageNode with default configuration
    #[tokio::test]
    async fn test_factory_create_default() {
        let temp_dir = TempDir::new().unwrap();

        let config = Config {
            node_id: "test-node-001".to_string(),
            data_dir: temp_dir.path().to_path_buf(),
            metadata_db_path: temp_dir.path().join("metadata.db"),
            default_stripe_size: 1024 * 1024, // 1MB
            default_data_shards: 2,
            default_parity_shards: 1,
            default_uid: 1000,
            default_gid: 1000,
            lock_timeout: std::time::Duration::from_secs(30),
            ..Default::default()
        };

        let node = StorageNodeFactory::create_concrete(config)
            .await
            .expect("Failed to create StorageNode");

        let status = node.get_status();
        assert_eq!(status.node_id, "test-node-001");
        assert!(!status.started); // Not started yet
        assert!(status.components.metadata_store);
        assert!(status.components.file_store);
        assert!(status.components.filesystem_service);
    }

    /// Test that factory returns error for invalid configuration
    #[tokio::test]
    async fn test_factory_invalid_config() {
        // Invalid: metadata_db_path in read-only location
        let config = Config {
            node_id: "test-node-002".to_string(),
            data_dir: PathBuf::from("/nonexistent/readonly/path"),
            metadata_db_path: PathBuf::from("/nonexistent/readonly/metadata.db"),
            default_stripe_size: 1024 * 1024,
            default_data_shards: 2,
            default_parity_shards: 1,
            default_uid: 1000,
            default_gid: 1000,
            lock_timeout: std::time::Duration::from_secs(30),
            ..Default::default()
        };

        let result = StorageNodeFactory::create(config).await;
        assert!(result.is_err(), "Should fail with invalid path");
    }

    /// Test that created node can be started and stopped
    #[tokio::test]
    async fn test_node_lifecycle() {
        let temp_dir = TempDir::new().unwrap();

        let config = Config {
            node_id: "test-node-003".to_string(),
            data_dir: temp_dir.path().to_path_buf(),
            metadata_db_path: temp_dir.path().join("metadata.db"),
            default_stripe_size: 1024 * 1024,
            default_data_shards: 2,
            default_parity_shards: 1,
            default_uid: 1000,
            default_gid: 1000,
            lock_timeout: std::time::Duration::from_secs(30),
            ..Default::default()
        };

        let mut node = StorageNodeFactory::create_concrete(config)
            .await
            .expect("Failed to create StorageNode");

        // Start the node
        node.start().await.expect("Failed to start node");
        assert!(node.get_status().started);

        // Shutdown the node
        node.shutdown().await.expect("Failed to shutdown node");
        assert!(!node.get_status().started);
    }
}
