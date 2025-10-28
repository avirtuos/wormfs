//! Factory for creating TransactionLogStore instances.

use super::implementation::TransactionLogStoreImpl;
use super::types::{LogError, TransactionLogConfig};

/// Factory for creating TransactionLogStore instances.
pub struct TransactionLogStoreFactory;

impl TransactionLogStoreFactory {
    /// Create a new TransactionLogStore instance.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for the transaction log store
    ///
    /// # Returns
    ///
    /// A new TransactionLogStore instance (boxed trait object).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Database initialization fails
    /// - Configuration is invalid
    /// - I/O error occurs
    ///
    pub fn new(
        config: TransactionLogConfig,
    ) -> Result<Box<dyn super::TransactionLogStore>, LogError> {
        let store = TransactionLogStoreImpl::new(config)?;
        Ok(Box::new(store))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_factory_creates_store() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_log.redb");

        let config = TransactionLogConfig {
            db_path: db_path.clone(),
            cache_size_mb: 8,
            compact_threshold_mb: 100,
            max_log_size_mb: 128,
            max_log_age_days: 7,
        };

        let result = TransactionLogStoreFactory::new(config);
        assert!(result.is_ok());
        assert!(db_path.exists());
    }
}
