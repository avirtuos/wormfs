//! redb-based implementation of TransactionLogStore.

use super::types::{LogEntry, LogError, LogStats, TransactionLogConfig};
use super::TransactionLogStore;
use async_trait::async_trait;
use crc32fast::Hasher;
use redb::{Database, ReadableTable, TableDefinition};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tokio::task;

/// Table definition for log entries
/// Key: u64 (log index), Value: serialized LogEntryData
const LOG_ENTRIES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("log_entries");

/// Table definition for metadata
/// Keys: "first_index", "last_index", "format_version"
const METADATA_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");

/// Serializable log entry data structure
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LogEntryData {
    term: u64,
    operations: Vec<u8>,
    timestamp_secs: u64,
    timestamp_nanos: u32,
    checksum: u32,
}

impl LogEntryData {
    /// Create a new log entry with CRC32 checksum
    fn new(term: u64, operations: Vec<u8>, timestamp: SystemTime) -> Self {
        let duration = timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let timestamp_secs = duration.as_secs();
        let timestamp_nanos = duration.subsec_nanos();

        // Calculate CRC32 checksum of the data
        let mut hasher = Hasher::new();
        hasher.update(&term.to_le_bytes());
        hasher.update(&operations);
        hasher.update(&timestamp_secs.to_le_bytes());
        hasher.update(&timestamp_nanos.to_le_bytes());
        let checksum = hasher.finalize();

        Self {
            term,
            operations,
            timestamp_secs,
            timestamp_nanos,
            checksum,
        }
    }

    /// Verify the CRC32 checksum
    fn verify_checksum(&self) -> bool {
        let mut hasher = Hasher::new();
        hasher.update(&self.term.to_le_bytes());
        hasher.update(&self.operations);
        hasher.update(&self.timestamp_secs.to_le_bytes());
        hasher.update(&self.timestamp_nanos.to_le_bytes());
        let computed_checksum = hasher.finalize();

        computed_checksum == self.checksum
    }

    /// Convert to LogEntry
    fn to_log_entry(&self, index: u64) -> LogEntry {
        let timestamp = SystemTime::UNIX_EPOCH
            + std::time::Duration::new(self.timestamp_secs, self.timestamp_nanos);

        LogEntry {
            index,
            term: self.term,
            operations: self.operations.clone(),
            timestamp,
        }
    }
}

/// Inner implementation with interior mutability
struct TransactionLogStoreInner {
    db: RwLock<Database>,
    config: TransactionLogConfig,
    // Cached indices for quick access (updated on writes)
    cached_first_index: RwLock<Option<u64>>,
    cached_last_index: RwLock<Option<u64>>,
}

/// TransactionLogStore implementation using redb
#[derive(Clone)]
pub struct TransactionLogStoreImpl {
    inner: Arc<TransactionLogStoreInner>,
}

impl TransactionLogStoreImpl {
    /// Create a new TransactionLogStore instance
    pub fn new(config: TransactionLogConfig) -> Result<Self, LogError> {
        // Ensure parent directory exists
        if let Some(parent) = config.db_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                LogError::DatabaseError(format!("Failed to create directory: {}", e))
            })?;
        }

        // Open or create the database
        let db = Database::create(&config.db_path)
            .map_err(|e| LogError::DatabaseError(format!("Failed to open database: {}", e)))?;

        // Initialize tables
        {
            let write_txn = db.begin_write().map_err(|e| {
                LogError::DatabaseError(format!("Failed to begin write transaction: {}", e))
            })?;

            // Create tables if they don't exist
            write_txn.open_table(LOG_ENTRIES_TABLE).map_err(|e| {
                LogError::DatabaseError(format!("Failed to open log_entries table: {}", e))
            })?;

            write_txn.open_table(METADATA_TABLE).map_err(|e| {
                LogError::DatabaseError(format!("Failed to open metadata table: {}", e))
            })?;

            write_txn.commit().map_err(|e| {
                LogError::DatabaseError(format!("Failed to commit transaction: {}", e))
            })?;
        }

        // Load cached indices
        let (first_index, last_index) = Self::load_indices_from_db(&db)?;

        Ok(Self {
            inner: Arc::new(TransactionLogStoreInner {
                db: RwLock::new(db),
                config,
                cached_first_index: RwLock::new(first_index),
                cached_last_index: RwLock::new(last_index),
            }),
        })
    }

    /// Load first and last indices from the database
    fn load_indices_from_db(db: &Database) -> Result<(Option<u64>, Option<u64>), LogError> {
        let read_txn = db.begin_read().map_err(|e| {
            LogError::DatabaseError(format!("Failed to begin read transaction: {}", e))
        })?;

        let table = read_txn.open_table(LOG_ENTRIES_TABLE).map_err(|e| {
            LogError::DatabaseError(format!("Failed to open log_entries table: {}", e))
        })?;

        // Get first index
        let first_index = table
            .first()
            .map_err(|e| LogError::DatabaseError(format!("Failed to get first entry: {}", e)))?
            .map(|(k, _)| k.value());

        // Get last index
        let last_index = table
            .last()
            .map_err(|e| LogError::DatabaseError(format!("Failed to get last entry: {}", e)))?
            .map(|(k, _)| k.value());

        Ok((first_index, last_index))
    }

    /// Update cached indices
    fn update_cached_indices(&self, new_first: Option<u64>, new_last: Option<u64>) {
        if let Some(first) = new_first {
            *self.inner.cached_first_index.write().unwrap() = Some(first);
        }
        if let Some(last) = new_last {
            *self.inner.cached_last_index.write().unwrap() = Some(last);
        }
    }

    /// Get a log entry from the database (blocking operation)
    fn get_entry_blocking(&self, index: u64) -> Result<LogEntry, LogError> {
        let db = self.inner.db.read().unwrap();
        let read_txn = db.begin_read().map_err(|e| {
            LogError::DatabaseError(format!("Failed to begin read transaction: {}", e))
        })?;

        let table = read_txn.open_table(LOG_ENTRIES_TABLE).map_err(|e| {
            LogError::DatabaseError(format!("Failed to open log_entries table: {}", e))
        })?;

        let value = table.get(index).map_err(|e| {
            LogError::DatabaseError(format!("Failed to get entry at index {}: {}", index, e))
        })?;

        match value {
            Some(data_guard) => {
                let data: LogEntryData = bincode::deserialize(data_guard.value()).map_err(|e| {
                    LogError::SerializationError(format!("Failed to deserialize entry: {}", e))
                })?;

                // Verify checksum
                if !data.verify_checksum() {
                    return Err(LogError::DatabaseError(format!(
                        "Checksum verification failed for entry at index {}",
                        index
                    )));
                }

                Ok(data.to_log_entry(index))
            }
            None => Err(LogError::EntryNotFound(index)),
        }
    }

    /// Get a range of log entries (blocking operation)
    fn get_entries_blocking(
        &self,
        start_index: u64,
        end_index: u64,
    ) -> Result<Vec<LogEntry>, LogError> {
        if start_index > end_index {
            return Err(LogError::InvalidIndex(start_index));
        }

        let db = self.inner.db.read().unwrap();
        let read_txn = db.begin_read().map_err(|e| {
            LogError::DatabaseError(format!("Failed to begin read transaction: {}", e))
        })?;

        let table = read_txn.open_table(LOG_ENTRIES_TABLE).map_err(|e| {
            LogError::DatabaseError(format!("Failed to open log_entries table: {}", e))
        })?;

        let mut entries = Vec::new();

        // Use range query for efficient retrieval
        let range = start_index..=end_index;
        for result in table
            .range(range)
            .map_err(|e| LogError::DatabaseError(format!("Failed to query range: {}", e)))?
        {
            let (key, value) = result
                .map_err(|e| LogError::DatabaseError(format!("Failed to read entry: {}", e)))?;

            let index = key.value();
            let data: LogEntryData = bincode::deserialize(value.value()).map_err(|e| {
                LogError::SerializationError(format!("Failed to deserialize entry: {}", e))
            })?;

            // Verify checksum
            if !data.verify_checksum() {
                return Err(LogError::DatabaseError(format!(
                    "Checksum verification failed for entry at index {}",
                    index
                )));
            }

            entries.push(data.to_log_entry(index));
        }

        Ok(entries)
    }

    /// Append a single entry (blocking operation)
    fn append_blocking(&self, term: u64, data: Vec<u8>) -> Result<u64, LogError> {
        let db = self.inner.db.write().unwrap();
        let write_txn = db.begin_write().map_err(|e| {
            LogError::DatabaseError(format!("Failed to begin write transaction: {}", e))
        })?;

        let next_index = {
            let mut table = write_txn.open_table(LOG_ENTRIES_TABLE).map_err(|e| {
                LogError::DatabaseError(format!("Failed to open log_entries table: {}", e))
            })?;

            // Determine next index
            let next_index = match table
                .last()
                .map_err(|e| LogError::DatabaseError(format!("Failed to get last entry: {}", e)))?
            {
                Some((k, _)) => k.value() + 1,
                None => 1, // Start at index 1
            };

            // Create log entry with checksum
            let entry_data = LogEntryData::new(term, data, SystemTime::now());
            let serialized = bincode::serialize(&entry_data).map_err(|e| {
                LogError::SerializationError(format!("Failed to serialize entry: {}", e))
            })?;

            // Insert entry
            table
                .insert(next_index, serialized.as_slice())
                .map_err(|e| LogError::DatabaseError(format!("Failed to insert entry: {}", e)))?;

            next_index
        };

        // Commit transaction (this includes fsync)
        write_txn
            .commit()
            .map_err(|e| LogError::DatabaseError(format!("Failed to commit transaction: {}", e)))?;

        // Update cached indices
        self.update_cached_indices(
            if next_index == 1 { Some(1) } else { None },
            Some(next_index),
        );

        Ok(next_index)
    }

    /// Append multiple entries atomically (blocking operation)
    fn append_batch_blocking(&self, entries: Vec<(u64, Vec<u8>)>) -> Result<u64, LogError> {
        if entries.is_empty() {
            return Err(LogError::InvalidIndex(0));
        }

        let db = self.inner.db.write().unwrap();
        let write_txn = db.begin_write().map_err(|e| {
            LogError::DatabaseError(format!("Failed to begin write transaction: {}", e))
        })?;

        let start_index = {
            let mut table = write_txn.open_table(LOG_ENTRIES_TABLE).map_err(|e| {
                LogError::DatabaseError(format!("Failed to open log_entries table: {}", e))
            })?;

            // Determine starting index
            let start_index = match table
                .last()
                .map_err(|e| LogError::DatabaseError(format!("Failed to get last entry: {}", e)))?
            {
                Some((k, _)) => k.value() + 1,
                None => 1,
            };

            // Insert all entries
            for (i, (term, data)) in entries.iter().enumerate() {
                let index = start_index + i as u64;
                let entry_data = LogEntryData::new(*term, data.clone(), SystemTime::now());
                let serialized = bincode::serialize(&entry_data).map_err(|e| {
                    LogError::SerializationError(format!("Failed to serialize entry: {}", e))
                })?;

                table.insert(index, serialized.as_slice()).map_err(|e| {
                    LogError::DatabaseError(format!(
                        "Failed to insert entry at index {}: {}",
                        index, e
                    ))
                })?;
            }

            start_index
        };

        // Commit transaction (this includes fsync for all entries)
        write_txn
            .commit()
            .map_err(|e| LogError::DatabaseError(format!("Failed to commit transaction: {}", e)))?;

        // Update cached indices
        let last_index = start_index + entries.len() as u64 - 1;
        self.update_cached_indices(
            if start_index == 1 { Some(1) } else { None },
            Some(last_index),
        );

        Ok(start_index)
    }

    /// Trim entries before the specified index (blocking operation)
    fn trim_blocking(&self, up_to_index: u64) -> Result<u64, LogError> {
        let db = self.inner.db.write().unwrap();
        let write_txn = db.begin_write().map_err(|e| {
            LogError::DatabaseError(format!("Failed to begin write transaction: {}", e))
        })?;

        let (count, new_first_index) = {
            let mut table = write_txn.open_table(LOG_ENTRIES_TABLE).map_err(|e| {
                LogError::DatabaseError(format!("Failed to open log_entries table: {}", e))
            })?;

            // Find all indices to remove
            let indices_to_remove: Vec<u64> = table
                .range(0u64..up_to_index)
                .map_err(|e| LogError::DatabaseError(format!("Failed to query range: {}", e)))?
                .map(|result| {
                    result.map(|(k, _)| k.value()).map_err(|e| {
                        LogError::DatabaseError(format!("Failed to read entry: {}", e))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            let count = indices_to_remove.len() as u64;

            // Remove entries
            for index in indices_to_remove {
                table.remove(index).map_err(|e| {
                    LogError::DatabaseError(format!(
                        "Failed to remove entry at index {}: {}",
                        index, e
                    ))
                })?;
            }

            // Determine new first index
            let new_first_index = table
                .first()
                .map_err(|e| LogError::DatabaseError(format!("Failed to get first entry: {}", e)))?
                .map(|(k, _)| k.value());

            (count, new_first_index)
        };

        // Commit transaction
        write_txn
            .commit()
            .map_err(|e| LogError::DatabaseError(format!("Failed to commit transaction: {}", e)))?;

        // Update cached first index
        *self.inner.cached_first_index.write().unwrap() = new_first_index;

        Ok(count)
    }
}

#[async_trait]
impl TransactionLogStore for TransactionLogStoreImpl {
    fn new(config: TransactionLogConfig) -> Result<Self, LogError>
    where
        Self: Sized,
    {
        TransactionLogStoreImpl::new(config)
    }

    async fn append(&self, term: u64, data: Vec<u8>) -> Result<u64, LogError> {
        let self_clone = self.clone();
        task::spawn_blocking(move || self_clone.append_blocking(term, data))
            .await
            .map_err(|e| LogError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn append_batch(&self, entries: Vec<(u64, Vec<u8>)>) -> Result<u64, LogError> {
        let self_clone = self.clone();
        task::spawn_blocking(move || self_clone.append_batch_blocking(entries))
            .await
            .map_err(|e| LogError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn get_entry(&self, index: u64) -> Result<LogEntry, LogError> {
        let self_clone = self.clone();
        task::spawn_blocking(move || self_clone.get_entry_blocking(index))
            .await
            .map_err(|e| LogError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn get_entries(
        &self,
        start_index: u64,
        end_index: u64,
    ) -> Result<Vec<LogEntry>, LogError> {
        let self_clone = self.clone();
        task::spawn_blocking(move || self_clone.get_entries_blocking(start_index, end_index))
            .await
            .map_err(|e| LogError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn get_last_entry(&self) -> Result<LogEntry, LogError> {
        let last_index = self.get_last_index();
        if last_index == 0 {
            return Err(LogError::EntryNotFound(0));
        }
        self.get_entry(last_index).await
    }

    fn get_last_index(&self) -> u64 {
        self.inner.cached_last_index.read().unwrap().unwrap_or(0)
    }

    fn get_first_index(&self) -> u64 {
        self.inner.cached_first_index.read().unwrap().unwrap_or(0)
    }

    async fn trim(&self, up_to_index: u64) -> Result<u64, LogError> {
        let self_clone = self.clone();
        task::spawn_blocking(move || self_clone.trim_blocking(up_to_index))
            .await
            .map_err(|e| LogError::DatabaseError(format!("Task join error: {}", e)))?
    }

    fn get_stats(&self) -> LogStats {
        let first_index = *self.inner.cached_first_index.read().unwrap();
        let last_index = *self.inner.cached_last_index.read().unwrap();

        let entry_count = match (first_index, last_index) {
            (Some(first), Some(last)) => last - first + 1,
            _ => 0,
        };

        // Get database file size
        let db_size_bytes = std::fs::metadata(&self.inner.config.db_path)
            .map(|m| m.len())
            .unwrap_or(0);

        LogStats {
            first_index,
            last_index,
            entry_count,
            db_size_bytes,
            last_compaction: None, // TODO: Track compaction time
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_config() -> (TransactionLogConfig, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_log.redb");

        let config = TransactionLogConfig {
            db_path,
            cache_size_mb: 8,
            compact_threshold_mb: 100,
            max_log_size_mb: 128,
            max_log_age_days: 7,
        };

        (config, temp_dir)
    }

    #[tokio::test]
    async fn test_new_creates_database() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config.clone()).unwrap();

        assert_eq!(store.get_first_index(), 0);
        assert_eq!(store.get_last_index(), 0);
        assert!(config.db_path.exists());
    }

    #[tokio::test]
    async fn test_append_and_get_entry() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        let data = b"test operation".to_vec();
        let index = store.append(1, data.clone()).await.unwrap();

        assert_eq!(index, 1);
        assert_eq!(store.get_last_index(), 1);

        let entry = store.get_entry(index).await.unwrap();
        assert_eq!(entry.index, 1);
        assert_eq!(entry.term, 1);
        assert_eq!(entry.operations, data);
    }

    #[tokio::test]
    async fn test_append_multiple_entries() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        for i in 1..=5 {
            let data = format!("operation {}", i).into_bytes();
            let index = store.append(i, data).await.unwrap();
            assert_eq!(index, i);
        }

        assert_eq!(store.get_first_index(), 1);
        assert_eq!(store.get_last_index(), 5);
    }

    #[tokio::test]
    async fn test_append_batch() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        let entries = vec![
            (1, b"op1".to_vec()),
            (1, b"op2".to_vec()),
            (2, b"op3".to_vec()),
        ];

        let start_index = store.append_batch(entries).await.unwrap();
        assert_eq!(start_index, 1);
        assert_eq!(store.get_last_index(), 3);

        let entry1 = store.get_entry(1).await.unwrap();
        assert_eq!(entry1.term, 1);
        assert_eq!(entry1.operations, b"op1");

        let entry3 = store.get_entry(3).await.unwrap();
        assert_eq!(entry3.term, 2);
    }

    #[tokio::test]
    async fn test_get_entries_range() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        for i in 1..=10 {
            let data = format!("operation {}", i).into_bytes();
            store.append(i, data).await.unwrap();
        }

        let entries = store.get_entries(3, 7).await.unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].index, 3);
        assert_eq!(entries[4].index, 7);
    }

    #[tokio::test]
    async fn test_get_last_entry() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        let data = b"last operation".to_vec();
        store.append(1, b"first".to_vec()).await.unwrap();
        store.append(2, data.clone()).await.unwrap();

        let last_entry = store.get_last_entry().await.unwrap();
        assert_eq!(last_entry.index, 2);
        assert_eq!(last_entry.term, 2);
        assert_eq!(last_entry.operations, data);
    }

    #[tokio::test]
    async fn test_get_last_entry_empty_log() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        let result = store.get_last_entry().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_trim() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        for i in 1..=10 {
            store
                .append(i, format!("op{}", i).into_bytes())
                .await
                .unwrap();
        }

        let trimmed = store.trim(5).await.unwrap();
        assert_eq!(trimmed, 4); // Entries 1-4 removed

        assert_eq!(store.get_first_index(), 5);
        assert_eq!(store.get_last_index(), 10);

        let result = store.get_entry(3).await;
        assert!(result.is_err());

        let entry5 = store.get_entry(5).await.unwrap();
        assert_eq!(entry5.index, 5);
    }

    #[tokio::test]
    async fn test_get_stats() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        for i in 1..=5 {
            store.append(i, b"test".to_vec()).await.unwrap();
        }

        let stats = store.get_stats();
        assert_eq!(stats.first_index, Some(1));
        assert_eq!(stats.last_index, Some(5));
        assert_eq!(stats.entry_count, 5);
        assert!(stats.db_size_bytes > 0);
    }

    #[tokio::test]
    async fn test_checksum_verification() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        let data = b"test data with checksum".to_vec();
        let index = store.append(1, data.clone()).await.unwrap();

        // Verify entry can be retrieved (checksum is valid)
        let entry = store.get_entry(index).await.unwrap();
        assert_eq!(entry.operations, data);
    }

    #[tokio::test]
    async fn test_persistence_across_restarts() {
        let (config, _temp_dir) = create_test_config();

        // Create store and add entries
        {
            let store = TransactionLogStoreImpl::new(config.clone()).unwrap();
            store.append(1, b"entry1".to_vec()).await.unwrap();
            store.append(2, b"entry2".to_vec()).await.unwrap();
        } // Store dropped here

        // Reopen database
        let store = TransactionLogStoreImpl::new(config).unwrap();
        assert_eq!(store.get_first_index(), 1);
        assert_eq!(store.get_last_index(), 2);

        let entry1 = store.get_entry(1).await.unwrap();
        assert_eq!(entry1.operations, b"entry1");
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let (config, _temp_dir) = create_test_config();
        let store = Arc::new(TransactionLogStoreImpl::new(config).unwrap());

        let mut handles = vec![];

        // Spawn multiple tasks to append entries
        for i in 0..10 {
            let store_clone = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                store_clone
                    .append(1, format!("concurrent op {}", i).into_bytes())
                    .await
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        assert_eq!(store.get_last_index(), 10);
    }
}
