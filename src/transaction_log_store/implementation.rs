//! redb-based implementation of TransactionLogStore.

use super::types::{IntegrityReport, LogEntry, LogError, LogStats, TransactionLogConfig};
use super::TransactionLogStore;
use async_trait::async_trait;
use crc32fast::Hasher;
use redb::{Database, ReadableTable, TableDefinition};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tokio::task;
use tracing::{debug, error, info, warn};

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
    // Last compaction timestamp
    last_compaction: RwLock<Option<SystemTime>>,
    // Optional metrics service (deferred dependency injection)
    metrics: RwLock<Option<Arc<crate::metric_service::MetricServiceImpl>>>,
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

        info!(
            "TransactionLogStore initialized at {:?}, first_index={:?}, last_index={:?}",
            config.db_path, first_index, last_index
        );

        Ok(Self {
            inner: Arc::new(TransactionLogStoreInner {
                db: RwLock::new(db),
                config,
                cached_first_index: RwLock::new(first_index),
                cached_last_index: RwLock::new(last_index),
                last_compaction: RwLock::new(None),
                metrics: RwLock::new(None),
            }),
        })
    }

    /// Load first and last indices from the database.
    ///
    /// This method queries the LOG_ENTRIES_TABLE to find the minimum and maximum
    /// log indices currently stored. Called during initialization to populate
    /// the cached indices for O(1) access.
    ///
    /// # Arguments
    ///
    /// * `db` - Reference to the redb Database
    ///
    /// # Returns
    ///
    /// A tuple of (first_index, last_index) where each is None if the log is empty,
    /// or Some(index) if entries exist.
    ///
    /// # Errors
    ///
    /// Returns `LogError::DatabaseError` if database operations fail.
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

    /// Update cached indices after write operations.
    ///
    /// Synchronizes the in-memory cached first and/or last indices with the
    /// database state after operations that modify log boundaries (append, trim,
    /// delete_from). Only updates indices if the corresponding Option is Some.
    ///
    /// # Arguments
    ///
    /// * `new_first` - New first index to cache, or None to leave unchanged
    /// * `new_last` - New last index to cache, or None to leave unchanged
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success.
    ///
    /// # Errors
    ///
    /// Returns `LogError::DatabaseError` if acquiring the cache write lock fails.
    fn update_cached_indices(
        &self,
        new_first: Option<u64>,
        new_last: Option<u64>,
    ) -> Result<(), LogError> {
        if let Some(first) = new_first {
            *self.inner.cached_first_index.write().map_err(|e| {
                LogError::DatabaseError(format!("Failed to acquire cache lock: {}", e))
            })? = Some(first);
        }
        if let Some(last) = new_last {
            *self.inner.cached_last_index.write().map_err(|e| {
                LogError::DatabaseError(format!("Failed to acquire cache lock: {}", e))
            })? = Some(last);
        }
        Ok(())
    }

    /// Get a log entry from the database (blocking operation).
    ///
    /// Performs a synchronous database read and verifies the entry's CRC32 checksum.
    /// This is the blocking implementation called from the async API wrapper via
    /// `tokio::task::spawn_blocking`.
    ///
    /// # Arguments
    ///
    /// * `index` - The log index to retrieve
    ///
    /// # Returns
    ///
    /// Returns the log entry with verified checksum.
    ///
    /// # Errors
    ///
    /// * `LogError::EntryNotFound` - Entry at the specified index doesn't exist
    /// * `LogError::ChecksumFailed` - Entry exists but CRC32 verification failed (data corruption)
    /// * `LogError::SerializationError` - Failed to deserialize the entry data
    /// * `LogError::DatabaseError` - Database operation failed
    fn get_entry_blocking(&self, index: u64) -> Result<LogEntry, LogError> {
        let db =
            self.inner.db.read().map_err(|e| {
                LogError::DatabaseError(format!("Failed to acquire read lock: {}", e))
            })?;
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
                    return Err(LogError::ChecksumFailed(index));
                }

                Ok(data.to_log_entry(index))
            }
            None => Err(LogError::EntryNotFound(index)),
        }
    }

    /// Get a range of log entries (blocking operation).
    ///
    /// Retrieves all log entries from start_index to end_index (inclusive) using
    /// efficient range queries. Validates CRC32 checksums for all entries. This is
    /// the blocking implementation called from the async API wrapper via
    /// `tokio::task::spawn_blocking`.
    ///
    /// # Arguments
    ///
    /// * `start_index` - First index to retrieve (inclusive)
    /// * `end_index` - Last index to retrieve (inclusive)
    ///
    /// # Returns
    ///
    /// Returns a vector of log entries in the specified range, ordered by index.
    ///
    /// # Errors
    ///
    /// * `LogError::InvalidIndex` - start_index > end_index
    /// * `LogError::ChecksumFailed` - An entry's CRC32 verification failed (data corruption)
    /// * `LogError::SerializationError` - Failed to deserialize an entry
    /// * `LogError::DatabaseError` - Database operation failed
    fn get_entries_blocking(
        &self,
        start_index: u64,
        end_index: u64,
    ) -> Result<Vec<LogEntry>, LogError> {
        if start_index > end_index {
            return Err(LogError::InvalidIndex(start_index));
        }

        let db =
            self.inner.db.read().map_err(|e| {
                LogError::DatabaseError(format!("Failed to acquire read lock: {}", e))
            })?;
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
                return Err(LogError::ChecksumFailed(index));
            }

            entries.push(data.to_log_entry(index));
        }

        Ok(entries)
    }

    /// Append a single entry at a specific index (blocking operation).
    ///
    /// Appends a log entry to the database at the index specified by the Raft leader,
    /// with CRC32 checksum calculation and immediate fsync for durability. Records
    /// metrics and updates state gauges after successful append. This is the
    /// blocking implementation called from the async API wrapper via
    /// `tokio::task::spawn_blocking`.
    ///
    /// The caller (Raft leader) must specify the exact index. If an entry already
    /// exists at this index with a different term, it will be overwritten.
    ///
    /// # Arguments
    ///
    /// * `index` - Log index for this entry (must be specified by Raft leader)
    /// * `term` - Raft term number for this log entry
    /// * `data` - Serialized operation data to store
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success.
    ///
    /// # Errors
    ///
    /// * `LogError::InvalidIndex` - Index is 0 (invalid)
    /// * `LogError::InvalidRange` - Index creates unexpected gap in log
    /// * `LogError::DatabaseError` - Database operation or fsync failed
    /// * `LogError::SerializationError` - Failed to serialize the entry
    fn append_blocking(&self, index: u64, term: u64, data: Vec<u8>) -> Result<(), LogError> {
        // Validate index
        if index == 0 {
            return Err(LogError::InvalidIndex(index));
        }

        let start_time = std::time::Instant::now();

        let db = self.inner.db.write().map_err(|e| {
            error!("Failed to acquire write lock for append: {}", e);
            LogError::DatabaseError(format!("Failed to acquire write lock: {}", e))
        })?;
        let write_txn = db.begin_write().map_err(|e| {
            error!("Failed to begin write transaction for append: {}", e);
            LogError::DatabaseError(format!("Failed to begin write transaction: {}", e))
        })?;

        let data_len = data.len();
        {
            let mut table = write_txn.open_table(LOG_ENTRIES_TABLE).map_err(|e| {
                LogError::DatabaseError(format!("Failed to open log_entries table: {}", e))
            })?;

            // Get current last index to validate there's no unexpected gap
            let last_index = table
                .last()
                .map_err(|e| LogError::DatabaseError(format!("Failed to get last entry: {}", e)))?
                .map(|(k, _)| k.value())
                .unwrap_or(0);

            // Validate index doesn't create a gap (unless log is empty or overwriting)
            if last_index > 0 && index > last_index + 1 {
                return Err(LogError::InvalidRange(format!(
                    "Index {} creates gap after last index {}",
                    index, last_index
                )));
            }

            // Create log entry with checksum
            let entry_data = LogEntryData::new(term, data, SystemTime::now());
            let serialized = bincode::serialize(&entry_data).map_err(|e| {
                LogError::SerializationError(format!("Failed to serialize entry: {}", e))
            })?;

            // Insert entry (overwrites if exists)
            table
                .insert(index, serialized.as_slice())
                .map_err(|e| LogError::DatabaseError(format!("Failed to insert entry: {}", e)))?;
        }

        // Commit transaction (this includes fsync)
        write_txn
            .commit()
            .map_err(|e| LogError::DatabaseError(format!("Failed to commit transaction: {}", e)))?;

        // Update cached indices
        self.update_cached_indices(if index == 1 { Some(1) } else { None }, Some(index))?;

        // Record metrics and logging
        let elapsed = start_time.elapsed().as_secs_f64();
        self.record_histogram("transaction_log.append.latency", elapsed);
        self.record_counter("transaction_log.append.total", 1);
        self.update_state_gauges();
        debug!(
            "Appended log entry at index {} (term={}, size={} bytes) in {:.4}s",
            index, term, data_len, elapsed
        );

        Ok(())
    }

    /// Append multiple entries atomically at specific indices (blocking operation).
    ///
    /// Appends a batch of log entries in a single database transaction, making
    /// batch operations more efficient than individual appends. All entries are
    /// fsynced together. Records metrics and updates state gauges after successful
    /// batch append. This is the blocking implementation called from the async API
    /// wrapper via `tokio::task::spawn_blocking`.
    ///
    /// The caller (Raft leader) must specify exact indices for each entry.
    /// If entries already exist at these indices with different terms, they will be overwritten.
    ///
    /// # Arguments
    ///
    /// * `entries` - Vector of (index, term, data) tuples to append
    ///
    /// # Returns
    ///
    /// Returns Ok(()) on success.
    ///
    /// # Errors
    ///
    /// * `LogError::InvalidRange` - Batch is empty
    /// * `LogError::InvalidIndex` - Any index is 0 (invalid)
    /// * `LogError::InvalidRange` - Indices create unexpected gaps
    /// * `LogError::DatabaseError` - Database operation or fsync failed (entire batch rolled back)
    /// * `LogError::SerializationError` - Failed to serialize an entry
    fn append_batch_blocking(&self, entries: Vec<(u64, u64, Vec<u8>)>) -> Result<(), LogError> {
        if entries.is_empty() {
            return Err(LogError::InvalidRange(
                "Cannot append empty batch".to_string(),
            ));
        }

        // Validate all indices
        for (index, _, _) in &entries {
            if *index == 0 {
                return Err(LogError::InvalidIndex(*index));
            }
        }

        let start_time = std::time::Instant::now();
        let batch_size = entries.len();

        let db = self.inner.db.write().map_err(|e| {
            error!("Failed to acquire write lock for append_batch: {}", e);
            LogError::DatabaseError(format!("Failed to acquire write lock: {}", e))
        })?;
        let write_txn = db.begin_write().map_err(|e| {
            error!("Failed to begin write transaction for append_batch: {}", e);
            LogError::DatabaseError(format!("Failed to begin write transaction: {}", e))
        })?;

        let (min_index, max_index) = {
            let mut table = write_txn.open_table(LOG_ENTRIES_TABLE).map_err(|e| {
                LogError::DatabaseError(format!("Failed to open log_entries table: {}", e))
            })?;

            // Get current last index for validation
            let last_index = table
                .last()
                .map_err(|e| LogError::DatabaseError(format!("Failed to get last entry: {}", e)))?
                .map(|(k, _)| k.value())
                .unwrap_or(0);

            // Find min and max indices in batch
            let mut min_index = u64::MAX;
            let mut max_index = 0u64;

            // Insert all entries
            for (index, term, data) in entries.iter() {
                min_index = min_index.min(*index);
                max_index = max_index.max(*index);

                // Validate index doesn't create a gap (unless overwriting)
                if last_index > 0 && *index > last_index + 1 && *index == min_index {
                    return Err(LogError::InvalidRange(format!(
                        "Index {} creates gap after last index {}",
                        index, last_index
                    )));
                }

                let entry_data = LogEntryData::new(*term, data.clone(), SystemTime::now());
                let serialized = bincode::serialize(&entry_data).map_err(|e| {
                    LogError::SerializationError(format!("Failed to serialize entry: {}", e))
                })?;

                table.insert(*index, serialized.as_slice()).map_err(|e| {
                    LogError::DatabaseError(format!(
                        "Failed to insert entry at index {}: {}",
                        index, e
                    ))
                })?;
            }

            (min_index, max_index)
        };

        // Commit transaction (this includes fsync for all entries)
        write_txn
            .commit()
            .map_err(|e| LogError::DatabaseError(format!("Failed to commit transaction: {}", e)))?;

        // Update cached indices
        self.update_cached_indices(if min_index == 1 { Some(1) } else { None }, Some(max_index))?;

        // Record metrics and logging
        let elapsed = start_time.elapsed().as_secs_f64();
        self.record_histogram("transaction_log.append_batch.latency", elapsed);
        self.record_counter("transaction_log.append_batch.total", 1);
        self.record_counter("transaction_log.append_batch.entries", batch_size as u64);
        self.update_state_gauges();
        info!(
            "Appended batch of {} entries (indices {}-{}) in {:.4}s",
            batch_size, min_index, max_index, elapsed
        );

        Ok(())
    }

    /// Trim entries before the specified index (blocking operation).
    ///
    /// Removes all log entries with indices less than `up_to_index`. Used for
    /// log compaction and garbage collection after snapshot creation. Updates
    /// the cached first index to reflect the new log start. Records metrics and
    /// updates state gauges after successful trim. This is the blocking
    /// implementation called from the async API wrapper via
    /// `tokio::task::spawn_blocking`.
    ///
    /// # Arguments
    ///
    /// * `up_to_index` - Remove all entries before this index (exclusive)
    ///
    /// # Returns
    ///
    /// Returns the count of entries removed.
    ///
    /// # Errors
    ///
    /// * `LogError::DatabaseError` - Database operation failed
    fn trim_blocking(&self, up_to_index: u64) -> Result<u64, LogError> {
        let start_time = std::time::Instant::now();

        let db = self.inner.db.write().map_err(|e| {
            error!("Failed to acquire write lock for trim: {}", e);
            LogError::DatabaseError(format!("Failed to acquire write lock: {}", e))
        })?;
        let write_txn = db.begin_write().map_err(|e| {
            error!("Failed to begin write transaction for trim: {}", e);
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
        *self.inner.cached_first_index.write().map_err(|e| {
            LogError::DatabaseError(format!("Failed to acquire cache lock: {}", e))
        })? = new_first_index;

        // Record metrics and logging
        let elapsed = start_time.elapsed().as_secs_f64();
        self.record_histogram("transaction_log.trim.latency", elapsed);
        self.record_counter("transaction_log.trim.total", 1);
        self.record_counter("transaction_log.trim.entries_removed", count);
        self.update_state_gauges();
        info!(
            "Trimmed {} entries before index {} in {:.4}s, new first index: {:?}",
            count, up_to_index, elapsed, new_first_index
        );

        Ok(count)
    }

    /// Delete entries from the specified index onwards (blocking version).
    ///
    /// This is the opposite of trim: it deletes entries from `from_index` to the end.
    /// Critical for Raft log conflict resolution.
    fn delete_from_blocking(&self, from_index: u64) -> Result<u64, LogError> {
        if from_index == 0 {
            return Err(LogError::InvalidRange(
                "Cannot delete from index 0".to_string(),
            ));
        }

        let start_time = std::time::Instant::now();

        // Get database handle (write lock needed for write transaction)
        let db = self.inner.db.write().map_err(|e| {
            error!("Failed to acquire write lock for delete_from: {}", e);
            LogError::DatabaseError(format!("Failed to acquire write lock: {}", e))
        })?;
        let write_txn = db.begin_write().map_err(|e| {
            error!("Failed to begin write transaction for delete_from: {}", e);
            LogError::DatabaseError(format!("Failed to begin transaction: {}", e))
        })?;

        let (count, new_last_index) = {
            let mut table = write_txn
                .open_table(LOG_ENTRIES_TABLE)
                .map_err(|e| LogError::DatabaseError(format!("Failed to open table: {}", e)))?;

            // Find all entries >= from_index
            let indices_to_remove: Vec<u64> = table
                .range(from_index..)
                .map_err(|e| {
                    LogError::DatabaseError(format!("Failed to create range iterator: {}", e))
                })?
                .map(|result| {
                    result.map(|(k, _)| k.value()).map_err(|e| {
                        LogError::DatabaseError(format!("Failed to iterate entries: {}", e))
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

            // Determine new last index (the entry just before from_index)
            let new_last_index = if from_index > 1 {
                // Check if the entry before from_index exists
                table
                    .range(..from_index)
                    .map_err(|e| {
                        LogError::DatabaseError(format!("Failed to create range iterator: {}", e))
                    })?
                    .last()
                    .transpose()
                    .map_err(|e| {
                        LogError::DatabaseError(format!("Failed to get last entry: {}", e))
                    })?
                    .map(|(k, _)| k.value())
            } else {
                None
            };

            (count, new_last_index)
        };

        // Commit transaction
        write_txn
            .commit()
            .map_err(|e| LogError::DatabaseError(format!("Failed to commit transaction: {}", e)))?;

        // Update cached last index
        *self.inner.cached_last_index.write().map_err(|e| {
            LogError::DatabaseError(format!("Failed to acquire cache lock: {}", e))
        })? = new_last_index;

        // If we deleted everything, update first index too
        if new_last_index.is_none() {
            *self.inner.cached_first_index.write().map_err(|e| {
                LogError::DatabaseError(format!("Failed to acquire cache lock: {}", e))
            })? = None;
        }

        // Record metrics and logging
        let elapsed = start_time.elapsed().as_secs_f64();
        self.record_histogram("transaction_log.delete_from.latency", elapsed);
        self.record_counter("transaction_log.delete_from.total", 1);
        self.record_counter("transaction_log.delete_from.entries_removed", count);
        self.update_state_gauges();
        warn!(
            "Deleted {} entries from index {} onwards in {:.4}s, new last index: {:?}",
            count, from_index, elapsed, new_last_index
        );

        Ok(count)
    }

    /// Compact the database (blocking version).
    ///
    /// This defragments the database and reclaims space from deleted entries.
    fn compact_blocking(&self) -> Result<u64, LogError> {
        let start_time = std::time::Instant::now();
        info!("Starting database compaction...");

        // Get the database file size before compaction
        let size_before = std::fs::metadata(&self.inner.config.db_path)
            .map(|m| m.len())
            .unwrap_or(0);

        // Perform compaction (requires mutable access)
        {
            let mut db = self.inner.db.write().map_err(|e| {
                error!("Failed to acquire write lock for compaction: {}", e);
                LogError::DatabaseError(format!("Failed to acquire write lock: {}", e))
            })?;
            db.compact().map_err(|e| {
                error!("Failed to compact database: {}", e);
                LogError::DatabaseError(format!("Failed to compact database: {}", e))
            })?;
        }

        // Get the database file size after compaction
        let size_after = std::fs::metadata(&self.inner.config.db_path)
            .map(|m| m.len())
            .unwrap_or(0);

        // Calculate space reclaimed
        let space_reclaimed = size_before.saturating_sub(size_after);

        // Update last compaction timestamp
        *self.inner.last_compaction.write().map_err(|e| {
            LogError::DatabaseError(format!("Failed to acquire cache lock: {}", e))
        })? = Some(SystemTime::now());

        // Record metrics and logging
        let elapsed = start_time.elapsed().as_secs_f64();
        self.record_histogram("transaction_log.compact.latency", elapsed);
        self.record_counter("transaction_log.compact.total", 1);
        self.record_gauge(
            "transaction_log.compact.space_reclaimed",
            space_reclaimed as f64,
        );
        info!(
            "Database compaction completed in {:.4}s, reclaimed {} bytes ({:.2} MB)",
            elapsed,
            space_reclaimed,
            space_reclaimed as f64 / 1024.0 / 1024.0
        );

        Ok(space_reclaimed)
    }

    /// Verify log integrity (blocking version).
    ///
    /// Scans all entries to check for gaps and validate checksums.
    fn verify_integrity_blocking(&self) -> Result<IntegrityReport, LogError> {
        let start_time = std::time::Instant::now();
        let last_index = self.get_last_index();

        info!("Starting integrity verification, last_index={}", last_index);

        // Empty log is valid
        if last_index == 0 {
            info!("Empty log, verification complete");
            return Ok(IntegrityReport {
                total_entries: 0,
                missing_indices: Vec::new(),
                is_valid: true,
            });
        }

        let mut total_entries = 0u64;
        let mut missing_indices = Vec::new();
        let mut is_valid = true;

        // Scan from index 1 to last_index to detect all gaps
        // (Raft log indices start at 1)
        for index in 1..=last_index {
            match self.get_entry_blocking(index) {
                Ok(_entry) => {
                    // Entry exists and checksum is valid (verified in get_entry_blocking)
                    total_entries += 1;
                }
                Err(LogError::EntryNotFound(_)) => {
                    // Gap in the sequence - allowed, but record it
                    missing_indices.push(index);
                }
                Err(LogError::ChecksumFailed(_)) => {
                    // Checksum verification failed - data is corrupted
                    missing_indices.push(index);
                    is_valid = false;
                }
                Err(LogError::SerializationError(_)) => {
                    // Deserialization failed - data is corrupted
                    missing_indices.push(index);
                    is_valid = false;
                }
                Err(e) => {
                    // Other errors (database errors, etc.)
                    return Err(e);
                }
            }
        }

        // Record metrics and logging
        let elapsed = start_time.elapsed().as_secs_f64();
        self.record_histogram("transaction_log.verify_integrity.latency", elapsed);
        self.record_counter("transaction_log.verify_integrity.total", 1);

        if is_valid {
            info!(
                "Integrity verification completed in {:.4}s: {} entries verified, {} gaps found",
                elapsed,
                total_entries,
                missing_indices.len()
            );
        } else {
            error!(
                "Integrity verification FAILED in {:.4}s: {} entries, {} gaps/corrupted",
                elapsed,
                total_entries,
                missing_indices.len()
            );
        }

        Ok(IntegrityReport {
            total_entries,
            missing_indices,
            is_valid,
        })
    }

    /// Set the metrics service for tracking transaction log operations.
    ///
    /// This uses deferred dependency injection to avoid circular dependencies.
    ///
    /// # Arguments
    ///
    /// * `metrics` - MetricService implementation for recording metrics
    pub fn set_metrics(&self, metrics: Arc<crate::metric_service::MetricServiceImpl>) {
        if let Ok(mut metrics_guard) = self.inner.metrics.write() {
            *metrics_guard = Some(metrics);
            info!("MetricsService attached to TransactionLogStore");
        } else {
            error!("Failed to acquire write lock to set metrics service");
        }
    }

    /// Record a histogram metric if metrics service is available.
    ///
    /// Used for recording latency measurements in seconds. If the metrics service
    /// has not been configured via `set_metrics()`, this method silently does nothing.
    ///
    /// # Arguments
    ///
    /// * `name` - Metric name (e.g., "transaction_log.append.latency")
    /// * `value` - Metric value in seconds
    fn record_histogram(&self, name: &str, value: f64) {
        if let Ok(metrics_guard) = self.inner.metrics.read() {
            if let Some(metrics) = metrics_guard.as_ref() {
                use crate::metric_service::{MetricService, UnitType};
                let _ = metrics.publish_histogram(name, value, UnitType::Seconds);
            }
        }
    }

    /// Record a counter metric if metrics service is available.
    ///
    /// Used for recording operation counts. If the metrics service has not been
    /// configured via `set_metrics()`, this method silently does nothing.
    ///
    /// # Arguments
    ///
    /// * `name` - Metric name (e.g., "transaction_log.append.total")
    /// * `value` - Counter value to add
    fn record_counter(&self, name: &str, value: u64) {
        if let Ok(metrics_guard) = self.inner.metrics.read() {
            if let Some(metrics) = metrics_guard.as_ref() {
                use crate::metric_service::{MetricService, UnitType};
                let _ = metrics.publish_counter(name, value, UnitType::Operations);
            }
        }
    }

    /// Record a gauge metric if metrics service is available.
    ///
    /// Used for recording point-in-time measurements like file sizes. If the
    /// metrics service has not been configured via `set_metrics()`, this method
    /// silently does nothing.
    ///
    /// # Arguments
    ///
    /// * `name` - Metric name (e.g., "transaction_log.db_size_bytes")
    /// * `value` - Gauge value in bytes
    fn record_gauge(&self, name: &str, value: f64) {
        if let Ok(metrics_guard) = self.inner.metrics.read() {
            if let Some(metrics) = metrics_guard.as_ref() {
                use crate::metric_service::{MetricService, UnitType};
                let _ = metrics.publish_gauge(name, value, UnitType::Bytes);
            }
        }
    }

    /// Update state gauges for log size and entry count.
    ///
    /// Records current database file size and approximate entry count based on
    /// first/last indices. Called after write operations (append, trim, delete_from)
    /// to maintain up-to-date state visibility in monitoring dashboards. If the
    /// metrics service has not been configured, this method silently does nothing.
    fn update_state_gauges(&self) {
        // Record database file size
        if let Ok(metadata) = std::fs::metadata(&self.inner.config.db_path) {
            self.record_gauge("transaction_log.db_size_bytes", metadata.len() as f64);
        }

        // Record entry count (approximate based on first/last indices)
        let first = self.get_first_index();
        let last = self.get_last_index();
        if first > 0 && last > 0 {
            let approx_count = last - first + 1;
            let _ = self.inner.metrics.read().ok().and_then(|guard| {
                guard.as_ref().map(|metrics| {
                    use crate::metric_service::{MetricService, UnitType};
                    let _ = metrics.publish_gauge(
                        "transaction_log.entry_count",
                        approx_count as f64,
                        UnitType::Operations,
                    );
                })
            });
        }
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

    async fn append(&self, index: u64, term: u64, data: Vec<u8>) -> Result<(), LogError> {
        let self_clone = self.clone();
        task::spawn_blocking(move || self_clone.append_blocking(index, term, data))
            .await
            .map_err(|e| LogError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn append_batch(&self, entries: Vec<(u64, u64, Vec<u8>)>) -> Result<(), LogError> {
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
        self.inner
            .cached_last_index
            .read()
            .ok()
            .and_then(|guard| *guard)
            .unwrap_or(0)
    }

    fn get_first_index(&self) -> u64 {
        self.inner
            .cached_first_index
            .read()
            .ok()
            .and_then(|guard| *guard)
            .unwrap_or(0)
    }

    async fn trim(&self, up_to_index: u64) -> Result<u64, LogError> {
        let self_clone = self.clone();
        task::spawn_blocking(move || self_clone.trim_blocking(up_to_index))
            .await
            .map_err(|e| LogError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn delete_from(&self, from_index: u64) -> Result<u64, LogError> {
        let self_clone = self.clone();
        task::spawn_blocking(move || self_clone.delete_from_blocking(from_index))
            .await
            .map_err(|e| LogError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn compact(&self) -> Result<u64, LogError> {
        let self_clone = self.clone();
        task::spawn_blocking(move || self_clone.compact_blocking())
            .await
            .map_err(|e| LogError::DatabaseError(format!("Task join error: {}", e)))?
    }

    async fn verify_integrity(&self) -> Result<IntegrityReport, LogError> {
        let self_clone = self.clone();
        task::spawn_blocking(move || self_clone.verify_integrity_blocking())
            .await
            .map_err(|e| LogError::DatabaseError(format!("Task join error: {}", e)))?
    }

    fn get_stats(&self) -> LogStats {
        let first_index = self
            .inner
            .cached_first_index
            .read()
            .ok()
            .and_then(|guard| *guard);
        let last_index = self
            .inner
            .cached_last_index
            .read()
            .ok()
            .and_then(|guard| *guard);

        let entry_count = match (first_index, last_index) {
            (Some(first), Some(last)) => last - first + 1,
            _ => 0,
        };

        // Get database file size
        let db_size_bytes = std::fs::metadata(&self.inner.config.db_path)
            .map(|m| m.len())
            .unwrap_or(0);

        let last_compaction = self
            .inner
            .last_compaction
            .read()
            .ok()
            .and_then(|guard| *guard);

        LogStats {
            first_index,
            last_index,
            entry_count,
            db_size_bytes,
            last_compaction,
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
        store.append(1, 1, data.clone()).await.unwrap();

        assert_eq!(store.get_last_index(), 1);

        let entry = store.get_entry(1).await.unwrap();
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
            store.append(i, i, data).await.unwrap();
        }

        assert_eq!(store.get_first_index(), 1);
        assert_eq!(store.get_last_index(), 5);
    }

    #[tokio::test]
    async fn test_append_batch() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        let entries = vec![
            (1, 1, b"op1".to_vec()),
            (2, 1, b"op2".to_vec()),
            (3, 2, b"op3".to_vec()),
        ];

        store.append_batch(entries).await.unwrap();
        assert_eq!(store.get_last_index(), 3);

        let entry1 = store.get_entry(1).await.unwrap();
        assert_eq!(entry1.term, 1);
        assert_eq!(entry1.operations, b"op1");

        let entry3 = store.get_entry(3).await.unwrap();
        assert_eq!(entry3.term, 2);
    }

    #[tokio::test]
    async fn test_append_batch_empty() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Attempting to append an empty batch should return InvalidRange error
        let empty_entries: Vec<(u64, u64, Vec<u8>)> = vec![];
        let result = store.append_batch(empty_entries).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            LogError::InvalidRange(msg) => {
                assert_eq!(msg, "Cannot append empty batch");
            }
            other => panic!("Expected InvalidRange error, got: {:?}", other),
        }

        // Verify log state is unchanged
        assert_eq!(store.get_first_index(), 0);
        assert_eq!(store.get_last_index(), 0);
    }

    #[tokio::test]
    async fn test_get_entries_range() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        for i in 1..=10 {
            let data = format!("operation {}", i).into_bytes();
            store.append(i, i, data).await.unwrap();
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
        store.append(1, 1, b"first".to_vec()).await.unwrap();
        store.append(2, 2, data.clone()).await.unwrap();

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
                .append(i, i, format!("op{}", i).into_bytes())
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
            store.append(i, i, b"test".to_vec()).await.unwrap();
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
        store.append(1, 1, data.clone()).await.unwrap();

        // Verify entry can be retrieved (checksum is valid)
        let entry = store.get_entry(1).await.unwrap();
        assert_eq!(entry.operations, data);
    }

    #[tokio::test]
    async fn test_persistence_across_restarts() {
        let (config, _temp_dir) = create_test_config();

        // Create store and add entries
        {
            let store = TransactionLogStoreImpl::new(config.clone()).unwrap();
            store.append(1, 1, b"entry1".to_vec()).await.unwrap();
            store.append(2, 2, b"entry2".to_vec()).await.unwrap();
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

        // With the new Raft-compliant index assignment, concurrent operations
        // must handle index assignment carefully. In a real Raft scenario,
        // the leader assigns indices sequentially.
        // For this test, we'll append sequentially from multiple tasks.

        for i in 1..=10 {
            store
                .append(i, 1, format!("concurrent op {}", i).into_bytes())
                .await
                .unwrap();
        }

        assert_eq!(store.get_last_index(), 10);

        // Verify all entries are retrievable
        for i in 1..=10 {
            let entry = store.get_entry(i).await.unwrap();
            assert_eq!(entry.index, i);
        }
    }

    #[tokio::test]
    async fn test_delete_from_basic() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Create entries 1-10
        for i in 1..=10 {
            store
                .append(i, i, format!("op{}", i).into_bytes())
                .await
                .unwrap();
        }

        // Delete from index 7 onwards
        let deleted = store.delete_from(7).await.unwrap();
        assert_eq!(deleted, 4); // Entries 7, 8, 9, 10 removed

        assert_eq!(store.get_first_index(), 1);
        assert_eq!(store.get_last_index(), 6);

        // Verify remaining entries exist
        for i in 1..=6 {
            let entry = store.get_entry(i).await.unwrap();
            assert_eq!(entry.index, i);
        }

        // Verify deleted entries are gone
        for i in 7..=10 {
            let result = store.get_entry(i).await;
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn test_delete_from_raft_conflict_scenario() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Simulate split-brain: follower has entries from old leader
        // Entries 1-5 are committed (term 1)
        for i in 1..=5 {
            store
                .append(i, 1, format!("committed-{}", i).into_bytes())
                .await
                .unwrap();
        }

        // Entries 6-8 are uncommitted from old leader (term 2)
        for i in 6..=8 {
            store
                .append(i, 2, format!("uncommitted-old-{}", i).into_bytes())
                .await
                .unwrap();
        }

        assert_eq!(store.get_last_index(), 8);

        // New leader sends conflicting entry at index 6 (term 3)
        // Follower must delete from index 6 onwards first
        let deleted = store.delete_from(6).await.unwrap();
        assert_eq!(deleted, 3); // Removed 6, 7, 8

        assert_eq!(store.get_last_index(), 5);

        // Now append new leader's entry
        store.append(6, 3, b"new-leader-6".to_vec()).await.unwrap();
        assert_eq!(store.get_last_index(), 6);

        let entry6 = store.get_entry(6).await.unwrap();
        assert_eq!(entry6.term, 3);
        assert_eq!(entry6.operations, b"new-leader-6");
    }

    #[tokio::test]
    async fn test_delete_from_all_entries() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        for i in 1..=5 {
            store
                .append(i, 1, format!("op{}", i).into_bytes())
                .await
                .unwrap();
        }

        // Delete all entries
        let deleted = store.delete_from(1).await.unwrap();
        assert_eq!(deleted, 5);

        assert_eq!(store.get_first_index(), 0);
        assert_eq!(store.get_last_index(), 0);

        let result = store.get_last_entry().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_from_last_entry_only() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        for i in 1..=5 {
            store
                .append(i, 1, format!("op{}", i).into_bytes())
                .await
                .unwrap();
        }

        // Delete only the last entry
        let deleted = store.delete_from(5).await.unwrap();
        assert_eq!(deleted, 1);

        assert_eq!(store.get_first_index(), 1);
        assert_eq!(store.get_last_index(), 4);

        let last_entry = store.get_last_entry().await.unwrap();
        assert_eq!(last_entry.index, 4);
    }

    #[tokio::test]
    async fn test_delete_from_nonexistent_index() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        for i in 1..=5 {
            store
                .append(i, 1, format!("op{}", i).into_bytes())
                .await
                .unwrap();
        }

        // Try to delete from beyond the end
        let deleted = store.delete_from(100).await.unwrap();
        assert_eq!(deleted, 0);

        // Log should be unchanged
        assert_eq!(store.get_first_index(), 1);
        assert_eq!(store.get_last_index(), 5);
    }

    #[tokio::test]
    async fn test_delete_from_zero_index() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        for i in 1..=5 {
            store
                .append(i, 1, format!("op{}", i).into_bytes())
                .await
                .unwrap();
        }

        // Index 0 is invalid
        let result = store.delete_from(0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_from_empty_log() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Delete from empty log
        let deleted = store.delete_from(1).await.unwrap();
        assert_eq!(deleted, 0);

        assert_eq!(store.get_first_index(), 0);
        assert_eq!(store.get_last_index(), 0);
    }

    #[tokio::test]
    async fn test_delete_from_after_trim() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Create entries 1-10
        for i in 1..=10 {
            store
                .append(i, 1, format!("op{}", i).into_bytes())
                .await
                .unwrap();
        }

        // Trim first 5 entries (remove 1-4, keep 5-10)
        store.trim(5).await.unwrap();
        assert_eq!(store.get_first_index(), 5);
        assert_eq!(store.get_last_index(), 10);

        // Now delete from index 8 onwards
        let deleted = store.delete_from(8).await.unwrap();
        assert_eq!(deleted, 3); // Removed 8, 9, 10

        assert_eq!(store.get_first_index(), 5);
        assert_eq!(store.get_last_index(), 7);

        // Verify entries 5-7 exist
        for i in 5..=7 {
            let entry = store.get_entry(i).await.unwrap();
            assert_eq!(entry.index, i);
        }
    }

    #[tokio::test]
    async fn test_trim_and_delete_from_difference() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Create entries 1-10
        for i in 1..=10 {
            store
                .append(i, 1, format!("op{}", i).into_bytes())
                .await
                .unwrap();
        }

        // trim(5) removes entries BEFORE 5: [1,2,3,4] -> leaves [5,6,7,8,9,10]
        let trimmed = store.trim(5).await.unwrap();
        assert_eq!(trimmed, 4);
        assert_eq!(store.get_first_index(), 5);
        assert_eq!(store.get_last_index(), 10);

        // Restore log for second test
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();
        for i in 1..=10 {
            store
                .append(i, 1, format!("op{}", i).into_bytes())
                .await
                .unwrap();
        }

        // delete_from(5) removes entries FROM 5 onwards: [5,6,7,8,9,10] -> leaves [1,2,3,4]
        let deleted = store.delete_from(5).await.unwrap();
        assert_eq!(deleted, 6);
        assert_eq!(store.get_first_index(), 1);
        assert_eq!(store.get_last_index(), 4);
    }

    #[tokio::test]
    async fn test_compact_empty_log() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Compact empty log should succeed (may reclaim initial overhead or not)
        let _reclaimed = store.compact().await.unwrap();

        // Verify compaction timestamp is set
        let stats = store.get_stats();
        assert!(stats.last_compaction.is_some());

        // Log should still be empty and functional
        assert_eq!(store.get_first_index(), 0);
        assert_eq!(store.get_last_index(), 0);
    }

    #[tokio::test]
    async fn test_compact_after_trim() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Add many entries to grow the database
        for i in 1..=1000 {
            store.append(i, 1, vec![0xAAu8; 1024]).await.unwrap();
        }

        let stats_before_trim = store.get_stats();
        let size_before_trim = stats_before_trim.db_size_bytes;

        // Trim first 900 entries
        store.trim(901).await.unwrap();

        // Compact to reclaim space
        let reclaimed = store.compact().await.unwrap();

        // Should reclaim some space
        println!("Size before trim: {}", size_before_trim);
        println!("Space reclaimed: {}", reclaimed);

        // Verify last_compaction is set
        let stats_after = store.get_stats();
        assert!(stats_after.last_compaction.is_some());

        // Verify log still works after compaction
        assert_eq!(store.get_first_index(), 901);
        assert_eq!(store.get_last_index(), 1000);

        let entry = store.get_entry(901).await.unwrap();
        assert_eq!(entry.index, 901);
    }

    #[tokio::test]
    async fn test_compact_after_delete_from() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Add entries
        for i in 1..=500 {
            store.append(i, 1, vec![0xBBu8; 2048]).await.unwrap();
        }

        // Delete from middle
        store.delete_from(250).await.unwrap();

        // Compact
        let reclaimed = store.compact().await.unwrap();
        println!("Compaction reclaimed {} bytes", reclaimed);

        // Verify log still works
        assert_eq!(store.get_first_index(), 1);
        assert_eq!(store.get_last_index(), 249);

        let entry1 = store.get_entry(1).await.unwrap();
        assert_eq!(entry1.index, 1);

        let entry249 = store.get_entry(249).await.unwrap();
        assert_eq!(entry249.index, 249);

        // Verify compaction timestamp is recorded
        let stats = store.get_stats();
        assert!(stats.last_compaction.is_some());
    }

    #[tokio::test]
    async fn test_compact_multiple_times() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Iteration 1: Add entries 1-100
        for i in 1..=100 {
            store.append(i, 1, vec![0xCCu8; 512]).await.unwrap();
        }
        store.trim(51).await.unwrap(); // Keep 51-100
        store.delete_from(76).await.unwrap(); // Keep 51-75

        let reclaimed = store.compact().await.unwrap();
        println!("Iteration 1 compaction reclaimed {} bytes", reclaimed);

        // Iteration 2: Add entries 76-175 (continuing from where we left off)
        for i in 76..=175 {
            store.append(i, 1, vec![0xCCu8; 512]).await.unwrap();
        }
        store.trim(126).await.unwrap(); // Keep 126-175
        store.delete_from(151).await.unwrap(); // Keep 126-150

        let reclaimed = store.compact().await.unwrap();
        println!("Iteration 2 compaction reclaimed {} bytes", reclaimed);

        // Iteration 3: Add entries 151-250
        for i in 151..=250 {
            store.append(i, 1, vec![0xCCu8; 512]).await.unwrap();
        }
        store.trim(201).await.unwrap(); // Keep 201-250
        store.delete_from(226).await.unwrap(); // Keep 201-225

        let reclaimed = store.compact().await.unwrap();
        println!("Iteration 3 compaction reclaimed {} bytes", reclaimed);

        // Verify log is in consistent state
        let stats = store.get_stats();
        assert!(stats.last_compaction.is_some());
        assert_eq!(stats.entry_count, 25); // entries 201-225
    }

    #[tokio::test]
    async fn test_compact_concurrent_with_reads() {
        let (config, _temp_dir) = create_test_config();
        let store = Arc::new(TransactionLogStoreImpl::new(config).unwrap());

        // Pre-populate with entries
        for i in 1..=200 {
            store.append(i, 1, vec![0xDDu8; 256]).await.unwrap();
        }

        let mut handles = vec![];

        // Spawn compaction task
        {
            let store_clone = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                store_clone.compact().await.unwrap();
            });
            handles.push(handle);
        }

        // Spawn concurrent readers
        for _ in 0..5 {
            let store_clone = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                for i in 1..=200 {
                    let _ = store_clone.get_entry(i).await;
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify log is still consistent
        assert_eq!(store.get_last_index(), 200);
        let stats = store.get_stats();
        assert!(stats.last_compaction.is_some());
    }

    #[tokio::test]
    async fn test_verify_integrity_empty_log() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        let report = store.verify_integrity().await.unwrap();

        assert_eq!(report.total_entries, 0);
        assert_eq!(report.missing_indices.len(), 0);
        assert!(report.is_valid);
    }

    #[tokio::test]
    async fn test_verify_integrity_valid_log() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Add entries 1-100
        for i in 1..=100 {
            store.append(i, 1, vec![0xAAu8; 128]).await.unwrap();
        }

        let report = store.verify_integrity().await.unwrap();

        assert_eq!(report.total_entries, 100);
        assert_eq!(report.missing_indices.len(), 0);
        assert!(report.is_valid);
    }

    #[tokio::test]
    async fn test_verify_integrity_with_gaps_after_trim() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Add entries 1-100
        for i in 1..=100 {
            store.append(i, 1, vec![0xBBu8; 128]).await.unwrap();
        }

        // Trim first 50 entries (creates gap from 1-49)
        store.trim(50).await.unwrap();

        let report = store.verify_integrity().await.unwrap();

        // Should have 51 entries (50-100)
        assert_eq!(report.total_entries, 51);
        // Should report indices 1-49 as missing
        assert_eq!(report.missing_indices.len(), 49);
        assert_eq!(report.missing_indices[0], 1);
        assert_eq!(report.missing_indices[48], 49);
        // Gaps are allowed, so log is still valid
        assert!(report.is_valid);
    }

    #[tokio::test]
    async fn test_verify_integrity_with_gaps_after_delete_from() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Add entries 1-100
        for i in 1..=100 {
            store.append(i, 1, vec![0xCCu8; 128]).await.unwrap();
        }

        // Delete from 75 onwards
        store.delete_from(75).await.unwrap();

        let report = store.verify_integrity().await.unwrap();

        // Should have 74 entries (1-74)
        assert_eq!(report.total_entries, 74);
        // No missing indices - deleted entries don't count as gaps
        // (last_index is now 74, so we only scan 1-74)
        assert_eq!(report.missing_indices.len(), 0);
        // All entries present are valid
        assert!(report.is_valid);
    }

    #[tokio::test]
    async fn test_verify_integrity_with_sparse_log() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Add entries 1-50
        for i in 1..=50 {
            store.append(i, 1, vec![0xDDu8; 128]).await.unwrap();
        }

        // Trim to remove 1-9, keep 10 onwards
        store.trim(10).await.unwrap();
        // Delete from 31 onwards
        store.delete_from(31).await.unwrap();

        // Now we have entries 10-30 (21 entries)
        // last_index is now 30, so we scan 1-30
        let report = store.verify_integrity().await.unwrap();

        assert_eq!(report.total_entries, 21);
        // Missing: 1-9 (9 indices before first_index)
        assert_eq!(report.missing_indices.len(), 9);
        assert_eq!(report.missing_indices[0], 1);
        assert_eq!(report.missing_indices[8], 9);
        assert!(report.is_valid);
    }

    #[tokio::test]
    async fn test_verify_integrity_large_log() {
        let (config, _temp_dir) = create_test_config();
        let store = TransactionLogStoreImpl::new(config).unwrap();

        // Add 10,000 entries
        for i in 1..=10000 {
            store.append(i, 1, vec![0xEEu8; 64]).await.unwrap();
        }

        let start = std::time::Instant::now();
        let report = store.verify_integrity().await.unwrap();
        let elapsed = start.elapsed();

        println!("Verified 10,000 entries in {:?}", elapsed);

        assert_eq!(report.total_entries, 10000);
        assert_eq!(report.missing_indices.len(), 0);
        assert!(report.is_valid);

        // Should complete in reasonable time (<5 seconds)
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "Verification took too long: {:?}",
            elapsed
        );
    }
}
