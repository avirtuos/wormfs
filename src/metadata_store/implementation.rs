//! Concrete implementation of MetadataStore using tokio-rusqlite for async operations.

use super::{
    cache::{CacheConfig, MetadataCache},
    ChunkId, ChunkRecord, ChunkStatus, ClientId, Config, DiskId, Error, FileId, FileMetadata,
    FileRecord, LockRecord, LockType, MetadataStore, NodeId, StripeId, StripeRecord,
};
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_rusqlite::Connection;
use tracing::{error, info};

// Import MetricService for instrumentation
use crate::metric_service::MetricService;

/// Maximum safe inode value (SQLite INTEGER is signed i64).
/// While the API uses u64 for consistency with filesystem conventions,
/// SQLite's INTEGER type can only safely store values up to 2^63-1.
/// This is still 9,223,372,036,854,775,807 inodes - effectively unlimited.
const MAX_SAFE_INODE: u64 = i64::MAX as u64;

/// Maximum cache size in MB to prevent i32 overflow when converting to KB.
const MAX_CACHE_SIZE_MB: usize = 2047;

/// Convert SystemTime to Unix timestamp (seconds since epoch).
fn system_time_to_unix(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Convert Unix timestamp to SystemTime.
fn unix_to_system_time(unix: i64) -> SystemTime {
    UNIX_EPOCH + std::time::Duration::from_secs(unix as u64)
}

/// Inner state for MetadataStore implementation.
///
/// With tokio-rusqlite, the Connection is cloneable and thread-safe.
/// Each clone shares the same underlying database connection managed
/// by a background thread.
struct MetadataStoreInner {
    /// Async SQLite connection (cloneable, thread-safe)
    conn: Connection,

    /// Configuration
    #[allow(dead_code)]
    config: Config,

    /// Optional metrics service for instrumentation (sync lock for fire-and-forget publishing)
    metrics: std::sync::RwLock<Option<Arc<crate::metric_service::MetricServiceImpl>>>,

    /// Metadata cache for stripe records and chunk lists
    cache: MetadataCache,
}

/// Concrete implementation of MetadataStore.
///
/// This implementation uses tokio-rusqlite for true async SQLite operations.
/// The Connection handle is cheap to clone and all operations are executed
/// in a dedicated background thread, allowing non-blocking async access.
#[derive(Clone)]
pub struct MetadataStoreImpl {
    inner: Arc<MetadataStoreInner>,
}

impl MetadataStoreImpl {
    /// Create a new MetadataStore instance.
    ///
    /// This constructor is `pub(super)` so it can only be called by the factory.
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
    /// Returns an error if database initialization fails.
    pub(super) async fn new(config: Config) -> Result<Self, Error> {
        // Validate configuration
        if config.cache_size_mb > MAX_CACHE_SIZE_MB {
            return Err(Error::ConfigInvalid(format!(
                "cache_size_mb ({}) exceeds maximum of {} MB to prevent integer overflow",
                config.cache_size_mb, MAX_CACHE_SIZE_MB
            )));
        }

        // Create database directory if it doesn't exist
        if let Some(parent) = config.database_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                Error::ConfigError(format!("Failed to create database directory: {}", e))
            })?;
        }

        // Open async connection
        let conn = Connection::open(&config.database_path)
            .await
            .map_err(|e| Error::ConnectionError(format!("Failed to open connection: {}", e)))?;

        // Configure connection with optimal settings
        Self::configure_connection(&conn, &config).await?;

        // Initialize metadata cache
        let cache_config = CacheConfig {
            stripe_cache_size_mb: config.stripe_cache_size_mb,
            stripe_cache_ttl_secs: config.stripe_cache_ttl_secs,
            stripe_cache_tti_secs: config.stripe_cache_tti_secs,
            chunk_cache_size_mb: config.chunk_cache_size_mb,
            chunk_cache_ttl_secs: config.chunk_cache_ttl_secs,
            chunk_cache_tti_secs: config.chunk_cache_tti_secs,
        };
        let cache = MetadataCache::new(&cache_config);

        let inner = MetadataStoreInner {
            conn,
            config,
            metrics: std::sync::RwLock::new(None),
            cache,
        };

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Set the metrics service for instrumentation.
    ///
    /// This method allows dependency injection of the metrics service after
    /// MetadataStore construction, avoiding circular dependencies during initialization.
    pub fn set_metrics(&self, metrics: Arc<crate::metric_service::MetricServiceImpl>) {
        *self.inner.metrics.write().unwrap() = Some(metrics);
    }

    /// Helper function to publish operation metrics.
    ///
    /// Publishes both aggregate metrics (read/write totals and latencies) and
    /// operation-specific metrics for critical operations.
    ///
    /// # Arguments
    ///
    /// * `operation` - Name of the operation (e.g., "create_file", "get_file_by_path")
    /// * `operation_type` - Either "read" or "write" for aggregate metrics
    /// * `start` - Start time of the operation (from tokio::time::Instant::now())
    /// * `is_error` - Whether the operation resulted in an error
    fn publish_metrics(
        &self,
        operation: &str,
        operation_type: &str, // "read" or "write"
        start: tokio::time::Instant,
        is_error: bool,
    ) {
        if let Some(ref metrics) = *self.inner.metrics.read().unwrap() {
            let elapsed = start.elapsed().as_secs_f64();

            // Publish aggregate read/write total counter
            let _ = metrics.publish_counter(
                &format!("metadata_store.{}.total", operation_type),
                1,
                crate::metric_service::UnitType::Operations,
            );

            // Publish aggregate read/write latency histogram
            let _ = metrics.publish_histogram(
                &format!("metadata_store.{}.latency", operation_type),
                elapsed,
                crate::metric_service::UnitType::Seconds,
            );

            // Publish error counter if operation failed
            if is_error {
                let mut labels = std::collections::HashMap::new();
                labels.insert("operation".to_string(), operation.to_string());
                labels.insert("type".to_string(), operation_type.to_string());
                let _ = metrics.publish_labeled(
                    "metadata_store.errors.total",
                    crate::metric_service::MetricValue::Counter(1),
                    crate::metric_service::MetricType::Counter,
                    crate::metric_service::UnitType::Operations,
                    labels,
                );
            }

            // Publish per-operation metrics (for all operations)
            let _ = metrics.publish_counter(
                &format!("metadata_store.{}.total", operation),
                1,
                crate::metric_service::UnitType::Operations,
            );

            let _ = metrics.publish_histogram(
                &format!("metadata_store.{}.latency", operation),
                elapsed,
                crate::metric_service::UnitType::Seconds,
            );
        }
    }

    /// Configure a SQLite connection with optimal settings.
    async fn configure_connection(conn: &Connection, config: &Config) -> Result<(), Error> {
        let enable_wal = config.enable_wal;
        let synchronous = config.synchronous;
        let enable_foreign_keys = config.enable_foreign_keys;
        let cache_size_mb = config.cache_size_mb;

        conn.call(move |conn| {
            // Build PRAGMA statements and execute them as a batch
            let mut pragmas = Vec::new();

            // Enable WAL mode for concurrent reads
            if enable_wal {
                pragmas.push("PRAGMA journal_mode=WAL;".to_string());
            }

            // Set synchronous mode
            let sync_mode = match synchronous {
                super::types::SynchronousMode::Off => "OFF",
                super::types::SynchronousMode::Normal => "NORMAL",
                super::types::SynchronousMode::Full => "FULL",
            };
            pragmas.push(format!("PRAGMA synchronous={};", sync_mode));

            // Configure foreign keys
            if enable_foreign_keys {
                pragmas.push("PRAGMA foreign_keys=ON;".to_string());
            } else {
                pragmas.push("PRAGMA foreign_keys=OFF;".to_string());
            }

            // Set cache size (negative value means KB)
            // Safe to cast after validation in new(): cache_size_mb <= MAX_CACHE_SIZE_MB
            let cache_size = -(cache_size_mb as i32 * 1024);
            pragmas.push(format!("PRAGMA cache_size={};", cache_size));

            // Execute all PRAGMAs as a batch
            conn.execute_batch(&pragmas.join(" "))?;

            Ok(())
        })
        .await
        .map_err(|e| Error::ConfigError(format!("Failed to configure connection: {}", e)))
    }

    /// Run migrations to initialize or update the schema.
    async fn run_migrations(&self) -> Result<(), Error> {
        let migrations = vec![
            include_str!("migrations/001_initial_schema.sql").to_string(),
            include_str!("migrations/002_indexes.sql").to_string(),
            include_str!("migrations/003_inode_management.sql").to_string(),
            include_str!("migrations/004_uuid_migration.sql").to_string(),
            include_str!("migrations/005_root_directory.sql").to_string(),
        ];

        self.inner
            .conn
            .call(move |conn| {
                let tx = conn.transaction()?;

                for (idx, migration) in migrations.iter().enumerate() {
                    tx.execute_batch(migration).map_err(|e| {
                        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Migration {} failed: {}", idx + 1, e),
                        )))
                    })?;
                }

                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(|e| Error::SchemaInitFailed(format!("{}", e)))
    }

    /// Test-only helper to insert nodes and disks directly.
    #[cfg(test)]
    pub async fn test_insert_node_and_disk(
        &self,
        node_id: NodeId,
        disk_id: DiskId,
    ) -> Result<(), Error> {
        let node_id_val = node_id.as_u64() as i64;
        let disk_id_val = disk_id.as_u64() as i64;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        self.inner.conn.call(move |conn| {
            // Insert node
            conn.execute(
                "INSERT OR IGNORE INTO nodes (node_id, address, status, last_seen, created_at)
                 VALUES (?1, ?2, 0, ?3, ?3)",
                rusqlite::params![node_id_val, "127.0.0.1:8080", now],
            )?;

            // Insert disk
            conn.execute(
                "INSERT OR IGNORE INTO disks (disk_id, node_id, path, total_space, free_space, status, created_at, updated_at)
                 VALUES (?1, ?2, ?3, 1099511627776, 1099511627776, 0, ?4, ?4)",
                rusqlite::params![disk_id_val, node_id_val, "/tmp/test_disk", now],
            )?;

            Ok(())
        })
        .await
        .map_err(|e| Error::QueryError(format!("Failed to insert test node/disk: {}", e)))
    }

    /// Diagnose foreign key constraint failures by checking which FK references are missing.
    ///
    /// This method is called synchronously when allocate_chunks fails with a foreign key constraint error.
    /// It runs diagnostic queries using the same database connection to identify which foreign key constraint failed.
    fn diagnose_fk_constraint_failure_sync(
        conn: &rusqlite::Connection,
        stripe_id: StripeId,
        chunks: &[ChunkRecord],
    ) {
        use tracing::{error, warn};

        error!("=== FOREIGN KEY CONSTRAINT DIAGNOSTIC ===");
        error!("Failed to allocate chunks for stripe_id={:?}", stripe_id);

        // Check 1: Does the stripe exist in the stripes table?
        let stripe_check: Result<(StripeId, FileId, i64, i64, i64), rusqlite::Error> = conn.query_row(
            "SELECT stripe_id, file_id, stripe_index, offset, size FROM stripes WHERE stripe_id = ?1",
            params![stripe_id],
            |row| Ok((
                row.get::<_, StripeId>(0)?,
                row.get::<_, FileId>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            )),
        );

        match stripe_check {
            Ok((sid, fid, idx, offset, size)) => {
                error!(
                    "✓ STRIPE EXISTS: stripe_id={:?}, file_id={:?}, stripe_index={}, offset={}, size={}",
                    sid, fid, idx, offset, size
                );
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                error!(
                    "✗ STRIPE MISSING: stripe_id={:?} NOT FOUND in stripes table",
                    stripe_id
                );
            }
            Err(e) => {
                error!("✗ STRIPE CHECK ERROR: {}", e);
            }
        }

        // Check 2: For each chunk, check if node_id and disk_id exist
        for (idx, chunk) in chunks.iter().enumerate() {
            error!("--- Checking chunk {} ---", idx);
            error!(
                "  chunk_id={:?}, node_id={}, disk_id={}, chunk_index={}",
                chunk.chunk_id,
                chunk.node_id.as_u64(),
                chunk.disk_id.as_u64(),
                chunk.chunk_index
            );

            // Check if node exists
            let node_id_val = chunk.node_id.as_u64() as i64;
            let node_check: Result<(i64, String), rusqlite::Error> = conn.query_row(
                "SELECT node_id, address FROM nodes WHERE node_id = ?1",
                params![node_id_val],
                |row| Ok((row.get(0)?, row.get(1)?)),
            );

            match node_check {
                Ok((nid, addr)) => {
                    error!("  ✓ NODE EXISTS: node_id={}, address={}", nid, addr);
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    error!(
                        "  ✗ NODE MISSING: node_id={} NOT FOUND in nodes table",
                        chunk.node_id.as_u64()
                    );
                }
                Err(e) => {
                    error!("  ✗ NODE CHECK ERROR: {}", e);
                }
            }

            // Check if disk exists
            let disk_id_val = chunk.disk_id.as_u64() as i64;
            let disk_check: Result<(i64, i64, String), rusqlite::Error> = conn.query_row(
                "SELECT disk_id, node_id, path FROM disks WHERE disk_id = ?1",
                params![disk_id_val],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            );

            match disk_check {
                Ok((did, nid, path)) => {
                    error!(
                        "  ✓ DISK EXISTS: disk_id={}, node_id={}, path={}",
                        did, nid, path
                    );
                    if nid != node_id_val {
                        warn!(
                            "  ⚠ DISK NODE MISMATCH: disk has node_id={}, but chunk expects node_id={}",
                            nid, node_id_val
                        );
                    }
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => {
                    error!(
                        "  ✗ DISK MISSING: disk_id={} NOT FOUND in disks table",
                        chunk.disk_id.as_u64()
                    );
                }
                Err(e) => {
                    error!("  ✗ DISK CHECK ERROR: {}", e);
                }
            }
        }

        // Check 3: List all nodes and disks in database for reference
        error!("=== ALL NODES IN DATABASE ===");
        if let Ok(mut stmt) = conn.prepare("SELECT node_id, address FROM nodes") {
            if let Ok(nodes) = stmt.query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            }) {
                for node_result in nodes {
                    if let Ok((nid, addr)) = node_result {
                        error!("  node_id={}, address={}", nid, addr);
                    }
                }
            }
        }

        error!("=== ALL DISKS IN DATABASE ===");
        if let Ok(mut stmt) = conn.prepare("SELECT disk_id, node_id, path FROM disks") {
            if let Ok(disks) = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            }) {
                for disk_result in disks {
                    if let Ok((did, nid, path)) = disk_result {
                        error!("  disk_id={}, node_id={}, path={}", did, nid, path);
                    }
                }
            }
        }

        error!("=== END DIAGNOSTIC ===");
    }
}

#[async_trait]
impl MetadataStore for MetadataStoreImpl {
    async fn initialize_schema(&self) -> Result<(), Error> {
        self.run_migrations().await
    }

    async fn initialize_node_and_disks(
        &self,
        node_id: u64,
        disk_paths: &[std::path::PathBuf],
    ) -> Result<(), Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let node_id_i64 = node_id as i64;

        // Insert node with the specified node_id
        self.inner
            .conn
            .call(move |conn| {
                // Use INSERT OR IGNORE to make it idempotent
                conn.execute(
                    "INSERT OR IGNORE INTO nodes (node_id, address, status, last_seen, created_at)
                     VALUES (?1, 'localhost:7000', 0, ?2, ?3)",
                    rusqlite::params![node_id_i64, now, now],
                )?;
                Ok(())
            })
            .await
            .map_err(|e| Error::QueryError(format!("Failed to insert node: {}", e)))?;

        // Insert disk records for each configured path
        for (index, disk_path) in disk_paths.iter().enumerate() {
            let path_str = disk_path.to_string_lossy().to_string();
            let disk_id = index as i64;
            let node_id_for_disk = node_id_i64; // Copy for move into closure
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;

            self.inner
                .conn
                .call(move |conn| {
                    // Use INSERT OR IGNORE to make it idempotent
                    conn.execute(
                        "INSERT OR IGNORE INTO disks (disk_id, node_id, path, total_space, free_space, status, created_at, updated_at)
                         VALUES (?1, ?2, ?3, 1000000000000, 500000000000, 0, ?4, ?5)",
                        rusqlite::params![disk_id, node_id_for_disk, path_str, now, now],
                    )?;
                    Ok(())
                })
                .await
                .map_err(|e| Error::QueryError(format!("Failed to insert disk: {}", e)))?;
        }

        Ok(())
    }

    async fn create_file(
        &self,
        file_id: FileId,
        path: &Path,
        inode: u64,
        metadata: FileMetadata,
    ) -> Result<(), Error> {
        let start = tokio::time::Instant::now();

        let path_str = path.to_string_lossy().to_string();
        let parent_path = path
            .parent()
            .unwrap_or_else(|| Path::new("/"))
            .to_string_lossy()
            .to_string();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        let file_type_val: i32 = metadata.file_type.into();

        let result = self
            .inner
            .conn
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO files (file_id, inode, path, parent_path, name, file_type, size, permissions, uid, gid, created_at, modified_at, accessed_at, storage_policy_id, target)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, 1, ?14)",
                    params![
                        file_id,
                        inode as i64,
                        path_str,
                        parent_path,
                        name,
                        file_type_val,
                        metadata.size as i64,
                        metadata.permissions as i64,
                        metadata.uid as i64,
                        metadata.gid as i64,
                        system_time_to_unix(metadata.created_at),
                        system_time_to_unix(metadata.modified_at),
                        system_time_to_unix(metadata.accessed_at),
                        metadata.target,
                    ],
                )?;
                Ok(())
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("UNIQUE constraint failed") {
                    Error::FileAlreadyExists(path.to_path_buf())
                } else {
                    Error::QueryError(format!(
                        "Failed to create file at path {:?} (inode {}): {}",
                        path, inode, e
                    ))
                }
            });

        // Publish metrics
        self.publish_metrics("create_file", "write", start, result.is_err());

        result
    }

    async fn get_file_by_path(&self, path: &Path) -> Result<FileRecord, Error> {
        let start = tokio::time::Instant::now();

        let path_str = path.to_string_lossy().to_string();
        let path_clone = path.to_path_buf();

        let result = self
            .inner
            .conn
            .call(move |conn| {
                Ok(conn.query_row(
                    "SELECT file_id, inode, path, parent_path, name, file_type, size, permissions, uid, gid, created_at, modified_at, accessed_at, storage_policy_id, target
                     FROM files WHERE path = ?1",
                    params![path_str],
                    |row| {
                        Ok(FileRecord {
                            file_id: row.get::<_, FileId>(0)?,
                            inode: row.get::<_, i64>(1)? as u64,
                            path: Path::new(&row.get::<_, String>(2)?).to_path_buf(),
                            parent_path: Path::new(&row.get::<_, String>(3)?).to_path_buf(),
                            name: row.get(4)?,
                            file_type: row.get::<_, i32>(5)?.into(),
                            size: row.get::<_, i64>(6)? as u64,
                            permissions: row.get::<_, i64>(7)? as u32,
                            uid: row.get::<_, i64>(8)? as u32,
                            gid: row.get::<_, i64>(9)? as u32,
                            created_at: unix_to_system_time(row.get(10)?),
                            modified_at: unix_to_system_time(row.get(11)?),
                            accessed_at: unix_to_system_time(row.get(12)?),
                            storage_policy_id: row.get::<_, i64>(13)? as u32,
                            target: row.get::<_, Option<String>>(14)?,
                        })
                    },
                )?)
            })
            .await
            .map_err(|e| match e {
                tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows) => {
                    Error::FileNotFoundByPath(path_clone.to_string_lossy().to_string())
                }
                _ => Error::QueryError(format!(
                    "Failed to query file by path {:?}: {}",
                    path_clone, e
                ))
            });

        // Publish metrics
        self.publish_metrics("get_file_by_path", "read", start, result.is_err());

        result
    }

    async fn get_file_by_inode(&self, inode: u64) -> Result<FileRecord, Error> {
        let start = tokio::time::Instant::now();

        let result = self
            .inner
            .conn
            .call(move |conn| {
                Ok(conn.query_row(
                    "SELECT file_id, inode, path, parent_path, name, file_type, size, permissions, uid, gid, created_at, modified_at, accessed_at, storage_policy_id, target
                     FROM files WHERE inode = ?1",
                    params![inode as i64],
                    |row| {
                        Ok(FileRecord {
                            file_id: row.get::<_, FileId>(0)?,
                            inode: row.get::<_, i64>(1)? as u64,
                            path: Path::new(&row.get::<_, String>(2)?).to_path_buf(),
                            parent_path: Path::new(&row.get::<_, String>(3)?).to_path_buf(),
                            name: row.get(4)?,
                            file_type: row.get::<_, i32>(5)?.into(),
                            size: row.get::<_, i64>(6)? as u64,
                            permissions: row.get::<_, i64>(7)? as u32,
                            uid: row.get::<_, i64>(8)? as u32,
                            gid: row.get::<_, i64>(9)? as u32,
                            created_at: unix_to_system_time(row.get(10)?),
                            modified_at: unix_to_system_time(row.get(11)?),
                            accessed_at: unix_to_system_time(row.get(12)?),
                            storage_policy_id: row.get::<_, i64>(13)? as u32,
                            target: row.get::<_, Option<String>>(14)?,
                        })
                    },
                )?)
            })
            .await
            .map_err(|e| match e {
                tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows) => {
                    Error::FileNotFoundByInode(inode)
                }
                _ => Error::QueryError(format!(
                    "Failed to query file by inode {}: {}",
                    inode, e
                ))
            });

        // Publish metrics
        self.publish_metrics("get_file_by_inode", "read", start, result.is_err());

        result
    }

    async fn get_file(&self, file_id: FileId) -> Result<FileRecord, Error> {
        let start = tokio::time::Instant::now();
        let file_id_clone = file_id;

        let result = self
            .inner
            .conn
            .call(move |conn| {
                Ok(conn.query_row(
                    "SELECT file_id, inode, path, parent_path, name, file_type, size, permissions, uid, gid, created_at, modified_at, accessed_at, storage_policy_id, target
                     FROM files WHERE file_id = ?1",
                    params![file_id],
                    |row| {
                        Ok(FileRecord {
                            file_id: row.get::<_, FileId>(0)?,
                            inode: row.get::<_, i64>(1)? as u64,
                            path: Path::new(&row.get::<_, String>(2)?).to_path_buf(),
                            parent_path: Path::new(&row.get::<_, String>(3)?).to_path_buf(),
                            name: row.get(4)?,
                            file_type: row.get::<_, i32>(5)?.into(),
                            size: row.get::<_, i64>(6)? as u64,
                            permissions: row.get::<_, i64>(7)? as u32,
                            uid: row.get::<_, i64>(8)? as u32,
                            gid: row.get::<_, i64>(9)? as u32,
                            created_at: unix_to_system_time(row.get(10)?),
                            modified_at: unix_to_system_time(row.get(11)?),
                            accessed_at: unix_to_system_time(row.get(12)?),
                            storage_policy_id: row.get::<_, i64>(13)? as u32,
                            target: row.get::<_, Option<String>>(14)?,
                        })
                    },
                )?)
            })
            .await
            .map_err(|e| match e {
                tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows) => {
                    Error::FileNotFoundByFileId(file_id_clone)
                }
                _ => Error::QueryError(format!(
                    "Failed to query file by ID {:?}: {}",
                    file_id_clone, e
                ))
            });

        // Publish metrics
        self.publish_metrics("get_file", "read", start, result.is_err());

        result
    }

    async fn update_file(&self, file_id: FileId, metadata: FileMetadata) -> Result<(), Error> {
        let start = tokio::time::Instant::now();

        let result = async {
            let rows_affected = self
                .inner
                .conn
                .call(move |conn| {
                    Ok(conn.execute(
                        "UPDATE files SET size = ?1, permissions = ?2, uid = ?3, gid = ?4, modified_at = ?5, accessed_at = ?6, target = ?7
                         WHERE file_id = ?8",
                        params![
                            metadata.size as i64,
                            metadata.permissions as i64,
                            metadata.uid as i64,
                            metadata.gid as i64,
                            system_time_to_unix(metadata.modified_at),
                            system_time_to_unix(metadata.accessed_at),
                            metadata.target,
                            file_id,
                        ],
                    )?)
                })
                .await
                .map_err(|e| {
                    Error::QueryError(format!(
                        "Failed to update file with ID {:?}: {}",
                        file_id, e
                    ))
                })?;

            if rows_affected == 0 {
                return Err(Error::FileNotFoundByFileId(file_id));
            }

            Ok(())
        }
        .await;

        // Publish metrics
        self.publish_metrics("update_file", "write", start, result.is_err());

        result
    }

    async fn delete_file(&self, file_id: FileId) -> Result<(), Error> {
        let start = tokio::time::Instant::now();

        let result = async {
            let rows_affected = self
                .inner
                .conn
                .call(move |conn| {
                    Ok(conn.execute("DELETE FROM files WHERE file_id = ?1", params![file_id])?)
                })
                .await
                .map_err(|e| {
                    Error::QueryError(format!(
                        "Failed to delete file with ID {:?}: {}",
                        file_id, e
                    ))
                })?;

            if rows_affected == 0 {
                return Err(Error::FileNotFoundByFileId(file_id));
            }

            Ok(())
        }
        .await;

        // Invalidate cache on successful delete
        if result.is_ok() {
            self.inner.cache.invalidate_file(&file_id).await;
        }

        // Publish metrics
        self.publish_metrics("delete_file", "write", start, result.is_err());

        result
    }

    async fn list_directory(&self, path: &Path) -> Result<Vec<FileRecord>, Error> {
        let start = tokio::time::Instant::now();

        let path_str = path.to_string_lossy().to_string();
        let path_str_for_error = path_str.clone();

        let result = self
            .inner
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT file_id, inode, path, parent_path, name, file_type, size, permissions, uid, gid, created_at, modified_at, accessed_at, storage_policy_id, target
                     FROM files WHERE parent_path = ?1 ORDER BY name",
                )?;

                let files = stmt
                    .query_map(params![path_str], |row| {
                        Ok(FileRecord {
                            file_id: row.get::<_, FileId>(0)?,
                            inode: row.get::<_, i64>(1)? as u64,
                            path: Path::new(&row.get::<_, String>(2)?).to_path_buf(),
                            parent_path: Path::new(&row.get::<_, String>(3)?).to_path_buf(),
                            name: row.get(4)?,
                            file_type: row.get::<_, i32>(5)?.into(),
                            size: row.get::<_, i64>(6)? as u64,
                            permissions: row.get::<_, i64>(7)? as u32,
                            uid: row.get::<_, i64>(8)? as u32,
                            gid: row.get::<_, i64>(9)? as u32,
                            created_at: unix_to_system_time(row.get(10)?),
                            modified_at: unix_to_system_time(row.get(11)?),
                            accessed_at: unix_to_system_time(row.get(12)?),
                            storage_policy_id: row.get::<_, i64>(13)? as u32,
                            target: row.get::<_, Option<String>>(14)?,
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(files)
            })
            .await
            .map_err(|e| {
                Error::QueryError(format!(
                    "Failed to list directory at path {:?}: {}",
                    path_str_for_error, e
                ))
            });

        // Publish metrics
        self.publish_metrics("list_directory", "read", start, result.is_err());

        result
    }

    async fn allocate_stripes(
        &self,
        file_id: FileId,
        stripes: Vec<StripeRecord>,
    ) -> Result<(), Error> {
        let start = tokio::time::Instant::now();
        let stripe_count = stripes.len();

        let result = self
            .inner
            .conn
            .call(move |conn| {
                let tx = conn.transaction()?;

                for stripe in stripes {
                    tx.execute(
                        "INSERT INTO stripes (stripe_id, file_id, stripe_index, offset, size, checksum, created_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![
                            stripe.stripe_id,
                            file_id,
                            stripe.stripe_index as i64,
                            stripe.offset as i64,
                            stripe.size as i64,
                            stripe.checksum as i64,
                            system_time_to_unix(stripe.created_at),
                        ],
                    )?;
                }

                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(|e| {
                Error::QueryError(format!(
                    "Failed to allocate {} stripes for file {:?}: {}",
                    stripe_count, file_id, e
                ))
            });

        // Publish metrics
        self.publish_metrics("allocate_stripes", "write", start, result.is_err());

        result
    }

    async fn get_stripe(&self, stripe_id: StripeId) -> Result<StripeRecord, Error> {
        let start = tokio::time::Instant::now();
        let stripe_id_clone = stripe_id;

        let result = self
            .inner
            .conn
            .call(move |conn| {
                Ok(conn.query_row(
                    "SELECT stripe_id, file_id, stripe_index, offset, size, checksum, created_at
                     FROM stripes WHERE stripe_id = ?1",
                    params![stripe_id],
                    |row| {
                        Ok(StripeRecord {
                            stripe_id: row.get::<_, StripeId>(0)?,
                            file_id: row.get::<_, FileId>(1)?,
                            stripe_index: row.get::<_, i64>(2)? as u32,
                            offset: row.get::<_, i64>(3)? as u64,
                            size: row.get::<_, i64>(4)? as u64,
                            checksum: row.get::<_, i64>(5)? as u32,
                            created_at: unix_to_system_time(row.get(6)?),
                        })
                    },
                )?)
            })
            .await
            .map_err(|e| match e {
                tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows) => {
                    Error::StripeNotFound(stripe_id_clone)
                }
                _ => Error::QueryError(format!(
                    "Failed to query stripe with ID {:?}: {}",
                    stripe_id_clone, e
                )),
            });

        // Publish metrics
        self.publish_metrics("get_stripe", "read", start, result.is_err());

        result
    }

    async fn get_file_stripes(&self, file_id: FileId) -> Result<Vec<StripeRecord>, Error> {
        let start = tokio::time::Instant::now();

        let result = self
            .inner
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT stripe_id, file_id, stripe_index, offset, size, checksum, created_at
                     FROM stripes WHERE file_id = ?1 ORDER BY stripe_index",
                )?;

                let stripes = stmt
                    .query_map(params![file_id], |row| {
                        Ok(StripeRecord {
                            stripe_id: row.get::<_, StripeId>(0)?,
                            file_id: row.get::<_, FileId>(1)?,
                            stripe_index: row.get::<_, i64>(2)? as u32,
                            offset: row.get::<_, i64>(3)? as u64,
                            size: row.get::<_, i64>(4)? as u64,
                            checksum: row.get::<_, i64>(5)? as u32,
                            created_at: unix_to_system_time(row.get(6)?),
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(stripes)
            })
            .await
            .map_err(|e| {
                Error::QueryError(format!(
                    "Failed to get stripes for file {:?}: {}",
                    file_id, e
                ))
            });

        // Publish metrics
        self.publish_metrics("get_file_stripes", "read", start, result.is_err());

        result
    }

    async fn get_stripe_at_offset(
        &self,
        file_id: FileId,
        offset: u64,
    ) -> Result<StripeRecord, Error> {
        let start = tokio::time::Instant::now();

        // Try cache first
        if let Some(cached_record) = self.inner.cache.get_stripe_by_offset(file_id, offset).await {
            // Cache hit! Clone Arc contents and return
            let result = Ok((*cached_record).clone());

            // Publish cache hit metric
            if let Some(ref metrics) = *self.inner.metrics.read().unwrap() {
                let _ = metrics.publish_counter(
                    "metadata_store.get_stripe_at_offset.cache_hit",
                    1,
                    crate::metric_service::UnitType::Operations,
                );
            }

            self.publish_metrics("get_stripe_at_offset", "read", start, false);
            return result;
        }

        // Cache miss - query database
        let file_id_clone = file_id;
        let result = self
            .inner
            .conn
            .call(move |conn| {
                Ok(conn.query_row(
                    "SELECT stripe_id, file_id, stripe_index, offset, size, checksum, created_at
                     FROM stripes
                     WHERE file_id = ?1 AND offset <= ?2 AND (offset + size) > ?2
                     ORDER BY stripe_index LIMIT 1",
                    params![file_id, offset as i64],
                    |row| {
                        Ok(StripeRecord {
                            stripe_id: row.get::<_, StripeId>(0)?,
                            file_id: row.get::<_, FileId>(1)?,
                            stripe_index: row.get::<_, i64>(2)? as u32,
                            offset: row.get::<_, i64>(3)? as u64,
                            size: row.get::<_, i64>(4)? as u64,
                            checksum: row.get::<_, i64>(5)? as u32,
                            created_at: unix_to_system_time(row.get(6)?),
                        })
                    },
                )?)
            })
            .await
            .map_err(|e| match e {
                tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows) => {
                    Error::QueryError(format!(
                        "No stripe found at offset {} for file {:?}",
                        offset, file_id_clone
                    ))
                }
                _ => Error::QueryError(format!(
                    "Failed to query stripe at offset {} for file {:?}: {}",
                    offset, file_id_clone, e
                )),
            });

        // If successful, populate cache (write-through)
        if let Ok(ref record) = result {
            self.inner.cache.insert_stripe(record.clone()).await;

            // Publish cache miss metric
            if let Some(ref metrics) = *self.inner.metrics.read().unwrap() {
                let _ = metrics.publish_counter(
                    "metadata_store.get_stripe_at_offset.cache_miss",
                    1,
                    crate::metric_service::UnitType::Operations,
                );
            }
        }

        // Publish metrics
        self.publish_metrics("get_stripe_at_offset", "read", start, result.is_err());

        result
    }

    async fn delete_stripe(&self, stripe_id: StripeId) -> Result<(), Error> {
        let start = tokio::time::Instant::now();

        let result = self
            .inner
            .conn
            .call(move |conn| {
                let tx = conn.transaction()?;

                // First delete all chunks for this stripe
                tx.execute(
                    "DELETE FROM chunks WHERE stripe_id = ?1",
                    params![stripe_id],
                )?;

                // Then delete the stripe itself
                let rows_deleted = tx.execute(
                    "DELETE FROM stripes WHERE stripe_id = ?1",
                    params![stripe_id],
                )?;

                if rows_deleted == 0 {
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::QueryReturnedNoRows,
                    ));
                }

                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(|e| match e {
                tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows) => {
                    Error::QueryError(format!("Stripe {:?} not found", stripe_id))
                }
                _ => Error::QueryError(format!("Failed to delete stripe {:?}: {}", stripe_id, e)),
            });

        // Invalidate cache on successful delete
        if result.is_ok() {
            self.inner.cache.invalidate_stripe(&stripe_id).await;
        }

        // Publish metrics
        self.publish_metrics("delete_stripe", "write", start, result.is_err());

        result
    }

    async fn allocate_chunks(
        &self,
        stripe_id: StripeId,
        chunks: Vec<ChunkRecord>,
    ) -> Result<(), Error> {
        let start = tokio::time::Instant::now();
        let chunk_count = chunks.len();

        let result = self
            .inner
            .conn
            .call(move |conn| {
                let mut needs_diagnostics = false;
                let result = (|| {
                    let tx = conn.transaction()?;

                    for chunk in &chunks {
                        let status = match chunk.status {
                            ChunkStatus::Healthy => 0,
                            ChunkStatus::Corrupt => 1,
                            ChunkStatus::Missing => 2,
                            ChunkStatus::Rebuilding => 3,
                        };

                        if let Err(e) = tx.execute(
                            "INSERT INTO chunks (chunk_id, stripe_id, chunk_index, node_id, disk_id, checksum, status, created_at, last_verified)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                            params![
                                chunk.chunk_id,
                                stripe_id,
                                chunk.chunk_index as i64,
                                chunk.node_id.as_u64() as i64,
                                chunk.disk_id.as_u64() as i64,
                                chunk.checksum as i64,
                                status,
                                system_time_to_unix(chunk.created_at),
                                chunk.last_verified.map(system_time_to_unix),
                            ],
                        ) {
                            if e.to_string().contains("FOREIGN KEY") {
                                needs_diagnostics = true;
                            }
                            return Err(e);
                        }
                    }

                    if let Err(e) = tx.commit() {
                        if e.to_string().contains("FOREIGN KEY") {
                            needs_diagnostics = true;
                        }
                        return Err(e);
                    }
                    Ok(())
                })();

                // Run diagnostics if FK error occurred (transaction is now dropped)
                if needs_diagnostics {
                    Self::diagnose_fk_constraint_failure_sync(conn, stripe_id, &chunks);
                }

                result.map_err(|e| e.into())
            })
            .await
            .map_err(|e| {
                Error::QueryError(format!(
                    "Failed to allocate {} chunks for stripe {:?}: {}",
                    chunk_count, stripe_id, e
                ))
            });

        // Invalidate cache on successful allocation (new chunks added to stripe)
        if result.is_ok() {
            self.inner.cache.invalidate_stripe(&stripe_id).await;
        }

        // Publish metrics
        self.publish_metrics("allocate_chunks", "write", start, result.is_err());

        result
    }

    async fn get_chunk(&self, chunk_id: ChunkId) -> Result<ChunkRecord, Error> {
        let start = tokio::time::Instant::now();
        let chunk_id_clone = chunk_id;

        let result = self
            .inner
            .conn
            .call(move |conn| {
                Ok(conn.query_row(
                    "SELECT chunk_id, stripe_id, chunk_index, node_id, disk_id, checksum, status, created_at, last_verified
                     FROM chunks WHERE chunk_id = ?1",
                    params![chunk_id],
                    |row| {
                        let status_val: i64 = row.get(6)?;
                        let status = match status_val {
                            0 => ChunkStatus::Healthy,
                            1 => ChunkStatus::Corrupt,
                            2 => ChunkStatus::Missing,
                            3 => ChunkStatus::Rebuilding,
                            _ => ChunkStatus::Healthy,
                        };

                        Ok(ChunkRecord {
                            chunk_id: row.get::<_, ChunkId>(0)?,
                            stripe_id: row.get::<_, StripeId>(1)?,
                            chunk_index: row.get::<_, i64>(2)? as u8,
                            node_id: NodeId::new(row.get::<_, i64>(3)? as u64),
                            disk_id: DiskId::new(row.get::<_, i64>(4)? as u64),
                            checksum: row.get::<_, i64>(5)? as u32,
                            status,
                            created_at: unix_to_system_time(row.get(7)?),
                            last_verified: row.get::<_, Option<i64>>(8)?.map(unix_to_system_time),
                        })
                    },
                )?)
            })
            .await
            .map_err(|e| match e {
                tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows) => {
                    Error::ChunkNotFound(chunk_id_clone)
                }
                _ => Error::QueryError(format!(
                    "Failed to query chunk with ID {:?}: {}",
                    chunk_id_clone, e
                ))
            });

        // Publish metrics
        self.publish_metrics("get_chunk", "read", start, result.is_err());

        result
    }

    async fn get_stripe_chunks(&self, stripe_id: StripeId) -> Result<Vec<ChunkRecord>, Error> {
        let start = tokio::time::Instant::now();

        // Try cache first
        if let Some(cached_chunks) = self.inner.cache.get_chunks(&stripe_id).await {
            // Cache hit! Clone Arc contents and return
            let result = Ok((*cached_chunks).clone());

            // Publish cache hit metric
            if let Some(ref metrics) = *self.inner.metrics.read().unwrap() {
                let _ = metrics.publish_counter(
                    "metadata_store.get_stripe_chunks.cache_hit",
                    1,
                    crate::metric_service::UnitType::Operations,
                );
            }

            self.publish_metrics("get_stripe_chunks", "read", start, false);
            return result;
        }

        // Cache miss - query database
        let result = self
            .inner
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT chunk_id, stripe_id, chunk_index, node_id, disk_id, checksum, status, created_at, last_verified
                     FROM chunks WHERE stripe_id = ?1 ORDER BY chunk_index",
                )?;

                let chunks = stmt
                    .query_map(params![stripe_id], |row| {
                        let status_val: i64 = row.get(6)?;
                        let status = match status_val {
                            0 => ChunkStatus::Healthy,
                            1 => ChunkStatus::Corrupt,
                            2 => ChunkStatus::Missing,
                            3 => ChunkStatus::Rebuilding,
                            _ => ChunkStatus::Healthy,
                        };

                        Ok(ChunkRecord {
                            chunk_id: row.get::<_, ChunkId>(0)?,
                            stripe_id: row.get::<_, StripeId>(1)?,
                            chunk_index: row.get::<_, i64>(2)? as u8,
                            node_id: NodeId::new(row.get::<_, i64>(3)? as u64),
                            disk_id: DiskId::new(row.get::<_, i64>(4)? as u64),
                            checksum: row.get::<_, i64>(5)? as u32,
                            status,
                            created_at: unix_to_system_time(row.get(7)?),
                            last_verified: row.get::<_, Option<i64>>(8)?.map(unix_to_system_time),
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(chunks)
            })
            .await
            .map_err(|e| {
                Error::QueryError(format!(
                    "Failed to get chunks for stripe {:?}: {}",
                    stripe_id, e
                ))
            });

        // If successful, populate cache (write-through)
        if let Ok(ref chunks) = result {
            self.inner
                .cache
                .insert_chunks(stripe_id, chunks.clone())
                .await;

            // Publish cache miss metric
            if let Some(ref metrics) = *self.inner.metrics.read().unwrap() {
                let _ = metrics.publish_counter(
                    "metadata_store.get_stripe_chunks.cache_miss",
                    1,
                    crate::metric_service::UnitType::Operations,
                );
            }
        }

        // Publish metrics
        self.publish_metrics("get_stripe_chunks", "read", start, result.is_err());

        result
    }

    async fn update_chunk_location(
        &self,
        chunk_id: ChunkId,
        node_id: NodeId,
        disk_id: DiskId,
    ) -> Result<(), Error> {
        let start = tokio::time::Instant::now();

        // Query chunk first to get stripe_id for cache invalidation
        let chunk = self.get_chunk(chunk_id).await?;
        let stripe_id = chunk.stripe_id;

        let node_id_val = node_id.as_u64();
        let disk_id_val = disk_id.as_u64();

        let result = async {
            let rows_affected = self
                .inner
                .conn
                .call(move |conn| {
                    Ok(conn.execute(
                        "UPDATE chunks SET node_id = ?1, disk_id = ?2 WHERE chunk_id = ?3",
                        params![node_id_val as i64, disk_id_val as i64, chunk_id,],
                    )?)
                })
                .await
                .map_err(|e| {
                    Error::QueryError(format!(
                        "Failed to update location for chunk {:?}: {}",
                        chunk_id, e
                    ))
                })?;

            if rows_affected == 0 {
                return Err(Error::ChunkNotFound(chunk_id));
            }

            Ok(())
        }
        .await;

        // Invalidate cache on successful update (chunk location changed)
        if result.is_ok() {
            self.inner.cache.invalidate_stripe(&stripe_id).await;
        }

        // Publish metrics
        self.publish_metrics("update_chunk_location", "write", start, result.is_err());

        result
    }

    async fn mark_chunk_corrupt(&self, chunk_id: ChunkId) -> Result<(), Error> {
        let start = tokio::time::Instant::now();

        // Query chunk first to get stripe_id for cache invalidation
        let chunk = self.get_chunk(chunk_id).await?;
        let stripe_id = chunk.stripe_id;

        let result = async {
            let rows_affected = self
                .inner
                .conn
                .call(move |conn| {
                    Ok(conn.execute(
                        "UPDATE chunks SET status = 1 WHERE chunk_id = ?1",
                        params![chunk_id],
                    )?)
                })
                .await
                .map_err(|e| {
                    Error::QueryError(format!(
                        "Failed to mark chunk {:?} as corrupt: {}",
                        chunk_id, e
                    ))
                })?;

            if rows_affected == 0 {
                return Err(Error::ChunkNotFound(chunk_id));
            }

            Ok(())
        }
        .await;

        // Invalidate cache on successful update (chunk status changed)
        if result.is_ok() {
            self.inner.cache.invalidate_stripe(&stripe_id).await;
        }

        // Publish metrics
        self.publish_metrics("mark_chunk_corrupt", "write", start, result.is_err());

        result
    }

    async fn update_chunk_verification(
        &self,
        chunk_id: ChunkId,
        verified_at: SystemTime,
    ) -> Result<(), Error> {
        let start = tokio::time::Instant::now();

        // Query chunk first to get stripe_id for cache invalidation
        let chunk = self.get_chunk(chunk_id).await?;
        let stripe_id = chunk.stripe_id;

        let verified_at_unix = system_time_to_unix(verified_at);

        let result = async {
            let rows_affected = self
                .inner
                .conn
                .call(move |conn| {
                    Ok(conn.execute(
                        "UPDATE chunks SET last_verified = ?1 WHERE chunk_id = ?2",
                        params![verified_at_unix, chunk_id],
                    )?)
                })
                .await
                .map_err(|e| {
                    Error::QueryError(format!(
                        "Failed to update verification status for chunk {:?}: {}",
                        chunk_id, e
                    ))
                })?;

            if rows_affected == 0 {
                return Err(Error::ChunkNotFound(chunk_id));
            }

            Ok(())
        }
        .await;

        // Invalidate cache on successful update (verification timestamp changed)
        if result.is_ok() {
            self.inner.cache.invalidate_stripe(&stripe_id).await;
        }

        // Publish metrics
        self.publish_metrics("update_chunk_verification", "write", start, result.is_err());

        result
    }

    async fn acquire_read_lock(
        &self,
        file_id: FileId,
        client_id: ClientId,
        expires_at: SystemTime,
    ) -> Result<u64, Error> {
        let start = tokio::time::Instant::now();
        let client_id_val = client_id.as_u64();
        let expires_at_unix = system_time_to_unix(expires_at);

        let result = self
            .inner
            .conn
            .call(move |conn| {
                // Use IMMEDIATE transaction to prevent race conditions
                let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

                // Capture time inside transaction for consistency
                let now = system_time_to_unix(SystemTime::now());

                // Check for conflicting write locks within the transaction
                let has_write_lock: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM locks WHERE file_id = ?1 AND lock_type = 1 AND expires_at > ?2)",
                    params![file_id, now],
                    |row| row.get(0),
                )?;

                if has_write_lock {
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::ToSqlConversionFailure(Box::new(
                            std::io::Error::new(std::io::ErrorKind::WouldBlock, "Lock conflict: write lock exists")
                        ))
                    ));
                }

                // Acquire read lock
                tx.execute(
                    "INSERT INTO locks (file_id, client_id, lock_type, acquired_at, expires_at)
                     VALUES (?1, ?2, 0, ?3, ?4)",
                    params![file_id, client_id_val as i64, now, expires_at_unix],
                )?;

                let lock_id = tx.last_insert_rowid() as u64;
                tx.commit()?;
                Ok(lock_id)
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("Lock conflict") {
                    Error::LockConflict {
                        file_id,
                        lock_type: "read".to_string(),
                    }
                } else {
                    Error::QueryError(format!(
                        "Failed to acquire read lock on file {:?} for client {:?}: {}",
                        file_id, client_id, e
                    ))
                }
            });

        // Publish metrics
        self.publish_metrics("acquire_read_lock", "write", start, result.is_err());

        result
    }

    async fn acquire_write_lock(
        &self,
        file_id: FileId,
        client_id: ClientId,
        node_id: u64,
        expires_at: SystemTime,
    ) -> Result<u64, Error> {
        let start = tokio::time::Instant::now();
        let client_id_val = client_id.as_u64();
        let node_id_val = node_id;
        let expires_at_unix = system_time_to_unix(expires_at);

        let result = self
            .inner
            .conn
            .call(move |conn| {
                // Use IMMEDIATE transaction to prevent race conditions
                let tx =
                    conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

                // Capture time inside transaction for consistency
                let now = system_time_to_unix(SystemTime::now());

                // Check for any existing locks within the transaction
                let has_any_lock: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM locks WHERE file_id = ?1 AND expires_at > ?2)",
                    params![file_id, now],
                    |row| row.get(0),
                )?;

                if has_any_lock {
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                            std::io::ErrorKind::WouldBlock,
                            "Lock conflict: existing lock(s)",
                        ))),
                    ));
                }

                // Acquire write lock with node_id
                tx.execute(
                    "INSERT INTO locks (file_id, client_id, lock_type, node_id, acquired_at, expires_at)
                     VALUES (?1, ?2, 1, ?3, ?4, ?5)",
                    params![file_id, client_id_val as i64, node_id_val as i64, now, expires_at_unix],
                )?;

                let lock_id = tx.last_insert_rowid() as u64;
                tx.commit()?;
                Ok(lock_id)
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("Lock conflict") {
                    Error::LockConflict {
                        file_id,
                        lock_type: "write".to_string(),
                    }
                } else {
                    Error::QueryError(format!(
                        "Failed to acquire write lock on file {:?} for client {:?}: {}",
                        file_id, client_id, e
                    ))
                }
            });

        // Publish metrics
        self.publish_metrics("acquire_write_lock", "write", start, result.is_err());

        result
    }

    async fn release_lock(&self, file_id: FileId, client_id: ClientId) -> Result<(), Error> {
        let start = tokio::time::Instant::now();
        let client_id_val = client_id.as_u64();

        let result = async {
            let rows_affected = self
                .inner
                .conn
                .call(move |conn| {
                    Ok(conn.execute(
                        "DELETE FROM locks WHERE file_id = ?1 AND client_id = ?2",
                        params![file_id, client_id_val as i64],
                    )?)
                })
                .await
                .map_err(|e| {
                    Error::QueryError(format!(
                        "Failed to release lock for file {:?} and client {:?}: {}",
                        file_id, client_id, e
                    ))
                })?;

            if rows_affected == 0 {
                return Err(Error::LockNotFound { file_id, client_id });
            }

            Ok(())
        }
        .await;

        // Publish metrics
        self.publish_metrics("release_lock", "write", start, result.is_err());

        result
    }

    async fn extend_lock(
        &self,
        file_id: FileId,
        client_id: ClientId,
        new_expiry: SystemTime,
    ) -> Result<(), Error> {
        let start = tokio::time::Instant::now();
        let client_id_val = client_id.as_u64();
        let new_expiry_unix = system_time_to_unix(new_expiry);

        let result = async {
            let rows_affected = self
                .inner
                .conn
                .call(move |conn| {
                    Ok(conn.execute(
                        "UPDATE locks SET expires_at = ?1 WHERE file_id = ?2 AND client_id = ?3",
                        params![new_expiry_unix, file_id, client_id_val as i64,],
                    )?)
                })
                .await
                .map_err(|e| {
                    Error::QueryError(format!(
                        "Failed to extend lock for file {:?} and client {:?}: {}",
                        file_id, client_id, e
                    ))
                })?;

            if rows_affected == 0 {
                return Err(Error::LockNotFound { file_id, client_id });
            }

            Ok(())
        }
        .await;

        // Publish metrics
        self.publish_metrics("extend_lock", "write", start, result.is_err());

        result
    }

    async fn get_file_locks(&self, file_id: FileId) -> Result<Vec<LockRecord>, Error> {
        let start = tokio::time::Instant::now();

        let result = self
            .inner
            .conn
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT lock_id, file_id, client_id, lock_type, acquired_at, expires_at
                     FROM locks WHERE file_id = ?1",
                )?;

                let locks = stmt
                    .query_map(params![file_id], |row| {
                        let lock_type_val: i64 = row.get(3)?;
                        let lock_type = match lock_type_val {
                            0 => LockType::Read,
                            1 => LockType::Write,
                            _ => LockType::Read,
                        };

                        Ok(LockRecord {
                            lock_id: row.get::<_, i64>(0)? as u64,
                            file_id: row.get::<_, FileId>(1)?,
                            client_id: ClientId::new(row.get::<_, i64>(2)? as u64),
                            lock_type,
                            acquired_at: unix_to_system_time(row.get(4)?),
                            expires_at: unix_to_system_time(row.get(5)?),
                        })
                    })?
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(locks)
            })
            .await
            .map_err(|e| {
                Error::QueryError(format!("Failed to get locks for file {:?}: {}", file_id, e))
            });

        // Publish metrics
        self.publish_metrics("get_file_locks", "read", start, result.is_err());

        result
    }

    async fn cleanup_expired_locks(&self) -> Result<u64, Error> {
        let start = tokio::time::Instant::now();

        let result = async {
            let rows_affected = self
                .inner
                .conn
                .call(move |conn| {
                    // Capture time inside the call to ensure consistency
                    let now = system_time_to_unix(SystemTime::now());
                    Ok(conn.execute("DELETE FROM locks WHERE expires_at <= ?1", params![now])?)
                })
                .await
                .map_err(|e| {
                    Error::QueryError(format!("Failed to cleanup expired locks: {}", e))
                })?;

            Ok(rows_affected as u64)
        }
        .await;

        // Publish metrics
        self.publish_metrics("cleanup_expired_locks", "write", start, result.is_err());

        result
    }

    async fn reserve_inode(&self) -> Result<u64, Error> {
        let start = tokio::time::Instant::now();

        let result = self
            .inner
            .conn
            .call(move |conn| {
                let tx = conn.transaction()?;

                // Get next inode
                let inode: i64 =
                    tx.query_row("SELECT next_inode FROM inode_pool WHERE id = 1", [], |row| {
                        row.get(0)
                    })?;

                // Check for overflow before incrementing
                // SQLite INTEGER is signed i64, so we can't exceed i64::MAX
                if inode >= i64::MAX {
                    return Err(tokio_rusqlite::Error::Other(Box::new(
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Inode space exhausted"
                        )
                    )));
                }

                // Increment next_inode
                tx.execute(
                    "UPDATE inode_pool SET next_inode = next_inode + 1 WHERE id = 1",
                    [],
                )?;

                // Create reservation (1 hour expiration)
                let now = system_time_to_unix(SystemTime::now());
                let expires_at = now + 3600; // 1 hour
                tx.execute(
                    "INSERT INTO inode_reservations (inode, reserved_at, expires_at) VALUES (?1, ?2, ?3)",
                    params![inode, now, expires_at],
                )?;

                tx.commit()?;
                Ok(inode as u64)
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("Inode space exhausted") {
                    Error::InodeSpaceExhausted
                } else {
                    Error::QueryError(format!("Failed to reserve inode: {}", e))
                }
            });

        // Publish metrics
        self.publish_metrics("reserve_inode", "write", start, result.is_err());

        result
    }

    async fn confirm_inode(&self, inode: u64) -> Result<(), Error> {
        let start = tokio::time::Instant::now();
        let now = system_time_to_unix(SystemTime::now());

        let result = self
            .inner
            .conn
            .call(move |conn| {
                // Check if inode is reserved and not expired
                let is_valid: Option<i64> = conn
                    .query_row(
                        "SELECT inode FROM inode_reservations WHERE inode = ?1 AND expires_at > ?2",
                        params![inode as i64, now],
                        |row| row.get(0),
                    )
                    .optional()?;

                if is_valid.is_none() {
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Inode not reserved",
                        ))),
                    ));
                }

                // Remove reservation (inode is now in use via files table)
                conn.execute(
                    "DELETE FROM inode_reservations WHERE inode = ?1",
                    params![inode as i64],
                )?;

                Ok(())
            })
            .await
            .map_err(|e| {
                if e.to_string().contains("Inode not reserved") {
                    Error::InodeNotReserved(inode)
                } else {
                    Error::QueryError(format!("Failed to confirm inode: {}", e))
                }
            });

        // Publish metrics
        self.publish_metrics("confirm_inode", "write", start, result.is_err());

        result
    }

    async fn release_inode(&self, inode: u64) -> Result<(), Error> {
        let start = tokio::time::Instant::now();

        let result = async {
            let rows_affected = self
                .inner
                .conn
                .call(move |conn| {
                    Ok(conn.execute(
                        "DELETE FROM inode_reservations WHERE inode = ?1",
                        params![inode as i64],
                    )?)
                })
                .await
                .map_err(|e| Error::QueryError(format!("Failed to release inode: {}", e)))?;

            if rows_affected == 0 {
                return Err(Error::InodeNotReserved(inode));
            }

            Ok(())
        }
        .await;

        // Publish metrics
        self.publish_metrics("release_inode", "write", start, result.is_err());

        result
    }

    async fn cleanup_expired_inode_reservations(&self) -> Result<u64, Error> {
        // TODO: This method should be called periodically by a background maintenance task.
        // Currently, expired reservations (1-hour TTL) remain in the database until:
        // 1. Explicitly released on error paths in FileSystemService
        // 2. This cleanup method is manually invoked (only happens in tests currently)
        // 3. Phase 2: Implement periodic background task to call this every 10-15 minutes

        let start = tokio::time::Instant::now();
        let now = system_time_to_unix(SystemTime::now());

        let result = async {
            let rows_affected = self
                .inner
                .conn
                .call(move |conn| {
                    Ok(conn.execute(
                        "DELETE FROM inode_reservations WHERE expires_at <= ?1",
                        params![now],
                    )?)
                })
                .await
                .map_err(|e| {
                    Error::QueryError(format!(
                        "Failed to cleanup expired inode reservations: {}",
                        e
                    ))
                })?;

            Ok(rows_affected as u64)
        }
        .await;

        // Publish metrics
        self.publish_metrics(
            "cleanup_expired_inode_reservations",
            "write",
            start,
            result.is_err(),
        );

        result
    }

    async fn create_snapshot(&self, snapshot_path: &Path) -> Result<(), Error> {
        let snapshot_path_buf = snapshot_path.to_path_buf();

        info!(
            "MetadataStore::create_snapshot() called with path: {}",
            snapshot_path.display()
        );

        // Create parent directory if needed
        if let Some(parent) = snapshot_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                Error::SnapshotFailed(format!("Failed to create snapshot directory: {}", e))
            })?;
        }

        // Remove existing snapshot file and associated files if they exist
        // (VACUUM INTO requires target doesn't exist)
        if tokio::fs::try_exists(&snapshot_path).await.unwrap_or(false) {
            info!(
                "Removing existing snapshot file: {}",
                snapshot_path.display()
            );
            tokio::fs::remove_file(&snapshot_path).await.map_err(|e| {
                Error::SnapshotFailed(format!("Failed to remove existing snapshot file: {}", e))
            })?;
        }

        // Also remove WAL and SHM files if they exist
        let wal_path = snapshot_path_buf.with_extension("db-wal");
        if tokio::fs::try_exists(&wal_path).await.unwrap_or(false) {
            let _ = tokio::fs::remove_file(&wal_path).await; // Ignore errors
        }

        let shm_path = snapshot_path_buf.with_extension("db-shm");
        if tokio::fs::try_exists(&shm_path).await.unwrap_or(false) {
            let _ = tokio::fs::remove_file(&shm_path).await; // Ignore errors
        }

        self.inner
            .conn
            .call(move |conn| {
                // Use VACUUM INTO to create the snapshot
                // This is the recommended approach for SQLite snapshots (since SQLite 3.27.0).
                // VACUUM INTO:
                // - Creates a completely fresh database file
                // - Copies all data from the source
                // - Handles WAL mode correctly
                // - Works with concurrent access to the source database
                // - Creates a clean, optimized copy
                // Reference: https://sqlite.org/lang_vacuum.html

                info!(
                    "Executing VACUUM INTO for snapshot: {}",
                    snapshot_path_buf.display()
                );
                conn.execute(
                    &format!("VACUUM INTO '{}'", snapshot_path_buf.display()),
                    [],
                )?;

                // Now open the snapshot and set it to DELETE journal mode
                // (ensures the snapshot is a single self-contained file)
                let snapshot_conn = rusqlite::Connection::open(&snapshot_path_buf)?;
                snapshot_conn.execute_batch(
                    "PRAGMA journal_mode=DELETE;
                     PRAGMA synchronous=FULL;",
                )?;
                drop(snapshot_conn);

                info!(
                    "Snapshot created successfully at: {}",
                    snapshot_path_buf.display()
                );
                Ok(())
            })
            .await
            .map_err(|e| Error::SnapshotFailed(format!("Snapshot creation failed: {}", e)))?;

        // Verify the file exists
        if tokio::fs::try_exists(&snapshot_path).await.unwrap_or(false) {
            let metadata = tokio::fs::metadata(&snapshot_path).await.map_err(|e| {
                Error::SnapshotFailed(format!("Failed to get snapshot metadata: {}", e))
            })?;
            info!(
                "Snapshot file verified: {} (size: {} bytes)",
                snapshot_path.display(),
                metadata.len()
            );
        } else {
            error!(
                "Snapshot file NOT FOUND after creation: {}",
                snapshot_path.display()
            );
            return Err(Error::SnapshotFailed(format!(
                "Snapshot file not found after creation: {}",
                snapshot_path.display()
            )));
        }

        Ok(())
    }

    async fn restore_from_snapshot(&self, snapshot_path: &Path) -> Result<(), Error> {
        let snapshot_path_buf = snapshot_path.to_path_buf();

        self.inner
            .conn
            .call(move |conn| {
                // Open snapshot database
                let mut snapshot_conn = rusqlite::Connection::open(&snapshot_path_buf)?;

                // Restore from snapshot to main database
                let backup = rusqlite::backup::Backup::new(&mut snapshot_conn, conn)?;
                backup.run_to_completion(5, std::time::Duration::from_millis(250), None)?;

                Ok(())
            })
            .await
            .map_err(|e| Error::RestoreFailed(format!("Snapshot restoration failed: {}", e)))
    }
}
