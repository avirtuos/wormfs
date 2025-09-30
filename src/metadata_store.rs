//! Metadata Storage Module
//!
//! This module provides a SQLite-based abstraction for WormFS metadata operations,
//! including file metadata, chunk tracking, and stripe management.
//!
//! **Optimization**: Uses compound identifiers instead of UUIDs for stripes and chunks
//! to dramatically reduce metadata overhead while maintaining perfect alignment with
//! erasure coding patterns.

use rusqlite::{Connection, OptionalExtension, Result as SqliteResult, Row};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use uuid::Uuid;

use crate::erasure_coding::ErasureCodingConfig;

/// Current database schema version
const CURRENT_SCHEMA_VERSION: u32 = 2; // Increment for compound ID schema

/// Compound identifier for stripes within a file
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StripeId {
    pub file_id: Uuid,
    pub stripe_index: u64,
}

impl StripeId {
    /// Create a new stripe identifier
    pub fn new(file_id: Uuid, stripe_index: u64) -> Self {
        Self {
            file_id,
            stripe_index,
        }
    }
}

impl fmt::Display for StripeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.file_id, self.stripe_index)
    }
}

/// Compound identifier for chunks within a stripe
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId {
    pub file_id: Uuid,
    pub stripe_index: u64,
    pub chunk_index: u8,
}

impl ChunkId {
    /// Create a new chunk identifier
    pub fn new(file_id: Uuid, stripe_index: u64, chunk_index: u8) -> Self {
        Self {
            file_id,
            stripe_index,
            chunk_index,
        }
    }

    /// Create chunk ID from stripe ID and chunk index
    pub fn from_stripe(stripe_id: StripeId, chunk_index: u8) -> Self {
        Self {
            file_id: stripe_id.file_id,
            stripe_index: stripe_id.stripe_index,
            chunk_index,
        }
    }

    /// Get the stripe ID for this chunk
    pub fn stripe_id(&self) -> StripeId {
        StripeId {
            file_id: self.file_id,
            stripe_index: self.stripe_index,
        }
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}",
            self.file_id, self.stripe_index, self.chunk_index
        )
    }
}

/// File metadata structure (optimized)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileMetadata {
    pub file_id: Uuid,
    pub path: PathBuf,
    pub size: u64,
    pub permissions: u32,
    pub created_at: SystemTime,
    pub modified_at: SystemTime,
    pub accessed_at: SystemTime,
    pub stripe_count: u64, // OPTIMIZED: Single count instead of Vec<Uuid>
    pub checksum: u32,     // CRC32 of the entire file
}

impl FileMetadata {
    /// Create new file metadata
    pub fn new(path: PathBuf, size: u64, permissions: u32) -> Self {
        let now = SystemTime::now();
        Self {
            file_id: Uuid::new_v4(),
            path,
            size,
            permissions,
            created_at: now,
            modified_at: now,
            accessed_at: now,
            stripe_count: 0, // Start with no stripes
            checksum: 0,     // Will be set when file is fully stored
        }
    }

    /// Update access time
    pub fn touch_access(&mut self) {
        self.accessed_at = SystemTime::now();
    }

    /// Update modification time
    pub fn touch_modify(&mut self) {
        self.modified_at = SystemTime::now();
    }

    /// Add a stripe to this file (increments count)
    pub fn add_stripe(&mut self) -> u64 {
        let stripe_index = self.stripe_count;
        self.stripe_count += 1;
        stripe_index
    }

    /// Get stripe IDs for this file
    pub fn stripe_ids(&self) -> impl Iterator<Item = StripeId> + '_ {
        (0..self.stripe_count).map(move |index| StripeId::new(self.file_id, index))
    }
}

/// Chunk metadata structure (optimized)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkMetadata {
    // Compound ID components (no separate chunk_id UUID needed!)
    pub file_id: Uuid,
    pub stripe_index: u64,
    pub chunk_index: u8, // Index within the stripe (0..k+m)

    // Chunk data
    pub size: u64,
    pub checksum: u32, // CRC32 of chunk data
    pub storage_location: StorageLocation,
    pub created_at: SystemTime,
    pub last_verified: Option<SystemTime>,
}

impl ChunkMetadata {
    /// Create new chunk metadata
    pub fn new(
        file_id: Uuid,
        stripe_index: u64,
        chunk_index: u8,
        size: u64,
        checksum: u32,
        storage_location: StorageLocation,
    ) -> Self {
        Self {
            file_id,
            stripe_index,
            chunk_index,
            size,
            checksum,
            storage_location,
            created_at: SystemTime::now(),
            last_verified: None,
        }
    }

    /// Get the compound chunk ID
    pub fn chunk_id(&self) -> ChunkId {
        ChunkId::new(self.file_id, self.stripe_index, self.chunk_index)
    }

    /// Get the stripe ID this chunk belongs to
    pub fn stripe_id(&self) -> StripeId {
        StripeId::new(self.file_id, self.stripe_index)
    }

    /// Mark chunk as verified
    pub fn mark_verified(&mut self) {
        self.last_verified = Some(SystemTime::now());
    }
}

/// Storage location for a chunk
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageLocation {
    pub node_id: Uuid,
    pub disk_id: String,
    pub path: PathBuf,
}

impl StorageLocation {
    /// Create new storage location
    pub fn new(node_id: Uuid, disk_id: String, path: PathBuf) -> Self {
        Self {
            node_id,
            disk_id,
            path,
        }
    }
}

/// Stripe metadata structure (optimized)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StripeMetadata {
    // Compound ID components (no separate stripe_id UUID needed!)
    pub file_id: Uuid,
    pub stripe_index: u64, // Index within the file

    // Stripe data
    pub original_size: usize, // Size before erasure coding
    pub chunk_count: u8,      // OPTIMIZED: Count instead of Vec<Uuid>
    pub erasure_config: ErasureCodingConfig,
    pub created_at: SystemTime,
}

impl StripeMetadata {
    /// Create new stripe metadata
    pub fn new(
        file_id: Uuid,
        stripe_index: u64,
        original_size: usize,
        erasure_config: ErasureCodingConfig,
    ) -> Self {
        Self {
            file_id,
            stripe_index,
            original_size,
            chunk_count: 0, // Start with no chunks
            erasure_config,
            created_at: SystemTime::now(),
        }
    }

    /// Get the compound stripe ID
    pub fn stripe_id(&self) -> StripeId {
        StripeId::new(self.file_id, self.stripe_index)
    }

    /// Add a chunk to this stripe (increments count)
    pub fn add_chunk(&mut self) -> u8 {
        let chunk_index = self.chunk_count;
        self.chunk_count += 1;
        chunk_index
    }

    /// Get chunk IDs for this stripe
    pub fn chunk_ids(&self) -> impl Iterator<Item = ChunkId> + '_ {
        (0..self.chunk_count).map(move |index| ChunkId::new(self.file_id, self.stripe_index, index))
    }
}

/// Errors that can occur during metadata operations
#[derive(Error, Debug)]
pub enum MetadataError {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("File not found: {file_id}")]
    FileNotFound { file_id: Uuid },

    #[error("Chunk not found: {chunk_id}")]
    ChunkNotFound { chunk_id: ChunkId },

    #[error("Stripe not found: {stripe_id}")]
    StripeNotFound { stripe_id: StripeId },

    #[error("Path conflict: {path:?} already exists")]
    PathConflict { path: PathBuf },

    #[error("Schema version mismatch: expected {expected}, found {found}")]
    SchemaVersionMismatch { expected: u32, found: u32 },

    #[error("Invalid metadata: {reason}")]
    InvalidMetadata { reason: String },

    #[error("Transaction error: {reason}")]
    Transaction { reason: String },
}

/// Result type for metadata operations
pub type MetadataResult<T> = Result<T, MetadataError>;

/// Helper functions for time conversion
fn system_time_to_timestamp(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn timestamp_to_system_time(timestamp: i64) -> SystemTime {
    UNIX_EPOCH + std::time::Duration::from_secs(timestamp as u64)
}

/// Helper function to parse file metadata from database row
fn parse_file_metadata(row: &Row) -> SqliteResult<FileMetadata> {
    Ok(FileMetadata {
        file_id: Uuid::parse_str(&row.get::<_, String>("file_id")?).map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                0,
                "file_id".to_string(),
                rusqlite::types::Type::Text,
            )
        })?,
        path: PathBuf::from(row.get::<_, String>("path")?),
        size: row.get::<_, i64>("size")? as u64,
        permissions: row.get::<_, i64>("permissions")? as u32,
        created_at: timestamp_to_system_time(row.get("created_at")?),
        modified_at: timestamp_to_system_time(row.get("modified_at")?),
        accessed_at: timestamp_to_system_time(row.get("accessed_at")?),
        stripe_count: row.get::<_, i64>("stripe_count")? as u64,
        checksum: row.get::<_, i64>("checksum")? as u32,
    })
}

/// Helper function to parse chunk metadata from database row
fn parse_chunk_metadata(row: &Row) -> SqliteResult<ChunkMetadata> {
    let storage_location_json: String = row.get("storage_location")?;
    let storage_location: StorageLocation =
        serde_json::from_str(&storage_location_json).map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                6,
                "storage_location".to_string(),
                rusqlite::types::Type::Text,
            )
        })?;

    let last_verified = row
        .get::<_, Option<i64>>("last_verified")?
        .map(timestamp_to_system_time);

    Ok(ChunkMetadata {
        file_id: Uuid::parse_str(&row.get::<_, String>("file_id")?).map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                0,
                "file_id".to_string(),
                rusqlite::types::Type::Text,
            )
        })?,
        stripe_index: row.get::<_, i64>("stripe_index")? as u64,
        chunk_index: row.get::<_, i64>("chunk_index")? as u8,
        size: row.get::<_, i64>("size")? as u64,
        checksum: row.get::<_, i64>("checksum")? as u32,
        storage_location,
        created_at: timestamp_to_system_time(row.get("created_at")?),
        last_verified,
    })
}

/// Helper function to parse stripe metadata from database row
fn parse_stripe_metadata(row: &Row) -> SqliteResult<StripeMetadata> {
    let erasure_config_json: String = row.get("erasure_config")?;
    let erasure_config: ErasureCodingConfig =
        serde_json::from_str(&erasure_config_json).map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                5,
                "erasure_config".to_string(),
                rusqlite::types::Type::Text,
            )
        })?;

    Ok(StripeMetadata {
        file_id: Uuid::parse_str(&row.get::<_, String>("file_id")?).map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                0,
                "file_id".to_string(),
                rusqlite::types::Type::Text,
            )
        })?,
        stripe_index: row.get::<_, i64>("stripe_index")? as u64,
        original_size: row.get::<_, i64>("original_size")? as usize,
        chunk_count: row.get::<_, i64>("chunk_count")? as u8,
        erasure_config,
        created_at: timestamp_to_system_time(row.get("created_at")?),
    })
}

/// Metadata store implementation using SQLite with compound identifiers
pub struct MetadataStore {
    conn: Connection,
}

impl MetadataStore {
    /// Create or open a metadata store at the specified path
    pub fn new<P: AsRef<Path>>(db_path: P) -> MetadataResult<Self> {
        let conn = Connection::open(db_path)?;

        // Enable foreign key constraints
        conn.execute("PRAGMA foreign_keys = ON", [])?;

        let store = Self { conn };

        // Initialize schema if needed
        store.initialize_schema()?;

        Ok(store)
    }

    /// Initialize database schema and check version
    fn initialize_schema(&self) -> MetadataResult<()> {
        // Create tables with compound key design
        self.conn.execute_batch(
            r#"
            -- Files table (optimized with stripe_count)
            CREATE TABLE IF NOT EXISTS files (
                file_id TEXT PRIMARY KEY,
                path TEXT UNIQUE NOT NULL,
                size INTEGER NOT NULL,
                permissions INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                modified_at INTEGER NOT NULL,
                accessed_at INTEGER NOT NULL,
                stripe_count INTEGER NOT NULL DEFAULT 0,
                checksum INTEGER NOT NULL
            );

            -- Stripes table (compound primary key)
            CREATE TABLE IF NOT EXISTS stripes (
                file_id TEXT NOT NULL,
                stripe_index INTEGER NOT NULL,
                original_size INTEGER NOT NULL,
                chunk_count INTEGER NOT NULL DEFAULT 0,
                erasure_config TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (file_id, stripe_index),
                FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE
            );

            -- Chunks table (triple compound primary key)
            CREATE TABLE IF NOT EXISTS chunks (
                file_id TEXT NOT NULL,
                stripe_index INTEGER NOT NULL,
                chunk_index INTEGER NOT NULL,
                size INTEGER NOT NULL,
                checksum INTEGER NOT NULL,
                storage_location TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                last_verified INTEGER,
                PRIMARY KEY (file_id, stripe_index, chunk_index),
                FOREIGN KEY (file_id, stripe_index) REFERENCES stripes(file_id, stripe_index) ON DELETE CASCADE
            );

            -- Schema version tracking
            CREATE TABLE IF NOT EXISTS schema_info (
                version INTEGER PRIMARY KEY
            );

            -- Indexes for performance
            CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
            CREATE INDEX IF NOT EXISTS idx_stripes_file_id ON stripes(file_id);
            CREATE INDEX IF NOT EXISTS idx_chunks_file_stripe ON chunks(file_id, stripe_index);
            "#,
        )?;

        // Check and set schema version
        let current_version: Option<u32> = self
            .conn
            .query_row(
                "SELECT version FROM schema_info WHERE version = ?1",
                [CURRENT_SCHEMA_VERSION],
                |row| row.get(0),
            )
            .optional()?;

        if current_version.is_none() {
            // Check if there's a different version
            let existing_version: Option<u32> = self
                .conn
                .query_row("SELECT version FROM schema_info LIMIT 1", [], |row| {
                    row.get(0)
                })
                .optional()?;

            if let Some(version) = existing_version {
                if version != CURRENT_SCHEMA_VERSION {
                    return Err(MetadataError::SchemaVersionMismatch {
                        expected: CURRENT_SCHEMA_VERSION,
                        found: version,
                    });
                }
            } else {
                // Insert current version
                self.conn.execute(
                    "INSERT INTO schema_info (version) VALUES (?1)",
                    [CURRENT_SCHEMA_VERSION],
                )?;
            }
        }

        Ok(())
    }

    /// Create a new file entry
    pub fn create_file(&self, metadata: FileMetadata) -> MetadataResult<Uuid> {
        // Check for path conflicts
        let existing_file: Option<String> = self
            .conn
            .query_row(
                "SELECT file_id FROM files WHERE path = ?1",
                [metadata.path.to_string_lossy().as_ref()],
                |row| row.get(0),
            )
            .optional()?;

        if existing_file.is_some() {
            return Err(MetadataError::PathConflict {
                path: metadata.path,
            });
        }

        let file_id = metadata.file_id;

        self.conn.execute(
            r#"
            INSERT INTO files (
                file_id, path, size, permissions, created_at, modified_at, 
                accessed_at, stripe_count, checksum
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            (
                file_id.to_string(),
                metadata.path.to_string_lossy(),
                metadata.size as i64,
                metadata.permissions as i64,
                system_time_to_timestamp(metadata.created_at),
                system_time_to_timestamp(metadata.modified_at),
                system_time_to_timestamp(metadata.accessed_at),
                metadata.stripe_count as i64,
                metadata.checksum as i64,
            ),
        )?;

        Ok(file_id)
    }

    /// Get file metadata by ID
    pub fn get_file(&self, file_id: Uuid) -> MetadataResult<FileMetadata> {
        let metadata = self
            .conn
            .query_row(
                r#"
            SELECT file_id, path, size, permissions, created_at, modified_at, 
                   accessed_at, stripe_count, checksum
            FROM files WHERE file_id = ?1
            "#,
                [file_id.to_string()],
                parse_file_metadata,
            )
            .optional()?;

        match metadata {
            Some(metadata) => Ok(metadata),
            None => Err(MetadataError::FileNotFound { file_id }),
        }
    }

    /// Update file metadata
    pub fn update_file(&self, file_id: Uuid, metadata: FileMetadata) -> MetadataResult<()> {
        if file_id != metadata.file_id {
            return Err(MetadataError::InvalidMetadata {
                reason: "file_id mismatch".to_string(),
            });
        }

        let rows_affected = self.conn.execute(
            r#"
            UPDATE files SET 
                path = ?2, size = ?3, permissions = ?4, created_at = ?5, 
                modified_at = ?6, accessed_at = ?7, stripe_count = ?8, checksum = ?9
            WHERE file_id = ?1
            "#,
            (
                file_id.to_string(),
                metadata.path.to_string_lossy(),
                metadata.size as i64,
                metadata.permissions as i64,
                system_time_to_timestamp(metadata.created_at),
                system_time_to_timestamp(metadata.modified_at),
                system_time_to_timestamp(metadata.accessed_at),
                metadata.stripe_count as i64,
                metadata.checksum as i64,
            ),
        )?;

        if rows_affected == 0 {
            return Err(MetadataError::FileNotFound { file_id });
        }

        Ok(())
    }

    /// Delete file metadata and associated stripes/chunks
    pub fn delete_file(&self, file_id: Uuid) -> MetadataResult<()> {
        let tx = self.conn.unchecked_transaction()?;

        // SQLite foreign key constraints will handle cascade deletion
        let rows_affected = tx.execute(
            "DELETE FROM files WHERE file_id = ?1",
            [file_id.to_string()],
        )?;

        if rows_affected == 0 {
            return Err(MetadataError::FileNotFound { file_id });
        }

        tx.commit()?;
        Ok(())
    }

    /// List all files
    pub fn list_files(&self) -> MetadataResult<Vec<FileMetadata>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT file_id, path, size, permissions, created_at, modified_at, 
                   accessed_at, stripe_count, checksum
            FROM files ORDER BY created_at
            "#,
        )?;

        let file_iter = stmt.query_map([], parse_file_metadata)?;
        let mut files = Vec::new();

        for file in file_iter {
            files.push(file?);
        }

        Ok(files)
    }

    /// Register a new chunk (using compound ID)
    pub fn register_chunk(&self, chunk_id: ChunkId, metadata: ChunkMetadata) -> MetadataResult<()> {
        // Verify the chunk_id matches the metadata
        if chunk_id != metadata.chunk_id() {
            return Err(MetadataError::InvalidMetadata {
                reason: "chunk_id mismatch with metadata".to_string(),
            });
        }

        let storage_location_json = serde_json::to_string(&metadata.storage_location)?;
        let last_verified = metadata.last_verified.map(system_time_to_timestamp);

        self.conn.execute(
            r#"
            INSERT INTO chunks (
                file_id, stripe_index, chunk_index, size, checksum, 
                storage_location, created_at, last_verified
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            (
                chunk_id.file_id.to_string(),
                chunk_id.stripe_index as i64,
                chunk_id.chunk_index as i64,
                metadata.size as i64,
                metadata.checksum as i64,
                storage_location_json,
                system_time_to_timestamp(metadata.created_at),
                last_verified,
            ),
        )?;

        Ok(())
    }

    /// Get chunk metadata by compound ID
    pub fn get_chunk(&self, chunk_id: ChunkId) -> MetadataResult<ChunkMetadata> {
        let metadata = self
            .conn
            .query_row(
                r#"
            SELECT file_id, stripe_index, chunk_index, size, checksum, 
                   storage_location, created_at, last_verified
            FROM chunks WHERE file_id = ?1 AND stripe_index = ?2 AND chunk_index = ?3
            "#,
                (
                    chunk_id.file_id.to_string(),
                    chunk_id.stripe_index as i64,
                    chunk_id.chunk_index as i64,
                ),
                parse_chunk_metadata,
            )
            .optional()?;

        match metadata {
            Some(metadata) => Ok(metadata),
            None => Err(MetadataError::ChunkNotFound { chunk_id }),
        }
    }

    /// Update chunk location
    pub fn update_chunk_location(
        &self,
        chunk_id: ChunkId,
        location: StorageLocation,
    ) -> MetadataResult<()> {
        let storage_location_json = serde_json::to_string(&location)?;

        let rows_affected = self.conn.execute(
            "UPDATE chunks SET storage_location = ?4 WHERE file_id = ?1 AND stripe_index = ?2 AND chunk_index = ?3",
            (
                chunk_id.file_id.to_string(),
                chunk_id.stripe_index as i64,
                chunk_id.chunk_index as i64,
                storage_location_json,
            ),
        )?;

        if rows_affected == 0 {
            return Err(MetadataError::ChunkNotFound { chunk_id });
        }

        Ok(())
    }

    /// Get all chunks for a stripe
    pub fn get_chunks_for_stripe(&self, stripe_id: StripeId) -> MetadataResult<Vec<ChunkMetadata>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT file_id, stripe_index, chunk_index, size, checksum, 
                   storage_location, created_at, last_verified
            FROM chunks WHERE file_id = ?1 AND stripe_index = ?2 ORDER BY chunk_index
            "#,
        )?;

        let chunk_iter = stmt.query_map(
            (stripe_id.file_id.to_string(), stripe_id.stripe_index as i64),
            parse_chunk_metadata,
        )?;
        let mut chunks = Vec::new();

        for chunk in chunk_iter {
            chunks.push(chunk?);
        }

        Ok(chunks)
    }

    /// Create a new stripe entry (using compound ID)
    pub fn create_stripe(
        &self,
        stripe_id: StripeId,
        metadata: StripeMetadata,
    ) -> MetadataResult<()> {
        // Verify the stripe_id matches the metadata
        if stripe_id != metadata.stripe_id() {
            return Err(MetadataError::InvalidMetadata {
                reason: "stripe_id mismatch with metadata".to_string(),
            });
        }

        let erasure_config_json = serde_json::to_string(&metadata.erasure_config)?;

        self.conn.execute(
            r#"
            INSERT INTO stripes (
                file_id, stripe_index, original_size, chunk_count, 
                erasure_config, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            (
                stripe_id.file_id.to_string(),
                stripe_id.stripe_index as i64,
                metadata.original_size as i64,
                metadata.chunk_count as i64,
                erasure_config_json,
                system_time_to_timestamp(metadata.created_at),
            ),
        )?;

        Ok(())
    }

    /// Get stripe metadata by compound ID
    pub fn get_stripe(&self, stripe_id: StripeId) -> MetadataResult<StripeMetadata> {
        let metadata = self
            .conn
            .query_row(
                r#"
            SELECT file_id, stripe_index, original_size, chunk_count, 
                   erasure_config, created_at
            FROM stripes WHERE file_id = ?1 AND stripe_index = ?2
            "#,
                (stripe_id.file_id.to_string(), stripe_id.stripe_index as i64),
                parse_stripe_metadata,
            )
            .optional()?;

        match metadata {
            Some(metadata) => Ok(metadata),
            None => Err(MetadataError::StripeNotFound { stripe_id }),
        }
    }

    /// Update stripe metadata
    pub fn update_stripe(
        &self,
        stripe_id: StripeId,
        metadata: StripeMetadata,
    ) -> MetadataResult<()> {
        if stripe_id != metadata.stripe_id() {
            return Err(MetadataError::InvalidMetadata {
                reason: "stripe_id mismatch".to_string(),
            });
        }

        let erasure_config_json = serde_json::to_string(&metadata.erasure_config)?;

        let rows_affected = self.conn.execute(
            r#"
            UPDATE stripes SET 
                original_size = ?3, chunk_count = ?4, erasure_config = ?5, created_at = ?6
            WHERE file_id = ?1 AND stripe_index = ?2
            "#,
            (
                stripe_id.file_id.to_string(),
                stripe_id.stripe_index as i64,
                metadata.original_size as i64,
                metadata.chunk_count as i64,
                erasure_config_json,
                system_time_to_timestamp(metadata.created_at),
            ),
        )?;

        if rows_affected == 0 {
            return Err(MetadataError::StripeNotFound { stripe_id });
        }

        Ok(())
    }

    /// Delete stripe metadata
    pub fn delete_stripe(&self, stripe_id: StripeId) -> MetadataResult<()> {
        let rows_affected = self.conn.execute(
            "DELETE FROM stripes WHERE file_id = ?1 AND stripe_index = ?2",
            (stripe_id.file_id.to_string(), stripe_id.stripe_index as i64),
        )?;

        if rows_affected == 0 {
            return Err(MetadataError::StripeNotFound { stripe_id });
        }

        Ok(())
    }

    /// Get all stripes for a file
    pub fn get_stripes_for_file(&self, file_id: Uuid) -> MetadataResult<Vec<StripeMetadata>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT file_id, stripe_index, original_size, chunk_count, 
                   erasure_config, created_at
            FROM stripes WHERE file_id = ?1 ORDER BY stripe_index
            "#,
        )?;

        let stripe_iter = stmt.query_map([file_id.to_string()], parse_stripe_metadata)?;
        let mut stripes = Vec::new();

        for stripe in stripe_iter {
            stripes.push(stripe?);
        }

        Ok(stripes)
    }

    /// Get database statistics
    pub fn get_stats(&self) -> MetadataResult<MetadataStats> {
        let file_count: u64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM files", [], |row| row.get::<_, i64>(0))?
            as u64;

        let chunk_count: u64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM chunks", [], |row| {
                row.get::<_, i64>(0)
            })? as u64;

        let stripe_count: u64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM stripes", [], |row| {
                row.get::<_, i64>(0)
            })? as u64;

        let total_size: u64 =
            self.conn
                .query_row("SELECT COALESCE(SUM(size), 0) FROM files", [], |row| {
                    row.get::<_, i64>(0)
                })? as u64;

        Ok(MetadataStats {
            file_count,
            chunk_count,
            stripe_count,
            total_size,
        })
    }
}

/// Database statistics
#[derive(Debug, Clone)]
pub struct MetadataStats {
    pub file_count: u64,
    pub chunk_count: u64,
    pub stripe_count: u64,
    pub total_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn create_test_store() -> (MetadataStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let store = MetadataStore::new(db_path).unwrap();
        (store, temp_dir)
    }

    fn create_test_file_metadata() -> FileMetadata {
        FileMetadata::new(PathBuf::from("/test/file.txt"), 1024, 0o644)
    }

    fn create_test_storage_location() -> StorageLocation {
        StorageLocation::new(
            Uuid::new_v4(),
            "disk1".to_string(),
            PathBuf::from("/storage/chunks/abc123"),
        )
    }

    #[test]
    fn test_compound_identifiers() {
        let file_id = Uuid::new_v4();
        let stripe_id = StripeId::new(file_id, 0);
        let chunk_id = ChunkId::new(file_id, 0, 5);

        // Test string representation
        assert_eq!(stripe_id.to_string(), format!("{}:0", file_id));
        assert_eq!(chunk_id.to_string(), format!("{}:0:5", file_id));

        // Test relationships
        assert_eq!(chunk_id.stripe_id(), stripe_id);
        assert_eq!(ChunkId::from_stripe(stripe_id, 5), chunk_id);
    }

    #[test]
    fn test_metadata_store_creation() {
        let (_store, _temp_dir) = create_test_store();
        // Store creation should succeed
    }

    #[test]
    fn test_file_operations() {
        let (store, _temp_dir) = create_test_store();
        let mut metadata = create_test_file_metadata();
        let file_id = metadata.file_id;

        // Test stripe_count optimization
        assert_eq!(metadata.stripe_count, 0);
        let stripe_index = metadata.add_stripe();
        assert_eq!(stripe_index, 0);
        assert_eq!(metadata.stripe_count, 1);

        // Create file
        let created_id = store.create_file(metadata.clone()).unwrap();
        assert_eq!(created_id, file_id);

        // Get file
        let retrieved = store.get_file(file_id).unwrap();
        assert_eq!(retrieved.file_id, file_id);
        assert_eq!(retrieved.path, metadata.path);
        assert_eq!(retrieved.size, metadata.size);
        assert_eq!(retrieved.stripe_count, 1);

        // Update file
        let mut updated_metadata = retrieved;
        updated_metadata.size = 2048;
        updated_metadata.touch_modify();
        store
            .update_file(file_id, updated_metadata.clone())
            .unwrap();

        let retrieved = store.get_file(file_id).unwrap();
        assert_eq!(retrieved.size, 2048);

        // List files
        let files = store.list_files().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].file_id, file_id);

        // Delete file
        store.delete_file(file_id).unwrap();
        assert!(store.get_file(file_id).is_err());
    }

    #[test]
    fn test_path_conflict() {
        let (store, _temp_dir) = create_test_store();
        let metadata1 = create_test_file_metadata();
        let metadata2 = create_test_file_metadata(); // Same path

        store.create_file(metadata1).unwrap();
        assert!(matches!(
            store.create_file(metadata2),
            Err(MetadataError::PathConflict { .. })
        ));
    }

    #[test]
    fn test_chunk_operations() {
        let (store, _temp_dir) = create_test_store();

        // Create a file first (required for foreign key constraint)
        let file_metadata = create_test_file_metadata();
        let file_id = file_metadata.file_id;
        store.create_file(file_metadata).unwrap();

        // Create a stripe (required for foreign key constraint)
        let config = crate::erasure_coding::ErasureCodingConfig::new(4, 2, 1024).unwrap();
        let stripe_id = StripeId::new(file_id, 0);
        let stripe_metadata = StripeMetadata::new(file_id, 0, 1000, config);
        store.create_stripe(stripe_id, stripe_metadata).unwrap();

        let location = create_test_storage_location();
        let chunk_id = ChunkId::new(file_id, 0, 0);

        let chunk_metadata = ChunkMetadata::new(file_id, 0, 0, 256, 0x12345678, location.clone());

        // Register chunk
        store
            .register_chunk(chunk_id, chunk_metadata.clone())
            .unwrap();

        // Get chunk
        let retrieved = store.get_chunk(chunk_id).unwrap();
        assert_eq!(retrieved.chunk_id(), chunk_id);
        assert_eq!(retrieved.stripe_id(), stripe_id);
        assert_eq!(retrieved.chunk_index, 0);

        // Update chunk location
        let new_location = StorageLocation::new(
            Uuid::new_v4(),
            "disk2".to_string(),
            PathBuf::from("/storage/new/path"),
        );
        store
            .update_chunk_location(chunk_id, new_location.clone())
            .unwrap();

        let retrieved = store.get_chunk(chunk_id).unwrap();
        assert_eq!(retrieved.storage_location, new_location);

        // Get chunks for stripe
        let chunks = store.get_chunks_for_stripe(stripe_id).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_id(), chunk_id);
    }

    #[test]
    fn test_stripe_operations() {
        let (store, _temp_dir) = create_test_store();

        // Create a file first (required for foreign key constraint)
        let file_metadata = create_test_file_metadata();
        let file_id = file_metadata.file_id;
        store.create_file(file_metadata).unwrap();

        let config = crate::erasure_coding::ErasureCodingConfig::new(4, 2, 1024).unwrap();
        let stripe_id = StripeId::new(file_id, 0);

        let mut stripe_metadata = StripeMetadata::new(file_id, 0, 1000, config);

        // Test chunk_count optimization
        assert_eq!(stripe_metadata.chunk_count, 0);
        let chunk_index = stripe_metadata.add_chunk();
        assert_eq!(chunk_index, 0);
        assert_eq!(stripe_metadata.chunk_count, 1);

        // Create stripe
        store
            .create_stripe(stripe_id, stripe_metadata.clone())
            .unwrap();

        // Get stripe
        let retrieved = store.get_stripe(stripe_id).unwrap();
        assert_eq!(retrieved.stripe_id(), stripe_id);
        assert_eq!(retrieved.file_id, file_id);
        assert_eq!(retrieved.original_size, 1000);
        assert_eq!(retrieved.chunk_count, 1);

        // Update stripe
        let mut updated_metadata = retrieved;
        updated_metadata.add_chunk();
        store
            .update_stripe(stripe_id, updated_metadata.clone())
            .unwrap();

        let retrieved = store.get_stripe(stripe_id).unwrap();
        assert_eq!(retrieved.chunk_count, 2);

        // Get stripes for file
        let stripes = store.get_stripes_for_file(file_id).unwrap();
        assert_eq!(stripes.len(), 1);
        assert_eq!(stripes[0].stripe_id(), stripe_id);

        // Delete stripe
        store.delete_stripe(stripe_id).unwrap();
        assert!(store.get_stripe(stripe_id).is_err());
    }

    #[test]
    fn test_stats() {
        let (store, _temp_dir) = create_test_store();

        // Initial stats should be zero
        let stats = store.get_stats().unwrap();
        assert_eq!(stats.file_count, 0);
        assert_eq!(stats.chunk_count, 0);
        assert_eq!(stats.stripe_count, 0);
        assert_eq!(stats.total_size, 0);

        // Add some data
        let file_metadata = create_test_file_metadata();
        store.create_file(file_metadata).unwrap();

        let stats = store.get_stats().unwrap();
        assert_eq!(stats.file_count, 1);
        assert_eq!(stats.total_size, 1024);
    }

    #[test]
    fn test_database_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("persistence_test.db");
        let file_metadata = create_test_file_metadata();
        let file_id = file_metadata.file_id;

        // Create store and add data
        {
            let store = MetadataStore::new(&db_path).unwrap();
            store.create_file(file_metadata).unwrap();
        }

        // Reopen store and verify data persists
        {
            let store = MetadataStore::new(&db_path).unwrap();
            let retrieved = store.get_file(file_id).unwrap();
            assert_eq!(retrieved.file_id, file_id);
        }
    }

    #[test]
    fn test_error_conditions() {
        let (store, _temp_dir) = create_test_store();
        let nonexistent_file_id = Uuid::new_v4();
        let nonexistent_stripe_id = StripeId::new(nonexistent_file_id, 0);
        let nonexistent_chunk_id = ChunkId::new(nonexistent_file_id, 0, 0);

        // File not found
        assert!(matches!(
            store.get_file(nonexistent_file_id),
            Err(MetadataError::FileNotFound { .. })
        ));

        // Chunk not found
        assert!(matches!(
            store.get_chunk(nonexistent_chunk_id),
            Err(MetadataError::ChunkNotFound { .. })
        ));

        // Stripe not found
        assert!(matches!(
            store.get_stripe(nonexistent_stripe_id),
            Err(MetadataError::StripeNotFound { .. })
        ));
    }

    #[test]
    fn test_metadata_size_optimization() {
        // Test the size benefits of compound identifiers
        let _file_id = Uuid::new_v4();

        // Old approach simulation: would need Vec<Uuid> for 100 stripes
        let old_stripe_ids: Vec<Uuid> = (0..100).map(|_| Uuid::new_v4()).collect();
        let old_stripe_ids_size = old_stripe_ids.len() * 16; // 1600 bytes

        // New approach: just a count
        let _new_stripe_count: u64 = 100;
        let new_stripe_count_size = std::mem::size_of::<u64>(); // 8 bytes

        // Demonstrate massive savings
        assert!(new_stripe_count_size < old_stripe_ids_size);
        println!(
            "Old approach: {} bytes, New approach: {} bytes, Savings: {} bytes",
            old_stripe_ids_size,
            new_stripe_count_size,
            old_stripe_ids_size - new_stripe_count_size
        );
    }
}
