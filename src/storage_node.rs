//! Storage Node Module
//!
//! This module implements the main WormFS storage node that orchestrates
//! file storage and retrieval operations using erasure coding, metadata storage,
//! and local chunk placement.

use crate::chunk_format::{read_chunk, write_chunk, ChunkError, ChunkHeader};
use crate::erasure_coding::{
    decode_stripe_with_size, encode_stripe, ErasureCodingConfig, ErasureError,
};
use crate::metadata_store::{
    ChunkId, ChunkMetadata, FileMetadata, MetadataError, MetadataStore, StorageLocation, StripeId,
    StripeMetadata,
};
use crate::storage_layout::{StorageLayout, StorageLayoutConfig, StorageLayoutError};

use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use thiserror::Error;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Configuration for the storage node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageNodeConfig {
    /// Node identifier
    pub node_id: Uuid,
    /// Path to the metadata database
    pub metadata_db_path: PathBuf,
    /// Storage root directory for chunks
    pub storage_root: PathBuf,
    /// Erasure coding configuration
    pub erasure_config: ErasureCodingConfig,
    /// Minimum free space required (in bytes)
    pub min_free_space: u64,
    /// Maximum file size to accept (in bytes)
    pub max_file_size: u64,
}

impl StorageNodeConfig {
    /// Create a new storage node configuration with defaults
    pub fn new() -> Result<Self, StorageNodeError> {
        Ok(Self {
            node_id: Uuid::new_v4(),
            metadata_db_path: PathBuf::from("data/metadata.db"),
            storage_root: PathBuf::from("data/chunks"),
            erasure_config: ErasureCodingConfig::preset_4_2()?,
            min_free_space: 1024 * 1024 * 1024,      // 1GB
            max_file_size: 100 * 1024 * 1024 * 1024, // 100GB
        })
    }

    /// Load configuration from a YAML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, StorageNodeError> {
        let content = fs::read_to_string(path)?;
        let config: Self = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// Save configuration to a YAML file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), StorageNodeError> {
        let content = serde_yaml::to_string(self)?;
        fs::write(path, content)?;
        Ok(())
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), StorageNodeError> {
        if self.min_free_space == 0 {
            return Err(StorageNodeError::InvalidConfiguration {
                reason: "min_free_space must be greater than 0".to_string(),
            });
        }

        if self.max_file_size == 0 {
            return Err(StorageNodeError::InvalidConfiguration {
                reason: "max_file_size must be greater than 0".to_string(),
            });
        }

        // Validate erasure config by accessing fields
        let _data_shards = self.erasure_config.data_shards;
        let _parity_shards = self.erasure_config.parity_shards;

        Ok(())
    }
}

impl Default for StorageNodeConfig {
    fn default() -> Self {
        Self::new().expect("Failed to create default configuration")
    }
}

/// Errors that can occur during storage node operations
#[derive(Error, Debug)]
pub enum StorageNodeError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Metadata error: {0}")]
    Metadata(#[from] MetadataError),

    #[error("Storage layout error: {0}")]
    StorageLayout(#[from] StorageLayoutError),

    #[error("Erasure coding error: {0}")]
    ErasureCoding(#[from] ErasureError),

    #[error("Chunk format error: {0}")]
    ChunkFormat(#[from] ChunkError),

    #[error("Configuration error: {0}")]
    Configuration(#[from] serde_yaml::Error),

    #[error("Invalid configuration: {reason}")]
    InvalidConfiguration { reason: String },

    #[error("File too large: {size} bytes exceeds maximum {max_size} bytes")]
    FileTooLarge { size: u64, max_size: u64 },

    #[error("Insufficient space: need {required} bytes, have {available} bytes")]
    InsufficientSpace { required: u64, available: u64 },

    #[error("File not found: {file_id}")]
    FileNotFound { file_id: Uuid },

    #[error("File already exists at path: {path:?}")]
    FileAlreadyExists { path: PathBuf },

    #[error("Chunk reconstruction failed: missing {missing} chunks, need {required}")]
    ChunkReconstructionFailed { missing: usize, required: usize },

    #[error("Data corruption detected: {details}")]
    DataCorruption { details: String },

    #[error("Invalid file path: {path:?}")]
    InvalidFilePath { path: PathBuf },
}

/// Result type for storage node operations
pub type StorageNodeResult<T> = Result<T, StorageNodeError>;

/// Information about a stored file
#[derive(Debug, Clone)]
pub struct StoredFileInfo {
    pub file_id: Uuid,
    pub path: PathBuf,
    pub size: u64,
    pub stripe_count: u64,
    pub chunk_count: u64,
    pub created_at: SystemTime,
    pub checksum: u32,
}

/// Statistics about the storage node
#[derive(Debug, Clone)]
pub struct StorageNodeStats {
    pub total_files: u64,
    pub total_chunks: u64,
    pub total_size: u64,
    pub available_space: u64,
    pub erasure_config: ErasureCodingConfig,
}

/// Main storage node implementation
pub struct StorageNode {
    config: StorageNodeConfig,
    metadata_store: MetadataStore,
    storage_layout: StorageLayout,
}

impl StorageNode {
    /// Create a new storage node with the given configuration
    pub fn new(config: StorageNodeConfig) -> StorageNodeResult<Self> {
        // Validate configuration
        config.validate()?;

        info!("Initializing storage node with ID: {}", config.node_id);

        // Ensure directory structure exists
        if let Some(parent) = config.metadata_db_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::create_dir_all(&config.storage_root)?;

        // Initialize metadata store
        debug!("Opening metadata database: {:?}", config.metadata_db_path);
        let metadata_store = MetadataStore::new(&config.metadata_db_path)?;

        // Initialize storage layout
        debug!("Initializing storage layout: {:?}", config.storage_root);
        let storage_layout_config = StorageLayoutConfig::new(&config.storage_root)
            .with_min_free_space(config.min_free_space)
            .with_auto_create_dirs(true);
        let storage_layout = StorageLayout::new(storage_layout_config)?;

        info!("Storage node initialized successfully");

        Ok(Self {
            config,
            metadata_store,
            storage_layout,
        })
    }

    /// Create a storage node from a configuration file
    pub fn from_config_file<P: AsRef<Path>>(config_path: P) -> StorageNodeResult<Self> {
        let config = StorageNodeConfig::from_file(config_path)?;
        Self::new(config)
    }

    /// Store a file in the storage system
    pub fn store_file<P: AsRef<Path>, Q: AsRef<Path>>(
        &self,
        source_path: P,
        virtual_path: Q,
    ) -> StorageNodeResult<Uuid> {
        let source_path = source_path.as_ref();
        let virtual_path = virtual_path.as_ref();

        info!("Storing file: {:?} -> {:?}", source_path, virtual_path);

        // Validate input file
        let file_metadata = fs::metadata(source_path)?;
        let file_size = file_metadata.len();

        if file_size > self.config.max_file_size {
            return Err(StorageNodeError::FileTooLarge {
                size: file_size,
                max_size: self.config.max_file_size,
            });
        }

        if file_size == 0 {
            return Err(StorageNodeError::InvalidFilePath {
                path: source_path.to_path_buf(),
            });
        }

        // Create file metadata
        let mut file_meta = FileMetadata::new(
            virtual_path.to_path_buf(),
            file_size,
            0o644, // Default permissions
        );

        let file_id = file_meta.file_id;

        // Calculate file checksum
        let file_checksum = self.calculate_file_checksum(source_path)?;
        file_meta.checksum = file_checksum;

        debug!(
            "File ID: {}, Size: {} bytes, Checksum: {:08x}",
            file_id, file_size, file_checksum
        );

        // Register file in metadata store first (required for foreign key constraints)
        self.metadata_store.create_file(file_meta.clone())?;

        // Create chunk folder
        let chunk_folder = self.storage_layout.create_chunk_folder(
            file_id,
            virtual_path.to_path_buf(),
            file_size,
        )?;

        // Open source file
        let mut source_file = File::open(source_path)?;
        let mut buffer = vec![0u8; self.config.erasure_config.stripe_size];
        let mut stripe_index = 0u64;
        let mut total_chunks_stored = 0u64;

        // Process file stripe by stripe
        loop {
            // Read next stripe
            let bytes_read = source_file.read(&mut buffer)?;
            if bytes_read == 0 {
                break; // End of file
            }

            let stripe_data = &buffer[..bytes_read];
            debug!("Processing stripe {}: {} bytes", stripe_index, bytes_read);

            // Encode stripe
            let encoded_stripe = encode_stripe(stripe_data, &self.config.erasure_config)?;

            // Create stripe metadata
            let stripe_id = StripeId::new(file_id, stripe_index);
            let stripe_metadata = StripeMetadata::new(
                file_id,
                stripe_index,
                bytes_read,
                self.config.erasure_config.clone(),
            );

            // Store stripe metadata first (required for foreign key constraints)
            self.metadata_store
                .create_stripe(stripe_id, stripe_metadata.clone())?;

            // Store each chunk
            for (chunk_index, chunk_data) in encoded_stripe.shards.iter().enumerate() {
                let chunk_id = ChunkId::new(file_id, stripe_index, chunk_index as u8);

                // Create chunk header
                let chunk_header = ChunkHeader::new(
                    Uuid::new_v4(), // Individual chunk UUID for header
                    stripe_id.file_id,
                    file_id,
                    stripe_index * self.config.erasure_config.stripe_size as u64,
                    (stripe_index * self.config.erasure_config.stripe_size as u64)
                        + bytes_read as u64,
                    chunk_index as u8,
                    self.config.erasure_config.data_shards as u8,
                    self.config.erasure_config.parity_shards as u8,
                    file_checksum,
                    crate::chunk_format::CompressionAlgorithm::None,
                )?;

                // Generate chunk filename
                let chunk_filename = format!("chunk_{}_{}.dat", stripe_index, chunk_index);
                let chunk_file_path = chunk_folder.chunk_file_path(&chunk_filename);

                // Write chunk to disk
                let mut chunk_file = BufWriter::new(File::create(&chunk_file_path)?);
                write_chunk(&mut chunk_file, chunk_header, chunk_data)?;
                chunk_file.flush()?;

                // Calculate chunk size on disk
                let chunk_size = fs::metadata(&chunk_file_path)?.len();

                // Create storage location
                let storage_location = StorageLocation::new(
                    self.config.node_id,
                    "default".to_string(), // Single disk for Phase 1A
                    chunk_file_path,
                );

                // Create chunk metadata
                let chunk_metadata = ChunkMetadata::new(
                    file_id,
                    stripe_index,
                    chunk_index as u8,
                    chunk_size,
                    crate::chunk_format::calculate_checksum(chunk_data),
                    storage_location,
                );

                // Register chunk in metadata store
                self.metadata_store
                    .register_chunk(chunk_id, chunk_metadata)?;

                debug!("Stored chunk {}: {} bytes", chunk_id, chunk_size);
                total_chunks_stored += 1;
            }

            // Update stripe count in file metadata and update in database
            file_meta.add_stripe();
            self.metadata_store
                .update_file(file_id, file_meta.clone())?;
            stripe_index += 1;
        }

        info!(
            "File stored successfully: {} stripes, {} chunks",
            stripe_index, total_chunks_stored
        );

        Ok(file_id)
    }

    /// Retrieve a file from the storage system
    pub fn retrieve_file<P: AsRef<Path>>(
        &self,
        file_id: Uuid,
        output_path: P,
    ) -> StorageNodeResult<()> {
        let output_path = output_path.as_ref();

        info!("Retrieving file: {} -> {:?}", file_id, output_path);

        // Get file metadata
        let file_metadata = self.metadata_store.get_file(file_id)?;
        debug!(
            "File metadata: {} bytes, {} stripes",
            file_metadata.size, file_metadata.stripe_count
        );

        // Create output file
        let mut output_file = BufWriter::new(File::create(output_path)?);
        let mut bytes_written = 0u64;

        // Process each stripe
        for stripe_index in 0..file_metadata.stripe_count {
            let stripe_id = StripeId::new(file_id, stripe_index);

            // Get stripe metadata
            let stripe_metadata = self.metadata_store.get_stripe(stripe_id)?;

            // Get chunks for this stripe
            let chunk_metadatas = self.metadata_store.get_chunks_for_stripe(stripe_id)?;
            debug!("Stripe {}: {} chunks", stripe_index, chunk_metadatas.len());

            // Read chunk data
            let mut chunks = vec![None; self.config.erasure_config.total_shards()];

            for chunk_metadata in chunk_metadatas {
                let chunk_file_path = &chunk_metadata.storage_location.path;

                // Read chunk from disk
                if chunk_file_path.exists() {
                    let mut chunk_file = BufReader::new(File::open(chunk_file_path)?);
                    let (chunk_header, chunk_data) = read_chunk(&mut chunk_file)?;

                    // Verify chunk integrity
                    crate::chunk_format::validate_chunk(&chunk_header, &chunk_data)?;

                    chunks[chunk_metadata.chunk_index as usize] = Some(chunk_data);
                    debug!(
                        "Read chunk {}: {} bytes",
                        chunk_metadata.chunk_index, chunk_metadata.size
                    );
                } else {
                    warn!("Chunk file missing: {:?}", chunk_file_path);
                }
            }

            // Reconstruct stripe
            let reconstructed_data = decode_stripe_with_size(
                &chunks,
                &stripe_metadata.erasure_config,
                stripe_metadata.original_size,
            )?;

            // Write stripe data to output file
            output_file.write_all(&reconstructed_data)?;
            bytes_written += reconstructed_data.len() as u64;

            debug!(
                "Reconstructed stripe {}: {} bytes",
                stripe_index,
                reconstructed_data.len()
            );
        }

        output_file.flush()?;

        // Verify total size
        if bytes_written != file_metadata.size {
            return Err(StorageNodeError::DataCorruption {
                details: format!(
                    "Size mismatch: expected {} bytes, wrote {} bytes",
                    file_metadata.size, bytes_written
                ),
            });
        }

        // Verify file checksum
        let output_checksum = self.calculate_file_checksum(output_path)?;
        if output_checksum != file_metadata.checksum {
            return Err(StorageNodeError::DataCorruption {
                details: format!(
                    "Checksum mismatch: expected {:08x}, got {:08x}",
                    file_metadata.checksum, output_checksum
                ),
            });
        }

        info!("File retrieved successfully: {} bytes", bytes_written);
        Ok(())
    }

    /// List all stored files
    pub fn list_files(&self) -> StorageNodeResult<Vec<StoredFileInfo>> {
        let file_metadatas = self.metadata_store.list_files()?;
        let mut file_infos = Vec::new();

        for file_metadata in file_metadatas {
            let chunk_count = self.calculate_chunk_count(&file_metadata)?;

            let file_info = StoredFileInfo {
                file_id: file_metadata.file_id,
                path: file_metadata.path,
                size: file_metadata.size,
                stripe_count: file_metadata.stripe_count,
                chunk_count,
                created_at: file_metadata.created_at,
                checksum: file_metadata.checksum,
            };

            file_infos.push(file_info);
        }

        Ok(file_infos)
    }

    /// Delete a file from the storage system
    pub fn delete_file(&self, file_id: Uuid) -> StorageNodeResult<()> {
        info!("Deleting file: {}", file_id);

        // Get file metadata first
        let file_metadata = self.metadata_store.get_file(file_id)?;

        // Delete chunk files
        for stripe_index in 0..file_metadata.stripe_count {
            let stripe_id = StripeId::new(file_id, stripe_index);
            let chunk_metadatas = self.metadata_store.get_chunks_for_stripe(stripe_id)?;

            for chunk_metadata in chunk_metadatas {
                let chunk_path = &chunk_metadata.storage_location.path;
                if chunk_path.exists() {
                    fs::remove_file(chunk_path)?;
                    debug!("Deleted chunk file: {:?}", chunk_path);
                }
            }
        }

        // Remove chunk folder if it exists
        if self.storage_layout.chunk_folder_exists(&file_metadata.path) {
            self.storage_layout
                .remove_chunk_folder(&file_metadata.path)?;
            debug!("Removed chunk folder for: {:?}", file_metadata.path);
        }

        // Delete metadata (this will cascade delete stripes and chunks due to foreign keys)
        self.metadata_store.delete_file(file_id)?;

        info!("File deleted successfully");
        Ok(())
    }

    /// Get storage node statistics
    pub fn get_stats(&self) -> StorageNodeResult<StorageNodeStats> {
        let metadata_stats = self.metadata_store.get_stats()?;
        let storage_stats = self.storage_layout.get_storage_stats()?;

        Ok(StorageNodeStats {
            total_files: metadata_stats.file_count,
            total_chunks: metadata_stats.chunk_count,
            total_size: metadata_stats.total_size,
            available_space: storage_stats.available_space,
            erasure_config: self.config.erasure_config.clone(),
        })
    }

    /// Get the node configuration
    pub fn config(&self) -> &StorageNodeConfig {
        &self.config
    }

    /// Calculate the checksum of a file
    fn calculate_file_checksum<P: AsRef<Path>>(&self, path: P) -> StorageNodeResult<u32> {
        let mut file = File::open(path)?;
        let mut buffer = [0u8; 8192];
        let mut hasher = crc32fast::Hasher::new();

        loop {
            let bytes_read = file.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }

        Ok(hasher.finalize())
    }

    /// Calculate the total number of chunks for a file
    fn calculate_chunk_count(&self, file_metadata: &FileMetadata) -> StorageNodeResult<u64> {
        let chunks_per_stripe = self.config.erasure_config.total_shards() as u64;
        Ok(file_metadata.stripe_count * chunks_per_stripe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_config() -> (StorageNodeConfig, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let root = temp_dir.path();

        let mut config = StorageNodeConfig::new().unwrap();
        config.metadata_db_path = root.join("metadata.db");
        config.storage_root = root.join("chunks");
        config.min_free_space = 1024;
        config.max_file_size = 10 * 1024 * 1024; // 10MB for tests

        (config, temp_dir)
    }

    fn create_test_file(dir: &Path, name: &str, content: &[u8]) -> PathBuf {
        let file_path = dir.join(name);
        let mut file = File::create(&file_path).unwrap();
        file.write_all(content).unwrap();
        file_path
    }

    #[test]
    fn test_storage_node_creation() {
        let (config, _temp_dir) = create_test_config();
        let storage_node = StorageNode::new(config).unwrap();

        // Basic functionality check
        let stats = storage_node.get_stats().unwrap();
        assert_eq!(stats.total_files, 0);
        assert_eq!(stats.total_chunks, 0);
    }

    #[test]
    fn test_store_and_retrieve_file() {
        let (config, temp_dir) = create_test_config();
        let storage_node = StorageNode::new(config).unwrap();

        // Create test file
        let test_data = b"Hello, WormFS! This is a test file for storage and retrieval.";
        let source_path = create_test_file(temp_dir.path(), "test.txt", test_data);
        let virtual_path = PathBuf::from("/test/file.txt");

        // Store file
        let file_id = storage_node
            .store_file(&source_path, &virtual_path)
            .unwrap();

        // Verify file was stored
        let files = storage_node.list_files().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].file_id, file_id);
        assert_eq!(files[0].path, virtual_path);
        assert_eq!(files[0].size, test_data.len() as u64);

        // Retrieve file
        let output_path = temp_dir.path().join("retrieved.txt");
        storage_node.retrieve_file(file_id, &output_path).unwrap();

        // Verify retrieved file
        let retrieved_data = fs::read(&output_path).unwrap();
        assert_eq!(retrieved_data, test_data);

        // Get stats
        let stats = storage_node.get_stats().unwrap();
        assert_eq!(stats.total_files, 1);
        assert!(stats.total_chunks > 0);
    }

    #[test]
    fn test_delete_file() {
        let (config, temp_dir) = create_test_config();
        let storage_node = StorageNode::new(config).unwrap();

        // Store a file
        let test_data = b"File to be deleted";
        let source_path = create_test_file(temp_dir.path(), "delete_test.txt", test_data);
        let file_id = storage_node
            .store_file(&source_path, "/delete/test.txt")
            .unwrap();

        // Verify file exists
        let files_before = storage_node.list_files().unwrap();
        assert_eq!(files_before.len(), 1);

        // Delete file
        storage_node.delete_file(file_id).unwrap();

        // Verify file is gone
        let files_after = storage_node.list_files().unwrap();
        assert_eq!(files_after.len(), 0);

        // Verify retrieval fails
        let output_path = temp_dir.path().join("should_not_exist.txt");
        let result = storage_node.retrieve_file(file_id, &output_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_large_file_rejection() {
        let (mut config, _temp_dir) = create_test_config();
        config.max_file_size = 100; // Very small limit

        let storage_node = StorageNode::new(config).unwrap();

        // Create file larger than limit
        let large_data = vec![0u8; 200];
        let temp_dir = TempDir::new().unwrap();
        let source_path = create_test_file(temp_dir.path(), "large.txt", &large_data);

        // Should reject large file
        let result = storage_node.store_file(&source_path, "/large.txt");
        assert!(matches!(result, Err(StorageNodeError::FileTooLarge { .. })));
    }

    #[test]
    fn test_config_serialization() {
        let config = StorageNodeConfig::new().unwrap();
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.yaml");

        // Save config
        config.save_to_file(&config_path).unwrap();

        // Load config
        let loaded_config = StorageNodeConfig::from_file(&config_path).unwrap();

        // Verify they match
        assert_eq!(config.node_id, loaded_config.node_id);
        assert_eq!(
            config.erasure_config.data_shards,
            loaded_config.erasure_config.data_shards
        );
    }

    #[test]
    fn test_empty_file_rejection() {
        let (config, temp_dir) = create_test_config();
        let storage_node = StorageNode::new(config).unwrap();

        // Create empty file
        let source_path = create_test_file(temp_dir.path(), "empty.txt", b"");

        // Should reject empty file
        let result = storage_node.store_file(&source_path, "/empty.txt");
        assert!(result.is_err());
    }
}
