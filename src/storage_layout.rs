//! Storage Layout Module
//!
//! This module implements local chunk storage organization for WormFS, including:
//! - Deterministic chunk folder hashing (10-char alphanumeric)
//! - 1000 top-level folder distribution
//! - Chunk index file format and operations
//! - Basic chunk placement logic (single disk for now)
//! - Directory creation and cleanup functions

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use thiserror::Error;
use uuid::Uuid;

use crate::metadata_store::{ChunkId, StripeId};

/// Number of top-level storage directories
pub const TOP_LEVEL_DIRS: u16 = 1000;

/// Length of the chunk folder hash
pub const FOLDER_HASH_LENGTH: usize = 10;

/// Name of the chunk index file in each chunk folder
pub const INDEX_FILE_NAME: &str = "chunk_index.json";

/// Errors that can occur during storage layout operations
#[derive(Error, Debug)]
pub enum StorageLayoutError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Invalid storage root path: {path:?}")]
    InvalidRootPath { path: PathBuf },

    #[error("Chunk folder not found: {folder_hash}")]
    ChunkFolderNotFound { folder_hash: String },

    #[error("Index file not found in folder: {folder_path:?}")]
    IndexFileNotFound { folder_path: PathBuf },

    #[error("Invalid folder hash: {hash} (expected length {expected})")]
    InvalidFolderHash { hash: String, expected: usize },

    #[error("Disk space insufficient: required {required} bytes, available {available} bytes")]
    InsufficientSpace { required: u64, available: u64 },

    #[error("Invalid file path: {path:?}")]
    InvalidFilePath { path: PathBuf },
}

/// Result type for storage layout operations
pub type StorageLayoutResult<T> = Result<T, StorageLayoutError>;

/// Configuration for storage layout
#[derive(Debug, Clone)]
pub struct StorageLayoutConfig {
    /// Root directory for chunk storage
    pub root_path: PathBuf,
    /// Minimum free space required before rejecting new chunks (in bytes)
    pub min_free_space: u64,
    /// Whether to automatically create missing directories
    pub auto_create_dirs: bool,
}

impl StorageLayoutConfig {
    /// Create a new storage layout configuration
    pub fn new<P: Into<PathBuf>>(root_path: P) -> Self {
        Self {
            root_path: root_path.into(),
            min_free_space: 1024 * 1024 * 1024, // 1GB default
            auto_create_dirs: true,
        }
    }

    /// Set minimum free space requirement
    pub fn with_min_free_space(mut self, bytes: u64) -> Self {
        self.min_free_space = bytes;
        self
    }

    /// Set auto-create directories flag
    pub fn with_auto_create_dirs(mut self, auto_create: bool) -> Self {
        self.auto_create_dirs = auto_create;
        self
    }
}

/// Statistics about a chunk folder
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkFolderStats {
    /// Number of chunks in this folder
    pub chunk_count: u64,
    /// Total size of all chunks in bytes
    pub total_size: u64,
    /// Last time this folder was updated
    pub last_updated: SystemTime,
}

impl ChunkFolderStats {
    /// Create new empty folder stats
    pub fn new() -> Self {
        Self {
            chunk_count: 0,
            total_size: 0,
            last_updated: SystemTime::now(),
        }
    }

    /// Add a chunk to the statistics
    pub fn add_chunk(&mut self, size: u64) {
        self.chunk_count += 1;
        self.total_size += size;
        self.last_updated = SystemTime::now();
    }

    /// Remove a chunk from the statistics
    pub fn remove_chunk(&mut self, size: u64) {
        if self.chunk_count > 0 {
            self.chunk_count -= 1;
            if self.total_size >= size {
                self.total_size -= size;
            } else {
                self.total_size = 0;
            }
        }
        self.last_updated = SystemTime::now();
    }
}

impl Default for ChunkFolderStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Index file contents for a chunk folder
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkIndexFile {
    /// File ID this chunk folder belongs to
    pub file_id: Uuid,
    /// Original file path
    pub file_path: PathBuf,
    /// File size in bytes
    pub file_size: u64,
    /// Statistics about chunks in this folder
    pub folder_stats: ChunkFolderStats,
    /// Map of chunk_id to chunk metadata
    pub chunks: HashMap<String, ChunkIndexEntry>,
    /// Creation time of this index
    pub created_at: SystemTime,
    /// Last modification time of this index
    pub modified_at: SystemTime,
}

impl ChunkIndexFile {
    /// Create a new index file
    pub fn new(file_id: Uuid, file_path: PathBuf, file_size: u64) -> Self {
        let now = SystemTime::now();
        Self {
            file_id,
            file_path,
            file_size,
            folder_stats: ChunkFolderStats::new(),
            chunks: HashMap::new(),
            created_at: now,
            modified_at: now,
        }
    }

    /// Add a chunk entry to the index
    pub fn add_chunk(&mut self, chunk_id: ChunkId, entry: ChunkIndexEntry) {
        self.chunks.insert(chunk_id.to_string(), entry.clone());
        self.folder_stats.add_chunk(entry.size);
        self.modified_at = SystemTime::now();
    }

    /// Remove a chunk entry from the index
    pub fn remove_chunk(&mut self, chunk_id: &ChunkId) -> Option<ChunkIndexEntry> {
        if let Some(entry) = self.chunks.remove(&chunk_id.to_string()) {
            self.folder_stats.remove_chunk(entry.size);
            self.modified_at = SystemTime::now();
            Some(entry)
        } else {
            None
        }
    }

    /// Get a chunk entry by ID
    pub fn get_chunk(&self, chunk_id: &ChunkId) -> Option<&ChunkIndexEntry> {
        self.chunks.get(&chunk_id.to_string())
    }

    /// List all chunk IDs in this folder
    pub fn list_chunk_ids(&self) -> Vec<ChunkId> {
        self.chunks.values().map(|entry| entry.chunk_id).collect()
    }

    /// Check if the index is empty (no chunks)
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }
}

/// Entry in the chunk index file
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkIndexEntry {
    /// Chunk ID
    pub chunk_id: ChunkId,
    /// Stripe ID this chunk belongs to
    pub stripe_id: StripeId,
    /// Size of the chunk file in bytes
    pub size: u64,
    /// CRC32 checksum of the chunk data
    pub checksum: u32,
    /// Relative path to the chunk file within the folder
    pub chunk_filename: String,
    /// Creation time
    pub created_at: SystemTime,
    /// Last verification time
    pub last_verified: Option<SystemTime>,
}

impl ChunkIndexEntry {
    /// Create a new chunk index entry
    pub fn new(
        chunk_id: ChunkId,
        stripe_id: StripeId,
        size: u64,
        checksum: u32,
        chunk_filename: String,
    ) -> Self {
        Self {
            chunk_id,
            stripe_id,
            size,
            checksum,
            chunk_filename,
            created_at: SystemTime::now(),
            last_verified: None,
        }
    }

    /// Mark this chunk as verified
    pub fn mark_verified(&mut self) {
        self.last_verified = Some(SystemTime::now());
    }
}

/// Represents a chunk folder on disk
#[derive(Debug, Clone)]
pub struct ChunkFolder {
    /// Path to the folder
    pub path: PathBuf,
    /// Hash used to identify this folder
    pub folder_hash: String,
    /// The index file for this folder
    pub index: ChunkIndexFile,
}

impl ChunkFolder {
    /// Create a new chunk folder
    pub fn new(path: PathBuf, folder_hash: String, index: ChunkIndexFile) -> Self {
        Self {
            path,
            folder_hash,
            index,
        }
    }

    /// Get the path to the index file
    pub fn index_file_path(&self) -> PathBuf {
        self.path.join(INDEX_FILE_NAME)
    }

    /// Get the path to a specific chunk file
    pub fn chunk_file_path(&self, chunk_filename: &str) -> PathBuf {
        self.path.join(chunk_filename)
    }

    /// Save the index file to disk
    pub fn save_index(&self) -> StorageLayoutResult<()> {
        let index_path = self.index_file_path();
        let json = serde_json::to_string_pretty(&self.index)?;
        fs::write(index_path, json)?;
        Ok(())
    }

    /// Load index file from disk
    pub fn load_index(folder_path: &Path) -> StorageLayoutResult<ChunkIndexFile> {
        let index_path = folder_path.join(INDEX_FILE_NAME);
        if !index_path.exists() {
            return Err(StorageLayoutError::IndexFileNotFound {
                folder_path: folder_path.to_path_buf(),
            });
        }

        let content = fs::read_to_string(index_path)?;
        let index: ChunkIndexFile = serde_json::from_str(&content)?;
        Ok(index)
    }
}

/// Main storage layout manager
#[derive(Debug)]
pub struct StorageLayout {
    config: StorageLayoutConfig,
}

impl StorageLayout {
    /// Create a new storage layout manager
    pub fn new(config: StorageLayoutConfig) -> StorageLayoutResult<Self> {
        // Validate root path
        if config.auto_create_dirs {
            let layout = Self { config };
            layout.ensure_root_structure()?;
            Ok(layout)
        } else if !config.root_path.exists() {
            Err(StorageLayoutError::InvalidRootPath {
                path: config.root_path.clone(),
            })
        } else {
            Ok(Self { config })
        }
    }

    /// Ensure the root directory structure exists
    pub fn ensure_root_structure(&self) -> StorageLayoutResult<()> {
        // Create root directory
        if !self.config.root_path.exists() {
            fs::create_dir_all(&self.config.root_path)?;
        }

        // Create top-level directories (001-1000)
        for i in 1..=TOP_LEVEL_DIRS {
            let dir_name = format!("{:03}", i);
            let dir_path = self.config.root_path.join(dir_name);
            if !dir_path.exists() {
                fs::create_dir_all(dir_path)?;
            }
        }

        Ok(())
    }

    /// Generate a 10-character alphanumeric hash for a file path
    pub fn generate_folder_hash<P: AsRef<Path>>(file_path: P) -> String {
        let path_str = file_path.as_ref().to_string_lossy();
        let mut hasher = Hasher::new();
        hasher.update(path_str.as_bytes());
        let hash = hasher.finalize();

        // Convert to base36 and pad/truncate to exactly 10 characters
        let base36 = base36_encode(hash as u64);
        if base36.len() >= FOLDER_HASH_LENGTH {
            base36[..FOLDER_HASH_LENGTH].to_string()
        } else {
            format!("{:0>width$}", base36, width = FOLDER_HASH_LENGTH)
        }
    }

    /// Calculate which top-level directory a folder hash should go in
    pub fn calculate_top_level_dir(folder_hash: &str) -> StorageLayoutResult<u16> {
        if folder_hash.len() != FOLDER_HASH_LENGTH {
            return Err(StorageLayoutError::InvalidFolderHash {
                hash: folder_hash.to_string(),
                expected: FOLDER_HASH_LENGTH,
            });
        }

        // Use the first 3 characters to determine directory (1-1000)
        let hash_prefix = &folder_hash[..3];
        let mut hasher = Hasher::new();
        hasher.update(hash_prefix.as_bytes());
        let hash_val = hasher.finalize();

        let dir_num = (hash_val % TOP_LEVEL_DIRS as u32) + 1;
        Ok(dir_num as u16)
    }

    /// Get the full path to a chunk folder
    pub fn get_chunk_folder_path<P: AsRef<Path>>(
        &self,
        file_path: P,
    ) -> StorageLayoutResult<PathBuf> {
        let folder_hash = Self::generate_folder_hash(file_path);
        let top_level_dir = Self::calculate_top_level_dir(&folder_hash)?;
        let dir_name = format!("{:03}", top_level_dir);

        Ok(self.config.root_path.join(dir_name).join(folder_hash))
    }

    /// Create a new chunk folder for a file
    pub fn create_chunk_folder(
        &self,
        file_id: Uuid,
        file_path: PathBuf,
        file_size: u64,
    ) -> StorageLayoutResult<ChunkFolder> {
        let folder_path = self.get_chunk_folder_path(&file_path)?;
        let folder_hash = Self::generate_folder_hash(&file_path);

        // Create the directory
        if !folder_path.exists() {
            fs::create_dir_all(&folder_path)?;
        }

        // Create index file
        let index = ChunkIndexFile::new(file_id, file_path, file_size);
        let chunk_folder = ChunkFolder::new(folder_path, folder_hash, index);

        // Save initial index
        chunk_folder.save_index()?;

        Ok(chunk_folder)
    }

    /// Load an existing chunk folder
    pub fn load_chunk_folder<P: AsRef<Path>>(
        &self,
        file_path: P,
    ) -> StorageLayoutResult<ChunkFolder> {
        let folder_path = self.get_chunk_folder_path(&file_path)?;
        let folder_hash = Self::generate_folder_hash(&file_path);

        if !folder_path.exists() {
            return Err(StorageLayoutError::ChunkFolderNotFound { folder_hash });
        }

        let index = ChunkFolder::load_index(&folder_path)?;
        Ok(ChunkFolder::new(folder_path, folder_hash, index))
    }

    /// Check if a chunk folder exists for a file
    pub fn chunk_folder_exists<P: AsRef<Path>>(&self, file_path: P) -> bool {
        if let Ok(folder_path) = self.get_chunk_folder_path(file_path) {
            folder_path.exists() && folder_path.join(INDEX_FILE_NAME).exists()
        } else {
            false
        }
    }

    /// List all chunk folders
    pub fn list_chunk_folders(&self) -> StorageLayoutResult<Vec<ChunkFolder>> {
        let mut folders = Vec::new();

        for i in 1..=TOP_LEVEL_DIRS {
            let dir_name = format!("{:03}", i);
            let top_level_path = self.config.root_path.join(dir_name);

            if !top_level_path.exists() {
                continue;
            }

            for entry in fs::read_dir(top_level_path)? {
                let entry = entry?;
                let folder_path = entry.path();

                if folder_path.is_dir() {
                    let index_path = folder_path.join(INDEX_FILE_NAME);
                    if index_path.exists() {
                        if let Ok(index) = ChunkFolder::load_index(&folder_path) {
                            let folder_hash = folder_path
                                .file_name()
                                .and_then(|n| n.to_str())
                                .unwrap_or("unknown")
                                .to_string();

                            folders.push(ChunkFolder::new(folder_path, folder_hash, index));
                        }
                    }
                }
            }
        }

        Ok(folders)
    }

    /// Remove a chunk folder and all its contents
    pub fn remove_chunk_folder<P: AsRef<Path>>(&self, file_path: P) -> StorageLayoutResult<()> {
        let folder_path = self.get_chunk_folder_path(file_path)?;

        if folder_path.exists() {
            fs::remove_dir_all(folder_path)?;
        }

        Ok(())
    }

    /// Clean up empty directories
    pub fn cleanup_empty_directories(&self) -> StorageLayoutResult<u32> {
        let mut removed_count = 0;

        for i in 1..=TOP_LEVEL_DIRS {
            let dir_name = format!("{:03}", i);
            let top_level_path = self.config.root_path.join(dir_name);

            if !top_level_path.exists() {
                continue;
            }

            // Check subdirectories within each top-level directory
            for entry in fs::read_dir(&top_level_path)? {
                let entry = entry?;
                let subfolder_path = entry.path();

                if subfolder_path.is_dir() {
                    // Check if directory is empty or only contains empty subdirectories
                    if is_directory_empty(&subfolder_path)? {
                        fs::remove_dir_all(subfolder_path)?;
                        removed_count += 1;
                    }
                }
            }

            // Remove top-level directory if it's empty (but keep the numbered structure)
            // We don't remove top-level dirs to maintain the 1-1000 structure
        }

        Ok(removed_count)
    }

    /// Get storage statistics
    pub fn get_storage_stats(&self) -> StorageLayoutResult<StorageStats> {
        let folders = self.list_chunk_folders()?;

        let mut total_chunks = 0;
        let mut total_size = 0;
        let folder_count = folders.len();

        for folder in folders {
            total_chunks += folder.index.folder_stats.chunk_count;
            total_size += folder.index.folder_stats.total_size;
        }

        // Get available space
        let available_space = get_available_space(&self.config.root_path)?;

        Ok(StorageStats {
            folder_count: folder_count as u64,
            total_chunks,
            total_size,
            available_space,
        })
    }

    /// Get the configuration
    pub fn config(&self) -> &StorageLayoutConfig {
        &self.config
    }
}

/// Storage statistics
#[derive(Debug, Clone)]
pub struct StorageStats {
    /// Number of chunk folders
    pub folder_count: u64,
    /// Total number of chunks across all folders
    pub total_chunks: u64,
    /// Total size of all chunks in bytes
    pub total_size: u64,
    /// Available space on the storage device in bytes
    pub available_space: u64,
}

/// Helper function to encode a number in base36
fn base36_encode(mut num: u64) -> String {
    if num == 0 {
        return "0".to_string();
    }

    const CHARS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut result = Vec::new();

    while num > 0 {
        result.push(CHARS[(num % 36) as usize]);
        num /= 36;
    }

    result.reverse();
    String::from_utf8(result).unwrap()
}

/// Helper function to check if a directory is empty (recursively)
fn is_directory_empty(path: &Path) -> StorageLayoutResult<bool> {
    let entries: Result<Vec<_>, _> = fs::read_dir(path)?.collect();
    let entries = entries?;

    if entries.is_empty() {
        return Ok(true);
    }

    // Check if all entries are empty directories
    for entry in entries {
        let entry_path = entry.path();
        if entry_path.is_file() || (entry_path.is_dir() && !is_directory_empty(&entry_path)?) {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Helper function to get available disk space
fn get_available_space(path: &Path) -> StorageLayoutResult<u64> {
    // This is a simplified implementation
    // In a real system, you'd use platform-specific APIs
    match fs::metadata(path) {
        Ok(_) => {
            // For now, return a large number as a placeholder
            // Real implementation would use statvfs on Unix or GetDiskFreeSpaceEx on Windows
            Ok(u64::MAX / 2) // Placeholder: assume plenty of space
        }
        Err(e) => Err(StorageLayoutError::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_config() -> (StorageLayoutConfig, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageLayoutConfig::new(temp_dir.path().to_path_buf())
            .with_min_free_space(1024)
            .with_auto_create_dirs(true);
        (config, temp_dir)
    }

    #[test]
    fn test_folder_hash_generation() {
        let path1 = PathBuf::from("/test/file1.txt");
        let path2 = PathBuf::from("/test/file2.txt");
        let path3 = PathBuf::from("/test/file1.txt"); // Same as path1

        let hash1 = StorageLayout::generate_folder_hash(&path1);
        let hash2 = StorageLayout::generate_folder_hash(&path2);
        let hash3 = StorageLayout::generate_folder_hash(&path3);

        // Hashes should be exactly 10 characters
        assert_eq!(hash1.len(), FOLDER_HASH_LENGTH);
        assert_eq!(hash2.len(), FOLDER_HASH_LENGTH);
        assert_eq!(hash3.len(), FOLDER_HASH_LENGTH);

        // Same paths should produce same hashes
        assert_eq!(hash1, hash3);

        // Different paths should produce different hashes (with very high probability)
        assert_ne!(hash1, hash2);

        // Hashes should be alphanumeric
        assert!(hash1.chars().all(|c| c.is_ascii_alphanumeric()));
        assert!(hash2.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn test_top_level_dir_calculation() {
        let hash1 = "abc1234567";
        let hash2 = "xyz9876543";

        let dir1 = StorageLayout::calculate_top_level_dir(hash1).unwrap();
        let dir2 = StorageLayout::calculate_top_level_dir(hash2).unwrap();

        // Should be in valid range
        assert!((1..=TOP_LEVEL_DIRS).contains(&dir1));
        assert!((1..=TOP_LEVEL_DIRS).contains(&dir2));

        // Same hash should always give same directory
        let dir1_repeat = StorageLayout::calculate_top_level_dir(hash1).unwrap();
        assert_eq!(dir1, dir1_repeat);

        // Different hashes should give different directories (with high probability)
        // Note: This might occasionally fail due to hash collisions, but very unlikely
        assert_ne!(dir1, dir2);
    }

    #[test]
    fn test_invalid_folder_hash() {
        let result = StorageLayout::calculate_top_level_dir("short");
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            StorageLayoutError::InvalidFolderHash { .. }
        ));
    }

    #[test]
    fn test_storage_layout_creation() {
        let (config, _temp_dir) = create_test_config();
        let layout = StorageLayout::new(config).unwrap();

        // Should create the directory structure
        for i in 1..=10 {
            // Test first 10 directories
            let dir_name = format!("{:03}", i);
            let dir_path = layout.config.root_path.join(dir_name);
            assert!(dir_path.exists());
        }
    }

    #[test]
    fn test_chunk_folder_operations() {
        let (config, _temp_dir) = create_test_config();
        let layout = StorageLayout::new(config).unwrap();

        let file_id = Uuid::new_v4();
        let file_path = PathBuf::from("/test/example.txt");
        let file_size = 1024;

        // Create chunk folder
        let chunk_folder = layout
            .create_chunk_folder(file_id, file_path.clone(), file_size)
            .unwrap();

        assert_eq!(chunk_folder.index.file_id, file_id);
        assert_eq!(chunk_folder.index.file_path, file_path);
        assert_eq!(chunk_folder.index.file_size, file_size);
        assert!(chunk_folder.path.exists());
        assert!(chunk_folder.index_file_path().exists());

        // Check that folder exists
        assert!(layout.chunk_folder_exists(&file_path));

        // Load existing folder
        let loaded_folder = layout.load_chunk_folder(&file_path).unwrap();
        assert_eq!(loaded_folder.index.file_id, file_id);
        assert_eq!(loaded_folder.index.file_path, file_path);

        // Remove folder
        layout.remove_chunk_folder(&file_path).unwrap();
        assert!(!layout.chunk_folder_exists(&file_path));
    }

    #[test]
    fn test_chunk_index_operations() {
        let file_id = Uuid::new_v4();
        let file_path = PathBuf::from("/test/example.txt");
        let mut index = ChunkIndexFile::new(file_id, file_path, 1024);

        let chunk_id = ChunkId::new(file_id, 0, 0);
        let stripe_id = StripeId::new(file_id, 0);
        let entry = ChunkIndexEntry::new(
            chunk_id,
            stripe_id,
            256,
            0x12345678,
            "chunk_0_0.dat".to_string(),
        );

        // Test adding chunk
        assert!(index.is_empty());
        index.add_chunk(chunk_id, entry.clone());
        assert!(!index.is_empty());
        assert_eq!(index.folder_stats.chunk_count, 1);
        assert_eq!(index.folder_stats.total_size, 256);

        // Test getting chunk
        let retrieved = index.get_chunk(&chunk_id).unwrap();
        assert_eq!(*retrieved, entry);

        // Test listing chunks
        let chunk_ids = index.list_chunk_ids();
        assert_eq!(chunk_ids.len(), 1);
        assert_eq!(chunk_ids[0], chunk_id);

        // Test removing chunk
        let removed = index.remove_chunk(&chunk_id).unwrap();
        assert_eq!(removed, entry);
        assert!(index.is_empty());
        assert_eq!(index.folder_stats.chunk_count, 0);
        assert_eq!(index.folder_stats.total_size, 0);
    }

    #[test]
    fn test_chunk_folder_stats() {
        let mut stats = ChunkFolderStats::new();
        assert_eq!(stats.chunk_count, 0);
        assert_eq!(stats.total_size, 0);

        stats.add_chunk(100);
        assert_eq!(stats.chunk_count, 1);
        assert_eq!(stats.total_size, 100);

        stats.add_chunk(200);
        assert_eq!(stats.chunk_count, 2);
        assert_eq!(stats.total_size, 300);

        stats.remove_chunk(100);
        assert_eq!(stats.chunk_count, 1);
        assert_eq!(stats.total_size, 200);

        stats.remove_chunk(300); // Remove more than available
        assert_eq!(stats.chunk_count, 0);
        assert_eq!(stats.total_size, 0); // Should not go negative
    }

    #[test]
    fn test_index_file_persistence() {
        let (config, _temp_dir) = create_test_config();
        let layout = StorageLayout::new(config).unwrap();

        let file_id = Uuid::new_v4();
        let file_path = PathBuf::from("/test/persistence.txt");
        let mut chunk_folder = layout
            .create_chunk_folder(file_id, file_path.clone(), 2048)
            .unwrap();

        // Add some chunks to the index
        let chunk_id1 = ChunkId::new(file_id, 0, 0);
        let chunk_id2 = ChunkId::new(file_id, 0, 1);
        let stripe_id = StripeId::new(file_id, 0);

        let entry1 = ChunkIndexEntry::new(
            chunk_id1,
            stripe_id,
            512,
            0x11111111,
            "chunk_0_0.dat".to_string(),
        );
        let entry2 = ChunkIndexEntry::new(
            chunk_id2,
            stripe_id,
            512,
            0x22222222,
            "chunk_0_1.dat".to_string(),
        );

        chunk_folder.index.add_chunk(chunk_id1, entry1);
        chunk_folder.index.add_chunk(chunk_id2, entry2);

        // Save the index
        chunk_folder.save_index().unwrap();

        // Load it back
        let loaded_index = ChunkFolder::load_index(&chunk_folder.path).unwrap();
        assert_eq!(loaded_index.file_id, file_id);
        assert_eq!(loaded_index.chunks.len(), 2);
        assert_eq!(loaded_index.folder_stats.chunk_count, 2);
        assert_eq!(loaded_index.folder_stats.total_size, 1024);
    }

    #[test]
    fn test_list_chunk_folders() {
        let (config, _temp_dir) = create_test_config();
        let layout = StorageLayout::new(config).unwrap();

        // Create several chunk folders
        let file_paths = vec![
            PathBuf::from("/test/file1.txt"),
            PathBuf::from("/test/file2.txt"),
            PathBuf::from("/different/path/file3.txt"),
        ];

        for (i, path) in file_paths.iter().enumerate() {
            layout
                .create_chunk_folder(Uuid::new_v4(), path.clone(), (i + 1) as u64 * 1024)
                .unwrap();
        }

        // List all folders
        let folders = layout.list_chunk_folders().unwrap();
        assert_eq!(folders.len(), 3);

        // Verify they have the correct file paths
        let found_paths: std::collections::HashSet<_> =
            folders.iter().map(|f| f.index.file_path.clone()).collect();
        let expected_paths: std::collections::HashSet<_> = file_paths.into_iter().collect();
        assert_eq!(found_paths, expected_paths);
    }

    #[test]
    fn test_cleanup_empty_directories() {
        let (config, _temp_dir) = create_test_config();
        let layout = StorageLayout::new(config).unwrap();

        // Create a chunk folder then remove it, leaving empty directory structure
        let file_path = PathBuf::from("/test/temporary.txt");
        layout
            .create_chunk_folder(Uuid::new_v4(), file_path.clone(), 1024)
            .unwrap();

        let folder_path = layout.get_chunk_folder_path(&file_path).unwrap();
        assert!(folder_path.exists());

        // Remove the folder contents manually
        fs::remove_dir_all(&folder_path).unwrap();

        // Create the empty directory back
        fs::create_dir_all(&folder_path).unwrap();
        assert!(folder_path.exists());

        // Cleanup should remove the empty directory
        let removed_count = layout.cleanup_empty_directories().unwrap();
        assert!(removed_count > 0);
        assert!(!folder_path.exists());
    }

    #[test]
    fn test_storage_stats() {
        let (config, _temp_dir) = create_test_config();
        let layout = StorageLayout::new(config).unwrap();

        // Initially should be empty
        let stats = layout.get_storage_stats().unwrap();
        assert_eq!(stats.folder_count, 0);
        assert_eq!(stats.total_chunks, 0);
        assert_eq!(stats.total_size, 0);

        // Create some folders with chunks
        let file_id = Uuid::new_v4();
        let file_path = PathBuf::from("/test/stats.txt");
        let mut chunk_folder = layout
            .create_chunk_folder(file_id, file_path, 1024)
            .unwrap();

        // Add some mock chunks to the index
        let chunk_id1 = ChunkId::new(file_id, 0, 0);
        let chunk_id2 = ChunkId::new(file_id, 0, 1);
        let stripe_id = StripeId::new(file_id, 0);

        chunk_folder.index.add_chunk(
            chunk_id1,
            ChunkIndexEntry::new(chunk_id1, stripe_id, 256, 0x111, "chunk1.dat".to_string()),
        );
        chunk_folder.index.add_chunk(
            chunk_id2,
            ChunkIndexEntry::new(chunk_id2, stripe_id, 512, 0x222, "chunk2.dat".to_string()),
        );

        chunk_folder.save_index().unwrap();

        // Check updated stats
        let stats = layout.get_storage_stats().unwrap();
        assert_eq!(stats.folder_count, 1);
        assert_eq!(stats.total_chunks, 2);
        assert_eq!(stats.total_size, 768);
        assert!(stats.available_space > 0);
    }

    #[test]
    fn test_base36_encoding() {
        assert_eq!(base36_encode(0), "0");
        assert_eq!(base36_encode(1), "1");
        assert_eq!(base36_encode(35), "z");
        assert_eq!(base36_encode(36), "10");
        assert_eq!(base36_encode(1295), "zz");
    }
}
