//! FileSystemService implementation with MetadataStore integration.
//!
//! This module provides the concrete implementation of FileSystemService
//! that integrates with MetadataStore for metadata operations and prepares
//! for FileStore integration for data operations.

use super::inode::{InodeCache, InodeManager, ROOT_INODE};
use super::raft_commands::StorageRaftMemberStub;
use super::types::{ClientId, Config, DirEntry, Error, FileAttr, FileType, LockType, SetAttr};
use super::FileSystemService;
use crate::file_store::{FileStore, FileStoreImpl};
use crate::metadata_store::{FileId, FileMetadata, FileRecord, MetadataStore, MetadataStoreImpl};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

/// Concrete implementation of FileSystemService.
///
/// This implementation:
/// - Uses MetadataStore for file metadata operations
/// - Uses FileStore for chunk data operations (Phase 1: stub only)
/// - Routes metadata writes through RaftStub (Phase 1) / Raft (Phase 2+)
/// - Caches frequently accessed inodes for performance
pub struct FileSystemServiceImpl {
    /// Configuration
    config: Config,

    /// MetadataStore for file metadata
    metadata_store: MetadataStoreImpl,

    /// FileStore for chunk data (Phase 1: minimal use)
    file_store: Arc<FileStoreImpl>,

    /// Raft stub for metadata writes (Phase 1)
    raft_stub: Arc<StorageRaftMemberStub>,

    /// Inode management (allocation and caching)
    inode_manager: Arc<InodeManager>,
}

impl FileSystemServiceImpl {
    /// Create a new FileSystemServiceImpl.
    ///
    /// # Arguments
    ///
    /// * `config` - FileSystemService configuration
    /// * `metadata_store` - MetadataStore instance for metadata operations
    /// * `file_store` - FileStore instance for chunk operations
    pub fn new(
        config: Config,
        metadata_store: MetadataStoreImpl,
        file_store: Arc<FileStoreImpl>,
    ) -> Self {
        let inode_manager = Arc::new(InodeManager::new(
            config.inode_cache_size,
            config.inode_cache_ttl,
        ));

        Self {
            config,
            metadata_store,
            file_store,
            raft_stub: Arc::new(StorageRaftMemberStub::new()),
            inode_manager,
        }
    }

    /// Initialize the root directory if it doesn't exist.
    ///
    /// This should be called once during filesystem mount.
    pub async fn initialize_root(&self) -> Result<(), Error> {
        // Check if root already exists
        if let Ok(_) = self.metadata_store.get_file_by_inode(ROOT_INODE).await {
            // Root already exists
            return Ok(());
        }

        // Create root directory
        let root_metadata = FileMetadata {
            size: 0,
            permissions: 0o755,
            uid: self.config.uid,
            gid: self.config.gid,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
        };

        self.metadata_store
            .create_file(
                FileId::new(ROOT_INODE),
                Path::new("/"),
                ROOT_INODE,
                root_metadata,
            )
            .await
            .map_err(|e| Error::MetadataError(format!("Failed to create root directory: {}", e)))?;

        tracing::info!("Initialized root directory with inode {}", ROOT_INODE);
        Ok(())
    }

    /// Get the inode cache for external use (e.g., FuseAdapter).
    pub fn inode_cache(&self) -> Arc<InodeCache> {
        self.inode_manager.cache()
    }

    /// Get reference to metadata store (for FUSE adapter).
    pub fn metadata_store(&self) -> &MetadataStoreImpl {
        &self.metadata_store
    }

    /// Get reference to inode manager (for testing).
    pub fn inode_manager(&self) -> &Arc<InodeManager> {
        &self.inode_manager
    }

    /// Convert FileRecord to FileAttr for FUSE.
    fn file_record_to_attr(&self, record: &FileRecord) -> FileAttr {
        let kind = if record.path.to_str() == Some("/")
            || record
                .path
                .to_str()
                .map(|s| s.ends_with('/'))
                .unwrap_or(false)
        {
            FileType::Directory
        } else {
            FileType::RegularFile
        };

        FileAttr {
            ino: record.inode,
            size: record.size,
            blocks: (record.size + 4095) / 4096, // 4KB blocks
            atime: record.accessed_at,
            mtime: record.modified_at,
            ctime: record.modified_at, // SQLite doesn't have ctime
            crtime: record.created_at,
            kind,
            perm: record.permissions as u16,
            nlink: 1,
            uid: record.uid,
            gid: record.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    /// Helper to convert MetadataStore errors to FileSystemService errors.
    fn convert_metadata_error(&self, error: crate::metadata_store::Error) -> Error {
        match error {
            crate::metadata_store::Error::FileNotFound(msg) => {
                Error::NotFound(0) // We don't have inode in this error
            }
            crate::metadata_store::Error::FileAlreadyExists(path) => {
                Error::AlreadyExists(path.to_string_lossy().to_string())
            }
            crate::metadata_store::Error::ParentNotFound(path) => {
                Error::NotFound(0) // Parent directory
            }
            _ => Error::MetadataError(format!("{}", error)),
        }
    }
}

#[async_trait]
impl FileSystemService for FileSystemServiceImpl {
    // ===== File Operations =====

    async fn create(
        &self,
        parent: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        // Phase 1: Stub - will be implemented in Step 8
        Err(Error::NotSupported(
            "create not implemented in Step 7".into(),
        ))
    }

    async fn open(
        &self,
        _inode: u64,
        _flags: u32,
        _client_id: ClientId,
    ) -> Result<(u64, FileAttr), Error> {
        // Phase 1: Stub - will be implemented in Step 8
        Err(Error::NotSupported("open not implemented in Step 7".into()))
    }

    async fn read(
        &self,
        _inode: u64,
        _offset: u64,
        _size: u32,
        _client_id: ClientId,
    ) -> Result<Vec<u8>, Error> {
        // Phase 1: Stub - will be implemented in Step 8
        Err(Error::NotSupported("read not implemented in Step 7".into()))
    }

    async fn write(
        &self,
        _inode: u64,
        _offset: u64,
        _data: Vec<u8>,
        _client_id: ClientId,
    ) -> Result<u32, Error> {
        // Phase 1: Stub - will be implemented in Step 8
        Err(Error::NotSupported(
            "write not implemented in Step 7".into(),
        ))
    }

    async fn unlink(&self, _parent: u64, _name: &str, _client_id: ClientId) -> Result<(), Error> {
        // Phase 1: Stub - will be implemented in Step 8
        Err(Error::NotSupported(
            "unlink not implemented in Step 7".into(),
        ))
    }

    // ===== Directory Operations =====

    async fn mkdir(
        &self,
        _parent: u64,
        _name: &str,
        _mode: u32,
        _uid: u32,
        _gid: u32,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        // Phase 1: Stub - will be implemented in Step 9
        Err(Error::NotSupported(
            "mkdir not implemented in Step 7".into(),
        ))
    }

    async fn rmdir(&self, _parent: u64, _name: &str, _client_id: ClientId) -> Result<(), Error> {
        // Phase 1: Stub - will be implemented in Step 9
        Err(Error::NotSupported(
            "rmdir not implemented in Step 7".into(),
        ))
    }

    async fn readdir(
        &self,
        inode: u64,
        _offset: i64,
        _client_id: ClientId,
    ) -> Result<Vec<DirEntry>, Error> {
        // Get the directory's file record
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // List files in this directory
        let files = self
            .metadata_store
            .list_directory(&record.path)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Convert to DirEntry
        let mut entries = vec![
            // Add . and .. entries
            DirEntry {
                ino: inode,
                name: ".".to_string(),
                kind: FileType::Directory,
            },
            DirEntry {
                ino: inode, // TODO: Get actual parent inode
                name: "..".to_string(),
                kind: FileType::Directory,
            },
        ];

        for file in files {
            let name = file
                .path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();

            let kind = if file
                .path
                .to_str()
                .map(|s| s.ends_with('/'))
                .unwrap_or(false)
            {
                FileType::Directory
            } else {
                FileType::RegularFile
            };

            entries.push(DirEntry {
                ino: file.inode,
                name,
                kind,
            });
        }

        Ok(entries)
    }

    // ===== Metadata Operations =====

    async fn getattr(&self, inode: u64) -> Result<FileAttr, Error> {
        // Check cache first
        if let Some(cached) = self.inode_manager.cache().get(inode) {
            // Convert cached metadata to FileAttr
            // We need to query MetadataStore to get full FileRecord for path info
            // (cache only has FileMetadata, not path)
        }

        // Cache miss or need full record - query MetadataStore
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Update cache
        let metadata = FileMetadata {
            size: record.size,
            permissions: record.permissions,
            uid: record.uid,
            gid: record.gid,
            created_at: record.created_at,
            modified_at: record.modified_at,
            accessed_at: record.accessed_at,
        };
        self.inode_manager
            .cache()
            .insert(record.inode, record.file_id, metadata);

        Ok(self.file_record_to_attr(&record))
    }

    async fn setattr(
        &self,
        _inode: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<SystemTime>,
        _mtime: Option<SystemTime>,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        // Phase 1: Stub - will be implemented in Step 8
        Err(Error::NotSupported(
            "setattr not implemented in Step 7".into(),
        ))
    }

    // ===== Lock Operations =====

    async fn acquire_lock(
        &self,
        _inode: u64,
        _lock_type: LockType,
        _expires_at: SystemTime,
        _client_id: ClientId,
    ) -> Result<u64, Error> {
        // Phase 1: Stub - locks will be implemented in Step 8
        Err(Error::NotSupported(
            "locks not implemented in Step 7".into(),
        ))
    }

    async fn release_lock(&self, _inode: u64, _client_id: ClientId) -> Result<(), Error> {
        // Phase 1: Stub - locks will be implemented in Step 8
        Err(Error::NotSupported(
            "locks not implemented in Step 7".into(),
        ))
    }

    async fn extend_lock(
        &self,
        _inode: u64,
        _new_expiry: SystemTime,
        _client_id: ClientId,
    ) -> Result<(), Error> {
        // Phase 1: Stub - locks will be implemented in Step 8
        Err(Error::NotSupported(
            "locks not implemented in Step 7".into(),
        ))
    }
}

// Tests will be added via FUSE integration tests
#[cfg(test)]
mod tests {
    // TODO: Add unit tests once we have a public factory for MetadataStoreImpl
    // For now, FileSystemServiceImpl will be tested via FUSE integration tests

    /*
    use super::*;
    use crate::metadata_store::MetadataStore;
    use tempfile::TempDir;

    async fn create_test_service() -> (FileSystemServiceImpl, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("metadata.db");

        let metadata_config = crate::metadata_store::Config {
            database_path: db_path,
            ..Default::default()
        };

        let metadata_store = MetadataStoreImpl::new(metadata_config)
            .await
            .unwrap();
        metadata_store.initialize_schema().await.unwrap();

        let file_store_config = crate::file_store::types::Config {
            disk_paths: vec![temp_dir.path().to_path_buf()],
            max_chunk_size: 512,
            default_data_shards: 2,
            default_parity_shards: 1,
            max_concurrent_operations: 10,
            verification_interval: std::time::Duration::from_secs(3600),
            orphan_cleanup_age: std::time::Duration::from_secs(3600),
        };

        let file_store = Arc::new(FileStore::new(file_store_config).unwrap());

        let fs_config = Config {
            uid: 1000,
            gid: 1000,
            ..Default::default()
        };

        let service = FileSystemServiceImpl::new(fs_config, metadata_store, file_store);

        (service, temp_dir)
    }

    #[tokio::test]
    async fn test_initialize_root() {
        let (service, _temp_dir) = create_test_service().await;

        // Initialize root
        service.initialize_root().await.unwrap();

        // Should be able to get root attributes
        let attr = service.getattr(ROOT_INODE).await.unwrap();
        assert_eq!(attr.ino, ROOT_INODE);
        assert_eq!(attr.kind, FileType::Directory);
        assert_eq!(attr.perm, 0o755);
    }

    #[tokio::test]
    async fn test_initialize_root_idempotent() {
        let (service, _temp_dir) = create_test_service().await;

        // Initialize root twice - should not error
        service.initialize_root().await.unwrap();
        service.initialize_root().await.unwrap();

        // Root should still be accessible
        let attr = service.getattr(ROOT_INODE).await.unwrap();
        assert_eq!(attr.ino, ROOT_INODE);
    }

    #[tokio::test]
    async fn test_getattr_nonexistent() {
        let (service, _temp_dir) = create_test_service().await;
        service.initialize_root().await.unwrap();

        // Try to get attributes for non-existent inode
        let result = service.getattr(999).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_getattr_caching() {
        let (service, _temp_dir) = create_test_service().await;
        service.initialize_root().await.unwrap();

        // First getattr - cache miss
        let attr1 = service.getattr(ROOT_INODE).await.unwrap();
        assert_eq!(attr1.ino, ROOT_INODE);

        // Check cache has entry
        let cache = service.inode_cache();
        assert_eq!(cache.len(), 1);

        // Second getattr - should still work
        let attr2 = service.getattr(ROOT_INODE).await.unwrap();
        assert_eq!(attr2.ino, ROOT_INODE);
    }
    */
}
