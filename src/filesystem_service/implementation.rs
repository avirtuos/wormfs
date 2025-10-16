//! FileSystemService implementation with MetadataStore integration.
//!
//! This module provides the concrete implementation of FileSystemService
//! that integrates with MetadataStore for metadata operations and prepares
//! for FileStore integration for data operations.

use super::inode::{InodeCache, InodeManager, ROOT_INODE};
use super::raft_commands::StorageRaftMemberStub;
use super::types::{
    ClientId, Config, DirEntry, Error, FileAttr, FileType, LockType, OpenFile, SetAttr,
};
use super::FileSystemService;
use crate::file_store::{FileStore, FileStoreImpl};
use crate::metadata_store::{FileId, FileMetadata, FileRecord, MetadataStore, MetadataStoreImpl};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
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

    /// Open file handles (file handle → open file state)
    open_files: Arc<RwLock<HashMap<u64, Arc<OpenFile>>>>,

    /// Next file handle to allocate
    next_file_handle: AtomicU64,
}

impl FileSystemServiceImpl {
    /// Create a new FileSystemServiceImpl.
    ///
    /// This constructor is crate-private and should only be called via
    /// `FileSystemServiceImplFactory::create()`. This ensures consistent
    /// initialization and proper dependency injection.
    ///
    /// # Arguments
    ///
    /// * `config` - FileSystemService configuration
    /// * `metadata_store` - MetadataStore instance for metadata operations
    /// * `file_store` - FileStore instance for chunk operations
    pub(crate) fn new(
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
            raft_stub: Arc::new(StorageRaftMemberStub::new(metadata_store.clone())),
            metadata_store,
            file_store,
            inode_manager,
            open_files: Arc::new(RwLock::new(HashMap::new())),
            next_file_handle: AtomicU64::new(1), // Start file handles at 1
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
            file_type: crate::metadata_store::FileType::Directory,
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
                super::inode::ROOT_FILE_ID,
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
        // Convert MetadataStore FileType to FileSystemService FileType
        let kind = match record.file_type {
            crate::metadata_store::FileType::Directory => FileType::Directory,
            crate::metadata_store::FileType::RegularFile => FileType::RegularFile,
            crate::metadata_store::FileType::Symlink => FileType::Symlink,
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

    /// Convert cached metadata to FileAttr for FUSE (cache-hit path).
    ///
    /// This method constructs a FileAttr directly from cached metadata without
    /// querying the database, providing significant performance benefits for hot files.
    fn cached_metadata_to_attr(&self, inode: u64, cached: &super::inode::CachedInode) -> FileAttr {
        // Convert MetadataStore FileType to FileSystemService FileType
        let kind = match cached.metadata.file_type {
            crate::metadata_store::FileType::Directory => FileType::Directory,
            crate::metadata_store::FileType::RegularFile => FileType::RegularFile,
            crate::metadata_store::FileType::Symlink => FileType::Symlink,
        };

        FileAttr {
            ino: inode,
            size: cached.metadata.size,
            blocks: (cached.metadata.size + 4095) / 4096, // 4KB blocks
            atime: cached.metadata.accessed_at,
            mtime: cached.metadata.modified_at,
            ctime: cached.metadata.modified_at, // Use mtime as ctime
            crtime: cached.metadata.created_at,
            kind,
            perm: cached.metadata.permissions as u16,
            nlink: 1,
            uid: cached.metadata.uid,
            gid: cached.metadata.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    /// Get default storage policy for Phase 1.
    ///
    /// In Phase 2+, this will be configurable per-file via storage policies.
    fn default_storage_policy(&self) -> crate::file_store::StoragePolicy {
        crate::file_store::StoragePolicy {
            data_shards: 2,
            parity_shards: 1,
            chunk_size: 2 * 1024 * 1024, // 2MB chunks = 4MB stripes (2 data shards)
            compression: crate::file_store::CompressionAlgorithm::None,
        }
    }

    /// Convert ChunkRecord from MetadataStore to ChunkMetadata for FileStore.
    fn chunk_record_to_metadata(
        &self,
        record: &crate::metadata_store::ChunkRecord,
    ) -> crate::file_store::ChunkMetadata {
        crate::file_store::ChunkMetadata {
            chunk_id: record.chunk_id,
            node_id: record.node_id,
            disk_id: record.disk_id,
            chunk_index: record.chunk_index,
        }
    }

    /// Helper to convert MetadataStore errors to FileSystemService errors.
    fn convert_metadata_error(&self, error: crate::metadata_store::Error) -> Error {
        match error {
            crate::metadata_store::Error::FileNotFoundByPath(path) => {
                // We don't have the inode for a path-based lookup failure
                Error::MetadataError(format!("File not found at path: {}", path))
            }
            crate::metadata_store::Error::FileNotFoundByInode(inode) => {
                // We have the inode - preserve it for better debugging
                Error::NotFound(inode)
            }
            crate::metadata_store::Error::FileNotFoundByFileId(file_id) => {
                // File ID is internal - convert to generic metadata error with context
                Error::MetadataError(format!("File not found with file_id: {:?}", file_id))
            }
            crate::metadata_store::Error::FileAlreadyExists(path) => {
                Error::AlreadyExists(path.to_string_lossy().to_string())
            }
            crate::metadata_store::Error::ParentNotFound(path) => {
                // Parent not found - provide path context
                Error::MetadataError(format!("Parent directory not found: {:?}", path))
            }
            crate::metadata_store::Error::LockConflict { file_id, lock_type } => {
                // Convert to simple lock conflict with context
                Error::LockConflictSimple(format!(
                    "Cannot acquire {} lock on file_id {:?}",
                    lock_type, file_id
                ))
            }
            crate::metadata_store::Error::LockNotFound { file_id, client_id } => {
                // Convert to simple lock not held with context
                Error::LockNotHeldSimple(format!(
                    "Lock not found for file_id {:?} and client {:?}",
                    file_id, client_id
                ))
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
        tracing::debug!("create: parent={}, name={}, mode={:o}", parent, name, mode);

        // Step 1: Validate parent exists and is a directory
        let parent_record = self
            .metadata_store
            .get_file_by_inode(parent)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        if parent_record.file_type != crate::metadata_store::FileType::Directory {
            return Err(Error::NotADirectory(parent));
        }

        // Step 2: Reserve inode before Raft operation
        let inode = self
            .metadata_store
            .reserve_inode()
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 3: Construct path
        let path = parent_record.path.join(name);

        // Step 4: Propose file creation through Raft stub
        use crate::filesystem_service::raft_commands::{FileType as RaftFileType, RaftCommand};
        let command = RaftCommand::CreateFile {
            parent_inode: parent,
            name: name.to_string(),
            file_type: RaftFileType::Regular,
            mode,
            uid,
            gid,
        };

        let result = self
            .raft_stub
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        // Step 5: Extract file_id from result (inode already reserved)
        let file_id = match result {
            crate::filesystem_service::raft_commands::RaftCommandResult::FileCreated {
                file_id,
                ..
            } => file_id,
            crate::filesystem_service::raft_commands::RaftCommandResult::Error { message } => {
                // Release the reserved inode on error
                let _ = self.metadata_store.release_inode(inode).await;
                return Err(Error::MetadataError(message));
            }
            _ => {
                let _ = self.metadata_store.release_inode(inode).await;
                return Err(Error::Internal("Unexpected Raft result for create".into()));
            }
        };

        // Step 6: [TEMP Phase 1] Write directly to MetadataStore
        // In Phase 2+, this will be handled by the Raft state machine
        let now = SystemTime::now();
        let metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::RegularFile,
            size: 0,
            permissions: mode,
            uid,
            gid,
            created_at: now,
            modified_at: now,
            accessed_at: now,
        };

        self.metadata_store
            .create_file(file_id, &path, inode, metadata.clone())
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 7: Confirm inode reservation
        self.metadata_store
            .confirm_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 8: Cache the inode
        self.inode_manager.cache().insert(inode, file_id, metadata);

        // Step 9: Return FileAttr
        let attr = FileAttr {
            ino: inode,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::RegularFile,
            perm: mode as u16,
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        };

        tracing::info!(
            "Created file: path={:?}, inode={}, file_id={:?}",
            path,
            inode,
            file_id
        );
        Ok(attr)
    }

    async fn open(
        &self,
        inode: u64,
        flags: u32,
        _client_id: ClientId,
    ) -> Result<(u64, FileAttr), Error> {
        tracing::debug!("open: inode={}, flags={}", inode, flags);

        // Step 1: Verify file exists and get metadata
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 2: Check file type (can't open directories with open())
        if record.file_type == crate::metadata_store::FileType::Directory {
            return Err(Error::IsADirectory(inode));
        }

        // Step 3: Generate unique file handle
        let file_handle = self.next_file_handle.fetch_add(1, Ordering::SeqCst);

        // Step 4: Create OpenFile state
        let open_file = Arc::new(OpenFile {
            file_id: record.file_id,
            inode,
            lock_id: None, // Locks handled separately
            flags: super::types::OpenFlags {
                read: (flags & libc::O_ACCMODE as u32) != libc::O_WRONLY as u32,
                write: (flags & libc::O_ACCMODE as u32) != libc::O_RDONLY as u32,
                append: (flags & libc::O_APPEND as u32) != 0,
                truncate: (flags & libc::O_TRUNC as u32) != 0,
                create: (flags & libc::O_CREAT as u32) != 0,
                exclusive: (flags & libc::O_EXCL as u32) != 0,
            },
            offset: AtomicU64::new(0),
            refcount: AtomicU32::new(1),
        });

        // Step 5: Track open file
        {
            let mut open_files = self.open_files.write().unwrap();
            open_files.insert(file_handle, open_file);
        }

        // Step 6: Handle O_TRUNC flag (truncate file to 0)
        if (flags & libc::O_TRUNC as u32) != 0 {
            // Update file size to 0 via Raft
            use crate::filesystem_service::raft_commands::{FileUpdateFields, RaftCommand};
            let command = RaftCommand::UpdateFile {
                inode,
                updates: FileUpdateFields {
                    size: Some(0),
                    mode: None,
                    uid: None,
                    gid: None,
                    atime: None,
                    mtime: Some(SystemTime::now()),
                },
            };

            let _result = self
                .raft_stub
                .propose_operation(command)
                .await
                .map_err(|e| Error::RaftError(format!("{}", e)))?;

            // [TEMP Phase 1] Update metadata store directly
            let mut updated_metadata = FileMetadata {
                file_type: record.file_type,
                size: 0, // Truncated
                permissions: record.permissions,
                uid: record.uid,
                gid: record.gid,
                created_at: record.created_at,
                modified_at: SystemTime::now(),
                accessed_at: record.accessed_at,
            };

            self.metadata_store
                .update_file(record.file_id, updated_metadata.clone())
                .await
                .map_err(|e| self.convert_metadata_error(e))?;

            // Invalidate cache
            self.inode_manager.cache().invalidate(inode);
        }

        // Step 7: Update access time via Raft
        use crate::filesystem_service::raft_commands::{FileUpdateFields, RaftCommand};
        let command = RaftCommand::UpdateFile {
            inode,
            updates: FileUpdateFields {
                size: None,
                mode: None,
                uid: None,
                gid: None,
                atime: Some(SystemTime::now()),
                mtime: None,
            },
        };

        let _result = self
            .raft_stub
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        // [TEMP Phase 1] Update metadata store directly
        let updated_metadata = FileMetadata {
            file_type: record.file_type,
            size: if (flags & libc::O_TRUNC as u32) != 0 {
                0
            } else {
                record.size
            },
            permissions: record.permissions,
            uid: record.uid,
            gid: record.gid,
            created_at: record.created_at,
            modified_at: record.modified_at,
            accessed_at: SystemTime::now(),
        };

        self.metadata_store
            .update_file(record.file_id, updated_metadata)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 8: Return file handle and attributes
        let attr = self.file_record_to_attr(&record);
        tracing::info!("Opened file: inode={}, handle={}", inode, file_handle);
        Ok((file_handle, attr))
    }

    async fn read(
        &self,
        inode: u64,
        offset: u64,
        size: u32,
        _client_id: ClientId,
    ) -> Result<Vec<u8>, Error> {
        tracing::debug!("read: inode={}, offset={}, size={}", inode, offset, size);

        // Step 1: Get file metadata (direct read - no Raft needed)
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 2: Bounds checking
        if offset >= record.size {
            // Reading past EOF returns empty
            tracing::debug!(
                "read: offset {} >= file size {}, returning empty",
                offset,
                record.size
            );
            return Ok(Vec::new());
        }

        // Clamp read size to available data
        let available = record.size - offset;
        let read_size = std::cmp::min(size as u64, available) as usize;

        eprintln!(
            "read: offset={}, size={}, file_size={}, available={}, read_size={}",
            offset, size, record.size, available, read_size
        );

        if read_size == 0 {
            return Ok(Vec::new());
        }

        // Step 3: Calculate stripe range
        // For Phase 1, we'll use a simple stripe size (this should come from storage policy)
        const STRIPE_SIZE: u64 = 4 * 1024 * 1024; // 4MB stripes

        let start_stripe_idx = offset / STRIPE_SIZE;
        let end_stripe_idx = (offset + read_size as u64 - 1) / STRIPE_SIZE;

        tracing::debug!(
            "read: reading stripes {} to {} (total: {})",
            start_stripe_idx,
            end_stripe_idx,
            end_stripe_idx - start_stripe_idx + 1
        );

        // Step 4: Read each stripe and accumulate data
        let mut result_data = Vec::with_capacity(read_size);

        for stripe_idx in start_stripe_idx..=end_stripe_idx {
            let stripe_offset = stripe_idx * STRIPE_SIZE;

            // Try to get stripe metadata from MetadataStore
            let stripe_result = self
                .metadata_store
                .get_stripe_at_offset(record.file_id, stripe_offset)
                .await;

            let stripe_data = match stripe_result {
                Ok(stripe) => {
                    // Stripe exists - read it from storage
                    let chunk_records = self
                        .metadata_store
                        .get_stripe_chunks(stripe.stripe_id)
                        .await
                        .map_err(|e| self.convert_metadata_error(e))?;

                    // Convert to ChunkMetadata for FileStore
                    let chunks: Vec<_> = chunk_records
                        .iter()
                        .map(|r| self.chunk_record_to_metadata(r))
                        .collect();

                    // Read and decode stripe from FileStore
                    self.file_store
                        .read_stripe(record.file_id, stripe.stripe_id, chunks)
                        .await
                        .map_err(|e| Error::DataFailed(format!("Failed to read stripe: {}", e)))?
                }
                Err(_) => {
                    // Stripe doesn't exist - this is a sparse region
                    // Return zeros (POSIX sparse file semantics)
                    tracing::debug!(
                        "read: stripe {} doesn't exist, returning zeros (sparse region)",
                        stripe_idx
                    );
                    vec![0u8; STRIPE_SIZE as usize]
                }
            };

            // Calculate which part of this stripe we need
            let stripe_start = if stripe_idx == start_stripe_idx {
                (offset % STRIPE_SIZE) as usize
            } else {
                0
            };

            let stripe_end = if stripe_idx == end_stripe_idx {
                let end_offset_in_stripe =
                    ((offset + read_size as u64 - 1) % STRIPE_SIZE) as usize + 1;
                std::cmp::min(end_offset_in_stripe, stripe_data.len())
            } else {
                stripe_data.len()
            };

            eprintln!(
                "read: stripe {}: stripe_start={}, stripe_end={}, stripe_data.len()={}, result_data.len()={}",
                stripe_idx,
                stripe_start,
                stripe_end,
                stripe_data.len(),
                result_data.len()
            );

            // Extract the needed slice
            if stripe_start < stripe_data.len() {
                let slice_end = std::cmp::min(stripe_end, stripe_data.len());
                eprintln!(
                    "read: stripe {}: extracting slice [{}..{}], adding {} bytes",
                    stripe_idx,
                    stripe_start,
                    slice_end,
                    slice_end - stripe_start
                );
                result_data.extend_from_slice(&stripe_data[stripe_start..slice_end]);
            } else {
                eprintln!(
                    "read: stripe {}: stripe_start {} >= stripe_data.len() {}, skipping",
                    stripe_idx,
                    stripe_start,
                    stripe_data.len()
                );
            }
        }

        // Step 5: Update access time via Raft
        use crate::filesystem_service::raft_commands::{FileUpdateFields, RaftCommand};
        let command = RaftCommand::UpdateFile {
            inode,
            updates: FileUpdateFields {
                size: None,
                mode: None,
                uid: None,
                gid: None,
                atime: Some(SystemTime::now()),
                mtime: None,
            },
        };

        // Fire and forget - don't block read on this
        let raft_stub = Arc::clone(&self.raft_stub);
        let metadata_store = self.metadata_store.clone();
        let file_id = record.file_id;
        let file_type = record.file_type;
        let size = record.size;
        let permissions = record.permissions;
        let uid = record.uid;
        let gid = record.gid;
        let created_at = record.created_at;
        let modified_at = record.modified_at;

        tokio::spawn(async move {
            if let Err(e) = raft_stub.propose_operation(command).await {
                tracing::warn!("Failed to update access time via Raft: {}", e);
            }

            // [TEMP Phase 1] Update metadata store directly
            let updated_metadata = FileMetadata {
                file_type,
                size,
                permissions,
                uid,
                gid,
                created_at,
                modified_at,
                accessed_at: SystemTime::now(),
            };

            if let Err(e) = metadata_store.update_file(file_id, updated_metadata).await {
                tracing::warn!("Failed to update access time in metadata store: {}", e);
            }
        });

        tracing::debug!("read: returning {} bytes", result_data.len());
        Ok(result_data)
    }

    async fn write(
        &self,
        inode: u64,
        offset: u64,
        data: Vec<u8>,
        _client_id: ClientId,
    ) -> Result<u32, Error> {
        tracing::debug!(
            "write: inode={}, offset={}, size={}",
            inode,
            offset,
            data.len()
        );

        if data.is_empty() {
            return Ok(0);
        }

        // Step 1: Get file metadata (direct read)
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 2: Calculate new file size
        let new_size = std::cmp::max(record.size, offset + data.len() as u64);

        // Step 3: Calculate stripe range
        const STRIPE_SIZE: u64 = 4 * 1024 * 1024; // 4MB stripes

        let start_stripe_idx = offset / STRIPE_SIZE;
        let end_stripe_idx = (offset + data.len() as u64 - 1) / STRIPE_SIZE;

        tracing::debug!(
            "write: writing stripes {} to {} (total: {})",
            start_stripe_idx,
            end_stripe_idx,
            end_stripe_idx - start_stripe_idx + 1
        );

        // Step 4: Process each stripe (DATA PLANE - not via Raft)
        let mut data_offset = 0;
        let mut stripe_metadata_updates = Vec::new();

        for stripe_idx in start_stripe_idx..=end_stripe_idx {
            let stripe_offset = stripe_idx * STRIPE_SIZE;

            // Calculate what portion of data goes into this stripe
            let stripe_start = if stripe_idx == start_stripe_idx {
                (offset % STRIPE_SIZE) as usize
            } else {
                0
            };

            let stripe_end = if stripe_idx == end_stripe_idx {
                ((offset + data.len() as u64 - 1) % STRIPE_SIZE) as usize + 1
            } else {
                STRIPE_SIZE as usize
            };

            let data_len = stripe_end - stripe_start;
            let stripe_data_slice = &data[data_offset..data_offset + data_len];
            data_offset += data_len;

            // Check if this is a partial stripe write (requires read-modify-write)
            let is_partial = stripe_start > 0 || stripe_end < STRIPE_SIZE as usize;

            tracing::debug!(
                "write: stripe {} - start={}, end={}, partial={}",
                stripe_idx,
                stripe_start,
                stripe_end,
                is_partial
            );

            if is_partial && stripe_offset < record.size {
                // Read-modify-write for existing partial stripe

                // Get existing stripe
                let existing_stripe = self
                    .metadata_store
                    .get_stripe_at_offset(record.file_id, stripe_offset)
                    .await;

                match existing_stripe {
                    Ok(stripe_meta) => {
                        // Get chunks for existing stripe
                        let chunk_records = self
                            .metadata_store
                            .get_stripe_chunks(stripe_meta.stripe_id)
                            .await
                            .map_err(|e| self.convert_metadata_error(e))?;

                        // Convert to ChunkMetadata for FileStore
                        let chunks: Vec<_> = chunk_records
                            .iter()
                            .map(|r| self.chunk_record_to_metadata(r))
                            .collect();

                        // Update stripe with new data
                        let policy = self.default_storage_policy();
                        let updated_stripe = self
                            .file_store
                            .update_stripe_partial(
                                record.file_id,
                                stripe_meta.stripe_id,
                                stripe_offset,
                                chunks,
                                stripe_start as u64,
                                stripe_data_slice.to_vec(),
                                policy,
                            )
                            .await
                            .map_err(|e| {
                                Error::DataFailed(format!("Failed to update stripe: {}", e))
                            })?;

                        stripe_metadata_updates.push(updated_stripe);
                    }
                    Err(_) => {
                        // Stripe doesn't exist yet - write new stripe
                        use crate::file_store::StripeId;
                        let stripe_id = StripeId::generate();
                        let policy = self.default_storage_policy();

                        let new_stripe = self
                            .file_store
                            .write_stripe(
                                record.file_id,
                                stripe_id,
                                stripe_offset,
                                stripe_data_slice.to_vec(),
                                policy,
                            )
                            .await
                            .map_err(|e| {
                                Error::DataFailed(format!("Failed to write stripe: {}", e))
                            })?;

                        stripe_metadata_updates.push(new_stripe);
                    }
                }
            } else {
                // Full stripe write - no read needed
                use crate::file_store::StripeId;
                let stripe_id = StripeId::generate();
                let policy = self.default_storage_policy();

                let new_stripe = self
                    .file_store
                    .write_stripe(
                        record.file_id,
                        stripe_id,
                        stripe_offset,
                        stripe_data_slice.to_vec(),
                        policy,
                    )
                    .await
                    .map_err(|e| Error::DataFailed(format!("Failed to write stripe: {}", e)))?;

                stripe_metadata_updates.push(new_stripe);
            }
        }

        // Step 5: Update metadata via Raft (CONTROL PLANE)
        use crate::filesystem_service::raft_commands::{FileUpdateFields, RaftCommand};

        // Update file size and mtime
        let command = RaftCommand::UpdateFile {
            inode,
            updates: FileUpdateFields {
                size: Some(new_size),
                mode: None,
                uid: None,
                gid: None,
                atime: None,
                mtime: Some(SystemTime::now()),
            },
        };

        let _result = self
            .raft_stub
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        // [TEMP Phase 1] Update metadata store directly
        let updated_metadata = FileMetadata {
            file_type: record.file_type,
            size: new_size,
            permissions: record.permissions,
            uid: record.uid,
            gid: record.gid,
            created_at: record.created_at,
            modified_at: SystemTime::now(),
            accessed_at: record.accessed_at,
        };

        self.metadata_store
            .update_file(record.file_id, updated_metadata)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 6: Update stripe metadata via Raft
        // The Raft stub now handles all metadata persistence (stripes + chunks)
        for stripe_meta in stripe_metadata_updates {
            let command = RaftCommand::UpdateStripe {
                file_id: record.file_id,
                stripe_id: stripe_meta.stripe_id,
                metadata: stripe_meta.clone(),
            };

            self.raft_stub
                .propose_operation(command)
                .await
                .map_err(|e| Error::RaftError(format!("{}", e)))?;
        }

        // Step 7: Invalidate cache
        self.inode_manager.cache().invalidate(inode);

        tracing::info!(
            "Wrote {} bytes to inode {} at offset {}",
            data.len(),
            inode,
            offset
        );
        Ok(data.len() as u32)
    }

    async fn unlink(&self, parent: u64, name: &str, _client_id: ClientId) -> Result<(), Error> {
        tracing::debug!("unlink: parent={}, name={}", parent, name);

        // Step 1: Verify parent exists and is a directory
        let parent_record = self
            .metadata_store
            .get_file_by_inode(parent)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        if parent_record.file_type != crate::metadata_store::FileType::Directory {
            return Err(Error::NotADirectory(parent));
        }

        // Step 2: Construct path and lookup file
        let path = parent_record.path.join(name);
        let file_record = self
            .metadata_store
            .get_file_by_path(&path)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 3: Check file type (can't unlink directories - use rmdir)
        if file_record.file_type == crate::metadata_store::FileType::Directory {
            return Err(Error::IsADirectory(file_record.inode));
        }

        // Step 4: Check if file is open (for deferred deletion)
        let is_open = {
            let open_files = self.open_files.read().unwrap();
            open_files.values().any(|f| f.inode == file_record.inode)
        };

        if is_open {
            tracing::info!(
                "File {} is open, deferring deletion until all handles are closed",
                name
            );
            // In a full implementation, we would mark the file for deferred deletion
            // For Phase 1, we'll proceed with deletion but log a warning
            tracing::warn!("Deferred deletion not fully implemented in Phase 1");
        }

        // Step 5: Propose file deletion through Raft
        use crate::filesystem_service::raft_commands::RaftCommand;
        let command = RaftCommand::DeleteFile {
            parent_inode: parent,
            name: name.to_string(),
        };

        let result = self
            .raft_stub
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        match result {
            crate::filesystem_service::raft_commands::RaftCommandResult::FileDeleted => {}
            crate::filesystem_service::raft_commands::RaftCommandResult::Error { message } => {
                return Err(Error::MetadataError(message));
            }
            _ => {
                return Err(Error::Internal("Unexpected Raft result for unlink".into()));
            }
        }

        // Step 6: [TEMP Phase 1] Delete from MetadataStore directly
        self.metadata_store
            .delete_file(file_record.file_id)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 7: Queue async chunk cleanup (DATA PLANE)
        let file_id = file_record.file_id;
        let file_store = Arc::clone(&self.file_store);
        let metadata_store = self.metadata_store.clone();

        tokio::spawn(async move {
            tracing::debug!("Starting async cleanup for file_id {:?}", file_id);

            // Get all stripes for the file
            match metadata_store.get_file_stripes(file_id).await {
                Ok(stripes) => {
                    for stripe in stripes {
                        // Get chunks for this stripe
                        match metadata_store.get_stripe_chunks(stripe.stripe_id).await {
                            Ok(chunks) => {
                                // Delete each chunk
                                // TODO: Implement delete_chunk in FileStore for Phase 2
                                // For Phase 1, chunks will be orphaned (handled by StorageWatchdog)
                                tracing::debug!(
                                    "Would delete {} chunks for stripe {:?} (deferred to StorageWatchdog)",
                                    chunks.len(),
                                    stripe.stripe_id
                                );
                                // for chunk in chunks {
                                //     if let Err(e) = file_store.delete_chunk(chunk.chunk_id).await {
                                //         tracing::warn!(
                                //             "Failed to delete chunk {:?}: {}",
                                //             chunk.chunk_id,
                                //             e
                                //         );
                                //     }
                                // }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to get chunks for stripe {:?}: {}",
                                    stripe.stripe_id,
                                    e
                                );
                            }
                        }
                    }
                    tracing::info!("Completed cleanup for file_id {:?}", file_id);
                }
                Err(e) => {
                    tracing::warn!("Failed to get stripes for file_id {:?}: {}", file_id, e);
                }
            }
        });

        // Step 8: Invalidate cache
        self.inode_manager.cache().invalidate(file_record.inode);

        tracing::info!(
            "Unlinked file: path={:?}, inode={}",
            path,
            file_record.inode
        );
        Ok(())
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

        // Determine parent inode
        // For root directory ("/"), parent is itself
        // For all other directories, look up parent by parent_path
        let parent_inode = if inode == ROOT_INODE {
            ROOT_INODE
        } else {
            // Look up parent directory by parent_path
            match self
                .metadata_store
                .get_file_by_path(&record.parent_path)
                .await
            {
                Ok(parent_record) => parent_record.inode,
                Err(_) => {
                    // If parent lookup fails, fall back to root
                    // This shouldn't happen in a consistent filesystem, but provides safety
                    tracing::warn!(
                        "Failed to find parent directory for path {:?}, falling back to root",
                        record.path
                    );
                    ROOT_INODE
                }
            }
        };

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
                ino: parent_inode,
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

            // Convert MetadataStore FileType to FileSystemService FileType
            let kind = match file.file_type {
                crate::metadata_store::FileType::Directory => FileType::Directory,
                crate::metadata_store::FileType::RegularFile => FileType::RegularFile,
                crate::metadata_store::FileType::Symlink => FileType::Symlink,
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
        // Check cache first - use it if available
        if let Some(cached) = self.inode_manager.cache().get(inode) {
            return Ok(self.cached_metadata_to_attr(inode, &cached));
        }

        // Cache miss - query MetadataStore
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Update cache with fresh data from database
        let metadata = FileMetadata {
            file_type: record.file_type,
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
        inode: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        tracing::debug!("setattr: inode={}, mode={:?}, size={:?}", inode, mode, size);

        // Step 1: Get current metadata
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 2: Handle truncation if size is changing (DATA PLANE)
        if let Some(new_size) = size {
            if new_size < record.size {
                // Shrinking - need to delete/truncate stripes
                const STRIPE_SIZE: u64 = 4 * 1024 * 1024; // 4MB stripes

                let new_last_stripe_idx = if new_size == 0 {
                    0
                } else {
                    (new_size - 1) / STRIPE_SIZE
                };

                let old_last_stripe_idx = if record.size == 0 {
                    0
                } else {
                    (record.size - 1) / STRIPE_SIZE
                };

                tracing::debug!(
                    "setattr: truncating from {} to {} bytes (stripes {} to {})",
                    record.size,
                    new_size,
                    old_last_stripe_idx,
                    new_last_stripe_idx
                );

                // Get all stripes
                let stripes = self
                    .metadata_store
                    .get_file_stripes(record.file_id)
                    .await
                    .map_err(|e| self.convert_metadata_error(e))?;

                // Delete stripes beyond the new size
                for stripe in stripes {
                    let stripe_idx = stripe.offset / STRIPE_SIZE;

                    if stripe_idx > new_last_stripe_idx {
                        // Delete entire stripe (metadata and chunks)
                        tracing::debug!(
                            "setattr: deleting stripe at index {} (stripe_id={:?})",
                            stripe_idx,
                            stripe.stripe_id
                        );

                        // Delete stripe metadata and associated chunks from database
                        // Physical chunk deletion is deferred to StorageWatchdog (Phase 1)
                        self.metadata_store
                            .delete_stripe(stripe.stripe_id)
                            .await
                            .map_err(|e| self.convert_metadata_error(e))?;

                        tracing::debug!(
                            "setattr: deleted stripe metadata for stripe {}",
                            stripe_idx
                        );
                    } else if stripe_idx == new_last_stripe_idx && new_size % STRIPE_SIZE != 0 {
                        // Partial truncation of last stripe - would require read-modify-write
                        // For Phase 1, we'll leave the stripe as-is (wasted space)
                        tracing::debug!(
                            "setattr: partial truncation of stripe {} (Phase 1: not implemented)",
                            stripe_idx
                        );
                    }
                }
            }
            // Growing the file - no action needed (sparse file semantics)
        }

        // Step 3: Propose metadata update through Raft (CONTROL PLANE)
        use crate::filesystem_service::raft_commands::{FileUpdateFields, RaftCommand};
        let command = RaftCommand::UpdateFile {
            inode,
            updates: FileUpdateFields {
                size,
                mode,
                uid,
                gid,
                atime,
                mtime,
            },
        };

        let _result = self
            .raft_stub
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        // Step 4: [TEMP Phase 1] Update metadata store directly
        let now = SystemTime::now();
        let updated_metadata = FileMetadata {
            file_type: record.file_type,
            size: size.unwrap_or(record.size),
            permissions: mode.unwrap_or(record.permissions),
            uid: uid.unwrap_or(record.uid),
            gid: gid.unwrap_or(record.gid),
            created_at: record.created_at,
            modified_at: mtime.unwrap_or(now),
            accessed_at: atime.unwrap_or(record.accessed_at),
        };

        self.metadata_store
            .update_file(record.file_id, updated_metadata.clone())
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 5: Invalidate cache
        self.inode_manager.cache().invalidate(inode);

        // Step 6: Return updated attributes
        let attr = FileAttr {
            ino: inode,
            size: updated_metadata.size,
            blocks: (updated_metadata.size + 4095) / 4096,
            atime: updated_metadata.accessed_at,
            mtime: updated_metadata.modified_at,
            ctime: now, // ctime updated on metadata change
            crtime: record.created_at,
            kind: match record.file_type {
                crate::metadata_store::FileType::Directory => FileType::Directory,
                crate::metadata_store::FileType::RegularFile => FileType::RegularFile,
                crate::metadata_store::FileType::Symlink => FileType::Symlink,
            },
            perm: updated_metadata.permissions as u16,
            nlink: 1,
            uid: updated_metadata.uid,
            gid: updated_metadata.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        };

        tracing::info!("Updated attributes for inode {}", inode);
        Ok(attr)
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
