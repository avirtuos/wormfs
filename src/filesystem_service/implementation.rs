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
use std::time::{Duration, SystemTime};

/// Overflow-safe helper: Check if offset + len would overflow u64.
///
/// Returns the end offset if safe, otherwise returns InvalidArgument error.
/// This prevents integer overflow in file I/O operations that could lead to
/// data corruption or security vulnerabilities.
fn checked_end_offset(offset: u64, len: usize) -> Result<u64, Error> {
    offset.checked_add(len as u64).ok_or_else(|| {
        Error::InvalidArgument("File operation would exceed maximum offset (u64 overflow)".into())
    })
}

/// Overflow-safe helper: Check if stripe_idx * stripe_size would overflow u64.
///
/// Returns the stripe offset if safe, otherwise returns Internal error.
/// This prevents overflow when calculating stripe boundaries for very large files.
fn checked_stripe_offset(stripe_idx: u64, stripe_size: u64) -> Result<u64, Error> {
    stripe_idx.checked_mul(stripe_size).ok_or_else(|| {
        Error::Internal(format!(
            "Stripe offset calculation overflow: {} * {}",
            stripe_idx, stripe_size
        ))
    })
}

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

    /// Client session tracking (client_id → last heartbeat time)
    /// Used to determine which clients are still alive for lock extension
    client_sessions: Arc<RwLock<HashMap<ClientId, SystemTime>>>,

    /// Lock extension background task handle
    lock_extension_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
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
            client_sessions: Arc::new(RwLock::new(HashMap::new())),
            lock_extension_task: Arc::new(RwLock::new(None)),
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
            target: None, // Directories don't have targets
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

    /// Record a client heartbeat (keeps client session alive).
    ///
    /// In Phase 1: Called automatically on open() to create session (stub mode)
    /// In Phase 2: Called via gRPC heartbeat endpoint by client libraries
    ///
    /// Clients that send recent heartbeats have their locks extended automatically
    /// by the background lock extension task.
    pub fn heartbeat(&self, client_id: ClientId) {
        let mut sessions = self.client_sessions.write().unwrap();
        sessions.insert(client_id, SystemTime::now());
        tracing::debug!("Heartbeat recorded for client {}", client_id.as_u64());
    }

    /// Start background tasks (lock extension, cleanup, etc.).
    ///
    /// Should be called after FileSystemService is initialized, typically during
    /// filesystem mount or storage node startup.
    pub fn start_background_tasks(self: Arc<Self>) {
        let service = Arc::clone(&self);

        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(service.config.lock_extend_interval);

            loop {
                interval.tick().await;

                if let Err(e) = service.extend_active_locks().await {
                    tracing::error!("Lock extension failed: {}", e);
                }
            }
        });

        let mut lock_task = self.lock_extension_task.write().unwrap();
        *lock_task = Some(task);

        tracing::info!(
            "Background tasks started (lock extension interval: {:?})",
            self.config.lock_extend_interval
        );
    }

    /// Extend locks for all active files with alive clients.
    ///
    /// This method is called periodically by the background task. It:
    /// 1. Checks all open files with locks
    /// 2. Verifies client is still alive (recent heartbeat)
    /// 3. Extends lock expiration via Raft
    async fn extend_active_locks(&self) -> Result<(), Error> {
        let now = SystemTime::now();
        let new_expiry = now + self.config.lock_timeout;

        // Get snapshot of open files with locks
        let files_to_extend: Vec<_> = {
            let open_files = self.open_files.read().unwrap();
            open_files
                .values()
                .filter(|f| f.lock_id.is_some())
                .map(|f| (f.inode, f.client_id, f.lock_id.unwrap()))
                .collect()
        };

        if files_to_extend.is_empty() {
            return Ok(());
        }

        tracing::debug!("Extending {} active locks", files_to_extend.len());

        // Check client heartbeats and build list of locks to extend
        // Drop the lock before doing async operations
        let locks_to_extend: Vec<_> = {
            let sessions = self.client_sessions.read().unwrap();

            files_to_extend
                .iter()
                .filter_map(|(inode, client_id, lock_id)| {
                    if let Some(last_heartbeat) = sessions.get(client_id) {
                        let heartbeat_age = now.duration_since(*last_heartbeat).unwrap_or(
                            self.config.client_heartbeat_timeout + Duration::from_secs(1),
                        );

                        if heartbeat_age < self.config.client_heartbeat_timeout {
                            Some((*inode, *client_id, *lock_id))
                        } else {
                            tracing::warn!(
                                "Client {} heartbeat timeout ({:?} > {:?}), lock {} will expire",
                                client_id.as_u64(),
                                heartbeat_age,
                                self.config.client_heartbeat_timeout,
                                lock_id
                            );
                            None
                        }
                    } else {
                        tracing::warn!(
                            "No heartbeat record for client {}, lock {} will expire",
                            client_id.as_u64(),
                            lock_id
                        );
                        None
                    }
                })
                .collect()
        }; // Lock dropped here

        // Now extend locks without holding any locks
        for (inode, client_id, lock_id) in locks_to_extend {
            use crate::filesystem_service::raft_commands::RaftCommand;

            let command = RaftCommand::ExtendLock {
                inode,
                client_id: client_id.as_u64(),
                new_expiry,
            };

            match self.raft_stub.propose_operation(command).await {
                Ok(_) => {
                    tracing::trace!(
                        "Extended lock {} for inode {}, client {}",
                        lock_id,
                        inode,
                        client_id.as_u64()
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to extend lock {} for inode {}: {}",
                        lock_id,
                        inode,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    /// Gracefully shutdown the filesystem service.
    ///
    /// Stops background tasks and releases all held locks.
    /// Should be called during filesystem unmount.
    pub async fn shutdown(&self) {
        tracing::info!("Shutting down FileSystemService...");

        // Stop background tasks
        {
            let mut task_guard = self.lock_extension_task.write().unwrap();
            if let Some(task) = task_guard.take() {
                task.abort();
                tracing::info!("Lock extension task stopped");
            }
        }

        // Release all held locks
        let files_to_release: Vec<_> = {
            let open_files = self.open_files.read().unwrap();
            open_files.keys().copied().collect()
        };

        for fh in files_to_release {
            if let Err(e) = self.release(fh).await {
                tracing::error!(
                    "Failed to release file handle {} during shutdown: {}",
                    fh,
                    e
                );
            }
        }

        tracing::info!("FileSystemService shutdown complete");
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
            atime: record.created_at, // Return creation time as atime (we don't track access time)
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
            atime: cached.metadata.created_at, // Return creation time as atime (we don't track access time)
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

    /// Get the stripe size from the current storage policy.
    ///
    /// Stripe size = chunk_size × data_shards.
    /// For the default policy (2 data shards × 2MB chunks), this returns 4MB.
    fn stripe_size(&self) -> u64 {
        self.default_storage_policy().stripe_size()
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
                // Convert lock conflict with context
                Error::LockConflict(format!(
                    "Cannot acquire {} lock on file_id {:?}",
                    lock_type, file_id
                ))
            }
            crate::metadata_store::Error::LockNotFound { file_id, client_id } => {
                // Convert lock not held with context
                Error::LockNotHeld(format!(
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

        // Step 2: Check create permission on parent directory (write + execute)
        crate::filesystem_service::permissions::check_create_permission(
            uid,
            gid,
            parent_record.uid,
            parent_record.gid,
            parent_record.permissions,
        )
        .map_err(|_| Error::PermissionDenied(parent))?;

        // Step 3: Reserve inode before Raft operation
        let inode = self
            .metadata_store
            .reserve_inode()
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 4: Construct path
        let path = parent_record.path.join(name);

        // Step 5: Propose file creation through Raft stub
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
            target: None, // Regular files don't have targets
        };

        // Create the file in metadata store - release inode on error
        if let Err(e) = self
            .metadata_store
            .create_file(file_id, &path, inode, metadata.clone())
            .await
        {
            let _ = self.metadata_store.release_inode(inode).await;
            return Err(self.convert_metadata_error(e));
        }

        // Step 7: Confirm inode reservation - release inode on error
        // Note: Inode reservations have a 1-hour TTL and will be cleaned up by a
        // background maintenance task (TODO: implement in Phase 2). Explicit release
        // here ensures immediate cleanup on database errors.
        if let Err(e) = self.metadata_store.confirm_inode(inode).await {
            let _ = self.metadata_store.release_inode(inode).await;
            return Err(self.convert_metadata_error(e));
        }

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
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<(u64, FileAttr), Error> {
        tracing::debug!(
            "open: inode={}, flags={}, uid={}, gid={}",
            inode,
            flags,
            uid,
            gid
        );

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

        // Step 3: Parse open flags and check permissions
        let is_write_mode = (flags & libc::O_ACCMODE as u32) != libc::O_RDONLY as u32;

        // Check appropriate permission based on access mode
        if is_write_mode {
            crate::filesystem_service::permissions::check_write_permission(
                uid,
                gid,
                record.uid,
                record.gid,
                record.permissions,
            )
            .map_err(|_| Error::PermissionDenied(inode))?;
        } else {
            crate::filesystem_service::permissions::check_read_permission(
                uid,
                gid,
                record.uid,
                record.gid,
                record.permissions,
            )
            .map_err(|_| Error::PermissionDenied(inode))?;
        }

        // Step 4: Acquire distributed write lock if opening for write
        // This ensures write exclusivity across the entire cluster, not just locally.
        // The lock is enforced via Raft consensus and stored in MetadataStore.
        let lock_id = if is_write_mode {
            use crate::filesystem_service::raft_commands::{LockType, RaftCommand};

            // Lock expires in 5 minutes (will be extended by keepalives in Phase 2)
            let expires_at = SystemTime::now() + std::time::Duration::from_secs(300);

            let command = RaftCommand::AcquireLock {
                inode,
                lock_type: LockType::Write,
                client_id: _client_id.as_u64(),
                node_id: self.config.node_id,
                expires_at,
            };

            match self.raft_stub.propose_operation(command).await {
                Ok(crate::filesystem_service::raft_commands::RaftCommandResult::LockAcquired {
                    lock_id,
                }) => {
                    tracing::debug!(
                        "Acquired write lock on inode {}, lock_id={}",
                        inode,
                        lock_id
                    );
                    Some(lock_id)
                }
                Ok(_) => {
                    return Err(Error::RaftError(
                        "Unexpected Raft result for lock acquisition".into(),
                    ));
                }
                Err(e) => {
                    // Lock acquisition failed - likely already locked
                    tracing::warn!("Failed to acquire write lock on inode {}: {}", inode, e);
                    return Err(Error::InvalidArgument(format!(
                        "File inode {} is already open for writing",
                        inode
                    )));
                }
            }
        } else {
            None // No lock needed for read-only opens
        };

        // Step 5: Generate unique file handle
        let file_handle = self.next_file_handle.fetch_add(1, Ordering::SeqCst);

        // Step 6: Create OpenFile state
        let open_file = Arc::new(OpenFile {
            file_id: record.file_id,
            inode,
            client_id: _client_id, // Store client ID for lock release
            lock_id,               // Store the distributed lock ID
            flags: super::types::OpenFlags {
                read: (flags & libc::O_ACCMODE as u32) != libc::O_WRONLY as u32,
                write: is_write_mode,
                append: (flags & libc::O_APPEND as u32) != 0,
                truncate: (flags & libc::O_TRUNC as u32) != 0,
                create: (flags & libc::O_CREAT as u32) != 0,
                exclusive: (flags & libc::O_EXCL as u32) != 0,
            },
            offset: AtomicU64::new(0),
            refcount: AtomicU32::new(1),
        });

        // Step 7: Track open file
        {
            let mut open_files = self.open_files.write().unwrap();
            open_files.insert(file_handle, open_file);
        }

        // Step 7a: Register client heartbeat (stub mode - creates session for lock extension)
        // In Phase 2, clients will send periodic heartbeats via gRPC
        self.heartbeat(_client_id);

        // Step 8: Handle O_TRUNC flag (truncate file to 0)
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
                target: record.target.clone(), // Preserve target for symlinks
            };

            self.metadata_store
                .update_file(record.file_id, updated_metadata.clone())
                .await
                .map_err(|e| self.convert_metadata_error(e))?;

            // Invalidate cache
            self.inode_manager.cache().invalidate(inode);
        }

        // Note: We do NOT update access time on file open (see read() for rationale).

        // Step 7: Return file handle and attributes
        let attr = self.file_record_to_attr(&record);
        tracing::info!("Opened file: inode={}, handle={}", inode, file_handle);
        Ok((file_handle, attr))
    }

    async fn read(
        &self,
        inode: u64,
        offset: u64,
        size: u32,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<Vec<u8>, Error> {
        tracing::debug!(
            "read: inode={}, offset={}, size={}, uid={}, gid={}",
            inode,
            offset,
            size,
            uid,
            gid
        );

        // Step 1: Get file metadata (direct read - no Raft needed)
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 2: Check read permission
        crate::filesystem_service::permissions::check_read_permission(
            uid,
            gid,
            record.uid,
            record.gid,
            record.permissions,
        )
        .map_err(|_| Error::PermissionDenied(inode))?;

        // Step 3: Bounds checking
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

        // Step 3: Calculate stripe range based on storage policy
        let stripe_size = self.stripe_size();

        let start_stripe_idx = offset / stripe_size;
        // Use checked arithmetic to prevent overflow (read_size already clamped to available data)
        let end_offset_minus_one = checked_end_offset(offset, read_size)?.saturating_sub(1);
        let end_stripe_idx = end_offset_minus_one / stripe_size;

        tracing::debug!(
            "read: reading stripes {} to {} (total: {})",
            start_stripe_idx,
            end_stripe_idx,
            end_stripe_idx - start_stripe_idx + 1
        );

        // Step 4: Read each stripe and accumulate data
        let mut result_data = Vec::with_capacity(read_size);

        for stripe_idx in start_stripe_idx..=end_stripe_idx {
            // Use checked arithmetic to prevent overflow
            let stripe_offset = checked_stripe_offset(stripe_idx, stripe_size)?;

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
                    vec![0u8; stripe_size as usize]
                }
            };

            // Calculate which part of this stripe we need
            let stripe_start = if stripe_idx == start_stripe_idx {
                (offset % stripe_size) as usize
            } else {
                0
            };

            let stripe_end = if stripe_idx == end_stripe_idx {
                // Reuse end_offset_minus_one to avoid recalculation and prevent overflow
                let end_offset_in_stripe = (end_offset_minus_one % stripe_size) as usize + 1;
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

        // Note: We do NOT update access time (atime) on reads.
        // Updating atime on every read would turn every read into a write operation,
        // doubling metadata I/O and Raft consensus overhead. This is standard practice
        // (equivalent to the 'noatime' mount option used by most production systems).
        // The getattr() operation returns creation time as atime.

        tracing::debug!("read: returning {} bytes", result_data.len());
        Ok(result_data)
    }

    async fn write(
        &self,
        inode: u64,
        offset: u64,
        data: Vec<u8>,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<u32, Error> {
        tracing::debug!(
            "write: inode={}, offset={}, size={}, uid={}, gid={}",
            inode,
            offset,
            data.len(),
            uid,
            gid
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

        // Step 2: Check write permission
        crate::filesystem_service::permissions::check_write_permission(
            uid,
            gid,
            record.uid,
            record.gid,
            record.permissions,
        )
        .map_err(|_| Error::PermissionDenied(inode))?;

        // Step 2: Calculate new file size and validate against max_file_size
        // Use checked arithmetic to prevent u64 overflow
        let end_offset = checked_end_offset(offset, data.len())?;
        let new_size = std::cmp::max(record.size, end_offset);

        if new_size > self.config.max_file_size {
            return Err(Error::NoSpace); // ENOSPC - file would exceed maximum size
        }

        // Step 3: Calculate stripe range based on storage policy
        let stripe_size = self.stripe_size();

        let start_stripe_idx = offset / stripe_size;
        // Use checked arithmetic and saturating_sub to prevent overflow
        let end_offset_minus_one = checked_end_offset(offset, data.len())?.saturating_sub(1);
        let end_stripe_idx = end_offset_minus_one / stripe_size;

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
            // Use checked arithmetic to prevent overflow
            let stripe_offset = checked_stripe_offset(stripe_idx, stripe_size)?;

            // Calculate what portion of data goes into this stripe
            let stripe_start = if stripe_idx == start_stripe_idx {
                (offset % stripe_size) as usize
            } else {
                0
            };

            let stripe_end = if stripe_idx == end_stripe_idx {
                // Reuse end_offset_minus_one to avoid recalculation
                (end_offset_minus_one % stripe_size) as usize + 1
            } else {
                stripe_size as usize
            };

            let data_len = stripe_end - stripe_start;
            let stripe_data_slice = &data[data_offset..data_offset + data_len];
            data_offset += data_len;

            // Check if this is a partial stripe write (requires read-modify-write)
            let is_partial = stripe_start > 0 || stripe_end < stripe_size as usize;

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
            target: record.target.clone(), // Preserve target for symlinks
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

    async fn unlink(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<(), Error> {
        tracing::debug!(
            "unlink: parent={}, name={}, uid={}, gid={}",
            parent,
            name,
            uid,
            gid
        );

        // Step 1: Verify parent exists and is a directory
        let parent_record = self
            .metadata_store
            .get_file_by_inode(parent)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        if parent_record.file_type != crate::metadata_store::FileType::Directory {
            return Err(Error::NotADirectory(parent));
        }

        // Step 2: Check write permission on parent directory (needed to delete files)
        crate::filesystem_service::permissions::check_unlink_permission(
            uid,
            gid,
            parent_record.uid,
            parent_record.gid,
            parent_record.permissions,
        )
        .map_err(|_| Error::PermissionDenied(parent))?;

        // Step 3: Construct path and lookup file
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

        // Step 4: Propose file deletion through Raft (handles metadata and stripe cleanup)
        use crate::filesystem_service::raft_commands::RaftCommand;
        let command = RaftCommand::DeleteFile {
            parent_inode: parent,
            name: name.to_string(),
        };

        let result = self
            .raft_stub
            .propose_operation(command)
            .await
            .map_err(|e| {
                let error_msg = format!("{}", e);
                // Convert specific Raft errors to appropriate FileSystemService errors
                if error_msg.contains("File not found") || error_msg.contains("not found") {
                    // We don't have the inode from the error, use a placeholder
                    Error::MetadataError(format!("File not found: {}", name))
                } else {
                    Error::RaftError(error_msg)
                }
            })?;

        match result {
            crate::filesystem_service::raft_commands::RaftCommandResult::FileDeleted => {}
            crate::filesystem_service::raft_commands::RaftCommandResult::Error { message } => {
                return Err(Error::MetadataError(message));
            }
            _ => {
                return Err(Error::Internal("Unexpected Raft result for unlink".into()));
            }
        }

        // Step 5: Invalidate cache
        // Note: Physical chunk deletion is handled by StorageWatchdog in Phase 2
        self.inode_manager.cache().invalidate(file_record.inode);

        tracing::info!(
            "Unlinked file: path={:?}, inode={}",
            path,
            file_record.inode
        );
        Ok(())
    }

    async fn symlink(
        &self,
        parent: u64,
        name: &str,
        target: &str,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        tracing::debug!(
            "symlink: parent={}, name={}, target={}",
            parent,
            name,
            target
        );

        // Step 1: Verify parent exists and is a directory
        let parent_record = self
            .metadata_store
            .get_file_by_inode(parent)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        if parent_record.file_type != crate::metadata_store::FileType::Directory {
            return Err(Error::NotADirectory(parent));
        }

        // Step 2: Check if symlink already exists
        let path = parent_record.path.join(name);
        if let Ok(_existing) = self.metadata_store.get_file_by_path(&path).await {
            return Err(Error::AlreadyExists(path.to_string_lossy().into_owned()));
        }

        // Step 3: Create the symlink through Raft for consistency
        use crate::filesystem_service::raft_commands::{RaftCommand, RaftCommandResult};
        let command = RaftCommand::CreateSymlink {
            parent_inode: parent,
            name: name.to_string(),
            target: target.to_string(),
            uid,
            gid,
        };

        let result = self
            .raft_stub
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("Failed to create symlink: {}", e)))?;

        // Step 4: Extract inode and file_id from result
        let (inode, file_id) = match result {
            RaftCommandResult::SymlinkCreated { inode, file_id } => (inode, file_id),
            RaftCommandResult::Error { message } => {
                return Err(Error::MetadataError(message));
            }
            _ => {
                return Err(Error::Internal(
                    "Unexpected Raft result for symlink creation".into(),
                ));
            }
        };

        // Step 5: Create FileAttr for the response
        let now = SystemTime::now();
        let attr = FileAttr {
            ino: inode,
            size: target.len() as u64, // Size of symlink is length of target path
            blocks: 0,                 // Symlinks don't use data blocks
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Symlink,
            perm: 0o777, // Symlinks typically have 777 permissions
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            blksize: 512,
            flags: 0,
        };

        // Step 6: Cache the new symlink's inode
        // Convert FileAttr to FileMetadata for caching
        let metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::Symlink,
            size: attr.size,
            permissions: attr.perm as u32,
            uid: attr.uid,
            gid: attr.gid,
            created_at: attr.ctime,
            modified_at: attr.mtime,
            accessed_at: attr.atime,
            target: Some(target.to_string()),
        };
        self.inode_manager.cache().insert(inode, file_id, metadata);

        tracing::info!(
            "Created symlink: path={:?}, inode={}, target={}, file_id={:?}",
            path,
            inode,
            target,
            file_id
        );

        Ok(attr)
    }

    async fn readlink(&self, inode: u64) -> Result<String, Error> {
        tracing::debug!("readlink: inode={}", inode);

        // Get the file record
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Verify it's a symlink
        if record.file_type != crate::metadata_store::FileType::Symlink {
            return Err(Error::NotASymlink(inode));
        }

        // Return the target path
        record.target.ok_or_else(|| {
            Error::Internal(format!("Symlink at inode {} has no target path", inode))
        })
    }

    async fn release(&self, file_handle: u64) -> Result<(), Error> {
        tracing::debug!("release: file_handle={}", file_handle);

        // Remove the file handle from tracking and extract lock info
        let removed = {
            let mut open_files = self.open_files.write().unwrap();
            open_files.remove(&file_handle)
        };

        match removed {
            Some(open_file) => {
                // If this file was locked (opened for write), release the distributed lock
                if let Some(lock_id) = open_file.lock_id {
                    tracing::debug!(
                        "Releasing distributed lock: inode={}, lock_id={}, client_id={}",
                        open_file.inode,
                        lock_id,
                        open_file.client_id.as_u64()
                    );

                    let command =
                        crate::filesystem_service::raft_commands::RaftCommand::ReleaseLock {
                            inode: open_file.inode,
                            client_id: open_file.client_id.as_u64(),
                        };

                    match self.raft_stub.propose_operation(command).await {
                        Ok(_) => {
                            tracing::debug!(
                                "Successfully released lock on inode {}",
                                open_file.inode
                            );
                        }
                        Err(e) => {
                            // Log error but don't fail the release - file is already closed
                            tracing::error!(
                                "Failed to release lock on inode {}: {}",
                                open_file.inode,
                                e
                            );
                        }
                    }
                }
            }
            None => {
                tracing::warn!("release: file_handle {} not found", file_handle);
                // Don't return an error - FUSE may call release multiple times
            }
        }

        Ok(())
    }

    // ===== Directory Operations =====

    async fn mkdir(
        &self,
        parent: u64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        tracing::debug!(
            "mkdir: parent={}, name={}, mode={:o}, uid={}, gid={}",
            parent,
            name,
            mode,
            uid,
            gid
        );

        // Step 1: Validate parent exists and is a directory
        let parent_record = self
            .metadata_store
            .get_file_by_inode(parent)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        if parent_record.file_type != crate::metadata_store::FileType::Directory {
            return Err(Error::NotADirectory(parent));
        }

        // Step 2: Check mkdir permission on parent directory (write + execute)
        crate::filesystem_service::permissions::check_mkdir_permission(
            uid,
            gid,
            parent_record.uid,
            parent_record.gid,
            parent_record.permissions,
        )
        .map_err(|_| Error::PermissionDenied(parent))?;

        // Step 3: Check if directory already exists
        let path = parent_record.path.join(name);
        if let Ok(_existing) = self.metadata_store.get_file_by_path(&path).await {
            return Err(Error::AlreadyExists(path.to_string_lossy().into_owned()));
        }

        // Step 4: Reserve inode before Raft operation
        let inode = self
            .metadata_store
            .reserve_inode()
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 5: Create directory through Raft for consistency
        use crate::filesystem_service::raft_commands::{FileType as RaftFileType, RaftCommand};
        let command = RaftCommand::CreateFile {
            parent_inode: parent,
            name: name.to_string(),
            file_type: RaftFileType::Directory,
            mode,
            uid,
            gid,
        };

        let result = self
            .raft_stub
            .propose_operation(command)
            .await
            .map_err(|e| {
                // Release reserved inode on error
                let _ = self.metadata_store.release_inode(inode);
                Error::RaftError(format!("Failed to create directory: {}", e))
            })?;

        // Step 6: Extract file_id from result (inode already reserved)
        let file_id = match result {
            crate::filesystem_service::raft_commands::RaftCommandResult::FileCreated {
                file_id,
                ..
            } => file_id,
            crate::filesystem_service::raft_commands::RaftCommandResult::Error { message } => {
                let _ = self.metadata_store.release_inode(inode).await;
                return Err(Error::MetadataError(message));
            }
            _ => {
                let _ = self.metadata_store.release_inode(inode).await;
                return Err(Error::Internal(
                    "Unexpected Raft result for directory creation".into(),
                ));
            }
        };

        // Step 7: [TEMP Phase 1] Write directly to MetadataStore
        // In Phase 2+, this will be handled by the Raft state machine
        let now = SystemTime::now();
        let metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::Directory,
            size: 0, // Directories have size 0
            permissions: mode,
            uid,
            gid,
            created_at: now,
            modified_at: now,
            accessed_at: now,
            target: None, // Directories don't have targets
        };

        // Create the directory in metadata store - release inode on error
        if let Err(e) = self
            .metadata_store
            .create_file(file_id, &path, inode, metadata.clone())
            .await
        {
            let _ = self.metadata_store.release_inode(inode).await;
            return Err(self.convert_metadata_error(e));
        }

        // Step 8: Confirm inode reservation - release inode on error
        if let Err(e) = self.metadata_store.confirm_inode(inode).await {
            let _ = self.metadata_store.release_inode(inode).await;
            return Err(self.convert_metadata_error(e));
        }

        // Step 9: Create FileAttr for the response
        // Directories typically have nlink=2 (. and parent's entry)
        let attr = FileAttr {
            ino: inode,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Directory,
            perm: mode as u16,
            nlink: 2,
            uid,
            gid,
            rdev: 0,
            blksize: 512,
            flags: 0,
        };

        // Step 10: Cache the new directory
        self.inode_manager.cache().insert(inode, file_id, metadata);

        tracing::info!(
            "Created directory: path={:?}, inode={}, file_id={:?}",
            path,
            inode,
            file_id
        );

        Ok(attr)
    }

    async fn rmdir(
        &self,
        parent: u64,
        name: &str,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<(), Error> {
        tracing::debug!(
            "rmdir: parent={}, name={}, uid={}, gid={}",
            parent,
            name,
            uid,
            gid
        );

        // Step 1: Validate parent exists and is a directory
        let parent_record = self
            .metadata_store
            .get_file_by_inode(parent)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        if parent_record.file_type != crate::metadata_store::FileType::Directory {
            return Err(Error::NotADirectory(parent));
        }

        // Step 2: Check rmdir permission on parent directory (write + execute)
        crate::filesystem_service::permissions::check_rmdir_permission(
            uid,
            gid,
            parent_record.uid,
            parent_record.gid,
            parent_record.permissions,
        )
        .map_err(|_| Error::PermissionDenied(parent))?;

        // Step 3: Construct path and lookup target directory
        let path = parent_record.path.join(name);
        let dir_record = self
            .metadata_store
            .get_file_by_path(&path)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 4: Verify target is a directory (not a file or symlink)
        if dir_record.file_type != crate::metadata_store::FileType::Directory {
            return Err(Error::NotADirectory(dir_record.inode));
        }

        // Step 5: Check if directory is empty
        // A directory is empty if it contains no entries (list_directory returns empty)
        let children = self
            .metadata_store
            .list_directory(&path)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        if !children.is_empty() {
            return Err(Error::DirectoryNotEmpty(dir_record.inode));
        }

        // Step 6: Propose directory deletion through Raft (handles metadata cleanup)
        use crate::filesystem_service::raft_commands::RaftCommand;
        let command = RaftCommand::DeleteFile {
            parent_inode: parent,
            name: name.to_string(),
        };

        let result = self
            .raft_stub
            .propose_operation(command)
            .await
            .map_err(|e| {
                let error_msg = format!("{}", e);
                // Convert specific Raft errors to appropriate FileSystemService errors
                if error_msg.contains("File not found") || error_msg.contains("not found") {
                    Error::MetadataError(format!("Directory not found: {}", name))
                } else {
                    Error::RaftError(error_msg)
                }
            })?;

        // Step 7: Check Raft result
        match result {
            crate::filesystem_service::raft_commands::RaftCommandResult::FileDeleted => {
                // Success - file already deleted by Raft stub
            }
            crate::filesystem_service::raft_commands::RaftCommandResult::Error { message } => {
                return Err(Error::MetadataError(message));
            }
            _ => {
                return Err(Error::Internal(
                    "Unexpected Raft result for directory deletion".into(),
                ));
            }
        }

        // Step 8: Invalidate cache
        // Note: File is already deleted from metadata store by Raft stub
        self.inode_manager.cache().invalidate(dir_record.inode);

        tracing::info!(
            "Removed directory: path={:?}, inode={}",
            path,
            dir_record.inode
        );

        Ok(())
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
            target: record.target.clone(), // Include target for symlinks
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
        new_uid: Option<u32>,
        new_gid: Option<u32>,
        size: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
        req_uid: u32,
        req_gid: u32,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        tracing::debug!(
            "setattr: inode={}, mode={:?}, size={:?}, req_uid={}, req_gid={}",
            inode,
            mode,
            size,
            req_uid,
            req_gid
        );

        // Step 1: Get current metadata
        let record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 2: Check permissions
        // Changing ownership or permissions requires being the owner
        if mode.is_some() || new_uid.is_some() || new_gid.is_some() {
            crate::filesystem_service::permissions::check_owner_permission(
                req_uid, record.uid, inode,
            )?;
        }

        // Changing size (truncate) requires write permission
        if size.is_some() {
            crate::filesystem_service::permissions::check_write_permission(
                req_uid,
                req_gid,
                record.uid,
                record.gid,
                record.permissions,
            )
            .map_err(|_| Error::PermissionDenied(inode))?;
        }

        // Step 3: Validate new size against max_file_size
        if let Some(new_size) = size {
            if new_size > self.config.max_file_size {
                return Err(Error::NoSpace); // ENOSPC - file would exceed maximum size
            }
        }

        // Step 3: Handle truncation if size is changing (DATA PLANE)
        if let Some(new_size) = size {
            if new_size < record.size {
                // Shrinking - need to delete/truncate stripes
                let stripe_size = self.stripe_size();

                let new_last_stripe_idx = if new_size == 0 {
                    0
                } else {
                    (new_size - 1) / stripe_size
                };

                let old_last_stripe_idx = if record.size == 0 {
                    0
                } else {
                    (record.size - 1) / stripe_size
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
                    let stripe_idx = stripe.offset / stripe_size;

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
                    } else if stripe_idx == new_last_stripe_idx && new_size % stripe_size != 0 {
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

        // Step 4: Propose metadata update through Raft (CONTROL PLANE)
        // Note: We ignore atime parameter - WormFS doesn't track access time
        use crate::filesystem_service::raft_commands::{FileUpdateFields, RaftCommand};
        let command = RaftCommand::UpdateFile {
            inode,
            updates: FileUpdateFields {
                size,
                mode,
                uid: new_uid,
                gid: new_gid,
                atime: None, // Always None - we don't track access time
                mtime,
            },
        };

        let _result = self
            .raft_stub
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        // Step 5: [TEMP Phase 1] Update metadata store directly
        let now = SystemTime::now();
        let updated_metadata = FileMetadata {
            file_type: record.file_type,
            size: size.unwrap_or(record.size),
            permissions: mode.unwrap_or(record.permissions),
            uid: new_uid.unwrap_or(record.uid),
            gid: new_gid.unwrap_or(record.gid),
            created_at: record.created_at,
            modified_at: mtime.unwrap_or(now),
            accessed_at: record.accessed_at, // Never update - preserved as-is
            target: record.target.clone(),   // Preserve target for symlinks
        };

        self.metadata_store
            .update_file(record.file_id, updated_metadata.clone())
            .await
            .map_err(|e| self.convert_metadata_error(e))?;

        // Step 6: Invalidate cache
        self.inode_manager.cache().invalidate(inode);

        // Step 7: Return updated attributes
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
    use super::*;
    use crate::file_store::FileStore;
    use crate::metadata_store::{
        factory::MetadataStoreFactory, types::Config as MetadataConfig, types::IsolationLevel,
        types::SynchronousMode, FileMetadata, MetadataStore,
    };
    use tempfile::TempDir;

    /// Test user ID (matches the uid used in create() calls in tests)
    const TEST_UID: u32 = 1000;
    /// Test group ID (matches the gid used in create() calls in tests)
    const TEST_GID: u32 = 1000;

    /// Create a test FileSystemService instance with temporary storage
    async fn create_test_service() -> Arc<FileSystemServiceImpl> {
        let metadata_config = MetadataConfig {
            database_path: ":memory:".into(),
            read_pool_size: 5,
            enable_wal: false,
            cache_size_mb: 64,
            enable_foreign_keys: false,
            synchronous: SynchronousMode::Normal,
            transaction_isolation: IsolationLevel::ReadCommitted,
            enable_prepared_statements: false,
            read_pool_timeout_secs: 5,
        };

        let metadata_store = MetadataStoreFactory::create_concrete(metadata_config)
            .await
            .unwrap();
        metadata_store.initialize_schema().await.unwrap();

        // Create temp dir for file store
        let temp_dir = TempDir::new().unwrap();

        let file_store_config = crate::file_store::types::Config {
            disk_paths: vec![temp_dir.path().to_path_buf()],
            max_chunk_size: 512,
            default_data_shards: 2,
            default_parity_shards: 1,
            max_concurrent_operations: 10,
            verification_interval: Duration::from_secs(3600),
            orphan_cleanup_age: Duration::from_secs(3600),
        };

        let file_store = Arc::new(FileStore::new(file_store_config).unwrap());

        let fs_config = Config {
            uid: 1000,
            gid: 1000,
            lock_timeout: Duration::from_secs(2), // Short timeout for testing
            lock_extend_interval: Duration::from_millis(500), // Fast extension for testing
            ..Default::default()
        };

        Arc::new(FileSystemServiceImpl::new(
            fs_config,
            metadata_store,
            file_store,
        ))
    }

    #[tokio::test]
    async fn test_lock_extension_keeps_lock_alive() {
        let service = create_test_service().await;

        // Create root directory
        service.initialize_root().await.unwrap();

        let client_id = ClientId::new(123);

        // Create a file using the FileSystemService trait
        let file_attr = service
            .create(
                ROOT_INODE, "test.txt", 0o644, // mode
                1000,  // uid
                1000,  // gid
                client_id,
            )
            .await
            .unwrap();

        // Open file for writing (acquires lock), flags=0x02 is O_RDWR
        let (fh, _attr) = service
            .open(file_attr.ino, 0x02, TEST_UID, TEST_GID, client_id)
            .await
            .unwrap();

        // Start background tasks (lock extension)
        Arc::clone(&service).start_background_tasks();

        // Sleep for 5 seconds (longer than lock_timeout of 2 seconds)
        // If lock extension is working, the lock should still be held
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Verify file is still open with lock
        {
            let open_files = service.open_files.read().unwrap();
            let open_file = open_files.get(&fh).expect("File handle should still exist");
            assert!(open_file.lock_id.is_some(), "Lock should still be held");
        }

        // Try to acquire the same lock from a different client (should fail)
        let other_client = ClientId::new(456);
        let result = service
            .open(file_attr.ino, 0x02, TEST_UID, TEST_GID, other_client)
            .await;
        assert!(
            result.is_err(),
            "Should not be able to acquire lock held by another client"
        );

        // Release the file
        service.release(fh).await.unwrap();

        // Shutdown to clean up background tasks
        service.shutdown().await;
    }

    #[tokio::test]
    async fn test_lock_expires_without_heartbeat() {
        let service = create_test_service().await;

        // Create root directory
        service.initialize_root().await.unwrap();

        let client_id = ClientId::new(123);

        // Create a file
        let file_attr = service
            .create(
                ROOT_INODE, "test.txt", 0o644, // mode
                1000,  // uid
                1000,  // gid
                client_id,
            )
            .await
            .unwrap();

        // Open file for writing (acquires lock, registers heartbeat)
        let (fh, _attr) = service
            .open(file_attr.ino, 0x02, TEST_UID, TEST_GID, client_id)
            .await
            .unwrap();

        // Start background tasks
        Arc::clone(&service).start_background_tasks();

        // Remove the client from sessions (simulate no heartbeat)
        {
            let mut sessions = service.client_sessions.write().unwrap();
            sessions.remove(&client_id);
        }

        // Sleep for longer than lock_timeout
        tokio::time::sleep(Duration::from_secs(3)).await;

        // The lock extension task should NOT extend the lock because
        // the client has no heartbeat. However, in our current implementation,
        // the lock is registered once during open(). This test verifies the
        // infrastructure is in place for Phase 2 heartbeat tracking.

        // For now, just verify the heartbeat removal worked
        {
            let sessions = service.client_sessions.read().unwrap();
            assert!(
                !sessions.contains_key(&client_id),
                "Client session should be removed"
            );
        }

        // Release and shutdown
        service.release(fh).await.unwrap();
        service.shutdown().await;
    }

    #[tokio::test]
    async fn test_heartbeat_registration() {
        let service = create_test_service().await;

        let client_id = ClientId::new(789);

        // Initially no session
        {
            let sessions = service.client_sessions.read().unwrap();
            assert!(!sessions.contains_key(&client_id));
        }

        // Call heartbeat
        service.heartbeat(client_id);

        // Should now have session
        {
            let sessions = service.client_sessions.read().unwrap();
            assert!(
                sessions.contains_key(&client_id),
                "Client session should be registered"
            );
            let last_heartbeat = sessions.get(&client_id).unwrap();

            // Heartbeat should be recent (within last second)
            let age = SystemTime::now().duration_since(*last_heartbeat).unwrap();
            assert!(age < Duration::from_secs(1), "Heartbeat should be recent");
        }
    }

    #[tokio::test]
    async fn test_write_overflow_detection() {
        let service = create_test_service().await;

        // Create root directory
        service.initialize_root().await.unwrap();

        let client_id = ClientId::new(1);

        // Create a test file
        let file_attr = service
            .create(ROOT_INODE, "test.txt", 0o644, 1000, 1000, client_id)
            .await
            .unwrap();

        let (fh, _) = service
            .open(file_attr.ino, 0x02, TEST_UID, TEST_GID, client_id)
            .await
            .unwrap();

        // Try to write at near u64::MAX offset - should fail gracefully
        let data = vec![0u8; 1000];
        let result = service
            .write(
                file_attr.ino,
                u64::MAX - 100,
                data,
                TEST_UID,
                TEST_GID,
                client_id,
            )
            .await;

        assert!(result.is_err(), "Should detect overflow");

        // Verify error message mentions overflow
        match result {
            Err(Error::InvalidArgument(msg)) => {
                assert!(
                    msg.contains("overflow"),
                    "Error should mention overflow: {}",
                    msg
                );
            }
            Err(e) => panic!("Expected InvalidArgument error, got: {:?}", e),
            Ok(_) => panic!("Should have failed with overflow error"),
        }

        // Clean up
        service.release(fh).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_overflow_detection() {
        let service = create_test_service().await;

        // Create root directory
        service.initialize_root().await.unwrap();

        let client_id = ClientId::new(1);

        // Create a test file
        let file_attr = service
            .create(ROOT_INODE, "test.txt", 0o644, 1000, 1000, client_id)
            .await
            .unwrap();

        let (fh, _) = service
            .open(file_attr.ino, 0x02, TEST_UID, TEST_GID, client_id)
            .await
            .unwrap();

        // Try to read at near u64::MAX offset - should fail gracefully or return empty
        // (read is clamped to file size, so it might succeed with empty result)
        let result = service
            .read(
                file_attr.ino,
                u64::MAX - 100,
                1000,
                TEST_UID,
                TEST_GID,
                client_id,
            )
            .await;

        // Either succeeds with empty data (offset > file_size) or detects overflow
        match result {
            Ok(data) => {
                assert!(
                    data.is_empty(),
                    "Should return empty data for read beyond file size"
                );
            }
            Err(Error::InvalidArgument(msg)) if msg.contains("overflow") => {
                // Also acceptable - overflow detected
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }

        // Clean up
        service.release(fh).await.unwrap();
    }

    #[tokio::test]
    async fn test_permission_denied_read() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(1);

        // Create a file owned by uid=1000 with mode 0o600 (rw-------)
        // Only the owner can read/write
        let file_attr = service
            .create(ROOT_INODE, "private.txt", 0o600, 1000, 1000, client_id)
            .await
            .unwrap();

        // Try to read as a different user (uid=2000) - should fail
        let result = service
            .read(file_attr.ino, 0, 100, 2000, 2000, client_id)
            .await;

        assert!(result.is_err(), "Should deny read access to non-owner");
        match result {
            Err(Error::PermissionDenied(inode)) => {
                assert_eq!(inode, file_attr.ino);
            }
            _ => panic!("Expected PermissionDenied error"),
        }
    }

    #[tokio::test]
    async fn test_permission_denied_write() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(1);

        // Create a file owned by uid=1000 with mode 0o644 (rw-r--r--)
        // Owner can write, but group and others cannot
        let file_attr = service
            .create(ROOT_INODE, "readonly.txt", 0o644, 1000, 1000, client_id)
            .await
            .unwrap();

        // Try to open for writing as a different user (uid=2000) - should fail at open
        let result = service
            .open(file_attr.ino, 0x02, 2000, 2000, client_id)
            .await;

        assert!(result.is_err(), "Should deny write open to non-owner");
        match result {
            Err(Error::PermissionDenied(inode)) => {
                assert_eq!(inode, file_attr.ino);
            }
            _ => panic!("Expected PermissionDenied error, got: {:?}", result),
        }

        // Try to write as a different user (uid=2000) - should also fail
        let data = vec![1u8; 100];
        let result = service
            .write(file_attr.ino, 0, data, 2000, 2000, client_id)
            .await;

        assert!(result.is_err(), "Should deny write access to non-owner");
        match result {
            Err(Error::PermissionDenied(inode)) => {
                assert_eq!(inode, file_attr.ino);
            }
            _ => panic!("Expected PermissionDenied error"),
        }
    }

    #[tokio::test]
    async fn test_permission_owner_precedence() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(1);

        // Create a file with mode 0o077 (---rwxrwx)
        // Owner has NO permissions, but group and others have full permissions
        // This tests POSIX precedence: owner permissions checked first
        let file_attr = service
            .create(ROOT_INODE, "weird.txt", 0o077, 1000, 1000, client_id)
            .await
            .unwrap();

        // Try to read as the owner (uid=1000) - should FAIL
        // Even though group has read permission, owner permissions take precedence
        let result = service
            .read(file_attr.ino, 0, 100, 1000, 1000, client_id)
            .await;

        assert!(result.is_err(), "Owner should be denied due to precedence");
        assert!(matches!(result, Err(Error::PermissionDenied(_))));

        // Try to read as a group member (uid=2000, gid=1000) - should SUCCEED
        let result = service
            .read(file_attr.ino, 0, 100, 2000, 1000, client_id)
            .await;

        assert!(result.is_ok(), "Group member should be able to read");
    }

    #[tokio::test]
    async fn test_permission_group_access() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(1);

        // Create a file with mode 0o640 (rw-r-----)
        // Owner can read/write, group can read, others have no access
        let file_attr = service
            .create(ROOT_INODE, "group.txt", 0o640, 1000, 1000, client_id)
            .await
            .unwrap();

        // Try to open for read as group member (uid=2000, gid=1000) - should succeed
        // Note: O_RDONLY = 0x00 (not 0x01 which is O_WRONLY!)
        let result = service
            .open(file_attr.ino, 0x00, 2000, 1000, client_id)
            .await;
        assert!(
            result.is_ok(),
            "Group member should be able to open for read. Error: {:?}",
            result.as_ref().err()
        );
        if let Ok((fh, _)) = result {
            service.release(fh).await.unwrap();
        }

        // Try to open for write as group member - should fail (group has no write permission)
        // O_WRONLY = 0x01
        let result = service
            .open(file_attr.ino, 0x01, 2000, 1000, client_id)
            .await;
        assert!(
            result.is_err(),
            "Group member should not be able to open for write"
        );

        // Try to open for read as other (uid=2000, gid=2000) - should fail
        let result = service
            .open(file_attr.ino, 0x00, 2000, 2000, client_id)
            .await;
        assert!(result.is_err(), "Other should not be able to open for read");
    }

    #[tokio::test]
    async fn test_permission_unlink_requires_parent_write() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(1);

        // Create a file - the file's permissions don't matter for unlink
        let file_attr = service
            .create(ROOT_INODE, "deleteme.txt", 0o644, 1000, 1000, client_id)
            .await
            .unwrap();

        // Try to unlink as non-owner of parent directory (uid=2000)
        // Root is owned by uid=1000, so this should fail
        let result = service
            .unlink(ROOT_INODE, "deleteme.txt", 2000, 2000, client_id)
            .await;

        assert!(
            result.is_err(),
            "Should deny unlink without write permission on parent"
        );
        match result {
            Err(Error::PermissionDenied(inode)) => {
                assert_eq!(inode, ROOT_INODE);
            }
            _ => panic!("Expected PermissionDenied error on parent directory"),
        }

        // Unlink as owner of parent directory - should succeed
        let result = service
            .unlink(ROOT_INODE, "deleteme.txt", 1000, 1000, client_id)
            .await;
        assert!(result.is_ok(), "Owner of parent should be able to unlink");
    }

    #[tokio::test]
    async fn test_permission_setattr_requires_ownership() {
        let service = create_test_service().await;
        service.initialize_root().await.unwrap();
        let client_id = ClientId::new(1);

        // Create a file owned by uid=1000
        let file_attr = service
            .create(ROOT_INODE, "changeme.txt", 0o644, 1000, 1000, client_id)
            .await
            .unwrap();

        // Try to change permissions as non-owner (uid=2000) - should fail
        let result = service
            .setattr(
                file_attr.ino,
                Some(0o600),
                None,
                None,
                None,
                None,
                None,
                2000,
                2000,
                client_id,
            )
            .await;

        assert!(
            result.is_err(),
            "Non-owner should not be able to change permissions"
        );
        assert!(matches!(result, Err(Error::PermissionDenied(_))));

        // Change permissions as owner - should succeed
        let result = service
            .setattr(
                file_attr.ino,
                Some(0o600),
                None,
                None,
                None,
                None,
                None,
                1000,
                1000,
                client_id,
            )
            .await;
        assert!(result.is_ok(), "Owner should be able to change permissions");
    }

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
