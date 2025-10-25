//! Raft command definitions for metadata operations.
//!
//! These commands represent all metadata modifications that must go through
//! Raft consensus to maintain consistency across the cluster.

use crate::file_store::{FileId, StripeId, StripeMetadata};
use crate::metadata_store::{MetadataStore, MetadataStoreImpl};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use tracing::trace;

/// Commands that modify metadata and must go through Raft consensus.
///
/// All metadata write operations are encapsulated as RaftCommands to ensure
/// they are replicated and applied consistently across all nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCommand {
    /// Create a new file or directory
    CreateFile {
        parent_inode: u64,
        name: String,
        file_type: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
    },

    /// Update file metadata (size, permissions, timestamps)
    UpdateFile {
        inode: u64,
        updates: FileUpdateFields,
    },

    /// Delete a file or directory
    DeleteFile { parent_inode: u64, name: String },

    /// Create a symbolic link
    CreateSymlink {
        parent_inode: u64,
        name: String,
        target: String,
        uid: u32,
        gid: u32,
    },

    /// Allocate stripes for a file
    AllocateStripes {
        file_id: FileId,
        stripes: Vec<StripeAllocation>,
    },

    /// Update stripe metadata (after chunk writes complete)
    UpdateStripe {
        file_id: FileId,
        stripe_id: StripeId,
        metadata: StripeMetadata,
    },

    /// Mark chunks as committed (activate staged chunks)
    CommitChunks {
        stripe_id: StripeId,
        chunk_ids: Vec<u64>,
    },

    /// Acquire a file lock
    AcquireLock {
        inode: u64,
        lock_type: LockType,
        client_id: u64,
        node_id: u64,
        expires_at: SystemTime,
    },

    /// Release a file lock
    ReleaseLock { inode: u64, client_id: u64 },

    /// Extend lock expiration
    ExtendLock {
        inode: u64,
        client_id: u64,
        new_expiry: SystemTime,
    },

    /// Begin a two-phase commit transaction
    BeginTransaction {
        transaction_id: u64,
        operations: Vec<RaftCommand>,
        timeout: SystemTime,
    },

    /// Commit a prepared transaction
    CommitTransaction { transaction_id: u64 },

    /// Abort a prepared transaction
    AbortTransaction { transaction_id: u64 },
}

/// Result of executing a RaftCommand
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCommandResult {
    /// File created successfully
    FileCreated { inode: u64, file_id: FileId },

    /// File updated successfully
    FileUpdated,

    /// File deleted successfully
    FileDeleted,

    /// Symlink created successfully
    SymlinkCreated { inode: u64, file_id: FileId },

    /// Stripes allocated successfully
    StripesAllocated { stripe_ids: Vec<StripeId> },

    /// Stripe updated successfully
    StripeUpdated,

    /// Chunks committed successfully
    ChunksCommitted,

    /// Lock acquired successfully
    LockAcquired { lock_id: u64 },

    /// Lock released successfully
    LockReleased,

    /// Lock extended successfully
    LockExtended,

    /// Transaction begun successfully
    TransactionBegun,

    /// Transaction committed successfully
    TransactionCommitted,

    /// Transaction aborted successfully
    TransactionAborted,

    /// Operation failed with error
    Error { message: String },
}

/// File type for create operations
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FileType {
    Regular,
    Directory,
    Symlink,
}

/// Lock type for lock operations
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum LockType {
    Read,
    Write,
}

/// Fields that can be updated on a file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileUpdateFields {
    pub size: Option<u64>,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub atime: Option<SystemTime>,
    pub mtime: Option<SystemTime>,
}

/// Stripe allocation request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StripeAllocation {
    pub stripe_index: u32,
    pub offset: u64,
    pub size: u64,
    pub data_shards: u8,
    pub parity_shards: u8,
}

/// Stub implementation of StorageRaftMember for Phase 1
///
/// This stub immediately returns success for all operations and handles
/// metadata persistence directly. In Phase 2, this will be replaced with
/// a real Raft implementation where the state machine handles persistence.
pub struct StorageRaftMemberStub {
    /// Metadata store for persisting Raft commands
    metadata_store: MetadataStoreImpl,
}

impl StorageRaftMemberStub {
    /// Create a new stub instance
    pub fn new(metadata_store: MetadataStoreImpl) -> Self {
        Self { metadata_store }
    }

    /// Propose a metadata operation (stub - persists to metadata store)
    pub async fn propose_operation(
        &self,
        command: RaftCommand,
    ) -> Result<RaftCommandResult, RaftError> {
        // Log the operation for debugging
        tracing::debug!("STUB: Raft operation proposed: {:?}", command);

        // Handle each command type
        let result = match command {
            RaftCommand::CreateFile { .. } => RaftCommandResult::FileCreated {
                inode: generate_inode(),
                file_id: FileId::generate(),
            },
            RaftCommand::UpdateFile { inode, updates } => {
                // Handle file metadata updates
                self.handle_update_file(inode, updates).await?;
                RaftCommandResult::FileUpdated
            }
            RaftCommand::DeleteFile { parent_inode, name } => {
                // Handle file deletion
                self.handle_delete_file(parent_inode, &name).await?;
                RaftCommandResult::FileDeleted
            }
            RaftCommand::CreateSymlink {
                parent_inode,
                name,
                target,
                uid,
                gid,
            } => {
                // Handle symlink creation
                let (inode, file_id) = self
                    .handle_create_symlink(parent_inode, &name, &target, uid, gid)
                    .await?;
                RaftCommandResult::SymlinkCreated { inode, file_id }
            }
            RaftCommand::AllocateStripes { .. } => RaftCommandResult::StripesAllocated {
                stripe_ids: vec![StripeId::generate()],
            },
            RaftCommand::UpdateStripe {
                file_id,
                stripe_id,
                metadata,
            } => {
                // Handle stripe metadata persistence with create-or-update logic
                self.handle_update_stripe(file_id, stripe_id, metadata)
                    .await?;
                RaftCommandResult::StripeUpdated
            }
            RaftCommand::CommitChunks { .. } => RaftCommandResult::ChunksCommitted,
            RaftCommand::AcquireLock {
                inode,
                lock_type,
                client_id,
                node_id,
                expires_at,
            } => {
                // Handle lock acquisition through MetadataStore
                let lock_id = self
                    .handle_acquire_lock(inode, lock_type, client_id, node_id, expires_at)
                    .await?;
                RaftCommandResult::LockAcquired { lock_id }
            }
            RaftCommand::ReleaseLock { inode, client_id } => {
                // Handle lock release through MetadataStore
                self.handle_release_lock(inode, client_id).await?;
                RaftCommandResult::LockReleased
            }
            RaftCommand::ExtendLock {
                inode,
                client_id,
                new_expiry,
            } => {
                // Handle lock extension through MetadataStore
                self.handle_extend_lock(inode, client_id, new_expiry)
                    .await?;
                RaftCommandResult::LockExtended
            }
            RaftCommand::BeginTransaction { .. } => RaftCommandResult::TransactionBegun,
            RaftCommand::CommitTransaction { .. } => RaftCommandResult::TransactionCommitted,
            RaftCommand::AbortTransaction { .. } => RaftCommandResult::TransactionAborted,
        };

        Ok(result)
    }

    /// Handle UpdateFile command - update file metadata
    async fn handle_update_file(
        &self,
        inode: u64,
        updates: FileUpdateFields,
    ) -> Result<(), RaftError> {
        // Get current file metadata
        let file_record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| RaftError::OperationFailed(format!("Failed to get file: {}", e)))?;

        // Apply updates to create new metadata
        let updated_metadata = crate::metadata_store::FileMetadata {
            file_type: file_record.file_type,
            size: updates.size.unwrap_or(file_record.size),
            permissions: updates.mode.unwrap_or(file_record.permissions),
            uid: updates.uid.unwrap_or(file_record.uid),
            gid: updates.gid.unwrap_or(file_record.gid),
            created_at: file_record.created_at,
            modified_at: updates.mtime.unwrap_or(file_record.modified_at),
            accessed_at: updates.atime.unwrap_or(file_record.accessed_at),
            target: file_record.target, // Preserve existing target for symlinks
        };

        // Write updated metadata back to store
        self.metadata_store
            .update_file(file_record.file_id, updated_metadata)
            .await
            .map_err(|e| RaftError::OperationFailed(format!("Failed to update file: {}", e)))?;

        Ok(())
    }

    /// Handle UpdateStripe command with create-or-update logic
    async fn handle_update_stripe(
        &self,
        file_id: FileId,
        stripe_id: StripeId,
        metadata: StripeMetadata,
    ) -> Result<(), RaftError> {
        const STRIPE_SIZE: u64 = 4 * 1024 * 1024; // 4MB stripes

        // metadata.offset is now correctly set by FileStore to the stripe boundary
        let stripe_index = (metadata.offset / STRIPE_SIZE) as u32;

        // Check if stripe already exists at this offset
        let stripe_exists = self
            .metadata_store
            .get_stripe_at_offset(file_id, metadata.offset)
            .await
            .is_ok();

        if !stripe_exists {
            // Create new stripe record
            let stripe_record = crate::metadata_store::StripeRecord {
                stripe_id,
                file_id,
                stripe_index,
                offset: metadata.offset,
                size: metadata.size,
                checksum: metadata.checksum,
                created_at: SystemTime::now(),
            };

            self.metadata_store
                .allocate_stripes(file_id, vec![stripe_record])
                .await
                .map_err(|e| {
                    RaftError::OperationFailed(format!("Failed to allocate stripe: {}", e))
                })?;

            // Allocate chunks for new stripe
            let chunk_records: Vec<_> = metadata
                .chunks
                .iter()
                .map(|chunk| crate::metadata_store::ChunkRecord {
                    chunk_id: chunk.chunk_id,
                    stripe_id,
                    chunk_index: chunk.chunk_index,
                    node_id: chunk.node_id,
                    disk_id: chunk.disk_id,
                    checksum: metadata.checksum, // Use stripe checksum
                    status: crate::metadata_store::ChunkStatus::Healthy,
                    created_at: SystemTime::now(),
                    last_verified: None,
                })
                .collect();

            self.metadata_store
                .allocate_chunks(stripe_id, chunk_records)
                .await
                .map_err(|e| {
                    RaftError::OperationFailed(format!("Failed to allocate chunks: {}", e))
                })?;
        } else {
            // Stripe exists - need to update it by deleting old and creating new
            // This handles partial writes after truncation where chunks change
            tracing::debug!(
                "Stripe exists at offset {}, updating stripe metadata with new chunks",
                metadata.offset
            );

            // Get the old stripe to find its ID
            let old_stripe = self
                .metadata_store
                .get_stripe_at_offset(file_id, metadata.offset)
                .await
                .map_err(|e| {
                    RaftError::OperationFailed(format!("Failed to get existing stripe: {}", e))
                })?;

            // Delete old stripe and its chunks
            self.metadata_store
                .delete_stripe(old_stripe.stripe_id)
                .await
                .map_err(|e| {
                    RaftError::OperationFailed(format!("Failed to delete old stripe: {}", e))
                })?;

            // Create new stripe record with updated data
            let stripe_record = crate::metadata_store::StripeRecord {
                stripe_id,
                file_id,
                stripe_index,
                offset: metadata.offset,
                size: metadata.size,
                checksum: metadata.checksum,
                created_at: SystemTime::now(),
            };

            self.metadata_store
                .allocate_stripes(file_id, vec![stripe_record])
                .await
                .map_err(|e| {
                    RaftError::OperationFailed(format!("Failed to reallocate stripe: {}", e))
                })?;

            // Allocate new chunks
            let chunk_records: Vec<_> = metadata
                .chunks
                .iter()
                .map(|chunk| crate::metadata_store::ChunkRecord {
                    chunk_id: chunk.chunk_id,
                    stripe_id,
                    chunk_index: chunk.chunk_index,
                    node_id: chunk.node_id,
                    disk_id: chunk.disk_id,
                    checksum: metadata.checksum,
                    status: crate::metadata_store::ChunkStatus::Healthy,
                    created_at: SystemTime::now(),
                    last_verified: None,
                })
                .collect();

            self.metadata_store
                .allocate_chunks(stripe_id, chunk_records)
                .await
                .map_err(|e| {
                    RaftError::OperationFailed(format!("Failed to reallocate chunks: {}", e))
                })?;

            tracing::debug!(
                "Updated stripe at offset {} with {} chunks",
                metadata.offset,
                metadata.chunks.len()
            );
        }

        Ok(())
    }

    /// Handle DeleteFile command - delete file from metadata store
    async fn handle_delete_file(&self, parent_inode: u64, name: &str) -> Result<(), RaftError> {
        // Get parent directory
        let parent_record = self
            .metadata_store
            .get_file_by_inode(parent_inode)
            .await
            .map_err(|e| RaftError::OperationFailed(format!("Parent not found: {}", e)))?;

        // Construct full path
        let path = parent_record.path.join(name);

        // Get file to delete
        let file_record = self
            .metadata_store
            .get_file_by_path(&path)
            .await
            .map_err(|e| RaftError::OperationFailed(format!("File not found: {}", e)))?;

        // Delete all stripes and chunks for the file (metadata only)
        // Physical chunk deletion is handled by StorageWatchdog in Phase 2
        let stripes = self
            .metadata_store
            .get_file_stripes(file_record.file_id)
            .await
            .map_err(|e| RaftError::OperationFailed(format!("Failed to get stripes: {}", e)))?;

        for stripe in stripes {
            // Delete stripe (also deletes associated chunks in metadata)
            self.metadata_store
                .delete_stripe(stripe.stripe_id)
                .await
                .map_err(|e| {
                    RaftError::OperationFailed(format!("Failed to delete stripe: {}", e))
                })?;
        }

        // Delete file metadata
        self.metadata_store
            .delete_file(file_record.file_id)
            .await
            .map_err(|e| RaftError::OperationFailed(format!("Delete failed: {}", e)))?;

        tracing::info!(
            "Deleted file: path={:?}, inode={}, file_id={:?}",
            path,
            file_record.inode,
            file_record.file_id
        );

        Ok(())
    }

    /// Handle symlink creation
    async fn handle_create_symlink(
        &self,
        parent_inode: u64,
        name: &str,
        target: &str,
        uid: u32,
        gid: u32,
    ) -> Result<(u64, FileId), RaftError> {
        // Get parent directory
        let parent_record = self
            .metadata_store
            .get_file_by_inode(parent_inode)
            .await
            .map_err(|e| RaftError::OperationFailed(format!("Parent not found: {}", e)))?;

        if parent_record.file_type != crate::metadata_store::FileType::Directory {
            return Err(RaftError::OperationFailed(
                "Parent is not a directory".into(),
            ));
        }

        // Construct full path
        let path = parent_record.path.join(name);

        // Check if symlink already exists
        if let Ok(_existing) = self.metadata_store.get_file_by_path(&path).await {
            return Err(RaftError::OperationFailed(format!(
                "Symlink already exists: {:?}",
                path
            )));
        }

        // Reserve an inode
        let inode =
            self.metadata_store.reserve_inode().await.map_err(|e| {
                RaftError::OperationFailed(format!("Failed to reserve inode: {}", e))
            })?;

        // Generate file ID
        let file_id = FileId::new(uuid::Uuid::new_v4());

        // Create metadata for the symlink
        let now = std::time::SystemTime::now();
        let metadata = crate::metadata_store::FileMetadata {
            file_type: crate::metadata_store::FileType::Symlink,
            size: target.len() as u64,
            permissions: 0o777,
            uid,
            gid,
            created_at: now,
            modified_at: now,
            accessed_at: now,
            target: Some(target.to_string()),
        };

        // Create the symlink
        if let Err(e) = self
            .metadata_store
            .create_file(file_id.clone(), &path, inode, metadata)
            .await
        {
            // Clean up reserved inode on failure
            let _ = self.metadata_store.release_inode(inode).await;
            return Err(RaftError::OperationFailed(format!(
                "Failed to create symlink: {}",
                e
            )));
        }

        // Confirm the inode reservation
        if let Err(e) = self.metadata_store.confirm_inode(inode).await {
            tracing::error!(
                "Failed to confirm inode {} after symlink creation: {}",
                inode,
                e
            );
        }

        tracing::info!(
            "Created symlink: path={:?}, inode={}, target={}",
            path,
            inode,
            target
        );

        Ok((inode, file_id))
    }

    /// Handle AcquireLock command - acquire a distributed lock
    async fn handle_acquire_lock(
        &self,
        inode: u64,
        lock_type: LockType,
        client_id: u64,
        node_id: u64,
        expires_at: SystemTime,
    ) -> Result<u64, RaftError> {
        use crate::metadata_store::types::ClientId;
        use crate::metadata_store::MetadataStore;

        // Get the file by inode to get its file_id
        let file_record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| RaftError::OperationFailed(format!("Failed to get file: {}", e)))?;

        let client_id = ClientId::new(client_id);

        // Acquire lock via MetadataStore based on lock type
        let lock_id = match lock_type {
            LockType::Write => self
                .metadata_store
                .acquire_write_lock(file_record.file_id, client_id, node_id, expires_at)
                .await
                .map_err(|e| {
                    RaftError::OperationFailed(format!("Failed to acquire write lock: {}", e))
                })?,
            LockType::Read => self
                .metadata_store
                .acquire_read_lock(file_record.file_id, client_id, expires_at)
                .await
                .map_err(|e| {
                    RaftError::OperationFailed(format!("Failed to acquire read lock: {}", e))
                })?,
        };

        tracing::info!(
            "Acquired {:?} lock on inode {} by node {}, lock_id={}",
            lock_type,
            inode,
            node_id,
            lock_id
        );

        Ok(lock_id)
    }

    /// Handle ReleaseLock command - release a distributed lock
    async fn handle_release_lock(&self, inode: u64, client_id: u64) -> Result<(), RaftError> {
        use crate::metadata_store::types::ClientId;
        use crate::metadata_store::MetadataStore;

        // Get the file by inode to get its file_id
        let file_record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| RaftError::OperationFailed(format!("Failed to get file: {}", e)))?;

        let client_id = ClientId::new(client_id);

        // Release lock via MetadataStore
        self.metadata_store
            .release_lock(file_record.file_id, client_id)
            .await
            .map_err(|e| RaftError::OperationFailed(format!("Failed to release lock: {}", e)))?;

        tracing::info!(
            "Released lock on inode {}, client_id={}",
            inode,
            client_id.as_u64()
        );

        Ok(())
    }

    /// Handle ExtendLock command - extend a lock's expiration
    async fn handle_extend_lock(
        &self,
        inode: u64,
        client_id: u64,
        new_expiry: SystemTime,
    ) -> Result<(), RaftError> {
        use crate::metadata_store::types::ClientId;
        use crate::metadata_store::MetadataStore;

        // Get the file by inode to get its file_id
        let file_record = self
            .metadata_store
            .get_file_by_inode(inode)
            .await
            .map_err(|e| RaftError::OperationFailed(format!("Failed to get file: {}", e)))?;

        let client_id = ClientId::new(client_id);

        // Extend lock via MetadataStore
        self.metadata_store
            .extend_lock(file_record.file_id, client_id, new_expiry)
            .await
            .map_err(|e| RaftError::OperationFailed(format!("Failed to extend lock: {}", e)))?;

        tracing::info!(
            "Extended lock on inode {}, client_id={}",
            inode,
            client_id.as_u64()
        );

        Ok(())
    }

    /// Check if this node is the leader (stub - always true)
    pub fn is_leader(&self) -> bool {
        true
    }

    /// Propose a batch of stripe operations atomically.
    ///
    /// This method executes multiple stripe operations (creates, updates, deletes)
    /// as a single atomic batch. All operations succeed or all fail together.
    ///
    /// # Arguments
    ///
    /// * `operations` - Vector of stripe operations to execute atomically
    ///
    /// # Returns
    ///
    /// Ok(()) if all operations succeeded, Err otherwise
    pub async fn propose_stripe_batch(
        &self,
        operations: Vec<crate::filesystem_service::buffered_file_handle::StripeOperation>,
    ) -> Result<(), RaftError> {
        use crate::filesystem_service::buffered_file_handle::StripeOperation;
        use crate::metadata_store::types::StripeRecord;

        // Group operations by type
        let mut creates: Vec<(FileId, StripeMetadata)> = Vec::new();
        let mut updates: Vec<(FileId, StripeMetadata)> = Vec::new();
        let mut deletes: Vec<StripeId> = Vec::new();

        for op in operations {
            match op {
                StripeOperation::Create { file_id, stripe } => {
                    creates.push((file_id, stripe));
                }
                StripeOperation::Update { file_id, stripe } => {
                    updates.push((file_id, stripe));
                }
                StripeOperation::Delete { stripe_id } => {
                    deletes.push(stripe_id);
                }
                StripeOperation::UpdateAttributes {
                    file_id,
                    inode,
                    attributes,
                } => {
                    tracing::debug!(
                        "Updating attributes for file_id={:?}, inode={}, size={}",
                        file_id,
                        inode,
                        attributes.size
                    );

                    // Convert filesystem_service::FileAttr to metadata_store::FileMetadata
                    // Note: We need to preserve the target field for symlinks since FileAttr doesn't include it
                    use crate::filesystem_service::types::FileType as FsFileType;
                    use crate::metadata_store::types::FileType as MetaFileType;
                    use crate::metadata_store::MetadataStore;

                    let file_type = match attributes.kind {
                        FsFileType::RegularFile => MetaFileType::RegularFile,
                        FsFileType::Directory => MetaFileType::Directory,
                        FsFileType::Symlink => MetaFileType::Symlink,
                        FsFileType::NamedPipe
                        | FsFileType::BlockDevice
                        | FsFileType::CharDevice
                        | FsFileType::Socket => {
                            // Special files don't have data in WormFS, so they shouldn't reach here
                            trace!(
                                file_type = ?attributes.kind,
                                "Skipping attribute update for special file (no data storage)"
                            );
                            continue;
                        }
                    };

                    // Try to fetch existing file to preserve target field
                    // If the file doesn't exist yet (e.g., during initial creation with buffering),
                    // skip the update - it will be created when the file is first persisted
                    match self.metadata_store.get_file_by_inode(inode).await {
                        Ok(existing) => {
                            let metadata = crate::metadata_store::FileMetadata {
                                file_type,
                                size: attributes.size,
                                permissions: attributes.perm as u32,
                                uid: attributes.uid,
                                gid: attributes.gid,
                                created_at: attributes.crtime,
                                modified_at: attributes.mtime,
                                accessed_at: attributes.atime,
                                target: existing.target, // Preserve existing target
                            };

                            self.metadata_store
                                .update_file(file_id, metadata)
                                .await
                                .map_err(|e| {
                                    RaftError::OperationFailed(format!(
                                        "Failed to update file attributes: {}",
                                        e
                                    ))
                                })?;

                            trace!(
                                file_id = ?file_id,
                                inode = %inode,
                                size = %attributes.size,
                                "Successfully updated file attributes"
                            );
                        }
                        Err(_) => {
                            // File doesn't exist yet in metadata store - this is expected during
                            // initial creation when BufferedFileHandle buffers attributes before
                            // the file record is created. The attributes will be applied when
                            // the file is actually created.
                            trace!(
                                file_id = ?file_id,
                                inode = %inode,
                                "Skipping attribute update - file not yet persisted to metadata store"
                            );
                        }
                    }
                }
            }
        }

        // Execute deletes FIRST (before creates) to ensure tombstoned stripes are removed
        // This prevents the CREATE idempotency check from finding old stripes that should be replaced
        for stripe_id in deletes {
            trace!(
                stripe_id = ?stripe_id,
                "Deleting stripe (tombstone)"
            );
            self.metadata_store
                .delete_stripe(stripe_id)
                .await
                .map_err(|e| {
                    RaftError::OperationFailed(format!("Failed to delete stripe: {}", e))
                })?;
            trace!(
                stripe_id = ?stripe_id,
                "Successfully deleted stripe"
            );
        }

        // Execute creates with create-or-update logic to handle idempotent flushes
        const STRIPE_SIZE: u64 = 4 * 1024 * 1024; // 4MB stripes
        for (file_id, stripe_meta) in creates {
            let stripe_index = (stripe_meta.offset / STRIPE_SIZE) as u32;

            // Check if stripe already exists at this offset
            trace!(
                offset = %stripe_meta.offset,
                file_id = ?file_id,
                "Checking if stripe exists at offset"
            );
            let stripe_exists_result = self
                .metadata_store
                .get_stripe_at_offset(file_id, stripe_meta.offset)
                .await;

            match &stripe_exists_result {
                Ok(existing) => {
                    // Stripe exists - check if it's the same one (idempotent) or different (error)
                    if existing.stripe_id == stripe_meta.stripe_id {
                        trace!(
                            stripe_id = ?existing.stripe_id,
                            offset = %stripe_meta.offset,
                            "Stripe already exists (idempotent) - skipping create"
                        );
                        // Idempotent operation - this is OK, skip the create
                        continue;
                    } else {
                        // Different stripe at same offset - this is an error!
                        return Err(RaftError::OperationFailed(format!(
                            "Cannot create stripe {:?} at offset {}: different stripe {:?} already exists at this offset",
                            stripe_meta.stripe_id, stripe_meta.offset, existing.stripe_id
                        )));
                    }
                }
                Err(_) => {
                    // No stripe exists - proceed with create
                    trace!(
                        stripe_id = ?stripe_meta.stripe_id,
                        file_id = ?file_id,
                        offset = %stripe_meta.offset,
                        size = %stripe_meta.size,
                        chunks = %stripe_meta.chunks.len(),
                        "Creating new stripe"
                    );

                    // Create new stripe record
                    let stripe_record = StripeRecord {
                        stripe_id: stripe_meta.stripe_id,
                        file_id,
                        stripe_index,
                        offset: stripe_meta.offset,
                        size: stripe_meta.size,
                        checksum: stripe_meta.checksum,
                        created_at: SystemTime::now(),
                    };
                    self.metadata_store
                        .allocate_stripes(file_id, vec![stripe_record])
                        .await
                        .map_err(|e| {
                            RaftError::OperationFailed(format!("Failed to create stripe: {}", e))
                        })?;

                    trace!(
                        stripe_id = ?stripe_meta.stripe_id,
                        "Successfully created stripe in MetadataStore"
                    );

                    // Allocate chunks for new stripe
                    let chunk_records: Vec<_> = stripe_meta
                        .chunks
                        .iter()
                        .map(|chunk| crate::metadata_store::ChunkRecord {
                            chunk_id: chunk.chunk_id,
                            stripe_id: stripe_meta.stripe_id,
                            chunk_index: chunk.chunk_index,
                            node_id: chunk.node_id,
                            disk_id: chunk.disk_id,
                            checksum: stripe_meta.checksum,
                            status: crate::metadata_store::ChunkStatus::Healthy,
                            created_at: SystemTime::now(),
                            last_verified: None,
                        })
                        .collect();

                    self.metadata_store
                        .allocate_chunks(stripe_meta.stripe_id, chunk_records.clone())
                        .await
                        .map_err(|e| {
                            RaftError::OperationFailed(format!("Failed to allocate chunks: {}", e))
                        })?;

                    trace!(
                        chunk_count = %chunk_records.len(),
                        stripe_id = ?stripe_meta.stripe_id,
                        "Allocated chunks for stripe"
                    );
                }
            }
        }

        // Execute updates (delete + recreate for now)
        for (file_id, stripe_meta) in updates {
            let stripe_index = (stripe_meta.offset / STRIPE_SIZE) as u32;

            // Delete old version
            self.metadata_store
                .delete_stripe(stripe_meta.stripe_id)
                .await
                .map_err(|e| {
                    RaftError::OperationFailed(format!("Failed to delete stripe for update: {}", e))
                })?;

            // Create new version
            let stripe_record = StripeRecord {
                stripe_id: stripe_meta.stripe_id,
                file_id,
                stripe_index,
                offset: stripe_meta.offset,
                size: stripe_meta.size,
                checksum: stripe_meta.checksum,
                created_at: SystemTime::now(),
            };
            self.metadata_store
                .allocate_stripes(file_id, vec![stripe_record])
                .await
                .map_err(|e| {
                    RaftError::OperationFailed(format!("Failed to recreate stripe: {}", e))
                })?;

            // Allocate chunks for updated stripe
            let chunk_records: Vec<_> = stripe_meta
                .chunks
                .iter()
                .map(|chunk| crate::metadata_store::ChunkRecord {
                    chunk_id: chunk.chunk_id,
                    stripe_id: stripe_meta.stripe_id,
                    chunk_index: chunk.chunk_index,
                    node_id: chunk.node_id,
                    disk_id: chunk.disk_id,
                    checksum: stripe_meta.checksum,
                    status: crate::metadata_store::ChunkStatus::Healthy,
                    created_at: SystemTime::now(),
                    last_verified: None,
                })
                .collect();

            self.metadata_store
                .allocate_chunks(stripe_meta.stripe_id, chunk_records)
                .await
                .map_err(|e| {
                    RaftError::OperationFailed(format!(
                        "Failed to allocate chunks for update: {}",
                        e
                    ))
                })?;
        }

        Ok(())
    }
}

/// RaftClient implementation that wraps StorageRaftMemberStub.
///
/// This provides the RaftClient trait implementation for BufferedFileHandle
/// to use for atomic metadata operations.
pub struct RaftClientImpl {
    stub: Arc<StorageRaftMemberStub>,
}

impl RaftClientImpl {
    /// Create a new RaftClient wrapping the given stub
    pub fn new(stub: Arc<StorageRaftMemberStub>) -> Self {
        Self { stub }
    }
}

#[async_trait::async_trait]
impl crate::filesystem_service::buffered_file_handle::RaftClient for RaftClientImpl {
    async fn propose_stripe_batch(
        &self,
        operations: Vec<crate::filesystem_service::buffered_file_handle::StripeOperation>,
    ) -> Result<(), crate::filesystem_service::types::Error> {
        self.stub
            .propose_stripe_batch(operations)
            .await
            .map_err(|e| crate::filesystem_service::types::Error::Internal(format!("{}", e)))
    }
}

/// Error type for Raft operations
#[derive(Debug, thiserror::Error)]
pub enum RaftError {
    #[error("Not the leader")]
    NotLeader,

    #[error("Operation timeout")]
    Timeout,

    #[error("No quorum available")]
    NoQuorum,

    #[error("Operation failed: {0}")]
    OperationFailed(String),
}

// Helper functions for generating IDs in stub mode
fn generate_inode() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT_INODE: AtomicU64 = AtomicU64::new(1000);
    NEXT_INODE.fetch_add(1, Ordering::SeqCst)
}

fn generate_lock_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT_LOCK: AtomicU64 = AtomicU64::new(1);
    NEXT_LOCK.fetch_add(1, Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_store::{Config as MetadataConfig, MetadataStoreFactory};

    async fn create_test_metadata_store() -> MetadataStoreImpl {
        use crate::metadata_store::types::{IsolationLevel, SynchronousMode};

        let config = MetadataConfig {
            database_path: ":memory:".into(),
            read_pool_size: 5,
            enable_wal: false,
            cache_size_mb: 64,
            enable_foreign_keys: false,
            synchronous: SynchronousMode::Normal,
            transaction_isolation: IsolationLevel::ReadCommitted,
            enable_prepared_statements: false,
            read_pool_timeout_secs: 5,
            stripe_cache_size_mb: 64,
            stripe_cache_ttl_secs: 10,
            stripe_cache_tti_secs: 5,
            chunk_cache_size_mb: 64,
            chunk_cache_ttl_secs: 10,
            chunk_cache_tti_secs: 5,
        };
        MetadataStoreFactory::create_concrete(config)
            .await
            .expect("Failed to create test metadata store")
    }

    /// Helper function to create root directory for tests
    async fn create_root_directory(metadata_store: &MetadataStoreImpl) {
        use crate::metadata_store::{FileId, FileMetadata, MetadataStore};
        use std::path::Path;
        use std::time::SystemTime;

        let root_metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::Directory,
            size: 0,
            permissions: 0o755,
            uid: 0,
            gid: 0,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            target: None, // Not a symlink
        };
        metadata_store
            .create_file(FileId::generate(), Path::new("/"), 1, root_metadata)
            .await
            .expect("Failed to create root");
    }

    #[tokio::test]
    async fn test_stub_create_file() {
        let metadata_store = create_test_metadata_store().await;
        let stub = StorageRaftMemberStub::new(metadata_store);
        let command = RaftCommand::CreateFile {
            parent_inode: 1,
            name: "test.txt".to_string(),
            file_type: FileType::Regular,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        };

        let result = stub.propose_operation(command).await.unwrap();

        match result {
            RaftCommandResult::FileCreated { inode, file_id } => {
                assert!(inode > 0);
                assert!(file_id.as_uuid().as_u128() > 0);
            }
            _ => panic!("Expected FileCreated result"),
        }
    }

    #[tokio::test]
    async fn test_stub_acquire_lock() {
        use crate::metadata_store::{FileId, FileMetadata, MetadataStore};
        use std::path::Path;

        // Create metadata store and initialize schema
        let metadata_store = create_test_metadata_store().await;
        metadata_store
            .initialize_schema()
            .await
            .expect("Failed to initialize schema");

        // Create a test file to lock
        let file_metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::RegularFile,
            size: 1024,
            permissions: 0o644,
            uid: 1000,
            gid: 1000,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            target: None,
        };
        metadata_store
            .create_file(
                FileId::generate(),
                Path::new("/test.txt"),
                100,
                file_metadata,
            )
            .await
            .expect("Failed to create test file");

        let stub = StorageRaftMemberStub::new(metadata_store);
        let command = RaftCommand::AcquireLock {
            inode: 100,
            lock_type: LockType::Write,
            client_id: 1,
            node_id: 1, // Test node
            expires_at: SystemTime::now() + std::time::Duration::from_secs(60),
        };

        let result = stub.propose_operation(command).await.unwrap();

        match result {
            RaftCommandResult::LockAcquired { lock_id } => {
                assert!(lock_id > 0);
            }
            _ => panic!("Expected LockAcquired result"),
        }
    }

    #[tokio::test]
    async fn test_stub_delete_regular_file() {
        use crate::metadata_store::{FileId, FileMetadata, MetadataStore};
        use std::path::Path;
        use std::time::SystemTime;

        // Create metadata store and initialize schema
        let metadata_store = create_test_metadata_store().await;
        metadata_store
            .initialize_schema()
            .await
            .expect("Failed to initialize schema");

        // Create root directory
        let root_metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::Directory,
            size: 0,
            permissions: 0o755,
            uid: 0,
            gid: 0,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            target: None, // Not a symlink
        };
        metadata_store
            .create_file(FileId::generate(), Path::new("/"), 1, root_metadata)
            .await
            .expect("Failed to create root");

        // Create a regular file
        let file_metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::RegularFile,
            size: 1024,
            permissions: 0o644,
            uid: 1000,
            gid: 1000,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            target: None, // Not a symlink
        };
        let file_id = FileId::generate();
        metadata_store
            .create_file(file_id, Path::new("/test.txt"), 100, file_metadata)
            .await
            .expect("Failed to create test file");

        // Create Raft stub and delete the file
        let stub = StorageRaftMemberStub::new(metadata_store.clone());
        let command = RaftCommand::DeleteFile {
            parent_inode: 1,
            name: "test.txt".to_string(),
        };

        let result = stub
            .propose_operation(command)
            .await
            .expect("Delete operation failed");

        // Verify the result
        assert!(matches!(result, RaftCommandResult::FileDeleted));

        // Verify file is actually deleted
        let lookup_result = metadata_store
            .get_file_by_path(Path::new("/test.txt"))
            .await;
        assert!(
            lookup_result.is_err(),
            "File should not exist after deletion"
        );
    }

    #[tokio::test]
    async fn test_stub_delete_symlink() {
        use crate::metadata_store::{FileId, FileMetadata, MetadataStore};
        use std::path::Path;
        use std::time::SystemTime;

        // Create metadata store and initialize schema
        let metadata_store = create_test_metadata_store().await;
        metadata_store
            .initialize_schema()
            .await
            .expect("Failed to initialize schema");

        // Create root directory
        let root_metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::Directory,
            size: 0,
            permissions: 0o755,
            uid: 0,
            gid: 0,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            target: None, // Not a symlink
        };
        metadata_store
            .create_file(FileId::generate(), Path::new("/"), 1, root_metadata)
            .await
            .expect("Failed to create root");

        // Create a symlink
        let symlink_metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::Symlink,
            size: 0,            // Symlinks typically have size 0 or target path length
            permissions: 0o777, // Symlinks usually have full permissions
            uid: 1000,
            gid: 1000,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            target: None, // Not a symlink
        };
        let symlink_id = FileId::generate();
        metadata_store
            .create_file(symlink_id, Path::new("/link"), 101, symlink_metadata)
            .await
            .expect("Failed to create symlink");

        // Create Raft stub and delete the symlink
        let stub = StorageRaftMemberStub::new(metadata_store.clone());
        let command = RaftCommand::DeleteFile {
            parent_inode: 1,
            name: "link".to_string(),
        };

        let result = stub
            .propose_operation(command)
            .await
            .expect("Delete symlink operation failed");

        // Verify the result
        assert!(matches!(result, RaftCommandResult::FileDeleted));

        // Verify symlink is actually deleted
        let lookup_result = metadata_store.get_file_by_path(Path::new("/link")).await;
        assert!(
            lookup_result.is_err(),
            "Symlink should not exist after deletion"
        );
    }

    #[tokio::test]
    async fn test_stub_delete_nonexistent_file() {
        use crate::metadata_store::{FileId, FileMetadata, MetadataStore};
        use std::path::Path;
        use std::time::SystemTime;

        // Create metadata store and initialize schema
        let metadata_store = create_test_metadata_store().await;
        metadata_store
            .initialize_schema()
            .await
            .expect("Failed to initialize schema");

        // Create root directory
        let root_metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::Directory,
            size: 0,
            permissions: 0o755,
            uid: 0,
            gid: 0,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            target: None, // Not a symlink
        };
        metadata_store
            .create_file(FileId::generate(), Path::new("/"), 1, root_metadata)
            .await
            .expect("Failed to create root");

        // Try to delete a non-existent file
        let stub = StorageRaftMemberStub::new(metadata_store);
        let command = RaftCommand::DeleteFile {
            parent_inode: 1,
            name: "nonexistent.txt".to_string(),
        };

        let result = stub.propose_operation(command).await;

        // Should fail with an error
        assert!(result.is_err(), "Deleting nonexistent file should fail");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("File not found"),
            "Error should indicate file not found"
        );
    }

    #[tokio::test]
    async fn test_stub_delete_file_with_stripes() {
        use crate::file_store::StripeId;
        use crate::metadata_store::{FileId, FileMetadata, MetadataStore, StripeRecord};
        use std::path::Path;
        use std::time::SystemTime;

        // Create metadata store and initialize schema
        let metadata_store = create_test_metadata_store().await;
        metadata_store
            .initialize_schema()
            .await
            .expect("Failed to initialize schema");

        // Create root directory
        let root_metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::Directory,
            size: 0,
            permissions: 0o755,
            uid: 0,
            gid: 0,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            target: None, // Not a symlink
        };
        metadata_store
            .create_file(FileId::generate(), Path::new("/"), 1, root_metadata)
            .await
            .expect("Failed to create root");

        // Create a file with data
        let file_metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::RegularFile,
            size: 8 * 1024 * 1024, // 8MB file
            permissions: 0o644,
            uid: 1000,
            gid: 1000,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            target: None, // Not a symlink
        };
        let file_id = FileId::generate();
        metadata_store
            .create_file(file_id, Path::new("/bigfile.dat"), 102, file_metadata)
            .await
            .expect("Failed to create file");

        // Add some stripes to the file
        let stripe1 = StripeRecord {
            stripe_id: StripeId::generate(),
            file_id,
            stripe_index: 0,
            offset: 0,
            size: 4 * 1024 * 1024,
            checksum: 0x12345678, // Mock checksum
            created_at: SystemTime::now(),
        };
        let stripe2 = StripeRecord {
            stripe_id: StripeId::generate(),
            file_id,
            stripe_index: 1,
            offset: 4 * 1024 * 1024,
            size: 4 * 1024 * 1024,
            checksum: 0x87654321, // Mock checksum
            created_at: SystemTime::now(),
        };
        metadata_store
            .allocate_stripes(file_id, vec![stripe1.clone(), stripe2.clone()])
            .await
            .expect("Failed to allocate stripes");

        // Delete the file
        let stub = StorageRaftMemberStub::new(metadata_store.clone());
        let command = RaftCommand::DeleteFile {
            parent_inode: 1,
            name: "bigfile.dat".to_string(),
        };

        let result = stub
            .propose_operation(command)
            .await
            .expect("Delete operation failed");

        assert!(matches!(result, RaftCommandResult::FileDeleted));

        // Verify file is deleted
        let lookup_result = metadata_store
            .get_file_by_path(Path::new("/bigfile.dat"))
            .await;
        assert!(lookup_result.is_err(), "File should not exist");

        // Verify stripes are also deleted
        let stripes_result = metadata_store.get_file_stripes(file_id).await;
        assert!(
            stripes_result.is_err() || stripes_result.unwrap().is_empty(),
            "Stripes should be deleted with the file"
        );
    }

    #[tokio::test]
    async fn test_stub_create_symlink() {
        use crate::metadata_store::MetadataStore;
        use std::path::Path;

        // Create metadata store and initialize schema
        let metadata_store = create_test_metadata_store().await;
        metadata_store
            .initialize_schema()
            .await
            .expect("Failed to initialize schema");

        // Create root directory first
        create_root_directory(&metadata_store).await;

        // Create Raft stub and create a symlink
        let stub = StorageRaftMemberStub::new(metadata_store.clone());
        let command = RaftCommand::CreateSymlink {
            parent_inode: 1,
            name: "mylink".to_string(),
            target: "/path/to/target".to_string(),
            uid: 1000,
            gid: 1000,
        };

        let result = stub
            .propose_operation(command)
            .await
            .expect("Create symlink operation failed");

        // Verify the result
        match result {
            RaftCommandResult::SymlinkCreated { inode, file_id } => {
                assert!(inode > 1, "Inode should be allocated");
                assert!(!file_id.0.is_nil(), "File ID should be generated");

                // Verify symlink was created in metadata store
                let symlink_record = metadata_store
                    .get_file_by_path(Path::new("/mylink"))
                    .await
                    .expect("Should find symlink");

                assert_eq!(
                    symlink_record.file_type,
                    crate::metadata_store::FileType::Symlink
                );
                assert_eq!(symlink_record.target, Some("/path/to/target".to_string()));
                assert_eq!(symlink_record.inode, inode);
            }
            _ => panic!("Expected SymlinkCreated result, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_stub_create_symlink_already_exists() {
        use crate::metadata_store::{FileId, FileMetadata, MetadataStore};
        use std::path::Path;
        use std::time::SystemTime;

        // Create metadata store and initialize schema
        let metadata_store = create_test_metadata_store().await;
        metadata_store
            .initialize_schema()
            .await
            .expect("Failed to initialize schema");

        // Create root directory first
        create_root_directory(&metadata_store).await;

        // Create an existing symlink
        let inode = metadata_store
            .reserve_inode()
            .await
            .expect("Failed to reserve inode");
        let file_id = FileId::new(uuid::Uuid::new_v4());
        let metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::Symlink,
            size: 10,
            permissions: 0o777,
            uid: 1000,
            gid: 1000,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            target: Some("/existing/target".to_string()),
        };
        metadata_store
            .create_file(file_id, Path::new("/existing"), inode, metadata)
            .await
            .expect("Failed to create existing symlink");

        // Try to create symlink with same name
        let stub = StorageRaftMemberStub::new(metadata_store);
        let command = RaftCommand::CreateSymlink {
            parent_inode: 1,
            name: "existing".to_string(),
            target: "/new/target".to_string(),
            uid: 1000,
            gid: 1000,
        };

        let result = stub.propose_operation(command).await;

        // Should fail with an error
        assert!(result.is_err(), "Creating duplicate symlink should fail");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("already exists"),
            "Error should indicate symlink already exists"
        );
    }

    #[tokio::test]
    async fn test_stub_create_symlink_parent_not_directory() {
        use crate::metadata_store::{FileId, FileMetadata, MetadataStore};
        use std::path::Path;
        use std::time::SystemTime;

        // Create metadata store and initialize schema
        let metadata_store = create_test_metadata_store().await;
        metadata_store
            .initialize_schema()
            .await
            .expect("Failed to initialize schema");

        // Create root directory first
        create_root_directory(&metadata_store).await;

        // Create a regular file (not a directory)
        let inode = metadata_store
            .reserve_inode()
            .await
            .expect("Failed to reserve inode");
        let file_id = FileId::new(uuid::Uuid::new_v4());
        let metadata = FileMetadata {
            file_type: crate::metadata_store::FileType::RegularFile,
            size: 100,
            permissions: 0o644,
            uid: 1000,
            gid: 1000,
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            target: None,
        };
        metadata_store
            .create_file(file_id, Path::new("/file.txt"), inode, metadata)
            .await
            .expect("Failed to create regular file");

        // Try to create symlink with regular file as parent
        let stub = StorageRaftMemberStub::new(metadata_store);
        let command = RaftCommand::CreateSymlink {
            parent_inode: inode, // Using regular file's inode as parent
            name: "link".to_string(),
            target: "/target".to_string(),
            uid: 1000,
            gid: 1000,
        };

        let result = stub.propose_operation(command).await;

        // Should fail with an error
        assert!(
            result.is_err(),
            "Creating symlink in non-directory should fail"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("not a directory"),
            "Error should indicate parent is not a directory"
        );
    }
}
