//! Raft command definitions for metadata operations.
//!
//! These commands represent all metadata modifications that must go through
//! Raft consensus to maintain consistency across the cluster.

use crate::file_store::{FileId, StripeId, StripeMetadata};
use crate::metadata_store::{MetadataStore, MetadataStoreImpl};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

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
            RaftCommand::DeleteFile { .. } => RaftCommandResult::FileDeleted,
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
            RaftCommand::AcquireLock { .. } => RaftCommandResult::LockAcquired {
                lock_id: generate_lock_id(),
            },
            RaftCommand::ReleaseLock { .. } => RaftCommandResult::LockReleased,
            RaftCommand::ExtendLock { .. } => RaftCommandResult::LockExtended,
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

    /// Check if this node is the leader (stub - always true)
    pub fn is_leader(&self) -> bool {
        true
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
        };
        MetadataStoreFactory::create_concrete(config)
            .await
            .expect("Failed to create test metadata store")
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
        let metadata_store = create_test_metadata_store().await;
        let stub = StorageRaftMemberStub::new(metadata_store);
        let command = RaftCommand::AcquireLock {
            inode: 100,
            lock_type: LockType::Write,
            client_id: 1,
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
}
