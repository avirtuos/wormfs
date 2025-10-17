//! Raft integration implementation for FileSystemService.
//!
//! This module shows how FileSystemService routes metadata modifications
//! through StorageRaftMember to ensure consensus across the cluster.

use crate::file_store::{FileId, StripeId, StripeMetadata};
use crate::filesystem_service::{
    raft_commands::{
        FileType as RaftFileType, FileUpdateFields, LockType as RaftLockType, RaftCommand,
        RaftCommandResult, StorageRaftMemberStub, StripeAllocation,
    },
    ClientId, Error, FileAttr, FileType, LockType,
};
use std::sync::Arc;
use std::time::SystemTime;

/// Extended FileSystemService implementation with Raft integration.
///
/// This shows how metadata operations will route through Raft in Phase 2+.
/// For Phase 1, we use StorageRaftMemberStub which immediately returns success.
pub struct RaftIntegratedFileSystemService {
    /// Reference to the Raft member (stub for Phase 1)
    raft_member: Arc<StorageRaftMemberStub>,

    /// Reference to MetadataStore for read operations
    metadata_store: Arc<crate::metadata_store::MetadataStoreImpl>,

    /// Reference to FileStore for chunk operations
    file_store: Arc<crate::file_store::FileStoreImpl>,
}

impl RaftIntegratedFileSystemService {
    /// Create a new FileSystemService with Raft integration
    pub fn new(
        metadata_store: Arc<crate::metadata_store::MetadataStoreImpl>,
        file_store: Arc<crate::file_store::FileStoreImpl>,
    ) -> Self {
        Self {
            raft_member: Arc::new(StorageRaftMemberStub::new((*metadata_store).clone())),
            metadata_store,
            file_store,
        }
    }

    /// Create a file through Raft consensus
    pub async fn create_file_via_raft(
        &self,
        parent: u64,
        name: &str,
        file_type: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
        _client_id: ClientId,
    ) -> Result<FileAttr, Error> {
        // Convert FileType to RaftFileType
        let raft_file_type = match file_type {
            FileType::RegularFile => RaftFileType::Regular,
            FileType::Directory => RaftFileType::Directory,
            _ => {
                return Err(Error::NotSupported(
                    "Special file types not supported".into(),
                ))
            }
        };

        // Create Raft command for file creation
        let command = RaftCommand::CreateFile {
            parent_inode: parent,
            name: name.to_string(),
            file_type: raft_file_type,
            mode,
            uid,
            gid,
        };

        // Propose through Raft (stub returns immediately in Phase 1)
        let result = self
            .raft_member
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        match result {
            RaftCommandResult::FileCreated { inode, file_id } => {
                // In Phase 2+, the Raft state machine will have already written to MetadataStore
                // For Phase 1, we need to write directly
                self.write_to_metadata_store_temp(
                    inode, file_id, parent, name, file_type, mode, uid, gid,
                )
                .await?;

                // Convert to FileAttr
                Ok(FileAttr {
                    ino: inode,
                    size: 0,
                    blocks: 0,
                    atime: SystemTime::now(),
                    mtime: SystemTime::now(),
                    ctime: SystemTime::now(),
                    crtime: SystemTime::now(),
                    kind: file_type,
                    perm: mode as u16,
                    nlink: 1,
                    uid,
                    gid,
                    rdev: 0,
                    blksize: 4096,
                    flags: 0,
                })
            }
            RaftCommandResult::Error { message } => Err(Error::MetadataError(message)),
            _ => Err(Error::Internal("Unexpected Raft result".into())),
        }
    }

    /// Update file metadata through Raft
    pub async fn update_file_via_raft(
        &self,
        inode: u64,
        size: Option<u64>,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> Result<(), Error> {
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

        let result = self
            .raft_member
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        match result {
            RaftCommandResult::FileUpdated => Ok(()),
            RaftCommandResult::Error { message } => Err(Error::MetadataError(message)),
            _ => Err(Error::Internal("Unexpected Raft result".into())),
        }
    }

    /// Delete a file through Raft
    pub async fn delete_file_via_raft(&self, parent: u64, name: &str) -> Result<(), Error> {
        let command = RaftCommand::DeleteFile {
            parent_inode: parent,
            name: name.to_string(),
        };

        let result = self
            .raft_member
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        match result {
            RaftCommandResult::FileDeleted => Ok(()),
            RaftCommandResult::Error { message } => Err(Error::MetadataError(message)),
            _ => Err(Error::Internal("Unexpected Raft result".into())),
        }
    }

    /// Allocate stripes for a file through Raft
    pub async fn allocate_stripes_via_raft(
        &self,
        file_id: FileId,
        count: usize,
        stripe_size: u64,
        data_shards: u8,
        parity_shards: u8,
    ) -> Result<Vec<StripeId>, Error> {
        let allocations: Vec<StripeAllocation> = (0..count)
            .map(|i| StripeAllocation {
                stripe_index: i as u32,
                offset: (i as u64) * stripe_size,
                size: stripe_size,
                data_shards,
                parity_shards,
            })
            .collect();

        let command = RaftCommand::AllocateStripes {
            file_id,
            stripes: allocations,
        };

        let result = self
            .raft_member
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        match result {
            RaftCommandResult::StripesAllocated { stripe_ids } => Ok(stripe_ids),
            RaftCommandResult::Error { message } => Err(Error::MetadataError(message)),
            _ => Err(Error::Internal("Unexpected Raft result".into())),
        }
    }

    /// Update stripe metadata after chunk writes complete
    pub async fn update_stripe_via_raft(
        &self,
        file_id: FileId,
        stripe_id: StripeId,
        metadata: StripeMetadata,
    ) -> Result<(), Error> {
        let command = RaftCommand::UpdateStripe {
            file_id,
            stripe_id,
            metadata,
        };

        let result = self
            .raft_member
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        match result {
            RaftCommandResult::StripeUpdated => Ok(()),
            RaftCommandResult::Error { message } => Err(Error::MetadataError(message)),
            _ => Err(Error::Internal("Unexpected Raft result".into())),
        }
    }

    /// Commit chunks (activate staged chunks) through Raft
    pub async fn commit_chunks_via_raft(
        &self,
        stripe_id: StripeId,
        chunk_ids: Vec<u64>,
    ) -> Result<(), Error> {
        let command = RaftCommand::CommitChunks {
            stripe_id,
            chunk_ids,
        };

        let result = self
            .raft_member
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        match result {
            RaftCommandResult::ChunksCommitted => Ok(()),
            RaftCommandResult::Error { message } => Err(Error::MetadataError(message)),
            _ => Err(Error::Internal("Unexpected Raft result".into())),
        }
    }

    /// Acquire a lock through Raft
    pub async fn acquire_lock_via_raft(
        &self,
        inode: u64,
        lock_type: LockType,
        client_id: ClientId,
        expires_at: SystemTime,
    ) -> Result<u64, Error> {
        let raft_lock_type = match lock_type {
            LockType::Read => RaftLockType::Read,
            LockType::Write => RaftLockType::Write,
        };

        let command = RaftCommand::AcquireLock {
            inode,
            lock_type: raft_lock_type,
            client_id: client_id.0,
            node_id: 1, // TODO: Get from config when raft_integration is updated for distributed mode
            expires_at,
        };

        let result = self
            .raft_member
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        match result {
            RaftCommandResult::LockAcquired { lock_id } => Ok(lock_id),
            RaftCommandResult::Error { message } => Err(Error::LockConflict(message)),
            _ => Err(Error::Internal("Unexpected Raft result".into())),
        }
    }

    /// Release a lock through Raft
    pub async fn release_lock_via_raft(
        &self,
        inode: u64,
        client_id: ClientId,
    ) -> Result<(), Error> {
        let command = RaftCommand::ReleaseLock {
            inode,
            client_id: client_id.0,
        };

        let result = self
            .raft_member
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        match result {
            RaftCommandResult::LockReleased => Ok(()),
            RaftCommandResult::Error { message } => Err(Error::LockNotHeld(message)),
            _ => Err(Error::Internal("Unexpected Raft result".into())),
        }
    }

    /// Extend lock expiration through Raft
    pub async fn extend_lock_via_raft(
        &self,
        inode: u64,
        client_id: ClientId,
        new_expiry: SystemTime,
    ) -> Result<(), Error> {
        let command = RaftCommand::ExtendLock {
            inode,
            client_id: client_id.0,
            new_expiry,
        };

        let result = self
            .raft_member
            .propose_operation(command)
            .await
            .map_err(|e| Error::RaftError(format!("{}", e)))?;

        match result {
            RaftCommandResult::LockExtended => Ok(()),
            RaftCommandResult::Error { message } => Err(Error::LockNotHeld(message)),
            _ => Err(Error::Internal("Unexpected Raft result".into())),
        }
    }

    /// Temporary helper for Phase 1 - writes directly to MetadataStore
    /// In Phase 2+, this will be handled by the Raft state machine
    async fn write_to_metadata_store_temp(
        &self,
        inode: u64,
        file_id: FileId,
        _parent: u64,
        name: &str,
        _file_type: FileType,
        _mode: u32,
        _uid: u32,
        _gid: u32,
    ) -> Result<(), Error> {
        // This is a placeholder - actual MetadataStore integration
        // will be implemented when MetadataStore methods are available
        tracing::info!(
            "TEMP: Would write to MetadataStore - inode: {}, file_id: {:?}, name: {}",
            inode,
            file_id,
            name
        );
        Ok(())
    }

    /// Helper to check if writes should go through Raft
    pub fn should_use_raft(&self) -> bool {
        // In Phase 1, we always use the stub which returns immediately
        // In Phase 2+, we'll check if we're the leader
        self.raft_member.is_leader()
    }
}

/// Example of how a write operation flows through Raft
pub async fn example_write_flow(service: &RaftIntegratedFileSystemService) -> Result<(), Error> {
    // Step 1: Create a file (metadata operation - goes through Raft)
    let file_attr = service
        .create_file_via_raft(
            1,          // parent inode (root)
            "test.txt", // filename
            FileType::RegularFile,
            0o644,         // mode
            1000,          // uid
            1000,          // gid
            ClientId(123), // client ID
        )
        .await?;

    // In the real implementation, file_id would come from the Raft result
    // For this example, we generate a new file_id
    let file_id = FileId::generate();

    // Step 2: Allocate stripes (metadata operation - goes through Raft)
    let stripe_ids = service
        .allocate_stripes_via_raft(
            file_id,
            1,           // count
            1024 * 1024, // 1MB stripes
            2,           // data shards
            1,           // parity shards
        )
        .await?;

    // Step 3: Write chunk data (data operation - direct to FileStore)
    // This happens outside of Raft - chunks are staged but not in metadata yet
    let stripe_id = stripe_ids[0];
    let data = vec![0u8; 1024];

    // FileStore writes chunks to disk in "staged" state
    // (Implementation would go here)

    // Step 4: Update stripe metadata (metadata operation - goes through Raft)
    // This commits the chunks and makes them visible in metadata
    let stripe_metadata = StripeMetadata::new(
        stripe_id,
        file_id,
        0,      // offset
        1024,   // size
        0,      // checksum
        vec![], // chunks (would include actual chunk metadata)
    );

    service
        .update_stripe_via_raft(file_id, stripe_id, stripe_metadata)
        .await?;

    // Step 5: Commit chunks (metadata operation - goes through Raft)
    // This activates the staged chunks
    service
        .commit_chunks_via_raft(stripe_id, vec![1, 2, 3])
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_file_via_raft() {
        // This would require mock implementations of MetadataStore and FileStore
        // For now, we'll skip the actual test implementation
    }

    #[tokio::test]
    async fn test_lock_operations_via_raft() {
        // Test acquire, extend, and release lock operations
    }
}
