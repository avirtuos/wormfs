//! Raft command definitions for metadata operations.
//!
//! These commands represent all metadata modifications that must go through
//! Raft consensus to maintain consistency across the cluster.

use crate::file_store::{FileId, StripeId, StripeMetadata};
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
/// This stub immediately returns success for all operations without
/// actually performing consensus. It will be replaced with the real
/// implementation in Phase 2.
pub struct StorageRaftMemberStub;

impl StorageRaftMemberStub {
    /// Create a new stub instance
    pub fn new() -> Self {
        Self
    }

    /// Propose a metadata operation (stub - always succeeds)
    pub async fn propose_operation(
        &self,
        command: RaftCommand,
    ) -> Result<RaftCommandResult, RaftError> {
        // Log the operation for debugging
        tracing::debug!("STUB: Raft operation proposed: {:?}", command);

        // Return appropriate success result based on command type
        let result = match command {
            RaftCommand::CreateFile { .. } => RaftCommandResult::FileCreated {
                inode: generate_inode(),
                file_id: FileId::generate(),
            },
            RaftCommand::UpdateFile { .. } => RaftCommandResult::FileUpdated,
            RaftCommand::DeleteFile { .. } => RaftCommandResult::FileDeleted,
            RaftCommand::AllocateStripes { .. } => RaftCommandResult::StripesAllocated {
                stripe_ids: vec![StripeId::generate()],
            },
            RaftCommand::UpdateStripe { .. } => RaftCommandResult::StripeUpdated,
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

    #[tokio::test]
    async fn test_stub_create_file() {
        let stub = StorageRaftMemberStub::new();
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
                assert!(file_id.as_u64() > 0);
            }
            _ => panic!("Expected FileCreated result"),
        }
    }

    #[tokio::test]
    async fn test_stub_acquire_lock() {
        let stub = StorageRaftMemberStub::new();
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
