//! Common types for the TransactionManager component.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use thiserror::Error;

// Re-export types from other modules
pub use crate::file_store::types::{ChunkId, DiskId, FileId, StripeId};
pub use crate::storage_raft_member::types::{FileMetadata, MetadataOperation, StoragePolicy, TxId};

/// Configuration for TransactionManager.
///
/// This configuration controls transaction behavior, timeouts, and subscription settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Maximum number of active transactions (default: 1000)
    #[serde(default = "default_max_active")]
    pub max_active_transactions: usize,

    /// Timeout for transaction prepare phase in seconds (default: 30)
    #[serde(default = "default_prepare_timeout")]
    pub prepare_timeout_secs: u64,

    /// Lock acquisition timeout in seconds (default: 10)
    #[serde(default = "default_lock_timeout")]
    pub lock_timeout_secs: u64,

    /// Deadlock detection interval in milliseconds (default: 100)
    /// Note: Currently we use timeout-based prevention, not active detection
    #[serde(default = "default_deadlock_detection_interval")]
    pub deadlock_detection_interval_ms: u64,

    /// Enable subscription system for metadata changes (default: true)
    #[serde(default = "default_enable_subscriptions")]
    pub enable_subscriptions: bool,

    /// Maximum number of concurrent subscribers (default: 100)
    #[serde(default = "default_max_subscribers")]
    pub max_subscribers: usize,

    /// Interval for cleanup task to check for expired transactions in seconds (default: 1)
    #[serde(default = "default_cleanup_interval")]
    pub cleanup_interval_secs: u64,
}

fn default_max_active() -> usize {
    1000
}

fn default_prepare_timeout() -> u64 {
    30
}

fn default_lock_timeout() -> u64 {
    10
}

fn default_deadlock_detection_interval() -> u64 {
    100
}

fn default_enable_subscriptions() -> bool {
    true
}

fn default_max_subscribers() -> usize {
    100
}

fn default_cleanup_interval() -> u64 {
    1
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_active_transactions: default_max_active(),
            prepare_timeout_secs: default_prepare_timeout(),
            lock_timeout_secs: default_lock_timeout(),
            deadlock_detection_interval_ms: default_deadlock_detection_interval(),
            enable_subscriptions: default_enable_subscriptions(),
            max_subscribers: default_max_subscribers(),
            cleanup_interval_secs: default_cleanup_interval(),
        }
    }
}

impl Config {
    /// Get the prepare timeout as a Duration
    pub fn prepare_timeout(&self) -> Duration {
        Duration::from_secs(self.prepare_timeout_secs)
    }

    /// Get the lock timeout as a Duration
    pub fn lock_timeout(&self) -> Duration {
        Duration::from_secs(self.lock_timeout_secs)
    }

    /// Get the cleanup interval as a Duration
    pub fn cleanup_interval(&self) -> Duration {
        Duration::from_secs(self.cleanup_interval_secs)
    }

    /// Get the deadlock detection interval as a Duration
    pub fn deadlock_detection_interval(&self) -> Duration {
        Duration::from_millis(self.deadlock_detection_interval_ms)
    }
}

/// A batch of operations grouped together for atomic execution.
///
/// This struct represents a transaction that is being built up before submission
/// to Raft. Operations are validated locally before being added to the batch.
#[derive(Debug, Clone)]
pub struct TransactionBatch {
    /// Unique transaction identifier
    pub id: TxId,

    /// List of operations to execute atomically
    pub operations: Vec<Operation>,

    /// When this transaction was created
    pub created_at: SystemTime,

    /// How long before this transaction expires
    pub timeout: Duration,
}

impl TransactionBatch {
    /// Create a new transaction batch.
    pub fn new(id: TxId, timeout: Duration) -> Self {
        Self {
            id,
            operations: Vec::new(),
            created_at: SystemTime::now(),
            timeout,
        }
    }

    /// Check if this transaction has expired.
    pub fn is_expired(&self) -> bool {
        SystemTime::now()
            .duration_since(self.created_at)
            .map(|age| age > self.timeout)
            .unwrap_or(false)
    }

    /// Add an operation to this batch.
    pub fn add_operation(&mut self, operation: Operation) {
        self.operations.push(operation);
    }

    /// Get the number of operations in this batch.
    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }
}

/// High-level operations that can be included in a transaction.
///
/// These are higher-level than `MetadataOperation` and provide a cleaner API
/// for applications. They are converted to `MetadataOperation` before submission
/// to Raft.
#[derive(Debug, Clone)]
pub enum Operation {
    /// Create a new file
    CreateFile {
        /// Unique file identifier (caller should pre-generate using FileId::generate())
        file_id: FileId,
        /// Path where the file should be created
        path: PathBuf,
        /// Inode number for the file
        inode: u64,
        /// File metadata (size, timestamps, permissions, etc.)
        metadata: FileMetadata,
        /// Storage policy (erasure coding parameters, etc.)
        policy: StoragePolicy,
    },

    /// Update existing file metadata
    UpdateFile {
        /// File ID to update
        file_id: FileId,
        /// Inode number
        inode: u64,
        /// Updated metadata
        metadata: FileMetadata,
        /// Updated storage policy
        policy: StoragePolicy,
    },

    /// Delete a file
    DeleteFile {
        /// File ID to delete
        file_id: FileId,
        /// Inode number
        inode: u64,
    },

    /// Create a stripe for a file
    CreateStripe {
        /// File ID this stripe belongs to
        file_id: FileId,
        /// Unique stripe identifier
        stripe_id: StripeId,
        /// Stripe index within the file (0-based)
        stripe_index: u32,
        /// Storage policy for this stripe
        policy: StoragePolicy,
        /// Byte offset of this stripe in the file
        offset: u64,
        /// Size of this stripe in bytes
        size: u64,
        /// List of chunk IDs that make up this stripe
        chunks: Vec<ChunkId>,
    },

    /// Delete a stripe
    DeleteStripe {
        /// Stripe ID to delete
        stripe_id: StripeId,
        /// File ID this stripe belongs to
        file_id: FileId,
    },

    /// Acquire a read lock on a file
    AcquireReadLock {
        /// File ID to lock
        file_id: FileId,
        /// Client requesting the lock
        client_id: u64,
        /// Lock expiration time (for deadlock prevention)
        expires_at: SystemTime,
    },

    /// Acquire a write lock on a file
    AcquireWriteLock {
        /// File ID to lock
        file_id: FileId,
        /// Client requesting the lock
        client_id: u64,
        /// Node ID where the client is located
        node_id: u64,
        /// Lock expiration time (for deadlock prevention)
        expires_at: SystemTime,
    },

    /// Release a lock on a file
    ReleaseLock {
        /// File ID to unlock
        file_id: FileId,
        /// Client releasing the lock
        client_id: u64,
    },

    /// Extend lock expiration time
    ExtendLock {
        /// File ID
        file_id: FileId,
        /// Client ID
        client_id: u64,
        /// New expiration time
        new_expiry: SystemTime,
    },
}

impl Operation {
    /// Convert this high-level operation to a Raft `MetadataOperation`.
    pub fn to_metadata_operation(self) -> MetadataOperation {
        match self {
            Operation::CreateFile {
                file_id,
                path,
                inode,
                metadata,
                policy,
            } => MetadataOperation::FileCreate {
                file_id,
                path,
                inode,
                metadata,
                policy,
            },
            Operation::UpdateFile {
                file_id,
                inode,
                metadata,
                policy,
            } => MetadataOperation::FileUpdate {
                file_id,
                inode,
                metadata,
                policy,
            },
            Operation::DeleteFile { file_id, inode } => {
                MetadataOperation::FileDelete { file_id, inode }
            }
            Operation::CreateStripe {
                file_id,
                stripe_id,
                stripe_index,
                policy,
                offset,
                size,
                chunks,
            } => {
                // Convert Vec<ChunkId> to Vec<ChunkPlacement>
                // Note: transaction_manager doesn't have placement info, so use placeholders
                let chunk_placements: Vec<crate::storage_raft_member::types::ChunkPlacement> =
                    chunks
                        .iter()
                        .enumerate()
                        .map(|(idx, chunk_id)| {
                            crate::storage_raft_member::types::ChunkPlacement {
                                chunk_id: *chunk_id,
                                node_id: crate::storage_raft_member::types::NodeId(0), // Placeholder
                                disk_id: crate::file_store::types::DiskId(0), // Placeholder
                                chunk_index: idx as u32,
                            }
                        })
                        .collect();

                MetadataOperation::CreateStripe {
                    file_id,
                    stripe_id,
                    stripe_index,
                    policy,
                    offset,
                    size,
                    chunks: chunk_placements,
                }
            }
            Operation::DeleteStripe { stripe_id, file_id } => {
                MetadataOperation::DeleteStripe { stripe_id, file_id }
            }
            Operation::AcquireReadLock {
                file_id,
                client_id,
                expires_at,
            } => MetadataOperation::AcquireReadLock {
                file_id,
                client_id,
                expires_at,
            },
            Operation::AcquireWriteLock {
                file_id,
                client_id,
                node_id,
                expires_at,
            } => MetadataOperation::AcquireWriteLock {
                file_id,
                client_id,
                node_id,
                expires_at,
            },
            Operation::ReleaseLock { file_id, client_id } => {
                MetadataOperation::ReleaseLock { file_id, client_id }
            }
            Operation::ExtendLock {
                file_id,
                client_id,
                new_expiry,
            } => MetadataOperation::ExtendLock {
                file_id,
                client_id,
                new_expiry,
            },
        }
    }
}

/// Errors that can occur during transaction management.
#[derive(Error, Debug)]
pub enum Error {
    /// Transaction not found
    #[error("Transaction {0:?} not found")]
    TransactionNotFound(TxId),

    /// Transaction has expired
    #[error("Transaction {0:?} has expired")]
    TransactionExpired(TxId),

    /// Too many active transactions
    #[error("Too many active transactions (limit: {0})")]
    TooManyTransactions(usize),

    /// Transaction has no operations
    #[error("Transaction {0:?} has no operations to commit")]
    EmptyTransaction(TxId),

    /// Timeout value is invalid
    #[error("Invalid timeout: {0:?} (max: {1:?})")]
    InvalidTimeout(Duration, Duration),

    /// Operation validation failed
    #[error("Operation validation failed: {0}")]
    ValidationFailed(String),

    /// Parent directory not found
    #[error("Parent directory not found: {0:?}")]
    ParentNotFound(PathBuf),

    /// File already exists
    #[error("File already exists: {0:?}")]
    FileAlreadyExists(PathBuf),

    /// File not found
    #[error("File not found: {0:?}")]
    FileNotFound(FileId),

    /// Stripe not found
    #[error("Stripe not found: {0:?}")]
    StripeNotFound(StripeId),

    /// Lock conflict - another client holds a conflicting lock
    #[error("Lock conflict on file {0:?}: {1}")]
    LockConflict(FileId, String),

    /// Lock not found or already released
    #[error("Lock not found for file {0:?}, client {1}")]
    LockNotFound(FileId, u64),

    /// Lock has expired
    #[error("Lock has expired for file {0:?}, client {1}")]
    LockExpired(FileId, u64),

    /// Lock expiration time is invalid (too short or exceeds configured timeout)
    #[error("Invalid lock expiration: must be between 1s and {0}s from now, but expires_at would be {1:?} from now")]
    InvalidLockExpiry(u64, Duration),

    /// Raft operation failed
    #[error("Raft operation failed: {0}")]
    RaftError(String),

    /// Metadata store error
    #[error("MetadataStore error: {0}")]
    MetadataStoreError(String),
}

// Allow conversion from crate::metadata_store::types::Error
impl From<crate::metadata_store::types::Error> for Error {
    fn from(err: crate::metadata_store::types::Error) -> Self {
        use crate::metadata_store::types::Error as MsError;
        match err {
            MsError::FileNotFoundByFileId(file_id) => Error::FileNotFound(file_id),
            MsError::FileAlreadyExists(path) => Error::FileAlreadyExists(path),
            MsError::ParentNotFound(path) => Error::ParentNotFound(path),
            MsError::StripeNotFound(stripe_id) => Error::StripeNotFound(stripe_id),
            other => Error::MetadataStoreError(format!("{:?}", other)),
        }
    }
}

// Allow conversion from crate::storage_raft_member::types::Error
impl From<crate::storage_raft_member::types::Error> for Error {
    fn from(err: crate::storage_raft_member::types::Error) -> Self {
        Error::RaftError(format!("{:?}", err))
    }
}

/// Result type for transaction manager operations.
pub type Result<T> = std::result::Result<T, Error>;
