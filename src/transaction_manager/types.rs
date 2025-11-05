//! Common types for the TransactionManager component.

use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use thiserror::Error;

// Re-export types from other modules
pub use crate::file_store::types::{ChunkId, DiskId, FileId, StripeId};
pub use crate::storage_raft_member::types::{FileMetadata, MetadataOperation, StoragePolicy, TxId};

/// Configuration for TransactionManager.
#[derive(Debug, Clone)]
pub struct Config {
    /// Maximum number of active transactions
    pub max_active_transactions: usize,

    /// Default timeout for transactions if not specified
    pub default_timeout: Duration,

    /// Maximum timeout allowed for any transaction
    pub max_timeout: Duration,

    /// Interval for cleanup task to check for expired transactions
    pub cleanup_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_active_transactions: 1000,
            default_timeout: Duration::from_secs(30),
            max_timeout: Duration::from_secs(300),
            cleanup_interval: Duration::from_secs(1),
        }
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
}

impl Operation {
    /// Convert this high-level operation to a Raft `MetadataOperation`.
    pub fn to_metadata_operation(self) -> MetadataOperation {
        match self {
            Operation::CreateFile {
                path,
                inode,
                metadata,
                policy,
            } => MetadataOperation::FileCreate {
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
            } => MetadataOperation::CreateStripe {
                file_id,
                stripe_id,
                stripe_index,
                policy,
                offset,
                size,
                chunks,
            },
            Operation::DeleteStripe { stripe_id, file_id } => {
                MetadataOperation::DeleteStripe { stripe_id, file_id }
            }
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
