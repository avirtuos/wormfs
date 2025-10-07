// Core Raft types and metadata operations for WormFS
//
// This module defines the fundamental types used in the Raft consensus layer,
// including metadata operations that will be replicated across the cluster.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Node identifier in the Raft cluster
pub type NodeId = u64;

/// Unique identifier for a file in the system
pub type FileId = Uuid;

/// Unique identifier for a stripe within a file
pub type StripeId = Uuid;

/// Unique identifier for a chunk within a stripe
pub type ChunkId = Uuid;

/// Types of locks that can be acquired on files
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockType {
    /// Read lock - multiple readers allowed
    Read,
    /// Write lock - exclusive access
    Write,
}

/// Metadata operations that are replicated through Raft consensus
///
/// These operations represent all state changes to the metadata store.
/// They are serialized, replicated via Raft, and then applied to SQLite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataOp {
    // File operations
    /// Create a new file with the given metadata
    /// Note: FUSE layer is responsible for path-to-UUID mapping
    /// The path should be stored within FileMetadata if needed
    CreateFile { metadata: FileMetadata },

    /// Update existing file metadata
    /// Note: FUSE layer is responsible for path-to-UUID mapping
    UpdateFile {
        file_id: FileId,
        metadata: FileMetadata,
    },

    /// Delete a file and its metadata
    /// Note: FUSE layer is responsible for path-to-UUID mapping
    DeleteFile { file_id: FileId },

    // Chunk operations
    /// Register a new chunk location
    RegisterChunk {
        chunk_id: ChunkId,
        node_id: NodeId,
        stripe_id: StripeId,
        file_id: FileId,
    },

    /// Update the location of a chunk (for rebalancing/recovery)
    UpdateChunkLocation {
        chunk_id: ChunkId,
        new_node_id: NodeId,
    },

    /// Remove a chunk from the metadata store
    RemoveChunk { chunk_id: ChunkId },

    // Lock operations
    /// Acquire a lock on a file
    /// Note: FUSE layer is responsible for path-to-UUID mapping
    AcquireLock {
        file_id: FileId,
        lock_type: LockType,
        client_id: String,
    },

    /// Release a lock on a file
    /// Note: FUSE layer is responsible for path-to-UUID mapping
    ReleaseLock { file_id: FileId, client_id: String },

    /// Extend the expiration time of an existing lock
    /// Note: FUSE layer is responsible for path-to-UUID mapping
    ExtendLock { file_id: FileId, client_id: String },

    // Node membership operations
    /// Add a new node to the cluster
    AddNode { node_id: NodeId, address: String },

    /// Remove a node from the cluster
    RemoveNode { node_id: NodeId },
}

/// File metadata stored in the system
///
/// This represents the essential metadata needed for filesystem operations.
/// It is serialized as part of CreateFile and UpdateFile operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub file_id: FileId,
    pub size: u64,
    pub permissions: u32,
    pub uid: u32,
    pub gid: u32,
    pub created_at: i64,
    pub modified_at: i64,
    pub accessed_at: i64,
    pub stripe_size: u64,
    pub data_shards: u8,
    pub parity_shards: u8,
}

/// Read consistency modes for queries
///
/// These determine how reads are processed and what consistency guarantees
/// they provide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    /// Linearizable read - goes through Raft consensus, requires majority
    /// Guarantees the most recent committed value
    Linearizable,

    /// Lease-based read - leader serves directly using lease mechanism
    /// Requires leader to have a valid lease with the majority
    /// Provides linearizability with better performance
    LeaseRead,

    /// Stale read - reads from local state without consensus
    /// Works in minority partitions, may return stale data
    /// Response includes staleness indicator
    StaleRead,
}

/// Response wrapper that includes staleness information
#[derive(Debug, Clone)]
pub struct QueryResponse<T> {
    pub data: T,
    pub last_applied_index: u64,
    pub is_stale: bool,
}

impl<T> QueryResponse<T> {
    pub fn fresh(data: T, index: u64) -> Self {
        Self {
            data,
            last_applied_index: index,
            is_stale: false,
        }
    }

    pub fn stale(data: T, index: u64) -> Self {
        Self {
            data,
            last_applied_index: index,
            is_stale: true,
        }
    }
}

/// Type configuration for WormFS Raft using declare_raft_types macro
use std::io::Cursor;

pub type SnapshotData = Cursor<Vec<u8>>;

// Declare Raft types using the macro for proper API compatibility
openraft::declare_raft_types!(
    pub WormFSTypeConfig:
        D = MetadataOp,
        R = MetadataOpResponse,
        Node = (),
);

/// Helper type aliases for cleaner code
pub mod typ {
    use super::*;
    use openraft::error::Infallible;

    pub type Entry = openraft::Entry<WormFSTypeConfig>;

    pub type RaftError<E = Infallible> = openraft::error::RaftError<u64, E>;
    pub type RPCError<E = Infallible> = openraft::error::RPCError<u64, (), RaftError<E>>;

    pub type ClientWriteError = openraft::error::ClientWriteError<u64, ()>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<u64, ()>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<u64, ()>;
    pub type InitializeError = openraft::error::InitializeError<u64, ()>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<WormFSTypeConfig>;
}

/// Response type for metadata operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataOpResponse {
    /// Operation succeeded
    Success,
    /// Operation failed with an error message
    Error(String),
}
