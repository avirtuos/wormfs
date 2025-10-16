//! Common types for the FileStore component.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Unique identifier for a file in the system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FileId(pub u64);

impl FileId {
    /// Create a new FileId from a u64.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Generate a new unique FileId using UUID v4.
    ///
    /// Uses UUID v4 for globally unique, collision-resistant IDs.
    /// The 128-bit UUID is truncated to 64 bits, which provides
    /// sufficient uniqueness for non-cryptographic purposes.
    ///
    /// # Example
    ///
    /// ```
    /// use wormfs::file_store::types::FileId;
    ///
    /// let id1 = FileId::generate();
    /// let id2 = FileId::generate();
    /// assert_ne!(id1, id2); // Extremely unlikely to collide
    /// ```
    pub fn generate() -> Self {
        Self::new(uuid::Uuid::new_v4().as_u128() as u64)
    }

    /// Get the inner u64 value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Unique identifier for a stripe within a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StripeId(pub u64);

impl StripeId {
    /// Create a new StripeId from a u64.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Generate a new unique StripeId using UUID v4.
    ///
    /// See [`FileId::generate()`] for details on UUID-based ID generation.
    pub fn generate() -> Self {
        Self::new(uuid::Uuid::new_v4().as_u128() as u64)
    }

    /// Get the inner u64 value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Unique identifier for a chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ChunkId(pub u64);

impl ChunkId {
    /// Create a new ChunkId from a u64.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Generate a new unique ChunkId using UUID v4.
    ///
    /// See [`FileId::generate()`] for details on UUID-based ID generation.
    pub fn generate() -> Self {
        Self::new(uuid::Uuid::new_v4().as_u128() as u64)
    }

    /// Get the inner u64 value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Unique identifier for a storage node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl NodeId {
    /// Create a new NodeId.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the inner u64 value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Unique identifier for a disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DiskId(pub u64);

impl DiskId {
    /// Create a new DiskId.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the inner u64 value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Unique identifier for a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TxId(pub u64);

impl TxId {
    /// Create a new TxId.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the inner u64 value.
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// Configuration for FileStore.
#[derive(Debug, Clone)]
pub struct Config {
    /// Paths to disk mount points for chunk storage
    pub disk_paths: Vec<PathBuf>,

    /// Maximum chunk size in bytes
    ///
    /// This controls the target size of each chunk/shard after erasure encoding.
    /// Actual stripe size will be chunk_size × data_shards.
    ///
    /// Affects:
    /// - Disk I/O granularity (larger = more sequential, fewer seeks)
    /// - Memory usage during encoding/decoding
    /// - Network transfer sizes (Phase 2+)
    /// - Rebuild granularity
    pub max_chunk_size: u64,

    /// Default number of data shards for erasure coding
    pub default_data_shards: u8,

    /// Default number of parity shards for erasure coding
    pub default_parity_shards: u8,

    /// Maximum concurrent chunk operations
    pub max_concurrent_operations: usize,

    /// Chunk verification interval
    pub verification_interval: Duration,

    /// Orphan cleanup age threshold
    pub orphan_cleanup_age: Duration,
}

/// Errors that can occur during FileStore operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Erasure encoding failed
    #[error("Erasure encoding failed: {0}")]
    ErasureEncodingFailed(String),

    /// Erasure decoding failed
    #[error("Erasure decoding failed: {0}")]
    ErasureDecodingFailed(String),

    /// Insufficient storage nodes/disks available
    #[error("Insufficient storage: need {needed}, have {available}")]
    InsufficientStorage { needed: usize, available: usize },

    /// Chunk write failed
    #[error("Failed to write chunk {chunk_id:?}: {reason}")]
    ChunkWriteFailed { chunk_id: ChunkId, reason: String },

    /// Chunk read failed
    #[error("Failed to read chunk {chunk_id:?}: {reason}")]
    ChunkReadFailed { chunk_id: ChunkId, reason: String },

    /// Chunk not found
    #[error("Chunk {0:?} not found")]
    ChunkNotFound(ChunkId),

    /// Chunk checksum mismatch
    #[error("Chunk {chunk_id:?} checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch {
        chunk_id: ChunkId,
        expected: String,
        actual: String,
    },

    /// Insufficient chunks for reconstruction
    #[error("Insufficient chunks for reconstruction: need {needed}, have {available}")]
    InsufficientChunks { needed: usize, available: usize },

    /// Metadata lookup failed
    #[error("Metadata lookup failed: {0}")]
    MetadataLookupFailed(String),

    /// Transaction state error
    #[error("Invalid transaction state: {0}")]
    InvalidTransactionState(String),

    /// Disk not found
    #[error("Disk {0:?} not found")]
    DiskNotFound(DiskId),

    /// Disk initialization failed
    #[error("Failed to initialize disk at {path:?}: {reason}")]
    DiskInitFailed { path: PathBuf, reason: String },

    /// Disk removal failed
    #[error("Failed to remove disk {disk_id:?}: {reason}")]
    DiskRemovalFailed { disk_id: DiskId, reason: String },

    /// Disk full
    #[error("Disk {0:?} is full")]
    DiskFull(DiskId),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Erasure coding operation failed
    #[error("Erasure coding failed: {0}")]
    ErasureCodingFailed(String),

    /// Insufficient shards available for reconstruction
    #[error("Insufficient shards: need {required}, have {available}")]
    InsufficientShards { required: usize, available: usize },

    /// Chunk corrupt
    #[error("Chunk {0:?} is corrupt: {1}")]
    ChunkCorrupt(ChunkId, String),

    /// Feature not yet implemented
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Vote result from prepare phase of 2PC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrepareVote {
    /// Chunk successfully prepared, ready to commit
    Commit,

    /// Preparation failed, transaction should abort
    Abort,
}

/// Chunk verification result.
#[derive(Debug, Clone)]
pub struct VerificationResult {
    /// Whether chunk checksum is valid
    pub checksum_valid: bool,

    /// Whether chunk is readable
    pub readable: bool,

    /// Error message if verification failed
    pub error: Option<String>,
}

/// Stripe rebuild result.
#[derive(Debug, Clone)]
pub struct RebuildResult {
    /// Number of chunks that were rebuilt
    pub chunks_rebuilt: usize,

    /// Number of chunks that were verified as healthy
    pub chunks_verified: usize,

    /// List of chunks that could not be rebuilt
    pub failed_chunks: Vec<ChunkId>,
}

/// Disk usage statistics.
#[derive(Debug, Clone)]
pub struct DiskStats {
    /// Disk identifier
    pub disk_id: DiskId,

    /// Mount path
    pub path: PathBuf,

    /// Total space in bytes
    pub total_space: u64,

    /// Free space in bytes
    pub free_space: u64,

    /// Used space in bytes
    pub used_space: u64,

    /// Number of chunks stored
    pub chunk_count: u64,
}

// ===== Storage Policy Types =====

/// Compression algorithm used for chunk data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    // Future: Lz4, Zstd, etc.
}

/// Erasure coding algorithm used for stripe encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErasureAlgorithm {
    /// Reed-Solomon erasure coding
    ReedSolomon,
    /// No erasure coding (for testing)
    None,
}

/// Storage policy defining how data is encoded and distributed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePolicy {
    /// Number of data shards for erasure coding
    pub data_shards: u8,

    /// Number of parity shards for erasure coding
    pub parity_shards: u8,

    /// Target chunk size in bytes
    ///
    /// This is the target size for each chunk/shard after erasure encoding.
    /// The actual stripe size will be chunk_size × data_shards.
    pub chunk_size: u64,

    /// Compression algorithm to use
    pub compression: CompressionAlgorithm,
}

impl StoragePolicy {
    /// Create a new storage policy.
    pub fn new(
        data_shards: u8,
        parity_shards: u8,
        chunk_size: u64,
        compression: CompressionAlgorithm,
    ) -> Self {
        Self {
            data_shards,
            parity_shards,
            chunk_size,
            compression,
        }
    }

    /// Get the total number of shards (data + parity).
    pub fn total_shards(&self) -> u8 {
        self.data_shards + self.parity_shards
    }

    /// Get the minimum number of shards needed for reconstruction.
    pub fn min_shards_for_recovery(&self) -> u8 {
        self.data_shards
    }

    /// Get the maximum number of failures that can be tolerated.
    pub fn max_failures(&self) -> u8 {
        self.parity_shards
    }

    /// Calculate the stripe size (chunk_size × data_shards).
    ///
    /// The stripe size is the maximum amount of original data that will be
    /// encoded into a single stripe. Each stripe is divided into data_shards
    /// chunks, each of size chunk_size (approximately).
    pub fn stripe_size(&self) -> u64 {
        self.chunk_size * self.data_shards as u64
    }
}

// ===== Chunk Data Types =====

/// Chunk data including header and payload.
#[derive(Debug, Clone)]
pub struct ChunkData {
    /// Chunk header with metadata
    pub header: ChunkHeader,

    /// Raw chunk data (potentially compressed)
    pub data: Vec<u8>,
}

impl ChunkData {
    /// Create new chunk data.
    pub fn new(header: ChunkHeader, data: Vec<u8>) -> Self {
        Self { header, data }
    }

    /// Get the total size of the chunk (header + data).
    pub fn total_size(&self) -> usize {
        self.header.serialized_size() + self.data.len()
    }
}

/// Chunk header containing metadata for a stored chunk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkHeader {
    /// Magic bytes identifier (always "WORM")
    pub magic: [u8; 4],

    /// Format version for compatibility
    pub format_version: u16,

    /// CRC32 checksum of the chunk data
    pub chunk_checksum: u32,

    /// Unique identifier for this chunk
    pub chunk_id: ChunkId,

    /// Stripe this chunk belongs to
    pub stripe_id: StripeId,

    /// File this chunk belongs to
    pub file_id: FileId,

    /// Starting byte offset of this stripe in the file
    pub stripe_start_offset: u64,

    /// Ending byte offset of this stripe in the file
    pub stripe_end_offset: u64,

    /// Index of this chunk within the stripe (0 to total_shards-1)
    pub chunk_index: u8,

    /// Number of data shards in this stripe
    pub data_shards: u8,

    /// Number of parity shards in this stripe
    pub parity_shards: u8,

    /// Erasure coding algorithm used
    pub erasure_algorithm: ErasureAlgorithm,

    /// Compression algorithm used
    pub compression_algorithm: CompressionAlgorithm,

    /// CRC32 checksum of the entire stripe (for verification)
    pub stripe_checksum: u32,
}

impl ChunkHeader {
    /// Magic bytes for chunk headers.
    pub const MAGIC: [u8; 4] = *b"WORM";

    /// Current format version.
    pub const FORMAT_VERSION: u16 = 1;

    /// Create a new chunk header.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chunk_id: ChunkId,
        stripe_id: StripeId,
        file_id: FileId,
        stripe_start_offset: u64,
        stripe_end_offset: u64,
        chunk_index: u8,
        data_shards: u8,
        parity_shards: u8,
        erasure_algorithm: ErasureAlgorithm,
        compression_algorithm: CompressionAlgorithm,
        stripe_checksum: u32,
        chunk_checksum: u32,
    ) -> Self {
        Self {
            magic: Self::MAGIC,
            format_version: Self::FORMAT_VERSION,
            chunk_checksum,
            chunk_id,
            stripe_id,
            file_id,
            stripe_start_offset,
            stripe_end_offset,
            chunk_index,
            data_shards,
            parity_shards,
            erasure_algorithm,
            compression_algorithm,
            stripe_checksum,
        }
    }

    /// Compute the checksum for the given chunk data.
    pub fn compute_checksum(data: &[u8]) -> u32 {
        crc32fast::hash(data)
    }

    /// Verify that the header's magic bytes are correct.
    pub fn verify_magic(&self) -> bool {
        self.magic == Self::MAGIC
    }

    /// Get the total number of shards for this chunk's stripe.
    pub fn total_shards(&self) -> u8 {
        self.data_shards + self.parity_shards
    }

    /// Estimate the serialized size of this header.
    pub fn serialized_size(&self) -> usize {
        // This is an approximation - actual size may vary with serialization format
        std::mem::size_of::<Self>()
    }
}

// ===== Stripe Metadata Types =====

/// Metadata describing a stripe and its chunk locations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StripeMetadata {
    /// Unique identifier for this stripe
    pub stripe_id: StripeId,

    /// File this stripe belongs to
    pub file_id: FileId,

    /// Starting byte offset of this stripe in the file
    pub offset: u64,

    /// Size of the stripe data in bytes
    pub size: u64,

    /// CRC32 checksum of the stripe data
    pub checksum: u32,

    /// Locations of all chunks in this stripe
    pub chunks: Vec<ChunkMetadata>,
}

impl StripeMetadata {
    /// Create new stripe metadata.
    pub fn new(
        stripe_id: StripeId,
        file_id: FileId,
        offset: u64,
        size: u64,
        checksum: u32,
        chunks: Vec<ChunkMetadata>,
    ) -> Self {
        Self {
            stripe_id,
            file_id,
            offset,
            size,
            checksum,
            chunks,
        }
    }

    /// Get the number of chunks in this stripe.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }
}

/// Location of a chunk within the storage cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMetadata {
    /// Unique identifier for this chunk
    pub chunk_id: ChunkId,

    /// Node where this chunk is stored
    pub node_id: NodeId,

    /// Disk where this chunk is stored
    pub disk_id: DiskId,

    /// Index of this chunk within its stripe (0 to total_shards-1)
    pub chunk_index: u8,
}

impl ChunkMetadata {
    /// Create a new chunk location.
    pub fn new(chunk_id: ChunkId, node_id: NodeId, disk_id: DiskId, chunk_index: u8) -> Self {
        Self {
            chunk_id,
            node_id,
            disk_id,
            chunk_index,
        }
    }
}

// =============================================================================
// Chunk Caching Types
// =============================================================================

/// Entry in the chunk cache.
#[derive(Debug, Clone)]
pub struct ChunkCacheEntry {
    /// Chunk identifier
    pub chunk_id: ChunkId,
    /// Cached chunk data (encoded, not decoded)
    pub data: Vec<u8>,
    /// When the chunk was cached
    pub cached_at: std::time::Instant,
    /// Size of the chunk in bytes
    pub size: usize,
}

/// Prefetch policy for stripe chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrefetchPolicy {
    /// No prefetching
    None,
    /// Prefetch next stripe's chunks
    NextStripe,
    /// Prefetch N stripes ahead
    Lookahead { count: usize },
}
