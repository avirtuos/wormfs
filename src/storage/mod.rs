//! Storage module for WormFS
//!
//! This module contains all storage-related components:
//! - Chunk format and operations
//! - Erasure coding
//! - Disk management
//! - Storage layout
//! - Chunk placement strategies

pub mod chunk_format;
pub mod chunk_placement;
pub mod disk_manager;
pub mod erasure_coding;
pub mod storage_layout;

// Re-export commonly used types
pub use chunk_format::{
    calculate_checksum, read_chunk, write_chunk, ChunkHeader, CompressionAlgorithm,
};
pub use chunk_placement::ChunkPlacementStrategy;
pub use disk_manager::DiskManager;
pub use erasure_coding::ErasureCodingConfig;
pub use storage_layout::StorageLayout;
