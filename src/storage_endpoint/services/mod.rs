//! Services for StorageEndpoint
//!
//! This module contains gRPC service implementations for data transfer:
//! - Snapshot transfer (current)
//! - Chunk data operations (future)

pub mod snapshot;

pub use snapshot::SnapshotTransferServiceImpl;

// Future: Task for Phase 3A
// pub mod chunk_data;
