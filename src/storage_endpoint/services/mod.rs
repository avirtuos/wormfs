//! gRPC service implementations for StorageEndpoint.
//!
//! This module contains implementations of all gRPC services exposed by the
//! StorageEndpoint. Each service delegates to the appropriate internal components.

/// Buffer size for gRPC streaming response channels
#[cfg(feature = "tonic")]
pub(crate) const GRPC_STREAM_CHANNEL_BUFFER_SIZE: usize = 32;

#[cfg(feature = "tonic")]
pub mod chunk;
#[cfg(feature = "tonic")]
pub mod conversions;
#[cfg(feature = "tonic")]
pub mod filesystem;
#[cfg(feature = "tonic")]
pub mod health;
#[cfg(feature = "tonic")]
pub mod snapshot;
#[cfg(feature = "tonic")]
pub mod transaction_log;

#[cfg(feature = "tonic")]
pub use chunk::ChunkServiceImpl;
#[cfg(feature = "tonic")]
pub use filesystem::FilesystemServiceImpl;
#[cfg(feature = "tonic")]
pub use health::HealthServiceImpl;
#[cfg(feature = "tonic")]
pub use snapshot::SnapshotServiceImpl;
#[cfg(feature = "tonic")]
pub use transaction_log::TransactionLogServiceImpl;
