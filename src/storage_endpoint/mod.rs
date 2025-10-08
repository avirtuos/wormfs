//! Storage Endpoint module for WormFS
//!
//! This module provides gRPC-based data transfer services for:
//! - Snapshot transfer (efficient Raft snapshot distribution)
//! - Chunk data transfer (future: direct chunk read/write operations)
//!
//! The StorageEndpoint is separate from the StorageNetwork (libp2p) layer,
//! allowing independent lifecycle management and scalability.

pub mod config;
pub mod server;
pub mod services;

pub use config::StorageEndpointConfig;
pub use server::StorageEndpointServer;
pub use services::SnapshotTransferServiceImpl;

use std::fmt;

/// Storage endpoint error types
#[derive(Debug, thiserror::Error)]
pub enum StorageEndpointError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("gRPC error: {0}")]
    GrpcError(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Server error: {0}")]
    Server(String),

    #[error("Service not available: {0}")]
    ServiceUnavailable(String),
}

pub type Result<T> = std::result::Result<T, StorageEndpointError>;

impl fmt::Display for StorageEndpointConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StorageEndpoint {}:{} (node {})",
            self.bind_address, self.port, self.node_id
        )
    }
}
