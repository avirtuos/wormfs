//! WormFS - A distributed, erasure-coded filesystem with configurable redundancy
//!
//! This library provides the core functionality for WormFS, including chunk management,
//! erasure coding, and distributed storage operations.

pub mod metadata;
pub mod node;
pub mod raft;
pub mod storage;
pub mod storage_endpoint;
pub mod transport;

// Re-export commonly used types
pub use metadata::*;
pub use node::*;
pub use storage::*;
pub use transport::*;

// Re-export specific types from storage_endpoint to avoid ambiguity
pub use storage_endpoint::{StorageEndpointConfig, StorageEndpointError, StorageEndpointServer};
