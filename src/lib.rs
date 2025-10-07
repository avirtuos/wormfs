//! WormFS - A distributed, erasure-coded filesystem with configurable redundancy
//!
//! This library provides the core functionality for WormFS, including chunk management,
//! erasure coding, and distributed storage operations.

pub mod metadata;
pub mod node;
pub mod raft;
pub mod storage;
pub mod transport;

// Re-export commonly used types
pub use metadata::*;
pub use node::*;
pub use storage::*;
pub use transport::*;
