//! WormFS - A distributed, erasure-coded filesystem with configurable redundancy
//!
//! This library provides the core functionality for WormFS, including chunk management,
//! erasure coding, and distributed storage operations.

pub mod chunk_format;
pub mod erasure_coding;
pub mod integrity_checker;
pub mod metadata_store;
pub mod storage_layout;
pub mod storage_node;

pub use chunk_format::*;
pub use erasure_coding::*;
pub use integrity_checker::*;
pub use metadata_store::*;
pub use storage_layout::*;
pub use storage_node::*;
