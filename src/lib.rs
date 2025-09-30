//! WormFS - A distributed, erasure-coded filesystem with configurable redundancy
//!
//! This library provides the core functionality for WormFS, including chunk management,
//! erasure coding, and distributed storage operations.

pub mod chunk_format;
pub mod erasure_coding;

pub use chunk_format::*;
pub use erasure_coding::*;
