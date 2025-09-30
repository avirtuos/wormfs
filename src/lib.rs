//! WormFS - A distributed, erasure-coded filesystem with configurable redundancy
//! 
//! This crate provides the core functionality for WormFS, including chunk format,
//! erasure coding, metadata management, and distributed storage operations.

pub mod chunk_format;

pub use chunk_format::*;
