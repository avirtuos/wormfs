//! Library entrypoint for wormfs.
//!
//! This re-exports the erasure and file_system modules as a library surface so the binary
//! (and other crates) can consume them via `use wormfs::...`.

pub mod erasure;
pub mod file_system;

pub use file_system::{file::WormFile, stripe::WorkStripe, chunk::ChunkHeader};
pub use erasure::*;
