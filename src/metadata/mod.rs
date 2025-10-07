//! Metadata management module
//!
//! This module provides metadata storage and management functionality for WormFS,
//! including file metadata, chunk tracking, stripe management, and file locking.

pub mod metadata_store;

// Re-export commonly used types
pub use metadata_store::{
    ChunkId, ChunkMetadata, FileLock, FileMetadata, LockType, MetadataError, MetadataResult,
    MetadataStats, MetadataStore, StorageLocation, StripeId, StripeMetadata,
};
