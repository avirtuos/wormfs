//! Node management module
//!
//! This module provides node-level functionality for WormFS, including
//! storage node operations and integrity checking.

pub mod integrity_checker;
pub mod storage_node;

// Re-export commonly used types
pub use integrity_checker::{
    IntegrityCheckConfig, IntegrityCheckResult, IntegrityCheckStats, IntegrityChecker,
    IntegrityError, IntegrityIssue, IntegrityResult, IssueSeverity, IssueType,
};
pub use storage_node::{StorageNode, StorageNodeConfig, StorageNodeError};
