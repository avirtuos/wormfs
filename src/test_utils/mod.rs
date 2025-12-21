//! Test utilities module for WormFS
//!
//! This module provides mock implementations and test helpers that are only available
//! when the `test-utils` feature is enabled or during testing.
//!
//! ## Usage in Unit Tests
//!
//! Unit tests automatically have access to mocks:
//! ```rust
//! #[cfg(test)]
//! mod tests {
//!     use super::*;
//!     use crate::storage_node::MockStorageNode;
//!     
//!     #[tokio::test]
//!     async fn test_with_mock() {
//!         let mut mock = MockStorageNode::new();
//!         mock.expect_is_leader().returning(|| true);
//!         assert!(mock.is_leader());
//!     }
//! }
//! ```
//!
//! ## Usage in Integration Tests
//!
//! Integration tests in `/tests` need to enable the feature in Cargo.toml:
//! ```toml
//! [[test]]
//! name = "integration"
//! required-features = ["test-utils"]
//! ```

#[cfg(feature = "test-utils")]
pub mod mocks {
    //! Re-exported mocks for all WormFS components.

    pub use crate::storage_node::MockStorageNode;
    pub use crate::storage_raft_member::MockStorageRaftMember;
    // Note: StorageNetwork does not have automock due to Clone trait requirement
    // Manual mocks can be created as needed
    pub use crate::file_store::MockFileStore;
    // Note: MetadataStore does not have automock due to Clone trait + 6 associated types
    // Manual mocks can be created as needed
    pub use crate::filesystem_service::MockFileSystemService;
    pub use crate::metric_service::MockMetricService;
    pub use crate::snapshot_store::MockSnapshotStore;
    pub use crate::storage_endpoint::MockStorageEndpoint;
    pub use crate::storage_watchdog::MockStorageWatchdog;
    pub use crate::transaction_log_store::MockTransactionLogStore;
}
