//! Example integration test demonstrating mock usage
//!
//! This test requires the `test-utils` feature to be enabled.

#[cfg(feature = "test-utils")]
mod with_mocks {
    use wormfs::test_utils::mocks::*;
    // Import traits to use their methods on mocks
    use wormfs::{FileStore, StorageNode};

    #[tokio::test]
    async fn test_storage_node_mock() {
        // Use default() to create mock instances
        let mut mock = MockStorageNode::default();

        // Set up expectations
        mock.expect_is_leader().times(1).returning(|| true);

        // Use the mock
        assert!(mock.is_leader());
    }

    #[tokio::test]
    async fn test_file_store_mock() {
        // Use default() to create mock instances
        let mut mock = MockFileStore::default();

        // Mock the get_disk_stats method
        mock.expect_get_disk_stats().returning(Vec::new);

        let stats = mock.get_disk_stats();
        assert_eq!(stats.len(), 0);
    }
}

#[cfg(not(feature = "test-utils"))]
#[test]
fn test_without_mocks() {
    // This test runs when test-utils feature is not enabled
    // Demonstrates that mocks are properly gated behind the feature
    println!("Running without test-utils feature");
}
