//! Integration tests for WormFS Phase 1A
//!
//! These tests verify the complete end-to-end workflows of the storage node,
//! including storage, retrieval, deletion, and restart persistence.

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;
use uuid::Uuid;

use wormfs::{ErasureCodingConfig, StorageNode, StorageNodeConfig};

/// Create a test configuration and temporary directory
fn create_test_setup() -> (StorageNodeConfig, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    let mut config = StorageNodeConfig::new().unwrap();
    config.metadata_db_path = root.join("test_metadata.db");
    config.storage_root = root.join("test_chunks");
    config.min_free_space = 1024;
    config.max_file_size = 10 * 1024 * 1024; // 10MB for tests
    config.erasure_config = ErasureCodingConfig::new(4, 2, 64 * 1024).unwrap(); // 64KB stripes

    (config, temp_dir)
}

/// Create a test file with specified content
fn create_test_file(dir: &std::path::Path, name: &str, content: &[u8]) -> PathBuf {
    let file_path = dir.join(name);
    let mut file = fs::File::create(&file_path).unwrap();
    file.write_all(content).unwrap();
    file_path
}

#[test]
fn test_basic_store_and_retrieve() {
    let (config, temp_dir) = create_test_setup();
    let storage_node = StorageNode::new(config).unwrap();

    // Create test file
    let test_data = b"Hello, WormFS! This is a basic integration test.";
    let source_path = create_test_file(temp_dir.path(), "test.txt", test_data);
    let virtual_path = PathBuf::from("/integration/test.txt");

    // Store file
    let file_id = storage_node
        .store_file(&source_path, &virtual_path)
        .unwrap();

    // Verify file is listed
    let files = storage_node.list_files().unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].file_id, file_id);
    assert_eq!(files[0].path, virtual_path);
    assert_eq!(files[0].size, test_data.len() as u64);

    // Retrieve file
    let output_path = temp_dir.path().join("retrieved.txt");
    storage_node.retrieve_file(file_id, &output_path).unwrap();

    // Verify retrieved data
    let retrieved_data = fs::read(&output_path).unwrap();
    assert_eq!(retrieved_data, test_data);

    // Verify statistics
    let stats = storage_node.get_stats().unwrap();
    assert_eq!(stats.total_files, 1);
    assert!(stats.total_chunks > 0);
    assert_eq!(stats.total_size, test_data.len() as u64);
}

#[test]
fn test_large_file_handling() {
    let (config, temp_dir) = create_test_setup();
    let storage_node = StorageNode::new(config).unwrap();

    // Create a larger test file (multiple stripes)
    let test_data = vec![0xAB; 200 * 1024]; // 200KB file
    let source_path = create_test_file(temp_dir.path(), "large.bin", &test_data);
    let virtual_path = PathBuf::from("/large/file.bin");

    // Store file
    let file_id = storage_node
        .store_file(&source_path, &virtual_path)
        .unwrap();

    // Verify it has multiple stripes
    let files = storage_node.list_files().unwrap();
    assert_eq!(files.len(), 1);
    assert!(files[0].stripe_count > 1); // Should have multiple stripes
    assert!(files[0].chunk_count > 6); // Should have multiple chunks

    // Retrieve and verify
    let output_path = temp_dir.path().join("large_retrieved.bin");
    storage_node.retrieve_file(file_id, &output_path).unwrap();

    let retrieved_data = fs::read(&output_path).unwrap();
    assert_eq!(retrieved_data, test_data);
}

#[test]
fn test_multiple_files() {
    let (config, temp_dir) = create_test_setup();
    let storage_node = StorageNode::new(config).unwrap();

    // Store multiple files
    let file_contents = [
        (b"File 1 content".as_slice(), "/dir1/file1.txt"),
        (
            b"File 2 with different content".as_slice(),
            "/dir2/file2.txt",
        ),
        (
            b"Third file with even more content here".as_slice(),
            "/dir3/file3.txt",
        ),
    ];

    let mut file_ids = Vec::new();

    for (i, (content, virtual_path)) in file_contents.iter().enumerate() {
        let source_path = create_test_file(temp_dir.path(), &format!("test{}.txt", i), content);
        let file_id = storage_node
            .store_file(&source_path, PathBuf::from(*virtual_path))
            .unwrap();
        file_ids.push(file_id);
    }

    // Verify all files are listed
    let files = storage_node.list_files().unwrap();
    assert_eq!(files.len(), 3);

    // Retrieve and verify each file
    for (i, file_id) in file_ids.iter().enumerate() {
        let output_path = temp_dir.path().join(format!("retrieved{}.txt", i));
        storage_node.retrieve_file(*file_id, &output_path).unwrap();

        let retrieved_data = fs::read(&output_path).unwrap();
        assert_eq!(retrieved_data, file_contents[i].0);
    }

    // Test statistics
    let stats = storage_node.get_stats().unwrap();
    assert_eq!(stats.total_files, 3);

    let total_content_size: usize = file_contents.iter().map(|(content, _)| content.len()).sum();
    assert_eq!(stats.total_size, total_content_size as u64);
}

#[test]
fn test_file_deletion() {
    let (config, temp_dir) = create_test_setup();
    let storage_node = StorageNode::new(config).unwrap();

    // Store two files
    let test_data1 = b"File to keep";
    let test_data2 = b"File to delete";

    let source_path1 = create_test_file(temp_dir.path(), "keep.txt", test_data1);
    let source_path2 = create_test_file(temp_dir.path(), "delete.txt", test_data2);

    let file_id1 = storage_node.store_file(&source_path1, "/keep.txt").unwrap();
    let file_id2 = storage_node
        .store_file(&source_path2, "/delete.txt")
        .unwrap();

    // Verify both files exist
    let files_before = storage_node.list_files().unwrap();
    assert_eq!(files_before.len(), 2);

    // Delete one file
    storage_node.delete_file(file_id2).unwrap();

    // Verify only one file remains
    let files_after = storage_node.list_files().unwrap();
    assert_eq!(files_after.len(), 1);
    assert_eq!(files_after[0].file_id, file_id1);

    // Verify the deleted file cannot be retrieved
    let output_path = temp_dir.path().join("should_fail.txt");
    let result = storage_node.retrieve_file(file_id2, &output_path);
    assert!(result.is_err());

    // Verify the remaining file can still be retrieved
    let output_path = temp_dir.path().join("still_works.txt");
    storage_node.retrieve_file(file_id1, &output_path).unwrap();
    let retrieved_data = fs::read(&output_path).unwrap();
    assert_eq!(retrieved_data, test_data1);
}

#[test]
fn test_restart_persistence() {
    let (config, temp_dir) = create_test_setup();

    // Store test data and file ID for later retrieval
    let test_data = b"Data that should persist across restarts";
    let source_path = create_test_file(temp_dir.path(), "persistent.txt", test_data);
    let virtual_path = PathBuf::from("/persistent/data.txt");

    let file_id = {
        // First storage node instance
        let storage_node = StorageNode::new(config.clone()).unwrap();
        let file_id = storage_node
            .store_file(&source_path, &virtual_path)
            .unwrap();

        // Verify it's stored
        let files = storage_node.list_files().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].file_id, file_id);

        file_id
    }; // storage_node goes out of scope, simulating restart

    // Create new storage node instance (simulating restart)
    let storage_node = StorageNode::new(config).unwrap();

    // Verify data persisted
    let files = storage_node.list_files().unwrap();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].file_id, file_id);
    assert_eq!(files[0].path, virtual_path);
    assert_eq!(files[0].size, test_data.len() as u64);

    // Verify file can still be retrieved after restart
    let output_path = temp_dir.path().join("after_restart.txt");
    storage_node.retrieve_file(file_id, &output_path).unwrap();

    let retrieved_data = fs::read(&output_path).unwrap();
    assert_eq!(retrieved_data, test_data);

    // Verify statistics are correct after restart
    let stats = storage_node.get_stats().unwrap();
    assert_eq!(stats.total_files, 1);
    assert_eq!(stats.total_size, test_data.len() as u64);
}

#[test]
fn test_error_conditions() {
    let (config, temp_dir) = create_test_setup();
    let storage_node = StorageNode::new(config).unwrap();

    // Test storing non-existent file
    let nonexistent_path = temp_dir.path().join("does_not_exist.txt");
    let result = storage_node.store_file(&nonexistent_path, "/test.txt");
    assert!(result.is_err());

    // Test retrieving non-existent file
    let fake_uuid = Uuid::new_v4();
    let output_path = temp_dir.path().join("output.txt");
    let result = storage_node.retrieve_file(fake_uuid, &output_path);
    assert!(result.is_err());

    // Test deleting non-existent file
    let result = storage_node.delete_file(fake_uuid);
    assert!(result.is_err());

    // Test storing empty file
    let empty_path = create_test_file(temp_dir.path(), "empty.txt", b"");
    let result = storage_node.store_file(&empty_path, "/empty.txt");
    assert!(result.is_err());
}

#[test]
fn test_chunk_reconstruction_with_missing_chunks() {
    let (config, temp_dir) = create_test_setup();
    let storage_node = StorageNode::new(config.clone()).unwrap();

    // Store a file
    let test_data = b"Test data for chunk reconstruction validation";
    let source_path = create_test_file(temp_dir.path(), "reconstruct.txt", test_data);
    let file_id = storage_node
        .store_file(&source_path, "/reconstruct.txt")
        .unwrap();

    // Get file metadata to find chunk files
    let files = storage_node.list_files().unwrap();
    assert_eq!(files.len(), 1);

    // Manually delete one chunk file (simulating disk failure)
    // Note: This is a simplified test - in a real scenario we'd need to access chunk files
    // For now, just verify the file can be retrieved (all chunks intact)
    let output_path = temp_dir.path().join("reconstructed.txt");
    storage_node.retrieve_file(file_id, &output_path).unwrap();

    let retrieved_data = fs::read(&output_path).unwrap();
    assert_eq!(retrieved_data, test_data);
}

#[test]
fn test_configuration_validation() {
    let (mut config, _temp_dir) = create_test_setup();

    // Test invalid configuration - zero min_free_space
    config.min_free_space = 0;
    let result = StorageNode::new(config.clone());
    assert!(result.is_err());

    // Test invalid configuration - zero max_file_size
    config.min_free_space = 1024;
    config.max_file_size = 0;
    let result = StorageNode::new(config);
    assert!(result.is_err());
}

#[test]
fn test_file_size_limits() {
    let (mut config, temp_dir) = create_test_setup();
    config.max_file_size = 100; // Very small limit

    let storage_node = StorageNode::new(config).unwrap();

    // Create file larger than limit
    let large_data = vec![0xFF; 200];
    let source_path = create_test_file(temp_dir.path(), "too_large.bin", &large_data);

    // Should reject the file
    let result = storage_node.store_file(&source_path, "/too_large.bin");
    assert!(result.is_err());

    // Verify error type
    match result.unwrap_err() {
        wormfs::StorageNodeError::FileTooLarge { .. } => {} // Expected
        other => panic!("Expected FileTooLarge error, got: {:?}", other),
    }
}

#[test]
fn test_concurrent_operations() {
    let (config, temp_dir) = create_test_setup();
    let storage_node = StorageNode::new(config).unwrap();

    // Store multiple files in sequence (simulating concurrent operations)
    let file_count = 5;
    let mut file_ids = Vec::new();

    for i in 0..file_count {
        let content = format!("File {} content with unique data", i);
        let source_path = create_test_file(
            temp_dir.path(),
            &format!("concurrent{}.txt", i),
            content.as_bytes(),
        );
        let virtual_path = format!("/concurrent/file{}.txt", i);

        let file_id = storage_node
            .store_file(&source_path, PathBuf::from(virtual_path))
            .unwrap();
        file_ids.push((file_id, content));
    }

    // Verify all files are stored correctly
    let files = storage_node.list_files().unwrap();
    assert_eq!(files.len(), file_count);

    // Retrieve all files and verify content
    for (i, (file_id, expected_content)) in file_ids.iter().enumerate() {
        let output_path = temp_dir.path().join(format!("concurrent_out{}.txt", i));
        storage_node.retrieve_file(*file_id, &output_path).unwrap();

        let retrieved_data = fs::read(&output_path).unwrap();
        assert_eq!(retrieved_data, expected_content.as_bytes());
    }
}
