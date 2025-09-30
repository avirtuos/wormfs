//! Integration tests for the storage layout module
//!
//! These tests verify the storage layout functionality works correctly,
//! including chunk folder management, hash generation, and directory operations.

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use tempfile::TempDir;
use uuid::Uuid;
use wormfs::{
    ChunkId, ChunkIndexEntry, ChunkIndexFile, StorageLayout, StorageLayoutConfig, StripeId,
    FOLDER_HASH_LENGTH,
};

fn create_test_layout() -> (StorageLayout, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageLayoutConfig::new(temp_dir.path().to_path_buf())
        .with_min_free_space(1024)
        .with_auto_create_dirs(true);
    let layout = StorageLayout::new(config).unwrap();
    (layout, temp_dir)
}

#[test]
fn test_hash_determinism_across_restarts() {
    // Test that hash generation is deterministic across multiple runs
    let test_paths = vec![
        "/home/user/documents/file.txt",
        "/var/log/system.log",
        "/tmp/temp_file_123.dat",
        "/media/photos/IMG_20230101_120000.jpg",
        "/usr/local/bin/my_program",
    ];

    for path in test_paths {
        let path_buf = PathBuf::from(path);
        let hash1 = StorageLayout::generate_folder_hash(&path_buf);
        let hash2 = StorageLayout::generate_folder_hash(&path_buf);
        let hash3 = StorageLayout::generate_folder_hash(&path_buf);

        assert_eq!(hash1, hash2);
        assert_eq!(hash2, hash3);
        assert_eq!(hash1.len(), FOLDER_HASH_LENGTH);
    }
}

#[test]
fn test_hash_uniqueness() {
    // Test that different file paths produce different hashes (basic functionality)
    let test_paths = vec![
        "/home/user/documents/file.txt",
        "/var/log/system.log",
        "/tmp/temp_file_123.dat",
        "/media/photos/IMG_20230101_120000.jpg",
        "/usr/local/bin/my_program",
        "/etc/config/settings.conf",
        "/opt/app/data/database.db",
        "/backup/full_backup_2023.tar.gz",
    ];

    let mut hashes = HashSet::new();

    for path in test_paths {
        let hash = StorageLayout::generate_folder_hash(PathBuf::from(path));
        let dir_num = StorageLayout::calculate_top_level_dir(&hash).unwrap();

        // Each path should produce a unique hash
        assert!(
            hashes.insert(hash.clone()),
            "Duplicate hash for path: {}",
            path
        );

        // Directory should be in valid range
        assert!(
            (1..=1000).contains(&dir_num),
            "Invalid directory number: {}",
            dir_num
        );
    }

    // All hashes should be unique
    assert_eq!(hashes.len(), 8, "Expected 8 unique hashes");
}

#[test]
fn test_no_hash_collisions_common_patterns() {
    // Test common file patterns that might cause collisions
    let test_cases = vec![
        // Sequential files
        (0..1000)
            .map(|i| format!("/data/file_{:04}.txt", i))
            .collect::<Vec<_>>(),
        // Date-based files
        (1..=31)
            .map(|day| format!("/logs/2023-01-{:02}.log", day))
            .collect::<Vec<_>>(),
        // Backup files
        (0..100)
            .map(|i| format!("/backup/backup_{}.tar.gz", i))
            .collect::<Vec<_>>(),
        // Photo files
        (1..=100)
            .map(|i| format!("/photos/IMG_{:05}.jpg", i))
            .collect::<Vec<_>>(),
    ];

    for test_case in test_cases {
        let mut hashes = HashSet::new();
        for path in test_case {
            let hash = StorageLayout::generate_folder_hash(PathBuf::from(path));
            assert!(
                hashes.insert(hash.clone()),
                "Hash collision detected: {}",
                hash
            );
        }
    }
}

#[test]
fn test_chunk_folder_lifecycle() {
    let (layout, _temp_dir) = create_test_layout();

    let file_id = Uuid::new_v4();
    let file_path = PathBuf::from("/test/lifecycle_test.txt");
    let file_size = 4096;

    // Create chunk folder
    let mut chunk_folder = layout
        .create_chunk_folder(file_id, file_path.clone(), file_size)
        .unwrap();

    // Verify initial state
    assert_eq!(chunk_folder.index.file_id, file_id);
    assert_eq!(chunk_folder.index.file_path, file_path);
    assert_eq!(chunk_folder.index.file_size, file_size);
    assert!(chunk_folder.index.is_empty());

    // Add some chunks
    let stripe_id = StripeId::new(file_id, 0);
    for i in 0..6 {
        let chunk_id = ChunkId::new(file_id, 0, i);
        let entry = ChunkIndexEntry::new(
            chunk_id,
            stripe_id,
            512,
            0x12345678 + i as u32,
            format!("chunk_0_{}.dat", i),
        );
        chunk_folder.index.add_chunk(chunk_id, entry);
    }

    // Save changes
    chunk_folder.save_index().unwrap();

    // Verify persistence
    assert!(layout.chunk_folder_exists(&file_path));
    let loaded_folder = layout.load_chunk_folder(&file_path).unwrap();
    assert_eq!(loaded_folder.index.chunks.len(), 6);
    assert_eq!(loaded_folder.index.folder_stats.chunk_count, 6);
    assert_eq!(loaded_folder.index.folder_stats.total_size, 512 * 6);

    // Test chunk operations
    let chunk_ids = loaded_folder.index.list_chunk_ids();
    assert_eq!(chunk_ids.len(), 6);

    // Remove some chunks
    let mut updated_folder = loaded_folder;
    let removed_chunk = ChunkId::new(file_id, 0, 2);
    let removed_entry = updated_folder.index.remove_chunk(&removed_chunk).unwrap();
    assert_eq!(removed_entry.chunk_id, removed_chunk);
    assert_eq!(updated_folder.index.chunks.len(), 5);

    // Clean up
    layout.remove_chunk_folder(&file_path).unwrap();
    assert!(!layout.chunk_folder_exists(&file_path));
}

#[test]
fn test_multiple_files_functionality() {
    let (layout, _temp_dir) = create_test_layout();

    // Create multiple files with diverse paths to test basic functionality
    let file_paths = vec![
        PathBuf::from("/documents/report.pdf"),
        PathBuf::from("/photos/vacation_2023.jpg"),
        PathBuf::from("/config/settings.conf"),
        PathBuf::from("/logs/application.log"),
        PathBuf::from("/backup/database_dump.sql"),
    ];

    let mut created_folders = Vec::new();
    let mut paths_and_hashes = Vec::new();

    for file_path in file_paths {
        let hash = StorageLayout::generate_folder_hash(&file_path);
        let top_dir = StorageLayout::calculate_top_level_dir(&hash).unwrap();

        paths_and_hashes.push((file_path.clone(), hash, top_dir));

        let chunk_folder = layout
            .create_chunk_folder(Uuid::new_v4(), file_path, 1024)
            .unwrap();
        created_folders.push(chunk_folder);
    }

    // Verify all folders were created
    let all_folders = layout.list_chunk_folders().unwrap();
    assert_eq!(all_folders.len(), 5);

    // Verify each folder can be loaded correctly
    for (file_path, expected_hash, _) in &paths_and_hashes {
        let loaded_folder = layout.load_chunk_folder(file_path).unwrap();
        assert_eq!(loaded_folder.folder_hash, *expected_hash);
        assert_eq!(loaded_folder.index.file_path, *file_path);
    }
}

#[test]
fn test_chunk_index_serialization() {
    let file_id = Uuid::new_v4();
    let file_path = PathBuf::from("/test/serialization_test.txt");
    let mut index = ChunkIndexFile::new(file_id, file_path.clone(), 2048);

    // Add various chunks with different properties
    let test_chunks = vec![
        (ChunkId::new(file_id, 0, 0), 256, "chunk_0_0.dat"),
        (ChunkId::new(file_id, 0, 1), 512, "chunk_0_1.dat"),
        (ChunkId::new(file_id, 1, 0), 1024, "chunk_1_0.dat"),
        (ChunkId::new(file_id, 1, 1), 256, "chunk_1_1.dat"),
    ];

    for (chunk_id, size, filename) in test_chunks {
        let stripe_id = StripeId::new(chunk_id.file_id, chunk_id.stripe_index);
        let entry =
            ChunkIndexEntry::new(chunk_id, stripe_id, size, 0xDEADBEEF, filename.to_string());
        index.add_chunk(chunk_id, entry);
    }

    // Test JSON serialization
    let json = serde_json::to_string_pretty(&index).unwrap();
    let deserialized: ChunkIndexFile = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.file_id, index.file_id);
    assert_eq!(deserialized.file_path, index.file_path);
    assert_eq!(deserialized.file_size, index.file_size);
    assert_eq!(deserialized.chunks.len(), index.chunks.len());
    assert_eq!(
        deserialized.folder_stats.chunk_count,
        index.folder_stats.chunk_count
    );
    assert_eq!(
        deserialized.folder_stats.total_size,
        index.folder_stats.total_size
    );

    // Verify all chunks are preserved
    for chunk_id in index.list_chunk_ids() {
        let original = index.get_chunk(&chunk_id).unwrap();
        let restored = deserialized.get_chunk(&chunk_id).unwrap();
        assert_eq!(original, restored);
    }
}

#[test]
fn test_storage_stats_accuracy() {
    let (layout, _temp_dir) = create_test_layout();

    // Initially empty
    let stats = layout.get_storage_stats().unwrap();
    assert_eq!(stats.folder_count, 0);
    assert_eq!(stats.total_chunks, 0);
    assert_eq!(stats.total_size, 0);

    // Create files with known chunk counts and sizes
    let test_files = vec![
        ("/test/file1.txt", 3, 256),  // 3 chunks of 256 bytes each
        ("/test/file2.txt", 5, 512),  // 5 chunks of 512 bytes each
        ("/test/file3.txt", 2, 1024), // 2 chunks of 1024 bytes each
    ];

    let mut expected_total_chunks = 0;
    let mut expected_total_size = 0;

    for (path, chunk_count, chunk_size) in test_files {
        let file_id = Uuid::new_v4();
        let file_path = PathBuf::from(path);
        let mut chunk_folder = layout
            .create_chunk_folder(file_id, file_path, chunk_count * chunk_size)
            .unwrap();

        // Add chunks to the folder
        for i in 0..chunk_count {
            let chunk_id = ChunkId::new(file_id, 0, i as u8);
            let stripe_id = StripeId::new(file_id, 0);
            let entry = ChunkIndexEntry::new(
                chunk_id,
                stripe_id,
                chunk_size,
                0x12345678,
                format!("chunk_{}_{}.dat", 0, i),
            );
            chunk_folder.index.add_chunk(chunk_id, entry);
        }

        chunk_folder.save_index().unwrap();

        expected_total_chunks += chunk_count;
        expected_total_size += chunk_count * chunk_size;
    }

    // Verify final stats
    let final_stats = layout.get_storage_stats().unwrap();
    assert_eq!(final_stats.folder_count, 3);
    assert_eq!(final_stats.total_chunks, expected_total_chunks);
    assert_eq!(final_stats.total_size, expected_total_size);
    assert!(final_stats.available_space > 0);
}

#[test]
fn test_directory_cleanup() {
    let (layout, temp_dir) = create_test_layout();

    // Create some chunk folders
    let file_paths = vec![
        PathBuf::from("/test/temp1.txt"),
        PathBuf::from("/test/temp2.txt"),
        PathBuf::from("/different/temp3.txt"),
    ];

    let mut folder_paths = Vec::new();
    for path in &file_paths {
        let chunk_folder = layout
            .create_chunk_folder(Uuid::new_v4(), path.clone(), 1024)
            .unwrap();
        folder_paths.push(chunk_folder.path.clone());
    }

    // Verify folders exist
    for path in &folder_paths {
        assert!(path.exists());
    }

    // Remove chunks from folders manually (simulating external deletion)
    for path in &folder_paths {
        // Remove index file, leaving empty directory
        let index_path = path.join("chunk_index.json");
        if index_path.exists() {
            fs::remove_file(index_path).unwrap();
        }
    }

    // Run cleanup
    let removed_count = layout.cleanup_empty_directories().unwrap();
    assert!(removed_count > 0);

    // Verify empty directories were removed
    for path in &folder_paths {
        assert!(!path.exists());
    }

    // Top-level directories should still exist
    for i in 1..=10 {
        let dir_name = format!("{:03}", i);
        let top_level_path = temp_dir.path().join(dir_name);
        assert!(top_level_path.exists());
    }
}

#[test]
fn test_error_conditions() {
    let (layout, _temp_dir) = create_test_layout();

    // Test loading non-existent folder
    let nonexistent_path = PathBuf::from("/nonexistent/file.txt");
    let result = layout.load_chunk_folder(&nonexistent_path);
    assert!(result.is_err());

    // Test invalid folder hash
    let result = StorageLayout::calculate_top_level_dir("short");
    assert!(result.is_err());

    let result = StorageLayout::calculate_top_level_dir("way_too_long_hash");
    assert!(result.is_err());

    // Test chunk folder operations on non-existent folder
    assert!(!layout.chunk_folder_exists(&nonexistent_path));

    // Removing non-existent folder should not error
    layout.remove_chunk_folder(&nonexistent_path).unwrap();
}

#[test]
fn test_chunk_filename_patterns() {
    let (layout, _temp_dir) = create_test_layout();

    let file_id = Uuid::new_v4();
    let file_path = PathBuf::from("/test/pattern_test.txt");
    let mut chunk_folder = layout
        .create_chunk_folder(file_id, file_path.clone(), 4096)
        .unwrap();

    // Test various chunk filename patterns
    let test_patterns = [
        "chunk_0_0.dat",
        "data_stripe_1_chunk_2.bin",
        "parity_chunk_3.chk",
        "backup_chunk_4.dat",
    ];

    for (i, pattern) in test_patterns.iter().enumerate() {
        let chunk_id = ChunkId::new(file_id, 0, i as u8);
        let stripe_id = StripeId::new(file_id, 0);
        let entry = ChunkIndexEntry::new(
            chunk_id,
            stripe_id,
            256,
            0x12345678 + i as u32,
            pattern.to_string(),
        );

        chunk_folder.index.add_chunk(chunk_id, entry);

        // Test chunk file path generation
        let chunk_path = chunk_folder.chunk_file_path(pattern);
        assert!(chunk_path.ends_with(pattern));
        assert!(chunk_path.starts_with(&chunk_folder.path));
    }

    chunk_folder.save_index().unwrap();

    // Verify all patterns were saved and can be loaded
    let loaded_folder = layout.load_chunk_folder(&file_path).unwrap();
    assert_eq!(loaded_folder.index.chunks.len(), test_patterns.len());

    for (i, pattern) in test_patterns.iter().enumerate() {
        let chunk_id = ChunkId::new(file_id, 0, i as u8);
        let entry = loaded_folder.index.get_chunk(&chunk_id).unwrap();
        assert_eq!(entry.chunk_filename, *pattern);
    }
}

#[test]
fn test_concurrent_folder_access() {
    // This test simulates what might happen if multiple processes try to create
    // the same chunk folder simultaneously
    let (layout, _temp_dir) = create_test_layout();

    let file_id = Uuid::new_v4();
    let file_path = PathBuf::from("/test/concurrent_test.txt");

    // Create the same folder multiple times (simulating race condition)
    let folder1 = layout
        .create_chunk_folder(file_id, file_path.clone(), 1024)
        .unwrap();
    let folder2 = layout
        .create_chunk_folder(file_id, file_path.clone(), 1024)
        .unwrap();

    // Both should point to the same location
    assert_eq!(folder1.path, folder2.path);
    assert_eq!(folder1.folder_hash, folder2.folder_hash);

    // Should be able to load the folder
    let loaded_folder = layout.load_chunk_folder(&file_path).unwrap();
    assert_eq!(loaded_folder.path, folder1.path);
}

#[test]
fn test_storage_layout_config() {
    let temp_dir = TempDir::new().unwrap();

    // Test config builder pattern
    let config = StorageLayoutConfig::new(temp_dir.path())
        .with_min_free_space(2048)
        .with_auto_create_dirs(false);

    assert_eq!(config.min_free_space, 2048);
    assert!(!config.auto_create_dirs);
    assert_eq!(config.root_path, temp_dir.path());

    // Test creating layout with auto_create_dirs = false and non-existent path
    let bad_config = StorageLayoutConfig::new("/nonexistent/path").with_auto_create_dirs(false);
    let result = StorageLayout::new(bad_config);
    assert!(result.is_err());
}
