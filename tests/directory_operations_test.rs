//! Integration tests for directory operations (mkdir and rmdir)
//!
//! These tests verify that directory creation and removal work correctly,
//! including permission checks, error handling, and edge cases.

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use wormfs::file_store::{Config as FileStoreConfig, FileStore, FileStoreImpl};
use wormfs::filesystem_service::factory::FileSystemServiceImplFactory;
use wormfs::filesystem_service::implementation::FileSystemServiceImpl;
use wormfs::filesystem_service::FileSystemService;
use wormfs::metadata_store::{
    types::*, ClientId, Config as MetadataConfig, MetadataStore, MetadataStoreFactory,
};

/// Helper to create a test FileSystemService with temporary storage.
async fn create_test_filesystem_service() -> (FileSystemServiceImpl, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let chunks_dir = temp_dir.path().join("chunks");
    std::fs::create_dir(&chunks_dir).expect("Failed to create chunks dir");

    let metadata_config = MetadataConfig {
        database_path: db_path,
        read_pool_size: 4,
        enable_wal: true,
        cache_size_mb: 64,
        enable_foreign_keys: false,
        synchronous: SynchronousMode::Normal,
        transaction_isolation: IsolationLevel::ReadCommitted,
        enable_prepared_statements: true,
        read_pool_timeout_secs: 30,
        stripe_cache_size_mb: 64,
        stripe_cache_ttl_secs: 10,
        stripe_cache_tti_secs: 5,
        chunk_cache_size_mb: 64,
        chunk_cache_ttl_secs: 10,
        chunk_cache_tti_secs: 5,
    };

    let metadata_store = MetadataStoreFactory::create_concrete(metadata_config)
        .await
        .expect("Failed to create MetadataStore");

    metadata_store
        .initialize_schema()
        .await
        .expect("Failed to initialize schema");

    let file_store_config = FileStoreConfig {
        disk_paths: vec![chunks_dir.clone()],
        max_chunk_size: 2 * 1024 * 1024, // 2MB chunks
        default_data_shards: 2,
        default_parity_shards: 1,
        max_concurrent_operations: 10,
        verification_interval: Duration::from_secs(3600),
        orphan_cleanup_age: Duration::from_secs(86400),
        stripe_cache_size_mb: 256,
        stripe_cache_ttl_secs: 3600,
        stripe_cache_tti_secs: 600,
    };

    let mut file_store =
        <FileStoreImpl as FileStore>::new(file_store_config).expect("Failed to create FileStore");

    let _disk_id = file_store
        .add_disk(chunks_dir.clone())
        .await
        .expect("Failed to add disk");

    let file_store = Arc::new(file_store);

    let fs_config = wormfs::filesystem_service::types::Config::default();
    let service = FileSystemServiceImplFactory::create(fs_config, metadata_store, file_store, None, None)
        .await
        .expect("Failed to create FileSystemService");

    service
        .initialize_root()
        .await
        .expect("Failed to initialize root");

    (service, temp_dir)
}

#[tokio::test]
async fn test_mkdir_basic() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create a directory in root
    let attrs = service
        .mkdir(1, "testdir", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create directory");

    // Verify directory attributes
    assert_eq!(
        attrs.kind,
        wormfs::filesystem_service::types::FileType::Directory
    );
    assert_eq!(attrs.perm, 0o755);
    assert_eq!(attrs.uid, 1000);
    assert_eq!(attrs.gid, 1000);
    assert_eq!(attrs.nlink, 1); // Always 1 (see docs/posix_compliance.md)
    assert_eq!(attrs.size, 0); // Directories have size 0

    println!("✓ Basic mkdir created directory with correct attributes");
}

#[tokio::test]
async fn test_mkdir_nested() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create first level directory
    let level1 = service
        .mkdir(1, "level1", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create level1");

    // Create second level directory
    let level2 = service
        .mkdir(level1.ino, "level2", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create level2");

    // Create third level directory
    let level3 = service
        .mkdir(level2.ino, "level3", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create level3");

    // Verify all directories exist
    assert_eq!(
        level1.kind,
        wormfs::filesystem_service::types::FileType::Directory
    );
    assert_eq!(
        level2.kind,
        wormfs::filesystem_service::types::FileType::Directory
    );
    assert_eq!(
        level3.kind,
        wormfs::filesystem_service::types::FileType::Directory
    );

    println!("✓ Nested directories created successfully");
}

#[tokio::test]
async fn test_mkdir_already_exists() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create directory
    service
        .mkdir(1, "testdir", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create directory");

    // Try to create same directory again
    let result = service
        .mkdir(1, "testdir", 0o755, 1000, 1000, client_id)
        .await;

    assert!(result.is_err());
    println!("✓ Duplicate mkdir correctly returns error");
}

#[tokio::test]
async fn test_mkdir_permission_denied_no_write() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create a directory with restricted permissions (r-x for others)
    let parent = service
        .mkdir(1, "restricted", 0o750, 1000, 1000, client_id)
        .await
        .expect("Failed to create parent directory");

    // Try to create subdirectory as different user (uid=2000)
    // Parent is 0o750 (rwxr-x---), so others have no write permission
    let result = service
        .mkdir(parent.ino, "subdir", 0o755, 2000, 2000, client_id)
        .await;

    assert!(result.is_err());
    println!("✓ mkdir correctly denied for user without write permission");
}

#[tokio::test]
async fn test_mkdir_permission_denied_no_execute() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create a directory with write but no execute for others (rw- for others)
    let parent = service
        .mkdir(1, "noexec", 0o766, 1000, 1000, client_id)
        .await
        .expect("Failed to create parent directory");

    // Try to create subdirectory as different user (uid=2000)
    // Parent is 0o766 (rwxrw-rw-), so others have write but no execute
    let result = service
        .mkdir(parent.ino, "subdir", 0o755, 2000, 2000, client_id)
        .await;

    assert!(result.is_err());
    println!("✓ mkdir correctly denied for user without execute permission");
}

#[tokio::test]
async fn test_mkdir_permission_owner_success() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create a directory with restricted permissions for group/others
    let parent = service
        .mkdir(1, "owneddir", 0o700, 1000, 1000, client_id)
        .await
        .expect("Failed to create parent directory");

    // Owner should be able to create subdirectory
    let result = service
        .mkdir(parent.ino, "subdir", 0o755, 1000, 1000, client_id)
        .await;

    assert!(result.is_ok());
    println!("✓ mkdir succeeds for directory owner");
}

#[tokio::test]
async fn test_rmdir_basic() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create a directory
    service
        .mkdir(1, "testdir", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create directory");

    // Remove the directory
    service
        .rmdir(1, "testdir", 1000, 1000, client_id)
        .await
        .expect("Failed to remove directory");

    // Verify directory is gone by trying to create it again
    let result = service
        .mkdir(1, "testdir", 0o755, 1000, 1000, client_id)
        .await;

    assert!(
        result.is_ok(),
        "Should be able to recreate removed directory"
    );
    println!("✓ Basic rmdir successfully removed directory");
}

#[tokio::test]
async fn test_rmdir_not_empty() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create parent directory
    let parent = service
        .mkdir(1, "parent", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create parent directory");

    // Create child directory
    service
        .mkdir(parent.ino, "child", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create child directory");

    // Try to remove non-empty parent directory
    let result = service.rmdir(1, "parent", 1000, 1000, client_id).await;

    assert!(
        result.is_err(),
        "Should not be able to remove non-empty directory"
    );
    println!("✓ rmdir correctly fails for non-empty directory");
}

#[tokio::test]
async fn test_rmdir_with_file() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create directory
    let dir = service
        .mkdir(1, "testdir", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create directory");

    // Create file in directory
    service
        .create(dir.ino, "testfile", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    // Try to remove directory with file
    let result = service.rmdir(1, "testdir", 1000, 1000, client_id).await;

    assert!(
        result.is_err(),
        "Should not be able to remove directory with file"
    );
    println!("✓ rmdir correctly fails for directory containing file");
}

#[tokio::test]
async fn test_rmdir_not_a_directory() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create a file (not a directory)
    service
        .create(1, "testfile", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    // Try to rmdir a file
    let result = service.rmdir(1, "testfile", 1000, 1000, client_id).await;

    assert!(result.is_err(), "Should not be able to rmdir a file");
    println!("✓ rmdir correctly fails for regular file");
}

#[tokio::test]
async fn test_rmdir_permission_denied() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create parent directory with restricted permissions
    let parent = service
        .mkdir(1, "restricted", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create parent directory");

    // Create child directory
    service
        .mkdir(parent.ino, "child", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create child directory");

    // Change parent to no write for others
    service
        .setattr(
            parent.ino,
            None,                 // file_handle
            Some(0o755 & !0o002), // Remove write for others
            None,
            None,
            None,
            None,
            None,
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to change permissions");

    // Try to remove child as different user
    let result = service
        .rmdir(parent.ino, "child", 2000, 2000, client_id)
        .await;

    assert!(
        result.is_err(),
        "Should not be able to rmdir without permission"
    );
    println!("✓ rmdir correctly denied for user without permission");
}

#[tokio::test]
async fn test_rmdir_nonexistent() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Try to remove directory that doesn't exist
    let result = service.rmdir(1, "nonexistent", 1000, 1000, client_id).await;

    assert!(
        result.is_err(),
        "Should fail to remove nonexistent directory"
    );
    println!("✓ rmdir correctly fails for nonexistent directory");
}

#[tokio::test]
async fn test_mkdir_rmdir_cycle() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create and remove directory multiple times
    for i in 0..5 {
        let attrs = service
            .mkdir(1, "cycledir", 0o755, 1000, 1000, client_id)
            .await
            .expect(&format!("Failed to create directory on iteration {}", i));

        assert_eq!(
            attrs.kind,
            wormfs::filesystem_service::types::FileType::Directory
        );

        service
            .rmdir(1, "cycledir", 1000, 1000, client_id)
            .await
            .expect(&format!("Failed to remove directory on iteration {}", i));
    }

    println!("✓ mkdir/rmdir cycle works correctly");
}

#[tokio::test]
async fn test_mkdir_various_modes() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    let test_modes = vec![
        0o755, // rwxr-xr-x
        0o700, // rwx------
        0o777, // rwxrwxrwx
        0o750, // rwxr-x---
        0o770, // rwxrwx---
    ];

    for (idx, mode) in test_modes.iter().enumerate() {
        let name = format!("dir_{}", idx);
        let attrs = service
            .mkdir(1, &name, *mode, 1000, 1000, client_id)
            .await
            .expect(&format!("Failed to create directory with mode {:o}", mode));

        assert_eq!(
            attrs.perm, *mode as u16,
            "Directory mode should match requested mode {:o}",
            mode
        );
    }

    println!("✓ mkdir correctly sets various permission modes");
}

#[tokio::test]
async fn test_readdir_after_mkdir() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create several directories
    for i in 0..5 {
        let name = format!("dir_{}", i);
        service
            .mkdir(1, &name, 0o755, 1000, 1000, client_id)
            .await
            .expect(&format!("Failed to create {}", name));
    }

    // Read directory entries
    let entries = service
        .readdir(1, 0, client_id)
        .await
        .expect("Failed to read directory");

    // Count directory entries (excluding . and .. and empty names)
    let dir_count = entries
        .iter()
        .filter(|e| !e.name.is_empty() && e.name != "." && e.name != "..")
        .filter(|e| e.kind == wormfs::filesystem_service::types::FileType::Directory)
        .count();

    assert_eq!(dir_count, 5, "Should have 5 directories");
    println!("✓ readdir correctly lists created directories");
}

#[tokio::test]
async fn test_mkdir_rmdir_mixed_with_files() {
    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create directories and files in root
    service
        .mkdir(1, "dir1", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create dir1");

    service
        .create(1, "file1", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file1");

    service
        .mkdir(1, "dir2", 0o755, 1000, 1000, client_id)
        .await
        .expect("Failed to create dir2");

    service
        .create(1, "file2", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file2");

    // Remove one directory and one file
    service
        .rmdir(1, "dir1", 1000, 1000, client_id)
        .await
        .expect("Failed to remove dir1");

    service
        .unlink(1, "file1", 1000, 1000, client_id)
        .await
        .expect("Failed to unlink file1");

    // Verify remaining entries
    let entries = service
        .readdir(1, 0, client_id)
        .await
        .expect("Failed to read directory");

    let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();

    assert!(names.contains(&"dir2"), "dir2 should still exist");
    assert!(names.contains(&"file2"), "file2 should still exist");
    assert!(!names.contains(&"dir1"), "dir1 should be removed");
    assert!(!names.contains(&"file1"), "file1 should be removed");

    println!("✓ Mixed directory and file operations work correctly");
}
