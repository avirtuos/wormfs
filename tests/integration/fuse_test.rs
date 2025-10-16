//! Integration tests for FUSE filesystem operations.
//!
//! These tests verify the FileSystemService integration with MetadataStore
//! and simulate FUSE-like behavior without requiring actual FUSE mounts.

use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use tempfile::TempDir;
use wormfs::file_store::FileStore;
use wormfs::filesystem_service::factory::FileSystemServiceImplFactory;
use wormfs::filesystem_service::implementation::FileSystemServiceImpl;
use wormfs::filesystem_service::inode::ROOT_INODE;
use wormfs::filesystem_service::types::{ClientId, Config, FileType};
use wormfs::filesystem_service::FileSystemService;
use wormfs::metadata_store::{FileId, FileMetadata, MetadataStore, MetadataStoreFactory};

/// Helper to create a test filesystem service.
async fn create_test_fs() -> (FileSystemServiceImpl, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("metadata.db");
    let chunk_path = temp_dir.path().join("chunks");
    std::fs::create_dir_all(&chunk_path).expect("Failed to create chunk dir");

    // Create MetadataStore using factory (concrete type for FileSystemServiceImplFactory)
    let metadata_config = wormfs::metadata_store::Config {
        database_path: db_path,
        ..Default::default()
    };

    let metadata_store = MetadataStoreFactory::create_concrete(metadata_config)
        .await
        .expect("Failed to create MetadataStore");

    metadata_store
        .initialize_schema()
        .await
        .expect("Failed to initialize schema");

    // Create FileStore
    let file_store_config = wormfs::file_store::types::Config {
        disk_paths: vec![chunk_path],
        max_chunk_size: 1024,
        default_data_shards: 2,
        default_parity_shards: 1,
        max_concurrent_operations: 10,
        verification_interval: std::time::Duration::from_secs(3600),
        orphan_cleanup_age: std::time::Duration::from_secs(3600),
    };

    let file_store =
        Arc::new(FileStore::new(file_store_config).expect("Failed to create FileStore"));

    // Create FileSystemService via factory
    let fs_config = Config {
        uid: 1000,
        gid: 1000,
        ..Default::default()
    };

    let service = FileSystemServiceImplFactory::create(fs_config, metadata_store, file_store)
        .expect("Failed to create FileSystemService");

    (service, temp_dir)
}

#[tokio::test]
async fn test_initialize_and_query_root_directory() {
    // Create filesystem service
    let (service, _temp_dir) = create_test_fs().await;

    // Initialize root directory (simulates FUSE init())
    service
        .initialize_root()
        .await
        .expect("Failed to initialize root");

    // Query root directory attributes (simulates FUSE getattr())
    let root_attr = service
        .getattr(ROOT_INODE)
        .await
        .expect("Failed to get root attributes");

    // Verify root directory attributes
    assert_eq!(root_attr.ino, ROOT_INODE, "Root inode should be 1");
    assert_eq!(
        root_attr.kind,
        FileType::Directory,
        "Root should be a directory"
    );
    assert_eq!(root_attr.perm, 0o755, "Root permissions should be 0755");
    assert_eq!(root_attr.uid, 1000, "Root uid should match config");
    assert_eq!(root_attr.gid, 1000, "Root gid should match config");
    assert_eq!(root_attr.size, 0, "Root directory size should be 0");
    assert_eq!(root_attr.nlink, 1, "Root should have 1 link");

    println!("✓ Root directory initialized and queried successfully");
}

#[tokio::test]
async fn test_initialize_root_is_idempotent() {
    let (service, _temp_dir) = create_test_fs().await;

    // Initialize root multiple times
    service
        .initialize_root()
        .await
        .expect("First initialization failed");

    service
        .initialize_root()
        .await
        .expect("Second initialization failed");

    service
        .initialize_root()
        .await
        .expect("Third initialization failed");

    // Root should still be accessible with correct attributes
    let root_attr = service
        .getattr(ROOT_INODE)
        .await
        .expect("Failed to getattr");
    assert_eq!(root_attr.ino, ROOT_INODE);
    assert_eq!(root_attr.kind, FileType::Directory);

    println!("✓ Root initialization is idempotent");
}

#[tokio::test]
async fn test_lookup_file_via_metadata_store() {
    let (service, _temp_dir) = create_test_fs().await;

    // Initialize root
    service.initialize_root().await.expect("Init failed");

    // Pre-populate a test file in MetadataStore (simulates file creation)
    let file_id = FileId::new(100);
    let test_file_path = Path::new("/test.txt");
    let test_inode = 42;

    let file_metadata = FileMetadata {
        file_type: wormfs::metadata_store::FileType::RegularFile,
        size: 1024,
        permissions: 0o644,
        uid: 1000,
        gid: 1000,
        created_at: SystemTime::now(),
        modified_at: SystemTime::now(),
        accessed_at: SystemTime::now(),
    };

    service
        .metadata_store()
        .create_file(file_id, test_file_path, test_inode, file_metadata.clone())
        .await
        .expect("Failed to create test file in MetadataStore");

    // Now query via FileSystemService (simulates FUSE getattr())
    let file_attr = service
        .getattr(test_inode)
        .await
        .expect("Failed to getattr test file");

    // Verify attributes match what we inserted
    assert_eq!(file_attr.ino, test_inode);
    assert_eq!(file_attr.size, 1024);
    assert_eq!(file_attr.perm, 0o644);
    assert_eq!(file_attr.uid, 1000);
    assert_eq!(file_attr.gid, 1000);
    assert_eq!(file_attr.kind, FileType::RegularFile);

    println!("✓ File lookup via MetadataStore successful");
}

#[tokio::test]
async fn test_inode_cache_functionality() {
    let (service, _temp_dir) = create_test_fs().await;
    service.initialize_root().await.expect("Init failed");

    // First getattr - should populate cache
    let start = std::time::Instant::now();
    let attr1 = service.getattr(ROOT_INODE).await.expect("getattr failed");
    let first_duration = start.elapsed();

    // Second getattr - should hit cache (faster)
    let start = std::time::Instant::now();
    let attr2 = service.getattr(ROOT_INODE).await.expect("getattr failed");
    let second_duration = start.elapsed();

    // Verify cache hit (second call should be at least as fast)
    // Note: This is a weak assertion since both are very fast
    assert_eq!(attr1.ino, attr2.ino);
    assert_eq!(attr1.size, attr2.size);

    // Verify cache has entry
    let cache = service.inode_cache();
    assert_eq!(cache.len(), 1, "Cache should have 1 entry");

    let cached_entry = cache.get(ROOT_INODE);
    assert!(cached_entry.is_some(), "Cache should contain root inode");

    println!(
        "✓ Inode cache working (first: {:?}, second: {:?})",
        first_duration, second_duration
    );
}

#[tokio::test]
async fn test_getattr_nonexistent_inode() {
    let (service, _temp_dir) = create_test_fs().await;
    service.initialize_root().await.expect("Init failed");

    // Try to get attributes for non-existent inode
    let result = service.getattr(9999).await;

    assert!(
        result.is_err(),
        "getattr should fail for non-existent inode"
    );

    // Verify error type
    match result {
        Err(e) => {
            let errno = e.to_errno();
            assert_eq!(errno, libc::ENOENT, "Error should be ENOENT");
            println!("✓ Non-existent inode returns ENOENT");
        }
        Ok(_) => panic!("Should have returned error"),
    }
}

#[tokio::test]
async fn test_readdir_empty_root() {
    let (service, _temp_dir) = create_test_fs().await;
    service.initialize_root().await.expect("Init failed");

    let client_id = ClientId::new(1);

    // Read root directory (should only have . and ..)
    let entries = service
        .readdir(ROOT_INODE, 0, client_id)
        .await
        .expect("readdir failed");

    // Should have at least . and ..
    assert!(entries.len() >= 2, "Should have at least . and ..");

    // Verify . entry
    let dot_entry = entries.iter().find(|e| e.name == ".");
    assert!(dot_entry.is_some(), "Should have . entry");
    let dot = dot_entry.unwrap();
    assert_eq!(dot.ino, ROOT_INODE);
    assert_eq!(dot.kind, FileType::Directory);

    // Verify .. entry
    let dotdot_entry = entries.iter().find(|e| e.name == "..");
    assert!(dotdot_entry.is_some(), "Should have .. entry");

    println!("✓ readdir on empty root returns . and ..");
}

#[tokio::test]
async fn test_readdir_with_files() {
    let (service, _temp_dir) = create_test_fs().await;
    service.initialize_root().await.expect("Init failed");

    // Create a test file in root directory
    let file_id = FileId::new(200);
    let test_file_path = Path::new("/file1.txt");
    let test_inode = 100;

    let file_metadata = FileMetadata {
        file_type: wormfs::metadata_store::FileType::RegularFile,
        size: 512,
        permissions: 0o644,
        uid: 1000,
        gid: 1000,
        created_at: SystemTime::now(),
        modified_at: SystemTime::now(),
        accessed_at: SystemTime::now(),
    };

    service
        .metadata_store()
        .create_file(file_id, test_file_path, test_inode, file_metadata)
        .await
        .expect("Failed to create file");

    // Read directory
    let client_id = ClientId::new(1);
    let entries = service
        .readdir(ROOT_INODE, 0, client_id)
        .await
        .expect("readdir failed");

    // Should have ., .., and file1.txt
    assert!(entries.len() >= 3, "Should have at least 3 entries");

    // Find our file
    let file_entry = entries.iter().find(|e| e.name == "file1.txt");
    assert!(file_entry.is_some(), "Should find file1.txt");

    let file = file_entry.unwrap();
    assert_eq!(file.ino, test_inode);
    assert_eq!(file.kind, FileType::RegularFile);

    println!("✓ readdir shows created files");
}

#[tokio::test]
async fn test_inode_allocator_thread_safety() {
    let (service, _temp_dir) = create_test_fs().await;

    // Get the inode manager (Arc wraps InodeManager which contains InodeAllocator)
    let inode_manager = Arc::clone(service.inode_manager());

    // Allocate inodes from multiple threads
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let manager = Arc::clone(&inode_manager);
            std::thread::spawn(move || {
                let mut inodes = Vec::new();
                for _ in 0..100 {
                    inodes.push(manager.allocator().allocate());
                }
                inodes
            })
        })
        .collect();

    // Collect all allocated inodes
    let mut all_inodes = Vec::new();
    for handle in handles {
        all_inodes.extend(handle.join().expect("Thread failed"));
    }

    // Verify all inodes are unique
    let original_len = all_inodes.len();
    all_inodes.sort();
    all_inodes.dedup();
    assert_eq!(
        all_inodes.len(),
        original_len,
        "All inodes should be unique"
    );

    println!(
        "✓ Inode allocator is thread-safe ({} unique inodes)",
        original_len
    );
}

#[tokio::test]
async fn test_concurrent_getattr_operations() {
    let (service, _temp_dir) = create_test_fs().await;
    service.initialize_root().await.expect("Init failed");

    let service = Arc::new(service);

    // Spawn multiple concurrent getattr operations
    let mut handles = Vec::new();
    for _ in 0..20 {
        let svc = Arc::clone(&service);
        let handle =
            tokio::spawn(async move { svc.getattr(ROOT_INODE).await.expect("getattr failed") });
        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        let attr = handle.await.expect("Task failed");
        assert_eq!(attr.ino, ROOT_INODE);
    }

    println!("✓ Concurrent getattr operations successful");
}
