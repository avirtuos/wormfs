//! Integration tests for MetadataStore
//!
//! These tests verify end-to-end workflows and performance characteristics
//! as specified in GitHub Issue #59.

use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::task::JoinSet;
use wormfs::metadata_store::{
    factory::MetadataStoreFactory, types::*, Config, FileId, FileMetadata, MetadataStore,
};

/// Helper to create a test MetadataStore with a temporary database.
async fn create_test_store() -> (impl MetadataStore, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");

    let config = Config {
        database_path: db_path,
        read_pool_size: 8, // Higher for concurrency tests
        enable_wal: true,
        cache_size_mb: 64,
        enable_foreign_keys: true,
        synchronous: SynchronousMode::Normal,
        transaction_isolation: IsolationLevel::ReadCommitted,
        enable_prepared_statements: true,
        read_pool_timeout_secs: 30,
    };

    let store = MetadataStoreFactory::create(config)
        .await
        .expect("Failed to create MetadataStore");

    store
        .initialize_schema()
        .await
        .expect("Failed to initialize schema");

    (store, temp_dir)
}

/// Helper to create test file metadata.
fn test_file_metadata() -> FileMetadata {
    FileMetadata {
        file_type: FileType::RegularFile,
        size: 1024,
        permissions: 0o644,
        uid: 1000,
        gid: 1000,
        created_at: std::time::SystemTime::now(),
        modified_at: std::time::SystemTime::now(),
        accessed_at: std::time::SystemTime::now(),
    }
}

#[tokio::test]
async fn test_create_1000_files_and_query() {
    let (store, _temp) = create_test_store().await;
    let metadata = test_file_metadata();

    println!("Creating 1000 files...");
    let start = std::time::Instant::now();

    // Create 1000 files
    for i in 0..1000 {
        let path = PathBuf::from(format!("/test/file_{:04}.txt", i));
        let inode = store
            .reserve_inode()
            .await
            .expect("Failed to reserve inode");
        let file_id = FileId::generate();

        store
            .create_file(file_id, &path, inode, metadata.clone())
            .await
            .expect(&format!("Failed to create file {}", i));

        store
            .confirm_inode(inode)
            .await
            .expect("Failed to confirm inode");
    }

    let creation_time = start.elapsed();
    println!(
        "✓ Created 1000 files in {:?} ({:.2}ms per file)",
        creation_time,
        creation_time.as_secs_f64() * 1000.0 / 1000.0
    );

    // Query all files by path
    println!("Querying files by path...");
    let start = std::time::Instant::now();

    for i in 0..1000 {
        let path = PathBuf::from(format!("/test/file_{:04}.txt", i));
        let file = store
            .get_file_by_path(&path)
            .await
            .expect(&format!("Failed to get file {}", i));

        assert_eq!(file.path, path);
        assert_eq!(file.size, metadata.size);
    }

    let query_time = start.elapsed();
    println!(
        "✓ Queried 1000 files by path in {:?} ({:.2}ms per query)",
        query_time,
        query_time.as_secs_f64() * 1000.0 / 1000.0
    );

    // Query all files by inode
    println!("Querying files by inode...");
    let start = std::time::Instant::now();

    for i in 2..1002 {
        // Inodes start from 2
        let file = store
            .get_file_by_inode(i)
            .await
            .expect(&format!("Failed to get file with inode {}", i));

        assert_eq!(file.inode, i);
    }

    let inode_query_time = start.elapsed();
    println!(
        "✓ Queried 1000 files by inode in {:?} ({:.2}ms per query)",
        inode_query_time,
        inode_query_time.as_secs_f64() * 1000.0 / 1000.0
    );

    // List directory with all files
    println!("Listing directory with 1000 files...");
    let start = std::time::Instant::now();

    let files = store
        .list_directory(&PathBuf::from("/test"))
        .await
        .expect("Failed to list directory");

    let list_time = start.elapsed();
    println!("✓ Listed directory with 1000 files in {:?}", list_time);

    assert_eq!(files.len(), 1000);

    println!("\n=== Performance Summary ===");
    println!(
        "File creation:       {:.2}ms per file",
        creation_time.as_secs_f64() * 1000.0 / 1000.0
    );
    println!(
        "Path lookup:         {:.2}ms per query",
        query_time.as_secs_f64() * 1000.0 / 1000.0
    );
    println!(
        "Inode lookup:        {:.2}ms per query",
        inode_query_time.as_secs_f64() * 1000.0 / 1000.0
    );
    println!("Directory listing:   {:?} for 1000 files", list_time);
}

#[tokio::test]
async fn test_concurrent_reads() {
    let (store, _temp) = create_test_store().await;
    let metadata = test_file_metadata();

    // Create 100 files first
    println!("Setting up 100 files for concurrent access...");
    for i in 0..100 {
        let path = PathBuf::from(format!("/concurrent/file_{:03}.txt", i));
        let inode = store.reserve_inode().await.unwrap();
        let file_id = FileId::generate();

        store
            .create_file(file_id, &path, inode, metadata.clone())
            .await
            .unwrap();
        store.confirm_inode(inode).await.unwrap();
    }

    println!("Running concurrent read test with 50 parallel readers...");

    // Wrap store in Arc for sharing across tasks
    let store = Arc::new(store);
    let start = std::time::Instant::now();

    let mut tasks = JoinSet::new();

    // Spawn 50 concurrent read tasks
    for task_id in 0..50 {
        let store_clone = Arc::clone(&store);

        tasks.spawn(async move {
            // Each task reads 20 random files
            for i in 0..20 {
                let file_idx = (task_id * 20 + i) % 100;
                let path = PathBuf::from(format!("/concurrent/file_{:03}.txt", file_idx));

                let file = store_clone.get_file_by_path(&path).await.expect(&format!(
                    "Task {} failed to read file {}",
                    task_id, file_idx
                ));

                assert_eq!(file.path, path);
            }
        });
    }

    // Wait for all tasks to complete
    while let Some(result) = tasks.join_next().await {
        result.expect("Task panicked");
    }

    let concurrent_time = start.elapsed();

    println!(
        "✓ Completed 1000 concurrent reads (50 tasks × 20 reads) in {:?}",
        concurrent_time
    );
    println!(
        "  Average: {:.2}ms per read with concurrency",
        concurrent_time.as_secs_f64() * 1000.0 / 1000.0
    );

    // Verify concurrency benefit: should be faster than serial
    let expected_serial_time = std::time::Duration::from_millis(1000 * 3); // ~3ms per read serially
    assert!(
        concurrent_time < expected_serial_time,
        "Concurrent reads should be faster than serial reads"
    );
}

#[tokio::test]
async fn test_parent_child_relationships() {
    let (store, _temp) = create_test_store().await;
    let metadata = test_file_metadata();

    println!("Testing parent-child directory relationships...");

    // Create a directory tree:
    // /parent/
    //   ├── child1/
    //   │   ├── file1.txt
    //   │   └── file2.txt
    //   ├── child2/
    //   │   ├── file3.txt
    //   │   └── file4.txt
    //   └── file5.txt

    let test_structure = vec![
        "/parent/child1/file1.txt",
        "/parent/child1/file2.txt",
        "/parent/child2/file3.txt",
        "/parent/child2/file4.txt",
        "/parent/file5.txt",
    ];

    // Create all files
    for path_str in &test_structure {
        let path = PathBuf::from(path_str);
        let inode = store.reserve_inode().await.unwrap();
        let file_id = FileId::generate();

        store
            .create_file(file_id, &path, inode, metadata.clone())
            .await
            .expect(&format!("Failed to create {}", path_str));
        store.confirm_inode(inode).await.unwrap();
    }

    // Test listing /parent - should have 1 direct file (file5.txt)
    // Note: Subdirectories (child1, child2) are represented by their files having those parent_paths
    let parent_files = store
        .list_directory(&PathBuf::from("/parent"))
        .await
        .expect("Failed to list /parent");

    println!("✓ /parent contains {} entries", parent_files.len());
    assert_eq!(parent_files.len(), 1, "/parent should have 1 direct file");

    // Verify the entry
    assert_eq!(
        parent_files[0].name, "file5.txt",
        "Should find file5.txt in /parent"
    );

    // Test listing /parent/child1 - should have 2 files
    let child1_files = store
        .list_directory(&PathBuf::from("/parent/child1"))
        .await
        .expect("Failed to list /parent/child1");

    println!("✓ /parent/child1 contains {} files", child1_files.len());
    assert_eq!(child1_files.len(), 2, "/parent/child1 should have 2 files");

    // Test listing /parent/child2 - should have 2 files
    let child2_files = store
        .list_directory(&PathBuf::from("/parent/child2"))
        .await
        .expect("Failed to list /parent/child2");

    println!("✓ /parent/child2 contains {} files", child2_files.len());
    assert_eq!(child2_files.len(), 2, "/parent/child2 should have 2 files");

    // Verify all parent_path relationships are correct
    for file in &child1_files {
        assert_eq!(
            file.parent_path,
            PathBuf::from("/parent/child1"),
            "File in child1 should have correct parent_path"
        );
    }

    for file in &child2_files {
        assert_eq!(
            file.parent_path,
            PathBuf::from("/parent/child2"),
            "File in child2 should have correct parent_path"
        );
    }

    println!("✓ All parent-child relationships are correct");
}
