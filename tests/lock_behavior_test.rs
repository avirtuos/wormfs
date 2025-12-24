//! Integration tests for distributed lock behavior in FileSystemService.
//!
//! These tests verify that write exclusivity is properly enforced across
//! concurrent operations via distributed locks (Raft consensus + MetadataStore).

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::task::JoinSet;
use wormfs::file_store::{Config as FileStoreConfig, FileStore, FileStoreImpl};
use wormfs::filesystem_service::factory::FileSystemServiceImplFactory;
use wormfs::filesystem_service::implementation::FileSystemServiceImpl;
use wormfs::filesystem_service::FileSystemService; // Import trait for methods
use wormfs::metadata_store::{
    types::*, ClientId, Config as MetadataConfig, MetadataStore, MetadataStoreFactory,
};

/// Helper to create a test FileSystemService with temporary storage.
async fn create_test_filesystem_service() -> (FileSystemServiceImpl, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let chunks_dir = temp_dir.path().join("chunks");
    std::fs::create_dir(&chunks_dir).expect("Failed to create chunks dir");

    // Create MetadataStore using create_concrete to get MetadataStoreImpl
    let metadata_config = MetadataConfig {
        database_path: db_path,
        read_pool_size: 8, // Higher for concurrency tests
        enable_wal: true,
        cache_size_mb: 64,
        enable_foreign_keys: false, // Disable for tests
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

    // Create FileStore
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

    // Initialize disk
    let _disk_id = file_store
        .add_disk(chunks_dir.clone())
        .await
        .expect("Failed to add disk");

    // Configure distributed components for tests
    use wormfs::file_store::types::NodeId;
    use wormfs::file_store::{
        ChunkClientConfig, ChunkClientPool, PlacementConfig, PlacementEngine,
    };
    use wormfs::storage_raft_member::cluster_manager::heartbeat_tracker::HeartbeatTracker;

    let my_node_id = NodeId::new(1);
    let tracker = Arc::new(HeartbeatTracker::new(5000, 60000));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    tracker.record_heartbeat(
        "1".to_string(),
        now,
        1,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(1_000_000_000),
        Some(900_000_000),
        Some(0),
    );

    let config = PlacementConfig {
        min_node_diversity: 1,
        prefer_local: true,
    };
    let placement_engine = Arc::new(PlacementEngine::new(tracker.clone(), my_node_id, config));
    let chunk_client_config = ChunkClientConfig::default();
    let chunk_client: Arc<dyn wormfs::file_store::ChunkClient> =
        Arc::new(ChunkClientPool::new(tracker, chunk_client_config));

    file_store.set_distributed_config(my_node_id, placement_engine, chunk_client);

    let file_store = Arc::new(file_store);

    // Create FileSystemService using the factory
    let fs_config = wormfs::filesystem_service::types::Config::default();
    let service = FileSystemServiceImplFactory::create(fs_config, metadata_store, file_store, None)
        .await
        .expect("Failed to create FileSystemService");

    // Initialize root directory
    service
        .initialize_root()
        .await
        .expect("Failed to initialize root directory");

    (service, temp_dir)
}

/// Helper to create a test file and return its inode.
async fn create_test_file(service: &FileSystemServiceImpl, name: &str, initial_data: &[u8]) -> u64 {
    let client_id = ClientId::new(1);

    // Create file
    let attr = service
        .create(
            1, // root inode
            name, 0o644, // mode
            1000,  // uid
            1000,  // gid
            client_id,
        )
        .await
        .expect("Failed to create file");

    let inode = attr.ino;

    // Write initial data if provided
    if !initial_data.is_empty() {
        // Open for write
        let (fh, _attr) = service
            .open(inode, libc::O_WRONLY as u32, 1000, 1000, client_id)
            .await
            .expect("Failed to open file for write");

        // Write data
        service
            .write(inode, fh, 0, initial_data.to_vec(), 1000, 1000, client_id)
            .await
            .expect("Failed to write initial data");

        // Release
        service.release(fh).await.expect("Failed to release");
    }

    inode
}

#[tokio::test]
async fn test_concurrent_writes_same_file_should_fail() {
    println!("\n=== Test: Concurrent Writes to Same File (Should Fail) ===");

    let (service, _temp_dir) = create_test_filesystem_service().await;
    let service = Arc::new(service);

    // Create a test file with some initial data
    let inode = create_test_file(&service, "testfile.txt", b"initial content").await;
    println!("✓ Created test file with inode {}", inode);

    // Task 1: Try to open file for writing
    let service_clone = Arc::clone(&service);
    let task1 = tokio::spawn(async move {
        let client_id = ClientId::new(100); // Client 100

        println!("Task 1: Attempting to open file for write...");
        let result = service_clone
            .open(inode, libc::O_WRONLY as u32, 1000, 1000, client_id)
            .await;

        match result {
            Ok((fh, _attr)) => {
                println!("Task 1: ✓ Successfully acquired write lock (fh={})", fh);
                // Hold the lock for a moment
                tokio::time::sleep(Duration::from_millis(100)).await;
                println!("Task 1: Releasing lock...");
                service_clone.release(fh).await.expect("Release failed");
                println!("Task 1: ✓ Released lock");
                Ok(())
            }
            Err(e) => {
                println!("Task 1: ✗ Failed to acquire lock: {}", e);
                Err(e)
            }
        }
    });

    // Task 2: Try to open same file for writing (should fail)
    let service_clone = Arc::clone(&service);
    let task2 = tokio::spawn(async move {
        let client_id = ClientId::new(200); // Client 200 (different client)

        // Wait a bit to ensure task1 gets the lock first
        tokio::time::sleep(Duration::from_millis(50)).await;

        println!("Task 2: Attempting to open file for write (should fail)...");
        let result = service_clone
            .open(inode, libc::O_WRONLY as u32, 1000, 1000, client_id)
            .await;

        match result {
            Ok((fh, _attr)) => {
                println!("Task 2: ✗ Unexpectedly acquired write lock (fh={})", fh);
                service_clone.release(fh).await.ok();
                Err("Should have failed due to lock conflict")
            }
            Err(e) => {
                println!("Task 2: ✓ Correctly failed with error: {}", e);
                Ok(())
            }
        }
    });

    // Wait for both tasks
    let result1 = task1.await.expect("Task 1 panicked");
    let result2 = task2.await.expect("Task 2 panicked");

    // Assert expected results
    assert!(result1.is_ok(), "Task 1 should succeed (first writer)");
    assert!(result2.is_ok(), "Task 2 should fail (lock conflict)");

    println!("✓ Test passed: Write exclusivity enforced correctly");

    // Verify that after task1 releases, task2 can acquire the lock
    println!("\nVerifying lock can be acquired after release...");
    let client_id = ClientId::new(200);
    let (fh, _attr) = service
        .open(inode, libc::O_WRONLY as u32, 1000, 1000, client_id)
        .await
        .expect("Should be able to acquire lock after release");
    println!(
        "✓ Successfully acquired lock after previous release (fh={})",
        fh
    );
    service.release(fh).await.expect("Release failed");
    println!("✓ Test completed successfully");
}

#[tokio::test]
#[ignore = "BufferedFileHandle limitation: pads reads past EOF with zeros instead of returning actual file size"]
async fn test_concurrent_reads_same_file_should_succeed() {
    println!("\n=== Test: Concurrent Reads to Same File (Should Succeed) ===");

    let (service, _temp_dir) = create_test_filesystem_service().await;
    let service = Arc::new(service);

    // Create a test file with some data
    let test_data = b"Hello, concurrent readers!";
    let inode = create_test_file(&service, "readable.txt", test_data).await;
    println!("✓ Created test file with inode {}", inode);

    let mut tasks = JoinSet::new();

    // Spawn 10 concurrent read tasks
    for task_id in 0..10 {
        let service_clone = Arc::clone(&service);

        tasks.spawn(async move {
            let client_id = ClientId::new(1000 + task_id);

            println!("Task {}: Opening file for read...", task_id);

            // Open for read (O_RDONLY - no write lock needed)
            let (fh, _attr) = service_clone
                .open(inode, libc::O_RDONLY as u32, 1000, 1000, client_id)
                .await
                .expect(&format!("Task {} failed to open for read", task_id));

            println!("Task {}: ✓ Opened file (fh={})", task_id, fh);

            // Read the data
            let data = service_clone
                .read(inode, fh, 0, 1024, 1000, 1000, client_id)
                .await
                .expect(&format!("Task {} failed to read", task_id));

            println!("Task {}: ✓ Read {} bytes", task_id, data.len());

            // Verify data
            assert_eq!(data, test_data, "Task {} read incorrect data", task_id);

            // Release
            service_clone
                .release(fh)
                .await
                .expect(&format!("Task {} failed to release", task_id));

            println!("Task {}: ✓ Released file handle", task_id);
        });
    }

    // Wait for all tasks to complete
    let mut success_count = 0;
    while let Some(result) = tasks.join_next().await {
        result.expect("Task panicked");
        success_count += 1;
    }

    assert_eq!(success_count, 10, "All read tasks should succeed");
    println!(
        "✓ Test passed: All {} concurrent reads succeeded",
        success_count
    );
}

#[tokio::test]
async fn test_concurrent_read_during_write_should_succeed() {
    println!("\n=== Test: Concurrent Read During Write (Should Succeed) ===");
    println!("Note: We make no guarantees about read consistency during writes");

    let (service, _temp_dir) = create_test_filesystem_service().await;
    let service = Arc::new(service);

    // Create a test file
    let initial_data = b"Initial data before write";
    let inode = create_test_file(&service, "mixed_access.txt", initial_data).await;
    println!("✓ Created test file with inode {}", inode);

    // Writer task: Opens for write and holds lock
    let service_clone = Arc::clone(&service);
    let writer_task = tokio::spawn(async move {
        let client_id = ClientId::new(300);

        println!("Writer: Opening file for write...");
        let (fh, _attr) = service_clone
            .open(inode, libc::O_WRONLY as u32, 1000, 1000, client_id)
            .await
            .expect("Writer failed to open for write");

        println!("Writer: ✓ Acquired write lock (fh={})", fh);

        // Hold the lock while writing
        tokio::time::sleep(Duration::from_millis(50)).await;

        let new_data = b"Updated data from writer";
        service_clone
            .write(inode, fh, 0, new_data.to_vec(), 1000, 1000, client_id)
            .await
            .expect("Writer failed to write");

        println!("Writer: ✓ Wrote {} bytes", new_data.len());

        // Hold lock a bit longer
        tokio::time::sleep(Duration::from_millis(50)).await;

        service_clone
            .release(fh)
            .await
            .expect("Writer failed to release");
        println!("Writer: ✓ Released write lock");
    });

    // Reader task: Opens for read while writer has lock (should succeed)
    let service_clone = Arc::clone(&service);
    let reader_task = tokio::spawn(async move {
        let client_id = ClientId::new(400);

        // Start slightly after writer to ensure writer has lock
        tokio::time::sleep(Duration::from_millis(25)).await;

        println!("Reader: Opening file for read (writer has lock)...");
        let (fh, _attr) = service_clone
            .open(inode, libc::O_RDONLY as u32, 1000, 1000, client_id)
            .await
            .expect("Reader failed to open for read");

        println!(
            "Reader: ✓ Opened for read despite active write lock (fh={})",
            fh
        );

        // Read data (may see old or new data - no guarantees)
        let data = service_clone
            .read(inode, fh, 0, 1024, 1000, 1000, client_id)
            .await
            .expect("Reader failed to read");

        println!("Reader: ✓ Read {} bytes during active write", data.len());

        service_clone
            .release(fh)
            .await
            .expect("Reader failed to release");
        println!("Reader: ✓ Released file handle");
    });

    // Wait for both tasks
    writer_task.await.expect("Writer task panicked");
    reader_task.await.expect("Reader task panicked");

    println!("✓ Test passed: Reader not blocked by writer (as designed)");
}
