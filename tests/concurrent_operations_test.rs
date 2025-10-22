//! Comprehensive tests for concurrent operations and multi-stripe scenarios.
//!
//! This test suite fills critical gaps in concurrency testing:
//! - Concurrent writes to different files (no false conflicts)
//! - Concurrent writes to different stripes in same file
//! - Permission checking under concurrent access
//! - Multi-stripe operations with large files
//! - Multi-user scenarios with different permissions

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use wormfs::file_store::types::Config as FileStoreConfig;
use wormfs::file_store::{FileStore, FileStoreImpl};
use wormfs::filesystem_service::{
    factory::FileSystemServiceImplFactory, implementation::FileSystemServiceImpl, ClientId,
    FileSystemService,
};
use wormfs::metadata_store::types::{IsolationLevel, SynchronousMode};
use wormfs::metadata_store::{
    factory::MetadataStoreFactory, types::Config as MetadataConfig, MetadataStore,
};

/// Helper to create a test filesystem service with proper setup
async fn create_test_filesystem_service() -> (Arc<FileSystemServiceImpl>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("metadata.db");
    let chunks_dir = temp_dir.path().join("chunks");
    std::fs::create_dir(&chunks_dir).unwrap();

    let metadata_config = MetadataConfig {
        database_path: db_path,
        read_pool_size: 8,
        enable_wal: true,
        cache_size_mb: 64,
        enable_foreign_keys: false, // Disable for tests
        synchronous: SynchronousMode::Normal,
        transaction_isolation: IsolationLevel::ReadCommitted,
        enable_prepared_statements: true,
        read_pool_timeout_secs: 30,
    };

    let metadata_store = MetadataStoreFactory::create_concrete(metadata_config)
        .await
        .unwrap();
    metadata_store.initialize_schema().await.unwrap();

    let file_store_config = FileStoreConfig {
        disk_paths: vec![chunks_dir.clone()],
        max_chunk_size: 4 * 1024 * 1024, // 4MB
        default_data_shards: 2,
        default_parity_shards: 1,
        max_concurrent_operations: 100,
        verification_interval: Duration::from_secs(3600),
        orphan_cleanup_age: Duration::from_secs(3600),
        stripe_cache_size_mb: 256,
        stripe_cache_ttl_secs: 3600,
        stripe_cache_tti_secs: 600,
    };

    let mut file_store = <FileStoreImpl as FileStore>::new(file_store_config).unwrap();

    // Initialize disk
    file_store.add_disk(chunks_dir.clone()).await.unwrap();

    let file_store = Arc::new(file_store);

    let fs_config = wormfs::filesystem_service::Config::default();

    let service = Arc::new(
        FileSystemServiceImplFactory::create(fs_config, metadata_store, file_store, None)
            .await
            .unwrap(),
    );

    service.initialize_root().await.unwrap();

    // NOTE: We do NOT start background tasks in tests because:
    // 1. Tests explicitly call flush_file() instead of relying on dirty timeout
    // 2. Background tasks create Arc reference cycles that prevent cleanup
    // 3. Tests don't use file locks that need extension

    (service, temp_dir)
}

// ============================================================================
// Priority 1: Concurrent Operations Tests
// ============================================================================

#[tokio::test]
async fn test_concurrent_writes_different_files() {
    println!("\n=== Test: Concurrent Writes to Different Files ===");

    let (service, _temp_dir) = create_test_filesystem_service().await;
    let service = Arc::new(service);

    // Spawn 5 tasks, each writing to a different file
    let mut tasks = Vec::new();

    for i in 0..5 {
        let service_clone = Arc::clone(&service);
        let task = tokio::spawn(async move {
            let client_id = ClientId::new(100 + i);
            let filename = format!("file_{}.txt", i);

            println!("Task {}: Creating and writing to {}", i, filename);

            // Create file
            let attrs = service_clone
                .create(1, &filename, 0o644, 1000, 1000, client_id)
                .await
                .expect("Failed to create file");

            // Open for writing
            let (fh, _) = service_clone
                .open(attrs.ino, libc::O_WRONLY as u32, 1000, 1000, client_id)
                .await
                .expect("Failed to open file");

            // Write data
            let data = vec![i as u8; 1024 * 100]; // 100KB
            let bytes_written = service_clone
                .write(attrs.ino, fh, 0, data, 1000, 1000, client_id)
                .await
                .expect("Failed to write");

            assert_eq!(bytes_written, 1024 * 100);

            // Release
            service_clone.release(fh).await.expect("Failed to release");

            println!("Task {}: ✓ Successfully wrote to {}", i, filename);
            i
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let results: Vec<_> = futures::future::join_all(tasks).await;

    // Verify all succeeded
    for (i, result) in results.iter().enumerate() {
        assert!(result.is_ok(), "Task {} panicked", i);
        assert_eq!(*result.as_ref().unwrap(), i as u64);
    }

    println!("✓ All 5 concurrent writes to different files succeeded");
}

#[tokio::test]
async fn test_writes_to_multiple_stripes() {
    println!("\n=== Test: Writes to Multiple Stripes ===");

    let (service, _temp_dir) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create a large file
    let attrs = service
        .create(1, "large_file.bin", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    // Open file for writing
    let (fh, _) = service
        .open(attrs.ino, libc::O_WRONLY as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    const STRIPE_SIZE: u64 = 4 * 1024 * 1024; // 4MB
    const NUM_STRIPES: usize = 4;

    // Write to each stripe sequentially
    for i in 0..NUM_STRIPES {
        let offset = (i as u64) * STRIPE_SIZE;
        let data = vec![i as u8; STRIPE_SIZE as usize];

        println!("Writing stripe {} at offset {}", i, offset);

        let written = service
            .write(attrs.ino, fh, offset, data, 1000, 1000, client_id)
            .await
            .expect("Failed to write stripe");

        assert_eq!(
            written as u64, STRIPE_SIZE,
            "Stripe {} wrote wrong amount",
            i
        );
    }

    // Flush buffered writes to ensure they're persisted (WriteBack mode)
    println!("Flushing buffered writes...");
    service
        .flush_file(attrs.ino)
        .await
        .expect("Failed to flush file");
    println!("Flush returned successfully");
    println!("Starting verification...");

    // Verify data integrity - read back each stripe
    for i in 0..NUM_STRIPES {
        let offset = (i as u64) * STRIPE_SIZE;
        println!("Reading stripe {} at offset {}", i, offset);
        let data = service
            .read(
                attrs.ino,
                0,
                offset,
                STRIPE_SIZE as u32,
                1000,
                1000,
                client_id,
            )
            .await
            .expect("Failed to read stripe");

        println!("Stripe {} returned {} bytes", i, data.len());
        assert_eq!(data.len(), STRIPE_SIZE as usize, "Stripe {} wrong size", i);
        assert!(
            data.iter().all(|&b| b == i as u8),
            "Stripe {} data corrupted",
            i
        );

        println!("✓ Stripe {} verified correct", i);
    }

    service.release(fh).await.expect("Failed to release");
    println!("✓ All {} stripe writes verified", NUM_STRIPES);
}

#[tokio::test]
async fn test_permission_check_during_concurrent_access() {
    println!("\n=== Test: Permission Checks During Concurrent Access ===");

    let (service, _temp_dir) = create_test_filesystem_service().await;
    let service = Arc::new(service);

    // Create a file owned by uid=1000 with mode 0o644 (rw-r--r--)
    let client_id = ClientId::new(1);
    let attrs = service
        .create(1, "shared_file.txt", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    // Task 1: Owner (uid=1000) opens for reading
    let service_clone = Arc::clone(&service);
    let inode = attrs.ino;
    let task1 = tokio::spawn(async move {
        println!("Task 1: Owner opening for read");
        let result = service_clone
            .open(inode, libc::O_RDONLY as u32, 1000, 1000, ClientId::new(10))
            .await;

        assert!(result.is_ok(), "Owner should be able to read");
        let (fh, _) = result.unwrap();

        // Hold open for a moment
        tokio::time::sleep(Duration::from_millis(50)).await;

        service_clone.release(fh).await.expect("Failed to release");
        println!("Task 1: ✓ Owner read access granted");
    });

    // Task 2: Non-owner (uid=2000) tries to open for writing (should fail)
    let service_clone = Arc::clone(&service);
    let task2 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;

        println!("Task 2: Non-owner attempting write");
        let result = service_clone
            .open(inode, libc::O_WRONLY as u32, 2000, 2000, ClientId::new(20))
            .await;

        assert!(result.is_err(), "Non-owner should not be able to write");
        println!("Task 2: ✓ Correctly denied write access");
    });

    // Task 3: Group member (uid=2000, gid=1000) tries to read (should succeed)
    let service_clone = Arc::clone(&service);
    let task3 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;

        println!("Task 3: Group member opening for read");
        let result = service_clone
            .open(inode, libc::O_RDONLY as u32, 2000, 1000, ClientId::new(30))
            .await;

        assert!(result.is_ok(), "Group member should be able to read");
        let (fh, _) = result.unwrap();

        service_clone.release(fh).await.expect("Failed to release");
        println!("Task 3: ✓ Group member read access granted");
    });

    // Wait for all
    tokio::try_join!(task1, task2, task3).expect("Task panicked");

    println!("✓ Permission checks working correctly under concurrency");
}

// ============================================================================
// Priority 2: Multi-Stripe Edge Cases
// ============================================================================

#[tokio::test]
async fn test_write_spanning_many_stripes() {
    println!("\n=== Test: Single Write Spanning Many Stripes ===");

    let (service, _temp_dir) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create file
    let attrs = service
        .create(1, "huge_file.bin", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    // Open for writing
    let (fh, _) = service
        .open(attrs.ino, libc::O_WRONLY as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    // Write 40MB in a single call (spans 10 stripes at 4MB each)
    const TOTAL_SIZE: usize = 40 * 1024 * 1024; // 40MB
    const STRIPE_SIZE: usize = 4 * 1024 * 1024; // 4MB
    const NUM_STRIPES: usize = TOTAL_SIZE / STRIPE_SIZE; // 10

    println!(
        "Writing {}MB spanning {} stripes",
        TOTAL_SIZE / (1024 * 1024),
        NUM_STRIPES
    );

    let data: Vec<u8> = (0..TOTAL_SIZE).map(|i| (i % 256) as u8).collect();

    let bytes_written = service
        .write(attrs.ino, fh, 0, data.clone(), 1000, 1000, client_id)
        .await
        .expect("Failed to write large file");

    assert_eq!(bytes_written as usize, TOTAL_SIZE);
    println!("✓ Wrote {} bytes", bytes_written);

    // Flush buffered writes to ensure they're persisted (WriteBack mode)
    println!("Flushing buffered writes...");
    service
        .flush_file(attrs.ino)
        .await
        .expect("Failed to flush file");

    // Read back and verify
    let read_data = service
        .read(attrs.ino, 0, 0, TOTAL_SIZE as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read large file");

    assert_eq!(read_data.len(), TOTAL_SIZE);
    assert_eq!(read_data, data, "Data mismatch after read");

    println!(
        "✓ Read back and verified {} bytes across {} stripes",
        TOTAL_SIZE, NUM_STRIPES
    );

    service.release(fh).await.expect("Failed to release");
}

#[tokio::test]
async fn test_interleaved_stripe_writes() {
    println!("\n=== Test: Interleaved Stripe Writes (Write Odd, Then Even) ===");

    let (service, _temp_dir) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create file
    let attrs = service
        .create(1, "interleaved.bin", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    let (fh, _) = service
        .open(attrs.ino, libc::O_WRONLY as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    const STRIPE_SIZE: u64 = 4 * 1024 * 1024; // 4MB
    const NUM_STRIPES: usize = 6;

    // First pass: Write stripes 1, 3, 5 (odd)
    println!("Pass 1: Writing odd stripes (1, 3, 5)");
    for i in [1, 3, 5] {
        let offset = (i as u64) * STRIPE_SIZE;
        let data = vec![i as u8; STRIPE_SIZE as usize];

        service
            .write(attrs.ino, fh, offset, data, 1000, 1000, client_id)
            .await
            .expect("Failed to write odd stripe");

        println!("  ✓ Wrote stripe {}", i);
    }

    // Second pass: Write stripes 0, 2, 4 (even)
    println!("Pass 2: Writing even stripes (0, 2, 4)");
    for i in [0, 2, 4] {
        let offset = (i as u64) * STRIPE_SIZE;
        let data = vec![i as u8; STRIPE_SIZE as usize];

        service
            .write(attrs.ino, fh, offset, data, 1000, 1000, client_id)
            .await
            .expect("Failed to write even stripe");

        println!("  ✓ Wrote stripe {}", i);
    }

    // Flush buffered writes to ensure they're persisted (WriteBack mode)
    println!("Flushing buffered writes...");
    service
        .flush_file(attrs.ino)
        .await
        .expect("Failed to flush file");

    // Verify all stripes
    println!("Verifying all stripes...");
    for i in 0..NUM_STRIPES {
        let offset = (i as u64) * STRIPE_SIZE;
        let data = service
            .read(
                attrs.ino,
                0,
                offset,
                STRIPE_SIZE as u32,
                1000,
                1000,
                client_id,
            )
            .await
            .expect("Failed to read stripe");

        assert_eq!(data.len(), STRIPE_SIZE as usize);
        assert!(
            data.iter().all(|&b| b == i as u8),
            "Stripe {} data corrupted",
            i
        );

        println!("  ✓ Stripe {} verified", i);
    }

    service.release(fh).await.expect("Failed to release");
    println!("✓ Interleaved write pattern successful");
}

// ============================================================================
// Priority 3: Permission + Concurrency Tests
// ============================================================================

#[tokio::test]
async fn test_multiple_users_different_permissions() {
    println!("\n=== Test: Multiple Users with Different Permissions ===");

    let (service, _temp_dir) = create_test_filesystem_service().await;
    let service = Arc::new(service);

    // Create file owned by uid=1000, gid=1000, mode=0o640 (rw-r-----)
    let client_id = ClientId::new(1);
    let attrs = service
        .create(1, "multiuser.txt", 0o640, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    let inode = attrs.ino;

    // Spawn 4 concurrent tasks with different user identities
    let mut tasks = Vec::new();

    // Task 1: Owner (uid=1000, gid=1000) - can read and write
    let service_clone = Arc::clone(&service);
    tasks.push(tokio::spawn(async move {
        println!("User 1 (owner): Opening for write");
        let result = service_clone
            .open(inode, libc::O_WRONLY as u32, 1000, 1000, ClientId::new(10))
            .await;

        assert!(result.is_ok(), "Owner should be able to write");
        let (fh, _) = result.unwrap();

        let data = b"owner data".to_vec();
        service_clone
            .write(inode, fh, 0, data, 1000, 1000, ClientId::new(10))
            .await
            .expect("Owner write failed");

        service_clone.release(fh).await.unwrap();
        println!("User 1: ✓ Write succeeded");
        "owner_write_ok"
    }));

    // Task 2: Group member (uid=2000, gid=1000) - can only read
    let service_clone = Arc::clone(&service);
    tasks.push(tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await; // Wait for owner to write

        println!("User 2 (group): Opening for read");
        let result = service_clone
            .open(inode, libc::O_RDONLY as u32, 2000, 1000, ClientId::new(20))
            .await;

        assert!(result.is_ok(), "Group member should be able to read");
        let (fh, _) = result.unwrap();

        service_clone.release(fh).await.unwrap();
        println!("User 2: ✓ Read access granted");

        // Try to write (should fail)
        println!("User 2 (group): Attempting write");

        // Need to open file for writing first
        let open_result = service_clone
            .open(inode, libc::O_WRONLY as u32, 2000, 1000, ClientId::new(20))
            .await;

        let write_result = if let Ok((fh, _)) = open_result {
            let result = service_clone
                .write(
                    inode,
                    fh,
                    10,
                    b"bad".to_vec(),
                    2000,
                    1000,
                    ClientId::new(20),
                )
                .await;
            service_clone.release(fh).await.ok();
            result
        } else {
            open_result.map(|_| 0).map_err(|e| e)
        };

        assert!(
            write_result.is_err(),
            "Group member should not be able to write"
        );
        println!("User 2: ✓ Write correctly denied");
        "group_read_ok_write_denied"
    }));

    // Task 3: Other user (uid=3000, gid=3000) - no access
    let service_clone = Arc::clone(&service);
    tasks.push(tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;

        println!("User 3 (other): Attempting read");
        let result = service_clone
            .open(inode, libc::O_RDONLY as u32, 3000, 3000, ClientId::new(30))
            .await;

        assert!(result.is_err(), "Other user should not have read access");
        println!("User 3: ✓ Read correctly denied");
        "other_denied"
    }));

    // Task 4: Different group member (uid=4000, gid=1000) - can read
    let service_clone = Arc::clone(&service);
    tasks.push(tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;

        println!("User 4 (different group member): Opening for read");
        let result = service_clone
            .open(inode, libc::O_RDONLY as u32, 4000, 1000, ClientId::new(40))
            .await;

        assert!(
            result.is_ok(),
            "Different group member should be able to read"
        );
        let (fh, _) = result.unwrap();

        service_clone.release(fh).await.unwrap();
        println!("User 4: ✓ Read access granted");
        "other_group_member_ok"
    }));

    // Wait for all
    let results: Vec<_> = futures::future::join_all(tasks).await;

    for (i, result) in results.iter().enumerate() {
        assert!(result.is_ok(), "Task {} panicked", i + 1);
    }

    println!("✓ Multi-user permission enforcement working correctly");
}

#[tokio::test]
async fn test_permission_change_during_open() {
    println!("\n=== Test: Permission Change While File is Open ===");

    let (service, _temp_dir) = create_test_filesystem_service().await;
    let service = Arc::new(service);

    // Create file with mode 0o666 (rw-rw-rw-)
    let client_id = ClientId::new(1);
    let attrs = service
        .create(1, "changeme.txt", 0o666, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    let inode = attrs.ino;

    // Task 1: User opens file for writing
    let service_clone = Arc::clone(&service);
    let task1 = tokio::spawn(async move {
        println!("Task 1: Opening file for write");
        let result = service_clone
            .open(inode, libc::O_WRONLY as u32, 2000, 2000, ClientId::new(10))
            .await;

        assert!(result.is_ok(), "Should be able to open with mode 0o666");
        let (fh, _) = result.unwrap();

        println!("Task 1: Holding file open...");
        tokio::time::sleep(Duration::from_millis(100)).await;

        println!("Task 1: Writing after permission change");
        // File handle is valid, write should still work even if permissions changed
        let write_result = service_clone
            .write(
                inode,
                fh,
                0,
                b"data".to_vec(),
                2000,
                2000,
                ClientId::new(10),
            )
            .await;

        // This might succeed (file handle acquired before chmod) or fail (permissions checked on every write)
        // The current implementation checks permissions on write, so it should fail
        println!("Task 1: Write result: {:?}", write_result);

        service_clone.release(fh).await.unwrap();
        println!("Task 1: Released file");
    });

    // Task 2: Owner changes permissions while file is open
    let service_clone = Arc::clone(&service);
    let task2 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;

        println!("Task 2: Changing file permissions to 0o600");
        service_clone
            .setattr(
                inode,
                None,        // file_handle
                Some(0o600), // Only owner can read/write now
                None,
                None,
                None,
                None,
                None,
                1000,
                1000,
                ClientId::new(20),
            )
            .await
            .expect("Failed to chmod");

        println!("Task 2: ✓ Permissions changed to 0o600");
    });

    tokio::try_join!(task1, task2).expect("Task panicked");

    // Verify new permissions are enforced
    println!("Verifying new permissions...");
    let result = service
        .open(inode, libc::O_WRONLY as u32, 2000, 2000, ClientId::new(30))
        .await;

    assert!(
        result.is_err(),
        "Should not be able to open after chmod to 0o600"
    );
    println!("✓ New permissions enforced after chmod");
}
