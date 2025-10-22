//! Integration tests for stripe partial overwrite scenarios
//!
//! These tests verify that BufferedFileHandle correctly handles read-modify-write
//! when modifying data within already-written stripes. This exercises the critical
//! code path where existing stripe data must be loaded before creating a replacement.

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

const STRIPE_SIZE: usize = 4 * 1024 * 1024; // 4MB stripes
const MB: usize = 1024 * 1024;

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
    };

    let mut file_store =
        <FileStoreImpl as FileStore>::new(file_store_config).expect("Failed to create FileStore");

    let _disk_id = file_store
        .add_disk(chunks_dir.clone())
        .await
        .expect("Failed to add disk");

    let file_store = Arc::new(file_store);

    let fs_config = wormfs::filesystem_service::types::Config::default();
    let service = FileSystemServiceImplFactory::create(fs_config, metadata_store, file_store, None)
        .await
        .expect("Failed to create FileSystemService");

    service
        .initialize_root()
        .await
        .expect("Failed to initialize root");

    (service, temp_dir)
}

/// Helper to write data with a specific pattern to a file at given offset.
async fn write_pattern(
    service: &FileSystemServiceImpl,
    inode: u64,
    fh: u64,
    offset: u64,
    size: usize,
    pattern: u8,
    client_id: ClientId,
) {
    let data = vec![pattern; size];

    let bytes_written = service
        .write(inode, fh, offset, data, 1000, 1000, client_id)
        .await
        .expect(&format!(
            "Failed to write pattern 0x{:02x} at offset {}",
            pattern, offset
        ));

    assert_eq!(
        bytes_written as usize, size,
        "Write size mismatch: expected {}, got {}",
        size, bytes_written
    );
}

/// Helper to verify a region of data has the expected pattern.
/// Returns true if all bytes match, false otherwise.
fn verify_region(
    data: &[u8],
    start: usize,
    end: usize,
    expected_pattern: u8,
    region_name: &str,
) -> bool {
    if start >= data.len() {
        eprintln!(
            "✗ Region '{}': start offset {} is beyond data length {}",
            region_name,
            start,
            data.len()
        );
        return false;
    }

    let actual_end = end.min(data.len());
    let region = &data[start..actual_end];

    for (i, &byte) in region.iter().enumerate() {
        if byte != expected_pattern {
            eprintln!(
                "✗ Region '{}': mismatch at offset {} (global offset {}): expected 0x{:02x}, got 0x{:02x}",
                region_name, i, start + i, expected_pattern, byte
            );
            return false;
        }
    }

    println!(
        "✓ Region '{}' [{}..{}]: all {} bytes = 0x{:02x}",
        region_name,
        start,
        actual_end,
        actual_end - start,
        expected_pattern
    );
    true
}

/// Test Case 1: Partial overwrite in the middle of a stripe
///
/// This test writes 4 complete stripes (16MB), then modifies 1MB in the middle
/// of stripe 1, and verifies all data (modified and unmodified) is correct.
#[tokio::test]
async fn test_stripe_partial_overwrite_middle() {
    println!("\n=== Test: Stripe Partial Overwrite (Middle) ===\n");

    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create and open file
    let attrs = service
        .create(1, "testfile.dat", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");
    let inode = attrs.ino;

    let (fh, _) = service
        .open(inode, libc::O_RDWR as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    println!("Phase 1: Writing 16MB (4 stripes) with distinct patterns...");

    // Write 4 complete stripes with different patterns
    write_pattern(
        &service,
        inode,
        fh,
        0 * STRIPE_SIZE as u64,
        STRIPE_SIZE,
        0xAA,
        client_id,
    )
    .await;
    println!("  Stripe 0 (0-4MB): Pattern 0xAA");

    write_pattern(
        &service,
        inode,
        fh,
        1 * STRIPE_SIZE as u64,
        STRIPE_SIZE,
        0xBB,
        client_id,
    )
    .await;
    println!("  Stripe 1 (4-8MB): Pattern 0xBB");

    write_pattern(
        &service,
        inode,
        fh,
        2 * STRIPE_SIZE as u64,
        STRIPE_SIZE,
        0xCC,
        client_id,
    )
    .await;
    println!("  Stripe 2 (8-12MB): Pattern 0xCC");

    write_pattern(
        &service,
        inode,
        fh,
        3 * STRIPE_SIZE as u64,
        STRIPE_SIZE,
        0xDD,
        client_id,
    )
    .await;
    println!("  Stripe 3 (12-16MB): Pattern 0xDD");

    println!("\nPhase 2: Overwriting 1MB in middle of Stripe 1 (offset 5MB)...");

    // Modify 1MB in the middle of stripe 1 (offset 5MB, which is 1MB into stripe 1)
    let overwrite_offset = (1 * STRIPE_SIZE + 1 * MB) as u64; // 5MB
    let overwrite_size = 1 * MB; // 1MB
    write_pattern(
        &service,
        inode,
        fh,
        overwrite_offset,
        overwrite_size,
        0x11,
        client_id,
    )
    .await;
    println!("  Modified region (5-6MB): Pattern 0x11");

    // Flush to ensure all data is persisted
    service.flush_file(inode).await.expect("Failed to flush");
    println!("\nPhase 3: Flushed file");

    println!("\nPhase 4: Reading back and verifying all 16MB...");

    // Read entire file
    let read_data = service
        .read(inode, 0, 0, 16 * MB as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read file");

    assert_eq!(read_data.len(), 16 * MB, "Read size mismatch");
    println!("  Read {} bytes", read_data.len());

    println!("\nPhase 5: Byte-by-byte verification...");

    // Verify each region
    let mut all_ok = true;

    // Stripe 0: Should be unchanged (0xAA)
    all_ok &= verify_region(&read_data, 0, 4 * MB, 0xAA, "Stripe 0 (unchanged)");

    // Stripe 1 - First 1MB: Should be unchanged (0xBB)
    all_ok &= verify_region(
        &read_data,
        4 * MB,
        5 * MB,
        0xBB,
        "Stripe 1 first 1MB (unchanged)",
    );

    // Stripe 1 - Middle 1MB: Should be modified (0x11)
    all_ok &= verify_region(
        &read_data,
        5 * MB,
        6 * MB,
        0x11,
        "Stripe 1 middle 1MB (MODIFIED)",
    );

    // Stripe 1 - Last 2MB: Should be unchanged (0xBB)
    all_ok &= verify_region(
        &read_data,
        6 * MB,
        8 * MB,
        0xBB,
        "Stripe 1 last 2MB (unchanged)",
    );

    // Stripe 2: Should be unchanged (0xCC)
    all_ok &= verify_region(&read_data, 8 * MB, 12 * MB, 0xCC, "Stripe 2 (unchanged)");

    // Stripe 3: Should be unchanged (0xDD)
    all_ok &= verify_region(&read_data, 12 * MB, 16 * MB, 0xDD, "Stripe 3 (unchanged)");

    service
        .release(fh)
        .await
        .expect("Failed to release file handle");

    assert!(
        all_ok,
        "Data verification failed - some regions have incorrect patterns"
    );
    println!("\n✓ Test PASSED: All regions verified correctly!");
}

/// Test Case 2: Multiple partial overwrites across different stripes
///
/// This test writes 4 stripes, then modifies regions in stripes 0 and 2,
/// and verifies all data is correct.
#[tokio::test]
async fn test_stripe_multiple_partial_overwrites() {
    println!("\n=== Test: Multiple Partial Overwrites ===\n");

    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create and open file
    let attrs = service
        .create(1, "testfile.dat", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");
    let inode = attrs.ino;

    let (fh, _) = service
        .open(inode, libc::O_RDWR as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    println!("Phase 1: Writing 16MB (4 stripes) with patterns...");

    // Write 4 complete stripes
    write_pattern(
        &service,
        inode,
        fh,
        0 * STRIPE_SIZE as u64,
        STRIPE_SIZE,
        0xAA,
        client_id,
    )
    .await;
    write_pattern(
        &service,
        inode,
        fh,
        1 * STRIPE_SIZE as u64,
        STRIPE_SIZE,
        0xBB,
        client_id,
    )
    .await;
    write_pattern(
        &service,
        inode,
        fh,
        2 * STRIPE_SIZE as u64,
        STRIPE_SIZE,
        0xCC,
        client_id,
    )
    .await;
    write_pattern(
        &service,
        inode,
        fh,
        3 * STRIPE_SIZE as u64,
        STRIPE_SIZE,
        0xDD,
        client_id,
    )
    .await;

    println!("\nPhase 2: Multiple overwrites...");

    // Overwrite 512KB in stripe 0 at offset 1MB
    println!("  Overwrite 1: 512KB in Stripe 0 at offset 1MB");
    write_pattern(
        &service,
        inode,
        fh,
        1 * MB as u64,
        512 * 1024,
        0x11,
        client_id,
    )
    .await;

    // Overwrite 2MB in stripe 2 at offset 9MB
    println!("  Overwrite 2: 2MB in Stripe 2 at offset 9MB");
    write_pattern(&service, inode, fh, 9 * MB as u64, 2 * MB, 0x22, client_id).await;

    // Flush
    service.flush_file(inode).await.expect("Failed to flush");

    println!("\nPhase 3: Reading and verifying...");

    // Read entire file
    let read_data = service
        .read(inode, 0, 0, 16 * MB as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read file");

    let mut all_ok = true;

    // Stripe 0: First 1MB unchanged (0xAA)
    all_ok &= verify_region(&read_data, 0, 1 * MB, 0xAA, "Stripe 0 [0-1MB] unchanged");

    // Stripe 0: 512KB modified (0x11)
    all_ok &= verify_region(
        &read_data,
        1 * MB,
        1 * MB + 512 * 1024,
        0x11,
        "Stripe 0 [1-1.5MB] MODIFIED",
    );

    // Stripe 0: Rest unchanged (0xAA)
    all_ok &= verify_region(
        &read_data,
        1 * MB + 512 * 1024,
        4 * MB,
        0xAA,
        "Stripe 0 [1.5-4MB] unchanged",
    );

    // Stripe 1: All unchanged (0xBB)
    all_ok &= verify_region(&read_data, 4 * MB, 8 * MB, 0xBB, "Stripe 1 unchanged");

    // Stripe 2: First 1MB unchanged (0xCC)
    all_ok &= verify_region(
        &read_data,
        8 * MB,
        9 * MB,
        0xCC,
        "Stripe 2 [0-1MB] unchanged",
    );

    // Stripe 2: 2MB modified (0x22)
    all_ok &= verify_region(
        &read_data,
        9 * MB,
        11 * MB,
        0x22,
        "Stripe 2 [1-3MB] MODIFIED",
    );

    // Stripe 2: Last 1MB unchanged (0xCC)
    all_ok &= verify_region(
        &read_data,
        11 * MB,
        12 * MB,
        0xCC,
        "Stripe 2 [3-4MB] unchanged",
    );

    // Stripe 3: All unchanged (0xDD)
    all_ok &= verify_region(&read_data, 12 * MB, 16 * MB, 0xDD, "Stripe 3 unchanged");

    service.release(fh).await.expect("Failed to release");

    assert!(all_ok, "Data verification failed");
    println!("\n✓ Test PASSED: Multiple overwrites verified correctly!");
}

/// Test Case 3: Overwrite after explicit flush
///
/// This test writes 3 stripes, explicitly flushes them, then overwrites
/// part of stripe 1. This ensures read-modify-write works when loading
/// from MetadataStore/FileStore (not just from buffer).
#[tokio::test]
async fn test_stripe_overwrite_with_flush() {
    println!("\n=== Test: Overwrite After Flush ===\n");

    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create and open file
    let attrs = service
        .create(1, "testfile.dat", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");
    let inode = attrs.ino;

    let (fh, _) = service
        .open(inode, libc::O_RDWR as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    println!("Phase 1: Writing 12MB (3 stripes)...");

    write_pattern(&service, inode, fh, 0, STRIPE_SIZE, 0xAA, client_id).await;
    write_pattern(
        &service,
        inode,
        fh,
        STRIPE_SIZE as u64,
        STRIPE_SIZE,
        0xBB,
        client_id,
    )
    .await;
    write_pattern(
        &service,
        inode,
        fh,
        2 * STRIPE_SIZE as u64,
        STRIPE_SIZE,
        0xCC,
        client_id,
    )
    .await;

    println!("\nPhase 2: Flushing file (force persist to MetadataStore/FileStore)...");
    service.flush_file(inode).await.expect("Failed to flush");

    println!("\nPhase 3: Overwriting 1.5MB in middle of Stripe 1 (offset 5MB)...");

    // This should trigger read-modify-write from MetadataStore/FileStore
    let overwrite_offset = (STRIPE_SIZE + 1 * MB) as u64; // 5MB
    write_pattern(
        &service,
        inode,
        fh,
        overwrite_offset,
        MB + 512 * 1024,
        0x99,
        client_id,
    )
    .await;

    println!("\nPhase 4: Flushing again...");
    service.flush_file(inode).await.expect("Failed to flush");

    println!("\nPhase 5: Reading and verifying...");

    let read_data = service
        .read(inode, 0, 0, 12 * MB as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read file");

    let mut all_ok = true;

    // Stripe 0: All unchanged (0xAA)
    all_ok &= verify_region(&read_data, 0, 4 * MB, 0xAA, "Stripe 0 unchanged");

    // Stripe 1: First 1MB unchanged (0xBB)
    all_ok &= verify_region(
        &read_data,
        4 * MB,
        5 * MB,
        0xBB,
        "Stripe 1 [0-1MB] unchanged",
    );

    // Stripe 1: 1.5MB modified (0x99)
    all_ok &= verify_region(
        &read_data,
        5 * MB,
        5 * MB + MB + 512 * 1024,
        0x99,
        "Stripe 1 [1-2.5MB] MODIFIED",
    );

    // Stripe 1: Last 1.5MB unchanged (0xBB)
    all_ok &= verify_region(
        &read_data,
        5 * MB + MB + 512 * 1024,
        8 * MB,
        0xBB,
        "Stripe 1 [2.5-4MB] unchanged",
    );

    // Stripe 2: All unchanged (0xCC)
    all_ok &= verify_region(&read_data, 8 * MB, 12 * MB, 0xCC, "Stripe 2 unchanged");

    service.release(fh).await.expect("Failed to release");

    assert!(all_ok, "Data verification failed");
    println!("\n✓ Test PASSED: Overwrite after flush verified correctly!");
}
