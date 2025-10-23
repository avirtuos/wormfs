//! Integration test for incremental file writes
//!
//! This test writes a 5MB file incrementally in 100KB chunks and verifies
//! that the file size and contents are correct after reading back.
//! This helps verify that BufferedFileHandle correctly handles incremental writes.

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use wormfs::file_store::{Config as FileStoreConfig, FileStore, FileStoreImpl};
use wormfs::filesystem_service::factory::FileSystemServiceImplFactory;
use wormfs::filesystem_service::implementation::FileSystemServiceImpl;
use wormfs::filesystem_service::FileSystemService; // Import trait for methods
use wormfs::metadata_store::{
    types::*, ClientId, Config as MetadataConfig, MetadataStore, MetadataStoreFactory,
};

/// Helper to create a test FileSystemService with temporary storage and BufferedFileHandle enabled.
async fn create_test_filesystem_service() -> (FileSystemServiceImpl, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let chunks_dir = temp_dir.path().join("chunks");
    std::fs::create_dir(&chunks_dir).expect("Failed to create chunks dir");

    // Create MetadataStore
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

    // Create FileStore with 4MB stripe size
    let file_store_config = FileStoreConfig {
        disk_paths: vec![chunks_dir.clone()],
        max_chunk_size: 4 * 1024 * 1024, // 4MB stripes
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

    let file_store = Arc::new(file_store);

    // Create FileSystemService (BufferedFileHandle is always enabled)
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

#[tokio::test]
async fn test_incremental_write_5mb_file() {
    // Setup
    let (service, _temp_dir) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);
    let uid = 1000;
    let gid = 1000;

    // Create file
    let file_attr = service
        .create(1, "testfile.dat", 0o644, uid, gid, client_id)
        .await
        .expect("Failed to create file");

    let inode = file_attr.ino;

    // Open file for writing
    let (fh, _file_attr) = service
        .open(inode, 0x8002, uid, gid, client_id) // O_RDWR
        .await
        .expect("Failed to open file");

    println!("Created file with inode {} and file handle {}", inode, fh);

    // Incremental writes: 50 writes × 100KB = 5MB
    const CHUNK_SIZE: usize = 100 * 1024; // 100KB
    const NUM_CHUNKS: usize = 50;
    const TOTAL_SIZE: usize = CHUNK_SIZE * NUM_CHUNKS; // 5MB

    for chunk_idx in 0..NUM_CHUNKS {
        let offset = (chunk_idx * CHUNK_SIZE) as u64;

        // Create chunk data with deterministic pattern
        // Each byte is set to chunk_idx % 256
        let pattern_byte = (chunk_idx % 256) as u8;
        let chunk_data = vec![pattern_byte; CHUNK_SIZE];

        println!(
            "Writing chunk {}/{}: offset={}, size={}, pattern=0x{:02x}",
            chunk_idx + 1,
            NUM_CHUNKS,
            offset,
            CHUNK_SIZE,
            pattern_byte
        );

        // Write chunk
        let bytes_written = service
            .write(inode, fh, offset, chunk_data, uid, gid, client_id)
            .await
            .expect("Failed to write chunk");

        assert_eq!(
            bytes_written as usize, CHUNK_SIZE,
            "Expected to write {} bytes, wrote {}",
            CHUNK_SIZE, bytes_written
        );
    }

    println!("\nAll writes completed. Releasing file handle...");

    // Release file handle (triggers final flush)
    service
        .release(fh)
        .await
        .expect("Failed to release file handle");

    println!("File handle released.\n");

    // Verify final file size
    let final_attr = service
        .getattr(inode)
        .await
        .expect("Failed to get final file attributes");

    println!(
        "Final file size: {} bytes (expected {} bytes)",
        final_attr.size, TOTAL_SIZE
    );

    assert_eq!(
        final_attr.size, TOTAL_SIZE as u64,
        "File size mismatch! Expected {}, got {}",
        TOTAL_SIZE, final_attr.size
    );

    // Read back the entire file
    println!("\nReading file back...");

    let (fh_read, _) = service
        .open(inode, 0x8000, uid, gid, client_id) // O_RDONLY
        .await
        .expect("Failed to open file for reading");

    let read_data = service
        .read(inode, fh_read, 0, TOTAL_SIZE as u32, uid, gid, client_id)
        .await
        .expect("Failed to read file");

    println!("Read {} bytes from file", read_data.len());

    // Verify data length
    assert_eq!(
        read_data.len(),
        TOTAL_SIZE,
        "Read data length mismatch! Expected {}, got {}",
        TOTAL_SIZE,
        read_data.len()
    );

    // Verify data contents chunk by chunk
    println!("\nVerifying data contents...");
    let mut all_match = true;

    for chunk_idx in 0..NUM_CHUNKS {
        let chunk_start = chunk_idx * CHUNK_SIZE;
        let chunk_end = chunk_start + CHUNK_SIZE;
        let chunk_data = &read_data[chunk_start..chunk_end];

        let expected_pattern = (chunk_idx % 256) as u8;

        // Check if all bytes in this chunk match the expected pattern
        let chunk_matches = chunk_data.iter().all(|&b| b == expected_pattern);

        if !chunk_matches {
            eprintln!(
                "MISMATCH in chunk {}: expected pattern 0x{:02x}",
                chunk_idx, expected_pattern
            );
            // Find first mismatched byte
            for (i, &byte) in chunk_data.iter().enumerate() {
                if byte != expected_pattern {
                    eprintln!(
                        "  First mismatch at offset {} (global offset {}): expected 0x{:02x}, got 0x{:02x}",
                        i,
                        chunk_start + i,
                        expected_pattern,
                        byte
                    );
                    break;
                }
            }
            all_match = false;
        } else {
            println!(
                "Chunk {} verified: pattern 0x{:02x} ✓",
                chunk_idx, expected_pattern
            );
        }
    }

    service
        .release(fh_read)
        .await
        .expect("Failed to release read file handle");

    assert!(
        all_match,
        "Data verification failed - some chunks have incorrect patterns"
    );

    println!("\n✓ Test passed! File size and contents are correct.");
}
