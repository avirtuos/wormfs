//! Test to reproduce data corruption with 30MB files
//!
//! This test reproduces the corruption issue found in the demo script
//! where a 30MB file written to WormFS has a different checksum when read back.

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
    let service = FileSystemServiceImplFactory::create(fs_config, metadata_store, file_store, None)
        .await
        .expect("Failed to create FileSystemService");

    service
        .initialize_root()
        .await
        .expect("Failed to initialize root");

    (service, temp_dir)
}

#[tokio::test]
async fn test_30mb_file_integrity() {
    const FILE_SIZE: usize = 30 * 1024 * 1024; // 30MB
    const STRIPE_SIZE: usize = 4 * 1024 * 1024; // 4MB

    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    println!("Creating 30MB test file...");

    // Create file
    let attrs = service
        .create(1, "testfile.dat", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");
    let inode = attrs.ino;

    // Open file for writing
    let (fh, _) = service
        .open(inode, libc::O_RDWR as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    // Generate 30MB of pseudo-random data (deterministic)
    let mut data = Vec::with_capacity(FILE_SIZE);
    for i in 0..FILE_SIZE {
        data.push(((i * 137 + 42) % 256) as u8);
    }

    println!("Writing 30MB in chunks...");

    // Write in 1MB chunks (simulating how cp might work)
    const WRITE_CHUNK: usize = 1024 * 1024; // 1MB writes
    let mut offset = 0u64;
    let mut chunk_num = 0;
    for chunk in data.chunks(WRITE_CHUNK) {
        println!(
            "  Writing chunk {} at offset {} ({} bytes)",
            chunk_num,
            offset,
            chunk.len()
        );
        let bytes_written = service
            .write(inode, fh, offset, chunk.to_vec(), 1000, 1000, client_id)
            .await
            .expect(&format!("Failed to write chunk {}", chunk_num));

        assert_eq!(
            bytes_written as usize,
            chunk.len(),
            "Chunk {} write size mismatch",
            chunk_num
        );
        println!("    ✓ Wrote {} bytes", bytes_written);
        offset += bytes_written as u64;
        chunk_num += 1;
    }

    println!("✓ Wrote {} bytes in {} chunks", FILE_SIZE, chunk_num);
    println!(
        "  (spans {} full stripes + partial)",
        FILE_SIZE / STRIPE_SIZE
    );

    // Flush the file to ensure all data is persisted
    service.flush_file(inode).await.expect("Failed to flush");

    println!("✓ Flushed file");

    // Read back the entire file
    println!("Reading back entire file...");
    let read_data = service
        .read(inode, fh, 0, FILE_SIZE as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read file");

    println!("✓ Read {} bytes", read_data.len());

    // Verify size
    assert_eq!(
        read_data.len(),
        FILE_SIZE,
        "Read size mismatch: expected {}, got {}",
        FILE_SIZE,
        read_data.len()
    );

    // Verify data integrity byte-by-byte
    println!("Verifying data integrity...");
    let mut mismatch_count = 0;
    let mut first_mismatch = None;

    for i in 0..FILE_SIZE {
        if data[i] != read_data[i] {
            mismatch_count += 1;
            if first_mismatch.is_none() {
                first_mismatch = Some((i, data[i], read_data[i]));
            }
        }
    }

    if mismatch_count > 0 {
        if let Some((offset, expected, actual)) = first_mismatch {
            let stripe_num = offset / STRIPE_SIZE;
            let stripe_offset = offset % STRIPE_SIZE;
            eprintln!("✗ DATA CORRUPTION DETECTED!");
            eprintln!("  Total mismatches: {}", mismatch_count);
            eprintln!("  First mismatch at byte offset: {}", offset);
            eprintln!("    Stripe: {}", stripe_num);
            eprintln!("    Offset in stripe: {}", stripe_offset);
            eprintln!("    Expected: 0x{:02x}", expected);
            eprintln!("    Actual:   0x{:02x}", actual);

            // Show some context
            eprintln!("\n  Context (10 bytes before and after):");
            let start = offset.saturating_sub(10);
            let end = (offset + 10).min(FILE_SIZE);
            eprint!("    Expected: ");
            for i in start..end {
                if i == offset {
                    eprint!("[{:02x}] ", data[i]);
                } else {
                    eprint!("{:02x} ", data[i]);
                }
            }
            eprintln!();
            eprint!("    Actual:   ");
            for i in start..end {
                if i == offset {
                    eprint!("[{:02x}] ", read_data[i]);
                } else {
                    eprint!("{:02x} ", read_data[i]);
                }
            }
            eprintln!();
        }
        panic!("Data corruption: {} bytes corrupted", mismatch_count);
    }

    println!("✓ All {} bytes verified - NO CORRUPTION", FILE_SIZE);
}
