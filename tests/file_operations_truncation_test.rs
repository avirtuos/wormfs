//! Integration tests for file truncation data integrity
//!
//! These tests verify that partial stripe truncation (which leaves stripe data intact in Phase 1)
//! does not expose invalid data to users. The FileSystemService's read() operation should correctly
//! clamp reads based on metadata size, preventing access to "ghost" data beyond the truncation point.

use std::path::PathBuf;
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

/// Helper to create a test FileSystemService with temporary storage.
async fn create_test_filesystem_service() -> (FileSystemServiceImpl, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("test.db");
    let chunks_dir = temp_dir.path().join("chunks");
    std::fs::create_dir(&chunks_dir).expect("Failed to create chunks dir");

    // Create MetadataStore using create_concrete to get MetadataStoreImpl
    let metadata_config = MetadataConfig {
        database_path: db_path,
        read_pool_size: 4,
        enable_wal: true,
        cache_size_mb: 64,
        enable_foreign_keys: false, // Disable for tests - we don't set up full node/disk topology
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

    // Create FileStore using the trait's new() method
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

    // Initialize disk
    let _disk_id = file_store
        .add_disk(chunks_dir.clone())
        .await
        .expect("Failed to add disk");

    let file_store = Arc::new(file_store);

    // Create FileSystemService using the factory
    let fs_config = wormfs::filesystem_service::types::Config::default();
    let service = FileSystemServiceImplFactory::create(fs_config, metadata_store, file_store, None)
        .expect("Failed to create FileSystemService");

    // Initialize root directory
    service
        .initialize_root()
        .await
        .expect("Failed to initialize root directory");

    (service, temp_dir)
}

/// Helper to write a repeating pattern to a file.
async fn write_pattern_data(
    service: &FileSystemServiceImpl,
    inode: u64,
    pattern: &[u8],
    total_size: usize,
    client_id: ClientId,
) -> Vec<u8> {
    let mut written_data = Vec::with_capacity(total_size);
    let mut offset = 0u64;

    // Write in 4MB chunks (full stripe size) to match Phase 1 stripe allocation behavior
    // Phase 1 limitation: Stripe metadata is not updated on subsequent partial writes,
    // so we write full stripes at once to ensure metadata reflects the full stripe size
    const WRITE_CHUNK_SIZE: usize = 4 * 1024 * 1024;

    while written_data.len() < total_size {
        let remaining = total_size - written_data.len();
        let chunk_size = std::cmp::min(remaining, WRITE_CHUNK_SIZE);

        // Fill chunk with repeating pattern
        let mut chunk_data = Vec::with_capacity(chunk_size);
        while chunk_data.len() < chunk_size {
            let pattern_remaining = chunk_size - chunk_data.len();
            let pattern_size = std::cmp::min(pattern_remaining, pattern.len());
            chunk_data.extend_from_slice(&pattern[..pattern_size]);
        }

        let bytes_written = service
            .write(inode, offset, chunk_data.clone(), 1000, 1000, client_id)
            .await
            .expect(&format!(
                "Failed to write at offset {}, size {}",
                offset,
                chunk_data.len()
            ));

        assert_eq!(
            bytes_written as usize,
            chunk_data.len(),
            "Partial write at offset {}",
            offset
        );

        written_data.extend_from_slice(&chunk_data);
        offset += bytes_written as u64;

        // Small delay to avoid UUID collision due to u128->u64 truncation
        // This is a workaround for Phase 1; Phase 2 should use proper ID generation
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    written_data
}

/// Helper to verify data matches expected pattern.
fn verify_data_pattern(data: &[u8], expected: &[u8], context: &str) {
    assert_eq!(
        data.len(),
        expected.len(),
        "{}: Data length mismatch",
        context
    );
    assert_eq!(data, expected, "{}: Data content mismatch", context);
}

#[tokio::test]
async fn test_partial_stripe_truncation_middle() {
    // STRIPE_SIZE = 4MB (2MB chunks × 2 data shards)
    const STRIPE_SIZE: usize = 4 * 1024 * 1024;
    const INITIAL_SIZE: usize = STRIPE_SIZE * 2; // 8MB (2 full stripes)
    const TRUNCATE_SIZE: usize = STRIPE_SIZE + (STRIPE_SIZE / 2); // 6MB (1.5 stripes)

    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create a file
    // Test file: /test_truncate_middle.txt
    let attrs = service
        .create(1, "testfile", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");
    let inode = attrs.ino;

    // Open file
    let (_fh, _attrs) = service
        .open(inode, 0, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    // Write 8MB of pattern data: "AAAABBBB..." (4-byte repeating pattern)
    let pattern = b"ABCD";
    let written_data = write_pattern_data(&service, inode, pattern, INITIAL_SIZE, client_id).await;

    println!("✓ Wrote {} bytes of pattern data", INITIAL_SIZE);

    // Truncate to 6MB (middle of second stripe)
    service
        .setattr(
            inode,
            None,
            None,
            None,
            Some(TRUNCATE_SIZE as u64),
            None,
            None,
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to truncate file");

    println!(
        "✓ Truncated file from {}MB to {}MB",
        INITIAL_SIZE / (1024 * 1024),
        TRUNCATE_SIZE / (1024 * 1024)
    );

    // Read entire file - should get exactly 6MB
    let read_data = service
        .read(inode, 0, INITIAL_SIZE as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read file");

    assert_eq!(
        read_data.len(),
        TRUNCATE_SIZE,
        "Read should return truncated size, not full stripe size"
    );

    // Verify the data matches original pattern up to truncation point
    verify_data_pattern(
        &read_data,
        &written_data[..TRUNCATE_SIZE],
        "After truncation to middle of stripe",
    );

    println!(
        "✓ Read returned exactly {} bytes (truncated size)",
        TRUNCATE_SIZE
    );

    // Read from offset beyond truncation point - should return empty
    let beyond_read = service
        .read(inode, TRUNCATE_SIZE as u64, 1024, 1000, 1000, client_id)
        .await
        .expect("Failed to read beyond truncation");

    assert!(
        beyond_read.is_empty(),
        "Reading beyond truncation point should return empty data"
    );

    println!("✓ Reading beyond truncation point returns empty data");

    // Read from offset within truncated region - should work normally
    let mid_offset = TRUNCATE_SIZE - 1024;
    let mid_read = service
        .read(inode, mid_offset as u64, 1024, 1000, 1000, client_id)
        .await
        .expect("Failed to read from middle");

    assert_eq!(mid_read.len(), 1024, "Should read full 1KB from middle");
    verify_data_pattern(
        &mid_read,
        &written_data[mid_offset..mid_offset + 1024],
        "Reading from middle of truncated file",
    );

    println!("✓ Reading from within truncated region works correctly");
    println!("✅ test_partial_stripe_truncation_middle PASSED");
}

#[tokio::test]
async fn test_partial_stripe_truncation_with_rewrite() {
    const STRIPE_SIZE: usize = 4 * 1024 * 1024;
    const INITIAL_SIZE: usize = STRIPE_SIZE * 2; // 8MB
    const TRUNCATE_SIZE: usize = STRIPE_SIZE + (STRIPE_SIZE / 2); // 6MB
    const REWRITE_SIZE: usize = STRIPE_SIZE / 4; // 1MB

    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Test file: /test_truncate_rewrite.txt
    let attrs = service
        .create(1, "testfile", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");
    let inode = attrs.ino;

    let (_fh, _attrs) = service
        .open(inode, 0, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    // Write 8MB of pattern 'A'
    let pattern_a = b"AAAA";
    let written_a = write_pattern_data(&service, inode, pattern_a, INITIAL_SIZE, client_id).await;

    println!("✓ Wrote {} bytes of pattern 'A'", INITIAL_SIZE);

    // Truncate to 6MB
    service
        .setattr(
            inode,
            None,
            None,
            None,
            Some(TRUNCATE_SIZE as u64),
            None,
            None,
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to truncate");

    println!("✓ Truncated to {} bytes", TRUNCATE_SIZE);

    // Rewrite 1MB starting at 5MB with pattern 'B'
    let rewrite_offset = 5 * 1024 * 1024;
    let pattern_b = b"BBBB";
    let mut rewrite_data = Vec::with_capacity(REWRITE_SIZE);
    while rewrite_data.len() < REWRITE_SIZE {
        let remaining = REWRITE_SIZE - rewrite_data.len();
        let chunk_size = std::cmp::min(remaining, pattern_b.len());
        rewrite_data.extend_from_slice(&pattern_b[..chunk_size]);
    }

    let written = service
        .write(
            inode,
            rewrite_offset,
            rewrite_data.clone(),
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to rewrite");

    assert_eq!(written as usize, REWRITE_SIZE, "Should write full 1MB");

    println!("✓ Rewrote 1MB at offset 5MB with pattern 'B'");

    // Read entire file - should get 6MB with rewritten section
    let read_data = service
        .read(inode, 0, INITIAL_SIZE as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read");

    assert_eq!(read_data.len(), TRUNCATE_SIZE, "Should read truncated size");

    // Verify first 5MB is still pattern 'A'
    verify_data_pattern(
        &read_data[..(rewrite_offset as usize)],
        &written_a[..(rewrite_offset as usize)],
        "First 5MB should be pattern 'A'",
    );

    // Verify 5MB-6MB is pattern 'B'
    verify_data_pattern(
        &read_data[(rewrite_offset as usize)..],
        &rewrite_data,
        "Last 1MB should be pattern 'B'",
    );

    println!("✓ File contains correct data: 5MB of 'A' + 1MB of 'B'");

    // Verify reading beyond 6MB still returns nothing
    let beyond = service
        .read(inode, TRUNCATE_SIZE as u64, 1024, 1000, 1000, client_id)
        .await
        .expect("Failed to read beyond");

    assert!(beyond.is_empty(), "Should not read beyond 6MB");

    println!("✓ Reading beyond truncation point still returns empty");
    println!("✅ test_partial_stripe_truncation_with_rewrite PASSED");
}

#[tokio::test]
async fn test_stripe_boundary_truncation() {
    const STRIPE_SIZE: usize = 4 * 1024 * 1024;
    const INITIAL_SIZE: usize = STRIPE_SIZE * 3; // 12MB (3 full stripes)
    const TRUNCATE_SIZE: usize = STRIPE_SIZE * 2; // 8MB (exactly 2 stripes)

    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Test file: /test_truncate_boundary.txt
    let attrs = service
        .create(1, "testfile", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");
    let inode = attrs.ino;

    let (_fh, _attrs) = service
        .open(inode, 0, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    // Write 12MB with unique pattern per stripe
    let pattern = b"STRIPE";
    let written_data = write_pattern_data(&service, inode, pattern, INITIAL_SIZE, client_id).await;

    println!("✓ Wrote {} bytes (3 full stripes)", INITIAL_SIZE);

    // Truncate to exact stripe boundary (8MB = 2 stripes)
    service
        .setattr(
            inode,
            None,
            None,
            None,
            Some(TRUNCATE_SIZE as u64),
            None,
            None,
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to truncate");

    println!("✓ Truncated to exact stripe boundary (2 stripes = 8MB)");

    // Read entire file
    let read_data = service
        .read(inode, 0, INITIAL_SIZE as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read");

    assert_eq!(
        read_data.len(),
        TRUNCATE_SIZE,
        "Should read exactly 2 stripes"
    );

    verify_data_pattern(
        &read_data,
        &written_data[..TRUNCATE_SIZE],
        "After truncation to stripe boundary",
    );

    println!("✓ Read exactly 2 stripes worth of data");

    // Verify third stripe is not accessible
    let third_stripe_offset = TRUNCATE_SIZE;
    let beyond = service
        .read(
            inode,
            third_stripe_offset as u64,
            STRIPE_SIZE as u32,
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to read third stripe");

    assert!(beyond.is_empty(), "Third stripe should not be accessible");

    println!("✓ Third stripe is not accessible after boundary truncation");
    println!("✅ test_stripe_boundary_truncation PASSED");
}

#[tokio::test]
async fn test_multiple_truncations() {
    const STRIPE_SIZE: usize = 4 * 1024 * 1024;
    const INITIAL_SIZE: usize = STRIPE_SIZE * 3; // 12MB

    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Test file: /test_multiple_truncations.txt
    let attrs = service
        .create(1, "testfile", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");
    let inode = attrs.ino;

    let (_fh, _attrs) = service
        .open(inode, 0, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    // Write 12MB
    let pattern = b"DATA";
    let written_data = write_pattern_data(&service, inode, pattern, INITIAL_SIZE, client_id).await;

    println!("✓ Wrote {} bytes (3 stripes)", INITIAL_SIZE);

    // First truncation: 12MB -> 10MB (middle of stripe 3)
    let truncate1: usize = 10 * 1024 * 1024;
    service
        .setattr(
            inode,
            None,
            None,
            None,
            Some(truncate1 as u64),
            None,
            None,
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to truncate to 10MB");

    let read1 = service
        .read(inode, 0, INITIAL_SIZE as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read after first truncation");

    assert_eq!(read1.len(), truncate1, "Should read 10MB");
    verify_data_pattern(&read1, &written_data[..truncate1], "After first truncation");

    println!("✓ First truncation: 12MB -> 10MB verified");

    // Second truncation: 10MB -> 6MB (middle of stripe 2)
    let truncate2: usize = 6 * 1024 * 1024;
    service
        .setattr(
            inode,
            None,
            None,
            None,
            Some(truncate2 as u64),
            None,
            None,
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to truncate to 6MB");

    let read2 = service
        .read(inode, 0, INITIAL_SIZE as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read after second truncation");

    assert_eq!(read2.len(), truncate2, "Should read 6MB");
    verify_data_pattern(
        &read2,
        &written_data[..truncate2],
        "After second truncation",
    );

    println!("✓ Second truncation: 10MB -> 6MB verified");

    // Third truncation: 6MB -> 2MB (middle of stripe 1)
    let truncate3: usize = 2 * 1024 * 1024;
    service
        .setattr(
            inode,
            None,
            None,
            None,
            Some(truncate3 as u64),
            None,
            None,
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to truncate to 2MB");

    let read3 = service
        .read(inode, 0, INITIAL_SIZE as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read after third truncation");

    assert_eq!(read3.len(), truncate3, "Should read 2MB");
    verify_data_pattern(&read3, &written_data[..truncate3], "After third truncation");

    println!("✓ Third truncation: 6MB -> 2MB verified");

    // Verify no data accessible beyond 2MB
    let beyond = service
        .read(inode, truncate3 as u64, 1024, 1000, 1000, client_id)
        .await
        .expect("Failed to read beyond");

    assert!(beyond.is_empty(), "No data beyond final truncation");

    println!("✓ No data accessible beyond final truncation point");
    println!("✅ test_multiple_truncations PASSED");
}

#[tokio::test]
async fn test_truncate_grow_does_not_expose_old_data() {
    const STRIPE_SIZE: usize = 4 * 1024 * 1024;
    const INITIAL_SIZE: usize = STRIPE_SIZE * 2; // 8MB
    const TRUNCATE_DOWN: usize = STRIPE_SIZE; // 4MB
    const TRUNCATE_UP: usize = STRIPE_SIZE + (STRIPE_SIZE / 2); // 6MB

    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Test file: /test_truncate_grow.txt
    let attrs = service
        .create(1, "testfile", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");
    let inode = attrs.ino;

    let (_fh, _attrs) = service
        .open(inode, 0, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    // Write 8MB of pattern 'X'
    let pattern_x = b"XXXX";
    let _written_x = write_pattern_data(&service, inode, pattern_x, INITIAL_SIZE, client_id).await;

    println!("✓ Wrote {} bytes of pattern 'X'", INITIAL_SIZE);

    // Truncate down to 4MB
    service
        .setattr(
            inode,
            None,
            None,
            None,
            Some(TRUNCATE_DOWN as u64),
            None,
            None,
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to truncate down");

    println!("✓ Truncated down from 8MB to 4MB");

    // Truncate up to 6MB (this should extend the file with zeros, not expose old data)
    service
        .setattr(
            inode,
            None,
            None,
            None,
            Some(TRUNCATE_UP as u64),
            None,
            None,
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to truncate up");

    println!("✓ Truncated up from 4MB to 6MB");

    // Read the entire file
    let read_data = service
        .read(inode, 0, TRUNCATE_UP as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read file");

    assert_eq!(read_data.len(), TRUNCATE_UP, "Should read 6MB");

    // First 4MB should be pattern 'X'
    for (i, chunk) in read_data[..TRUNCATE_DOWN].chunks(4).enumerate() {
        assert_eq!(
            chunk, pattern_x,
            "First 4MB should be pattern 'X' at chunk {}",
            i
        );
    }

    println!("✓ First 4MB contains original pattern 'X'");

    // Last 2MB should be zeros (or at minimum, NOT the old pattern 'X')
    let extended_region = &read_data[TRUNCATE_DOWN..];

    // Check that the extended region is all zeros (POSIX behavior)
    for (i, &byte) in extended_region.iter().enumerate() {
        assert_eq!(
            byte,
            0,
            "Extended region should be zeros, found non-zero byte at offset {}",
            TRUNCATE_DOWN + i
        );
    }

    println!("✓ Extended region (4MB-6MB) contains zeros, not old data");
    println!("✅ test_truncate_grow_does_not_expose_old_data PASSED");
}
#[test]
fn test_stripe_id_uniqueness() {
    use std::collections::HashSet;
    use wormfs::file_store::types::StripeId;

    let mut seen = HashSet::new();
    for i in 0..1000 {
        let id = StripeId::generate();
        if !seen.insert(*id.as_uuid()) {
            panic!("Collision at iteration {}! ID: {}", i, id.as_uuid());
        }
    }
    println!("✓ Generated 1000 unique StripeIDs");
}
