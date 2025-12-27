//! Integration tests for multi-stripe metadata persistence and lookup
//!
//! These tests verify that stripe metadata is correctly stored, retrieved,
//! and that offsets are properly maintained across multiple stripes.

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

    let fs_config = wormfs::filesystem_service::types::Config::default();
    let service =
        FileSystemServiceImplFactory::create(fs_config, metadata_store, file_store, None, None)
            .await
            .expect("Failed to create FileSystemService");

    service
        .initialize_root()
        .await
        .expect("Failed to initialize root");

    (service, temp_dir)
}

#[tokio::test]
async fn test_multi_stripe_metadata_persistence() {
    const STRIPE_SIZE: usize = 4 * 1024 * 1024; // 4MB
    const NUM_STRIPES: usize = 3;
    const TOTAL_SIZE: usize = STRIPE_SIZE * NUM_STRIPES; // 12MB

    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create a file
    let attrs = service
        .create(1, "testfile", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");
    let inode = attrs.ino;

    // Open file
    let (fh, _) = service
        .open(inode, libc::O_RDWR as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    // Write 12MB in 4MB chunks (3 stripes)
    let mut data = Vec::with_capacity(TOTAL_SIZE);
    for i in 0..NUM_STRIPES {
        let stripe_data = vec![(i as u8); STRIPE_SIZE];
        data.extend_from_slice(&stripe_data);
    }

    let mut offset = 0u64;
    for chunk in data.chunks(STRIPE_SIZE) {
        let bytes_written = service
            .write(inode, fh, offset, chunk.to_vec(), 1000, 1000, client_id)
            .await
            .expect("Failed to write");

        assert_eq!(bytes_written as usize, chunk.len());
        offset += bytes_written as u64;
    }

    println!("✓ Wrote {} bytes in {} stripes", TOTAL_SIZE, NUM_STRIPES);

    // Read back and verify (use file handle to read from BufferedFileHandle)
    let read_data = service
        .read(inode, fh, 0, TOTAL_SIZE as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read");

    assert_eq!(read_data.len(), TOTAL_SIZE, "Should read all written data");

    // Verify each stripe has correct data
    for i in 0..NUM_STRIPES {
        let stripe_start = i * STRIPE_SIZE;
        let stripe_end = stripe_start + STRIPE_SIZE;
        let stripe_data = &read_data[stripe_start..stripe_end];

        assert!(
            stripe_data.iter().all(|&b| b == i as u8),
            "Stripe {} data mismatch",
            i
        );
    }

    println!("✓ All {} stripes read correctly", NUM_STRIPES);
}

#[tokio::test]
async fn test_stripe_lookup_at_boundaries() {
    const STRIPE_SIZE: usize = 4 * 1024 * 1024; // 4MB

    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create and open file
    let attrs = service
        .create(1, "testfile", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");
    let inode = attrs.ino;

    let (fh, _) = service
        .open(inode, libc::O_RDWR as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    // Write 3 stripes with distinct patterns
    for i in 0..3 {
        let offset = (i * STRIPE_SIZE) as u64;
        let data = vec![(i as u8 + 10); STRIPE_SIZE]; // Patterns: 10, 11, 12

        service
            .write(inode, fh, offset, data, 1000, 1000, client_id)
            .await
            .expect("Failed to write");
    }

    println!("✓ Wrote 3 stripes");

    // Test reads at stripe boundaries and within stripes
    let test_cases = vec![
        (0, 1024, 10u8, "Start of stripe 0"),
        (STRIPE_SIZE as u64, 1024, 11u8, "Start of stripe 1"),
        (2 * STRIPE_SIZE as u64, 1024, 12u8, "Start of stripe 2"),
        (2 * 1024 * 1024, 1024, 10u8, "Middle of stripe 0"),
        (
            STRIPE_SIZE as u64 + 2 * 1024 * 1024,
            1024,
            11u8,
            "Middle of stripe 1",
        ),
    ];

    for (offset, size, expected_byte, description) in test_cases {
        let data = service
            .read(inode, fh, offset, size, 1000, 1000, client_id)
            .await
            .expect(&format!("Failed to read at {}", description));

        assert_eq!(data.len(), size as usize, "{}: wrong size", description);
        assert!(
            data.iter().all(|&b| b == expected_byte),
            "{}: expected all bytes to be {}, got {:?}",
            description,
            expected_byte,
            &data[..10.min(data.len())]
        );
    }

    println!("✓ All stripe boundary lookups correct");
}

#[tokio::test]
async fn test_stripe_metadata_after_truncation() {
    const STRIPE_SIZE: usize = 4 * 1024 * 1024; // 4MB

    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    // Create and open file
    let attrs = service
        .create(1, "testfile", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");
    let inode = attrs.ino;

    let (fh, _) = service
        .open(inode, libc::O_RDWR as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    // Write 3 full stripes (12MB)
    for i in 0..3 {
        let offset = (i * STRIPE_SIZE) as u64;
        let data = vec![(i as u8 + 20); STRIPE_SIZE];

        service
            .write(inode, fh, offset, data, 1000, 1000, client_id)
            .await
            .expect("Failed to write");
    }

    // Truncate to 6MB (1.5 stripes)
    // Pass file handle so BufferedFileHandle is informed of the truncation
    service
        .setattr(
            inode,
            Some(fh), // file_handle - ensures BufferedFileHandle sees the size change
            None,
            None,
            None,
            Some(6 * 1024 * 1024),
            None,
            None,
            1000,
            1000,
            client_id,
        )
        .await
        .expect("Failed to truncate");

    // Read entire file - should get 6MB
    let data = service
        .read(inode, fh, 0, 12 * 1024 * 1024, 1000, 1000, client_id)
        .await
        .expect("Failed to read");

    assert_eq!(
        data.len(),
        6 * 1024 * 1024,
        "Should read 6MB after truncation"
    );

    // Verify stripe 0 intact
    assert!(data[..STRIPE_SIZE].iter().all(|&b| b == 20));

    // Verify first half of stripe 1 intact
    assert!(data[STRIPE_SIZE..2 * 1024 * 1024 + STRIPE_SIZE]
        .iter()
        .all(|&b| b == 21));

    println!("✓ Stripe metadata correct after truncation");
}

#[tokio::test]
async fn test_large_file_stripe_consistency() {
    const STRIPE_SIZE: usize = 4 * 1024 * 1024; // 4MB
    const NUM_STRIPES: usize = 5;
    const TOTAL_SIZE: usize = STRIPE_SIZE * NUM_STRIPES; // 20MB

    let (service, _temp) = create_test_filesystem_service().await;
    let client_id = ClientId::new(1);

    let attrs = service
        .create(1, "largefile", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");
    let inode = attrs.ino;

    let (fh, _) = service
        .open(inode, libc::O_RDWR as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to open file");

    // Write 20MB with unique pattern per stripe
    let mut expected_data = Vec::with_capacity(TOTAL_SIZE);
    for i in 0..NUM_STRIPES {
        let offset = (i * STRIPE_SIZE) as u64;
        let pattern = (i as u8 + 100);
        let data = vec![pattern; STRIPE_SIZE];

        service
            .write(inode, fh, offset, data.clone(), 1000, 1000, client_id)
            .await
            .expect("Failed to write");

        expected_data.extend_from_slice(&data);
    }

    // Read entire file (use file handle to read from BufferedFileHandle)
    let read_data = service
        .read(inode, fh, 0, TOTAL_SIZE as u32, 1000, 1000, client_id)
        .await
        .expect("Failed to read full file");

    assert_eq!(read_data.len(), TOTAL_SIZE);
    assert_eq!(read_data, expected_data, "Full file data mismatch");

    // Read middle sections crossing stripe boundaries
    let middle_offset = STRIPE_SIZE as u64 + (STRIPE_SIZE / 2) as u64; // 6MB
    let middle_size = STRIPE_SIZE as u32; // Read 4MB starting at 6MB

    let middle_data = service
        .read(inode, fh, middle_offset, middle_size, 1000, 1000, client_id)
        .await
        .expect("Failed to read middle section");

    assert_eq!(middle_data.len(), STRIPE_SIZE);

    // First half should be from stripe 1 (pattern 101)
    assert!(middle_data[..STRIPE_SIZE / 2].iter().all(|&b| b == 101));
    // Second half should be from stripe 2 (pattern 102)
    assert!(middle_data[STRIPE_SIZE / 2..].iter().all(|&b| b == 102));

    println!("✓ Large file stripe consistency verified");
}
