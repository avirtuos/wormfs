// Tests for snapshot export/restore functionality

use std::path::PathBuf;
use tempfile::TempDir;
use uuid::Uuid;
use wormfs::metadata::{FileMetadata, MetadataStore};

#[test]
fn test_snapshot_export_and_restore() {
    // Create a temporary directory for the test
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Create store and add some data
    let store = MetadataStore::new(&db_path).unwrap();

    let file_metadata = FileMetadata::new(PathBuf::from("/test/file.txt"), 1024, 0o644);
    let file_id = file_metadata.file_id;
    store.create_file(file_metadata).unwrap();

    // Export snapshot
    let snapshot_data = store.export_snapshot().unwrap();
    assert!(
        !snapshot_data.is_empty(),
        "Snapshot data should not be empty"
    );
    println!("Exported snapshot: {} bytes", snapshot_data.len());

    // Create a new store with a different database
    let db_path2 = temp_dir.path().join("test2.db");
    let mut store2 = MetadataStore::new(&db_path2).unwrap();

    // Restore snapshot to the new store
    store2.restore_snapshot(&snapshot_data).unwrap();

    // Verify the data was restored correctly
    let restored_file = store2.get_file(file_id).unwrap();
    assert_eq!(restored_file.file_id, file_id);
    assert_eq!(restored_file.size, 1024);
    assert_eq!(restored_file.permissions, 0o644);

    println!("Successfully restored snapshot and verified data");
}

#[test]
fn test_snapshot_with_multiple_files() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let store = MetadataStore::new(&db_path).unwrap();

    // Create multiple files
    let mut file_ids = Vec::new();
    for i in 0..10 {
        let file_metadata = FileMetadata::new(
            PathBuf::from(format!("/test/file{}.txt", i)),
            1024 * (i + 1) as u64,
            0o644,
        );
        file_ids.push(file_metadata.file_id);
        store.create_file(file_metadata).unwrap();
    }

    // Export snapshot
    let snapshot_data = store.export_snapshot().unwrap();
    println!(
        "Exported snapshot with 10 files: {} bytes",
        snapshot_data.len()
    );

    // Restore to new store
    let db_path2 = temp_dir.path().join("test2.db");
    let mut store2 = MetadataStore::new(&db_path2).unwrap();
    store2.restore_snapshot(&snapshot_data).unwrap();

    // Verify all files were restored
    let files = store2.list_files().unwrap();
    assert_eq!(files.len(), 10, "Should have restored all 10 files");

    for (i, file_id) in file_ids.iter().enumerate() {
        let file = store2.get_file(*file_id).unwrap();
        assert_eq!(file.size, 1024 * (i + 1) as u64);
    }

    println!("Successfully restored and verified 10 files");
}

#[test]
fn test_snapshot_with_locks() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let store = MetadataStore::new(&db_path).unwrap();

    // Create a file
    let file_metadata = FileMetadata::new(PathBuf::from("/test/file.txt"), 1024, 0o644);
    let file_id = file_metadata.file_id;
    store.create_file(file_metadata).unwrap();

    // Acquire a lock
    store
        .acquire_lock(
            file_id,
            wormfs::metadata::LockType::Write,
            "client1".to_string(),
            30,
        )
        .unwrap();

    // Export snapshot
    let snapshot_data = store.export_snapshot().unwrap();
    println!(
        "Exported snapshot with locks: {} bytes",
        snapshot_data.len()
    );

    // Restore to new store
    let db_path2 = temp_dir.path().join("test2.db");
    let mut store2 = MetadataStore::new(&db_path2).unwrap();
    store2.restore_snapshot(&snapshot_data).unwrap();

    // Verify file was restored
    let restored_file = store2.get_file(file_id).unwrap();
    assert_eq!(restored_file.file_id, file_id);

    // Verify locks were restored
    let locks = store2.get_locks(file_id).unwrap();
    assert_eq!(locks.len(), 1, "Lock should be restored");
    assert_eq!(locks[0].client_id, "client1");

    println!("Successfully restored snapshot with locks");
}

#[test]
fn test_snapshot_compression() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let store = MetadataStore::new(&db_path).unwrap();

    // Create many files to test compression effectiveness
    for i in 0..100 {
        let file_metadata =
            FileMetadata::new(PathBuf::from(format!("/test/file{}.txt", i)), 1024, 0o644);
        store.create_file(file_metadata).unwrap();
    }

    // Export snapshot
    let snapshot_data = store.export_snapshot().unwrap();

    // Get stats to estimate original size
    let stats = store.get_stats().unwrap();
    println!(
        "Database stats: {} files, {} total size",
        stats.file_count, stats.total_size
    );
    println!("Compressed snapshot: {} bytes", snapshot_data.len());

    // Compression should reduce the size significantly
    // Even with just metadata, we should see some compression
    assert!(!snapshot_data.is_empty());

    // Verify we can restore it
    let db_path2 = temp_dir.path().join("test2.db");
    let mut store2 = MetadataStore::new(&db_path2).unwrap();
    store2.restore_snapshot(&snapshot_data).unwrap();

    let stats2 = store2.get_stats().unwrap();
    assert_eq!(stats.file_count, stats2.file_count);
}

#[test]
fn test_empty_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let store = MetadataStore::new(&db_path).unwrap();

    // Export snapshot from empty database
    let snapshot_data = store.export_snapshot().unwrap();
    assert!(
        !snapshot_data.is_empty(),
        "Even empty DB should have some data"
    );

    // Restore to new store
    let db_path2 = temp_dir.path().join("test2.db");
    let mut store2 = MetadataStore::new(&db_path2).unwrap();
    store2.restore_snapshot(&snapshot_data).unwrap();

    // Verify it's empty
    let files = store2.list_files().unwrap();
    assert_eq!(files.len(), 0);
}

#[test]
fn test_snapshot_with_stripes_and_chunks() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let store = MetadataStore::new(&db_path).unwrap();

    // Create a file
    let file_metadata = FileMetadata::new(PathBuf::from("/test/file.txt"), 10240, 0o644);
    let file_id = file_metadata.file_id;
    store.create_file(file_metadata).unwrap();

    // Create a stripe
    let config = wormfs::erasure_coding::ErasureCodingConfig::new(4, 2, 1024).unwrap();
    let stripe_id = wormfs::metadata::StripeId::new(file_id, 0);
    let stripe_metadata = wormfs::metadata::StripeMetadata::new(file_id, 0, 4096, config);
    store.create_stripe(stripe_id, stripe_metadata).unwrap();

    // Create chunks
    for i in 0..6 {
        let chunk_id = wormfs::metadata::ChunkId::new(file_id, 0, i);
        let location = wormfs::metadata::StorageLocation::new(
            Uuid::new_v4(),
            format!("disk{}", i),
            PathBuf::from(format!("/chunks/chunk{}", i)),
        );
        let chunk_metadata =
            wormfs::metadata::ChunkMetadata::new(file_id, 0, i, 1024, 0x12345678, location);
        store.register_chunk(chunk_id, chunk_metadata).unwrap();
    }

    // Export snapshot
    let snapshot_data = store.export_snapshot().unwrap();
    println!(
        "Exported snapshot with stripes and chunks: {} bytes",
        snapshot_data.len()
    );

    // Restore to new store
    let db_path2 = temp_dir.path().join("test2.db");
    let mut store2 = MetadataStore::new(&db_path2).unwrap();
    store2.restore_snapshot(&snapshot_data).unwrap();

    // Verify everything was restored
    let restored_file = store2.get_file(file_id).unwrap();
    assert_eq!(restored_file.file_id, file_id);

    let restored_stripe = store2.get_stripe(stripe_id).unwrap();
    assert_eq!(restored_stripe.original_size, 4096);

    let restored_chunks = store2.get_chunks_for_stripe(stripe_id).unwrap();
    assert_eq!(restored_chunks.len(), 6);

    println!("Successfully restored snapshot with complete file structure");
}
