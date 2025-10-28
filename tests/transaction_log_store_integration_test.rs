//! Integration tests for TransactionLogStore
//!
//! These tests verify the TransactionLogStore implementation works correctly
//! across various scenarios including persistence, large logs, and concurrent access.

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;
use wormfs::transaction_log_store::{
    TransactionLogConfig, TransactionLogStore, TransactionLogStoreImpl,
};

fn create_test_config(temp_dir: &TempDir) -> TransactionLogConfig {
    let db_path = temp_dir.path().join("test_log.redb");

    TransactionLogConfig {
        db_path,
        cache_size_mb: 8,
        compact_threshold_mb: 100,
        max_log_size_mb: 128,
        max_log_age_days: 7,
    }
}

#[tokio::test]
async fn test_large_log_performance() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let store = TransactionLogStoreImpl::new(config).unwrap();

    // Append 1000 entries
    let start = std::time::Instant::now();
    for i in 1..=1000 {
        let data = format!("operation {}", i).into_bytes();
        store.append(i % 10 + 1, data).await.unwrap();
    }
    let elapsed = start.elapsed();

    println!("Appended 1000 entries in {:?}", elapsed);
    assert_eq!(store.get_last_index(), 1000);

    // Verify we can retrieve all entries
    let start = std::time::Instant::now();
    let entries = store.get_entries(1, 1000).await.unwrap();
    let elapsed = start.elapsed();

    println!("Retrieved 1000 entries in {:?}", elapsed);
    assert_eq!(entries.len(), 1000);

    // Timing is informational only - performance testing belongs in benchmarks
}

#[tokio::test]
async fn test_batch_append_performance() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let store = TransactionLogStoreImpl::new(config).unwrap();

    // Create batch of 10,000 entries
    let mut entries = Vec::new();
    for i in 1..=10000 {
        let data = format!("batch operation {}", i).into_bytes();
        entries.push((i % 10 + 1, data));
    }

    // Measure batch append time
    let start = std::time::Instant::now();
    store.append_batch(entries).await.unwrap();
    let elapsed = start.elapsed();

    println!("Appended 10,000 entries in batch in {:?}", elapsed);
    println!(
        "Throughput: {} entries/sec",
        10000.0 / elapsed.as_secs_f64()
    );

    assert_eq!(store.get_last_index(), 10000);

    // Calculate throughput for informational purposes
    let throughput = 10000.0 / elapsed.as_secs_f64();
    println!("Batch append throughput: {:.0} entries/sec", throughput);

    // Timing is informational only - performance testing belongs in benchmarks
}

#[tokio::test]
async fn test_persistence_across_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let db_path = config.db_path.clone();

    // First session: create and populate log
    {
        let store = TransactionLogStoreImpl::new(config.clone()).unwrap();

        for i in 1..=100 {
            let data = format!("persistent entry {}", i).into_bytes();
            store.append(i % 5 + 1, data).await.unwrap();
        }

        assert_eq!(store.get_last_index(), 100);
    } // Store dropped, database closed

    // Wait a bit to ensure clean shutdown
    sleep(Duration::from_millis(100)).await;

    // Second session: reopen and verify
    {
        let store = TransactionLogStoreImpl::new(config.clone()).unwrap();

        assert_eq!(store.get_first_index(), 1);
        assert_eq!(store.get_last_index(), 100);

        // Verify some entries
        let entry1 = store.get_entry(1).await.unwrap();
        assert_eq!(entry1.operations, b"persistent entry 1");

        let entry50 = store.get_entry(50).await.unwrap();
        assert_eq!(entry50.operations, b"persistent entry 50");

        let entry100 = store.get_entry(100).await.unwrap();
        assert_eq!(entry100.operations, b"persistent entry 100");
    }

    assert!(db_path.exists());
}

#[tokio::test]
async fn test_trim_and_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);

    // First session: create, populate, and trim
    {
        let store = TransactionLogStoreImpl::new(config.clone()).unwrap();

        for i in 1..=100 {
            store
                .append(1, format!("entry {}", i).into_bytes())
                .await
                .unwrap();
        }

        // Trim first 50 entries
        let trimmed = store.trim(51).await.unwrap();
        assert_eq!(trimmed, 50);
        assert_eq!(store.get_first_index(), 51);
        assert_eq!(store.get_last_index(), 100);
    }

    // Second session: verify trim persisted
    {
        let store = TransactionLogStoreImpl::new(config).unwrap();
        assert_eq!(store.get_first_index(), 51);
        assert_eq!(store.get_last_index(), 100);

        // Verify entry 50 is gone
        let result = store.get_entry(50).await;
        assert!(result.is_err());

        // Verify entry 51 is still there
        let entry51 = store.get_entry(51).await.unwrap();
        assert_eq!(entry51.operations, b"entry 51");
    }
}

#[tokio::test]
async fn test_concurrent_appends() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let store = Arc::new(TransactionLogStoreImpl::new(config).unwrap());

    let mut handles = vec![];

    // Spawn 20 tasks, each appending 50 entries
    for task_id in 0..20 {
        let store_clone = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            for i in 0..50 {
                let data = format!("task {} entry {}", task_id, i).into_bytes();
                store_clone.append(1, data).await.unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify total count
    assert_eq!(store.get_last_index(), 1000);

    // Verify all entries are retrievable
    for i in 1..=1000 {
        let entry = store.get_entry(i).await.unwrap();
        assert_eq!(entry.index, i);
    }
}

#[tokio::test]
async fn test_concurrent_reads_and_writes() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let store = Arc::new(TransactionLogStoreImpl::new(config).unwrap());

    // Pre-populate with some entries
    for i in 1..=100 {
        store
            .append(1, format!("initial {}", i).into_bytes())
            .await
            .unwrap();
    }

    let mut handles = vec![];

    // Spawn writers
    for task_id in 0..5 {
        let store_clone = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            for i in 0..20 {
                let data = format!("writer {} entry {}", task_id, i).into_bytes();
                store_clone.append(2, data).await.unwrap();
            }
        });
        handles.push(handle);
    }

    // Spawn readers
    for task_id in 0..10 {
        let store_clone = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            for _ in 0..50 {
                // Read various ranges
                let _ = store_clone.get_entry(task_id % 100 + 1).await;
                let _ = store_clone.get_entries(1, 50).await;
                let _ = store_clone.get_last_entry().await;
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify final state
    assert_eq!(store.get_last_index(), 200); // 100 initial + 100 from writers
}

#[tokio::test]
async fn test_large_entry_size() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let store = TransactionLogStoreImpl::new(config).unwrap();

    // Create a 1MB entry
    let large_data = vec![0xABu8; 1024 * 1024];
    let index = store.append(1, large_data.clone()).await.unwrap();

    // Verify it can be retrieved
    let entry = store.get_entry(index).await.unwrap();
    assert_eq!(entry.operations.len(), 1024 * 1024);
    assert_eq!(entry.operations, large_data);
}

#[tokio::test]
async fn test_empty_log_operations() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let store = TransactionLogStoreImpl::new(config).unwrap();

    // Verify empty log state
    assert_eq!(store.get_first_index(), 0);
    assert_eq!(store.get_last_index(), 0);

    // Get last entry should fail
    let result = store.get_last_entry().await;
    assert!(result.is_err());

    // Get non-existent entry should fail
    let result = store.get_entry(1).await;
    assert!(result.is_err());

    // Get entries on empty log should return empty vec
    let entries = store.get_entries(1, 10).await.unwrap();
    assert_eq!(entries.len(), 0);

    // Trim on empty log should return 0
    let trimmed = store.trim(100).await.unwrap();
    assert_eq!(trimmed, 0);
}

#[tokio::test]
async fn test_statistics() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let store = TransactionLogStoreImpl::new(config).unwrap();

    // Initial stats
    let stats = store.get_stats();
    assert_eq!(stats.first_index, None);
    assert_eq!(stats.last_index, None);
    assert_eq!(stats.entry_count, 0);

    // Add entries
    for i in 1..=50 {
        store
            .append(i % 3 + 1, format!("entry {}", i).into_bytes())
            .await
            .unwrap();
    }

    let stats = store.get_stats();
    assert_eq!(stats.first_index, Some(1));
    assert_eq!(stats.last_index, Some(50));
    assert_eq!(stats.entry_count, 50);
    assert!(stats.db_size_bytes > 0);

    // Trim and check stats
    store.trim(26).await.unwrap();

    let stats = store.get_stats();
    assert_eq!(stats.first_index, Some(26));
    assert_eq!(stats.last_index, Some(50));
    assert_eq!(stats.entry_count, 25);
}

#[tokio::test]
async fn test_range_query_edge_cases() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let store = TransactionLogStoreImpl::new(config).unwrap();

    // Populate with entries 1-10
    for i in 1..=10 {
        store
            .append(1, format!("entry {}", i).into_bytes())
            .await
            .unwrap();
    }

    // Query exact range
    let entries = store.get_entries(5, 5).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].index, 5);

    // Query full range
    let entries = store.get_entries(1, 10).await.unwrap();
    assert_eq!(entries.len(), 10);

    // Query partial range
    let entries = store.get_entries(3, 7).await.unwrap();
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[0].index, 3);
    assert_eq!(entries[4].index, 7);

    // Query beyond end (should return what's available)
    let entries = store.get_entries(8, 15).await.unwrap();
    assert_eq!(entries.len(), 3); // Only 8, 9, 10 exist
    assert_eq!(entries[2].index, 10);

    // Query before start (should return empty)
    let entries = store.get_entries(100, 200).await.unwrap();
    assert_eq!(entries.len(), 0);

    // Invalid range (start > end) should error
    let result = store.get_entries(10, 5).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_checksum_integrity() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let store = TransactionLogStoreImpl::new(config).unwrap();

    // Add entries with various data sizes
    let test_data = vec![
        b"small".to_vec(),
        vec![0xFFu8; 1024],       // 1KB
        vec![0xAAu8; 10 * 1024],  // 10KB
        vec![0x55u8; 100 * 1024], // 100KB
    ];

    for (i, data) in test_data.iter().enumerate() {
        let index = store.append((i + 1) as u64, data.clone()).await.unwrap();

        // Immediately verify the entry
        let entry = store.get_entry(index).await.unwrap();
        assert_eq!(entry.operations, *data, "Data mismatch at index {}", index);
    }

    // Verify all entries after all writes
    for i in 1..=test_data.len() {
        let entry = store.get_entry(i as u64).await.unwrap();
        assert_eq!(entry.operations, test_data[i - 1]);
    }
}

#[tokio::test]
async fn test_stress_mixed_operations() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let store = Arc::new(TransactionLogStoreImpl::new(config).unwrap());

    let mut handles = vec![];

    // Writer: continuous appends
    {
        let store_clone = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            for i in 1..=500 {
                store_clone
                    .append(i % 10 + 1, format!("append {}", i).into_bytes())
                    .await
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    // Batch writer
    {
        let store_clone = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            sleep(Duration::from_millis(10)).await;
            let batch: Vec<_> = (1..=100)
                .map(|i| (1u64, format!("batch {}", i).into_bytes()))
                .collect();
            store_clone.append_batch(batch).await.unwrap();
        });
        handles.push(handle);
    }

    // Readers
    for _ in 0..5 {
        let store_clone = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            for _ in 0..100 {
                sleep(Duration::from_millis(1)).await;
                let _ = store_clone.get_last_index();
                if store_clone.get_last_index() > 0 {
                    let _ = store_clone.get_last_entry().await;
                }
            }
        });
        handles.push(handle);
    }

    // Stats reader
    {
        let store_clone = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            for _ in 0..50 {
                sleep(Duration::from_millis(2)).await;
                let _stats = store_clone.get_stats();
            }
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Final verification
    let final_index = store.get_last_index();
    assert_eq!(final_index, 600); // 500 single appends + 100 batch appends

    let stats = store.get_stats();
    assert_eq!(stats.entry_count, 600);
    assert_eq!(stats.first_index, Some(1));
    assert_eq!(stats.last_index, Some(600));
}
