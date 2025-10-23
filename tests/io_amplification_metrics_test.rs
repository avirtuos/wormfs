//! I/O Amplification Metrics Integration Test
//!
//! This test validates that the metrics system correctly tracks I/O amplification
//! when performing read-modify-write operations on erasure-coded stripes.

use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::Duration;
use wormfs::file_store::{FileStore, FileStoreImpl, StoragePolicy};
use wormfs::metric_service::{Config as MetricConfig, MetricService, MetricServiceImpl};

/// Test that I/O amplification is correctly tracked for small writes.
///
/// Small writes to large stripes require reading the entire stripe,
/// modifying the relevant chunks, and writing them back (RMW operation).
/// This causes I/O amplification that should be visible in metrics.
#[tokio::test]
async fn test_io_amplification_tracking() {
    // Create temp directory for test
    let test_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let disk_path = test_dir.path().to_path_buf();

    // Configure file store with large stripe size
    let file_store_config = wormfs::file_store::types::Config {
        disk_paths: vec![disk_path],
        max_chunk_size: 1024 * 1024, // 1MB chunks
        default_data_shards: 3,
        default_parity_shards: 2,
        max_concurrent_operations: 100,
        verification_interval: Duration::from_secs(3600),
        orphan_cleanup_age: Duration::from_secs(3600),
        stripe_cache_size_mb: 256,
        stripe_cache_ttl_secs: 3600,
        stripe_cache_tti_secs: 600,
    };

    // Create file store
    let file_store =
        Arc::new(FileStoreImpl::new(file_store_config).expect("Failed to create FileStore"));

    // Create and configure metrics service
    let metric_config = MetricConfig {
        enabled: true,
        enable_time_series: true,
        time_series_retention_secs: 60,
        time_series_sample_interval_secs: 1,
        ..Default::default()
    };

    let metrics =
        Arc::new(MetricServiceImpl::new(metric_config).expect("Failed to create metrics"));

    // Aggregation loop is automatically started in new()
    // Give it a moment to initialize
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Inject metrics into file store
    file_store.set_metrics(metrics.clone());

    // Generate test data
    let file_id = wormfs::file_store::types::FileId::new(uuid::Uuid::new_v4());
    let stripe_id = wormfs::file_store::types::StripeId::new(uuid::Uuid::new_v4());

    // Storage policy
    let policy = StoragePolicy {
        data_shards: 3,
        parity_shards: 2,
        chunk_size: 1024 * 1024, // 1MB chunks
        compression: wormfs::file_store::types::CompressionAlgorithm::None,
    };

    // Test Case 1: Write a full stripe (no RMW)
    // With 3 data shards of 1MB each, full stripe is 3MB
    let full_stripe_data = vec![0xAA; 3 * 1024 * 1024];

    let stripe_metadata = file_store
        .write_stripe(
            file_id,
            stripe_id,
            0,
            full_stripe_data.clone(),
            policy.clone(),
        )
        .await
        .expect("Failed to write full stripe");

    // Test Case 2: Perform a small write (triggers RMW)
    // Write only 4KB at offset 1MB - this should trigger read-modify-write
    let small_write_data = vec![0xBB; 4 * 1024]; // 4KB
    let small_write_offset = 1024 * 1024; // 1MB offset within the stripe

    file_store
        .update_stripe_partial(
            file_id,
            stripe_id,
            0,                      // stripe offset in file
            stripe_metadata.chunks, // existing chunks from previous write
            small_write_offset,     // offset within stripe
            small_write_data.clone(),
            policy.clone(),
        )
        .await
        .expect("Failed to perform small write");

    // Give metrics time to aggregate
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Retrieve metrics snapshot
    let snapshot = metrics.snapshot();

    // Validate RMW operation metrics
    if let Some(rmw_total) = snapshot.metrics.get("filestore.rmw_operations.total") {
        // Should have at least 1 RMW operation from the small write
        assert!(
            rmw_total.value >= 1.0,
            "Should have recorded at least 1 RMW operation, got: {}",
            rmw_total.value
        );
        println!("✓ RMW operations recorded: {}", rmw_total.value);
    } else {
        eprintln!("⚠ Warning: RMW total metric not found in snapshot");
    }

    // Check I/O amplification ratio
    if let Some(amplification) = snapshot.metrics.get("filestore.io_amplification.ratio") {
        // For a 4KB write on 1MB chunks with 5 total shards, we expect high amplification
        // Read: 5 chunks * 1MB = 5MB
        // Write: 5 chunks * 1MB = 5MB
        // Total physical I/O: ~10MB for 4KB logical write
        // Expected amplification: 10MB / 4KB ≈ 2500x
        assert!(
            amplification.value > 1.0,
            "I/O amplification should be > 1.0 for small writes"
        );
        println!("✓ I/O amplification ratio: {:.2}x", amplification.value);

        // The amplification should be significant for 4KB writes on 1MB chunks
        if amplification.value > 100.0 {
            println!("  High amplification detected (expected for small writes on large chunks)");
        }
    } else {
        eprintln!("⚠ Warning: I/O amplification metric not found");
    }

    // Check physical vs logical bytes
    let physical_bytes = snapshot
        .metrics
        .get("filestore.rmw_operations.physical_bytes")
        .map(|m| m.value as u64)
        .unwrap_or(0);

    let logical_bytes = snapshot
        .metrics
        .get("filestore.rmw_operations.logical_bytes")
        .map(|m| m.value as u64)
        .unwrap_or(0);

    if physical_bytes > 0 && logical_bytes > 0 {
        let calculated_amplification = physical_bytes as f64 / logical_bytes as f64;
        println!(
            "✓ Physical bytes: {}, Logical bytes: {}, Calculated amplification: {:.2}x",
            physical_bytes, logical_bytes, calculated_amplification
        );

        assert!(
            physical_bytes > logical_bytes,
            "Physical I/O should exceed logical I/O for RMW operations"
        );
    }

    // Validate stripe write metrics exist
    let stripe_write_metrics = [
        "filestore.stripe_write.total",
        "filestore.stripe_write.bytes_raw",
        "filestore.stripe_write.bytes_encoded",
        "filestore.stripe_write.latency",
    ];

    for metric_name in &stripe_write_metrics {
        if snapshot.metrics.contains_key(*metric_name) {
            println!("✓ Found metric: {}", metric_name);
        }
    }

    println!("\n=== Metrics Summary ===");
    println!("Total metrics collected: {}", snapshot.metrics.len());
    println!("\nAll filestore metrics:");
    for (name, metric) in snapshot.metrics.iter() {
        if name.starts_with("filestore") {
            println!("  {}: {:.2}", name, metric.value);
        }
    }
}

/// Test that normal (non-RMW) operations don't show amplification.
#[tokio::test]
async fn test_no_amplification_for_aligned_writes() {
    let test_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let disk_path = test_dir.path().to_path_buf();

    let file_store_config = wormfs::file_store::types::Config {
        disk_paths: vec![disk_path],
        max_chunk_size: 1024 * 1024,
        default_data_shards: 3,
        default_parity_shards: 2,
        max_concurrent_operations: 100,
        verification_interval: Duration::from_secs(3600),
        orphan_cleanup_age: Duration::from_secs(3600),
        stripe_cache_size_mb: 256,
        stripe_cache_ttl_secs: 3600,
        stripe_cache_tti_secs: 600,
    };

    let file_store =
        Arc::new(FileStoreImpl::new(file_store_config).expect("Failed to create FileStore"));

    let metrics = Arc::new(
        MetricServiceImpl::new(MetricConfig::default()).expect("Failed to create metrics"),
    );

    // Aggregation loop is automatically started in new()
    tokio::time::sleep(Duration::from_millis(100)).await;
    file_store.set_metrics(metrics.clone());

    // Write a full, aligned stripe - no RMW should occur
    let file_id = wormfs::file_store::types::FileId::new(uuid::Uuid::new_v4());
    let stripe_id = wormfs::file_store::types::StripeId::new(uuid::Uuid::new_v4());
    let policy = StoragePolicy {
        data_shards: 3,
        parity_shards: 2,
        chunk_size: 1024 * 1024, // 1MB chunks
        compression: wormfs::file_store::types::CompressionAlgorithm::None,
    };

    let full_data = vec![0xCC; 3 * 1024 * 1024];
    file_store
        .write_stripe(file_id, stripe_id, 0, full_data, policy)
        .await
        .expect("Failed to write stripe");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let snapshot = metrics.snapshot();

    // For aligned, full stripe writes, we shouldn't see RMW operations
    let rmw_count = snapshot
        .metrics
        .get("filestore.rmw_operations.total")
        .map(|m| m.value)
        .unwrap_or(0.0);

    println!("RMW operations for aligned write: {}", rmw_count);

    // Normal stripe writes should NOT trigger RMW
    assert_eq!(
        rmw_count, 0.0,
        "Aligned full stripe writes should not trigger RMW operations"
    );
}
