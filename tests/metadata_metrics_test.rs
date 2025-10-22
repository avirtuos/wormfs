//! Test to verify MetadataStore metrics are being published

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use wormfs::file_store::{Config as FileStoreConfig, FileStore, FileStoreImpl};
use wormfs::filesystem_service::factory::FileSystemServiceImplFactory;
use wormfs::filesystem_service::FileSystemService;
use wormfs::metadata_store::{
    types::*, ClientId, Config as MetadataConfig, MetadataStore, MetadataStoreFactory,
};
use wormfs::metric_service::{MetricService, MetricServiceImpl};

#[tokio::test]
async fn test_metadata_metrics_are_published() {
    // Create temp directories
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

    // Create FileStore
    let file_store_config = FileStoreConfig {
        disk_paths: vec![chunks_dir.clone()],
        max_chunk_size: 4 * 1024 * 1024,
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

    // Create MetricService
    let metrics = Arc::new(
        MetricServiceImpl::new(wormfs::metric_service::Config::default())
            .expect("Failed to create MetricService"),
    );

    // Create FileSystemService WITH METRICS
    let fs_config = wormfs::filesystem_service::types::Config::default();
    let service = FileSystemServiceImplFactory::create(
        fs_config,
        metadata_store,
        file_store,
        Some(metrics.clone()),
    )
    .await
    .expect("Failed to create FileSystemService");

    // Initialize root directory
    service
        .initialize_root()
        .await
        .expect("Failed to initialize root directory");

    // Perform some operations to generate metrics
    let client_id = ClientId::new(1);
    let file_attr = service
        .create(1, "test.txt", 0o644, 1000, 1000, client_id)
        .await
        .expect("Failed to create file");

    // Get the file attributes (should increment metadata_store read metrics)
    let _ = service
        .getattr(file_attr.ino)
        .await
        .expect("Failed to getattr");

    // Give metrics time to be published
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Check if metrics were published
    let snapshot = metrics.snapshot();

    println!("All metrics collected:");
    for (key, _) in snapshot.metrics.iter() {
        println!("  - {}", key);
    }

    // Verify metadata_store metrics are present
    assert!(
        snapshot
            .metrics
            .iter()
            .any(|(k, _)| k.starts_with("metadata_store.")),
        "No metadata_store metrics found! Available metrics: {:?}",
        snapshot.metrics.keys().collect::<Vec<_>>()
    );

    // Check for specific expected metrics
    let expected_metrics = vec![
        "metadata_store.write.total",
        "metadata_store.write.latency",
        "metadata_store.read.total",
        "metadata_store.read.latency",
        "metadata_store.create_file.total",
        "metadata_store.create_file.latency",
    ];

    for metric_name in expected_metrics {
        assert!(
            snapshot.metrics.contains_key(metric_name),
            "Expected metric '{}' not found! Available: {:?}",
            metric_name,
            snapshot.metrics.keys().collect::<Vec<_>>()
        );
        println!("✓ Found metric: {}", metric_name);
    }

    println!("\n✓ All expected metadata_store metrics are being published!");
}
