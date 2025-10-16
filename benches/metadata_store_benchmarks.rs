//! Criterion benchmarks for MetadataStore
//!
//! These benchmarks measure performance against the targets specified in GitHub Issue #59:
//! - File creation: <5ms
//! - File lookup by inode: <2ms
//! - File lookup by path: <3ms
//! - Directory listing (100 files): <10ms
//!
//! Note: These benchmarks include database initialization overhead in each iteration
//! to ensure isolated, repeatable measurements. For pure operation timings without
//! initialization overhead, see tests/metadata_store_integration_test.rs which shows:
//! - File creation: 0.20ms
//! - Inode lookup: 0.03ms
//! - Path lookup: 0.03ms
//! - Directory listing (1000 files): 2.7ms

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use std::path::PathBuf;
use tempfile::TempDir;
use wormfs::metadata_store::{
    factory::MetadataStoreFactory, types::*, Config, FileId, FileMetadata, MetadataStore,
};

/// Helper to create a test MetadataStore with a temporary database.
async fn create_test_store() -> (impl MetadataStore, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("bench.db");

    let config = Config {
        database_path: db_path,
        read_pool_size: 8,
        enable_wal: true,
        cache_size_mb: 64,
        enable_foreign_keys: true,
        synchronous: SynchronousMode::Normal,
        transaction_isolation: IsolationLevel::ReadCommitted,
        enable_prepared_statements: true,
        read_pool_timeout_secs: 30,
    };

    let store = MetadataStoreFactory::create(config)
        .await
        .expect("Failed to create MetadataStore");

    store
        .initialize_schema()
        .await
        .expect("Failed to initialize schema");

    (store, temp_dir)
}

/// Helper to create test file metadata.
fn test_file_metadata() -> FileMetadata {
    FileMetadata {
        file_type: FileType::RegularFile,
        size: 1024,
        permissions: 0o644,
        uid: 1000,
        gid: 1000,
        created_at: std::time::SystemTime::now(),
        modified_at: std::time::SystemTime::now(),
        accessed_at: std::time::SystemTime::now(),
    }
}

/// Benchmark: Single file creation operation
/// Target: <5ms (actual: ~0.20ms from integration tests)
fn bench_single_file_creation(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("single_file_creation", |b| {
        b.to_async(&runtime).iter(|| async {
            let (store, _temp) = create_test_store().await;
            let metadata = test_file_metadata();

            let path = PathBuf::from("/test/benchmark_file.txt");
            let inode = store.reserve_inode().await.unwrap();
            let file_id = FileId::generate();

            // This is the operation being measured
            store
                .create_file(
                    black_box(file_id),
                    black_box(&path),
                    black_box(inode),
                    black_box(metadata),
                )
                .await
                .unwrap();

            store.confirm_inode(inode).await.unwrap();
        });
    });
}

/// Benchmark: File lookup by inode after creating 100 files
/// Target: <2ms (actual: ~0.03ms from integration tests)
fn bench_lookup_by_inode_realistic(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("lookup_by_inode_100_files", |b| {
        b.iter_batched(
            || {
                // Setup: Create store with 100 files (not measured)
                runtime.block_on(async {
                    let (store, temp) = create_test_store().await;
                    let metadata = test_file_metadata();

                    let mut target_inode = 0;
                    for i in 0..100 {
                        let path = PathBuf::from(format!("/test/file_{}.txt", i));
                        let inode = store.reserve_inode().await.unwrap();
                        let file_id = FileId::generate();

                        store
                            .create_file(file_id, &path, inode, metadata.clone())
                            .await
                            .unwrap();
                        store.confirm_inode(inode).await.unwrap();

                        if i == 50 {
                            target_inode = inode;
                        }
                    }

                    (store, temp, target_inode)
                })
            },
            |(store, _temp, target_inode)| {
                // Measured operation: lookup by inode
                runtime.block_on(async move {
                    store
                        .get_file_by_inode(black_box(target_inode))
                        .await
                        .unwrap();
                })
            },
            BatchSize::SmallInput,
        );
    });
}

/// Benchmark: File lookup by path after creating 100 files
/// Target: <3ms (actual: ~0.03ms from integration tests)
fn bench_lookup_by_path_realistic(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("lookup_by_path_100_files", |b| {
        b.iter_batched(
            || {
                // Setup: Create store with 100 files (not measured)
                runtime.block_on(async {
                    let (store, temp) = create_test_store().await;
                    let metadata = test_file_metadata();

                    for i in 0..100 {
                        let path = PathBuf::from(format!("/test/file_{}.txt", i));
                        let inode = store.reserve_inode().await.unwrap();
                        let file_id = FileId::generate();

                        store
                            .create_file(file_id, &path, inode, metadata.clone())
                            .await
                            .unwrap();
                        store.confirm_inode(inode).await.unwrap();
                    }

                    (store, temp)
                })
            },
            |(store, _temp)| {
                // Measured operation: lookup by path
                runtime.block_on(async move {
                    let target_path = PathBuf::from("/test/file_50.txt");
                    store
                        .get_file_by_path(black_box(&target_path))
                        .await
                        .unwrap();
                })
            },
            BatchSize::SmallInput,
        );
    });
}

/// Benchmark: Directory listing with varying sizes
/// Target: <10ms for 100 files (actual: 2.7ms for 1000 files from integration tests)
fn bench_directory_listing(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("directory_listing");

    for size in [10, 50, 100, 500].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    // Setup: Create store with N files (not measured)
                    runtime.block_on(async move {
                        let (store, temp) = create_test_store().await;
                        let metadata = test_file_metadata();

                        for i in 0..size {
                            let path = PathBuf::from(format!("/test/file_{}.txt", i));
                            let inode = store.reserve_inode().await.unwrap();
                            let file_id = FileId::generate();

                            store
                                .create_file(file_id, &path, inode, metadata.clone())
                                .await
                                .unwrap();
                            store.confirm_inode(inode).await.unwrap();
                        }

                        (store, temp)
                    })
                },
                |(store, _temp)| {
                    // Measured operation: list directory
                    runtime.block_on(async move {
                        let dir_path = PathBuf::from("/test");
                        store.list_directory(black_box(&dir_path)).await.unwrap();
                    })
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark: Inode reservation
fn bench_inode_reservation(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("inode_reservation", |b| {
        b.iter_batched(
            || {
                // Setup: Create store (not measured)
                runtime.block_on(async { create_test_store().await })
            },
            |(store, _temp)| {
                // Measured operation: reserve inode
                runtime.block_on(async move {
                    store.reserve_inode().await.unwrap();
                })
            },
            BatchSize::SmallInput,
        );
    });
}

/// Benchmark: Stripe allocation
fn bench_stripe_allocation(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("stripe_allocation", |b| {
        b.iter_batched(
            || {
                // Setup: Create store with a file (not measured)
                runtime.block_on(async {
                    let (store, temp) = create_test_store().await;
                    let metadata = test_file_metadata();

                    let path = PathBuf::from("/test/file.txt");
                    let inode = store.reserve_inode().await.unwrap();
                    let file_id = FileId::generate();

                    store
                        .create_file(file_id, &path, inode, metadata)
                        .await
                        .unwrap();
                    store.confirm_inode(inode).await.unwrap();

                    (store, temp, file_id)
                })
            },
            |(store, _temp, file_id)| {
                // Measured operation: allocate stripes
                runtime.block_on(async move {
                    let stripes = vec![
                        StripeRecord {
                            stripe_id: StripeId::generate(),
                            file_id,
                            stripe_index: 0,
                            offset: 0,
                            size: 1024,
                            checksum: 12345,
                            created_at: std::time::SystemTime::now(),
                        },
                        StripeRecord {
                            stripe_id: StripeId::generate(),
                            file_id,
                            stripe_index: 1,
                            offset: 1024,
                            size: 1024,
                            checksum: 67890,
                            created_at: std::time::SystemTime::now(),
                        },
                    ];

                    store
                        .allocate_stripes(black_box(file_id), black_box(stripes))
                        .await
                        .unwrap();
                })
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    bench_single_file_creation,
    bench_lookup_by_inode_realistic,
    bench_lookup_by_path_realistic,
    bench_directory_listing,
    bench_inode_reservation,
    bench_stripe_allocation
);

criterion_main!(benches);
