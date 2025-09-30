//! Performance benchmarks for the erasure coding module
//!
//! These benchmarks measure encoding and decoding performance across different
//! configurations and data sizes to help optimize WormFS performance.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Duration;
use wormfs::erasure_coding::*;

/// Generate test data of specified size with a simple pattern
fn generate_test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

/// Benchmark encoding performance across different stripe sizes
fn bench_encoding_stripe_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode_stripe_sizes");

    let stripe_sizes = vec![
        1024,            // 1KB
        4 * 1024,        // 4KB
        16 * 1024,       // 16KB
        64 * 1024,       // 64KB
        256 * 1024,      // 256KB
        1024 * 1024,     // 1MB
        4 * 1024 * 1024, // 4MB
    ];

    for size in stripe_sizes {
        let data = generate_test_data(size);
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(
            BenchmarkId::new("encode", format!("{}", size)),
            &data,
            |b, data| {
                b.iter(|| {
                    let config = ErasureCodingConfig::new(4, 2, size).unwrap();
                    encode_stripe(black_box(data), black_box(&config))
                });
            },
        );
    }

    group.finish();
}

/// Benchmark decoding performance across different stripe sizes
fn bench_decoding_stripe_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_stripe_sizes");

    let stripe_sizes = vec![
        1024,            // 1KB
        4 * 1024,        // 4KB
        16 * 1024,       // 16KB
        64 * 1024,       // 64KB
        256 * 1024,      // 256KB
        1024 * 1024,     // 1MB
        4 * 1024 * 1024, // 4MB
    ];

    for size in stripe_sizes {
        let data = generate_test_data(size);
        let config = ErasureCodingConfig::new(4, 2, size).unwrap();
        let encoded = encode_stripe(&data, &config).unwrap();
        let chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();

        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(
            BenchmarkId::new("decode_full", format!("{}", size)),
            &(&chunks, &config, data.len()),
            |b, (chunks, config, original_size)| {
                b.iter(|| {
                    decode_stripe_with_size(
                        black_box(chunks),
                        black_box(config),
                        black_box(*original_size),
                    )
                });
            },
        );
    }

    group.finish();
}

/// Benchmark different erasure coding configurations
fn bench_different_configurations(c: &mut Criterion) {
    let mut group = c.benchmark_group("configurations");

    let configs = vec![
        (2, 1, "2+1"),   // Minimal redundancy
        (3, 2, "3+2"),   // Common
        (4, 2, "4+2"),   // Standard
        (6, 3, "6+3"),   // Medium redundancy
        (8, 4, "8+4"),   // High redundancy
        (10, 2, "10+2"), // Many data shards
        (4, 8, "4+8"),   // High parity ratio
    ];

    let test_data = generate_test_data(1024 * 1024); // 1MB test data

    for (data_shards, parity_shards, name) in configs {
        let config = ErasureCodingConfig::new(data_shards, parity_shards, 1024 * 1024).unwrap();

        group.bench_with_input(
            BenchmarkId::new("encode", name),
            &(&test_data, &config),
            |b, (data, config)| {
                b.iter(|| encode_stripe(black_box(data), black_box(config)));
            },
        );
    }

    group.finish();
}

/// Benchmark decoding with missing chunks
fn bench_decoding_with_missing_chunks(c: &mut Criterion) {
    let mut group = c.benchmark_group("decode_missing_chunks");
    group.measurement_time(Duration::from_secs(10));

    let config = ErasureCodingConfig::new(8, 4, 1024 * 1024).unwrap(); // 8+4 configuration
    let test_data = generate_test_data(1024 * 1024); // 1MB
    let encoded = encode_stripe(&test_data, &config).unwrap();

    // Test different numbers of missing chunks
    let missing_patterns = vec![
        (0, "no_missing"),
        (1, "1_missing"),
        (2, "2_missing"),
        (3, "3_missing"),
        (4, "4_missing"), // Maximum missing (still recoverable)
    ];

    for (num_missing, name) in missing_patterns {
        let mut chunks: Vec<Option<Vec<u8>>> = encoded.shards.iter().cloned().map(Some).collect();

        // Remove chunks from the end (parity chunks first)
        for i in 0..num_missing {
            let index = chunks.len() - 1 - i;
            chunks[index] = None;
        }

        group.bench_with_input(
            BenchmarkId::new("decode", name),
            &(&chunks, &config, test_data.len()),
            |b, (chunks, config, original_size)| {
                b.iter(|| {
                    decode_stripe_with_size(
                        black_box(chunks),
                        black_box(config),
                        black_box(*original_size),
                    )
                });
            },
        );
    }

    group.finish();
}

/// Benchmark memory usage patterns
fn bench_memory_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_patterns");

    let config = ErasureCodingConfig::new(4, 2, 4 * 1024 * 1024).unwrap(); // 4MB stripes

    // Test different data patterns that might affect performance
    let patterns = vec![
        ("zeros", vec![0u8; 4 * 1024 * 1024]),
        ("ones", vec![0xFFu8; 4 * 1024 * 1024]),
        ("random", generate_test_data(4 * 1024 * 1024)),
        (
            "alternating",
            (0..4 * 1024 * 1024)
                .map(|i| if i % 2 == 0 { 0xAA } else { 0x55 })
                .collect(),
        ),
    ];

    for (name, data) in patterns {
        group.bench_with_input(
            BenchmarkId::new("encode", name),
            &(&data, &config),
            |b, (data, config)| {
                b.iter(|| encode_stripe(black_box(data), black_box(config)));
            },
        );
    }

    group.finish();
}

/// Benchmark chunk availability analysis
fn bench_chunk_availability_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_availability");

    let config = ErasureCodingConfig::new(8, 4, 1024 * 1024).unwrap();
    let test_data = generate_test_data(1024 * 1024);
    let encoded = encode_stripe(&test_data, &config).unwrap();

    // Create different availability patterns
    let patterns = vec![
        (
            "all_available",
            encoded.shards.iter().cloned().map(Some).collect::<Vec<_>>(),
        ),
        ("half_missing", {
            let mut chunks: Vec<Option<Vec<u8>>> =
                encoded.shards.iter().cloned().map(Some).collect();
            for i in (0..chunks.len()).step_by(2) {
                chunks[i] = None;
            }
            chunks
        }),
        ("minimal_available", {
            let mut chunks: Vec<Option<Vec<u8>>> = vec![None; encoded.shards.len()];
            for (i, chunk) in chunks.iter_mut().enumerate().take(config.data_shards) {
                *chunk = Some(encoded.shards[i].clone());
            }
            chunks
        }),
    ];

    for (name, chunks) in patterns {
        group.bench_with_input(
            BenchmarkId::new("analyze", name),
            &(&chunks, &config),
            |b, (chunks, config)| {
                b.iter(|| analyze_chunk_availability(black_box(chunks), black_box(config)));
            },
        );

        group.bench_with_input(
            BenchmarkId::new("verify", name),
            &(&chunks, &config),
            |b, (chunks, config)| {
                b.iter(|| verify_chunks_availability(black_box(chunks), black_box(config)));
            },
        );
    }

    group.finish();
}

/// Benchmark utility functions
fn bench_utility_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("utilities");

    let config = ErasureCodingConfig::new(8, 4, 1024 * 1024).unwrap();
    let test_data = generate_test_data(1024 * 1024);
    let encoded = encode_stripe(&test_data, &config).unwrap();

    let mut chunks: Vec<Option<Vec<u8>>> = encoded.shards.iter().cloned().map(Some).collect();
    chunks[1] = None;
    chunks[3] = None;
    chunks[7] = None;
    chunks[9] = None;

    group.bench_function("get_missing_indices", |b| {
        b.iter(|| get_missing_chunk_indices(black_box(&chunks)));
    });

    group.bench_function("get_available_indices", |b| {
        b.iter(|| get_available_chunk_indices(black_box(&chunks)));
    });

    group.finish();
}

/// Benchmark large data handling
fn bench_large_data(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_data");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(10); // Fewer samples for large data tests

    let config = ErasureCodingConfig::new(8, 4, 16 * 1024 * 1024).unwrap(); // 16MB stripes

    let sizes = vec![
        16 * 1024 * 1024, // 16MB
        32 * 1024 * 1024, // 32MB
        64 * 1024 * 1024, // 64MB
    ];

    for size in sizes {
        let data = generate_test_data(size);

        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(
            BenchmarkId::new("encode_large", format!("{}MB", size / (1024 * 1024))),
            &(&data, &config),
            |b, (data, config)| {
                b.iter(|| encode_stripe(black_box(data), black_box(config)));
            },
        );
    }

    group.finish();
}

/// Benchmark configuration overhead
fn bench_configuration_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("configuration");

    group.bench_function("config_creation", |b| {
        b.iter(|| ErasureCodingConfig::new(black_box(4), black_box(2), black_box(1024 * 1024)));
    });

    group.bench_function("config_preset_4_2", |b| {
        b.iter(ErasureCodingConfig::preset_4_2);
    });

    group.bench_function("config_preset_6_3", |b| {
        b.iter(ErasureCodingConfig::preset_6_3);
    });

    group.bench_function("config_preset_8_4", |b| {
        b.iter(ErasureCodingConfig::preset_8_4);
    });

    let config = ErasureCodingConfig::new(4, 2, 1024 * 1024).unwrap();

    group.bench_function("config_calculations", |b| {
        b.iter(|| {
            let _ = black_box(&config).total_shards();
            let _ = black_box(&config).shard_size();
            let _ = black_box(&config).padded_stripe_size();
        });
    });

    group.finish();
}

/// Benchmark encoding with varying k+m ratios
fn bench_redundancy_ratios(c: &mut Criterion) {
    let mut group = c.benchmark_group("redundancy_ratios");

    let test_data = generate_test_data(1024 * 1024); // 1MB

    // Different redundancy ratios
    let ratios = vec![
        (6, 1, "6:1"), // Low redundancy
        (4, 2, "2:1"), // Standard
        (3, 3, "1:1"), // Equal data/parity
        (2, 4, "1:2"), // High redundancy
        (1, 6, "1:6"), // Very high redundancy
    ];

    for (data_shards, parity_shards, ratio_name) in ratios {
        let config = ErasureCodingConfig::new(data_shards, parity_shards, 1024 * 1024).unwrap();

        group.bench_with_input(
            BenchmarkId::new("encode", ratio_name),
            &(&test_data, &config),
            |b, (data, config)| {
                b.iter(|| encode_stripe(black_box(data), black_box(config)));
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_encoding_stripe_sizes,
    bench_decoding_stripe_sizes,
    bench_different_configurations,
    bench_decoding_with_missing_chunks,
    bench_memory_patterns,
    bench_chunk_availability_analysis,
    bench_utility_functions,
    bench_large_data,
    bench_configuration_overhead,
    bench_redundancy_ratios,
);

criterion_main!(benches);
