use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use std::io::Cursor;
use uuid::Uuid;
use wormfs::chunk_format::*;

fn create_test_header() -> ChunkHeader {
    ChunkHeader::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        0,
        1024 * 1024, // 1MB stripe
        0,
        4,
        2,
        0x12345678,
        CompressionAlgorithm::None,
    ).unwrap()
}

fn bench_header_serialization(c: &mut Criterion) {
    let header = create_test_header();
    
    c.bench_function("header_serialize", |b| {
        b.iter(|| {
            black_box(header.serialize().unwrap())
        })
    });
}

fn bench_header_deserialization(c: &mut Criterion) {
    let header = create_test_header();
    let serialized = header.serialize().unwrap();
    
    c.bench_function("header_deserialize", |b| {
        b.iter(|| {
            black_box(ChunkHeader::deserialize(&serialized).unwrap())
        })
    });
}

fn bench_checksum_calculation(c: &mut Criterion) {
    let mut group = c.benchmark_group("checksum_calculation");
    
    for size in [1024, 4096, 16384, 65536, 262144].iter() {
        let data = vec![0xAB; *size];
        
        group.bench_with_input(BenchmarkId::new("crc32", size), size, |b, _| {
            b.iter(|| {
                black_box(calculate_checksum(&data))
            })
        });
    }
    
    group.finish();
}

fn bench_chunk_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_write");
    
    for size in [1024, 4096, 16384, 65536, 262144].iter() {
        let header = create_test_header();
        let data = vec![0xCD; *size];
        
        group.bench_with_input(BenchmarkId::new("write_chunk", size), size, |b, _| {
            b.iter(|| {
                let mut buffer = Vec::new();
                write_chunk(&mut buffer, header.clone(), &data).unwrap();
                black_box(())
            })
        });
    }
    
    group.finish();
}

fn bench_chunk_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_read");
    
    for size in [1024, 4096, 16384, 65536, 262144].iter() {
        let header = create_test_header();
        let data = vec![0xEF; *size];
        
        // Pre-create the chunk data
        let mut buffer = Vec::new();
        write_chunk(&mut buffer, header.clone(), &data).unwrap();
        
        group.bench_with_input(BenchmarkId::new("read_chunk", size), size, |b, _| {
            b.iter(|| {
                let mut cursor = Cursor::new(&buffer);
                black_box(read_chunk(&mut cursor).unwrap())
            })
        });
    }
    
    group.finish();
}

fn bench_chunk_validation(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_validation");
    
    for size in [1024, 4096, 16384, 65536, 262144].iter() {
        let data = vec![0x42; *size];
        let checksum = calculate_checksum(&data);
        
        let mut header = create_test_header();
        header.data_checksum = checksum;
        
        group.bench_with_input(BenchmarkId::new("validate_chunk", size), size, |b, _| {
            b.iter(|| {
                validate_chunk(&header, &data).unwrap();
                black_box(())
            })
        });
    }
    
    group.finish();
}

fn bench_roundtrip_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("roundtrip_operations");
    
    for size in [1024, 4096, 16384, 65536].iter() {
        let header = create_test_header();
        let data = vec![0x99; *size];
        
        group.bench_with_input(BenchmarkId::new("write_read_roundtrip", size), size, |b, _| {
            b.iter(|| {
                // Write chunk
                let mut buffer = Vec::new();
                write_chunk(&mut buffer, header.clone(), &data).unwrap();
                
                // Read chunk back
                let mut cursor = Cursor::new(buffer);
                let (read_header, read_data) = read_chunk(&mut cursor).unwrap();
                
                black_box((read_header, read_data))
            })
        });
    }
    
    group.finish();
}

fn bench_multiple_chunks_same_stripe(c: &mut Criterion) {
    let stripe_id = Uuid::new_v4();
    let file_id = Uuid::new_v4();
    
    // Create 6 chunks for the same stripe (4 data + 2 parity)
    let chunks: Vec<_> = (0..6).map(|i| {
        ChunkHeader::new(
            Uuid::new_v4(),
            stripe_id,
            file_id,
            i * 4096,
            (i + 1) * 4096,
            i as u8,
            4,
            2,
            0x12345678,
            CompressionAlgorithm::None,
        ).unwrap()
    }).collect();
    
    let chunk_data = vec![0x77; 4096];
    
    c.bench_function("write_multiple_chunks", |b| {
        b.iter(|| {
            let mut buffers = Vec::new();
            for chunk in &chunks {
                let mut buffer = Vec::new();
                write_chunk(&mut buffer, chunk.clone(), &chunk_data).unwrap();
                buffers.push(buffer);
            }
            black_box(buffers)
        })
    });
}

fn bench_header_field_access(c: &mut Criterion) {
    let header = create_test_header();
    
    c.bench_function("header_field_access", |b| {
        b.iter(|| {
            black_box((
                header.chunk_id,
                header.stripe_id,
                header.file_id,
                header.data_shards,
                header.parity_shards,
                header.chunk_index,
                header.stripe_start_offset,
                header.stripe_end_offset,
            ))
        })
    });
}

fn bench_compression_algorithm_conversion(c: &mut Criterion) {
    c.bench_function("compression_algorithm_from_u8", |b| {
        b.iter(|| {
            for i in 0..=255u8 {
                black_box(CompressionAlgorithm::from(i));
            }
        })
    });
}

criterion_group!(
    benches,
    bench_header_serialization,
    bench_header_deserialization,
    bench_checksum_calculation,
    bench_chunk_write,
    bench_chunk_read,
    bench_chunk_validation,
    bench_roundtrip_operations,
    bench_multiple_chunks_same_stripe,
    bench_header_field_access,
    bench_compression_algorithm_conversion
);

criterion_main!(benches);
