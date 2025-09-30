//! Integration tests for the erasure coding module
//!
//! These tests verify the integration between erasure coding and chunk format modules,
//! as well as comprehensive failure scenarios and edge cases.

use std::io::Cursor;
use uuid::Uuid;
use wormfs::chunk_format::*;
use wormfs::erasure_coding::*;

/// Helper function to create test data of specified size
fn create_test_data(size: usize, pattern: u8) -> Vec<u8> {
    vec![pattern; size]
}

/// Helper function to create a test erasure coding configuration
fn create_test_config(
    data_shards: usize,
    parity_shards: usize,
    stripe_size: usize,
) -> ErasureCodingConfig {
    ErasureCodingConfig::new(data_shards, parity_shards, stripe_size).unwrap()
}

/// Test integration between erasure coding and chunk format
#[test]
fn test_erasure_coding_with_chunk_format_integration() {
    let config = create_test_config(4, 2, 1024);
    let test_data = b"Integration test data for erasure coding with chunk format.";

    // Encode stripe
    let encoded = encode_stripe(test_data, &config).unwrap();

    // Create chunk headers and write chunks to buffers
    let stripe_id = Uuid::new_v4();
    let file_id = Uuid::new_v4();
    let stripe_checksum = calculate_checksum(test_data);

    let mut chunk_buffers = Vec::new();

    for (i, shard_data) in encoded.shards.iter().enumerate() {
        let chunk_id = Uuid::new_v4();
        let header = ChunkHeader::new(
            chunk_id,
            stripe_id,
            file_id,
            0,
            test_data.len() as u64,
            i as u8,
            config.data_shards as u8,
            config.parity_shards as u8,
            stripe_checksum,
            CompressionAlgorithm::None,
        )
        .unwrap();

        let mut buffer = Vec::new();
        write_chunk(&mut buffer, header, shard_data).unwrap();
        chunk_buffers.push(buffer);
    }

    // Read chunks back and extract data
    let mut chunk_data = Vec::new();
    for buffer in &chunk_buffers {
        let mut cursor = Cursor::new(buffer);
        let (header, data) = read_chunk(&mut cursor).unwrap();

        // Verify header fields
        assert_eq!(header.stripe_id, stripe_id);
        assert_eq!(header.file_id, file_id);
        assert_eq!(header.data_shards, config.data_shards as u8);
        assert_eq!(header.parity_shards, config.parity_shards as u8);
        assert_eq!(header.stripe_checksum, stripe_checksum);

        chunk_data.push(data);
    }

    // Convert to erasure coding format and decode
    let chunks: Vec<Option<Vec<u8>>> = chunk_data.into_iter().map(Some).collect();
    let decoded = decode_stripe_with_size(&chunks, &config, test_data.len()).unwrap();

    assert_eq!(decoded, test_data);
}

/// Test encoding and decoding with various data sizes
#[test]
fn test_various_data_sizes() {
    let test_sizes = vec![
        1,     // Single byte
        15,    // Small data
        256,   // One shard size
        512,   // Half stripe
        1024,  // Full stripe
        1500,  // Larger than stripe
        4096,  // Multiple stripes worth
        10000, // Large data
    ];

    for size in test_sizes {
        // Use appropriate stripe size for each test data size
        #[allow(clippy::manual_div_ceil)] // Avoiding type ambiguity with div_ceil
        let stripe_size = ((size + 3) / 4) * 4; // Round up to multiple of 4 for alignment
        let config = create_test_config(4, 2, stripe_size.max(1024));
        let test_data = create_test_data(size, 0xAB);

        // Encode
        let encoded = encode_stripe(&test_data, &config).unwrap();
        assert_eq!(encoded.original_size, size);
        assert_eq!(encoded.shards.len(), config.total_shards());

        // Decode with all chunks
        let chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();
        let decoded = decode_stripe_with_size(&chunks, &config, size).unwrap();

        assert_eq!(decoded.len(), size);
        assert_eq!(decoded, test_data);
    }
}

/// Test reconstruction with different patterns of missing chunks
#[test]
fn test_missing_chunk_patterns() {
    let config = create_test_config(4, 2, 1024);
    let test_data = create_test_data(800, 0x42);

    let encoded = encode_stripe(&test_data, &config).unwrap();
    let original_chunks: Vec<Vec<u8>> = encoded.shards;

    // Test patterns: (missing_indices, should_succeed)
    let test_patterns = vec![
        (vec![0], true),           // Missing one data chunk
        (vec![4], true),           // Missing one parity chunk
        (vec![0, 4], true),        // Missing one data + one parity
        (vec![1, 2], true),        // Missing two data chunks
        (vec![4, 5], true),        // Missing two parity chunks
        (vec![0, 1, 4], false),    // Missing too many (3 chunks, need 4)
        (vec![0, 1, 2], false),    // Missing too many data chunks
        (vec![0, 1, 2, 3], false), // Missing all data chunks
    ];

    for (missing_indices, should_succeed) in test_patterns {
        let mut chunks: Vec<Option<Vec<u8>>> = original_chunks.iter().cloned().map(Some).collect();

        // Remove specified chunks
        for &index in &missing_indices {
            chunks[index] = None;
        }

        let result = decode_stripe_with_size(&chunks, &config, test_data.len());

        if should_succeed {
            let decoded = result.unwrap();
            assert_eq!(decoded, test_data, "Failed pattern: {:?}", missing_indices);
        } else {
            assert!(
                result.is_err(),
                "Should have failed for pattern: {:?}",
                missing_indices
            );
        }
    }
}

/// Test different erasure coding configurations
#[test]
fn test_different_configurations() {
    let configs = vec![
        (2, 1, 1024),  // Minimal: 2+1
        (3, 2, 1500),  // Common: 3+2
        (4, 2, 2048),  // Standard: 4+2
        (6, 3, 2048),  // Medium: 6+3
        (8, 4, 4096),  // High redundancy: 8+4
        (10, 2, 2048), // Many data shards: 10+2
    ];

    for (data_shards, parity_shards, stripe_size) in configs {
        let config = create_test_config(data_shards, parity_shards, stripe_size);
        let test_data = create_test_data(stripe_size, 0x55); // Use data that fits in stripe

        // Encode
        let encoded = encode_stripe(&test_data, &config).unwrap();
        assert_eq!(encoded.shards.len(), data_shards + parity_shards);

        // Test reconstruction with exactly minimum required chunks
        let mut chunks: Vec<Option<Vec<u8>>> = vec![None; config.total_shards()];
        for (i, chunk) in chunks.iter_mut().enumerate().take(data_shards) {
            *chunk = Some(encoded.shards[i].clone());
        }

        let decoded = decode_stripe_with_size(&chunks, &config, test_data.len()).unwrap();
        assert_eq!(decoded, test_data);
    }
}

/// Test error conditions and edge cases
#[test]
fn test_error_conditions() {
    let config = create_test_config(4, 2, 1024);

    // Test empty data
    let result = encode_stripe(&[], &config);
    assert!(matches!(result, Err(ErasureError::InvalidData { .. })));

    // Test invalid configuration
    assert!(ErasureCodingConfig::new(0, 2, 1024).is_err());
    assert!(ErasureCodingConfig::new(4, 0, 1024).is_err());
    assert!(ErasureCodingConfig::new(4, 2, 0).is_err());
    assert!(ErasureCodingConfig::new(200, 200, 1024).is_err());

    // Test chunk size mismatch
    let test_data = create_test_data(1000, 0x77);
    let encoded = encode_stripe(&test_data, &config).unwrap();

    let mut chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();
    chunks[0].as_mut().unwrap().push(0xFF); // Make chunk too large

    let result = decode_stripe(&chunks, &config);
    assert!(matches!(
        result,
        Err(ErasureError::ChunkSizeMismatch { .. })
    ));

    // Test wrong number of chunks
    let chunks_wrong_count = vec![Some(vec![0u8; 256]); 5]; // Should be 6
    let result = decode_stripe(&chunks_wrong_count, &config);
    assert!(matches!(result, Err(ErasureError::InvalidData { .. })));
}

/// Test chunk availability analysis
#[test]
fn test_chunk_availability_analysis() {
    let config = create_test_config(4, 2, 1024);
    let test_data = create_test_data(800, 0x88);

    let encoded = encode_stripe(&test_data, &config).unwrap();
    let mut chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();

    // All chunks available
    let stats = analyze_chunk_availability(&chunks, &config);
    assert_eq!(stats.total_chunks, 6);
    assert_eq!(stats.available_chunks, 6);
    assert_eq!(stats.missing_chunks, 0);
    assert_eq!(stats.data_chunks_available, 4);
    assert_eq!(stats.parity_chunks_available, 2);
    assert!(stats.can_reconstruct);

    // Remove some chunks
    chunks[1] = None; // Remove data chunk
    chunks[4] = None; // Remove parity chunk

    let stats = analyze_chunk_availability(&chunks, &config);
    assert_eq!(stats.total_chunks, 6);
    assert_eq!(stats.available_chunks, 4);
    assert_eq!(stats.missing_chunks, 2);
    assert_eq!(stats.data_chunks_available, 3);
    assert_eq!(stats.parity_chunks_available, 1);
    assert!(stats.can_reconstruct);

    // Remove too many chunks
    chunks[0] = None;
    chunks[2] = None;

    let stats = analyze_chunk_availability(&chunks, &config);
    assert_eq!(stats.available_chunks, 2);
    assert!(!stats.can_reconstruct);
}

/// Test utility functions
#[test]
fn test_utility_functions() {
    let chunks = vec![
        Some(vec![1, 2, 3]),
        None,
        Some(vec![4, 5, 6]),
        None,
        Some(vec![7, 8, 9]),
        None,
    ];

    let missing = get_missing_chunk_indices(&chunks);
    assert_eq!(missing, vec![1, 3, 5]);

    let available = get_available_chunk_indices(&chunks);
    assert_eq!(available, vec![0, 2, 4]);

    let config = create_test_config(4, 2, 1024);

    // Test verify_chunks_availability
    let result = verify_chunks_availability(&chunks, &config);
    assert!(result.is_err()); // Only 3 available, need 4

    let chunks_sufficient = vec![
        Some(vec![1; 256]),
        Some(vec![2; 256]),
        Some(vec![3; 256]),
        Some(vec![4; 256]),
        None,
        None,
    ];

    let result = verify_chunks_availability(&chunks_sufficient, &config);
    assert!(result.is_ok()); // 4 available, need 4
}

/// Test preset configurations
#[test]
fn test_preset_configurations() {
    let test_data = create_test_data(1000, 0x99);

    let presets = vec![
        ErasureCodingConfig::preset_4_2().unwrap(),
        ErasureCodingConfig::preset_6_3().unwrap(),
        ErasureCodingConfig::preset_8_4().unwrap(),
    ];

    for config in presets {
        // Encode
        let encoded = encode_stripe(&test_data, &config).unwrap();

        // Verify configuration
        assert_eq!(encoded.config, config);
        assert_eq!(encoded.shards.len(), config.total_shards());

        // Test decode
        let chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();
        let decoded = decode_stripe_with_size(&chunks, &config, test_data.len()).unwrap();
        assert_eq!(decoded, test_data);
    }
}

/// Test large data handling
#[test]
fn test_large_data_handling() {
    let large_data_size = 100 * 1024; // 100KB data
    let config = create_test_config(6, 3, large_data_size); // Use stripe size that accommodates data
    let large_data = create_test_data(large_data_size, 0xCC);

    let encoded = encode_stripe(&large_data, &config).unwrap();
    assert_eq!(encoded.original_size, large_data_size);

    // Test with some missing chunks
    let mut chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();
    chunks[1] = None; // Remove data chunk
    chunks[7] = None; // Remove parity chunk
    chunks[8] = None; // Remove another parity chunk

    let decoded = decode_stripe_with_size(&chunks, &config, large_data.len()).unwrap();
    assert_eq!(decoded, large_data);
}

/// Test boundary conditions for shard sizes
#[test]
fn test_shard_size_boundaries() {
    // Test when data size exactly matches shard boundaries
    let config = create_test_config(4, 2, 1024);
    let shard_size = config.shard_size(); // 256 bytes

    // Data that exactly fills data shards
    let exact_data = create_test_data(shard_size * 4, 0xDD);
    let encoded = encode_stripe(&exact_data, &config).unwrap();

    let chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();
    let decoded = decode_stripe_with_size(&chunks, &config, exact_data.len()).unwrap();
    assert_eq!(decoded, exact_data);

    // Data that's one byte less than exact fit
    let almost_exact = create_test_data(shard_size * 4 - 1, 0xEE);
    let encoded = encode_stripe(&almost_exact, &config).unwrap();

    let chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();
    let decoded = decode_stripe_with_size(&chunks, &config, almost_exact.len()).unwrap();
    assert_eq!(decoded, almost_exact);

    // Data that's one byte more than exact fit - need larger stripe size
    let over_exact_size = shard_size * 4 + 1;
    let over_exact_config = create_test_config(4, 2, over_exact_size);
    let over_exact = create_test_data(over_exact_size, 0xFF);
    let encoded = encode_stripe(&over_exact, &over_exact_config).unwrap();

    let chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();
    let decoded = decode_stripe_with_size(&chunks, &over_exact_config, over_exact.len()).unwrap();
    assert_eq!(decoded, over_exact);
}

/// Test corrupted data detection
#[test]
fn test_data_corruption_scenarios() {
    let config = create_test_config(4, 2, 1024);
    let test_data = create_test_data(800, 0x11);

    let encoded = encode_stripe(&test_data, &config).unwrap();
    let mut chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();

    // Corrupt a byte in a data chunk
    chunks[0].as_mut().unwrap()[10] = 0xFF;

    // Remove enough parity chunks so reconstruction relies on corrupted data
    chunks[4] = None;
    chunks[5] = None;

    let decoded_result = decode_stripe_with_size(&chunks, &config, test_data.len());
    // This should succeed but return corrupted data (RS can't detect corruption)
    assert!(decoded_result.is_ok());
    let decoded = decoded_result.unwrap();
    assert_ne!(decoded, test_data); // Data should be different due to corruption
}

/// Stress test with random patterns
#[test]
fn test_stress_random_patterns() {
    use std::collections::HashSet;

    let config = create_test_config(8, 4, 4096);

    // Generate test data with pattern
    let mut test_data = Vec::with_capacity(3000);
    for i in 0..3000 {
        test_data.push((i % 256) as u8);
    }

    let encoded = encode_stripe(&test_data, &config).unwrap();

    // Test multiple random combinations of missing chunks
    let total_shards = config.total_shards();
    let min_required = config.data_shards;

    for num_missing in 0..=(total_shards - min_required) {
        // Try different combinations of missing chunks
        for attempt in 0..5 {
            let mut missing_set = HashSet::new();

            // Select random chunks to remove
            while missing_set.len() < num_missing {
                let index = (attempt * 7 + missing_set.len() * 13) % total_shards;
                missing_set.insert(index);
            }

            let mut chunks: Vec<Option<Vec<u8>>> =
                encoded.shards.iter().cloned().map(Some).collect();
            for &missing_index in &missing_set {
                chunks[missing_index] = None;
            }

            let result = decode_stripe_with_size(&chunks, &config, test_data.len());

            if chunks.iter().filter(|c| c.is_some()).count() >= min_required {
                let decoded = result.unwrap();
                assert_eq!(
                    decoded, test_data,
                    "Failed with missing chunks: {:?}",
                    missing_set
                );
            } else {
                assert!(
                    result.is_err(),
                    "Should fail with insufficient chunks: {:?}",
                    missing_set
                );
            }
        }
    }
}
