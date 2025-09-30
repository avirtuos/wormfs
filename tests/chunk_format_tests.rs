use std::io::Cursor;
use uuid::Uuid;
use wormfs::chunk_format::*;

#[test]
fn test_chunk_header_creation_valid() {
    let chunk_id = Uuid::new_v4();
    let stripe_id = Uuid::new_v4();
    let file_id = Uuid::new_v4();
    
    let header = ChunkHeader::new(
        chunk_id,
        stripe_id,
        file_id,
        0,
        1024,
        0,
        4,
        2,
        0x12345678,
        CompressionAlgorithm::None,
    );
    
    assert!(header.is_ok());
    let header = header.unwrap();
    assert_eq!(header.chunk_id, chunk_id);
    assert_eq!(header.stripe_id, stripe_id);
    assert_eq!(header.file_id, file_id);
    assert_eq!(header.data_shards, 4);
    assert_eq!(header.parity_shards, 2);
    assert_eq!(header.version, CHUNK_FORMAT_VERSION);
}

#[test]
fn test_chunk_header_invalid_erasure_params() {
    let chunk_id = Uuid::new_v4();
    let stripe_id = Uuid::new_v4();
    let file_id = Uuid::new_v4();
    
    // Test zero data shards
    let result = ChunkHeader::new(
        chunk_id,
        stripe_id,
        file_id,
        0,
        1024,
        0,
        0, // Invalid
        2,
        0x12345678,
        CompressionAlgorithm::None,
    );
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ChunkError::InvalidErasureParams { .. }));
    
    // Test zero parity shards
    let result = ChunkHeader::new(
        chunk_id,
        stripe_id,
        file_id,
        0,
        1024,
        0,
        4,
        0, // Invalid
        0x12345678,
        CompressionAlgorithm::None,
    );
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ChunkError::InvalidErasureParams { .. }));
    
    // Test chunk index out of range
    let result = ChunkHeader::new(
        chunk_id,
        stripe_id,
        file_id,
        0,
        1024,
        6, // Invalid: >= (4 + 2)
        4,
        2,
        0x12345678,
        CompressionAlgorithm::None,
    );
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ChunkError::InvalidErasureParams { .. }));
}

#[test]
fn test_header_serialization_deserialization() {
    let original = ChunkHeader::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        1000,
        2000,
        2,
        6,
        3,
        0xABCDEF12,
        CompressionAlgorithm::None,
    ).unwrap();
    
    let serialized = original.serialize().unwrap();
    let deserialized = ChunkHeader::deserialize(&serialized).unwrap();
    
    assert_eq!(original, deserialized);
}

#[test]
fn test_header_serialized_size() {
    let header = ChunkHeader::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        0,
        1024,
        0,
        4,
        2,
        0x12345678,
        CompressionAlgorithm::None,
    ).unwrap();
    
    let serialized = header.serialize().unwrap();
    assert_eq!(serialized.len() as u16, header.serialized_size());
}

#[test]
fn test_invalid_version_deserialization() {
    let mut header = ChunkHeader::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        0,
        1024,
        0,
        4,
        2,
        0x12345678,
        CompressionAlgorithm::None,
    ).unwrap();
    
    // Manually set invalid version
    header.version = 99;
    let mut serialized = header.serialize().unwrap();
    serialized[0] = 99; // Corrupt version byte
    
    let result = ChunkHeader::deserialize(&serialized);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ChunkError::InvalidVersion { .. }));
}

#[test]
fn test_checksum_calculation() {
    let test_data = b"Hello, WormFS! This is test data for checksum calculation.";
    let checksum1 = calculate_checksum(test_data);
    let checksum2 = calculate_checksum(test_data);
    
    // Same data should produce same checksum
    assert_eq!(checksum1, checksum2);
    
    // Different data should produce different checksum
    let different_data = b"Different test data";
    let checksum3 = calculate_checksum(different_data);
    assert_ne!(checksum1, checksum3);
}

#[test]
fn test_write_read_chunk_roundtrip() {
    let header = ChunkHeader::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        0,
        1024,
        1,
        4,
        2,
        0x87654321,
        CompressionAlgorithm::None,
    ).unwrap();
    
    let test_data = b"This is test chunk data for write/read roundtrip testing.";
    
    // Write chunk to buffer
    let mut buffer = Vec::new();
    write_chunk(&mut buffer, header.clone(), test_data).unwrap();
    
    // Read chunk from buffer
    let mut cursor = Cursor::new(buffer);
    let (read_header, read_data) = read_chunk(&mut cursor).unwrap();
    
    // Verify data matches
    assert_eq!(read_data, test_data);
    
    // Verify header fields match (except data_checksum which is calculated)
    assert_eq!(read_header.chunk_id, header.chunk_id);
    assert_eq!(read_header.stripe_id, header.stripe_id);
    assert_eq!(read_header.file_id, header.file_id);
    assert_eq!(read_header.stripe_start_offset, header.stripe_start_offset);
    assert_eq!(read_header.stripe_end_offset, header.stripe_end_offset);
    assert_eq!(read_header.chunk_index, header.chunk_index);
    assert_eq!(read_header.data_shards, header.data_shards);
    assert_eq!(read_header.parity_shards, header.parity_shards);
    assert_eq!(read_header.stripe_checksum, header.stripe_checksum);
    assert_eq!(read_header.compression_algorithm, header.compression_algorithm);
    
    // Verify checksum was calculated correctly
    let expected_checksum = calculate_checksum(test_data);
    assert_eq!(read_header.data_checksum, expected_checksum);
}

#[test]
fn test_write_empty_chunk_data() {
    let header = ChunkHeader::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        0,
        1024,
        0,
        4,
        2,
        0x12345678,
        CompressionAlgorithm::None,
    ).unwrap();
    
    let empty_data = b"";
    let mut buffer = Vec::new();
    let result = write_chunk(&mut buffer, header, empty_data);
    
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ChunkError::EmptyChunkData));
}

#[test]
fn test_validate_chunk_success() {
    let test_data = b"Valid chunk data for validation testing";
    let checksum = calculate_checksum(test_data);
    
    let mut header = ChunkHeader::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        0,
        1024,
        0,
        4,
        2,
        0x12345678,
        CompressionAlgorithm::None,
    ).unwrap();
    
    header.data_checksum = checksum;
    
    let result = validate_chunk(&header, test_data);
    assert!(result.is_ok());
}

#[test]
fn test_validate_chunk_checksum_mismatch() {
    let test_data = b"Test data for checksum mismatch validation";
    
    let mut header = ChunkHeader::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        0,
        1024,
        0,
        4,
        2,
        0x12345678,
        CompressionAlgorithm::None,
    ).unwrap();
    
    header.data_checksum = 0xDEADBEEF; // Wrong checksum
    
    let result = validate_chunk(&header, test_data);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ChunkError::DataChecksumMismatch { .. }));
}

#[test]
fn test_validate_empty_chunk_data() {
    let header = ChunkHeader::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        0,
        1024,
        0,
        4,
        2,
        0x12345678,
        CompressionAlgorithm::None,
    ).unwrap();
    
    let empty_data = b"";
    let result = validate_chunk(&header, empty_data);
    
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ChunkError::EmptyChunkData));
}

#[test]
fn test_compression_algorithm_conversion() {
    assert_eq!(CompressionAlgorithm::from(0), CompressionAlgorithm::None);
    assert_eq!(CompressionAlgorithm::from(255), CompressionAlgorithm::None); // Unknown defaults to None
}

#[test]
fn test_large_chunk_data() {
    let header = ChunkHeader::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        0,
        1024 * 1024, // 1MB
        0,
        8,
        4,
        0x12345678,
        CompressionAlgorithm::None,
    ).unwrap();
    
    // Create 64KB of test data
    let test_data = vec![0xAB; 64 * 1024];
    
    // Write and read large chunk
    let mut buffer = Vec::new();
    write_chunk(&mut buffer, header.clone(), &test_data).unwrap();
    
    let mut cursor = Cursor::new(buffer);
    let (read_header, read_data) = read_chunk(&mut cursor).unwrap();
    
    assert_eq!(read_data, test_data);
    assert_eq!(read_header.chunk_id, header.chunk_id);
}

#[test]
fn test_multiple_chunks_same_stripe() {
    let stripe_id = Uuid::new_v4();
    let file_id = Uuid::new_v4();
    
    // Create multiple chunks for the same stripe
    let chunks: Vec<_> = (0..6).map(|i| {
        ChunkHeader::new(
            Uuid::new_v4(),
            stripe_id,
            file_id,
            i * 1024,
            (i + 1) * 1024,
            i as u8,
            4,
            2,
            0x12345678,
            CompressionAlgorithm::None,
        ).unwrap()
    }).collect();
    
    // Verify all chunks belong to same stripe and file
    for chunk in &chunks {
        assert_eq!(chunk.stripe_id, stripe_id);
        assert_eq!(chunk.file_id, file_id);
        assert_eq!(chunk.data_shards, 4);
        assert_eq!(chunk.parity_shards, 2);
    }
    
    // Verify chunk indices are correct
    for (i, chunk) in chunks.iter().enumerate() {
        assert_eq!(chunk.chunk_index, i as u8);
    }
}

#[test]
fn test_header_truncated_data() {
    let header = ChunkHeader::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        0,
        1024,
        0,
        4,
        2,
        0x12345678,
        CompressionAlgorithm::None,
    ).unwrap();
    
    let serialized = header.serialize().unwrap();
    
    // Try to deserialize truncated header
    let truncated = &serialized[..10]; // Only first 10 bytes
    let result = ChunkHeader::deserialize(truncated);
    
    assert!(result.is_err());
    // Should be either InvalidHeaderLength or IO error
    assert!(matches!(result.unwrap_err(), 
        ChunkError::InvalidHeaderLength { .. } | ChunkError::Io(_)));
}
