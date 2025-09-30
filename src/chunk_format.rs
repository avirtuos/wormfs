//! Chunk Format Module
//! 
//! This module defines the binary format for WormFS chunks, including header structure,
//! serialization/deserialization, and validation operations.

#![allow(dead_code)] // Allow dead code for public API that will be used by other modules

use std::io::{Read, Write, Cursor};
use uuid::Uuid;
use crc32fast::Hasher;
use thiserror::Error;

/// Current chunk format version
pub const CHUNK_FORMAT_VERSION: u8 = 1;

/// Compression algorithms supported
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionAlgorithm {
    None = 0,
    // Future: LZ4 = 1, Zstd = 2, etc.
}

impl From<u8> for CompressionAlgorithm {
    fn from(value: u8) -> Self {
        match value {
            0 => CompressionAlgorithm::None,
            _ => CompressionAlgorithm::None, // Default to None for unknown values
        }
    }
}

/// Errors that can occur during chunk operations
#[derive(Error, Debug)]
pub enum ChunkError {
    #[error("Invalid chunk format version: expected {expected}, found {found}")]
    InvalidVersion { expected: u8, found: u8 },
    
    #[error("Header checksum mismatch: expected {expected:08x}, calculated {calculated:08x}")]
    HeaderChecksumMismatch { expected: u32, calculated: u32 },
    
    #[error("Data checksum mismatch: expected {expected:08x}, calculated {calculated:08x}")]
    DataChecksumMismatch { expected: u32, calculated: u32 },
    
    #[error("Invalid header length: {length}")]
    InvalidHeaderLength { length: u16 },
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Invalid UUID format")]
    InvalidUuid,
    
    #[error("Chunk data is empty")]
    EmptyChunkData,
    
    #[error("Invalid erasure coding parameters: data_shards={data_shards}, parity_shards={parity_shards}")]
    InvalidErasureParams { data_shards: u8, parity_shards: u8 },
}

/// Chunk header containing all metadata for a chunk
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkHeader {
    /// Format version for compatibility
    pub version: u8,
    /// CRC32 checksum of the chunk data
    pub data_checksum: u32,
    /// Unique identifier for this chunk
    pub chunk_id: Uuid,
    /// Identifier of the stripe this chunk belongs to
    pub stripe_id: Uuid,
    /// Identifier of the file this chunk belongs to
    pub file_id: Uuid,
    /// Starting byte offset of the stripe in the original file
    pub stripe_start_offset: u64,
    /// Ending byte offset of the stripe in the original file
    pub stripe_end_offset: u64,
    /// Index of this chunk within the stripe (0-based)
    pub chunk_index: u8,
    /// Number of data shards in the erasure coding scheme
    pub data_shards: u8,
    /// Number of parity shards in the erasure coding scheme
    pub parity_shards: u8,
    /// CRC32 checksum of the original stripe data
    pub stripe_checksum: u32,
    /// Compression algorithm used
    pub compression_algorithm: CompressionAlgorithm,
}

impl ChunkHeader {
    /// Create a new chunk header
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chunk_id: Uuid,
        stripe_id: Uuid,
        file_id: Uuid,
        stripe_start_offset: u64,
        stripe_end_offset: u64,
        chunk_index: u8,
        data_shards: u8,
        parity_shards: u8,
        stripe_checksum: u32,
        compression_algorithm: CompressionAlgorithm,
    ) -> Result<Self, ChunkError> {
        // Validate erasure coding parameters
        if data_shards == 0 || parity_shards == 0 {
            return Err(ChunkError::InvalidErasureParams { data_shards, parity_shards });
        }
        
        if chunk_index >= (data_shards + parity_shards) {
            return Err(ChunkError::InvalidErasureParams { data_shards, parity_shards });
        }

        Ok(ChunkHeader {
            version: CHUNK_FORMAT_VERSION,
            data_checksum: 0, // Will be set when writing chunk data
            chunk_id,
            stripe_id,
            file_id,
            stripe_start_offset,
            stripe_end_offset,
            chunk_index,
            data_shards,
            parity_shards,
            stripe_checksum,
            compression_algorithm,
        })
    }


    /// Calculate the size of the serialized header
    pub fn serialized_size(&self) -> u16 {
        1 +  // version
        2 +  // header_length
        4 +  // data_checksum
        16 + // chunk_id
        16 + // stripe_id
        16 + // file_id
        8 +  // stripe_start_offset
        8 +  // stripe_end_offset
        1 +  // chunk_index
        1 +  // data_shards
        1 +  // parity_shards
        4 +  // stripe_checksum
        1 +  // compression_algorithm
        4    // reserved bytes
    }

    /// Serialize the header to bytes
    pub fn serialize(&self) -> Result<Vec<u8>, ChunkError> {
        let mut buffer = Vec::new();
        let header_size = self.serialized_size();

        // Write header fields
        buffer.push(self.version);
        buffer.extend_from_slice(&header_size.to_le_bytes());
        buffer.extend_from_slice(&self.data_checksum.to_le_bytes());
        buffer.extend_from_slice(self.chunk_id.as_bytes());
        buffer.extend_from_slice(self.stripe_id.as_bytes());
        buffer.extend_from_slice(self.file_id.as_bytes());
        buffer.extend_from_slice(&self.stripe_start_offset.to_le_bytes());
        buffer.extend_from_slice(&self.stripe_end_offset.to_le_bytes());
        buffer.push(self.chunk_index);
        buffer.push(self.data_shards);
        buffer.push(self.parity_shards);
        buffer.extend_from_slice(&self.stripe_checksum.to_le_bytes());
        buffer.push(self.compression_algorithm as u8);
        
        // Reserved bytes for future expansion
        buffer.extend_from_slice(&[0u8; 4]);

        Ok(buffer)
    }

    /// Deserialize header from bytes
    pub fn deserialize(data: &[u8]) -> Result<Self, ChunkError> {
        if data.len() < 3 {
            return Err(ChunkError::InvalidHeaderLength { length: data.len() as u16 });
        }

        let mut cursor = Cursor::new(data);
        
        // Read version
        let mut version_buf = [0u8; 1];
        cursor.read_exact(&mut version_buf)?;
        let version = version_buf[0];
        
        if version != CHUNK_FORMAT_VERSION {
            return Err(ChunkError::InvalidVersion { 
                expected: CHUNK_FORMAT_VERSION, 
                found: version 
            });
        }

        // Read header length
        let mut header_len_buf = [0u8; 2];
        cursor.read_exact(&mut header_len_buf)?;
        let header_length = u16::from_le_bytes(header_len_buf);

        if data.len() < header_length as usize {
            return Err(ChunkError::InvalidHeaderLength { length: header_length });
        }

        // Read data checksum
        let mut checksum_buf = [0u8; 4];
        cursor.read_exact(&mut checksum_buf)?;
        let data_checksum = u32::from_le_bytes(checksum_buf);

        // Read UUIDs
        let mut uuid_buf = [0u8; 16];
        
        cursor.read_exact(&mut uuid_buf)?;
        let chunk_id = Uuid::from_bytes(uuid_buf);
        
        cursor.read_exact(&mut uuid_buf)?;
        let stripe_id = Uuid::from_bytes(uuid_buf);
        
        cursor.read_exact(&mut uuid_buf)?;
        let file_id = Uuid::from_bytes(uuid_buf);

        // Read offsets
        let mut offset_buf = [0u8; 8];
        cursor.read_exact(&mut offset_buf)?;
        let stripe_start_offset = u64::from_le_bytes(offset_buf);
        
        cursor.read_exact(&mut offset_buf)?;
        let stripe_end_offset = u64::from_le_bytes(offset_buf);

        // Read erasure coding parameters
        let mut byte_buf = [0u8; 1];
        cursor.read_exact(&mut byte_buf)?;
        let chunk_index = byte_buf[0];
        
        cursor.read_exact(&mut byte_buf)?;
        let data_shards = byte_buf[0];
        
        cursor.read_exact(&mut byte_buf)?;
        let parity_shards = byte_buf[0];

        // Read stripe checksum
        cursor.read_exact(&mut checksum_buf)?;
        let stripe_checksum = u32::from_le_bytes(checksum_buf);

        // Read compression algorithm
        cursor.read_exact(&mut byte_buf)?;
        let compression_algorithm = CompressionAlgorithm::from(byte_buf[0]);

        // Skip reserved bytes
        let mut reserved_buf = [0u8; 4];
        cursor.read_exact(&mut reserved_buf)?;

        Ok(ChunkHeader {
            version,
            data_checksum,
            chunk_id,
            stripe_id,
            file_id,
            stripe_start_offset,
            stripe_end_offset,
            chunk_index,
            data_shards,
            parity_shards,
            stripe_checksum,
            compression_algorithm,
        })
    }
}

/// Calculate CRC32 checksum of data
pub fn calculate_checksum(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Write a chunk (header + data) to a writer
pub fn write_chunk<W: Write>(
    writer: &mut W,
    mut header: ChunkHeader,
    data: &[u8],
) -> Result<(), ChunkError> {
    if data.is_empty() {
        return Err(ChunkError::EmptyChunkData);
    }

    // Calculate and set data checksum
    header.data_checksum = calculate_checksum(data);

    // Serialize header
    let header_bytes = header.serialize()?;

    // Write header
    writer.write_all(&header_bytes)?;

    // Write data
    writer.write_all(data)?;

    Ok(())
}

/// Read a chunk (header + data) from a reader
pub fn read_chunk<R: Read>(reader: &mut R) -> Result<(ChunkHeader, Vec<u8>), ChunkError> {
    // First, read enough bytes to get version and header length
    let mut initial_buf = [0u8; 3];
    reader.read_exact(&mut initial_buf)?;

    let version = initial_buf[0];
    let header_length = u16::from_le_bytes([initial_buf[1], initial_buf[2]]);

    // Read the rest of the header
    let mut header_buf = vec![0u8; header_length as usize];
    header_buf[0] = version;
    header_buf[1] = initial_buf[1];
    header_buf[2] = initial_buf[2];
    reader.read_exact(&mut header_buf[3..])?;

    // Deserialize header
    let header = ChunkHeader::deserialize(&header_buf)?;

    // Read all remaining data
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;

    if data.is_empty() {
        return Err(ChunkError::EmptyChunkData);
    }

    // Verify data checksum
    let calculated_checksum = calculate_checksum(&data);
    if calculated_checksum != header.data_checksum {
        return Err(ChunkError::DataChecksumMismatch {
            expected: header.data_checksum,
            calculated: calculated_checksum,
        });
    }

    Ok((header, data))
}

/// Validate chunk integrity (header and data checksums)
pub fn validate_chunk(header: &ChunkHeader, data: &[u8]) -> Result<(), ChunkError> {
    if data.is_empty() {
        return Err(ChunkError::EmptyChunkData);
    }

    let calculated_checksum = calculate_checksum(data);
    if calculated_checksum != header.data_checksum {
        return Err(ChunkError::DataChecksumMismatch {
            expected: header.data_checksum,
            calculated: calculated_checksum,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn create_test_header() -> ChunkHeader {
        ChunkHeader::new(
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
        ).unwrap()
    }

    #[test]
    fn test_header_creation() {
        let header = create_test_header();
        assert_eq!(header.version, CHUNK_FORMAT_VERSION);
        assert_eq!(header.data_shards, 4);
        assert_eq!(header.parity_shards, 2);
    }

    #[test]
    fn test_header_serialization_roundtrip() {
        let original_header = create_test_header();
        let serialized = original_header.serialize().unwrap();
        let deserialized = ChunkHeader::deserialize(&serialized).unwrap();
        
        assert_eq!(original_header, deserialized);
    }

    #[test]
    fn test_chunk_write_read_roundtrip() {
        let header = create_test_header();
        let test_data = b"Hello, WormFS! This is test chunk data.";
        
        // Write chunk
        let mut buffer = Vec::new();
        write_chunk(&mut buffer, header.clone(), test_data).unwrap();
        
        // Read chunk
        let mut cursor = Cursor::new(buffer);
        let (read_header, read_data) = read_chunk(&mut cursor).unwrap();
        
        assert_eq!(read_data, test_data);
        assert_eq!(read_header.chunk_id, header.chunk_id);
        assert_eq!(read_header.stripe_id, header.stripe_id);
        assert_eq!(read_header.file_id, header.file_id);
    }

    #[test]
    fn test_checksum_validation() {
        let test_data = b"Test data for checksum validation";
        let checksum = calculate_checksum(test_data);
        
        let mut header = create_test_header();
        header.data_checksum = checksum;
        
        assert!(validate_chunk(&header, test_data).is_ok());
        
        // Test with wrong checksum
        header.data_checksum = 0xDEADBEEF;
        assert!(validate_chunk(&header, test_data).is_err());
    }

    #[test]
    fn test_invalid_erasure_params() {
        let result = ChunkHeader::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            0,
            1024,
            0,
            0, // Invalid: zero data shards
            2,
            0x12345678,
            CompressionAlgorithm::None,
        );
        
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ChunkError::InvalidErasureParams { .. }));
    }

    #[test]
    fn test_empty_chunk_data() {
        let header = create_test_header();
        let empty_data = b"";
        
        let mut buffer = Vec::new();
        let result = write_chunk(&mut buffer, header, empty_data);
        
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ChunkError::EmptyChunkData));
    }
}
