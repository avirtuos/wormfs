# Chunk Format API Documentation

## Overview

The `chunk_format` module provides the core binary format for WormFS chunks, including header structure, serialization/deserialization, and validation operations. This is the foundation upon which all other WormFS components are built.

## Key Concepts

### Chunk Structure

Each chunk consists of:
1. **Variable-length binary header** - Contains metadata about the chunk
2. **Chunk data** - The actual erasure-coded data

### Header Format

The chunk header contains the following fields:

| Field | Size | Description |
|-------|------|-------------|
| Version | 1 byte | Format version for compatibility |
| Header Length | 2 bytes | Total size of the header |
| Data Checksum | 4 bytes | CRC32 checksum of chunk data |
| Chunk ID | 16 bytes | Unique identifier for this chunk |
| Stripe ID | 16 bytes | Identifier of the stripe this chunk belongs to |
| File ID | 16 bytes | Identifier of the file this chunk belongs to |
| Stripe Start Offset | 8 bytes | Starting byte offset of stripe in file |
| Stripe End Offset | 8 bytes | Ending byte offset of stripe in file |
| Chunk Index | 1 byte | Index of this chunk within the stripe |
| Data Shards | 1 byte | Number of data shards in erasure coding |
| Parity Shards | 1 byte | Number of parity shards in erasure coding |
| Stripe Checksum | 4 bytes | CRC32 checksum of original stripe data |
| Compression Algorithm | 1 byte | Compression algorithm used |
| Reserved | 4 bytes | Reserved for future expansion |

**Total Header Size:** 83 bytes (fixed for version 1)

## API Reference

### Types

#### `ChunkHeader`

Represents the metadata for a chunk.

```rust
pub struct ChunkHeader {
    pub version: u8,
    pub data_checksum: u32,
    pub chunk_id: Uuid,
    pub stripe_id: Uuid,
    pub file_id: Uuid,
    pub stripe_start_offset: u64,
    pub stripe_end_offset: u64,
    pub chunk_index: u8,
    pub data_shards: u8,
    pub parity_shards: u8,
    pub stripe_checksum: u32,
    pub compression_algorithm: CompressionAlgorithm,
}
```

#### `CompressionAlgorithm`

Enumeration of supported compression algorithms.

```rust
pub enum CompressionAlgorithm {
    None = 0,
    // Future: LZ4 = 1, Zstd = 2, etc.
}
```

#### `ChunkError`

Error types that can occur during chunk operations.

```rust
pub enum ChunkError {
    InvalidVersion { expected: u8, found: u8 },
    HeaderChecksumMismatch { expected: u32, calculated: u32 },
    DataChecksumMismatch { expected: u32, calculated: u32 },
    InvalidHeaderLength { length: u16 },
    Io(std::io::Error),
    InvalidUuid,
    EmptyChunkData,
    InvalidErasureParams { data_shards: u8, parity_shards: u8 },
}
```

### Functions

#### `ChunkHeader::new()`

Creates a new chunk header with validation.

```rust
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
) -> Result<Self, ChunkError>
```

**Parameters:**
- `chunk_id` - Unique identifier for this chunk
- `stripe_id` - Identifier of the stripe this chunk belongs to
- `file_id` - Identifier of the file this chunk belongs to
- `stripe_start_offset` - Starting byte offset of the stripe in the original file
- `stripe_end_offset` - Ending byte offset of the stripe in the original file
- `chunk_index` - Index of this chunk within the stripe (0-based)
- `data_shards` - Number of data shards in the erasure coding scheme
- `parity_shards` - Number of parity shards in the erasure coding scheme
- `stripe_checksum` - CRC32 checksum of the original stripe data
- `compression_algorithm` - Compression algorithm used

**Returns:** `Result<ChunkHeader, ChunkError>`

**Errors:**
- `InvalidErasureParams` - If data_shards or parity_shards is 0, or chunk_index is out of range

#### `ChunkHeader::serialize()`

Serializes the header to bytes.

```rust
pub fn serialize(&self) -> Result<Vec<u8>, ChunkError>
```

**Returns:** `Result<Vec<u8>, ChunkError>` - The serialized header bytes

#### `ChunkHeader::deserialize()`

Deserializes header from bytes.

```rust
pub fn deserialize(data: &[u8]) -> Result<Self, ChunkError>
```

**Parameters:**
- `data` - Byte slice containing the serialized header

**Returns:** `Result<ChunkHeader, ChunkError>`

**Errors:**
- `InvalidVersion` - If the version doesn't match the expected version
- `InvalidHeaderLength` - If the header length is invalid
- `Io` - If there's an error reading the data

#### `write_chunk()`

Writes a chunk (header + data) to a writer.

```rust
pub fn write_chunk<W: Write>(
    writer: &mut W,
    header: ChunkHeader,
    data: &[u8],
) -> Result<(), ChunkError>
```

**Parameters:**
- `writer` - Writer to write the chunk to
- `header` - Chunk header (data_checksum will be calculated automatically)
- `data` - Chunk data to write

**Returns:** `Result<(), ChunkError>`

**Errors:**
- `EmptyChunkData` - If the data is empty
- `Io` - If there's an error writing to the writer

#### `read_chunk()`

Reads a chunk (header + data) from a reader.

```rust
pub fn read_chunk<R: Read>(reader: &mut R) -> Result<(ChunkHeader, Vec<u8>), ChunkError>
```

**Parameters:**
- `reader` - Reader to read the chunk from

**Returns:** `Result<(ChunkHeader, Vec<u8>), ChunkError>` - Tuple of header and data

**Errors:**
- `InvalidVersion` - If the header version is invalid
- `InvalidHeaderLength` - If the header length is invalid
- `DataChecksumMismatch` - If the data checksum doesn't match
- `EmptyChunkData` - If no data was read
- `Io` - If there's an error reading from the reader

#### `validate_chunk()`

Validates chunk integrity by checking data checksum.

```rust
pub fn validate_chunk(header: &ChunkHeader, data: &[u8]) -> Result<(), ChunkError>
```

**Parameters:**
- `header` - Chunk header containing expected checksum
- `data` - Chunk data to validate

**Returns:** `Result<(), ChunkError>`

**Errors:**
- `DataChecksumMismatch` - If the calculated checksum doesn't match the header
- `EmptyChunkData` - If the data is empty

#### `calculate_checksum()`

Calculates CRC32 checksum of data.

```rust
pub fn calculate_checksum(data: &[u8]) -> u32
```

**Parameters:**
- `data` - Data to calculate checksum for

**Returns:** `u32` - CRC32 checksum

## Usage Examples

### Creating and Writing a Chunk

```rust
use uuid::Uuid;
use wormfs::chunk_format::*;
use std::io::Cursor;

// Create a new chunk header
let header = ChunkHeader::new(
    Uuid::new_v4(),           // chunk_id
    Uuid::new_v4(),           // stripe_id  
    Uuid::new_v4(),           // file_id
    0,                        // stripe_start_offset
    1024,                     // stripe_end_offset
    0,                        // chunk_index
    4,                        // data_shards
    2,                        // parity_shards
    0x12345678,              // stripe_checksum
    CompressionAlgorithm::None, // compression_algorithm
)?;

// Chunk data
let data = b"Hello, WormFS! This is chunk data.";

// Write chunk to buffer
let mut buffer = Vec::new();
write_chunk(&mut buffer, header, data)?;
```

### Reading and Validating a Chunk

```rust
use std::io::Cursor;

// Read chunk from buffer
let mut cursor = Cursor::new(buffer);
let (header, data) = read_chunk(&mut cursor)?;

// Validate chunk integrity
validate_chunk(&header, &data)?;

println!("Chunk ID: {}", header.chunk_id);
println!("Data size: {} bytes", data.len());
```

### Header Serialization

```rust
// Serialize header
let header_bytes = header.serialize()?;

// Deserialize header
let restored_header = ChunkHeader::deserialize(&header_bytes)?;

assert_eq!(header, restored_header);
```

## Performance Characteristics

Based on benchmarks:

- **Header serialization**: ~100ns per operation
- **Header deserialization**: ~200ns per operation  
- **CRC32 checksum calculation**: ~1GB/s throughput
- **Chunk write/read roundtrip**: Scales linearly with data size

## Error Handling

All functions return `Result` types with specific error variants. Common error handling patterns:

```rust
match write_chunk(&mut writer, header, data) {
    Ok(()) => println!("Chunk written successfully"),
    Err(ChunkError::EmptyChunkData) => eprintln!("Cannot write empty chunk"),
    Err(ChunkError::Io(e)) => eprintln!("IO error: {}", e),
    Err(e) => eprintln!("Other error: {}", e),
}
```

## Thread Safety

All types in this module are `Send + Sync` and can be safely used across threads. The functions are stateless and thread-safe.

## Future Compatibility

The header format includes:
- Version field for format evolution
- Reserved bytes for future expansion
- Extensible compression algorithm enum

This ensures backward compatibility as the format evolves.
