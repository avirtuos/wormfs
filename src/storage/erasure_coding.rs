//! Erasure Coding Module
//!
//! This module provides a WormFS-specific wrapper around the reed-solomon-erasure crate,
//! implementing stripe encoding and decoding operations with comprehensive error handling.

use reed_solomon_erasure::galois_8::ReedSolomon;
use reed_solomon_erasure::Error as ReedSolomonError;
use thiserror::Error;

/// Configuration for erasure coding operations
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ErasureCodingConfig {
    /// Number of data shards (k)
    pub data_shards: usize,
    /// Number of parity shards (m)
    pub parity_shards: usize,
    /// Target stripe size in bytes (will be padded to align with shard count)
    pub stripe_size: usize,
}

impl ErasureCodingConfig {
    /// Create a new erasure coding configuration
    pub fn new(
        data_shards: usize,
        parity_shards: usize,
        stripe_size: usize,
    ) -> Result<Self, ErasureError> {
        if data_shards == 0 {
            return Err(ErasureError::InvalidConfiguration {
                reason: "data_shards must be greater than 0".to_string(),
            });
        }

        if parity_shards == 0 {
            return Err(ErasureError::InvalidConfiguration {
                reason: "parity_shards must be greater than 0".to_string(),
            });
        }

        if data_shards + parity_shards > 255 {
            return Err(ErasureError::InvalidConfiguration {
                reason: format!(
                    "total shards ({}) cannot exceed 255",
                    data_shards + parity_shards
                ),
            });
        }

        if stripe_size == 0 {
            return Err(ErasureError::InvalidConfiguration {
                reason: "stripe_size must be greater than 0".to_string(),
            });
        }

        Ok(ErasureCodingConfig {
            data_shards,
            parity_shards,
            stripe_size,
        })
    }

    /// Get total number of shards (data + parity)
    pub fn total_shards(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    /// Calculate the size of each shard for a given stripe size
    pub fn shard_size(&self) -> usize {
        // Round up to ensure all data fits
        self.stripe_size.div_ceil(self.data_shards)
    }

    /// Calculate the padded stripe size (aligned to shard boundaries)
    pub fn padded_stripe_size(&self) -> usize {
        self.shard_size() * self.data_shards
    }

    /// Get common configuration presets
    pub fn preset_4_2() -> Result<Self, ErasureError> {
        Self::new(4, 2, 1024 * 1024) // 1MB stripes, 4+2 encoding
    }

    pub fn preset_6_3() -> Result<Self, ErasureError> {
        Self::new(6, 3, 2 * 1024 * 1024) // 2MB stripes, 6+3 encoding
    }

    pub fn preset_8_4() -> Result<Self, ErasureError> {
        Self::new(8, 4, 4 * 1024 * 1024) // 4MB stripes, 8+4 encoding
    }
}

/// Errors that can occur during erasure coding operations
#[derive(Error, Debug)]
pub enum ErasureError {
    #[error("Invalid configuration: {reason}")]
    InvalidConfiguration { reason: String },

    #[error("Insufficient chunks for reconstruction: need {needed}, have {available}")]
    InsufficientChunks { needed: usize, available: usize },

    #[error("Chunk index out of range: index {index}, max {max}")]
    ChunkIndexOutOfRange { index: usize, max: usize },

    #[error("Chunk size mismatch: expected {expected}, got {actual}")]
    ChunkSizeMismatch { expected: usize, actual: usize },

    #[error("Reed-Solomon error: {0}")]
    ReedSolomon(#[from] ReedSolomonError),

    #[error("Data corruption detected during reconstruction")]
    DataCorruption,

    #[error("Invalid data: {reason}")]
    InvalidData { reason: String },
}

/// Result of stripe encoding operation
#[derive(Debug, Clone)]
pub struct EncodedStripe {
    /// The chunk data for each shard (data shards first, then parity shards)
    pub shards: Vec<Vec<u8>>,
    /// Configuration used for encoding
    pub config: ErasureCodingConfig,
    /// Original data size (before padding)
    pub original_size: usize,
}

impl EncodedStripe {
    /// Get data shards (indices 0..data_shards)
    pub fn data_shards(&self) -> &[Vec<u8>] {
        &self.shards[..self.config.data_shards]
    }

    /// Get parity shards (indices data_shards..total_shards)
    pub fn parity_shards(&self) -> &[Vec<u8>] {
        &self.shards[self.config.data_shards..]
    }

    /// Get a specific shard by index
    pub fn get_shard(&self, index: usize) -> Result<&Vec<u8>, ErasureError> {
        if index >= self.shards.len() {
            return Err(ErasureError::ChunkIndexOutOfRange {
                index,
                max: self.shards.len() - 1,
            });
        }
        Ok(&self.shards[index])
    }
}

/// Encode a stripe of data into erasure coded chunks
pub fn encode_stripe(
    data: &[u8],
    config: &ErasureCodingConfig,
) -> Result<EncodedStripe, ErasureError> {
    if data.is_empty() {
        return Err(ErasureError::InvalidData {
            reason: "input data cannot be empty".to_string(),
        });
    }

    let original_size = data.len();
    let shard_size = config.shard_size();
    let padded_size = config.padded_stripe_size();

    // Create Reed-Solomon encoder
    let rs = ReedSolomon::new(config.data_shards, config.parity_shards)?;

    // Pad data to align with shard boundaries
    let mut padded_data = data.to_vec();
    padded_data.resize(padded_size, 0);

    // Split data into shards
    let mut shards: Vec<Vec<u8>> = Vec::with_capacity(config.total_shards());

    // Create data shards
    for i in 0..config.data_shards {
        let start = i * shard_size;
        let end = start + shard_size;
        shards.push(padded_data[start..end].to_vec());
    }

    // Create empty parity shards
    for _ in 0..config.parity_shards {
        shards.push(vec![0u8; shard_size]);
    }

    // Encode parity shards
    rs.encode(&mut shards)?;

    Ok(EncodedStripe {
        shards,
        config: config.clone(),
        original_size,
    })
}

/// Decode a stripe from available chunks (some may be missing)
pub fn decode_stripe(
    chunks: &[Option<Vec<u8>>],
    config: &ErasureCodingConfig,
) -> Result<Vec<u8>, ErasureError> {
    if chunks.len() != config.total_shards() {
        return Err(ErasureError::InvalidData {
            reason: format!(
                "chunks length ({}) doesn't match total shards ({})",
                chunks.len(),
                config.total_shards()
            ),
        });
    }

    // Count available chunks
    let available_count = chunks.iter().filter(|chunk| chunk.is_some()).count();
    if available_count < config.data_shards {
        return Err(ErasureError::InsufficientChunks {
            needed: config.data_shards,
            available: available_count,
        });
    }

    // Validate chunk sizes
    let expected_shard_size = config.shard_size();
    for data in chunks.iter().flatten() {
        if data.len() != expected_shard_size {
            return Err(ErasureError::ChunkSizeMismatch {
                expected: expected_shard_size,
                actual: data.len(),
            });
        }
    }

    // Create Reed-Solomon decoder
    let rs = ReedSolomon::new(config.data_shards, config.parity_shards)?;

    // Convert to format expected by reed-solomon-erasure
    let mut shards: Vec<Option<Vec<u8>>> = chunks.to_vec();

    // Attempt reconstruction
    rs.reconstruct(&mut shards)?;

    // Extract data from reconstructed shards
    let mut reconstructed_data = Vec::with_capacity(config.padded_stripe_size());
    for shard in shards.iter().take(config.data_shards) {
        if let Some(ref shard_data) = shard {
            reconstructed_data.extend_from_slice(shard_data);
        } else {
            return Err(ErasureError::DataCorruption);
        }
    }

    Ok(reconstructed_data)
}

/// Decode a stripe and trim to original size
pub fn decode_stripe_with_size(
    chunks: &[Option<Vec<u8>>],
    config: &ErasureCodingConfig,
    original_size: usize,
) -> Result<Vec<u8>, ErasureError> {
    let mut data = decode_stripe(chunks, config)?;

    if original_size > data.len() {
        return Err(ErasureError::InvalidData {
            reason: format!(
                "original_size ({}) exceeds reconstructed data length ({})",
                original_size,
                data.len()
            ),
        });
    }

    data.truncate(original_size);
    Ok(data)
}

/// Verify that chunks can be used for reconstruction without actually reconstructing
pub fn verify_chunks_availability(
    chunks: &[Option<Vec<u8>>],
    config: &ErasureCodingConfig,
) -> Result<(), ErasureError> {
    if chunks.len() != config.total_shards() {
        return Err(ErasureError::InvalidData {
            reason: format!(
                "chunks length ({}) doesn't match total shards ({})",
                chunks.len(),
                config.total_shards()
            ),
        });
    }

    let available_count = chunks.iter().filter(|chunk| chunk.is_some()).count();
    if available_count < config.data_shards {
        return Err(ErasureError::InsufficientChunks {
            needed: config.data_shards,
            available: available_count,
        });
    }

    Ok(())
}

/// Get indices of missing chunks
pub fn get_missing_chunk_indices(chunks: &[Option<Vec<u8>>]) -> Vec<usize> {
    chunks
        .iter()
        .enumerate()
        .filter_map(|(i, chunk)| if chunk.is_none() { Some(i) } else { None })
        .collect()
}

/// Get indices of available chunks
pub fn get_available_chunk_indices(chunks: &[Option<Vec<u8>>]) -> Vec<usize> {
    chunks
        .iter()
        .enumerate()
        .filter_map(|(i, chunk)| if chunk.is_some() { Some(i) } else { None })
        .collect()
}

/// Statistics about chunk availability
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkAvailabilityStats {
    pub total_chunks: usize,
    pub available_chunks: usize,
    pub missing_chunks: usize,
    pub data_chunks_available: usize,
    pub parity_chunks_available: usize,
    pub can_reconstruct: bool,
}

/// Analyze chunk availability for a stripe
pub fn analyze_chunk_availability(
    chunks: &[Option<Vec<u8>>],
    config: &ErasureCodingConfig,
) -> ChunkAvailabilityStats {
    let total_chunks = chunks.len();
    let available_chunks = chunks.iter().filter(|chunk| chunk.is_some()).count();
    let missing_chunks = total_chunks - available_chunks;

    let data_chunks_available = chunks[..config.data_shards.min(chunks.len())]
        .iter()
        .filter(|chunk| chunk.is_some())
        .count();

    let parity_start = config.data_shards.min(chunks.len());
    let parity_chunks_available = if parity_start < chunks.len() {
        chunks[parity_start..]
            .iter()
            .filter(|chunk| chunk.is_some())
            .count()
    } else {
        0
    };

    let can_reconstruct =
        available_chunks >= config.data_shards && total_chunks == config.total_shards();

    ChunkAvailabilityStats {
        total_chunks,
        available_chunks,
        missing_chunks,
        data_chunks_available,
        parity_chunks_available,
        can_reconstruct,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> ErasureCodingConfig {
        ErasureCodingConfig::new(4, 2, 1024).unwrap()
    }

    #[test]
    fn test_config_creation() {
        let config = create_test_config();
        assert_eq!(config.data_shards, 4);
        assert_eq!(config.parity_shards, 2);
        assert_eq!(config.total_shards(), 6);
        assert_eq!(config.shard_size(), 256); // 1024 / 4 = 256
    }

    #[test]
    fn test_config_validation() {
        // Test zero data shards
        assert!(ErasureCodingConfig::new(0, 2, 1024).is_err());

        // Test zero parity shards
        assert!(ErasureCodingConfig::new(4, 0, 1024).is_err());

        // Test too many total shards
        assert!(ErasureCodingConfig::new(200, 200, 1024).is_err());

        // Test zero stripe size
        assert!(ErasureCodingConfig::new(4, 2, 0).is_err());
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let config = create_test_config();
        let test_data = b"Hello, WormFS! This is test data for erasure coding.";

        // Encode
        let encoded = encode_stripe(test_data, &config).unwrap();
        assert_eq!(encoded.shards.len(), 6);
        assert_eq!(encoded.original_size, test_data.len());

        // Create chunks array (all chunks available)
        let chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();

        // Decode
        let decoded = decode_stripe_with_size(&chunks, &config, test_data.len()).unwrap();
        assert_eq!(decoded, test_data);
    }

    #[test]
    fn test_decode_with_missing_chunks() {
        let config = create_test_config();
        let test_data = b"Test data for missing chunk reconstruction.";

        // Encode
        let encoded = encode_stripe(test_data, &config).unwrap();

        // Create chunks with some missing (remove chunks 1 and 4)
        let mut chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();
        chunks[1] = None; // Missing data chunk
        chunks[4] = None; // Missing parity chunk

        // Should still be able to decode (have 4 out of 6 chunks, need 4)
        let decoded = decode_stripe_with_size(&chunks, &config, test_data.len()).unwrap();
        assert_eq!(decoded, test_data);
    }

    #[test]
    fn test_insufficient_chunks() {
        let config = create_test_config();
        let test_data = b"Test data for insufficient chunks.";

        // Encode
        let encoded = encode_stripe(test_data, &config).unwrap();

        // Create chunks with too many missing (only 3 available, need 4)
        let mut chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();
        chunks[0] = None;
        chunks[1] = None;
        chunks[2] = None;

        // Should fail to decode
        let result = decode_stripe(&chunks, &config);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ErasureError::InsufficientChunks { .. }
        ));
    }

    #[test]
    fn test_empty_data() {
        let config = create_test_config();
        let empty_data = b"";

        let result = encode_stripe(empty_data, &config);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ErasureError::InvalidData { .. }
        ));
    }

    #[test]
    fn test_chunk_availability_analysis() {
        let config = create_test_config();
        let test_data = b"Test data for availability analysis.";

        let encoded = encode_stripe(test_data, &config).unwrap();
        let mut chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();
        chunks[1] = None; // Remove one data chunk
        chunks[4] = None; // Remove one parity chunk

        let stats = analyze_chunk_availability(&chunks, &config);
        assert_eq!(stats.total_chunks, 6);
        assert_eq!(stats.available_chunks, 4);
        assert_eq!(stats.missing_chunks, 2);
        assert_eq!(stats.data_chunks_available, 3);
        assert_eq!(stats.parity_chunks_available, 1);
        assert!(stats.can_reconstruct);
    }

    #[test]
    fn test_get_missing_indices() {
        let chunks = vec![
            Some(vec![1, 2, 3]),
            None,
            Some(vec![4, 5, 6]),
            None,
            Some(vec![7, 8, 9]),
        ];

        let missing = get_missing_chunk_indices(&chunks);
        assert_eq!(missing, vec![1, 3]);

        let available = get_available_chunk_indices(&chunks);
        assert_eq!(available, vec![0, 2, 4]);
    }

    #[test]
    fn test_preset_configurations() {
        let config_4_2 = ErasureCodingConfig::preset_4_2().unwrap();
        assert_eq!(config_4_2.data_shards, 4);
        assert_eq!(config_4_2.parity_shards, 2);

        let config_6_3 = ErasureCodingConfig::preset_6_3().unwrap();
        assert_eq!(config_6_3.data_shards, 6);
        assert_eq!(config_6_3.parity_shards, 3);

        let config_8_4 = ErasureCodingConfig::preset_8_4().unwrap();
        assert_eq!(config_8_4.data_shards, 8);
        assert_eq!(config_8_4.parity_shards, 4);
    }

    #[test]
    fn test_large_data() {
        let config = ErasureCodingConfig::new(4, 2, 10000).unwrap(); // Use larger stripe size for 10KB data
        let large_data = vec![0xAB; 10000]; // 10KB of data

        let encoded = encode_stripe(&large_data, &config).unwrap();
        assert_eq!(encoded.original_size, 10000);

        // Verify we can reconstruct with all chunks
        let chunks: Vec<Option<Vec<u8>>> = encoded.shards.into_iter().map(Some).collect();
        let decoded = decode_stripe_with_size(&chunks, &config, 10000).unwrap();
        assert_eq!(decoded, large_data);
    }
}
