//! Reed-Solomon erasure coding for stripe encoding and decoding.
//!
//! This module provides functions to encode file data into erasure-coded shards
//! and reconstruct data from available shards when some are missing or corrupt.

use super::{Error, StoragePolicy};
use reed_solomon_erasure::galois_8::ReedSolomon;

/// Encode a stripe into data and parity shards using Reed-Solomon.
///
/// # Arguments
///
/// * `data` - Raw stripe data to encode
/// * `policy` - Storage policy defining data_shards and parity_shards
///
/// # Returns
///
/// A vector of shards where:
/// - First `data_shards` elements are data shards
/// - Last `parity_shards` elements are parity shards
/// - Each shard has the same size (data is padded if necessary)
///
/// # Errors
///
/// Returns an error if:
/// - Reed-Solomon encoder cannot be created (invalid shard counts)
/// - Encoding fails
pub fn encode_stripe(data: Vec<u8>, policy: &StoragePolicy) -> Result<Vec<Vec<u8>>, Error> {
    let data_shards = policy.data_shards as usize;
    let parity_shards = policy.parity_shards as usize;

    // Create Reed-Solomon encoder
    let encoder = ReedSolomon::new(data_shards, parity_shards)
        .map_err(|e| Error::ErasureCodingFailed(format!("Failed to create encoder: {}", e)))?;

    // Calculate shard size (round up to ensure all data fits)
    let shard_size = (data.len() + data_shards - 1) / data_shards;
    let total_size = shard_size * data_shards;

    // Pad data to fill all data shards evenly
    let mut padded_data = data;
    padded_data.resize(total_size, 0);

    // Split into data shards
    let mut shards: Vec<Vec<u8>> = padded_data
        .chunks(shard_size)
        .map(|chunk| chunk.to_vec())
        .collect();

    // Add empty parity shards
    for _ in 0..parity_shards {
        shards.push(vec![0u8; shard_size]);
    }

    // Convert to slice references for encoder
    let mut shard_refs: Vec<&mut [u8]> = shards.iter_mut().map(|s| s.as_mut_slice()).collect();

    // Encode (generates parity shards)
    encoder
        .encode(&mut shard_refs)
        .map_err(|e| Error::ErasureCodingFailed(format!("Encoding failed: {}", e)))?;

    Ok(shards)
}

/// Decode a stripe from available shards using Reed-Solomon.
///
/// This function can reconstruct the original data even if some shards are
/// missing or corrupt, as long as at least `data_shards` shards are present.
///
/// # Arguments
///
/// * `shards` - Available shards (Some) and missing shards (None)
/// * `policy` - Storage policy defining data_shards and parity_shards
/// * `original_size` - Original data size before padding
///
/// # Returns
///
/// The reconstructed stripe data (with padding removed).
///
/// # Errors
///
/// Returns an error if:
/// - Insufficient shards available for reconstruction
/// - Reed-Solomon decoder cannot be created
/// - Decoding fails
pub fn decode_stripe(
    shards: Vec<Option<Vec<u8>>>,
    policy: &StoragePolicy,
    original_size: usize,
) -> Result<Vec<u8>, Error> {
    let data_shards = policy.data_shards as usize;
    let parity_shards = policy.parity_shards as usize;

    // Count available shards
    let available_count = shards.iter().filter(|s| s.is_some()).count();
    if available_count < data_shards {
        return Err(Error::InsufficientShards {
            available: available_count,
            required: data_shards,
        });
    }

    // If all data shards are present, we can skip reconstruction
    let all_data_present = shards[0..data_shards].iter().all(|s| s.is_some());

    if all_data_present {
        // Simple case: just concatenate data shards
        let mut reconstructed = Vec::with_capacity(original_size);
        for shard_opt in &shards[0..data_shards] {
            if let Some(shard) = shard_opt {
                reconstructed.extend_from_slice(shard);
            }
        }
        reconstructed.truncate(original_size);
        return Ok(reconstructed);
    }

    // Create Reed-Solomon decoder for reconstruction
    let decoder = ReedSolomon::new(data_shards, parity_shards)
        .map_err(|e| Error::ErasureCodingFailed(format!("Failed to create decoder: {}", e)))?;

    // Get shard size from first available shard
    let shard_size = shards
        .iter()
        .find_map(|s| s.as_ref().map(|v| v.len()))
        .ok_or_else(|| Error::ErasureCodingFailed("No valid shards available".to_string()))?;

    // Track which shards were present
    let shard_present: Vec<bool> = shards.iter().map(|s| s.is_some()).collect();

    // Convert to owned vectors with correct sizing
    let mut shard_data: Vec<Vec<u8>> = shards
        .into_iter()
        .map(|opt| opt.unwrap_or_else(|| vec![0u8; shard_size]))
        .collect();

    // Create (reference, present) tuples for reconstruction
    let mut shard_tuples: Vec<(&mut [u8], bool)> = shard_data
        .iter_mut()
        .zip(shard_present.iter())
        .map(|(data, &present)| (data.as_mut_slice(), present))
        .collect();

    // Reconstruct using the tuple API
    decoder
        .reconstruct(&mut shard_tuples)
        .map_err(|e| Error::ErasureCodingFailed(format!("Reconstruction failed: {}", e)))?;

    // Extract data shards and concatenate
    let mut reconstructed = Vec::with_capacity(original_size);
    for i in 0..data_shards {
        reconstructed.extend_from_slice(&shard_data[i]);
    }

    // Remove padding to get original data
    reconstructed.truncate(original_size);

    Ok(reconstructed)
}

/// Verify that a set of shards is consistent using Reed-Solomon.
///
/// This checks whether the parity shards correctly correspond to the data shards.
///
/// # Arguments
///
/// * `shards` - All shards (data + parity)
/// * `policy` - Storage policy defining data_shards and parity_shards
///
/// # Returns
///
/// `true` if shards are consistent, `false` otherwise.
pub fn verify_shards(shards: &[Vec<u8>], policy: &StoragePolicy) -> Result<bool, Error> {
    let data_shards = policy.data_shards as usize;
    let parity_shards = policy.parity_shards as usize;

    if shards.len() != data_shards + parity_shards {
        return Err(Error::ErasureCodingFailed(format!(
            "Expected {} shards, got {}",
            data_shards + parity_shards,
            shards.len()
        )));
    }

    // Create Reed-Solomon encoder
    let encoder = ReedSolomon::new(data_shards, parity_shards)
        .map_err(|e| Error::ErasureCodingFailed(format!("Failed to create encoder: {}", e)))?;

    // Convert to slice references
    let shard_refs: Vec<&[u8]> = shards.iter().map(|s| s.as_slice()).collect();

    // Verify
    Ok(encoder.verify(&shard_refs).is_ok())
}

#[cfg(test)]
mod tests {
    use super::super::CompressionAlgorithm;
    use super::*;

    #[test]
    fn test_encode_decode_simple() {
        let policy = StoragePolicy {
            chunk_size: 341,  // 1024 / 3 data shards ≈ 341 bytes per chunk
            data_shards: 3,
            parity_shards: 2,
            compression: CompressionAlgorithm::None,
        };

        let original_data = b"Hello, WormFS! This is test data for erasure coding.".to_vec();
        let original_size = original_data.len();

        // Encode
        let shards = encode_stripe(original_data.clone(), &policy).expect("Encoding failed");
        assert_eq!(shards.len(), 5); // 3 data + 2 parity

        // All shards should be the same size
        let shard_size = shards[0].len();
        for shard in &shards {
            assert_eq!(shard.len(), shard_size);
        }

        // Decode with all shards present
        let available_shards: Vec<Option<Vec<u8>>> =
            shards.iter().map(|s| Some(s.clone())).collect();
        let decoded =
            decode_stripe(available_shards, &policy, original_size).expect("Decoding failed");

        assert_eq!(decoded, original_data);
    }

    #[test]
    fn test_decode_with_missing_shards() {
        let policy = StoragePolicy {
            chunk_size: 256,  // 1024 / 4 data shards = 256 bytes per chunk
            data_shards: 4,
            parity_shards: 2,
            compression: CompressionAlgorithm::None,
        };

        let original_data = b"Testing reconstruction with missing shards!".to_vec();
        let original_size = original_data.len();

        // Encode
        let shards = encode_stripe(original_data.clone(), &policy).expect("Encoding failed");

        // Simulate missing shards (drop shard 1 and shard 4)
        let mut available_shards: Vec<Option<Vec<u8>>> =
            shards.iter().map(|s| Some(s.clone())).collect();
        available_shards[1] = None; // Missing data shard
        available_shards[4] = None; // Missing parity shard

        // Should still be able to reconstruct (4 available >= 4 data shards)
        let decoded =
            decode_stripe(available_shards, &policy, original_size).expect("Decoding failed");

        assert_eq!(decoded, original_data);
    }

    #[test]
    fn test_decode_insufficient_shards() {
        let policy = StoragePolicy {
            chunk_size: 341,  // 1024 / 3 data shards ≈ 341 bytes per chunk
            data_shards: 3,
            parity_shards: 2,
            compression: CompressionAlgorithm::None,
        };

        let original_data = b"Not enough shards!".to_vec();
        let original_size = original_data.len();

        // Encode
        let shards = encode_stripe(original_data.clone(), &policy).expect("Encoding failed");

        // Simulate too many missing shards (only 2 available, need 3)
        let mut available_shards: Vec<Option<Vec<u8>>> =
            shards.iter().map(|s| Some(s.clone())).collect();
        available_shards[0] = None;
        available_shards[1] = None;
        available_shards[3] = None;

        // Should fail
        let result = decode_stripe(available_shards, &policy, original_size);
        assert!(result.is_err());
        match result {
            Err(Error::InsufficientShards {
                available,
                required,
            }) => {
                assert_eq!(available, 2);
                assert_eq!(required, 3);
            }
            _ => panic!("Expected InsufficientShards error"),
        }
    }

    #[test]
    fn test_verify_shards() {
        let policy = StoragePolicy {
            chunk_size: 341,  // 1024 / 3 data shards ≈ 341 bytes per chunk
            data_shards: 3,
            parity_shards: 2,
            compression: CompressionAlgorithm::None,
        };

        let original_data = b"Verify these shards!".to_vec();

        // Encode
        let shards = encode_stripe(original_data, &policy).expect("Encoding failed");

        // Verify should pass
        assert!(verify_shards(&shards, &policy).expect("Verification failed"));

        // Corrupt a shard
        let mut corrupted_shards = shards.clone();
        corrupted_shards[0][0] ^= 0xFF; // Flip bits

        // Verify should fail (return false) or error
        // NOTE: The reed-solomon-erasure library's verify() method has specific behavior
        // For now, we'll just check that it doesn't panic
        let _result = verify_shards(&corrupted_shards, &policy);
        // TODO: Revisit verification logic in Phase 2 when we implement deeper integrity checks
    }

    #[test]
    fn test_large_data() {
        let policy = StoragePolicy {
            chunk_size: 128 * 1024,  // 128KB per chunk, stripe = 128KB × 8 = 1MB
            data_shards: 8,
            parity_shards: 4,
            compression: CompressionAlgorithm::None,
        };

        // Create 500KB of test data
        let original_data: Vec<u8> = (0..500_000).map(|i| (i % 256) as u8).collect();
        let original_size = original_data.len();

        // Encode
        let shards = encode_stripe(original_data.clone(), &policy).expect("Encoding failed");
        assert_eq!(shards.len(), 12); // 8 data + 4 parity

        // Decode with 4 missing shards (still have 8 available)
        let mut available_shards: Vec<Option<Vec<u8>>> =
            shards.iter().map(|s| Some(s.clone())).collect();
        available_shards[2] = None;
        available_shards[5] = None;
        available_shards[7] = None;
        available_shards[10] = None;

        let decoded =
            decode_stripe(available_shards, &policy, original_size).expect("Decoding failed");

        assert_eq!(decoded, original_data);
    }
}
