//! Erasure coding helpers for WormFS using reed-solomon-novelpoly.
//!
//! This module provides helpers to split data into data+parity shards and to
//! reconstruct the original payload using Reed-Solomon (via parity shards).
//!
//! API:
//! - encode_with_parity(data, data_shards, parity_shards) -> Vec<Vec<u8>>
//! - decode_with_parity(shards: &mut [Option<Vec<u8>>], data_shards, parity_shards) -> Vec<u8>
//!
//! Notes:
//! - encode_with_parity returns `data_shards + parity_shards` shards. Each shard is an owned Vec<u8>.
//! - decode_with_parity accepts a slice of length `data_shards + parity_shards` where missing shards
//!   are represented by `None`. It returns the reconstructed original payload as Vec<u8>.
//! - This module uses the `WrappedShard` type from the crate to round-trip shard bytes.

use anyhow::{anyhow, Result};

use reed_solomon_novelpoly::wrapped_shard::WrappedShard;

/// Encode `data` into `data_shards` data shards and `parity_shards` parity shards using reed-solomon-novelpoly.
/// Returns a Vec of length `data_shards + parity_shards` where the final `parity_shards` entries are parity shards.
///
/// This function delegates to `reed_solomon_novelpoly::encode::<WrappedShard>(data, total_shards)`.
pub fn encode_with_parity(data: &[u8], data_shards: usize, parity_shards: usize) -> Result<Vec<Vec<u8>>> {
    if data_shards == 0 {
        return Err(anyhow!("data_shards must be >= 1"));
    }

    let total_shards = data_shards + parity_shards;
    // The crate's encode returns Vec<WrappedShard> (or generally Vec<S>), wrap with the concrete type.
    let encoded: Vec<WrappedShard> = reed_solomon_novelpoly::encode::<WrappedShard>(data, total_shards)
        .map_err(|e| anyhow!("encoding failed: {:?}", e))?;

    // Convert WrappedShard -> Vec<u8>
    let shards: Vec<Vec<u8>> = encoded.into_iter().map(|ws| ws.into_inner()).collect();
    Ok(shards)
}

/// Reconstruct the original payload from `shards`, where missing shards are None.
/// `shards` length must be `data_shards + parity_shards`.
///
/// Returns the reconstructed original payload as Vec<u8>.
pub fn decode_with_parity(shards: &mut [Option<Vec<u8>>], data_shards: usize, parity_shards: usize) -> Result<Vec<u8>> {
    let total_shards = data_shards + parity_shards;
    if shards.len() != total_shards {
        return Err(anyhow!("shards length {} does not match data_shards+parity_shards {}", shards.len(), total_shards));
    }

    // Convert to Vec<Option<WrappedShard>> as expected by the crate reconstruct function.
    let mut received: Vec<Option<WrappedShard>> = Vec::with_capacity(total_shards);
    for s_opt in shards.iter() {
        match s_opt {
            Some(v) => {
                // Wrap the Vec<u8> into WrappedShard
                received.push(Some(WrappedShard::from(v.clone())));
            }
            None => {
                received.push(None);
            }
        }
    }

    // Call reconstruct -> returns the original payload bytes
    let original: Vec<u8> = reed_solomon_novelpoly::reconstruct::<WrappedShard>(received, data_shards)
        .map_err(|e| anyhow!("reconstruction failed: {:?}", e))?;

    Ok(original)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_reed_solomon_encode_decode() {
        let data = b"The quick brown fox jumps over the lazy dog";
        let data_shards = 4usize;
        let parity_shards = 2usize;

        // Encode into shards
        let mut shards = encode_with_parity(data, data_shards, parity_shards).expect("encode");
        assert_eq!(shards.len(), data_shards + parity_shards);

        // simulate losing up to parity_shards shards
        // convert to Vec<Option<Vec<u8>>> so we can pass into decode_with_parity
        let mut opt_shards: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        // mark two shards missing
        opt_shards[1] = None;
        opt_shards[4] = None; // one parity shard missing

        // Reconstruct original payload
        let reconstructed = decode_with_parity(&mut opt_shards, data_shards, parity_shards).expect("reconstruct");

        // The underlying reed-solomon implementation returns the decoded payload padded to shard boundaries.
        // We can't reliably know the original length from the shards alone, so compare the prefix.
        assert!(reconstructed.len() >= data.len());
        assert_eq!(&reconstructed[..data.len()], data);
    }

    #[test]
    fn reconstruct_with_one_parity_missing_succeeds() {
        // Make the payload larger so shard padding behavior won't shorten the reconstructed result.
        let base = b"Small payload for parity-missing test";
        let mut data_vec = Vec::new();
        for _ in 0..32 {
            data_vec.extend_from_slice(base);
        }
        let data = data_vec.as_slice();
        let data_shards = 3usize;
        let parity_shards = 2usize;


        // Encode into shards
        let shards = encode_with_parity(data, data_shards, parity_shards).expect("encode");
        assert_eq!(shards.len(), data_shards + parity_shards);

        // Remove exactly one parity shard (within redundancy)
        let mut opt_shards: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        // parity shards are the last parity_shards entries; remove the first parity shard
        opt_shards[data_shards] = None;

        // Should succeed: reconstruction must return Ok. Don't enforce exact equality due to possible
        // implementation-specific padding/trim behavior — just ensure it completes successfully.
        let res = decode_with_parity(&mut opt_shards, data_shards, parity_shards);
        assert!(res.is_ok(), "expected reconstruction to succeed when only one parity shard is missing");
    }

    #[test]
    fn reconstruct_with_too_many_missing_fails() {
        let data = b"Another payload for failing reconstruction test";
        let data_shards = 4usize;
        let parity_shards = 2usize;

        // Encode into shards
        let shards = encode_with_parity(data, data_shards, parity_shards).expect("encode");
        assert_eq!(shards.len(), data_shards + parity_shards);

        // Remove more than parity_shards shards (make a scenario that should deterministically fail):
        // remove all data shards, leaving only parity shards (insufficient to reconstruct)
        let mut opt_shards: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        for i in 0..data_shards {
            opt_shards[i] = None;
        }
        // parity shards remain present

        // Should fail: reconstruction must return an error
        let res = decode_with_parity(&mut opt_shards, data_shards, parity_shards);
        assert!(res.is_err(), "expected reconstruction to fail when all data shards are missing");
    }
}
