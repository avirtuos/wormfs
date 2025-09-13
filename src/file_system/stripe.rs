//! WorkStripe: represent a stripe of a file and produce chunk headers + payloads via erasure encoding.

use std::io;

use crate::file_system::chunk::ChunkHeader;
use crate::erasure;

/// WorkStripe carries the metadata about a stripe and can encode stripe bytes into chunk payloads.
#[derive(Debug, Clone)]
pub struct WorkStripe {
    pub file_path: String,
    pub index: u64,
    pub offset: u64,
    pub len: usize, // logical unpadded length of this stripe
    pub data_shards: usize,
    pub parity_shards: usize,
}

impl WorkStripe {
    /// Create a new WorkStripe describing this stripe.
    pub fn new(
        file_path: String,
        index: u64,
        offset: u64,
        len: usize,
        data_shards: usize,
        parity_shards: usize,
    ) -> Self {
        Self {
            file_path,
            index,
            offset,
            len,
            data_shards,
            parity_shards,
        }
    }

    /// Encode stripe bytes into N = data_shards + parity_shards shards.
    ///
    /// Returns a Vec of (ChunkHeader, payload_bytes) ready to be written to disk.
    pub fn encode(&self, stripe_data: &[u8]) -> io::Result<Vec<(ChunkHeader, Vec<u8>)>> {
        // Use the project erasure module (reed-solomon-novelpoly wrapper)
        let shards = erasure::encode_with_parity(stripe_data, self.data_shards, self.parity_shards)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("encode error: {}", e)))?;

        let mut out = Vec::with_capacity(shards.len());
        for (i, shard) in shards.into_iter().enumerate() {
            let hdr = ChunkHeader {
                file_path: self.file_path.clone(),
                stripe_index: self.index,
                stripe_offset: self.offset,
                stripe_len: self.len as u64,
                chunk_index: i as u16,
                data_shards: self.data_shards as u16,
                parity_shards: self.parity_shards as u16,
                payload_len: shard.len() as u32,
            };
            out.push((hdr, shard));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stripe_encode_produces_expected_shard_count() {
        let data = vec![0u8; 1024];
        let s = WorkStripe::new("/a/b".to_string(), 0, 0, data.len(), 3, 2);
        let shards = s.encode(&data).expect("encode");
        assert_eq!(shards.len(), 5);
        for (hdr, payload) in shards {
            assert_eq!(hdr.payload_len as usize, payload.len());
            assert_eq!(hdr.stripe_len as usize, data.len());
        }
    }
}
