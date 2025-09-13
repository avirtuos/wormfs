//! WormFile: high-level representation of a logical file split into stripes and chunks.
//!
//! This provides streaming encode/decode helpers that read/write one stripe at a time to
//! bound memory usage. Chunks are persisted using the chunk module; the metadata header in
//! each chunk allows reconstructing file metadata by scanning chunk files.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::file_system::chunk::{chunk_path_for, read_chunk_full, read_chunk_header, write_chunk, ChunkHeader};
use crate::file_system::stripe::WorkStripe;

/// WormFile config and utilities.
///
/// - path: logical path within the fuse namespace (e.g., "/foo/bar")
/// - data_shards / parity_shards: erasure params
/// - stripe_size: number of bytes to process per stripe (caps memory)
/// - chunk_root: directory where chunk files are stored
#[derive(Debug, Clone)]
pub struct WormFile {
    pub path: String,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub stripe_size: usize,
    pub chunk_root: PathBuf,
}

impl WormFile {
    /// Create a new WormFile instance.
    pub fn new<P: Into<String>, Q: Into<PathBuf>>(
        path: P,
        data_shards: usize,
        parity_shards: usize,
        stripe_size: usize,
        chunk_root: Q,
    ) -> Self {
        Self {
            path: path.into(),
            data_shards,
            parity_shards,
            stripe_size,
            chunk_root: chunk_root.into(),
        }
    }

    /// Encode from any reader. This streams the input, encoding one stripe at a time and writing
    /// resulting chunk files to disk under chunk_root.
    ///
    /// This function overwrites existing chunk files for the same (file_path, stripe, chunk_index).
    pub fn encode_from_reader<R: Read>(&self, mut src: R) -> io::Result<()> {
        let mut index: u64 = 0;
        let mut offset: u64 = 0;

        loop {
            // read up to stripe_size bytes
            let mut buf = vec![0u8; self.stripe_size];
            let n = src.read(&mut buf)?;
            if n == 0 {
                break;
            }
            buf.truncate(n);
            let stripe = WorkStripe::new(
                self.path.clone(),
                index,
                offset,
                n,
                self.data_shards,
                self.parity_shards,
            );

            let encoded = stripe.encode(&buf).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("stripe encode failed: {}", e))
            })?;

            // write each shard as a chunk file
            for (hdr, payload) in encoded.into_iter() {
                let _p = write_chunk(&self.chunk_root, &hdr, &payload)?;
            }

            index += 1;
            offset += n as u64;
        }

        Ok(())
    }

    /// Helper: scan chunk_root recursively and return a mapping stripe_index -> Vec<ChunkHeader>
    /// for chunks that belong to this file (hdr.file_path == self.path).
    pub fn scan_chunks_for_file(&self) -> io::Result<HashMap<u64, Vec<ChunkHeader>>> {
        let mut map: HashMap<u64, Vec<ChunkHeader>> = HashMap::new();
        if !self.chunk_root.exists() {
            return Ok(map);
        }
        let mut stack = vec![self.chunk_root.clone()];
        while let Some(dir) = stack.pop() {
            if dir.is_dir() {
                for entry in fs::read_dir(&dir)? {
                    let ent = entry?;
                    let p = ent.path();
                    if p.is_dir() {
                        stack.push(p);
                        continue;
                    }
                    // try to open and read header; ignore errors (skip non-chunk files)
                    if let Ok(mut f) = File::open(&p) {
                        if let Ok(hdr) = read_chunk_header(&mut f) {
                            if hdr.file_path == self.path {
                                map.entry(hdr.stripe_index).or_default().push(hdr);
                            }
                        }
                    }
                }
            }
        }
        Ok(map)
    }

    /// Find available chunk file paths for a given stripe index by scanning chunk_root.
    /// Returns a Vec of paths to chunk files that belong to this file & stripe.
    pub fn available_chunks_for_stripe(&self, stripe_index: u64) -> io::Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        if !self.chunk_root.exists() {
            return Ok(out);
        }
        let mut stack = vec![self.chunk_root.clone()];
        while let Some(dir) = stack.pop() {
            if dir.is_dir() {
                for entry in fs::read_dir(&dir)? {
                    let ent = entry?;
                    let p = ent.path();
                    if p.is_dir() {
                        stack.push(p);
                        continue;
                    }
                    // read header and filter
                    if let Ok(mut f) = File::open(&p) {
                        if let Ok(hdr) = read_chunk_header(&mut f) {
                            if hdr.file_path == self.path && hdr.stripe_index == stripe_index {
                                out.push(p.clone());
                            }
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// Decode chunks and write the reconstructed file to `dst` (writer).
    ///
    /// This scans chunk_root for stripe 0,1,2,... and attempts to reconstruct each stripe.
    /// It stops when it finds no chunks for the next stripe index.
    pub fn decode_to_writer<W: Write>(&self, mut dst: W) -> io::Result<()> {
        let mut stripe_index: u64 = 0;

        loop {
            let available = self.available_chunks_for_stripe(stripe_index)?;
            if available.is_empty() {
                // no more stripes found; stop
                break;
            }

            // determine total shards count from any header
            let mut headers: Vec<ChunkHeader> = Vec::new();
            let mut total_shards_expected: usize = 0;
            let mut stripe_len_opt: Option<u64> = None;

            for p in available.iter() {
                let (hdr, payload) = read_chunk_full(p)?;
                // capture header and payload in memory—as we will feed to erasure decoder
                total_shards_expected = (hdr.data_shards as usize) + (hdr.parity_shards as usize);
                stripe_len_opt = Some(hdr.stripe_len);
                headers.push(ChunkHeader {
                    file_path: hdr.file_path,
                    stripe_index: hdr.stripe_index,
                    stripe_offset: hdr.stripe_offset,
                    stripe_len: hdr.stripe_len,
                    chunk_index: hdr.chunk_index,
                    data_shards: hdr.data_shards,
                    parity_shards: hdr.parity_shards,
                    payload_len: hdr.payload_len,
                });
            }

            if total_shards_expected == 0 {
                // malformed or no valid headers; stop
                break;
            }

            // Prepare shards vector with Option<Vec<u8>> sized to total_shards_expected
            let mut shards: Vec<Option<Vec<u8>>> = vec![None; total_shards_expected];

            // Re-open and read payloads to populate shards (we already read headers above,
            // but payloads were dropped—re-read to avoid storing both header+payload lists)
            for p in available.into_iter() {
                let (hdr, payload) = read_chunk_full(&p)?;
                let idx = hdr.chunk_index as usize;
                if idx < shards.len() {
                    shards[idx] = Some(payload);
                }
            }

            // Attempt reconstruction
            let data_shards = (headers[0].data_shards) as usize;
            let parity_shards = (headers[0].parity_shards) as usize;

            match crate::erasure::decode_with_parity(&mut shards, data_shards, parity_shards) {
                Ok(reconstructed) => {
                    // stripe_len tells how many original bytes to keep (unpadded length)
                    let stripe_len = stripe_len_opt.unwrap_or(0) as usize;
                    let write_len = std::cmp::min(reconstructed.len(), stripe_len);
                    dst.write_all(&reconstructed[..write_len])?;
                }
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("failed to reconstruct stripe {}: {}", stripe_index, e),
                    ));
                }
            }

            stripe_index += 1;
        }

        Ok(())
    }

    /// Scan chunk_root and reconstruct a light index for this WormFile: mapping stripe_index -> list of headers found.
    /// This can be used to rebuild metadata DB.
    pub fn scan_and_reconstruct_index(&self) -> io::Result<HashMap<u64, Vec<ChunkHeader>>> {
        self.scan_chunks_for_file()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;

    #[test]
    fn encode_then_decode_roundtrip() {
        let tmp = env::temp_dir().join("wormfs-file-test");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();

        let chunk_root = tmp.join("chunks");
        fs::create_dir_all(&chunk_root).unwrap();

        let wf = WormFile::new("/x/y".to_string(), 3, 2, 1024, chunk_root.clone());

        // prepare source data
        let data = vec![0xABu8; 3000];
        wf.encode_from_reader(&data[..]).expect("encode");

        // decode to buffer
        let mut out: Vec<u8> = Vec::new();
        wf.decode_to_writer(&mut out).expect("decode");

        // original data is prefix of out (stripe padding may create extra bytes)
        assert_eq!(&out[..data.len()], &data[..]);

        let _ = fs::remove_dir_all(&tmp);
    }
}
