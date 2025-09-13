//! Chunk file header and I/O helpers.
//!
//! Chunk file layout (little-endian):
//! - magic: [u8;4] = b"WCK1"
//! - version: u16
//! - path_len: u32
//! - stripe_index: u64
//! - stripe_offset: u64
//! - stripe_len: u64
//! - chunk_index: u16
//! - data_shards: u16
//! - parity_shards: u16
//! - payload_len: u32
//! - reserved: [u8;8]
//! - file_path: [u8; path_len]
//! - payload: [u8; payload_len]
//!
//! Utilities:
//! - ChunkHeader struct for in-memory representation
//! - write_chunk(header, payload) -> PathBuf
//! - read_chunk_header(&mut File) -> ChunkHeader
//! - read_chunk_full(path) -> (ChunkHeader, Vec<u8>)
//! - chunk_path_for(header, chunk_root) -> PathBuf (hashed filename with directory fanout)

use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// Fixed magic for chunk files
const CHUNK_MAGIC: &[u8; 4] = b"WCK1";
const CHUNK_VERSION: u16 = 1;

/// Header that lives at the start of every chunk file (excluding the variable-length path bytes).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkHeader {
    pub file_path: String,   // logical file path this chunk belongs to (UTF-8)
    pub stripe_index: u64,   // stripe index (0-based)
    pub stripe_offset: u64,  // byte offset within the original file for this stripe
    pub stripe_len: u64,     // logical (unpadded) length of this stripe in bytes
    pub chunk_index: u16,    // index within the shards [0..N)
    pub data_shards: u16,
    pub parity_shards: u16,
    pub payload_len: u32,    // number of bytes following header for the shard payload
}

impl fmt::Display for ChunkHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ChunkHeader(path={}, stripe={} offset={} stripe_len={} chunk={} data={} parity={} payload_len={})",
            self.file_path,
            self.stripe_index,
            self.stripe_offset,
            self.stripe_len,
            self.chunk_index,
            self.data_shards,
            self.parity_shards,
            self.payload_len
        )
    }
}

/// Serialize a header to a writer (excluding the payload). Uses little-endian encoding.
fn write_header_to<W: Write>(mut w: W, hdr: &ChunkHeader) -> io::Result<()> {
    // magic
    w.write_all(CHUNK_MAGIC)?;
    // version
    w.write_all(&CHUNK_VERSION.to_le_bytes())?;
    // path_len
    let path_bytes = hdr.file_path.as_bytes();
    let path_len = path_bytes.len() as u32;
    w.write_all(&path_len.to_le_bytes())?;
    // stripe_index
    w.write_all(&hdr.stripe_index.to_le_bytes())?;
    // stripe_offset
    w.write_all(&hdr.stripe_offset.to_le_bytes())?;
    // stripe_len
    w.write_all(&hdr.stripe_len.to_le_bytes())?;
    // chunk_index
    w.write_all(&hdr.chunk_index.to_le_bytes())?;
    // data_shards
    w.write_all(&hdr.data_shards.to_le_bytes())?;
    // parity_shards
    w.write_all(&hdr.parity_shards.to_le_bytes())?;
    // payload_len
    w.write_all(&hdr.payload_len.to_le_bytes())?;
    // reserved (8 bytes)
    w.write_all(&[0u8; 8])?;
    // path bytes
    w.write_all(path_bytes)?;
    Ok(())
}

/// Read a header from a reader positioned at the start of a chunk file.
fn read_header_from<R: Read + Seek>(mut r: R) -> io::Result<ChunkHeader> {
    // Read fixed-size prefix up to path_len
    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)?;
    if &magic != CHUNK_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid chunk magic"));
    }
    let mut vbuf = [0u8; 2];
    r.read_exact(&mut vbuf)?;
    let _version = u16::from_le_bytes(vbuf);
    // path_len
    let mut u32buf = [0u8; 4];
    r.read_exact(&mut u32buf)?;
    let path_len = u32::from_le_bytes(u32buf) as usize;
    // stripe_index
    let mut u64buf = [0u8; 8];
    r.read_exact(&mut u64buf)?;
    let stripe_index = u64::from_le_bytes(u64buf);
    // stripe_offset
    r.read_exact(&mut u64buf)?;
    let stripe_offset = u64::from_le_bytes(u64buf);
    // stripe_len
    r.read_exact(&mut u64buf)?;
    let stripe_len = u64::from_le_bytes(u64buf);
    // chunk_index
    let mut u16buf = [0u8; 2];
    r.read_exact(&mut u16buf)?;
    let chunk_index = u16::from_le_bytes(u16buf);
    // data_shards
    r.read_exact(&mut u16buf)?;
    let data_shards = u16::from_le_bytes(u16buf);
    // parity_shards
    r.read_exact(&mut u16buf)?;
    let parity_shards = u16::from_le_bytes(u16buf);
    // payload_len
    r.read_exact(&mut u32buf)?;
    let payload_len = u32::from_le_bytes(u32buf);
    // reserved
    let mut _reserved = [0u8; 8];
    r.read_exact(&mut _reserved)?;
    // path bytes
    let mut path_bytes = vec![0u8; path_len];
    if path_len > 0 {
        r.read_exact(&mut path_bytes)?;
    }
    let file_path = String::from_utf8(path_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "path is not valid utf8"))?;

    Ok(ChunkHeader {
        file_path,
        stripe_index,
        stripe_offset,
        stripe_len,
        chunk_index,
        data_shards,
        parity_shards,
        payload_len,
    })
}

/// Write a chunk file under `chunk_root`. Returns the path written.
///
/// The filename is deterministic based on the header.file_path (sha256-based directory fanout),
/// stripe_index and chunk_index. Directories are created as needed. Existing files will be overwritten.
pub fn write_chunk(chunk_root: &Path, header: &ChunkHeader, payload: &[u8]) -> io::Result<PathBuf> {
    let path = chunk_path_for(header, chunk_root);

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut f = File::create(&path)?;
    write_header_to(&mut f, header)?;
    f.write_all(payload)?;
    f.sync_all()?;
    Ok(path)
}

/// Read only the header from an open file (file cursor must be at start). The file is left positioned
/// immediately after the header (start of payload).
pub fn read_chunk_header(f: &mut File) -> io::Result<ChunkHeader> {
    f.seek(SeekFrom::Start(0))?;
    read_header_from(f)
}

/// Read full chunk (header + payload) from path.
pub fn read_chunk_full(path: &Path) -> io::Result<(ChunkHeader, Vec<u8>)> {
    let mut f = File::open(path)?;
    f.seek(SeekFrom::Start(0))?;
    let hdr = read_header_from(&mut f)?;
    let mut payload = vec![0u8; hdr.payload_len as usize];
    f.read_exact(&mut payload)?;
    Ok((hdr, payload))
}

/// Compute a deterministic chunk file path for a header under `chunk_root`.
///
/// Naming scheme:
/// - hash = sha256(file_path) hex
/// - use first 4 bytes for two-level fanout directories: h0h1 / h2h3
/// - filename: {hash_short}-{stripe_index:08}-{chunk_index:04}.wck
pub fn chunk_path_for(header: &ChunkHeader, chunk_root: &Path) -> PathBuf {
    let mut hasher = Sha256::new();
    hasher.update(header.file_path.as_bytes());
    let digest = hasher.finalize();
    // hex of full digest
    let hex: String = digest.iter().map(|b| format!("{:02x}", b)).collect();
    // short components
    let dir1 = &hex[0..2];
    let dir2 = &hex[2..4];
    let short = &hex[..16]; // first 8 bytes hex (16 chars)

    let filename = format!("{}-{:08}-{:04}.wck", short, header.stripe_index, header.chunk_index);
    chunk_root.join(dir1).join(dir2).join(filename)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;

    #[test]
    fn header_roundtrip_and_io() {
        let tmp = env::temp_dir().join("wormfs-chunk-test");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();

        let hdr = ChunkHeader {
            file_path: "/some/long/path/file.bin".to_string(),
            stripe_index: 5,
            stripe_offset: 4096,
            stripe_len: 4096,
            chunk_index: 2,
            data_shards: 3,
            parity_shards: 2,
            payload_len: 128,
        };

        let payload = vec![0x5Au8; hdr.payload_len as usize];
        let p = write_chunk(&tmp, &hdr, &payload).expect("write_chunk");
        let (hdr2, payload2) = read_chunk_full(&p).expect("read_chunk_full");
        assert_eq!(hdr, hdr2);
        assert_eq!(payload, payload2);

        let _ = fs::remove_dir_all(&tmp);
    }
}
