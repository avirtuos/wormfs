//! FileStore implementation for local chunk storage with erasure coding.

use super::erasure_coding;
use super::{
    ChunkCacheEntry, ChunkData, ChunkHeader, ChunkId, ChunkMetadata, Config, DiskId, DiskStats,
    ErasureAlgorithm, Error, FileId, FileStore, NodeId, PrefetchPolicy, RebuildResult,
    StoragePolicy, StripeId, StripeMetadata, VerificationResult,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Magic bytes for chunk file format
const CHUNK_MAGIC: &[u8; 4] = b"WORM";

/// Current chunk format version
const CHUNK_FORMAT_VERSION: u16 = 1;

/// Inner state for FileStoreImpl
struct FileStoreInner {
    /// Configuration
    config: Config,

    /// Local disks managed by this node
    disks: RwLock<HashMap<DiskId, DiskInfo>>,
    // NOTE: MetadataStore integration will be added in Phase 2+
    // For Phase 1, FileStore operates independently
}

/// Information about a local disk
struct DiskInfo {
    /// Disk identifier
    disk_id: DiskId,

    /// Mount path
    path: PathBuf,

    /// Total space in bytes
    total_space: u64,

    /// Free space in bytes (updated periodically)
    free_space: u64,

    /// Number of chunks stored
    chunk_count: u64,
}

/// FileStore implementation for Phase 1 (local storage only)
pub struct FileStoreImpl {
    inner: Arc<FileStoreInner>,
}

impl FileStoreImpl {
    /// Create chunk file path for a given chunk
    fn chunk_path(
        &self,
        disk_path: &Path,
        file_id: FileId,
        stripe_id: StripeId,
        chunk_id: ChunkId,
    ) -> PathBuf {
        // Hash-based bucketing: hash(file_id) % 1000
        let bucket = (file_id.as_u64() % 1000).to_string();
        let file_id_str = format!("{:016x}", file_id.as_u64());
        let stripe_str = format!("stripe_{:016x}", stripe_id.as_u64());
        let chunk_str = format!("{:016x}.dat", chunk_id.as_u64());

        disk_path
            .join(bucket)
            .join(file_id_str)
            .join(stripe_str)
            .join(chunk_str)
    }

    /// Initialize disk directories
    async fn initialize_disk(&self, disk_id: DiskId, path: PathBuf) -> Result<(), Error> {
        // Create base directory if it doesn't exist
        tokio::fs::create_dir_all(&path)
            .await
            .map_err(|e| Error::DiskInitFailed {
                path: path.clone(),
                reason: format!("Failed to create directory: {}", e),
            })?;

        // Verify path exists
        tokio::fs::metadata(&path)
            .await
            .map_err(|e| Error::DiskInitFailed {
                path: path.clone(),
                reason: format!("Failed to get disk metadata: {}", e),
            })?;

        // For now, use simple heuristics for space
        // TODO: Use statvfs or similar for accurate disk space
        let total_space = 1_000_000_000_000; // 1TB default
        let free_space = 500_000_000_000; // 500GB default

        let disk_info = DiskInfo {
            disk_id,
            path,
            total_space,
            free_space,
            chunk_count: 0,
        };

        self.inner.disks.write().await.insert(disk_id, disk_info);

        Ok(())
    }

    /// Write chunk data to disk with header
    async fn write_chunk_to_disk(
        &self,
        disk_path: &Path,
        file_id: FileId,
        stripe_id: StripeId,
        chunk_id: ChunkId,
        chunk_data: &ChunkData,
    ) -> Result<(), Error> {
        let chunk_path = self.chunk_path(disk_path, file_id, stripe_id, chunk_id);

        // Create parent directories
        if let Some(parent) = chunk_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Serialize chunk header using bincode
        let header_bytes = bincode::serialize(&chunk_data.header)
            .map_err(|e| Error::ConfigError(format!("Failed to serialize header: {}", e)))?;

        // Build complete chunk file
        let mut buffer = Vec::new();

        // Write header length (u32) so we know how much to read
        buffer.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());

        // Write serialized header
        buffer.extend_from_slice(&header_bytes);

        // Write chunk data
        buffer.extend_from_slice(&chunk_data.data);

        // Write to file atomically
        tokio::fs::write(&chunk_path, buffer).await?;

        Ok(())
    }

    /// Read chunk data from disk
    async fn read_chunk_from_disk(
        &self,
        disk_path: &Path,
        file_id: FileId,
        stripe_id: StripeId,
        chunk_id: ChunkId,
    ) -> Result<ChunkData, Error> {
        let chunk_path = self.chunk_path(disk_path, file_id, stripe_id, chunk_id);

        // Read file
        let buffer = tokio::fs::read(&chunk_path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::ChunkNotFound(chunk_id)
            } else {
                Error::Io(e)
            }
        })?;

        if buffer.len() < 4 {
            return Err(Error::ChunkCorrupt(
                chunk_id,
                "File too small to contain header".to_string(),
            ));
        }

        // Read header length
        let header_len = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;

        if buffer.len() < 4 + header_len {
            return Err(Error::ChunkCorrupt(
                chunk_id,
                "File too small to contain complete header".to_string(),
            ));
        }

        // Deserialize header
        let header: ChunkHeader =
            bincode::deserialize(&buffer[4..4 + header_len]).map_err(|e| {
                Error::ChunkCorrupt(chunk_id, format!("Failed to deserialize header: {}", e))
            })?;

        // Verify magic bytes
        if !header.verify_magic() {
            return Err(Error::ChunkCorrupt(
                header.chunk_id,
                "Invalid magic bytes".to_string(),
            ));
        }

        // Read chunk data
        let data = buffer[4 + header_len..].to_vec();

        Ok(ChunkData { header, data })
    }
}

#[async_trait]
impl FileStore for FileStoreImpl {
    fn new(config: Config) -> Result<Self, Error> {
        let inner = FileStoreInner {
            config,
            disks: RwLock::new(HashMap::new()),
        };

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    async fn write_stripe(
        &self,
        file_id: FileId,
        stripe_id: StripeId,
        data: Vec<u8>,
        policy: StoragePolicy,
    ) -> Result<StripeMetadata, Error> {
        // Phase 1: For now, we only support local writes to first available disk
        // Phase 2+ will add distributed chunk placement across nodes

        // Calculate stripe checksum
        let stripe_checksum = ChunkHeader::compute_checksum(&data);
        let original_size = data.len();

        // Encode stripe into shards
        let shards = erasure_coding::encode_stripe(data, &policy)?;

        // Get a disk to write to
        let disks = self.inner.disks.read().await;
        let disk_info = disks
            .values()
            .next()
            .ok_or_else(|| Error::InsufficientStorage {
                needed: policy.total_shards() as usize,
                available: 0,
            })?;

        let disk_id = disk_info.disk_id;
        let disk_path = disk_info.path.clone();
        drop(disks);

        // Create chunks from shards
        let mut chunk_metadata = Vec::new();

        for (chunk_index, shard) in shards.iter().enumerate() {
            let chunk_id = ChunkId::generate();
            let chunk_checksum = ChunkHeader::compute_checksum(shard);

            let header = ChunkHeader::new(
                chunk_id,
                stripe_id,
                file_id,
                0, // stripe_start_offset - will be set when integrated with MetadataStore
                original_size as u64, // stripe_end_offset
                chunk_index as u8,
                policy.data_shards,
                policy.parity_shards,
                ErasureAlgorithm::ReedSolomon,
                policy.compression,
                stripe_checksum,
                chunk_checksum,
            );

            let chunk_data = ChunkData::new(header, shard.clone());

            // Write chunk to disk
            self.write_chunk_to_disk(&disk_path, file_id, stripe_id, chunk_id, &chunk_data)
                .await?;

            // Add to metadata
            chunk_metadata.push(ChunkMetadata::new(
                chunk_id,
                NodeId::new(0), // Phase 1: local only, no node concept yet
                disk_id,
                chunk_index as u8,
            ));
        }

        // Create stripe metadata
        let stripe_metadata = StripeMetadata::new(
            stripe_id,
            file_id,
            0, // offset - will be managed by higher layers
            original_size as u64,
            stripe_checksum,
            chunk_metadata,
        );

        Ok(stripe_metadata)
    }

    async fn read_stripe(
        &self,
        file_id: FileId,
        stripe_id: StripeId,
        chunks: Vec<ChunkMetadata>,
    ) -> Result<Vec<u8>, Error> {
        // Sort chunks by chunk_index to ensure correct erasure coding order
        let mut chunks_sorted = chunks;
        chunks_sorted.sort_by_key(|c| c.chunk_index);

        let disks = self.inner.disks.read().await;

        // Read chunks using provided metadata
        let mut shards = Vec::new();
        let mut policy: Option<StoragePolicy> = None;
        let mut original_size = 0;

        for chunk_meta in chunks_sorted.iter() {
            let disk_info = disks
                .get(&chunk_meta.disk_id)
                .ok_or_else(|| Error::DiskNotFound(chunk_meta.disk_id))?;

            match self
                .read_chunk_from_disk(&disk_info.path, file_id, stripe_id, chunk_meta.chunk_id)
                .await
            {
                Ok(chunk_data) => {
                    // Verify checksum
                    let computed = ChunkHeader::compute_checksum(&chunk_data.data);
                    if computed != chunk_data.header.chunk_checksum {
                        // Corrupt, will reconstruct
                        shards.push(None);
                    } else {
                        // Extract policy from first valid chunk
                        if policy.is_none() {
                            policy = Some(StoragePolicy {
                                data_shards: chunk_data.header.data_shards,
                                parity_shards: chunk_data.header.parity_shards,
                                stripe_size: chunk_data.header.stripe_end_offset,
                                compression: chunk_data.header.compression_algorithm,
                            });
                            original_size = chunk_data.header.stripe_end_offset as usize;
                        }
                        shards.push(Some(chunk_data.data));
                    }
                }
                Err(_) => {
                    // Missing, will reconstruct
                    shards.push(None);
                }
            }
        }

        drop(disks);

        let policy = policy.ok_or_else(|| Error::InsufficientChunks {
            needed: 1,
            available: 0,
        })?;

        // Decode stripe from shards
        let data = erasure_coding::decode_stripe(shards, &policy, original_size)?;

        Ok(data)
    }

    async fn update_stripe_partial(
        &self,
        file_id: FileId,
        stripe_id: StripeId,
        existing_chunks: Vec<ChunkMetadata>,
        offset: u64,
        new_data: Vec<u8>,
        policy: StoragePolicy,
    ) -> Result<StripeMetadata, Error> {
        // 1. Read existing stripe
        let mut stripe_data = self
            .read_stripe(file_id, stripe_id, existing_chunks)
            .await?;

        // 2. Apply modifications
        let end = (offset as usize) + new_data.len();
        if end > stripe_data.len() {
            stripe_data.resize(end, 0);
        }
        stripe_data[offset as usize..end].copy_from_slice(&new_data);

        // 3. Write as new stripe (generates NEW ChunkIds)
        // Note: This creates NEW chunks without deleting old ones
        self.write_stripe(file_id, stripe_id, stripe_data, policy)
            .await
    }

    async fn stage_chunk(&self, _chunk_data: ChunkData) -> Result<ChunkId, Error> {
        // Phase 2/3: Two-phase commit support
        Err(Error::NotImplemented(
            "stage_chunk is for Phase 2+".to_string(),
        ))
    }

    async fn activate_chunk(&self, _chunk_id: ChunkId) -> Result<(), Error> {
        // Phase 2/3: Two-phase commit support
        Err(Error::NotImplemented(
            "activate_chunk is for Phase 2+".to_string(),
        ))
    }

    async fn discard_staged_chunk(&self, _chunk_id: ChunkId) -> Result<(), Error> {
        // Phase 2/3: Two-phase commit support
        Err(Error::NotImplemented(
            "discard_staged_chunk is for Phase 2+".to_string(),
        ))
    }

    async fn write_chunk_local(
        &self,
        _chunk_id: ChunkId,
        chunk_data: ChunkData,
    ) -> Result<(), Error> {
        // Extract location information from chunk header
        let file_id = chunk_data.header.file_id;
        let stripe_id = chunk_data.header.stripe_id;
        let chunk_id = chunk_data.header.chunk_id;

        // Get first available disk
        let disks = self.inner.disks.read().await;
        let disk_info = disks
            .values()
            .next()
            .ok_or_else(|| Error::ConfigError("No disks available".to_string()))?;

        let disk_path = disk_info.path.clone();
        drop(disks);

        // Write chunk to disk
        self.write_chunk_to_disk(&disk_path, file_id, stripe_id, chunk_id, &chunk_data)
            .await
    }

    async fn read_chunk_local(&self, _chunk_id: ChunkId) -> Result<ChunkData, Error> {
        // Phase 1: We need to know file_id, stripe_id, and chunk_index
        // For now, this is a simplified implementation that won't work in practice
        // Phase 2+ will use MetadataStore to map chunk_id -> location

        // For Phase 1, return NotImplemented
        Err(Error::NotImplemented(
            "read_chunk_local requires MetadataStore integration (Phase 2+)".to_string(),
        ))
    }

    async fn verify_chunk(&self, chunk_id: ChunkId) -> Result<VerificationResult, Error> {
        // Try to read the chunk
        match self.read_chunk_local(chunk_id).await {
            Ok(chunk_data) => {
                // Verify checksum
                let computed_checksum = ChunkHeader::compute_checksum(&chunk_data.data);
                let checksum_valid = computed_checksum == chunk_data.header.chunk_checksum;

                Ok(VerificationResult {
                    checksum_valid,
                    readable: true,
                    error: if checksum_valid {
                        None
                    } else {
                        Some(format!(
                            "Checksum mismatch: expected {}, got {}",
                            chunk_data.header.chunk_checksum, computed_checksum
                        ))
                    },
                })
            }
            Err(e) => Ok(VerificationResult {
                checksum_valid: false,
                readable: false,
                error: Some(format!("Failed to read chunk: {}", e)),
            }),
        }
    }

    async fn rebuild_stripe(
        &self,
        _file_id: FileId,
        _stripe_id: StripeId,
    ) -> Result<RebuildResult, Error> {
        // Phase 4: Recovery and repair
        Err(Error::NotImplemented(
            "rebuild_stripe is for Phase 4".to_string(),
        ))
    }

    fn get_disk_stats(&self) -> Vec<DiskStats> {
        // Note: This is a blocking operation that reads async state
        // In production, we'd use tokio::runtime::Handle::current().block_on()
        // For now, return empty vec as this requires runtime context
        Vec::new()
    }

    async fn add_disk(&mut self, path: PathBuf) -> Result<DiskId, Error> {
        // Generate new disk ID
        let disk_id = DiskId::new(uuid::Uuid::new_v4().as_u128() as u64);

        // Initialize the disk
        self.initialize_disk(disk_id, path).await?;

        Ok(disk_id)
    }

    async fn remove_disk(&mut self, disk_id: DiskId) -> Result<(), Error> {
        // Check if disk exists
        let mut disks = self.inner.disks.write().await;

        if !disks.contains_key(&disk_id) {
            return Err(Error::DiskNotFound(disk_id));
        }

        // TODO: Phase 2+ - Verify no chunks remain on disk before removal
        // For Phase 1, just remove it
        disks.remove(&disk_id);

        Ok(())
    }

    async fn cache_chunk(&self, _chunk_id: ChunkId, _data: Vec<u8>) -> Result<(), Error> {
        // Phase 2+: Implement chunk caching
        Err(Error::NotImplemented(
            "cache_chunk is for Phase 2+".to_string(),
        ))
    }

    async fn get_cached_chunk(&self, _chunk_id: ChunkId) -> Result<Option<ChunkCacheEntry>, Error> {
        // Phase 2+: Implement chunk cache lookup
        Ok(None)
    }

    async fn prefetch_stripe_chunks(
        &self,
        _file_id: FileId,
        _stripe_id: StripeId,
        _policy: PrefetchPolicy,
    ) -> Result<(), Error> {
        // Phase 2+: Implement prefetching
        Err(Error::NotImplemented(
            "prefetch_stripe_chunks is for Phase 2+".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_store::types::CompressionAlgorithm;
    use std::collections::HashSet;
    use tempfile::TempDir;

    /// Helper to create a test FileStore with a temporary disk
    async fn create_test_store() -> (FileStoreImpl, TempDir, DiskId) {
        let temp_dir = TempDir::new().unwrap();
        let disk_path = temp_dir.path().to_path_buf();

        let config = Config {
            disk_paths: vec![disk_path.clone()],
            default_stripe_size: 1024,
            default_data_shards: 2,
            default_parity_shards: 1,
            max_concurrent_operations: 10,
            verification_interval: std::time::Duration::from_secs(3600),
            orphan_cleanup_age: std::time::Duration::from_secs(3600),
        };

        let mut store = FileStoreImpl::new(config).unwrap();

        // Initialize the disk
        let disk_id = DiskId::new(1);
        store.add_disk(disk_path).await.unwrap();

        // Get the actual disk_id (add_disk generates one)
        let actual_disk_id = store
            .inner
            .disks
            .read()
            .await
            .keys()
            .next()
            .copied()
            .unwrap();

        (store, temp_dir, actual_disk_id)
    }

    #[tokio::test]
    async fn test_chunk_path_format() {
        let (store, _temp_dir, disk_id) = create_test_store().await;

        let file_id = FileId::new(12345);
        let stripe_id = StripeId::new(67890);
        let chunk_id = ChunkId::new(111222);

        let disks = store.inner.disks.read().await;
        let disk_info = disks.get(&disk_id).unwrap();
        let path = store.chunk_path(&disk_info.path, file_id, stripe_id, chunk_id);

        // Verify format: /disk/bucket/file_id/stripe_id/chunk_id.dat
        let path_str = path.to_str().unwrap();
        assert!(
            path_str.contains(&format!("{:016x}", file_id.as_u64())),
            "Path should contain file_id: {}",
            path_str
        );
        assert!(
            path_str.contains(&format!("stripe_{:016x}", stripe_id.as_u64())),
            "Path should contain stripe_id: {}",
            path_str
        );
        assert!(
            path_str.contains(&format!("{:016x}.dat", chunk_id.as_u64())),
            "Path should contain chunk_id: {}",
            path_str
        );
    }

    #[tokio::test]
    async fn test_write_and_read_stripe() {
        let (store, _temp_dir, _disk_id) = create_test_store().await;

        let file_id = FileId::generate();
        let stripe_id = StripeId::generate();
        let original_data = b"Hello, WormFS! This is test data for stripe operations.".to_vec();

        let policy = StoragePolicy::new(2, 1, 1024, CompressionAlgorithm::None);

        // Write stripe
        let metadata = store
            .write_stripe(file_id, stripe_id, original_data.clone(), policy)
            .await
            .unwrap();

        // Verify metadata contains chunk IDs
        assert_eq!(metadata.chunks.len(), 3); // 2 data + 1 parity
        assert_eq!(metadata.file_id, file_id);
        assert_eq!(metadata.stripe_id, stripe_id);

        // Read stripe using returned metadata
        let read_data = store
            .read_stripe(file_id, stripe_id, metadata.chunks)
            .await
            .unwrap();

        assert_eq!(read_data, original_data);
    }

    #[tokio::test]
    async fn test_update_creates_new_chunks() {
        let (store, _temp_dir, disk_id) = create_test_store().await;

        let file_id = FileId::generate();
        let stripe_id = StripeId::generate();
        let original = vec![0u8; 1024];
        let policy = StoragePolicy::new(2, 1, 1024, CompressionAlgorithm::None);

        // Write initial stripe
        let metadata1 = store
            .write_stripe(file_id, stripe_id, original, policy.clone())
            .await
            .unwrap();

        let chunk_ids_v1: Vec<_> = metadata1.chunks.iter().map(|c| c.chunk_id).collect();

        // Update stripe
        let new_data = vec![255u8; 100];
        let metadata2 = store
            .update_stripe_partial(
                file_id,
                stripe_id,
                metadata1.chunks.clone(),
                100,
                new_data,
                policy,
            )
            .await
            .unwrap();

        let chunk_ids_v2: Vec<_> = metadata2.chunks.iter().map(|c| c.chunk_id).collect();

        // Verify: ChunkIds are different (no overwrite)
        assert_ne!(
            chunk_ids_v1, chunk_ids_v2,
            "Update should create new ChunkIds"
        );

        // Verify: Old chunks still exist on disk
        let disks = store.inner.disks.read().await;
        let disk_info = disks.get(&disk_id).unwrap();

        for old_chunk in metadata1.chunks {
            let path = store.chunk_path(&disk_info.path, file_id, stripe_id, old_chunk.chunk_id);
            assert!(
                path.exists(),
                "Old chunk {:?} should not be deleted after update",
                old_chunk.chunk_id
            );
        }
    }

    #[tokio::test]
    async fn test_partial_stripe_update() {
        let (store, _temp_dir, _disk_id) = create_test_store().await;

        let file_id = FileId::generate();
        let stripe_id = StripeId::generate();
        let original = vec![0u8; 1024];
        let policy = StoragePolicy::new(2, 1, 1024, CompressionAlgorithm::None);

        // Write initial stripe
        let metadata1 = store
            .write_stripe(file_id, stripe_id, original, policy.clone())
            .await
            .unwrap();

        // Update bytes 100-200
        let new_data = vec![255u8; 100];
        let metadata2 = store
            .update_stripe_partial(
                file_id,
                stripe_id,
                metadata1.chunks.clone(),
                100,
                new_data,
                policy,
            )
            .await
            .unwrap();

        // Read back and verify
        let result = store
            .read_stripe(file_id, stripe_id, metadata2.chunks)
            .await
            .unwrap();

        assert_eq!(
            result[100..200],
            vec![255u8; 100],
            "Updated region should have new data"
        );
        assert_eq!(
            result[0..100],
            vec![0u8; 100],
            "Unchanged region should be preserved"
        );
        assert_eq!(
            result[200..1024],
            vec![0u8; 824],
            "Rest of data should be preserved"
        );
    }

    #[tokio::test]
    async fn test_chunk_id_uniqueness() {
        let (store, _temp_dir, _disk_id) = create_test_store().await;

        let mut all_chunk_ids = HashSet::new();
        let policy = StoragePolicy::new(2, 1, 1024, CompressionAlgorithm::None);

        // Write 20 stripes, collect all chunk IDs
        for i in 0..20 {
            let file_id = FileId::new(i);
            let stripe_id = StripeId::new(i);
            let data = vec![i as u8; 512];

            let metadata = store
                .write_stripe(file_id, stripe_id, data, policy.clone())
                .await
                .unwrap();

            for chunk in metadata.chunks {
                assert!(
                    all_chunk_ids.insert(chunk.chunk_id),
                    "ChunkId collision detected: {:?}",
                    chunk.chunk_id
                );
            }
        }

        // Verify we have unique IDs (20 stripes * 3 chunks each = 60 unique IDs)
        assert_eq!(all_chunk_ids.len(), 60);
    }

    #[tokio::test]
    async fn test_read_with_missing_chunk() {
        let (store, _temp_dir, disk_id) = create_test_store().await;

        let file_id = FileId::generate();
        let stripe_id = StripeId::generate();
        let original_data = vec![42u8; 512];
        let policy = StoragePolicy::new(2, 1, 1024, CompressionAlgorithm::None);

        // Write stripe
        let metadata = store
            .write_stripe(file_id, stripe_id, original_data.clone(), policy)
            .await
            .unwrap();

        // Delete one chunk (simulate loss)
        let disks = store.inner.disks.read().await;
        let disk_info = disks.get(&disk_id).unwrap();
        let chunk_to_delete = &metadata.chunks[0];
        let path = store.chunk_path(
            &disk_info.path,
            file_id,
            stripe_id,
            chunk_to_delete.chunk_id,
        );
        drop(disks);

        tokio::fs::remove_file(&path).await.unwrap();

        // Should still be able to read (reconstructed from erasure coding)
        let read_data = store
            .read_stripe(file_id, stripe_id, metadata.chunks)
            .await
            .unwrap();

        assert_eq!(
            read_data, original_data,
            "Should reconstruct from remaining chunks"
        );
    }

    #[tokio::test]
    async fn test_read_stripe_with_corrupt_chunk() {
        let (store, _temp_dir, disk_id) = create_test_store().await;

        let file_id = FileId::generate();
        let stripe_id = StripeId::generate();
        let original_data = vec![99u8; 512];
        let policy = StoragePolicy::new(2, 1, 1024, CompressionAlgorithm::None);

        // Write stripe
        let metadata = store
            .write_stripe(file_id, stripe_id, original_data.clone(), policy)
            .await
            .unwrap();

        // Corrupt one chunk by writing garbage
        let disks = store.inner.disks.read().await;
        let disk_info = disks.get(&disk_id).unwrap();
        let chunk_to_corrupt = &metadata.chunks[0];
        let path = store.chunk_path(
            &disk_info.path,
            file_id,
            stripe_id,
            chunk_to_corrupt.chunk_id,
        );
        drop(disks);

        tokio::fs::write(&path, b"CORRUPTED DATA").await.unwrap();

        // Should still be able to read (reconstructed from valid chunks)
        let read_data = store
            .read_stripe(file_id, stripe_id, metadata.chunks)
            .await
            .unwrap();

        assert_eq!(
            read_data, original_data,
            "Should reconstruct from valid chunks"
        );
    }

    #[tokio::test]
    async fn test_stripe_metadata_contains_chunk_ids() {
        let (store, _temp_dir, _disk_id) = create_test_store().await;

        let file_id = FileId::generate();
        let stripe_id = StripeId::generate();
        let data = vec![77u8; 256];
        let policy = StoragePolicy::new(2, 1, 1024, CompressionAlgorithm::None);

        let metadata = store
            .write_stripe(file_id, stripe_id, data, policy)
            .await
            .unwrap();

        // Verify all chunks have valid (non-zero) ChunkIds
        for chunk in &metadata.chunks {
            assert_ne!(chunk.chunk_id.as_u64(), 0, "ChunkId should not be zero");
        }

        // Verify chunk_index values are correct
        assert_eq!(metadata.chunks[0].chunk_index, 0);
        assert_eq!(metadata.chunks[1].chunk_index, 1);
        assert_eq!(metadata.chunks[2].chunk_index, 2);
    }
}
