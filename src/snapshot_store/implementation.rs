//! Implementation of the SnapshotStore trait.

use super::types::{
    CompressionAlgorithm, Config, Error, SnapshotInfo, SnapshotReader, SnapshotStats,
};
use super::SnapshotStore;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, info, warn};

/// Inner state for SnapshotStore with interior mutability.
struct SnapshotStoreInner {
    /// Configuration
    config: Config,

    /// In-memory snapshot registry
    /// Maps snapshot_id -> SnapshotInfo
    /// Built by scanning the snapshot directory on initialization
    registry: RwLock<HashMap<u64, SnapshotInfo>>,

    /// Node ID for tracking which node created snapshots
    node_id: String,
}

/// Implementation of SnapshotStore using in-memory registry.
///
/// The registry is built by scanning the snapshot directory on disk during initialization.
/// This is a cheap-to-clone handle using Arc for interior mutability.
#[derive(Clone)]
pub struct SnapshotStoreImpl {
    inner: Arc<SnapshotStoreInner>,
}

impl SnapshotStoreImpl {
    /// Create a new SnapshotStore implementation.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for the snapshot store
    ///
    /// # Returns
    ///
    /// A new SnapshotStore implementation.
    ///
    /// # Errors
    ///
    /// Returns an error if configuration is invalid.
    pub fn new(config: Config) -> Result<Self, Error> {
        // Get hostname for node_id
        let node_id = hostname::get()
            .ok()
            .and_then(|h| h.into_string().ok())
            .unwrap_or_else(|| "unknown".to_string());

        let inner = SnapshotStoreInner {
            config,
            registry: RwLock::new(HashMap::new()),
            node_id,
        };

        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Helper function to calculate SHA256 checksum of a file.
    async fn calculate_checksum(&self, path: &Path) -> Result<String, Error> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; 8192];

        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buffer[..n]);
        }

        let result = hasher.finalize();
        Ok(format!("{:x}", result))
    }

    /// Helper function to create snapshot directory structure.
    async fn create_snapshot_directory(&self, snapshot_id: u64) -> Result<PathBuf, Error> {
        let snapshot_dir = self
            .inner
            .config
            .storage_dir
            .join(format!("snapshot_{:06}", snapshot_id));

        tokio::fs::create_dir_all(&snapshot_dir).await?;
        Ok(snapshot_dir)
    }

    /// Helper function to scan snapshot directory and rebuild registry.
    ///
    /// Called during initialization to load existing snapshots into memory.
    async fn scan_snapshot_directory(&self) -> Result<(), Error> {
        let storage_dir = &self.inner.config.storage_dir;

        // Check if storage directory exists
        if !tokio::fs::try_exists(storage_dir).await.unwrap_or(false) {
            info!(
                "Snapshot storage directory does not exist: {}",
                storage_dir.display()
            );
            return Ok(());
        }

        let mut entries = tokio::fs::read_dir(storage_dir).await?;
        let mut count = 0;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            // Skip if not a directory
            if !path.is_dir() {
                continue;
            }

            // Look for metadata.json in the snapshot directory
            let metadata_path = path.join("metadata.json");
            if !tokio::fs::try_exists(&metadata_path).await.unwrap_or(false) {
                warn!(
                    "Snapshot directory missing metadata.json: {}",
                    path.display()
                );
                continue;
            }

            // Read and parse metadata
            match tokio::fs::read_to_string(&metadata_path).await {
                Ok(json) => match serde_json::from_str::<SnapshotInfo>(&json) {
                    Ok(info) => {
                        debug!("Loaded snapshot: {:?}", info.snapshot_id);
                        let mut registry = self.inner.registry.write().unwrap();
                        registry.insert(info.snapshot_id, info);
                        count += 1;
                    }
                    Err(e) => {
                        error!("Failed to parse metadata.json in {}: {}", path.display(), e);
                    }
                },
                Err(e) => {
                    error!("Failed to read metadata.json in {}: {}", path.display(), e);
                }
            }
        }

        info!("Loaded {} snapshots from {}", count, storage_dir.display());
        Ok(())
    }

    /// Helper function to compress a file using the configured compression algorithm.
    async fn compress_file(
        &self,
        input_path: &Path,
        output_path: &Path,
        algorithm: CompressionAlgorithm,
    ) -> Result<u64, Error> {
        match algorithm {
            CompressionAlgorithm::None => {
                // Just copy the file
                tokio::fs::copy(input_path, output_path).await?;
                let metadata = tokio::fs::metadata(output_path).await?;
                Ok(metadata.len())
            }
            CompressionAlgorithm::Zstd { level } => {
                // Read input file
                let input_data = tokio::fs::read(input_path).await?;

                // Compress using zstd
                let compressed_data = zstd::bulk::compress(&input_data, level).map_err(|e| {
                    Error::CompressionError(format!("Zstd compression failed: {}", e))
                })?;

                // Write compressed data
                tokio::fs::write(output_path, &compressed_data).await?;

                Ok(compressed_data.len() as u64)
            }
        }
    }

    /// Helper function to decompress a file using the specified compression algorithm.
    async fn decompress_file(
        &self,
        input_path: &Path,
        output_path: &Path,
        algorithm: CompressionAlgorithm,
    ) -> Result<u64, Error> {
        match algorithm {
            CompressionAlgorithm::None => {
                // Just copy the file
                tokio::fs::copy(input_path, output_path).await?;
                let metadata = tokio::fs::metadata(output_path).await?;
                Ok(metadata.len())
            }
            CompressionAlgorithm::Zstd { .. } => {
                // Read compressed file
                let compressed_data = tokio::fs::read(input_path).await?;

                // Decompress using zstd
                let decompressed_data =
                    zstd::bulk::decompress(&compressed_data, 10 * 1024 * 1024 * 1024).map_err(
                        |e| Error::CompressionError(format!("Zstd decompression failed: {}", e)),
                    )?;

                // Write decompressed data
                tokio::fs::write(output_path, &decompressed_data).await?;

                Ok(decompressed_data.len() as u64)
            }
        }
    }
}

#[async_trait]
impl SnapshotStore for SnapshotStoreImpl {
    async fn initialize(&self) -> Result<(), Error> {
        info!(
            "Initializing SnapshotStore at {}",
            self.inner.config.storage_dir.display()
        );

        // Create storage directory if it doesn't exist
        tokio::fs::create_dir_all(&self.inner.config.storage_dir).await?;

        // Scan existing snapshots and rebuild registry
        self.scan_snapshot_directory().await?;

        info!("SnapshotStore initialized successfully");
        Ok(())
    }

    async fn ingest_snapshot(
        &self,
        snapshot_id: u64,
        log_index: u64,
        log_term: u64,
        metadata_db_path: &Path,
    ) -> Result<SnapshotInfo, Error> {
        info!(
            "Ingesting snapshot {} from {}",
            snapshot_id,
            metadata_db_path.display()
        );

        // Verify source file exists
        if !tokio::fs::try_exists(metadata_db_path)
            .await
            .unwrap_or(false)
        {
            return Err(Error::IoError(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Source file not found: {}", metadata_db_path.display()),
            )));
        }

        // Create snapshot directory
        let snapshot_dir = self.create_snapshot_directory(snapshot_id).await?;

        // Determine compression settings
        let compression = self.inner.config.compression;
        let target_filename = match compression {
            CompressionAlgorithm::None => "metadata.db",
            CompressionAlgorithm::Zstd { .. } => "metadata.db.zst",
        };
        let target_path = snapshot_dir.join(target_filename);

        // Copy/compress the metadata database
        let file_size = self
            .compress_file(metadata_db_path, &target_path, compression)
            .await?;

        // Calculate checksum of the stored file (compressed or uncompressed)
        let checksum = self.calculate_checksum(&target_path).await?;

        // Create snapshot info
        let info = SnapshotInfo {
            snapshot_id,
            log_index,
            log_term,
            timestamp: SystemTime::now(),
            format_version: 1,
            metadata_db_size: file_size,
            metadata_db_checksum: checksum.clone(),
            compression,
            node_id: self.inner.node_id.clone(),
            storage_path: snapshot_dir.clone(),
        };

        // Write metadata.json
        let metadata_json = serde_json::to_string_pretty(&info)
            .map_err(|e| Error::SerializationError(e.to_string()))?;
        tokio::fs::write(info.metadata_json_path(), metadata_json).await?;

        // Write checksum.sha256
        tokio::fs::write(info.checksum_path(), &checksum).await?;

        // Add to registry
        {
            let mut registry = self.inner.registry.write().unwrap();
            registry.insert(snapshot_id, info.clone());
        }

        info!(
            "Successfully ingested snapshot {} (size: {} bytes, compressed: {:?})",
            snapshot_id, file_size, compression
        );

        // Trigger automatic pruning
        if let Err(e) = self.prune_snapshots().await {
            warn!("Failed to prune snapshots after ingestion: {:?}", e);
        }

        Ok(info)
    }

    async fn get_latest_snapshot(&self) -> Result<Option<SnapshotInfo>, Error> {
        let registry = self.inner.registry.read().unwrap();

        // Find snapshot with highest snapshot_id
        let latest = registry
            .values()
            .max_by_key(|info| info.snapshot_id)
            .cloned();

        Ok(latest)
    }

    async fn get_snapshot(&self, snapshot_id: u64) -> Result<SnapshotInfo, Error> {
        let registry = self.inner.registry.read().unwrap();

        registry
            .get(&snapshot_id)
            .cloned()
            .ok_or(Error::NotFound(snapshot_id))
    }

    async fn get_snapshot_at_index(&self, log_index: u64) -> Result<Option<SnapshotInfo>, Error> {
        let registry = self.inner.registry.read().unwrap();

        // Find snapshot with highest log_index <= requested index
        let snapshot = registry
            .values()
            .filter(|info| info.log_index <= log_index)
            .max_by_key(|info| info.log_index)
            .cloned();

        Ok(snapshot)
    }

    async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>, Error> {
        let registry = self.inner.registry.read().unwrap();

        let mut snapshots: Vec<SnapshotInfo> = registry.values().cloned().collect();
        snapshots.sort_by_key(|info| info.snapshot_id);

        Ok(snapshots)
    }

    async fn open_snapshot(&self, snapshot_id: u64) -> Result<SnapshotReader, Error> {
        let info = self.get_snapshot(snapshot_id).await?;

        // Determine the actual metadata file path based on compression
        let metadata_path = match info.compression {
            CompressionAlgorithm::None => info.storage_path.join("metadata.db"),
            CompressionAlgorithm::Zstd { .. } => info.storage_path.join("metadata.db.zst"),
        };

        // Verify file exists
        if !tokio::fs::try_exists(&metadata_path).await.unwrap_or(false) {
            return Err(Error::NotFound(snapshot_id));
        }

        Ok(SnapshotReader::new(snapshot_id, metadata_path, info))
    }

    async fn stream_snapshot(
        &self,
        snapshot_id: u64,
        mut sink: Box<dyn tokio::io::AsyncWrite + Unpin + Send>,
    ) -> Result<(), Error> {
        let reader = self.open_snapshot(snapshot_id).await?;
        let metadata_path = reader.get_metadata_db_path();

        // Open file for reading
        let mut file = tokio::fs::File::open(metadata_path).await?;

        // Stream in chunks
        let chunk_size = self.inner.config.stream_chunk_size;
        let mut buffer = vec![0u8; chunk_size];
        let mut total_bytes = 0u64;

        loop {
            let n = file.read(&mut buffer).await?;
            if n == 0 {
                break;
            }

            sink.write_all(&buffer[..n]).await?;
            total_bytes += n as u64;
        }

        sink.flush().await?;

        info!("Streamed snapshot {} ({} bytes)", snapshot_id, total_bytes);
        Ok(())
    }

    async fn receive_snapshot(
        &self,
        snapshot_id: u64,
        log_index: u64,
        log_term: u64,
        mut source: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
    ) -> Result<SnapshotInfo, Error> {
        info!("Receiving snapshot {} from remote node", snapshot_id);

        // Create temporary file for receiving
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join(format!("snapshot_{}_temp.db", snapshot_id));

        // Receive data to temporary file
        let mut file = tokio::fs::File::create(&temp_file).await?;
        let chunk_size = self.inner.config.stream_chunk_size;
        let mut buffer = vec![0u8; chunk_size];
        let mut total_bytes = 0u64;

        loop {
            let n = source.read(&mut buffer).await?;
            if n == 0 {
                break;
            }

            file.write_all(&buffer[..n]).await?;
            total_bytes += n as u64;
        }

        file.sync_all().await?;
        drop(file);

        info!(
            "Received snapshot {} ({} bytes), ingesting...",
            snapshot_id, total_bytes
        );

        // Ingest the received snapshot
        let result = self
            .ingest_snapshot(snapshot_id, log_index, log_term, &temp_file)
            .await;

        // Clean up temporary file
        if let Err(e) = tokio::fs::remove_file(&temp_file).await {
            warn!("Failed to remove temporary file: {}", e);
        }

        result
    }

    async fn verify_snapshot(&self, snapshot_id: u64) -> Result<bool, Error> {
        let info = self.get_snapshot(snapshot_id).await?;

        // Determine the actual metadata file path
        let metadata_path = match info.compression {
            CompressionAlgorithm::None => info.storage_path.join("metadata.db"),
            CompressionAlgorithm::Zstd { .. } => info.storage_path.join("metadata.db.zst"),
        };

        // Calculate current checksum
        let current_checksum = self.calculate_checksum(&metadata_path).await?;

        // Compare with stored checksum
        Ok(current_checksum == info.metadata_db_checksum)
    }

    async fn prune_snapshots(&self) -> Result<Vec<u64>, Error> {
        let retention = &self.inner.config.retention_policy;
        let mut deleted_ids = Vec::new();

        // Get all snapshots sorted by snapshot_id (ascending)
        let mut snapshots = self.list_snapshots().await?;
        snapshots.sort_by_key(|info| info.snapshot_id);

        // Always keep at least min_snapshots
        if snapshots.len() <= retention.min_snapshots {
            debug!(
                "Skipping pruning: only {} snapshots (min: {})",
                snapshots.len(),
                retention.min_snapshots
            );
            return Ok(deleted_ids);
        }

        let now = SystemTime::now();

        // Determine which snapshots to delete
        let mut to_delete = Vec::new();

        for (idx, info) in snapshots.iter().enumerate() {
            // Always keep the last min_snapshots
            if idx >= snapshots.len() - retention.min_snapshots {
                break;
            }

            // Check max_snapshots limit
            let exceeds_max_count = snapshots.len() - to_delete.len() > retention.max_snapshots;

            // Check max_age limit
            let exceeds_max_age = now
                .duration_since(info.timestamp)
                .map(|age| age > retention.max_age)
                .unwrap_or(false);

            if exceeds_max_count || exceeds_max_age {
                to_delete.push(info.snapshot_id);
            }
        }

        // Delete snapshots
        for snapshot_id in to_delete {
            match self.delete_snapshot(snapshot_id).await {
                Ok(()) => {
                    deleted_ids.push(snapshot_id);
                }
                Err(e) => {
                    error!("Failed to delete snapshot {}: {:?}", snapshot_id, e);
                }
            }
        }

        if !deleted_ids.is_empty() {
            info!("Pruned {} snapshots: {:?}", deleted_ids.len(), deleted_ids);
        }

        Ok(deleted_ids)
    }

    async fn delete_snapshot(&self, snapshot_id: u64) -> Result<(), Error> {
        let info = self.get_snapshot(snapshot_id).await?;

        // Delete snapshot directory and all contents
        tokio::fs::remove_dir_all(&info.storage_path).await?;

        // Remove from registry
        {
            let mut registry = self.inner.registry.write().unwrap();
            registry.remove(&snapshot_id);
        }

        info!("Deleted snapshot {}", snapshot_id);
        Ok(())
    }

    fn get_stats(&self) -> SnapshotStats {
        let registry = self.inner.registry.read().unwrap();

        let total_snapshots = registry.len();
        let total_size: u64 = registry.values().map(|info| info.metadata_db_size).sum();

        let oldest_snapshot = registry.values().map(|info| info.timestamp).min();
        let newest_snapshot = registry.values().map(|info| info.timestamp).max();

        SnapshotStats {
            total_snapshots,
            total_size,
            oldest_snapshot,
            newest_snapshot,
            disk_usage: total_size, // Approximate
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn create_test_store(temp_dir: &TempDir) -> SnapshotStoreImpl {
        let config = Config {
            storage_dir: temp_dir.path().to_path_buf(),
            retention_policy: super::super::types::RetentionPolicy {
                max_snapshots: 3,
                max_age: std::time::Duration::from_secs(30 * 24 * 60 * 60),
                min_snapshots: 1,
            },
            compression: CompressionAlgorithm::None,
            stream_chunk_size: 1024,
        };

        let store = SnapshotStoreImpl::new(config).unwrap();
        store.initialize().await.unwrap();
        store
    }

    async fn create_dummy_snapshot(dir: &Path, content: &[u8]) -> PathBuf {
        let path = dir.join("test_snapshot.db");
        tokio::fs::write(&path, content).await.unwrap();
        path
    }

    #[tokio::test]
    async fn test_initialize() {
        let temp_dir = TempDir::new().unwrap();
        let _store = create_test_store(&temp_dir).await;

        // Verify storage directory was created
        assert!(temp_dir.path().exists());
    }

    #[tokio::test]
    async fn test_ingest_and_get_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let store = create_test_store(&temp_dir).await;

        // Create a dummy snapshot file
        let content = b"test snapshot data";
        let source_path = create_dummy_snapshot(temp_dir.path(), content).await;

        // Ingest snapshot
        let info = store
            .ingest_snapshot(1, 100, 5, &source_path)
            .await
            .unwrap();

        assert_eq!(info.snapshot_id, 1);
        assert_eq!(info.log_index, 100);
        assert_eq!(info.log_term, 5);

        // Retrieve snapshot
        let retrieved = store.get_snapshot(1).await.unwrap();
        assert_eq!(retrieved.snapshot_id, 1);
    }

    #[tokio::test]
    async fn test_get_latest_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let store = create_test_store(&temp_dir).await;

        // No snapshots initially
        assert!(store.get_latest_snapshot().await.unwrap().is_none());

        // Ingest multiple snapshots
        let content = b"data";
        let source_path = create_dummy_snapshot(temp_dir.path(), content).await;

        for id in [1, 3, 2] {
            store
                .ingest_snapshot(id, id * 100, 5, &source_path)
                .await
                .unwrap();
        }

        // Latest should be snapshot 3
        let latest = store.get_latest_snapshot().await.unwrap().unwrap();
        assert_eq!(latest.snapshot_id, 3);
    }

    #[tokio::test]
    async fn test_list_snapshots() {
        let temp_dir = TempDir::new().unwrap();
        let store = create_test_store(&temp_dir).await;

        let content = b"data";
        let source_path = create_dummy_snapshot(temp_dir.path(), content).await;

        // Ingest snapshots in random order
        for id in [3, 1, 2] {
            store
                .ingest_snapshot(id, id * 100, 5, &source_path)
                .await
                .unwrap();
        }

        // List should be sorted by snapshot_id
        let snapshots = store.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 3);
        assert_eq!(snapshots[0].snapshot_id, 1);
        assert_eq!(snapshots[1].snapshot_id, 2);
        assert_eq!(snapshots[2].snapshot_id, 3);
    }

    #[tokio::test]
    async fn test_snapshot_pruning() {
        let temp_dir = TempDir::new().unwrap();
        let store = create_test_store(&temp_dir).await;

        let content = b"data";
        let source_path = create_dummy_snapshot(temp_dir.path(), content).await;

        // Ingest 5 snapshots (max_snapshots is 3)
        for id in 1..=5 {
            store
                .ingest_snapshot(id, id * 100, 5, &source_path)
                .await
                .unwrap();
        }

        // Should have pruned to 3 snapshots
        let snapshots = store.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 3);

        // Should keep the 3 most recent (3, 4, 5)
        assert_eq!(snapshots[0].snapshot_id, 3);
        assert_eq!(snapshots[1].snapshot_id, 4);
        assert_eq!(snapshots[2].snapshot_id, 5);
    }

    #[tokio::test]
    async fn test_get_snapshot_at_index() {
        let temp_dir = TempDir::new().unwrap();
        let store = create_test_store(&temp_dir).await;

        let content = b"data";
        let source_path = create_dummy_snapshot(temp_dir.path(), content).await;

        // Ingest snapshots at different log indices
        store
            .ingest_snapshot(1, 100, 5, &source_path)
            .await
            .unwrap();
        store
            .ingest_snapshot(2, 200, 5, &source_path)
            .await
            .unwrap();
        store
            .ingest_snapshot(3, 300, 5, &source_path)
            .await
            .unwrap();

        // Get snapshot at index 150 (should return snapshot 1)
        let snapshot = store.get_snapshot_at_index(150).await.unwrap().unwrap();
        assert_eq!(snapshot.snapshot_id, 1);
        assert_eq!(snapshot.log_index, 100);

        // Get snapshot at index 250 (should return snapshot 2)
        let snapshot = store.get_snapshot_at_index(250).await.unwrap().unwrap();
        assert_eq!(snapshot.snapshot_id, 2);
        assert_eq!(snapshot.log_index, 200);

        // Get snapshot at index 350 (should return snapshot 3)
        let snapshot = store.get_snapshot_at_index(350).await.unwrap().unwrap();
        assert_eq!(snapshot.snapshot_id, 3);
        assert_eq!(snapshot.log_index, 300);

        // Get snapshot at index 50 (before any snapshot)
        let snapshot = store.get_snapshot_at_index(50).await.unwrap();
        assert!(snapshot.is_none());
    }

    #[tokio::test]
    async fn test_verify_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let store = create_test_store(&temp_dir).await;

        let content = b"test snapshot data";
        let source_path = create_dummy_snapshot(temp_dir.path(), content).await;

        // Ingest snapshot
        store
            .ingest_snapshot(1, 100, 5, &source_path)
            .await
            .unwrap();

        // Verify should pass
        assert!(store.verify_snapshot(1).await.unwrap());
    }

    #[tokio::test]
    async fn test_get_stats() {
        let temp_dir = TempDir::new().unwrap();
        let store = create_test_store(&temp_dir).await;

        // No snapshots initially
        let stats = store.get_stats();
        assert_eq!(stats.total_snapshots, 0);
        assert_eq!(stats.total_size, 0);

        // Ingest snapshots
        let content = b"test data";
        let source_path = create_dummy_snapshot(temp_dir.path(), content).await;

        store
            .ingest_snapshot(1, 100, 5, &source_path)
            .await
            .unwrap();
        store
            .ingest_snapshot(2, 200, 5, &source_path)
            .await
            .unwrap();

        let stats = store.get_stats();
        assert_eq!(stats.total_snapshots, 2);
        assert!(stats.total_size > 0);
        assert!(stats.oldest_snapshot.is_some());
        assert!(stats.newest_snapshot.is_some());
    }
}
