// Snapshot storage module for persisting Raft snapshots to disk
//
// This module handles:
// - Saving snapshots to disk with metadata
// - Loading the most recent snapshot
// - Cleaning up old snapshots

use openraft::{SnapshotMeta, StorageError, StorageIOError};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

/// Metadata for a persisted snapshot (extends SnapshotMeta with additional info)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSnapshotMeta {
    /// The OpenRaft snapshot metadata
    pub raft_meta: SnapshotMeta<u64, ()>,
    /// Checksum of the data file (CRC32)
    pub data_checksum: u32,
    /// Size of the compressed data in bytes
    pub data_size: u64,
    /// Timestamp when snapshot was created
    pub created_at: u64,
}

/// Manages snapshot persistence to disk
pub struct SnapshotStore {
    /// Directory where snapshots are stored
    snapshot_dir: PathBuf,
    /// Maximum number of snapshots to keep
    max_snapshots: usize,
}

impl SnapshotStore {
    /// Create a new snapshot store
    pub fn new(snapshot_dir: PathBuf, max_snapshots: usize) -> Result<Self, io::Error> {
        // Create snapshot directory if it doesn't exist
        fs::create_dir_all(&snapshot_dir)?;

        Ok(Self {
            snapshot_dir,
            max_snapshots,
        })
    }

    /// Save a snapshot to disk
    #[allow(clippy::result_large_err)]
    pub fn save_snapshot(
        &self,
        meta: &SnapshotMeta<u64, ()>,
        data: &[u8],
    ) -> Result<(), StorageError<u64>> {
        let snapshot_id = &meta.snapshot_id;

        // Calculate checksum
        let data_checksum = crc32fast::hash(data);

        // Create persisted metadata
        let persisted_meta = PersistedSnapshotMeta {
            raft_meta: meta.clone(),
            data_checksum,
            data_size: data.len() as u64,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        // Write metadata file
        let meta_path = self.snapshot_dir.join(format!("{}.meta", snapshot_id));
        let meta_json =
            serde_json::to_string_pretty(&persisted_meta).map_err(|e| StorageError::IO {
                source: StorageIOError::write(&e),
            })?;
        fs::write(&meta_path, meta_json).map_err(|e| StorageError::IO {
            source: StorageIOError::write(&e),
        })?;

        // Write data file
        let data_path = self.snapshot_dir.join(format!("{}.data", snapshot_id));
        fs::write(&data_path, data).map_err(|e| StorageError::IO {
            source: StorageIOError::write(&e),
        })?;

        tracing::info!(
            "Saved snapshot '{}' ({} bytes compressed) to disk",
            snapshot_id,
            data.len()
        );

        // Clean up old snapshots
        self.cleanup_old_snapshots()?;

        Ok(())
    }

    /// Load the most recent snapshot from disk
    #[allow(clippy::result_large_err, clippy::type_complexity)]
    pub fn load_latest_snapshot(
        &self,
    ) -> Result<Option<(SnapshotMeta<u64, ()>, Vec<u8>)>, StorageError<u64>> {
        // List all snapshot metadata files
        let entries = fs::read_dir(&self.snapshot_dir).map_err(|e| StorageError::IO {
            source: StorageIOError::read(&e),
        })?;

        let mut snapshots = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                if let Ok(meta) = self.load_snapshot_meta(&path) {
                    snapshots.push(meta);
                }
            }
        }

        if snapshots.is_empty() {
            return Ok(None);
        }

        // Sort by log index (descending) to get the latest
        snapshots.sort_by(|a, b| {
            let a_index = a
                .raft_meta
                .last_log_id
                .as_ref()
                .map(|id| id.index)
                .unwrap_or(0);
            let b_index = b
                .raft_meta
                .last_log_id
                .as_ref()
                .map(|id| id.index)
                .unwrap_or(0);
            b_index.cmp(&a_index)
        });

        let latest = &snapshots[0];
        let snapshot_id = &latest.raft_meta.snapshot_id;

        // Load the data file
        let data_path = self.snapshot_dir.join(format!("{}.data", snapshot_id));
        let data = fs::read(&data_path).map_err(|e| StorageError::IO {
            source: StorageIOError::read(&e),
        })?;

        // Verify checksum
        let checksum = crc32fast::hash(&data);
        if checksum != latest.data_checksum {
            return Err(StorageError::IO {
                source: StorageIOError::read(&io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Snapshot checksum mismatch: expected {}, got {}",
                        latest.data_checksum, checksum
                    ),
                )),
            });
        }

        tracing::info!(
            "Loaded snapshot '{}' ({} bytes) from disk",
            snapshot_id,
            data.len()
        );

        Ok(Some((latest.raft_meta.clone(), data)))
    }

    /// Load snapshot metadata from a file
    fn load_snapshot_meta(&self, path: &Path) -> Result<PersistedSnapshotMeta, io::Error> {
        let mut file = fs::File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        serde_json::from_str(&contents).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Clean up old snapshots, keeping only the most recent max_snapshots
    #[allow(clippy::result_large_err)]
    fn cleanup_old_snapshots(&self) -> Result<(), StorageError<u64>> {
        // List all snapshot metadata files
        let entries = fs::read_dir(&self.snapshot_dir).map_err(|e| StorageError::IO {
            source: StorageIOError::read(&e),
        })?;

        let mut snapshots = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("meta") {
                if let Ok(meta) = self.load_snapshot_meta(&path) {
                    snapshots.push(meta);
                }
            }
        }

        if snapshots.len() <= self.max_snapshots {
            return Ok(());
        }

        // Sort by log index (descending)
        snapshots.sort_by(|a, b| {
            let a_index = a
                .raft_meta
                .last_log_id
                .as_ref()
                .map(|id| id.index)
                .unwrap_or(0);
            let b_index = b
                .raft_meta
                .last_log_id
                .as_ref()
                .map(|id| id.index)
                .unwrap_or(0);
            b_index.cmp(&a_index)
        });

        // Delete old snapshots (keep only max_snapshots)
        for old_snapshot in snapshots.iter().skip(self.max_snapshots) {
            let snapshot_id = &old_snapshot.raft_meta.snapshot_id;
            let meta_path = self.snapshot_dir.join(format!("{}.meta", snapshot_id));
            let data_path = self.snapshot_dir.join(format!("{}.data", snapshot_id));

            if let Err(e) = fs::remove_file(&meta_path) {
                tracing::warn!("Failed to delete old snapshot meta {}: {}", snapshot_id, e);
            }
            if let Err(e) = fs::remove_file(&data_path) {
                tracing::warn!("Failed to delete old snapshot data {}: {}", snapshot_id, e);
            }

            tracing::info!("Cleaned up old snapshot '{}'", snapshot_id);
        }

        Ok(())
    }

    /// Get the path to the snapshot directory
    pub fn snapshot_dir(&self) -> &Path {
        &self.snapshot_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{LogId, StoredMembership};
    use tempfile::TempDir;

    fn create_test_snapshot_meta(index: u64) -> SnapshotMeta<u64, ()> {
        use openraft::LeaderId;

        SnapshotMeta {
            last_log_id: Some(LogId::new(LeaderId::new(1, 1), index)),
            last_membership: StoredMembership::default(),
            snapshot_id: format!("test-snapshot-{}", index),
        }
    }

    #[test]
    fn test_snapshot_store_creation() {
        let temp_dir = TempDir::new().unwrap();
        let store = SnapshotStore::new(temp_dir.path().to_path_buf(), 3).unwrap();
        assert!(store.snapshot_dir().exists());
    }

    #[test]
    fn test_save_and_load_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let store = SnapshotStore::new(temp_dir.path().to_path_buf(), 3).unwrap();

        let meta = create_test_snapshot_meta(100);
        let data = b"test snapshot data".to_vec();

        // Save snapshot
        store.save_snapshot(&meta, &data).unwrap();

        // Load snapshot
        let loaded = store.load_latest_snapshot().unwrap();
        assert!(loaded.is_some());

        let (loaded_meta, loaded_data) = loaded.unwrap();
        assert_eq!(loaded_meta.snapshot_id, meta.snapshot_id);
        assert_eq!(loaded_data, data);
    }

    #[test]
    fn test_load_latest_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let store = SnapshotStore::new(temp_dir.path().to_path_buf(), 3).unwrap();

        // Save multiple snapshots
        for i in 1..=5 {
            let meta = create_test_snapshot_meta(i * 100);
            let data = format!("snapshot data {}", i).into_bytes();
            store.save_snapshot(&meta, &data).unwrap();
        }

        // Load latest should return snapshot with highest index
        let loaded = store.load_latest_snapshot().unwrap();
        assert!(loaded.is_some());

        let (loaded_meta, _) = loaded.unwrap();
        assert_eq!(
            loaded_meta.last_log_id.as_ref().unwrap().index,
            500 // Latest snapshot
        );
    }

    #[test]
    fn test_cleanup_old_snapshots() {
        let temp_dir = TempDir::new().unwrap();
        let store = SnapshotStore::new(temp_dir.path().to_path_buf(), 3).unwrap();

        // Save 5 snapshots
        for i in 1..=5 {
            let meta = create_test_snapshot_meta(i * 100);
            let data = format!("snapshot data {}", i).into_bytes();
            store.save_snapshot(&meta, &data).unwrap();
        }

        // Count snapshot files (should be 3 * 2 = 6 files: 3 meta + 3 data)
        let file_count = fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .count();
        assert_eq!(file_count, 6);
    }

    #[test]
    fn test_load_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let store = SnapshotStore::new(temp_dir.path().to_path_buf(), 3).unwrap();

        let loaded = store.load_latest_snapshot().unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_checksum_verification() {
        let temp_dir = TempDir::new().unwrap();
        let store = SnapshotStore::new(temp_dir.path().to_path_buf(), 3).unwrap();

        let meta = create_test_snapshot_meta(100);
        let data = b"test data".to_vec();

        store.save_snapshot(&meta, &data).unwrap();

        // Corrupt the data file
        let data_path = temp_dir.path().join(format!("{}.data", meta.snapshot_id));
        fs::write(&data_path, b"corrupted data").unwrap();

        // Load should fail due to checksum mismatch
        let result = store.load_latest_snapshot();
        assert!(result.is_err());
    }
}
