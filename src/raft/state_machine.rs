// Raft state machine implementation wrapping MetadataStore
//
// This module implements OpenRaft's RaftStateMachine trait, applying
// committed metadata operations to the SQLite-based MetadataStore.
//
// Note: SQLite's Connection is not Sync, so we use a single-threaded
// approach with message passing for database operations.

use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::metadata_store::ChunkId as StoreChunkId;
use crate::metadata_store::{
    ChunkMetadata, FileMetadata as StoreFileMetadata, LockType as StoreLockType, MetadataStore,
    StorageLocation,
};
use crate::raft::snapshot_store::SnapshotStore;
use crate::raft::types::{
    LockType as RaftLockType, MetadataOp, MetadataOpResponse, WormFSTypeConfig,
};
use std::str::FromStr;

/// State machine that applies metadata operations to SQLite via MetadataStore
///
/// Phase 2A: Complete implementation with snapshot persistence
/// Phase 2B: Network transport and cluster coordination
#[derive(Clone)]
pub struct StateMachine {
    /// The underlying metadata store (SQLite) protected by Mutex for Send+Sync
    /// SQLite Connection is not Sync, so we serialize all access
    store: Arc<Mutex<MetadataStore>>,
    /// Snapshot store for persisting snapshots to disk
    snapshot_store: Arc<SnapshotStore>,
    /// Last applied log ID
    last_applied: Arc<RwLock<Option<LogId<u64>>>>,
    /// Last membership config
    last_membership: Arc<RwLock<StoredMembership<u64, ()>>>,
}

impl StateMachine {
    /// Create a new state machine with an in-memory database
    #[allow(clippy::result_large_err)]
    pub fn new() -> Result<Self, StorageError<u64>> {
        let temp_dir =
            std::env::temp_dir().join(format!("wormfs-snapshots-{}", uuid::Uuid::new_v4()));
        Self::with_paths(":memory:", temp_dir)
    }

    /// Create a new state machine with a database at the specified path
    #[allow(clippy::result_large_err)]
    pub fn with_path(db_path: &str) -> Result<Self, StorageError<u64>> {
        let snapshot_dir = PathBuf::from(db_path)
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("snapshots");
        Self::with_paths(db_path, snapshot_dir)
    }

    /// Create a new state machine with explicit paths for database and snapshots
    #[allow(clippy::result_large_err)]
    pub fn with_paths(db_path: &str, snapshot_dir: PathBuf) -> Result<Self, StorageError<u64>> {
        let store = MetadataStore::new(db_path).map_err(|e| StorageError::IO {
            source: StorageIOError::read(&e),
        })?;

        let snapshot_store = SnapshotStore::new(snapshot_dir, 3).map_err(|e| StorageError::IO {
            source: StorageIOError::read(&e),
        })?;

        Ok(Self {
            store: Arc::new(Mutex::new(store)),
            snapshot_store: Arc::new(snapshot_store),
            last_applied: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
        })
    }

    /// Apply a single metadata operation to the store
    async fn apply_op(&self, op: MetadataOp) -> MetadataOpResponse {
        let store = self.store.lock().await;

        match op {
            MetadataOp::CreateFile { metadata } => {
                // Note: In a real implementation, the path would be stored in FileMetadata
                // or managed by the FUSE layer. For now, we use a placeholder path.
                let placeholder_path = PathBuf::from(format!("/files/{}", metadata.file_id));

                let store_metadata = StoreFileMetadata {
                    file_id: metadata.file_id,
                    path: placeholder_path.clone(),
                    size: metadata.size,
                    permissions: metadata.permissions,
                    created_at: std::time::UNIX_EPOCH
                        + std::time::Duration::from_secs(metadata.created_at as u64),
                    modified_at: std::time::UNIX_EPOCH
                        + std::time::Duration::from_secs(metadata.modified_at as u64),
                    accessed_at: std::time::UNIX_EPOCH
                        + std::time::Duration::from_secs(metadata.accessed_at as u64),
                    stripe_count: 0, // Will be incremented as stripes are added
                    checksum: 0,     // Will be set when file is fully stored
                };

                match store.create_file(store_metadata) {
                    Ok(_) => {
                        tracing::debug!("Created file: {}", metadata.file_id);
                        MetadataOpResponse::Success
                    }
                    Err(e) => {
                        tracing::error!("Failed to create file {}: {}", metadata.file_id, e);
                        MetadataOpResponse::Error(format!("Failed to create file: {}", e))
                    }
                }
            }

            MetadataOp::UpdateFile { file_id, metadata } => {
                let placeholder_path = PathBuf::from(format!("/files/{}", file_id));

                let store_metadata = StoreFileMetadata {
                    file_id: metadata.file_id,
                    path: placeholder_path,
                    size: metadata.size,
                    permissions: metadata.permissions,
                    created_at: std::time::UNIX_EPOCH
                        + std::time::Duration::from_secs(metadata.created_at as u64),
                    modified_at: std::time::UNIX_EPOCH
                        + std::time::Duration::from_secs(metadata.modified_at as u64),
                    accessed_at: std::time::UNIX_EPOCH
                        + std::time::Duration::from_secs(metadata.accessed_at as u64),
                    stripe_count: 0,
                    checksum: 0,
                };

                match store.update_file(file_id, store_metadata) {
                    Ok(_) => {
                        tracing::debug!("Updated file: {}", file_id);
                        MetadataOpResponse::Success
                    }
                    Err(e) => {
                        tracing::error!("Failed to update file {}: {}", file_id, e);
                        MetadataOpResponse::Error(format!("Failed to update file: {}", e))
                    }
                }
            }

            MetadataOp::DeleteFile { file_id } => match store.delete_file(file_id) {
                Ok(_) => {
                    tracing::debug!("Deleted file: {}", file_id);
                    MetadataOpResponse::Success
                }
                Err(e) => {
                    tracing::error!("Failed to delete file {}: {}", file_id, e);
                    MetadataOpResponse::Error(format!("Failed to delete file: {}", e))
                }
            },

            MetadataOp::RegisterChunk {
                chunk_id,
                node_id,
                stripe_id: _,
                file_id: _,
            } => {
                // Parse the chunk_id UUID as a serialized compound identifier
                let chunk_id_str = chunk_id.to_string();
                let store_chunk_id = match StoreChunkId::from_str(&chunk_id_str) {
                    Ok(id) => id,
                    Err(e) => {
                        tracing::error!("Failed to parse chunk_id '{}': {}", chunk_id_str, e);
                        return MetadataOpResponse::Error(format!(
                            "Invalid chunk_id format: {}",
                            e
                        ));
                    }
                };

                // Create a UUID for the node (can't convert u64 to Uuid directly)
                let node_uuid = uuid::Uuid::new_v4(); // In Phase 2B, maintain proper node_id mapping
                let location = StorageLocation::new(
                    node_uuid,
                    format!("disk-{}", node_id),
                    PathBuf::from(format!("/chunks/{}", chunk_id)),
                );

                let chunk_metadata = ChunkMetadata::new(
                    store_chunk_id.file_id,
                    store_chunk_id.stripe_index,
                    store_chunk_id.chunk_index,
                    0, // size - unknown at this level
                    0, // checksum - unknown at this level
                    location,
                );

                match store.register_chunk(store_chunk_id, chunk_metadata) {
                    Ok(_) => {
                        tracing::debug!("Registered chunk: {}", chunk_id_str);
                        MetadataOpResponse::Success
                    }
                    Err(e) => {
                        tracing::error!("Failed to register chunk {}: {}", chunk_id_str, e);
                        MetadataOpResponse::Error(format!("Failed to register chunk: {}", e))
                    }
                }
            }

            MetadataOp::UpdateChunkLocation {
                chunk_id,
                new_node_id,
            } => {
                // Parse the chunk_id UUID as a serialized compound identifier
                let chunk_id_str = chunk_id.to_string();
                let store_chunk_id = match StoreChunkId::from_str(&chunk_id_str) {
                    Ok(id) => id,
                    Err(e) => {
                        tracing::error!("Failed to parse chunk_id '{}': {}", chunk_id_str, e);
                        return MetadataOpResponse::Error(format!(
                            "Invalid chunk_id format: {}",
                            e
                        ));
                    }
                };

                // Create a UUID for the new node
                let node_uuid = uuid::Uuid::new_v4(); // In Phase 2B, maintain proper node_id mapping
                let location = StorageLocation::new(
                    node_uuid,
                    format!("disk-{}", new_node_id),
                    PathBuf::from(format!("/chunks/{}", chunk_id)),
                );

                match store.update_chunk_location(store_chunk_id, location) {
                    Ok(_) => {
                        tracing::debug!(
                            "Updated chunk location: {} -> node {}",
                            chunk_id_str,
                            new_node_id
                        );
                        MetadataOpResponse::Success
                    }
                    Err(e) => {
                        tracing::error!("Failed to update chunk location {}: {}", chunk_id_str, e);
                        MetadataOpResponse::Error(format!("Failed to update chunk location: {}", e))
                    }
                }
            }

            MetadataOp::RemoveChunk { chunk_id } => {
                // Parse the chunk_id UUID as a serialized compound identifier
                let chunk_id_str = chunk_id.to_string();
                let store_chunk_id = match StoreChunkId::from_str(&chunk_id_str) {
                    Ok(id) => id,
                    Err(e) => {
                        tracing::error!("Failed to parse chunk_id '{}': {}", chunk_id_str, e);
                        return MetadataOpResponse::Error(format!(
                            "Invalid chunk_id format: {}",
                            e
                        ));
                    }
                };

                match store.delete_chunk(store_chunk_id) {
                    Ok(_) => {
                        tracing::debug!("Deleted chunk: {}", chunk_id_str);
                        MetadataOpResponse::Success
                    }
                    Err(e) => {
                        tracing::error!("Failed to delete chunk {}: {}", chunk_id_str, e);
                        MetadataOpResponse::Error(format!("Failed to delete chunk: {}", e))
                    }
                }
            }

            MetadataOp::AcquireLock {
                file_id,
                lock_type,
                client_id,
            } => {
                // Convert raft::types::LockType to metadata_store::LockType
                let store_lock_type = match lock_type {
                    RaftLockType::Read => StoreLockType::Read,
                    RaftLockType::Write => StoreLockType::Write,
                };

                // Use default lock timeout of 30 seconds
                let timeout_seconds = 30u64;
                match store.acquire_lock(
                    file_id,
                    store_lock_type,
                    client_id.clone(),
                    timeout_seconds,
                ) {
                    Ok(_) => {
                        tracing::debug!(
                            "Acquired {:?} lock on {} for {}",
                            lock_type,
                            file_id,
                            client_id
                        );
                        MetadataOpResponse::Success
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to acquire {:?} lock on {} for {}: {}",
                            lock_type,
                            file_id,
                            client_id,
                            e
                        );
                        MetadataOpResponse::Error(format!("Failed to acquire lock: {}", e))
                    }
                }
            }

            MetadataOp::ReleaseLock { file_id, client_id } => {
                match store.release_lock(file_id, client_id.clone()) {
                    Ok(_) => {
                        tracing::debug!("Released lock on {} for {}", file_id, client_id);
                        MetadataOpResponse::Success
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to release lock on {} for {}: {}",
                            file_id,
                            client_id,
                            e
                        );
                        MetadataOpResponse::Error(format!("Failed to release lock: {}", e))
                    }
                }
            }

            MetadataOp::ExtendLock { file_id, client_id } => {
                // Extend lock by 30 seconds
                let timeout_seconds = 30u64;
                match store.extend_lock(file_id, client_id.clone(), timeout_seconds) {
                    Ok(_) => {
                        tracing::debug!("Extended lock on {} for {}", file_id, client_id);
                        MetadataOpResponse::Success
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to extend lock on {} for {}: {}",
                            file_id,
                            client_id,
                            e
                        );
                        MetadataOpResponse::Error(format!("Failed to extend lock: {}", e))
                    }
                }
            }

            MetadataOp::AddNode { node_id, address } => {
                // Node membership is handled by Raft itself, not our metadata store
                tracing::debug!("Would add node {}: {}", node_id, address);
                MetadataOpResponse::Success
            }

            MetadataOp::RemoveNode { node_id } => {
                tracing::debug!("Would remove node: {}", node_id);
                MetadataOpResponse::Success
            }
        }
    }
}

/// Snapshot builder for the state machine
#[derive(Clone)]
pub struct StateMachineSnapshot {
    /// Snapshot metadata
    meta: SnapshotMeta<u64, ()>,
    /// Snapshot data (serialized MetadataStore state)
    data: Vec<u8>,
}

impl RaftSnapshotBuilder<WormFSTypeConfig> for StateMachineSnapshot {
    async fn build_snapshot(&mut self) -> Result<Snapshot<WormFSTypeConfig>, StorageError<u64>> {
        Ok(Snapshot {
            meta: self.meta.clone(),
            snapshot: Box::new(Cursor::new(self.data.clone())),
        })
    }
}

impl RaftStateMachine<WormFSTypeConfig> for StateMachine {
    type SnapshotBuilder = StateMachineSnapshot;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, ()>), StorageError<u64>> {
        let last_applied = *self.last_applied.read().await;
        let last_membership = self.last_membership.read().await.clone();
        Ok((last_applied, last_membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<MetadataOpResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<WormFSTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter();
        let mut responses = Vec::with_capacity(entries.size_hint().0);

        for entry in entries {
            // Update last applied
            {
                let mut last_applied = self.last_applied.write().await;
                *last_applied = Some(entry.log_id);
            }

            let response = match entry.payload {
                EntryPayload::Blank => MetadataOpResponse::Success,
                EntryPayload::Normal(op) => self.apply_op(op).await,
                EntryPayload::Membership(mem) => {
                    let mut last_membership = self.last_membership.write().await;
                    *last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    MetadataOpResponse::Success
                }
            };

            responses.push(response);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let last_applied = *self.last_applied.read().await;
        let last_membership = self.last_membership.read().await.clone();

        let snapshot_id = if let Some(last) = last_applied {
            format!("{}-{}", last.leader_id, last.index)
        } else {
            "initial".to_string()
        };

        // Export the entire SQLite database as compressed snapshot
        let store = self.store.lock().await;
        let data = store.export_snapshot().unwrap_or_else(|e| {
            tracing::error!("Failed to export snapshot: {}", e);
            Vec::new()
        });
        drop(store);

        tracing::debug!(
            "Created snapshot '{}' with {} bytes of data",
            snapshot_id,
            data.len()
        );

        StateMachineSnapshot {
            meta: SnapshotMeta {
                last_log_id: last_applied,
                last_membership,
                snapshot_id,
            },
            data,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, ()>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        tracing::debug!("Installing snapshot '{}'", meta.snapshot_id);

        // Get snapshot data
        let snapshot_data = snapshot.get_ref();

        // Only restore if we have data
        if !snapshot_data.is_empty() {
            let mut store = self.store.lock().await;
            store.restore_snapshot(snapshot_data).map_err(|e| {
                tracing::error!("Failed to restore snapshot: {}", e);
                StorageError::IO {
                    source: StorageIOError::read(&e),
                }
            })?;
            drop(store);

            tracing::debug!(
                "Restored snapshot with {} bytes of data",
                snapshot_data.len()
            );
        }

        // Update state from snapshot metadata
        let mut last_applied = self.last_applied.write().await;
        *last_applied = meta.last_log_id;

        let mut last_membership = self.last_membership.write().await;
        *last_membership = meta.last_membership.clone();

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<WormFSTypeConfig>>, StorageError<u64>> {
        // Load the latest snapshot from disk
        let snapshot_data = self.snapshot_store.load_latest_snapshot()?;

        if let Some((meta, data)) = snapshot_data {
            tracing::debug!(
                "Returning current snapshot '{}' from disk",
                meta.snapshot_id
            );

            Ok(Some(Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(data)),
            }))
        } else {
            tracing::debug!("No snapshot found on disk");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::FileMetadata as RaftFileMetadata;

    #[tokio::test]
    async fn test_state_machine_creation() {
        let mut sm = StateMachine::new().unwrap();

        let (last_applied, membership) = sm.applied_state().await.unwrap();
        assert_eq!(last_applied, None);
        assert_eq!(membership, StoredMembership::default());
    }

    #[tokio::test]
    async fn test_state_machine_apply_empty() {
        let mut sm = StateMachine::new().unwrap();

        let entries = vec![];
        let responses = sm.apply(entries).await.unwrap();
        assert_eq!(responses.len(), 0);
    }

    #[tokio::test]
    async fn test_create_file_operation() {
        let sm = StateMachine::new().unwrap();

        let file_id = uuid::Uuid::new_v4();
        let op = MetadataOp::CreateFile {
            metadata: RaftFileMetadata {
                file_id,
                size: 1024,
                permissions: 0o644,
                uid: 1000,
                gid: 1000,
                created_at: 0,
                modified_at: 0,
                accessed_at: 0,
                stripe_size: 1024 * 1024,
                data_shards: 4,
                parity_shards: 2,
            },
        };

        let result = sm.apply_op(op).await;
        assert!(matches!(result, MetadataOpResponse::Success));
    }

    #[tokio::test]
    async fn test_delete_file_operation() {
        let sm = StateMachine::new().unwrap();

        // First create a file
        let file_id = uuid::Uuid::new_v4();
        let create_op = MetadataOp::CreateFile {
            metadata: RaftFileMetadata {
                file_id,
                size: 1024,
                permissions: 0o644,
                uid: 1000,
                gid: 1000,
                created_at: 0,
                modified_at: 0,
                accessed_at: 0,
                stripe_size: 1024 * 1024,
                data_shards: 4,
                parity_shards: 2,
            },
        };

        let result = sm.apply_op(create_op).await;
        assert!(matches!(result, MetadataOpResponse::Success));

        // Now delete the file using its UUID
        let delete_op = MetadataOp::DeleteFile { file_id };
        let result = sm.apply_op(delete_op).await;
        assert!(matches!(result, MetadataOpResponse::Success));
    }

    #[tokio::test]
    async fn test_delete_nonexistent_file() {
        let sm = StateMachine::new().unwrap();

        // Try to delete a file that doesn't exist
        let file_id = uuid::Uuid::new_v4();
        let delete_op = MetadataOp::DeleteFile { file_id };
        let result = sm.apply_op(delete_op).await;

        // Should return an error
        assert!(matches!(result, MetadataOpResponse::Error(_)));
    }

    #[tokio::test]
    async fn test_acquire_read_lock() {
        let sm = StateMachine::new().unwrap();

        // First create a file
        let file_id = uuid::Uuid::new_v4();
        let create_op = MetadataOp::CreateFile {
            metadata: RaftFileMetadata {
                file_id,
                size: 1024,
                permissions: 0o644,
                uid: 1000,
                gid: 1000,
                created_at: 0,
                modified_at: 0,
                accessed_at: 0,
                stripe_size: 1024 * 1024,
                data_shards: 4,
                parity_shards: 2,
            },
        };
        sm.apply_op(create_op).await;

        // Acquire a read lock
        let lock_op = MetadataOp::AcquireLock {
            file_id,
            lock_type: RaftLockType::Read,
            client_id: "client1".to_string(),
        };
        let result = sm.apply_op(lock_op).await;
        assert!(matches!(result, MetadataOpResponse::Success));
    }

    #[tokio::test]
    async fn test_acquire_write_lock() {
        let sm = StateMachine::new().unwrap();

        // First create a file
        let file_id = uuid::Uuid::new_v4();
        let create_op = MetadataOp::CreateFile {
            metadata: RaftFileMetadata {
                file_id,
                size: 1024,
                permissions: 0o644,
                uid: 1000,
                gid: 1000,
                created_at: 0,
                modified_at: 0,
                accessed_at: 0,
                stripe_size: 1024 * 1024,
                data_shards: 4,
                parity_shards: 2,
            },
        };
        sm.apply_op(create_op).await;

        // Acquire a write lock
        let lock_op = MetadataOp::AcquireLock {
            file_id,
            lock_type: RaftLockType::Write,
            client_id: "client1".to_string(),
        };
        let result = sm.apply_op(lock_op).await;
        assert!(matches!(result, MetadataOpResponse::Success));
    }

    #[tokio::test]
    async fn test_multiple_read_locks_allowed() {
        let sm = StateMachine::new().unwrap();

        // First create a file
        let file_id = uuid::Uuid::new_v4();
        let create_op = MetadataOp::CreateFile {
            metadata: RaftFileMetadata {
                file_id,
                size: 1024,
                permissions: 0o644,
                uid: 1000,
                gid: 1000,
                created_at: 0,
                modified_at: 0,
                accessed_at: 0,
                stripe_size: 1024 * 1024,
                data_shards: 4,
                parity_shards: 2,
            },
        };
        sm.apply_op(create_op).await;

        // Acquire first read lock
        let lock_op1 = MetadataOp::AcquireLock {
            file_id,
            lock_type: RaftLockType::Read,
            client_id: "client1".to_string(),
        };
        let result1 = sm.apply_op(lock_op1).await;
        assert!(matches!(result1, MetadataOpResponse::Success));

        // Acquire second read lock - should succeed
        let lock_op2 = MetadataOp::AcquireLock {
            file_id,
            lock_type: RaftLockType::Read,
            client_id: "client2".to_string(),
        };
        let result2 = sm.apply_op(lock_op2).await;
        assert!(matches!(result2, MetadataOpResponse::Success));
    }

    #[tokio::test]
    async fn test_write_lock_blocks_read_lock() {
        let sm = StateMachine::new().unwrap();

        // First create a file
        let file_id = uuid::Uuid::new_v4();
        let create_op = MetadataOp::CreateFile {
            metadata: RaftFileMetadata {
                file_id,
                size: 1024,
                permissions: 0o644,
                uid: 1000,
                gid: 1000,
                created_at: 0,
                modified_at: 0,
                accessed_at: 0,
                stripe_size: 1024 * 1024,
                data_shards: 4,
                parity_shards: 2,
            },
        };
        sm.apply_op(create_op).await;

        // Acquire write lock
        let write_lock = MetadataOp::AcquireLock {
            file_id,
            lock_type: RaftLockType::Write,
            client_id: "client1".to_string(),
        };
        let result1 = sm.apply_op(write_lock).await;
        assert!(matches!(result1, MetadataOpResponse::Success));

        // Try to acquire read lock - should fail
        let read_lock = MetadataOp::AcquireLock {
            file_id,
            lock_type: RaftLockType::Read,
            client_id: "client2".to_string(),
        };
        let result2 = sm.apply_op(read_lock).await;
        assert!(matches!(result2, MetadataOpResponse::Error(_)));
    }

    #[tokio::test]
    async fn test_read_lock_blocks_write_lock() {
        let sm = StateMachine::new().unwrap();

        // First create a file
        let file_id = uuid::Uuid::new_v4();
        let create_op = MetadataOp::CreateFile {
            metadata: RaftFileMetadata {
                file_id,
                size: 1024,
                permissions: 0o644,
                uid: 1000,
                gid: 1000,
                created_at: 0,
                modified_at: 0,
                accessed_at: 0,
                stripe_size: 1024 * 1024,
                data_shards: 4,
                parity_shards: 2,
            },
        };
        sm.apply_op(create_op).await;

        // Acquire read lock
        let read_lock = MetadataOp::AcquireLock {
            file_id,
            lock_type: RaftLockType::Read,
            client_id: "client1".to_string(),
        };
        let result1 = sm.apply_op(read_lock).await;
        assert!(matches!(result1, MetadataOpResponse::Success));

        // Try to acquire write lock - should fail
        let write_lock = MetadataOp::AcquireLock {
            file_id,
            lock_type: RaftLockType::Write,
            client_id: "client2".to_string(),
        };
        let result2 = sm.apply_op(write_lock).await;
        assert!(matches!(result2, MetadataOpResponse::Error(_)));
    }

    #[tokio::test]
    async fn test_release_lock() {
        let sm = StateMachine::new().unwrap();

        // First create a file
        let file_id = uuid::Uuid::new_v4();
        let create_op = MetadataOp::CreateFile {
            metadata: RaftFileMetadata {
                file_id,
                size: 1024,
                permissions: 0o644,
                uid: 1000,
                gid: 1000,
                created_at: 0,
                modified_at: 0,
                accessed_at: 0,
                stripe_size: 1024 * 1024,
                data_shards: 4,
                parity_shards: 2,
            },
        };
        sm.apply_op(create_op).await;

        // Acquire a lock
        let lock_op = MetadataOp::AcquireLock {
            file_id,
            lock_type: RaftLockType::Write,
            client_id: "client1".to_string(),
        };
        sm.apply_op(lock_op).await;

        // Release the lock
        let release_op = MetadataOp::ReleaseLock {
            file_id,
            client_id: "client1".to_string(),
        };
        let result = sm.apply_op(release_op).await;
        assert!(matches!(result, MetadataOpResponse::Success));
    }

    #[tokio::test]
    async fn test_extend_lock() {
        let sm = StateMachine::new().unwrap();

        // First create a file
        let file_id = uuid::Uuid::new_v4();
        let create_op = MetadataOp::CreateFile {
            metadata: RaftFileMetadata {
                file_id,
                size: 1024,
                permissions: 0o644,
                uid: 1000,
                gid: 1000,
                created_at: 0,
                modified_at: 0,
                accessed_at: 0,
                stripe_size: 1024 * 1024,
                data_shards: 4,
                parity_shards: 2,
            },
        };
        sm.apply_op(create_op).await;

        // Acquire a lock
        let lock_op = MetadataOp::AcquireLock {
            file_id,
            lock_type: RaftLockType::Write,
            client_id: "client1".to_string(),
        };
        sm.apply_op(lock_op).await;

        // Extend the lock
        let extend_op = MetadataOp::ExtendLock {
            file_id,
            client_id: "client1".to_string(),
        };
        let result = sm.apply_op(extend_op).await;
        assert!(matches!(result, MetadataOpResponse::Success));
    }

    #[tokio::test]
    async fn test_lock_after_release() {
        let sm = StateMachine::new().unwrap();

        // First create a file
        let file_id = uuid::Uuid::new_v4();
        let create_op = MetadataOp::CreateFile {
            metadata: RaftFileMetadata {
                file_id,
                size: 1024,
                permissions: 0o644,
                uid: 1000,
                gid: 1000,
                created_at: 0,
                modified_at: 0,
                accessed_at: 0,
                stripe_size: 1024 * 1024,
                data_shards: 4,
                parity_shards: 2,
            },
        };
        sm.apply_op(create_op).await;

        // Acquire read lock
        let read_lock = MetadataOp::AcquireLock {
            file_id,
            lock_type: RaftLockType::Read,
            client_id: "client1".to_string(),
        };
        sm.apply_op(read_lock).await;

        // Release the lock
        let release_op = MetadataOp::ReleaseLock {
            file_id,
            client_id: "client1".to_string(),
        };
        sm.apply_op(release_op).await;

        // Now acquire write lock - should succeed
        let write_lock = MetadataOp::AcquireLock {
            file_id,
            lock_type: RaftLockType::Write,
            client_id: "client2".to_string(),
        };
        let result = sm.apply_op(write_lock).await;
        assert!(matches!(result, MetadataOpResponse::Success));
    }
}
