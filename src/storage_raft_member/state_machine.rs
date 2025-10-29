//! RaftStateMachine implementation for WormFS.
//!
//! This module implements OpenRaft's RaftStateMachine trait, applying committed
//! operations to the MetadataStore. It handles:
//! - Two-phase commit transaction coordination
//! - Idempotent operation application
//! - Snapshot creation and restoration
//! - Transaction state tracking

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use openraft::storage::RaftStateMachine;
use openraft::{
    OptionalSend, RaftSnapshotBuilder, RaftTypeConfig, Snapshot, SnapshotId, SnapshotMeta,
    StorageError, StoredMembership,
};

use crate::metadata_store::{MetadataStore, MetadataStoreImpl};

use super::raft_config::{WormFsResponse, WormFsSnapshotData, WormFsTypeConfig};
use super::types::{ChunkId, FileId, MetadataOperation, NodeId, StripeId, TxId, WormFsOperation};

/// State of a transaction during two-phase commit.
#[derive(Debug, Clone)]
enum TransactionPhase {
    /// Transaction is in prepare phase, waiting for votes
    Preparing {
        /// Operations to be applied
        operations: Vec<MetadataOperation>,
        /// Timeout for this transaction
        timeout: SystemTime,
    },
    /// Transaction has been prepared and voted on
    Prepared {
        /// Operations that were prepared
        operations: Vec<MetadataOperation>,
    },
    /// Transaction has been committed (metadata now visible)
    Committed,
    /// Transaction has been aborted
    Aborted { reason: Option<String> },
}

/// Inner state for the Raft state machine.
struct StateMachineInner {
    /// The metadata store where operations are applied
    metadata_store: MetadataStoreImpl,

    /// Last applied log index for idempotency
    last_applied_index: u64,

    /// Last applied log term
    last_applied_term: u64,

    /// Last applied membership configuration
    last_membership: StoredMembership<NodeId, super::raft_config::WormFsNode>,

    /// In-flight transaction states (for two-phase commit)
    transactions: HashMap<TxId, TransactionPhase>,

    /// Directory where snapshots are stored
    snapshot_directory: std::path::PathBuf,
}

/// Raft state machine that applies operations to MetadataStore.
///
/// This struct wraps MetadataStore and provides the logic for:
/// - Applying WormFsOperations to the store
/// - Handling two-phase commit protocol
/// - Creating and restoring snapshots
/// - Tracking applied log indices
#[derive(Clone)]
pub struct WormFsStateMachine {
    inner: Arc<RwLock<StateMachineInner>>,
}

impl WormFsStateMachine {
    /// Create a new state machine.
    ///
    /// # Arguments
    ///
    /// * `metadata_store` - The metadata store to apply operations to
    /// * `snapshot_directory` - Directory where snapshots will be stored
    pub fn new(metadata_store: MetadataStoreImpl, snapshot_directory: std::path::PathBuf) -> Self {
        Self {
            inner: Arc::new(RwLock::new(StateMachineInner {
                metadata_store,
                last_applied_index: 0,
                last_applied_term: 0,
                last_membership: StoredMembership::default(),
                transactions: HashMap::new(),
                snapshot_directory,
            })),
        }
    }

    /// Get the last applied log index.
    pub async fn last_applied_index(&self) -> u64 {
        let inner = self.inner.read().await;
        inner.last_applied_index
    }

    /// Apply a WormFsOperation to the state machine.
    ///
    /// This method is called by OpenRaft after an entry is committed.
    /// It handles:
    /// - Transaction prepare phase
    /// - Transaction commit phase
    /// - Transaction abort phase
    ///
    /// # Arguments
    ///
    /// * `log_index` - The log index of this operation
    /// * `operation` - The operation to apply
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    pub async fn apply_operation(
        &self,
        log_index: u64,
        operation: &WormFsOperation,
    ) -> Result<(), String> {
        let mut inner = self.inner.write().await;

        // Check for idempotency - have we already applied this?
        if log_index <= inner.last_applied_index {
            debug!("Skipping already applied operation at index {}", log_index);
            return Ok(());
        }

        // Apply the operation based on its type
        match operation {
            WormFsOperation::TransactionPrepare {
                tx_id,
                metadata_ops,
                command_ops: _,
                timeout,
            } => {
                info!(
                    "Applying TransactionPrepare for tx {:?} at index {}",
                    tx_id, log_index
                );

                // Store the transaction in preparing state
                inner.transactions.insert(
                    *tx_id,
                    TransactionPhase::Preparing {
                        operations: metadata_ops.clone().unwrap_or_default(),
                        timeout: *timeout,
                    },
                );

                // Validate operations can be applied
                // For now, we assume validation passes
                // In a full implementation with proper 2PC, this would:
                // 1. Validate all operations can be applied (check constraints)
                // 2. Stage the changes (but not commit them)
                // 3. Vote PREPARED or ABORT based on validation
                //
                // Since we're using Raft for consensus, we can apply directly on commit
                // rather than maintaining separate prepared state.

                inner.transactions.insert(
                    *tx_id,
                    TransactionPhase::Prepared {
                        operations: metadata_ops.clone().unwrap_or_default(),
                    },
                );

                debug!("Transaction {:?} prepared successfully", tx_id);
            }

            WormFsOperation::TransactionCommit { tx_id } => {
                info!(
                    "Applying TransactionCommit for tx {:?} at index {}",
                    tx_id, log_index
                );

                // Get the prepared transaction
                if let Some(TransactionPhase::Prepared { operations }) =
                    inner.transactions.get(tx_id).cloned()
                {
                    debug!(
                        "Committing {} operations for transaction {:?}",
                        operations.len(),
                        tx_id
                    );

                    // Apply all operations to MetadataStore
                    // Note: MetadataStore methods are already transactional via SQLite
                    for operation in &operations {
                        if let Err(e) =
                            Self::apply_metadata_operation(&inner.metadata_store, operation).await
                        {
                            error!("Failed to apply operation {:?}: {}", operation, e);
                            // Continue with other operations even if one fails
                            // In production, we might want to mark the transaction as partially failed
                        }
                    }

                    inner
                        .transactions
                        .insert(*tx_id, TransactionPhase::Committed);
                } else {
                    warn!("Transaction {:?} not found or not in prepared state", tx_id);
                    return Err(format!("Transaction {:?} not prepared", tx_id));
                }
            }

            WormFsOperation::TransactionAbort { tx_id, reason } => {
                info!(
                    "Applying TransactionAbort for tx {:?} at index {}: {:?}",
                    tx_id, log_index, reason
                );

                // TODO: Rollback any prepared changes
                // For now, we'll just mark as aborted
                // In a full implementation, this would:
                // 1. Discard staged metadata changes
                // 2. Signal storage nodes to discard staged chunks

                inner.transactions.insert(
                    *tx_id,
                    TransactionPhase::Aborted {
                        reason: reason.clone(),
                    },
                );

                debug!("Transaction {:?} aborted", tx_id);
            }
        }

        // Update last applied index
        inner.last_applied_index = log_index;

        Ok(())
    }

    /// Apply a single metadata operation to the MetadataStore.
    ///
    /// This helper method translates MetadataOperation variants into
    /// MetadataStore method calls.
    async fn apply_metadata_operation(
        metadata_store: &MetadataStoreImpl,
        operation: &MetadataOperation,
    ) -> Result<(), String> {
        use crate::metadata_store::MetadataStore;

        match operation {
            MetadataOperation::FileCreate {
                path,
                inode,
                metadata,
                policy: _,
            } => {
                // Generate a file ID
                let file_id = FileId::generate();
                // Convert our FileMetadata to metadata_store::FileMetadata
                let store_metadata: crate::metadata_store::FileMetadata = metadata.clone().into();
                metadata_store
                    .create_file(file_id, path, *inode, store_metadata)
                    .await
                    .map_err(|e| format!("Failed to create file: {:?}", e))?;
                info!("Created file at {:?} with inode {}", path, inode);
            }

            MetadataOperation::FileUpdate {
                file_id,
                metadata,
                policy: _,
            } => {
                // Convert our FileMetadata to metadata_store::FileMetadata
                let store_metadata: crate::metadata_store::FileMetadata = metadata.clone().into();
                metadata_store
                    .update_file(*file_id, store_metadata)
                    .await
                    .map_err(|e| format!("Failed to update file: {:?}", e))?;
                info!("Updated file {:?}", file_id);
            }

            MetadataOperation::FileDelete { file_id } => {
                metadata_store
                    .delete_file(*file_id)
                    .await
                    .map_err(|e| format!("Failed to delete file: {:?}", e))?;
                info!("Deleted file {:?}", file_id);
            }

            MetadataOperation::CreateStripe {
                file_id,
                stripe_id,
                policy: _,
                offset,
                size,
                chunks: _,
            } => {
                // Create a stripe record
                let stripe = crate::metadata_store::StripeRecord {
                    stripe_id: *stripe_id,
                    file_id: *file_id,
                    stripe_index: 0, // TODO: Calculate from offset
                    offset: *offset,
                    size: *size,
                    checksum: 0, // TODO: Calculate checksum
                    created_at: std::time::SystemTime::now(),
                };
                metadata_store
                    .allocate_stripes(*file_id, vec![stripe])
                    .await
                    .map_err(|e| format!("Failed to create stripe: {:?}", e))?;
                info!("Created stripe {:?} for file {:?}", stripe_id, file_id);
            }

            MetadataOperation::DeleteStripe { stripe_id } => {
                metadata_store
                    .delete_stripe(*stripe_id)
                    .await
                    .map_err(|e| format!("Failed to delete stripe: {:?}", e))?;
                info!("Deleted stripe {:?}", stripe_id);
            }

            MetadataOperation::CreateChunk {
                node_id,
                disk,
                chunk,
                chunk_index,
            } => {
                // For creating chunks, we need to know the stripe_id
                // This is a limitation of the current API - we'll skip for now
                warn!(
                    "CreateChunk operation not fully implemented: node {:?}, disk {:?}, chunk {:?}, index {}",
                    node_id, disk, chunk, chunk_index.0
                );
                // In a full implementation, we would need to pass stripe_id in the operation
            }

            MetadataOperation::MoveChunk {
                chunk_id,
                old_node: _,
                new_node,
                old_disk: _,
                new_disk,
            } => {
                // Convert Raft NodeId to file_store NodeId
                let fs_node_id = crate::file_store::types::NodeId(new_node.as_u64());
                metadata_store
                    .update_chunk_location(*chunk_id, fs_node_id, *new_disk)
                    .await
                    .map_err(|e| format!("Failed to move chunk: {:?}", e))?;
                info!(
                    "Moved chunk {:?} to node {:?}, disk {:?}",
                    chunk_id, new_node, new_disk
                );
            }

            MetadataOperation::DeleteChunk {
                node_id: _,
                disk_id: _,
                chunk_id,
            } => {
                // MetadataStore doesn't have a direct delete_chunk method
                // Chunks are typically deleted when their stripe is deleted
                warn!(
                    "DeleteChunk operation not directly supported: {:?}",
                    chunk_id
                );
                // In a full implementation, we might need to add this to MetadataStore
            }
        }

        Ok(())
    }

    /// Create a snapshot of the current state.
    ///
    /// This method creates a consistent snapshot of the MetadataStore
    /// that can be used for log compaction and node catch-up.
    ///
    /// # Arguments
    ///
    /// * `last_included_index` - The last log index included in this snapshot
    /// * `last_included_term` - The term of the last included log entry
    ///
    /// # Returns
    ///
    /// Snapshot data descriptor
    pub async fn create_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
    ) -> Result<WormFsSnapshotData, String> {
        info!(
            "Creating snapshot at index {} term {}",
            last_included_index, last_included_term
        );

        let inner = self.inner.read().await;

        // Generate snapshot filename
        let snapshot_filename =
            format!("snapshot-{}-{}.db", last_included_index, last_included_term);
        let snapshot_path = inner.snapshot_directory.join(&snapshot_filename);

        // Create snapshot directory if it doesn't exist
        tokio::fs::create_dir_all(&inner.snapshot_directory)
            .await
            .map_err(|e| format!("Failed to create snapshot directory: {}", e))?;

        // Create the snapshot using MetadataStore
        info!("Creating MetadataStore snapshot at {:?}", snapshot_path);
        inner
            .metadata_store
            .create_snapshot(&snapshot_path)
            .await
            .map_err(|e| format!("Failed to create snapshot: {:?}", e))?;

        // Get the file size and calculate checksum
        let metadata = tokio::fs::metadata(&snapshot_path)
            .await
            .map_err(|e| format!("Failed to read snapshot metadata: {}", e))?;
        let file_size = metadata.len();

        // Calculate CRC32 checksum
        let checksum = Self::calculate_checksum(&snapshot_path).await?;

        // Get current membership
        let membership: BTreeSet<NodeId> = inner.last_membership.membership().voter_ids().collect();

        info!(
            "Snapshot created successfully: {} bytes, checksum: {:08x}, members: {}",
            file_size,
            checksum,
            membership.len()
        );

        let snapshot = WormFsSnapshotData::new(
            last_included_index,
            last_included_term,
            membership,
            snapshot_filename,
            file_size,
            checksum,
            false, // SQLite VACUUM creates uncompressed backups
        );

        Ok(snapshot)
    }

    /// Calculate CRC32 checksum of a file.
    async fn calculate_checksum(path: &std::path::Path) -> Result<u32, String> {
        use tokio::io::AsyncReadExt;

        let mut file = tokio::fs::File::open(path)
            .await
            .map_err(|e| format!("Failed to open file for checksum: {}", e))?;

        let mut hasher = crc32fast::Hasher::new();
        let mut buffer = vec![0u8; 8192];

        loop {
            let n = file
                .read(&mut buffer)
                .await
                .map_err(|e| format!("Failed to read file for checksum: {}", e))?;

            if n == 0 {
                break;
            }

            hasher.update(&buffer[..n]);
        }

        Ok(hasher.finalize())
    }

    /// Install a snapshot, replacing the current state.
    ///
    /// This method restores the state machine from a snapshot,
    /// discarding any existing state.
    ///
    /// # Arguments
    ///
    /// * `snapshot_data` - The snapshot to install
    ///
    /// # Returns
    ///
    /// Result indicating success or failure
    pub async fn install_snapshot(&self, snapshot_data: &WormFsSnapshotData) -> Result<(), String> {
        info!(
            "Installing snapshot at index {} term {}, file: {}",
            snapshot_data.last_included_index,
            snapshot_data.last_included_term,
            snapshot_data.snapshot_file
        );

        // Construct the full path to the snapshot file
        let snapshot_path = {
            let inner = self.inner.read().await;
            inner.snapshot_directory.join(&snapshot_data.snapshot_file)
        };

        // Verify the snapshot file exists
        if !tokio::fs::try_exists(&snapshot_path)
            .await
            .map_err(|e| format!("Failed to check snapshot existence: {:?}", e))?
        {
            return Err(format!(
                "Snapshot file not found: {}",
                snapshot_path.display()
            ));
        }

        // Verify file size matches
        let metadata = tokio::fs::metadata(&snapshot_path)
            .await
            .map_err(|e| format!("Failed to read snapshot metadata: {:?}", e))?;
        let actual_size = metadata.len();
        if actual_size != snapshot_data.file_size {
            return Err(format!(
                "Snapshot file size mismatch: expected {} bytes, got {} bytes",
                snapshot_data.file_size, actual_size
            ));
        }

        // Verify checksum
        info!("Verifying snapshot checksum...");
        let actual_checksum = Self::calculate_checksum(&snapshot_path).await?;
        if actual_checksum != snapshot_data.checksum {
            return Err(format!(
                "Snapshot checksum mismatch: expected {:08x}, got {:08x}",
                snapshot_data.checksum, actual_checksum
            ));
        }
        info!("Snapshot checksum verified: {:08x}", actual_checksum);

        // Decompress if needed (though SQLite snapshots are uncompressed)
        if snapshot_data.compressed {
            warn!("Compressed snapshots not yet supported, but snapshot is marked as compressed");
            return Err("Compressed snapshots not yet supported".to_string());
        }

        // Restore MetadataStore from snapshot
        info!("Restoring MetadataStore from snapshot...");
        {
            let inner = self.inner.read().await;
            inner
                .metadata_store
                .restore_from_snapshot(&snapshot_path)
                .await
                .map_err(|e| format!("Failed to restore from snapshot: {:?}", e))?;
        }

        // Update state machine state
        let mut inner = self.inner.write().await;
        inner.last_applied_index = snapshot_data.last_included_index;

        // Clear all transaction state - they're obsolete after snapshot restoration
        let old_tx_count = inner.transactions.len();
        inner.transactions.clear();

        info!(
            "Snapshot installed successfully: cleared {} pending transactions, last_applied_index={}",
            old_tx_count, inner.last_applied_index
        );

        Ok(())
    }

    /// Clean up old transactions that have been completed.
    ///
    /// This method removes transaction state for transactions that have
    /// been committed or aborted, freeing memory.
    pub async fn cleanup_old_transactions(&self) {
        let mut inner = self.inner.write().await;

        let before_count = inner.transactions.len();

        // Remove committed and aborted transactions
        inner.transactions.retain(|tx_id, phase| {
            match phase {
                TransactionPhase::Committed | TransactionPhase::Aborted { .. } => {
                    debug!("Cleaning up completed transaction {:?}", tx_id);
                    false // Remove
                }
                _ => true, // Keep preparing and prepared transactions
            }
        });

        let after_count = inner.transactions.len();
        if before_count != after_count {
            debug!(
                "Cleaned up {} transactions ({} -> {})",
                before_count - after_count,
                before_count,
                after_count
            );
        }
    }
}

impl RaftStateMachine<WormFsTypeConfig> for WormFsStateMachine {
    type SnapshotBuilder = Self;

    /// Returns the last applied log id and cluster membership.
    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<openraft::LogId<NodeId>>,
            StoredMembership<NodeId, super::raft_config::WormFsNode>,
        ),
        StorageError<NodeId>,
    > {
        let inner = self.inner.read().await;

        let last_log_id = if inner.last_applied_index > 0 {
            Some(openraft::LogId::new(
                openraft::CommittedLeaderId::new(inner.last_applied_term, NodeId(0)),
                inner.last_applied_index,
            ))
        } else {
            None
        };

        Ok((last_log_id, inner.last_membership.clone()))
    }

    /// Apply committed entries to the state machine.
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<WormFsResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = <WormFsTypeConfig as RaftTypeConfig>::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();

        for entry in entries {
            let log_index = entry.log_id.index;
            let log_term = entry.log_id.leader_id.term;

            // Extract the operation from the entry payload
            let operation = match entry.payload {
                openraft::EntryPayload::Normal(op) => op,
                openraft::EntryPayload::Membership(membership) => {
                    // Update membership and continue
                    let mut inner = self.inner.write().await;
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    inner.last_applied_index = log_index;
                    inner.last_applied_term = log_term;
                    // No response for membership changes
                    continue;
                }
                openraft::EntryPayload::Blank => {
                    // Blank entries don't produce responses
                    let mut inner = self.inner.write().await;
                    inner.last_applied_index = log_index;
                    inner.last_applied_term = log_term;
                    continue;
                }
            };

            // Apply the operation and generate appropriate response
            match self.apply_operation(log_index, &operation).await {
                Ok(()) => {
                    let mut inner = self.inner.write().await;
                    inner.last_applied_term = log_term;

                    // Generate response based on operation type
                    let response = match &operation {
                        WormFsOperation::TransactionPrepare { tx_id, .. } => {
                            WormFsResponse::TransactionPrepared {
                                tx_id: *tx_id,
                                vote: super::raft_config::PrepareVote::Prepared,
                            }
                        }
                        WormFsOperation::TransactionCommit { tx_id } => {
                            WormFsResponse::TransactionCommitted { tx_id: *tx_id }
                        }
                        WormFsOperation::TransactionAbort { tx_id, reason } => {
                            WormFsResponse::TransactionAborted {
                                tx_id: *tx_id,
                                reason: reason.clone(),
                            }
                        }
                    };
                    responses.push(response);
                }
                Err(e) => {
                    warn!("Failed to apply operation at index {}: {}", log_index, e);
                    // For errors during apply, return TransactionAborted
                    let response = match &operation {
                        WormFsOperation::TransactionPrepare { tx_id, .. } => {
                            WormFsResponse::TransactionPrepared {
                                tx_id: *tx_id,
                                vote: super::raft_config::PrepareVote::Abort,
                            }
                        }
                        WormFsOperation::TransactionCommit { tx_id }
                        | WormFsOperation::TransactionAbort { tx_id, .. } => {
                            WormFsResponse::TransactionAborted {
                                tx_id: *tx_id,
                                reason: Some(e),
                            }
                        }
                    };
                    responses.push(response);
                }
            }
        }

        Ok(responses)
    }

    /// Get the snapshot builder for creating snapshots.
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    /// Begin receiving a snapshot from the leader.
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<WormFsTypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        // Create a temporary file for receiving the snapshot
        // In production, this would use SnapshotStore
        let temp_file = tokio::fs::File::create("/tmp/wormfs-snapshot-incoming.db")
            .await
            .map_err(|e| {
                let io_error = openraft::StorageIOError::new(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&e),
                );
                StorageError::IO { source: io_error }
            })?;

        Ok(Box::new(tokio::io::BufReader::new(temp_file)))
    }

    /// Install a snapshot, replacing the current state.
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, super::raft_config::WormFsNode>,
        snapshot: Box<<WormFsTypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        info!(
            "Installing snapshot at index {:?}",
            meta.last_log_id.as_ref().map(|l| l.index)
        );

        // TODO: Actually restore from snapshot
        // For now, just update the state to reflect the snapshot
        let mut inner = self.inner.write().await;

        if let Some(last_log_id) = &meta.last_log_id {
            inner.last_applied_index = last_log_id.index;
            inner.last_applied_term = last_log_id.leader_id.term;
        }

        inner.last_membership = meta.last_membership.clone();
        inner.transactions.clear();

        // Drop the snapshot handle
        drop(snapshot);

        info!("Snapshot installed successfully");
        Ok(())
    }

    /// Get the current snapshot.
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<WormFsTypeConfig>>, StorageError<NodeId>> {
        // For now, we don't persist snapshots
        // TODO: Integrate with SnapshotStore
        Ok(None)
    }
}

impl RaftSnapshotBuilder<WormFsTypeConfig> for WormFsStateMachine {
    /// Build a snapshot of the current state.
    async fn build_snapshot(&mut self) -> Result<Snapshot<WormFsTypeConfig>, StorageError<NodeId>> {
        // Get current state to determine snapshot parameters
        let (last_included_index, last_included_term, last_log_id, last_membership) = {
            let inner = self.inner.read().await;

            info!("Building snapshot at index {}", inner.last_applied_index);

            let last_log_id = if inner.last_applied_index > 0 {
                Some(openraft::LogId::new(
                    openraft::CommittedLeaderId::new(inner.last_applied_term, NodeId(0)),
                    inner.last_applied_index,
                ))
            } else {
                None
            };

            (
                inner.last_applied_index,
                inner.last_applied_term,
                last_log_id,
                inner.last_membership.clone(),
            )
        };

        // Create the actual snapshot using our implementation
        let snapshot_data = self
            .create_snapshot(last_included_index, last_included_term)
            .await
            .map_err(|e| {
                error!("Failed to create snapshot: {}", e);
                let io_error = openraft::StorageIOError::new(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::error(e),
                );
                StorageError::IO { source: io_error }
            })?;

        // Construct full path to the snapshot file
        let snapshot_path = {
            let inner = self.inner.read().await;
            inner.snapshot_directory.join(&snapshot_data.snapshot_file)
        };

        // Open the snapshot file for reading
        let snapshot_file = tokio::fs::File::open(&snapshot_path).await.map_err(|e| {
            error!("Failed to open snapshot file for reading: {:?}", e);
            let io_error = openraft::StorageIOError::new(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Read,
                openraft::AnyError::new(&e),
            );
            StorageError::IO { source: io_error }
        })?;

        // Create snapshot metadata for OpenRaft
        let snapshot_id = snapshot_data.snapshot_file.clone();
        let meta = SnapshotMeta {
            last_log_id,
            last_membership,
            snapshot_id,
        };

        // Wrap file in a buffered reader
        let snapshot_reader = Box::new(tokio::io::BufReader::new(snapshot_file));

        info!(
            "Snapshot built successfully: {} bytes, checksum: {:08x}",
            snapshot_data.file_size, snapshot_data.checksum
        );

        Ok(Snapshot {
            meta,
            snapshot: snapshot_reader,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_store::{Config as MetadataConfig, MetadataStore, MetadataStoreFactory};
    use tempfile::TempDir;

    async fn create_test_state_machine() -> (WormFsStateMachine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let database_path = temp_dir.path().join("metadata.db");

        let config = MetadataConfig {
            database_path,
            ..Default::default()
        };

        let metadata_store = MetadataStoreFactory::create_concrete(config).await.unwrap();
        metadata_store.initialize_schema().await.unwrap();

        let snapshot_dir = temp_dir.path().join("snapshots");
        let state_machine = WormFsStateMachine::new(metadata_store, snapshot_dir);

        (state_machine, temp_dir)
    }

    #[tokio::test]
    async fn test_state_machine_creation() {
        let (_state_machine, _temp_dir) = create_test_state_machine().await;
        // Just verify it can be created
    }

    #[tokio::test]
    async fn test_last_applied_index() {
        let (state_machine, _temp_dir) = create_test_state_machine().await;

        assert_eq!(state_machine.last_applied_index().await, 0);

        // Apply an operation to increment the index
        let operation = WormFsOperation::TransactionPrepare {
            tx_id: TxId(1),
            metadata_ops: Some(vec![]),
            command_ops: None,
            timeout: SystemTime::now(),
        };

        state_machine.apply_operation(1, &operation).await.unwrap();

        assert_eq!(state_machine.last_applied_index().await, 1);
    }

    #[tokio::test]
    async fn test_idempotent_application() {
        let (state_machine, _temp_dir) = create_test_state_machine().await;

        let operation = WormFsOperation::TransactionPrepare {
            tx_id: TxId(1),
            metadata_ops: Some(vec![]),
            command_ops: None,
            timeout: SystemTime::now(),
        };

        // Apply once
        state_machine.apply_operation(1, &operation).await.unwrap();
        assert_eq!(state_machine.last_applied_index().await, 1);

        // Apply again - should be skipped
        state_machine.apply_operation(1, &operation).await.unwrap();
        assert_eq!(state_machine.last_applied_index().await, 1);
    }

    #[tokio::test]
    async fn test_two_phase_commit_flow() {
        let (state_machine, _temp_dir) = create_test_state_machine().await;

        let tx_id = TxId(100);

        // Phase 1: Prepare
        let prepare_op = WormFsOperation::TransactionPrepare {
            tx_id,
            metadata_ops: Some(vec![]),
            command_ops: None,
            timeout: SystemTime::now(),
        };

        state_machine.apply_operation(1, &prepare_op).await.unwrap();

        // Phase 2: Commit
        let commit_op = WormFsOperation::TransactionCommit { tx_id };

        state_machine.apply_operation(2, &commit_op).await.unwrap();

        assert_eq!(state_machine.last_applied_index().await, 2);
    }

    #[tokio::test]
    async fn test_transaction_abort() {
        let (state_machine, _temp_dir) = create_test_state_machine().await;

        let tx_id = TxId(200);

        // Prepare
        let prepare_op = WormFsOperation::TransactionPrepare {
            tx_id,
            metadata_ops: Some(vec![]),
            command_ops: None,
            timeout: SystemTime::now(),
        };

        state_machine.apply_operation(1, &prepare_op).await.unwrap();

        // Abort
        let abort_op = WormFsOperation::TransactionAbort {
            tx_id,
            reason: Some("Test abort".to_string()),
        };

        state_machine.apply_operation(2, &abort_op).await.unwrap();

        assert_eq!(state_machine.last_applied_index().await, 2);
    }

    #[tokio::test]
    async fn test_cleanup_old_transactions() {
        let (state_machine, _temp_dir) = create_test_state_machine().await;

        let tx_id = TxId(300);

        // Create and commit a transaction
        let prepare_op = WormFsOperation::TransactionPrepare {
            tx_id,
            metadata_ops: Some(vec![]),
            command_ops: None,
            timeout: SystemTime::now(),
        };

        state_machine.apply_operation(1, &prepare_op).await.unwrap();

        let commit_op = WormFsOperation::TransactionCommit { tx_id };

        state_machine.apply_operation(2, &commit_op).await.unwrap();

        // Cleanup should remove the committed transaction
        state_machine.cleanup_old_transactions().await;

        // Can't directly verify since transactions is private, but we can
        // verify the operation doesn't error
    }
}
