//! RaftStateMachine implementation for WormFS.
//!
//! This module implements OpenRaft's RaftStateMachine trait, applying committed
//! operations to the MetadataStore. It handles:
//! - Two-phase commit transaction coordination
//! - Idempotent operation application
//! - Snapshot creation and restoration
//! - Transaction state tracking

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::metadata_store::MetadataStoreImpl;

use super::raft_config::WormFsSnapshotData;
use super::types::{MetadataOperation, TxId, WormFsOperation};

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

    /// In-flight transaction states (for two-phase commit)
    transactions: HashMap<TxId, TransactionPhase>,
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
    pub fn new(metadata_store: MetadataStoreImpl) -> Self {
        Self {
            inner: Arc::new(RwLock::new(StateMachineInner {
                metadata_store,
                last_applied_index: 0,
                transactions: HashMap::new(),
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

                // TODO: Actually prepare the operations in MetadataStore
                // For now, we'll just mark them as prepared
                // In a full implementation, this would:
                // 1. Validate all operations can be applied
                // 2. Stage the changes (but not commit them)
                // 3. Vote PREPARED or ABORT based on validation

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
                    inner.transactions.get(tx_id)
                {
                    // TODO: Apply all operations to MetadataStore atomically
                    // For now, we'll just mark as committed
                    // In a full implementation, this would:
                    // 1. Begin a database transaction
                    // 2. Apply all metadata operations
                    // 3. Commit the database transaction
                    // 4. Signal storage nodes to activate staged chunks

                    debug!(
                        "Committing {} operations for transaction {:?}",
                        operations.len(),
                        tx_id
                    );

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

        // TODO: Actually create a snapshot using MetadataStore's snapshot capabilities
        // For now, return a placeholder
        // In a full implementation, this would:
        // 1. Call metadata_store.create_snapshot()
        // 2. Get the snapshot file path and metadata
        // 3. Calculate checksum
        // 4. Return WormFsSnapshotData with all details

        let snapshot = WormFsSnapshotData::new(
            last_included_index,
            last_included_term,
            std::collections::BTreeSet::new(), // TODO: Get actual membership
            format!("snapshot-{}-{}.db", last_included_index, last_included_term),
            0,    // TODO: Get actual file size
            0,    // TODO: Calculate actual checksum
            true, // Compressed
        );

        info!("Snapshot created successfully");
        Ok(snapshot)
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
            "Installing snapshot at index {} term {}",
            snapshot_data.last_included_index, snapshot_data.last_included_term
        );

        let mut inner = self.inner.write().await;

        // TODO: Actually restore from snapshot
        // For now, just update the last applied index
        // In a full implementation, this would:
        // 1. Verify snapshot checksum
        // 2. Decompress if needed
        // 3. Restore MetadataStore from snapshot file
        // 4. Update last_applied_index
        // 5. Clear transaction state (they're obsolete)

        inner.last_applied_index = snapshot_data.last_included_index;
        inner.transactions.clear();

        info!("Snapshot installed successfully");
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

// TODO: Implement OpenRaft's RaftStateMachine trait
//
// This requires implementing the trait with correct lifetime parameters:
//
// #[async_trait]
// impl RaftStateMachine<WormFsTypeConfig> for WormFsStateMachine {
//     type SnapshotBuilder = Self;
//
//     async fn applied_state<'life0, 'async_trait>(
//         &'life0 mut self,
//     ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, WormFsNode>), StorageError<NodeId>>
//     where
//         'life0: 'async_trait,
//         Self: 'async_trait,
//     {
//         // Return the last applied log ID and cluster membership
//         todo!()
//     }
//
//     async fn apply<'life0, 'async_trait, I>(
//         &'life0 mut self,
//         entries: I,
//     ) -> Result<Vec<WormFsResponse>, StorageError<NodeId>>
//     where
//         I: IntoIterator<Item = Entry<WormFsTypeConfig>> + Send + 'async_trait,
//         I::IntoIter: Send,
//         'life0: 'async_trait,
//         Self: 'async_trait,
//     {
//         // Apply entries and return responses
//         todo!()
//     }
//
//     // ... other methods
// }

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

        let state_machine = WormFsStateMachine::new(metadata_store);

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
