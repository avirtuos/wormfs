//! RaftStateMachine implementation for WormFS.
#![allow(dead_code)]
//!
//! This module implements OpenRaft's RaftStateMachine trait, applying committed
//! operations to the MetadataStore. It handles:
//! - Two-phase commit transaction coordination
//! - Idempotent operation application
//! - Snapshot creation and restoration
//! - Transaction state tracking

use std::collections::{BTreeSet, HashMap};
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace, warn};

use openraft::storage::RaftStateMachine;
use openraft::{
    OptionalSend, RaftSnapshotBuilder, RaftTypeConfig, Snapshot, SnapshotMeta, StorageError,
    StoredMembership,
};

use crate::metadata_store::{MetadataStore, MetadataStoreImpl};
use crate::snapshot_store::{CompressionAlgorithm, SnapshotStore, SnapshotStoreImpl};

use super::raft_config::{WormFsResponse, WormFsSnapshotData, WormFsTypeConfig};
use super::types::{
    MetadataChangeEvent, MetadataChangeType, MetadataOperation, NodeId, TxId, WormFsOperation,
};
use super::utils::current_time_secs;

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

/// Subscription handle for metadata change events.
///
/// Each subscriber receives events via a broadcast channel with optional filtering.
pub(crate) struct Subscription {
    /// Channel for sending events to subscriber
    pub(crate) sender: tokio::sync::broadcast::Sender<MetadataChangeEvent>,
    /// Optional filter for specific event types
    pub(crate) filter: Option<Vec<MetadataChangeType>>,
}

/// Inner state for the Raft state machine.
pub(crate) struct StateMachineInner {
    /// The metadata store where operations are applied
    pub(crate) metadata_store: MetadataStoreImpl,

    /// Last applied log index for idempotency
    last_applied_index: u64,

    /// Last applied log term
    last_applied_term: u64,

    /// Last applied leader node ID
    last_applied_leader_id: NodeId,

    /// Last applied membership configuration
    last_membership: StoredMembership<NodeId, super::raft_config::WormFsNode>,

    /// In-flight transaction states (for two-phase commit)
    transactions: HashMap<TxId, TransactionPhase>,

    /// Directory where snapshots are stored
    snapshot_directory: std::path::PathBuf,

    /// SnapshotStore for managing persistent snapshots
    snapshot_store: Arc<SnapshotStoreImpl>,

    /// Compression algorithm for snapshots
    snapshot_compression: CompressionAlgorithm,

    /// Next snapshot ID (incremented for each snapshot)
    next_snapshot_id: u64,

    /// Mutex to serialize snapshot creation (prevents concurrent MetadataStore backups)
    snapshot_creation_lock: Arc<tokio::sync::Mutex<()>>,

    /// Path to the temporary snapshot file being received
    /// Set by begin_receiving_snapshot(), used by install_snapshot()
    incoming_snapshot_path: Option<std::path::PathBuf>,

    /// Active metadata change subscriptions
    pub(crate) subscriptions: Vec<Subscription>,

    /// Whether the state machine needs resynchronization due to apply failure
    needs_resync: AtomicBool,

    /// Reason for needing resync (if needs_resync is true)
    resync_reason: Option<String>,

    /// List of operations that failed and triggered resync
    failed_operations: Vec<String>,

    /// Timestamp when resync was triggered
    resync_triggered_at: Option<SystemTime>,

    /// Whether the state machine is in read-only mode (during resync)
    read_only_mode: AtomicBool,
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
    ///
    /// # Note
    ///
    /// This method creates a state machine without compression enabled.
    /// Use `new_with_config` for compression support.
    pub fn new(metadata_store: MetadataStoreImpl, snapshot_directory: std::path::PathBuf) -> Self {
        Self::new_with_config(metadata_store, snapshot_directory, false, 3)
    }

    /// Create a new state machine with snapshot compression configuration.
    ///
    /// # Arguments
    ///
    /// * `metadata_store` - The metadata store to apply operations to
    /// * `snapshot_directory` - Directory where snapshots will be stored
    /// * `enable_compression` - Whether to enable zstd compression for snapshots
    /// * `compression_level` - Compression level (1-22, higher = better compression)
    pub fn new_with_config(
        metadata_store: MetadataStoreImpl,
        snapshot_directory: std::path::PathBuf,
        enable_compression: bool,
        compression_level: i32,
    ) -> Self {
        // Create SnapshotStore with configured compression
        let snapshot_compression = if enable_compression {
            CompressionAlgorithm::Zstd {
                level: compression_level,
            }
        } else {
            CompressionAlgorithm::None
        };

        let snapshot_store_config = crate::snapshot_store::Config {
            storage_dir: snapshot_directory.clone(),
            retention_policy: crate::snapshot_store::RetentionPolicy {
                max_snapshots: 10,
                max_age: std::time::Duration::from_secs(30 * 24 * 60 * 60), // 30 days
                min_snapshots: 3,
            },
            compression: snapshot_compression,
            stream_chunk_size: 64 * 1024, // 64KB
        };

        let snapshot_store =
            SnapshotStoreImpl::new(snapshot_store_config).expect("Failed to create SnapshotStore");

        let snapshot_store_arc = Arc::new(snapshot_store);

        Self {
            inner: Arc::new(RwLock::new(StateMachineInner {
                metadata_store,
                last_applied_index: 0,
                last_applied_term: 0,
                last_applied_leader_id: NodeId(0),
                last_membership: StoredMembership::default(),
                transactions: HashMap::new(),
                snapshot_directory,
                snapshot_store: snapshot_store_arc,
                snapshot_compression,
                next_snapshot_id: 1,
                snapshot_creation_lock: Arc::new(tokio::sync::Mutex::new(())),
                incoming_snapshot_path: None,
                subscriptions: Vec::new(),
                needs_resync: AtomicBool::new(false),
                resync_reason: None,
                failed_operations: Vec::new(),
                resync_triggered_at: None,
                read_only_mode: AtomicBool::new(false),
            })),
        }
    }

    /// Initialize the state machine (must be called after construction).
    ///
    /// This initializes the SnapshotStore, creating the snapshot directory and scanning
    /// for existing snapshots.
    pub async fn initialize(&self) -> Result<(), crate::snapshot_store::Error> {
        let inner = self.inner.read().await;
        inner.snapshot_store.initialize().await
    }

    /// Get a handle to the inner state (for accessing subscriptions from StorageRaftMember).
    pub(crate) fn inner_handle(&self) -> Arc<RwLock<StateMachineInner>> {
        Arc::clone(&self.inner)
    }

    /// Get the last applied log index.
    pub async fn last_applied_index(&self) -> u64 {
        let inner = self.inner.read().await;
        inner.last_applied_index
    }

    /// Subscribe to metadata change events.
    ///
    /// Returns a receiver channel for metadata change notifications.
    /// Events are sent when metadata operations are committed through Raft.
    ///
    /// # Arguments
    ///
    /// * `filter` - Optional list of event types to receive. If None, all events are sent.
    /// * `capacity` - Channel buffer capacity (default: 100)
    ///
    /// # Returns
    ///
    /// A receiver that will receive MetadataChangeEvent notifications
    pub async fn subscribe_metadata_changes(
        &self,
        filter: Option<Vec<MetadataChangeType>>,
        capacity: Option<usize>,
    ) -> tokio::sync::broadcast::Receiver<MetadataChangeEvent> {
        let mut inner = self.inner.write().await;

        // Create a new broadcast channel for this subscription
        let capacity = capacity.unwrap_or(100);
        let (sender, receiver) = tokio::sync::broadcast::channel(capacity);

        debug!(
            "Added metadata change subscription (filter: {:?}, capacity: {})",
            filter.as_ref().map(|f| f.len()),
            capacity
        );

        // Add to subscriptions list
        inner.subscriptions.push(Subscription { sender, filter });

        receiver
    }

    /// Get the current state machine status.
    pub async fn get_status(&self) -> super::types::StateMachineStatus {
        let inner = self.inner.read().await;

        if inner.needs_resync.load(Ordering::SeqCst) {
            super::types::StateMachineStatus::NeedsResync {
                reason: inner
                    .resync_reason
                    .clone()
                    .unwrap_or_else(|| "Unknown".to_string()),
                failed_operations: inner.failed_operations.clone(),
                triggered_at: inner.resync_triggered_at.unwrap_or_else(SystemTime::now),
            }
        } else if inner.read_only_mode.load(Ordering::SeqCst) {
            super::types::StateMachineStatus::Resyncing {
                progress: 0.0, // TODO: Track actual progress
                started_at: inner.resync_triggered_at.unwrap_or_else(SystemTime::now),
            }
        } else {
            super::types::StateMachineStatus::Normal
        }
    }

    /// Trigger state machine resynchronization due to apply failure.
    ///
    /// This marks the state machine as needing resync and enters read-only mode.
    async fn trigger_resync(&self, reason: String, failed_ops: Vec<String>) {
        let mut inner = self.inner.write().await;

        error!(
            failed_ops = %failed_ops.len(),
            reason = %reason,
            last_applied = inner.last_applied_index,
            "STATE MACHINE APPLY FAILURE - Triggering automatic resync"
        );

        // Set resync flags
        inner.needs_resync.store(true, Ordering::SeqCst);
        inner.read_only_mode.store(true, Ordering::SeqCst);
        inner.resync_reason = Some(reason.clone());
        inner.failed_operations = failed_ops.clone();
        inner.resync_triggered_at = Some(SystemTime::now());

        // Write corruption marker file
        let marker_path = inner.snapshot_directory.join("NEEDS_RESYNC");
        let marker_content = format!(
            "Reason: {}\nFailed Operations: {}\nTriggered: {:?}\nLast Applied Index: {}\n",
            reason,
            failed_ops.join(", "),
            SystemTime::now(),
            inner.last_applied_index
        );

        if let Err(e) = std::fs::write(&marker_path, marker_content) {
            error!("Failed to write resync marker file: {}", e);
        } else {
            info!("Wrote resync marker to {:?}", marker_path);
        }

        warn!(
            "Node entered read-only mode and needs state resync. \
             Operator should restart node to trigger snapshot-based recovery."
        );
    }

    /// Clear resync state after successful snapshot installation.
    ///
    /// This should be called after a snapshot is successfully installed to
    /// restore normal operation.
    async fn clear_resync_state(&self) {
        let mut inner = self.inner.write().await;

        if inner.needs_resync.load(Ordering::SeqCst) {
            info!("Clearing resync state - node recovered");

            inner.needs_resync.store(false, Ordering::SeqCst);
            inner.read_only_mode.store(false, Ordering::SeqCst);
            inner.resync_reason = None;
            inner.failed_operations.clear();
            inner.resync_triggered_at = None;

            // Remove the marker file
            let marker_path = inner.snapshot_directory.join("NEEDS_RESYNC");
            if marker_path.exists() {
                if let Err(e) = std::fs::remove_file(&marker_path) {
                    warn!("Failed to remove resync marker file: {}", e);
                } else {
                    info!("Removed resync marker file");
                }
            }
        }
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
                    // We track which operations succeed to only emit events for those.
                    let mut successful_operations = Vec::new();
                    let mut failed_operations = Vec::new();

                    for operation in &operations {
                        match Self::apply_metadata_operation(&inner.metadata_store, operation).await
                        {
                            Ok(()) => {
                                successful_operations.push(operation.clone());
                            }
                            Err(e) => {
                                error!("Failed to apply operation {:?}: {}", operation, e);
                                failed_operations.push((operation.clone(), e));
                            }
                        }
                    }

                    // If any operations failed, trigger automatic resync to recover state.
                    // This prevents state divergence by entering read-only mode and requesting
                    // a snapshot from the leader.
                    if !failed_operations.is_empty() {
                        let reason = format!(
                            "Transaction {:?} commit failed: {} of {} operations failed",
                            tx_id,
                            failed_operations.len(),
                            successful_operations.len() + failed_operations.len()
                        );

                        let failed_op_strings: Vec<String> = failed_operations
                            .iter()
                            .map(|(op, err)| format!("{:?}: {}", op, err))
                            .collect();

                        // Drop the write lock before calling trigger_resync
                        drop(inner);
                        self.trigger_resync(reason.clone(), failed_op_strings).await;

                        // Return error to indicate apply failure
                        return Err(reason);
                    }

                    // Convert ONLY successful operations to metadata change events
                    let changes: Vec<super::types::MetadataChange> = successful_operations
                        .iter()
                        .filter_map(|op| Self::operation_to_change(op))
                        .collect();

                    // Emit change event if there are any changes
                    if !changes.is_empty() {
                        let event = MetadataChangeEvent {
                            committed_at: SystemTime::now(),
                            log_index,
                            changes,
                        };
                        Self::emit_metadata_change(&mut inner, log_index, event);
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

                // Abort the transaction by marking it as aborted.
                //
                // No metadata rollback is needed because the Prepare phase only stores
                // operations in memory - nothing is applied to MetadataStore until Commit.
                // We simply discard the in-memory transaction state.
                //
                // Note: Cleanup of staged chunks on storage nodes (if any) is handled
                // by the higher-level coordinator (FileSystemService or similar) which
                // tracks chunk staging and sends cleanup signals on abort. The state
                // machine is metadata-only and doesn't interact with chunk storage.

                inner.transactions.insert(
                    *tx_id,
                    TransactionPhase::Aborted {
                        reason: reason.clone(),
                    },
                );

                debug!("Transaction {:?} aborted", tx_id);
            }

            WormFsOperation::AtomicTransaction {
                tx_id,
                operations,
                timeout: _,
            } => {
                info!(
                    "Applying AtomicTransaction {:?} with {} operations at index {}",
                    tx_id,
                    operations.len(),
                    log_index
                );

                // Apply all operations atomically
                // Track which operations succeed to emit events only for those.
                let mut successful_operations = Vec::new();
                let mut failed_operations = Vec::new();

                for operation in operations {
                    match Self::apply_metadata_operation(&inner.metadata_store, operation).await {
                        Ok(()) => {
                            successful_operations.push(operation.clone());
                        }
                        Err(e) => {
                            error!("Failed to apply operation {:?}: {}", operation, e);
                            failed_operations.push((operation.clone(), e));
                        }
                    }
                }

                // If any operations failed, trigger automatic resync to recover state.
                // This prevents state divergence by entering read-only mode and requesting
                // a snapshot from the leader.
                if !failed_operations.is_empty() {
                    let reason = format!(
                        "AtomicTransaction {:?} failed: {} of {} operations failed",
                        tx_id,
                        failed_operations.len(),
                        successful_operations.len() + failed_operations.len()
                    );

                    let failed_op_strings: Vec<String> = failed_operations
                        .iter()
                        .map(|(op, err)| format!("{:?}: {}", op, err))
                        .collect();

                    // Drop the write lock before calling trigger_resync
                    drop(inner);
                    self.trigger_resync(reason.clone(), failed_op_strings).await;

                    // Return error to indicate apply failure
                    return Err(reason);
                }

                // Convert successful operations to metadata change events
                let changes: Vec<super::types::MetadataChange> = successful_operations
                    .iter()
                    .filter_map(|op| Self::operation_to_change(op))
                    .collect();

                // Emit change event if there are any changes
                if !changes.is_empty() {
                    let event = MetadataChangeEvent {
                        committed_at: SystemTime::now(),
                        log_index,
                        changes,
                    };
                    Self::emit_metadata_change(&mut inner, log_index, event);
                }

                // Store transaction as committed for tracking
                inner
                    .transactions
                    .insert(*tx_id, TransactionPhase::Committed);

                info!(
                    "AtomicTransaction {:?} committed successfully with {} operations",
                    tx_id,
                    successful_operations.len()
                );
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
                file_id,
                path,
                inode,
                metadata,
                policy: _,
            } => {
                // Use the file_id from the operation (pre-generated before Raft proposal)
                // Convert our FileMetadata to metadata_store::FileMetadata
                let store_metadata: crate::metadata_store::FileMetadata = metadata.clone().into();
                metadata_store
                    .create_file(*file_id, path, *inode, store_metadata)
                    .await
                    .map_err(|e| format!("Failed to create file: {:?}", e))?;
                info!("Created file at {:?} with inode {}", path, inode);
            }

            MetadataOperation::FileUpdate {
                file_id,
                inode,
                metadata,
                policy: _,
            } => {
                // Convert our FileMetadata to metadata_store::FileMetadata
                let store_metadata: crate::metadata_store::FileMetadata = metadata.clone().into();
                metadata_store
                    .update_file(*file_id, store_metadata)
                    .await
                    .map_err(|e| format!("Failed to update file: {:?}", e))?;
                info!("Updated file {:?} (inode {})", file_id, inode);
            }

            MetadataOperation::FileDelete { file_id, inode } => {
                metadata_store
                    .delete_file(*file_id)
                    .await
                    .map_err(|e| format!("Failed to delete file: {:?}", e))?;
                info!("Deleted file {:?} (inode {})", file_id, inode);
            }

            MetadataOperation::CreateStripe {
                file_id,
                stripe_id,
                stripe_index,
                policy: _,
                offset,
                size,
                chunks,
            } => {
                // Create a stripe record
                let stripe = crate::metadata_store::StripeRecord {
                    stripe_id: *stripe_id,
                    file_id: *file_id,
                    stripe_index: *stripe_index,
                    offset: *offset,
                    size: *size,
                    // Checksum is initialized to 0 because data hasn't been written yet.
                    // The workflow is: CreateStripe -> write chunks -> UpdateStripe with checksum.
                    // The filesystem_service layer will issue an UpdateStripe command once
                    // chunks are written and the checksum can be calculated.
                    checksum: 0,
                    created_at: std::time::SystemTime::now(),
                };
                metadata_store
                    .allocate_stripes(*file_id, vec![stripe])
                    .await
                    .map_err(|e| format!("Failed to create stripe: {:?}", e))?;

                // Create chunk records using placement information
                if !chunks.is_empty() {
                    let chunk_records: Vec<crate::metadata_store::ChunkRecord> = chunks
                        .iter()
                        .map(|cp| crate::metadata_store::ChunkRecord {
                            chunk_id: cp.chunk_id,
                            stripe_id: *stripe_id,
                            chunk_index: cp.chunk_index as u8,
                            // Convert Raft NodeId to FileStore NodeId
                            node_id: crate::file_store::types::NodeId(cp.node_id.as_u64()),
                            disk_id: cp.disk_id,
                            checksum: 0,
                            status: crate::metadata_store::ChunkStatus::Healthy,
                            created_at: std::time::SystemTime::now(),
                            last_verified: None,
                        })
                        .collect();

                    metadata_store
                        .allocate_chunks(*stripe_id, chunk_records)
                        .await
                        .map_err(|e| format!("Failed to allocate chunks: {:?}", e))?;

                    info!(
                        "Created stripe {:?} for file {:?} with {} chunks",
                        stripe_id,
                        file_id,
                        chunks.len()
                    );
                } else {
                    info!("Created stripe {:?} for file {:?}", stripe_id, file_id);
                }
            }

            MetadataOperation::DeleteStripe { stripe_id, file_id } => {
                metadata_store
                    .delete_stripe(*stripe_id)
                    .await
                    .map_err(|e| format!("Failed to delete stripe: {:?}", e))?;
                info!("Deleted stripe {:?} from file {:?}", stripe_id, file_id);
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

            MetadataOperation::AcquireReadLock {
                file_id,
                client_id,
                expires_at,
            } => {
                use crate::metadata_store::types::ClientId;

                metadata_store
                    .acquire_read_lock(*file_id, ClientId::new(*client_id), *expires_at)
                    .await
                    .map_err(|e| e.to_string())?;

                debug!(
                    "Read lock acquired for file {:?} by client {}",
                    file_id, client_id
                );
            }

            MetadataOperation::AcquireWriteLock {
                file_id,
                client_id,
                node_id,
                expires_at,
            } => {
                use crate::metadata_store::types::ClientId;

                metadata_store
                    .acquire_write_lock(*file_id, ClientId::new(*client_id), *node_id, *expires_at)
                    .await
                    .map_err(|e| e.to_string())?;

                debug!(
                    "Write lock acquired for file {:?} by client {} on node {}",
                    file_id, client_id, node_id
                );
            }

            MetadataOperation::ReleaseLock { file_id, client_id } => {
                use crate::metadata_store::types::ClientId;

                metadata_store
                    .release_lock(*file_id, ClientId::new(*client_id))
                    .await
                    .map_err(|e| e.to_string())?;

                debug!(
                    "Lock released for file {:?} by client {}",
                    file_id, client_id
                );
            }

            MetadataOperation::ExtendLock {
                file_id,
                client_id,
                new_expiry,
            } => {
                use crate::metadata_store::types::ClientId;

                metadata_store
                    .extend_lock(*file_id, ClientId::new(*client_id), *new_expiry)
                    .await
                    .map_err(|e| e.to_string())?;

                debug!(
                    "Lock extended for file {:?} by client {}",
                    file_id, client_id
                );
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
        membership: &openraft::StoredMembership<NodeId, super::raft_config::WormFsNode>,
    ) -> Result<WormFsSnapshotData, String> {
        info!(
            "Creating snapshot at index {} term {}",
            last_included_index, last_included_term
        );

        let (snapshot_store, snapshot_id, snapshot_compression, snapshot_lock) = {
            let mut inner = self.inner.write().await;
            let snapshot_id = inner.next_snapshot_id;
            inner.next_snapshot_id += 1;
            (
                Arc::clone(&inner.snapshot_store),
                snapshot_id,
                inner.snapshot_compression,
                Arc::clone(&inner.snapshot_creation_lock),
            )
        };

        // Acquire lock to prevent concurrent snapshot creation
        // This is critical because MetadataStore.create_snapshot() uses SQLite backup API
        // which can fail with IOERR_DATA if multiple backups run concurrently
        let _guard = snapshot_lock.lock().await;
        info!(
            "Acquired snapshot creation lock for snapshot {}",
            snapshot_id
        );

        // Create temporary snapshot file using MetadataStore
        // Use UUID to ensure uniqueness across concurrent snapshot creation on multiple nodes
        let unique_id = uuid::Uuid::new_v4();
        let temp_dir = std::env::temp_dir();
        let temp_snapshot_file = temp_dir.join(format!(
            "wormfs_snapshot_{}_{}_{}_{}_temp.db",
            unique_id, snapshot_id, last_included_index, last_included_term
        ));

        info!(
            "Creating temporary MetadataStore snapshot at {:?}",
            temp_snapshot_file
        );
        {
            let inner = self.inner.read().await;
            inner
                .metadata_store
                .create_snapshot(&temp_snapshot_file)
                .await
                .map_err(|e| format!("Failed to create snapshot: {:?}", e))?;
        }

        // Serialize membership configuration to JSON
        let membership_config = serde_json::to_string(membership.membership())
            .map_err(|e| format!("Failed to serialize membership: {:?}", e))?;

        // Extract membership log_id and leader node ID
        let (membership_log_index, membership_log_term, membership_leader_node_id) =
            if let Some(log_id) = membership.log_id() {
                (
                    Some(log_id.index),
                    Some(log_id.leader_id.term),
                    Some(log_id.leader_id.node_id.0),
                )
            } else {
                (None, None, None)
            };

        // Get snapshot leader node ID from the state machine
        let snapshot_leader_node_id = {
            let inner = self.inner.read().await;
            inner.last_applied_leader_id.0
        };

        // Ingest snapshot into SnapshotStore (handles compression and persistence)
        let snapshot_info = snapshot_store
            .ingest_snapshot(
                snapshot_id,
                last_included_index,
                last_included_term,
                snapshot_leader_node_id,
                &temp_snapshot_file,
                membership_log_index,
                membership_log_term,
                membership_leader_node_id,
                membership_config,
            )
            .await
            .map_err(|e| format!("Failed to ingest snapshot into SnapshotStore: {:?}", e))?;

        // Clean up temporary file
        if let Err(e) = tokio::fs::remove_file(&temp_snapshot_file).await {
            warn!("Failed to remove temporary snapshot file: {}", e);
        }

        // Get current membership
        let membership: BTreeSet<NodeId> = {
            let inner = self.inner.read().await;
            inner.last_membership.membership().voter_ids().collect()
        };

        info!(
            "Snapshot {} created successfully: {} bytes, compressed: {:?}, members: {}",
            snapshot_id,
            snapshot_info.metadata_db_size,
            matches!(snapshot_compression, CompressionAlgorithm::Zstd { .. }),
            membership.len()
        );

        // Determine the actual snapshot file name within the snapshot directory
        let snapshot_db_filename = match snapshot_compression {
            CompressionAlgorithm::None => "metadata.db",
            CompressionAlgorithm::Zstd { .. } => "metadata.db.zst",
        };

        // Calculate CRC32 checksum of the stored file
        let checksum =
            Self::calculate_checksum(&snapshot_info.storage_path.join(snapshot_db_filename))
                .await?;

        // Build relative path from snapshot_directory to the actual snapshot file
        // Format: snapshot_{id}/metadata.db or snapshot_{id}/metadata.db.zst
        let snapshot_relative_path =
            format!("snapshot_{:06}/{}", snapshot_id, snapshot_db_filename);

        let snapshot = WormFsSnapshotData::new(
            last_included_index,
            last_included_term,
            membership,
            snapshot_relative_path,
            snapshot_info.metadata_db_size,
            checksum,
            matches!(snapshot_compression, CompressionAlgorithm::Zstd { .. }),
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

    /// Convert a metadata operation into a metadata change event.
    ///
    /// Returns None if the operation doesn't produce a change event.
    fn operation_to_change(operation: &MetadataOperation) -> Option<super::types::MetadataChange> {
        use super::types::{ChunkLocation, FileAttributeChanges, MetadataChange};

        match operation {
            MetadataOperation::FileCreate {
                file_id,
                path,
                inode,
                ..
            } => {
                // Now that file_id is pre-generated before Raft proposal, we can emit FileCreated events
                Some(MetadataChange::FileCreated {
                    file_id: *file_id,
                    inode: *inode,
                    path: path.clone(),
                })
            }
            MetadataOperation::FileUpdate {
                file_id,
                inode,
                metadata,
                ..
            } => {
                // We report all metadata fields as changed since we don't track deltas.
                // A more sophisticated implementation could compare before/after state.
                Some(MetadataChange::FileUpdated {
                    file_id: *file_id,
                    inode: *inode,
                    changed_attrs: FileAttributeChanges {
                        size: Some(metadata.size),
                        mtime: Some(metadata.modified),
                        atime: None,
                        mode: Some(metadata.mode),
                        uid: None,
                        gid: None,
                    },
                })
            }
            MetadataOperation::FileDelete { file_id, inode } => Some(MetadataChange::FileDeleted {
                file_id: *file_id,
                inode: *inode,
            }),
            MetadataOperation::CreateStripe {
                file_id,
                stripe_id,
                offset,
                size,
                ..
            } => Some(MetadataChange::StripeCreated {
                file_id: *file_id,
                stripe_id: *stripe_id,
                offset: *offset,
                size: *size,
            }),
            MetadataOperation::DeleteStripe { stripe_id, file_id } => {
                Some(MetadataChange::StripeDeleted {
                    file_id: *file_id,
                    stripe_id: *stripe_id,
                })
            }
            MetadataOperation::MoveChunk {
                chunk_id,
                old_node,
                new_node,
                old_disk,
                new_disk,
            } => Some(MetadataChange::ChunkMoved {
                chunk_id: *chunk_id,
                old_location: ChunkLocation {
                    node_id: *old_node,
                    disk_id: *old_disk,
                },
                new_location: ChunkLocation {
                    node_id: *new_node,
                    disk_id: *new_disk,
                },
            }),
            MetadataOperation::ReleaseLock { file_id: _, .. } => {
                // Note: We don't have the inode in ReleaseLock operation,
                // so we can't emit a full LockReleased event.
                // In practice, lock release events may not be critical for most subscribers.
                // If needed, we could query the metadata store for the inode before releasing.
                None
            }
            // CreateChunk, DeleteChunk, and lock acquire/extend operations don't produce change events
            _ => None,
        }
    }

    /// Emit a metadata change event to all subscribers.
    ///
    /// Events are filtered based on each subscription's filter list.
    /// Slow or disconnected subscribers may miss events (at-most-once delivery).
    ///
    /// # Arguments
    ///
    /// * `inner` - Mutable reference to state machine inner (must hold write lock)
    /// * `log_index` - The log index where this change was committed
    /// * `event` - The metadata change event to emit
    fn emit_metadata_change(
        inner: &mut StateMachineInner,
        log_index: u64,
        event: MetadataChangeEvent,
    ) {
        // Remove subscriptions with no receivers
        inner
            .subscriptions
            .retain(|sub| sub.sender.receiver_count() > 0);

        let mut sent_count = 0;
        let mut filtered_count = 0;

        for subscription in &inner.subscriptions {
            // Check if this event matches the subscription filter
            let should_send = if let Some(filter) = &subscription.filter {
                event.changes.iter().any(|change| {
                    let change_type = match change {
                        super::types::MetadataChange::FileCreated { .. } => {
                            MetadataChangeType::FileCreated
                        }
                        super::types::MetadataChange::FileUpdated { .. } => {
                            MetadataChangeType::FileUpdated
                        }
                        super::types::MetadataChange::FileDeleted { .. } => {
                            MetadataChangeType::FileDeleted
                        }
                        super::types::MetadataChange::DirectoryCreated { .. } => {
                            MetadataChangeType::DirectoryCreated
                        }
                        super::types::MetadataChange::DirectoryDeleted { .. } => {
                            MetadataChangeType::DirectoryDeleted
                        }
                        super::types::MetadataChange::StripeCreated { .. } => {
                            MetadataChangeType::StripeCreated
                        }
                        super::types::MetadataChange::StripeDeleted { .. } => {
                            MetadataChangeType::StripeDeleted
                        }
                        super::types::MetadataChange::ChunkMoved { .. } => {
                            MetadataChangeType::ChunkMoved
                        }
                        super::types::MetadataChange::LockReleased { .. } => {
                            MetadataChangeType::LockReleased
                        }
                    };
                    filter.contains(&change_type)
                })
            } else {
                true // No filter means send all events
            };

            if should_send {
                // Try to send, but don't block if channel is full
                match subscription.sender.send(event.clone()) {
                    Ok(_) => sent_count += 1,
                    Err(_) => {
                        // Channel is full or has no receivers - subscriber is too slow
                        warn!(
                            "Failed to send metadata change event at index {} - subscriber channel full or closed",
                            log_index
                        );
                    }
                }
            } else {
                filtered_count += 1;
            }
        }

        if sent_count > 0 {
            debug!(
                "Emitted metadata change event at index {} to {} subscribers ({} filtered)",
                log_index, sent_count, filtered_count
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
                openraft::CommittedLeaderId::new(
                    inner.last_applied_term,
                    inner.last_applied_leader_id,
                ),
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

        debug!("[StateMachine] apply() called");

        for entry in entries {
            let log_index = entry.log_id.index;
            let log_term = entry.log_id.leader_id.term;
            let leader_id = entry.log_id.leader_id.node_id;

            trace!(
                "[StateMachine] Processing entry at index {}, term {}, leader {}",
                log_index,
                log_term,
                leader_id.0
            );

            // Extract the operation from the entry payload
            let operation = match entry.payload {
                openraft::EntryPayload::Normal(op) => op,
                openraft::EntryPayload::Membership(membership) => {
                    // Update membership and return empty response (OpenRaft requires one response per entry)
                    let mut inner = self.inner.write().await;
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    inner.last_applied_index = log_index;
                    inner.last_applied_term = log_term;
                    inner.last_applied_leader_id = leader_id;
                    // Add empty response for membership changes
                    responses.push(WormFsResponse::Empty);
                    continue;
                }
                openraft::EntryPayload::Blank => {
                    // Blank entries still need a response (OpenRaft requires one response per entry)
                    let mut inner = self.inner.write().await;
                    inner.last_applied_index = log_index;
                    inner.last_applied_term = log_term;
                    inner.last_applied_leader_id = leader_id;
                    // Add empty response for blank entries
                    responses.push(WormFsResponse::Empty);
                    continue;
                }
            };

            // Apply the operation and generate appropriate response
            let apply_result = self.apply_operation(log_index, &operation).await;

            // Record proposal in history (for AdminUI tracking on all nodes)
            {
                let inner = self.inner.read().await;

                // Serialize operation to JSON for details view
                let operation_details = serde_json::to_string(&operation).unwrap_or_default();

                // Extract operation metadata
                let (operation_type, tx_id_str, operation_count) = match &operation {
                    WormFsOperation::AtomicTransaction {
                        tx_id, operations, ..
                    } => (
                        "AtomicTransaction",
                        Some(tx_id.to_hex_short()),
                        operations.len(),
                    ),
                    WormFsOperation::TransactionPrepare {
                        tx_id,
                        metadata_ops,
                        command_ops,
                        ..
                    } => {
                        let count = metadata_ops.as_ref().map(|v| v.len()).unwrap_or(0)
                            + command_ops.as_ref().map(|v| v.len()).unwrap_or(0);
                        ("TransactionPrepare", Some(tx_id.to_hex_short()), count)
                    }
                    WormFsOperation::TransactionCommit { tx_id } => {
                        ("TransactionCommit", Some(tx_id.to_hex_short()), 1)
                    }
                    WormFsOperation::TransactionAbort { tx_id, .. } => {
                        ("TransactionAbort", Some(tx_id.to_hex_short()), 1)
                    }
                };

                // Record the proposal (fire-and-forget, don't block on errors)
                let success = apply_result.is_ok();
                let error_message = apply_result.as_ref().err().map(|e| e.as_str());

                let _ = inner
                    .metadata_store
                    .record_applied_proposal(
                        log_index,
                        log_term,
                        leader_id.0,
                        operation_type,
                        tx_id_str.as_deref(),
                        operation_count,
                        success,
                        error_message,
                        &operation_details,
                    )
                    .await;

                // Cleanup old proposals periodically (every 100th entry)
                if log_index % 100 == 0 {
                    let _ = inner.metadata_store.cleanup_old_proposals(50).await;
                }
            }

            // Generate response based on apply result
            match apply_result {
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
                        WormFsOperation::AtomicTransaction { tx_id, .. } => {
                            WormFsResponse::TransactionCommitted { tx_id: *tx_id }
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
                        | WormFsOperation::TransactionAbort { tx_id, .. }
                        | WormFsOperation::AtomicTransaction { tx_id, .. } => {
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

        debug!(
            "[StateMachine] apply() returning {} responses",
            responses.len()
        );
        Ok(responses)
    }

    /// Get the snapshot builder for creating snapshots.
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    /// Begin receiving a snapshot from the leader.
    ///
    /// Creates a temporary file in the snapshot directory for receiving snapshot data.
    /// The file will be moved to its final location after successful installation.
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<WormFsTypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<NodeId>> {
        let (snapshot_dir, temp_path) = {
            let inner = self.inner.read().await;

            // Create a unique temporary filename using timestamp and random suffix
            let temp_filename = format!(
                "snapshot-incoming-{}-{}.db.tmp",
                current_time_secs(),
                rand::random::<u32>()
            );
            let temp_path = inner.snapshot_directory.join(&temp_filename);

            (inner.snapshot_directory.clone(), temp_path)
        };

        // Create snapshot directory if it doesn't exist
        tokio::fs::create_dir_all(&snapshot_dir)
            .await
            .map_err(|e| {
                error!("Failed to create snapshot directory: {:?}", e);
                let io_error = openraft::StorageIOError::new(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&e),
                );
                StorageError::IO { source: io_error }
            })?;

        info!("Creating temporary snapshot file: {}", temp_path.display());

        // Create the temporary file for receiving snapshot data
        let temp_file = tokio::fs::File::create(&temp_path).await.map_err(|e| {
            error!("Failed to create temporary snapshot file: {:?}", e);
            let io_error = openraft::StorageIOError::new(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                openraft::AnyError::new(&e),
            );
            StorageError::IO { source: io_error }
        })?;

        // Store the temp path for use in install_snapshot()
        {
            let mut inner = self.inner.write().await;
            inner.incoming_snapshot_path = Some(temp_path.clone());
        }

        // Return a buffered reader wrapping the file
        // OpenRaft will write snapshot data to this handle
        Ok(Box::new(tokio::io::BufReader::new(temp_file)))
    }

    /// Install a snapshot, replacing the current state.
    ///
    /// This is called by OpenRaft after receiving a snapshot from the leader.
    /// The snapshot data has been written to the temp file created by begin_receiving_snapshot().
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, super::raft_config::WormFsNode>,
        snapshot: Box<<WormFsTypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<NodeId>> {
        let last_log_id = meta.last_log_id.as_ref();
        let (last_included_index, last_included_term, last_included_leader_id) =
            if let Some(log_id) = last_log_id {
                (
                    log_id.index,
                    log_id.leader_id.term,
                    log_id.leader_id.node_id,
                )
            } else {
                (0, 0, NodeId(0))
            };

        info!(
            "Installing snapshot at index {} term {}",
            last_included_index, last_included_term
        );

        // Drop the snapshot handle to ensure data is flushed
        drop(snapshot);

        // Get the temp file path from state
        let temp_path = {
            let mut inner = self.inner.write().await;
            inner.incoming_snapshot_path.take().ok_or_else(|| {
                error!("No incoming snapshot path found");
                let io_error = openraft::StorageIOError::new(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::error("No incoming snapshot path"),
                );
                StorageError::IO { source: io_error }
            })?
        };

        // Verify the temp file exists
        if !tokio::fs::try_exists(&temp_path).await.map_err(|e| {
            error!("Failed to check temp snapshot existence: {:?}", e);
            let io_error = openraft::StorageIOError::new(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Read,
                openraft::AnyError::new(&e),
            );
            StorageError::IO { source: io_error }
        })? {
            error!("Temp snapshot file not found: {}", temp_path.display());
            let io_error = openraft::StorageIOError::new(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Read,
                openraft::AnyError::error("Temp snapshot file not found"),
            );
            return Err(StorageError::IO { source: io_error });
        }

        // Calculate the final snapshot filename
        let final_filename = format!("snapshot-{}-{}.db", last_included_index, last_included_term);
        let final_path = {
            let inner = self.inner.read().await;
            inner.snapshot_directory.join(&final_filename)
        };

        // Check if the snapshot is compressed by examining the snapshot_id in metadata
        // The snapshot_id contains the filename like "snapshot_000001/metadata.db.zst"
        let is_compressed = meta.snapshot_id.ends_with(".zst");

        // If compressed, decompress the temp file before moving it
        let decompressed_path = if is_compressed {
            info!("Snapshot is compressed, decompressing...");

            // Create path for decompressed file
            let decompressed_path = temp_path.with_extension("db");

            // Decompress using streaming API (sync work in task pool)
            let compressed_data = tokio::fs::read(&temp_path).await.map_err(|e| {
                error!("Failed to read compressed snapshot: {:?}", e);
                let io_error = openraft::StorageIOError::new(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::new(&e),
                );
                StorageError::IO { source: io_error }
            })?;

            let decompressed_data = tokio::task::spawn_blocking(move || {
                let cursor = std::io::Cursor::new(compressed_data);
                let mut decoder = zstd::stream::read::Decoder::new(cursor)
                    .map_err(|e| format!("Zstd decoder init failed: {}", e))?;

                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| format!("Zstd decompression failed: {}", e))?;

                Ok::<Vec<u8>, String>(decompressed)
            })
            .await
            .map_err(|e| {
                error!("Failed to decompress snapshot: {:?}", e);
                let io_error = openraft::StorageIOError::new(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::error(format!("Decompression task failed: {}", e)),
                );
                StorageError::IO { source: io_error }
            })?
            .map_err(|e| {
                error!("Failed to decompress snapshot: {}", e);
                let io_error = openraft::StorageIOError::new(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::error(e),
                );
                StorageError::IO { source: io_error }
            })?;

            let decompressed_size = decompressed_data.len();

            tokio::fs::write(&decompressed_path, decompressed_data)
                .await
                .map_err(|e| {
                    error!("Failed to write decompressed snapshot: {:?}", e);
                    let io_error = openraft::StorageIOError::new(
                        openraft::ErrorSubject::Snapshot(None),
                        openraft::ErrorVerb::Write,
                        openraft::AnyError::new(&e),
                    );
                    StorageError::IO { source: io_error }
                })?;

            // Remove the compressed temp file
            let _ = tokio::fs::remove_file(&temp_path).await;

            info!("Decompression complete: {} bytes", decompressed_size);
            decompressed_path
        } else {
            temp_path
        };

        // Move decompressed file to final location
        info!(
            "Moving snapshot from {} to {}",
            decompressed_path.display(),
            final_path.display()
        );
        tokio::fs::rename(&decompressed_path, &final_path)
            .await
            .map_err(|e| {
                error!("Failed to move snapshot to final location: {:?}", e);
                let io_error = openraft::StorageIOError::new(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Write,
                    openraft::AnyError::new(&e),
                );
                StorageError::IO { source: io_error }
            })?;

        // Restore from snapshot using MetadataStore
        info!("Restoring MetadataStore from snapshot...");
        {
            let inner = self.inner.read().await;
            inner
                .metadata_store
                .restore_from_snapshot(&final_path)
                .await
                .map_err(|e| {
                    error!("Failed to restore from snapshot: {:?}", e);
                    let io_error = openraft::StorageIOError::new(
                        openraft::ErrorSubject::Store,
                        openraft::ErrorVerb::Write,
                        openraft::AnyError::error(format!("MetadataStore restore failed: {:?}", e)),
                    );
                    StorageError::IO { source: io_error }
                })?;
        }

        // Update state machine state
        let mut inner = self.inner.write().await;
        inner.last_applied_index = last_included_index;
        inner.last_applied_term = last_included_term;
        inner.last_applied_leader_id = last_included_leader_id;
        inner.last_membership = meta.last_membership.clone();

        // Clear all transaction state - obsolete after snapshot
        let old_tx_count = inner.transactions.len();
        inner.transactions.clear();

        info!(
            "Snapshot installed successfully: cleared {} transactions, last_applied_index={}",
            old_tx_count, last_included_index
        );

        // Release the lock before calling clear_resync_state
        drop(inner);

        // Clear resync state if node was in resync mode
        self.clear_resync_state().await;

        Ok(())
    }

    /// Get the current snapshot.
    ///
    /// Returns the most recent snapshot from the SnapshotStore if one exists.
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<WormFsTypeConfig>>, StorageError<NodeId>> {
        let (snapshot_store, snapshot_directory) = {
            let inner = self.inner.read().await;
            (
                Arc::clone(&inner.snapshot_store),
                inner.snapshot_directory.clone(),
            )
        };

        // Get the latest snapshot from SnapshotStore
        let snapshot_info = snapshot_store.get_latest_snapshot().await.map_err(|e| {
            error!("Failed to get latest snapshot: {}", e);
            let io_error = openraft::StorageIOError::new(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Read,
                openraft::AnyError::error(e),
            );
            StorageError::IO { source: io_error }
        })?;

        if let Some(info) = snapshot_info {
            info!(
                "get_current_snapshot() found snapshot: id={}, index={}, term={}, membership_log_id={:?}",
                info.snapshot_id, info.log_index, info.log_term, info.membership_log_index
            );

            // Deserialize the stored membership configuration
            let membership: openraft::Membership<NodeId, super::raft_config::WormFsNode> =
                serde_json::from_str(&info.membership_config).map_err(|e| {
                    error!("Failed to deserialize membership: {:?}", e);
                    let io_error = openraft::StorageIOError::new(
                        openraft::ErrorSubject::Snapshot(None),
                        openraft::ErrorVerb::Read,
                        openraft::AnyError::error(e),
                    );
                    StorageError::IO { source: io_error }
                })?;

            // Reconstruct the StoredMembership with the log_id from the snapshot
            let membership_log_id = match (
                info.membership_log_index,
                info.membership_log_term,
                info.membership_leader_node_id,
            ) {
                (Some(index), Some(term), Some(node_id)) => Some(openraft::LogId::new(
                    openraft::CommittedLeaderId::new(term, NodeId(node_id)),
                    index,
                )),
                _ => None,
            };

            let last_membership = openraft::StoredMembership::new(membership_log_id, membership);

            // Construct the full path to the snapshot file
            let snapshot_path = snapshot_directory.join(format!(
                "snapshot_{:06}/{}",
                info.snapshot_id,
                if info.compression != crate::snapshot_store::CompressionAlgorithm::None {
                    "metadata.db.zst"
                } else {
                    "metadata.db"
                }
            ));

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

            // Create LogId from snapshot info
            let last_log_id = Some(openraft::LogId::new(
                openraft::CommittedLeaderId::new(
                    info.log_term,
                    NodeId(info.snapshot_leader_node_id),
                ),
                info.log_index,
            ));

            let snapshot_id = format!(
                "snapshot_{:06}/metadata.db{}",
                info.snapshot_id,
                if info.compression != crate::snapshot_store::CompressionAlgorithm::None {
                    ".zst"
                } else {
                    ""
                }
            );

            let meta = SnapshotMeta {
                last_log_id,
                last_membership,
                snapshot_id,
            };

            let snapshot_data = Box::new(tokio::io::BufReader::new(snapshot_file));

            Ok(Some(Snapshot {
                meta,
                snapshot: snapshot_data,
            }))
        } else {
            debug!("get_current_snapshot() - no snapshot available");
            Ok(None)
        }
    }
}

impl RaftSnapshotBuilder<WormFsTypeConfig> for WormFsStateMachine {
    /// Build a snapshot of the current state.
    async fn build_snapshot(&mut self) -> Result<Snapshot<WormFsTypeConfig>, StorageError<NodeId>> {
        // Get current state to determine snapshot parameters
        let (
            last_included_index,
            last_included_term,
            last_log_id,
            _snapshot_store,
            _snapshot_directory,
        ) = {
            let inner = self.inner.read().await;

            info!("Building snapshot at index {}", inner.last_applied_index);

            let last_log_id = if inner.last_applied_index > 0 {
                Some(openraft::LogId::new(
                    openraft::CommittedLeaderId::new(
                        inner.last_applied_term,
                        inner.last_applied_leader_id,
                    ),
                    inner.last_applied_index,
                ))
            } else {
                None
            };

            (
                inner.last_applied_index,
                inner.last_applied_term,
                last_log_id,
                Arc::clone(&inner.snapshot_store),
                inner.snapshot_directory.clone(),
            )
        };

        // Determine the correct membership to use for this snapshot
        // The membership must have a log_id <= the snapshot's last_log_id
        let last_membership = {
            let inner = self.inner.read().await;
            let current_membership = &inner.last_membership;

            info!(
                "Checking membership: membership_log_id={:?}, snapshot_log_id={:?}",
                current_membership.log_id(),
                last_log_id
            );

            // Check if the current membership's log_id is valid for this snapshot
            match (current_membership.log_id(), &last_log_id) {
                (Some(membership_log_id), Some(snapshot_log_id))
                    if membership_log_id.index > snapshot_log_id.index =>
                {
                    // Current membership is newer than the snapshot
                    // We need to use a membership with log_id <= snapshot_log_id
                    info!(
                        "Current membership log_id {} > snapshot log_id {}, using snapshot_log_id as membership log_id",
                        membership_log_id.index, snapshot_log_id.index
                    );

                    // Use the current membership configuration but with the snapshot's log_id
                    // This is a conservative approach - the membership at snapshot_log_id might have been
                    // different, but since we don't track historical membership, we use the current one
                    // with an adjusted log_id
                    StoredMembership::new(
                        Some(snapshot_log_id.clone()),
                        current_membership.membership().clone(),
                    )
                }
                _ => {
                    // Current membership is valid for this snapshot
                    current_membership.clone()
                }
            }
        };

        // Create the actual snapshot using our implementation
        info!(
            "Calling create_snapshot() for index={}, term={} with membership log_id={:?}",
            last_included_index,
            last_included_term,
            last_membership.log_id()
        );
        let snapshot_data = self
            .create_snapshot(last_included_index, last_included_term, &last_membership)
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

        info!(
            "create_snapshot() returned: snapshot_file={}, file_size={}, checksum={:08x}",
            snapshot_data.snapshot_file, snapshot_data.file_size, snapshot_data.checksum
        );

        // Construct full path to the snapshot file
        let snapshot_path = {
            let inner = self.inner.read().await;
            let path = inner.snapshot_directory.join(&snapshot_data.snapshot_file);
            info!(
                "Constructed snapshot path: {} (snapshot_directory={}, snapshot_file={})",
                path.display(),
                inner.snapshot_directory.display(),
                snapshot_data.snapshot_file
            );
            path
        };

        // Verify file exists before trying to open it
        if !tokio::fs::try_exists(&snapshot_path).await.unwrap_or(false) {
            error!(
                "Snapshot file DOES NOT EXIST at constructed path: {}",
                snapshot_path.display()
            );

            // List what files DO exist in the snapshot directory
            let snapshot_dir = {
                let inner = self.inner.read().await;
                inner.snapshot_directory.clone()
            };

            if let Ok(mut entries) = tokio::fs::read_dir(&snapshot_dir).await {
                error!("Files in snapshot directory {}:", snapshot_dir.display());
                while let Ok(Some(entry)) = entries.next_entry().await {
                    error!("  - {}", entry.path().display());
                }
            }

            let io_error = openraft::StorageIOError::new(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Read,
                openraft::AnyError::error("snapshot file not found after creation"),
            );
            return Err(StorageError::IO { source: io_error });
        }

        info!(
            "Snapshot file exists, opening for reading: {}",
            snapshot_path.display()
        );

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
            tx_id: TxId::new(1),
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
            tx_id: TxId::new(1),
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

        let tx_id = TxId::new(100);

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

    #[tokio::test]
    async fn test_metadata_subscriptions() {
        use crate::file_store::types::FileId;
        use crate::storage_raft_member::types::{FileMetadata, MetadataChange, StoragePolicy};
        use std::path::PathBuf;
        use uuid::Uuid;

        let (state_machine, _temp_dir) = create_test_state_machine().await;

        // Subscribe to metadata changes (no filter = all events)
        let mut receiver = state_machine
            .subscribe_metadata_changes(None, Some(10))
            .await;

        // Subscribe with filter for only file updates
        let mut filtered_receiver = state_machine
            .subscribe_metadata_changes(Some(vec![MetadataChangeType::FileUpdated]), Some(10))
            .await;

        // First create a file so subsequent operations can reference it
        let create_tx_id = TxId(399);
        let test_file_id = FileId::generate();
        let file_create_op = MetadataOperation::FileCreate {
            file_id: test_file_id,
            path: PathBuf::from("/test/file.txt"),
            inode: 12345,
            metadata: FileMetadata {
                size: 0,
                created: SystemTime::now(),
                modified: SystemTime::now(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                file_type: 0, // Regular file
                target: None,
            },
            policy: StoragePolicy {
                data_chunks: 2,
                parity_chunks: 1,
                replication_factor: 1,
            },
        };

        let create_prepare_op = WormFsOperation::TransactionPrepare {
            tx_id: create_tx_id,
            metadata_ops: Some(vec![file_create_op]),
            command_ops: None,
            timeout: SystemTime::now(),
        };

        state_machine
            .apply_operation(1, &create_prepare_op)
            .await
            .unwrap();
        let create_commit_op = WormFsOperation::TransactionCommit {
            tx_id: create_tx_id,
        };
        state_machine
            .apply_operation(2, &create_commit_op)
            .await
            .unwrap();

        // Create a transaction with stripe creation operation
        // Now we can use the deterministic file_id that all nodes will have
        use crate::file_store::types::StripeId;

        let tx_id = TxId(400);
        let stripe_create_op = MetadataOperation::CreateStripe {
            file_id: test_file_id,
            stripe_id: StripeId::new(Uuid::new_v4()),
            stripe_index: 0,
            policy: StoragePolicy {
                data_chunks: 2,
                parity_chunks: 1,
                replication_factor: 1,
            },
            offset: 0,
            size: 1024,
            chunks: vec![],
        };

        let prepare_op = WormFsOperation::TransactionPrepare {
            tx_id,
            metadata_ops: Some(vec![stripe_create_op]),
            command_ops: None,
            timeout: SystemTime::now(),
        };

        // First, we should receive the FileCreated event from log_index 2
        let file_created_event =
            tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
                .await
                .expect("Timeout waiting for FileCreated event")
                .expect("Channel closed");

        assert_eq!(file_created_event.log_index, 2);
        assert_eq!(file_created_event.changes.len(), 1);
        assert!(matches!(
            file_created_event.changes[0],
            MetadataChange::FileCreated { .. }
        ));

        // Prepare and commit the stripe creation
        state_machine.apply_operation(3, &prepare_op).await.unwrap();
        let commit_op = WormFsOperation::TransactionCommit { tx_id };
        state_machine.apply_operation(4, &commit_op).await.unwrap();

        // Check that the unfiltered subscriber received the StripeCreated event
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .expect("Timeout waiting for StripeCreated event")
            .expect("Channel closed");

        assert_eq!(event.log_index, 4);
        assert_eq!(event.changes.len(), 1);
        assert!(matches!(
            event.changes[0],
            MetadataChange::StripeCreated { .. }
        ));

        // The filtered subscriber should NOT receive it (it's filtered for FileUpdated only)
        let filtered_result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            filtered_receiver.recv(),
        )
        .await;
        assert!(
            filtered_result.is_err(),
            "Filtered subscriber should not receive StripeCreated"
        );
    }
}
