//! Implementation of the TransactionManager trait.

use super::types::{Config, Error, Operation, Result, TransactionBatch, TxId};
use super::TransactionManager;
use crate::metadata_store::{MetadataStore, MetadataStoreImpl};
use crate::metric_service::{MetricService, MetricServiceImpl, UnitType};
use crate::storage_raft_member::{types::WormFsOperation, StorageRaftMember};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// Implementation of the TransactionManager trait.
///
/// This implementation coordinates transaction batching and validation before
/// submitting atomic operations through Raft consensus.
pub struct TransactionManagerImpl {
    /// Active transaction batches (not yet committed or aborted)
    active_transactions: Arc<RwLock<HashMap<TxId, TransactionBatch>>>,

    /// Raft member for consensus operations
    raft_member: Arc<dyn StorageRaftMember<Operation = WormFsOperation, OperationResult = ()>>,

    /// Metadata store for validation queries
    metadata_store: MetadataStoreImpl,

    /// Configuration
    config: Config,

    /// Metrics service for observability
    metrics: MetricServiceImpl,

    /// Shutdown signal sender for cleanup task
    /// Using broadcast channel to break reference cycle and enable graceful shutdown
    shutdown_tx: Arc<tokio::sync::broadcast::Sender<()>>,
}

impl TransactionManagerImpl {
    /// Create a new TransactionManager implementation.
    ///
    /// This also starts a background cleanup task that periodically checks for
    /// expired transactions and aborts them.
    pub fn new(
        raft_member: Arc<dyn StorageRaftMember<Operation = WormFsOperation, OperationResult = ()>>,
        metadata_store: MetadataStoreImpl,
        config: Config,
        metrics: MetricServiceImpl,
    ) -> Arc<Self> {
        let active_transactions = Arc::new(RwLock::new(HashMap::new()));

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = tokio::sync::broadcast::channel(1);

        // Start cleanup task (captures only what it needs, not the manager)
        Self::start_cleanup_task(
            active_transactions.clone(),
            metrics.clone(),
            shutdown_rx,
            config.cleanup_interval(),
        );

        // Create the manager
        let manager = Arc::new(Self {
            active_transactions,
            raft_member,
            metadata_store,
            config,
            metrics,
            shutdown_tx: Arc::new(shutdown_tx),
        });

        info!("TransactionManager initialized");
        manager
    }

    /// Start the background cleanup task.
    ///
    /// This task captures only the active_transactions HashMap and metrics service,
    /// not the entire TransactionManagerImpl, to avoid reference cycles.
    /// The task listens for shutdown signals via the broadcast channel.
    fn start_cleanup_task(
        active_transactions: Arc<RwLock<HashMap<TxId, TransactionBatch>>>,
        metrics: MetricServiceImpl,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
        interval: Duration,
    ) {
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        // Cleanup expired transactions inline
                        let mut transactions = active_transactions.write().await;
                        let mut expired = Vec::new();

                        // Find expired transactions
                        for (tx_id, batch) in transactions.iter() {
                            if batch.is_expired() {
                                expired.push(*tx_id);
                            }
                        }

                        // Remove expired transactions
                        for tx_id in &expired {
                            transactions.remove(tx_id);
                            warn!("Transaction {:?} expired and was auto-aborted", tx_id);
                            let _ = metrics.publish_counter(
                                "transaction_manager.transactions_expired",
                                1,
                                UnitType::Operations,
                            );
                        }

                        if !expired.is_empty() {
                            debug!("Cleaned up {} expired transactions", expired.len());
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("TransactionManager cleanup task stopped (shutdown signal)");
                        break;
                    }
                }
            }
        });
    }

    /// Clean up expired transactions.
    async fn cleanup_expired_transactions(&self) -> Result<()> {
        let mut transactions = self.active_transactions.write().await;
        let mut expired = Vec::new();

        // Find expired transactions
        for (tx_id, batch) in transactions.iter() {
            if batch.is_expired() {
                expired.push(*tx_id);
            }
        }

        // Remove expired transactions
        for tx_id in &expired {
            transactions.remove(tx_id);
            warn!("Transaction {:?} expired and was auto-aborted", tx_id);
            let _ = self.metrics.publish_counter(
                "transaction_manager.transactions_expired",
                1,
                UnitType::Operations,
            );
        }

        if !expired.is_empty() {
            debug!("Cleaned up {} expired transactions", expired.len());
        }

        Ok(())
    }

    /// Validate a CreateFile operation.
    async fn validate_create_file(&self, path: &std::path::Path) -> Result<()> {
        // Check that parent directory exists
        if let Some(parent) = path.parent() {
            // Use ? operator to preserve the specific error type
            // The From impl will convert MetadataStore errors appropriately
            self.metadata_store.get_file_by_path(parent).await?;
        }

        // Check that file doesn't already exist
        if self.metadata_store.get_file_by_path(path).await.is_ok() {
            return Err(Error::FileAlreadyExists(path.to_path_buf()));
        }

        Ok(())
    }

    /// Validate an UpdateFile operation.
    async fn validate_update_file(&self, file_id: crate::file_store::types::FileId) -> Result<()> {
        // Check that file exists
        // Use ? operator - From impl converts FileNotFoundByFileId to FileNotFound
        self.metadata_store.get_file(file_id).await?;

        Ok(())
    }

    /// Validate a DeleteFile operation.
    async fn validate_delete_file(&self, file_id: crate::file_store::types::FileId) -> Result<()> {
        // Check that file exists
        self.metadata_store.get_file(file_id).await?;

        // Note: In a full implementation, you might also check:
        // - File has no children (if it's a directory)
        // - File is not locked
        // - etc.

        Ok(())
    }

    /// Validate a CreateStripe operation.
    async fn validate_create_stripe(
        &self,
        file_id: crate::file_store::types::FileId,
    ) -> Result<()> {
        // Check that file exists
        self.metadata_store.get_file(file_id).await?;

        Ok(())
    }

    /// Validate a DeleteStripe operation.
    async fn validate_delete_stripe(
        &self,
        stripe_id: crate::file_store::types::StripeId,
    ) -> Result<()> {
        // Check that stripe exists
        self.metadata_store.get_stripe(stripe_id).await?;

        Ok(())
    }

    /// Validate an AcquireReadLock operation.
    async fn validate_acquire_read_lock(
        &self,
        file_id: crate::file_store::types::FileId,
    ) -> Result<()> {
        // Check that file exists
        self.metadata_store.get_file(file_id).await?;

        // Note: Actual lock conflict checking is done by the metadata store
        // when the operation is applied. We just verify the file exists.
        Ok(())
    }

    /// Validate an AcquireWriteLock operation.
    async fn validate_acquire_write_lock(
        &self,
        file_id: crate::file_store::types::FileId,
    ) -> Result<()> {
        // Check that file exists
        self.metadata_store.get_file(file_id).await?;

        // Note: Actual lock conflict checking is done by the metadata store
        // when the operation is applied. We just verify the file exists.
        Ok(())
    }

    /// Validate a ReleaseLock operation.
    async fn validate_release_lock(
        &self,
        file_id: crate::file_store::types::FileId,
        client_id: u64,
    ) -> Result<()> {
        use crate::metadata_store::types::ClientId;

        // Check that file exists
        self.metadata_store.get_file(file_id).await?;

        // Check that the client actually has a lock on this file
        let locks = self.metadata_store.get_file_locks(file_id).await?;

        let has_lock = locks
            .iter()
            .any(|lock| lock.client_id == ClientId::new(client_id));

        if !has_lock {
            return Err(Error::LockNotFound(file_id, client_id));
        }

        Ok(())
    }

    /// Validate an ExtendLock operation.
    async fn validate_extend_lock(
        &self,
        file_id: crate::file_store::types::FileId,
        client_id: u64,
    ) -> Result<()> {
        use crate::metadata_store::types::ClientId;

        // Check that file exists
        self.metadata_store.get_file(file_id).await?;

        // Check that the client actually has a lock on this file
        let locks = self.metadata_store.get_file_locks(file_id).await?;

        let has_lock = locks
            .iter()
            .any(|lock| lock.client_id == ClientId::new(client_id));

        if !has_lock {
            return Err(Error::LockNotFound(file_id, client_id));
        }

        Ok(())
    }

    /// Validate a lock expiration time against configured timeout.
    ///
    /// Ensures that:
    /// - Lock expires at least 1 second in the future (not already expired)
    /// - Lock expires no more than lock_timeout_secs in the future (respects config)
    ///
    /// This enforcement prevents DoS attacks via long-lived locks and ensures
    /// the deadlock prevention mechanism works as designed.
    fn validate_lock_timeout(&self, expires_at: SystemTime) -> Result<()> {
        let now = SystemTime::now();
        let max_lock_duration = self.config.lock_timeout();
        let min_lock_duration = Duration::from_secs(1);

        // Calculate how far in the future the lock expires
        let duration_until_expiry = expires_at
            .duration_since(now)
            .unwrap_or(Duration::from_secs(0));

        // Check minimum: lock must expire at least 1 second in the future
        if duration_until_expiry < min_lock_duration {
            return Err(Error::InvalidLockExpiry(
                self.config.lock_timeout_secs,
                duration_until_expiry,
            ));
        }

        // Check maximum: lock must not exceed configured timeout
        if duration_until_expiry > max_lock_duration {
            return Err(Error::InvalidLockExpiry(
                self.config.lock_timeout_secs,
                duration_until_expiry,
            ));
        }

        Ok(())
    }

    /// Validate an operation before adding it to the transaction.
    async fn validate_operation(&self, operation: &Operation) -> Result<()> {
        match operation {
            Operation::CreateFile { path, .. } => {
                self.validate_create_file(path).await?;
            }
            Operation::UpdateFile { file_id, .. } => {
                self.validate_update_file(*file_id).await?;
            }
            Operation::DeleteFile { file_id, .. } => {
                self.validate_delete_file(*file_id).await?;
            }
            Operation::CreateStripe { file_id, .. } => {
                self.validate_create_stripe(*file_id).await?;
            }
            Operation::DeleteStripe { stripe_id, .. } => {
                self.validate_delete_stripe(*stripe_id).await?;
            }
            Operation::AcquireReadLock {
                file_id,
                expires_at,
                ..
            } => {
                self.validate_acquire_read_lock(*file_id).await?;
                self.validate_lock_timeout(*expires_at)?;
            }
            Operation::AcquireWriteLock {
                file_id,
                expires_at,
                ..
            } => {
                self.validate_acquire_write_lock(*file_id).await?;
                self.validate_lock_timeout(*expires_at)?;
            }
            Operation::ReleaseLock { file_id, client_id } => {
                self.validate_release_lock(*file_id, *client_id).await?;
            }
            Operation::ExtendLock {
                file_id,
                client_id,
                new_expiry,
            } => {
                self.validate_extend_lock(*file_id, *client_id).await?;
                self.validate_lock_timeout(*new_expiry)?;
            }
        }

        Ok(())
    }
}

#[async_trait]
impl TransactionManager for TransactionManagerImpl {
    async fn begin(&self, timeout: Duration) -> Result<TxId> {
        // Validate timeout (max is 5 minutes - 300 seconds)
        let max_timeout = Duration::from_secs(300);
        if timeout > max_timeout {
            return Err(Error::InvalidTimeout(timeout, max_timeout));
        }

        // Check active transaction limit
        let transactions = self.active_transactions.read().await;
        if transactions.len() >= self.config.max_active_transactions {
            return Err(Error::TooManyTransactions(
                self.config.max_active_transactions,
            ));
        }
        drop(transactions);

        // Generate new transaction ID using UUIDv7
        // This provides time-ordered, globally unique IDs that work correctly
        // in distributed deployments without coordination between nodes
        let tx_id = TxId::generate();

        // Create transaction batch
        let batch = TransactionBatch::new(tx_id, timeout);

        // Store in active transactions
        let mut transactions = self.active_transactions.write().await;
        transactions.insert(tx_id, batch);

        info!("Transaction started: {:?}", tx_id);
        let _ = self.metrics.publish_counter(
            "transaction_manager.transactions_started",
            1,
            UnitType::Operations,
        );
        let _ = self.metrics.publish_gauge(
            "transaction_manager.active_transactions",
            transactions.len() as f64,
            UnitType::Operations,
        );

        Ok(tx_id)
    }

    async fn add_operation(&self, tx_id: TxId, operation: Operation) -> Result<()> {
        // Get transaction batch
        let mut transactions = self.active_transactions.write().await;
        let batch = transactions
            .get_mut(&tx_id)
            .ok_or(Error::TransactionNotFound(tx_id))?;

        // Check if expired
        if batch.is_expired() {
            drop(transactions);
            self.abort(tx_id).await?;
            return Err(Error::TransactionExpired(tx_id));
        }

        // Validate operation (drop lock during async call)
        let op_clone = operation.clone();
        drop(transactions);
        self.validate_operation(&op_clone).await?;

        // Add operation to batch
        let mut transactions = self.active_transactions.write().await;
        if let Some(batch) = transactions.get_mut(&tx_id) {
            batch.add_operation(operation);
            debug!(
                "Added operation to transaction {:?} (total: {})",
                tx_id,
                batch.operation_count()
            );
            let _ = self.metrics.publish_counter(
                "transaction_manager.operations_added",
                1,
                UnitType::Operations,
            );
        } else {
            return Err(Error::TransactionNotFound(tx_id));
        }

        Ok(())
    }

    async fn commit(&self, tx_id: TxId) -> Result<()> {
        // Remove transaction from active list
        let mut transactions = self.active_transactions.write().await;
        let batch = transactions
            .remove(&tx_id)
            .ok_or(Error::TransactionNotFound(tx_id))?;

        let _ = self.metrics.publish_gauge(
            "transaction_manager.active_transactions",
            transactions.len() as f64,
            UnitType::Operations,
        );
        drop(transactions);

        // Check for empty transaction
        if batch.operations.is_empty() {
            return Err(Error::EmptyTransaction(tx_id));
        }

        info!(
            "Committing transaction {:?} with {} operations",
            tx_id,
            batch.operations.len()
        );

        // Convert operations to MetadataOperations
        let metadata_ops: Vec<_> = batch
            .operations
            .into_iter()
            .map(|op| op.to_metadata_operation())
            .collect();

        // Create atomic transaction operation
        let raft_op = WormFsOperation::AtomicTransaction {
            tx_id,
            operations: metadata_ops,
            timeout: batch.created_at + batch.timeout,
        };

        // Submit to Raft (this blocks until committed and applied)
        let start = std::time::Instant::now();
        self.raft_member
            .propose_operation(raft_op)
            .await
            .map_err(|e| Error::RaftError(format!("{:?}", e)))?;
        let duration = start.elapsed();

        info!(
            "Transaction {:?} committed successfully (duration: {:?})",
            tx_id, duration
        );
        let _ = self.metrics.publish_counter(
            "transaction_manager.transactions_committed",
            1,
            UnitType::Operations,
        );
        let _ = self.metrics.publish_histogram(
            "transaction_manager.commit_duration_seconds",
            duration.as_secs_f64(),
            UnitType::Seconds,
        );

        Ok(())
    }

    async fn abort(&self, tx_id: TxId) -> Result<()> {
        // Remove transaction from active list
        let mut transactions = self.active_transactions.write().await;
        let batch = transactions
            .remove(&tx_id)
            .ok_or(Error::TransactionNotFound(tx_id))?;

        let _ = self.metrics.publish_gauge(
            "transaction_manager.active_transactions",
            transactions.len() as f64,
            UnitType::Operations,
        );

        info!(
            "Transaction {:?} aborted (had {} operations)",
            tx_id,
            batch.operations.len()
        );
        let _ = self.metrics.publish_counter(
            "transaction_manager.transactions_aborted",
            1,
            UnitType::Operations,
        );

        Ok(())
    }

    async fn active_count(&self) -> usize {
        let transactions = self.active_transactions.read().await;
        transactions.len()
    }

    async fn subscribe_metadata_changes(
        &self,
        filter: Option<Vec<crate::storage_raft_member::types::MetadataChangeType>>,
    ) -> tokio::sync::mpsc::UnboundedReceiver<crate::storage_raft_member::types::MetadataChangeEvent>
    {
        use crate::storage_raft_member::StorageRaftMember;

        // Forward the subscription request to the underlying Raft member
        self.raft_member.subscribe_metadata_changes(filter).await
    }
}

impl Drop for TransactionManagerImpl {
    fn drop(&mut self) {
        // Send shutdown signal to cleanup task (non-blocking)
        // The task will exit gracefully when it receives the signal
        let _ = self.shutdown_tx.send(());
        debug!("TransactionManager dropped, shutdown signal sent to cleanup task");
    }
}
