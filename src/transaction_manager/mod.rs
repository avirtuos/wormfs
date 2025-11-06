//! # TransactionManager Component
//!
//! The TransactionManager provides a high-level API for grouping multiple metadata operations
//! into atomic transactions. It leverages Raft's consensus guarantees to ensure that all
//! operations in a transaction are applied atomically across the cluster.
//!
//! ## Design Philosophy
//!
//! Rather than implementing two-phase commit on top of Raft (which would double latency),
//! this component batches operations and submits them as a single Raft log entry. This
//! leverages OpenRaft's native atomicity guarantees:
//!
//! - **Single Raft Consensus Round**: All operations commit together in one log entry
//! - **Pre-validation**: Operations are validated locally before Raft submission (fail fast)
//! - **Atomic Application**: Either all operations apply to MetadataStore, or none do
//! - **Linearizable**: Raft's quorum-based commits provide linearizability
//!
//! ## Architecture
//!
//! ```text
//! Application
//!     ↓
//! TransactionManager (this component)
//!     ├─ Validate operations locally
//!     ├─ Batch operations
//!     └─ Submit to Raft as single AtomicTransaction
//!         ↓
//! StorageRaftMember
//!     ├─ Replicate to quorum
//!     ├─ Commit log entry
//!     └─ Apply to state machine
//!         ↓
//! MetadataStore (all operations applied atomically)
//! ```
//!
//! ## Usage Example
//!
//! ```rust,ignore
//! use wormfs::transaction_manager::{TransactionManager, Operation};
//! use wormfs::storage_raft_member::types::{FileMetadata, StoragePolicy};
//! use std::time::Duration;
//! use std::path::PathBuf;
//!
//! # async fn example(tx_manager: std::sync::Arc<dyn TransactionManager>) -> Result<(), Box<dyn std::error::Error>> {
//! // Begin a new transaction
//! let tx_id = tx_manager.begin(Duration::from_secs(30)).await?;
//!
//! // Add operations to the transaction
//! tx_manager.add_operation(tx_id, Operation::CreateFile {
//!     path: PathBuf::from("/videos/movie.mp4"),
//!     inode: 12345,
//!     metadata: FileMetadata { /* ... */ },
//!     policy: StoragePolicy::default(),
//! }).await?;
//!
//! tx_manager.add_operation(tx_id, Operation::CreateStripe {
//!     file_id: /* ... */,
//!     stripe_id: /* ... */,
//!     // ...
//! }).await?;
//!
//! // Commit the transaction (single Raft round)
//! tx_manager.commit(tx_id).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Performance Characteristics
//!
//! - **Latency**: Single Raft consensus round (~50ms typical in 3-node cluster)
//! - **Throughput**: Limited by Raft leader's write capacity
//! - **Validation**: O(n) where n = number of operations (local, no Raft overhead)
//! - **Memory**: O(active_transactions * operations_per_transaction)
//!
//! ## Consistency Guarantees
//!
//! - **Atomicity**: All operations in a transaction commit together or none do
//! - **Consistency**: Pre-validation ensures operations are valid before Raft submission
//! - **Isolation**: Operations are not visible until committed
//! - **Durability**: Once committed, operations are durable across majority quorum
//!
//! ## Error Handling
//!
//! - **Validation Failures**: Caught early, before Raft submission (fast failure)
//! - **Raft Failures**: Network issues, leader election, quorum loss (transparent retry)
//! - **Apply Failures**: Critical error - indicates state divergence (node panics)
//!
//! ## Comparison with Two-Phase Commit
//!
//! Traditional 2PC on top of Raft would require:
//! 1. **Prepare**: Submit to Raft, wait for quorum (~50ms)
//! 2. **Commit**: Submit to Raft, wait for quorum (~50ms)
//! 3. **Total**: ~100ms latency
//!
//! Our approach:
//! 1. **Validate**: Local check (~1ms)
//! 2. **Commit**: Submit to Raft, wait for quorum (~50ms)
//! 3. **Total**: ~51ms latency
//!
//! We achieve **50% latency reduction** by trusting Raft's atomicity guarantees.

pub mod types;

mod factory;
mod implementation;

#[cfg(test)]
mod tests;

pub use factory::TransactionManagerFactory;
pub use types::{Config, Error, Operation, Result, TransactionBatch, TxId};

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

/// Core trait for transaction management.
///
/// This trait provides the interface for batching multiple metadata operations
/// into atomic transactions that are submitted through Raft consensus.
///
/// ## Lifecycle
///
/// 1. **Begin**: Create a new transaction with a timeout
/// 2. **Add Operations**: Add one or more operations (validated locally)
/// 3. **Commit**: Submit all operations as a single atomic transaction
///
/// Or:
///
/// 3. **Abort**: Discard the transaction (local-only, no Raft interaction)
///
/// ## Thread Safety
///
/// All methods are async and can be called concurrently from multiple tasks.
/// The implementation uses internal synchronization to ensure correctness.
#[async_trait]
pub trait TransactionManager: Send + Sync {
    /// Begin a new transaction.
    ///
    /// Creates a new transaction batch with the specified timeout. Returns a unique
    /// transaction ID that is used in subsequent operations.
    ///
    /// # Arguments
    ///
    /// * `timeout` - How long before this transaction expires and is auto-aborted
    ///
    /// # Returns
    ///
    /// * `Ok(TxId)` - Unique transaction identifier
    /// * `Err(Error::TooManyTransactions)` - If max active transactions limit reached
    /// * `Err(Error::InvalidTimeout)` - If timeout exceeds configured maximum
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # async fn example(tx_manager: std::sync::Arc<dyn wormfs::TransactionManager>) -> Result<(), Box<dyn std::error::Error>> {
    /// use std::time::Duration;
    ///
    /// let tx_id = tx_manager.begin(Duration::from_secs(30)).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn begin(&self, timeout: Duration) -> Result<TxId>;

    /// Add an operation to an existing transaction.
    ///
    /// Validates the operation locally before adding it to the batch. This provides
    /// fail-fast behavior - invalid operations are rejected immediately without any
    /// Raft overhead.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - Transaction identifier from `begin()`
    /// * `operation` - Operation to add to the transaction
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Operation validated and added successfully
    /// * `Err(Error::TransactionNotFound)` - Transaction ID not found or already completed
    /// * `Err(Error::TransactionExpired)` - Transaction has expired
    /// * `Err(Error::ValidationFailed)` - Operation validation failed
    ///
    /// # Validation
    ///
    /// The following checks are performed:
    /// - **CreateFile**: Parent directory exists, no conflict with existing file
    /// - **UpdateFile**: File exists
    /// - **DeleteFile**: File exists
    /// - **CreateStripe**: File exists
    /// - **DeleteStripe**: Stripe exists
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # async fn example(
    /// #     tx_manager: std::sync::Arc<dyn wormfs::TransactionManager>,
    /// #     tx_id: wormfs::storage_raft_member::types::TxId,
    /// # ) -> Result<(), Box<dyn std::error::Error>> {
    /// use wormfs::transaction_manager::Operation;
    /// use std::path::PathBuf;
    ///
    /// tx_manager.add_operation(tx_id, Operation::DeleteFile {
    ///     file_id: /* ... */,
    ///     inode: 12345,
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn add_operation(&self, tx_id: TxId, operation: Operation) -> Result<()>;

    /// Commit a transaction.
    ///
    /// Submits all operations in the transaction as a single atomic Raft operation.
    /// This method blocks until the transaction has been replicated to a quorum of nodes
    /// and applied to the state machine.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - Transaction identifier from `begin()`
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Transaction committed successfully across cluster
    /// * `Err(Error::TransactionNotFound)` - Transaction ID not found
    /// * `Err(Error::EmptyTransaction)` - No operations were added to the transaction
    /// * `Err(Error::RaftError)` - Raft consensus failed
    ///
    /// # Guarantees
    ///
    /// - **Atomicity**: All operations commit together or none do
    /// - **Durability**: Once this returns Ok, the transaction is durable
    /// - **Linearizability**: Operations are visible to all nodes in log order
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # async fn example(
    /// #     tx_manager: std::sync::Arc<dyn wormfs::TransactionManager>,
    /// #     tx_id: wormfs::storage_raft_member::types::TxId,
    /// # ) -> Result<(), Box<dyn std::error::Error>> {
    /// // After adding operations...
    /// tx_manager.commit(tx_id).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn commit(&self, tx_id: TxId) -> Result<()>;

    /// Abort a transaction.
    ///
    /// Discards the transaction batch without submitting to Raft. This is a local-only
    /// operation with no cluster coordination required.
    ///
    /// # Arguments
    ///
    /// * `tx_id` - Transaction identifier from `begin()`
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Transaction aborted successfully
    /// * `Err(Error::TransactionNotFound)` - Transaction ID not found
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # async fn example(
    /// #     tx_manager: std::sync::Arc<dyn wormfs::TransactionManager>,
    /// #     tx_id: wormfs::storage_raft_member::types::TxId,
    /// # ) -> Result<(), Box<dyn std::error::Error>> {
    /// // If something goes wrong...
    /// tx_manager.abort(tx_id).await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn abort(&self, tx_id: TxId) -> Result<()>;

    /// Get the number of currently active transactions.
    ///
    /// This is useful for monitoring and observability. Active transactions are those
    /// that have been started with `begin()` but not yet committed or aborted.
    ///
    /// # Returns
    ///
    /// The number of active transactions
    async fn active_count(&self) -> usize;

    /// Subscribe to metadata change events.
    ///
    /// Returns a channel receiver that will receive notifications when metadata changes
    /// are committed through the Raft state machine. This allows applications to react
    /// to filesystem changes in real-time.
    ///
    /// # Arguments
    ///
    /// * `filter` - Optional list of event types to receive. If None, all events are received.
    ///
    /// # Returns
    ///
    /// An unbounded receiver channel for `MetadataChangeEvent`.
    ///
    /// # Notes
    ///
    /// - Events are sent asynchronously and do not block transaction commits
    /// - Slow subscribers may experience channel capacity issues
    /// - At-most-once delivery semantics (events may be missed if channel is full)
    /// - The receiver should be consumed in a loop to prevent memory growth
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # async fn example(tx_manager: std::sync::Arc<dyn wormfs::TransactionManager>) -> Result<(), Box<dyn std::error::Error>> {
    /// use wormfs::storage_raft_member::types::{MetadataChangeType, MetadataChangeEvent};
    ///
    /// // Subscribe to all events
    /// let mut rx = tx_manager.subscribe_metadata_changes(None).await;
    ///
    /// // Or subscribe to specific event types
    /// let mut rx = tx_manager.subscribe_metadata_changes(Some(vec![
    ///     MetadataChangeType::FileCreated,
    ///     MetadataChangeType::FileDeleted,
    /// ])).await;
    ///
    /// // Process events
    /// tokio::spawn(async move {
    ///     while let Some(event) = rx.recv().await {
    ///         println!("Metadata changed at log index {}", event.log_index);
    ///         for change in event.changes {
    ///             // Handle each change...
    ///         }
    ///     }
    /// });
    /// # Ok(())
    /// # }
    /// ```
    async fn subscribe_metadata_changes(
        &self,
        filter: Option<Vec<crate::storage_raft_member::types::MetadataChangeType>>,
    ) -> tokio::sync::mpsc::UnboundedReceiver<crate::storage_raft_member::types::MetadataChangeEvent>;
}

/// Type alias for shared TransactionManager trait object.
pub type TransactionManagerRef = Arc<dyn TransactionManager>;
