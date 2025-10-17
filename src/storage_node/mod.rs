//! # StorageNode Component
//!
//! StorageNode is the top-level orchestrator component that represents a fully-featured
//! WormFS storage node. It serves as the main entry point and dependency injection container.
//!
//! ## Responsibilities
//!
//! - Initializing and wiring together all subsystem components
//! - Managing the component lifecycle (startup, shutdown, graceful degradation)
//! - Loading and validating configuration
//! - Providing health checks and status reporting
//! - Coordinating graceful shutdown sequences
//!
//! ## Lifecycle Management
//!
//! StorageNode manages the initialization order of components:
//! 1. Load and validate configuration
//! 2. Initialize StorageNetwork (libp2p swarm)
//! 3. Initialize storage layers (TransactionLogStore, MetadataStore, SnapshotStore)
//! 4. Initialize FileStore for chunk management
//! 5. Initialize StorageRaftMember for consensus
//! 6. Initialize service layers (FileSystemService, StorageEndpoint)
//! 7. Initialize monitoring (StorageWatchdog, MetricService)
//!
//! ## Graceful Shutdown
//!
//! The shutdown sequence ensures data integrity:
//! 1. Stop accepting new requests (shutdown StorageEndpoint)
//! 2. Allow in-flight requests to complete (configurable timeout)
//! 3. Step down Raft leadership if leader
//! 4. Flush pending Raft log entries
//! 5. Stop StorageWatchdog
//! 6. Close stores cleanly
//! 7. Shutdown network
//!
//! ## Configuration
//!
//! Configuration can be loaded from:
//! - TOML configuration file (primary)
//! - Environment variables (override specific values)
//! - Command-line arguments (highest priority)

pub mod factory;
pub mod implementation;
pub mod types;

use async_trait::async_trait;
pub use factory::StorageNodeFactory;
pub use implementation::StorageNodeImpl;
pub use types::{Config, Error};

/// StorageNode trait defines the interface for the top-level storage node orchestrator.
///
/// Implementations of this trait are responsible for initializing all WormFS components
/// and managing their lifecycle.
#[cfg_attr(any(test, feature = "test-utils"), mockall::automock(
    type Status = ();
    type ClusterInfo = ();
))]
#[async_trait]
pub trait StorageNode: Send + Sync {
    /// Status type returned by health checks
    type Status;
    /// Cluster information type
    type ClusterInfo;

    /// Create and initialize a new StorageNode with the given configuration.
    ///
    /// This method performs all component initialization in the correct dependency order.
    /// If any component fails to initialize, cleanup is performed on already-initialized
    /// components.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for the storage node and all its components
    ///
    /// # Returns
    ///
    /// Returns `Ok(Self)` if initialization succeeds, or an error describing what failed.
    ///
    /// # Errors
    ///
    /// - Configuration validation errors
    /// - Component initialization failures
    /// - Network binding failures
    /// - Storage I/O errors
    async fn new(config: Config) -> Result<Self, Error>
    where
        Self: Sized;

    /// Start all components and begin serving requests.
    ///
    /// This method:
    /// - Starts the network swarm event loop
    /// - Begins Raft consensus participation
    /// - Opens the gRPC endpoint for client requests
    /// - Starts the watchdog monitoring loop
    /// - Initializes metric collection
    ///
    /// # Errors
    ///
    /// Returns an error if any component fails to start.
    async fn start(&mut self) -> Result<(), Error>;

    /// Gracefully shutdown the storage node.
    ///
    /// This method coordinates an orderly shutdown of all components, ensuring
    /// that in-flight operations complete and persistent state is flushed to disk.
    ///
    /// The shutdown sequence is designed to:
    /// - Prevent data loss
    /// - Maintain cluster stability
    /// - Allow quick restarts
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown cannot be completed cleanly. Callers should
    /// log the error and proceed with forceful shutdown if necessary.
    async fn shutdown(&mut self) -> Result<(), Error>;

    /// Get current node status and health information.
    ///
    /// This method provides a snapshot of the node's current state, including:
    /// - Raft role (Leader, Follower, Candidate)
    /// - Health status of all components
    /// - Uptime and basic statistics
    /// - Recent errors or warnings
    ///
    /// # Returns
    ///
    /// A status object containing health and state information.
    fn get_status(&self) -> Self::Status;

    /// Check if this node is currently the Raft leader.
    ///
    /// # Returns
    ///
    /// `true` if this node is the current Raft leader, `false` otherwise.
    fn is_leader(&self) -> bool;

    /// Get information about the cluster membership and state.
    ///
    /// This method returns:
    /// - List of all nodes in the cluster
    /// - Current leader
    /// - Node health status
    /// - Cluster-wide storage statistics
    ///
    /// # Errors
    ///
    /// Returns an error if cluster information cannot be retrieved (e.g., node is isolated).
    async fn get_cluster_info(&self) -> Result<Self::ClusterInfo, Error>;
}
