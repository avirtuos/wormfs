//! # ClusterManager
//!
//! Responsible for bootstrapping and managing the embedded storage cluster.

use crate::worm_validator::types::{ClusterConfig, ValidatorError};
use std::net::SocketAddr;
use std::path::PathBuf;

/// Manages the lifecycle of an embedded single-node storage cluster.
pub struct ClusterManager {
    /// Temporary directory for cluster data
    temp_dir: PathBuf,
    /// Cluster configuration
    config: ClusterConfig,
    /// Storage node instance (None if not started)
    storage_node: Option<()>, // TODO: Replace with Arc<dyn StorageNode>
    /// Endpoint address for gRPC connections
    endpoint_address: Option<SocketAddr>,
}

impl ClusterManager {
    /// Create a new ClusterManager.
    ///
    /// # Arguments
    ///
    /// * `temp_dir` - Temporary directory for cluster data
    /// * `config` - Cluster configuration
    pub fn new(temp_dir: PathBuf, config: ClusterConfig) -> Self {
        Self {
            temp_dir,
            config,
            storage_node: None,
            endpoint_address: None,
        }
    }

    /// Start the embedded storage cluster.
    ///
    /// This method:
    /// - Creates necessary directories
    /// - Initializes all storage components
    /// - Starts the StorageNode
    /// - Waits for the node to be ready
    ///
    /// # Errors
    ///
    /// Returns an error if cluster startup fails.
    pub async fn start(&mut self) -> Result<(), ValidatorError> {
        // TODO: Implement cluster startup
        // 1. Create temp directories
        // 2. Build StorageNode config
        // 3. Initialize StorageNode
        // 4. Start StorageNode
        // 5. Wait for readiness
        unimplemented!("ClusterManager::start")
    }

    /// Stop the embedded storage cluster.
    ///
    /// This method performs a graceful shutdown of the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown fails.
    pub async fn stop(&mut self) -> Result<(), ValidatorError> {
        // TODO: Implement cluster shutdown
        // 1. Shutdown StorageNode
        // 2. Wait for clean shutdown
        // 3. Clear storage_node reference
        unimplemented!("ClusterManager::stop")
    }

    /// Get the endpoint address for gRPC connections.
    ///
    /// # Returns
    ///
    /// The socket address of the gRPC endpoint, or None if cluster is not started.
    pub fn endpoint_address(&self) -> Option<SocketAddr> {
        self.endpoint_address
    }

    /// Check if the cluster is running.
    pub fn is_running(&self) -> bool {
        self.storage_node.is_some()
    }
}
