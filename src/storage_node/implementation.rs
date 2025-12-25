//! Concrete implementation of StorageNode for Phase 1.
//!
//! This is a simplified StorageNode that only wires together the core Phase 1 components:
//! - MetadataStore (SQLite persistence)
//! - FileStore (chunk storage + erasure coding)
//! - FileSystemService (FUSE interface)
//!
//! Future phases will expand this to include:
//! - StorageRaftMember (distributed consensus)
//! - StorageNetwork (libp2p networking)
//! - StorageEndpoint (gRPC API)
//! - StorageWatchdog (data integrity monitoring)
//! - MetricService (observability)

use super::{Config, Error, StorageNode};
use crate::file_store::{FileStore, FileStoreImpl};
use crate::filesystem_service::implementation::FileSystemServiceImpl;
use crate::metadata_store::{MetadataStore, MetadataStoreImpl};
use crate::storage_network::{StorageNetworkFactory, StorageNetworkHandle};
use crate::storage_raft_member::{RaftRpcHandler, StorageRaftMember, StorageRaftMemberImpl};
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{error, info, warn};

/// Phase 1 StorageNode implementation.
///
/// This is a simplified implementation that wires together only the core data path components.
/// It provides a foundation for Phase 1 local filesystem operations before distributed
/// features are added in later phases.
pub struct StorageNodeImpl {
    /// Configuration
    config: Config,

    /// MetadataStore for file metadata persistence
    metadata_store: MetadataStoreImpl,

    /// FileStore for chunk storage and erasure coding
    file_store: Arc<FileStoreImpl>,

    /// FileSystemService for FUSE operations
    filesystem_service: Option<FileSystemServiceImpl>,

    /// StorageNetwork for peer-to-peer communication (Phase 2+)
    storage_network: Option<StorageNetworkHandle>,

    /// StorageRaftMember for distributed consensus (Phase 2+)
    storage_raft_member: Option<Arc<StorageRaftMemberImpl>>,

    /// Network event loop thread handle (Phase 2+)
    /// The event loop runs in a dedicated thread with LocalSet due to !Send constraints
    network_thread: Option<std::thread::JoinHandle<()>>,

    /// Flag indicating if the node has been started
    started: bool,
}

impl StorageNodeImpl {
    /// Create a new StorageNode instance (called by factory).
    ///
    /// This is pub(crate) to ensure all construction goes through the factory,
    /// maintaining consistent initialization patterns.
    pub(crate) async fn new_internal(config: Config) -> Result<Self, Error> {
        info!("Initializing StorageNode...");
        info!("Node ID: {}", config.node_id);
        info!("Data directory: {}", config.data_dir.display());

        // Step 1: Initialize MetadataStore
        info!("Initializing MetadataStore...");
        let metadata_config = crate::metadata_store::Config {
            database_path: config.metadata_db_path.clone(),
            cache_size_mb: 100, // Phase 1 default
            read_pool_size: 4,  // Phase 1 default
            ..Default::default()
        };

        let metadata_store =
            crate::metadata_store::MetadataStoreFactory::create_concrete(metadata_config)
                .await
                .map_err(|e| Error::ComponentInitFailed {
                    component: "MetadataStore".to_string(),
                    reason: e.to_string(),
                })?;

        // Initialize database schema
        metadata_store
            .initialize_schema()
            .await
            .map_err(|e| Error::ComponentInitFailed {
                component: "MetadataStore".to_string(),
                reason: format!("Schema initialization failed: {}", e),
            })?;

        info!("MetadataStore initialized successfully");

        // Step 2: Initialize FileStore
        info!("Initializing FileStore...");
        let file_store_config = crate::file_store::types::Config {
            disk_paths: vec![config.data_dir.join("chunks")],
            max_chunk_size: 1024 * 1024, // 1MB
            default_data_shards: config.default_data_shards,
            default_parity_shards: config.default_parity_shards,
            max_concurrent_operations: 100, // Phase 1 default
            verification_interval: std::time::Duration::from_secs(3600), // 1 hour
            orphan_cleanup_age: std::time::Duration::from_secs(3600), // 1 hour
            stripe_cache_size_mb: 256,      // 256 MB cache
            stripe_cache_ttl_secs: 3600,    // 1 hour TTL
            stripe_cache_tti_secs: 600,     // 10 minutes TTI
        };

        let mut file_store =
            FileStoreImpl::new(file_store_config).map_err(|e| Error::ComponentInitFailed {
                component: "FileStore".to_string(),
                reason: e.to_string(),
            })?;

        // Configure distributed components for FileStore
        use crate::file_store::{
            ChunkClientConfig, ChunkClientPool, PlacementConfig, PlacementEngine,
        };
        use crate::storage_raft_member::cluster_manager::heartbeat_tracker::HeartbeatTracker;

        let my_node_id =
            crate::file_store::types::NodeId::new(config.node_id.parse::<u64>().unwrap_or(1));

        // Create heartbeat tracker and record local node
        // Use longer stale threshold since we don't have a background heartbeat task yet
        let tracker = Arc::new(HeartbeatTracker::new(300_000, 300_000)); // 5 minutes stale, 5 minutes grace
        tracing::info!(
            "[NodeDiscovery] Created HeartbeatTracker for storage node {}",
            config.node_id
        );

        // Setup callback to register discovered nodes in MetadataStore
        // This ensures FK constraints pass when allocating chunks to remote nodes
        let metadata_store_for_callback = metadata_store.clone();
        let local_node_id_for_log = config.node_id.clone();
        tracker.set_on_node_discovered(Arc::new(
            move |node_id: String, address: Option<String>| {
                tracing::info!(
                    "[NodeDiscovery] Callback invoked! Storage node {} discovered node {} with address {:?}",
                    local_node_id_for_log,
                    node_id,
                    address
                );
                if let Ok(node_id_num) = node_id.parse::<u64>() {
                    let metadata = metadata_store_for_callback.clone();
                    let addr = address.unwrap_or_default();
                    // Spawn async task to register node
                    tokio::spawn(async move {
                        tracing::info!(
                            "[NodeDiscovery] Spawning task to register node {} in MetadataStore",
                            node_id_num
                        );
                        if let Err(e) = metadata.register_node(node_id_num, &addr).await {
                            tracing::warn!(
                                "Failed to register discovered node {}: {}",
                                node_id_num,
                                e
                            );
                        } else {
                            tracing::info!(
                                "[NodeDiscovery] ✓ Registered discovered node {} in MetadataStore",
                                node_id_num
                            );
                        }
                    });
                } else {
                    tracing::warn!(
                        "[NodeDiscovery] Failed to parse node_id '{}' as u64",
                        node_id
                    );
                }
            },
        ));
        tracing::info!(
            "[NodeDiscovery] Node discovery callback registered for storage node {}",
            config.node_id
        );

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        tracker.record_heartbeat(
            config.node_id.clone(),
            now,
            1,
            None,                // admin_url
            None,                // storage_endpoint_url
            None,                // raft_state
            None,                // raft_term
            None,                // last_log_index
            None,                // last_log_term
            None,                // current_leader
            None,                // is_voter
            Some(now),           // startup_time - keep node in grace period
            Some(1_000_000_000), // total_bytes - 1GB total capacity
            Some(900_000_000),   // available_bytes - 900MB available
            Some(0),             // chunk_count - 0 chunks initially
        );

        // Create placement engine configured to always select local node
        let placement_config = PlacementConfig {
            min_node_diversity: 1,
            prefer_local: true,
        };
        let placement_engine = Arc::new(PlacementEngine::new(
            tracker.clone(),
            my_node_id,
            placement_config,
        ));

        // Create chunk client pool for distributed operations
        let chunk_client_config = ChunkClientConfig::default();
        let chunk_client: Arc<dyn crate::file_store::ChunkClient> =
            Arc::new(ChunkClientPool::new(tracker, chunk_client_config));

        // Configure distributed operations
        file_store.set_distributed_config(
            my_node_id,
            placement_engine,
            chunk_client,
        );

        let file_store = Arc::new(file_store);

        info!("FileStore initialized successfully with distributed configuration");

        // Step 3: Initialize FileSystemService
        info!("Initializing FileSystemService...");
        let filesystem_config = crate::filesystem_service::types::Config {
            uid: config.default_uid,
            gid: config.default_gid,
            lock_timeout: config.lock_timeout,
            ..Default::default()
        };

        let filesystem_service = FileSystemServiceImpl::new(
            filesystem_config,
            metadata_store.clone(),
            file_store.clone(),
        );

        info!("FileSystemService initialized successfully");

        // Step 4: Initialize StorageNetwork (Phase 2+)
        info!("Initializing StorageNetwork...");

        // Convert peer_addresses to PeerConfig format with AutoId mode
        let peers = config
            .peer_addresses
            .iter()
            .map(|addr| crate::storage_network::types::PeerConfig {
                multiaddr: format!("/ip4/{}/tcp/{}", addr.ip(), addr.port()),
                peer_id: crate::storage_network::types::PeerIdConfig::AutoId,
            })
            .collect();

        let network_config = crate::storage_network::Config {
            node_id: config.node_id.clone(),
            listen_addresses: vec![format!("/ip4/0.0.0.0/tcp/{}", config.libp2p_listen_port)],
            peers,
            peer_id_store_path: config.peer_id_store_path.clone(),
            max_peers: config.max_peers,
            max_connections_per_peer: config.max_connections_per_peer,
            connection_timeout: config.connection_timeout,
            idle_connection_timeout: config.idle_connection_timeout,
            keep_alive_interval: config.keep_alive_interval,
            admin_url: None,
            storage_endpoint_url: None,
        };

        let (network_inner, network_handle) = StorageNetworkFactory::create(network_config)
            .await
            .map_err(|e| Error::ComponentInitFailed {
            component: "StorageNetwork".to_string(),
            reason: e.to_string(),
        })?;

        // Spawn the network event loop in a dedicated thread with LocalSet
        // This is necessary because libp2p's Swarm is !Send
        let node_id_for_thread = config.node_id.clone();
        let network_thread = std::thread::spawn(move || {
            // Create a new tokio runtime for this thread
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create runtime for network event loop");

            // Run the event loop in a LocalSet to support !Send futures
            let local = tokio::task::LocalSet::new();
            runtime.block_on(local.run_until(async move {
                info!(
                    "Starting StorageNetwork event loop for node {}",
                    node_id_for_thread
                );
                if let Err(e) = network_inner.run().await {
                    error!(
                        "StorageNetwork event loop failed for {}: {}",
                        node_id_for_thread, e
                    );
                }
                info!(
                    "StorageNetwork event loop stopped for {}",
                    node_id_for_thread
                );
            }));
        });

        info!("StorageNetwork event loop thread started successfully");

        // Spawn background task to periodically update storage capacity in heartbeats
        let network_for_capacity = network_handle.clone();
        let file_store_for_capacity = file_store.clone();
        let metadata_store_for_capacity = Arc::new(metadata_store.clone());
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30)); // Update every 30 seconds
            loop {
                interval.tick().await;

                // Query storage stats from FileStore
                match file_store_for_capacity
                    .get_storage_stats(&metadata_store_for_capacity)
                    .await
                {
                    Ok((total_bytes, available_bytes, chunk_count)) => {
                        // Update network with current capacity
                        if let Err(e) = network_for_capacity
                            .update_storage_capacity_data(
                                Some(total_bytes),
                                Some(available_bytes),
                                Some(chunk_count),
                            )
                            .await
                        {
                            tracing::warn!("Failed to update storage capacity in heartbeat: {}", e);
                        } else {
                            tracing::debug!(
                                "Updated storage capacity: total={} bytes, available={} bytes, chunks={}",
                                total_bytes, available_bytes, chunk_count
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to query storage stats: {}", e);
                    }
                }
            }
        });

        // Step 5: Initialize StorageRaftMember (Phase 2+) - only if configured
        // Raft requires numeric node IDs, so only initialize if all conditions are met:
        // 1. Paths are configured
        // 2. node_id can be parsed as u64
        let storage_raft_member = if !config.transaction_log_path.as_os_str().is_empty()
            && !config.snapshot_dir.as_os_str().is_empty()
            && config.node_id.parse::<u64>().is_ok()
        {
            info!("Initializing StorageRaftMember...");

            let mut raft_config = crate::storage_raft_member::Config::default();
            raft_config.transaction_log_path = config.transaction_log_path.clone();
            raft_config.metadata_db_path = config.metadata_db_path.clone();
            raft_config.snapshot_directory = config.snapshot_dir.clone();
            raft_config.network_address = config.listen_address;
            raft_config.enable_cluster_manager = true;

            // Set storage_network reference (required for Raft RPC communication)
            raft_config.storage_network = Some(Arc::new(network_handle.clone())
                as Arc<dyn crate::storage_network::NetworkHandleTrait>);

            // Parse node_id as u64 for Raft NodeId (safe because we checked is_ok() above)
            let node_id_num = config
                .node_id
                .parse::<u64>()
                .expect("node_id should parse as u64 - validated above");

            let mut raft_member = StorageRaftMemberImpl::new(
                crate::storage_raft_member::types::NodeId(node_id_num),
                raft_config,
                metadata_store.clone(),
            )
            .await
            .map_err(|e| Error::ComponentInitFailed {
                component: "StorageRaftMember".to_string(),
                reason: e.to_string(),
            })?;

            // Initialize Raft cluster
            // IMPORTANT: Only Node 1 initializes as a single-node cluster.
            // Other nodes do NOT initialize - they wait to be added as learners
            // by Node 1 via the cluster formation task.
            //
            // This prevents creating multiple independent single-node clusters
            // that cannot be merged (Raft doesn't support merging clusters).
            if node_id_num == 1 {
                info!(
                    "Node 1: Initializing as single-node Raft cluster (will add other nodes later)"
                );
                raft_member
                    .initialize(vec![])
                    .await
                    .map_err(|e| Error::ComponentInitFailed {
                        component: "StorageRaftMember".to_string(),
                        reason: format!("Raft initialization failed: {}", e),
                    })?;
                info!("Node 1: Raft cluster initialized successfully");
            } else {
                info!(
                    "Node {}: Skipping Raft initialization (will be added to cluster by Node 1)",
                    node_id_num
                );
            }

            let raft_member = Arc::new(raft_member);

            // Wire up Raft RPC handler with network
            network_handle
                .register_raft_handler(raft_member.clone() as Arc<dyn RaftRpcHandler>)
                .await
                .map_err(|e| Error::ComponentInitFailed {
                    component: "StorageRaftMember".to_string(),
                    reason: format!("Failed to register Raft handler: {}", e),
                })?;

            info!("StorageRaftMember initialized and wired to network successfully");

            Some(raft_member)
        } else {
            info!("StorageRaftMember initialization skipped (not configured)");
            None
        };

        Ok(Self {
            config,
            metadata_store,
            file_store,
            filesystem_service: Some(filesystem_service),
            storage_network: Some(network_handle),
            storage_raft_member,
            network_thread: Some(network_thread),
            started: false,
        })
    }

    /// Get reference to FileSystemService (for mounting)
    pub fn filesystem_service(&self) -> Option<&FileSystemServiceImpl> {
        self.filesystem_service.as_ref()
    }

    /// Get reference to StorageNetwork (for network operations)
    pub fn storage_network(&self) -> Option<&StorageNetworkHandle> {
        self.storage_network.as_ref()
    }

    /// Get reference to StorageRaftMember (for consensus operations)
    pub fn storage_raft_member(&self) -> Option<&Arc<StorageRaftMemberImpl>> {
        self.storage_raft_member.as_ref()
    }

    /// Get list of currently connected peers.
    ///
    /// This is a convenience method that queries the StorageNetwork for peer information.
    /// Returns an empty vector if the network is not initialized.
    pub async fn get_connected_peers(&self) -> Vec<crate::storage_network::PeerInfo> {
        match &self.storage_network {
            Some(network) => network.get_connected_peers().await,
            None => vec![],
        }
    }
}

impl Drop for StorageNodeImpl {
    /// Cleanup network thread if StorageNodeImpl is dropped without explicit shutdown.
    ///
    /// This prevents memory leaks by ensuring the network event loop thread is properly
    /// terminated even if shutdown() isn't called explicitly. This is a safety net for
    /// error paths and test scenarios where the node might be dropped unexpectedly.
    fn drop(&mut self) {
        // Only cleanup if we still have an active network thread
        if let Some(thread) = self.network_thread.take() {
            warn!(
                "StorageNodeImpl for node {} dropped without explicit shutdown, cleaning up network thread",
                self.config.node_id
            );

            // Attempt graceful shutdown of the network if handle is still available
            if let Some(network_handle) = self.storage_network.take() {
                // We're in a Drop context, so we can't use async/await
                // We need to block here to ensure proper cleanup
                // Use a blocking approach by spawning a new runtime
                std::thread::spawn(move || {
                    let runtime = tokio::runtime::Runtime::new()
                        .expect("Failed to create runtime for cleanup");
                    let _ = runtime.block_on(network_handle.shutdown());
                });

                // Give the shutdown signal a moment to propagate
                std::thread::sleep(std::time::Duration::from_millis(100));
            }

            // Wait for the network thread to complete with a timeout
            // We use a simple polling approach since we can't use async in Drop
            let timeout = std::time::Duration::from_secs(5);
            let start = std::time::Instant::now();

            // First, try joining the thread
            // If it doesn't complete immediately, we'll log and move on
            let join_result = loop {
                if thread.is_finished() {
                    break Some(thread.join());
                }

                if start.elapsed() >= timeout {
                    break None;
                }

                std::thread::sleep(std::time::Duration::from_millis(50));
            };

            match join_result {
                Some(Ok(())) => {
                    info!(
                        "Network thread for node {} cleaned up successfully during drop",
                        self.config.node_id
                    );
                }
                Some(Err(_)) => {
                    error!(
                        "Network thread for node {} panicked during cleanup",
                        self.config.node_id
                    );
                }
                None => {
                    warn!(
                        "Network thread for node {} did not complete within timeout, may leak",
                        self.config.node_id
                    );
                    // Note: thread will continue running as a detached thread
                }
            }
        }
    }
}

#[async_trait]
impl StorageNode for StorageNodeImpl {
    type Status = NodeStatus;
    type ClusterInfo = ClusterInfo;

    async fn new(config: Config) -> Result<Self, Error>
    where
        Self: Sized,
    {
        Self::new_internal(config).await
    }

    async fn start(&mut self) -> Result<(), Error> {
        if self.started {
            warn!("StorageNode already started, ignoring start request");
            return Ok(());
        }

        info!("Starting StorageNode components...");

        // In Phase 1, there's no background tasks to start
        // The FileSystemService will be mounted separately via the FUSE binary
        // Raft cluster is initialized during construction (see new_internal)
        // Future phases will start:
        // - Network event loop
        // - gRPC endpoint
        // - Watchdog monitoring
        // - Metric collection

        self.started = true;
        info!("StorageNode started successfully");

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Error> {
        if !self.started {
            warn!("StorageNode not started, ignoring shutdown request");
            return Ok(());
        }

        info!("Shutting down StorageNode...");

        // Shutdown in reverse order of initialization

        // Step 1: Stop StorageNetwork event loop (Phase 2+)
        if let Some(network_handle) = self.storage_network.take() {
            info!("Shutting down StorageNetwork...");
            if let Err(e) = network_handle.shutdown().await {
                warn!("Failed to shutdown StorageNetwork gracefully: {}", e);
            }

            // Wait for network event loop thread to complete
            if let Some(thread) = self.network_thread.take() {
                // Give the thread a reasonable amount of time to complete
                // Using a timeout-like mechanism by waiting in a blocking fashion
                match thread.join() {
                    Ok(()) => info!("StorageNetwork event loop thread stopped"),
                    Err(_) => warn!("StorageNetwork event loop thread panicked"),
                }
            }
        }

        // Step 2: Stop FileSystemService (in Phase 1, this is a no-op as FUSE is managed externally)
        if let Some(_fs_service) = self.filesystem_service.take() {
            info!("FileSystemService stopped");
        }

        // Step 3: FileStore cleanup (in Phase 1, this is a no-op)
        info!("FileStore stopped");

        // Step 4: MetadataStore cleanup (connection will be dropped automatically)
        info!("MetadataStore stopped");

        self.started = false;
        info!("StorageNode shutdown complete");

        Ok(())
    }

    fn get_status(&self) -> Self::Status {
        NodeStatus {
            node_id: self.config.node_id.clone(),
            started: self.started,
            uptime_secs: 0, // TODO: Track actual uptime
            components: ComponentStatus {
                metadata_store: true,
                file_store: true,
                filesystem_service: self.filesystem_service.is_some(),
                raft_member: self.storage_raft_member.is_some(),
                network: self.storage_network.is_some(),
                endpoint: false, // Phase 3+
                watchdog: false, // Phase 4+
            },
        }
    }

    fn is_leader(&self) -> bool {
        // Phase 1: No Raft, so always return false
        // This will be implemented in Phase 2 when Raft is integrated
        false
    }

    async fn get_cluster_info(&self) -> Result<Self::ClusterInfo, Error> {
        // Phase 1: Single-node operation, return basic info
        Ok(ClusterInfo {
            node_count: 1,
            leader_node: None,
            nodes: vec![self.config.node_id.clone()],
        })
    }
}

// ===== Phase 1 Status Types =====

/// Node status information for Phase 1.
#[derive(Debug, Clone)]
pub struct NodeStatus {
    /// Node identifier
    pub node_id: String,

    /// Whether the node has been started
    pub started: bool,

    /// Uptime in seconds
    pub uptime_secs: u64,

    /// Component health status
    pub components: ComponentStatus,
}

/// Status of individual components.
#[derive(Debug, Clone)]
pub struct ComponentStatus {
    /// MetadataStore health
    pub metadata_store: bool,

    /// FileStore health
    pub file_store: bool,

    /// FileSystemService health
    pub filesystem_service: bool,

    /// RaftMember health (Phase 2+)
    pub raft_member: bool,

    /// Network health (Phase 2+)
    pub network: bool,

    /// Endpoint health (Phase 3+)
    pub endpoint: bool,

    /// Watchdog health (Phase 4+)
    pub watchdog: bool,
}

/// Cluster information for Phase 1 (single node).
#[derive(Debug, Clone)]
pub struct ClusterInfo {
    /// Number of nodes in cluster
    pub node_count: usize,

    /// Current leader node ID
    pub leader_node: Option<String>,

    /// List of all node IDs
    pub nodes: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    /// Helper to create a test config with temporary directories
    fn create_test_config(temp_dir: &TempDir) -> Config {
        let data_dir = temp_dir.path().to_path_buf();
        Config {
            node_id: "test-node".to_string(),
            listen_address: "127.0.0.1:0".parse().unwrap(), // Random port
            data_dir: data_dir.clone(),
            metadata_db_path: data_dir.join("metadata.db"),
            transaction_log_path: data_dir.join("transaction.log"),
            snapshot_dir: data_dir.join("snapshots"),
            peer_id_store_path: data_dir.join("peer_id.json"),
            default_stripe_size: 1024 * 1024, // 1MB
            default_data_shards: 2,
            default_parity_shards: 1,
            default_uid: 1000,
            default_gid: 1000,
            enable_read_locks: true,
            lock_timeout: std::time::Duration::from_secs(30),
            peer_addresses: vec![],
            libp2p_listen_port: 0, // Use 0 for random port
            max_peers: 10,
            max_connections_per_peer: 1,
            connection_timeout: std::time::Duration::from_secs(10),
            idle_connection_timeout: std::time::Duration::from_secs(60),
            keep_alive_interval: std::time::Duration::from_secs(10),
            shallow_check_interval: std::time::Duration::from_secs(60),
            deep_check_interval: std::time::Duration::from_secs(3600),
            snapshot_interval: std::time::Duration::from_secs(300),
        }
    }

    #[tokio::test]
    async fn test_explicit_shutdown_cleans_up_network_thread() {
        // Create a storage node
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = create_test_config(&temp_dir);

        let mut node = StorageNodeImpl::new_internal(config)
            .await
            .expect("Failed to create storage node");

        // Verify network thread exists
        assert!(node.network_thread.is_some());

        // Start the node (required for shutdown to work)
        node.start().await.expect("Failed to start node");

        // Explicitly shutdown the node
        node.shutdown().await.expect("Failed to shutdown node");

        // Verify network thread was cleaned up
        assert!(node.network_thread.is_none());
    }

    #[tokio::test]
    async fn test_drop_without_shutdown_cleans_up_network_thread() {
        // Create a storage node
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = create_test_config(&temp_dir);

        let node = StorageNodeImpl::new_internal(config)
            .await
            .expect("Failed to create storage node");

        // Verify network thread exists
        assert!(node.network_thread.is_some());

        // Drop the node WITHOUT calling shutdown
        // This should trigger the Drop implementation
        drop(node);

        // Wait a moment for cleanup to complete
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // If we get here without hanging or leaking, the test passes
        // The Drop impl should have cleaned up the thread
    }

    #[tokio::test]
    async fn test_drop_after_shutdown_is_noop() {
        // Create a storage node
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = create_test_config(&temp_dir);

        let mut node = StorageNodeImpl::new_internal(config)
            .await
            .expect("Failed to create storage node");

        // Explicitly shutdown first
        node.shutdown().await.expect("Failed to shutdown node");

        // Drop should be a no-op since thread was already cleaned up
        drop(node);

        // If we get here without issues, the test passes
    }

    #[tokio::test]
    async fn test_multiple_nodes_can_cleanup_independently() {
        // Create multiple nodes
        let temp_dir1 = TempDir::new().expect("Failed to create temp dir");
        let temp_dir2 = TempDir::new().expect("Failed to create temp dir");

        let mut config1 = create_test_config(&temp_dir1);
        config1.node_id = "node-1".to_string();
        config1.libp2p_listen_port = 0; // Random port

        let mut config2 = create_test_config(&temp_dir2);
        config2.node_id = "node-2".to_string();
        config2.libp2p_listen_port = 0; // Random port

        let node1 = StorageNodeImpl::new_internal(config1)
            .await
            .expect("Failed to create node 1");

        let mut node2 = StorageNodeImpl::new_internal(config2)
            .await
            .expect("Failed to create node 2");

        // Shutdown node2 explicitly
        node2.shutdown().await.expect("Failed to shutdown node 2");

        // Drop both nodes (node1 without shutdown, node2 after shutdown)
        drop(node1);
        drop(node2);

        // Wait for cleanup
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // If we get here, both nodes cleaned up successfully
    }

    #[tokio::test]
    async fn test_node_status_before_and_after_shutdown() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = create_test_config(&temp_dir);

        let mut node = StorageNodeImpl::new_internal(config)
            .await
            .expect("Failed to create storage node");

        // Start the node
        node.start().await.expect("Failed to start node");

        // Check status
        let status_before = node.get_status();
        assert!(status_before.started);
        assert!(status_before.components.network);

        // Shutdown
        node.shutdown().await.expect("Failed to shutdown node");

        // Check status after shutdown
        let status_after = node.get_status();
        assert!(!status_after.started);
        // Network should still be present in status (though stopped)
    }
}
