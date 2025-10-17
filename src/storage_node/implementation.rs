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
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{info, warn};

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
            cache_size_mb: 100,    // Phase 1 default
            read_pool_size: 4,     // Phase 1 default
            ..Default::default()
        };

        let metadata_store = crate::metadata_store::MetadataStoreFactory::create_concrete(metadata_config)
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
            max_chunk_size: 1024 * 1024,                 // 1MB
            default_data_shards: config.default_data_shards,
            default_parity_shards: config.default_parity_shards,
            max_concurrent_operations: 100,              // Phase 1 default
            verification_interval: std::time::Duration::from_secs(3600), // 1 hour
            orphan_cleanup_age: std::time::Duration::from_secs(3600),    // 1 hour
        };

        let file_store =
            FileStoreImpl::new(file_store_config).map_err(|e| Error::ComponentInitFailed {
                component: "FileStore".to_string(),
                reason: e.to_string(),
            })?;

        let file_store = Arc::new(file_store);

        info!("FileStore initialized successfully");

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

        Ok(Self {
            config,
            metadata_store,
            file_store,
            filesystem_service: Some(filesystem_service),
            started: false,
        })
    }

    /// Get reference to FileSystemService (for mounting)
    pub fn filesystem_service(&self) -> Option<&FileSystemServiceImpl> {
        self.filesystem_service.as_ref()
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
        // Future phases will start:
        // - Raft consensus loop
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

        // Step 1: Stop FileSystemService (in Phase 1, this is a no-op as FUSE is managed externally)
        if let Some(_fs_service) = self.filesystem_service.take() {
            info!("FileSystemService stopped");
        }

        // Step 2: FileStore cleanup (in Phase 1, this is a no-op)
        info!("FileStore stopped");

        // Step 3: MetadataStore cleanup (connection will be dropped automatically)
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
                raft_member: false, // Phase 2+
                network: false,     // Phase 2+
                endpoint: false,    // Phase 3+
                watchdog: false,    // Phase 4+
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
