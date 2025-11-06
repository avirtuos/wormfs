//! Mount and unmount utilities for WormFS FUSE filesystem.
//!
//! Provides high-level functions to mount and unmount the filesystem
//! with proper configuration and error handling.

use super::factory::FileSystemServiceImplFactory;
use super::types::{Config, Error};
use crate::file_store::FileStore;
use crate::metadata_store::{MetadataStore, MetadataStoreFactory};
use crate::metric_service::{MetricService, MetricServiceImpl};
use std::path::Path;
use std::sync::Arc;

#[cfg(feature = "fuser")]
use super::fuse_adapter::FuseAdapter;

// TODO: Proper solution is to make FileSystemServiceImpl generic over MetadataStore trait
// For Phase 1, we work with the concrete type directly

/// Automatically form a multi-node Raft cluster by having the lowest-ID node add peers.
///
/// This function waits for peers to connect via StorageNetwork, then adds them to the
/// Raft cluster if this node has the lowest ID.
#[cfg(feature = "fuser")]
async fn form_raft_cluster(
    node_id: u64,
    raft_member: Arc<crate::storage_raft_member::StorageRaftMemberImpl>,
    network: Arc<crate::storage_network::StorageNetworkHandle>,
    network_config: Option<crate::storage_network::Config>,
) {
    use crate::storage_raft_member::StorageRaftMember;

    // Log immediately to confirm task is executing
    tracing::info!("Node {}: Cluster formation task STARTED", node_id);

    // Wait for peers to connect and exchange at least 3 heartbeats (heartbeat interval is 5s)
    // 18 seconds allows: initial connection (5s) + 2-3 heartbeats (10-15s) = stable peer info
    tracing::info!("Node {}: Waiting 18 seconds for peers to connect and exchange heartbeats before forming Raft cluster...", node_id);
    tokio::time::sleep(tokio::time::Duration::from_secs(18)).await;

    tracing::info!(
        "Node {}: Wait complete, checking for connected peers...",
        node_id
    );

    // Get connected peers
    let peers = network.get_connected_peers().await;
    tracing::info!("Node {}: Found {} connected peer(s)", node_id, peers.len());

    if peers.is_empty() {
        tracing::info!(
            "Node {}: No peers connected, remaining as single-node cluster",
            node_id
        );
        return;
    }

    // Parse peer information from network config to get ports
    let network_cfg = match network_config {
        Some(cfg) => cfg,
        None => {
            tracing::warn!(
                "Node {}: No network config available for cluster formation",
                node_id
            );
            return;
        }
    };

    // Extract peer node IDs from connected peers
    let mut peer_node_ids: Vec<u64> = Vec::new();
    for peer_info in &peers {
        if let Some(ref peer_node_id_str) = peer_info.node_id {
            match peer_node_id_str.parse::<u64>() {
                Ok(peer_node_id) => {
                    peer_node_ids.push(peer_node_id);
                }
                Err(e) => {
                    tracing::warn!(
                        "Node {}: Failed to parse peer node_id '{}': {}",
                        node_id,
                        peer_node_id_str,
                        e
                    );
                }
            }
        }
    }

    if peer_node_ids.is_empty() {
        tracing::info!(
            "Node {}: No valid peer node IDs found, remaining as single-node cluster",
            node_id
        );
        return;
    }

    // Determine if this node should initiate cluster formation (lowest ID)
    let mut all_node_ids = peer_node_ids.clone();
    all_node_ids.push(node_id);
    all_node_ids.sort();

    if all_node_ids[0] != node_id {
        tracing::info!(
            "Node {}: Not the lowest ID node (lowest is {}), waiting to be added by leader",
            node_id,
            all_node_ids[0]
        );
        return;
    }

    tracing::info!(
        "Node {}: Lowest ID node, initiating cluster formation with {} peers",
        node_id,
        peer_node_ids.len()
    );

    // Add each peer to the cluster
    for peer_info in peers {
        tracing::info!(
            "Node {}: Processing peer - node_id={:?}, addresses={:?}",
            node_id,
            peer_info.node_id,
            peer_info.addresses
        );

        if let Some(ref peer_node_id_str) = peer_info.node_id {
            let peer_node_id = match peer_node_id_str.parse::<u64>() {
                Ok(id) => {
                    tracing::info!("Node {}: Parsed peer node_id {} successfully", node_id, id);
                    id
                }
                Err(e) => {
                    tracing::warn!(
                        "Node {}: Failed to parse peer node_id '{}': {}",
                        node_id,
                        peer_node_id_str,
                        e
                    );
                    continue;
                }
            };

            // Find the corresponding multiaddr from config to extract IP and port
            tracing::info!(
                "Node {}: Looking for multiaddr in {} peer configs",
                node_id,
                network_cfg.peers.len()
            );
            let mut peer_socket_addr = None;
            for peer_cfg in &network_cfg.peers {
                tracing::info!(
                    "Node {}: Checking peer config multiaddr: {}",
                    node_id,
                    peer_cfg.multiaddr
                );

                // Parse multiaddr to extract IP and port
                // Format: /ip4/127.0.0.1/tcp/7102
                let parts: Vec<&str> = peer_cfg.multiaddr.split('/').collect();

                let ip_str = if parts.len() >= 3 && parts[1] == "ip4" {
                    Some(parts[2])
                } else {
                    None
                };

                let port_str = if parts.len() >= 5 && parts[3] == "tcp" {
                    Some(parts[4])
                } else {
                    None
                };

                if let (Some(ip), Some(port)) = (ip_str, port_str) {
                    if let (Ok(ip_addr), Ok(port_num)) =
                        (ip.parse::<std::net::IpAddr>(), port.parse::<u16>())
                    {
                        let socket_addr = std::net::SocketAddr::new(ip_addr, port_num);
                        tracing::info!(
                            "Node {}: Extracted socket address {} from multiaddr",
                            node_id,
                            socket_addr
                        );
                        peer_socket_addr = Some(socket_addr);
                        break;
                    }
                }
            }

            let socket_addr = match peer_socket_addr {
                Some(addr) => addr,
                None => {
                    tracing::warn!("Node {}: Could not extract socket address for peer {} (checked {} peer configs)",
                        node_id, peer_node_id, network_cfg.peers.len());
                    continue;
                }
            };

            // Convert peer_id to base58 string (libp2p standard format)
            let peer_id_str = match libp2p::PeerId::from_bytes(peer_info.peer_id.as_bytes()) {
                Ok(libp2p_peer_id) => libp2p_peer_id.to_string(),
                Err(e) => {
                    tracing::error!(
                        "Node {}: Failed to convert peer_id to libp2p format: {}",
                        node_id,
                        e
                    );
                    continue;
                }
            };

            tracing::info!(
                "Node {}: Adding peer {} (addr={}, peer_id={}) to Raft cluster",
                node_id,
                peer_node_id,
                socket_addr,
                peer_id_str
            );

            match raft_member
                .add_node(
                    crate::storage_raft_member::types::NodeId(peer_node_id),
                    socket_addr,
                    peer_id_str,
                )
                .await
            {
                Ok(_) => {
                    tracing::info!(
                        "Node {}: Successfully added peer {} to Raft cluster",
                        node_id,
                        peer_node_id
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "Node {}: Failed to add peer {} to Raft cluster: {}",
                        node_id,
                        peer_node_id,
                        e
                    );
                }
            }
        }
    }

    tracing::info!("Node {}: Raft cluster formation complete", node_id);
}

/// Mount configuration combining all component configs.
#[derive(Debug, Clone)]
pub struct MountConfig {
    /// Filesystem service configuration
    pub filesystem_config: Config,

    /// MetadataStore configuration
    pub metadata_config: crate::metadata_store::Config,

    /// FileStore configuration
    pub file_store_config: crate::file_store::types::Config,

    /// MetricService configuration (optional)
    pub metric_config: Option<crate::metric_service::Config>,

    /// Admin server configuration (optional)
    pub admin_config: Option<crate::admin::Config>,

    /// StorageNetwork configuration (optional)
    pub network_config: Option<crate::storage_network::Config>,

    /// StorageRaftMember configuration (optional)
    pub raft_config: Option<crate::storage_raft_member::Config>,

    /// Mount point path
    pub mount_point: std::path::PathBuf,

    /// Mount options
    pub mount_options: MountOptions,
}

/// FUSE mount options.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MountOptions {
    /// Allow root user to access the filesystem
    pub allow_root: bool,

    /// Allow other users to access the filesystem
    pub allow_other: bool,

    /// Run in foreground (don't daemonize)
    pub foreground: bool,

    /// Filesystem name for df/mount output
    pub fsname: String,

    /// Enable auto-unmount on process exit
    pub auto_unmount: bool,

    /// Enable debug logging from FUSE
    pub debug: bool,
}

impl Default for MountOptions {
    fn default() -> Self {
        Self {
            allow_root: false,
            allow_other: false,
            foreground: true,
            fsname: "wormfs".to_string(),
            auto_unmount: false, // Disabled by default to avoid requiring user_allow_other in /etc/fuse.conf
            debug: false,
        }
    }
}

/// Mount the filesystem at the specified mount point.
///
/// This function:
/// 1. Initializes MetadataStore and FileStore
/// 2. Creates FileSystemServiceImpl
/// 3. Initializes root directory
/// 4. Mounts via FUSE
///
/// # Arguments
///
/// * `config` - Mount configuration
///
/// # Returns
///
/// Returns Ok(()) if mount succeeds. The function will block until unmount.
///
/// # Errors
///
/// Returns an error if:
/// - Mount point doesn't exist
/// - MetadataStore initialization fails
/// - FileStore initialization fails
/// - FUSE mount fails
#[cfg(feature = "fuser")]
pub async fn mount_filesystem(config: MountConfig) -> Result<(), Error> {
    use tokio::runtime::Handle;

    tracing::info!("Mounting WormFS at {:?}", config.mount_point);

    // Verify mount point exists
    if !config.mount_point.exists() {
        return Err(Error::InvalidArgument(format!(
            "Mount point does not exist: {:?}",
            config.mount_point
        )));
    }

    // Initialize MetadataStore (concrete type for FileSystemServiceImplFactory)
    tracing::info!("Initializing MetadataStore...");
    let metadata_store = MetadataStoreFactory::create_concrete(config.metadata_config.clone())
        .await
        .map_err(|e| Error::MetadataError(format!("Failed to create MetadataStore: {}", e)))?;

    metadata_store
        .initialize_schema()
        .await
        .map_err(|e| Error::MetadataError(format!("Failed to initialize schema: {}", e)))?;

    // Initialize node and disks in database
    tracing::info!("Initializing node and disks...");
    metadata_store
        .initialize_node_and_disks(&config.file_store_config.disk_paths)
        .await
        .map_err(|e| Error::MetadataError(format!("Failed to initialize node and disks: {}", e)))?;

    // Initialize FileStore
    tracing::info!("Initializing FileStore...");
    let file_store = Arc::new(
        FileStore::new(config.file_store_config.clone())
            .map_err(|e| Error::DataFailed(format!("Failed to create FileStore: {}", e)))?,
    );

    // Initialize MetricService if configured
    let metrics = if let Some(metric_config) = config.metric_config.clone() {
        tracing::info!("Initializing MetricService...");
        let metric_service = MetricServiceImpl::new(metric_config.clone())
            .map_err(|e| Error::Internal(format!("Failed to create MetricService: {}", e)))?;

        // Background aggregation loop is automatically started in new()
        // No need to spawn a separate task here

        Some(Arc::new(metric_service))
    } else {
        tracing::info!("MetricService disabled");
        None
    };

    // Initialize StorageNetwork if configured
    let network_handle = if let Some(network_config) = config.network_config.clone() {
        tracing::info!("Initializing StorageNetwork...");

        use crate::storage_network::StorageNetworkFactory;

        let (network_inner, handle) = StorageNetworkFactory::create(network_config)
            .await
            .map_err(|e| Error::Internal(format!("Failed to create StorageNetwork: {}", e)))?;

        // Spawn the network event loop in a dedicated thread with LocalSet
        // This is necessary because libp2p's Swarm is !Send
        let node_id = handle.config.node_id.clone();
        std::thread::spawn(move || {
            // Create a new tokio runtime for this thread
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create runtime for network event loop");

            // Run the event loop in a LocalSet to support !Send futures
            let local = tokio::task::LocalSet::new();
            runtime.block_on(local.run_until(async move {
                tracing::info!("Starting StorageNetwork event loop for node {}", node_id);
                if let Err(e) = network_inner.run().await {
                    tracing::error!("StorageNetwork event loop failed for {}: {}", node_id, e);
                }
                tracing::info!("StorageNetwork event loop stopped for {}", node_id);
            }));
        });

        // Dial configured peers
        tracing::info!("Dialing configured peers...");
        if let Err(e) = handle.dial_configured_peers().await {
            tracing::warn!("Failed to dial some peers: {}", e);
        } else {
            tracing::info!("Peer dialing initiated");
        }

        Some(Arc::new(handle))
    } else {
        tracing::info!("StorageNetwork disabled");
        None
    };

    // Initialize StorageRaftMember if configured
    let raft_member = if let Some(mut raft_config) = config.raft_config.clone() {
        // Node ID from filesystem config (already a u64)
        let node_id_num = config.filesystem_config.node_id;
        tracing::info!("Initializing StorageRaftMember for node {}...", node_id_num);

        use crate::storage_raft_member::{
            RaftRpcHandler, StorageRaftMember, StorageRaftMemberImpl,
        };

        // Set paths if not already set
        if raft_config.transaction_log_path.as_os_str().is_empty() {
            return Err(Error::InvalidArgument(
                "Raft transaction_log_path must be configured".to_string(),
            ));
        }
        if raft_config.snapshot_directory.as_os_str().is_empty() {
            return Err(Error::InvalidArgument(
                "Raft snapshot_dir must be configured".to_string(),
            ));
        }

        raft_config.metadata_db_path = config.metadata_config.database_path.clone();

        // Set storage_network reference (required for Raft RPC communication)
        if let Some(ref network) = network_handle {
            raft_config.storage_network = Some(Arc::new(network.as_ref().clone())
                as Arc<dyn crate::storage_network::NetworkHandleTrait>);
        } else {
            return Err(Error::InvalidArgument(
                "StorageNetwork must be configured to use Raft".to_string(),
            ));
        }

        let mut raft_member = StorageRaftMemberImpl::new(
            crate::storage_raft_member::types::NodeId(node_id_num),
            raft_config,
            metadata_store.clone(),
        )
        .await
        .map_err(|e| Error::Internal(format!("Failed to create StorageRaftMember: {}", e)))?;

        // Initialize Raft cluster
        // IMPORTANT: Only Node 1 initializes as a single-node cluster.
        // Other nodes do NOT initialize - they wait to be added as learners
        // by Node 1 via the cluster formation task.
        //
        // This prevents creating multiple independent single-node clusters
        // that cannot be merged (Raft doesn't support merging clusters).
        if node_id_num == 1 {
            tracing::info!(
                "Node 1: Initializing as single-node Raft cluster (will add other nodes later)"
            );
            raft_member
                .initialize(vec![])
                .await
                .map_err(|e| Error::Internal(format!("Raft initialization failed: {}", e)))?;
            tracing::info!("Node 1: Raft cluster initialized successfully");
        } else {
            tracing::info!(
                "Node {}: Skipping Raft initialization (will be added to cluster by Node 1)",
                node_id_num
            );
        }

        let raft_member = Arc::new(raft_member);

        // Wire up Raft RPC handler with network if available
        if let Some(ref network) = network_handle {
            network
                .register_raft_handler(raft_member.clone() as Arc<dyn RaftRpcHandler>)
                .await
                .map_err(|e| Error::Internal(format!("Failed to register Raft handler: {}", e)))?;
            tracing::info!("StorageRaftMember wired to network successfully");

            // Spawn background task to form multi-node cluster automatically
            // Only the node with the lowest ID attempts to add other nodes
            // Store the handle to prevent the task from being dropped
            let raft_for_cluster_formation = raft_member.clone();
            let network_for_cluster_formation = network.clone();
            let network_config_for_cluster_formation = config.network_config.clone();
            let cluster_formation_handle = tokio::spawn(async move {
                form_raft_cluster(
                    node_id_num,
                    raft_for_cluster_formation,
                    network_for_cluster_formation,
                    network_config_for_cluster_formation,
                )
                .await;
            });

            // Log that we've started the cluster formation task
            tracing::info!(
                "Node {}: Cluster formation task spawned successfully",
                node_id_num
            );

            // Spawn a task to log the result of cluster formation (non-blocking)
            tokio::spawn(async move {
                match cluster_formation_handle.await {
                    Ok(()) => tracing::info!("Cluster formation task completed successfully"),
                    Err(e) => tracing::error!("Cluster formation task failed: {}", e),
                }
            });
        } else {
            tracing::warn!("StorageNetwork not available - Raft RPC handler not registered");
        }

        Some(raft_member)
    } else {
        tracing::info!("StorageRaftMember disabled");
        None
    };

    // Start admin server if configured and metrics are available
    let _admin_handle = if let (Some(admin_cfg), Some(metrics_svc)) =
        (config.admin_config.clone(), metrics.as_ref())
    {
        tracing::info!(
            "Starting admin server on http://{}:{}",
            admin_cfg.bind_address,
            admin_cfg.port
        );

        // Wrap the config in Arc for sharing with admin server
        let mount_config_arc = Arc::new(config.clone());

        let admin_server = crate::admin::AdminServer::new(
            admin_cfg.clone(),
            mount_config_arc,
            Arc::clone(metrics_svc),
            network_handle.clone(),
            raft_member.clone(),
        );

        match admin_server.start() {
            Ok(handle) => {
                tracing::info!("Admin server task spawned successfully");

                // Give the server a moment to bind to the port
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                tracing::info!(
                    "Admin server available at http://{}:{}",
                    admin_cfg.bind_address,
                    admin_cfg.port
                );

                Some(handle)
            }
            Err(e) => {
                tracing::warn!("Failed to start admin server: {}", e);
                None
            }
        }
    } else {
        if config.admin_config.is_some() && metrics.is_none() {
            tracing::warn!("Admin server requires metrics to be enabled");
        }
        None
    };

    // Create FileSystemService via factory
    tracing::info!("Creating FileSystemService...");
    let service = Arc::new(
        FileSystemServiceImplFactory::create(
            config.filesystem_config.clone(),
            metadata_store,
            file_store,
            metrics,
        )
        .await?,
    );

    // Initialize root directory
    tracing::info!("Initializing root directory...");
    service
        .initialize_root()
        .await
        .map_err(|e| Error::Internal(format!("Failed to initialize root: {}", e)))?;

    // Start background tasks (StripeCache flush task, lock extension task)
    tracing::info!("Starting background tasks...");
    Arc::clone(&service).start_background_tasks().await;

    // Create FUSE adapter
    let runtime_handle = Handle::current();
    let fuse_adapter = FuseAdapter::new(Arc::clone(&service), runtime_handle);

    // Build mount options
    let mut mount_opts = vec![fuser::MountOption::FSName(
        config.mount_options.fsname.clone(),
    )];

    if config.mount_options.allow_root {
        mount_opts.push(fuser::MountOption::AllowRoot);
    }

    if config.mount_options.allow_other {
        mount_opts.push(fuser::MountOption::AllowOther);
    }

    if config.mount_options.auto_unmount {
        mount_opts.push(fuser::MountOption::AutoUnmount);
    }

    // Mount the filesystem
    tracing::info!("Mounting FUSE filesystem...");

    // Run FUSE in a blocking task since it blocks the thread
    let mount_point = config.mount_point.clone();
    let result =
        tokio::task::spawn_blocking(move || fuser::mount2(fuse_adapter, &mount_point, &mount_opts))
            .await
            .map_err(|e| Error::Internal(format!("Mount task failed: {}", e)))?;

    result.map_err(|e| Error::Internal(format!("FUSE mount failed: {}", e)))?;

    tracing::info!("Filesystem unmounted");
    Ok(())
}

/// Stub for when fuser feature is disabled.
#[cfg(not(feature = "fuser"))]
pub async fn mount_filesystem(_config: MountConfig) -> Result<(), Error> {
    Err(Error::NotSupported(
        "FUSE support not compiled (enable 'fuser' feature)".into(),
    ))
}

/// Unmount the filesystem at the specified mount point.
///
/// # Arguments
///
/// * `mount_point` - Path to the mount point
///
/// # Errors
///
/// Returns an error if unmount fails.
#[cfg(feature = "fuser")]
pub fn unmount_filesystem(mount_point: &Path) -> Result<(), Error> {
    tracing::info!("Unmounting filesystem at {:?}", mount_point);

    // Use fusermount on Linux, umount on macOS
    #[cfg(target_os = "linux")]
    let result = std::process::Command::new("fusermount")
        .arg("-u")
        .arg(mount_point)
        .status();

    #[cfg(target_os = "macos")]
    let result = std::process::Command::new("umount")
        .arg(mount_point)
        .status();

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let result = Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "Unmount not supported on this platform",
    ));

    match result {
        Ok(status) if status.success() => {
            tracing::info!("Filesystem unmounted successfully");
            Ok(())
        }
        Ok(status) => Err(Error::Internal(format!(
            "Unmount failed with exit code: {:?}",
            status.code()
        ))),
        Err(e) => Err(Error::Internal(format!("Failed to execute unmount: {}", e))),
    }
}

/// Stub for when fuser feature is disabled.
#[cfg(not(feature = "fuser"))]
pub fn unmount_filesystem(_mount_point: &Path) -> Result<(), Error> {
    Err(Error::NotSupported(
        "FUSE support not compiled (enable 'fuser' feature)".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mount_options_default() {
        let opts = MountOptions::default();
        assert_eq!(opts.allow_root, false);
        assert_eq!(opts.allow_other, false);
        assert_eq!(opts.foreground, true);
        assert_eq!(opts.fsname, "wormfs");
        assert_eq!(opts.auto_unmount, false);
        assert_eq!(opts.debug, false);
    }
}
