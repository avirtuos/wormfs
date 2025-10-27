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
